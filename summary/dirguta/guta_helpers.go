/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package dirguta

import (
	"encoding/binary"
	"maps"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	// MaxNumOfGUTAKeys is the maximum number of per-age keys emitted for one file.
	MaxNumOfGUTAKeys = 34

	lengthOfGUTAKey = 12
)

// GUTAKeyPool reuses fixed-size key arrays for per-file key generation.
var GUTAKeyPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return new([MaxNumOfGUTAKeys]GUTAKey)
	},
}

var summaryWithTimesPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return new(summary.SummaryWithTimes)
	},
}

// GUTAKey identifies one group, user, file type, and age bucket summary.
type GUTAKey struct {
	GID, UID uint32
	FileType db.DirGUTAFileType
	Age      db.DirGUTAge
}

func (g GUTAKey) String() string {
	var a [lengthOfGUTAKey]byte

	binary.BigEndian.PutUint32(a[:4], g.GID)
	binary.BigEndian.PutUint32(a[4:8], g.UID)
	a[8] = uint8(g.FileType) //nolint:gosec // filetype values are constrained to <= 255 in this context
	a[9] = uint8(g.Age)

	return unsafe.String(&a[0], len(a))
}

// GetGUTA converts a store entry into a db.GUTA.
func GetGUTA(store GUTAStore, guta GUTAKey) *db.GUTA {
	s := store.SumMap[guta]

	return &db.GUTA{
		GID:         guta.GID,
		UID:         guta.UID,
		FT:          guta.FileType,
		Age:         guta.Age,
		Count:       uint64(s.Count), //nolint:gosec
		Size:        uint64(s.Size),  //nolint:gosec
		Atime:       s.Atime,
		ATimeRanges: s.AtimeBuckets,
		Mtime:       s.Mtime,
		MTimeRanges: s.MtimeBuckets,
	}
}

// GUTAKeys is a sortable list of GUTAKey values.
type GUTAKeys []GUTAKey

// GUTAKeysFromEntry returns keys for a given GID, UID, and file type.
func GUTAKeysFromEntry(gid, uid uint32, ft db.DirGUTAFileType) GUTAKeys {
	var keys GUTAKeys

	keys.Append(gid, uid, ft)

	return keys
}

func (g GUTAKeys) Len() int {
	return len(g)
}

func (g GUTAKeys) Less(i, j int) bool {
	if g[i].GID < g[j].GID {
		return true
	}

	if g[i].GID > g[j].GID {
		return false
	}

	if g[i].UID < g[j].UID {
		return true
	}

	if g[i].UID > g[j].UID {
		return false
	}

	if g[i].FileType < g[j].FileType {
		return true
	}

	if g[i].FileType > g[j].FileType {
		return false
	}

	return g[i].Age < g[j].Age
}

func (g GUTAKeys) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

// Append appends keys for all ages for the given group, user, and file type.
func (g *GUTAKeys) Append(gid, uid uint32, fileType db.DirGUTAFileType) {
	for _, age := range db.DirGUTAges {
		*g = append(*g, GUTAKey{gid, uid, fileType, age})
	}
}

// UpdateExistingHardlink merges parent and child inode entries and updates stores.
func UpdateExistingHardlink(parentStore, childStore *GUTAStore, parentEntry, childEntry *InodeEntry) {
	existingParentKeys := GUTAKeysFromEntry(parentEntry.gid, parentEntry.uid, parentEntry.fileType)

	parentStore.SubtractFromStore(existingParentKeys, parentEntry.size, parentEntry.atime, parentEntry.mtime)

	existingChildKeys := GUTAKeysFromEntry(childEntry.gid, childEntry.uid, childEntry.fileType)

	childStore.SubtractFromStore(existingChildKeys, childEntry.size, childEntry.atime, childEntry.mtime)

	parentEntry.fileType |= childEntry.fileType
	parentEntry.size = max(parentEntry.size, childEntry.size)
	parentEntry.atime = min(parentEntry.atime, childEntry.atime)
	parentEntry.mtime = max(parentEntry.mtime, childEntry.mtime)

	updatedKeys := GUTAKeysFromEntry(parentEntry.gid, parentEntry.uid, parentEntry.fileType)

	childStore.AddForEach(updatedKeys, parentEntry.size, parentEntry.atime, parentEntry.mtime)
}

func addNewHardlink(
	store *GUTAStore,
	seenHardlinks map[int64]*InodeEntry,
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) {
	keys := GUTAKeysFromEntry(info.GID, info.UID, ft)

	seenHardlinks[info.Inode] = &InodeEntry{
		fileType: ft,
		size:     info.Size,
		atime:    atime,
		mtime:    info.MTime,
		gid:      info.GID,
		uid:      info.UID,
	}
	store.AddForEach(keys, info.Size, atime, info.MTime)
}

func updateSeenHardlink(
	store *GUTAStore,
	entry *InodeEntry,
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) {
	keys := GUTAKeysFromEntry(entry.gid, entry.uid, entry.fileType)

	store.SubtractFromStore(keys, entry.size, entry.atime, entry.mtime)

	entry.fileType |= ft
	entry.size = max(entry.size, info.Size)
	entry.atime = min(entry.atime, atime)
	entry.mtime = max(entry.mtime, info.MTime)

	keys = GUTAKeysFromEntry(entry.gid, entry.uid, entry.fileType)

	store.AddForEach(keys, entry.size, entry.atime, entry.mtime)
}

// GUTAStore accumulates summaries by GUTA key using a fixed reference time.
type GUTAStore struct {
	SumMap  map[GUTAKey]*summary.SummaryWithTimes
	RefTime int64
}

// NewGUTAStore returns an empty GUTAStore using refTime for age buckets.
func NewGUTAStore(refTime int64) GUTAStore {
	return GUTAStore{
		SumMap:  make(map[GUTAKey]*summary.SummaryWithTimes),
		RefTime: refTime,
	}
}

// Add adds a file summary for key when the file fits the key's age bucket.
func (store *GUTAStore) Add(gkey GUTAKey, size int64, atime int64, mtime int64) {
	if !gkey.Age.FitsAgeInterval(atime, mtime, store.RefTime) {
		return
	}

	s, ok := store.SumMap[gkey]
	if !ok {
		s = newSummaryWithTimes()
		store.SumMap[gkey] = s
	}

	s.Add(size, atime, mtime, store.RefTime)
}

func newSummaryWithTimes() *summary.SummaryWithTimes {
	return summaryWithTimesPool.Get().(*summary.SummaryWithTimes) //nolint:errcheck,forcetypeassert
}

// Sort returns the store keys in deterministic GUTA order.
func (store GUTAStore) Sort() GUTAKeys {
	keys := GUTAKeys(slices.Collect(maps.Keys(store.SumMap)))

	sort.Sort(keys)

	return keys
}

// AddForEach adds one file summary for each supplied key.
func (store *GUTAStore) AddForEach(gutaKeys []GUTAKey, size int64, atime int64, mtime int64) {
	for _, agutaKey := range gutaKeys {
		store.Add(agutaKey, size, atime, mtime)
	}
}

// SubtractFromStore subtracts one file summary from each key in the store.
func (store *GUTAStore) SubtractFromStore(keys GUTAKeys, size int64, atime int64, mtime int64) {
	for _, key := range keys {
		if !key.Age.FitsAgeInterval(atime, mtime, store.RefTime) {
			continue
		}

		summary := store.SumMap[key]
		summary.Count--
		summary.Size -= size
	}
}

// DrainInto moves every summary from the child store into parent.
func (store *GUTAStore) DrainInto(parent *GUTAStore) {
	for key, childSummary := range store.SumMap {
		if existing, ok := parent.SumMap[key]; ok {
			existing.AddSummary(childSummary)
			recycleSummaryWithTimes(childSummary)
		} else {
			parent.SumMap[key] = childSummary
		}

		delete(store.SumMap, key)
	}
}

func recycleSummaryWithTimes(s *summary.SummaryWithTimes) {
	if s == nil {
		return
	}

	*s = summary.SummaryWithTimes{}
	summaryWithTimesPool.Put(s)
}

// Clear recycles all summary values and empties the store.
func (store *GUTAStore) Clear() {
	for _, sum := range store.SumMap {
		recycleSummaryWithTimes(sum)
	}

	clear(store.SumMap)
}

// HandleHardlink updates store and seenHardlinks for a hardlink file.
func HandleHardlink(
	store *GUTAStore,
	seenHardlinks map[int64]*InodeEntry,
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) bool {
	if info.IsDir() || info.Nlink <= 1 || info.Inode == 0 {
		return false
	}

	entry, exists := seenHardlinks[info.Inode]
	if !exists {
		addNewHardlink(store, seenHardlinks, info, ft, atime)

		return true
	}

	updateSeenHardlink(store, entry, info, ft, atime)

	return true
}

// MergeSeenHardlinks merges a child's inode set into the parent's inode set.
func MergeSeenHardlinks(
	parentStore *GUTAStore,
	parentSeen map[int64]*InodeEntry,
	childStore *GUTAStore,
	childSeen map[int64]*InodeEntry,
) {
	for inode, childEntry := range childSeen {
		if parentEntry, exists := parentSeen[inode]; exists {
			UpdateExistingHardlink(parentStore, childStore, parentEntry, childEntry)
		} else {
			parentSeen[inode] = childEntry
		}
	}
}

type gutaStore = GUTAStore

// InodeEntry stores metadata for a specific inode to track hardlinks.
type InodeEntry struct {
	fileType db.DirGUTAFileType
	size     int64
	atime    int64
	mtime    int64
	gid      uint32
	uid      uint32
}

type inodeEntry = InodeEntry
