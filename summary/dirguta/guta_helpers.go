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

	numGUTAAges               = len(db.DirGUTAges)
	initialTupleSummariesSize = 2
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
	s := store.Summary(guta)
	if s == nil {
		return nil
	}

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

func compareMaterializedGUTA(left *db.GUTA, right *db.GUTA) int {
	leftKey := GUTAKey{GID: left.GID, UID: left.UID, FileType: left.FT, Age: left.Age}
	rightKey := GUTAKey{GID: right.GID, UID: right.UID, FileType: right.FT, Age: right.Age}
	keys := GUTAKeys{leftKey, rightKey}

	switch {
	case keys.Less(0, 1):
		return -1
	case keys.Less(1, 0):
		return 1
	default:
		return 0
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

// MaterializeGUTAs combines ordinary summaries with finalised hardlink inode entries.
func MaterializeGUTAs(store GUTAStore, seenHardlinks map[int64]*InodeEntry) db.GUTAs {
	ordinary := materializeStoreGUTAs(store)
	if len(seenHardlinks) == 0 {
		return ordinary
	}

	hardlinks := NewGUTAStore(store.RefTime)

	for _, entry := range seenHardlinks {
		keys := GUTAKeysFromEntry(entry.gid, entry.uid, entry.fileType)
		hardlinks.AddForEach(keys, entry.size, entry.atime, entry.mtime)
	}

	materialised := mergeMaterializedGUTAs(ordinary, materializeStoreGUTAs(hardlinks))
	hardlinks.Clear()

	return materialised
}

func materializeStoreGUTAs(store GUTAStore) db.GUTAs {
	keys := store.Sort()
	gutas := make(db.GUTAs, 0, len(keys))

	for _, key := range keys {
		gutas = append(gutas, GetGUTA(store, key))
	}

	return gutas
}

func mergeMaterializedGUTAs(left db.GUTAs, right db.GUTAs) db.GUTAs {
	values := make([]db.GUTA, 0, len(left)+len(right))
	merged := make(db.GUTAs, 0, len(left)+len(right))

	for len(left) > 0 || len(right) > 0 {
		next, remainingLeft, remainingRight := nextMaterializedGUTA(left, right)
		left = remainingLeft
		right = remainingRight

		values = append(values, next)
		merged = append(merged, &values[len(values)-1])
	}

	return merged
}

func nextMaterializedGUTA(left db.GUTAs, right db.GUTAs) (db.GUTA, db.GUTAs, db.GUTAs) {
	switch {
	case len(right) == 0 || len(left) > 0 && compareMaterializedGUTA(left[0], right[0]) < 0:
		return *left[0], left[1:], right
	case len(left) == 0 || compareMaterializedGUTA(left[0], right[0]) > 0:
		return *right[0], left, right[1:]
	default:
		merged := *left[0]
		addMaterializedGUTA(&merged, right[0])

		return merged, left[1:], right[1:]
	}
}

func addMaterializedGUTA(sum *db.GUTA, addition *db.GUTA) {
	sum.Count += addition.Count

	sum.Size += addition.Size
	if addition.Atime > 0 && (sum.Atime == 0 || addition.Atime < sum.Atime) {
		sum.Atime = addition.Atime
	}

	if addition.Mtime > sum.Mtime {
		sum.Mtime = addition.Mtime
	}

	for index := range sum.ATimeRanges {
		sum.ATimeRanges[index] += addition.ATimeRanges[index]
		sum.MTimeRanges[index] += addition.MTimeRanges[index]
	}
}

type gutaTupleKey struct {
	GID      uint32
	UID      uint32
	FileType db.DirGUTAFileType
}

func tupleKeyForGUTA(key GUTAKey) gutaTupleKey {
	return gutaTupleKey{
		GID:      key.GID,
		UID:      key.UID,
		FileType: key.FileType,
	}
}

func (key gutaTupleKey) gutaKey(age db.DirGUTAge) GUTAKey {
	return GUTAKey{
		GID:      key.GID,
		UID:      key.UID,
		FileType: key.FileType,
		Age:      age,
	}
}

type gutaInlineTuple struct {
	key           gutaTupleKey
	singleMask    uint32
	singleSummary *summary.SummaryWithTimes
	maskSummaries map[uint32]*summary.SummaryWithTimes
	ageCache      [numGUTAAges]*summary.SummaryWithTimes
	ageMask       uint32
}

func newGUTAInlineTuple(key gutaTupleKey) *gutaInlineTuple {
	return &gutaInlineTuple{key: key}
}

func (tuple *gutaInlineTuple) len() int {
	return bitsOnesCount32(tuple.ageMask)
}

func bitsOnesCount32(mask uint32) int {
	count := 0

	for mask != 0 {
		mask &= mask - 1
		count++
	}

	return count
}

func (tuple *gutaInlineTuple) summary(age db.DirGUTAge) *summary.SummaryWithTimes {
	idx, ok := gutaAgeIndex(age)
	if !ok || tuple.ageMask&(uint32(1)<<idx) == 0 {
		return nil
	}

	if tuple.ageCache[idx] == nil {
		tuple.ageCache[idx] = tuple.materializeAge(uint32(1) << idx)
	}

	return tuple.ageCache[idx]
}

func (tuple *gutaInlineTuple) materializeAge(ageBit uint32) *summary.SummaryWithTimes {
	var out *summary.SummaryWithTimes

	tuple.forEachMaskSummary(func(mask uint32, sum *summary.SummaryWithTimes) {
		if mask&ageBit == 0 {
			return
		}

		if out == nil {
			out = newSummaryWithTimes()
		}

		out.AddSummary(sum)
	})

	return out
}

func (tuple *gutaInlineTuple) add(
	ageMask uint32,
	size int64,
	atime int64,
	mtime int64,
	refTime int64,
) {
	tuple.clearAgeCache()
	tuple.ageMask |= ageMask
	tuple.summaryForMask(ageMask).Add(size, atime, mtime, refTime)
}

func (tuple *gutaInlineTuple) summaryForMask(ageMask uint32) *summary.SummaryWithTimes {
	if tuple.singleSummary == nil && len(tuple.maskSummaries) == 0 {
		tuple.singleMask = ageMask
		tuple.singleSummary = newSummaryWithTimes()

		return tuple.singleSummary
	}

	if tuple.singleSummary != nil && tuple.singleMask == ageMask {
		return tuple.singleSummary
	}

	tuple.promoteSingleMask()

	sum := tuple.maskSummaries[ageMask]
	if sum == nil {
		sum = newSummaryWithTimes()
		tuple.maskSummaries[ageMask] = sum
	}

	return sum
}

func (tuple *gutaInlineTuple) takeSummary(age db.DirGUTAge, sum *summary.SummaryWithTimes) {
	idx, ok := gutaAgeIndex(age)
	if !ok {
		return
	}

	ageMask := uint32(1) << idx

	tuple.clearAgeCache()
	tuple.ageMask |= ageMask

	existing := tuple.exactSummary(ageMask)
	if existing != nil {
		existing.AddSummary(sum)
		recycleSummaryWithTimes(sum)

		return
	}

	tuple.storeMaskSummary(ageMask, sum)
}

func gutaAgeIndex(age db.DirGUTAge) (int, bool) {
	idx := int(age)

	return idx, idx < numGUTAAges
}

func (tuple *gutaInlineTuple) exactSummary(ageMask uint32) *summary.SummaryWithTimes {
	if tuple.singleSummary != nil && tuple.singleMask == ageMask {
		return tuple.singleSummary
	}

	return tuple.maskSummaries[ageMask]
}

func (tuple *gutaInlineTuple) storeMaskSummary(ageMask uint32, sum *summary.SummaryWithTimes) {
	if tuple.singleSummary == nil && len(tuple.maskSummaries) == 0 {
		tuple.singleMask = ageMask
		tuple.singleSummary = sum

		return
	}

	tuple.promoteSingleMask()
	tuple.maskSummaries[ageMask] = sum
}

func (tuple *gutaInlineTuple) promoteSingleMask() {
	if tuple.singleSummary == nil {
		if tuple.maskSummaries == nil {
			tuple.maskSummaries = make(map[uint32]*summary.SummaryWithTimes)
		}

		return
	}

	tuple.maskSummaries = map[uint32]*summary.SummaryWithTimes{
		tuple.singleMask: tuple.singleSummary,
	}
	tuple.singleMask = 0
	tuple.singleSummary = nil
}

func (tuple *gutaInlineTuple) appendKeys(keys *GUTAKeys) {
	for idx := range numGUTAAges {
		if tuple.ageMask&(uint32(1)<<idx) == 0 {
			continue
		}

		*keys = append(*keys, tuple.gutaKey(db.DirGUTAge(idx)))
	}
}

func (tuple *gutaInlineTuple) drainExpanded(fn func(GUTAKey, *summary.SummaryWithTimes)) {
	for idx := range numGUTAAges {
		if tuple.ageMask&(uint32(1)<<idx) == 0 {
			continue
		}

		sum := tuple.summary(db.DirGUTAge(idx))
		fn(tuple.gutaKey(db.DirGUTAge(idx)), sum)
		tuple.ageCache[idx] = nil
	}

	tuple.recycleMaskSummaries()
	tuple.ageMask = 0
}

func (tuple *gutaInlineTuple) merge(child *gutaInlineTuple) {
	child.clearAgeCache()
	tuple.clearAgeCache()

	child.forEachMaskSummary(func(mask uint32, childSummary *summary.SummaryWithTimes) {
		existing := tuple.summaryForMask(mask)
		existing.AddSummary(childSummary)
		recycleSummaryWithTimes(childSummary)
	})

	tuple.ageMask |= child.ageMask
	child.clearMaskSummaries()
	child.ageMask = 0
}

func (tuple *gutaInlineTuple) forEachMaskSummary(fn func(uint32, *summary.SummaryWithTimes)) {
	if tuple.singleSummary != nil {
		fn(tuple.singleMask, tuple.singleSummary)

		return
	}

	for mask, sum := range tuple.maskSummaries {
		fn(mask, sum)
	}
}

func (tuple *gutaInlineTuple) clearAgeCache() {
	for idx, sum := range tuple.ageCache {
		if sum == nil {
			continue
		}

		recycleSummaryWithTimes(sum)

		tuple.ageCache[idx] = nil
	}
}

func (tuple *gutaInlineTuple) recycleMaskSummaries() {
	tuple.forEachMaskSummary(func(_ uint32, sum *summary.SummaryWithTimes) {
		recycleSummaryWithTimes(sum)
	})
	tuple.clearMaskSummaries()
}

func (tuple *gutaInlineTuple) clearMaskSummaries() {
	tuple.singleMask = 0
	tuple.singleSummary = nil
	clear(tuple.maskSummaries)
	tuple.maskSummaries = nil
}

func (tuple *gutaInlineTuple) recycle() {
	tuple.clearAgeCache()
	tuple.recycleMaskSummaries()
	tuple.ageMask = 0
}

func (tuple *gutaInlineTuple) gutaKey(age db.DirGUTAge) GUTAKey {
	return tuple.key.gutaKey(age)
}

// GUTAStore accumulates summaries by GUTA key using a fixed reference time.
type GUTAStore struct {
	SumMap         map[GUTAKey]*summary.SummaryWithTimes
	RefTime        int64
	inlineTuple    *gutaInlineTuple
	tupleSummaries map[gutaTupleKey]*gutaInlineTuple
}

// NewGUTAStore returns an empty GUTAStore using refTime for age buckets.
func NewGUTAStore(refTime int64) GUTAStore {
	return GUTAStore{
		RefTime: refTime,
	}
}

// Add adds a file summary for key when the file fits the key's age bucket.
func (store *GUTAStore) Add(gkey GUTAKey, size int64, atime int64, mtime int64) {
	if !gkey.Age.FitsAgeInterval(atime, mtime, store.RefTime) {
		return
	}

	store.addMask(tupleKeyForGUTA(gkey), uint32(1)<<gkey.Age, size, atime, mtime)
}

func newSummaryWithTimes() *summary.SummaryWithTimes {
	return summaryWithTimesPool.Get().(*summary.SummaryWithTimes) //nolint:errcheck,forcetypeassert
}

// Sort returns the store keys in deterministic GUTA order.
func (store GUTAStore) Sort() GUTAKeys {
	keys := make(GUTAKeys, 0, store.Len())

	store.appendInlineKeys(&keys)

	for key := range store.SumMap {
		keys = append(keys, key)
	}

	sort.Sort(keys)

	return keys
}

// Len returns the number of GUTA summaries currently stored.
func (store GUTAStore) Len() int {
	length := len(store.SumMap)
	if store.inlineTuple != nil {
		length += store.inlineTuple.len()
	}

	for _, tuple := range store.tupleSummaries {
		length += tuple.len()
	}

	return length
}

// Empty reports whether the store has no summaries.
func (store GUTAStore) Empty() bool {
	return len(store.SumMap) == 0 && store.inlineTuple == nil && len(store.tupleSummaries) == 0
}

// Summary returns the summary stored for key, or nil when key is absent.
func (store GUTAStore) Summary(key GUTAKey) *summary.SummaryWithTimes {
	if store.SumMap != nil {
		return store.SumMap[key]
	}

	if store.inlineTuple == nil || store.inlineTuple.key != tupleKeyForGUTA(key) {
		tuple := store.tupleSummaries[tupleKeyForGUTA(key)]
		if tuple == nil {
			return nil
		}

		return tuple.summary(key.Age)
	}

	return store.inlineTuple.summary(key.Age)
}

func (store GUTAStore) appendInlineKeys(keys *GUTAKeys) {
	if store.inlineTuple == nil {
		for _, tuple := range store.tupleSummaries {
			tuple.appendKeys(keys)
		}

		return
	}

	store.inlineTuple.appendKeys(keys)

	for _, tuple := range store.tupleSummaries {
		tuple.appendKeys(keys)
	}
}

func (store *GUTAStore) addMask(
	tupleKey gutaTupleKey,
	ageMask uint32,
	size int64,
	atime int64,
	mtime int64,
) {
	if ageMask == 0 {
		return
	}

	if store.SumMap != nil {
		store.addMaskToMap(tupleKey, ageMask, size, atime, mtime)

		return
	}

	store.tupleForAdd(tupleKey).add(ageMask, size, atime, mtime, store.RefTime)
}

func (store *GUTAStore) addMaskToMap(
	tupleKey gutaTupleKey,
	ageMask uint32,
	size int64,
	atime int64,
	mtime int64,
) {
	for idx := range numGUTAAges {
		if ageMask&(uint32(1)<<idx) == 0 {
			continue
		}

		key := tupleKey.gutaKey(db.DirGUTAge(idx))
		store.mapSummaryForAdd(key).Add(size, atime, mtime, store.RefTime)
	}
}

func (store *GUTAStore) mapSummaryForAdd(key GUTAKey) *summary.SummaryWithTimes {
	if store.SumMap == nil {
		store.SumMap = make(map[GUTAKey]*summary.SummaryWithTimes)
	}

	s, ok := store.SumMap[key]
	if !ok {
		s = newSummaryWithTimes()
		store.SumMap[key] = s
	}

	return s
}

func (store *GUTAStore) promoteInlineTuple() {
	if store.inlineTuple == nil {
		return
	}

	store.ensureTupleSummaryMap()
	store.inlineTuple.clearAgeCache()
	store.tupleSummaries[store.inlineTuple.key] = store.inlineTuple
	store.inlineTuple = nil
}

func (store *GUTAStore) tupleForAdd(tupleKey gutaTupleKey) *gutaInlineTuple {
	if store.inlineTuple == nil && len(store.tupleSummaries) == 0 {
		store.inlineTuple = newGUTAInlineTuple(tupleKey)

		return store.inlineTuple
	}

	if store.inlineTuple != nil {
		if store.inlineTuple.key == tupleKey {
			return store.inlineTuple
		}

		store.promoteInlineTuple()
	}

	tuple := store.tupleSummaries[tupleKey]
	if tuple == nil {
		tuple = newGUTAInlineTuple(tupleKey)
		store.tupleSummaries[tupleKey] = tuple
	}

	return tuple
}

func (store *GUTAStore) takeSummary(key GUTAKey, childSummary *summary.SummaryWithTimes) {
	if childSummary == nil {
		return
	}

	if store.SumMap != nil {
		existing := store.SumMap[key]
		if existing == nil {
			store.SumMap[key] = childSummary

			return
		}

		existing.AddSummary(childSummary)
		recycleSummaryWithTimes(childSummary)

		return
	}

	store.tupleForAdd(tupleKeyForGUTA(key)).takeSummary(key.Age, childSummary)
}

func (store *GUTAStore) takeInlineTuple(child *gutaInlineTuple) {
	if child == nil {
		return
	}

	if store.SumMap != nil {
		child.drainExpanded(func(key GUTAKey, sum *summary.SummaryWithTimes) {
			store.takeSummary(key, sum)
		})

		return
	}

	if store.moveInlineTuple(child) {
		return
	}

	if store.mergeInlineTuple(child) {
		return
	}

	store.takeMappedTuple(child)
}

func (store *GUTAStore) moveInlineTuple(child *gutaInlineTuple) bool {
	if store.inlineTuple != nil || len(store.tupleSummaries) != 0 {
		return false
	}

	child.clearAgeCache()
	store.inlineTuple = child

	return true
}

func (store *GUTAStore) mergeInlineTuple(child *gutaInlineTuple) bool {
	if store.inlineTuple == nil {
		return false
	}

	if store.inlineTuple.key != child.key {
		store.promoteInlineTuple()

		return false
	}

	store.inlineTuple.merge(child)

	return true
}

func (store *GUTAStore) takeMappedTuple(child *gutaInlineTuple) {
	store.ensureTupleSummaryMap()

	if existing := store.tupleSummaries[child.key]; existing != nil {
		existing.merge(child)

		return
	}

	child.clearAgeCache()
	store.tupleSummaries[child.key] = child
}

func (store *GUTAStore) ensureTupleSummaryMap() {
	if store.tupleSummaries == nil {
		store.tupleSummaries = make(map[gutaTupleKey]*gutaInlineTuple, initialTupleSummariesSize)
	}
}

// AddForEach adds one file summary for each supplied key.
func (store *GUTAStore) AddForEach(gutaKeys []GUTAKey, size int64, atime int64, mtime int64) {
	tupleKey, ageMask, ok := store.inlineMaskForKeys(gutaKeys, atime, mtime)
	if ok {
		store.addMask(tupleKey, ageMask, size, atime, mtime)

		return
	}

	for _, agutaKey := range gutaKeys {
		store.Add(agutaKey, size, atime, mtime)
	}
}

func (store *GUTAStore) inlineMaskForKeys(
	gutaKeys []GUTAKey,
	atime int64,
	mtime int64,
) (gutaTupleKey, uint32, bool) {
	if len(gutaKeys) == 0 || store.SumMap != nil {
		return gutaTupleKey{}, 0, false
	}

	tupleKey := tupleKeyForGUTA(gutaKeys[0])
	ageMask := uint32(0)

	for _, key := range gutaKeys {
		if tupleKeyForGUTA(key) != tupleKey {
			return gutaTupleKey{}, 0, false
		}

		idx, ok := gutaAgeIndex(key.Age)
		if !ok {
			return gutaTupleKey{}, 0, false
		}

		if key.Age.FitsAgeInterval(atime, mtime, store.RefTime) {
			ageMask |= uint32(1) << idx
		}
	}

	return tupleKey, ageMask, true
}

// DrainInto moves every summary from the child store into parent.
func (store *GUTAStore) DrainInto(parent *GUTAStore) {
	if store.inlineTuple != nil {
		parent.takeInlineTuple(store.inlineTuple)
		store.inlineTuple = nil
	}

	for key, tuple := range store.tupleSummaries {
		parent.takeInlineTuple(tuple)
		delete(store.tupleSummaries, key)
	}

	store.tupleSummaries = nil

	for key, childSummary := range store.SumMap {
		parent.takeSummary(key, childSummary)
		delete(store.SumMap, key)
	}

	store.SumMap = nil
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
	if store.inlineTuple != nil {
		store.inlineTuple.recycle()
		store.inlineTuple = nil
	}

	for key, tuple := range store.tupleSummaries {
		tuple.recycle()
		delete(store.tupleSummaries, key)
	}

	store.tupleSummaries = nil

	for _, sum := range store.SumMap {
		recycleSummaryWithTimes(sum)
	}

	store.SumMap = nil
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

// TrackHardlink finalises hardlink metadata in an inode map without updating aggregates.
func TrackHardlink(
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
		seenHardlinks[info.Inode] = &InodeEntry{
			fileType: ft,
			size:     info.Size,
			atime:    atime,
			mtime:    info.MTime,
			gid:      info.GID,
			uid:      info.UID,
		}

		return true
	}

	entry.fileType |= ft
	entry.size = max(entry.size, info.Size)
	entry.atime = min(entry.atime, atime)
	entry.mtime = max(entry.mtime, info.MTime)

	return true
}

// MergeHardlinks merges finalised child inode entries into a parent inode map.
func MergeHardlinks(parentSeen map[int64]*InodeEntry, childSeen map[int64]*InodeEntry) {
	for inode, childEntry := range childSeen {
		parentEntry, exists := parentSeen[inode]
		if !exists {
			parentSeen[inode] = childEntry

			continue
		}

		parentEntry.fileType |= childEntry.fileType
		parentEntry.size = max(parentEntry.size, childEntry.size)
		parentEntry.atime = min(parentEntry.atime, childEntry.atime)
		parentEntry.mtime = max(parentEntry.mtime, childEntry.mtime)
	}
}

type inodeEntry = InodeEntry
