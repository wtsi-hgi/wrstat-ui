/*******************************************************************************
 * Copyright (c) 2022, 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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
	"time"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// Error is a custom error type.
type Error string

// Error implements the error interface.
func (e Error) Error() string { return string(e) }

const (
	maxNumOfGUTAKeys = 34
	lengthOfGUTAKey  = 12
)

var gutaKeyPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return new([maxNumOfGUTAKeys]gutaKey)
	},
}

// gutaStore is a sortable map with gid,uid,filetype,age as keys and
// summaryWithAtime as values.
type gutaStore struct {
	sumMap  map[gutaKey]*summary.SummaryWithTimes
	refTime int64
}

// add will auto-vivify a summary for the given key (which should have been
// generated with statToGUTAKey()) and call add(size, atime, mtime) on it.
func (store gutaStore) add(gkey gutaKey, size int64, atime int64, mtime int64) {
	if !gkey.Age.FitsAgeInterval(atime, mtime, store.refTime) {
		return
	}

	s, ok := store.sumMap[gkey]
	if !ok {
		s = new(summary.SummaryWithTimes)
		store.sumMap[gkey] = s
	}

	s.Add(size, atime, mtime)
}

// sort returns a slice of our summaryWithAtime values, sorted by our dguta keys
// which are also returned.
func (store gutaStore) sort() gutaKeys {
	keys := gutaKeys(slices.Collect(maps.Keys(store.sumMap)))

	sort.Sort(keys)

	return keys
}

// DB contains the method that will be called for each directories DGUTA
// information.
type DB interface {
	Add(dguta db.RecordDGUTA) error
}

// DirGroupUserTypeAge is used to summarise file stats by directory, group,
// user, file type and age.
type DirGroupUserTypeAge struct {
	parent    *DirGroupUserTypeAge
	db        DB
	store     gutaStore
	thisDir   *summary.DirectoryPath
	children  []string
	now       int64
	isTempDir bool
}

// NewDirGroupUserTypeAge returns a DirGroupUserTypeAge.
func NewDirGroupUserTypeAge(db DB) summary.OperationGenerator {
	return newDirGroupUserTypeAge(db, time.Now().Unix())
}

func newDirGroupUserTypeAge(db DB, refTime int64) summary.OperationGenerator {
	now := time.Now().Unix()

	var last *DirGroupUserTypeAge

	return func() summary.Operation {
		last = &DirGroupUserTypeAge{
			parent: last,
			db:     db,
			store:  gutaStore{make(map[gutaKey]*summary.SummaryWithTimes), refTime},
			now:    now,
		}

		return last
	}
}

// Add is a summary.Operation method. It will break path in to its directories
// and add the file size, increment the file count to each, summed for the
// info's group, user, filetype and age. It will also record the oldest file
// access time for each directory, plus the newest modification time.
//
// If path is a directory, its access time is treated as now, so that when
// interested in files that haven't been accessed in a long time, directories
// that haven't been manually visted in a longer time don't hide the "real"
// results.
//
// NB: the "temp" filetype is an extra filetype on top of the other normal
// filetypes, so if you sum all the filetypes to get information about a given
// directory+group+user combination, you should ignore "temp". Only count "temp"
// when it's the only type you're considering, or you'll count some files twice.
func (d *DirGroupUserTypeAge) Add(info *summary.FileInfo) error {
	if d.thisDir == nil {
		d.thisDir = info.Path

		if d.parent != nil && d.parent.isTempDir {
			d.isTempDir = true
		} else {
			d.isTempDir = IsTemp(info.Name)
		}
	}

	if info.IsDir() && info.Path != nil && info.Path.Parent == d.thisDir {
		d.children = append(d.children, string(info.Name))
	}

	if info.Path != d.thisDir {
		return nil
	}

	atime := info.ATime

	if info.IsDir() {
		atime = d.now
	}

	gutaKeysA := gutaKeyPool.Get().(*[maxNumOfGUTAKeys]gutaKey) //nolint:errcheck,forcetypeassert
	gKeys := gutaKeys(gutaKeysA[:0])

	filetype := FilenameToType(info.Name)
	isTmp := d.isTempDir || IsTemp(info.Name)

	gKeys.append(info.GID, info.UID, filetype)

	if isTmp {
		gKeys.append(info.GID, info.UID, db.DGUTAFileTypeTemp)
	}

	d.addForEach(gKeys, info.Size, atime, maxInt(0, info.MTime))

	gutaKeyPool.Put(gutaKeysA)

	return nil
}

type gutaKey struct {
	GID, UID uint32
	FileType db.DirGUTAFileType
	Age      db.DirGUTAge
}

type gutaKeys []gutaKey

func (g gutaKeys) Len() int {
	return len(g)
}

func (g gutaKeys) Less(i, j int) bool {
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

func (g gutaKeys) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func (g gutaKey) String() string {
	var a [lengthOfGUTAKey]byte

	binary.BigEndian.PutUint32(a[:4], g.GID)
	binary.BigEndian.PutUint32(a[4:8], g.UID)
	a[8] = uint8(g.FileType)
	a[9] = uint8(g.Age)

	return unsafe.String(&a[0], len(a))
}

// appendGUTAKeys appends gutaKeys with keys including the given gid, uid, file
// type and age.
func (g *gutaKeys) append(gid, uid uint32, fileType db.DirGUTAFileType) {
	for _, age := range db.DirGUTAges {
		*g = append(*g, gutaKey{gid, uid, fileType, age})
	}
}

// maxInt returns the greatest of the inputs.
func maxInt(ints ...int64) int64 {
	var maxInt int64

	for _, i := range ints {
		if i > maxInt {
			maxInt = i
		}
	}

	return maxInt
}

// addForEach breaks path into each directory, gets a gutaStore for each and
// adds a file of the given size to them under the given gutaKeys.
func (d *DirGroupUserTypeAge) addForEach(gutaKeys []gutaKey, size int64, atime int64, mtime int64) {
	for _, agutaKey := range gutaKeys {
		d.store.add(agutaKey, size, atime, mtime)
	}
}

// Output is a summary.Operation method, and will write summary information for
// all the paths previously added. The format is (tab separated):
//
// directory gid uid filetype age filecount filesize atime mtime
//
// Where atime is oldest access time in seconds since Unix epoch of any file
// nested within directory. mtime is similar, but the newest modification time.
//
// age is one of our age ints:
//
//	    0 = all ages
//	    1 = older than one month according to atime
//	    2 = older than two months according to atime
//	    3 = older than six months according to atime
//	    4 = older than one year according to atime
//	    5 = older than two years according to atime
//	    6 = older than three years according to atime
//	    7 = older than five years according to atime
//	    8 = older than seven years according to atime
//	    9 = older than one month according to mtime
//	   10 = older than two months according to mtime
//	   11 = older than six months according to mtime
//	   12 = older than one year according to mtime
//	   13 = older than two years according to mtime
//	   14 = older than three years according to mtime
//	15 = older than five years according to mtime
//	   16 = older than seven years according to mtime
//
// directory, gid, uid, filetype and age are sorted. The sort on the columns is
// not numeric, but alphabetical. So gid 10 will come before gid 2.
//
// filetype is one of our filetype ints:
//
//	 0 = other (not any of the others below)
//	 1 = temp (.tmp | temp suffix, or .tmp. | .temp. | tmp. | temp. prefix, or
//	           a directory in its path is named "tmp" or "temp")
//	 2 = vcf
//	 3 = vcf.gz
//	 4 = bcf
//	 5 = sam
//	 6 = bam
//	 7 = cram
//	 8 = fasta (.fa | .fasta suffix)
//	 9 = fastq (.fq | .fastq suffix)
//	10 = fastq.gz (.fq.gz | .fastq.gz suffix)
//	11 = ped/bed (.ped | .map | .bed | .bim | .fam suffix)
//	12 = compresed (.bzip2 | .gz | .tgz | .zip | .xz | .bgz suffix)
//	13 = text (.csv | .tsv | .txt | .text | .md | .dat | readme suffix)
//	14 = log (.log | .out | .o | .err | .e | .err | .oe suffix)
//
// Returns an error on failure to write.
func (d *DirGroupUserTypeAge) Output() error {
	dgutas := d.store.sort()
	dguta := db.RecordDGUTA{Dir: d.thisDir, Children: d.children}

	for _, guta := range dgutas {
		dguta.GUTAs = append(dguta.GUTAs, d.getGUTA(guta))
	}

	if err := d.db.Add(dguta); err != nil {
		return err
	}

	if d.parent == nil {
		if err := d.outputRoot(dguta); err != nil {
			return err
		}
	} else {
		d.parent.addChild(d.store)
	}

	d.clear()

	return nil
}

func (d *DirGroupUserTypeAge) addChild(child gutaStore) {
	for key, summary := range child.sumMap {
		if existing, ok := d.store.sumMap[key]; ok {
			existing.AddSummary(summary)
		} else {
			d.store.sumMap[key] = summary
		}
	}
}

func (d *DirGroupUserTypeAge) getGUTA(guta gutaKey) *db.GUTA {
	s := d.store.sumMap[guta]

	return &db.GUTA{
		GID:   guta.GID,
		UID:   guta.UID,
		FT:    guta.FileType,
		Age:   guta.Age,
		Count: uint64(s.Count), //nolint:gosec
		Size:  uint64(s.Size),  //nolint:gosec
		Atime: s.Atime,
		Mtime: s.Mtime,
	}
}

func (d *DirGroupUserTypeAge) outputRoot(dguta db.RecordDGUTA) error {
	for thisDir := d.thisDir; thisDir.Parent != nil; thisDir = thisDir.Parent {
		dguta.Dir = thisDir.Parent
		dguta.Children = []string{thisDir.Name}

		if err := d.db.Add(dguta); err != nil {
			return err
		}
	}

	return nil
}

func (d *DirGroupUserTypeAge) clear() {
	for k := range d.store.sumMap {
		delete(d.store.sumMap, k)
	}

	d.thisDir = nil
	d.children = nil
}
