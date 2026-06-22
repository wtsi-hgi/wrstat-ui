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
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const parentSentinel = summary.ParentSentinel

// ErrTooManyDirs is returned when the next directory id would collide with the
// reserved parent sentinel value.
var ErrTooManyDirs = summary.ErrTooManyDirs

// ErrNonContiguousInput is returned when a directory boundary is re-entered
// after its subtree has already been closed.
var ErrNonContiguousInput = summary.ErrNonContiguousInput

// Error is a custom error type.
type Error string

// Error implements the error interface.
func (e Error) Error() string { return string(e) }

func newDirGroupUserTypeAge(
	d DB,
	refTime int64,
	now int64,
	alloc ...*summary.DirIDAllocator,
) summary.OperationGenerator {
	var last *DirGroupUserTypeAge

	idAssigner := newDirIDAssigner()
	idAllocator := optionalDirIDAllocator(alloc)

	return func() summary.Operation {
		last = &DirGroupUserTypeAge{
			parent:        last,
			db:            d,
			idAssigner:    idAssigner,
			idAllocator:   idAllocator,
			store:         NewGUTAStore(refTime),
			now:           now,
			seenHardlinks: make(map[int64]*inodeEntry),
		}

		return last
	}
}

func newDirIDAssigner() *dirIDAssigner {
	return new(dirIDAssigner)
}

func optionalDirIDAllocator(alloc []*summary.DirIDAllocator) *summary.DirIDAllocator {
	if len(alloc) == 0 {
		return nil
	}

	return alloc[0]
}

func sameDirectoryPath(a *summary.DirectoryPath, b *summary.DirectoryPath) bool {
	return !a.Less(b) && !b.Less(a)
}

// DB contains the method that will be called for each directories DGUTA
// information.
type DB interface {
	Add(dguta db.RecordDGUTA) error
}

// NewDirGroupUserTypeAge returns a DirGroupUserTypeAge.
func NewDirGroupUserTypeAge(db DB, alloc ...*summary.DirIDAllocator) summary.OperationGenerator {
	refTime := time.Now().Unix()

	return newDirGroupUserTypeAge(db, refTime, time.Now().Unix(), alloc...)
}

// NewDirGroupUserTypeAgeAt returns a DirGroupUserTypeAge using referenceTime
// for age buckets and directory access times.
func NewDirGroupUserTypeAgeAt(
	db DB,
	referenceTime time.Time,
	alloc ...*summary.DirIDAllocator,
) summary.OperationGenerator {
	refTime := referenceTime.Unix()

	return newDirGroupUserTypeAge(db, refTime, refTime, alloc...)
}

type dirIDAssigner struct {
	next        uint32
	closedFloor *summary.DirectoryPath
	reserved    bool
}

func (a *dirIDAssigner) assign(dir *summary.DirectoryPath) (uint32, error) {
	if a.closedFloor != nil && (!a.closedFloor.Less(dir) || isAncestorOrSelf(a.closedFloor, dir)) {
		return 0, ErrNonContiguousInput
	}

	if !a.reserved && a.next == 0 {
		return a.assignDataRoot(dir)
	}

	if a.next == parentSentinel {
		return 0, ErrTooManyDirs
	}

	a.reserved = true

	id := a.next
	a.next++

	return id, nil
}

func isAncestorOrSelf(ancestor *summary.DirectoryPath, dir *summary.DirectoryPath) bool {
	if ancestor.Depth > dir.Depth {
		return false
	}

	for dir.Depth > ancestor.Depth {
		dir = dir.Parent
	}

	return sameDirectoryPath(ancestor, dir)
}

func (a *dirIDAssigner) assignDataRoot(dir *summary.DirectoryPath) (uint32, error) {
	id, err := reservedDirIDForDepth(dir.Depth)
	if err != nil {
		return 0, err
	}

	a.reserved = true
	a.next = id + 1

	return id, nil
}

func reservedDirIDForDepth(depth int) (uint32, error) {
	if depth < 0 || uint64(depth) >= uint64(parentSentinel) {
		return 0, ErrTooManyDirs
	}

	return uint32(depth), nil
}

func (a *dirIDAssigner) close(dir *summary.DirectoryPath) {
	if a.closedFloor == nil || a.closedFloor.Less(dir) {
		a.closedFloor = dir
	}
}

// DirGroupUserTypeAge is used to summarise file stats by directory, group,
// user, file type and age.
type DirGroupUserTypeAge struct {
	parent         *DirGroupUserTypeAge
	db             DB
	idAssigner     *dirIDAssigner
	idAllocator    *summary.DirIDAllocator
	store          gutaStore
	thisDir        *summary.DirectoryPath
	children       []string
	childCount     uint64
	childFileCount uint64
	now            int64
	isTempDir      bool
	seenHardlinks  map[int64]*inodeEntry
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	idAssigned     bool
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
func (d *DirGroupUserTypeAge) Add(info *summary.FileInfo) error { //nolint:funlen,gocyclo,cyclop
	if d.thisDir == nil {
		d.thisDir = info.Path
		d.isTempDir = d.parent != nil && d.parent.isTempDir || IsTemp(info.Name)

		if err := d.assignDirectoryID(info.Path); err != nil {
			return err
		}
	}

	if info.IsDir() && info.Path != nil && info.Path.Parent == d.thisDir {
		if err := d.addChildName(string(info.Name)); err != nil {
			return err
		}
	}

	if info.Path != d.thisDir {
		return nil
	}

	if !info.IsDir() {
		d.addChildFile()
	}

	ft := FileTypeWithTemp(info.Name, d.isTempDir)

	atime := info.ATime
	if info.IsDir() {
		atime = d.now
	}

	if HandleHardlink(&d.store, d.seenHardlinks, info, ft, atime) {
		return nil
	}

	gutaKeysA := GUTAKeyPool.Get().(*[MaxNumOfGUTAKeys]GUTAKey) //nolint:errcheck,forcetypeassert
	gKeys := GUTAKeys(gutaKeysA[:0])

	gKeys.Append(info.GID, info.UID, ft)

	d.store.AddForEach(gKeys, info.Size, atime, max(0, info.MTime))
	GUTAKeyPool.Put(gutaKeysA)

	return nil
}

func (d *DirGroupUserTypeAge) assignDirectoryID(dir *summary.DirectoryPath) error {
	if d.idAllocator != nil {
		return d.assignDirectoryIDWithAllocator(dir)
	}

	d.ensureIDAssigner()

	parentID, err := d.parentDirID(dir)
	if err != nil {
		return err
	}

	dirID, err := d.idAssigner.assign(dir)
	if err != nil {
		return err
	}

	d.dirID = dirID
	d.parentID = parentID
	d.depth = uint16(dir.Depth) //nolint:gosec // directory depth is bounded by summariser traversal.
	d.idAssigned = true

	return nil
}

func (d *DirGroupUserTypeAge) assignDirectoryIDWithAllocator(dir *summary.DirectoryPath) error {
	parentID, err := d.parentDirID(dir)
	if err != nil {
		return err
	}

	dirID, err := d.idAllocator.Enter(dir)
	if err != nil {
		return err
	}

	d.dirID = dirID
	d.parentID = parentID
	d.depth = uint16(dir.Depth) //nolint:gosec // directory depth is bounded by summariser traversal.
	d.idAssigned = true

	return nil
}

func (d *DirGroupUserTypeAge) parentDirID(dir *summary.DirectoryPath) (uint32, error) {
	if d.parent == nil {
		return reservedParentIDForDepth(dir.Depth)
	}

	if d.parent.thisDir != dir.Parent || !d.parent.idAssigned {
		return 0, ErrNonContiguousInput
	}

	return d.parent.dirID, nil
}

func (d *DirGroupUserTypeAge) ensureIDAssigner() {
	if d.idAssigner != nil {
		return
	}

	if d.parent != nil && d.parent.idAssigner != nil {
		d.idAssigner = d.parent.idAssigner

		return
	}

	d.idAssigner = newDirIDAssigner()
}

func (d *DirGroupUserTypeAge) addChildName(child string) error {
	d.childCount++
	d.children = append(d.children, child)

	return nil
}

func (d *DirGroupUserTypeAge) addChildFile() {
	d.childFileCount++
}

// handleHardlink checks if a file is a hardlink that has been seen before.
// If it is a new inode, it adds it to the seenHardlinks map and updates the store.
// If it is an existing inode, it adjusts counts and sizes to avoid double-counting,
// merging file types and updating atime and mtime as needed. Returns true if the
// file was handled as a hardlink, false otherwise.
func (d *DirGroupUserTypeAge) handleHardlink(info *summary.FileInfo, ft db.DirGUTAFileType, atime int64) bool {
	return HandleHardlink(&d.store, d.seenHardlinks, info, ft, atime)
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
	dgutas := d.store.Sort()

	if err := d.finishDirectoryID(); err != nil {
		return err
	}

	dguta := db.RecordDGUTA{
		Dir:            d.thisDir,
		DirID:          d.dirID,
		ParentID:       d.parentID,
		SubtreeEnd:     d.subtreeEnd,
		Depth:          d.depth,
		Children:       d.children,
		ChildCount:     d.childCount,
		ChildFileCount: d.childFileCount,
	}

	for _, guta := range dgutas {
		dguta.GUTAs = append(dguta.GUTAs, GetGUTA(d.store, guta))
	}

	if err := d.db.Add(dguta); err != nil {
		return err
	}

	if d.parent == nil { //nolint:nestif
		if err := d.outputRoot(); err != nil {
			return err
		}
	} else {
		d.parent.addChild(&d.store, d.seenHardlinks)
	}

	d.clear()

	return nil
}

func (d *DirGroupUserTypeAge) finishDirectoryID() error {
	if !d.idAssigned {
		return nil
	}

	if d.idAllocator != nil {
		subtreeEnd, err := d.idAllocator.Leave(d.thisDir)
		if err != nil {
			return err
		}

		d.subtreeEnd = subtreeEnd

		return nil
	}

	d.subtreeEnd = d.idAssigner.next
	d.idAssigner.close(d.thisDir)

	return nil
}

// addChild merges a child directory's store and seen inodes into this DirGroupUserTypeAge.
func (d *DirGroupUserTypeAge) addChild(child *gutaStore, childSeen map[int64]*inodeEntry) {
	MergeSeenHardlinks(&d.store, d.seenHardlinks, child, childSeen)
	child.DrainInto(&d.store)
}

func (d *DirGroupUserTypeAge) outputRoot() error {
	for thisDir := d.thisDir; thisDir.Parent != nil; thisDir = thisDir.Parent {
		ancestor := thisDir.Parent

		dirID, err := reservedDirIDForDepth(ancestor.Depth)
		if err != nil {
			return err
		}

		parentID, err := reservedParentIDForDepth(ancestor.Depth)
		if err != nil {
			return err
		}

		dguta := db.RecordDGUTA{
			Dir:        ancestor,
			DirID:      dirID,
			ParentID:   parentID,
			SubtreeEnd: d.nextDirID(),
			Depth:      uint16(ancestor.Depth), //nolint:gosec // directory depth is bounded by summariser traversal.
			Children:   []string{thisDir.Name},
			ChildCount: 1,
		}

		if err := d.db.Add(dguta); err != nil {
			return err
		}
	}

	return nil
}

func reservedParentIDForDepth(depth int) (uint32, error) {
	if depth == 0 {
		return parentSentinel, nil
	}

	return reservedDirIDForDepth(depth - 1)
}

func (d *DirGroupUserTypeAge) nextDirID() uint32 {
	if d.idAllocator != nil {
		return d.idAllocator.Next()
	}

	return d.idAssigner.next
}

func (d *DirGroupUserTypeAge) clear() {
	d.store.Clear()
	clear(d.seenHardlinks)

	d.thisDir = nil
	d.children = nil
	d.childCount = 0
	d.childFileCount = 0
	d.dirID = 0
	d.parentID = 0
	d.subtreeEnd = 0
	d.depth = 0
	d.idAssigned = false
}
