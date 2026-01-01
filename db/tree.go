/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package db

import (
	"sort"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// DirSummary holds nested file count, size, atime and mtime information on a
// directory. It also holds which users and groups own files nested under the
// directory, what the file types are, and the age group.
type DirSummary struct {
	Dir         string
	Count       uint64
	Size        uint64
	Atime       time.Time
	CommonATime summary.AgeRange
	Mtime       time.Time
	CommonMTime summary.AgeRange
	UIDs        []uint32
	GIDs        []uint32
	FT          DirGUTAFileType
	Age         DirGUTAge
	Modtime     time.Time
}

// DCSs is a Size-sortable slice of DirSummary.
type DCSs []*DirSummary

func (d DCSs) Len() int {
	return len(d)
}

func (d DCSs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d DCSs) Less(i, j int) bool {
	if d[i].Size == d[j].Size {
		return d[i].Dir < d[j].Dir
	}

	return d[i].Size > d[j].Size
}

// SortByDirAndAge sorts by Dir first then Age instead of Size.
func (d DCSs) SortByDirAndAge() {
	sort.Slice(d, func(i, j int) bool {
		if d[i].Dir != d[j].Dir {
			return d[i].Dir < d[j].Dir
		}

		return d[i].Age < d[j].Age
	})
}

// DirInfo holds nested file count, size, UID and GID information on a
// directory, and also its immediate child directories.
type DirInfo struct {
	Current  *DirSummary
	Children []*DirSummary
}

// IsSameAsChild tells you if this DirInfo has only 1 child, and the child
// has the same file count. Ie. our child contains the same files as us.
func (d *DirInfo) IsSameAsChild() bool {
	return len(d.Children) == 1 && d.Children[0].Count == d.Current.Count
}

// Tree is used to do high-level queries on DB.Store() database files.
type Tree struct {
	db *DB
}

// NewTree, given the paths to one or more dguta database files (as created by
// DB.Store()), returns a *Tree that can be used to do high-level queries on the
// stats of a tree of disk folders. You should Close() the tree after use.
func NewTree(paths ...string) (*Tree, error) {
	db := NewDB(paths...)

	if err := db.Open(); err != nil {
		return nil, err
	}

	return &Tree{db: db}, nil
}

// OpenFrom create a new Tree, potentially removing some database sources and
// adding others.
//
// Parameters:
//   - add:    New underlying database file paths to include.
//   - remove: Existing database paths that should be excluded.
func (t *Tree) OpenFrom(add, remove []string) (*Tree, error) {
	db, err := t.db.OpenFrom(add, remove)
	if err != nil {
		return nil, err
	}

	return &Tree{db}, nil
}

// CloseOnly closes the databases corresponding to the given paths.
//
// Should only be used after calling OpenFrom to close the unused databases in
// the old Tree.
func (t *Tree) CloseOnly(paths []string) error {
	return t.db.CloseOnly(paths)
}

// DirInfo tells you the total number of files and their total size nested under
// the given directory, along with the UIDs and GIDs that own those files.
// See GUTAs.Summary for an explanation of the filter.
//
// It also tells you the same information about the immediate child directories
// of the given directory (if the children have files in them that pass the
// filter).
//
// Returns an error if dir doesn't exist.
func (t *Tree) DirInfo(dir string, filter *Filter) (*DirInfo, error) {
	dcs, err := t.getSummaryInfo(dir, filter)
	if err != nil {
		return nil, err
	}

	if dcs == nil {
		return nil, nil //nolint:nilnil
	}

	di := &DirInfo{
		Current: dcs,
	}

	children := t.db.Children(di.Current.Dir)
	err = t.addChildInfo(di, children, filter)

	return di, err
}

// DirHasChildren tells you if the given directory has any child directories
// with files in them that pass the filter. See GUTAs.Summary for an explanation
// of the filter.
func (t *Tree) DirHasChildren(dir string, filter *Filter) bool {
	children := t.db.Children(dir)

	for _, child := range children {
		ds, _ := t.getSummaryInfo(child, filter) //nolint:errcheck
		if ds == nil {
			continue
		}

		if ds.Count > 0 {
			return true
		}
	}

	return false
}

// getSummaryInfo accesses the database to retrieve the count, size and atime
// info for a given directory and filter, along with the UIDs and GIDs that own
// those files, the file types of those files.
func (t *Tree) getSummaryInfo(dir string, filter *Filter) (*DirSummary, error) {
	ds, err := t.db.DirInfo(dir, filter)
	if ds != nil {
		ds.Dir = dir
	}

	return ds, err
}

// addChildInfo adds DirSummary info of the given child paths to the di's
// Children. If a child dir has no files in it, it is ignored.
func (t *Tree) addChildInfo(di *DirInfo, children []string, filter *Filter) error {
	for _, child := range children {
		dcs, errc := t.getSummaryInfo(child, filter)
		if errc != nil {
			return errc
		}

		if dcs == nil {
			continue
		}

		if dcs.Count > 0 {
			di.Children = append(di.Children, dcs)
		}
	}

	return nil
}

// Where tells you where files are nested under dir that pass the filter. With a
// depth of 0 it only returns the single deepest directory that has all passing
// files nested under it.
//
// The recurseCount function returns a path dependent depth value.
//
// With a depth of 1, it also returns the results that calling Where() with a
// depth of 0 on each of the deepest directory's children would give. And so on
// recursively for higher depths.
//
// See GUTAs.Summary for an explanation of the filter.
//
// It's recommended to set the Age filter to summary.DGUTAgeAll.
//
// For example, if all user 354's files are in the directories /a/b/c/d (2
// files), /a/b/c/d/1 (1 files), /a/b/c/d/2 (2 files) and /a/b/e/f/g (2 files),
// Where("/", &Filter{UIDs: []uint32{354}}, 0) would tell you that "/a/b" has 7
// files. With a depth of 1 it would tell you that "/a/b" has 7 files,
// "/a/b/c/d" has 5 files and "/a/b/e/f/g" has 2 files. With a depth of 2 it
// would tell you that "/a/b" has 7 files, "/a/b/c/d" has 5 files, "/a/b/c/d/1"
// has 1 file, "/a/b/c/d/2" has 2 files, and "/a/b/e/f/g" has 2 files.
//
// The returned DirSummarys are sorted by Size, largest first.
//
// Returns an error if dir doesn't exist.
func (t *Tree) Where(dir string, filter *Filter, recurseCount split.SplitFn) (DCSs, error) {
	if filter == nil {
		filter = new(Filter)
	}

	if filter.FT == 0 {
		filter.FT = AllTypesExceptDirectories
	}

	dcss, err := t.recurseWhere(dir, filter, recurseCount, 0)
	if err != nil {
		return nil, err
	}

	sort.Sort(dcss)

	return dcss, nil
}

func (t *Tree) recurseWhere(dir string, filter *Filter, recurseCount func(string) int, step int) (DCSs, error) {
	di, err := t.where0(dir, filter)
	if err != nil {
		return nil, err
	}

	if di == nil {
		return nil, nil
	}

	dcss := DCSs{di.Current}

	if recurseCount(dir) > step {
		for _, dcs := range di.Children {
			d, err := t.recurseWhere(dcs.Dir, filter, recurseCount, step+1)
			if err != nil {
				return nil, err
			}

			dcss = append(dcss, d...)
		}
	}

	return dcss, nil
}

// where0 is the implementation of Where() for a depth of 0.
func (t *Tree) where0(dir string, filter *Filter) (*DirInfo, error) {
	di, err := t.DirInfo(dir, filter)
	if err != nil {
		return nil, err
	}

	if di == nil {
		return nil, nil //nolint:nilnil
	}

	for di.IsSameAsChild() {
		// DirInfo can't return an error here, because we're supplying it a
		// directory name that came from the database.
		//nolint:errcheck
		di, _ = t.DirInfo(di.Children[0].Dir, filter)
	}

	return di, nil
}

// FileLocations, starting from the given dir, finds the first directory that
// directly contains filter-passing files along every branch from dir.
//
// See GUTAs.Summary for an explanation of the filter.
//
// The results are returned sorted by directory.
func (t *Tree) FileLocations(dir string, filter *Filter) (DCSs, error) {
	var dcss DCSs

	di, err := t.DirInfo(dir, filter)
	if err != nil {
		return nil, err
	}

	var childCount uint64

	for _, child := range di.Children {
		childCount += child.Count
	}

	if childCount < di.Current.Count {
		dcss = append(dcss, di.Current)

		return dcss, nil
	}

	for _, child := range di.Children {
		// FileLocations can't return an error here, because we're supplying it
		// a directory name that came from the database.
		//nolint:errcheck
		childDCSs, _ := t.FileLocations(child.Dir, filter)
		dcss = append(dcss, childDCSs...)
	}

	dcss.SortByDirAndAge()

	return dcss, nil
}

// Close should be called after you've finished querying the tree to release its
// database locks.
func (t *Tree) Close() {
	if t.db != nil {
		t.db.Close()
	}
}
