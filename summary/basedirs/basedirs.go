/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const numAges = len(db.DirGUTAges)

type output interface {
	Output(users, groups basedirs.IDAgeDirs) error
}

type outputForDir func(*summary.DirectoryPath) bool

type dirSummary struct {
	Path *summary.DirectoryPath
	basedirs.SummaryWithChildren
}

func newDirSummary(parent *summary.DirectoryPath, age db.DirGUTAge) *dirSummary {
	return &dirSummary{
		Path: parent,
		SummaryWithChildren: basedirs.SummaryWithChildren{
			DirSummary: db.DirSummary{
				Age: age,
			},
			Children: []*basedirs.SubDir{
				{
					FileUsage: make(basedirs.UsageBreakdownByType),
				},
			},
		},
	}
}

func setTimes(d *basedirs.SummaryWithChildren, atime, mtime time.Time) {
	if atime.Before(d.Atime) || d.Atime.IsZero() {
		d.Atime = atime
	}

	if mtime.After(d.Mtime) {
		d.Mtime = mtime
		d.Children[0].LastModified = mtime
	}
}

func (d *dirSummary) Merge(old *dirSummary) {
	p := old.Path

	for p.Depth > d.Path.Depth+1 {
		p = p.Parent
	}

	merge(&d.SummaryWithChildren, &old.SummaryWithChildren, p.Name)
}

func merge(newS, oldS *basedirs.SummaryWithChildren, name string) {
	for n, c := range oldS.Children[0].FileUsage {
		newS.Children[0].FileUsage[n] += c
	}

	for _, uid := range oldS.UIDs {
		newS.UIDs = addToSlice(newS.UIDs, uid)
	}

	for _, gid := range oldS.GIDs {
		newS.GIDs = addToSlice(newS.GIDs, gid)
	}

	setTimes(newS, oldS.Atime, oldS.Mtime)

	newS.Children[0].NumFiles += oldS.Children[0].NumFiles
	newS.Children[0].SizeFiles += oldS.Children[0].SizeFiles
	oldS.Children[0].SubDir = name
	newS.Children = append(newS.Children, oldS.Children[0])
}

type ageBaseDirs [numAges]*dirSummary

func (b *ageBaseDirs) Set(i db.DirGUTAge, fi *summary.FileInfo, parent *summary.DirectoryPath, tmp bool) {
	if b[i] == nil {
		b[i] = newDirSummary(parent, i)
		b[i].Age = i
	} else if b[i].Path != parent {
		old := b[i]
		b[i] = newDirSummary(parent, i)
		b[i].Merge(old)
	}

	b[i].Children[0].NumFiles++
	b[i].Children[0].SizeFiles += uint64(fi.Size) //nolint:gosec

	setTimes(&b[i].SummaryWithChildren, time.Unix(fi.ATime, 0), time.Unix(fi.MTime, 0))

	ft := dirguta.FilenameToType(fi.Name)
	b[i].Children[0].FileUsage[ft] += uint64(fi.Size) //nolint:gosec

	if tmp || dirguta.IsTemp(fi.Name) {
		b[i].Children[0].FileUsage[db.DGUTAFileTypeTemp] += uint64(fi.Size) //nolint:gosec
	}

	b[i].GIDs = addToSlice(b[i].GIDs, fi.GID)
	b[i].UIDs = addToSlice(b[i].UIDs, fi.UID)
}

func addToSlice(s []uint32, id uint32) []uint32 {
	pos, ok := slices.BinarySearch(s, id)
	if ok {
		return s
	}

	return slices.Insert(s, pos, id)
}

type baseDirsMap map[uint32]*ageBaseDirs

func (b baseDirsMap) Get(id uint32) *ageBaseDirs {
	bd, ok := b[id]
	if !ok {
		bd = new(ageBaseDirs)
		b[id] = bd
	}

	return bd
}

func (b baseDirsMap) Add(fn func(uint32, basedirs.SummaryWithChildren, db.DirGUTAge)) { //nolint:gocognit
	for id, bd := range b {
		for age, ds := range bd {
			if ds != nil {
				ds.Dir = string(ds.Path.AppendTo(make([]byte, 0, ds.Path.Len())))

				for n, c := range ds.Children[0].FileUsage {
					if c > 0 {
						ds.FTs = append(ds.FTs, n)
					}
				}

				slices.Sort(ds.FTs)

				ds.Children[0].SubDir = "."
				ds.Children[0].LastModified = ds.Mtime
				ds.Count = ds.Children[0].NumFiles
				ds.Size = ds.Children[0].SizeFiles

				fn(id, ds.SummaryWithChildren, db.DirGUTAges[age])
			}
		}
	}
}

func (b baseDirsMap) mergeTo(pbm baseDirsMap, parent *summary.DirectoryPath) { //nolint:gocognit
	for id, bm := range b {
		pm, ok := pbm[id]
		if !ok {
			pbm[id] = bm

			delete(b, id)

			continue
		}

		for n, p := range bm {
			if p == nil { //nolint:gocritic,nestif
				continue
			} else if pm[n] == nil {
				pm[n] = p
			} else if pm[n].Path == parent {
				pm[n].Merge(p)
			} else {
				old := pm[n]
				pm[n] = newDirSummary(parent, db.DirGUTAge(n)) //nolint:gosec
				pm[n].Merge(old)
				pm[n].Merge(p)
			}
		}
	}
}

type baseDirs struct {
	root          *rootBaseDirs
	parent        *baseDirs
	output        outputForDir
	refTime       int64
	thisDir       *summary.DirectoryPath
	users, groups baseDirsMap
	isTempDir     bool
}

type rootBaseDirs struct {
	baseDirs
	db            output
	users, groups basedirs.IDAgeDirs
}

// NewBaseDirs returns a summary.OperationGenerator that calculates base
// directory information.
func NewBaseDirs(output outputForDir, db output) summary.OperationGenerator { //nolint:funlen
	root := &rootBaseDirs{
		baseDirs: baseDirs{
			output: output,
			users:  make(baseDirsMap),
			groups: make(baseDirsMap),
		},
		db:     db,
		users:  make(basedirs.IDAgeDirs),
		groups: make(basedirs.IDAgeDirs),
	}

	root.root = root
	now := time.Now().Unix()

	var parent *baseDirs

	return func() summary.Operation {
		if parent == nil {
			parent = &root.baseDirs

			return root
		}

		parent = &baseDirs{
			parent:  parent,
			output:  output,
			root:    root,
			refTime: now,
			users:   make(baseDirsMap),
			groups:  make(baseDirsMap),
		}

		return parent
	}
}

// Add is a summary.Operation method.
func (b *baseDirs) Add(info *summary.FileInfo) error { //nolint:gocyclo
	if b.thisDir == nil {
		b.thisDir = info.Path
		b.isTempDir = b.parent != nil && b.parent.isTempDir ||
			dirguta.IsTemp(unsafe.Slice(unsafe.StringData(info.Path.Name), len(info.Path.Name)))
	}

	if info.Path != b.thisDir || info.IsDir() {
		return nil
	}

	gidBasedir := b.groups.Get(info.GID)
	uidBasedir := b.users.Get(info.UID)

	for _, threshold := range db.DirGUTAges {
		if threshold.FitsAgeInterval(info.ATime, info.MTime, b.refTime) {
			gidBasedir.Set(threshold, info, b.thisDir, b.isTempDir)
			uidBasedir.Set(threshold, info, b.thisDir, b.isTempDir)
		}
	}

	return nil
}

// Output is a summary.Operation method.
func (b *baseDirs) Output() error {
	if b.output(b.thisDir) {
		b.groups.Add(b.root.addGroupBase)
		b.users.Add(b.root.addUserBase)
	} else {
		b.addToParent()
	}

	b.cleanup()

	return nil
}

func (b *baseDirs) addToParent() {
	if b.parent == nil {
		return
	}

	b.groups.mergeTo(b.parent.groups, b.parent.thisDir)
	b.users.mergeTo(b.parent.users, b.parent.thisDir)
}

func (b *baseDirs) cleanup() {
	b.thisDir = nil

	for k := range b.groups {
		delete(b.groups, k)
	}

	for k := range b.users {
		delete(b.users, k)
	}
}

// Output is a summary.Operation method.
func (r *rootBaseDirs) Output() error {
	r.baseDirs.Output() //nolint:errcheck

	removeChildDirFilesFromDot(r.users)
	removeChildDirFilesFromDot(r.groups)

	return r.db.Output(r.users, r.groups)
}

func removeChildDirFilesFromDot(idag basedirs.IDAgeDirs) { //nolint:gocognit,gocyclo
	for _, ad := range idag {
		for _, c := range ad {
			for n := range c {
				swc := &c[n]
				swc.Dir = strings.TrimSuffix(swc.Dir, "/")
				dot := swc.Children[0]

				for _, sd := range swc.Children[1:] {
					sd.SubDir = strings.TrimSuffix(sd.SubDir, "/")
					dot.NumFiles -= sd.NumFiles
					dot.SizeFiles -= sd.SizeFiles

					for typ, count := range sd.FileUsage {
						dot.FileUsage[typ] -= count
					}
				}

				if dot.NumFiles == 0 { //nolint:nestif
					swc.Children = swc.Children[1:]
				} else {
					for typ, count := range dot.FileUsage {
						if count == 0 {
							delete(dot.FileUsage, typ)
						}
					}
				}
			}
		}
	}
}

func (r *rootBaseDirs) addUserBase(uid uint32, ds basedirs.SummaryWithChildren, age db.DirGUTAge) {
	addIDAgePath(r.users, uid, ds, age)
}

func addIDAgePath(m basedirs.IDAgeDirs, id uint32, ds basedirs.SummaryWithChildren, age db.DirGUTAge) {
	ap := m.Get(id)

	ap[age] = append(slices.DeleteFunc(ap[age], func(p basedirs.SummaryWithChildren) bool {
		if strings.HasPrefix(p.Dir, ds.Dir) {
			pos := strings.LastIndexByte(p.Dir[:len(p.Dir)-1], '/')
			merge(&ds, &p, p.Dir[pos+1:])

			return true
		}

		return false
	}), ds)
}

func (r *rootBaseDirs) addGroupBase(uid uint32, ds basedirs.SummaryWithChildren, age db.DirGUTAge) {
	addIDAgePath(r.groups, uid, ds, age)
}
