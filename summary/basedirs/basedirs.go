package basedirs

import (
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const numAges = len(db.DirGUTAges)

type DB interface {
	Output(users, groups basedirs.IDAgeDirs) error
}

type outputForDir func(*summary.DirectoryPath) bool

type DirSummary struct {
	Path *summary.DirectoryPath
	basedirs.SummaryWithChildren
}

func newDirSummary(parent *summary.DirectoryPath) *DirSummary {
	return &DirSummary{
		Path: parent,
		SummaryWithChildren: basedirs.SummaryWithChildren{
			Children: []*basedirs.SubDir{
				{
					FileUsage: make(basedirs.UsageBreakdownByType),
				},
			},
		},
	}
}

func setTimes(d *basedirs.SummaryWithChildren, atime, mtime time.Time) {
	if atime.Before(d.Atime) {
		d.Atime = atime
	}

	if mtime.After(d.Mtime) {
		d.Mtime = mtime
	}
}

func (d *DirSummary) Merge(old *DirSummary) {
	p := old.Path

	for p.Depth > d.Path.Depth+1 {
		p = p.Parent
	}

	merge(&d.SummaryWithChildren, &old.SummaryWithChildren, p.Name)
}

func merge(new, old *basedirs.SummaryWithChildren, name string) {
	for n, c := range old.Children[0].FileUsage {
		new.Children[0].FileUsage[n] += c
	}

	setTimes(new, old.Atime, old.Mtime)

	new.Children[0].NumFiles += old.Children[0].NumFiles
	new.Children[0].SizeFiles += old.Children[0].SizeFiles
	old.Children[0].SubDir = name
	new.Children = append(new.Children, old.Children[0])
}

type baseDirs [numAges]*DirSummary

func (b *baseDirs) Set(i int, fi *summary.FileInfo, parent *summary.DirectoryPath) {
	if b[i] == nil {
		b[i] = newDirSummary(parent)
	} else if b[i].Path != parent {
		old := b[i]
		b[i] = newDirSummary(parent)
		b[i].Merge(old)
	}

	b[i].Children[0].NumFiles++
	b[i].Children[0].SizeFiles += uint64(fi.Size)

	setTimes(&b[i].SummaryWithChildren, time.Unix(fi.ATime, 0), time.Unix(fi.MTime, 0))

	t, tmp := dirguta.InfoToType(fi)

	b[i].Children[0].FileUsage[t]++

	if tmp {
		b[i].Children[0].FileUsage[db.DGUTAFileTypeTemp]++
	}

	if !slices.Contains(b[i].GIDs, fi.GID) {
		b[i].GIDs = append(b[i].GIDs, fi.GID)
	}

	if !slices.Contains(b[i].UIDs, fi.UID) {
		b[i].UIDs = append(b[i].UIDs, fi.UID)
	}
}

type baseDirsMap map[uint32]*baseDirs

func (b baseDirsMap) Get(id uint32) *baseDirs {
	bd, ok := b[id]
	if !ok {
		bd = new(baseDirs)
		b[id] = bd
	}

	return bd
}

func (b baseDirsMap) Add(fn func(uint32, basedirs.SummaryWithChildren, db.DirGUTAge)) error {
	for id, bd := range b {
		for age, ds := range bd {
			if ds != nil {
				ds.Dir = string(ds.Path.AppendTo(make([]byte, 0, ds.Path.Len())))

				for n, c := range ds.Children[0].FileUsage {
					if c > 0 {
						ds.FTs = append(ds.FTs, db.DirGUTAFileType(n))
					}
				}

				ds.Children[0].SubDir = "."
				ds.Children[0].LastModified = ds.Mtime
				ds.Count = ds.Children[0].NumFiles
				ds.Size = ds.Children[0].SizeFiles

				fn(id, ds.SummaryWithChildren, db.DirGUTAges[age])
			}
		}
	}

	return nil
}

func (b baseDirsMap) mergeTo(pbm baseDirsMap, parent *summary.DirectoryPath) {
	for id, bm := range b {
		pm, ok := pbm[id]
		if !ok {
			pbm[id] = bm

			delete(b, id)

			continue
		}

		for n, p := range bm {
			if p == nil {
				continue
			} else if pm[n] == nil {
				pm[n] = p
			} else if pm[n].Path == parent {
				pm[n].Merge(p)
			} else {
				old := pm[n]
				pm[n] = newDirSummary(parent)
				pm[n].Merge(old)
				pm[n].Merge(p)
			}
		}
	}
}

type BaseDirs struct {
	root          *RootBaseDirs
	parent        *BaseDirs
	output        outputForDir
	refTime       int64
	thisDir       *summary.DirectoryPath
	users, groups baseDirsMap
}

type RootBaseDirs struct {
	BaseDirs
	db            DB
	users, groups basedirs.IDAgeDirs
}

func NewBaseDirs(output outputForDir, db DB) summary.OperationGenerator {
	root := &RootBaseDirs{
		BaseDirs: BaseDirs{
			output: output,
			users:  make(baseDirsMap),
			groups: make(baseDirsMap),
		},
		db:     db,
		users:  make(basedirs.IDAgeDirs),
		groups: make(basedirs.IDAgeDirs),
	}

	root.root = root

	var parent *BaseDirs

	now := time.Now().Unix()

	return func() summary.Operation {
		if parent == nil {
			parent = &root.BaseDirs

			return root
		}

		parent = &BaseDirs{
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

func (b *BaseDirs) Add(info *summary.FileInfo) error {
	if b.thisDir == nil {
		b.thisDir = info.Path
	}

	if info.Path != b.thisDir || info.IsDir() {
		return nil
	}

	gidBasedir := b.groups.Get(info.GID)
	uidBasedir := b.users.Get(info.UID)

	for n, threshold := range db.DirGUTAges {
		if threshold.FitsAgeInterval(info.ATime, info.MTime, b.refTime) {
			gidBasedir.Set(n, info, b.thisDir)
			uidBasedir.Set(n, info, b.thisDir)
		}
	}

	return nil
}

func (b *BaseDirs) Output() error {
	if b.output(b.thisDir) {
		b.groups.Add(b.root.AddGroupBase)
		b.users.Add(b.root.AddUserBase)
	} else {
		b.addToParent()
	}

	b.cleanup()

	return nil
}

func (b *BaseDirs) addToParent() {
	if b.parent == nil {
		return
	}

	b.groups.mergeTo(b.parent.groups, b.parent.thisDir)
	b.users.mergeTo(b.parent.users, b.parent.thisDir)
}

func (b *BaseDirs) cleanup() {
	b.thisDir = nil

	for k := range b.groups {
		delete(b.groups, k)
	}

	for k := range b.users {
		delete(b.users, k)
	}
}

func (r *RootBaseDirs) Output() error {
	r.BaseDirs.Output()

	return r.db.Output(r.users, r.groups)
}

func (r *RootBaseDirs) AddUserBase(uid uint32, ds basedirs.SummaryWithChildren, age db.DirGUTAge) {
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

func (r *RootBaseDirs) AddGroupBase(uid uint32, ds basedirs.SummaryWithChildren, age db.DirGUTAge) {
	addIDAgePath(r.groups, uid, ds, age)
}
