package basedirs

import (
	"sync"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const numAges = len(db.DirGUTAges)

type DB interface {
	AddUserBase(uid uint32, path *summary.DirectoryPath, age db.DirGUTAge) error
	AddGroupBase(uid uint32, path *summary.DirectoryPath, age db.DirGUTAge) error
}

type minDepth func(*summary.DirectoryPath) int

type baseDirs [numAges]*summary.DirectoryPath

var baseDirsPool = sync.Pool{
	New: func() any {
		return new(baseDirs)
	},
}

func (b *baseDirs) Set(i int, new, parent *summary.DirectoryPath) {
	if b[i] != nil {
		new = parent
	}

	b[i] = new
}

type baseDirsMap map[uint32]*baseDirs

func (b baseDirsMap) Get(id uint32) *baseDirs {
	bd, ok := b[id]
	if !ok {
		bd = baseDirsPool.Get().(*baseDirs)
		b[id] = bd
	}

	return bd
}

func (b baseDirsMap) AddAll(fn func(id uint32, path *summary.DirectoryPath, age db.DirGUTAge) error) error {
	for id, bd := range b {
		for age, path := range bd {
			if path != nil {
				if err := fn(id, path, db.DirGUTAges[age]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b baseDirsMap) AddChildren(fn func(id uint32, path *summary.DirectoryPath, age db.DirGUTAge) error, parent *summary.DirectoryPath) error {
	for id, bd := range b {
		for age, path := range bd {
			if path != nil && path != parent {
				if err := fn(id, path, db.DirGUTAges[age]); err != nil {
					return err
				}

				bd[age] = nil
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
			} else {
				pm[n] = parent
			}
		}
	}
}

type BaseDirs struct {
	parent        *BaseDirs
	minDepth      minDepth
	db            DB
	refTime       int64
	thisDir       *summary.DirectoryPath
	users, groups baseDirsMap
}

func NewBaseDirs(minDepth minDepth, db DB) summary.OperationGenerator {
	var parent *BaseDirs

	now := time.Now().Unix()

	return func() summary.Operation {
		parent = &BaseDirs{
			parent:   parent,
			minDepth: minDepth,
			db:       db,
			refTime:  now,
			users:    make(baseDirsMap),
			groups:   make(baseDirsMap),
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
			gidBasedir.Set(n, info.Path, b.thisDir)
			uidBasedir.Set(n, info.Path, b.thisDir)
		}
	}

	return nil
}

func (b *BaseDirs) Output() error {
	minDepth := b.minDepth(b.thisDir)

	if b.thisDir.Depth == minDepth {
		if err := b.groups.AddAll(b.db.AddGroupBase); err != nil {
			return err
		}

		if err := b.users.AddAll(b.db.AddUserBase); err != nil {
			return err
		}
	} else if b.thisDir.Depth > minDepth {
		if err := b.groups.AddChildren(b.db.AddGroupBase, b.thisDir); err != nil {
			return err
		}

		if err := b.users.AddChildren(b.db.AddUserBase, b.thisDir); err != nil {
			return err
		}

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

	for _, v := range b.groups {
		*v = baseDirs{}

		baseDirsPool.Put(v)
	}

	for _, v := range b.users {
		*v = baseDirs{}

		baseDirsPool.Put(v)
	}

	for k := range b.groups {
		delete(b.groups, k)
	}

	for k := range b.users {
		delete(b.users, k)
	}
}
