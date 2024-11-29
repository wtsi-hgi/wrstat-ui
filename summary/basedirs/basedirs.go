package basedirs

import (
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const numAges = len(db.DirGUTAges)

type minDepth func(*summary.DirectoryPath) bool

type splits func(*summary.DirectoryPath) int

type baseDirs [numAges]*summary.DirectoryPath

type baseDirsMap map[uint32]*baseDirs

func (b baseDirsMap) Get(id uint32) *baseDirs {
	bd, ok := b[id]
	if !ok {
		bd = new(baseDirs)
		b[id] = bd
	}

	return bd
}

func (b baseDirsMap) mergeTo(pbm baseDirsMap, parent *summary.DirectoryPath) {
	for id, bm := range b {
		pm, ok := pbm[id]
		if !ok {
			pbm[id] = bm

			continue
		}

		for n := range bm {
			if pm[n] == nil {
				pm[n] = bm[n]
			} else {
				pm[n] = parent
			}
		}
	}
}

type BaseDirs struct {
	parent        *BaseDirs
	minDepth      minDepth
	splits        splits
	refTime       int64
	thisDir       *summary.DirectoryPath
	users, groups baseDirsMap
}

func NewBaseDirs(minDepth minDepth, splits splits) summary.OperationGenerator {
	var parent *BaseDirs

	now := time.Now().Unix()

	return func() summary.Operation {
		parent := &BaseDirs{
			parent:   parent,
			minDepth: minDepth,
			splits:   splits,
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

	if info.Path.Parent != b.thisDir {
		return nil
	}

	gidBasedir := b.groups.Get(info.GID)
	uidBasedir := b.users.Get(info.UID)

	for n, threshold := range db.DirGUTAges {
		if threshold.FitsAgeInterval(info.ATime, info.MTime, b.refTime) {
			gidBasedir[n] = b.thisDir
			uidBasedir[n] = b.thisDir
		}
	}

	return nil
}

func (b *BaseDirs) Output() error {
	// output if appropriate

	b.addToParent()

	b.thisDir = nil

	for k := range b.groups {
		delete(b.groups, k)
	}

	for k := range b.users {
		delete(b.users, k)
	}

	return nil
}

func (b *BaseDirs) addToParent() {
	if b.parent == nil {
		return
	}

	b.groups.mergeTo(b.parent.groups, b.parent.thisDir)
	b.users.mergeTo(b.parent.users, b.parent.thisDir)
}
