/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package bolt

import (
	"encoding/binary"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

const (
	dgutaKeyExtraCap   = 2
	dgutaKeyTerminator = 0xFF
)

type dgutaReadSet struct {
	dir       string
	dgutas    *bolt.DB
	children  *bolt.DB
	updatedAt time.Time
}

func openDGUTAReadSet(dir string) (*dgutaReadSet, error) {
	dgutaDB, childrenDB, err := openDGUTADBPairs(dir)
	if err != nil {
		return nil, err
	}

	updatedAt, err := readUpdatedAt(dgutaDB)
	if err != nil {
		_ = dgutaDB.Close()
		_ = childrenDB.Close()

		return nil, err
	}

	if updatedAt.IsZero() {
		if st, statErr := os.Stat(filepathParent(dir)); statErr == nil {
			updatedAt = st.ModTime()
		}
	}

	return &dgutaReadSet{
		dir:       dir,
		dgutas:    dgutaDB,
		children:  childrenDB,
		updatedAt: updatedAt,
	}, nil
}

func (s *dgutaReadSet) Close() error {
	if s == nil {
		return nil
	}

	var err error

	if s.dgutas != nil {
		err = errors.Join(err, s.dgutas.Close())
	}

	if s.children != nil {
		err = errors.Join(err, s.children.Close())
	}

	return err
}

func closeReadSets(sets []*dgutaReadSet) {
	for _, s := range sets {
		if s == nil {
			continue
		}

		_ = s.Close()
	}
}

type dgutaDatabase struct {
	paths []string
	sets  []*dgutaReadSet
	ch    codec.Handle
}

func openDGUTADatabase(paths []string) (*dgutaDatabase, error) {
	if len(paths) == 0 {
		return nil, db.ErrDBNotExists
	}

	sets := make([]*dgutaReadSet, 0, len(paths))
	for _, dir := range paths {
		set, err := openDGUTAReadSet(dir)
		if err != nil {
			closeReadSets(sets)

			return nil, err
		}

		sets = append(sets, set)
	}

	return &dgutaDatabase{
		paths: paths,
		sets:  sets,
		ch:    new(codec.BincHandle),
	}, nil
}

func (d *dgutaDatabase) Close() error {
	if d == nil {
		return nil
	}

	var err error

	for _, s := range d.sets {
		err = errors.Join(err, s.Close())
	}

	return err
}

func (d *dgutaDatabase) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	combined, found, lastUpdated := d.combineDGUTAs(dir)

	if !found {
		return &db.DirSummary{Modtime: lastUpdated}, db.ErrDirNotFound
	}

	ds := combined.Summary(filter)
	if ds != nil {
		ds.Modtime = lastUpdated
	}

	return ds, nil
}

func (d *dgutaDatabase) combineDGUTAs(dir string) (*db.DGUTA, bool, time.Time) {
	var (
		found       bool
		lastUpdated time.Time
	)

	combined := &db.DGUTA{}

	for _, s := range d.sets {
		if s.updatedAt.After(lastUpdated) {
			lastUpdated = s.updatedAt
		}

		err := s.dgutas.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(dgutaBucketName))
			if b == nil {
				return db.ErrDirNotFound
			}

			return getDGUTAFromDBAndAppend(b, dir, combined)
		})
		if err == nil {
			found = true
		}
	}

	return combined, found, lastUpdated
}

func getDGUTAFromDBAndAppend(b *bolt.Bucket, dir string, combined *db.DGUTA) error {
	thisDGUTA, err := getDGUTAFromDB(b, dir)
	if err != nil {
		return err
	}

	if combined.Dir == "" {
		combined.Dir = thisDGUTA.Dir
		combined.GUTAs = thisDGUTA.GUTAs

		return nil
	}

	combined.Append(thisDGUTA)

	return nil
}

func (d *dgutaDatabase) Children(dir string) ([]string, error) {
	children := make(map[string]bool)

	for _, s := range d.sets {
		err := s.children.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(childrenBucketName))
			if b == nil {
				return nil
			}

			for _, child := range d.getChildrenFromDB(b, dir) {
				children[child] = true
			}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return mapToSortedKeys(children), nil
}

func mapToSortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}

	keys := slices.Collect(maps.Keys(m))
	sort.Strings(keys)

	return keys
}

func (d *dgutaDatabase) getChildrenFromDB(b *bolt.Bucket, dir string) []string {
	key := []byte(dir)
	if !strings.HasSuffix(dir, "/") {
		key = append(key, '/')
	}

	v := b.Get(key)
	if v == nil {
		return nil
	}

	dec := codec.NewDecoderBytes(v, d.ch)

	var children []string
	dec.MustDecode(&children)

	return children
}

func (d *dgutaDatabase) Info() (*db.Info, error) {
	info := &db.Info{}

	for _, s := range d.sets {
		setInfo, err := d.setInfo(s)
		if err != nil {
			return nil, err
		}

		info.NumDirs += setInfo.NumDirs
		info.NumDGUTAs += setInfo.NumDGUTAs
		info.NumParents += setInfo.NumParents
		info.NumChildren += setInfo.NumChildren
	}

	return info, nil
}

func (d *dgutaDatabase) setInfo(s *dgutaReadSet) (*db.Info, error) {
	info := &db.Info{}

	if err := d.scanGUTAInfo(s.dgutas, info); err != nil {
		return nil, err
	}

	if err := d.scanChildrenInfo(s.children, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (d *dgutaDatabase) scanGUTAInfo(dbh *bolt.DB, info *db.Info) error {
	return dbh.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dgutaBucketName))
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			info.NumDirs++

			dguta := db.DecodeDGUTAbytes(k, v)
			info.NumDGUTAs += len(dguta.GUTAs)

			return nil
		})
	})
}

func (d *dgutaDatabase) scanChildrenInfo(dbh *bolt.DB, info *db.Info) error {
	return dbh.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(childrenBucketName))
		if b == nil {
			return nil
		}

		return b.ForEach(func(_, v []byte) error {
			info.NumParents++

			var children []string

			dec := codec.NewDecoderBytes(v, d.ch)
			dec.MustDecode(&children)

			info.NumChildren += len(children)

			return nil
		})
	})
}

// OpenDatabase opens a Bolt-backed database implementation that satisfies the
// db.Database interface. Each provided path is a dataset directory containing
// dguta.db and dguta.db.children.
func OpenDatabase(paths ...string) (db.Database, error) {
	return openDGUTADatabase(paths)
}

func openDGUTADBPairs(dir string) (*bolt.DB, *bolt.DB, error) {
	dgutaDB, err := openBoltReadOnly(filepath.Join(dir, dgutaDBBasename))
	if err != nil {
		return nil, nil, err
	}

	childrenDB, err := openBoltReadOnly(filepath.Join(dir, childrenDBBasename))
	if err != nil {
		_ = dgutaDB.Close()

		return nil, nil, err
	}

	return dgutaDB, childrenDB, nil
}

func openBoltReadOnly(path string) (*bolt.DB, error) {
	return bolt.Open(path, boltFilePerms, &bolt.Options{ReadOnly: true})
}

func readUpdatedAt(dbh *bolt.DB) (time.Time, error) {
	var t time.Time

	err := dbh.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketName))
		if b == nil {
			return nil
		}

		v := b.Get([]byte(metaKeyUpdatedAt))
		if len(v) != unixSecondsBytesLen {
			return nil
		}

		raw := binary.LittleEndian.Uint64(v)
		t = time.Unix(int64(raw), 0) //nolint: gosec

		return nil
	})

	return t, err
}

func getDGUTAFromDB(b *bolt.Bucket, dir string) (*db.DGUTA, error) {
	bdir := make([]byte, 0, dgutaKeyExtraCap+len(dir))
	bdir = append(bdir, dir...)

	if !strings.HasSuffix(dir, "/") {
		bdir = append(bdir, '/')
	}

	bdir = append(bdir, dgutaKeyTerminator)

	v := b.Get(bdir)
	if v == nil {
		return nil, db.ErrDirNotFound
	}

	return db.DecodeDGUTAbytes(bdir, v), nil
}

func filepathParent(path string) string {
	path = strings.TrimSuffix(path, "/")

	idx := strings.LastIndexByte(path, '/')
	if idx <= 0 {
		return "/"
	}

	return path[:idx]
}
