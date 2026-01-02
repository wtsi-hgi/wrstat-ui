/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   GitHub Copilot
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
	"os"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

type dgutaReadSet struct {
	dir       string
	dgutas    *bolt.DB
	children  *bolt.DB
	updatedAt time.Time
}

type dgutaDatabase struct {
	paths []string
	sets  []*dgutaReadSet
	ch    codec.Handle
}

// OpenDatabase opens a Bolt-backed database implementation that satisfies the
// db.Database interface. Each provided path is a dataset directory containing
// dguta.db and dguta.db.children.
func OpenDatabase(paths ...string) (db.Database, error) {
	return openDGUTADatabase(paths)
}

func openDGUTADatabase(paths []string) (*dgutaDatabase, error) {
	if len(paths) == 0 {
		return nil, db.ErrDBNotExists
	}

	sets := make([]*dgutaReadSet, len(paths))
	for i, dir := range paths {
		set, err := openDGUTAReadSet(dir)
		if err != nil {
			for _, s := range sets {
				if s != nil {
					_ = s.Close()
				}
			}
			return nil, err
		}
		sets[i] = set
	}

	return &dgutaDatabase{
		paths: paths,
		sets:  sets,
		ch:    new(codec.BincHandle),
	}, nil
}

func openDGUTAReadSet(dir string) (*dgutaReadSet, error) {
	dgutaDB, err := openBoltReadOnly(filepathJoin(dir, dgutaDBBasename))
	if err != nil {
		return nil, err
	}

	childrenDB, err := openBoltReadOnly(filepathJoin(dir, childrenDBBasename))
	if err != nil {
		_ = dgutaDB.Close()
		return nil, err
	}

	updatedAt, err := readUpdatedAt(dgutaDB)
	if err != nil {
		_ = dgutaDB.Close()
		_ = childrenDB.Close()
		return nil, err
	}

	if updatedAt.IsZero() {
		if st, err := os.Stat(filepathParent(dir)); err == nil {
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
		if len(v) != 8 {
			return nil
		}

		secs := int64(binary.LittleEndian.Uint64(v))
		t = time.Unix(secs, 0)

		return nil
	})

	return t, err
}

func (s *dgutaReadSet) Close() error {
	if s == nil {
		return nil
	}

	var errm *multierror.Error

	if s.dgutas != nil {
		err := s.dgutas.Close()
		errm = multierror.Append(errm, err)
	}

	if s.children != nil {
		err := s.children.Close()
		errm = multierror.Append(errm, err)
	}

	return errm.ErrorOrNil()
}

func (d *dgutaDatabase) Close() error {
	if d == nil {
		return nil
	}

	var errm *multierror.Error
	for _, s := range d.sets {
		err := s.Close()
		errm = multierror.Append(errm, err)
	}

	return errm.ErrorOrNil()
}

func (d *dgutaDatabase) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	combined, notFound, lastUpdated := d.combineDGUTAs(dir)

	if notFound == len(d.sets) {
		return &db.DirSummary{Modtime: lastUpdated}, db.ErrDirNotFound
	}

	ds := combined.Summary(filter)
	if ds != nil {
		ds.Modtime = lastUpdated
	}

	return ds, nil
}

func (d *dgutaDatabase) combineDGUTAs(dir string) (*db.DGUTA, int, time.Time) {
	var (
		notFound    int
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
		if err != nil {
			notFound++
		}
	}

	return combined, notFound, lastUpdated
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

func getDGUTAFromDB(b *bolt.Bucket, dir string) (*db.DGUTA, error) {
	bdir := make([]byte, 0, 2+len(dir))
	bdir = append(bdir, dir...)

	if !strings.HasSuffix(dir, "/") {
		bdir = append(bdir, '/')
	}

	bdir = append(bdir, 255)

	v := b.Get(bdir)
	if v == nil {
		return nil, db.ErrDirNotFound
	}

	return db.DecodeDGUTAbytes(bdir, v), nil
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

func mapToSortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func (d *dgutaDatabase) Info() (*db.DBInfo, error) {
	return nil, errors.New("not implemented")
}

func filepathParent(path string) string {
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	idx := strings.LastIndexByte(path, '/')
	if idx <= 0 {
		return "/"
	}

	return path[:idx]
}
