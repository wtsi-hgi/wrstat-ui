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
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	gutaBucket         = "gut"
	childBucket        = "children"
	dbBasenameDGUTA    = "dguta.db"
	dbBasenameChildren = dbBasenameDGUTA + ".children"
	dbOpenMode         = 0600
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrDBExists = Error("database already exists")
const ErrDBNotExists = Error("database doesn't exist")
const ErrDirNotFound = Error("directory not found")

// a dbSet is 2 databases, one for storing DGUTAs, one for storing children.
type dbSet struct {
	dir      string
	dgutas   *bolt.DB
	children *bolt.DB
	modtime  time.Time
}

// newDBSet creates a new newDBSet that knows where its database files are
// located or should be created.
func newDBSet(dir string) (*dbSet, error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}

	return &dbSet{
		dir:     dir,
		modtime: fi.ModTime(),
	}, nil
}

// Create creates new database files in our directory. Returns an error if those
// files already exist.
func (s *dbSet) Create() error {
	paths := s.paths()

	if s.pathsExist(paths) {
		return ErrDBExists
	}

	db, err := openBoltWritable(paths[0], gutaBucket)
	if err != nil {
		return err
	}

	s.dgutas = db

	db, err = openBoltWritable(paths[1], childBucket)
	s.children = db

	return err
}

// paths returns the expected paths for our dguta and children databases
// respectively.
func (s *dbSet) paths() []string {
	return []string{
		filepath.Join(s.dir, dbBasenameDGUTA),
		filepath.Join(s.dir, dbBasenameChildren),
	}
}

// pathsExist tells you if the databases at the given paths already exist.
func (s *dbSet) pathsExist(paths []string) bool {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil && info.Size() != 0 {
			return true
		}
	}

	return false
}

// openBoltWritable creates a new database at the given path with the given
// bucket inside.
func openBoltWritable(path, bucket string) (*bolt.DB, error) {
	db, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, errc := tx.CreateBucketIfNotExists([]byte(bucket))

		return errc
	})

	return db, err
}

// Open opens our constituent databases read-only.
func (s *dbSet) Open() error {
	paths := s.paths()

	db, err := openBoltReadOnly(paths[0])
	if err != nil {
		return err
	}

	s.dgutas = db

	db, err = openBoltReadOnly(paths[1])
	if err != nil {
		return err
	}

	s.children = db

	return nil
}

// openBoltReadOnly opens a bolt database at the given path in read-only mode.
func openBoltReadOnly(path string) (*bolt.DB, error) {
	return bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly:  true,
		MmapFlags: syscall.MAP_POPULATE,
	})
}

// Close closes our constituent databases.
func (s *dbSet) Close() error {
	var errm *multierror.Error

	err := s.dgutas.Close()
	errm = multierror.Append(errm, err)

	err = s.children.Close()
	errm = multierror.Append(errm, err)

	return errm.ErrorOrNil()
}

type DBInfo struct {
	NumDirs     int
	NumDGUTAs   int
	NumParents  int
	NumChildren int
}

// Info opens our constituent databases read-only, gets summary info about their
// contents, returns that info and closes the databases.
func (s *dbSet) Info() (*DBInfo, error) {
	paths := s.paths()
	info := &DBInfo{}
	ch := new(codec.BincHandle)

	err := gutaDBInfo(paths[0], info, ch)
	if err != nil {
		return nil, err
	}

	err = childrenDBInfo(paths[1], info, ch)

	return info, err
}

func gutaDBInfo(path string, info *DBInfo, ch codec.Handle) error {
	gutaDB, err := openBoltReadOnlyUnPopulated(path)
	if err != nil {
		return err
	}

	slog.Debug("opened bolt file", "path", path)

	defer gutaDB.Close()

	fullBucketScan(gutaDB, gutaBucket, func(k, v []byte) {
		if k[len(k)-1] == byte(DGUTAgeAll) {
			info.NumDirs++
		}

		dguta := decodeDGUTAbytes(ch, k, v)
		info.NumDGUTAs += len(dguta.GUTAs)
	})

	slog.Debug("went through bucket", "name", gutaBucket)

	return nil
}

// openBoltReadOnlyUnPopulated opens a bolt database at the given path in
// read-only mode, without MAP_POPULATE.
func openBoltReadOnlyUnPopulated(path string) (*bolt.DB, error) {
	return bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly: true,
	})
}

func fullBucketScan(db *bolt.DB, bucketName string, cb func(k, v []byte)) {
	db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		b := tx.Bucket([]byte(bucketName))

		return b.ForEach(func(k, v []byte) error {
			cb(k, v)

			return nil
		})
	})
}

func childrenDBInfo(path string, info *DBInfo, ch codec.Handle) error {
	childDB, err := openBoltReadOnlyUnPopulated(path)
	if err != nil {
		return err
	}

	slog.Debug("opened bolt file", "path", path)

	defer childDB.Close()

	fullBucketScan(childDB, childBucket, func(_, v []byte) {
		info.NumParents++

		dec := codec.NewDecoderBytes(v, ch)

		var children []string

		dec.MustDecode(&children)

		info.NumChildren += len(children)
	})

	slog.Debug("went through bucket", "name", childBucket)

	return nil
}

// DB is used to create and query a database made from a dguta file, which is the
// directory,group,user,type,age summary output produced by the summary packages'
// DirGroupUserTypeAge.Output() method.
type DB struct {
	paths      []string
	writeSet   *dbSet
	readSets   []*dbSet
	batchSize  int
	writeBatch []RecordDGUTA
	writeErr   error
	ch         codec.Handle
}

// NewDB returns a *DB that can be used to create or query a dguta database.
// Provide the path to directory that (will) store(s) the database files. In the
// case of only reading databases with Open(), you can supply multiple directory
// paths to query all of them simultaneously.
func NewDB(paths ...string) *DB {
	return &DB{paths: paths, batchSize: 1}
}

// Store will read the given dguta file data (as output by
// summary.DirGroupUserTypeAge.Output()) and store it in 2 database files that
// offer fast lookup of the information by directory.
//
// The path for the database directory you provided to NewDB() (only the first
// will be used) must not already have database files in it to create a new
// database. You can't add to an existing database. If you create multiple sets
// of data to store, instead Store them to individual database directories, and
// then load all them together during Open().
//
// batchSize is how many directories worth of information are written to the
// database in one go. More is faster, but uses more memory. 10,000 might be a
// good number to try.
// func (d *DB) Store(data io.Reader, batchSize int) (err error) {
// 	d.batchSize = batchSize

// 	err = d.createDB()
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		errc := d.writeSet.Close()
// 		if err == nil {
// 			err = errc
// 		}
// 	}()

// 	if err = d.storeData(data); err != nil {
// 		return err
// 	}

// 	if d.writeBatch[0] != nil {
// 		d.storeBatch()
// 	}

// 	err = d.writeErr

// 	return err
// }

// createDB creates a new database set, but only if it doesn't already exist.
func (d *DB) createDB() error {
	set, err := newDBSet(d.paths[0])
	if err != nil {
		return err
	}

	err = set.Create()
	if err != nil {
		return err
	}

	d.writeSet = set
	d.ch = new(codec.BincHandle)

	return err
}

// storeData parses the data and stores it in our database file. Only call this
// after calling createDB(), and only call it once.
// func (d *DB) storeData(data io.Reader) error {
// 	d.resetBatch()

// 	return parseDGUTALines(data, d.parserCB)
// }

// resetBatch prepares us to receive a new batch of DGUTAs from the parser.
func (d *DB) resetBatch() {
	d.writeBatch = d.writeBatch[:0]
}

// Add is a dgutaParserCallBack that is called during parsing of dguta file
// data. It batches up the DGUTs we receive, and writes them to the database
// when a batch is full.
func (d *DB) Add(dguta RecordDGUTA) {
	d.writeBatch = append(d.writeBatch, dguta)

	if len(d.writeBatch) == d.batchSize {
		d.storeBatch()
		d.resetBatch()
	}
}

// storeBatch writes the current batch of DGUTAs to the database. It also updates
// our dir->child lookup in the database.
func (d *DB) storeBatch() {
	if d.writeErr != nil {
		return
	}

	// var errm *multierror.Error

	//err := d.writeSet.children.Update(d.storeChildren)
	//errm = multierror.Append(errm, err)

	d.writeErr = d.writeSet.dgutas.Update(d.storeDGUTAs)
	//errm = multierror.Append(errm, err)

	// err = errm.ErrorOrNil()
	// if err != nil {
	// 	d.writeErr = err
	// }
}

// storeChildren stores the Dirs of the current DGUTA batch in the db.
// func (d *DB) storeChildren(txn *bolt.Tx) error {
// 	b := txn.Bucket([]byte(childBucket))

// 	parentToChildren := d.calculateChildrenOfParents(b)

// 	for parent, children := range parentToChildren {
// 		if err := b.Put([]byte(parent), d.encodeChildren(children)); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// calculateChildrenOfParents works out what the children of every parent
// directory of every dguta.Dir is in the current writeBatch. Returns a map
// of parent keys and children slice value.
// func (d *DB) calculateChildrenOfParents(b *bolt.Bucket) map[string][]string {
// 	parentToChildren := make(map[string][]string)

// 	for _, dguta := range d.writeBatch {
// 		if dguta == nil {
// 			continue
// 		}

// 		d.storeChildrenOfParentInMap(b, dguta.Dir, parentToChildren)
// 	}

// 	return parentToChildren
// }

// storeChildrenOfParentInMap gets current children of child's parent in the db
// and stores them in the store map, then once stored in the map, appends this
// child to the parent's children.
func (d *DB) storeChildrenOfParentInMap(b *bolt.Bucket, child string, store map[string][]string) {
	if child == "/" {
		return
	}

	parent := filepath.Dir(child)

	var children []string

	if storedChildren, stored := store[parent]; stored {
		children = storedChildren
	} else {
		children = d.getChildrenFromDB(b, parent)
	}

	children = append(children, child)

	store[parent] = children
}

// getChildrenFromDB retrieves the child directory values associated with the
// given directory key in the given db. Returns an empty slice if the dir wasn't
// found.
func (d *DB) getChildrenFromDB(b *bolt.Bucket, dir string) []string {
	v := b.Get([]byte(dir))
	if v == nil {
		return []string{}
	}

	return d.decodeChildrenBytes(v)
}

// decodeChildBytes converts the byte slice returned by encodeChildren() back
// in to a []string.
func (d *DB) decodeChildrenBytes(encoded []byte) []string {
	dec := codec.NewDecoderBytes(encoded, d.ch)

	var children []string

	dec.MustDecode(&children)

	return children
}

// encodeChildren returns converts the given string slice into a []byte suitable
// for storing on disk.
func (d *DB) encodeChildren(dirs []string) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, d.ch)
	enc.MustEncode(dirs)

	return encoded
}

// storeDGUTAs stores the current batch of DGUTAs in the db.
func (d *DB) storeDGUTAs(tx *bolt.Tx) error {
	b := tx.Bucket([]byte(gutaBucket))

	for _, dguta := range d.writeBatch {
		if err := d.storeDGUTA(b, dguta); err != nil {
			return err
		}
	}

	return nil
}

// storeDGUTA stores a DGUTA in the db. DGUTAs are expected to be unique per
// Store() operation and database.
func (d *DB) storeDGUTA(b *bolt.Bucket, dguta RecordDGUTA) error {
	var dgutas [len(DirGUTAges)]RecordDGUTA

	for _, v := range dguta.GUTAs {
		dgutas[v.Age].GUTAs = append(dgutas[v.Age].GUTAs, v)
	}

	for age, v := range dgutas {
		dir, gutas := v.encodeToBytes(d.ch, DirGUTAge(age))

		if err := b.Put(dir, gutas); err != nil {
			return err
		}
	}

	return nil
}

// Open opens the database(s) for reading. You need to call this before using
// the query methods like DirInfo() and Which(). You should call Close() after
// you've finished.
func (d *DB) Open() error {
	readSets := make([]*dbSet, len(d.paths))

	for i, path := range d.paths {
		readSet, err := newDBSet(path)
		if err != nil {
			return err
		}

		if !readSet.pathsExist(readSet.paths()) {
			return ErrDBNotExists
		}

		err = readSet.Open()
		if err != nil {
			return err
		}

		readSets[i] = readSet
	}

	d.readSets = readSets

	d.ch = new(codec.BincHandle)

	return nil
}

// Close closes the database(s) after reading. You should call this once
// you've finished reading, but it's not necessary; errors are ignored.
func (d *DB) Close() {
	if d.readSets == nil {
		return
	}

	for _, readSet := range d.readSets {
		readSet.Close()
	}
}

// DirInfo tells you the total number of files, their total size, oldest atime
// and newset mtime nested under the given directory, along with the UIDs, GIDs
// and FTs of those files. See GUTAs.Summary for an explanation of the filter.
//
// Returns an error if dir doesn't exist.
//
// You must call Open() before calling this.
func (d *DB) DirInfo(dir string, filter *Filter) (*DirSummary, error) {
	var age DirGUTAge

	if filter != nil {
		age = filter.Age
	}

	dguta, notFound, lastUpdated := d.combineDGUTAsFromReadSets(dir, age)

	if notFound == len(d.readSets) {
		return &DirSummary{Modtime: lastUpdated}, ErrDirNotFound
	}

	ds := dguta.Summary(filter)
	if ds != nil {
		ds.Modtime = lastUpdated
	}

	return ds, nil
}

func (d *DB) combineDGUTAsFromReadSets(dir string, age DirGUTAge) (*DGUTA, int, time.Time) {
	var (
		notFound    int
		lastUpdated time.Time
	)

	dguta := &DGUTA{}

	for _, readSet := range d.readSets {
		if err := readSet.dgutas.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(gutaBucket))

			if readSet.modtime.After(lastUpdated) {
				lastUpdated = readSet.modtime
			}

			return getDGUTAFromDBAndAppend(b, dir, d.ch, dguta, age)
		}); err != nil {
			notFound++
		}
	}

	return dguta, notFound, lastUpdated
}

// getDGUTAFromDBAndAppend calls getDGUTAFromDB() and appends the result
// to the given dguta. If the given dguta is empty, it will be populated with the
// content of the result instead.
func getDGUTAFromDBAndAppend(b *bolt.Bucket, dir string, ch codec.Handle, dguta *DGUTA, age DirGUTAge) error {
	thisDGUTA, err := getDGUTAFromDB(b, dir, ch, age)
	if err != nil {
		return err
	}

	if dguta.Dir == "" {
		dguta.Dir = thisDGUTA.Dir
		dguta.GUTAs = thisDGUTA.GUTAs
	} else {
		dguta.Append(thisDGUTA)
	}

	return nil
}

// getDGUTAFromDB gets and decodes a dguta from the given database.
func getDGUTAFromDB(b *bolt.Bucket, dir string, ch codec.Handle, age DirGUTAge) (*DGUTA, error) {
	bdir := make([]byte, 0, 1+len(dir))
	bdir = append(bdir, dir...)
	bdir = append(bdir, byte(age))

	v := b.Get(bdir)
	if v == nil {
		return nil, ErrDirNotFound
	}

	dguta := decodeDGUTAbytes(ch, bdir, v)

	return dguta, nil
}

// Children returns the directory paths that are directly inside the given
// directory.
//
// Returns an empty slice if dir had no children (because it was a leaf dir,
// or didn't exist at all).
//
// The same children from multiple databases are de-duplicated.
//
// You must call Open() before calling this.
func (d *DB) Children(dir string) []string {
	children := make(map[string]bool)

	for _, readSet := range d.readSets {
		// no error is possible here, but the View function requires we return
		// one.
		//nolint:errcheck
		readSet.children.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(childBucket))

			for _, child := range d.getChildrenFromDB(b, dir) {
				children[child] = true
			}

			return nil
		})
	}

	return mapToSortedKeys(children)
}

// mapToSortedKeys takes the keys from the given map and returns them as a
// sorted slice. If map length is 0, returns nil.
func mapToSortedKeys(things map[string]bool) []string {
	if len(things) == 0 {
		return nil
	}

	keys := make([]string, len(things))
	i := 0

	for thing := range things {
		keys[i] = thing
		i++
	}

	sort.Strings(keys)

	return keys
}

// Info opens our constituent databases read-only, gets summary info about their
// contents, returns that info and closes the databases.
func (d *DB) Info() (*DBInfo, error) {
	infos := &DBInfo{}

	readSets := make([]*dbSet, len(d.paths))

	for i, path := range d.paths {
		readSet, err := newDBSet(path)
		if err != nil {
			return nil, err
		}

		if !readSet.pathsExist(readSet.paths()) {
			return nil, ErrDBNotExists
		}

		readSets[i] = readSet
	}

	for _, rs := range readSets {
		info, err := rs.Info()
		if err != nil {
			return nil, err
		}

		infos.NumDirs += info.NumDirs
		infos.NumDGUTAs += info.NumDGUTAs
		infos.NumParents += info.NumParents
		infos.NumChildren += info.NumChildren
	}

	return infos, nil
}
