/*******************************************************************************
 * Copyright (c) 2022, 2025 Genome Research Ltd.
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

package db

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
)

var (
	// ErrNoStorageFactory indicates no storage factory has been registered.
	ErrNoStorageFactory = errors.New("no storage factory registered")
)

const (
	GUTABucket  = "gut"
	ChildBucket = "children"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrDBExists    = Error("database already exists")
	ErrDBNotExists = Error("database doesn't exist")
	ErrDirNotFound = Error("directory not found")
)

// a dbSet is 2 databases, one for storing DGUTAs, one for storing children.
type dbSet struct {
	src   Source
	store Store
}

// NewDBSetFromSource creates a new dbSet from a Source implementation.
func NewDBSetFromSource(src Source) *dbSet { //nolint:revive
	return &dbSet{src: src}
}

// Create creates new database files in our directory. Returns an error if those
// files already exist.
func (s *dbSet) Create() error {
	exists, err := s.src.Exists()
	if err != nil {
		return err
	}

	if exists {
		return ErrDBExists
	}

	f := Default()
	if f == nil {
		return ErrNoStorageFactory
	}

	st, err := f.Create(s.src)
	if err != nil {
		return err
	}

	s.store = st

	return nil
}

// Paths returns the expected Paths for our dguta and children databases
// respectively.
// Paths removed: path computation is backend-specific; handled by Source.

// pathsExist tells you if the databases at the given paths already exist.
// pathsExist removed: use Source.Exists instead.

// Open opens our constituent databases read-only.
func (s *dbSet) Open() error {
	f := Default()
	if f == nil {
		return ErrNoStorageFactory
	}

	st, err := f.OpenReadOnly(s.src)
	if err != nil {
		return err
	}

	s.store = st
	return nil
}

// Close closes our constituent databases.
func (s *dbSet) Close() error {
	if s == nil {
		return nil
	}

	var errm *multierror.Error

	if s.store != nil {
		err := s.store.Close()
		errm = multierror.Append(errm, err)
	}

	return errm.ErrorOrNil()
}

type DBInfo struct { //nolint:revive
	NumDirs     int
	NumDGUTAs   int
	NumParents  int
	NumChildren int
}

// Info opens our constituent databases read-only, gets summary info about their
// contents, returns that info and closes the databases.
func (s *dbSet) Info() (*DBInfo, error) {
	info := &DBInfo{}
	ch := new(codec.BincHandle)

	f := Default()
	if f == nil {
		return nil, ErrNoStorageFactory
	}

	st, err := f.OpenReadOnlyUnPopulated(s.src)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	// DGUTA info
	if err := st.ForEachDGUTA(func(k, v []byte) error {
		info.NumDirs++
		dguta := DecodeDGUTAbytes(k, v)
		info.NumDGUTAs += len(dguta.GUTAs)

		return nil
	}); err != nil {
		return nil, err
	}

	// Children info
	if err := st.ForEachChildren(func(v []byte) error {
		info.NumParents++
		dec := codec.NewDecoderBytes(v, ch)
		var children []string
		dec.MustDecode(&children)
		info.NumChildren += len(children)

		return nil
	}); err != nil {
		return nil, err
	}

	return info, nil
}

// DB is used to create and query a database made from a dguta file, which is the
// directory,group,user,type,age summary output produced by the summary packages'
// DirGroupUserTypeAge.Output() method.
type DB struct {
	sources    []Source
	writeSet   *dbSet
	readSets   []*dbSet
	batchSize  int
	writeBatch []RecordDGUTA
	writeErr   error
	ch         codec.Handle
}

// NewDB constructs a DB from backend-agnostic Sources.
func NewDB(srcs ...Source) *DB { return &DB{sources: srcs, batchSize: 1} }

func (d *DB) SetBatchSize(batchSize int) {
	d.batchSize = batchSize
}

// CreateDB creates a new database set, but only if it doesn't already exist.
func (d *DB) CreateDB() error {
	set := NewDBSetFromSource(d.sources[0])

	err := set.Create()
	if err != nil {
		return err
	}

	d.writeSet = set
	d.ch = new(codec.BincHandle)

	return err
}

// resetBatch prepares us to receive a new batch of DGUTAs from the parser.
func (d *DB) resetBatch() {
	d.writeBatch = d.writeBatch[:0]
}

// Add is a dgutaParserCallBack that is called during parsing of dguta file
// data. It batches up the DGUTs we receive, and writes them to the database
// when a batch is full.
func (d *DB) Add(dguta RecordDGUTA) error {
	d.writeBatch = append(d.writeBatch, dguta)

	if len(d.writeBatch) == d.batchSize {
		d.storeBatch()
		d.resetBatch()
	}

	return d.writeErr
}

// storeBatch writes the current batch of DGUTAs to the database. It also updates
// our dir->child lookup in the database.
func (d *DB) storeBatch() {
	if d.writeErr != nil {
		return
	}

	// Build and write children first, then DGUTAs
	childrenPairs := d.buildChildrenPairs()
	if err := d.writeSet.store.PutChildren(childrenPairs); err != nil {
		d.writeErr = err

		return
	}

	dgutaPairs := d.buildDGUTAPairs()
	if err := d.writeSet.store.PutDGUTAs(dgutaPairs); err != nil {
		d.writeErr = err

		return
	}
}

// storeChildren stores the Dirs of the current DGUTA batch in the db.
func (d *DB) buildChildrenPairs() [][2][]byte {
	pairs := make([][2][]byte, 0, len(d.writeBatch))
	for _, r := range d.writeBatch {
		if len(r.Children) == 0 {
			continue
		}
		parent := string(r.Dir.AppendTo(nil))
		for n := range r.Children {
			r.Children[n] = parent + strings.TrimSuffix(r.Children[n], "/")
		}

		key := cloneBytes(r.pathBytes())
		val := cloneBytes(d.encodeChildren(r.Children))
		pairs = append(pairs, [2][]byte{key, val})
	}

	return pairs
}

// getChildrenFromDB retrieves the child directory values associated with the
// given directory key in the given db. Returns an empty slice if the dir wasn't
// found.
func (d *DB) getChildrenFromStore(st Store, dir string) []string {
	key := []byte(dir)
	if !strings.HasSuffix(dir, "/") {
		key = append(key, '/')
	}

	v, err := st.GetChildren(key)
	if err != nil || v == nil {
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
func (d *DB) buildDGUTAPairs() [][2][]byte {
	pairs := make([][2][]byte, 0, len(d.writeBatch))
	for _, dguta := range d.writeBatch {
		k, v := dguta.EncodeToBytes()
		pairs = append(pairs, [2][]byte{cloneBytes(k), v})
	}

	return pairs
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// storeDGUTA stores a DGUTA in the db. DGUTAs are expected to be unique per
// Store() operation and database.
// removed storeDGUTA; handled by buildDGUTAPairs

// Open opens the database(s) for reading. You need to call this before using
// the query methods like DirInfo() and Which(). You should call Close() after
// you've finished.
func (d *DB) Open() error {
	readSets := make([]*dbSet, len(d.sources))

	for i, src := range d.sources {
		readSet := NewDBSetFromSource(src)

		exists, err := src.Exists()
		if err != nil {
			return err
		}

		if !exists {
			return ErrDBNotExists
		}

		if err = readSet.Open(); err != nil {
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
func (d *DB) Close() error {
	if len(d.writeBatch) != 0 {
		d.storeBatch()
		d.resetBatch()
	}

	for _, readSet := range d.readSets {
		if err := readSet.Close(); err != nil {
			return err
		}
	}

	if d.writeErr != nil {
		return d.writeErr
	}

	if d.writeSet != nil {
		return d.writeSet.Close()
	}

	return nil
}

// DirInfo tells you the total number of files, their total size, oldest atime
// and newset mtime nested under the given directory, along with the UIDs, GIDs
// and FTs of those files. See GUTAs.Summary for an explanation of the filter.
//
// Returns an error if dir doesn't exist.
//
// You must call Open() before calling this.
func (d *DB) DirInfo(dir string, filter *Filter) (*DirSummary, error) {
	dguta, notFound, _ := d.combineDGUTAsFromReadSets(dir)

	if notFound == len(d.readSets) {
		return &DirSummary{}, ErrDirNotFound
	}

	ds := dguta.Summary(filter)

	return ds, nil
}

func (d *DB) combineDGUTAsFromReadSets(dir string) (*DGUTA, int, time.Time) {
	var (
		notFound    int
		lastUpdated time.Time
	)

	dguta := &DGUTA{}

	for _, readSet := range d.readSets {
		if err := getDGUTAFromStoreAndAppend(readSet.store, dir, dguta); err != nil {
			notFound++
		}
	}

	return dguta, notFound, lastUpdated
}

// getDGUTAFromDBAndAppend calls getDGUTAFromDB() and appends the result
// to the given dguta. If the given dguta is empty, it will be populated with the
// content of the result instead.
func getDGUTAFromStoreAndAppend(st Store, dir string, dguta *DGUTA) error {
	thisDGUTA, err := getDGUTAFromStore(st, dir)
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

// getDGUTAFromStore gets and decodes a dguta from the given store.
func getDGUTAFromStore(st Store, dir string) (*DGUTA, error) {
	bdir := make([]byte, 0, 2+len(dir)) //nolint:mnd
	bdir = append(bdir, dir...)

	if !strings.HasSuffix(dir, "/") {
		bdir = append(bdir, '/')
	}

	bdir = append(bdir, endByte)

	v, err := st.GetDGUTA(bdir)
	if err != nil || v == nil {
		return nil, ErrDirNotFound
	}

	dguta := DecodeDGUTAbytes(bdir, v)

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
		for _, child := range d.getChildrenFromStore(readSet.store, dir) {
			children[child] = true
		}
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

	readSets := make([]*dbSet, len(d.sources))
	for i, src := range d.sources {
		readSets[i] = NewDBSetFromSource(src)
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
