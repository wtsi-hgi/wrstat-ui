package bolt

import (
	"os"

	bbolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// Options mirrors a subset of bbolt.Options used in this project.
type Options struct {
	NoFreelistSync bool
	NoGrowSync     bool
	FreelistType   FreelistType
	ReadOnly       bool
}

// FreelistType is a minimal enum matching bbolt's freelist types.
type FreelistType int

const (
	FreelistArrayType FreelistType = iota
	FreelistMapType
)

// MaxKeySize is provided for tests that validate key size errors.
const MaxKeySize = bbolt.MaxKeySize

// ErrBucketNotFound re-exports the specific error value used for logic checks.
var ErrBucketNotFound = berrors.ErrBucketNotFound

// DB is our wrapper around bbolt.DB.
type DB struct {
	inner *bbolt.DB
}

// Tx is our wrapper around bbolt.Tx.
type Tx struct {
	inner *bbolt.Tx
}

// Bucket is our wrapper around bbolt.Bucket.
type Bucket struct {
	inner *bbolt.Bucket
}

// Cursor is our wrapper around bbolt.Cursor.
type Cursor struct {
	inner *bbolt.Cursor
}

// Open opens or creates a database at the given path.
func Open(path string, mode os.FileMode, opt *Options) (*DB, error) {
	var bopt *bbolt.Options
	if opt != nil {
		bopt = &bbolt.Options{
			NoFreelistSync: opt.NoFreelistSync,
			NoGrowSync:     opt.NoGrowSync,
			ReadOnly:       opt.ReadOnly,
		}
		switch opt.FreelistType {
		case FreelistArrayType:
			bopt.FreelistType = bbolt.FreelistArrayType
		case FreelistMapType:
			bopt.FreelistType = bbolt.FreelistMapType
		default:
			// leave zero value
		}
	}

	db, err := bbolt.Open(path, mode, bopt)
	if err != nil {
		return nil, err
	}

	return &DB{inner: db}, nil
}

// Close closes the database.
func (d *DB) Close() error { //nolint:wrapcheck
	if d == nil || d.inner == nil {
		return nil
	}

	return d.inner.Close()
}

// Update runs a read-write transaction.
func (d *DB) Update(fn func(*Tx) error) error { //nolint:wrapcheck
	return d.inner.Update(func(tx *bbolt.Tx) error { return fn(&Tx{inner: tx}) })
}

// View runs a read-only transaction.
func (d *DB) View(fn func(*Tx) error) error { //nolint:wrapcheck
	return d.inner.View(func(tx *bbolt.Tx) error { return fn(&Tx{inner: tx}) })
}

// CreateBucketIfNotExists creates a bucket in this transaction if missing.
func (t *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) { //nolint:wrapcheck
	b, err := t.inner.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}

	return &Bucket{inner: b}, nil
}

// DeleteBucket deletes a bucket.
func (t *Tx) DeleteBucket(name []byte) error { //nolint:wrapcheck
	return t.inner.DeleteBucket(name)
}

// Bucket retrieves a bucket by name.
func (t *Tx) Bucket(name []byte) *Bucket {
	b := t.inner.Bucket(name)
	if b == nil {
		return nil
	}

	return &Bucket{inner: b}
}

// Get fetches a value by key.
func (b *Bucket) Get(key []byte) []byte {
	if b == nil || b.inner == nil {
		return nil
	}

	return b.inner.Get(key)
}

// Put stores a key/value pair.
func (b *Bucket) Put(key, value []byte) error { //nolint:wrapcheck
	return b.inner.Put(key, value)
}

// Delete removes a key from the bucket.
func (b *Bucket) Delete(key []byte) error { //nolint:wrapcheck
	return b.inner.Delete(key)
}

// ForEach iterates over all key/value pairs in a bucket.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error { //nolint:wrapcheck
	return b.inner.ForEach(fn)
}

// Cursor creates a cursor for iterating over a bucket.
func (b *Bucket) Cursor() *Cursor {
	return &Cursor{inner: b.inner.Cursor()}
}

// First moves the cursor to the first item.
func (c *Cursor) First() (key, value []byte) { return c.inner.First() }

// Next moves the cursor to the next item.
func (c *Cursor) Next() (key, value []byte) { return c.inner.Next() }

// Delete deletes the current key/value pair.
func (c *Cursor) Delete() error { //nolint:wrapcheck
	return c.inner.Delete()
}
