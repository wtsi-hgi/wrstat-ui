package bolt

import (
	"os"

	bbolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// options mirrors a subset of bbolt.Options used in this project.
type boptions struct {
	NoFreelistSync bool
	NoGrowSync     bool
	FreelistType   bfreelistType
	ReadOnly       bool
}

// freelistType is a minimal enum matching bbolt's freelist types.
type bfreelistType int

const (
	bfreelistArrayType bfreelistType = iota
	bfreelistMapType
)

// MaxKeySize is provided for tests that validate key size errors.
const MaxKeySize = bbolt.MaxKeySize

// ErrBucketNotFound re-exports the specific error value used for logic checks.
var ErrBucketNotFound = berrors.ErrBucketNotFound

// db is our wrapper around bbolt.DB.
type bdb struct {
	inner *bbolt.DB
}

// tx is our wrapper around bbolt.Tx.
type btx struct {
	inner *bbolt.Tx
}

// bucket is our wrapper around bbolt.Bucket.
type bbucket struct {
	inner *bbolt.Bucket
}

// cursor is our wrapper around bbolt.Cursor.
type bcursor struct {
	inner *bbolt.Cursor
}

// open opens or creates a database at the given path.
func open(path string, mode os.FileMode, opt *boptions) (*bdb, error) { //nolint:unparam
	var bopt *bbolt.Options
	if opt != nil {
		bopt = &bbolt.Options{
			NoFreelistSync: opt.NoFreelistSync,
			NoGrowSync:     opt.NoGrowSync,
			ReadOnly:       opt.ReadOnly,
		}
		switch opt.FreelistType {
		case bfreelistArrayType:
			bopt.FreelistType = bbolt.FreelistArrayType
		case bfreelistMapType:
			bopt.FreelistType = bbolt.FreelistMapType
		default:
			// leave zero value
		}
	}

	dbi, err := bbolt.Open(path, mode, bopt)
	if err != nil {
		return nil, err
	}

	return &bdb{inner: dbi}, nil
}

// Close closes the database.
func (d *bdb) Close() error { //nolint:wrapcheck
	if d == nil || d.inner == nil {
		return nil
	}

	return d.inner.Close()
}

// Update runs a read-write transaction.
func (d *bdb) Update(fn func(*btx) error) error { //nolint:wrapcheck
	return d.inner.Update(func(txn *bbolt.Tx) error { return fn(&btx{inner: txn}) })
}

// View runs a read-only transaction.
func (d *bdb) View(fn func(*btx) error) error { //nolint:wrapcheck
	return d.inner.View(func(txn *bbolt.Tx) error { return fn(&btx{inner: txn}) })
}

// CreateBucketIfNotExists creates a bucket in this transaction if missing.
func (t *btx) CreateBucketIfNotExists(name []byte) (*bbucket, error) { //nolint:wrapcheck
	b, err := t.inner.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}

	return &bbucket{inner: b}, nil
}

// DeleteBucket deletes a bucket.
func (t *btx) DeleteBucket(name []byte) error { //nolint:wrapcheck
	return t.inner.DeleteBucket(name)
}

// Bucket retrieves a bucket by name.
func (t *btx) Bucket(name []byte) *bbucket {
	b := t.inner.Bucket(name)
	if b == nil {
		return nil
	}

	return &bbucket{inner: b}
}

// Get fetches a value by key.
func (b *bbucket) Get(key []byte) []byte {
	if b == nil || b.inner == nil {
		return nil
	}

	return b.inner.Get(key)
}

// Put stores a key/value pair.
func (b *bbucket) Put(key, value []byte) error { //nolint:wrapcheck
	return b.inner.Put(key, value)
}

// Delete removes a key from the bucket.
func (b *bbucket) Delete(key []byte) error { //nolint:wrapcheck
	return b.inner.Delete(key)
}

// ForEach iterates over all key/value pairs in a bucket.
func (b *bbucket) ForEach(fn func(k, v []byte) error) error { //nolint:wrapcheck
	return b.inner.ForEach(fn)
}

// Cursor creates a cursor for iterating over a bucket.
func (b *bbucket) Cursor() *bcursor {
	return &bcursor{inner: b.inner.Cursor()}
}

// First moves the cursor to the first item.
func (c *bcursor) First() (key, value []byte) { return c.inner.First() }

// Next moves the cursor to the next item.
func (c *bcursor) Next() (key, value []byte) { return c.inner.Next() }

// Delete deletes the current key/value pair.
func (c *bcursor) Delete() error { //nolint:wrapcheck
	return c.inner.Delete()
}

// ReadBucketKeys is a small public helper for tests that need to read all keys
// from a bucket in a bolt DB without exposing transaction or bucket types.
func ReadBucketKeys(path, bucket string) ([]string, error) {
	rdb, err := open(path, OpenMode, &boptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer rdb.Close()

	var keys []string

	err = rdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))

			return nil
		})
	})

	return keys, err
}
