package bolt

import (
	"os"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

// Basedirs is a Bolt-backed implementation of basedirs.BasedirsStore.
// Use NewBasedirs for writable and OpenReadOnlyBasedirs for read-only access.
type Basedirs struct{ db *DB }

// NewBasedirs creates a new writable basedirs store at the given path.
func NewBasedirs(path string) (*Basedirs, error) {
	db, err := Open(path, 0640, &Options{ //nolint:mnd
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	return &Basedirs{db: db}, nil
}

// OpenReadOnlyBasedirs opens a read-only basedirs store at the given path.
func OpenReadOnlyBasedirs(path string) (*Basedirs, error) {
	db, err := Open(path, os.FileMode(0640), &Options{ReadOnly: true}) //nolint:mnd
	if err != nil {
		return nil, err
	}
	return &Basedirs{db: db}, nil
}

// Close closes the underlying database.
func (s *Basedirs) Close() error { return s.db.Close() }

// Update runs a read-write transaction using the basedirs KVTx adapter.
func (s *Basedirs) Update(fn func(basedirs.KVTx) error) error {
	return s.db.Update(func(tx *Tx) error { return fn(&bdTx{tx}) })
}

// View runs a read-only transaction using the basedirs KVTx adapter.
func (s *Basedirs) View(fn func(basedirs.KVTx) error) error {
	return s.db.View(func(tx *Tx) error { return fn(&bdTx{tx}) })
}

// bdTx adapts bolt.Tx to the basedirs.KVTx interface.
type bdTx struct{ *Tx }

func (t *bdTx) Put(bucket string, key, value []byte) error {
	if _, err := t.Tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}
	b := t.Tx.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.Put(key, value)
}

func (t *bdTx) Get(bucket string, key []byte) ([]byte, error) {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	return b.Get(key), nil
}

func (t *bdTx) ForEach(bucket string, fn func(k, v []byte) error) error {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.ForEach(fn)
}

func (t *bdTx) Delete(bucket string, key []byte) error {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.Delete(key)
}

func (t *bdTx) CreateBucketIfNotExists(bucket string) error {
	_, err := t.Tx.CreateBucketIfNotExists([]byte(bucket))
	return err
}

func (t *bdTx) DeleteBucket(bucket string) error {
	return t.Tx.DeleteBucket([]byte(bucket))
}
