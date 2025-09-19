package boltbasedirs

import (
	"os"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	bbolt "github.com/wtsi-hgi/wrstat-ui/bolt"
)

// Store adapts bolt.DB to the basedirs.BasedirsStore interface.
// It lives in a separate package to avoid import cycles.
type Store struct{ db *bbolt.DB }

// New creates a new writable store at the given path.
func New(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0640, &bbolt.Options{ //nolint:mnd
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bbolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// OpenReadOnly opens a read-only store at the given path.
func OpenReadOnly(path string) (*Store, error) {
	db, err := bbolt.Open(path, os.FileMode(0640), &bbolt.Options{ReadOnly: true}) //nolint:mnd
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Update(fn func(basedirs.KVTx) error) error {
	return s.db.Update(func(tx *bbolt.Tx) error { return fn(&txAdapter{tx}) })
}

func (s *Store) View(fn func(basedirs.KVTx) error) error {
	return s.db.View(func(tx *bbolt.Tx) error { return fn(&txAdapter{tx}) })
}

type txAdapter struct{ *bbolt.Tx }

func (t *txAdapter) Put(bucket string, key, value []byte) error {
	if _, err := t.Tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}
	b := t.Tx.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.Put(key, value)
}

func (t *txAdapter) Get(bucket string, key []byte) ([]byte, error) {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	return b.Get(key), nil
}

func (t *txAdapter) ForEach(bucket string, fn func(k, v []byte) error) error {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.ForEach(fn)
}

func (t *txAdapter) Delete(bucket string, key []byte) error {
	b := t.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.Delete(key)
}

func (t *txAdapter) CreateBucketIfNotExists(bucket string) error {
	_, err := t.Tx.CreateBucketIfNotExists([]byte(bucket))
	return err
}

func (t *txAdapter) DeleteBucket(bucket string) error {
	return t.Tx.DeleteBucket([]byte(bucket))
}
