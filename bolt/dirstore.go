package bolt

import (
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

var (
	// ErrUnsupportedSourceType indicates the provided db.Source isn't a bolt.DirSource.
	ErrUnsupportedSourceType = errors.New("unsupported source type for bolt")
	// ErrReadOnlyStore is returned when attempting to write to a read-only store.
	ErrReadOnlyStore = errors.New("read-only store")
	// ErrNotFound indicates a key was not found in the store.
	ErrNotFound = errors.New("not found")
)

type boltStore struct {
	gdb *bdb
	cdb *bdb
	ro  bool
}

func (s *boltStore) Close() error {
	var err error
	if e := s.gdb.Close(); e != nil {
		err = e
	}

	if e := s.cdb.Close(); e != nil && err == nil {
		err = e
	}

	return err
}

func (s *boltStore) PutDGUTAs(pairs [][2][]byte) error {
	if s.ro {
		return ErrReadOnlyStore
	}

	return s.gdb.Update(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))
		for _, kv := range pairs {
			if err := b.Put(kv[0], kv[1]); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *boltStore) PutChildren(pairs [][2][]byte) error {
	if s.ro {
		return ErrReadOnlyStore
	}

	return s.cdb.Update(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))
		for _, kv := range pairs {
			if err := b.Put(kv[0], kv[1]); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *boltStore) GetDGUTA(key []byte) ([]byte, error) {
	var out []byte

	err := s.gdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))

		v := b.Get(key)
		if v == nil {
			return ErrNotFound
		}

		out = append([]byte(nil), v...)

		return nil
	})

	return out, err
}

func (s *boltStore) GetChildren(key []byte) ([]byte, error) {
	var out []byte

	err := s.cdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))

		v := b.Get(key)
		if v == nil {
			out = nil

			return nil
		}

		out = append([]byte(nil), v...)

		return nil
	})

	return out, err
}

func (s *boltStore) ForEachDGUTA(fn func(key, value []byte) error) error {
	return s.gdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))

		return b.ForEach(func(k, v []byte) error { return fn(k, v) })
	})
}

func (s *boltStore) ForEachChildren(fn func(value []byte) error) error {
	return s.cdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))

		return b.ForEach(func(_, v []byte) error { return fn(v) })
	})
}

// boltFactory implements dirstore.Factory backed by bolt DBs.
type boltFactory struct{}

func (boltFactory) Create(src db.Source) (db.Store, error) {
	ds, ok := src.(*DirSource)
	if !ok {
		return nil, ErrUnsupportedSourceType
	}

	gdb, cdb, err := openStorePair(ds, false)
	if err != nil {
		return nil, err
	}

	return &boltStore{gdb: gdb, cdb: cdb, ro: false}, nil
}

// openStorePair opens the dguta and children DBs for the given DirSource and
// ensures required buckets exist when opening writable DBs.
func openStorePair(ds *DirSource, readOnly bool) (*bdb, *bdb, error) {
	gdb, err := openDBForPath(ds.dgutaPath(), readOnly)
	if err != nil {
		return nil, nil, err
	}

	cdb, err := openDBForPath(ds.childrenPath(), readOnly)
	if err != nil {
		_ = gdb.Close()

		return nil, nil, err
	}

	if !readOnly {
		if err := ensureRequiredBuckets(gdb, cdb); err != nil {
			_ = gdb.Close()
			_ = cdb.Close()

			return nil, nil, err
		}
	}

	return gdb, cdb, nil
}

func (boltFactory) OpenReadOnly(src db.Source) (db.Store, error) {
	ds, ok := src.(*DirSource)
	if !ok {
		return nil, ErrUnsupportedSourceType
	}

	gdb, err := open(ds.dgutaPath(), OpenMode, &boptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	cdb, err := open(ds.childrenPath(), OpenMode, &boptions{ReadOnly: true})
	if err != nil {
		_ = gdb.Close()

		return nil, err
	}

	return &boltStore{gdb: gdb, cdb: cdb, ro: true}, nil
}

func (boltFactory) OpenReadOnlyUnPopulated(src db.Source) (db.Store, error) {
	return (&boltFactory{}).OpenReadOnly(src)
}

// NewDirStoreFactory returns a dirstore.Factory backed by bolt.
// NewDirStoreFactory returns a dirstore.Factory backed by bolt.
// It returns the interface type to avoid exposing the concrete factory type.
func NewDirStoreFactory() db.Factory {
	return &boltFactory{}
}

//nolint:gochecknoinits
func init() { db.Register("bolt", NewDirStoreFactory()) }

// ensureRequiredBuckets ensures the expected buckets exist in both DBs.
func ensureRequiredBuckets(gdb, cdb *bdb) error {
	if err := ensureBucketExists(gdb, "gut"); err != nil {
		return err
	}

	if err := ensureBucketExists(cdb, "children"); err != nil {
		return err
	}

	return nil
}

func ensureBucketExists(db *bdb, name string) error {
	return db.Update(func(tx *btx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(name))

		return e
	})
}

// openDBForPath prepares options and opens a DB at the given path.
func openDBForPath(path string, readOnly bool) (*bdb, error) {
	opts := &boptions{ReadOnly: readOnly}
	if !readOnly {
		opts.NoFreelistSync = true
		opts.NoGrowSync = true
		opts.FreelistType = bfreelistMapType
	}

	return open(path, OpenMode, opts)
}
