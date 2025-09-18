package bolt

import (
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

// boltFactory implements dirstore.Factory backed by bolt DBs.
type boltFactory struct{}

// NewDirStoreFactory returns a dirstore.Factory backed by bolt.
func NewDirStoreFactory() db.Factory { //nolint:ireturn
	return &boltFactory{}
}

func init() { db.Register("bolt", NewDirStoreFactory()) }

func (boltFactory) Create(src db.Source) (db.Store, error) { //nolint:ireturn
	ds, ok := src.(*DirSource)
	if !ok {
		return nil, errors.New("unsupported source type for bolt")
	}
	gdb, err := Open(ds.dgutaPath(), OpenMode, &Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	cdb, err := Open(ds.childrenPath(), OpenMode, &Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   FreelistMapType,
	})
	if err != nil {
		_ = gdb.Close()
		return nil, err
	}
	// ensure buckets
	if err := gdb.Update(func(tx *Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte("gut"))
		return e
	}); err != nil {
		_ = gdb.Close()
		_ = cdb.Close()
		return nil, err
	}
	if err := cdb.Update(func(tx *Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte("children"))
		return e
	}); err != nil {
		_ = gdb.Close()
		_ = cdb.Close()
		return nil, err
	}
	return &boltStore{gdb: gdb, cdb: cdb, ro: false}, nil
}

func (boltFactory) OpenReadOnly(src db.Source) (db.Store, error) { //nolint:ireturn
	ds, ok := src.(*DirSource)
	if !ok {
		return nil, errors.New("unsupported source type for bolt")
	}
	gdb, err := Open(ds.dgutaPath(), OpenMode, &Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	cdb, err := Open(ds.childrenPath(), OpenMode, &Options{ReadOnly: true})
	if err != nil {
		_ = gdb.Close()
		return nil, err
	}
	return &boltStore{gdb: gdb, cdb: cdb, ro: true}, nil
}

func (boltFactory) OpenReadOnlyUnPopulated(src db.Source) (db.Store, error) { //nolint:ireturn
	return (&boltFactory{}).OpenReadOnly(src)
}

type boltStore struct {
	gdb *DB
	cdb *DB
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
		return errors.New("read-only store")
	}
	return s.gdb.Update(func(tx *Tx) error {
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
		return errors.New("read-only store")
	}
	return s.cdb.Update(func(tx *Tx) error {
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
	err := s.gdb.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("gut"))
		v := b.Get(key)
		if v == nil {
			return errors.New("not found")
		}
		out = append([]byte(nil), v...)
		return nil
	})
	return out, err
}

func (s *boltStore) GetChildren(key []byte) ([]byte, error) {
	var out []byte
	err := s.cdb.View(func(tx *Tx) error {
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
	return s.gdb.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("gut"))
		return b.ForEach(func(k, v []byte) error { return fn(k, v) })
	})
}

func (s *boltStore) ForEachChildren(fn func(value []byte) error) error {
	return s.cdb.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("children"))
		return b.ForEach(func(_k, v []byte) error { return fn(v) })
	})
}
