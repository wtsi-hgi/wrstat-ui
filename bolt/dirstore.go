package bolt

import (
	"errors"
	"strings"

	"github.com/ugorji/go/codec"
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
	ch  codec.Handle
}

const endByte = 255

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

// DGUTARepository implementation

func (s *boltStore) PutDGUTARecords(records []db.RecordDGUTA) error {
	if s.ro {
		return ErrReadOnlyStore
	}

	return s.gdb.Update(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))
		for i := range records {
			k, v := records[i].EncodeToBytes()
			if err := b.Put(k, v); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *boltStore) PutChildrenMap(children map[string][]string) error {
	if s.ro {
		return ErrReadOnlyStore
	}

	return s.cdb.Update(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))
		for parent, list := range children {
			key := []byte(parent)
			if !strings.HasSuffix(parent, "/") {
				key = append(key, '/')
			}

			val := s.encodeChildren(list)
			if err := b.Put(key, val); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *boltStore) GetDGUTAByDir(dir string) (*db.DGUTA, error) {
	var out *db.DGUTA

	err := s.gdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))

		key := []byte(dir)
		if !strings.HasSuffix(dir, "/") {
			key = append(key, '/')
		}
		key = append(key, endByte)

		v := b.Get(key)
		if v == nil {
			return db.ErrDirNotFound
		}

		out = db.DecodeDGUTAbytes(key, v)

		return nil
	})

	return out, err
}

func (s *boltStore) GetChildrenByDir(parent string) ([]string, error) {
	var out []string
	err := s.cdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))

		key := []byte(parent)
		if !strings.HasSuffix(parent, "/") {
			key = append(key, '/')
		}

		v := b.Get(key)
		if v == nil {
			out = []string{}
			return nil
		}

		out = s.decodeChildrenBytes(v)

		return nil
	})

	return out, err
}

func (s *boltStore) ForEachDGUTA(fn func(*db.DGUTA) error) error {
	return s.gdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("gut"))

		return b.ForEach(func(k, v []byte) error {
			dg := db.DecodeDGUTAbytes(k, v)
			return fn(dg)
		})
	})
}

func (s *boltStore) ForEachChildren(fn func(children []string) error) error {
	return s.cdb.View(func(tx *btx) error {
		b := tx.Bucket([]byte("children"))

		return b.ForEach(func(_, v []byte) error {
			children := s.decodeChildrenBytes(v)
			return fn(children)
		})
	})
}

func (s *boltStore) encodeChildren(dirs []string) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, s.ch)
	enc.MustEncode(dirs)
	return encoded
}

func (s *boltStore) decodeChildrenBytes(encoded []byte) []string {
	dec := codec.NewDecoderBytes(encoded, s.ch)
	var children []string
	dec.MustDecode(&children)
	return children
}

// boltFactory implements dirstore.Factory backed by bolt DBs.
type boltFactory struct{}

func (boltFactory) Create(src db.Source) (db.DGUTARepository, error) {
	ds, ok := src.(*DirSource)
	if !ok {
		return nil, ErrUnsupportedSourceType
	}

	gdb, cdb, err := openStorePair(ds, false)
	if err != nil {
		return nil, err
	}

	return &boltStore{gdb: gdb, cdb: cdb, ro: false, ch: new(codec.BincHandle)}, nil
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

func (boltFactory) OpenReadOnly(src db.Source) (db.DGUTARepository, error) {
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

	return &boltStore{gdb: gdb, cdb: cdb, ro: true, ch: new(codec.BincHandle)}, nil
}

func (boltFactory) OpenReadOnlyUnPopulated(src db.Source) (db.DGUTARepository, error) {
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
