package db

import (
	bolt "github.com/wtsi-hgi/wrstat-ui/bolt"
)

// KVDB is the minimal key/value database interface used by this package.
type KVDB interface {
	Update(func(Tx) error) error
	View(func(Tx) error) error
	Close() error
}

// Tx is the minimal transaction interface used by this package.
type Tx interface {
	CreateBucketIfNotExists(name []byte) (Bucket, error)
	DeleteBucket(name []byte) error
	Bucket(name []byte) Bucket
}

// Bucket is the minimal bucket interface used by this package.
type Bucket interface {
	Get(key []byte) []byte
	Put(key, value []byte) error
	ForEach(func(k, v []byte) error) error
}

// StorageFactory opens databases with the required modes used by this package.
type StorageFactory interface {
	OpenWritable(path, createBucket string) (KVDB, error)
	OpenReadOnly(path string) (KVDB, error)
	OpenReadOnlyUnPopulated(path string) (KVDB, error)
}

// storageFactory is the package-level factory used to open databases.
// It defaults to a bolt-backed implementation.
var storageFactory StorageFactory = &boltStorage{}

// SetStorageFactory allows callers (eg. main/cmd) to override the storage backend.
func SetStorageFactory(f StorageFactory) { //nolint:revive
	if f != nil {
		storageFactory = f
	}
}

// boltStorage implements StorageFactory using the bolt wrapper package.
type boltStorage struct{}

func (boltStorage) OpenWritable(path, createBucket string) (KVDB, error) {
	db, err := bolt.Open(path, DBOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	// Ensure bucket exists
	if err := db.Update(func(tx *bolt.Tx) error {
		_, errc := tx.CreateBucketIfNotExists([]byte(createBucket))
		return errc
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &kvdbBolt{db: db}, nil
}

func (boltStorage) OpenReadOnly(path string) (KVDB, error) {
	db, err := bolt.Open(path, DBOpenMode, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &kvdbBolt{db: db}, nil
}

func (boltStorage) OpenReadOnlyUnPopulated(path string) (KVDB, error) {
	// For bolt, this is the same as OpenReadOnly
	return (&boltStorage{}).OpenReadOnly(path)
}

// kvdbBolt adapts *bolt.DB to KVDB
type kvdbBolt struct{ db *bolt.DB }

func (k *kvdbBolt) Update(fn func(Tx) error) error {
	return k.db.Update(func(tx *bolt.Tx) error { return fn(&txBolt{tx: tx}) })
}

func (k *kvdbBolt) View(fn func(Tx) error) error {
	return k.db.View(func(tx *bolt.Tx) error { return fn(&txBolt{tx: tx}) })
}

func (k *kvdbBolt) Close() error { return k.db.Close() }

// txBolt adapts *bolt.Tx to Tx
type txBolt struct{ tx *bolt.Tx }

func (t *txBolt) CreateBucketIfNotExists(name []byte) (Bucket, error) {
	b, err := t.tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return &bucketBolt{b: b}, nil
}

func (t *txBolt) DeleteBucket(name []byte) error { return t.tx.DeleteBucket(name) }

func (t *txBolt) Bucket(name []byte) Bucket { return &bucketBolt{b: t.tx.Bucket(name)} }

// bucketBolt adapts *bolt.Bucket to Bucket
type bucketBolt struct{ b *bolt.Bucket }

func (b *bucketBolt) Get(key []byte) []byte { return b.b.Get(key) }

func (b *bucketBolt) Put(key, value []byte) error { return b.b.Put(key, value) }

func (b *bucketBolt) ForEach(fn func(k, v []byte) error) error { return b.b.ForEach(fn) }
