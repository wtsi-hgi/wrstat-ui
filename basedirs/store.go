package basedirs

// KVTx represents a transactional key-value accessor scoped by logical bucket names.
// Backends map these to collections/tables as appropriate.
type KVTx interface {
	// Put stores a value for a key within a logical bucket.
	Put(bucket string, key, value []byte) error
	// Get retrieves a value for a key within a logical bucket.
	Get(bucket string, key []byte) ([]byte, error)
	// ForEach iterates all key/value pairs within a logical bucket.
	ForEach(bucket string, fn func(k, v []byte) error) error
	// Delete removes a key within a logical bucket.
	Delete(bucket string, key []byte) error
	// CreateBucketIfNotExists ensures a logical bucket exists.
	CreateBucketIfNotExists(bucket string) error
	// DeleteBucket removes a logical bucket if it exists.
	DeleteBucket(bucket string) error
}

// BasedirsStore abstracts storage for basedirs data independent of any backend.
// Implementations live in backend packages (eg. bolt, clickhouse).
type BasedirsStore interface {
	// Update performs a read-write transaction.
	Update(func(KVTx) error) error
	// View performs a read-only transaction.
	View(func(KVTx) error) error
	// Close releases any resources.
	Close() error
}
