package basedirs

// KVTx represents a transactional key-value accessor scoped by logical bucket names.
// Backends map these to collections/tables as appropriate.
//
// Deprecated: This low-level KV abstraction will be replaced by domain-level
// Reader/Writer interfaces. New backends should target the domain interfaces
// defined below. Existing code will be migrated in a follow-up.
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

// Reader is a domain-level read interface for basedirs data. Implementations
// should provide efficient access to usage, subdirs, history and info without
// exposing buckets or keys.
//
// Note: This is introduced for future migration; current code paths still use
// KVTx via BasedirsStore. Adapters in backend packages will implement this.
type Reader interface {
	// GroupUsage returns usage for every GID-BaseDir for a given age.
	GroupUsage(age uint16) ([]*Usage, error)
	// UserUsage returns usage for every UID-BaseDir for a given age.
	UserUsage(age uint16) ([]*Usage, error)
	// GroupSubDirs returns the subdirectory details for a group's basedir.
	GroupSubDirs(gid uint32, basedir string, age uint16) ([]*SubDir, error)
	// UserSubDirs returns the subdirectory details for a user's basedir.
	UserSubDirs(uid uint32, basedir string, age uint16) ([]*SubDir, error)
}

// Writer is a domain-level write interface covering updates used by summarise
// and post-processing. Methods should be transactional via Store.Update.
type Writer interface {
	// PutGroupUsage stores a Usage record for a gid-basedir-age triple.
	PutGroupUsage(u *Usage) error
	// PutUserUsage stores a Usage record for a uid-basedir-age triple.
	PutUserUsage(u *Usage) error
	// PutGroupSubDirs stores subdir info for a gid-basedir-age triple.
	PutGroupSubDirs(gid uint32, basedir string, age uint16, subs []*SubDir) error
	// PutUserSubDirs stores subdir info for a uid-basedir-age triple.
	PutUserSubDirs(uid uint32, basedir string, age uint16, subs []*SubDir) error
	// PutHistory upserts history entries for a gid and mountpoint.
	PutHistory(gid uint32, mountpoint string, histories []History) error
	// ForEachGroupUsage iterates all group usage records (age==All) to allow
	// precomputations like DateQuotaFull. Return error to abort.
	ForEachGroupUsage(func(*Usage) error) error
}

// Store coordinates domain-level read/write transactions.
type Store interface {
	// Update provides a Writer-backed transaction for modifications.
	Update(func(Writer) error) error
	// View provides a Reader-backed transaction for reads.
	View(func(Reader) error) error
	// Close releases resources.
	Close() error
}
