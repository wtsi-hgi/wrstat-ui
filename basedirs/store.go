package basedirs

// Store coordinates domain-level read/write transactions for basedirs data.
// Implementations live in backend packages (eg. bolt, clickhouse).
//
// The Store interface provides transaction-based access to basedirs data,
// enabling consistent reads and writes across different storage backends.
// It follows a common pattern of separating read and write operations
// into distinct transaction types (Reader and Writer).
//
// Each backend must provide an implementation that handles the specifics
// of transaction management, consistency, and data persistence in a way
// appropriate for that storage system.
type Store interface {
	// Update provides a Writer-backed transaction for modifications.
	// The function should execute the provided callback within a transaction,
	// committing changes if the callback returns nil, or rolling back if an error occurs.
	// Any error from the callback or transaction handling should be returned.
	Update(func(Writer) error) error

	// View provides a Reader-backed transaction for reads.
	// The function should execute the provided callback within a read-only transaction.
	// Implementations should optimize for concurrent read access.
	// Any error from the callback or transaction handling should be returned.
	View(func(Reader) error) error

	// Close releases resources associated with this store.
	// This should close any open connections, file handles, or other resources.
	Close() error
}

// Reader is a domain-level read interface for basedirs data. Implementations
// should provide efficient access to usage, subdirs, history and info without
// exposing backend-specific details like buckets or keys.
//
// This interface defines methods to query information about:
// - Group and user storage usage across different time periods (ages)
// - Subdirectory structures and their storage patterns
// - Historical usage patterns for quota predictions
//
// All Reader methods should be safe to call from multiple goroutines.
type Reader interface {
	// GroupUsage returns usage for every GID-BaseDir for a given age.
	// The age parameter filters records to a specific time period.
	// Returns an empty slice if no records match.
	GroupUsage(age uint16) ([]*Usage, error)

	// UserUsage returns usage for every UID-BaseDir for a given age.
	// The age parameter filters records to a specific time period.
	// Returns an empty slice if no records match.
	UserUsage(age uint16) ([]*Usage, error)

	// GroupSubDirs returns the subdirectory details for a group's basedir.
	// This provides drill-down information showing the structure beneath a basedir.
	// The triple (gid, basedir, age) uniquely identifies the record to retrieve.
	// Returns nil if no matching record exists.
	GroupSubDirs(gid uint32, basedir string, age uint16) ([]*SubDir, error)

	// UserSubDirs returns the subdirectory details for a user's basedir.
	// This provides drill-down information showing the structure beneath a basedir.
	// The triple (uid, basedir, age) uniquely identifies the record to retrieve.
	// Returns nil if no matching record exists.
	UserSubDirs(uid uint32, basedir string, age uint16) ([]*SubDir, error)

	// History returns the history for a gid and mountpoint path.
	// This data is used for quota prediction and historical trends.
	// Returns nil if no history exists for the given gid and mountpoint.
	History(gid uint32, path string) ([]History, error)

	// ForEachRaw allows scanning a logical collection for utilities like Info/Clean.
	// The bucket parameter identifies which logical collection to scan.
	// This is primarily for administrative and maintenance operations.
	// The callback receives the raw key and value bytes.
	ForEachRaw(bucket string, fn func(k, v []byte) error) error
}

// Writer is a domain-level write interface covering updates used by summarise
// and post-processing. Methods should be transactional via Store.Update.
//
// This interface defines methods to store and modify:
// - Group and user storage usage
// - Subdirectory structures
// - Historical usage records
//
// Implementations must ensure that all operations within a single Update transaction
// are atomic - either all succeed or all fail.
type Writer interface {
	// PutGroupUsage stores a Usage record for a gid-basedir-age triple.
	// If a record with the same key already exists, it should be overwritten.
	PutGroupUsage(u *Usage) error

	// PutUserUsage stores a Usage record for a uid-basedir-age triple.
	// If a record with the same key already exists, it should be overwritten.
	PutUserUsage(u *Usage) error

	// PutGroupSubDirs stores subdir info for a gid-basedir-age triple.
	// This provides the detailed breakdown of storage within a basedir.
	// If a record with the same key already exists, it should be overwritten.
	PutGroupSubDirs(gid uint32, basedir string, age uint16, subs []*SubDir) error

	// PutUserSubDirs stores subdir info for a uid-basedir-age triple.
	// This provides the detailed breakdown of storage within a basedir.
	// If a record with the same key already exists, it should be overwritten.
	PutUserSubDirs(uid uint32, basedir string, age uint16, subs []*SubDir) error

	// PutHistory upserts history entries for a gid and mountpoint.
	// If history already exists, implementations should merge or replace
	// according to the application's requirements for historical data integrity.
	PutHistory(gid uint32, mountpoint string, histories []History) error

	// ForEachGroupUsage iterates all group usage records (age==All) to allow
	// precomputations like DateQuotaFull. Return error to abort.
	// This is used for aggregate calculations across all group records.
	ForEachGroupUsage(func(*Usage) error) error

	// History returns the history for a gid and mountpoint path (within write txn).
	// This allows reading history data within a write transaction for atomic update patterns.
	History(gid uint32, path string) ([]History, error)

	// EnsureHistoryBucket ensures the history collection exists.
	// This should create the collection if it doesn't exist, or do nothing if it does.
	EnsureHistoryBucket() error

	// PutRawHistory writes a raw history key/value (used for copying from another store).
	// This is a low-level operation primarily used for migration and admin functions.
	PutRawHistory(key, value []byte) error

	// DeleteHistoryKey deletes a raw key from the history collection (used for cleaning).
	// This is a low-level operation primarily used for maintenance and cleanup.
	DeleteHistoryKey(key []byte) error
}
