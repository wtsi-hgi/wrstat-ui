package basedirs

// Store coordinates domain-level read/write transactions.
// Implementations live in backend packages (eg. bolt, clickhouse).
type Store interface {
	// Update provides a Writer-backed transaction for modifications.
	Update(func(Writer) error) error
	// View provides a Reader-backed transaction for reads.
	View(func(Reader) error) error
	// Close releases resources.
	Close() error
}

// Reader is a domain-level read interface for basedirs data. Implementations
// should provide efficient access to usage, subdirs, history and info without
// exposing buckets or keys.
type Reader interface {
	// GroupUsage returns usage for every GID-BaseDir for a given age.
	GroupUsage(age uint16) ([]*Usage, error)
	// UserUsage returns usage for every UID-BaseDir for a given age.
	UserUsage(age uint16) ([]*Usage, error)
	// GroupSubDirs returns the subdirectory details for a group's basedir.
	GroupSubDirs(gid uint32, basedir string, age uint16) ([]*SubDir, error)
	// UserSubDirs returns the subdirectory details for a user's basedir.
	UserSubDirs(uid uint32, basedir string, age uint16) ([]*SubDir, error)
	// History returns the history for a gid and mountpoint path.
	History(gid uint32, path string) ([]History, error)
	// ForEachRaw allows scanning a logical collection for utilities like Info/Clean.
	ForEachRaw(bucket string, fn func(k, v []byte) error) error
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
	// History returns the history for a gid and mountpoint path (within write txn).
	History(gid uint32, path string) ([]History, error)
	// EnsureHistoryBucket ensures the history collection exists.
	EnsureHistoryBucket() error
	// PutRawHistory writes a raw history key/value (used for copying from another store).
	PutRawHistory(key, value []byte) error
	// DeleteHistoryKey deletes a raw key from the history collection (used for cleaning).
	DeleteHistoryKey(key []byte) error
}
