package db

import (
	"sync"
	"time"
)

// Store is a domain-level interface for persisting DGUTA and children.
//
// The Store interface abstracts the storage operations needed by the application,
// hiding implementation details of specific database backends. Each backend (e.g., bolt,
// clickhouse) must provide a complete implementation of this interface.
//
// This interface focuses on two primary types of data:
// 1. DGUTAs (Directory, Group, User, Type, Age information) - key/value pairs with directory paths as keys
// 2. Children - directory relationships showing parent-child structure
//
// Implementations must ensure thread safety for concurrent operations.
type Store interface {
	// Close releases resources associated with this store.
	// Should be called when the store is no longer needed.
	Close() error

	// PutDGUTAs stores encoded DGUTAs as key/value pairs.
	// Each pair consists of a key (directory path) and value (encoded DGUTA data).
	// This operation should be atomic for the entire batch of pairs.
	PutDGUTAs(pairs [][2][]byte) error

	// PutChildren stores encoded children lists as key/value pairs.
	// Each pair consists of a key (parent directory path) and value (encoded list of child directories).
	// This operation should be atomic for the entire batch of pairs.
	PutChildren(pairs [][2][]byte) error

	// GetDGUTA returns the encoded value for a DGUTA key.
	// The key represents a directory path. Returns error if the key doesn't exist.
	// The returned value should be decoded using DecodeDGUTAbytes.
	GetDGUTA(key []byte) ([]byte, error)

	// GetChildren returns the encoded value for a children key.
	// The key represents a parent directory path. Returns nil (not error) if the key doesn't exist.
	// The returned value contains an encoded list of child directories.
	GetChildren(key []byte) ([]byte, error)

	// ForEachDGUTA iterates all DGUTA key/value pairs.
	// Implementations should call the provided function for each DGUTA entry.
	// If the callback returns an error, iteration should stop and return that error.
	ForEachDGUTA(fn func(key, value []byte) error) error

	// ForEachChildren iterates all children values.
	// Implementations should call the provided function for each children entry.
	// If the callback returns an error, iteration should stop and return that error.
	ForEachChildren(fn func(value []byte) error) error
}

// Source represents a backend-agnostic database source (eg. a directory of bolt files,
// or a ClickHouse DSN). Implementations live in backend packages.
//
// Each storage backend (e.g., bolt, clickhouse) should provide its own implementation
// of Source that encapsulates the location information and configuration needed to
// access the underlying storage. For example, a bolt implementation might represent
// a filesystem path, while a clickhouse implementation might represent connection details.
//
// This interface allows the rest of the application to work with different database
// backends without knowing their specific implementation details.
type Source interface {
	// Exists reports whether a database already exists at this source.
	// This method should check if the database structure is already initialised
	// at the given source location and is ready to be opened.
	// Should return (false, nil) if the database doesn't exist but there was no error
	// in checking.
	Exists() (bool, error)

	// GetMountTimestamps returns a map of mount points to their last update timestamps.
	// Each mount point identifies a filesystem that the source contains data for.
	//
	// For a bolt implementation with a single mount point, this would typically
	// return a map with a single entry extracted from the directory name.
	// For a ClickHouse implementation that handles multiple mount points in a single
	// source, this would return a map with an entry for each mount point in the database.
	//
	// This method is used both for identifying which mount points the source represents
	// and for determining when the data was last updated.
	GetMountTimestamps() map[string]time.Time
}

// Factory constructs Store instances for a given Source.
// The Factory interface provides methods to create and open Store instances
// for a specific database backend (e.g., bolt, clickhouse). Each backend must
// register a Factory implementation using the Register function.
//
// Factories enable the application to dynamically select which database backend
// to use at runtime without changing application code. They abstract away the
// details of store creation, such as connection establishment, schema initialization,
// and configuration.
type Factory interface {
	// Create creates a new Store at the specified Source with write access.
	// This method should:
	// 1. Initialise the database schema if it doesn't exist
	// 2. Create any necessary tables, buckets, or structures
	// 3. Return a Store instance ready for read/write operations
	// If the Source already exists, the implementation may choose to fail or reuse it.
	Create(src Source) (Store, error)

	// OpenReadOnly opens an existing Store at the specified Source with read-only access.
	// The Source must already exist (Exists() returns true).
	// This method is used for normal operation of the application.
	OpenReadOnly(src Source) (Store, error)

	// OpenReadOnlyUnPopulated opens an existing Store with minimal initialization.
	// This is used primarily for obtaining database statistics and information.
	// The implementation may choose to skip loading indexes or other optimizations
	// that are not needed for basic queries.
	OpenReadOnlyUnPopulated(src Source) (Store, error)
}

//nolint:gochecknoglobals
var (
	regMu       sync.RWMutex
	reg         = map[string]Factory{}
	defaultName string
)

// Register makes a Factory available under a name.
func Register(name string, f Factory) {
	regMu.Lock()
	defer regMu.Unlock()

	reg[name] = f
	if defaultName == "" {
		defaultName = name
	}
}

// Get returns a Factory by name and whether it exists.
func Get(name string) (Factory, bool) { //nolint:ireturn
	regMu.RLock()
	defer regMu.RUnlock()

	f, ok := reg[name]

	return f, ok
}

// Default returns the first registered factory, or nil if none.
func Default() Factory { //nolint:ireturn
	regMu.RLock()
	defer regMu.RUnlock()

	if defaultName == "" {
		return nil
	}

	return reg[defaultName]
}
