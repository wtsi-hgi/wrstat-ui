package db

import (
	"sync"
	"time"
)

// Factory constructs repositories for a given Source.
// Each backend registers a Factory implementation using Register.
type Factory interface {
	// Create creates a writable repository at the Source.
	Create(src Source) (DGUTARepository, error)

	// OpenReadOnly opens an existing repository read-only.
	OpenReadOnly(src Source) (DGUTARepository, error)

	// OpenReadOnlyUnPopulated opens an existing repository with minimal initialization
	// for lightweight info queries.
	OpenReadOnlyUnPopulated(src Source) (DGUTARepository, error)
}

//nolint:gochecknoglobals
var (
	regMu       sync.RWMutex
	reg         = map[string]Factory{}
	defaultName string
)

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

// Get returns a Factory by name and whether it exists.
// It returns the Factory interface so callers don't depend on concrete types.
func Get(name string) (Factory, bool) {
	regMu.RLock()
	defer regMu.RUnlock()

	f, ok := reg[name]

	return f, ok
}

// Default returns the first registered factory, or nil if none.
// It intentionally returns the Factory interface.
func Default() Factory {
	regMu.RLock()
	defer regMu.RUnlock()

	if defaultName == "" {
		return nil
	}

	return reg[defaultName]
}

// Register makes a Factory available under a name.
func Register(name string, f Factory) {
	regMu.Lock()
	defer regMu.Unlock()

	reg[name] = f
	if defaultName == "" {
		defaultName = name
	}
}
