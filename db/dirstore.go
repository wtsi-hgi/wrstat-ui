package db

import (
	"sync"
	"time"
)

// Store is a domain-level interface for persisting DGUTA and children.
type Store interface {
	Close() error

	// PutDGUTAs stores encoded DGUTAs as key/value pairs.
	PutDGUTAs(pairs [][2][]byte) error
	// PutChildren stores encoded children lists as key/value pairs.
	PutChildren(pairs [][2][]byte) error

	// GetDGUTA returns the encoded value for a DGUTA key.
	GetDGUTA(key []byte) ([]byte, error)
	// GetChildren returns the encoded value for a children key.
	GetChildren(key []byte) ([]byte, error)

	// ForEachDGUTA iterates all DGUTA key/value pairs.
	ForEachDGUTA(func(key, value []byte) error) error
	// ForEachChildren iterates all children values.
	ForEachChildren(func(value []byte) error) error
}

// Source represents a backend-agnostic database source (eg. a directory of bolt files,
// or a ClickHouse DSN). Implementations live in backend packages.
type Source interface {
	// ModTime returns the last modification time for the source (used for
	// reporting freshness in summaries). If unknown, return time.Time{}.
	ModTime() time.Time
	// Exists reports whether a database already exists at this source.
	Exists() (bool, error)
}

// Factory constructs Store instances for a given Source.
type Factory interface {
	Create(src Source) (Store, error)
	OpenReadOnly(src Source) (Store, error)
	OpenReadOnlyUnPopulated(src Source) (Store, error)
}

// SourceFactory creates Source objects from path-like inputs. This keeps
// callers agnostic: they can pass a string and the backend turns it into a Source.
type SourceFactory interface {
	FromPath(path string) (Source, error)
}

var (
	regMu                sync.RWMutex
	reg                  = map[string]Factory{}
	defaultName          string
	defaultSourceFactory SourceFactory
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
func Get(name string) (Factory, bool) {
	regMu.RLock()
	defer regMu.RUnlock()
	f, ok := reg[name]
	return f, ok
}

// Default returns the first registered factory, or nil if none.
func Default() Factory {
	regMu.RLock()
	defer regMu.RUnlock()
	if defaultName == "" {
		return nil
	}
	return reg[defaultName]
}

// SetDefaultSourceFactory sets the factory used to translate string inputs
// into Sources.
func SetDefaultSourceFactory(sf SourceFactory) {
	regMu.Lock()
	defer regMu.Unlock()
	defaultSourceFactory = sf
}

// DefaultSourceFactory returns the SourceFactory used for string inputs.
func DefaultSourceFactory() SourceFactory {
	regMu.RLock()
	defer regMu.RUnlock()
	return defaultSourceFactory
}
