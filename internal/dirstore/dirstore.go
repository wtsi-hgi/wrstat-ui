package dirstore

import "sync"

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

// Factory constructs Store instances for given file paths.
type Factory interface {
	Create(dgutaPath, childrenPath string) (Store, error)
	OpenReadOnly(dgutaPath, childrenPath string) (Store, error)
	OpenReadOnlyUnPopulated(dgutaPath, childrenPath string) (Store, error)
}

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
