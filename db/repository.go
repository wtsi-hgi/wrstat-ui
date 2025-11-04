package db

// DGUTARepository is a storage-agnostic, domain-oriented interface over DGUTA and children data.
// It hides key/byte-level details behind domain types so non-KV backends can implement it naturally.
type DGUTARepository interface {
	// Close releases resources associated with the repository.
	Close() error

	// PutDGUTARecords stores DGUTA records in batch.
	PutDGUTARecords(records []RecordDGUTA) error

	// PutChildrenMap stores parent->children relationships in batch.
	PutChildrenMap(children map[string][]string) error

	// GetDGUTAByDir returns the DGUTA for the given directory path.
	GetDGUTAByDir(dir string) (*DGUTA, error)

	// GetChildrenByDir returns the direct children of the given parent directory path.
	GetChildrenByDir(parent string) ([]string, error)

	// GetInfo returns summary information about the repository without
	// materialising all records.
	GetInfo() (*DBInfo, error)
}
