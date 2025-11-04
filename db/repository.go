package db

// DGUTARepository is a storage-agnostic, domain-oriented interface over DGUTA and children data.
// It hides key/byte-level details behind domain types so non-KV backends can implement it naturally.
type DGUTARepository interface {
	// Close releases resources associated with the repository.
	Close() error

	// WriteDirs stores a batch of directory summaries and their direct-child
	// relationships atomically. Implementations should upsert per-directory
	// DGUTA snapshots and update parent->children mappings accordingly.
	WriteDirs(records []RecordDGUTA) error

	// GetDirSummary returns the DGUTA summary for the given directory path.
	GetDirSummary(dir string) (*DGUTA, error)

	// ListChildren returns the direct child directory paths for the given parent.
	ListChildren(parent string) ([]string, error)

	// GetInfo returns summary information about the repository without
	// materialising all records.
	GetInfo() (*DBInfo, error)
}
