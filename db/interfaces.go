package db

import "time"

// Database is the storage interface that Tree uses internally.
//
// Implementations MUST NOT expose Bolt concepts (tx/bucket/cursor) or []byte
// values.
type Database interface {
	// DirInfo returns the directory summary for dir, after applying filter.
	//
	// It MUST preserve multi-source semantics:
	// - return ErrDirNotFound only if dir is missing from all sources
	// - merge GUTA state across sources before applying the filter
	// - set DirSummary.Modtime to the latest dataset updatedAt across sources
	DirInfo(dir string, filter *Filter) (*DirSummary, error)

	// Children returns the immediate child directory paths for dir.
	//
	// It MUST de-duplicate and sort children across all sources.
	// It MUST return nil/empty if no children exist (leaf or missing dir).
	Children(dir string) ([]string, error)

	// Info returns summary information about the database (e.g. counts).
	// Used by cmd/dbinfo.
	Info() (*DBInfo, error)

	Close() error
}

// DGUTAWriter is the full interface for writing DGUTA data.
//
// cmd/summarise uses this to configure the writer before passing it to
// summary/dirguta (which only uses the Add method).
type DGUTAWriter interface {
	// Add adds a DGUTA record to the database.
	Add(dguta RecordDGUTA) error

	// SetBatchSize controls flush batching.
	SetBatchSize(batchSize int)

	// SetMountPath sets the mount directory path for this dataset.
	// MUST be called before Add(). The path must be absolute and end with '/'.
	SetMountPath(mountPath string)

	// SetUpdatedAt sets the dataset snapshot time (typically from stats.gz mtime).
	// MUST be called before Add().
	SetUpdatedAt(updatedAt time.Time)

	Close() error
}
