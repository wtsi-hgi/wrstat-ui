package basedirs

import (
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

// SubDirKey identifies a subdir entry.
type SubDirKey struct {
	ID      uint32 // GID or UID
	BaseDir string
	Age     db.DirGUTAge
}

// HistoryKey identifies a history series.
// MountPath MUST be an absolute mount directory path ending with '/'.
type HistoryKey struct {
	GID       uint32
	MountPath string
}

// Store is the storage backend interface for basedirs persistence.
//
// This interface is domain-oriented (no buckets/tx/cursors/[]byte), and is
// designed so BaseDirs.Output() can stream its writes without materialising
// extra large slices.
type Store interface {
	// SetMountPath sets the mount directory path for this dataset.
	// MUST be called before any write methods.
	SetMountPath(mountPath string)

	// SetUpdatedAt sets the dataset snapshot time (stats.gz mtime).
	// MUST be called before any write methods.
	SetUpdatedAt(updatedAt time.Time)

	// Reset prepares the destination for a new summarise run for this mount.
	Reset() error

	PutGroupUsage(u *Usage) error
	PutUserUsage(u *Usage) error
	PutGroupSubDirs(key SubDirKey, subdirs []*SubDir) error
	PutUserSubDirs(key SubDirKey, subdirs []*SubDir) error

	// AppendGroupHistory applies the append-only rule for histories.
	AppendGroupHistory(key HistoryKey, point History) error

	//nolint:misspell // Finalize spelling follows interface_spec
	// Finalize is called once after all Put*/Append calls.
	Finalize() error

	Close() error
}

// Reader is the query interface for basedirs data.
//
//nolint:interfacebloat
type Reader interface {
	GroupUsage(age db.DirGUTAge) ([]*Usage, error)
	UserUsage(age db.DirGUTAge) ([]*Usage, error)

	GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error)
	UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error)

	History(gid uint32, path string) ([]History, error)
	SetMountPoints(mountpoints []string)
	SetCachedGroup(gid uint32, name string)
	SetCachedUser(uid uint32, name string)

	Info() (*DBInfo, error)
	MountTimestamps() (map[string]time.Time, error)

	Close() error
}

// HistoryIssue describes a history entry that would be considered invalid for
// a given mount-path prefix.
type HistoryIssue struct {
	GID       uint32
	MountPath string
}

// HistoryMaintainer defines storage-neutral maintenance operations over
// basedirs history.
type HistoryMaintainer interface {
	CleanHistoryForMount(prefix string) error
	FindInvalidHistory(prefix string) ([]HistoryIssue, error)
}
