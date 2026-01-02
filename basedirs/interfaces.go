/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   GitHub Copilot
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

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

	// Finalize is called once after all Put*/Append calls.
	Finalize() error

	Close() error
}

// Reader is the query interface for basedirs data.
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
