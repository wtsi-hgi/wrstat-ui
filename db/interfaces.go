/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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
	Info() (*Info, error)

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
