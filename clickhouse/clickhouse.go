/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

// Package clickhouse provides ClickHouse database integration for storing and
// querying file statistics data for the wrstat-ui application.
package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
)

// Clickhouse provides database operations for the ClickHouse database.
// It encapsulates the third-party ClickHouse client implementation.
type Clickhouse struct {
	conn chdriver.Conn
	// cachedMounts stores normalised mount paths (with trailing slash),
	// sorted longest-first for efficient prefix matching.
	cachedMounts []string
	// params stores the connection parameters used to construct this instance.
	params ConnectionParams
}

// Close closes the connection to the ClickHouse database.
func (c *Clickhouse) Close() error {
	return c.conn.Close()
}

// Params returns the connection parameters used to create this client.
// Callers can reuse these to create additional independent connections
// to the same database, or to extract the database name.
func (c *Clickhouse) Params() ConnectionParams {
	return c.params
}

// ExecuteQuery executes a query that returns a single row and scans the result into dest.
// For queries that don't return results (like CREATE DATABASE), no destination is needed.
func (c *Clickhouse) ExecuteQuery(ctx context.Context, query string, args ...interface{}) error {
	// If the last argument is a pointer for scanning results, separate it from query args
	if len(args) > 0 {
		last := args[len(args)-1]
		if last != nil && reflect.ValueOf(last).Kind() == reflect.Ptr {
			// This is a scan destination
			queryArgs := args[:len(args)-1]

			row := c.conn.QueryRow(ctx, query, queryArgs...)
			if row.Err() != nil {
				return row.Err()
			}

			return row.Scan(last)
		}
	}

	// For DDL statements or queries without result sets
	return c.conn.Exec(ctx, query, args...)
}

// CHBatch defines the interface for a ClickHouse batch operation.
type CHBatch interface {
	Append(values ...any) error
	Send() error
}

// Constants for ClickHouse connection and processing.
const (
	// Lower batch sizes to balance performance and memory usage.
	FileBatchSize   = 100_000
	RollupBatchSize = 100_000

	// ClickHouse connection settings.
	DialTimeoutSeconds  = 10
	MaxInsertBlockSize  = 1000000
	MinInsertBlockRows  = 100000
	MinInsertBlockBytes = 10485760 // 10MB

	// Default capacity for result slices.
	DefaultResultCapacity = 100
)

// FileType represents the type of a file system entry.
type FileType uint8

const (
	FileTypeUnknown FileType = iota
	FileTypeFile
	FileTypeDir
	FileTypeSymlink
	FileTypeDevice
	FileTypePipe
	FileTypeSocket
	FileTypeChar
)

// FileEntry represents a file or directory entry in the ClickHouse database.
type FileEntry struct {
	Path       string
	ParentPath string
	Basename   string
	Depth      uint16
	Ext        string
	FType      uint8
	INode      uint64
	Size       uint64
	UID        uint32
	GID        uint32
	MTime      time.Time
	ATime      time.Time
	CTime      time.Time
}

// Summary contains aggregated statistics for a subtree or query result.
type Summary struct {
	TotalSize       uint64
	FileCount       uint64
	MostRecentATime time.Time
	OldestATime     time.Time
	MostRecentMTime time.Time
	OldestMTime     time.Time
	UIDs            []uint32
	GIDs            []uint32
	Exts            []string
	FTypes          []uint8 // File types for Phase 1 - correspond to db.DirGUTAFileType constants
	Age             uint8   // Age bucket for Phase 1 - corresponds to db.DirGUTAge constants
}

// Filters specifies filtering criteria for ClickHouse queries.
type Filters struct {
	GIDs        []uint32
	UIDs        []uint32
	Exts        []string
	ATimeBucket string // one of: 0d, >1m, >2m, >6m, >1y, >2y, >3y, >5y, >7y
	MTimeBucket string // same set
}

// ChildSummary represents a child directory with its summary statistics.
type ChildSummary struct {
	Path    string
	Summary Summary
}

// Common errors.
var (
	ErrInvalidBucket = errors.New("invalid bucket")
)

// ensureMounts populates the cached mount paths if empty.
func (c *Clickhouse) ensureMounts(ctx context.Context) error {
	if len(c.cachedMounts) > 0 {
		return nil
	}

	return c.refreshMounts(ctx)
}

// refreshMounts queries ClickHouse for known mount paths and caches them.
// It pulls distinct mount_path values from ready scans and sorts them
// longest-first for deterministic prefix matching.
func (c *Clickhouse) refreshMounts(ctx context.Context) error {
	rows, err := c.conn.Query(ctx, `
		SELECT DISTINCT mount_path
		FROM scans
		WHERE state = 'ready'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	mounts := make([]string, 0)
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return err
		}

		mounts = append(mounts, NormalizeMount(m))
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Sort by length desc so the most specific mount matches first
	sort.Slice(mounts, func(i, j int) bool { return len(mounts[i]) > len(mounts[j]) })

	c.cachedMounts = mounts

	return nil
}

// findMountForPrefix returns the mount that contains the given absolute path prefix.
// It assumes cachedMounts is populated and normalised; returns "" if none match.
func (c *Clickhouse) findMountForPrefix(prefix string) string {
	p := NormalizeMount(prefix)
	for _, m := range c.cachedMounts {
		if strings.HasPrefix(p, m) {
			return m
		}
	}

	return ""
}
