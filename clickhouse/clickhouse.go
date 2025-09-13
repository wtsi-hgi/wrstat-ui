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
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
)

// Clickhouse provides database operations for the ClickHouse database.
// It encapsulates the third-party ClickHouse client implementation.
type Clickhouse struct {
	conn chdriver.Conn
}

// Close closes the connection to the ClickHouse database.
func (c *Clickhouse) Close() error {
	return c.conn.Close()
}

// ExecuteQuery executes a query that returns a single row and scans the result into dest.
// For queries that don't return results (like CREATE DATABASE), no destination is needed.
func (c *Clickhouse) ExecuteQuery(ctx context.Context, query string, args ...interface{}) error {
	// Check if the last argument is a pointer for scanning results
	var (
		dest      interface{}
		queryArgs []interface{}
	)

	// Default to using all args as query arguments
	queryArgs = args

	// If we have arguments, check if the last one is a pointer for scanning results
	if len(args) > 0 {
		last := args[len(args)-1]

		if isPointer(last) {
			dest = last
			queryArgs = args[:len(args)-1]
		}
	}

	// For DDL statements or queries without result sets
	if dest == nil {
		return c.conn.Exec(ctx, query, queryArgs...)
	}

	// For queries that return results
	row := c.conn.QueryRow(ctx, query, queryArgs...)
	if row.Err() != nil {
		return row.Err()
	}

	return row.Scan(dest)
}

// isPointer checks if an interface value is a pointer.
func isPointer(v interface{}) bool {
	if v == nil {
		return false
	}

	val := reflect.ValueOf(v)

	return val.Kind() == reflect.Ptr
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
	Name       string
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
}

// Filters specifies filtering criteria for ClickHouse queries.
type Filters struct {
	GIDs        []uint32
	UIDs        []uint32
	Exts        []string
	ATimeBucket string // one of: 0d, >1m, >2m, >6m, >1y, >2y, >3y, >5y, >7y
	MTimeBucket string // same set
}

// Common errors.
var (
	ErrInvalidBucket = errors.New("invalid bucket")
)
