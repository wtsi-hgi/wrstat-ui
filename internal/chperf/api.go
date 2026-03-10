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

package chperf

import (
	"context"
	"io"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/provider"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// ImportAPI creates the storage-backed writers required by Import.
type ImportAPI interface {
	NewDGUTAWriter() (db.DGUTAWriter, error)
	NewFileIngestOperation(mountPath string, updatedAt time.Time) (summary.OperationGenerator, io.Closer, error)
	NewBaseDirsStore() (basedirs.Store, error)
}

// QueryRow captures the file metadata fields used by the perf harness.
type QueryRow struct {
	Path      string
	Ext       string
	EntryType byte
}

// QueryClient wraps the ClickHouse file queries used by the perf harness.
type QueryClient interface {
	ListDir(ctx context.Context, dir string, limit int64) ([]QueryRow, error)
	StatPath(ctx context.Context, path string) error
	PermissionAnyInDir(ctx context.Context, dir string, uid uint32, gids []uint32) error
	FindByGlob(
		ctx context.Context,
		baseDirs []string,
		patterns []string,
		requireOwner bool,
		uid uint32,
		gids []uint32,
	) error
	Close() error
}

// QueryMetrics captures the query log metrics printed by the perf harness.
type QueryMetrics struct {
	DurationMs  uint64
	ReadRows    uint64
	ReadBytes   uint64
	ResultRows  uint64
	ResultBytes uint64
}

// QueryInspector exposes EXPLAIN and per-query metrics for the perf harness.
type QueryInspector interface {
	ExplainListDir(ctx context.Context, mountPath, dir string, limit, offset int64) (string, error)
	ExplainStatPath(ctx context.Context, mountPath, path string) (string, error)
	Measure(ctx context.Context, run func(ctx context.Context) error) (*QueryMetrics, error)
	Close() error
}

// QueryAPI creates the storage-backed readers required by Query.
type QueryAPI interface {
	OpenProvider() (provider.Provider, error)
	NewQueryClient() (QueryClient, error)
	NewQueryInspector() (QueryInspector, error)
}
