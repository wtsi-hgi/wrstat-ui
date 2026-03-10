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
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/provider"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var _ ImportAPI = (*ClickHouseAPI)(nil)

var _ QueryAPI = (*ClickHouseAPI)(nil)

type queryClientAdapter struct {
	client *clickhouse.Client
}

func (c queryClientAdapter) ListDir(
	ctx context.Context,
	dir string,
	limit int64,
) ([]QueryRow, error) {
	rows, err := c.client.ListDir(ctx, dir, clickhouse.ListOptions{Limit: limit})
	if err != nil {
		return nil, err
	}

	converted := make([]QueryRow, len(rows))
	for i, row := range rows {
		converted[i] = QueryRow{
			Path:      row.Path,
			Ext:       row.Ext,
			EntryType: row.EntryType,
		}
	}

	return converted, nil
}

func (c queryClientAdapter) StatPath(ctx context.Context, path string) error {
	_, err := c.client.StatPath(ctx, path, clickhouse.StatOptions{})

	return err
}

func (c queryClientAdapter) PermissionAnyInDir(
	ctx context.Context,
	dir string,
	uid uint32,
	gids []uint32,
) error {
	_, err := c.client.PermissionAnyInDir(ctx, dir, uid, gids)

	return err
}

func (c queryClientAdapter) FindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	requireOwner bool,
	uid uint32,
	gids []uint32,
) error {
	_, err := c.client.FindByGlob(ctx, baseDirs, patterns, clickhouse.FindOptions{
		RequireOwner: requireOwner,
		UID:          uid,
		GIDs:         gids,
	})

	return err
}

func (c queryClientAdapter) Close() error {
	return c.client.Close()
}

type queryInspectorAdapter struct {
	inspector *clickhouse.Inspector
}

func (i queryInspectorAdapter) ExplainListDir(
	ctx context.Context,
	mountPath, dir string,
	limit, offset int64,
) (string, error) {
	return i.inspector.ExplainListDir(ctx, mountPath, dir, limit, offset)
}

func (i queryInspectorAdapter) ExplainStatPath(
	ctx context.Context,
	mountPath, path string,
) (string, error) {
	return i.inspector.ExplainStatPath(ctx, mountPath, path)
}

func (i queryInspectorAdapter) Measure(
	ctx context.Context,
	run func(ctx context.Context) error,
) (*QueryMetrics, error) {
	metrics, err := i.inspector.Measure(ctx, run)
	if err != nil || metrics == nil {
		return nil, err
	}

	return &QueryMetrics{
		DurationMs:  metrics.DurationMs,
		ReadRows:    metrics.ReadRows,
		ReadBytes:   metrics.ReadBytes,
		ResultRows:  metrics.ResultRows,
		ResultBytes: metrics.ResultBytes,
	}, nil
}

func (i queryInspectorAdapter) Close() error {
	return i.inspector.Close()
}

// ClickHouseAPI adapts the ClickHouse backend to the performance harness.
type ClickHouseAPI struct {
	cfg clickhouse.Config
}

// NewClickHouseAPI returns a ClickHouse-backed adapter for the perf harness.
func NewClickHouseAPI(cfg clickhouse.Config) *ClickHouseAPI {
	return &ClickHouseAPI{cfg: cfg}
}

func (a *ClickHouseAPI) NewQueryClient() (QueryClient, error) {
	client, err := clickhouse.NewClient(a.cfg)
	if err != nil {
		return nil, err
	}

	return queryClientAdapter{client: client}, nil
}

func (a *ClickHouseAPI) NewQueryInspector() (QueryInspector, error) {
	inspector, err := clickhouse.NewInspector(a.cfg)
	if err != nil {
		return nil, err
	}

	return queryInspectorAdapter{inspector: inspector}, nil
}

func (a *ClickHouseAPI) NewDGUTAWriter() (db.DGUTAWriter, error) {
	return clickhouse.NewDGUTAWriter(a.cfg)
}

func (a *ClickHouseAPI) NewFileIngestOperation(
	mountPath string,
	updatedAt time.Time,
) (summary.OperationGenerator, io.Closer, error) {
	return clickhouse.NewFileIngestOperation(a.cfg, mountPath, updatedAt)
}

func (a *ClickHouseAPI) NewBaseDirsStore() (basedirs.Store, error) {
	return clickhouse.NewBaseDirsStore(a.cfg)
}

func (a *ClickHouseAPI) OpenProvider() (provider.Provider, error) {
	return clickhouse.OpenProvider(a.cfg)
}
