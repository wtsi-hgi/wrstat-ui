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
	"fmt"
	"io"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/provider"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	clickHouseFileFieldPath      = "path"
	clickHouseFileFieldExt       = "ext"
	clickHouseFileFieldEntryType = "entry_type"
)

const importFactsStatsQuery = "SELECT " +
	"toUInt64(count()), " +
	"toUInt64(sum(length(gids))), " +
	"if(count() = 0, 0, avg(length(gids))), " +
	"toUInt64(max(length(gids))), " +
	"toUInt64(count()), " +
	"toUInt64(countIf(arrayExists(b -> length(b) > 0, atime_buckets) " +
	"OR arrayExists(b -> length(b) > 0, mtime_buckets))), " +
	"toUInt64(greatest(" +
	"max(arrayMax(arrayConcat([0], arrayMap(b -> length(b), atime_buckets)))), " +
	"max(arrayMax(arrayConcat([0], arrayMap(b -> length(b), mtime_buckets))))" +
	")), " +
	"toUInt64(countIf(arrayExists(b -> length(b) NOT IN (0, 10), atime_buckets) " +
	"OR arrayExists(b -> length(b) NOT IN (0, 10), mtime_buckets))) " +
	"FROM wrstat_dir_facts"

// ClickHouseAPI adapts the ClickHouse backend to the perf harness.
type ClickHouseAPI interface {
	ImportAPI
	QueryAPI
}

type clickHouseFileAPI interface {
	ListDir(ctx context.Context, dir string, opts clickhouse.ListOptions) ([]clickhouse.FileRow, error)
	StatPath(ctx context.Context, path string, opts clickhouse.StatOptions) (*clickhouse.FileRow, error)
	IsDir(ctx context.Context, path string) (bool, error)
	PermissionAnyInDir(ctx context.Context, dir string, uid uint32, gids []uint32) (bool, error)
	PermissionPath(ctx context.Context, path string, uid uint32, gids []uint32) (bool, error)
	CountByGlob(
		ctx context.Context,
		baseDirs []string,
		patterns []string,
		opts clickhouse.FindOptions,
	) (int, error)
	FindByGlob(
		ctx context.Context,
		baseDirs []string,
		patterns []string,
		opts clickhouse.FindOptions,
	) ([]clickhouse.FileRow, error)
	Close() error
}

var (
	_ ClickHouseAPI         = (*clickHouseAPI)(nil)
	_ clickHouseFileAPI     = (*clickhouse.Client)(nil)
	_ ImportAPI             = (*clickHouseAPI)(nil)
	_ ImportReportStatsAPI  = (*clickHouseAPI)(nil)
	_ ImportStorageAuditAPI = (*clickHouseAPI)(nil)
	_ QueryAPI              = (*clickHouseAPI)(nil)
	_ QueryCacheResetter    = (*clickHouseAPI)(nil)
)

// NewClickHouseAPI returns a ClickHouse-backed adapter for the perf harness.
func NewClickHouseAPI(cfg clickhouse.Config) ClickHouseAPI {
	return newClickHouseAPIWithOpenProvider(cfg, clickhouse.OpenProvider)
}

func newClickHouseAPIWithOpenProvider(
	cfg clickhouse.Config,
	openProvider clickHouseOpenProvider,
) ClickHouseAPI {
	if openProvider == nil {
		openProvider = clickhouse.OpenProvider
	}

	return &clickHouseAPI{cfg: cfg, openProvider: openProvider}
}

type clickHouseOpenProvider func(clickhouse.Config) (provider.Provider, error)

type clickHouseQueryClient struct {
	client clickHouseFileAPI
}

func (c clickHouseQueryClient) ListDir(
	ctx context.Context,
	dir string,
	limit int64,
) ([]QueryRow, error) {
	rows, err := c.client.ListDir(ctx, dir, clickhouse.ListOptions{
		Fields: []string{
			clickHouseFileFieldPath,
			clickHouseFileFieldExt,
			clickHouseFileFieldEntryType,
		},
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}

	return convertQueryRows(rows), nil
}

func convertQueryRows(rows []clickhouse.FileRow) []QueryRow {
	converted := make([]QueryRow, len(rows))
	for i, row := range rows {
		converted[i] = QueryRow{
			Path:      row.Path,
			Ext:       row.Ext,
			EntryType: row.EntryType,
		}
	}

	return converted
}

func (c clickHouseQueryClient) StatPath(ctx context.Context, path string) (*QueryRow, error) {
	row, err := c.client.StatPath(ctx, path, clickhouse.StatOptions{
		Fields: []string{
			clickHouseFileFieldPath,
			clickHouseFileFieldExt,
			clickHouseFileFieldEntryType,
		},
	})
	if err != nil {
		return nil, err
	}

	return &QueryRow{
		Path:      row.Path,
		Ext:       row.Ext,
		EntryType: row.EntryType,
	}, nil
}

func (c clickHouseQueryClient) IsDir(ctx context.Context, path string) (bool, error) {
	return c.client.IsDir(ctx, path)
}

func (c clickHouseQueryClient) PermissionAnyInDir(
	ctx context.Context,
	dir string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	return c.client.PermissionAnyInDir(ctx, dir, uid, gids)
}

func (c clickHouseQueryClient) PermissionPath(
	ctx context.Context,
	path string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	return c.client.PermissionPath(ctx, path, uid, gids)
}

func (c clickHouseQueryClient) FindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	requireOwner bool,
	uid uint32,
	gids []uint32,
) ([]QueryRow, error) {
	rows, err := c.client.FindByGlob(ctx, baseDirs, patterns, clickhouse.FindOptions{
		Fields: []string{
			clickHouseFileFieldPath,
			clickHouseFileFieldExt,
			clickHouseFileFieldEntryType,
		},
		RequireOwner: requireOwner,
		UID:          uid,
		GIDs:         gids,
	})
	if err != nil {
		return nil, err
	}

	return convertQueryRows(rows), nil
}

func (c clickHouseQueryClient) CountByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	requireOwner bool,
	uid uint32,
	gids []uint32,
) (int, error) {
	return c.client.CountByGlob(ctx, baseDirs, patterns, clickhouse.FindOptions{
		RequireOwner: requireOwner,
		UID:          uid,
		GIDs:         gids,
	})
}

func (c clickHouseQueryClient) Close() error {
	return c.client.Close()
}

type clickHouseQueryInspector struct {
	inspector *clickhouse.Inspector
}

func (i clickHouseQueryInspector) ExplainListDir(
	ctx context.Context,
	mountPath, dir string,
	limit, offset int64,
) (string, error) {
	return i.inspector.ExplainListDir(ctx, mountPath, dir, limit, offset)
}

func (i clickHouseQueryInspector) ExplainStatPath(
	ctx context.Context,
	mountPath, path string,
) (string, error) {
	return i.inspector.ExplainStatPath(ctx, mountPath, path)
}

func (i clickHouseQueryInspector) ExplainFindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
) (string, error) {
	return i.inspector.ExplainFindByGlob(ctx, baseDirs, patterns, clickhouse.FindOptions{
		Fields: []string{clickHouseFileFieldPath},
	})
}

func (i clickHouseQueryInspector) Measure(
	ctx context.Context,
	run func(ctx context.Context) error,
) (*QueryMetrics, error) {
	metrics, err := i.inspector.Measure(ctx, run)
	if err != nil || metrics == nil {
		return nil, err
	}

	return convertQueryMetrics(metrics), nil
}

func convertQueryMetrics(metrics *clickhouse.QueryMetrics) *QueryMetrics {
	return &QueryMetrics{
		DurationMs:  metrics.DurationMs,
		ReadRows:    metrics.ReadRows,
		ReadBytes:   metrics.ReadBytes,
		ReadMarks:   metrics.ReadMarks,
		MemoryBytes: metrics.MemoryBytes,
		ResultRows:  metrics.ResultRows,
		ResultBytes: metrics.ResultBytes,
	}
}

func (i clickHouseQueryInspector) Close() error {
	return i.inspector.Close()
}

type clickHouseAPI struct {
	cfg          clickhouse.Config
	openProvider clickHouseOpenProvider
}

func (a *clickHouseAPI) providerConfig() clickhouse.Config {
	cfg := a.cfg
	cfg.PollInterval = 0

	return cfg
}

func (a *clickHouseAPI) NewQueryClient() (QueryClient, error) {
	client, err := clickhouse.NewClient(a.cfg)
	if err != nil {
		return nil, err
	}

	return clickHouseQueryClient{client: client}, nil
}

func (a *clickHouseAPI) NewQueryInspector() (QueryInspector, error) {
	inspector, err := clickhouse.NewInspector(a.cfg)
	if err != nil {
		return nil, err
	}

	return clickHouseQueryInspector{inspector: inspector}, nil
}

func (a *clickHouseAPI) NewDGUTAWriter() (db.DGUTAWriter, error) {
	return clickhouse.NewDGUTAWriter(a.cfg)
}

func (a *clickHouseAPI) NewFileIngestOperation(
	mountPath string,
	updatedAt time.Time,
	alloc *summary.DirIDAllocator,
) (summary.OperationGenerator, io.Closer, error) {
	return clickhouse.NewFileIngestOperation(a.cfg, mountPath, updatedAt, alloc)
}

func (a *clickHouseAPI) NewBaseDirsStore() (basedirs.Store, error) {
	return clickhouse.NewBaseDirsStore(a.cfg)
}

func (a *clickHouseAPI) ImportTableStats(
	ctx context.Context,
	tables []string,
) (map[string]perfreport.TableStats, error) {
	conn, err := a.openStatsConn(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	return queryImportTableStats(ctx, conn, a.cfg.Database, tables)
}

func queryImportTableStats(
	ctx context.Context,
	conn driver.Conn,
	database string,
	tables []string,
) (map[string]perfreport.TableStats, error) {
	query, args := importTableStatsQuery(database, tables)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	return scanImportTableStats(rows)
}

func (a *clickHouseAPI) ImportFactsStats(
	ctx context.Context,
) (perfreport.FactsVectorStats, perfreport.FactsBucketStats, error) {
	conn, err := a.openStatsConn(ctx)
	if err != nil {
		return perfreport.FactsVectorStats{}, perfreport.FactsBucketStats{}, err
	}

	defer func() { _ = conn.Close() }()

	return queryImportFactsStats(ctx, conn)
}

func queryImportFactsStats(
	ctx context.Context,
	conn driver.Conn,
) (perfreport.FactsVectorStats, perfreport.FactsBucketStats, error) {
	row := conn.QueryRow(ctx, importFactsStatsQuery)

	var (
		vector perfreport.FactsVectorStats
		bucket perfreport.FactsBucketStats
	)

	err := row.Scan(
		&vector.Rows,
		&vector.TotalEntries,
		&vector.AverageEntriesPerDir,
		&vector.MaxEntriesPerDir,
		&bucket.Rows,
		&bucket.NonEmptyRows,
		&bucket.MaxBuckets,
		&bucket.MismatchedBucketRows,
	)
	if err != nil {
		return vector, bucket, fmt.Errorf("clickhouse perf facts stats: %w", err)
	}

	return vector, bucket, nil
}

func (a *clickHouseAPI) ImportHotRowPathStringTables(
	ctx context.Context,
	tables []string,
) ([]string, error) {
	conn, err := a.openStatsConn(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	return queryImportHotRowPathStringTables(ctx, conn, a.cfg.Database, tables)
}

func queryImportHotRowPathStringTables(
	ctx context.Context,
	conn driver.Conn,
	database string,
	tables []string,
) ([]string, error) {
	query, args := importHotRowPathStringTablesQuery(database, tables)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	return scanImportHotRowPathStringTables(rows)
}

func (a *clickHouseAPI) OpenProvider() (provider.Provider, error) {
	return a.openProvider(a.providerConfig())
}

func (a *clickHouseAPI) ResetQueryCaches() {
	clickhouse.ResetTreeQueryCaches()
}

func (a *clickHouseAPI) openStatsConn(ctx context.Context) (driver.Conn, error) {
	opts, err := ch.ParseDSN(a.cfg.DSN)
	if err != nil {
		return nil, err
	}

	opts.Auth.Database = a.cfg.Database

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return conn, nil
}

func importTableStatsQuery(database string, tables []string) (string, []any) {
	tableFilter := "startsWith(table, 'wrstat_')"
	query := "SELECT table, toUInt64(sum(rows)), toUInt64(count()), " +
		"toUInt64(sum(data_compressed_bytes)), toUInt64(sum(data_uncompressed_bytes)) " +
		"FROM system.parts WHERE database = ? AND active AND "

	args := make([]any, 0, max(len(tables)+1, 1))

	args = append(args, database)

	if len(tables) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(tables)), ",")
		tableFilter = "table IN (" + placeholders + ")"

		for _, table := range tables {
			args = append(args, table)
		}
	}

	query += tableFilter + " GROUP BY table ORDER BY table"

	return query, args
}

func importHotRowPathStringTablesQuery(database string, tables []string) (string, []any) {
	tableFilter := "startsWith(table, 'wrstat_')"
	query := "SELECT DISTINCT table FROM system.columns " +
		"WHERE database = ? AND table != ? AND " +
		"positionCaseInsensitive(type, 'String') > 0 AND " +
		"lower(name) IN ('dir', 'parent_dir', 'child_dir', 'path') AND "
	args := make([]any, 0, max(len(tables)+2, 2))

	args = append(args, database, tableCatalog)

	if len(tables) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(tables)), ",")
		tableFilter = "table IN (" + placeholders + ")"

		for _, table := range tables {
			args = append(args, table)
		}
	}

	query += tableFilter + " ORDER BY table"

	return query, args
}

func scanImportTableStats(rows driver.Rows) (map[string]perfreport.TableStats, error) {
	stats := make(map[string]perfreport.TableStats)

	for rows.Next() {
		var (
			table string
			s     perfreport.TableStats
		)

		if err := rows.Scan(
			&table,
			&s.Rows,
			&s.ActiveParts,
			&s.CompressedBytes,
			&s.UncompressedBytes,
		); err != nil {
			return nil, err
		}

		stats[table] = s
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

func scanImportHotRowPathStringTables(rows driver.Rows) ([]string, error) {
	tables := make([]string, 0)

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}
