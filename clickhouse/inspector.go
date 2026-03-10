/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	explainPrefix = "EXPLAIN indexes = 1 "

	serverTimeQuery = "SELECT now()"

	flushLogsStmt = "SYSTEM FLUSH LOGS"

	queryLogQuery = "SELECT " +
		"toUInt64(query_duration_ms) AS duration_ms, " +
		"toUInt64(read_rows) AS read_rows, " +
		"toUInt64(read_bytes) AS read_bytes, " +
		"toUInt64(result_rows) AS result_rows, " +
		"toUInt64(result_bytes) AS result_bytes " +
		"FROM system.query_log " +
		"WHERE type = 'QueryFinish' " +
		"AND event_time >= ? " +
		"AND NOT startsWith(trimLeft(query), 'SYSTEM FLUSH LOGS') " +
		"ORDER BY event_time DESC " +
		"LIMIT 1"
)

// QueryMetrics are pulled from ClickHouse query logs.
type QueryMetrics struct {
	DurationMs  uint64
	ReadRows    uint64
	ReadBytes   uint64
	ResultRows  uint64
	ResultBytes uint64
}

func fallbackQueryMetrics(runDuration time.Duration) *QueryMetrics {
	return &QueryMetrics{DurationMs: durationMillis(runDuration)}
}

// Inspector can run EXPLAIN and query system.query_log without exposing
// clickhouse-go types.
type Inspector struct {
	cfg  Config
	conn ch.Conn
}

// NewInspector returns a new Inspector configured to use the ClickHouse
// database.
func NewInspector(cfg Config) (*Inspector, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
	if err != nil {
		return nil, err
	}

	return &Inspector{cfg: cfg, conn: conn}, nil
}

// ExplainListDir returns EXPLAIN output for the ListDir SQL statement.
// It uses the same SQL text as Client.ListDir.
func (i *Inspector) ExplainListDir(
	ctx context.Context,
	mountPath, dir string,
	limit, offset int64,
) (string, error) {
	q, _, err := listDirQuery(ListOptions{})
	if err != nil {
		return "", err
	}

	explainQ := explainPrefix + q

	return i.runExplain(ctx, explainQ, mountPath, mountPath, dir, limit, offset)
}

// ExplainStatPath returns EXPLAIN output for the StatPath SQL statement.
// It uses the same SQL text as Client.StatPath.
func (i *Inspector) ExplainStatPath(
	ctx context.Context,
	mountPath, path string,
) (string, error) {
	parentDir, name, ok := splitPathParentAndName(path)
	if !ok {
		return "", errInvalidPath
	}

	q, _, err := statPathQuery(StatOptions{})
	if err != nil {
		return "", err
	}

	explainQ := explainPrefix + q

	return i.runExplain(ctx, explainQ, mountPath, mountPath, parentDir, name)
}

// Measure runs the provided function, then returns metrics for the last
// completed query executed on the configured server after the run started.
func (i *Inspector) Measure(
	ctx context.Context,
	run func(ctx context.Context) error,
) (*QueryMetrics, error) {
	t0, err := i.serverTime(ctx)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to get server time: %w", err)
	}

	start := time.Now()

	if runErr := run(ctx); runErr != nil {
		return nil, runErr
	}

	runDuration := time.Since(start)

	m, err := i.queryMetricsSince(ctx, t0)
	if err == nil {
		return m, nil
	}

	if shouldFallbackQueryMetrics(err) {
		return fallbackQueryMetrics(runDuration), nil
	}

	return nil, err
}

func shouldFallbackQueryMetrics(err error) bool {
	return isMissingQueryLogError(err) || errors.Is(err, sql.ErrNoRows)
}

// Close closes the inspector's connection.
func (i *Inspector) Close() error {
	if i == nil || i.conn == nil {
		return nil
	}

	return i.conn.Close()
}

func (i *Inspector) runExplain(ctx context.Context, query string, args ...any) (string, error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout(i.cfg))
	defer cancel()

	rows, err := i.conn.Query(qctx, query, args...)
	if err != nil {
		return "", fmt.Errorf("clickhouse: EXPLAIN failed: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return collectExplainOutput(rows)
}

func collectExplainOutput(rows explainRows) (string, error) {
	lines := make([]string, 0)

	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", fmt.Errorf("clickhouse: failed to scan EXPLAIN row: %w", err)
		}

		lines = append(lines, line)
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("clickhouse: EXPLAIN iteration error: %w", err)
	}

	return strings.Join(lines, "\n"), nil
}

func (i *Inspector) serverTime(ctx context.Context) (time.Time, error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout(i.cfg))
	defer cancel()

	row := i.conn.QueryRow(qctx, serverTimeQuery)

	var t time.Time
	if err := row.Scan(&t); err != nil {
		return time.Time{}, err
	}

	return t, nil
}

func (i *Inspector) queryMetricsSince(ctx context.Context, t0 time.Time) (*QueryMetrics, error) {
	if err := i.flushLogs(ctx); err != nil {
		return nil, err
	}

	return i.scanQueryMetrics(ctx, t0)
}

func (i *Inspector) flushLogs(ctx context.Context) error {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout(i.cfg))
	defer cancel()

	if err := i.conn.Exec(qctx, flushLogsStmt); err != nil {
		return fmt.Errorf("clickhouse: failed to flush logs: %w", err)
	}

	return nil
}

func (i *Inspector) scanQueryMetrics(ctx context.Context, t0 time.Time) (*QueryMetrics, error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout(i.cfg))
	defer cancel()

	row := i.conn.QueryRow(qctx, queryLogQuery, t0)

	var m QueryMetrics
	if err := row.Scan(&m.DurationMs, &m.ReadRows, &m.ReadBytes, &m.ResultRows, &m.ResultBytes); err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query metrics: %w", err)
	}

	return &m, nil
}

type explainRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func isMissingQueryLogError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "system.query_log") &&
		(strings.Contains(msg, "unknown table") ||
			strings.Contains(msg, "doesn't exist"))
}

func durationMillis(runDuration time.Duration) uint64 {
	if runDuration <= 0 {
		return 0
	}

	ms := runDuration.Milliseconds()
	if ms <= 0 {
		return 0
	}

	value, err := strconv.ParseUint(strconv.FormatInt(ms, 10), 10, 64)
	if err != nil {
		return 0
	}

	return value
}
