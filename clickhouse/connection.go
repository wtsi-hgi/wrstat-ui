/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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
	"fmt"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// ConnectionParams contains parameters for creating a new ClickHouse connection.
type ConnectionParams struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// New creates and configures a new Clickhouse instance.
func New(params ConnectionParams) (*Clickhouse, error) {
	conn, err := chdriver.Open(&chdriver.Options{
		Addr:        []string{fmt.Sprintf("%s:%s", params.Host, params.Port)},
		Auth:        chdriver.Auth{Database: params.Database, Username: params.Username, Password: params.Password},
		DialTimeout: DialTimeoutSeconds * time.Second,
		Compression: &chdriver.Compression{Method: chdriver.CompressionLZ4},
		Settings: chdriver.Settings{
			"max_insert_block_size":       MaxInsertBlockSize,
			"min_insert_block_size_rows":  MinInsertBlockRows,
			"min_insert_block_size_bytes": MinInsertBlockBytes, // 10MB
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	return &Clickhouse{conn: conn}, nil
}

// SQL constants for table and view creation.
const (
	createScansTable = `
CREATE TABLE IF NOT EXISTS scans (
	mount_path String CODEC(ZSTD),
	scan_id UUID CODEC(ZSTD),
	scan_time DateTime CODEC(DoubleDelta, ZSTD),
	state Enum8('loading' = 0, 'ready' = 1) CODEC(ZSTD),
	started_at DateTime CODEC(DoubleDelta, ZSTD),
	finished_at Nullable(DateTime) CODEC(DoubleDelta, ZSTD)
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_time)
ORDER BY (mount_path, scan_time)
SETTINGS index_granularity = 8192`

	createFsEntriesTableNoPathIdx = `
CREATE TABLE IF NOT EXISTS fs_entries (
	mount_path String CODEC(ZSTD),
	scan_id UUID CODEC(ZSTD),
	scan_time DateTime CODEC(DoubleDelta, ZSTD),
	path String CODEC(ZSTD),
	parent_path String CODEC(ZSTD),
	basename String CODEC(ZSTD),
		depth UInt16 CODEC(T64, ZSTD),
	ext_low String CODEC(ZSTD),
	ftype UInt8 CODEC(ZSTD),
	inode UInt64 CODEC(T64, ZSTD),
	size UInt64 CODEC(T64, ZSTD),
	uid UInt32 CODEC(T64, ZSTD),
	gid UInt32 CODEC(T64, ZSTD),
	mtime DateTime CODEC(DoubleDelta, ZSTD),
	atime DateTime CODEC(DoubleDelta, ZSTD),
	ctime DateTime CODEC(DoubleDelta, ZSTD),
	INDEX idx_uid uid TYPE minmax GRANULARITY 8192,
	INDEX idx_gid gid TYPE minmax GRANULARITY 8192,
	INDEX idx_mtime mtime TYPE minmax GRANULARITY 8192,
	INDEX idx_atime atime TYPE minmax GRANULARITY 8192,
		INDEX idx_parent_path parent_path TYPE minmax GRANULARITY 8192,
		INDEX idx_depth depth TYPE minmax GRANULARITY 8192
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_time)
ORDER BY (mount_path, parent_path, basename)
SETTINGS index_granularity = 8192`

	createRollupRawTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups_raw (
	mount_path String CODEC(ZSTD),
	scan_id UUID CODEC(ZSTD),
	scan_time DateTime CODEC(DoubleDelta, ZSTD),
	ancestor String CODEC(ZSTD),
	size UInt64 CODEC(T64, ZSTD),
	atime DateTime CODEC(DoubleDelta, ZSTD),
	mtime DateTime CODEC(DoubleDelta, ZSTD),
	uid UInt32 CODEC(T64, ZSTD),
	gid UInt32 CODEC(T64, ZSTD),
	ext_low String CODEC(ZSTD)
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_time)
ORDER BY (mount_path, ancestor)
SETTINGS index_granularity = 8192`

	createRollupStateTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups_state (
		mount_path String CODEC(ZSTD),
				scan_id UUID CODEC(ZSTD),
				scan_time DateTime CODEC(DoubleDelta, ZSTD),
		ancestor String CODEC(ZSTD),
	total_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	file_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	atime_min AggregateFunction(min, DateTime) CODEC(ZSTD),
	atime_max AggregateFunction(max, DateTime) CODEC(ZSTD),
	mtime_min AggregateFunction(min, DateTime) CODEC(ZSTD),
	mtime_max AggregateFunction(max, DateTime) CODEC(ZSTD),
		uids AggregateFunction(groupUniqArray, UInt32) CODEC(ZSTD),
		gids AggregateFunction(groupUniqArray, UInt32) CODEC(ZSTD),
		exts AggregateFunction(groupUniqArray, String) CODEC(ZSTD),
	at_within_0d_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_within_0d_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_1m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_1m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_2m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_2m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_6m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_6m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_1y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_1y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_2y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_2y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_3y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_3y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_5y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_5y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_7y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	at_older_7y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_1m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_1m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_2m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_2m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_6m_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_6m_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_1y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_1y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_2y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_2y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_3y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_3y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_5y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_5y_count AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_7y_size AggregateFunction(sum, UInt64) CODEC(ZSTD),
	mt_older_7y_count AggregateFunction(sum, UInt64) CODEC(ZSTD)
) ENGINE = AggregatingMergeTree
	PARTITION BY (mount_path, scan_time)
ORDER BY (mount_path, ancestor)
SETTINGS index_granularity = 8192`

	//nolint:misspell // ClickHouse requires American English spelling "MATERIALIZED"
	createRollupMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS ancestor_rollups_mv
TO ancestor_rollups_state AS
SELECT
  mount_path,
  scan_id,
	scan_time,
  ancestor,
  sumState(size) AS total_size,
  sumState(toUInt64(1)) AS file_count,
  minState(atime) AS atime_min,
  maxState(atime) AS atime_max,
  minState(mtime) AS mtime_min,
  maxState(mtime) AS mtime_max,
	groupUniqArrayState(uid) AS uids,
	groupUniqArrayState(gid) AS gids,
	groupUniqArrayState(ext_low) AS exts,
  sumIfState(size, atime >= (scan_time - INTERVAL 1 DAY)) AS at_within_0d_size,
  sumIfState(toUInt64(1), atime >= (scan_time - INTERVAL 1 DAY)) AS at_within_0d_count,
  sumIfState(size, atime < (scan_time - INTERVAL 1 MONTH)) AS at_older_1m_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 1 MONTH)) AS at_older_1m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 2 MONTH)) AS at_older_2m_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 2 MONTH)) AS at_older_2m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 6 MONTH)) AS at_older_6m_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 6 MONTH)) AS at_older_6m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 1 YEAR)) AS at_older_1y_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 1 YEAR)) AS at_older_1y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 2 YEAR)) AS at_older_2y_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 2 YEAR)) AS at_older_2y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 3 YEAR)) AS at_older_3y_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 3 YEAR)) AS at_older_3y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 5 YEAR)) AS at_older_5y_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 5 YEAR)) AS at_older_5y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 7 YEAR)) AS at_older_7y_size,
  sumIfState(toUInt64(1), atime < (scan_time - INTERVAL 7 YEAR)) AS at_older_7y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 1 MONTH)) AS mt_older_1m_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 1 MONTH)) AS mt_older_1m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 2 MONTH)) AS mt_older_2m_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 2 MONTH)) AS mt_older_2m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 6 MONTH)) AS mt_older_6m_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 6 MONTH)) AS mt_older_6m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 1 YEAR)) AS mt_older_1y_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 1 YEAR)) AS mt_older_1y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 2 YEAR)) AS mt_older_2y_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 2 YEAR)) AS mt_older_2y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 3 YEAR)) AS mt_older_3y_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 3 YEAR)) AS mt_older_3y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 5 YEAR)) AS mt_older_5y_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 5 YEAR)) AS mt_older_5y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 7 YEAR)) AS mt_older_7y_size,
  sumIfState(toUInt64(1), mtime < (scan_time - INTERVAL 7 YEAR)) AS mt_older_7y_count
FROM ancestor_rollups_raw
GROUP BY mount_path, scan_id, scan_time, ancestor`

	createFilesCurrentView = `
CREATE VIEW IF NOT EXISTS fs_entries_current AS
SELECT e.*
FROM fs_entries e
INNER JOIN (
	SELECT mount_path, argMax(scan_id, finished_at) AS scan_id
  FROM scans
  WHERE state = 'ready'
  GROUP BY mount_path
) r USING (mount_path, scan_id)`

	createRollupsCurrentView = `
CREATE VIEW IF NOT EXISTS ancestor_rollups_current AS
SELECT s.mount_path,
	   s.scan_id,
	   s.scan_time,
       s.ancestor,
       sumMerge(total_size) AS total_size,
       sumMerge(file_count) AS file_count,
       minMerge(atime_min)  AS atime_min,
       maxMerge(atime_max)  AS atime_max,
       minMerge(mtime_min)  AS mtime_min,
       maxMerge(mtime_max)  AS mtime_max,
	   groupUniqArrayMerge(uids) AS uids,
	   groupUniqArrayMerge(gids) AS gids,
	   groupUniqArrayMerge(exts) AS exts,
       sumMerge(at_within_0d_size) AS at_within_0d_size,
       sumMerge(at_within_0d_count) AS at_within_0d_count,
       sumMerge(at_older_1m_size) AS at_older_1m_size,
       sumMerge(at_older_1m_count) AS at_older_1m_count,
       sumMerge(at_older_2m_size) AS at_older_2m_size,
       sumMerge(at_older_2m_count) AS at_older_2m_count,
       sumMerge(at_older_6m_size) AS at_older_6m_size,
       sumMerge(at_older_6m_count) AS at_older_6m_count,
       sumMerge(at_older_1y_size) AS at_older_1y_size,
       sumMerge(at_older_1y_count) AS at_older_1y_count,
       sumMerge(at_older_2y_size) AS at_older_2y_size,
       sumMerge(at_older_2y_count) AS at_older_2y_count,
       sumMerge(at_older_3y_size) AS at_older_3y_size,
       sumMerge(at_older_3y_count) AS at_older_3y_count,
       sumMerge(at_older_5y_size) AS at_older_5y_size,
       sumMerge(at_older_5y_count) AS at_older_5y_count,
       sumMerge(at_older_7y_size) AS at_older_7y_size,
       sumMerge(at_older_7y_count) AS at_older_7y_count,
       sumMerge(mt_older_1m_size) AS mt_older_1m_size,
       sumMerge(mt_older_1m_count) AS mt_older_1m_count,
       sumMerge(mt_older_2m_size) AS mt_older_2m_size,
       sumMerge(mt_older_2m_count) AS mt_older_2m_count,
       sumMerge(mt_older_6m_size) AS mt_older_6m_size,
       sumMerge(mt_older_6m_count) AS mt_older_6m_count,
       sumMerge(mt_older_1y_size) AS mt_older_1y_size,
       sumMerge(mt_older_1y_count) AS mt_older_1y_count,
       sumMerge(mt_older_2y_size) AS mt_older_2y_size,
       sumMerge(mt_older_2y_count) AS mt_older_2y_count,
       sumMerge(mt_older_3y_size) AS mt_older_3y_size,
       sumMerge(mt_older_3y_count) AS mt_older_3y_count,
       sumMerge(mt_older_5y_size) AS mt_older_5y_size,
       sumMerge(mt_older_5y_count) AS mt_older_5y_count,
       sumMerge(mt_older_7y_size) AS mt_older_7y_size,
       sumMerge(mt_older_7y_count) AS mt_older_7y_count
FROM ancestor_rollups_state s
INNER JOIN (
	SELECT mount_path, argMax(scan_id, finished_at) AS scan_id
  FROM scans
  WHERE state = 'ready'
  GROUP BY mount_path
) r USING (mount_path, scan_id)
GROUP BY s.mount_path, s.scan_id, s.scan_time, s.ancestor`
)

// CreateSchema creates all necessary tables and views in the ClickHouse database.
func (c *Clickhouse) CreateSchema(ctx context.Context) error {
	// Create scans table first
	if err := c.createTableWithStatement(ctx, createScansTable); err != nil {
		return err
	}

	// Create fs_entries table with fallback
	if err := c.createFsEntriesTable(ctx); err != nil {
		return err
	}

	// Create rollup raw table
	if err := c.createTableWithStatement(ctx, createRollupRawTable); err != nil {
		return err
	}

	// Create state table
	if err := c.createTableWithStatement(ctx, createRollupStateTable); err != nil {
		return err
	}

	// Create all views
	if err := c.createViews(ctx); err != nil {
		return err
	}

	return nil
}

// createTableWithStatement executes a CREATE TABLE statement and returns any error.
func (c *Clickhouse) createTableWithStatement(ctx context.Context, statement string) error {
	return c.conn.Exec(ctx, statement)
}

func (c *Clickhouse) createFsEntriesTable(ctx context.Context) error {
	// Use schema without path bloom/secondary indexes for prefix-only queries
	return c.conn.Exec(ctx, createFsEntriesTableNoPathIdx)
}

// createViews creates all the necessary views for the schema.
func (c *Clickhouse) createViews(ctx context.Context) error {
	// Drop views/MV if they exist to ensure we get the latest definitions
	// Materialised views are dropped with DROP TABLE
	c.dropPartitionIgnoreErrors(ctx, "DROP VIEW IF EXISTS fs_entries_current")
	c.dropPartitionIgnoreErrors(ctx, "DROP VIEW IF EXISTS ancestor_rollups_current")
	c.dropPartitionIgnoreErrors(ctx, "DROP TABLE IF EXISTS ancestor_rollups_mv")

	//nolint:misspell // ClickHouse requires American English spelling "materialized".
	// Create materialized view and current views
	if err := c.conn.Exec(ctx, createRollupMV); err != nil {
		return err
	}

	if err := c.conn.Exec(ctx, createFilesCurrentView); err != nil {
		return err
	}

	if err := c.conn.Exec(ctx, createRollupsCurrentView); err != nil {
		return err
	}

	return nil
}

// RegisterScan adds a new scan record with 'loading' state.
func (c *Clickhouse) registerScan(
	ctx context.Context,
	mountPath string,
	scanID uuid.UUID,
	scanTime, started time.Time,
) error {
	err := c.conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, scan_time, state, started_at, finished_at) 
		VALUES (?, ?, ?, 'loading', ?, NULL)`,
		mountPath, scanID, scanTime, started)
	if err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}

	return nil
}

// PromoteScan marks a scan as 'ready' by inserting a new record.
func (c *Clickhouse) promoteScan(
	ctx context.Context,
	mountPath string,
	scanID uuid.UUID,
	scanTime, started, finished time.Time,
) error {
	return c.conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, scan_time, state, started_at, finished_at)
		VALUES (?, ?, ?, 'ready', ?, ?)`,
		mountPath, scanID, scanTime, started, finished)
}

// DropPartitionIgnoreErrors executes a query and ignores any errors.
// #nosec G104 - we intentionally ignore errors here for cleanup operations
func (c *Clickhouse) dropPartitionIgnoreErrors(ctx context.Context, query string) {
	// We intentionally ignore errors from these cleanup operations
	//nolint:errcheck
	c.conn.Exec(ctx, query)
}

// SetupRollbackHandler creates a deferred function that handles cleanup on error.
func (c *Clickhouse) setupRollbackHandler(ctx context.Context, mountPath string, scanTime time.Time) func(error) {
	return func(retErr error) {
		if retErr == nil {
			return
		}
		// Construct partition tuple literal using scan_time
		part := fmt.Sprintf(
			"('%s', toDateTime('%s'))",
			EscapeCHSingleQuotes(mountPath),
			scanTime.Format("2006-01-02 15:04:05"),
		)

		// Drop partitions - ignoring errors on cleanup
		// We use a separate function to avoid the empty block lint warnings
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE fs_entries DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE ancestor_rollups_raw DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE ancestor_rollups_state DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE scans DROP PARTITION "+part)
	}
}

// DropOlderScans removes older scans for a specific mount path.
func (c *Clickhouse) dropOlderScans(ctx context.Context, mountPath string, keepFinishedAt time.Time) error {
	// Get older scans for this mount by finished_at and their scan_time for partition drops
	rows, err := c.conn.Query(ctx, `
		SELECT scan_time 
		FROM scans 
		WHERE mount_path = ? AND finished_at IS NOT NULL AND finished_at < ? 
		ORDER BY scan_time`,
		mountPath, keepFinishedAt)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Process each older scan
	var st time.Time
	for rows.Next() {
		if err := rows.Scan(&st); err != nil {
			return err
		}

		if err := c.dropSingleScan(ctx, mountPath, st); err != nil {
			return err
		}
	}

	return rows.Err()
}

// DropSingleScan drops all data for a single scan ID.
func (c *Clickhouse) dropSingleScan(ctx context.Context, mountPath string, scanTime time.Time) error {
	// Construct partition tuple literal safely
	part := fmt.Sprintf(
		"('%s', toDateTime('%s'))",
		EscapeCHSingleQuotes(mountPath),
		scanTime.Format("2006-01-02 15:04:05"),
	)

	// Drop partitions from tables
	// Try drop using (mount_path, scan_time) partition; on failure, fall back to legacy
	tables := []string{"fs_entries", "ancestor_rollups_raw", "ancestor_rollups_state", "scans"}
	for _, tbl := range tables {
		// First attempt: new partitioning
		dropStmt := "ALTER TABLE " + tbl + " DROP PARTITION " + part
		if err := c.conn.Exec(ctx, dropStmt); err != nil {
			// Fallback: legacy schemas may have partitioned only by mount_path
			// In that case, drop by single key
			legacy := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tbl, EscapeCHSingleQuotes(mountPath))
			if err2 := c.conn.Exec(ctx, legacy); err2 != nil {
				return err // return the original error to aid debugging
			}
		}
	}

	return nil
}

// GetLastScanTimes retrieves the most recent scan times for each mount.
func (c *Clickhouse) GetLastScanTimes(ctx context.Context) (map[string]time.Time, error) {
	// Return the finished_at time of the latest ready scan per mount
	rows, err := c.conn.Query(ctx, `
		SELECT mount_path, max(finished_at)
		FROM scans
		WHERE state = 'ready'
		GROUP BY mount_path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results
	result := make(map[string]time.Time)

	for rows.Next() {
		var (
			mountPath string
			timestamp time.Time
		)

		if err := rows.Scan(&mountPath, &timestamp); err != nil {
			return nil, err
		}

		result[mountPath] = timestamp
	}

	return result, rows.Err()
}
