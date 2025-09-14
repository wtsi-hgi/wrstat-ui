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
  mount_path String,
  scan_id UInt64,
  state Enum8('loading' = 0, 'ready' = 1),
  started_at DateTime,
  finished_at Nullable(DateTime)
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_id)
ORDER BY (mount_path, scan_id)`

	createFsEntriesTable = `
CREATE TABLE IF NOT EXISTS fs_entries (
  mount_path String,
  scan_id UInt64,
  path String,
  parent_path String,
  name String,
  ext_low String,
  ftype UInt8,
  inode UInt64,
  size UInt64,
  uid UInt32,
  gid UInt32,
  mtime DateTime,
  atime DateTime,
  ctime DateTime,
  INDEX idx_uid uid TYPE minmax GRANULARITY 8192,
  INDEX idx_gid gid TYPE minmax GRANULARITY 8192,
  INDEX idx_mtime mtime TYPE minmax GRANULARITY 8192,
  INDEX idx_atime atime TYPE minmax GRANULARITY 8192,
  INDEX idx_path_bf path TYPE tokenbf_v1(256) GRANULARITY 4,
  INDEX idx_parent_path parent_path TYPE minmax GRANULARITY 8192
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_id)
ORDER BY (mount_path, parent_path, name)
SETTINGS index_granularity = 8192`

	// Fallback without tokenbf index in case the server doesn't support it.
	createFsEntriesTableNoPathIdx = `
CREATE TABLE IF NOT EXISTS fs_entries (
	mount_path String,
	scan_id UInt64,
	path String,
	parent_path String,
	name String,
	ext_low String,
	ftype UInt8,
	inode UInt64,
	size UInt64,
	uid UInt32,
	gid UInt32,
	mtime DateTime,
	atime DateTime,
	ctime DateTime,
	INDEX idx_uid uid TYPE minmax GRANULARITY 8192,
	INDEX idx_gid gid TYPE minmax GRANULARITY 8192,
	INDEX idx_mtime mtime TYPE minmax GRANULARITY 8192,
	INDEX idx_atime atime TYPE minmax GRANULARITY 8192,
	INDEX idx_parent_path parent_path TYPE minmax GRANULARITY 8192
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_id)
ORDER BY (mount_path, parent_path, name)
SETTINGS index_granularity = 8192`

	createRollupRawTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups_raw (
  mount_path String,
  scan_id UInt64,
  ancestor String,
  size UInt64,
	atime DateTime,
	mtime DateTime,
	uid UInt32,
	gid UInt32,
	ext_low String
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_id)
ORDER BY (mount_path, ancestor)
SETTINGS index_granularity = 8192`

	createRollupStateTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups_state (
  mount_path String,
  scan_id UInt64,
  ancestor String,
  total_size AggregateFunction(sum, UInt64),
  file_count AggregateFunction(sum, UInt64),
  atime_min AggregateFunction(min, DateTime),
  atime_max AggregateFunction(max, DateTime),
  mtime_min AggregateFunction(min, DateTime),
  mtime_max AggregateFunction(max, DateTime),
	uids AggregateFunction(groupUniqArray, UInt32),
	gids AggregateFunction(groupUniqArray, UInt32),
	exts AggregateFunction(groupUniqArray, String),
  at_within_0d_size AggregateFunction(sum, UInt64),
  at_within_0d_count AggregateFunction(sum, UInt64),
  at_older_1m_size AggregateFunction(sum, UInt64),
  at_older_1m_count AggregateFunction(sum, UInt64),
  at_older_2m_size AggregateFunction(sum, UInt64),
  at_older_2m_count AggregateFunction(sum, UInt64),
  at_older_6m_size AggregateFunction(sum, UInt64),
  at_older_6m_count AggregateFunction(sum, UInt64),
  at_older_1y_size AggregateFunction(sum, UInt64),
  at_older_1y_count AggregateFunction(sum, UInt64),
  at_older_2y_size AggregateFunction(sum, UInt64),
  at_older_2y_count AggregateFunction(sum, UInt64),
  at_older_3y_size AggregateFunction(sum, UInt64),
  at_older_3y_count AggregateFunction(sum, UInt64),
  at_older_5y_size AggregateFunction(sum, UInt64),
  at_older_5y_count AggregateFunction(sum, UInt64),
  at_older_7y_size AggregateFunction(sum, UInt64),
  at_older_7y_count AggregateFunction(sum, UInt64),
  mt_older_1m_size AggregateFunction(sum, UInt64),
  mt_older_1m_count AggregateFunction(sum, UInt64),
  mt_older_2m_size AggregateFunction(sum, UInt64),
  mt_older_2m_count AggregateFunction(sum, UInt64),
  mt_older_6m_size AggregateFunction(sum, UInt64),
  mt_older_6m_count AggregateFunction(sum, UInt64),
  mt_older_1y_size AggregateFunction(sum, UInt64),
  mt_older_1y_count AggregateFunction(sum, UInt64),
  mt_older_2y_size AggregateFunction(sum, UInt64),
  mt_older_2y_count AggregateFunction(sum, UInt64),
  mt_older_3y_size AggregateFunction(sum, UInt64),
  mt_older_3y_count AggregateFunction(sum, UInt64),
  mt_older_5y_size AggregateFunction(sum, UInt64),
  mt_older_5y_count AggregateFunction(sum, UInt64),
  mt_older_7y_size AggregateFunction(sum, UInt64),
  mt_older_7y_count AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY (mount_path, scan_id)
ORDER BY (mount_path, ancestor)
SETTINGS index_granularity = 8192`

	//nolint:misspell // ClickHouse requires American English spelling "MATERIALIZED"
	createRollupMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS ancestor_rollups_mv
TO ancestor_rollups_state AS
WITH toDateTime(scan_id) AS scan_time
SELECT
  mount_path,
  scan_id,
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
GROUP BY mount_path, scan_id, ancestor`

	createFilesCurrentView = `
CREATE VIEW IF NOT EXISTS fs_entries_current AS
SELECT e.*
FROM fs_entries e
INNER JOIN (
  SELECT mount_path, max(scan_id) AS scan_id
  FROM scans
  WHERE state = 'ready'
  GROUP BY mount_path
) r USING (mount_path, scan_id)`

	createRollupsCurrentView = `
CREATE VIEW IF NOT EXISTS ancestor_rollups_current AS
SELECT s.mount_path,
       s.scan_id,
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
  SELECT mount_path, max(scan_id) AS scan_id
  FROM scans
  WHERE state = 'ready'
  GROUP BY mount_path
) r USING (mount_path, scan_id)
GROUP BY s.mount_path, s.scan_id, s.ancestor`
)

// CreateSchema creates all necessary tables and views in the ClickHouse database.
func (c *Clickhouse) CreateSchema(ctx context.Context) error {
	// Create scans table first
	if err := c.createTableWithStatement(ctx, createScansTable); err != nil {
		return err
	}

	// Create fs_entries table with fallback
	if err := c.createFsEntriesTableWithFallback(ctx); err != nil {
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
	return c.createViews(ctx)
}

// createTableWithStatement executes a CREATE TABLE statement and returns any error.
func (c *Clickhouse) createTableWithStatement(ctx context.Context, statement string) error {
	return c.conn.Exec(ctx, statement)
}

// createFsEntriesTableWithFallback tries to create the fs_entries table with path bloom filter,
// but falls back to no path index if the server doesn't support it.
func (c *Clickhouse) createFsEntriesTableWithFallback(ctx context.Context) error {
	// Try fs_entries with path bloom filter, fallback if server doesn't support it
	if err := c.conn.Exec(ctx, createFsEntriesTable); err != nil {
		// Retry with no path index
		if err2 := c.conn.Exec(ctx, createFsEntriesTableNoPathIdx); err2 != nil {
			return err2
		}
	}

	return nil
}

// createViews creates all the necessary views for the schema.
func (c *Clickhouse) createViews(ctx context.Context) error {
	//nolint:misspell // ClickHouse requires American English spelling "materialized"
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
func (c *Clickhouse) registerScan(ctx context.Context, mountPath string, scanID uint64, started time.Time) error {
	err := c.conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at) 
		VALUES (?, ?, 'loading', ?, NULL)`,
		mountPath, scanID, started)
	if err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}

	return nil
}

// PromoteScan marks a scan as 'ready' by inserting a new record.
func (c *Clickhouse) promoteScan(
	ctx context.Context,
	mountPath string,
	scanID uint64,
	started, finished time.Time,
) error {
	return c.conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at)
		VALUES (?, ?, 'ready', ?, ?)`,
		mountPath, scanID, started, finished)
}

// DropPartitionIgnoreErrors executes a query and ignores any errors.
// #nosec G104 - we intentionally ignore errors here for cleanup operations
func (c *Clickhouse) dropPartitionIgnoreErrors(ctx context.Context, query string) {
	// We intentionally ignore errors from these cleanup operations
	//nolint:errcheck
	c.conn.Exec(ctx, query)
}

// SetupRollbackHandler creates a deferred function that handles cleanup on error.
func (c *Clickhouse) setupRollbackHandler(ctx context.Context, mountPath string, scanID uint64) func(error) {
	return func(retErr error) {
		if retErr == nil {
			return
		}
		// Construct partition tuple literal
		part := fmt.Sprintf("('%s', %d)", EscapeCHSingleQuotes(mountPath), scanID)

		// Drop partitions - ignoring errors on cleanup
		// We use a separate function to avoid the empty block lint warnings
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE fs_entries DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE ancestor_rollups_raw DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE ancestor_rollups_state DROP PARTITION "+part)
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE scans DROP PARTITION "+part)
	}
}

// DropOlderScans removes older scans for a specific mount path.
func (c *Clickhouse) dropOlderScans(ctx context.Context, mountPath string, keepScanID uint64) error {
	// Get older scan_ids for this mount
	rows, err := c.conn.Query(ctx, `
		SELECT scan_id 
		FROM scans 
		WHERE mount_path = ? AND scan_id < ? 
		ORDER BY scan_id`,
		mountPath, keepScanID)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Process each older scan ID
	var sid uint64
	for rows.Next() {
		if err := rows.Scan(&sid); err != nil {
			return err
		}

		if err := c.dropSingleScan(ctx, mountPath, sid); err != nil {
			return err
		}
	}

	return rows.Err()
}

// DropSingleScan drops all data for a single scan ID.
func (c *Clickhouse) dropSingleScan(ctx context.Context, mountPath string, scanID uint64) error {
	// Construct partition tuple literal safely
	part := fmt.Sprintf("('%s', %d)", EscapeCHSingleQuotes(mountPath), scanID)

	// Drop partitions from tables
	for _, dropStmt := range []string{
		"ALTER TABLE fs_entries DROP PARTITION " + part,
		"ALTER TABLE ancestor_rollups_raw DROP PARTITION " + part,
		"ALTER TABLE ancestor_rollups_state DROP PARTITION " + part,
		"ALTER TABLE scans DROP PARTITION " + part,
	} {
		if err := c.conn.Exec(ctx, dropStmt); err != nil {
			return err
		}
	}

	return nil
}

// GetLastScanTimes retrieves the most recent scan times for each mount.
func (c *Clickhouse) GetLastScanTimes(ctx context.Context) (map[string]time.Time, error) {
	// Return the finished_at time of the latest ready scan per mount
	// Using argMax(finished_at, scan_id) ensures we pick the finished time corresponding
	// to the maximum scan_id (latest) for each mount.
	rows, err := c.conn.Query(ctx, `
		SELECT mount_path, argMax(finished_at, scan_id)
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
