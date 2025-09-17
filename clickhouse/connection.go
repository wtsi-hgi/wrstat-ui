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
	"os"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// ConnectionParams contains parameters for creating a new ClickHouse connection.
type ConnectionParams struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// ConnectionParamsFromEnv builds ConnectionParams using environment variables.
//
// It first attempts to load variables from .env files based on WRSTATUI_ENV.
// Allowed WRSTATUI_ENV values: production, development, test. If unset or
// invalid, defaults to "development". Files are loaded in the following order
// (first file wins; later files do not override earlier values):
//
//	.env.<env>.local, .env.<env>, .env.local, .env
//
// The following environment variables are read with defaults maintained:
//
//	WRSTATUI_CLICKHOUSE_HOST     (default: 127.0.0.1)
//	WRSTATUI_CLICKHOUSE_PORT     (default: 9000)
//	WRSTATUI_CLICKHOUSE_DATABASE (default: default)
//	WRSTATUI_CLICKHOUSE_USERNAME (default: default)
//	WRSTATUI_CLICKHOUSE_PASSWORD (default: "")
func ConnectionParamsFromEnv() ConnectionParams {
	// Load dotenv files with priority based on WRSTATUI_ENV
	env := resolveRuntimeEnv()
	loadDotenvFiles(env)

	getenv := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}

		return def
	}

	return ConnectionParams{
		Host:     getenv("WRSTATUI_CLICKHOUSE_HOST", "127.0.0.1"),
		Port:     getenv("WRSTATUI_CLICKHOUSE_PORT", "9000"),
		Database: getenv("WRSTATUI_CLICKHOUSE_DATABASE", "default"),
		Username: getenv("WRSTATUI_CLICKHOUSE_USERNAME", "default"),
		Password: getenv("WRSTATUI_CLICKHOUSE_PASSWORD", ""),
	}
}

// resolveRuntimeEnv returns a normalised environment name.
func resolveRuntimeEnv() string {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("WRSTATUI_ENV")))
	switch env {
	case "production", "development", "test":
		return env
	default:
		return "development"
	}
}

// loadDotenvFiles loads .env files in priority order, ignoring missing files and parse errors.
func loadDotenvFiles(env string) {
	files := []string{
		".env." + env + ".local",
		".env." + env,
		".env.local",
		".env",
	}

	for _, f := range files {
		if _, statErr := os.Stat(f); statErr == nil {
			_ = godotenv.Load(f) //nolint:errcheck // allow missing/partial .env files
		}
	}
}

// New creates and configures a new Clickhouse instance.
func New(params ConnectionParams) (*Clickhouse, error) {
	// First try to connect directly to the requested database
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
		// Fallback: connect to default DB (do not create the requested DB here)
		conn, err = chdriver.Open(&chdriver.Options{
			Addr:        []string{fmt.Sprintf("%s:%s", params.Host, params.Port)},
			Auth:        chdriver.Auth{Database: "default", Username: params.Username, Password: params.Password},
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
	}

	return &Clickhouse{conn: conn, params: params}, nil
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

	createAncestorRollupsTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups (
  mount_path String CODEC(ZSTD),
  scan_id UUID CODEC(ZSTD),
  scan_time DateTime CODEC(DoubleDelta, ZSTD),
  ancestor String CODEC(ZSTD),
  total_size UInt64 CODEC(T64, ZSTD),
  file_count UInt64 CODEC(T64, ZSTD),
  oldest_atime DateTime CODEC(DoubleDelta, ZSTD),
  newest_atime DateTime CODEC(DoubleDelta, ZSTD),
  oldest_mtime DateTime CODEC(DoubleDelta, ZSTD),
  newest_mtime DateTime CODEC(DoubleDelta, ZSTD),
  uids Array(UInt32) CODEC(ZSTD),
  gids Array(UInt32) CODEC(ZSTD),
  exts Array(String) CODEC(ZSTD),
  ftypes Array(UInt8) CODEC(ZSTD),
  age UInt8 CODEC(T64, ZSTD)
) ENGINE = MergeTree
PARTITION BY (mount_path, scan_time)
ORDER BY (mount_path, ancestor)
SETTINGS index_granularity = 8192`

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
SELECT ar.*
FROM ancestor_rollups ar
INNER JOIN (
	SELECT mount_path, argMax(scan_id, finished_at) AS scan_id
	FROM scans
	WHERE state = 'ready'
	GROUP BY mount_path
) r USING (mount_path, scan_id)`
)

// CreateSchema creates all necessary tables and views in the ClickHouse database.
//
// nolint // create schema contains many sequential DDL steps
func (c *Clickhouse) CreateSchema(ctx context.Context) error {
	// Ensure the target database exists for subsequent DDL
	dbName := c.params.Database
	if dbName == "" {
		dbName = "default"
	}

	if err := c.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName); err != nil {
		// Fallback: create via admin connection to default database
		admin, aerr := chdriver.Open(&chdriver.Options{
			Addr:        []string{fmt.Sprintf("%s:%s", c.params.Host, c.params.Port)},
			Auth:        chdriver.Auth{Database: "default", Username: c.params.Username, Password: c.params.Password},
			DialTimeout: DialTimeoutSeconds * time.Second,
		})
		if aerr == nil {
			// Attempt creation via admin connection and fail fast if it doesn't work
			execErr := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName)
			_ = admin.Close()

			if execErr != nil {
				return fmt.Errorf("create database via admin: %w", execErr)
			}
		} else {
			return err
		}
	}

	// Ensure subsequent DDL is executed in the target database
	if err := c.conn.Exec(ctx, "USE "+dbName); err != nil {
		return fmt.Errorf("select db: %w", err)
	}

	// Create scans table first
	if err := c.createTableWithStatement(ctx, createScansTable); err != nil {
		return err
	}

	// Create fs_entries table with fallback
	if err := c.createFsEntriesTable(ctx); err != nil {
		return err
	}

	// Create ancestor rollups summary table
	if err := c.createTableWithStatement(ctx, createAncestorRollupsTable); err != nil {
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
	c.dropPartitionIgnoreErrors(ctx, "DROP TABLE IF EXISTS ancestor_rollups_raw")
	c.dropPartitionIgnoreErrors(ctx, "DROP TABLE IF EXISTS ancestor_rollups_state")
	c.dropPartitionIgnoreErrors(ctx, "DROP TABLE IF EXISTS ancestor_rollups_mv")

	// Ensure legacy tables are removed (idempotent) before creating simplified table
	c.dropPartitionIgnoreErrors(ctx, "DROP TABLE IF EXISTS ancestor_rollups")
	if err := c.conn.Exec(ctx, createAncestorRollupsTable); err != nil {
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
		c.dropPartitionIgnoreErrors(ctx, "ALTER TABLE ancestor_rollups DROP PARTITION "+part)
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
	tables := []string{"fs_entries", "ancestor_rollups", "scans"}
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
