/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	defaultDir string

	quotaPath      string
	basedirsConfig string
	mounts         string

	// ClickHouse connection settings.
	chHost     string
	chPort     string
	chDatabase string
	chUsername string
	chPassword string
)

// summariseCmd represents the stat command.
//
// This command ingests a single mount's scan into ClickHouse with atomic
// promotion semantics (loading -> ready), maintains only the latest ready scan
// per mount, and provides precomputed subtree rollups.
var summariseCmd = &cobra.Command{
	Use:   "summarise <mount_path> <stats_file|->",
	Short: "Summarise stat data",
	Long: `Summarise stat data into ClickHouse for fast, interactive queries.

This command ingests a single mount's scan into ClickHouse with atomic promotion
semantics (loading -> ready), maintains only the latest ready scan per mount,
and provides precomputed subtree rollups.`,
	Run: func(_ *cobra.Command, args []string) {
		if err := Run(args); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(summariseCmd)

	// Add ClickHouse connection settings
	summariseCmd.Flags().StringVar(&chHost, "ch-host", "127.0.0.1", "ClickHouse host")
	summariseCmd.Flags().StringVar(&chPort, "ch-port", "9000", "ClickHouse port")
	summariseCmd.Flags().StringVar(&chDatabase, "ch-database", "default", "ClickHouse database")
	summariseCmd.Flags().StringVar(&chUsername, "ch-username", "default", "ClickHouse username")
	summariseCmd.Flags().StringVar(&chPassword, "ch-password", "", "ClickHouse password")
}

// Run executes the summarise command with the given arguments.
func Run(args []string) (err error) {
	mountPath, statsPath, err := checkArgs(args)
	if err != nil {
		return err
	}

	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return err
	}

	defer r.Close()

	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
		Auth:        clickhouse.Auth{Database: chDatabase, Username: chUsername, Password: chPassword},
		DialTimeout: 10 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Settings: clickhouse.Settings{
			"max_insert_block_size":       1000000,
			"min_insert_block_size_rows":  100000,
			"min_insert_block_size_bytes": 10485760, // 10MB
		},
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	if err := createSchema(ctx, conn); err != nil {
		return fmt.Errorf("createSchema: %w", err)
	}

	return updateClickhouse(ctx, conn, mountPath, r)
}

func checkArgs(args []string) (string, string, error) {
	if len(args) != 2 {
		return "", "", errors.New("usage: summarise <mount_path> <stats_file|->") //nolint:err113
	}

	mountPath := NormalizeMount(args[0])
	statsPath := args[1]

	if mountPath == "/" {
		return "", "", errors.New("mount_path must not be '/' — use the real mount point path") //nolint:err113
	}

	return mountPath, statsPath, nil
}

// Helper to close multiple resources when wrapping readers.
type readMultiCloser struct {
	r       io.Reader
	closers []io.Closer
}

func (m *readMultiCloser) Read(p []byte) (int, error) { return m.r.Read(p) }

func (m *readMultiCloser) Close() error {
	var firstErr error
	for i := len(m.closers) - 1; i >= 0; i-- {
		if err := m.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// OpenStatsFile exports the internal openStatsFile function for testing.
func OpenStatsFile(statsFile string) (io.ReadCloser, time.Time, error) {
	return openStatsFile(statsFile)
}

func openStatsFile(statsFile string) (io.ReadCloser, time.Time, error) {
	if statsFile == "-" {
		return os.Stdin, time.Now(), nil
	}

	f, err := os.Open(statsFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to open stats file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()

		return nil, time.Time{}, err
	}

	if strings.HasSuffix(statsFile, ".gz") {
		zr, err := pgzip.NewReader(f)
		if err != nil {
			_ = f.Close()

			return nil, time.Time{}, fmt.Errorf("failed to decompress stats file: %w", err)
		}

		return &readMultiCloser{r: zr, closers: []io.Closer{zr, f}}, fi.ModTime(), nil
	}

	return f, fi.ModTime(), nil
}

// --- ClickHouse schema and ingestion ---.

const (
	// Lower batch sizes to balance performance and memory usage.
	fileBatchSize   = 100_000
	rollupBatchSize = 100_000
)

type ftype uint8

const (
	ftUnknown ftype = iota
	ftFile
	ftDir
	ftSymlink
	ftDevice
	ftPipe
	ftSocket
	ftChar
)

func mapEntryType(b byte) ftype {
	switch b {
	case stats.FileType:
		return ftFile
	case stats.DirType:
		return ftDir
	case stats.SymlinkType:
		return ftSymlink
	case stats.DeviceType:
		return ftDevice
	case stats.PipeType:
		return ftPipe
	case stats.SocketType:
		return ftSocket
	case stats.CharType:
		return ftChar
	default:
		return ftUnknown
	}
}

func NormalizeMount(m string) string {
	if m == "" {
		return m
	}

	if !strings.HasSuffix(m, "/") {
		return m + "/"
	}

	return m
}

func IsDirPath(path string) bool {
	return strings.HasSuffix(path, "/")
}

func SplitParentAndName(path string) (parent, name string) {
	// Treat directories as having trailing slash in the input.
	p := path
	if IsDirPath(p) {
		p = p[:len(p)-1]
	}

	idx := strings.LastIndexByte(p, '/')
	if idx <= 0 {
		// Root-like; parent is "/" and name is remainder
		return "/", p
	}

	return p[:idx+1], p[idx+1:]
}

func ForEachAncestor(dir, mountPath string, fn func(a string) bool) {
	// dir must be a directory path ending with '/'
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	// Only iterate ancestors at or below mountPath
	if !strings.HasPrefix(dir, mountPath) {
		return
	}

	for {
		if !fn(dir) {
			return
		}

		if dir == mountPath {
			return
		}

		// Get parent directory by truncating after the last slash before the final slash
		lastSlash := strings.LastIndexByte(dir[:len(dir)-1], '/')
		if lastSlash < 0 {
			return
		}

		dir = dir[:lastSlash+1]
	}
}

// escapeCHSingleQuotes escapes single quotes for embedding string literals
// into ClickHouse queries.
func escapeCHSingleQuotes(s string) string { return strings.ReplaceAll(s, "'", "''") }

func DeriveExtLower(name string, isDir bool) string {
	if isDir {
		return ""
	}

	// Hidden files: ignore leading dot for ext purposes
	base := strings.TrimPrefix(name, ".")

	dot := strings.LastIndexByte(base, '.')
	if dot == -1 {
		return ""
	}

	ext := strings.ToLower(base[dot+1:])

	// Multi-dot rule for common compressed suffixes
	compressExts := map[string]bool{
		"gz": true, "bz2": true, "xz": true,
		"zst": true, "lz4": true, "lz": true, "br": true,
	}

	if compressExts[ext] {
		prevDot := strings.LastIndexByte(base[:dot], '.')
		if prevDot != -1 {
			prevExt := strings.ToLower(base[prevDot+1 : dot])
			if prevExt != "" {
				return prevExt + "." + ext
			}
		}
	}

	return ext
}

// SQL constants.
const (
	createScansTable = `
CREATE TABLE IF NOT EXISTS scans (
  mount_path String,
  scan_id UInt64,
  state Enum8('loading' = 0, 'ready' = 1),
  started_at DateTime,
  finished_at Nullable(DateTime)
) ENGINE = MergeTree
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
CREATE OR REPLACE VIEW fs_entries_current AS
SELECT e.*
FROM fs_entries e
INNER JOIN (
  SELECT mount_path, max(scan_id) AS scan_id
  FROM scans
  WHERE state = 'ready'
  GROUP BY mount_path
) r USING (mount_path, scan_id)`

	createRollupsCurrentView = `
CREATE OR REPLACE VIEW ancestor_rollups_current AS
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

// CreateSchema exports the internal createSchema function for testing.
func CreateSchema(ctx context.Context, conn clickhouse.Conn) error {
	return createSchema(ctx, conn)
}

func createSchema(ctx context.Context, conn clickhouse.Conn) error {
	// Create scans table first
	if err := conn.Exec(ctx, createScansTable); err != nil {
		return err
	}

	// Try fs_entries with path bloom filter, fallback if server doesn't support it
	if err := conn.Exec(ctx, createFsEntriesTable); err != nil {
		// Retry with no path index
		if err2 := conn.Exec(ctx, createFsEntriesTableNoPathIdx); err2 != nil {
			return err2
		}
	}

	// Create rollup raw table
	if err := conn.Exec(ctx, createRollupRawTable); err != nil {
		return err
	}

	// Create state table
	if err := conn.Exec(ctx, createRollupStateTable); err != nil {
		return err
	}

	// All required columns are declared in the CREATE statements above.

	// Create materialized view and current views
	if err := conn.Exec(ctx, createRollupMV); err != nil {
		return err
	}

	if err := conn.Exec(ctx, createFilesCurrentView); err != nil {
		return err
	}

	if err := conn.Exec(ctx, createRollupsCurrentView); err != nil {
		return err
	}

	return nil
}

// UpdateClickhouse exports the internal updateClickhouse function for testing.
func UpdateClickhouse(ctx context.Context, conn clickhouse.Conn, mountPath string, r io.Reader) error {
	return updateClickhouse(ctx, conn, mountPath, r)
}

func updateClickhouse(ctx context.Context, conn clickhouse.Conn, mountPath string, r io.Reader) (retErr error) {
	// Use current time as scan ID
	scanID := uint64(time.Now().Unix()) //nolint:gosec // monotonic timestamp scan identifier
	started := time.Now()

	// Register scan as loading
	if err := conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at) 
		VALUES (?, ?, 'loading', ?, NULL)`,
		mountPath, scanID, started); err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}

	// Rollback on failure: drop this scan's partitions and scan row
	defer func() {
		if retErr == nil {
			return
		}
		// Construct partition tuple literal
		part := fmt.Sprintf("('%s', %d)", escapeCHSingleQuotes(mountPath), scanID)

		// Drop partitions - ignoring errors on cleanup
		if err := conn.Exec(ctx, "ALTER TABLE fs_entries DROP PARTITION "+part); err != nil {
			// noop
		}
		if err := conn.Exec(ctx, "ALTER TABLE ancestor_rollups_raw DROP PARTITION "+part); err != nil {
			// noop
		}
		if err := conn.Exec(ctx, "ALTER TABLE ancestor_rollups_state DROP PARTITION "+part); err != nil {
			// noop
		}
		if err := conn.Exec(ctx, `ALTER TABLE scans DELETE WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID); err != nil {
			// noop
		}
	}()

	if err := ingestScan(ctx, conn, mountPath, scanID, r); err != nil {
		return err
	}

	// Promote to ready by inserting a new row (avoids ALTER UPDATE pitfalls)
	finished := time.Now()
	if err := conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at)
		VALUES (?, ?, 'ready', ?, ?)`,
		mountPath, scanID, started, finished); err != nil {
		return fmt.Errorf("promote scan (insert ready): %w", err)
	}

	// Drop older scans for this mount
	if err := dropOlderScans(ctx, conn, mountPath, scanID); err != nil {
		return fmt.Errorf("retention: %w", err)
	}

	return nil
}

// Helper functions for batched processing of files.
type chBatch interface {
	Append(values ...any) error
	Send() error
}

type batchProcessor struct {
	filesBatch      chBatch
	rollupsBatch    chBatch
	conn            clickhouse.Conn
	filesCount      int
	rollupsCount    int
	mountPath       string
	scanID          uint64
	filesBatchSQL   string
	rollupsBatchSQL string
}

// Create a new batch processor.
func newBatchProcessor(ctx context.Context, conn clickhouse.Conn,
	mountPath string, scanID uint64) (*batchProcessor, error) {
	filesBatchSQL := `
		INSERT INTO fs_entries 
		(mount_path, scan_id, path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime)`

	rollupsBatchSQL := `
		INSERT INTO ancestor_rollups_raw 
		(mount_path, scan_id, ancestor, size, atime, mtime, uid, gid, ext_low)`

	filesBatch, err := conn.PrepareBatch(ctx, filesBatchSQL)
	if err != nil {
		return nil, err
	}

	rollupsBatch, err := conn.PrepareBatch(ctx, rollupsBatchSQL)
	if err != nil {
		return nil, err
	}

	return &batchProcessor{
		filesBatch:      filesBatch,
		rollupsBatch:    rollupsBatch,
		conn:            conn,
		mountPath:       mountPath,
		scanID:          scanID,
		filesBatchSQL:   filesBatchSQL,
		rollupsBatchSQL: rollupsBatchSQL,
	}, nil
}

// Add a file entry to the batch.
func (bp *batchProcessor) addFile(path string, parent string, name string, ext string,
	ft ftype, inode uint64, size uint64, uid uint32, gid uint32, mtime, atime, ctime time.Time) error {
	if err := bp.filesBatch.Append(
		bp.mountPath, bp.scanID, path, parent, name, ext, uint8(ft),
		inode, size, uid, gid, mtime, atime, ctime,
	); err != nil {
		return err
	}

	bp.filesCount++

	return nil
}

// Add an ancestor rollup entry to the batch.
func (bp *batchProcessor) addRollup(ancestor string, size uint64,
	atime, mtime time.Time, uid, gid uint32, ext string) error {
	if err := bp.rollupsBatch.Append(
		bp.mountPath, bp.scanID, ancestor, size, atime, mtime, uid, gid, ext); err != nil {
		return err
	}

	bp.rollupsCount++

	return nil
}

// Check if either batch needs flushing.
func (bp *batchProcessor) needsFlush() bool {
	return bp.filesCount >= fileBatchSize || bp.rollupsCount >= rollupBatchSize
}

// Flush both batches if they contain any data.
func (bp *batchProcessor) flush(ctx context.Context) error {
	// Send files batch if non-empty
	if bp.filesCount > 0 {
		if err := bp.filesBatch.Send(); err != nil {
			return err
		}

		bp.filesCount = 0

		filesBatch, err := bp.conn.PrepareBatch(ctx, bp.filesBatchSQL)
		if err != nil {
			return err
		}

		bp.filesBatch = filesBatch
	}

	// Send rollups batch if non-empty
	if bp.rollupsCount > 0 {
		if err := bp.rollupsBatch.Send(); err != nil {
			return err
		}

		bp.rollupsCount = 0

		rollupsBatch, err := bp.conn.PrepareBatch(ctx, bp.rollupsBatchSQL)
		if err != nil {
			return err
		}

		bp.rollupsBatch = rollupsBatch
	}

	return nil
}

func ingestScan(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64, r io.Reader) error {
	// Create batch processor
	bp, err := newBatchProcessor(ctx, conn, mountPath, scanID)
	if err != nil {
		return err
	}

	parser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)

	var parseErr error

	for {
		// Read the next entry
		if parseErr = parser.Scan(fi); parseErr != nil {
			break
		}

		path := string(fi.Path)
		isDir := fi.EntryType == stats.DirType || IsDirPath(path)

		parent, name := SplitParentAndName(path)
		ext := DeriveExtLower(name, isDir)
		ft := mapEntryType(fi.EntryType)
		mtime := time.Unix(fi.MTime, 0)
		atime := time.Unix(fi.ATime, 0)
		ctime := time.Unix(fi.CTime, 0)

		// Handle potential integer overflow by using explicit conversions
		inode := uint64(0)
		if fi.Inode > 0 {
			inode = uint64(fi.Inode) //nolint:gosec // values originate from trusted stats parser
		}

		size := uint64(0)
		if fi.Size > 0 {
			size = uint64(fi.Size) //nolint:gosec // values originate from trusted stats parser
		}

		// Add file entry to batch
		if err := bp.addFile(path, parent, name, ext, ft, inode, size,
			fi.UID, fi.GID, mtime, atime, ctime); err != nil {
			return fmt.Errorf("failed to add file entry: %w", err)
		}

		// Rollups for each ancestor directory (including mount root)
		// Include the directory itself in its own subtree if the entry is a directory
		base := parent
		if isDir {
			base = path
		}

		// Process all ancestors
		var ancestorErr error

		ForEachAncestor(base, mountPath, func(a string) bool {
			if err := bp.addRollup(a, size, atime, mtime, fi.UID, fi.GID, ext); err != nil {
				ancestorErr = err

				return false
			}

			return true
		})

		if ancestorErr != nil {
			return fmt.Errorf("failed to add ancestor rollup: %w", ancestorErr)
		}

		// Flush batches if needed
		if bp.needsFlush() {
			if err := bp.flush(ctx); err != nil {
				return fmt.Errorf("failed to flush batches: %w", err)
			}
		}
	}

	// Check for parser errors (excluding EOF which is expected)
	if !errors.Is(parseErr, io.EOF) {
		return fmt.Errorf("parser error: %w", parseErr)
	}

	// Final flush
	return bp.flush(ctx)
}

func dropOlderScans(ctx context.Context, conn clickhouse.Conn, mountPath string, keepScanID uint64) error {
	// Get older scan_ids for this mount
	rows, err := conn.Query(ctx, `
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

		// Construct partition tuple literal safely
		part := fmt.Sprintf("('%s', %d)", escapeCHSingleQuotes(mountPath), sid)

		// Drop partitions from tables
		for _, dropStmt := range []string{
			"ALTER TABLE fs_entries DROP PARTITION " + part,
			"ALTER TABLE ancestor_rollups_raw DROP PARTITION " + part,
			"ALTER TABLE ancestor_rollups_state DROP PARTITION " + part,
		} {
			if err := conn.Exec(ctx, dropStmt); err != nil {
				return err
			}
		}

		// Delete scan record
		if err := conn.Exec(ctx, `
			ALTER TABLE scans DELETE
			WHERE mount_path = ? AND scan_id = ?`,
			mountPath, sid); err != nil {
			return err
		}
	}

	return rows.Err()
}

// --- Query helpers ---.

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

type Filters struct {
	GIDs        []uint32
	UIDs        []uint32
	Exts        []string
	ATimeBucket string // one of: 0d, >1m, >2m, >6m, >1y, >2y, >3y, >5y, >7y
	MTimeBucket string // same set
}

func GetLastScanTimes(ctx context.Context, conn clickhouse.Conn) (map[string]time.Time, error) {
	// Query the most recent scan_id for each mount that is in the 'ready' state
	rows, err := conn.Query(ctx, `
		SELECT mount_path, toDateTime(max(scan_id)) 
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
		var mountPath string

		var timestamp time.Time

		if err := rows.Scan(&mountPath, &timestamp); err != nil {
			return nil, err
		}

		result[mountPath] = timestamp
	}

	return result, rows.Err()
}

func ListImmediateChildren(ctx context.Context, conn clickhouse.Conn, mountPath, dir string) ([]FileEntry, error) {
	// Ensure the directory path ends with a slash
	dir = ensureDir(dir)

	// Query direct children of the directory
	rows, err := conn.Query(ctx, `
		SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
		FROM fs_entries_current
		WHERE mount_path = ? AND parent_path = ?`,
		mountPath, dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results
	var results []FileEntry
	for rows.Next() {
		var entry FileEntry
		if err := rows.Scan(
			&entry.Path, &entry.ParentPath, &entry.Name, &entry.Ext,
			&entry.FType, &entry.INode, &entry.Size,
			&entry.UID, &entry.GID,
			&entry.MTime, &entry.ATime, &entry.CTime); err != nil {
			return nil, err
		}

		results = append(results, entry)
	}

	return results, rows.Err()
}

func ensureDir(path string) string {
	if !strings.HasSuffix(path, "/") {
		return path + "/"
	}

	return path
}

var errInvalidBucket = errors.New("invalid bucket")

func buildBucketPredicate(col, bucket string) (string, error) {
	if bucket == "" {
		return "", nil
	}

	// Map of bucket identifiers to their time interval expressions
	timeIntervals := map[string]string{
		"0d":  ">= (toDateTime(max_scan) - INTERVAL 1 DAY)",
		">1m": "< (toDateTime(max_scan) - INTERVAL 1 MONTH)",
		">2m": "< (toDateTime(max_scan) - INTERVAL 2 MONTH)",
		">6m": "< (toDateTime(max_scan) - INTERVAL 6 MONTH)",
		">1y": "< (toDateTime(max_scan) - INTERVAL 1 YEAR)",
		">2y": "< (toDateTime(max_scan) - INTERVAL 2 YEAR)",
		">3y": "< (toDateTime(max_scan) - INTERVAL 3 YEAR)",
		">5y": "< (toDateTime(max_scan) - INTERVAL 5 YEAR)",
		">7y": "< (toDateTime(max_scan) - INTERVAL 7 YEAR)",
	}

	interval, found := timeIntervals[bucket]
	if !found {
		return "", fmt.Errorf("%w: %s", errInvalidBucket, bucket)
	}

	return col + " " + interval, nil
}

func SubtreeSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string, f Filters) (Summary, error) {
	dir = ensureDir(dir)

	// Fast path when no filters other than path/mount.
	if len(f.GIDs) == 0 && len(f.UIDs) == 0 && len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == "" {
		row := conn.QueryRow(ctx, `
WITH (SELECT max(scan_id) FROM scans WHERE mount_path = ? AND state = 'ready') AS max_scan
SELECT 
  sum(size),
  count(),
  max(atime),
  min(atime),
  max(mtime),
  min(mtime),
  groupUniqArray(uid),
  groupUniqArray(gid),
  groupUniqArray(ext_low)
FROM fs_entries_current
WHERE mount_path = ? AND path LIKE ?`, mountPath, mountPath, dir+"%")

		var s Summary
		if err := row.Scan(&s.TotalSize, &s.FileCount, &s.MostRecentATime, &s.OldestATime,
			&s.MostRecentMTime, &s.OldestMTime, &s.UIDs, &s.GIDs, &s.Exts); err != nil {
			return Summary{}, err
		}

		return s, nil
	}

	// Basic where clauses that apply to all queries
	where := []string{"mount_path = ?", "path LIKE ?"}
	args := []any{mountPath, dir + "%"}

	// Add filter for GIDs if provided
	if len(f.GIDs) > 0 {
		placeholders := make([]string, len(f.GIDs))
		for i, v := range f.GIDs {
			placeholders[i] = "?"

			args = append(args, v)
		}

		where = append(where, fmt.Sprintf("gid IN (%s)", strings.Join(placeholders, ",")))
	}

	// Add filter for UIDs if provided
	if len(f.UIDs) > 0 {
		placeholders := make([]string, len(f.UIDs))
		for i, v := range f.UIDs {
			placeholders[i] = "?"

			args = append(args, v)
		}

		where = append(where, fmt.Sprintf("uid IN (%s)", strings.Join(placeholders, ",")))
	}

	// Add filter for extensions if provided
	if len(f.Exts) > 0 {
		placeholders := make([]string, len(f.Exts))
		for i, v := range f.Exts {
			placeholders[i] = "?"

			args = append(args, strings.ToLower(v))
		}

		where = append(where, fmt.Sprintf("ext_low IN (%s)", strings.Join(placeholders, ",")))
	}

	// Build time bucket filters
	bucketFilter := ""
	if f.ATimeBucket != "" || f.MTimeBucket != "" {
		predicates := []string{}

		// Access time filter
		if f.ATimeBucket != "" {
			atPred, err := buildBucketPredicate("atime", f.ATimeBucket)
			if err != nil {
				return Summary{}, err
			}

			if atPred != "" {
				predicates = append(predicates, atPred)
			}
		}

		// Modification time filter
		if f.MTimeBucket != "" {
			mtPred, err := buildBucketPredicate("mtime", f.MTimeBucket)
			if err != nil {
				return Summary{}, err
			}

			if mtPred != "" {
				predicates = append(predicates, mtPred)
			}
		}

		// Combine predicates
		if len(predicates) > 0 {
			bucketFilter = " AND (" + strings.Join(predicates, " AND ") + ")"
		}
	}

	// Construct the full query with CTE for max scan_id
	query := `
WITH (SELECT max(scan_id) FROM scans WHERE mount_path = ? AND state = 'ready') AS max_scan
SELECT 
  sum(size) AS total_size,
  count() AS file_count,
  max(atime) AS most_recent_atime,
  min(atime) AS oldest_atime,
  max(mtime) AS most_recent_mtime,
  min(mtime) AS oldest_mtime,
  groupUniqArray(uid) AS uids,
  groupUniqArray(gid) AS gids,
  groupUniqArray(ext_low) AS exts
FROM fs_entries_current
WHERE ` + strings.Join(where, " AND ") + bucketFilter

	// Add mountPath as the first argument for the CTE
	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, mountPath)
	allArgs = append(allArgs, args...)

	// Execute query and scan result
	row := conn.QueryRow(ctx, query, allArgs...)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts); err != nil {
		return Summary{}, err
	}

	return s, nil
}

// OptimizedSubtreeSummary attempts to use precomputed ancestor rollups when
// possible. Falls back to the regular implementation for filtered queries.
func OptimizedSubtreeSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string, f Filters) (Summary, error) {
	// Only use rollups when there are no filters at all
	useRollups := len(f.GIDs) == 0 && len(f.UIDs) == 0 &&
		len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == ""

	if useRollups {
		dir = ensureDir(dir)

		// Use the precomputed rollups table for better performance
		query := `
		SELECT 
			total_size,
			file_count,
			atime_max AS most_recent_atime,
			atime_min AS oldest_atime,
			mtime_max AS most_recent_mtime,
			mtime_min AS oldest_mtime,
			uids,
			gids,
			exts
		FROM ancestor_rollups_current
		WHERE mount_path = ? AND ancestor = ?`

		row := conn.QueryRow(ctx, query, mountPath, dir)

		var s Summary
		if err := row.Scan(
			&s.TotalSize, &s.FileCount,
			&s.MostRecentATime, &s.OldestATime,
			&s.MostRecentMTime, &s.OldestMTime,
			&s.UIDs, &s.GIDs, &s.Exts); err != nil {
			// If no rows or other error, return empty summary (not an error condition)
			if errors.Is(err, io.EOF) {
				return Summary{}, nil
			}

			return Summary{}, err
		}

		return s, nil
	}

	// Fall back to regular implementation for filtered queries
	return SubtreeSummary(ctx, conn, mountPath, dir, f)
}

func SearchGlobPaths(ctx context.Context, conn clickhouse.Conn, mountPath, globPattern string, limit int, caseInsensitive bool) ([]string, error) {
	// Convert glob pattern to SQL LIKE pattern
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	// Build the query based on case sensitivity
	var query string
	if caseInsensitive {
		query = `SELECT path FROM fs_entries_current 
			WHERE mount_path = ? AND lowerUTF8(path) LIKE lowerUTF8(?)`
	} else {
		query = `SELECT path FROM fs_entries_current 
			WHERE mount_path = ? AND path LIKE ?`
	}

	// Add limit if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	// Execute query
	rows, err := conn.Query(ctx, query, mountPath, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results
	var result []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}

		result = append(result, path)
	}

	return result, rows.Err()
}
