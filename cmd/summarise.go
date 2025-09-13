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
// setupClickHouseConnection creates and configures a new ClickHouse connection.
//
//nolint:ireturn // We need to return the interface for compatibility with other functions
func setupClickHouseConnection() (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
		Auth:        clickhouse.Auth{Database: chDatabase, Username: chUsername, Password: chPassword},
		DialTimeout: dialTimeoutSeconds * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Settings: clickhouse.Settings{
			"max_insert_block_size":       maxInsertBlockSize,
			"min_insert_block_size_rows":  minInsertBlockRows,
			"min_insert_block_size_bytes": minInsertBlockBytes, // 10MB
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	return conn, nil
}

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

	conn, err := setupClickHouseConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := createSchema(ctx, conn); err != nil {
		return fmt.Errorf("createSchema: %w", err)
	}

	return updateClickhouse(ctx, conn, mountPath, r)
}

func checkArgs(args []string) (string, string, error) {
	if len(args) != expectedArgCount {
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

	// ClickHouse connection settings.
	dialTimeoutSeconds  = 10
	maxInsertBlockSize  = 1000000
	minInsertBlockRows  = 100000
	minInsertBlockBytes = 10485760 // 10MB

	// Command line arguments.
	expectedArgCount = 2

	// Default capacity for result slices.
	defaultResultCapacity = 100
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

// typeMap maps stats package entry type bytes to our internal ftype representation.
var typeMap = map[byte]ftype{
	stats.FileType:    ftFile,
	stats.DirType:     ftDir,
	stats.SymlinkType: ftSymlink,
	stats.DeviceType:  ftDevice,
	stats.PipeType:    ftPipe,
	stats.SocketType:  ftSocket,
	stats.CharType:    ftChar,
}

func mapEntryType(b byte) ftype {
	if ft, ok := typeMap[b]; ok {
		return ft
	}

	return ftUnknown
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

// isCompressedExtension checks if the given extension is a compressed file extension.
func isCompressedExtension(ext string) bool {
	compressExts := map[string]bool{
		"gz": true, "bz2": true, "xz": true,
		"zst": true, "lz4": true, "lz": true, "br": true,
	}

	return compressExts[ext]
}

// handleCompressedExtension processes compound extensions like .tar.gz, .csv.bz2, etc.
func handleCompressedExtension(base string, dot int, ext string) string {
	prevDot := strings.LastIndexByte(base[:dot], '.')
	if prevDot == -1 {
		return ext
	}

	prevExt := strings.ToLower(base[prevDot+1 : dot])
	if prevExt == "" {
		return ext
	}

	return prevExt + "." + ext
}

func DeriveExtLower(name string, isDir bool) string {
	// Directories and files without extensions return empty string
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

	// If not a compressed extension, return as is
	if !isCompressedExtension(ext) {
		return ext
	}

	// Handle compound extensions for compressed files
	return handleCompressedExtension(base, dot, ext)
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

// createTableWithStatement executes a CREATE TABLE statement and returns any error.
func createTableWithStatement(ctx context.Context, conn clickhouse.Conn, statement string) error {
	return conn.Exec(ctx, statement)
}

// createFsEntriesTableWithFallback tries to create the fs_entries table with path bloom filter,
// but falls back to no path index if the server doesn't support it.
func createFsEntriesTableWithFallback(ctx context.Context, conn clickhouse.Conn) error {
	// Try fs_entries with path bloom filter, fallback if server doesn't support it
	if err := conn.Exec(ctx, createFsEntriesTable); err != nil {
		// Retry with no path index
		if err2 := conn.Exec(ctx, createFsEntriesTableNoPathIdx); err2 != nil {
			return err2
		}
	}

	return nil
}

// createViews creates all the necessary views for the schema.
func createViews(ctx context.Context, conn clickhouse.Conn) error {
	//nolint:misspell // ClickHouse requires American English spelling "materialized"
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

func createSchema(ctx context.Context, conn clickhouse.Conn) error {
	// Create scans table first
	if err := createTableWithStatement(ctx, conn, createScansTable); err != nil {
		return err
	}

	// Create fs_entries table with fallback
	if err := createFsEntriesTableWithFallback(ctx, conn); err != nil {
		return err
	}

	// Create rollup raw table
	if err := createTableWithStatement(ctx, conn, createRollupRawTable); err != nil {
		return err
	}

	// Create state table
	if err := createTableWithStatement(ctx, conn, createRollupStateTable); err != nil {
		return err
	}

	// Create all views
	return createViews(ctx, conn)
}

// UpdateClickhouse exports the internal updateClickhouse function for testing.
func UpdateClickhouse(ctx context.Context, conn clickhouse.Conn, mountPath string, r io.Reader) error {
	return updateClickhouse(ctx, conn, mountPath, r)
}

// dropPartitionIgnoreErrors executes a query and ignores any errors.
// #nosec G104 - we intentionally ignore errors here for cleanup operations
func dropPartitionIgnoreErrors(ctx context.Context, conn clickhouse.Conn, query string, args ...any) {
	// We intentionally ignore errors from these cleanup operations
	//nolint:errcheck
	conn.Exec(ctx, query, args...)
}

// registerScan adds a new scan record with 'loading' state.
func registerScan(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64, started time.Time) error {
	err := conn.Exec(ctx, `
		INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at) 
		VALUES (?, ?, 'loading', ?, NULL)`,
		mountPath, scanID, started)
	if err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}

	return nil
}

// setupRollbackHandler creates a deferred function that handles cleanup on error.
func setupRollbackHandler(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64) func(error) {
	return func(retErr error) {
		if retErr == nil {
			return
		}
		// Construct partition tuple literal
		part := fmt.Sprintf("('%s', %d)", escapeCHSingleQuotes(mountPath), scanID)

		// Drop partitions - ignoring errors on cleanup
		// We use a separate function to avoid the empty block lint warnings
		dropPartitionIgnoreErrors(ctx, conn, "ALTER TABLE fs_entries DROP PARTITION "+part)
		dropPartitionIgnoreErrors(ctx, conn, "ALTER TABLE ancestor_rollups_raw DROP PARTITION "+part)
		dropPartitionIgnoreErrors(ctx, conn, "ALTER TABLE ancestor_rollups_state DROP PARTITION "+part)
		dropPartitionIgnoreErrors(ctx, conn,
			`ALTER TABLE scans DELETE WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID)
	}
}

func updateClickhouse(ctx context.Context, conn clickhouse.Conn, mountPath string, r io.Reader) (retErr error) {
	// Use current time as scan ID
	scanID := uint64(time.Now().Unix()) //nolint:gosec // monotonic timestamp scan identifier
	started := time.Now()

	// Register scan as loading
	if err := registerScan(ctx, conn, mountPath, scanID, started); err != nil {
		return err
	}

	// Set up rollback handler for cleanup on error
	defer setupRollbackHandler(ctx, conn, mountPath, scanID)(retErr)

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
	if err := bp.flushFilesBatch(ctx); err != nil {
		return err
	}

	if err := bp.flushRollupsBatch(ctx); err != nil {
		return err
	}

	return nil
}

// flushFilesBatch sends the files batch if it's non-empty.
func (bp *batchProcessor) flushFilesBatch(ctx context.Context) error {
	if bp.filesCount == 0 {
		return nil
	}

	if err := bp.filesBatch.Send(); err != nil {
		return err
	}

	bp.filesCount = 0

	filesBatch, err := bp.conn.PrepareBatch(ctx, bp.filesBatchSQL)
	if err != nil {
		return err
	}

	bp.filesBatch = filesBatch

	return nil
}

// flushRollupsBatch sends the rollups batch if it's non-empty.
func (bp *batchProcessor) flushRollupsBatch(ctx context.Context) error {
	if bp.rollupsCount == 0 {
		return nil
	}

	if err := bp.rollupsBatch.Send(); err != nil {
		return err
	}

	bp.rollupsCount = 0

	rollupsBatch, err := bp.conn.PrepareBatch(ctx, bp.rollupsBatchSQL)
	if err != nil {
		return err
	}

	bp.rollupsBatch = rollupsBatch

	return nil
}

// processFileEntry handles a single file entry during scan ingestion.
func processFileEntry(bp *batchProcessor, fi *stats.FileInfo, mountPath string) error {
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
		inode = uint64(fi.Inode) // Values originate from trusted stats parser
	}

	size := uint64(0)
	if fi.Size > 0 {
		size = uint64(fi.Size) // Values originate from trusted stats parser
	}

	// Add file entry to batch
	if err := bp.addFile(path, parent, name, ext, ft, inode, size,
		fi.UID, fi.GID, mtime, atime, ctime); err != nil {
		return fmt.Errorf("failed to add file entry: %w", err)
	}

	return processAncestorRollups(bp, fi, path, parent, isDir, size, atime, mtime, ext, mountPath)
}

// processAncestorRollups processes rollups for all ancestor directories.
// It calculates rollups for each directory in the path hierarchy.
func processAncestorRollups(bp *batchProcessor, fi *stats.FileInfo, path, parent string,
	isDir bool, size uint64, atime, mtime time.Time, ext, mountPath string) error {
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

	return nil
}

// processScanEntry processes a single entry during scan ingestion.
// Returns a boolean indicating if we should continue scanning, and any error encountered.
func processScanEntry(ctx context.Context, bp *batchProcessor, fi *stats.FileInfo, mountPath string) (bool, error) {
	// Process the file entry
	if err := processFileEntry(bp, fi, mountPath); err != nil {
		return false, err
	}

	// Flush batches if needed
	if bp.needsFlush() {
		if err := bp.flush(ctx); err != nil {
			return false, fmt.Errorf("failed to flush batches: %w", err)
		}
	}

	return true, nil
}

func ingestScan(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64, r io.Reader) error {
	// Create batch processor
	bp, err := newBatchProcessor(ctx, conn, mountPath, scanID)
	if err != nil {
		return err
	}

	if err := scanAndProcessEntries(ctx, bp, r, mountPath); err != nil {
		return err
	}

	// Final flush
	return bp.flush(ctx)
}

// scanAndProcessEntries scans through the file records and processes each entry.
func scanAndProcessEntries(ctx context.Context, bp *batchProcessor, r io.Reader, mountPath string) error {
	parser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)

	var parseErr error

	for {
		// Read the next entry
		if parseErr = parser.Scan(fi); parseErr != nil {
			break
		}

		shouldContinue, err := processScanEntry(ctx, bp, fi, mountPath)
		if !shouldContinue || err != nil {
			return err
		}
	}

	// Check for parser errors (excluding EOF which is expected)
	if !errors.Is(parseErr, io.EOF) {
		return fmt.Errorf("parser error: %w", parseErr)
	}

	return nil
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

		if err := dropSingleScan(ctx, conn, mountPath, sid); err != nil {
			return err
		}
	}

	return rows.Err()
}

// dropSingleScan drops all data for a single scan ID.
func dropSingleScan(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64) error {
	// Construct partition tuple literal safely
	part := fmt.Sprintf("('%s', %d)", escapeCHSingleQuotes(mountPath), scanID)

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
		mountPath, scanID); err != nil {
		return err
	}

	return nil
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

	// Collect results - preallocate for better performance
	results := make([]FileEntry, 0, defaultResultCapacity) // Start with a reasonable capacity
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
	if isNoFilters(f) {
		return getUnfilteredSummary(ctx, conn, mountPath, dir)
	}

	// Build query for filtered results
	where, args := buildBasicWhereClause(mountPath, dir)
	where, args = appendFilterClauses(where, args, f)

	// Build time bucket filter
	bucketFilter := buildTimeBucketFilter(f)

	// Construct the full query with CTE for max scan_id
	query := buildSummaryQuery(where, bucketFilter)

	// Add mountPath as the first argument for the CTE
	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, mountPath)
	allArgs = append(allArgs, args...)

	// Execute query and scan result
	return executeSummaryQuery(ctx, conn, query, allArgs)
}

// Helper function to check if no filters are applied.
func isNoFilters(f Filters) bool {
	return len(f.GIDs) == 0 && len(f.UIDs) == 0 &&
		len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == ""
}

// Helper function for getting unfiltered summary.
func getUnfilteredSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string) (Summary, error) {
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

// Build basic where clause and args for the query.
func buildBasicWhereClause(mountPath, dir string) ([]string, []any) {
	where := []string{"mount_path = ?", "path LIKE ?"}
	args := []any{mountPath, dir + "%"}

	return where, args
}

// Append filter clauses based on the provided filters.
func appendFilterClauses(where []string, args []any, f Filters) ([]string, []any) {
	// Add filter for GIDs if provided
	if len(f.GIDs) > 0 {
		gidClause, gidArgs := buildInClause("gid", f.GIDs)
		where = append(where, gidClause)
		args = append(args, gidArgs...)
	}

	// Add filter for UIDs if provided
	if len(f.UIDs) > 0 {
		uidClause, uidArgs := buildInClause("uid", f.UIDs)
		where = append(where, uidClause)
		args = append(args, uidArgs...)
	}

	// Add filter for extensions if provided
	if len(f.Exts) > 0 {
		extClause, extArgs := buildExtensionClause(f.Exts)
		where = append(where, extClause)
		args = append(args, extArgs...)
	}

	return where, args
}

// Build IN clause for filters.
func buildInClause(field string, values []uint32) (string, []any) {
	placeholders := make([]string, len(values))
	args := make([]any, len(values))

	for i, v := range values {
		placeholders[i] = "?"
		args[i] = v
	}

	return fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")), args
}

// Build extension filter clause.
func buildExtensionClause(exts []string) (string, []any) {
	placeholders := make([]string, len(exts))
	args := make([]any, len(exts))

	for i, v := range exts {
		placeholders[i] = "?"
		args[i] = strings.ToLower(v)
	}

	return fmt.Sprintf("ext_low IN (%s)", strings.Join(placeholders, ",")), args
}

// Build time bucket filter clause.
// buildAccessTimeFilter builds the access time filter predicate.
func buildAccessTimeFilter(aTimeBucket string) string {
	if aTimeBucket == "" {
		return ""
	}

	atPred, err := buildBucketPredicate("atime", aTimeBucket)
	if err != nil || atPred == "" {
		return ""
	}

	return atPred
}

// buildModificationTimeFilter builds the modification time filter predicate.
func buildModificationTimeFilter(mTimeBucket string) string {
	if mTimeBucket == "" {
		return ""
	}

	mtPred, err := buildBucketPredicate("mtime", mTimeBucket)
	if err != nil || mtPred == "" {
		return ""
	}

	return mtPred
}

// joinTimeFilters combines multiple time filter predicates.
func joinTimeFilters(predicates []string) string {
	if len(predicates) == 0 {
		return ""
	}

	return " AND (" + strings.Join(predicates, " AND ") + ")"
}

func buildTimeBucketFilter(f Filters) string {
	if f.ATimeBucket == "" && f.MTimeBucket == "" {
		return ""
	}

	var predicates []string

	// Access time filter
	if atPred := buildAccessTimeFilter(f.ATimeBucket); atPred != "" {
		predicates = append(predicates, atPred)
	}

	// Modification time filter
	if mtPred := buildModificationTimeFilter(f.MTimeBucket); mtPred != "" {
		predicates = append(predicates, mtPred)
	}

	return joinTimeFilters(predicates)
}

// Build the complete summary query.
func buildSummaryQuery(where []string, bucketFilter string) string {
	return `
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
}

// Execute the summary query and return results.
func executeSummaryQuery(ctx context.Context, conn clickhouse.Conn, query string, args []any) (Summary, error) {
	row := conn.QueryRow(ctx, query, args...)

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
func OptimizedSubtreeSummary(
	ctx context.Context,
	conn clickhouse.Conn,
	mountPath, dir string,
	f Filters,
) (Summary, error) {
	// Only use rollups when there are no filters at all
	if !isNoFilters(f) {
		// Fall back to regular implementation for filtered queries
		return SubtreeSummary(ctx, conn, mountPath, dir, f)
	}

	// Use precomputed rollups
	return getRollupSummary(ctx, conn, mountPath, ensureDir(dir))
} // Helper function to retrieve summary from the rollups table.
func getRollupSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string) (Summary, error) {
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

// buildGlobSearchQuery constructs a SQL query for searching paths with a glob pattern.
func buildGlobSearchQuery(caseInsensitive bool, limit int) string {
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

	return query
}

// SearchGlobPaths searches for paths matching a glob pattern in ClickHouse.
func SearchGlobPaths(
	ctx context.Context,
	conn clickhouse.Conn,
	mountPath, globPattern string,
	limit int,
	caseInsensitive bool,
) ([]string, error) {
	// Build the query
	query := buildGlobSearchQuery(caseInsensitive, limit)

	// Convert glob pattern to SQL LIKE pattern
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	// Execute query
	rows, err := conn.Query(ctx, query, mountPath, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results - preallocate for better performance
	result := make([]string, 0, defaultResultCapacity) // Start with a reasonable capacity
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}

		result = append(result, path)
	}

	return result, rows.Err()
}
