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

	// ClickHouse connection settings
	chHost     string
	chPort     string
	chDatabase string
	chUsername string
	chPassword string
)

const dbBatchSize = 10000

// summariseCmd represents the stat command.
var summariseCmd = &cobra.Command{
	Use:   "summarise <mount_path> <stats_file|->",
	Short: "Summarise stat data",
	Long: `Summarise stat data into ClickHouse for fast, interactive queries.

This command ingests a single mount's scan into ClickHouse with atomic promotion
semantics (loading -> ready), maintains only the latest ready scan per mount,
and provides precomputed subtree rollups.`,
	Run: func(_ *cobra.Command, args []string) {
		if err := run(args); err != nil {
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

func run(args []string) (err error) {
	mountPath, statsPath, err := checkArgs(args)
	if err != nil {
		return err
	}

	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return err
	}

	defer r.Close()

	return updateClickhouse(mountPath, r)
}

func checkArgs(args []string) (string, string, error) {
	if len(args) != 2 {
		return "", "", errors.New("usage: summarise <mount_path> <stats_file|->") //nolint:err113
	}
	mountPath := normalizeMount(args[0])
	statsPath := args[1]
	if mountPath == "/" {
		return "", "", errors.New("mount_path must not be '/' — use the real mount point path") //nolint:err113
	}
	return mountPath, statsPath, nil
}

// Helper to close multiple resources when wrapping readers
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

// --- ClickHouse schema and ingestion ---

const (
	fileBatchSize   = 500_000 // Increased for better performance
	rollupBatchSize = 500_000 // Increased for better performance
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

func normalizeMount(m string) string {
	if m == "" {
		return m
	}
	if !strings.HasSuffix(m, "/") {
		m += "/"
	}
	return m
}

func isDirPath(path string) bool {
	return strings.HasSuffix(path, "/")
}

func splitParentAndName(path string) (parent, name string) {
	// Treat directories as having trailing slash in the input.
	p := path
	if isDirPath(p) {
		p = p[:len(p)-1]
	}
	idx := strings.LastIndexByte(p, '/')
	if idx <= 0 {
		// Root-like; parent is "/" and name is remainder
		return "/", p
	}
	return p[:idx+1], p[idx+1:]
}

func forEachAncestor(dir, mountPath string, fn func(a string) bool) {
	// dir must be a directory path ending with '/'
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	// Only iterate ancestors at or below mountPath
	if !strings.HasPrefix(dir, mountPath) {
		return
	}
	cur := dir
	for {
		if !fn(cur) {
			return
		}
		if cur == mountPath {
			return
		}
		trim := strings.TrimRight(cur, "/")
		idx := strings.LastIndexByte(trim, '/')
		if idx <= 0 {
			return
		}
		cur = trim[:idx+1]
	}
}

func deriveExtLower(name string, isDir bool) string {
	if isDir {
		return ""
	}
	// Hidden files: ignore leading dot for ext purposes
	base := strings.TrimPrefix(name, ".")
	dot := strings.LastIndexByte(base, '.')
	if dot == -1 {
		return ""
	}
	ext1 := base[dot+1:]
	lower1 := strings.ToLower(ext1)
	// Multi-dot rule for common compressed suffixes -> previousExt+"."+compressExt
	switch lower1 {
	case "gz", "bz2", "xz", "zst", "lz4", "lz", "br":
		prev := strings.LastIndexByte(base[:dot], '.')
		if prev != -1 {
			prevExt := strings.ToLower(base[prev+1 : dot])
			if prevExt != "" {
				return prevExt + "." + lower1
			}
		}
		return lower1
	}
	return lower1
}

// SQL constants
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

	createRollupRawTable = `
CREATE TABLE IF NOT EXISTS ancestor_rollups_raw (
  mount_path String,
  scan_id UInt64,
  ancestor String,
  size UInt64,
  atime DateTime,
  mtime DateTime
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
  sumState(1) AS file_count,
  minState(atime) AS atime_min,
  maxState(atime) AS atime_max,
  minState(mtime) AS mtime_min,
  maxState(mtime) AS mtime_max,
  sumIfState(size, atime >= (scan_time - INTERVAL 1 DAY)) AS at_within_0d_size,
  sumIfState(1, atime >= (scan_time - INTERVAL 1 DAY)) AS at_within_0d_count,
  sumIfState(size, atime < (scan_time - INTERVAL 1 MONTH)) AS at_older_1m_size,
  sumIfState(1, atime < (scan_time - INTERVAL 1 MONTH)) AS at_older_1m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 2 MONTH)) AS at_older_2m_size,
  sumIfState(1, atime < (scan_time - INTERVAL 2 MONTH)) AS at_older_2m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 6 MONTH)) AS at_older_6m_size,
  sumIfState(1, atime < (scan_time - INTERVAL 6 MONTH)) AS at_older_6m_count,
  sumIfState(size, atime < (scan_time - INTERVAL 1 YEAR)) AS at_older_1y_size,
  sumIfState(1, atime < (scan_time - INTERVAL 1 YEAR)) AS at_older_1y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 2 YEAR)) AS at_older_2y_size,
  sumIfState(1, atime < (scan_time - INTERVAL 2 YEAR)) AS at_older_2y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 3 YEAR)) AS at_older_3y_size,
  sumIfState(1, atime < (scan_time - INTERVAL 3 YEAR)) AS at_older_3y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 5 YEAR)) AS at_older_5y_size,
  sumIfState(1, atime < (scan_time - INTERVAL 5 YEAR)) AS at_older_5y_count,
  sumIfState(size, atime < (scan_time - INTERVAL 7 YEAR)) AS at_older_7y_size,
  sumIfState(1, atime < (scan_time - INTERVAL 7 YEAR)) AS at_older_7y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 1 MONTH)) AS mt_older_1m_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 1 MONTH)) AS mt_older_1m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 2 MONTH)) AS mt_older_2m_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 2 MONTH)) AS mt_older_2m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 6 MONTH)) AS mt_older_6m_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 6 MONTH)) AS mt_older_6m_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 1 YEAR)) AS mt_older_1y_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 1 YEAR)) AS mt_older_1y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 2 YEAR)) AS mt_older_2y_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 2 YEAR)) AS mt_older_2y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 3 YEAR)) AS mt_older_3y_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 3 YEAR)) AS mt_older_3y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 5 YEAR)) AS mt_older_5y_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 5 YEAR)) AS mt_older_5y_count,
  sumIfState(size, mtime < (scan_time - INTERVAL 7 YEAR)) AS mt_older_7y_size,
  sumIfState(1, mtime < (scan_time - INTERVAL 7 YEAR)) AS mt_older_7y_count
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

func createSchema(ctx context.Context, conn clickhouse.Conn) error {
	stmts := []string{
		createScansTable,
		createFsEntriesTable,
		createRollupRawTable,
		createRollupStateTable,
		createRollupMV,
		createFilesCurrentView,
		createRollupsCurrentView,
	}
	for _, s := range stmts {
		if err := conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func updateClickhouse(mountPath string, r io.Reader) (retErr error) {
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

	scanID := uint64(time.Now().Unix())
	started := time.Now()

	// register scan as loading
	if err := conn.Exec(ctx, `INSERT INTO scans (mount_path, scan_id, state, started_at, finished_at) VALUES (?, ?, 'loading', ?, NULL)`, mountPath, scanID, started); err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}

	// rollback on failure: drop this scan's partitions and scan row
	defer func() {
		if retErr == nil {
			return
		}
		_ = conn.Exec(ctx, `ALTER TABLE fs_entries DROP PARTITION WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID)
		_ = conn.Exec(ctx, `ALTER TABLE ancestor_rollups_raw DROP PARTITION WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID)
		_ = conn.Exec(ctx, `ALTER TABLE ancestor_rollups_state DROP PARTITION WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID)
		_ = conn.Exec(ctx, `ALTER TABLE scans DELETE WHERE mount_path = ? AND scan_id = ?`, mountPath, scanID)
	}()

	if err := ingestScan(ctx, conn, mountPath, scanID, r); err != nil {
		return err
	}

	// promote to ready
	finished := time.Now()
	if err := conn.Exec(ctx, `ALTER TABLE scans UPDATE state = 'ready', finished_at = ? WHERE mount_path = ? AND scan_id = ?`, finished, mountPath, scanID); err != nil {
		return fmt.Errorf("promote scan: %w", err)
	}

	// drop older scans for this mount
	if err := dropOlderScans(ctx, conn, mountPath, scanID); err != nil {
		return fmt.Errorf("retention: %w", err)
	}

	return nil
}

func ingestScan(ctx context.Context, conn clickhouse.Conn, mountPath string, scanID uint64, r io.Reader) error {
	filesBatch, err := conn.PrepareBatch(ctx, `INSERT INTO fs_entries (mount_path, scan_id, path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime)`)
	if err != nil {
		return err
	}
	rollBatch, err := conn.PrepareBatch(ctx, `INSERT INTO ancestor_rollups_raw (mount_path, scan_id, ancestor, size, atime, mtime)`)
	if err != nil {
		return err
	}

	parser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)
	inFiles := 0
	inRolls := 0

	flush := func() error {
		if inFiles > 0 {
			if err := filesBatch.Send(); err != nil {
				return err
			}
			inFiles = 0
			filesBatch, err = conn.PrepareBatch(ctx, `INSERT INTO fs_entries (mount_path, scan_id, path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime)`)
			if err != nil {
				return err
			}
		}
		if inRolls > 0 {
			if err := rollBatch.Send(); err != nil {
				return err
			}
			inRolls = 0
			rollBatch, err = conn.PrepareBatch(ctx, `INSERT INTO ancestor_rollups_raw (mount_path, scan_id, ancestor, size, atime, mtime)`)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for parser.Scan(fi) == nil {
		path := string(fi.Path)
		isDir := fi.EntryType == stats.DirType || isDirPath(path)

		parent, name := splitParentAndName(path)
		ext := deriveExtLower(name, isDir)
		ft := mapEntryType(fi.EntryType)
		mtime := time.Unix(fi.MTime, 0)
		atime := time.Unix(fi.ATime, 0)
		ctime := time.Unix(fi.CTime, 0)

		if err := filesBatch.Append(
			mountPath, scanID, path, parent, name, ext, uint8(ft), uint64(fi.Inode), uint64(fi.Size), uint32(fi.UID), uint32(fi.GID), mtime, atime, ctime,
		); err != nil {
			return err
		}
		inFiles++

		// rollups for each ancestor directory (including mount root)
		dir := parent
		forEachAncestor(dir, mountPath, func(a string) bool {
			if err := rollBatch.Append(mountPath, scanID, a, uint64(fi.Size), atime, mtime); err != nil {
				return false
			}
			inRolls++
			return true
		})

		if inFiles >= fileBatchSize || inRolls >= rollupBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := parser.Err(); err != nil {
		return err
	}

	return flush()
}

func dropOlderScans(ctx context.Context, conn clickhouse.Conn, mountPath string, keepScanID uint64) error {
	// Drop data for older scan_ids for this mount
	if err := conn.Exec(ctx, `ALTER TABLE fs_entries DROP PARTITION WHERE mount_path = ? AND scan_id < ?`, mountPath, keepScanID); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `ALTER TABLE ancestor_rollups_raw DROP PARTITION WHERE mount_path = ? AND scan_id < ?`, mountPath, keepScanID); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `ALTER TABLE ancestor_rollups_state DROP PARTITION WHERE mount_path = ? AND scan_id < ?`, mountPath, keepScanID); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `ALTER TABLE scans DELETE WHERE mount_path = ? AND scan_id < ?`, mountPath, keepScanID); err != nil {
		return err
	}
	return nil
}

// --- Query helpers ---

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
	rows, err := conn.Query(ctx, `SELECT mount_path, toDateTime(max(scan_id)) FROM scans WHERE state = 'ready' GROUP BY mount_path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := make(map[string]time.Time)
	for rows.Next() {
		var m string
		var ts time.Time
		if err := rows.Scan(&m, &ts); err != nil {
			return nil, err
		}
		res[m] = ts
	}
	return res, rows.Err()
}

func ListImmediateChildren(ctx context.Context, conn clickhouse.Conn, mountPath, dir string) ([]FileEntry, error) {
	dir = ensureDir(dir)
	rows, err := conn.Query(ctx, `
        SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
        FROM fs_entries_current
        WHERE mount_path = ? AND parent_path = ?`, mountPath, dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FileEntry
	for rows.Next() {
		var e FileEntry
		if err := rows.Scan(&e.Path, &e.ParentPath, &e.Name, &e.Ext, &e.FType, &e.INode, &e.Size, &e.UID, &e.GID, &e.MTime, &e.ATime, &e.CTime); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func ensureDir(path string) string {
	if !strings.HasSuffix(path, "/") {
		return path + "/"
	}
	return path
}

func buildBucketPredicate(col, bucket string) (string, error) {
	switch bucket {
	case "":
		return "", nil
	case "0d":
		return fmt.Sprintf("%s >= (toDateTime(max_scan) - INTERVAL 1 DAY)", col), nil
	case ">1m":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 1 MONTH)", col), nil
	case ">2m":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 2 MONTH)", col), nil
	case ">6m":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 6 MONTH)", col), nil
	case ">1y":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 1 YEAR)", col), nil
	case ">2y":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 2 YEAR)", col), nil
	case ">3y":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 3 YEAR)", col), nil
	case ">5y":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 5 YEAR)", col), nil
	case ">7y":
		return fmt.Sprintf("%s < (toDateTime(max_scan) - INTERVAL 7 YEAR)", col), nil
	default:
		return "", fmt.Errorf("invalid bucket: %s", bucket)
	}
}

func SubtreeSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string, f Filters) (Summary, error) {
	dir = ensureDir(dir)

	where := []string{"mount_path = ?", "path LIKE ?"}
	args := []any{mountPath, dir + "%"}

	if len(f.GIDs) > 0 {
		ph := make([]string, len(f.GIDs))
		for i, v := range f.GIDs {
			ph[i] = "?"
			args = append(args, v)
		}
		where = append(where, fmt.Sprintf("gid IN (%s)", strings.Join(ph, ",")))
	}
	if len(f.UIDs) > 0 {
		ph := make([]string, len(f.UIDs))
		for i, v := range f.UIDs {
			ph[i] = "?"
			args = append(args, v)
		}
		where = append(where, fmt.Sprintf("uid IN (%s)", strings.Join(ph, ",")))
	}
	if len(f.Exts) > 0 {
		ph := make([]string, len(f.Exts))
		for i, v := range f.Exts {
			ph[i] = "?"
			args = append(args, strings.ToLower(v))
		}
		where = append(where, fmt.Sprintf("ext_low IN (%s)", strings.Join(ph, ",")))
	}

	// time buckets
	bucketFilter := ""
	if f.ATimeBucket != "" || f.MTimeBucket != "" {
		atPred, err := buildBucketPredicate("atime", f.ATimeBucket)
		if err != nil {
			return Summary{}, err
		}
		mtPred, err := buildBucketPredicate("mtime", f.MTimeBucket)
		if err != nil {
			return Summary{}, err
		}
		preds := []string{}
		if atPred != "" {
			preds = append(preds, atPred)
		}
		if mtPred != "" {
			preds = append(preds, mtPred)
		}
		if len(preds) > 0 {
			bucketFilter = " AND (" + strings.Join(preds, " AND ") + ")"
		}
	}

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
	argsWithScan := make([]any, 0, len(args)+1)
	argsWithScan = append(argsWithScan, mountPath)
	argsWithScan = append(argsWithScan, args...)
	row2 := conn.QueryRow(ctx, query, argsWithScan...)

	var s Summary
	if err := row2.Scan(&s.TotalSize, &s.FileCount, &s.MostRecentATime, &s.OldestATime, &s.MostRecentMTime, &s.OldestMTime, &s.UIDs, &s.GIDs, &s.Exts); err != nil {
		return Summary{}, err
	}
	return s, nil
}

// OptimizedSubtreeSummary attempts to use precomputed ancestor rollups when possible
// Falls back to the regular implementation for filtered queries
func OptimizedSubtreeSummary(ctx context.Context, conn clickhouse.Conn, mountPath, dir string, f Filters) (Summary, error) {
	// Only use rollups when there are no filters at all; ensures correctness for min/max times
	if len(f.GIDs) == 0 && len(f.UIDs) == 0 && len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == "" {
		dir = ensureDir(dir)

		query := `
		SELECT 
		  total_size,
		  file_count,
		  atime_max AS most_recent_atime,
		  atime_min AS oldest_atime,
		  mtime_max AS most_recent_mtime,
		  mtime_min AS oldest_mtime
		FROM ancestor_rollups_current
		WHERE mount_path = ? AND ancestor = ?`

		row := conn.QueryRow(ctx, query, mountPath, dir)

		var s Summary
		if err := row.Scan(&s.TotalSize, &s.FileCount, &s.MostRecentATime, &s.OldestATime, &s.MostRecentMTime, &s.OldestMTime); err != nil {
			if errors.Is(err, io.EOF) {
				return Summary{}, nil
			}
			return Summary{}, err
		}

		uniqueQuery := `
		SELECT 
		  groupUniqArray(uid) AS uids,
		  groupUniqArray(gid) AS gids,
		  groupUniqArray(ext_low) AS exts
		FROM fs_entries_current
		WHERE mount_path = ? AND path LIKE ?`

		row2 := conn.QueryRow(ctx, uniqueQuery, mountPath, dir+"%")
		if err := row2.Scan(&s.UIDs, &s.GIDs, &s.Exts); err != nil {
			return Summary{}, err
		}

		return s, nil
	}

	return SubtreeSummary(ctx, conn, mountPath, dir, f)
}

func SearchGlobPaths(ctx context.Context, conn clickhouse.Conn, mountPath, globPattern string, limit int, caseInsensitive bool) ([]string, error) {
	if limit <= 0 {
		limit = 0
	}
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	var q string
	if caseInsensitive {
		q = `SELECT path FROM fs_entries_current WHERE mount_path = ? AND lowerUTF8(path) LIKE lowerUTF8(?)`
	} else {
		q = `SELECT path FROM fs_entries_current WHERE mount_path = ? AND path LIKE ?`
	}

	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := conn.Query(ctx, q, mountPath, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, rows.Err()
}
