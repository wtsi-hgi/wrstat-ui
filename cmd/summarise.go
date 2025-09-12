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
	"path/filepath"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	defaultDir        string
	userGroup         string
	groupUser         string
	basedirsDB        string
	basedirsHistoryDB string
	dirgutaDB         string

	quotaPath      string
	basedirsConfig string
	mounts         string
)

const dbBatchSize = 10000

// summariseCmd represents the stat command.
var summariseCmd = &cobra.Command{
	Use:   "summarise",
	Short: "Summarise stat data",
	Long: `Summarise stat data in to dirguta database, basedirs database, ` +
		`and usergroup/groupuser files.

Summarise processes stat files from the output of 'wrstat multi' into different
summaries.

Summarise takes the following arguments

  --defaultDir,-d
	output all summarisers to here with the default names.

  --userGroup,-u
	usergroup output file. Defaults to DEFAULTDIR/byusergroup.gz, if --defaultDir is set.
	If filename ends in '.gz' the file will be gzip compressed.

  --groupUser,-g
	groupUser output file. Defaults to DEFAULTDIR/bygroup, if --defaultDir is set.
	If filename ends in '.gz' the file will be gzip compressed.

  --basedirsDB,-b
	basedirs output file. Defaults to DEFAULTDIR/basedirs.db, if --defaultDir is set.

  --tree,-t
	tree output dir. Defaults to DEFAULTDIR/dguta.dbs, if --defaultDir is set.

  --basedirsHistoryDB,-s
	basedirs file containing previous history.

  --quota,-q
	Required for basedirs, format is a csv of gid,disk,size_quota,inode_quota

  --config,-c
	Required for basedirs, path to basedirs config file.

  --mounts,-m
	Provide a file containing quoted mount points, one-per-line, instead of
	relying on automatically discovered mount points.
	The following is an example command that can be used to generate an
	appropriate file:
		findmnt -ln --real -o target | sed -e 's/^/"/' -e 's/$/"/' > mounts

NB: All existing output files will be deleted or truncated during initialisation.

An example command would be the following:

	wrstat-ui summarise -d /path/to/output -s /path/to/previous/basedirs.db -q ` +
		`/path/to/quota.file -c /path/to/basedirs.config /path/to/stats.file
`,
	Run: func(_ *cobra.Command, args []string) {
		if err := run(args); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(summariseCmd)

	summariseCmd.Flags().StringVarP(&defaultDir, "defaultDir", "d", "", "output all summarisers to here")
	summariseCmd.Flags().StringVarP(&userGroup, "userGroup", "u", "", "usergroup output file")
	summariseCmd.Flags().StringVarP(&groupUser, "groupUser", "g", "", "groupUser output file")
	summariseCmd.Flags().StringVarP(&basedirsDB, "basedirsDB", "b", "", "basedirs output file")
	summariseCmd.Flags().StringVarP(&basedirsHistoryDB, "basedirsHistoryDB", "s", "",
		"basedirs file containing previous history")
	summariseCmd.Flags().StringVarP(&dirgutaDB, "tree", "t", "", "tree output dir")
	summariseCmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	summariseCmd.Flags().StringVarP(&basedirsConfig, "config", "c", "", "path to basedirs config file")
	summariseCmd.Flags().StringVarP(&mounts, "mounts", "m", "", "path to a file containing a list of quoted mountpoints")
}

func run(args []string) (err error) {
	if err = checkArgs(args); err != nil {
		return err
	}

	r, _, err := openStatsFile(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	err = updateClickhouse(r)
	if err != nil {
		return err
	}

	return nil
}

func checkArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("exactly 1 input file should be provided") //nolint:err113
	}

	if defaultDir == "" && userGroup == "" && groupUser == "" && basedirsDB == "" && dirgutaDB == "" {
		return errors.New("no output files specified") //nolint:err113
	}

	return nil
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

// Ancestor aggregate record (scanner-produced, positive values for present
// files).
type AncestorAgg struct {
	Ancestor   string
	TotalSize  int64
	TotalCount int64
	ScanTS     uint32
	ScanMonth  uint16
	IsDeleted  uint8 // 0 normally
}

func updateClickhouse(r io.Reader) error {
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	if err := createSchema(ctx, conn); err != nil {
		return fmt.Errorf("createSchema: %w", err)
	}

	now := time.Now()
	scanID := uint32(now.Unix())

	if err := ingestStats(ctx, conn, r, scanID); err != nil {
		return fmt.Errorf("ingestStats: %w", err)
	}

	// monthly run:
	if false {
		if err := rebuildAndSwap(ctx, conn); err != nil {
			return fmt.Errorf("rebuildAndSwap: %w", err)
		}
	}

	return nil
}

func createSchema(ctx context.Context, conn clickhouse.Conn) error {
	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS files_active (
    path String,
    size UInt64,
    uid UInt32,
    gid UInt32,
    mtime DateTime,
    atime DateTime,
    ctime DateTime,
    is_deleted UInt8,
    scan_id UInt64
) ENGINE = MergeTree
PARTITION BY scan_id
ORDER BY path
`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS ancestor_aggs (
    ancestor String,
    total_size UInt64,
    file_count UInt64,
    scan_id UInt64
) ENGINE = SummingMergeTree
PARTITION BY scan_id
ORDER BY (ancestor, scan_id)
`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
CREATE OR REPLACE VIEW files_current AS
WITH (SELECT max(scan_id) FROM files_active) AS max_scan
SELECT
    path,
    size,
    uid,
    gid,
    mtime,
    atime,
    ctime,
    is_deleted
FROM files_active
WHERE scan_id = max_scan`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
CREATE OR REPLACE VIEW ancestor_aggs_current AS
WITH (SELECT max(scan_id) FROM ancestor_aggs) AS max_scan
SELECT
    ancestor,
    sum(total_size) AS total_size,
    sum(file_count) AS file_count
FROM ancestor_aggs
WHERE scan_id = max_scan
GROUP BY ancestor`); err != nil {
		return err
	}

	return nil
}

const (
	sqlBatchSize          = 100000
	sqlInsertFilesActive  = `INSERT INTO files_active(path, size, uid, gid, mtime, atime, ctime, is_deleted, scan_id)`
	sqlInsertAncestorAggs = `INSERT INTO ancestor_aggs(ancestor, total_size, file_count, scan_id)`
)

// ingestStats inserts scanner results into files_active and ancestor_aggs.
// It pre-aggregates ancestor rows per batch to reduce insert volume.
func ingestStats(ctx context.Context, conn clickhouse.Conn, r io.Reader, scanID uint32) error {
	faBatch, err := conn.PrepareBatch(ctx, sqlInsertFilesActive)
	if err != nil {
		return err
	}
	aaBatch, err := conn.PrepareBatch(ctx, sqlInsertAncestorAggs)
	if err != nil {
		return err
	}

	type agg struct {
		size  uint64
		count uint64
	}
	ancestorAgg := make(map[string]agg, 1<<15) // pre-size for fewer allocs

	statsParser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)
	filesInBatch := 0

	flushFiles := func() error {
		if filesInBatch == 0 {
			return nil
		}
		if err := faBatch.Send(); err != nil {
			return err
		}
		filesInBatch = 0
		var err error
		faBatch, err = conn.PrepareBatch(ctx, sqlInsertFilesActive)
		return err
	}
	flushAncestorAgg := func() error {
		if len(ancestorAgg) == 0 {
			return nil
		}
		for dir, a := range ancestorAgg {
			if err := aaBatch.Append(
				dir, a.size, a.count, uint64(scanID),
			); err != nil {
				return err
			}
		}
		ancestorAgg = make(map[string]agg, 1<<15)

		if err := aaBatch.Send(); err != nil {
			return err
		}
		var err error
		aaBatch, err = conn.PrepareBatch(ctx, sqlInsertAncestorAggs)
		return err
	}

	const ancestorAggFlushSize = 50000

	for statsParser.Scan(fi) == nil {
		path := string(fi.Path)

		// Convert types for ClickHouse
		size := uint64(fi.Size)
		uid := uint32(fi.UID)
		gid := uint32(fi.GID)
		mtime := time.Unix(fi.MTime, 0)
		atime := time.Unix(fi.ATime, 0)
		ctime := time.Unix(fi.CTime, 0)
		isDeleted := uint8(0)

		if err := faBatch.Append(
			path, size, uid, gid, mtime, atime, ctime, isDeleted, uint64(scanID),
		); err != nil {
			return err
		}
		filesInBatch++

		// Pre-aggregate ancestor stats for this file
		dir := filepath.Dir(path)
		for {
			a := ancestorAgg[dir]
			a.size += size
			a.count++
			ancestorAgg[dir] = a

			if dir == "/" || dir == "." {
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}

		// Flush independently to keep memory bounded
		if filesInBatch >= sqlBatchSize {
			if err := flushFiles(); err != nil {
				return err
			}
		}
		if len(ancestorAgg) >= ancestorAggFlushSize {
			if err := flushAncestorAgg(); err != nil {
				return err
			}
		}
	}

	if err := statsParser.Err(); err != nil {
		return err
	}

	// Final flush
	if err := flushFiles(); err != nil {
		return err
	}
	if err := flushAncestorAgg(); err != nil {
		return err
	}

	return nil
}

func rebuildAndSwap(ctx context.Context, conn clickhouse.Conn) error {
	// Drop any stale leftovers from a failed/partial run
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS files_compacted`); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS ancestor_aggs_compacted`); err != nil {
		return err
	}

	// Create compacted tables mirroring engines and layouts to avoid drift
	if err := conn.Exec(ctx, `
        CREATE TABLE files_compacted
        AS files_active
        ENGINE = MergeTree
        PARTITION BY scan_id
        ORDER BY path`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
        CREATE TABLE ancestor_aggs_compacted
        AS ancestor_aggs
        ENGINE = SummingMergeTree
        PARTITION BY scan_id
        ORDER BY (ancestor, scan_id)`); err != nil {
		return err
	}

	// Populate compacted tables with the latest snapshot only
	if err := conn.Exec(ctx, `
        INSERT INTO files_compacted
        SELECT
            path,
            size,
            uid,
            gid,
            mtime,
            atime,
            ctime,
            is_deleted,
            scan_id
        FROM files_active
        WHERE scan_id = (SELECT max(scan_id) FROM files_active)`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
        INSERT INTO ancestor_aggs_compacted
        WITH (SELECT max(scan_id) FROM ancestor_aggs) AS max_scan
        SELECT
            ancestor,
            sum(total_size) AS total_size,
            sum(file_count) AS file_count,
            max_scan AS scan_id
        FROM ancestor_aggs
        WHERE scan_id = max_scan
        GROUP BY ancestor`); err != nil {
		return err
	}

	// Swap tables atomically
	if err := conn.Exec(ctx, `
        RENAME TABLE
            files_active TO files_old,
            files_compacted TO files_active`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
        RENAME TABLE
            ancestor_aggs TO ancestor_aggs_old,
            ancestor_aggs_compacted TO ancestor_aggs`); err != nil {
		return err
	}

	// Drop old tables
	_ = conn.Exec(ctx, `DROP TABLE IF EXISTS files_old`)
	_ = conn.Exec(ctx, `DROP TABLE IF EXISTS ancestor_aggs_old`)

	return nil
}
