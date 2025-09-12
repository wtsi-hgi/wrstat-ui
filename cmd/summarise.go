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
	"log"
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

func openStatsFile(statsFile string) (io.Reader, time.Time, error) {
	if statsFile == "-" {
		return os.Stdin, time.Now(), nil
	}

	f, err := os.Open(statsFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to open stats file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}

	var r io.Reader = f

	if strings.HasSuffix(statsFile, ".gz") {
		if r, err = pgzip.NewReader(f); err != nil {
			return nil, time.Time{}, fmt.Errorf("failed to decompress stats file: %w", err)
		}
	}

	return r, fi.ModTime(), nil
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
	})
	if err != nil {
		return fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	if err := createSchema(ctx, conn); err != nil {
		log.Fatalf("createSchema: %v", err)
	}

	// Simulate a daily scanner output (small example). In production, your
	// scanner would stream batches of FileRecord to ingestStaging.
	now := time.Now()
	scanTS := uint32(now.Unix())
	scanMonth := uint16(now.Year()*100 + int(now.Month()))

	// 1) Insert today's scan into staging
	ags, err := ingestStaging(ctx, conn, r, scanTS, scanMonth)
	if err != nil {
		log.Fatalf("ingestStaging: %v", err)
	}

	// 2) Generate tombstones (set-based in ClickHouse) and negative ancestor
	// aggregates
	if err := generateTombstonesAndNegativeAggs(ctx, conn, scanTS, scanMonth); err != nil {
		log.Fatalf("generate tombstones: %v", err)
	}

	// 3) Append today's staging rows into append-only files table
	if err := appendStagingToFiles(ctx, conn); err != nil {
		log.Fatalf("appendStagingToFiles: %v", err)
	}

	// 4) Insert scanner-side positive ancestor aggregates (batch)
	if err := ingestAncestorAggs(ctx, conn, ags); err != nil {
		log.Fatalf("ingestAncestorAggs: %v", err)
	}

	// 5) Truncate staging to free space
	if err := truncateStaging(ctx, conn); err != nil {
		log.Fatalf("truncateStaging: %v", err)
	}

	return nil
}

func createSchema(ctx context.Context, conn clickhouse.Conn) error {
	// staging table (today)
	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS files_today (
  path String,
  dirname String,
  basename String,
  size UInt64,
  uid UInt32,
  gid UInt32,
  mtime DateTime,
  atime DateTime,
  ctime DateTime,
  scan_ts UInt32,
  scan_month UInt16
) ENGINE = MergeTree()
ORDER BY path
`); err != nil {
		return err
	}

	// append-only files table (history + tombstones)
	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS files (
  path String,
  dirname String,
  basename String,
  size UInt64,
  uid UInt32,
  gid UInt32,
  mtime DateTime,
  atime DateTime,
  ctime DateTime,
  scan_ts UInt32,
  scan_month UInt16,
  is_deleted UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY scan_month
ORDER BY (path, scan_ts)
`); err != nil {
		return err
	}

	// compact "current" view of files (latest per path) via ReplacingMergeTree
	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS files_current (
  path String,
  dirname String,
  basename String,
  size UInt64,
  uid UInt32,
  gid UInt32,
  mtime DateTime,
  atime DateTime,
  ctime DateTime,
  last_seen UInt32,
  is_deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(last_seen)
ORDER BY path
`); err != nil {
		return err
	}

	// materialized view to keep files_current updated from append-only files
	// It groups by path/dirname/basename and computes latest values
	if err := conn.Exec(ctx, `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_files_current TO files_current AS
SELECT
  path,
  dirname,
  basename,
  anyLast(size) AS size,
  anyLast(uid) AS uid,
  anyLast(gid) AS gid,
  anyLast(mtime) AS mtime,
  anyLast(atime) AS atime,
  anyLast(ctime) AS ctime,
  max(scan_ts) AS last_seen,
  anyLast(is_deleted) AS is_deleted
FROM files
GROUP BY path, dirname, basename
`); err != nil {
		return err
	}

	// per-ancestor aggregates (SummingMergeTree accepts positive/negative rows)
	if err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS file_aggregates (
  ancestor String,
  total_size Int64,   -- can be negative for tombstone adjustments
  total_count Int64,
  scan_ts UInt32,
  scan_month UInt16,
  is_deleted UInt8 DEFAULT 0
) ENGINE = SummingMergeTree
PARTITION BY scan_month
ORDER BY (ancestor, scan_ts)
`); err != nil {
		return err
	}

	return nil
}

// ingestStaging inserts scanner results into files_today in batches.
func ingestStaging(ctx context.Context, conn clickhouse.Conn, r io.Reader, scanTS uint32, scanMonth uint16) ([]AncestorAgg, error) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO files_today (path, dirname, basename, size, uid, gid, mtime, atime, ctime, scan_ts, scan_month)")
	if err != nil {
		return nil, err
	}

	statsParser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)
	agg := map[string]*AncestorAgg{}

	for statsParser.Scan(fi) == nil {
		path := string(fi.Path)
		dir := filepath.Dir(path)
		name := filepath.Base(path)

		if err := batch.Append(
			path, dir, name, fi.Size, fi.UID, fi.GID,
			fi.MTime, fi.ATime, fi.CTime, scanTS, scanMonth,
		); err != nil {
			return nil, err
		}

		for _, anc := range dirToAncestors(path, dir) {
			if _, ok := agg[anc]; !ok {
				agg[anc] = &AncestorAgg{
					Ancestor:  anc,
					ScanTS:    scanTS,
					ScanMonth: scanMonth,
					IsDeleted: 0,
				}
			}
			agg[anc].TotalSize += int64(fi.Size)
			agg[anc].TotalCount += 1
		}
	}

	if err := statsParser.Err(); err != nil {
		return nil, err
	}

	err = batch.Send()
	if err != nil {
		return nil, err
	}

	aggs := make([]AncestorAgg, 0, len(agg))
	for _, v := range agg {
		aggs = append(aggs, *v)
	}

	return aggs, nil
}

func dirToAncestors(path, dir string) []string {
	ancestors := []string{}
	if path != "/" {
		parts := strings.Split(dir, string(os.PathSeparator))
		for i := range parts {
			if i == 0 && parts[i] == "" {
				ancestors = append(ancestors, "/")

				continue
			}

			if parts[i] == "" {
				continue
			}

			anc := strings.Join(parts[0:i+1], string(os.PathSeparator))
			if anc == "" {
				anc = "/"
			}

			ancestors = append(ancestors, anc)
		}
	} else {
		ancestors = append(ancestors, "/")
	}

	return ancestors
}

// generateTombstonesAndNegativeAggs:
//   - Inserts tombstones rows into `files` for paths present in files_current but not in files_today.
//   - Inserts negative per-ancestor aggregate rows into file_aggregates by using arrayJoin(splitByChar('/', path))
//
// This runs entirely in ClickHouse and requires no large memory in Go.
func generateTombstonesAndNegativeAggs(ctx context.Context, conn clickhouse.Conn, scanTS uint32, scanMonth uint16) error {
	// 1) Insert tombstone rows into files table for missing paths
	// (is_deleted=1). Use files_current (the latest-per-path view) to find live
	// paths; left join with files_today.
	tombstoneSQL := fmt.Sprintf(`
INSERT INTO files (path, dirname, basename, size, uid, gid, mtime, atime, ctime, scan_ts, scan_month, is_deleted)
SELECT f.path, f.dirname, f.basename, 0 AS size, 0 AS uid, 0 AS gid, now() AS mtime, now() AS atime, now() AS ctime, %d AS scan_ts, %d AS scan_month, 1 AS is_deleted
FROM files_current f
LEFT JOIN files_today t ON f.path = t.path
WHERE f.is_deleted = 0 AND t.path IS NULL
`, scanTS, scanMonth)

	if err := conn.Exec(ctx, tombstoneSQL); err != nil {
		return fmt.Errorf("tombstone insert failed: %w", err)
	}

	// 2) Insert negative ancestor aggregates for those deleted paths.
	// We use arrayJoin(splitByChar('/', path)) to expand ancestors server-side.
	negAggSQL := fmt.Sprintf(`
INSERT INTO file_aggregates (ancestor, total_size, total_count, scan_ts, scan_month, is_deleted)
SELECT ancestor, -sum(size) AS total_size, -count() AS total_count, %d AS scan_ts, %d AS scan_month, 1 AS is_deleted
FROM (
  SELECT arrayJoin(splitByChar('/', path)) AS ancestor, size
  FROM files_current f
  LEFT JOIN files_today t ON f.path = t.path
  WHERE f.is_deleted = 0 AND t.path IS NULL
)
GROUP BY ancestor
`, scanTS, scanMonth)

	if err := conn.Exec(ctx, negAggSQL); err != nil {
		return fmt.Errorf("negative agg insert failed: %w", err)
	}

	return nil
}

// appendStagingToFiles appends all rows from files_today into files (history
// table).
func appendStagingToFiles(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `
INSERT INTO files (path, dirname, basename, size, uid, gid, mtime, atime, ctime, scan_ts, scan_month, is_deleted)
SELECT path, dirname, basename, size, uid, gid, mtime, atime, ctime, scan_ts, scan_month, 0 AS is_deleted FROM files_today
`)
}

// ingestAncestorAggs inserts scanner-computed positive aggregates into
// file_aggregates.
func ingestAncestorAggs(ctx context.Context, conn clickhouse.Conn, aggs []AncestorAgg) error {
	if len(aggs) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO file_aggregates (ancestor, total_size, total_count, scan_ts, scan_month, is_deleted)")
	if err != nil {
		return err
	}

	for _, a := range aggs {
		if err := batch.Append(a.Ancestor, a.TotalSize, a.TotalCount, a.ScanTS, a.ScanMonth, a.IsDeleted); err != nil {
			return err
		}
	}

	return batch.Send()
}

// truncateStaging clears files_today (so next scan starts fresh).
func truncateStaging(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, "TRUNCATE TABLE files_today")
}
