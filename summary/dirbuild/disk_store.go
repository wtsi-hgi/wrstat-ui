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

package dirbuild

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3" // register sqlite3 for disk-backed dirbuild scratch stores.
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const sqliteDiskSummaryCacheKiB = 64 * 1024

const sqliteDiskSummaryBaseColumnCount = 9

type diskHardlinkEntry struct {
	fileType db.DirGUTAFileType
	size     int64
	atime    int64
	mtime    int64
	gid      uint32
	uid      uint32
}

type diskSummaryStore interface {
	addFile(dirID uint32, keys []dirguta.GUTAKey, size int64, atime int64, mtime int64) error
	addHardlinkEntry(dirID uint32, entry *diskHardlinkEntry) error
	subtractHardlinkEntry(dirID uint32, entry *diskHardlinkEntry) error
	materialise(dirID uint32) (db.GUTAs, error)
	merge(parentID uint32, childID uint32) error
	clear(dirID uint32) error
	close() error
}

func newDiskBackedSummaryStore(opts Options, refTime int64) (diskSummaryStore, error) {
	dir, err := makeDiskSummaryTempDir(opts.TempDir)
	if err != nil {
		return nil, err
	}

	store, err := openSQLiteDiskSummaryStore(dir, refTime, !opts.RetainTempDir)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(dir))
	}

	return store, nil
}

type diskSummaryRow struct {
	gid          uint32
	uid          uint32
	ft           db.DirGUTAFileType
	mask         uint32
	count        int64
	size         int64
	atime        int64
	mtime        int64
	atimeBuckets summary.AgeBuckets
	mtimeBuckets summary.AgeBuckets
}

func scanDiskSummaryRow(rows *sql.Rows) (diskSummaryRow, error) {
	var row diskSummaryRow

	args := []any{
		&row.gid,
		&row.uid,
		&row.ft,
		&row.mask,
		&row.count,
		&row.size,
		&row.atime,
		&row.mtime,
	}

	args = appendBucketScanArgs(args, &row.atimeBuckets)
	args = appendBucketScanArgs(args, &row.mtimeBuckets)

	return row, rows.Scan(args...)
}

func (row diskSummaryRow) expandInto(expanded map[dirguta.GUTAKey]*summary.SummaryWithTimes) {
	for _, age := range db.DirGUTAges {
		if row.mask&(uint32(1)<<age) == 0 {
			continue
		}

		key := dirguta.GUTAKey{
			GID:      row.gid,
			UID:      row.uid,
			FileType: row.ft,
			Age:      age,
		}

		sum := expanded[key]
		if sum == nil {
			sum = new(summary.SummaryWithTimes)
			expanded[key] = sum
		}

		sum.AddSummary(row.summary())
	}
}

func (row diskSummaryRow) summary() *summary.SummaryWithTimes {
	return &summary.SummaryWithTimes{
		Summary: summary.Summary{
			Count: row.count,
			Size:  row.size,
		},
		Atime:        row.atime,
		Mtime:        row.mtime,
		AtimeBuckets: row.atimeBuckets,
		MtimeBuckets: row.mtimeBuckets,
	}
}

type sqliteDiskSummaryStore struct {
	dir              string
	db               *sql.DB
	tx               *sql.Tx
	upsert           *sql.Stmt
	subtract         *sql.Stmt
	selectBy         *sql.Stmt
	deleteBy         *sql.Stmt
	closed           bool
	refTime          int64
	removeDirOnClose bool
}

func openSQLiteDiskSummaryStore(dir string, refTime int64, removeDirOnClose bool) (*sqliteDiskSummaryStore, error) {
	handle, err := sql.Open("sqlite3", filepath.Join(dir, "summaries.sqlite"))
	if err != nil {
		return nil, err
	}

	store := &sqliteDiskSummaryStore{
		dir:              dir,
		db:               handle,
		refTime:          refTime,
		removeDirOnClose: removeDirOnClose,
	}
	if err := store.configure(); err != nil {
		return nil, errors.Join(err, handle.Close())
	}

	return store, nil
}

func (s *sqliteDiskSummaryStore) configure() error {
	ctx := context.Background()

	for _, pragma := range sqliteDiskSummaryPragmas() {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}

	if _, err := s.db.ExecContext(ctx, sqliteDiskSummarySchema()); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	s.tx = tx

	return s.prepareStatements()
}

func sqliteDiskSummaryPragmas() []string {
	return []string{
		"PRAGMA journal_mode=OFF",
		"PRAGMA synchronous=OFF",
		"PRAGMA temp_store=FILE",
		"PRAGMA mmap_size=0",
		"PRAGMA locking_mode=EXCLUSIVE",
		fmt.Sprintf("PRAGMA cache_size=-%d", sqliteDiskSummaryCacheKiB),
	}
}

func sqliteDiskSummarySchema() string {
	return "CREATE TABLE summaries (" +
		"dir_id INTEGER NOT NULL, gid INTEGER NOT NULL, uid INTEGER NOT NULL, " +
		"ft INTEGER NOT NULL, mask INTEGER NOT NULL, count INTEGER NOT NULL, " +
		"size INTEGER NOT NULL, atime INTEGER NOT NULL, mtime INTEGER NOT NULL, " +
		sqliteBucketColumns("ab") + ", " + sqliteBucketColumns("mb") + ", " +
		"PRIMARY KEY (dir_id, gid, uid, ft, mask)) WITHOUT ROWID"
}

func (s *sqliteDiskSummaryStore) prepareStatements() error {
	ctx := context.Background()

	var err error

	if s.upsert, err = s.tx.PrepareContext(ctx, sqliteDiskSummaryUpsertSQL()); err != nil {
		return err
	}

	if s.subtract, err = s.tx.PrepareContext(ctx, sqliteDiskSummarySubtractSQL()); err != nil {
		return err
	}

	if s.selectBy, err = s.tx.PrepareContext(ctx, sqliteDiskSummarySelectSQL()); err != nil {
		return err
	}

	if s.deleteBy, err = s.tx.PrepareContext(ctx, "DELETE FROM summaries WHERE dir_id = ?"); err != nil {
		return err
	}

	return nil
}

func sqliteDiskSummaryUpsertSQL() string {
	return "INSERT INTO summaries (" + sqliteDiskSummaryColumnList() + ") VALUES (" +
		sqlitePlaceholders(sqliteDiskSummaryColumnCount()) + ") " +
		"ON CONFLICT(dir_id, gid, uid, ft, mask) DO UPDATE SET " +
		"count = count + excluded.count, size = size + excluded.size, " +
		"atime = CASE WHEN atime = 0 THEN excluded.atime " +
		"WHEN excluded.atime = 0 THEN atime " +
		"WHEN excluded.atime < atime THEN excluded.atime ELSE atime END, " +
		"mtime = CASE WHEN excluded.mtime > mtime THEN excluded.mtime ELSE mtime END, " +
		sqliteBucketAddAssignments("ab") + ", " + sqliteBucketAddAssignments("mb")
}

func sqliteDiskSummarySubtractSQL() string {
	return "UPDATE summaries SET count = count - ?, size = size - ? " +
		"WHERE dir_id = ? AND gid = ? AND uid = ? AND ft = ? AND mask = ?"
}

func sqliteDiskSummarySelectSQL() string {
	return "SELECT gid, uid, ft, mask, count, size, atime, mtime, " +
		sqliteBucketNames("ab") + ", " + sqliteBucketNames("mb") +
		" FROM summaries WHERE dir_id = ?"
}

func (s *sqliteDiskSummaryStore) addFile(
	dirID uint32,
	keys []dirguta.GUTAKey,
	size int64,
	atime int64,
	mtime int64,
) error {
	tuple, mask, ok := diskSummaryMaskForKeys(keys, atime, mtime, s.refTime)
	if !ok || mask == 0 {
		return nil
	}

	sum := summary.SummaryWithTimes{}
	sum.Add(size, atime, mtime, s.refTime)

	return s.upsertSummary(dirID, tuple, mask, &sum)
}

func diskSummaryMaskForKeys(
	keys []dirguta.GUTAKey,
	atime int64,
	mtime int64,
	refTime int64,
) (dirguta.GUTAKey, uint32, bool) {
	if len(keys) == 0 {
		return dirguta.GUTAKey{}, 0, false
	}

	tuple := keys[0]
	tuple.Age = db.DGUTAgeAll
	mask := uint32(0)

	for _, key := range keys {
		if key.GID != tuple.GID || key.UID != tuple.UID || key.FileType != tuple.FileType {
			return dirguta.GUTAKey{}, 0, false
		}

		if key.Age.FitsAgeInterval(atime, mtime, refTime) {
			mask |= uint32(1) << key.Age
		}
	}

	return tuple, mask, true
}

func (s *sqliteDiskSummaryStore) addHardlinkEntry(dirID uint32, entry *diskHardlinkEntry) error {
	keys := dirguta.GUTAKeysFromEntry(entry.gid, entry.uid, entry.fileType)

	return s.addFile(dirID, keys, entry.size, entry.atime, entry.mtime)
}

func (s *sqliteDiskSummaryStore) subtractHardlinkEntry(dirID uint32, entry *diskHardlinkEntry) error {
	keys := dirguta.GUTAKeysFromEntry(entry.gid, entry.uid, entry.fileType)

	_, mask, ok := diskSummaryMaskForKeys(keys, entry.atime, entry.mtime, s.refTime)
	if !ok || mask == 0 {
		return nil
	}

	_, err := s.subtract.ExecContext(
		context.Background(),
		int64(1),
		entry.size,
		dirID,
		entry.gid,
		entry.uid,
		uint16(entry.fileType),
		mask,
	)

	return err
}

func (s *sqliteDiskSummaryStore) upsertSummary(
	dirID uint32,
	key dirguta.GUTAKey,
	mask uint32,
	sum *summary.SummaryWithTimes,
) error {
	_, err := s.upsert.ExecContext(context.Background(), sqliteDiskSummaryArgs(dirID, key, mask, sum)...)

	return err
}

func sqliteDiskSummaryArgs(
	dirID uint32,
	key dirguta.GUTAKey,
	mask uint32,
	sum *summary.SummaryWithTimes,
) []any {
	args := []any{
		dirID,
		key.GID,
		key.UID,
		uint16(key.FileType),
		mask,
		sum.Count,
		sum.Size,
		sum.Atime,
		sum.Mtime,
	}

	args = appendBucketArgs(args, sum.AtimeBuckets)
	args = appendBucketArgs(args, sum.MtimeBuckets)

	return args
}

func (s *sqliteDiskSummaryStore) materialise(dirID uint32) (db.GUTAs, error) {
	rows, err := s.summaryRows(dirID)
	if err != nil {
		return nil, err
	}

	expanded := make(map[dirguta.GUTAKey]*summary.SummaryWithTimes)
	for _, row := range rows {
		row.expandInto(expanded)
	}

	keys := make(dirguta.GUTAKeys, 0, len(expanded))
	for key := range expanded {
		keys = append(keys, key)
	}

	sort.Sort(keys)

	return diskMaterializedGUTAs(keys, expanded), nil
}

func diskMaterializedGUTAs(
	keys dirguta.GUTAKeys,
	summaries map[dirguta.GUTAKey]*summary.SummaryWithTimes,
) db.GUTAs {
	if len(keys) == 0 {
		return nil
	}

	values := make([]db.GUTA, len(keys))
	gutas := make(db.GUTAs, len(keys))

	for idx, key := range keys {
		sum := summaries[key]
		values[idx] = db.GUTA{
			GID:         key.GID,
			UID:         key.UID,
			FT:          key.FileType,
			Age:         key.Age,
			Count:       uint64(sum.Count), //nolint:gosec
			Size:        uint64(sum.Size),  //nolint:gosec
			Atime:       sum.Atime,
			ATimeRanges: sum.AtimeBuckets,
			Mtime:       sum.Mtime,
			MTimeRanges: sum.MtimeBuckets,
		}
		gutas[idx] = &values[idx]
	}

	return gutas
}

func (s *sqliteDiskSummaryStore) summaryRows(dirID uint32) ([]diskSummaryRow, error) {
	rows, err := s.selectBy.QueryContext(context.Background(), dirID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []diskSummaryRow

	for rows.Next() {
		row, err := scanDiskSummaryRow(rows)
		if err != nil {
			return nil, err
		}

		summaries = append(summaries, row)
	}

	return summaries, rows.Err()
}

func (s *sqliteDiskSummaryStore) merge(parentID uint32, childID uint32) error {
	rows, err := s.summaryRows(childID)
	if err != nil {
		return err
	}

	for _, row := range rows {
		key := dirguta.GUTAKey{
			GID:      row.gid,
			UID:      row.uid,
			FileType: row.ft,
		}
		if err := s.upsertSummary(parentID, key, row.mask, row.summary()); err != nil {
			return err
		}
	}

	return s.clear(childID)
}

func (s *sqliteDiskSummaryStore) clear(dirID uint32) error {
	_, err := s.deleteBy.ExecContext(context.Background(), dirID)

	return err
}

func (s *sqliteDiskSummaryStore) close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	stmtErr := errors.Join(closeStmt(s.upsert), closeStmt(s.subtract), closeStmt(s.selectBy), closeStmt(s.deleteBy))
	commitErr := s.tx.Commit()
	closeErr := s.db.Close()

	if !s.removeDirOnClose {
		return errors.Join(stmtErr, commitErr, closeErr)
	}

	return errors.Join(stmtErr, commitErr, closeErr, os.RemoveAll(s.dir))
}

func closeStmt(stmt *sql.Stmt) error {
	if stmt == nil {
		return nil
	}

	return stmt.Close()
}

func makeDiskSummaryTempDir(parent string) (string, error) {
	if parent == "" {
		return os.MkdirTemp("", "wrstat-dirbuild-summary-*")
	}

	return os.MkdirTemp(parent, "wrstat-dirbuild-summary-*")
}

func sqliteBucketColumns(prefix string) string {
	cols := make([]string, 0, len(summary.AgeBuckets{}))
	for idx := range (summary.AgeBuckets{}) {
		cols = append(cols, fmt.Sprintf("%s%d INTEGER NOT NULL", prefix, idx))
	}

	return strings.Join(cols, ", ")
}

func sqliteDiskSummaryColumnList() string {
	return "dir_id, gid, uid, ft, mask, count, size, atime, mtime, " +
		sqliteBucketNames("ab") + ", " + sqliteBucketNames("mb")
}

func sqliteBucketNames(prefix string) string {
	names := make([]string, 0, len(summary.AgeBuckets{}))
	for idx := range (summary.AgeBuckets{}) {
		names = append(names, fmt.Sprintf("%s%d", prefix, idx))
	}

	return strings.Join(names, ", ")
}

func sqliteDiskSummaryColumnCount() int {
	return sqliteDiskSummaryBaseColumnCount + len(summary.AgeBuckets{})*2
}

func sqlitePlaceholders(count int) string {
	values := make([]string, count)
	for idx := range values {
		values[idx] = "?"
	}

	return strings.Join(values, ", ")
}

func sqliteBucketAddAssignments(prefix string) string {
	assignments := make([]string, 0, len(summary.AgeBuckets{}))

	for idx := range (summary.AgeBuckets{}) {
		name := fmt.Sprintf("%s%d", prefix, idx)
		assignments = append(assignments, fmt.Sprintf("%s = %s + excluded.%s", name, name, name))
	}

	return strings.Join(assignments, ", ")
}

func appendBucketArgs(args []any, buckets summary.AgeBuckets) []any {
	for _, value := range buckets {
		args = append(args, int64(value)) //nolint:gosec // summary counters are already bounded by int64 row counts.
	}

	return args
}

func appendBucketScanArgs(args []any, buckets *summary.AgeBuckets) []any {
	for idx := range buckets {
		args = append(args, &buckets[idx])
	}

	return args
}
