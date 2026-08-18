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
	"slices"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // register sqlite3 for disk-backed dirbuild scratch stores.
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const sqliteDiskSummaryCacheKiB = 64 * 1024

const sqliteDiskSummaryBaseColumnCount = 9

const sqliteDiskSummaryWriteBehindBytes = 4 * 1024 * 1024

const sqliteDiskSummaryAccumulatorEntryBytes = 256

const sqliteDiskSummaryFileName = "summaries.sqlite"

// DiskMetrics reports work performed by the disk-backed summary path.
// ProcessWriteBytes and its phase fields are Linux process write_bytes deltas
// observed while each phase runs; DatabaseBytes is the resulting SQLite file.
type DiskMetrics struct {
	WriteBehindLimitBytes            uint64
	MaxWriteBehindBytes              uint64
	RowsReceived                     uint64
	RowsWritten                      uint64
	RowsCombined                     uint64
	MaxRowsCombinedPerFlush          uint64
	Flushes                          uint64
	SQLiteStatements                 uint64
	SelectStatements                 uint64
	DatabaseBytes                    uint64
	ProcessWriteBytes                uint64
	Pass2ProcessWriteBytes           uint64
	RollupProcessWriteBytes          uint64
	ProcessWriteBytesAvailable       bool
	Pass2ProcessWriteBytesAvailable  bool
	RollupProcessWriteBytesAvailable bool
	ProcessWriteBytesSource          string
	Pass2Elapsed                     time.Duration
	RollupElapsed                    time.Duration
	readProcessWriteBytes            func() (uint64, error)
	readDatabaseBytes                func() uint64
}

type diskSummaryStore interface {
	addFile(dirID uint32, keys []dirguta.GUTAKey, size int64, atime int64, mtime int64) error
	rollUp(dirID uint32, parentID *uint32, hardlinks db.GUTAs, emit func(db.GUTAs) error) error
	flush() error
	close(success bool) error
}

func newDiskBackedSummaryStore(opts Options, refTime int64) (diskSummaryStore, error) {
	dir, err := makeDiskSummaryTempDir(opts.TempDir)
	if err != nil {
		return nil, err
	}

	store, err := openSQLiteDiskSummaryStore(dir, refTime, !opts.RetainTempDir)
	if err != nil {
		return nil, err
	}

	if opts.DiskMetrics != nil {
		store.metrics = opts.DiskMetrics
		store.resetMetricLimit()
	}

	return store, nil
}

type diskSummaryKey struct {
	dirID uint32
	gid   uint32
	uid   uint32
	ft    db.DirGUTAFileType
	mask  uint32
}

func compareDiskSummaryKeys(a, b diskSummaryKey) int {
	if a.dirID != b.dirID {
		return cmpUint32(a.dirID, b.dirID)
	}

	if a.gid != b.gid {
		return cmpUint32(a.gid, b.gid)
	}

	if a.uid != b.uid {
		return cmpUint32(a.uid, b.uid)
	}

	if a.ft != b.ft {
		return cmpUint32(uint32(a.ft), uint32(b.ft))
	}

	return cmpUint32(a.mask, b.mask)
}

func cmpUint32(a, b uint32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
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
	selectBy         *sql.Stmt
	deleteBy         *sql.Stmt
	closed           bool
	refTime          int64
	removeDirOnClose bool
	pending          map[diskSummaryKey]*summary.SummaryWithTimes
	pendingBytes     uint64
	pendingRows      uint64
	writeBehindLimit uint64
	metrics          *DiskMetrics
	observeFlush     func([]diskSummaryKey)
}

func openSQLiteDiskSummaryStore(dir string, refTime int64, removeDirOnClose bool) (*sqliteDiskSummaryStore, error) {
	handle, err := sql.Open("sqlite3", filepath.Join(dir, sqliteDiskSummaryFileName))
	if err != nil {
		return nil, err
	}

	store := &sqliteDiskSummaryStore{
		dir:              dir,
		db:               handle,
		refTime:          refTime,
		removeDirOnClose: removeDirOnClose,
		pending:          make(map[diskSummaryKey]*summary.SummaryWithTimes),
		writeBehindLimit: sqliteDiskSummaryWriteBehindBytes,
		metrics:          new(DiskMetrics),
	}
	store.resetMetricLimit()

	if err := store.configure(); err != nil {
		return nil, errors.Join(err, handle.Close())
	}

	return store, nil
}

func (s *sqliteDiskSummaryStore) resetMetricLimit() {
	s.metrics.WriteBehindLimitBytes = s.writeBehindLimit
	s.metrics.ProcessWriteBytesSource = linuxProcessWriteBytesSource
	s.metrics.readDatabaseBytes = func() uint64 {
		return regularFileSize(filepath.Join(s.dir, sqliteDiskSummaryFileName))
	}
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

func sqliteDiskSummarySelectSQL() string {
	return "SELECT gid, uid, ft, mask, count, size, atime, mtime, " +
		sqliteBucketNames("ab") + ", " + sqliteBucketNames("mb") +
		" FROM summaries WHERE dir_id = ? ORDER BY gid, uid, ft, mask"
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

func (s *sqliteDiskSummaryStore) upsertSummary(
	dirID uint32,
	key dirguta.GUTAKey,
	mask uint32,
	sum *summary.SummaryWithTimes,
) error {
	s.resetMetricLimit()

	pendingKey := diskSummaryKey{dirID: dirID, gid: key.GID, uid: key.UID, ft: key.FileType, mask: mask}
	if s.shouldFlushBefore(pendingKey) {
		if err := s.flush(); err != nil {
			return err
		}
	}

	pending := s.pending[pendingKey]
	if pending == nil {
		pending = new(summary.SummaryWithTimes)
		s.pending[pendingKey] = pending
		s.pendingBytes += sqliteDiskSummaryAccumulatorEntryBytes
	}

	pending.AddSummary(sum)

	s.pendingRows++
	s.metrics.RowsReceived++
	s.metrics.MaxWriteBehindBytes = max(s.metrics.MaxWriteBehindBytes, s.pendingBytes)

	return nil
}

func (s *sqliteDiskSummaryStore) shouldFlushBefore(key diskSummaryKey) bool {
	_, exists := s.pending[key]

	return !exists && len(s.pending) > 0 &&
		s.pendingBytes+sqliteDiskSummaryAccumulatorEntryBytes > s.writeBehindLimit
}

func (s *sqliteDiskSummaryStore) flush() error {
	if len(s.pending) == 0 {
		return nil
	}

	keys := make([]diskSummaryKey, 0, len(s.pending))
	for key := range s.pending {
		keys = append(keys, key)
	}

	slices.SortFunc(keys, compareDiskSummaryKeys)

	if s.observeFlush != nil {
		s.observeFlush(keys)
	}

	if err := s.flushRows(keys); err != nil {
		return err
	}

	written := uint64(len(keys))
	combined := s.pendingRows - written
	s.metrics.Flushes++
	s.metrics.RowsCombined += combined
	s.metrics.MaxRowsCombinedPerFlush = max(s.metrics.MaxRowsCombinedPerFlush, combined)
	s.metrics.DatabaseBytes = regularFileSize(filepath.Join(s.dir, sqliteDiskSummaryFileName))
	clear(s.pending)
	s.pendingBytes = 0
	s.pendingRows = 0

	return nil
}

func (s *sqliteDiskSummaryStore) flushRows(keys []diskSummaryKey) error {
	for _, key := range keys {
		gutaKey := dirguta.GUTAKey{GID: key.gid, UID: key.uid, FileType: key.ft}
		if _, err := s.upsert.ExecContext(
			context.Background(), sqliteDiskSummaryArgs(key.dirID, gutaKey, key.mask, s.pending[key])...,
		); err != nil {
			return err
		}

		s.metrics.SQLiteStatements++
		s.metrics.RowsWritten++
	}

	return nil
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

func (s *sqliteDiskSummaryStore) rollUp(
	dirID uint32,
	parentID *uint32,
	hardlinks db.GUTAs,
	emit func(db.GUTAs) error,
) error {
	rows, err := s.summaryRows(dirID)
	if err != nil {
		return err
	}

	if err := emit(materialiseDiskSummaryRows(rows, hardlinks)); err != nil {
		return err
	}

	if parentID != nil {
		for _, row := range rows {
			key := dirguta.GUTAKey{GID: row.gid, UID: row.uid, FileType: row.ft}
			if err := s.upsertSummary(*parentID, key, row.mask, row.summary()); err != nil {
				return err
			}
		}
	}

	return s.clear(dirID)
}

func materialiseDiskSummaryRows(rows []diskSummaryRow, hardlinks db.GUTAs) db.GUTAs {
	expanded := make(map[dirguta.GUTAKey]*summary.SummaryWithTimes)
	for _, row := range rows {
		row.expandInto(expanded)
	}

	for _, guta := range hardlinks {
		expandMaterializedGUTA(expanded, guta)
	}

	keys := make(dirguta.GUTAKeys, 0, len(expanded))
	for key := range expanded {
		keys = append(keys, key)
	}

	sort.Sort(keys)

	return diskMaterializedGUTAs(keys, expanded)
}

func regularFileSize(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0
	}

	return uint64(info.Size()) //nolint:gosec // guarded positive int64 file size.
}

func expandMaterializedGUTA(
	expanded map[dirguta.GUTAKey]*summary.SummaryWithTimes,
	guta *db.GUTA,
) {
	key := dirguta.GUTAKey{
		GID:      guta.GID,
		UID:      guta.UID,
		FileType: guta.FT,
		Age:      guta.Age,
	}

	sum := expanded[key]
	if sum == nil {
		sum = new(summary.SummaryWithTimes)
		expanded[key] = sum
	}

	sum.AddSummary(&summary.SummaryWithTimes{
		Summary: summary.Summary{
			Count: int64(guta.Count), //nolint:gosec
			Size:  int64(guta.Size),  //nolint:gosec
		},
		Atime:        guta.Atime,
		Mtime:        guta.Mtime,
		AtimeBuckets: guta.ATimeRanges,
		MtimeBuckets: guta.MTimeRanges,
	})
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
	if err := s.flush(); err != nil {
		return nil, err
	}

	rows, err := s.selectBy.QueryContext(context.Background(), dirID)
	if err != nil {
		return nil, err
	}

	s.metrics.SQLiteStatements++
	s.metrics.SelectStatements++

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

func (s *sqliteDiskSummaryStore) clear(dirID uint32) error {
	_, err := s.deleteBy.ExecContext(context.Background(), dirID)
	if err == nil {
		s.metrics.SQLiteStatements++
	}

	return err
}

func (s *sqliteDiskSummaryStore) close(success bool) error {
	if s.closed {
		return nil
	}

	flushErr := s.flush()
	stmtErr := errors.Join(closeStmt(s.upsert), closeStmt(s.selectBy), closeStmt(s.deleteBy))
	commitErr := s.tx.Commit()
	closeErr := s.db.Close()
	s.closed = true
	s.metrics.DatabaseBytes = regularFileSize(filepath.Join(s.dir, sqliteDiskSummaryFileName))
	s.metrics.readDatabaseBytes = nil
	closeResult := errors.Join(flushErr, stmtErr, commitErr, closeErr)

	if !s.removeDirOnClose || !success || closeResult != nil {
		return closeResult
	}

	return os.RemoveAll(s.dir)
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
