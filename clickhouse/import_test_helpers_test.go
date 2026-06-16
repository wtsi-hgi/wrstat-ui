/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var errForcedFailure = errors.New("forced failure")

const (
	testMountPath            = "/mnt/test/"
	testRootMountPath        = "/mnt/"
	testT283ImagingMountPath = "/nfs/t283_imaging/"
	providerTestMountPath    = "/mnt/test/"

	dgutaWriterTestSnapshotIDColumn = "snapshot_id"
	dgutaWriterTestMountPathColumn  = "mount_path"
	dgutaWriterTestUpdatedAtColumn  = "updated_at"

	insertDGUTAQuery = insertMountDirSummaryQuery

	testInsertBasedirsHistoryStmt = "INSERT INTO wrstat_basedirs_history (mount_path, gid, date, " +
		"usage_size, quota_size, usage_inodes, quota_inodes) VALUES (?, ?, ?, ?, ?, ?, ?)"

	testInsertInfoFactVectorStmt = "INSERT INTO wrstat_dir_facts " +
		"(mount_path, snapshot_id, dir_id, parent_id, subtree_end, updated_at, gids, uids, fts, ages, " +
		"counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) " +
		"VALUES (?, ?, ?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"

	dgutaWriterTestCountAgeAllQuery = "SELECT count() FROM wrstat_dir_filter_ageall WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?)"
)

func d1CountActiveSetRowsQuery(table string) string {
	return fmt.Sprintf("SELECT count() FROM %s WHERE active_set_id = ?", table)
}

func alterTableNameForTest(query string) string {
	fields := strings.Fields(query)
	for i := range fields {
		if i+2 >= len(fields) {
			break
		}

		if strings.EqualFold(fields[i], "ALTER") && strings.EqualFold(fields[i+1], "TABLE") {
			return fields[i+2]
		}
	}

	return ""
}

type d1ActiveVirtualSetForTest struct {
	ready       uint8
	summaryRows uint64
	filterRows  uint64
	childRows   uint64
}

func readActiveVirtualSetForTest(
	ctx context.Context,
	conn interface {
		Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	},
	activeSetID string,
) d1ActiveVirtualSetForTest {
	rows, err := conn.Query(ctx, "SELECT ready, summary_rows, filter_rows, child_rows "+
		"FROM wrstat_active_virtual_sets WHERE active_set_id = ?", activeSetID)
	So(err, ShouldBeNil)

	defer func() { _ = rows.Close() }()

	So(rows.Next(), ShouldBeTrue)

	var out d1ActiveVirtualSetForTest
	So(rows.Scan(&out.ready, &out.summaryRows, &out.filterRows, &out.childRows), ShouldBeNil)
	So(rows.Next(), ShouldBeFalse)

	return out
}

type dgutaWriterCloseContextRows struct {
	columns []string
	values  [][]any
	index   int
}

func emptyMountsActiveRowsForTest() *dgutaWriterCloseContextRows {
	return &dgutaWriterCloseContextRows{
		columns: []string{
			dgutaWriterTestMountPathColumn,
			dgutaWriterTestSnapshotIDColumn,
			dgutaWriterTestUpdatedAtColumn,
		},
	}
}

func zeroCountRowsForTest() *dgutaWriterCloseContextRows {
	return &dgutaWriterCloseContextRows{
		columns: []string{"count()"},
		values:  [][]any{{uint64(0)}},
	}
}

func activeVirtualValidationRowsForBatches(
	batches []*b1ImportSQLSpyBatch,
	start int,
	end int,
) *dgutaWriterCloseContextRows {
	rows := &dgutaWriterCloseContextRows{}

	for _, batch := range batches {
		for _, values := range batch.values {
			rows.values = append(rows.values, append([]any(nil), values[start:end]...))
		}
	}

	return rows
}

func activeVirtualChildValidationRowsForBatches(
	batches []*b1ImportSQLSpyBatch,
	start int,
	end int,
) *dgutaWriterCloseContextRows {
	rows := activeVirtualValidationRowsForBatches(batches, start, end)
	sort.Slice(rows.values, func(i, j int) bool {
		return activeVirtualChildValidationRowLess(rows.values[i], rows.values[j])
	})

	return rows
}

func (r *dgutaWriterCloseContextRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}

	r.index++

	return true
}

func (r *dgutaWriterCloseContextRows) HasData() bool {
	return len(r.values) > 0
}

func (r *dgutaWriterCloseContextRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.values) {
		return errBootstrapTestUnexpectedCall
	}

	row := r.values[r.index-1]
	for i, value := range row {
		switch ptr := dest[i].(type) {
		case *string:
			str, ok := value.(string)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = str
		case *time.Time:
			t, ok := value.(time.Time)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = t
		case *uint64:
			n, ok := value.(uint64)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *uint32:
			n, ok := value.(uint32)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *uint16:
			n, ok := value.(uint16)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *int64:
			n, ok := value.(int64)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *uint8:
			n, ok := value.(uint8)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *[]uint64:
			n, ok := value.([]uint64)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		case *[]uint32:
			n, ok := value.([]uint32)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = n
		default:
			return errBootstrapTestUnexpectedCall
		}
	}

	return nil
}

func (r *dgutaWriterCloseContextRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *dgutaWriterCloseContextRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *dgutaWriterCloseContextRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *dgutaWriterCloseContextRows) Columns() []string {
	return r.columns
}

func (r *dgutaWriterCloseContextRows) Close() error {
	return nil
}

func (r *dgutaWriterCloseContextRows) Err() error {
	return nil
}

func mountsActiveRowsForTest(rows []mountsActiveRow) driver.Rows {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{row.mountPath, row.snapshotID, row.updatedAt})
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestMountPathColumn, dgutaWriterTestSnapshotIDColumn, dgutaWriterTestUpdatedAtColumn},
		values:  values,
	}
}

func countRows(ctx context.Context, conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}, query string, args ...any) uint64 {
	rows, err := conn.Query(ctx, query, args...)
	So(err, ShouldBeNil)

	defer func() { _ = rows.Close() }()

	So(rows.Next(), ShouldBeTrue)

	var n uint64
	So(rows.Scan(&n), ShouldBeNil)
	So(rows.Next(), ShouldBeFalse)

	return n
}

type partitionDropDeadlineConn struct {
	bootstrapTestConn

	normalWindow time.Duration
	err          error

	drops           atomic.Int32
	cleanupDeadline atomic.Int32
}

func (c *partitionDropDeadlineConn) Exec(ctx context.Context, query string, _ ...any) error {
	if !strings.HasPrefix(query, "ALTER TABLE") {
		return errBootstrapTestUnexpectedCall
	}

	c.drops.Add(1)

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return context.DeadlineExceeded
	}

	c.cleanupDeadline.Add(1)

	return c.err
}

func (c *partitionDropDeadlineConn) partitionDrops() int {
	return int(c.drops.Load())
}

func (c *partitionDropDeadlineConn) cleanupDeadlineDrops() int {
	return int(c.cleanupDeadline.Load())
}

type countingDGUTABatch struct {
	rows     int
	maxRows  int
	appended int
	sent     bool
	flushes  int
	sends    int
}

func (b *countingDGUTABatch) Abort() error {
	b.sent = true
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) Append(...any) error {
	b.rows++
	b.appended++

	return nil
}

func (b *countingDGUTABatch) AppendStruct(any) error {
	b.rows++
	b.appended++

	return nil
}

func (b *countingDGUTABatch) Column(int) driver.BatchColumn {
	return nil
}

func (b *countingDGUTABatch) Flush() error {
	b.recordRows()
	b.flushes++
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) Send() error {
	b.recordRows()
	b.sent = true
	b.sends++
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) IsSent() bool {
	return b.sent
}

func (b *countingDGUTABatch) Rows() int {
	return b.rows
}

func (b *countingDGUTABatch) Columns() []column.Interface {
	return nil
}

func (b *countingDGUTABatch) Close() error {
	return nil
}

func (b *countingDGUTABatch) recordRows() {
	if b.rows > b.maxRows {
		b.maxRows = b.rows
	}
}

func writeD1SingleRecord(
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	dir *summary.DirectoryPath,
	gid uint32,
) {
	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	So(w, ShouldNotBeNil)

	w.SetMountPath(mountPath)
	w.SetUpdatedAt(updatedAt)
	So(w.Add(db.RecordDGUTA{
		Dir:        dir,
		DirID:      1,
		SubtreeEnd: 2,
		Depth:      uint16(dir.Depth), //nolint:gosec // Test fixture paths have bounded depth.
		GUTAs: db.GUTAs{
			b1GUTA(gid, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
		},
	}), ShouldBeNil)
	So(w.Close(), ShouldBeNil)
}

func b1GUTA(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count, size uint64,
	atime, mtime int64,
) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         age,
		Count:       count,
		Size:        size,
		Atime:       atime,
		Mtime:       mtime,
		ATimeRanges: [9]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		MTimeRanges: [9]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	}
}

type lazyDGUTAImportConn struct {
	bootstrapTestConn

	batches map[string][]*countingDGUTABatch
}

func (c *lazyDGUTAImportConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{dgutaWriterTestSnapshotIDColumn},
		}, nil
	case mountsActiveRowsQuery:
		return emptyMountsActiveRowsForTest(), nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *lazyDGUTAImportConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if !isLazyDGUTAImportQuery(query) {
		return nil, errBootstrapTestUnexpectedCall
	}

	batch := &countingDGUTABatch{}

	if c.batches == nil {
		c.batches = make(map[string][]*countingDGUTABatch)
	}

	c.batches[query] = append(c.batches[query], batch)

	return batch, nil
}

func isLazyDGUTAImportQuery(query string) bool {
	switch query {
	case insertDirsQuery,
		insertMountDirSummaryQuery,
		insertDirFilterAgeAllQuery,
		insertChildFilterAllQuery,
		insertDirFilterAllQuery:
		return true
	default:
		return false
	}
}

func (c *lazyDGUTAImportConn) totalRowsFor(query string) int {
	var count int
	for _, batch := range c.batches[query] {
		count += batch.appended
	}

	return count
}

func (c *lazyDGUTAImportConn) maxRowsFor(query string) int {
	var maxRows int
	for _, batch := range c.batches[query] {
		if batch.maxRows > maxRows {
			maxRows = batch.maxRows
		}
	}

	return maxRows
}

type b1ImportSQLSpyBatch struct {
	countingDGUTABatch

	appends int
	sends   int
	values  [][]any
}

func (b *b1ImportSQLSpyBatch) Append(values ...any) error {
	b.appends++
	b.values = append(b.values, append([]any(nil), values...))

	return b.countingDGUTABatch.Append(values...)
}

func (b *b1ImportSQLSpyBatch) Send() error {
	b.sends++

	return b.countingDGUTABatch.Send()
}

type b1ImportSQLSpyConn struct {
	bootstrapTestConn

	prepared   []string
	batches    map[string]*b1ImportSQLSpyBatch
	allBatches map[string][]*b1ImportSQLSpyBatch
}

func (c *b1ImportSQLSpyConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	c.prepared = append(c.prepared, query)

	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{dgutaWriterTestSnapshotIDColumn},
		}, nil
	case mountsActiveRowsQuery:
		return emptyMountsActiveRowsForTest(), nil
	default:
		if rows, ok := c.activeVirtualValidationRows(query); ok {
			return rows, nil
		}

		if d1FakeSnapshotCountQuery(query) {
			return zeroCountRowsForTest(), nil
		}

		if isB1ImportSQLSpyActiveVirtualSourceQuery(query) {
			return &dgutaWriterCloseContextRows{}, nil
		}

		return nil, errBootstrapTestUnexpectedCall
	}
}

func d1FakeSnapshotCountQuery(query string) bool {
	return strings.HasPrefix(query, "SELECT count() FROM wrstat_") &&
		strings.Contains(query, "WHERE mount_path = ? AND snapshot_id = toUUID(?)")
}

func isB1ImportSQLSpyActiveVirtualSourceQuery(query string) bool {
	return strings.Contains(query, "arrayJoin") ||
		strings.Contains(query, "full_path = d.mount_path") ||
		strings.Contains(query, "full_path = f.mount_path")
}

func (c *b1ImportSQLSpyConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepared = append(c.prepared, query)

	switch query {
	case insertDirsQuery,
		insertMountDirSummaryQuery,
		insertDirFilterAgeAllQuery,
		insertChildFilterAllQuery,
		insertDirFilterAllQuery,
		insertMountDirSummarySetQuery,
		insertActiveVirtualDirQuery,
		insertActiveVirtualSummaryQuery,
		insertActiveVirtualFilterAllQuery,
		insertActiveVirtualChildQuery,
		insertActiveVirtualSetQuery:
	default:
		return nil, errBootstrapTestUnexpectedCall
	}

	if c.batches == nil {
		c.batches = make(map[string]*b1ImportSQLSpyBatch)
	}

	if c.allBatches == nil {
		c.allBatches = make(map[string][]*b1ImportSQLSpyBatch)
	}

	batch := &b1ImportSQLSpyBatch{}
	c.batches[query] = batch
	c.allBatches[query] = append(c.allBatches[query], batch)

	return batch, nil
}

func (c *b1ImportSQLSpyConn) batchStats(query string) b1ImportSQLSpyBatch {
	if c.batches == nil || c.batches[query] == nil {
		return b1ImportSQLSpyBatch{}
	}

	return *c.batches[query]
}

func (c *b1ImportSQLSpyConn) activeVirtualValidationRows(query string) (*dgutaWriterCloseContextRows, bool) {
	switch query {
	case selectActiveVirtualSummariesValidationQuery:
		return activeVirtualValidationRowsForBatches(c.allBatches[insertActiveVirtualSummaryQuery], 1, 19), true
	case selectActiveVirtualFilterAllValidationQuery:
		return activeVirtualValidationRowsForBatches(c.allBatches[insertActiveVirtualFilterAllQuery], 1, 14), true
	case selectActiveVirtualChildrenValidationQuery:
		return activeVirtualChildValidationRowsForBatches(c.allBatches[insertActiveVirtualChildQuery], 1, 8), true
	default:
		return nil, false
	}
}

func activeVirtualChildValidationRowLess(a, b []any) bool {
	aParent, aParentOK := a[0].(uint32)

	bParent, bParentOK := b[0].(uint32)
	if !aParentOK || !bParentOK {
		return false
	}

	if aParent != bParent {
		return aParent < bParent
	}

	aChild, aChildOK := a[1].(uint32)

	bChild, bChildOK := b[1].(uint32)
	if !aChildOK || !bChildOK {
		return false
	}

	return aChild < bChild
}
