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

package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

type importContextColumn struct{}

func (importContextColumn) Append(any) error {
	return nil
}

func (importContextColumn) AppendRow(any) error {
	return nil
}

type importBlockWriterSpyBatch struct {
	rows        int
	appends     int
	sends       int
	aborts      int
	emptySends  int
	sentRows    []int
	sendCtxErrs []error
	ctxErr      func() error
	deadline    time.Time
	hasDeadline bool
}

func (b *importBlockWriterSpyBatch) Abort() error {
	b.aborts++
	b.rows = 0

	return nil
}

func (b *importBlockWriterSpyBatch) Append(...any) error {
	b.appends++
	b.rows++

	return nil
}

func (b *importBlockWriterSpyBatch) AppendStruct(any) error {
	b.appends++
	b.rows++

	return nil
}

func (b *importBlockWriterSpyBatch) Column(int) driver.BatchColumn {
	return nil
}

func (b *importBlockWriterSpyBatch) Flush() error {
	return nil
}

func (b *importBlockWriterSpyBatch) Send() error {
	b.sends++

	b.sendCtxErrs = append(b.sendCtxErrs, b.ctxErr())
	if b.rows == 0 {
		b.emptySends++
	} else {
		b.sentRows = append(b.sentRows, b.rows)
	}

	b.rows = 0

	return nil
}

func (b *importBlockWriterSpyBatch) IsSent() bool {
	return b.sends > 0 || b.aborts > 0
}

func (b *importBlockWriterSpyBatch) Rows() int {
	return b.rows
}

func (b *importBlockWriterSpyBatch) Columns() []column.Interface {
	return nil
}

func (b *importBlockWriterSpyBatch) Close() error {
	return nil
}

func TestClickHouseImportBatchContexts(t *testing.T) {
	Convey("Import batches do not inherit normal query deadlines into their Send lifecycle", t, func() {
		conn := &importContextConn{}

		ctx, cancel := queryContext(context.Background(), time.Millisecond)
		defer cancel()

		batch, err := prepareImportBatch(ctx, conn, insertDGUTAQuery)
		So(err, ShouldBeNil)
		So(batch, ShouldNotBeNil)

		waitForContextDone(ctx)

		So(ctx.Err(), ShouldNotBeNil)
		So(batch.Send(), ShouldBeNil)
		So(conn.prepares, ShouldEqual, 1)
		So(conn.deadlinePrepares, ShouldEqual, 1)
	})

	Convey("Import batch sends do not depend on released-connection reacquire semantics", t, func() {
		conn := &importContextConn{releasedSendErr: context.DeadlineExceeded}

		batch, err := prepareImportBatch(context.Background(), conn, insertDGUTAQuery)
		So(err, ShouldBeNil)
		So(batch, ShouldNotBeNil)

		So(batch.Send(), ShouldBeNil)
		So(conn.prepares, ShouldEqual, 1)
		So(conn.releasePrepares, ShouldEqual, 0)
	})

	Convey("Import batch reprepare replaces an already-expired normal query context", t, func() {
		conn := &importContextConn{}
		batch := &countingDGUTABatch{}
		So(batch.Append("row"), ShouldBeNil)

		ctx, cancel := queryContext(context.Background(), time.Millisecond)
		defer cancel()

		waitForContextDone(ctx)

		next, err := sendAndReprepareBatch(ctx, conn, batch, insertChildrenQuery)
		So(err, ShouldBeNil)
		So(next, ShouldNotBeNil)
		So(batch.sends, ShouldEqual, 1)
		So(conn.prepares, ShouldEqual, 1)
		So(conn.deadlinePrepares, ShouldEqual, 1)
		So(conn.releasePrepares, ShouldEqual, 0)
	})

	Convey("Every long-running import prepared-batch table uses the shared import context semantics", t, func() {
		ctx, cancel := queryContext(context.Background(), time.Millisecond)
		defer cancel()

		waitForContextDone(ctx)

		queries := []string{
			insertDGUTAQuery,
			insertChildrenQuery,
			insertMountDirSummaryQuery,
			insertMountDirSummarySetQuery,
			insertFilesBatchQuery,
			insertBasedirsGroupUsageQuery,
			insertBasedirsUserUsageQuery,
			insertBasedirsGroupSubdirsQuery,
			insertBasedirsUserSubdirsQuery,
		}

		var totalPrepares int

		for _, query := range queries {
			conn := &importContextConn{}
			batch, err := prepareImportBatch(ctx, conn, query)
			So(err, ShouldBeNil)
			So(batch, ShouldNotBeNil)
			So(conn.deadlinePrepares, ShouldEqual, 1)
			So(conn.releasePrepares, ShouldEqual, 0)

			totalPrepares += conn.prepares
		}

		So(totalPrepares, ShouldEqual, len(queries))
	})

	Convey("File ingest can prepare a fresh files batch after the caller query context is spent", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		conn := &importContextConn{}
		writer := &fileIngestWriter{
			cfg:       Config{QueryTimeout: time.Millisecond},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
			prepared:  true,
			batchSize: 1,
		}
		writer.buf.appendRow(fileIngestRow{
			mountPath:    testMountPath,
			snapshot:     uuid.New(),
			parentDir:    testMountPath,
			name:         "late.txt",
			ext:          "txt",
			entryType:    1,
			size:         1,
			apparentSize: 1,
			atime:        updatedAt,
			mtime:        updatedAt,
			ctime:        updatedAt,
		})

		ctx, cancel := queryContext(context.Background(), time.Millisecond)
		defer cancel()

		waitForContextDone(ctx)

		So(writer.sendBufferedData(ctx), ShouldBeNil)
		So(writer.batch, ShouldBeNil)
		So(writer.buf.rows(), ShouldEqual, 0)
		So(conn.prepares, ShouldEqual, 1)
		So(conn.deadlinePrepares, ShouldEqual, 1)
	})

	Convey("Partition cleanup replaces an already-expired normal query context", t, func() {
		conn := &partitionDropDeadlineConn{normalWindow: time.Millisecond}

		ctx, cancel := queryContext(context.Background(), time.Millisecond)
		defer cancel()

		waitForContextDone(ctx)

		err := dropPartitionIgnoreUnknown(
			ctx,
			conn,
			testMountPath,
			"00000000-0000-0000-0000-000000000009",
			dropFilesPartitionQuery,
		)
		So(err, ShouldBeNil)
		So(conn.partitionDrops(), ShouldEqual, 1)
		So(conn.cleanupDeadlineDrops(), ShouldEqual, 1)
	})
}

func waitForContextDone(ctx context.Context) {
	<-ctx.Done()
}

func TestClickHouseImportBlockWriter(t *testing.T) {
	Convey("Import block writer closes an unused writer without preparing a batch", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)

		So(writer.close(), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 0)
		So(conn.totalAppends(), ShouldEqual, 0)
		So(conn.totalSends(), ShouldEqual, 0)
		So(conn.totalAborts(), ShouldEqual, 0)
	})

	Convey("Import block writer prepares lazily on the first appended row", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)

		So(conn.prepareCalls, ShouldEqual, 0)
		So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 1)
	})

	Convey("Import block writer sends bounded blocks and skips empty sends", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)

		for range 5 {
			So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)
		}

		So(writer.close(), ShouldBeNil)
		So(conn.sentRows(), ShouldResemble, []int{2, 2, 1})
		So(conn.emptySends(), ShouldEqual, 0)
	})

	Convey("Import block writer sends an old non-empty block before appending the next row", t, func() {
		now := time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC)
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 100, func() time.Time { return now })

		So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)

		firstBatch := conn.batches[0]
		now = now.Add(importBatchMaxOpenDuration)

		So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)
		So(firstBatch.sends, ShouldEqual, 1)
		So(conn.sentRows(), ShouldResemble, []int{1})
		So(conn.batches, ShouldHaveLength, 2)
		So(conn.batches[1].Rows(), ShouldEqual, 1)
	})

	Convey("Import block writer does not prepare again after an automatic full-block send", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)

		So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)
		So(writer.append(context.Background(), appendSpyImportBlockRow), ShouldBeNil)
		So(writer.close(), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.totalSends(), ShouldEqual, 1)
	})

	Convey("Import block writer aborts a prepared empty batch without sending", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)

		So(writer.ensureReady(context.Background()), ShouldBeNil)
		So(writer.close(), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.totalAborts(), ShouldEqual, 1)
		So(conn.totalSends(), ShouldEqual, 0)
	})

	Convey("Import batch send uses a detached timeout context after the parent is canceled", t, func() {
		conn := &importBlockWriterSpyConn{}
		writer := newTestImportBlockWriter(conn, 2, time.Now)
		parent, cancel := context.WithCancel(context.Background())

		So(writer.append(parent, appendSpyImportBlockRow), ShouldBeNil)
		cancel()

		So(writer.close(), ShouldBeNil)
		So(conn.parentCanceledSendErrs(), ShouldResemble, []error{nil})
		So(conn.prepareDeadlines(), ShouldHaveLength, 1)
	})
}

func newTestImportBlockWriter(
	conn *importBlockWriterSpyConn,
	batchSize int,
	now func() time.Time,
) *importBlockWriter {
	var (
		batch    driver.Batch
		openedAt time.Time
		writeErr error
	)

	return &importBlockWriter{
		conn:      conn,
		query:     "INSERT INTO spy",
		name:      "spy",
		batch:     &batch,
		openedAt:  &openedAt,
		batchSize: batchSize,
		now:       now,
		writeErr:  &writeErr,
	}
}

func appendSpyImportBlockRow(batch driver.Batch) error {
	return batch.Append("row")
}

type importBlockWriterSpyConn struct {
	bootstrapTestConn

	prepareCalls int
	batches      []*importBlockWriterSpyBatch
}

func (c *importBlockWriterSpyConn) PrepareBatch(
	ctx context.Context,
	_ string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepareCalls++

	batch := &importBlockWriterSpyBatch{ctxErr: ctx.Err}
	batch.deadline, batch.hasDeadline = ctx.Deadline()
	c.batches = append(c.batches, batch)

	return batch, nil
}

func (c *importBlockWriterSpyConn) sentRows() []int {
	var rows []int
	for _, batch := range c.batches {
		rows = append(rows, batch.sentRows...)
	}

	return rows
}

func (c *importBlockWriterSpyConn) emptySends() int {
	var sends int
	for _, batch := range c.batches {
		sends += batch.emptySends
	}

	return sends
}

func (c *importBlockWriterSpyConn) totalAppends() int {
	var appends int
	for _, batch := range c.batches {
		appends += batch.appends
	}

	return appends
}

func (c *importBlockWriterSpyConn) totalSends() int {
	var sends int
	for _, batch := range c.batches {
		sends += batch.sends
	}

	return sends
}

func (c *importBlockWriterSpyConn) totalAborts() int {
	var aborts int
	for _, batch := range c.batches {
		aborts += batch.aborts
	}

	return aborts
}

func (c *importBlockWriterSpyConn) parentCanceledSendErrs() []error {
	var errs []error
	for _, batch := range c.batches {
		errs = append(errs, batch.sendCtxErrs...)
	}

	return errs
}

func (c *importBlockWriterSpyConn) prepareDeadlines() []time.Time {
	var deadlines []time.Time

	for _, batch := range c.batches {
		if batch.hasDeadline {
			deadlines = append(deadlines, batch.deadline)
		}
	}

	return deadlines
}

type importContextBatch struct {
	*countingDGUTABatch

	ctxErr func() error

	releasedSendErr   error
	releaseConnection bool
}

func (b *importContextBatch) Column(int) driver.BatchColumn {
	return importContextColumn{}
}

func (b *importContextBatch) Send() error {
	if b.releaseConnection && b.releasedSendErr != nil {
		return b.releasedSendErr
	}

	if err := b.ctxErr(); err != nil {
		return err
	}

	return b.countingDGUTABatch.Send()
}

type importContextConn struct {
	bootstrapTestConn

	prepares         int
	deadlinePrepares int
	releasePrepares  int
	releasedSendErr  error
}

func (c *importContextConn) PrepareBatch(
	ctx context.Context,
	_ string,
	opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, ok := ctx.Deadline(); ok {
		c.deadlinePrepares++
	}

	var batchOpts driver.PrepareBatchOptions
	for _, opt := range opts {
		opt(&batchOpts)
	}

	if batchOpts.ReleaseConnection {
		c.releasePrepares++
	}

	c.prepares++

	return &importContextBatch{
		countingDGUTABatch: &countingDGUTABatch{},
		ctxErr:             ctx.Err,
		releasedSendErr:    c.releasedSendErr,
		releaseConnection:  batchOpts.ReleaseConnection,
	}, nil
}
