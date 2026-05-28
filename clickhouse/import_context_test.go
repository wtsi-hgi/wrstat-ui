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
		So(conn.deadlinePrepares, ShouldEqual, 0)
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
		So(conn.deadlinePrepares, ShouldEqual, 0)
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
			insertMountDirDGUTAVectorQuery,
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
			So(conn.deadlinePrepares, ShouldEqual, 0)
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
		So(conn.deadlinePrepares, ShouldEqual, 0)
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
