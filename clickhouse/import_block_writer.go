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
	"errors"
	"fmt"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	importBatchSendTimeout    = defaultCHReceiveTimeout
	importFinalizationTimeout = defaultCHReceiveTimeout
)

var errImportBlockBatchNotPrepared = errors.New("clickhouse: import block batch is not prepared")

func importFinalizationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := queryContext(context.WithoutCancel(parent), importFinalizationTimeout)
	stopParentCancel := context.AfterFunc(parent, func() {
		if errors.Is(parent.Err(), context.Canceled) {
			cancel()
		}
	})

	return ctx, func() {
		stopParentCancel()
		cancel()
	}
}

type importBlockWriter struct {
	conn ch.Conn

	query string
	name  string

	batch    *driver.Batch
	openedAt *time.Time
	writeErr *error

	batchSize   int
	notPrepared error
	now         func() time.Time
}

func (w *importBlockWriter) append(
	ctx context.Context,
	appendRow func(driver.Batch) error,
) error {
	if err := w.err(); err != nil {
		return err
	}

	if err := w.sendIfTooOld(); err != nil {
		return err
	}

	if err := w.ensureReady(ctx); err != nil {
		return err
	}

	if err := appendRow(*w.batch); err != nil {
		return fmt.Errorf(
			"clickhouse: failed to append %s row: %w",
			w.name,
			err,
		)
	}

	return w.sendIfFull()
}

func (w *importBlockWriter) ensureReady(ctx context.Context) error {
	if err := w.err(); err != nil {
		return err
	}

	if w.batch == nil || w.conn == nil {
		return w.setErr(w.notPreparedError())
	}

	if *w.batch != nil {
		return nil
	}

	prepared, err := prepareImportBatch(ctx, w.conn, w.query)
	if err != nil {
		return w.setErr(fmt.Errorf(
			"clickhouse: failed to prepare %s batch: %w",
			w.name,
			err,
		))
	}

	*w.batch = prepared
	markImportBatchOpened(w.openedAt, w.importBatchNow)

	return nil
}

func prepareImportBatch(ctx context.Context, conn ch.Conn, query string) (driver.Batch, error) {
	importCtx, cancel := importBatchContext(ctx)

	batch, err := conn.PrepareBatch(importCtx, query)
	if err != nil {
		cancel()

		return nil, err
	}

	return &importPreparedBatch{Batch: batch, cancel: cancel}, nil
}

func markImportBatchOpened(openedAt *time.Time, now func() time.Time) {
	if openedAt != nil {
		*openedAt = now()
	}
}

func (w *importBlockWriter) sendIfFull() error {
	if w.batch == nil || *w.batch == nil || w.batchSize <= 0 {
		return nil
	}

	if (*w.batch).Rows() < w.batchSize {
		return nil
	}

	return w.send()
}

func (w *importBlockWriter) sendIfTooOld() error {
	if !w.openTooLong() {
		return nil
	}

	return w.send()
}

func (w *importBlockWriter) openTooLong() bool {
	return w.batch != nil &&
		*w.batch != nil &&
		(*w.batch).Rows() > 0 &&
		w.openedAt != nil &&
		importBatchOpenTooLong(*w.openedAt, w.importBatchNow)
}

func importBatchOpenTooLong(openedAt time.Time, now func() time.Time) bool {
	if openedAt.IsZero() {
		return false
	}

	return now().Sub(openedAt) >= importBatchMaxOpenDuration
}

func (w *importBlockWriter) close() error {
	if err := w.err(); err != nil {
		return err
	}

	if w.batch == nil || *w.batch == nil {
		return nil
	}

	if (*w.batch).Rows() == 0 {
		return w.abort()
	}

	return w.send()
}

func (w *importBlockWriter) abort() error {
	if w.batch == nil || *w.batch == nil {
		return nil
	}

	err := (*w.batch).Abort()
	*w.batch = nil
	clearImportBatchOpened(w.openedAt)

	if err == nil {
		return nil
	}

	return w.setErr(fmt.Errorf(
		"clickhouse: failed to abort %s batch: %w",
		w.name,
		err,
	))
}

func clearImportBatchOpened(openedAt *time.Time) {
	if openedAt != nil {
		*openedAt = time.Time{}
	}
}

func (w *importBlockWriter) send() error {
	if w.batch == nil || *w.batch == nil {
		return nil
	}

	err := (*w.batch).Send()
	*w.batch = nil
	clearImportBatchOpened(w.openedAt)

	if err == nil {
		return nil
	}

	return w.setErr(fmt.Errorf(
		"clickhouse: failed to send %s batch: %w",
		w.name,
		err,
	))
}

func (w *importBlockWriter) err() error {
	if w != nil && w.writeErr != nil {
		return *w.writeErr
	}

	return nil
}

func (w *importBlockWriter) setErr(err error) error {
	if w != nil && w.writeErr != nil {
		*w.writeErr = err
	}

	return err
}

func (w *importBlockWriter) importBatchNow() time.Time {
	if w != nil && w.now != nil {
		return w.now()
	}

	return time.Now()
}

func (w *importBlockWriter) notPreparedError() error {
	if w.notPrepared != nil {
		return w.notPrepared
	}

	return fmt.Errorf("%w: %s", errImportBlockBatchNotPrepared, w.name)
}

type importPreparedBatch struct {
	driver.Batch

	cancel     context.CancelFunc
	cancelOnce sync.Once
}

func (b *importPreparedBatch) Send() error {
	defer b.cancelContext()

	return b.Batch.Send()
}

func (b *importPreparedBatch) Abort() error {
	defer b.cancelContext()

	return b.Batch.Abort()
}

func (b *importPreparedBatch) Close() error {
	defer b.cancelContext()

	return b.Batch.Close()
}

func (b *importPreparedBatch) cancelContext() {
	if b.cancel == nil {
		return
	}

	b.cancelOnce.Do(b.cancel)
}

func importBatchContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	// clickhouse-go stores the PrepareBatch context and reuses it during Send,
	// so import batches detach from caller cancellation while retaining a
	// bounded send lifetime.
	return queryContext(context.WithoutCancel(parent), importBatchSendTimeout)
}
