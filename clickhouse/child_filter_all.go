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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const insertChildFilterAllQuery = "INSERT INTO wrstat_child_filter_all " +
	"(mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir, count, size, " +
	"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, " +
	"child_count, has_filter_children, has_children, refreshed_at) " +
	"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

var errChildFilterAllBatchNotPrepared = errors.New("clickhouse: child full-filter batch is not prepared")

type childFilterAllWriter struct {
	conn ch.Conn

	batch    driver.Batch
	openedAt time.Time
	batchNow func() time.Time

	batchSize int
	writeErr  error
}

func newChildFilterAllWriter(conn ch.Conn, batchSize int) *childFilterAllWriter {
	return &childFilterAllWriter{
		conn:      conn,
		batchSize: batchSize,
	}
}

func (w *childFilterAllWriter) appendRow(ctx context.Context, row filterAllRow) error {
	return w.blockWriter().append(ctx, func(batch driver.Batch) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.ParentDir,
			uint8(row.Age),
			row.GID,
			row.UID,
			uint16(row.FT),
			row.Dir,
			row.Count,
			row.Size,
			row.AtimeMin,
			row.MtimeMax,
			row.AtimeBuckets,
			row.MtimeBuckets,
			row.FilterChildCount,
			row.ChildCount,
			row.HasFilterChildren,
			row.HasChildren,
			row.RefreshedAt,
		)
	})
}

func (w *childFilterAllWriter) flush(context.Context) error {
	return w.blockWriter().close()
}

func (w *childFilterAllWriter) abort() error {
	return w.blockWriter().abort()
}

func (w *childFilterAllWriter) blockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertChildFilterAllQuery,
		name:        "child full-filter",
		batch:       &w.batch,
		openedAt:    &w.openedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.batchSize,
		notPrepared: errChildFilterAllBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *childFilterAllWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}
