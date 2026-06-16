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
	"hash/fnv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const insertDirsQuery = "INSERT INTO wrstat_dirs " +
	"(mount_path, snapshot_id, dir_id, parent_id, subtree_end, depth, name, full_path, " +
	"child_dir_count, child_file_count, path_hash) " +
	"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)"

var errCatalogBatchNotPrepared = errors.New("clickhouse: directory catalog batch is not prepared")

type catalogRow struct {
	mountPath      string
	snapshotID     string
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	name           string
	fullPath       string
	childDirCount  uint32
	childFileCount uint32
	pathHash       uint64
}

type catalogWriter struct {
	conn ch.Conn

	batch    driver.Batch
	openedAt time.Time
	batchNow func() time.Time

	batchSize int
	writeErr  error
}

func newCatalogWriter(conn ch.Conn, batchSize int) *catalogWriter {
	return &catalogWriter{
		conn:      conn,
		batchSize: batchSize,
	}
}

func (w *catalogWriter) appendRecord(
	ctx context.Context,
	mount activeMount,
	record db.RecordDGUTA,
	fullPath string,
) error {
	row := catalogRowFromRecord(mount, record, fullPath)

	return w.blockWriter().append(ctx, func(batch driver.Batch) error {
		return batch.Append(
			row.mountPath,
			row.snapshotID,
			row.dirID,
			row.parentID,
			row.subtreeEnd,
			row.depth,
			row.name,
			row.fullPath,
			row.childDirCount,
			row.childFileCount,
			row.pathHash,
		)
	})
}

func catalogRowFromRecord(mount activeMount, record db.RecordDGUTA, fullPath string) catalogRow {
	fullPath = ensureTrailingSlash(fullPath)

	return catalogRow{
		mountPath:      mount.mountPath,
		snapshotID:     mount.snapshotID,
		dirID:          record.DirID,
		parentID:       record.ParentID,
		subtreeEnd:     record.SubtreeEnd,
		depth:          record.Depth,
		name:           catalogNameForFullPath(fullPath),
		fullPath:       fullPath,
		childDirCount:  safeUint32(record.ChildCount),
		childFileCount: safeUint32(record.ChildFileCount),
		pathHash:       catalogPathHash(fullPath),
	}
}

func catalogNameForFullPath(fullPath string) string {
	fullPath = ensureTrailingSlash(fullPath)
	if fullPath == "/" {
		return "/"
	}

	trimmed := strings.TrimSuffix(fullPath, "/")

	idx := strings.LastIndexByte(trimmed, '/')
	if idx < 0 {
		return trimmed + "/"
	}

	return trimmed[idx+1:] + "/"
}

func catalogPathHash(fullPath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fullPath))

	return h.Sum64()
}

func safeUint32(value uint64) uint32 {
	if value > uint64(^uint32(0)) {
		return ^uint32(0)
	}

	return uint32(value)
}

func (w *catalogWriter) flush(context.Context) error {
	return w.blockWriter().close()
}

func (w *catalogWriter) abort() error {
	return w.blockWriter().abort()
}

func (w *catalogWriter) sendFullBatchIfFull() error {
	return w.blockWriter().sendIfFull()
}

func (w *catalogWriter) blockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertDirsQuery,
		name:        "directory catalog",
		batch:       &w.batch,
		openedAt:    &w.openedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.batchSize,
		notPrepared: errCatalogBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *catalogWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}
