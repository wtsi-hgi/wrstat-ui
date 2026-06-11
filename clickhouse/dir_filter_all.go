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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const insertDirFilterAllQuery = "INSERT INTO wrstat_dir_filter_all " +
	"(mount_path, snapshot_id, age, gid, uid, ft, dir, parent_dir, count, size, " +
	"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, " +
	"child_count, has_filter_children, has_children, refreshed_at) " +
	"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

var errDirFilterAllBatchNotPrepared = errors.New("clickhouse: dir full-filter batch is not prepared")

type filterAllRow struct {
	MountPath         string
	SnapshotID        string
	ParentDir         string
	Dir               string
	Age               db.DirGUTAge
	GID               uint32
	UID               uint32
	FT                db.DirGUTAFileType
	Count             uint64
	Size              uint64
	AtimeMin          int64
	MtimeMax          int64
	AtimeBuckets      []uint64
	MtimeBuckets      []uint64
	FilterChildCount  uint64
	ChildCount        uint64
	HasFilterChildren uint8
	HasChildren       uint8
	RefreshedAt       time.Time
}

func fullFilterRowForGUTA(
	mount activeMount,
	dir string,
	guta *db.GUTA,
	childCount uint64,
	refreshedAt time.Time,
) filterAllRow {
	return filterAllRow{
		MountPath:    mount.mountPath,
		SnapshotID:   mount.snapshotID,
		ParentDir:    parentFactsParentDir(dir),
		Dir:          dir,
		Age:          guta.Age,
		GID:          guta.GID,
		UID:          guta.UID,
		FT:           guta.FT,
		Count:        guta.Count,
		Size:         guta.Size,
		AtimeMin:     guta.Atime,
		MtimeMax:     guta.Mtime,
		AtimeBuckets: ageBucketsSlice(&guta.ATimeRanges),
		MtimeBuckets: ageBucketsSlice(&guta.MTimeRanges),
		ChildCount:   childCount,
		HasChildren:  fullFilterHasChildrenValue(childCount),
		RefreshedAt:  refreshedAt,
	}
}

type fullFilterTupleKey struct {
	age db.DirGUTAge
	gid uint32
	uid uint32
	ft  db.DirGUTAFileType
}

func fullFilterKeyForRow(row filterAllRow) fullFilterTupleKey {
	return fullFilterTupleKey{
		age: row.Age,
		gid: row.GID,
		uid: row.UID,
		ft:  row.FT,
	}
}

type fullFilterChildTupleKey struct {
	childDir string
	tuple    fullFilterTupleKey
}

type fullFilterPendingDir struct {
	dir       string
	rows      []filterAllRow
	seenChild map[fullFilterChildTupleKey]struct{}
}

type dirFilterAllWriter struct {
	conn ch.Conn

	batch    driver.Batch
	openedAt time.Time
	batchNow func() time.Time

	batchSize int
	writeErr  error
}

func newDirFilterAllWriter(conn ch.Conn, batchSize int) *dirFilterAllWriter {
	return &dirFilterAllWriter{
		conn:      conn,
		batchSize: batchSize,
	}
}

func (w *dirFilterAllWriter) appendRow(ctx context.Context, row filterAllRow) error {
	return w.blockWriter().append(ctx, func(batch driver.Batch) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			uint8(row.Age),
			row.GID,
			row.UID,
			uint16(row.FT),
			row.Dir,
			row.ParentDir,
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

func (w *dirFilterAllWriter) flush(context.Context) error {
	return w.blockWriter().close()
}

func (w *dirFilterAllWriter) abort() error {
	return w.blockWriter().abort()
}

func (w *dirFilterAllWriter) blockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertDirFilterAllQuery,
		name:        "dir full-filter",
		batch:       &w.batch,
		openedAt:    &w.openedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.batchSize,
		notPrepared: errDirFilterAllBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *dirFilterAllWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}

type fullFilterAllWriter struct {
	dirWriter   *dirFilterAllWriter
	childWriter *childFilterAllWriter

	refreshedAt time.Time
	pending     []fullFilterPendingDir
}

func newFullFilterAllWriter(conn ch.Conn, batchSize int, refreshedAt time.Time) *fullFilterAllWriter {
	if refreshedAt.IsZero() {
		refreshedAt = time.Now().UTC()
	}

	return &fullFilterAllWriter{
		dirWriter:   newDirFilterAllWriter(conn, batchSize),
		childWriter: newChildFilterAllWriter(conn, batchSize),
		refreshedAt: refreshedAt,
	}
}

func (w *fullFilterAllWriter) appendRecord(
	ctx context.Context,
	mount activeMount,
	dir string,
	gutas db.GUTAs,
	_ []string,
	childCount uint64,
	_ []db.DirGUTAge,
) error {
	if err := w.flushCompleted(ctx, dir); err != nil {
		return err
	}

	tuples := fullFilterTupleKeys(gutas)
	w.noteDirectChildTuples(parentFactsParentDir(dir), dir, tuples)

	rows := fullFilterRowsForGUTAs(mount, dir, gutas, childCount, w.refreshedAt)
	if len(rows) == 0 {
		return nil
	}

	w.pending = append(w.pending, fullFilterPendingDir{
		dir:       ensureTrailingSlash(dir),
		rows:      rows,
		seenChild: make(map[fullFilterChildTupleKey]struct{}),
	})

	return nil
}

func fullFilterTupleKeys(gutas db.GUTAs) map[fullFilterTupleKey]struct{} {
	keys := make(map[fullFilterTupleKey]struct{}, len(gutas))
	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		keys[fullFilterTupleKey{
			age: guta.Age,
			gid: guta.GID,
			uid: guta.UID,
			ft:  guta.FT,
		}] = struct{}{}
	}

	return keys
}

func fullFilterRowsForGUTAs(
	mount activeMount,
	dir string,
	gutas db.GUTAs,
	childCount uint64,
	refreshedAt time.Time,
) []filterAllRow {
	rows := make([]filterAllRow, 0, len(gutas))
	dir = ensureTrailingSlash(dir)

	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		rows = append(rows, fullFilterRowForGUTA(mount, dir, guta, childCount, refreshedAt))
	}

	return rows
}

func (w *fullFilterAllWriter) noteDirectChildTuples(
	parentDir string,
	childDir string,
	tuples map[fullFilterTupleKey]struct{},
) {
	if len(tuples) == 0 {
		return
	}

	parentDir = ensureTrailingSlash(parentDir)
	childDir = ensureTrailingSlash(childDir)

	for pendingIdx := range w.pending {
		if w.pending[pendingIdx].dir != parentDir {
			continue
		}

		notePendingDirectChildTuples(&w.pending[pendingIdx], childDir, tuples)
	}
}

func notePendingDirectChildTuples(
	pending *fullFilterPendingDir,
	childDir string,
	tuples map[fullFilterTupleKey]struct{},
) {
	for rowIdx := range pending.rows {
		tuple := fullFilterKeyForRow(pending.rows[rowIdx])
		if _, ok := tuples[tuple]; !ok {
			continue
		}

		childKey := fullFilterChildTupleKey{childDir: childDir, tuple: tuple}
		if _, seen := pending.seenChild[childKey]; seen {
			continue
		}

		pending.seenChild[childKey] = struct{}{}
		pending.rows[rowIdx].FilterChildCount++
	}
}

func (w *fullFilterAllWriter) flushCompleted(ctx context.Context, currentDir string) error {
	currentDir = ensureTrailingSlash(currentDir)

	for len(w.pending) > 0 && !fullFilterAncestorOrSame(w.pending[len(w.pending)-1].dir, currentDir) {
		if err := w.flushLastPending(ctx); err != nil {
			return err
		}
	}

	return nil
}

func fullFilterAncestorOrSame(ancestor, dir string) bool {
	ancestor = ensureTrailingSlash(ancestor)
	dir = ensureTrailingSlash(dir)

	return ancestor == dir || ancestor == "/" || strings.HasPrefix(dir, ancestor)
}

func (w *fullFilterAllWriter) flushLastPending(ctx context.Context) error {
	lastIdx := len(w.pending) - 1
	pending := w.pending[lastIdx]
	w.pending = w.pending[:lastIdx]

	for _, row := range pending.rows {
		row.HasFilterChildren = fullFilterHasChildrenValue(row.FilterChildCount)

		if err := w.dirWriter.appendRow(ctx, row); err != nil {
			return err
		}

		if err := w.childWriter.appendRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func fullFilterHasChildrenValue(childCount uint64) uint8 {
	if childCount > 0 {
		return 1
	}

	return 0
}

func (w *fullFilterAllWriter) flush(ctx context.Context) error {
	for len(w.pending) > 0 {
		if err := w.flushLastPending(ctx); err != nil {
			return err
		}
	}

	return errors.Join(w.dirWriter.flush(ctx), w.childWriter.flush(ctx))
}

func (w *fullFilterAllWriter) abort() error {
	w.pending = nil

	return errors.Join(w.dirWriter.abort(), w.childWriter.abort())
}

func (w *fullFilterAllWriter) importPhase() string {
	return importPhaseFullFilterAllInsert
}
