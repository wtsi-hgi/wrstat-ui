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
	"io"
	"path/filepath"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const insertFilesBatchQuery = "INSERT INTO wrstat_files " +
	"(mount_path, snapshot_id, parent_dir, name, ext, entry_type, size, apparent_size, " +
	"uid, gid, atime, mtime, ctime, inode, nlink)"

type fileIngestWriter struct {
	cfg Config

	conn ch.Conn

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	prepared bool
	batch    driver.Batch
	buf      fileIngestBuffer

	batchSize int

	closed bool
}

type fileIngestBuffer struct {
	mountPath    []string
	snapshot     []uuid.UUID
	parentDir    []string
	name         []string
	ext          []string
	entryType    []uint8
	size         []uint64
	apparentSize []uint64
	uid          []uint32
	gid          []uint32
	atime        []time.Time
	mtime        []time.Time
	ctime        []time.Time
	inode        []uint64
	nlink        []uint64
}

func (b *fileIngestBuffer) rows() int {
	return len(b.name)
}

func (b *fileIngestBuffer) reset() {
	b.mountPath = b.mountPath[:0]
	b.snapshot = b.snapshot[:0]
	b.parentDir = b.parentDir[:0]
	b.name = b.name[:0]
	b.ext = b.ext[:0]
	b.entryType = b.entryType[:0]
	b.size = b.size[:0]
	b.apparentSize = b.apparentSize[:0]
	b.uid = b.uid[:0]
	b.gid = b.gid[:0]
	b.atime = b.atime[:0]
	b.mtime = b.mtime[:0]
	b.ctime = b.ctime[:0]
	b.inode = b.inode[:0]
	b.nlink = b.nlink[:0]
}

func (b *fileIngestBuffer) appendRow(
	mountPath string,
	snapshot uuid.UUID,
	parentDir string,
	name string,
	ext string,
	entryType uint8,
	size uint64,
	apparentSize uint64,
	uid uint32,
	gid uint32,
	atime time.Time,
	mtime time.Time,
	ctime time.Time,
	inode uint64,
	nlink uint64,
) {
	b.mountPath = append(b.mountPath, mountPath)
	b.snapshot = append(b.snapshot, snapshot)
	b.parentDir = append(b.parentDir, parentDir)
	b.name = append(b.name, name)
	b.ext = append(b.ext, ext)
	b.entryType = append(b.entryType, entryType)
	b.size = append(b.size, size)
	b.apparentSize = append(b.apparentSize, apparentSize)
	b.uid = append(b.uid, uid)
	b.gid = append(b.gid, gid)
	b.atime = append(b.atime, atime)
	b.mtime = append(b.mtime, mtime)
	b.ctime = append(b.ctime, ctime)
	b.inode = append(b.inode, inode)
	b.nlink = append(b.nlink, nlink)
}

type fileIngestOperation struct {
	w *fileIngestWriter
}

func (o *fileIngestOperation) Add(info *summary.FileInfo) error {
	if o == nil || o.w == nil {
		return errClientClosed
	}

	return o.w.append(info)
}

func (o *fileIngestOperation) Output() error {
	// Global operation output is a no-op; flushing happens in Close() per spec.
	return nil
}

func (w *fileIngestWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	var out error
	if w.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
		out = errors.Join(out, w.flushBuffer(ctx, false))
		cancel()

		w.batch = nil
		out = errors.Join(out, w.conn.Close())
		w.conn = nil
	}

	if out == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to close file ingest operation: %w", out)
}

func (w *fileIngestWriter) append(info *summary.FileInfo) error {
	if w == nil || w.conn == nil {
		return errClientClosed
	}
	if info == nil {
		return nil
	}
	if w.mountPath == "" {
		return errMountPathRequired
	}
	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}
	if info.Path == nil {
		return fmt.Errorf("clickhouse: file ingest requires directory path")
	}
	if len(info.Name) == 0 {
		return fmt.Errorf("clickhouse: file ingest requires entry name")
	}
	if info.Size < 0 || info.ApparentSize < 0 {
		return fmt.Errorf("clickhouse: file ingest requires non-negative sizes")
	}
	if info.Inode < 0 || info.Nlink < 0 {
		return fmt.Errorf("clickhouse: file ingest requires non-negative inode and nlink")
	}

	parentDir := string(info.Path.AppendTo(make([]byte, 0, info.Path.Len())))
	name := string(info.Name)
	ext := extFromName(name)

	// We can buffer without touching ClickHouse until a flush boundary.
	w.buf.appendRow(
		w.mountPath,
		w.snapshot,
		parentDir,
		name,
		ext,
		uint8(info.EntryType),
		uint64(info.Size),
		uint64(info.ApparentSize),
		info.UID,
		info.GID,
		time.Unix(info.ATime, 0),
		time.Unix(info.MTime, 0),
		time.Unix(info.CTime, 0),
		uint64(info.Inode),
		uint64(info.Nlink),
	)

	if w.buf.rows() < w.batchSize {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
	defer cancel()

	return w.flushBuffer(ctx, true)
}

func (w *fileIngestWriter) flushBuffer(ctx context.Context, reprepare bool) error {
	if w == nil || w.conn == nil {
		return errClientClosed
	}
	if w.buf.rows() == 0 {
		return nil
	}

	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	// Spec-required columnar append: Append(slice) per column.
	if err := w.batch.Column(0).Append(w.buf.mountPath); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 0: %w", err)
	}
	if err := w.batch.Column(1).Append(w.buf.snapshot); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 1: %w", err)
	}
	if err := w.batch.Column(2).Append(w.buf.parentDir); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 2: %w", err)
	}
	if err := w.batch.Column(3).Append(w.buf.name); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 3: %w", err)
	}
	if err := w.batch.Column(4).Append(w.buf.ext); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 4: %w", err)
	}
	if err := w.batch.Column(5).Append(w.buf.entryType); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 5: %w", err)
	}
	if err := w.batch.Column(6).Append(w.buf.size); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 6: %w", err)
	}
	if err := w.batch.Column(7).Append(w.buf.apparentSize); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 7: %w", err)
	}
	if err := w.batch.Column(8).Append(w.buf.uid); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 8: %w", err)
	}
	if err := w.batch.Column(9).Append(w.buf.gid); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 9: %w", err)
	}
	if err := w.batch.Column(10).Append(w.buf.atime); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 10: %w", err)
	}
	if err := w.batch.Column(11).Append(w.buf.mtime); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 11: %w", err)
	}
	if err := w.batch.Column(12).Append(w.buf.ctime); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 12: %w", err)
	}
	if err := w.batch.Column(13).Append(w.buf.inode); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 13: %w", err)
	}
	if err := w.batch.Column(14).Append(w.buf.nlink); err != nil {
		return fmt.Errorf("clickhouse: failed to append files column 14: %w", err)
	}

	if err := w.batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send files batch: %w", err)
	}

	w.buf.reset()

	if !reprepare {
		return nil
	}

	newBatch, err := w.conn.PrepareBatch(context.WithoutCancel(ctx), insertFilesBatchQuery, driver.WithReleaseConnection())
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare replacement files batch: %w", err)
	}
	w.batch = newBatch

	return nil
}

func (w *fileIngestWriter) ensureWriteReady(ctx context.Context) error {
	if w.prepared {
		return nil
	}

	if w.snapshot == uuid.Nil {
		w.snapshot = snapshotID(w.mountPath, w.updatedAt)
	}

	if err := dropPartitionIgnoreUnknown(ctx, w.conn, w.mountPath, w.snapshot.String(), dropFilesPartitionQuery); err != nil {
		return err
	}

	batchCtx := context.WithoutCancel(ctx)
	batch, err := w.conn.PrepareBatch(batchCtx, insertFilesBatchQuery, driver.WithReleaseConnection())
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare files batch: %w", err)
	}

	w.batch = batch
	w.prepared = true

	return nil
}

func extFromName(name string) string {
	// Directories include a trailing '/', and we don't store extensions for them.
	if strings.HasSuffix(name, "/") {
		return ""
	}

	// Spec semantics: ext is the portion after the last '.', lowercased.
	// If there's no '.', or the name begins with '.' and has no other '.', ext is empty.
	idx := strings.LastIndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return ""
	}

	return strings.ToLower(filepath.Base(name[idx+1:]))
}

// NewFileIngestOperation returns a summary global operation and a closer that
// streams file-level rows into wrstat_files.
func NewFileIngestOperation(
	cfg Config,
	mountPath string,
	updatedAt time.Time,
) (summary.OperationGenerator, io.Closer, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg))
	defer cancel()

	conn, err := connectAndBootstrap(ctx, opts, cfg.Database)
	if err != nil {
		return nil, nil, err
	}

	w := &fileIngestWriter{
		cfg:       cfg,
		conn:      conn,
		mountPath: mountPath,
		updatedAt: updatedAt,
		snapshot:  snapshotID(mountPath, updatedAt),
		batchSize: defaultBatchSize,
	}

	gen := func() summary.Operation {
		return &fileIngestOperation{w: w}
	}

	return gen, w, nil
}
