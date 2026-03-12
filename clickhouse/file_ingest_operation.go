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

var (
	errFileIngestNoDirPath = errors.New(
		"clickhouse: file ingest requires directory path",
	)
	errFileIngestNoName = errors.New(
		"clickhouse: file ingest requires entry name",
	)
	errFileIngestNegativeSize = errors.New(
		"clickhouse: file ingest requires non-negative sizes",
	)
	errFileIngestNegativeInode = errors.New(
		"clickhouse: file ingest requires non-negative inode and nlink",
	)
)

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

func (b *fileIngestBuffer) appendRow( //nolint:funlen
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

	importPhaseRecorder func(string, time.Duration)

	closed bool
}

func (w *fileIngestWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
	}
}

func (w *fileIngestWriter) SetImportPhaseRecorder(recorder func(string, time.Duration)) {
	w.importPhaseRecorder = recorder
}

func (w *fileIngestWriter) recordImportPhase(phase string, d time.Duration) {
	if w == nil || w.importPhaseRecorder == nil || d <= 0 {
		return
	}

	w.importPhaseRecorder(phase, d)
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

	if err := w.validateWriteState(); err != nil {
		return err
	}

	if err := validateFileInfo(info); err != nil {
		return err
	}

	w.bufferFileInfo(info)

	if w.buf.rows() < w.batchSize {
		return nil
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), queryTimeout(w.cfg),
	)
	defer cancel()

	return w.flushBuffer(ctx, true)
}

func validateFileInfo(info *summary.FileInfo) error {
	if info.Path == nil {
		return errFileIngestNoDirPath
	}

	if len(info.Name) == 0 {
		return errFileIngestNoName
	}

	if info.Size < 0 || info.ApparentSize < 0 {
		return errFileIngestNegativeSize
	}

	if info.Inode < 0 || info.Nlink < 0 {
		return errFileIngestNegativeInode
	}

	return nil
}

func (w *fileIngestWriter) bufferFileInfo(info *summary.FileInfo) {
	parentDir := string(info.Path.AppendTo(
		make([]byte, 0, info.Path.Len()),
	))
	name := string(info.Name)

	w.buf.appendRow(
		w.mountPath,
		w.snapshot,
		parentDir,
		name,
		extFromName(name),
		info.EntryType,
		uint64(info.Size),         //nolint:gosec
		uint64(info.ApparentSize), //nolint:gosec
		info.UID,
		info.GID,
		time.Unix(info.ATime, 0),
		time.Unix(info.MTime, 0),
		time.Unix(info.CTime, 0),
		uint64(info.Inode), //nolint:gosec
		uint64(info.Nlink), //nolint:gosec
	)
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

func (w *fileIngestWriter) validateWriteState() error {
	if w.mountPath == "" {
		return errMountPathRequired
	}

	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	return nil
}

func (w *fileIngestWriter) flushBuffer(
	ctx context.Context,
	reprepare bool,
) error {
	if w == nil || w.conn == nil {
		return errClientClosed
	}

	if w.buf.rows() == 0 {
		return nil
	}

	if err := w.sendBufferedData(ctx); err != nil {
		return err
	}

	if !reprepare {
		return nil
	}

	return w.reprepareFilesBatch(ctx)
}

func (w *fileIngestWriter) sendBufferedData(
	ctx context.Context,
) error {
	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	if err := w.appendColumnarData(); err != nil {
		return err
	}

	if err := w.batch.Send(); err != nil {
		return fmt.Errorf(
			"clickhouse: failed to send files batch: %w", err,
		)
	}

	w.buf.reset()

	return nil
}

func (w *fileIngestWriter) reprepareFilesBatch(
	ctx context.Context,
) error {
	newBatch, err := w.conn.PrepareBatch(
		context.WithoutCancel(ctx),
		insertFilesBatchQuery,
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf(
			"clickhouse: failed to prepare replacement files batch: %w",
			err,
		)
	}

	w.batch = newBatch

	return nil
}

func (w *fileIngestWriter) appendColumnarData() error {
	columns := []any{
		w.buf.mountPath,
		w.buf.snapshot,
		w.buf.parentDir,
		w.buf.name,
		w.buf.ext,
		w.buf.entryType,
		w.buf.size,
		w.buf.apparentSize,
		w.buf.uid,
		w.buf.gid,
		w.buf.atime,
		w.buf.mtime,
		w.buf.ctime,
		w.buf.inode,
		w.buf.nlink,
	}

	for i, col := range columns {
		if err := w.batch.Column(i).Append(col); err != nil {
			return fmt.Errorf(
				"clickhouse: failed to append files column %d: %w",
				i, err,
			)
		}
	}

	return nil
}

func (w *fileIngestWriter) ensureWriteReady(
	ctx context.Context,
) error {
	if w.prepared {
		return nil
	}

	if w.snapshot == uuid.Nil {
		w.snapshot = snapshotID(w.mountPath, w.updatedAt)
	}

	if err := refuseActiveSnapshotRewrite(ctx, w.conn, w.mountPath, w.snapshot); err != nil {
		return err
	}

	dropStart := time.Now()
	err := dropPartitionIgnoreUnknown(
		ctx, w.conn, w.mountPath,
		w.snapshot.String(), dropFilesPartitionQuery,
	)
	w.recordImportPhase(importPhasePartitionDropReset, time.Since(dropStart))

	if err != nil {
		return err
	}

	return w.prepareFilesBatch(ctx)
}

func (w *fileIngestWriter) prepareFilesBatch(
	ctx context.Context,
) error {
	batchCtx := context.WithoutCancel(ctx)

	batch, err := w.conn.PrepareBatch(
		batchCtx, insertFilesBatchQuery,
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf(
			"clickhouse: failed to prepare files batch: %w", err,
		)
	}

	w.batch = batch
	w.prepared = true

	return nil
}

// NewFileIngestOperation returns a summary global operation and a closer that
// streams file-level rows into wrstat_files.
func NewFileIngestOperation(
	cfg Config,
	mountPath string,
	updatedAt time.Time,
) (summary.OperationGenerator, io.Closer, error) {
	conn, err := connectForFileIngest(cfg)
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

func connectForFileIngest(cfg Config) (ch.Conn, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
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
