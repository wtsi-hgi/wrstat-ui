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
	errFileIngestNoDirPath     = errors.New("clickhouse: file ingest requires directory path")
	errFileIngestNoName        = errors.New("clickhouse: file ingest requires entry name")
	errFileIngestNegativeSize  = errors.New("clickhouse: file ingest requires non-negative sizes")
	errFileIngestNegativeInode = errors.New("clickhouse: file ingest requires non-negative inode and nlink")
)

type fileIngestRow struct {
	mountPath    string
	snapshot     uuid.UUID
	parentDir    string
	name         string
	ext          string
	entryType    uint8
	size         uint64
	apparentSize uint64
	uid          uint32
	gid          uint32
	atime        time.Time
	mtime        time.Time
	ctime        time.Time
	inode        uint64
	nlink        uint64
}

func fileIngestRowFromInfo(
	mountPath string,
	snapshot uuid.UUID,
	parentDir string,
	name string,
	info *summary.FileInfo,
) (fileIngestRow, error) {
	size, apparentSize, inode, nlink, err := unsignedFileInfoValues(info)
	if err != nil {
		return fileIngestRow{}, err
	}

	return fileIngestRow{
		mountPath:    mountPath,
		snapshot:     snapshot,
		parentDir:    parentDir,
		name:         name,
		ext:          extFromName(name),
		entryType:    info.EntryType,
		size:         size,
		apparentSize: apparentSize,
		uid:          info.UID,
		gid:          info.GID,
		atime:        time.Unix(info.ATime, 0),
		mtime:        time.Unix(info.MTime, 0),
		ctime:        time.Unix(info.CTime, 0),
		inode:        inode,
		nlink:        nlink,
	}, nil
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

func (b *fileIngestBuffer) appendRow(row fileIngestRow) {
	b.mountPath = append(b.mountPath, row.mountPath)
	b.snapshot = append(b.snapshot, row.snapshot)
	b.parentDir = append(b.parentDir, row.parentDir)
	b.name = append(b.name, row.name)
	b.ext = append(b.ext, row.ext)
	b.entryType = append(b.entryType, row.entryType)
	b.size = append(b.size, row.size)
	b.apparentSize = append(b.apparentSize, row.apparentSize)
	b.uid = append(b.uid, row.uid)
	b.gid = append(b.gid, row.gid)
	b.atime = append(b.atime, row.atime)
	b.mtime = append(b.mtime, row.mtime)
	b.ctime = append(b.ctime, row.ctime)
	b.inode = append(b.inode, row.inode)
	b.nlink = append(b.nlink, row.nlink)
}

func (b *fileIngestBuffer) columns() []any {
	return []any{
		b.mountPath,
		b.snapshot,
		b.parentDir,
		b.name,
		b.ext,
		b.entryType,
		b.size,
		b.apparentSize,
		b.uid,
		b.gid,
		b.atime,
		b.mtime,
		b.ctime,
		b.inode,
		b.nlink,
	}
}

type fileIngestWriter struct {
	cfg Config

	conn ch.Conn

	mountPath   string
	updatedAt   time.Time
	snapshot    uuid.UUID
	idAllocator *summary.DirIDAllocator

	prepared bool
	batch    driver.Batch
	buf      fileIngestBuffer
	sendErr  error

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
	if w == nil {
		return
	}

	recordImportPhase(w.importPhaseRecorder, phase, d)
}

func (w *fileIngestWriter) timeImportPhase(phase string, fn func() error) error {
	return timeImportPhase(w.recordImportPhase, phase, fn)
}

func (w *fileIngestWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	var out error

	if w.conn != nil {
		ctx, cancel := configQueryContext(w.cfg)
		if w.sendErr == nil {
			out = errors.Join(out, w.flushBuffer(ctx))
		}

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

	keep, err := w.prepareAppend(info)
	if err != nil {
		return err
	}

	if !keep {
		return nil
	}

	return w.flushIfBatchFull()
}

func (w *fileIngestWriter) prepareAppend(info *summary.FileInfo) (bool, error) {
	if info == nil {
		return false, nil
	}

	if err := w.validateWriteState(); err != nil {
		return false, err
	}

	if err := validateFileInfo(info); err != nil {
		return false, err
	}

	return w.bufferFileInfo(info)
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

func (w *fileIngestWriter) flushIfBatchFull() error {
	if w.buf.rows() < w.batchSize {
		return nil
	}

	ctx, cancel := configQueryContext(w.cfg)
	defer cancel()

	return w.flushBuffer(ctx)
}

func (w *fileIngestWriter) bufferFileInfo(info *summary.FileInfo) (bool, error) {
	parentDir, name := fileIngestParentAndName(info)

	parentDir, name, keep := canonicalFileIngestPath(w.mountPath, parentDir, name)
	if !keep {
		return false, nil
	}

	if _, err := w.fileInfoDirID(info); err != nil {
		return false, err
	}

	row, err := fileIngestRowFromInfo(w.mountPath, w.snapshot, parentDir, name, info)
	if err != nil {
		return false, err
	}

	w.buf.appendRow(row)

	return true, nil
}

func fileIngestParentAndName(info *summary.FileInfo) (string, string) {
	parentDir := string(info.Path.AppendTo(make([]byte, 0, info.Path.Len())))

	return parentDir, string(info.Name)
}

func canonicalFileIngestPath(mountPath, parentDir, name string) (string, string, bool) {
	parentDir = canonicalPathForMount(mountPath, parentDir)

	selfNamedDir := strings.HasSuffix(name, "/") && strings.HasSuffix(parentDir, name)
	if !selfNamedDir {
		return parentDir, name, true
	}

	if parentDir == "/" && name == "/" {
		return "", "", false
	}

	return strings.TrimSuffix(parentDir, name), name, true
}

func (w *fileIngestWriter) fileInfoDirID(info *summary.FileInfo) (uint32, error) {
	if w.idAllocator == nil {
		return 0, nil
	}

	return w.idAllocator.DirID(fileIngestDirIDPath(info))
}

func fileIngestDirIDPath(info *summary.FileInfo) *summary.DirectoryPath {
	if info.IsDir() {
		return info.Path.Parent
	}

	return info.Path
}

func (w *fileIngestWriter) ensureSnapshotID() {
	if w.snapshot != uuid.Nil {
		return
	}

	w.snapshot = snapshotID(w.mountPath, w.updatedAt)
}

func (w *fileIngestWriter) validateWriteState() error {
	if w.sendErr != nil {
		return w.sendErr
	}

	if w.mountPath == "" {
		return errMountPathRequired
	}

	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	return nil
}

func (w *fileIngestWriter) flushBuffer(ctx context.Context) error {
	if w == nil || w.conn == nil {
		return errClientClosed
	}

	if w.buf.rows() == 0 {
		return nil
	}

	return w.sendBufferedData(ctx)
}

func (w *fileIngestWriter) sendBufferedData(ctx context.Context) error {
	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	if err := w.prepareFilesBatch(ctx); err != nil {
		return err
	}

	if err := w.appendColumnarData(); err != nil {
		return err
	}

	if err := w.batch.Send(); err != nil {
		w.sendErr = fmt.Errorf(
			"clickhouse: failed to send files batch: %w", err,
		)
		w.batch = nil

		return w.sendErr
	}

	w.buf.reset()
	w.batch = nil

	return nil
}

func (w *fileIngestWriter) appendColumnarData() error {
	for i, col := range w.buf.columns() {
		if err := w.batch.Column(i).Append(col); err != nil {
			return fmt.Errorf(
				"clickhouse: failed to append files column %d: %w",
				i, err,
			)
		}
	}

	return nil
}

func (w *fileIngestWriter) ensureWriteReady(ctx context.Context) error {
	if w.prepared {
		return nil
	}

	w.ensureSnapshotID()

	if err := refuseActiveSnapshotRewrite(ctx, w.conn, w.mountPath, w.snapshot); err != nil {
		return err
	}

	if err := w.timeImportPhase(importPhasePartitionDropReset, func() error {
		return dropPartitionIgnoreUnknown(
			ctx, w.conn, w.mountPath,
			w.snapshot.String(), dropFilesPartitionQuery,
		)
	}); err != nil {
		return err
	}

	return nil
}

func (w *fileIngestWriter) prepareFilesBatch(ctx context.Context) error {
	if w.batch != nil {
		return nil
	}

	batch, err := prepareImportBatch(ctx, w.conn, insertFilesBatchQuery)
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
	alloc ...*summary.DirIDAllocator,
) (summary.OperationGenerator, io.Closer, error) {
	conn, err := connectForFileIngest(cfg)
	if err != nil {
		return nil, nil, err
	}

	w := &fileIngestWriter{
		cfg:         cfg,
		conn:        conn,
		mountPath:   mountPath,
		updatedAt:   updatedAt,
		snapshot:    snapshotID(mountPath, updatedAt),
		idAllocator: optionalFileIngestDirIDAllocator(alloc),
		batchSize:   defaultBatchSize,
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

	return connectForImportFromConfig(cfg)
}

func optionalFileIngestDirIDAllocator(alloc []*summary.DirIDAllocator) *summary.DirIDAllocator {
	if len(alloc) == 0 {
		return nil
	}

	return alloc[0]
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

func unsignedFileInfoValues(info *summary.FileInfo) (uint64, uint64, uint64, uint64, error) {
	size, err := nonNegativeInt64ToUint64(info.Size, errFileIngestNegativeSize)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	apparentSize, err := nonNegativeInt64ToUint64(info.ApparentSize, errFileIngestNegativeSize)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	inode, err := nonNegativeInt64ToUint64(info.Inode, errFileIngestNegativeInode)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	nlink, err := nonNegativeInt64ToUint64(info.Nlink, errFileIngestNegativeInode)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	return size, apparentSize, inode, nlink, nil
}

func nonNegativeInt64ToUint64(v int64, negativeErr error) (uint64, error) {
	if v < 0 {
		return 0, negativeErr
	}

	return uint64(v), nil
}

func canonicalPathForMount(mountPath, path string) string {
	if mountPath != "/" || path == "" {
		return path
	}

	trimmed := strings.TrimLeft(path, "/")
	if trimmed == "" {
		return "/"
	}

	return "/" + trimmed
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

	return strings.ToLower(name[idx+1:])
}
