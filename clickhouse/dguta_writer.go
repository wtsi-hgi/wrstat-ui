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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	defaultBatchSize = 100_000

	activeSnapshotQuery = "SELECT toString(snapshot_id), updated_at FROM wrstat_mounts_active " +
		"WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) " +
		"VALUES (?, now64(3), toUUID(?), ?)"

	dropDGUTAPartitionQuery    = "ALTER TABLE wrstat_dguta DROP PARTITION tuple(?, toUUID(?))"
	dropChildrenPartitionQuery = "ALTER TABLE wrstat_children DROP PARTITION tuple(?, toUUID(?))"

	insertDGUTAQuery = "INSERT INTO wrstat_dguta " +
		"(mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertChildrenQuery = "INSERT INTO wrstat_children " +
		"(mount_path, snapshot_id, parent_dir, child) " +
		"VALUES (?, toUUID(?), ?, ?)"
)

var (
	errMountPathRequired = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired = errors.New("clickhouse: updated at is required")
	errDirRequired       = errors.New("clickhouse: record dir is required")
)

type dgutaWriter struct {
	cfg Config

	conn ch.Conn

	batchSize int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	prepared bool

	dgutaBatch    driver.Batch
	childrenBatch driver.Batch

	closed bool
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
	}
}

func (w *dgutaWriter) SetMountPath(mountPath string) {
	w.mountPath = mountPath
}

func (w *dgutaWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
}

func (w *dgutaWriter) Add(dguta db.RecordDGUTA) error {
	if err := w.validateAdd(dguta); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
	defer cancel()

	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	parentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))

	if err := w.appendDGUTARows(dguta, parentDir); err != nil {
		return err
	}

	if err := w.appendChildrenRows(dguta.Children, parentDir); err != nil {
		return err
	}

	return w.flushFullBatches(ctx)
}

func (w *dgutaWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	if w.conn == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
	defer cancel()

	if err := w.flushAllBatches(); err != nil {
		_ = w.conn.Close()

		return err
	}

	if w.shouldSwitchSnapshot() {
		if err := w.switchActiveSnapshot(ctx); err != nil {
			_ = w.conn.Close()

			return err
		}
	}

	return w.conn.Close()
}

func (w *dgutaWriter) validateAdd(dguta db.RecordDGUTA) error {
	if w.mountPath == "" {
		return errMountPathRequired
	}

	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	if dguta.Dir == nil {
		return errDirRequired
	}

	return nil
}

func (w *dgutaWriter) shouldSwitchSnapshot() bool {
	return w.mountPath != "" && !w.updatedAt.IsZero()
}

func (w *dgutaWriter) ensureSnapshotID() {
	if w.snapshot != uuid.Nil {
		return
	}

	w.snapshot = snapshotID(w.mountPath, w.updatedAt)
}

func (w *dgutaWriter) switchActiveSnapshot(ctx context.Context) error {
	w.ensureSnapshotID()

	// Read previous snapshot (required by spec). We don't use it yet.
	rows, err := w.conn.Query(ctx, activeSnapshotQuery, w.mountPath)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to read active snapshot: %w", err)
	}

	_ = rows.Close()

	if err := w.conn.Exec(ctx, switchSnapshotQuery, w.mountPath, w.snapshot.String(), w.updatedAt); err != nil {
		return fmt.Errorf("clickhouse: failed to switch active snapshot: %w", err)
	}

	return nil
}

func (w *dgutaWriter) ensureWriteReady(ctx context.Context) error {
	w.ensureSnapshotID()

	if w.prepared {
		return nil
	}

	if err := w.dropNewSnapshotPartitions(ctx); err != nil {
		return err
	}

	batchCtx := context.WithoutCancel(ctx)

	dgutaBatch, childrenBatch, err := w.prepareBatches(batchCtx)
	if err != nil {
		return err
	}

	w.dgutaBatch = dgutaBatch
	w.childrenBatch = childrenBatch
	w.prepared = true

	return nil
}

func (w *dgutaWriter) prepareBatches(ctx context.Context) (driver.Batch, driver.Batch, error) {
	dgutaBatch, err := w.prepareBatch(ctx, insertDGUTAQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: failed to prepare dguta batch: %w", err)
	}

	childrenBatch, err := w.prepareBatch(ctx, insertChildrenQuery)
	if err != nil {
		if abortErr := dgutaBatch.Abort(); abortErr != nil {
			return nil, nil, fmt.Errorf(
				"clickhouse: failed to abort dguta batch after children prepare failed: %w",
				abortErr,
			)
		}

		return nil, nil, fmt.Errorf("clickhouse: failed to prepare children batch: %w", err)
	}

	return dgutaBatch, childrenBatch, nil
}

func (w *dgutaWriter) prepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return w.conn.PrepareBatch(ctx, query, driver.WithReleaseConnection())
}

func (w *dgutaWriter) dropNewSnapshotPartitions(ctx context.Context) error {
	sid := w.snapshot.String()

	if err := w.dropPartition(ctx, dropDGUTAPartitionQuery, sid); err != nil {
		return err
	}

	if err := w.dropPartition(ctx, dropChildrenPartitionQuery, sid); err != nil {
		return err
	}

	return nil
}

func (w *dgutaWriter) dropPartition(ctx context.Context, query, sid string) error {
	err := w.conn.Exec(ctx, query, w.mountPath, sid)
	if err == nil {
		return nil
	}

	var ex *proto.Exception
	if errors.As(err, &ex) {
		// ClickHouse returns UNKNOWN_PARTITION for first-time snapshots.
		if strings.Contains(ex.Message, "UNKNOWN_PARTITION") ||
			strings.Contains(ex.Message, "Unknown partition") {

			return nil
		}
	}

	return fmt.Errorf("clickhouse: failed to drop partition: %w", err)
}

func (w *dgutaWriter) appendDGUTARows(dguta db.RecordDGUTA, parentDir string) error {
	for _, guta := range dguta.GUTAs {
		if guta == nil {
			continue
		}

		err := w.dgutaBatch.Append(
			w.mountPath,
			w.snapshot.String(),
			parentDir,
			guta.GID,
			guta.UID,
			uint16(guta.FT),
			uint8(guta.Age),
			guta.Count,
			guta.Size,
			guta.Atime,
			guta.Mtime,
			guta.ATimeRanges[:],
			guta.MTimeRanges[:],
		)
		if err != nil {
			return fmt.Errorf("clickhouse: failed to append dguta row: %w", err)
		}
	}

	return nil
}

func (w *dgutaWriter) appendChildrenRows(children []string, parentDir string) error {
	for _, child := range children {
		child = strings.TrimSuffix(child, "/")
		if child == "" {
			continue
		}

		if err := w.childrenBatch.Append(w.mountPath, w.snapshot.String(), parentDir, child); err != nil {
			return fmt.Errorf("clickhouse: failed to append child row: %w", err)
		}
	}

	return nil
}

func (w *dgutaWriter) flushFullBatches(ctx context.Context) error {
	if w.dgutaBatch != nil && w.dgutaBatch.Rows() >= w.batchSize {
		if err := w.sendAndReplaceDGUTABatch(ctx); err != nil {
			return err
		}
	}

	if w.childrenBatch != nil && w.childrenBatch.Rows() >= w.batchSize {
		if err := w.sendAndReplaceChildrenBatch(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) flushAllBatches() error {
	if w.dgutaBatch != nil && w.dgutaBatch.Rows() > 0 {
		if err := w.sendAndCloseDGUTABatch(); err != nil {
			return err
		}
	}

	if w.childrenBatch != nil && w.childrenBatch.Rows() > 0 {
		if err := w.sendAndCloseChildrenBatch(); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) sendAndReplaceDGUTABatch(ctx context.Context) error {
	if err := w.dgutaBatch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send dguta batch: %w", err)
	}

	batchCtx := context.WithoutCancel(ctx)

	batch, err := w.conn.PrepareBatch(
		batchCtx,
		insertDGUTAQuery,
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare dguta batch: %w", err)
	}

	w.dgutaBatch = batch

	return nil
}

func (w *dgutaWriter) sendAndReplaceChildrenBatch(ctx context.Context) error {
	if err := w.childrenBatch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send children batch: %w", err)
	}

	batchCtx := context.WithoutCancel(ctx)

	batch, err := w.conn.PrepareBatch(
		batchCtx,
		insertChildrenQuery,
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare children batch: %w", err)
	}

	w.childrenBatch = batch

	return nil
}

func (w *dgutaWriter) sendAndCloseDGUTABatch() error {
	if err := w.dgutaBatch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send dguta batch: %w", err)
	}

	w.dgutaBatch = nil

	return nil
}

func (w *dgutaWriter) sendAndCloseChildrenBatch() error {
	if err := w.childrenBatch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send children batch: %w", err)
	}

	w.childrenBatch = nil

	return nil
}

// NewDGUTAWriter returns a ClickHouse-backed implementation of db.DGUTAWriter.
func NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg))
	defer cancel()

	conn, err := connectAndBootstrap(ctx, opts, cfg.Database)
	if err != nil {
		return nil, err
	}

	return &dgutaWriter{cfg: cfg, conn: conn, batchSize: defaultBatchSize}, nil
}
