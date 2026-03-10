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

	importPhasePartitionDropReset = "partition_drop_reset"
	importPhaseDGUTAInsert        = "wrstat_dguta_insert"
	importPhaseChildrenInsert     = "wrstat_children_insert"
	importPhaseMountSwitch        = "mount_switch"
	importPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"

	activeSnapshotQuery = "SELECT toString(snapshot_id), updated_at FROM wrstat_mounts_active " +
		"WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) " +
		"VALUES (?, now64(3), toUUID(?), ?)"

	dropDGUTAPartitionQuery    = "ALTER TABLE wrstat_dguta DROP PARTITION tuple(?, toUUID(?))"
	dropChildrenPartitionQuery = "ALTER TABLE wrstat_children DROP PARTITION tuple(?, toUUID(?))"
	dropFilesPartitionQuery    = "ALTER TABLE wrstat_files DROP PARTITION tuple(?, toUUID(?))"

	dropBasedirsGroupUsagePartitionQuery   = "ALTER TABLE wrstat_basedirs_group_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserUsagePartitionQuery    = "ALTER TABLE wrstat_basedirs_user_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsGroupSubdirsPartitionQuery = "ALTER TABLE wrstat_basedirs_group_subdirs DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserSubdirsPartitionQuery  = "ALTER TABLE wrstat_basedirs_user_subdirs DROP PARTITION tuple(?, toUUID(?))"

	insertDGUTAQuery = "INSERT INTO wrstat_dguta " +
		"(mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertChildrenQuery = "INSERT INTO wrstat_children " +
		"(mount_path, snapshot_id, parent_dir, child) " +
		"VALUES (?, toUUID(?), ?, ?)"
)

var (
	errMountPathRequired     = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired     = errors.New("clickhouse: updated at is required")
	errDirRequired           = errors.New("clickhouse: record dir is required")
	errActiveSnapshotRewrite = errors.New(
		"clickhouse: refusing to rewrite active snapshot",
	)
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

	importPhaseRecorder func(string, time.Duration)

	// failBeforeSwitchErr forces Close() to fail before switching snapshots.
	// Used only by integration tests.
	failBeforeSwitchErr error

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

func (w *dgutaWriter) SetImportPhaseRecorder(recorder func(string, time.Duration)) {
	w.importPhaseRecorder = recorder
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
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if w.shouldSwitchSnapshot() {
		if err := w.switchSnapshotAndDropOld(ctx); err != nil {
			_ = w.conn.Close()

			return err
		}
	}

	return w.conn.Close()
}

func (w *dgutaWriter) Abort() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	if w.conn == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
	defer cancel()

	return w.closeWithNewSnapshotCleanup(ctx, nil)
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

	if err := w.conn.Exec(ctx, switchSnapshotQuery, w.mountPath, w.snapshot.String(), w.updatedAt); err != nil {
		return fmt.Errorf("clickhouse: failed to switch active snapshot: %w", err)
	}

	return nil
}

func (w *dgutaWriter) switchSnapshotAndDropOld(ctx context.Context) error {
	previousSID, hasPrevious, err := w.readPreviousActiveSnapshotID(ctx)
	if err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.switchSnapshotOrCleanup(ctx); err != nil {
		return err
	}

	if !hasPrevious {
		return nil
	}

	return w.dropPreviousSnapshotPartitions(ctx, previousSID)
}

func (w *dgutaWriter) switchSnapshotOrCleanup(ctx context.Context) error {
	if w.failBeforeSwitchErr != nil {
		return w.closeWithNewSnapshotCleanup(ctx, w.failBeforeSwitchErr)
	}

	switchStart := time.Now()
	err := w.switchActiveSnapshot(ctx)
	w.recordImportPhase(importPhaseMountSwitch, time.Since(switchStart))

	if err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	return nil
}

func (w *dgutaWriter) dropPreviousSnapshotPartitions(ctx context.Context, previousSID string) error {
	// Idempotent retry: if previous snapshot id equals the new snapshot id,
	// do not drop partitions (we would drop the data we just wrote).
	if previousSID == w.snapshot.String() {
		return nil
	}

	dropStart := time.Now()
	err := w.dropAllSnapshotPartitions(ctx, previousSID)
	w.recordImportPhase(importPhaseOldSnapshotDrop, time.Since(dropStart))

	return err
}

func (w *dgutaWriter) readPreviousActiveSnapshotID(ctx context.Context) (string, bool, error) {
	return readActiveSnapshotID(ctx, w.conn, w.mountPath)
}

func readActiveSnapshotID(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
) (string, bool, error) {
	rows, err := conn.Query(ctx, activeSnapshotQuery, mountPath)
	if err != nil {
		return "", false, fmt.Errorf("clickhouse: failed to read active snapshot: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return "", false, nil
	}

	var (
		sid       string
		updatedAt time.Time
	)
	if err := rows.Scan(&sid, &updatedAt); err != nil {
		return "", false, fmt.Errorf("clickhouse: failed to scan active snapshot: %w", err)
	}

	return sid, true, nil
}

func (w *dgutaWriter) closeWithNewSnapshotCleanup(ctx context.Context, cause error) error {
	cause = errors.Join(cause, w.abortAllBatches())

	if !w.shouldSwitchSnapshot() {
		return errors.Join(cause, w.conn.Close())
	}

	w.ensureSnapshotID()

	cleanupCtx, cancel := w.snapshotCleanupContext(ctx)
	defer cancel()

	cleanupErr := w.dropCurrentSnapshotPartitionsIfInactive(cleanupCtx)
	closeErr := w.conn.Close()

	return errors.Join(cause, cleanupErr, closeErr)
}

func (w *dgutaWriter) dropCurrentSnapshotPartitionsIfInactive(ctx context.Context) error {
	activeSID, hasActive, err := w.readPreviousActiveSnapshotID(ctx)
	if err != nil {
		return err
	}

	if hasActive && activeSID == w.snapshot.String() {
		return nil
	}

	return w.dropAllSnapshotPartitions(ctx, w.snapshot.String())
}

func (w *dgutaWriter) snapshotCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}

	return queryContext(context.Background(), queryTimeout(w.cfg))
}

func (w *dgutaWriter) abortAllBatches() error {
	var err error

	err = errors.Join(err, abortBatch(&w.dgutaBatch, "dguta"))
	err = errors.Join(err, abortBatch(&w.childrenBatch, "children"))

	return err
}

func abortBatch(batch *driver.Batch, name string) error {
	if batch == nil || *batch == nil {
		return nil
	}

	err := (*batch).Abort()
	*batch = nil

	if err == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to abort %s batch: %w", name, err)
}

func (w *dgutaWriter) dropAllSnapshotPartitions(ctx context.Context, sid string) error {
	queries := [...]string{
		dropDGUTAPartitionQuery,
		dropChildrenPartitionQuery,
		dropFilesPartitionQuery,
		dropBasedirsGroupUsagePartitionQuery,
		dropBasedirsUserUsagePartitionQuery,
		dropBasedirsGroupSubdirsPartitionQuery,
		dropBasedirsUserSubdirsPartitionQuery,
	}

	for _, query := range queries {
		if err := w.dropPartition(ctx, query, sid); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) ensureWriteReady(ctx context.Context) error {
	w.ensureSnapshotID()

	if w.prepared {
		return nil
	}

	if err := refuseActiveSnapshotRewrite(ctx, w.conn, w.mountPath, w.snapshot); err != nil {
		return err
	}

	if err := w.timeImportPhase(importPhasePartitionDropReset, func() error {
		return w.dropNewSnapshotPartitions(ctx)
	}); err != nil {
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

func refuseActiveSnapshotRewrite(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	snapshot uuid.UUID,
) error {
	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
	if err != nil {
		return err
	}

	if !hasActive || activeSID != snapshot.String() {
		return nil
	}

	return fmt.Errorf(
		"%w: mount_path=%s snapshot_id=%s",
		errActiveSnapshotRewrite,
		mountPath,
		activeSID,
	)
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
		if strings.Contains(ex.Message, "UNKNOWN_PARTITION") || strings.Contains(ex.Message, "Unknown partition") {
			return nil
		}
	}

	return fmt.Errorf("clickhouse: failed to drop partition: %w", err)
}

func (w *dgutaWriter) appendDGUTARows(dguta db.RecordDGUTA, parentDir string) error {
	return w.timeImportPhase(importPhaseDGUTAInsert, func() error {
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
	})
}

func (w *dgutaWriter) appendChildrenRows(children []string, parentDir string) error {
	return w.timeImportPhase(importPhaseChildrenInsert, func() error {
		for _, child := range children {
			child = childPathForParent(parentDir, child)
			if child == "" {
				continue
			}

			if err := w.childrenBatch.Append(w.mountPath, w.snapshot.String(), parentDir, child); err != nil {
				return fmt.Errorf("clickhouse: failed to append child row: %w", err)
			}
		}

		return nil
	})
}

func childPathForParent(parentDir, child string) string {
	child = strings.TrimSuffix(child, "/")
	if child == "" {
		return ""
	}

	if strings.HasPrefix(child, "/") {
		return child
	}

	return parentDir + child
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
	return w.timeImportPhase(importPhaseDGUTAInsert, func() error {
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
	})
}

func (w *dgutaWriter) sendAndReplaceChildrenBatch(ctx context.Context) error {
	return w.timeImportPhase(importPhaseChildrenInsert, func() error {
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
	})
}

func (w *dgutaWriter) sendAndCloseDGUTABatch() error {
	return w.timeImportPhase(importPhaseDGUTAInsert, func() error {
		if err := w.dgutaBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send dguta batch: %w", err)
		}

		w.dgutaBatch = nil

		return nil
	})
}

func (w *dgutaWriter) sendAndCloseChildrenBatch() error {
	return w.timeImportPhase(importPhaseChildrenInsert, func() error {
		if err := w.childrenBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send children batch: %w", err)
		}

		w.childrenBatch = nil

		return nil
	})
}

func (w *dgutaWriter) recordImportPhase(phase string, d time.Duration) {
	if w == nil || w.importPhaseRecorder == nil || d <= 0 {
		return
	}

	w.importPhaseRecorder(phase, d)
}

func (w *dgutaWriter) timeImportPhase(phase string, fn func() error) error {
	start := time.Now()
	err := fn()

	w.recordImportPhase(phase, time.Since(start))

	return err
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

	conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
	if err != nil {
		return nil, err
	}

	return &dgutaWriter{cfg: cfg, conn: conn, batchSize: defaultBatchSize}, nil
}
