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
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	defaultBatchSize           = 100_000
	defaultRawDGUTABatchSize   = 10_000
	defaultProjectionBatchSize = 10_000
	defaultChildrenBatchSize   = 10_000

	importPhasePartitionDropReset = "partition_drop_reset"
	importPhaseDGUTAInsert        = "wrstat_dguta_insert"
	importPhaseChildrenInsert     = "wrstat_children_insert"
	importPhaseMountSwitch        = "mount_switch"
	importPhaseDirProjectionWrite = "wrstat_dir_projection_insert"
	importPhaseTreeSummaryRefresh = "wrstat_tree_summary_refresh"
	importPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"

	activeSnapshotQuery = "SELECT toString(snapshot_id) FROM wrstat_mounts_active_v2 " +
		"WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) " +
		"SELECT ?, greatest(coalesce(max(switched_at) + toIntervalMillisecond(1), now64(3)), now64(3)), " +
		"toUUID(?), ? FROM wrstat_mounts WHERE mount_path = ?"

	dropDGUTAPartitionQuery          = "ALTER TABLE wrstat_dguta DROP PARTITION tuple(?, toUUID(?))"
	dropChildrenPartitionQuery       = "ALTER TABLE wrstat_children DROP PARTITION tuple(?, toUUID(?))"
	dropFilesPartitionQuery          = "ALTER TABLE wrstat_files DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummaryPartitionQuery     = "ALTER TABLE wrstat_dir_summary DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummarySetPartitionQuery  = "ALTER TABLE wrstat_dir_summary_sets DROP PARTITION tuple(?, toUUID(?))"
	dropDirDGUTAVectorPartitionQuery = "ALTER TABLE wrstat_dir_dguta_vector " +
		"DROP PARTITION tuple(?, toUUID(?))"

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
	errMountPathRequired        = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired        = errors.New("clickhouse: updated at is required")
	errDirRequired              = errors.New("clickhouse: record dir is required")
	errDGUTABatchNotPrepared    = errors.New("clickhouse: dguta batch is not prepared")
	errChildrenBatchNotPrepared = errors.New("clickhouse: children batch is not prepared")
	errActiveSnapshotRewrite    = errors.New(
		"clickhouse: refusing to rewrite active snapshot",
	)
)

func basedirsPartitionDropQueries() []string {
	return []string{
		dropBasedirsGroupUsagePartitionQuery,
		dropBasedirsUserUsagePartitionQuery,
		dropBasedirsGroupSubdirsPartitionQuery,
		dropBasedirsUserSubdirsPartitionQuery,
	}
}

type dgutaRowKey struct {
	dir         string
	gid         uint32
	uid         uint32
	ft          uint16
	age         uint8
	count       uint64
	size        uint64
	atime       int64
	mtime       int64
	aTimeRanges [9]uint64
	mTimeRanges [9]uint64
}

func newDGUTARowKey(dir string, guta *db.GUTA) dgutaRowKey {
	return dgutaRowKey{
		dir:         dir,
		gid:         guta.GID,
		uid:         guta.UID,
		ft:          uint16(guta.FT),
		age:         uint8(guta.Age),
		count:       guta.Count,
		size:        guta.Size,
		atime:       guta.Atime,
		mtime:       guta.Mtime,
		aTimeRanges: guta.ATimeRanges,
		mTimeRanges: guta.MTimeRanges,
	}
}

type dgutaRecordRows struct {
	rawDir       string
	canonicalDir string
	keys         map[dgutaRowKey]struct{}
}

func (r dgutaRecordRows) ages() []db.DirGUTAge {
	ages := make([]db.DirGUTAge, 0, len(r.keys))
	seen := make(map[db.DirGUTAge]struct{}, len(r.keys))

	for key := range r.keys {
		age := db.DirGUTAge(key.age)
		if _, ok := seen[age]; ok {
			continue
		}

		seen[age] = struct{}{}
		ages = append(ages, age)
	}

	return ages
}

type dgutaBatchSlot struct {
	batch     *driver.Batch
	flushed   *bool
	batchSize int
	phase     string
	name      string
}

func (slot dgutaBatchSlot) rows() int {
	if slot.batch == nil || *slot.batch == nil {
		return 0
	}

	return (*slot.batch).Rows()
}

func (slot dgutaBatchSlot) wasFlushed() bool {
	return slot.flushed != nil && *slot.flushed
}

type dgutaWriter struct {
	cfg Config

	conn ch.Conn

	batchSize           int
	dgutaBatchSize      int
	projectionBatchSize int
	childrenBatchSize   int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	prepared bool

	dgutaBatch    driver.Batch
	childrenBatch driver.Batch
	dgutaFlushed  bool
	childFlushed  bool

	importPhaseRecorder func(string, time.Duration)

	// failBeforeSwitchErr forces Close() to fail before switching snapshots.
	// Used only by integration tests.
	failBeforeSwitchErr error

	previousDGUTARows dgutaRecordRows
	dirProjection     mountDirProjectionWriter

	closed   bool
	writeErr error
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
		w.dgutaBatchSize = rawDGUTABatchSizeFor(batchSize)
		w.projectionBatchSize = projectionBatchSizeFor(batchSize)
		w.childrenBatchSize = childrenBatchSizeFor(batchSize)
	}
}

func rawDGUTABatchSizeFor(batchSize int) int {
	if batchSize <= 0 {
		return defaultRawDGUTABatchSize
	}

	return min(batchSize, defaultRawDGUTABatchSize)
}

func projectionBatchSizeFor(batchSize int) int {
	if batchSize <= 0 {
		return defaultProjectionBatchSize
	}

	return min(batchSize, defaultProjectionBatchSize)
}

func childrenBatchSizeFor(batchSize int) int {
	if batchSize <= 0 {
		return defaultChildrenBatchSize
	}

	return min(batchSize, defaultChildrenBatchSize)
}

func (w *dgutaWriter) SetProjectionBatchSize(batchSize int) {
	if batchSize > 0 {
		w.projectionBatchSize = batchSize
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

	ctx, cancel := configQueryContext(w.cfg)
	defer cancel()

	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	return w.addReadyRecord(dguta)
}

func (w *dgutaWriter) addReadyRecord(dguta db.RecordDGUTA) error {
	rawParentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))
	parentDir := canonicalPathForMount(w.mountPath, rawParentDir)

	appendedGUTAs, err := w.appendDGUTARows(dguta, rawParentDir, parentDir)
	if err != nil {
		return err
	}

	appendedChildren, err := w.appendChildrenRows(dguta.Children, parentDir)
	if err != nil {
		return err
	}

	if err := w.appendMountDirProjectionRows(
		parentDir,
		appendedGUTAs,
		appendedChildren,
		w.previousDGUTARows.ages(),
	); err != nil {
		return err
	}

	return w.flushFullBatches()
}

func (w *dgutaWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	if w.conn == nil {
		return nil
	}

	ctx, cancel := configQueryContext(w.cfg)
	defer cancel()

	if err := w.ensureCloseReady(ctx); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.flushAllBatches(); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.publishSnapshotOnClose(ctx); err != nil {
		return err
	}

	return w.conn.Close()
}

func (w *dgutaWriter) publishSnapshotOnClose(ctx context.Context) error {
	if !w.shouldSwitchSnapshot() {
		return nil
	}

	if err := w.writeMountDirProjectionSummarySet(ctx); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.switchSnapshotAndDropOld(ctx); err != nil {
		_ = w.conn.Close()

		return err
	}

	return nil
}

func (w *dgutaWriter) Abort() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	if w.conn == nil {
		return nil
	}

	ctx, cancel := configQueryContext(w.cfg)
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

func (w *dgutaWriter) ensureCloseReady(ctx context.Context) error {
	if w.writeErr != nil {
		return w.writeErr
	}

	if !w.shouldSwitchSnapshot() || w.prepared {
		return nil
	}

	return w.ensureWriteReady(ctx)
}

func (w *dgutaWriter) ensureSnapshotID() {
	if w.snapshot != uuid.Nil {
		return
	}

	w.snapshot = snapshotID(w.mountPath, w.updatedAt)
}

func (w *dgutaWriter) switchActiveSnapshot(ctx context.Context) error {
	w.ensureSnapshotID()

	if err := w.conn.Exec(
		ctx,
		switchSnapshotQuery,
		w.mountPath,
		w.snapshot.String(),
		w.updatedAt,
		w.mountPath,
	); err != nil {
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

	if hasPrevious {
		if err := w.dropPreviousSnapshotPartitions(ctx, previousSID); err != nil {
			return err
		}
	}

	w.refreshActiveTreeSummariesBestEffort(ctx)

	return nil
}

func (w *dgutaWriter) refreshActiveTreeSummariesBestEffort(ctx context.Context) {
	if err := w.refreshActiveTreeSummaries(ctx); err != nil {
		return
	}
}

func (w *dgutaWriter) writeMountDirProjectionSummarySet(ctx context.Context) error {
	w.ensureSnapshotID()

	return w.timeImportPhase(importPhaseDirProjectionWrite, func() error {
		return writeMountDirSummarySetRow(ctx, w.conn, w.activeMount(), w.dirProjection.refreshedAt)
	})
}

func (w *dgutaWriter) activeMount() activeMount {
	return activeMount{
		mountPath:  w.mountPath,
		snapshotID: w.snapshot.String(),
		updatedAt:  w.updatedAt,
	}
}

func (w *dgutaWriter) refreshActiveTreeSummaries(ctx context.Context) error {
	return w.timeImportPhase(importPhaseTreeSummaryRefresh, func() error {
		rows, err := queryMountsActiveRows(ctx, w.conn)
		if err != nil {
			return err
		}

		return ensureActiveTreeSummaries(ctx, w.conn, rows)
	})
}

func (w *dgutaWriter) switchSnapshotOrCleanup(ctx context.Context) error {
	if w.failBeforeSwitchErr != nil {
		return w.closeWithNewSnapshotCleanup(ctx, w.failBeforeSwitchErr)
	}

	if err := w.timeImportPhase(importPhaseMountSwitch, func() error {
		return w.switchActiveSnapshot(ctx)
	}); err != nil {
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

	cleanupCtx, cleanupCancel := queryContext(context.WithoutCancel(ctx), activeSnapshotCleanupTimeout)
	defer cleanupCancel()

	return w.timeImportPhase(importPhaseOldSnapshotDrop, func() error {
		return w.dropAllSnapshotPartitions(cleanupCtx, previousSID)
	})
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
		rowErr := rows.Err()
		if rowErr != nil {
			return "", false, fmt.Errorf("clickhouse: active snapshot iteration error: %w", rowErr)
		}

		return "", false, nil
	}

	sid, err := scanActiveSnapshotID(rows)
	if err != nil {
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

	sid := w.snapshot.String()
	if hasActive && activeSID == sid {
		return nil
	}

	return w.dropAllSnapshotPartitions(ctx, sid)
}

func (w *dgutaWriter) snapshotCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}

	return configQueryContext(w.cfg)
}

func (w *dgutaWriter) abortAllBatches() error {
	return errors.Join(
		abortBatch(&w.dgutaBatch, "dguta"),
		abortBatch(&w.childrenBatch, "children"),
		w.dirProjection.abortAll(),
	)
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
	return dropSnapshotPartitionsForMount(ctx, w.conn, w.mountPath, sid, allPartitionDropQueries())
}

func dropSnapshotPartitionsForMount(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	queries []string,
) error {
	for _, query := range queries {
		if err := dropPartitionIgnoreUnknown(ctx, conn, mountPath, sid, query); err != nil {
			return err
		}
	}

	return nil
}

func allPartitionDropQueries() []string {
	return []string{
		dropDGUTAPartitionQuery,
		dropChildrenPartitionQuery,
		dropFilesPartitionQuery,
		dropDirSummaryPartitionQuery,
		dropDirSummarySetPartitionQuery,
		dropDirDGUTAVectorPartitionQuery,
		dropBasedirsGroupUsagePartitionQuery,
		dropBasedirsUserUsagePartitionQuery,
		dropBasedirsGroupSubdirsPartitionQuery,
		dropBasedirsUserSubdirsPartitionQuery,
	}
}

func (w *dgutaWriter) ensureWriteReady(ctx context.Context) error {
	if w.writeErr != nil {
		return w.writeErr
	}

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

	return w.prepareWriteBatches(ctx)
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

func (w *dgutaWriter) prepareWriteBatches(ctx context.Context) error {
	dgutaBatch, childrenBatch, dirProjection, err := w.prepareBatches(ctx)
	if err != nil {
		return err
	}

	w.dgutaBatch = dgutaBatch
	w.childrenBatch = childrenBatch
	w.dirProjection = dirProjection
	w.prepared = true

	return nil
}

func (w *dgutaWriter) prepareBatches(
	ctx context.Context,
) (driver.Batch, driver.Batch, mountDirProjectionWriter, error) {
	dgutaBatch, err := w.prepareBatch(ctx, insertDGUTAQuery)
	if err != nil {
		return nil, nil, mountDirProjectionWriter{}, fmt.Errorf("clickhouse: failed to prepare dguta batch: %w", err)
	}

	childrenBatch, err := w.prepareChildrenBatch(ctx, dgutaBatch)
	if err != nil {
		return nil, nil, mountDirProjectionWriter{}, err
	}

	dirProjection, err := w.prepareDirProjectionBatches(ctx, dgutaBatch, childrenBatch)
	if err != nil {
		return nil, nil, mountDirProjectionWriter{}, err
	}

	return dgutaBatch, childrenBatch, dirProjection, nil
}

func (w *dgutaWriter) prepareChildrenBatch(ctx context.Context, dgutaBatch driver.Batch) (driver.Batch, error) {
	childrenBatch, err := w.prepareBatch(ctx, insertChildrenQuery)
	if err == nil {
		return childrenBatch, nil
	}

	if abortErr := dgutaBatch.Abort(); abortErr != nil {
		return nil, fmt.Errorf(
			"clickhouse: failed to prepare children batch and abort dguta batch: %w",
			errors.Join(err, abortErr),
		)
	}

	return nil, fmt.Errorf("clickhouse: failed to prepare children batch: %w", err)
}

func (w *dgutaWriter) prepareDirProjectionBatches(
	ctx context.Context,
	dgutaBatch, childrenBatch driver.Batch,
) (mountDirProjectionWriter, error) {
	dirProjection, err := prepareMountDirProjectionWriter(ctx, w.conn)
	if err == nil {
		return dirProjection, nil
	}

	abortErr := errors.Join(
		abortBatch(&childrenBatch, "children"),
		abortBatch(&dgutaBatch, "dguta"),
	)
	if abortErr != nil {
		return mountDirProjectionWriter{}, fmt.Errorf(
			"clickhouse: failed to prepare dir projection batches and abort import batches: %w",
			errors.Join(err, abortErr),
		)
	}

	return mountDirProjectionWriter{}, err
}

func (w *dgutaWriter) prepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return prepareBatchWithRelease(ctx, w.conn, query)
}

func prepareBatchWithRelease(ctx context.Context, conn ch.Conn, query string) (driver.Batch, error) {
	return conn.PrepareBatch(importBatchContext(ctx), query, driver.WithReleaseConnection())
}

func (w *dgutaWriter) dropNewSnapshotPartitions(ctx context.Context) error {
	return dropSnapshotPartitionsForMount(
		ctx,
		w.conn,
		w.mountPath,
		w.snapshot.String(),
		dgutaPartitionDropQueries(),
	)
}

func dgutaPartitionDropQueries() []string {
	return []string{
		dropDGUTAPartitionQuery,
		dropChildrenPartitionQuery,
		dropDirSummaryPartitionQuery,
		dropDirSummarySetPartitionQuery,
		dropDirDGUTAVectorPartitionQuery,
	}
}

func (w *dgutaWriter) appendDGUTARows(
	dguta db.RecordDGUTA,
	rawParentDir, parentDir string,
) (db.GUTAs, error) {
	keys := make(map[dgutaRowKey]struct{}, len(dguta.GUTAs))
	appendedGUTAs := make(db.GUTAs, 0, len(dguta.GUTAs))

	err := w.timeImportPhase(importPhaseDGUTAInsert, func() error {
		for _, guta := range dguta.GUTAs {
			if err := w.appendDGUTARowForRecord(rawParentDir, parentDir, guta, keys, &appendedGUTAs); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	w.previousDGUTARows = dgutaRecordRows{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		keys:         keys,
	}

	return appendedGUTAs, nil
}

func (w *dgutaWriter) appendDGUTARowForRecord(
	rawParentDir, parentDir string,
	guta *db.GUTA,
	keys map[dgutaRowKey]struct{},
	appendedGUTAs *db.GUTAs,
) error {
	key, keep, appended, err := w.appendDGUTARow(rawParentDir, parentDir, guta)
	if err != nil {
		return err
	}

	if keep {
		keys[key] = struct{}{}
	}

	if appended {
		*appendedGUTAs = append(*appendedGUTAs, guta)
	}

	return nil
}

func (w *dgutaWriter) appendDGUTARow(
	rawParentDir, parentDir string,
	guta *db.GUTA,
) (dgutaRowKey, bool, bool, error) {
	if guta == nil {
		return dgutaRowKey{}, false, false, nil
	}

	rowKey := newDGUTARowKey(parentDir, guta)
	if w.isConsecutiveCanonicalDGUTADuplicate(rawParentDir, parentDir, rowKey) {
		return rowKey, true, false, nil
	}

	if err := w.appendDGUTABatchRow(parentDir, guta); err != nil {
		return dgutaRowKey{}, false, false, fmt.Errorf("clickhouse: failed to append dguta row: %w", err)
	}

	return rowKey, true, true, nil
}

func (w *dgutaWriter) appendDGUTABatchRow(parentDir string, guta *db.GUTA) error {
	if w.writeErr != nil {
		return w.writeErr
	}

	if w.dgutaBatch == nil {
		return errDGUTABatchNotPrepared
	}

	if err := w.dgutaBatch.Append(
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
	); err != nil {
		return err
	}

	return w.sendFullDGUTABatchIfFull()
}

func (w *dgutaWriter) sendFullDGUTABatchIfFull() error {
	batchSize := w.effectiveDGUTABatchSize()
	if w.dgutaBatch == nil || batchSize <= 0 || w.dgutaBatch.Rows() < batchSize {
		return nil
	}

	return w.sendFullDGUTABatch()
}

func (w *dgutaWriter) sendFullDGUTABatch() error {
	if err := w.dgutaBatch.Send(); err != nil {
		w.dgutaBatch = nil
		w.writeErr = fmt.Errorf("clickhouse: failed to send dguta batch: %w", err)

		return w.writeErr
	}

	w.dgutaBatch = nil
	w.dgutaFlushed = false

	batch, err := w.prepareBatch(context.Background(), insertDGUTAQuery)
	if err != nil {
		w.writeErr = fmt.Errorf("clickhouse: failed to prepare dguta batch: %w", err)

		return w.writeErr
	}

	w.dgutaBatch = batch

	return nil
}

func (w *dgutaWriter) isConsecutiveCanonicalDGUTADuplicate(rawDir, canonicalDir string, key dgutaRowKey) bool {
	prev := w.previousDGUTARows
	if prev.keys == nil || prev.rawDir == rawDir || prev.canonicalDir != canonicalDir {
		return false
	}

	if prev.rawDir == prev.canonicalDir && rawDir == canonicalDir {
		return false
	}

	_, ok := prev.keys[key]

	return ok
}

func (w *dgutaWriter) appendChildrenRows(children []string, parentDir string) (uint64, error) {
	snapshotID := w.snapshot.String()

	var appended uint64

	err := w.timeImportPhase(importPhaseChildrenInsert, func() error {
		for _, child := range children {
			ok, err := w.appendChildRow(snapshotID, parentDir, child)
			if err != nil {
				return err
			}

			if ok {
				appended++
			}
		}

		return nil
	})

	return appended, err
}

func (w *dgutaWriter) appendChildRow(snapshotID, parentDir, child string) (bool, error) {
	if w.writeErr != nil {
		return false, w.writeErr
	}

	if w.childrenBatch == nil {
		w.writeErr = errChildrenBatchNotPrepared

		return false, w.writeErr
	}

	child = childPathForParent(parentDir, child)

	child = canonicalPathForMount(w.mountPath, child)
	if child == "" {
		return false, nil
	}

	if err := w.childrenBatch.Append(w.mountPath, snapshotID, parentDir, child); err != nil {
		return false, fmt.Errorf("clickhouse: failed to append child row: %w", err)
	}

	if err := w.sendFullChildrenBatchIfFull(); err != nil {
		return false, err
	}

	return true, nil
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

func (w *dgutaWriter) sendFullChildrenBatchIfFull() error {
	batchSize := w.effectiveChildrenBatchSize()
	if w.childrenBatch == nil || batchSize <= 0 || w.childrenBatch.Rows() < batchSize {
		return nil
	}

	return w.sendFullChildrenBatch()
}

func (w *dgutaWriter) sendFullChildrenBatch() error {
	if err := w.childrenBatch.Send(); err != nil {
		w.childrenBatch = nil
		w.writeErr = fmt.Errorf("clickhouse: failed to send children batch: %w", err)

		return w.writeErr
	}

	w.childrenBatch = nil
	w.childFlushed = false

	batch, err := w.prepareBatch(context.Background(), insertChildrenQuery)
	if err != nil {
		w.writeErr = fmt.Errorf("clickhouse: failed to prepare children batch: %w", err)

		return w.writeErr
	}

	w.childrenBatch = batch

	return nil
}

func (w *dgutaWriter) appendMountDirProjectionRows(
	parentDir string,
	gutas db.GUTAs,
	childCount uint64,
	recordAges []db.DirGUTAge,
) error {
	err := w.timeImportPhase(importPhaseDirProjectionWrite, func() error {
		return w.dirProjection.appendRecord(
			w.activeMount(),
			parentDir,
			gutas,
			childCount,
			recordAges,
			w.effectiveProjectionBatchSize(),
		)
	})
	if err != nil {
		w.writeErr = err
	}

	return err
}

func (w *dgutaWriter) flushFullBatches() error {
	if err := w.sendFullDGUTABatchIfFull(); err != nil {
		return err
	}

	if err := w.sendFullChildrenBatchIfFull(); err != nil {
		return err
	}

	if err := w.dirProjection.sendFullBatchIfFull(
		&w.dirProjection.summaryBatch,
		insertMountDirSummaryQuery,
		w.effectiveProjectionBatchSize(),
		"dir summary",
	); err != nil {
		return err
	}

	return w.dirProjection.sendFullBatchIfFull(
		&w.dirProjection.vectorBatch,
		insertMountDirDGUTAVectorQuery,
		w.effectiveProjectionBatchSize(),
		"dir dguta vector",
	)
}

func (w *dgutaWriter) flushAllBatches() error {
	for _, slot := range w.batchSlots() {
		if slot.rows() == 0 && !slot.wasFlushed() {
			_ = abortBatch(slot.batch, slot.name) //nolint:errcheck // Best-effort release; preserve close error behaviour.

			continue
		}

		if err := w.sendAndCloseBatch(slot); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) batchSlots() [4]dgutaBatchSlot {
	return [...]dgutaBatchSlot{
		w.rawDGUTABatchSlot(),
		{
			batch:     &w.childrenBatch,
			flushed:   &w.childFlushed,
			batchSize: w.effectiveChildrenBatchSize(),
			phase:     importPhaseChildrenInsert,
			name:      "children",
		},
		{
			batch:     &w.dirProjection.summaryBatch,
			flushed:   &w.dirProjection.summaryFlushed,
			batchSize: w.effectiveProjectionBatchSize(),
			phase:     importPhaseDirProjectionWrite,
			name:      "dir summary",
		},
		{
			batch:     &w.dirProjection.vectorBatch,
			flushed:   &w.dirProjection.vectorFlushed,
			batchSize: w.effectiveProjectionBatchSize(),
			phase:     importPhaseDirProjectionWrite,
			name:      "dir dguta vector",
		},
	}
}

func (w *dgutaWriter) rawDGUTABatchSlot() dgutaBatchSlot {
	return dgutaBatchSlot{
		batch:     &w.dgutaBatch,
		flushed:   &w.dgutaFlushed,
		batchSize: w.effectiveDGUTABatchSize(),
		phase:     importPhaseDGUTAInsert,
		name:      "dguta",
	}
}

func (w *dgutaWriter) effectiveDGUTABatchSize() int {
	if w.dgutaBatchSize > 0 {
		return w.dgutaBatchSize
	}

	return rawDGUTABatchSizeFor(w.batchSize)
}

func (w *dgutaWriter) effectiveProjectionBatchSize() int {
	if w.projectionBatchSize > 0 {
		return w.projectionBatchSize
	}

	return projectionBatchSizeFor(w.batchSize)
}

func (w *dgutaWriter) effectiveChildrenBatchSize() int {
	if w.childrenBatchSize > 0 {
		return w.childrenBatchSize
	}

	return childrenBatchSizeFor(w.batchSize)
}

func (w *dgutaWriter) sendAndCloseBatch(slot dgutaBatchSlot) error {
	return w.timeImportPhase(slot.phase, func() error {
		if err := (*slot.batch).Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send %s batch: %w", slot.name, err)
		}

		*slot.batch = nil

		return nil
	})
}

func (w *dgutaWriter) recordImportPhase(phase string, d time.Duration) {
	if w == nil {
		return
	}

	recordImportPhase(w.importPhaseRecorder, phase, d)
}

func (w *dgutaWriter) timeImportPhase(phase string, fn func() error) error {
	return timeImportPhase(w.recordImportPhase, phase, fn)
}

// NewDGUTAWriter returns a ClickHouse-backed implementation of db.DGUTAWriter.
func NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	conn, err := connectFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &dgutaWriter{
		cfg:                 cfg,
		conn:                conn,
		batchSize:           defaultBatchSize,
		dgutaBatchSize:      rawDGUTABatchSizeFor(defaultBatchSize),
		projectionBatchSize: projectionBatchSizeFor(defaultBatchSize),
		childrenBatchSize:   childrenBatchSizeFor(defaultBatchSize),
	}, nil
}

func importBatchContext(parent context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}

	// clickhouse-go stores the PrepareBatch context and reuses it during Send,
	// so import batches must not retain normal query deadlines across long runs.
	return context.WithoutCancel(parent)
}

func scanActiveSnapshotID(rows driver.Rows) (string, error) {
	if len(rows.Columns()) == 1 {
		var sid string

		return sid, rows.Scan(&sid)
	}

	var (
		sid       string
		updatedAt time.Time
	)

	return sid, rows.Scan(&sid, &updatedAt)
}
