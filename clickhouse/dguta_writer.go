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
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	defaultBatchSize           = 100_000
	defaultProjectionBatchSize = 15_300
	defaultChildrenBatchSize   = 10_000
	dgutaAgeMaskBits           = 32
	defaultCHReceiveTimeout    = 300 * time.Second
	importBatchReceiveGuard    = time.Minute
	importBatchMaxOpenDuration = defaultCHReceiveTimeout - importBatchReceiveGuard

	importPhasePartitionDropReset = "partition_drop_reset"
	importPhaseDGUTAInsert        = "wrstat_dguta_insert"
	importPhaseChildrenInsert     = "wrstat_children_insert"
	importPhaseMountSwitch        = "mount_switch"
	importPhaseDirProjectionWrite = "wrstat_dir_projection_insert"
	importPhaseTreeSummaryRefresh = "wrstat_tree_summary_refresh"
	importPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"

	activeSnapshotQuery = "SELECT toString(snapshot_id) FROM wrstat_mounts_active " +
		"WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mount_events " +
		"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, greatest(coalesce(max(event_at) + toIntervalMillisecond(1), now64(3)), now64(3)), " +
		"1, toUUID(?), ?, 'publish' FROM wrstat_mount_events WHERE mount_path = ?"

	dropChildrenPartitionQuery       = "ALTER TABLE wrstat_children DROP PARTITION tuple(?, toUUID(?))"
	dropFilesPartitionQuery          = "ALTER TABLE wrstat_files DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummaryPartitionQuery     = "ALTER TABLE wrstat_dir_facts DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummarySetPartitionQuery  = "ALTER TABLE wrstat_dir_projection_sets DROP PARTITION tuple(?, toUUID(?))"
	dropDirDGUTAVectorPartitionQuery = "ALTER TABLE wrstat_dir_facts " +
		"DROP PARTITION tuple(?, toUUID(?))"

	dropBasedirsGroupUsagePartitionQuery   = "ALTER TABLE wrstat_basedirs_group_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserUsagePartitionQuery    = "ALTER TABLE wrstat_basedirs_user_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsGroupSubdirsPartitionQuery = "ALTER TABLE wrstat_basedirs_group_subdirs DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserSubdirsPartitionQuery  = "ALTER TABLE wrstat_basedirs_user_subdirs DROP PARTITION tuple(?, toUUID(?))"

	insertChildrenQuery = "INSERT INTO wrstat_children " +
		"(mount_path, snapshot_id, parent_dir, child) " +
		"VALUES (?, toUUID(?), ?, ?)"
)

var (
	errMountPathRequired        = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired        = errors.New("clickhouse: updated at is required")
	errDirRequired              = errors.New("clickhouse: record dir is required")
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
	ageMask      uint32
}

func (r dgutaRecordRows) ages() []db.DirGUTAge {
	if r.ageMask == 0 {
		return nil
	}

	ages := make([]db.DirGUTAge, 0, len(db.DirGUTAges))
	for _, age := range db.DirGUTAges {
		if dgutaAgeMaskHas(r.ageMask, age) {
			ages = append(ages, age)
		}
	}

	return ages
}

func dgutaAgeMaskHas(mask uint32, age db.DirGUTAge) bool {
	bit := dgutaAgeBit(age)

	return bit != 0 && mask&bit != 0
}

type dgutaRecordTracker struct {
	trackDuplicateKeys bool
	keys               map[dgutaRowKey]struct{}
	ageMask            uint32
}

type dgutaDerivedIndexWriter interface {
	appendRecord(
		ctx context.Context,
		mount activeMount,
		parentDir string,
		gutas db.GUTAs,
		children []string,
		ages []db.DirGUTAge,
	) error
	flush(ctx context.Context) error
	abort() error
}

type dgutaBatchSlot struct {
	batch     *driver.Batch
	openedAt  *time.Time
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
	projectionBatchSize int
	childrenBatchSize   int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	prepared bool

	childrenBatch driver.Batch
	childOpenedAt time.Time
	batchNow      func() time.Time
	childFlushed  bool

	importPhaseRecorder func(string, time.Duration)

	// failBeforeSwitchErr forces Close() to fail before switching snapshots.
	// Used only by integration tests.
	failBeforeSwitchErr error

	previousDGUTARows dgutaRecordRows
	dirProjection     mountDirProjectionWriter

	selectedDerivedIndexes []dgutaDerivedIndexWriter

	closed   bool
	writeErr error
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
		w.projectionBatchSize = projectionBatchSizeFor(batchSize)
		w.childrenBatchSize = childrenBatchSizeFor(batchSize)
	}
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
	w.mountPath = normalizeImportMountPath(mountPath)
}

func normalizeImportMountPath(mountPath string) string {
	if mountPath == "" || mountPath == "/" {
		return mountPath
	}

	return ensureTrailingSlash(mountPath)
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

	return w.addReadyRecord(ctx, dguta)
}

func (w *dgutaWriter) addReadyRecord(ctx context.Context, dguta db.RecordDGUTA) error {
	rawParentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))
	parentDir := canonicalPathForMount(w.mountPath, rawParentDir)
	children := w.canonicalChildrenForParent(parentDir, dguta.Children)
	childCount := max(dguta.ChildCount, uint64(len(children)))

	appendedGUTAs, err := w.appendDGUTARows(dguta, rawParentDir, parentDir)
	if err != nil {
		return err
	}

	if err := w.appendMountDirProjectionRows(
		ctx,
		parentDir,
		appendedGUTAs,
		childCount,
		w.previousDGUTARows.ages(),
	); err != nil {
		return err
	}

	if err := w.appendChildrenRows(ctx, children, parentDir); err != nil {
		return err
	}

	if err := w.appendSelectedDerivedIndexRows(ctx, parentDir, appendedGUTAs, children); err != nil {
		return err
	}

	return w.flushFullBatches()
}

func (w *dgutaWriter) AddChildren(parent *summary.DirectoryPath, children []string) error {
	if err := w.validateAddChildren(parent); err != nil {
		return err
	}

	ctx, cancel := configQueryContext(w.cfg)
	defer cancel()

	if err := w.ensureWriteReady(ctx); err != nil {
		return err
	}

	rawParentDir := string(parent.AppendTo(make([]byte, 0, parent.Len())))
	parentDir := canonicalPathForMount(w.mountPath, rawParentDir)

	if err := w.appendChildrenRows(ctx, children, parentDir); err != nil {
		return err
	}

	return w.sendFullChildrenBatchIfFull()
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

	if err := w.flushAllBatchesWithContext(ctx); err != nil {
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

func (w *dgutaWriter) validateAddChildren(parent *summary.DirectoryPath) error {
	if w.mountPath == "" {
		return errMountPathRequired
	}

	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	if parent == nil {
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

	invalidateActiveMetadataCache(w.cfg)

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
		if err := w.dropAllSnapshotPartitions(cleanupCtx, previousSID); err != nil {
			return fmt.Errorf("clickhouse: %s: mount_path=%s snapshot_id=%s: %w",
				importPhaseOldSnapshotDrop, w.mountPath, previousSID, err)
		}

		return nil
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
		abortBatch(&w.childrenBatch, "children"),
		w.dirProjection.abortAll(),
		w.abortSelectedDerivedIndexes(),
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
		return fmt.Errorf("clickhouse: failed_snapshot_partition_drop: mount_path=%s snapshot_id=%s: %w",
			w.mountPath, w.snapshot.String(), err)
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
	w.dirProjection = prepareMountDirProjectionWriter(ctx, w.conn)
	w.prepared = true

	return nil
}

func (w *dgutaWriter) dropNewSnapshotPartitions(ctx context.Context) error {
	return dropSnapshotPartitionsForMount(
		ctx,
		w.conn,
		w.mountPath,
		w.snapshot.String(),
		allPartitionDropQueries(),
	)
}

func (w *dgutaWriter) appendDGUTARows(
	dguta db.RecordDGUTA,
	rawParentDir, parentDir string,
) (db.GUTAs, error) {
	tracker := w.newDGUTARecordTracker(len(dguta.GUTAs))
	appendedGUTAs := make(db.GUTAs, 0, len(dguta.GUTAs))

	err := w.timeImportPhase(importPhaseDGUTAInsert, func() error {
		return w.appendDGUTARecordRows(dguta, rawParentDir, parentDir, &tracker, &appendedGUTAs)
	})
	if err != nil {
		return nil, err
	}

	w.previousDGUTARows = dgutaRecordRows{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		keys:         tracker.keys,
		ageMask:      tracker.ageMask,
	}

	return appendedGUTAs, nil
}

func (w *dgutaWriter) appendDGUTARecordRows(
	dguta db.RecordDGUTA,
	rawParentDir, parentDir string,
	tracker *dgutaRecordTracker,
	appendedGUTAs *db.GUTAs,
) error {
	for _, guta := range dguta.GUTAs {
		if err := w.appendDGUTARowForRecord(rawParentDir, parentDir, guta, tracker, appendedGUTAs); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) newDGUTARecordTracker(numGUTAs int) dgutaRecordTracker {
	tracker := dgutaRecordTracker{trackDuplicateKeys: w.shouldTrackCanonicalDGUTADuplicateKeys()}
	if tracker.trackDuplicateKeys {
		tracker.keys = make(map[dgutaRowKey]struct{}, numGUTAs)
	}

	return tracker
}

func (w *dgutaWriter) shouldTrackCanonicalDGUTADuplicateKeys() bool {
	return w.mountPath == "/"
}

func (w *dgutaWriter) appendDGUTARowForRecord(
	rawParentDir, parentDir string,
	guta *db.GUTA,
	tracker *dgutaRecordTracker,
	appendedGUTAs *db.GUTAs,
) error {
	key, keep, project := w.appendDGUTARow(
		rawParentDir,
		parentDir,
		guta,
		tracker.trackDuplicateKeys,
	)

	if keep {
		if tracker.trackDuplicateKeys {
			tracker.keys[key] = struct{}{}
		}

		tracker.ageMask = dgutaAgeMaskWith(tracker.ageMask, guta.Age)
	}

	if project {
		*appendedGUTAs = append(*appendedGUTAs, guta)
	}

	return nil
}

func dgutaAgeMaskWith(mask uint32, age db.DirGUTAge) uint32 {
	bit := dgutaAgeBit(age)
	if bit == 0 {
		return mask
	}

	return mask | bit
}

func (w *dgutaWriter) appendDGUTARow(
	rawParentDir, parentDir string,
	guta *db.GUTA,
	trackDuplicateKeys bool,
) (dgutaRowKey, bool, bool) {
	if guta == nil {
		return dgutaRowKey{}, false, false
	}

	if !trackDuplicateKeys {
		return dgutaRowKey{}, true, true
	}

	rowKey := newDGUTARowKey(parentDir, guta)
	if w.isConsecutiveCanonicalDGUTADuplicate(rawParentDir, parentDir, rowKey) {
		return rowKey, true, false
	}

	return rowKey, true, true
}

func (w *dgutaWriter) compactInternalDGUTAAges(parentDir string) bool {
	return isInternalMountDir(w.mountPath, parentDir)
}

func isInternalMountDir(mountPath, dir string) bool {
	mountPath = normalizeImportMountPath(mountPath)

	return mountPath != "" &&
		mountPath != "/" &&
		dir != mountPath &&
		strings.HasPrefix(dir, mountPath)
}

func (w *dgutaWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
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

func (w *dgutaWriter) appendChildrenRows(
	ctx context.Context,
	children []string,
	parentDir string,
) error {
	snapshotID := w.snapshot.String()

	return w.timeImportPhase(importPhaseChildrenInsert, func() error {
		for _, child := range children {
			_, err := w.appendChildRowWithContext(ctx, snapshotID, parentDir, child)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (w *dgutaWriter) canonicalChildrenForParent(parentDir string, children []string) []string {
	out := make([]string, 0, len(children))

	for _, child := range children {
		child = childPathForParent(parentDir, child)

		child = canonicalPathForMount(w.mountPath, child)
		if child != "" {
			out = append(out, child)
		}
	}

	return out
}

func (w *dgutaWriter) appendChildRow(snapshotID, parentDir, child string) (bool, error) {
	return w.appendChildRowWithContext(context.Background(), snapshotID, parentDir, child)
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

func (w *dgutaWriter) appendChildRowWithContext(
	ctx context.Context,
	snapshotID, parentDir, child string,
) (bool, error) {
	if w.writeErr != nil {
		return false, w.writeErr
	}

	child = childPathForParent(parentDir, child)

	child = canonicalPathForMount(w.mountPath, child)
	if child == "" {
		return false, nil
	}

	if err := w.childrenBlockWriter().append(ctx, func(batch driver.Batch) error {
		return batch.Append(w.mountPath, snapshotID, parentDir, child)
	}); err != nil {
		return false, err
	}

	return true, nil
}

func (w *dgutaWriter) childrenBlockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertChildrenQuery,
		name:        "children",
		batch:       &w.childrenBatch,
		openedAt:    &w.childOpenedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.effectiveChildrenBatchSize(),
		notPrepared: errChildrenBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *dgutaWriter) sendFullChildrenBatchIfFull() error {
	return w.childrenBlockWriter().sendIfFull()
}

func (w *dgutaWriter) appendMountDirProjectionRows(
	ctx context.Context,
	parentDir string,
	gutas db.GUTAs,
	childCount uint64,
	recordAges []db.DirGUTAge,
) error {
	err := w.timeImportPhase(importPhaseDirProjectionWrite, func() error {
		return w.dirProjection.appendRecordWithContext(
			ctx,
			w.activeMount(),
			parentDir,
			gutas,
			childCount,
			recordAges,
			w.compactInternalDGUTAAges(parentDir),
			w.effectiveProjectionBatchSize(),
		)
	})
	if err != nil {
		w.writeErr = err
	}

	return err
}

func (w *dgutaWriter) appendSelectedDerivedIndexRows(
	ctx context.Context,
	parentDir string,
	gutas db.GUTAs,
	children []string,
) error {
	for _, writer := range w.selectedDerivedIndexes {
		err := writer.appendRecord(ctx, w.activeMount(), parentDir, gutas, children, w.previousDGUTARows.ages())
		if err != nil {
			w.writeErr = err

			return err
		}
	}

	return nil
}

func (w *dgutaWriter) flushFullBatches() error {
	if err := w.dirProjection.sendFullBatchIfFull(
		&w.dirProjection.summaryBatch,
		&w.dirProjection.summaryOpenedAt,
		w.effectiveProjectionBatchSize(),
		"dir facts",
	); err != nil {
		return err
	}

	if err := w.sendFullChildrenBatchIfFull(); err != nil {
		return err
	}

	return nil
}

func (w *dgutaWriter) flushAllBatches() error {
	return w.flushAllBatchesWithContext(context.Background())
}

func (w *dgutaWriter) flushAllBatchesWithContext(ctx context.Context) error {
	for _, slot := range w.batchSlots() {
		if slot.rows() == 0 && !slot.wasFlushed() {
			_ = abortBatch(slot.batch, slot.name) //nolint:errcheck // Best-effort release; preserve close error behaviour.

			continue
		}

		if err := w.sendAndCloseBatch(slot); err != nil {
			return err
		}
	}

	return w.flushSelectedDerivedIndexes(ctx)
}

func (w *dgutaWriter) batchSlots() [2]dgutaBatchSlot {
	return [...]dgutaBatchSlot{
		{
			batch:     &w.dirProjection.summaryBatch,
			openedAt:  &w.dirProjection.summaryOpenedAt,
			flushed:   &w.dirProjection.summaryFlushed,
			batchSize: w.effectiveProjectionBatchSize(),
			phase:     importPhaseDirProjectionWrite,
			name:      "dir facts",
		},
		{
			batch:     &w.childrenBatch,
			openedAt:  &w.childOpenedAt,
			flushed:   &w.childFlushed,
			batchSize: w.effectiveChildrenBatchSize(),
			phase:     importPhaseChildrenInsert,
			name:      "children",
		},
	}
}

func (w *dgutaWriter) flushSelectedDerivedIndexes(ctx context.Context) error {
	for _, writer := range w.selectedDerivedIndexes {
		if err := writer.flush(ctx); err != nil {
			w.writeErr = err

			return err
		}
	}

	return nil
}

func (w *dgutaWriter) abortSelectedDerivedIndexes() error {
	var err error

	for _, writer := range w.selectedDerivedIndexes {
		err = errors.Join(err, writer.abort())
	}

	return err
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
		return (&importBlockWriter{
			name:      slot.name,
			batch:     slot.batch,
			openedAt:  slot.openedAt,
			writeErr:  &w.writeErr,
			batchSize: slot.batchSize,
			now:       w.importBatchNow,
		}).close()
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

	conn, err := connectForImportFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &dgutaWriter{
		cfg:                 cfg,
		conn:                conn,
		batchSize:           defaultBatchSize,
		projectionBatchSize: projectionBatchSizeFor(defaultBatchSize),
		childrenBatchSize:   childrenBatchSizeFor(defaultBatchSize),
	}, nil
}

func dgutaAgeBit(age db.DirGUTAge) uint32 {
	if age >= dgutaAgeMaskBits {
		return 0
	}

	return 1 << age
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
