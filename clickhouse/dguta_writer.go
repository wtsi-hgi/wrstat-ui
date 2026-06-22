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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	defaultBatchSize           = 100_000
	defaultProjectionBatchSize = 15_300
	dgutaAgeMaskBits           = 32
	defaultCHReceiveTimeout    = 300 * time.Second
	importBatchReceiveGuard    = time.Minute
	importBatchMaxOpenDuration = defaultCHReceiveTimeout - importBatchReceiveGuard
	activeVirtualSummaryName   = "summary"
	activeVirtualFilterName    = "filter"
	activeVirtualChildName     = "child"

	importPhasePartitionDropReset   = "partition_drop_reset"
	importPhaseCatalogInsert        = "wrstat_dirs_insert"
	importPhaseDGUTAInsert          = "wrstat_dguta_insert"
	importPhaseDirFilterAllInsert   = "wrstat_dir_filter_all_insert"
	importPhaseChildFilterAllInsert = "wrstat_child_filter_all_insert"
	importPhaseSchema3Ready         = "wrstat_schema3_snapshot_ready"
	importPhaseActiveVirtualInsert  = "wrstat_active_virtual_insert"
	importPhaseActiveVirtualReady   = "wrstat_active_virtual_ready"
	importPhaseMountSwitch          = "mount_switch"
	importPhaseDirProjectionWrite   = "wrstat_dir_projection_insert"
	importPhaseTreeSummaryRefresh   = "wrstat_tree_summary_refresh"
	importPhaseActivePrefixRefresh  = "wrstat_active_prefix_rollup_refresh"
	importPhaseOldSnapshotDrop      = "old_snapshot_partition_drop"

	activeSnapshotQuery = "SELECT toString(snapshot_id) FROM wrstat_mounts_active " +
		"WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mount_events " +
		"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, greatest(coalesce(max(event_at) + toIntervalMillisecond(1), now64(3)), now64(3)), " +
		"1, toUUID(?), ?, 'publish' FROM wrstat_mount_events WHERE mount_path = ?"

	dropDirsPartitionQuery            = "ALTER TABLE wrstat_dirs DROP PARTITION tuple(?, toUUID(?))"
	dropFilesPartitionQuery           = "ALTER TABLE wrstat_files DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummaryPartitionQuery      = "ALTER TABLE wrstat_dir_facts DROP PARTITION tuple(?, toUUID(?))"
	dropDirSummarySetPartitionQuery   = "ALTER TABLE wrstat_dir_projection_sets DROP PARTITION tuple(?, toUUID(?))"
	dropDirFilterAgeAllPartitionQuery = "ALTER TABLE wrstat_dir_filter_ageall " +
		"DROP PARTITION tuple(?, toUUID(?))"
	dropChildFilterAllPartitionQuery = "ALTER TABLE wrstat_child_filter_all " +
		"DROP PARTITION tuple(?, toUUID(?))"
	dropDirFilterAllPartitionQuery = "ALTER TABLE wrstat_dir_filter_all " +
		"DROP PARTITION tuple(?, toUUID(?))"
	dropSchema3SnapshotSetPartitionQuery = "ALTER TABLE wrstat_schema3_snapshot_sets " +
		"DROP PARTITION tuple(?, toUUID(?))"
	dropDirDGUTAVectorPartitionQuery = "ALTER TABLE wrstat_dir_facts " +
		"DROP PARTITION tuple(?, toUUID(?))"

	dropActiveVirtualDirsPartitionQuery      = "ALTER TABLE wrstat_active_virtual_dirs DROP PARTITION ?"
	dropActiveVirtualSummariesPartitionQuery = "ALTER TABLE wrstat_active_virtual_summaries DROP PARTITION ?"
	dropActiveVirtualFilterAllPartitionQuery = "ALTER TABLE wrstat_active_virtual_filter_all DROP PARTITION ?"
	dropActiveVirtualChildrenPartitionQuery  = "ALTER TABLE wrstat_active_virtual_children DROP PARTITION ?"
	dropActiveVirtualSetsPartitionQuery      = "ALTER TABLE wrstat_active_virtual_sets DROP PARTITION ?"

	dropBasedirsGroupUsagePartitionQuery   = "ALTER TABLE wrstat_basedirs_group_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserUsagePartitionQuery    = "ALTER TABLE wrstat_basedirs_user_usage DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsGroupSubdirsPartitionQuery = "ALTER TABLE wrstat_basedirs_group_subdirs DROP PARTITION tuple(?, toUUID(?))"
	dropBasedirsUserSubdirsPartitionQuery  = "ALTER TABLE wrstat_basedirs_user_subdirs DROP PARTITION tuple(?, toUUID(?))"

	countSnapshotTableRowsQuery   = "SELECT count() FROM %s WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	insertSchema3SnapshotSetQuery = "INSERT INTO wrstat_schema3_snapshot_sets " +
		"(mount_path, snapshot_id, schema3_version, dirs_rows, dir_facts_rows, " +
		"child_filter_all_rows, dir_filter_all_rows, manifest_sha256, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?)"
	insertActiveVirtualDirQuery = "INSERT INTO wrstat_active_virtual_dirs " +
		"(active_set_id, virtual_id, parent_id, name, full_path, mount_path, snapshot_id, mount_root_dir_id, " +
		"is_mount_root_box, refreshed_at) VALUES (?, ?, ?, ?, ?, ?, toUUID(?), ?, ?, ?)"
	insertActiveVirtualSummaryQuery = "INSERT INTO wrstat_active_virtual_summaries " +
		"(active_set_id, virtual_id, mount_path, snapshot_id, mount_root_dir_id, is_mount_root_box, " +
		"updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, " +
		"all_ft, file_count, file_size, child_count, refreshed_at) " +
		"VALUES (?, ?, ?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertActiveVirtualFilterAllQuery = "INSERT INTO wrstat_active_virtual_filter_all " +
		"(active_set_id, virtual_id, age, gid, uid, ft, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, filter_child_count, child_count, refreshed_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertActiveVirtualChildQuery = "INSERT INTO wrstat_active_virtual_children " +
		"(active_set_id, parent_virtual_id, child_virtual_id, mount_path, snapshot_id, mount_root_dir_id, " +
		"is_mount_root_box, child_count, refreshed_at) VALUES (?, ?, ?, ?, toUUID(?), ?, ?, ?, ?)"
	insertActiveVirtualSetQuery = "INSERT INTO wrstat_active_virtual_sets " +
		"(active_set_id, schema3_version, mounts_sha256, active_mount_count, summary_rows, filter_rows, " +
		"child_rows, manifest_sha256, ready, refreshed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	selectActiveVirtualSummariesValidationQuery = "SELECT virtual_id, mount_path, toString(snapshot_id), " +
		"mount_root_dir_id, is_mount_root_box, updated_at, " +
		"all_count, all_size, all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, " +
		"all_uids, all_gids, all_ft, file_count, file_size, child_count " +
		"FROM wrstat_active_virtual_summaries WHERE active_set_id = ? ORDER BY virtual_id"
	selectActiveVirtualFilterAllValidationQuery = "SELECT virtual_id, age, gid, uid, ft, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, child_count " +
		"FROM wrstat_active_virtual_filter_all WHERE active_set_id = ? ORDER BY virtual_id, age, gid, uid, ft"
	selectActiveVirtualChildrenValidationQuery = "SELECT parent_virtual_id, child_virtual_id, mount_path, " +
		"toString(snapshot_id), mount_root_dir_id, is_mount_root_box, child_count " +
		"FROM wrstat_active_virtual_children WHERE active_set_id = ? " +
		"ORDER BY parent_virtual_id, child_virtual_id"
)

var (
	errMountPathRequired         = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired         = errors.New("clickhouse: updated at is required")
	errDirRequired               = errors.New("clickhouse: record dir is required")
	errSnapshotCountNoRows       = errors.New("clickhouse: snapshot table count returned no rows")
	errActiveVirtualRowsMismatch = errors.New(
		"clickhouse: active virtual row validation mismatch",
	)
	errActiveSnapshotRewrite = errors.New(
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

type dgutaRecordContext struct {
	rawDir       string
	canonicalDir string
	dirID        uint32
	parentID     uint32
	subtreeEnd   uint32
	depth        uint16
}

func dgutaRecordContextForRecord(mountPath string, dguta db.RecordDGUTA) (string, string, dgutaRecordContext) {
	rawParentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))
	parentDir := canonicalPathForMount(mountPath, rawParentDir)

	return rawParentDir, parentDir, dgutaRecordContext{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		dirID:        dguta.DirID,
		parentID:     dguta.ParentID,
		subtreeEnd:   dguta.SubtreeEnd,
		depth:        dguta.Depth,
	}
}

type dgutaPostFlushDerivedIndexWriter interface {
	derive(ctx context.Context) error
	deriveImportPhase() string
}

func compareActiveVirtualChildRows(a, b activeVirtualChildRow) int {
	if a.ParentVirtualID < b.ParentVirtualID {
		return -1
	}

	if a.ParentVirtualID > b.ParentVirtualID {
		return 1
	}

	if a.ChildVirtualID < b.ChildVirtualID {
		return -1
	}

	if a.ChildVirtualID > b.ChildVirtualID {
		return 1
	}

	return 0
}

func (w *activeVirtualOverlayWriter) appendDir(
	ctx context.Context,
	row activeVirtualDirRow,
) error {
	return w.blockWriter(insertActiveVirtualDirQuery, &w.dirBatch, &w.dirOpenedAt, "active virtual dir").
		append(ctx, func(batch driver.Batch) error {
			return batch.Append(
				row.ActiveSetID,
				row.VirtualID,
				row.ParentID,
				row.Name,
				row.FullPath,
				row.MountPath,
				row.SnapshotID,
				row.MountRootDirID,
				row.IsMountRootBox,
				row.RefreshedAt,
			)
		})
}

func (w *dgutaWriter) withFreshCloseQueryContext(
	parent context.Context,
	fn func(context.Context) error,
) error {
	ctx, cancel := w.freshCloseQueryContext(parent)
	defer cancel()

	return fn(ctx)
}

func (w *dgutaWriter) freshCloseQueryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return queryContext(withoutCancelOrBackground(parent), queryTimeout(w.cfg))
}

func withoutCancelOrBackground(parent context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}

	return context.WithoutCancel(parent)
}

func (w *dgutaWriter) appendCatalogRow(ctx context.Context, dguta db.RecordDGUTA, fullPath string) error {
	err := w.timeImportPhase(importPhaseCatalogInsert, func() error {
		return w.catalog.appendRecord(ctx, w.activeMount(), dguta, fullPath)
	})
	if err != nil {
		w.writeErr = err
	}

	return err
}

func activeSetUpdatedAt(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}

	return t.UTC().Truncate(time.Second)
}

func (w *dgutaWriter) activeVirtualPublishContext(parent context.Context) (context.Context, context.CancelFunc) {
	return queryContext(withoutCancelOrBackground(parent), activeSnapshotCleanupTimeout)
}

func stagedMountsActiveRows(rows []mountsActiveRow, candidate mountsActiveRow) []mountsActiveRow {
	candidate = normalizeActiveSetMountRow(candidate)
	out := make([]mountsActiveRow, 0, len(rows)+1)
	replaced := false

	for _, row := range rows {
		row = normalizeActiveSetMountRow(row)
		if row.mountPath == candidate.mountPath {
			out = append(out, candidate)
			replaced = true

			continue
		}

		out = append(out, row)
	}

	if !replaced {
		out = append(out, candidate)
	}

	return out
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
		record dgutaRecordContext,
		gutas db.GUTAs,
		childCount uint64,
		ages []db.DirGUTAge,
	) error
	flush(ctx context.Context) error
	abort() error
	importPhase() string
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

type schema3SnapshotRowCounts struct {
	dirsRows           uint64
	dirFactsRows       uint64
	childFilterAllRows uint64
	dirFilterAllRows   uint64
}

type activeVirtualSummaryRow struct {
	ActiveSetID     string
	VirtualID       uint32
	Dir             string
	MountPath       string
	SnapshotID      string
	MountRootDirID  uint32
	IsMountRootBox  uint8
	UpdatedAt       time.Time
	AllCount        uint64
	AllSize         uint64
	AllAtimeMin     int64
	AllMtimeMax     int64
	AllAtimeBuckets []uint64
	AllMtimeBuckets []uint64
	AllUIDs         []uint32
	AllGIDs         []uint32
	AllFT           uint16
	FileCount       uint64
	FileSize        uint64
	ChildCount      uint64
	RefreshedAt     time.Time
}

func activeVirtualSummaryRowWithCounts(
	row activeVirtualSummaryRow,
	childCount uint64,
	updatedAt time.Time,
	refreshedAt time.Time,
) activeVirtualSummaryRow {
	row.UpdatedAt = updatedAt
	row.ChildCount = childCount
	row.AllAtimeBuckets = emptyAgeBuckets()
	row.AllMtimeBuckets = emptyAgeBuckets()
	row.RefreshedAt = refreshedAt

	return row
}

func activeVirtualSummaryRowsForChildren(
	activeSetID string,
	mounts []activeMount,
	childRows []activeVirtualChildRow,
	refreshedAt time.Time,
) []activeVirtualSummaryRow {
	dirs, childCounts := activeVirtualSummarySeedRows(activeSetID, mounts, childRows)
	updatedAt := maxUpdatedAtForMounts(mounts)
	out := make([]activeVirtualSummaryRow, 0, len(dirs))

	for _, row := range dirs {
		out = append(out, activeVirtualSummaryRowWithCounts(row, childCounts[row.Dir], updatedAt, refreshedAt))
	}

	sortActiveVirtualSummaryRows(out)

	return out
}

//nolint:funlen
func activeVirtualSummarySeedRows(
	activeSetID string,
	mounts []activeMount,
	childRows []activeVirtualChildRow,
) (map[string]activeVirtualSummaryRow, map[string]uint64) {
	dirs := make(map[string]activeVirtualSummaryRow, len(childRows)+1)
	childCounts := make(map[string]uint64)

	for _, row := range childRows {
		childCounts[row.ParentDir]++
		dirs[row.ParentDir] = activeVirtualSummaryRow{
			ActiveSetID: activeSetID,
			VirtualID:   row.ParentVirtualID,
			Dir:         row.ParentDir,
			SnapshotID:  activeVirtualZeroSnapshot,
		}
		childDir := activeVirtualSummaryDirForChild(row)
		dirs[childDir] = activeVirtualSummaryRow{
			ActiveSetID:    activeSetID,
			VirtualID:      row.ChildVirtualID,
			Dir:            childDir,
			MountPath:      row.MountPath,
			SnapshotID:     row.SnapshotID,
			MountRootDirID: row.MountRootDirID,
			IsMountRootBox: row.IsMountRootBox,
		}
	}

	for _, mount := range mounts {
		dir := ensureTrailingSlash(mount.mountPath)
		row := dirs[dir]
		row.ActiveSetID = activeSetID

		row.Dir = dir
		if row.VirtualID == 0 {
			row.VirtualID = virtualIDForDir(dir)
		}

		row.MountPath = dir
		if row.SnapshotID == "" {
			row.SnapshotID = mount.snapshotID
		}

		row.IsMountRootBox = 1
		dirs[dir] = row
	}

	return dirs, childCounts
}

func activeVirtualSummaryDirForChild(row activeVirtualChildRow) string {
	return ensureTrailingSlash(row.ChildDir)
}

func sortActiveVirtualSummaryRows(rows []activeVirtualSummaryRow) {
	slices.SortFunc(rows, func(a, b activeVirtualSummaryRow) int {
		if a.VirtualID != 0 && b.VirtualID != 0 {
			return cmp.Compare(a.VirtualID, b.VirtualID)
		}

		return strings.Compare(a.Dir, b.Dir)
	})
}

func emptyAgeBuckets() []uint64 {
	return append([]uint64(nil), ageBucketsSlice(nil)...)
}

type activeVirtualFilterAllRow struct {
	ActiveSetID      string
	VirtualID        uint32
	Dir              string
	Age              uint8
	GID              uint32
	UID              uint32
	FT               uint16
	Count            uint64
	Size             uint64
	AtimeMin         int64
	MtimeMax         int64
	AtimeBuckets     []uint64
	MtimeBuckets     []uint64
	FilterChildCount uint64
	ChildCount       uint64
	RefreshedAt      time.Time
}

func activeVirtualFilterSortKey(row activeVirtualFilterAllRow) string {
	prefix := row.Dir
	if row.VirtualID != 0 {
		prefix = fmt.Sprintf("%010d", row.VirtualID)
	}

	return fmt.Sprintf("%s\x00%03d\x00%010d\x00%010d\x00%05d", prefix, row.Age, row.GID, row.UID, row.FT)
}

type activeVirtualChildRow struct {
	ActiveSetID     string
	ParentDir       string
	ChildDir        string
	ParentVirtualID uint32
	ChildVirtualID  uint32
	MountPath       string
	SnapshotID      string
	MountRootDirID  uint32
	IsMountRootBox  uint8
	ChildCount      uint64
	RefreshedAt     time.Time
}

//nolint:unused
func activeVirtualChildRowsForMounts(
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
) []activeVirtualChildRow {
	return activeVirtualNamespaceForMounts(activeSetID, mounts, nil, refreshedAt).childRows(activeSetID, refreshedAt)
}

//nolint:unused
func boolAsUInt8(v bool) uint8 {
	if v {
		return 1
	}

	return 0
}

type activeVirtualSetRow struct {
	ActiveSetID      string
	Schema3Version   uint32
	MountsSHA256     string
	ActiveMountCount uint64
	SummaryRows      uint64
	FilterRows       uint64
	ChildRows        uint64
	ManifestSHA256   string
	Ready            uint8
	RefreshedAt      time.Time
}

func activeVirtualSetRowForRows(
	activeSetID string,
	rows []mountsActiveRow,
	summaryRows []activeVirtualSummaryRow,
	filterRows []activeVirtualFilterAllRow,
	childRows []activeVirtualChildRow,
	refreshedAt time.Time,
) activeVirtualSetRow {
	return activeVirtualSetRow{
		ActiveSetID:      activeSetID,
		Schema3Version:   currentSchemaVersion,
		MountsSHA256:     activeSetID,
		ActiveMountCount: countActiveMountRows(rows),
		SummaryRows:      uint64(len(summaryRows)),
		FilterRows:       uint64(len(filterRows)),
		ChildRows:        uint64(len(childRows)),
		ManifestSHA256:   activeVirtualManifestSHA256(activeSetID, len(summaryRows), len(filterRows), len(childRows)),
		Ready:            1,
		RefreshedAt:      refreshedAt,
	}
}

type activeVirtualValidation struct {
	rows     uint64
	checksum string
}

func activeVirtualSummaryValidation(rows []activeVirtualSummaryRow) activeVirtualValidation {
	sorted := slices.Clone(rows)
	sortActiveVirtualSummaryRows(sorted)

	hash := sha256.New()
	for _, row := range sorted {
		writeActiveVirtualSummaryChecksum(hash, row)
	}

	return activeVirtualValidation{rows: uint64(len(rows)), checksum: hex.EncodeToString(hash.Sum(nil))}
}

func activeVirtualFilterValidation(rows []activeVirtualFilterAllRow) activeVirtualValidation {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b activeVirtualFilterAllRow) int {
		return strings.Compare(activeVirtualFilterSortKey(a), activeVirtualFilterSortKey(b))
	})

	hash := sha256.New()
	for _, row := range sorted {
		writeActiveVirtualFilterChecksum(hash, row)
	}

	return activeVirtualValidation{rows: uint64(len(rows)), checksum: hex.EncodeToString(hash.Sum(nil))}
}

func activeVirtualChildValidation(rows []activeVirtualChildRow) activeVirtualValidation {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, compareActiveVirtualChildRows)

	hash := sha256.New()
	for _, row := range sorted {
		writeActiveVirtualChildChecksum(hash, row)
	}

	return activeVirtualValidation{rows: uint64(len(rows)), checksum: hex.EncodeToString(hash.Sum(nil))}
}

type activeVirtualOverlayWriter struct {
	conn ch.Conn

	dirBatch     driver.Batch
	summaryBatch driver.Batch
	filterBatch  driver.Batch
	childBatch   driver.Batch
	setBatch     driver.Batch

	dirOpenedAt     time.Time
	summaryOpenedAt time.Time
	filterOpenedAt  time.Time
	childOpenedAt   time.Time
	setOpenedAt     time.Time

	batchSize int
	writeErr  error
	now       func() time.Time
}

func newActiveVirtualOverlayWriter(conn ch.Conn, batchSize int) *activeVirtualOverlayWriter {
	return &activeVirtualOverlayWriter{
		conn:      conn,
		batchSize: batchSize,
	}
}

func (w *activeVirtualOverlayWriter) appendSummary(
	ctx context.Context,
	row activeVirtualSummaryRow,
) error {
	return w.blockWriter(insertActiveVirtualSummaryQuery, &w.summaryBatch, &w.summaryOpenedAt, "active virtual summary").
		append(ctx, func(batch driver.Batch) error {
			return batch.Append(
				row.ActiveSetID,
				row.VirtualID,
				row.MountPath,
				row.SnapshotID,
				row.MountRootDirID,
				row.IsMountRootBox,
				row.UpdatedAt,
				row.AllCount,
				row.AllSize,
				row.AllAtimeMin,
				row.AllMtimeMax,
				row.AllAtimeBuckets,
				row.AllMtimeBuckets,
				row.AllUIDs,
				row.AllGIDs,
				row.AllFT,
				row.FileCount,
				row.FileSize,
				row.ChildCount,
				row.RefreshedAt,
			)
		})
}

func (w *activeVirtualOverlayWriter) appendFilterAll(
	ctx context.Context,
	row activeVirtualFilterAllRow,
) error {
	return w.blockWriter(
		insertActiveVirtualFilterAllQuery,
		&w.filterBatch,
		&w.filterOpenedAt,
		"active virtual filter-all",
	).append(ctx, func(batch driver.Batch) error {
		return batch.Append(
			row.ActiveSetID,
			row.VirtualID,
			row.Age,
			row.GID,
			row.UID,
			row.FT,
			row.Count,
			row.Size,
			row.AtimeMin,
			row.MtimeMax,
			row.AtimeBuckets,
			row.MtimeBuckets,
			row.FilterChildCount,
			row.ChildCount,
			row.RefreshedAt,
		)
	})
}

func (w *activeVirtualOverlayWriter) appendChild(
	ctx context.Context,
	row activeVirtualChildRow,
) error {
	return w.blockWriter(insertActiveVirtualChildQuery, &w.childBatch, &w.childOpenedAt, "active virtual child").
		append(ctx, func(batch driver.Batch) error {
			return batch.Append(
				row.ActiveSetID,
				row.ParentVirtualID,
				row.ChildVirtualID,
				row.MountPath,
				row.SnapshotID,
				row.MountRootDirID,
				row.IsMountRootBox,
				row.ChildCount,
				row.RefreshedAt,
			)
		})
}

func (w *activeVirtualOverlayWriter) appendSet(
	ctx context.Context,
	row activeVirtualSetRow,
) error {
	err := w.blockWriter(insertActiveVirtualSetQuery, &w.setBatch, &w.setOpenedAt, "active virtual set").
		append(ctx, func(batch driver.Batch) error {
			return batch.Append(
				row.ActiveSetID,
				row.Schema3Version,
				row.MountsSHA256,
				row.ActiveMountCount,
				row.SummaryRows,
				row.FilterRows,
				row.ChildRows,
				row.ManifestSHA256,
				row.Ready,
				row.RefreshedAt,
			)
		})
	if err != nil {
		return err
	}

	return w.closeBatch(&w.setBatch, &w.setOpenedAt, "active virtual set")
}

func (w *activeVirtualOverlayWriter) flush(ctx context.Context) error {
	_ = ctx

	return errors.Join(
		w.closeBatch(&w.dirBatch, &w.dirOpenedAt, "active virtual dir"),
		w.closeBatch(&w.summaryBatch, &w.summaryOpenedAt, "active virtual summary"),
		w.closeBatch(&w.filterBatch, &w.filterOpenedAt, "active virtual filter-all"),
		w.closeBatch(&w.childBatch, &w.childOpenedAt, "active virtual child"),
	)
}

func (w *activeVirtualOverlayWriter) blockWriter(
	query string,
	batch *driver.Batch,
	openedAt *time.Time,
	name string,
) *importBlockWriter {
	return &importBlockWriter{
		conn:      w.conn,
		query:     query,
		name:      name,
		batch:     batch,
		openedAt:  openedAt,
		writeErr:  &w.writeErr,
		batchSize: w.batchSize,
		now:       w.importBatchNow,
	}
}

func (w *activeVirtualOverlayWriter) closeBatch(
	batch *driver.Batch,
	openedAt *time.Time,
	name string,
) error {
	if batch == nil || *batch == nil {
		return nil
	}

	return (&importBlockWriter{
		name:      name,
		batch:     batch,
		openedAt:  openedAt,
		writeErr:  &w.writeErr,
		batchSize: w.batchSize,
		now:       w.importBatchNow,
	}).close()
}

func (w *activeVirtualOverlayWriter) importBatchNow() time.Time {
	if w != nil && w.now != nil {
		return w.now()
	}

	return time.Now()
}

type dgutaWriter struct {
	cfg Config

	conn ch.Conn

	batchSize           int
	projectionBatchSize int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	prepared bool

	batchNow func() time.Time

	importPhaseRecorder func(string, time.Duration)

	// failBeforeSwitchErr forces Close() to fail before switching snapshots.
	// Used only by integration tests.
	failBeforeSwitchErr error

	stagedActiveSetID string

	previousDGUTARows dgutaRecordRows
	catalog           catalogWriter
	dirProjection     mountDirProjectionWriter

	selectedDerivedIndexes []dgutaDerivedIndexWriter

	closed   bool
	writeErr error
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
		w.projectionBatchSize = projectionBatchSizeFor(batchSize)
	}
}

func projectionBatchSizeFor(batchSize int) int {
	if batchSize <= 0 {
		return defaultProjectionBatchSize
	}

	return min(batchSize, defaultProjectionBatchSize)
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
	rawParentDir, parentDir, record := dgutaRecordContextForRecord(w.mountPath, dguta)
	childCount := max(dguta.ChildCount, uint64(len(dguta.Children)))

	if err := w.appendCatalogRow(ctx, dguta, parentDir); err != nil {
		return err
	}

	appendedGUTAs, err := w.appendDGUTARows(dguta, rawParentDir, parentDir)
	if err != nil {
		return err
	}

	if err := w.appendMountDirProjectionRows(
		ctx,
		record,
		appendedGUTAs,
		childCount,
		w.previousDGUTARows.ages(),
	); err != nil {
		return err
	}

	if err := w.appendSelectedDerivedIndexRows(ctx, record, appendedGUTAs, childCount); err != nil {
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

	if err := w.withFreshCloseQueryContext(ctx, w.writeMountDirProjectionSummarySet); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.withFreshCloseQueryContext(ctx, w.writeSchema3SnapshotReadiness); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.writeActiveVirtualReadiness(ctx); err != nil {
		return w.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := w.withFreshCloseQueryContext(ctx, w.switchSnapshotAndDropOld); err != nil {
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
		activeSetUpdatedAt(w.updatedAt),
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

	previousActiveSetID, nextActiveSetID, err := w.activeSetIDsForSwitch(ctx)
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

	if err := w.dropPreviousActiveVirtualPartitions(ctx, previousActiveSetID, nextActiveSetID); err != nil {
		return err
	}

	w.refreshActiveTreeSummariesBestEffort(ctx)
	w.refreshActivePrefixRollupsBestEffort(ctx)

	return nil
}

func (w *dgutaWriter) activeSetIDsForSwitch(ctx context.Context) (string, string, error) {
	rows, err := queryMountsActiveRows(ctx, w.conn)
	if err != nil {
		return "", "", err
	}

	previousID := fingerprintForMountsActive(rows)

	nextID := w.stagedActiveSetID
	if nextID == "" {
		nextRows := stagedMountsActiveRows(rows, mountsActiveRow(w.activeMount()))
		nextID = fingerprintForMountsActive(nextRows)
	}

	return previousID, nextID, nil
}

func (w *dgutaWriter) refreshActiveTreeSummariesBestEffort(ctx context.Context) {
	if err := w.refreshActiveTreeSummaries(ctx); err != nil {
		return
	}
}

func (w *dgutaWriter) refreshActivePrefixRollupsBestEffort(ctx context.Context) {
	if err := w.refreshActivePrefixRollups(ctx); err != nil {
		return
	}
}

func (w *dgutaWriter) writeMountDirProjectionSummarySet(ctx context.Context) error {
	w.ensureSnapshotID()

	return w.timeImportPhase(importPhaseDirProjectionWrite, func() error {
		return writeMountDirSummarySetRow(ctx, w.conn, w.activeMount(), w.dirProjection.refreshedAt)
	})
}

func (w *dgutaWriter) writeSchema3SnapshotReadiness(ctx context.Context) error {
	w.ensureSnapshotID()

	return w.timeImportPhase(importPhaseSchema3Ready, func() error {
		counts, err := w.schema3SnapshotRowCounts(ctx)
		if err != nil {
			return err
		}

		return w.insertSchema3SnapshotSet(ctx, counts)
	})
}

func (w *dgutaWriter) schema3SnapshotRowCounts(ctx context.Context) (schema3SnapshotRowCounts, error) {
	var counts schema3SnapshotRowCounts

	for _, table := range []struct {
		name string
		dest *uint64
	}{
		{name: "wrstat_dirs", dest: &counts.dirsRows},
		{name: "wrstat_dir_facts", dest: &counts.dirFactsRows},
		{name: "wrstat_child_filter_all", dest: &counts.childFilterAllRows},
		{name: "wrstat_dir_filter_all", dest: &counts.dirFilterAllRows},
	} {
		n, err := w.countSnapshotRows(ctx, table.name)
		if err != nil {
			return schema3SnapshotRowCounts{}, err
		}

		*table.dest = n
	}

	return counts, nil
}

func (w *dgutaWriter) countSnapshotRows(ctx context.Context, table string) (uint64, error) {
	rows, err := w.conn.Query(
		ctx,
		fmt.Sprintf(countSnapshotTableRowsQuery, table),
		w.mountPath,
		w.snapshot.String(),
	)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: failed to count %s rows: %w", table, err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("clickhouse: %s count iteration error: %w", table, err)
		}

		return 0, fmt.Errorf("%w: %s", errSnapshotCountNoRows, table)
	}

	var count uint64
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("clickhouse: failed to scan %s row count: %w", table, err)
	}

	return count, nil
}

func (w *dgutaWriter) insertSchema3SnapshotSet(ctx context.Context, counts schema3SnapshotRowCounts) error {
	manifest := schema3SnapshotManifestSHA256(w.activeMount(), counts)
	if err := w.conn.Exec(
		ctx,
		insertSchema3SnapshotSetQuery,
		w.mountPath,
		w.snapshot.String(),
		currentSchemaVersion,
		counts.dirsRows,
		counts.dirFactsRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
		manifest,
		w.dirProjection.refreshedAt,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to write schema3 snapshot readiness: %w", err)
	}

	return nil
}

func schema3SnapshotManifestSHA256(mount activeMount, counts schema3SnapshotRowCounts) string {
	input := fmt.Sprintf(
		"%s|%s|%d|%d|%d|%d|%d",
		mount.mountPath,
		mount.snapshotID,
		currentSchemaVersion,
		counts.dirsRows,
		counts.dirFactsRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
	)

	return sha256Hex(input)
}

func (w *dgutaWriter) writeActiveVirtualReadiness(ctx context.Context) error {
	ctx, cancel := w.activeVirtualPublishContext(ctx)
	defer cancel()

	return w.timeImportPhase(importPhaseActiveVirtualInsert, func() error {
		rows, err := w.stagedMountsActiveRows(ctx)
		if err != nil {
			return err
		}

		activeSetID := fingerprintForMountsActive(rows)

		w.stagedActiveSetID = activeSetID
		if activeSetID == "" {
			return nil
		}

		dropErr := w.dropActiveVirtualPartitions(ctx, activeSetID)
		if dropErr != nil {
			return dropErr
		}

		setRow, err := w.writeActiveVirtualOverlay(ctx, rows, activeSetID)
		if err != nil {
			return err
		}

		return w.timeImportPhase(importPhaseActiveVirtualReady, func() error {
			return newActiveVirtualOverlayWriter(w.conn, w.effectiveProjectionBatchSize()).appendSet(ctx, setRow)
		})
	})
}

func (w *dgutaWriter) stagedMountsActiveRows(ctx context.Context) ([]mountsActiveRow, error) {
	rows, err := queryMountsActiveRows(ctx, w.conn)
	if err != nil {
		return nil, err
	}

	return stagedMountsActiveRows(rows, mountsActiveRow(w.activeMount())), nil
}

//nolint:funlen,gocyclo
func (w *dgutaWriter) writeActiveVirtualOverlay(
	ctx context.Context,
	rows []mountsActiveRow,
	activeSetID string,
) (activeVirtualSetRow, error) {
	refreshedAt := w.dirProjection.refreshedAt
	if refreshedAt.IsZero() {
		refreshedAt = time.Now().UTC()
	}

	mounts := newActiveMountsSnapshot(rows).all()
	writer := newActiveVirtualOverlayWriter(w.conn, w.effectiveProjectionBatchSize())

	rootGUTAs, err := queryActiveVirtualRootGUTAs(ctx, w.conn, mounts)
	if err != nil {
		return activeVirtualSetRow{}, err
	}

	rootFilterRows, err := queryActiveVirtualRootFilterRows(ctx, w.conn, mounts)
	if err != nil {
		return activeVirtualSetRow{}, err
	}

	mountRootLinks, err := queryActiveVirtualMountRootLinks(ctx, w.conn, mounts)
	if err != nil {
		return activeVirtualSetRow{}, err
	}

	namespace, summaryRows, filterRows, childRows := activeVirtualRowsForMountsFromDataWithLinks(
		activeSetID,
		mounts,
		refreshedAt,
		rootGUTAs,
		rootFilterRows,
		mountRootLinks,
	)

	if err := appendActiveVirtualOverlayRows(ctx, writer, namespace.rows, summaryRows, filterRows, childRows); err != nil {
		return activeVirtualSetRow{}, err
	}

	if err := writer.flush(ctx); err != nil {
		return activeVirtualSetRow{}, err
	}

	if err := w.validateActiveVirtualOverlay(ctx, activeSetID, summaryRows, filterRows, childRows); err != nil {
		return activeVirtualSetRow{}, err
	}

	return activeVirtualSetRowForRows(activeSetID, rows, summaryRows, filterRows, childRows, refreshedAt), nil
}

//nolint:funlen,gocognit,gocyclo
func appendActiveVirtualOverlayRows(
	ctx context.Context,
	writer *activeVirtualOverlayWriter,
	dirRows []activeVirtualDirRow,
	summaryRows []activeVirtualSummaryRow,
	filterRows []activeVirtualFilterAllRow,
	childRows []activeVirtualChildRow,
) error {
	for _, row := range dirRows {
		if err := writer.appendDir(ctx, row); err != nil {
			return err
		}
	}

	for _, row := range summaryRows {
		if err := writer.appendSummary(ctx, row); err != nil {
			return err
		}
	}

	for _, row := range filterRows {
		if err := writer.appendFilterAll(ctx, row); err != nil {
			return err
		}
	}

	for _, row := range childRows {
		if err := writer.appendChild(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) validateActiveVirtualOverlay( //nolint:funlen
	ctx context.Context,
	activeSetID string,
	summaryRows []activeVirtualSummaryRow,
	filterRows []activeVirtualFilterAllRow,
	childRows []activeVirtualChildRow,
) error {
	if err := w.validateActiveVirtualCheck(
		ctx,
		activeSetID,
		activeVirtualSummaryName,
		activeVirtualSummaryValidation(summaryRows),
		w.readActiveVirtualSummaryValidation,
	); err != nil {
		return err
	}

	if err := w.validateActiveVirtualCheck(
		ctx,
		activeSetID,
		activeVirtualFilterName,
		activeVirtualFilterValidation(filterRows),
		w.readActiveVirtualFilterValidation,
	); err != nil {
		return err
	}

	return w.validateActiveVirtualCheck(
		ctx,
		activeSetID,
		activeVirtualChildName,
		activeVirtualChildValidation(childRows),
		w.readActiveVirtualChildValidation,
	)
}

func (w *dgutaWriter) validateActiveVirtualCheck(
	ctx context.Context,
	activeSetID string,
	name string,
	expected activeVirtualValidation,
	read func(context.Context, string) (activeVirtualValidation, error),
) error {
	actual, err := read(ctx, activeSetID)
	if err != nil {
		return err
	}

	if actual == expected {
		return nil
	}

	return fmt.Errorf(
		"%w: %s rows expected=%d/%s actual=%d/%s",
		errActiveVirtualRowsMismatch,
		name,
		expected.rows,
		expected.checksum,
		actual.rows,
		actual.checksum,
	)
}

//nolint:funlen
func (w *dgutaWriter) readActiveVirtualSummaryValidation(
	ctx context.Context,
	activeSetID string,
) (activeVirtualValidation, error) {
	rows, err := w.conn.Query(ctx, selectActiveVirtualSummariesValidationQuery, activeSetID)
	if err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to validate active virtual summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	hash := sha256.New()

	var count uint64

	for rows.Next() {
		row := activeVirtualSummaryRow{ActiveSetID: activeSetID}
		if err := rows.Scan(
			&row.VirtualID,
			&row.MountPath,
			&row.SnapshotID,
			&row.MountRootDirID,
			&row.IsMountRootBox,
			&row.UpdatedAt,
			&row.AllCount,
			&row.AllSize,
			&row.AllAtimeMin,
			&row.AllMtimeMax,
			&row.AllAtimeBuckets,
			&row.AllMtimeBuckets,
			&row.AllUIDs,
			&row.AllGIDs,
			&row.AllFT,
			&row.FileCount,
			&row.FileSize,
			&row.ChildCount,
		); err != nil {
			return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to scan active virtual summary: %w", err)
		}

		writeActiveVirtualSummaryChecksum(hash, row)

		count++
	}

	if err := rows.Err(); err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: active virtual summary validation iteration: %w", err)
	}

	return activeVirtualValidation{rows: count, checksum: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writeActiveVirtualSummaryChecksum(hash hashWriter, row activeVirtualSummaryRow) {
	writeChecksumFields(
		hash,
		row.ActiveSetID,
		row.VirtualID,
		row.MountPath,
		row.SnapshotID,
		row.MountRootDirID,
		row.IsMountRootBox,
		checksumTime(row.UpdatedAt),
		row.AllCount,
		row.AllSize,
		row.AllAtimeMin,
		row.AllMtimeMax,
		row.AllAtimeBuckets,
		row.AllMtimeBuckets,
		row.AllUIDs,
		row.AllGIDs,
		row.AllFT,
		row.FileCount,
		row.FileSize,
		row.ChildCount,
	)
}

//nolint:funlen
func (w *dgutaWriter) readActiveVirtualFilterValidation(
	ctx context.Context,
	activeSetID string,
) (activeVirtualValidation, error) {
	rows, err := w.conn.Query(ctx, selectActiveVirtualFilterAllValidationQuery, activeSetID)
	if err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to validate active virtual filter rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	hash := sha256.New()

	var count uint64

	for rows.Next() {
		row := activeVirtualFilterAllRow{ActiveSetID: activeSetID}
		if err := rows.Scan(
			&row.VirtualID,
			&row.Age,
			&row.GID,
			&row.UID,
			&row.FT,
			&row.Count,
			&row.Size,
			&row.AtimeMin,
			&row.MtimeMax,
			&row.AtimeBuckets,
			&row.MtimeBuckets,
			&row.FilterChildCount,
			&row.ChildCount,
		); err != nil {
			return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to scan active virtual filter row: %w", err)
		}

		writeActiveVirtualFilterChecksum(hash, row)

		count++
	}

	if err := rows.Err(); err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: active virtual filter validation iteration: %w", err)
	}

	return activeVirtualValidation{rows: count, checksum: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writeActiveVirtualFilterChecksum(hash hashWriter, row activeVirtualFilterAllRow) {
	writeChecksumFields(
		hash,
		row.ActiveSetID,
		row.VirtualID,
		row.Age,
		row.GID,
		row.UID,
		row.FT,
		row.Count,
		row.Size,
		row.AtimeMin,
		row.MtimeMax,
		row.AtimeBuckets,
		row.MtimeBuckets,
		row.FilterChildCount,
		row.ChildCount,
	)
}

//nolint:funlen
func (w *dgutaWriter) readActiveVirtualChildValidation(
	ctx context.Context,
	activeSetID string,
) (activeVirtualValidation, error) {
	rows, err := w.conn.Query(ctx, selectActiveVirtualChildrenValidationQuery, activeSetID)
	if err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to validate active virtual children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	hash := sha256.New()

	var count uint64

	for rows.Next() {
		row := activeVirtualChildRow{ActiveSetID: activeSetID}
		if err := rows.Scan(
			&row.ParentVirtualID,
			&row.ChildVirtualID,
			&row.MountPath,
			&row.SnapshotID,
			&row.MountRootDirID,
			&row.IsMountRootBox,
			&row.ChildCount,
		); err != nil {
			return activeVirtualValidation{}, fmt.Errorf("clickhouse: failed to scan active virtual child: %w", err)
		}

		writeActiveVirtualChildChecksum(hash, row)

		count++
	}

	if err := rows.Err(); err != nil {
		return activeVirtualValidation{}, fmt.Errorf("clickhouse: active virtual child validation iteration: %w", err)
	}

	return activeVirtualValidation{rows: count, checksum: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writeActiveVirtualChildChecksum(hash hashWriter, row activeVirtualChildRow) {
	writeChecksumFields(
		hash,
		row.ActiveSetID,
		row.ParentVirtualID,
		row.ChildVirtualID,
		row.MountPath,
		row.SnapshotID,
		row.MountRootDirID,
		row.IsMountRootBox,
		row.ChildCount,
	)
}

func (w *dgutaWriter) activeMount() activeMount {
	return activeMount{
		mountPath:  w.mountPath,
		snapshotID: w.snapshot.String(),
		updatedAt:  activeSetUpdatedAt(w.updatedAt),
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

func (w *dgutaWriter) refreshActivePrefixRollups(ctx context.Context) error {
	return w.timeImportPhase(importPhaseActivePrefixRefresh, func() error {
		return refreshCurrentActivePrefixRollups(ctx, w.conn)
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

	cleanupErr := errors.Join(
		w.dropCurrentSnapshotPartitionsIfInactive(cleanupCtx),
		w.dropStagedActiveVirtualPartitions(cleanupCtx),
	)
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

func (w *dgutaWriter) dropStagedActiveVirtualPartitions(ctx context.Context) error {
	if w.stagedActiveSetID == "" {
		return nil
	}

	return w.dropActiveVirtualPartitions(ctx, w.stagedActiveSetID)
}

func (w *dgutaWriter) dropActiveVirtualPartitions(ctx context.Context, activeSetID string) error {
	for _, query := range activeVirtualPartitionDropQueries() {
		if err := dropActiveSetPartition(ctx, w.conn, query, activeSetID, "active-virtual"); err != nil {
			return err
		}
	}

	return nil
}

func activeVirtualPartitionDropQueries() []string {
	return []string{
		dropActiveVirtualDirsPartitionQuery,
		dropActiveVirtualSummariesPartitionQuery,
		dropActiveVirtualFilterAllPartitionQuery,
		dropActiveVirtualChildrenPartitionQuery,
		dropActiveVirtualSetsPartitionQuery,
	}
}

func (w *dgutaWriter) snapshotCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}

	return configQueryContext(w.cfg)
}

func (w *dgutaWriter) abortAllBatches() error {
	return errors.Join(
		w.catalog.abort(),
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
		dropDirsPartitionQuery,
		dropFilesPartitionQuery,
		dropDirSummaryPartitionQuery,
		dropDirFilterAgeAllPartitionQuery,
		dropChildFilterAllPartitionQuery,
		dropDirFilterAllPartitionQuery,
		dropSchema3SnapshotSetPartitionQuery,
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
	w.catalog = *newCatalogWriter(w.conn, w.effectiveProjectionBatchSize())
	w.dirProjection = prepareMountDirProjectionWriter(ctx, w.conn)
	w.selectedDerivedIndexes = append(w.selectedDerivedIndexes, newDirFilterAgeAllWriter(
		w.conn,
		w.effectiveProjectionBatchSize(),
		w.dirProjection.refreshedAt,
	))
	w.selectedDerivedIndexes = append(w.selectedDerivedIndexes, newFullFilterAllWriter(
		w.conn,
		w.effectiveProjectionBatchSize(),
		w.dirProjection.refreshedAt,
	))
	w.prepared = true

	return nil
}

func (w *dgutaWriter) dropNewSnapshotPartitions(ctx context.Context) error {
	if err := dropSnapshotPartitionsForMount(
		ctx,
		w.conn,
		w.mountPath,
		w.snapshot.String(),
		allPartitionDropQueries(),
	); err != nil {
		return err
	}

	rows, err := w.stagedMountsActiveRows(ctx)
	if err != nil {
		return err
	}

	activeSetID := fingerprintForMountsActive(rows)
	if activeSetID == "" {
		return nil
	}

	return w.dropActiveVirtualPartitions(ctx, activeSetID)
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

func (w *dgutaWriter) appendMountDirProjectionRows(
	ctx context.Context,
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
	recordAges []db.DirGUTAge,
) error {
	err := w.timeImportPhase(importPhaseDirProjectionWrite, func() error {
		return w.dirProjection.appendRecordWithContext(
			ctx,
			w.activeMount(),
			record,
			gutas,
			childCount,
			recordAges,
			w.compactInternalDGUTAAges(record.canonicalDir),
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
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
) error {
	for _, writer := range w.selectedDerivedIndexes {
		err := w.appendSelectedDerivedIndexRow(ctx, writer, record, gutas, childCount)
		if err != nil {
			w.writeErr = err

			return err
		}
	}

	return nil
}

func (w *dgutaWriter) appendSelectedDerivedIndexRow(
	ctx context.Context,
	writer dgutaDerivedIndexWriter,
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
) error {
	appendRecord := func() error {
		return writer.appendRecord(
			ctx,
			w.activeMount(),
			record,
			gutas,
			childCount,
			w.previousDGUTARows.ages(),
		)
	}

	if phase := writer.importPhase(); phase != "" {
		return w.timeImportPhase(phase, appendRecord)
	}

	return appendRecord()
}

func (w *dgutaWriter) flushFullBatches() error {
	if err := w.catalog.sendFullBatchIfFull(); err != nil {
		return err
	}

	if err := w.dirProjection.sendFullBatchIfFull(
		&w.dirProjection.summaryBatch,
		&w.dirProjection.summaryOpenedAt,
		w.effectiveProjectionBatchSize(),
		"dir facts",
	); err != nil {
		return err
	}

	return nil
}

func (w *dgutaWriter) flushAllBatchesWithContext(ctx context.Context) error {
	if err := w.catalog.flush(ctx); err != nil {
		return err
	}

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

func (w *dgutaWriter) batchSlots() [1]dgutaBatchSlot {
	return [...]dgutaBatchSlot{
		{
			batch:     &w.dirProjection.summaryBatch,
			openedAt:  &w.dirProjection.summaryOpenedAt,
			flushed:   &w.dirProjection.summaryFlushed,
			batchSize: w.effectiveProjectionBatchSize(),
			phase:     importPhaseDirProjectionWrite,
			name:      "dir facts",
		},
	}
}

func (w *dgutaWriter) flushSelectedDerivedIndexes(ctx context.Context) error {
	for _, writer := range w.selectedDerivedIndexes {
		if err := w.flushSelectedDerivedIndex(ctx, writer); err != nil {
			w.writeErr = err

			return err
		}
	}

	return nil
}

func (w *dgutaWriter) flushSelectedDerivedIndex(
	ctx context.Context,
	writer dgutaDerivedIndexWriter,
) error {
	flush := func() error {
		return writer.flush(ctx)
	}

	if err := w.flushDerivedIndexPhase(writer.importPhase(), flush); err != nil {
		return err
	}

	deriver, ok := writer.(dgutaPostFlushDerivedIndexWriter)
	if !ok {
		return nil
	}

	derive := func() error {
		return deriver.derive(ctx)
	}

	if phase := deriver.deriveImportPhase(); phase != "" {
		return w.timeImportPhase(phase, derive)
	}

	return derive()
}

func (w *dgutaWriter) flushDerivedIndexPhase(phase string, flush func() error) error {
	if phase == "" {
		return flush()
	}

	return w.timeImportPhase(phase, flush)
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
	}, nil
}

type hashWriter interface {
	Write(p []byte) (n int, err error)
}

func writeChecksumFields(hash hashWriter, fields ...any) {
	for _, field := range fields {
		if _, err := fmt.Fprint(hash, field); err != nil {
			panic(err)
		}

		writeChecksumBytes(hash, []byte{0})
	}

	writeChecksumBytes(hash, []byte{'\n'})
}

func writeChecksumBytes(hash hashWriter, value []byte) {
	if _, err := hash.Write(value); err != nil {
		panic(err)
	}
}

func normalizeActiveSetMountRow(row mountsActiveRow) mountsActiveRow {
	row.updatedAt = activeSetUpdatedAt(row.updatedAt)

	return row
}

func checksumTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
}

func activeVirtualManifestSHA256(activeSetID string, summaryRows, filterRows, childRows int) string {
	return sha256Hex(fmt.Sprintf(
		"%s|%d|%d|%d|%d",
		activeSetID,
		currentSchemaVersion,
		summaryRows,
		filterRows,
		childRows,
	))
}

func sha256Hex(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
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
