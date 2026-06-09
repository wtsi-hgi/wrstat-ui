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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	activePrefixMountReadinessMaxWait      = 500 * time.Millisecond
	activePrefixMountReadinessPollInterval = 25 * time.Millisecond
	activePrefixRollupReadyQuery           = "SELECT 1 FROM wrstat_active_prefix_rollup_sets " +
		"WHERE active_set_id = ? LIMIT 1"
	activePrefixDirFactsReadyQuery = "SELECT mount_path, toString(snapshot_id) FROM wrstat_dir_facts " +
		"WHERE dir = ? AND %s GROUP BY mount_path, snapshot_id"
	activePrefixRootFactsReadyQuery = "SELECT mount_path, toString(snapshot_id) FROM wrstat_dir_facts " +
		"WHERE %s GROUP BY mount_path, snapshot_id"
	activePrefixDirAgeAllReadyQuery = "SELECT mount_path, toString(snapshot_id) FROM wrstat_dir_filter_ageall " +
		"WHERE dir = ? AND %s GROUP BY mount_path, snapshot_id"
	activePrefixRootAgeAllReadyQuery = "SELECT mount_path, toString(snapshot_id) FROM wrstat_dir_filter_ageall " +
		"WHERE %s GROUP BY mount_path, snapshot_id"
	activePrefixScalarRollupReadQuery = "SELECT updated_at, toUInt8(0) AS age, all_count AS count, " +
		"all_size AS size, all_atime_min AS atime_min, all_mtime_max AS mtime_max, " +
		"all_atime_buckets AS atime_buckets, all_mtime_buckets AS mtime_buckets, " +
		"all_uids AS uids, all_gids AS gids, all_ft AS ft " +
		"FROM wrstat_active_prefix_rollups PREWHERE active_set_id = ? WHERE dir = ? LIMIT 1"
	insertActivePrefixRollupQuery = "INSERT INTO wrstat_active_prefix_rollups " +
		"(active_set_id, dir, updated_at, all_count, all_size, all_atime_min, all_mtime_max, " +
		"all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, " +
		"file_count, file_size, child_count, refreshed_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	activePrefixAgeAllRowsQuery = "SELECT gid, uid, ft, sum(count), sum(size), " +
		"minIf(atime_min, atime_min != 0), max(mtime_max), " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) " +
		"FROM wrstat_dir_filter_ageall WHERE dir = ? AND %s GROUP BY gid, uid, ft ORDER BY gid, uid, ft"
	activePrefixRootAgeAllRowsQuery = "SELECT gid, uid, ft, sum(count), sum(size), " +
		"minIf(atime_min, atime_min != 0), max(mtime_max), " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) " +
		"FROM wrstat_dir_filter_ageall WHERE %s GROUP BY gid, uid, ft ORDER BY gid, uid, ft"
	activePrefixAgeAllSummaryReadQuery = "SELECT dir, count() AS raw_rows, " +
		"sum(count) AS total_count, sum(size) AS total_size, " +
		"minIf(atime_min, atime_min != 0) AS atime_min, max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, arraySort(groupUniqArray(gid)) AS gids, " +
		"groupBitOr(p.ft) AS ft FROM wrstat_active_prefix_filter_ageall AS p " +
		"PREWHERE active_set_id = ? AND dir = ? WHERE %s GROUP BY dir"
	insertActivePrefixAgeAllQuery = "INSERT INTO wrstat_active_prefix_filter_ageall " +
		"(active_set_id, dir, gid, uid, ft, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, refreshed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertActivePrefixRollupSetQuery = "INSERT INTO wrstat_active_prefix_rollup_sets " +
		"(active_set_id, active_mount_count, prefix_count, refreshed_at) VALUES (?, ?, ?, ?)"
	activePrefixRollupSetIDsQuery = "SELECT DISTINCT active_set_id FROM wrstat_active_prefix_rollup_sets " +
		"WHERE active_set_id != ?"
	dropActivePrefixRollupPartitionQuery = "ALTER TABLE wrstat_active_prefix_rollups DROP PARTITION ?"
	dropActivePrefixAgeAllPartitionQuery = "ALTER TABLE wrstat_active_prefix_filter_ageall DROP PARTITION ?"
	dropActivePrefixSetPartitionQuery    = "ALTER TABLE wrstat_active_prefix_rollup_sets DROP PARTITION ?"
)

var activePrefixRollupMissCounter atomic.Uint64 //nolint:gochecknoglobals

func activePrefixRollupMisses() uint64 {
	return activePrefixRollupMissCounter.Load()
}

func resetActivePrefixRollupMissesForTest() {
	activePrefixRollupMissCounter.Store(0)
}

type activePrefixAgeAllRow struct {
	gid          uint32
	uid          uint32
	ft           uint16
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func activePrefixAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) ([]activePrefixAgeAllRow, error) {
	if len(mounts) == 0 {
		return nil, nil
	}

	ready, err := queryReadyActivePrefixAgeAllRows(ctx, conn, dir, mounts)
	if err != nil {
		return nil, err
	}

	dirRows, err := activePrefixAgeAllRowsForDir(ctx, conn, dir, mounts)
	if err != nil {
		return nil, err
	}

	missing := missingActiveMounts(mounts, ready)
	if len(missing) == 0 {
		return dirRows, nil
	}

	rootRows, err := activePrefixRootAgeAllRows(ctx, conn, missing)
	if err != nil {
		return nil, err
	}

	return append(dirRows, rootRows...), nil
}

func queryReadyActivePrefixAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	query, args := activeMountsQuery(
		activePrefixDirAgeAllReadyQuery,
		"mount_path",
		"snapshot_id",
		mounts,
		dir,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix AgeAll readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanReadyActiveMountRows(rows)
}

func missingActiveMounts(mounts []activeMount, ready map[treeMountCacheKey]bool) []activeMount {
	missing := make([]activeMount, 0)

	for _, mount := range mounts {
		if ready[newTreeMountCacheKey(mount.mountPath, mount.snapshotID)] {
			continue
		}

		missing = append(missing, mount)
	}

	return missing
}

func activePrefixAgeAllRowsForDir(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) ([]activePrefixAgeAllRow, error) {
	query, args := activeMountsQuery(
		activePrefixAgeAllRowsQuery,
		"mount_path",
		"snapshot_id",
		mounts,
		dir,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix AgeAll rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActivePrefixAgeAllRows(rows)
}

func activePrefixRootAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) ([]activePrefixAgeAllRow, error) {
	condition, args := activeMountRootDirTuplesCondition(
		"mount_path",
		"snapshot_id",
		"dir",
		mounts,
	)
	query := fmt.Sprintf(activePrefixRootAgeAllRowsQuery, condition)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix root AgeAll rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActivePrefixAgeAllRows(rows)
}

func scanActivePrefixAgeAllRows(rows driver.Rows) ([]activePrefixAgeAllRow, error) {
	out := make([]activePrefixAgeAllRow, 0)

	for rows.Next() {
		var row activePrefixAgeAllRow
		if err := rows.Scan(
			&row.gid,
			&row.uid,
			&row.ft,
			&row.count,
			&row.size,
			&row.atimeMin,
			&row.mtimeMax,
			&row.atimeBuckets,
			&row.mtimeBuckets,
		); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active-prefix AgeAll row: %w", err)
		}

		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: active-prefix AgeAll iteration error: %w", err)
	}

	return out, nil
}

func appendActivePrefixAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	batch *driver.Batch,
	activeSetID string,
	dir string,
	rows []activePrefixAgeAllRow,
	refreshedAt time.Time,
) error {
	for _, row := range rows {
		if *batch == nil {
			prepared, err := conn.PrepareBatch(ctx, insertActivePrefixAgeAllQuery)
			if err != nil {
				return fmt.Errorf("clickhouse: failed to prepare active-prefix AgeAll batch: %w", err)
			}

			*batch = prepared
		}

		if err := (*batch).Append(activePrefixAgeAllValues(activeSetID, dir, row, refreshedAt)...); err != nil {
			return fmt.Errorf("clickhouse: failed to append active-prefix AgeAll row: %w", err)
		}
	}

	return nil
}

func activePrefixAgeAllValues(
	activeSetID string,
	dir string,
	row activePrefixAgeAllRow,
	refreshedAt time.Time,
) []any {
	return []any{
		activeSetID,
		dir,
		row.gid,
		row.uid,
		row.ft,
		row.count,
		row.size,
		row.atimeMin,
		row.mtimeMax,
		row.atimeBuckets,
		row.mtimeBuckets,
		refreshedAt,
	}
}

func refreshActivePrefixRollupsForActiveSet(ctx context.Context, conn ch.Conn, activeSetID string) error {
	rows, err := queryMountsActiveRows(ctx, conn)
	if err != nil || activeSetID == "" {
		return err
	}

	if fingerprintForMountsActive(rows) != activeSetID {
		return nil
	}

	if err := ensureActivePrefixRollups(ctx, conn, rows); err != nil {
		return err
	}

	return cleanupOldActivePrefixRollupSets(ctx, conn, activeSetID)
}

func refreshCurrentActivePrefixRollupsBestEffort(ctx context.Context, conn ch.Conn) {
	if err := refreshCurrentActivePrefixRollups(ctx, conn); err != nil {
		return
	}
}

func refreshCurrentActivePrefixRollups(ctx context.Context, conn ch.Conn) error {
	rows, err := queryMountsActiveRows(ctx, conn)
	if err != nil {
		return err
	}

	activeSetID := fingerprintForMountsActive(rows)
	if activeSetID == "" {
		return nil
	}

	if err := ensureActivePrefixRollups(ctx, conn, rows); err != nil {
		return err
	}

	return cleanupOldActivePrefixRollupSets(ctx, conn, activeSetID)
}

func ensureActivePrefixRollups(ctx context.Context, conn ch.Conn, rows []mountsActiveRow) error {
	activeSetID := fingerprintForMountsActive(rows)

	refresh, err := shouldRefreshActivePrefixRollups(ctx, conn, rows, activeSetID)
	if err != nil || !refresh {
		return err
	}

	err = refreshActivePrefixRollups(ctx, conn, rows, activeSetID)
	if err != nil && isUnknownTable(err) {
		return nil
	}

	return err
}

func shouldRefreshActivePrefixRollups(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	activeSetID string,
) (bool, error) {
	if activeSetID == "" {
		return false, nil
	}

	ready, err := activePrefixRollupsReady(ctx, conn, activeSetID)
	if err != nil || ready {
		return false, err
	}

	return activePrefixRollupMountsReady(ctx, conn, rows)
}

func activePrefixRollupProjectionState(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (mountDirProjectionState, error) {
	state, err := treeSummaryProjectionState(ctx, conn, dir, mounts)
	if err != nil {
		return mountDirProjectionState{}, err
	}

	if activePrefixNeedsRootFacts(state, dir) {
		if err := addActivePrefixRootGUTAs(ctx, conn, dir, mounts, &state); err != nil {
			return mountDirProjectionState{}, err
		}
	}

	return state, nil
}

func activePrefixNeedsRootFacts(state mountDirProjectionState, dir string) bool {
	return len(state.vectors[dir]) == 0
}

func addActivePrefixRootGUTAs(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
	state *mountDirProjectionState,
) error {
	query, args := activeMountRootDirTuplesQuery(mounts)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query active-prefix root facts: %w", err)
	}

	defer func() { _ = rows.Close() }()

	gutasByRoot, err := scanDGUTARowsByDir(rows)
	if err != nil {
		return err
	}

	state.addGUTAs(dir, activeMountRootGUTAs(mounts, gutasByRoot))

	return nil
}

func cleanupOldActivePrefixRollupSets(ctx context.Context, conn ch.Conn, keepActiveSetID string) error {
	oldSetIDs, err := activePrefixRollupSetIDsExcept(ctx, conn, keepActiveSetID)
	if err != nil {
		return err
	}

	for _, activeSetID := range oldSetIDs {
		if err := dropActivePrefixRollupPartitions(ctx, conn, activeSetID); err != nil {
			return err
		}
	}

	return nil
}

func activePrefixRollupSetIDsExcept(
	ctx context.Context,
	conn ch.Conn,
	keepActiveSetID string,
) ([]string, error) {
	rows, err := conn.Query(ctx, activePrefixRollupSetIDsQuery, keepActiveSetID)
	if err != nil {
		if isUnknownTable(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("clickhouse: failed to query old active-prefix rollup sets: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActivePrefixRollupSetIDs(rows)
}

func scanActivePrefixRollupSetIDs(rows driver.Rows) ([]string, error) {
	setIDs := make([]string, 0)

	for rows.Next() {
		var activeSetID string
		if err := rows.Scan(&activeSetID); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active-prefix rollup set: %w", err)
		}

		setIDs = append(setIDs, activeSetID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: active-prefix rollup set iteration error: %w", err)
	}

	return setIDs, nil
}

func dropActivePrefixRollupPartitions(ctx context.Context, conn ch.Conn, activeSetID string) error {
	for _, query := range []string{
		dropActivePrefixRollupPartitionQuery,
		dropActivePrefixAgeAllPartitionQuery,
		dropActivePrefixSetPartitionQuery,
	} {
		if err := dropActiveSetPartition(ctx, conn, query, activeSetID, "active-prefix"); err != nil {
			return err
		}
	}

	return nil
}

func activePrefixScalarRollupOrFallback(
	ctx context.Context,
	conn ch.Conn,
	activeSetID, dir string,
	fallback func() (*db.DirSummary, error),
) (*db.DirSummary, error) {
	ready, err := activePrefixRollupsReady(ctx, conn, activeSetID)
	if err != nil {
		return nil, err
	}

	if !ready {
		return activePrefixRollupFallback(fallback)
	}

	sum, found, err := readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)
	if err != nil {
		return nil, err
	}

	if !found {
		return activePrefixRollupFallback(fallback)
	}

	return sum, nil
}

func activePrefixDirSummary(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, bool, error) {
	if conn == nil || activeSetID == "" || !activePrefixCanHandleFilter(filter) {
		return nil, false, false, nil
	}

	ready, err := activePrefixRollupsReady(ctx, conn, activeSetID)
	if err != nil {
		return nil, false, false, err
	}

	if !ready {
		return nil, false, true, nil
	}

	return readReadyActivePrefixDirSummary(ctx, conn, activeSetID, dir, filter)
}

func activePrefixCanHandleFilter(filter *db.Filter) bool {
	return activePrefixScalarCanHandleFilter(filter) ||
		activePrefixAgeAllCanHandleFilter(filter)
}

func activePrefixRollupsReady(ctx context.Context, conn ch.Conn, activeSetID string) (bool, error) {
	rows, err := conn.Query(ctx, activePrefixRollupReadyQuery, activeSetID)
	if err != nil {
		if isUnknownTable(err) {
			return false, nil
		}

		return false, fmt.Errorf("clickhouse: failed to query active-prefix rollup readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: active-prefix rollup readiness iteration error: %w", err)
	}

	return false, nil
}

func readReadyActivePrefixDirSummary(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, bool, error) {
	if activePrefixScalarCanHandleFilter(filter) {
		sum, found, readErr := readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)
		if readErr != nil {
			return nil, false, false, readErr
		}

		return sum, found, !found, nil
	}

	sum, found, err := readActivePrefixFilteredAgeAllRollup(ctx, conn, activeSetID, dir, filter)
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, true, nil
		}

		return nil, false, false, err
	}

	return sum, found, !found, nil
}

func activePrefixScalarCanHandleFilter(filter *db.Filter) bool {
	return filter == nil ||
		(filter.GIDs == nil &&
			filter.UIDs == nil &&
			filter.FT == 0 &&
			filter.Age == db.DGUTAgeAll)
}

func activePrefixAgeAllCanHandleFilter(filter *db.Filter) bool {
	return filter != nil && filter.Age == db.DGUTAgeAll
}

func activePrefixRollupFallback(fallback func() (*db.DirSummary, error)) (*db.DirSummary, error) {
	activePrefixRollupMissCounter.Add(1)

	return fallback()
}

func refreshActivePrefixRollups(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	activeSetID string,
) error {
	snapshot := newActiveMountsSnapshot(rows)
	mounts := snapshot.all()
	dirs := activeTreeDirs(mounts)
	refreshedAt := time.Now().UTC()

	for _, dir := range dirs {
		if err := insertActivePrefixRollup(ctx, conn, activeSetID, dir, snapshot, refreshedAt); err != nil {
			return err
		}
	}

	if err := insertActivePrefixAgeAllRows(ctx, conn, activeSetID, dirs, snapshot, refreshedAt); err != nil {
		return err
	}

	return insertActivePrefixRollupSet(ctx, conn, activeSetID, countActiveMountRows(rows), uint64(len(dirs)), refreshedAt)
}

func insertActivePrefixRollup(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
	snapshot *activeMountsSnapshot,
	refreshedAt time.Time,
) error {
	mounts := snapshot.under(dir)
	if len(mounts) == 0 {
		return nil
	}

	state, err := activePrefixRollupProjectionState(ctx, conn, dir, mounts)
	if err != nil {
		return err
	}

	values := activePrefixRollupRowValues(
		activeSetID,
		dir,
		maxUpdatedAtForMounts(mounts),
		refreshedAt,
		state,
	)
	if err := conn.Exec(ctx, insertActivePrefixRollupQuery, values...); err != nil {
		return fmt.Errorf("clickhouse: failed to insert active-prefix rollup: %w", err)
	}

	return nil
}

func activePrefixRollupRowValues(
	activeSetID string,
	dir string,
	updatedAt time.Time,
	refreshedAt time.Time,
	state mountDirProjectionState,
) []any {
	key := mountDirSummaryKey{dir: dir, age: db.DGUTAgeAll}
	allAcc := summaryAccumulatorOrZero(state.summaries[key])
	fileAcc := state.fileSummaries[key]

	return []any{
		activeSetID,
		dir,
		updatedAt,
		allAcc.count,
		allAcc.size,
		allAcc.atimeMin,
		allAcc.mtimeMax,
		ageBucketsSlice(&allAcc.atimeBuckets),
		ageBucketsSlice(&allAcc.mtimeBuckets),
		sortedUint32Set(allAcc.uids),
		sortedUint32Set(allAcc.gids),
		uint16(allAcc.ft),
		summaryCount(fileAcc),
		summarySize(fileAcc),
		state.childCounts[dir],
		refreshedAt,
	}
}

func insertActivePrefixAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dirs []string,
	snapshot *activeMountsSnapshot,
	refreshedAt time.Time,
) error {
	var batch driver.Batch

	for _, dir := range dirs {
		rows, err := activePrefixAgeAllRows(ctx, conn, dir, snapshot.under(dir))
		if err != nil {
			return err
		}

		if err := appendActivePrefixAgeAllRows(ctx, conn, &batch, activeSetID, dir, rows, refreshedAt); err != nil {
			return err
		}
	}

	if batch == nil {
		return nil
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to insert active-prefix AgeAll rows: %w", err)
	}

	return nil
}

func insertActivePrefixRollupSet(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	activeMountCount uint64,
	prefixCount uint64,
	refreshedAt time.Time,
) error {
	if err := conn.Exec(
		ctx,
		insertActivePrefixRollupSetQuery,
		activeSetID,
		activeMountCount,
		prefixCount,
		refreshedAt,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to insert active-prefix rollup set: %w", err)
	}

	return nil
}

func activePrefixRollupMountsReady(ctx context.Context, conn ch.Conn, rows []mountsActiveRow) (bool, error) {
	deadline := time.Now().Add(activePrefixMountReadinessMaxWait)

	for {
		ready, err := activePrefixRollupMountsReadyNow(ctx, conn, rows)
		if err != nil || ready || time.Now().After(deadline) {
			return ready, err
		}

		if !sleepForActivePrefixMountReadiness(ctx) {
			return false, ctx.Err()
		}
	}
}

func activePrefixRollupMountsReadyNow(ctx context.Context, conn ch.Conn, rows []mountsActiveRow) (bool, error) {
	snapshot := newActiveMountsSnapshot(rows)
	mounts := snapshot.all()

	ready, err := queryReadyActiveMountRows(ctx, conn, mounts)
	if err != nil || !allActiveMountsReady(mounts, ready) {
		return false, err
	}

	return activePrefixRollupSourcesReady(ctx, conn, snapshot)
}

func queryReadyActiveMountRows(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	query, args := activeMountsQuery(
		mountDirSummariesReadyQuery,
		"mount_path",
		"snapshot_id",
		mounts,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query batched dir summary readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanReadyActiveMountRows(rows)
}

func sleepForActivePrefixMountReadiness(ctx context.Context) bool {
	timer := time.NewTimer(activePrefixMountReadinessPollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func queryReadyActivePrefixDirFactRows(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	query, args := activeMountsQuery(
		activePrefixDirFactsReadyQuery,
		"mount_path",
		"snapshot_id",
		mounts,
		dir,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix dir fact readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanReadyActiveMountRows(rows)
}

func queryReadyActiveMountRootFactRows(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	condition, args := activeMountRootDirTuplesCondition(
		"mount_path",
		"snapshot_id",
		"dir",
		mounts,
	)
	query := fmt.Sprintf(activePrefixRootFactsReadyQuery, condition)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix root fact readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanReadyActiveMountRows(rows)
}

func activePrefixRollupFactSourcesReady(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (bool, error) {
	ready, err := queryReadyActivePrefixDirFactRows(ctx, conn, dir, mounts)
	if err != nil {
		return false, err
	}

	missing := missingActiveMounts(mounts, ready)
	if len(missing) == 0 {
		return true, nil
	}

	ready, err = queryReadyActiveMountRootFactRows(ctx, conn, missing)
	if err != nil || !allActiveMountsReady(missing, ready) {
		return false, err
	}

	return true, nil
}

func activePrefixRollupAgeAllSourcesReady(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (bool, error) {
	ready, err := queryReadyActivePrefixAgeAllRows(ctx, conn, dir, mounts)
	if err != nil {
		return false, err
	}

	missing := missingActiveMounts(mounts, ready)
	if len(missing) == 0 {
		return true, nil
	}

	ready, err = queryReadyActiveMountRootAgeAllRows(ctx, conn, missing)
	if err != nil || !allActiveMountsReady(missing, ready) {
		return false, err
	}

	return true, nil
}

func queryReadyActiveMountRootAgeAllRows(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	condition, args := activeMountRootDirTuplesCondition(
		"mount_path",
		"snapshot_id",
		"dir",
		mounts,
	)
	query := fmt.Sprintf(activePrefixRootAgeAllReadyQuery, condition)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix root AgeAll readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanReadyActiveMountRows(rows)
}

func allActiveMountsReady(mounts []activeMount, ready map[treeMountCacheKey]bool) bool {
	for _, mount := range mounts {
		if !ready[newTreeMountCacheKey(mount.mountPath, mount.snapshotID)] {
			return false
		}
	}

	return true
}

func activePrefixRollupSourcesReady(
	ctx context.Context,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) (bool, error) {
	for _, dir := range activeTreeDirs(snapshot.all()) {
		ready, err := activePrefixRollupDirSourcesReady(ctx, conn, dir, snapshot.under(dir))
		if err != nil || !ready {
			return ready, err
		}
	}

	return true, nil
}

func activePrefixRollupDirSourcesReady(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (bool, error) {
	ready, err := activePrefixRollupFactSourcesReady(ctx, conn, dir, mounts)
	if err != nil || !ready {
		return ready, err
	}

	return activePrefixRollupAgeAllSourcesReady(ctx, conn, dir, mounts)
}

func readActivePrefixFilteredAgeAllRollup(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	scalar, found, err := readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)
	if err != nil || !found {
		return nil, found, err
	}

	sum, err := readActivePrefixAgeAllSummary(ctx, conn, activeSetID, dir, filter, scalar.Modtime)

	return sum, true, err
}

func readActivePrefixScalarRollup(
	ctx context.Context,
	conn ch.Conn,
	activeSetID, dir string,
) (*db.DirSummary, bool, error) {
	rows, err := conn.Query(ctx, activePrefixScalarRollupReadQuery, activeSetID, ensureTrailingSlash(dir))
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("clickhouse: failed to query active-prefix scalar rollup: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanTreeDirSummaryRow(rows)
}

func readActivePrefixAgeAllSummary(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
	filter *db.Filter,
	updatedAt time.Time,
) (*db.DirSummary, error) {
	query, args := activePrefixAgeAllSummaryQuery(activeSetID, dir, filter)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active-prefix AgeAll summary: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, updatedAt)
	if err != nil {
		return nil, err
	}

	return summaries[ensureTrailingSlash(dir)], nil
}

func activePrefixAgeAllSummaryQuery(
	activeSetID string,
	dir string,
	filter *db.Filter,
) (string, []any) {
	filterExpr, filterArgs := activePrefixAgeAllFilterExpression(filter)
	query := fmt.Sprintf(activePrefixAgeAllSummaryReadQuery, filterExpr)

	args := make([]any, 0, queryScopeArgs+len(filterArgs))
	args = append(args, activeSetID, ensureTrailingSlash(dir))
	args = append(args, filterArgs...)

	return query, args
}

func activePrefixAgeAllFilterExpression(filter *db.Filter) (string, []any) {
	if filter == nil {
		return "1", nil
	}

	if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return "0", nil
	}

	clauses := make([]string, 0, dirSummaryFilterClauseInitialCap)
	args := make([]any, 0, dirSummaryFilterArgCap(filter))

	appendIDMembershipClause(&clauses, &args, "p.gid", filter.GIDs)
	appendIDMembershipClause(&clauses, &args, "p.uid", filter.UIDs)
	appendFTMembershipClause(&clauses, &args, "p.ft", filter.FT)

	if len(clauses) == 0 {
		return "1", args
	}

	return strings.Join(clauses, " AND "), args
}
