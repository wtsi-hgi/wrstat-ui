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
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	mountDirSummaryVersion = 4

	insertMountDirSummaryQuery = "INSERT INTO wrstat_dir_summary " +
		"(mount_path, snapshot_id, dir, updated_at, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, uids, gids, ft, " +
		"file_count, file_size, file_atime_min, file_mtime_max, " +
		"file_atime_buckets, file_mtime_buckets, file_uids, file_gids, file_ft, " +
		"child_count, refreshed_at) " +
		"SELECT ?, toUUID(?), d.dir, ?, d.age, sum(d.count), sum(d.size), " +
		"minIf(d.atime_min, d.atime_min != 0), max(d.mtime_max), " +
		"arrayReduce('sumForEach', groupArray(d.atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(d.mtime_buckets)), " +
		"arraySort(groupUniqArray(d.uid)), arraySort(groupUniqArray(d.gid)), groupBitOr(d.ft), " +
		"sumIf(d.count, d.file_summary), sumIf(d.size, d.file_summary), " +
		"minIf(d.atime_min, d.file_summary AND d.atime_min != 0), maxIf(d.mtime_max, d.file_summary), " +
		"arrayReduce('sumForEach', groupArrayIf(d.atime_buckets, d.file_summary)), " +
		"arrayReduce('sumForEach', groupArrayIf(d.mtime_buckets, d.file_summary)), " +
		"arraySort(groupUniqArrayIf(d.uid, d.file_summary)), arraySort(groupUniqArrayIf(d.gid, d.file_summary)), " +
		"groupBitOrIf(d.ft, d.file_summary), any(ifNull(c.child_count, 0)), ? " +
		"FROM (" +
		"SELECT *, bitAnd(ft, ?) > 0 AS file_summary FROM wrstat_dguta " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)" +
		") AS d " +
		"LEFT JOIN (" +
		"SELECT parent_dir, count() AS child_count FROM wrstat_children " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?) GROUP BY parent_dir" +
		") AS c ON c.parent_dir = d.dir " +
		"GROUP BY d.dir, d.age"

	insertMountDirSummarySetQuery = "INSERT INTO wrstat_dir_summary_sets " +
		"(mount_path, snapshot_id, updated_at, summary_version, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?)"

	mountDirSummaryReadyQuery = "SELECT 1 FROM wrstat_dir_summary_sets FINAL " +
		"WHERE mount_path = ? AND snapshot_id = ? AND summary_version = ? LIMIT 1"

	mountDirSummariesForDirsQuery = "SELECT dir, updated_at, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, uids, gids, ft, child_count " +
		"FROM wrstat_dir_summary " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND age = ? " +
		"WHERE dir IN (%s)"

	mountDirFileSummariesForDirsQuery = "SELECT dir, updated_at, age, file_count, file_size, " +
		"file_atime_min, file_mtime_max, file_atime_buckets, file_mtime_buckets, " +
		"file_uids, file_gids, file_ft, child_count " +
		"FROM wrstat_dir_summary " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND age = ? " +
		"WHERE dir IN (%s)"

	mountDirSummariesForExternalDirsQuery = "SELECT s.dir, s.updated_at, s.age, s.count, s.size, " +
		"s.atime_min, s.mtime_max, s.atime_buckets, s.mtime_buckets, s.uids, s.gids, s.ft, s.child_count " +
		"FROM wrstat_dir_summary AS s " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = s.dir " +
		"WHERE s.mount_path = ? AND s.snapshot_id = ? AND s.age = ?"

	mountDirFileSummariesForExternalDirsQuery = "SELECT s.dir, s.updated_at, s.age, " +
		"s.file_count, s.file_size, s.file_atime_min, s.file_mtime_max, " +
		"s.file_atime_buckets, s.file_mtime_buckets, s.file_uids, s.file_gids, s.file_ft, s.child_count " +
		"FROM wrstat_dir_summary AS s " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = s.dir " +
		"WHERE s.mount_path = ? AND s.snapshot_id = ? AND s.age = ?"
)

type mountDirSummaryMode uint8

const (
	mountDirSummaryAll mountDirSummaryMode = iota
	mountDirSummaryFiles
)

func mountDirSummaryMissingMeansNotFound(filter *db.Filter) bool {
	return filter != nil && filter.Age == db.DGUTAgeAll
}

func mountDirSummaryModeForFilter(filter *db.Filter) (mountDirSummaryMode, bool) {
	if filter == nil || filter.GIDs != nil || filter.UIDs != nil {
		return mountDirSummaryAll, false
	}

	switch filter.FT {
	case 0:
		return mountDirSummaryAll, true
	case db.AllTypesExceptDirectories:
		return mountDirSummaryFiles, true
	default:
		return mountDirSummaryAll, false
	}
}

func mountDirSummariesForExternalDirsQueryForMode(mode mountDirSummaryMode) string {
	if mode == mountDirSummaryFiles {
		return mountDirFileSummariesForExternalDirsQuery
	}

	return mountDirSummariesForExternalDirsQuery
}

func mountDirSummariesForDirsQueryForMode(mode mountDirSummaryMode) string {
	if mode == mountDirSummaryFiles {
		return mountDirFileSummariesForDirsQuery
	}

	return mountDirSummariesForDirsQuery
}

func ensureActiveMountDirSummaries(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
) error {
	return ensureActiveMountDirSummariesWithProgress(ctx, conn, rows, nil)
}

func ensureActiveMountDirSummariesWithProgress(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	progress mountDirProjectionProgress,
) error {
	total := len(rows)

	for i, row := range rows {
		if err := ensureActiveMountDirSummaryWithProgress(ctx, conn, row, i+1, total, progress); err != nil {
			return err
		}
	}

	return nil
}

func ensureActiveMountDirSummaryWithProgress(
	ctx context.Context,
	conn ch.Conn,
	row mountsActiveRow,
	index, total int,
	progress mountDirProjectionProgress,
) error {
	started := time.Now()
	mount := activeMount(row)

	reportMountDirProjectionMountStarted(progress, row, index, total)

	ready, err := mountDirProjectionsReady(ctx, conn, mount.mountPath, mount.snapshotID)
	if err != nil {
		return err
	}

	if ready {
		reportMountDirProjectionMountSkipped(progress, row, index, total, time.Since(started))

		return nil
	}

	if err := refreshMountDirSummariesWithProgress(ctx, conn, mount, row, index, total, progress); err != nil {
		return err
	}

	reportMountDirProjectionMountCompleted(progress, row, index, total, time.Since(started))

	return nil
}

func reportMountDirProjectionMountStarted(
	progress mountDirProjectionProgress,
	row mountsActiveRow,
	index, total int,
) {
	if progress != nil {
		progress.mountStarted(row, index, total)
	}
}

func reportMountDirProjectionMountSkipped(
	progress mountDirProjectionProgress,
	row mountsActiveRow,
	index, total int,
	duration time.Duration,
) {
	if progress != nil {
		progress.mountSkipped(row, index, total, duration)
	}
}

func reportMountDirProjectionMountCompleted(
	progress mountDirProjectionProgress,
	row mountsActiveRow,
	index, total int,
	duration time.Duration,
) {
	if progress != nil {
		progress.mountCompleted(row, index, total, duration)
	}
}

func mountDirProjectionsReady(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID string,
) (bool, error) {
	return mountDirSummaryReady(ctx, conn, mountPath, snapshotID)
}

func mountDirSummaryReady(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID string,
) (bool, error) {
	rows, err := conn.Query(ctx, mountDirSummaryReadyQuery, mountPath, snapshotID, mountDirSummaryVersion)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query dir summary readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: dir summary readiness iteration error: %w", err)
	}

	return false, nil
}

func refreshMountDirSummaries(ctx context.Context, conn ch.Conn, mount activeMount) error {
	row := mountsActiveRow(mount)

	return refreshMountDirSummariesWithProgress(ctx, conn, mount, row, 1, 1, nil)
}

func refreshMountDirSummariesWithProgress(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	row mountsActiveRow,
	index, total int,
	progress mountDirProjectionProgress,
) error {
	if mount.mountPath == "" || mount.snapshotID == "" {
		return nil
	}

	refreshedAt := time.Now().UTC()

	if err := runMountDirProjectionPhase(progress, row, index, total, "drop_partitions", func() error {
		return dropMountDirSummaryPartitions(ctx, conn, mount)
	}); err != nil {
		return err
	}

	return insertMountDirSummaryRowsWithProgress(ctx, conn, mount, row, index, total, refreshedAt, progress)
}

func dropMountDirSummaryPartitions(ctx context.Context, conn ch.Conn, mount activeMount) error {
	if err := dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummarySetPartitionQuery,
	); err != nil {
		return err
	}

	if err := dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummaryPartitionQuery,
	); err != nil {
		return err
	}

	return dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirDGUTAVectorPartitionQuery,
	)
}

func insertMountDirSummaryRowsWithProgress(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	row mountsActiveRow,
	index, total int,
	refreshedAt time.Time,
	progress mountDirProjectionProgress,
) error {
	if err := runMountDirProjectionPhase(progress, row, index, total, "dir_summary", func() error {
		return insertMountDirSummaries(ctx, conn, mount, refreshedAt)
	}); err != nil {
		return err
	}

	if err := runMountDirProjectionPhase(progress, row, index, total, "dguta_vector", func() error {
		return insertMountDirDGUTAVectors(ctx, conn, mount, refreshedAt)
	}); err != nil {
		return err
	}

	return runMountDirProjectionPhase(progress, row, index, total, "mark_ready", func() error {
		return insertMountDirSummarySet(ctx, conn, mount, refreshedAt)
	})
}

func runMountDirProjectionPhase(
	progress mountDirProjectionProgress,
	row mountsActiveRow,
	index, total int,
	phase string,
	fn func() error,
) error {
	if progress != nil {
		progress.phaseStarted(row, index, total, phase)
	}

	started := time.Now()

	if err := fn(); err != nil {
		return err
	}

	if progress != nil {
		progress.phaseCompleted(row, index, total, phase, time.Since(started))
	}

	return nil
}

func insertMountDirSummaries(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	refreshedAt time.Time,
) error {
	if err := conn.Exec(ctx, insertMountDirSummaryQuery,
		mount.mountPath,
		mount.snapshotID,
		mount.updatedAt,
		refreshedAt,
		uint16(db.AllTypesExceptDirectories),
		mount.mountPath,
		mount.snapshotID,
		mount.mountPath,
		mount.snapshotID,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to refresh dir summaries: %w", err)
	}

	return nil
}

func insertMountDirSummarySet(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	refreshedAt time.Time,
) error {
	if err := conn.Exec(ctx, insertMountDirSummarySetQuery,
		mount.mountPath,
		mount.snapshotID,
		mount.updatedAt,
		mountDirSummaryVersion,
		refreshedAt,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to mark dir summaries refreshed: %w", err)
	}

	return nil
}

func scanMountDirSummaryRows(
	rows rowsScanner,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	summaries := make(map[string]*db.DirSummary)
	handled := make(map[string]bool)
	childCounts := make(map[string]uint64)

	for rows.Next() {
		dir, sum, childCount, err := scanMountDirSummaryRow(rows)
		if err != nil {
			return nil, nil, nil, err
		}

		summaries[dir] = sum
		handled[dir] = true
		childCounts[dir] = childCount
	}

	if err := rowsErr(rows); err != nil {
		return nil, nil, nil, fmt.Errorf("clickhouse: maintained dir summary iteration error: %w", err)
	}

	return summaries, handled, childCounts, nil
}

func scanMountDirSummaryRow(rows rowsScanner) (string, *db.DirSummary, uint64, error) {
	var (
		dir        string
		childCount uint64
		s          treeDirSummaryScanned
	)

	if err := rows.Scan(
		&dir,
		&s.updatedAt,
		&s.age,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
		&s.uids,
		&s.gids,
		&s.ft,
		&childCount,
	); err != nil {
		return "", nil, 0, fmt.Errorf("clickhouse: failed to scan maintained dir summary: %w", err)
	}

	if s.count == 0 {
		return dir, nil, childCount, nil
	}

	return dir, s.summary(), childCount, nil
}

func mergeMountDirSummaryRows(
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	childCounts map[string]uint64,
	batchSummaries map[string]*db.DirSummary,
	batchHandled map[string]bool,
	batchChildCounts map[string]uint64,
) {
	for dir, sum := range batchSummaries {
		summaries[dir] = sum
	}

	for dir := range batchHandled {
		handled[dir] = true
	}

	for dir, childCount := range batchChildCounts {
		childCounts[dir] = childCount
	}
}

func (d *clickHouseDatabase) mountDirSummariesForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return nil, nil, false, nil
	}

	summaries, handled, missing := d.cachedMountDirSummaries(mountPath, snapshotID, dirs, filter.Age, mode)
	if len(missing) == 0 {
		return summaries, handled, true, nil
	}

	ok, err := d.addMissingMountDirSummaries(
		summaries, handled, mountPath, snapshotID, missing, filter.Age, mode,
	)
	if err != nil || !ok {
		return nil, nil, ok, err
	}

	return summaries, handled, true, nil
}

func (d *clickHouseDatabase) addMissingMountDirSummaries(
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	mountPath, snapshotID string,
	missing []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return false, err
	}

	queried, queryHandled, childCounts, err := d.queryMountDirSummariesForDirs(
		ctx, mountPath, snapshotID, missing, age, mode,
	)
	if err != nil {
		return true, err
	}

	d.addQueriedMountDirSummaries(
		summaries, handled, mountPath, snapshotID, age, mode, queried, queryHandled, childCounts,
	)

	return true, nil
}

func (d *clickHouseDatabase) mountDirSummaryReadyCached(
	ctx context.Context,
	mountPath, snapshotID string,
) (bool, error) {
	key := newTreeMountCacheKey(mountPath, snapshotID)
	if d.treeCache.getMountDirSummaryReady(key) {
		return true, nil
	}

	ready, err := mountDirSummaryReady(ctx, d.conn, mountPath, snapshotID)
	if err != nil || !ready {
		return ready, err
	}

	d.treeCache.putMountDirSummaryReady(key)

	return true, nil
}

func (d *clickHouseDatabase) cachedMountDirSummaries(
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, []string) {
	summaries := make(map[string]*db.DirSummary, len(dirs))
	handled := make(map[string]bool, len(dirs))
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, age, mode)
		if seen[key.dir] {
			continue
		}

		seen[key.dir] = true
		if summary, ok := d.treeCache.getDirSummary(key); ok {
			summaries[key.dir] = summary
			handled[key.dir] = true

			continue
		}

		missing = append(missing, key.dir)
	}

	return summaries, handled, missing
}

func (d *clickHouseDatabase) queryMountDirSummariesForDirs(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	if len(dirs) > queryStringINMaxValues {
		return d.queryMountDirSummariesForExternalDirs(ctx, mountPath, snapshotID, dirs, age, mode)
	}

	return d.queryMountDirSummariesForDirsBatches(ctx, mountPath, snapshotID, dirs, age, mode)
}

func (d *clickHouseDatabase) queryMountDirSummariesForDirsBatches(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	summaries := make(map[string]*db.DirSummary)
	handled := make(map[string]bool)
	childCounts := make(map[string]uint64)

	for _, batchDirs := range stringValueBatches(dirs) {
		err := d.addMountDirSummaryBatch(
			ctx, mountPath, snapshotID, batchDirs, age, mode, summaries, handled, childCounts,
		)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return summaries, handled, childCounts, nil
}

func (d *clickHouseDatabase) addMountDirSummaryBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	childCounts map[string]uint64,
) error {
	batchSummaries, batchHandled, batchChildCounts, err := d.queryMountDirSummariesForDirsBatch(
		ctx, mountPath, snapshotID, dirs, age, mode,
	)
	if err != nil {
		return err
	}

	mergeMountDirSummaryRows(summaries, handled, childCounts, batchSummaries, batchHandled, batchChildCounts)

	return nil
}

func (d *clickHouseDatabase) queryMountDirSummariesForExternalDirs(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	externalCtx, err := contextWithExternalDirs(ctx, dirs)
	if err != nil {
		return nil, nil, nil, err
	}

	rows, err := d.conn.Query(
		externalCtx,
		mountDirSummariesForExternalDirsQueryForMode(mode),
		mountPath,
		snapshotID,
		uint8(age),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("clickhouse: failed to query maintained external dir summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanMountDirSummaryRows(rows)
}

func (d *clickHouseDatabase) queryMountDirSummariesForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	query, args := scopedBatchQuery(
		mountDirSummariesForDirsQueryForMode(mode),
		dirs,
		mountPath,
		snapshotID,
		uint8(age),
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("clickhouse: failed to query maintained dir summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanMountDirSummaryRows(rows)
}

func (d *clickHouseDatabase) addQueriedMountDirSummaries(
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	mountPath, snapshotID string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
	queried map[string]*db.DirSummary,
	queryHandled map[string]bool,
	childCounts map[string]uint64,
) {
	for dir := range queryHandled {
		sum := queried[dir]
		d.treeCache.putDirSummary(
			newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, age, mode),
			sum,
		)
		handled[dir] = true

		if sum != nil {
			summaries[dir] = cloneDirSummary(sum)
		}

		if childCounts[dir] == 0 {
			d.treeCache.putChildren(newTreeCacheKey(mountPath, snapshotID, dir), nil)
		}
	}
}
