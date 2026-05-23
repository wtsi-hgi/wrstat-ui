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
	mountDirSummaryVersion = 2

	insertMountDirSummaryQuery = "INSERT INTO wrstat_dir_summary " +
		"(mount_path, snapshot_id, dir, updated_at, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, uids, gids, ft, child_count, refreshed_at) " +
		"SELECT ?, toUUID(?), d.dir, ?, d.age, sum(d.count), sum(d.size), " +
		"minIf(d.atime_min, d.atime_min != 0), max(d.mtime_max), " +
		"arrayReduce('sumForEach', groupArray(d.atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(d.mtime_buckets)), " +
		"arraySort(groupUniqArray(d.uid)), arraySort(groupUniqArray(d.gid)), " +
		"groupBitOr(d.ft), any(ifNull(c.child_count, 0)), ? " +
		"FROM wrstat_dguta AS d " +
		"LEFT JOIN (" +
		"SELECT parent_dir, count() AS child_count FROM wrstat_children " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?) GROUP BY parent_dir" +
		") AS c ON c.parent_dir = d.dir " +
		"WHERE d.mount_path = ? AND d.snapshot_id = toUUID(?) " +
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

	mountDirSummariesForExternalDirsQuery = "SELECT s.dir, s.updated_at, s.age, s.count, s.size, " +
		"s.atime_min, s.mtime_max, s.atime_buckets, s.mtime_buckets, s.uids, s.gids, s.ft, s.child_count " +
		"FROM wrstat_dir_summary AS s " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = s.dir " +
		"WHERE s.mount_path = ? AND s.snapshot_id = ? AND s.age = ?"
)

func mountDirSummaryMissingMeansNotFound(filter *db.Filter) bool {
	return filter != nil && filter.Age == db.DGUTAgeAll
}

func ensureActiveMountDirSummaries(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
) error {
	for _, row := range rows {
		mount := activeMount(row)

		ready, err := mountDirSummaryReady(ctx, conn, mount.mountPath, mount.snapshotID)
		if err != nil {
			return err
		}

		if ready {
			continue
		}

		if err := refreshMountDirSummaries(ctx, conn, mount); err != nil {
			return err
		}
	}

	return nil
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
	if mount.mountPath == "" || mount.snapshotID == "" {
		return nil
	}

	refreshedAt := time.Now().UTC()

	if err := dropMountDirSummaryPartitions(ctx, conn, mount); err != nil {
		return err
	}

	return insertMountDirSummaryRows(ctx, conn, mount, refreshedAt)
}

func dropMountDirSummaryPartitions(ctx context.Context, conn ch.Conn, mount activeMount) error {
	if err := dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummarySetPartitionQuery,
	); err != nil {
		return err
	}

	return dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummaryPartitionQuery,
	)
}

func insertMountDirSummaryRows(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	refreshedAt time.Time,
) error {
	if err := insertMountDirSummaries(ctx, conn, mount, refreshedAt); err != nil {
		return err
	}

	return insertMountDirSummarySet(ctx, conn, mount, refreshedAt)
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

func canUseMountDirSummary(filter *db.Filter) bool {
	return filter != nil && filter.GIDs == nil && filter.UIDs == nil && filter.FT == 0
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
	if !canUseMountDirSummary(filter) {
		return nil, nil, false, nil
	}

	summaries, handled, missing := d.cachedMountDirSummaries(mountPath, snapshotID, dirs, filter.Age)
	if len(missing) == 0 {
		return summaries, handled, true, nil
	}

	ok, err := d.addMissingMountDirSummaries(summaries, handled, mountPath, snapshotID, missing, filter.Age)
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
) (bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return false, err
	}

	queried, queryHandled, childCounts, err := d.queryMountDirSummariesForDirs(
		ctx, mountPath, snapshotID, missing, age,
	)
	if err != nil {
		return true, err
	}

	d.addQueriedMountDirSummaries(summaries, handled, mountPath, snapshotID, queried, queryHandled, childCounts)

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
) (map[string]*db.DirSummary, map[string]bool, []string) {
	summaries := make(map[string]*db.DirSummary, len(dirs))
	handled := make(map[string]bool, len(dirs))
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, age)
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
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	if len(dirs) > queryStringINMaxValues {
		return d.queryMountDirSummariesForExternalDirs(ctx, mountPath, snapshotID, dirs, age)
	}

	return d.queryMountDirSummariesForDirsBatches(ctx, mountPath, snapshotID, dirs, age)
}

func (d *clickHouseDatabase) queryMountDirSummariesForDirsBatches(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	age db.DirGUTAge,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	summaries := make(map[string]*db.DirSummary)
	handled := make(map[string]bool)
	childCounts := make(map[string]uint64)

	for _, batchDirs := range stringValueBatches(dirs) {
		err := d.addMountDirSummaryBatch(
			ctx, mountPath, snapshotID, batchDirs, age, summaries, handled, childCounts,
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
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	childCounts map[string]uint64,
) error {
	batchSummaries, batchHandled, batchChildCounts, err := d.queryMountDirSummariesForDirsBatch(
		ctx, mountPath, snapshotID, dirs, age,
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
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	externalCtx, err := contextWithExternalDirs(ctx, dirs)
	if err != nil {
		return nil, nil, nil, err
	}

	rows, err := d.conn.Query(externalCtx, mountDirSummariesForExternalDirsQuery, mountPath, snapshotID, uint8(age))
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
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	query, args := scopedBatchQuery(mountDirSummariesForDirsQuery, dirs, mountPath, snapshotID, uint8(age))

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
	queried map[string]*db.DirSummary,
	queryHandled map[string]bool,
	childCounts map[string]uint64,
) {
	for dir, sum := range queried {
		d.treeCache.putDirSummary(
			newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, sum.Age),
			sum,
		)
		summaries[dir] = cloneDirSummary(sum)

		if childCounts[dir] == 0 {
			d.treeCache.putChildren(newTreeCacheKey(mountPath, snapshotID, dir), nil)
		}
	}

	for dir := range queryHandled {
		handled[dir] = true
	}
}
