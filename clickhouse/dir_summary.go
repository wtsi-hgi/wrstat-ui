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
	insertMountDirSummaryQuery = "INSERT INTO wrstat_dir_facts " +
		"(mount_path, snapshot_id, dir, updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, " +
		"all_uids, all_gids, all_ft, " +
		"file_count, file_size, file_atime_min, file_mtime_max, " +
		"file_atime_buckets, file_mtime_buckets, file_uids, file_gids, file_ft, " +
		"gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, " +
		"atime_buckets, mtime_buckets, child_count, refreshed_at) VALUES " +
		"(?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertMountDirSummarySetQuery = "INSERT INTO wrstat_dir_projection_sets " +
		"(mount_path, snapshot_id, updated_at, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?)"

	mountDirSummaryReadyQuery = "SELECT 1 FROM wrstat_dir_projection_sets " +
		"WHERE mount_path = ? AND snapshot_id = ? LIMIT 1"

	mountDirSummariesForDirsQuery = "SELECT dir, updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, child_count " +
		"FROM wrstat_dir_facts " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)"

	mountDirFileSummariesForDirsQuery = "SELECT dir, updated_at, file_count, file_size, " +
		"file_atime_min, file_mtime_max, file_atime_buckets, file_mtime_buckets, " +
		"file_uids, file_gids, file_ft, child_count " +
		"FROM wrstat_dir_facts " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)"

	mountDirSummariesForExternalDirsQuery = "SELECT s.dir, s.updated_at, s.all_count, s.all_size, " +
		"s.all_atime_min, s.all_mtime_max, s.all_atime_buckets, s.all_mtime_buckets, " +
		"s.all_uids, s.all_gids, s.all_ft, s.child_count " +
		"FROM wrstat_dir_facts AS s " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = s.dir " +
		"WHERE s.mount_path = ? AND s.snapshot_id = ?"

	mountDirFileSummariesForExternalDirsQuery = "SELECT s.dir, s.updated_at, " +
		"s.file_count, s.file_size, s.file_atime_min, s.file_mtime_max, " +
		"s.file_atime_buckets, s.file_mtime_buckets, s.file_uids, s.file_gids, s.file_ft, s.child_count " +
		"FROM wrstat_dir_facts AS s " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = s.dir " +
		"WHERE s.mount_path = ? AND s.snapshot_id = ?"
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
	if filter == nil || filter.GIDs != nil || filter.UIDs != nil || filter.Age != db.DGUTAgeAll {
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

func newMountDirSummaryScan() *mountDirSummaryScan {
	return &mountDirSummaryScan{
		summaries:    make(map[string]*db.DirSummary),
		handled:      make(map[string]bool),
		childCounts:  make(map[string]uint64),
		accumulators: make(map[string]*mountDirSummaryAccumulator),
		updatedAts:   make(map[string]time.Time),
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

type mountDirSummaryScan struct {
	summaries    map[string]*db.DirSummary
	handled      map[string]bool
	childCounts  map[string]uint64
	accumulators map[string]*mountDirSummaryAccumulator
	updatedAts   map[string]time.Time
}

func (s *mountDirSummaryScan) add(dir string, row treeDirSummaryScanned, childCount uint64) {
	s.handled[dir] = true
	s.childCounts[dir] += childCount

	if row.count == 0 {
		return
	}

	acc := s.accumulator(dir, db.DirGUTAge(row.age))
	acc.addScanned(row)
	s.updatedAts[dir] = row.updatedAt
}

func (s *mountDirSummaryScan) accumulator(dir string, age db.DirGUTAge) *mountDirSummaryAccumulator {
	acc := s.accumulators[dir]
	if acc != nil {
		return acc
	}

	acc = newMountDirSummaryAccumulator(age)
	s.accumulators[dir] = acc

	return acc
}

func (s *mountDirSummaryScan) result() (map[string]*db.DirSummary, map[string]bool, map[string]uint64) {
	for dir, acc := range s.accumulators {
		s.summaries[dir] = acc.summary(s.updatedAts[dir])
	}

	return s.summaries, s.handled, s.childCounts
}

func mountDirSummaryReady(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID string,
) (bool, error) {
	rows, err := conn.Query(ctx, mountDirSummaryReadyQuery, mountPath, snapshotID)
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

func scanMountDirSummaryRows(
	rows rowsScanner,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	scanned := newMountDirSummaryScan()

	for rows.Next() {
		dir, s, childCount, err := scanMountDirSummaryRow(rows)
		if err != nil {
			return nil, nil, nil, err
		}

		scanned.add(dir, s, childCount)
	}

	if err := rowsErr(rows); err != nil {
		return nil, nil, nil, fmt.Errorf("clickhouse: maintained dir summary iteration error: %w", err)
	}

	summaries, handled, childCounts := scanned.result()

	return summaries, handled, childCounts, nil
}

func scanMountDirSummaryRow(rows rowsScanner) (string, treeDirSummaryScanned, uint64, error) {
	var (
		dir        string
		childCount uint64
		s          treeDirSummaryScanned
	)

	s.age = uint8(db.DGUTAgeAll)

	if err := rows.Scan(
		&dir,
		&s.updatedAt,
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
		return "", treeDirSummaryScanned{}, 0, fmt.Errorf("clickhouse: failed to scan maintained dir summary: %w", err)
	}

	return dir, s, childCount, nil
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

func mergeMountDirChildCounts(childCounts, queryChildCounts map[string]uint64) {
	for dir, childCount := range queryChildCounts {
		childCounts[dir] = childCount
	}
}

func (d *clickHouseDatabase) mountDirSummariesForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, bool, error) {
	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return nil, nil, nil, false, nil
	}

	summaries, handled, childCounts, missing := d.cachedMountDirSummaries(
		mountPath,
		snapshotID,
		dirs,
		filter.Age,
		mode,
	)
	if len(missing) == 0 {
		return summaries, handled, childCounts, true, nil
	}

	queryChildCounts, ok, err := d.addMissingMountDirSummaries(
		summaries, handled, mountPath, snapshotID, missing, filter.Age, mode,
	)
	if err != nil || !ok {
		return nil, nil, nil, ok, err
	}

	mergeMountDirChildCounts(childCounts, queryChildCounts)

	return summaries, handled, childCounts, true, nil
}

func (d *clickHouseDatabase) addMissingMountDirSummaries(
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	mountPath, snapshotID string,
	missing []string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]uint64, bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return nil, false, err
	}

	queried, queryHandled, childCounts, err := d.queryMountDirSummariesForDirs(
		ctx, mountPath, snapshotID, missing, age, mode,
	)
	if err != nil {
		return nil, true, err
	}

	d.addQueriedMountDirSummaries(
		summaries, handled, mountPath, snapshotID, age, mode, queried, queryHandled, childCounts,
	)

	return childCounts, true, nil
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
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, []string) {
	summaries := make(map[string]*db.DirSummary, len(dirs))
	handled := make(map[string]bool, len(dirs))
	childCounts := make(map[string]uint64, len(dirs))
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, age, mode)
		if seen[key.dir] {
			continue
		}

		seen[key.dir] = true
		if summary, ok := d.treeCache.getDirSummary(key); ok {
			d.addCachedMountDirSummary(summaries, handled, childCounts, key, summary)

			continue
		}

		missing = append(missing, key.dir)
	}

	return summaries, handled, childCounts, missing
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
	_ db.DirGUTAge,
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
	_ db.DirGUTAge,
	mode mountDirSummaryMode,
) (map[string]*db.DirSummary, map[string]bool, map[string]uint64, error) {
	query, args := scopedBatchQuery(
		mountDirSummariesForDirsQueryForMode(mode),
		dirs,
		mountPath,
		snapshotID,
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
		d.treeCache.putDirSummaryWithChildCount(
			newTreeDirSummaryCacheKey(mountPath, snapshotID, dir, age, mode),
			sum,
			childCounts[dir],
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

func (d *clickHouseDatabase) addCachedMountDirSummary(
	summaries map[string]*db.DirSummary,
	handled map[string]bool,
	childCounts map[string]uint64,
	key treeDirSummaryCacheKey,
	summary *db.DirSummary,
) {
	summaries[key.dir] = summary
	handled[key.dir] = true

	if childCount, ok := d.treeCache.getDirSummaryChildCount(key); ok {
		childCounts[key.dir] = childCount
	}
}
