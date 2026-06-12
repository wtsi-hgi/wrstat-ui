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
	"slices"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	activeVirtualReadyQuery = "SELECT 1 FROM wrstat_active_virtual_sets " +
		"WHERE active_set_id = ? AND ready = 1 LIMIT 1"
	activeVirtualScalarSummaryQuery = "SELECT dir, toUInt64(1) AS raw_rows, %s AS total_count, %s AS total_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft " +
		"FROM wrstat_active_virtual_summaries PREWHERE active_set_id = ? WHERE dir IN (%s)"
	activeVirtualFullFilterSummaryQuery = "SELECT dir, count() AS raw_rows, sum(count) AS total_count, " +
		"sum(size) AS total_size, minIf(atime_min, atime_min != 0) AS atime_min, max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, arraySort(groupUniqArray(gid)) AS gids, groupBitOr(v.ft) AS ft " +
		"FROM wrstat_active_virtual_filter_all AS v PREWHERE active_set_id = ? " +
		"WHERE age = ? %s AND dir IN (%s) GROUP BY dir"
	activeVirtualExistingDirsQuery = "SELECT dir FROM wrstat_active_virtual_summaries " +
		"PREWHERE active_set_id = ? WHERE dir IN (%s)"
	activeVirtualChildrenQuery = "SELECT child_dir FROM wrstat_active_virtual_children " +
		"PREWHERE active_set_id = ? WHERE parent_dir = ? ORDER BY child_dir"
	activeVirtualChildrenForParentsQuery = "SELECT parent_dir, child_dir FROM wrstat_active_virtual_children " +
		"PREWHERE active_set_id = ? WHERE parent_dir IN (%s) ORDER BY parent_dir, child_dir"
	activeVirtualChildCountQuery = "SELECT child_count FROM wrstat_active_virtual_summaries " +
		"PREWHERE active_set_id = ? WHERE dir = ? LIMIT 1"
	activeVirtualMountRootBoxQuery = "SELECT is_mount_root_box FROM wrstat_active_virtual_summaries " +
		"PREWHERE active_set_id = ? WHERE dir = ? LIMIT 1"
	activeVirtualRootGUTAsQuery = "SELECT dir, " + dgutaTupleColumns + " FROM (" +
		"SELECT d.mount_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d WHERE d.dir = d.mount_path AND %s)"
	activeVirtualRootFilterRowsQuery = "SELECT mount_path, age, gid, uid, ft, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, child_count " +
		"FROM wrstat_dir_filter_all WHERE dir = mount_path AND %s ORDER BY mount_path, age, gid, uid, ft"
	activeVirtualMountChildCountsQuery = "SELECT c.mount_path, count() FROM wrstat_children c " +
		"WHERE c.parent_dir = c.mount_path AND %s GROUP BY c.mount_path"
)

func queryActiveVirtualRootGUTAs(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[string]db.GUTAs, error) {
	query, args := activeMountsQuery(activeVirtualRootGUTAsQuery, "d.mount_path", "d.snapshot_id", mounts)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active virtual root facts: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanDGUTARowsByDir(rows)
}

func queryActiveVirtualRootFilterRows( //nolint:funlen
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[string][]activeVirtualFilterAllRow, error) {
	query, args := activeMountsQuery(
		activeVirtualRootFilterRowsQuery,
		"mount_path",
		"snapshot_id",
		mounts,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return map[string][]activeVirtualFilterAllRow{}, nil
		}

		return nil, fmt.Errorf("clickhouse: failed to query active virtual root filter rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	out := make(map[string][]activeVirtualFilterAllRow)

	for rows.Next() {
		var mountPath string

		row := activeVirtualFilterAllRow{}
		if err := rows.Scan(
			&mountPath,
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
			return nil, fmt.Errorf("clickhouse: failed to scan active virtual root filter row: %w", err)
		}

		out[mountPath] = append(out[mountPath], row)
	}

	return out, rowIterationErr(rows, "clickhouse: active virtual root filter row iteration error")
}

func queryActiveVirtualMountChildCounts( //nolint:funlen
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[string]uint64, error) {
	query, args := activeMountsQuery(
		activeVirtualMountChildCountsQuery,
		"c.mount_path",
		"c.snapshot_id",
		mounts,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active virtual child counts: %w", err)
	}

	defer func() { _ = rows.Close() }()

	counts := make(map[string]uint64)

	for rows.Next() {
		var (
			mountPath string
			count     uint64
		)
		if err := rows.Scan(&mountPath, &count); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active virtual child count: %w", err)
		}

		counts[mountPath] = count
	}

	return counts, rowIterationErr(rows, "clickhouse: active virtual child count iteration error")
}

type activeVirtualGUTAAggregate struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
	uids         []uint32
	gids         []uint32
	ft           uint16
}

func activeVirtualGUTAAggregateForFilter(gutas db.GUTAs, filter *db.Filter) activeVirtualGUTAAggregate {
	aggregate := activeVirtualGUTAAggregate{
		atimeBuckets: emptyAgeBuckets(),
		mtimeBuckets: emptyAgeBuckets(),
	}
	uidSet := make(map[uint32]bool)
	gidSet := make(map[uint32]bool)

	for _, guta := range gutas {
		if !guta.PassesFilter(filter) {
			continue
		}

		aggregate.count += guta.Count
		aggregate.size += guta.Size
		aggregate.atimeMin = minNonZeroInt64(aggregate.atimeMin, guta.Atime)
		aggregate.mtimeMax = max(aggregate.mtimeMax, guta.Mtime)
		aggregate.atimeBuckets = sumUint64Slices(aggregate.atimeBuckets, guta.ATimeRanges[:])
		aggregate.mtimeBuckets = sumUint64Slices(aggregate.mtimeBuckets, guta.MTimeRanges[:])
		aggregate.ft |= uint16(guta.FT)
		uidSet[guta.UID] = true
		gidSet[guta.GID] = true
	}

	aggregate.uids = sortedUint32Keys(uidSet)
	aggregate.gids = sortedUint32Keys(gidSet)

	return aggregate
}

func fillActiveVirtualSummaries( //nolint:funlen
	rows []activeVirtualSummaryRow,
	contributors map[string][]string,
	rootGUTAs map[string]db.GUTAs,
	mountChildCounts map[string]uint64,
) {
	for i := range rows {
		gutas := mergeRootGUTAsForActiveVirtual(contributors[rows[i].Dir], rootGUTAs)

		all := activeVirtualGUTAAggregateForFilter(gutas, &db.Filter{Age: db.DGUTAgeAll})
		if all.count > 0 {
			rows[i].AllCount = all.count
			rows[i].AllSize = all.size
			rows[i].AllAtimeMin = all.atimeMin
			rows[i].AllMtimeMax = all.mtimeMax
			rows[i].AllAtimeBuckets = all.atimeBuckets
			rows[i].AllMtimeBuckets = all.mtimeBuckets
			rows[i].AllUIDs = all.uids
			rows[i].AllGIDs = all.gids
			rows[i].AllFT = all.ft
		}

		files := activeVirtualGUTAAggregateForFilter(
			gutas,
			&db.Filter{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories},
		)
		if files.count > 0 {
			rows[i].FileCount = files.count
			rows[i].FileSize = files.size
		}

		if rows[i].IsMountRootBox == 1 {
			rows[i].ChildCount = mountChildCounts[rows[i].MountPath]
		} else if contributorCount := uint64(len(contributors[rows[i].Dir])); contributorCount > rows[i].ChildCount {
			rows[i].ChildCount = contributorCount
		}
	}
}

func mergeRootGUTAsForActiveVirtual(
	mountPaths []string,
	rootGUTAs map[string]db.GUTAs,
) db.GUTAs {
	var gutas db.GUTAs
	for _, mountPath := range mountPaths {
		gutas = append(gutas, rootGUTAs[mountPath]...)
	}

	return gutas
}

type activeVirtualFilterAggregate struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func addActiveVirtualFilterAggregate(
	aggregate activeVirtualFilterAggregate,
	row activeVirtualFilterAllRow,
) activeVirtualFilterAggregate {
	aggregate.count += row.Count
	aggregate.size += row.Size
	aggregate.atimeMin = minNonZeroInt64(aggregate.atimeMin, row.AtimeMin)
	aggregate.mtimeMax = max(aggregate.mtimeMax, row.MtimeMax)
	aggregate.atimeBuckets = sumUint64Slices(aggregate.atimeBuckets, row.AtimeBuckets)
	aggregate.mtimeBuckets = sumUint64Slices(aggregate.mtimeBuckets, row.MtimeBuckets)

	return aggregate
}

//nolint:funlen
func aggregateActiveVirtualFilterRows(
	summaryRows []activeVirtualSummaryRow,
	contributors map[string][]string,
	filterRows map[string][]activeVirtualFilterAllRow,
	refreshedAt time.Time,
) []activeVirtualFilterAllRow {
	out := make([]activeVirtualFilterAllRow, 0, len(summaryRows)*len(filterRows))
	for _, summaryRow := range summaryRows {
		aggregates := make(map[string]activeVirtualFilterAggregate)
		keys := make(map[string]activeVirtualFilterAllRow)

		for _, mountPath := range contributors[summaryRow.Dir] {
			for _, row := range filterRows[mountPath] {
				key := activeVirtualFilterSortKey(row)
				keys[key] = row
				aggregates[key] = addActiveVirtualFilterAggregate(aggregates[key], row)
			}
		}

		for key, aggregate := range aggregates {
			row := keys[key]
			row.ActiveSetID = summaryRow.ActiveSetID
			row.Dir = summaryRow.Dir
			row.Count = aggregate.count
			row.Size = aggregate.size
			row.AtimeMin = aggregate.atimeMin
			row.MtimeMax = aggregate.mtimeMax
			row.AtimeBuckets = aggregate.atimeBuckets
			row.MtimeBuckets = aggregate.mtimeBuckets
			row.FilterChildCount = summaryRow.ChildCount
			row.ChildCount = summaryRow.ChildCount
			row.RefreshedAt = refreshedAt
			out = append(out, row)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(activeVirtualFilterSortKey(out[i]), activeVirtualFilterSortKey(out[j])) < 0
	})

	return out
}

func activeVirtualDirsQuery(query string, activeSetID string, dirs []string) (string, []any) {
	args := make([]any, 0, len(dirs)+1)

	args = append(args, activeSetID)
	for _, dir := range dirs {
		args = append(args, ensureTrailingSlash(dir))
	}

	return query, args
}

func activeVirtualRowsForMountsFromData(
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
	rootGUTAs map[string]db.GUTAs,
	filterRows map[string][]activeVirtualFilterAllRow,
	mountChildCounts map[string]uint64,
) ([]activeVirtualSummaryRow, []activeVirtualFilterAllRow, []activeVirtualChildRow) {
	childRows := activeVirtualChildRowsForMounts(activeSetID, mounts, refreshedAt)
	summaryRows := activeVirtualSummaryRowsForChildren(activeSetID, mounts, childRows, refreshedAt)
	contributors := activeVirtualContributors(summaryRows, mounts)

	fillActiveVirtualSummaries(summaryRows, contributors, rootGUTAs, mountChildCounts)
	filterAllRows := aggregateActiveVirtualFilterRows(summaryRows, contributors, filterRows, refreshedAt)
	fillActiveVirtualChildCounts(childRows, summaryRows)

	return summaryRows, filterAllRows, childRows
}

func activeVirtualContributors(
	summaryRows []activeVirtualSummaryRow,
	mounts []activeMount,
) map[string][]string {
	contributors := make(map[string][]string, len(summaryRows))
	for _, row := range summaryRows {
		if row.IsMountRootBox == 1 {
			contributors[row.Dir] = []string{row.MountPath}

			continue
		}

		for _, mount := range mounts {
			if strings.HasPrefix(mount.mountPath, row.Dir) {
				contributors[row.Dir] = append(contributors[row.Dir], mount.mountPath)
			}
		}
	}

	return contributors
}

func fillActiveVirtualChildCounts(childRows []activeVirtualChildRow, summaryRows []activeVirtualSummaryRow) {
	childCounts := make(map[string]uint64, len(summaryRows))
	for _, row := range summaryRows {
		childCounts[row.Dir] = row.ChildCount
	}

	for i := range childRows {
		childRows[i].ChildCount = childCounts[activeVirtualSummaryDirForChild(childRows[i])]
	}
}

func sortedUint32Keys(values map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(values))
	for value := range values {
		out = append(out, value)
	}

	slices.Sort(out)

	return out
}

func minNonZeroInt64(a, b int64) int64 {
	if a == 0 {
		return b
	}

	if b == 0 {
		return a
	}

	return min(a, b)
}

func sumUint64Slices(a, b []uint64) []uint64 {
	if len(a) == 0 {
		return append([]uint64(nil), b...)
	}

	for i := range min(len(a), len(b)) {
		a[i] += b[i]
	}

	return a
}

func activeVirtualFullFilterOptionalClauses(filter *db.Filter) (string, []any) {
	clauses := make([]string, 0, fullFilterOptionalClauseCount)
	args := make([]any, 0, dirSummaryFilterArgCap(filter))

	appendIDMembershipClause(&clauses, &args, "v.gid", filter.GIDs)
	appendIDMembershipClause(&clauses, &args, "v.uid", filter.UIDs)
	appendFTMembershipClause(&clauses, &args, "v.ft", filter.FT)

	if len(clauses) == 0 {
		return "", args
	}

	return " AND " + strings.Join(clauses, " AND "), args
}

func activeVirtualDirInfoCandidateDirs(
	dirs []string,
	mounts []activeMount,
	filter *db.Filter,
) []string {
	candidates := activeVirtualCandidateDirs(dirs, mounts)
	if !activeVirtualCanSummarizeExactMountRoot(filter) {
		return candidates
	}

	seen := make(map[string]bool, len(candidates))
	for _, dir := range candidates {
		seen[ensureTrailingSlash(dir)] = true
	}

	for _, dir := range dirs {
		key := ensureTrailingSlash(dir)
		if seen[key] || !activeVirtualExactMountRootCandidate(key, mounts) {
			continue
		}

		candidates = append(candidates, dir)
		seen[key] = true
	}

	return candidates
}

func activeVirtualCandidateDirs(dirs []string, mounts []activeMount) []string {
	candidates := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := ensureTrailingSlash(dir)
		if seen[key] || !activeVirtualCandidateDir(key, mounts) {
			continue
		}

		candidates = append(candidates, dir)
		seen[key] = true
	}

	return candidates
}

func activeVirtualCandidateDir(dir string, mounts []activeMount) bool {
	key := ensureTrailingSlash(dir)
	for _, mount := range mounts {
		mountPath := ensureTrailingSlash(mount.mountPath)
		if key != mountPath && strings.HasPrefix(mountPath, key) {
			return true
		}
	}

	return false
}

func activeVirtualCanSummarizeExactMountRoot(filter *db.Filter) bool {
	if filter == nil || fullFilterAlwaysEmpty(filter) {
		return false
	}

	if _, ok := mountDirSummaryModeForFilter(filter); ok {
		return false
	}

	return dirFilterAllCanHandleFilter(filter)
}

func activeVirtualExactMountRootCandidate(dir string, mounts []activeMount) bool {
	for _, mount := range mounts {
		if dir == ensureTrailingSlash(mount.mountPath) {
			return true
		}
	}

	return false
}

func (d *clickHouseDatabase) activeVirtualReadySetForDirs(
	ctx context.Context,
	dirs []string,
) (string, []string, bool, error) {
	activeSetID, mounts, err := d.currentActiveMountsSet(ctx)
	if err != nil || activeSetID == "" {
		return "", nil, false, err
	}

	candidates := activeVirtualCandidateDirs(dirs, mounts)
	if len(candidates) == 0 {
		return activeSetID, nil, false, nil
	}

	ready, err := d.activeVirtualSetReadyCached(ctx, activeSetID)
	if err != nil || !ready {
		return activeSetID, candidates, false, err
	}

	return activeSetID, candidates, true, nil
}

func (d *clickHouseDatabase) activeVirtualReadySetForDirInfos(
	ctx context.Context,
	dirs []string,
	filter *db.Filter,
) (string, []activeMount, []string, bool, error) {
	activeSetID, mounts, err := d.currentActiveMountsSet(ctx)
	if err != nil || activeSetID == "" {
		return "", nil, nil, false, err
	}

	candidates := activeVirtualDirInfoCandidateDirs(dirs, mounts, filter)
	if len(candidates) == 0 {
		return activeSetID, mounts, nil, false, nil
	}

	ready, err := d.activeVirtualSetReadyCached(ctx, activeSetID)
	if err != nil || !ready {
		return activeSetID, mounts, candidates, false, err
	}

	return activeSetID, mounts, candidates, true, nil
}

func (d *clickHouseDatabase) activeVirtualSetReadyCached(ctx context.Context, activeSetID string) (bool, error) {
	key := newTreeActiveMetadataCacheKey(
		activeSetID,
		currentSchemaVersion,
		activeVirtualReadyQueryVersion,
	)
	if ready, cached := d.treeCache.getActiveVirtualReady(key); cached {
		return ready, nil
	}

	ready, err := d.activeVirtualSetReady(ctx, activeSetID)
	if err != nil {
		return ready, err
	}

	d.treeCache.putActiveVirtualReady(key, ready)

	return ready, nil
}

func (d *clickHouseDatabase) activeVirtualSetReady(ctx context.Context, activeSetID string) (bool, error) {
	rows, err := d.conn.Query(ctx, activeVirtualReadyQuery, activeSetID)
	if err != nil {
		if isUnknownTable(err) {
			return false, nil
		}

		return false, fmt.Errorf("clickhouse: failed to query active virtual readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return true, nil
	}

	return false, rowIterationErr(rows, "clickhouse: active virtual readiness iteration error")
}

//nolint:funlen,gocognit,gocyclo
func (d *clickHouseDatabase) addActiveVirtualDirInfos(
	result map[string]*db.DirSummary,
	dirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	handled := make(map[string]bool, len(dirs))
	if len(dirs) == 0 || d.conn == nil {
		return handled, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	activeSetID, mounts, dirs, ready, err := d.activeVirtualReadySetForDirInfos(ctx, dirs, filter)
	if err != nil || !ready {
		return handled, err
	}

	if fullFilterAlwaysEmpty(filter) {
		return d.activeVirtualExistingDirs(ctx, activeSetID, dirs)
	}

	updatedAt := maxUpdatedAtForMounts(mounts)

	summaries, handled, ok, err := d.activeVirtualSummaries(ctx, activeSetID, updatedAt, dirs, filter)
	if err != nil || !ok {
		return handled, err
	}

	for _, dir := range dirs {
		key := ensureTrailingSlash(dir)

		sum := summaries[key]
		if sum == nil {
			continue
		}

		cp := cloneDirSummary(sum)
		cp.Dir = dir
		result[dir] = cp
	}

	return handled, nil
}

func (d *clickHouseDatabase) activeVirtualSummaries(
	ctx context.Context,
	activeSetID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if mode, ok := mountDirSummaryModeForFilter(filter); ok {
		if mode == mountDirSummaryFiles {
			return d.activeVirtualFullFilterSummaries(ctx, activeSetID, updatedAt, dirs, filter)
		}

		return d.activeVirtualScalarSummaries(ctx, activeSetID, updatedAt, dirs, filter)
	}

	if !dirFilterAllCanHandleFilter(filter) {
		return nil, nil, false, nil
	}

	return d.activeVirtualFullFilterSummaries(ctx, activeSetID, updatedAt, dirs, filter)
}

func (d *clickHouseDatabase) activeVirtualScalarSummaries( //nolint:funlen
	ctx context.Context,
	activeSetID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	countColumn, sizeColumn := "all_count", "all_size"
	if mode, _ := mountDirSummaryModeForFilter(filter); mode == mountDirSummaryFiles {
		countColumn, sizeColumn = "file_count", "file_size"
	}

	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(activeVirtualScalarSummaryQuery, countColumn, sizeColumn, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, nil, false, nil
		}

		return nil, nil, true, fmt.Errorf("clickhouse: failed to query active virtual summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, handled, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, handled, true, err
}

func (d *clickHouseDatabase) activeVirtualFullFilterSummaries( //nolint:funlen
	ctx context.Context,
	activeSetID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	clauses, filterArgs := activeVirtualFullFilterOptionalClauses(filter)
	query := fmt.Sprintf(activeVirtualFullFilterSummaryQuery, clauses, placeholders(len(dirs)))
	args := make([]any, 0, 2+len(filterArgs)+len(dirs))
	args = append(args, activeSetID, uint8(filter.Age))

	args = append(args, filterArgs...)
	for _, dir := range dirs {
		args = append(args, ensureTrailingSlash(dir))
	}

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, nil, false, nil
		}

		return nil, nil, true, fmt.Errorf("clickhouse: failed to query active virtual filter summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, handled, err := scanDirSummaryRows(rows, filter, updatedAt)
	if err != nil {
		return nil, nil, true, err
	}

	existing, err := d.activeVirtualExistingDirs(ctx, activeSetID, dirs)
	if err != nil {
		return nil, nil, true, err
	}

	for dir := range existing {
		handled[dir] = true
	}

	return summaries, handled, true, nil
}

func (d *clickHouseDatabase) activeVirtualExistingDirs( //nolint:funlen
	ctx context.Context,
	activeSetID string,
	dirs []string,
) (map[string]bool, error) {
	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(activeVirtualExistingDirsQuery, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return map[string]bool{}, nil
		}

		return nil, fmt.Errorf("clickhouse: failed to query active virtual dirs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	handled := make(map[string]bool, len(dirs))

	for rows.Next() {
		var dir string
		if err := rows.Scan(&dir); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active virtual dir: %w", err)
		}

		handled[dir] = true
	}

	return handled, rowIterationErr(rows, "clickhouse: active virtual dirs iteration error")
}

//nolint:funlen,gocyclo
func (d *clickHouseDatabase) activeVirtualChildren(
	parentDir string,
) ([]string, bool, error) {
	if d.conn == nil {
		return nil, false, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	activeSetID, dirs, ready, err := d.activeVirtualReadySetForDirs(ctx, []string{parentDir})
	if err != nil || !ready {
		return nil, false, err
	}

	parentDir = ensureTrailingSlash(dirs[0])

	children, err := d.queryChildren(ctx, activeVirtualChildrenQuery, "active virtual children", activeSetID, parentDir)
	if err != nil {
		return nil, false, err
	}

	if len(children) > 0 {
		return children, true, nil
	}

	isMountRootBox, err := d.activeVirtualDirIsMountRootBox(ctx, activeSetID, parentDir)
	if err != nil || isMountRootBox {
		return nil, false, err
	}

	handled, err := d.activeVirtualExistingDirs(ctx, activeSetID, []string{parentDir})
	if err != nil {
		return nil, false, err
	}

	return nil, handled[ensureTrailingSlash(parentDir)], nil
}

func (d *clickHouseDatabase) activeVirtualDirIsMountRootBox(
	ctx context.Context,
	activeSetID string,
	dir string,
) (bool, error) {
	rows, err := d.conn.Query(ctx, activeVirtualMountRootBoxQuery, activeSetID, ensureTrailingSlash(dir))
	if err != nil {
		if isUnknownTable(err) {
			return false, nil
		}

		return false, fmt.Errorf("clickhouse: failed to query active virtual mount-root box: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return false, rowIterationErr(rows, "clickhouse: active virtual mount-root box iteration error")
	}

	var isMountRootBox uint8
	if err := rows.Scan(&isMountRootBox); err != nil {
		return false, fmt.Errorf("clickhouse: failed to scan active virtual mount-root box: %w", err)
	}

	return isMountRootBox == 1, nil
}

//nolint:funlen,gocyclo
func (d *clickHouseDatabase) activeVirtualChildrenForParents(
	dirs []string,
) (map[string][]string, map[string]bool, error) {
	if d.conn == nil || len(dirs) == 0 {
		return map[string][]string{}, map[string]bool{}, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	activeSetID, dirs, ready, err := d.activeVirtualReadySetForDirs(ctx, dirs)
	if err != nil || !ready {
		return map[string][]string{}, map[string]bool{}, err
	}

	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(activeVirtualChildrenForParentsQuery, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return map[string][]string{}, map[string]bool{}, nil
		}

		return nil, nil, fmt.Errorf("clickhouse: failed to query active virtual children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	children, err := scanChildrenRowsByParent(rows)
	if err != nil {
		return nil, nil, err
	}

	handled, err := d.activeVirtualExistingDirs(ctx, activeSetID, dirs)
	if err != nil {
		return nil, nil, err
	}

	return children, handled, nil
}

//nolint:funlen,gocyclo
func (d *clickHouseDatabase) activeVirtualHasChildren(
	dir string,
	filter *db.Filter,
) (bool, bool, error) {
	if !broadFilterCanUseChildRows(filter) {
		return false, false, nil
	}

	if d.conn == nil {
		return false, false, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	activeSetID, dirs, ready, err := d.activeVirtualReadySetForDirs(ctx, []string{dir})
	if err != nil || !ready {
		return false, false, err
	}

	rows, err := d.conn.Query(ctx, activeVirtualChildCountQuery, activeSetID, ensureTrailingSlash(dirs[0]))
	if err != nil {
		if isUnknownTable(err) {
			return false, false, nil
		}

		return false, false, fmt.Errorf("clickhouse: failed to query active virtual child count: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return false, false, rowIterationErr(rows, "clickhouse: active virtual child count iteration error")
	}

	var childCount uint64
	if err := rows.Scan(&childCount); err != nil {
		return false, false, fmt.Errorf("clickhouse: failed to scan active virtual child count: %w", err)
	}

	return childCount > 0, true, nil
}
