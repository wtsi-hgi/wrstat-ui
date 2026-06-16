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
	activeVirtualScalarSummaryQuery = "SELECT d.full_path AS dir, toUInt64(1) AS raw_rows, " +
		"%s AS total_count, %s AS total_size, " +
		"s.all_atime_min, s.all_mtime_max, s.all_atime_buckets, s.all_mtime_buckets, " +
		"s.all_uids, s.all_gids, s.all_ft " +
		"FROM wrstat_active_virtual_summaries AS s " +
		"INNER JOIN wrstat_active_virtual_dirs AS d " +
		"ON d.active_set_id = s.active_set_id AND d.virtual_id = s.virtual_id " +
		"PREWHERE s.active_set_id = ? WHERE d.full_path IN (%s)"
	activeVirtualFullFilterSummaryQuery = "SELECT d.full_path AS dir, count() AS raw_rows, sum(v.count) AS total_count, " +
		"sum(size) AS total_size, minIf(atime_min, atime_min != 0) AS atime_min, max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, arraySort(groupUniqArray(gid)) AS gids, groupBitOr(v.ft) AS ft " +
		"FROM wrstat_active_virtual_filter_all AS v " +
		"INNER JOIN wrstat_active_virtual_dirs AS d " +
		"ON d.active_set_id = v.active_set_id AND d.virtual_id = v.virtual_id " +
		"PREWHERE v.active_set_id = ? WHERE v.age = ? %s AND d.full_path IN (%s) GROUP BY d.full_path"
	activeVirtualExistingDirsQuery = "SELECT full_path FROM wrstat_active_virtual_dirs " +
		"PREWHERE active_set_id = ? WHERE full_path IN (%s)"
	activeVirtualCatalogChildrenQuery = "SELECT child.full_path FROM wrstat_active_virtual_dirs AS parent " +
		"INNER JOIN wrstat_active_virtual_dirs AS child " +
		"ON child.active_set_id = parent.active_set_id AND child.parent_id = parent.virtual_id " +
		"PREWHERE parent.active_set_id = ? WHERE parent.full_path = ? ORDER BY child.virtual_id"
	activeVirtualCatalogChildrenForParentsQuery = "SELECT parent.full_path, child.full_path " +
		"FROM wrstat_active_virtual_dirs AS parent INNER JOIN wrstat_active_virtual_dirs AS child " +
		"ON child.active_set_id = parent.active_set_id AND child.parent_id = parent.virtual_id " +
		"PREWHERE parent.active_set_id = ? WHERE parent.full_path IN (%s) " +
		"ORDER BY parent.virtual_id, child.virtual_id"
	activeVirtualChildCountQuery = "SELECT s.child_count FROM wrstat_active_virtual_summaries AS s " +
		"INNER JOIN wrstat_active_virtual_dirs AS d " +
		"ON d.active_set_id = s.active_set_id AND d.virtual_id = s.virtual_id " +
		"PREWHERE s.active_set_id = ? WHERE d.full_path = ? LIMIT 1"
	activeVirtualMountRootBoxQuery = "SELECT is_mount_root_box FROM wrstat_active_virtual_dirs " +
		"PREWHERE active_set_id = ? WHERE full_path = ? LIMIT 1"
	activeVirtualMountRootBoxesQuery = "SELECT full_path, is_mount_root_box FROM wrstat_active_virtual_dirs " +
		"PREWHERE active_set_id = ? WHERE full_path IN (%s)"
	activeVirtualRootGUTAsQuery = "SELECT dir, " + dgutaTupleColumns + " FROM (" +
		"SELECT d.mount_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE c.full_path = d.mount_path AND %s)"
	activeVirtualRootFilterRowsQuery = "SELECT f.mount_path, f.age, f.gid, f.uid, f.ft, f.count, f.size, " +
		"f.atime_min, f.mtime_max, f.atime_buckets, f.mtime_buckets, f.filter_child_count, f.child_count " +
		"FROM wrstat_dir_filter_all AS f INNER JOIN wrstat_dirs AS c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE c.full_path = f.mount_path AND %s ORDER BY f.mount_path, f.age, f.gid, f.uid, f.ft"
	activeVirtualMountChildCountsQuery = "SELECT d.mount_path, toString(any(d.snapshot_id)), any(d.dir_id), " +
		"toUInt64(any(d.child_dir_count)) FROM wrstat_dirs d " +
		"WHERE d.full_path = d.mount_path AND %s GROUP BY d.mount_path"
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
		"f.mount_path",
		"f.snapshot_id",
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

//nolint:unused
func queryActiveVirtualMountChildCounts(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[string]uint64, error) {
	links, err := queryActiveVirtualMountRootLinks(ctx, conn, mounts)
	if err != nil {
		return nil, err
	}

	counts := make(map[string]uint64, len(links))
	for mountPath, link := range links {
		counts[mountPath] = link.ChildCount
	}

	return counts, nil
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
	mountRootLinks map[string]activeVirtualMountRootLink,
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
			rows[i].ChildCount = mountRootLinks[ensureTrailingSlash(rows[i].MountPath)].ChildCount
		} else if contributorCount := uint64(len(contributors[rows[i].Dir])); rows[i].ChildCount == 0 {
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
			row.VirtualID = summaryRow.VirtualID
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

func queryActiveVirtualMountRootLinks( //nolint:funlen
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (map[string]activeVirtualMountRootLink, error) {
	query, args := activeMountsQuery(
		activeVirtualMountChildCountsQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active virtual mount-root links: %w", err)
	}

	defer func() { _ = rows.Close() }()

	links := make(map[string]activeVirtualMountRootLink)

	for rows.Next() {
		var (
			mountPath  string
			snapshotID string
			dirID      uint32
			childCount uint64
		)
		if err := rows.Scan(&mountPath, &snapshotID, &dirID, &childCount); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active virtual mount-root link: %w", err)
		}

		links[ensureTrailingSlash(mountPath)] = activeVirtualMountRootLink{
			SnapshotID: snapshotID,
			DirID:      dirID,
			ChildCount: childCount,
		}
	}

	return links, rowIterationErr(rows, "clickhouse: active virtual mount-root link iteration error")
}

func activeVirtualDirsQuery(query string, activeSetID string, dirs []string) (string, []any) {
	args := make([]any, 0, len(dirs)+1)

	args = append(args, activeSetID)
	for _, dir := range dirs {
		args = append(args, ensureTrailingSlash(dir))
	}

	return query, args
}

//nolint:unused
func activeVirtualRowsForMountsFromData(
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
	rootGUTAs map[string]db.GUTAs,
	filterRows map[string][]activeVirtualFilterAllRow,
	mountChildCounts map[string]uint64,
) ([]activeVirtualSummaryRow, []activeVirtualFilterAllRow, []activeVirtualChildRow) {
	links := activeVirtualLinksFromChildCounts(mounts, mountChildCounts)
	_, summaryRows, filterAllRows, childRows := activeVirtualRowsForMountsFromDataWithLinks(
		activeSetID,
		mounts,
		refreshedAt,
		rootGUTAs,
		filterRows,
		links,
	)

	return summaryRows, filterAllRows, childRows
}

//nolint:unused
func activeVirtualLinksFromChildCounts(
	mounts []activeMount,
	counts map[string]uint64,
) map[string]activeVirtualMountRootLink {
	if counts == nil {
		return nil
	}

	links := make(map[string]activeVirtualMountRootLink, len(mounts))
	for _, mount := range mounts {
		mountPath := ensureTrailingSlash(mount.mountPath)
		links[mountPath] = activeVirtualMountRootLink{
			SnapshotID: mount.snapshotID,
			ChildCount: counts[mountPath],
		}
	}

	return links
}

func activeVirtualRowsForMountsFromDataWithLinks(
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
	rootGUTAs map[string]db.GUTAs,
	filterRows map[string][]activeVirtualFilterAllRow,
	mountRootLinks map[string]activeVirtualMountRootLink,
) (activeVirtualNamespace, []activeVirtualSummaryRow, []activeVirtualFilterAllRow, []activeVirtualChildRow) {
	namespace := activeVirtualNamespaceForMounts(activeSetID, mounts, mountRootLinks, refreshedAt)
	childRows := namespace.childRows(activeSetID, refreshedAt)
	summaryRows := activeVirtualSummaryRowsForChildren(activeSetID, mounts, childRows, refreshedAt)
	contributors := activeVirtualContributors(summaryRows, mounts)

	fillActiveVirtualSummaries(summaryRows, contributors, rootGUTAs, mountRootLinks)
	filterAllRows := aggregateActiveVirtualFilterRows(summaryRows, contributors, filterRows, refreshedAt)
	fillActiveVirtualChildCounts(childRows, summaryRows)

	return namespace, summaryRows, filterAllRows, childRows
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
	includeExactMountRoots bool,
) []string {
	candidates := activeVirtualCandidateDirs(dirs, mounts)
	if !activeVirtualCanIncludeExactMountRootCandidates(filter, mounts, includeExactMountRoots) {
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
		if mountPath != key && strings.HasPrefix(mountPath, key) {
			return true
		}
	}

	return false
}

func activeVirtualCanIncludeExactMountRootCandidates(
	filter *db.Filter,
	mounts []activeMount,
	includeExactMountRoots bool,
) bool {
	if !includeExactMountRoots {
		return false
	}

	if !activeVirtualCanSummarizeExactMountRoot(filter) {
		return false
	}

	return activeVirtualCanSummarizeMountRootBoxes(mounts)
}

func activeVirtualCanSummarizeExactMountRoot(filter *db.Filter) bool {
	if fullFilterAlwaysEmpty(filter) {
		return false
	}

	if _, ok := mountDirSummaryModeForFilter(filter); ok {
		return true
	}

	return filter == nil || dirFilterAllCanHandleFilter(filter)
}

func activeVirtualCanSummarizeMountRootBoxes(mounts []activeMount) bool {
	return len(mounts) > 0
}

func activeVirtualExactMountRootCandidate(dir string, mounts []activeMount) bool {
	for _, mount := range mounts {
		if dir == ensureTrailingSlash(mount.mountPath) {
			return true
		}
	}

	return false
}

func activeVirtualHandledDirsWithoutChildren(
	dirs []string,
	children map[string][]string,
	handled map[string]bool,
) []string {
	out := make([]string, 0, len(dirs))

	for _, dir := range dirs {
		key := ensureTrailingSlash(dir)
		if handled[key] && len(children[key]) == 0 {
			out = append(out, key)
		}
	}

	return out
}

func scanActiveVirtualMountRootBoxes(rows rowsScanner) (map[string]bool, error) {
	out := make(map[string]bool)

	for rows.Next() {
		var (
			dir            string
			isMountRootBox uint8
		)

		if err := rows.Scan(&dir, &isMountRootBox); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active virtual mount-root box: %w", err)
		}

		out[dir] = isMountRootBox == 1
	}

	return out, rowIterationErr(rows, "clickhouse: active virtual mount-root boxes iteration error")
}

func (d *clickHouseDatabase) activeVirtualExactMountRootCandidatesAllowed(mounts []activeMount) bool {
	return d.snapshot != nil || len(mounts) > 1
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

	candidates := activeVirtualDirInfoCandidateDirs(
		dirs,
		mounts,
		filter,
		d.activeVirtualExactMountRootCandidatesAllowed(mounts),
	)
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
	if filter == nil {
		return d.activeVirtualScalarSummaries(ctx, activeSetID, updatedAt, dirs, filter)
	}

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

	countColumn = "s." + countColumn
	sizeColumn = "s." + sizeColumn

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

//nolint:cyclop,funlen,gocognit,gocyclo
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

	rows, err := d.conn.Query(ctx, activeVirtualCatalogChildrenQuery, activeSetID, parentDir)
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("clickhouse: failed to query active virtual children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	children, err := scanChildrenRows(rows)
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

//nolint:funlen,gocognit,gocyclo
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
		fmt.Sprintf(activeVirtualCatalogChildrenForParentsQuery, placeholders(len(dirs))),
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

	if err := d.removeHandledActiveVirtualMountRootBoxLeaves(ctx, activeSetID, dirs, children, handled); err != nil {
		return nil, nil, err
	}

	return children, handled, nil
}

func (d *clickHouseDatabase) removeHandledActiveVirtualMountRootBoxLeaves(
	ctx context.Context,
	activeSetID string,
	dirs []string,
	children map[string][]string,
	handled map[string]bool,
) error {
	dirsWithoutChildren := activeVirtualHandledDirsWithoutChildren(dirs, children, handled)
	if len(dirsWithoutChildren) == 0 {
		return nil
	}

	mountRootBoxes, err := d.activeVirtualMountRootBoxes(ctx, activeSetID, dirsWithoutChildren)
	if err != nil {
		return err
	}

	for dir, isMountRootBox := range mountRootBoxes {
		if isMountRootBox {
			delete(handled, dir)
		}
	}

	return nil
}

func (d *clickHouseDatabase) activeVirtualMountRootBoxes(
	ctx context.Context,
	activeSetID string,
	dirs []string,
) (map[string]bool, error) {
	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(activeVirtualMountRootBoxesQuery, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return map[string]bool{}, nil
		}

		return nil, fmt.Errorf("clickhouse: failed to query active virtual mount-root boxes: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActiveVirtualMountRootBoxes(rows)
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
