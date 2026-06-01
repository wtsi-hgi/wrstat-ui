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

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	dirFilterAgeAllTableName = "wrstat_dir_filter_ageall"

	dirFilterAgeAllActiveRowsQuery = "SELECT sum(rows) FROM system.parts " +
		"WHERE database = currentDatabase() AND table = ? AND active"

	dirFilterAgeAllWhereSummariesQuery = "SELECT dir, count() AS raw_rows, " +
		"sum(count) AS total_count, " +
		"sum(size) AS total_size, " +
		"minIf(atime_min, atime_min != 0) AS atime_min, " +
		"max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, " +
		"arraySort(groupUniqArray(gid)) AS gids, " +
		"groupBitOr(ft) AS file_types " +
		"FROM wrstat_dir_filter_ageall " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE %s " +
		"GROUP BY dir"

	dirsHaveMatchingChildrenAgeAllQuery = "SELECT c.parent_dir " +
		"FROM wrstat_children c " +
		"INNER JOIN wrstat_dir_filter_ageall f " +
		"ON f.mount_path = c.mount_path " +
		"AND f.snapshot_id = c.snapshot_id " +
		"AND f.dir = if(endsWith(c.child, '/'), c.child, concat(c.child, '/')) " +
		"WHERE c.mount_path = ? AND c.snapshot_id = ? " +
		"AND c.parent_dir IN (%s) AND %s " +
		"GROUP BY c.parent_dir " +
		"ORDER BY c.parent_dir ASC"
)

func dirFilterAgeAllCanHandleFilter(filter *db.Filter) bool {
	if filter == nil || filter.Age != db.DGUTAgeAll {
		return false
	}

	if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return false
	}

	return filter.GIDs != nil || filter.UIDs != nil || filter.FT != 0
}

func dirFilterAgeAllFilterExpression(filter *db.Filter) (string, []any) {
	if filter == nil || isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return "0", nil
	}

	clauses := make([]string, 0, dirSummaryFilterClauseInitialCap)
	args := make([]any, 0, len(filter.GIDs)+len(filter.UIDs)+1)

	appendIDMembershipClause(&clauses, &args, "gid", filter.GIDs)
	appendIDMembershipClause(&clauses, &args, "uid", filter.UIDs)
	appendFTMembershipClause(&clauses, &args, "ft", filter.FT)

	if len(clauses) == 0 {
		return "0", nil
	}

	return strings.Join(clauses, " AND "), args
}

func (d *clickHouseDatabase) dirFilterAgeAllReadyForFilter(
	ctx context.Context,
	filter *db.Filter,
) (bool, error) {
	if !dirFilterAgeAllCanHandleFilter(filter) {
		return false, nil
	}

	var rows uint64
	if err := d.conn.QueryRow(ctx, dirFilterAgeAllActiveRowsQuery, dirFilterAgeAllTableName).Scan(&rows); err != nil {
		return false, fmt.Errorf("clickhouse: failed to query AgeAll filter index readiness: %w", err)
	}

	return rows > 0, nil
}

func (d *clickHouseDatabase) dirFilterAgeAllWhereSummaries(
	ctx context.Context,
	mount activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, filter)
	if err != nil || !ready {
		return nil, false, err
	}

	filterExpr, filterArgs := dirFilterAgeAllFilterExpression(filter)
	query := fmt.Sprintf(dirFilterAgeAllWhereSummariesQuery, filterExpr)
	args := make([]any, 0, queryScopeArgs+len(filterArgs))
	args = append(args, mount.mountPath, mount.snapshotID)
	args = append(args, filterArgs...)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, true, fmt.Errorf("clickhouse: failed to query AgeAll filter index summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)
	if err != nil || len(summaries) > 0 {
		return summaries, true, err
	}

	return nil, false, nil
}

func (d *clickHouseDatabase) parentDirsWithAgeAllFilteredChildRows(
	ctx context.Context,
	group *activeMountDirGroup,
	parentDirs []string,
	filter *db.Filter,
) (map[string]bool, bool, error) {
	if len(parentDirs) == 0 {
		return map[string]bool{}, true, nil
	}

	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, filter)
	if err != nil || !ready {
		return nil, false, err
	}

	parents, err := d.queryAgeAllFilteredChildParentBatches(ctx, group, parentDirs, filter)
	if err != nil {
		return nil, true, err
	}

	if len(parents) > 0 {
		return parents, true, nil
	}

	return nil, false, nil
}

func (d *clickHouseDatabase) queryAgeAllFilteredChildParentBatches(
	ctx context.Context,
	group *activeMountDirGroup,
	parentDirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	parents := make(map[string]bool)

	for _, batchDirs := range stringValueBatches(parentDirs) {
		batchParents, err := d.queryAgeAllFilteredChildParents(ctx, group, batchDirs, filter)
		if err != nil {
			return nil, err
		}

		for parent := range batchParents {
			parents[parent] = true
		}
	}

	return parents, nil
}

func (d *clickHouseDatabase) queryAgeAllFilteredChildParents(
	ctx context.Context,
	group *activeMountDirGroup,
	parentDirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	filterExpr, filterArgs := dirFilterAgeAllFilterExpression(filter)
	query := fmt.Sprintf(dirsHaveMatchingChildrenAgeAllQuery, placeholders(len(parentDirs)), filterExpr)
	args := make([]any, 0, queryScopeArgs+len(parentDirs)+len(filterArgs))
	args = append(args, group.mount.mountPath, group.mount.snapshotID)

	for _, parentDir := range parentDirs {
		args = append(args, parentDir)
	}

	args = append(args, filterArgs...)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query AgeAll filtered child dirs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentDirSet(rows)
}
