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
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	childrenInitialCap            = 16
	childFilterAllPacketBaseArgs  = 4
	dirFactVectorColumnCount      = 10
	fullFilterOptionalClauseCount = 3
	mountDirFactScalarColumns     = 2
	resolvedDirFactScalarColumns  = 4

	dgutaInitialCap                      = 8
	dirsHaveChildrenSummaryPrefetchLimit = 64
	dirsHaveChildrenSummaryFanoutLimit   = 16
	dirSummaryFilterClauseInitialCap     = 3
	groupedDirSummaryMinDirs             = 4096
	queryStringINMaxValues               = 1000
	queryScopeArgs                       = 2
	activeMountDirTupleArgs              = 3

	dirForFullPathQuery = "SELECT dir_id, parent_id, subtree_end, full_path FROM wrstat_dirs " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE path_hash = ? AND full_path = ? " +
		"ORDER BY dir_id LIMIT 1"

	dirForIDQuery = "SELECT dir_id, parent_id, subtree_end, full_path FROM wrstat_dirs " +
		"WHERE mount_path = ? AND snapshot_id = ? AND dir_id = ? " +
		"LIMIT 1"

	whereRangeCatalogQuery = "SELECT dir_id, parent_id, full_path FROM wrstat_dirs " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir_id >= ? AND dir_id < ? " +
		"ORDER BY dir_id"

	dirIDsForDirsQuery = "SELECT full_path, dir_id FROM wrstat_dirs " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND full_path IN (%s) " +
		"ORDER BY full_path ASC, dir_id ASC"

	dirIDsForActiveMountsDirQuery = "SELECT mount_path, toString(snapshot_id), dir_id " +
		"FROM wrstat_dirs WHERE full_path = ? AND %s " +
		"ORDER BY mount_path ASC, snapshot_id ASC, dir_id ASC"

	childrenForDirIDQuery = "SELECT DISTINCT full_path FROM wrstat_dirs " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE parent_id = ? " +
		"ORDER BY full_path"

	dirFactRowsForDirIDsQuery = "SELECT dir_id, updated_at, gids, uids, fts, ages, " +
		"counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets " +
		"FROM wrstat_dir_facts " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir_id IN (%s)"

	dirFactRowsForResolvedDirsQuery = "SELECT mount_path, toString(snapshot_id), dir_id, " +
		"updated_at, gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, " +
		"atime_buckets, mtime_buckets FROM wrstat_dir_facts " +
		"WHERE (mount_path, snapshot_id, dir_id) IN (%s)"

	dgutaArrayZipExpr = "arrayZip(gids, uids, fts, ages, counts, sizes, " +
		"atime_mins, mtime_maxs, atime_buckets, mtime_buckets)"

	dgutaPrefixedArrayZipExpr = "arrayZip(d.gids, d.uids, d.fts, d.ages, d.counts, d.sizes, " +
		"d.atime_mins, d.mtime_maxs, d.atime_buckets, d.mtime_buckets)"

	dgutaTupleColumns = "tupleElement(g, 1) AS gid, tupleElement(g, 2) AS uid, " +
		"tupleElement(g, 3) AS ft, tupleElement(g, 4) AS age, " +
		"tupleElement(g, 5) AS count, tupleElement(g, 6) AS size, " +
		"tupleElement(g, 7) AS atime_min, tupleElement(g, 8) AS mtime_max, " +
		"tupleElement(g, 9) AS atime_buckets, tupleElement(g, 10) AS mtime_buckets"

	dgutaQuery = "SELECT " + dgutaTupleColumns + " FROM (" +
		"SELECT arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE d.mount_path = ? AND d.snapshot_id = ? AND c.full_path = ?)"

	infoDGUTAQuery = "SELECT " +
		"count() AS num_dirs, " +
		"sum(length(gids)) AS num_dgutas " +
		"FROM wrstat_dir_facts " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	infoChildrenQuery = "SELECT " +
		"uniqExact(parent_id) AS num_parents, " +
		"sum(child_dir_count) AS num_children " +
		"FROM wrstat_dirs " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	activeSetIDExpr = "(SELECT lower(hex(SHA256(arrayStringConcat(" +
		"arrayMap(x -> concat(x, unhex('00')), " +
		"arraySort(groupArray(concat(mount_path, '|', toString(snapshot_id), '|', " +
		"formatDateTime(updated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC'))))))))) " +
		"FROM wrstat_mounts_active)"

	resolveMountQuery = "SELECT mount_path, toString(snapshot_id), updated_at, " +
		activeSetIDExpr + " AS active_set_id FROM wrstat_mounts_active " +
		"WHERE startsWith(?, mount_path) " +
		"ORDER BY length(mount_path) DESC LIMIT 1"

	resolveExactMountQuery = "SELECT mount_path, toString(snapshot_id), updated_at, " +
		activeSetIDExpr + " AS active_set_id FROM wrstat_mounts_active " +
		"WHERE mount_path = ? LIMIT 1"

	dgutaAncestorSnapshotQuery = "SELECT " + dgutaTupleColumns + " FROM (" +
		"SELECT arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE c.full_path = ? AND %s)"

	dgutasForActiveMountRootDirsQuery = "SELECT d.mount_path AS dir, " +
		"d.gids, d.uids, d.fts, d.ages, d.counts, d.sizes, " +
		"d.atime_mins, d.mtime_maxs, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE c.full_path = d.mount_path AND %s SETTINGS use_query_cache = 1"

	dgutasForActiveMountDirsQuery = "SELECT dir, " + dgutaTupleColumns + " FROM (" +
		"SELECT c.full_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE (d.mount_path, d.snapshot_id, c.full_path) IN (%s))"

	dgutasForDirsQuery = "SELECT dir, " + dgutaTupleColumns + " FROM (" +
		"SELECT c.full_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE d.mount_path = ? AND d.snapshot_id = ? AND c.full_path IN (%s))"

	dirSummariesForDirsQuery = "SELECT dir, count() AS raw_rows, " +
		"sumIf(file_count, passes_filter) AS total_count, " +
		"sumIf(file_size, passes_filter) AS total_size, " +
		"minIf(atime_min, passes_filter AND atime_min != 0) AS atime_min, " +
		"maxIf(mtime_max, passes_filter) AS mtime_max, " +
		"sumForEachIf(atime_buckets, passes_filter) AS atime_buckets, " +
		"sumForEachIf(mtime_buckets, passes_filter) AS mtime_buckets, " +
		"arraySort(groupUniqArrayIf(uid, passes_filter)) AS uids, " +
		"arraySort(groupUniqArrayIf(gid, passes_filter)) AS gids, " +
		"groupBitOrIf(ft, passes_filter) AS ft " +
		"FROM (" +
		"SELECT dir, tupleElement(g, 1) AS gid, tupleElement(g, 2) AS uid, tupleElement(g, 3) AS ft, " +
		"tupleElement(g, 4) AS age, tupleElement(g, 5) AS file_count, tupleElement(g, 6) AS file_size, " +
		"tupleElement(g, 7) AS atime_min, tupleElement(g, 8) AS mtime_max, " +
		"tupleElement(g, 9) AS atime_buckets, tupleElement(g, 10) AS mtime_buckets, " +
		"%s AS passes_filter " +
		"FROM (" +
		"SELECT c.full_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE d.mount_path = ? AND d.snapshot_id = ? AND c.full_path IN (%s)" +
		")" +
		") " +
		"GROUP BY dir"

	schema3DirFilterAllReadyQuery = "SELECT dir_filter_all_rows FROM wrstat_schema3_snapshot_sets " +
		"WHERE mount_path = ? AND snapshot_id = ? AND schema3_version = ? " +
		"ORDER BY refreshed_at DESC LIMIT 1"

	schema3ChildFilterAllReadyQuery = "SELECT child_filter_all_rows FROM wrstat_schema3_snapshot_sets " +
		"WHERE mount_path = ? AND snapshot_id = ? AND schema3_version = ? " +
		"ORDER BY refreshed_at DESC LIMIT 1"

	dirFilterAllSummariesForDirsQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, " +
		"sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, " +
		"max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, " +
		"arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types " +
		"FROM wrstat_dir_filter_all f " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = ? AND f.age = ? %s AND c.full_path IN (%s) " +
		"GROUP BY c.full_path"

	dirFilterAllWhereSummariesQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, " +
		"sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, " +
		"max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, " +
		"arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types " +
		"FROM wrstat_dir_filter_all f " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = ? AND f.age = ? %s AND startsWith(c.full_path, ?) " +
		"GROUP BY c.full_path"

	whereRangeDirFactsScalarSummariesQuery = "SELECT c.full_path AS dir, toUInt64(1) AS raw_rows, " +
		"%s AS total_count, " +
		"%s AS total_size, " +
		"%s AS atime_min, " +
		"%s AS mtime_max, " +
		"%s AS atime_buckets, " +
		"%s AS mtime_buckets, " +
		"%s AS uids, " +
		"%s AS gids, " +
		"%s AS file_types " +
		"FROM wrstat_dir_facts f " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = ? AND f.dir_id >= ? AND f.dir_id < ? " +
		"ORDER BY c.full_path"

	dirFilterAgeAllWhereRangeSummariesQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, " +
		"sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, " +
		"max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, " +
		"arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types " +
		"FROM wrstat_dir_filter_ageall f " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = ? AND f.dir_id >= ? AND f.dir_id < ? AND %s " +
		"GROUP BY c.full_path"

	childFilterAllChildSummariesPacketQuery = "SELECT child.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, " +
		"sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, " +
		"max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, " +
		"arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types, " +
		"max(f.filter_child_count) AS filter_child_count, " +
		"max(f.child_count) AS child_count, " +
		"max(f.has_filter_children) AS has_filter_children, " +
		"max(f.has_children) AS has_children " +
		"FROM wrstat_child_filter_all f " +
		"INNER JOIN wrstat_dirs parent " +
		"ON parent.mount_path = f.mount_path AND parent.snapshot_id = f.snapshot_id AND parent.dir_id = f.parent_id " +
		"INNER JOIN wrstat_dirs child " +
		"ON child.mount_path = f.mount_path AND child.snapshot_id = f.snapshot_id AND child.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = ? AND parent.full_path = ? AND f.age = ? %s " +
		"GROUP BY child.full_path " +
		"ORDER BY child.full_path"

	childrenForParentsQuery = "SELECT parent.full_path, child.full_path " +
		"FROM wrstat_dirs AS child " +
		"INNER JOIN wrstat_dirs AS parent " +
		"ON parent.mount_path = child.mount_path " +
		"AND parent.snapshot_id = child.snapshot_id " +
		"AND parent.dir_id = child.parent_id " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE parent.full_path IN (%s) " +
		"ORDER BY parent.full_path ASC, child.full_path ASC"

	childrenForExternalParentsQuery = "SELECT parent.full_path, child.full_path " +
		"FROM wrstat_dirs AS child " +
		"INNER JOIN wrstat_dirs AS parent " +
		"ON parent.mount_path = child.mount_path " +
		"AND parent.snapshot_id = child.snapshot_id " +
		"AND parent.dir_id = child.parent_id " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = parent.full_path " +
		"WHERE child.mount_path = ? AND child.snapshot_id = ? " +
		"ORDER BY parent.full_path ASC, child.full_path ASC"

	activeMountRootChildrenQuery = "SELECT parent.full_path, child.full_path " +
		"FROM wrstat_dirs child " +
		"INNER JOIN wrstat_dirs parent " +
		"ON parent.mount_path = child.mount_path " +
		"AND parent.snapshot_id = child.snapshot_id " +
		"AND parent.dir_id = child.parent_id " +
		"WHERE parent.full_path = parent.mount_path AND %s " +
		"ORDER BY parent.full_path ASC, child.full_path ASC"

	dirsHaveCatalogChildrenQuery = "SELECT full_path FROM wrstat_dirs " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE full_path IN (%s) AND child_dir_count > 0 " +
		"ORDER BY full_path ASC"

	dirsHaveFilteredChildrenQuery = "SELECT parent_id FROM wrstat_child_filter_all " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE parent_id IN (%s) AND age = ? %s AND count > 0 " +
		"GROUP BY parent_id ORDER BY parent_id ASC"

	dirsHaveMatchingChildrenQuery = "SELECT parent.full_path " +
		"FROM wrstat_dirs parent " +
		"INNER JOIN wrstat_dirs child " +
		"ON child.mount_path = parent.mount_path " +
		"AND child.snapshot_id = parent.snapshot_id " +
		"AND child.parent_id = parent.dir_id " +
		"INNER JOIN wrstat_dir_facts d " +
		"ON d.mount_path = child.mount_path " +
		"AND d.snapshot_id = child.snapshot_id " +
		"AND d.dir_id = child.dir_id " +
		"WHERE parent.mount_path = ? AND parent.snapshot_id = ? " +
		"AND parent.full_path IN (%s) %s " +
		"GROUP BY parent.full_path " +
		"ORDER BY parent.full_path ASC"

	infoDGUTASnapshotQuery = "SELECT " +
		"count() AS num_dirs, " +
		"sum(length(gids)) AS num_dgutas " +
		"FROM wrstat_dir_facts " +
		"WHERE %s"

	infoChildrenSnapshotQuery = "SELECT " +
		"uniqExact(parent_id) AS num_parents, " +
		"sum(child_dir_count) AS num_children " +
		"FROM wrstat_dirs " +
		"WHERE %s"

	filteredMountWhereSummariesQuery = "SELECT dir, count() AS raw_rows, " +
		"sum(file_count) AS total_count, " +
		"sum(file_size) AS total_size, " +
		"minIf(atime_min, atime_min != 0) AS atime_min, " +
		"max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, " +
		"arraySort(groupUniqArray(gid)) AS gids, " +
		"groupBitOr(ft) AS file_types " +
		"FROM (" +
		"SELECT dir, tupleElement(g, 1) AS gid, tupleElement(g, 2) AS uid, tupleElement(g, 3) AS ft, " +
		"tupleElement(g, 4) AS age, tupleElement(g, 5) AS file_count, tupleElement(g, 6) AS file_size, " +
		"tupleElement(g, 7) AS atime_min, tupleElement(g, 8) AS mtime_max, " +
		"tupleElement(g, 9) AS atime_buckets, tupleElement(g, 10) AS mtime_buckets " +
		"FROM (" +
		"SELECT c.full_path AS dir, arrayJoin(" + dgutaPrefixedArrayZipExpr + ") AS g " +
		"FROM wrstat_dir_facts d " +
		"INNER JOIN wrstat_dirs c " +
		"ON c.mount_path = d.mount_path AND c.snapshot_id = d.snapshot_id AND c.dir_id = d.dir_id " +
		"WHERE d.mount_path = ? AND d.snapshot_id = ? " +
		")" +
		") " +
		"WHERE %s " +
		"GROUP BY dir"

	mountDirSummariesReadyQuery = "SELECT mount_path, toString(snapshot_id) " +
		"FROM wrstat_dir_projection_sets WHERE %s"
)

var errIntOverflow = errors.New("value overflows int")

var errReaderClosed = errors.New("clickhouse: reader is closed")

var errCatalogParentCycle = errors.New("clickhouse: catalog parent_id cycle")

var errCatalogParentMissing = errors.New("clickhouse: missing catalog parent")

var whereDirAgeAllRouteEvidenceFor = func( //nolint:gochecknoglobals // Tests inject measured p95 evidence.
	_ context.Context,
	_ *clickHouseDatabase,
	_ activeMount,
	_ string,
	_ *db.Filter,
) (whereDirAgeAllRouteEvidence, bool, error) {
	return whereDirAgeAllRouteEvidence{}, false, nil
}

type activeMountDirGroup struct {
	mount                            activeMount
	originalDirs                     map[string]string
	queryDirs                        []string
	schema3ChildFilterAllUnavailable bool
}

func activeMountGroup(
	groups map[string]*activeMountDirGroup,
	mount activeMount,
) *activeMountDirGroup {
	key := mount.mountPath + "\x00" + mount.snapshotID

	group := groups[key]
	if group != nil {
		return group
	}

	group = &activeMountDirGroup{
		mount:        mount,
		originalDirs: make(map[string]string),
	}
	groups[key] = group

	return group
}

func remainingDirInfoGroup(
	group *activeMountDirGroup,
	handled map[string]bool,
) *activeMountDirGroup {
	if len(handled) == 0 {
		return group
	}

	remaining := &activeMountDirGroup{
		mount:        group.mount,
		originalDirs: make(map[string]string),
	}

	for _, queryDir := range group.queryDirs {
		if handled[queryDir] {
			continue
		}

		remaining.queryDirs = append(remaining.queryDirs, queryDir)
		remaining.originalDirs[queryDir] = group.originalDirs[queryDir]
	}

	return remaining
}

func activeMountRootDirGroup(mount activeMount) *activeMountDirGroup {
	return &activeMountDirGroup{
		mount:        mount,
		queryDirs:    []string{mount.mountPath},
		originalDirs: map[string]string{mount.mountPath: mount.mountPath},
	}
}

type activeMountRootDirs struct {
	mounts       []activeMount
	originalDirs map[string][]string
}

type whereTraversalItem struct {
	dir  string
	step int
}

type treeCatalogDirRef struct {
	dirID      uint32
	parentID   uint32
	subtreeEnd uint32
	fullPath   string
}

func scanTreeCatalogDirRef(rows rowsScanner, what string) (treeCatalogDirRef, bool, error) {
	if !rows.Next() {
		if err := rowsErr(rows); err != nil {
			return treeCatalogDirRef{}, false, fmt.Errorf("clickhouse: %s iteration error: %w", what, err)
		}

		return treeCatalogDirRef{}, false, nil
	}

	var ref treeCatalogDirRef
	if err := rows.Scan(&ref.dirID, &ref.parentID, &ref.subtreeEnd, &ref.fullPath); err != nil {
		return treeCatalogDirRef{}, false, fmt.Errorf("clickhouse: failed to scan catalog dir: %w", err)
	}

	return ref, true, nil
}

type whereRangeCatalogNode struct {
	dirID    uint32
	parentID uint32
	fullPath string
}

type dirFactRowKey struct {
	mountPath  string
	snapshotID string
	dirID      uint32
}

func scanResolvedDirFactSummaryRow(
	rows rowsScanner,
) (dirFactRowKey, time.Time, db.GUTAs, error) {
	var (
		key       dirFactRowKey
		updatedAt time.Time
		vector    dgutaVectorColumns
	)

	dest := make([]any, 0, resolvedDirFactScalarColumns+dirFactVectorColumnCount)
	dest = append(dest,
		&key.mountPath,
		&key.snapshotID,
		&key.dirID,
		&updatedAt,
	)
	dest = append(dest, dirFactVectorScanDest(&vector)...)

	if err := rows.Scan(dest...); err != nil {
		return dirFactRowKey{}, time.Time{}, nil, fmt.Errorf("clickhouse: failed to scan active dir facts row: %w", err)
	}

	gutas, err := vector.gutas("dir_id", strconv.FormatUint(uint64(key.dirID), 10))
	if err != nil {
		return dirFactRowKey{}, time.Time{}, nil, err
	}

	return key, updatedAt.UTC(), gutas, nil
}

func resolvedDirFactKeys(resolved []resolvedDirFact) map[dirFactRowKey]bool {
	keys := make(map[dirFactRowKey]bool, len(resolved))
	for _, fact := range resolved {
		keys[fact.key] = true
	}

	return keys
}

func addResolvedDirFactSummaryRow(
	rows rowsScanner,
	wanted map[dirFactRowKey]bool,
	acc *dirFactSummaryAccumulator,
) (bool, error) {
	key, updatedAt, gutas, err := scanResolvedDirFactSummaryRow(rows)
	if err != nil || !wanted[key] {
		return false, err
	}

	acc.gutas = append(acc.gutas, gutas...)
	if updatedAt.After(acc.updatedAt) {
		acc.updatedAt = updatedAt
	}

	return true, nil
}

type resolvedDirFact struct {
	key dirFactRowKey
	dir string
}

type dirFactSummaryAccumulator struct {
	gutas     db.GUTAs
	updatedAt time.Time
}

func scanResolvedDirFactSummaryRows(
	rows rowsScanner,
	resolved []resolvedDirFact,
) (*dirFactSummaryAccumulator, bool, error) {
	wanted := resolvedDirFactKeys(resolved)
	acc := &dirFactSummaryAccumulator{}
	found := false

	for rows.Next() {
		rowFound, err := addResolvedDirFactSummaryRow(rows, wanted, acc)
		if err != nil {
			return nil, false, err
		}

		found = found || rowFound
	}

	if err := rowsErr(rows); err != nil {
		return nil, false, fmt.Errorf("clickhouse: active dir facts row iteration error: %w", err)
	}

	return acc, found, nil
}

func addDirFactSummary(
	accs map[string]*dirFactSummaryAccumulator,
	dir string,
	updatedAt time.Time,
	gutas db.GUTAs,
) {
	acc := accs[dir]
	if acc == nil {
		acc = &dirFactSummaryAccumulator{}
		accs[dir] = acc
	}

	acc.gutas = append(acc.gutas, gutas...)

	if updatedAt.After(acc.updatedAt) {
		acc.updatedAt = updatedAt
	}
}

type whereRangeScalarColumns struct {
	count        string
	size         string
	atimeMin     string
	mtimeMax     string
	atimeBuckets string
	mtimeBuckets string
	uids         string
	gids         string
	ft           string
}

func whereRangeScalarSummaryColumns(mode mountDirSummaryMode) whereRangeScalarColumns {
	if mode == mountDirSummaryFiles {
		return whereRangeScalarColumns{
			count:        "f.file_count",
			size:         "f.file_size",
			atimeMin:     "f.file_atime_min",
			mtimeMax:     "f.file_mtime_max",
			atimeBuckets: "f.file_atime_buckets",
			mtimeBuckets: "f.file_mtime_buckets",
			uids:         "f.file_uids",
			gids:         "f.file_gids",
			ft:           "f.file_ft",
		}
	}

	return whereRangeScalarColumns{
		count:        "f.all_count",
		size:         "f.all_size",
		atimeMin:     "f.all_atime_min",
		mtimeMax:     "f.all_mtime_max",
		atimeBuckets: "f.all_atime_buckets",
		mtimeBuckets: "f.all_mtime_buckets",
		uids:         "f.all_uids",
		gids:         "f.all_gids",
		ft:           "f.all_ft",
	}
}

type whereRangeData struct {
	summaries map[string]*db.DirSummary
	children  map[string][]string
}

func newWhereRangeData(
	nodes []whereRangeCatalogNode,
	summaries map[string]*db.DirSummary,
) *whereRangeData {
	children := make(map[string][]string)

	dirsByID := make(map[uint32]string, len(nodes))
	for _, node := range nodes {
		dirsByID[node.dirID] = node.fullPath
	}

	for _, node := range nodes {
		parent, ok := dirsByID[node.parentID]
		if !ok {
			continue
		}

		children[parent] = append(children[parent], node.fullPath)
	}

	for parent, childDirs := range children {
		children[parent] = canonicalSortedChildren(childDirs)
	}

	return &whereRangeData{
		summaries: summaries,
		children:  children,
	}
}

func (r *whereRangeData) where(dir string, recurseCount func(string) int) db.DCSs {
	var dcss db.DCSs

	frontier := []whereTraversalItem{{dir: dir}}
	for len(frontier) > 0 {
		next := make([]whereTraversalItem, 0)

		for _, item := range frontier {
			info := r.where0(item.dir)
			if info == nil {
				continue
			}

			dcss = append(dcss, info.Current)
			if recurseCount(item.dir) > item.step {
				next = append(next, childWhereTraversalItems(info.Children, item.step+1)...)
			}
		}

		frontier = next
	}

	return dcss
}

func childWhereTraversalItems(children []*db.DirSummary, step int) []whereTraversalItem {
	items := make([]whereTraversalItem, len(children))
	for i, child := range children {
		items[i] = whereTraversalItem{dir: child.Dir, step: step}
	}

	return items
}

func (r *whereRangeData) where0(dir string) *db.DirInfo {
	key := ensureTrailingSlash(dir)
	displayDir := dir

	for {
		info := r.dirInfo(key, displayDir)
		if info == nil {
			return nil
		}

		if !info.IsSameAsChild() {
			return info
		}

		displayDir = info.Children[0].Dir
		key = ensureTrailingSlash(displayDir)
	}
}

func (r *whereRangeData) dirInfo(key, displayDir string) *db.DirInfo {
	current := r.summaryForDir(key, displayDir)
	if current == nil {
		return nil
	}

	return &db.DirInfo{
		Current:  current,
		Children: r.childSummaries(key),
	}
}

func (r *whereRangeData) childSummaries(parent string) []*db.DirSummary {
	children := make([]*db.DirSummary, 0, len(r.children[parent]))
	for _, child := range r.children[parent] {
		if summary := r.summaryForDir(child, child); summary != nil && summary.Count > 0 {
			children = append(children, summary)
		}
	}

	return children
}

func (r *whereRangeData) summaryForDir(key, displayDir string) *db.DirSummary {
	summary := r.summaries[key]
	if summary == nil {
		return nil
	}

	cp := *summary
	cp.Dir = displayDir

	return &cp
}

type whereDirAgeAllRouteEvidence struct {
	ageAllExact bool
	allExact    bool
	ageAllP95   time.Duration
	allP95      time.Duration
}

type dirInfoMountRoute func(
	map[string]*db.DirSummary,
	*activeMountDirGroup,
	*db.Filter,
) (*activeMountDirGroup, error)

type childFilterAllSummary struct {
	Dir         string
	Summary     *db.DirSummary
	Age         db.DirGUTAge
	HasChildren bool
	ChildCount  uint64
}

func (t *whereTraversal) summaryDirsForInfos(parentDirs []string) []string {
	dirs := make([]string, 0, len(parentDirs))
	dirs = append(dirs, parentDirs...)

	return append(dirs, t.childSummaryDirs(parentDirs)...)
}

func (t *whereTraversal) loadGroupedMountSummaries(dirs []string) (bool, error) {
	summaries, _, ok, err := t.database.dirSummariesForDirsMount(
		t.mount.mountPath,
		t.mount.snapshotID,
		t.mount.updatedAt,
		dirs,
		t.filter,
	)
	if err != nil || !ok {
		return ok, err
	}

	for _, dir := range dirs {
		t.summaryLoaded[dir] = true
		if sum := summaries[dir]; sum != nil {
			t.summaries[dir] = sum
		}
	}

	return true, nil
}

func (t *whereTraversal) preloadFilteredMountWhere(queryDir string) error {
	if !t.shouldPreloadFilteredMountWhere(queryDir) {
		return nil
	}

	ctx, cancel := configQueryContext(t.database.cfg)
	defer cancel()

	summaries, ok, err := t.database.filteredMountWhereTraversalSummaries(ctx, *t.mount, queryDir, t.filter)
	if err != nil || !ok {
		return err
	}

	childrenByParent, err := t.database.childrenForParentsMount(
		t.mount.mountPath,
		t.mount.snapshotID,
		preloadedSummaryDirs(summaries),
	)
	if err != nil {
		return err
	}

	t.storePreloadedFilteredMountWhere(queryDir, summaries, childrenByParent)

	return nil
}

func preloadedSummaryDirs(summaries map[string]*db.DirSummary) []string {
	dirs := make([]string, 0, len(summaries))
	for dir := range summaries {
		dirs = append(dirs, ensureTrailingSlash(dir))
	}

	sort.Strings(dirs)

	return dirs
}

func (t *whereTraversal) shouldPreloadFilteredMountWhere(queryDir string) bool {
	if t.mount == nil || !dirFilterAllCanHandleFilter(t.filter) {
		return false
	}

	return strings.HasPrefix(ensureTrailingSlash(queryDir), ensureTrailingSlash(t.mount.mountPath))
}

func (t *whereTraversal) storePreloadedFilteredMountWhere(
	queryDir string,
	summaries map[string]*db.DirSummary,
	childrenByParent map[string][]string,
) {
	knownDirs := make(map[string]bool, len(summaries)+len(childrenByParent)+1)

	t.storePreloadedMountSummaries(summaries, knownDirs)
	t.storePreloadedMountChildren(childrenByParent, knownDirs)

	knownDirs[ensureTrailingSlash(queryDir)] = true
	knownDirs[ensureTrailingSlash(t.mount.mountPath)] = true
	t.markPreloadedMountDirs(knownDirs)
}

func (t *whereTraversal) storePreloadedMountSummaries(
	summaries map[string]*db.DirSummary,
	knownDirs map[string]bool,
) {
	for dir, summary := range summaries {
		key := ensureTrailingSlash(dir)
		t.summaries[key] = summary
		knownDirs[key] = true
	}
}

func (t *whereTraversal) storePreloadedMountChildren(
	childrenByParent map[string][]string,
	knownDirs map[string]bool,
) {
	for parent, children := range childrenByParent {
		key := ensureTrailingSlash(parent)
		canonicalChildren := canonicalSortedChildren(children)

		t.children[key] = canonicalChildren
		t.childrenLoaded[key] = true
		knownDirs[key] = true

		for _, child := range canonicalChildren {
			knownDirs[child] = true
		}
	}
}

func (t *whereTraversal) markPreloadedMountDirs(knownDirs map[string]bool) {
	for dir := range knownDirs {
		t.summaryLoaded[dir] = true
		if t.childrenLoaded[dir] {
			continue
		}

		t.children[dir] = nil
		t.childrenLoaded[dir] = true
	}
}

func (t *whereTraversal) loadRawMountSummaries(dirs []string) error {
	gutasByDir, err := t.database.gutasForDirs(
		t.mount.mountPath,
		t.mount.snapshotID,
		dirs,
	)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		t.summaryLoaded[dir] = true

		sum := dirSummaryWithModtime(gutasByDir[dir], t.filter, t.mount.updatedAt)
		if sum != nil {
			t.summaries[dir] = sum
		}
	}

	return nil
}

func (t *whereTraversal) loadFallbackChildGroupRootMounts(
	groups map[string]*activeMountDirGroup,
) []activeMount {
	mounts := make([]activeMount, 0)
	seen := make(map[string]bool, len(groups))

	for _, group := range groups {
		for _, dir := range group.queryDirs {
			if ensureTrailingSlash(dir) != ensureTrailingSlash(group.mount.mountPath) {
				continue
			}

			key := group.mount.mountPath + "\x00" + group.mount.snapshotID
			if seen[key] {
				continue
			}

			seen[key] = true

			mounts = append(mounts, group.mount)
		}
	}

	return mounts
}

func (t *whereTraversal) loadActiveMountRootChildren(mounts []activeMount) error {
	if len(mounts) == 0 {
		return nil
	}

	childrenByParent, err := t.database.childrenForActiveMountRoots(mounts)
	if err != nil {
		return err
	}

	for _, mount := range mounts {
		t.storeChildren(mount.mountPath, childrenByParent[ensureTrailingSlash(mount.mountPath)])
	}

	return nil
}

func nonActiveMountRootQueryDirs(group *activeMountDirGroup) []string {
	dirs := make([]string, 0, len(group.queryDirs))
	mountPath := ensureTrailingSlash(group.mount.mountPath)

	for _, dir := range group.queryDirs {
		if ensureTrailingSlash(dir) == mountPath {
			continue
		}

		dirs = append(dirs, dir)
	}

	return dirs
}

type clickHouseDatabase struct {
	cfg  Config
	conn ch.Conn

	mountPoints    basedirs.MountPoints
	mountPointsErr error

	snapshot *activeMountsSnapshot
	closed   atomic.Bool

	treeCache *treeQueryCache
	navIndex  *navIndexManager
}

func newClickHouseDatabaseWithSnapshot(
	cfg Config,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) *clickHouseDatabase {
	return newClickHouseDatabaseWithSnapshotContext(context.Background(), cfg, conn, snapshot)
}

func newClickHouseDatabaseWithSnapshotContext(
	ctx context.Context,
	cfg Config,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) *clickHouseDatabase {
	mountPoints, err := mountPointsFromConfig(cfg)

	database := &clickHouseDatabase{
		cfg:            cfg,
		conn:           conn,
		mountPoints:    mountPoints,
		mountPointsErr: err,
		snapshot:       snapshot,
		treeCache:      treeQueryCacheForConfig(cfg),
	}

	database.navIndex = newNavIndexManager(cfg, conn, snapshot)
	database.navIndex.start(ctx)

	return database
}

func (d *clickHouseDatabase) dirInfoSingleMountGuard(
	mountPath, snapshotID string,
	updatedAt time.Time,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	ready, err := d.activeMountFactsReady(mountPath, snapshotID)
	if err != nil {
		return nil, true, err
	}

	if !ready {
		return &db.DirSummary{Modtime: updatedAt}, true, db.ErrDirNotFound
	}

	if fullFilterAlwaysEmpty(filter) {
		return nil, true, nil
	}

	return nil, false, nil
}

func (d *clickHouseDatabase) dirInfoSingleMountFacts(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	if d.conn == nil {
		return nil, false, nil
	}

	sum, found, err := d.dirFactSummaryForDirMount(mountPath, snapshotID, dir, filter)
	if err != nil || found {
		return sum, true, err
	}

	return &db.DirSummary{Modtime: updatedAt}, true, db.ErrDirNotFound
}

func (d *clickHouseDatabase) childFilterAllParentsWithMatchingChildren(
	ctx context.Context,
	group *activeMountDirGroup,
	filter *db.Filter,
) (map[string]bool, map[string]bool, error) {
	dirsByID, handled, err := d.resolveChildFilterAllParentDirs(ctx, group)
	if err != nil {
		return nil, nil, err
	}

	if len(dirsByID) == 0 {
		return map[string]bool{}, handled, nil
	}

	parentIDs, err := d.queryChildFilterAllMatchingParentIDs(
		ctx,
		group.mount.mountPath,
		group.mount.snapshotID,
		dirFactIDs(dirsByID),
		filter,
	)
	if err != nil {
		return nil, nil, err
	}

	return matchingParentDirsByID(parentIDs, dirsByID), handled, nil
}

func dirFactIDs(dirsByID map[uint32]string) []uint32 {
	ids := make([]uint32, 0, len(dirsByID))
	for dirID := range dirsByID {
		ids = append(ids, dirID)
	}

	return ids
}

func matchingParentDirsByID(
	parentIDs map[uint32]bool,
	dirsByID map[uint32]string,
) map[string]bool {
	parents := make(map[string]bool, len(parentIDs))
	for parentID := range parentIDs {
		if dir, ok := dirsByID[parentID]; ok {
			parents[dir] = true
		}
	}

	return parents
}

func (d *clickHouseDatabase) resolveChildFilterAllParentDirs(
	ctx context.Context,
	group *activeMountDirGroup,
) (map[uint32]string, map[string]bool, error) {
	dirsByID, err := d.resolveDirIDsForDirsMount(
		ctx,
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return nil, nil, err
	}

	return dirsByID, handledDirFilterAllDirs(group.queryDirs), nil
}

func (d *clickHouseDatabase) queryChildFilterAllMatchingParentIDs(
	ctx context.Context,
	mountPath, snapshotID string,
	parentIDs []uint32,
	filter *db.Filter,
) (map[uint32]bool, error) {
	parents := make(map[uint32]bool)

	for _, batchIDs := range uint32ValueBatches(parentIDs) {
		batchParents, err := d.queryChildFilterAllMatchingParentIDBatch(
			ctx,
			mountPath,
			snapshotID,
			batchIDs,
			filter,
		)
		if err != nil {
			return nil, err
		}

		for parentID := range batchParents {
			parents[parentID] = true
		}
	}

	return parents, nil
}

func uint32ValueBatches(values []uint32) [][]uint32 {
	if len(values) == 0 {
		return nil
	}

	if len(values) <= queryStringINMaxValues {
		return [][]uint32{values}
	}

	batches := make([][]uint32, 0, (len(values)+queryStringINMaxValues-1)/queryStringINMaxValues)
	for start := 0; start < len(values); start += queryStringINMaxValues {
		end := min(start+queryStringINMaxValues, len(values))
		batches = append(batches, values[start:end])
	}

	return batches
}

func (d *clickHouseDatabase) queryChildFilterAllMatchingParentIDBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	parentIDs []uint32,
	filter *db.Filter,
) (map[uint32]bool, error) {
	query, args := childFilterAllMatchingParentIDQuery(mountPath, snapshotID, parentIDs, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query filtered child parent_ids: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentIDSet(rows)
}

func childFilterAllMatchingParentIDQuery(
	mountPath, snapshotID string,
	parentIDs []uint32,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	query := fmt.Sprintf(dirsHaveFilteredChildrenQuery, placeholders(len(parentIDs)), clauses)

	args := make([]any, 0, queryScopeArgs+len(parentIDs)+1+len(filterArgs))
	args = append(args, mountPath, snapshotID)
	args = appendUint32Args(args, parentIDs)
	args = append(args, uint8(filter.Age))
	args = append(args, filterArgs...)

	return query, args
}

func scanParentIDSet(rows rowsScanner) (map[uint32]bool, error) {
	parents := make(map[uint32]bool)

	for rows.Next() {
		var parentID uint32
		if err := rows.Scan(&parentID); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan matching child parent_id: %w", err)
		}

		parents[parentID] = true
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: matching child parent_id iteration error: %w", err)
	}

	return parents, nil
}

func (d *clickHouseDatabase) addCatalogDirsHaveChildrenForMount(
	result map[string]bool,
	group *activeMountDirGroup,
) (*activeMountDirGroup, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	parents, err := d.catalogParentsWithChildrenForMount(
		ctx,
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return nil, err
	}

	for _, queryDir := range group.queryDirs {
		if parents[queryDir] {
			result[group.originalDirs[queryDir]] = true
		}
	}

	return remainingDirInfoGroup(group, handledDirFilterAllDirs(group.queryDirs)), nil
}

func (d *clickHouseDatabase) catalogParentsWithChildrenForMount(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]bool, error) {
	if parents, handled, err := d.navIndexParentsWithChildren(mountPath, snapshotID, dirs); handled {
		return parents, err
	}

	parents := make(map[string]bool)

	for _, batchDirs := range stringValueBatches(uniqueQueryDirs(dirs)) {
		batchParents, err := d.queryCatalogParentsWithChildrenBatch(ctx, mountPath, snapshotID, batchDirs)
		if err != nil {
			return nil, err
		}

		for parent := range batchParents {
			parents[parent] = true
		}
	}

	return parents, nil
}

func uniqueQueryDirs(dirs []string) []string {
	unique := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		queryDir := ensureTrailingSlash(dir)
		if seen[queryDir] {
			continue
		}

		seen[queryDir] = true
		unique = append(unique, queryDir)
	}

	return unique
}

func (d *clickHouseDatabase) queryCatalogParentsWithChildrenBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]bool, error) {
	query, args := scopedBatchQuery(dirsHaveCatalogChildrenQuery, dirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query catalog child counts: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentDirSet(rows)
}

func (d *clickHouseDatabase) dirFactGUTAsForAncestorMounts(
	dir string,
	mounts []activeMount,
) (db.GUTAs, time.Time, bool, error) {
	if len(mounts) == 0 {
		return nil, time.Time{}, false, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	resolved, err := d.resolveDirIDsForActiveMountsDir(ctx, dir, mounts)
	if err != nil || len(resolved) == 0 {
		return nil, time.Time{}, false, err
	}

	acc, found, err := d.queryResolvedDirFactSummaryAccumulator(ctx, resolved)
	if err != nil || !found {
		return nil, time.Time{}, false, err
	}

	return acc.gutas, acc.updatedAt, true, nil
}

func (d *clickHouseDatabase) resolveDirIDsForActiveMountsDir(
	ctx context.Context,
	dir string,
	mounts []activeMount,
) ([]resolvedDirFact, error) {
	query, args := activeMountsQuery(
		dirIDsForActiveMountsDirQuery,
		"mount_path",
		"snapshot_id",
		mounts,
		ensureTrailingSlash(dir),
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to resolve active dir_ids: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanResolvedDirFacts(rows, ensureTrailingSlash(dir))
}

func scanResolvedDirFacts(rows rowsScanner, dir string) ([]resolvedDirFact, error) {
	resolved := make([]resolvedDirFact, 0)

	for rows.Next() {
		var fact resolvedDirFact

		fact.dir = dir

		if err := rows.Scan(&fact.key.mountPath, &fact.key.snapshotID, &fact.key.dirID); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan active dir_id: %w", err)
		}

		resolved = append(resolved, fact)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: active dir_id iteration error: %w", err)
	}

	return resolved, nil
}

func (d *clickHouseDatabase) queryResolvedDirFactSummaryAccumulator(
	ctx context.Context,
	resolved []resolvedDirFact,
) (*dirFactSummaryAccumulator, bool, error) {
	query, args := resolvedDirFactRowsQuery(resolved)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("clickhouse: failed to query active dir facts rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanResolvedDirFactSummaryRows(rows, resolved)
}

func resolvedDirFactRowsQuery(resolved []resolvedDirFact) (string, []any) {
	var b strings.Builder

	args := make([]any, 0, len(resolved)*activeMountDirTupleArgs)

	for i, fact := range resolved {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, toUUID(?), ?)")

		args = append(args, fact.key.mountPath, fact.key.snapshotID, fact.key.dirID)
	}

	return fmt.Sprintf(dirFactRowsForResolvedDirsQuery, b.String()), args
}

func (d *clickHouseDatabase) resolveDirIDForMount(
	ctx context.Context,
	mountPath, snapshotID string,
	dir string,
) (uint32, bool, error) {
	ref, ok, err := d.resolveCatalogDirForMount(ctx, mountPath, snapshotID, dir)
	if err != nil || !ok {
		return 0, ok, err
	}

	return ref.dirID, true, nil
}

func (d *clickHouseDatabase) resolveCatalogDirForMount(
	ctx context.Context,
	mountPath, snapshotID string,
	dir string,
) (treeCatalogDirRef, bool, error) {
	if ref, found, handled, err := d.navIndexCatalogDirForMount(mountPath, snapshotID, dir); handled {
		return ref, found, err
	}

	queryDir := ensureTrailingSlash(dir)

	rows, err := d.conn.Query(
		ctx,
		dirForFullPathQuery,
		mountPath,
		snapshotID,
		catalogPathHash(queryDir),
		queryDir,
	)
	if err != nil {
		return treeCatalogDirRef{}, false, fmt.Errorf("clickhouse: failed to resolve dir_id: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanTreeCatalogDirRef(rows, "dir resolution")
}

func (d *clickHouseDatabase) catalogDirForID(
	ctx context.Context,
	mountPath, snapshotID string,
	dirID uint32,
) (treeCatalogDirRef, bool, error) {
	if ref, found, handled, err := d.navIndexCatalogDirForID(mountPath, snapshotID, dirID); handled {
		return ref, found, err
	}

	rows, err := d.conn.Query(ctx, dirForIDQuery, mountPath, snapshotID, dirID)
	if err != nil {
		return treeCatalogDirRef{}, false, fmt.Errorf("clickhouse: failed to resolve catalog parent: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanTreeCatalogDirRef(rows, "catalog parent resolution")
}

func (d *clickHouseDatabase) catalogAncestorPathsForMount(
	ctx context.Context,
	mountPath, snapshotID string,
	dir string,
) ([]string, bool, error) {
	ref, ok, err := d.resolveCatalogDirForMount(ctx, mountPath, snapshotID, dir)
	if err != nil || !ok {
		return nil, ok, err
	}

	return d.catalogAncestorPathsFromRef(ctx, mountPath, snapshotID, ref)
}

func (d *clickHouseDatabase) catalogAncestorPathsFromRef(
	ctx context.Context,
	mountPath, snapshotID string,
	ref treeCatalogDirRef,
) ([]string, bool, error) {
	refs, err := d.catalogAncestorRefs(ctx, mountPath, snapshotID, ref)
	if err != nil {
		return nil, false, err
	}

	paths := make([]string, len(refs))
	for i, ancestor := range refs {
		paths[len(refs)-1-i] = ancestor.fullPath
	}

	return paths, true, nil
}

func (d *clickHouseDatabase) catalogAncestorRefs(
	ctx context.Context,
	mountPath, snapshotID string,
	ref treeCatalogDirRef,
) ([]treeCatalogDirRef, error) {
	refs := []treeCatalogDirRef{ref}
	seen := map[uint32]bool{ref.dirID: true}

	for ref.parentID != 0 {
		if seen[ref.parentID] {
			return nil, fmt.Errorf("%w: dir_id %d", errCatalogParentCycle, ref.parentID)
		}

		parent, ok, err := d.catalogDirForID(ctx, mountPath, snapshotID, ref.parentID)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, fmt.Errorf("%w: dir_id %d", errCatalogParentMissing, ref.parentID)
		}

		refs = append(refs, parent)
		seen[parent.dirID] = true
		ref = parent
	}

	return refs, nil
}

func (d *clickHouseDatabase) dirFactSummaryForDirMount(
	mountPath, snapshotID string,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	queryDir := ensureTrailingSlash(dir)

	summaries, handled, err := d.dirFactSummariesForDirsMount(
		mountPath,
		snapshotID,
		[]string{queryDir},
		filter,
	)
	if err != nil || !handled[queryDir] {
		return nil, false, err
	}

	return summaries[queryDir], true, nil
}

func (d *clickHouseDatabase) addDirFactInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (bool, error) {
	summaries, _, err := d.dirFactSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
		filter,
	)
	if err != nil {
		return false, err
	}

	d.addGroupedDirSummaries(result, group, summaries)

	return true, nil
}

func (d *clickHouseDatabase) dirFactSummariesForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, error) {
	if len(dirs) == 0 {
		return map[string]*db.DirSummary{}, map[string]bool{}, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	dirsByID, err := d.resolveDirIDsForDirsMount(ctx, mountPath, snapshotID, dirs)
	if err != nil || len(dirsByID) == 0 {
		return map[string]*db.DirSummary{}, map[string]bool{}, err
	}

	accs, err := d.queryDirFactSummaryAccumulatorsForDirIDs(ctx, mountPath, snapshotID, dirsByID)
	if err != nil {
		return nil, nil, err
	}

	return dirFactSummariesFromAccumulators(accs, filter), handledDirFactAccumulators(accs), nil
}

func dirFactSummariesFromAccumulators(
	accs map[string]*dirFactSummaryAccumulator,
	filter *db.Filter,
) map[string]*db.DirSummary {
	summaries := make(map[string]*db.DirSummary, len(accs))
	for dir, acc := range accs {
		if sum := dirSummaryWithModtime(acc.gutas, filter, acc.updatedAt); sum != nil {
			summaries[dir] = sum
		}
	}

	return summaries
}

func handledDirFactAccumulators(accs map[string]*dirFactSummaryAccumulator) map[string]bool {
	handled := make(map[string]bool, len(accs))
	for dir := range accs {
		handled[dir] = true
	}

	return handled
}

func (d *clickHouseDatabase) resolveDirIDsForDirsMount(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[uint32]string, error) {
	result := make(map[uint32]string, len(dirs))

	for _, batchDirs := range stringValueBatches(uniqueQueryDirs(dirs)) {
		if err := d.addResolvedDirIDsForDirsBatch(ctx, mountPath, snapshotID, batchDirs, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (d *clickHouseDatabase) addResolvedDirIDsForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
	result map[uint32]string,
) error {
	query, args := scopedBatchQuery(dirIDsForDirsQuery, dirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to resolve dir_id batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanResolvedDirIDRows(rows, result)
}

func scanResolvedDirIDRows(rows rowsScanner, result map[uint32]string) error {
	for rows.Next() {
		var (
			dir   string
			dirID uint32
		)

		if err := rows.Scan(&dir, &dirID); err != nil {
			return fmt.Errorf("clickhouse: failed to scan dir_id batch row: %w", err)
		}

		result[dirID] = dir
	}

	if err := rowsErr(rows); err != nil {
		return fmt.Errorf("clickhouse: dir_id batch iteration error: %w", err)
	}

	return nil
}

func (d *clickHouseDatabase) queryDirFactSummaryAccumulatorsForDirIDs(
	ctx context.Context,
	mountPath, snapshotID string,
	dirsByID map[uint32]string,
) (map[string]*dirFactSummaryAccumulator, error) {
	accs := make(map[string]*dirFactSummaryAccumulator, len(dirsByID))

	for _, batchIDs := range uint32ValueBatches(dirFactIDs(dirsByID)) {
		if err := d.addDirFactSummaryAccumulatorBatch(ctx, mountPath, snapshotID, batchIDs, dirsByID, accs); err != nil {
			return nil, err
		}
	}

	return accs, nil
}

func (d *clickHouseDatabase) addDirFactSummaryAccumulatorBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirIDs []uint32,
	dirsByID map[uint32]string,
	accs map[string]*dirFactSummaryAccumulator,
) error {
	query, args := scopedUint32BatchQuery(dirFactRowsForDirIDsQuery, dirIDs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query dir facts rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanDirFactSummaryRowsForMount(rows, dirsByID, accs)
}

func scopedUint32BatchQuery(queryFmt string, values []uint32, scopeArgs ...any) (string, []any) {
	query := fmt.Sprintf(queryFmt, placeholders(len(values)))
	args := make([]any, 0, len(values)+len(scopeArgs))
	args = append(args, scopeArgs...)

	for _, value := range values {
		args = append(args, value)
	}

	return query, args
}

func scanDirFactSummaryRowsForMount(
	rows rowsScanner,
	dirsByID map[uint32]string,
	accs map[string]*dirFactSummaryAccumulator,
) error {
	for rows.Next() {
		dirID, updatedAt, gutas, err := scanDirFactSummaryRow(rows)
		if err != nil {
			return err
		}

		dir, ok := dirsByID[dirID]
		if !ok {
			continue
		}

		addDirFactSummary(accs, dir, updatedAt, gutas)
	}

	if err := rowsErr(rows); err != nil {
		return fmt.Errorf("clickhouse: dir facts row iteration error: %w", err)
	}

	return nil
}

func (d *clickHouseDatabase) addDirInfosForMountFallback(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) error {
	if dirFilterAllCanHandleFilter(filter) {
		return d.addFullFilterDirInfosForMount(result, group, filter)
	}

	group, err := d.addChildFilterAllDirInfosForMount(result, group, filter)
	if dirInfoRouteDone(group, err) {
		return err
	}

	ok, err := d.addGroupedDirInfosForMount(result, group, filter)
	if err != nil || ok {
		return err
	}

	return d.addRawDirInfosForMount(result, group, filter)
}

func (d *clickHouseDatabase) addIndexedActiveMountRootDirsHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mounts []activeMount,
	filter *db.Filter,
) (bool, error) {
	if fullFilterAlwaysEmpty(filter) {
		return true, nil
	}

	if broadFilterCanUseChildRows(filter) {
		return true, d.addCatalogActiveMountRootDirsHaveChildren(result, roots, mounts)
	}

	return d.addChildFilterAllActiveMountRootDirsHaveChildren(result, roots, mounts, filter)
}

func (d *clickHouseDatabase) addCatalogActiveMountRootDirsHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mounts []activeMount,
) error {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	for _, mount := range mounts {
		parents, err := d.catalogParentsWithChildrenForMount(
			ctx,
			mount.mountPath,
			mount.snapshotID,
			[]string{mount.mountPath},
		)
		if err != nil {
			return err
		}

		if parents[mount.mountPath] {
			markActiveMountRootHasChildren(result, roots, mount.mountPath)
		}
	}

	return nil
}

func markActiveMountRootHasChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mountPath string,
) {
	for _, original := range roots.originalDirs[mountPath] {
		result[original] = true
	}
}

func (d *clickHouseDatabase) addChildFilterAllActiveMountRootDirsHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mounts []activeMount,
	filter *db.Filter,
) (bool, error) {
	if !childFilterAllCanHandleDirsHaveChildrenFilter(filter) {
		return false, nil
	}

	handledAll := true

	for _, mount := range mounts {
		handled, err := d.addChildFilterAllActiveMountRootDirHaveChildren(result, roots, mount, filter)
		if err != nil {
			return false, err
		}

		if !handled {
			handledAll = false
		}
	}

	return handledAll, nil
}

func (d *clickHouseDatabase) addChildFilterAllActiveMountRootDirHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mount activeMount,
	filter *db.Filter,
) (bool, error) {
	canonicalResult := map[string]bool{mount.mountPath: false}
	group := activeMountRootDirGroup(mount)

	remaining, err := d.addChildFilterAllDirsHaveChildrenForMount(canonicalResult, group, filter)
	if err != nil {
		return false, err
	}

	if len(remaining.queryDirs) > 0 {
		return false, nil
	}

	if canonicalResult[mount.mountPath] {
		markActiveMountRootHasChildren(result, roots, mount.mountPath)
	}

	return true, nil
}

func (d *clickHouseDatabase) whereByDirIDRange(
	queryDir string,
	displayDir string,
	filter *db.Filter,
	recurseCount func(string) int,
) (db.DCSs, bool, error) {
	mount, ok, err := d.whereRangeActiveMount(queryDir)
	if err != nil || !ok {
		return nil, ok, err
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ref, err := d.whereRangeDirRef(ctx, mount, queryDir)
	if err != nil {
		return nil, true, err
	}

	nodes, err := d.whereRangeCatalog(ctx, mount, ref)
	if err != nil {
		return nil, true, err
	}

	summaries, err := d.whereRangeSummaries(ctx, mount, ref, queryDir, filter)
	if err != nil {
		return nil, true, err
	}

	return sortedWhereRangeDCSs(nodes, summaries, displayDir, recurseCount), true, nil
}

func sortedWhereRangeDCSs(
	nodes []whereRangeCatalogNode,
	summaries map[string]*db.DirSummary,
	displayDir string,
	recurseCount func(string) int,
) db.DCSs {
	dcss := newWhereRangeData(nodes, summaries).where(displayDir, recurseCount)
	sort.Sort(dcss)

	return dcss
}

func (d *clickHouseDatabase) whereRangeDirRef(
	ctx context.Context,
	mount activeMount,
	queryDir string,
) (treeCatalogDirRef, error) {
	ref, found, err := d.resolveCatalogDirForMount(ctx, mount.mountPath, mount.snapshotID, queryDir)
	if err != nil {
		return treeCatalogDirRef{}, err
	}

	if !found {
		return treeCatalogDirRef{}, db.ErrDirNotFound
	}

	return ref, nil
}

func (d *clickHouseDatabase) whereRangeActiveMount(queryDir string) (activeMount, bool, error) {
	mountPath, ok, err := d.resolveMountScope(queryDir)
	if err != nil || !ok {
		return activeMount{}, false, err
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil || !found {
		return activeMount{}, false, err
	}

	ready, err := d.activeMountReady(mount)
	if err != nil || !ready {
		return activeMount{}, false, err
	}

	return mount, true, nil
}

func (d *clickHouseDatabase) whereRangeCatalog(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
) ([]whereRangeCatalogNode, error) {
	rows, err := d.conn.Query(ctx, whereRangeCatalogQuery, mount.mountPath, mount.snapshotID, ref.dirID, ref.subtreeEnd)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query where catalog range: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanWhereRangeCatalogRows(rows)
}

func scanWhereRangeCatalogRows(rows rowsScanner) ([]whereRangeCatalogNode, error) {
	nodes := make([]whereRangeCatalogNode, 0)

	for rows.Next() {
		var node whereRangeCatalogNode
		if err := rows.Scan(&node.dirID, &node.parentID, &node.fullPath); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan where catalog range: %w", err)
		}

		node.fullPath = ensureTrailingSlash(node.fullPath)
		nodes = append(nodes, node)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: where catalog range iteration error: %w", err)
	}

	return nodes, nil
}

func (d *clickHouseDatabase) whereRangeSummaries(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	if summaries, ok, err := d.whereRangeAgeAllSummaries(ctx, mount, ref, queryDir, filter); err != nil || ok {
		return summaries, err
	}

	if summaries, ok, err := d.whereRangeDirFilterAllSummaries(ctx, mount, ref, filter); err != nil || ok {
		return summaries, err
	}

	if summaries, ok, err := d.whereRangeScalarSummaries(ctx, mount, ref, filter); err != nil || ok {
		return summaries, err
	}

	return d.whereRangeVectorSummaries(ctx, mount, ref, filter)
}

func (d *clickHouseDatabase) whereRangeAgeAllSummaries(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	useAgeAll, err := d.useDirFilterAgeAllForWhere(ctx, mount, queryDir, filter)
	if err != nil || !useAgeAll {
		return nil, false, err
	}

	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, mount.mountPath, mount.snapshotID, filter)
	if err != nil || !ready {
		return nil, false, err
	}

	query, args := whereRangeAgeAllSummariesQuery(mount, ref, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("clickhouse: failed to query AgeAll where range summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanWhereRangeSummaryRows(rows, filter, mount.updatedAt)
}

func whereRangeAgeAllSummariesQuery(
	mount activeMount,
	ref treeCatalogDirRef,
	filter *db.Filter,
) (string, []any) {
	filterExpr, filterArgs := dirFilterAgeAllFilterExpression(filter)
	query := fmt.Sprintf(dirFilterAgeAllWhereRangeSummariesQuery, filterExpr)
	args := make([]any, 0, queryScopeArgs+2+len(filterArgs))
	args = append(args, mount.mountPath, mount.snapshotID, ref.dirID, ref.subtreeEnd)
	args = append(args, filterArgs...)

	return query, args
}

func scanWhereRangeSummaryRows(
	rows rowsScanner,
	filter *db.Filter,
	updatedAt time.Time,
) (map[string]*db.DirSummary, bool, error) {
	summaries, _, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, true, err
}

func (d *clickHouseDatabase) whereRangeDirFilterAllSummaries(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	if !dirFilterAllCanHandleFilter(filter) {
		return nil, false, nil
	}

	ready, err := d.schema3DirFilterAllReady(ctx, mount.mountPath, mount.snapshotID)
	if err != nil || !ready {
		return nil, false, err
	}

	query, args := whereRangeDirFilterAllSummariesQuery(mount, ref, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("clickhouse: failed to query full-filter where range summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanWhereRangeSummaryRows(rows, filter, mount.updatedAt)
}

func whereRangeDirFilterAllSummariesQuery(
	mount activeMount,
	ref treeCatalogDirRef,
	filter *db.Filter,
) (string, []any) {
	return dgutaMaterialisedSubtreeSummariesQuery(
		mount.mountPath,
		mount.snapshotID,
		ref.dirID,
		ref.subtreeEnd,
		filter,
	)
}

func (d *clickHouseDatabase) whereRangeScalarSummaries(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return nil, false, nil
	}

	query, args := whereRangeScalarSummariesQuery(mount, ref, mode)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, true, fmt.Errorf("clickhouse: failed to query scalar where range summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, true, err
}

func whereRangeScalarSummariesQuery(
	mount activeMount,
	ref treeCatalogDirRef,
	mode mountDirSummaryMode,
) (string, []any) {
	columns := whereRangeScalarSummaryColumns(mode)
	query := fmt.Sprintf(
		whereRangeDirFactsScalarSummariesQuery,
		columns.count,
		columns.size,
		columns.atimeMin,
		columns.mtimeMax,
		columns.atimeBuckets,
		columns.mtimeBuckets,
		columns.uids,
		columns.gids,
		columns.ft,
	)

	return query, []any{mount.mountPath, mount.snapshotID, ref.dirID, ref.subtreeEnd}
}

func (d *clickHouseDatabase) whereRangeVectorSummaries(
	ctx context.Context,
	mount activeMount,
	ref treeCatalogDirRef,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := dgutaVectorSubtreeSummariesQuery(
		mount.mountPath,
		mount.snapshotID,
		ref.dirID,
		ref.subtreeEnd,
		filter,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query vector where range summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, err
}

func (d *clickHouseDatabase) DirInfo(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	mountPath, ok, err := d.resolveMountScope(dir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return d.dirInfoOutsideMount(dir, filter)
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return d.dirInfoMissingActiveMount(dir, filter)
	}

	return d.dirInfoActiveMount(mount, dir, filter)
}

func (d *clickHouseDatabase) dirInfoActiveMount(
	mount activeMount,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	if sum, handled, err := d.activeVirtualExactMountRootDirInfo(mount, dir, filter); err != nil || handled {
		return sum, err
	}

	return d.dirInfoSingleMount(
		mount.mountPath, mount.snapshotID, mount.updatedAt, dir, filter,
	)
}

func (d *clickHouseDatabase) activeVirtualExactMountRootDirInfo(
	mount activeMount,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	if ensureTrailingSlash(dir) != ensureTrailingSlash(mount.mountPath) {
		return nil, false, nil
	}

	return d.activeVirtualDirInfo(dir, filter)
}

func (d *clickHouseDatabase) dirInfoOutsideMount(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	if sum, handled, err := d.activeVirtualDirInfo(dir, filter); err != nil || handled {
		return sum, err
	}

	return d.dirInfoAncestor(dir, filter)
}

func (d *clickHouseDatabase) dirInfoMissingActiveMount(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	if sum, handled, err := d.activeVirtualDirInfo(dir, filter); err != nil || handled {
		return sum, err
	}

	return &db.DirSummary{}, db.ErrDirNotFound
}

func (d *clickHouseDatabase) activeVirtualDirInfo(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	result := make(map[string]*db.DirSummary, 1)

	handled, err := d.addActiveVirtualDirInfos(result, []string{dir}, filter)
	if err != nil {
		return nil, false, err
	}

	if !handled[ensureTrailingSlash(dir)] {
		return nil, false, nil
	}

	return result[dir], true, nil
}

func (d *clickHouseDatabase) dirInfoSingleMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string, filter *db.Filter,
) (*db.DirSummary, error) {
	if sum, done, err := d.dirInfoSingleMountGuard(mountPath, snapshotID, updatedAt, filter); done || err != nil {
		return sum, err
	}

	sum, handled, err := d.dirInfoSingleMountFacts(mountPath, snapshotID, updatedAt, dir, filter)
	if err != nil || handled {
		return sum, err
	}

	sum, ok, err := d.maintainedDirInfoSingleMount(mountPath, snapshotID, updatedAt, dir, filter)
	if dirInfoSingleRouteDone(ok, err) {
		return sum, err
	}

	sum, ok, err = d.vectorDirInfoSingleMount(mountPath, snapshotID, updatedAt, dir, filter)
	if dirInfoSingleRouteDone(ok, err) {
		return sum, err
	}

	return d.dirInfoSingleMountFallback(mountPath, snapshotID, updatedAt, dir, filter)
}

func fullFilterAlwaysEmpty(filter *db.Filter) bool {
	return filter != nil && (isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs))
}

func dirInfoSingleRouteDone(ok bool, err error) bool {
	return err != nil || ok
}

func (d *clickHouseDatabase) maintainedDirInfoSingleMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	sum, found, ok, err := d.dirSummaryForDirMount(mountPath, snapshotID, updatedAt, dir, filter)
	if err != nil || !ok {
		return nil, ok, err
	}

	if !found && !mountDirSummaryMissingMeansNotFound(filter) {
		return nil, false, nil
	}

	result, err := dirInfoSummaryResult(sum, found, updatedAt)

	return result, true, err
}

func dirInfoSummaryResult(
	sum *db.DirSummary,
	found bool,
	updatedAt time.Time,
) (*db.DirSummary, error) {
	if !found {
		return &db.DirSummary{Modtime: updatedAt}, db.ErrDirNotFound
	}

	return sum, nil
}

func (d *clickHouseDatabase) vectorDirInfoSingleMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	sum, found, ok, err := d.dirInfoDGUTAVectorMount(mountPath, snapshotID, updatedAt, dir, filter)
	if err != nil || !ok {
		return nil, ok, err
	}

	result, err := dirInfoSummaryResult(sum, found, updatedAt)

	return result, true, err
}

func (d *clickHouseDatabase) activeMountFactsReady(mountPath, snapshotID string) (bool, error) {
	if d.conn == nil {
		return false, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
}

func (d *clickHouseDatabase) activeMountReady(mount activeMount) (bool, error) {
	return d.activeMountFactsReady(mount.mountPath, mount.snapshotID)
}

func (d *clickHouseDatabase) readyActiveMounts(mounts []activeMount) ([]activeMount, error) {
	if len(mounts) == 0 || d.conn == nil {
		return nil, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.readyActiveMountsCached(ctx, mounts)
}

func (d *clickHouseDatabase) readyActiveMountsCached(
	ctx context.Context,
	mounts []activeMount,
) ([]activeMount, error) {
	readyMounts, missing := d.cachedReadyActiveMounts(mounts)
	if len(missing) == 0 {
		return readyMounts, nil
	}

	queried, err := d.queryReadyActiveMounts(ctx, missing)
	if err != nil {
		return nil, err
	}

	for _, mount := range missing {
		key := newTreeMountCacheKey(mount.mountPath, mount.snapshotID)
		if !queried[key] {
			continue
		}

		d.treeCache.putMountDirSummaryReady(key)

		readyMounts = append(readyMounts, mount)
	}

	return readyMounts, nil
}

func (d *clickHouseDatabase) cachedReadyActiveMounts(
	mounts []activeMount,
) ([]activeMount, []activeMount) {
	readyMounts := make([]activeMount, 0, len(mounts))
	missing := make([]activeMount, 0, len(mounts))
	seen := make(map[treeMountCacheKey]bool, len(mounts))

	for _, mount := range mounts {
		key := newTreeMountCacheKey(mount.mountPath, mount.snapshotID)
		if seen[key] {
			continue
		}

		seen[key] = true
		if d.treeCache.getMountDirSummaryReady(key) {
			readyMounts = append(readyMounts, mount)

			continue
		}

		missing = append(missing, mount)
	}

	return readyMounts, missing
}

func (d *clickHouseDatabase) queryReadyActiveMounts(
	ctx context.Context,
	mounts []activeMount,
) (map[treeMountCacheKey]bool, error) {
	return queryReadyActiveMountRows(ctx, d.conn, mounts)
}

func (d *clickHouseDatabase) dirFilterAllSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if len(dirs) == 0 {
		return map[string]*db.DirSummary{}, map[string]bool{}, true, nil
	}

	if !dirFilterAllCanHandleFilter(filter) {
		return nil, nil, false, nil
	}

	if fullFilterAlwaysEmpty(filter) {
		return map[string]*db.DirSummary{}, handledDirFilterAllDirs(dirs), true, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.schema3DirFilterAllReady(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return nil, nil, false, err
	}

	return d.readyDirFilterAllSummariesForDirsMount(ctx, mountPath, snapshotID, updatedAt, dirs, filter)
}

func dirFilterAllCanHandleFilter(filter *db.Filter) bool {
	if filter == nil {
		return false
	}

	if fullFilterAlwaysEmpty(filter) {
		return true
	}

	if _, ok := mountDirSummaryModeForFilter(filter); ok {
		return false
	}

	return true
}

func (d *clickHouseDatabase) readyDirFilterAllSummariesForDirsMount(
	ctx context.Context,
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	summaries, err := d.dirFilterAllSummaryBatches(ctx, mountPath, snapshotID, updatedAt, dirs, filter)
	if err != nil {
		if isUnknownTable(err) {
			return nil, nil, false, nil
		}

		return nil, nil, true, err
	}

	return summaries, handledDirFilterAllDirs(dirs), true, nil
}

func handledDirFilterAllDirs(dirs []string) map[string]bool {
	handled := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		handled[dir] = true
	}

	return handled
}

func (d *clickHouseDatabase) schema3DirFilterAllReady(
	ctx context.Context,
	mountPath, snapshotID string,
) (bool, error) {
	key := newTreeMountCacheKey(mountPath, snapshotID)
	if ready, cached := d.treeCache.getDirFilterAllReady(key); cached {
		return ready, nil
	}

	ready, err := d.queryLatestSchema3RowCountReady(ctx, schema3DirFilterAllReadyQuery, mountPath, snapshotID)
	if err != nil {
		return ready, err
	}

	d.treeCache.putDirFilterAllReady(key, ready)

	return ready, nil
}

func (d *clickHouseDatabase) schema3ChildFilterAllReady(
	ctx context.Context,
	mountPath, snapshotID string,
) (bool, error) {
	key := newTreeMountCacheKey(mountPath, snapshotID)
	if ready, cached := d.treeCache.getChildFilterAllReady(key); cached {
		return ready, nil
	}

	ready, err := d.queryLatestSchema3RowCountReady(ctx, schema3ChildFilterAllReadyQuery, mountPath, snapshotID)
	if err != nil {
		return ready, err
	}

	d.treeCache.putChildFilterAllReady(key, ready)

	return ready, nil
}

func (d *clickHouseDatabase) queryLatestSchema3RowCountReady(
	ctx context.Context,
	query string,
	mountPath, snapshotID string,
) (bool, error) {
	rows, err := d.conn.Query(ctx, query, mountPath, snapshotID, currentSchemaVersion)
	if err != nil {
		if isUnknownTable(err) {
			return false, nil
		}

		return false, fmt.Errorf("clickhouse: failed to query schema3 row-count readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		var count uint64
		if err := rows.Scan(&count); err != nil {
			return false, fmt.Errorf("clickhouse: failed to scan schema3 row-count readiness: %w", err)
		}

		return count > 0, nil
	}

	if err := rowsErr(rows); err != nil {
		return false, fmt.Errorf("clickhouse: schema3 row-count readiness iteration error: %w", err)
	}

	return false, nil
}

func (d *clickHouseDatabase) dirFilterAllSummaryBatches(
	ctx context.Context,
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	summaries := make(map[string]*db.DirSummary)

	for _, batchDirs := range stringValueBatches(dirs) {
		batch, err := d.queryDirFilterAllSummariesForDirsBatch(
			ctx,
			mountPath,
			snapshotID,
			updatedAt,
			batchDirs,
			filter,
		)
		if err != nil {
			return nil, err
		}

		mergeDirFilterAllSummaries(summaries, batch)
	}

	return summaries, nil
}

func mergeDirFilterAllSummaries(summaries, batch map[string]*db.DirSummary) {
	for dir, sum := range batch {
		summaries[dir] = sum
	}
}

func (d *clickHouseDatabase) queryDirFilterAllSummariesForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := dirFilterAllSummariesForDirsQueryForFilter(mountPath, snapshotID, dirs, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query full-filter dir summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, err
}

func dirFilterAllSummariesForDirsQueryForFilter(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	query := fmt.Sprintf(dirFilterAllSummariesForDirsQuery, clauses, placeholders(len(dirs)))
	args := make([]any, 0, queryScopeArgs+1+len(filterArgs)+len(dirs))
	args = append(args, mountPath, snapshotID, uint8(filter.Age))
	args = append(args, filterArgs...)

	for _, dir := range dirs {
		args = append(args, dir)
	}

	return query, args
}

func (d *clickHouseDatabase) childFilterAllPacketChildSummaries(
	ctx context.Context,
	mount activeMount,
	parentDir string,
	filter *db.Filter,
) ([]childFilterAllSummary, error) {
	facts, err := d.queryChildFilterAllPacketChildSummaries(ctx, mount, parentDir, filter)
	if err != nil {
		return nil, err
	}

	d.treeCache.childFilterAllReads.Add(1)

	return facts, nil
}

func (d *clickHouseDatabase) queryChildFilterAllPacketChildSummaries(
	ctx context.Context,
	mount activeMount,
	parentDir string,
	filter *db.Filter,
) ([]childFilterAllSummary, error) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	args := make([]any, 0, childFilterAllPacketBaseArgs+len(filterArgs))
	args = append(args,
		mount.mountPath,
		mount.snapshotID,
		ensureTrailingSlash(parentDir),
		uint8(filter.Age),
	)
	args = append(args, filterArgs...)

	rows, err := d.conn.Query(
		ctx,
		fmt.Sprintf(childFilterAllChildSummariesPacketQuery, clauses),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query filtered child summary packet: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanChildFilterAllChildSummaryRows(rows, filter, mount.updatedAt)
}

func fullFilterOptionalClauses(filter *db.Filter) (string, []any) {
	clauses := make([]string, 0, fullFilterOptionalClauseCount)
	args := make([]any, 0, dirSummaryFilterArgCap(filter))

	appendIDMembershipClause(&clauses, &args, "gid", filter.GIDs)
	appendIDMembershipClause(&clauses, &args, "uid", filter.UIDs)
	appendFTMembershipClause(&clauses, &args, "ft", filter.FT)

	if len(clauses) == 0 {
		return "", args
	}

	return " AND " + strings.Join(clauses, " AND "), args
}

func scanChildFilterAllChildSummaryRows(
	rows rowsScanner,
	filter *db.Filter,
	updatedAt time.Time,
) ([]childFilterAllSummary, error) {
	summaries := make([]childFilterAllSummary, 0)

	for rows.Next() {
		summary, err := scanChildFilterAllChildSummaryRow(rows, filter, updatedAt)
		if err != nil {
			return nil, err
		}

		summaries = append(summaries, summary)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: filtered child summary packet iteration error: %w", err)
	}

	return summaries, nil
}

func (d *clickHouseDatabase) addChildFilterAllDirsHaveChildrenForMount(
	result map[string]bool,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	if !childFilterAllCanHandleDirsHaveChildrenFilter(filter) {
		return group, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.schema3ChildFilterAllReady(ctx, group.mount.mountPath, group.mount.snapshotID)
	if err != nil {
		return nil, err
	}

	if !ready {
		group.schema3ChildFilterAllUnavailable = true

		return group, nil
	}

	return d.addReadyChildFilterAllDirsHaveChildren(ctx, result, group, filter)
}

func childFilterAllCanHandleDirsHaveChildrenFilter(filter *db.Filter) bool {
	return dirFilterAllCanHandleFilter(filter)
}

func (d *clickHouseDatabase) addReadyChildFilterAllDirsHaveChildren(
	ctx context.Context,
	result map[string]bool,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	parents, handled, err := d.childFilterAllParentsWithMatchingChildren(ctx, group, filter)
	if err != nil {
		if isUnknownTable(err) {
			group.schema3ChildFilterAllUnavailable = true

			return group, nil
		}

		return nil, err
	}

	for _, queryDir := range group.queryDirs {
		if parents[queryDir] {
			result[group.originalDirs[queryDir]] = true
		}
	}

	remaining := remainingDirInfoGroup(group, handled)

	return remaining, nil
}

func dirInfoRequestsByParent(group *activeMountDirGroup) map[string]map[string]bool {
	requests := make(map[string]map[string]bool)

	for _, queryDir := range group.queryDirs {
		parentDir := parentDirForPath(queryDir)

		requested := requests[parentDir]
		if requested == nil {
			requested = make(map[string]bool)
			requests[parentDir] = requested
		}

		requested[queryDir] = true
	}

	return requests
}

func sortedRequestParents(requests map[string]map[string]bool) []string {
	parentDirs := make([]string, 0, len(requests))
	for parentDir := range requests {
		parentDirs = append(parentDirs, parentDir)
	}

	sort.Strings(parentDirs)

	return parentDirs
}

func (d *clickHouseDatabase) addIndexedDirsHaveChildrenForMount(
	result map[string]bool,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	if fullFilterAlwaysEmpty(filter) {
		return remainingDirInfoGroup(group, handledDirFilterAllDirs(group.queryDirs)), nil
	}

	if broadFilterCanUseChildRows(filter) {
		return d.addCatalogDirsHaveChildrenForMount(result, group)
	}

	group, err := d.addChildFilterAllDirsHaveChildrenForMount(result, group, filter)
	if err != nil || len(group.queryDirs) == 0 {
		return group, err
	}

	return group, nil
}

func (d *clickHouseDatabase) addChildFilterAllDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	if !dirFilterAllCanHandleFilter(filter) {
		return group, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.schema3ChildFilterAllReady(ctx, group.mount.mountPath, group.mount.snapshotID)
	if err != nil || !ready {
		return group, err
	}

	requests := dirInfoRequestsByParent(group)

	handled, err := d.addChildFilterAllDirInfoRequestResults(ctx, result, group, requests, filter)
	if err != nil {
		if isUnknownTable(err) {
			return group, nil
		}

		return nil, err
	}

	return remainingDirInfoGroup(group, handled), nil
}

func (d *clickHouseDatabase) addChildFilterAllDirInfoRequestResults(
	ctx context.Context,
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	requests map[string]map[string]bool,
	filter *db.Filter,
) (map[string]bool, error) {
	handled := make(map[string]bool)

	for _, parentDir := range sortedRequestParents(requests) {
		facts, err := d.childFilterAllPacketChildSummaries(ctx, group.mount, parentDir, filter)
		if err != nil {
			return nil, err
		}

		d.addChildFilterAllDirInfoResults(result, group, requests[parentDir], facts, handled)

		for queryDir := range requests[parentDir] {
			handled[queryDir] = true
		}
	}

	return handled, nil
}

func (d *clickHouseDatabase) addFullFilterDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) error {
	if fullFilterAlwaysEmpty(filter) {
		return nil
	}

	for _, route := range d.fullFilterDirInfoRoutes(filter) {
		var err error

		group, err = route(result, group, filter)
		if dirInfoRouteDone(group, err) {
			return err
		}
	}

	ok, err := d.addGroupedDirInfosForMount(result, group, filter)
	if err != nil || ok {
		return err
	}

	return d.addRawDirInfosForMount(result, group, filter)
}

func (d *clickHouseDatabase) fullFilterDirInfoRoutes(filter *db.Filter) []dirInfoMountRoute {
	var routes []dirInfoMountRoute
	if dirFilterAgeAllCanHandleFilter(filter) {
		routes = append(routes, d.addDirFilterAgeAllDirInfosForMount)
	}

	dirFilterAllPreferred := dirFilterAllPreferredForDirInfo(filter)
	if dirFilterAllPreferred {
		routes = append(routes, d.addDirFilterAllDirInfosForMount)
	}

	routes = append(routes, d.addWideChildFilterAllDirInfosForMount)

	if !dirFilterAllPreferred {
		routes = append(routes, d.addDirFilterAllDirInfosForMount)
	}

	routes = append(routes, d.addChildFilterAllDirInfosForMount)

	return routes
}

func dirFilterAllPreferredForDirInfo(filter *db.Filter) bool {
	return filter != nil &&
		filter.Age != db.DGUTAgeAll &&
		filter.GIDs == nil &&
		filter.UIDs == nil &&
		filter.FT == 0
}

func (d *clickHouseDatabase) addWideChildFilterAllDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	if len(group.queryDirs) <= dirsHaveChildrenSummaryFanoutLimit {
		return group, nil
	}

	return d.addChildFilterAllDirInfosForMount(result, group, filter)
}

func (d *clickHouseDatabase) addDirFilterAgeAllDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	summaries, handled, ok, err := d.dirFilterAgeAllSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.mount.updatedAt,
		group.queryDirs,
		filter,
	)
	if err != nil || !ok {
		return group, err
	}

	d.addGroupedDirSummaries(result, group, summaries)

	return remainingDirInfoGroup(group, handled), nil
}

func (d *clickHouseDatabase) addDirFilterAllDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (*activeMountDirGroup, error) {
	summaries, handled, ok, err := d.dirFilterAllSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.mount.updatedAt,
		group.queryDirs,
		filter,
	)
	if err != nil || !ok {
		return group, err
	}

	d.addGroupedDirSummaries(result, group, summaries)

	return remainingDirInfoGroup(group, handled), nil
}

func dirInfoRouteDone(group *activeMountDirGroup, err error) bool {
	return err != nil || len(group.queryDirs) == 0
}

func (d *clickHouseDatabase) filteredMountWhereSummariesFromAgeAll(
	ctx context.Context,
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	useAgeAll, err := d.useDirFilterAgeAllForWhere(ctx, mount, queryDir, filter)
	if err != nil || !useAgeAll {
		return nil, false, err
	}

	return d.dirFilterAgeAllWhereSummaries(ctx, mount, queryDir, filter)
}

func (d *clickHouseDatabase) useDirFilterAgeAllForWhere(
	ctx context.Context,
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (bool, error) {
	if !dirFilterAgeAllCanHandleFilter(filter) {
		return false, nil
	}

	evidence, ok, err := whereDirAgeAllRouteEvidenceFor(ctx, d, mount, queryDir, filter)
	if err != nil || !ok {
		return false, err
	}

	return evidence.ageAllExact && evidence.allExact && evidence.ageAllP95 < evidence.allP95, nil
}

func (d *clickHouseDatabase) dirFilterAllWhereSummaries(
	ctx context.Context,
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	ready, err := d.schema3DirFilterAllReady(ctx, mount.mountPath, mount.snapshotID)
	if err != nil || !ready {
		return nil, false, err
	}

	query, args := dirFilterAllWhereSummariesQueryForFilter(mount, queryDir, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("clickhouse: failed to query full-filter where summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, true, err
}

func dirFilterAllWhereSummariesQueryForFilter(
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	query := fmt.Sprintf(dirFilterAllWhereSummariesQuery, clauses)

	args := make([]any, 0, queryScopeArgs+2+len(filterArgs))
	args = append(args, mount.mountPath, mount.snapshotID, uint8(filter.Age))
	args = append(args, filterArgs...)
	args = append(args, ensureTrailingSlash(queryDir))

	return query, args
}

func (d *clickHouseDatabase) queryActiveMount(
	query string,
	args ...any,
) (activeMount, bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return activeMount{}, false, fmt.Errorf("clickhouse: failed to resolve active mount: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: active mount iteration error"); iterErr != nil {
			return activeMount{}, false, iterErr
		}

		return activeMount{}, false, nil
	}

	mount, err := scanActiveMountRow(rows)
	if err != nil {
		return activeMount{}, false, err
	}

	return mount, true, nil
}

func scanActiveMountRow(rows rowsScanner) (activeMount, error) {
	var (
		mountPath, snapshotID, activeSetID string
		updatedAt                          time.Time
	)

	if err := rows.Scan(&mountPath, &snapshotID, &updatedAt, &activeSetID); err != nil {
		return activeMount{}, fmt.Errorf("clickhouse: failed to scan active mount: %w", err)
	}

	updatedAt = updatedAt.UTC()

	return activeMount{
		mountPath:   mountPath,
		snapshotID:  snapshotID,
		updatedAt:   updatedAt,
		activeSetID: activeSetID,
	}, nil
}

func (d *clickHouseDatabase) addActiveVirtualDirsHaveChildren(
	result map[string]bool,
	dirs []string,
	filter *db.Filter,
) ([]string, error) {
	remaining := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		hasChildren, handled, err := d.activeVirtualHasChildren(dir, filter)
		if err != nil {
			return nil, err
		}

		if !handled {
			remaining = append(remaining, dir)

			continue
		}

		result[dir] = hasChildren
	}

	return remaining, nil
}

func (d *clickHouseDatabase) filteredMountWhereTraversalSummaries(
	ctx context.Context,
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	summaries, ok, err := d.filteredMountWhereSummariesFromAgeAll(ctx, mount, queryDir, filter)
	if err != nil || ok {
		return summaries, ok, err
	}

	summaries, ok, err = d.dirFilterAllWhereSummaries(ctx, mount, queryDir, filter)
	if err != nil || ok || !legacyFilteredMountWherePreloadCanHandle(filter) {
		return summaries, ok, err
	}

	summaries, err = d.filteredMountWhereFactsSummaries(ctx, mount, filter)

	return summaries, true, err
}

func legacyFilteredMountWherePreloadCanHandle(filter *db.Filter) bool {
	return filter == nil ||
		(filter.Age == db.DGUTAgeAll && (filter.GIDs != nil || filter.UIDs != nil))
}

func (d *clickHouseDatabase) filteredMountWhereFactsSummaries(
	ctx context.Context,
	mount activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := filteredMountWhereSummariesQueryForFilter(mount.mountPath, mount.snapshotID, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query filtered mount where summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, err
}

func (d *clickHouseDatabase) childrenOutsideMount(parentDir string) ([]string, error) {
	if children, handled, err := d.activeVirtualChildren(parentDir); err != nil || handled {
		return children, err
	}

	return d.childrenForAncestor(parentDir)
}

func (d *clickHouseDatabase) childrenMissingActiveMount(parentDir string) ([]string, error) {
	if children, handled, err := d.activeVirtualChildren(parentDir); err != nil || handled {
		return children, err
	}

	return nil, nil
}

func (d *clickHouseDatabase) addChildFilterAllDirInfoResults(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	requested map[string]bool,
	facts []childFilterAllSummary,
	handled map[string]bool,
) {
	for _, fact := range facts {
		queryDir := ensureTrailingSlash(fact.Dir)
		if !requested[queryDir] {
			continue
		}

		handled[queryDir] = true

		if fact.Summary == nil {
			if fact.HasChildren {
				originalDir := group.originalDirs[queryDir]
				result[originalDir] = emptyDirInfoSummary(originalDir, fact.Age, group.mount.updatedAt)
			}

			continue
		}

		originalDir := group.originalDirs[queryDir]
		fact.Summary.Dir = originalDir
		result[originalDir] = fact.Summary
	}
}

func emptyDirInfoSummary(
	dir string,
	age db.DirGUTAge,
	updatedAt time.Time,
) *db.DirSummary {
	return &db.DirSummary{
		Dir:     dir,
		Age:     age,
		Modtime: updatedAt.UTC(),
	}
}

func (d *clickHouseDatabase) addFallbackActiveMountRootDirsHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	mounts []activeMount,
	filter *db.Filter,
) error {
	childrenByParent, err := d.childrenForActiveMountRoots(mounts)
	if err != nil {
		return err
	}

	parents, err := d.parentActiveMountRootsWithChildren(mounts, filter, childrenByParent)
	if err != nil {
		return err
	}

	for parent := range parents {
		for _, original := range roots.originalDirs[parent] {
			result[original] = true
		}
	}

	return nil
}

func unhandledDirs(dirs []string, handled map[string]bool) []string {
	remaining := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if handled[ensureTrailingSlash(dir)] {
			continue
		}

		remaining = append(remaining, dir)
	}

	return remaining
}

func scanChildFilterAllChildSummaryRow(
	rows rowsScanner,
	filter *db.Filter,
	updatedAt time.Time,
) (childFilterAllSummary, error) {
	var scanned childFilterAllSummaryScanned
	if err := scanned.scanFrom(rows); err != nil {
		return childFilterAllSummary{}, err
	}

	age := db.DGUTAgeAll
	if filter != nil {
		age = filter.Age
	}

	return childFilterAllSummary{
		Dir:         scanned.summary.dir,
		Summary:     scanned.summary.summary(filter, updatedAt),
		Age:         age,
		HasChildren: scanned.filterChildCount > 0,
		ChildCount:  scanned.filterChildCount,
	}, nil
}

func scanDirFactSummaryRow(rows rowsScanner) (uint32, time.Time, db.GUTAs, error) {
	var (
		dirID     uint32
		updatedAt time.Time
		vector    dgutaVectorColumns
	)

	dest := make([]any, 0, mountDirFactScalarColumns+dirFactVectorColumnCount)
	dest = append(dest,
		&dirID,
		&updatedAt,
	)
	dest = append(dest, dirFactVectorScanDest(&vector)...)

	if err := rows.Scan(dest...); err != nil {
		return 0, time.Time{}, nil, fmt.Errorf("clickhouse: failed to scan dir facts row: %w", err)
	}

	gutas, err := vector.gutas("dir_id", strconv.FormatUint(uint64(dirID), 10))
	if err != nil {
		return 0, time.Time{}, nil, err
	}

	return dirID, updatedAt.UTC(), gutas, nil
}

func dirFactVectorScanDest(vector *dgutaVectorColumns) []any {
	return []any{
		&vector.gids,
		&vector.uids,
		&vector.fts,
		&vector.ages,
		&vector.counts,
		&vector.sizes,
		&vector.atimeMins,
		&vector.mtimeMaxs,
		&vector.atimeBuckets,
		&vector.mtimeBuckets,
	}
}

func scanReadyActiveMountRows(rows rowsScanner) (map[treeMountCacheKey]bool, error) {
	ready := make(map[treeMountCacheKey]bool)

	for rows.Next() {
		var mountPath, snapshotID string
		if err := rows.Scan(&mountPath, &snapshotID); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan batched dir summary readiness: %w", err)
		}

		ready[newTreeMountCacheKey(mountPath, snapshotID)] = true
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: batched dir summary readiness iteration error: %w", err)
	}

	return ready, nil
}

func (d *clickHouseDatabase) readyActiveMountsUnder(dir string) ([]activeMount, error) {
	mounts, err := d.activeMountsUnder(dir)
	if err != nil || len(mounts) == 0 {
		return mounts, err
	}

	return d.readyActiveMounts(mounts)
}

func (d *clickHouseDatabase) dirInfoSingleMountFallback(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	sum, found, ok, err := d.dirSummaryForDirMount(mountPath, snapshotID, updatedAt, dir, filter)
	if err != nil {
		return nil, err
	}

	if ok && found {
		return dirInfoSummaryResult(sum, found, updatedAt)
	}

	gutas, err := d.gutasForDir(
		mountPath, snapshotID, ensureTrailingSlash(dir),
	)
	if err != nil {
		return nil, err
	}

	if len(gutas) == 0 {
		return &db.DirSummary{Modtime: updatedAt}, db.ErrDirNotFound
	}

	return dirSummaryWithModtime(gutas, filter, updatedAt), nil
}

func (d *clickHouseDatabase) dirInfoDGUTAVectorMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, bool, error) {
	if !mountDirDGUTAVectorCanHandleFilter(filter) {
		return nil, false, false, nil
	}

	queryDir := ensureTrailingSlash(dir)

	gutasByDir, ok, err := d.mountDirDGUTAVectorsForDirsMount(mountPath, snapshotID, []string{queryDir})
	if err != nil || !ok {
		return nil, false, ok, err
	}

	gutas, found := gutasByDir[queryDir]
	if !found {
		return nil, false, true, nil
	}

	return dirSummaryWithModtime(gutas, filter, updatedAt), true, true, nil
}

// Where resolves Tree.Where with set-oriented ClickHouse subtree summaries.
func (d *clickHouseDatabase) Where(
	dir string,
	filter *db.Filter,
	recurseCount func(string) int,
) (db.DCSs, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	filter = defaultWhereFilter(filter)
	if fullFilterAlwaysEmpty(filter) {
		return db.DCSs{}, nil
	}

	queryDir := ensureTrailingSlash(dir)
	if dcss, handled, err := d.whereByDirIDRange(queryDir, dir, filter, recurseCount); err != nil || handled {
		return dcss, err
	}

	traversal, err := d.whereTraversalFor(queryDir, filter)
	if err != nil {
		return nil, err
	}

	if err := traversal.preloadFilteredMountWhere(queryDir); err != nil {
		return nil, err
	}

	return d.whereFromTraversal(dir, filter, recurseCount, traversal)
}

func defaultWhereFilter(filter *db.Filter) *db.Filter {
	if filter == nil {
		filter = new(db.Filter)
	}

	if filter.FT == 0 {
		filter.FT = db.AllTypesExceptDirectories
	}

	return filter
}

func ensureTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}

	return dir + "/"
}

func dirSummaryWithModtime(gutas db.GUTAs, filter *db.Filter, updatedAt time.Time) *db.DirSummary {
	sum := gutas.Summary(filter)
	if sum != nil {
		sum.Modtime = updatedAt
	}

	return sum
}

func (d *clickHouseDatabase) whereTraversalFor(
	queryDir string,
	filter *db.Filter,
) (*whereTraversal, error) {
	mountPath, ok, err := d.resolveMountScope(queryDir)
	if err != nil {
		return nil, err
	}

	traversal := newWhereTraversal(d, filter)
	if !ok {
		return traversal, nil
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return traversal, nil
	}

	return d.readyWhereTraversal(traversal, mount)
}

func newWhereTraversal(
	d *clickHouseDatabase,
	filter *db.Filter,
) *whereTraversal {
	return &whereTraversal{
		database:       d,
		filter:         filter,
		summaries:      make(map[string]*db.DirSummary),
		summaryLoaded:  make(map[string]bool),
		children:       make(map[string][]string),
		childrenLoaded: make(map[string]bool),
	}
}

func (d *clickHouseDatabase) readyWhereTraversal(
	traversal *whereTraversal,
	mount activeMount,
) (*whereTraversal, error) {
	ready, err := d.activeMountReady(mount)
	if err != nil || !ready {
		return traversal, err
	}

	traversal.mount = &mount

	return traversal, nil
}

func (d *clickHouseDatabase) whereFromTraversal(
	dir string,
	filter *db.Filter,
	recurseCount func(string) int,
	traversal *whereTraversal,
) (db.DCSs, error) {
	dcss, err := traversal.where(dir, recurseCount)
	if err != nil {
		return nil, err
	}

	if len(dcss) == 0 {
		sum, err := d.dirInfoAllowVirtualAncestor(dir, filter)
		if err != nil || sum == nil {
			return nil, err
		}
	}

	sort.Sort(dcss)

	return dcss, nil
}

func (d *clickHouseDatabase) dirInfoAncestor(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	normDir := ensureTrailingSlash(dir)

	if sum, ok, err := d.dirInfoActivePrefixAncestor(normDir, filter); err != nil || ok {
		return sum, err
	}

	return d.dirInfoAncestorAfterActivePrefix(normDir, filter)
}

func (d *clickHouseDatabase) dirInfoAncestorAfterActivePrefix(
	normDir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	if sum, ok, err := d.dirInfoTreeSummaryAncestor(normDir, filter); err != nil || ok {
		return sum, err
	}

	return d.dirInfoAncestorFallback(normDir, filter)
}

func (d *clickHouseDatabase) dirInfoActivePrefixAncestor(
	normDir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	sum, handled, miss, err := d.activePrefixAncestorDirSummary(normDir, filter)
	if err != nil || handled {
		return sum, handled, err
	}

	if !miss {
		return nil, false, nil
	}

	sum, err = activePrefixRollupFallback(func() (*db.DirSummary, error) {
		return d.dirInfoAncestorAfterActivePrefix(normDir, filter)
	})

	return sum, true, err
}

func (d *clickHouseDatabase) activePrefixAncestorDirSummary(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	activeSetID, _, err := d.currentActiveMountsSet(ctx)
	if err != nil || activeSetID == "" {
		return nil, false, false, err
	}

	key := activePrefixSummaryCacheKey(activeSetID, dir, filter)
	if sum, ok := d.treeCache.getActivePrefixDirSummary(key); ok {
		return sum, true, false, nil
	}

	sum, handled, miss, err := activePrefixDirSummary(ctx, d.conn, activeSetID, dir, filter)
	if err != nil || !handled {
		return sum, handled, miss, err
	}

	d.treeCache.putActivePrefixDirSummary(key, sum)

	return sum, handled, miss, nil
}

func activePrefixSummaryCacheKey(
	activeSetID string,
	dir string,
	filter *db.Filter,
) treeActivePrefixSummaryCacheKey {
	return newTreeActivePrefixSummaryCacheKey(
		activeSetID,
		dir,
		filter,
		treePermissionCacheInputs{},
		currentSchemaVersion,
		activePrefixDirSummaryQueryVersion,
	)
}

func (d *clickHouseDatabase) dirInfoAncestorFallback(
	normDir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	mounts, err := d.readyActiveMountsUnder(normDir)
	if err != nil {
		return nil, err
	}

	gutas, updatedAt, found, err := d.dirFactGUTAsForAncestorMounts(normDir, mounts)
	if err != nil {
		return nil, err
	}

	if !found {
		sum, ok, virtualErr := d.dirInfoVirtualAncestorFromFacts(normDir, filter)
		if virtualErr != nil || ok {
			return sum, virtualErr
		}

		return &db.DirSummary{}, db.ErrDirNotFound
	}

	return dirSummaryWithModtime(gutas, filter, updatedAt), nil
}

func (d *clickHouseDatabase) dirInfoAllowVirtualAncestor(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	sum, err := d.DirInfo(dir, filter)
	if err == nil || !errors.Is(err, db.ErrDirNotFound) {
		return sum, err
	}

	virtual, ok, virtualErr := d.dirInfoVirtualAncestor(ensureTrailingSlash(dir), filter)
	if virtualErr != nil || ok {
		return virtual, virtualErr
	}

	return sum, err
}

func (d *clickHouseDatabase) dirInfoVirtualAncestor(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	sum, handled, miss, err := d.activePrefixAncestorDirSummary(dir, filter)
	if err != nil || handled {
		return sum, handled, err
	}

	if miss {
		var ok bool

		sum, err = activePrefixRollupFallback(func() (*db.DirSummary, error) {
			var fallbackErr error

			sum, ok, fallbackErr = d.dirInfoVirtualAncestorFromFacts(dir, filter)

			return sum, fallbackErr
		})

		return sum, ok || err != nil, err
	}

	return d.dirInfoVirtualAncestorFromFacts(dir, filter)
}

func (d *clickHouseDatabase) dirInfoVirtualAncestorFromFacts(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	mounts, err := d.activeMountsUnder(dir)
	if err != nil || len(mounts) == 0 {
		return nil, false, err
	}

	mounts, err = d.readyActiveMounts(mounts)
	if err != nil || len(mounts) == 0 {
		return nil, false, err
	}

	gutasByRoot, err := d.gutasForActiveMountRootDirs(mounts)
	if err != nil {
		return nil, false, err
	}

	gutas := activeMountRootGUTAs(mounts, gutasByRoot)
	if len(gutas) == 0 {
		return nil, false, nil
	}

	updatedAt := maxUpdatedAtForMounts(mounts)

	return dirSummaryWithModtime(gutas, filter, updatedAt), true, nil
}

func activeMountRootGUTAs(mounts []activeMount, gutasByRoot map[string]db.GUTAs) db.GUTAs {
	gutas := make(db.GUTAs, 0, len(gutasByRoot))
	for _, mount := range mounts {
		gutas = append(gutas, gutasByRoot[mount.mountPath]...)
	}

	return gutas
}

func (d *clickHouseDatabase) dirInfoTreeSummaryAncestor(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	if sum, ok, err := d.treeDirSummary(ctx, dir, filter); err != nil || ok {
		return sum, ok, err
	}

	gutas, updatedAt, ok, err := d.treeSummaryGUTAs(ctx, dir)
	if err != nil || !ok {
		return nil, ok, err
	}

	return dirSummaryWithModtime(gutas, filter, updatedAt), true, nil
}

func (d *clickHouseDatabase) Children(dir string) ([]string, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	parentDir := ensureTrailingSlash(dir)

	mountPath, ok, err := d.resolveMountScope(dir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return d.childrenOutsideMount(parentDir)
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return d.childrenMissingActiveMount(parentDir)
	}

	return d.childrenForReadyActiveMount(mount, parentDir)
}

func (d *clickHouseDatabase) childrenForReadyActiveMount(
	mount activeMount,
	parentDir string,
) ([]string, error) {
	ready, err := d.activeMountReady(mount)
	if err != nil || !ready {
		return nil, err
	}

	return d.childrenForMount(mount.mountPath, mount.snapshotID, parentDir)
}

// DirInfos returns directory summaries for multiple directories.
//
//nolint:funlen
func (d *clickHouseDatabase) DirInfos(
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	result := make(map[string]*db.DirSummary, len(dirs))

	handled, err := d.addActiveVirtualDirInfos(result, dirs, filter)
	if err != nil {
		return nil, err
	}

	roots, remaining, err := d.splitActiveMountRootDirs(unhandledDirs(dirs, handled))
	if err != nil {
		return nil, err
	}

	if addErr := d.addActiveMountRootDirInfos(result, roots, filter); addErr != nil {
		return nil, addErr
	}

	groups, fallback, err := d.groupDirsByActiveMount(remaining)
	if err != nil {
		return nil, err
	}

	if err := d.addDirInfoGroups(result, groups, filter); err != nil {
		return nil, err
	}

	return result, d.addFallbackDirInfos(result, fallback, filter)
}

// DirsHaveChildren returns whether each directory has filter-passing child
// directories.
func (d *clickHouseDatabase) DirsHaveChildren(
	dirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	result := newDirsHaveChildrenResult(dirs)

	remaining, err := d.addActiveVirtualDirsHaveChildren(result, dirs, filter)
	if err != nil {
		return nil, err
	}

	fallback, err := d.addBatchedDirsHaveChildren(result, remaining, filter)
	if err != nil {
		return nil, err
	}

	for _, dir := range fallback {
		result[dir] = d.dirHasChildrenSlow(dir, filter)
	}

	return result, nil
}

func newDirsHaveChildrenResult(dirs []string) map[string]bool {
	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = false
	}

	return result
}

func (d *clickHouseDatabase) addBatchedDirsHaveChildren(
	result map[string]bool,
	dirs []string,
	filter *db.Filter,
) ([]string, error) {
	roots, remaining, err := d.splitActiveMountRootDirs(dirs)
	if err != nil {
		return nil, err
	}

	if addErr := d.addActiveMountRootDirsHaveChildren(result, roots, filter); addErr != nil {
		return nil, addErr
	}

	groups, fallback, err := d.groupDirsByActiveMount(remaining)
	if err != nil {
		return nil, err
	}

	if groupErr := d.addDirsHaveChildrenGroups(result, groups, filter); groupErr != nil {
		return nil, groupErr
	}

	unhandledFallback, err := d.addTreeSummaryDirsHaveChildren(result, fallback, filter)
	if err != nil {
		return nil, err
	}

	return unhandledFallback, nil
}

func (d *clickHouseDatabase) addDirsHaveChildrenGroups(
	result map[string]bool,
	groups map[string]*activeMountDirGroup,
	filter *db.Filter,
) error {
	for _, group := range groups {
		if err := d.addDirsHaveChildrenForMount(result, group, filter); err != nil {
			return err
		}
	}

	return nil
}

func (d *clickHouseDatabase) addActiveMountRootDirsHaveChildren(
	result map[string]bool,
	roots activeMountRootDirs,
	filter *db.Filter,
) error {
	mounts, err := d.readyActiveRootMounts(roots)
	if err != nil || len(mounts) == 0 {
		return err
	}

	handled, err := d.addIndexedActiveMountRootDirsHaveChildren(result, roots, mounts, filter)
	if err != nil || handled {
		return err
	}

	return d.addFallbackActiveMountRootDirsHaveChildren(result, roots, mounts, filter)
}

func (d *clickHouseDatabase) readyActiveRootMounts(
	roots activeMountRootDirs,
) ([]activeMount, error) {
	if len(roots.mounts) == 0 {
		return nil, nil
	}

	return d.readyActiveMounts(roots.mounts)
}

func (d *clickHouseDatabase) parentActiveMountRootsWithChildren(
	mounts []activeMount,
	filter *db.Filter,
	childrenByParent map[string][]string,
) (map[string]bool, error) {
	if broadFilterCanUseChildRows(filter) {
		dirs := activeMountRootDirsList(mounts)

		return parentDirSet(parentDirsWithAnyChildren(dirs, childrenByParent)), nil
	}

	return d.parentActiveMountRootsWithFilteredChildren(mounts, filter, childrenByParent)
}

func activeMountRootDirsList(mounts []activeMount) []string {
	dirs := make([]string, len(mounts))
	for i, mount := range mounts {
		dirs[i] = mount.mountPath
	}

	return dirs
}

func (d *clickHouseDatabase) parentActiveMountRootsWithFilteredChildren(
	mounts []activeMount,
	filter *db.Filter,
	childrenByParent map[string][]string,
) (map[string]bool, error) {
	childParents, childMounts, childDirs := collectActiveMountRootChildParents(mounts, childrenByParent)
	if len(childDirs) == 0 {
		return map[string]bool{}, nil
	}

	childSummaries, err := d.activeMountChildSummaries(childDirs, childMounts, filter)
	if err != nil {
		return nil, err
	}

	return parentDirsWithMatchingChildSummaries(childParents, childSummaries), nil
}

func collectActiveMountRootChildParents(
	mounts []activeMount,
	childrenByParent map[string][]string,
) (map[string][]string, map[string]activeMount, []string) {
	childParents := make(map[string][]string)
	childMounts := make(map[string]activeMount)
	childDirs := make([]string, 0)

	for _, mount := range mounts {
		for _, child := range childrenByParent[mount.mountPath] {
			childDir := ensureTrailingSlash(child)
			if _, exists := childParents[childDir]; !exists {
				childDirs = append(childDirs, childDir)
				childMounts[childDir] = mount
			}

			childParents[childDir] = append(childParents[childDir], mount.mountPath)
		}
	}

	return childParents, childMounts, childDirs
}

func (d *clickHouseDatabase) activeMountChildSummaries(
	childDirs []string,
	childMounts map[string]activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	if mountDirDGUTAVectorCanHandleFilter(filter) {
		return d.activeMountChildSummariesFromDirInfos(childDirs, filter)
	}

	gutasByDir, err := d.gutasForActiveMountDirs(childDirs, childMounts)
	if err != nil {
		return nil, err
	}

	summaries := make(map[string]*db.DirSummary, len(gutasByDir))
	for _, childDir := range childDirs {
		gutas := gutasByDir[childDir]
		if len(gutas) == 0 {
			continue
		}

		mount := childMounts[childDir]
		sum := dirSummaryWithModtime(gutas, filter, mount.updatedAt)
		d.cacheActiveMountChildSummary(mount, childDir, filter, sum)

		if sum != nil {
			summaries[childDir] = sum
		}
	}

	return summaries, nil
}

func (d *clickHouseDatabase) activeMountChildSummariesFromDirInfos(
	childDirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	childSummaries, err := d.DirInfos(childDirs, filter)
	if err != nil {
		return nil, err
	}

	summaries := make(map[string]*db.DirSummary, len(childSummaries))
	for _, childDir := range childDirs {
		sum := childSummaries[childDir]
		if sum != nil {
			summaries[childDir] = sum
		}
	}

	return summaries, nil
}

func (d *clickHouseDatabase) cacheActiveMountChildSummary(
	mount activeMount,
	childDir string,
	filter *db.Filter,
	sum *db.DirSummary,
) {
	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return
	}

	d.treeCache.putDirSummary(
		newTreeDirSummaryCacheKey(mount.mountPath, mount.snapshotID, childDir, filter.Age, mode),
		sum,
	)
}

func (d *clickHouseDatabase) addTreeSummaryDirsHaveChildren(
	result map[string]bool,
	dirs []string,
	filter *db.Filter,
) ([]string, error) {
	if len(dirs) == 0 {
		return nil, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	hasChildren, ok, err := d.treeSummaryDirsHaveChildren(ctx, dirs, filter)
	if err != nil || !ok {
		return dirs, err
	}

	for dir, has := range hasChildren {
		result[dir] = has
	}

	if err := d.addVirtualAncestorDirsHaveChildren(result, dirs, filter); err != nil {
		return nil, err
	}

	return nil, nil
}

func (d *clickHouseDatabase) addVirtualAncestorDirsHaveChildren(
	result map[string]bool,
	dirs []string,
	filter *db.Filter,
) error {
	for _, dir := range dirs {
		if result[dir] {
			continue
		}

		hasChildren, err := d.virtualAncestorHasChildren(dir, filter)
		if err != nil {
			return err
		}

		result[dir] = hasChildren
	}

	return nil
}

func (d *clickHouseDatabase) virtualAncestorHasChildren(
	dir string,
	filter *db.Filter,
) (bool, error) {
	children, err := d.childrenForVirtualAncestor(dir)
	if err != nil || len(children) == 0 {
		return false, err
	}

	for _, child := range children {
		hasSummary, err := d.virtualChildHasSummary(child, filter)
		if err != nil {
			return false, err
		}

		if hasSummary {
			return true, nil
		}
	}

	return false, nil
}

func (d *clickHouseDatabase) virtualChildHasSummary(
	child string,
	filter *db.Filter,
) (bool, error) {
	sum, err := d.dirInfoAllowVirtualAncestor(child, filter)
	if errors.Is(err, db.ErrDirNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return sum != nil && sum.Count > 0, nil
}

func (d *clickHouseDatabase) childrenForMount(mountPath, snapshotID, parentDir string) ([]string, error) {
	if children, handled, err := d.navIndexChildrenForMount(mountPath, snapshotID, parentDir); handled {
		return children, err
	}

	key := newTreeCacheKey(mountPath, snapshotID, parentDir)
	if children, ok := d.treeCache.getChildren(key); ok {
		return children, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	parentID, ok, err := d.resolveDirIDForMount(ctx, mountPath, snapshotID, parentDir)
	if err != nil || !ok {
		return nil, err
	}

	children, err := d.queryChildren(ctx, childrenForDirIDQuery, "children", mountPath, snapshotID, parentID)
	if err != nil {
		return nil, err
	}

	d.treeCache.putChildren(key, children)

	return cloneStrings(children), nil
}

func (d *clickHouseDatabase) childrenForActiveMountRoots(
	mounts []activeMount,
) (map[string][]string, error) {
	if len(mounts) == 0 {
		return map[string][]string{}, nil
	}

	result, missing := d.cachedChildrenForActiveMountRoots(mounts)
	if len(missing) == 0 {
		return result, nil
	}

	queried, err := d.queryChildrenForActiveMountRoots(missing)
	if err != nil {
		return nil, err
	}

	d.addQueriedChildrenForActiveMountRoots(result, missing, queried)

	return result, nil
}

func (d *clickHouseDatabase) cachedChildrenForActiveMountRoots(
	mounts []activeMount,
) (map[string][]string, []activeMount) {
	result := make(map[string][]string, len(mounts))
	missing := make([]activeMount, 0, len(mounts))

	for _, mount := range mounts {
		key := newTreeCacheKey(mount.mountPath, mount.snapshotID, mount.mountPath)
		if d.addCachedChildrenForParent(result, key) {
			continue
		}

		missing = append(missing, mount)
	}

	return result, missing
}

func (d *clickHouseDatabase) queryChildrenForActiveMountRoots(
	mounts []activeMount,
) (map[string][]string, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := activeMountsQuery(
		activeMountRootChildrenQuery,
		"c.mount_path",
		"c.snapshot_id",
		mounts,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active mount root children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanChildrenRowsByParent(rows)
}

func (d *clickHouseDatabase) addQueriedChildrenForActiveMountRoots(
	result map[string][]string,
	missing []activeMount,
	queried map[string][]string,
) {
	for _, mount := range missing {
		children := queried[mount.mountPath]
		d.treeCache.putChildren(newTreeCacheKey(mount.mountPath, mount.snapshotID, mount.mountPath), children)
		result[mount.mountPath] = cloneStrings(children)
	}
}

func (d *clickHouseDatabase) splitActiveMountRootDirs(
	dirs []string,
) (activeMountRootDirs, []string, error) {
	roots := activeMountRootDirs{
		originalDirs: make(map[string][]string),
	}
	remaining := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		mount, ok, err := d.activeMountRootForDir(dir)
		if err != nil {
			return activeMountRootDirs{}, nil, err
		}

		if !ok {
			remaining = append(remaining, dir)

			continue
		}

		roots.originalDirs[mount.mountPath] = append(roots.originalDirs[mount.mountPath], dir)
		if seen[mount.mountPath] {
			continue
		}

		roots.mounts = append(roots.mounts, mount)
		seen[mount.mountPath] = true
	}

	return roots, remaining, nil
}

func (d *clickHouseDatabase) activeMountRootForDir(dir string) (activeMount, bool, error) {
	queryDir := ensureTrailingSlash(dir)
	if d.snapshot != nil {
		mount, ok := d.snapshot.mount(queryDir)

		return mount, ok, nil
	}

	return d.activeMountForMountPath(queryDir)
}

func (d *clickHouseDatabase) addActiveMountRootDirInfos(
	result map[string]*db.DirSummary,
	roots activeMountRootDirs,
	filter *db.Filter,
) error {
	mounts, err := d.readyActiveRootMounts(roots)
	if err != nil || len(mounts) == 0 {
		return err
	}

	summaries, err := d.activeMountRootSummaries(mounts, filter)
	if err != nil {
		return err
	}

	for _, mount := range mounts {
		sum := summaries[mount.mountPath]
		if sum == nil {
			continue
		}

		for _, original := range roots.originalDirs[mount.mountPath] {
			cp := cloneDirSummary(sum)
			cp.Dir = original
			result[original] = cp
		}
	}

	return nil
}

func (d *clickHouseDatabase) activeMountRootSummaries(
	mounts []activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	gutasByRoot, err := d.gutasForActiveMountRootDirs(mounts)
	if err != nil {
		return nil, err
	}

	summaries := make(map[string]*db.DirSummary, len(gutasByRoot))
	for _, mount := range mounts {
		gutas := gutasByRoot[mount.mountPath]
		if len(gutas) == 0 {
			continue
		}

		sum := dirSummaryWithModtime(gutas, filter, mount.updatedAt)
		d.cacheActiveMountRootSummary(mount, filter, sum)

		if sum != nil {
			summaries[mount.mountPath] = sum
		}
	}

	return summaries, nil
}

func (d *clickHouseDatabase) cacheActiveMountRootSummary(
	mount activeMount,
	filter *db.Filter,
	sum *db.DirSummary,
) {
	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return
	}

	d.treeCache.putDirSummary(
		newTreeDirSummaryCacheKey(mount.mountPath, mount.snapshotID, mount.mountPath, filter.Age, mode),
		sum,
	)
}

func (d *clickHouseDatabase) addDirInfoGroups(
	result map[string]*db.DirSummary,
	groups map[string]*activeMountDirGroup,
	filter *db.Filter,
) error {
	for _, group := range groups {
		if err := d.addDirInfosForMount(result, group, filter); err != nil {
			return err
		}
	}

	return nil
}

func (d *clickHouseDatabase) addFallbackDirInfos(
	result map[string]*db.DirSummary,
	dirs []string,
	filter *db.Filter,
) error {
	for _, dir := range dirs {
		sum, err := d.dirInfoAllowVirtualAncestor(dir, filter)
		if err != nil {
			return err
		}

		if sum == nil {
			continue
		}

		sum.Dir = dir
		result[dir] = sum
	}

	return nil
}

func (d *clickHouseDatabase) groupDirsByActiveMount(
	dirs []string,
) (map[string]*activeMountDirGroup, []string, error) {
	groups := make(map[string]*activeMountDirGroup)
	fallback := make([]string, 0)

	for _, dir := range dirs {
		if err := d.addDirToActiveMountGroups(groups, &fallback, dir); err != nil {
			return nil, nil, err
		}
	}

	return groups, fallback, nil
}

func (d *clickHouseDatabase) addDirToActiveMountGroups(
	groups map[string]*activeMountDirGroup,
	fallback *[]string,
	dir string,
) error {
	mountPath, ok, err := d.resolveMountScope(dir)
	if err != nil {
		return err
	}

	if !ok {
		*fallback = append(*fallback, dir)

		return nil
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return err
	}

	if !found {
		*fallback = append(*fallback, dir)

		return nil
	}

	return d.addReadyDirToActiveMountGroup(groups, mount, dir)
}

func (d *clickHouseDatabase) addReadyDirToActiveMountGroup(
	groups map[string]*activeMountDirGroup,
	mount activeMount,
	dir string,
) error {
	ready, err := d.activeMountReady(mount)
	if err != nil || !ready {
		return err
	}

	addDirToActiveMountGroup(groups, mount, dir)

	return nil
}

func addDirToActiveMountGroup(
	groups map[string]*activeMountDirGroup,
	mount activeMount,
	dir string,
) {
	group := activeMountGroup(groups, mount)

	queryDir := ensureTrailingSlash(dir)
	if _, exists := group.originalDirs[queryDir]; !exists {
		group.queryDirs = append(group.queryDirs, queryDir)
	}

	group.originalDirs[queryDir] = dir
}

func (d *clickHouseDatabase) addDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) error {
	if fullFilterAlwaysEmpty(filter) {
		return nil
	}

	handled, err := d.addDirFactInfosForMount(result, group, filter)
	if err != nil || handled {
		return err
	}

	return d.addDirInfosForMountFallback(result, group, filter)
}

func (d *clickHouseDatabase) addGroupedDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) (bool, error) {
	summaries, handled, ok, err := d.dirSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.mount.updatedAt,
		group.queryDirs,
		filter,
	)
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	d.addGroupedDirSummaries(result, group, summaries)

	return true, d.addMissingDirInfoSummaries(result, group, filter, handled)
}

func (d *clickHouseDatabase) addRawDirInfosForMount(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
) error {
	gutasByDir, err := d.gutasForDirs(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return err
	}

	d.addSummariesForGUTAs(result, group, filter, gutasByDir)

	return d.addMissingDirInfoSummaries(result, group, filter, handledGUTADirs(gutasByDir))
}

func handledGUTADirs(gutasByDir map[string]db.GUTAs) map[string]bool {
	handled := make(map[string]bool, len(gutasByDir))
	for dir := range gutasByDir {
		handled[dir] = true
	}

	return handled
}

func (d *clickHouseDatabase) addGroupedDirSummaries(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	summaries map[string]*db.DirSummary,
) {
	for queryDir, sum := range summaries {
		if sum == nil {
			continue
		}

		originalDir := group.originalDirs[queryDir]
		sum.Dir = originalDir
		result[originalDir] = sum
	}
}

func (d *clickHouseDatabase) addSummariesForGUTAs(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
	gutasByDir map[string]db.GUTAs,
) {
	for queryDir, gutas := range gutasByDir {
		sum := dirSummaryWithModtime(gutas, filter, group.mount.updatedAt)
		if sum == nil {
			continue
		}

		originalDir := group.originalDirs[queryDir]
		sum.Dir = originalDir
		result[originalDir] = sum
	}
}

func (d *clickHouseDatabase) addMissingDirInfoSummaries(
	result map[string]*db.DirSummary,
	group *activeMountDirGroup,
	filter *db.Filter,
	handled map[string]bool,
) error {
	for _, queryDir := range group.queryDirs {
		if handled[queryDir] {
			continue
		}

		originalDir := group.originalDirs[queryDir]

		sum, err := d.DirInfo(originalDir, filter)
		if errors.Is(err, db.ErrDirNotFound) {
			continue
		}

		if err != nil {
			return err
		}

		if sum != nil {
			sum.Dir = originalDir
			result[originalDir] = sum
		}
	}

	return nil
}

func (d *clickHouseDatabase) addDirsHaveChildrenForMount(
	result map[string]bool,
	group *activeMountDirGroup,
	filter *db.Filter,
) error {
	group, err := d.addIndexedDirsHaveChildrenForMount(result, group, filter)
	if err != nil || len(group.queryDirs) == 0 {
		return err
	}

	parentsWithChildren, err := d.parentDirsWithFilteredChildrenForMount(group, filter)
	if err != nil {
		return err
	}

	for _, queryDir := range group.queryDirs {
		if parentsWithChildren[queryDir] {
			result[group.originalDirs[queryDir]] = true
		}
	}

	return nil
}

func (d *clickHouseDatabase) parentDirsWithFilteredChildrenForMount(
	group *activeMountDirGroup,
	filter *db.Filter,
) (map[string]bool, error) {
	parents, ok, skipSummaryPrefetch := d.parentDirsWithMaintainedChildCountsForMount(group, filter)
	if ok {
		return parents, nil
	}

	childrenByParent, parentDirs, err := d.childRowsForDirsHaveChildren(group)
	if err != nil {
		return nil, err
	}

	if len(parentDirs) == 0 {
		return map[string]bool{}, nil
	}

	if broadFilterCanUseChildRows(filter) {
		d.prefetchBroadDirsHaveChildrenSummaries(group, filter, childrenByParent, skipSummaryPrefetch)

		return parentDirSet(parentDirs), nil
	}

	return d.parentDirsWithFilteredChildRows(group, filter, childrenByParent, parentDirs)
}

func broadFilterCanUseChildRows(filter *db.Filter) bool {
	return filter == nil ||
		(filter.GIDs == nil &&
			filter.UIDs == nil &&
			filter.FT == 0 &&
			filter.Age == db.DGUTAgeAll)
}

func parentDirSet(parentDirs []string) map[string]bool {
	set := make(map[string]bool, len(parentDirs))
	for _, parentDir := range parentDirs {
		set[parentDir] = true
	}

	return set
}

func (d *clickHouseDatabase) childRowsForDirsHaveChildren(
	group *activeMountDirGroup,
) (map[string][]string, []string, error) {
	childrenByParent, err := d.childrenForParentsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return nil, nil, err
	}

	return childrenByParent, parentDirsWithAnyChildren(group.queryDirs, childrenByParent), nil
}

func (d *clickHouseDatabase) prefetchBroadDirsHaveChildrenSummaries(
	group *activeMountDirGroup,
	filter *db.Filter,
	childrenByParent map[string][]string,
	skip bool,
) {
	if skip {
		return
	}

	ignoreDirsHaveChildrenPrefetchError(
		d.prefetchDirsHaveChildrenSummaries(group, filter, childrenByParent),
	)
}

func ignoreDirsHaveChildrenPrefetchError(error) {}

func (d *clickHouseDatabase) parentDirsWithMaintainedChildCountsForMount(
	group *activeMountDirGroup,
	filter *db.Filter,
) (map[string]bool, bool, bool) {
	if !broadFilterCanUseChildRows(filter) {
		return nil, false, false
	}

	if _, ok := mountDirSummaryModeForFilter(filter); !ok {
		return nil, false, false
	}

	_, handled, childCounts, ok, err := d.mountDirSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
		filter,
	)
	if err != nil || !ok {
		return nil, false, true
	}

	parents, ok := maintainedChildCountParents(group.queryDirs, handled, childCounts, filter)

	return parents, ok, ok
}

func maintainedChildCountParents(
	dirs []string,
	handled map[string]bool,
	childCounts map[string]uint64,
	filter *db.Filter,
) (map[string]bool, bool) {
	parents := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		childCount, ok := maintainedChildCountForDir(dir, handled, childCounts, filter)
		if !ok {
			return nil, false
		}

		if childCount > 0 {
			parents[dir] = true
		}
	}

	return parents, true
}

func (d *clickHouseDatabase) prefetchDirsHaveChildrenSummaries(
	group *activeMountDirGroup,
	filter *db.Filter,
	childrenByParent map[string][]string,
) error {
	if _, ok := mountDirSummaryModeForFilter(filter); !ok {
		return nil
	}

	childDirs, ok := boundedUniqueChildDirs(
		childrenByParent,
		dirsHaveChildrenSummaryPrefetchLimit,
	)
	if !ok || len(childDirs) == 0 {
		return nil
	}

	return d.cacheMountDirSummariesForDirsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		childDirs,
		filter,
	)
}

func boundedUniqueChildDirs(
	childrenByParent map[string][]string,
	limit int,
) ([]string, bool) {
	if limit <= 0 {
		return nil, false
	}

	childDirs := make([]string, 0, limit)
	seen := make(map[string]bool, limit)

	for _, children := range childrenByParent {
		for _, child := range children {
			if !addBoundedUniqueChildDir(&childDirs, seen, ensureTrailingSlash(child), limit) {
				return nil, false
			}
		}
	}

	return childDirs, true
}

func (d *clickHouseDatabase) cacheMountDirSummariesForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) error {
	summaries, handled, childCounts, ok, err := d.mountDirSummariesForDirsMount(
		mountPath,
		snapshotID,
		dirs,
		filter,
	)
	_ = summaries
	_ = handled
	_ = childCounts
	_ = ok

	return err
}

func (d *clickHouseDatabase) parentDirsWithFilteredChildRows(
	group *activeMountDirGroup,
	filter *db.Filter,
	childrenByParent map[string][]string,
	parentDirs []string,
) (map[string]bool, error) {
	childParents, childDirs := collectChildParents(childrenByParent)
	if len(childDirs) == 0 {
		return map[string]bool{}, nil
	}

	if len(childDirs) <= dirsHaveChildrenSummaryFanoutLimit {
		return d.parentDirsWithSummarizedChildren(childParents, childDirs, filter)
	}

	return d.parentDirsWithWideFilteredChildRows(group, filter, childParents, childDirs, parentDirs)
}

func collectChildParents(childrenByParent map[string][]string) (map[string][]string, []string) {
	childParents := make(map[string][]string)
	childDirs := make([]string, 0)

	for parent, children := range childrenByParent {
		for _, child := range children {
			if _, exists := childParents[child]; !exists {
				childDirs = append(childDirs, child)
			}

			childParents[child] = append(childParents[child], parent)
		}
	}

	return childParents, childDirs
}

func parentDirsWithAnyChildren(
	parentDirs []string,
	childrenByParent map[string][]string,
) []string {
	dirs := make([]string, 0, len(parentDirs))

	for _, parentDir := range parentDirs {
		if len(childrenByParent[parentDir]) == 0 {
			continue
		}

		dirs = append(dirs, parentDir)
	}

	return dirs
}

func (d *clickHouseDatabase) parentDirsWithWideFilteredChildRows(
	group *activeMountDirGroup,
	filter *db.Filter,
	childParents map[string][]string,
	childDirs []string,
	parentDirs []string,
) (map[string]bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	if parents, ok, err := d.parentDirsWithAgeAllFilteredChildRows(ctx, group, parentDirs, filter); err != nil || ok {
		return parents, err
	}

	if ready, err := d.mountDirDGUTAVectorsReadyForFilter(group, filter); err != nil || ready {
		if err != nil {
			return nil, err
		}

		return d.parentDirsWithSummarizedChildren(childParents, childDirs, filter)
	}

	return d.parentDirsWithMatchingChildrenMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		parentDirs,
		filter,
	)
}

func (d *clickHouseDatabase) mountDirDGUTAVectorsReadyForFilter(
	group *activeMountDirGroup,
	filter *db.Filter,
) (bool, error) {
	if !mountDirDGUTAVectorCanHandleFilter(filter) {
		return false, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.mountDirDGUTAVectorReadyCached(ctx, group.mount.mountPath, group.mount.snapshotID)
}

func (d *clickHouseDatabase) parentDirsWithSummarizedChildren(
	childParents map[string][]string,
	childDirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	childSummaries, err := d.DirInfos(childDirs, filter)
	if err != nil {
		return nil, err
	}

	return parentDirsWithMatchingChildSummaries(childParents, childSummaries), nil
}

func parentDirsWithMatchingChildSummaries(
	childParents map[string][]string,
	childSummaries map[string]*db.DirSummary,
) map[string]bool {
	parentDirs := make(map[string]bool)

	for child, summary := range childSummaries {
		if summary == nil || summary.Count == 0 {
			continue
		}

		for _, parent := range childParents[child] {
			parentDirs[parent] = true
		}
	}

	return parentDirs
}

func (d *clickHouseDatabase) dirHasChildrenSlow(dir string, filter *db.Filter) bool {
	children, err := d.Children(dir)
	if err != nil {
		return false
	}

	for _, child := range children {
		ds, _ := d.dirInfoAllowVirtualAncestor(child, filter) //nolint:errcheck
		if ds != nil && ds.Count > 0 {
			return true
		}
	}

	return false
}

func (d *clickHouseDatabase) resolveMountScope(dir string) (string, bool, error) {
	if d.mountPointsErr != nil {
		return "", false, d.mountPointsErr
	}

	normDir := ensureTrailingSlash(dir)

	mountPath, ok, err := d.resolveConfiguredMountScope(normDir)
	if err != nil {
		return "", false, err
	}

	if ok {
		return mountPath, true, nil
	}

	if d.hasNestedMountPoint(normDir) {
		return "", false, nil
	}

	mount, ok, err := d.activeMountForDir(normDir)
	if err != nil {
		return "", false, err
	}

	if !ok {
		return "", false, nil
	}

	return mount.mountPath, true, nil
}

func (d *clickHouseDatabase) resolveConfiguredMountScope(dir string) (string, bool, error) {
	mountPath := d.mountPoints.PrefixOf(dir)
	if mountPath == "" || mountPath == "/" {
		return "", false, nil
	}

	mount, ok, err := d.activeMountForDir(dir)
	if err != nil {
		return "", false, err
	}

	if ok {
		return mount.mountPath, true, nil
	}

	return mountPath, true, nil
}

func (d *clickHouseDatabase) hasNestedMountPoint(dir string) bool {
	for _, mountPath := range d.mountPoints {
		if mountPath == dir {
			continue
		}

		if strings.HasPrefix(mountPath, dir) {
			return true
		}
	}

	return false
}

func (d *clickHouseDatabase) activeMountForMountPath(mountPath string) (activeMount, bool, error) {
	if d.snapshot != nil {
		mount, ok := d.snapshot.mount(mountPath)

		return mount, ok, nil
	}

	return d.queryActiveMount(resolveExactMountQuery, ensureTrailingSlash(mountPath))
}

func (d *clickHouseDatabase) activeMountForDir(dir string) (activeMount, bool, error) {
	if d.snapshot != nil {
		mount, ok := d.snapshot.resolve(dir)

		return mount, ok, nil
	}

	return d.queryActiveMount(resolveMountQuery, ensureTrailingSlash(dir))
}

func rowIterationErr(rows any, msg string) error {
	if err := rowsErr(rows); err != nil {
		return fmt.Errorf("%s: %w", msg, err)
	}

	return nil
}

func (d *clickHouseDatabase) childrenForAncestor(
	parentDir string,
) ([]string, error) {
	children, ok, err := d.childrenForTreeSummaryAncestor(parentDir)
	if err != nil || (ok && len(children) > 0) {
		return children, err
	}

	if d.snapshot != nil {
		return d.snapshotOrVirtualChildrenForAncestor(parentDir)
	}

	return d.queryOrVirtualChildrenForAncestor(parentDir)
}

func (d *clickHouseDatabase) queryOrVirtualChildrenForAncestor(parentDir string) ([]string, error) {
	return d.readyChildrenForAncestor(parentDir)
}

func (d *clickHouseDatabase) snapshotOrVirtualChildrenForAncestor(parentDir string) ([]string, error) {
	return d.readyChildrenForAncestor(parentDir)
}

func (d *clickHouseDatabase) readyChildrenForAncestor(parentDir string) ([]string, error) {
	mounts, err := d.readyActiveMountsUnder(parentDir)
	if err != nil {
		return nil, err
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	virtualChildren, err := d.virtualChildrenForReadyAncestorMounts(ctx, parentDir, mounts)
	if err != nil {
		return nil, err
	}

	coveringChildren, err := d.childrenForCoveringActiveMount(parentDir)
	if err != nil {
		return nil, err
	}

	return mergeChildren(coveringChildren, virtualChildren), nil
}

func mergeChildren(childLists ...[]string) []string {
	seen := make(map[string]bool)
	children := make([]string, 0)

	for _, childList := range childLists {
		for _, child := range childList {
			if seen[child] {
				continue
			}

			seen[child] = true
			children = append(children, child)
		}
	}

	sort.Strings(children)

	if len(children) == 0 {
		return nil
	}

	return children
}

func (d *clickHouseDatabase) virtualChildrenForReadyAncestorMounts(
	ctx context.Context,
	parentDir string,
	mounts []activeMount,
) ([]string, error) {
	if len(mounts) == 0 {
		return nil, nil
	}

	children, ok, err := d.virtualChildrenForAncestor(ctx, parentDir, mounts)
	if err != nil || ok {
		return children, err
	}

	return immediateChildrenForMounts(parentDir, mounts), nil
}

func (d *clickHouseDatabase) childrenForCoveringActiveMount(parentDir string) ([]string, error) {
	if d.conn == nil && d.snapshot == nil {
		return nil, nil
	}

	mount, found, err := d.activeMountForDir(parentDir)
	if err != nil || !found {
		return nil, err
	}

	return d.childrenForReadyActiveMount(mount, parentDir)
}

func (d *clickHouseDatabase) childrenForVirtualAncestor(parentDir string) ([]string, error) {
	mounts, err := d.readyActiveMountsUnder(parentDir)
	if err != nil || len(mounts) == 0 {
		return nil, err
	}

	return immediateChildrenForMounts(parentDir, mounts), nil
}

func immediateChildrenForMounts(parentDir string, mounts []activeMount) []string {
	seen := make(map[string]bool)
	children := make([]string, 0, len(mounts))

	for _, mount := range mounts {
		child, ok := immediateChildForMount(parentDir, mount.mountPath)
		if !ok || seen[child] {
			continue
		}

		seen[child] = true
		children = append(children, child)
	}

	sort.Strings(children)

	return children
}

func (d *clickHouseDatabase) activeMountsUnder(dir string) ([]activeMount, error) {
	if d.snapshot != nil {
		return d.snapshot.under(dir), nil
	}

	if d.conn == nil {
		return nil, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	rows, err := queryMountsActiveRows(ctx, d.conn)
	if err != nil {
		return nil, err
	}

	return newActiveMountsSnapshot(rows).under(dir), nil
}

func (d *clickHouseDatabase) childrenForTreeSummaryAncestor(parentDir string) ([]string, bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.treeSummaryChildren(ctx, parentDir)
}

func (d *clickHouseDatabase) queryChildren(
	ctx context.Context,
	query string,
	what string,
	args ...any,
) ([]string, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s: %w", what, err)
	}

	defer func() { _ = rows.Close() }()

	children, err := scanChildrenRows(rows)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, nil
	}

	return children, nil
}

func scanChildrenRows(rows rowsScanner) ([]string, error) {
	children := make([]string, 0, childrenInitialCap)

	for rows.Next() {
		var child string
		if err := rows.Scan(&child); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan child: %w", err)
		}

		children = append(children, child)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: children iteration error: %w", err)
	}

	return children, nil
}

func (d *clickHouseDatabase) Info() (*db.Info, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	numDirs, numDGUTAs, err := d.infoDGUTACounts(ctx)
	if err != nil {
		return nil, err
	}

	numParents, numChildren, err := d.infoChildrenCounts(ctx)
	if err != nil {
		return nil, err
	}

	info, err := makeDBInfo(numDirs, numDGUTAs, numParents, numChildren)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func makeDBInfo(numDirs, numDGUTAs, numParents, numChildren uint64) (*db.Info, error) {
	dirs, err := infoCountToInt("num_dirs", numDirs)
	if err != nil {
		return nil, err
	}

	dgutas, err := infoCountToInt("num_dgutas", numDGUTAs)
	if err != nil {
		return nil, err
	}

	parents, err := infoCountToInt("num_parents", numParents)
	if err != nil {
		return nil, err
	}

	children, err := infoCountToInt("num_children", numChildren)
	if err != nil {
		return nil, err
	}

	return &db.Info{
		NumDirs:     dirs,
		NumDGUTAs:   dgutas,
		NumParents:  parents,
		NumChildren: children,
	}, nil
}

func (d *clickHouseDatabase) queryGUTAs(
	ctx context.Context,
	what string,
	query string,
	args ...any,
) (db.GUTAs, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s: %w", what, err)
	}

	d.treeCache.factVectorReads.Add(1)

	defer func() { _ = rows.Close() }()

	return scanDGUTARows(rows)
}

func scanDGUTARows(rows rowsScanner) (db.GUTAs, error) {
	gutas := make(db.GUTAs, 0, dgutaInitialCap)

	for rows.Next() {
		g, err := scanDGUTARow(rows)
		if err != nil {
			return nil, err
		}

		gutas = append(gutas, g)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: dguta iteration error: %w", err)
	}

	return gutas, nil
}

func (d *clickHouseDatabase) infoDGUTACounts(ctx context.Context) (uint64, uint64, error) {
	if d.snapshot == nil {
		return d.queryInfoCounts(ctx, infoDGUTAQuery, "dguta")
	}

	return d.infoCountsForSnapshot(
		ctx,
		infoDGUTASnapshotQuery,
		"dguta",
		d.snapshot.all(),
	)
}

func (d *clickHouseDatabase) infoChildrenCounts(ctx context.Context) (uint64, uint64, error) {
	if d.snapshot == nil {
		return d.queryInfoCounts(ctx, infoChildrenQuery, "children")
	}

	return d.infoCountsForSnapshot(
		ctx,
		infoChildrenSnapshotQuery,
		"children",
		d.snapshot.all(),
	)
}

func (d *clickHouseDatabase) queryInfoCounts(
	ctx context.Context,
	query, desc string,
	args ...any,
) (uint64, uint64, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("clickhouse: failed to query %s counts: %w", desc, err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		msg := fmt.Sprintf("clickhouse: %s counts iteration error", desc)
		if iterErr := rowIterationErr(rows, msg); iterErr != nil {
			return 0, 0, iterErr
		}

		return 0, 0, nil
	}

	var a, b uint64
	if err := rows.Scan(&a, &b); err != nil {
		return 0, 0, fmt.Errorf("clickhouse: failed to scan %s counts: %w", desc, err)
	}

	return a, b, nil
}

func (d *clickHouseDatabase) infoCountsForSnapshot(
	ctx context.Context,
	queryFmt, desc string,
	mounts []activeMount,
) (uint64, uint64, error) {
	if len(mounts) == 0 {
		return 0, 0, nil
	}

	query, args := activeMountsQuery(
		queryFmt,
		"mount_path",
		"snapshot_id",
		mounts,
	)

	return d.queryInfoCounts(ctx, query, desc, args...)
}

func (d *clickHouseDatabase) Close() error {
	if d == nil {
		return nil
	}

	d.closed.Store(true)
	d.navIndex.close()

	return nil
}

func (d *clickHouseDatabase) ensureOpen() error {
	if d == nil || d.closed.Load() {
		return errReaderClosed
	}

	return nil
}

func (d *clickHouseDatabase) gutasForDir(
	mountPath, snapshotID, dir string,
) (db.GUTAs, error) {
	key := newTreeCacheKey(mountPath, snapshotID, dir)
	if gutas, ok := d.treeCache.getGUTAs(key); ok {
		return gutas, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	gutas, err := d.queryGUTAs(ctx, "dguta", dgutaQuery, mountPath, snapshotID, dir)
	if err != nil {
		return nil, err
	}

	d.treeCache.putGUTAs(key, gutas)

	return cloneGUTAs(gutas), nil
}

func (d *clickHouseDatabase) gutasForActiveMountRootDirs(
	mounts []activeMount,
) (map[string]db.GUTAs, error) {
	if len(mounts) == 0 {
		return map[string]db.GUTAs{}, nil
	}

	result, missing := d.cachedGUTAsForActiveMountRoots(mounts)
	if len(missing) == 0 {
		return result, nil
	}

	queried, err := d.queryGUTAsForActiveMountRootDirs(missing)
	if err != nil {
		return nil, err
	}

	d.addQueriedGUTAsForActiveMountRoots(result, missing, queried)

	return result, nil
}

func (d *clickHouseDatabase) cachedGUTAsForActiveMountRoots(
	mounts []activeMount,
) (map[string]db.GUTAs, []activeMount) {
	result := make(map[string]db.GUTAs, len(mounts))
	missing := make([]activeMount, 0, len(mounts))

	for _, mount := range mounts {
		key := newTreeCacheKey(mount.mountPath, mount.snapshotID, mount.mountPath)

		gutas, ok := d.treeCache.getGUTAs(key)
		if !ok {
			missing = append(missing, mount)

			continue
		}

		if len(gutas) > 0 {
			result[mount.mountPath] = gutas
		}
	}

	return result, missing
}

func (d *clickHouseDatabase) queryGUTAsForActiveMountRootDirs(
	mounts []activeMount,
) (map[string]db.GUTAs, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := activeMountRootDirTuplesQuery(mounts)

	return d.queryMountRootDGUTAVectors(ctx, query, args...)
}

func (d *clickHouseDatabase) queryMountRootDGUTAVectors(
	ctx context.Context,
	query string,
	args ...any,
) (map[string]db.GUTAs, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query active mount root dguta vector batch: %w", err)
	}

	d.treeCache.factVectorReads.Add(1)

	defer func() { _ = rows.Close() }()

	return scanMountRootDGUTAVectorRows(rows)
}

func scanMountRootDGUTAVectorRows(rows rowsScanner) (map[string]db.GUTAs, error) {
	gutasByDir := make(map[string]db.GUTAs)

	for rows.Next() {
		dir, gutas, err := scanMountRootDGUTAVectorRow(rows)
		if err != nil {
			return nil, err
		}

		gutasByDir[dir] = append(gutasByDir[dir], gutas...)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: active mount root dguta vector iteration error: %w", err)
	}

	return gutasByDir, nil
}

func (d *clickHouseDatabase) addQueriedGUTAsForActiveMountRoots(
	result map[string]db.GUTAs,
	missing []activeMount,
	queried map[string]db.GUTAs,
) {
	for _, mount := range missing {
		gutas := queried[mount.mountPath]
		d.treeCache.putGUTAs(newTreeCacheKey(mount.mountPath, mount.snapshotID, mount.mountPath), gutas)

		if len(gutas) > 0 {
			result[mount.mountPath] = cloneGUTAs(gutas)
		}
	}
}

func (d *clickHouseDatabase) gutasForDirs(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	if len(dirs) == 0 {
		return map[string]db.GUTAs{}, nil
	}

	result, missing := d.cachedGUTAsForDirs(mountPath, snapshotID, dirs)
	if len(missing) == 0 {
		return result, nil
	}

	queried, err := d.queryGUTAsForDirs(mountPath, snapshotID, missing)
	if err != nil {
		return nil, err
	}

	d.addQueriedGUTAsForDirs(result, mountPath, snapshotID, missing, queried)

	return result, nil
}

func (d *clickHouseDatabase) gutasForActiveMountDirs(
	dirs []string,
	mountsByDir map[string]activeMount,
) (map[string]db.GUTAs, error) {
	if len(dirs) == 0 {
		return map[string]db.GUTAs{}, nil
	}

	result, missing := d.cachedGUTAsForActiveMountDirs(dirs, mountsByDir)
	if len(missing) == 0 {
		return result, nil
	}

	queried, err := d.queryGUTAsForActiveMountDirs(missing, mountsByDir)
	if err != nil {
		return nil, err
	}

	d.addQueriedGUTAsForActiveMountDirs(result, missing, mountsByDir, queried)

	return result, nil
}

func (d *clickHouseDatabase) cachedGUTAsForActiveMountDirs(
	dirs []string,
	mountsByDir map[string]activeMount,
) (map[string]db.GUTAs, []string) {
	result := make(map[string]db.GUTAs, len(dirs))
	missing := make([]string, 0, len(dirs))

	for _, dir := range dirs {
		mount := mountsByDir[dir]
		key := newTreeCacheKey(mount.mountPath, mount.snapshotID, dir)

		gutas, ok := d.treeCache.getGUTAs(key)
		if !ok {
			missing = append(missing, key.dir)

			continue
		}

		if len(gutas) > 0 {
			result[key.dir] = gutas
		}
	}

	return result, missing
}

func (d *clickHouseDatabase) queryGUTAsForActiveMountDirs(
	dirs []string,
	mountsByDir map[string]activeMount,
) (map[string]db.GUTAs, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	result := make(map[string]db.GUTAs)

	for _, batchDirs := range stringValueBatches(dirs) {
		queried, err := d.queryGUTAsForActiveMountDirsBatch(ctx, batchDirs, mountsByDir)
		if err != nil {
			return nil, err
		}

		for dir, gutas := range queried {
			result[dir] = gutas
		}
	}

	return result, nil
}

func (d *clickHouseDatabase) queryGUTAsForActiveMountDirsBatch(
	ctx context.Context,
	dirs []string,
	mountsByDir map[string]activeMount,
) (map[string]db.GUTAs, error) {
	query, args := activeMountDirTuplesQuery(dirs, mountsByDir)

	return d.queryGUTAsByDir(ctx, "active mount child dguta batch", query, args...)
}

func activeMountDirTuplesQuery(
	dirs []string,
	mountsByDir map[string]activeMount,
) (string, []any) {
	query := fmt.Sprintf(dgutasForActiveMountDirsQuery, activeMountDirTuplePlaceholders(len(dirs)))
	args := make([]any, 0, len(dirs)*activeMountDirTupleArgs)

	for _, dir := range dirs {
		mount := mountsByDir[dir]
		args = append(args, mount.mountPath, mount.snapshotID, dir)
	}

	return query, args
}

func (d *clickHouseDatabase) addQueriedGUTAsForActiveMountDirs(
	result map[string]db.GUTAs,
	missing []string,
	mountsByDir map[string]activeMount,
	queried map[string]db.GUTAs,
) {
	for _, dir := range missing {
		mount := mountsByDir[dir]
		gutas := queried[dir]
		d.treeCache.putGUTAs(newTreeCacheKey(mount.mountPath, mount.snapshotID, dir), gutas)

		if len(gutas) > 0 {
			result[dir] = cloneGUTAs(gutas)
		}
	}
}

func (d *clickHouseDatabase) cachedGUTAsForDirs(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, []string) {
	result := make(map[string]db.GUTAs, len(dirs))
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := newTreeCacheKey(mountPath, snapshotID, dir)
		if d.addCachedGUTAsForDir(result, key) || seen[key.dir] {
			continue
		}

		missing = append(missing, key.dir)
		seen[key.dir] = true
	}

	return result, missing
}

func (d *clickHouseDatabase) addCachedGUTAsForDir(
	result map[string]db.GUTAs,
	key treeCacheKey,
) bool {
	gutas, ok := d.treeCache.getGUTAs(key)
	if !ok {
		return false
	}

	if len(gutas) > 0 {
		result[key.dir] = gutas
	}

	return true
}

func (d *clickHouseDatabase) queryGUTAsForDirs(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	result := make(map[string]db.GUTAs)

	for _, batchDirs := range stringValueBatches(dirs) {
		queried, err := d.queryGUTAsForDirsBatch(ctx, mountPath, snapshotID, batchDirs)
		if err != nil {
			return nil, err
		}

		for dir, gutas := range queried {
			result[dir] = gutas
		}
	}

	return result, nil
}

func stringValueBatches(values []string) [][]string {
	if len(values) == 0 {
		return nil
	}

	if len(values) <= queryStringINMaxValues {
		return [][]string{values}
	}

	batches := make([][]string, 0, (len(values)+queryStringINMaxValues-1)/queryStringINMaxValues)
	for start := 0; start < len(values); start += queryStringINMaxValues {
		end := min(start+queryStringINMaxValues, len(values))
		batches = append(batches, values[start:end])
	}

	return batches
}

func (d *clickHouseDatabase) queryGUTAsForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	query, args := scopedBatchQuery(dgutasForDirsQuery, dirs, mountPath, snapshotID)

	return d.queryGUTAsByDir(ctx, "dguta batch", query, args...)
}

func scopedBatchQuery(queryFmt string, values []string, scopeArgs ...any) (string, []any) {
	query := fmt.Sprintf(queryFmt, placeholders(len(values)))
	args := make([]any, 0, len(values)+len(scopeArgs))
	args = append(args, scopeArgs...)

	for _, value := range values {
		args = append(args, value)
	}

	return query, args
}

func (d *clickHouseDatabase) queryGUTAsByDir(
	ctx context.Context,
	what string,
	query string,
	args ...any,
) (map[string]db.GUTAs, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s: %w", what, err)
	}

	d.treeCache.factVectorReads.Add(1)

	defer func() { _ = rows.Close() }()

	return scanDGUTARowsByDir(rows)
}

func scanDGUTARowsByDir(rows rowsScanner) (map[string]db.GUTAs, error) {
	gutasByDir := make(map[string]db.GUTAs)

	for rows.Next() {
		dir, g, err := scanDirDGUTARow(rows)
		if err != nil {
			return nil, err
		}

		gutasByDir[dir] = append(gutasByDir[dir], g)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: dguta batch iteration error: %w", err)
	}

	return gutasByDir, nil
}

func (d *clickHouseDatabase) dirSummaryForDirMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, bool, error) {
	summaries, handled, ok, err := d.dirSummariesForDirsMount(
		mountPath,
		snapshotID,
		updatedAt,
		[]string{ensureTrailingSlash(dir)},
		filter,
	)
	if err != nil || !ok {
		return nil, false, ok, err
	}

	queryDir := ensureTrailingSlash(dir)
	if !handled[queryDir] && !mountDirSummaryMissingMeansNotFound(filter) {
		return nil, false, false, nil
	}

	return summaries[queryDir], handled[queryDir], true, nil
}

func (d *clickHouseDatabase) dirSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if len(dirs) == 0 {
		return map[string]*db.DirSummary{}, map[string]bool{}, true, nil
	}

	return firstDirSummariesForDirsMount(
		func() (map[string]*db.DirSummary, map[string]bool, bool, error) {
			return d.maintainedDirSummariesForDirsMount(mountPath, snapshotID, dirs, filter)
		},
		func() (map[string]*db.DirSummary, map[string]bool, bool, error) {
			return d.dirFilterAllSummariesForDirsMount(mountPath, snapshotID, updatedAt, dirs, filter)
		},
		func() (map[string]*db.DirSummary, map[string]bool, bool, error) {
			return d.dirFilterAgeAllSummariesForDirsMount(mountPath, snapshotID, updatedAt, dirs, filter)
		},
		func() (map[string]*db.DirSummary, map[string]bool, bool, error) {
			return d.mountDirDGUTAVectorSummariesForDirsMount(mountPath, snapshotID, updatedAt, dirs, filter)
		},
		func() (map[string]*db.DirSummary, map[string]bool, bool, error) {
			return d.groupedDirSummariesForDirsMount(mountPath, snapshotID, updatedAt, dirs, filter)
		},
	)
}

func firstDirSummariesForDirsMount(
	loaders ...dirSummariesForDirsMountLoader,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	for _, load := range loaders {
		summaries, handled, ok, err := load()
		if err != nil || ok {
			return summaries, handled, ok, err
		}
	}

	return nil, nil, false, nil
}

func (d *clickHouseDatabase) maintainedDirSummariesForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	summaries, handled, _, ok, err := d.mountDirSummariesForDirsMount(
		mountPath,
		snapshotID,
		dirs,
		filter,
	)

	return summaries, handled, ok, err
}

func (d *clickHouseDatabase) groupedDirSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if !shouldUseGroupedDirSummaries(dirs) {
		return nil, nil, false, nil
	}

	return d.queryGroupedDirSummariesForDirsMount(mountPath, snapshotID, updatedAt, dirs, filter)
}

func shouldUseGroupedDirSummaries(dirs []string) bool {
	return len(dirs) >= groupedDirSummaryMinDirs
}

func (d *clickHouseDatabase) queryGroupedDirSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	query, args := dirSummariesForDirsMountQuery(mountPath, snapshotID, dirs, filter)

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, true, fmt.Errorf("clickhouse: failed to query dir summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, handled, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, handled, true, err
}

func dirSummariesForDirsMountQuery(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (string, []any) {
	filterExpr, filterArgs := dirSummaryFilterExpression(filter)

	query := fmt.Sprintf(
		dirSummariesForDirsQuery,
		filterExpr,
		placeholders(len(dirs)),
	)

	args := make([]any, 0, len(filterArgs)+queryScopeArgs+len(dirs))
	args = append(args, filterArgs...)
	args = append(args, mountPath, snapshotID)

	for _, dir := range dirs {
		args = append(args, dir)
	}

	return query, args
}

func scanDirSummaryRows(
	rows rowsScanner,
	filter *db.Filter,
	updatedAt time.Time,
) (map[string]*db.DirSummary, map[string]bool, error) {
	summaries := make(map[string]*db.DirSummary)
	handled := make(map[string]bool)

	for rows.Next() {
		var s dirSummaryScanned
		if err := s.scanFrom(rows); err != nil {
			return nil, nil, err
		}

		handled[s.dir] = true

		if sum := s.summary(filter, updatedAt); sum != nil {
			summaries[s.dir] = sum
		}
	}

	if err := rowsErr(rows); err != nil {
		return nil, nil, fmt.Errorf("clickhouse: dir summary iteration error: %w", err)
	}

	return summaries, handled, nil
}

func filteredMountWhereSummariesQueryForFilter(
	mountPath, snapshotID string,
	filter *db.Filter,
) (string, []any) {
	filterExpr, filterArgs := dirSummaryFilterExpression(filter)
	query := fmt.Sprintf(filteredMountWhereSummariesQuery, filterExpr)

	args := make([]any, 0, queryScopeArgs+len(filterArgs))
	args = append(args, mountPath, snapshotID)
	args = append(args, filterArgs...)

	return query, args
}

func (d *clickHouseDatabase) addQueriedGUTAsForDirs(
	result map[string]db.GUTAs,
	mountPath string,
	snapshotID string,
	missing []string,
	queried map[string]db.GUTAs,
) {
	for _, dir := range missing {
		gutas := queried[dir]
		d.treeCache.putGUTAs(newTreeCacheKey(mountPath, snapshotID, dir), gutas)

		if len(gutas) > 0 {
			result[dir] = cloneGUTAs(gutas)
		}
	}
}

func scanMountRootDGUTAVectorRow(rows rowsScanner) (string, db.GUTAs, error) {
	var s mountRootDGUTAVectorScanned
	if err := s.scanFrom(rows); err != nil {
		return "", nil, err
	}

	gutas, err := s.gutas()
	if err != nil {
		return "", nil, err
	}

	return s.dir, gutas, nil
}

type childFilterAllSummaryScanned struct {
	summary           dirSummaryScanned
	filterChildCount  uint64
	childCount        uint64
	hasFilterChildren uint8
	hasChildren       uint8
}

func (s *childFilterAllSummaryScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.summary.dir,
		&s.summary.rawRows,
		&s.summary.count,
		&s.summary.size,
		&s.summary.atimeMin,
		&s.summary.mtimeMax,
		&s.summary.atimeBuckets,
		&s.summary.mtimeBuckets,
		&s.summary.uids,
		&s.summary.gids,
		&s.summary.ft,
		&s.filterChildCount,
		&s.childCount,
		&s.hasFilterChildren,
		&s.hasChildren,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan filtered child summary packet: %w", err)
	}

	return nil
}

type mountRootDGUTAVectorScanned struct {
	dir    string
	vector dgutaVectorColumns
}

func (s *mountRootDGUTAVectorScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.dir,
		&s.vector.gids,
		&s.vector.uids,
		&s.vector.fts,
		&s.vector.ages,
		&s.vector.counts,
		&s.vector.sizes,
		&s.vector.atimeMins,
		&s.vector.mtimeMaxs,
		&s.vector.atimeBuckets,
		&s.vector.mtimeBuckets,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan active mount root dguta vector: %w", err)
	}

	return nil
}

func (s *mountRootDGUTAVectorScanned) gutas() (db.GUTAs, error) {
	return s.vector.gutas("mount", s.dir)
}

type dirSummariesForDirsMountLoader func() (map[string]*db.DirSummary, map[string]bool, bool, error)

func scanMaxUpdatedAt(rows rowsScanner) (time.Time, error) {
	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: ancestor max updated_at iteration error"); iterErr != nil {
			return time.Time{}, iterErr
		}

		return time.Time{}, nil
	}

	var updatedAt time.Time

	if err := rows.Scan(&updatedAt); err != nil {
		return time.Time{}, fmt.Errorf(
			"clickhouse: failed to scan ancestor max updated_at: %w",
			err,
		)
	}

	return updatedAt.UTC(), nil
}

func (d *clickHouseDatabase) childrenForParentsMount(
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, error) {
	if len(parentDirs) == 0 {
		return map[string][]string{}, nil
	}

	if children, handled, err := d.navIndexChildrenForParents(mountPath, snapshotID, parentDirs); handled {
		return children, err
	}

	result, missing := d.cachedChildrenForParents(mountPath, snapshotID, parentDirs)
	if len(missing) == 0 {
		return result, nil
	}

	queried, err := d.queryChildrenForParentsMount(mountPath, snapshotID, missing)
	if err != nil {
		return nil, err
	}

	d.addQueriedChildrenForParents(result, mountPath, snapshotID, missing, queried)

	return result, nil
}

func (d *clickHouseDatabase) cachedChildrenForParents(
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, []string) {
	result := make(map[string][]string, len(parentDirs))
	missing := make([]string, 0, len(parentDirs))
	seen := make(map[string]bool, len(parentDirs))

	for _, parentDir := range parentDirs {
		key := newTreeCacheKey(mountPath, snapshotID, parentDir)
		if d.addCachedChildrenForParent(result, key) || seen[key.dir] {
			continue
		}

		missing = append(missing, key.dir)
		seen[key.dir] = true
	}

	return result, missing
}

func (d *clickHouseDatabase) addCachedChildrenForParent(
	result map[string][]string,
	key treeCacheKey,
) bool {
	children, ok := d.treeCache.getChildren(key)
	if !ok {
		return false
	}

	result[key.dir] = children

	return true
}

func (d *clickHouseDatabase) queryChildrenForParentsMount(
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	if len(parentDirs) > queryStringINMaxValues {
		return d.queryChildrenForExternalParentsMount(ctx, mountPath, snapshotID, parentDirs)
	}

	result := make(map[string][]string)

	for _, batchDirs := range stringValueBatches(parentDirs) {
		queried, err := d.queryChildrenForParentsMountBatch(ctx, mountPath, snapshotID, batchDirs)
		if err != nil {
			return nil, err
		}

		for parent, children := range queried {
			result[parent] = children
		}
	}

	return result, nil
}

func (d *clickHouseDatabase) queryChildrenForExternalParentsMount(
	ctx context.Context,
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, error) {
	externalCtx, err := contextWithExternalDirs(ctx, parentDirs)
	if err != nil {
		return nil, err
	}

	rows, err := d.conn.Query(externalCtx, childrenForExternalParentsQuery, mountPath, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query external children batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanChildrenRowsByParent(rows)
}

func scanChildrenRowsByParent(rows rowsScanner) (map[string][]string, error) {
	children := make(map[string][]string)

	for rows.Next() {
		var parent, child string
		if err := rows.Scan(&parent, &child); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan child batch: %w", err)
		}

		children[parent] = append(children[parent], child)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: children batch iteration error: %w", err)
	}

	return children, nil
}

type whereTraversal struct {
	database *clickHouseDatabase
	filter   *db.Filter
	mount    *activeMount

	summaries      map[string]*db.DirSummary
	summaryLoaded  map[string]bool
	children       map[string][]string
	childrenLoaded map[string]bool
}

func (t *whereTraversal) where(dir string, recurseCount func(string) int) (db.DCSs, error) {
	var dcss db.DCSs

	frontier := []whereTraversalItem{{dir: dir}}
	for len(frontier) > 0 {
		infos, err := t.where0Batch(whereTraversalDirs(frontier))
		if err != nil {
			return nil, err
		}

		dcss = appendWhereSummaries(dcss, infos)
		frontier = nextWhereFrontier(frontier, infos, recurseCount)
	}

	return dcss, nil
}

func whereTraversalDirs(frontier []whereTraversalItem) []string {
	dirs := make([]string, len(frontier))
	for i, item := range frontier {
		dirs[i] = item.dir
	}

	return dirs
}

func appendWhereSummaries(dcss db.DCSs, infos []*db.DirInfo) db.DCSs {
	for _, di := range infos {
		if di != nil {
			dcss = append(dcss, di.Current)
		}
	}

	return dcss
}

func nextWhereFrontier(
	frontier []whereTraversalItem,
	infos []*db.DirInfo,
	recurseCount func(string) int,
) []whereTraversalItem {
	next := make([]whereTraversalItem, 0)

	for i, item := range frontier {
		di := infos[i]
		if di == nil || recurseCount(item.dir) <= item.step {
			continue
		}

		next = append(next, childWhereTraversalItems(di.Children, item.step+1)...)
	}

	return next
}

func (t *whereTraversal) where0Batch(dirs []string) ([]*db.DirInfo, error) {
	results := make([]*db.DirInfo, len(dirs))
	current := append([]string(nil), dirs...)

	pending := make([]bool, len(dirs))
	for i := range pending {
		pending[i] = true
	}

	for pendingCount(pending) > 0 {
		infos, err := t.dirInfos(currentDirs(current, pending))
		if err != nil {
			return nil, err
		}

		applyWhere0BatchInfos(current, pending, results, infos)
	}

	return results, nil
}

func pendingCount(pending []bool) int {
	count := 0

	for _, isPending := range pending {
		if isPending {
			count++
		}
	}

	return count
}

func currentDirs(current []string, pending []bool) []string {
	dirs := make([]string, 0, len(current))
	seen := make(map[string]bool, len(current))

	for i, dir := range current {
		if !pending[i] {
			continue
		}

		key := ensureTrailingSlash(dir)
		if seen[key] {
			continue
		}

		dirs = append(dirs, dir)
		seen[key] = true
	}

	return dirs
}

func applyWhere0BatchInfos(
	current []string,
	pending []bool,
	results []*db.DirInfo,
	infos map[string]*db.DirInfo,
) {
	for i, dir := range current {
		if !pending[i] {
			continue
		}

		applyWhere0Info(i, dir, current, pending, results, infos)
	}
}

func (t *whereTraversal) dirInfos(dirs []string) (map[string]*db.DirInfo, error) {
	keys := canonicalDirs(dirs)

	if err := t.loadChildren(keys); err != nil {
		return nil, err
	}

	if err := t.loadSummaries(t.summaryDirsForInfos(keys)); err != nil {
		return nil, err
	}

	result := make(map[string]*db.DirInfo, len(keys))
	for i, key := range keys {
		t.addDirInfo(result, key, dirs[i])
	}

	return result, nil
}

func canonicalDirs(dirs []string) []string {
	keys := make([]string, len(dirs))
	for i, dir := range dirs {
		keys[i] = ensureTrailingSlash(dir)
	}

	return keys
}

func (t *whereTraversal) addDirInfo(
	result map[string]*db.DirInfo,
	key string,
	displayDir string,
) {
	current := t.summaryForDir(key, displayDir)
	if current == nil {
		return
	}

	result[key] = &db.DirInfo{
		Current:  current,
		Children: t.childSummaries(key),
	}
}

func (t *whereTraversal) childSummaries(parent string) []*db.DirSummary {
	children := make([]*db.DirSummary, 0, len(t.children[parent]))

	for _, child := range t.children[parent] {
		summary := t.summaryForDir(child, whereDisplayDir(child))
		if summary != nil && summary.Count > 0 {
			children = append(children, summary)
		}
	}

	return children
}

func (t *whereTraversal) childSummaryDirs(parentDirs []string) []string {
	seen := make(map[string]bool)
	childDirs := make([]string, 0)

	for _, parent := range parentDirs {
		for _, child := range t.children[parent] {
			if seen[child] {
				continue
			}

			childDirs = append(childDirs, child)
			seen[child] = true
		}
	}

	return childDirs
}

func (t *whereTraversal) loadSummaries(dirs []string) error {
	missing := t.unloadedSummaries(dirs)
	if len(missing) == 0 {
		return nil
	}

	if t.mount != nil {
		return t.loadMountSummaries(missing)
	}

	summaries, err := t.database.DirInfos(missing, t.filter)
	if err != nil {
		return err
	}

	for _, dir := range missing {
		t.summaryLoaded[dir] = true
		if summary := summaries[dir]; summary != nil {
			t.summaries[dir] = summary
		}
	}

	return nil
}

func (t *whereTraversal) unloadedSummaries(dirs []string) []string {
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		if seen[dir] || t.summaryLoaded[dir] {
			continue
		}

		missing = append(missing, dir)
		seen[dir] = true
	}

	return missing
}

func (t *whereTraversal) loadMountSummaries(dirs []string) error {
	ok, err := t.loadGroupedMountSummaries(dirs)
	if err != nil || ok {
		return err
	}

	return t.loadRawMountSummaries(dirs)
}

func (t *whereTraversal) loadChildren(dirs []string) error {
	missing := t.unloadedChildren(dirs)
	if len(missing) == 0 {
		return nil
	}

	if t.mount != nil {
		return t.loadMountChildren(missing)
	}

	return t.loadFallbackChildren(missing)
}

func (t *whereTraversal) unloadedChildren(dirs []string) []string {
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		if seen[dir] || t.childrenLoaded[dir] {
			continue
		}

		missing = append(missing, dir)
		seen[dir] = true
	}

	return missing
}

func (t *whereTraversal) loadMountChildren(dirs []string) error {
	childrenByParent, err := t.database.childrenForParentsMount(
		t.mount.mountPath,
		t.mount.snapshotID,
		dirs,
	)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		t.children[dir] = canonicalSortedChildren(childrenByParent[dir])
		t.childrenLoaded[dir] = true
	}

	return nil
}

func canonicalSortedChildren(children []string) []string {
	if len(children) == 0 {
		return nil
	}

	out := make([]string, 0, len(children))
	seen := make(map[string]bool, len(children))

	for _, child := range children {
		key := ensureTrailingSlash(child)
		if seen[key] {
			continue
		}

		out = append(out, key)
		seen[key] = true
	}

	sort.Strings(out)

	return out
}

func (t *whereTraversal) loadFallbackChildren(dirs []string) error {
	groups, fallback, err := t.database.groupDirsByActiveMount(dirs)
	if err != nil {
		return err
	}

	if err := t.loadFallbackChildGroups(groups); err != nil {
		return err
	}

	if err := t.loadFallbackChildDirs(fallback); err != nil {
		return err
	}

	t.markChildrenLoaded(dirs)

	return nil
}

func (t *whereTraversal) loadFallbackChildGroups(
	groups map[string]*activeMountDirGroup,
) error {
	rootMounts := t.loadFallbackChildGroupRootMounts(groups)
	if err := t.loadActiveMountRootChildren(rootMounts); err != nil {
		return err
	}

	for _, group := range groups {
		group.queryDirs = nonActiveMountRootQueryDirs(group)
		if len(group.queryDirs) == 0 {
			continue
		}

		if err := t.loadGroupChildren(group); err != nil {
			return err
		}
	}

	return nil
}

func (t *whereTraversal) loadFallbackChildDirs(dirs []string) error {
	childrenByParent, handled, err := t.database.activeVirtualChildrenForParents(dirs)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if handled[ensureTrailingSlash(dir)] {
			t.storeChildren(dir, childrenByParent[ensureTrailingSlash(dir)])

			continue
		}

		children, err := t.database.childrenForTraversalFallback(dir)
		if err != nil {
			return err
		}

		t.storeChildren(dir, children)
	}

	return nil
}

func (t *whereTraversal) markChildrenLoaded(dirs []string) {
	for _, dir := range dirs {
		if t.childrenLoaded[dir] {
			continue
		}

		t.children[dir] = nil
		t.childrenLoaded[dir] = true
	}
}

func (t *whereTraversal) storeChildren(parent string, children []string) {
	key := ensureTrailingSlash(parent)
	t.children[key] = canonicalSortedChildren(children)
	t.childrenLoaded[key] = true
}

func (t *whereTraversal) loadGroupChildren(group *activeMountDirGroup) error {
	childrenByParent, err := t.database.childrenForParentsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return err
	}

	for _, dir := range group.queryDirs {
		t.children[dir] = canonicalSortedChildren(childrenByParent[dir])
		t.childrenLoaded[dir] = true
	}

	return nil
}

func (t *whereTraversal) summaryForDir(key, displayDir string) *db.DirSummary {
	summary := t.summaries[key]
	if summary == nil {
		return nil
	}

	cp := *summary
	cp.Dir = displayDir

	return &cp
}

func (d *clickHouseDatabase) childrenForTraversalFallback(dir string) ([]string, error) {
	children, err := d.Children(dir)
	if err != nil || len(children) > 0 {
		return children, err
	}

	virtualChildren, virtualErr := d.childrenForVirtualAncestor(dir)
	if virtualErr != nil || len(virtualChildren) > 0 {
		return virtualChildren, virtualErr
	}

	return children, nil
}

func (d *clickHouseDatabase) queryChildrenForParentsMountBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, error) {
	query, args := scopedBatchQuery(childrenForParentsQuery, parentDirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query children batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanChildrenRowsByParent(rows)
}

func (d *clickHouseDatabase) addQueriedChildrenForParents(
	result map[string][]string,
	mountPath string,
	snapshotID string,
	missing []string,
	queried map[string][]string,
) {
	for _, parentDir := range missing {
		children := queried[parentDir]
		d.treeCache.putChildren(newTreeCacheKey(mountPath, snapshotID, parentDir), children)
		result[parentDir] = cloneStrings(children)
	}
}

func (d *clickHouseDatabase) parentDirsWithMatchingChildrenMount(
	mountPath, snapshotID string,
	parentDirs []string,
	filter *db.Filter,
) (map[string]bool, error) {
	if len(parentDirs) == 0 {
		return map[string]bool{}, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := dirsHaveMatchingChildrenMountQuery(
		parentDirs,
		mountPath,
		snapshotID,
		filter,
	)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query matching child dirs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentDirSet(rows)
}

func dirsHaveMatchingChildrenMountQuery(
	parentDirs []string,
	mountPath, snapshotID string,
	filter *db.Filter,
) (string, []any) {
	filterClause, filterArgs := gutaExistenceFilterClause(filter, "d.")
	query := fmt.Sprintf(
		dirsHaveMatchingChildrenQuery,
		placeholders(len(parentDirs)),
		filterClause,
	)
	args := make([]any, 0, 2+len(parentDirs)+len(filterArgs))
	args = append(args, mountPath, snapshotID)

	for _, parentDir := range parentDirs {
		args = append(args, parentDir)
	}

	args = append(args, filterArgs...)

	return query, args
}

func scanParentDirSet(rows rowsScanner) (map[string]bool, error) {
	parents := make(map[string]bool)

	for rows.Next() {
		var parent string
		if err := rows.Scan(&parent); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan matching child parent: %w", err)
		}

		parents[parent] = true
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: matching child dirs iteration error: %w", err)
	}

	return parents, nil
}

func whereDisplayDir(dir string) string {
	if dir == "/" {
		return dir
	}

	return strings.TrimSuffix(dir, "/")
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
}

func scanDGUTARow(rows rowsScanner) (*db.GUTA, error) {
	var s dgutaScanned
	if err := s.scanFrom(rows); err != nil {
		return nil, err
	}

	return s.guta(), nil
}

func scanDirDGUTARow(rows rowsScanner) (string, *db.GUTA, error) {
	var (
		dir string
		s   dgutaScanned
	)

	if err := s.scanFromWithDir(rows, &dir); err != nil {
		return "", nil, err
	}

	return dir, s.guta(), nil
}

type dirSummaryScanned struct {
	dir          string
	rawRows      uint64
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

func (s *dirSummaryScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.dir,
		&s.rawRows,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
		&s.uids,
		&s.gids,
		&s.ft,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan dir summary: %w", err)
	}

	return nil
}

func (s *dirSummaryScanned) summary(filter *db.Filter, updatedAt time.Time) *db.DirSummary {
	if s.count == 0 {
		return nil
	}

	var age db.DirGUTAge
	if filter != nil {
		age = filter.Age
	}

	return &db.DirSummary{
		Count:       s.count,
		Size:        s.size,
		Atime:       time.Unix(s.atimeMin, 0),
		CommonATime: summary.MostCommonBucket(sliceToAgeBuckets(s.atimeBuckets)),
		Mtime:       time.Unix(s.mtimeMax, 0),
		CommonMTime: summary.MostCommonBucket(sliceToAgeBuckets(s.mtimeBuckets)),
		UIDs:        s.uids,
		GIDs:        s.gids,
		FT:          db.DirGUTAFileType(s.ft),
		Age:         age,
		Modtime:     updatedAt,
	}
}

func (s *dgutaScanned) guta() *db.GUTA {
	return &db.GUTA{
		GID:         s.gid,
		UID:         s.uid,
		FT:          db.DirGUTAFileType(s.ft),
		Age:         db.DirGUTAge(s.age),
		Count:       s.count,
		Size:        s.size,
		Atime:       s.atimeMin,
		ATimeRanges: sliceToAgeBuckets(s.atimeBuckets),
		Mtime:       s.mtimeMax,
		MTimeRanges: sliceToAgeBuckets(s.mtimeBuckets),
	}
}

func sliceToAgeBuckets(in []uint64) summary.AgeBuckets {
	var out summary.AgeBuckets

	for i := range min(len(out), len(in)) {
		out[i] = in[i]
	}

	return out
}

type rowsErrer interface {
	Err() error
}

func rowsErr(rows any) error {
	errer, ok := rows.(rowsErrer)
	if !ok {
		return nil
	}

	return errer.Err()
}

type dgutaScanned struct {
	gid          uint32
	uid          uint32
	ft           uint16
	age          uint8
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func (s *dgutaScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.gid,
		&s.uid,
		&s.ft,
		&s.age,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan dguta: %w", err)
	}

	return nil
}

func (s *dgutaScanned) scanFromWithDir(rows rowsScanner, dir *string) error {
	if err := rows.Scan(
		dir,
		&s.gid,
		&s.uid,
		&s.ft,
		&s.age,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan dguta batch: %w", err)
	}

	return nil
}

func maintainedChildCountForDir(
	dir string,
	handled map[string]bool,
	childCounts map[string]uint64,
	filter *db.Filter,
) (uint64, bool) {
	if !handled[dir] {
		return 0, mountDirSummaryMissingMeansNotFound(filter)
	}

	childCount, ok := childCounts[dir]

	return childCount, ok
}

func activeMountDirTuplePlaceholders(n int) string {
	var b strings.Builder

	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, toUUID(?), ?)")
	}

	return b.String()
}

func immediateChildForMount(parentDir, mountPath string) (string, bool) {
	parentDir = ensureTrailingSlash(parentDir)
	mountPath = ensureTrailingSlash(mountPath)

	if parentDir == mountPath || !strings.HasPrefix(mountPath, parentDir) {
		return "", false
	}

	relative := strings.TrimPrefix(mountPath, parentDir)

	part, _, _ := strings.Cut(relative, "/")
	if part == "" {
		return "", false
	}

	if parentDir == "/" {
		return "/" + part, true
	}

	return strings.TrimSuffix(parentDir, "/") + "/" + part, true
}

func addBoundedUniqueChildDir(
	childDirs *[]string,
	seen map[string]bool,
	dir string,
	limit int,
) bool {
	if seen[dir] {
		return true
	}

	if len(*childDirs) == limit {
		return false
	}

	seen[dir] = true
	*childDirs = append(*childDirs, dir)

	return true
}

func dirSummaryFilterExpression(filter *db.Filter) (string, []any) {
	if filter == nil {
		return "1", nil
	}

	if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return "0", nil
	}

	clauses := make([]string, 0, dirSummaryFilterClauseInitialCap)
	args := make([]any, 0, dirSummaryFilterArgCap(filter))

	appendIDMembershipClause(&clauses, &args, "gid", filter.GIDs)
	appendIDMembershipClause(&clauses, &args, "uid", filter.UIDs)
	appendFTMembershipClause(&clauses, &args, "ft", filter.FT)

	clauses = append(clauses, "age = ?")
	args = append(args, uint8(filter.Age))

	return strings.Join(clauses, " AND "), args
}

func isEmptyIDFilter(values []uint32) bool {
	return values != nil && len(values) == 0
}

func dirSummaryFilterArgCap(filter *db.Filter) int {
	capacity := len(filter.GIDs) + len(filter.UIDs) + 1
	if filter.FT != 0 {
		capacity++
	}

	return capacity
}

func appendIDMembershipClause(
	clauses *[]string,
	args *[]any,
	column string,
	values []uint32,
) {
	if values == nil {
		return
	}

	*clauses = append(*clauses, column+" IN ("+placeholders(len(values))+")")

	for _, value := range values {
		*args = append(*args, value)
	}
}

func appendFTMembershipClause(
	clauses *[]string,
	args *[]any,
	column string,
	ft db.DirGUTAFileType,
) {
	if ft == 0 {
		return
	}

	*clauses = append(*clauses, "bitAnd("+column+", ?) > 0")
	*args = append(*args, uint16(ft))
}

func gutaExistenceFilterClause(filter *db.Filter, columnPrefix string) (string, []any) {
	args := make([]any, 0, 1)
	conditions := make([]string, 0, dirSummaryFilterClauseInitialCap+1)

	conditions = append(conditions, "count > 0")

	if filter != nil {
		if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
			return " AND 0", nil
		}

		appendIDMembershipClause(&conditions, &args, "gid", filter.GIDs)
		appendIDMembershipClause(&conditions, &args, "uid", filter.UIDs)
		appendFTMembershipClause(&conditions, &args, "ft", filter.FT)
		conditions = append(conditions, "age = ?")
		args = append(args, uint8(filter.Age))
	}

	clause := " AND arrayExists((gid, uid, ft, age, count) -> " +
		strings.Join(conditions, " AND ") + ", " +
		columnPrefix + "gids, " + columnPrefix + "uids, " + columnPrefix + "fts, " +
		columnPrefix + "ages, " + columnPrefix + "counts)"

	return clause, args
}

func applyWhere0Info(
	i int,
	dir string,
	current []string,
	pending []bool,
	results []*db.DirInfo,
	infos map[string]*db.DirInfo,
) {
	di := infos[ensureTrailingSlash(dir)]
	if di == nil {
		pending[i] = false

		return
	}

	if di.IsSameAsChild() {
		current[i] = di.Children[0].Dir

		return
	}

	results[i] = di
	pending[i] = false
}

func placeholders(n int) string {
	var b strings.Builder

	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("?")
	}

	return b.String()
}

func infoCountToInt(name string, v uint64) (int, error) {
	out, err := safeUint64ToInt(v)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: invalid %s: %w", name, err)
	}

	return out, nil
}

func safeUint64ToInt(v uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if v > maxInt {
		return 0, fmt.Errorf("%w: %d", errIntOverflow, v)
	}

	return int(v), nil
}
