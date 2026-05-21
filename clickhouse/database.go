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
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	childrenInitialCap = 16

	dgutaInitialCap                    = 8
	dirsHaveChildrenSummaryFanoutLimit = 16
	queryScopeArgs                     = 2
	whereSingleMountBaseArgs           = 3
	whereAncestorBaseArgs              = 2

	childrenQuery = "SELECT DISTINCT child FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND parent_dir = ? " +
		"ORDER BY child"

	childrenAncestorQuery = "WITH active AS (" +
		"SELECT mount_path, snapshot_id " +
		"FROM wrstat_mounts_active " +
		"WHERE startsWith(mount_path, ?)" +
		") " +
		"SELECT DISTINCT c.child " +
		"FROM wrstat_children c " +
		"ANY INNER JOIN active a " +
		"ON c.mount_path = a.mount_path " +
		"AND c.snapshot_id = a.snapshot_id " +
		"WHERE c.parent_dir = ? " +
		"ORDER BY c.child ASC"

	dgutaQuery = "SELECT gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets " +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND dir = ?"

	dgutaAncestorQuery = "WITH active AS (" +
		"SELECT mount_path, snapshot_id " +
		"FROM wrstat_mounts_active " +
		"WHERE startsWith(mount_path, ?)" +
		") " +
		"SELECT d.gid, d.uid, d.ft, d.age, d.count, d.size, " +
		"d.atime_min, d.mtime_max, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dguta d " +
		"ANY INNER JOIN active a " +
		"ON d.mount_path = a.mount_path " +
		"AND d.snapshot_id = a.snapshot_id " +
		"WHERE d.dir = ?"

	ancestorMaxUpdatedAtQuery = "SELECT max(updated_at) " +
		"FROM wrstat_mounts_active " +
		"WHERE startsWith(mount_path, ?)"

	infoDGUTAQuery = "SELECT " +
		"uniqExact(dir) AS num_dirs, " +
		"count() AS num_dgutas " +
		"FROM wrstat_dguta " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	infoChildrenQuery = "SELECT " +
		"uniqExact(parent_dir) AS num_parents, " +
		"count() AS num_children " +
		"FROM wrstat_children " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	resolveMountQuery = "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active " +
		"WHERE startsWith(?, mount_path) " +
		"ORDER BY length(mount_path) DESC LIMIT 1"

	resolveExactMountQuery = "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active " +
		"WHERE mount_path = ? LIMIT 1"

	childrenAncestorSnapshotQuery = "SELECT DISTINCT c.child " +
		"FROM wrstat_children c " +
		"WHERE c.parent_dir = ? AND %s " +
		"ORDER BY c.child ASC"

	dgutaAncestorSnapshotQuery = "SELECT d.gid, d.uid, d.ft, d.age, d.count, d.size, " +
		"d.atime_min, d.mtime_max, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dguta d " +
		"WHERE d.dir = ? AND %s"

	dgutasForDirsQuery = "SELECT dir, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets " +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)"

	childrenForParentsQuery = "SELECT parent_dir, child " +
		"FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE parent_dir IN (%s) " +
		"ORDER BY parent_dir ASC, child ASC"

	dirsHaveMatchingChildrenQuery = "SELECT c.parent_dir " +
		"FROM wrstat_children c " +
		"INNER JOIN wrstat_dguta d " +
		"ON d.mount_path = c.mount_path " +
		"AND d.snapshot_id = c.snapshot_id " +
		"AND d.dir = if(endsWith(c.child, '/'), c.child, concat(c.child, '/')) " +
		"WHERE c.mount_path = ? AND c.snapshot_id = ? " +
		"AND c.parent_dir IN (%s) %s " +
		"GROUP BY c.parent_dir " +
		"ORDER BY c.parent_dir ASC"

	whereSubtreeSummarySelect = "SELECT dir, " +
		"arraySort(groupUniqArray(uid)) AS uids, " +
		"arraySort(groupUniqArray(gid)) AS gids, " +
		"toUInt16(groupBitOr(ft)) AS file_type, " +
		"sum(count) AS total_count, " +
		"sum(size) AS total_size, " +
		"minIf(atime_min, atime_min != 0) AS atime_min, " +
		"max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets "

	whereSubtreeSingleMountQuery = whereSubtreeSummarySelect +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE startsWith(dir, ?) %s " +
		"GROUP BY dir HAVING total_count > 0 ORDER BY dir ASC"

	whereSubtreeAncestorQuery = "WITH active AS (" +
		"SELECT mount_path, snapshot_id, updated_at " +
		"FROM wrstat_mounts_active " +
		"WHERE startsWith(mount_path, ?)" +
		") " +
		whereSubtreeSummarySelect +
		", max(a.updated_at) AS updated_at " +
		"FROM wrstat_dguta d " +
		"ANY INNER JOIN active a " +
		"ON d.mount_path = a.mount_path " +
		"AND d.snapshot_id = a.snapshot_id " +
		"WHERE startsWith(d.dir, ?) %s " +
		"GROUP BY d.dir HAVING total_count > 0 ORDER BY dir ASC"

	whereSubtreeAncestorSnapshotQuery = whereSubtreeSummarySelect +
		"FROM wrstat_dguta d " +
		"WHERE startsWith(d.dir, ?) AND %s %s " +
		"GROUP BY d.dir HAVING total_count > 0 ORDER BY dir ASC"

	infoDGUTASnapshotQuery = "SELECT " +
		"uniqExact(dir) AS num_dirs, " +
		"count() AS num_dgutas " +
		"FROM wrstat_dguta " +
		"WHERE %s"

	infoChildrenSnapshotQuery = "SELECT " +
		"uniqExact(parent_dir) AS num_parents, " +
		"count() AS num_children " +
		"FROM wrstat_children " +
		"WHERE %s"
)

var errIntOverflow = errors.New("value overflows int")

var errReaderClosed = errors.New("clickhouse: reader is closed")

type activeMountDirGroup struct {
	mount        activeMount
	originalDirs map[string]string
	queryDirs    []string
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

type whereTraversalItem struct {
	dir  string
	step int
}

func childWhereTraversalItems(children []*db.DirSummary, step int) []whereTraversalItem {
	items := make([]whereTraversalItem, len(children))
	for i, child := range children {
		items[i] = whereTraversalItem{dir: child.Dir, step: step}
	}

	return items
}

type clickHouseDatabase struct {
	cfg  Config
	conn ch.Conn

	mountPoints    basedirs.MountPoints
	mountPointsErr error

	snapshot *activeMountsSnapshot
	closed   atomic.Bool
}

func newClickHouseDatabase(cfg Config, conn ch.Conn) *clickHouseDatabase {
	return newClickHouseDatabaseWithSnapshot(cfg, conn, nil)
}

func newClickHouseDatabaseWithSnapshot(
	cfg Config,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) *clickHouseDatabase {
	mountPoints, err := mountPointsFromConfig(cfg)

	return &clickHouseDatabase{
		cfg:            cfg,
		conn:           conn,
		mountPoints:    mountPoints,
		mountPointsErr: err,
		snapshot:       snapshot,
	}
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
		return d.dirInfoAncestor(dir, filter)
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return &db.DirSummary{}, db.ErrDirNotFound
	}

	return d.dirInfoSingleMount(
		mount.mountPath, mount.snapshotID, mount.updatedAt, dir, filter,
	)
}

func (d *clickHouseDatabase) dirInfoSingleMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
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
	queryDir := ensureTrailingSlash(dir)

	if recurseCount(dir) <= 1 {
		return d.whereFromSubtree(dir, queryDir, filter, recurseCount)
	}

	traversal, fallbackToSubtree, err := d.whereTraversalFor(queryDir, filter)
	if err != nil {
		return nil, err
	}

	if fallbackToSubtree {
		return d.whereFromSubtree(dir, queryDir, filter, recurseCount)
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
) (*whereTraversal, bool, error) {
	mountPath, ok, err := d.resolveMountScope(queryDir)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, true, nil
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, false, err
	}

	traversal := newWhereTraversal(d, filter)
	if found {
		traversal.mount = &mount
	}

	return traversal, false, nil
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
		sum, err := d.DirInfo(dir, filter)
		if err != nil || sum == nil {
			return nil, err
		}
	}

	sort.Sort(dcss)

	return dcss, nil
}

func (d *clickHouseDatabase) whereFromSubtree(
	dir string,
	queryDir string,
	filter *db.Filter,
	recurseCount func(string) int,
) (db.DCSs, error) {
	summaries, err := d.whereSubtreeSummaries(queryDir, filter)
	if err != nil {
		return nil, err
	}

	if summaries[queryDir] == nil {
		sum, err := d.DirInfo(dir, filter)
		if err != nil || sum == nil {
			return nil, err
		}
	}

	dcss := newWhereSubtree(summaries).where(dir, recurseCount)
	sort.Sort(dcss)

	return dcss, nil
}

func newWhereSubtree(summaries map[string]*db.DirSummary) *whereSubtree {
	return &whereSubtree{
		summaries: summaries,
		children:  whereSubtreeChildren(summaries),
	}
}

func (d *clickHouseDatabase) dirInfoAncestor(
	dir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	normDir := ensureTrailingSlash(dir)

	gutas, err := d.gutasForAncestor(normDir)
	if err != nil {
		return nil, err
	}

	if len(gutas) == 0 {
		return &db.DirSummary{}, db.ErrDirNotFound
	}

	updatedAt, err := d.ancestorMaxUpdatedAt(normDir)
	if err != nil {
		return nil, err
	}

	return dirSummaryWithModtime(gutas, filter, updatedAt), nil
}

func (d *clickHouseDatabase) Children(dir string) ([]string, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	mountPath, ok, err := d.resolveMountScope(dir)
	if err != nil {
		return nil, err
	}

	parentDir := ensureTrailingSlash(dir)

	if !ok {
		return d.childrenForAncestor(parentDir)
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	return d.childrenForMount(
		mount.mountPath, mount.snapshotID, parentDir,
	)
}

// DirInfos returns directory summaries for multiple directories.
func (d *clickHouseDatabase) DirInfos(
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	if err := d.ensureOpen(); err != nil {
		return nil, err
	}

	result := make(map[string]*db.DirSummary, len(dirs))

	groups, fallback, err := d.groupDirsByActiveMount(dirs)
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

	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = false
	}

	groups, fallback, err := d.groupDirsByActiveMount(dirs)
	if err != nil {
		return nil, err
	}

	for _, group := range groups {
		if err := d.addDirsHaveChildrenForMount(result, group, filter); err != nil {
			return nil, err
		}
	}

	for _, dir := range fallback {
		result[dir] = d.dirHasChildrenSlow(dir, filter)
	}

	return result, nil
}

func (d *clickHouseDatabase) whereSubtreeSummaries(
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	mountPath, ok, err := d.resolveMountScope(queryDir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return d.whereSubtreeAncestorSummaries(queryDir, filter)
	}

	mount, found, err := d.activeMountForMountPath(mountPath)
	if err != nil || !found {
		return nil, err
	}

	return d.whereSubtreeSingleMountSummaries(mount, queryDir, filter)
}

func (d *clickHouseDatabase) whereSubtreeSingleMountSummaries(
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	filterClause, filterArgs := whereFilterClause(filter, "")
	query := fmt.Sprintf(whereSubtreeSingleMountQuery, filterClause)
	args := make([]any, 0, whereSingleMountBaseArgs+len(filterArgs))
	args = append(args, mount.mountPath, mount.snapshotID, queryDir)
	args = append(args, filterArgs...)

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryWhereSubtree(ctx, query, false, mount.updatedAt, filter.Age, args...)
}

func whereFilterClause(filter *db.Filter, columnPrefix string) (string, []any) {
	var b strings.Builder

	args := make([]any, 0, 1)

	appendIDFilter(&b, &args, columnPrefix+"gid", filter.GIDs)
	appendIDFilter(&b, &args, columnPrefix+"uid", filter.UIDs)
	appendFTFilter(&b, &args, columnPrefix+"ft", filter.FT)

	b.WriteString(" AND ")
	b.WriteString(columnPrefix)
	b.WriteString("age = ?")

	args = append(args, uint8(filter.Age))

	return b.String(), args
}

func (d *clickHouseDatabase) whereSubtreeAncestorSummaries(
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	if d.snapshot != nil {
		return d.whereSubtreeSnapshotAncestorSummaries(queryDir, filter)
	}

	filterClause, filterArgs := whereFilterClause(filter, "d.")
	query := fmt.Sprintf(whereSubtreeAncestorQuery, filterClause)
	args := make([]any, 0, whereAncestorBaseArgs+len(filterArgs))
	args = append(args, queryDir, queryDir)
	args = append(args, filterArgs...)

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryWhereSubtree(ctx, query, true, time.Time{}, filter.Age, args...)
}

func (d *clickHouseDatabase) whereSubtreeSnapshotAncestorSummaries(
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	mounts := d.snapshot.under(queryDir)
	updatedAt, _ := d.snapshot.maxUpdatedAt(queryDir)

	condition, conditionArgs := activeMountsTupleCondition(
		"d.mount_path",
		"d.snapshot_id",
		mounts,
	)
	filterClause, filterArgs := whereFilterClause(filter, "d.")
	query := fmt.Sprintf(whereSubtreeAncestorSnapshotQuery, condition, filterClause)
	args := make([]any, 0, 1+len(conditionArgs)+len(filterArgs))
	args = append(args, queryDir)
	args = append(args, conditionArgs...)
	args = append(args, filterArgs...)

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	summaries, err := d.queryWhereSubtree(ctx, query, false, time.Time{}, filter.Age, args...)
	if err != nil {
		return nil, err
	}

	applySnapshotWhereModtimes(summaries, mounts, updatedAt)

	return summaries, nil
}

func applySnapshotWhereModtimes(
	summaries map[string]*db.DirSummary,
	mounts []activeMount,
	defaultUpdatedAt time.Time,
) {
	for dir, summary := range summaries {
		updatedAt := whereUpdatedAtForDir(dir, mounts)
		if updatedAt.IsZero() {
			updatedAt = defaultUpdatedAt.UTC()
		}

		summary.Modtime = updatedAt
	}
}

func (d *clickHouseDatabase) queryWhereSubtree(
	ctx context.Context,
	query string,
	scanUpdatedAt bool,
	updatedAt time.Time,
	age db.DirGUTAge,
	args ...any,
) (map[string]*db.DirSummary, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query where subtree: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanWhereSubtreeRows(rows, scanUpdatedAt, updatedAt, age)
}

func scanWhereSubtreeRows(
	rows rowsScanner,
	scanUpdatedAt bool,
	defaultUpdatedAt time.Time,
	age db.DirGUTAge,
) (map[string]*db.DirSummary, error) {
	summaries := make(map[string]*db.DirSummary)

	for rows.Next() {
		dir, summary, err := scanWhereSubtreeRow(rows, scanUpdatedAt, defaultUpdatedAt, age)
		if err != nil {
			return nil, err
		}

		summaries[dir] = summary
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: where subtree iteration error: %w", err)
	}

	return summaries, nil
}

func (d *clickHouseDatabase) childrenForMount(mountPath, snapshotID, parentDir string) ([]string, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryChildren(ctx, childrenQuery, "children", mountPath, snapshotID, parentDir)
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
		sum, err := d.DirInfo(dir, filter)
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
	if err != nil || !found {
		return err
	}

	group := activeMountGroup(groups, mount)

	queryDir := ensureTrailingSlash(dir)
	if _, exists := group.originalDirs[queryDir]; !exists {
		group.queryDirs = append(group.queryDirs, queryDir)
	}

	group.originalDirs[queryDir] = dir

	return nil
}

func (d *clickHouseDatabase) addDirInfosForMount(
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

	return d.addMissingDirInfoSummaries(result, group, filter, gutasByDir)
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
	gutasByDir map[string]db.GUTAs,
) error {
	for _, queryDir := range group.queryDirs {
		if _, found := gutasByDir[queryDir]; found {
			continue
		}

		originalDir := group.originalDirs[queryDir]

		sum, err := d.DirInfo(originalDir, filter)
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
	childrenByParent, err := d.childrenForParentsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return nil, err
	}

	childParents, childDirs := collectChildParents(childrenByParent)
	if len(childDirs) == 0 {
		return map[string]bool{}, nil
	}

	if len(childDirs) <= dirsHaveChildrenSummaryFanoutLimit {
		return d.parentDirsWithSummarizedChildren(childParents, childDirs, filter)
	}

	parentDirs := parentDirsWithAnyChildren(group.queryDirs, childrenByParent)

	return d.parentDirsWithMatchingChildrenMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		parentDirs,
		filter,
	)
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
		ds, _ := d.DirInfo(child, filter) //nolint:errcheck
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

func rowIterationErr(rows any, msg string) error {
	if err := rowsErr(rows); err != nil {
		return fmt.Errorf("%s: %w", msg, err)
	}

	return nil
}

func scanActiveMountRow(rows rowsScanner) (activeMount, error) {
	var (
		mountPath, snapshotID string
		updatedAt             time.Time
	)

	if err := rows.Scan(&mountPath, &snapshotID, &updatedAt); err != nil {
		return activeMount{}, fmt.Errorf("clickhouse: failed to scan active mount: %w", err)
	}

	return activeMount{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		updatedAt:  updatedAt.UTC(),
	}, nil
}

func (d *clickHouseDatabase) childrenForAncestor(
	parentDir string,
) ([]string, error) {
	if d.snapshot != nil {
		return d.snapshotChildrenForAncestor(parentDir)
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryChildren(ctx, childrenAncestorQuery, "ancestor children", parentDir, parentDir)
}

func (d *clickHouseDatabase) snapshotChildrenForAncestor(
	parentDir string,
) ([]string, error) {
	mounts := d.snapshot.under(parentDir)
	if len(mounts) == 0 {
		return nil, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := activeMountsQuery(
		childrenAncestorSnapshotQuery,
		"c.mount_path",
		"c.snapshot_id",
		mounts,
		parentDir,
	)

	return d.queryChildren(ctx, query, "ancestor children", args...)
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
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryGUTAs(ctx, "dguta", dgutaQuery, mountPath, snapshotID, dir)
}

func (d *clickHouseDatabase) gutasForDirs(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	if len(dirs) == 0 {
		return map[string]db.GUTAs{}, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := scopedBatchQuery(dgutasForDirsQuery, dirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query dguta batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanDGUTARowsByDir(rows)
}

func scopedBatchQuery(queryFmt string, values []string, scopeArgs ...any) (string, []any) {
	query := fmt.Sprintf(queryFmt, placeholders(len(values)))
	args := make([]any, 0, len(values)+queryScopeArgs)
	args = append(args, scopeArgs...)

	for _, value := range values {
		args = append(args, value)
	}

	return query, args
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

func (d *clickHouseDatabase) gutasForAncestor(
	dir string,
) (db.GUTAs, error) {
	if d.snapshot != nil {
		return d.snapshotGUTAsForAncestor(dir)
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	return d.queryGUTAs(ctx, "ancestor dguta", dgutaAncestorQuery, dir, dir)
}

func (d *clickHouseDatabase) snapshotGUTAsForAncestor(dir string) (db.GUTAs, error) {
	mounts := d.snapshot.under(dir)
	if len(mounts) == 0 {
		return nil, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := activeMountsQuery(
		dgutaAncestorSnapshotQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
		dir,
	)

	return d.queryGUTAs(ctx, "ancestor dguta", query, args...)
}

func (d *clickHouseDatabase) ancestorMaxUpdatedAt(
	dir string,
) (time.Time, error) {
	if d.snapshot != nil {
		updatedAt, ok := d.snapshot.maxUpdatedAt(dir)
		if !ok {
			return time.Time{}, nil
		}

		return updatedAt.UTC(), nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	rows, err := d.conn.Query(
		ctx, ancestorMaxUpdatedAtQuery, dir,
	)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"clickhouse: failed to query ancestor max updated_at: %w",
			err,
		)
	}

	defer func() { _ = rows.Close() }()

	return scanMaxUpdatedAt(rows)
}

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

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	query, args := scopedBatchQuery(childrenForParentsQuery, parentDirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query children batch: %w", err)
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
	if err := t.loadSummaries(keys); err != nil {
		return nil, err
	}

	if err := t.loadChildren(keys); err != nil {
		return nil, err
	}

	childKeys := t.childSummaryDirs(keys)
	if err := t.loadSummaries(childKeys); err != nil {
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
	for _, group := range groups {
		if err := t.loadGroupChildren(group); err != nil {
			return err
		}
	}

	return nil
}

func (t *whereTraversal) loadFallbackChildDirs(dirs []string) error {
	for _, dir := range dirs {
		children, err := t.database.Children(dir)
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

type whereSubtree struct {
	summaries map[string]*db.DirSummary
	children  map[string][]string
}

func (s *whereSubtree) where(dir string, recurseCount func(string) int) db.DCSs {
	return s.recurseWhere(dir, recurseCount, 0)
}

func (s *whereSubtree) recurseWhere(dir string, recurseCount func(string) int, step int) db.DCSs {
	di := s.where0(dir)
	if di == nil {
		return nil
	}

	dcss := db.DCSs{di.Current}
	if recurseCount(dir) <= step {
		return dcss
	}

	for _, dcs := range di.Children {
		dcss = append(dcss, s.recurseWhere(dcs.Dir, recurseCount, step+1)...)
	}

	return dcss
}

func (s *whereSubtree) where0(dir string) *db.DirInfo {
	di := s.dirInfo(dir)
	if di == nil {
		return nil
	}

	for di.IsSameAsChild() {
		di = s.dirInfo(di.Children[0].Dir)
	}

	return di
}

func (s *whereSubtree) dirInfo(dir string) *db.DirInfo {
	key := ensureTrailingSlash(dir)

	current := s.summaryForDir(key, dir)
	if current == nil {
		return nil
	}

	di := &db.DirInfo{Current: current}

	for _, child := range s.children[key] {
		childSummary := s.summaryForDir(child, whereDisplayDir(child))
		if childSummary != nil && childSummary.Count > 0 {
			di.Children = append(di.Children, childSummary)
		}
	}

	return di
}

func whereDisplayDir(dir string) string {
	if dir == "/" {
		return dir
	}

	return strings.TrimSuffix(dir, "/")
}

func (s *whereSubtree) summaryForDir(key, displayDir string) *db.DirSummary {
	summary := s.summaries[key]
	if summary == nil {
		return nil
	}

	cp := *summary
	cp.Dir = displayDir

	return &cp
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
}

func scanWhereSubtreeRow(
	rows rowsScanner,
	scanUpdatedAt bool,
	updatedAt time.Time,
	age db.DirGUTAge,
) (string, *db.DirSummary, error) {
	var scanned whereSubtreeScanned
	if err := scanned.scanFrom(rows, scanUpdatedAt, updatedAt); err != nil {
		return "", nil, err
	}

	return scanned.dir, scanned.summary(age), nil
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

type whereSubtreeScanned struct {
	dir          string
	uids, gids   []uint32
	ft           uint16
	count, size  uint64
	atime, mtime int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
	updatedAt    time.Time
}

func (s *whereSubtreeScanned) scanFrom(
	rows rowsScanner,
	scanUpdatedAt bool,
	updatedAt time.Time,
) error {
	s.updatedAt = updatedAt

	dest := []any{
		&s.dir,
		&s.uids,
		&s.gids,
		&s.ft,
		&s.count,
		&s.size,
		&s.atime,
		&s.mtime,
		&s.atimeBuckets,
		&s.mtimeBuckets,
	}
	if scanUpdatedAt {
		dest = append(dest, &s.updatedAt)
	}

	if err := rows.Scan(dest...); err != nil {
		return fmt.Errorf("clickhouse: failed to scan where subtree: %w", err)
	}

	return nil
}

func (s *whereSubtreeScanned) summary(age db.DirGUTAge) *db.DirSummary {
	return &db.DirSummary{
		Count:       s.count,
		Size:        s.size,
		Atime:       time.Unix(s.atime, 0),
		CommonATime: summary.MostCommonBucket(sliceToAgeBuckets(s.atimeBuckets)),
		Mtime:       time.Unix(s.mtime, 0),
		CommonMTime: summary.MostCommonBucket(sliceToAgeBuckets(s.mtimeBuckets)),
		UIDs:        s.uids,
		GIDs:        s.gids,
		FT:          db.DirGUTAFileType(s.ft),
		Age:         age,
		Modtime:     s.updatedAt.UTC(),
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

func gutaExistenceFilterClause(filter *db.Filter, columnPrefix string) (string, []any) {
	var b strings.Builder

	args := make([]any, 0, 1)

	b.WriteString(" AND ")
	b.WriteString(columnPrefix)
	b.WriteString("count > 0")

	if filter == nil {
		return b.String(), nil
	}

	appendIDFilter(&b, &args, columnPrefix+"gid", filter.GIDs)
	appendIDFilter(&b, &args, columnPrefix+"uid", filter.UIDs)
	appendFTFilter(&b, &args, columnPrefix+"ft", filter.FT)

	b.WriteString(" AND ")
	b.WriteString(columnPrefix)
	b.WriteString("age = ?")

	args = append(args, uint8(filter.Age))

	return b.String(), args
}

func appendFTFilter(
	b *strings.Builder,
	args *[]any,
	column string,
	ft db.DirGUTAFileType,
) {
	if ft == 0 {
		return
	}

	b.WriteString(" AND bitAnd(")
	b.WriteString(column)
	b.WriteString(", ?) > 0")

	*args = append(*args, uint16(ft))
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

func whereUpdatedAtForDir(dir string, mounts []activeMount) time.Time {
	var updatedAt time.Time

	for _, mount := range mounts {
		if !strings.HasPrefix(dir, mount.mountPath) && !strings.HasPrefix(mount.mountPath, dir) {
			continue
		}

		if mount.updatedAt.After(updatedAt) {
			updatedAt = mount.updatedAt.UTC()
		}
	}

	return updatedAt
}

func appendIDFilter(
	b *strings.Builder,
	args *[]any,
	column string,
	values []uint32,
) {
	if values == nil {
		return
	}

	if len(values) == 0 {
		b.WriteString(" AND 1 = 0")

		return
	}

	b.WriteString(" AND ")
	b.WriteString(column)
	b.WriteString(" IN (")
	b.WriteString(placeholders(len(values)))
	b.WriteString(")")

	for _, value := range values {
		*args = append(*args, value)
	}
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

func whereSubtreeChildren(summaries map[string]*db.DirSummary) map[string][]string {
	children := make(map[string][]string)

	for dir := range summaries {
		parent, ok := whereParentDir(dir)
		if !ok {
			continue
		}

		children[parent] = append(children[parent], dir)
	}

	for parent := range children {
		sort.Strings(children[parent])
	}

	return children
}

func whereParentDir(dir string) (string, bool) {
	trimmed := strings.TrimSuffix(dir, "/")
	if trimmed == "" {
		return "", false
	}

	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return "", false
	}

	if idx == 0 {
		return "/", true
	}

	return trimmed[:idx+1], true
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
