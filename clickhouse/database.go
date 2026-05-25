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

	dgutaInitialCap                      = 8
	dirsHaveChildrenSummaryPrefetchLimit = 64
	dirsHaveChildrenSummaryFanoutLimit   = 16
	dirSummaryFilterClauseInitialCap     = 3
	groupedDirSummaryMinDirs             = 4096
	queryStringINMaxValues               = 1000
	queryScopeArgs                       = 2
	activeMountDirTupleArgs              = 3

	childrenQuery = "SELECT DISTINCT child FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND parent_dir = ? " +
		"ORDER BY child"

	childrenAncestorQuery = "WITH active AS (" +
		"SELECT mount_path, snapshot_id " +
		"FROM wrstat_mounts_active_v2 " +
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
		"FROM wrstat_mounts_active_v2 " +
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
		"FROM wrstat_mounts_active_v2 " +
		"WHERE startsWith(mount_path, ?)"

	infoDGUTAQuery = "SELECT " +
		"uniqExact(dir) AS num_dirs, " +
		"count() AS num_dgutas " +
		"FROM wrstat_dguta " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active_v2" +
		")"

	infoChildrenQuery = "SELECT " +
		"uniqExact(parent_dir) AS num_parents, " +
		"count() AS num_children " +
		"FROM wrstat_children " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active_v2" +
		")"

	resolveMountQuery = "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active_v2 " +
		"WHERE startsWith(?, mount_path) " +
		"ORDER BY length(mount_path) DESC LIMIT 1"

	resolveExactMountQuery = "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active_v2 " +
		"WHERE mount_path = ? LIMIT 1"

	childrenAncestorSnapshotQuery = "SELECT DISTINCT c.child " +
		"FROM wrstat_children c " +
		"WHERE c.parent_dir = ? AND %s " +
		"ORDER BY c.child ASC"

	dgutaAncestorSnapshotQuery = "SELECT d.gid, d.uid, d.ft, d.age, d.count, d.size, " +
		"d.atime_min, d.mtime_max, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dguta d " +
		"WHERE d.dir = ? AND %s"

	dgutasForActiveMountRootDirsQuery = "SELECT d.mount_path, d.gid, d.uid, d.ft, d.age, d.count, d.size, " +
		"d.atime_min, d.mtime_max, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dguta d " +
		"WHERE d.dir = d.mount_path AND %s"

	dgutasForActiveMountDirsQuery = "SELECT d.dir, d.gid, d.uid, d.ft, d.age, d.count, d.size, " +
		"d.atime_min, d.mtime_max, d.atime_buckets, d.mtime_buckets " +
		"FROM wrstat_dguta d " +
		"WHERE (d.mount_path, d.snapshot_id, d.dir) IN (%s)"

	dgutasForDirsQuery = "SELECT dir, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets " +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)"

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
		"SELECT dir, gid, uid, ft, count AS file_count, size AS file_size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, " +
		"%s AS passes_filter " +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)" +
		") " +
		"GROUP BY dir"

	childrenForParentsQuery = "SELECT parent_dir, child " +
		"FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE parent_dir IN (%s) " +
		"ORDER BY parent_dir ASC, child ASC"

	childrenForExternalParentsQuery = "SELECT c.parent_dir, c.child " +
		"FROM wrstat_children AS c " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = c.parent_dir " +
		"WHERE c.mount_path = ? AND c.snapshot_id = ? " +
		"ORDER BY c.parent_dir ASC, c.child ASC"

	activeMountRootChildrenQuery = "SELECT c.parent_dir, c.child " +
		"FROM wrstat_children c " +
		"WHERE c.parent_dir = c.mount_path AND %s " +
		"ORDER BY c.parent_dir ASC, c.child ASC"

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

	filteredMountWhereSummariesQuery = "SELECT dir, count() AS raw_rows, " +
		"sum(count) AS total_count, " +
		"sum(size) AS total_size, " +
		"minIf(atime_min, atime_min != 0) AS atime_min, " +
		"max(mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(uid)) AS uids, " +
		"arraySort(groupUniqArray(gid)) AS gids, " +
		"groupBitOr(ft) AS file_types " +
		"FROM wrstat_dguta " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE %s " +
		"GROUP BY dir"
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

type activeMountRootDirs struct {
	mounts       []activeMount
	originalDirs map[string][]string
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

	summaries, err := t.database.filteredMountWhereSummaries(*t.mount, t.filter)
	if err != nil {
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
	if t.mount == nil || !mountDirDGUTAVectorCanHandleFilter(t.filter) {
		return false
	}

	return ensureTrailingSlash(queryDir) == ensureTrailingSlash(t.mount.mountPath)
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

type clickHouseDatabase struct {
	cfg  Config
	conn ch.Conn

	mountPoints    basedirs.MountPoints
	mountPointsErr error

	snapshot *activeMountsSnapshot
	closed   atomic.Bool

	treeCache *treeQueryCache
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
		treeCache:      treeQueryCacheForConfig(cfg),
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
	sum, found, ok, err := d.dirInfoDGUTAVectorMount(mountPath, snapshotID, updatedAt, dir, filter)
	if err != nil {
		return nil, err
	}

	if ok {
		return dirInfoSummaryResult(sum, found, updatedAt)
	}

	return d.dirInfoSingleMountFallback(
		mountPath, snapshotID, updatedAt, dir, filter,
	)
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

	if ok {
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

	gutasByDir, _, ok, err := d.mountDirDGUTAVectorsForDirsMount(mountPath, snapshotID, []string{queryDir})
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
	queryDir := ensureTrailingSlash(dir)

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

	if found {
		traversal.mount = &mount
	}

	return traversal, nil
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

	if sum, ok, err := d.dirInfoTreeSummaryAncestor(normDir, filter); err != nil || ok {
		return sum, err
	}

	return d.dirInfoAncestorFallback(normDir, filter)
}

func (d *clickHouseDatabase) dirInfoAncestorFallback(
	normDir string,
	filter *db.Filter,
) (*db.DirSummary, error) {
	gutas, err := d.gutasForAncestor(normDir)
	if err != nil {
		return nil, err
	}

	if len(gutas) == 0 {
		sum, ok, virtualErr := d.dirInfoVirtualAncestor(normDir, filter)
		if virtualErr != nil || ok {
			return sum, virtualErr
		}

		return &db.DirSummary{}, db.ErrDirNotFound
	}

	updatedAt, err := d.ancestorMaxUpdatedAt(normDir)
	if err != nil {
		return nil, err
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
	mounts, err := d.activeMountsUnder(dir)
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

	roots, remaining, err := d.splitActiveMountRootDirs(dirs)
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

	fallback, err := d.addBatchedDirsHaveChildren(result, dirs, filter)
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
	if len(roots.mounts) == 0 {
		return nil
	}

	childrenByParent, err := d.childrenForActiveMountRoots(roots.mounts)
	if err != nil {
		return err
	}

	parents, err := d.parentActiveMountRootsWithChildren(roots.mounts, filter, childrenByParent)
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
	key := newTreeCacheKey(mountPath, snapshotID, parentDir)
	if children, ok := d.treeCache.getChildren(key); ok {
		return children, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	children, err := d.queryChildren(ctx, childrenQuery, "children", mountPath, snapshotID, parentDir)
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
	if len(roots.mounts) == 0 {
		return nil
	}

	summaries, err := d.activeMountRootSummaries(roots.mounts, filter)
	if err != nil {
		return err
	}

	for _, mount := range roots.mounts {
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
	ok, err := d.addGroupedDirInfosForMount(result, group, filter)
	if err != nil || ok {
		return err
	}

	return d.addRawDirInfosForMount(result, group, filter)
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

	parentDirs := parentDirsWithAnyChildren(group.queryDirs, childrenByParent)
	if len(parentDirs) == 0 {
		return map[string]bool{}, nil
	}

	if broadFilterCanUseChildRows(filter) {
		parents := parentDirSet(parentDirs)

		ignoreDirsHaveChildrenPrefetchError(
			d.prefetchDirsHaveChildrenSummaries(group, filter, childrenByParent),
		)

		return parents, nil
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

func ignoreDirsHaveChildrenPrefetchError(error) {}

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
	summaries, handled, ok, err := d.mountDirSummariesForDirsMount(
		mountPath,
		snapshotID,
		dirs,
		filter,
	)
	_ = summaries
	_ = handled
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
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	children, err := d.queryChildren(ctx, childrenAncestorQuery, "ancestor children", parentDir, parentDir)
	if err != nil || len(children) > 0 {
		return children, err
	}

	return d.childrenForVirtualAncestor(parentDir)
}

func (d *clickHouseDatabase) snapshotOrVirtualChildrenForAncestor(parentDir string) ([]string, error) {
	children, err := d.snapshotChildrenForAncestor(parentDir)
	if err != nil || len(children) > 0 {
		return children, err
	}

	return d.childrenForVirtualAncestor(parentDir)
}

func (d *clickHouseDatabase) childrenForVirtualAncestor(parentDir string) ([]string, error) {
	mounts, err := d.activeMountsUnder(parentDir)
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

	query, args := activeMountsQuery(
		dgutasForActiveMountRootDirsQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
	)

	return d.queryGUTAsByDir(ctx, "active mount root dguta batch", query, args...)
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

	summaries, handled, ok, err := d.mountDirSummariesForDirsMount(mountPath, snapshotID, dirs, filter)
	if err != nil || ok {
		return summaries, handled, ok, err
	}

	summaries, handled, ok, err = d.mountDirDGUTAVectorSummariesForDirsMount(
		mountPath,
		snapshotID,
		updatedAt,
		dirs,
		filter,
	)
	if err != nil || ok {
		return summaries, handled, ok, err
	}

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

func (d *clickHouseDatabase) filteredMountWhereSummaries(
	mount activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := filteredMountWhereSummariesQueryForFilter(mount.mountPath, mount.snapshotID, filter)

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query filtered mount where summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, err
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
	for _, group := range groups {
		if err := t.loadGroupChildren(group); err != nil {
			return err
		}
	}

	return nil
}

func (t *whereTraversal) loadFallbackChildDirs(dirs []string) error {
	for _, dir := range dirs {
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
