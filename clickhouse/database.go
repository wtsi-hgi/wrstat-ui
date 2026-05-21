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

	dgutaInitialCap = 8
	queryScopeArgs  = 2

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
	childrenByParent, err := d.childrenForParentsMount(
		group.mount.mountPath,
		group.mount.snapshotID,
		group.queryDirs,
	)
	if err != nil {
		return err
	}

	childParents, childDirs := collectChildParents(childrenByParent)

	childSummaries, err := d.DirInfos(childDirs, filter)
	if err != nil {
		return err
	}

	markParentsWithMatchingChildren(result, group, childParents, childSummaries)

	return nil
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

func markParentsWithMatchingChildren(
	result map[string]bool,
	group *activeMountDirGroup,
	childParents map[string][]string,
	childSummaries map[string]*db.DirSummary,
) {
	for child, summary := range childSummaries {
		if summary == nil || summary.Count == 0 {
			continue
		}

		for _, parent := range childParents[child] {
			result[group.originalDirs[parent]] = true
		}
	}
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
