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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	treeSummaryDirsHaveChildrenSummaryFixedArgs = 2

	treeSummaryExistsQuery = "SELECT 1 FROM wrstat_tree_summary_sets FINAL " +
		"WHERE fingerprint = ? LIMIT 1"

	insertTreeSummarySetQuery = "INSERT INTO wrstat_tree_summary_sets " +
		"(fingerprint, active_mount_count, refreshed_at) VALUES (?, ?, ?)"

	insertTreeSummaryDGUTAQuery = "INSERT INTO wrstat_tree_dguta " +
		"(fingerprint, dir, updated_at, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, refreshed_at) " +
		"SELECT ?, ?, ?, d.gid, d.uid, d.ft, d.age, sum(d.count), sum(d.size), " +
		"minIf(d.atime_min, d.atime_min != 0), max(d.mtime_max), " +
		"arrayReduce('sumForEach', groupArray(d.atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(d.mtime_buckets)), ? " +
		"FROM wrstat_dguta d WHERE d.dir = ? AND %s " +
		"GROUP BY d.gid, d.uid, d.ft, d.age"

	insertTreeDirSummaryQuery = "INSERT INTO wrstat_tree_dir_summary " +
		"(fingerprint, dir, updated_at, age, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, uids, gids, ft, refreshed_at) " +
		"SELECT ?, ?, ?, d.age, sum(d.count), sum(d.size), " +
		"minIf(d.atime_min, d.atime_min != 0), max(d.mtime_max), " +
		"arrayReduce('sumForEach', groupArray(d.atime_buckets)), " +
		"arrayReduce('sumForEach', groupArray(d.mtime_buckets)), " +
		"arraySort(groupUniqArray(d.uid)), arraySort(groupUniqArray(d.gid)), " +
		"groupBitOr(d.ft), ? " +
		"FROM wrstat_dguta d WHERE d.dir = ? AND %s GROUP BY d.age"

	insertTreeSummaryChildrenQuery = "INSERT INTO wrstat_tree_children " +
		"(fingerprint, parent_dir, child, refreshed_at) " +
		"SELECT DISTINCT ?, c.parent_dir, c.child, ? " +
		"FROM wrstat_children c WHERE c.parent_dir = ? AND %s"

	treeSummaryDGUTAQuery = "SELECT updated_at, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets " +
		"FROM wrstat_tree_dguta FINAL WHERE fingerprint = ? AND dir = ?"

	treeDirSummaryQuery = "SELECT updated_at, age, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, uids, gids, ft " +
		"FROM wrstat_tree_dir_summary FINAL WHERE fingerprint = ? AND dir = ? AND age = ?"

	treeSummaryChildrenQuery = "SELECT child FROM wrstat_tree_children FINAL " +
		"WHERE fingerprint = ? AND parent_dir = ? ORDER BY child"

	treeSummaryDirsHaveChildrenSummaryQuery = "SELECT c.parent_dir " +
		"FROM wrstat_tree_children FINAL AS c " +
		"INNER JOIN wrstat_tree_dir_summary FINAL AS s " +
		"ON s.fingerprint = c.fingerprint " +
		"AND s.dir = if(endsWith(c.child, '/'), c.child, concat(c.child, '/')) " +
		"WHERE c.fingerprint = ? AND c.parent_dir IN (%s) " +
		"AND s.age = ? AND s.count > 0 " +
		"GROUP BY c.parent_dir ORDER BY c.parent_dir ASC"

	treeSummaryDirsHaveChildrenQuery = "SELECT c.parent_dir " +
		"FROM wrstat_tree_children FINAL AS c " +
		"INNER JOIN wrstat_tree_dguta FINAL AS s " +
		"ON s.fingerprint = c.fingerprint " +
		"AND s.dir = if(endsWith(c.child, '/'), c.child, concat(c.child, '/')) " +
		"WHERE c.fingerprint = ? AND c.parent_dir IN (%s) %s " +
		"GROUP BY c.parent_dir ORDER BY c.parent_dir ASC"
)

var errTreeDirSummaryMultipleRows = errors.New("clickhouse: tree dir summary returned multiple rows")

type treeDirSummaryScanned struct {
	updatedAt    time.Time
	age          uint8
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

func (s *treeDirSummaryScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
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
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan tree dir summary: %w", err)
	}

	return nil
}

func (s *treeDirSummaryScanned) summary() *db.DirSummary {
	atimeBuckets := sliceToAgeBuckets(s.atimeBuckets)
	mtimeBuckets := sliceToAgeBuckets(s.mtimeBuckets)

	return &db.DirSummary{
		Count:       s.count,
		Size:        s.size,
		Atime:       time.Unix(s.atimeMin, 0),
		CommonATime: summary.MostCommonBucket(atimeBuckets),
		Mtime:       time.Unix(s.mtimeMax, 0),
		CommonMTime: summary.MostCommonBucket(mtimeBuckets),
		UIDs:        s.uids,
		GIDs:        s.gids,
		FT:          db.DirGUTAFileType(s.ft),
		Age:         db.DirGUTAge(s.age),
		Modtime:     s.updatedAt.UTC(),
	}
}

func scanTreeDirSummary(rows rowsScanner) (*db.DirSummary, error) {
	var s treeDirSummaryScanned
	if err := s.scanFrom(rows); err != nil {
		return nil, err
	}

	return s.summary(), nil
}

func ensureActiveTreeSummaries(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
) error {
	if len(rows) == 0 {
		return nil
	}

	fingerprint := fingerprintForMountsActive(rows)

	exists, err := treeSummaryReady(ctx, conn, fingerprint)
	if err != nil || exists {
		return err
	}

	return refreshActiveTreeSummaries(ctx, conn, rows, fingerprint)
}

func treeSummaryReady(ctx context.Context, conn ch.Conn, fingerprint string) (bool, error) {
	setExists, err := treeSummaryExists(ctx, conn, treeSummaryExistsQuery, fingerprint, "set")
	if err != nil || !setExists {
		return setExists, err
	}

	return treeSummaryExists(ctx, conn, treeDirSummaryExistsQuery(), fingerprint, "dir summary")
}

func treeSummaryExists(ctx context.Context, conn ch.Conn, query, fingerprint, what string) (bool, error) {
	rows, err := conn.Query(ctx, query, fingerprint)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query tree summary %s: %w", what, err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: tree summary %s iteration error: %w", what, err)
	}

	return false, nil
}

func treeDirSummaryExistsQuery() string {
	return "SELECT 1 FROM wrstat_tree_dir_summary FINAL WHERE fingerprint = ? LIMIT 1"
}

func refreshActiveTreeSummaries(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	fingerprint string,
) error {
	snapshot := newActiveMountsSnapshot(rows)
	refreshedAt := time.Now().UTC()

	for _, dir := range activeTreeDirs(snapshot.all()) {
		mounts := snapshot.under(dir)
		if len(mounts) == 0 {
			continue
		}

		updatedAt, _ := maxUpdatedAtForMounts(mounts)

		if err := insertTreeSummaryDGUTA(ctx, conn, fingerprint, dir, updatedAt, refreshedAt, mounts); err != nil {
			return err
		}

		if err := insertTreeDirSummary(ctx, conn, fingerprint, dir, updatedAt, refreshedAt, mounts); err != nil {
			return err
		}

		if err := insertTreeSummaryChildren(ctx, conn, fingerprint, dir, refreshedAt, mounts); err != nil {
			return err
		}
	}

	return insertTreeSummarySet(ctx, conn, fingerprint, countActiveMountRows(rows), refreshedAt)
}

func activeTreeDirs(mounts []activeMount) []string {
	seen := make(map[string]bool)

	for _, mount := range mounts {
		for _, dir := range treeDirsForMount(mount.mountPath) {
			seen[dir] = true
		}
	}

	dirs := make([]string, 0, len(seen))
	for dir := range seen {
		dirs = append(dirs, dir)
	}

	sort.Strings(dirs)

	return dirs
}

func treeDirsForMount(mountPath string) []string {
	mountPath = ensureTrailingSlash(mountPath)
	if mountPath == "/" {
		return []string{"/"}
	}

	parts := strings.Split(strings.Trim(mountPath, "/"), "/")
	dirs := []string{"/"}
	current := "/"

	for _, part := range parts {
		if part == "" {
			continue
		}

		current += part + "/"
		dirs = append(dirs, current)
	}

	return dirs
}

func maxUpdatedAtForMounts(mounts []activeMount) (time.Time, bool) {
	var (
		latest time.Time
		ok     bool
	)

	for _, mount := range mounts {
		if ok && !mount.updatedAt.After(latest) {
			continue
		}

		latest = mount.updatedAt.UTC()
		ok = true
	}

	return latest, ok
}

func insertTreeSummaryDGUTA(
	ctx context.Context,
	conn ch.Conn,
	fingerprint, dir string,
	updatedAt, refreshedAt time.Time,
	mounts []activeMount,
) error {
	query, args := activeMountsQuery(
		insertTreeSummaryDGUTAQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
		fingerprint,
		dir,
		updatedAt,
		refreshedAt,
		dir,
	)

	if err := conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("clickhouse: failed to insert tree dguta summary: %w", err)
	}

	return nil
}

func insertTreeDirSummary(
	ctx context.Context,
	conn ch.Conn,
	fingerprint, dir string,
	updatedAt, refreshedAt time.Time,
	mounts []activeMount,
) error {
	query, args := activeMountsQuery(
		insertTreeDirSummaryQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
		fingerprint,
		dir,
		updatedAt,
		refreshedAt,
		dir,
	)

	if err := conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("clickhouse: failed to insert tree dir summary: %w", err)
	}

	return nil
}

func insertTreeSummaryChildren(
	ctx context.Context,
	conn ch.Conn,
	fingerprint, parentDir string,
	refreshedAt time.Time,
	mounts []activeMount,
) error {
	query, args := activeMountsQuery(
		insertTreeSummaryChildrenQuery,
		"c.mount_path",
		"c.snapshot_id",
		mounts,
		fingerprint,
		refreshedAt,
		parentDir,
	)

	if err := conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("clickhouse: failed to insert tree children summary: %w", err)
	}

	return nil
}

func insertTreeSummarySet(
	ctx context.Context,
	conn ch.Conn,
	fingerprint string,
	activeMountCount uint64,
	refreshedAt time.Time,
) error {
	if err := conn.Exec(ctx, insertTreeSummarySetQuery, fingerprint, activeMountCount, refreshedAt); err != nil {
		return fmt.Errorf("clickhouse: failed to insert tree summary set: %w", err)
	}

	return nil
}

func countActiveMountRows(rows []mountsActiveRow) uint64 {
	var count uint64
	for range rows {
		count++
	}

	return count
}

func treeSummaryDirsHaveChildrenSQL(
	parentDirs []string,
	fingerprint string,
	filter *db.Filter,
) (string, []any) {
	if canUseTreeDirSummary(filter) {
		return treeSummaryDirsHaveChildrenSummarySQL(parentDirs, fingerprint, filter)
	}

	filterClause, filterArgs := gutaExistenceFilterClause(filter, "s.")
	query := fmt.Sprintf(
		treeSummaryDirsHaveChildrenQuery,
		placeholders(len(parentDirs)),
		filterClause,
	)

	args := make([]any, 0, 1+len(parentDirs)+len(filterArgs))
	args = append(args, fingerprint)

	for _, parentDir := range parentDirs {
		args = append(args, parentDir)
	}

	args = append(args, filterArgs...)

	return query, args
}

func canUseTreeDirSummary(filter *db.Filter) bool {
	return filter != nil && filter.GIDs == nil && filter.UIDs == nil && filter.FT == 0
}

func treeSummaryDirsHaveChildrenSummarySQL(
	parentDirs []string,
	fingerprint string,
	filter *db.Filter,
) (string, []any) {
	query := fmt.Sprintf(
		treeSummaryDirsHaveChildrenSummaryQuery,
		placeholders(len(parentDirs)),
	)

	args := make([]any, 0, treeSummaryDirsHaveChildrenSummaryFixedArgs+len(parentDirs))
	args = append(args, fingerprint)

	for _, parentDir := range parentDirs {
		args = append(args, parentDir)
	}

	args = append(args, uint8(filter.Age))

	return query, args
}

func scanTreeDirSummaryRow(rows driver.Rows) (*db.DirSummary, bool, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, fmt.Errorf("clickhouse: tree dir summary iteration error: %w", err)
		}

		return nil, false, nil
	}

	sum, err := scanTreeDirSummary(rows)
	if err != nil {
		return nil, false, err
	}

	if rows.Next() {
		return nil, false, errTreeDirSummaryMultipleRows
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("clickhouse: tree dir summary iteration error: %w", err)
	}

	return sum, true, nil
}

func scanTreeSummaryDGUTARows(rows driver.Rows) (db.GUTAs, time.Time, error) {
	gutas := make(db.GUTAs, 0, dgutaInitialCap)

	var updatedAt time.Time

	for rows.Next() {
		guta, rowUpdatedAt, err := scanTreeSummaryDGUTARow(rows)
		if err != nil {
			return nil, time.Time{}, err
		}

		if updatedAt.IsZero() || rowUpdatedAt.After(updatedAt) {
			updatedAt = rowUpdatedAt.UTC()
		}

		gutas = append(gutas, guta)
	}

	if err := rows.Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("clickhouse: tree dguta summary iteration error: %w", err)
	}

	return gutas, updatedAt.UTC(), nil
}

func scanTreeSummaryDGUTARow(rows rowsScanner) (*db.GUTA, time.Time, error) {
	var (
		updatedAt time.Time
		s         dgutaScanned
	)

	if err := rows.Scan(
		&updatedAt,
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
		return nil, time.Time{}, fmt.Errorf("clickhouse: failed to scan tree dguta summary: %w", err)
	}

	return s.guta(), updatedAt.UTC(), nil
}

func canonicalTreeSummaryDirs(dirs []string) ([]string, map[string][]string) {
	queryDirs := make([]string, 0, len(dirs))
	originalDirs := make(map[string][]string, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		queryDir := ensureTrailingSlash(dir)
		originalDirs[queryDir] = append(originalDirs[queryDir], dir)

		if seen[queryDir] {
			continue
		}

		queryDirs = append(queryDirs, queryDir)
		seen[queryDir] = true
	}

	return queryDirs, originalDirs
}

func originalTreeSummaryParentResults(
	parents map[string]bool,
	originalDirs map[string][]string,
) map[string]bool {
	result := make(map[string]bool)

	for queryDir, originals := range originalDirs {
		if !parents[queryDir] {
			continue
		}

		for _, original := range originals {
			result[original] = true
		}
	}

	return result
}

func (d *clickHouseDatabase) treeSummaryGUTAs(
	ctx context.Context,
	dir string,
) (db.GUTAs, time.Time, bool, error) {
	fingerprint, ok, err := d.activeTreeSummaryFingerprint(ctx)
	if err != nil || !ok {
		return nil, time.Time{}, false, err
	}

	rows, err := d.conn.Query(ctx, treeSummaryDGUTAQuery, fingerprint, dir)
	if err != nil {
		return nil, time.Time{}, false, fmt.Errorf("clickhouse: failed to query tree dguta summary: %w", err)
	}

	defer func() { _ = rows.Close() }()

	gutas, updatedAt, err := scanTreeSummaryDGUTARows(rows)
	if err != nil {
		return nil, time.Time{}, false, err
	}

	return gutas, updatedAt, len(gutas) > 0, nil
}

func (d *clickHouseDatabase) treeDirSummary(
	ctx context.Context,
	dir string,
	filter *db.Filter,
) (*db.DirSummary, bool, error) {
	if !canUseTreeDirSummary(filter) {
		return nil, false, nil
	}

	fingerprint, ok, err := d.activeTreeSummaryFingerprint(ctx)
	if err != nil || !ok {
		return nil, false, err
	}

	rows, err := d.conn.Query(ctx, treeDirSummaryQuery, fingerprint, dir, uint8(filter.Age))
	if err != nil {
		return nil, false, fmt.Errorf("clickhouse: failed to query tree dir summary: %w", err)
	}

	defer func() { _ = rows.Close() }()

	sum, ok, err := scanTreeDirSummaryRow(rows)
	if err != nil || !ok {
		return sum, false, err
	}

	return sum, true, nil
}

func (d *clickHouseDatabase) treeSummaryChildren(
	ctx context.Context,
	parentDir string,
) ([]string, bool, error) {
	fingerprint, ok, err := d.activeTreeSummaryFingerprint(ctx)
	if err != nil || !ok {
		return nil, false, err
	}

	children, err := d.queryChildren(
		ctx,
		treeSummaryChildrenQuery,
		"tree summary children",
		fingerprint,
		parentDir,
	)
	if err != nil {
		return nil, false, err
	}

	return children, true, nil
}

func (d *clickHouseDatabase) treeSummaryDirsHaveChildren(
	ctx context.Context,
	dirs []string,
	filter *db.Filter,
) (map[string]bool, bool, error) {
	if len(dirs) == 0 {
		return map[string]bool{}, true, nil
	}

	fingerprint, ok, err := d.activeTreeSummaryFingerprint(ctx)
	if err != nil || !ok {
		return nil, false, err
	}

	queryDirs, originalDirs := canonicalTreeSummaryDirs(dirs)
	query, args := treeSummaryDirsHaveChildrenSQL(queryDirs, fingerprint, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("clickhouse: failed to query tree summary child dirs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	parents, err := scanParentDirSet(rows)
	if err != nil {
		return nil, false, err
	}

	return originalTreeSummaryParentResults(parents, originalDirs), true, nil
}

func (d *clickHouseDatabase) activeTreeSummaryFingerprint(
	ctx context.Context,
) (string, bool, error) {
	if d.snapshot != nil {
		return d.snapshot.treeSummaryFingerprint()
	}

	if d.conn == nil {
		return "", false, nil
	}

	rows, err := queryMountsActiveRows(ctx, d.conn)
	if err != nil || len(rows) == 0 {
		return "", false, err
	}

	fingerprint := fingerprintForMountsActive(rows)

	ready, readyErr := treeSummaryReady(ctx, d.conn, fingerprint)
	if readyErr != nil {
		ready = false
	}

	if !ready {
		return "", false, nil
	}

	return fingerprint, true, nil
}
