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
	treeSummaryDirsHaveChildrenSummaryFixedArgs = 3
	treeSummaryCacheColumnCount                 = 24

	treeSummaryExistsQuery = "SELECT 1 FROM wrstat_virtual_summary_sets " +
		"WHERE active_set_id = ? LIMIT 1"

	insertTreeSummarySetQuery = "INSERT INTO wrstat_virtual_summary_sets " +
		"(active_set_id, active_mount_count, refreshed_at) VALUES (?, ?, ?)"

	insertTreeSummaryCacheQuery = "INSERT INTO wrstat_virtual_summary_cache " +
		"(active_set_id, dir, updated_at, all_count, all_size, all_atime_min, all_mtime_max, " +
		"all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, gids, uids, fts, ages, " +
		"counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, child_count, refreshed_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	treeSummaryDGUTAQuery = "SELECT updated_at, " + dgutaTupleColumns + " " +
		"FROM (" +
		"SELECT updated_at, arrayJoin(" + dgutaArrayZipExpr + ") AS g " +
		"FROM wrstat_virtual_summary_cache WHERE active_set_id = ? AND dir = ?)"

	treeDirSummaryQuery = "SELECT updated_at, age, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, uids, gids, ft " +
		"FROM (" +
		"SELECT updated_at, ? AS age, all_count AS count, all_size AS size, all_atime_min AS atime_min, " +
		"all_mtime_max AS mtime_max, all_atime_buckets AS atime_buckets, all_mtime_buckets AS mtime_buckets, " +
		"all_uids AS uids, all_gids AS gids, all_ft AS ft " +
		"FROM wrstat_virtual_summary_cache WHERE active_set_id = ? AND dir = ?)"

	treeSummaryDirsHaveChildrenSummaryQuery = "SELECT dir AS parent_dir " +
		"FROM wrstat_virtual_summary_cache WHERE active_set_id = ? AND dir IN (%s) " +
		"AND child_count > 0 AND ? = ? ORDER BY parent_dir ASC"

	treeSummaryDirsHaveChildrenQuery = "SELECT dir AS parent_dir " +
		"FROM wrstat_virtual_summary_cache WHERE active_set_id = ? AND dir IN (%s) " +
		"AND child_count > 0 %s ORDER BY parent_dir ASC"
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

type treeSummaryProgress interface {
	ancestorStarted(dir string, index, total, mountCount int)
	ancestorCompleted(dir string, index, total, mountCount int, duration time.Duration)
	phaseStarted(dir string, index, total, mountCount int, phase string)
	phaseCompleted(dir string, index, total, mountCount int, phase string, duration time.Duration)
}

func insertTreeSummaryCache(
	ctx context.Context,
	conn ch.Conn,
	fingerprint, dir string,
	updatedAt, refreshedAt time.Time,
	mounts []activeMount,
) error {
	state, err := treeSummaryProjectionState(ctx, conn, dir, mounts)
	if err != nil {
		return err
	}

	if err := conn.Exec(
		ctx,
		insertTreeSummaryCacheQuery,
		treeSummaryCacheRowValues(fingerprint, dir, updatedAt, refreshedAt, state)...,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to insert tree summary cache: %w", err)
	}

	return nil
}

func treeSummaryProjectionState(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
) (mountDirProjectionState, error) {
	state := newMountDirProjectionState()

	if err := addTreeSummaryGUTAs(ctx, conn, dir, mounts, &state); err != nil {
		return mountDirProjectionState{}, err
	}

	if err := addTreeSummaryChildren(ctx, conn, dir, mounts, &state); err != nil {
		return mountDirProjectionState{}, err
	}

	return state, nil
}

func addTreeSummaryGUTAs(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
	state *mountDirProjectionState,
) error {
	query, args := activeMountsQuery(
		dgutaAncestorSnapshotQuery,
		"d.mount_path",
		"d.snapshot_id",
		mounts,
		dir,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query tree summary dgutas: %w", err)
	}

	defer func() { _ = rows.Close() }()

	gutas, err := scanDGUTARows(rows)
	if err != nil {
		return err
	}

	state.addGUTAs(dir, gutas)

	return nil
}

func addTreeSummaryChildren(
	ctx context.Context,
	conn ch.Conn,
	dir string,
	mounts []activeMount,
	state *mountDirProjectionState,
) error {
	query, args := activeMountsQuery(
		"SELECT c.parent_dir, count() FROM wrstat_children c WHERE c.parent_dir = ? AND %s GROUP BY c.parent_dir",
		"c.mount_path",
		"c.snapshot_id",
		mounts,
		dir,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query tree summary children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanTreeSummaryChildren(rows, state)
}

func scanTreeSummaryChildren(rows driver.Rows, state *mountDirProjectionState) error {
	for rows.Next() {
		var (
			parentDir string
			count     uint64
		)

		if err := rows.Scan(&parentDir, &count); err != nil {
			return fmt.Errorf("clickhouse: failed to scan tree summary children: %w", err)
		}

		state.addChildren(parentDir, count)
	}

	return rows.Err()
}

func treeSummaryCacheRowValues(
	fingerprint, dir string,
	updatedAt, refreshedAt time.Time,
	state mountDirProjectionState,
) []any {
	allKey := mountDirSummaryKey{dir: dir, age: db.DGUTAgeAll}
	acc := summaryAccumulatorOrZero(state.summaries[allKey])
	values := make([]any, 0, treeSummaryCacheColumnCount)
	values = append(values,
		fingerprint, dir, updatedAt,
		acc.count, acc.size, acc.atimeMin, acc.mtimeMax,
		ageBucketsSlice(&acc.atimeBuckets), ageBucketsSlice(&acc.mtimeBuckets),
		sortedUint32Set(acc.uids), sortedUint32Set(acc.gids), uint16(acc.ft),
	)
	values = append(values, mountDirProjectionVectorValues(state.vectors[dir])...)
	values = append(values, state.childCounts[dir], refreshedAt)

	return values
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
	return treeSummaryExists(ctx, conn, treeSummaryExistsQuery, fingerprint, "set")
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

func refreshActiveTreeSummaries(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	fingerprint string,
) error {
	return refreshActiveTreeSummariesWithProgress(ctx, conn, rows, fingerprint, nil)
}

func refreshActiveTreeSummariesWithProgress(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	fingerprint string,
	progress treeSummaryProgress,
) error {
	snapshot := newActiveMountsSnapshot(rows)
	refreshedAt := time.Now().UTC()
	dirs := activeTreeDirs(snapshot.all())

	for i, dir := range dirs {
		if err := refreshActiveTreeSummaryDirWithProgress(
			ctx,
			conn,
			snapshot,
			dir,
			i+1,
			len(dirs),
			fingerprint,
			refreshedAt,
			progress,
		); err != nil {
			return err
		}
	}

	return insertTreeSummarySet(ctx, conn, fingerprint, countActiveMountRows(rows), refreshedAt)
}

func refreshActiveTreeSummaryDirWithProgress(
	ctx context.Context,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
	dir string,
	index, total int,
	fingerprint string,
	refreshedAt time.Time,
	progress treeSummaryProgress,
) error {
	refresh, ok := newTreeSummaryDirRefresh(snapshot, dir, index, total, fingerprint, refreshedAt)
	if !ok {
		return nil
	}

	started := time.Now()

	reportTreeSummaryAncestorStarted(progress, refresh)

	if err := insertTreeSummaryDirRowsWithProgress(ctx, conn, refresh, progress); err != nil {
		return err
	}

	reportTreeSummaryAncestorCompleted(progress, refresh, time.Since(started))

	return nil
}

type treeSummaryDirRefresh struct {
	dir         string
	index       int
	total       int
	mountCount  int
	fingerprint string
	updatedAt   time.Time
	refreshedAt time.Time
	mounts      []activeMount
}

func newTreeSummaryDirRefresh(
	snapshot *activeMountsSnapshot,
	dir string,
	index, total int,
	fingerprint string,
	refreshedAt time.Time,
) (treeSummaryDirRefresh, bool) {
	mounts := snapshot.under(dir)
	if len(mounts) == 0 {
		return treeSummaryDirRefresh{}, false
	}

	return treeSummaryDirRefresh{
		dir:         dir,
		index:       index,
		total:       total,
		mountCount:  len(mounts),
		fingerprint: fingerprint,
		updatedAt:   maxUpdatedAtForMounts(mounts),
		refreshedAt: refreshedAt,
		mounts:      mounts,
	}, true
}

func reportTreeSummaryAncestorStarted(progress treeSummaryProgress, refresh treeSummaryDirRefresh) {
	if progress != nil {
		progress.ancestorStarted(refresh.dir, refresh.index, refresh.total, refresh.mountCount)
	}
}

func reportTreeSummaryAncestorCompleted(
	progress treeSummaryProgress,
	refresh treeSummaryDirRefresh,
	duration time.Duration,
) {
	if progress != nil {
		progress.ancestorCompleted(refresh.dir, refresh.index, refresh.total, refresh.mountCount, duration)
	}
}

func insertTreeSummaryDirRowsWithProgress(
	ctx context.Context,
	conn ch.Conn,
	refresh treeSummaryDirRefresh,
	progress treeSummaryProgress,
) error {
	return runTreeSummaryDirPhase(progress, refresh, "summary", func() error {
		return insertTreeSummaryCache(
			ctx, conn, refresh.fingerprint, refresh.dir, refresh.updatedAt, refresh.refreshedAt, refresh.mounts,
		)
	})
}

func runTreeSummaryDirPhase(
	progress treeSummaryProgress,
	refresh treeSummaryDirRefresh,
	phase string,
	fn func() error,
) error {
	return runTreeSummaryPhase(
		progress,
		refresh.dir,
		refresh.index,
		refresh.total,
		refresh.mountCount,
		phase,
		fn,
	)
}

func runTreeSummaryPhase(
	progress treeSummaryProgress,
	dir string,
	index, total, mountCount int,
	phase string,
	fn func() error,
) error {
	if progress != nil {
		progress.phaseStarted(dir, index, total, mountCount, phase)
	}

	started := time.Now()

	if err := fn(); err != nil {
		return err
	}

	if progress != nil {
		progress.phaseCompleted(dir, index, total, mountCount, phase, time.Since(started))
	}

	return nil
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

	for i, part := range parts {
		if part == "" {
			continue
		}

		current += part + "/"
		if i < len(parts)-1 {
			dirs = append(dirs, current)
		}
	}

	return dirs
}

func maxUpdatedAtForMounts(mounts []activeMount) time.Time {
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

	return latest
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

	filterClause, filterArgs := gutaExistenceFilterClause(filter, "")
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

	args = append(args, uint8(filter.Age), uint8(db.DGUTAgeAll))

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

	rows, err := d.conn.Query(ctx, treeDirSummaryQuery, uint8(filter.Age), fingerprint, dir)
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
	_ context.Context,
	_ string,
) ([]string, bool, error) {
	return nil, false, nil
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
