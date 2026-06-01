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
	"errors"
	"fmt"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	mountDirDGUTAVectorsForDirsQuery = "SELECT dir, updated_at, gids, uids, fts, ages, " +
		"counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, child_count " +
		"FROM wrstat_dir_facts " +
		"PREWHERE mount_path = ? AND snapshot_id = ? " +
		"WHERE dir IN (%s)"

	mountDirDGUTAVectorsForExternalDirsQuery = "SELECT v.dir, v.updated_at, v.gids, v.uids, " +
		"v.fts, v.ages, v.counts, v.sizes, v.atime_mins, v.mtime_maxs, " +
		"v.atime_buckets, v.mtime_buckets, v.child_count " +
		"FROM wrstat_dir_facts AS v " +
		"ANY INNER JOIN " + externalDirsTableName + " AS q ON q.dir = v.dir " +
		"WHERE v.mount_path = ? AND v.snapshot_id = ?"
)

var errMountDirDGUTAVectorLengthMismatch = errors.New("clickhouse: dir dguta vector column lengths differ")

type mountDirDGUTAVectorScanned struct {
	dir          string
	updatedAt    time.Time
	gids         []uint32
	uids         []uint32
	fts          []uint16
	ages         []uint8
	counts       []uint64
	sizes        []uint64
	atimeMins    []int64
	mtimeMaxs    []int64
	atimeBuckets [][]uint64
	mtimeBuckets [][]uint64
	childCount   uint64
}

func (s *mountDirDGUTAVectorScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.dir,
		&s.updatedAt,
		&s.gids,
		&s.uids,
		&s.fts,
		&s.ages,
		&s.counts,
		&s.sizes,
		&s.atimeMins,
		&s.mtimeMaxs,
		&s.atimeBuckets,
		&s.mtimeBuckets,
		&s.childCount,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan dir dguta vector: %w", err)
	}

	return nil
}

func (s *mountDirDGUTAVectorScanned) gutas() (db.GUTAs, error) {
	if err := s.validateLengths(); err != nil {
		return nil, err
	}

	gutas := make(db.GUTAs, len(s.gids))
	for i := range s.gids {
		gutas[i] = &db.GUTA{
			GID:         s.gids[i],
			UID:         s.uids[i],
			FT:          db.DirGUTAFileType(s.fts[i]),
			Age:         db.DirGUTAge(s.ages[i]),
			Count:       s.counts[i],
			Size:        s.sizes[i],
			Atime:       s.atimeMins[i],
			Mtime:       s.mtimeMaxs[i],
			ATimeRanges: sliceToAgeBuckets(s.atimeBuckets[i]),
			MTimeRanges: sliceToAgeBuckets(s.mtimeBuckets[i]),
		}
	}

	return gutas, nil
}

func (s *mountDirDGUTAVectorScanned) validateLengths() error {
	expected := len(s.gids)
	lengths := []int{
		len(s.uids),
		len(s.fts),
		len(s.ages),
		len(s.counts),
		len(s.sizes),
		len(s.atimeMins),
		len(s.mtimeMaxs),
		len(s.atimeBuckets),
		len(s.mtimeBuckets),
	}

	for _, length := range lengths {
		if length != expected {
			return fmt.Errorf("%w: dir=%s", errMountDirDGUTAVectorLengthMismatch, s.dir)
		}
	}

	return nil
}

func scanMountDirDGUTAVectorRow(rows rowsScanner) (string, db.GUTAs, error) {
	var s mountDirDGUTAVectorScanned
	if err := s.scanFrom(rows); err != nil {
		return "", nil, err
	}

	gutas, err := s.gutas()
	if err != nil {
		return "", nil, err
	}

	return s.dir, gutas, nil
}

func mountDirDGUTAVectorCanHandleFilter(filter *db.Filter) bool {
	if filter == nil {
		return false
	}

	_, ok := mountDirSummaryModeForFilter(filter)

	return !ok
}

func scanMountDirDGUTAVectorRows(rows rowsScanner) (map[string]db.GUTAs, error) {
	gutasByDir := make(map[string]db.GUTAs)

	for rows.Next() {
		dir, gutas, err := scanMountDirDGUTAVectorRow(rows)
		if err != nil {
			return nil, err
		}

		gutasByDir[dir] = append(gutasByDir[dir], gutas...)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: dir dguta vector iteration error: %w", err)
	}

	return gutasByDir, nil
}

func (d *clickHouseDatabase) mountDirDGUTAVectorSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if !mountDirDGUTAVectorCanHandleFilter(filter) {
		return nil, nil, false, nil
	}

	gutasByDir, ok, err := d.mountDirDGUTAVectorsForDirsMount(mountPath, snapshotID, dirs)
	if err != nil || !ok {
		return nil, nil, ok, err
	}

	summaries := make(map[string]*db.DirSummary, len(gutasByDir))

	summaryHandled := make(map[string]bool, len(gutasByDir))
	for dir, gutas := range gutasByDir {
		summaryHandled[dir] = true
		if sum := dirSummaryWithModtime(gutas, filter, updatedAt); sum != nil {
			summaries[dir] = sum
		}
	}

	return summaries, summaryHandled, true, nil
}

func (d *clickHouseDatabase) mountDirDGUTAVectorsForDirsMount(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, bool, error) {
	gutasByDir, handled, missing := d.cachedMountDirDGUTAVectors(mountPath, snapshotID, dirs)
	if len(missing) == 0 {
		return gutasByDir, true, nil
	}

	ok, err := d.addMissingMountDirDGUTAVectors(gutasByDir, handled, mountPath, snapshotID, missing)
	if err != nil || !ok {
		return nil, ok, err
	}

	return gutasByDir, true, nil
}

func (d *clickHouseDatabase) cachedMountDirDGUTAVectors(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, map[string]bool, []string) {
	gutasByDir := make(map[string]db.GUTAs, len(dirs))
	handled := make(map[string]bool, len(dirs))
	missing := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		key := newTreeCacheKey(mountPath, snapshotID, dir)
		if seen[key.dir] {
			continue
		}

		seen[key.dir] = true
		if gutas, ok := d.treeCache.getGUTAs(key); ok {
			handled[key.dir] = true
			if len(gutas) > 0 {
				gutasByDir[key.dir] = gutas
			}

			continue
		}

		missing = append(missing, key.dir)
	}

	return gutasByDir, handled, missing
}

func (d *clickHouseDatabase) addMissingMountDirDGUTAVectors(
	gutasByDir map[string]db.GUTAs,
	handled map[string]bool,
	mountPath, snapshotID string,
	missing []string,
) (bool, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.mountDirDGUTAVectorReadyCached(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return false, err
	}

	queried, err := d.queryMountDirDGUTAVectorsForDirs(ctx, mountPath, snapshotID, missing)
	if err != nil {
		return true, err
	}

	d.addQueriedMountDirDGUTAVectors(gutasByDir, handled, mountPath, snapshotID, missing, queried)

	return true, nil
}

func (d *clickHouseDatabase) mountDirDGUTAVectorReadyCached(
	ctx context.Context,
	mountPath, snapshotID string,
) (bool, error) {
	key := newTreeMountCacheKey(mountPath, snapshotID)
	if d.treeCache.getMountDirDGUTAVectorReady(key) {
		return true, nil
	}

	ready, err := d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
	if err != nil || !ready {
		return ready, err
	}

	d.treeCache.putMountDirDGUTAVectorReady(key)

	return true, nil
}

func (d *clickHouseDatabase) queryMountDirDGUTAVectorsForDirs(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	if len(dirs) > queryStringINMaxValues {
		return d.queryMountDirDGUTAVectorsForExternalDirs(ctx, mountPath, snapshotID, dirs)
	}

	return d.queryMountDirDGUTAVectorsForDirsBatches(ctx, mountPath, snapshotID, dirs)
}

func (d *clickHouseDatabase) queryMountDirDGUTAVectorsForDirsBatches(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	gutasByDir := make(map[string]db.GUTAs)

	for _, batchDirs := range stringValueBatches(dirs) {
		batch, err := d.queryMountDirDGUTAVectorsForDirsBatch(ctx, mountPath, snapshotID, batchDirs)
		if err != nil {
			return nil, err
		}

		for dir, gutas := range batch {
			gutasByDir[dir] = gutas
		}
	}

	return gutasByDir, nil
}

func (d *clickHouseDatabase) queryMountDirDGUTAVectorsForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	query, args := scopedBatchQuery(mountDirDGUTAVectorsForDirsQuery, dirs, mountPath, snapshotID)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query dir dguta vectors: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanMountDirDGUTAVectorRows(rows)
}

func (d *clickHouseDatabase) queryMountDirDGUTAVectorsForExternalDirs(
	ctx context.Context,
	mountPath, snapshotID string,
	dirs []string,
) (map[string]db.GUTAs, error) {
	externalCtx, err := contextWithExternalDirs(ctx, dirs)
	if err != nil {
		return nil, err
	}

	rows, err := d.conn.Query(externalCtx, mountDirDGUTAVectorsForExternalDirsQuery, mountPath, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query external dir dguta vectors: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanMountDirDGUTAVectorRows(rows)
}

func (d *clickHouseDatabase) addQueriedMountDirDGUTAVectors(
	gutasByDir map[string]db.GUTAs,
	handled map[string]bool,
	mountPath, snapshotID string,
	missing []string,
	queried map[string]db.GUTAs,
) {
	for _, dir := range missing {
		gutas := queried[dir]
		if len(gutas) > 0 {
			d.treeCache.putGUTAs(newTreeCacheKey(mountPath, snapshotID, dir), gutas)
			handled[dir] = true
			gutasByDir[dir] = cloneGUTAs(gutas)
		}
	}
}
