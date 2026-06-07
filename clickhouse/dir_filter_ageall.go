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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	dirFilterAgeAllTableName = "wrstat_dir_filter_ageall"

	insertDirFilterAgeAllQuery = "INSERT INTO wrstat_dir_filter_ageall " +
		"(mount_path, snapshot_id, gid, uid, ft, dir, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

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

	dirFilterAgeAllSummariesForDirsQuery = "SELECT dir, count() AS raw_rows, " +
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
		"WHERE dir IN (%s) AND %s " +
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

var errDirFilterAgeAllBatchNotPrepared = errors.New("clickhouse: AgeAll filter batch is not prepared")

var errDirFilterAgeAllTableUnavailable = errors.New("clickhouse: AgeAll filter table is unavailable")

type dirFilterAgeAllWriter struct {
	conn ch.Conn

	batch    driver.Batch
	openedAt time.Time
	batchNow func() time.Time

	batchSize   int
	refreshedAt time.Time
	writeErr    error
}

func newDirFilterAgeAllWriter(conn ch.Conn, batchSize int, refreshedAt time.Time) *dirFilterAgeAllWriter {
	writer := &dirFilterAgeAllWriter{
		conn:        conn,
		batchSize:   batchSize,
		refreshedAt: refreshedAt,
	}
	if writer.refreshedAt.IsZero() {
		writer.refreshedAt = writer.importBatchNow().UTC()
	}

	return writer
}

func (w *dirFilterAgeAllWriter) appendRecord(
	ctx context.Context,
	mount activeMount,
	parentDir string,
	gutas db.GUTAs,
	_ []string,
	_ []db.DirGUTAge,
) error {
	for _, guta := range gutas {
		if !dirFilterAgeAllWritableGUTA(guta) {
			continue
		}

		if err := w.appendRow(ctx, mount, parentDir, guta); err != nil {
			return err
		}
	}

	return nil
}

func dirFilterAgeAllWritableGUTA(guta *db.GUTA) bool {
	return guta != nil && guta.Age == db.DGUTAgeAll
}

func (w *dirFilterAgeAllWriter) appendRow(
	ctx context.Context,
	mount activeMount,
	parentDir string,
	guta *db.GUTA,
) error {
	return w.blockWriter().append(ctx, func(batch driver.Batch) error {
		return batch.Append(
			mount.mountPath,
			mount.snapshotID,
			guta.GID,
			guta.UID,
			uint16(guta.FT),
			parentDir,
			guta.Count,
			guta.Size,
			guta.Atime,
			guta.Mtime,
			ageBucketsSlice(&guta.ATimeRanges),
			ageBucketsSlice(&guta.MTimeRanges),
			w.refreshedAt,
		)
	})
}

func (w *dirFilterAgeAllWriter) flush(context.Context) error {
	return w.blockWriter().close()
}

func (w *dirFilterAgeAllWriter) abort() error {
	return w.blockWriter().abort()
}

func (w *dirFilterAgeAllWriter) blockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertDirFilterAgeAllQuery,
		name:        "AgeAll filter",
		batch:       &w.batch,
		openedAt:    &w.openedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.batchSize,
		notPrepared: errDirFilterAgeAllBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *dirFilterAgeAllWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}

func dirFilterAgeAllCanHandleFilter(filter *db.Filter) bool {
	if filter == nil || filter.Age != db.DGUTAgeAll {
		return false
	}

	if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return false
	}

	return filter.GIDs != nil || filter.UIDs != nil || filter.FT != 0
}

func dirFilterAgeAllSummariesForDirsQueryForFilter(
	mountPath, snapshotID string,
	dirs []string,
	filter *db.Filter,
) (string, []any) {
	filterExpr, filterArgs := dirFilterAgeAllFilterExpression(filter)
	query := fmt.Sprintf(dirFilterAgeAllSummariesForDirsQuery, placeholders(len(dirs)), filterExpr)

	args := make([]any, 0, queryScopeArgs+len(dirs)+len(filterArgs))
	args = append(args, mountPath, snapshotID)

	for _, dir := range dirs {
		args = append(args, dir)
	}

	args = append(args, filterArgs...)

	return query, args
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

func handledDirFilterAgeAllDirs(dirs []string) map[string]bool {
	handled := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		handled[dir] = true
	}

	return handled
}

func mergeDirFilterAgeAllSummaries(
	summaries, batch map[string]*db.DirSummary,
) {
	for dir, sum := range batch {
		summaries[dir] = sum
	}
}

func (d *clickHouseDatabase) dirFilterAgeAllReadyForFilter(
	ctx context.Context,
	mountPath, snapshotID string,
	filter *db.Filter,
) (bool, error) {
	if !dirFilterAgeAllCanHandleFilter(filter) {
		return false, nil
	}

	ready, err := d.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query AgeAll filter index readiness: %w", err)
	}

	return ready, nil
}

func (d *clickHouseDatabase) dirFilterAgeAllWhereSummaries(
	ctx context.Context,
	mount activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, bool, error) {
	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, mount.mountPath, mount.snapshotID, filter)
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
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("clickhouse: failed to query AgeAll filter index summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, true, err
}

func (d *clickHouseDatabase) dirFilterAgeAllSummariesForDirsMount(
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, map[string]bool, bool, error) {
	if len(dirs) == 0 {
		return map[string]*db.DirSummary{}, map[string]bool{}, true, nil
	}

	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, mountPath, snapshotID, filter)
	if err != nil || !ready {
		return nil, nil, false, err
	}

	summaries, err := d.dirFilterAgeAllSummaryBatches(ctx, mountPath, snapshotID, updatedAt, dirs, filter)
	if err != nil {
		if errors.Is(err, errDirFilterAgeAllTableUnavailable) {
			return nil, nil, false, nil
		}

		return nil, nil, true, err
	}

	return summaries, handledDirFilterAgeAllDirs(dirs), true, nil
}

func (d *clickHouseDatabase) dirFilterAgeAllSummaryBatches(
	ctx context.Context,
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	summaries := make(map[string]*db.DirSummary)

	for _, batchDirs := range stringValueBatches(dirs) {
		batch, err := d.queryDirFilterAgeAllSummariesForDirsBatch(
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

		mergeDirFilterAgeAllSummaries(summaries, batch)
	}

	return summaries, nil
}

func (d *clickHouseDatabase) queryDirFilterAgeAllSummariesForDirsBatch(
	ctx context.Context,
	mountPath, snapshotID string,
	updatedAt time.Time,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := dirFilterAgeAllSummariesForDirsQueryForFilter(mountPath, snapshotID, dirs, filter)

	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		if isUnknownTable(err) {
			return nil, errDirFilterAgeAllTableUnavailable
		}

		return nil, fmt.Errorf("clickhouse: failed to query AgeAll filter index dir summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, err
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

	ready, err := d.dirFilterAgeAllReadyForFilter(ctx, group.mount.mountPath, group.mount.snapshotID, filter)
	if err != nil || !ready {
		return nil, false, err
	}

	parents, err := d.queryAgeAllFilteredChildParentBatches(ctx, group, parentDirs, filter)
	if err != nil {
		if errors.Is(err, errDirFilterAgeAllTableUnavailable) {
			return nil, false, nil
		}

		return nil, true, err
	}

	return parents, true, nil
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
		if isUnknownTable(err) {
			return nil, errDirFilterAgeAllTableUnavailable
		}

		return nil, fmt.Errorf("clickhouse: failed to query AgeAll filtered child dirs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentDirSet(rows)
}
