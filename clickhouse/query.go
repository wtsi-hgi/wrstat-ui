/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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
	"io"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

// ListImmediateChildren returns direct children of a directory (global, all mounts).
// This supports navigation at '/', '/lustre/', etc., leveraging synthetic dirs.
//
//nolint:gocognit,funlen
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, dir string) ([]FileEntry, error) {
	dir = EnsureDir(dir)

	// Use a query that ensures only unique paths are returned
	// Filter to directories only at the SQL level
	debugf("ListImmediateChildren: dir=%s", dir)

	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
		FROM (
			-- Select the unique entries with the highest inode number for each path
			SELECT *
			FROM fs_entries_current
			WHERE parent_path = ? AND path != '/'
			ORDER BY path, inode DESC
			LIMIT 1 BY path
		)`,
		dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]FileEntry, 0, DefaultResultCapacity)
	for rows.Next() {
		var entry FileEntry
		if err := rows.Scan(
			&entry.Path, &entry.ParentPath, &entry.Name, &entry.Ext,
			&entry.FType, &entry.INode, &entry.Size,
			&entry.UID, &entry.GID,
			&entry.MTime, &entry.ATime, &entry.CTime); err != nil {
			return nil, err
		}

		results = append(results, entry)
	}

	if debugEnabled() {
		debugf("ListImmediateChildren: got %d entries", len(results))

		if debugVerbose() {
			for i, e := range results {
				// Split to avoid overly long lines
				debugf("  [%d] path=%s parent=%s name=%s", i, e.Path, e.ParentPath, e.Name)
				debugf("        ftype=%d size=%d uid=%d gid=%d", e.FType, e.Size, e.UID, e.GID)
			}
		}
	}

	return results, rows.Err()
}

// buildBucketPredicate builds a time bucket predicate for filtering.
// buildBucketPredicateWithScanExpr builds a time bucket predicate using the provided scan time expression.
// scanExpr should evaluate to a DateTime, e.g. "toDateTime(max_scan)" or "toDateTime(scan_id)".
func buildBucketPredicateWithScanExpr(scanExpr, col, bucket string) (string, error) {
	if bucket == "" {
		return "", nil
	}

	// Map of bucket identifiers to their time interval expressions
	timeIntervals := map[string]string{
		"0d":  ">= (toDateTime(max_scan) - INTERVAL 1 DAY)",
		">1m": "< (toDateTime(max_scan) - INTERVAL 1 MONTH)",
		">2m": "< (toDateTime(max_scan) - INTERVAL 2 MONTH)",
		">6m": "< (toDateTime(max_scan) - INTERVAL 6 MONTH)",
		">1y": "< (toDateTime(max_scan) - INTERVAL 1 YEAR)",
		">2y": "< (toDateTime(max_scan) - INTERVAL 2 YEAR)",
		">3y": "< (toDateTime(max_scan) - INTERVAL 3 YEAR)",
		">5y": "< (toDateTime(max_scan) - INTERVAL 5 YEAR)",
		">7y": "< (toDateTime(max_scan) - INTERVAL 7 YEAR)",
	}

	interval, found := timeIntervals[bucket]
	if !found {
		return "", fmt.Errorf("%w: %s", ErrInvalidBucket, bucket)
	}

	// Replace max_scan reference with the provided scan expression
	interval = strings.ReplaceAll(interval, "toDateTime(max_scan)", scanExpr)

	return col + " " + interval, nil
}

// subtreeSummaryScan returns statistics for a subtree by scanning fs_entries_current (global, all mounts),
// filtered by the given criteria. This is the fallback when rollups cannot be used.
func (c *Clickhouse) subtreeSummaryScan(ctx context.Context, dir string, f Filters) (Summary, error) {
	dir = EnsureDir(dir)
	// For phase 1 correctness, prefer scan-based summaries on fs_entries_current
	// which align with Bolt semantics (unique paths, file-only aggregation), and
	// avoid rollup double-counting complexities.
	where, args := buildAllWhere(dir, f)
	bucketFilter := buildGlobalTimeBucketFilter(f)

	query := buildAllSummaryQuery(where, bucketFilter)

	if debugEnabled() {
		debugf("subtreeSummaryScan: dir=%s filters=%+v", dir, f)
		debugf("subtreeSummaryScan SQL:\n%s", query)
		debugf("subtreeSummaryScan args=%v", args)
	}

	return c.executeSummaryQuery(ctx, query, args)
}

// Helper function to check if no filters are applied.
// (helper removed) isNoFilters

// Helper function for getting unfiltered summary.
// Removed per-mount unfiltered summary; global unfiltered handled by getUnfilteredAllSummary

// Build basic where clause and args for the query.
// Removed per-mount where builder (API unified to global)

// appendFilterClauses removed; buildAllWhere handles filters directly

// Build IN clause for filters.
func buildInClause(field string, values []uint32) (string, []any) {
	placeholders := make([]string, len(values))
	args := make([]any, len(values))

	for i, v := range values {
		placeholders[i] = "?"
		args[i] = v
	}

	return fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")), args
}

// Build extension filter clause.
func buildExtensionClause(exts []string) (string, []any) {
	placeholders := make([]string, len(exts))
	args := make([]any, len(exts))

	for i, v := range exts {
		placeholders[i] = "?"
		args[i] = strings.ToLower(v)
	}

	return fmt.Sprintf("ext_low IN (%s)", strings.Join(placeholders, ",")), args
}

// buildAccessTimeFilter removed in global API; use buildGlobalTimeBucketFilter.
// buildModificationTimeFilter removed in global API; use buildGlobalTimeBucketFilter.

// joinTimeFilters combines multiple time filter predicates.
func joinTimeFilters(predicates []string) string {
	if len(predicates) == 0 {
		return ""
	}

	return " AND (" + strings.Join(predicates, " AND ") + ")"
}

// buildTimeBucketFilter removed in global API; use buildGlobalTimeBucketFilter.

// buildGlobalTimeBucketFilter builds time bucket filters using per-row scan time (toDateTime(scan_id)).
func buildGlobalTimeBucketFilter(f Filters) string { //nolint:gocyclo
	if f.ATimeBucket == "" && f.MTimeBucket == "" {
		return ""
	}

	var preds []string

	add := func(expr, col, bucket string) {
		p, err := buildBucketPredicateWithScanExpr(expr, col, bucket)
		if err == nil && p != "" {
			preds = append(preds, p)
		}
	}

	if f.ATimeBucket != "" {
		add("toDateTime(scan_id)", "atime", f.ATimeBucket)
	}

	if f.MTimeBucket != "" {
		add("toDateTime(scan_id)", "mtime", f.MTimeBucket)
	}

	s := joinTimeFilters(preds)
	if debugEnabled() && s != "" {
		debugf("buildGlobalTimeBucketFilter: %s", s)
	}

	return s
}

// Build the complete summary query.
// Removed per-mount summary query builder (API unified to global)

// buildAllSummaryQuery builds the summary query across all mounts (no mount_path filter),
// using per-row scan time for any time buckets.
func buildAllSummaryQuery(where []string, bucketFilter string) string {
	// To match Bolt behaviour, we need to:
	// 1. Deduplicate directory entries by normalising paths
	// 2. Handle extra synthetic directory entries that are part of ClickHouse but not Bolt
	// 3. Count only real entries, not duplicates from path expansion
	//
	// Use a subquery that first filters to unique paths, then aggregates only those.
	// Filter out synthetic directories by excluding entries with uid=0 AND gid=0 (synthetic markers).
	q := `
SELECT
	sum(agg_size) AS total_size,
	count() AS file_count,
	max(agg_atime) AS most_recent_atime,
	min(agg_atime) AS oldest_atime,
	max(agg_mtime) AS most_recent_mtime,
	min(agg_mtime) AS oldest_mtime,
	groupUniqArray(sel_uid) AS uids,
	groupUniqArray(sel_gid) AS gids,
	groupUniqArray(sel_ext) AS exts,
	groupUniqArray(sel_ftype) AS ftypes
FROM (
	-- Normalise by trimming a trailing slash so '/x' and '/x/' group together
	SELECT
		if(endsWith(path, '/'), substring(path, 1, length(path) - 1), path) AS norm_path,
		max(size) AS agg_size,
		max(atime) AS agg_atime,
		max(mtime) AS agg_mtime,
		anyLast(uid) AS sel_uid,
		anyLast(gid) AS sel_gid,
		anyLast(ext_low) AS sel_ext,
		anyLast(ftype) AS sel_ftype
	FROM fs_entries_current
	WHERE ` + strings.Join(where, " AND ") + bucketFilter + `
	  AND ftype = ` + fmt.Sprintf("%d", FileTypeFile) + `
	GROUP BY norm_path
)
`

	return q
}

// Execute the summary query and return results.
func (c *Clickhouse) executeSummaryQuery(ctx context.Context, query string, args []any) (Summary, error) {
	if debugEnabled() {
		debugf("executeSummaryQuery: args=%v", args)
	}

	row := c.conn.QueryRow(ctx, query, args...)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts,
		&s.FTypes); err != nil {
		return Summary{}, err
	}

	if debugEnabled() {
		debugf("executeSummaryQuery: result size=%d count=%d", s.TotalSize, s.FileCount)

		if debugVerbose() {
			// Split logs to avoid overly long lines
			debugf("  UIDs=%v GIDs=%v Exts=%v FTypes=%v", s.UIDs, s.GIDs, s.Exts, s.FTypes)
			debugf("  ATime[min]=%s", s.OldestATime.Format("2006-01-02 15:04:05"))
			debugf("  ATime[max]=%s", s.MostRecentATime.Format("2006-01-02 15:04:05"))
			debugf("  MTime[min]=%s", s.OldestMTime.Format("2006-01-02 15:04:05"))
			debugf("  MTime[max]=%s", s.MostRecentMTime.Format("2006-01-02 15:04:05"))
		}
	}

	return s, nil
}

// SubtreeSummary uses rollups when possible (no filters) and falls back to a scan for filtered queries.
// This keeps optimization as an implementation detail behind a single public API.
func (c *Clickhouse) SubtreeSummary(ctx context.Context, dir string, f Filters) (Summary, error) { //nolint:gocognit
	// For phase 1 correctness, prefer scan-based summaries which align with Bolt semantics
	// (unique paths) and avoid rollup double-counting complexities.
	sum, err := c.subtreeSummaryScan(ctx, dir, f)
	if err != nil {
		return Summary{}, err
	}

	// Empirically, scan-based aggregation over fs_entries_current counts file-only
	// stats twice due to the way the underlying stats stream encodes entries.
	// Adjust totals here to match Bolt semantics before adding directory contributions.
	// if sum.FileCount > 0 {
	// 	sum.FileCount /= 2
	// 	sum.TotalSize /= 2
	// }

	// Augment with directory-with-files contributions to match Bolt semantics:
	// For each distinct directory that directly contains at least one file
	// within the subtree, add +1 to count and +DirectorySize to size.
	// Only augment when absolutely no filters are applied (unfiltered view),
	// i.e. no GIDs, UIDs, Exts, ATimeBucket or MTimeBucket.
	if len(f.GIDs) == 0 && len(f.UIDs) == 0 && len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == "" {
		if dirCnt, derr := c.DirCountWithFiles(ctx, dir, f); derr == nil {
			sum.FileCount += dirCnt
			sum.TotalSize += dirCnt * DirectorySize

			// Ensure 'dir' appears in file types if any directories-with-files exist
			if dirCnt > 0 {
				// FileTypeDir is 2 (see clickhouse.FileTypeDir)
				hasDir := false

				for _, ft := range sum.FTypes {
					if ft == uint8(FileTypeDir) {
						hasDir = true

						break
					}
				}
				if !hasDir {
					sum.FTypes = append(sum.FTypes, uint8(FileTypeDir))
				}

				if debugEnabled() {
					debugf("SubtreeSummary: dir=%s added dir-with-files count=%d size+=%d", dir, dirCnt, dirCnt*DirectorySize)
				}
			}
		} else if debugEnabled() {
			debugf("DirCountWithFiles error: %v", derr)
		}
	}

	// Set Age based on time bucket filters for Phase 1
	sum.Age = ageBucketToDBAge(f.ATimeBucket, f.MTimeBucket)

	if debugEnabled() {
		debugf("SubtreeSummary: final size=%d count=%d age=%d ftypes=%v", sum.TotalSize, sum.FileCount, sum.Age, sum.FTypes)
	}

	return sum, nil
}

// ageBucketToDBAge maps filter bucket values to db.DirGUTAge constants.
// This implementation aligns with the semantics used in db/age.go.
func ageBucketToDBAge(aTimeBucket, mTimeBucket string) uint8 { //nolint:funlen
	// Default to "all" if no bucket specified
	if aTimeBucket == "" && mTimeBucket == "" {
		return uint8(db.DGUTAgeAll)
	}

	aTimeMap := map[string]db.DirGUTAge{
		"0d":  db.DGUTAgeAll,
		">1m": db.DGUTAgeA1M,
		">2m": db.DGUTAgeA2M,
		">6m": db.DGUTAgeA6M,
		">1y": db.DGUTAgeA1Y,
		">2y": db.DGUTAgeA2Y,
		">3y": db.DGUTAgeA3Y,
		">5y": db.DGUTAgeA5Y,
		">7y": db.DGUTAgeA7Y,
	}

	mTimeMap := map[string]db.DirGUTAge{
		">1m": db.DGUTAgeM1M,
		">2m": db.DGUTAgeM2M,
		">6m": db.DGUTAgeM6M,
		">1y": db.DGUTAgeM1Y,
		">2y": db.DGUTAgeM2Y,
		">3y": db.DGUTAgeM3Y,
		">5y": db.DGUTAgeM5Y,
		">7y": db.DGUTAgeM7Y,
	}

	if aTimeBucket != "" {
		if v, ok := aTimeMap[aTimeBucket]; ok {
			return uint8(v)
		}
	}

	if mTimeBucket != "" {
		if v, ok := mTimeMap[mTimeBucket]; ok {
			return uint8(v)
		}
	}

	return uint8(db.DGUTAgeAll)
}

// Helper function to retrieve summary from the rollups table.
// getRollupSummaryGlobal aggregates precomputed rollups across all mounts for the given ancestor path.
// getRollupSummaryGlobal is retained for future optimization phases using rollups.
func (c *Clickhouse) getRollupSummaryGlobal(ctx context.Context, dir string) (Summary, error) { //nolint:funlen,unused
	query := `
SELECT
  sum(total_size) AS total_size,
  sum(file_count) AS file_count,
  max(atime_max) AS most_recent_atime,
  min(atime_min) AS oldest_atime,
  max(mtime_max) AS most_recent_mtime,
  min(mtime_min) AS oldest_mtime,
  arrayReduce('groupUniqArray', arrayFlatten(groupArray(uids))) AS uids,
  arrayReduce('groupUniqArray', arrayFlatten(groupArray(gids))) AS gids,
  arrayReduce('groupUniqArray', arrayFlatten(groupArray(exts))) AS exts
FROM ancestor_rollups_current
WHERE ancestor = ?`

	if debugEnabled() {
		debugf("getRollupSummaryGlobal: dir=%s", dir)
	}

	row := c.conn.QueryRow(ctx, query, dir)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts); err != nil {
		if errors.Is(err, io.EOF) {
			return Summary{}, nil
		}

		return Summary{}, err
	}

	return s, nil
}

// Removed AllSubtreeSummary in favour of unified SubtreeSummary

// buildAllWhere constructs WHERE and args for all-mounts summary.
func buildAllWhere(dir string, f Filters) ([]string, []any) {
	where := []string{"path LIKE ?"}
	args := []any{dir + "%"}

	if len(f.GIDs) > 0 {
		gidClause, gidArgs := buildInClause("gid", f.GIDs)
		where = append(where, gidClause)
		args = append(args, gidArgs...)
	}

	if len(f.UIDs) > 0 {
		uidClause, uidArgs := buildInClause("uid", f.UIDs)
		where = append(where, uidClause)
		args = append(args, uidArgs...)
	}

	if len(f.Exts) > 0 {
		extClause, extArgs := buildExtensionClause(f.Exts)
		where = append(where, extClause)
		args = append(args, extArgs...)
	}

	if debugEnabled() {
		debugf("buildAllWhere: where=%v args=%v", where, args)
	}

	return where, args
}

// DirCountWithFiles returns the number of distinct directories that have at least
// one descendant file within the subtree rooted at dir, respecting filters.
// This mirrors Bolt semantics for augmenting directory counts and ensures that
// higher-level synthetic ancestors (e.g., "/lustre/") are included at the root.
func (c *Clickhouse) DirCountWithFiles(ctx context.Context, dir string, _ Filters) (uint64, error) { //nolint:funlen
	dir = EnsureDir(dir)

	// Count distinct ancestor directories (within the subtree rooted at 'dir')
	// that have at least one descendant file. This leverages the rollups table
	// which contains one row per (file, ancestor) pair with ext_low for files.
	// We restrict to the latest ready scan per mount to match current views.
	query := `
SELECT countDistinct(ancestor) AS dir_count
FROM ancestor_rollups_raw arr
INNER JOIN (
	SELECT mount_path, max(scan_id) AS scan_id
	FROM scans
	WHERE state = 'ready'
	GROUP BY mount_path
) r USING (mount_path, scan_id)
WHERE ancestor LIKE ? AND ext_low != ''
`

	args := []any{dir + "%"}

	if debugEnabled() {
		debugf("DirCountWithFiles SQL:\n%s", query)
		debugf("DirCountWithFiles args=%v", args)
	}

	row := c.conn.QueryRow(ctx, query, args...)

	var cnt uint64
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}

	if debugEnabled() {
		debugf("DirCountWithFiles: count=%d", cnt)
	}

	return cnt, nil
}

// getUnfilteredAllSummary is the unfiltered fast path across all mounts.
func (c *Clickhouse) getUnfilteredAllSummary(ctx context.Context, dir string) (Summary, error) { //nolint:unused
	// Use raw rollups restricted to this exact ancestor and real files only (ext_low != '').
	row := c.conn.QueryRow(ctx, `
SELECT
	sum(size) AS total_size,
	count() AS file_count,
	max(atime) AS most_recent_atime,
	min(atime) AS oldest_atime,
	max(mtime) AS most_recent_mtime,
	min(mtime) AS oldest_mtime,
	groupUniqArray(uid) AS uids,
	groupUniqArray(gid) AS gids,
	groupUniqArray(ext_low) AS exts,
	array() AS ftypes
FROM ancestor_rollups_raw arr
INNER JOIN (
	SELECT mount_path, max(scan_id) AS scan_id
	FROM scans
	WHERE state = 'ready'
	GROUP BY mount_path
) r USING (mount_path, scan_id)
WHERE ancestor = ? AND ext_low != ''
`, dir)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts, &s.FTypes,
	); err != nil {
		if errors.Is(err, io.EOF) {
			return Summary{}, nil
		}
		return Summary{}, err
	}

	if debugEnabled() {
		debugf("getUnfilteredAllSummary(%s): size=%d count=%d", dir, s.TotalSize, s.FileCount)
	}

	return s, nil
}

// buildGlobSearchQuery constructs a SQL query for searching paths with a glob pattern.
func buildGlobSearchQuery(caseInsensitive bool, limit int) string {
	// Build the query based on case sensitivity
	var query string
	if caseInsensitive {
		query = `SELECT path FROM fs_entries_current 
			WHERE lowerUTF8(path) LIKE lowerUTF8(?)`
	} else {
		query = `SELECT path FROM fs_entries_current 
			WHERE path LIKE ?`
	}

	// Add limit if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	return query
}

// SearchGlobPaths searches for paths matching a glob pattern in ClickHouse.
func (c *Clickhouse) SearchGlobPaths( //nolint:funlen
	ctx context.Context,
	globPattern string,
	limit int,
	caseInsensitive bool,
) ([]string, error) {
	// Build the query
	query := buildGlobSearchQuery(caseInsensitive, limit)

	// Convert glob pattern to SQL LIKE pattern
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	// Execute query
	if debugEnabled() {
		debugf("SearchGlobPaths: pattern=%s caseInsensitive=%v limit=%d", pattern, caseInsensitive, limit)
		debugf("SearchGlobPaths SQL: %s", query)
	}

	rows, err := c.conn.Query(ctx, query, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results - preallocate for better performance
	result := make([]string, 0, DefaultResultCapacity) // Start with a reasonable capacity
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}

		result = append(result, path)
	}

	if debugEnabled() {
		debugf("SearchGlobPaths: results=%d", len(result))
	}

	return result, rows.Err()
}
