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
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, dir string) ([]FileEntry, error) {
	dir = EnsureDir(dir)

	// Direct query without duplicate handling since we're guaranteed unique paths
	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
		FROM fs_entries_current
		WHERE parent_path = ? AND path != '/'`,
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

	return results, rows.Err()
}

// buildBucketPredicateWithScanExpr builds a time bucket predicate using the provided scan time expression.
// scanExpr should evaluate to a DateTime, e.g. "toDateTime(max_scan)" or "scan_time".
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
// filtered by the given criteria.
func (c *Clickhouse) subtreeSummaryScan(ctx context.Context, dir string, f Filters) (Summary, error) {
	dir = EnsureDir(dir)
	where, args := buildAllWhere(dir, f)
	bucketFilter := buildGlobalTimeBucketFilter(f)

	query := buildAllSummaryQuery(where, bucketFilter)

	return c.executeSummaryQuery(ctx, query, args)
}

// Helper function to check if no filters are applied.
// isNoFilters checks if no filters are applied.
// Note: This is unused but kept for documentation purposes.
// It would be used if getUnfilteredAllSummary was integrated as a fast path.
func isNoFilters(f Filters) bool {
	return len(f.UIDs) == 0 &&
		len(f.GIDs) == 0 &&
		len(f.Exts) == 0 &&
		f.ATimeBucket == "" &&
		f.MTimeBucket == ""
}

// buildInClause constructs an SQL IN clause for filtering.
func buildInClause(field string, values []uint32) (string, []any) {
	placeholders := make([]string, len(values))
	args := make([]any, len(values))

	for i, v := range values {
		placeholders[i] = "?"
		args[i] = v
	}

	return fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")), args
}

// buildExtensionClause constructs an SQL IN clause for file extensions.
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

// buildGlobalTimeBucketFilter builds time bucket filters using per-row scan time.
func buildGlobalTimeBucketFilter(f Filters) string {
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
		add("scan_time", "atime", f.ATimeBucket)
	}

	if f.MTimeBucket != "" {
		add("scan_time", "mtime", f.MTimeBucket)
	}

	return joinTimeFilters(preds)
}

// Build the complete summary query.
// Removed per-mount summary query builder (API unified to global)

// buildAllSummaryQuery builds the summary query across all mounts,
// using per-row scan time for any time buckets.
func buildAllSummaryQuery(where []string, bucketFilter string) string {
	// To match Bolt behaviour, we need to:
	// 1. Deduplicate directory entries by normalising paths
	// 2. Count only real entries, not duplicates from path expansion
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

// executeSummaryQuery executes a summary query and returns the results.
func (c *Clickhouse) executeSummaryQuery(ctx context.Context, query string, args []any) (Summary, error) {
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

	return s, nil
}

// SubtreeSummary provides statistics for a directory subtree, including
// file counts, sizes, and time-based statistics.
// Directories with files are included in the count and size calculations.
func (c *Clickhouse) SubtreeSummary(ctx context.Context, dir string, f Filters) (Summary, error) {
	// Get base statistics from files
	sum, err := c.subtreeSummaryScan(ctx, dir, f)
	if err != nil {
		return Summary{}, err
	}

	// Augment with directory-with-files contributions to match Bolt semantics:
	// For each distinct directory that directly contains at least one file
	// within the subtree, add +1 to count and +DirectorySize to size.
	dirCnt, derr := c.DirCountWithFiles(ctx, dir, f)
	if derr == nil {
		sum.FileCount += dirCnt
		sum.TotalSize += dirCnt * DirectorySize

		// Ensure 'dir' appears in file types if directories exist
		if dirCnt > 0 && !containsFileType(sum.FTypes, uint8(FileTypeDir)) {
			sum.FTypes = append(sum.FTypes, uint8(FileTypeDir))
		}
	}

	// Set Age based on time bucket filters for Phase 1
	sum.Age = ageBucketToDBAge(f.ATimeBucket, f.MTimeBucket)

	return sum, nil
}

// containsFileType checks if a slice of file types contains a specific type.
func containsFileType(types []uint8, fileType uint8) bool {
	for _, ft := range types {
		if ft == fileType {
			return true
		}
	}

	return false
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

// Removed AllSubtreeSummary in favour of unified SubtreeSummary

// buildAllWhere constructs WHERE conditions and args for all-mounts summary.
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

	return where, args
}

// DirCountWithFiles returns the number of distinct directories that have at least
// one descendant file within the subtree rooted at dir, respecting filters.
// This mirrors Bolt semantics for augmenting directory counts.
func (c *Clickhouse) DirCountWithFiles(ctx context.Context, dir string, f Filters) (uint64, error) {
	dir = EnsureDir(dir)

	// Count distinct ancestor directories (within the subtree rooted at 'dir')
	// that have at least one descendant file.
	base := `
SELECT countDistinct(ancestor) AS dir_count
FROM ancestor_rollups_raw arr
INNER JOIN (
	SELECT mount_path, argMax(scan_id, finished_at) AS scan_id
	FROM scans
	WHERE state = 'ready'
	GROUP BY mount_path
) r USING (mount_path, scan_id)
WHERE ancestor LIKE ? AND ext_low != ''`

	// Build optional filters
	where := []string{base}
	args := []any{dir + "%"}

	if len(f.GIDs) > 0 {
		clause, a := buildInClause("gid", f.GIDs)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}

	if len(f.UIDs) > 0 {
		clause, a := buildInClause("uid", f.UIDs)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}

	if len(f.Exts) > 0 {
		clause, a := buildExtensionClause(f.Exts)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}

	// Time bucket filters against per-row scan_time
	if pred, err := buildBucketPredicateWithScanExpr("scan_time", "atime", f.ATimeBucket); err == nil && pred != "" {
		where = append(where, "AND "+pred)
	}

	if pred, err := buildBucketPredicateWithScanExpr("scan_time", "mtime", f.MTimeBucket); err == nil && pred != "" {
		where = append(where, "AND "+pred)
	}

	query := strings.Join(where, " ")
	row := c.conn.QueryRow(ctx, query, args...)

	var cnt uint64
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}

	return cnt, nil
}

// getUnfilteredAllSummary is an unimplemented fast path across all mounts.
// Note: This function is not currently used in production code but is kept
// as a reference for a potential optimization for unfiltered queries.
// Tests show that integrating this function would require adapting it to
// match the current implementation's behavior and expectations.
func (c *Clickhouse) getUnfilteredAllSummary(ctx context.Context, dir string) (Summary, error) {
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
	SELECT mount_path, argMax(scan_id, finished_at) AS scan_id
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

	return s, nil
}

// buildGlobSearchQuery constructs a SQL query for searching paths with a glob pattern.
func buildGlobSearchQuery(caseInsensitive bool, limit int) string {
	var query string
	if caseInsensitive {
		query = `SELECT path FROM fs_entries_current 
			WHERE lowerUTF8(path) LIKE lowerUTF8(?)`
	} else {
		query = `SELECT path FROM fs_entries_current 
			WHERE path LIKE ?`
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	return query
}

// SearchGlobPaths searches for paths matching a glob pattern in ClickHouse.
func (c *Clickhouse) SearchGlobPaths(
	ctx context.Context,
	globPattern string,
	limit int,
	caseInsensitive bool,
) ([]string, error) {
	// Convert glob pattern to SQL LIKE pattern
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	// Build and execute query
	query := buildGlobSearchQuery(caseInsensitive, limit)

	rows, err := c.conn.Query(ctx, query, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results
	result := make([]string, 0, DefaultResultCapacity)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}

		result = append(result, path)
	}

	return result, rows.Err()
}
