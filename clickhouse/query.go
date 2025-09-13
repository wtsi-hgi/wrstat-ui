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
)

// ListImmediateChildren returns direct children of a directory.
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, mountPath, dir string) ([]FileEntry, error) {
	// Ensure the directory path ends with a slash
	dir = EnsureDir(dir)

	// Query direct children of the directory
	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
		FROM fs_entries_current
		WHERE mount_path = ? AND parent_path = ?`,
		mountPath, dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect results - preallocate for better performance
	results := make([]FileEntry, 0, DefaultResultCapacity) // Start with a reasonable capacity
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

// buildBucketPredicate defaults to using toDateTime(max_scan) for backward compatibility (per-mount).
func buildBucketPredicate(col, bucket string) (string, error) {
	return buildBucketPredicateWithScanExpr("toDateTime(max_scan)", col, bucket)
}

// SubtreeSummary returns statistics for a subtree, filtered by the given criteria.
func (c *Clickhouse) SubtreeSummary(ctx context.Context, mountPath, dir string, f Filters) (Summary, error) {
	dir = EnsureDir(dir)

	// If no mount is specified (or the root dir is requested), aggregate across all mounts
	if mountPath == "" || dir == "/" {
		return c.AllSubtreeSummary(ctx, dir, f)
	}

	// Fast path when no filters other than path/mount.
	if isNoFilters(f) {
		return c.getUnfilteredSummary(ctx, mountPath, dir)
	}

	// Build query for filtered results
	where, args := buildBasicWhereClause(mountPath, dir)
	where, args = appendFilterClauses(where, args, f)

	// Build time bucket filter
	bucketFilter := buildTimeBucketFilter(f)

	// Construct the full query with CTE for max scan_id
	query := buildSummaryQuery(where, bucketFilter)

	// Add mountPath as the first argument for the CTE
	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, mountPath)
	allArgs = append(allArgs, args...)

	// Execute query and scan result
	return c.executeSummaryQuery(ctx, query, allArgs)
}

// Helper function to check if no filters are applied.
func isNoFilters(f Filters) bool {
	return len(f.GIDs) == 0 && len(f.UIDs) == 0 &&
		len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == ""
}

// Helper function for getting unfiltered summary.
func (c *Clickhouse) getUnfilteredSummary(ctx context.Context, mountPath, dir string) (Summary, error) {
	row := c.conn.QueryRow(ctx, `
WITH (SELECT max(scan_id) FROM scans WHERE mount_path = ? AND state = 'ready') AS max_scan
SELECT 
  sum(size),
  count(),
  max(atime),
  min(atime),
  max(mtime),
  min(mtime),
  groupUniqArray(uid),
  groupUniqArray(gid),
  groupUniqArray(ext_low)
FROM fs_entries_current
WHERE mount_path = ? AND path LIKE ?`, mountPath, mountPath, dir+"%")

	var s Summary
	if err := row.Scan(&s.TotalSize, &s.FileCount, &s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime, &s.UIDs, &s.GIDs, &s.Exts); err != nil {
		return Summary{}, err
	}

	return s, nil
}

// Build basic where clause and args for the query.
func buildBasicWhereClause(mountPath, dir string) ([]string, []any) {
	where := []string{"mount_path = ?", "path LIKE ?"}
	args := []any{mountPath, dir + "%"}

	return where, args
}

// Append filter clauses based on the provided filters.
func appendFilterClauses(where []string, args []any, f Filters) ([]string, []any) {
	// Add filter for GIDs if provided
	if len(f.GIDs) > 0 {
		gidClause, gidArgs := buildInClause("gid", f.GIDs)
		where = append(where, gidClause)
		args = append(args, gidArgs...)
	}

	// Add filter for UIDs if provided
	if len(f.UIDs) > 0 {
		uidClause, uidArgs := buildInClause("uid", f.UIDs)
		where = append(where, uidClause)
		args = append(args, uidArgs...)
	}

	// Add filter for extensions if provided
	if len(f.Exts) > 0 {
		extClause, extArgs := buildExtensionClause(f.Exts)
		where = append(where, extClause)
		args = append(args, extArgs...)
	}

	return where, args
}

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

// buildAccessTimeFilter builds the access time filter predicate.
func buildAccessTimeFilter(aTimeBucket string) string {
	if aTimeBucket == "" {
		return ""
	}

	atPred, err := buildBucketPredicate("atime", aTimeBucket)
	if err != nil || atPred == "" {
		return ""
	}

	return atPred
}

// buildModificationTimeFilter builds the modification time filter predicate.
func buildModificationTimeFilter(mTimeBucket string) string {
	if mTimeBucket == "" {
		return ""
	}

	mtPred, err := buildBucketPredicate("mtime", mTimeBucket)
	if err != nil || mtPred == "" {
		return ""
	}

	return mtPred
}

// joinTimeFilters combines multiple time filter predicates.
func joinTimeFilters(predicates []string) string {
	if len(predicates) == 0 {
		return ""
	}

	return " AND (" + strings.Join(predicates, " AND ") + ")"
}

// buildTimeBucketFilter builds time bucket filter clauses.
func buildTimeBucketFilter(f Filters) string {
	if f.ATimeBucket == "" && f.MTimeBucket == "" {
		return ""
	}

	var predicates []string

	// Access time filter
	if atPred := buildAccessTimeFilter(f.ATimeBucket); atPred != "" {
		predicates = append(predicates, atPred)
	}

	// Modification time filter
	if mtPred := buildModificationTimeFilter(f.MTimeBucket); mtPred != "" {
		predicates = append(predicates, mtPred)
	}

	return joinTimeFilters(predicates)
}

// buildGlobalTimeBucketFilter builds time bucket filters using per-row scan time (toDateTime(scan_id)).
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
		add("toDateTime(scan_id)", "atime", f.ATimeBucket)
	}

	if f.MTimeBucket != "" {
		add("toDateTime(scan_id)", "mtime", f.MTimeBucket)
	}

	return joinTimeFilters(preds)
}

// Build the complete summary query.
func buildSummaryQuery(where []string, bucketFilter string) string {
	return `
WITH (SELECT max(scan_id) FROM scans WHERE mount_path = ? AND state = 'ready') AS max_scan
SELECT 
  sum(size) AS total_size,
	count() AS file_count,
  max(atime) AS most_recent_atime,
  min(atime) AS oldest_atime,
  max(mtime) AS most_recent_mtime,
  min(mtime) AS oldest_mtime,
  groupUniqArray(uid) AS uids,
  groupUniqArray(gid) AS gids,
  groupUniqArray(ext_low) AS exts
FROM fs_entries_current
WHERE ` + strings.Join(where, " AND ") + bucketFilter
}

// buildAllSummaryQuery builds the summary query across all mounts (no mount_path filter),
// using per-row scan time for any time buckets.
func buildAllSummaryQuery(where []string, bucketFilter string) string {
	return `
SELECT 
	sumIf(size, NOT endsWith(path, '/')) AS total_size,
	countIf(NOT endsWith(path, '/')) AS file_count,
  max(atime) AS most_recent_atime,
  min(atime) AS oldest_atime,
  max(mtime) AS most_recent_mtime,
  min(mtime) AS oldest_mtime,
  groupUniqArray(uid) AS uids,
  groupUniqArray(gid) AS gids,
  groupUniqArray(ext_low) AS exts
FROM fs_entries_current
WHERE ` + strings.Join(where, " AND ") + bucketFilter
}

// Execute the summary query and return results.
func (c *Clickhouse) executeSummaryQuery(ctx context.Context, query string, args []any) (Summary, error) {
	row := c.conn.QueryRow(ctx, query, args...)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts); err != nil {
		return Summary{}, err
	}

	return s, nil
}

// OptimizedSubtreeSummary attempts to use precomputed ancestor rollups when
// possible. Falls back to the regular implementation for filtered queries.
func (c *Clickhouse) OptimizedSubtreeSummary(ctx context.Context, mountPath, dir string, f Filters) (Summary, error) {
	// Only use rollups when there are no filters at all
	if !isNoFilters(f) {
		// Fall back to regular implementation for filtered queries
		return c.SubtreeSummary(ctx, mountPath, dir, f)
	}

	// Use precomputed rollups
	return c.getRollupSummary(ctx, mountPath, EnsureDir(dir))
}

// Helper function to retrieve summary from the rollups table.
func (c *Clickhouse) getRollupSummary(ctx context.Context, mountPath, dir string) (Summary, error) {
	query := `
	SELECT 
		total_size,
		file_count,
		atime_max AS most_recent_atime,
		atime_min AS oldest_atime,
		mtime_max AS most_recent_mtime,
		mtime_min AS oldest_mtime,
		uids,
		gids,
		exts
	FROM ancestor_rollups_current
	WHERE mount_path = ? AND ancestor = ?`

	row := c.conn.QueryRow(ctx, query, mountPath, dir)

	var s Summary
	if err := row.Scan(
		&s.TotalSize, &s.FileCount,
		&s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime,
		&s.UIDs, &s.GIDs, &s.Exts); err != nil {
		// If no rows or other error, return empty summary (not an error condition)
		if errors.Is(err, io.EOF) {
			return Summary{}, nil
		}

		return Summary{}, err
	}

	return s, nil
}

// AllSubtreeSummary returns statistics for a subtree across all mounts.
// It aggregates by path prefix without restricting to a specific mount.
// FileCount counts only files (ftype = 1), and bucket filters are relative to each row's scan time.
func (c *Clickhouse) AllSubtreeSummary(ctx context.Context, dir string, f Filters) (Summary, error) {
	dir = EnsureDir(dir)

	if isNoFilters(f) {
		return c.getUnfilteredAllSummary(ctx, dir)
	}

	where, args := buildAllWhere(dir, f)
	bucketFilter := buildGlobalTimeBucketFilter(f)
	query := buildAllSummaryQuery(where, bucketFilter)

	return c.executeSummaryQuery(ctx, query, args)
}

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

	return where, args
}

// getUnfilteredAllSummary is the unfiltered fast path across all mounts.
func (c *Clickhouse) getUnfilteredAllSummary(ctx context.Context, dir string) (Summary, error) {
	row := c.conn.QueryRow(ctx, `
SELECT 
	sumIf(size, NOT endsWith(path, '/')),
	countIf(NOT endsWith(path, '/')),
  max(atime),
  min(atime),
  max(mtime),
  min(mtime),
  groupUniqArray(uid),
  groupUniqArray(gid),
  groupUniqArray(ext_low)
FROM fs_entries_current
WHERE path LIKE ?`, dir+"%")

	var s Summary
	if err := row.Scan(&s.TotalSize, &s.FileCount, &s.MostRecentATime, &s.OldestATime,
		&s.MostRecentMTime, &s.OldestMTime, &s.UIDs, &s.GIDs, &s.Exts); err != nil {
		return Summary{}, err
	}

	return s, nil
}

// buildGlobSearchQuery constructs a SQL query for searching paths with a glob pattern.
func buildGlobSearchQuery(caseInsensitive bool, limit int) string {
	// Build the query based on case sensitivity
	var query string
	if caseInsensitive {
		query = `SELECT path FROM fs_entries_current 
			WHERE mount_path = ? AND lowerUTF8(path) LIKE lowerUTF8(?)`
	} else {
		query = `SELECT path FROM fs_entries_current 
			WHERE mount_path = ? AND path LIKE ?`
	}

	// Add limit if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	return query
}

// SearchGlobPaths searches for paths matching a glob pattern in ClickHouse.
func (c *Clickhouse) SearchGlobPaths(
	ctx context.Context,
	mountPath, globPattern string,
	limit int,
	caseInsensitive bool,
) ([]string, error) {
	// Build the query
	query := buildGlobSearchQuery(caseInsensitive, limit)

	// Convert glob pattern to SQL LIKE pattern
	pattern := strings.ReplaceAll(strings.ReplaceAll(globPattern, "*", "%"), "?", "_")

	// Execute query
	rows, err := c.conn.Query(ctx, query, mountPath, pattern)
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

	return result, rows.Err()
}
