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

// ListImmediateChildren returns direct children of a directory (global, all mounts).
// This supports navigation at '/', '/lustre/', etc., leveraging synthetic dirs.
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, dir string) ([]FileEntry, error) {
	dir = EnsureDir(dir)

	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
		FROM fs_entries_current
		WHERE parent_path = ?`,
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

// SubtreeSummary returns statistics for a subtree (global, all mounts), filtered by the given criteria.
func (c *Clickhouse) SubtreeSummary(ctx context.Context, dir string, f Filters) (Summary, error) {
	dir = EnsureDir(dir)

	if isNoFilters(f) {
		return c.getUnfilteredAllSummary(ctx, dir)
	}

	where, args := buildAllWhere(dir, f)
	bucketFilter := buildGlobalTimeBucketFilter(f)
	query := buildAllSummaryQuery(where, bucketFilter)

	return c.executeSummaryQuery(ctx, query, args)
}

// Helper function to check if no filters are applied.
func isNoFilters(f Filters) bool {
	return len(f.GIDs) == 0 && len(f.UIDs) == 0 &&
		len(f.Exts) == 0 && f.ATimeBucket == "" && f.MTimeBucket == ""
}

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
// Removed per-mount summary query builder (API unified to global)

// buildAllSummaryQuery builds the summary query across all mounts (no mount_path filter),
// using per-row scan time for any time buckets.
func buildAllSummaryQuery(where []string, bucketFilter string) string {
	return `
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

// OptimizedSubtreeSummary attempts to use precomputed ancestor rollups when possible.
// It falls back to the regular implementation for filtered queries.
func (c *Clickhouse) OptimizedSubtreeSummary(ctx context.Context, dir string, f Filters) (Summary, error) {
	if !isNoFilters(f) {
		return c.SubtreeSummary(ctx, dir, f)
	}

	return c.getRollupSummaryGlobal(ctx, EnsureDir(dir))
}

// Helper function to retrieve summary from the rollups table.
// getRollupSummaryGlobal aggregates precomputed rollups across all mounts for the given ancestor path.
func (c *Clickhouse) getRollupSummaryGlobal(ctx context.Context, dir string) (Summary, error) {
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

	return where, args
}

// getUnfilteredAllSummary is the unfiltered fast path across all mounts.
func (c *Clickhouse) getUnfilteredAllSummary(ctx context.Context, dir string) (Summary, error) {
	row := c.conn.QueryRow(ctx, `
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
func (c *Clickhouse) SearchGlobPaths(
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

	return result, rows.Err()
}
