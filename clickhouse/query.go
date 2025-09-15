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
	"fmt"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

// ListImmediateChildren returns direct children of a directory (global, all mounts).
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, dir string) ([]FileEntry, error) {
	dir = EnsureDir(dir)

	// Direct query without duplicate handling since we're guaranteed unique paths
	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, basename, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
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
			&entry.Path, &entry.ParentPath, &entry.Basename, &entry.Ext,
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

// buildGlobSearchQuery constructs a SQL query for searching paths with a glob pattern.
// buildGlobSearchQuery removed; we avoid LIKE on parent_path and instead
// apply range conditions with optional basename LIKE.

// prefixUpperBound returns an upper bound string for prefix range scans: [prefix, prefix\xFF).
// It appends a single 0xFF byte which is beyond all valid UTF-8 continuation bytes,
// ensuring all strings starting with the prefix are covered by the half-open interval.
func prefixUpperBound(prefix string) string {
	return prefix + string([]byte{0xFF})
}

// execPathQuery runs a simple single-column path query and collects results.
func (c *Clickhouse) execPathQuery(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// SearchGlobPaths searches for paths matching a glob pattern in ClickHouse using
// mount-constrained parent_path range scans. Case-insensitive globs are not supported.
func (c *Clickhouse) SearchGlobPaths(ctx context.Context, globPattern string, limit int) ([]string, error) {
	// We only support case-sensitive globs and optimise for prefix-based patterns.
	// Strategy:
	// 1) Determine the mount from the provided pattern and ensure we know mounts.
	// 2) Extract the parent_path prefix from the glob: everything up to the last '/'
	//    before a wildcard is used as the range prefix; range on parent_path.
	// 3) Constrain mount_path = ? so ClickHouse can prune partitions.
	// 4) Avoid LIKE on parent_path; use LIKE only on basename or full path if needed.
	if err := c.ensureMounts(ctx); err != nil {
		return nil, err
	}

	// Build list of mounts to query. If the pattern has a known mount prefix,
	// query just that mount; otherwise iterate all known mounts.
	mount := c.findMountForPrefix(globPattern)
	if mount == "" {
		// Unknown mount: refresh once (eg. first query after startup) and retry.
		if err := c.refreshMounts(ctx); err != nil {
			return nil, err
		}

		mount = c.findMountForPrefix(globPattern)
	}

	mounts := c.cachedMounts
	if mount != "" {
		mounts = []string{mount}
	}

	// Compose a per-mount absolute pattern from the input glob.
	compose := func(mnt string) string {
		if strings.HasPrefix(globPattern, "*/") {
			return mnt + strings.TrimPrefix(globPattern[2:], "/")
		}
		if strings.HasPrefix(globPattern, "/") {
			return mnt + strings.TrimPrefix(globPattern, "/")
		}

		return mnt + globPattern
	}

	seen := make(map[string]struct{}, 64)
	out := make([]string, 0, DefaultResultCapacity)

	for _, mnt := range mounts {
		if limit > 0 && len(out) >= limit {
			break
		}

		effective := compose(mnt)

		// If the original pattern begins with "*/", we cannot derive a stable
		// parent_path prefix. Fall back to a path LIKE constrained by mount.
		if strings.HasPrefix(globPattern, "*/") {
			// Build a path LIKE pattern from the suffix after the leading '*/',
			// matching at any depth under the mount: path LIKE CONCAT(mount, '%/', suffix)
			suffix := strings.TrimPrefix(globPattern[2:], "/")
			like := strings.ReplaceAll(strings.ReplaceAll(suffix, "*", "%"), "?", "_")
			q := `SELECT path FROM fs_entries_current WHERE mount_path = ? AND path LIKE ?`
			likeArg := mnt + "%/" + like
			if limit > 0 {
				perLimit := limit - len(out)
				if perLimit < 1 {
					break
				}

				q += fmt.Sprintf(" LIMIT %d", perLimit)
			}

			paths, err := c.execPathQuery(ctx, q, mnt, likeArg)
			if err != nil {
				return nil, err
			}
			for _, p := range paths {
				if _, ok := seen[p]; ok {
					continue
				}
				seen[p] = struct{}{}
				out = append(out, p)
			}

			continue
		}

		// Determine fixed portion up to first wildcard.
		wc := strings.IndexAny(effective, "*?")
		fixed := effective
		if wc >= 0 {
			fixed = effective[:wc]
		}

		// If the pattern has a slash following the first wildcard, it's a complex
		// multi-segment match (e.g., "/k*/tmp/*" or "/a/*/b/*.txt"). In this case,
		// fall back to a mount-constrained path LIKE which preserves the interior
		// directory semantics of the glob. This avoids losing segments like "/tmp/".
		if wc >= 0 && strings.Contains(effective[wc:], "/") {
			like := strings.ReplaceAll(strings.ReplaceAll(effective, "*", "%"), "?", "_")
			q := `SELECT path FROM fs_entries_current WHERE mount_path = ? AND path LIKE ?`
			if limit > 0 {
				perLimit := limit - len(out)
				if perLimit < 1 {
					break
				}

				q += fmt.Sprintf(" LIMIT %d", perLimit)
			}

			paths, err := c.execPathQuery(ctx, q, mnt, like)
			if err != nil {
				return nil, err
			}
			for _, p := range paths {
				if _, ok := seen[p]; ok {
					continue
				}
				seen[p] = struct{}{}
				out = append(out, p)
			}

			continue
		}

		// Ensure fixed covers at least the mount; if not, use the mount as prefix.
		if !strings.HasPrefix(fixed, mnt) {
			fixed = mnt
		}

		// Use the fixed portion directly as the parent_path range prefix (eg. '/k').
		parentPrefix := fixed

		// Range bounds for parent_path
		lo := parentPrefix
		hi := prefixUpperBound(parentPrefix)

		// Per-mount remaining limit
		perLimit := 0
		if limit > 0 {
			perLimit = limit - len(out)
			if perLimit < 1 {
				break
			}
		}

		if wc == -1 {
			// Exact path match
			q := `SELECT path FROM fs_entries_current
WHERE mount_path = ? AND parent_path >= ? AND parent_path < ? AND path = ?`
			if perLimit > 0 {
				q += fmt.Sprintf(" LIMIT %d", perLimit)
			}

			paths, err := c.execPathQuery(ctx, q, mnt, lo, hi, effective)
			if err != nil {
				return nil, err
			}

			for _, p := range paths {
				if _, ok := seen[p]; ok {
					continue
				}
				seen[p] = struct{}{}
				out = append(out, p)
			}

			continue
		}

		// Basename LIKE while keeping parent_path a range
		lastSlashGlobal := strings.LastIndex(effective, "/")
		basenameGlob := effective
		if lastSlashGlobal >= 0 {
			basenameGlob = effective[lastSlashGlobal+1:]
		}
		basenamePattern := strings.ReplaceAll(strings.ReplaceAll(basenameGlob, "*", "%"), "?", "_")
		if basenamePattern == "" {
			basenamePattern = "%"
		}

		// Add a path LIKE constraint to ensure matches stay under the fixed prefix
		pathLike := fixed + "%"
		q := `SELECT path FROM fs_entries_current
WHERE mount_path = ? AND parent_path >= ? AND parent_path < ? AND path LIKE ? AND basename LIKE ?`
		if perLimit > 0 {
			q += fmt.Sprintf(" LIMIT %d", perLimit)
		}

		paths, err := c.execPathQuery(ctx, q, mnt, lo, hi, pathLike, basenamePattern)
		if err != nil {
			return nil, err
		}

		for _, p := range paths {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}

	return out, nil
}
