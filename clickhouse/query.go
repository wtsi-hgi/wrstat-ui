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
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

var errGlobMustStartWithSlash = errors.New("glob must start with '/'")

// ListImmediateChildren returns direct children of a directory (global, all mounts).
func (c *Clickhouse) ListImmediateChildren(ctx context.Context, dir string) ([]FileEntry, error) {
	dir = EnsureDir(dir)

	// Direct query without duplicate handling since we're guaranteed unique paths
	rows, err := c.conn.Query(ctx, `
		SELECT path, parent_path, basename, depth, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime
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
			&entry.Path, &entry.ParentPath, &entry.Basename, &entry.Depth, &entry.Ext,
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
	// To match Bolt behaviour, we need to deduplicate directory entries
	// by normalising paths and count only real entries.
	return allSummaryQueryPrefix + strings.Join(where, " AND ") + bucketFilter + allSummaryQuerySuffix
}

const allSummaryQueryPrefix = `
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
	WHERE `

// allSummaryQuerySuffix uses ftype FileTypeFile (1).
const allSummaryQuerySuffix = `
	AND ftype = 1
GROUP BY norm_path
)
`

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
	query, args := buildDirCountQuery(dir, f)

	row := c.conn.QueryRow(ctx, query, args...)

	var cnt uint64
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}

	return cnt, nil
}

// buildDirCountQuery constructs the query and args for DirCountWithFiles.
func buildDirCountQuery(dir string, f Filters) (string, []any) { //nolint:funlen
	base := `
SELECT countDistinct(ancestor) AS dir_count
FROM ancestor_rollups_current arr
WHERE ancestor LIKE ?`

	where := []string{base}
	args := []any{dir + "%"}

	// Adapt filters: arrays contain distinct values per directory summary
	if len(f.Exts) > 0 {
		clause, a := buildArrayAnyClause("exts", f.Exts)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}
	if len(f.GIDs) > 0 {
		clause, a := buildArrayAnyUIntClause("gids", f.GIDs)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}
	if len(f.UIDs) > 0 {
		clause, a := buildArrayAnyUIntClause("uids", f.UIDs)
		where = append(where, "AND "+clause)
		args = append(args, a...)
	}

	// Time bucket predicates use oldest/newest times; approximate using any overlap
	// Reuse existing predicate builder on oldest/newest atime/mtime if needed (future enhancement)

	query := strings.Join(where, " ")

	return query, args
}

// buildArrayAnyClause builds a clause checking any string in array column matches provided list.
func buildArrayAnyClause(col string, vals []string) (string, []any) {
	placeholders := make([]string, len(vals))
	args := make([]any, len(vals))
	for i, v := range vals {
		placeholders[i] = "?"
		args[i] = v
	}

	return fmt.Sprintf(
		"arrayExists(x -> x IN (%s), %s)",
		strings.Join(placeholders, ","),
		col,
	), args
}

// buildArrayAnyUIntClause builds a clause checking any uint value in array column matches provided list.
func buildArrayAnyUIntClause[T ~uint32 | ~uint64](col string, vals []T) (string, []any) { //nolint:ireturn
	placeholders := make([]string, len(vals))
	args := make([]any, len(vals))
	for i, v := range vals {
		placeholders[i] = "?"
		args[i] = v
	}

	return fmt.Sprintf(
		"arrayExists(x -> x IN (%s), %s)",
		strings.Join(placeholders, ","),
		col,
	), args
}

// appendInClauses adds IN/extension clauses for gid, uid and ext filters.
func appendInClauses(where []string, args []any, f Filters) ([]string, []any) {
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

	return where, args
}

// appendBucketPredicates adds time-bucket predicates to the WHERE clause.
func appendBucketPredicates(where []string, f Filters) []string {
	if pred, err := buildBucketPredicateWithScanExpr("scan_time", "atime", f.ATimeBucket); err == nil && pred != "" {
		where = append(where, "AND "+pred)
	}

	if pred, err := buildBucketPredicateWithScanExpr("scan_time", "mtime", f.MTimeBucket); err == nil && pred != "" {
		where = append(where, "AND "+pred)
	}

	return where
}

// SearchGlobPaths executes a glob query against fs_entries.
// Semantics: "*" matches recursively at any depth.
func (c *Clickhouse) SearchGlobPaths(ctx context.Context, glob string, limit int) ([]string, error) {
	mount, rel, err := c.resolveMountAndRel(ctx, glob)
	if err != nil {
		return nil, err
	}

	// No mount found
	if mount == "" {
		return nil, nil
	}

	// Case 1: no wildcards → exact match
	if !strings.ContainsAny(rel, "*?") {
		// rel currently is the raw path relative to mount, but searchGlobExact
		// expects the full glob path, so rebuild it.
		return c.searchGlobExact(ctx, mount, glob, limit)
	}

	// Trim any leading slash for classifier
	rel = strings.TrimPrefix(rel, "/")

	switch classifyGlob(rel) {
	case globExt:
		return c.searchGlobByExt(ctx, mount, rel, limit)
	case globPrefix:
		return c.searchGlobByPrefix(ctx, mount, rel, limit)
	case globDepth:
		return c.searchGlobByDepth(ctx, mount, rel, limit)
	default:
		return c.searchGlobByLike(ctx, mount, rel, limit)
	}
}

// resolveMountAndRel encapsulates initial validation and mount resolution for a glob.
func (c *Clickhouse) resolveMountAndRel(ctx context.Context, glob string) (mount string, rel string, err error) {
	if !strings.HasPrefix(glob, "/") {
		return "", "", errGlobMustStartWithSlash
	}

	if err := c.ensureMounts(ctx); err != nil {
		return "", "", err
	}

	mount = c.findMountForPrefix(glob)
	if mount == "" {
		return "", "", nil
	}

	relRaw := strings.TrimPrefix(glob, mount)

	return mount, relRaw, nil
}

type globKind int

const (
	globUnknown globKind = iota
	globExact
	globExt
	globPrefix
	globDepth
	globLike
)

// classifyGlob returns a kind describing the glob pattern for optimised handling.
//
//nolint:gocyclo
func classifyGlob(rel string) globKind {
	if rel == "" {
		return globLike
	}

	// If last segment is "*.ext" and contains no '?', it's an extension pattern.
	parts := strings.Split(rel, "/")

	last := parts[len(parts)-1]
	if strings.HasPrefix(last, "*.") && !strings.ContainsAny(last, "?") {
		return globExt
	}

	// Simple trailing '*' with single '*' and no '?' is a prefix scan.
	if !strings.ContainsAny(rel, "?") && strings.Count(rel, "*") == 1 && strings.HasSuffix(rel, "*") {
		return globPrefix
	}

	// If glob contains any '*' (not just trailing), prefer depth-optimised path.
	if strings.Contains(rel, "*") {
		return globDepth
	}

	return globLike
}

// helper: exact match
func (c *Clickhouse) searchGlobExact(ctx context.Context, mount, glob string, limit int) ([]string, error) {
	q := `SELECT path FROM fs_entries_current
		  WHERE mount_path = ? AND path = ? ORDER BY path`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	return c.execPathQuery(ctx, q, mount, glob)
}

// helper: extension optimisation returns (paths, matched, err)
func (c *Clickhouse) searchGlobByExt(ctx context.Context, mount, rel string, limit int) ([]string, error) {
	parts := strings.Split(rel, "/")
	if len(parts) == 0 {
		return nil, nil
	}

	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "*.") || strings.ContainsAny(last, "?") {
		return nil, nil
	}

	prefix := mount
	if len(parts) > 1 {
		prefix += strings.Join(parts[:len(parts)-1], "/") + "/"
	}

	ext := strings.ToLower(strings.TrimPrefix(last, "*."))

	q := `SELECT path FROM fs_entries_current
		  WHERE mount_path = ?
			AND path >= ?
			AND path < ?
			AND ext_low = ?
		  ORDER BY path`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	return c.execPathQuery(ctx, q, mount, prefix, prefixUpperBound(prefix), ext)
}

// helper: trailing prefix star optimisation returns (paths, matched, err)
func (c *Clickhouse) searchGlobByPrefix(ctx context.Context, mount, rel string, limit int) ([]string, error) {
	if strings.ContainsAny(rel, "?") || strings.Count(rel, "*") != 1 || !strings.HasSuffix(rel, "*") {
		return nil, nil
	}

	literal := strings.TrimSuffix(rel, "*")
	var prefix string
	if literal == "" {
		prefix = mount
	} else {
		prefix = mount + literal
	}

	var q string
	if literal == "" {
		q = `SELECT path FROM fs_entries_current
					WHERE mount_path = ?
						AND path >= ?
						AND path < ?
					ORDER BY path`
	} else {
		q = `SELECT path FROM fs_entries_current
					WHERE mount_path = ?
						AND path > ?
						AND path < ?
					ORDER BY path`
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	return c.execPathQuery(ctx, q, mount, prefix, prefixUpperBound(prefix))
}

// helper: anchored multi-segment glob using depth optimisation
func (c *Clickhouse) searchGlobByDepth(ctx context.Context, mount, rel string, limit int) ([]string, error) {
	baseDepth := strings.Count(strings.Trim(mount, "/"), "/")
	segCount := strings.Count(strings.Trim(rel, "/"), "/") + 1
	targetDepth := baseDepth + segCount

	like := globToLike(mount + rel)

	q := `SELECT path FROM fs_entries_current
		  WHERE mount_path = ?
			AND depth >= ?
			AND path LIKE ?
		  ORDER BY path`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	return c.execPathQuery(ctx, q, mount, targetDepth, like)
}

// helper: fallback full LIKE
func (c *Clickhouse) searchGlobByLike(ctx context.Context, mount, rel string, limit int) ([]string, error) {
	like := globToLike(mount + rel)

	q := `SELECT path FROM fs_entries_current
		  WHERE mount_path = ? AND path LIKE ?
		  ORDER BY path`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	return c.execPathQuery(ctx, q, mount, like)
}

// prefixUpperBound returns an upper bound string for prefix range scans: [prefix, prefix\xFF).
// It appends a single 0xFF byte which is beyond all valid UTF-8 continuation bytes,
// ensuring all strings starting with the prefix are covered by the half-open interval.
func prefixUpperBound(prefix string) string {
	return prefix + string([]byte{0xFF})
}

// globToLike converts glob syntax to SQL LIKE.
func globToLike(glob string) string {
	var b strings.Builder

	for _, r := range glob {
		switch r {
		case '*':
			b.WriteRune('%')
		case '?':
			b.WriteRune('_')
		case '%', '_':
			b.WriteRune('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}

	return b.String()
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
