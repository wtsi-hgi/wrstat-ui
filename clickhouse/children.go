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
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SummaryChild represents a summary for a single child directory.
// It mirrors the structure of Summary but includes the child's path.
type SummaryChild struct {
	Path       string
	ParentPath string
	Basename   string
	Summary    Summary
}

// ChildrenSummaries returns summaries for the immediate children of the given directory.
// This aggregates files and stats by parent_path and name for immediate children.
func (c *Clickhouse) ChildrenSummaries(
	ctx context.Context,
	dir string,
	f Filters,
) ([]SummaryChild, error) {
	dir = EnsureDir(dir)

	// We use a direct query approach instead of doing a summary for each child
	// to avoid multiple round trips for performance reasons
	query, args := buildChildrenSummariesQuery(dir, f)

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanChildrenSummaries(rows, dir, f)
}

// scanChildrenSummaries scans the rows and returns the children summaries.
func scanChildrenSummaries(rows driver.Rows, dir string, f Filters) ([]SummaryChild, error) {
	results := make([]SummaryChild, 0, DefaultResultCapacity)

	for rows.Next() {
		child, err := scanChildRow(rows, dir, f)
		if err != nil {
			return nil, err
		}

		results = append(results, child)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// scanChildRow scans a single row into a SummaryChild and applies age mapping.
func scanChildRow(rows driver.Rows, dir string, f Filters) (SummaryChild, error) {
	var (
		child                SummaryChild
		childPath, childBase string
	)

	if err := rows.Scan(
		&childPath,
		&childBase,
		&child.Summary.TotalSize,
		&child.Summary.FileCount,
		&child.Summary.MostRecentATime,
		&child.Summary.OldestATime,
		&child.Summary.MostRecentMTime,
		&child.Summary.OldestMTime,
		&child.Summary.UIDs,
		&child.Summary.GIDs,
		&child.Summary.Exts,
		&child.Summary.FTypes); err != nil {
		return SummaryChild{}, err
	}

	// Set Age based on time bucket filters (same as in SubtreeSummary)
	child.Summary.Age = ageBucketToDBAge(f.ATimeBucket, f.MTimeBucket)

	child.Path = childPath
	child.ParentPath = dir
	child.Basename = childBase

	return child, nil
}

// buildChildrenSummariesQuery builds a query to get summaries for immediate children directories.
func buildChildrenSummariesQuery(dir string, f Filters) (string, []any) {
	args, conditions := childrenQueryConditions(dir, f)
	query := childrenQuerySQL(strings.Join(conditions, " AND "), buildChildrenBucketFilter(f))

	// Constrain to directories only at the output stage
	args = append(args, uint8(FileTypeDir))

	return query, args
}

// childrenQueryConditions builds the WHERE clause conditions and corresponding args for the children query.
func childrenQueryConditions(dir string, f Filters) ([]any, []string) {
	args := []any{dir}
	conditions := []string{"parent_path = ?"}

	if len(f.GIDs) > 0 {
		gidClause, gidArgs := buildInClause("gid", f.GIDs)
		conditions = append(conditions, gidClause)
		args = append(args, gidArgs...)
	}

	if len(f.UIDs) > 0 {
		uidClause, uidArgs := buildInClause("uid", f.UIDs)
		conditions = append(conditions, uidClause)
		args = append(args, uidArgs...)
	}

	if len(f.Exts) > 0 {
		extClause, extArgs := buildExtensionClause(f.Exts)
		conditions = append(conditions, extClause)
		args = append(args, extArgs...)
	}

	return args, conditions
}

// childrenQuerySQL returns the SQL template for the children summaries query given WHERE and bucket filter strings.
const childrenQueryPrefix = `
SELECT
	children.path,
	children.basename,
	sum(children.size) AS total_size,
	count() AS file_count,
	max(children.atime) AS most_recent_atime,
	min(children.atime) AS oldest_atime,
	max(children.mtime) AS most_recent_mtime,
	min(children.mtime) AS oldest_mtime,
	groupUniqArray(children.uid) AS uids,
	groupUniqArray(children.gid) AS gids,
	groupUniqArray(children.ext_low) AS exts,
	groupUniqArray(children.ftype) AS ftypes
FROM (
	SELECT
		path,
		basename,
		parent_path,
		max(size) AS size,
		max(atime) AS atime,
		max(mtime) AS mtime,
		max(uid) AS uid,
		max(gid) AS gid,
		anyLast(ext_low) AS ext_low,
		anyLast(ftype) AS ftype
	FROM fs_entries_current
	WHERE `

const childrenQuerySuffix = `
	GROUP BY path, parent_path, basename
) AS children
WHERE children.ftype = ?
GROUP BY children.path, children.basename
ORDER BY children.basename
`

func childrenQuerySQL(whereClause, bucketFilter string) string {
	return childrenQueryPrefix + whereClause + bucketFilter + childrenQuerySuffix
}

// buildChildrenBucketFilter builds the time bucket filter used in buildChildrenSummariesQuery.
func buildChildrenBucketFilter(f Filters) string {
	if f.ATimeBucket == "" && f.MTimeBucket == "" {
		return ""
	}

	var predicates []string

	maybeAddBucketPred := func(expr, col, bucket string) {
		if bucket == "" {
			return
		}

		if pred, err := buildBucketPredicateWithScanExpr(expr, col, bucket); err == nil && pred != "" {
			predicates = append(predicates, pred)
		}
	}

	maybeAddBucketPred("scan_time", "atime", f.ATimeBucket)
	maybeAddBucketPred("scan_time", "mtime", f.MTimeBucket)

	if len(predicates) > 0 {
		return " AND (" + strings.Join(predicates, " AND ") + ")"
	}

	return ""
}
