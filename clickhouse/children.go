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
)

// SummaryChild represents a summary for a single child directory.
// It mirrors the structure of Summary but includes the child's path.
type SummaryChild struct {
	Path       string
	ParentPath string
	Name       string
	Summary    Summary
}

// ChildrenSummaries returns summaries for the immediate children of the given directory.
// This aggregates files and stats by parent_path and name for immediate children.
func (c *Clickhouse) ChildrenSummaries(ctx context.Context, dir string, f Filters) ([]SummaryChild, error) {
	dir = EnsureDir(dir)

	// We use a direct query approach instead of doing a summary for each child
	// to avoid multiple round trips for performance reasons
	query, args := buildChildrenSummariesQuery(dir, f)

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SummaryChild, 0, DefaultResultCapacity)

	for rows.Next() {
		var child SummaryChild
		var childPath, childName string

		// Scan the base fields
		if err := rows.Scan(
			&childPath,
			&childName,
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
			return nil, err
		}

		// Set Age based on time bucket filters (same as in SubtreeSummary)
		child.Summary.Age = ageBucketToDBAge(f.ATimeBucket, f.MTimeBucket)

		child.Path = childPath
		child.ParentPath = dir
		child.Name = childName

		results = append(results, child)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// buildChildrenSummariesQuery builds a query to get summaries for immediate children directories.
func buildChildrenSummariesQuery(dir string, f Filters) (string, []any) {
	// Start with the directory as the first parameter
	args := []any{dir}

	// Build the filter conditions
	var conditions []string

	// Always filter for immediate children of the given directory
	conditions = append(conditions, "parent_path = ?")

	// Add GID filters if provided
	if len(f.GIDs) > 0 {
		gidClause, gidArgs := buildInClause("gid", f.GIDs)
		conditions = append(conditions, gidClause)
		args = append(args, gidArgs...)
	}

	// Add UID filters if provided
	if len(f.UIDs) > 0 {
		uidClause, uidArgs := buildInClause("uid", f.UIDs)
		conditions = append(conditions, uidClause)
		args = append(args, uidArgs...)
	}

	// Add extension filters if provided
	if len(f.Exts) > 0 {
		extClause, extArgs := buildExtensionClause(f.Exts)
		conditions = append(conditions, extClause)
		args = append(args, extArgs...)
	}

	// Build the time bucket filter
	bucketFilter := ""
	if f.ATimeBucket != "" || f.MTimeBucket != "" {
		var predicates []string

		// Add atime bucket filter if provided
		if f.ATimeBucket != "" {
			if pred, err := buildBucketPredicateWithScanExpr("scan_time", "atime", f.ATimeBucket); err == nil && pred != "" {
				predicates = append(predicates, pred)
			}
		}

		// Add mtime bucket filter if provided
		if f.MTimeBucket != "" {
			if pred, err := buildBucketPredicateWithScanExpr("scan_time", "mtime", f.MTimeBucket); err == nil && pred != "" {
				predicates = append(predicates, pred)
			}
		}

		if len(predicates) > 0 {
			bucketFilter = " AND " + "(" + strings.Join(predicates, " AND ") + ")"
		}
	}

	// Build the complete SQL query
	query := `
SELECT
	children.path,
	children.name,
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
		name,
		parent_path,
		max(size) AS size,
		max(atime) AS atime,
		max(mtime) AS mtime,
		max(uid) AS uid,
		max(gid) AS gid,
		anyLast(ext_low) AS ext_low,
		anyLast(ftype) AS ftype
	FROM fs_entries_current
	WHERE ` + strings.Join(conditions, " AND ") + bucketFilter + `
	GROUP BY path, parent_path, name
) AS children
WHERE children.ftype = ?
GROUP BY children.path, children.name
ORDER BY children.name
`

	// Constrain to directories only at the output stage
	args = append(args, uint8(FileTypeDir))

	return query, args
}
