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

package server

import (
	"context"
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

// debugClickHouseEntries prints all entries in the ClickHouse database for a given path.
// This is used for debugging during tests to understand discrepancies between
// expected and actual entries.
func (s *Server) debugClickHouseEntries(c *gin.Context, path string) {
	// Only run this in test mode
	if os.Getenv("WRSTAT_USE_CLICKHOUSE") != "1" {
		return
	}

	// Get the ClickHouse connection
	ch, err := s.getClickHouse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting ClickHouse connection: %v\n", err)

		return
	}

	// Use ListImmediateChildren to get all entries
	entries, err := ch.ListImmediateChildren(context.Background(), path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing entries: %v\n", err)

		return
	}

	fmt.Fprintf(os.Stderr, "\n==== DEBUG: IMMEDIATE CHILDREN OF %s ====\n", path)
	fmt.Fprintf(os.Stderr, "Total entries: %d\n", len(entries))

	for i, entry := range entries {
		fmt.Fprintf(os.Stderr, "Entry %d: path=%s, parent=%s, name=%s, ext=%s, ftype=%d, size=%d, uid=%d, gid=%d\n",
			i+1, entry.Path, entry.ParentPath, entry.Name, entry.Ext, entry.FType, entry.Size, entry.UID, entry.GID)
	}

	// Get subtree summary to see file count and size
	fmt.Fprintf(os.Stderr, "\n==== DEBUG: SUBTREE SUMMARY FOR %s ====\n", path)
	sum, err := ch.SubtreeSummary(context.Background(), path, clickhouse.Filters{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting subtree summary: %v\n", err)

		return
	}

	fmt.Fprintf(os.Stderr, "FileCount: %d, TotalSize: %d\n", sum.FileCount, sum.TotalSize)
	fmt.Fprintf(os.Stderr, "UIDs: %v\n", sum.UIDs)
	fmt.Fprintf(os.Stderr, "GIDs: %v\n", sum.GIDs)
	fmt.Fprintf(os.Stderr, "Extensions: %v\n", sum.Exts)
	fmt.Fprintf(os.Stderr, "File Types: %v\n", sum.FTypes)
	fmt.Fprintf(os.Stderr, "==== END DEBUG ====\n\n")

	// Extra diagnostics to investigate duplication in scan-based summaries
	ctx := context.Background()
	var n uint64

	if err := ch.ExecuteQuery(ctx,
		"SELECT count() FROM fs_entries_current WHERE path LIKE ? AND ftype = ?",
		clickhouse.EnsureDir(path)+"%", uint8(clickhouse.FileTypeFile), &n); err == nil {
		fmt.Fprintf(os.Stderr, "DEBUG: total file rows under %s: %d\n", path, n)
	}

	if err := ch.ExecuteQuery(ctx,
		"SELECT countDistinct(path) FROM fs_entries_current WHERE path LIKE ? AND ftype = ?",
		clickhouse.EnsureDir(path)+"%", uint8(clickhouse.FileTypeFile), &n); err == nil {
		fmt.Fprintf(os.Stderr, "DEBUG: distinct file paths under %s: %d\n", path, n)
	}

	//nolint:lll // SQL string is clearer unwrapped
	if err := ch.ExecuteQuery(ctx,
		"SELECT count() FROM (SELECT path, count() AS c FROM fs_entries_current WHERE path LIKE ? AND ftype = ? GROUP BY path HAVING c > 1)",
		clickhouse.EnsureDir(path)+"%", uint8(clickhouse.FileTypeFile), &n); err == nil {
		fmt.Fprintf(os.Stderr, "DEBUG: num duplicate path groups under %s: %d\n", path, n)
	}

	//nolint:lll // SQL string is clearer unwrapped
	if err := ch.ExecuteQuery(ctx,
		"SELECT countDistinct(parent_path) FROM (SELECT anyLast(parent_path) AS parent_path FROM fs_entries_current WHERE path LIKE ? AND ftype = ? GROUP BY path)",
		clickhouse.EnsureDir(path)+"%", uint8(clickhouse.FileTypeFile), &n); err == nil {
		fmt.Fprintf(os.Stderr, "DEBUG: distinct parent directories with files under %s: %d\n", path, n)
	}

	// Compare with DirCountWithFiles implementation
	if cnt, err := ch.DirCountWithFiles(ctx, path, clickhouse.Filters{}); err == nil {
		fmt.Fprintf(os.Stderr, "DEBUG: DirCountWithFiles(%s) => %d\n", path, cnt)
	} else {
		fmt.Fprintf(os.Stderr, "DEBUG: DirCountWithFiles error: %v\n", err)
	}

	// Note: Additional low-level diagnostics removed to avoid relying on
	// unexported connection details. Use ClickHouse helpers if needed.
}
