/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	dirsCatalogParentSentinel = uint32(0xFFFFFFFF)
	dirsCatalogInsertStmt     = "INSERT INTO wrstat_dirs (" +
		"mount_path, snapshot_id, dir_id, parent_id, subtree_end, depth, name, full_path, " +
		"child_dir_count, child_file_count, path_hash) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sipHash64(?))"
)

type dirsCatalogRow struct {
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	name           string
	fullPath       string
	childDirCount  uint32
	childFileCount uint32
}

func dirsCatalogFixtureRows() []dirsCatalogRow {
	return []dirsCatalogRow{
		{
			dirID:          0,
			parentID:       dirsCatalogParentSentinel,
			subtreeEnd:     7,
			depth:          0,
			name:           "/",
			fullPath:       "/",
			childDirCount:  1,
			childFileCount: 0,
		},
		{
			dirID:          1,
			parentID:       0,
			subtreeEnd:     7,
			depth:          1,
			name:           "catalog/",
			fullPath:       "/catalog/",
			childDirCount:  1,
			childFileCount: 0,
		},
		{
			dirID:          2,
			parentID:       1,
			subtreeEnd:     7,
			depth:          2,
			name:           "team/",
			fullPath:       "/catalog/team/",
			childDirCount:  2,
			childFileCount: 3,
		},
		{
			dirID:          3,
			parentID:       2,
			subtreeEnd:     5,
			depth:          3,
			name:           "branch-a/",
			fullPath:       "/catalog/team/branch-a/",
			childDirCount:  1,
			childFileCount: 2,
		},
		{
			dirID:          4,
			parentID:       3,
			subtreeEnd:     5,
			depth:          4,
			name:           "deeper/",
			fullPath:       "/catalog/team/branch-a/deeper/",
			childDirCount:  0,
			childFileCount: 1,
		},
		{
			dirID:          5,
			parentID:       2,
			subtreeEnd:     7,
			depth:          3,
			name:           "branch-b/",
			fullPath:       "/catalog/team/branch-b/",
			childDirCount:  1,
			childFileCount: 1,
		},
		{
			dirID:          6,
			parentID:       5,
			subtreeEnd:     7,
			depth:          4,
			name:           "leaf/",
			fullPath:       "/catalog/team/branch-b/leaf/",
			childDirCount:  0,
			childFileCount: 4,
		},
	}
}

func insertDirsCatalogRows(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
	rows []dirsCatalogRow,
) {
	t.Helper()

	for _, row := range rows {
		if err := conn.Exec(
			ctx,
			dirsCatalogInsertStmt,
			mountPath,
			snapshotID,
			row.dirID,
			row.parentID,
			row.subtreeEnd,
			row.depth,
			row.name,
			row.fullPath,
			row.childDirCount,
			row.childFileCount,
			row.fullPath,
		); err != nil {
			t.Fatalf("failed to insert wrstat_dirs row %q: %v", row.fullPath, err)
		}
	}
}

func dirsCatalogIntervalIDs(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
	row dirsCatalogRow,
) []uint32 {
	t.Helper()

	return queryDirsCatalogIDs(
		ctx,
		t,
		conn,
		"SELECT dir_id FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = ? "+
			"AND dir_id >= ? AND dir_id < ? ORDER BY dir_id",
		mountPath,
		snapshotID,
		row.dirID,
		row.subtreeEnd,
	)
}

func queryDirsCatalogIDs(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	query string,
	args ...any,
) []uint32 {
	t.Helper()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("failed to query wrstat_dirs ids: %v", err)
	}

	defer func() { _ = rows.Close() }()

	ids := make([]uint32, 0, len(dirsCatalogFixtureRows()))

	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan wrstat_dirs id: %v", err)
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("failed while reading wrstat_dirs ids: %v", err)
	}

	return ids
}

func dirsCatalogRowsByID(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
) map[uint32]dirsCatalogRow {
	t.Helper()

	rows, err := conn.Query(
		ctx,
		"SELECT dir_id, parent_id, name, full_path FROM wrstat_dirs "+
			"WHERE mount_path = ? AND snapshot_id = ? ORDER BY dir_id",
		mountPath,
		snapshotID,
	)
	if err != nil {
		t.Fatalf("failed to query wrstat_dirs path rows: %v", err)
	}

	defer func() { _ = rows.Close() }()

	byID := make(map[uint32]dirsCatalogRow)

	for rows.Next() {
		var row dirsCatalogRow
		if err := rows.Scan(&row.dirID, &row.parentID, &row.name, &row.fullPath); err != nil {
			t.Fatalf("failed to scan wrstat_dirs path row: %v", err)
		}

		byID[row.dirID] = row
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("failed while reading wrstat_dirs path rows: %v", err)
	}

	return byID
}

func reconstructDirsCatalogPath(rows map[uint32]dirsCatalogRow, dirID uint32) (string, bool) {
	names := make([]string, 0, len(rows))
	for {
		row, ok := rows[dirID]
		if !ok {
			return "", false
		}

		if row.parentID == dirsCatalogParentSentinel {
			if row.name != "/" {
				return "", false
			}

			return "/" + strings.Join(reverseDirsCatalogNames(names), ""), true
		}

		names = append(names, row.name)
		dirID = row.parentID
	}
}

func reverseDirsCatalogNames(names []string) []string {
	reversed := make([]string, len(names))
	for i, name := range names {
		reversed[len(names)-1-i] = name
	}

	return reversed
}

func TestDirsCatalogA1(t *testing.T) {
	Convey("catalog rows carry direct file-child counts from RecordDGUTA", t, func() {
		row := catalogRowFromRecord(
			activeMount{mountPath: "/mnt/test/", snapshotID: uuid.NewString()},
			db.RecordDGUTA{
				DirID:          7,
				ParentID:       6,
				SubtreeEnd:     8,
				Depth:          3,
				ChildCount:     2,
				ChildFileCount: 4,
			},
			"/mnt/test/team/",
		)

		So(row.childDirCount, ShouldEqual, uint32(2))
		So(row.childFileCount, ShouldEqual, uint32(4))
	})

	Convey("A1 wrstat_dirs stores one directory row per snapshot with navigable ids", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		conn := connectDirsCatalogTestDB(t, cfg)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		mountPath := "/catalog/team/"
		snapshotID := uuid.NewString()
		rows := dirsCatalogFixtureRows()

		insertDirsCatalogRows(ctx, t, conn, mountPath, snapshotID, rows)

		Convey("count and distinct full_path both equal the directory count", func() {
			count, distinctFullPaths := dirsCatalogCounts(ctx, t, conn, mountPath, snapshotID)

			So(count, ShouldEqual, len(rows))
			So(distinctFullPaths, ShouldEqual, len(rows))
		})

		Convey("interval descendants match recursive parent_id descendants for every row", func() {
			for _, row := range rows {
				intervalIDs := dirsCatalogIntervalIDs(ctx, t, conn, mountPath, snapshotID, row)
				walkIDs := dirsCatalogParentWalkIDs(ctx, t, conn, mountPath, snapshotID, row.dirID)

				So(intervalIDs, ShouldResemble, walkIDs)
			}
		})

		Convey("the root directory uses the parent sentinel", func() {
			parentID := dirsCatalogParentID(ctx, t, conn, mountPath, snapshotID, "/")

			So(parentID, ShouldEqual, dirsCatalogParentSentinel)
		})

		Convey("stored full_path bytes match paths reconstructed by parent_id walks", func() {
			catalogRows := dirsCatalogRowsByID(ctx, t, conn, mountPath, snapshotID)

			for _, row := range rows {
				reconstructed, ok := reconstructDirsCatalogPath(catalogRows, row.dirID)

				So(ok, ShouldBeTrue)
				So(reconstructed, ShouldEqual, row.fullPath)
			}
		})
	})
}

func connectDirsCatalogTestDB(t *testing.T, cfg Config) ch.Conn {
	t.Helper()

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("failed to build ClickHouse options: %v", err)
	}

	conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
	if err != nil {
		t.Fatalf("failed to bootstrap ClickHouse test database: %v", err)
	}

	return conn
}

func dirsCatalogCounts(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
) (uint64, uint64) {
	t.Helper()

	row := conn.QueryRow(
		ctx,
		"SELECT count(), count(DISTINCT full_path) FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = ?",
		mountPath,
		snapshotID,
	)

	var count, distinctFullPaths uint64
	if err := row.Scan(&count, &distinctFullPaths); err != nil {
		t.Fatalf("failed to scan wrstat_dirs counts: %v", err)
	}

	return count, distinctFullPaths
}

func dirsCatalogParentWalkIDs(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
	dirID uint32,
) []uint32 {
	t.Helper()

	return queryDirsCatalogIDs(
		ctx,
		t,
		conn,
		"WITH RECURSIVE descendants AS ("+
			"SELECT dir_id FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = ? AND dir_id = ? "+
			"UNION ALL SELECT child.dir_id FROM wrstat_dirs AS child "+
			"INNER JOIN descendants AS parent ON child.parent_id = parent.dir_id "+
			"WHERE child.mount_path = ? AND child.snapshot_id = ?"+
			") SELECT dir_id FROM descendants ORDER BY dir_id",
		mountPath,
		snapshotID,
		dirID,
		mountPath,
		snapshotID,
	)
}

func dirsCatalogParentID(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	snapshotID string,
	fullPath string,
) uint32 {
	t.Helper()

	row := conn.QueryRow(
		ctx,
		"SELECT parent_id FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = ? AND full_path = ?",
		mountPath,
		snapshotID,
		fullPath,
	)

	var parentID uint32
	if err := row.Scan(&parentID); err != nil {
		t.Fatalf("failed to scan wrstat_dirs parent_id for %q: %v", fullPath, err)
	}

	return parentID
}
