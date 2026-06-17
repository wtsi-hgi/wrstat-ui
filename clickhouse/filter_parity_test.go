/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type queryer interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

func assertD3SummaryRouteParity(
	ctx context.Context,
	conn queryer,
	updatedAt time.Time,
	filter *db.Filter,
	build func(dgutaFilterRoute) (string, []any),
) {
	materialisedQuery, materialisedArgs := build(dgutaFilterRouteMaterialised)
	vectorQuery, vectorArgs := build(dgutaFilterRouteVector)

	materialised, err := queryD3SummaryRows(ctx, conn, updatedAt, filter, materialisedQuery, materialisedArgs)
	So(err, ShouldBeNil)

	vector, err := queryD3SummaryRows(ctx, conn, updatedAt, filter, vectorQuery, vectorArgs)
	So(err, ShouldBeNil)
	So(vector, ShouldResemble, materialised)
}

func assertD3ChildRouteParity(
	ctx context.Context,
	conn queryer,
	updatedAt time.Time,
	filter *db.Filter,
	build func(dgutaFilterRoute) (string, []any),
) {
	materialisedQuery, materialisedArgs := build(dgutaFilterRouteMaterialised)
	vectorQuery, vectorArgs := build(dgutaFilterRouteVector)

	materialised, err := queryD3ChildSummaryRows(ctx, conn, updatedAt, filter, materialisedQuery, materialisedArgs)
	So(err, ShouldBeNil)

	vector, err := queryD3ChildSummaryRows(ctx, conn, updatedAt, filter, vectorQuery, vectorArgs)
	So(err, ShouldBeNil)
	So(vector, ShouldResemble, materialised)
}

func d4FilterTableColumns(
	ctx context.Context,
	conn queryer,
	database string,
) (map[string][]string, error) {
	rows, err := conn.Query(
		ctx,
		"SELECT table, name FROM system.columns "+
			"WHERE database = ? AND table IN (?, ?, ?) ORDER BY table, name",
		database,
		dgutaFilterMaterialisationChildAll,
		dgutaFilterMaterialisationDirAll,
		dgutaFilterMaterialisationAgeAll,
	)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	columns := make(map[string][]string)

	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			return nil, err
		}

		columns[table] = append(columns[table], name)
	}

	if err := rowsErr(rows); err != nil {
		return nil, err
	}

	return columns, nil
}

func assertD4DirInfoMatchesMaterialisedBaseline(
	ctx context.Context,
	conn queryer,
	dbch *clickHouseDatabase,
	mount activeMount,
	filter *db.Filter,
) {
	expected := d4MaterialisedSummaryBaseline(ctx, conn, mount, filter, func() (string, []any) {
		return dgutaFilterExactSummariesQueryForRoute(
			dgutaFilterRouteMaterialised,
			mount.mountPath,
			mount.snapshotID,
			[]uint32{2},
			filter,
		)
	})

	actual, err := dbch.DirInfo(mount.mountPath+"a/", filter)
	So(err, ShouldBeNil)
	So(d4SummaryValues(actual), ShouldResemble, expected[mount.mountPath+"a/"])
}

func d4SummaryValues(sum *db.DirSummary) *db.DirSummary {
	if sum == nil {
		return nil
	}

	cp := *sum
	cp.Dir = ""

	return &cp
}

func assertD4DirInfosMatchMaterialisedBaseline(
	ctx context.Context,
	conn queryer,
	dbch *clickHouseDatabase,
	mount activeMount,
	filter *db.Filter,
) {
	expected := d4MaterialisedChildBaseline(ctx, conn, mount, filter)
	actual, err := dbch.DirInfos([]string{mount.mountPath + "a/", mount.mountPath + "b/"}, filter)
	So(err, ShouldBeNil)

	So(actual[mount.mountPath+"a/"].Dir, ShouldEqual, mount.mountPath+"a/")
	So(actual[mount.mountPath+"b/"].Dir, ShouldEqual, mount.mountPath+"b/")
	So(d4SummaryValues(actual[mount.mountPath+"a/"]), ShouldResemble, expected[mount.mountPath+"a/"].Summary)
	So(d4SummaryValues(actual[mount.mountPath+"b/"]), ShouldResemble, expected[mount.mountPath+"b/"].Summary)
}

func assertD4DirsHaveChildrenMatchesMaterialisedBaseline(
	ctx context.Context,
	conn queryer,
	dbch *clickHouseDatabase,
	mount activeMount,
	filter *db.Filter,
) {
	expectedChildren := d4MaterialisedChildBaseline(ctx, conn, mount, filter)
	expected := map[string]bool{
		mount.mountPath + "a/": expectedChildren[mount.mountPath+"a/"].HasChildren,
		mount.mountPath + "b/": expectedChildren[mount.mountPath+"b/"].HasChildren,
	}

	actual, err := dbch.DirsHaveChildren([]string{mount.mountPath + "a/", mount.mountPath + "b/"}, filter)
	So(err, ShouldBeNil)
	So(actual, ShouldResemble, expected)
}

func d4MaterialisedChildBaseline(
	ctx context.Context,
	conn queryer,
	mount activeMount,
	filter *db.Filter,
) map[string]childFilterAllSummary {
	query, args := dgutaFilterChildrenSummariesQueryForRoute(
		dgutaFilterRouteMaterialised,
		mount.mountPath,
		mount.snapshotID,
		1,
		filter,
	)
	expected, err := queryD3ChildSummaryRows(ctx, conn, mount.updatedAt, filter, query, args)
	So(err, ShouldBeNil)

	return expected
}

func queryD3ChildSummaryRows(
	ctx context.Context,
	conn queryer,
	updatedAt time.Time,
	filter *db.Filter,
	query string,
	args []any,
) (map[string]childFilterAllSummary, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	facts, err := scanChildFilterAllChildSummaryRows(rows, filter, updatedAt)
	if err != nil {
		return nil, err
	}

	byDir := make(map[string]childFilterAllSummary, len(facts))
	for _, fact := range facts {
		byDir[fact.Dir] = fact
	}

	return byDir, nil
}

func assertD4WhereMatchesMaterialisedBaseline(
	ctx context.Context,
	conn queryer,
	dbch *clickHouseDatabase,
	mount activeMount,
	filter *db.Filter,
) {
	subtree := d4MaterialisedSummaryBaseline(ctx, conn, mount, filter, func() (string, []any) {
		return dgutaFilterSubtreeSummariesQueryForRoute(
			dgutaFilterRouteMaterialised,
			mount.mountPath,
			mount.snapshotID,
			1,
			5,
			filter,
		)
	})
	expected := map[string]*db.DirSummary{
		mount.mountPath: subtree[mount.mountPath],
	}

	actualDCSs, err := db.NewTree(d4GenericWhereDB{database: dbch}).
		Where(mount.mountPath, filter, split.SplitsToSplitFn(0))
	So(err, ShouldBeNil)
	So(d4SummaryValuesByDir(d4SummaryMapFromDCSs(actualDCSs)), ShouldResemble, expected)
}

func d4SummaryValuesByDir(summaries map[string]*db.DirSummary) map[string]*db.DirSummary {
	values := make(map[string]*db.DirSummary, len(summaries))
	for dir, sum := range summaries {
		values[dir] = d4SummaryValues(sum)
	}

	return values
}

func d4SummaryMapFromDCSs(dcss db.DCSs) map[string]*db.DirSummary {
	summaries := make(map[string]*db.DirSummary, len(dcss))
	for _, sum := range dcss {
		summaries[sum.Dir] = sum
	}

	return summaries
}

func d4MaterialisedSummaryBaseline(
	ctx context.Context,
	conn queryer,
	mount activeMount,
	filter *db.Filter,
	build func() (string, []any),
) map[string]*db.DirSummary {
	query, args := build()
	expected, err := queryD3SummaryRows(ctx, conn, mount.updatedAt, filter, query, args)
	So(err, ShouldBeNil)

	return expected
}

func queryD3SummaryRows(
	ctx context.Context,
	conn queryer,
	updatedAt time.Time,
	filter *db.Filter,
	query string,
	args []any,
) (map[string]*db.DirSummary, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, updatedAt)

	return summaries, err
}

type d4GenericWhereDB struct {
	database *clickHouseDatabase
}

func (d d4GenericWhereDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	return d.database.DirInfo(dir, filter)
}

func (d d4GenericWhereDB) DirInfos(dirs []string, filter *db.Filter) (map[string]*db.DirSummary, error) {
	return d.database.DirInfos(dirs, filter)
}

func (d d4GenericWhereDB) Children(dir string) ([]string, error) {
	return d.database.Children(dir)
}

func (d d4GenericWhereDB) Info() (*db.Info, error) {
	return d.database.Info()
}

func (d d4GenericWhereDB) Close() error {
	return nil
}

func TestD4FilterTablesDoNotStorePathColumns(t *testing.T) {
	Convey("D4 filter materialisations expose numeric dir ids without path-like columns", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		client, err := NewClient(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(client.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		columns, err := d4FilterTableColumns(ctx, client.conn, cfg.Database)
		So(err, ShouldBeNil)

		var forbidden []string

		for _, table := range d4FilterTableNames() {
			So(columns, ShouldContainKey, table)

			for _, name := range columns[table] {
				if d4ForbiddenFilterColumn(name) {
					forbidden = append(forbidden, table+"."+name)
				}
			}
		}

		So(forbidden, ShouldBeEmpty)
	})
}

func d4FilterTableNames() []string {
	return []string{
		dgutaFilterMaterialisationChildAll,
		dgutaFilterMaterialisationDirAll,
		dgutaFilterMaterialisationAgeAll,
	}
}

func d4ForbiddenFilterColumn(name string) bool {
	switch strings.ToLower(name) {
	case "dir", "parent_dir", "path", "full_path":
		return true
	default:
		return false
	}
}

func TestD4VectorFilterParity(t *testing.T) {
	Convey("D4 vector exact, child, and subtree summaries match materialised rows", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/d4-api-parity/"}

		client, err := NewClient(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(client.Close(), ShouldBeNil) })

		mountPath := "/mnt/d3-parity/"
		snapshotID := uuid.NewString()
		updatedAt := time.Unix(1_700_000_000, 0).UTC()

		setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		seedD3ParityRows(setupCtx, client.conn, mountPath, snapshotID, updatedAt)
		setupCancel()

		for _, testCase := range d3ParityFilterCases() {
			filter := testCase.filter
			t.Logf("checking D4 vector parity for %s", testCase.name)

			caseCtx, caseCancel := context.WithTimeout(context.Background(), 10*time.Second)

			assertD3SummaryRouteParity(caseCtx, client.conn, updatedAt, filter,
				func(route dgutaFilterRoute) (string, []any) {
					return dgutaFilterExactSummariesQueryForRoute(route, mountPath, snapshotID, []uint32{2, 3}, filter)
				},
			)
			assertD3ChildRouteParity(caseCtx, client.conn, updatedAt, filter,
				func(route dgutaFilterRoute) (string, []any) {
					return dgutaFilterChildrenSummariesQueryForRoute(route, mountPath, snapshotID, 1, filter)
				},
			)
			assertD3SummaryRouteParity(caseCtx, client.conn, updatedAt, filter,
				func(route dgutaFilterRoute) (string, []any) {
					return dgutaFilterSubtreeSummariesQueryForRoute(route, mountPath, snapshotID, 1, 5, filter)
				},
			)

			caseCancel()
		}
	})
}

func seedD3ParityRows(
	ctx context.Context,
	conn execer,
	mountPath, snapshotID string,
	updatedAt time.Time,
) {
	So(conn.Exec(ctx, testInsertMountStmt, mountPath, updatedAt, snapshotID, updatedAt), ShouldBeNil)

	insertD3Dir(ctx, conn, mountPath, snapshotID, 1, 0, 5, 0, "", mountPath, 2)
	insertD3Dir(ctx, conn, mountPath, snapshotID, 2, 1, 4, 1, "a", mountPath+"a/", 1)
	insertD3Dir(ctx, conn, mountPath, snapshotID, 3, 1, 4, 1, "b", mountPath+"b/", 0)
	insertD3Dir(ctx, conn, mountPath, snapshotID, 4, 2, 5, 2, "leaf", mountPath+"a/leaf/", 0)

	insertD3Facts(ctx, conn, d3FactRow{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		dirID:      1,
		parentID:   0,
		subtreeEnd: 5,
		updatedAt:  updatedAt,
		childCount: 2,
		gutas: []d3GUTA{
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
		},
	})
	insertD3Facts(ctx, conn, d3FactRow{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		dirID:      2,
		parentID:   1,
		subtreeEnd: 4,
		updatedAt:  updatedAt,
		childCount: 1,
		gutas: []d3GUTA{
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20, 90, 250),
			d3GUTARow(8, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3, 30, 80, 260),
			d3GUTARow(7, 12, db.DGUTAFileTypeCram, db.DGUTAgeAll, 4, 40, 70, 270),
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 5, 50, 60, 280),
		},
	})
	insertD3Facts(ctx, conn, d3FactRow{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		dirID:      3,
		parentID:   1,
		subtreeEnd: 4,
		updatedAt:  updatedAt,
		childCount: 0,
		gutas: []d3GUTA{
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 6, 60, 110, 300),
			d3GUTARow(7, 11, db.DGUTAFileTypeOther, db.DGUTAgeAll, 7, 70, 120, 310),
		},
	})
	insertD3Facts(ctx, conn, d3FactRow{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		dirID:      4,
		parentID:   2,
		subtreeEnd: 5,
		updatedAt:  updatedAt,
		childCount: 0,
		gutas: []d3GUTA{
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 8, 80, 50, 400),
		},
	})

	insertD3FilterRows(ctx, conn, mountPath, snapshotID, updatedAt)
	seedD4MaterialisationReadiness(ctx, conn, mountPath, snapshotID, updatedAt)
}

func insertD3Dir(
	ctx context.Context,
	conn execer,
	mountPath, snapshotID string,
	dirID, parentID, subtreeEnd uint32,
	depth uint16,
	name, fullPath string,
	childCount uint32,
) {
	So(conn.Exec(ctx,
		"INSERT INTO wrstat_dirs "+
			"(mount_path, snapshot_id, dir_id, parent_id, subtree_end, depth, name, full_path, "+
			"child_dir_count, child_file_count, path_hash) "+
			"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, 0, sipHash64(?))",
		mountPath,
		snapshotID,
		dirID,
		parentID,
		subtreeEnd,
		depth,
		name,
		fullPath,
		childCount,
		fullPath,
	), ShouldBeNil)
}

func insertD3Facts(ctx context.Context, conn execer, row d3FactRow) {
	gids, uids, fts, ages, counts, sizes, atimeMins, mtimeMaxs, atimeBuckets, mtimeBuckets := row.arrays()

	So(conn.Exec(ctx,
		"INSERT INTO wrstat_dir_facts "+
			"(mount_path, snapshot_id, dir_id, parent_id, subtree_end, updated_at, all_count, all_size, "+
			"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, "+
			"file_count, file_size, file_atime_min, file_mtime_max, file_atime_buckets, file_mtime_buckets, "+
			"file_uids, file_gids, file_ft, gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, "+
			"atime_buckets, mtime_buckets, child_count, refreshed_at) "+
			"VALUES (?, toUUID(?), ?, ?, ?, ?, 0, 0, 0, 0, [], [], [], [], 0, 0, 0, 0, 0, [], [], [], [], 0, "+
			"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())",
		row.mountPath,
		row.snapshotID,
		row.dirID,
		row.parentID,
		row.subtreeEnd,
		row.updatedAt,
		gids,
		uids,
		fts,
		ages,
		counts,
		sizes,
		atimeMins,
		mtimeMaxs,
		atimeBuckets,
		mtimeBuckets,
		row.childCount,
	), ShouldBeNil)
}

func d3GUTARow(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count, size uint64,
	atime, mtime int64,
) d3GUTA {
	return d3GUTA{
		gid:          gid,
		uid:          uid,
		ft:           ft,
		age:          age,
		count:        count,
		size:         size,
		atimeMin:     atime,
		mtimeMax:     mtime,
		atimeBuckets: d3BucketSlice(0, count),
		mtimeBuckets: d3BucketSlice(1, count),
	}
}

func d3BucketSlice(idx int, value uint64) []uint64 {
	buckets := make([]uint64, len(summary.AgeBuckets{}))
	buckets[idx] = value

	return buckets
}

func insertD3FilterRows(ctx context.Context, conn execer, mountPath, snapshotID string, refreshedAt time.Time) {
	for _, row := range d3MaterialisedFilterRows(mountPath, snapshotID, refreshedAt) {
		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dir_filter_all "+
				"(mount_path, snapshot_id, age, gid, uid, ft, dir_id, subtree_end, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, child_count, "+
				"has_filter_children, has_children, refreshed_at) "+
				"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			row.values()...,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_child_filter_all "+
				"(mount_path, snapshot_id, parent_id, age, gid, uid, ft, dir_id, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, child_count, "+
				"has_filter_children, has_children, refreshed_at) "+
				"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			row.childValues()...,
		), ShouldBeNil)
	}
}

func d3MaterialisedFilterRows(mountPath, snapshotID string, refreshedAt time.Time) []d3FilterRow {
	return []d3FilterRow{
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 1, 0, 5, 2, 2,
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 2, 1, 4, 1, 1,
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20, 90, 250),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 2, 1, 4, 1, 1,
			d3GUTARow(8, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3, 30, 80, 260),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 2, 1, 4, 1, 1,
			d3GUTARow(7, 12, db.DGUTAFileTypeCram, db.DGUTAgeAll, 4, 40, 70, 270),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 2, 1, 4, 0, 1,
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 5, 50, 60, 280),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 3, 1, 4, 0, 0,
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 6, 60, 110, 300),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 3, 1, 4, 0, 0,
			d3GUTARow(7, 11, db.DGUTAFileTypeOther, db.DGUTAgeAll, 7, 70, 120, 310),
		),
		d3FilterRowForGUTA(
			mountPath, snapshotID, refreshedAt, 4, 2, 5, 0, 0,
			d3GUTARow(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 8, 80, 50, 400),
		),
	}
}

func d3FilterRowForGUTA(
	mountPath, snapshotID string,
	refreshedAt time.Time,
	dirID, parentID, subtreeEnd uint32,
	filterChildCount, childCount uint64,
	guta d3GUTA,
) d3FilterRow {
	return d3FilterRow{
		mountPath:        mountPath,
		snapshotID:       snapshotID,
		parentID:         parentID,
		dirID:            dirID,
		subtreeEnd:       subtreeEnd,
		filterChildCount: filterChildCount,
		childCount:       childCount,
		refreshedAt:      refreshedAt,
		guta:             guta,
	}
}

func seedD4MaterialisationReadiness(
	ctx context.Context,
	conn execer,
	mountPath, snapshotID string,
	updatedAt time.Time,
) {
	So(conn.Exec(ctx, insertMountDirSummarySetQuery, mountPath, snapshotID, updatedAt, updatedAt), ShouldBeNil)

	counts := schema3SnapshotRowCounts{
		dirsRows:           4,
		dirFactsRows:       4,
		childFilterAllRows: uint64(len(d3MaterialisedFilterRows(mountPath, snapshotID, updatedAt))),
		dirFilterAllRows:   uint64(len(d3MaterialisedFilterRows(mountPath, snapshotID, updatedAt))),
	}
	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetQuery,
		mountPath,
		snapshotID,
		currentSchemaVersion,
		counts.dirsRows,
		counts.dirFactsRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
		schema3SnapshotManifestSHA256(activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID,
			updatedAt:  updatedAt,
		}, counts),
		updatedAt,
	), ShouldBeNil)
}

func d3ParityFilterCases() []d3ParityFilterCase {
	return []d3ParityFilterCase{
		{
			name: "gid-only filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeAll,
				GIDs: []uint32{7},
			},
		},
		{
			name: "uid-only filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeAll,
				UIDs: []uint32{11},
			},
		},
		{
			name: "ft-only filter",
			filter: &db.Filter{
				Age: db.DGUTAgeAll,
				FT:  db.DGUTAFileTypeBam,
			},
		},
		{
			name: "age-only filter",
			filter: &db.Filter{
				Age: db.DGUTAgeA1M,
			},
		},
		{
			name: "gid and uid filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeAll,
				GIDs: []uint32{7},
				UIDs: []uint32{11},
			},
		},
		{
			name: "gid and ft filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeAll,
				GIDs: []uint32{7},
				FT:   db.DGUTAFileTypeBam,
			},
		},
		{
			name: "uid and ft filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeAll,
				UIDs: []uint32{11},
				FT:   db.DGUTAFileTypeBam,
			},
		},
		{
			name: "gid and age filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeA1M,
				GIDs: []uint32{7},
			},
		},
		{
			name: "uid and age filter",
			filter: &db.Filter{
				Age:  db.DGUTAgeA1M,
				UIDs: []uint32{11},
			},
		},
		{
			name: "ft and age filter",
			filter: &db.Filter{
				Age: db.DGUTAgeA1M,
				FT:  db.DGUTAFileTypeBam,
			},
		},
		{
			name:   "gid, uid, ft, and age filter",
			filter: d3ParityFilter(),
		},
	}
}

func d3ParityFilter() *db.Filter {
	return &db.Filter{
		Age:  db.DGUTAgeAll,
		GIDs: []uint32{7},
		UIDs: []uint32{11},
		FT:   db.DGUTAFileTypeBam,
	}
}

func TestD4FilteredAPIsMatchMaterialisedBaseline(t *testing.T) {
	Convey("D4 filtered APIs match materialised-route baseline output", t, func() {
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		client, err := NewClient(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(client.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		mountPath := "/mnt/d4-api-parity/"
		snapshotID := uuid.NewString()
		updatedAt := time.Unix(1_700_000_000, 0).UTC()
		filter := d3ParityFilter()

		seedD3ParityRows(ctx, client.conn, mountPath, snapshotID, updatedAt)

		mount := activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID,
			updatedAt:  updatedAt,
		}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, client.conn, e1Snapshot(mount))

		assertD4DirInfoMatchesMaterialisedBaseline(ctx, client.conn, dbch, mount, filter)
		assertD4DirInfosMatchMaterialisedBaseline(ctx, client.conn, dbch, mount, filter)
		assertD4DirsHaveChildrenMatchesMaterialisedBaseline(ctx, client.conn, dbch, mount, filter)
		assertD4WhereMatchesMaterialisedBaseline(ctx, client.conn, dbch, mount, filter)
	})
}

type execer interface {
	Exec(ctx context.Context, query string, args ...any) error
}

type d3GUTA struct {
	gid          uint32
	uid          uint32
	ft           db.DirGUTAFileType
	age          db.DirGUTAge
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

type d3FactRow struct {
	mountPath  string
	snapshotID string
	dirID      uint32
	parentID   uint32
	subtreeEnd uint32
	updatedAt  time.Time
	childCount uint64
	gutas      []d3GUTA
}

func (r d3FactRow) arrays() (
	[]uint32,
	[]uint32,
	[]uint16,
	[]uint8,
	[]uint64,
	[]uint64,
	[]int64,
	[]int64,
	[][]uint64,
	[][]uint64,
) {
	gids := make([]uint32, len(r.gutas))
	uids := make([]uint32, len(r.gutas))
	fts := make([]uint16, len(r.gutas))
	ages := make([]uint8, len(r.gutas))
	counts := make([]uint64, len(r.gutas))
	sizes := make([]uint64, len(r.gutas))
	atimeMins := make([]int64, len(r.gutas))
	mtimeMaxs := make([]int64, len(r.gutas))
	atimeBuckets := make([][]uint64, len(r.gutas))
	mtimeBuckets := make([][]uint64, len(r.gutas))

	for i, guta := range r.gutas {
		gids[i] = guta.gid
		uids[i] = guta.uid
		fts[i] = uint16(guta.ft)
		ages[i] = uint8(guta.age)
		counts[i] = guta.count
		sizes[i] = guta.size
		atimeMins[i] = guta.atimeMin
		mtimeMaxs[i] = guta.mtimeMax
		atimeBuckets[i] = guta.atimeBuckets
		mtimeBuckets[i] = guta.mtimeBuckets
	}

	return gids, uids, fts, ages, counts, sizes, atimeMins, mtimeMaxs, atimeBuckets, mtimeBuckets
}

type d3FilterRow struct {
	mountPath        string
	snapshotID       string
	parentID         uint32
	dirID            uint32
	subtreeEnd       uint32
	filterChildCount uint64
	childCount       uint64
	refreshedAt      time.Time
	guta             d3GUTA
}

func (r d3FilterRow) values() []any {
	return []any{
		r.mountPath,
		r.snapshotID,
		uint8(r.guta.age),
		r.guta.gid,
		r.guta.uid,
		uint16(r.guta.ft),
		r.dirID,
		r.subtreeEnd,
		r.guta.count,
		r.guta.size,
		r.guta.atimeMin,
		r.guta.mtimeMax,
		r.guta.atimeBuckets,
		r.guta.mtimeBuckets,
		r.filterChildCount,
		r.childCount,
		d3BoolByte(r.filterChildCount > 0),
		d3BoolByte(r.childCount > 0),
		r.refreshedAt,
	}
}

func d3BoolByte(ok bool) uint8 {
	if ok {
		return 1
	}

	return 0
}

func (r d3FilterRow) childValues() []any {
	values := r.values()

	out := append([]any{
		values[0],
		values[1],
		r.parentID,
	}, values[2:7]...)

	return append(out, values[8:]...)
}

type d3ParityFilterCase struct {
	name   string
	filter *db.Filter
}

func TestD4FilterCollapseDecisionProcedure(t *testing.T) {
	Convey("D4 keeps every materialisation by default when Phase 7 has no measurement", t, func() {
		decisions := defaultDGUTAFilterCollapseDecisions()
		byPattern := d4CollapseDecisionsByPattern(decisions)

		So(decisions, ShouldHaveLength, 3)

		for _, pattern := range []dgutaFilterPattern{
			dgutaFilterPatternExact,
			dgutaFilterPatternChildren,
			dgutaFilterPatternSubtree,
		} {
			decision := byPattern[pattern]
			So(decision.Pattern, ShouldEqual, pattern)
			So(decision.Route, ShouldEqual, dgutaFilterRouteMaterialised)
			So(decision.Collapsed, ShouldBeFalse)
			So(decision.Materialisation, ShouldNotEqual, "")
			So(decision.Citation, ShouldEqual, "")
			So(decision.Reason, ShouldContainSubstring, "no Phase 7 D4 measurement")
		}
	})

	Convey("D4 allows collapse only with a cited parity measurement under the latency gate", t, func() {
		decision, err := dgutaFilterCollapseDecisionFor(dgutaFilterCollapseRequest{
			Pattern:  dgutaFilterPatternExact,
			Collapse: true,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternExact,
				Dataset:        "phase7-high-fanout",
				Fanout:         "single-row exact",
				Citation:       "phase7-report.json#filtered_exact/high-fanout",
				InQueryP95MS:   42,
				LatencyGateMS:  100,
				ParityProven:   true,
				PatternCovered: true,
			},
		})

		So(err, ShouldBeNil)
		So(decision.Route, ShouldEqual, dgutaFilterRouteVector)
		So(decision.Collapsed, ShouldBeTrue)
		So(decision.Citation, ShouldEqual, "phase7-report.json#filtered_exact/high-fanout")
		So(decision.InQueryP95MS, ShouldEqual, 42)
		So(decision.LatencyGateMS, ShouldEqual, 100)
	})

	Convey("D4 rejects unproven collapse requests", t, func() {
		for _, request := range d4UnprovenCollapseRequests() {
			decision, err := dgutaFilterCollapseDecisionFor(request)
			So(errors.Is(err, errDGUTAFilterCollapseUnproven), ShouldBeTrue)
			So(decision.Route, ShouldEqual, dgutaFilterRouteMaterialised)
			So(decision.Collapsed, ShouldBeFalse)
		}
	})

	Convey("D4 reports unreproduced bounded datasets as retained", t, func() {
		decision, err := dgutaFilterCollapseDecisionFor(dgutaFilterCollapseRequest{
			Pattern: dgutaFilterPatternChildren,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternChildren,
				Dataset:        "bounded-local",
				Fanout:         "not reproduced",
				PatternCovered: false,
			},
		})

		So(err, ShouldBeNil)
		So(decision.Route, ShouldEqual, dgutaFilterRouteMaterialised)
		So(decision.Collapsed, ShouldBeFalse)
		So(decision.Reason, ShouldContainSubstring, "bounded dataset could not reproduce")
	})
}

func d4CollapseDecisionsByPattern(
	decisions []dgutaFilterCollapseDecision,
) map[dgutaFilterPattern]dgutaFilterCollapseDecision {
	byPattern := make(map[dgutaFilterPattern]dgutaFilterCollapseDecision, len(decisions))
	for _, decision := range decisions {
		byPattern[decision.Pattern] = decision
	}

	return byPattern
}

func d4UnprovenCollapseRequests() []dgutaFilterCollapseRequest {
	return []dgutaFilterCollapseRequest{
		{Pattern: dgutaFilterPatternExact, Collapse: true},
		{
			Pattern:  dgutaFilterPatternExact,
			Collapse: true,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternExact,
				Citation:       "phase7-report.json#filtered_exact/high-fanout",
				InQueryP95MS:   101,
				LatencyGateMS:  100,
				ParityProven:   true,
				PatternCovered: true,
			},
		},
		{
			Pattern:  dgutaFilterPatternChildren,
			Collapse: true,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternChildren,
				InQueryP95MS:   99,
				LatencyGateMS:  100,
				ParityProven:   true,
				PatternCovered: true,
			},
		},
		{
			Pattern:  dgutaFilterPatternSubtree,
			Collapse: true,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternSubtree,
				Citation:       "phase7-report.json#filtered_subtree/t283",
				InQueryP95MS:   99,
				LatencyGateMS:  100,
				PatternCovered: true,
			},
		},
		{
			Pattern:  dgutaFilterPatternSubtree,
			Collapse: true,
			Measurement: &dgutaFilterCollapseMeasurement{
				Pattern:        dgutaFilterPatternExact,
				Citation:       "phase7-report.json#filtered_exact/t283",
				InQueryP95MS:   99,
				LatencyGateMS:  100,
				ParityProven:   true,
				PatternCovered: true,
			},
		},
	}
}

func TestD4VectorFilterRouteSelection(t *testing.T) {
	Convey("D4 exact summaries are selectable between materialised and vector routes", t, func() {
		filter := d3ParityFilter()
		materialised, materialisedArgs := dgutaFilterExactSummariesQueryForRoute(
			dgutaFilterRouteMaterialised,
			"/mnt/d3/",
			"snapshot",
			[]uint32{2, 3},
			filter,
		)
		vector, vectorArgs := dgutaFilterExactSummariesQueryForRoute(
			dgutaFilterRouteVector,
			"/mnt/d3/",
			"snapshot",
			[]uint32{2, 3},
			filter,
		)

		So(materialised, ShouldContainSubstring, "FROM wrstat_dir_filter_all")
		So(materialised, ShouldNotContainSubstring, "arrayFilter")
		So(materialisedArgs, ShouldResemble, []any{
			"/mnt/d3/",
			"snapshot",
			uint8(db.DGUTAgeAll),
			uint32(7),
			uint32(11),
			uint16(db.DGUTAFileTypeBam),
			uint32(2),
			uint32(3),
		})

		So(vector, ShouldContainSubstring, "FROM wrstat_dir_facts")
		So(vector, ShouldContainSubstring, "arrayFilter")
		So(vector, ShouldContainSubstring, "arrayReduce")
		So(vector, ShouldContainSubstring, "d.dir_id IN (?, ?)")
		So(vector, ShouldNotContainSubstring, "wrstat_dir_filter_all")
		So(vectorArgs, ShouldResemble, []any{
			uint32(7),
			uint32(11),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			"/mnt/d3/",
			"snapshot",
			uint32(2),
			uint32(3),
		})
	})

	Convey("D4 child and subtree vector routes operate on dir_id bands", t, func() {
		filter := d3ParityFilter()

		children, _ := dgutaFilterChildrenSummariesQueryForRoute(
			dgutaFilterRouteVector,
			"/mnt/d3/",
			"snapshot",
			1,
			filter,
		)
		So(children, ShouldContainSubstring, "FROM wrstat_dir_facts")
		So(children, ShouldContainSubstring, "d.parent_id = ?")
		So(children, ShouldContainSubstring, "arrayFilter")
		So(children, ShouldContainSubstring, "filter_child_count")

		subtree, _ := dgutaFilterSubtreeSummariesQueryForRoute(
			dgutaFilterRouteVector,
			"/mnt/d3/",
			"snapshot",
			1,
			5,
			filter,
		)
		So(subtree, ShouldContainSubstring, "FROM wrstat_dir_facts")
		So(subtree, ShouldContainSubstring, "d.dir_id >= ? AND d.dir_id < ?")
		So(subtree, ShouldContainSubstring, "arrayFilter")
		So(subtree, ShouldContainSubstring, "arrayReduce")
	})
}
