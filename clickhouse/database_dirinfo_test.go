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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/server"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	errForcedMaintainedSummaryPrefetchFailure = errors.New("forced maintained summary prefetch failure")
	errUnexpectedMountDirSummaryReadyQuery    = errors.New("unexpected query in mount dir summary readiness test")
)

const (
	createDirFilterAgeAllTableForTest = "CREATE TABLE IF NOT EXISTS wrstat_dir_filter_ageall (" +
		"mount_path LowCardinality(String), snapshot_id UUID, gid UInt32, uid UInt32, ft UInt16, " +
		"dir String, count UInt64, size UInt64, atime_min Int64, mtime_max Int64, " +
		"atime_buckets Array(UInt64), mtime_buckets Array(UInt64), refreshed_at DateTime64(3)" +
		") ENGINE = MergeTree PARTITION BY (mount_path, snapshot_id) " +
		"ORDER BY (mount_path, snapshot_id, gid, uid, ft, dir)"
	insertDirFilterAgeAllForTest = "INSERT INTO wrstat_dir_filter_ageall " +
		"(mount_path, snapshot_id, gid, uid, ft, dir, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, refreshed_at) VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"
	createDirFilterAllTableForTest = "CREATE TABLE IF NOT EXISTS wrstat_dir_filter_all (" +
		"mount_path LowCardinality(String), snapshot_id UUID, age UInt8, gid UInt32, uid UInt32, ft UInt16, " +
		"dir String, parent_dir String, count UInt64, size UInt64, atime_min Int64, mtime_max Int64, " +
		"atime_buckets Array(UInt64), mtime_buckets Array(UInt64), filter_child_count UInt64, child_count UInt64, " +
		"has_filter_children UInt8, has_children UInt8, refreshed_at DateTime64(3)" +
		") ENGINE = MergeTree PARTITION BY (mount_path, snapshot_id) " +
		"ORDER BY (mount_path, snapshot_id, age, gid, uid, ft, dir)"
	createChildFilterAllTableForTest = "CREATE TABLE IF NOT EXISTS wrstat_child_filter_all (" +
		"mount_path LowCardinality(String), snapshot_id UUID, parent_dir String, " +
		"age UInt8, gid UInt32, uid UInt32, ft UInt16, dir String, count UInt64, size UInt64, " +
		"atime_min Int64, mtime_max Int64, atime_buckets Array(UInt64), mtime_buckets Array(UInt64), " +
		"filter_child_count UInt64, child_count UInt64, has_filter_children UInt8, has_children UInt8, " +
		"refreshed_at DateTime64(3)" +
		") ENGINE = MergeTree PARTITION BY (mount_path, snapshot_id) " +
		"ORDER BY (mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir)"
	createSchema3SnapshotSetsTableForTest = "CREATE TABLE IF NOT EXISTS wrstat_schema3_snapshot_sets (" +
		"mount_path LowCardinality(String), snapshot_id UUID, schema3_version UInt32, dir_facts_rows UInt64, " +
		"parent_facts_rows UInt64, children_rows UInt64, child_filter_all_rows UInt64, dir_filter_all_rows UInt64, " +
		"manifest_sha256 String, refreshed_at DateTime64(3)" +
		") ENGINE = MergeTree PARTITION BY (mount_path, snapshot_id) " +
		"ORDER BY (mount_path, snapshot_id, schema3_version)"
	insertDirFilterAllForTest = "INSERT INTO wrstat_dir_filter_all " +
		"(mount_path, snapshot_id, age, gid, uid, ft, dir, parent_dir, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, filter_child_count, child_count, has_filter_children, has_children, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"
	insertChildFilterAllForTest = "INSERT INTO wrstat_child_filter_all " +
		"(mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, filter_child_count, child_count, has_filter_children, has_children, refreshed_at)"
	insertSchema3SnapshotSetForTest = "INSERT INTO wrstat_schema3_snapshot_sets " +
		"(mount_path, snapshot_id, schema3_version, dir_facts_rows, parent_facts_rows, children_rows, " +
		"child_filter_all_rows, dir_filter_all_rows, manifest_sha256, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, now())"
)

const (
	a2T283MatchingDirs = 34998
	a2T283TotalRows    = 100000
	a2T283TotalFiles   = 764218
	a2T283TotalBytes   = 1197943849957
)

const (
	schema3A2ChildCount = 11205
	schema3A2MountPath  = "/lustre/scratch125/"
	schema3A2ParentDir  = schema3A2MountPath + "casm/restricted/dbGaP-team219-43354/VCFS/"
	schema3A2LeafChild  = "leaf/"
)

const (
	b2ActiveRootTupleRowsPerMount          = 40960
	b2ActiveRootTuplePreseededRowsPerMount = 3
	b2ActiveRootTupleRepeat                = 20
	b2ActiveRootTupleP50MaxMS              = 5
	b2ActiveRootTupleP95MaxMS              = 6
	c2LustreAncestor                       = "/lustre/"
	nfsAncestor                            = "/nfs/"
)

type clickHouseGenericTreeDB struct {
	d *clickHouseDatabase
}

func (d *clickHouseGenericTreeDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	return d.d.DirInfo(dir, filter)
}

func (d *clickHouseGenericTreeDB) Children(dir string) ([]string, error) {
	return d.d.Children(dir)
}

func (d *clickHouseGenericTreeDB) Info() (*db.Info, error) {
	return d.d.Info()
}

func (d *clickHouseGenericTreeDB) Close() error {
	return d.d.Close()
}

func TestClickHouseDatabaseFactsRoutingC1(t *testing.T) {
	Convey("tree reads route only through facts, readiness, active mount, and virtual hierarchy tables", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c1-route/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c1-route/"

		updatedAt := time.Date(2026, 6, 1, 14, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 10)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 6)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 8, 4)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"a"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"b"), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		spy := &treeRouteQuerySpyConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, spy)
		tree := db.NewTree(dbch)

		_, err = dbch.DirInfo(mountPath, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)

		_, err = dbch.DirInfos([]string{mountPath, mountPath + "a/"}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)

		_, err = tree.DirInfo(mountPath, nil)
		So(err, ShouldBeNil)

		_, err = dbch.Children(mountPath)
		So(err, ShouldBeNil)

		_, err = dbch.DirsHaveChildren([]string{mountPath, mountPath + "a/"}, nil)
		So(err, ShouldBeNil)

		_, err = tree.Where(mountPath, nil, split.SplitsToSplitFn(1))
		So(err, ShouldBeNil)

		_, err = dbch.Info()
		So(err, ShouldBeNil)

		mountPoints, err := mountPointsFromConfig(cfg)
		So(err, ShouldBeNil)

		client := &Client{cfg: cfg, conn: spy, mountPoints: mountPoints}

		allowed, err := client.PermissionAnyInDir(ctx, mountPath+"a", 9, []uint32{7})
		So(err, ShouldBeNil)
		So(allowed, ShouldBeTrue)

		assertNoLegacyTreeRouteTables(spy.queries)
		So(treeRouteTablesOutsideAllowlist(spy.queries), ShouldBeEmpty)
	})

	Convey("DirInfo filters vector entries by GID and returns matching sorted UIDs", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c1-gid/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c1-gid/"

		updatedAt := time.Date(2026, 6, 1, 14, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		dir := mountPath + "a/"

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 7, 10, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 3)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 8, 11, db.DGUTAFileTypeCram, db.DGUTAgeA1M, 5)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 7, 9, db.DGUTAFileTypeCram, db.DGUTAgeA1M, 4)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		dbch := newClickHouseDatabase(cfg, cp.conn)
		sum, err := dbch.DirInfo(dir, &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeA1M})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 7)
		So(sum.Size, ShouldEqual, 70)
		So(sum.UIDs, ShouldResemble, []uint32{9, 10})
		So(sum.GIDs, ShouldResemble, []uint32{7})
	})

	Convey("DirInfo returns nil summary for non-empty UID and empty GID filters", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c1-empty/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c1-empty/"

		updatedAt := time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 3)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		dbch := newClickHouseDatabase(cfg, cp.conn)
		sum, err := dbch.DirInfo(
			mountPath,
			&db.Filter{UIDs: []uint32{9}, GIDs: []uint32{}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(sum, ShouldBeNil)
	})

	Convey("Where age-specific filters read vector ages instead of scalar summary columns", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c1-age/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c1-age/"

		updatedAt := time.Date(2026, 6, 1, 15, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		child := mountPath + "a/"

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), mountPath, 7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA6M, 6)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), child, 7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA6M, 6)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), child, 7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 100)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"a"), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		spy := &treeRouteQuerySpyConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, spy))

		dcss, err := tree.Where(
			mountPath,
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeA6M},
			split.SplitsToSplitFn(1),
		)
		So(err, ShouldBeNil)
		So(dirSummariesByDir(dcss)[child[:len(child)-1]].Count, ShouldEqual, 6)
		So(factsQueriesMentionVectorAge(spy.queries), ShouldBeTrue)
		So(factsQueriesMentionScalarSummaryColumns(spy.queries), ShouldBeFalse)
	})

	Convey("Where nil filters normalise to non-directory AgeAll facts reads", t, func() {
		filter := defaultWhereFilter(nil)
		So(filter.FT, ShouldEqual, db.AllTypesExceptDirectories)
		So(filter.Age, ShouldEqual, db.DGUTAgeAll)
	})
}

func assertNoLegacyTreeRouteTables(queries []string) {
	for _, query := range queries {
		lower := strings.ToLower(query)
		So(lower, ShouldNotContainSubstring, "wrstat_dguta")
		So(lower, ShouldNotContainSubstring, "wrstat_files")
	}
}

func treeRouteTablesOutsideAllowlist(queries []string) []string {
	allowed := map[string]bool{
		"wrstat_children":                   true,
		"wrstat_dir_facts":                  true,
		"wrstat_dir_projection_sets":        true,
		"wrstat_mounts_active":              true,
		string(NavigationObjectParentFacts): true,
		"wrstat_virtual_children":           true,
		"wrstat_virtual_children_sets":      true,
		"wrstat_virtual_summary_cache":      true,
		"wrstat_virtual_summary_sets":       true,
	}

	offenders := make([]string, 0)

	for _, table := range treeRouteTablesFromQueries(queries) {
		if !allowed[table] {
			offenders = append(offenders, table)
		}
	}

	return offenders
}

func treeRouteTablesFromQueries(queries []string) []string {
	tables := make([]string, 0)
	replacer := strings.NewReplacer("(", " ", ")", " ", ",", " ", "\n", " ", "\t", " ")

	for _, query := range queries {
		fields := strings.Fields(replacer.Replace(query))
		for i, field := range fields[:max(len(fields)-1, 0)] {
			if !isSQLTablePrefix(field) {
				continue
			}

			table := strings.Trim(fields[i+1], "`")
			if strings.HasPrefix(table, "wrstat_") {
				tables = append(tables, table)
			}
		}
	}

	return tables
}

func isSQLTablePrefix(field string) bool {
	upper := strings.ToUpper(field)

	return upper == "FROM" || upper == "JOIN"
}

func insertC1FactVector(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID, dir string,
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count uint64,
) {
	So(conn.Exec(
		ctx,
		testInsertDGUTAStmt,
		mountPath,
		snapshotID,
		dir,
		gid,
		uid,
		uint16(ft),
		uint8(age),
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func factsQueriesMentionVectorAge(queries []string) bool {
	for _, query := range queries {
		if !strings.Contains(query, "FROM wrstat_dir_facts") {
			continue
		}

		if strings.Contains(query, "gids, uids, fts, ages, counts") {
			return true
		}
	}

	return false
}

func factsQueriesMentionScalarSummaryColumns(queries []string) bool {
	for _, query := range queries {
		if !strings.Contains(query, "FROM wrstat_dir_facts") {
			continue
		}

		if strings.Contains(query, "all_count") || strings.Contains(query, "file_count") {
			return true
		}
	}

	return false
}

func TestClickHouseDatabaseParentFactsDisktreeRoutingC3(t *testing.T) {
	Convey("C3.1 ready parent facts route DiskTree child summaries through one parent range", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testT283ImagingMountPath}

		parentDir := testT283ImagingMountPath + "wide/"
		updatedAt := time.Date(2026, 6, 7, 16, 30, 0, 0, time.UTC)
		seedC3ParentFactsDisktree(t, cfg, testT283ImagingMountPath, updatedAt, parentDir, 2)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		actualTree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		actual, err := actualTree.DirInfo(parentDir, nil)
		So(err, ShouldBeNil)
		So(actual.Current.Count, ShouldEqual, uint64(1))
		So(actual.Children, ShouldHaveLength, 2)
		So(actual.Children[0].Dir, ShouldEqual, parentDir+"child000")
		So(actual.Children[0].Count, ShouldEqual, uint64(2))
		So(actual.Children[0].Size, ShouldEqual, uint64(20))
		So(actual.Children[1].Dir, ShouldEqual, parentDir+"child001")
		So(actual.Children[1].Count, ShouldEqual, uint64(2))
		So(actual.Children[1].Size, ShouldEqual, uint64(21))
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.parentFactScalarQueries(), ShouldEqual, 1)
		So(countingConn.parentFactVectorQueries(), ShouldEqual, 0)
		So(countingConn.dirFactsINQueries(), ShouldEqual, 0)

		hasChildren := actualTree.DirsHaveChildren([]string{parentDir + "child000", parentDir + "child001"}, nil)
		So(hasChildren, ShouldResemble, map[string]bool{
			parentDir + "child000": true,
			parentDir + "child001": false,
		})
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.childrenBatchQueries(), ShouldEqual, 0)
	})

	Convey("C3.2 missing parent facts fall back to the legacy children plus facts route", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)
		resetParentFactsFallbackRoutesForTest()
		Reset(resetParentFactsFallbackRoutesForTest)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c3-fallback/"}

		const mountPath = "/mnt/c3-fallback/"

		parentDir := mountPath + "wide/"
		updatedAt := time.Date(2026, 6, 7, 17, 0, 0, 0, time.UTC)
		sid := seedC3ParentFactsDisktree(t, cfg, mountPath, updatedAt, parentDir, 2)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, dropParentFactsPartitionQuery, mountPath, sid.String()), ShouldBeNil)

		expectedTree := db.NewTree(&clickHouseGenericTreeDB{
			d: newClickHouseDatabase(cfg, conn),
		})
		expected, err := expectedTree.DirInfo(parentDir, nil)
		So(err, ShouldBeNil)
		So(expected.Children, ShouldHaveLength, 2)

		resetSharedTreeQueryCachesForTesting()

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		actualTree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		actual, err := actualTree.DirInfo(parentDir, nil)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.dirFactsINQueries(), ShouldBeGreaterThan, 0)
		So(parentFactsFallbackRouteName(), ShouldEqual, "parent_facts_fallback")
		So(parentFactsFallbackRoutes(), ShouldEqual, uint64(1))
	})

	Convey("C3.4 DiskTree child summary filters keep the intended table routes", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c3-filters/"}

		const mountPath = "/mnt/c3-filters/"

		parentDir := mountPath + "wide/"
		updatedAt := time.Date(2026, 6, 7, 17, 30, 0, 0, time.UTC)
		seedC3ParentFactsDisktree(t, cfg, mountPath, updatedAt, parentDir, 2)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		assertRoute := func(filter *db.Filter, parentFacts, childFilterAll, ageAll, vector bool) {
			resetSharedTreeQueryCachesForTesting()

			countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
			tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
			di, err := tree.DirInfo(parentDir, filter)
			So(err, ShouldBeNil)
			So(di.Children, ShouldNotBeEmpty)

			if parentFacts {
				So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
			} else {
				So(countingConn.parentFactRangeQueries(), ShouldEqual, 0)
			}

			if childFilterAll {
				So(countingConn.childFilterAllPacketQueries(), ShouldEqual, 1)
			} else {
				So(countingConn.childFilterAllPacketQueries(), ShouldEqual, 0)
			}

			assertParentFactsReadShape(countingConn, parentFacts, vector)

			if ageAll {
				So(countingConn.ageAllQueries(), ShouldBeGreaterThan, 0)
			} else {
				So(countingConn.ageAllQueries(), ShouldEqual, 0)
			}
		}

		assertRoute(nil, true, false, false, false)
		assertRoute(&db.Filter{Age: db.DGUTAgeAll}, true, false, false, false)
		assertRoute(&db.Filter{FT: db.AllTypesExceptDirectories, Age: db.DGUTAgeAll}, true, false, false, false)
		assertRoute(
			&db.Filter{GIDs: []uint32{7}, UIDs: []uint32{11}, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll},
			false,
			false,
			true,
			false,
		)
		assertRoute(
			&db.Filter{GIDs: []uint32{7}, UIDs: []uint32{11}, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeA6M},
			true,
			false,
			false,
			true,
		)
	})

	Convey("C3.5 high-fanout DiskTree route reports one parent range and all children", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testT283ImagingMountPath}

		parentDir := testT283ImagingMountPath + "fanout/"
		updatedAt := time.Date(2026, 6, 7, 18, 0, 0, 0, time.UTC)
		seedC3ParentFactsDisktree(t, cfg, testT283ImagingMountPath, updatedAt, parentDir, 305)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		di, err := tree.DirInfo(parentDir, nil)
		So(err, ShouldBeNil)
		So(di.Children, ShouldHaveLength, 305)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.dirFactsINQueries(), ShouldEqual, 0)
	})
}

func seedC3ParentFactsDisktree(
	t *testing.T,
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	parentDir string,
	childCount int,
) uuid.UUID {
	t.Helper()

	sid := snapshotID(mountPath, updatedAt)
	paths := internaltest.NewDirectoryPathCreator()

	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(mountPath)
	w.SetUpdatedAt(updatedAt)

	So(w.Add(db.RecordDGUTA{
		Dir:      paths.ToDirectoryPath(mountPath),
		Children: []string{strings.TrimPrefix(parentDir, mountPath)},
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1, 100, 200),
		},
	}), ShouldBeNil)

	children := make([]string, childCount)
	for i := range childCount {
		children[i] = fmt.Sprintf("child%03d/", i)
	}

	childCountUint, err := strconv.ParseUint(strconv.Itoa(childCount), 10, 64)
	So(err, ShouldBeNil)

	So(w.Add(db.RecordDGUTA{
		Dir:        paths.ToDirectoryPath(parentDir),
		ChildCount: childCountUint,
		Children:   children,
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1, 100, 200),
		},
	}), ShouldBeNil)

	for i, child := range children {
		dir := parentDir + child

		record := db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(dir),
			GUTAs: db.GUTAs{
				b1GUTA(uint32(7+i%3), uint32(11+i%5), db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, uint64(20+i), 100, 200),
				b1GUTA(uint32(7+i%3), uint32(11+i%5), db.DGUTAFileTypeBam, db.DGUTAgeA6M, 1, uint64(10+i), 90, 250),
			},
		}
		if i == 0 {
			record.ChildCount = 1
			record.Children = []string{schema3A2LeafChild}
		}

		So(w.Add(record), ShouldBeNil)
	}

	So(w.Add(db.RecordDGUTA{
		Dir: paths.ToDirectoryPath(parentDir + "child000/" + schema3A2LeafChild),
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
		},
	}), ShouldBeNil)
	So(w.Close(), ShouldBeNil)

	return sid
}

func assertParentFactsReadShape(
	countingConn *parentFactsDisktreeRouteConn,
	parentFacts bool,
	vector bool,
) {
	if vector {
		So(countingConn.parentFactVectorQueries(), ShouldEqual, 1)
		So(countingConn.parentFactScalarQueries(), ShouldEqual, 0)

		return
	}

	So(countingConn.parentFactVectorQueries(), ShouldEqual, 0)

	if parentFacts {
		So(countingConn.parentFactScalarQueries(), ShouldEqual, 1)
	}
}

func TestClickHouseDatabaseParentPacketsA2(t *testing.T) {
	Convey("A2 parent packets are coherent high-fanout request units", t, func() {
		env, cleanup := newSchema3A2ParentPacketEnv(t)
		defer cleanup()

		countingConn := &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)
		tree := db.NewTree(dbch)

		di, err := tree.DirInfo(schema3A2ParentDir, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Children, ShouldHaveLength, schema3A2ChildCount)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)

		firstChild := schema3A2ChildPath(0)

		countingConn.resetCounts()

		summary, err := dbch.DirInfo(firstChild, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(summary, ShouldNotBeNil)
		So(summary.Count, ShouldEqual, uint64(2))
		So(summary.Size, ShouldEqual, uint64(20))
		So(countingConn.queryCountValue(), ShouldEqual, 0)

		hasChildren, err := dbch.DirsHaveChildren([]string{firstChild}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{firstChild: true})
		So(countingConn.queryCountValue(), ShouldEqual, 0)

		resetSharedTreeQueryCachesForTesting()

		countingConn = &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch = newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)
		childPaths := schema3A2ChildPaths()

		summaries, err := dbch.DirInfos(childPaths, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(summaries, ShouldHaveLength, schema3A2ChildCount)
		So(summaries[schema3A2ChildPath(0)].Count, ShouldEqual, uint64(2))
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.dirFactsINQueries(), ShouldEqual, 0)
		So(countingConn.parentFactVectorQueries(), ShouldEqual, 0)

		resetSharedTreeQueryCachesForTesting()

		countingConn = &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch = newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)
		secondChild := schema3A2ChildPath(1)

		summaries, err = dbch.DirInfos([]string{firstChild}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(summaries, ShouldHaveLength, 1)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)

		countingConn.resetCounts()

		sibling, err := dbch.DirInfo(secondChild, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sibling, ShouldNotBeNil)
		So(sibling.Count, ShouldEqual, uint64(2))
		So(sibling.Size, ShouldEqual, uint64(21))
		So(countingConn.queryCountValue(), ShouldEqual, 0)
	})
}

func newSchema3A2ParentPacketEnv(t *testing.T) (schema3A2ParentPacketEnv, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{schema3A2MountPath}

	updatedAt := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	sid := seedC3ParentFactsDisktree(t, cfg, schema3A2MountPath, updatedAt, schema3A2ParentDir, schema3A2ChildCount)

	conn := th.openConn(cfg.DSN)
	cleanup := func() {
		So(conn.Close(), ShouldBeNil)
		resetSharedTreeQueryCachesForTesting()
		os.Unsetenv("WRSTAT_ENV")
	}

	snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
		mountPath:  schema3A2MountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}})

	return schema3A2ParentPacketEnv{cfg: cfg, conn: conn, snapshot: snapshot}, cleanup
}

func schema3A2ChildPath(i int) string {
	return schema3A2ParentDir + fmt.Sprintf("child%03d", i)
}

func schema3A2ChildPaths() []string {
	paths := make([]string, schema3A2ChildCount)
	for i := range schema3A2ChildCount {
		paths[i] = schema3A2ChildPath(i)
	}

	return paths
}

func TestClickHouseDatabaseWhereFrontierPacketsA4(t *testing.T) {
	Convey("A4 Tree.Where reuses one packet for shared-parent frontier dirs", t, func() {
		os.Setenv("WRSTAT_ENV", "test")

		defer os.Unsetenv("WRSTAT_ENV")

		resetSharedTreeQueryCachesForTesting()
		defer resetSharedTreeQueryCachesForTesting()

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 10 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/a4-frontier/"}

		const mountPath = "/mnt/a4-frontier/"

		updatedAt := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
		sid := seedA4SharedFrontierTree(t, cfg, mountPath, updatedAt)
		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})

		conn := th.openConn(cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		filter := &db.Filter{Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)
		genericTree := db.NewTree(&clickHouseGenericTreeDB{
			d: newClickHouseDatabaseWithSnapshot(cfg, conn, snapshot),
		})
		expected, err := genericTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)

		resetSharedTreeQueryCachesForTesting()

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		actualTree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot))
		actual, err := actualTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(e2DCSsDigest(actual), ShouldEqual, e2DCSsDigest(expected))
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 2)
		So(countingConn.childrenBatchQueries(), ShouldEqual, 0)
	})

	Convey("A4 Tree.Where reads each filtered high-fanout packet once", t, func() {
		os.Setenv("WRSTAT_ENV", "test")

		defer os.Unsetenv("WRSTAT_ENV")

		resetSharedTreeQueryCachesForTesting()
		defer resetSharedTreeQueryCachesForTesting()

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 10 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/a4-filtered/"}

		const (
			childCount = 128
			mountPath  = "/mnt/a4-filtered/"
			parentDir  = mountPath + "wide/"
		)

		updatedAt := time.Date(2026, 6, 11, 10, 30, 0, 0, time.UTC)
		sid := seedA4FilteredFanoutTree(t, cfg, mountPath, updatedAt, parentDir, childCount)
		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})

		conn := th.openConn(cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		filter := &db.Filter{FT: db.DGUTAFileTypeOther, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)
		genericTree := db.NewTree(&clickHouseGenericTreeDB{
			d: newClickHouseDatabaseWithSnapshot(cfg, conn, snapshot),
		})
		expected, err := genericTree.Where(parentDir, filter, splitFn)
		So(err, ShouldBeNil)

		resetSharedTreeQueryCachesForTesting()

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		actualTree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot))
		actual, err := actualTree.Where(parentDir, filter, splitFn)
		So(err, ShouldBeNil)
		So(e2DCSsDigest(actual), ShouldEqual, e2DCSsDigest(expected))
		So(countingConn.parentFactVectorQueries(), ShouldEqual, 2)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 2)
		So(countingConn.childrenBatchQueries(), ShouldEqual, 0)
		So(countingConn.queryCountValue(), ShouldBeLessThan, childCount/2)
	})
}

func seedA4SharedFrontierTree(
	t *testing.T,
	cfg Config,
	mountPath string,
	updatedAt time.Time,
) uuid.UUID {
	t.Helper()

	return seedA4Tree(t, cfg, mountPath, updatedAt, []a4TreeRecord{
		{dir: mountPath, count: 10, children: []string{"stem/"}},
		{dir: mountPath + "stem/", count: 10, children: []string{"branch-a/", "branch-b/"}},
		{dir: mountPath + "stem/branch-a/", count: 6},
		{dir: mountPath + "stem/branch-b/", count: 4},
	})
}

func seedA4Tree(
	t *testing.T,
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	records []a4TreeRecord,
) uuid.UUID {
	t.Helper()

	sid := snapshotID(mountPath, updatedAt)
	paths := internaltest.NewDirectoryPathCreator()

	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(mountPath)
	w.SetUpdatedAt(updatedAt)

	for _, record := range records {
		So(w.Add(db.RecordDGUTA{
			Dir:        paths.ToDirectoryPath(record.dir),
			ChildCount: uint64(len(record.children)),
			Children:   record.children,
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeOther, db.DGUTAgeAll, record.count, record.count*10, 100, 200),
			},
		}), ShouldBeNil)
	}

	So(w.Close(), ShouldBeNil)

	return sid
}

func seedA4FilteredFanoutTree(
	t *testing.T,
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	parentDir string,
	childCount int,
) uuid.UUID {
	t.Helper()

	children := make([]string, childCount)

	records := make([]a4TreeRecord, 0, childCount+2)
	for i := range childCount {
		child := fmt.Sprintf("child%03d/", i)
		children[i] = child
		records = append(records, a4TreeRecord{
			dir:   parentDir + child,
			count: uint64(1 + i%3),
		})
	}

	childCountUint, err := strconv.ParseUint(strconv.Itoa(childCount), 10, 64)
	So(err, ShouldBeNil)

	records = append([]a4TreeRecord{
		{dir: mountPath, count: childCountUint * 2, children: []string{"wide/"}},
		{dir: parentDir, count: childCountUint * 2, children: children},
	}, records...)

	return seedA4Tree(t, cfg, mountPath, updatedAt, records)
}

func TestClickHouseDatabaseActiveVirtualOverlayC2(t *testing.T) {
	Convey("C2 mixed8 active virtual scenarios share one read-only ClickHouse fixture", t, func() {
		seeds := c2Mixed8MountSeeds()

		env, cleanup := newC2ActiveVirtualEnv(t, seeds)
		defer cleanup()

		assertC2ActiveVirtualSummaries(env)
		assertC2FullFilterVirtualDirInfo(env)
		assertC2Mixed8VirtualPaths(env, seeds)
		assertC2VirtualRootWhere(env, seeds)
		assertC2RESTWhereRoutes(t, env, seeds)
		assertC2LiteralAgeVirtualTree(t, env, seeds)
	})

	Convey("C2.3 synthetic 100-small-NFS active set keeps /nfs/ virtual", t, func() {
		assertC2SyntheticSmallNFSActiveSet(t)
	})
}

func assertC2ActiveVirtualSummaries(env c2ActiveVirtualEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows := readC2ActiveVirtualSummaries(ctx, env.conn, env.activeSetID)
	So(rows["/"].count, ShouldEqual, uint64(1_750_001))
	So(rows["/"].size, ShouldEqual, uint64(61_484_536_134_482))
	So(rows["/"].childCount, ShouldEqual, 8)
	So(rows["/lustre/"].count, ShouldEqual, uint64(1_500_001))
	So(rows["/lustre/"].size, ShouldEqual, uint64(61_176_182_464_512))
	So(rows["/lustre/"].childCount, ShouldEqual, 7)
	So(rows["/nfs/"].count, ShouldEqual, uint64(250_000))
	So(rows["/nfs/"].size, ShouldEqual, uint64(308_353_669_970))
	So(rows["/nfs/"].childCount, ShouldEqual, 1)

	dbch := newClickHouseDatabaseWithSnapshot(env.cfg, env.providerConn, env.snapshot)
	root, err := dbch.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
	So(err, ShouldBeNil)
	So(root.Count, ShouldEqual, uint64(1_750_001))

	lustre, err := dbch.DirInfo("/lustre/", &db.Filter{Age: db.DGUTAgeAll})
	So(err, ShouldBeNil)
	So(lustre.Count, ShouldEqual, uint64(1_500_001))

	children, err := dbch.Children("/lustre/")
	So(err, ShouldBeNil)
	So(children, ShouldHaveLength, 7)
}

func assertC2FullFilterVirtualDirInfo(env c2ActiveVirtualEnv) {
	countingConn := &c2ActiveVirtualRouteCountingConn{Conn: env.providerConn}
	dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

	filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{11}, Age: db.DGUTAgeAll}
	for _, expected := range c2Mixed8ExpectedSummaries() {
		sum, err := dbch.DirInfo(expected.dir, filter)
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, expected.count)
		So(sum.Size, ShouldEqual, expected.size)
	}

	So(countingConn.activeVirtualFilterReadsValue(), ShouldBeGreaterThan, 0)
	So(countingConn.dirFactReadsValue(), ShouldEqual, 0)
}

func assertC2SyntheticSmallNFSActiveSet(t *testing.T) {
	t.Helper()

	suffix := fmt.Sprintf("c2%d", time.Now().UnixNano())
	seeds := make([]c2MountSeed, 0, 100)

	base := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	for i := range 100 {
		seeds = append(seeds, c2MountSeed{
			mountPath: fmt.Sprintf("/nfs/%s-project%03d/", suffix, i),
			updatedAt: base.Add(time.Duration(i) * time.Minute),
			count:     1,
		})
	}

	mounts := make([]activeMount, 0, len(seeds))
	for _, seed := range seeds {
		mounts = append(mounts, activeMount{
			mountPath:  seed.mountPath,
			snapshotID: snapshotID(seed.mountPath, seed.updatedAt).String(),
			updatedAt:  seed.updatedAt,
		})
	}

	childRows := activeVirtualChildRowsForMounts("c2-shape", mounts, base)
	So(activeVirtualSummaryRowsForChildren("c2-shape", mounts, childRows, base), ShouldHaveLength, 102)
	So(childRows, ShouldHaveLength, 101)

	env, cleanup := newC2ActiveVirtualEnv(t, seeds)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(countRows(ctx, env.conn, d1CountActiveSetRowsQuery("wrstat_active_virtual_summaries"), env.activeSetID),
		ShouldEqual, 102)
	So(countRows(ctx, env.conn, d1CountActiveSetRowsQuery("wrstat_active_virtual_children"), env.activeSetID),
		ShouldEqual, 101)

	dbch := newClickHouseDatabaseWithSnapshot(env.cfg, env.providerConn, env.snapshot)
	children, err := dbch.Children("/nfs/")
	So(err, ShouldBeNil)
	So(children, ShouldHaveLength, 100)
	So(children[0], ShouldEqual, fmt.Sprintf("/nfs/%s-project000", suffix))
	So(children[99], ShouldEqual, fmt.Sprintf("/nfs/%s-project099", suffix))

	sum, err := dbch.DirInfo("/nfs/", &db.Filter{Age: db.DGUTAgeAll})
	So(err, ShouldBeNil)
	So(sum.Count, ShouldEqual, 100)
}

func assertC2Mixed8VirtualPaths(env c2ActiveVirtualEnv, seeds []c2MountSeed) {
	for _, tc := range c2Mixed8FilterMatrix() {
		expectedSummaries := c2Mixed8ExpectedSummariesForSeedsAndAge(seeds, tc.ageDivisor)
		So(expectedSummaries, ShouldHaveLength, 11)

		resetSharedTreeQueryCachesForTesting()
		ResetSchema3FallbackRoutes()

		countingConn := &c2ActiveVirtualRouteCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		for _, expected := range expectedSummaries {
			sum, err := dbch.DirInfo(expected.dir, tc.filter)
			So(err, ShouldBeNil)
			So(c2DirSummaryDigest(sum), ShouldEqual, c2ExpectedDirSummaryDigest(expected, tc.expectedAge))
		}

		c2AssertActiveVirtualOnlyRoute(countingConn)

		resetSharedTreeQueryCachesForTesting()
		ResetSchema3FallbackRoutes()

		countingConn = &c2ActiveVirtualRouteCountingConn{Conn: env.providerConn}
		dbch = newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		dirs := c2ExpectedSummaryDirs(expectedSummaries)
		summaries, err := dbch.DirInfos(dirs, tc.filter)
		So(err, ShouldBeNil)

		for _, expected := range expectedSummaries {
			sum := summaries[expected.dir]
			So(c2DirSummaryDigest(sum), ShouldEqual, c2ExpectedDirSummaryDigest(expected, tc.expectedAge))
		}

		c2AssertActiveVirtualOnlyRoute(countingConn)
	}
}

func assertC2VirtualRootWhere(env c2ActiveVirtualEnv, seeds []c2MountSeed) {
	cases := []struct {
		name    string
		dir     string
		age     db.DirGUTAge
		divisor uint64
	}{
		{name: "root where unused 1y", dir: "/", age: db.DirGUTAge(4), divisor: 10},
		{name: "nfs where unchanged 1y", dir: nfsAncestor, age: db.DirGUTAge(12), divisor: 20},
	}

	for _, tc := range cases {
		ResetSchema3FallbackRoutes()

		countingConn := &c2ActiveVirtualRouteCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		got, err := db.NewTree(dbch).Where(
			tc.dir,
			&db.Filter{Age: tc.age},
			split.SplitsToSplitFn(2),
		)
		So(err, ShouldBeNil)
		So(c2WhereDigest(got), ShouldEqual,
			c2WhereDigest(c2ExpectedWhereSummaries(seeds, tc.dir, tc.age, tc.divisor)))
		So(countingConn.activeVirtualReadsValue(), ShouldBeGreaterThan, 0)
		So(countingConn.dirFactReadsValue(), ShouldEqual, 0)
		So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
	}
}

func assertC2RESTWhereRoutes(t *testing.T, env c2ActiveVirtualEnv, seeds []c2MountSeed) {
	t.Helper()

	cases := []struct {
		name    string
		dir     string
		age     db.DirGUTAge
		ageText string
		divisor uint64
	}{
		{name: "root rest where unused 1y", dir: "/", age: db.DGUTAgeA1Y, ageText: "A1Y", divisor: 10},
		{name: "nfs rest where unchanged 1y", dir: nfsAncestor, age: db.DGUTAgeM1Y, ageText: "M1Y", divisor: 20},
	}

	for _, tc := range cases {
		ResetSchema3FallbackRoutes()

		route, closeProvider := newC2ActiveVirtualRouteServer(t, env.cfg, env.snapshot, false)
		defer closeProvider()

		got := requestC2RESTWhere(t, route.server, url.Values{
			"dir":    {tc.dir},
			"age":    {tc.ageText},
			"splits": {"2"},
		})

		So(c2RESTWhereDigest(got), ShouldEqual,
			c2ExpectedRESTWhereDigest(c2ExpectedWhereSummaries(seeds, tc.dir, tc.age, tc.divisor)))
		So(route.server.ResponseCacheHits(), ShouldEqual, uint64(0))
		So(route.counting.activeVirtualReadsValue(), ShouldBeGreaterThan, 0)
		So(route.counting.dirFactReadsValue(), ShouldEqual, 0)
		So(route.counting.factVectorReadsValue(), ShouldEqual, 0)
		So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))

		ResetSchema3FallbackRoutes()

		cliRoute, closeCLIProvider := newC2ActiveVirtualRouteServer(t, env.cfg, env.snapshot, true)
		defer closeCLIProvider()

		cliGot := requestC2CLIWhere(t, cliRoute, tc.dir, tc.age)
		So(c2RESTWhereDigest(cliGot), ShouldEqual,
			c2ExpectedRESTWhereDigest(c2ExpectedWhereSummaries(seeds, tc.dir, tc.age, tc.divisor)))
		So(cliRoute.server.ResponseCacheHits(), ShouldEqual, uint64(0))
		So(cliRoute.counting.activeVirtualReadsValue(), ShouldBeGreaterThan, 0)
		So(cliRoute.counting.dirFactReadsValue(), ShouldEqual, 0)
		So(cliRoute.counting.factVectorReadsValue(), ShouldEqual, 0)
		So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
	}
}

func assertC2LiteralAgeVirtualTree(t *testing.T, env c2ActiveVirtualEnv, seeds []c2MountSeed) {
	t.Helper()

	cases := []struct {
		name          string
		dir           string
		age           db.DirGUTAge
		divisor       uint64
		expectedChild []string
	}{
		{
			name:          "root tree unused 1y",
			dir:           "/",
			age:           db.DirGUTAge(4),
			divisor:       10,
			expectedChild: []string{"/lustre", "/nfs"},
		},
		{
			name:          "nfs tree unchanged 1y",
			dir:           nfsAncestor,
			age:           db.DirGUTAge(12),
			divisor:       20,
			expectedChild: c2SeedMountPathsWithPrefix(seeds, nfsAncestor),
		},
	}

	for _, tc := range cases {
		ResetSchema3FallbackRoutes()

		route, closeProvider := newC2ActiveVirtualRouteServer(t, env.cfg, env.snapshot, true)
		defer closeProvider()

		got := requestC2AuthTree(t, route, tc.dir, tc.age)
		So(got.Children, ShouldHaveLength, len(tc.expectedChild))
		So(c2RESTTreeCurrentDigest(got), ShouldEqual,
			c2ExpectedRESTTreeDigest(db.DCSs{
				c2ExpectedDirSummary(tc.dir, c2ExpectedVirtualSummary(tc.dir, tc.divisor).count,
					c2ExpectedVirtualSummary(tc.dir, tc.divisor).size, tc.age),
			}))
		So(c2RESTTreeChildrenDigest(got.Children), ShouldEqual,
			c2ExpectedRESTTreeDigest(c2ExpectedTreeChildren(seeds, tc.expectedChild, tc.age, tc.divisor)))
		So(route.server.ResponseCacheHits(), ShouldEqual, uint64(0))
		So(route.counting.activeVirtualReadsValue(), ShouldBeGreaterThan, 0)
		So(route.counting.dirFactReadsValue(), ShouldEqual, 0)
		So(route.counting.factVectorReadsValue(), ShouldEqual, 0)
		So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
	}
}

func newC2ActiveVirtualEnv(t *testing.T, seeds []c2MountSeed) (c2ActiveVirtualEnv, func()) {
	t.Helper()
	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{c2LustreAncestor}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	cp, ok := p.(*chProvider)
	So(ok, ShouldBeTrue)

	conn := th.openConn(cfg.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	offset := time.Duration(time.Now().UnixNano()%int64(time.Second)) * time.Nanosecond

	rows := make([]mountsActiveRow, 0, len(seeds))
	for _, seed := range seeds {
		seed.updatedAt = seed.updatedAt.Add(offset)
		rows = append(rows, seedC2ActiveVirtualMount(ctx, conn, seed))
	}

	activeSetID := fingerprintForMountsActive(rows)
	So(writeC2ActiveVirtualOverlay(ctx, conn, rows, activeSetID), ShouldBeNil)

	cleanup := func() {
		cancel()
		So(conn.Close(), ShouldBeNil)
		So(p.Close(), ShouldBeNil)
		os.Unsetenv("WRSTAT_ENV")
		resetSharedTreeQueryCachesForTesting()
	}

	return c2ActiveVirtualEnv{
		cfg:          cfg,
		conn:         conn,
		provider:     cp,
		providerConn: cp.conn,
		snapshot:     newActiveMountsSnapshot(rows),
		activeSetID:  activeSetID,
	}, cleanup
}

func seedC2ActiveVirtualMount(ctx context.Context, conn ch.Conn, seed c2MountSeed) mountsActiveRow {
	sid := snapshotID(seed.mountPath, seed.updatedAt)

	size := seed.size
	if size == 0 {
		size = seed.count * 10
	}

	So(conn.Exec(ctx, testInsertMountStmt, seed.mountPath, time.Now(), sid, seed.updatedAt), ShouldBeNil)
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		seed.mountPath,
		sid.String(),
		seed.mountPath,
		uint32(7),
		uint32(11),
		uint16(db.DGUTAFileTypeBam),
		uint8(db.DGUTAgeAll),
		seed.count,
		size,
		int64(10),
		int64(20),
		[]uint64{seed.count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, seed.count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
	So(conn.Exec(ctx,
		insertDirFilterAllForTest,
		seed.mountPath,
		sid.String(),
		uint8(db.DGUTAgeAll),
		uint32(7),
		uint32(11),
		uint16(db.DGUTAFileTypeBam),
		seed.mountPath,
		parentFactsParentDir(seed.mountPath),
		seed.count,
		size,
		int64(10),
		int64(20),
		[]uint64{seed.count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, seed.count, 0, 0, 0, 0, 0, 0, 0},
		uint64(0),
		uint64(1),
		uint8(0),
		uint8(1),
	), ShouldBeNil)

	for _, ageRow := range []struct {
		age   db.DirGUTAge
		count uint64
		size  uint64
	}{
		{age: db.DGUTAgeA1Y, count: seed.count / 10, size: size / 10},
		{age: db.DGUTAgeM1Y, count: seed.count / 20, size: size / 20},
	} {
		So(conn.Exec(ctx,
			insertDirFilterAllForTest,
			seed.mountPath,
			sid.String(),
			uint8(ageRow.age),
			uint32(7),
			uint32(11),
			uint16(db.DGUTAFileTypeBam),
			seed.mountPath,
			parentFactsParentDir(seed.mountPath),
			ageRow.count,
			ageRow.size,
			int64(10),
			int64(20),
			[]uint64{ageRow.count, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, ageRow.count, 0, 0, 0, 0, 0, 0, 0},
			uint64(0),
			uint64(1),
			uint8(0),
			uint8(1),
		), ShouldBeNil)
	}

	So(conn.Exec(
		ctx,
		testInsertChildrenStmt,
		seed.mountPath,
		sid.String(),
		seed.mountPath,
		seed.mountPath+"leaf",
	), ShouldBeNil)

	return mountsActiveRow{mountPath: seed.mountPath, snapshotID: sid.String(), updatedAt: seed.updatedAt}
}

func writeC2ActiveVirtualOverlay(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
	activeSetID string,
) error {
	mounts := newActiveMountsSnapshot(rows).all()
	refreshedAt := time.Now().UTC()

	for _, query := range activeVirtualPartitionDropQueries() {
		if err := dropActiveSetPartition(ctx, conn, query, activeSetID, "test active virtual"); err != nil {
			return err
		}
	}

	rootGUTAs, err := queryActiveVirtualRootGUTAs(ctx, conn, mounts)
	if err != nil {
		return err
	}

	rootFilterRows, err := queryActiveVirtualRootFilterRows(ctx, conn, mounts)
	if err != nil {
		return err
	}

	childCounts, err := queryActiveVirtualMountChildCounts(ctx, conn, mounts)
	if err != nil {
		return err
	}

	summaryRows, filterRows, childRows := activeVirtualRowsForMountsFromData(
		activeSetID,
		mounts,
		refreshedAt,
		rootGUTAs,
		rootFilterRows,
		childCounts,
	)

	writer := newActiveVirtualOverlayWriter(conn, 1000)
	if err := appendActiveVirtualOverlayRows(ctx, writer, summaryRows, filterRows, childRows); err != nil {
		return err
	}

	if err := writer.flush(ctx); err != nil {
		return err
	}

	setRow := activeVirtualSetRowForRows(activeSetID, rows, summaryRows, filterRows, childRows, refreshedAt)

	return writer.appendSet(ctx, setRow)
}

func c2Mixed8MountSeeds() []c2MountSeed {
	suffix := fmt.Sprintf("c2%d", time.Now().UnixNano())
	base := time.Date(2026, 6, 11, 8, 0, 0, 0, time.UTC)
	seeds := make([]c2MountSeed, 0, 8)

	for i := range 6 {
		seeds = append(seeds, c2MountSeed{
			mountPath: fmt.Sprintf("/lustre/%s-project%03d/", suffix, i),
			updatedAt: base.Add(time.Duration(i) * time.Minute),
			count:     200_000,
			size:      8_000_000_000_000,
		})
	}

	seeds = append(seeds, c2MountSeed{
		mountPath: fmt.Sprintf("/lustre/%s-project006/", suffix),
		updatedAt: base.Add(6 * time.Minute),
		count:     300_001,
		size:      13_176_182_464_512,
	})
	seeds = append(seeds, c2MountSeed{
		mountPath: fmt.Sprintf("/nfs/%s-project000/", suffix),
		updatedAt: base.Add(7 * time.Minute),
		count:     250_000,
		size:      308_353_669_970,
	})

	return seeds
}

func readC2ActiveVirtualSummaries(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
) map[string]c2ActiveVirtualSummaryForTest {
	rows, err := conn.Query(ctx,
		"SELECT dir, all_count, all_size, child_count, is_mount_root_box "+
			"FROM wrstat_active_virtual_summaries WHERE active_set_id = ?",
		activeSetID,
	)

	So(err, ShouldBeNil)
	defer func() { So(rows.Close(), ShouldBeNil) }()

	out := make(map[string]c2ActiveVirtualSummaryForTest)

	for rows.Next() {
		var (
			dir string
			row c2ActiveVirtualSummaryForTest
		)
		So(rows.Scan(&dir, &row.count, &row.size, &row.childCount, &row.isMountRootBox), ShouldBeNil)
		out[dir] = row
	}

	So(rows.Err(), ShouldBeNil)

	return out
}

func c2Mixed8ExpectedSummaries() []struct {
	dir   string
	count uint64
	size  uint64
} {
	return []struct {
		dir   string
		count uint64
		size  uint64
	}{
		{dir: "/", count: 1_750_001, size: 61_484_536_134_482},
		{dir: c2LustreAncestor, count: 1_500_001, size: 61_176_182_464_512},
		{dir: nfsAncestor, count: 250_000, size: 308_353_669_970},
	}
}

func c2Mixed8FilterMatrix() []struct {
	name        string
	filter      *db.Filter
	expectedAge db.DirGUTAge
	ageDivisor  uint64
} {
	return []struct {
		name        string
		filter      *db.Filter
		expectedAge db.DirGUTAge
		ageDivisor  uint64
	}{
		{name: "nil default", filter: nil, expectedAge: db.DGUTAgeAll, ageDivisor: 1},
		{name: "default empty", filter: &db.Filter{}, expectedAge: db.DGUTAgeAll, ageDivisor: 1},
		{name: "age all", filter: &db.Filter{Age: db.DGUTAgeAll}, expectedAge: db.DGUTAgeAll, ageDivisor: 1},
		{
			name:        "file only",
			filter:      &db.Filter{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories},
			expectedAge: db.DGUTAgeAll,
			ageDivisor:  1,
		},
		{name: "gid", filter: &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}, expectedAge: db.DGUTAgeAll, ageDivisor: 1},
		{name: "uid", filter: &db.Filter{UIDs: []uint32{11}, Age: db.DGUTAgeAll}, expectedAge: db.DGUTAgeAll, ageDivisor: 1},
		{
			name:        "type bitmask",
			filter:      &db.Filter{FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll},
			expectedAge: db.DGUTAgeAll,
			ageDivisor:  1,
		},
		{
			name: "owner user type",
			filter: &db.Filter{
				GIDs: []uint32{7},
				UIDs: []uint32{11},
				FT:   db.DGUTAFileTypeBam,
				Age:  db.DGUTAgeAll,
			},
			expectedAge: db.DGUTAgeAll,
			ageDivisor:  1,
		},
		{name: "unused 1y", filter: &db.Filter{Age: db.DGUTAgeA1Y}, expectedAge: db.DGUTAgeA1Y, ageDivisor: 10},
		{name: "unchanged 1y", filter: &db.Filter{Age: db.DGUTAgeM1Y}, expectedAge: db.DGUTAgeM1Y, ageDivisor: 20},
	}
}

func c2Mixed8ExpectedSummariesForSeedsAndAge(seeds []c2MountSeed, divisor uint64) []struct {
	dir   string
	count uint64
	size  uint64
} {
	out := c2Mixed8ExpectedSummariesForAge(divisor)

	for _, seed := range seeds {
		out = append(out, struct {
			dir   string
			count uint64
			size  uint64
		}{
			dir:   seed.mountPath,
			count: seed.count / divisor,
			size:  c2SeedSize(seed) / divisor,
		})
	}

	return out
}

func c2Mixed8ExpectedSummariesForAge(divisor uint64) []struct {
	dir   string
	count uint64
	size  uint64
} {
	out := c2Mixed8ExpectedSummaries()
	if divisor == 1 {
		return out
	}

	for i := range out {
		out[i].count /= divisor
		out[i].size /= divisor
	}

	if divisor == 20 {
		out[0].size = 3_074_226_806_723
		out[1].size = 3_058_809_123_225
	}

	return out
}

func c2DirSummaryDigest(sum *db.DirSummary) string {
	So(sum, ShouldNotBeNil)

	return c2SummaryDigest(c2DigestSummary{
		Count: sum.Count,
		Size:  sum.Size,
		UIDs:  append([]uint32(nil), sum.UIDs...),
		GIDs:  append([]uint32(nil), sum.GIDs...),
		FT:    uint16(sum.FT),
		Age:   uint8(sum.Age),
	})
}

func c2SummaryDigest(summary c2DigestSummary) string {
	sort.Slice(summary.UIDs, func(i, j int) bool { return summary.UIDs[i] < summary.UIDs[j] })
	sort.Slice(summary.GIDs, func(i, j int) bool { return summary.GIDs[i] < summary.GIDs[j] })

	data, err := json.Marshal(summary)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func c2ExpectedDirSummaryDigest(
	expected struct {
		dir   string
		count uint64
		size  uint64
	},
	age db.DirGUTAge,
) string {
	return c2SummaryDigest(c2DigestSummary{
		Count: expected.count,
		Size:  expected.size,
		UIDs:  []uint32{11},
		GIDs:  []uint32{7},
		FT:    uint16(db.DGUTAFileTypeBam),
		Age:   uint8(age),
	})
}

func c2AssertActiveVirtualOnlyRoute(countingConn *c2ActiveVirtualRouteCountingConn) {
	So(countingConn.activeVirtualReadsValue(), ShouldBeGreaterThan, 0)
	So(countingConn.dirFactReadsValue(), ShouldEqual, 0)
	So(countingConn.factVectorReadsValue(), ShouldEqual, 0)
	So(ReadSchema3FallbackRoutes()[parentFactsFallbackRouteName()], ShouldEqual, uint64(0))
}

func c2ExpectedSummaryDirs(expectedSummaries []struct {
	dir   string
	count uint64
	size  uint64
}) []string {
	dirs := make([]string, 0, len(expectedSummaries))
	for _, expected := range expectedSummaries {
		dirs = append(dirs, expected.dir)
	}

	return dirs
}

func c2WhereDigest(summaries db.DCSs) string {
	elements := make([]c2WhereDigestSummary, 0, len(summaries))
	for _, sum := range summaries {
		elements = append(elements, c2WhereDigestSummary{
			Dir:   sum.Dir,
			Count: sum.Count,
			Size:  sum.Size,
			UIDs:  append([]uint32(nil), sum.UIDs...),
			GIDs:  append([]uint32(nil), sum.GIDs...),
			FT:    uint16(sum.FT),
			Age:   uint8(sum.Age),
		})
	}

	sort.Slice(elements, func(i, j int) bool {
		return elements[i].Dir < elements[j].Dir
	})

	for i := range elements {
		sort.Slice(elements[i].UIDs, func(a, b int) bool { return elements[i].UIDs[a] < elements[i].UIDs[b] })
		sort.Slice(elements[i].GIDs, func(a, b int) bool { return elements[i].GIDs[a] < elements[i].GIDs[b] })
	}

	data, err := json.Marshal(elements)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func c2ExpectedWhereSummaries(
	seeds []c2MountSeed,
	dir string,
	age db.DirGUTAge,
	divisor uint64,
) db.DCSs {
	var out db.DCSs

	for _, expected := range c2Mixed8ExpectedSummariesForAge(divisor) {
		if c2WhereIncludesVirtualSummary(dir, expected.dir) {
			out = append(out, c2ExpectedDirSummary(c2WhereResultDir(expected.dir), expected.count, expected.size, age))
		}
	}

	for _, seed := range seeds {
		if c2WhereIncludesMountBox(dir, seed.mountPath) {
			out = append(out, c2ExpectedDirSummary(
				c2WhereResultDir(seed.mountPath),
				seed.count/divisor,
				c2SeedSize(seed)/divisor,
				age,
			))
		}
	}

	return out
}

func c2WhereIncludesVirtualSummary(queryDir, summaryDir string) bool {
	switch ensureTrailingSlash(queryDir) {
	case "/":
		return summaryDir == "/" || summaryDir == "/lustre/"
	case "/nfs/":
		return false
	default:
		return summaryDir == ensureTrailingSlash(queryDir)
	}
}

func c2ExpectedDirSummary(dir string, count uint64, size uint64, age db.DirGUTAge) *db.DirSummary {
	return &db.DirSummary{
		Dir:   dir,
		Count: count,
		Size:  size,
		UIDs:  []uint32{11},
		GIDs:  []uint32{7},
		FT:    db.DGUTAFileTypeBam,
		Age:   age,
	}
}

func c2WhereResultDir(dir string) string {
	if dir == "/" {
		return dir
	}

	return strings.TrimSuffix(dir, "/")
}

func c2WhereIncludesMountBox(queryDir, mountPath string) bool {
	queryDir = ensureTrailingSlash(queryDir)

	return queryDir == "/" || strings.HasPrefix(mountPath, queryDir)
}

func c2SeedSize(seed c2MountSeed) uint64 {
	if seed.size != 0 {
		return seed.size
	}

	return seed.count * 10
}

func newC2ActiveVirtualRouteServer(
	t *testing.T,
	cfg Config,
	snapshot *activeMountsSnapshot,
	withAuthTree bool,
) (c2ActiveVirtualRouteServer, func()) {
	t.Helper()

	p, counting := newC2ActiveVirtualRouteProvider(t, cfg, snapshot)
	s := server.New(gas.NewStringLogger())
	setC2RouteServerNames(s)

	route := c2ActiveVirtualRouteServer{
		server:   s,
		provider: p,
		counting: counting,
	}

	if withAuthTree {
		cert, key, err := gas.CreateTestCert(t)
		So(err, ShouldBeNil)
		So(s.EnableAuth(cert, key, func(_, _ string) (bool, string) {
			return true, ""
		}), ShouldBeNil)
		So(s.AddTreePage(), ShouldBeNil)

		route.cert = cert
		route.key = key
	}

	So(s.SetProvider(p), ShouldBeNil)
	counting.reset()

	if withAuthTree {
		addr, stop, err := gas.StartTestServer(s, route.cert, route.key)
		So(err, ShouldBeNil)

		token, err := gas.Login(gas.NewClientRequest(addr, route.cert), "c2", "pass")
		So(err, ShouldBeNil)

		route.addr = addr
		route.token = token
		route.stop = stop
	}

	return route, func() {
		if route.stop != nil {
			So(route.stop(), ShouldBeNil)
		}

		So(p.Close(), ShouldBeNil)
		ResetTreeQueryCaches()
	}
}

func newC2ActiveVirtualRouteProvider(
	t *testing.T,
	cfg Config,
	snapshot *activeMountsSnapshot,
) (*chProvider, *c2ActiveVirtualRouteCountingConn) {
	t.Helper()

	conn, err := connectFromConfigContext(context.Background(), cfg)
	So(err, ShouldBeNil)

	counting := &c2ActiveVirtualRouteCountingConn{Conn: conn}
	p := newChProvider(cfg, counting)

	dbImpl := newClickHouseDatabaseWithSnapshot(cfg, counting, snapshot)
	tree := db.NewTree(dbImpl)
	bd, err := newClickHouseBaseDirsReaderWithSnapshot(cfg, counting, snapshot)
	So(err, ShouldBeNil)
	So(p.publishLazyReaders(dbImpl, tree, bd, snapshot.fingerprint), ShouldBeTrue)

	return p, counting
}

func setC2RouteServerNames(s *server.Server) {
	s.SetCachedUserName(11, "c2-user-11")
	s.SetCachedGroupName(7, "c2-group-7")
}

func requestC2RESTWhere(t *testing.T, s *server.Server, values url.Values) []*server.DirSummary {
	t.Helper()

	response, err := gas.QueryREST(s.Router(), server.EndPointWhere, "?"+values.Encode())
	So(err, ShouldBeNil)

	if response.Code != http.StatusOK {
		t.Logf("C2 REST where failed with %d: %s", response.Code, response.Body.String())
	}

	So(response.Code, ShouldEqual, http.StatusOK)

	var got []*server.DirSummary
	So(json.Unmarshal(response.Body.Bytes(), &got), ShouldBeNil)

	return got
}

func c2RESTWhereDigest(summaries []*server.DirSummary) string {
	elements := make([]c2RESTDigestSummary, 0, len(summaries))
	for _, sum := range summaries {
		elements = append(elements, c2RESTDigestSummary{
			Dir:       sum.Dir,
			Count:     sum.Count,
			Size:      sum.Size,
			Users:     c2SortedStrings(sum.Users),
			Groups:    c2SortedStrings(sum.Groups),
			FileTypes: c2SortedStrings(sum.FileTypes),
			Age:       sum.Age,
		})
	}

	return c2RESTDigest(elements)
}

func c2SortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)

	return out
}

func c2RESTDigest(elements []c2RESTDigestSummary) string {
	sort.Slice(elements, func(i, j int) bool {
		return elements[i].Dir < elements[j].Dir
	})

	data, err := json.Marshal(elements)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func c2ExpectedRESTWhereDigest(summaries db.DCSs) string {
	return c2ExpectedRESTTreeDigest(summaries)
}

func c2ExpectedRESTTreeDigest(summaries db.DCSs) string {
	elements := make([]c2RESTDigestSummary, 0, len(summaries))
	for _, sum := range summaries {
		elements = append(elements, c2RESTDigestSummary{
			Dir:       sum.Dir,
			Count:     sum.Count,
			Size:      sum.Size,
			Users:     c2RESTUsers(sum.UIDs),
			Groups:    c2RESTGroups(sum.GIDs),
			FileTypes: c2RESTFileTypes(sum.FT),
			Age:       sum.Age,
		})
	}

	return c2RESTDigest(elements)
}

func c2RESTUsers(uids []uint32) []string {
	out := make([]string, 0, len(uids))
	for _, uid := range uids {
		if uid == 11 {
			out = append(out, "c2-user-11")
		}
	}

	return c2SortedStrings(out)
}

func c2RESTGroups(gids []uint32) []string {
	out := make([]string, 0, len(gids))
	for _, gid := range gids {
		if gid == 7 {
			out = append(out, "c2-group-7")
		}
	}

	return c2SortedStrings(out)
}

func c2RESTFileTypes(ft db.DirGUTAFileType) []string {
	if ft == 0 {
		return nil
	}

	return []string{ft.String()}
}

func requestC2CLIWhere(
	t *testing.T,
	route c2ActiveVirtualRouteServer,
	dir string,
	age db.DirGUTAge,
) []*server.DirSummary {
	t.Helper()

	tokenDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", tokenDir)

	client, err := gas.NewClientCLI(
		"jwt",
		"server-token",
		route.addr,
		route.cert,
		false,
	)
	So(err, ShouldBeNil)
	So(client.Login("c2", "pass"), ShouldBeNil)

	_, got, err := server.GetWhereDataIs(client, dir, "", "", "", age, "2")
	So(err, ShouldBeNil)

	return got
}

func c2SeedMountPathsWithPrefix(seeds []c2MountSeed, prefix string) []string {
	out := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		if strings.HasPrefix(seed.mountPath, prefix) {
			out = append(out, c2WhereResultDir(seed.mountPath))
		}
	}

	sort.Strings(out)

	return out
}

func requestC2AuthTree(
	t *testing.T,
	route c2ActiveVirtualRouteServer,
	dir string,
	age db.DirGUTAge,
) server.TreeElement {
	t.Helper()

	response, err := gas.NewAuthenticatedClientRequest(route.addr, route.cert, route.token).
		SetResult(&server.TreeElement{}).
		ForceContentType("application/json").
		SetQueryParams(map[string]string{
			"path": dir,
			"age":  strconv.Itoa(int(age)),
		}).
		Get(server.EndPointAuthTree)
	So(err, ShouldBeNil)

	if response.StatusCode() != http.StatusOK {
		t.Logf("C2 auth tree failed with %d: %s", response.StatusCode(), response.String())
	}

	So(response.StatusCode(), ShouldEqual, http.StatusOK)

	got, ok := response.Result().(*server.TreeElement)
	So(ok, ShouldBeTrue)
	So(got, ShouldNotBeNil)

	return *got
}

func c2RESTTreeCurrentDigest(got server.TreeElement) string {
	return c2RESTDigest([]c2RESTDigestSummary{{
		Dir:       got.Path,
		Count:     got.Count,
		Size:      got.Size,
		Users:     c2SortedStrings(got.Users),
		Groups:    c2SortedStrings(got.Groups),
		FileTypes: c2SortedStrings(got.FileTypes),
		Age:       got.Age,
	}})
}

func c2ExpectedVirtualSummary(dir string, divisor uint64) struct {
	dir   string
	count uint64
	size  uint64
} {
	key := ensureTrailingSlash(dir)
	for _, expected := range c2Mixed8ExpectedSummariesForAge(divisor) {
		if expected.dir == key {
			return expected
		}
	}

	return struct {
		dir   string
		count uint64
		size  uint64
	}{dir: key}
}

func c2RESTTreeChildrenDigest(children []*server.TreeElement) string {
	elements := make([]c2RESTDigestSummary, 0, len(children))
	for _, child := range children {
		elements = append(elements, c2RESTDigestSummary{
			Dir:       child.Path,
			Count:     child.Count,
			Size:      child.Size,
			Users:     c2SortedStrings(child.Users),
			Groups:    c2SortedStrings(child.Groups),
			FileTypes: c2SortedStrings(child.FileTypes),
			Age:       child.Age,
		})
	}

	return c2RESTDigest(elements)
}

func c2ExpectedTreeChildren(
	seeds []c2MountSeed,
	childDirs []string,
	age db.DirGUTAge,
	divisor uint64,
) db.DCSs {
	out := make(db.DCSs, 0, len(childDirs))
	for _, childDir := range childDirs {
		if strings.HasPrefix(ensureTrailingSlash(childDir), "/nfs/") && ensureTrailingSlash(childDir) != "/nfs/" {
			out = append(out, c2ExpectedSeedSummary(seeds, childDir, age, divisor))

			continue
		}

		out = append(out, c2ExpectedDirSummary(
			childDir,
			c2ExpectedVirtualSummary(childDir, divisor).count,
			c2ExpectedVirtualSummary(childDir, divisor).size,
			age,
		))
	}

	return out
}

func c2ExpectedSeedSummary(seeds []c2MountSeed, dir string, age db.DirGUTAge, divisor uint64) *db.DirSummary {
	for _, seed := range seeds {
		if seed.mountPath == ensureTrailingSlash(dir) {
			return c2ExpectedDirSummary(dir, seed.count/divisor, c2SeedSize(seed)/divisor, age)
		}
	}

	return c2ExpectedDirSummary(dir, 0, 0, age)
}

func TestClickHouseDatabaseAllFilterReaderRoutingB2(t *testing.T) {
	Convey("B2.1 full-filter DirInfos match facts-vector summaries for every filter family", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		rawDB := newClickHouseDatabase(env.cfg, env.providerConn)
		countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		dirs := []string{env.mount.mountPath + "a", env.mount.mountPath + "b/"}

		for _, filter := range b2AllFilterFamilies() {
			expected := env.expectedDirInfos(rawDB, dirs, filter)

			countingConn.reset()

			actual, err := dbch.DirInfos(dirs, filter)
			So(err, ShouldBeNil)
			So(actual, ShouldResemble, expected)
			So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
			So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
			So(countingConn.dirFilterAllQueryCount()+countingConn.childFilterAllQueryCount(), ShouldBeGreaterThan, 0)
		}
	})

	Convey("B2.2 DirInfo sums GID lists and file-type bitmasks for an age-specific full filter", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		filter := &db.Filter{
			GIDs: []uint32{7, 9},
			FT:   db.DGUTAFileTypeBam | db.DGUTAFileTypeCram,
			Age:  db.DGUTAgeA1M,
		}

		sum, err := dbch.DirInfo(env.mount.mountPath+"a/", filter)
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, uint64(6))
		So(sum.Size, ShouldEqual, uint64(60))
		So(sum.UIDs, ShouldResemble, []uint32{11, 12})
		So(sum.GIDs, ShouldResemble, []uint32{7, 9})
		So(sum.FT, ShouldEqual, db.DGUTAFileTypeBam|db.DGUTAFileTypeCram)
		So(sum.Age, ShouldEqual, db.DGUTAgeA1M)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
		So(countingConn.dirFilterAllQueryCount()+countingConn.childFilterAllQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("B2.3 empty UID and GID filters return empty DirInfos and Where results without full-filter reads", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		dirs := []string{env.mount.mountPath + "a", env.mount.mountPath + "b"}

		for _, filter := range []*db.Filter{
			{GIDs: []uint32{}, Age: db.DGUTAgeAll},
			{UIDs: []uint32{}, Age: db.DGUTAgeAll},
		} {
			countingConn.reset()

			summaries, err := dbch.DirInfos(dirs, filter)
			So(err, ShouldBeNil)
			So(summaries, ShouldHaveLength, 0)
			So(countingConn.dirFilterAllQueryCount(), ShouldEqual, 0)
			So(countingConn.childFilterAllQueryCount(), ShouldEqual, 0)
		}

		whereConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, whereConn))
		dcss, err := tree.Where(
			env.mount.mountPath,
			&db.Filter{GIDs: []uint32{}, Age: db.DGUTAgeAll},
			split.SplitsToSplitFn(1),
		)
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 0)
		So(whereConn.queryCountValue(), ShouldEqual, 0)
	})

	Convey("B2.4 age-specific DirInfos do not query AgeAll rows or facts vectors", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		summaries, err := dbch.DirInfos(
			[]string{env.mount.mountPath + "a", env.mount.mountPath + "b"},
			&db.Filter{Age: db.DGUTAgeA1M},
		)
		So(err, ShouldBeNil)
		So(summaries, ShouldHaveLength, 2)
		So(countingConn.dirFilterAllQueryCount()+countingConn.childFilterAllQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.ageAllQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
	})

	Convey("B2.5 broad default and scalar file-only DirInfos keep facts routes", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		dirs := []string{env.mount.mountPath + "a", env.mount.mountPath + "b"}

		for _, filter := range []*db.Filter{
			{Age: db.DGUTAgeAll},
			{},
			{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories},
		} {
			resetSharedTreeQueryCachesForTesting()

			countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
			dbch := newClickHouseDatabase(env.cfg, countingConn)

			summaries, err := dbch.DirInfos(dirs, filter)
			So(err, ShouldBeNil)
			So(summaries, ShouldHaveLength, 2)
			So(countingConn.mountDirSummaryQueryCount(), ShouldBeGreaterThan, 0)
			So(countingConn.dirFilterAllQueryCount(), ShouldEqual, 0)
			So(countingConn.childFilterAllQueryCount(), ShouldEqual, 0)
		}
	})

	Convey("B2.6 owner-user-type and age-specific DirInfos use only full-filter summaries", t, func() {
		env, cleanup := newB2AllFilterReaderEnv(t)
		defer cleanup()

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		dirs := []string{env.mount.mountPath + "a", env.mount.mountPath + "b"}

		for _, filter := range []*db.Filter{
			{
				GIDs: []uint32{7, 9},
				UIDs: []uint32{11, 12},
				FT:   db.DGUTAFileTypeBam | db.DGUTAFileTypeCram,
				Age:  db.DGUTAgeAll,
			},
			{Age: db.DGUTAgeM1Y},
		} {
			countingConn.reset()

			summaries, err := dbch.DirInfos(dirs, filter)
			So(err, ShouldBeNil)
			So(summaries, ShouldHaveLength, 2)
			So(countingConn.dirFilterAllQueryCount()+countingConn.childFilterAllQueryCount(), ShouldBeGreaterThan, 0)
			So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
			So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
			So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		}
	})
}

func newB2AllFilterReaderEnv(t *testing.T) (b2AllFilterReaderEnv, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 5 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{"/mnt/b2-allfilter/"}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	cp, ok := p.(*chProvider)
	So(ok, ShouldBeTrue)

	conn := th.openConn(cfg.DSN)
	mountPath := "/mnt/b2-allfilter/"
	updatedAt := time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC)
	sid := snapshotID(mountPath, updatedAt).String()
	mount := activeMount{mountPath: mountPath, snapshotID: sid, updatedAt: updatedAt}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
	So(conn.Exec(ctx, createDirFilterAllTableForTest), ShouldBeNil)
	So(conn.Exec(ctx, createChildFilterAllTableForTest), ShouldBeNil)
	So(conn.Exec(ctx, createSchema3SnapshotSetsTableForTest), ShouldBeNil)
	seedB2AllFilterFacts(ctx, conn, mount)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
	seedB2AllFilterRows(ctx, conn, mount)

	cleanup := func() {
		So(conn.Close(), ShouldBeNil)
		So(p.Close(), ShouldBeNil)
		os.Unsetenv("WRSTAT_ENV")
		resetSharedTreeQueryCachesForTesting()
	}

	return b2AllFilterReaderEnv{
		cfg:          cfg,
		conn:         conn,
		providerConn: cp.conn,
		mount:        mount,
	}, cleanup
}

func seedB2AllFilterFacts(ctx context.Context, conn ch.Conn, mount activeMount) {
	for _, record := range b2AllFilterRecords(mount.mountPath) {
		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mount.mountPath,
			mount.snapshotID,
			record.dir,
			record.gid,
			record.uid,
			uint16(record.ft),
			uint8(record.age),
			record.count,
			record.size,
			record.atime,
			record.mtime,
			b2Bucket(record.count),
			b2MtimeBucket(record.count),
		), ShouldBeNil)
	}

	So(conn.Exec(
		ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, mount.mountPath, mount.mountPath+"a",
	), ShouldBeNil)
	So(conn.Exec(
		ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, mount.mountPath, mount.mountPath+"b",
	), ShouldBeNil)
}

func b2AllFilterRecords(mountPath string) []b2AllFilterRecord {
	return []b2AllFilterRecord{
		b2Rec(mountPath+"a/", db.DGUTAgeAll, 7, 11, db.DGUTAFileTypeBam, 3, 100, 200),
		b2Rec(mountPath+"a/", db.DGUTAgeAll, 9, 12, db.DGUTAFileTypeCram, 5, 80, 250),
		b2Rec(mountPath+"a/", db.DGUTAgeAll, 8, 13, db.DGUTAFileTypeOther, 7, 90, 230),
		b2Rec(mountPath+"a/", db.DGUTAgeA1M, 7, 11, db.DGUTAFileTypeBam, 2, 110, 210),
		b2Rec(mountPath+"a/", db.DGUTAgeA1M, 9, 12, db.DGUTAFileTypeCram, 4, 120, 220),
		b2Rec(mountPath+"a/", db.DGUTAgeA1M, 8, 13, db.DGUTAFileTypeBam, 6, 130, 240),
		b2Rec(mountPath+"a/", db.DGUTAgeA1Y, 7, 11, db.DGUTAFileTypeBam, 8, 70, 180),
		b2Rec(mountPath+"a/", db.DGUTAgeM1Y, 7, 11, db.DGUTAFileTypeBam, 9, 60, 170),
		b2Rec(mountPath+"b/", db.DGUTAgeAll, 7, 11, db.DGUTAFileTypeBam, 10, 100, 200),
		b2Rec(mountPath+"b/", db.DGUTAgeAll, 9, 12, db.DGUTAFileTypeCram, 11, 80, 250),
		b2Rec(mountPath+"b/", db.DGUTAgeA1M, 7, 11, db.DGUTAFileTypeBam, 12, 110, 210),
		b2Rec(mountPath+"b/", db.DGUTAgeA1Y, 9, 12, db.DGUTAFileTypeCram, 13, 70, 180),
		b2Rec(mountPath+"b/", db.DGUTAgeM1Y, 7, 11, db.DGUTAFileTypeBam, 14, 60, 170),
	}
}

func b2Rec(
	dir string,
	age db.DirGUTAge,
	gid, uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
	atime, mtime int64,
) b2AllFilterRecord {
	return b2AllFilterRecord{
		dir:   dir,
		age:   age,
		gid:   gid,
		uid:   uid,
		ft:    ft,
		count: count,
		size:  count * 10,
		atime: atime,
		mtime: mtime,
	}
}

func b2Bucket(count uint64) []uint64 {
	return []uint64{count, 0, 0, 0, 0, 0, 0, 0, 0}
}

func b2MtimeBucket(count uint64) []uint64 {
	return []uint64{0, count, 0, 0, 0, 0, 0, 0, 0}
}

func seedB2AllFilterRows(ctx context.Context, conn ch.Conn, mount activeMount) {
	So(conn.Exec(
		ctx,
		"ALTER TABLE wrstat_dir_filter_all DELETE "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) SETTINGS mutations_sync = 1",
		mount.mountPath,
		mount.snapshotID,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		"ALTER TABLE wrstat_child_filter_all DELETE "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) SETTINGS mutations_sync = 1",
		mount.mountPath,
		mount.snapshotID,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		"ALTER TABLE wrstat_schema3_snapshot_sets DELETE "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) AND schema3_version = ? "+
			"SETTINGS mutations_sync = 1",
		mount.mountPath,
		mount.snapshotID,
		currentSchemaVersion,
	), ShouldBeNil)

	batch, err := conn.PrepareBatch(ctx, insertDirFilterAllForTest)
	So(err, ShouldBeNil)
	appendB2DirFilterAllRows(batch, mount)
	So(batch.Send(), ShouldBeNil)

	childBatch, err := conn.PrepareBatch(ctx, insertChildFilterAllForTest)
	So(err, ShouldBeNil)
	appendB2ChildFilterAllRows(childBatch, mount)
	So(childBatch.Send(), ShouldBeNil)

	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetForTest,
		mount.mountPath,
		mount.snapshotID,
		currentSchemaVersion,
		uint64(2),
		uint64(1),
		uint64(2),
		uint64(len(b2AllFilterRecords(mount.mountPath))),
		uint64(len(b2AllFilterRecords(mount.mountPath))),
		"b2-all-filter",
	), ShouldBeNil)
}

func appendB2DirFilterAllRows(batch interface{ Append(values ...any) error }, mount activeMount) {
	for _, record := range b2AllFilterRecords(mount.mountPath) {
		So(batch.Append(
			mount.mountPath,
			mount.snapshotID,
			uint8(record.age),
			record.gid,
			record.uid,
			uint16(record.ft),
			record.dir,
			parentFactsParentDir(record.dir),
			record.count,
			record.size,
			record.atime,
			record.mtime,
			b2Bucket(record.count),
			b2MtimeBucket(record.count),
			uint64(0),
			uint64(0),
			uint8(0),
			uint8(0),
			time.Now(),
		), ShouldBeNil)
	}
}

func appendB2ChildFilterAllRows(batch interface{ Append(values ...any) error }, mount activeMount) {
	for _, record := range b2AllFilterRecords(mount.mountPath) {
		So(batch.Append(
			mount.mountPath,
			mount.snapshotID,
			parentFactsParentDir(record.dir),
			uint8(record.age),
			record.gid,
			record.uid,
			uint16(record.ft),
			record.dir,
			record.count,
			record.size,
			record.atime,
			record.mtime,
			b2Bucket(record.count),
			b2MtimeBucket(record.count),
			uint64(0),
			uint64(0),
			uint8(0),
			uint8(0),
			time.Now(),
		), ShouldBeNil)
	}
}

func b2AllFilterFamilies() []*db.Filter {
	return []*db.Filter{
		{UIDs: []uint32{11}, Age: db.DGUTAgeAll},
		{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		{FT: db.DGUTAFileTypeBam | db.DGUTAFileTypeCram, Age: db.DGUTAgeAll},
		{GIDs: []uint32{7, 9}, UIDs: []uint32{11, 12}, FT: db.DGUTAFileTypeBam | db.DGUTAFileTypeCram, Age: db.DGUTAgeAll},
		{GIDs: []uint32{7, 9}, Age: db.DGUTAgeAll},
		{Age: db.DGUTAgeA1M},
		{Age: db.DGUTAgeA1Y},
		{Age: db.DGUTAgeM1Y},
	}
}

func TestClickHouseDatabaseWhereDirFilterAllB3(t *testing.T) {
	Convey("B3 full-filter Tree.Where scenarios share one read-only ClickHouse fixture", t, func() {
		env, cleanup := newB3WhereDirFilterAllEnv(t)
		defer cleanup()

		assertB3WhereMatchesFactsVectors(env)
		assertB3HighFanoutWhere(env)
		assertB3ClickHouseProjectWhereFixtures(env)
	})

	Convey("B3.2 AgeAll keeps the narrow route only when exact and strictly faster", t, func() {
		dbch := &clickHouseDatabase{}
		mount := activeMount{mountPath: "/mnt/b3-ageall/", snapshotID: uuid.NewString()}
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		previous := whereDirAgeAllRouteEvidenceFor
		defer func() { whereDirAgeAllRouteEvidenceFor = previous }()

		cases := []struct {
			name     string
			evidence whereDirAgeAllRouteEvidence
			expected bool
		}{
			{
				name: "strictly faster",
				evidence: whereDirAgeAllRouteEvidence{
					ageAllExact: true,
					allExact:    true,
					ageAllP95:   90 * time.Millisecond,
					allP95:      100 * time.Millisecond,
				},
				expected: true,
			},
			{
				name: "equal p95",
				evidence: whereDirAgeAllRouteEvidence{
					ageAllExact: true,
					allExact:    true,
					ageAllP95:   100 * time.Millisecond,
					allP95:      100 * time.Millisecond,
				},
				expected: false,
			},
			{
				name: "all-filter faster",
				evidence: whereDirAgeAllRouteEvidence{
					ageAllExact: true,
					allExact:    true,
					ageAllP95:   110 * time.Millisecond,
					allP95:      100 * time.Millisecond,
				},
				expected: false,
			},
			{
				name: "not exact",
				evidence: whereDirAgeAllRouteEvidence{
					ageAllExact: true,
					allExact:    false,
					ageAllP95:   90 * time.Millisecond,
					allP95:      100 * time.Millisecond,
				},
				expected: false,
			},
		}

		for _, tc := range cases {
			whereDirAgeAllRouteEvidenceFor = func(
				context.Context,
				*clickHouseDatabase,
				activeMount,
				string,
				*db.Filter,
			) (whereDirAgeAllRouteEvidence, bool, error) {
				return tc.evidence, true, nil
			}

			got, err := dbch.useDirFilterAgeAllForWhere(context.Background(), mount, mount.mountPath, filter)
			So(err, ShouldBeNil)
			So(got, ShouldEqual, tc.expected)
		}
	})
}

func assertB3WhereMatchesFactsVectors(env b3WhereDirFilterAllEnv) {
	filter := &db.Filter{
		GIDs: []uint32{7},
		UIDs: []uint32{11},
		FT:   db.DGUTAFileTypeBam,
		Age:  db.DGUTAgeA1Y,
	}
	splitFn := split.SplitsToSplitFn(2)
	genericTree := db.NewTree(&clickHouseGenericTreeDB{
		d: newClickHouseDatabaseWithSnapshot(env.cfg, env.conn, env.snapshot),
	})
	expected, err := genericTree.Where(env.projectDir, filter, splitFn)
	So(err, ShouldBeNil)

	resetSharedTreeQueryCachesForTesting()

	countingConn := &whereQueryCountingConn{Conn: env.conn}
	fastTree := db.NewTree(newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot))
	actual, err := fastTree.Where(env.projectDir, filter, splitFn)
	So(err, ShouldBeNil)

	So(b3WhereSummaryDirs(actual), ShouldResemble, []string{
		env.projectDir,
		env.projectDir + "alpha",
		env.projectDir + "beta",
		env.projectDir + "gamma",
		env.projectDir + "delta",
	})
	So(b3WhereSummarySizes(actual), ShouldResemble, []uint64{200, 100, 80, 60, 40})
	So(b3DCSsDigest(actual), ShouldEqual, b3DCSsDigest(expected))
	So(countingConn.filterAllQueryCountValue(), ShouldBeGreaterThan, 0)
	So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
	So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
	So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
}

func assertB3HighFanoutWhere(env b3WhereDirFilterAllEnv) {
	resetSharedTreeQueryCachesForTesting()
	ResetSchema3FallbackRoutes()

	countingConn := &whereQueryCountingConn{Conn: env.conn}
	tree := db.NewTree(newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot))
	filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{11}, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeA1Y}
	inspector := &Inspector{cfg: env.cfg, conn: env.conn}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var dcss db.DCSs

	metrics, err := inspector.Measure(ctx, func(context.Context) error {
		var whereErr error

		dcss, whereErr = tree.Where(env.projectDir, filter, split.SplitsToSplitFn(2))

		return whereErr
	})

	So(err, ShouldBeNil)
	So(dcss, ShouldHaveLength, 5)
	So(len(b3WhereChildDirs(env.projectDir)), ShouldBeGreaterThan, 100)
	So(countingConn.filterAllQueryCountValue(), ShouldEqual, 1)
	So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
	So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
	So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)

	ceiling := b3ReadVolumeCeiling(ctx, env.conn, "wrstat_dir_filter_all", uint64(len(dcss))).
		add(b3ReadVolumeCeiling(ctx, env.conn, "wrstat_schema3_snapshot_sets", 1))

	So(metrics.ReadRows, ShouldBeGreaterThan, uint64(0))
	So(metrics.ReadRows, ShouldBeLessThanOrEqualTo, ceiling.rows)
	So(metrics.ReadBytes, ShouldBeGreaterThan, uint64(0))
	So(metrics.ReadBytes, ShouldBeLessThanOrEqualTo, ceiling.bytes)
	So(metrics.ReadMarks, ShouldBeGreaterThan, uint64(0))
	So(metrics.ReadMarks, ShouldBeLessThanOrEqualTo, ceiling.marks)
	So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
}

func assertB3ClickHouseProjectWhereFixtures(env b3WhereDirFilterAllEnv) {
	for _, fixture := range b3ClickHouseProjectWhereFixtures() {
		resetSharedTreeQueryCachesForTesting()
		ResetSchema3FallbackRoutes()

		countingConn := &whereQueryCountingConn{Conn: env.conn}
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot))
		dcss, err := tree.Where(
			env.projectDir,
			&db.Filter{Age: fixture.age},
			split.SplitsToSplitFn(2),
		)

		So(err, ShouldBeNil)
		So(b3DCSsDigest(dcss), ShouldEqual, b3ClickHouseProjectWhereManifestDigest(fixture.manifestKey))
		So(countingConn.filterAllQueryCountValue(), ShouldEqual, 1)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
		So(ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
	}
}

func newB3WhereDirFilterAllEnv(t *testing.T) (b3WhereDirFilterAllEnv, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0

	const mountPath = "/m/"

	cfg.MountPoints = []string{mountPath}

	provider, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	conn := th.openConn(cfg.DSN)
	projectDir := mountPath + "project/"
	updatedAt := time.Date(2026, 6, 11, 11, 0, 0, 0, time.UTC)
	sid := snapshotID(mountPath, updatedAt).String()
	mount := activeMount{mountPath: mountPath, snapshotID: sid, updatedAt: updatedAt}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
	insertMountDirProjectionSetForTest(ctx, conn, mount)
	So(conn.Exec(ctx, createDirFilterAllTableForTest), ShouldBeNil)
	So(conn.Exec(ctx, createSchema3SnapshotSetsTableForTest), ShouldBeNil)
	seedB3WhereFacts(ctx, conn, mount, projectDir)
	seedB3WhereDirFilterAll(ctx, conn, mount, projectDir)

	snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
		mountPath:  mountPath,
		snapshotID: sid,
		updatedAt:  updatedAt,
	}})
	cleanup := func() {
		So(conn.Close(), ShouldBeNil)
		So(provider.Close(), ShouldBeNil)
		os.Unsetenv("WRSTAT_ENV")
		resetSharedTreeQueryCachesForTesting()
	}

	return b3WhereDirFilterAllEnv{
		cfg:        cfg,
		conn:       conn,
		provider:   provider,
		snapshot:   snapshot,
		projectDir: projectDir,
	}, cleanup
}

func seedB3WhereFacts(ctx context.Context, conn ch.Conn, mount activeMount, projectDir string) {
	for _, row := range b3WhereRows(projectDir) {
		So(conn.Exec(
			ctx,
			testInsertDGUTAStmt,
			mount.mountPath,
			mount.snapshotID,
			row.dir,
			row.gid,
			row.uid,
			uint16(row.ft),
			uint8(row.age),
			row.count,
			row.size,
			int64(100),
			int64(200),
			[]uint64{row.count, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, row.count, 0, 0, 0, 0, 0, 0, 0},
		), ShouldBeNil)
	}

	for _, child := range b3WhereChildDirs(projectDir) {
		So(conn.Exec(ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, projectDir, child), ShouldBeNil)
	}
}

func b3WhereRows(projectDir string) []b3WhereRow {
	wideRows := b3WhereHighFanoutRows(projectDir)
	rows := make([]b3WhereRow, 0, 11+len(wideRows))
	rows = append(rows,
		b3WhereMatchingRow(projectDir, 20),
		b3WhereMatchingRow(projectDir+"alpha/", 10),
		b3WhereMatchingRow(projectDir+"beta/", 8),
		b3WhereMatchingRow(projectDir+"gamma/", 6),
		b3WhereMatchingRow(projectDir+"delta/", 4),
		b3WhereRow{dir: projectDir, age: db.DGUTAgeM1Y, gid: 8, uid: 12, ft: db.DGUTAFileTypeCram, count: 8, size: 80},
		b3WhereRow{
			dir:   projectDir + "mtime-alpha/",
			age:   db.DGUTAgeM1Y,
			gid:   8,
			uid:   12,
			ft:    db.DGUTAFileTypeCram,
			count: 5,
			size:  50,
		},
		b3WhereRow{
			dir:   projectDir + "mtime-omega/",
			age:   db.DGUTAgeM1Y,
			gid:   9,
			uid:   13,
			ft:    db.DGUTAFileTypeOther,
			count: 3,
			size:  30,
		},
		b3WhereRow{
			dir:   projectDir + "other-gid/",
			age:   db.DGUTAgeA1Y,
			gid:   8,
			uid:   11,
			ft:    db.DGUTAFileTypeBam,
			count: 100,
			size:  1000,
		},
		b3WhereRow{
			dir:   projectDir + "other-uid/",
			age:   db.DGUTAgeA1Y,
			gid:   7,
			uid:   12,
			ft:    db.DGUTAFileTypeBam,
			count: 100,
			size:  1000,
		},
		b3WhereRow{
			dir:   projectDir + "other-type/",
			age:   db.DGUTAgeA1Y,
			gid:   7,
			uid:   11,
			ft:    db.DGUTAFileTypeCram,
			count: 100,
			size:  1000,
		},
	)

	return append(rows, wideRows...)
}

func b3WhereHighFanoutRows(projectDir string) []b3WhereRow {
	const highFanoutChildren = 128

	rows := make([]b3WhereRow, highFanoutChildren)
	for i := range highFanoutChildren {
		rows[i] = b3WhereRow{
			dir:   fmt.Sprintf("%swide-child-%03d/", projectDir, i),
			age:   db.DGUTAgeA2Y,
			gid:   17,
			uid:   23,
			ft:    db.DGUTAFileTypeOther,
			count: 1,
			size:  1,
		}
	}

	return rows
}

func b3WhereMatchingRow(dir string, count uint64) b3WhereRow {
	return b3WhereRow{
		dir:   dir,
		age:   db.DGUTAgeA1Y,
		gid:   7,
		uid:   11,
		ft:    db.DGUTAFileTypeBam,
		count: count,
		size:  count * 10,
	}
}

func b3WhereChildDirs(projectDir string) []string {
	wideRows := b3WhereHighFanoutRows(projectDir)
	dirs := make([]string, 0, 9+len(wideRows))
	dirs = append(dirs,
		projectDir+"alpha",
		projectDir+"beta",
		projectDir+"gamma",
		projectDir+"delta",
		projectDir+"mtime-alpha",
		projectDir+"mtime-omega",
		projectDir+"other-gid",
		projectDir+"other-uid",
		projectDir+"other-type",
	)

	for _, row := range wideRows {
		dirs = append(dirs, strings.TrimSuffix(row.dir, "/"))
	}

	return dirs
}

func seedB3WhereDirFilterAll(ctx context.Context, conn ch.Conn, mount activeMount, projectDir string) {
	batch, err := conn.PrepareBatch(ctx, insertDirFilterAllForTest)
	So(err, ShouldBeNil)

	for _, row := range b3WhereRows(projectDir) {
		So(batch.Append(
			mount.mountPath,
			mount.snapshotID,
			uint8(row.age),
			row.gid,
			row.uid,
			uint16(row.ft),
			row.dir,
			parentFactsParentDir(row.dir),
			row.count,
			row.size,
			int64(100),
			int64(200),
			[]uint64{row.count, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, row.count, 0, 0, 0, 0, 0, 0, 0},
			uint64(0),
			uint64(0),
			uint8(0),
			uint8(0),
			time.Now(),
		), ShouldBeNil)
	}

	So(batch.Send(), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetForTest,
		mount.mountPath,
		mount.snapshotID,
		currentSchemaVersion,
		uint64(len(b3WhereRows(projectDir))),
		uint64(0),
		uint64(len(b3WhereChildDirs(projectDir))),
		uint64(0),
		uint64(len(b3WhereRows(projectDir))),
		"b3-where-dir-filter-all",
	), ShouldBeNil)
}

func b3WhereSummaryDirs(dcss db.DCSs) []string {
	dirs := make([]string, len(dcss))
	for i, summary := range dcss {
		dirs[i] = summary.Dir
	}

	return dirs
}

func b3WhereSummarySizes(dcss db.DCSs) []uint64 {
	sizes := make([]uint64, len(dcss))
	for i, summary := range dcss {
		sizes[i] = summary.Size
	}

	return sizes
}

func b3DCSsDigest(dcss db.DCSs) string {
	summaries := make([]b3DigestSummary, len(dcss))
	for i, summary := range dcss {
		uids := append([]uint32(nil), summary.UIDs...)
		gids := append([]uint32(nil), summary.GIDs...)

		sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
		sort.Slice(gids, func(i, j int) bool { return gids[i] < gids[j] })
		summaries[i] = b3DigestSummary{
			Dir:   summary.Dir,
			Count: summary.Count,
			Size:  summary.Size,
			UIDs:  uids,
			GIDs:  gids,
			FT:    uint16(summary.FT),
			Age:   uint8(summary.Age),
		}
	}

	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Dir == summaries[j].Dir {
			return summaries[i].Age < summaries[j].Age
		}

		return summaries[i].Dir < summaries[j].Dir
	})

	data, err := json.Marshal(summaries)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func b3ReadVolumeCeiling(ctx context.Context, conn ch.Conn, table string, expectedRows uint64) b3ReadVolume {
	const indexGranularity = 8192

	compressedBytes, partRows := b3ActivePartStats(ctx, conn, table)
	So(partRows, ShouldBeGreaterThan, uint64(0))
	So(compressedBytes, ShouldBeGreaterThan, uint64(0))

	marks := uint64(math.Ceil(float64(expectedRows)/indexGranularity)) + 2
	if marks < 1 {
		marks = 1
	}

	bytesPerRow := float64(compressedBytes) / float64(partRows)

	return b3ReadVolume{
		rows:  marks * indexGranularity,
		bytes: uint64(math.Ceil(float64(marks*indexGranularity) * bytesPerRow * 1.25)),
		marks: marks,
	}
}

func b3ActivePartStats(ctx context.Context, conn ch.Conn, table string) (uint64, uint64) {
	row := conn.QueryRow(ctx,
		"SELECT toUInt64(sum(data_compressed_bytes)), toUInt64(sum(rows)) "+
			"FROM system.parts WHERE database = currentDatabase() AND table = ? AND active",
		table,
	)

	var compressedBytes, rows uint64
	So(row.Scan(&compressedBytes, &rows), ShouldBeNil)

	return compressedBytes, rows
}

func b3ClickHouseProjectWhereFixtures() []struct {
	manifestKey string
	age         db.DirGUTAge
} {
	return []struct {
		manifestKey string
		age         db.DirGUTAge
	}{
		{manifestKey: "project_where_unused_1y", age: db.DGUTAgeA1Y},
		{manifestKey: "project_where_unchanged_1y", age: db.DGUTAgeM1Y},
	}
}

func b3ClickHouseProjectWhereManifestDigest(key string) string {
	return map[string]string{
		"project_where_unused_1y":    "sha256:3b698287472f7a06cc1176561fbc201abcef5cba226af1870ee72381594ec821",
		"project_where_unchanged_1y": "sha256:8498f9d82f8b5ea244d3a4b71c153b0b2ad4796d94b49fe8b08f15ba563db2b9",
	}[key]
}

func TestClickHouseDatabaseExactSummaryRoutesA1(t *testing.T) {
	Convey("A1.1 broad current summaries use wrstat_dir_facts over parent-fact duplicates", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/m/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountPath = "/m/"
			dir       = "/m/a/"
		)

		updatedAt := time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, dir, db.DGUTAFileTypeBam, 7, 3)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)
		insertA1ParentFactDuplicate(ctx, conn, mountPath, sid.String(), updatedAt, dir, 999, 9990)

		countingConn := &parentFactsDisktreeRouteConn{Conn: conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		sum, err := dbch.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, uint64(3))
		So(sum.Size, ShouldEqual, uint64(30))
		So(countingConn.dirFactsINQueries(), ShouldEqual, 1)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 0)
	})

	Convey("A1.2 ready full-filter current summaries use wrstat_dir_filter_all", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/m/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountPath = "/m/"
			dir       = "/m/a/"
		)

		updatedAt := time.Date(2026, 6, 11, 9, 15, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})
		insertA1VectorGUTA(ctx, conn, mountPath, sid.String(), dir, 7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 99)
		createAndSeedA1DirFilterAll(ctx, conn, mountPath, sid.String(), dir)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{11},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeA1M,
		}

		sum, err := dbch.DirInfo(dir, filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, uint64(5))
		So(sum.GIDs, ShouldResemble, []uint32{7})
		So(sum.UIDs, ShouldResemble, []uint32{11})
		So(sum.FT, ShouldEqual, db.DGUTAFileTypeBam)
		So(countingConn.dirFilterAllQueryCount(), ShouldEqual, 1)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})
}

func insertA1ParentFactDuplicate(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	updatedAt time.Time,
	dir string,
	count uint64,
	size uint64,
) {
	So(conn.Exec(ctx, insertParentFactsQuery,
		mountPath,
		sid,
		parentFactsParentDir(dir),
		dir,
		updatedAt,
		count,
		size,
		int64(100),
		int64(200),
		a1AgeBuckets(count),
		a1AgeBuckets(count),
		[]uint32{7},
		[]uint32{11},
		uint16(db.DGUTAFileTypeBam),
		count,
		size,
		int64(100),
		int64(200),
		a1AgeBuckets(count),
		a1AgeBuckets(count),
		[]uint32{7},
		[]uint32{11},
		uint16(db.DGUTAFileTypeBam),
		[]uint32{7},
		[]uint32{11},
		[]uint16{uint16(db.DGUTAFileTypeBam)},
		[]uint8{uint8(db.DGUTAgeAll)},
		[]uint64{count},
		[]uint64{size},
		[]int64{100},
		[]int64{200},
		[][]uint64{a1AgeBuckets(count)},
		[][]uint64{a1AgeBuckets(count)},
		uint64(0),
		uint8(0),
		updatedAt,
	), ShouldBeNil)
}

func a1AgeBuckets(count uint64) []uint64 {
	return []uint64{count, 0, 0, 0, 0, 0, 0, 0, 0}
}

func insertA1VectorGUTA(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	dir string,
	gid uint32,
	uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count uint64,
) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mountPath,
		sid,
		dir,
		gid,
		uid,
		uint16(ft),
		uint8(age),
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func createAndSeedA1DirFilterAll(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	dir string,
) {
	So(conn.Exec(ctx, createDirFilterAllTableForTest), ShouldBeNil)
	So(conn.Exec(ctx, createSchema3SnapshotSetsTableForTest), ShouldBeNil)
	So(conn.Exec(
		ctx,
		"ALTER TABLE wrstat_schema3_snapshot_sets DELETE "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) AND schema3_version = ? "+
			"SETTINGS mutations_sync = 1",
		mountPath,
		sid,
		currentSchemaVersion,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetForTest,
		mountPath,
		sid,
		currentSchemaVersion,
		1,
		1,
		0,
		0,
		1,
		"test",
	), ShouldBeNil)
	So(conn.Exec(ctx, insertDirFilterAllForTest,
		mountPath,
		sid,
		uint8(db.DGUTAgeA1M),
		uint32(7),
		uint32(11),
		uint16(db.DGUTAFileTypeBam),
		dir,
		parentFactsParentDir(dir),
		uint64(5),
		uint64(50),
		int64(100),
		int64(200),
		a1AgeBuckets(5),
		a1AgeBuckets(5),
		uint64(0),
		uint64(0),
		uint8(0),
		uint8(0),
	), ShouldBeNil)
}

func TestClickHouseDatabaseDirInfo(t *testing.T) {
	Convey("DirInfo does not read active facts before projection readiness exists", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/not-ready/"

		updatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 2)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"child/"), ShouldBeNil)

		guardedConn := &dirFactsReadinessGuardConn{Conn: conn}
		dbch := newClickHouseDatabase(cfg, guardedConn)

		sum, err := dbch.DirInfo(mountPath, nil)
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)
		So(sum, ShouldNotBeNil)
		So(guardedConn.factQueriesAfterReadiness(), ShouldEqual, 0)
		So(guardedConn.snapshotQueriesBeforeReadiness(), ShouldEqual, 0)
		So(guardedConn.snapshotQueriesAfterReadiness(), ShouldEqual, 0)
	})

	Convey("batched and tree readers do not use active snapshot facts before projection readiness exists", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/batch-not-ready/"

		updatedAt := time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 2)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"child/", 7, 1)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"child"), ShouldBeNil)

		guardedConn := &dirFactsReadinessGuardConn{Conn: conn}
		dbch := newClickHouseDatabase(cfg, guardedConn)

		summaries, err := dbch.DirInfos(
			[]string{mountPath, mountPath + "child/"},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(summaries, ShouldBeEmpty)

		hasChildren, err := dbch.DirsHaveChildren([]string{mountPath, mountPath + "child/"}, nil)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath:            false,
			mountPath + "child/": false,
		})

		_, err = db.NewTree(dbch).Where(
			mountPath,
			&db.Filter{
				GIDs: []uint32{7},
				UIDs: []uint32{9},
				FT:   db.DGUTAFileTypeBam,
				Age:  db.DGUTAgeAll,
			},
			split.SplitsToSplitFn(1),
		)
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)
		So(guardedConn.readinessQueryCount(), ShouldBeGreaterThan, 0)
		So(guardedConn.snapshotQueriesBeforeReadiness(), ShouldEqual, 0)
		So(guardedConn.snapshotQueriesAfterReadiness(), ShouldEqual, 0)
	})

	Convey("ancestor readers do not expose unready active snapshots", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/", "/lustre/partial/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/lustre/partial/"

		updatedAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, "/", 7, 11)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), "/", "/lustre"), ShouldBeNil)

		dbch := newClickHouseDatabase(cfg, conn)
		sum, err := dbch.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)
		So(sum, ShouldNotBeNil)

		children, err := dbch.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldBeNil)

		_, err = db.NewTree(dbch).DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		snapshotDB := newClickHouseDatabaseWithSnapshot(cfg, conn, snapshot)

		sum, err = snapshotDB.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)
		So(sum, ShouldNotBeNil)

		children, err = snapshotDB.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldBeNil)

		_, err = db.NewTree(snapshotDB).DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(errors.Is(err, db.ErrDirNotFound), ShouldBeTrue)
	})

	Convey("DirInfo uses ready scalar facts for AgeAll active-mount filters", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/ready-scalar/"

		updatedAt := time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 3)
		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		sum, err := dbch.DirInfo(mountPath, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 3)
		So(sum.Size, ShouldEqual, 30)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
	})

	Convey("DirInfo returns a summary from wrstat_dir_facts for active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		dbch := newClickHouseDatabase(cfg, cp.conn)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountPath = "/mnt/test/"
			dir       = mountPath
		)

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid,
			dir,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(2),
			uint64(123),
			int64(10),
			int64(20),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		sum, err := dbch.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 2)
		So(sum.Size, ShouldEqual, 123)
		So(sum.Modtime, ShouldResemble, updatedAt)
	})

	Convey("DirInfo uses the active root snapshot below inactive nested mountpoints", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/", "/dev/", "/proc/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		dbch := newClickHouseDatabase(cfg, cp.conn)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountPath = "/"
			dir       = "/dev/"
		)

		updatedAt := time.Date(2026, 1, 12, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid,
			mountPath,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(3),
			uint64(456),
			int64(10),
			int64(20),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid,
			dir,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(2),
			uint64(123),
			int64(10),
			int64(20),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			sid,
			mountPath,
			"/dev",
		), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		children, err := dbch.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{"/dev"})

		sum, err := dbch.DirInfo("/dev", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 2)
		So(sum.Size, ShouldEqual, 123)
		So(sum.Modtime, ShouldResemble, updatedAt)

		di, err := db.NewTree(dbch).DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 3)
		So(di.Children, ShouldHaveLength, 1)
		So(di.Children[0].Dir, ShouldEqual, "/dev")
		So(di.Children[0].Count, ShouldEqual, 2)
	})
}

func insertMountDirProjectionSetForTest(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
) {
	So(writeMountDirSummarySetRow(ctx, conn, mount, time.Now().UTC()), ShouldBeNil)
}

func TestClickHouseDatabaseDirFilterAgeAllRoutingA2(t *testing.T) {
	Convey("A2.1 eligible AgeAll owner filters can route to narrow rows", t, func() {
		filter := &db.Filter{Age: db.DGUTAgeAll, GIDs: []uint32{7}}

		So(dirFilterAgeAllCanHandleFilter(filter), ShouldBeTrue)
	})

	Convey("A2.2 age-specific owner filters stay on facts SQL", t, func() {
		filter := &db.Filter{Age: db.DGUTAgeA2M, GIDs: []uint32{7}}

		So(dirFilterAgeAllCanHandleFilter(filter), ShouldBeFalse)

		query, _ := filteredMountWhereSummariesQueryForFilter("/mnt/a/", "snapshot", filter)
		So(query, ShouldContainSubstring, "FROM wrstat_dir_facts")
		So(query, ShouldNotContainSubstring, dirFilterAgeAllTableName)
	})

	Convey("A2.3 empty AgeAll GID filters return no DirInfo summary without scanning AgeAll rows", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/a2-empty/")
		defer cleanup()

		env.createAndSeedAgeAllIndex()

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)

		sum, err := dbch.DirInfo(env.mount.mountPath, &db.Filter{Age: db.DGUTAgeAll, GIDs: []uint32{}})
		So(err, ShouldBeNil)
		So(sum, ShouldBeNil)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
	})

	Convey("A2.4 ready AgeAll rows match facts-vector DirInfos for owner/type filters", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/a2-equivalent/")
		defer cleanup()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		seedA2EquivalentFacts(ctx, env.conn, env.mount)

		filter := &db.Filter{
			Age:  db.DGUTAgeAll,
			GIDs: []uint32{7},
			UIDs: []uint32{11},
			FT:   db.DGUTAFileTypeBam,
		}
		dirs := []string{env.mount.mountPath + "a/", env.mount.mountPath + "b/", env.mount.mountPath + "c/"}

		expected, err := factsVectorDirInfosForTest(env.cfg, env.providerConn, env.mount, dirs, filter)
		So(err, ShouldBeNil)

		insertDirFilterAgeAllRowsFromFactsForTest(ctx, env.conn, env.mount)

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		actualDB := newClickHouseDatabase(env.cfg, countingConn)
		actual, err := actualDB.DirInfos(dirs, filter)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldBeGreaterThan, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
	})

	Convey("A2.5 t283-shaped subset AgeAll summaries match facts-vector totals", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, testT283ImagingMountPath)
		defer cleanup()
		defer useStrictlyFasterAgeAllWhereRouteForTest()()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		seedA2T283Subset(ctx, env.conn, env.mount)

		filter := a2T283Filter()
		expected, err := factsFilteredMountWhereSummariesForTest(ctx, env.providerConn, env.mount, filter)
		So(err, ShouldBeNil)

		insertDirFilterAgeAllRowsFromFactsForTest(ctx, env.conn, env.mount)

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		actualDB := newClickHouseDatabase(env.cfg, countingConn)
		actual, err := actualDB.filteredMountWhereSummaries(env.mount, env.mount.mountPath, filter)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)

		dirs, files, bytes := dirSummaryMapTotals(actual)
		So(dirs, ShouldEqual, uint64(a2T283MatchingDirs))
		So(files, ShouldEqual, uint64(764218))
		So(bytes, ShouldEqual, uint64(1197943849957))
		So(countingConn.filterAgeAllQueryCountValue(), ShouldBeGreaterThan, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 0)
	})

	Convey("A2.6 t283-shaped AgeAll route prunes granules and records read metrics", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, testT283ImagingMountPath)
		defer cleanup()
		defer useStrictlyFasterAgeAllWhereRouteForTest()()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		seedA2T283Subset(ctx, env.conn, env.mount)
		insertDirFilterAgeAllRowsFromFactsForTest(ctx, env.conn, env.mount)
		optimizeDirFilterAgeAllForTest(ctx, env.conn)

		filter := a2T283Filter()
		explain, err := explainA2AgeAllQuery(ctx, env.conn, env.mount, filter)
		So(err, ShouldBeNil)

		readGranules, totalGranules, ok := explainGranules(explain)
		So(ok, ShouldBeTrue)
		So(readGranules, ShouldBeLessThanOrEqualTo, uint64(5))
		So(totalGranules, ShouldEqual, uint64(13))

		inspector := &Inspector{cfg: env.cfg, conn: env.providerConn}
		metrics, err := inspector.Measure(ctx, func(context.Context) error {
			_, measureErr := newClickHouseDatabase(env.cfg, env.providerConn).
				filteredMountWhereSummaries(env.mount, env.mount.mountPath, filter)

			return measureErr
		})
		So(err, ShouldBeNil)
		So(metrics.ReadRows, ShouldBeGreaterThan, uint64(0))
		So(metrics.ReadBytes, ShouldBeGreaterThan, uint64(0))
	})
}

func seedA2EquivalentFacts(ctx context.Context, conn ch.Conn, mount activeMount) {
	mountPath := mount.mountPath
	sid := mount.snapshotID

	insertA2Fact(ctx, conn, mount, mountPath+"a/", 7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20)
	insertA2Fact(ctx, conn, mount, mountPath+"a/", 7, 12, db.DGUTAFileTypeOther, db.DGUTAgeAll, 1, 5)
	insertA2Fact(ctx, conn, mount, mountPath+"a/", 7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA2M, 4, 40)
	insertA2Fact(ctx, conn, mount, mountPath+"b/", 7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3, 30)
	insertA2Fact(ctx, conn, mount, mountPath+"c/", 8, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 5, 50)

	So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"b"), ShouldBeNil)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
}

func insertA2Fact(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dir string,
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count, size uint64,
) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mount.mountPath,
		mount.snapshotID,
		dir,
		gid,
		uid,
		uint16(ft),
		uint8(age),
		count,
		size,
		int64(10),
		int64(20),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func factsVectorDirInfosForTest(
	cfg Config,
	conn ch.Conn,
	mount activeMount,
	dirs []string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	queryDirs, displayDirs := queryAndDisplayDirsForTest(dirs)

	gutasByDir, err := newClickHouseDatabase(cfg, conn).
		gutasForDirs(mount.mountPath, mount.snapshotID, queryDirs)
	if err != nil {
		return nil, err
	}

	summaries := make(map[string]*db.DirSummary, len(dirs))

	for _, displayDir := range displayDirs {
		sum := dirSummaryWithModtime(gutasByDir[displayDir.query], filter, mount.updatedAt)
		if sum == nil {
			continue
		}

		sum.Dir = displayDir.display
		summaries[displayDir.display] = sum
	}

	return summaries, nil
}

func queryAndDisplayDirsForTest(dirs []string) ([]string, []queryAndDisplayDirForTest) {
	queryDirs := make([]string, 0, len(dirs))
	displayDirs := make([]queryAndDisplayDirForTest, 0, len(dirs))

	for _, dir := range dirs {
		queryDir := ensureTrailingSlash(dir)
		queryDirs = append(queryDirs, queryDir)
		displayDirs = append(displayDirs, queryAndDisplayDirForTest{query: queryDir, display: dir})
	}

	return queryDirs, displayDirs
}

func insertDirFilterAgeAllRowsFromFactsForTest(ctx context.Context, conn ch.Conn, mount activeMount) {
	So(conn.Exec(ctx,
		"INSERT INTO wrstat_dir_filter_ageall "+
			"SELECT mount_path, snapshot_id, tupleElement(g, 1) AS gid, tupleElement(g, 2) AS uid, "+
			"tupleElement(g, 3) AS ft, dir, tupleElement(g, 5) AS count, tupleElement(g, 6) AS size, "+
			"tupleElement(g, 7) AS atime_min, tupleElement(g, 8) AS mtime_max, "+
			"tupleElement(g, 9) AS atime_buckets, tupleElement(g, 10) AS mtime_buckets, now() "+
			"FROM (SELECT mount_path, snapshot_id, dir, arrayJoin("+dgutaArrayZipExpr+") AS g "+
			"FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = toUUID(?)) "+
			"WHERE tupleElement(g, 4) = ?",
		mount.mountPath,
		mount.snapshotID,
		uint8(db.DGUTAgeAll),
	), ShouldBeNil)
}

func useStrictlyFasterAgeAllWhereRouteForTest() func() {
	previous := whereDirAgeAllRouteEvidenceFor
	whereDirAgeAllRouteEvidenceFor = func(
		context.Context,
		*clickHouseDatabase,
		activeMount,
		string,
		*db.Filter,
	) (whereDirAgeAllRouteEvidence, bool, error) {
		return whereDirAgeAllRouteEvidence{
			ageAllExact: true,
			allExact:    true,
			ageAllP95:   90 * time.Millisecond,
			allP95:      100 * time.Millisecond,
		}, true, nil
	}

	return func() {
		whereDirAgeAllRouteEvidenceFor = previous
	}
}

func seedA2T283Subset(ctx context.Context, conn ch.Conn, mount activeMount) {
	countBase := a2T283TotalFiles / a2T283MatchingDirs
	countRemainder := a2T283TotalFiles % a2T283MatchingDirs
	sizeBase := a2T283TotalBytes / a2T283MatchingDirs
	sizeRemainder := a2T283TotalBytes % a2T283MatchingDirs

	countExpr := a2T283ConditionalUInt64(uint64(countBase), uint64(countRemainder))
	sizeExpr := a2T283ConditionalUInt64(uint64(sizeBase), uint64(sizeRemainder))
	query := "INSERT INTO wrstat_dir_facts " +
		"(mount_path, snapshot_id, dir, updated_at, gids, uids, fts, ages, counts, sizes, " +
		"atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) " +
		"SELECT ?, toUUID(?), concat(?, 'dir', leftPad(toString(number), 5, '0'), '/'), ?, " +
		a2T283IDArrayExpr(14976, 14977) + ", " +
		a2T283IDArrayExpr(20155, 20156) + ", " +
		fmt.Sprintf("[toUInt16(%d)], [toUInt8(%d)], ", uint16(db.DGUTAFileTypeOther), uint8(db.DGUTAgeAll)) +
		"[" + countExpr + "], [" + sizeExpr + "], " +
		"[toInt64(10)], [toInt64(20)], " +
		a2T283BucketArrayExpr(0, countExpr) + ", " +
		a2T283BucketArrayExpr(1, countExpr) + ", " +
		fmt.Sprintf("now() FROM numbers(%d)", a2T283TotalRows)

	So(conn.Exec(ctx, query, mount.mountPath, mount.snapshotID, mount.mountPath, mount.updatedAt), ShouldBeNil)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
}

func a2T283ConditionalUInt64(base, remainder uint64) string {
	return fmt.Sprintf(
		"if(number < %d, toUInt64(%d + if(number < %d, 1, 0)), toUInt64(1))",
		a2T283MatchingDirs,
		base,
		remainder,
	)
}

func a2T283IDArrayExpr(matchValue, otherValue uint32) string {
	return fmt.Sprintf(
		"[if(number < %d, toUInt32(%d), toUInt32(%d))]",
		a2T283MatchingDirs,
		matchValue,
		otherValue,
	)
}

func a2T283BucketArrayExpr(populatedIndex int, value string) string {
	buckets := make([]string, len(summary.AgeBuckets{}))
	for i := range buckets {
		buckets[i] = "toUInt64(0)"
	}

	buckets[populatedIndex] = value

	return "[[" + strings.Join(buckets, ", ") + "]]"
}

func a2T283Filter() *db.Filter {
	return &db.Filter{
		Age:  db.DGUTAgeAll,
		GIDs: []uint32{14976},
		UIDs: []uint32{20155},
		FT:   db.DGUTAFileTypeOther,
	}
}

func factsFilteredMountWhereSummariesForTest(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	query, args := filteredMountWhereSummariesQueryForFilter(mount.mountPath, mount.snapshotID, filter)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	summaries, _, err := scanDirSummaryRows(rows, filter, mount.updatedAt)

	return summaries, err
}

func dirSummaryMapTotals(summaries map[string]*db.DirSummary) (uint64, uint64, uint64) {
	var files, bytes uint64
	for _, sum := range summaries {
		files += sum.Count
		bytes += sum.Size
	}

	return uint64(len(summaries)), files, bytes
}

func optimizeDirFilterAgeAllForTest(ctx context.Context, conn ch.Conn) {
	So(conn.Exec(ctx, "OPTIM"+"IZE TABLE wrstat_dir_filter_ageall FINAL"), ShouldBeNil)
}

func explainA2AgeAllQuery(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	filter *db.Filter,
) (string, error) {
	filterExpr, filterArgs := dirFilterAgeAllFilterExpression(filter)
	query := "SELECT dir FROM wrstat_dir_filter_ageall " +
		"PREWHERE mount_path = ? AND snapshot_id = ? WHERE " + filterExpr
	args := make([]any, 0, queryScopeArgs+len(filterArgs))
	args = append(args, mount.mountPath, mount.snapshotID)
	args = append(args, filterArgs...)

	rows, err := conn.Query(ctx, explainPrefix+query, args...)
	if err != nil {
		return "", err
	}

	defer func() { _ = rows.Close() }()

	var lines []string

	for rows.Next() {
		var line string
		So(rows.Scan(&line), ShouldBeNil)
		lines = append(lines, line)
	}

	So(rows.Err(), ShouldBeNil)

	return strings.Join(lines, "\n"), nil
}

func explainGranules(explain string) (uint64, uint64, bool) {
	matches := regexp.MustCompile(`Granules:\s+(\d+)/(\d+)`).FindAllStringSubmatch(explain, -1)
	if len(matches) == 0 {
		return 0, 0, false
	}

	last := matches[len(matches)-1]
	read, readErr := strconv.ParseUint(last[1], 10, 64)
	total, totalErr := strconv.ParseUint(last[2], 10, 64)

	return read, total, readErr == nil && totalErr == nil
}

func TestClickHouseDatabaseActivePrefixRoutingB2(t *testing.T) {
	Convey("B2.1-B2.2 DirInfo routes root and namespace summaries through active-prefix rollups", t, func() {
		env, ctx, cleanup := newB2ActivePrefixEnv(t)
		defer cleanup()

		So(ensureActivePrefixRollups(ctx, env.conn, env.rows), ShouldBeNil)

		countingConn := &activePrefixRouteCountingConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		tree := db.NewTree(dbch)

		rootExpected := readActivePrefixSummaryForB2(ctx, env.conn, env.activeSetID, "/")
		lustreExpected := readActivePrefixSummaryForB2(ctx, env.conn, env.activeSetID, "/lustre/")
		nfsExpected := readActivePrefixSummaryForB2(ctx, env.conn, env.activeSetID, nfsAncestor)

		di, err := tree.DirInfo("/", nil)
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		assertB2SummaryMatches(di.Current, rootExpected)
		So(di.Children, ShouldHaveLength, 2)
		So(di.Children[0].Dir, ShouldEqual, c3LustreChild)
		assertB2SummaryMatches(di.Children[0], lustreExpected)
		So(di.Children[1].Dir, ShouldEqual, c3NFSChild)
		assertB2SummaryMatches(di.Children[1], nfsExpected)

		lustre, err := dbch.DirInfo("/lustre/", nil)
		So(err, ShouldBeNil)
		assertB2SummaryMatches(lustre, lustreExpected)
		So(lustre.Count, ShouldEqual, env.lustreCount)
		So(lustre.Count, ShouldNotEqual, env.nfsCount)

		nfs, err := dbch.DirInfo(nfsAncestor, nil)
		So(err, ShouldBeNil)
		assertB2SummaryMatches(nfs, nfsExpected)
		So(nfs.Count, ShouldEqual, env.nfsCount)
		So(nfs.Count, ShouldNotEqual, env.lustreCount)

		So(countingConn.activePrefixScalarQueryCount(), ShouldEqual, 3)
		So(countingConn.activeMountRootFactQueryCount(), ShouldEqual, 0)
	})

	Convey("B2.3 filtered namespace DirInfo uses active-prefix AgeAll rows", t, func() {
		env, ctx, cleanup := newB2ActivePrefixEnv(t)
		defer cleanup()

		So(ensureActivePrefixRollups(ctx, env.conn, env.rows), ShouldBeNil)

		filter := &db.Filter{
			Age:  db.DGUTAgeAll,
			GIDs: []uint32{14976},
			UIDs: []uint32{20155},
			FT:   db.DGUTAFileTypeOther,
		}
		countingConn := &activePrefixRouteCountingConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)

		sum, err := dbch.DirInfo("/nfs/", filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, env.nfsOtherCount)
		So(sum.Size, ShouldEqual, env.nfsOtherCount*10)
		So(sum.GIDs, ShouldResemble, []uint32{14976})
		So(sum.UIDs, ShouldResemble, []uint32{20155})
		So(sum.FT, ShouldEqual, db.DGUTAFileTypeOther)

		So(countingConn.activePrefixAgeAllQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.dirFilterAgeAllQueryCount(), ShouldEqual, 0)
		So(countingConn.activeMountRootFactQueryCount(), ShouldEqual, 0)
	})

	Convey("B2.4 missing active-prefix tables fall back to facts and record the fallback route", t, func() {
		env, ctx, cleanup := newB2ActivePrefixEnv(t)
		defer cleanup()

		resetActivePrefixRollupMissesForTest()
		dropB2ActivePrefixTables(ctx, env.conn)

		countingConn := &activePrefixRouteCountingConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)

		sum, err := dbch.DirInfo("/", nil)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, env.rootCount)
		So(sum.Size, ShouldEqual, env.rootCount*10)
		So(activePrefixRollupMisses(), ShouldEqual, uint64(1))
		So(countingConn.activeMountRootFactQueryCount(), ShouldEqual, 0)
		So(countingConn.ancestorFactQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("B2.5 active mount-root fact SQL binds full root tuples", t, func() {
		mounts := []activeMount{
			{mountPath: c3Scratch120Mount, snapshotID: "00000000-0000-0000-0000-000000000120"},
			{mountPath: testT283ImagingMountPath, snapshotID: "00000000-0000-0000-0000-000000000283"},
		}

		query, args := activeMountRootDirTuplesQuery(mounts)
		normalised := strings.Join(strings.Fields(query), " ")
		So(normalised, ShouldContainSubstring, "(d.mount_path, d.snapshot_id, d.dir) IN")
		So(normalised, ShouldNotContainSubstring, "d.dir = d.mount_path")
		So(normalised, ShouldNotContainSubstring, "(d.mount_path, d.snapshot_id) IN")
		So(args, ShouldResemble, []any{
			c3Scratch120Mount,
			"00000000-0000-0000-0000-000000000120",
			c3Scratch120Mount,
			testT283ImagingMountPath,
			"00000000-0000-0000-0000-000000000283",
			testT283ImagingMountPath,
		})
	})

	Convey("B2.6 tuned active-root tuple SQL prunes to four granules and stays below latency gates", t, func() {
		env, ctx, cleanup := newB2ActivePrefixEnv(t)
		defer cleanup()

		seedB2ActiveRootTupleRows(ctx, env.conn, env.mounts)
		So(env.conn.Exec(ctx, "OPTIM"+"IZE TABLE wrstat_dir_facts FINAL"), ShouldBeNil)

		explain, err := explainB2ActiveRootTupleQuery(ctx, env.conn, env.mounts)
		So(err, ShouldBeNil)

		readGranules, totalGranules, ok := explainGranules(explain)
		So(ok, ShouldBeTrue)
		So(readGranules, ShouldBeLessThanOrEqualTo, uint64(4))
		So(totalGranules, ShouldEqual, uint64(20))

		durations := measureB2ActiveRootTupleDurations(ctx, env.cfg, env.conn, env.mounts)
		So(b2Percentile(durations, 0.50), ShouldBeLessThanOrEqualTo, float64(b2ActiveRootTupleP50MaxMS))
		So(b2Percentile(durations, 0.95), ShouldBeLessThanOrEqualTo, float64(b2ActiveRootTupleP95MaxMS))
	})
}

func newB2ActivePrefixEnv(t *testing.T) (*b2ActivePrefixEnv, context.Context, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{"/"}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	conn := th.openConn(cfg.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	env := &b2ActivePrefixEnv{
		cfg:      cfg,
		conn:     conn,
		provider: p,
	}
	env.seed(ctx)

	return env, ctx, func() {
		cancel()
		So(conn.Close(), ShouldBeNil)
		So(p.Close(), ShouldBeNil)
		resetSharedTreeQueryCachesForTesting()
		os.Unsetenv("WRSTAT_ENV")
	}
}

func readActivePrefixSummaryForB2(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	dir string,
) *db.DirSummary {
	sum, found, err := readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)
	So(err, ShouldBeNil)
	So(found, ShouldBeTrue)

	return sum
}

func assertB2SummaryMatches(actual, expected *db.DirSummary) {
	So(actual, ShouldNotBeNil)
	So(actual.Count, ShouldEqual, expected.Count)
	So(actual.Size, ShouldEqual, expected.Size)
	So(actual.CommonATime, ShouldEqual, expected.CommonATime)
	So(actual.CommonMTime, ShouldEqual, expected.CommonMTime)
	So(actual.UIDs, ShouldResemble, expected.UIDs)
	So(actual.GIDs, ShouldResemble, expected.GIDs)
	So(actual.FT, ShouldEqual, expected.FT)
	So(actual.Modtime, ShouldResemble, expected.Modtime)
}

func dropB2ActivePrefixTables(ctx context.Context, conn ch.Conn) {
	for _, table := range []string{
		"wrstat_active_prefix_rollup_sets",
		"wrstat_active_prefix_filter_ageall",
		"wrstat_active_prefix_rollups",
	} {
		So(conn.Exec(ctx, "DROP TABLE "+table), ShouldBeNil)
	}
}

func seedB2ActiveRootTupleRows(ctx context.Context, conn ch.Conn, mounts []activeMount) {
	for _, mount := range mounts {
		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dir_facts "+
				"(mount_path, snapshot_id, dir, updated_at, gids, uids, fts, ages, counts, sizes, "+
				"atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) "+
				"SELECT ?, toUUID(?), if(number = 0, ?, concat(?, 'tuple-dir-', leftPad(toString(number), 5, '0'), '/')), ?, "+
				"[toUInt32(14976)], [toUInt32(20155)], [toUInt16(?)], [toUInt8(?)], [toUInt64(1)], [toUInt64(10)], "+
				"[toInt64(10)], [toInt64(20)], [[toUInt64(1),0,0,0,0,0,0,0,0]], [[0,toUInt64(1),0,0,0,0,0,0,0]], now() "+
				"FROM numbers(?)",
			mount.mountPath,
			mount.snapshotID,
			mount.mountPath,
			mount.mountPath,
			mount.updatedAt,
			uint16(db.DGUTAFileTypeOther),
			uint8(db.DGUTAgeAll),
			b2ActiveRootTupleRowsPerMount-b2ActiveRootTuplePreseededRowsPerMount,
		), ShouldBeNil)
	}
}

func measureB2ActiveRootTupleDurations(
	ctx context.Context,
	cfg Config,
	conn ch.Conn,
	mounts []activeMount,
) []float64 {
	inspector := &Inspector{cfg: cfg, conn: conn}
	durations := make([]float64, 0, b2ActiveRootTupleRepeat)

	for range b2ActiveRootTupleRepeat {
		metrics, err := inspector.Measure(ctx, func(context.Context) error {
			gutas, queryErr := newClickHouseDatabase(cfg, conn).queryGUTAsForActiveMountRootDirs(mounts)
			So(gutas, ShouldHaveLength, len(mounts))

			return queryErr
		})
		So(err, ShouldBeNil)

		durations = append(durations, float64(metrics.DurationMs))
	}

	return durations
}

func b2Percentile(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	index := int(float64(len(sorted))*p + 0.999999)
	if index < 1 {
		index = 1
	}

	if index > len(sorted) {
		index = len(sorted)
	}

	return sorted[index-1]
}

func explainB2ActiveRootTupleQuery(
	ctx context.Context,
	conn ch.Conn,
	mounts []activeMount,
) (string, error) {
	query, args := activeMountRootDirTuplesQuery(mounts)

	rows, err := conn.Query(ctx, explainPrefix+query, args...)
	if err != nil {
		return "", err
	}

	defer func() { _ = rows.Close() }()

	var lines []string

	for rows.Next() {
		var line string
		So(rows.Scan(&line), ShouldBeNil)
		lines = append(lines, line)
	}

	So(rows.Err(), ShouldBeNil)

	return strings.Join(lines, "\n"), nil
}

type c2DigestSummary struct {
	Count uint64   `json:"count"`
	Size  uint64   `json:"size"`
	UIDs  []uint32 `json:"uids"`
	GIDs  []uint32 `json:"gids"`
	FT    uint16   `json:"ft"`
	Age   uint8    `json:"age"`
}

type c2WhereDigestSummary struct {
	Dir   string   `json:"dir"`
	Count uint64   `json:"count"`
	Size  uint64   `json:"size"`
	UIDs  []uint32 `json:"uids"`
	GIDs  []uint32 `json:"gids"`
	FT    uint16   `json:"ft"`
	Age   uint8    `json:"age"`
}

type c2RESTDigestSummary struct {
	Dir       string       `json:"dir"`
	Count     uint64       `json:"count"`
	Size      uint64       `json:"size"`
	Users     []string     `json:"users"`
	Groups    []string     `json:"groups"`
	FileTypes []string     `json:"filetypes"`
	Age       db.DirGUTAge `json:"age"`
}

type c2ActiveVirtualRouteCountingConn struct {
	ch.Conn

	activeVirtualReads       atomic.Int32
	activeVirtualFilterReads atomic.Int32
	dirFactReads             atomic.Int32
	factVectorReads          atomic.Int32
}

func (c *c2ActiveVirtualRouteCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM wrstat_active_virtual_") {
		c.activeVirtualReads.Add(1)
	}

	if strings.Contains(query, "FROM wrstat_active_virtual_filter_all") {
		c.activeVirtualFilterReads.Add(1)
	}

	if strings.Contains(query, "FROM wrstat_dir_facts") {
		c.dirFactReads.Add(1)
	}

	if strings.Contains(query, "FROM wrstat_dir_facts") && strings.Contains(query, "arrayJoin") {
		c.factVectorReads.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *c2ActiveVirtualRouteCountingConn) activeVirtualReadsValue() int {
	return int(c.activeVirtualReads.Load())
}

func (c *c2ActiveVirtualRouteCountingConn) activeVirtualFilterReadsValue() int {
	return int(c.activeVirtualFilterReads.Load())
}

func (c *c2ActiveVirtualRouteCountingConn) dirFactReadsValue() int {
	return int(c.dirFactReads.Load())
}

func (c *c2ActiveVirtualRouteCountingConn) factVectorReadsValue() int {
	return int(c.factVectorReads.Load())
}

func (c *c2ActiveVirtualRouteCountingConn) reset() {
	c.activeVirtualReads.Store(0)
	c.activeVirtualFilterReads.Store(0)
	c.dirFactReads.Store(0)
	c.factVectorReads.Store(0)
}

type c2ActiveVirtualRouteServer struct {
	server   *server.Server
	provider *chProvider
	counting *c2ActiveVirtualRouteCountingConn
	addr     string
	cert     string
	key      string
	token    string
	stop     func() error
}

type c2ActiveVirtualEnv struct {
	cfg          Config
	conn         ch.Conn
	provider     *chProvider
	providerConn ch.Conn
	snapshot     *activeMountsSnapshot
	activeSetID  string
}

type c2MountSeed struct {
	mountPath string
	updatedAt time.Time
	count     uint64
	size      uint64
}

type c2ActiveVirtualSummaryForTest struct {
	count          uint64
	size           uint64
	childCount     uint64
	isMountRootBox uint8
}

type b3ReadVolume struct {
	rows  uint64
	bytes uint64
	marks uint64
}

func (v b3ReadVolume) add(other b3ReadVolume) b3ReadVolume {
	return b3ReadVolume{
		rows:  v.rows + other.rows,
		bytes: v.bytes + other.bytes,
		marks: v.marks + other.marks,
	}
}

type a4TreeRecord struct {
	dir      string
	count    uint64
	children []string
}

type schema3A2ParentPacketEnv struct {
	cfg      Config
	conn     ch.Conn
	snapshot *activeMountsSnapshot
}

func newSchema3A3ParentPacketEnv(t *testing.T, seedFilteredPacket bool) (schema3A2ParentPacketEnv, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()
	resetParentFactsFallbackRoutesForTest()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{schema3A2MountPath}

	updatedAt := time.Date(2026, 6, 8, 13, 0, 0, 0, time.UTC)
	sid := seedSchema3A3ParentFactsDisktree(t, cfg, schema3A2MountPath, updatedAt, schema3A2ParentDir)

	conn := th.openConn(cfg.DSN)
	if seedFilteredPacket {
		seedSchema3A3ChildFilterAll(t, conn, schema3A2MountPath, sid.String())
	} else {
		seedSchema3A3Readiness(t, conn, schema3A2MountPath, sid.String(), 0)
	}

	cleanup := func() {
		So(conn.Close(), ShouldBeNil)
		resetSharedTreeQueryCachesForTesting()
		resetParentFactsFallbackRoutesForTest()
		os.Unsetenv("WRSTAT_ENV")
	}

	snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
		mountPath:  schema3A2MountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}})

	return schema3A2ParentPacketEnv{cfg: cfg, conn: conn, snapshot: snapshot}, cleanup
}

func TestClickHouseDatabasePacketChildPresenceA3(t *testing.T) {
	Convey("A3.1 broad DirsHaveChildren uses packet has_children values", t, func() {
		env, cleanup := newSchema3A3ParentPacketEnv(t, false)
		defer cleanup()

		countingConn := &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		got, err := dbch.DirsHaveChildren(schema3A2ChildPaths(), &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(got, ShouldResemble, schema3A3AlternatingChildPresence())
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 1)
		So(countingConn.childrenQueries(), ShouldEqual, 0)
		So(countingConn.dirFactsINQueries(), ShouldEqual, 0)
	})

	Convey("A3.2 full-filter DirsHaveChildren uses child_filter_all has_filter_children values", t, func() {
		env, cleanup := newSchema3A3ParentPacketEnv(t, true)
		defer cleanup()

		countingConn := &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		got, err := dbch.DirsHaveChildren(schema3A2ChildPaths(), schema3A3FullFilter())
		So(err, ShouldBeNil)
		So(got, ShouldResemble, schema3A3FilteredChildPresence())
		So(countingConn.childFilterAllPacketQueries(), ShouldEqual, 1)
		So(countingConn.parentFactRangeQueries(), ShouldEqual, 0)
		So(countingConn.childrenQueries(), ShouldEqual, 0)
		So(parentFactsFallbackRoutes(), ShouldEqual, uint64(0))
	})

	Convey("A3.3 missing filtered packets record a named fallback and preserve results", t, func() {
		env, cleanup := newSchema3A3ParentPacketEnv(t, false)
		defer cleanup()

		countingConn := &parentFactsDisktreeRouteConn{Conn: env.conn}
		dbch := newClickHouseDatabaseWithSnapshot(env.cfg, countingConn, env.snapshot)

		got, err := dbch.DirsHaveChildren(schema3A2ChildPaths(), schema3A3FullFilter())
		So(err, ShouldBeNil)
		So(got[schema3A2ChildPath(0)], ShouldBeTrue)
		So(got[schema3A2ChildPath(1)], ShouldBeFalse)
		So(countingConn.childFilterAllPacketQueries(), ShouldEqual, 0)
		So(countingConn.childrenQueries(), ShouldBeGreaterThan, 0)
		So(parentFactsFallbackRouteName(), ShouldEqual, "parent_facts_fallback")
		So(parentFactsFallbackRoutes(), ShouldEqual, uint64(1))
	})
}

func schema3A3AlternatingChildPresence() map[string]bool {
	presence := make(map[string]bool, schema3A2ChildCount)
	for i := range schema3A2ChildCount {
		presence[schema3A2ChildPath(i)] = i%2 == 0
	}

	return presence
}

func schema3A3FullFilter() *db.Filter {
	return &db.Filter{
		GIDs: []uint32{7},
		UIDs: []uint32{11},
		FT:   db.DGUTAFileTypeBam,
		Age:  db.DGUTAgeA1M,
	}
}

func schema3A3FilteredChildPresence() map[string]bool {
	presence := make(map[string]bool, schema3A2ChildCount)
	for i := range schema3A2ChildCount {
		presence[schema3A2ChildPath(i)] = schema3A3FilteredHasChildren(i) > 0
	}

	return presence
}

func schema3A3FilteredHasChildren(index int) uint8 {
	switch index {
	case 0, 10, 20:
		return 1
	default:
		return 0
	}
}

func TestE2ClickHouseT283FilteredRESTAnomaly(t *testing.T) {
	Convey("E2.1/E2.2 t283 type-only Where count and digest are independent of root warming order", t, func() {
		env, cleanup := newE2T283OrderEnv(t)
		defer cleanup()

		filter := &db.Filter{FT: db.DGUTAFileTypeOther}
		splitFn := split.SplitsToSplitFn(2)

		directProvider, err := OpenProvider(env.cfg)
		So(err, ShouldBeNil)
		direct, err := directProvider.Tree().Where(testT283ImagingMountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(directProvider.Close(), ShouldBeNil)

		resetSharedTreeQueryCachesForTesting()

		warmedProvider, err := OpenProvider(env.cfg)
		So(err, ShouldBeNil)
		_, err = warmedProvider.Tree().Where("/", filter, splitFn)
		So(err, ShouldBeNil)
		warmed, err := warmedProvider.Tree().Where(testT283ImagingMountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(warmedProvider.Close(), ShouldBeNil)

		So(e2DCSsDigest(direct), ShouldEqual, e2DCSsDigest(warmed))
		So(len(direct), ShouldEqual, len(warmed))
		So(len(direct), ShouldEqual, 3)
		So(len(direct), ShouldNotEqual, 2)
	})

	Convey("E2.3 active-prefix cache hit keys include path, filter, active set id, and query version", t, func() {
		cache := newTreeQueryCache()
		filter := &db.Filter{
			FT:  db.DGUTAFileTypeOther,
			Age: db.DGUTAgeAll,
		}
		key := newTreeActivePrefixSummaryCacheKey(
			"e2-active-set",
			testT283ImagingMountPath,
			filter,
			treePermissionCacheInputs{},
			currentSchemaVersion,
			activePrefixDirSummaryQueryVersion,
		)

		cache.putActivePrefixDirSummary(key, &db.DirSummary{Dir: testT283ImagingMountPath, Count: 87})
		_, ok := cache.getActivePrefixDirSummary(key)
		So(ok, ShouldBeTrue)

		stats := cache.stats()
		So(stats.activePrefixSummaryHitKeys, ShouldHaveLength, 1)

		hitKey := stats.activePrefixSummaryHitKeys[0]
		So(hitKey, ShouldContainSubstring, "path=/nfs/t283_imaging/")
		So(hitKey, ShouldContainSubstring, "filter=")
		So(hitKey, ShouldContainSubstring, "active_set_id=e2-active-set")
		So(hitKey, ShouldContainSubstring, "query_version=1")
	})
}

func newE2T283OrderEnv(t *testing.T) (*e2T283OrderEnv, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{"/"}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)
	So(p.Close(), ShouldBeNil)

	conn := th.openConn(cfg.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	updatedAt := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	sid := snapshotID(testT283ImagingMountPath, updatedAt)
	mount := activeMount{
		mountPath:  testT283ImagingMountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}
	So(conn.Exec(ctx, testInsertMountStmt, mount.mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

	insertE2T283Summary(ctx, conn, mount, "/", 87)
	insertE2T283Summary(ctx, conn, mount, nfsAncestor, 87)
	insertE2T283Summary(ctx, conn, mount, testT283ImagingMountPath, 87)
	insertE2T283Summary(ctx, conn, mount, testT283ImagingMountPath+"plateA/", 50)
	insertE2T283Summary(ctx, conn, mount, testT283ImagingMountPath+"plateB/", 37)
	So(conn.Exec(ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, "/", "/nfs"), ShouldBeNil)
	So(conn.Exec(
		ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, nfsAncestor, "/nfs/t283_imaging",
	), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID,
		testT283ImagingMountPath, testT283ImagingMountPath+"plateA"), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID,
		testT283ImagingMountPath, testT283ImagingMountPath+"plateB"), ShouldBeNil)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
	insertDirFilterAgeAllRowsFromFactsForTest(ctx, conn, mount)
	So(ensureActivePrefixRollups(ctx, conn, []mountsActiveRow{mountsActiveRow(mount)}), ShouldBeNil)

	return &e2T283OrderEnv{cfg: cfg, conn: conn}, func() {
		cancel()
		So(conn.Close(), ShouldBeNil)
		resetSharedTreeQueryCachesForTesting()
		os.Unsetenv("WRSTAT_ENV")
	}
}

func insertE2T283Summary(ctx context.Context, conn ch.Conn, mount activeMount, dir string, count uint64) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mount.mountPath,
		mount.snapshotID,
		dir,
		uint32(14976),
		uint32(20155),
		uint16(db.DGUTAFileTypeOther),
		uint8(db.DGUTAgeAll),
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func e2DCSsDigest(dcss db.DCSs) string {
	data, err := json.Marshal(e2DigestSummaries(dcss))
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func e2DigestSummaries(dcss db.DCSs) []e2DigestSummary {
	summaries := make([]e2DigestSummary, 0, len(dcss))
	for _, summary := range dcss {
		summaries = append(summaries, e2DigestSummary{
			Dir:   summary.Dir,
			Count: summary.Count,
			Size:  summary.Size,
			FT:    uint16(summary.FT),
			Age:   uint8(summary.Age),
		})
	}

	return summaries
}

type b2MountSeed struct {
	mountPath  string
	updatedAt  time.Time
	bamCount   uint64
	otherCount uint64
	dirCount   uint64
	childCount uint64
}

type b2ActivePrefixEnv struct {
	cfg           Config
	conn          ch.Conn
	provider      interface{ Close() error }
	rows          []mountsActiveRow
	mounts        []activeMount
	activeSetID   string
	rootCount     uint64
	lustreCount   uint64
	nfsCount      uint64
	nfsOtherCount uint64
}

func (e *b2ActivePrefixEnv) seed(ctx context.Context) {
	seeds := []b2MountSeed{
		{
			mountPath:  c3Scratch120Mount,
			updatedAt:  time.Date(2026, 6, 7, 9, 0, 0, 0, time.UTC),
			bamCount:   90,
			otherCount: 7,
			dirCount:   3,
			childCount: 2,
		},
		{
			mountPath:  "/lustre/scratch122/",
			updatedAt:  time.Date(2026, 6, 7, 9, 1, 0, 0, time.UTC),
			bamCount:   80,
			otherCount: 5,
			dirCount:   4,
			childCount: 2,
		},
		{
			mountPath:  "/lustre/scratch127/",
			updatedAt:  time.Date(2026, 6, 7, 9, 2, 0, 0, time.UTC),
			bamCount:   70,
			otherCount: 9,
			dirCount:   5,
			childCount: 2,
		},
		{
			mountPath:  testT283ImagingMountPath,
			updatedAt:  time.Date(2026, 6, 7, 9, 3, 0, 0, time.UTC),
			bamCount:   60,
			otherCount: 11,
			dirCount:   6,
			childCount: 3,
		},
	}

	e.rows = make([]mountsActiveRow, 0, len(seeds))
	e.mounts = make([]activeMount, 0, len(seeds))

	for _, seed := range seeds {
		mount := seedB2Mount(ctx, e.conn, seed)
		e.mounts = append(e.mounts, mount)
		e.rows = append(e.rows, mountsActiveRow(mount))
		e.addSeedCounts(seed)
	}

	e.activeSetID = fingerprintForMountsActive(e.rows)
}

func seedB2Mount(ctx context.Context, conn ch.Conn, seed b2MountSeed) activeMount {
	sid := snapshotID(seed.mountPath, seed.updatedAt)
	mount := activeMount{
		mountPath:  seed.mountPath,
		snapshotID: sid.String(),
		updatedAt:  seed.updatedAt,
	}

	So(conn.Exec(ctx, testInsertMountStmt, seed.mountPath, time.Now().UTC(), sid, seed.updatedAt), ShouldBeNil)

	for _, dir := range []string{"/", activePrefixB1Namespace(seed.mountPath), seed.mountPath} {
		insertB2Fact(ctx, conn, mount, dir, db.DGUTAFileTypeBam, seed.bamCount)
		insertB2Fact(ctx, conn, mount, dir, db.DGUTAFileTypeOther, seed.otherCount)
		insertB2Fact(ctx, conn, mount, dir, db.DGUTAFileTypeDir, seed.dirCount)
	}

	insertActivePrefixB1Children(ctx, conn, mount, seed.childCount)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
	insertDirFilterAgeAllRowsFromFactsForTest(ctx, conn, mount)

	return mount
}

func (e *b2ActivePrefixEnv) addSeedCounts(seed b2MountSeed) {
	count := seed.bamCount + seed.otherCount + seed.dirCount
	e.rootCount += count

	if strings.HasPrefix(seed.mountPath, "/nfs/") {
		e.nfsCount += count
		e.nfsOtherCount += seed.otherCount

		return
	}

	e.lustreCount += count
}

type activePrefixRouteCountingConn struct {
	ch.Conn

	activeMountRootFactQueries atomic.Int32
	activePrefixAgeAllQueries  atomic.Int32
	activePrefixScalarQueries  atomic.Int32
	ancestorFactQueries        atomic.Int32
	dirFilterAgeAllQueries     atomic.Int32
}

func (c *activePrefixRouteCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if isActivePrefixScalarReadQuery(query) {
		c.activePrefixScalarQueries.Add(1)
	}

	if isActivePrefixAgeAllReadQuery(query) {
		c.activePrefixAgeAllQueries.Add(1)
	}

	if isActiveMountRootFactReadQuery(query) {
		c.activeMountRootFactQueries.Add(1)
	}

	if isAncestorFactVectorQuery(query) {
		c.ancestorFactQueries.Add(1)
	}

	if isDirFilterAgeAllReadQuery(query) {
		c.dirFilterAgeAllQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isActivePrefixScalarReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")

	return strings.Contains(normalised, "FROM wrstat_active_prefix_rollups")
}

func isActivePrefixAgeAllReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")

	return strings.Contains(normalised, "FROM wrstat_active_prefix_filter_ageall")
}

func isActiveMountRootFactReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")

	return strings.Contains(normalised, "FROM wrstat_dir_facts d") &&
		strings.Contains(normalised, "(d.mount_path, d.snapshot_id, d.dir) IN")
}

func (c *activePrefixRouteCountingConn) activeMountRootFactQueryCount() int {
	return int(c.activeMountRootFactQueries.Load())
}

func (c *activePrefixRouteCountingConn) activePrefixAgeAllQueryCount() int {
	return int(c.activePrefixAgeAllQueries.Load())
}

func (c *activePrefixRouteCountingConn) activePrefixScalarQueryCount() int {
	return int(c.activePrefixScalarQueries.Load())
}

func (c *activePrefixRouteCountingConn) ancestorFactQueryCount() int {
	return int(c.ancestorFactQueries.Load())
}

func (c *activePrefixRouteCountingConn) dirFilterAgeAllQueryCount() int {
	return int(c.dirFilterAgeAllQueries.Load())
}

type queryAndDisplayDirForTest struct {
	query   string
	display string
}

func seedSchema3A3ParentFactsDisktree(
	t *testing.T,
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	parentDir string,
) uuid.UUID {
	t.Helper()

	sid := snapshotID(mountPath, updatedAt)
	paths := internaltest.NewDirectoryPathCreator()

	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(mountPath)
	w.SetUpdatedAt(updatedAt)

	So(w.Add(db.RecordDGUTA{
		Dir:      paths.ToDirectoryPath(mountPath),
		Children: []string{strings.TrimPrefix(parentDir, mountPath)},
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1, 100, 200),
		},
	}), ShouldBeNil)

	children := make([]string, schema3A2ChildCount)
	for i := range schema3A2ChildCount {
		children[i] = fmt.Sprintf("child%03d/", i)
	}

	So(w.Add(db.RecordDGUTA{
		Dir:        paths.ToDirectoryPath(parentDir),
		ChildCount: uint64(schema3A2ChildCount),
		Children:   children,
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1, 100, 200),
		},
	}), ShouldBeNil)

	for i, child := range children {
		record := db.RecordDGUTA{
			Dir:        paths.ToDirectoryPath(parentDir + child),
			ChildCount: schema3A3AlternatingChildCount(i),
			GUTAs: db.GUTAs{
				b1GUTA(uint32(7+i%3), uint32(11+i%5), db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, uint64(20+i), 100, 200),
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 1, uint64(10+i), 90, 250),
			},
		}
		if i == 0 {
			record.Children = []string{schema3A2LeafChild}
		}

		So(w.Add(record), ShouldBeNil)
	}

	So(w.Add(db.RecordDGUTA{
		Dir: paths.ToDirectoryPath(parentDir + "child000/" + schema3A2LeafChild),
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 1, 10, 90, 250),
		},
	}), ShouldBeNil)

	So(w.Close(), ShouldBeNil)

	return sid
}

func schema3A3AlternatingChildCount(index int) uint64 {
	if index%2 == 0 {
		return 1
	}

	return 0
}

func TestClickHouseDatabaseOptionalDirFilterAgeAll(t *testing.T) {
	Convey("C2.1 mandatory AgeAll index serves whole-mount Where", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-mandatory/")
		defer cleanup()

		assertAgeAllWhereUsesMandatoryIndex(env)
	})

	Convey("C2.2 mandatory AgeAll index serves high-fanout DirsHaveChildren", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-mandatory/")
		defer cleanup()

		env.createAndSeedAgeAllIndex()

		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		hasChildren, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{env.parentDir: true})
		So(countingConn.filterAgeAllQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.mountVectorQueryCount(), ShouldEqual, 0)
	})

	Convey("C2.3 ready mandatory AgeAll index serves whole-mount Where with facts-equivalent summaries", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-ready/")
		defer cleanup()
		defer useStrictlyFasterAgeAllWhereRouteForTest()()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		expected, err := factsVectorDirInfosForTest(
			env.cfg, env.providerConn, env.mount, []string{env.mount.mountPath}, filter,
		)
		So(err, ShouldBeNil)

		env.createAndSeedAgeAllIndex()

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, countingConn))
		actual, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dirSummariesByDir(actual), ShouldResemble, expected)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldBeGreaterThan, 0)
	})

	Convey("C2.4 ready mandatory AgeAll index serves high-fanout DirsHaveChildren", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-ready/")
		defer cleanup()

		env.createAndSeedAgeAllIndex()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		actual, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, map[string]bool{env.parentDir: true})
		So(countingConn.filterAgeAllQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("C2.5 present mandatory AgeAll index with readiness serves Where", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-present/")
		defer cleanup()

		assertAgeAllWhereUsesMandatoryIndex(env)
	})

	Convey("C2.6 present mandatory AgeAll index with readiness serves DirsHaveChildren", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-present/")
		defer cleanup()

		env.createAndSeedAgeAllIndex()

		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		hasChildren, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{env.parentDir: true})
		So(countingConn.filterAgeAllQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("C2.7 age-specific filters never read the mandatory AgeAll index", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-age-specific/")
		defer cleanup()

		env.insertAgeSpecificFacts()
		env.createAndSeedAgeAllIndex()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeM1Y}
		whereConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, whereConn))
		_, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)

		childrenConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, childrenConn)
		_, err = dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(whereConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
		So(childrenConn.filterAgeAllQueryCount(), ShouldEqual, 0)
	})

	Convey("C2.8 schema SQL includes the mandatory AgeAll index", t, func() {
		stmts, err := schemaSQL()
		So(err, ShouldBeNil)

		found := false
		for _, stmt := range stmts {
			found = found || strings.Contains(stmt, "wrstat_dir_filter_ageall")
		}

		So(found, ShouldBeTrue)
	})
}

func newDirFilterAgeAllTestEnv(
	t *testing.T,
	mountPath string,
) (dirFilterAgeAllTestEnv, func()) {
	t.Helper()
	os.Setenv("WRSTAT_ENV", "test")

	resetSharedTreeQueryCachesForTesting()

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 5 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{mountPath}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	cp, ok := p.(*chProvider)
	So(ok, ShouldBeTrue)

	conn := th.openConn(cfg.DSN)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	updatedAt := time.Date(2026, 1, 13, 12, 0, 0, 0, time.UTC)
	sid := snapshotID(mountPath, updatedAt).String()
	mount := activeMount{mountPath: mountPath, snapshotID: sid, updatedAt: updatedAt}

	So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
	seedDirFilterAgeAllFacts(ctx, conn, mount)
	insertMountDirProjectionSetForTest(ctx, conn, mount)

	cleanup := func() {
		So(conn.Close(), ShouldBeNil)
		So(p.Close(), ShouldBeNil)
		resetSharedTreeQueryCachesForTesting()
		os.Unsetenv("WRSTAT_ENV")
	}

	return dirFilterAgeAllTestEnv{
		cfg:          cfg,
		conn:         conn,
		provider:     p,
		providerConn: cp.conn,
		mount:        mount,
		parentDir:    mountPath + "wide/",
	}, cleanup
}

func seedDirFilterAgeAllFacts(ctx context.Context, conn ch.Conn, mount activeMount) {
	mountPath := mount.mountPath
	sid := mount.snapshotID
	insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, db.DGUTAFileTypeBam, 12)
	insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", db.DGUTAFileTypeBam, 10)
	insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", db.DGUTAFileTypeCram, 2)
	So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"b"), ShouldBeNil)

	parent := mountPath + "wide/"
	for i := range dirsHaveChildrenSummaryFanoutLimit + 1 {
		child := fmt.Sprintf("%schild%02d", parent, i)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
	}

	insertDirSummaryTestGUTA(ctx, conn, mountPath, stringerString(sid), parent+"child05/", 7, 3)
	insertDirSummaryTestGUTA(ctx, conn, mountPath, stringerString(sid), parent+"child06/", 8, 2)
}

func assertMaintainedReadinessRetriesMissesAndCachesHits(
	checkReady func(*clickHouseDatabase, context.Context, string, string) (bool, error),
	mountPath string,
) {
	resetSharedTreeQueryCachesForTesting()
	Reset(resetSharedTreeQueryCachesForTesting)

	conn := &mountDirSummaryReadinessConn{}
	dbch := newClickHouseDatabase(Config{QueryTimeout: time.Second}, conn)
	ctx := context.Background()

	ready, err := checkReady(dbch, ctx, mountPath, "snapshot")
	So(err, ShouldBeNil)
	So(ready, ShouldBeFalse)
	So(conn.queryCount(), ShouldEqual, 1)

	conn.setReady(true)

	ready, err = checkReady(dbch, ctx, mountPath, "snapshot")
	So(err, ShouldBeNil)
	So(ready, ShouldBeTrue)
	So(conn.queryCount(), ShouldEqual, 2)

	ready, err = checkReady(dbch, ctx, mountPath, "snapshot")
	So(err, ShouldBeNil)
	So(ready, ShouldBeTrue)
	So(conn.queryCount(), ShouldEqual, 2)
}

type treeRouteQuerySpyConn struct {
	ch.Conn

	queries []string
}

func (c *treeRouteQuerySpyConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)

	return c.Conn.Query(ctx, query, args...)
}

type mountDirSummaryReadinessRows struct {
	ready bool
	seen  bool
}

func (r *mountDirSummaryReadinessRows) Next() bool {
	if !r.ready || r.seen {
		return false
	}

	r.seen = true

	return true
}

func (r *mountDirSummaryReadinessRows) Scan(...any) error {
	return nil
}

func (r *mountDirSummaryReadinessRows) ScanStruct(any) error {
	return nil
}

func (r *mountDirSummaryReadinessRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *mountDirSummaryReadinessRows) Totals(...any) error {
	return nil
}

func (r *mountDirSummaryReadinessRows) Columns() []string {
	return nil
}

func (r *mountDirSummaryReadinessRows) Close() error {
	return nil
}

func (r *mountDirSummaryReadinessRows) Err() error {
	return nil
}

func (r *mountDirSummaryReadinessRows) HasData() bool {
	return r.ready
}

func TestClickHouseDatabaseBatchedTreeExpansion(t *testing.T) {
	Convey("batched summaries and child-existence checks support first-level tree expansion", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		rawDB := newClickHouseDatabase(cfg, cp.conn)
		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/test/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(dir string, gid uint32, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				gid,
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}

		insertGUTA(mountPath+"a/", 7, 3)
		insertGUTA(mountPath+"a/", 8, 4)
		insertGUTA(mountPath+"b/", 7, 2)
		insertGUTA(mountPath+"a/grand/", 7, 1)
		insertGUTA(mountPath+"b/grand/", 8, 1)

		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountPath, sid, mountPath+"a/", mountPath+"a/grand",
		), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountPath, sid, mountPath+"b/", mountPath+"b/grand",
		), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		summaries, err := dbch.DirInfos(
			[]string{mountPath + "a", mountPath + "b"},
			filter,
		)
		So(err, ShouldBeNil)
		So(summaries[mountPath+"a"].Count, ShouldEqual, 3)
		So(summaries[mountPath+"a"].Dir, ShouldEqual, mountPath+"a")
		So(summaries[mountPath+"b"].Count, ShouldEqual, 2)
		So(summaries[mountPath+"b"].Modtime, ShouldResemble, updatedAt)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		expectedSummary := func(dir string, filter *db.Filter) *db.DirSummary {
			gutas, errg := rawDB.gutasForDir(mountPath, sid.String(), ensureTrailingSlash(dir))
			So(errg, ShouldBeNil)

			sum := dirSummaryWithModtime(gutas, filter, updatedAt)
			So(sum, ShouldNotBeNil)

			sum.Dir = dir

			return sum
		}

		broadSummaries, err := dbch.DirInfos(
			[]string{mountPath + "a", mountPath + "b/"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(broadSummaries, ShouldResemble, map[string]*db.DirSummary{
			mountPath + "a":  expectedSummary(mountPath+"a", &db.Filter{Age: db.DGUTAgeAll}),
			mountPath + "b/": expectedSummary(mountPath+"b/", &db.Filter{Age: db.DGUTAgeAll}),
		})
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		defaultSummaries, err := dbch.DirInfos([]string{mountPath + "a"}, &db.Filter{})
		So(err, ShouldBeNil)
		So(defaultSummaries, ShouldResemble, map[string]*db.DirSummary{
			mountPath + "a": expectedSummary(mountPath+"a", &db.Filter{}),
		})
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		emptyGIDSummaries, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{GIDs: []uint32{}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(emptyGIDSummaries, ShouldHaveLength, 0)

		emptyUIDSummaries, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{UIDs: []uint32{}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(emptyUIDSummaries, ShouldHaveLength, 0)

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{mountPath + "a", mountPath + "b"},
			filter,
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath + "a": true,
			mountPath + "b": false,
		})
	})
}

func TestClickHouseDatabaseMountDirSummary(t *testing.T) {
	Convey("DirInfos uses maintained per-mount summaries for broad active-mount filters", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/dirsummary/"

		updatedAt := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 8, 4)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 7, 2)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		summaries, err := dbch.DirInfos(
			[]string{mountPath + "a", mountPath + "b/"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(summaries[mountPath+"a"].Count, ShouldEqual, 7)
		So(summaries[mountPath+"a"].GIDs, ShouldResemble, []uint32{7, 8})
		So(summaries[mountPath+"a"].Modtime, ShouldResemble, updatedAt)
		So(summaries[mountPath+"b/"].Count, ShouldEqual, 2)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		defaultSummaries, err := dbch.DirInfos([]string{mountPath + "a"}, &db.Filter{})
		So(err, ShouldBeNil)
		So(defaultSummaries[mountPath+"a"].Count, ShouldEqual, 7)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		emptyAgeSummary, err := dbch.DirInfo(mountPath+"a/", &db.Filter{Age: db.DGUTAgeA1M})
		So(err, ShouldBeNil)
		So(emptyAgeSummary, ShouldBeNil)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 1)
	})

	Convey("DirInfos uses maintained non-directory summaries for the default where filter", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/nondirsummary/"

		updatedAt := time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, mountPath+"a/", db.DGUTAFileTypeBam, 7, 3)
		insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, mountPath+"a/", db.DGUTAFileTypeDir, 7, 11)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		allSummaries, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(allSummaries[mountPath+"a"].Count, ShouldEqual, 14)
		So(allSummaries[mountPath+"a"].FT, ShouldEqual, db.DGUTAFileTypeBam|db.DGUTAFileTypeDir)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)

		countingConn.reset()

		fileSummaries, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories},
		)
		So(err, ShouldBeNil)
		So(fileSummaries[mountPath+"a"].Count, ShouldEqual, 3)
		So(fileSummaries[mountPath+"a"].FT, ShouldEqual, db.DGUTAFileTypeBam)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		allSummariesAgain, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(allSummariesAgain[mountPath+"a"].Count, ShouldEqual, 14)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("DirInfos and Where omit maintained non-directory summaries with no files", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/dironlysummary/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/dironlysummary/"

		updatedAt := time.Date(2026, 1, 10, 10, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, mountPath, db.DGUTAFileTypeBam, 7, 2)
		insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, mountPath+"dironly/", db.DGUTAFileTypeDir, 7, 11)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"dironly"), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories}

		summaries, err := dbch.DirInfos([]string{mountPath + "dironly"}, filter)
		So(err, ShouldBeNil)
		So(summaries, ShouldNotContainKey, mountPath+"dironly")
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		dcss, err := db.NewTree(dbch).Where(mountPath, filter, split.SplitsToSplitFn(1))
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 1)
		So(dcss[0].Dir, ShouldEqual, mountPath)
		So(dcss[0].Count, ShouldEqual, 2)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("DirInfos uses facts vectors for UID, GID, and specific file-type filters", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/filteredsummary/"

		updatedAt := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 8, 4)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 7, 5)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		summaries, err := dbch.DirInfos(
			[]string{mountPath + "a"},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(summaries[mountPath+"a"].Count, ShouldEqual, 3)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 1)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		ftSummaries, err := dbch.DirInfos(
			[]string{mountPath + "b"},
			&db.Filter{FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(ftSummaries[mountPath+"b"].Count, ShouldEqual, 5)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 1)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("DirInfos uses maintained facts vectors for arbitrary active-mount filters", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/vectorfiltered/"

		updatedAt := time.Date(2026, 1, 10, 10, 15, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		insertVectorGUTA := func(dir string, gid, uid uint32, ft db.DirGUTAFileType, age db.DirGUTAge, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid.String(),
				dir,
				gid,
				uid,
				uint16(ft),
				uint8(age),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}

		insertVectorGUTA(mountPath+"a/", 7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3)
		insertVectorGUTA(mountPath+"a/", 8, 9, db.DGUTAFileTypeCram, db.DGUTAgeAll, 4)
		insertVectorGUTA(mountPath+"a/", 7, 10, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 5)
		insertVectorGUTA(mountPath+"b/", 7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		rawDB := newClickHouseDatabase(cfg, cp.conn)
		expectedSummary := func(dir string, filter *db.Filter) *db.DirSummary {
			gutas, errg := rawDB.gutasForDir(mountPath, sid.String(), ensureTrailingSlash(dir))
			So(errg, ShouldBeNil)

			sum := dirSummaryWithModtime(gutas, filter, updatedAt)
			So(sum, ShouldNotBeNil)

			sum.Dir = dir

			return sum
		}

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeAll,
		}

		summaries, err := dbch.DirInfos([]string{mountPath + "a", mountPath + "b/"}, filter)
		So(err, ShouldBeNil)
		So(summaries, ShouldResemble, map[string]*db.DirSummary{
			mountPath + "a":  expectedSummary(mountPath+"a", filter),
			mountPath + "b/": expectedSummary(mountPath+"b/", filter),
		})
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("DirInfos avoids grouped fact-vector summaries for small batches without maintained rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/rawsmall/"

		updatedAt := time.Date(2026, 1, 10, 11, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 7, 2)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		summaries, err := dbch.DirInfos(
			[]string{mountPath + "a", mountPath + "b"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(summaries[mountPath+"a"].Count, ShouldEqual, 3)
		So(summaries[mountPath+"b"].Count, ShouldEqual, 2)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("maintained summaries are scoped to the active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/scopedsummary/"

		firstUpdatedAt := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
		firstSID := snapshotID(mountPath, firstUpdatedAt)
		secondUpdatedAt := firstUpdatedAt.Add(time.Hour)
		secondSID := snapshotID(mountPath, secondUpdatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, firstUpdatedAt, firstSID, firstUpdatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, firstSID, mountPath+"a/", 7, 1)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: firstSID.String(),
			updatedAt:  firstUpdatedAt,
		}), ShouldBeNil)

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, secondUpdatedAt, secondSID, secondUpdatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, secondSID, mountPath+"a/", 7, 9)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: secondSID.String(),
			updatedAt:  secondUpdatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		sum, err := dbch.DirInfo(mountPath+"a/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, 9)
		So(sum.Modtime, ShouldResemble, secondUpdatedAt)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
	})

	Convey("Tree.DirInfo prefetches child maintained summaries into a bounded cache", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/prefetchsummary/"

		updatedAt := time.Date(2026, 1, 10, 13, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 10)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 6)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{Age: db.DGUTAgeAll}

		di, err := db.NewTree(dbch).DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(di.Children, ShouldHaveLength, 1)
		So(countingConn.mountDirSummaryQueryCount(), ShouldBeGreaterThanOrEqualTo, 2)

		countingConn.reset()

		sum, err := dbch.DirInfo(mountPath+"a/", filter)
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, 6)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)

		cache := newTreeQueryCache()

		for i := range treeDirSummaryCacheMaxEntries + 5 {
			key := newTreeDirSummaryCacheKey(
				"/mnt/cache/",
				fmt.Sprintf("snapshot-%d", i),
				fmt.Sprintf("/mnt/cache/%d/", i),
				db.DGUTAgeAll,
				mountDirSummaryAll,
			)
			cache.putDirSummary(key, &db.DirSummary{Count: uint64(i + 1)})
		}

		So(cache.dirSummaryEntryCount(), ShouldEqual, treeDirSummaryCacheMaxEntries)

		values := make([]string, queryStringINMaxValues+1)
		for i := range values {
			values[i] = fmt.Sprintf("/mnt/chunk/%d/", i)
		}

		batches := stringValueBatches(values)
		So(batches, ShouldHaveLength, 2)
		So(batches[0], ShouldHaveLength, queryStringINMaxValues)
		So(batches[1], ShouldHaveLength, 1)
	})
}

func insertDirSummaryTestGUTA(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid fmt.Stringer,
	dir string,
	gid uint32,
	count uint64,
) {
	insertDirSummaryTestTypedGUTA(ctx, conn, mountPath, sid, dir, db.DGUTAFileTypeBam, gid, count)
}

func insertDirSummaryTestTypedGUTA(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid fmt.Stringer,
	dir string,
	ft db.DirGUTAFileType,
	gid uint32,
	count uint64,
) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mountPath,
		sid.String(),
		dir,
		gid,
		uint32(9),
		uint16(ft),
		uint8(db.DGUTAgeAll),
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func writeMaintainedMountDirProjectionForTest(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
) error {
	state, err := mountDirProjectionStateFromSeededRows(ctx, conn, mount)
	if err != nil {
		return err
	}

	return writeMountDirProjectionRows(ctx, conn, mount, state, defaultBatchSize)
}

func mountDirProjectionStateFromSeededRows(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
) (mountDirProjectionState, error) {
	state := newMountDirProjectionState()

	if err := addSeededDGUTAToMountDirProjectionState(ctx, conn, mount, &state); err != nil {
		return mountDirProjectionState{}, err
	}

	if err := addSeededChildrenToMountDirProjectionState(ctx, conn, mount, &state); err != nil {
		return mountDirProjectionState{}, err
	}

	return state, nil
}

func TestClickHouseDatabaseTreeCache(t *testing.T) {
	Convey("summary-only cache writes clear stale maintained child counts", t, func() {
		cache := newTreeQueryCache()
		key := newTreeDirSummaryCacheKey(
			"/mnt/test/",
			"00000000-0000-0000-0000-000000000001",
			"/mnt/test/a/",
			db.DGUTAgeAll,
			mountDirSummaryAll,
		)

		cache.putDirSummaryWithChildCount(key, &db.DirSummary{Dir: "/mnt/test/a/", Count: 1}, 3)
		childCount, ok := cache.getDirSummaryChildCount(key)
		So(ok, ShouldBeTrue)
		So(childCount, ShouldEqual, 3)

		cache.putDirSummary(key, &db.DirSummary{Dir: "/mnt/test/a/", Count: 2})
		_, ok = cache.getDirSummaryChildCount(key)
		So(ok, ShouldBeFalse)
	})

	Convey("maintained projection readiness caches retry misses and keep hits", t, func() {
		assertMaintainedReadinessRetriesMissesAndCachesHits(
			func(dbch *clickHouseDatabase, ctx context.Context, mountPath, snapshotID string) (bool, error) {
				return dbch.mountDirSummaryReadyCached(ctx, mountPath, snapshotID)
			},
			"/mnt/later/",
		)
	})

	Convey("maintained vector readiness also retries after summary readiness misses", t, func() {
		assertMaintainedReadinessRetriesMissesAndCachesHits(
			func(dbch *clickHouseDatabase, ctx context.Context, mountPath, snapshotID string) (bool, error) {
				return dbch.mountDirDGUTAVectorReadyCached(ctx, mountPath, snapshotID)
			},
			"/mnt/vector-later/",
		)
	})

	Convey("Tree.DirInfo and DiskTree-shaped calls reuse cached rows for the active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(snapshotID, dir string, gid, uid uint32, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				snapshotID,
				dir,
				gid,
				uid,
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(snapshotID, parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, snapshotID, parent, child), ShouldBeNil)
		}

		insertGUTA(sid.String(), mountPath, 7, 9, 10)
		insertGUTA(sid.String(), mountPath+"a/", 7, 9, 6)
		insertGUTA(sid.String(), mountPath+"b/", 7, 9, 4)
		insertGUTA(sid.String(), mountPath+"a/deep/", 7, 9, 2)
		insertGUTA(sid.String(), mountPath+"b/deep/", 8, 9, 1)

		insertChild(sid.String(), mountPath, mountPath+"a")
		insertChild(sid.String(), mountPath, mountPath+"b")
		insertChild(sid.String(), mountPath+"a/", mountPath+"a/deep")
		insertChild(sid.String(), mountPath+"b/", mountPath+"b/deep")

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)
		tree := db.NewTree(dbch)
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeAll,
		}

		di, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 10)
		So(di.Current.Modtime, ShouldResemble, updatedAt)
		So(di.Children, ShouldHaveLength, 2)
		So(di.Children[0].Dir, ShouldEqual, mountPath+"a")
		So(di.Children[0].Count, ShouldEqual, 6)
		So(di.Children[0].Modtime, ShouldResemble, updatedAt)
		So(di.Children[1].Dir, ShouldEqual, mountPath+"b")
		So(di.Children[1].Count, ShouldEqual, 4)

		childPaths := func(info *db.DirInfo) []string {
			paths := make([]string, len(info.Children))
			for i, child := range info.Children {
				paths[i] = child.Dir
			}

			return paths
		}

		hasChildren := tree.DirsHaveChildren(childPaths(di), filter)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath + "a": true,
			mountPath + "b": false,
		})

		warmQueries := countingConn.queryCountValue()
		So(warmQueries, ShouldBeGreaterThan, 0)

		children, err := dbch.Children(mountPath)
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{mountPath + "a", mountPath + "b"})
		children[0] = mountPath + "mutated"

		childrenAgain, err := dbch.Children(mountPath)
		So(err, ShouldBeNil)
		So(childrenAgain, ShouldResemble, []string{mountPath + "a", mountPath + "b"})
		So(countingConn.queryCountValue(), ShouldEqual, warmQueries)

		again, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(again, ShouldResemble, di)
		So(tree.DirsHaveChildren(childPaths(again), filter), ShouldResemble, hasChildren)
		So(countingConn.queryCountValue(), ShouldEqual, warmQueries)
	})

	Convey("Tree.Where reuses cached child rows on repeated traversals", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/where/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 13, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(dir string, gid, uid uint32, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				gid,
				uid,
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertGUTA(mountPath, 7, 9, 10)
		insertGUTA(mountPath+"a/", 7, 9, 6)
		insertGUTA(mountPath+"b/", 7, 9, 4)
		insertGUTA(mountPath+"a/deep/", 7, 9, 2)
		insertGUTA(mountPath+"b/deep/", 8, 9, 1)

		insertChild(mountPath, mountPath+"a")
		insertChild(mountPath, mountPath+"b")
		insertChild(mountPath+"a/", mountPath+"a/deep")
		insertChild(mountPath+"b/", mountPath+"b/deep")

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot))
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			Age:  db.DGUTAgeAll,
		}
		splitFn := split.SplitsToSplitFn(2)

		expected, err := tree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(expected, ShouldHaveLength, 4)

		warmQueries := countingConn.queryCountValue()
		warmChildBatches := countingConn.childBatchQueryCountValue()
		warmFilteredSummaries := countingConn.filteredMountSummaryQueryCountValue()

		So(warmQueries, ShouldBeGreaterThan, 0)
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)

		actual, err := tree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.queryCountValue(), ShouldEqual, warmQueries+1)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, warmFilteredSummaries+1)
		So(countingConn.childBatchQueryCountValue(), ShouldEqual, warmChildBatches)
	})

	Convey("Tree.Where shares cached rows across same-namespace database instances", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/sharedwhere/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 15, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		insertGUTA := func(dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertGUTA(mountPath, 10)
		insertGUTA(mountPath+"a/", 6)
		insertGUTA(mountPath+"b/", 4)
		insertGUTA(mountPath+"a/deep/", 2)
		insertChild(mountPath, mountPath+"a")
		insertChild(mountPath, mountPath+"b")
		insertChild(mountPath+"a/", mountPath+"a/deep")

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)

		firstDB := newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)
		expected, err := db.NewTree(firstDB).Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(expected, ShouldHaveLength, 4)
		So(countingConn.childBatchQueryCountValue(), ShouldBeGreaterThan, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
		So(firstDB.Close(), ShouldBeNil)

		countingConn.resetCounts()
		secondDB := newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)
		actual, err := db.NewTree(secondDB).Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.childBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.queryCountValue(), ShouldEqual, 1)
		So(secondDB.Close(), ShouldBeNil)
	})

	Convey("Tree.Where does not share cached rows across different cache namespaces", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/namespacewhere/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 16, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		insertGUTA := func(dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertGUTA(mountPath, 10)
		insertGUTA(mountPath+"a/", 6)
		insertGUTA(mountPath+"b/", 4)
		insertChild(mountPath, mountPath+"a")
		insertChild(mountPath, mountPath+"b")

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(1)

		expected, err := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)).
			Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(expected, ShouldHaveLength, 3)

		cfgOther := cfg
		cfgOther.DSN += "&cache_namespace_test=other"

		countingConn.resetCounts()

		actual, err := db.NewTree(newClickHouseDatabaseWithSnapshot(cfgOther, countingConn, snapshot)).
			Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.childBatchQueryCountValue(), ShouldBeGreaterThan, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
	})

	Convey("cache keys are scoped by active snapshot id", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/scope/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updated1 := time.Date(2026, 1, 9, 14, 0, 0, 0, time.UTC)
		updated2 := updated1.Add(time.Hour)
		sid1 := snapshotID(mountPath, updated1)
		sid2 := snapshotID(mountPath, updated2)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}
		insertGUTA := func(snapshotID, dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				snapshotID,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(snapshotID, parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, snapshotID, parent, child), ShouldBeNil)
		}

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, updated1, sid1, updated1), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid1.String(),
			updatedAt:  updated1,
		})
		insertGUTA(sid1.String(), mountPath, 1)
		insertGUTA(sid1.String(), mountPath+"a/", 1)
		insertChild(sid1.String(), mountPath, mountPath+"a")
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid1.String(),
			updatedAt:  updated1,
		}), ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		filter := &db.Filter{Age: db.DGUTAgeAll}

		first, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(first.Current.Count, ShouldEqual, 1)
		So(first.Children, ShouldHaveLength, 1)
		So(first.Children[0].Dir, ShouldEqual, mountPath+"a")

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, updated2, sid2, updated2), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid2.String(),
			updatedAt:  updated2,
		})
		insertGUTA(sid2.String(), mountPath, 2)
		insertGUTA(sid2.String(), mountPath+"b/", 2)
		insertChild(sid2.String(), mountPath, mountPath+"b")
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid2.String(),
			updatedAt:  updated2,
		}), ShouldBeNil)

		second, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(second.Current.Count, ShouldEqual, 2)
		So(second.Children, ShouldHaveLength, 1)
		So(second.Children[0].Dir, ShouldEqual, mountPath+"b")
	})
}

func TestClickHouseDatabaseDirsHaveChildrenFastPath(t *testing.T) {
	Convey("Disktree virtual ancestor clicks batch active mount roots", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0

		const (
			nfsMountCount   = 100
			firstNFSProject = "/nfs/project000"
			lastNFSProject  = "/nfs/project099"
		)

		cfg.MountPoints = make([]string, 0, nfsMountCount+1)
		cfg.MountPoints = append(cfg.MountPoints, "/")

		for i := range nfsMountCount {
			cfg.MountPoints = append(cfg.MountPoints, fmt.Sprintf("/nfs/project%03d/", i))
		}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		mountBatch, err := conn.PrepareBatch(ctx, testInsertMountBatchStmt)
		So(err, ShouldBeNil)
		childrenBatch, err := conn.PrepareBatch(ctx, insertChildrenQuery)
		So(err, ShouldBeNil)
		projectionBatch, err := conn.PrepareBatch(ctx, insertMountDirSummaryQuery)
		So(err, ShouldBeNil)
		projectionSetBatch, err := conn.PrepareBatch(ctx, insertMountDirSummarySetQuery)
		So(err, ShouldBeNil)

		baseUpdatedAt := time.Date(2026, 1, 12, 11, 0, 0, 0, time.UTC)
		refreshedAt := time.Now().UTC()
		rows := make([]mountsActiveRow, 0, nfsMountCount)

		var nfsCount uint64

		for i := range nfsMountCount {
			count := uint64(i + 10)
			nfsCount += count

			mountPath := fmt.Sprintf("/nfs/project%03d/", i)
			updatedAt := baseUpdatedAt.Add(time.Duration(i) * time.Minute)
			sid := snapshotID(mountPath, updatedAt)
			mount := activeMount{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			}

			appendTestMountEventRow(mountBatch, mount)
			So(childrenBatch.Append(mountPath, sid.String(), mountPath, mountPath+"leaf"), ShouldBeNil)

			state := newMountDirProjectionState()
			state.addGUTA(mountPath, newTestDGUTA(uint32(7), uint32(9), db.DGUTAFileTypeBam, count))
			state.addGUTA(mountPath+"leaf/", newTestDGUTA(uint32(7), uint32(9), db.DGUTAFileTypeBam, 1))
			state.addChildren(mountPath, 1)
			appendTestMountDirProjectionRows(projectionBatch, projectionSetBatch, mount, state, refreshedAt)

			rows = append(rows, mountsActiveRow{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			})
		}

		So(mountBatch.Send(), ShouldBeNil)
		So(childrenBatch.Send(), ShouldBeNil)
		So(projectionBatch.Send(), ShouldBeNil)
		So(projectionSetBatch.Send(), ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: conn}
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(
			cfg,
			countingConn,
			newActiveMountsSnapshot(rows),
		))
		filter := &db.Filter{Age: db.DGUTAgeAll}

		di, err := tree.DirInfo("/nfs", filter)
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, nfsCount)
		So(di.Children, ShouldHaveLength, nfsMountCount)
		So(di.Children[0].Dir, ShouldEqual, firstNFSProject)
		So(di.Children[nfsMountCount-1].Dir, ShouldEqual, lastNFSProject)

		childPaths := make([]string, len(di.Children))
		for i, child := range di.Children {
			childPaths[i] = child.Dir
		}

		hasChildren := tree.DirsHaveChildren(childPaths, filter)
		So(hasChildren, ShouldHaveLength, nfsMountCount)
		So(hasChildren[firstNFSProject], ShouldBeTrue)
		So(hasChildren[lastNFSProject], ShouldBeTrue)

		clicked, err := tree.DirInfo(firstNFSProject, filter)
		So(err, ShouldBeNil)
		So(clicked, ShouldNotBeNil)
		So(clicked.Current.Count, ShouldEqual, 10)
		So(clicked.Children, ShouldHaveLength, 1)
		So(clicked.Children[0].Dir, ShouldEqual, firstNFSProject+"/leaf")

		So(countingConn.queryCountValue(), ShouldBeLessThanOrEqualTo, 120)

		matchingFilteredChildren := tree.DirsHaveChildren(
			[]string{firstNFSProject, lastNFSProject},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(matchingFilteredChildren, ShouldResemble, map[string]bool{
			firstNFSProject: true,
			lastNFSProject:  true,
		})

		emptyFilteredChildren := tree.DirsHaveChildren(
			[]string{firstNFSProject, lastNFSProject},
			&db.Filter{GIDs: []uint32{42}, Age: db.DGUTAgeAll},
		)
		So(emptyFilteredChildren, ShouldResemble, map[string]bool{
			firstNFSProject: false,
			lastNFSProject:  false,
		})
	})

	Convey("DirsHaveChildren skips unnecessary DGUTA work for broad child checks", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}

		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}
		for _, dir := range []string{mountPath + "leaf-a/", mountPath + "leaf-b/"} {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				uint64(1),
				uint64(10),
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}

		broadParent := mountPath + "broad/"
		for i := range dirsHaveChildrenSummaryFanoutLimit + 1 {
			child := fmt.Sprintf("%schild%02d", broadParent, i)
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, broadParent, child), ShouldBeNil)
		}

		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{mountPath + "leaf-b", mountPath + "leaf-a", mountPath + "missing"},
			nil,
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath + "leaf-a":  false,
			mountPath + "leaf-b":  false,
			mountPath + "missing": false,
		})
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.childBatchQueryCount(), ShouldEqual, 1)

		hasAllChildren, err := dbch.DirsHaveChildren([]string{broadParent}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(hasAllChildren, ShouldResemble, map[string]bool{broadParent: true})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 1)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)

		hasDefaultChildren, err := dbch.DirsHaveChildren([]string{broadParent}, &db.Filter{})
		So(err, ShouldBeNil)
		So(hasDefaultChildren, ShouldResemble, map[string]bool{broadParent: true})
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)

		hasFilteredChildren, err := dbch.DirsHaveChildren(
			[]string{broadParent},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(hasFilteredChildren, ShouldResemble, map[string]bool{broadParent: false})
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren checks active mount root children through facts vectors", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/rootvector/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 6, 1, 9, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		childDir := mountPath + "compact/"
		missingDir := mountPath + "empty/"
		So(conn.Exec(
			ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, childDir[:len(childDir)-1],
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid.String(),
			childDir,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(3),
			uint64(30),
			int64(10),
			int64(20),
			[]uint64{3, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, 3, 0, 0, 0, 0, 0, 0, 0},
		), ShouldBeNil)

		state := newMountDirProjectionState()
		state.addGUTAs(childDir, db.GUTAs{
			testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3),
			testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 2),
		})
		So(writeMountDirProjectionRows(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}, state, defaultBatchSize), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeA1M,
		}
		hasChildren, err := dbch.DirsHaveChildren([]string{mountPath, missingDir}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath:  true,
			missingDir: false,
		})
		So(countingConn.mountVectorQueryCount(), ShouldEqual, 1)

		wrongGIDChildren, err := dbch.DirsHaveChildren(
			[]string{mountPath},
			&db.Filter{GIDs: []uint32{42}, Age: db.DGUTAgeA1M},
		)
		So(err, ShouldBeNil)
		So(wrongGIDChildren, ShouldResemble, map[string]bool{mountPath: false})

		allChildren, err := dbch.DirsHaveChildren([]string{mountPath}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(allChildren, ShouldResemble, map[string]bool{mountPath: true})
	})

	Convey("DirsHaveChildren batches child summaries for small child fanout", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		oldSID := snapshotID(mountPath, updatedAt.Add(-time.Hour))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(snapshotID, dir string, gid, uid uint32, age db.DirGUTAge) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				snapshotID,
				dir,
				gid,
				uid,
				uint16(db.DGUTAFileTypeBam),
				uint8(age),
				uint64(1),
				uint64(10),
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(snapshotID, parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, snapshotID, parent, child), ShouldBeNil)
		}

		parentMatch := mountPath + "match/"
		parentFiltered := mountPath + "filtered/"
		missingParent := mountPath + "missing/"

		matchingChild := parentMatch + "target"
		filteredChild := parentMatch + "wrong-gid"
		onlyFilteredChild := parentFiltered + "wrong-uid"
		oldMatchingChild := parentFiltered + "old-target"

		insertChild(sid.String(), parentMatch, matchingChild)
		insertGUTA(sid.String(), matchingChild+"/", uint32(7), uint32(9), db.DGUTAgeA1M)
		insertChild(sid.String(), parentMatch, filteredChild)
		insertGUTA(sid.String(), filteredChild+"/", uint32(8), uint32(9), db.DGUTAgeA1M)
		insertChild(sid.String(), parentFiltered, onlyFilteredChild)
		insertGUTA(sid.String(), onlyFilteredChild+"/", uint32(7), uint32(10), db.DGUTAgeA1M)
		insertChild(oldSID.String(), parentFiltered, oldMatchingChild)
		insertGUTA(oldSID.String(), oldMatchingChild+"/", uint32(7), uint32(9), db.DGUTAgeA1M)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeA1M,
		}

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{parentFiltered, missingParent, parentMatch, parentMatch},
			filter,
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			parentMatch:    true,
			parentFiltered: false,
			missingParent:  false,
		})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 1)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren uses facts vectors for wide arbitrary-filter child checks", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/vectorchildren/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 12, 8, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		parent := mountPath + "wide/"
		for i := range dirsHaveChildrenSummaryFanoutLimit + 1 {
			child := fmt.Sprintf("%schild%02d", parent, i)
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), parent, child), ShouldBeNil)
		}

		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, parent+"child05/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, parent+"child06/", 8, 2)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{GIDs: []uint32{7}, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll}

		hasChildren, err := dbch.DirsHaveChildren([]string{parent}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{parent: true})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 1)
		So(countingConn.mountVectorQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren answers broad checks from maintained child counts", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/countchildren/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 13, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		parentWithChild := mountPath + "parent/"
		parentWithoutChild := mountPath + "empty/"
		missingParent := mountPath + "missing/"

		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, parentWithChild, 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, parentWithoutChild, 7, 1)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parentWithChild, parentWithChild+"leaf"), ShouldBeNil)

		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{parentWithChild, parentWithoutChild, missingParent},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			parentWithChild:    true,
			parentWithoutChild: false,
			missingParent:      false,
		})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.mountSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren reuses maintained child counts and leaves clicks on maintained summaries", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, 7, 10)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 6)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 7, 4)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/g1/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/g2/", 7, 2)

		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"b"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath+"a/", mountPath+"a/g1"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath+"a/", mountPath+"a/g2"), ShouldBeNil)

		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		filter := &db.Filter{Age: db.DGUTAgeAll}

		parent, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(parent.Children, ShouldHaveLength, 2)

		countingConn.reset()

		hasChildren := tree.DirsHaveChildren([]string{mountPath + "a", mountPath + "b"}, filter)
		So(hasChildren, ShouldResemble, map[string]bool{
			mountPath + "a": true,
			mountPath + "b": false,
		})
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		clicked, err := tree.DirInfo(mountPath+"a", filter)
		So(err, ShouldBeNil)
		So(clicked.Children, ShouldHaveLength, 2)
		So(clicked.Children[0].Count, ShouldEqual, 3)
		So(clicked.Children[1].Count, ShouldEqual, 2)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		leafClicked, err := tree.DirInfo(mountPath+"b", filter)
		So(err, ShouldBeNil)
		So(leafClicked.Children, ShouldBeEmpty)
		So(leafClicked.Current.Count, ShouldEqual, 4)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren uses maintained child counts for broad checks with large child fanout", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"wide/", 7, 1)

		parent := mountPath + "wide/"
		for i := range dirsHaveChildrenSummaryPrefetchLimit + 1 {
			child := fmt.Sprintf("%schild%03d", parent, i)
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		hasChildren, err := dbch.DirsHaveChildren([]string{parent}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{parent: true})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.mountSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren ignores broad summary prefetch failures after child rows prove the answer", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)

		const mountPath = "/mnt/test/"

		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{mountPath}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		parentWithChild := mountPath + "parent/"
		parentWithoutChild := mountPath + "empty/"
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parentWithChild, parentWithChild+"child"), ShouldBeNil)

		failingConn := &dirsHaveChildrenPrefetchFailureConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, failingConn)

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{parentWithChild, parentWithoutChild},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			parentWithChild:    true,
			parentWithoutChild: false,
		})
		So(failingConn.prefetchFailures(), ShouldEqual, 1)
	})

	Convey("DirsHaveChildren keeps the parent-existence query for larger child fanout", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/test/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/test/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		oldSID := snapshotID(mountPath, updatedAt.Add(-time.Hour))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(snapshotID, dir string, gid, uid uint32, ft db.DirGUTAFileType, age db.DirGUTAge) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				snapshotID,
				dir,
				gid,
				uid,
				uint16(ft),
				uint8(age),
				uint64(1),
				uint64(10),
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(snapshotID, parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, snapshotID, parent, child), ShouldBeNil)
		}

		parentMatch := mountPath + "match/"
		parentWrongGID := mountPath + "wrong-gid/"
		parentWrongUID := mountPath + "wrong-uid/"
		parentWrongFT := mountPath + "wrong-ft/"
		parentWrongAge := mountPath + "wrong-age/"
		missingParent := mountPath + "missing/"
		largeFanout := dirsHaveChildrenSummaryFanoutLimit + 1

		for i := range largeFanout {
			child := fmt.Sprintf("%schild%02d", parentMatch, i)
			insertChild(sid.String(), parentMatch, child)
			insertGUTA(
				sid.String(),
				child+"/",
				uint32(8),
				uint32(9),
				db.DGUTAFileTypeBam,
				db.DGUTAgeA1M,
			)
		}

		matchingChild := parentMatch + "target"
		insertChild(sid.String(), parentMatch, matchingChild)
		insertGUTA(
			sid.String(),
			matchingChild+"/",
			uint32(7),
			uint32(9),
			db.DGUTAFileTypeBam,
			db.DGUTAgeA1M,
		)

		for i := range largeFanout {
			child := fmt.Sprintf("%schild%02d", parentWrongGID, i)
			insertChild(sid.String(), parentWrongGID, child)
			insertGUTA(
				sid.String(),
				child+"/",
				uint32(8),
				uint32(9),
				db.DGUTAFileTypeBam,
				db.DGUTAgeA1M,
			)
		}

		oldMatchingChild := parentWrongGID + "old-target"
		insertChild(oldSID.String(), parentWrongGID, oldMatchingChild)
		insertGUTA(
			oldSID.String(),
			oldMatchingChild+"/",
			uint32(7),
			uint32(9),
			db.DGUTAFileTypeBam,
			db.DGUTAgeA1M,
		)

		wrongUIDChild := parentWrongUID + "target"
		insertChild(sid.String(), parentWrongUID, wrongUIDChild)
		insertGUTA(
			sid.String(),
			wrongUIDChild+"/",
			uint32(7),
			uint32(10),
			db.DGUTAFileTypeBam,
			db.DGUTAgeA1M,
		)

		wrongFTChild := parentWrongFT + "target"
		insertChild(sid.String(), parentWrongFT, wrongFTChild)
		insertGUTA(
			sid.String(),
			wrongFTChild+"/",
			uint32(7),
			uint32(9),
			db.DGUTAFileTypeCram,
			db.DGUTAgeA1M,
		)

		wrongAgeChild := parentWrongAge + "target"
		insertChild(sid.String(), parentWrongAge, wrongAgeChild)
		insertGUTA(
			sid.String(),
			wrongAgeChild+"/",
			uint32(7),
			uint32(9),
			db.DGUTAFileTypeBam,
			db.DGUTAgeAll,
		)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeA1M,
		}

		hasChildren, err := dbch.DirsHaveChildren(
			[]string{
				parentWrongGID,
				parentMatch,
				missingParent,
				parentMatch,
				parentWrongUID,
				parentWrongFT,
				parentWrongAge,
			},
			filter,
		)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{
			parentMatch:    true,
			parentWrongGID: false,
			parentWrongUID: false,
			parentWrongFT:  false,
			parentWrongAge: false,
			missingParent:  false,
		})

		hasAnyChildren, err := dbch.DirsHaveChildren([]string{parentWrongGID}, nil)
		So(err, ShouldBeNil)
		So(hasAnyChildren, ShouldResemble, map[string]bool{parentWrongGID: true})

		hasEmptyGIDChildren, err := dbch.DirsHaveChildren(
			[]string{parentMatch},
			&db.Filter{GIDs: []uint32{}, Age: db.DGUTAgeA1M},
		)
		So(err, ShouldBeNil)
		So(hasEmptyGIDChildren, ShouldResemble, map[string]bool{parentMatch: false})

		hasEmptyUIDChildren, err := dbch.DirsHaveChildren(
			[]string{parentMatch},
			&db.Filter{UIDs: []uint32{}, Age: db.DGUTAgeA1M},
		)
		So(err, ShouldBeNil)
		So(hasEmptyUIDChildren, ShouldResemble, map[string]bool{parentMatch: false})

		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
	})
}

const (
	testInsertMountBatchStmt = "INSERT INTO wrstat_mount_events (mount_path, event_at, event_type, " +
		"snapshot_id, updated_at, reason)"
	testInsertDGUTABatchStmt = "INSERT INTO wrstat_dir_facts (mount_path, snapshot_id, dir, updated_at, gids, uids, " +
		"fts, ages, counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at)"
)

func appendTestMountEventRow(batch driver.Batch, mount activeMount) {
	So(batch.Append(mount.mountPath, time.Now(), uint8(1), mount.snapshotID, mount.updatedAt, "publish"), ShouldBeNil)
}

func appendTestDGUTAFactRow(
	batch driver.Batch,
	mountPath string,
	sid string,
	dir string,
	gid uint32,
	uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
) {
	So(batch.Append(
		mountPath,
		sid,
		dir,
		time.Now(),
		[]uint32{gid},
		[]uint32{uid},
		[]uint16{uint16(ft)},
		[]uint8{uint8(db.DGUTAgeAll)},
		[]uint64{count},
		[]uint64{count * 10},
		[]int64{10},
		[]int64{20},
		[][]uint64{testATimeBuckets()},
		[][]uint64{testMTimeBuckets()},
		time.Now(),
	), ShouldBeNil)
}

func newTestDGUTA(gid uint32, uid uint32, ft db.DirGUTAFileType, count uint64) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         db.DGUTAgeAll,
		Count:       count,
		Size:        count * 10,
		Atime:       10,
		Mtime:       20,
		ATimeRanges: summary.AgeBuckets{1, 0, 0, 0, 0, 0, 0, 0, 0},
		MTimeRanges: summary.AgeBuckets{0, 1, 0, 0, 0, 0, 0, 0, 0},
	}
}

func appendTestMountDirProjectionRows(
	summaryBatch driver.Batch,
	setBatch driver.Batch,
	mount activeMount,
	state mountDirProjectionState,
	refreshedAt time.Time,
) {
	for _, dir := range state.factDirs(false) {
		So(appendMountDirFactRow(summaryBatch, mount, dir, state, refreshedAt), ShouldBeNil)
	}

	appendTestMountDirProjectionSetRow(setBatch, mount, refreshedAt)
}

func appendTestMountDirProjectionSetRow(batch driver.Batch, mount activeMount, refreshedAt time.Time) {
	So(batch.Append(mount.mountPath, mount.snapshotID, mount.updatedAt, refreshedAt), ShouldBeNil)
}

func testATimeBuckets() []uint64 {
	return []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
}

func testMTimeBuckets() []uint64 {
	return []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}
}

func TestClickHouseDatabaseDirInfoAncestor(t *testing.T) {
	Convey("DirInfo merges results across mounts for ancestor dirs", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		dbch := newClickHouseDatabase(cfg, cp.conn)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountA = "/lustre/scratchA/"
			mountB = "/lustre/scratchB/"
		)

		updatedA := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		updatedB := time.Date(2026, 1, 10, 14, 0, 0, 0, time.UTC)
		sidA := snapshotID(mountA, updatedA)
		sidB := snapshotID(mountB, updatedB)

		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt,
			mountA, time.Now(), sidA, updatedA,
		), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountA,
			snapshotID: sidA.String(),
			updatedAt:  updatedA,
		})

		So(conn.Exec(ctx, testInsertMountStmt,
			mountB, time.Now(), sidB, updatedB,
		), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountB,
			snapshotID: sidB.String(),
			updatedAt:  updatedB,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		So(conn.Exec(ctx, testInsertDGUTAStmt,
			mountA, sidA, "/lustre/",
			uint32(7), uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(10), uint64(100),
			int64(10), int64(20),
			atimeBuckets, mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx, testInsertDGUTAStmt,
			mountB, sidB, "/lustre/",
			uint32(7), uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(5), uint64(50),
			int64(5), int64(25),
			atimeBuckets, mtimeBuckets,
		), ShouldBeNil)

		sum, err := dbch.DirInfo(
			"/lustre/", &db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 15)
		So(sum.Size, ShouldEqual, 150)
		So(sum.Modtime, ShouldResemble, updatedB)

		Convey("returns ErrDirNotFound for non-existent ancestor", func() {
			_, err := dbch.DirInfo(
				"/nonexistent/",
				&db.Filter{Age: db.DGUTAgeAll},
			)
			So(err, ShouldEqual, db.ErrDirNotFound)
		})
	})
}

func TestClickHouseDatabaseDirInfoScopeResolution(t *testing.T) {
	Convey("DirInfo keeps configured parent mounts in single-mount scope even without an active snapshot", t, func() {
		const (
			parentMount = "/mnt/parent/"
			childMount  = "/mnt/parent/child/"
		)

		updatedAt := time.Date(2026, 1, 11, 9, 0, 0, 0, time.UTC)
		dbch := newClickHouseDatabaseWithSnapshot(
			Config{MountPoints: []string{"/", parentMount, childMount}},
			nil,
			newActiveMountsSnapshot([]mountsActiveRow{{
				mountPath:  childMount,
				snapshotID: snapshotID(childMount, updatedAt).String(),
				updatedAt:  updatedAt,
			}}),
		)

		mountPath, singleMount, err := dbch.resolveMountScope(parentMount)
		So(err, ShouldBeNil)
		So(singleMount, ShouldBeTrue)
		So(mountPath, ShouldEqual, parentMount)

		_, err = dbch.DirInfo(parentMount, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldEqual, db.ErrDirNotFound)
	})

	Convey("DirInfo keeps ancestor scope for directories above nested mountpoints", t, func() {
		dbch := newClickHouseDatabaseWithSnapshot(
			Config{MountPoints: []string{"/", "/lustre/scratchA/", "/lustre/scratchB/"}},
			nil,
			newActiveMountsSnapshot(nil),
		)

		mountPath, singleMount, err := dbch.resolveMountScope("/lustre/")
		So(err, ShouldBeNil)
		So(singleMount, ShouldBeFalse)
		So(mountPath, ShouldBeBlank)
	})
}

func TestClickHouseDatabaseActiveAncestorSummaries(t *testing.T) {
	Convey("Disktree root and ancestor has_children use maintained tree summary rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		const (
			lustreAncestor = "/lustre/"
			mountA         = lustreAncestor + "agentA/"
			mountB         = nfsAncestor + "projectB/"
		)

		cfg.MountPoints = []string{"/", mountA, mountB}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedA := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		updatedB := time.Date(2026, 1, 10, 14, 0, 0, 0, time.UTC)
		sidA := snapshotID(mountA, updatedA)
		sidB := snapshotID(mountB, updatedB)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountA, time.Now(), sidA, updatedA), ShouldBeNil)
		So(conn.Exec(ctx, testInsertMountStmt, mountB, time.Now(), sidB, updatedB), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountA,
			snapshotID: sidA.String(),
			updatedAt:  updatedA,
		})
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountB,
			snapshotID: sidB.String(),
			updatedAt:  updatedB,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(mountPath string, sid fmt.Stringer, dir string, gid uint32, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid.String(),
				dir,
				gid,
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}
		insertChild := func(mountPath string, sid fmt.Stringer, parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), parent, child), ShouldBeNil)
		}

		for _, dir := range []string{"/", lustreAncestor, mountA} {
			insertGUTA(mountA, sidA, dir, 7, 10)
		}

		insertGUTA(mountA, sidA, mountA+"deep/", 7, 3)

		for _, dir := range []string{"/", nfsAncestor, mountB} {
			insertGUTA(mountB, sidB, dir, 8, 5)
		}

		insertGUTA(mountB, sidB, mountB+"deep/", 8, 2)

		insertChild(mountA, sidA, "/", "/lustre")
		insertChild(mountB, sidB, "/", "/nfs")
		insertChild(mountA, sidA, lustreAncestor, "/lustre/agentA")
		insertChild(mountB, sidB, nfsAncestor, "/nfs/projectB")
		insertChild(mountA, sidA, mountA, mountA+"deep")
		insertChild(mountB, sidB, mountB, mountB+"deep")

		rows := []mountsActiveRow{
			{mountPath: mountA, snapshotID: sidA.String(), updatedAt: updatedA},
			{mountPath: mountB, snapshotID: sidB.String(), updatedAt: updatedB},
		}
		So(ensureActiveTreeSummaries(ctx, conn, rows), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		snapshot, _, err := cp.captureActiveMountsState(context.Background())
		So(err, ShouldBeNil)

		countingConn := &ancestorSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)
		tree := db.NewTree(dbch)

		filter := &db.Filter{Age: db.DGUTAgeAll}
		di, err := tree.DirInfo("/", filter)
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 15)
		So(di.Current.Size, ShouldEqual, 150)
		So(di.Current.Modtime, ShouldResemble, updatedB)
		So(di.Children, ShouldHaveLength, 2)
		So(di.Children[0].Dir, ShouldEqual, c3LustreChild)
		So(di.Children[0].Count, ShouldEqual, 10)
		So(di.Children[1].Dir, ShouldEqual, c3NFSChild)
		So(di.Children[1].Count, ShouldEqual, 5)

		hasChildren := tree.DirsHaveChildren(
			[]string{c3LustreChild, c3NFSChild},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(hasChildren, ShouldResemble, map[string]bool{
			c3LustreChild: true,
			c3NFSChild:    false,
		})

		for _, emptyFilter := range []*db.Filter{
			{GIDs: []uint32{}, Age: db.DGUTAgeAll},
			{UIDs: []uint32{}, Age: db.DGUTAgeAll},
		} {
			for _, dir := range []string{"/", lustreAncestor, nfsAncestor} {
				sum, err := dbch.DirInfo(dir, emptyFilter)
				So(err, ShouldBeNil)
				So(sum, ShouldBeNil)

				di, err := tree.DirInfo(dir, emptyFilter)
				So(err, ShouldBeNil)
				So(di, ShouldBeNil)
			}

			So(tree.DirsHaveChildren(
				[]string{"/", lustreAncestor, nfsAncestor},
				emptyFilter,
			), ShouldResemble, map[string]bool{
				"/":            false,
				lustreAncestor: false,
				nfsAncestor:    false,
			})
		}

		So(countingConn.treeSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.ancestorFactVectorQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("Ancestor queries fall back while tree summary refresh is unavailable and use summaries once ready", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		const (
			lustreAncestor = "/lustre/"
			mountPath      = lustreAncestor + "agentA/"
		)

		cfg.MountPoints = []string{"/", mountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		for _, dir := range []string{"/", lustreAncestor} {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid.String(),
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				uint64(4),
				uint64(40),
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}

		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), "/", "/lustre"), ShouldBeNil)

		failingConn := &treeSummaryRefreshDeadlineConn{Conn: conn}
		uncachedCountingConn := &ancestorSummaryQueryCountingConn{Conn: failingConn}
		dbch := newClickHouseDatabase(cfg, uncachedCountingConn)
		filter := &db.Filter{Age: db.DGUTAgeAll}

		sum, err := dbch.DirInfo("/", filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 4)
		So(sum.Size, ShouldEqual, 40)

		hasChildren, err := dbch.DirsHaveChildren([]string{"/"}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{"/": true})
		So(failingConn.treeSummaryAvailabilityQueries(), ShouldBeGreaterThan, 0)
		So(uncachedCountingConn.ancestorFactVectorQueryCount(), ShouldBeGreaterThan, 0)

		rows := []mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}}
		So(ensureActiveTreeSummaries(ctx, conn, rows), ShouldBeNil)

		readyCountingConn := &ancestorSummaryQueryCountingConn{Conn: conn}
		readyDB := newClickHouseDatabase(cfg, readyCountingConn)

		sum, err = readyDB.DirInfo("/", filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 4)
		So(sum.Size, ShouldEqual, 40)
		So(readyCountingConn.treeSummaryQueryCount(), ShouldBeGreaterThan, 0)
		So(readyCountingConn.ancestorFactVectorQueryCount(), ShouldBeGreaterThan, 0)
	})
}

func insertWhereSummaryTestGUTA(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	dir string,
	ft db.DirGUTAFileType,
	count uint64,
) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mountPath,
		sid,
		dir,
		uint32(7),
		uint32(9),
		uint16(ft),
		uint8(db.DGUTAgeAll),
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func dirSummariesByDir(dcss db.DCSs) map[string]*db.DirSummary {
	byDir := make(map[string]*db.DirSummary, len(dcss))
	for _, dcs := range dcss {
		byDir[dcs.Dir] = dcs
	}

	return byDir
}

func addSeededDGUTAToMountDirProjectionState(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	state *mountDirProjectionState,
) error {
	rows, err := conn.Query(ctx,
		"SELECT dir, "+dgutaTupleColumns+" FROM ("+
			"SELECT dir, arrayJoin("+dgutaArrayZipExpr+") AS g FROM wrstat_dir_facts "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?))",
		mount.mountPath,
		mount.snapshotID,
	)
	if err != nil {
		return err
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		dir, guta, err := scanSeededProjectionGUTA(rows)
		if err != nil {
			return err
		}

		state.addGUTA(dir, guta)
	}

	return rows.Err()
}

func scanSeededProjectionGUTA(rows rowsScanner) (string, *db.GUTA, error) {
	var (
		dir string
		s   dgutaScanned
	)

	if err := rows.Scan(
		&dir,
		&s.gid,
		&s.uid,
		&s.ft,
		&s.age,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
	); err != nil {
		return "", nil, err
	}

	return dir, s.guta(), nil
}

func addSeededChildrenToMountDirProjectionState(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	state *mountDirProjectionState,
) error {
	rows, err := conn.Query(ctx,
		"SELECT parent_dir, count() FROM wrstat_children "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) GROUP BY parent_dir",
		mount.mountPath,
		mount.snapshotID,
	)
	if err != nil {
		return err
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			parentDir string
			count     uint64
		)

		if err := rows.Scan(&parentDir, &count); err != nil {
			return err
		}

		state.addChildren(parentDir, count)
	}

	return rows.Err()
}

type providerCloser interface {
	Close() error
}

type dirFilterAgeAllTestEnv struct {
	cfg          Config
	conn         ch.Conn
	provider     providerCloser
	providerConn ch.Conn
	mount        activeMount
	parentDir    string
}

func (e dirFilterAgeAllTestEnv) createAgeAllIndex() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(e.conn.Exec(ctx, createDirFilterAgeAllTableForTest), ShouldBeNil)
}

func (e dirFilterAgeAllTestEnv) createAndSeedAgeAllIndex() {
	e.createAgeAllIndex()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(e.conn.Exec(ctx,
		"INSERT INTO wrstat_dir_filter_ageall "+
			"SELECT mount_path, snapshot_id, tupleElement(g, 1) AS gid, tupleElement(g, 2) AS uid, "+
			"tupleElement(g, 3) AS ft, dir, tupleElement(g, 5) AS count, tupleElement(g, 6) AS size, "+
			"tupleElement(g, 7) AS atime_min, tupleElement(g, 8) AS mtime_max, "+
			"tupleElement(g, 9) AS atime_buckets, tupleElement(g, 10) AS mtime_buckets, now() "+
			"FROM (SELECT mount_path, snapshot_id, dir, arrayJoin("+dgutaArrayZipExpr+") AS g "+
			"FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = toUUID(?)) "+
			"WHERE tupleElement(g, 4) = ?",
		e.mount.mountPath,
		e.mount.snapshotID,
		uint8(db.DGUTAgeAll),
	), ShouldBeNil)
}

func (e dirFilterAgeAllTestEnv) insertAgeSpecificFacts() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(e.conn.Exec(ctx,
		testInsertDGUTAStmt,
		e.mount.mountPath,
		e.mount.snapshotID,
		e.mount.mountPath+"a/",
		uint32(7),
		uint32(9),
		uint16(db.DGUTAFileTypeBam),
		uint8(db.DGUTAgeM1Y),
		uint64(4),
		uint64(40),
		int64(10),
		int64(20),
		[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func assertAgeAllWhereUsesMandatoryIndex(env dirFilterAgeAllTestEnv) {
	env.createAndSeedAgeAllIndex()

	defer useStrictlyFasterAgeAllWhereRouteForTest()()

	countingConn := &whereQueryCountingConn{Conn: env.providerConn}
	tree := db.NewTree(newClickHouseDatabase(env.cfg, countingConn))
	filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

	dcss, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
	So(err, ShouldBeNil)
	So(dirSummariesByDir(dcss)[env.mount.mountPath].Count, ShouldEqual, 12)
	So(countingConn.filterAgeAllQueryCountValue(), ShouldBeGreaterThan, 0)
	So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 0)
}

type stringerString string

func (s stringerString) String() string {
	return string(s)
}

type mountDirSummaryReadinessConn struct {
	ch.Conn

	queries atomic.Int32
	ready   atomic.Bool
}

func (c *mountDirSummaryReadinessConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if strings.Contains(query, "FROM wrstat_dir_projection_sets") {
		c.queries.Add(1)

		return &mountDirSummaryReadinessRows{ready: c.ready.Load()}, nil
	}

	return nil, errUnexpectedMountDirSummaryReadyQuery
}

func (c *mountDirSummaryReadinessConn) setReady(ready bool) {
	c.ready.Store(ready)
}

func (c *mountDirSummaryReadinessConn) queryCount() int {
	return int(c.queries.Load())
}

type treeSummaryRefreshDeadlineConn struct {
	ch.Conn

	failures            atomic.Int32
	availabilityQueries atomic.Int32
}

func (c *treeSummaryRefreshDeadlineConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	isAvailabilityQuery := strings.Contains(query, "FROM wrstat_virtual_summary_sets") ||
		strings.Contains(query, "FROM wrstat_virtual_summary_cache")

	if isAvailabilityQuery {
		c.availabilityQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *treeSummaryRefreshDeadlineConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.Contains(query, "INSERT INTO wrstat_virtual_summary_") {
		c.failures.Add(1)

		return context.DeadlineExceeded
	}

	return c.Conn.Exec(ctx, query, args...)
}

func (c *treeSummaryRefreshDeadlineConn) treeSummaryRefreshFailures() int {
	return int(c.failures.Load())
}

func (c *treeSummaryRefreshDeadlineConn) treeSummaryAvailabilityQueries() int {
	return int(c.availabilityQueries.Load())
}

func seedSchema3A3ChildFilterAll(t *testing.T, conn ch.Conn, mountPath string, sid string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	So(conn.Exec(ctx, createChildFilterAllTableForTest), ShouldBeNil)
	So(conn.Exec(
		ctx,
		"ALTER TABLE wrstat_child_filter_all DELETE "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?) SETTINGS mutations_sync = 1",
		mountPath,
		sid,
	), ShouldBeNil)
	seedSchema3A3Readiness(t, conn, mountPath, sid, uint64(schema3A2ChildCount))

	batch, err := conn.PrepareBatch(ctx, insertChildFilterAllForTest)
	So(err, ShouldBeNil)

	for i := range schema3A2ChildCount {
		So(batch.Append(
			mountPath,
			sid,
			schema3A2ParentDir,
			uint8(db.DGUTAgeA1M),
			uint32(7),
			uint32(11),
			uint16(db.DGUTAFileTypeBam),
			schema3A2ChildPath(i),
			uint64(1),
			uint64(10+i),
			int64(90),
			int64(250),
			a1AgeBuckets(1),
			a1AgeBuckets(1),
			schema3A3FilteredChildCount(i),
			schema3A3AlternatingChildCount(i),
			schema3A3FilteredHasChildren(i),
			schema3A3AlternatingHasChildren(i),
			time.Now(),
		), ShouldBeNil)
	}

	So(batch.Send(), ShouldBeNil)
}

func schema3A3FilteredChildCount(index int) uint64 {
	if schema3A3FilteredHasChildren(index) > 0 {
		return 1
	}

	return 0
}

func schema3A3AlternatingHasChildren(index int) uint8 {
	if schema3A3AlternatingChildCount(index) > 0 {
		return 1
	}

	return 0
}

func seedSchema3A3Readiness(
	t *testing.T,
	conn ch.Conn,
	mountPath string,
	sid string,
	childFilterAllRows uint64,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(conn.Exec(ctx, createSchema3SnapshotSetsTableForTest), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetForTest,
		mountPath,
		sid,
		currentSchemaVersion,
		1,
		uint64(schema3A2ChildCount+1),
		uint64(schema3A2ChildCount),
		childFilterAllRows,
		0,
		"test",
	), ShouldBeNil)
}

type b2AllFilterReaderEnv struct {
	cfg          Config
	conn         ch.Conn
	providerConn ch.Conn
	mount        activeMount
}

func (e b2AllFilterReaderEnv) expectedDirInfos(
	rawDB *clickHouseDatabase,
	dirs []string,
	filter *db.Filter,
) map[string]*db.DirSummary {
	expected := make(map[string]*db.DirSummary, len(dirs))

	for _, dir := range dirs {
		queryDir := ensureTrailingSlash(dir)
		gutas, err := rawDB.gutasForDir(e.mount.mountPath, e.mount.snapshotID, queryDir)
		So(err, ShouldBeNil)

		sum := dirSummaryWithModtime(gutas, filter, e.mount.updatedAt)
		if sum == nil {
			continue
		}

		sum.Dir = dir
		expected[dir] = sum
	}

	return expected
}

type b2AllFilterRecord struct {
	dir   string
	age   db.DirGUTAge
	gid   uint32
	uid   uint32
	ft    db.DirGUTAFileType
	count uint64
	size  uint64
	atime int64
	mtime int64
}

type hasChildrenQueryCountingConn struct {
	ch.Conn

	childBatchQueries        atomic.Int32
	childSummaryBatchQueries atomic.Int32
	existenceQueries         atomic.Int32
	filterAgeAllQueries      atomic.Int32
	mountSummaryQueries      atomic.Int32
	mountVectorQueries       atomic.Int32
}

func (c *hasChildrenQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	isChildBatchQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
	if isChildBatchQuery {
		c.childBatchQueries.Add(1)
	}

	isChildSummaryBatchQuery := isFactVectorDirInfoBatchQuery(query) || isGroupedDirInfoSummaryQuery(query)
	if isChildSummaryBatchQuery {
		c.childSummaryBatchQueries.Add(1)
	}

	isExistenceQuery := strings.Contains(query, "INNER JOIN wrstat_dir_facts d") &&
		strings.Contains(query, "GROUP BY c.parent_dir")
	if isExistenceQuery {
		c.existenceQueries.Add(1)
	}

	if isDirFilterAgeAllReadQuery(query) {
		c.filterAgeAllQueries.Add(1)
	}

	if isMountDirInfoSummaryQuery(query) {
		c.mountSummaryQueries.Add(1)
	}

	if isMountDirInfoVectorQuery(query) {
		c.mountVectorQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isFactVectorDirInfoBatchQuery(query string) bool {
	return strings.Contains(query, "arrayJoin(arrayZip(gids, uids, fts, ages, counts") &&
		!strings.Contains(query, "sumForEachIf") &&
		strings.Contains(query, "WHERE dir IN (")
}

func isGroupedDirInfoSummaryQuery(query string) bool {
	return strings.Contains(query, "sumForEachIf") &&
		strings.Contains(query, "arrayJoin(arrayZip(gids, uids, fts, ages, counts") &&
		strings.Contains(query, "WHERE dir IN (") &&
		strings.Contains(query, "GROUP BY dir")
}

func isDirFilterAgeAllReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")
	if strings.Contains(normalised, "SELECT count() FROM wrstat_dir_filter_ageall") {
		return false
	}

	return strings.Contains(normalised, "FROM wrstat_dir_filter_ageall") ||
		strings.Contains(normalised, "JOIN wrstat_dir_filter_ageall")
}

func isMountDirInfoSummaryQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts") &&
		(strings.Contains(query, "all_count") || strings.Contains(query, "file_count")) &&
		!strings.Contains(query, "wrstat_dir_projection_sets")
}

func isMountDirInfoVectorQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts") &&
		strings.Contains(query, "updated_at, gids, uids")
}

func (c *hasChildrenQueryCountingConn) childBatchQueryCount() int {
	return int(c.childBatchQueries.Load())
}

func (c *hasChildrenQueryCountingConn) childSummaryBatchQueryCount() int {
	return int(c.childSummaryBatchQueries.Load())
}

func (c *hasChildrenQueryCountingConn) existenceQueryCount() int {
	return int(c.existenceQueries.Load())
}

func (c *hasChildrenQueryCountingConn) filterAgeAllQueryCount() int {
	return int(c.filterAgeAllQueries.Load())
}

func (c *hasChildrenQueryCountingConn) mountSummaryQueryCount() int {
	return int(c.mountSummaryQueries.Load())
}

func (c *hasChildrenQueryCountingConn) mountVectorQueryCount() int {
	return int(c.mountVectorQueries.Load())
}

type dirsHaveChildrenPrefetchFailureConn struct {
	ch.Conn

	failures atomic.Int32
}

func (c *dirsHaveChildrenPrefetchFailureConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if isMountDirInfoSummaryQuery(query) {
		c.failures.Add(1)

		return nil, errForcedMaintainedSummaryPrefetchFailure
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *dirsHaveChildrenPrefetchFailureConn) prefetchFailures() int {
	return int(c.failures.Load())
}

type dirInfoSummaryQueryCountingConn struct {
	ch.Conn

	childFilterAllQueries  atomic.Int32
	ageAllQueries          atomic.Int32
	groupedSummaryQueries  atomic.Int32
	dirFilterAllQueries    atomic.Int32
	factVectorBatchQueries atomic.Int32
	mountSummaryQueries    atomic.Int32
	mountVectorQueries     atomic.Int32
}

func (c *dirInfoSummaryQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.recordSummaryReadQuery(query)

	return c.Conn.Query(ctx, query, args...)
}

func (c *dirInfoSummaryQueryCountingConn) recordSummaryReadQuery(query string) {
	if isGroupedDirInfoSummaryQuery(query) {
		c.groupedSummaryQueries.Add(1)
	}

	if isFactVectorDirInfoBatchQuery(query) {
		c.factVectorBatchQueries.Add(1)
	}

	if isDirFilterAllReadQuery(query) {
		c.dirFilterAllQueries.Add(1)
	}

	if isChildFilterAllPacketQuery(query) {
		c.childFilterAllQueries.Add(1)
	}

	if isDirFilterAgeAllReadQuery(query) {
		c.ageAllQueries.Add(1)
	}

	c.recordMaintainedSummaryReadQuery(query)
}

func isDirFilterAllReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")
	if strings.Contains(normalised, "FROM wrstat_schema3_snapshot_sets") {
		return false
	}

	return strings.Contains(normalised, "FROM wrstat_dir_filter_all") ||
		strings.Contains(normalised, "JOIN wrstat_dir_filter_all")
}

func (c *dirInfoSummaryQueryCountingConn) recordMaintainedSummaryReadQuery(query string) {
	if isMountDirInfoSummaryQuery(query) {
		c.mountSummaryQueries.Add(1)
	}

	if isMountDirInfoVectorQuery(query) {
		c.mountVectorQueries.Add(1)
	}
}

func (c *dirInfoSummaryQueryCountingConn) reset() {
	c.childFilterAllQueries.Store(0)
	c.ageAllQueries.Store(0)
	c.groupedSummaryQueries.Store(0)
	c.dirFilterAllQueries.Store(0)
	c.factVectorBatchQueries.Store(0)
	c.mountSummaryQueries.Store(0)
	c.mountVectorQueries.Store(0)
}

func (c *dirInfoSummaryQueryCountingConn) groupedSummaryQueryCount() int {
	return int(c.groupedSummaryQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) factVectorBatchQueryCount() int {
	return int(c.factVectorBatchQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) ageAllQueryCount() int {
	return int(c.ageAllQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) dirFilterAllQueryCount() int {
	return int(c.dirFilterAllQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) childFilterAllQueryCount() int {
	return int(c.childFilterAllQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) mountDirSummaryQueryCount() int {
	return int(c.mountSummaryQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) mountDirVectorQueryCount() int {
	return int(c.mountVectorQueries.Load())
}

type parentFactsDisktreeRouteConn struct {
	ch.Conn

	ageAllReads           atomic.Int32
	childFilterAllReads   atomic.Int32
	childrenReads         atomic.Int32
	childrenBatchReads    atomic.Int32
	dirFactsINReads       atomic.Int32
	parentFactRangeReads  atomic.Int32
	parentFactScalarReads atomic.Int32
	parentFactVectorReads atomic.Int32
	queries               atomic.Int32
}

func (c *parentFactsDisktreeRouteConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries.Add(1)

	if isParentFactsRangeQuery(query) {
		c.parentFactRangeReads.Add(1)
	}

	if isParentFactsScalarReadQuery(query) {
		c.parentFactScalarReads.Add(1)
	}

	if isParentFactsVectorReadQuery(query) {
		c.parentFactVectorReads.Add(1)
	}

	if isDirFactsINReadQuery(query) {
		c.dirFactsINReads.Add(1)
	}

	if isDirFilterAgeAllReadQuery(query) {
		c.ageAllReads.Add(1)
	}

	if isChildFilterAllPacketQuery(query) {
		c.childFilterAllReads.Add(1)
	}

	if isChildrenReadQuery(query) {
		c.childrenReads.Add(1)
	}

	if isChildrenBatchReadQuery(query) {
		c.childrenBatchReads.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isParentFactsScalarReadQuery(query string) bool {
	return isParentFactsRangeQuery(query) && !isParentFactsVectorReadQuery(query)
}

func isParentFactsVectorReadQuery(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")

	return isParentFactsRangeQuery(query) &&
		strings.Contains(normalised, "gids, uids, fts, ages, counts")
}

func isDirFactsINReadQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts") &&
		strings.Contains(query, "WHERE dir IN (")
}

func isChildFilterAllPacketQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_child_filter_all")
}

func isChildrenReadQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_children") ||
		strings.Contains(query, "FROM wrstat_children AS")
}

func isChildrenBatchReadQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
}

func (c *parentFactsDisktreeRouteConn) ageAllQueries() int {
	return int(c.ageAllReads.Load())
}

func (c *parentFactsDisktreeRouteConn) childFilterAllPacketQueries() int {
	return int(c.childFilterAllReads.Load())
}

func (c *parentFactsDisktreeRouteConn) childrenQueries() int {
	return int(c.childrenReads.Load())
}

func (c *parentFactsDisktreeRouteConn) childrenBatchQueries() int {
	return int(c.childrenBatchReads.Load())
}

func (c *parentFactsDisktreeRouteConn) dirFactsINQueries() int {
	return int(c.dirFactsINReads.Load())
}

func (c *parentFactsDisktreeRouteConn) parentFactRangeQueries() int {
	return int(c.parentFactRangeReads.Load())
}

func (c *parentFactsDisktreeRouteConn) parentFactScalarQueries() int {
	return int(c.parentFactScalarReads.Load())
}

func (c *parentFactsDisktreeRouteConn) parentFactVectorQueries() int {
	return int(c.parentFactVectorReads.Load())
}

func (c *parentFactsDisktreeRouteConn) queryCountValue() int {
	return int(c.queries.Load())
}

func (c *parentFactsDisktreeRouteConn) resetCounts() {
	c.ageAllReads.Store(0)
	c.childFilterAllReads.Store(0)
	c.childrenReads.Store(0)
	c.childrenBatchReads.Store(0)
	c.dirFactsINReads.Store(0)
	c.parentFactRangeReads.Store(0)
	c.parentFactScalarReads.Store(0)
	c.parentFactVectorReads.Store(0)
	c.queries.Store(0)
}

type dirFactsReadinessGuardConn struct {
	ch.Conn

	readinessSeen         atomic.Bool
	readinessQueries      atomic.Int32
	factQueries           atomic.Int32
	snapshotQueriesBefore atomic.Int32
	snapshotQueriesAfter  atomic.Int32
}

func (c *dirFactsReadinessGuardConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM wrstat_dir_projection_sets") {
		c.readinessQueries.Add(1)
		c.readinessSeen.Store(true)

		return c.Conn.Query(ctx, query, args...)
	}

	if isSnapshotProjectionRead(query) {
		c.recordSnapshotProjectionRead(query)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isSnapshotProjectionRead(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts") ||
		strings.Contains(query, "FROM wrstat_children") ||
		strings.Contains(query, "FROM wrstat_dir_filter_ageall")
}

func (c *dirFactsReadinessGuardConn) recordSnapshotProjectionRead(query string) {
	if !c.readinessSeen.Load() {
		c.snapshotQueriesBefore.Add(1)

		return
	}

	c.snapshotQueriesAfter.Add(1)

	if strings.Contains(query, "FROM wrstat_dir_facts") {
		c.factQueries.Add(1)
	}
}

func (c *dirFactsReadinessGuardConn) readinessQueryCount() int {
	return int(c.readinessQueries.Load())
}

func (c *dirFactsReadinessGuardConn) factQueriesAfterReadiness() int {
	return int(c.factQueries.Load())
}

func (c *dirFactsReadinessGuardConn) snapshotQueriesBeforeReadiness() int {
	return int(c.snapshotQueriesBefore.Load())
}

func (c *dirFactsReadinessGuardConn) snapshotQueriesAfterReadiness() int {
	return int(c.snapshotQueriesAfter.Load())
}

type whereQueryCountingConn struct {
	ch.Conn

	allMountChildQueries        atomic.Int32
	childBatchQueries           atomic.Int32
	filterAllQueries            atomic.Int32
	filterAgeAllQueries         atomic.Int32
	filteredMountSummaryQueries atomic.Int32
	mountVectorQueries          atomic.Int32
	queries                     atomic.Int32
	summaryBatchQueries         atomic.Int32
	subtreeQueries              atomic.Int32
}

func (c *whereQueryCountingConn) allMountChildQueryCountValue() int {
	return int(c.allMountChildQueries.Load())
}

func (c *whereQueryCountingConn) childBatchQueryCountValue() int {
	return int(c.childBatchQueries.Load())
}

func (c *whereQueryCountingConn) filterAllQueryCountValue() int {
	return int(c.filterAllQueries.Load())
}

func (c *whereQueryCountingConn) filterAgeAllQueryCountValue() int {
	return int(c.filterAgeAllQueries.Load())
}

func (c *whereQueryCountingConn) filteredMountSummaryQueryCountValue() int {
	return int(c.filteredMountSummaryQueries.Load())
}

func (c *whereQueryCountingConn) mountDirVectorQueryCount() int {
	return int(c.mountVectorQueries.Load())
}

func (c *whereQueryCountingConn) subtreeQueryCountValue() int {
	return int(c.subtreeQueries.Load())
}

func (c *whereQueryCountingConn) resetCounts() {
	c.allMountChildQueries.Store(0)
	c.childBatchQueries.Store(0)
	c.filterAllQueries.Store(0)
	c.filterAgeAllQueries.Store(0)
	c.filteredMountSummaryQueries.Store(0)
	c.mountVectorQueries.Store(0)
	c.queries.Store(0)
	c.summaryBatchQueries.Store(0)
	c.subtreeQueries.Store(0)
}

func (c *whereQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries.Add(1)

	isSubtreeQuery := strings.Contains(query, "startsWith(dir, ?)") ||
		strings.Contains(query, "startsWith(d.dir, ?)")
	if isSubtreeQuery {
		c.subtreeQueries.Add(1)
	}

	if isFactVectorDirInfoBatchQuery(query) {
		c.summaryBatchQueries.Add(1)
	}

	if isFilteredMountWhereSummaryQuery(query) {
		c.filteredMountSummaryQueries.Add(1)
	}

	if isDirFilterAllReadQuery(query) {
		c.filterAllQueries.Add(1)
	}

	if isDirFilterAgeAllReadQuery(query) {
		c.filterAgeAllQueries.Add(1)
	}

	if isMountDirInfoVectorQuery(query) {
		c.mountVectorQueries.Add(1)
	}

	isChildBatchQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
	if isChildBatchQuery {
		c.childBatchQueries.Add(1)
	}

	isAllMountChildQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		!strings.Contains(query, "WHERE parent_dir IN (") &&
		!strings.Contains(query, "JOIN")
	if isAllMountChildQuery {
		c.allMountChildQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isFilteredMountWhereSummaryQuery(query string) bool {
	return strings.Contains(query, "arrayJoin(arrayZip(gids, uids, fts, ages, counts") &&
		strings.Contains(query, "GROUP BY dir") &&
		!strings.Contains(query, "WHERE dir IN (") &&
		!strings.Contains(query, "startsWith")
}

func (c *whereQueryCountingConn) queryCountValue() int {
	return int(c.queries.Load())
}

func TestClickHouseDatabaseWhereFastPath(t *testing.T) {
	Convey("Where synthesises virtual ancestors over many active mount roots", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/", "/lustre/", nfsAncestor}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		mountBatch, err := conn.PrepareBatch(ctx, testInsertMountBatchStmt)
		So(err, ShouldBeNil)
		gutaBatch, err := conn.PrepareBatch(ctx, testInsertDGUTABatchStmt)
		So(err, ShouldBeNil)
		projectionSetBatch, err := conn.PrepareBatch(ctx, insertMountDirSummarySetQuery)
		So(err, ShouldBeNil)

		refreshedAt := time.Now().UTC()
		gutaBatchRows := 0
		projectionSetBatchRows := 0

		flushGUTABatch := func() {
			if gutaBatchRows == 0 {
				return
			}

			So(gutaBatch.Send(), ShouldBeNil)

			var prepareErr error

			gutaBatch, prepareErr = conn.PrepareBatch(ctx, testInsertDGUTABatchStmt)
			So(prepareErr, ShouldBeNil)

			gutaBatchRows = 0
		}

		flushProjectionSetBatch := func() {
			if projectionSetBatchRows == 0 {
				return
			}

			So(projectionSetBatch.Send(), ShouldBeNil)

			var prepareErr error

			projectionSetBatch, prepareErr = conn.PrepareBatch(ctx, insertMountDirSummarySetQuery)
			So(prepareErr, ShouldBeNil)

			projectionSetBatchRows = 0
		}

		insertMount := func(mountPath string, updatedAt time.Time, count uint64) mountsActiveRow {
			sid := snapshotID(mountPath, updatedAt)
			mount := activeMount{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			}
			appendTestMountEventRow(mountBatch, mount)
			appendTestMountDirProjectionSetRow(projectionSetBatch, mount, refreshedAt)

			projectionSetBatchRows++

			if projectionSetBatchRows >= 90 {
				flushProjectionSetBatch()
			}

			appendTestDGUTAFactRow(
				gutaBatch, mountPath, sid.String(), mountPath, uint32(7), uint32(9), db.DGUTAFileTypeBam, count,
			)

			gutaBatchRows++

			if gutaBatchRows >= 90 {
				flushGUTABatch()
			}

			return mountsActiveRow{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			}
		}

		const nfsMountCount = 100

		rows := make([]mountsActiveRow, 0, nfsMountCount+1)

		var nfsCount uint64

		baseUpdatedAt := time.Date(2026, 1, 12, 11, 0, 0, 0, time.UTC)

		for i := range nfsMountCount {
			count := uint64(i + 1)
			nfsCount += count
			mountPath := fmt.Sprintf("/nfs/project%03d/", i)
			rows = append(rows, insertMount(mountPath, baseUpdatedAt.Add(time.Duration(i)*time.Minute), count))
		}

		const lustreCount = uint64(200)

		lustreMount := "/lustre/scratchZ/"
		rows = append(rows, insertMount(lustreMount, baseUpdatedAt.Add(3*time.Hour), lustreCount))

		So(mountBatch.Send(), ShouldBeNil)
		flushGUTABatch()
		flushProjectionSetBatch()

		So(ensureActiveTreeSummaries(ctx, conn, rows), ShouldBeNil)

		snapshot := newActiveMountsSnapshot(rows)
		snapshot.markTreeSummaryReady()
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, cp.conn, snapshot))
		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)

		nfsDCSs, err := tree.Where("/nfs", filter, splitFn)
		So(err, ShouldBeNil)
		So(nfsDCSs, ShouldNotBeEmpty)

		nfsByDir := dirSummariesByDir(nfsDCSs)
		So(nfsByDir["/nfs"].Count, ShouldEqual, nfsCount)
		So(nfsByDir["/nfs/project000"].Count, ShouldEqual, 1)
		So(nfsByDir["/nfs/project099"].Count, ShouldEqual, 100)

		rootDCSs, err := tree.Where("/", filter, splitFn)
		So(err, ShouldBeNil)
		So(rootDCSs, ShouldNotBeEmpty)

		rootByDir := dirSummariesByDir(rootDCSs)
		So(rootByDir["/"].Count, ShouldEqual, nfsCount+lustreCount)
		So(rootByDir["/nfs"].Count, ShouldEqual, nfsCount)
		So(rootByDir["/lustre/scratchZ"].Count, ShouldEqual, lustreCount)
	})

	Convey("Where resolves a filtered subtree without recursive query fanout", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/test/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/test/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(dir string, gid, uid uint32, ft db.DirGUTAFileType, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				gid,
				uid,
				uint16(ft),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}

		insertGUTA(mountPath, 7, 9, db.DGUTAFileTypeBam, 6)
		insertGUTA(mountPath+"a/", 7, 9, db.DGUTAFileTypeBam, 6)
		insertGUTA(mountPath+"a/b/", 7, 9, db.DGUTAFileTypeBam, 4)
		insertGUTA(mountPath+"a/b/deep/", 7, 9, db.DGUTAFileTypeBam, 4)
		insertGUTA(mountPath+"a/c/", 7, 9, db.DGUTAFileTypeBam, 2)
		insertGUTA(mountPath, 8, 10, db.DGUTAFileTypeCram, 100)
		insertGUTA(mountPath+"a/", 8, 10, db.DGUTAFileTypeCram, 100)
		insertGUTA(mountPath+"a/c/", 8, 10, db.DGUTAFileTypeCram, 100)

		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath+"a/", mountPath+"a/b"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath+"a/", mountPath+"a/c"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath+"a/b/", mountPath+"a/b/deep"), ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeAll,
		}

		dcss, err := tree.Where(mountPath, filter, split.SplitsToSplitFn(1))
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, db.DCSs{
			{
				Dir:         mountPath + "a",
				Count:       6,
				Size:        60,
				Atime:       time.Unix(10, 0),
				CommonATime: summary.Range7Years,
				Mtime:       time.Unix(20, 0),
				CommonMTime: summary.Range5Years,
				UIDs:        []uint32{9},
				GIDs:        []uint32{7},
				FT:          db.DGUTAFileTypeBam,
				Age:         db.DGUTAgeAll,
				Modtime:     updatedAt,
			},
			{
				Dir:         mountPath + "a/b/deep",
				Count:       4,
				Size:        40,
				Atime:       time.Unix(10, 0),
				CommonATime: summary.Range7Years,
				Mtime:       time.Unix(20, 0),
				CommonMTime: summary.Range5Years,
				UIDs:        []uint32{9},
				GIDs:        []uint32{7},
				FT:          db.DGUTAFileTypeBam,
				Age:         db.DGUTAgeAll,
				Modtime:     updatedAt,
			},
			{
				Dir:         mountPath + "a/c",
				Count:       2,
				Size:        20,
				Atime:       time.Unix(10, 0),
				CommonATime: summary.Range7Years,
				Mtime:       time.Unix(20, 0),
				CommonMTime: summary.Range5Years,
				UIDs:        []uint32{9},
				GIDs:        []uint32{7},
				FT:          db.DGUTAFileTypeBam,
				Age:         db.DGUTAgeAll,
				Modtime:     updatedAt,
			},
		})
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)
		So(countingConn.queryCountValue(), ShouldBeLessThanOrEqualTo, 12)
	})

	Convey("Where merges ancestor directories across pinned active mounts", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/", "/lustre/scratchA/", "/lustre/scratchB/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const (
			mountA = "/lustre/scratchA/"
			mountB = "/lustre/scratchB/"
		)

		updatedA := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		updatedB := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
		sidA := snapshotID(mountA, updatedA)
		sidB := snapshotID(mountB, updatedB)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountA, time.Now(), sidA, updatedA), ShouldBeNil)
		So(conn.Exec(ctx, testInsertMountStmt, mountB, time.Now(), sidB, updatedB), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountA,
			snapshotID: sidA.String(),
			updatedAt:  updatedA,
		})
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountB,
			snapshotID: sidB.String(),
			updatedAt:  updatedB,
		})

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		insertGUTA := func(mountPath, sid, dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				atimeBuckets,
				mtimeBuckets,
			), ShouldBeNil)
		}

		insertGUTA(mountA, sidA.String(), "/lustre/", 10)
		insertGUTA(mountA, sidA.String(), mountA, 10)
		insertGUTA(mountB, sidB.String(), "/lustre/", 5)
		insertGUTA(mountB, sidB.String(), mountB, 5)

		So(conn.Exec(ctx, testInsertChildrenStmt, mountA, sidA, "/lustre/", mountA), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountB, sidB, "/lustre/", mountB), ShouldBeNil)

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{
			{mountPath: mountA, snapshotID: sidA.String(), updatedAt: updatedA},
			{mountPath: mountB, snapshotID: sidB.String(), updatedAt: updatedB},
		})
		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot))

		dcss, err := tree.Where("/lustre/", nil, split.SplitsToSplitFn(1))
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 3)
		So(dcss[0].Dir, ShouldEqual, "/lustre/")
		So(dcss[0].Count, ShouldEqual, 15)
		So(dcss[0].Modtime, ShouldResemble, updatedB)
		So(dcss[1].Dir, ShouldEqual, "/lustre/scratchA")
		So(dcss[1].Count, ShouldEqual, 10)
		So(dcss[1].Modtime, ShouldResemble, updatedA)
		So(dcss[2].Dir, ShouldEqual, "/lustre/scratchB")
		So(dcss[2].Count, ShouldEqual, 5)
		So(dcss[2].Modtime, ShouldResemble, updatedB)
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)
		So(countingConn.queryCountValue(), ShouldBeLessThanOrEqualTo, 8)
	})

	Convey("Where traverses shallow single-mount splits without full subtree scans", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/shallow/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/shallow/"

		updatedAt := time.Date(2026, 1, 11, 13, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt).String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		})

		insertSummary := func(dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertSummary(mountPath, 10)
		insertSummary(mountPath+"branch/", 10)
		insertSummary(mountPath+"branch/a/", 6)
		insertSummary(mountPath+"branch/b/", 4)
		insertChild(mountPath, mountPath+"branch")
		insertChild(mountPath+"branch/", mountPath+"branch/a")
		insertChild(mountPath+"branch/", mountPath+"branch/b")

		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(0)
		genericTree := db.NewTree(&clickHouseGenericTreeDB{
			d: newClickHouseDatabase(cfg, cp.conn),
		})
		expected, err := genericTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		fastTree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		actual, err := fastTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)
		So(countingConn.summaryBatchQueryCountValue(), ShouldBeLessThanOrEqualTo, 2)
		So(countingConn.queryCountValue(), ShouldBeLessThanOrEqualTo, 6)
	})

	Convey("Where batches only the traversed split frontier", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/frontier/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		mountPath := "/mnt/frontier/"
		updatedAt := time.Date(2026, 1, 11, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt).String()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		})

		insertSummary := func(dir string, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				uint32(7),
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertSummary(mountPath, 100)
		insertSummary(mountPath+"stem/", 100)
		insertChild(mountPath, mountPath+"stem")

		for branch := range 8 {
			branchDir := fmt.Sprintf("%sstem/branch%02d/", mountPath, branch)
			insertSummary(branchDir, 5)
			insertChild(mountPath+"stem/", strings.TrimSuffix(branchDir, "/"))

			for leaf := range 3 {
				leafDir := fmt.Sprintf("%sleaf%02d/", branchDir, leaf)
				insertSummary(leafDir, 1)
				insertChild(branchDir, strings.TrimSuffix(leafDir, "/"))

				for extra := range 2 {
					extraDir := fmt.Sprintf("%sunused%02d/", leafDir, extra)
					insertSummary(extraDir, 1)
					insertChild(leafDir, strings.TrimSuffix(extraDir, "/"))
				}
			}
		}

		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)
		genericTree := db.NewTree(&clickHouseGenericTreeDB{
			d: newClickHouseDatabase(cfg, cp.conn),
		})
		expected, err := genericTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		fastTree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		actual, err := fastTree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)
		So(countingConn.queryCountValue(), ShouldBeLessThan, 20)
	})

	Convey("Where uses maintained non-directory summaries for large default-filter child batches", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/wheregrouped/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/wheregrouped/"

		updatedAt := time.Date(2026, 1, 12, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt).String()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		})
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, db.DGUTAFileTypeBam, 3)
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, db.DGUTAFileTypeDir, 7)
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"child0000/", db.DGUTAFileTypeBam, 2)
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"child0001/", db.DGUTAFileTypeDir, 7)

		batch, err := conn.PrepareBatch(ctx, insertChildrenQuery)
		So(err, ShouldBeNil)

		var (
			appendErr error
			appended  int
		)

		for i := range groupedDirSummaryMinDirs {
			child := fmt.Sprintf("%schild%04d", mountPath, i)

			if appendErr != nil {
				continue
			}

			appendErr = batch.Append(mountPath, sid, mountPath, child)
			if appendErr == nil {
				appended++
			}
		}

		So(appendErr, ShouldBeNil)
		So(appended, ShouldEqual, groupedDirSummaryMinDirs)
		So(batch.Send(), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))

		dcss, err := tree.Where(mountPath, nil, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 1)
		So(dcss[0].Dir, ShouldEqual, mountPath)
		So(dcss[0].Count, ShouldEqual, 3)
		So(dcss[0].FT, ShouldEqual, db.DGUTAFileTypeBam)
		So(dcss[0].Modtime, ShouldResemble, updatedAt)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})

	Convey("Where preloads selective mount summaries for arbitrary-filter traversal", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/wherevector/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/wherevector/"

		updatedAt := time.Date(2026, 1, 12, 9, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt).String()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		insertGUTA := func(dir string, gid uint32, count uint64) {
			So(conn.Exec(ctx,
				testInsertDGUTAStmt,
				mountPath,
				sid,
				dir,
				gid,
				uint32(9),
				uint16(db.DGUTAFileTypeBam),
				uint8(db.DGUTAgeAll),
				count,
				count*10,
				int64(10),
				int64(20),
				[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
			), ShouldBeNil)
		}
		insertChild := func(parent, child string) {
			So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, parent, child), ShouldBeNil)
		}

		insertGUTA(mountPath, 7, 10)
		insertGUTA(mountPath+"a/", 7, 10)
		insertGUTA(mountPath+"a/leaf1/", 7, 6)
		insertGUTA(mountPath+"a/leaf2/", 7, 4)
		insertGUTA(mountPath+"b/", 8, 2)
		insertChild(mountPath, mountPath+"a")
		insertChild(mountPath, mountPath+"b")
		insertChild(mountPath+"a/", mountPath+"a/leaf1")
		insertChild(mountPath+"a/", mountPath+"a/leaf2")
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		}})
		tree := db.NewTree(newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot))
		filter := &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{9}, Age: db.DGUTAgeAll}

		dcss, err := tree.Where(mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 1)
		So(dcss[0].Dir, ShouldEqual, mountPath+"a")
		So(dcss[0].Count, ShouldEqual, 10)
		So(dcss[0].Size, ShouldEqual, 100)
		So(dcss[0].Modtime, ShouldResemble, updatedAt)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.allMountChildQueryCountValue(), ShouldEqual, 0)
		So(countingConn.mountDirVectorQueryCount(), ShouldEqual, 0)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.childBatchQueryCountValue(), ShouldEqual, 1)
	})

	Convey("Where keeps fact-vector summary batches for small active-mount child batches", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/whereraw/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/whereraw/"

		updatedAt := time.Date(2026, 1, 12, 10, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt).String()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertMountDirProjectionSetForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		})
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath, db.DGUTAFileTypeBam, 3)
		insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", db.DGUTAFileTypeBam, 3)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"a"), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid,
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))

		dcss, err := tree.Where(mountPath, nil, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dcss, ShouldHaveLength, 1)
		So(dcss[0].Count, ShouldEqual, 3)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.factVectorBatchQueryCount(), ShouldEqual, 0)
	})
}

func (c *whereQueryCountingConn) summaryBatchQueryCountValue() int {
	return int(c.summaryBatchQueries.Load())
}

type b3WhereDirFilterAllEnv struct {
	cfg        Config
	conn       ch.Conn
	provider   interface{ Close() error }
	snapshot   *activeMountsSnapshot
	projectDir string
}

type b3WhereRow struct {
	dir   string
	age   db.DirGUTAge
	gid   uint32
	uid   uint32
	ft    db.DirGUTAFileType
	count uint64
	size  uint64
}

type b3DigestSummary struct {
	Dir   string   `json:"dir"`
	Count uint64   `json:"count"`
	Size  uint64   `json:"size"`
	UIDs  []uint32 `json:"uids"`
	GIDs  []uint32 `json:"gids"`
	FT    uint16   `json:"ft"`
	Age   uint8    `json:"age"`
}

type e2T283OrderEnv struct {
	cfg  Config
	conn ch.Conn
}

type e2DigestSummary struct {
	Dir   string `json:"dir"`
	Count uint64 `json:"count"`
	Size  uint64 `json:"size"`
	FT    uint16 `json:"ft"`
	Age   uint8  `json:"age"`
}

type ancestorSummaryQueryCountingConn struct {
	ch.Conn

	ancestorFactVectorQueries atomic.Int32
	treeSummaryQueries        atomic.Int32
}

func (c *ancestorSummaryQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if isAncestorFactVectorQuery(query) {
		c.ancestorFactVectorQueries.Add(1)
	}

	if isTreeSummaryQuery(query) {
		c.treeSummaryQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isAncestorFactVectorQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts d") &&
		strings.Contains(query, "arrayJoin") &&
		strings.Contains(query, "WHERE d.dir = ?")
}

func isTreeSummaryQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_virtual_summary_cache") ||
		strings.Contains(query, "JOIN wrstat_virtual_summary_cache") ||
		strings.Contains(query, "FROM wrstat_virtual_summary_sets")
}

func (c *ancestorSummaryQueryCountingConn) ancestorFactVectorQueryCount() int {
	return int(c.ancestorFactVectorQueries.Load())
}

func (c *ancestorSummaryQueryCountingConn) treeSummaryQueryCount() int {
	return int(c.treeSummaryQueries.Load())
}

func insertB2Fact(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dir string,
	ft db.DirGUTAFileType,
	count uint64,
) {
	insertActivePrefixB1Fact(ctx, conn, mount, dir, ft, count)
}
