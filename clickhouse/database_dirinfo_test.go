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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
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
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 7, 10, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 8, 11, db.DGUTAFileTypeCram, db.DGUTAgeAll, 5)
		insertC1FactVector(ctx, conn, mountPath, sid.String(), dir, 7, 9, db.DGUTAFileTypeCram, db.DGUTAgeAll, 4)
		So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		dbch := newClickHouseDatabase(cfg, cp.conn)
		sum, err := dbch.DirInfo(dir, &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll})
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
		"wrstat_children":              true,
		"wrstat_dir_facts":             true,
		"wrstat_dir_projection_sets":   true,
		"wrstat_mounts_active":         true,
		"wrstat_virtual_children":      true,
		"wrstat_virtual_children_sets": true,
		"wrstat_virtual_summary_cache": true,
		"wrstat_virtual_summary_sets":  true,
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

func TestClickHouseDatabaseOptionalDirFilterAgeAll(t *testing.T) {
	Convey("C2.1 absent optional AgeAll index keeps whole-mount Where on facts", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-absent/")
		defer cleanup()

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, countingConn))
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		dcss, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dirSummariesByDir(dcss)[env.mount.mountPath].Count, ShouldEqual, 12)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
	})

	Convey("C2.2 absent optional AgeAll index keeps high-fanout DirsHaveChildren on facts vectors", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-absent/")
		defer cleanup()

		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		hasChildren, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{env.parentDir: true})
		So(countingConn.mountVectorQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.filterAgeAllQueryCount(), ShouldEqual, 0)
	})

	Convey("C2.3 ready optional AgeAll index serves whole-mount Where with facts-equivalent summaries", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-ready/")
		defer cleanup()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		referenceTree := db.NewTree(&clickHouseGenericTreeDB{d: newClickHouseDatabase(env.cfg, env.providerConn)})
		expected, err := referenceTree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)

		env.createAndSeedAgeAllIndex()

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, countingConn))
		actual, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldBeGreaterThan, 0)
	})

	Convey("C2.4 ready optional AgeAll index serves high-fanout DirsHaveChildren", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-ready/")
		defer cleanup()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		referenceDB := newClickHouseDatabase(env.cfg, env.providerConn)
		expected, err := referenceDB.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)

		env.createAndSeedAgeAllIndex()

		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		actual, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.filterAgeAllQueryCount(), ShouldBeGreaterThan, 0)
	})

	Convey("C2.5 present optional AgeAll index without readiness falls back to facts for Where", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-where-not-ready/")
		defer cleanup()

		env.createAgeAllIndex()

		countingConn := &whereQueryCountingConn{Conn: env.providerConn}
		tree := db.NewTree(newClickHouseDatabase(env.cfg, countingConn))
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		dcss, err := tree.Where(env.mount.mountPath, filter, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dirSummariesByDir(dcss)[env.mount.mountPath].Count, ShouldEqual, 12)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
	})

	Convey("C2.6 present optional AgeAll index without readiness falls back to facts for DirsHaveChildren", t, func() {
		env, cleanup := newDirFilterAgeAllTestEnv(t, "/mnt/c2-children-not-ready/")
		defer cleanup()

		env.createAgeAllIndex()

		countingConn := &hasChildrenQueryCountingConn{Conn: env.providerConn}
		dbch := newClickHouseDatabase(env.cfg, countingConn)
		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}

		hasChildren, err := dbch.DirsHaveChildren([]string{env.parentDir}, filter)
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{env.parentDir: true})
		So(countingConn.filterAgeAllQueryCount(), ShouldEqual, 0)
	})

	Convey("C2.7 age-specific filters never read the optional AgeAll index", t, func() {
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

	Convey("C2.8 facts-only schema SQL does not include the optional AgeAll index", t, func() {
		stmts, err := schemaSQL()
		So(err, ShouldBeNil)

		for _, stmt := range stmts {
			So(stmt, ShouldNotContainSubstring, "wrstat_dir_filter_ageall")
		}
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

		childrenBatch, err := conn.PrepareBatch(ctx, insertChildrenQuery)
		So(err, ShouldBeNil)

		baseUpdatedAt := time.Date(2026, 1, 12, 11, 0, 0, 0, time.UTC)
		rows := make([]mountsActiveRow, 0, nfsMountCount)

		var nfsCount uint64

		for i := range nfsMountCount {
			count := uint64(i + 10)
			nfsCount += count

			mountPath := fmt.Sprintf("/nfs/project%03d/", i)
			updatedAt := baseUpdatedAt.Add(time.Duration(i) * time.Minute)
			sid := snapshotID(mountPath, updatedAt)

			So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
			insertMountDirProjectionSetForTest(ctx, conn, activeMount{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			})
			appendDisktreeNFSClickGUTA(ctx, conn, mountPath, sid.String(), mountPath, count)
			appendDisktreeNFSClickGUTA(ctx, conn, mountPath, sid.String(), mountPath+"leaf/", 1)
			So(childrenBatch.Append(mountPath, sid.String(), mountPath, mountPath+"leaf"), ShouldBeNil)

			rows = append(rows, mountsActiveRow{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			})
		}

		So(childrenBatch.Send(), ShouldBeNil)

		for _, row := range rows {
			So(writeMaintainedMountDirProjectionForTest(ctx, conn, activeMount(row)), ShouldBeNil)
		}

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

func appendDisktreeNFSClickGUTA(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	dir string,
	count uint64,
) {
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
			nfsAncestor    = "/nfs/"
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
	return strings.Contains(query, "FROM wrstat_dir_filter_ageall") ||
		strings.Contains(query, "JOIN wrstat_dir_filter_ageall")
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

	groupedSummaryQueries  atomic.Int32
	factVectorBatchQueries atomic.Int32
	mountSummaryQueries    atomic.Int32
	mountVectorQueries     atomic.Int32
}

func (c *dirInfoSummaryQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if isGroupedDirInfoSummaryQuery(query) {
		c.groupedSummaryQueries.Add(1)
	}

	if isFactVectorDirInfoBatchQuery(query) {
		c.factVectorBatchQueries.Add(1)
	}

	if isMountDirInfoSummaryQuery(query) {
		c.mountSummaryQueries.Add(1)
	}

	if isMountDirInfoVectorQuery(query) {
		c.mountVectorQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *dirInfoSummaryQueryCountingConn) reset() {
	c.groupedSummaryQueries.Store(0)
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

func (c *dirInfoSummaryQueryCountingConn) mountDirSummaryQueryCount() int {
	return int(c.mountSummaryQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) mountDirVectorQueryCount() int {
	return int(c.mountVectorQueries.Load())
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
		cfg.MountPoints = []string{"/", "/lustre/", "/nfs/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		insertMount := func(mountPath string, updatedAt time.Time, count uint64) mountsActiveRow {
			sid := snapshotID(mountPath, updatedAt)
			So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
			insertMountDirProjectionSetForTest(ctx, conn, activeMount{
				mountPath:  mountPath,
				snapshotID: sid.String(),
				updatedAt:  updatedAt,
			})
			insertWhereSummaryTestGUTA(ctx, conn, mountPath, sid.String(), mountPath, db.DGUTAFileTypeBam, count)

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
