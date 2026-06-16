//go:build legacy_parent_facts && legacy_parent_facts_deleted_api_tests && !legacy_parent_facts_deleted_api_tests
// +build legacy_parent_facts,legacy_parent_facts_deleted_api_tests,!legacy_parent_facts_deleted_api_tests

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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const testInsertInfoFactVectorStmt = "INSERT INTO wrstat_dir_facts " +
	"(mount_path, snapshot_id, dir, updated_at, gids, uids, fts, ages, counts, sizes, " +
	"atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) " +
	"VALUES (?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"

const testInsertRollbackMountStmt = "INSERT INTO wrstat_mount_events " +
	"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
	"VALUES (?, ?, 1, ?, ?, 'rollback')"

type infoFactFixture struct {
	dir       string
	vectorLen int
}

func TestClickHouseDatabaseInfo(t *testing.T) {
	Convey("D1.1 Info counts active canonical facts and children", t, func() {
		const mountPath = "/mnt/test/"

		env, ctx := newInfoTestEnv(t)

		insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			d1ActiveFacts(mountPath),
			d1ActiveChildren(mountPath),
		)

		info, err := env.dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 3, 6, 1, 2)
	})

	Convey("D1.2 Info ignores stale snapshots and derived table rows", t, func() {
		const mountPath = "/mnt/stale-derived/"

		env, ctx := newInfoTestEnv(t)

		staleUpdatedAt := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
		activeUpdatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		staleSID := insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			staleUpdatedAt,
			staleInfoFacts(mountPath, 99),
			nil,
		)
		activeSID := insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			activeUpdatedAt,
			d1ActiveFacts(mountPath),
			d1ActiveChildren(mountPath),
		)

		insertInfoDerivedRows(ctx, env.conn, mountPath, staleSID, staleUpdatedAt)
		insertInfoDerivedRows(ctx, env.conn, mountPath, activeSID, activeUpdatedAt)

		info, err := env.dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 3, 6, 1, 2)
	})

	Convey("D1.3 Info on a snapshot-scoped database uses the pinned snapshot", t, func() {
		const mountPath = "/mnt/pinned-info/"

		env, ctx := newInfoTestEnv(t)

		pinnedUpdatedAt := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
		activeUpdatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		pinnedSID := insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			pinnedUpdatedAt,
			[]infoFactFixture{{dir: mountPath, vectorLen: 4}},
			[]infoChildFixture{{parent: mountPath, child: mountPath + "pinned/"}},
		)
		insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			activeUpdatedAt,
			d1ActiveFacts(mountPath),
			d1ActiveChildren(mountPath),
		)

		snapshot := newActiveMountsSnapshot([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: pinnedSID,
			updatedAt:  pinnedUpdatedAt,
		}})
		pinnedDB := newClickHouseDatabaseWithSnapshot(env.dbch.cfg, env.conn, snapshot)

		info, err := pinnedDB.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 1, 4, 1, 1)
	})

	Convey("D1.4 Info follows publish and rollback active snapshot changes", t, func() {
		const mountPath = "/mnt/publish-rollback/"

		env, ctx := newInfoTestEnv(t)

		activeUpdatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		replacementUpdatedAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
		activeSID := insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			activeUpdatedAt,
			d1ActiveFacts(mountPath),
			d1ActiveChildren(mountPath),
		)

		info, err := env.dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 3, 6, 1, 2)

		replacementSID := snapshotID(mountPath, replacementUpdatedAt).String()
		insertInfoSnapshotRows(
			ctx,
			env.conn,
			mountPath,
			replacementSID,
			[]infoFactFixture{{dir: mountPath, vectorLen: 2}},
			nil,
		)
		So(env.conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			replacementUpdatedAt,
			replacementSID,
			replacementUpdatedAt,
		), ShouldBeNil)

		info, err = env.dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 1, 2, 0, 0)

		So(env.conn.Exec(
			ctx,
			testInsertRollbackMountStmt,
			mountPath,
			replacementUpdatedAt.Add(time.Second),
			activeSID,
			activeUpdatedAt,
		), ShouldBeNil)

		info, err = env.dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 3, 6, 1, 2)
	})

	Convey("D1.5 Info query routing reads only canonical facts and children counts", t, func() {
		const mountPath = "/mnt/info-routes/"

		env, ctx := newInfoTestEnv(t)

		sid := insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			d1ActiveFacts(mountPath),
			d1ActiveChildren(mountPath),
		)
		insertInfoDerivedRows(
			ctx,
			env.conn,
			mountPath,
			sid,
			time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		)

		spy := &treeRouteQuerySpyConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.dbch.cfg, spy)

		info, err := dbch.Info()
		So(err, ShouldBeNil)
		assertInfoCounts(info, 3, 6, 1, 2)
		assertInfoCanonicalCountQueries(spy.queries)
	})

	Convey("Info and tree permission checks avoid legacy source tables", t, func() {
		const mountPath = "/mnt/c4-spy/"

		env, ctx := newInfoTestEnv(t)

		dir := mountPath + "a/"
		updatedAt := time.Date(2026, 6, 1, 16, 0, 0, 0, time.UTC)
		insertInfoCanonicalSnapshot(
			ctx,
			env.conn,
			mountPath,
			updatedAt,
			[]infoFactFixture{{dir: dir, vectorLen: 1}},
			nil,
		)

		spy := &treeRouteQuerySpyConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.dbch.cfg, spy)

		_, err := dbch.Info()
		So(err, ShouldBeNil)

		cfg := env.dbch.cfg
		cfg.MountPoints = []string{mountPath}
		mountPoints, err := mountPointsFromConfig(cfg)
		So(err, ShouldBeNil)

		client := &Client{cfg: cfg, conn: spy, mountPoints: mountPoints}
		ok, err := client.PermissionAnyInDir(ctx, dir, 11, []uint32{9})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		assertNoLegacyTreeRouteTables(spy.queries)
	})
}

func newInfoTestEnv(t *testing.T) (*infoTestEnv, context.Context) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	Reset(func() { os.Unsetenv("WRSTAT_ENV") })

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)
	Reset(func() { So(p.Close(), ShouldBeNil) })

	cp, ok := p.(*chProvider)
	So(ok, ShouldBeTrue)

	conn := th.openConn(cfg.DSN)

	Reset(func() { So(conn.Close(), ShouldBeNil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	Reset(cancel)

	return &infoTestEnv{
		conn: conn,
		dbch: newClickHouseDatabase(cfg, cp.conn),
	}, ctx
}

func d1ActiveChildren(mountPath string) []infoChildFixture {
	return []infoChildFixture{
		{parent: mountPath, child: mountPath + "a/"},
		{parent: mountPath, child: mountPath + "b/"},
	}
}

func assertInfoCounts(info *db.Info, dirs, dgutas, parents, children int) {
	So(info, ShouldNotBeNil)
	So(info.NumDirs, ShouldEqual, dirs)
	So(info.NumDGUTAs, ShouldEqual, dgutas)
	So(info.NumParents, ShouldEqual, parents)
	So(info.NumChildren, ShouldEqual, children)
}

func insertInfoDerivedRows(ctx context.Context, conn ch.Conn, mountPath, sid string, updatedAt time.Time) {
	const activeSetID = "info-derived-active-set"

	So(conn.Exec(
		ctx,
		insertDirFilterAgeAllQuery,
		mountPath,
		sid,
		uint32(7),
		uint32(9),
		uint16(db.DGUTAFileTypeBam),
		mountPath,
		uint64(99),
		uint64(123),
		int64(100),
		int64(200),
		[]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
		updatedAt,
	), ShouldBeNil)
	insertInfoDerivedParentFact(ctx, conn, mountPath, sid, updatedAt)
	So(conn.Exec(
		ctx,
		insertActivePrefixRollupQuery,
		activeSetID,
		mountPath,
		updatedAt,
		uint64(99),
		uint64(123),
		int64(100),
		int64(200),
		[]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
		[]uint32{9},
		[]uint32{7},
		uint16(db.DGUTAFileTypeBam),
		uint64(99),
		uint64(123),
		uint64(9),
		updatedAt,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertVirtualChildQuery,
		activeSetID,
		"/mnt/",
		mountPath,
		uint8(1),
		mountPath,
		updatedAt,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertVirtualChildrenSetQuery,
		activeSetID,
		uint64(1),
		updatedAt,
	), ShouldBeNil)
}

func insertInfoDerivedParentFact(ctx context.Context, conn ch.Conn, mountPath, sid string, updatedAt time.Time) {
	metrics := infoDerivedMetrics()
	identity := infoDerivedIdentity()

	args := make([]any, 0, 36)
	args = append(args,
		mountPath,
		sid,
		"/mnt/",
		mountPath,
		updatedAt,
	)
	args = append(args, metrics.summaryArgs()...)
	args = append(args, identity.scalarArgs()...)
	args = append(args, metrics.summaryArgs()...)
	args = append(args, identity.scalarArgs()...)
	args = append(args, identity.vectorArgs()...)
	args = append(args, metrics.vectorArgs()...)
	args = append(args, uint64(9), uint8(1), updatedAt)

	So(conn.Exec(ctx, insertParentFactsQuery, args...), ShouldBeNil)
}

func infoDerivedMetrics() infoDerivedMetricFixture {
	return infoDerivedMetricFixture{
		count:        99,
		size:         123,
		atimeMin:     100,
		mtimeMax:     200,
		atimeBuckets: []uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		mtimeBuckets: []uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
	}
}

func infoDerivedIdentity() infoDerivedIdentityFixture {
	return infoDerivedIdentityFixture{
		gid: 7,
		uid: 9,
		ft:  uint16(db.DGUTAFileTypeBam),
		age: uint8(db.DGUTAgeAll),
	}
}

func assertInfoCanonicalCountQueries(queries []string) {
	So(queries, ShouldHaveLength, 2)

	var factQueries, childQueries int

	for _, query := range queries {
		lower := strings.ToLower(query)
		assertInfoQueryAvoidsDerivedTables(lower)

		if strings.Contains(lower, "from wrstat_dir_facts") {
			factQueries++

			So(lower, ShouldContainSubstring, "sum(length(gids))")
		}

		if strings.Contains(lower, "from wrstat_children") {
			childQueries++

			So(lower, ShouldContainSubstring, "uniqexact(parent_dir)")
		}
	}

	So(factQueries, ShouldEqual, 1)
	So(childQueries, ShouldEqual, 1)
}

func assertInfoQueryAvoidsDerivedTables(query string) {
	for _, table := range infoDerivedTableNames() {
		So(query, ShouldNotContainSubstring, table)
	}
}

func infoDerivedTableNames() []string {
	prefix := infoDerivedTableName("active", "prefix")
	virtualChildren := infoDerivedTableName("virtual", infoChildrenTablePart())
	virtualSummary := infoDerivedTableName("virtual", "summary")

	return []string{
		dirFilterAgeAllTableName,
		string(NavigationObjectParentFacts),
		prefix + "_rollups",
		prefix + "_filter_ageall",
		prefix + "_rollup_sets",
		virtualChildren,
		virtualChildren + "_sets",
		virtualSummary + "_cache",
		virtualSummary + "_sets",
	}
}

func infoDerivedTableName(parts ...string) string {
	return testDatabaseName + "_" + strings.Join(parts, "_")
}

func infoChildrenTablePart() string {
	return strings.TrimSuffix("children_", "_")
}

func insertInfoCanonicalSnapshot(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	updatedAt time.Time,
	facts []infoFactFixture,
	children []infoChildFixture,
) string {
	sid := snapshotID(mountPath, updatedAt).String()
	insertInfoSnapshotRows(ctx, conn, mountPath, sid, facts, children)
	So(conn.Exec(ctx, testInsertMountStmt, mountPath, updatedAt, sid, updatedAt), ShouldBeNil)

	return sid
}

func d1ActiveFacts(mountPath string) []infoFactFixture {
	return []infoFactFixture{
		{dir: mountPath, vectorLen: 2},
		{dir: mountPath + "a/", vectorLen: 1},
		{dir: mountPath + "b/", vectorLen: 3},
	}
}

func staleInfoFacts(mountPath string, n int) []infoFactFixture {
	facts := make([]infoFactFixture, 0, n)
	for i := range n {
		facts = append(facts, infoFactFixture{
			dir:       fmt.Sprintf("%sstale-%03d/", mountPath, i),
			vectorLen: 1,
		})
	}

	return facts
}

func insertInfoSnapshotRows(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	facts []infoFactFixture,
	children []infoChildFixture,
) {
	for _, fact := range facts {
		insertInfoFactVector(ctx, conn, mountPath, sid, fact.dir, fact.vectorLen)
	}

	for _, child := range children {
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, child.parent, child.child), ShouldBeNil)
	}
}

func insertInfoFactVector(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID, dir string,
	vectorLen int,
) {
	gids := make([]uint32, vectorLen)
	uids := make([]uint32, vectorLen)
	fts := make([]uint16, vectorLen)
	ages := make([]uint8, vectorLen)
	counts := make([]uint64, vectorLen)
	sizes := make([]uint64, vectorLen)
	atimeMins := make([]int64, vectorLen)
	mtimeMaxs := make([]int64, vectorLen)
	atimeBuckets := make([][]uint64, vectorLen)
	mtimeBuckets := make([][]uint64, vectorLen)

	for i := range vectorLen {
		gids[i] = uint32(7 + i)
		uids[i] = uint32(11 + i)
		fts[i] = uint16(db.DGUTAFileTypeBam)
		ages[i] = uint8(db.DGUTAgeAll)
		counts[i] = 1
		sizes[i] = 10
		atimeMins[i] = 10
		mtimeMaxs[i] = 20
		atimeBuckets[i] = []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets[i] = []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}
	}

	So(conn.Exec(
		ctx,
		testInsertInfoFactVectorStmt,
		mountPath,
		snapshotID,
		dir,
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
	), ShouldBeNil)
}

type infoDerivedMetricFixture struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func (f infoDerivedMetricFixture) summaryArgs() []any {
	return []any{f.count, f.size, f.atimeMin, f.mtimeMax, f.atimeBuckets, f.mtimeBuckets}
}

func (f infoDerivedMetricFixture) vectorArgs() []any {
	return []any{
		[]uint64{f.count},
		[]uint64{f.size},
		[]int64{f.atimeMin},
		[]int64{f.mtimeMax},
		[][]uint64{f.atimeBuckets},
		[][]uint64{f.mtimeBuckets},
	}
}

type infoDerivedIdentityFixture struct {
	gid uint32
	uid uint32
	ft  uint16
	age uint8
}

func (f infoDerivedIdentityFixture) scalarArgs() []any {
	return []any{[]uint32{f.uid}, []uint32{f.gid}, f.ft}
}

func (f infoDerivedIdentityFixture) vectorArgs() []any {
	return []any{[]uint32{f.gid}, []uint32{f.uid}, []uint16{f.ft}, []uint8{f.age}}
}

type infoChildFixture struct {
	parent string
	child  string
}

type infoTestEnv struct {
	conn ch.Conn
	dbch *clickHouseDatabase
}
