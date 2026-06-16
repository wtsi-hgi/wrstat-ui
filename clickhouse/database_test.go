//go:build legacy_parent_facts
// +build legacy_parent_facts

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
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const createVirtualSummaryCacheForTest = "CREATE TABLE wrstat_virtual_summary_cache (" +
	"active_set_id String, dir String, updated_at DateTime, all_count UInt64, all_size UInt64, " +
	"all_atime_min Int64, all_mtime_max Int64, all_atime_buckets Array(UInt64), " +
	"all_mtime_buckets Array(UInt64), all_uids Array(UInt32), all_gids Array(UInt32), all_ft UInt16, " +
	"gids Array(UInt32), uids Array(UInt32), fts Array(UInt16), ages Array(UInt8), counts Array(UInt64), " +
	"sizes Array(UInt64), atime_mins Array(Int64), mtime_maxs Array(Int64), " +
	"atime_buckets Array(Array(UInt64)), mtime_buckets Array(Array(UInt64)), child_count UInt64, " +
	"refreshed_at DateTime64(3)) ENGINE = MergeTree PARTITION BY active_set_id ORDER BY (active_set_id, dir)"

const insertVirtualSummaryCacheForTest = "INSERT INTO wrstat_virtual_summary_cache " +
	"(active_set_id, dir, updated_at, all_count, all_size, all_atime_min, all_mtime_max, " +
	"all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, gids, uids, fts, ages, counts, " +
	"sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, child_count, refreshed_at) " +
	"VALUES (?, ?, now(), ?, 9990, 1, 2, [999], [999], [99], [99], 1, [99], [99], [1], [0], [999], " +
	"[9990], [1], [2], [[999]], [[999]], 0, now())"

const (
	c3LustreChild     = "/lustre"
	c3NFSChild        = "/nfs"
	c3ReplacedRoot    = "/lustre/replaced/"
	c3Scratch120Mount = "/lustre/scratch120/"
	c3Scratch120Child = "/lustre/scratch120"
	c3Scratch127Mount = "/lustre/scratch127/"
	c3Scratch127Child = "/lustre/scratch127"
)

func TestClickHouseDatabaseChildren(t *testing.T) {
	Convey("Children returns sorted, distinct children for active snapshot", t, func() {
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

		db := newClickHouseDatabase(cfg, cp.conn)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/test/"

		parentDir := mountPath
		childA := mountPath + "a"
		childB := mountPath + "b"
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

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			sid,
			parentDir,
			childB,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			sid,
			parentDir,
			childA,
		), ShouldBeNil)

		// duplicate row should be de-duped
		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			sid,
			parentDir,
			childA,
		), ShouldBeNil)

		children, err := db.Children("/mnt/test")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{"/mnt/test/a", "/mnt/test/b"})
	})
}

func TestClickHouseDatabaseChildrenAncestor(t *testing.T) {
	Convey("Children merges across mounts for ancestor dirs", t, func() {
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

		db := newClickHouseDatabase(cfg, cp.conn)

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

		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountA, sidA, "/lustre/", "/lustre/scratchA",
		), ShouldBeNil)

		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountB, sidB, "/lustre/", "/lustre/scratchB",
		), ShouldBeNil)

		children, err := db.Children("/lustre")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{
			"/lustre/scratchA", "/lustre/scratchB",
		})

		Convey("returns nil for non-existent ancestor", func() {
			ch, err := db.Children("/nonexistent")
			So(err, ShouldBeNil)
			So(ch, ShouldBeNil)
		})
	})
}

func TestClickHouseDatabaseChildrenSingleMountScope(t *testing.T) {
	Convey("Children does not merge child mounts when a configured parent mount has no active snapshot", t, func() {
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

		children, err := dbch.Children(parentMount)
		So(err, ShouldBeNil)
		So(children, ShouldBeNil)
	})
}

type c3FactSeed struct {
	gid          uint32
	uid          uint32
	ft           db.DirGUTAFileType
	count        uint64
	size         uint64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func TestClickHouseDatabaseVirtualActiveAncestorsC3(t *testing.T) {
	Convey("C3.1 virtual children refresh stores active ancestor edges", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{
			{mountPath: c3Scratch120Mount, updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)},
			{mountPath: c3Scratch127Mount, updatedAt: time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)},
			{mountPath: testT283ImagingMountPath, updatedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)},
		})
		defer cleanup()

		So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)

		rootChildren, err := env.db.Children("/")
		So(err, ShouldBeNil)
		So(rootChildren, ShouldResemble, []string{c3LustreChild, c3NFSChild})

		lustreChildren, err := env.db.Children("/lustre/")
		So(err, ShouldBeNil)
		So(lustreChildren, ShouldResemble, []string{c3Scratch120Child, c3Scratch127Child})
	})

	Convey("C3.1 virtual child reads use the full active set fingerprint", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{
			{mountPath: c3Scratch120Mount, updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)},
			{mountPath: c3Scratch127Mount, updatedAt: time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)},
			{mountPath: testT283ImagingMountPath, updatedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)},
		})
		defer cleanup()

		fullSetID := env.activeSetID()
		partialSetID := fingerprintForMountsActive(env.rows[:2])
		So(partialSetID, ShouldNotEqual, fullSetID)
		So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)

		lustreChildren, err := env.db.Children("/lustre/")
		So(err, ShouldBeNil)
		So(lustreChildren, ShouldResemble, []string{c3Scratch120Child, c3Scratch127Child})
		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children_sets WHERE active_set_id = ?", fullSetID,
		), ShouldEqual, 1)
		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children_sets WHERE active_set_id = ?", partialSetID,
		), ShouldEqual, 0)
		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children WHERE active_set_id = ?", partialSetID,
		), ShouldEqual, 0)
	})

	Convey("C3.1 virtual child reads expose only ready active branches from a mixed active set", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{
			{
				mountPath: c3Scratch120Mount,
				updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
				facts: []c3FactSeed{{
					gid: 7, uid: 9, ft: db.DGUTAFileTypeBam, count: 2, size: 20,
					atimeBuckets: []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
					mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
				}},
			},
			{
				mountPath:      testT283ImagingMountPath,
				updatedAt:      time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC),
				skipFactsReady: true,
				facts: []c3FactSeed{{
					gid: 8, uid: 10, ft: db.DGUTAFileTypeCram, count: 9, size: 90,
					atimeBuckets: []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
					mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
				}},
			},
		})
		defer cleanup()

		So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)
		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children_sets WHERE active_set_id = ?", env.activeSetID(),
		), ShouldEqual, 1)

		children, err := env.db.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{c3LustreChild})

		di, err := db.NewTree(env.db).DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 2)
		So(di.Children, ShouldHaveLength, 1)
		So(di.Children[0].Dir, ShouldEqual, c3LustreChild)
		So(di.Children[0].Count, ShouldEqual, 2)
	})

	Convey("C3.2 DirInfo composes virtual ancestors from active mount-root facts", t, func() {
		env, _, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{
			{
				mountPath: c3Scratch120Mount,
				updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
				facts: []c3FactSeed{{
					gid: 7, uid: 9, ft: db.DGUTAFileTypeBam, count: 2, size: 20,
					atimeBuckets: []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
					mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 2},
				}},
			},
			{
				mountPath: c3Scratch127Mount,
				updatedAt: time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC),
				facts: []c3FactSeed{{
					gid: 8, uid: 10, ft: db.DGUTAFileTypeCram, count: 3, size: 30,
					atimeBuckets: []uint64{0, 4, 0, 0, 0, 0, 0, 0, 0},
					mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 3, 0},
				}},
			},
			{
				mountPath: testT283ImagingMountPath,
				updatedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
				facts: []c3FactSeed{{
					gid: 7, uid: 11, ft: db.DGUTAFileTypeText, count: 5, size: 50,
					atimeBuckets: []uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
					mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
				}},
			},
		})
		defer cleanup()

		sum, err := env.db.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 10)
		So(sum.Size, ShouldEqual, 100)
		So(sum.UIDs, ShouldResemble, []uint32{9, 10, 11})
		So(sum.GIDs, ShouldResemble, []uint32{7, 8})
		So(sum.FT, ShouldEqual, db.DGUTAFileTypeBam|db.DGUTAFileTypeCram|db.DGUTAFileTypeText)
		So(sum.CommonATime, ShouldEqual, summary.Range5Years)
		So(sum.CommonMTime, ShouldEqual, summary.RangeLess1Month)
		So(sum.Modtime, ShouldResemble, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	})

	Convey("C3.4 virtual summary cache rows without readiness are ignored", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{{
			mountPath: "/lustre/cache/",
			updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
			facts: []c3FactSeed{{
				gid: 7, uid: 9, ft: db.DGUTAFileTypeBam, count: 3, size: 30,
				atimeBuckets: []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
			}},
		}})
		defer cleanup()

		So(env.conn.Exec(ctx, createVirtualSummaryCacheForTest), ShouldBeNil)
		So(env.conn.Exec(ctx, insertVirtualSummaryCacheForTest, env.activeSetID(), "/", uint64(999)), ShouldBeNil)

		sum, err := env.db.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 3)
		So(sum.Size, ShouldEqual, 30)
	})

	Convey("C3.5 hot virtual ancestor reads do not use FINAL", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{{
			mountPath: "/lustre/hot/",
			updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
			facts: []c3FactSeed{{
				gid: 7, uid: 9, ft: db.DGUTAFileTypeBam, count: 3, size: 30,
				atimeBuckets: []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
			}},
		}})
		defer cleanup()

		So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)

		spy := &treeRouteQuerySpyConn{Conn: env.conn}
		dbch := newClickHouseDatabase(env.cfg, spy)

		_, err := dbch.Children("/")
		So(err, ShouldBeNil)
		_, err = dbch.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)

		for _, query := range spy.queries {
			So(strings.ToUpper(query), ShouldNotContainSubstring, " FINAL")
		}
	})

	Convey("C3.6 old active-set cleanup preserves the replacement active set", t, func() {
		env, ctx, cleanup := newC3VirtualAncestorEnv(t, []c3MountSeed{{
			mountPath: c3ReplacedRoot,
			updatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
			facts: []c3FactSeed{{
				gid: 7, uid: 9, ft: db.DGUTAFileTypeBam, count: 1, size: 10,
				atimeBuckets: []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				mtimeBuckets: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 1},
			}},
		}})
		defer cleanup()

		oldSetID := env.activeSetID()
		So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)

		newUpdatedAt := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
		newSID := snapshotID(c3ReplacedRoot, newUpdatedAt)
		So(env.conn.Exec(ctx, testInsertDGUTAStmt, c3ReplacedRoot, newSID.String(), c3ReplacedRoot,
			uint32(8), uint32(10), uint16(db.DGUTAFileTypeCram), uint8(db.DGUTAgeAll), uint64(9), uint64(90),
			int64(10), int64(20), []uint64{0, 2, 0, 0, 0, 0, 0, 0, 0}, []uint64{0, 0, 0, 0, 0, 0, 0, 0, 2},
		), ShouldBeNil)
		So(writeMaintainedMountDirProjectionForTest(ctx, env.conn, activeMount{
			mountPath: c3ReplacedRoot, snapshotID: newSID.String(), updatedAt: newUpdatedAt,
		}), ShouldBeNil)
		So(env.conn.Exec(
			ctx, testInsertMountStmt, c3ReplacedRoot, time.Now().UTC(), newSID, newUpdatedAt,
		), ShouldBeNil)

		rows, err := queryMountsActiveRows(ctx, env.conn)
		So(err, ShouldBeNil)

		newSetID := fingerprintForMountsActive(rows)
		So(newSetID, ShouldNotEqual, oldSetID)
		So(refreshActiveVirtualChildren(ctx, env.conn, rows), ShouldBeNil)
		So(cleanupOldVirtualChildrenSets(ctx, env.conn, newSetID), ShouldBeNil)

		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children WHERE active_set_id = ?", oldSetID,
		), ShouldEqual, 0)
		So(countRows(
			ctx, env.conn, "SELECT count() FROM wrstat_virtual_children_sets WHERE active_set_id = ?", newSetID,
		), ShouldEqual, 1)

		children, err := env.db.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{c3LustreChild})

		sum, err := env.db.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, 9)
		So(sum.Size, ShouldEqual, 90)
	})
}

func newC3VirtualAncestorEnv(
	t *testing.T,
	seeds []c3MountSeed,
) (*c3VirtualAncestorEnv, context.Context, func()) {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 2 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{"/"}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	conn := th.openConn(cfg.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	env := &c3VirtualAncestorEnv{cfg: cfg, conn: conn}
	env.seed(ctx, seeds)
	env.db = newClickHouseDatabase(cfg, conn)

	return env, ctx, func() {
		cancel()
		So(conn.Close(), ShouldBeNil)
		So(p.Close(), ShouldBeNil)
		os.Unsetenv("WRSTAT_ENV")
	}
}

type c3MountSeed struct {
	mountPath      string
	updatedAt      time.Time
	facts          []c3FactSeed
	skipFactsReady bool
}

type c3VirtualAncestorEnv struct {
	cfg  Config
	conn ch.Conn
	db   *clickHouseDatabase
	rows []mountsActiveRow
}

func (e *c3VirtualAncestorEnv) seed(ctx context.Context, seeds []c3MountSeed) {
	e.rows = make([]mountsActiveRow, 0, len(seeds))

	for _, seed := range seeds {
		sid := snapshotID(seed.mountPath, seed.updatedAt)
		So(e.conn.Exec(ctx, testInsertMountStmt, seed.mountPath, time.Now().UTC(), sid, seed.updatedAt), ShouldBeNil)

		for _, fact := range seed.facts {
			So(e.conn.Exec(ctx, testInsertDGUTAStmt, seed.mountPath, sid.String(), seed.mountPath,
				fact.gid, fact.uid, uint16(fact.ft), uint8(db.DGUTAgeAll), fact.count, fact.size,
				int64(10), int64(20), fact.atimeBuckets, fact.mtimeBuckets,
			), ShouldBeNil)
		}

		if !seed.skipFactsReady {
			So(writeMaintainedMountDirProjectionForTest(ctx, e.conn, activeMount{
				mountPath: seed.mountPath, snapshotID: sid.String(), updatedAt: seed.updatedAt,
			}), ShouldBeNil)
		}

		e.rows = append(e.rows, mountsActiveRow{
			mountPath: seed.mountPath, snapshotID: sid.String(), updatedAt: seed.updatedAt,
		})
	}
}

func (e *c3VirtualAncestorEnv) activeSetID() string {
	return fingerprintForMountsActive(e.rows)
}
