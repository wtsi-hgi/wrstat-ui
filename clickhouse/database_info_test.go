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

func TestClickHouseDatabaseInfo(t *testing.T) {
	Convey("Info counts are computed over active snapshots only", t, func() {
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

		const mountPath = "/mnt/test/"

		oldUpdatedAt := time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC)
		newUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		oldSID := snapshotID(mountPath, oldUpdatedAt)
		newSID := snapshotID(mountPath, newUpdatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx,
			testInsertMountStmt,
			mountPath,
			time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC),
			oldSID,
			oldUpdatedAt,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertDGUTAStmt,
			mountPath,
			oldSID,
			mountPath,
			uint32(1),
			uint32(1),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(1),
			uint64(1),
			int64(1),
			int64(1),
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			oldSID,
			mountPath,
			mountPath+"oldchild",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertMountStmt,
			mountPath,
			time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC),
			newSID,
			newUpdatedAt,
		), ShouldBeNil)

		dirA := mountPath
		dirB := mountPath + "a/"

		insertInfoFactVector(ctx, conn, mountPath, newSID.String(), dirA, 2)
		insertInfoFactVector(ctx, conn, mountPath, newSID.String(), dirB, 1)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			newSID,
			dirA,
			mountPath+"a",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			newSID,
			dirA,
			mountPath+"b",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertChildrenStmt,
			mountPath,
			newSID,
			dirB,
			mountPath+"a/c",
		), ShouldBeNil)

		info, err := dbch.Info()
		So(err, ShouldBeNil)
		So(info, ShouldNotBeNil)
		So(info.NumDirs, ShouldEqual, 2)
		So(info.NumDGUTAs, ShouldEqual, 3)
		So(info.NumParents, ShouldEqual, 2)
		So(info.NumChildren, ShouldEqual, 3)
	})

	Convey("Info counts active fact rows, vector entries, and child edges", t, func() {
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

		const mountPath = "/mnt/c4-info/"

		updatedAt := time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		insertInfoFactVector(ctx, conn, mountPath, sid.String(), mountPath, 2)
		insertInfoFactVector(ctx, conn, mountPath, sid.String(), mountPath+"a/", 0)
		insertInfoFactVector(ctx, conn, mountPath, sid.String(), mountPath+"b/", 5)

		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"a"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath, mountPath+"b"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath+"a/", mountPath+"a/c"), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), mountPath+"a/", mountPath+"a/d"), ShouldBeNil)

		info, err := dbch.Info()
		So(err, ShouldBeNil)
		So(info, ShouldNotBeNil)
		So(info.NumDirs, ShouldEqual, 3)
		So(info.NumDGUTAs, ShouldEqual, 7)
		So(info.NumParents, ShouldEqual, 2)
		So(info.NumChildren, ShouldEqual, 4)
	})

	Convey("Info and tree permission checks avoid legacy source tables", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/c4-spy/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c4-spy/"

		dir := mountPath + "a/"
		updatedAt := time.Date(2026, 6, 1, 16, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertInfoFactVector(ctx, conn, mountPath, sid.String(), dir, 1)

		spy := &treeRouteQuerySpyConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, spy)

		_, err = dbch.Info()
		So(err, ShouldBeNil)

		mountPoints, err := mountPointsFromConfig(cfg)
		So(err, ShouldBeNil)

		client := &Client{cfg: cfg, conn: spy, mountPoints: mountPoints}
		ok, err = client.PermissionAnyInDir(ctx, dir, 11, []uint32{9})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		assertNoLegacyTreeRouteTables(spy.queries)
	})
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
