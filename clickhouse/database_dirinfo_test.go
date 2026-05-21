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

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

func TestClickHouseDatabaseDirInfo(t *testing.T) {
	Convey("DirInfo returns a summary from wrstat_dguta for active snapshot", t, func() {
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

		dbch := newClickHouseDatabase(cfg, cp.conn)
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
		insertGUTA(mountPath+"b/", 7, 2)
		insertGUTA(mountPath+"a/grand/", 7, 1)
		insertGUTA(mountPath+"b/grand/", 8, 1)

		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountPath, sid, mountPath+"a/", mountPath+"a/grand",
		), ShouldBeNil)
		So(conn.Exec(ctx, testInsertChildrenStmt,
			mountPath, sid, mountPath+"b/", mountPath+"b/grand",
		), ShouldBeNil)

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

		So(conn.Exec(ctx, testInsertMountStmt,
			mountB, time.Now(), sidB, updatedB,
		), ShouldBeNil)

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
