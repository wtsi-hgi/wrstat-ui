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

func TestClickHouseDatabaseTreeCache(t *testing.T) {
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

	Convey("Tree.Where reuses cached rows on repeated traversals", t, func() {
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
		So(warmQueries, ShouldBeGreaterThan, 0)
		So(countingConn.subtreeQueryCountValue(), ShouldEqual, 0)

		actual, err := tree.Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.queryCountValue(), ShouldEqual, warmQueries)
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
		insertGUTA(sid1.String(), mountPath, 1)
		insertGUTA(sid1.String(), mountPath+"a/", 1)
		insertChild(sid1.String(), mountPath, mountPath+"a")

		countingConn := &whereQueryCountingConn{Conn: cp.conn}
		tree := db.NewTree(newClickHouseDatabase(cfg, countingConn))
		filter := &db.Filter{Age: db.DGUTAgeAll}

		first, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(first.Current.Count, ShouldEqual, 1)
		So(first.Children, ShouldHaveLength, 1)
		So(first.Children[0].Dir, ShouldEqual, mountPath+"a")

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, updated2, sid2, updated2), ShouldBeNil)
		insertGUTA(sid2.String(), mountPath, 2)
		insertGUTA(sid2.String(), mountPath+"b/", 2)
		insertChild(sid2.String(), mountPath, mountPath+"b")

		second, err := tree.DirInfo(mountPath, filter)
		So(err, ShouldBeNil)
		So(second.Current.Count, ShouldEqual, 2)
		So(second.Children, ShouldHaveLength, 1)
		So(second.Children[0].Dir, ShouldEqual, mountPath+"b")
	})
}

func TestClickHouseDatabaseDirsHaveChildrenFastPath(t *testing.T) {
	Convey("DirsHaveChildren skips the existence join when requested parents have no child rows", t, func() {
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
		So(countingConn.childSummaryBatchQueryCount(), ShouldEqual, 1)
		So(countingConn.existenceQueryCount(), ShouldEqual, 0)
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
		So(countingConn.existenceQueryCount(), ShouldEqual, 4)
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

type hasChildrenQueryCountingConn struct {
	ch.Conn

	childBatchQueries        atomic.Int32
	childSummaryBatchQueries atomic.Int32
	existenceQueries         atomic.Int32
}

func (c *hasChildrenQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	isChildBatchQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
	if isChildBatchQuery {
		c.childBatchQueries.Add(1)
	}

	isChildSummaryBatchQuery := strings.Contains(query, "SELECT dir, gid, uid, ft, age, count, size") &&
		strings.Contains(query, "WHERE dir IN (")
	if isChildSummaryBatchQuery {
		c.childSummaryBatchQueries.Add(1)
	}

	isExistenceQuery := strings.Contains(query, "INNER JOIN wrstat_dguta d") &&
		strings.Contains(query, "GROUP BY c.parent_dir")
	if isExistenceQuery {
		c.existenceQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
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

type whereQueryCountingConn struct {
	ch.Conn

	queries        atomic.Int32
	subtreeQueries atomic.Int32
}

func (c *whereQueryCountingConn) subtreeQueryCountValue() int {
	return int(c.subtreeQueries.Load())
}

func (c *whereQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries.Add(1)

	isSubtreeQuery := strings.Contains(query, "startsWith(dir, ?)") ||
		strings.Contains(query, "startsWith(d.dir, ?)")
	if isSubtreeQuery {
		c.subtreeQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *whereQueryCountingConn) queryCountValue() int {
	return int(c.queries.Load())
}

func TestClickHouseDatabaseWhereFastPath(t *testing.T) {
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
		So(countingConn.queryCountValue(), ShouldBeLessThanOrEqualTo, 3)
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
		So(countingConn.queryCountValue(), ShouldEqual, 1)
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
}
