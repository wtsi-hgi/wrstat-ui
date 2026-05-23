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

var errForcedMaintainedSummaryPrefetchFailure = errors.New("forced maintained summary prefetch failure")

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
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 1)

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
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 1)

		defaultSummaries, err := dbch.DirInfos([]string{mountPath + "a"}, &db.Filter{})
		So(err, ShouldBeNil)
		So(defaultSummaries, ShouldResemble, map[string]*db.DirSummary{
			mountPath + "a": expectedSummary(mountPath+"a", &db.Filter{}),
		})
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 1)

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
		So(refreshMountDirSummaries(ctx, conn, activeMount{
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
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		defaultSummaries, err := dbch.DirInfos([]string{mountPath + "a"}, &db.Filter{})
		So(err, ShouldBeNil)
		So(defaultSummaries[mountPath+"a"].Count, ShouldEqual, 7)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 0)

		countingConn.reset()

		emptyAgeSummary, err := dbch.DirInfo(mountPath+"a/", &db.Filter{Age: db.DGUTAgeA1M})
		So(err, ShouldBeNil)
		So(emptyAgeSummary, ShouldBeNil)
		So(countingConn.rawSummaryQueryCount(), ShouldEqual, 1)
	})

	Convey("DirInfos falls back for UID and GID filters even when mount summaries exist", t, func() {
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
		So(refreshMountDirSummaries(ctx, conn, activeMount{
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
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 1)
	})

	Convey("DirInfos avoids grouped raw summaries for small batches without maintained rows", t, func() {
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
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"a/", 7, 3)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, sid, mountPath+"b/", 7, 2)

		countingConn := &dirInfoSummaryQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		summaries, err := dbch.DirInfos(
			[]string{mountPath + "a", mountPath + "b"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(summaries[mountPath+"a"].Count, ShouldEqual, 3)
		So(summaries[mountPath+"b"].Count, ShouldEqual, 2)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.groupedSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 1)
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
		So(refreshMountDirSummaries(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: firstSID.String(),
			updatedAt:  firstUpdatedAt,
		}), ShouldBeNil)

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, secondUpdatedAt, secondSID, secondUpdatedAt), ShouldBeNil)
		insertDirSummaryTestGUTA(ctx, conn, mountPath, secondSID, mountPath+"a/", 7, 9)
		So(refreshMountDirSummaries(ctx, conn, activeMount{
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
		So(countingConn.rawSummaryQueryCount(), ShouldEqual, 0)
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
		So(refreshMountDirSummaries(ctx, conn, activeMount{
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
		So(countingConn.rawSummaryQueryCount(), ShouldEqual, 0)

		cache := newTreeQueryCache()

		for i := range treeDirSummaryCacheMaxEntries + 5 {
			key := newTreeDirSummaryCacheKey(
				"/mnt/cache/",
				fmt.Sprintf("snapshot-%d", i),
				fmt.Sprintf("/mnt/cache/%d/", i),
				db.DGUTAgeAll,
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
		[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
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
		So(countingConn.summaryBatchQueryCountValue(), ShouldBeGreaterThan, 0)
		So(firstDB.Close(), ShouldBeNil)

		countingConn.resetCounts()
		secondDB := newClickHouseDatabaseWithSnapshot(cfg, countingConn, snapshot)
		actual, err := db.NewTree(secondDB).Where(mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(countingConn.childBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.summaryBatchQueryCountValue(), ShouldEqual, 0)
		So(countingConn.queryCountValue(), ShouldEqual, 0)
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
		So(countingConn.summaryBatchQueryCountValue(), ShouldBeGreaterThan, 0)
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
		So(countingConn.childBatchQueryCount(), ShouldEqual, 2)
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
		So(countingConn.existenceQueryCount(), ShouldEqual, 1)
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

	Convey("DirsHaveChildren prefetches bounded visible-child summaries for broad Disktree clicks", t, func() {
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

		So(refreshMountDirSummaries(ctx, conn, activeMount{
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
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 1)

		countingConn.reset()

		clicked, err := tree.DirInfo(mountPath+"a", filter)
		So(err, ShouldBeNil)
		So(clicked.Children, ShouldHaveLength, 2)
		So(clicked.Children[0].Count, ShouldEqual, 3)
		So(clicked.Children[1].Count, ShouldEqual, 2)
		So(countingConn.mountDirSummaryQueryCount(), ShouldEqual, 0)
		So(countingConn.rawSummaryBatchQueryCount(), ShouldEqual, 0)
		So(countingConn.rawSummaryQueryCount(), ShouldEqual, 0)
	})

	Convey("DirsHaveChildren skips broad summary prefetch when discovered children exceed the bound", t, func() {
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

		So(refreshMountDirSummaries(ctx, conn, activeMount{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}), ShouldBeNil)

		countingConn := &hasChildrenQueryCountingConn{Conn: cp.conn}
		dbch := newClickHouseDatabase(cfg, countingConn)

		hasChildren, err := dbch.DirsHaveChildren([]string{parent}, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(hasChildren, ShouldResemble, map[string]bool{parent: true})
		So(countingConn.childBatchQueryCount(), ShouldEqual, 1)
		So(countingConn.mountSummaryQueryCount(), ShouldEqual, 0)
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
		So(countingConn.existenceQueryCount(), ShouldEqual, 3)
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
		So(di.Children[0].Dir, ShouldEqual, "/lustre")
		So(di.Children[0].Count, ShouldEqual, 10)
		So(di.Children[1].Dir, ShouldEqual, "/nfs")
		So(di.Children[1].Count, ShouldEqual, 5)

		hasChildren := tree.DirsHaveChildren(
			[]string{"/lustre", "/nfs"},
			&db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll},
		)
		So(hasChildren, ShouldResemble, map[string]bool{
			"/lustre": true,
			"/nfs":    false,
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

		So(countingConn.treeSummaryQueryCount(), ShouldBeGreaterThan, 0)
		So(countingConn.ancestorDGUTAQueryCount(), ShouldEqual, 0)
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
		fallbackCountingConn := &ancestorSummaryQueryCountingConn{Conn: failingConn}
		dbch := newClickHouseDatabase(cfg, fallbackCountingConn)
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
		So(fallbackCountingConn.ancestorDGUTAQueryCount(), ShouldBeGreaterThan, 0)

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
		So(readyCountingConn.ancestorDGUTAQueryCount(), ShouldEqual, 0)
	})
}

type treeSummaryRefreshDeadlineConn struct {
	ch.Conn

	failures            atomic.Int32
	availabilityQueries atomic.Int32
}

func (c *treeSummaryRefreshDeadlineConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	isAvailabilityQuery := strings.Contains(query, "FROM wrstat_tree_summary_sets") ||
		strings.Contains(query, "FROM wrstat_tree_dir_summary")

	if isAvailabilityQuery {
		c.availabilityQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *treeSummaryRefreshDeadlineConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.Contains(query, "INSERT INTO wrstat_tree_") {
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
	mountSummaryQueries      atomic.Int32
}

func (c *hasChildrenQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	isChildBatchQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
	if isChildBatchQuery {
		c.childBatchQueries.Add(1)
	}

	isChildSummaryBatchQuery := isRawDirInfoBatchQuery(query) || isGroupedDirInfoSummaryQuery(query)
	if isChildSummaryBatchQuery {
		c.childSummaryBatchQueries.Add(1)
	}

	isExistenceQuery := strings.Contains(query, "INNER JOIN wrstat_dguta d") &&
		strings.Contains(query, "GROUP BY c.parent_dir")
	if isExistenceQuery {
		c.existenceQueries.Add(1)
	}

	if isMountDirInfoSummaryQuery(query) {
		c.mountSummaryQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isRawDirInfoBatchQuery(query string) bool {
	return strings.Contains(query, "SELECT dir, gid, uid, ft, age, count, size") &&
		strings.Contains(query, "FROM wrstat_dguta") &&
		strings.Contains(query, "WHERE dir IN (")
}

func isGroupedDirInfoSummaryQuery(query string) bool {
	return strings.Contains(query, "sumForEachIf") &&
		strings.Contains(query, "FROM wrstat_dguta") &&
		strings.Contains(query, "WHERE dir IN (") &&
		strings.Contains(query, "GROUP BY dir")
}

func isMountDirInfoSummaryQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_summary") &&
		!strings.Contains(query, "wrstat_dir_summary_sets")
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

func (c *hasChildrenQueryCountingConn) mountSummaryQueryCount() int {
	return int(c.mountSummaryQueries.Load())
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
	isPrefetchQuery := strings.Contains(query, "FROM wrstat_dir_summary_sets") ||
		isMountDirInfoSummaryQuery(query)
	if isPrefetchQuery {
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
	rawSummaryBatchQueries atomic.Int32
	rawSummaryQueries      atomic.Int32
	mountSummaryQueries    atomic.Int32
}

func (c *dirInfoSummaryQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if isGroupedDirInfoSummaryQuery(query) {
		c.groupedSummaryQueries.Add(1)
	}

	if isRawDirInfoBatchQuery(query) {
		c.rawSummaryBatchQueries.Add(1)
	}

	if isRawDirInfoQuery(query) {
		c.rawSummaryQueries.Add(1)
	}

	if isMountDirInfoSummaryQuery(query) {
		c.mountSummaryQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isRawDirInfoQuery(query string) bool {
	return strings.Contains(query, "SELECT gid, uid, ft, age, count, size") &&
		strings.Contains(query, "FROM wrstat_dguta") &&
		strings.Contains(query, "dir = ?")
}

func (c *dirInfoSummaryQueryCountingConn) reset() {
	c.groupedSummaryQueries.Store(0)
	c.rawSummaryBatchQueries.Store(0)
	c.rawSummaryQueries.Store(0)
	c.mountSummaryQueries.Store(0)
}

func (c *dirInfoSummaryQueryCountingConn) groupedSummaryQueryCount() int {
	return int(c.groupedSummaryQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) rawSummaryBatchQueryCount() int {
	return int(c.rawSummaryBatchQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) rawSummaryQueryCount() int {
	return int(c.rawSummaryQueries.Load())
}

func (c *dirInfoSummaryQueryCountingConn) mountDirSummaryQueryCount() int {
	return int(c.mountSummaryQueries.Load())
}

type whereQueryCountingConn struct {
	ch.Conn

	childBatchQueries   atomic.Int32
	queries             atomic.Int32
	summaryBatchQueries atomic.Int32
	subtreeQueries      atomic.Int32
}

func (c *whereQueryCountingConn) childBatchQueryCountValue() int {
	return int(c.childBatchQueries.Load())
}

func (c *whereQueryCountingConn) subtreeQueryCountValue() int {
	return int(c.subtreeQueries.Load())
}

func (c *whereQueryCountingConn) resetCounts() {
	c.childBatchQueries.Store(0)
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

	isSummaryBatchQuery := strings.Contains(query, "FROM wrstat_dguta") &&
		strings.Contains(query, "WHERE dir IN (")
	if isSummaryBatchQuery {
		c.summaryBatchQueries.Add(1)
	}

	isChildBatchQuery := strings.Contains(query, "parent_dir, child") &&
		strings.Contains(query, "FROM wrstat_children") &&
		strings.Contains(query, "WHERE parent_dir IN (")
	if isChildBatchQuery {
		c.childBatchQueries.Add(1)
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

func (c *whereQueryCountingConn) summaryBatchQueryCountValue() int {
	return int(c.summaryBatchQueries.Load())
}

type ancestorSummaryQueryCountingConn struct {
	ch.Conn

	ancestorDGUTAQueries atomic.Int32
	treeSummaryQueries   atomic.Int32
}

func (c *ancestorSummaryQueryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if isAncestorDGUTAQuery(query) {
		c.ancestorDGUTAQueries.Add(1)
	}

	if isTreeSummaryQuery(query) {
		c.treeSummaryQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func isAncestorDGUTAQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_dguta d") &&
		strings.Contains(query, "WHERE d.dir = ?")
}

func isTreeSummaryQuery(query string) bool {
	return strings.Contains(query, "FROM wrstat_tree_dguta") ||
		strings.Contains(query, "JOIN wrstat_tree_dguta") ||
		strings.Contains(query, "FROM wrstat_tree_dir_summary") ||
		strings.Contains(query, "JOIN wrstat_tree_dir_summary")
}

func (c *ancestorSummaryQueryCountingConn) ancestorDGUTAQueryCount() int {
	return int(c.ancestorDGUTAQueries.Load())
}

func (c *ancestorSummaryQueryCountingConn) treeSummaryQueryCount() int {
	return int(c.treeSummaryQueries.Load())
}
