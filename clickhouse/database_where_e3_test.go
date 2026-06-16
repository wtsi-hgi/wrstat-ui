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
)

type e3ProviderCloser interface {
	Close() error
}

type e3WhereRangeShapeConn struct {
	ch.Conn

	summaryRangeReads atomic.Int32
	startsWithReads   atomic.Int32
}

func (c *e3WhereRangeShapeConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	normalised := strings.Join(strings.Fields(query), " ")
	if e3WhereSummaryRangeQuery(normalised) {
		c.summaryRangeReads.Add(1)
	}

	if strings.Contains(normalised, "startsWith(") {
		c.startsWithReads.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func e3WhereSummaryRangeQuery(query string) bool {
	return (strings.Contains(query, "FROM wrstat_dir_facts") ||
		strings.Contains(query, "FROM wrstat_dir_filter_all") ||
		strings.Contains(query, "FROM wrstat_dir_filter_ageall")) &&
		strings.Contains(query, "dir_id >= ?") &&
		strings.Contains(query, "dir_id < ?")
}

func (c *e3WhereRangeShapeConn) summaryRangeReadsValue() int {
	return int(c.summaryRangeReads.Load())
}

func (c *e3WhereRangeShapeConn) startsWithReadsValue() int {
	return int(c.startsWithReads.Load())
}

func TestClickHouseDatabaseWhereE3DirIDRanges(t *testing.T) {
	Convey("E3 Where range scan matches broad and filtered generic baselines", t, func() {
		env := newE3WhereRangeEnv(t, "/mnt/e3-range/")
		defer env.close()

		splitFn := split.SplitsToSplitFn(1)
		genericTree := db.NewTree(&e3GenericTreeDB{
			d: newClickHouseDatabaseWithSnapshot(env.cfg, env.conn, e3Snapshot(env.mount)),
		})
		fastTree := db.NewTree(newClickHouseDatabaseWithSnapshot(env.cfg, env.shapeConn, e3Snapshot(env.mount)))

		expectedBroad, err := genericTree.Where(env.mount.mountPath, &db.Filter{}, splitFn)
		So(err, ShouldBeNil)

		actualBroadFilter := &db.Filter{}
		actualBroad, err := fastTree.Where(env.mount.mountPath, actualBroadFilter, splitFn)
		So(err, ShouldBeNil)
		So(actualBroad, ShouldResemble, expectedBroad)
		So(actualBroadFilter.FT, ShouldEqual, db.AllTypesExceptDirectories)
		So(e3WhereDCSsSorted(actualBroad), ShouldBeTrue)

		filter := &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{11},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeAll,
		}
		expectedFiltered, err := genericTree.Where(env.mount.mountPath, filter, splitFn)
		So(err, ShouldBeNil)

		actualFiltered, err := fastTree.Where(env.mount.mountPath, cloneE3Filter(filter), splitFn)
		So(err, ShouldBeNil)
		So(actualFiltered, ShouldResemble, expectedFiltered)
		So(e3WhereDCSsSorted(actualFiltered), ShouldBeTrue)
		So(e3WhereDirs(actualFiltered), ShouldResemble, []string{
			env.mount.mountPath,
			env.mount.mountPath + "alpha/deep/",
			env.mount.mountPath + "beta/open/",
		})
		So(env.shapeConn.summaryRangeReadsValue(), ShouldBeGreaterThanOrEqualTo, 2)
		So(env.shapeConn.startsWithReadsValue(), ShouldEqual, 0)
	})

	Convey("E3 auth-restricted Where only returns permitted directories", t, func() {
		env := newE3WhereRangeEnv(t, "/mnt/e3-auth/")
		defer env.close()

		filter := &db.Filter{GIDs: []uint32{7}, Age: db.DGUTAgeAll}
		splitFn := split.SplitsToSplitFn(2)
		genericTree := db.NewTree(&e3GenericTreeDB{
			d: newClickHouseDatabaseWithSnapshot(env.cfg, env.conn, e3Snapshot(env.mount)),
		})
		expected, err := genericTree.Where(env.mount.mountPath, cloneE3Filter(filter), splitFn)
		So(err, ShouldBeNil)

		fastTree := db.NewTree(newClickHouseDatabaseWithSnapshot(env.cfg, env.shapeConn, e3Snapshot(env.mount)))
		actual, err := fastTree.Where(env.mount.mountPath, filter, splitFn)
		So(err, ShouldBeNil)
		So(actual, ShouldResemble, expected)
		So(e3WhereDirs(actual), ShouldResemble, []string{
			env.mount.mountPath,
			env.mount.mountPath + "alpha/deep/",
			env.mount.mountPath + "beta/open/",
		})
		So(e3WhereAllGIDsAllowed(actual, 7), ShouldBeTrue)
		So(env.shapeConn.summaryRangeReadsValue(), ShouldBeGreaterThan, 0)
	})
}

func newE3WhereRangeEnv(t *testing.T, mountPath string) e3WhereRangeEnv {
	t.Helper()

	So(os.Setenv("WRSTAT_ENV", "test"), ShouldBeNil)
	Reset(func() { So(os.Unsetenv("WRSTAT_ENV"), ShouldBeNil) })
	ResetTreeQueryCaches()
	Reset(ResetTreeQueryCaches)

	th := newClickHouseTestHarness(t)
	cfg := th.newConfig()
	cfg.QueryTimeout = 5 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{mountPath}

	p, err := OpenProvider(cfg)
	So(err, ShouldBeNil)

	conn := th.openConn(cfg.DSN)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	updatedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	mount := e3SeedMount(ctx, conn, mountPath, updatedAt)
	e3SeedProjectionSet(ctx, conn, mount)
	e3SeedWhereRangeTree(ctx, conn, mount)

	return e3WhereRangeEnv{
		cfg:       cfg,
		conn:      conn,
		provider:  p,
		shapeConn: &e3WhereRangeShapeConn{Conn: conn},
		mount:     mount,
	}
}

func e3SeedMount(ctx context.Context, conn ch.Conn, mountPath string, updatedAt time.Time) activeMount {
	sid := snapshotID(mountPath, updatedAt)
	So(conn.Exec(ctx, testInsertMountStmt, mountPath, updatedAt, sid, updatedAt), ShouldBeNil)

	return activeMount{
		mountPath:  mountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}
}

func e3SeedProjectionSet(ctx context.Context, conn ch.Conn, mount activeMount) {
	So(conn.Exec(ctx, insertMountDirSummarySetQuery, mount.mountPath, mount.snapshotID, mount.updatedAt, time.Now().UTC()),
		ShouldBeNil)
}

func e3SeedWhereRangeTree(ctx context.Context, conn ch.Conn, mount activeMount) {
	e3SeedCatalogDir(ctx, conn, mount, 1, 0, 7, 0, mount.mountPath, 2)
	e3SeedCatalogDir(ctx, conn, mount, 2, 1, 5, 1, mount.mountPath+"alpha/", 2)
	e3SeedCatalogDir(ctx, conn, mount, 3, 2, 4, 2, mount.mountPath+"alpha/deep/", 0)
	e3SeedCatalogDir(ctx, conn, mount, 4, 2, 5, 2, mount.mountPath+"alpha/side/", 0)
	e3SeedCatalogDir(ctx, conn, mount, 5, 1, 7, 1, mount.mountPath+"beta/", 1)
	e3SeedCatalogDir(ctx, conn, mount, 6, 5, 7, 2, mount.mountPath+"beta/open/", 0)

	batch, err := conn.PrepareBatch(ctx, insertMountDirSummaryQuery)
	So(err, ShouldBeNil)

	rows := []e3WhereFactRow{
		{dirID: 1, parentID: 0, subtreeEnd: 7, childCount: 2, gutas: db.GUTAs{
			e3GUTA(7, 11, db.DGUTAFileTypeBam, 10, 350),
			e3GUTA(8, 12, db.DGUTAFileTypeCram, 6, 50),
			e3GUTA(7, 11, db.DGUTAFileTypeDir, 2, 999),
		}},
		{dirID: 2, parentID: 1, subtreeEnd: 5, childCount: 2, gutas: db.GUTAs{
			e3GUTA(7, 11, db.DGUTAFileTypeBam, 6, 200),
			e3GUTA(8, 12, db.DGUTAFileTypeCram, 2, 10),
			e3GUTA(7, 11, db.DGUTAFileTypeDir, 1, 1000),
		}},
		{dirID: 3, parentID: 2, subtreeEnd: 4, gutas: db.GUTAs{
			e3GUTA(7, 11, db.DGUTAFileTypeBam, 6, 200),
		}},
		{dirID: 4, parentID: 2, subtreeEnd: 5, gutas: db.GUTAs{
			e3GUTA(8, 12, db.DGUTAFileTypeCram, 2, 10),
		}},
		{dirID: 5, parentID: 1, subtreeEnd: 7, childCount: 1, gutas: db.GUTAs{
			e3GUTA(7, 11, db.DGUTAFileTypeBam, 4, 150),
		}},
		{dirID: 6, parentID: 5, subtreeEnd: 7, gutas: db.GUTAs{
			e3GUTA(7, 11, db.DGUTAFileTypeBam, 4, 150),
		}},
	}
	for _, row := range rows {
		record := dgutaRecordContext{dirID: row.dirID, parentID: row.parentID, subtreeEnd: row.subtreeEnd}
		err = appendMountDirFactRowValuesForRecord(batch, mount, record, row.gutas, row.childCount, time.Now().UTC())
		So(err, ShouldBeNil)
	}

	So(batch.Send(), ShouldBeNil)
}

func e3SeedCatalogDir(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dirID uint32,
	parentID uint32,
	subtreeEnd uint32,
	depth uint16,
	fullPath string,
	childCount uint32,
) {
	fullPath = ensureTrailingSlash(fullPath)
	So(conn.Exec(
		ctx,
		insertDirsQuery,
		mount.mountPath,
		mount.snapshotID,
		dirID,
		parentID,
		subtreeEnd,
		depth,
		catalogNameForFullPath(fullPath),
		fullPath,
		childCount,
		uint32(0),
		catalogPathHash(fullPath),
	), ShouldBeNil)
}

func e3GUTA(gid, uid uint32, ft db.DirGUTAFileType, count, size uint64) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         db.DGUTAgeAll,
		Count:       count,
		Size:        size,
		Atime:       10,
		Mtime:       20,
		ATimeRanges: [9]uint64{count},
		MTimeRanges: [9]uint64{0, count},
	}
}

func e3Snapshot(mount activeMount) *activeMountsSnapshot {
	return newActiveMountsSnapshot([]mountsActiveRow{{
		mountPath:  mount.mountPath,
		snapshotID: mount.snapshotID,
		updatedAt:  mount.updatedAt,
	}})
}

func e3WhereDCSsSorted(dcss db.DCSs) bool {
	for i := 1; i < len(dcss); i++ {
		if dcss[i-1].Size < dcss[i].Size {
			return false
		}

		if dcss[i-1].Size == dcss[i].Size && dcss[i-1].Dir > dcss[i].Dir {
			return false
		}
	}

	return true
}

func cloneE3Filter(filter *db.Filter) *db.Filter {
	if filter == nil {
		return nil
	}

	clone := *filter
	clone.GIDs = append([]uint32(nil), filter.GIDs...)
	clone.UIDs = append([]uint32(nil), filter.UIDs...)

	return &clone
}

func e3WhereDirs(dcss db.DCSs) []string {
	dirs := make([]string, len(dcss))
	for i, dcs := range dcss {
		dirs[i] = dcs.Dir
	}

	return dirs
}

func e3WhereAllGIDsAllowed(dcss db.DCSs, allowed uint32) bool {
	for _, dcs := range dcss {
		for _, gid := range dcs.GIDs {
			if gid != allowed {
				return false
			}
		}
	}

	return true
}

type e3WhereRangeEnv struct {
	cfg       Config
	conn      ch.Conn
	provider  e3ProviderCloser
	shapeConn *e3WhereRangeShapeConn
	mount     activeMount
}

func (e e3WhereRangeEnv) close() {
	So(e.conn.Close(), ShouldBeNil)
	So(e.provider.Close(), ShouldBeNil)
}

type e3GenericTreeDB struct {
	d *clickHouseDatabase
}

func (d *e3GenericTreeDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	return d.d.DirInfo(dir, filter)
}

func (d *e3GenericTreeDB) Children(dir string) ([]string, error) {
	return d.d.Children(dir)
}

func (d *e3GenericTreeDB) Info() (*db.Info, error) {
	return d.d.Info()
}

func (d *e3GenericTreeDB) Close() error {
	return d.d.Close()
}

type e3WhereFactRow struct {
	dirID      uint32
	parentID   uint32
	subtreeEnd uint32
	childCount uint64
	gutas      db.GUTAs
}
