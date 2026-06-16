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
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type e1QueryShapeConn struct {
	ch.Conn

	catalogBandReads              int
	catalogChildCountReads        int
	childFilterAllParentBandReads int
	factRowReads                  int
	vectorArrayJoins              int
}

func (c *e1QueryShapeConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if isE1CatalogBandRead(query) {
		c.catalogBandReads++
	}

	if isE2CatalogChildCountRead(query) {
		c.catalogChildCountReads++
	}

	if isE2ChildFilterAllParentBandRead(query) {
		c.childFilterAllParentBandReads++
	}

	if isE1FactRowRead(query) {
		c.factRowReads++
	}

	if strings.Contains(query, "arrayJoin(") {
		c.vectorArrayJoins++
	}

	return c.Conn.Query(ctx, query, args...)
}

func isE1CatalogBandRead(query string) bool {
	return strings.Contains(query, "FROM wrstat_dirs") &&
		strings.Contains(query, "parent_id = ?") &&
		strings.Contains(query, "full_path")
}

func isE2CatalogChildCountRead(query string) bool {
	return strings.Contains(query, "FROM wrstat_dirs") &&
		strings.Contains(query, "full_path IN") &&
		strings.Contains(query, "child_dir_count > 0")
}

func isE2ChildFilterAllParentBandRead(query string) bool {
	return strings.Contains(query, "FROM wrstat_child_filter_all") &&
		strings.Contains(query, "parent_id IN") &&
		strings.Contains(query, "count > 0")
}

func isE1FactRowRead(query string) bool {
	return strings.Contains(query, "FROM wrstat_dir_facts") &&
		strings.Contains(query, "dir_id IN") &&
		strings.Contains(query, "gids")
}

func TestClickHouseDatabaseE1IntegerBands(t *testing.T) {
	Convey("Children reads the resolved catalog parent_id band", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e1-children/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 9, 0, 0, 0, time.UTC)
		mount := e1SeedMount(ctx, conn, "/mnt/e1-children/", updatedAt)
		e1SeedProjectionSet(ctx, conn, mount)
		e1SeedCatalogDir(ctx, conn, mount, 1, 0, 5, 0, mount.mountPath, 2)
		e1SeedCatalogDir(ctx, conn, mount, 2, 1, 3, 1, mount.mountPath+"b/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 3, 1, 4, 1, mount.mountPath+"a/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 4, 1, 5, 1, mount.mountPath+"a/", 0)

		shapeConn := &e1QueryShapeConn{Conn: conn}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, shapeConn, e1Snapshot(mount))

		children, err := dbch.Children(strings.TrimSuffix(mount.mountPath, "/"))
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{mount.mountPath + "a/", mount.mountPath + "b/"})
		So(shapeConn.catalogBandReads, ShouldEqual, 1)
	})

	Convey("Children de-duplicates and sorts virtual ancestor children across active mounts", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{
			"/mnt/e1-multi/shared/one/",
			"/mnt/e1-multi/alpha/run/",
			"/mnt/e1-multi/shared/two/",
			"/mnt/e1-multi/zeta/",
		}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 11, 0, 0, 0, time.UTC)

		mounts := make([]activeMount, 0, len(cfg.MountPoints))
		for i, mountPath := range cfg.MountPoints {
			mount := e1SeedMount(ctx, conn, mountPath, updatedAt.Add(time.Duration(i)*time.Minute))
			e1SeedProjectionSet(ctx, conn, mount)
			mounts = append(mounts, mount)
		}

		dbch := newClickHouseDatabaseWithSnapshot(cfg, conn, e1Snapshots(mounts...))

		children, err := dbch.Children("/mnt/e1-multi")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{
			"/mnt/e1-multi/alpha",
			"/mnt/e1-multi/shared",
			"/mnt/e1-multi/zeta",
		})
	})

	Convey("Children returns nil for active-mount leaves and missing dirs", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e1-leaf/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
		mount := e1SeedMount(ctx, conn, "/mnt/e1-leaf/", updatedAt)
		e1SeedProjectionSet(ctx, conn, mount)
		e1SeedCatalogDir(ctx, conn, mount, 1, 0, 3, 0, mount.mountPath, 1)
		e1SeedCatalogDir(ctx, conn, mount, 2, 1, 3, 1, mount.mountPath+"child/", 0)

		dbch := newClickHouseDatabaseWithSnapshot(cfg, conn, e1Snapshot(mount))

		children, err := dbch.Children(mount.mountPath + "child")
		So(err, ShouldBeNil)
		So(children, ShouldBeNil)

		children, err = dbch.Children(mount.mountPath + "missing")
		So(err, ShouldBeNil)
		So(children, ShouldBeNil)
	})

	Convey("DirInfo merges active mount roots and keeps the latest Modtime", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e1-modtime/old/", "/mnt/e1-modtime/new/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		olderUpdatedAt := time.Date(2026, 6, 16, 13, 0, 0, 0, time.UTC)
		newerUpdatedAt := time.Date(2026, 6, 16, 14, 30, 0, 0, time.UTC)
		olderMount := e1SeedMount(ctx, conn, "/mnt/e1-modtime/old/", olderUpdatedAt)
		newerMount := e1SeedMount(ctx, conn, "/mnt/e1-modtime/new/", newerUpdatedAt)

		for _, mount := range []activeMount{olderMount, newerMount} {
			e1SeedProjectionSet(ctx, conn, mount)
			e1SeedCatalogDir(ctx, conn, mount, 1, 0, 2, 0, mount.mountPath, 0)
		}

		e1SeedFact(ctx, conn, olderMount, 1, 0, 2, db.GUTAs{
			e1GUTA(31, 41, db.DGUTAFileTypeBam, 2),
		}, 0)
		e1SeedFact(ctx, conn, newerMount, 1, 0, 2, db.GUTAs{
			e1GUTA(32, 42, db.DGUTAFileTypeBam, 5),
		}, 0)

		dbch := newClickHouseDatabaseWithSnapshot(cfg, conn, e1Snapshots(olderMount, newerMount))
		filter := &db.Filter{FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll}

		sum, err := dbch.DirInfo("/mnt/e1-modtime", filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 7)
		So(sum.GIDs, ShouldResemble, []uint32{31, 32})
		So(sum.Modtime, ShouldResemble, activeSetUpdatedAt(newerUpdatedAt))
	})

	Convey("DirInfo and DirInfos summarise direct facts rows for arbitrary filters", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e1-facts/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
		mount := e1SeedMount(ctx, conn, "/mnt/e1-facts/", updatedAt)
		e1SeedProjectionSet(ctx, conn, mount)
		e1SeedCatalogDir(ctx, conn, mount, 1, 0, 4, 0, mount.mountPath, 2)
		e1SeedCatalogDir(ctx, conn, mount, 2, 1, 3, 1, mount.mountPath+"a/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 3, 1, 4, 1, mount.mountPath+"b/", 0)

		e1SeedFact(ctx, conn, mount, 1, 0, 4, nil, 2)
		e1SeedFact(ctx, conn, mount, 2, 1, 3, db.GUTAs{
			e1GUTA(7, 11, db.DGUTAFileTypeBam, 3),
			e1GUTA(8, 11, db.DGUTAFileTypeCram, 5),
		}, 0)
		e1SeedFact(ctx, conn, mount, 3, 1, 4, db.GUTAs{
			e1GUTA(7, 12, db.DGUTAFileTypeBam, 2),
		}, 0)

		shapeConn := &e1QueryShapeConn{Conn: conn}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, shapeConn, e1Snapshot(mount))
		filter := &db.Filter{GIDs: []uint32{7}, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll}

		sum, err := dbch.DirInfo(mount.mountPath+"a/", filter)
		So(err, ShouldBeNil)
		So(sum, ShouldNotBeNil)
		So(sum.Count, ShouldEqual, 3)
		So(sum.GIDs, ShouldResemble, []uint32{7})
		So(sum.Modtime, ShouldResemble, updatedAt)

		summaries, err := dbch.DirInfos([]string{mount.mountPath + "a", mount.mountPath + "b/"}, filter)
		So(err, ShouldBeNil)
		So(summaries, ShouldContainKey, mount.mountPath+"a")
		So(summaries, ShouldContainKey, mount.mountPath+"b/")
		So(summaries[mount.mountPath+"a"].Count, ShouldEqual, 3)
		So(summaries[mount.mountPath+"b/"].Count, ShouldEqual, 2)
		So(shapeConn.factRowReads, ShouldBeGreaterThanOrEqualTo, 2)
		So(shapeConn.vectorArrayJoins, ShouldEqual, 0)
	})

	Convey("DirsHaveChildren reads broad catalog counts and filtered child-filter parent bands", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e2-children/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 15, 0, 0, 0, time.UTC)
		mount := e1SeedMount(ctx, conn, "/mnt/e2-children/", updatedAt)
		e1SeedProjectionSet(ctx, conn, mount)
		e1SeedCatalogDir(ctx, conn, mount, 1, 0, 8, 0, mount.mountPath, 3)
		e1SeedCatalogDir(ctx, conn, mount, 2, 1, 5, 1, mount.mountPath+"a/", 2)
		e1SeedCatalogDir(ctx, conn, mount, 3, 2, 4, 2, mount.mountPath+"a/match/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 4, 2, 5, 2, mount.mountPath+"a/empty/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 5, 1, 7, 1, mount.mountPath+"b/", 1)
		e1SeedCatalogDir(ctx, conn, mount, 6, 5, 7, 2, mount.mountPath+"b/nomatch/", 0)
		e1SeedCatalogDir(ctx, conn, mount, 7, 1, 8, 1, mount.mountPath+"leaf/", 0)
		e1SeedSchema3Readiness(ctx, conn, mount, 3)
		e1SeedChildFilterAll(ctx, conn, mount, 2, 3, db.DGUTAgeA1M, 7, 11, db.DGUTAFileTypeBam, 4)
		e1SeedChildFilterAll(ctx, conn, mount, 2, 4, db.DGUTAgeA1M, 8, 11, db.DGUTAFileTypeBam, 2)
		e1SeedChildFilterAll(ctx, conn, mount, 5, 6, db.DGUTAgeA1M, 8, 11, db.DGUTAFileTypeBam, 3)

		shapeConn := &e1QueryShapeConn{Conn: conn}
		dbch := newClickHouseDatabaseWithSnapshot(cfg, shapeConn, e1Snapshot(mount))
		dirs := []string{
			mount.mountPath + "a",
			mount.mountPath + "b/",
			mount.mountPath + "leaf",
			mount.mountPath + "missing",
		}

		broad, err := dbch.DirsHaveChildren(dirs, nil)
		So(err, ShouldBeNil)
		So(broad, ShouldResemble, map[string]bool{
			mount.mountPath + "a":       true,
			mount.mountPath + "b/":      true,
			mount.mountPath + "leaf":    false,
			mount.mountPath + "missing": false,
		})
		So(shapeConn.catalogChildCountReads, ShouldBeGreaterThanOrEqualTo, 1)

		filtered, err := dbch.DirsHaveChildren(dirs, &db.Filter{
			GIDs: []uint32{7},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeA1M,
		})
		So(err, ShouldBeNil)
		So(filtered, ShouldResemble, map[string]bool{
			mount.mountPath + "a":       true,
			mount.mountPath + "b/":      false,
			mount.mountPath + "leaf":    false,
			mount.mountPath + "missing": false,
		})
		So(shapeConn.childFilterAllParentBandReads, ShouldBeGreaterThanOrEqualTo, 1)
	})

	Convey("Catalog parent_id walk resolves breadcrumb ancestors byte-identically", t, func() {
		setE1ClickHouseTestEnv()
		ResetTreeQueryCaches()
		Reset(ResetTreeQueryCaches)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{"/mnt/e2-ancestors/"}

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 16, 16, 0, 0, 0, time.UTC)
		mount := e1SeedMount(ctx, conn, "/mnt/e2-ancestors/", updatedAt)
		e1SeedProjectionSet(ctx, conn, mount)
		e1SeedCatalogDir(ctx, conn, mount, 1, 0, 5, 0, mount.mountPath, 1)
		e1SeedCatalogDir(ctx, conn, mount, 2, 1, 5, 1, mount.mountPath+"alpha/", 1)
		e1SeedCatalogDir(ctx, conn, mount, 3, 2, 5, 2, mount.mountPath+"alpha/beta/", 1)
		e1SeedCatalogDir(ctx, conn, mount, 4, 3, 5, 3, mount.mountPath+"alpha/beta/leaf/", 0)

		dbch := newClickHouseDatabaseWithSnapshot(cfg, conn, e1Snapshot(mount))
		ancestors, ok, err := dbch.catalogAncestorPathsForMount(
			ctx,
			mount.mountPath,
			mount.snapshotID,
			mount.mountPath+"alpha/beta/leaf",
		)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(ancestors, ShouldResemble, []string{
			mount.mountPath,
			mount.mountPath + "alpha/",
			mount.mountPath + "alpha/beta/",
			mount.mountPath + "alpha/beta/leaf/",
		})
	})
}

func setE1ClickHouseTestEnv() {
	So(os.Setenv("WRSTAT_ENV", "test"), ShouldBeNil)
	Reset(func() { So(os.Unsetenv("WRSTAT_ENV"), ShouldBeNil) })
}

func e1SeedMount(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	updatedAt time.Time,
) activeMount {
	sid := snapshotID(mountPath, updatedAt)
	So(conn.Exec(ctx, testInsertMountStmt, mountPath, updatedAt, sid, updatedAt), ShouldBeNil)

	return activeMount{
		mountPath:  mountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}
}

func e1SeedProjectionSet(ctx context.Context, conn ch.Conn, mount activeMount) {
	So(conn.Exec(
		ctx,
		insertMountDirSummarySetQuery,
		mount.mountPath,
		mount.snapshotID,
		mount.updatedAt,
		time.Now().UTC(),
	), ShouldBeNil)
}

func e1SeedCatalogDir(
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

func e1Snapshot(mount activeMount) *activeMountsSnapshot {
	return e1Snapshots(mount)
}

func e1Snapshots(mounts ...activeMount) *activeMountsSnapshot {
	rows := make([]mountsActiveRow, len(mounts))
	for i, mount := range mounts {
		rows[i] = mountsActiveRow{
			mountPath:  mount.mountPath,
			snapshotID: mount.snapshotID,
			updatedAt:  mount.updatedAt,
		}
	}

	return newActiveMountsSnapshot(rows)
}

func e1SeedFact(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dirID uint32,
	parentID uint32,
	subtreeEnd uint32,
	gutas db.GUTAs,
	childCount uint64,
) {
	batch, err := conn.PrepareBatch(ctx, insertMountDirSummaryQuery)
	So(err, ShouldBeNil)

	err = appendMountDirFactRowValuesForRecord(batch, mount, dgutaRecordContext{
		dirID:      dirID,
		parentID:   parentID,
		subtreeEnd: subtreeEnd,
	}, gutas, childCount, time.Now().UTC())
	So(err, ShouldBeNil)
	So(batch.Send(), ShouldBeNil)
}

func e1GUTA(
	gid uint32,
	uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         db.DGUTAgeAll,
		Count:       count,
		Size:        count * 10,
		Atime:       10,
		ATimeRanges: summary.AgeBuckets{count, 0, 0, 0, 0, 0, 0, 0, 0},
		Mtime:       20,
		MTimeRanges: summary.AgeBuckets{0, count, 0, 0, 0, 0, 0, 0, 0},
	}
}

func e1SeedSchema3Readiness(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	childFilterAllRows uint64,
) {
	So(conn.Exec(
		ctx,
		insertSchema3SnapshotSetQuery,
		mount.mountPath,
		mount.snapshotID,
		currentSchemaVersion,
		uint64(1),
		uint64(1),
		childFilterAllRows,
		uint64(0),
		"e2-test",
		time.Now().UTC(),
	), ShouldBeNil)
}

func e1SeedChildFilterAll(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	parentID uint32,
	dirID uint32,
	age db.DirGUTAge,
	gid uint32,
	uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
) {
	So(conn.Exec(
		ctx,
		insertChildFilterAllQuery,
		mount.mountPath,
		mount.snapshotID,
		parentID,
		uint8(age),
		gid,
		uid,
		uint16(ft),
		dirID,
		count,
		count*10,
		int64(10),
		int64(20),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
		uint64(0),
		uint64(0),
		uint8(0),
		uint8(0),
		time.Now().UTC(),
	), ShouldBeNil)
}
