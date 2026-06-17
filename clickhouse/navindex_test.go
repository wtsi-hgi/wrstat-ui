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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	navIndexTestMount     = testRootMountPath + "a/"
	navIndexTestSnapshot  = "00000000-0000-0000-0000-000000000001"
	navIndexTestAlpha     = navIndexTestMount + "alpha/"
	navIndexTestBeta      = navIndexTestMount + "beta/"
	navIndexTestAlphaLeaf = navIndexTestAlpha + "leaf/"
)

type navIndexCatalogConn struct {
	bootstrapTestConn

	mu      sync.Mutex
	queries int
}

func newNavIndexCatalogConn() *navIndexCatalogConn {
	return &navIndexCatalogConn{}
}

func (c *navIndexCatalogConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	c.queries++
	c.mu.Unlock()

	switch {
	case strings.Contains(query, "SELECT dir_id, parent_id, subtree_end, name"):
		return newC2Rows(navIndexCatalogRows()), nil
	case strings.Contains(query, "path_hash") && strings.Contains(query, "full_path = ?"):
		return c.resolvePathRows(args), nil
	case strings.Contains(query, "dir_id = ?"):
		return c.resolveIDRows(args), nil
	case strings.Contains(query, "parent_id = ?"):
		return c.childrenRows(args), nil
	case strings.Contains(query, "child_dir_count > 0"):
		return c.parentsWithChildrenRows(args), nil
	case strings.Contains(query, "parent.full_path IN"):
		return c.childrenByParentRows(args), nil
	default:
		return newC2Rows(nil), nil
	}
}

func navIndexCatalogRows() [][]any {
	rows := make([][]any, 0, len(navIndexRefs()))
	for _, row := range navIndexRefs() {
		rows = append(rows, []any{
			row.dirID,
			row.parentID,
			row.subtreeEnd,
			row.name,
			row.childDirCount,
			row.childFileCount,
		})
	}

	return rows
}

func (c *navIndexCatalogConn) queryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.queries
}

func (c *navIndexCatalogConn) resetQueryCount() {
	c.mu.Lock()
	c.queries = 0
	c.mu.Unlock()
}

func (c *navIndexCatalogConn) resolvePathRows(args []any) driver.Rows {
	path := c2Arg[string](args, 3)
	for _, row := range navIndexRefs() {
		if row.fullPath == path {
			return newC2Rows([][]any{{row.dirID, row.parentID, row.subtreeEnd, row.fullPath}})
		}
	}

	return newC2Rows(nil)
}

func navIndexRefs() []navIndexCatalogFixtureRow {
	return []navIndexCatalogFixtureRow{
		{dirID: 0, parentID: summary.ParentSentinel, subtreeEnd: 6, name: "/", childDirCount: 1, fullPath: "/"},
		{dirID: 1, parentID: 0, subtreeEnd: 6, name: "mnt/", childDirCount: 1, fullPath: testRootMountPath},
		{dirID: 2, parentID: 1, subtreeEnd: 6, name: "a/", childDirCount: 2, childFileCount: 3, fullPath: navIndexTestMount},
		{dirID: 3, parentID: 2, subtreeEnd: 5, name: "alpha/", childDirCount: 1, fullPath: navIndexTestAlpha},
		{dirID: 4, parentID: 2, subtreeEnd: 5, name: "beta/", fullPath: navIndexTestBeta},
		{dirID: 5, parentID: 3, subtreeEnd: 6, name: "leaf/", fullPath: navIndexTestAlphaLeaf},
	}
}

func (c *navIndexCatalogConn) resolveIDRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)
	for _, row := range navIndexRefs() {
		if row.dirID == dirID {
			return newC2Rows([][]any{{row.dirID, row.parentID, row.subtreeEnd, row.fullPath}})
		}
	}

	return newC2Rows(nil)
}

func (c *navIndexCatalogConn) childrenRows(args []any) driver.Rows {
	parentID := c2Arg[uint32](args, 2)
	rows := make([][]any, 0)

	for _, row := range navIndexRefs() {
		if row.parentID == parentID && row.dirID != parentID {
			rows = append(rows, []any{row.fullPath})
		}
	}

	return newC2Rows(rows)
}

func (c *navIndexCatalogConn) parentsWithChildrenRows(args []any) driver.Rows {
	wanted := map[string]bool{}

	for _, arg := range args[2:] {
		if dir, ok := arg.(string); ok {
			wanted[ensureTrailingSlash(dir)] = true
		}
	}

	rows := make([][]any, 0)

	for _, row := range navIndexRefs() {
		if wanted[row.fullPath] && row.childDirCount > 0 {
			rows = append(rows, []any{row.fullPath})
		}
	}

	return newC2Rows(rows)
}

func (c *navIndexCatalogConn) childrenByParentRows(args []any) driver.Rows {
	wanted := map[string]bool{}

	for _, arg := range args[2:] {
		if dir, ok := arg.(string); ok {
			wanted[ensureTrailingSlash(dir)] = true
		}
	}

	rows := make([][]any, 0)

	for _, parent := range navIndexRefs() {
		if !wanted[parent.fullPath] {
			continue
		}

		for _, child := range navIndexRefs() {
			if child.parentID == parent.dirID && child.dirID != parent.dirID {
				rows = append(rows, []any{parent.fullPath, child.fullPath})
			}
		}
	}

	return newC2Rows(rows)
}

func TestNavIndexI1(t *testing.T) {
	Convey("I1 flag off keeps the ClickHouse-only path", t, func() {
		conn := newNavIndexCatalogConn()
		database := newNavIndexTestDatabase(Config{}, conn)

		children, err := database.childrenForMount(navIndexTestMount, navIndexTestSnapshot, navIndexTestMount)
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{
			navIndexTestAlpha,
			navIndexTestBeta,
		})
		So(conn.queryCount(), ShouldEqual, 2)

		ancestors, ok, err := database.catalogAncestorPathsForMount(
			context.Background(),
			navIndexTestMount,
			navIndexTestSnapshot,
			navIndexTestAlphaLeaf,
		)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(ancestors, ShouldResemble, []string{
			"/",
			testRootMountPath,
			navIndexTestMount,
			navIndexTestAlpha,
			navIndexTestAlphaLeaf,
		})
		So(conn.queryCount(), ShouldEqual, 7)
	})

	Convey("I1 flag on uses a ready index without ClickHouse round-trips", t, func() {
		conn := newNavIndexCatalogConn()
		database := newNavIndexTestDatabase(Config{NavIndex: true}, conn)
		So(database.navIndex.build(context.Background()), ShouldBeNil)
		conn.resetQueryCount()

		children, err := database.childrenForMount(navIndexTestMount, navIndexTestSnapshot, navIndexTestMount)
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{
			navIndexTestAlpha,
			navIndexTestBeta,
		})

		result := map[string]bool{navIndexTestMount: false, navIndexTestBeta: false}
		group := navIndexTestGroup([]string{navIndexTestMount, navIndexTestBeta})
		remaining, err := database.addCatalogDirsHaveChildrenForMount(result, group)
		So(err, ShouldBeNil)
		So(remaining.queryDirs, ShouldBeEmpty)
		So(result, ShouldResemble, map[string]bool{navIndexTestMount: true, navIndexTestBeta: false})

		ancestors, ok, err := database.catalogAncestorPathsForMount(
			context.Background(),
			navIndexTestMount,
			navIndexTestSnapshot,
			navIndexTestAlphaLeaf,
		)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(ancestors, ShouldResemble, []string{
			"/",
			testRootMountPath,
			navIndexTestMount,
			navIndexTestAlpha,
			navIndexTestAlphaLeaf,
		})
		So(conn.queryCount(), ShouldEqual, 0)
	})

	Convey("I1 flag on falls back until the async index is ready", t, func() {
		conn := newNavIndexCatalogConn()
		database := newNavIndexTestDatabase(Config{NavIndex: true}, conn)

		children, err := database.childrenForMount(navIndexTestMount, navIndexTestSnapshot, navIndexTestMount)
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{
			navIndexTestAlpha,
			navIndexTestBeta,
		})
		So(conn.queryCount(), ShouldEqual, 2)
	})

	Convey("I1 benchmark evidence reports build memory and latency delta", t, func() {
		conn := newNavIndexCatalogConn()
		index := newNavIndexManager(Config{NavIndex: true}, conn, navIndexTestSnapshotState())
		So(index.build(context.Background()), ShouldBeNil)

		evidence := index.benchmarkEvidence(context.Background(), navIndexTestMount, navIndexTestSnapshot, navIndexTestMount)
		So(evidence["dir_count"], ShouldEqual, 6)
		So(evidence["estimated_bytes"], ShouldBeGreaterThan, 0)
		So(evidence["measured_heap_bytes"], ShouldBeGreaterThan, 0)
		So(evidence["build_duration_ms"], ShouldBeGreaterThanOrEqualTo, 0.0)
		So(evidence["clickhouse_latency_ns"], ShouldBeGreaterThanOrEqualTo, int64(0))
		So(evidence["index_latency_ns"], ShouldBeGreaterThanOrEqualTo, int64(0))
		So(evidence["latency_delta_ns"], ShouldNotBeNil)
		So(evidence["estimate_formula"], ShouldContainSubstring, "4 dir_id implicit")

		t.Logf(
			"nav-index fixture evidence: dir_count=%v estimated_bytes=%v measured_heap_bytes=%v "+
				"build_duration_ms=%v index_latency_ns=%v clickhouse_latency_ns=%v latency_delta_ns=%v",
			evidence["dir_count"],
			evidence["estimated_bytes"],
			evidence["measured_heap_bytes"],
			evidence["build_duration_ms"],
			evidence["index_latency_ns"],
			evidence["clickhouse_latency_ns"],
			evidence["latency_delta_ns"],
		)
	})
}

func navIndexTestGroup(dirs []string) *activeMountDirGroup {
	group := &activeMountDirGroup{
		mount:        navIndexTestMountActive(),
		originalDirs: make(map[string]string, len(dirs)),
	}
	for _, dir := range dirs {
		group.queryDirs = append(group.queryDirs, ensureTrailingSlash(dir))
		group.originalDirs[ensureTrailingSlash(dir)] = dir
	}

	return group
}

func navIndexTestMountActive() activeMount {
	return activeMount{
		mountPath:  navIndexTestMount,
		snapshotID: navIndexTestSnapshot,
		updatedAt:  time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
	}
}

func navIndexTestSnapshotState() *activeMountsSnapshot {
	return newActiveMountsSnapshot([]mountsActiveRow{{
		mountPath:  navIndexTestMount,
		snapshotID: navIndexTestSnapshot,
		updatedAt:  time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
	}})
}

func newNavIndexTestDatabase(cfg Config, conn *navIndexCatalogConn) *clickHouseDatabase {
	ResetTreeQueryCaches()

	snapshot := navIndexTestSnapshotState()
	database := newClickHouseDatabaseWithSnapshot(Config{}, conn, snapshot)
	database.cfg = cfg
	database.navIndex = newNavIndexManager(cfg, conn, snapshot)
	database.treeCache.putMountDirSummaryReady(newTreeMountCacheKey(navIndexTestMount, navIndexTestSnapshot))

	return database
}

type navIndexCatalogFixtureRow struct {
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	name           string
	childDirCount  uint32
	childFileCount uint32
	fullPath       string
}
