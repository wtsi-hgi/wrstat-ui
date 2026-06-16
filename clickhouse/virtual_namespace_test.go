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
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	g2LustreAMount = "/lustre/a/"
	g2LustreBMount = "/lustre/b/"
	g2NFSMount     = "/nfs/x/"

	g2LustreASnapshot = "11111111-1111-1111-1111-111111111111"
	g2LustreBSnapshot = "22222222-2222-2222-2222-222222222222"
	g2NFSSnapshot     = "33333333-3333-3333-3333-333333333333"
)

type g2Rows struct {
	namespace activeVirtualNamespace
	summaries []activeVirtualSummaryRow
	filters   []activeVirtualFilterAllRow
	children  []activeVirtualChildRow
}

func g2OverlayRows(refreshedAt time.Time, filterRows map[string][]activeVirtualFilterAllRow) g2Rows {
	rootGUTAs := map[string]db.GUTAs{
		g2LustreAMount: {g2GUTA(10, 100)},
		g2LustreBMount: {g2GUTA(20, 200)},
		g2NFSMount:     {g2GUTA(30, 300)},
	}

	if filterRows == nil {
		filterRows = map[string][]activeVirtualFilterAllRow{
			g2LustreAMount: {g2FilterRow(10, 100)},
			g2LustreBMount: {g2FilterRow(20, 200)},
			g2NFSMount:     {g2FilterRow(30, 300)},
		}
	}

	namespace, summaries, filters, children := activeVirtualRowsForMountsFromDataWithLinks(
		"g2-set",
		g2ActiveMounts(),
		refreshedAt,
		rootGUTAs,
		filterRows,
		g2MountRootLinks(),
	)

	return g2Rows{namespace: namespace, summaries: summaries, filters: filters, children: children}
}

func (r g2Rows) summaryByPath(path string) activeVirtualSummaryRow {
	for _, row := range r.summaries {
		if ensureTrailingSlash(row.Dir) == ensureTrailingSlash(path) {
			return row
		}
	}

	return activeVirtualSummaryRow{}
}

func (r g2Rows) filterByPath(path string) activeVirtualFilterAllRow {
	for _, row := range r.filters {
		if ensureTrailingSlash(row.Dir) == ensureTrailingSlash(path) {
			return row
		}
	}

	return activeVirtualFilterAllRow{}
}

func (r g2Rows) childIDs(parentID uint32) []uint32 {
	out := []uint32{}

	for _, row := range r.children {
		if row.ParentVirtualID == parentID {
			out = append(out, row.ChildVirtualID)
		}
	}

	return out
}

func TestActiveVirtualNamespaceG2(t *testing.T) {
	Convey("G2 virtual catalog uses a dense per-active-set id space for synthetic nodes only", t, func() {
		refreshedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
		mounts := g2ActiveMounts()
		links := g2MountRootLinks()

		namespace := activeVirtualNamespaceForMounts("g2-set", mounts, links, refreshedAt)

		So(namespace.rows, ShouldHaveLength, 6)
		So(g2NamespacePaths(namespace.rows), ShouldResemble, []string{
			"/",
			"/lustre/",
			"/lustre/a/",
			"/lustre/b/",
			"/nfs/",
			"/nfs/x/",
		})
		So(g2NamespaceIDs(namespace.rows), ShouldResemble, []uint32{1, 2, 3, 4, 5, 6})
		So(namespace.idForPath("/lustre/"), ShouldEqual, uint32(2))
		So(namespace.parentIDForPath(g2LustreAMount), ShouldEqual, uint32(2))
		So(namespace.rowForPath(g2LustreAMount).MountRootDirID, ShouldEqual, uint32(41))
		So(namespace.rowForPath(g2LustreAMount).SnapshotID, ShouldEqual, g2LustreASnapshot)
		So(namespace.rowForPath("/lustre/").IsMountRootBox, ShouldEqual, uint8(0))
		So(namespace.rowForPath(g2LustreAMount).IsMountRootBox, ShouldEqual, uint8(1))
	})

	Convey("G2 root summaries aggregate selected mounts once with separate namespace boxes", t, func() {
		refreshedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
		rows := g2OverlayRows(refreshedAt, nil)

		root := rows.summaryByPath("/")
		lustre := rows.summaryByPath("/lustre/")
		nfs := rows.summaryByPath("/nfs/")

		So(root.AllCount, ShouldEqual, uint64(60))
		So(root.AllSize, ShouldEqual, uint64(600))
		So(root.ChildCount, ShouldEqual, uint64(2))
		So(lustre.AllCount, ShouldEqual, uint64(30))
		So(lustre.ChildCount, ShouldEqual, uint64(2))
		So(nfs.AllCount, ShouldEqual, uint64(30))
		So(nfs.ChildCount, ShouldEqual, uint64(1))
	})

	Convey("G2 virtual children, filtered summaries, and active-prefix inputs use the same virtual ids", t, func() {
		refreshedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
		rows := g2OverlayRows(refreshedAt, nil)

		rootID := rows.namespace.idForPath("/")
		lustreID := rows.namespace.idForPath("/lustre/")
		nfsID := rows.namespace.idForPath("/nfs/")

		So(rows.childIDs(rootID), ShouldResemble, []uint32{lustreID, nfsID})
		So(rows.filterByPath("/lustre/").VirtualID, ShouldEqual, lustreID)
		So(rows.summaryByPath("/lustre/").VirtualID, ShouldEqual, lustreID)
		So(rows.filterByPath("/lustre/").Count, ShouldEqual, uint64(30))
	})

	Convey("G2 exact virtual parent summaries do not double-count per-mount above-root rows", t, func() {
		refreshedAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
		rows := g2OverlayRows(refreshedAt, nil)

		So(rows.summaryByPath("/lustre/").AllCount, ShouldEqual, uint64(30))
		So(rows.summaryByPath("/lustre/").AllCount, ShouldNotEqual, uint64(60))
	})

	Convey("G2 overlay precedence routes virtual paths through the catalog and not wrstat_children", t, func() {
		So(activeVirtualMountChildCountsQuery, ShouldNotContainSubstring, "wrstat_children")
		So(activeVirtualCatalogChildrenQuery, ShouldContainSubstring, "wrstat_active_virtual_dirs")
		So(activeVirtualCatalogChildrenQuery, ShouldContainSubstring, "parent_id")
	})

	Convey("G2 active-set derivation and readiness are reused", t, func() {
		So(activeVirtualReadyQuery, ShouldContainSubstring, "wrstat_active_virtual_sets")
		So(activeVirtualReadyQueryVersion, ShouldEqual, activeVirtualReadyQueryVersionC2)

		rows := []mountsActiveRow{
			{mountPath: g2NFSMount, snapshotID: g2NFSSnapshot, updatedAt: time.Unix(1, 0).UTC()},
			{mountPath: g2LustreAMount, snapshotID: g2LustreASnapshot, updatedAt: time.Unix(2, 0).UTC()},
		}
		So(fingerprintForMountsActive(rows), ShouldEqual, fingerprintForMountsActive(slicesReversed(rows)))
	})

	Convey("G2 hot active-virtual tables have no path string key columns", t, func() {
		ddl, err := os.ReadFile("schema/018_active_virtual_overlay.sql")
		So(err, ShouldBeNil)

		children := g2CreateTableDDL(string(ddl), "wrstat_active_virtual_children")
		summaries := g2CreateTableDDL(string(ddl), "wrstat_active_virtual_summaries")
		filters := g2CreateTableDDL(string(ddl), "wrstat_active_virtual_filter_all")

		for _, table := range []string{children, summaries, filters} {
			So(table, ShouldNotContainSubstring, "\n  dir String")
			So(table, ShouldNotContainSubstring, "\n  parent_dir String")
			So(table, ShouldNotContainSubstring, "\n  child_dir String")
		}

		So(children, ShouldContainSubstring, "parent_virtual_id UInt32")
		So(children, ShouldContainSubstring, "child_virtual_id UInt32")
		So(children, ShouldContainSubstring, "mount_root_dir_id UInt32")
		So(summaries, ShouldContainSubstring, "virtual_id UInt32")
		So(filters, ShouldContainSubstring, "virtual_id UInt32")
	})
}

func g2ActiveMounts() []activeMount {
	return []activeMount{
		{mountPath: g2LustreAMount, snapshotID: g2LustreASnapshot, updatedAt: time.Unix(1, 0).UTC()},
		{mountPath: g2LustreBMount, snapshotID: g2LustreBSnapshot, updatedAt: time.Unix(2, 0).UTC()},
		{mountPath: g2NFSMount, snapshotID: g2NFSSnapshot, updatedAt: time.Unix(3, 0).UTC()},
	}
}

func g2MountRootLinks() map[string]activeVirtualMountRootLink {
	return map[string]activeVirtualMountRootLink{
		g2LustreAMount: {SnapshotID: g2LustreASnapshot, DirID: 41, ChildCount: 10},
		g2LustreBMount: {SnapshotID: g2LustreBSnapshot, DirID: 42, ChildCount: 20},
		g2NFSMount:     {SnapshotID: g2NFSSnapshot, DirID: 51, ChildCount: 30},
	}
}

func g2NamespacePaths(rows []activeVirtualDirRow) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row.FullPath
	}

	return out
}

func g2NamespaceIDs(rows []activeVirtualDirRow) []uint32 {
	out := make([]uint32, len(rows))
	for i, row := range rows {
		out[i] = row.VirtualID
	}

	return out
}

func slicesReversed(rows []mountsActiveRow) []mountsActiveRow {
	out := append([]mountsActiveRow(nil), rows...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return out
}

func g2CreateTableDDL(ddl string, table string) string {
	start := strings.Index(ddl, "CREATE TABLE IF NOT EXISTS "+table)
	if start < 0 {
		return ""
	}

	rest := ddl[start:]

	next := strings.Index(rest[len("CREATE TABLE IF NOT EXISTS "+table):], "CREATE TABLE IF NOT EXISTS ")
	if next < 0 {
		return rest
	}

	return rest[:len("CREATE TABLE IF NOT EXISTS "+table)+next]
}

func g2GUTA(count, size uint64) *db.GUTA {
	return &db.GUTA{
		GID:   1,
		UID:   2,
		FT:    db.DGUTAFileTypeBam,
		Age:   db.DGUTAgeAll,
		Count: count,
		Size:  size,
		Atime: 10,
		Mtime: 20,
	}
}

func g2FilterRow(count, size uint64) activeVirtualFilterAllRow {
	return activeVirtualFilterAllRow{
		Age:          uint8(db.DGUTAgeAll),
		GID:          1,
		UID:          2,
		FT:           uint16(db.DGUTAFileTypeBam),
		Count:        count,
		Size:         size,
		AtimeBuckets: emptyAgeBuckets(),
		MtimeBuckets: emptyAgeBuckets(),
	}
}
