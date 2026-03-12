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

		So(conn.Exec(ctx, testInsertMountStmt,
			mountB, time.Now(), sidB, updatedB,
		), ShouldBeNil)

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
