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
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const testInsertFileStmt = "INSERT INTO wrstat_files (mount_path, snapshot_id, parent_dir, " +
	"name, ext, entry_type, size, apparent_size, uid, gid, atime, mtime, ctime, inode, nlink) " +
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

func TestClientStatPath(t *testing.T) {
	Convey("Client.StatPath returns FileRow for active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = providerTestMountPath

		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)

		parentDir := mountPath + "dir/"
		name := "file.txt"
		path := parentDir + name
		atime := time.Now().UTC().Truncate(time.Second)
		mtime := atime.Add(-time.Minute)
		ctime := atime.Add(-2 * time.Minute)

		So(conn.Exec(
			ctx,
			testInsertFileStmt,
			mountPath,
			sid,
			parentDir,
			name,
			"txt",
			uint8(stats.FileType),
			uint64(123),
			uint64(456),
			uint32(1000),
			uint32(100),
			atime,
			mtime,
			ctime,
			uint64(777),
			uint64(1),
		), ShouldBeNil)

		row, err := c.StatPath(ctx, path, StatOptions{})
		So(err, ShouldBeNil)
		So(row, ShouldNotBeNil)
		So(row.Path, ShouldEqual, path)
		So(row.ParentDir, ShouldEqual, parentDir)
		So(row.Name, ShouldEqual, name)
		So(row.Ext, ShouldEqual, "txt")
		So(row.EntryType, ShouldEqual, byte(stats.FileType))
		So(row.Size, ShouldEqual, int64(123))
		So(row.ApparentSize, ShouldEqual, int64(456))
		So(row.UID, ShouldEqual, uint32(1000))
		So(row.GID, ShouldEqual, uint32(100))
		So(row.Inode, ShouldEqual, int64(777))
		So(row.Nlink, ShouldEqual, int64(1))
	})
}

func TestClientIsDir(t *testing.T) {
	Convey("Client.IsDir reports directory entry_type", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = providerTestMountPath

		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)

		parentDir := mountPath
		name := "dir/"
		path := parentDir + name
		now := time.Now().UTC().Truncate(time.Second)

		So(conn.Exec(
			ctx,
			testInsertFileStmt,
			mountPath,
			sid,
			parentDir,
			name,
			"",
			uint8(stats.DirType),
			uint64(0),
			uint64(0),
			uint32(1000),
			uint32(100),
			now,
			now,
			now,
			uint64(888),
			uint64(2),
		), ShouldBeNil)

		isDir, err := c.IsDir(ctx, path)
		So(err, ShouldBeNil)
		So(isDir, ShouldBeTrue)
	})
}

func TestClientListDir(t *testing.T) {
	Convey("Client.ListDir lists directory entries from the active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = providerTestMountPath

		base := mountPath + "dir/"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Insert two snapshots; second becomes active.
		updatedAt1 := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
		sid1 := snapshotID(mountPath, updatedAt1)
		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid1, updatedAt1), ShouldBeNil)

		updatedAt2 := time.Now().UTC().Truncate(time.Second)
		sid2 := snapshotID(mountPath, updatedAt2)
		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now().Add(time.Millisecond), sid2, updatedAt2), ShouldBeNil)

		now := time.Now().UTC().Truncate(time.Second)

		// Old snapshot entry that must not appear.
		So(conn.Exec(
			ctx,
			testInsertFileStmt,
			mountPath,
			sid1,
			base,
			"zzz_old.txt",
			"txt",
			uint8(stats.FileType),
			uint64(1),
			uint64(1),
			uint32(1),
			uint32(1),
			now,
			now,
			now,
			uint64(1),
			uint64(1),
		), ShouldBeNil)

		// Active snapshot entries.
		names := []string{"b.txt", "a.txt", "c.txt"}
		for _, name := range names {
			So(conn.Exec(
				ctx,
				testInsertFileStmt,
				mountPath,
				sid2,
				base,
				name,
				"txt",
				uint8(stats.FileType),
				uint64(10),
				uint64(10),
				uint32(1000),
				uint32(100),
				now,
				now,
				now,
				uint64(2),
				uint64(1),
			), ShouldBeNil)
		}

		// Pass dir without trailing slash to ensure normalisation.
		rows, err := c.ListDir(ctx, mountPath+"dir", ListOptions{Limit: 100, Offset: 0})
		So(err, ShouldBeNil)
		So(len(rows), ShouldEqual, 3)
		So(rows[0].Name, ShouldEqual, "a.txt")
		So(rows[1].Name, ShouldEqual, "b.txt")
		So(rows[2].Name, ShouldEqual, "c.txt")
		So(rows[0].ParentDir, ShouldEqual, base)
		So(rows[0].Path, ShouldEqual, base+"a.txt")
	})

	Convey("Client.ListDir supports limit and offset", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = providerTestMountPath

		base := mountPath + "dir/"
		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		now := time.Now().UTC().Truncate(time.Second)
		for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
			So(conn.Exec(
				ctx,
				testInsertFileStmt,
				mountPath,
				sid,
				base,
				name,
				"txt",
				uint8(stats.FileType),
				uint64(10),
				uint64(10),
				uint32(1000),
				uint32(100),
				now,
				now,
				now,
				uint64(2),
				uint64(1),
			), ShouldBeNil)
		}

		rows, err := c.ListDir(ctx, base, ListOptions{Limit: 2, Offset: 1})
		So(err, ShouldBeNil)
		So(len(rows), ShouldEqual, 2)
		So(rows[0].Name, ShouldEqual, "b.txt")
		So(rows[1].Name, ShouldEqual, "c.txt")
	})
}

func TestClientPermissionAnyInDir(t *testing.T) {
	Convey("Client.PermissionAnyInDir checks ownership against dguta rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = providerTestMountPath

		dir := mountPath + "dir/"

		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		So(conn.Exec(
			ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid,
			dir,
			uint32(111),
			uint32(222),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(1),
			uint64(1),
			int64(1),
			int64(1),
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		), ShouldBeNil)

		ok, err := c.PermissionAnyInDir(ctx, mountPath+"dir", 222, nil)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionAnyInDir(ctx, dir, 999, []uint32{111})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionAnyInDir(ctx, dir, 999, []uint32{999})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}
