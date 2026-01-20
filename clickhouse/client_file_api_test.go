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
