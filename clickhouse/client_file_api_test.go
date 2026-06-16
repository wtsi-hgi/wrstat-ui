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
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const testInsertFileStmt = "INSERT INTO wrstat_files (mount_path, snapshot_id, dir_id, " +
	"name, ext, entry_type, size, apparent_size, uid, gid, atime, mtime, ctime, inode, nlink) " +
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

const testInsertFileDirStmt = "INSERT INTO wrstat_dirs " +
	"(mount_path, snapshot_id, dir_id, parent_id, subtree_end, depth, name, full_path, " +
	"child_dir_count, child_file_count, path_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

const clientFileAPIBName = "b.txt"

const clientFileAPITestFanout uint32 = 32

const clientFileAPITestMaxDepth = 5

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

		insertClientFileForTest(
			ctx,
			conn,
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
		)

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

func TestClientStatPathErrors(t *testing.T) {
	Convey("Client.StatPath preserves missing and invalid path errors", t, func() {
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

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		row, err := c.StatPath(ctx, mountPath+"missing.txt", StatOptions{})
		So(row, ShouldBeNil)
		So(errors.Is(err, errPathNotFound), ShouldBeTrue)

		rootMountClient := &Client{
			cfg:         Config{QueryTimeout: time.Second},
			conn:        &bootstrapTestConn{},
			mountPoints: basedirs.ValidateMountPoints([]string{"/"}),
		}

		row, err = rootMountClient.StatPath(ctx, "/", StatOptions{})
		So(row, ShouldBeNil)
		So(errors.Is(err, errInvalidPath), ShouldBeTrue)

		row, err = c.StatPath(ctx, "/outside/missing.txt", StatOptions{})
		So(row, ShouldBeNil)
		So(errors.Is(err, basedirs.ErrInvalidBasePath), ShouldBeTrue)
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

		insertClientFileForTest(
			ctx,
			conn,
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
		)

		isDir, err := c.IsDir(ctx, path)
		So(err, ShouldBeNil)
		So(isDir, ShouldBeTrue)
	})
}

func TestClientIsDirErrors(t *testing.T) {
	Convey("Client.IsDir preserves missing and invalid path errors", t, func() {
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

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		isDir, err := c.IsDir(ctx, mountPath+"missing/")
		So(err, ShouldBeNil)
		So(isDir, ShouldBeFalse)

		rootMountClient := &Client{
			cfg:         Config{QueryTimeout: time.Second},
			conn:        &bootstrapTestConn{},
			mountPoints: basedirs.ValidateMountPoints([]string{"/"}),
		}

		isDir, err = rootMountClient.IsDir(ctx, "/")
		So(errors.Is(err, errInvalidPath), ShouldBeTrue)
		So(isDir, ShouldBeFalse)

		isDir, err = c.IsDir(ctx, "/outside/missing/")
		So(errors.Is(err, basedirs.ErrInvalidBasePath), ShouldBeTrue)
		So(isDir, ShouldBeFalse)
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
		insertClientFileForTest(
			ctx,
			conn,
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
		)

		// Active snapshot entries.
		names := []string{clientFileAPIBName, "a.txt", "c.txt"}
		for _, name := range names {
			insertClientFileForTest(
				ctx,
				conn,
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
			)
		}

		// Pass dir without trailing slash to ensure normalisation.
		rows, err := c.ListDir(ctx, mountPath+"dir", ListOptions{Limit: 100, Offset: 0})
		So(err, ShouldBeNil)
		So(len(rows), ShouldEqual, 3)
		So(rows[0].Name, ShouldEqual, "a.txt")
		So(rows[1].Name, ShouldEqual, clientFileAPIBName)
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
		for _, name := range []string{"a.txt", clientFileAPIBName, "c.txt"} {
			insertClientFileForTest(
				ctx,
				conn,
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
			)
		}

		rows, err := c.ListDir(ctx, base, ListOptions{Limit: 2, Offset: 1})
		So(err, ShouldBeNil)
		So(len(rows), ShouldEqual, 2)
		So(rows[0].Name, ShouldEqual, clientFileAPIBName)
		So(rows[1].Name, ShouldEqual, "c.txt")
	})
}

func TestClientListDirErrors(t *testing.T) {
	Convey("Client.ListDir preserves empty missing directories and invalid base path errors", t, func() {
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

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		rows, err := c.ListDir(ctx, mountPath+"missing", ListOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldBeEmpty)

		rows, err = c.ListDir(ctx, "/outside/missing", ListOptions{})
		So(errors.Is(err, basedirs.ErrInvalidBasePath), ShouldBeTrue)
		So(rows, ShouldBeNil)
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

		insertPermissionAgeAllFact(ctx, conn, mountPath, sid.String(), dir, 222, 111)

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

	Convey("Client.PermissionAnyInDir checks active AgeAll fact vectors", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/mnt/a/"}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/a/"

		dir := mountPath

		staleUpdatedAt := time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)
		staleSID := snapshotID(mountPath, staleUpdatedAt)
		updatedAt := time.Date(2026, 6, 1, 15, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, staleUpdatedAt, staleSID, staleUpdatedAt), ShouldBeNil)
		insertPermissionAgeAllFact(ctx, conn, mountPath, staleSID.String(), dir, 99, 99)

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		insertPermissionAgeAllFact(ctx, conn, mountPath, sid.String(), dir, 11, 7)

		ok, err := c.PermissionAnyInDir(ctx, dir, 11, []uint32{8})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionAnyInDir(ctx, dir, 99, []uint32{7})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionAnyInDir(ctx, dir, 99, []uint32{8})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}

func insertPermissionAgeAllFact(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	dir string,
	uid uint32,
	gid uint32,
) {
	dir = ensureTrailingSlash(dir)
	dirID := clientFileAPIDirID(dir)

	ensureClientFileAPICatalogChain(ctx, conn, mountPath, sid, dir)

	So(conn.Exec(
		ctx,
		testInsertInfoFactVectorStmt,
		mountPath,
		sid,
		dirID,
		uint32(0),
		clientFileAPISubtreeEnd(dir),
		[]uint32{gid},
		[]uint32{uid},
		[]uint16{uint16(db.DGUTAFileTypeBam)},
		[]uint8{uint8(db.DGUTAgeAll)},
		[]uint64{1},
		[]uint64{10},
		[]int64{10},
		[]int64{20},
		[][]uint64{{1, 0, 0, 0, 0, 0, 0, 0, 0}},
		[][]uint64{{0, 1, 0, 0, 0, 0, 0, 0, 0}},
	), ShouldBeNil)
}

func TestClientPermissionPath(t *testing.T) {
	Convey("Client.PermissionPath checks exact active file rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/mnt/c5-perm/"}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c5-perm/"

		base := mountPath + "a/"
		updatedAt := time.Date(2026, 6, 1, 16, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		now := time.Now().UTC().Truncate(time.Second)
		insert := func(name string, uid uint32, gid uint32) {
			insertClientFileForTest(
				ctx,
				conn,
				mountPath,
				sid,
				base,
				name,
				"txt",
				uint8(stats.FileType),
				uint64(1),
				uint64(1),
				uid,
				gid,
				now,
				now,
				now,
				uint64(1),
				uint64(1),
			)
		}

		insert("owned.txt", 10, 30)
		insert("group.txt", 11, 20)
		insert("denied.txt", 11, 30)

		ok, err := c.PermissionPath(ctx, base+"owned.txt", 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionPath(ctx, base+"group.txt", 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionPath(ctx, base+"denied.txt", 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})

	Convey("Client.PermissionPath checks only the active exact file row", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/mnt/a/"}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/a/"

		name := "file.bam"
		path := mountPath + name
		staleUpdatedAt := time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)
		staleSID := snapshotID(mountPath, staleUpdatedAt)
		updatedAt := time.Date(2026, 6, 1, 16, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, staleUpdatedAt, staleSID, staleUpdatedAt), ShouldBeNil)
		insertPermissionFile(ctx, conn, mountPath, staleSID.String(), mountPath, name, 99, 99)

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, updatedAt, sid, updatedAt), ShouldBeNil)
		insertPermissionFile(ctx, conn, mountPath, sid.String(), mountPath, name, 11, 7)

		ok, err := c.PermissionPath(ctx, path, 11, []uint32{8})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionPath(ctx, path, 99, []uint32{7})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = c.PermissionPath(ctx, path, 99, []uint32{8})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}

func TestClientFindByGlob(t *testing.T) {
	Convey("Client.FindByGlob finds files matching gitignore-style patterns", t, func() {
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

		atime := time.Now().UTC().Truncate(time.Second)
		mtime := atime.Add(-time.Minute)
		ctime := atime.Add(-2 * time.Minute)

		path1 := base + "a.txt"
		insertClientFileForTest(
			ctx,
			conn,
			mountPath,
			sid,
			base,
			"a.txt",
			"txt",
			uint8(stats.FileType),
			uint64(1),
			uint64(1),
			uint32(222),
			uint32(111),
			atime,
			mtime,
			ctime,
			uint64(1),
			uint64(1),
		)

		path2 := base + "sub/nested.txt"
		insertClientFileForTest(
			ctx,
			conn,
			mountPath,
			sid,
			base+"sub/",
			"nested.txt",
			"txt",
			uint8(stats.FileType),
			uint64(2),
			uint64(2),
			uint32(333),
			uint32(999),
			atime,
			mtime,
			ctime,
			uint64(2),
			uint64(1),
		)

		base2 := mountPath + "other/"
		path3 := base2 + clientFileAPIBName
		insertClientFileForTest(
			ctx,
			conn,
			mountPath,
			sid,
			base2,
			clientFileAPIBName,
			"txt",
			uint8(stats.FileType),
			uint64(3),
			uint64(3),
			uint32(444),
			uint32(111),
			atime,
			mtime,
			ctime,
			uint64(3),
			uint64(1),
		)

		rows, err := c.FindByGlob(ctx, []string{base}, nil, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldBeEmpty)

		rows, err = c.FindByGlob(ctx, []string{base}, []string{"*"}, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 1)
		So(rows[0].Path, ShouldEqual, path1)

		rows, err = c.FindByGlob(ctx, []string{base}, []string{"**"}, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So([]string{rows[0].Path, rows[1].Path}, ShouldResemble, []string{path1, path2})

		rows, err = c.FindByGlob(ctx, []string{base}, []string{"**/*.txt"}, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So([]string{rows[0].Path, rows[1].Path}, ShouldResemble, []string{path1, path2})

		rows, err = c.FindByGlob(ctx, []string{base}, []string{"**"}, FindOptions{RequireOwner: true, UID: 999})
		So(err, ShouldBeNil)
		So(rows, ShouldBeEmpty)

		rows, err = c.FindByGlob(
			ctx,
			[]string{base},
			[]string{"**"},
			FindOptions{RequireOwner: true, UID: 999, GIDs: []uint32{111}},
		)
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 1)
		So(rows[0].Path, ShouldEqual, path1)

		patterns := make([]string, 0, 33)
		for i := 0; i < 32; i++ {
			patterns = append(patterns, "does-not-match")
		}

		patterns = append(patterns, "*")

		rows, err = c.FindByGlob(ctx, []string{base}, patterns, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 1)
		So(rows[0].Path, ShouldEqual, path1)

		rows, err = c.FindByGlob(ctx, []string{base, base2}, []string{"*"}, FindOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So([]string{rows[0].Path, rows[1].Path}, ShouldResemble, []string{path1, path3})

		count, err := c.CountByGlob(ctx, []string{base}, []string{"**"}, FindOptions{})
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 2)

		count, err = c.CountByGlob(
			ctx,
			[]string{base},
			[]string{"**"},
			FindOptions{RequireOwner: true, UID: 999, GIDs: []uint32{111}},
		)
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 1)

		count, err = c.CountByGlob(ctx, []string{base}, []string{"**"}, FindOptions{Limit: 1, Offset: 1})
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 1)
	})
}

func insertPermissionFile(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	parentDir string,
	name string,
	uid uint32,
	gid uint32,
) {
	now := time.Now().UTC().Truncate(time.Second)

	insertClientFileForTest(
		ctx,
		conn,
		mountPath,
		sid,
		parentDir,
		name,
		"bam",
		uint8(stats.FileType),
		uint64(1),
		uint64(1),
		uid,
		gid,
		now,
		now,
		now,
		uint64(1),
		uint64(1),
	)
}

func TestClientFindByGlobC5ExtensionPredicates(t *testing.T) {
	Convey("Client.FindByGlob preserves regex authority with exact-safe extension pruning", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/mnt/c5-glob/"}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c5-glob/"

		base := mountPath
		nested := mountPath + "a/"
		updatedAt := time.Date(2026, 6, 1, 16, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		now := time.Now().UTC().Truncate(time.Second)
		insert := func(parentDir string, name string, ext string, uid uint32, gid uint32, inode uint64) {
			insertClientFileForTest(
				ctx,
				conn,
				mountPath,
				sid,
				parentDir,
				name,
				ext,
				uint8(stats.FileType),
				uint64(1),
				uint64(1),
				uid,
				gid,
				now,
				now,
				now,
				inode,
				uint64(1),
			)
		}

		insert(base, "a.bam", "bam", 10, 30, 1)
		insert(base, ".bam", "", 10, 30, 2)
		insert(base, "b.BAM", "bam", 10, 30, 3)
		insert(base, "c.cram", "cram", 10, 30, 4)
		insert(base, "a.tar.gz", "gz", 10, 30, 9)
		insert(base, "b.tar.bz2", "bz2", 10, 30, 10)
		insert(nested, "owned.bam", "bam", 10, 30, 5)
		insert(nested, "group.bam", "bam", 11, 20, 6)
		insert(nested, "denied.bam", "bam", 11, 30, 7)
		insert(nested, "fake.cram", "bam", 10, 20, 8)

		rows, err := c.FindByGlob(ctx, []string{base}, []string{"*.bam"}, FindOptions{})
		So(err, ShouldBeNil)
		So(fileRowPaths(rows), ShouldResemble, []string{base + ".bam", base + "a.bam"})

		rows, err = c.FindByGlob(ctx, []string{base}, []string{"*.tar.gz"}, FindOptions{})
		So(err, ShouldBeNil)
		So(fileRowPaths(rows), ShouldResemble, []string{base + "a.tar.gz"})
	})
}

func TestClientFindByGlobC5OwnerAndRegexAuthority(t *testing.T) {
	Convey("Client.FindByGlob combines exact-safe extension pruning with owner filtering", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/mnt/c5-owner/"}

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/c5-owner/"

		base := mountPath
		nested := mountPath + "a/"
		updatedAt := time.Date(2026, 6, 1, 17, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		now := time.Now().UTC().Truncate(time.Second)
		insert := func(name string, ext string, uid uint32, gid uint32, inode uint64) {
			insertClientFileForTest(
				ctx,
				conn,
				mountPath,
				sid,
				nested,
				name,
				ext,
				uint8(stats.FileType),
				uint64(1),
				uint64(1),
				uid,
				gid,
				now,
				now,
				now,
				inode,
				uint64(1),
			)
		}

		insert("owned.bam", "bam", 10, 30, 1)
		insert("group.bam", "bam", 11, 20, 2)
		insert("denied.bam", "bam", 11, 30, 3)
		insert("fake.cram", "bam", 10, 20, 4)

		rows, err := c.FindByGlob(
			ctx,
			[]string{base},
			[]string{findByGlobRecursiveBamPattern},
			FindOptions{RequireOwner: true, UID: 10, GIDs: []uint32{20}},
		)
		So(err, ShouldBeNil)
		So(fileRowPaths(rows), ShouldResemble, []string{
			nested + "group.bam",
			nested + "owned.bam",
		})
	})
}

func insertClientFileForTest(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid any,
	parentDir string,
	name string,
	ext string,
	entryType uint8,
	size uint64,
	apparentSize uint64,
	uid uint32,
	gid uint32,
	atime time.Time,
	mtime time.Time,
	ctime time.Time,
	inode uint64,
	nlink uint64,
) {
	parentDir = ensureTrailingSlash(parentDir)
	dirID := clientFileAPIDirID(parentDir)

	ensureClientFileAPICatalogChain(ctx, conn, mountPath, sid, parentDir)

	So(conn.Exec(
		ctx,
		testInsertFileStmt,
		mountPath,
		sid,
		dirID,
		name,
		ext,
		entryType,
		size,
		apparentSize,
		uid,
		gid,
		atime,
		mtime,
		ctime,
		inode,
		nlink,
	), ShouldBeNil)
}

func clientFileAPICatalogChain(dir string) []string {
	trimmed := strings.Trim(ensureTrailingSlash(dir), "/")
	if trimmed == "" {
		return []string{"/"}
	}

	segments := strings.Split(trimmed, "/")
	out := make([]string, 0, len(segments))
	current := "/"

	for _, segment := range segments {
		current += segment + "/"
		out = append(out, current)
	}

	return out
}

func ensureClientFileAPICatalogDir(ctx context.Context, conn ch.Conn, mountPath string, sid any, dir string) {
	dir = ensureTrailingSlash(dir)
	dirID := clientFileAPIDirID(dir)

	dirExists := countRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = ? AND dir_id = ?",
		mountPath,
		sid,
		dirID,
	) > 0

	if !dirExists {
		So(conn.Exec(
			ctx,
			testInsertFileDirStmt,
			mountPath,
			sid,
			dirID,
			clientFileAPIParentID(dir),
			clientFileAPISubtreeEnd(dir),
			clientFileAPIDepth(dir),
			catalogNameForFullPath(dir),
			dir,
			uint32(0),
			uint32(0),
			catalogPathHash(dir),
		), ShouldBeNil)
	}
}

func clientFileAPIParentID(dir string) uint32 {
	parentDir, _, ok := splitPathParentAndName(ensureTrailingSlash(dir))
	if !ok {
		return 0
	}

	return clientFileAPIDirID(parentDir)
}

func ensureClientFileAPICatalogChain(ctx context.Context, conn ch.Conn, mountPath string, sid any, dir string) {
	for _, catalogDir := range clientFileAPICatalogChain(dir) {
		ensureClientFileAPICatalogDir(ctx, conn, mountPath, sid, catalogDir)
	}
}

func clientFileAPISubtreeEnd(dir string) uint32 {
	id := clientFileAPIDirID(dir)

	trimmed := strings.Trim(ensureTrailingSlash(dir), "/")
	if trimmed == "" {
		return id + clientFileAPIPow(clientFileAPITestFanout, clientFileAPITestMaxDepth+1)
	}

	exp := max(clientFileAPITestMaxDepth-len(strings.Split(trimmed, "/"))+1, 0)

	return id + clientFileAPIPow(clientFileAPITestFanout, exp)
}

func clientFileAPIDirID(dir string) uint32 {
	trimmed := strings.Trim(ensureTrailingSlash(dir), "/")
	if trimmed == "" {
		return 1
	}

	var id uint32 = 1

	segments := strings.Split(trimmed, "/")
	for i, segment := range segments {
		exp := max(clientFileAPITestMaxDepth-i, 0)
		id += clientFileAPIComponentRank(segment) * clientFileAPIPow(clientFileAPITestFanout, exp)
	}

	return id
}

func clientFileAPIComponentRank(segment string) uint32 {
	if segment == "" {
		return 1
	}

	c := strings.ToLower(segment[:1])[0]
	switch {
	case c >= 'a' && c <= 'z':
		return uint32(c-'a') + 1
	case c >= '0' && c <= '9':
		return uint32(c-'0') + 1
	default:
		return 1
	}
}

func clientFileAPIPow(base uint32, exp int) uint32 {
	out := uint32(1)
	for range exp {
		out *= base
	}

	return out
}

func clientFileAPIDepth(dir string) uint16 {
	return uint16(strings.Count(strings.Trim(dir, "/"), "/")) //nolint:gosec // Test fixture paths have bounded depth.
}

func fileRowPaths(rows []FileRow) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row.Path
	}

	return out
}
