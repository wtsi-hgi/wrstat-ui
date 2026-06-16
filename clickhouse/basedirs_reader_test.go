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
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	testInsertBasedirsGroupUsageIDStmt = "INSERT INTO wrstat_basedirs_group_usage " +
		"(mount_path, snapshot_id, gid, basedir_id, basedir_external, age, uids, usage_size, quota_size, " +
		"usage_inodes, quota_inodes, mtime, date_no_space, date_no_files) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	testInsertBasedirsUserUsageIDStmt = "INSERT INTO wrstat_basedirs_user_usage " +
		"(mount_path, snapshot_id, uid, basedir_id, basedir_external, age, gids, usage_size, quota_size, " +
		"usage_inodes, quota_inodes, mtime) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	testInsertBasedirsGroupSubdirsIDStmt = "INSERT INTO wrstat_basedirs_group_subdirs " +
		"(mount_path, snapshot_id, gid, basedir_id, basedir_external, age, pos, subdir_id, subdir_external, " +
		"num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	testInsertBasedirsUserSubdirsIDStmt = "INSERT INTO wrstat_basedirs_user_subdirs " +
		"(mount_path, snapshot_id, uid, basedir_id, basedir_external, age, pos, subdir_id, subdir_external, " +
		"num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	basedirsReaderAlphaSubdir = "alpha"
	basedirsReaderBetaSubdir  = "beta"
)

func TestOpenProviderBaseDirsInfoCountsHistoryOnlyForActiveMounts(t *testing.T) {
	Convey("BaseDirs Info ignores history rows for stale mounts", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		const (
			activeMount = "/mnt/active/"
			staleMount  = "/mnt/stale/"
		)

		updatedAt := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
		So(conn.Exec(
			ctx,
			testInsertMountStmt,
			activeMount,
			time.Now().UTC(),
			snapshotID(activeMount, updatedAt),
			updatedAt,
		), ShouldBeNil)

		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			activeMount,
			uint32(7),
			updatedAt.Add(-2*time.Hour),
			uint64(10),
			uint64(20),
			uint64(1),
			uint64(2),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			activeMount,
			uint32(8),
			updatedAt.Add(-time.Hour),
			uint64(11),
			uint64(21),
			uint64(2),
			uint64(3),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			activeMount,
			uint32(8),
			updatedAt,
			uint64(12),
			uint64(22),
			uint64(3),
			uint64(4),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			staleMount,
			uint32(9),
			updatedAt.Add(-2*time.Hour),
			uint64(13),
			uint64(23),
			uint64(4),
			uint64(5),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			staleMount,
			uint32(9),
			updatedAt.Add(-time.Hour),
			uint64(14),
			uint64(24),
			uint64(5),
			uint64(6),
		), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		info, err := p.BaseDirs().Info()
		So(err, ShouldBeNil)
		So(info, ShouldNotBeNil)
		So(info.GroupMountCombos, ShouldEqual, 2)
		So(info.GroupHistories, ShouldEqual, 3)
	})
}

type basedirsReaderDirFixture struct {
	id            uint32
	parentID      uint32
	subtreeEnd    uint32
	depth         uint16
	name          string
	fullPath      string
	childDirCount uint32
}

func TestOpenProviderBaseDirsReaderG1NumericIDs(t *testing.T) {
	Convey("G1 BaseDirs readers preserve baseline paths and ordering from catalog IDs", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testMountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 5, 7, 10, 30, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		baseDir := testMountPath + "project/"
		gid := uint32(7)
		uid := uint32(17)

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)
		insertBasedirsReaderDirs(ctx, conn, sid, []basedirsReaderDirFixture{
			{id: 10, parentID: 1, subtreeEnd: 13, depth: 3, name: "project", fullPath: baseDir, childDirCount: 2},
			{
				id: 11, parentID: 10, subtreeEnd: 12, depth: 4,
				name: basedirsReaderAlphaSubdir, fullPath: baseDir + basedirsReaderAlphaSubdir + "/",
				childDirCount: 0,
			},
			{
				id: 12, parentID: 10, subtreeEnd: 13, depth: 4,
				name: basedirsReaderBetaSubdir, fullPath: baseDir + basedirsReaderBetaSubdir + "/",
				childDirCount: 0,
			},
		})

		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			testMountPath,
			gid,
			updatedAt.Add(-time.Hour),
			uint64(20),
			uint64(100),
			uint64(2),
			uint64(10),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			testMountPath,
			gid,
			updatedAt,
			uint64(40),
			uint64(100),
			uint64(4),
			uint64(10),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupUsageIDStmt,
			testMountPath,
			sid,
			gid,
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			[]uint32{uid},
			uint64(40),
			uint64(100),
			uint64(4),
			uint64(10),
			updatedAt,
			updatedAt.Add(24*time.Hour),
			updatedAt.Add(48*time.Hour),
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsUserUsageIDStmt,
			testMountPath,
			sid,
			uid,
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			[]uint32{gid},
			uint64(41),
			uint64(101),
			uint64(5),
			uint64(11),
			updatedAt.Add(time.Minute),
		), ShouldBeNil)

		fileUsage := map[uint16]uint64{uint16(db.DGUTAFileTypeBam): 3}
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupSubdirsIDStmt,
			testMountPath,
			sid,
			gid,
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			uint32(0),
			uint32(11),
			"",
			uint64(1),
			uint64(10),
			updatedAt,
			fileUsage,
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupSubdirsIDStmt,
			testMountPath,
			sid,
			gid,
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			uint32(1),
			uint32(12),
			"",
			uint64(2),
			uint64(20),
			updatedAt.Add(time.Minute),
			map[uint16]uint64{uint16(db.DGUTAFileTypeCram): 4},
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsUserSubdirsIDStmt,
			testMountPath,
			sid,
			uid,
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			uint32(0),
			uint32(11),
			"",
			uint64(1),
			uint64(10),
			updatedAt,
			fileUsage,
		), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		reader := p.BaseDirs()
		So(reader, ShouldNotBeNil)

		groupUsage, err := reader.GroupUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(groupUsage, ShouldHaveLength, 1)
		So(groupUsage[0].GID, ShouldEqual, gid)
		So(groupUsage[0].BaseDir, ShouldEqual, baseDir)
		So(groupUsage[0].UIDs, ShouldResemble, []uint32{uid})
		So(groupUsage[0].UsageSize, ShouldEqual, 40)
		So(groupUsage[0].DateNoSpace.Unix(), ShouldEqual, updatedAt.Add(24*time.Hour).Unix())
		So(groupUsage[0].DateNoFiles.Unix(), ShouldEqual, updatedAt.Add(48*time.Hour).Unix())

		userUsage, err := reader.UserUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(userUsage, ShouldHaveLength, 1)
		So(userUsage[0].UID, ShouldEqual, uid)
		So(userUsage[0].BaseDir, ShouldEqual, baseDir)
		So(userUsage[0].GIDs, ShouldResemble, []uint32{gid})
		So(userUsage[0].UsageSize, ShouldEqual, 41)

		groupSubdirs, err := reader.GroupSubDirs(gid, baseDir, db.DGUTAgeAll)
		So(err, ShouldBeNil)
		assertBaseDirsSubDirs(groupSubdirs, []*basedirs.SubDir{
			{
				SubDir:       basedirsReaderAlphaSubdir,
				NumFiles:     1,
				SizeFiles:    10,
				LastModified: updatedAt,
				FileUsage:    basedirs.UsageBreakdownByType{db.DGUTAFileTypeBam: 3},
			},
			{
				SubDir:       basedirsReaderBetaSubdir,
				NumFiles:     2,
				SizeFiles:    20,
				LastModified: updatedAt.Add(time.Minute),
				FileUsage:    basedirs.UsageBreakdownByType{db.DGUTAFileTypeCram: 4},
			},
		})

		userSubdirs, err := reader.UserSubDirs(uid, baseDir, db.DGUTAgeAll)
		So(err, ShouldBeNil)
		assertBaseDirsSubDirs(userSubdirs, []*basedirs.SubDir{
			{
				SubDir:       basedirsReaderAlphaSubdir,
				NumFiles:     1,
				SizeFiles:    10,
				LastModified: updatedAt,
				FileUsage:    basedirs.UsageBreakdownByType{db.DGUTAFileTypeBam: 3},
			},
		})

		history, err := reader.History(gid, baseDir)
		So(err, ShouldBeNil)
		So(history, ShouldHaveLength, 2)
		So(history[0].UsageSize, ShouldEqual, 20)
		So(history[1].UsageSize, ShouldEqual, 40)
	})
}

func TestOpenProviderBaseDirsReaderG1Readiness(t *testing.T) {
	Convey("G1 BaseDirs readiness fails for an unresolved active basedir", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testMountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 5, 7, 11, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupUsageIDStmt,
			testMountPath,
			sid,
			uint32(7),
			uint32(0),
			testMountPath+"missing/",
			uint8(db.DGUTAgeAll),
			[]uint32{},
			uint64(1),
			uint64(2),
			uint64(3),
			uint64(4),
			updatedAt,
			updatedAt,
			updatedAt,
		), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(p, ShouldBeNil)
		So(errors.Is(err, ErrIDUnresolved), ShouldBeTrue)
	})

	Convey("G1 BaseDirs readiness fails for an unresolved active subdir", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testMountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 5, 7, 11, 30, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		baseDir := testMountPath + "project/"
		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)
		insertBasedirsReaderDirs(ctx, conn, sid, []basedirsReaderDirFixture{
			{id: 10, parentID: 1, subtreeEnd: 11, depth: 3, name: "project", fullPath: baseDir, childDirCount: 0},
		})
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupSubdirsIDStmt,
			testMountPath,
			sid,
			uint32(7),
			uint32(10),
			"",
			uint8(db.DGUTAgeAll),
			uint32(0),
			uint32(0),
			"missing",
			uint64(1),
			uint64(2),
			updatedAt,
			map[uint16]uint64{},
		), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(p, ShouldBeNil)
		So(errors.Is(err, ErrIDUnresolved), ShouldBeTrue)
	})
}

func insertBasedirsReaderDirs(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	sid string,
	dirs []basedirsReaderDirFixture,
) {
	for _, dir := range dirs {
		So(conn.Exec(
			ctx,
			testInsertFileDirStmt,
			testMountPath,
			sid,
			dir.id,
			dir.parentID,
			dir.subtreeEnd,
			dir.depth,
			dir.name,
			dir.fullPath,
			dir.childDirCount,
			uint32(0),
			catalogPathHash(dir.fullPath),
		), ShouldBeNil)
	}
}

func TestOpenProviderBaseDirsReaderG1ExternalFallback(t *testing.T) {
	Convey("G1 BaseDirs readers return external fallback paths absent from the catalog", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testMountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		externalBase := "/archive/offline/project/"

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupUsageIDStmt,
			testMountPath,
			sid,
			uint32(7),
			uint32(0),
			externalBase,
			uint8(db.DGUTAgeAll),
			[]uint32{17},
			uint64(50),
			uint64(100),
			uint64(5),
			uint64(10),
			updatedAt,
			updatedAt,
			updatedAt,
		), ShouldBeNil)
		So(conn.Exec(
			ctx,
			testInsertBasedirsGroupSubdirsIDStmt,
			testMountPath,
			sid,
			uint32(7),
			uint32(0),
			externalBase,
			uint8(db.DGUTAgeAll),
			uint32(0),
			uint32(0),
			"cold",
			uint64(3),
			uint64(30),
			updatedAt,
			map[uint16]uint64{uint16(db.DGUTAFileTypeOther): 8},
		), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		reader := p.BaseDirs()
		groupUsage, err := reader.GroupUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(groupUsage, ShouldHaveLength, 1)
		So(groupUsage[0].BaseDir, ShouldEqual, externalBase)
		So(groupUsage[0].UsageSize, ShouldEqual, 50)

		subdirs, err := reader.GroupSubDirs(7, externalBase, db.DGUTAgeAll)
		So(err, ShouldBeNil)
		assertBaseDirsSubDirs(subdirs, []*basedirs.SubDir{{
			SubDir:       "cold",
			NumFiles:     3,
			SizeFiles:    30,
			LastModified: updatedAt,
			FileUsage:    basedirs.UsageBreakdownByType{db.DGUTAFileTypeOther: 8},
		}})
	})
}

func TestOpenProviderBaseDirsReadersPreserveImportedRows(t *testing.T) {
	Convey("BaseDirs readers return usage, subdirs, and history for the active imported snapshot", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{testMountPath}

		updatedAt := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		gid := uint32(7)
		uid := uint32(17)

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)
		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			basedirs.History{
				Date: updatedAt.Add(-24 * time.Hour), UsageSize: 20,
				QuotaSize: 100, UsageInodes: 2, QuotaInodes: 10,
			},
		), ShouldBeNil)
		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			basedirs.History{
				Date: updatedAt, UsageSize: 40,
				QuotaSize: 100, UsageInodes: 4, QuotaInodes: 10,
			},
		), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{uid},
			UsageSize:   40,
			QuotaSize:   100,
			UsageInodes: 4,
			QuotaInodes: 10,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeAll,
		}), ShouldBeNil)
		So(store.PutUserUsage(&basedirs.Usage{
			UID:         uid,
			BaseDir:     basedirsStoreTestBaseDir,
			GIDs:        []uint32{gid},
			UsageSize:   41,
			QuotaSize:   101,
			UsageInodes: 5,
			QuotaInodes: 11,
			Mtime:       updatedAt.Add(time.Minute),
			Age:         db.DGUTAgeAll,
		}), ShouldBeNil)

		fileUsage := basedirs.UsageBreakdownByType{db.DGUTAFileTypeBam: 3}
		subdirs := []*basedirs.SubDir{
			{
				SubDir:       basedirsReaderAlphaSubdir,
				NumFiles:     1,
				SizeFiles:    10,
				LastModified: updatedAt,
				FileUsage:    fileUsage,
			},
			{
				SubDir:       basedirsReaderBetaSubdir,
				NumFiles:     2,
				SizeFiles:    20,
				LastModified: updatedAt.Add(time.Minute),
				FileUsage:    basedirs.UsageBreakdownByType{db.DGUTAFileTypeCram: 4},
			},
		}
		So(store.PutGroupSubDirs(
			basedirs.SubDirKey{ID: gid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeAll},
			subdirs,
		), ShouldBeNil)
		So(store.PutUserSubDirs(
			basedirs.SubDirKey{ID: uid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeAll},
			subdirs,
		), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		reader := p.BaseDirs()
		So(reader, ShouldNotBeNil)

		groupUsage, err := reader.GroupUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(groupUsage, ShouldHaveLength, 1)
		So(groupUsage[0].GID, ShouldEqual, gid)
		So(groupUsage[0].UIDs, ShouldResemble, []uint32{uid})
		So(groupUsage[0].BaseDir, ShouldEqual, basedirsStoreTestBaseDir)
		So(groupUsage[0].UsageSize, ShouldEqual, 40)
		So(groupUsage[0].QuotaSize, ShouldEqual, 100)

		userUsage, err := reader.UserUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(userUsage, ShouldHaveLength, 1)
		So(userUsage[0].UID, ShouldEqual, uid)
		So(userUsage[0].GIDs, ShouldResemble, []uint32{gid})
		So(userUsage[0].UsageSize, ShouldEqual, 41)

		groupSubdirs, err := reader.GroupSubDirs(gid, basedirsStoreTestBaseDir, db.DGUTAgeAll)
		So(err, ShouldBeNil)
		assertBaseDirsSubDirs(groupSubdirs, subdirs)

		userSubdirs, err := reader.UserSubDirs(uid, basedirsStoreTestBaseDir, db.DGUTAgeAll)
		So(err, ShouldBeNil)
		assertBaseDirsSubDirs(userSubdirs, subdirs)

		history, err := reader.History(gid, testMountPath+"project")
		So(err, ShouldBeNil)
		So(history, ShouldHaveLength, 2)
		So(history[0].UsageSize, ShouldEqual, 20)
		So(history[1].UsageSize, ShouldEqual, 40)
	})
}

func assertBaseDirsSubDirs(got, expected []*basedirs.SubDir) {
	So(got, ShouldHaveLength, len(expected))

	for i, want := range expected {
		So(got[i].SubDir, ShouldEqual, want.SubDir)
		So(got[i].NumFiles, ShouldEqual, want.NumFiles)
		So(got[i].SizeFiles, ShouldEqual, want.SizeFiles)
		So(got[i].LastModified.Unix(), ShouldEqual, want.LastModified.Unix())
		So(got[i].FileUsage, ShouldHaveLength, len(want.FileUsage))

		for fileType, usage := range want.FileUsage {
			So(got[i].FileUsage[fileType], ShouldEqual, usage)
		}
	}
}
