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
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
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
				SubDir:       "alpha",
				NumFiles:     1,
				SizeFiles:    10,
				LastModified: updatedAt,
				FileUsage:    fileUsage,
			},
			{
				SubDir:       "beta",
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
