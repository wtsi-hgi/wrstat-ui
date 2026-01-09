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
)

func TestClickHouseDatabaseInfo(t *testing.T) {
	Convey("Info counts are computed over active snapshots only", t, func() {
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

		dbch := newClickHouseDatabase(cfg, cp.conn)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		const mountPath = "/mnt/test/"

		oldUpdatedAt := time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC)
		newUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		oldSID := snapshotID(mountPath, oldUpdatedAt)
		newSID := snapshotID(mountPath, newUpdatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) VALUES (?, ?, ?, ?)",
			mountPath,
			time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC),
			oldSID,
			oldUpdatedAt,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dguta (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			mountPath,
			oldSID,
			mountPath,
			uint32(1),
			uint32(1),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(1),
			uint64(1),
			int64(1),
			int64(1),
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_children (mount_path, snapshot_id, parent_dir, child) VALUES (?, ?, ?, ?)",
			mountPath,
			oldSID,
			mountPath,
			mountPath+"oldchild",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) VALUES (?, ?, ?, ?)",
			mountPath,
			time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC),
			newSID,
			newUpdatedAt,
		), ShouldBeNil)

		atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
		mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

		dirA := mountPath
		dirB := mountPath + "a/"

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dguta (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			mountPath,
			newSID,
			dirA,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(2),
			uint64(123),
			int64(10),
			int64(20),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dguta (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			mountPath,
			newSID,
			dirA,
			uint32(8),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(1),
			uint64(1),
			int64(11),
			int64(21),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_dguta (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, "+
				"atime_min, mtime_max, atime_buckets, mtime_buckets) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			mountPath,
			newSID,
			dirB,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(3),
			uint64(3),
			int64(12),
			int64(22),
			atimeBuckets,
			mtimeBuckets,
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_children (mount_path, snapshot_id, parent_dir, child) VALUES (?, ?, ?, ?)",
			mountPath,
			newSID,
			dirA,
			mountPath+"a",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_children (mount_path, snapshot_id, parent_dir, child) VALUES (?, ?, ?, ?)",
			mountPath,
			newSID,
			dirA,
			mountPath+"b",
		), ShouldBeNil)

		So(conn.Exec(ctx,
			"INSERT INTO wrstat_children (mount_path, snapshot_id, parent_dir, child) VALUES (?, ?, ?, ?)",
			mountPath,
			newSID,
			dirB,
			mountPath+"a/c",
		), ShouldBeNil)

		info, err := dbch.Info()
		So(err, ShouldBeNil)
		So(info, ShouldNotBeNil)
		So(info.NumDirs, ShouldEqual, 2)
		So(info.NumDGUTAs, ShouldEqual, 3)
		So(info.NumParents, ShouldEqual, 2)
		So(info.NumChildren, ShouldEqual, 3)
	})
}
