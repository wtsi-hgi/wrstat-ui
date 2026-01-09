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
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

func TestClickHouseDGUTAWriter(t *testing.T) {
	Convey("DGUTAWriter enforces required metadata", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)
		Reset(func() { So(w.Close(), ShouldBeNil) })

		paths := internaltest.NewDirectoryPathCreator()
		err = w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/"), GUTAs: nil})
		So(err, ShouldNotBeNil)

		w.SetMountPath("/mnt/test/")
		w.SetUpdatedAt(time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC))
		err = w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/"), GUTAs: nil})
		So(err, ShouldBeNil)
	})

	Convey("DGUTAWriter switches the active snapshot on Close", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		const mountPath = "/mnt/test/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx,
			"SELECT toString(snapshot_id), updated_at FROM wrstat_mounts_active WHERE mount_path = ?",
			mountPath,
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var (
			gotSID       string
			gotUpdatedAt time.Time
		)

		So(rows.Scan(&gotSID, &gotUpdatedAt), ShouldBeNil)

		expectedSID := snapshotID(mountPath, updatedAt)
		So(gotSID, ShouldEqual, expectedSID.String())
		So(gotUpdatedAt, ShouldEqual, updatedAt)
	})

	Convey("DGUTAWriter writes dguta + children rows and supports idempotent retry", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		const mountPath = "/mnt/test/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		expectedSID := snapshotID(mountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		writeOnce := func(gid uint32, child string) {
			w, err := NewDGUTAWriter(cfg)
			So(err, ShouldBeNil)
			So(w, ShouldNotBeNil)

			w.SetMountPath(mountPath)
			w.SetUpdatedAt(updatedAt)

			err = w.Add(db.RecordDGUTA{
				Dir: dir,
				GUTAs: db.GUTAs{&db.GUTA{
					GID:         gid,
					UID:         123,
					FT:          db.DGUTAFileTypeBam,
					Age:         db.DGUTAgeA1M,
					Count:       7,
					Size:        99,
					Atime:       1,
					Mtime:       2,
					ATimeRanges: [9]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
					MTimeRanges: [9]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
				}},
				Children: []string{child},
			})
			So(err, ShouldBeNil)
			So(w.Close(), ShouldBeNil)
		}

		writeOnce(42, "/foo/")
		writeOnce(77, "/bar/")

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx,
			"SELECT gid FROM wrstat_dguta WHERE mount_path = ? AND snapshot_id = toUUID(?) AND dir = ?",
			mountPath,
			expectedSID.String(),
			"/",
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var gotGID uint32
		So(rows.Scan(&gotGID), ShouldBeNil)
		So(gotGID, ShouldEqual, 77)
		So(rows.Next(), ShouldBeFalse)

		childRows, err := conn.Query(ctx,
			"SELECT child FROM wrstat_children WHERE mount_path = ? AND snapshot_id = toUUID(?) AND parent_dir = ?",
			mountPath,
			expectedSID.String(),
			"/",
		)
		So(err, ShouldBeNil)

		defer func() { _ = childRows.Close() }()

		So(childRows.Next(), ShouldBeTrue)

		var gotChild string
		So(childRows.Scan(&gotChild), ShouldBeNil)
		So(gotChild, ShouldEqual, "/bar")
		So(childRows.Next(), ShouldBeFalse)
	})
}
