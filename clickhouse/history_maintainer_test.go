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
	"sort"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	testInsertBasedirsHistoryStmt = "INSERT INTO wrstat_basedirs_history (mount_path, gid, date, " +
		"usage_size, quota_size, usage_inodes, quota_inodes) VALUES (?, ?, ?, ?, ?, ?, ?)"
	testSelectBasedirsHistoryRowsQuery = "SELECT gid, mount_path FROM wrstat_basedirs_history ORDER BY gid"
)

func TestClickHouseHistoryMaintainer(t *testing.T) {
	Convey("NewHistoryMaintainer can find and clean invalid history rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		m, err := NewHistoryMaintainer(cfg)
		So(err, ShouldBeNil)
		So(m, ShouldNotBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		keepPrefix := "/mnt/keep/"
		keepMount := "/mnt/keep/a/"
		dropMount := "/mnt/drop/b/"

		So(conn.Exec(ctx,
			testInsertBasedirsHistoryStmt,
			keepMount,
			uint32(7),
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			uint64(1),
			uint64(2),
			uint64(3),
			uint64(4),
		), ShouldBeNil)

		So(conn.Exec(ctx,
			testInsertBasedirsHistoryStmt,
			dropMount,
			uint32(9),
			time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			uint64(10),
			uint64(20),
			uint64(30),
			uint64(40),
		), ShouldBeNil)

		issues, err := m.FindInvalidHistory(keepPrefix)
		So(err, ShouldBeNil)
		So(issues, ShouldNotBeNil)
		So(len(issues), ShouldEqual, 1)
		So(issues[0].GID, ShouldEqual, 9)
		So(issues[0].MountPath, ShouldEqual, dropMount)

		So(m.CleanHistoryForMount(keepPrefix), ShouldBeNil)

		rows, err := conn.Query(ctx, testSelectBasedirsHistoryRowsQuery)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		types := make([]string, 0, 2)

		for rows.Next() {
			var (
				gid       uint32
				mountPath string
			)

			So(rows.Scan(&gid, &mountPath), ShouldBeNil)
			types = append(types, mountPath)
		}

		sort.Strings(types)
		So(types, ShouldResemble, []string{keepMount})
	})
}
