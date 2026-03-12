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
