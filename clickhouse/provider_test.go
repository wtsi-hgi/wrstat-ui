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
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

func TestOpenProviderPolling(t *testing.T) {
	Convey("OpenProvider polls wrstat_mounts_active and calls OnUpdate on change", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 50 * time.Millisecond

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		So(p.Tree(), ShouldNotBeNil)

		updateCh := make(chan struct{}, 1)

		p.OnUpdate(func() {
			select {
			case updateCh <- struct{}{}:
			default:
			}
		})

		// Let the poller establish a baseline.
		time.Sleep(2 * cfg.PollInterval)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		mountPath := "/mnt/test/"
		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		)
		So(err, ShouldBeNil)

		select {
		case <-updateCh:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate")
		}
	})

	Convey("OpenProvider does not poll when PollInterval <= 0", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		So(p.Tree(), ShouldNotBeNil)

		updateCh := make(chan struct{}, 1)

		p.OnUpdate(func() { updateCh <- struct{}{} })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		mountPath := "/mnt/test/"
		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		)
		So(err, ShouldBeNil)

		select {
		case <-updateCh:
			t.Fatalf("OnUpdate should not be called when polling is disabled")
		case <-time.After(200 * time.Millisecond):
			// ok
		}
	})
}

func TestOpenProviderBaseDirs(t *testing.T) {
	Convey("OpenProvider returns a basedirs reader", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		cfg.OwnersCSVPath = ownersPath

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		bd := p.BaseDirs()
		So(bd, ShouldNotBeNil)

		mt, err := bd.MountTimestamps()
		So(err, ShouldBeNil)
		So(mt, ShouldNotBeNil)
		So(len(mt), ShouldEqual, 0)

		gu, err := bd.GroupUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(gu, ShouldNotBeNil)
		So(len(gu), ShouldEqual, 0)

		uu, err := bd.UserUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(uu, ShouldNotBeNil)
		So(len(uu), ShouldEqual, 0)
	})
}
