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
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

type providerSwapTestDB struct {
	closed atomic.Bool
}

func (d *providerSwapTestDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	return &db.DirSummary{}, nil
}

func (d *providerSwapTestDB) Children(dir string) ([]string, error) {
	return nil, nil
}

func (d *providerSwapTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *providerSwapTestDB) Close() error {
	d.closed.Store(true)

	return nil
}

type providerSwapTestBD struct {
	closed atomic.Bool
}

func (r *providerSwapTestBD) GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, nil
}

func (r *providerSwapTestBD) UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, nil
}

func (r *providerSwapTestBD) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, nil
}

func (r *providerSwapTestBD) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, nil
}

func (r *providerSwapTestBD) History(gid uint32, path string) ([]basedirs.History, error) {
	return nil, nil
}

func (r *providerSwapTestBD) SetMountPoints(mountpoints []string)    {}
func (r *providerSwapTestBD) SetCachedGroup(gid uint32, name string) {}
func (r *providerSwapTestBD) SetCachedUser(uid uint32, name string)  {}

func (r *providerSwapTestBD) Info() (*basedirs.DBInfo, error) {
	return &basedirs.DBInfo{}, nil
}

func (r *providerSwapTestBD) MountTimestamps() (map[string]time.Time, error) {
	return map[string]time.Time{}, nil
}

func (r *providerSwapTestBD) Close() error {
	r.closed.Store(true)

	return nil
}

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

		const mountPath = providerTestMountPath

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

		const mountPath = providerTestMountPath

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

const providerTestMountPath = "/mnt/test/"

var (
	errProviderTestErr1 = errors.New("provider test err1")
	errProviderTestErr2 = errors.New("provider test err2")
	errProviderTestErr3 = errors.New("provider test err3")
)

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

func TestOpenProviderUpdateSwapSemantics(t *testing.T) {
	Convey("OpenProvider update swap semantics", t, func() {
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

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)

		// Install a deterministic reader factory so we can observe close behaviour.
		var (
			builtDB1 *providerSwapTestDB
			builtBD1 *providerSwapTestBD
			builtDB2 *providerSwapTestDB
			builtBD2 *providerSwapTestBD
			calls    int
		)

		cp.buildReaders = func() (db.Database, *db.Tree, basedirs.Reader) {
			calls++
			dbImpl := &providerSwapTestDB{}
			bdImpl := &providerSwapTestBD{}
			tree := db.NewTree(dbImpl)

			switch calls {
			case 1:
				builtDB1, builtBD1 = dbImpl, bdImpl
			case 2:
				builtDB2, builtBD2 = dbImpl, bdImpl
			}

			return dbImpl, tree, bdImpl
		}

		oldTree := p.Tree()
		oldBD := p.BaseDirs()

		So(oldTree, ShouldNotBeNil)
		So(oldBD, ShouldNotBeNil)
		So(builtDB1, ShouldNotBeNil)
		So(builtBD1, ShouldNotBeNil)

		callbackStarted := make(chan struct{}, 1)

		type updateObserved struct {
			tree *db.Tree
			bd   basedirs.Reader
		}

		observed := make(chan updateObserved, 1)
		allowCallbackReturn := make(chan struct{})
		callbackDone := make(chan struct{}, 1)

		p.OnUpdate(func() {
			// Capture state for assertions on the main goroutine.
			observed <- updateObserved{tree: p.Tree(), bd: p.BaseDirs()}

			callbackStarted <- struct{}{}

			<-allowCallbackReturn

			callbackDone <- struct{}{}
		})

		// Trigger a mounts_active change by inserting into wrstat_mounts.
		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		const mountPath = providerTestMountPath

		updatedAt := time.Now().UTC().Truncate(time.Second)
		sid := snapshotID(mountPath, updatedAt)

		So(conn.Exec(
			ctx,
			testInsertMountStmt,
			mountPath,
			time.Now(),
			sid,
			updatedAt,
		), ShouldBeNil)

		select {
		case <-callbackStarted:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate to start")
		}

		var got updateObserved
		select {
		case got = <-observed:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate observation")
		}

		So(got.tree, ShouldNotEqual, oldTree)
		So(got.bd, ShouldNotEqual, oldBD)

		// Old readers must remain usable (and not closed) until callback returns.
		So(builtDB1.closed.Load(), ShouldBeFalse)
		So(builtBD1.closed.Load(), ShouldBeFalse)
		So(builtDB2, ShouldNotBeNil)
		So(builtBD2, ShouldNotBeNil)

		close(allowCallbackReturn)

		select {
		case <-callbackDone:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate to finish")
		}

		// After callback returns, old readers should be closed.
		deadline := time.Now().Add(2 * time.Second)

		for !builtDB1.closed.Load() || !builtBD1.closed.Load() {
			if time.Now().After(deadline) {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		So(builtDB1.closed.Load(), ShouldBeTrue)
		So(builtBD1.closed.Load(), ShouldBeTrue)
	})
}

func TestProviderOnErrorQueueAndSerialization(t *testing.T) {
	Convey("OnError callbacks are serialised and errors are not dropped", t, func() {
		cp := &chProvider{errCh: make(chan struct{}, 1)}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			defer close(done)

			cp.errorLoop(ctx)
		}()

		Reset(func() {
			cancel()

			select {
			case <-done:
				// ok
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for error loop to stop")
			}
		})

		allowFirstReturn := make(chan struct{})
		got := make(chan error, 3)

		var (
			inCallback atomic.Int32
			calls      atomic.Int32
			concurrent atomic.Bool
		)

		cp.OnError(func(err error) {
			if inCallback.Add(1) != 1 {
				concurrent.Store(true)
			}
			defer inCallback.Add(-1)

			got <- err

			if calls.Add(1) == 1 {
				<-allowFirstReturn
			}
		})

		cp.queueError(errProviderTestErr1)

		select {
		case err := <-got:
			So(err, ShouldEqual, errProviderTestErr1)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for first OnError")
		}

		// While the callback is blocked, queue multiple errors. None should be
		// dropped, and callback invocations must not overlap.
		cp.queueError(errProviderTestErr2)
		cp.queueError(errProviderTestErr3)

		close(allowFirstReturn)

		select {
		case err := <-got:
			So(err, ShouldEqual, errProviderTestErr2)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for second OnError")
		}

		select {
		case err := <-got:
			So(err, ShouldEqual, errProviderTestErr3)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for third OnError")
		}

		So(concurrent.Load(), ShouldBeFalse)
	})
}
