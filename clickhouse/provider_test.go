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
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

const providerTestMountPath = "/mnt/test/"

var (
	errProviderTestErr1 = errors.New("provider test err1")
	errProviderTestErr2 = errors.New("provider test err2")
	errProviderTestErr3 = errors.New("provider test err3")
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

func TestOpenProviderUpdatePinsClickHouseSnapshots(t *testing.T) {
	Convey("OpenProvider keeps old ClickHouse-backed readers on their old snapshot during callback", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 50 * time.Millisecond

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider, ShouldNotBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		oldUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		newUpdatedAt := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

		So(insertProviderSnapshot(ctx, conn, providerTestMountPath, oldUpdatedAt, 2, 100, 1000), ShouldBeNil)

		p, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		Reset(func() { So(p.Close(), ShouldBeNil) })

		oldTree := p.Tree()
		oldBD := p.BaseDirs()

		So(oldTree, ShouldNotBeNil)
		So(oldBD, ShouldNotBeNil)

		oldInfo, err := oldTree.DirInfo(providerTestMountPath, &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(oldInfo, ShouldNotBeNil)
		So(oldInfo.Current.Count, ShouldEqual, 2)

		oldUsage, err := oldBD.GroupUsage(db.DGUTAgeAll)
		So(err, ShouldBeNil)
		So(len(oldUsage), ShouldEqual, 1)
		So(oldUsage[0].UsageSize, ShouldEqual, 1000)

		type providerSnapshotObservation struct {
			oldInfo     *db.DirInfo
			newInfo     *db.DirInfo
			oldInfoErr  error
			newInfoErr  error
			oldUsage    []*basedirs.Usage
			newUsage    []*basedirs.Usage
			oldUsageErr error
			newUsageErr error
			oldMounts   map[string]time.Time
			newMounts   map[string]time.Time
			oldMountErr error
			newMountErr error
		}

		observed := make(chan providerSnapshotObservation, 1)
		callbackStarted := make(chan struct{}, 1)
		allowCallbackReturn := make(chan struct{})
		callbackDone := make(chan struct{}, 1)

		p.OnUpdate(func() {
			obs := providerSnapshotObservation{}

			obs.oldInfo, obs.oldInfoErr = oldTree.DirInfo(
				providerTestMountPath,
				&db.Filter{Age: db.DGUTAgeAll},
			)
			obs.newInfo, obs.newInfoErr = p.Tree().DirInfo(
				providerTestMountPath,
				&db.Filter{Age: db.DGUTAgeAll},
			)
			obs.oldUsage, obs.oldUsageErr = oldBD.GroupUsage(db.DGUTAgeAll)
			obs.newUsage, obs.newUsageErr = p.BaseDirs().GroupUsage(db.DGUTAgeAll)
			obs.oldMounts, obs.oldMountErr = oldBD.MountTimestamps()
			obs.newMounts, obs.newMountErr = p.BaseDirs().MountTimestamps()

			observed <- obs

			callbackStarted <- struct{}{}

			<-allowCallbackReturn

			callbackDone <- struct{}{}
		})

		// Let the poller establish a baseline for the old snapshot first.
		time.Sleep(2 * cfg.PollInterval)

		So(insertProviderSnapshot(ctx, conn, providerTestMountPath, newUpdatedAt, 5, 500, 2000), ShouldBeNil)

		select {
		case <-callbackStarted:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate to start")
		}

		var got providerSnapshotObservation
		select {
		case got = <-observed:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for snapshot observation")
		}

		mountKey := strings.ReplaceAll(providerTestMountPath, "/", "／")

		So(got.oldInfoErr, ShouldBeNil)
		So(got.newInfoErr, ShouldBeNil)
		So(got.oldInfo, ShouldNotBeNil)
		So(got.newInfo, ShouldNotBeNil)
		So(got.oldInfo.Current.Count, ShouldEqual, 2)
		So(got.oldInfo.Current.Size, ShouldEqual, 100)
		So(got.oldInfo.Current.Modtime, ShouldResemble, oldUpdatedAt)
		So(got.newInfo.Current.Count, ShouldEqual, 5)
		So(got.newInfo.Current.Size, ShouldEqual, 500)
		So(got.newInfo.Current.Modtime, ShouldResemble, newUpdatedAt)

		So(got.oldUsageErr, ShouldBeNil)
		So(got.newUsageErr, ShouldBeNil)
		So(len(got.oldUsage), ShouldEqual, 1)
		So(len(got.newUsage), ShouldEqual, 1)
		So(got.oldUsage[0].UsageSize, ShouldEqual, 1000)
		So(got.newUsage[0].UsageSize, ShouldEqual, 2000)

		So(got.oldMountErr, ShouldBeNil)
		So(got.newMountErr, ShouldBeNil)
		So(got.oldMounts[mountKey], ShouldResemble, oldUpdatedAt)
		So(got.newMounts[mountKey], ShouldResemble, newUpdatedAt)

		close(allowCallbackReturn)

		select {
		case <-callbackDone:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for OnUpdate to finish")
		}

		deadline := time.Now().Add(2 * time.Second)

		var (
			closedInfoErr  error
			closedUsageErr error
			closedMountErr error
		)

		for time.Now().Before(deadline) {
			_, closedInfoErr = oldTree.DirInfo(
				providerTestMountPath,
				&db.Filter{Age: db.DGUTAgeAll},
			)
			_, closedUsageErr = oldBD.GroupUsage(db.DGUTAgeAll)
			_, closedMountErr = oldBD.MountTimestamps()

			closedReaders := errors.Is(closedInfoErr, errReaderClosed) &&
				errors.Is(closedUsageErr, errReaderClosed) &&
				errors.Is(closedMountErr, errReaderClosed)

			if closedReaders {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		So(errors.Is(closedInfoErr, errReaderClosed), ShouldBeTrue)
		So(errors.Is(closedUsageErr, errReaderClosed), ShouldBeTrue)
		So(errors.Is(closedMountErr, errReaderClosed), ShouldBeTrue)
	})
}

func insertProviderSnapshot(
	ctx context.Context,
	conn providerExecConn,
	mountPath string,
	updatedAt time.Time,
	count, size, usageSize uint64,
) error {
	sid := snapshotID(mountPath, updatedAt)
	atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
	mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}
	basedir := mountPath + "project/"
	quotaSize := usageSize * 2
	quotaInodes := count * 2

	if err := conn.Exec(
		ctx,
		testInsertDGUTAStmt,
		mountPath,
		sid,
		mountPath,
		uint32(7),
		uint32(9),
		uint16(db.DGUTAFileTypeBam),
		uint8(db.DGUTAgeAll),
		count,
		size,
		int64(10),
		int64(20),
		atimeBuckets,
		mtimeBuckets,
	); err != nil {
		return err
	}

	if err := conn.Exec(
		ctx,
		insertBasedirsGroupUsageQuery,
		mountPath,
		sid.String(),
		uint32(7),
		basedir,
		uint8(db.DGUTAgeAll),
		[]uint32{9},
		usageSize,
		quotaSize,
		count,
		quotaInodes,
		updatedAt,
		unixEpochUTC(),
		unixEpochUTC(),
	); err != nil {
		return err
	}

	return conn.Exec(
		ctx,
		testInsertMountStmt,
		mountPath,
		time.Now().UTC(),
		sid,
		updatedAt,
	)
}

func TestProviderRefreshCaptureFailureKeepsPublishedReaders(t *testing.T) {
	Convey("refresh capture failures keep the published readers pinned in place", t, func() {
		oldDB := &providerSwapTestDB{}
		oldBD := &providerSwapTestBD{}
		oldTree := db.NewTree(oldDB)

		var buildCalled atomic.Bool

		cp := &chProvider{
			db:                 oldDB,
			tree:               oldTree,
			bd:                 oldBD,
			errCh:              make(chan struct{}, 1),
			currentFingerprint: "old-fingerprint",
			buildReaders: func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error) {
				buildCalled.Store(true)

				dbImpl := &providerSwapTestDB{}
				bdImpl := &providerSwapTestBD{}

				return dbImpl, db.NewTree(dbImpl), bdImpl, nil
			},
			captureSnapshot: func(context.Context) (*activeMountsSnapshot, string, error) {
				return nil, "", errProviderTestErr1
			},
		}

		var updateCalled atomic.Bool

		errorCalled := make(chan error, 1)

		cp.OnUpdate(func() {
			updateCalled.Store(true)
		})
		cp.OnError(func(err error) {
			errorCalled <- err
		})

		cp.queueUpdate("new-fingerprint")
		cp.drainUpdates(context.Background())
		cp.drainErrors(context.Background())

		So(buildCalled.Load(), ShouldBeFalse)
		So(updateCalled.Load(), ShouldBeFalse)
		So(cp.db, ShouldEqual, oldDB)
		So(cp.tree, ShouldEqual, oldTree)
		So(cp.bd, ShouldEqual, oldBD)
		So(cp.currentPublishedFingerprint(), ShouldEqual, "old-fingerprint")
		So(oldDB.closed.Load(), ShouldBeFalse)
		So(oldBD.closed.Load(), ShouldBeFalse)

		select {
		case err := <-errorCalled:
			So(err, ShouldEqual, errProviderTestErr1)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for OnError")
		}
	})
}

func TestProviderCallbacksRunOnFreshGoroutine(t *testing.T) {
	Convey("OnUpdate and OnError callbacks run on a fresh goroutine", t, func() {
		callerID := currentGoroutineID()

		cp := &chProvider{
			captureSnapshot: func(context.Context) (*activeMountsSnapshot, string, error) {
				return &activeMountsSnapshot{}, "", nil
			},
			buildReaders: func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error) {
				dbImpl := &providerSwapTestDB{}
				bdImpl := &providerSwapTestBD{}

				return dbImpl, db.NewTree(dbImpl), bdImpl, nil
			},
		}

		updateGID := make(chan string, 1)

		cp.swapReadersAndInvoke(context.Background(), "", func() {
			updateGID <- currentGoroutineID()
		})

		So(<-updateGID, ShouldNotEqual, callerID)

		type callbackInfo struct {
			gid string
			err error
		}

		errorInfo := make(chan callbackInfo, 1)

		cp.OnError(func(err error) {
			errorInfo <- callbackInfo{gid: currentGoroutineID(), err: err}
		})
		cp.queueError(errProviderTestErr1)
		cp.drainErrors(context.Background())

		got := <-errorInfo
		So(got.err, ShouldEqual, errProviderTestErr1)
		So(got.gid, ShouldNotEqual, callerID)
	})
}

func currentGoroutineID() string {
	buf := make([]byte, 64)
	n := runtime.Stack(buf, false)

	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return ""
	}

	return fields[1]
}

func TestProviderCloseStopsPollingPromptly(t *testing.T) {
	Convey("Close stops polling promptly between ticks", t, func() {
		conn := &providerCloseTestConn{firstQuery: make(chan struct{}, 1)}
		p := &chProvider{
			cfg: Config{
				PollInterval: time.Second,
				QueryTimeout: time.Second,
			},
			conn: conn,
		}

		p.startPolling()

		select {
		case <-conn.firstQuery:
			// ok
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for initial poll")
		}

		closeDone := make(chan error, 1)

		go func() {
			closeDone <- p.Close()
		}()

		returnedPromptly := false

		select {
		case err := <-closeDone:
			So(err, ShouldBeNil)

			returnedPromptly = true
		case <-time.After(200 * time.Millisecond):
		}

		So(returnedPromptly, ShouldBeTrue)

		if !returnedPromptly {
			select {
			case err := <-closeDone:
				So(err, ShouldBeNil)
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for Close to return")
			}
		}

		So(conn.closed.Load(), ShouldBeTrue)
	})
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

func (r *providerSwapTestBD) SetMountPoints(mountpoints []string) {}

func (r *providerSwapTestBD) SetCachedGroup(gid uint32, name string) {}

func (r *providerSwapTestBD) SetCachedUser(uid uint32, name string) {}

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
		cfg.MountPoints = []string{providerTestMountPath}

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

	Convey("OpenProvider fails fast on invalid owners CSV", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		ownersPath := t.TempDir() + "/owners.csv"
		So(os.WriteFile(ownersPath, []byte("bad,line,format\n"), 0o600), ShouldBeNil)

		cfg.OwnersCSVPath = ownersPath

		p, err := OpenProvider(cfg)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "failed to parse owners csv")
		So(err.Error(), ShouldContainSubstring, basedirs.ErrInvalidOwnersFile.Error())
		So(p, ShouldBeNil)
	})

	Convey("OpenProvider fails fast on mount autodiscovery errors", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		origDiscoverMountPoints := discoverMountPoints

		Reset(func() { discoverMountPoints = origDiscoverMountPoints })

		discoverMountPoints = func() (basedirs.MountPoints, error) {
			return nil, errProviderTestErr2
		}

		p, err := OpenProvider(cfg)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "failed to auto-discover mountpoints")
		So(err.Error(), ShouldContainSubstring, errProviderTestErr2.Error())
		So(p, ShouldBeNil)
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

		cp.buildReaders = func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error) {
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

			return dbImpl, tree, bdImpl, nil
		}

		liveBD, liveDB := cp.detachReaders()
		cp.closeOldReaders(liveDB, liveBD)

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

type providerCloseTestConn struct {
	bootstrapTestConn

	queries    atomic.Int32
	firstQuery chan struct{}
}

func (c *providerCloseTestConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.queries.Add(1) == 1 {
		select {
		case c.firstQuery <- struct{}{}:
		default:
		}
	}

	return &findByGlobEmptyRows{}, nil
}

type providerExecConn interface {
	Exec(ctx context.Context, query string, args ...any) error
}
