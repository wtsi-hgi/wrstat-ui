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
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
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

var errProviderMaintenanceRefreshPrimary = errors.New("primary connection used for maintenance refresh")

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

		So(insertProviderSnapshot(ctx, conn, oldUpdatedAt, 2, 100, 1000), ShouldBeNil)

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

		So(insertProviderSnapshot(ctx, conn, newUpdatedAt, 5, 500, 2000), ShouldBeNil)

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
	updatedAt time.Time,
	count, size, usageSize uint64,
) error {
	const mountPath = providerTestMountPath

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

func TestOpenProviderTreeSummaryRefreshFailure(t *testing.T) {
	Convey("startup schedules mount dir projection refresh without synchronous backfill", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 100 * time.Millisecond
		cfg.PollInterval = 0
		cfg.MountPoints = []string{providerTestMountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)
		providerOwnsConn := false

		Reset(func() {
			if !providerOwnsConn {
				So(conn.Close(), ShouldBeNil)
			}
		})

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderSnapshot(ctx, conn, updatedAt, 2, 100, 1000), ShouldBeNil)

		refreshConn := &mountDirProjectionTimeoutThenMaintenanceConn{Conn: conn}
		started := time.Now()

		p, err := openProviderWithConnector(cfg, func(Config) (ch.Conn, error) {
			return refreshConn, nil
		})
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		So(time.Since(started), ShouldBeLessThan, time.Second)

		providerOwnsConn = true

		Reset(func() { So(p.Close(), ShouldBeNil) })

		messenger, ok := p.(interface{ OnMessage(cb func(msg string)) })
		So(ok, ShouldBeTrue)

		gotMessage := make(chan string, 32)

		messenger.OnMessage(func(msg string) {
			gotMessage <- msg
		})

		msg := waitForProviderMessage(t, gotMessage, "active mount dir projection refresh scheduled asynchronously")
		So(msg, ShouldContainSubstring, "active_mounts=1")

		msg = waitForProviderMessage(t, gotMessage, "active mount dir projection refresh started")
		So(msg, ShouldContainSubstring, "active_mounts=1")

		msg = waitForProviderMessage(t, gotMessage, "active mount dir projection refresh mount started")
		So(msg, ShouldContainSubstring, "mount_index=1")
		So(msg, ShouldContainSubstring, "total_mounts=1")
		So(msg, ShouldContainSubstring, fmt.Sprintf("mount_path=%q", providerTestMountPath))

		msg = waitForProviderMessage(
			t,
			gotMessage,
			"active mount dir projection refresh phase started",
			"phase=dir_summary",
		)
		So(msg, ShouldContainSubstring, "mount_index=1")

		msg = waitForProviderMessage(
			t,
			gotMessage,
			"active mount dir projection refresh phase completed",
			"phase=dir_summary",
		)
		So(msg, ShouldContainSubstring, "duration=")

		msg = waitForProviderMessage(
			t,
			gotMessage,
			"active mount dir projection refresh phase started",
			"phase=dguta_vector",
		)
		So(msg, ShouldContainSubstring, "mount_index=1")

		msg = waitForProviderMessage(t, gotMessage, "active mount dir projection refresh mount completed")
		So(msg, ShouldContainSubstring, "duration=")

		msg = waitForProviderMessage(t, gotMessage, "active mount dir projection refresh completed")
		So(msg, ShouldContainSubstring, "active_mounts=1")

		So(refreshConn.inlineProjectionFailures(), ShouldEqual, 0)
		So(refreshConn.maintenanceProjectionInserts(), ShouldBeGreaterThan, 0)

		readinessBefore := refreshConn.mountDirReadinessQueryCount()
		vectorBefore := refreshConn.mountDirVectorQueryCount()

		di, err := p.Tree().DirInfo(providerTestMountPath, &db.Filter{
			GIDs: []uint32{7},
			UIDs: []uint32{9},
			FT:   db.DGUTAFileTypeBam,
			Age:  db.DGUTAgeAll,
		})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 2)
		So(refreshConn.mountDirReadinessQueryCount(), ShouldEqual, readinessBefore)
		So(refreshConn.mountDirVectorQueryCount(), ShouldBeGreaterThan, vectorBefore)
	})

	Convey("startup schedules maintenance refresh without synchronous backfill", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 100 * time.Millisecond
		cfg.PollInterval = 0

		const (
			lustreAncestor = "/lustre/"
			mountPath      = lustreAncestor + "agentA/"
		)

		cfg.MountPoints = []string{"/", mountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)
		providerOwnsConn := false

		Reset(func() {
			if !providerOwnsConn {
				So(conn.Close(), ShouldBeNil)
			}
		})

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderAncestorSnapshot(ctx, conn, mountPath, updatedAt), ShouldBeNil)

		refreshConn := &treeSummaryRefreshTimeoutThenMaintenanceConn{Conn: conn}
		started := time.Now()

		p, err := openProviderWithConnector(cfg, func(Config) (ch.Conn, error) {
			return refreshConn, nil
		})
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		So(time.Since(started), ShouldBeLessThan, time.Second)

		providerOwnsConn = true

		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)
		So(cp.pendingErrs, ShouldHaveLength, 0)

		messenger, ok := p.(interface{ OnMessage(cb func(msg string)) })
		So(ok, ShouldBeTrue)

		gotMessage := make(chan string, 32)

		messenger.OnMessage(func(msg string) {
			gotMessage <- msg
		})

		msg := waitForProviderMessage(t, gotMessage, "active tree summary refresh scheduled asynchronously")
		So(msg, ShouldContainSubstring, "active_mounts=1")
		So(msg, ShouldContainSubstring, "ancestor_dirs=")

		msg = waitForProviderMessage(t, gotMessage, "active tree summary refresh started")
		So(msg, ShouldContainSubstring, "active_mounts=1")
		So(msg, ShouldContainSubstring, "ancestor_dirs=")

		msg = waitForProviderMessage(t, gotMessage, "active tree summary refresh ancestor started")
		So(msg, ShouldContainSubstring, "ancestor_index=")
		So(msg, ShouldContainSubstring, "total_ancestor_dirs=")

		msg = waitForProviderMessage(
			t,
			gotMessage,
			"active tree summary refresh phase started",
			"phase=dguta_summary",
		)
		So(msg, ShouldContainSubstring, "mounts_under_dir=")

		msg = waitForProviderMessage(
			t,
			gotMessage,
			"active tree summary refresh phase completed",
			"phase=dguta_summary",
		)
		So(msg, ShouldContainSubstring, "duration=")

		msg = waitForProviderMessage(t, gotMessage, "active tree summary refresh ancestor completed")
		So(msg, ShouldContainSubstring, "duration=")

		msg = waitForProviderMessage(t, gotMessage, "active tree summary refresh completed")
		So(msg, ShouldContainSubstring, "active_mounts=1")

		fingerprint := fingerprintForMountsActive([]mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}})
		So(countRows(ctx, refreshConn,
			"SELECT count() FROM wrstat_tree_summary_sets FINAL WHERE fingerprint = ?",
			fingerprint,
		), ShouldEqual, 1)
		So(refreshConn.inlineRefreshFailures(), ShouldEqual, 0)
		So(refreshConn.maintenanceRefreshInserts(), ShouldBeGreaterThan, 0)

		treeBefore := refreshConn.treeSummaryQueryCount()
		ancestorBefore := refreshConn.ancestorDGUTAQueryCount()

		di, err := p.Tree().DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 4)
		So(refreshConn.treeSummaryQueryCount(), ShouldBeGreaterThan, treeBefore)
		So(refreshConn.ancestorDGUTAQueryCount(), ShouldEqual, ancestorBefore)
	})

	Convey("provider reader build publishes readers without synchronous tree summary refresh", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderSnapshot(ctx, conn, updatedAt, 2, 100, 1000), ShouldBeNil)

		cp := &chProvider{
			cfg:   cfg,
			conn:  &treeSummaryRefreshDeadlineConn{Conn: conn},
			errCh: make(chan struct{}, 1),
		}

		dbImpl, tree, bd, fingerprint, err := cp.buildReadersNow(context.Background())
		So(err, ShouldBeNil)
		So(dbImpl, ShouldNotBeNil)
		So(tree, ShouldNotBeNil)
		So(bd, ShouldNotBeNil)
		So(fingerprint, ShouldNotBeBlank)
		So(cp.pendingErrs, ShouldHaveLength, 0)

		deadlineConn, ok := cp.conn.(*treeSummaryRefreshDeadlineConn)
		So(ok, ShouldBeTrue)
		So(deadlineConn.treeSummaryRefreshFailures(), ShouldEqual, 0)
		So(dbImpl.Close(), ShouldBeNil)
		So(bd.Close(), ShouldBeNil)
	})

	Convey("async tree summary refresh errors reach OnError after polling starts", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 50 * time.Millisecond

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)
		providerOwnsConn := false

		Reset(func() {
			if !providerOwnsConn {
				So(conn.Close(), ShouldBeNil)
			}
		})

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderSnapshot(ctx, conn, updatedAt, 2, 100, 1000), ShouldBeNil)

		failingConn := &treeSummaryRefreshDeadlineConn{Conn: conn}

		p, err := openProviderWithConnector(cfg, func(Config) (ch.Conn, error) {
			return failingConn, nil
		})
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		providerOwnsConn = true

		Reset(func() { So(p.Close(), ShouldBeNil) })

		got := make(chan error, 1)

		p.OnError(func(err error) {
			got <- err
		})

		select {
		case err := <-got:
			So(err.Error(), ShouldContainSubstring, "failed to refresh active tree summaries asynchronously")
			So(err.Error(), ShouldContainSubstring, context.DeadlineExceeded.Error())
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for async refresh OnError")
		}

		So(failingConn.treeSummaryRefreshFailures(), ShouldBeGreaterThan, 0)
	})

	Convey("ready tree summaries avoid scheduling maintenance refresh", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.PollInterval = 0

		const (
			lustreAncestor = "/lustre/"
			mountPath      = lustreAncestor + "agentA/"
		)

		cfg.MountPoints = []string{"/", mountPath}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)
		providerOwnsConn := false

		Reset(func() {
			if !providerOwnsConn {
				So(conn.Close(), ShouldBeNil)
			}
		})

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderAncestorSnapshot(ctx, conn, mountPath, updatedAt), ShouldBeNil)

		rows := []mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}}
		So(ensureActiveTreeSummaries(ctx, conn, rows), ShouldBeNil)
		So(ensureActiveMountDirSummaries(ctx, conn, rows), ShouldBeNil)

		refreshConn := &treeSummaryRefreshTimeoutThenMaintenanceConn{Conn: conn}

		p, err := openProviderWithConnector(cfg, func(Config) (ch.Conn, error) {
			return refreshConn, nil
		})
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		providerOwnsConn = true

		Reset(func() { So(p.Close(), ShouldBeNil) })

		cp, ok := p.(*chProvider)
		So(ok, ShouldBeTrue)
		So(cp.pendingMessages, ShouldHaveLength, 0)
		So(refreshConn.inlineRefreshFailures(), ShouldEqual, 0)
		So(refreshConn.maintenanceRefreshInserts(), ShouldEqual, 0)

		treeBefore := refreshConn.treeSummaryQueryCount()
		ancestorBefore := refreshConn.ancestorDGUTAQueryCount()

		di, err := p.Tree().DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
		So(err, ShouldBeNil)
		So(di, ShouldNotBeNil)
		So(di.Current.Count, ShouldEqual, 4)
		So(refreshConn.treeSummaryQueryCount(), ShouldBeGreaterThan, treeBefore)
		So(refreshConn.ancestorDGUTAQueryCount(), ShouldEqual, ancestorBefore)
	})
}

func waitForProviderMessage(t *testing.T, messages <-chan string, want string, more ...string) string {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		select {
		case msg := <-messages:
			if messageContainsAll(msg, append([]string{want}, more...)...) {
				return msg
			}
		case <-deadline:
			t.Fatalf("timed out waiting for provider message containing %q", want)
		}
	}
}

func messageContainsAll(msg string, wants ...string) bool {
	for _, want := range wants {
		if !strings.Contains(msg, want) {
			return false
		}
	}

	return true
}

func insertProviderAncestorSnapshot(
	ctx context.Context,
	conn providerExecConn,
	mountPath string,
	updatedAt time.Time,
) error {
	sid := snapshotID(mountPath, updatedAt)
	atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
	mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

	if err := conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now().UTC(), sid, updatedAt); err != nil {
		return err
	}

	for _, dir := range []string{"/", "/lustre/"} {
		if err := conn.Exec(
			ctx,
			testInsertDGUTAStmt,
			mountPath,
			sid.String(),
			dir,
			uint32(7),
			uint32(9),
			uint16(db.DGUTAFileTypeBam),
			uint8(db.DGUTAgeAll),
			uint64(4),
			uint64(40),
			int64(10),
			int64(20),
			atimeBuckets,
			mtimeBuckets,
		); err != nil {
			return err
		}
	}

	return conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid.String(), "/", "/lustre")
}

func TestProviderLazyReaderBuildSchedulesRefreshWithoutDeadlock(t *testing.T) {
	Convey("lazy reader build releases the provider lock before scheduling tree summary refresh", t, func() {
		var cp *chProvider

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		rows := []mountsActiveRow{{
			mountPath:  providerTestMountPath,
			snapshotID: "snapshot-1",
			updatedAt:  updatedAt,
		}}
		snapshot := newActiveMountsSnapshot(rows)

		cp = &chProvider{
			msgCh: make(chan struct{}, 1),
			buildReaders: func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error) {
				dbImpl := &providerSwapTestDB{}
				bdImpl := &providerSwapTestBD{}

				return dbImpl, db.NewTree(dbImpl), bdImpl, nil
			},
			captureSnapshot: func(context.Context) (*activeMountsSnapshot, string, error) {
				cp.scheduleTreeSummaryRefresh(context.Background(), snapshot, rows)

				return snapshot, snapshot.fingerprint, nil
			},
		}

		gotTree := make(chan *db.Tree, 1)

		go func() {
			gotTree <- cp.Tree()
		}()

		select {
		case tree := <-gotTree:
			So(tree, ShouldNotBeNil)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for lazy reader build")
		}

		So(cp.currentPublishedFingerprint(), ShouldEqual, snapshot.fingerprint)
	})
}

func TestProviderMaintenanceConnection(t *testing.T) {
	Convey("provider-owned async maintenance uses a separate lazy connection", t, func() {
		cfg := Config{
			DSN:      "clickhouse://127.0.0.1:9000/default?database=wrstat&read_timeout=100ms",
			Database: testDatabaseName,
		}
		primaryConn := &bootstrapTestConn{}
		maintenanceConn := &bootstrapTestConn{}

		var calls atomic.Int32

		cp := &chProvider{
			cfg:  cfg,
			conn: primaryConn,
			connectMaintenance: func(ctx context.Context, got Config) (ch.Conn, error) {
				So(ctx.Err(), ShouldBeNil)
				So(got, ShouldResemble, cfg)
				calls.Add(1)

				return maintenanceConn, nil
			},
		}

		conn, err := cp.maintenanceConnection(context.Background())
		So(err, ShouldBeNil)
		So(conn, ShouldEqual, maintenanceConn)
		So(calls.Load(), ShouldEqual, 1)

		conn, err = cp.maintenanceConnection(context.Background())
		So(err, ShouldBeNil)
		So(conn, ShouldEqual, maintenanceConn)
		So(calls.Load(), ShouldEqual, 1)
		So(primaryConn.closed.Load(), ShouldBeFalse)

		So(cp.Close(), ShouldBeNil)
		So(maintenanceConn.closed.Load(), ShouldBeTrue)
		So(primaryConn.closed.Load(), ShouldBeTrue)
	})

	Convey("test connector path can share the foreground connection", t, func() {
		primaryConn := &bootstrapTestConn{}
		cp := &chProvider{conn: primaryConn}

		conn, err := cp.maintenanceConnection(context.Background())
		So(err, ShouldBeNil)
		So(conn, ShouldEqual, primaryConn)
	})
}

func TestProviderMaintenanceRefreshUsesMaintenanceConnection(t *testing.T) {
	Convey("mount dir projection refresh uses the provider maintenance connection", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		primaryConn := &providerMaintenanceRefreshConn{
			Conn:              th.openConn(cfg.DSN),
			failOnMaintenance: true,
		}
		maintenanceConn := &providerMaintenanceRefreshConn{Conn: th.openConn(cfg.DSN)}

		var maintenanceCalls atomic.Int32

		cp := &chProvider{
			cfg:  cfg,
			conn: primaryConn,
			connectMaintenance: func(ctx context.Context, got Config) (ch.Conn, error) {
				So(ctx.Err(), ShouldBeNil)
				So(got, ShouldResemble, cfg)
				maintenanceCalls.Add(1)

				return maintenanceConn, nil
			},
		}

		Reset(func() {
			So(cp.Close(), ShouldBeNil)

			if maintenanceCalls.Load() == 0 {
				So(maintenanceConn.Close(), ShouldBeNil)
			}
		})

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(providerTestMountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderSnapshot(ctx, primaryConn, updatedAt, 2, 100, 1000), ShouldBeNil)

		rows := []mountsActiveRow{{
			mountPath:  providerTestMountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}}

		ok := cp.tryMountDirProjectionRefresh(context.Background(), newMountDirProjectionRefreshJob(rows))
		So(ok, ShouldBeTrue)
		So(maintenanceCalls.Load(), ShouldEqual, 1)
		So(primaryConn.mountDirProjectionInsertFailures(), ShouldEqual, 0)
		So(maintenanceConn.mountDirProjectionInserts(), ShouldBeGreaterThan, 0)
		So(countRows(ctx, maintenanceConn,
			"SELECT count() FROM wrstat_dir_summary_sets FINAL WHERE mount_path = ? AND snapshot_id = ?",
			providerTestMountPath,
			sid.String(),
		), ShouldEqual, 1)
	})

	Convey("tree summary refresh uses the provider maintenance connection", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/", "/lustre/agentA/"}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		primaryConn := &providerMaintenanceRefreshConn{
			Conn:              th.openConn(cfg.DSN),
			failOnMaintenance: true,
		}
		maintenanceConn := &providerMaintenanceRefreshConn{Conn: th.openConn(cfg.DSN)}

		var maintenanceCalls atomic.Int32

		cp := &chProvider{
			cfg:  cfg,
			conn: primaryConn,
			connectMaintenance: func(ctx context.Context, got Config) (ch.Conn, error) {
				So(ctx.Err(), ShouldBeNil)
				So(got, ShouldResemble, cfg)
				maintenanceCalls.Add(1)

				return maintenanceConn, nil
			},
		}

		Reset(func() {
			So(cp.Close(), ShouldBeNil)

			if maintenanceCalls.Load() == 0 {
				So(maintenanceConn.Close(), ShouldBeNil)
			}
		})

		const mountPath = "/lustre/agentA/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(insertProviderAncestorSnapshot(ctx, primaryConn, mountPath, updatedAt), ShouldBeNil)

		rows := []mountsActiveRow{{
			mountPath:  mountPath,
			snapshotID: sid.String(),
			updatedAt:  updatedAt,
		}}
		job := newTreeSummaryRefreshJob(rows)

		ok := cp.tryTreeSummaryRefresh(context.Background(), job)
		So(ok, ShouldBeTrue)
		So(maintenanceCalls.Load(), ShouldEqual, 1)
		So(primaryConn.treeSummaryInsertFailures(), ShouldEqual, 0)
		So(maintenanceConn.treeSummaryInserts(), ShouldBeGreaterThan, 0)
		So(countRows(ctx, maintenanceConn,
			"SELECT count() FROM wrstat_tree_summary_sets FINAL WHERE fingerprint = ?",
			job.fingerprint,
		), ShouldEqual, 1)
	})
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

		So(got.tree == oldTree, ShouldBeFalse)
		So(got.bd == oldBD, ShouldBeFalse)

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

type mountDirProjectionTimeoutThenMaintenanceConn struct {
	ch.Conn

	inlineFailures     atomic.Int32
	maintenanceInserts atomic.Int32
	readinessQueries   atomic.Int32
	vectorQueries      atomic.Int32
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if strings.Contains(query, "FROM wrstat_dir_summary_sets") {
		c.readinessQueries.Add(1)
	}

	if strings.Contains(query, "FROM wrstat_dir_dguta_vector") {
		c.vectorQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if isMountDirProjectionInsert(query) {
		if _, ok := ctx.Deadline(); ok {
			c.inlineFailures.Add(1)

			<-ctx.Done()

			return ctx.Err()
		}

		c.maintenanceInserts.Add(1)
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isMountDirProjectionInsert(query string) bool {
	return strings.Contains(query, "INSERT INTO wrstat_dir_summary ") ||
		strings.Contains(query, "INSERT INTO wrstat_dir_dguta_vector ")
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) inlineProjectionFailures() int {
	return int(c.inlineFailures.Load())
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) maintenanceProjectionInserts() int {
	return int(c.maintenanceInserts.Load())
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) mountDirReadinessQueryCount() int {
	return int(c.readinessQueries.Load())
}

func (c *mountDirProjectionTimeoutThenMaintenanceConn) mountDirVectorQueryCount() int {
	return int(c.vectorQueries.Load())
}

type providerMaintenanceRefreshConn struct {
	ch.Conn

	failOnMaintenance bool
	mountDirInserts   atomic.Int32
	mountDirFailures  atomic.Int32
	treeInserts       atomic.Int32
	treeFailures      atomic.Int32
}

func (c *providerMaintenanceRefreshConn) Exec(ctx context.Context, query string, args ...any) error {
	if isMountDirProjectionInsert(query) {
		if c.failOnMaintenance {
			c.mountDirFailures.Add(1)

			return errProviderMaintenanceRefreshPrimary
		}

		c.mountDirInserts.Add(1)
	}

	if isTreeSummaryInsert(query) {
		if c.failOnMaintenance {
			c.treeFailures.Add(1)

			return errProviderMaintenanceRefreshPrimary
		}

		c.treeInserts.Add(1)
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isTreeSummaryInsert(query string) bool {
	return strings.Contains(query, "INSERT INTO wrstat_tree_")
}

func (c *providerMaintenanceRefreshConn) mountDirProjectionInsertFailures() int {
	return int(c.mountDirFailures.Load())
}

func (c *providerMaintenanceRefreshConn) mountDirProjectionInserts() int {
	return int(c.mountDirInserts.Load())
}

func (c *providerMaintenanceRefreshConn) treeSummaryInsertFailures() int {
	return int(c.treeFailures.Load())
}

func (c *providerMaintenanceRefreshConn) treeSummaryInserts() int {
	return int(c.treeInserts.Load())
}

type treeSummaryRefreshTimeoutThenMaintenanceConn struct {
	ch.Conn

	ancestorDGUTAQueries atomic.Int32
	inlineFailures       atomic.Int32
	maintenanceInserts   atomic.Int32
	treeSummaryQueries   atomic.Int32
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if isAncestorDGUTAQuery(query) {
		c.ancestorDGUTAQueries.Add(1)
	}

	if isTreeSummaryQuery(query) {
		c.treeSummaryQueries.Add(1)
	}

	return c.Conn.Query(ctx, query, args...)
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if strings.Contains(query, "INSERT INTO wrstat_tree_") {
		if _, ok := ctx.Deadline(); ok {
			c.inlineFailures.Add(1)

			<-ctx.Done()

			return ctx.Err()
		}

		c.maintenanceInserts.Add(1)
	}

	return c.Conn.Exec(ctx, query, args...)
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) ancestorDGUTAQueryCount() int {
	return int(c.ancestorDGUTAQueries.Load())
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) inlineRefreshFailures() int {
	return int(c.inlineFailures.Load())
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) maintenanceRefreshInserts() int {
	return int(c.maintenanceInserts.Load())
}

func (c *treeSummaryRefreshTimeoutThenMaintenanceConn) treeSummaryQueryCount() int {
	return int(c.treeSummaryQueries.Load())
}
