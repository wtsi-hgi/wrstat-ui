package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	basedirsStoreTestCountGroupUsageQuery = "SELECT count() FROM wrstat_basedirs_group_usage " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	basedirsStoreTestCountUserUsageQuery = "SELECT count() FROM wrstat_basedirs_user_usage " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	basedirsStoreTestCountGroupSubdirsQuery = "SELECT count() FROM wrstat_basedirs_group_subdirs " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	basedirsStoreTestCountHistoryQuery     = "SELECT count() FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ?"
	basedirsStoreTestSelectQuotaDatesQuery = "SELECT date_no_space, date_no_files FROM wrstat_basedirs_group_usage " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?) AND gid = ? AND age = ? LIMIT 1"
	basedirsStoreTestCountUserSubdirsQuery = "SELECT count() FROM wrstat_basedirs_user_subdirs " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	basedirsStoreTestBaseDir = "/base/"
)

type basedirsIdleBatch struct {
	countingDGUTABatch

	conn              *basedirsIdleBatchConn
	sideQueriesAtOpen int
}

func (b *basedirsIdleBatch) Send() error {
	if b.Rows() > 0 && b.conn.sideQueries() > b.sideQueriesAtOpen {
		b.conn.lateSendCount++

		return context.DeadlineExceeded
	}

	return b.countingDGUTABatch.Send()
}

type basedirsIdleBatchConn struct {
	bootstrapTestConn

	historyQueries  int
	finaliseQueries int
	lateSendCount   int
}

func (c *basedirsIdleBatchConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{dgutaWriterTestSnapshotIDColumn},
		}, nil
	case queryBasedirsHistoryLastDate:
		c.historyQueries++

		return &dgutaWriterCloseContextRows{columns: []string{"max(date)"}}, nil
	case queryBasedirsHistorySeries:
		c.finaliseQueries++

		return &dgutaWriterCloseContextRows{
			columns: []string{"date", "usage_size", "quota_size", "usage_inodes", "quota_inodes"},
		}, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *basedirsIdleBatchConn) Exec(_ context.Context, query string, _ ...any) error {
	switch {
	case strings.HasPrefix(query, "ALTER TABLE"):
		return nil
	case query == insertBasedirsHistoryPoint:
		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *basedirsIdleBatchConn) PrepareBatch(
	_ context.Context,
	_ string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return &basedirsIdleBatch{conn: c, sideQueriesAtOpen: c.sideQueries()}, nil
}

func (c *basedirsIdleBatchConn) lateSends() int {
	return c.lateSendCount
}

func (c *basedirsIdleBatchConn) sideQueries() int {
	return c.historyQueries + c.finaliseQueries
}

func TestClickHouseBaseDirsStore(t *testing.T) {
	Convey("BaseDirsStore retry reset drops all basedirs snapshot partitions before rewriting rows", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

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
		So(writeBasedirsSnapshotRows(store, gid, uid, updatedAt, 10), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		assertBasedirsSnapshotRowCounts(ctx, conn, sid, 1)

		retryStore, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(retryStore, ShouldNotBeNil)

		retryStore.SetMountPath(testMountPath)
		retryStore.SetUpdatedAt(updatedAt)
		So(retryStore.Reset(), ShouldBeNil)
		assertBasedirsSnapshotRowCounts(ctx, conn, sid, 0)

		So(writeBasedirsSnapshotRows(retryStore, gid, uid, updatedAt, 20), ShouldBeNil)
		So(retryStore.Finalise(), ShouldBeNil)
		So(retryStore.Close(), ShouldBeNil)

		assertBasedirsSnapshotRowCounts(ctx, conn, sid, 1)
	})

	Convey("BaseDirsStore Abort drops partial usage rows without advancing active mount metadata", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		impl, ok := store.(*chBaseDirsStore)
		So(ok, ShouldBeTrue)
		impl.SetBatchSize(1)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         7,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{17},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
		So(impl.Abort(), ShouldBeNil)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 0)

		_, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
	})

	Convey("BaseDirsStore writes basedirs snapshots and maintains history under a low configured import pool", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MaxOpenConns = 1
		cfg.MaxIdleConns = 1

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)

		impl, ok := store.(*chBaseDirsStore)
		So(ok, ShouldBeTrue)

		// History append rule (only strictly increasing dates).
		gid := uint32(7)
		hKey := basedirs.HistoryKey{GID: gid, MountPath: testMountPath}

		h1 := basedirs.History{
			Date: time.Unix(1709000000, 0).UTC(), UsageSize: 50,
			QuotaSize: 200, UsageInodes: 5, QuotaInodes: 20,
		}
		h2 := basedirs.History{
			Date: time.Unix(1709100000, 0).UTC(), UsageSize: 100,
			QuotaSize: 200, UsageInodes: 10, QuotaInodes: 20,
		}
		hOld := basedirs.History{
			Date: time.Unix(1708000000, 0).UTC(), UsageSize: 1,
			QuotaSize: 200, UsageInodes: 1, QuotaInodes: 20,
		}

		So(store.AppendGroupHistory(hKey, h1), ShouldBeNil)
		So(impl.LastHistoryAppendInserted(), ShouldBeTrue)
		So(store.AppendGroupHistory(hKey, h2), ShouldBeNil)
		So(impl.LastHistoryAppendInserted(), ShouldBeTrue)
		So(store.AppendGroupHistory(hKey, hOld), ShouldBeNil)
		So(impl.LastHistoryAppendInserted(), ShouldBeFalse)

		// Usage rows; age=all must be buffered and inserted in Finalise with quota dates.
		uAll := &basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1, 2},
			UsageSize:   100,
			QuotaSize:   200,
			UsageInodes: 10,
			QuotaInodes: 20,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeAll,
		}
		uA1M := &basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   200,
			UsageInodes: 1,
			QuotaInodes: 20,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}

		So(store.PutGroupUsage(uAll), ShouldBeNil)
		So(store.PutGroupUsage(uA1M), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 2)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 2)

		rows, err := conn.Query(ctx, basedirsStoreTestSelectQuotaDatesQuery, testMountPath, sid, gid, uint8(db.DGUTAgeAll))
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var gotNoSpace, gotNoFiles time.Time
		So(rows.Scan(&gotNoSpace, &gotNoFiles), ShouldBeNil)

		expNoSpace, expNoFiles := basedirs.DateQuotaFull([]basedirs.History{h1, h2})
		So(gotNoSpace.Unix(), ShouldEqual, expNoSpace.Unix())
		So(gotNoFiles.Unix(), ShouldEqual, expNoFiles.Unix())
	})

	Convey("BaseDirsStore refreshes subdir batches after a mid-call flush", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		impl, ok := store.(*chBaseDirsStore)
		So(ok, ShouldBeTrue)

		impl.SetBatchSize(1)
		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)

		subdirs := []*basedirs.SubDir{
			{
				SubDir:       "first",
				NumFiles:     1,
				SizeFiles:    10,
				LastModified: updatedAt,
			},
			{
				SubDir:       "second",
				NumFiles:     2,
				SizeFiles:    20,
				LastModified: updatedAt.Add(time.Minute),
			},
		}

		So(store.PutGroupSubDirs(
			basedirs.SubDirKey{ID: 7, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeAll},
			subdirs,
		), ShouldBeNil)
		So(store.PutUserSubDirs(
			basedirs.SubDirKey{ID: 17, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeAll},
			subdirs,
		), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, basedirsStoreTestCountGroupSubdirsQuery, testMountPath, sid), ShouldEqual, 2)
		So(countRows(ctx, conn, basedirsStoreTestCountUserSubdirsQuery, testMountPath, sid), ShouldEqual, 2)
	})

	Convey("BaseDirsStore records basedirs phases through its recorder wiring", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		gid := uint32(7)
		uid := uint32(17)
		phases := make(map[string]time.Duration)

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		impl, ok := store.(*chBaseDirsStore)
		So(ok, ShouldBeTrue)

		impl.SetBatchSize(1)
		impl.SetImportPhaseRecorder(func(phase string, d time.Duration) {
			phases[phase] += d
		})

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)
		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			basedirs.History{
				Date: updatedAt, UsageSize: 100,
				QuotaSize: 200, UsageInodes: 10, QuotaInodes: 20,
			},
		), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{uid},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)
		So(store.PutUserUsage(&basedirs.Usage{
			UID:         uid,
			BaseDir:     basedirsStoreTestBaseDir,
			GIDs:        []uint32{gid},
			UsageSize:   11,
			QuotaSize:   21,
			UsageInodes: 2,
			QuotaInodes: 3,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)

		subdirs := []*basedirs.SubDir{{
			SubDir:       "child",
			NumFiles:     1,
			SizeFiles:    10,
			LastModified: updatedAt,
		}}
		So(store.PutGroupSubDirs(
			basedirs.SubDirKey{ID: gid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeA1M},
			subdirs,
		), ShouldBeNil)
		So(store.PutUserSubDirs(
			basedirs.SubDirKey{ID: uid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeA1M},
			subdirs,
		), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		for _, phase := range []string{
			"wrstat_basedirs_reset",
			"wrstat_basedirs_group_usage_insert",
			"wrstat_basedirs_user_usage_insert",
			"wrstat_basedirs_group_subdirs_insert",
			"wrstat_basedirs_user_subdirs_insert",
			"wrstat_basedirs_history_insert",
			"wrstat_basedirs_finalise",
			"wrstat_basedirs_flush",
		} {
			So(phases[phase], ShouldBeGreaterThan, time.Duration(0))
		}
	})

	Convey("BaseDirsStore flushes pending usage batches before history side queries", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		conn := &basedirsIdleBatchConn{}
		store := &chBaseDirsStore{
			cfg:       Config{QueryTimeout: 100 * time.Millisecond},
			conn:      conn,
			batchSize: 100_000,
			mountPath: testMountPath,
			updatedAt: updatedAt,
		}

		So(store.Reset(), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         7,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 7, MountPath: testMountPath},
			basedirs.History{
				Date: updatedAt, UsageSize: 10,
				QuotaSize: 20, UsageInodes: 1, QuotaInodes: 2,
			},
		), ShouldBeNil)
		So(store.Close(), ShouldBeNil)
		So(conn.lateSends(), ShouldEqual, 0)
	})

	Convey("BaseDirsStore reads finalise histories before opening age-all usage batches", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		conn := &basedirsIdleBatchConn{}
		store := &chBaseDirsStore{
			cfg:       Config{QueryTimeout: 100 * time.Millisecond},
			conn:      conn,
			batchSize: 100_000,
			mountPath: testMountPath,
			updatedAt: updatedAt,
		}

		So(store.Reset(), ShouldBeNil)

		for _, gid := range []uint32{7, 8} {
			So(store.PutGroupUsage(&basedirs.Usage{
				GID:         gid,
				BaseDir:     basedirsStoreTestBaseDir,
				UIDs:        []uint32{1},
				UsageSize:   10,
				QuotaSize:   20,
				UsageInodes: 1,
				QuotaInodes: 2,
				Mtime:       updatedAt,
				Age:         db.DGUTAgeAll,
			}), ShouldBeNil)
		}

		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)
		So(conn.lateSends(), ShouldEqual, 0)
		So(conn.finaliseQueries, ShouldEqual, 2)
	})

	Convey("BaseDirsStore flushes stale usage batches before moving to another basedirs table", t, func() {
		now := time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC)
		updatedAt := time.Unix(1710000000, 0).UTC()
		conn := &basedirsPhaseBatchConn{}
		store := &chBaseDirsStore{
			cfg:       Config{QueryTimeout: 100 * time.Millisecond},
			conn:      conn,
			batchSize: 100_000,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			batchNow:  func() time.Time { return now },
		}

		So(store.Reset(), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         7,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)

		groupBatches := conn.batchesFor(insertBasedirsGroupUsageQuery)
		So(groupBatches, ShouldHaveLength, 1)
		So(groupBatches[0].sends, ShouldEqual, 0)

		now = now.Add(importBatchMaxOpenDuration)

		So(store.PutUserUsage(&basedirs.Usage{
			UID:         9,
			BaseDir:     basedirsStoreTestBaseDir,
			GIDs:        []uint32{7},
			UsageSize:   11,
			UsageInodes: 3,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)

		So(groupBatches[0].sends, ShouldEqual, 1)
		So(conn.batchesFor(insertBasedirsUserUsageQuery), ShouldHaveLength, 1)
	})

	Convey("BaseDirsStore refuses to rewrite an active deterministic snapshot", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         7,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		retryStore, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(retryStore, ShouldNotBeNil)

		retryStore.SetMountPath(testMountPath)
		retryStore.SetUpdatedAt(updatedAt)

		err = retryStore.Reset()
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotRewrite), ShouldBeTrue)
		So(retryStore.Close(), ShouldBeNil)

		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
	})

	Convey("BaseDirsStore Abort rolls back newly appended history without touching older history", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()
		gid := uint32(7)
		oldPoint := basedirs.History{
			Date: time.Unix(1709000000, 0).UTC(), UsageSize: 50,
			QuotaSize: 200, UsageInodes: 5, QuotaInodes: 20,
		}
		newPoint := basedirs.History{
			Date: updatedAt, UsageSize: 100,
			QuotaSize: 200, UsageInodes: 10, QuotaInodes: 20,
		}

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			testMountPath,
			gid,
			oldPoint.Date,
			oldPoint.UsageSize,
			oldPoint.QuotaSize,
			oldPoint.UsageInodes,
			oldPoint.QuotaInodes,
		), ShouldBeNil)
		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			newPoint,
		), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 2)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)

		aborter, ok := store.(interface{ Abort() error })
		So(ok, ShouldBeTrue)
		So(aborter.Abort(), ShouldBeNil)

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 1)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 0)

		retryStore, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(retryStore, ShouldNotBeNil)

		retryStore.SetMountPath(testMountPath)
		retryStore.SetUpdatedAt(updatedAt)
		So(retryStore.Reset(), ShouldBeNil)
		So(retryStore.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			newPoint,
		), ShouldBeNil)
		So(retryStore.PutGroupUsage(&basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   11,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)
		So(retryStore.Finalise(), ShouldBeNil)
		So(retryStore.Close(), ShouldBeNil)

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 2)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
	})

	Convey("BaseDirsStore Abort preserves published data after the snapshot is active", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()
		gid := uint32(7)
		point := basedirs.History{
			Date: updatedAt, UsageSize: 100,
			QuotaSize: 200, UsageInodes: 10, QuotaInodes: 20,
		}

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)
		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: gid, MountPath: testMountPath},
			point,
		), ShouldBeNil)
		So(store.PutGroupUsage(&basedirs.Usage{
			GID:         gid,
			BaseDir:     basedirsStoreTestBaseDir,
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   20,
			UsageInodes: 1,
			QuotaInodes: 2,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now(), sid, updatedAt), ShouldBeNil)
		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 1)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)

		aborter, ok := store.(interface{ Abort() error })
		So(ok, ShouldBeTrue)
		So(aborter.Abort(), ShouldBeNil)

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 1)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
	})
}

func writeBasedirsSnapshotRows(
	store basedirs.Store,
	gid uint32,
	uid uint32,
	updatedAt time.Time,
	usageSize uint64,
) error {
	if err := store.PutGroupUsage(&basedirs.Usage{
		GID:         gid,
		BaseDir:     basedirsStoreTestBaseDir,
		UIDs:        []uint32{uid},
		UsageSize:   usageSize,
		QuotaSize:   100,
		UsageInodes: 1,
		QuotaInodes: 10,
		Mtime:       updatedAt,
		Age:         db.DGUTAgeA1M,
	}); err != nil {
		return err
	}

	if err := store.PutUserUsage(&basedirs.Usage{
		UID:         uid,
		BaseDir:     basedirsStoreTestBaseDir,
		GIDs:        []uint32{gid},
		UsageSize:   usageSize + 1,
		QuotaSize:   101,
		UsageInodes: 2,
		QuotaInodes: 11,
		Mtime:       updatedAt,
		Age:         db.DGUTAgeA1M,
	}); err != nil {
		return err
	}

	subdirs := []*basedirs.SubDir{{
		SubDir:       "child",
		NumFiles:     1,
		SizeFiles:    usageSize,
		LastModified: updatedAt,
	}}

	if err := store.PutGroupSubDirs(
		basedirs.SubDirKey{ID: gid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeA1M},
		subdirs,
	); err != nil {
		return err
	}

	return store.PutUserSubDirs(
		basedirs.SubDirKey{ID: uid, BaseDir: basedirsStoreTestBaseDir, Age: db.DGUTAgeA1M},
		subdirs,
	)
}

func assertBasedirsSnapshotRowCounts(
	ctx context.Context,
	conn interface {
		Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	},
	sid string,
	expected uint64,
) {
	queries := []string{
		basedirsStoreTestCountGroupUsageQuery,
		basedirsStoreTestCountUserUsageQuery,
		basedirsStoreTestCountGroupSubdirsQuery,
		basedirsStoreTestCountUserSubdirsQuery,
	}

	for _, query := range queries {
		So(countRows(ctx, conn, query, testMountPath, sid), ShouldEqual, expected)
	}
}

type basedirsPhaseBatch struct {
	countingDGUTABatch

	query string
}

type basedirsPhaseBatchConn struct {
	basedirsIdleBatchConn

	batches []*basedirsPhaseBatch
}

func (c *basedirsPhaseBatchConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	batch := &basedirsPhaseBatch{query: query}
	c.batches = append(c.batches, batch)

	return batch, nil
}

func (c *basedirsPhaseBatchConn) batchesFor(query string) []*basedirsPhaseBatch {
	var batches []*basedirsPhaseBatch
	for _, batch := range c.batches {
		if batch.query == query {
			batches = append(batches, batch)
		}
	}

	return batches
}
