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
	"syscall"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var errForcedFailure = errors.New("forced failure")

var errUnknownParentFactsTable = errors.New("UNKNOWN_TABLE: wrstat_parent_facts does not exist")

const testMountPath = "/mnt/test/"

const testT283ImagingMountPath = "/nfs/t283_imaging/"

const dgutaWriterTestPhasePartitionDropReset = "partition_drop_reset"

const dgutaWriterTestSnapshotIDColumn = "snapshot_id"

const dgutaWriterTestChildName = "child/"

const (
	insertDGUTAQuery = "INSERT INTO wrstat_dir_facts " +
		"(mount_path, snapshot_id, dir, updated_at, gids, uids, fts, ages, " +
		"counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, now(), [?], [?], [?], [?], [?], [?], [?], [?], [?], [?], now())"

	dgutaWriterTestActiveSnapshotQuery = "SELECT toString(snapshot_id), updated_at FROM wrstat_mounts_active " +
		"WHERE mount_path = ?"
	dgutaWriterTestSelectGIDQuery = "SELECT arrayJoin(gids) AS gid FROM wrstat_dir_facts WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND dir = ? ORDER BY gid"
	dgutaWriterTestSelectChildQuery = "SELECT child FROM wrstat_children WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND parent_dir = ?"

	dgutaWriterTestCountActiveMountQuery = "SELECT count() FROM wrstat_mounts_active WHERE mount_path = ?"
	dgutaWriterTestCountDirFactsQuery    = "SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?)"
	dgutaWriterTestCountChildrenQuery = "SELECT count() FROM wrstat_children WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?)"
	dgutaWriterTestCountAgeAllQuery = "SELECT count() FROM wrstat_dir_filter_ageall WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?)"
	dgutaWriterTestCountDirFactsForDirQuery = "SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND dir = ?"
	dgutaWriterTestCountChildrenForParentQuery = "SELECT count() FROM wrstat_children WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND parent_dir = ?"
)

const b1ReadDirFactQuery = "SELECT gids, uids, fts, ages, counts, sizes, " +
	"atime_mins, mtime_maxs, atime_buckets, mtime_buckets, " +
	"all_count, all_size, all_uids, all_gids, all_ft, " +
	"file_count, file_size, file_atime_min, file_mtime_max, " +
	"file_atime_buckets, file_mtime_buckets, file_uids, file_gids, file_ft, " +
	"child_count FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = toUUID(?) AND dir = ?"

const activePrefixB1AllCount = 100000

const (
	activePrefixScalarRollupPerfName = "active_prefix_scalar_rollup_prototype"
	activePrefixScalarRollupRepeat   = 20
	activePrefixScalarRollupWarmup   = 1
	activePrefixScalarRollupP50MaxMS = 2
	activePrefixScalarRollupP95MaxMS = 4
)

type forbiddenProjectionRefreshConn struct {
	ch.Conn

	forbidden atomic.Int32
}

func (c *forbiddenProjectionRefreshConn) Exec(ctx context.Context, query string, args ...any) error {
	if isForbiddenProjectionRefreshSQL(query) {
		c.forbidden.Add(1)
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isForbiddenProjectionRefreshSQL(query string) bool {
	normalised := strings.Join(strings.Fields(query), " ")

	return strings.Contains(normalised, "INSERT INTO wrstat_dir_facts ") &&
		strings.Contains(normalised, " SELECT ") &&
		strings.Contains(normalised, " FROM wrstat_dguta")
}

func (c *forbiddenProjectionRefreshConn) forbiddenProjectionRefreshes() int {
	return int(c.forbidden.Load())
}

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

	Convey("DGUTAWriter records initial partition drop/reset time", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		phases := make(map[string]time.Duration)

		impl.SetImportPhaseRecorder(func(phase string, d time.Duration) {
			phases[phase] += d
		})

		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC))

		paths := internaltest.NewDirectoryPathCreator()
		err = w.Add(singleDGUTARecord(paths.ToDirectoryPath("/"), 42, "/child/"))
		So(err, ShouldBeNil)
		So(phases[dgutaWriterTestPhasePartitionDropReset], ShouldBeGreaterThan, time.Duration(0))
		So(w.Close(), ShouldBeNil)
	})

	Convey("DGUTAWriter retry reset drops every snapshot table before writing rows", t, func() {
		queryTimeout := 100 * time.Millisecond
		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt).String()
		conn := &dgutaRetryResetConn{normalWindow: queryTimeout}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: queryTimeout},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
		}

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("/"), 42, "/child/")), ShouldBeNil)
		So(conn.prepared.Load(), ShouldBeTrue)
		So(conn.partitionDrops(), ShouldEqual, len(allPartitionDropQueries()))
		So(conn.longDeadlineDrops(), ShouldEqual, len(allPartitionDropQueries()))
		So(conn.firstPreparedAfterDrops(), ShouldBeTrue)
		So(conn.seenSnapshotIDs(), ShouldResemble, []string{sid})
	})

	Convey("DGUTAWriter retry reset wraps partition drop failures before preparing batches", t, func() {
		queryTimeout := 100 * time.Millisecond
		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		conn := &dgutaRetryResetConn{
			normalWindow: queryTimeout,
			dropErr:      errForcedFailure,
		}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: queryTimeout},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
		}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		err := w.ensureWriteReady(ctx)
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "failed_snapshot_partition_drop")
		So(conn.prepared.Load(), ShouldBeFalse)
		So(conn.partitionDrops(), ShouldEqual, 1)
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

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)
		So(w.Close(), ShouldBeNil)

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx,
			dgutaWriterTestActiveSnapshotQuery,
			testMountPath,
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var (
			gotSID       string
			gotUpdatedAt time.Time
		)

		So(rows.Scan(&gotSID, &gotUpdatedAt), ShouldBeNil)

		expectedSID := snapshotID(testMountPath, updatedAt)
		So(gotSID, ShouldEqual, expectedSID.String())
		So(gotUpdatedAt, ShouldEqual, updatedAt)
	})

	Convey("DGUTAWriter appends publish events and exposes only the latest active snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		olderUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		newerUpdatedAt := olderUpdatedAt.Add(time.Hour)
		olderSID := snapshotID(testMountPath, olderUpdatedAt)
		newerSID := snapshotID(testMountPath, newerUpdatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		writeSingleDGUTARecord(cfg, olderUpdatedAt, paths.ToDirectoryPath("/"), 42, "/first/")
		writeSingleDGUTARecord(cfg, newerUpdatedAt, paths.ToDirectoryPath("/"), 77, "/second/")

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND event_type = 1",
			testMountPath,
		), ShouldEqual, 2)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			testMountPath,
			olderSID.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			testMountPath,
			newerSID.String(),
		), ShouldEqual, 1)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, newerSID.String())
	})

	Convey("DGUTAWriter invalidates scoped active metadata caches after publishing a snapshot", t, func() {
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		updatedAt := time.Date(2026, 1, 9, 15, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)
		cfg := Config{DSN: "clickhouse://localhost:9000/?database=cache_publish", Database: "cache_publish"}
		cache := treeQueryCacheForConfig(cfg)
		key := newTreeCacheKey(testMountPath, sid.String(), testMountPath)
		cache.putChildren(key, []string{testMountPath + "stale"})

		children, ok := cache.getChildren(key)
		So(ok, ShouldBeTrue)
		So(children, ShouldResemble, []string{testMountPath + "stale"})

		w := &dgutaWriter{
			cfg:       cfg,
			conn:      &activeMetadataInvalidationConn{},
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  sid,
		}

		So(w.switchActiveSnapshot(context.Background()), ShouldBeNil)

		_, ok = cache.getChildren(key)
		So(ok, ShouldBeFalse)
	})

	Convey("DGUTAWriter writes dguta + children rows", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		expectedSID := snapshotID(testMountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		writeSingleDGUTARecord(cfg, updatedAt, dir, 42, "/foo/")

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx,
			dgutaWriterTestSelectGIDQuery,
			testMountPath,
			expectedSID.String(),
			"/",
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var gotGID uint32
		So(rows.Scan(&gotGID), ShouldBeNil)
		So(gotGID, ShouldEqual, 42)
		So(rows.Next(), ShouldBeFalse)

		childRows, err := conn.Query(ctx,
			dgutaWriterTestSelectChildQuery,
			testMountPath,
			expectedSID.String(),
			"/",
		)
		So(err, ShouldBeNil)

		defer func() { _ = childRows.Close() }()

		So(childRows.Next(), ShouldBeTrue)

		var gotChild string
		So(childRows.Scan(&gotChild), ShouldBeNil)
		So(gotChild, ShouldEqual, "/foo")
		So(childRows.Next(), ShouldBeFalse)
	})

	Convey("DGUTAWriter supports retry after aborting an unpublished snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		impl.SetBatchSize(1)
		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)
		So(w.Add(singleDGUTARecord(dir, 42, "/foo/")), ShouldBeNil)

		aborter, ok := w.(interface{ Abort() error })
		So(ok, ShouldBeTrue)
		So(aborter.Abort(), ShouldBeNil)

		writeSingleDGUTARecord(cfg, updatedAt, dir, 77, "/bar/")

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			testMountPath,
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)

		rows, err := conn.Query(ctx,
			dgutaWriterTestSelectGIDQuery,
			testMountPath,
			sid.String(),
			"/",
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var gotGID uint32
		So(rows.Scan(&gotGID), ShouldBeNil)
		So(gotGID, ShouldEqual, 77)
		So(rows.Next(), ShouldBeFalse)
	})

	Convey("DGUTAWriter republishes a deterministic snapshot after cleanup tombstones it", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		futureSwitchAt := time.Now().UTC().Add(time.Hour)
		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, futureSwitchAt, sid.String(), updatedAt), ShouldBeNil)
		insertSnapshotCleanupRows(ctx, conn, sid.String(), updatedAt)

		So(cleanActiveSnapshotAttemptWithConn(cfg, conn, testMountPath, updatedAt), ShouldBeNil)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)

		paths := internaltest.NewDirectoryPathCreator()
		writeSingleDGUTARecord(cfg, updatedAt, paths.ToDirectoryPath("/"), 77, "/retry/")

		activeSID, hasActive, err = readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, sid.String())
	})

	Convey("DGUTAWriter expands summariser child names to full child paths", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		expectedSID := snapshotID(testMountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath(testMountPath)

		writeSingleDGUTARecord(cfg, updatedAt, dir, 88, "child/")

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		childRows, err := conn.Query(ctx,
			dgutaWriterTestSelectChildQuery,
			testMountPath,
			expectedSID.String(),
			testMountPath,
		)
		So(err, ShouldBeNil)

		defer func() { _ = childRows.Close() }()

		So(childRows.Next(), ShouldBeTrue)

		var gotChild string
		So(childRows.Scan(&gotChild), ShouldBeNil)
		So(gotChild, ShouldEqual, testMountPath+"child")
		So(childRows.Next(), ShouldBeFalse)
	})

	Convey("DGUTAWriter canonicalises root mount paths without double-counting", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/"}

		const mountPath = "/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		expectedSID := snapshotID(mountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("/"), 42, "")), ShouldBeNil)
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("//"), 42, "boot/")), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsForDirQuery,
			mountPath,
			expectedSID.String(),
			"/",
		), ShouldEqual, 2)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsForDirQuery,
			mountPath,
			expectedSID.String(),
			"//",
		), ShouldEqual, 0)

		dbch := newClickHouseDatabase(cfg, conn)

		sum, err := dbch.DirInfo("/", &db.Filter{Age: db.DGUTAgeA1M})
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, 7)

		children, err := dbch.Children("/")
		So(err, ShouldBeNil)
		So(children, ShouldResemble, []string{"/boot"})

		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenForParentQuery,
			mountPath,
			expectedSID.String(),
			"//",
		), ShouldEqual, 0)
	})

	Convey("DGUTAWriter preserves repeated rows for the same canonical directory", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/"}

		const mountPath = "/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		expectedSID := snapshotID(mountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		So(w.Add(singleDGUTARecord(dir, 42, "")), ShouldBeNil)
		So(w.Add(singleDGUTARecord(dir, 42, "")), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsForDirQuery,
			mountPath,
			expectedSID.String(),
			"/",
		), ShouldEqual, 2)

		dbch := newClickHouseDatabase(cfg, conn)

		sum, err := dbch.DirInfo("/", &db.Filter{Age: db.DGUTAgeA1M})
		So(err, ShouldBeNil)
		So(sum.Count, ShouldEqual, 14)
	})

	Convey("DGUTAWriter drops previous snapshot partitions on Close", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt1 := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		updatedAt2 := updatedAt1.Add(1 * time.Hour)

		sid1 := snapshotID(testMountPath, updatedAt1)
		sid2 := snapshotID(testMountPath, updatedAt2)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		writeSingleDGUTARecord(cfg, updatedAt1, dir, 111, "/old/")

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid1.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid1.String(),
		), ShouldEqual, 1)

		writeSingleDGUTARecord(cfg, updatedAt2, dir, 222, "/new/")

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid1.String(),
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid1.String(),
		), ShouldEqual, 0)

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid2.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid2.String(),
		), ShouldEqual, 1)
	})

	Convey("A3.3 DGUTAWriter drops old AgeAll partitions after publishing a replacement snapshot", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/ageall-cleanup/"}

		const mountPath = "/mnt/ageall-cleanup/"

		updatedAtA := time.Date(2026, 6, 7, 11, 0, 0, 0, time.UTC)
		updatedAtB := updatedAtA.Add(time.Hour)
		sidA := snapshotID(mountPath, updatedAtA)
		sidB := snapshotID(mountPath, updatedAtB)

		writeAgeAllFixtureDGUTARecord(cfg, mountPath, updatedAtA, 2)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sidA.String()), ShouldEqual, 1)

		writeAgeAllFixtureDGUTARecord(cfg, mountPath, updatedAtB, 3)

		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sidA.String()), ShouldEqual, 0)
		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sidB.String()), ShouldEqual, 1)
	})

	Convey("DGUTAWriter cleans up new snapshot if Close fails before switching", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")
		err = w.Add(singleDGUTARecord(dir, 111, "/child/"))
		So(err, ShouldBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		impl.failBeforeSwitchErr = errForcedFailure

		So(w.Close(), ShouldNotBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			testMountPath,
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
	})

	Convey("DGUTAWriter Abort cleans up new snapshot without switching", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")
		So(w.Add(singleDGUTARecord(dir, 444, "/child/")), ShouldBeNil)

		aborter, ok := w.(interface{ Abort() error })
		So(ok, ShouldBeTrue)
		So(aborter.Abort(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			testMountPath,
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
	})

	Convey("DGUTAWriter cleanup uses a fresh timeout when the close context is already done", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		impl.SetBatchSize(1)
		impl.SetMountPath(testMountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")
		So(impl.Add(singleDGUTARecord(dir, 333, "/child/")), ShouldBeNil)

		cleanupCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = impl.closeWithNewSnapshotCleanup(cleanupCtx, errForcedFailure)
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ctxCancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 0)
	})

	Convey("DGUTAWriter refuses to rewrite an active snapshot and Abort preserves published data", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		dir := paths.ToDirectoryPath("/")

		writeSingleDGUTARecord(cfg, updatedAt, dir, 42, "/foo/")

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		impl.SetBatchSize(1)
		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)

		err = w.Add(singleDGUTARecord(dir, 77, "/bar/"))
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotRewrite), ShouldBeTrue)

		aborter, ok := w.(interface{ Abort() error })
		So(ok, ShouldBeTrue)
		So(aborter.Abort(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			testMountPath,
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountChildrenQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)
	})

	Convey("DGUTAWriter publishes clean v1 facts and readiness rows on snapshot switch", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/", "/lustre/agentA/"}

		const mountPath = "/lustre/agentA/"

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("/"), 42, "/lustre/")), ShouldBeNil)
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("/lustre/"), 42, mountPath)), ShouldBeNil)
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath(mountPath), 42, mountPath+"deep/")), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			mountPath,
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_dir_projection_sets WHERE mount_path = ? AND snapshot_id = ?",
			mountPath,
			sid.String(),
		), ShouldBeGreaterThan, 0)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = ? AND dir = ?",
			mountPath,
			sid.String(),
			"/",
		), ShouldBeGreaterThan, 0)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = ? AND dir = ?",
			mountPath,
			sid.String(),
			mountPath,
		), ShouldEqual, 1)
	})

	Convey("DGUTAWriter writes mount dir projection rows without ClickHouse rebuild SQL", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/projections/"}

		const mountPath = "/mnt/projections/"

		updatedAt := time.Date(2026, 1, 11, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		trackedConn := &forbiddenProjectionRefreshConn{Conn: impl.conn}
		impl.conn = trackedConn
		impl.SetBatchSize(2)
		impl.SetMountPath(mountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 10),
				testProjectionGUTA(8, 9, db.DGUTAFileTypeDir, db.DGUTAgeAll, 2),
			},
			Children: []string{"alpha/", "beta/"},
		}), ShouldBeNil)
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath + "alpha/"),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3),
				testProjectionGUTA(7, 10, db.DGUTAFileTypeCram, db.DGUTAgeAll, 4),
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 2),
			},
			Children: []string{"leaf/"},
		}), ShouldBeNil)
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath + "beta/"),
			GUTAs: db.GUTAs{
				testProjectionGUTA(8, 9, db.DGUTAFileTypeDir, db.DGUTAgeAll, 6),
			},
		}), ShouldBeNil)
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath + "alpha/leaf/"),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1),
			},
		}), ShouldBeNil)
		So(impl.Close(), ShouldBeNil)
		So(trackedConn.forbiddenProjectionRefreshes(), ShouldEqual, 0)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_dir_projection_sets WHERE mount_path = ? AND snapshot_id = ?",
			mountPath,
			sid.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			"SELECT sum(child_count) FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = ? AND dir = ?",
			mountPath,
			sid.String(),
			mountPath,
		), ShouldEqual, 2)

		dbch := newClickHouseDatabase(cfg, conn)

		allSummaries, err := dbch.DirInfos(
			[]string{mountPath, mountPath + "alpha/", mountPath + "beta/"},
			&db.Filter{Age: db.DGUTAgeAll},
		)
		So(err, ShouldBeNil)
		So(allSummaries[mountPath].Count, ShouldEqual, 12)
		So(allSummaries[mountPath].GIDs, ShouldResemble, []uint32{7, 8})
		So(allSummaries[mountPath].FT, ShouldEqual, db.DGUTAFileTypeBam|db.DGUTAFileTypeDir)
		So(allSummaries[mountPath].Modtime, ShouldResemble, updatedAt)
		So(allSummaries[mountPath+"alpha/"].Count, ShouldEqual, 7)
		So(allSummaries[mountPath+"beta/"].Count, ShouldEqual, 6)

		fileSummaries, err := dbch.DirInfos(
			[]string{mountPath, mountPath + "beta/"},
			&db.Filter{Age: db.DGUTAgeAll, FT: db.AllTypesExceptDirectories},
		)
		So(err, ShouldBeNil)
		So(fileSummaries[mountPath].Count, ShouldEqual, 10)
		So(fileSummaries[mountPath].FT, ShouldEqual, db.DGUTAFileTypeBam)
		So(fileSummaries, ShouldNotContainKey, mountPath+"beta/")

		vectorSummaries, err := dbch.DirInfos(
			[]string{mountPath + "alpha/", mountPath + "alpha/leaf/"},
			&db.Filter{
				GIDs: []uint32{7},
				UIDs: []uint32{9},
				FT:   db.DGUTAFileTypeBam,
				Age:  db.DGUTAgeA1M,
			},
		)
		So(err, ShouldBeNil)
		So(vectorSummaries[mountPath+"alpha/"].Count, ShouldEqual, 2)
		So(vectorSummaries, ShouldNotContainKey, mountPath+"alpha/leaf/")

		ageOnlySummaries, err := dbch.DirInfos(
			[]string{mountPath + "alpha/"},
			&db.Filter{Age: db.DGUTAgeA1M},
		)
		So(err, ShouldBeNil)
		So(ageOnlySummaries[mountPath+"alpha/"].Count, ShouldEqual, 2)
	})

	Convey("DGUTAWriter streams one canonical facts row per mount directory with aligned vectors and children", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/"}

		const mountPath = "/mnt/"

		updatedAt := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		paths := internaltest.NewDirectoryPathCreator()

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		So(w.Add(db.RecordDGUTA{
			Dir:      paths.ToDirectoryPath("/mnt/"),
			Children: []string{"a/", "c/"},
		}), ShouldBeNil)
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath("/mnt/a/"),
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20, 100, 200),
				b1GUTA(7, 12, db.DGUTAFileTypeOther, db.DGUTAgeAll, 1, 5, 90, 150),
				b1GUTA(8, 11, db.DGUTAFileTypeBam, db.DGUTAgeA6M, 4, 40, 80, 250),
			},
			Children: []string{"b/"},
		}), ShouldBeNil)
		So(w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/mnt/a/b/")}), ShouldBeNil)
		So(w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/mnt/c/")}), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, dgutaWriterTestCountDirFactsQuery, mountPath, sid.String()), ShouldEqual, 4)

		fact := readDirFactForTest(ctx, conn, mountPath, sid.String(), "/mnt/a/")
		So(fact.gids, ShouldResemble, []uint32{7, 7, 8})
		So(fact.uids, ShouldResemble, []uint32{11, 12, 11})
		So(fact.fts, ShouldResemble, []uint16{
			uint16(db.DGUTAFileTypeBam),
			uint16(db.DGUTAFileTypeOther),
			uint16(db.DGUTAFileTypeBam),
		})
		So(fact.ages, ShouldResemble, []uint8{0, 0, 3})
		So(fact.counts, ShouldResemble, []uint64{2, 1, 4})
		So(fact.sizes, ShouldResemble, []uint64{20, 5, 40})
		So(fact.atimeMins, ShouldResemble, []int64{100, 90, 80})
		So(fact.mtimeMaxs, ShouldResemble, []int64{200, 150, 250})
		So(fact.atimeBuckets, ShouldHaveLength, 3)
		So(fact.mtimeBuckets, ShouldHaveLength, 3)
		So(fact.atimeBuckets[0], ShouldResemble, []uint64{2, 0, 0, 0, 0, 0, 0, 0, 0})
		So(fact.mtimeBuckets[2], ShouldResemble, []uint64{0, 4, 0, 0, 0, 0, 0, 0, 0})

		So(fact.allCount, ShouldEqual, 3)
		So(fact.allSize, ShouldEqual, 25)
		So(fact.allUIDs, ShouldResemble, []uint32{11, 12})
		So(fact.allGIDs, ShouldResemble, []uint32{7})
		So(fact.allFT&uint16(db.DGUTAFileTypeBam), ShouldBeGreaterThan, 0)
		So(fact.childCount, ShouldEqual, 1)

		childRows, err := conn.Query(ctx, dgutaWriterTestSelectChildQuery, mountPath, sid.String(), "/mnt/a/")
		So(err, ShouldBeNil)

		defer func() { _ = childRows.Close() }()

		So(childRows.Next(), ShouldBeTrue)

		var child string
		So(childRows.Scan(&child), ShouldBeNil)
		So(child, ShouldEqual, "/mnt/a/b")
		So(childRows.Next(), ShouldBeFalse)
	})

	Convey("DGUTAWriter writes only narrow AgeAll owner/type rows during import", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/a/"}

		const mountPath = "/mnt/a/"

		updatedAt := time.Date(2026, 6, 7, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20, 100, 200),
				b1GUTA(7, 12, db.DGUTAFileTypeOther, db.DGUTAgeAll, 1, 5, 90, 150),
				b1GUTA(8, 11, db.DGUTAFileTypeBam, db.DGUTAgeA2M, 4, 40, 80, 250),
			},
		}), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx,
			"SELECT count(), sum(count), sum(size), countIf(gid = 8) "+
				"FROM wrstat_dir_filter_ageall WHERE mount_path = ? AND snapshot_id = toUUID(?) AND dir = ?",
			mountPath,
			sid.String(),
			mountPath,
		)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var rowCount, totalCount, totalSize, ageSpecificRows uint64
		So(rows.Scan(&rowCount, &totalCount, &totalSize, &ageSpecificRows), ShouldBeNil)
		So(rows.Next(), ShouldBeFalse)
		So(rowCount, ShouldEqual, 2)
		So(totalCount, ShouldEqual, 3)
		So(totalSize, ShouldEqual, 25)
		So(ageSpecificRows, ShouldEqual, 0)
	})

	Convey("DGUTAWriter flushes final partial AgeAll import blocks", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/ageall-batch/"}

		const mountPath = "/mnt/ageall-batch/"

		updatedAt := time.Date(2026, 6, 7, 9, 30, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		trackedConn := &ageAllStreamingConn{Conn: impl.conn}
		impl.conn = trackedConn
		impl.SetBatchSize(2)
		impl.SetMountPath(mountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
				b1GUTA(7, 12, db.DGUTAFileTypeOther, db.DGUTAgeAll, 1, 10, 100, 200),
				b1GUTA(8, 11, db.DGUTAFileTypeCram, db.DGUTAgeAll, 1, 10, 100, 200),
				b1GUTA(8, 12, db.DGUTAFileTypeText, db.DGUTAgeAll, 1, 10, 100, 200),
				b1GUTA(9, 13, db.DGUTAFileTypeLog, db.DGUTAgeAll, 1, 10, 100, 200),
			},
		}), ShouldBeNil)
		So(impl.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sid.String()), ShouldEqual, 5)
		So(trackedConn.sentAgeAllRows(), ShouldResemble, []int{2, 2, 1})
	})

	Convey("A3.1 AgeAll rows without snapshot readiness fall back to facts summaries", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/ageall-not-ready/"}

		const mountPath = "/mnt/ageall-not-ready/"

		updatedAt := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
		mount := activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID(mountPath, updatedAt).String(),
			updatedAt:  updatedAt,
		}

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		insertA3AgeAllFactAndRow(ctx, conn, mount, mountPath, 7, 11, db.DGUTAFileTypeBam, 5)
		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, mount.snapshotID), ShouldEqual, 1)

		countingConn := &whereQueryCountingConn{Conn: conn}
		summaries, err := newClickHouseDatabase(cfg, countingConn).filteredMountWhereSummaries(
			mount,
			&db.Filter{
				Age:  db.DGUTAgeAll,
				GIDs: []uint32{7},
				UIDs: []uint32{11},
				FT:   db.DGUTAFileTypeBam,
			},
		)

		So(err, ShouldBeNil)
		So(summaries[mountPath].Count, ShouldEqual, 5)
		So(summaries[mountPath].Size, ShouldEqual, 50)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
	})

	Convey("A3.2 readiness for snapshot A does not enable AgeAll routing for active snapshot B", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/ageall-retry/"}

		const mountPath = "/mnt/ageall-retry/"

		updatedAtA := time.Date(2026, 6, 7, 12, 30, 0, 0, time.UTC)
		updatedAtB := updatedAtA.Add(time.Hour)
		mountA := activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID(mountPath, updatedAtA).String(),
			updatedAt:  updatedAtA,
		}
		mountB := activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID(mountPath, updatedAtB).String(),
			updatedAt:  updatedAtB,
		}

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		insertA3AgeAllFactAndRow(ctx, conn, mountA, mountPath, 7, 11, db.DGUTAFileTypeBam, 2)
		insertMountDirProjectionSetForTest(ctx, conn, mountA)
		insertA3AgeAllFactAndRow(ctx, conn, mountB, mountPath, 7, 11, db.DGUTAFileTypeBam, 9)
		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), mountB.snapshotID, updatedAtB), ShouldBeNil)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, mountB.snapshotID)

		countingConn := &whereQueryCountingConn{Conn: conn}
		summaries, err := newClickHouseDatabase(cfg, countingConn).filteredMountWhereSummaries(
			mountB,
			&db.Filter{
				Age:  db.DGUTAgeAll,
				GIDs: []uint32{7},
				UIDs: []uint32{11},
				FT:   db.DGUTAFileTypeBam,
			},
		)

		So(err, ShouldBeNil)
		So(summaries[mountPath].Count, ShouldEqual, 9)
		So(countingConn.filterAgeAllQueryCountValue(), ShouldEqual, 0)
		So(countingConn.filteredMountSummaryQueryCountValue(), ShouldEqual, 1)
	})

	Convey("A3.5 DGUTAWriter resets deterministic AgeAll partitions before retry import", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/ageall-reset/"}

		const mountPath = "/mnt/ageall-reset/"

		updatedAt := time.Date(2026, 6, 7, 14, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		insertA3AgeAllRow(ctx, conn, mountPath, sid.String(), mountPath, 90, 91, db.DGUTAFileTypeBam, 1)
		insertA3AgeAllRow(ctx, conn, mountPath, sid.String(), mountPath, 92, 93, db.DGUTAFileTypeCram, 1)
		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sid.String()), ShouldEqual, 2)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
				b1GUTA(8, 12, db.DGUTAFileTypeOther, db.DGUTAgeAll, 1, 10, 100, 200),
			},
		}), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		So(countRows(ctx, conn, dgutaWriterTestCountAgeAllQuery, mountPath, sid.String()), ShouldEqual, 2)
	})

	Convey("DGUTAWriter rolls back AgeAll rows on ambiguous close send", t, func() {
		updatedAt := time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC)
		previousSID := snapshotID(testMountPath, updatedAt.Add(-time.Hour))
		nextSID := snapshotID(testMountPath, updatedAt)
		conn := &ambiguousAgeAllSendConn{
			previousSID: previousSID.String(),
			sendErr:     errForcedFailure,
		}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  nextSID,
		}
		w.SetBatchSize(100)

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(testMountPath),
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200),
			},
			Children: []string{dgutaWriterTestChildName},
		}), ShouldBeNil)

		err := w.Close()
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.activePublishes(), ShouldEqual, 0)
		So(conn.previousSnapshotQueries(), ShouldBeGreaterThanOrEqualTo, 2)
		So(conn.partitionDrops(), ShouldContain, "wrstat_files")
		So(conn.partitionDrops(), ShouldContain, "wrstat_children")
		So(conn.partitionDrops(), ShouldContain, "wrstat_dir_facts")
		So(conn.partitionDrops(), ShouldContain, "wrstat_dir_filter_ageall")
		So(conn.partitionDrops(), ShouldContain, "wrstat_parent_facts")
		So(conn.partitionDrops(), ShouldContain, "wrstat_dir_projection_sets")
		So(conn.latestActiveSnapshot(), ShouldEqual, previousSID.String())
	})

	Convey("DGUTAWriter keeps AgeAll import heap growth bounded for generated facts", t, func() {
		factsDir := t.TempDir()
		updatedAt := time.Date(2026, 6, 7, 10, 30, 0, 0, time.UTC)
		mountPath := ensureTrailingSlash(factsDir)
		mount := activeMount{
			mountPath:  mountPath,
			snapshotID: snapshotID(mountPath, updatedAt).String(),
			updatedAt:  updatedAt,
		}
		conn := &lazyDGUTAImportConn{}
		writer := newDirFilterAgeAllWriter(conn, 10_000, updatedAt)

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		const facts = 1_000_000

		var writeErr error

		for n := range facts {
			dir := fmt.Sprintf("%sdir%06d/", mountPath, n)
			guta := b1GUTA(
				uint32(n%97),
				uint32(n%193),
				db.DGUTAFileTypeBam,
				db.DGUTAgeAll,
				1,
				10,
				100,
				200,
			)
			writeErr = errors.Join(writeErr, writer.appendRecord(
				context.Background(),
				mount,
				dir,
				db.GUTAs{guta},
				nil,
				nil,
			))
		}

		So(writeErr, ShouldBeNil)
		So(writer.flush(context.Background()), ShouldBeNil)

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		var growth uint64
		if after.HeapInuse > before.HeapInuse {
			growth = after.HeapInuse - before.HeapInuse
		}

		So(growth, ShouldBeLessThan, uint64(20*1024*1024))
		So(conn.totalRowsFor(insertDirFilterAgeAllQuery), ShouldEqual, facts)
		So(conn.maxRowsFor(insertDirFilterAgeAllQuery), ShouldBeLessThanOrEqualTo, 10_000)
	})

	Convey("DGUTAWriter accepts streamed child rows with final explicit child count", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		const mountPath = testT283ImagingMountPath

		updatedAt := time.Date(2026, 6, 4, 9, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		paths := internaltest.NewDirectoryPathCreator()

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)

		childWriter, ok := w.(db.DGUTAChildrenWriter)
		So(ok, ShouldBeTrue)

		parent := paths.ToDirectoryPath(mountPath + "wide/")
		So(childWriter.AddChildren(parent, []string{"child-a/", "child-b/"}), ShouldBeNil)
		So(w.Add(db.RecordDGUTA{
			Dir:        parent,
			ChildCount: 2,
			GUTAs: db.GUTAs{
				b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 20, 100, 200),
			},
		}), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fact := readDirFactForTest(ctx, conn, mountPath, sid.String(), mountPath+"wide/")
		So(fact.childCount, ShouldEqual, 2)

		childRows, err := conn.Query(ctx, dgutaWriterTestSelectChildQuery, mountPath, sid.String(), mountPath+"wide/")
		So(err, ShouldBeNil)

		defer func() { _ = childRows.Close() }()

		children := make([]string, 0, 2)

		for childRows.Next() {
			var child string
			So(childRows.Scan(&child), ShouldBeNil)
			children = append(children, child)
		}

		So(children, ShouldResemble, []string{mountPath + "wide/child-a", mountPath + "wide/child-b"})
	})

	Convey("DGUTAWriter derives file-only scalar hot summaries from AgeAll canonical facts", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/file-scalars/"}

		const mountPath = "/mnt/file-scalars/"

		updatedAt := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
		sid := snapshotID(mountPath, updatedAt)
		paths := internaltest.NewDirectoryPathCreator()

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(mountPath)
		w.SetUpdatedAt(updatedAt)
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				b1GUTAWithBuckets(
					7, 11, db.DGUTAFileTypeBam, 2, 20, 100, 200,
					[9]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
					[9]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
				),
				b1GUTAWithBuckets(
					8, 12, db.DGUTAFileTypeDir, 1, 5, 80, 250,
					[9]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
					[9]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
				),
				b1GUTAWithBuckets(
					8, 13, db.DGUTAFileTypeCram, 3, 30, 120, 180,
					[9]uint64{0, 0, 3, 0, 0, 0, 0, 0, 0},
					[9]uint64{0, 0, 3, 0, 0, 0, 0, 0, 0},
				),
			},
		}), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fact := readDirFactForTest(ctx, conn, mountPath, sid.String(), mountPath)
		So(fact.fileCount, ShouldEqual, 5)
		So(fact.fileSize, ShouldEqual, 50)
		So(fact.fileAtimeMin, ShouldEqual, 100)
		So(fact.fileMtimeMax, ShouldEqual, 200)
		So(fact.fileAtimeBuckets, ShouldResemble, []uint64{2, 0, 3, 0, 0, 0, 0, 0, 0})
		So(fact.fileMtimeBuckets, ShouldResemble, []uint64{0, 2, 3, 0, 0, 0, 0, 0, 0})
		So(fact.fileUIDs, ShouldResemble, []uint32{11, 13})
		So(fact.fileGIDs, ShouldResemble, []uint32{7, 8})
		So(fact.fileFT, ShouldEqual, uint16(db.DGUTAFileTypeBam|db.DGUTAFileTypeCram))
	})

	Convey("DGUTAWriter keeps t283-shaped fact streaming memory and block sizes bounded", t, func() {
		conn := &lazyDGUTAImportConn{}
		updatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		mountPath := testT283ImagingMountPath
		impl := &dgutaWriter{
			conn:      conn,
			mountPath: mountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(mountPath, updatedAt),
			prepared:  true,
			dirProjection: mountDirProjectionWriter{
				conn:        conn,
				refreshedAt: updatedAt,
			},
		}
		impl.SetBatchSize(10_000)

		runtime.GC()

		var usageBefore syscall.Rusage
		So(syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore), ShouldBeNil)

		root := &summary.DirectoryPath{Name: mountPath}

		const dirs = 230_000

		var addErr error

		for n := range dirs {
			dir := &summary.DirectoryPath{
				Name:   fmt.Sprintf("dir%06d/", n),
				Depth:  1,
				Parent: root,
			}
			addErr = errors.Join(addErr, impl.addReadyRecord(context.Background(), db.RecordDGUTA{
				Dir:   dir,
				GUTAs: b1T283GUTAs(n),
			}))
		}

		So(addErr, ShouldBeNil)
		So(impl.flushAllBatches(), ShouldBeNil)

		var usageAfter syscall.Rusage
		So(syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter), ShouldBeNil)

		So(maxRSSKilobytes(usageBefore, usageAfter), ShouldBeLessThan, int64(1_572_864))
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, dirs)
		So(conn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, 10_000)
	})

	Convey("DGUTAWriter streams selected import writers and does not rebuild facts with INSERT SELECT SQL", t, func() {
		updatedAt := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
		mountPath := "/mnt/sql-spy/"
		conn := &b1ImportSQLSpyConn{}
		impl := &dgutaWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: mountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(mountPath, updatedAt),
		}
		impl.SetBatchSize(1)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir:      paths.ToDirectoryPath(mountPath),
			GUTAs:    db.GUTAs{b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200)},
			Children: []string{dgutaWriterTestChildName},
		}), ShouldBeNil)
		So(impl.Close(), ShouldBeNil)

		So(conn.batchStats(insertMountDirSummaryQuery).appends, ShouldBeGreaterThan, 0)
		So(conn.batchStats(insertMountDirSummaryQuery).sends, ShouldBeGreaterThan, 0)
		So(conn.batchStats(insertChildrenQuery).appends, ShouldBeGreaterThan, 0)
		So(conn.batchStats(insertChildrenQuery).sends, ShouldBeGreaterThan, 0)
		So(conn.batchStats(insertDirFilterAgeAllQuery).appends, ShouldBeGreaterThan, 0)
		So(conn.batchStats(insertDirFilterAgeAllQuery).sends, ShouldBeGreaterThan, 0)
		So(conn.hasForbiddenInsertSelect(), ShouldBeFalse)
	})

	Convey("DGUTAWriter does not write child edges or publish readiness when facts writing fails", t, func() {
		updatedAt := time.Date(2026, 6, 1, 11, 30, 0, 0, time.UTC)
		mountPath := "/mnt/facts-fail/"
		conn := &b2PublishReadinessConn{factPrepareErr: errForcedFailure}
		impl := &dgutaWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: mountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(mountPath, updatedAt),
		}
		impl.SetBatchSize(1)

		paths := internaltest.NewDirectoryPathCreator()
		err := impl.Add(db.RecordDGUTA{
			Dir:      paths.ToDirectoryPath(mountPath),
			GUTAs:    db.GUTAs{b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200)},
			Children: []string{dgutaWriterTestChildName},
		})
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)

		closeErr := impl.Close()
		So(errors.Is(closeErr, errForcedFailure), ShouldBeTrue)
		So(conn.batchAppends(insertChildrenQuery), ShouldEqual, 0)
		So(conn.batchAppends(insertMountDirSummarySetQuery), ShouldEqual, 0)
		So(conn.activePublishes(), ShouldEqual, 0)
	})

	Convey("DGUTAWriter drops facts and withholds readiness when a selected derived index fails", t, func() {
		updatedAt := time.Date(2026, 6, 1, 11, 45, 0, 0, time.UTC)
		mountPath := "/mnt/derived-index-fail/"
		conn := &b2PublishReadinessConn{}
		derived := &b2FailingDerivedIndexWriter{flushErr: errForcedFailure}
		impl := &dgutaWriter{
			cfg:                    Config{QueryTimeout: time.Second},
			conn:                   conn,
			mountPath:              mountPath,
			updatedAt:              updatedAt,
			snapshot:               snapshotID(mountPath, updatedAt),
			selectedDerivedIndexes: []dgutaDerivedIndexWriter{derived},
		}
		impl.SetBatchSize(1)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir:      paths.ToDirectoryPath(mountPath),
			GUTAs:    db.GUTAs{b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 100, 200)},
			Children: []string{dgutaWriterTestChildName},
		}), ShouldBeNil)

		err := impl.Close()
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(derived.appendedRecords(), ShouldEqual, 1)
		So(conn.batchAppends(insertMountDirSummaryQuery), ShouldBeGreaterThan, 0)
		So(conn.batchAppends(insertMountDirSummarySetQuery), ShouldEqual, 0)
		So(conn.activePublishes(), ShouldEqual, 0)
		So(conn.dirFactPartitionDrops(), ShouldBeGreaterThan, 0)
	})

	Convey("DGUTAWriter streams mount dir projection batches before Close", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/streaming/"}

		const mountPath = "/mnt/streaming/"

		updatedAt := time.Date(2026, 1, 12, 9, 0, 0, 0, time.UTC)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)
		Reset(func() { So(impl.Abort(), ShouldBeNil) })

		trackedConn := &projectionStreamingConn{Conn: impl.conn}
		impl.conn = trackedConn
		impl.SetBatchSize(1)
		impl.SetMountPath(mountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 10),
			},
			Children: []string{"alpha/"},
		}), ShouldBeNil)

		So(trackedConn.sentProjectionBatches(), ShouldBeGreaterThan, 0)
		So(trackedConn.sentSummarySetBatches(), ShouldEqual, 0)
		So(impl.Close(), ShouldBeNil)
		So(trackedConn.sentSummarySetBatches(), ShouldEqual, 1)
	})

	Convey("DGUTAWriter flushes large records without oversize import batches", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/large-record/"}

		const mountPath = "/mnt/large-record/"

		updatedAt := time.Date(2026, 1, 13, 9, 0, 0, 0, time.UTC)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		trackedConn := newBatchFlushLimitConn(impl.conn)
		impl.conn = trackedConn
		impl.SetBatchSize(2)
		impl.SetMountPath(mountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 10),
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 9),
				testProjectionGUTA(7, 10, db.DGUTAFileTypeCram, db.DGUTAgeA2M, 8),
				testProjectionGUTA(8, 11, db.DGUTAFileTypeText, db.DGUTAgeA6M, 7),
				testProjectionGUTA(9, 12, db.DGUTAFileTypeDir, db.DGUTAgeM1Y, 6),
			},
			Children: []string{"a/", "b/", "c/", "d/", "e/"},
		}), ShouldBeNil)
		So(impl.Close(), ShouldBeNil)

		So(trackedConn.maxRowsFor(insertChildrenQuery), ShouldBeLessThanOrEqualTo, 2)
		So(trackedConn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, 2)
	})

	Convey("DGUTAWriter does not prepare empty import batches when writes become ready", t, func() {
		conn := &lazyDGUTAImportConn{}
		impl := &dgutaWriter{
			conn:      conn,
			mountPath: "/mnt/lazy-ready/",
			updatedAt: time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC),
		}

		So(impl.ensureWriteReady(context.Background()), ShouldBeNil)
		So(impl.prepared, ShouldBeTrue)
		So(conn.preparedBatches(), ShouldEqual, 0)
		So(impl.childrenBatch, ShouldBeNil)
		So(impl.dirProjection.summaryBatch, ShouldBeNil)
	})

	Convey("DGUTAWriter sends capped children blocks as separate prepared batches", t, func() {
		conn := &childrenSendPrepareConn{}
		firstBatch := &countingDGUTABatch{}
		impl := &dgutaWriter{
			conn:          conn,
			childrenBatch: firstBatch,
			mountPath:     "/mnt/children-cap/",
		}
		impl.SetBatchSize(100_000)

		var (
			appendErr error
			appended  int
		)

		for range defaultChildrenBatchSize + 1 {
			ok, err := impl.appendChildRow(
				"00000000-0000-0000-0000-000000000004",
				"/mnt/children-cap/",
				"child/",
			)
			appendErr = errors.Join(appendErr, err)

			if ok {
				appended++
			}
		}

		So(appendErr, ShouldBeNil)
		So(appended, ShouldEqual, defaultChildrenBatchSize+1)
		So(firstBatch.maxRows, ShouldEqual, defaultChildrenBatchSize)
		So(firstBatch.flushes, ShouldEqual, 0)
		So(firstBatch.sends, ShouldEqual, 1)
		So(conn.preparedBatches(), ShouldEqual, 1)
		So(conn.batches[0].Rows(), ShouldEqual, 1)
		So(impl.batchSize, ShouldEqual, 100_000)
		So(impl.effectiveChildrenBatchSize(), ShouldEqual, defaultChildrenBatchSize)
	})

	Convey("DGUTAWriter prepares children batches lazily and does not reprepare after send", t, func() {
		conn := &childrenSendPrepareConn{prepareErr: errForcedFailure}
		firstBatch := &countingDGUTABatch{}
		impl := &dgutaWriter{
			conn:          conn,
			childrenBatch: firstBatch,
			mountPath:     "/mnt/children-reprepare/",
		}
		impl.SetBatchSize(1)

		ok, err := impl.appendChildRow(
			"00000000-0000-0000-0000-000000000005",
			"/mnt/children-reprepare/",
			"child/",
		)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(firstBatch.flushes, ShouldEqual, 0)
		So(firstBatch.sends, ShouldEqual, 1)
		So(conn.preparedBatches(), ShouldEqual, 0)
		So(impl.childrenBatch, ShouldBeNil)

		var nextErr error

		So(func() {
			ok, nextErr = impl.appendChildRow(
				"00000000-0000-0000-0000-000000000005",
				"/mnt/children-reprepare/",
				"child/",
			)
		}, ShouldNotPanic)
		So(ok, ShouldBeFalse)
		So(errors.Is(nextErr, errForcedFailure), ShouldBeTrue)

		impl.mountPath = ""

		So(func() {
			nextErr = impl.Close()
		}, ShouldNotPanic)
		So(errors.Is(nextErr, errForcedFailure), ShouldBeTrue)
	})

	Convey("DGUTAWriter does not retry an ambiguous child batch send before cleanup", t, func() {
		updatedAt := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
		mountPath := "/mnt/ambiguous-send/"
		conn := &ambiguousDGUTASendConn{sendErr: errForcedFailure}
		impl := &dgutaWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: mountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(mountPath, updatedAt),
		}
		impl.SetBatchSize(1)

		paths := internaltest.NewDirectoryPathCreator()
		err := impl.Add(singleDGUTARecord(paths.ToDirectoryPath(mountPath), 42, "child/"))
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.childBatch.sendCalls, ShouldEqual, 1)

		err = impl.Close()
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.childBatch.sendCalls, ShouldEqual, 1)
		So(conn.currentSnapshotPartitionDrops(), ShouldEqual,
			len(allPartitionDropQueries())+len(allPartitionDropQueries()))
	})

	Convey("DGUTAWriter sends slow partial children batches before the receive timeout window", t, func() {
		now := time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC)

		conn := &childrenSendPrepareConn{}
		impl := &dgutaWriter{
			conn:      conn,
			mountPath: "/mnt/children-slow/",
			batchNow:  func() time.Time { return now },
		}
		impl.SetBatchSize(100)

		ok, err := impl.appendChildRow(
			"00000000-0000-0000-0000-000000000007",
			"/mnt/children-slow/",
			"child/",
		)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(conn.preparedBatches(), ShouldEqual, 1)

		firstBatch := conn.batches[0]
		So(firstBatch.Rows(), ShouldEqual, 1)
		So(firstBatch.sends, ShouldEqual, 0)

		now = now.Add(4*time.Minute + time.Second)

		ok, err = impl.appendChildRow(
			"00000000-0000-0000-0000-000000000007",
			"/mnt/children-slow/",
			"next/",
		)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(firstBatch.sends, ShouldEqual, 1)
		So(conn.preparedBatches(), ShouldEqual, 2)
		So(conn.batches[1].Rows(), ShouldEqual, 1)
	})

	Convey("DGUTAWriter sends the final partial children batch once", t, func() {
		conn := &childrenSendPrepareConn{}
		firstBatch := &countingDGUTABatch{}
		impl := &dgutaWriter{
			conn:          conn,
			childrenBatch: firstBatch,
			mountPath:     "/mnt/children-final/",
		}
		impl.SetBatchSize(2)

		for range 3 {
			ok, err := impl.appendChildRow(
				"00000000-0000-0000-0000-000000000006",
				"/mnt/children-final/",
				"child/",
			)
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
		}

		So(firstBatch.flushes, ShouldEqual, 0)
		So(firstBatch.sends, ShouldEqual, 1)
		So(conn.preparedBatches(), ShouldEqual, 1)

		finalBatch := conn.batches[0]
		So(finalBatch.Rows(), ShouldEqual, 1)
		So(impl.flushAllBatches(), ShouldBeNil)
		So(finalBatch.flushes, ShouldEqual, 0)
		So(finalBatch.sends, ShouldEqual, 1)
		So(impl.childrenBatch, ShouldBeNil)
	})

	Convey("DGUTAWriter tracks only ages for non-root duplicate checks", t, func() {
		impl := &dgutaWriter{
			mountPath: testT283ImagingMountPath,
			snapshot:  snapshotID(testT283ImagingMountPath, time.Date(2026, 5, 30, 9, 0, 0, 0, time.UTC)),
		}
		impl.SetBatchSize(100)

		record := db.RecordDGUTA{
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1),
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2),
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA2M, 3),
			},
		}

		appended, err := impl.appendDGUTARows(
			record,
			testT283ImagingMountPath+"a/",
			testT283ImagingMountPath+"a/",
		)
		So(err, ShouldBeNil)
		So(appended, ShouldHaveLength, 3)
		So(impl.previousDGUTARows.keys, ShouldBeNil)
		So(impl.previousDGUTARows.ages(), ShouldResemble, []db.DirGUTAge{db.DGUTAgeAll, db.DGUTAgeA2M})
	})

	Convey("DGUTAWriter sends capped clean fact blocks as separate prepared batches", t, func() {
		conn := &projectionSendPrepareConn{}
		writer := mountDirProjectionWriter{conn: conn, refreshedAt: time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC)}

		mount := activeMount{
			mountPath:  "/mnt/projection-send/",
			snapshotID: "00000000-0000-0000-0000-000000000001",
			updatedAt:  time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC),
		}
		So(writer.appendRecordWithContext(
			context.Background(),
			mount,
			"/mnt/projection-send/",
			db.GUTAs{testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1)},
			0,
			nil,
			false,
			1,
		), ShouldBeNil)

		summaryBatches := conn.factBatches()

		So(summaryBatches, ShouldHaveLength, 1)
		So(summaryBatches[0].flushes, ShouldEqual, 0)
		So(summaryBatches[0].sends, ShouldEqual, 1)
		So(summaryBatches[0].maxRows, ShouldEqual, 1)
		So(writer.summaryBatch, ShouldBeNil)
		So(writer.summaryFlushed, ShouldBeFalse)
		So(writer.abortAll(), ShouldBeNil)
	})

	Convey("DGUTAWriter prepares projection batches lazily and does not reprepare after send", t, func() {
		conn := &projectionSendPrepareConn{}
		writer := mountDirProjectionWriter{
			conn:        conn,
			refreshedAt: time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC),
		}

		mount := activeMount{
			mountPath:  "/mnt/projection-reprepare/",
			snapshotID: "00000000-0000-0000-0000-000000000002",
			updatedAt:  time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC),
		}
		err := writer.appendRecordWithContext(
			context.Background(),
			mount,
			"/mnt/projection-reprepare/",
			db.GUTAs{testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1)},
			0,
			nil,
			false,
			1,
		)
		So(err, ShouldBeNil)
		So(conn.factBatches()[0].flushes, ShouldEqual, 0)
		So(conn.factBatches()[0].sends, ShouldEqual, 1)
		So(conn.factBatches(), ShouldHaveLength, 1)
		So(writer.summaryBatch, ShouldBeNil)

		conn.prepareErrs = map[string]error{
			insertMountDirSummaryQuery: errForcedFailure,
		}

		var nextErr error

		So(func() {
			nextErr = writer.appendRecordWithContext(
				context.Background(),
				mount,
				"/mnt/projection-reprepare/",
				db.GUTAs{testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1)},
				0,
				nil,
				false,
				1,
			)
		}, ShouldNotPanic)
		So(errors.Is(nextErr, errForcedFailure), ShouldBeTrue)
		So(writer.abortAll(), ShouldBeNil)
	})

	Convey("DGUTAWriter sends slow partial projection batches before the receive timeout window", t, func() {
		now := time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC)

		conn := &projectionSendPrepareConn{}
		writer := mountDirProjectionWriter{
			conn:        conn,
			refreshedAt: now,
			batchNow:    func() time.Time { return now },
		}
		mount := activeMount{
			mountPath:  "/mnt/projection-slow/",
			snapshotID: "00000000-0000-0000-0000-000000000008",
			updatedAt:  now,
		}

		So(writer.appendRecordWithContext(
			context.Background(),
			mount,
			"/mnt/projection-slow/",
			db.GUTAs{testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1)},
			0,
			nil,
			false,
			100,
		), ShouldBeNil)

		summaryBatch := conn.factBatches()[0]

		So(summaryBatch.Rows(), ShouldEqual, 1)
		So(summaryBatch.sends, ShouldEqual, 0)

		now = now.Add(4*time.Minute + time.Second)

		So(writer.appendRecordWithContext(
			context.Background(),
			mount,
			"/mnt/projection-slow/next/",
			db.GUTAs{testProjectionGUTA(7, 10, db.DGUTAFileTypeCram, db.DGUTAgeA2M, 1)},
			0,
			nil,
			false,
			100,
		), ShouldBeNil)

		So(summaryBatch.sends, ShouldEqual, 1)
		So(conn.factBatches(), ShouldHaveLength, 2)
		So(conn.factBatches()[1].Rows(), ShouldEqual, 1)
	})

	Convey("Projection row helper prepares lazily and closes slow partial batches", t, func() {
		now := time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC)

		conn := &projectionSendPrepareConn{}
		appendCalls := 0

		err := writeProjectionRowsWithClock(
			context.Background(),
			conn,
			insertMountDirSummaryQuery,
			"dir summary",
			[]string{"first", "second"},
			100,
			func(batch driver.Batch, _ string) error {
				appendCalls++

				if err := batch.Append("row"); err != nil {
					return err
				}

				if appendCalls == 1 {
					now = now.Add(4*time.Minute + time.Second)
				}

				return nil
			},
			func() time.Time { return now },
		)
		So(err, ShouldBeNil)
		So(appendCalls, ShouldEqual, 2)

		batches := conn.factBatches()
		So(batches, ShouldHaveLength, 2)
		So(batches[0].sends, ShouldEqual, 1)
		So(batches[0].maxRows, ShouldEqual, 1)
		So(batches[1].sends, ShouldEqual, 1)
		So(batches[1].maxRows, ShouldEqual, 1)

		emptyConn := &projectionSendPrepareConn{}
		So(writeProjectionRows(
			context.Background(),
			emptyConn,
			insertMountDirSummaryQuery,
			"dir summary",
			[]string{},
			100,
			func(driver.Batch, string) error { return nil },
		), ShouldBeNil)
		So(emptyConn.factBatches(), ShouldHaveLength, 0)
	})

	Convey("DGUTAWriter sends the final partial projection batches once", t, func() {
		conn := &projectionSendPrepareConn{}
		writer := mountDirProjectionWriter{conn: conn, refreshedAt: time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC)}

		impl := &dgutaWriter{dirProjection: writer}
		impl.SetBatchSize(2)

		mount := activeMount{
			mountPath:  "/mnt/projection-final/",
			snapshotID: "00000000-0000-0000-0000-000000000003",
			updatedAt:  time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC),
		}
		So(impl.dirProjection.appendRecordWithContext(
			context.Background(),
			mount,
			"/mnt/projection-final/",
			db.GUTAs{testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1)},
			0,
			nil,
			false,
			2,
		), ShouldBeNil)

		summaryBatch := conn.factBatches()[0]

		So(summaryBatch.sends, ShouldEqual, 0)

		So(impl.flushAllBatches(), ShouldBeNil)
		So(summaryBatch.flushes, ShouldEqual, 0)
		So(summaryBatch.sends, ShouldEqual, 1)
		So(impl.dirProjection.summaryBatch, ShouldBeNil)
	})

	Convey("DGUTAWriter caps clean fact and children batches by default", t, func() {
		impl := &dgutaWriter{}

		impl.SetBatchSize(100_000)
		So(impl.batchSize, ShouldEqual, 100_000)
		So(impl.effectiveChildrenBatchSize(), ShouldEqual, defaultChildrenBatchSize)
		So(impl.projectionBatchSize, ShouldEqual, defaultProjectionBatchSize)

		impl.SetBatchSize(7)
		So(impl.batchSize, ShouldEqual, 7)
		So(impl.effectiveChildrenBatchSize(), ShouldEqual, 7)
		So(impl.projectionBatchSize, ShouldEqual, 7)
	})

	Convey("DGUTAWriter builds projection vector columns without cloning GUTA rows", t, func() {
		gutas := make(db.GUTAs, 128)
		for i := range gutas {
			gutas[i] = testProjectionGUTA(
				uint32(i%7),
				uint32(i%11),
				db.DGUTAFileTypeBam,
				db.DirGUTAges[i%len(db.DirGUTAges)],
				uint32(i+1),
			)
		}

		var columns mountDirProjectionVectorColumns

		allocs := testing.AllocsPerRun(20, func() {
			columns = mountDirProjectionVectorColumnsFor(gutas)
		})

		So(columns.gids, ShouldHaveLength, len(gutas))
		So(allocs, ShouldBeLessThan, 50.0)
	})

	Convey("DGUTAWriter keeps direct projection summaries scoped to age all", t, func() {
		gutas := db.GUTAs{
			testProjectionGUTA(1, 2, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3),
			testProjectionGUTA(1, 2, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 3),
			testProjectionGUTA(1, 2, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1),
		}

		allSummary, fileSummary := mountDirRecordSummaries(gutas)
		columns := mountDirProjectionVectorColumnsFor(gutas)

		So(allSummary.count, ShouldEqual, 4)
		So(fileSummary.count, ShouldEqual, 3)
		So(columns.counts, ShouldResemble, []uint64{3, 1, 3})
	})

	Convey("DGUTAWriter compacts t283-shaped internal age rows while preserving vectors", t, func() {
		conn := &lazyDGUTAImportConn{}
		updatedAt := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
		mountPath := testT283ImagingMountPath

		impl := &dgutaWriter{
			conn:      conn,
			mountPath: mountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(mountPath, updatedAt),
			prepared:  true,
			dirProjection: mountDirProjectionWriter{
				conn:        conn,
				refreshedAt: updatedAt,
			},
		}
		impl.SetBatchSize(100_000)

		const dirs = 100

		paths := internaltest.NewDirectoryPathCreator()

		for n := range dirs {
			record := db.RecordDGUTA{
				Dir:   paths.ToDirectoryPath(fmt.Sprintf("%sdir%04d/", mountPath, n)),
				GUTAs: t283DirectoryGUTAs(),
			}
			So(impl.addReadyRecord(context.Background(), record), ShouldBeNil)
		}

		So(impl.flushAllBatches(), ShouldBeNil)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, dirs)
	})

	Convey("DGUTAWriter keeps single-record projection appends allocation bounded", t, func() {
		conn := &lazyDGUTAImportConn{}
		updatedAt := time.Date(2026, 6, 1, 9, 30, 0, 0, time.UTC)
		mount := activeMount{
			mountPath:  testT283ImagingMountPath,
			snapshotID: snapshotID(testT283ImagingMountPath, updatedAt).String(),
			updatedAt:  updatedAt,
		}
		writer := mountDirProjectionWriter{
			conn:        conn,
			refreshedAt: updatedAt,
		}

		allocs := testing.AllocsPerRun(20, func() {
			So(writer.appendRecordWithContext(
				context.Background(),
				mount,
				testT283ImagingMountPath+"dir/",
				t283DirectoryGUTAs(),
				1,
				db.DirGUTAges[:],
				true,
				100_000,
			), ShouldBeNil)
		})

		So(allocs, ShouldBeLessThan, 65.0)
	})

	Convey("DGUTAWriter normalises mount paths before internal age compaction", t, func() {
		conn := &lazyDGUTAImportConn{}
		updatedAt := time.Date(2026, 6, 1, 9, 15, 0, 0, time.UTC)
		mountPath := testT283ImagingMountPath

		impl := &dgutaWriter{
			conn:      conn,
			updatedAt: updatedAt,
			prepared:  true,
			dirProjection: mountDirProjectionWriter{
				conn:        conn,
				refreshedAt: updatedAt,
			},
		}
		impl.SetBatchSize(100_000)
		impl.SetMountPath(strings.TrimSuffix(mountPath, "/"))
		impl.snapshot = snapshotID(impl.mountPath, updatedAt)
		So(impl.mountPath, ShouldEqual, mountPath)

		paths := internaltest.NewDirectoryPathCreator()
		for _, dir := range []string{
			mountPath,
			mountPath + "internal/",
			"/nfs/t283_imaging2/sibling/",
		} {
			So(impl.addReadyRecord(context.Background(), db.RecordDGUTA{
				Dir: paths.ToDirectoryPath(dir),
				GUTAs: db.GUTAs{
					testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1),
					testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 2),
					testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA2M, 3),
				},
			}), ShouldBeNil)
		}

		So(impl.flushAllBatches(), ShouldBeNil)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, 3)
	})

	Convey("DGUTAWriter flushes clean fact batches separately from child batches", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/mnt/separate-batches/"}

		const mountPath = "/mnt/separate-batches/"

		updatedAt := time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		impl, ok := w.(*dgutaWriter)
		So(ok, ShouldBeTrue)

		trackedConn := newBatchFlushLimitConn(impl.conn)
		impl.conn = trackedConn
		impl.SetBatchSize(4)
		impl.SetProjectionBatchSize(2)
		impl.SetMountPath(mountPath)
		impl.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 10),
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeA1M, 9),
				testProjectionGUTA(7, 10, db.DGUTAFileTypeCram, db.DGUTAgeA2M, 8),
				testProjectionGUTA(8, 11, db.DGUTAFileTypeText, db.DGUTAgeA6M, 7),
				testProjectionGUTA(9, 12, db.DGUTAFileTypeDir, db.DGUTAgeM1Y, 6),
			},
			Children: []string{"a/", "b/", "c/", "d/"},
		}), ShouldBeNil)
		So(impl.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(mountPath + "a/"),
			GUTAs: db.GUTAs{
				testProjectionGUTA(7, 9, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3),
			},
		}), ShouldBeNil)
		So(impl.Close(), ShouldBeNil)

		So(trackedConn.maxRowsFor(insertChildrenQuery), ShouldEqual, 4)
		So(trackedConn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, 2)
	})

	Convey("DGUTAWriter keeps the active snapshot when tree summary refresh times out", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/", testMountPath}

		updatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		sid := snapshotID(testMountPath, updatedAt)

		w, err := NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		w.SetMountPath(testMountPath)
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		So(w.Add(singleDGUTARecord(paths.ToDirectoryPath("/"), 42, testMountPath)), ShouldBeNil)

		So(w.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn,
			dgutaWriterTestCountActiveMountQuery,
			testMountPath,
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			dgutaWriterTestCountDirFactsQuery,
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_dir_projection_sets WHERE mount_path = ? AND snapshot_id = ?",
			testMountPath,
			sid.String(),
		), ShouldEqual, 1)
	})

	Convey("DGUTAWriter drops the previous snapshot when tree summary refresh exhausts close context", t, func() {
		updatedAt := time.Date(2026, 1, 9, 14, 0, 0, 0, time.UTC)
		previousSID := snapshotID(testMountPath, updatedAt.Add(-1*time.Hour))
		nextSID := snapshotID(testMountPath, updatedAt)
		conn := &dgutaWriterCloseContextConn{
			previousSID: previousSID.String(),
			nextSID:     nextSID.String(),
			updatedAt:   updatedAt,
		}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  nextSID,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		So(w.switchSnapshotAndDropOld(ctx), ShouldBeNil)
		So(conn.oldSnapshotPartitionDrops(), ShouldBeGreaterThan, 0)
		So(conn.treeSummaryRefreshes(), ShouldBeGreaterThan, 0)
	})
}

func countRows(ctx context.Context, conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}, query string, args ...any) uint64 {
	rows, err := conn.Query(ctx, query, args...)
	So(err, ShouldBeNil)

	defer func() { _ = rows.Close() }()

	So(rows.Next(), ShouldBeTrue)

	var n uint64
	So(rows.Scan(&n), ShouldBeNil)
	So(rows.Next(), ShouldBeFalse)

	return n
}

func writeSingleDGUTARecord(
	cfg Config,
	updatedAt time.Time,
	dir *summary.DirectoryPath,
	gid uint32,
	child string,
) {
	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	So(w, ShouldNotBeNil)

	w.SetMountPath(testMountPath)
	w.SetUpdatedAt(updatedAt)

	err = w.Add(singleDGUTARecord(dir, gid, child))
	So(err, ShouldBeNil)
	So(w.Close(), ShouldBeNil)
}

func writeAgeAllFixtureDGUTARecord(
	cfg Config,
	mountPath string,
	updatedAt time.Time,
	count uint64,
) {
	w, err := NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	So(w, ShouldNotBeNil)

	w.SetMountPath(mountPath)
	w.SetUpdatedAt(updatedAt)

	paths := internaltest.NewDirectoryPathCreator()
	So(w.Add(db.RecordDGUTA{
		Dir: paths.ToDirectoryPath(mountPath),
		GUTAs: db.GUTAs{
			b1GUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, count, count*10, 100, 200),
		},
	}), ShouldBeNil)
	So(w.Close(), ShouldBeNil)
}

func singleDGUTARecord(dir *summary.DirectoryPath, gid uint32, child string) db.RecordDGUTA {
	return db.RecordDGUTA{
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
	}
}

func insertA3AgeAllFactAndRow(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dir string,
	gid, uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
) {
	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mount.mountPath,
		mount.snapshotID,
		dir,
		gid,
		uid,
		uint16(ft),
		uint8(db.DGUTAgeAll),
		count,
		count*10,
		int64(100),
		int64(200),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
	insertA3AgeAllRow(ctx, conn, mount.mountPath, mount.snapshotID, dir, gid, uid, ft, count)
}

func insertA3AgeAllRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID, dir string,
	gid, uid uint32,
	ft db.DirGUTAFileType,
	count uint64,
) {
	So(conn.Exec(
		ctx,
		insertDirFilterAgeAllQuery,
		mountPath,
		snapshotID,
		gid,
		uid,
		uint16(ft),
		dir,
		count,
		count*10,
		int64(100),
		int64(200),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
		time.Now().UTC(),
	), ShouldBeNil)
}

func testProjectionGUTA(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count uint32,
) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         age,
		Count:       uint64(count),
		Size:        uint64(count) * 10,
		Atime:       int64(10 + count),
		Mtime:       int64(20 + count),
		ATimeRanges: [9]uint64{uint64(count), 0, 0, 0, 0, 0, 0, 0, 0},
		MTimeRanges: [9]uint64{0, uint64(count), 0, 0, 0, 0, 0, 0, 0},
	}
}

func b1GUTA(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count, size uint64,
	atime, mtime int64,
) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         age,
		Count:       count,
		Size:        size,
		Atime:       atime,
		Mtime:       mtime,
		ATimeRanges: [9]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		MTimeRanges: [9]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	}
}

func readDirFactForTest(ctx context.Context, conn ch.Conn, mountPath, sid, dir string) b1DirFact {
	rows, err := conn.Query(ctx, b1ReadDirFactQuery, mountPath, sid, dir)
	So(err, ShouldBeNil)

	defer func() { _ = rows.Close() }()

	So(rows.Next(), ShouldBeTrue)

	var fact b1DirFact
	So(rows.Scan(
		&fact.gids,
		&fact.uids,
		&fact.fts,
		&fact.ages,
		&fact.counts,
		&fact.sizes,
		&fact.atimeMins,
		&fact.mtimeMaxs,
		&fact.atimeBuckets,
		&fact.mtimeBuckets,
		&fact.allCount,
		&fact.allSize,
		&fact.allUIDs,
		&fact.allGIDs,
		&fact.allFT,
		&fact.fileCount,
		&fact.fileSize,
		&fact.fileAtimeMin,
		&fact.fileMtimeMax,
		&fact.fileAtimeBuckets,
		&fact.fileMtimeBuckets,
		&fact.fileUIDs,
		&fact.fileGIDs,
		&fact.fileFT,
		&fact.childCount,
	), ShouldBeNil)
	So(rows.Next(), ShouldBeFalse)

	return fact
}

func b1GUTAWithBuckets(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	count, size uint64,
	atime, mtime int64,
	atimeBuckets, mtimeBuckets [9]uint64,
) *db.GUTA {
	guta := b1GUTA(gid, uid, ft, db.DGUTAgeAll, count, size, atime, mtime)
	guta.ATimeRanges = atimeBuckets
	guta.MTimeRanges = mtimeBuckets

	return guta
}

func b1T283GUTAs(n int) db.GUTAs {
	age := db.DirGUTAges[n%len(db.DirGUTAges)]

	return db.GUTAs{
		b1GUTA(66, 15008, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1, 100, 200),
		b1GUTA(66, 15008, db.DGUTAFileTypeDir, age, 1, 1, 100, 200),
		b1GUTA(66, 15009, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10, 101, 201),
		b1GUTA(67, 15009, db.DGUTAFileTypeCram, age, 1, 20, 102, 202),
	}
}

func maxRSSKilobytes(before, after syscall.Rusage) int64 {
	if after.Maxrss > before.Maxrss {
		return after.Maxrss
	}

	return before.Maxrss
}

func newBatchFlushLimitConn(conn ch.Conn) *batchFlushLimitConn {
	return &batchFlushLimitConn{
		Conn:    conn,
		maxRows: make(map[string]int),
	}
}

func t283DirectoryGUTAs() db.GUTAs {
	gutas := make(db.GUTAs, 0, len(db.DirGUTAges))
	for _, age := range db.DirGUTAges {
		gutas = append(gutas, testProjectionGUTA(66, 15008, db.DGUTAFileTypeDir, age, 1))
	}

	return gutas
}

type activePrefixScalarRollupForTest struct {
	allCount   uint64
	fileCount  uint64
	childCount uint64
}

func readActivePrefixScalarRollupForTest(
	ctx context.Context,
	conn ch.Conn,
	activeSetID, dir string,
) activePrefixScalarRollupForTest {
	rows, err := conn.Query(ctx,
		"SELECT all_count, file_count, child_count FROM wrstat_active_prefix_rollups "+
			"WHERE active_set_id = ? AND dir = ?",
		activeSetID,
		dir,
	)
	So(err, ShouldBeNil)

	defer func() { _ = rows.Close() }()

	So(rows.Next(), ShouldBeTrue)

	var row activePrefixScalarRollupForTest
	So(rows.Scan(&row.allCount, &row.fileCount, &row.childCount), ShouldBeNil)
	So(rows.Next(), ShouldBeFalse)

	return row
}

func TestActivePrefixRollupsB1(t *testing.T) {
	Convey("B1.3-B1.5 active-prefix refresh writes ready scalar and AgeAll rows for the four-mount subset", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{"/"}

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatedAt := time.Date(2026, 6, 7, 9, 0, 0, 0, time.UTC)
		rows := []mountsActiveRow{
			seedActivePrefixB1Mount(ctx, conn, c3Scratch120Mount, updatedAt, 90000, 2),
			seedActivePrefixB1Mount(ctx, conn, "/lustre/scratch122/", updatedAt.Add(time.Minute), 90000, 2),
			seedActivePrefixB1Mount(ctx, conn, "/lustre/scratch127/", updatedAt.Add(2*time.Minute), 90000, 2),
			seedActivePrefixB1Mount(ctx, conn, testT283ImagingMountPath, updatedAt.Add(3*time.Minute), 83197, 3),
		}
		activeSetID := fingerprintForMountsActive(rows)

		So(ensureActivePrefixRollups(ctx, conn, rows), ShouldBeNil)

		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_active_prefix_rollups WHERE active_set_id = ? "+
				"AND dir IN ('/', '/lustre/', '/nfs/')",
			activeSetID,
		), ShouldEqual, 3)
		So(countRows(ctx, conn,
			"SELECT prefix_count FROM wrstat_active_prefix_rollup_sets WHERE active_set_id = ?",
			activeSetID,
		), ShouldEqual, 3)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_active_prefix_filter_ageall WHERE active_set_id = ? AND dir = '/'",
			activeSetID,
		), ShouldBeGreaterThan, 0)

		root := readActivePrefixScalarRollupForTest(ctx, conn, activeSetID, "/")
		So(root.allCount, ShouldEqual, 400000)
		So(root.fileCount, ShouldEqual, 353197)
		So(root.childCount, ShouldEqual, 9)

		So(conn.Exec(ctx, "OPTIM"+"IZE TABLE wrstat_active_prefix_rollups FINAL"), ShouldBeNil)

		explain, err := explainActivePrefixScalarRollupForTest(ctx, conn, activeSetID, "/")
		So(err, ShouldBeNil)

		readGranules, totalGranules, ok := explainGranules(explain)
		So(ok, ShouldBeTrue)
		So(readGranules, ShouldEqual, uint64(1))
		So(totalGranules, ShouldEqual, uint64(1))

		report := activePrefixScalarRollupPerfReportForTest(
			ctx,
			cfg,
			conn,
			activeSetID,
			"/",
			readGranules,
			totalGranules,
		)
		So(report.Operations, ShouldHaveLength, 1)

		op := report.Operations[0]
		So(op.Name, ShouldEqual, activePrefixScalarRollupPerfName)
		So(op.Inputs["read_granules"], ShouldEqual, readGranules)
		So(op.Inputs["total_granules"], ShouldEqual, totalGranules)
		So(op.DurationsMS, ShouldHaveLength, activePrefixScalarRollupRepeat)
		So(op.ReadRows, ShouldHaveLength, activePrefixScalarRollupRepeat)
		So(op.ResultCount, ShouldHaveLength, activePrefixScalarRollupRepeat)
		So(op.P50MS, ShouldBeLessThanOrEqualTo, float64(activePrefixScalarRollupP50MaxMS))
		So(op.P95MS, ShouldBeLessThanOrEqualTo, float64(activePrefixScalarRollupP95MaxMS))

		for _, readRows := range op.ReadRows {
			So(readRows, ShouldBeGreaterThan, uint64(0))
		}

		for _, resultCount := range op.ResultCount {
			So(resultCount, ShouldEqual, uint64(1))
		}
	})

	Convey("B1.6 active-prefix reader falls back after refresh writes rows but fails readiness publish", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		bootstrapProvider, err := OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(bootstrapProvider.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resetActivePrefixRollupMissesForTest()

		updatedAt := time.Date(2026, 6, 7, 9, 0, 0, 0, time.UTC)
		rows := []mountsActiveRow{
			seedActivePrefixB1Mount(ctx, conn, c3Scratch120Mount, updatedAt, 90000, 2),
		}
		activeSetID := fingerprintForMountsActive(rows)
		failingConn := &activePrefixRollupSetFailureConn{Conn: conn}

		err = refreshActivePrefixRollups(ctx, failingConn, rows, activeSetID)
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(failingConn.failedSetInserts(), ShouldEqual, 1)
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_active_prefix_rollups WHERE active_set_id = ? AND dir = '/'",
			activeSetID,
		), ShouldEqual, uint64(1))
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_active_prefix_filter_ageall WHERE active_set_id = ? AND dir = '/'",
			activeSetID,
		), ShouldBeGreaterThan, uint64(0))
		So(countRows(ctx, conn,
			"SELECT count() FROM wrstat_active_prefix_rollup_sets WHERE active_set_id = ?",
			activeSetID,
		), ShouldEqual, uint64(0))

		fallback := &db.DirSummary{Count: 42}
		fallbackCalls := 0

		got, err := activePrefixScalarRollupOrFallback(ctx, conn, activeSetID, "/", func() (*db.DirSummary, error) {
			fallbackCalls++

			return fallback, nil
		})

		So(err, ShouldBeNil)
		So(got, ShouldEqual, fallback)
		So(fallbackCalls, ShouldEqual, 1)
		So(activePrefixRollupMisses(), ShouldEqual, uint64(1))
	})
}

func seedActivePrefixB1Mount(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	updatedAt time.Time,
	fileCount, childCount uint64,
) mountsActiveRow {
	sid := snapshotID(mountPath, updatedAt)
	mount := activeMount{
		mountPath:  mountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}

	So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now().UTC(), sid, updatedAt), ShouldBeNil)
	insertActivePrefixB1Facts(ctx, conn, mount, "/", activePrefixB1AllCount, fileCount)
	insertActivePrefixB1Facts(ctx, conn, mount, activePrefixB1Namespace(mountPath), activePrefixB1AllCount, fileCount)
	insertActivePrefixB1Children(ctx, conn, mount, childCount)
	So(writeMaintainedMountDirProjectionForTest(ctx, conn, mount), ShouldBeNil)
	insertDirFilterAgeAllRowsFromFactsForTest(ctx, conn, mount)

	return mountsActiveRow{
		mountPath:  mountPath,
		snapshotID: sid.String(),
		updatedAt:  updatedAt,
	}
}

func insertActivePrefixB1Facts(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dir string,
	allCount, fileCount uint64,
) {
	insertActivePrefixB1Fact(ctx, conn, mount, dir, db.DGUTAFileTypeBam, fileCount)

	if allCount > fileCount {
		insertActivePrefixB1Fact(ctx, conn, mount, dir, db.DGUTAFileTypeDir, allCount-fileCount)
	}
}

func insertActivePrefixB1Fact(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	dir string,
	ft db.DirGUTAFileType,
	count uint64,
) {
	if count == 0 {
		return
	}

	So(conn.Exec(ctx,
		testInsertDGUTAStmt,
		mount.mountPath,
		mount.snapshotID,
		dir,
		uint32(14976),
		uint32(20155),
		uint16(ft),
		uint8(db.DGUTAgeAll),
		count,
		count*10,
		int64(100),
		int64(200),
		[]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	), ShouldBeNil)
}

func activePrefixB1Namespace(mountPath string) string {
	if strings.HasPrefix(mountPath, nfsAncestor) {
		return nfsAncestor
	}

	return "/lustre/"
}

func insertActivePrefixB1Children(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	childCount uint64,
) {
	mountName := strings.ReplaceAll(strings.Trim(mount.mountPath, "/"), "/", "-")
	for i := range childCount {
		child := fmt.Sprintf("/%s-child-%d", mountName, i)
		So(conn.Exec(ctx, testInsertChildrenStmt, mount.mountPath, mount.snapshotID, "/", child), ShouldBeNil)
	}
}

func explainActivePrefixScalarRollupForTest(
	ctx context.Context,
	conn ch.Conn,
	activeSetID, dir string,
) (string, error) {
	rows, err := conn.Query(ctx, explainPrefix+activePrefixScalarRollupReadQuery, activeSetID, dir)
	if err != nil {
		return "", err
	}

	defer func() { _ = rows.Close() }()

	lines := make([]string, 0)

	for rows.Next() {
		var line string
		So(rows.Scan(&line), ShouldBeNil)
		lines = append(lines, line)
	}

	So(rows.Err(), ShouldBeNil)

	return strings.Join(lines, "\n"), nil
}

func activePrefixScalarRollupPerfReportForTest(
	ctx context.Context,
	cfg Config,
	conn ch.Conn,
	activeSetID string,
	dir string,
	readGranules uint64,
	totalGranules uint64,
) perfreport.Report {
	inspector := &Inspector{cfg: cfg, conn: conn}

	_, _ = measureActivePrefixScalarRollupForTest(ctx, inspector, conn, activeSetID, dir)

	durations := make([]float64, 0, activePrefixScalarRollupRepeat)
	readRows := make([]uint64, 0, activePrefixScalarRollupRepeat)
	readBytes := make([]uint64, 0, activePrefixScalarRollupRepeat)
	readMarks := make([]uint64, 0, activePrefixScalarRollupRepeat)
	resultCounts := make([]uint64, 0, activePrefixScalarRollupRepeat)

	for range activePrefixScalarRollupRepeat {
		metrics, resultCount := measureActivePrefixScalarRollupForTest(ctx, inspector, conn, activeSetID, dir)

		durations = append(durations, float64(metrics.DurationMs))
		readRows = append(readRows, metrics.ReadRows)
		readBytes = append(readBytes, metrics.ReadBytes)
		readMarks = append(readMarks, metrics.ReadMarks)
		resultCounts = append(resultCounts, resultCount)
	}

	report := perfreport.NewReport(
		"clickhouse",
		"",
		activePrefixScalarRollupRepeat,
		activePrefixScalarRollupWarmup,
	)
	report.AddOperationWithCounters(
		activePrefixScalarRollupPerfName,
		map[string]any{
			"dir":             dir,
			"read_granules":   readGranules,
			"total_granules":  totalGranules,
			"duration_source": "clickhouse_query_log",
		},
		durations,
		readRows,
		readBytes,
		readMarks,
		resultCounts,
	)

	return report
}

func measureActivePrefixScalarRollupForTest(
	ctx context.Context,
	inspector *Inspector,
	conn ch.Conn,
	activeSetID string,
	dir string,
) (*QueryMetrics, uint64) {
	var found bool

	metrics, err := inspector.Measure(ctx, func(ctx context.Context) error {
		var measureErr error

		_, found, measureErr = readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)

		return measureErr
	})
	So(err, ShouldBeNil)
	So(found, ShouldBeTrue)
	So(metrics, ShouldNotBeNil)

	if found {
		return metrics, 1
	}

	return metrics, 0
}

type activePrefixRollupSetFailureConn struct {
	ch.Conn

	setInsertFailures atomic.Int32
}

func (c *activePrefixRollupSetFailureConn) Exec(ctx context.Context, query string, args ...any) error {
	if query == insertActivePrefixRollupSetQuery {
		c.setInsertFailures.Add(1)

		return errForcedFailure
	}

	return c.Conn.Exec(ctx, query, args...)
}

func (c *activePrefixRollupSetFailureConn) failedSetInserts() int {
	return int(c.setInsertFailures.Load())
}

type b1DirFact struct {
	gids         []uint32
	uids         []uint32
	fts          []uint16
	ages         []uint8
	counts       []uint64
	sizes        []uint64
	atimeMins    []int64
	mtimeMaxs    []int64
	atimeBuckets [][]uint64
	mtimeBuckets [][]uint64

	allCount uint64
	allSize  uint64
	allUIDs  []uint32
	allGIDs  []uint32
	allFT    uint16

	fileCount        uint64
	fileSize         uint64
	fileAtimeMin     int64
	fileMtimeMax     int64
	fileAtimeBuckets []uint64
	fileMtimeBuckets []uint64
	fileUIDs         []uint32
	fileGIDs         []uint32
	fileFT           uint16

	childCount uint64
}

type activeMetadataInvalidationConn struct {
	bootstrapTestConn
}

func (b *projectionStreamingBatch) Flush() error {
	if err := b.Batch.Flush(); err != nil {
		return err
	}

	b.recordSend()

	return nil
}

func (b *projectionStreamingBatch) recordSend() {
	if b.query == insertMountDirSummarySetQuery {
		b.conn.summarySetSends.Add(1)
	} else {
		b.conn.projectionSends.Add(1)
	}
}

type projectionStreamingConn struct {
	ch.Conn

	projectionSends atomic.Int32
	summarySetSends atomic.Int32
}

func (c *projectionStreamingConn) PrepareBatch(
	ctx context.Context,
	query string,
	opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		return nil, err
	}

	if !isMountDirProjectionInsert(query) {
		return batch, nil
	}

	return &projectionStreamingBatch{
		Batch: batch,
		query: query,
		conn:  c,
	}, nil
}

func isMountDirProjectionInsert(query string) bool {
	return query == insertMountDirSummaryQuery ||
		query == insertMountDirSummarySetQuery
}

func (c *projectionStreamingConn) sentProjectionBatches() int {
	return int(c.projectionSends.Load())
}

func (c *projectionStreamingConn) sentSummarySetBatches() int {
	return int(c.summarySetSends.Load())
}

type projectionStreamingBatch struct {
	driver.Batch

	query string
	conn  *projectionStreamingConn
}

func (b *projectionStreamingBatch) Send() error {
	if err := b.Batch.Send(); err != nil {
		return err
	}

	b.recordSend()

	return nil
}

type batchFlushLimitBatch struct {
	driver.Batch

	query string
	conn  *batchFlushLimitConn
}

func (b *batchFlushLimitBatch) Flush() error {
	b.conn.recordRows(b.query, b.Rows())

	return b.Batch.Flush()
}

func (b *batchFlushLimitBatch) Send() error {
	b.conn.recordRows(b.query, b.Rows())

	return b.Batch.Send()
}

type batchFlushLimitConn struct {
	ch.Conn

	maxRows map[string]int
}

func (c *batchFlushLimitConn) PrepareBatch(
	ctx context.Context,
	query string,
	opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		return nil, err
	}

	return &batchFlushLimitBatch{
		Batch: batch,
		query: query,
		conn:  c,
	}, nil
}

func (c *batchFlushLimitConn) maxRowsFor(query string) int {
	return c.maxRows[query]
}

func (c *batchFlushLimitConn) recordRows(query string, rows int) {
	if rows > c.maxRows[query] {
		c.maxRows[query] = rows
	}
}

type dgutaWriterCloseContextRows struct {
	columns []string
	values  [][]any
	index   int
}

func (r *dgutaWriterCloseContextRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}

	r.index++

	return true
}

func (r *dgutaWriterCloseContextRows) HasData() bool {
	return len(r.values) > 0
}

func (r *dgutaWriterCloseContextRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.values) {
		return errBootstrapTestUnexpectedCall
	}

	row := r.values[r.index-1]
	for i, value := range row {
		switch ptr := dest[i].(type) {
		case *string:
			str, ok := value.(string)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = str
		case *time.Time:
			t, ok := value.(time.Time)
			if !ok {
				return errBootstrapTestUnexpectedCall
			}

			*ptr = t
		default:
			return errBootstrapTestUnexpectedCall
		}
	}

	return nil
}

func (r *dgutaWriterCloseContextRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *dgutaWriterCloseContextRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *dgutaWriterCloseContextRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *dgutaWriterCloseContextRows) Columns() []string {
	return r.columns
}

func (r *dgutaWriterCloseContextRows) Close() error {
	return nil
}

func (r *dgutaWriterCloseContextRows) Err() error {
	return nil
}

type dgutaWriterCloseContextConn struct {
	bootstrapTestConn

	previousSID string
	nextSID     string
	updatedAt   time.Time

	oldSnapshotDrops atomic.Int32
	treeRefreshes    atomic.Int32
}

func (c *dgutaWriterCloseContextConn) Query(
	ctx context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{dgutaWriterTestSnapshotIDColumn},
			values:  [][]any{{c.previousSID}},
		}, nil
	case mountsActiveRowsQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{"mount_path", dgutaWriterTestSnapshotIDColumn, "updated_at"},
			values:  [][]any{{testMountPath, c.nextSID, c.updatedAt}},
		}, nil
	default:
		isTreeSummaryAvailabilityQuery := strings.Contains(query, "FROM wrstat_virtual_summary_sets") ||
			strings.Contains(query, "FROM wrstat_virtual_summary_cache")
		if isTreeSummaryAvailabilityQuery {
			c.treeRefreshes.Add(1)
			<-ctx.Done()

			return nil, ctx.Err()
		}

		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *dgutaWriterCloseContextConn) Exec(ctx context.Context, query string, _ ...any) error {
	switch {
	case query == switchSnapshotQuery:
		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		c.oldSnapshotDrops.Add(1)

		return ctx.Err()
	case strings.Contains(query, "INSERT INTO wrstat_virtual_summary_"):
		c.treeRefreshes.Add(1)
		<-ctx.Done()

		return ctx.Err()
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *dgutaWriterCloseContextConn) oldSnapshotPartitionDrops() int {
	return int(c.oldSnapshotDrops.Load())
}

func (c *dgutaWriterCloseContextConn) treeSummaryRefreshes() int {
	return int(c.treeRefreshes.Load())
}

func TestDGUTAWriterOldSnapshotDropUsesCleanupTimeout(t *testing.T) {
	Convey("DGUTAWriter uses the cleanup timeout when dropping the previous snapshot after switch", t, func() {
		updatedAt := time.Date(2026, 1, 9, 15, 0, 0, 0, time.UTC)
		queryTimeout := 100 * time.Millisecond
		previousSID := snapshotID(testMountPath, updatedAt.Add(-1*time.Hour))
		nextSID := snapshotID(testMountPath, updatedAt)
		conn := &oldSnapshotDropDeadlineConn{
			previousSID:  previousSID.String(),
			normalWindow: queryTimeout,
		}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: queryTimeout},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  nextSID,
		}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		So(w.switchSnapshotAndDropOld(ctx), ShouldBeNil)
		So(conn.oldSnapshotPartitionDrops(), ShouldEqual, len(allPartitionDropQueries()))
		So(conn.longDeadlineDrops(), ShouldEqual, len(allPartitionDropQueries()))
	})

	Convey("DGUTAWriter reports old snapshot drop failures after publishing the new snapshot", t, func() {
		updatedAt := time.Date(2026, 1, 9, 15, 0, 0, 0, time.UTC)
		previousSID := snapshotID(testMountPath, updatedAt.Add(-1*time.Hour))
		nextSID := snapshotID(testMountPath, updatedAt)
		conn := &oldSnapshotDropFailingConn{previousSID: previousSID.String()}
		w := &dgutaWriter{
			cfg:       Config{QueryTimeout: 100 * time.Millisecond},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  nextSID,
		}

		ctx, cancel := queryContext(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := w.switchSnapshotAndDropOld(ctx)
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "old_snapshot_partition_drop")
		So(conn.switchCount(), ShouldEqual, 1)
		So(conn.dropCount(), ShouldEqual, 1)
	})
}

type oldSnapshotDropDeadlineConn struct {
	bootstrapTestConn

	previousSID  string
	normalWindow time.Duration

	oldSnapshotDrops atomic.Int32
	longDeadline     atomic.Int32
}

func (c *oldSnapshotDropDeadlineConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errForcedFailure
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
		values:  [][]any{{c.previousSID}},
	}, nil
}

func (c *oldSnapshotDropDeadlineConn) Exec(ctx context.Context, query string, _ ...any) error {
	switch {
	case query == switchSnapshotQuery:
		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		c.oldSnapshotDrops.Add(1)

		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= c.normalWindow {
			return context.DeadlineExceeded
		}

		c.longDeadline.Add(1)

		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *oldSnapshotDropDeadlineConn) oldSnapshotPartitionDrops() int {
	return int(c.oldSnapshotDrops.Load())
}

func (c *oldSnapshotDropDeadlineConn) longDeadlineDrops() int {
	return int(c.longDeadline.Load())
}

type oldSnapshotDropFailingConn struct {
	bootstrapTestConn

	previousSID string

	switches atomic.Int32
	drops    atomic.Int32
}

func (c *oldSnapshotDropFailingConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errForcedFailure
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
		values:  [][]any{{c.previousSID}},
	}, nil
}

func (c *oldSnapshotDropFailingConn) Exec(_ context.Context, query string, _ ...any) error {
	switch {
	case query == switchSnapshotQuery:
		c.switches.Add(1)

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		c.drops.Add(1)

		return errForcedFailure
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *oldSnapshotDropFailingConn) switchCount() int {
	return int(c.switches.Load())
}

func (c *oldSnapshotDropFailingConn) dropCount() int {
	return int(c.drops.Load())
}

type dgutaRetryResetConn struct {
	bootstrapTestConn

	normalWindow time.Duration
	dropErr      error

	drops                  atomic.Int32
	longDeadline           atomic.Int32
	prepared               atomic.Bool
	preparedAfterDropCount atomic.Int32
	snapshotIDObservations []string
}

func (c *dgutaRetryResetConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
	}, nil
}

func (c *dgutaRetryResetConn) Exec(ctx context.Context, query string, args ...any) error {
	if !strings.HasPrefix(query, "ALTER TABLE") {
		return errBootstrapTestUnexpectedCall
	}

	c.drops.Add(1)

	if len(args) >= 2 {
		if sid, ok := args[1].(string); ok {
			c.snapshotIDObservations = append(c.snapshotIDObservations, sid)
		}
	}

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return context.DeadlineExceeded
	}

	c.longDeadline.Add(1)

	return c.dropErr
}

func (c *dgutaRetryResetConn) PrepareBatch(
	_ context.Context,
	_ string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepared.Store(true)
	c.preparedAfterDropCount.Store(c.drops.Load())

	return &countingDGUTABatch{}, nil
}

func (c *dgutaRetryResetConn) partitionDrops() int {
	return int(c.drops.Load())
}

func (c *dgutaRetryResetConn) longDeadlineDrops() int {
	return int(c.longDeadline.Load())
}

func (c *dgutaRetryResetConn) firstPreparedAfterDrops() bool {
	return int(c.preparedAfterDropCount.Load()) == len(allPartitionDropQueries())
}

func (c *dgutaRetryResetConn) seenSnapshotIDs() []string {
	seen := make(map[string]struct{}, len(c.snapshotIDObservations))

	ids := make([]string, 0, len(c.snapshotIDObservations))
	for _, sid := range c.snapshotIDObservations {
		if _, ok := seen[sid]; ok {
			continue
		}

		seen[sid] = struct{}{}
		ids = append(ids, sid)
	}

	return ids
}

type partitionDropDeadlineConn struct {
	bootstrapTestConn

	normalWindow time.Duration
	err          error

	drops           atomic.Int32
	cleanupDeadline atomic.Int32
}

func (c *partitionDropDeadlineConn) Exec(ctx context.Context, query string, _ ...any) error {
	if !strings.HasPrefix(query, "ALTER TABLE") {
		return errBootstrapTestUnexpectedCall
	}

	c.drops.Add(1)

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return context.DeadlineExceeded
	}

	c.cleanupDeadline.Add(1)

	return c.err
}

func (c *partitionDropDeadlineConn) partitionDrops() int {
	return int(c.drops.Load())
}

func (c *partitionDropDeadlineConn) cleanupDeadlineDrops() int {
	return int(c.cleanupDeadline.Load())
}

func TestPartitionDropUsesCleanupTimeout(t *testing.T) {
	Convey("Partition drops replace normal query deadlines with the cleanup timeout", t, func() {
		queryTimeout := 100 * time.Millisecond
		conn := &partitionDropDeadlineConn{normalWindow: queryTimeout}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		err := dropPartitionIgnoreUnknown(
			ctx,
			conn,
			testMountPath,
			"00000000-0000-0000-0000-000000000007",
			dropFilesPartitionQuery,
		)
		So(err, ShouldBeNil)
		So(conn.partitionDrops(), ShouldEqual, 1)
		So(conn.cleanupDeadlineDrops(), ShouldEqual, 1)
	})

	Convey("Partition drops still propagate real cleanup-timeout errors", t, func() {
		queryTimeout := 100 * time.Millisecond
		conn := &partitionDropDeadlineConn{
			normalWindow: queryTimeout,
			err:          errForcedFailure,
		}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		err := dropPartitionIgnoreUnknown(
			ctx,
			conn,
			testMountPath,
			"00000000-0000-0000-0000-000000000008",
			dropFilesPartitionQuery,
		)
		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.partitionDrops(), ShouldEqual, 1)
		So(conn.cleanupDeadlineDrops(), ShouldEqual, 1)
	})

	Convey("Partition drops ignore missing future snapshot tables", t, func() {
		queryTimeout := 100 * time.Millisecond
		conn := &partitionDropDeadlineConn{
			normalWindow: queryTimeout,
			err:          errUnknownParentFactsTable,
		}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		err := dropPartitionIgnoreUnknown(
			ctx,
			conn,
			testMountPath,
			"00000000-0000-0000-0000-000000000009",
			"ALTER TABLE wrstat_parent_facts DROP PARTITION tuple(?, toUUID(?))",
		)
		So(err, ShouldBeNil)
		So(conn.partitionDrops(), ShouldEqual, 1)
		So(conn.cleanupDeadlineDrops(), ShouldEqual, 1)
	})
}

type countingDGUTABatch struct {
	rows     int
	maxRows  int
	appended int
	sent     bool
	flushes  int
	sends    int
}

func (b *countingDGUTABatch) Abort() error {
	b.sent = true
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) Append(...any) error {
	b.rows++
	b.appended++

	return nil
}

func (b *countingDGUTABatch) AppendStruct(any) error {
	b.rows++
	b.appended++

	return nil
}

func (b *countingDGUTABatch) Column(int) driver.BatchColumn {
	return nil
}

func (b *countingDGUTABatch) Flush() error {
	b.recordRows()
	b.flushes++
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) Send() error {
	b.recordRows()
	b.sent = true
	b.sends++
	b.rows = 0

	return nil
}

func (b *countingDGUTABatch) IsSent() bool {
	return b.sent
}

func (b *countingDGUTABatch) Rows() int {
	return b.rows
}

func (b *countingDGUTABatch) Columns() []column.Interface {
	return nil
}

func (b *countingDGUTABatch) Close() error {
	return nil
}

func (b *countingDGUTABatch) recordRows() {
	if b.rows > b.maxRows {
		b.maxRows = b.rows
	}
}

type ambiguousDGUTASendBatch struct {
	countingDGUTABatch

	sendErr   error
	sendCalls int
}

func (b *ambiguousDGUTASendBatch) Send() error {
	b.sendCalls++
	if b.sendErr != nil {
		return b.sendErr
	}

	return b.countingDGUTABatch.Send()
}

type ambiguousDGUTASendConn struct {
	bootstrapTestConn

	sendErr error

	childBatch ambiguousDGUTASendBatch
	factBatch  countingDGUTABatch
	drops      atomic.Int32
}

func (c *ambiguousDGUTASendConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
	}, nil
}

func (c *ambiguousDGUTASendConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	switch query {
	case insertMountDirSummaryQuery:
		return &c.factBatch, nil
	case insertChildrenQuery:
		c.childBatch.sendErr = c.sendErr

		return &c.childBatch, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *ambiguousDGUTASendConn) Exec(_ context.Context, query string, _ ...any) error {
	if !strings.HasPrefix(query, "ALTER TABLE") {
		return errBootstrapTestUnexpectedCall
	}

	c.drops.Add(1)

	return nil
}

func (c *ambiguousDGUTASendConn) currentSnapshotPartitionDrops() int {
	return int(c.drops.Load())
}

type ambiguousAgeAllSendConn struct {
	bootstrapTestConn

	previousSID string
	sendErr     error

	ageAllBatch ambiguousDGUTASendBatch
	childBatch  countingDGUTABatch
	factBatch   countingDGUTABatch

	activeEvents  atomic.Int32
	activeQueries atomic.Int32
	drops         []string
}

func (c *ambiguousAgeAllSendConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	c.activeQueries.Add(1)

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
		values:  [][]any{{c.previousSID}},
	}, nil
}

func (c *ambiguousAgeAllSendConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	switch query {
	case insertMountDirSummaryQuery:
		return &c.factBatch, nil
	case insertChildrenQuery:
		return &c.childBatch, nil
	case insertDirFilterAgeAllQuery:
		c.ageAllBatch.sendErr = c.sendErr

		return &c.ageAllBatch, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *ambiguousAgeAllSendConn) Exec(_ context.Context, query string, _ ...any) error {
	switch {
	case query == switchSnapshotQuery:
		c.activeEvents.Add(1)

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		c.drops = append(c.drops, alterTableNameForTest(query))

		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func alterTableNameForTest(query string) string {
	fields := strings.Fields(query)
	for i := range fields {
		if i+2 >= len(fields) {
			break
		}

		if strings.EqualFold(fields[i], "ALTER") && strings.EqualFold(fields[i+1], "TABLE") {
			return fields[i+2]
		}
	}

	return ""
}

func (c *ambiguousAgeAllSendConn) activePublishes() int {
	return int(c.activeEvents.Load())
}

func (c *ambiguousAgeAllSendConn) previousSnapshotQueries() int {
	return int(c.activeQueries.Load())
}

func (c *ambiguousAgeAllSendConn) partitionDrops() []string {
	drops := make([]string, len(c.drops))
	copy(drops, c.drops)

	return drops
}

func (c *ambiguousAgeAllSendConn) latestActiveSnapshot() string {
	return c.previousSID
}

type childrenSendPrepareConn struct {
	bootstrapTestConn

	batches    []*countingDGUTABatch
	prepareErr error
}

func (c *childrenSendPrepareConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if query != insertChildrenQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	if c.prepareErr != nil {
		return nil, c.prepareErr
	}

	batch := &countingDGUTABatch{}
	c.batches = append(c.batches, batch)

	return batch, nil
}

func (c *childrenSendPrepareConn) preparedBatches() int {
	return len(c.batches)
}

type projectionSendPrepareConn struct {
	bootstrapTestConn

	batches     map[string][]*countingDGUTABatch
	prepareErrs map[string]error
}

func (c *projectionSendPrepareConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if !isMountDirProjectionInsert(query) {
		return nil, errBootstrapTestUnexpectedCall
	}

	if err := c.prepareErrs[query]; err != nil {
		return nil, err
	}

	batch := &countingDGUTABatch{}

	if c.batches == nil {
		c.batches = make(map[string][]*countingDGUTABatch)
	}

	c.batches[query] = append(c.batches[query], batch)

	return batch, nil
}

func (c *projectionSendPrepareConn) factBatches() []*countingDGUTABatch {
	return c.batches[insertMountDirSummaryQuery]
}

type lazyDGUTAImportConn struct {
	bootstrapTestConn

	batches map[string][]*countingDGUTABatch
}

func (c *lazyDGUTAImportConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
	}, nil
}

func (c *lazyDGUTAImportConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if !isLazyDGUTAImportQuery(query) {
		return nil, errBootstrapTestUnexpectedCall
	}

	batch := &countingDGUTABatch{}

	if c.batches == nil {
		c.batches = make(map[string][]*countingDGUTABatch)
	}

	c.batches[query] = append(c.batches[query], batch)

	return batch, nil
}

func isLazyDGUTAImportQuery(query string) bool {
	switch query {
	case insertChildrenQuery,
		insertMountDirSummaryQuery,
		insertDirFilterAgeAllQuery:
		return true
	default:
		return false
	}
}

func (c *lazyDGUTAImportConn) preparedBatches() int {
	var count int
	for _, batches := range c.batches {
		count += len(batches)
	}

	return count
}

func (c *lazyDGUTAImportConn) totalRowsFor(query string) int {
	var count int
	for _, batch := range c.batches[query] {
		count += batch.appended
	}

	return count
}

func (c *lazyDGUTAImportConn) maxRowsFor(query string) int {
	var maxRows int
	for _, batch := range c.batches[query] {
		if batch.maxRows > maxRows {
			maxRows = batch.maxRows
		}
	}

	return maxRows
}

type ageAllStreamingConn struct {
	ch.Conn

	ageAllRows []int
}

func (c *ageAllStreamingConn) PrepareBatch(
	ctx context.Context,
	query string,
	opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil || query != insertDirFilterAgeAllQuery {
		return batch, err
	}

	return &ageAllStreamingBatch{Batch: batch, conn: c}, nil
}

func (c *ageAllStreamingConn) sentAgeAllRows() []int {
	rows := make([]int, len(c.ageAllRows))
	copy(rows, c.ageAllRows)

	return rows
}

type ageAllStreamingBatch struct {
	driver.Batch

	conn *ageAllStreamingConn
}

func (b *ageAllStreamingBatch) Send() error {
	rows := b.Rows()
	if err := b.Batch.Send(); err != nil {
		return err
	}

	b.conn.ageAllRows = append(b.conn.ageAllRows, rows)

	return nil
}

type b1ImportSQLSpyBatch struct {
	countingDGUTABatch

	appends int
	sends   int
}

func (b *b1ImportSQLSpyBatch) Append(values ...any) error {
	b.appends++

	return b.countingDGUTABatch.Append(values...)
}

func (b *b1ImportSQLSpyBatch) Send() error {
	b.sends++

	return b.countingDGUTABatch.Send()
}

type b1ImportSQLSpyConn struct {
	bootstrapTestConn

	prepared []string
	batches  map[string]*b1ImportSQLSpyBatch
}

func (c *b1ImportSQLSpyConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	c.prepared = append(c.prepared, query)

	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{dgutaWriterTestSnapshotIDColumn},
		}, nil
	case mountsActiveRowsQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{"mount_path", dgutaWriterTestSnapshotIDColumn, "updated_at"},
		}, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *b1ImportSQLSpyConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepared = append(c.prepared, query)

	switch query {
	case insertChildrenQuery, insertMountDirSummaryQuery, insertDirFilterAgeAllQuery, insertMountDirSummarySetQuery:
	default:
		return nil, errBootstrapTestUnexpectedCall
	}

	if c.batches == nil {
		c.batches = make(map[string]*b1ImportSQLSpyBatch)
	}

	batch := &b1ImportSQLSpyBatch{}
	c.batches[query] = batch

	return batch, nil
}

func (c *b1ImportSQLSpyConn) batchStats(query string) b1ImportSQLSpyBatch {
	if c.batches == nil || c.batches[query] == nil {
		return b1ImportSQLSpyBatch{}
	}

	return *c.batches[query]
}

func (c *b1ImportSQLSpyConn) hasForbiddenInsertSelect() bool {
	queries := append(c.executedQueries(), c.prepared...)
	for _, query := range queries {
		if b1ForbiddenInsertSelect(query) {
			return true
		}
	}

	return false
}

func b1ForbiddenInsertSelect(query string) bool {
	normalised := strings.ToLower(strings.Join(strings.Fields(query), " "))
	if !strings.Contains(normalised, "insert into") || !strings.Contains(normalised, " select ") {
		return false
	}

	return strings.Contains(normalised, "wrstat_dir_facts") ||
		strings.Contains(normalised, "wrstat_children") ||
		strings.Contains(normalised, "wrstat_dir_filter_ageall")
}

type b2PublishReadinessConn struct {
	bootstrapTestConn

	factPrepareErr error
	batches        map[string]*countingDGUTABatch

	activeEvents atomic.Int32
	dirFactDrops atomic.Int32
}

func (c *b2PublishReadinessConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
	}, nil
}

func (c *b2PublishReadinessConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	switch query {
	case insertMountDirSummaryQuery:
		if c.factPrepareErr != nil {
			return nil, c.factPrepareErr
		}
	case insertChildrenQuery, insertDirFilterAgeAllQuery, insertMountDirSummarySetQuery:
	default:
		return nil, errBootstrapTestUnexpectedCall
	}

	if c.batches == nil {
		c.batches = make(map[string]*countingDGUTABatch)
	}

	batch := &countingDGUTABatch{}
	c.batches[query] = batch

	return batch, nil
}

func (c *b2PublishReadinessConn) Exec(_ context.Context, query string, _ ...any) error {
	switch {
	case query == switchSnapshotQuery:
		c.activeEvents.Add(1)

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		if strings.Contains(query, "wrstat_dir_facts") {
			c.dirFactDrops.Add(1)
		}

		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *b2PublishReadinessConn) batchAppends(query string) int {
	if c.batches == nil || c.batches[query] == nil {
		return 0
	}

	return c.batches[query].appended
}

func (c *b2PublishReadinessConn) activePublishes() int {
	return int(c.activeEvents.Load())
}

func (c *b2PublishReadinessConn) dirFactPartitionDrops() int {
	return int(c.dirFactDrops.Load())
}

type b2FailingDerivedIndexWriter struct {
	records atomic.Int32
	aborts  atomic.Int32

	flushErr error
}

func (w *b2FailingDerivedIndexWriter) appendRecord(
	context.Context,
	activeMount,
	string,
	db.GUTAs,
	[]string,
	[]db.DirGUTAge,
) error {
	w.records.Add(1)

	return nil
}

func (w *b2FailingDerivedIndexWriter) flush(context.Context) error {
	return w.flushErr
}

func (w *b2FailingDerivedIndexWriter) abort() error {
	w.aborts.Add(1)

	return nil
}

func (w *b2FailingDerivedIndexWriter) appendedRecords() int {
	return int(w.records.Load())
}
