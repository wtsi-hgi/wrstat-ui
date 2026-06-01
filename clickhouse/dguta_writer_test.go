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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var errForcedFailure = errors.New("forced failure")

const testMountPath = "/mnt/test/"

const testT283ImagingMountPath = "/nfs/t283_imaging/"

const dgutaWriterTestPhasePartitionDropReset = "partition_drop_reset"

const dgutaWriterTestSnapshotIDColumn = "snapshot_id"

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
	dgutaWriterTestCountDirFactsForDirQuery = "SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND dir = ?"
	dgutaWriterTestCountChildrenForParentQuery = "SELECT count() FROM wrstat_children WHERE mount_path = ? " +
		"AND snapshot_id = toUUID(?) AND parent_dir = ?"
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

		conn := th.openConn(cfg.DSN)

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

		conn := th.openConn(cfg.DSN)

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
		So(impl.dirProjection.vectorBatch, ShouldBeNil)
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
		So(writer.appendRecord(
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
		err := writer.appendRecord(
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
			nextErr = writer.appendRecord(
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

		So(writer.appendRecord(
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

		So(writer.appendRecord(
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
		So(impl.dirProjection.appendRecord(
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

	Convey("DGUTAWriter compacts t283-shaped internal age rows while preserving vectors", t, func() {
		conn := &lazyDGUTAImportConn{}
		updatedAt := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
		mountPath := "/nfs/t283_imaging/"

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
			So(impl.addReadyRecord(record), ShouldBeNil)
		}

		So(impl.flushAllBatches(), ShouldBeNil)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, dirs)
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
			So(impl.addReadyRecord(db.RecordDGUTA{
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
		query == insertMountDirDGUTAVectorQuery ||
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
		insertMountDirDGUTAVectorQuery:
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
