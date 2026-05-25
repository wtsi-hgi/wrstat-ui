/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const (
	activeSnapshotCleanupCountHistoryQuery = "SELECT count() FROM wrstat_basedirs_history " +
		"WHERE mount_path = ? AND gid = ?"
)

var errActiveSnapshotCleanupDeleteForbidden = errors.New("active snapshot cleanup delete should not run")

var errActiveSnapshotCleanupNormalDeadline = errors.New("active snapshot cleanup used normal query timeout")

type activeSnapshotCleanupDeleteRejectingConn struct {
	ch.Conn
	sawDelete bool
}

func (c *activeSnapshotCleanupDeleteRejectingConn) Exec(ctx context.Context, query string, args ...any) error {
	if query == deleteActiveSnapshotMountRowsQuery {
		c.sawDelete = true

		return errActiveSnapshotCleanupDeleteForbidden
	}

	return c.Conn.Exec(ctx, query, args...)
}

func TestCleanActiveSnapshotAttempt(t *testing.T) {
	Convey("CleanActiveSnapshotAttempt removes only the failed active snapshot and preserves older data", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		olderUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		failedUpdatedAt := olderUpdatedAt.Add(time.Hour)
		olderSID := snapshotID(testMountPath, olderUpdatedAt).String()
		failedSID := snapshotID(testMountPath, failedUpdatedAt).String()

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, olderUpdatedAt, olderSID, olderUpdatedAt), ShouldBeNil)
		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedUpdatedAt, failedSID, failedUpdatedAt), ShouldBeNil)
		insertSnapshotCleanupRows(ctx, conn, olderSID, olderUpdatedAt)
		insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)
		So(conn.Exec(
			ctx,
			testInsertBasedirsHistoryStmt,
			testMountPath,
			uint32(7),
			failedUpdatedAt,
			uint64(10),
			uint64(20),
			uint64(1),
			uint64(2),
		), ShouldBeNil)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, failedSID)

		So(CleanActiveSnapshotAttempt(cfg, testMountPath, failedUpdatedAt), ShouldBeNil)

		activeSID, hasActive, err = readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, olderSID)

		So(countRows(ctx, conn, activeSnapshotCleanupCountHistoryQuery, testMountPath, uint32(7)), ShouldEqual, 1)
		assertSnapshotCleanupRows(ctx, conn, olderSID, 1)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt rolls back to an older snapshot without deleting mount rows", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		olderUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		failedUpdatedAt := olderUpdatedAt.Add(time.Hour)
		olderSID := snapshotID(testMountPath, olderUpdatedAt).String()
		failedSID := snapshotID(testMountPath, failedUpdatedAt).String()

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, olderUpdatedAt, olderSID, olderUpdatedAt), ShouldBeNil)
		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedUpdatedAt, failedSID, failedUpdatedAt), ShouldBeNil)
		insertSnapshotCleanupRows(ctx, conn, olderSID, olderUpdatedAt)
		insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

		wrapped := &activeSnapshotCleanupDeleteRejectingConn{Conn: conn}
		So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
		So(wrapped.sawDelete, ShouldBeFalse)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, olderSID)
		assertSnapshotCleanupRows(ctx, conn, olderSID, 1)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt does not use the normal query timeout for unavoidable deletes", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		failedSID := snapshotID(testMountPath, failedUpdatedAt).String()

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedUpdatedAt, failedSID, failedUpdatedAt), ShouldBeNil)
		insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

		wrapped := &activeSnapshotCleanupDeadlineCheckingConn{
			Conn:         conn,
			queryTimeout: cfg.QueryTimeout,
		}
		So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
		So(wrapped.sawDelete, ShouldBeTrue)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})
}

func insertSnapshotCleanupRows(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	sid string,
	updatedAt time.Time,
) {
	atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
	mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

	So(conn.Exec(
		ctx,
		testInsertDGUTAStmt,
		testMountPath,
		sid,
		testMountPath,
		uint32(7),
		uint32(9),
		uint16(db.DGUTAFileTypeBam),
		uint8(db.DGUTAgeAll),
		uint64(2),
		uint64(123),
		int64(10),
		int64(20),
		atimeBuckets,
		mtimeBuckets,
	), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, testMountPath, sid, testMountPath, testMountPath+"child"), ShouldBeNil)
	So(conn.Exec(
		ctx,
		testInsertFileStmt,
		testMountPath,
		sid,
		testMountPath,
		"file.bam",
		"bam",
		uint8(stats.FileType),
		uint64(123),
		uint64(123),
		uint32(9),
		uint32(7),
		updatedAt,
		updatedAt,
		updatedAt,
		uint64(101),
		uint64(1),
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertBasedirsGroupUsageQuery,
		testMountPath,
		sid,
		uint32(7),
		basedirsStoreTestBaseDir,
		uint8(db.DGUTAgeAll),
		[]uint32{9},
		uint64(10),
		uint64(20),
		uint64(1),
		uint64(2),
		updatedAt,
		unixEpochUTC(),
		unixEpochUTC(),
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertBasedirsUserUsageQuery,
		testMountPath,
		sid,
		uint32(9),
		basedirsStoreTestBaseDir,
		uint8(db.DGUTAgeAll),
		[]uint32{7},
		uint64(10),
		uint64(20),
		uint64(1),
		uint64(2),
		updatedAt,
	), ShouldBeNil)
	insertSnapshotCleanupSubdirs(ctx, conn, sid, updatedAt)
}

func insertSnapshotCleanupSubdirs(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	sid string,
	updatedAt time.Time,
) {
	fileUsage := map[uint16]uint64{uint16(db.DGUTAFileTypeBam): 1}

	So(conn.Exec(
		ctx,
		insertBasedirsGroupSubdirsQuery,
		testMountPath,
		sid,
		uint32(7),
		basedirsStoreTestBaseDir,
		uint8(db.DGUTAgeAll),
		uint32(0),
		"child",
		uint64(1),
		uint64(10),
		updatedAt,
		fileUsage,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertBasedirsUserSubdirsQuery,
		testMountPath,
		sid,
		uint32(9),
		basedirsStoreTestBaseDir,
		uint8(db.DGUTAgeAll),
		uint32(0),
		"child",
		uint64(1),
		uint64(10),
		updatedAt,
		fileUsage,
	), ShouldBeNil)
}

func assertSnapshotCleanupRows(
	ctx context.Context,
	conn interface {
		Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	},
	sid string,
	expected uint64,
) {
	queries := []string{
		dgutaWriterTestCountDGUTAQuery,
		dgutaWriterTestCountChildrenQuery,
		filesIngestTestCountQuery,
		basedirsStoreTestCountGroupUsageQuery,
		"SELECT count() FROM wrstat_basedirs_user_usage WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		basedirsStoreTestCountGroupSubdirsQuery,
		basedirsStoreTestCountUserSubdirsQuery,
	}

	for _, query := range queries {
		So(countRows(ctx, conn, query, testMountPath, sid), ShouldEqual, expected)
	}
}

type activeSnapshotCleanupDeadlineCheckingConn struct {
	ch.Conn
	queryTimeout time.Duration
	sawDelete    bool
}

func (c *activeSnapshotCleanupDeadlineCheckingConn) Exec(ctx context.Context, query string, args ...any) error {
	if query == deleteActiveSnapshotMountRowsQuery {
		c.sawDelete = true

		deadline, ok := ctx.Deadline()
		if ok && time.Until(deadline) <= c.queryTimeout {
			return errActiveSnapshotCleanupNormalDeadline
		}
	}

	return c.Conn.Exec(ctx, query, args...)
}
