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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const (
	activeSnapshotCleanupCountMountRowsQuery = "SELECT count() FROM wrstat_mounts " +
		"WHERE mount_path = ? AND active_snapshot = toUUID(?)"
	activeSnapshotCleanupCountHistoryQuery = "SELECT count() FROM wrstat_basedirs_history " +
		"WHERE mount_path = ? AND gid = ?"
)

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
		insertSnapshotCleanupRows(ctx, conn, testMountPath, olderSID, olderUpdatedAt)
		insertSnapshotCleanupRows(ctx, conn, testMountPath, failedSID, failedUpdatedAt)
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

		So(countRows(ctx, conn, activeSnapshotCleanupCountMountRowsQuery, testMountPath, failedSID), ShouldEqual, 0)
		So(countRows(ctx, conn, activeSnapshotCleanupCountMountRowsQuery, testMountPath, olderSID), ShouldEqual, 1)
		So(countRows(ctx, conn, activeSnapshotCleanupCountHistoryQuery, testMountPath, uint32(7)), ShouldEqual, 1)
		assertSnapshotCleanupRows(ctx, conn, testMountPath, olderSID, 1)
		assertSnapshotCleanupRows(ctx, conn, testMountPath, failedSID, 0)
	})
}

func insertSnapshotCleanupRows(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	mountPath string,
	sid string,
	updatedAt time.Time,
) {
	atimeBuckets := []uint64{1, 0, 0, 0, 0, 0, 0, 0, 0}
	mtimeBuckets := []uint64{0, 1, 0, 0, 0, 0, 0, 0, 0}

	So(conn.Exec(
		ctx,
		testInsertDGUTAStmt,
		mountPath,
		sid,
		mountPath,
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
	So(conn.Exec(ctx, testInsertChildrenStmt, mountPath, sid, mountPath, mountPath+"child"), ShouldBeNil)
	So(conn.Exec(
		ctx,
		testInsertFileStmt,
		mountPath,
		sid,
		mountPath,
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
		mountPath,
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
		mountPath,
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
	insertSnapshotCleanupSubdirs(ctx, conn, mountPath, sid, updatedAt)
}

func insertSnapshotCleanupSubdirs(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	mountPath string,
	sid string,
	updatedAt time.Time,
) {
	fileUsage := map[uint16]uint64{uint16(db.DGUTAFileTypeBam): 1}

	So(conn.Exec(
		ctx,
		insertBasedirsGroupSubdirsQuery,
		mountPath,
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
		mountPath,
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
	mountPath string,
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
		So(countRows(ctx, conn, query, mountPath, sid), ShouldEqual, expected)
	}
}
