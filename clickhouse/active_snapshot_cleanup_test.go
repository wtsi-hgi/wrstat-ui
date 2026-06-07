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
	"fmt"
	"strings"
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
	activeSnapshotCleanupCountDirFactsQuery = "SELECT count() FROM wrstat_dir_facts " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	activeSnapshotCleanupCountAgeAllActivePartsQuery = "SELECT count() FROM system.parts " +
		"WHERE database = ? AND table = 'wrstat_dir_filter_ageall' AND active = 1 AND rows > 0"
	activeSnapshotCleanupInsertDirFactsQuery = "INSERT INTO wrstat_dir_facts " +
		"(mount_path, snapshot_id, dir, updated_at, all_count, all_size, refreshed_at) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?)"
)

var errActiveSnapshotCleanupDeleteForbidden = errors.New("active snapshot cleanup delete should not run")

var errActiveSnapshotCleanupNormalDeadline = errors.New("active snapshot cleanup used normal query timeout")

func TestActiveSetCleanupB3(t *testing.T) {
	Convey("B3.1 cleanup drops replaced active-prefix and virtual-child partitions and keeps replacement data",
		t, func() {
			env, ctx, cleanup := newB2ActivePrefixEnv(t)
			defer cleanup()

			oldSetID := env.activeSetID
			So(refreshActiveVirtualChildren(ctx, env.conn, env.rows), ShouldBeNil)
			So(ensureActivePrefixRollups(ctx, env.conn, env.rows), ShouldBeNil)
			assertActiveSetRowsPresent(ctx, env.conn, oldSetID)

			seedB2Mount(ctx, env.conn, b2MountSeed{
				mountPath:  c3Scratch120Mount,
				updatedAt:  time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC),
				bamCount:   300,
				otherCount: 30,
				dirCount:   3,
				childCount: 2,
			})

			rows, err := queryMountsActiveRows(ctx, env.conn)
			So(err, ShouldBeNil)

			newSetID := fingerprintForMountsActive(rows)
			So(newSetID, ShouldNotEqual, oldSetID)
			So(refreshActiveVirtualChildren(ctx, env.conn, rows), ShouldBeNil)
			So(ensureActivePrefixRollups(ctx, env.conn, rows), ShouldBeNil)
			assertActiveSetRowsPresent(ctx, env.conn, newSetID)

			So(cleanupOldVirtualChildrenSets(ctx, env.conn, newSetID), ShouldBeNil)
			So(cleanupOldActivePrefixRollupSets(ctx, env.conn, newSetID), ShouldBeNil)

			for _, table := range activeSetPartitionTablesForB3() {
				So(countActiveSetRowsForB3(ctx, env.conn, table, oldSetID), ShouldEqual, 0)
			}

			assertActiveSetRowsPresent(ctx, env.conn, newSetID)
		})
}

func activeSetPartitionTablesForB3() []string {
	return []string{
		"wrstat_virtual_children",
		"wrstat_virtual_children_sets",
		"wrstat_active_prefix_rollups",
		"wrstat_active_prefix_filter_ageall",
		"wrstat_active_prefix_rollup_sets",
	}
}

func assertActiveSetRowsPresent(ctx context.Context, conn ch.Conn, activeSetID string) {
	for _, table := range activeSetPartitionTablesForB3() {
		So(countActiveSetRowsForB3(ctx, conn, table, activeSetID), ShouldBeGreaterThan, 0)
	}
}

func countActiveSetRowsForB3(ctx context.Context, conn ch.Conn, table, activeSetID string) uint64 {
	return countRows(ctx, conn, fmt.Sprintf("SELECT count() FROM %s WHERE active_set_id = ?", table), activeSetID)
}

type activeSnapshotCleanupDeleteRejectingConn struct {
	ch.Conn
	sawDelete bool
}

func (c *activeSnapshotCleanupDeleteRejectingConn) Exec(ctx context.Context, query string, args ...any) error {
	if isActiveSnapshotCleanupMountDeleteQuery(query) {
		c.sawDelete = true

		return errActiveSnapshotCleanupDeleteForbidden
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isActiveSnapshotCleanupMountDeleteQuery(query string) bool {
	return strings.HasPrefix(query, "ALTER TABLE wrstat_mounts DELETE")
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

	Convey("CleanActiveSnapshotAttempt tombstones a failed snapshot with no previous mount row "+
		"without deleting", t, func() {
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

		wrapped := &activeSnapshotCleanupDeleteRejectingConn{Conn: conn}
		So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
		So(wrapped.sawDelete, ShouldBeFalse)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)

		matches, err := ActiveSnapshotMatches(cfg, testMountPath, failedUpdatedAt)
		So(err, ShouldBeNil)
		So(matches, ShouldBeFalse)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt leaves active metadata untouched when current partition drop times out",
		t, func() {
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

			wrapped := &activeSnapshotCleanupDropBlockingConn{
				Conn:          conn,
				normalTimeout: cfg.QueryTimeout,
			}

			err = cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "current_snapshot_partition_drop")
			So(wrapped.sawLongDropDeadline, ShouldBeTrue)
			So(wrapped.metadataChanges, ShouldEqual, 0)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeTrue)
			So(activeSID, ShouldEqual, failedSID)
			So(countRows(ctx, conn,
				"SELECT count() FROM wrstat_mount_events WHERE mount_path = ?",
				testMountPath,
			), ShouldEqual, 1)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 1)
		})

	Convey("CleanActiveSnapshotAttempt invalidates scoped active metadata caches after tombstoning", t, func() {
		resetSharedTreeQueryCachesForTesting()
		Reset(resetSharedTreeQueryCachesForTesting)

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

		cache := treeQueryCacheForConfig(cfg)
		key := newTreeCacheKey(testMountPath, failedSID, testMountPath)
		cache.putChildren(key, []string{testMountPath + "stale"})
		_, ok := cache.getChildren(key)
		So(ok, ShouldBeTrue)

		So(cleanActiveSnapshotAttemptWithConn(cfg, conn, testMountPath, failedUpdatedAt), ShouldBeNil)

		_, ok = cache.getChildren(key)
		So(ok, ShouldBeFalse)
	})

	Convey("CleanActiveSnapshotAttempt treats same-millisecond active rows as the cleanup baseline",
		t, func() {
			th := newClickHouseTestHarness(t)
			cfg := th.newConfig()
			cfg.QueryTimeout = 5 * time.Second

			failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
			failedSwitchAt := time.Date(2026, 1, 9, 12, 0, 1, 221*int(time.Millisecond), time.UTC)
			failedSID := snapshotID(testMountPath, failedUpdatedAt).String()

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)

			conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
			So(err, ShouldBeNil)

			Reset(func() { So(conn.Close(), ShouldBeNil) })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			insertSnapshotCleanupMountAt(ctx, conn, failedSwitchAt, failedSID, failedUpdatedAt)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			wrapped := &activeSnapshotCleanupCoarseLatestActiveParamConn{Conn: conn}
			So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
			So(wrapped.sawMillisecondBaseline, ShouldBeTrue)
			So(wrapped.sawCoarseTimeBaseline, ShouldBeFalse)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeFalse)
			So(activeSID, ShouldBeBlank)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("CleanActiveSnapshotAttempt repairs a no-effect inactive mount insert before dropping failed partitions",
		t, func() {
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

			wrapped := &activeSnapshotCleanupInactiveNoopConn{Conn: conn}
			So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
			So(wrapped.noopedInactiveInserts, ShouldEqual, 1)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeFalse)
			So(activeSID, ShouldBeBlank)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("CleanActiveSnapshotAttempt repairs no-effect tombstones using millisecond cleanup baselines",
		t, func() {
			th := newClickHouseTestHarness(t)
			cfg := th.newConfig()
			cfg.QueryTimeout = 5 * time.Second

			failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
			failedSwitchAt := time.Date(2026, 1, 9, 12, 0, 1, 221*int(time.Millisecond), time.UTC)
			failedSID := snapshotID(testMountPath, failedUpdatedAt).String()

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)

			conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
			So(err, ShouldBeNil)

			Reset(func() { So(conn.Close(), ShouldBeNil) })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			insertSnapshotCleanupMountAt(ctx, conn, failedSwitchAt, failedSID, failedUpdatedAt)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			wrapped := &activeSnapshotCleanupCoarseRepairParamConn{Conn: conn}
			So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
			So(wrapped.noopedInactiveInserts, ShouldEqual, 1)
			So(wrapped.sawMillisecondRepairBaseline, ShouldBeTrue)
			So(wrapped.noopedRepairs, ShouldEqual, 0)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeFalse)
			So(activeSID, ShouldBeBlank)
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

	Convey("CleanActiveSnapshotAttempt tombstones the failed snapshot when rollback does not displace it", t, func() {
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

		wrapped := &activeSnapshotCleanupRollbackNoopConn{Conn: conn}
		So(cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt), ShouldBeNil)
		So(wrapped.sawRollback, ShouldBeTrue)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)
		assertSnapshotCleanupRows(ctx, conn, olderSID, 1)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt reports a concurrent deterministic publish after the cleanup baseline",
		t, func() {
			th := newClickHouseTestHarness(t)
			cfg := th.newConfig()
			cfg.QueryTimeout = 5 * time.Second

			failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
			failedSID := snapshotID(testMountPath, failedUpdatedAt).String()
			failedSwitchAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)

			conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
			So(err, ShouldBeNil)

			Reset(func() { So(conn.Close(), ShouldBeNil) })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedSwitchAt, failedSID, failedUpdatedAt), ShouldBeNil)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			wrapped := &activeSnapshotCleanupConcurrentPublishConn{
				Conn:       conn,
				mountPath:  testMountPath,
				sid:        failedSID,
				updatedAt:  failedUpdatedAt,
				switchedAt: failedSwitchAt.Add(time.Second),
			}

			err = cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt)
			So(wrapped.published, ShouldBeTrue)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, errActiveSnapshotStillActive), ShouldBeTrue)
			So(err.Error(), ShouldContainSubstring, "newer than cleanup_baseline")
			So(err.Error(), ShouldContainSubstring, "latest_mount_rows=")
			So(wrapped.sawDelete, ShouldBeFalse)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeTrue)
			So(activeSID, ShouldEqual, failedSID)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("CleanActiveSnapshotAttempt refuses a newer active row hidden by an inactive tombstone", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
		failedSID := snapshotID(testMountPath, failedUpdatedAt).String()
		failedSwitchAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
		So(err, ShouldBeNil)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedSwitchAt, failedSID, failedUpdatedAt), ShouldBeNil)
		insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

		wrapped := &activeSnapshotCleanupHiddenActiveAfterInactiveConn{
			Conn:       conn,
			mountPath:  testMountPath,
			sid:        failedSID,
			updatedAt:  failedUpdatedAt,
			switchedAt: failedSwitchAt.Add(time.Second),
		}

		err = cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt)
		So(wrapped.published, ShouldBeTrue)
		So(wrapped.baseActiveChecked, ShouldBeTrue)
		So(wrapped.switchedAt.After(failedSwitchAt), ShouldBeTrue)
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotStillActive), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "newer than cleanup_baseline")
		So(err.Error(), ShouldContainSubstring, "latest_mount_rows=")

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt reports a concurrent publish for a different snapshot after the cleanup baseline",
		t, func() {
			th := newClickHouseTestHarness(t)
			cfg := th.newConfig()
			cfg.QueryTimeout = 5 * time.Second

			failedUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
			newerUpdatedAt := failedUpdatedAt.Add(time.Hour)
			failedSID := snapshotID(testMountPath, failedUpdatedAt).String()
			newerSID := snapshotID(testMountPath, newerUpdatedAt).String()
			failedSwitchAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)

			conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
			So(err, ShouldBeNil)

			Reset(func() { So(conn.Close(), ShouldBeNil) })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedSwitchAt, failedSID, failedUpdatedAt), ShouldBeNil)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			wrapped := &activeSnapshotCleanupConcurrentPublishConn{
				Conn:       conn,
				mountPath:  testMountPath,
				sid:        newerSID,
				updatedAt:  newerUpdatedAt,
				switchedAt: failedSwitchAt.Add(time.Second),
			}

			err = cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt)
			So(wrapped.published, ShouldBeTrue)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, errActiveSnapshotStillActive), ShouldBeTrue)
			So(err.Error(), ShouldContainSubstring, "newer than cleanup_baseline")
			So(err.Error(), ShouldContainSubstring, "latest_mount_rows=")
			So(err.Error(), ShouldContainSubstring, newerSID)

			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("CleanActiveSnapshotAttempt does not roll back to a snapshot tombstoned before the failed publish",
		t, func() {
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
			So(conn.Exec(
				ctx,
				testInsertInactiveMountStmt,
				testMountPath,
				olderUpdatedAt.Add(time.Millisecond),
				olderSID,
				olderUpdatedAt,
			), ShouldBeNil)
			So(conn.Exec(ctx, testInsertMountStmt, testMountPath, failedUpdatedAt, failedSID, failedUpdatedAt), ShouldBeNil)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			So(cleanActiveSnapshotAttemptWithConn(cfg, conn, testMountPath, failedUpdatedAt), ShouldBeNil)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeFalse)
			So(activeSID, ShouldBeBlank)
			assertSnapshotCleanupRows(ctx, conn, olderSID, 0)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("CleanActiveSnapshotAttempt keeps a mount inactive when the failed snapshot was tombstoned before publish",
		t, func() {
			th := newClickHouseTestHarness(t)
			cfg := th.newConfig()
			cfg.QueryTimeout = 5 * time.Second

			olderUpdatedAt := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)
			failedUpdatedAt := olderUpdatedAt.Add(time.Hour)
			olderSID := snapshotID(testMountPath, olderUpdatedAt).String()
			failedSID := snapshotID(testMountPath, failedUpdatedAt).String()
			olderSwitchAt := time.Date(2026, 1, 9, 13, 0, 0, 0, time.UTC)
			failedInactiveAt := olderSwitchAt.Add(time.Second)
			failedSwitchAt := failedInactiveAt.Add(time.Second)

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)

			conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
			So(err, ShouldBeNil)

			Reset(func() { So(conn.Close(), ShouldBeNil) })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			insertSnapshotCleanupMountAt(ctx, conn, olderSwitchAt, olderSID, olderUpdatedAt)
			So(conn.Exec(
				ctx,
				testInsertInactiveMountStmt,
				testMountPath,
				failedInactiveAt,
				failedSID,
				failedUpdatedAt,
			), ShouldBeNil)
			insertSnapshotCleanupMountAt(ctx, conn, failedSwitchAt, failedSID, failedUpdatedAt)
			insertSnapshotCleanupRows(ctx, conn, olderSID, olderUpdatedAt)
			insertSnapshotCleanupRows(ctx, conn, failedSID, failedUpdatedAt)

			So(cleanActiveSnapshotAttemptWithConn(cfg, conn, testMountPath, failedUpdatedAt), ShouldBeNil)

			activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
			So(err, ShouldBeNil)
			So(hasActive, ShouldBeFalse)
			So(activeSID, ShouldBeBlank)
			assertSnapshotCleanupRows(ctx, conn, olderSID, 1)
			assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
		})

	Convey("A3.4 CleanActiveSnapshotAttempt drops AgeAll active parts on tombstone cleanup", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		failedUpdatedAt := time.Date(2026, 6, 7, 13, 0, 0, 0, time.UTC)
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
		So(countRows(ctx, conn, activeSnapshotCleanupCountAgeAllActivePartsQuery, cfg.Database), ShouldBeGreaterThan, 0)

		So(cleanActiveSnapshotAttemptWithConn(cfg, conn, testMountPath, failedUpdatedAt), ShouldBeNil)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)
		So(activeSID, ShouldBeBlank)
		So(countRows(ctx, conn, activeSnapshotCleanupCountAgeAllActivePartsQuery, cfg.Database), ShouldEqual, 0)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt reports latest mount metadata when repair cannot deactivate a snapshot", t, func() {
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

		wrapped := &activeSnapshotCleanupMetadataNoopConn{Conn: conn}

		err = cleanActiveSnapshotAttemptWithConn(cfg, wrapped, testMountPath, failedUpdatedAt)
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotStillActive), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "latest_mount_rows=")
		So(err.Error(), ShouldContainSubstring, failedSID)
		So(wrapped.sawDelete, ShouldBeFalse)
		assertSnapshotCleanupRows(ctx, conn, failedSID, 0)
	})

	Convey("CleanActiveSnapshotAttempt does not use the normal query timeout for cleanup metadata changes", t, func() {
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
		So(wrapped.sawMetadataChange, ShouldBeTrue)

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
	So(conn.Exec(
		ctx,
		activeSnapshotCleanupInsertDirFactsQuery,
		testMountPath,
		sid,
		testMountPath,
		updatedAt,
		uint64(2),
		uint64(123),
		updatedAt,
	), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertParentFactsQuery,
		testMountPath,
		sid,
		"/mnt/",
		testMountPath,
		updatedAt,
		uint64(2),
		uint64(123),
		int64(100),
		int64(200),
		[]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
		[]uint32{9},
		[]uint32{7},
		uint16(db.DGUTAFileTypeBam),
		uint64(2),
		uint64(123),
		int64(100),
		int64(200),
		[]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
		[]uint32{9},
		[]uint32{7},
		uint16(db.DGUTAFileTypeBam),
		[]uint32{7},
		[]uint32{9},
		[]uint16{uint16(db.DGUTAFileTypeBam)},
		[]uint8{uint8(db.DGUTAgeAll)},
		[]uint64{2},
		[]uint64{123},
		[]int64{100},
		[]int64{200},
		[][]uint64{{2, 0, 0, 0, 0, 0, 0, 0, 0}},
		[][]uint64{{0, 2, 0, 0, 0, 0, 0, 0, 0}},
		uint64(1),
		uint8(1),
		updatedAt,
	), ShouldBeNil)
	So(conn.Exec(ctx, testInsertChildrenStmt, testMountPath, sid, testMountPath, testMountPath+"child"), ShouldBeNil)
	So(conn.Exec(
		ctx,
		insertDirFilterAgeAllQuery,
		testMountPath,
		sid,
		uint32(7),
		uint32(9),
		uint16(db.DGUTAFileTypeBam),
		testMountPath,
		uint64(2),
		uint64(123),
		int64(100),
		int64(200),
		[]uint64{2, 0, 0, 0, 0, 0, 0, 0, 0},
		[]uint64{0, 2, 0, 0, 0, 0, 0, 0, 0},
		updatedAt,
	), ShouldBeNil)
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
		activeSnapshotCleanupCountDirFactsQuery,
		dgutaWriterTestCountParentFactsQuery,
		dgutaWriterTestCountChildrenQuery,
		dgutaWriterTestCountAgeAllQuery,
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

func insertSnapshotCleanupMountAt(
	ctx context.Context,
	conn interface {
		Exec(ctx context.Context, query string, args ...any) error
	},
	switchedAt time.Time,
	sid string,
	updatedAt time.Time,
) {
	const query = "INSERT INTO wrstat_mount_events (mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, fromUnixTimestamp64Milli(?), 1, toUUID(?), ?, 'test'"

	So(conn.Exec(ctx, query, testMountPath, switchedAt.UTC().UnixMilli(), sid, updatedAt), ShouldBeNil)
}

type activeSnapshotCleanupDropBlockingConn struct {
	ch.Conn

	normalTimeout       time.Duration
	sawLongDropDeadline bool
	metadataChanges     int
}

func (c *activeSnapshotCleanupDropBlockingConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "ALTER TABLE") {
		deadline, ok := ctx.Deadline()
		if ok && time.Until(deadline) > c.normalTimeout {
			c.sawLongDropDeadline = true
		}

		return context.DeadlineExceeded
	}

	if isActiveSnapshotCleanupMetadataChangeQuery(query) {
		c.metadataChanges++
	}

	return c.Conn.Exec(ctx, query, args...)
}

type activeSnapshotCleanupDeadlineCheckingConn struct {
	ch.Conn
	queryTimeout      time.Duration
	sawMetadataChange bool
}

func (c *activeSnapshotCleanupDeadlineCheckingConn) Exec(ctx context.Context, query string, args ...any) error {
	if isActiveSnapshotCleanupMetadataQuery(query) {
		c.sawMetadataChange = true

		deadline, ok := ctx.Deadline()
		if ok && time.Until(deadline) <= c.queryTimeout {
			return errActiveSnapshotCleanupNormalDeadline
		}
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isActiveSnapshotCleanupMetadataQuery(query string) bool {
	return query == insertInactiveSnapshotMountRowQuery ||
		query == repairInactiveSnapshotMountRowQuery ||
		query == rollbackActiveSnapshotMountRowQuery ||
		isActiveSnapshotCleanupMountDeleteQuery(query)
}

type activeSnapshotCleanupRollbackNoopConn struct {
	ch.Conn
	sawRollback bool
}

func (c *activeSnapshotCleanupRollbackNoopConn) Exec(ctx context.Context, query string, args ...any) error {
	if query == rollbackActiveSnapshotMountRowQuery {
		c.sawRollback = true

		return nil
	}

	return c.Conn.Exec(ctx, query, args...)
}

type activeSnapshotCleanupInactiveNoopConn struct {
	ch.Conn
	noopedInactiveInserts int
}

func (c *activeSnapshotCleanupInactiveNoopConn) Exec(ctx context.Context, query string, args ...any) error {
	if query == insertInactiveSnapshotMountRowQuery && c.noopedInactiveInserts == 0 {
		c.noopedInactiveInserts++

		return nil
	}

	return c.Conn.Exec(ctx, query, args...)
}

type activeSnapshotCleanupConcurrentPublishConn struct {
	ch.Conn
	mountPath  string
	sid        string
	updatedAt  time.Time
	switchedAt time.Time
	published  bool
	sawDelete  bool
}

func (c *activeSnapshotCleanupConcurrentPublishConn) Exec(ctx context.Context, query string, args ...any) error {
	if isActiveSnapshotCleanupMountDeleteQuery(query) {
		c.sawDelete = true

		return errActiveSnapshotCleanupDeleteForbidden
	}

	if query == insertInactiveSnapshotMountRowQuery && !c.published {
		c.published = true

		return c.Conn.Exec(ctx, testInsertMountStmt, c.mountPath, c.switchedAt, c.sid, c.updatedAt)
	}

	return c.Conn.Exec(ctx, query, args...)
}

type activeSnapshotCleanupHiddenActiveAfterInactiveConn struct {
	ch.Conn
	mountPath         string
	sid               string
	updatedAt         time.Time
	switchedAt        time.Time
	published         bool
	baseActiveChecked bool
}

func (c *activeSnapshotCleanupHiddenActiveAfterInactiveConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if query != insertInactiveSnapshotMountRowQuery || c.published {
		return c.Conn.Exec(ctx, query, args...)
	}

	if err := c.Conn.Exec(ctx, query, args...); err != nil {
		return err
	}

	if err := c.Conn.Exec(ctx, testInsertMountStmt, c.mountPath, c.switchedAt, c.sid, c.updatedAt); err != nil {
		return err
	}

	if err := c.Conn.Exec(
		ctx,
		testInsertInactiveMountStmt,
		c.mountPath,
		c.switchedAt.Add(time.Millisecond),
		c.sid,
		c.updatedAt,
	); err != nil {
		return err
	}

	c.published = true

	return nil
}

func (c *activeSnapshotCleanupHiddenActiveAfterInactiveConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if query == latestActiveSnapshotSwitchedAtQuery && c.published {
		c.baseActiveChecked = true

		return c.Conn.Query(ctx, "SELECT toNullable(toDateTime64(?, 3))", c.switchedAt)
	}

	return c.Conn.Query(ctx, query, args...)
}

type activeSnapshotCleanupMetadataNoopConn struct {
	ch.Conn
	sawDelete bool
}

func (c *activeSnapshotCleanupMetadataNoopConn) Exec(ctx context.Context, query string, args ...any) error {
	if isActiveSnapshotCleanupMountDeleteQuery(query) {
		c.sawDelete = true

		return errActiveSnapshotCleanupDeleteForbidden
	}

	if isActiveSnapshotCleanupMetadataChangeQuery(query) {
		return nil
	}

	return c.Conn.Exec(ctx, query, args...)
}

func isActiveSnapshotCleanupMetadataChangeQuery(query string) bool {
	return query == insertInactiveSnapshotMountRowQuery ||
		query == repairInactiveSnapshotMountRowQuery ||
		query == rollbackActiveSnapshotMountRowQuery
}

type activeSnapshotCleanupCoarseLatestActiveParamConn struct {
	ch.Conn
	sawCoarseTimeBaseline  bool
	sawMillisecondBaseline bool
}

func (c *activeSnapshotCleanupCoarseLatestActiveParamConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if query != latestActiveSnapshotSwitchedAtQuery {
		return c.Conn.Query(ctx, query, args...)
	}

	switch baseline := args[1].(type) {
	case time.Time:
		c.sawCoarseTimeBaseline = true

		return c.Conn.Query(ctx, "SELECT toNullable(fromUnixTimestamp64Milli(?))", baseline.UTC().UnixMilli())
	case int64:
		c.sawMillisecondBaseline = true
	}

	return c.Conn.Query(ctx, query, args...)
}

type activeSnapshotCleanupCoarseRepairParamConn struct {
	ch.Conn
	noopedInactiveInserts        int
	noopedRepairs                int
	sawMillisecondRepairBaseline bool
}

func (c *activeSnapshotCleanupCoarseRepairParamConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if query == insertInactiveSnapshotMountRowQuery && c.noopedInactiveInserts == 0 {
		c.noopedInactiveInserts++

		return nil
	}

	if query != repairInactiveSnapshotMountRowQuery {
		return c.Conn.Exec(ctx, query, args...)
	}

	switch args[3].(type) {
	case time.Time:
		c.noopedRepairs++

		return nil
	case int64:
		c.sawMillisecondRepairBaseline = true
	}

	return c.Conn.Exec(ctx, query, args...)
}
