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
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	summariseSpoolHistoryCountQuery = "SELECT count() FROM wrstat_basedirs_history " +
		"WHERE mount_path = ? AND gid = ?"
	summariseSpoolLoaderCountActivePrefixAgeAllQuery = "SELECT count() FROM wrstat_active_prefix_filter_ageall " +
		"WHERE active_set_id = ?"
	summariseSpoolLoaderCountActivePrefixSetQuery = "SELECT count() FROM wrstat_active_prefix_rollup_sets " +
		"WHERE active_set_id = ?"
	summariseSpoolExistingMountPath     = "/mnt/existing-spool-publish/"
	summariseSpoolLoaderMountPathColumn = "mount_path"
	summariseSpoolLoaderSchemaMarker    = "test"
	summariseSpoolLoaderUpdatedAtColumn = "updated_at"
)

var errSummariseSpoolLoaderTestSystemPartsTimedOut = errors.New("system.parts timed out")

const (
	summariseSpoolPublishFaultEventActiveSnapshot = "query active_snapshot"
	summariseSpoolPublishFaultEventActiveVirtual  = "drop wrstat_active_virtual_summaries"
	summariseSpoolPublishFaultEventMountsActive   = "query mounts_active"
	summariseSpoolPublishFaultEventPublish        = "publish"
)

type summariseSpoolCountRow struct {
	value       uint64
	stringValue string
	err         error
}

func (r summariseSpoolCountRow) Err() error {
	return r.err
}

func (r summariseSpoolCountRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != 1 {
		return errBootstrapTestUnexpectedScanDestinationN
	}

	switch value := dest[0].(type) {
	case *uint64:
		*value = r.value
	case *string:
		*value = r.stringValue
	default:
		return errBootstrapTestUnexpectedScanDestination
	}

	return nil
}

func (r summariseSpoolCountRow) ScanStruct(any) error {
	return r.err
}

type summariseSpoolSlowBatch struct {
	countingDGUTABatch

	delay time.Duration
}

func (b *summariseSpoolSlowBatch) Send() error {
	time.Sleep(b.delay)

	return b.countingDGUTABatch.Send()
}

type summariseSpoolFreshContextConn struct {
	bootstrapTestConn

	sendDelay time.Duration
	prepares  int
	counts    int
	switches  int
}

func (c *summariseSpoolFreshContextConn) Query(
	ctx context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}, nil
	case mountsActiveRowsQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{
				summariseSpoolLoaderMountPathColumn,
				dgutaWriterTestSnapshotIDColumn,
				summariseSpoolLoaderUpdatedAtColumn,
			},
		}, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *summariseSpoolFreshContextConn) QueryRow(
	ctx context.Context,
	_ string,
	_ ...any,
) driver.Row {
	c.counts++

	if err := ctx.Err(); err != nil {
		return summariseSpoolCountRow{err: err}
	}

	return summariseSpoolCountRow{value: 1}
}

func (c *summariseSpoolFreshContextConn) Exec(ctx context.Context, query string, _ ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if query == switchSnapshotQuery {
		c.switches++
	}

	return nil
}

func (c *summariseSpoolFreshContextConn) PrepareBatch(
	ctx context.Context,
	_ string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.prepares++

	return &summariseSpoolSlowBatch{delay: c.sendDelay}, nil
}

func TestClickHouseSummariseSpoolLoader(t *testing.T) {
	Convey("summarise spool load rejects manifests missing schema2 table manifests", t, func() {
		manifest := &chspool.Manifest{
			Version:    chspool.Version,
			Format:     chspool.Format,
			State:      chspool.Complete,
			MountPath:  testMountPath,
			SnapshotID: "00000000-0000-0000-0000-000000000001",
			UpdatedAt:  time.Date(2026, 6, 8, 8, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			Tables: map[string]chspool.TableManifest{
				chspool.TableFiles:    {Table: chspool.TableFiles},
				chspool.TableDirFacts: {Table: chspool.TableDirFacts},
			},
		}

		err := validateSummariseSpoolLoad(Config{DSN: testNativeDSN, Database: testDatabaseName}, manifest)

		So(errors.Is(err, chspool.ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, chspool.TableDirs)
	})

	Convey("D2.3 summarise spool load rejects manifests missing child full-filter table manifests", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		manifest := writeSummariseSpoolLoaderSchema3Spool(filepath.Join(t.TempDir(), "spool"), time.Date(
			2026,
			6,
			9,
			8,
			0,
			0,
			0,
			time.UTC,
		))
		delete(manifest.Tables, chspool.TableChildFilterAll)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := LoadSummariseSpool(ctx, cfg, filepath.Join(t.TempDir(), "unused"), manifest, nil)

		So(errors.Is(err, chspool.ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, chspool.TableChildFilterAll)

		conn, err := connectForImportFromConfig(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(conn.Close(), ShouldBeNil) })
		So(summariseSpoolLoaderBlockedPublishRows(ctx, conn, manifest), ShouldEqual, uint64(0))
	})

	Convey("D2.4 summarise spool load rejects manifests missing active virtual table manifests", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		conn, err := connectForImportFromConfig(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(conn.Close(), ShouldBeNil) })

		for _, table := range []string{
			chspool.TableActiveVirtualSummaries,
			chspool.TableActiveVirtualFilterAll,
			chspool.TableActiveVirtualChildren,
			chspool.TableActiveVirtualSets,
		} {
			manifest := writeSummariseSpoolLoaderSchema3Spool(filepath.Join(t.TempDir(), "spool"), time.Date(
				2026,
				6,
				9,
				9,
				0,
				0,
				0,
				time.UTC,
			))
			delete(manifest.Tables, table)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := LoadSummariseSpool(ctx, cfg, filepath.Join(t.TempDir(), "unused"), manifest, nil)

			cancel()

			So(errors.Is(err, chspool.ErrManifestMismatch), ShouldBeTrue)
			So(err.Error(), ShouldContainSubstring, table)
			So(summariseSpoolLoaderBlockedPublishRows(context.Background(), conn, manifest), ShouldEqual, uint64(0))
		}
	})

	Convey("summarise spool reload is idempotent and does not duplicate basedirs history", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderTestSpool(spoolDir, updatedAt)

		conn, err := connectForImportFromConfig(cfg)
		So(err, ShouldBeNil)

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(loader.prepareSnapshot(ctx), ShouldBeNil)
		So(loader.loadTables(ctx), ShouldBeNil)
		So(conn.Close(), ShouldBeNil)

		verifyConn := th.openConn(cfg.DSN)

		Reset(func() { So(verifyConn.Close(), ShouldBeNil) })

		sid := manifest.SnapshotID
		So(countRows(ctx, verifyConn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolHistoryCountQuery, testMountPath, uint32(7)), ShouldEqual, 1)

		_, hasActive, err := readActiveSnapshotID(ctx, verifyConn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)

		So(LoadSummariseSpool(ctx, cfg, spoolDir, manifest, nil), ShouldBeNil)
		So(countRows(ctx, verifyConn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolHistoryCountQuery, testMountPath, uint32(7)), ShouldEqual, 1)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, verifyConn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, sid)
	})

	Convey("summarise spool replay loads schema2 rows before active-prefix publish refresh", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema2PublishSpool(spoolDir, updatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(LoadSummariseSpool(ctx, cfg, spoolDir, manifest, nil), ShouldBeNil)

		verifyConn := th.openConn(cfg.DSN)

		Reset(func() { So(verifyConn.Close(), ShouldBeNil) })

		sid := manifest.SnapshotID
		So(countRows(ctx, verifyConn, dgutaWriterTestCountAgeAllQuery, testMountPath, sid), ShouldEqual, 2)
		dirRows := countRows(
			ctx,
			verifyConn,
			"SELECT count() FROM wrstat_dirs WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			testMountPath,
			sid,
		)
		So(dirRows, ShouldEqual, 3)

		activeRows, err := queryMountsActiveRows(ctx, verifyConn)
		So(err, ShouldBeNil)

		activeSetID := fingerprintForMountsActive(activeRows)
		So(activeSetID, ShouldNotBeBlank)
		So(countRows(ctx, verifyConn, summariseSpoolLoaderCountActivePrefixSetQuery, activeSetID), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolLoaderCountActivePrefixAgeAllQuery, activeSetID),
			ShouldBeGreaterThan, uint64(0))
	})

	Convey("summarise spool load publishes active virtual readiness for all active mounts", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		const existingMountPath = "/mnt/d3-existing/"

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{existingMountPath, testMountPath}

		paths := internaltest.NewDirectoryPathCreator()
		existingUpdatedAt := time.Date(2026, 6, 8, 13, 0, 0, 0, time.UTC)
		spoolUpdatedAt := existingUpdatedAt.Add(time.Hour)

		writeD1SingleRecord(cfg, existingMountPath, existingUpdatedAt, paths.ToDirectoryPath(existingMountPath), 7)

		spoolDir := filepath.Join(t.TempDir(), "spool")
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, spoolUpdatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(LoadSummariseSpool(ctx, cfg, spoolDir, manifest, nil), ShouldBeNil)

		verifyConn := th.openConn(cfg.DSN)

		Reset(func() { So(verifyConn.Close(), ShouldBeNil) })

		activeRows, err := queryMountsActiveRows(ctx, verifyConn)
		So(err, ShouldBeNil)
		So(activeRows, ShouldHaveLength, 2)

		postPublishActiveSetID := fingerprintForMountsActive(activeRows)
		spoolOnlyActiveSetID := fingerprintForMountsActive([]mountsActiveRow{{
			mountPath:  testMountPath,
			snapshotID: manifest.SnapshotID,
			updatedAt:  spoolUpdatedAt,
		}})
		So(postPublishActiveSetID, ShouldNotEqual, spoolOnlyActiveSetID)
		So(countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_sets"), postPublishActiveSetID),
			ShouldEqual, uint64(1))
		So(countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_sets"), spoolOnlyActiveSetID),
			ShouldEqual, uint64(0))

		activeSet := readActiveVirtualSetForTest(ctx, verifyConn, postPublishActiveSetID)
		So(activeSet.ready, ShouldEqual, uint8(1))
		So(activeSet.summaryRows,
			ShouldEqual, countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_summaries"),
				postPublishActiveSetID))
		So(activeSet.filterRows,
			ShouldEqual, countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_filter_all"),
				postPublishActiveSetID))
		So(activeSet.childRows,
			ShouldEqual, countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_children"),
				postPublishActiveSetID))
	})

	Convey("summarise spool load validates combined active virtual rows with subsecond mtimes", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		const existingMountPath = "/mnt/d3-subsecond-existing/"

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{existingMountPath, testMountPath}

		paths := internaltest.NewDirectoryPathCreator()
		existingUpdatedAt := time.Date(2026, 6, 8, 13, 0, 0, 0, time.UTC)
		spoolUpdatedAt := time.Date(2026, 6, 8, 14, 0, 0, 237_000_000, time.UTC)

		writeD1SingleRecord(cfg, existingMountPath, existingUpdatedAt, paths.ToDirectoryPath(existingMountPath), 7)

		spoolDir := filepath.Join(t.TempDir(), "spool")
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, spoolUpdatedAt)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(LoadSummariseSpool(ctx, cfg, spoolDir, manifest, nil), ShouldBeNil)

		verifyConn := th.openConn(cfg.DSN)

		Reset(func() { So(verifyConn.Close(), ShouldBeNil) })

		activeRows, err := queryMountsActiveRows(ctx, verifyConn)
		So(err, ShouldBeNil)
		So(activeRows, ShouldHaveLength, 2)

		activeSetID := fingerprintForMountsActive(activeRows)
		activeSet := readActiveVirtualSetForTest(ctx, verifyConn, activeSetID)
		So(activeSet.ready, ShouldEqual, uint8(1))
		So(activeSet.summaryRows,
			ShouldEqual, countRows(ctx, verifyConn, d1CountActiveSetRowsQuery("wrstat_active_virtual_summaries"),
				activeSetID))
	})

	Convey("summarise spool load uses fresh query contexts after a slow table replay", t, func() {
		cfg := Config{QueryTimeout: 25 * time.Millisecond}
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 5, 13, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderFileOnlySpool(spoolDir, updatedAt)
		conn := &summariseSpoolFreshContextConn{sendDelay: 2 * cfg.QueryTimeout}

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.load(context.Background()), ShouldBeNil)
		So(conn.switches, ShouldEqual, 1)
		So(conn.counts, ShouldEqual, 1)
		So(conn.prepares, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history retry cleanup uses the cleanup timeout", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		conn := &summariseSpoolHistoryDeleteDeadlineConn{normalWindow: cfg.QueryTimeout}
		loader := &summariseSpoolLoader{cfg: cfg, conn: conn}
		rows := []chspool.BasedirsHistoryRow{{
			MountPath: testMountPath,
			GID:       7,
			Date:      time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC),
		}}

		ctx, cancel := queryContext(context.Background(), cfg.QueryTimeout)
		defer cancel()

		So(loader.deleteManifestHistoryRows(ctx, rows), ShouldBeNil)
		So(conn.deleteCalls, ShouldEqual, 1)
		So(conn.cleanupDeadlineCalls, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history cleanup propagates real cleanup errors", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		conn := &summariseSpoolHistoryDeleteDeadlineConn{
			normalWindow: cfg.QueryTimeout,
			err:          errForcedFailure,
		}
		loader := &summariseSpoolLoader{cfg: cfg, conn: conn}
		rows := []chspool.BasedirsHistoryRow{{
			MountPath: testMountPath,
			GID:       8,
			Date:      time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC),
		}}

		err := loader.deleteManifestHistoryRows(context.Background(), rows)

		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.deleteCalls, ShouldEqual, 1)
		So(conn.cleanupDeadlineCalls, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history retry cleanup waits for local mutation visibility", t, func() {
		conn := &summariseSpoolHistoryDeleteLocalMutationConn{}
		loader := &summariseSpoolLoader{conn: conn}
		rows := []chspool.BasedirsHistoryRow{{
			MountPath: testMountPath,
			GID:       9,
			Date:      time.Date(2026, 6, 12, 18, 0, 0, 0, time.UTC),
		}}

		So(loader.deleteManifestHistoryRows(context.Background(), rows), ShouldBeNil)
		So(conn.deleteCalls, ShouldEqual, 1)
		So(conn.query, ShouldContainSubstring, "mount_path = ?")
		So(conn.query, ShouldContainSubstring, "(gid, date) IN")
		So(conn.query, ShouldContainSubstring, "mutations_sync = 1")
	})

	Convey("summarise spool basedirs history load batches last-date checks and inserts", t, func() {
		updatedAt := time.Date(2026, 6, 12, 19, 0, 0, 0, time.UTC)
		conn := &summariseSpoolHistoryBatchInsertConn{
			lastDates: map[summariseSpoolHistoryLastDateKeyForTest]time.Time{
				{mountPath: testMountPath, gid: 8}: updatedAt.Add(time.Hour),
			},
		}
		loader := &summariseSpoolLoader{
			conn: conn,
			manifest: &chspool.Manifest{
				MountPath: testMountPath,
			},
		}
		rows := []chspool.BasedirsHistoryRow{
			{
				MountPath:   testMountPath,
				GID:         7,
				Date:        updatedAt,
				UsageSize:   10,
				QuotaSize:   20,
				UsageInodes: 1,
				QuotaInodes: 2,
			},
			{
				MountPath:   testMountPath,
				GID:         7,
				Date:        updatedAt.Add(time.Minute),
				UsageSize:   11,
				QuotaSize:   21,
				UsageInodes: 3,
				QuotaInodes: 4,
			},
			{
				MountPath:   testMountPath,
				GID:         8,
				Date:        updatedAt,
				UsageSize:   12,
				QuotaSize:   22,
				UsageInodes: 5,
				QuotaInodes: 6,
			},
		}

		So(loader.insertEligibleHistoryRows(context.Background(), rows), ShouldBeNil)
		So(conn.lastDateQueries, ShouldEqual, 1)
		So(conn.perRowLastDateQueries, ShouldEqual, 0)
		So(conn.execInserts, ShouldEqual, 0)
		So(conn.insertBatch.appends, ShouldEqual, 2)
		So(conn.insertBatch.sends, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history cleanup and replay keep mount paths exact", t, func() {
		updatedAt := time.Date(2026, 6, 12, 19, 15, 0, 0, time.UTC)
		otherMountPath := "/mnt/other-spool-publish/"
		conn := &summariseSpoolHistoryBatchInsertConn{
			lastDates: map[summariseSpoolHistoryLastDateKeyForTest]time.Time{
				{mountPath: testMountPath, gid: 7}: updatedAt.Add(time.Hour),
			},
		}
		loader := &summariseSpoolLoader{conn: conn}
		rows := []chspool.BasedirsHistoryRow{
			{
				MountPath:   testMountPath,
				GID:         7,
				Date:        updatedAt,
				UsageSize:   10,
				QuotaSize:   20,
				UsageInodes: 1,
				QuotaInodes: 2,
			},
			{
				MountPath:   otherMountPath,
				GID:         7,
				Date:        updatedAt,
				UsageSize:   30,
				QuotaSize:   40,
				UsageInodes: 3,
				QuotaInodes: 4,
			},
		}

		So(loader.deleteManifestHistoryRows(context.Background(), rows), ShouldBeNil)
		So(conn.deleteMountPaths, ShouldResemble, []string{testMountPath, otherMountPath})

		So(loader.insertEligibleHistoryRows(context.Background(), rows), ShouldBeNil)
		So(conn.lastDateMountPaths, ShouldResemble, []string{testMountPath, otherMountPath})
		So(conn.insertBatch.appends, ShouldEqual, 1)
		So(conn.insertBatch.values[0][0], ShouldEqual, otherMountPath)
	})

	Convey("summarise spool publish gives generated active virtual rows a cleanup-class deadline", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 12, 18, 30, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, updatedAt)
		existingUpdatedAt := updatedAt.Add(-time.Hour)
		conn := &summariseSpoolPublishActiveVirtualDeadlineConn{
			normalWindow: cfg.QueryTimeout,
			sourceDelay:  2 * cfg.QueryTimeout,
			existingMount: activeMount{
				mountPath:  summariseSpoolExistingMountPath,
				snapshotID: SnapshotID(summariseSpoolExistingMountPath, existingUpdatedAt),
				updatedAt:  existingUpdatedAt,
			},
		}

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.publish(context.Background()), ShouldBeNil)
		So(conn.activeVirtualSourceQueries, ShouldEqual, 3)
		So(conn.longDeadlineSourceQueries, ShouldEqual, 3)
		So(conn.switches, ShouldEqual, 1)
		So(conn.batchStats(insertActiveVirtualSetQuery).appends, ShouldEqual, 1)
	})

	Convey("summarise spool publish composes zero-row active virtual overlays from the previous ready set", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 12, 19, 30, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderZeroContributionActiveVirtualSpool(spoolDir, updatedAt)
		previousUpdatedAt := updatedAt.Add(-time.Hour)
		previousRows := []mountsActiveRow{{
			mountPath:  summariseSpoolExistingMountPath,
			snapshotID: SnapshotID(summariseSpoolExistingMountPath, previousUpdatedAt),
			updatedAt:  previousUpdatedAt,
		}}
		previousActiveSetID := fingerprintForMountsActive(previousRows)
		conn := &summariseSpoolZeroActiveVirtualComposeConn{
			activeRows:          previousRows,
			previousActiveSetID: previousActiveSetID,
			previousSummaryRows: []activeVirtualSummaryRow{
				summariseSpoolPreviousActiveVirtualSummary("/", previousActiveSetID, previousUpdatedAt, 1),
				summariseSpoolPreviousActiveVirtualSummary("/mnt/", previousActiveSetID, previousUpdatedAt, 1),
			},
			previousFilterRows: []activeVirtualFilterAllRow{
				summariseSpoolPreviousActiveVirtualFilter("/", previousActiveSetID, previousUpdatedAt),
				summariseSpoolPreviousActiveVirtualFilter("/mnt/", previousActiveSetID, previousUpdatedAt),
			},
			previousSet: activeVirtualSetRow{
				ActiveSetID:      previousActiveSetID,
				Schema3Version:   currentSchemaVersion,
				MountsSHA256:     previousActiveSetID,
				ActiveMountCount: 1,
				SummaryRows:      3,
				FilterRows:       2,
				ChildRows:        2,
				ManifestSHA256:   activeVirtualManifestSHA256(previousActiveSetID, 3, 2, 2),
				Ready:            1,
				RefreshedAt:      previousUpdatedAt,
			},
		}

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.publish(context.Background()), ShouldBeNil)
		So(conn.activeVirtualSourceQueries, ShouldEqual, 0)
		So(conn.copyQueries, ShouldEqual, 3)
		So(conn.switches, ShouldEqual, 1)
		So(conn.batchStats(insertActiveVirtualSummaryQuery).appends, ShouldEqual, 3)
		So(conn.batchStats(insertActiveVirtualFilterAllQuery).appends, ShouldEqual, 2)
		So(conn.batchStats(insertActiveVirtualChildQuery).appends, ShouldEqual, 3)
		So(conn.batchStats(insertActiveVirtualSetQuery).appends, ShouldEqual, 1)
	})

	Convey("summarise spool publish preserves sibling mount-root child counts in zero-row overlays", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 12, 19, 45, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderZeroContributionActiveVirtualSpool(spoolDir, updatedAt)
		previousUpdatedAt := updatedAt.Add(-time.Hour)
		previousMountPath := summariseSpoolExistingMountPath
		previousRows := []mountsActiveRow{{
			mountPath:  previousMountPath,
			snapshotID: SnapshotID(previousMountPath, previousUpdatedAt),
			updatedAt:  previousUpdatedAt,
		}}
		previousActiveSetID := fingerprintForMountsActive(previousRows)
		conn := &summariseSpoolZeroActiveVirtualComposeConn{
			activeRows:          previousRows,
			previousActiveSetID: previousActiveSetID,
			previousSummaryRows: []activeVirtualSummaryRow{
				summariseSpoolPreviousActiveVirtualSummary("/", previousActiveSetID, previousUpdatedAt, 1),
				summariseSpoolPreviousActiveVirtualSummary("/mnt/", previousActiveSetID, previousUpdatedAt, 1),
				summariseSpoolPreviousActiveVirtualMountRootSummary(
					previousMountPath,
					previousActiveSetID,
					previousUpdatedAt,
					6,
				),
			},
			previousFilterRows: []activeVirtualFilterAllRow{
				summariseSpoolPreviousActiveVirtualFilter("/", previousActiveSetID, previousUpdatedAt),
				summariseSpoolPreviousActiveVirtualFilter("/mnt/", previousActiveSetID, previousUpdatedAt),
			},
			previousSet: activeVirtualSetRow{
				ActiveSetID:      previousActiveSetID,
				Schema3Version:   currentSchemaVersion,
				MountsSHA256:     previousActiveSetID,
				ActiveMountCount: 1,
				SummaryRows:      3,
				FilterRows:       2,
				ChildRows:        2,
				ManifestSHA256:   activeVirtualManifestSHA256(previousActiveSetID, 3, 2, 2),
				Ready:            1,
				RefreshedAt:      previousUpdatedAt,
			},
		}

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.publish(context.Background()), ShouldBeNil)

		childCount, ok := summariseSpoolActiveVirtualChildCountForTest(
			conn.batchStats(insertActiveVirtualChildQuery).values,
			"/mnt/",
			strings.TrimSuffix(previousMountPath, "/"),
		)
		So(ok, ShouldBeTrue)
		So(childCount, ShouldEqual, uint64(6))
	})

	Convey("summarise spool replay caps schema2 catalog and fact batches", t, func() {
		const (
			dirsRows   = defaultProjectionBatchSize + 80
			factRows   = defaultProjectionBatchSize + 100
			ageAllRows = defaultProjectionBatchSize + 90
		)

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 8, 11, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema2BatchSpool(
			spoolDir,
			updatedAt,
			dirsRows,
			factRows,
			ageAllRows,
		)
		conn := &summariseSpoolLoaderLazyImportConn{}

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.loadTables(context.Background()), ShouldBeNil)
		So(conn.totalRowsFor(insertDirsQuery), ShouldEqual, dirsRows)
		So(conn.maxRowsFor(insertDirsQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, factRows)
		So(conn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertDirFilterAgeAllQuery), ShouldEqual, ageAllRows)
		So(conn.maxRowsFor(insertDirFilterAgeAllQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
	})

	Convey("D3.1 summarise spool loader publishes only after schema3 and active virtual readiness", t, func() {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, updatedAt)
		conn := newSummariseSpoolLoaderSpyConn(manifest)

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.load(context.Background()), ShouldBeNil)
		So(conn.insertedRows(chspool.TableChildFilterAll), ShouldEqual, manifest.Tables[chspool.TableChildFilterAll].Rows)
		So(conn.insertedRows(chspool.TableDirFilterAll), ShouldEqual, manifest.Tables[chspool.TableDirFilterAll].Rows)
		So(conn.insertedRows(chspool.TableActiveVirtualSummaries),
			ShouldEqual, manifest.Tables[chspool.TableActiveVirtualSummaries].Rows)
		So(conn.insertedRows(chspool.TableActiveVirtualFilterAll),
			ShouldEqual, manifest.Tables[chspool.TableActiveVirtualFilterAll].Rows)
		So(conn.insertedRows(chspool.TableActiveVirtualChildren),
			ShouldEqual, manifest.Tables[chspool.TableActiveVirtualChildren].Rows)
		So(conn.insertedRows(chspool.TableActiveVirtualSets),
			ShouldEqual, manifest.Tables[chspool.TableActiveVirtualSets].Rows)
		So(conn.eventIndex("count "+chspool.TableDirFilterAll),
			ShouldBeLessThan, conn.eventIndex("send "+chspool.TableSchema3SnapshotSets))
		So(conn.eventIndex("send "+chspool.TableSchema3SnapshotSets),
			ShouldBeLessThan, conn.eventIndex("send "+chspool.TableActiveVirtualSets))
		So(conn.eventIndex("send "+chspool.TableActiveVirtualSets), ShouldBeLessThan, conn.eventIndex("publish"))
		So(conn.publishedSID, ShouldEqual, manifest.SnapshotID)
	})

	Convey("E1.5 summarise spool load reports measured rows, table stats, resources, and publish evidence",
		t,
		func() {
			spoolDir := filepath.Join(t.TempDir(), "spool")
			updatedAt := time.Date(2026, 6, 9, 10, 30, 0, 0, time.UTC)
			manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, updatedAt)
			conn := newSummariseSpoolLoaderSpyConn(manifest)

			loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			report, err := loader.loadReport(context.Background())

			So(err, ShouldBeNil)

			reportTable := chspool.TableDirFacts
			total := summariseSpoolReportOperation(report, "spool_load_total")
			So(total, ShouldNotBeNil)
			So(total.DurationsMS, ShouldHaveLength, 1)
			So(summariseSpoolReportUint64MapInput(total.Inputs, "loaded_table_rows")[reportTable],
				ShouldEqual, manifest.Tables[reportTable].Rows)
			So(report.TableStats[reportTable].Rows, ShouldEqual, manifest.Tables[reportTable].Rows)
			So(report.TableStats[reportTable].ActiveParts, ShouldEqual, uint64(1))
			So(report.TableStats[reportTable].CompressedBytes, ShouldBeGreaterThan, uint64(0))
			So(report.TableStats[reportTable].UncompressedBytes, ShouldBeGreaterThan, uint64(0))

			for _, key := range []string{
				"user_cpu_ms",
				"system_cpu_ms",
				"total_cpu_ms",
				"peak_rss_bytes",
				"spool_bytes",
				"publish_latency_ms",
			} {
				_, ok := total.Inputs[key]
				So(ok, ShouldBeTrue)
			}

			So(summariseSpoolReportUint64Input(total.Inputs, "user_cpu_ms"), ShouldBeGreaterThanOrEqualTo, uint64(0))
			So(summariseSpoolReportUint64Input(total.Inputs, "system_cpu_ms"), ShouldBeGreaterThanOrEqualTo, uint64(0))
			So(summariseSpoolReportUint64Input(total.Inputs, "total_cpu_ms"), ShouldBeGreaterThanOrEqualTo, uint64(0))
			So(summariseSpoolReportUint64Input(total.Inputs, "peak_rss_bytes"), ShouldBeGreaterThan, uint64(0))
			So(summariseSpoolReportUint64Input(total.Inputs, "spool_bytes"), ShouldBeGreaterThan, uint64(0))
			partCounts := summariseSpoolReportUint64MapInput(total.Inputs, "part_counts")

			var totalParts uint64
			for _, count := range partCounts {
				totalParts += count
			}

			So(partCounts[reportTable], ShouldEqual, uint64(1))
			So(total.Inputs["budget_source"], ShouldEqual, "computed_from_measurements")
			So(summariseSpoolReportUint64Input(total.Inputs, "budget_measurement_count"),
				ShouldEqual, uint64(len(total.DurationsMS)))
			So(summariseSpoolReportUint64Input(total.Inputs, "wall_time_budget_ms"),
				ShouldEqual, uint64(math.Ceil(total.P95MS)))
			So(summariseSpoolReportUint64Input(total.Inputs, "total_cpu_budget_ms"),
				ShouldEqual, summariseSpoolReportUint64Input(total.Inputs, "total_cpu_ms"))
			So(summariseSpoolReportUint64Input(total.Inputs, "peak_rss_budget_bytes"),
				ShouldEqual, summariseSpoolReportUint64Input(total.Inputs, "peak_rss_bytes"))
			So(summariseSpoolReportUint64Input(total.Inputs, "spool_byte_budget"),
				ShouldEqual, summariseSpoolReportUint64Input(total.Inputs, "spool_bytes"))
			So(summariseSpoolReportUint64Input(total.Inputs, "part_count_budget"), ShouldEqual, totalParts)
			So(total.Inputs["retry_cleanup_result"], ShouldEqual, "success")
			So(summariseSpoolReportUint64Input(total.Inputs, "publish_latency_ms"),
				ShouldBeGreaterThanOrEqualTo, uint64(0))
		})

	Convey("E1.5 summarise spool load reports succeed when optional table stats need extra grants",
		t,
		func() {
			spoolDir := filepath.Join(t.TempDir(), "spool")
			updatedAt := time.Date(2026, 6, 12, 12, 45, 0, 0, time.UTC)
			manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, updatedAt)
			conn := newSummariseSpoolLoaderSpyConn(manifest)
			conn.tableStatsErr = &proto.Exception{
				Code: 497,
				Message: "wrstat: Not enough privileges. To execute this query, " +
					"it's necessary to have the grant SELECT(`table`, rows, " +
					"data_compressed_bytes, data_uncompressed_bytes, database, active) ON system.parts",
			}

			loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			report, err := loader.loadReport(context.Background())

			So(err, ShouldBeNil)
			So(conn.publishedSID, ShouldEqual, manifest.SnapshotID)
			So(report.TableStats, ShouldHaveLength, 0)
			So(report.SelectedTables, ShouldHaveLength, 0)

			total := summariseSpoolReportOperation(report, "spool_load_total")
			So(total, ShouldNotBeNil)

			reportTable := chspool.TableDirFacts
			So(summariseSpoolReportUint64MapInput(total.Inputs, "loaded_table_rows")[reportTable],
				ShouldEqual, manifest.Tables[reportTable].Rows)
			So(summariseSpoolReportUint64MapInput(total.Inputs, "part_counts"), ShouldHaveLength, 0)
			So(total.Inputs["table_stats_status"], ShouldEqual, "unavailable")
			So(total.Inputs["table_stats_error"], ShouldContainSubstring, "insufficient_privileges")
			So(total.Inputs["retry_cleanup_result"], ShouldEqual, "success")
			So(summariseSpoolReportUint64Input(total.Inputs, "publish_latency_ms"),
				ShouldBeGreaterThanOrEqualTo, uint64(0))
		})

	Convey("E1.5 summarise spool load reports still fail on non-optional table stats errors",
		t,
		func() {
			spoolDir := filepath.Join(t.TempDir(), "spool")
			manifest := writeSummariseSpoolLoaderSchema3Spool(
				spoolDir,
				time.Date(2026, 6, 12, 13, 0, 0, 0, time.UTC),
			)
			conn := newSummariseSpoolLoaderSpyConn(manifest)
			conn.tableStatsErr = errSummariseSpoolLoaderTestSystemPartsTimedOut

			loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			_, err = loader.loadReport(context.Background())

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "system.parts timed out")
		})

	Convey("summarise spool report retry does not rewrite completed publish state before table evidence",
		t,
		func() {
			spoolDir := filepath.Join(t.TempDir(), "spool")
			manifest := writeSummariseSpoolLoaderSchema3Spool(
				spoolDir,
				time.Date(2026, 6, 12, 14, 0, 0, 0, time.UTC),
			)
			conn := newSummariseSpoolLoaderSpyConn(manifest)
			conn.tableStatsErr = errSummariseSpoolLoaderTestSystemPartsTimedOut

			loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			_, err = loader.loadReport(context.Background())

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "system.parts timed out")

			conn.tableStatsErr = nil

			So(os.Chmod(spoolDir, 0o500), ShouldBeNil)
			Reset(func() { So(os.Chmod(spoolDir, 0o700), ShouldBeNil) })

			loader, err = newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			report, err := loader.loadReport(context.Background())

			So(err, ShouldBeNil)
			So(summariseSpoolReportOperation(report, "spool_load_total"), ShouldNotBeNil)
		})

	Convey("summarise spool publish resumes from injected failures at every durable phase boundary",
		t,
		func() {
			cases := []summariseSpoolPublishFaultCase{
				{
					name:             "tables loaded",
					failEvent:        summariseSpoolPublishFaultEventMountsActive,
					completedPhase:   summariseSpoolPublishPhaseTablesLoaded,
					wantRetryPublish: true,
					wantRetryPhases: []string{
						importPhaseMountSwitch,
						importPhaseOldSnapshotDrop,
						importPhaseTreeSummaryRefresh,
						importPhaseActivePrefixRefresh,
					},
				},
				{
					name:             "active virtual ready",
					failEvent:        summariseSpoolPublishFaultEventActiveSnapshot,
					failSkip:         1,
					completedPhase:   summariseSpoolPublishPhaseActiveVirtualReady,
					wantRetryPublish: true,
					wantRetryPhases: []string{
						importPhaseMountSwitch,
						importPhaseOldSnapshotDrop,
						importPhaseTreeSummaryRefresh,
						importPhaseActivePrefixRefresh,
					},
				},
				{
					name:             "switch planned",
					failEvent:        summariseSpoolPublishFaultEventPublish,
					completedPhase:   summariseSpoolPublishPhaseSwitchPlanned,
					wantRetryPublish: true,
					wantRetryPhases: []string{
						importPhaseMountSwitch,
						importPhaseOldSnapshotDrop,
						importPhaseTreeSummaryRefresh,
						importPhaseActivePrefixRefresh,
					},
				},
				{
					name:                   "mount switched",
					failEvent:              "drop wrstat_dirs",
					failAfterMountSwitched: true,
					completedPhase:         summariseSpoolPublishPhaseMountSwitched,
					wantRetryEvents: []string{
						"drop wrstat_dirs",
						summariseSpoolPublishFaultEventActiveVirtual,
					},
					wantRetryPhases: []string{
						importPhaseOldSnapshotDrop,
						importPhaseTreeSummaryRefresh,
						importPhaseActivePrefixRefresh,
					},
				},
				{
					name:                   "old snapshot dropped",
					failEvent:              summariseSpoolPublishFaultEventActiveVirtual,
					failAfterMountSwitched: true,
					completedPhase:         summariseSpoolPublishPhaseOldSnapshotDropped,
					wantRetryEvents:        []string{summariseSpoolPublishFaultEventActiveVirtual},
					wantRetryPhases: []string{
						importPhaseOldSnapshotDrop,
						importPhaseTreeSummaryRefresh,
						importPhaseActivePrefixRefresh,
					},
				},
				{
					name:              "old active virtual dropped",
					chmodOnPhase:      importPhaseTreeSummaryRefresh,
					completedPhase:    summariseSpoolPublishPhaseOldActiveVirtualDropped,
					wantRetryPhases:   []string{importPhaseTreeSummaryRefresh, importPhaseActivePrefixRefresh},
					forbidRetryEvents: []string{summariseSpoolPublishFaultEventActiveVirtual},
				},
				{
					name:              "tree summary refreshed",
					chmodOnPhase:      importPhaseActivePrefixRefresh,
					completedPhase:    summariseSpoolPublishPhaseTreeSummaryRefreshed,
					wantRetryPhases:   []string{importPhaseActivePrefixRefresh},
					forbidRetryPhases: []string{importPhaseTreeSummaryRefresh},
				},
			}

			for _, tc := range cases {
				Convey(tc.name, func() {
					spoolDir := filepath.Join(t.TempDir(), "spool")
					manifest := writeSummariseSpoolLoaderSchema3Spool(
						spoolDir,
						time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
					)
					conn := newSummariseSpoolPublishFaultConn(manifest)
					conn.failEvent = tc.failEvent
					conn.failAfterMountSwitched = tc.failAfterMountSwitched
					conn.failSkip = tc.failSkip

					firstRecorder := summariseSpoolPublishChmodRecorder(t, spoolDir, tc.chmodOnPhase)
					loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest,
						firstRecorder)
					So(err, ShouldBeNil)

					err = loader.load(context.Background())

					So(err, ShouldNotBeNil)

					if tc.chmodOnPhase == "" {
						So(errors.Is(err, errForcedFailure), ShouldBeTrue)
					} else {
						So(err.Error(), ShouldContainSubstring, "summarise spool publish state")
						So(os.Chmod(spoolDir, 0o700), ShouldBeNil)
					}

					phases := summariseSpoolPublishStatePhasesForTest(t, spoolDir, manifest)
					So(phases[tc.completedPhase], ShouldNotBeBlank)
					So(phases[summariseSpoolPublishPhasePostSpoolPublishComplete], ShouldBeBlank)

					conn.disableFault()
					conn.resetEvents()

					var retryPhases []string

					loader, err = newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest,
						func(phase string, _ time.Duration) {
							retryPhases = append(retryPhases, phase)
						})
					So(err, ShouldBeNil)

					So(loader.load(context.Background()), ShouldBeNil)
					summariseSpoolPublishAssertAllPhasesComplete(t, spoolDir, manifest)
					summariseSpoolPublishAssertRetry(t, conn, retryPhases, tc)
				})
			}
		})

	Convey("summarise spool publish resumes after active-prefix refresh before final publish marker",
		t,
		func() {
			spoolDir := filepath.Join(t.TempDir(), "spool")
			manifest := writeSummariseSpoolLoaderSchema3Spool(
				spoolDir,
				time.Date(2026, 6, 12, 16, 0, 0, 0, time.UTC),
			)
			conn := newSummariseSpoolPublishFaultConn(manifest)
			conn.publishCurrentManifest()
			summariseSpoolPublishSeedStateThrough(t, spoolDir, manifest,
				summariseSpoolPublishPhaseActivePrefixRefreshed)

			So(os.Chmod(spoolDir, 0o500), ShouldBeNil)

			loader, err := newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest, nil)
			So(err, ShouldBeNil)

			err = loader.load(context.Background())

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "summarise spool publish state")
			So(os.Chmod(spoolDir, 0o700), ShouldBeNil)

			conn.resetEvents()

			var retryPhases []string

			loader, err = newSummariseSpoolLoader(Config{Database: testDatabaseName}, conn, spoolDir, manifest,
				func(phase string, _ time.Duration) {
					retryPhases = append(retryPhases, phase)
				})
			So(err, ShouldBeNil)

			So(loader.load(context.Background()), ShouldBeNil)
			summariseSpoolPublishAssertAllPhasesComplete(t, spoolDir, manifest)
			So(conn.eventIndex(summariseSpoolPublishFaultEventPublish), ShouldEqual, -1)
			So(summariseSpoolPublishPhaseCount(retryPhases, importPhaseTreeSummaryRefresh), ShouldEqual, 0)
			So(summariseSpoolPublishPhaseCount(retryPhases, importPhaseActivePrefixRefresh), ShouldEqual, 0)
		})

	Convey("D3.2 summarise spool loader blocks readiness when dir-filter rows mismatch", t, func() {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, time.Date(2026, 6, 9, 11, 0, 0, 0, time.UTC))
		conn := newSummariseSpoolLoaderSpyConn(manifest)
		conn.countOverrides[chspool.TableDirFilterAll] = manifest.Tables[chspool.TableDirFilterAll].Rows - 1

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		err = loader.load(context.Background())

		So(errors.Is(err, errSpoolLoadedRowsMismatch), ShouldBeTrue)
		So(conn.insertedRows(chspool.TableSchema3SnapshotSets), ShouldEqual, 0)
		So(conn.activePublishes(), ShouldEqual, 0)
	})

	Convey("D3.3 summarise spool loader drops stale partitions before deterministic retry publish", t, func() {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, updatedAt)
		conn := newSummariseSpoolLoaderSpyConn(manifest)
		conn.countOverrides[chspool.TableDirFilterAll] = manifest.Tables[chspool.TableDirFilterAll].Rows - 1

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)
		So(errors.Is(loader.load(context.Background()), errSpoolLoadedRowsMismatch), ShouldBeTrue)

		delete(conn.countOverrides, chspool.TableDirFilterAll)
		conn.resetEvents()

		loader, err = newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)
		So(loader.load(context.Background()), ShouldBeNil)

		So(conn.eventIndex("drop wrstat_dir_filter_all"),
			ShouldBeLessThan, conn.eventIndex("send "+chspool.TableDirFilterAll))
		So(conn.publishedSID, ShouldEqual, SnapshotID(testMountPath, updatedAt))
	})

	Convey("D3.4 summarise spool loader blocks active readiness when active virtual filters mismatch", t, func() {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, time.Date(2026, 6, 9, 13, 0, 0, 0, time.UTC))
		conn := newSummariseSpoolLoaderSpyConn(manifest)
		conn.countOverrides[chspool.TableActiveVirtualFilterAll] =
			manifest.Tables[chspool.TableActiveVirtualFilterAll].Rows - 1

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		err = loader.load(context.Background())

		So(errors.Is(err, errSpoolLoadedRowsMismatch), ShouldBeTrue)
		So(conn.insertedRows(chspool.TableSchema3SnapshotSets),
			ShouldEqual, manifest.Tables[chspool.TableSchema3SnapshotSets].Rows)
		So(conn.insertedRows(chspool.TableActiveVirtualSets), ShouldEqual, 0)
		So(conn.activePublishes(), ShouldEqual, 0)
	})

	Convey("D3.5 summarise spool loader drops partial active virtual child partitions before retry", t, func() {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		manifest := writeSummariseSpoolLoaderSchema3Spool(spoolDir, time.Date(2026, 6, 9, 14, 0, 0, 0, time.UTC))
		conn := newSummariseSpoolLoaderSpyConn(manifest)
		conn.countOverrides[chspool.TableActiveVirtualChildren] =
			manifest.Tables[chspool.TableActiveVirtualChildren].Rows - 1

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)
		So(errors.Is(loader.load(context.Background()), errSpoolLoadedRowsMismatch), ShouldBeTrue)
		So(conn.insertedRows(chspool.TableActiveVirtualSets), ShouldEqual, 0)
		So(conn.activePublishes(), ShouldEqual, 0)

		delete(conn.countOverrides, chspool.TableActiveVirtualChildren)
		conn.resetEvents()

		loader, err = newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)
		So(loader.load(context.Background()), ShouldBeNil)
		So(conn.eventIndex("drop wrstat_active_virtual_children"),
			ShouldBeLessThan, conn.eventIndex("send "+chspool.TableActiveVirtualChildren))
	})
}

func writeSummariseSpoolLoaderSchema3Spool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	activeSetID := fingerprintForMountsActive([]mountsActiveRow{{
		mountPath:  testMountPath,
		snapshotID: sid,
		updatedAt:  updatedAt,
	}})
	rootFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, "/", 1)
	namespaceFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, "/mnt/", 1)
	mountFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, testMountPath, 1)
	childFilter := summariseSpoolLoaderChildFilterAllRow(sid, updatedAt)
	dirFilter := summariseSpoolLoaderDirFilterAllRow(childFilter)
	writeErr := errors.Join(
		set.WriteDir(summariseSpoolLoaderDirRow(sid, "/", 1, 0)),
		set.WriteDir(summariseSpoolLoaderDirRow(sid, "/mnt/", 1, 1)),
		set.WriteDir(summariseSpoolLoaderDirRow(sid, testMountPath, 1, 1)),
		set.WriteDir(summariseSpoolLoaderDirRow(sid, testMountPath+"project/", 0, 0)),
		set.WriteDirFact(rootFact),
		set.WriteDirFact(namespaceFact),
		set.WriteDirFact(mountFact),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(rootFact)),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(namespaceFact)),
		set.WriteChildFilterAll(childFilter),
		set.WriteDirFilterAll(dirFilter),
		set.WriteDirProjectionSet(chspool.DirProjectionSetRow{
			MountPath:   testMountPath,
			SnapshotID:  sid,
			UpdatedAt:   updatedAt,
			RefreshedAt: updatedAt,
		}),
		set.WriteSchema3SnapshotSet(chspool.Schema3SnapshotSetRow{
			MountPath:          testMountPath,
			SnapshotID:         sid,
			Schema3Version:     currentSchemaVersion,
			DirsRows:           4,
			DirFactsRows:       3,
			ChildFilterAllRows: 1,
			DirFilterAllRows:   1,
			ManifestSHA256:     "schema3-snapshot-test",
			RefreshedAt:        updatedAt,
		}),
		set.WriteActiveVirtualSummary(summariseSpoolLoaderActiveVirtualSummaryRow(activeSetID, updatedAt)),
		set.WriteActiveVirtualFilterAll(summariseSpoolLoaderActiveVirtualFilterRow(activeSetID, updatedAt)),
		set.WriteActiveVirtualChild(chspool.ActiveVirtualChildRow{
			ActiveSetID:     activeSetID,
			ParentVirtualID: summariseSpoolLoaderVirtualIDForDir(testRootMountPath),
			ChildVirtualID:  summariseSpoolLoaderVirtualIDForDir(testMountPath),
			MountPath:       testMountPath,
			SnapshotID:      sid,
			MountRootDirID:  summariseSpoolLoaderDirID(testMountPath),
			IsMountRootBox:  1,
			ChildCount:      1,
			RefreshedAt:     updatedAt,
		}),
		set.WriteActiveVirtualSet(chspool.ActiveVirtualSetRow{
			ActiveSetID:      activeSetID,
			Schema3Version:   currentSchemaVersion,
			MountsSHA256:     activeSetID,
			ActiveMountCount: 1,
			SummaryRows:      1,
			FilterRows:       1,
			ChildRows:        1,
			ManifestSHA256:   "active-virtual-test",
			Ready:            1,
			RefreshedAt:      updatedAt,
		}),
	)

	So(writeErr, ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func summariseSpoolLoaderDirID(dir string) uint32 {
	switch dir {
	case "/":
		return 0
	case "/mnt/":
		return 1
	case testMountPath:
		return 2
	default:
		return 3
	}
}

func summariseSpoolLoaderParentID(dir string) uint32 {
	switch dir {
	case "/":
		return summary.ParentSentinel
	case "/mnt/":
		return 0
	case testMountPath:
		return 1
	default:
		return 2
	}
}

func summariseSpoolLoaderChildFilterAllRow(sid string, updatedAt time.Time) chspool.ChildFilterAllRow {
	return chspool.ChildFilterAllRow{
		MountPath:         testMountPath,
		SnapshotID:        sid,
		ParentID:          summariseSpoolLoaderDirID(testMountPath),
		Age:               uint8(db.DGUTAgeAll),
		GID:               7,
		UID:               17,
		FT:                uint16(db.DGUTAFileTypeBam),
		DirID:             summariseSpoolLoaderDirID(testMountPath + "project/"),
		Count:             3,
		Size:              30,
		AtimeMin:          100,
		MtimeMax:          200,
		AtimeBuckets:      summariseSpoolLoaderSchema2Buckets(3),
		MtimeBuckets:      summariseSpoolLoaderSchema2Buckets(3),
		FilterChildCount:  1,
		ChildCount:        1,
		HasFilterChildren: 1,
		HasChildren:       1,
		RefreshedAt:       updatedAt,
	}
}

func summariseSpoolLoaderDirFilterAllRow(row chspool.ChildFilterAllRow) chspool.DirFilterAllRow {
	return chspool.DirFilterAllRow{
		MountPath:         row.MountPath,
		SnapshotID:        row.SnapshotID,
		Age:               row.Age,
		GID:               row.GID,
		UID:               row.UID,
		FT:                row.FT,
		DirID:             row.DirID,
		SubtreeEnd:        row.DirID + 1,
		Count:             row.Count,
		Size:              row.Size,
		AtimeMin:          row.AtimeMin,
		MtimeMax:          row.MtimeMax,
		AtimeBuckets:      row.AtimeBuckets,
		MtimeBuckets:      row.MtimeBuckets,
		FilterChildCount:  row.FilterChildCount,
		ChildCount:        row.ChildCount,
		HasFilterChildren: row.HasFilterChildren,
		HasChildren:       row.HasChildren,
		RefreshedAt:       row.RefreshedAt,
	}
}

func summariseSpoolLoaderDirRow(sid string, dir string, childDirs, childFiles uint32) chspool.DirRow {
	return chspool.DirRow{
		MountPath:      testMountPath,
		SnapshotID:     sid,
		DirID:          summariseSpoolLoaderDirID(dir),
		ParentID:       summariseSpoolLoaderParentID(dir),
		SubtreeEnd:     summariseSpoolLoaderDirID(dir) + 1,
		Depth:          summariseSpoolLoaderDepth(dir),
		Name:           catalogNameForFullPath(dir),
		FullPath:       ensureTrailingSlash(dir),
		ChildDirCount:  childDirs,
		ChildFileCount: childFiles,
		PathHash:       catalogPathHash(dir),
	}
}

func summariseSpoolLoaderDepth(dir string) uint16 {
	return uint16(strings.Count(strings.Trim(dir, "/"), "/")) //nolint:gosec // Test fixture paths have bounded depth.
}

func summariseSpoolLoaderActiveVirtualSummaryRow(
	activeSetID string,
	updatedAt time.Time,
) chspool.ActiveVirtualSummaryRow {
	return chspool.ActiveVirtualSummaryRow{
		ActiveSetID:     activeSetID,
		VirtualID:       summariseSpoolLoaderVirtualIDForDir(testRootMountPath),
		MountPath:       testMountPath,
		SnapshotID:      SnapshotID(testMountPath, updatedAt),
		MountRootDirID:  summariseSpoolLoaderDirID(testMountPath),
		UpdatedAt:       updatedAt,
		AllAtimeBuckets: summariseSpoolLoaderSchema2Buckets(0),
		AllMtimeBuckets: summariseSpoolLoaderSchema2Buckets(0),
		ChildCount:      1,
		RefreshedAt:     updatedAt,
	}
}

func summariseSpoolLoaderVirtualIDForDir(dir string) uint32 {
	return uint32(catalogPathHash(ensureTrailingSlash(dir))) //nolint:gosec // Fixture IDs intentionally use low hash bits.
}

func summariseSpoolLoaderActiveVirtualFilterRow(
	activeSetID string,
	updatedAt time.Time,
) chspool.ActiveVirtualFilterAllRow {
	return chspool.ActiveVirtualFilterAllRow{
		ActiveSetID:      activeSetID,
		VirtualID:        summariseSpoolLoaderVirtualIDForDir(testRootMountPath),
		Age:              uint8(db.DGUTAgeAll),
		AtimeBuckets:     summariseSpoolLoaderSchema2Buckets(0),
		MtimeBuckets:     summariseSpoolLoaderSchema2Buckets(0),
		FilterChildCount: 1,
		ChildCount:       1,
		RefreshedAt:      updatedAt,
	}
}

func summariseSpoolLoaderBlockedPublishRows(
	ctx context.Context,
	conn driver.Conn,
	manifest *chspool.Manifest,
) uint64 {
	return countRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_schema3_snapshot_sets WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	) + countRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_active_virtual_sets",
	) + countRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	)
}

func writeSummariseSpoolLoaderTestSpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	So(set.WriteBasedirsHistory(chspool.BasedirsHistoryRow{
		MountPath:   testMountPath,
		GID:         7,
		Date:        updatedAt,
		UsageSize:   50,
		QuotaSize:   100,
		UsageInodes: 5,
		QuotaInodes: 10,
	}), ShouldBeNil)
	So(set.WriteBasedirsGroupUsage(chspool.BasedirsGroupUsageRow{
		MountPath:       testMountPath,
		SnapshotID:      sid,
		GID:             7,
		BaseDirID:       1,
		BaseDirExternal: basedirsStoreTestBaseDir,
		Age:             uint8(db.DGUTAgeAll),
		UIDs:            []uint32{17},
		UsageSize:       50,
		QuotaSize:       100,
		UsageInodes:     5,
		QuotaInodes:     10,
		Mtime:           updatedAt,
		DateNoSpace:     time.Unix(0, 0).UTC(),
		DateNoFiles:     time.Unix(0, 0).UTC(),
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func writeSummariseSpoolLoaderSchema2PublishSpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)

	rootFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, "/", 0)
	namespaceFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, "/mnt/", 0)
	mountFact := summariseSpoolLoaderSchema2FactRow(sid, updatedAt, testMountPath, 1)
	writeErr := errors.Join(
		set.WriteDir(summariseSpoolLoaderDirRow(sid, "/", 1, 0)),
		set.WriteDir(summariseSpoolLoaderDirRow(sid, "/mnt/", 1, 1)),
		set.WriteDir(summariseSpoolLoaderDirRow(sid, testMountPath, 0, 0)),
		set.WriteDirFact(rootFact),
		set.WriteDirFact(namespaceFact),
		set.WriteDirFact(mountFact),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(rootFact)),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(namespaceFact)),
		set.WriteDirProjectionSet(chspool.DirProjectionSetRow{
			MountPath:   testMountPath,
			SnapshotID:  sid,
			UpdatedAt:   updatedAt,
			RefreshedAt: updatedAt,
		}),
	)

	So(writeErr, ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func summariseSpoolLoaderSchema2FactRow(
	sid string,
	updatedAt time.Time,
	dir string,
	childCount uint64,
) chspool.DirFactRow {
	const count uint64 = 3

	size := count * 10
	buckets := summariseSpoolLoaderSchema2Buckets(count)

	return chspool.DirFactRow{
		MountPath:        testMountPath,
		SnapshotID:       sid,
		DirID:            summariseSpoolLoaderDirID(dir),
		ParentID:         summariseSpoolLoaderParentID(dir),
		SubtreeEnd:       summariseSpoolLoaderDirID(dir) + 1,
		UpdatedAt:        updatedAt,
		AllCount:         count,
		AllSize:          size,
		AllAtimeMin:      100,
		AllMtimeMax:      200,
		AllAtimeBuckets:  buckets,
		AllMtimeBuckets:  buckets,
		AllUIDs:          []uint32{17},
		AllGIDs:          []uint32{7},
		AllFT:            uint16(db.DGUTAFileTypeBam),
		FileCount:        count,
		FileSize:         size,
		FileAtimeMin:     100,
		FileMtimeMax:     200,
		FileAtimeBuckets: buckets,
		FileMtimeBuckets: buckets,
		FileUIDs:         []uint32{17},
		FileGIDs:         []uint32{7},
		FileFT:           uint16(db.DGUTAFileTypeBam),
		GIDs:             []uint32{7},
		UIDs:             []uint32{17},
		FTs:              []uint16{uint16(db.DGUTAFileTypeBam)},
		Ages:             []uint8{uint8(db.DGUTAgeAll)},
		Counts:           []uint64{count},
		Sizes:            []uint64{size},
		AtimeMins:        []int64{100},
		MtimeMaxs:        []int64{200},
		AtimeBuckets:     [][]uint64{buckets},
		MtimeBuckets:     [][]uint64{buckets},
		ChildCount:       childCount,
		RefreshedAt:      updatedAt,
	}
}

func summariseSpoolLoaderSchema2Buckets(count uint64) []uint64 {
	return []uint64{count, 0, 0, 0, 0, 0, 0, 0, 0}
}

func summariseSpoolLoaderSchema2AgeAllRow(row chspool.DirFactRow) chspool.DirFilterAgeAllRow {
	return chspool.DirFilterAgeAllRow{
		MountPath:    row.MountPath,
		SnapshotID:   row.SnapshotID,
		GID:          row.GIDs[0],
		UID:          row.UIDs[0],
		FT:           row.FTs[0],
		DirID:        row.DirID,
		SubtreeEnd:   row.SubtreeEnd,
		Count:        row.Counts[0],
		Size:         row.Sizes[0],
		AtimeMin:     row.AtimeMins[0],
		MtimeMax:     row.MtimeMaxs[0],
		AtimeBuckets: row.AtimeBuckets[0],
		MtimeBuckets: row.MtimeBuckets[0],
		RefreshedAt:  row.RefreshedAt,
	}
}

func writeSummariseSpoolLoaderFileOnlySpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	So(set.WriteFile(chspool.FileRow{
		MountPath:  testMountPath,
		SnapshotID: sid,
		DirID:      summariseSpoolLoaderDirID(testMountPath),
		Name:       "file.txt",
		ATime:      updatedAt,
		MTime:      updatedAt,
		CTime:      updatedAt,
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func writeSummariseSpoolLoaderZeroContributionActiveVirtualSpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	activeRows := []mountsActiveRow{{
		mountPath:  testMountPath,
		snapshotID: sid,
		updatedAt:  activeSetUpdatedAt(updatedAt),
	}}
	activeSetID := fingerprintForMountsActive(activeRows)
	So(set.WriteActiveVirtualSet(chspool.ActiveVirtualSetRow{
		ActiveSetID:      activeSetID,
		Schema3Version:   currentSchemaVersion,
		MountsSHA256:     activeSetID,
		ActiveMountCount: 1,
		SummaryRows:      3,
		FilterRows:       0,
		ChildRows:        2,
		ManifestSHA256:   activeVirtualManifestSHA256(activeSetID, 3, 0, 2),
		Ready:            1,
		RefreshedAt:      updatedAt,
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func summariseSpoolPreviousActiveVirtualSummary(
	dir string,
	activeSetID string,
	updatedAt time.Time,
	childCount uint64,
) activeVirtualSummaryRow {
	return activeVirtualSummaryRow{
		ActiveSetID:     activeSetID,
		Dir:             dir,
		UpdatedAt:       updatedAt,
		AllCount:        10,
		AllSize:         100,
		AllAtimeMin:     1,
		AllMtimeMax:     2,
		AllAtimeBuckets: emptyAgeBuckets(),
		AllMtimeBuckets: emptyAgeBuckets(),
		AllUIDs:         []uint32{17},
		AllGIDs:         []uint32{7},
		AllFT:           uint16(db.DGUTAFileTypeBam),
		FileCount:       10,
		FileSize:        100,
		ChildCount:      childCount,
		RefreshedAt:     updatedAt,
	}
}

func summariseSpoolPreviousActiveVirtualFilter(
	dir string,
	activeSetID string,
	updatedAt time.Time,
) activeVirtualFilterAllRow {
	const childCount = uint64(1)

	return activeVirtualFilterAllRow{
		ActiveSetID:      activeSetID,
		Dir:              dir,
		Age:              uint8(db.DGUTAgeAll),
		GID:              7,
		UID:              17,
		FT:               uint16(db.DGUTAFileTypeBam),
		Count:            10,
		Size:             100,
		AtimeMin:         1,
		MtimeMax:         2,
		AtimeBuckets:     emptyAgeBuckets(),
		MtimeBuckets:     emptyAgeBuckets(),
		FilterChildCount: childCount,
		ChildCount:       childCount,
		RefreshedAt:      updatedAt,
	}
}

func summariseSpoolPreviousActiveVirtualMountRootSummary(
	dir string,
	activeSetID string,
	updatedAt time.Time,
	childCount uint64,
) activeVirtualSummaryRow {
	row := summariseSpoolPreviousActiveVirtualSummary(dir, activeSetID, updatedAt, childCount)
	row.MountPath = ensureTrailingSlash(dir)
	row.IsMountRootBox = 1

	return row
}

func summariseSpoolActiveVirtualChildCountForTest(
	values [][]any,
	parentDir string,
	childDir string,
) (uint64, bool) {
	parentID := summariseSpoolLoaderVirtualIDForDir(parentDir)
	childID := summariseSpoolLoaderVirtualIDForDir(childDir)

	for _, row := range values {
		if len(row) < 6 {
			continue
		}

		parent, parentOK := row[1].(uint32)
		child, childOK := row[2].(uint32)

		count, countOK := row[5].(uint64)
		if parentOK && childOK && countOK && parent == parentID && child == childID {
			return count, true
		}
	}

	return 0, false
}

func writeSummariseSpoolLoaderSchema2BatchSpool(
	spoolDir string,
	updatedAt time.Time,
	dirsRows int,
	factRows int,
	ageAllRows int,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)

	var writeErr error
	for i := range dirsRows {
		writeErr = errors.Join(writeErr, set.WriteDir(chspool.DirRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			DirID:      uint32(i + 100),
			ParentID:   summariseSpoolLoaderDirID(testMountPath),
			SubtreeEnd: uint32(i + 101),
			Name:       "batch",
			FullPath:   testMountPath + "batch/",
			PathHash:   uint64(i + 100),
		}))
	}

	for range factRows {
		writeErr = errors.Join(writeErr, set.WriteDirFact(chspool.DirFactRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			DirID:      summariseSpoolLoaderDirID(testMountPath),
			ParentID:   summariseSpoolLoaderParentID(testMountPath),
			SubtreeEnd: summariseSpoolLoaderDirID(testMountPath) + 1,
			UpdatedAt:  updatedAt,
		}))
	}

	for range ageAllRows {
		writeErr = errors.Join(writeErr, set.WriteDirFilterAgeAll(chspool.DirFilterAgeAllRow{
			MountPath:   testMountPath,
			SnapshotID:  sid,
			DirID:       summariseSpoolLoaderDirID(testMountPath),
			SubtreeEnd:  summariseSpoolLoaderDirID(testMountPath) + 1,
			RefreshedAt: updatedAt,
		}))
	}

	So(writeErr, ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func newSummariseSpoolLoaderSpyConn(manifest *chspool.Manifest) *summariseSpoolLoaderSpyConn {
	return &summariseSpoolLoaderSpyConn{
		manifest:       manifest,
		batches:        map[string][]*summariseSpoolLoaderSpyBatch{},
		countOverrides: map[string]uint64{},
	}
}

func summariseSpoolReportOperation(report perfreport.Report, name string) *perfreport.Operation {
	for i := range report.Operations {
		if report.Operations[i].Name == name {
			return &report.Operations[i]
		}
	}

	return nil
}

func summariseSpoolReportUint64MapInput(inputs map[string]any, key string) map[string]uint64 {
	value, ok := inputs[key]
	if !ok {
		return nil
	}

	typed, ok := value.(map[string]uint64)
	if !ok {
		return nil
	}

	return typed
}

func summariseSpoolReportUint64Input(inputs map[string]any, key string) uint64 {
	value, ok := inputs[key]
	if !ok {
		return 0
	}

	typed, ok := value.(uint64)
	if !ok {
		return 0
	}

	return typed
}

func newSummariseSpoolPublishFaultConn(manifest *chspool.Manifest) *summariseSpoolPublishFaultConn {
	conn := &summariseSpoolPublishFaultConn{
		summariseSpoolLoaderSpyConn: newSummariseSpoolLoaderSpyConn(manifest),
	}
	plan := summariseSpoolPublishPlanForTest(manifest)
	conn.activeRows = append([]mountsActiveRow(nil), plan.previousRows...)

	return conn
}

func summariseSpoolPublishPlanForTest(manifest *chspool.Manifest) summariseSpoolPublishPlanFixture {
	updatedAt, err := time.Parse(time.RFC3339Nano, manifest.UpdatedAt)
	So(err, ShouldBeNil)

	previousUpdatedAt := updatedAt.Add(-time.Hour)
	previousSnapshotID := SnapshotID(manifest.MountPath, previousUpdatedAt)
	previousRows := []mountsActiveRow{{
		mountPath:  manifest.MountPath,
		snapshotID: previousSnapshotID,
		updatedAt:  previousUpdatedAt,
	}}
	nextRows := stagedMountsActiveRows(previousRows, mountsActiveRow{
		mountPath:  manifest.MountPath,
		snapshotID: manifest.SnapshotID,
		updatedAt:  updatedAt,
	})

	return summariseSpoolPublishPlanFixture{
		previousRows:        previousRows,
		previousSnapshotID:  previousSnapshotID,
		previousActiveSetID: fingerprintForMountsActive(previousRows),
		nextActiveSetID:     fingerprintForMountsActive(nextRows),
	}
}

func summariseSpoolPublishChmodRecorder(
	t *testing.T,
	spoolDir string,
	chmodOnPhase string,
) func(string, time.Duration) {
	t.Helper()

	if chmodOnPhase == "" {
		return nil
	}

	chmodded := false

	return func(phase string, _ time.Duration) {
		if phase != chmodOnPhase || chmodded {
			return
		}

		chmodded = true

		So(os.Chmod(spoolDir, 0o500), ShouldBeNil)
	}
}

func summariseSpoolPublishStatePhasesForTest(
	t *testing.T,
	spoolDir string,
	manifest *chspool.Manifest,
) map[string]string {
	t.Helper()

	tracker, err := newSummariseSpoolPublishTracker(spoolDir, manifest)
	So(err, ShouldBeNil)

	return tracker.state.CompletedPhases
}

func summariseSpoolPublishAssertAllPhasesComplete(
	t *testing.T,
	spoolDir string,
	manifest *chspool.Manifest,
) {
	t.Helper()

	phases := summariseSpoolPublishStatePhasesForTest(t, spoolDir, manifest)
	for _, phase := range summariseSpoolPublishOrderedPhasesForTest() {
		So(phases[phase], ShouldNotBeBlank)
	}
}

func summariseSpoolPublishOrderedPhasesForTest() []string {
	return []string{
		summariseSpoolPublishPhaseTablesLoaded,
		summariseSpoolPublishPhaseActiveVirtualReady,
		summariseSpoolPublishPhaseSwitchPlanned,
		summariseSpoolPublishPhaseMountSwitched,
		summariseSpoolPublishPhaseOldSnapshotDropped,
		summariseSpoolPublishPhaseOldActiveVirtualDropped,
		summariseSpoolPublishPhaseTreeSummaryRefreshed,
		summariseSpoolPublishPhaseActivePrefixRefreshed,
		summariseSpoolPublishPhasePostSpoolPublishComplete,
	}
}

func summariseSpoolPublishAssertRetry(
	t *testing.T,
	conn *summariseSpoolPublishFaultConn,
	retryPhases []string,
	tc summariseSpoolPublishFaultCase,
) {
	t.Helper()

	So(conn.eventIndex("send "+chspool.TableFiles), ShouldEqual, -1)

	if tc.wantRetryPublish {
		So(conn.eventIndex(summariseSpoolPublishFaultEventPublish), ShouldBeGreaterThanOrEqualTo, 0)
	} else {
		So(conn.eventIndex(summariseSpoolPublishFaultEventPublish), ShouldEqual, -1)
	}

	for _, event := range tc.wantRetryEvents {
		So(conn.eventIndex(event), ShouldBeGreaterThanOrEqualTo, 0)
	}

	for _, event := range tc.forbidRetryEvents {
		So(conn.eventIndex(event), ShouldEqual, -1)
	}

	for _, phase := range tc.wantRetryPhases {
		So(summariseSpoolPublishPhaseCount(retryPhases, phase), ShouldBeGreaterThan, 0)
	}

	for _, phase := range tc.forbidRetryPhases {
		So(summariseSpoolPublishPhaseCount(retryPhases, phase), ShouldEqual, 0)
	}
}

func summariseSpoolPublishPhaseCount(phases []string, want string) int {
	var count int

	for _, phase := range phases {
		if phase == want {
			count++
		}
	}

	return count
}

func summariseSpoolPublishSeedStateThrough(
	t *testing.T,
	spoolDir string,
	manifest *chspool.Manifest,
	throughPhase string,
) {
	t.Helper()

	tracker, err := newSummariseSpoolPublishTracker(spoolDir, manifest)
	So(err, ShouldBeNil)

	plan := summariseSpoolPublishPlanForTest(manifest)
	switchPlan := summariseSpoolSwitchPlan{
		HasPrevious:         true,
		PreviousSnapshotID:  plan.previousSnapshotID,
		PreviousActiveSetID: plan.previousActiveSetID,
		NextActiveSetID:     plan.nextActiveSetID,
	}

	for _, phase := range summariseSpoolPublishOrderedPhasesForTest() {
		switch phase {
		case summariseSpoolPublishPhaseActiveVirtualReady:
			So(tracker.setNextActiveSetID(plan.nextActiveSetID), ShouldBeNil)
			So(tracker.mark(phase), ShouldBeNil)
		case summariseSpoolPublishPhaseSwitchPlanned:
			So(tracker.setSwitchPlan(switchPlan), ShouldBeNil)
		default:
			So(tracker.mark(phase), ShouldBeNil)
		}

		if phase == throughPhase {
			return
		}
	}
}

type summariseSpoolPublishFaultCase struct {
	name                   string
	failEvent              string
	failAfterMountSwitched bool
	failSkip               int
	chmodOnPhase           string
	completedPhase         string
	wantRetryPublish       bool
	wantRetryEvents        []string
	forbidRetryEvents      []string
	wantRetryPhases        []string
	forbidRetryPhases      []string
}

func summariseSpoolLoaderVirtualDirForTest(args ...any) (string, bool) {
	if len(args) < 3 {
		return "", false
	}

	virtualID, ok := args[2].(uint32)
	if !ok {
		return "", false
	}

	for _, dir := range []string{"/", testRootMountPath, testMountPath, testMountPath + "project/"} {
		if summariseSpoolLoaderVirtualIDForDir(dir) == virtualID {
			return ensureTrailingSlash(dir), true
		}
	}

	return "", false
}

type summariseSpoolPublishFaultConn struct {
	*summariseSpoolLoaderSpyConn

	activeRows             []mountsActiveRow
	failEvent              string
	failAfterMountSwitched bool
	failSkip               int
	mountSwitched          bool
}

func (c *summariseSpoolPublishFaultConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch query {
	case activeSnapshotQuery:
		if err := c.maybeFail(summariseSpoolPublishFaultEventActiveSnapshot); err != nil {
			return nil, err
		}

		return c.activeSnapshotRows(args...), nil
	case mountsActiveRowsQuery:
		if err := c.maybeFail(summariseSpoolPublishFaultEventMountsActive); err != nil {
			return nil, err
		}

		return mountsActiveRowsForTest(c.activeRows), nil
	default:
		return c.summariseSpoolLoaderSpyConn.Query(ctx, query, args...)
	}
}

func (c *summariseSpoolPublishFaultConn) activeSnapshotRows(args ...any) driver.Rows {
	mountPath := ""

	if len(args) > 0 {
		if value, ok := args[0].(string); ok {
			mountPath = value
		}
	}

	rows := &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}

	for _, row := range c.activeRows {
		if row.mountPath == mountPath {
			rows.values = append(rows.values, []any{row.snapshotID})

			break
		}
	}

	return rows
}

func (c *summariseSpoolPublishFaultConn) Exec(ctx context.Context, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch {
	case query == switchSnapshotQuery:
		c.recordEvent(summariseSpoolPublishFaultEventPublish)

		if err := c.maybeFail(summariseSpoolPublishFaultEventPublish); err != nil {
			return err
		}

		c.publishRow(switchSnapshotMountPathArg(args), switchSnapshotIDArg(args), switchSnapshotUpdatedAtArg(args))

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		table := alterTableNameForTest(query)
		event := "drop " + table
		c.recordEvent(event)

		if err := c.maybeFail(event); err != nil {
			return err
		}

		c.batches[summariseSpoolLoaderCHTableToSpoolTable(table)] = nil

		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func (c *summariseSpoolPublishFaultConn) publishCurrentManifest() {
	manifest := c.manifest
	updatedAt, err := time.Parse(time.RFC3339Nano, manifest.UpdatedAt)
	So(err, ShouldBeNil)

	c.publishRow(manifest.MountPath, manifest.SnapshotID, activeSetUpdatedAt(updatedAt))
}

func (c *summariseSpoolPublishFaultConn) publishRow(mountPath string, snapshotID string, updatedAt time.Time) {
	c.activeEvents++
	c.publishedSID = snapshotID
	c.mountSwitched = true
	c.activeRows = stagedMountsActiveRows(c.activeRows, mountsActiveRow{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		updatedAt:  updatedAt,
	})
}

func (c *summariseSpoolPublishFaultConn) maybeFail(event string) error {
	if c.failEvent != event {
		return nil
	}

	if c.failAfterMountSwitched && !c.mountSwitched {
		return nil
	}

	if c.failSkip > 0 {
		c.failSkip--

		return nil
	}

	return errForcedFailure
}

func (c *summariseSpoolPublishFaultConn) disableFault() {
	c.failEvent = ""
	c.failSkip = 0
}

type summariseSpoolPublishPlanFixture struct {
	previousRows        []mountsActiveRow
	previousSnapshotID  string
	previousActiveSetID string
	nextActiveSetID     string
}

type summariseSpoolHistoryDeleteDeadlineConn struct {
	bootstrapTestConn

	normalWindow time.Duration
	err          error

	deleteCalls          int
	cleanupDeadlineCalls int
}

func (c *summariseSpoolHistoryDeleteDeadlineConn) Exec(
	ctx context.Context,
	query string,
	_ ...any,
) error {
	if !strings.HasPrefix(query, "ALTER TABLE wrstat_basedirs_history DELETE") {
		return errBootstrapTestUnexpectedCall
	}

	c.deleteCalls++

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return context.DeadlineExceeded
	}

	c.cleanupDeadlineCalls++

	return c.err
}

type summariseSpoolHistoryDeleteLocalMutationConn struct {
	bootstrapTestConn

	query       string
	deleteCalls int
}

func (c *summariseSpoolHistoryDeleteLocalMutationConn) Exec(
	_ context.Context,
	query string,
	_ ...any,
) error {
	if !strings.HasPrefix(query, "ALTER TABLE wrstat_basedirs_history DELETE") {
		return errBootstrapTestUnexpectedCall
	}

	c.query = query
	c.deleteCalls++

	if strings.Contains(query, "mutations_sync = 2") {
		return context.DeadlineExceeded
	}

	if !strings.Contains(query, "mutations_sync = 1") {
		return errBootstrapTestUnexpectedCall
	}

	return nil
}

type summariseSpoolHistoryLastDateKeyForTest struct {
	mountPath string
	gid       uint32
}

type summariseSpoolHistoryBatchInsertConn struct {
	bootstrapTestConn

	lastDates map[summariseSpoolHistoryLastDateKeyForTest]time.Time

	insertBatch           b1ImportSQLSpyBatch
	deleteMountPaths      []string
	lastDateMountPaths    []string
	lastDateQueries       int
	perRowLastDateQueries int
	execInserts           int
}

func (c *summariseSpoolHistoryBatchInsertConn) Query(
	_ context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if query == queryBasedirsHistoryLastDate {
		c.perRowLastDateQueries++

		return emptyMountsActiveRowsForTest(), nil
	}

	unknownLastDatesQuery := !strings.Contains(query, "FROM wrstat_basedirs_history") ||
		!strings.Contains(query, "GROUP BY gid")
	if unknownLastDatesQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	c.lastDateQueries++

	if len(args) == 0 {
		return nil, errBootstrapTestUnexpectedCall
	}

	mountPath, ok := args[0].(string)
	if !ok {
		return nil, errBootstrapTestUnexpectedCall
	}

	c.lastDateMountPaths = append(c.lastDateMountPaths, mountPath)

	rows := &dgutaWriterCloseContextRows{}

	for _, arg := range args[1:] {
		gid, ok := arg.(uint32)
		if !ok {
			continue
		}

		date, ok := c.lastDates[summariseSpoolHistoryLastDateKeyForTest{
			mountPath: mountPath,
			gid:       gid,
		}]
		if ok {
			rows.values = append(rows.values, []any{gid, date})
		}
	}

	return rows, nil
}

func (c *summariseSpoolHistoryBatchInsertConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if query != insertBasedirsHistoryPoint {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &c.insertBatch, nil
}

func (c *summariseSpoolHistoryBatchInsertConn) Exec(
	_ context.Context,
	query string,
	args ...any,
) error {
	if query == insertBasedirsHistoryPoint {
		c.execInserts++

		return nil
	}

	if !strings.HasPrefix(query, "ALTER TABLE wrstat_basedirs_history DELETE") {
		return errBootstrapTestUnexpectedCall
	}

	if len(args) == 0 {
		return errBootstrapTestUnexpectedCall
	}

	mountPath, ok := args[0].(string)
	if !ok {
		return errBootstrapTestUnexpectedCall
	}

	c.deleteMountPaths = append(c.deleteMountPaths, mountPath)

	return nil
}

type summariseSpoolPublishActiveVirtualDeadlineConn struct {
	b1ImportSQLSpyConn

	normalWindow  time.Duration
	sourceDelay   time.Duration
	existingMount activeMount

	activeVirtualSourceQueries int
	longDeadlineSourceQueries  int
	switches                   int
	published                  activeMount
}

func (c *summariseSpoolPublishActiveVirtualDeadlineConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	switch {
	case query == activeSnapshotQuery:
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}, nil
	case query == mountsActiveRowsQuery:
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return c.mountRows(), nil
	case isB1ImportSQLSpyActiveVirtualSourceQuery(query):
		return c.activeVirtualSourceRows(ctx)
	default:
		return c.b1ImportSQLSpyConn.Query(ctx, query, args...)
	}
}

func (c *summariseSpoolPublishActiveVirtualDeadlineConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if query == switchSnapshotQuery {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.switches++
		c.published = activeMount{
			mountPath:  switchSnapshotMountPathArg(args),
			snapshotID: switchSnapshotIDArg(args),
			updatedAt:  switchSnapshotUpdatedAtArg(args),
		}

		return nil
	}

	return c.b1ImportSQLSpyConn.Exec(ctx, query, args...)
}

func switchSnapshotMountPathArg(args []any) string {
	if len(args) < 1 {
		return ""
	}

	value, ok := args[0].(string)
	if !ok {
		return ""
	}

	return value
}

func switchSnapshotIDArg(args []any) string {
	if len(args) < 2 {
		return ""
	}

	value, ok := args[1].(string)
	if !ok {
		return ""
	}

	return value
}

func switchSnapshotUpdatedAtArg(args []any) time.Time {
	if len(args) < 3 {
		return time.Time{}
	}

	value, ok := args[2].(time.Time)
	if !ok {
		return time.Time{}
	}

	return value
}

func (c *summariseSpoolPublishActiveVirtualDeadlineConn) mountRows() driver.Rows {
	values := [][]any{{
		c.existingMount.mountPath,
		c.existingMount.snapshotID,
		c.existingMount.updatedAt,
	}}
	if c.published.mountPath != "" {
		values = append(values, []any{
			c.published.mountPath,
			c.published.snapshotID,
			c.published.updatedAt,
		})
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{
			dgutaWriterTestMountPathColumn,
			dgutaWriterTestSnapshotIDColumn,
			dgutaWriterTestUpdatedAtColumn,
		},
		values: values,
	}
}

func (c *summariseSpoolPublishActiveVirtualDeadlineConn) activeVirtualSourceRows(
	ctx context.Context,
) (driver.Rows, error) {
	c.activeVirtualSourceQueries++

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return nil, context.DeadlineExceeded
	}

	c.longDeadlineSourceQueries++
	if c.sourceDelay > 0 {
		time.Sleep(c.sourceDelay)
	}

	return &dgutaWriterCloseContextRows{}, nil
}

type summariseSpoolZeroActiveVirtualComposeConn struct {
	b1ImportSQLSpyConn

	activeRows          []mountsActiveRow
	previousActiveSetID string
	previousSummaryRows []activeVirtualSummaryRow
	previousFilterRows  []activeVirtualFilterAllRow
	previousSet         activeVirtualSetRow

	activeVirtualSourceQueries int
	copyQueries                int
	switches                   int
}

func (c *summariseSpoolZeroActiveVirtualComposeConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if rows, ok := c.activeVirtualValidationRows(query); ok {
		return rows, nil
	}

	switch {
	case query == activeSnapshotQuery:
		return c.activeSnapshotRows(args...), nil
	case query == mountsActiveRowsQuery:
		return mountsActiveRowsForTest(c.activeRows), nil
	case isB1ImportSQLSpyActiveVirtualSourceQuery(query):
		c.activeVirtualSourceQueries++

		return &dgutaWriterCloseContextRows{}, nil
	case strings.Contains(query, "FROM wrstat_active_virtual_sets") &&
		strings.Contains(query, "summary_rows"):
		return c.previousActiveVirtualSetRows(args...), nil
	case strings.Contains(query, "FROM wrstat_active_virtual_summaries") &&
		strings.Contains(query, "dir IN"):
		return activeVirtualSummaryRowsForComposeTest(c.previousSummaryRows, args...), nil
	case strings.Contains(query, "FROM wrstat_active_virtual_filter_all") &&
		strings.Contains(query, "dir IN"):
		return activeVirtualFilterRowsForComposeTest(c.previousFilterRows, args...), nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func activeVirtualSummaryRowsForComposeTest(rows []activeVirtualSummaryRow, args ...any) driver.Rows {
	out := &dgutaWriterCloseContextRows{}
	dirs := activeVirtualDirsForComposeTest(args...)

	for _, row := range rows {
		if !activeVirtualComposeTestWantsDir(dirs, row.Dir) {
			continue
		}

		out.values = append(out.values, []any{
			row.Dir,
			row.MountPath,
			row.IsMountRootBox,
			row.UpdatedAt,
			row.AllCount,
			row.AllSize,
			row.AllAtimeMin,
			row.AllMtimeMax,
			row.AllAtimeBuckets,
			row.AllMtimeBuckets,
			row.AllUIDs,
			row.AllGIDs,
			row.AllFT,
			row.FileCount,
			row.FileSize,
			row.ChildCount,
		})
	}

	return out
}

func activeVirtualFilterRowsForComposeTest(rows []activeVirtualFilterAllRow, args ...any) driver.Rows {
	out := &dgutaWriterCloseContextRows{}
	dirs := activeVirtualDirsForComposeTest(args...)

	for _, row := range rows {
		if !activeVirtualComposeTestWantsDir(dirs, row.Dir) {
			continue
		}

		out.values = append(out.values, []any{
			row.Dir,
			row.Age,
			row.GID,
			row.UID,
			row.FT,
			row.Count,
			row.Size,
			row.AtimeMin,
			row.MtimeMax,
			row.AtimeBuckets,
			row.MtimeBuckets,
			row.FilterChildCount,
			row.ChildCount,
		})
	}

	return out
}

func (c *summariseSpoolZeroActiveVirtualComposeConn) activeSnapshotRows(args ...any) driver.Rows {
	mountPath := ""

	if len(args) > 0 {
		value, ok := args[0].(string)
		if ok {
			mountPath = value
		}
	}

	rows := &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}

	for _, row := range c.activeRows {
		if row.mountPath == mountPath {
			rows.values = append(rows.values, []any{row.snapshotID})

			return rows
		}
	}

	return rows
}

func (c *summariseSpoolZeroActiveVirtualComposeConn) previousActiveVirtualSetRows(args ...any) driver.Rows {
	rows := &dgutaWriterCloseContextRows{}
	if len(args) == 0 || args[0] != c.previousActiveSetID {
		return rows
	}

	rows.values = append(rows.values, []any{
		c.previousSet.SummaryRows,
		c.previousSet.FilterRows,
		c.previousSet.ChildRows,
	})

	return rows
}

func (c *summariseSpoolZeroActiveVirtualComposeConn) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) driver.Row {
	switch {
	case strings.Contains(query, "FROM wrstat_active_virtual_summaries"):
		return summariseSpoolCountRow{value: 4}
	case strings.Contains(query, "FROM wrstat_active_virtual_filter_all"):
		return summariseSpoolCountRow{value: 2}
	case strings.Contains(query, "FROM wrstat_active_virtual_children"):
		return summariseSpoolCountRow{value: 3}
	default:
		return summariseSpoolCountRow{err: errBootstrapTestUnexpectedCall}
	}
}

func (c *summariseSpoolZeroActiveVirtualComposeConn) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch {
	case query == switchSnapshotQuery:
		c.switches++
		c.activeRows = stagedMountsActiveRows(c.activeRows, mountsActiveRow{
			mountPath:  switchSnapshotMountPathArg(args),
			snapshotID: switchSnapshotIDArg(args),
			updatedAt:  switchSnapshotUpdatedAtArg(args),
		})

		return nil
	case strings.HasPrefix(query, "INSERT INTO wrstat_active_virtual_") &&
		strings.Contains(query, " SELECT "):
		c.copyQueries++

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

type summariseSpoolLoaderLazyImportConn struct {
	lazyDGUTAImportConn
}

func (c *summariseSpoolLoaderLazyImportConn) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) driver.Row {
	table := summariseSpoolLoaderCountQueryTable(query)
	if table == "" {
		return summariseSpoolCountRow{err: errBootstrapTestUnexpectedCall}
	}

	insertQuery := summariseSpoolLoaderInsertQuery(table)
	if insertQuery == "" {
		return summariseSpoolCountRow{err: errBootstrapTestUnexpectedCall}
	}

	return summariseSpoolCountRow{value: summariseSpoolLoaderUint64ForTest(c.totalRowsFor(insertQuery))}
}

func summariseSpoolLoaderCountQueryTable(query string) string {
	for spoolTable, chTable := range summariseSpoolLoaderCHTables() {
		if strings.Contains(query, "FROM "+chTable+" ") {
			return spoolTable
		}
	}

	return ""
}

func summariseSpoolLoaderInsertQuery(table string) string {
	switch table {
	case chspool.TableDirs:
		return insertDirsQuery
	case chspool.TableFiles:
		return insertFilesBatchQuery
	case chspool.TableDirFacts:
		return insertMountDirSummaryQuery
	case chspool.TableDirFilterAgeAll:
		return insertDirFilterAgeAllQuery
	case chspool.TableChildFilterAll:
		return insertChildFilterAllQuery
	case chspool.TableDirFilterAll:
		return insertDirFilterAllQuery
	case chspool.TableDirProjectionSets:
		return insertMountDirSummarySetQuery
	case chspool.TableSchema3SnapshotSets:
		return insertSchema3SnapshotSetQuery
	case chspool.TableActiveVirtualSummaries:
		return insertActiveVirtualSummaryQuery
	case chspool.TableActiveVirtualFilterAll:
		return insertActiveVirtualFilterAllQuery
	case chspool.TableActiveVirtualChildren:
		return insertActiveVirtualChildQuery
	case chspool.TableActiveVirtualSets:
		return insertActiveVirtualSetQuery
	case chspool.TableBasedirsGroupUsage:
		return insertBasedirsGroupUsageQuery
	case chspool.TableBasedirsUserUsage:
		return insertBasedirsUserUsageQuery
	case chspool.TableBasedirsGroupSubdirs:
		return insertBasedirsGroupSubdirsQuery
	case chspool.TableBasedirsUserSubdirs:
		return insertBasedirsUserSubdirsQuery
	default:
		return ""
	}
}

func summariseSpoolLoaderUint64ForTest(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}

type summariseSpoolLoaderSpyConn struct {
	bootstrapTestConn

	manifest       *chspool.Manifest
	batches        map[string][]*summariseSpoolLoaderSpyBatch
	events         []string
	countOverrides map[string]uint64
	tableStatsErr  error
	activeEvents   int
	publishedSID   string
}

func (c *summariseSpoolLoaderSpyConn) Query(
	_ context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}, nil
	case mountsActiveRowsQuery:
		return emptyMountsActiveRowsForTest(), nil
	default:
		if strings.Contains(query, "FROM system.parts") {
			if c.tableStatsErr != nil {
				return nil, c.tableStatsErr
			}

			return c.tableStatsRows(args[1:]...), nil
		}

		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *summariseSpoolLoaderSpyConn) tableStatsRows(tables ...any) driver.Rows {
	rows := &dgutaWriterCloseContextRows{
		columns: []string{
			"table",
			"sum(rows)",
			"count()",
			"sum(data_compressed_bytes)",
			"sum(data_uncompressed_bytes)",
		},
	}

	for _, value := range tables {
		table, ok := value.(string)
		if !ok {
			continue
		}

		count := c.insertedRows(table)
		if count == 0 {
			continue
		}

		bytes := summariseSpoolLoaderReportBytes(c.manifest.Tables[table])
		rows.values = append(rows.values, []any{table, count, uint64(1), bytes, bytes * 2})
	}

	return rows
}

func summariseSpoolLoaderReportBytes(table chspool.TableManifest) uint64 {
	if table.Bytes <= 0 {
		return 1
	}

	return uint64(table.Bytes)
}

func (c *summariseSpoolLoaderSpyConn) QueryRow(
	_ context.Context,
	query string,
	args ...any,
) driver.Row {
	if strings.Contains(query, "FROM wrstat_dirs") && strings.Contains(query, "toUInt32(path_hash)") {
		dir, ok := summariseSpoolLoaderVirtualDirForTest(args...)
		if !ok {
			return summariseSpoolCountRow{err: errBootstrapTestUnexpectedCall}
		}

		return summariseSpoolCountRow{stringValue: dir}
	}

	table := summariseSpoolLoaderCountQueryTable(query)
	if table == "" {
		return summariseSpoolCountRow{err: errBootstrapTestUnexpectedCall}
	}

	c.recordEvent("count " + table)

	if got, ok := c.countOverrides[table]; ok {
		return summariseSpoolCountRow{value: got}
	}

	return summariseSpoolCountRow{value: c.insertedRows(table)}
}

func (c *summariseSpoolLoaderSpyConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	table := summariseSpoolLoaderInsertQueryTable(query)
	if table == "" {
		return nil, errBootstrapTestUnexpectedCall
	}

	batch := &summariseSpoolLoaderSpyBatch{
		b1ImportSQLSpyBatch: b1ImportSQLSpyBatch{},
		conn:                c,
		table:               table,
	}
	c.batches[table] = append(c.batches[table], batch)

	return batch, nil
}

func summariseSpoolLoaderInsertQueryTable(query string) string {
	switch query {
	case insertDirsQuery:
		return chspool.TableDirs
	case insertFilesBatchQuery:
		return chspool.TableFiles
	case insertMountDirSummaryQuery:
		return chspool.TableDirFacts
	case insertDirFilterAgeAllQuery:
		return chspool.TableDirFilterAgeAll
	case insertChildFilterAllQuery:
		return chspool.TableChildFilterAll
	case insertDirFilterAllQuery:
		return chspool.TableDirFilterAll
	case insertMountDirSummarySetQuery:
		return chspool.TableDirProjectionSets
	case insertSchema3SnapshotSetQuery:
		return chspool.TableSchema3SnapshotSets
	case insertActiveVirtualSummaryQuery:
		return chspool.TableActiveVirtualSummaries
	case insertActiveVirtualFilterAllQuery:
		return chspool.TableActiveVirtualFilterAll
	case insertActiveVirtualChildQuery:
		return chspool.TableActiveVirtualChildren
	case insertActiveVirtualSetQuery:
		return chspool.TableActiveVirtualSets
	case insertBasedirsGroupUsageQuery:
		return chspool.TableBasedirsGroupUsage
	case insertBasedirsUserUsageQuery:
		return chspool.TableBasedirsUserUsage
	case insertBasedirsGroupSubdirsQuery:
		return chspool.TableBasedirsGroupSubdirs
	case insertBasedirsUserSubdirsQuery:
		return chspool.TableBasedirsUserSubdirs
	default:
		return ""
	}
}

func (c *summariseSpoolLoaderSpyConn) Exec(_ context.Context, query string, args ...any) error {
	switch {
	case query == switchSnapshotQuery:
		c.activeEvents++

		sid, ok := args[1].(string)
		if !ok {
			return errBootstrapTestUnexpectedCall
		}

		c.publishedSID = sid
		c.recordEvent("publish")

		return nil
	case strings.HasPrefix(query, "ALTER TABLE"):
		table := alterTableNameForTest(query)
		c.recordEvent("drop " + table)
		c.batches[summariseSpoolLoaderCHTableToSpoolTable(table)] = nil

		return nil
	default:
		return errBootstrapTestUnexpectedCall
	}
}

func summariseSpoolLoaderCHTableToSpoolTable(table string) string {
	for spoolTable, chTable := range summariseSpoolLoaderCHTables() {
		if chTable == table {
			return spoolTable
		}
	}

	return ""
}

func (c *summariseSpoolLoaderSpyConn) insertedRows(table string) uint64 {
	var count uint64
	for _, batch := range c.batches[table] {
		count += summariseSpoolLoaderUint64ForTest(batch.appended)
	}

	return count
}

func (c *summariseSpoolLoaderSpyConn) activePublishes() int {
	return c.activeEvents
}

func (c *summariseSpoolLoaderSpyConn) eventIndex(event string) int {
	for i, got := range c.events {
		if got == event {
			return i
		}
	}

	return -1
}

func (c *summariseSpoolLoaderSpyConn) recordEvent(event string) {
	c.events = append(c.events, event)
}

func (c *summariseSpoolLoaderSpyConn) resetEvents() {
	c.events = nil
}

type summariseSpoolLoaderSpyBatch struct {
	b1ImportSQLSpyBatch

	conn  *summariseSpoolLoaderSpyConn
	table string
}

func (b *summariseSpoolLoaderSpyBatch) Send() error {
	b.conn.recordEvent("send " + b.table)

	return b.b1ImportSQLSpyBatch.Send()
}

func activeVirtualDirsForComposeTest(args ...any) map[string]struct{} {
	if len(args) < 2 {
		return nil
	}

	out := make(map[string]struct{}, len(args)-1)
	for _, arg := range args[1:] {
		dir, ok := arg.(string)
		if !ok {
			continue
		}

		out[ensureTrailingSlash(dir)] = struct{}{}
	}

	return out
}

func activeVirtualComposeTestWantsDir(dirs map[string]struct{}, dir string) bool {
	if len(dirs) == 0 {
		return true
	}

	_, ok := dirs[ensureTrailingSlash(dir)]

	return ok
}

func summariseSpoolLoaderCHTables() map[string]string {
	return map[string]string{
		chspool.TableDirs:                   chspool.TableDirs,
		chspool.TableFiles:                  chspool.TableFiles,
		chspool.TableDirFacts:               chspool.TableDirFacts,
		chspool.TableDirFilterAgeAll:        chspool.TableDirFilterAgeAll,
		chspool.TableChildFilterAll:         chspool.TableChildFilterAll,
		chspool.TableDirFilterAll:           chspool.TableDirFilterAll,
		chspool.TableDirProjectionSets:      chspool.TableDirProjectionSets,
		chspool.TableSchema3SnapshotSets:    chspool.TableSchema3SnapshotSets,
		chspool.TableActiveVirtualSummaries: chspool.TableActiveVirtualSummaries,
		chspool.TableActiveVirtualFilterAll: chspool.TableActiveVirtualFilterAll,
		chspool.TableActiveVirtualChildren:  chspool.TableActiveVirtualChildren,
		chspool.TableActiveVirtualSets:      chspool.TableActiveVirtualSets,
		chspool.TableBasedirsGroupUsage:     chspool.TableBasedirsGroupUsage,
		chspool.TableBasedirsUserUsage:      chspool.TableBasedirsUserUsage,
		chspool.TableBasedirsGroupSubdirs:   chspool.TableBasedirsGroupSubdirs,
		chspool.TableBasedirsUserSubdirs:    chspool.TableBasedirsUserSubdirs,
	}
}
