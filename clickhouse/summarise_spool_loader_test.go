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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	summariseSpoolHistoryCountQuery = "SELECT count() FROM wrstat_basedirs_history " +
		"WHERE mount_path = ? AND gid = ?"
	summariseSpoolLoaderCountActivePrefixAgeAllQuery = "SELECT count() FROM wrstat_active_prefix_filter_ageall " +
		"WHERE active_set_id = ?"
	summariseSpoolLoaderCountActivePrefixSetQuery = "SELECT count() FROM wrstat_active_prefix_rollup_sets " +
		"WHERE active_set_id = ?"
	summariseSpoolLoaderMountPathColumn = "mount_path"
	summariseSpoolLoaderSchemaMarker    = "test"
	summariseSpoolLoaderUpdatedAtColumn = "updated_at"
)

type summariseSpoolCountRow struct {
	value uint64
	err   error
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

	value, ok := dest[0].(*uint64)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	*value = r.value

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
		So(err.Error(), ShouldContainSubstring, chspool.TableDirFilterAgeAll)
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
		So(countRows(ctx, verifyConn, dgutaWriterTestCountParentFactsQuery, testMountPath, sid), ShouldEqual, 1)

		activeRows, err := queryMountsActiveRows(ctx, verifyConn)
		So(err, ShouldBeNil)

		activeSetID := fingerprintForMountsActive(activeRows)
		So(activeSetID, ShouldNotBeBlank)
		So(countRows(ctx, verifyConn, summariseSpoolLoaderCountActivePrefixSetQuery, activeSetID), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolLoaderCountActivePrefixAgeAllQuery, activeSetID),
			ShouldBeGreaterThan, uint64(0))
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

	Convey("summarise spool replay caps schema2 fact and child batches", t, func() {
		const (
			factRows       = defaultProjectionBatchSize + 100
			ageAllRows     = defaultProjectionBatchSize + 90
			parentFactRows = defaultProjectionBatchSize + 80
			childRows      = defaultChildrenBatchSize + 100
		)

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 8, 11, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema2BatchSpool(
			spoolDir,
			updatedAt,
			factRows,
			ageAllRows,
			parentFactRows,
			childRows,
		)
		conn := &summariseSpoolLoaderLazyImportConn{}

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.loadTables(context.Background()), ShouldBeNil)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, factRows)
		So(conn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertDirFilterAgeAllQuery), ShouldEqual, ageAllRows)
		So(conn.maxRowsFor(insertDirFilterAgeAllQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertParentFactsQuery), ShouldEqual, parentFactRows)
		So(conn.maxRowsFor(insertParentFactsQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertChildrenQuery), ShouldEqual, childRows)
		So(conn.maxRowsFor(insertChildrenQuery), ShouldBeLessThanOrEqualTo, defaultChildrenBatchSize)
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
		set.WriteDirFact(rootFact),
		set.WriteDirFact(namespaceFact),
		set.WriteDirFact(mountFact),
		set.WriteChild(chspool.ChildRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			ParentDir:  testMountPath,
			Child:      testMountPath + "project/",
		}),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(rootFact)),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(namespaceFact)),
		set.WriteParentFact(summariseSpoolLoaderSchema2ParentFactRow(mountFact, "/")),
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
			DirFactsRows:       3,
			ParentFactsRows:    1,
			ChildrenRows:       1,
			ChildFilterAllRows: 1,
			DirFilterAllRows:   1,
			ManifestSHA256:     "schema3-snapshot-test",
			RefreshedAt:        updatedAt,
		}),
		set.WriteActiveVirtualSummary(summariseSpoolLoaderActiveVirtualSummaryRow(activeSetID, updatedAt)),
		set.WriteActiveVirtualFilterAll(summariseSpoolLoaderActiveVirtualFilterRow(activeSetID, updatedAt)),
		set.WriteActiveVirtualChild(chspool.ActiveVirtualChildRow{
			ActiveSetID:    activeSetID,
			ParentDir:      testRootMountPath,
			ChildDir:       testMountPath,
			MountPath:      testMountPath,
			IsMountRootBox: 1,
			ChildCount:     1,
			RefreshedAt:    updatedAt,
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

func summariseSpoolLoaderChildFilterAllRow(sid string, updatedAt time.Time) chspool.ChildFilterAllRow {
	return chspool.ChildFilterAllRow{
		MountPath:         testMountPath,
		SnapshotID:        sid,
		ParentDir:         testMountPath,
		Age:               uint8(db.DGUTAgeAll),
		GID:               7,
		UID:               17,
		FT:                uint16(db.DGUTAFileTypeBam),
		Dir:               testMountPath + "project/",
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
		Dir:               row.Dir,
		ParentDir:         row.ParentDir,
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

func summariseSpoolLoaderActiveVirtualSummaryRow(
	activeSetID string,
	updatedAt time.Time,
) chspool.ActiveVirtualSummaryRow {
	return chspool.ActiveVirtualSummaryRow{
		ActiveSetID:     activeSetID,
		Dir:             testRootMountPath,
		UpdatedAt:       updatedAt,
		AllAtimeBuckets: summariseSpoolLoaderSchema2Buckets(0),
		AllMtimeBuckets: summariseSpoolLoaderSchema2Buckets(0),
		ChildCount:      1,
		RefreshedAt:     updatedAt,
	}
}

func summariseSpoolLoaderActiveVirtualFilterRow(
	activeSetID string,
	updatedAt time.Time,
) chspool.ActiveVirtualFilterAllRow {
	return chspool.ActiveVirtualFilterAllRow{
		ActiveSetID:      activeSetID,
		Dir:              testRootMountPath,
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
		MountPath:   testMountPath,
		SnapshotID:  sid,
		GID:         7,
		BaseDir:     basedirsStoreTestBaseDir,
		Age:         uint8(db.DGUTAgeAll),
		UIDs:        []uint32{17},
		UsageSize:   50,
		QuotaSize:   100,
		UsageInodes: 5,
		QuotaInodes: 10,
		Mtime:       updatedAt,
		DateNoSpace: time.Unix(0, 0).UTC(),
		DateNoFiles: time.Unix(0, 0).UTC(),
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
		set.WriteDirFact(rootFact),
		set.WriteDirFact(namespaceFact),
		set.WriteDirFact(mountFact),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(rootFact)),
		set.WriteDirFilterAgeAll(summariseSpoolLoaderSchema2AgeAllRow(namespaceFact)),
		set.WriteParentFact(summariseSpoolLoaderSchema2ParentFactRow(mountFact, "/")),
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
		Dir:              dir,
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
		Dir:          row.Dir,
		Count:        row.Counts[0],
		Size:         row.Sizes[0],
		AtimeMin:     row.AtimeMins[0],
		MtimeMax:     row.MtimeMaxs[0],
		AtimeBuckets: row.AtimeBuckets[0],
		MtimeBuckets: row.MtimeBuckets[0],
		RefreshedAt:  row.RefreshedAt,
	}
}

func summariseSpoolLoaderSchema2ParentFactRow(row chspool.DirFactRow, parentDir string) chspool.ParentFactRow {
	hasChildren := uint8(0)
	if row.ChildCount > 0 {
		hasChildren = 1
	}

	return chspool.ParentFactRow{
		MountPath:        row.MountPath,
		SnapshotID:       row.SnapshotID,
		ParentDir:        parentDir,
		Dir:              row.Dir,
		UpdatedAt:        row.UpdatedAt,
		AllCount:         row.AllCount,
		AllSize:          row.AllSize,
		AllAtimeMin:      row.AllAtimeMin,
		AllMtimeMax:      row.AllMtimeMax,
		AllAtimeBuckets:  row.AllAtimeBuckets,
		AllMtimeBuckets:  row.AllMtimeBuckets,
		AllUIDs:          row.AllUIDs,
		AllGIDs:          row.AllGIDs,
		AllFT:            row.AllFT,
		FileCount:        row.FileCount,
		FileSize:         row.FileSize,
		FileAtimeMin:     row.FileAtimeMin,
		FileMtimeMax:     row.FileMtimeMax,
		FileAtimeBuckets: row.FileAtimeBuckets,
		FileMtimeBuckets: row.FileMtimeBuckets,
		FileUIDs:         row.FileUIDs,
		FileGIDs:         row.FileGIDs,
		FileFT:           row.FileFT,
		GIDs:             row.GIDs,
		UIDs:             row.UIDs,
		FTs:              row.FTs,
		Ages:             row.Ages,
		Counts:           row.Counts,
		Sizes:            row.Sizes,
		AtimeMins:        row.AtimeMins,
		MtimeMaxs:        row.MtimeMaxs,
		AtimeBuckets:     row.AtimeBuckets,
		MtimeBuckets:     row.MtimeBuckets,
		ChildCount:       row.ChildCount,
		HasChildren:      hasChildren,
		RefreshedAt:      row.RefreshedAt,
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
		ParentDir:  testMountPath,
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

func writeSummariseSpoolLoaderSchema2BatchSpool(
	spoolDir string,
	updatedAt time.Time,
	factRows int,
	ageAllRows int,
	parentFactRows int,
	childRows int,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)

	var writeErr error
	for range factRows {
		writeErr = errors.Join(writeErr, set.WriteDirFact(chspool.DirFactRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			Dir:        testMountPath,
			UpdatedAt:  updatedAt,
		}))
	}

	for range ageAllRows {
		writeErr = errors.Join(writeErr, set.WriteDirFilterAgeAll(chspool.DirFilterAgeAllRow{
			MountPath:   testMountPath,
			SnapshotID:  sid,
			Dir:         testMountPath,
			RefreshedAt: updatedAt,
		}))
	}

	for range parentFactRows {
		writeErr = errors.Join(writeErr, set.WriteParentFact(chspool.ParentFactRow{
			MountPath:   testMountPath,
			SnapshotID:  sid,
			ParentDir:   "/",
			Dir:         testMountPath,
			UpdatedAt:   updatedAt,
			RefreshedAt: updatedAt,
		}))
	}

	for range childRows {
		writeErr = errors.Join(writeErr, set.WriteChild(chspool.ChildRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			ParentDir:  testMountPath,
			Child:      "spool-child",
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
	case chspool.TableFiles:
		return insertFilesBatchQuery
	case chspool.TableDirFacts:
		return insertMountDirSummaryQuery
	case chspool.TableChildren:
		return insertChildrenQuery
	case chspool.TableParentFacts:
		return insertParentFactsQuery
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
	_ ...any,
) driver.Row {
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
	case insertFilesBatchQuery:
		return chspool.TableFiles
	case insertMountDirSummaryQuery:
		return chspool.TableDirFacts
	case insertChildrenQuery:
		return chspool.TableChildren
	case insertParentFactsQuery:
		return chspool.TableParentFacts
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

func summariseSpoolLoaderCHTables() map[string]string {
	return map[string]string{
		chspool.TableFiles:                  chspool.TableFiles,
		chspool.TableDirFacts:               chspool.TableDirFacts,
		chspool.TableChildren:               chspool.TableChildren,
		chspool.TableParentFacts:            chspool.TableParentFacts,
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
