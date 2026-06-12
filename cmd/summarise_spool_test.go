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

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

func TestSummariseClickHouseSpoolRetry(t *testing.T) {
	Convey("summarise retry reuses a completed spool after ClickHouse publish fails", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
			So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThan, uint64(0))

			if loadCalls == 1 {
				return perfreport.Report{}, errSummariseTestClose
			}

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		err = run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise rebuilds instead of loading a corrupt completed spool", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			return perfreport.Report{}, errSummariseTestClose
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)

		manifestPath := chspool.ManifestPath(summariseClickHouseSpoolDir(fixture.outputDir))
		So(os.WriteFile(manifestPath, []byte(`{"state":"complete","mount_path":"wrong"}`), 0o600), ShouldBeNil)
		writeGzipStats(t, fixture.statsPath, []byte("not a valid wrstat row\n"))
		So(os.Chtimes(fixture.statsPath, fixture.updatedAt, fixture.updatedAt), ShouldBeNil)

		err = run([]string{fixture.statsPath})
		So(err, ShouldNotBeNil)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)
	})
}

func TestSummariseClickHouseSpoolRows(t *testing.T) {
	Convey("summarise spool records small-fixture rows for files, tree facts and basedirs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)

		target := &clickHouseSummariseTarget{
			cfg:       clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath: summariseTestMountPath,
			modtime:   fixture.updatedAt,
			outputDir: fixture.outputDir,
		}
		expected, err := newSummariseSpoolManifest(fixture.statsPath, target)
		So(err, ShouldBeNil)

		manifest, err := buildSummariseSpool(
			fixture.statsPath,
			summariseClickHouseSpoolDir(fixture.outputDir),
			expected,
			target,
			newSummariseDiagnostics(fixture.statsPath),
		)
		So(err, ShouldBeNil)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)

		So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThanOrEqualTo, uint64(3))
		So(manifest.Tables[chspool.TableChildren].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirFacts].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirFilterAgeAll].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableParentFacts].Rows, ShouldEqual, manifest.Tables[chspool.TableDirFacts].Rows)
		So(manifest.Tables[chspool.TableDirProjectionSets].Rows, ShouldEqual, uint64(1))
		So(manifest.Tables[chspool.TableBasedirsHistory].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsGroupUsage].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsUserUsage].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsGroupSubdirs].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsUserSubdirs].Rows, ShouldBeGreaterThan, uint64(0))

		var files []chspool.FileRow

		So(chspool.DecodeRows[chspool.FileRow](spoolDir, chspool.TableFiles, func(row chspool.FileRow) error {
			files = append(files, row)

			return nil
		}), ShouldBeNil)
		So(files[0].MountPath, ShouldEqual, summariseTestMountPath)
		So(files[len(files)-1].SnapshotID, ShouldEqual, manifest.SnapshotID)

		var facts []chspool.DirFactRow

		So(chspool.DecodeRows[chspool.DirFactRow](spoolDir, chspool.TableDirFacts, func(row chspool.DirFactRow) error {
			facts = append(facts, row)

			return nil
		}), ShouldBeNil)
		So(facts[0].GIDs, ShouldContain, uint32(7))
		So(facts[0].UIDs, ShouldContain, uint32(17))
	})

	Convey("D2.7 actual summarise command path writes and verifies every schema3 spool table", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		expectedTarget := &clickHouseSummariseTarget{
			cfg:             clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath:       summariseTestMountPath,
			mountpointsPath: mounts,
			modtime:         fixture.updatedAt,
			outputDir:       fixture.outputDir,
		}
		expectedManifest, err := newSummariseSpoolManifest(fixture.statsPath, expectedTarget)
		So(err, ShouldBeNil)

		// Build expected table bytes and hashes from an independent canonical
		// fixture spool before the command path under test writes its own spool.
		canonicalSpoolDir := filepath.Join(fixture.outputDir, "d2-schema3-canonical-spool")
		canonicalManifest, err := buildSummariseSpool(
			fixture.statsPath,
			canonicalSpoolDir,
			expectedManifest,
			expectedTarget,
			newSummariseDiagnostics(fixture.statsPath),
		)
		So(err, ShouldBeNil)

		var (
			manifest *chspool.Manifest
			spoolDir string
		)

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			gotSpoolDir string,
			gotManifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			spoolDir = gotSpoolDir
			manifest = gotManifest

			return perfreport.NewReport("clickhouse", gotSpoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(manifest, ShouldNotBeNil)
		So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
		So(chspool.VerifyManifest(spoolDir, manifest, expectedManifest), ShouldBeNil)

		expectedRows := d2Schema3ExpectedRowCounts()
		expectedTables := d2Schema3ExpectedTablesFromCanonicalManifest(canonicalManifest, expectedRows)
		assertD2Schema3ManifestTables(spoolDir, manifest, expectedRows, expectedTables)
		assertD2Schema3CanonicalCounts(manifest, expectedManifest, fixture.updatedAt, expectedRows)
	})

	Convey("D3.6 actual summarise command path loads and publishes schema3 spool rows", t, func() {
		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()
		cfg.QueryTimeout = 10 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{summariseTestMountPath}

		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickhouseDSN = cfg.DSN
		clickhouseDatabase = cfg.Database
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}

		orderingConn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(orderingConn.Close(), ShouldBeNil) }()

		orderingCtx, orderingCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer orderingCancel()

		sawReadinessBeforeActiveMount := false
		loadSummariseClickHouseSpool = func(
			ctx context.Context,
			cfg clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			recorder func(string, time.Duration),
		) (perfreport.Report, error) {
			wrappedRecorder := func(phase string, duration time.Duration) {
				recorder(phase, duration)

				if phase != "wrstat_active_virtual_ready" {
					return
				}

				activeSetID := d2ExpectedActiveSetID(*manifest, fixture.updatedAt)
				assertD3ReadinessVisibleBeforeActiveMount(orderingCtx, orderingConn, manifest, activeSetID)

				sawReadinessBeforeActiveMount = true
			}

			return clickhouse.LoadSummariseSpoolReport(ctx, cfg, spoolDir, manifest, wrappedRecorder)
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(sawReadinessBeforeActiveMount, ShouldBeTrue)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		report := readSummariseSpoolLoadReport(t, spoolDir)
		So(summariseSpoolReportHasOperation(report, "spool_load_total"), ShouldBeTrue)

		conn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		expectedRows := d2Schema3ExpectedRowCounts()
		activeSetID := d2ExpectedActiveSetID(*manifest, fixture.updatedAt)
		assertD3LoadedSchema3Counts(ctx, conn, manifest, activeSetID, expectedRows)
		assertD3LoadedSchema3Readiness(ctx, conn, manifest, fixture.updatedAt, activeSetID, expectedRows)
		assertD3ColdSchema3Probes(cfg)
	})
}

func writeBasedirsSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	root := statsdata.NewRoot(summariseTestMountPath, updatedAt.Unix())
	project := root.AddDirectory("project")
	file := project.AddFile("file.bam")
	file.Size = 50
	file.UID = 17
	file.GID = 7
	file.ATime = updatedAt.Unix()
	file.MTime = updatedAt.Unix()
	file.CTime = updatedAt.Unix()
	file.Inode = 99
	file.Nlink = 1

	var buf bytes.Buffer

	_, err := root.WriteTo(&buf)
	So(err, ShouldBeNil)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func d2Schema3ExpectedRowCounts() map[string]uint64 {
	return map[string]uint64{
		chspool.TableChildFilterAll:         68,
		chspool.TableDirFilterAll:           68,
		chspool.TableSchema3SnapshotSets:    1,
		chspool.TableActiveVirtualSummaries: 4,
		chspool.TableActiveVirtualFilterAll: 4,
		chspool.TableActiveVirtualChildren:  2,
		chspool.TableActiveVirtualSets:      1,
	}
}

func d2Schema3ExpectedTablesFromCanonicalManifest(
	canonicalManifest *chspool.Manifest,
	expectedRows map[string]uint64,
) map[string]chspool.TableManifest {
	out := make(map[string]chspool.TableManifest, len(expectedRows))

	for table, expectedRows := range expectedRows {
		tm, ok := canonicalManifest.Tables[table]
		So(ok, ShouldBeTrue)
		So(tm.Rows, ShouldEqual, expectedRows)
		So(tm.Bytes, ShouldBeGreaterThan, int64(0))
		So(tm.SHA256, ShouldNotBeBlank)

		out[table] = tm
	}

	return out
}

func assertD2Schema3ManifestTables(
	spoolDir string,
	manifest *chspool.Manifest,
	expectedRows map[string]uint64,
	expectedTables map[string]chspool.TableManifest,
) {
	var seen uint64

	for _, table := range chspool.TableOrder() {
		expectedRowCount, ok := expectedRows[table]
		if !ok {
			continue
		}

		seen++
		tm, ok := manifest.Tables[table]
		So(ok, ShouldBeTrue)
		So(tm, ShouldResemble, expectedTables[table])
		So(d2DecodedRowsForTable(spoolDir, table), ShouldEqual, expectedRowCount)
	}

	So(seen, ShouldEqual, uint64(len(expectedRows)))
}

func d2DecodedRowsForTable(spoolDir string, table string) uint64 {
	var rows uint64

	switch table {
	case chspool.TableChildFilterAll:
		So(chspool.DecodeRows[chspool.ChildFilterAllRow](spoolDir, table, func(chspool.ChildFilterAllRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableDirFilterAll:
		So(chspool.DecodeRows[chspool.DirFilterAllRow](spoolDir, table, func(chspool.DirFilterAllRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableSchema3SnapshotSets:
		So(chspool.DecodeRows[chspool.Schema3SnapshotSetRow](spoolDir, table, func(chspool.Schema3SnapshotSetRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualSummaries:
		So(chspool.DecodeRows[chspool.ActiveVirtualSummaryRow](spoolDir, table, func(chspool.ActiveVirtualSummaryRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualFilterAll:
		So(chspool.DecodeRows[chspool.ActiveVirtualFilterAllRow](
			spoolDir,
			table,
			func(chspool.ActiveVirtualFilterAllRow) error {
				rows++

				return nil
			},
		), ShouldBeNil)
	case chspool.TableActiveVirtualChildren:
		So(chspool.DecodeRows[chspool.ActiveVirtualChildRow](spoolDir, table, func(chspool.ActiveVirtualChildRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualSets:
		So(chspool.DecodeRows[chspool.ActiveVirtualSetRow](spoolDir, table, func(chspool.ActiveVirtualSetRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	}

	return rows
}

func assertD2Schema3CanonicalCounts(
	manifest *chspool.Manifest,
	expectedManifest chspool.Manifest,
	updatedAt time.Time,
	expectedRows map[string]uint64,
) {
	var snapshotSets []chspool.Schema3SnapshotSetRow

	So(chspool.DecodeRows[chspool.Schema3SnapshotSetRow](
		summariseClickHouseSpoolDir(expectedManifest.OutputDir),
		chspool.TableSchema3SnapshotSets,
		func(row chspool.Schema3SnapshotSetRow) error {
			snapshotSets = append(snapshotSets, row)

			return nil
		},
	), ShouldBeNil)
	So(snapshotSets, ShouldHaveLength, 1)

	snapshotCounts := d2Schema3ExpectedSnapshotCounts(expectedRows)
	So(snapshotSets[0].DirFactsRows, ShouldEqual, snapshotCounts.dirFactsRows)
	So(snapshotSets[0].ParentFactsRows, ShouldEqual, snapshotCounts.parentFactsRows)
	So(snapshotSets[0].ChildrenRows, ShouldEqual, snapshotCounts.childrenRows)
	So(snapshotSets[0].ChildFilterAllRows, ShouldEqual, snapshotCounts.childFilterAllRows)
	So(snapshotSets[0].DirFilterAllRows, ShouldEqual, snapshotCounts.dirFilterAllRows)
	So(snapshotSets[0].ManifestSHA256, ShouldEqual, d2ExpectedSchema3SnapshotDigest(expectedManifest, snapshotCounts))

	var activeSets []chspool.ActiveVirtualSetRow

	So(chspool.DecodeRows[chspool.ActiveVirtualSetRow](
		summariseClickHouseSpoolDir(expectedManifest.OutputDir),
		chspool.TableActiveVirtualSets,
		func(row chspool.ActiveVirtualSetRow) error {
			activeSets = append(activeSets, row)

			return nil
		},
	), ShouldBeNil)
	So(activeSets, ShouldHaveLength, 1)

	activeSetID := d2ExpectedActiveSetID(expectedManifest, updatedAt)
	So(activeSets[0].ActiveSetID, ShouldEqual, activeSetID)
	So(activeSets[0].MountsSHA256, ShouldEqual, activeSetID)
	So(activeSets[0].ActiveMountCount, ShouldEqual, uint64(1))
	So(activeSets[0].Ready, ShouldEqual, uint8(1))
	So(activeSets[0].SummaryRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualSummaries])
	So(activeSets[0].FilterRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualFilterAll])
	So(activeSets[0].ChildRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualChildren])
	So(activeSets[0].ManifestSHA256, ShouldEqual, d2ExpectedActiveVirtualDigest(activeSetID, expectedRows))
	So(manifest.Tables[chspool.TableChildFilterAll].Rows, ShouldEqual, expectedRows[chspool.TableChildFilterAll])
	So(manifest.Tables[chspool.TableDirFilterAll].Rows, ShouldEqual, expectedRows[chspool.TableDirFilterAll])
}

func assertD3ReadinessVisibleBeforeActiveMount(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	activeSetID string,
) {
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_schema3_snapshot_sets WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	), ShouldEqual, uint64(1))
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_active_virtual_sets WHERE active_set_id = ? AND ready = 1",
		activeSetID,
	), ShouldEqual, uint64(1))
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND snapshot_id = toUUID(?) AND event_type = 1",
		manifest.MountPath,
		manifest.SnapshotID,
	), ShouldEqual, uint64(0))
}

func readSummariseSpoolLoadReport(t *testing.T, spoolDir string) perfreport.Report {
	t.Helper()

	data, err := os.ReadFile(summariseSpoolLoadReportPath(spoolDir))
	So(err, ShouldBeNil)

	var report perfreport.Report
	So(json.Unmarshal(data, &report), ShouldBeNil)

	return report
}

func summariseSpoolReportHasOperation(report perfreport.Report, name string) bool {
	for _, op := range report.Operations {
		if op.Name == name {
			return true
		}
	}

	return false
}

func assertD3LoadedSchema3Counts(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	activeSetID string,
	expectedRows map[string]uint64,
) {
	for table, expected := range map[string]uint64{
		chspool.TableChildFilterAll:      expectedRows[chspool.TableChildFilterAll],
		chspool.TableDirFilterAll:        expectedRows[chspool.TableDirFilterAll],
		chspool.TableSchema3SnapshotSets: expectedRows[chspool.TableSchema3SnapshotSets],
	} {
		So(d3CountRows(
			ctx,
			conn,
			"SELECT count() FROM "+table+" WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			manifest.MountPath,
			manifest.SnapshotID,
		), ShouldEqual, expected)
		So(manifest.Tables[table].Rows, ShouldEqual, expected)
	}

	for table, expected := range map[string]uint64{
		chspool.TableActiveVirtualSummaries: expectedRows[chspool.TableActiveVirtualSummaries],
		chspool.TableActiveVirtualFilterAll: expectedRows[chspool.TableActiveVirtualFilterAll],
		chspool.TableActiveVirtualChildren:  expectedRows[chspool.TableActiveVirtualChildren],
		chspool.TableActiveVirtualSets:      expectedRows[chspool.TableActiveVirtualSets],
	} {
		So(d3CountRows(ctx, conn, "SELECT count() FROM "+table+" WHERE active_set_id = ?", activeSetID),
			ShouldEqual, expected)
		So(manifest.Tables[table].Rows, ShouldEqual, expected)
	}
}

func d3CountRows(ctx context.Context, conn ch.Conn, query string, args ...any) uint64 {
	row := conn.QueryRow(ctx, query, args...)

	var got uint64
	So(row.Scan(&got), ShouldBeNil)

	return got
}

func assertD3LoadedSchema3Readiness(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	updatedAt time.Time,
	activeSetID string,
	expectedRows map[string]uint64,
) {
	snapshotCounts := d2Schema3ExpectedSnapshotCounts(expectedRows)
	row := conn.QueryRow(
		ctx,
		"SELECT dir_facts_rows, parent_facts_rows, children_rows, child_filter_all_rows, "+
			"dir_filter_all_rows, manifest_sha256 FROM wrstat_schema3_snapshot_sets "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	)

	var (
		dirFactsRows       uint64
		parentFactsRows    uint64
		childrenRows       uint64
		childFilterAllRows uint64
		dirFilterAllRows   uint64
		manifestSHA256     string
	)
	So(row.Scan(
		&dirFactsRows,
		&parentFactsRows,
		&childrenRows,
		&childFilterAllRows,
		&dirFilterAllRows,
		&manifestSHA256,
	), ShouldBeNil)
	So(dirFactsRows, ShouldEqual, snapshotCounts.dirFactsRows)
	So(parentFactsRows, ShouldEqual, snapshotCounts.parentFactsRows)
	So(childrenRows, ShouldEqual, snapshotCounts.childrenRows)
	So(childFilterAllRows, ShouldEqual, snapshotCounts.childFilterAllRows)
	So(dirFilterAllRows, ShouldEqual, snapshotCounts.dirFilterAllRows)
	So(manifestSHA256, ShouldEqual, d2ExpectedSchema3SnapshotDigest(*manifest, snapshotCounts))

	row = conn.QueryRow(
		ctx,
		"SELECT ready, summary_rows, filter_rows, child_rows, manifest_sha256 "+
			"FROM wrstat_active_virtual_sets WHERE active_set_id = ?",
		activeSetID,
	)

	var (
		ready       uint8
		summaryRows uint64
		filterRows  uint64
		childRows   uint64
		activeSHA   string
	)
	So(row.Scan(&ready, &summaryRows, &filterRows, &childRows, &activeSHA), ShouldBeNil)
	So(ready, ShouldEqual, uint8(1))
	So(summaryRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualSummaries])
	So(filterRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualFilterAll])
	So(childRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualChildren])
	So(activeSHA, ShouldEqual, d2ExpectedActiveVirtualDigest(activeSetID, expectedRows))
	So(activeSetID, ShouldEqual, d2ExpectedActiveSetID(*manifest, updatedAt))
}

func d2Schema3ExpectedSnapshotCounts(expectedRows map[string]uint64) summariseSchema3SnapshotCounts {
	return summariseSchema3SnapshotCounts{
		dirFactsRows:       4,
		parentFactsRows:    4,
		childrenRows:       3,
		childFilterAllRows: expectedRows[chspool.TableChildFilterAll],
		dirFilterAllRows:   expectedRows[chspool.TableDirFilterAll],
	}
}

func d2ExpectedSchema3SnapshotDigest(
	expectedManifest chspool.Manifest,
	counts summariseSchema3SnapshotCounts,
) string {
	return d2SHA256Hex(fmt.Sprintf(
		"%s|%s|%d|%d|%d|%d|%d|%d",
		expectedManifest.MountPath,
		expectedManifest.SnapshotID,
		clickHouseSpoolSchema3Version,
		counts.dirFactsRows,
		counts.parentFactsRows,
		counts.childrenRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
	))
}

func d2ExpectedActiveVirtualDigest(activeSetID string, expectedRows map[string]uint64) string {
	return d2SHA256Hex(fmt.Sprintf(
		"%s|%d|%d|%d|%d",
		activeSetID,
		clickHouseSpoolSchema3Version,
		expectedRows[chspool.TableActiveVirtualSummaries],
		expectedRows[chspool.TableActiveVirtualFilterAll],
		expectedRows[chspool.TableActiveVirtualChildren],
	))
}

func d2SHA256Hex(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
}

func d2ExpectedActiveSetID(expectedManifest chspool.Manifest, updatedAt time.Time) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(expectedManifest.MountPath + "|" + expectedManifest.SnapshotID + "|" +
		updatedAt.UTC().Format(time.RFC3339Nano)))
	_, _ = hash.Write([]byte{0})

	return hex.EncodeToString(hash.Sum(nil))
}

func assertD3ColdSchema3Probes(cfg clickhouse.Config) {
	clickhouse.ResetTreeQueryCaches()
	clickhouse.ResetTreeQueryCacheStats(cfg)
	clickhouse.ResetSchema3FallbackRoutes()

	p, err := clickhouse.OpenProvider(cfg)

	So(err, ShouldBeNil)
	defer func() { So(p.Close(), ShouldBeNil) }()

	tree := p.Tree()
	So(tree, ShouldNotBeNil)

	filter := &db.Filter{
		GIDs: []uint32{7},
		UIDs: []uint32{17},
		FT:   db.DGUTAFileTypeBam,
		Age:  db.DGUTAgeAll,
	}
	projectDir := summariseTestMountPath + "project/"

	info, err := tree.DirInfo(projectDir, filter)
	So(err, ShouldBeNil)
	So(info.Current.Count, ShouldEqual, uint64(1))

	haveChildren := tree.DirsHaveChildren([]string{projectDir}, filter)
	So(haveChildren[projectDir], ShouldBeFalse)

	dcss, err := tree.Where(projectDir, filter, split.SplitsToSplitFn(0))
	So(err, ShouldBeNil)
	So(dcss, ShouldHaveLength, 1)

	stats := clickhouse.ReadTreeQueryCacheStats(cfg)
	So(stats.FactVectorReads, ShouldEqual, uint64(0))
	So(clickhouse.ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
}
