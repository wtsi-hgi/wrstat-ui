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

package chperf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const finalGateT283Digest = "sha256:t283"

const (
	finalGateE1BoltPerfCommand = "bolt-perf"
	finalGateE1OldWrstatUI     = "wrstat-ui-old"
	finalGateE1ProjectDir      = "/m/project/"
	finalGateE1ProjectTreeKey  = "project_tree_unused_1y"
	finalGateE1QueryCommand    = "query"
	finalGateE1WrstatUI        = "wrstat-ui"
	finalGateOtherType         = finalGateSelectedTreeTypes
)

const (
	finalGateTestDifferentDigest            = "sha256:different"
	finalGateTestE2ExpectedDigestInput      = "expected_result_digest"
	finalGateTestE2FactsVectorRowsInput     = "facts_vector_rows_read"
	finalGateTestE2ParentPacketTableInput   = "parent_packet_table"
	finalGateTestE2ProactiveWarmingInput    = "proactive_warming"
	finalGateTestE2ReadBytesCeilingInput    = "read_bytes_ceiling"
	finalGateTestE2ReadMarksCeilingInput    = "read_marks_ceiling"
	finalGateTestE2ReadRowsCeilingInput     = "read_rows_ceiling"
	finalGateTestE2ScenarioDirInfosBroad    = "dirinfos_broad_parent_packet"
	finalGateTestE2ScenarioDHCFiltered      = "dirshavechildren_filtered_parent_packet"
	finalGateTestE2ScenarioFirstRootWhere   = "first_root_where_splits_2"
	finalGateTestE2ScenarioHighFanoutBroad  = "high_fanout_first_click_broad"
	finalGateTestE2ScenarioHighFanoutFilter = "high_fanout_first_click_filtered"
	finalGateTestE2ScenarioNFSHeavyWhere    = "nfs_heavy_first_where_dir"
	finalGateTestE2ScenarioRESTFirst        = "rest_tree_first_requests"
	finalGateTestE2ScenarioRealWhere        = "real_first_where_dir"
	finalGateTestE2ScenarioSwitch           = "first_filter_switch_after_unfiltered_tree"
	finalGateTestE2ScenarioKey              = "e2_scenario"
)

type finalGateE1Assembly struct {
	options        FinalGateReportOptions
	manifestPath   string
	fixtureDigest  string
	boltReportPath string
	logPath        string
}

func finalGateE1AssemblyFixture(
	t *testing.T,
	root string,
	status string,
) finalGateE1Assembly {
	t.Helper()

	fixturePath := filepath.Join(root, "fixture-output.json")
	fixtureSummaries := finalGateE1FixtureSummaries()
	finalGateWriteCanonicalFixture(t, fixturePath, fixtureSummaries)
	fixtureDigest := dcssDigest(fixtureSummaries)

	manifestPath := filepath.Join(root, "fixture-manifest.json")
	finalGateWriteExpectedDigestManifest(t, manifestPath, finalGateE1ProjectTreeKey, fixtureDigest)

	clickHousePath := filepath.Join(root, "clickhouse-report.json")
	finalGateWritePerfReport(t, clickHousePath, finalGateE1ArtifactClickHouseReport())

	restPath := filepath.Join(root, "rest-report.json")
	finalGateWritePerfReport(t, restPath, finalGateE1ArtifactRESTReport())

	importPath := filepath.Join(root, "import-report.json")
	finalGateWritePerfReport(t, importPath, finalGateE1ImportReport("import_total", 0, "not_attempted"))

	spoolPath := filepath.Join(root, "spool-report.json")
	finalGateWritePerfReport(t, spoolPath,
		finalGateE1ImportReport("spool_load_total", 8192, finalGateComparisonStatusSuccess))

	boltPath := filepath.Join(root, "bolt-report.json")
	finalGateWritePerfReport(t, boltPath, finalGateE1BoltArtifactReport())

	logPath := filepath.Join(root, "comparison.log")
	finalGateWriteFile(t, logPath, "storage snapshot is unavailable\n")

	storagePath := filepath.Join(root, "bolt.db")
	finalGateWriteFile(t, storagePath, "bolt storage bytes")

	return finalGateE1Assembly{
		options: FinalGateReportOptions{
			FixtureDigests: []FinalGateFixtureDigestSpec{
				{
					Key:          finalGateE1ProjectTreeKey,
					ManifestPath: manifestPath,
					InputPath:    fixturePath,
				},
			},
			ClickHouseReportPaths: []string{clickHousePath},
			RESTReportPaths:       []string{restPath},
			ImportReportPaths:     []string{importPath},
			SpoolLoadReportPaths:  []string{spoolPath},
			Comparison: FinalGateComparisonSpec{
				Kind:                finalGateComparisonKindBolt,
				Status:              status,
				DatasetManifestPath: manifestPath,
				CommandArgv:         finalGateAssemblyCommand(root, status),
				OutputArtifactPath:  finalGateOutputArtifactPath(boltPath, status),
				StoragePath:         storagePath,
				LogPath:             logPath,
				AttemptedPath:       filepath.Join(root, "pre-clickhouse"),
				SourceRevision:      "oldrev",
				ToolVersion:         "prototype",
			},
		},
		manifestPath:   manifestPath,
		fixtureDigest:  fixtureDigest,
		boltReportPath: boltPath,
		logPath:        logPath,
	}
}

func TestE1FinalGateReportEvidence(t *testing.T) {
	Convey("E1.1 speed-gated reports require result count, digest, and correctness status", t, func() {
		report := finalGateE1Report()

		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeTrue)
		So(result.TimingEvaluated, ShouldBeTrue)

		report.ClickHouseReports[0].Operations[0].ResultCount = nil
		result = ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 correctness evidence").Detail,
			ShouldContainSubstring, "missing correctness fields")

		report = finalGateE1Report()
		delete(report.ClickHouseReports[0].Operations[0].Inputs, queryInputResultDigest)
		result = ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 correctness evidence").Detail,
			ShouldContainSubstring, "missing correctness fields")

		report = finalGateE1Report()
		delete(report.ClickHouseReports[0].Operations[0].Inputs, finalGateCorrectnessStatusInput)
		result = ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 correctness evidence").Detail,
			ShouldContainSubstring, "missing correctness fields")
	})

	Convey("E1.2 REST tree and where reports include REST counters, byte sizes, and percentiles", t, func() {
		report := finalGateE1Report()
		op := report.RESTReports[0].Operations[0]

		So(uint64SliceInput(op.Inputs, "query_count"), ShouldResemble, finalGateE1Counts(1))
		So(uint64SliceInput(op.Inputs, finalGateRESTInputCacheHits), ShouldResemble, finalGateE1Counts(2))
		So(uint64SliceInput(op.Inputs, finalGateRESTInputCacheMisses), ShouldResemble, finalGateE1Counts(3))
		So(uint64SliceInput(op.Inputs, finalGateRESTInputJSONBytes), ShouldResemble, finalGateE1Counts(4096))
		So(uint64SliceInput(op.Inputs, finalGateRESTInputGzipBytes), ShouldResemble, finalGateE1Counts(1024))
		So(op.P50MS, ShouldEqual, 3.0)
		So(op.P95MS, ShouldEqual, 5.0)
		So(op.P99MS, ShouldEqual, 5.0)

		delete(report.RESTReports[0].Operations[0].Inputs, finalGateRESTInputJSONBytes)
		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 REST evidence").Detail,
			ShouldContainSubstring, "REST evidence")
	})

	Convey("E1.3 ClickHouse query reports include read and result counters", t, func() {
		report := finalGateE1Report()
		op := report.ClickHouseReports[0].Operations[0]

		So(op.ReadRows, ShouldResemble, finalGateE1Counts(10))
		So(op.ReadBytes, ShouldResemble, finalGateE1Counts(2048))
		So(op.ReadMarks, ShouldResemble, finalGateE1Counts(4))
		So(op.ResultCount, ShouldResemble, finalGateE1Counts(7))
		So(op.ResultBytes, ShouldResemble, finalGateE1Counts(512))

		report.ClickHouseReports[0].Operations[0].ResultBytes = nil
		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 ClickHouse evidence").Detail,
			ShouldContainSubstring, "result bytes")
	})

	Convey("E1.4 direct import reports include table, resource, cleanup, and publish evidence", t, func() {
		report := finalGateE1Report()
		total := report.ImportReports[0].Operations[0]

		So(report.ImportReports[0].TableStats[tableFiles].Rows, ShouldEqual, uint64(17))
		So(report.ImportReports[0].TableStats[tableFiles].ActiveParts, ShouldEqual, uint64(2))
		So(uint64Input(total.Inputs, importInputUserCPUMS), ShouldEqual, uint64(11))
		So(uint64Input(total.Inputs, importInputSystemCPUMS), ShouldEqual, uint64(13))
		So(uint64Input(total.Inputs, importInputTotalCPUMS), ShouldEqual, uint64(24))
		So(uint64Input(total.Inputs, importInputPeakRSSBytes), ShouldEqual, uint64(4096))
		So(uint64Input(total.Inputs, finalGateE2InputSpoolBytes), ShouldEqual, uint64(0))
		So(uint64Input(total.Inputs, importInputPublishLatency), ShouldEqual, uint64(19))
		So(total.Inputs["retry_cleanup_result"], ShouldEqual, "not_attempted")

		delete(report.ImportReports[0].Operations[0].Inputs, importInputUserCPUMS)
		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 import evidence").Detail,
			ShouldContainSubstring, "import evidence")
	})

	Convey("E1.5 summarise spool-load reports include load, resource, cleanup, and publish evidence", t, func() {
		report := finalGateE1Report()
		total := report.SpoolLoadReports[0].Operations[0]

		So(uint64MapInput(total.Inputs, "loaded_table_rows")[tableFiles], ShouldEqual, uint64(17))
		So(uint64Input(total.Inputs, finalGateE2InputSpoolBytes), ShouldEqual, uint64(8192))
		So(total.Inputs["retry_cleanup_result"], ShouldEqual, finalGateComparisonStatusSuccess)

		delete(report.SpoolLoadReports[0].Operations[0].Inputs, "loaded_table_rows")
		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 spool-load evidence").Detail,
			ShouldContainSubstring, "spool-load evidence")
	})

	Convey("E1.6 final gate fails without Bolt or sidecar comparison evidence", t, func() {
		report := finalGateE1Report()
		report.Comparison = nil

		result := ValidateFinalGateReport(report)

		So(result.Passed, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 comparison presence").Detail,
			ShouldContainSubstring, "missing comparison evidence")
	})

	Convey("E1.7 successful same-subset comparison writes required Bolt evidence", t, func() {
		root := t.TempDir()
		assembly := finalGateE1AssemblyFixture(t, root, finalGateComparisonStatusSuccess)

		written, err := BuildFinalGateReport(assembly.options)

		So(err, ShouldBeNil)
		So(written.Comparison.Status, ShouldEqual, finalGateComparisonStatusSuccess)
		So(written.Comparison.Kind, ShouldEqual, finalGateComparisonKindBolt)
		So(written.Comparison.DatasetManifestPath, ShouldEqual, assembly.manifestPath)
		So(written.Comparison.DatasetManifestSHA256, ShouldEqual, finalGateTestFileDigest(t, assembly.manifestPath))
		So(written.Comparison.CommandArgv, ShouldResemble,
			[]string{finalGateE1WrstatUI, finalGateE1BoltPerfCommand, finalGateE1QueryCommand, root})
		So(written.Comparison.SourceRevision, ShouldEqual, "bolt-artefact-revision")
		So(written.Comparison.ToolVersion, ShouldEqual, "v1.2.3-e1")
		So(written.Comparison.OutputArtifactPath, ShouldEqual, assembly.boltReportPath)
		So(written.Comparison.LogPath, ShouldEqual, assembly.logPath)
		So(written.Comparison.StorageBytes, ShouldEqual, uint64(len("bolt storage bytes")))
		So(written.Comparison.P50MS, ShouldEqual, 3.0)
		So(written.Comparison.P95MS, ShouldEqual, 5.0)
		So(written.Comparison.P99MS, ShouldEqual, 5.0)
		So(written.Comparison.ResultDigest, ShouldEqual, finalGateE1Digest("comparison"))
		So(written.Comparison.FallbackCount, ShouldEqual, uint64(1))

		So(written.FixtureDigests[0].ExpectedDigest, ShouldEqual, assembly.fixtureDigest)
		So(written.FixtureDigests[0].RecomputedDigest, ShouldEqual, assembly.fixtureDigest)
		So(written.ClickHouseReports[0].Operations[0].Inputs[finalGateCorrectnessStatusInput],
			ShouldEqual, finalGateComparisonStatusSuccess)
		So(written.RESTReports[0].Operations[0].Inputs[finalGateCorrectnessStatusInput],
			ShouldEqual, finalGateComparisonStatusSuccess)
		So(ValidateFinalGateReport(written).Passed, ShouldBeTrue)
	})

	Convey("E1.8 infeasible comparison writes attempted route and evidence-backed reason", t, func() {
		root := t.TempDir()
		assembly := finalGateE1AssemblyFixture(t, root, finalGateComparisonInfeasible)

		written, err := BuildFinalGateReport(assembly.options)

		So(err, ShouldBeNil)
		So(written.Comparison.Status, ShouldEqual, "infeasible")
		So(written.Comparison.AttemptedPath, ShouldEqual, filepath.Join(root, "pre-clickhouse"))
		So(written.Comparison.CommandArgv, ShouldResemble, []string{finalGateE1OldWrstatUI, finalGateWhereCommandName})
		So(written.Comparison.DatasetManifestPath, ShouldEqual, assembly.manifestPath)
		So(written.Comparison.DatasetManifestSHA256, ShouldEqual, finalGateTestFileDigest(t, assembly.manifestPath))
		So(written.Comparison.SourceRevision, ShouldEqual, "oldrev")
		So(written.Comparison.ToolVersion, ShouldEqual, "prototype")
		So(written.Comparison.LogPath, ShouldEqual, assembly.logPath)
		So(written.Comparison.ErrorOutput, ShouldContainSubstring, "storage snapshot is unavailable")
		So(written.ClickHouseReports[0].Operations[0].Inputs[finalGateCorrectnessStatusInput],
			ShouldEqual, finalGateComparisonInfeasible)
	})

	Convey("E1.9 complete infeasible comparison blocks the gate instead of passing", t, func() {
		report := finalGateE1Report()
		report.Comparison = finalGateE1InfeasibleComparison()

		result := ValidateFinalGateReport(report)

		So(result.Passed, ShouldBeFalse)
		So(result.Blocked, ShouldBeTrue)
		So(result.Status, ShouldEqual, "blocked")
		So(finalGateReportTestCheck(result, "E1 infeasible comparison block").Blocked, ShouldBeTrue)
		So(finalGateReportTestCheck(result, "E1 infeasible comparison block").Detail,
			ShouldContainSubstring, "requires unavailable storage snapshot")
		So(finalGateReportTestCheck(result, "E1 infeasible comparison block").Detail,
			ShouldContainSubstring, "/tmp/infeasible.log")
	})

	Convey("E1.10 fixture digest validation fails missing or stale expected digests before timing", t, func() {
		report := finalGateE1Report()
		report.FixtureDigests[0].ExpectedDigest = ""

		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(result.TimingEvaluated, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 fixture digest validation").Detail,
			ShouldContainSubstring, "missing expected digest")

		report = finalGateE1Report()
		report.FixtureDigests[0].ExpectedDigest = finalGateE1Digest("stale")

		result = ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(result.TimingEvaluated, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 fixture digest validation").Detail,
			ShouldContainSubstring, "stale expected digest")
	})

	Convey("E1.11 fixture digest validation hashes canonical result output, not input bytes", t, func() {
		root := t.TempDir()
		key := finalGateE1ProjectTreeKey
		fixturePath := filepath.Join(root, "fixture-output.json")
		summaries := finalGateE1FixtureSummaries()
		finalGateWriteCanonicalFixture(t, fixturePath, summaries)
		rawDigest := finalGateTestFileDigest(t, fixturePath)
		canonicalDigest := dcssDigest(summaries)
		So(rawDigest, ShouldNotEqual, canonicalDigest)

		manifestPath := filepath.Join(root, "fixture-manifest.json")
		finalGateWriteExpectedDigestManifest(t, manifestPath, key, rawDigest)

		digest, err := finalGateBuildFixtureDigest(FinalGateFixtureDigestSpec{
			Key:          key,
			ManifestPath: manifestPath,
			InputPath:    fixturePath,
		})
		So(err, ShouldBeNil)
		So(digest.ExpectedDigest, ShouldEqual, rawDigest)
		So(digest.RecomputedDigest, ShouldEqual, canonicalDigest)

		report := finalGateE1Report()
		report.FixtureDigests = []FinalGateFixtureDigestEvidence{digest}
		result := ValidateFinalGateReport(report)
		So(result.Passed, ShouldBeFalse)
		So(result.TimingEvaluated, ShouldBeFalse)
		So(finalGateReportTestCheck(result, "E1 fixture digest validation").Detail,
			ShouldContainSubstring, "stale expected digest")

		finalGateWriteExpectedDigestManifest(t, manifestPath, key, canonicalDigest)
		digest, err = finalGateBuildFixtureDigest(FinalGateFixtureDigestSpec{
			Key:          key,
			ManifestPath: manifestPath,
			InputPath:    fixturePath,
		})
		So(err, ShouldBeNil)
		So(digest.ExpectedDigest, ShouldEqual, canonicalDigest)
		So(digest.RecomputedDigest, ShouldEqual, canonicalDigest)

		report.FixtureDigests = []FinalGateFixtureDigestEvidence{digest}
		So(ValidateFinalGateReport(report).Passed, ShouldBeTrue)
	})
}

func finalGateE1Report() FinalGateReport {
	return FinalGateReport{
		SchemaVersion: finalGateReportSchemaVersion,
		FixtureDigests: []FinalGateFixtureDigestEvidence{
			{
				Key:              finalGateE1ProjectTreeKey,
				ManifestPath:     "/fixtures/project/manifest.json",
				ExpectedDigest:   finalGateE1Digest("fixture"),
				RecomputedDigest: finalGateE1Digest("fixture"),
			},
		},
		ClickHouseReports: []perfreport.Report{finalGateE1ClickHouseReport()},
		RESTReports:       []perfreport.Report{finalGateE1RESTReport()},
		ImportReports:     []perfreport.Report{finalGateE1ImportReport("import_total", 0, "not_attempted")},
		SpoolLoadReports: []perfreport.Report{
			finalGateE1ImportReport("spool_load_total", 8192, finalGateComparisonStatusSuccess),
		},
		Comparison: finalGateE1SuccessComparison(),
	}
}

func finalGateE1Digest(label string) string {
	return "sha256:e1-" + label
}

func finalGateE1ClickHouseReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "/fixtures/mixed8", finalGateMinRepeats, 0)
	inputs := map[string]any{
		queryInputDirKey:                finalGateE1ProjectDir,
		queryInputResultDigest:          finalGateE1Digest("query"),
		finalGateCorrectnessStatusInput: finalGateComparisonStatusSuccess,
	}
	report.AddOperationWithFullCounters(
		queryOpTreeWhereName,
		inputs,
		[]float64{1, 2, 3, 4, 5},
		finalGateE1Counts(10),
		finalGateE1Counts(2048),
		finalGateE1Counts(4),
		finalGateE1Counts(128),
		finalGateE1Counts(512),
		finalGateE1Counts(7),
	)

	return report
}

func finalGateE1Counts(value uint64) []uint64 {
	return []uint64{value, value, value, value, value}
}

func finalGateE1RESTReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse_rest", "/fixtures/mixed8", finalGateMinRepeats, 0)
	inputs := finalGateRESTInputs(
		"/rest/v1/auth/tree",
		map[string]string{finalGateRESTParamPath: finalGateE1ProjectDir},
		finalGateE1Digest("rest"),
	)
	inputs[finalGateCorrectnessStatusInput] = finalGateComparisonStatusSuccess
	inputs["query_count"] = finalGateE1Counts(1)
	inputs[finalGateRESTInputCacheHits] = finalGateE1Counts(2)
	inputs[finalGateRESTInputCacheMisses] = finalGateE1Counts(3)
	inputs[finalGateRESTInputJSONBytes] = finalGateE1Counts(4096)
	inputs[finalGateRESTInputGzipBytes] = finalGateE1Counts(1024)
	report.AddOperationWithCounters(
		finalGateRESTOpTree,
		inputs,
		[]float64{1, 2, 3, 4, 5},
		nil,
		nil,
		nil,
		finalGateE1Counts(3),
	)

	whereInputs := finalGateRESTInputs(
		"/rest/v1/where",
		map[string]string{queryInputDirKey: finalGateE1ProjectDir},
		finalGateE1Digest("rest-where"),
	)
	whereInputs[finalGateCorrectnessStatusInput] = finalGateComparisonStatusSuccess
	whereInputs["query_count"] = finalGateE1Counts(1)
	whereInputs[finalGateRESTInputCacheHits] = finalGateE1Counts(2)
	whereInputs[finalGateRESTInputCacheMisses] = finalGateE1Counts(3)
	whereInputs[finalGateRESTInputJSONBytes] = finalGateE1Counts(4096)
	whereInputs[finalGateRESTInputGzipBytes] = finalGateE1Counts(1024)
	report.AddOperationWithCounters(
		finalGateRESTOpWhere,
		whereInputs,
		[]float64{1, 2, 3, 4, 5},
		nil,
		nil,
		nil,
		finalGateE1Counts(3),
	)

	return report
}

func finalGateE1ImportReport(operation string, spoolBytes uint64, cleanup string) perfreport.Report {
	report := perfreport.NewReport("clickhouse", "/fixtures/mixed8", 1, 0)
	report.MaxRSSBytes = 4096
	report.TableStats = map[string]perfreport.TableStats{
		tableFiles: {
			Rows:              17,
			ActiveParts:       2,
			CompressedBytes:   300,
			UncompressedBytes: 900,
		},
		tableDirSummary: {
			Rows:              5,
			ActiveParts:       1,
			CompressedBytes:   100,
			UncompressedBytes: 200,
		},
	}

	inputs := map[string]any{
		importInputUserCPUMS:       uint64(11),
		importInputSystemCPUMS:     uint64(13),
		importInputTotalCPUMS:      uint64(24),
		importInputPeakRSSBytes:    uint64(4096),
		finalGateE2InputSpoolBytes: spoolBytes,
		finalGateE2InputPartCounts: map[string]uint64{tableFiles: 2, tableDirSummary: 1},
		"retry_cleanup_result":     cleanup,
		importInputPublishLatency:  uint64(19),
	}
	if operation == "spool_load_total" {
		inputs["loaded_table_rows"] = map[string]uint64{tableFiles: 17, tableDirSummary: 5}
	}

	report.AddOperation(operation, inputs, []float64{42})

	return report
}

func finalGateE1SuccessComparison() *FinalGateComparisonEvidence {
	return &FinalGateComparisonEvidence{
		Kind:                  finalGateComparisonKindBolt,
		Status:                finalGateComparisonStatusSuccess,
		DatasetManifestPath:   "/fixtures/mixed8/manifest.json",
		DatasetManifestSHA256: finalGateE1Digest("manifest"),
		CommandArgv:           []string{finalGateE1WrstatUI, finalGateE1BoltPerfCommand, finalGateE1QueryCommand},
		SourceRevision:        "abcdef0",
		ToolVersion:           "v0.0.0-test",
		OutputArtifactPath:    "/tmp/bolt-report.json",
		LogPath:               "/tmp/bolt-report.log",
		StorageBytes:          12345,
		P50MS:                 1,
		P95MS:                 2,
		P99MS:                 3,
		ResultDigest:          finalGateE1Digest("comparison"),
		FallbackCount:         0,
	}
}

func finalGateReportTestCheck(result FinalGateReportResult, name string) FinalGateCheck {
	for _, check := range result.Checks {
		if check.Name == name {
			return check
		}
	}

	return FinalGateCheck{}
}

func finalGateTestFileDigest(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func finalGateE1InfeasibleComparison() *FinalGateComparisonEvidence {
	return &FinalGateComparisonEvidence{
		Kind:                  finalGateComparisonKindBolt,
		Status:                "infeasible",
		AttemptedPath:         "/src/pre-clickhouse",
		DatasetManifestPath:   "/fixtures/mixed8/manifest.json",
		DatasetManifestSHA256: finalGateE1Digest("manifest"),
		CommandArgv:           []string{finalGateE1OldWrstatUI, finalGateWhereCommandName},
		SourceRevision:        "oldrev",
		ToolVersion:           "prototype",
		LogPath:               "/tmp/infeasible.log",
		Reason:                "requires unavailable storage snapshot",
	}
}

func finalGateE1FixtureSummaries() db.DCSs {
	return db.DCSs{
		{
			Dir:   "/m/project/alpha/",
			Count: 3,
			Size:  30,
			UIDs:  []uint32{11},
			GIDs:  []uint32{7},
			FT:    db.DGUTAFileTypeBam,
			Age:   db.DGUTAgeA1Y,
		},
		{
			Dir:   "/m/project/beta/",
			Count: 2,
			Size:  20,
			UIDs:  []uint32{12},
			GIDs:  []uint32{8},
			FT:    db.DGUTAFileTypeCram,
			Age:   db.DGUTAgeA1Y,
		},
	}
}

func finalGateWriteCanonicalFixture(t *testing.T, path string, summaries db.DCSs) {
	t.Helper()

	data, err := json.MarshalIndent(map[string]db.DCSs{"results": summaries}, "", "  ")
	So(err, ShouldBeNil)
	So(os.WriteFile(path, data, 0o600), ShouldBeNil)
}

func TestE2ColdPerformanceGates(t *testing.T) {
	Convey("E3 sidecar fallback stays inactive when phase 6 E2 gates pass", t, func() {
		result := ValidateFinalGates(finalGateE2Evidence())

		So(result.Passed, ShouldBeTrue)
		So(result.SidecarFallback.Triggered, ShouldBeFalse)
		So(result.SidecarFallback.Status, ShouldEqual, "inactive")
		So(result.SidecarFallback.Reason, ShouldContainSubstring, "E2 cold gates passed")
		So(result.SidecarFallback.MissedChecks, ShouldHaveLength, 0)
	})

	Convey("E3 sidecar fallback triggers only from an E2 cold performance miss", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioNFSHeavyWhere, func(op *perfreport.Operation) {
			op.P95MS = 2000
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.SidecarFallback.Triggered, ShouldBeTrue)
		So(result.SidecarFallback.Status, ShouldEqual, "triggered")
		So(result.SidecarFallback.Reason, ShouldContainSubstring, "E2 cold gate miss")
		So(result.SidecarFallback.MissedChecks, ShouldHaveLength, 1)
		So(result.SidecarFallback.MissedChecks[0].Name, ShouldEqual, "E2 NFS-heavy first where")
	})

	Convey("E3 sidecar fallback stays inactive when an A-D gate fails alongside an E2 p95 miss", t, func() {
		evidence := finalGateE2Evidence()
		finalGateSetCandidateP95(&evidence, queryOpPermissionCheckName, 11.01)
		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioNFSHeavyWhere, func(op *perfreport.Operation) {
			op.P95MS = 2000
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "D2 permission/auth baseline").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 NFS-heavy first where").Passed, ShouldBeFalse)
		So(result.SidecarFallback.Triggered, ShouldBeFalse)
		So(result.SidecarFallback.Status, ShouldEqual, "inactive")
		So(result.SidecarFallback.MissedChecks, ShouldHaveLength, 0)
	})

	Convey("E3 sidecar fallback stays inactive when E2 read-volume evidence is missing", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, "dirshavechildren_broad_parent_packet", func(op *perfreport.Operation) {
			op.ReadRows = nil
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Detail,
			ShouldContainSubstring, "missing read-volume")
		So(result.SidecarFallback.Triggered, ShouldBeFalse)
		So(result.SidecarFallback.Status, ShouldEqual, "inactive")
		So(result.SidecarFallback.MissedChecks, ShouldHaveLength, 0)
	})

	Convey("E2.1 REST tree first requests are cold, correct, and under 500 ms", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 REST tree first requests").Passed, ShouldBeTrue)
		finalGateE2RESTTreeOp(evidence, finalGateNFSRootDir, func(op perfreport.Operation) {
			So(op.P95MS, ShouldBeLessThan, 500)
			So(op.Inputs[finalGateTestE2ProactiveWarmingInput], ShouldBeFalse)
			So(op.Inputs[queryInputResultDigest], ShouldEqual,
				op.Inputs[finalGateTestE2ExpectedDigestInput])
		})

		finalGateMutateE2OpRoot(
			&evidence,
			finalGateTestE2ScenarioRESTFirst,
			finalGateNFSRootDir,
			func(op *perfreport.Operation) {
				op.Inputs[queryInputResultDigest] = finalGateTestDifferentDigest
				op.P95MS = 900
			},
		)

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 REST tree first requests").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 REST tree first requests").Detail,
			ShouldContainSubstring, finalGateResultDigestMismatch)
	})

	Convey("E2.1 REST tree first requests reject infeasible correctness before timing", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2OpRoot(
			&evidence,
			finalGateTestE2ScenarioRESTFirst,
			finalGateNFSRootDir,
			func(op *perfreport.Operation) {
				op.Inputs[finalGateCorrectnessStatusInput] = finalGateComparisonInfeasible
				op.P95MS = 900
			},
		)

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 REST tree first requests").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 REST tree first requests").Detail,
			ShouldContainSubstring, "correctness equivalence status")
	})

	Convey("E2.2 high-fanout broad first click uses one current read and one parent packet", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, finalGateE2HighFanoutBroadCheckName).Passed, ShouldBeTrue)
		finalGateE2ScenarioOp(evidence, finalGateTestE2ScenarioHighFanoutBroad, func(op perfreport.Operation) {
			So(uint64Input(op.Inputs, "current_read_count"), ShouldEqual, uint64(1))
			So(uint64Input(op.Inputs, "parent_packet_read_count"), ShouldEqual, uint64(1))
			So(op.Inputs[finalGateTestE2ParentPacketTableInput], ShouldEqual, tableDirFacts)
			So(uint64Input(op.Inputs, "per_child_query_count"), ShouldEqual, uint64(0))
			So(uint64Input(op.Inputs, "subtree_scan_count"), ShouldEqual, uint64(0))
		})

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioHighFanoutBroad, func(op *perfreport.Operation) {
			op.Inputs["parent_packet_read_count"] = uint64(2)
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, finalGateE2HighFanoutBroadCheckName).Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, finalGateE2HighFanoutBroadCheckName).Detail,
			ShouldContainSubstring, "parent-packet")
	})

	Convey("E2.3 high-fanout filtered first click uses one child-filter packet", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, finalGateE2HighFanoutFilteredCheckName).Passed, ShouldBeTrue)
		finalGateE2ScenarioOp(evidence, finalGateTestE2ScenarioHighFanoutFilter, func(op perfreport.Operation) {
			So(op.Inputs[finalGateTestE2ParentPacketTableInput], ShouldEqual, "wrstat_child_filter_all")
			So(uint64Input(op.Inputs, "per_child_query_count"), ShouldEqual, uint64(0))
			So(uint64Input(op.Inputs, "subtree_scan_count"), ShouldEqual, uint64(0))
		})

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioHighFanoutFilter, func(op *perfreport.Operation) {
			op.Inputs[finalGateTestE2ParentPacketTableInput] = tableDirFacts
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, finalGateE2HighFanoutFilteredCheckName).Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, finalGateE2HighFanoutFilteredCheckName).Detail,
			ShouldContainSubstring, "wrstat_child_filter_all")
	})

	Convey("E2.4 focused high-fanout DirInfos broad uses one catalog parent-id packet", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 high-fanout DirInfos broad").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioDirInfosBroad, func(op *perfreport.Operation) {
			op.Inputs["per_child_query_count"] = uint64(1)
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 high-fanout DirInfos broad").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirInfos broad").Detail,
			ShouldContainSubstring, "per-child")
	})

	Convey("E2.5 focused high-fanout DirInfos filtered avoids facts vectors and subtree scans", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 high-fanout DirInfos filtered").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, "dirinfos_filtered_child_filter_packet", func(op *perfreport.Operation) {
			op.Inputs[finalGateTestE2FactsVectorRowsInput] = uint64(1)
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 high-fanout DirInfos filtered").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirInfos filtered").Detail,
			ShouldContainSubstring, "facts-vector")
	})

	Convey("E2.6 focused high-fanout DirsHaveChildren broad enforces bounded reads", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, "dirshavechildren_broad_parent_packet", func(op *perfreport.Operation) {
			op.Inputs[finalGateTestE2ReadRowsCeilingInput] = uint64(1_000_000)
			op.ReadRows = []uint64{60_000}
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Detail,
			ShouldContainSubstring, "read-volume")
	})

	Convey("E2.6 read-volume gates ignore permissive supplied ceilings", t, func() {
		cases := []struct {
			name   string
			mutate func(*perfreport.Operation)
		}{
			{
				name: finalGateE2ReadVolumeRowsName,
				mutate: func(op *perfreport.Operation) {
					op.Inputs[finalGateTestE2ReadRowsCeilingInput] = uint64(1_000_000)
					op.ReadRows = []uint64{60_000}
				},
			},
			{
				name: finalGateE2ReadVolumeBytesName,
				mutate: func(op *perfreport.Operation) {
					op.Inputs[finalGateTestE2ReadBytesCeilingInput] = uint64(1_000_000)
					op.ReadBytes = []uint64{2_000_000}
				},
			},
			{
				name: finalGateE2ReadVolumeMarksName,
				mutate: func(op *perfreport.Operation) {
					op.Inputs[finalGateTestE2ReadMarksCeilingInput] = uint64(100)
					op.ReadMarks = []uint64{8}
				},
			},
		}

		for _, tc := range cases {
			evidence := finalGateE2Evidence()
			finalGateMutateE2Op(&evidence, "dirshavechildren_broad_parent_packet", tc.mutate)

			result := ValidateFinalGates(evidence)
			check := finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad")
			So(check.Passed, ShouldBeFalse)
			So(check.Detail, ShouldContainSubstring, "read-volume "+tc.name+" exceeded ceiling")
		}
	})

	Convey("E2.6 read-volume gates require measured table stats", t, func() {
		evidence := finalGateE2Evidence()
		finalGateSetE2QueryTableStats(&evidence, tableDirFacts, perfreport.TableStats{})

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "missing read-volume table stats")

		evidence = finalGateE2Evidence()
		finalGateSetE2QueryTableStats(&evidence, tableDirFacts, perfreport.TableStats{
			ActiveParts:       1,
			CompressedBytes:   10,
			UncompressedBytes: 20,
		})

		result = ValidateFinalGates(evidence)

		check = finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "missing read-volume table stats")
	})

	Convey("E2.2 read-volume gates require current and readiness table stats", t, func() {
		cases := []struct {
			name      string
			scenario  string
			checkName string
			table     string
		}{
			{
				name:      "broad current",
				scenario:  finalGateTestE2ScenarioHighFanoutBroad,
				checkName: finalGateE2HighFanoutBroadCheckName,
				table:     tableDirSummary,
			},
			{
				name:      "broad readiness",
				scenario:  finalGateTestE2ScenarioHighFanoutBroad,
				checkName: finalGateE2HighFanoutBroadCheckName,
				table:     tableDirSummarySets,
			},
			{
				name:      "filtered current",
				scenario:  finalGateTestE2ScenarioHighFanoutFilter,
				checkName: finalGateE2HighFanoutFilteredCheckName,
				table:     finalGateE2DirFilterAllTable,
			},
			{
				name:      "filtered readiness",
				scenario:  finalGateTestE2ScenarioHighFanoutFilter,
				checkName: finalGateE2HighFanoutFilteredCheckName,
				table:     finalGateE2Schema3SnapshotSetsTable,
			},
		}

		for _, tc := range cases {
			evidence := finalGateE2Evidence()
			finalGateDeleteE2TableStats(&evidence, tc.table)
			finalGateMutateE2Op(&evidence, tc.scenario, func(op *perfreport.Operation) {
				op.ReadRows = []uint64{1}
				op.ReadBytes = []uint64{1}
				op.ReadMarks = []uint64{1}
			})

			result := ValidateFinalGates(evidence)
			check := finalGateTestCheck(result, tc.checkName)

			So(check.Passed, ShouldBeFalse)
			So(check.Detail, ShouldContainSubstring, "missing read-volume table stats for "+tc.table)
		}
	})

	Convey("E2.2 read-volume gates sum packet, current, and readiness ceilings", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioHighFanoutBroad, func(op *perfreport.Operation) {
			op.Inputs[finalGateTestE2ReadRowsCeilingInput] = uint64(1_000_000_000)
			op.Inputs[finalGateTestE2ReadBytesCeilingInput] = uint64(1_000_000_000)
			op.Inputs[finalGateTestE2ReadMarksCeilingInput] = uint64(1_000_000_000)
			op.ReadRows = []uint64{81_920}
			op.ReadBytes = []uint64{1_000_000}
			op.ReadMarks = []uint64{10}
		})

		result := ValidateFinalGates(evidence)
		check := finalGateTestCheck(result, finalGateE2HighFanoutBroadCheckName)
		So(check.Passed, ShouldBeTrue)

		cases := []struct {
			name   string
			mutate func(*perfreport.Operation)
		}{
			{
				name: finalGateE2ReadVolumeRowsName,
				mutate: func(op *perfreport.Operation) {
					op.ReadRows = []uint64{1_000_000_000}
				},
			},
			{
				name: finalGateE2ReadVolumeBytesName,
				mutate: func(op *perfreport.Operation) {
					op.ReadBytes = []uint64{1_000_000_000}
				},
			},
			{
				name: finalGateE2ReadVolumeMarksName,
				mutate: func(op *perfreport.Operation) {
					op.ReadMarks = []uint64{1_000_000}
				},
			},
		}

		for _, tc := range cases {
			evidence = finalGateE2Evidence()
			finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioHighFanoutBroad, tc.mutate)

			result = ValidateFinalGates(evidence)
			check = finalGateTestCheck(result, finalGateE2HighFanoutBroadCheckName)
			So(check.Passed, ShouldBeFalse)
			So(check.Detail, ShouldContainSubstring, "read-volume "+tc.name+" exceeded ceiling")
		}
	})

	Convey("E2.6 computed read-volume ceilings pass valid measured reads", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, "dirshavechildren_broad_parent_packet", func(op *perfreport.Operation) {
			delete(op.Inputs, finalGateTestE2ReadRowsCeilingInput)
			delete(op.Inputs, finalGateTestE2ReadBytesCeilingInput)
			delete(op.Inputs, finalGateTestE2ReadMarksCeilingInput)
		})

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren broad").Passed, ShouldBeTrue)
	})

	Convey("E2.7 focused high-fanout DirsHaveChildren filtered returns only expected true children", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren filtered").Passed, ShouldBeTrue)
		finalGateE2ScenarioOp(evidence, finalGateTestE2ScenarioDHCFiltered, func(op perfreport.Operation) {
			So(stringSliceInput(op.Inputs, "actual_true_children"),
				ShouldResemble, stringSliceInput(op.Inputs, "expected_true_children"))
			So(uint64Input(op.Inputs, finalGateTestE2FactsVectorRowsInput), ShouldEqual, uint64(0))
		})

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioDHCFiltered, func(op *perfreport.Operation) {
			op.Inputs["actual_true_children"] = []string{"/unexpected/"}
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren filtered").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 high-fanout DirsHaveChildren filtered").Detail,
			ShouldContainSubstring, "expected child")
	})

	Convey("E2.8 first root where with splits 2 matches current facts under 1 s", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 first root where splits 2").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioFirstRootWhere, func(op *perfreport.Operation) {
			op.Inputs[queryInputSplitsKey] = uint64(3)
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 first root where splits 2").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first root where splits 2").Detail,
			ShouldContainSubstring, "splits 2")
	})

	Convey("E2.9 first filter switch does not reuse broad packet cache entries", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 first filter switch").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioSwitch, func(op *perfreport.Operation) {
			op.Inputs["used_broad_packet_cache"] = true
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 first filter switch").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first filter switch").Detail,
			ShouldContainSubstring, "broad packet cache")
	})

	Convey("E2.9 first filter switch requires cold cache scope and scoped parent-packet hits", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioSwitch, func(op *perfreport.Operation) {
			delete(op.Inputs, queryInputCacheScope)
		})

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 first filter switch").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first filter switch").Detail, ShouldContainSubstring, "cache scope")

		evidence = finalGateE2Evidence()
		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioSwitch, func(op *perfreport.Operation) {
			op.Inputs[queryInputCacheHitKeysKey] = []string{
				"parent_packet:path=" + finalGateHighFanoutParentDir + ";filter=other;active_set_id=e2;query_version=1",
				"active_prefix_summary:path=/prior/;filter=ft:1;active_set_id=old;query_version=1",
			}
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 first filter switch").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first filter switch").Detail, ShouldContainSubstring, "cache hit")
	})

	Convey("E2.9 rejects root parent-packet hit keys scoped to another parent", t, func() {
		evidence := finalGateE2Evidence()
		finalGateMutateE2OpRoot(&evidence, finalGateTestE2ScenarioFirstRootWhere, "/", func(op *perfreport.Operation) {
			op.Inputs[queryInputCacheHitKeysKey] = []string{
				"parent_packet:parent_dir=/unrelated/;filter=other;active_set_id=e2;query_version=1",
			}
		})

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 first root where splits 2").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first root where splits 2").Detail,
			ShouldContainSubstring, "cache hit")
	})

	Convey("E2.9 rejects parent-packet hit keys that only prefix-match the operation root", t, func() {
		evidence := finalGateE2Evidence()
		root := strings.TrimSuffix(finalGateHighFanoutParentDir, "/")

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioSwitch, func(op *perfreport.Operation) {
			op.Inputs[queryInputDirKey] = root
			op.Inputs[queryInputCacheHitKeysKey] = []string{
				"parent_packet:parent_dir=" + root + "child/;filter=other;active_set_id=e2;query_version=1",
			}
		})

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 first filter switch").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 first filter switch").Detail,
			ShouldContainSubstring, "cache hit")
	})

	Convey("E2.10 real first where --dir root and high-fanout match current facts", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 real first where dirs").Passed, ShouldBeTrue)

		finalGateMutateE2OpRoot(&evidence, finalGateTestE2ScenarioRealWhere, finalGateHighFanoutParentDir,
			func(op *perfreport.Operation) {
				op.Inputs[queryInputResultDigest] = "sha256:wrong-high-fanout"
			})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 real first where dirs").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 real first where dirs").Detail,
			ShouldContainSubstring, finalGateResultDigestMismatch)
	})

	Convey("E2.11 NFS-heavy first where --dir matches current facts under 2 s", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 NFS-heavy first where").Passed, ShouldBeTrue)

		finalGateMutateE2Op(&evidence, finalGateTestE2ScenarioNFSHeavyWhere, func(op *perfreport.Operation) {
			op.P95MS = 2000
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 NFS-heavy first where").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 NFS-heavy first where").Detail, ShouldContainSubstring, "p95")
	})

	Convey("E2.12 import and spool-load budgets are computed from measured reports", t, func() {
		evidence := finalGateE2Evidence()

		result := ValidateFinalGates(evidence)

		So(finalGateTestCheck(result, "E2 import and spool budgets").Passed, ShouldBeTrue)
		finalGateE2ImportBudgetOp(evidence.ImportReports[0], "import_total", func(op perfreport.Operation) {
			So(op.Inputs["budget_source"], ShouldBeNil)
			So(op.Inputs["budget_measurement_count"], ShouldBeNil)
			So(op.Inputs["wall_time_budget_ms"], ShouldBeNil)
		})

		finalGateMutateImportBudgetOp(&evidence.ImportReports[0], "import_total", func(op *perfreport.Operation) {
			op.Inputs["budget_source"] = "hardcoded_before_measurement"
		})

		result = ValidateFinalGates(evidence)
		So(finalGateTestCheck(result, "E2 import and spool budgets").Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E2 import and spool budgets").Detail,
			ShouldContainSubstring, "computed from measurements")
	})

	Convey("E2.12 rejects import budgets without measured resource evidence", t, func() {
		for _, key := range []string{importInputTotalCPUMS, importInputPeakRSSBytes, finalGateE2InputSpoolBytes} {
			evidence := finalGateE2Evidence()
			finalGateMutateImportBudgetOp(&evidence.ImportReports[0], "import_total", func(op *perfreport.Operation) {
				delete(op.Inputs, key)
			})

			result := ValidateFinalGates(evidence)
			check := finalGateTestCheck(result, "E2 import and spool budgets")
			So(check.Passed, ShouldBeFalse)
			So(check.Detail, ShouldContainSubstring, "missing measured "+key)
		}

		evidence := finalGateE2Evidence()
		finalGateMutateImportBudgetOp(&evidence.ImportReports[0], "import_total", func(op *perfreport.Operation) {
			op.Inputs[finalGateE2InputPartCounts] = map[string]uint64{}
		})

		result := ValidateFinalGates(evidence)
		check := finalGateTestCheck(result, "E2 import and spool budgets")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "missing measured "+finalGateE2InputPartCounts)
	})

	Convey("E2.12 rejects spool-load budgets without measured resource evidence", t, func() {
		for _, key := range []string{importInputTotalCPUMS, importInputPeakRSSBytes, finalGateE2InputSpoolBytes} {
			report := finalGateE1ImportReport("spool_load_total", 8192, finalGateComparisonStatusSuccess)
			finalGateMutateImportBudgetOp(&report, "spool_load_total", func(op *perfreport.Operation) {
				delete(op.Inputs, key)
			})

			reason := finalGateE2BudgetReportsFailure([]perfreport.Report{report}, "spool_load_total")
			So(reason, ShouldContainSubstring, "missing measured "+key)
		}

		report := finalGateE1ImportReport("spool_load_total", 8192, finalGateComparisonStatusSuccess)
		finalGateMutateImportBudgetOp(&report, "spool_load_total", func(op *perfreport.Operation) {
			op.Inputs[finalGateE2InputPartCounts] = map[string]uint64{}
		})

		reason := finalGateE2BudgetReportsFailure([]perfreport.Report{report}, "spool_load_total")
		So(reason, ShouldContainSubstring, "missing measured "+finalGateE2InputPartCounts)
	})
}

func finalGateE2Evidence() FinalGateEvidence {
	return finalGateTestEvidence(false, false)
}

func TestValidateFinalGates(t *testing.T) {
	Convey("ValidateFinalGates covers all 41 final acceptance gates", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(result.TimingEvaluated, ShouldBeTrue)
		So(result.Checks, ShouldHaveLength, 41)

		for i, check := range result.Checks {
			So(check.ID, ShouldEqual, i+1)
			So(check.Passed, ShouldBeTrue)
		}
	})

	Convey("ValidateFinalGates fails the import gate without repeated import artefacts", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ImportReports = evidence.ImportReports[:4]

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
		So(result.Checks[0].Detail, ShouldContainSubstring, "at least 5")
	})

	Convey("ValidateFinalGates validates E1 before speed gates", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.FinalGateReport.FixtureDigests[0].ExpectedDigest = ""

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Status, ShouldEqual, finalGateReportStatusFailed)
		So(result.TimingEvaluated, ShouldBeFalse)
		So(result.Checks, ShouldHaveLength, 1)
		So(result.Checks[0].Name, ShouldEqual, "E1 fixture digest validation")
		So(result.Checks[0].Detail, ShouldContainSubstring, "missing expected digest")
	})

	Convey("ValidateFinalGates blocks on infeasible E1 comparison before speed gates", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.FinalGateReport.Comparison = finalGateE1InfeasibleComparison()
		finalGateSetReportCorrectnessStatus(evidence.FinalGateReport, finalGateComparisonInfeasible)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Blocked, ShouldBeTrue)
		So(result.Status, ShouldEqual, finalGateReportStatusBlocked)
		So(result.TimingEvaluated, ShouldBeFalse)
		So(finalGateTestCheck(result, "E1 infeasible comparison block").Blocked, ShouldBeTrue)
		So(finalGateTestCheck(result, "E2 direct import").Name, ShouldEqual, "")
	})

	Convey("ValidateFinalGates fails result-equivalence metrics when counts vary", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.QueryReports[0].Operations[0].ResultCount = []uint64{1, 2, 1, 1, 1}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[1].Passed, ShouldBeFalse)
		So(result.Checks[1].Detail, ShouldContainSubstring, "result-count")
	})

	Convey("ValidateFinalGates requires five measured visible-child repetitions", t, func() {
		evidence := finalGateTestEvidence(false, false)
		opIndex := finalGateFindQueryOpIndex(evidence.QueryReports[0], queryOpTreeDiskTreeVisibleChildName)
		evidence.QueryReports[0].Operations[opIndex].DurationsMS = []float64{0.01, 0.01, 0.01, 0.01}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[11].Passed, ShouldBeFalse)
		So(result.Checks[11].Detail, ShouldContainSubstring, "visible-child")
	})

	Convey("ValidateFinalGates requires table evidence when the AgeAll index is selected", t, func() {
		evidence := finalGateTestEvidence(true, false)
		for i := range evidence.ImportReports {
			delete(evidence.ImportReports[i].TableStats, tableDirFilterAgeAll)
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[14].Passed, ShouldBeFalse)
		So(result.Checks[14].Detail, ShouldContainSubstring, "AgeAll")
	})

	Convey("ValidateFinalGates requires table evidence when the virtual cache is selected", t, func() {
		evidence := finalGateTestEvidence(false, true)
		for i := range evidence.ImportReports {
			delete(evidence.ImportReports[i].TableStats, tableTreeDGUTA)
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[13].Passed, ShouldBeFalse)
		So(result.Checks[13].Detail, ShouldContainSubstring, "virtual-summary")
	})

	Convey("ValidateFinalGates fails when a required root is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.RequiredQueryRoots = append(evidence.RequiredQueryRoots, "/missing/")

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[7].Passed, ShouldBeFalse)
		So(result.Checks[7].Detail, ShouldContainSubstring, "DirInfo")
	})

	Convey("ValidateFinalGates fails when a later supplied report has a bad required operation", t, func() {
		evidence := finalGateTestEvidence(false, false)
		badReport := finalGateQueryReport()
		opIndex := finalGateFindQueryOpIndex(badReport, queryOpDirInfoBroadName)
		badReport.Operations[opIndex].DurationsMS = []float64{2, 2, 2, 2, 2}
		badReport.Operations[opIndex].P50MS = 2
		badReport.Operations[opIndex].P95MS = 2
		badReport.Operations[opIndex].P99MS = 2
		evidence.QueryReports = append(evidence.QueryReports, badReport)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[7].Passed, ShouldBeFalse)
		So(result.Checks[7].Detail, ShouldContainSubstring, "DirInfo")
	})

	Convey("ValidateFinalGates fails when paired equivalence evidence is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ResultEquivalence = nil

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[1].Passed, ShouldBeFalse)
		So(result.Checks[1].Detail, ShouldContainSubstring, "equivalence")
	})

	Convey("ValidateFinalGates fails when paired equivalence digests differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ResultEquivalence[0].CandidateDigest = "different"

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[1].Passed, ShouldBeFalse)
		So(result.Checks[1].Detail, ShouldContainSubstring, "equivalence")
	})

	Convey("ValidateFinalGates enforces the one-row projection set baseline", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ImportReports[0].TableStats[tableDirSummarySets] = finalGateTableStats(2)

		result := ValidateFinalGates(evidence)

		rows, ok := finalGateNonOptionalRowBaseline(tableDirSummarySets)
		So(ok, ShouldBeTrue)
		So(rows, ShouldEqual, 1)
		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
	})

	Convey("ValidateFinalGates enforces the per-cap directory facts row cap", t, func() {
		evidence := finalGateTestEvidence(false, false)
		maxRows, ok := finalGateNonOptionalRowCap(
			tableDirSummary,
			finalGateImportCap100K,
			uint64(len(finalGateRequiredImportRoots())),
		)
		So(ok, ShouldBeTrue)

		evidence.ImportReports[0].TableStats[tableDirSummary] = finalGateTableStats(maxRows + 1)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
	})

	Convey("D2.6 final gate passes matching permission/auth baseline and candidate evidence", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(result.Checks[15].Passed, ShouldBeTrue)
		So(result.Checks[15].Detail, ShouldContainSubstring, "permission/auth")
	})

	Convey("D2.6 final gate accepts matching permission/auth reports produced by runSuite", t, func() {
		runSuiteReport, err := finalGateRunSuiteD2AuthReport()
		So(err, ShouldBeNil)

		evidence := finalGateTestEvidence(false, false)
		finalGateReplaceD2AuthOps(&evidence.BaselineQueryReports[0], runSuiteReport)
		finalGateReplaceD2AuthOps(&evidence.QueryReports[0], runSuiteReport)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeTrue)
		So(result.Checks[15].Passed, ShouldBeTrue)

		authTree, ok := firstOperation(runSuiteReport, queryOpAuthTreeName, nil)
		So(ok, ShouldBeTrue)
		So(authTree.Inputs[queryInputSurfaceKey], ShouldEqual, queryInputSurfaceInProcessEquivalent)
		So(finalGateInputPresent(authTree, finalGateRESTInputJSONBytes), ShouldBeFalse)
		So(finalGateInputPresent(authTree, finalGateRESTInputCacheHits), ShouldBeFalse)
	})

	Convey("D1.6 final gate accepts matching Info count vectors", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetReportInfoCountVector(&evidence.BaselineQueryReports[0], []uint64{3, 6, 1, 2})
		finalGateSetReportInfoCountVector(&evidence.QueryReports[0], []uint64{3, 6, 1, 2})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeTrue)
		So(result.Checks[15].Passed, ShouldBeTrue)
	})

	Convey("D1.6 final gate fails mismatched Info count vectors", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetReportInfoCountVector(&evidence.BaselineQueryReports[0], []uint64{3, 6, 1, 2})
		finalGateSetReportInfoCountVector(&evidence.QueryReports[0], []uint64{3, 7, 1, 2})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, finalGateResultCountMismatch)
	})

	Convey("D2.6 final gate fails when candidate p95 is more than 10% slower", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateP95(&evidence, queryOpPermissionCheckName, 11.01)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "p95")
	})

	Convey("D2.6 final gate fails when result digests differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(
			&evidence,
			queryOpAuthWhereRestrictedName,
			queryInputResultDigest,
			finalGateTestDifferentDigest,
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "digest")
	})

	Convey("D2.6 final gate fails when NoAuth flags differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpAuthTreeName, queryInputNoAuthFlagsKey, map[string]bool{
			queryOpTestRootDir:   false,
			queryOpTestChildADir: true,
			queryOpTestChildBDir: true,
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "NoAuth")
	})

	Convey("D2.6 final gate fails when status codes differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpAuthTreeName, "status_codes", []uint64{200, 200, 500, 200, 200})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "status code")
	})

	Convey("D2.6 final gate fails when JSON bytes differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(
			&evidence,
			queryOpAuthWhereRestrictedName,
			"json_bytes",
			[]uint64{8192, 8192, 8193, 8192, 8192},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "JSON byte")
	})

	Convey("D2.6 final gate fails when REST array evidence is incomplete", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateDeleteReportInput(evidence.BaselineQueryReports, queryOpAuthTreeName, finalGateRESTInputJSONBytes)
		finalGateDeleteReportInput(evidence.QueryReports, queryOpAuthTreeName, finalGateRESTInputJSONBytes)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "JSON byte")
	})

	Convey("D2.6 final gate fails when gzip bytes differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpNoAuthWhereName, "gzip_bytes", []uint64{2048, 2048, 2049, 2048, 2048})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "gzip byte")
	})

	Convey("D2.6 final gate fails when cache hit counters differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpAuthTreeName, "cache_hits", []uint64{3, 3, 4, 3, 3})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "cache counter")
	})

	Convey("D2.6 final gate fails when cache miss counters differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpAuthWhereRestrictedName, "cache_misses", []uint64{1, 1, 2, 1, 1})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, "cache counter")
	})

	Convey("E1.5 final gate passes matching Bolt query counts and digests", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(result.Checks[16].Passed, ShouldBeTrue)
		So(result.Checks[16].Detail, ShouldContainSubstring, "Bolt query")
	})

	Convey("E1.5 final gate fails when Bolt result counts differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateBoltOp(&evidence, queryOpTreeWhereName, func(op *perfreport.Operation) {
			op.ResultCount = []uint64{7, 7, 8, 7, 7}
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[16].Passed, ShouldBeFalse)
		So(result.Checks[16].Detail, ShouldContainSubstring, "result-count")
	})

	Convey("E1.5 final gate fails when Bolt result digests differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateBoltOp(&evidence, queryOpTreeDirInfoName, func(op *perfreport.Operation) {
			op.Inputs[queryInputResultDigest] = finalGateTestDifferentDigest
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[16].Passed, ShouldBeFalse)
		So(result.Checks[16].Detail, ShouldContainSubstring, "digest")
	})

	Convey("E2.4 failed t283 filtered REST order anomaly blocks filtered gates", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.T283FilteredRESTOrder = &FinalGateT283FilteredRESTOrderEvidence{
			DirectDigest:      "sha256:direct",
			WarmedDigest:      "sha256:warmed",
			DirectResultCount: 2,
			WarmedResultCount: 87,
			WarmedCacheHitKeys: []string{
				queryTestE2CacheHitKey,
			},
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[3].Passed, ShouldBeFalse)
		So(result.Checks[3].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
		So(result.Checks[5].Passed, ShouldBeFalse)
		So(result.Checks[5].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
	})

	Convey("E2.4 missing t283 filtered REST order evidence blocks filtered gates", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.T283FilteredRESTOrder = nil

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[3].Passed, ShouldBeFalse)
		So(result.Checks[3].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
		So(result.Checks[5].Passed, ShouldBeFalse)
		So(result.Checks[5].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
	})

	Convey("E2.4 final gate derives t283 REST order evidence from REST reports", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.T283FilteredRESTOrder = nil
		evidence.QueryReports = append(
			evidence.QueryReports,
			finalGateT283RESTReport("direct", 0, nil),
			finalGateT283RESTReport("warmed", 1, []string{queryTestE2CacheHitKey}),
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeTrue)
		So(result.Checks[3].Passed, ShouldBeTrue)
		So(result.Checks[5].Passed, ShouldBeTrue)
	})

	Convey("E2.4 enabled cache counters require warmed scoped hit keys", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.T283FilteredRESTOrder = nil
		evidence.QueryReports = append(
			evidence.QueryReports,
			finalGateT283RESTReport("direct", 0, nil),
			finalGateT283RESTReport("warmed", 1, nil),
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[3].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
		So(result.Checks[5].Detail, ShouldContainSubstring, "t283_filtered_rest_order_anomaly")
	})
}

func TestE3FinalPerformanceGates(t *testing.T) {
	Convey("E3 final gates pass browser, Bolt, digest, table stats, baseline, and info/auth checks", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 root refresh").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 first mount clicks").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 root filter switch").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 root where vs Bolt").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 facts digest equivalence").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 table stats row amplification evidence").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 non-targeted baseline regression").Passed, ShouldBeTrue)
		So(result.Checks[15].Passed, ShouldBeTrue)
	})

	Convey("E3 import coverage fails when the 250k capped subset is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ImportReports = evidence.ImportReports[:finalGateMinRepeats]

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
		So(result.Checks[0].Detail, ShouldContainSubstring, "250k")
	})

	Convey("E3 import coverage fails when a 250k report exceeds its row cap", t, func() {
		evidence := finalGateTestEvidence(false, false)
		report := &evidence.ImportReports[finalGateMinRepeats]
		opIndex := finalGateFindQueryOpIndex(*report, "import_file_total")
		report.Operations[opIndex].Inputs["lines"] = uint64(finalGateImportCap250K + 1)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
		So(result.Checks[0].Detail, ShouldContainSubstring, "250k")
	})

	Convey("E3.1 final gate fails when first root refresh p95 reaches 1000 ms", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			"rest_tree",
			finalGateRESTPathPredicate("/"),
			func(op *perfreport.Operation) {
				op.P95MS = 1000
				op.P99MS = 1000
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 root refresh").Passed, ShouldBeFalse)
	})

	Convey("E3.2 final gate fails when lustre or nfs first clicks exceed 500 ms", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			"rest_tree",
			finalGateRESTPathPredicate(finalGateNFSRootDir),
			func(op *perfreport.Operation) {
				op.P95MS = 501
				op.P99MS = 501
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 first mount clicks").Passed, ShouldBeFalse)
	})

	Convey("E3.2 final gate fails when selected t283 mount-root first click is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateRemoveCandidateOpMatching(&evidence, "rest_tree", finalGateRESTPathPredicate(finalGateT283Dir))

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 first mount clicks").Passed, ShouldBeFalse)
	})

	Convey("E3.2 final gate fails when selected t283 mount-root first click is slow", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			"rest_tree",
			finalGateRESTPathPredicate(finalGateT283Dir),
			func(op *perfreport.Operation) {
				op.P95MS = 501
				op.P99MS = 501
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 first mount clicks").Passed, ShouldBeFalse)
	})

	Convey("E3.3 final gate fails when first root filter switch exceeds 1000 ms", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			"rest_where",
			finalGateRESTDirPredicate("/"),
			func(op *perfreport.Operation) {
				op.P95MS = 1001
				op.P99MS = 1001
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 root filter switch").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when filtered t283 ClickHouse where p95 is over the Bolt tolerance", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(&evidence, queryOpTreeWhereColdName,
			finalGateExactDirPredicate(finalGateT283Dir), func(op *perfreport.Operation) {
				op.P95MS = 86.389
				op.P99MS = 86.389
			})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when t283 cli_where evidence is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateRemoveCandidateOpMatching(&evidence, finalGateRESTOpCLIWhere, finalGateRESTDirPredicate(finalGateT283Dir))

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when t283 cli_where evidence is slow", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			finalGateRESTOpCLIWhere,
			finalGateRESTDirPredicate(finalGateT283Dir),
			func(op *perfreport.Operation) {
				op.P95MS = 86.389
				op.P99MS = 86.389
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when t283 rest_where evidence is missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateRemoveCandidateOpMatching(&evidence, finalGateRESTOpWhere, finalGateRESTDirPredicate(finalGateT283Dir))

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when t283 rest_where evidence is slow", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			finalGateRESTOpWhere,
			finalGateRESTDirPredicate(finalGateT283Dir),
			func(op *perfreport.Operation) {
				op.P95MS = 86.389
				op.P99MS = 86.389
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate fails when t283 rest_where status or result evidence is invalid", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(
			&evidence,
			finalGateRESTOpWhere,
			finalGateRESTDirPredicate(finalGateT283Dir),
			func(op *perfreport.Operation) {
				op.Inputs[finalGateRESTInputStatusCodes] = []uint64{200, 200, 500, 200, 200}
				op.ResultCount = []uint64{87, 88, 87, 87, 87}
			},
		)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.4 final gate requires the exact selected t283 REST where tuple", t, func() {
		So(finalGateT283WhereCheckPassesAfterRESTParamMutation(func(params map[string]string) {
			params[finalGateRESTParamGroups] = "14977"
		}), ShouldBeFalse)
		So(finalGateT283WhereCheckPassesAfterRESTParamMutation(func(params map[string]string) {
			delete(params, finalGateRESTParamUsers)
		}), ShouldBeFalse)
		So(finalGateT283WhereCheckPassesAfterRESTParamMutation(func(params map[string]string) {
			delete(params, finalGateRESTParamTypes)
		}), ShouldBeFalse)
	})

	Convey("E3.5 final gate fails when filtered root ClickHouse where p95 is over the Bolt tolerance", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOpMatching(&evidence, queryOpTreeWhereColdName,
			finalGateExactDirPredicate("/"), func(op *perfreport.Operation) {
				op.P95MS = 44.717
				op.P99MS = 44.717
			})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 root where vs Bolt").Passed, ShouldBeFalse)
	})

	Convey("E3.6 final gate fails when where-frontier paired facts digests differ", t, func() {
		evidence := finalGateTestEvidence(false, false)
		for i := range evidence.ResultEquivalence {
			if evidence.ResultEquivalence[i].Operation == queryOpWhereFilteredWholeMountName {
				evidence.ResultEquivalence[i].CandidateDigest = finalGateTestDifferentDigest
			}
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 facts digest equivalence").Passed, ShouldBeFalse)
	})

	Convey("E3.7 final gate fails when new table stats lack memory and amplification evidence", t, func() {
		evidence := finalGateTestEvidence(false, false)
		stats := evidence.ImportReports[0].TableStats[tableDirFacts]
		stats.ImportMemoryBytes = 0
		stats.RowAmplificationVsDirFacts = 0
		stats.RowAmplificationVsCatalog = 0
		evidence.ImportReports[0].TableStats[tableDirFacts] = stats

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 table stats row amplification evidence").Passed, ShouldBeFalse)
	})

	Convey("E3.7 final gate fails when active-prefix table stats are missing", t, func() {
		evidence := finalGateTestEvidence(false, false)
		for i := range evidence.ImportReports {
			delete(evidence.ImportReports[i].TableStats, tableActivePrefixRollups)
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 table stats row amplification evidence").Passed, ShouldBeFalse)
	})

	Convey("E3.8 final gate fails p95/p99 regressions for non-targeted operations", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOp(&evidence, queryOpFilesListDirName, func(op *perfreport.Operation) {
			op.P50MS = 9
			op.P95MS = 11
			op.P99MS = 12
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 non-targeted baseline regression").Passed, ShouldBeFalse)
	})

	Convey("E3.8 final gate allows sub-5 ms noisy-run regressions within 10 percent", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOp(&evidence, queryOpFilesListDirName, func(op *perfreport.Operation) {
			op.P50MS = 4
			op.P95MS = 10.5
			op.P99MS = 10.5
		})

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "E3 non-targeted baseline regression").Passed, ShouldBeTrue)
	})

	Convey("E3.8 final gate keeps non-targeted REST operations in baseline regression", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateAddRESTTreeOp(&evidence.BaselineQueryReports[0], "/non-targeted/", 10)
		finalGateAddRESTTreeOp(&evidence.QueryReports[0], "/non-targeted/", 20)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 non-targeted baseline regression").Passed, ShouldBeFalse)
	})

	Convey("E3.9 final gate includes info correctness in permission/auth baseline comparison", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateSetCandidateInput(&evidence, queryOpInfoName, queryInputResultDigest, finalGateTestDifferentDigest)

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, queryOpInfoName)
	})
}

func TestJ6FinalGates(t *testing.T) {
	Convey("J6 records every matrix delta and storage byte comparison", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "J6 matrix correctness and deltas").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "J6 absolute cold UX").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "J6 storage layout and table bytes").Passed, ShouldBeTrue)
		So(finalGateTestCheck(result, "J6 D4 collapse decisions").Passed, ShouldBeTrue)
		So(result.J6QueryDeltas, ShouldHaveLength, len(j4RequiredMatrixOperations()))
		So(result.J6QueryDeltas[0].Operation, ShouldEqual, queryOpTreeDirInfoName)
		So(result.J6QueryDeltas[0].QueryVariant, ShouldEqual, "DirInfo selected directory")
		So(result.J6TableByteDeltas, ShouldNotBeEmpty)
	})

	Convey("J6 fails when a matrix operation variant is missing or reports a wrong row", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateRemoveJ6MatrixOperation(&evidence, queryOpFilesStatPathName)

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryOpFilesStatPathName)

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpDirInfoFilteredName, func(op *perfreport.Operation) {
			op.P50MS = -1
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "p50/p95/p99")
		So(check.Detail, ShouldContainSubstring, queryOpDirInfoFilteredName)

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpTreeDirInfoName, func(op *perfreport.Operation) {
			op.Inputs[finalGateJ6WrongRowCountInput] = uint64(1)
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "wrong row")
	})

	Convey("J6 fails when required virtual and maintenance matrix rows are missing", t, func() {
		check := finalGateJ6MatrixCheckAfterRemoving(queryOpVirtualActivePrefixRollupName)
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryOpVirtualActivePrefixRollupName)

		check = finalGateJ6MatrixCheckAfterRemoving(queryOpImportReadinessPublishName)
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryOpImportReadinessPublishName)

		check = finalGateJ6MatrixCheckAfterRemoving(queryOpActiveSnapshotCleanupName)
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryOpActiveSnapshotCleanupName)
	})

	Convey("J6 rejects synthetic virtual and maintenance matrix evidence", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpImportReadinessPublishName, func(op *perfreport.Operation) {
			op.Inputs[queryInputDurationSource] = querySourceWall
		})

		result := ValidateFinalGates(evidence)
		check := finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "duration_source")

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpActiveSnapshotCleanupName, func(op *perfreport.Operation) {
			delete(op.Inputs, queryInputAuditCounts)
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryInputAuditCounts)

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpImportReadinessPublishName, func(op *perfreport.Operation) {
			op.Inputs[queryInputAuditSurfaces] = []string{
				tableSchema3SnapshotSets,
				queryAuditSurfaceActiveVirtualReady,
				tableActivePrefixRollupSets,
			}
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryAuditSurfaceMountEventsPublish)

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6MatrixOperation(&evidence, queryOpVirtualActivePrefixRollupName, func(op *perfreport.Operation) {
			op.Inputs[queryInputActivePrefixRouteProof] = queryActivePrefixRouteProofUnobserved
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 matrix correctness and deltas")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, queryInputActivePrefixRouteProof)
	})

	Convey("J6 fails storage when hot rows store paths or table bytes lack a baseline", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateJ6StorageAudit(&evidence, func(op *perfreport.Operation) {
			op.Inputs[finalGateJ6HotRowPathStringTablesInput] = []string{tableDirFacts}
		})

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "J6 storage layout and table bytes")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "path string")

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateJ6StorageAudit(&evidence, func(op *perfreport.Operation) {
			op.Inputs[finalGateJ6PathTextCopiesPerDirSnapshotInput] = float64(2)
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 storage layout and table bytes")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "one copy")

		evidence = finalGateTestEvidence(false, false)
		finalGateDeleteE2TableStats(&evidence, tableChildFilterAll)

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 storage layout and table bytes")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "filter table")

		evidence = finalGateTestEvidence(false, false)
		evidence.BaselineImportReports = nil

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 storage layout and table bytes")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "baseline")
	})

	Convey("J6 enforces the absolute cold UX p95 thresholds", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateCandidateOp(&evidence, queryOpFilesStatPathName, func(op *perfreport.Operation) {
			op.P95MS = 100
			op.P99MS = 100
		})

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "J6 absolute cold UX")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "file_stat")

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateCandidateOp(&evidence, queryOpTreeWhereColdProviderName, func(op *perfreport.Operation) {
			op.Inputs[queryInputCacheScope] = queryScopeColdProvider
			op.P95MS = 750
			op.P99MS = 750
		})

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 absolute cold UX")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "where_cold_provider")

		evidence = finalGateTestEvidence(false, false)
		finalGateRemoveCandidateOpMatching(&evidence, queryOpTreeWhereFreshName, nil)

		result = ValidateFinalGates(evidence)
		check = finalGateTestCheck(result, "J6 absolute cold UX")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "where_fresh_provider missing")
	})

	Convey("J6 requires collapsed D4 materialisations to cite a passing measurement", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateMutateD4Decision(&evidence, finalGateJ6D4PatternFilteredChildren, func(op *perfreport.Operation) {
			op.Inputs[finalGateJ6D4DecisionInput] = finalGateJ6D4DecisionCollapsed
			op.Inputs[finalGateJ6D4CitationInput] = ""
		})

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "J6 D4 collapse decisions")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "citation")

		evidence = finalGateTestEvidence(false, false)
		finalGateMutateD4Decision(&evidence, finalGateJ6D4PatternFilteredChildren, func(op *perfreport.Operation) {
			op.Inputs[finalGateJ6D4DecisionInput] = finalGateJ6D4DecisionCollapsed
			op.Inputs[finalGateJ6D4MeasuredP95Input] = float64(500)
		})

		result = ValidateFinalGates(evidence)

		check = finalGateTestCheck(result, "J6 D4 collapse decisions")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "latency gate")
	})

	Convey("J6 rejects D4 decisions supplied only by import reports", t, func() {
		evidence := finalGateTestEvidence(false, false)
		finalGateDeleteQueryD4Decisions(&evidence)

		result := ValidateFinalGates(evidence)

		check := finalGateTestCheck(result, "J6 D4 collapse decisions")
		So(check.Passed, ShouldBeFalse)
		So(check.Detail, ShouldContainSubstring, "missing D4 decision")
	})
}

func finalGateJ6MatrixReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", finalGateMinRepeats, 0)
	for _, spec := range j4RequiredMatrixOperations() {
		finalGateAddJ6MatrixOp(&report, spec)
	}

	return report
}

func finalGateAddJ6MatrixOp(
	report *perfreport.Report,
	spec j4RequiredMatrixOperation,
) {
	inputs := map[string]any{
		queryInputDirKey:          queryOpTestRootDir,
		queryInputQueryTypeKey:    spec.QueryType,
		queryInputQueryVariantKey: spec.QueryVariant,
		queryInputDurationSource:  querySourceClickHouseLog,
		queryInputResultDigest:    "sha256:j6-" + strings.ReplaceAll(spec.Operation, "_", "-"),
	}
	queryMatrixAddRequiredEvidence(inputs, spec.Operation)

	report.AddOperationWithCounters(
		spec.Operation,
		inputs,
		[]float64{0.01, 0.01, 0.01, 0.01, 0.01},
		finalGateE1Counts(100),
		finalGateE1Counts(2048),
		finalGateE1Counts(2),
		finalGateE1Counts(9),
	)
}

func finalGateCacheScopeInputs(scope string) map[string]any {
	return map[string]any{
		queryInputCacheScope: scope,
	}
}

func finalGateFilteredCacheScopeInputs(scope string) map[string]any {
	inputs := finalGateFilteredInputs()
	inputs[queryInputCacheScope] = scope

	return inputs
}

func finalGateJ6MatrixCheckAfterRemoving(operation string) FinalGateCheck {
	evidence := finalGateTestEvidence(false, false)
	finalGateRemoveJ6MatrixOperation(&evidence, operation)

	result := ValidateFinalGates(evidence)

	return finalGateTestCheck(result, "J6 matrix correctness and deltas")
}

func finalGateRemoveJ6MatrixOperation(evidence *FinalGateEvidence, operation string) {
	remove := func(reports []perfreport.Report) {
		for reportIndex := range reports {
			ops := reports[reportIndex].Operations
			for opIndex := range ops {
				if finalGateJ6MatrixOperationMatches(ops[opIndex], operation) {
					reports[reportIndex].Operations = slices.Delete(ops, opIndex, opIndex+1)

					return
				}
			}
		}
	}

	remove(evidence.QueryReports)
}

func finalGateMutateJ6MatrixOperation(
	evidence *FinalGateEvidence,
	operation string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]
			if finalGateJ6MatrixOperationMatches(*op, operation) {
				mutate(op)

				return
			}
		}
	}
}

func finalGateJ6MatrixOperationMatches(op perfreport.Operation, operation string) bool {
	return op.Name == operation && stringInput(op.Inputs, queryInputQueryTypeKey) != ""
}

func finalGateMutateJ6StorageAudit(
	evidence *FinalGateEvidence,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.ImportReports {
		for opIndex := range evidence.ImportReports[reportIndex].Operations {
			op := &evidence.ImportReports[reportIndex].Operations[opIndex]
			if op.Name == finalGateJ6StorageAuditOpName {
				mutate(op)

				return
			}
		}
	}
}

func finalGateTableStatsWithBaselines(
	rows uint64,
	dirFactsRows uint64,
	catalogRows uint64,
) perfreport.TableStats {
	return perfreport.TableStats{
		Rows:                       rows,
		ActiveParts:                1,
		CompressedBytes:            rows * 10,
		UncompressedBytes:          rows * 20,
		ImportMemoryBytes:          402_000 * 1024,
		RowAmplificationVsDirFacts: float64(rows) / float64(dirFactsRows),
		RowAmplificationVsCatalog:  float64(rows) / float64(catalogRows),
		ImportPhaseDurationsMS: map[string]float64{
			phaseDirProjectionWrite: 1,
		},
	}
}

func finalGateSetE2QueryTableStats(
	evidence *FinalGateEvidence,
	table string,
	stats perfreport.TableStats,
) {
	for reportIndex := range evidence.QueryReports {
		if evidence.QueryReports[reportIndex].TableStats == nil {
			evidence.QueryReports[reportIndex].TableStats = make(map[string]perfreport.TableStats)
		}

		evidence.QueryReports[reportIndex].TableStats[table] = stats
	}
}

func finalGateDeleteE2TableStats(evidence *FinalGateEvidence, table string) {
	for reportIndex := range evidence.ImportReports {
		delete(evidence.ImportReports[reportIndex].TableStats, table)
	}

	for reportIndex := range evidence.QueryReports {
		delete(evidence.QueryReports[reportIndex].TableStats, table)
	}

	if evidence.FinalGateReport == nil {
		return
	}

	for reportIndex := range evidence.FinalGateReport.SpoolLoadReports {
		delete(evidence.FinalGateReport.SpoolLoadReports[reportIndex].TableStats, table)
	}
}

func finalGateAddImportFileTotals(report *perfreport.Report, rowCap uint64) {
	for _, root := range finalGateRequiredImportRoots() {
		report.AddOperation("import_file_total", map[string]any{
			importInputDataset:   "2026-06-07-" + strings.Trim(strings.ReplaceAll(root, "/", "_"), "_"),
			importInputStatsPath: "/home/ubuntu/output" + strings.TrimSuffix(root, "/") + "/stats.gz",
			importInputMountPath: root,
			importInputRowCap:    rowCap,
			"lines":              rowCap,
			"rows_per_table": map[string]uint64{
				tableFiles:      rowCap,
				tableCatalog:    rowCap / 2,
				tableDirSummary: rowCap / 2,
			},
		}, []float64{1})
	}
}

func finalGateRunSuiteD2AuthReport() (perfreport.Report, error) {
	database := newQueryOpTestDB()
	database.summaries[queryOpTestRootDir].GIDs = []uint32{7, 8}
	database.summaries[queryOpTestChildADir].GIDs = []uint32{7}
	database.summaries[queryOpTestChildBDir].GIDs = []uint32{8}

	qctx := queryContext{
		provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
		client: &fakeQueryClient{rows: []QueryRow{{
			Path:      queryOpTestRootDir + "sample.bam",
			Ext:       queryTestBamExt,
			EntryType: 'f',
		}}},
		inspector: fakeQueryInspector{
			measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
				if err := run(ctx); err != nil {
					return nil, err
				}

				return &QueryMetrics{DurationMs: 3}, nil
			},
		},
		dir:  queryOpTestRootDir,
		uid:  20155,
		gids: []uint32{7},
	}
	report := perfreport.NewReport("clickhouse", "", finalGateMinRepeats, 0)

	err := runSuite(&report, qctx, QueryOptions{
		Repeat: finalGateMinRepeats,
		Ops:    finalGatePermissionAuthOps(),
		Splits: 1,
	}, func(string, ...any) {})

	return report, err
}

func finalGateReplaceD2AuthOps(report *perfreport.Report, replacement perfreport.Report) {
	for _, name := range finalGatePermissionAuthOps() {
		replacementOp, ok := firstOperation(replacement, name, nil)
		So(ok, ShouldBeTrue)

		opIndex := finalGateFindQueryOpIndex(*report, name)
		So(opIndex, ShouldBeGreaterThanOrEqualTo, 0)

		report.Operations[opIndex] = replacementOp
	}
}

func finalGateBoltQueryReport() perfreport.Report {
	report := finalGateQueryReport()
	report.Backend = finalGateComparisonKindBolt
	finalGateSetReportOpP95(&report, queryOpTreeWhereColdName, "/", finalGateRootBoltP95MS)
	finalGateSetReportOpP95(&report, queryOpTreeWhereColdName, finalGateT283Dir, finalGateT283BoltP95MS)

	return report
}

func finalGateSetReportOpP95(report *perfreport.Report, name string, root string, p95 float64) {
	for i := range report.Operations {
		op := &report.Operations[i]
		if op.Name == name && operationRootEquals(*op, root) {
			op.P50MS = p95
			op.P95MS = p95
			op.P99MS = p95
			op.DurationsMS = []float64{p95, p95, p95, p95, p95}

			return
		}
	}
}

func finalGateAttachE2Evidence(evidence *FinalGateEvidence) {
	e2QueryReport := finalGateE2QueryReport()
	evidence.QueryReports = append(evidence.QueryReports, e2QueryReport)
	evidence.BaselineQueryReports = append(evidence.BaselineQueryReports, e2QueryReport)
	evidence.FinalGateReport.SpoolLoadReports = finalGateE2SpoolLoadReports()
}

func finalGateE2QueryReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "mixed8", finalGateMinRepeats, 0)

	finalGateAddE2RESTFirstTreeOps(&report)
	finalGateAddE2HighFanoutPacketOps(&report)
	finalGateAddE2WhereOps(&report)

	return report
}

func finalGateAddE2RESTFirstTreeOps(report *perfreport.Report) {
	for _, path := range []string{"/", finalGateLustreRootDir, finalGateNFSRootDir} {
		inputs := finalGateE2RESTInputs(
			finalGateTestE2ScenarioRESTFirst,
			path,
			"sha256:e2-rest-tree-"+path,
			false,
		)
		finalGateAddE2MeasuredOp(report, finalGateRESTOpTree, inputs, 120, 3, 10, 1024, 1)
	}
}

func finalGateAddE2HighFanoutPacketOps(report *perfreport.Report) {
	finalGateAddE2PacketOp(
		report,
		finalGateRESTOpTree,
		finalGateTestE2ScenarioHighFanoutBroad,
		tableDirFacts,
		300,
		true,
		false,
	)
	finalGateAddE2PacketOp(
		report,
		finalGateRESTOpTree,
		finalGateTestE2ScenarioHighFanoutFilter,
		"wrstat_child_filter_all",
		280,
		true,
		true,
	)
	finalGateAddE2PacketOp(
		report,
		queryOpDirInfosBroadName,
		finalGateTestE2ScenarioDirInfosBroad,
		tableDirFacts,
		420,
		false,
		false,
	)
	finalGateAddE2PacketOp(
		report,
		queryOpDirInfosFilteredName,
		"dirinfos_filtered_child_filter_packet",
		"wrstat_child_filter_all",
		410,
		false,
		true,
	)
	finalGateAddE2PacketOp(
		report,
		queryOpDirsHaveChildrenBroadName,
		"dirshavechildren_broad_parent_packet",
		tableDirFacts,
		390,
		false,
		false,
	)
	finalGateAddE2PacketOp(
		report,
		queryOpDirsHaveChildrenFilteredName,
		finalGateTestE2ScenarioDHCFiltered,
		"wrstat_child_filter_all",
		380,
		false,
		true,
	)
}

func finalGateAddE2PacketOp(
	report *perfreport.Report,
	name string,
	scenario string,
	packetTable string,
	duration float64,
	includeCurrent bool,
	filtered bool,
) {
	inputs := finalGateE2PacketInputs(scenario, packetTable, filtered)
	if includeCurrent {
		inputs["current_read_count"] = uint64(1)
	}

	finalGateAddE2MeasuredOp(report, name, inputs, duration, 11205, 602, 48_000, 4)
}

func finalGateE2PacketInputs(scenario string, packetTable string, filtered bool) map[string]any {
	inputs := finalGateE2CorrectInputs(scenario, finalGateHighFanoutParentDir, "sha256:e2-"+scenario)
	inputs[queryInputParentDirKey] = finalGateHighFanoutParentDir
	inputs[finalGateRESTInputQueryParams] = map[string]string{finalGateRESTParamPath: finalGateHighFanoutParentDir}
	inputs[finalGateTestE2ParentPacketTableInput] = packetTable
	inputs["parent_packet_read_count"] = uint64(1)
	inputs["per_child_query_count"] = uint64(0)
	inputs["subtree_scan_count"] = uint64(0)
	inputs[finalGateTestE2ReadRowsCeilingInput] = uint64(700)
	inputs["read_bytes_ceiling"] = uint64(64_000)
	inputs["read_marks_ceiling"] = uint64(8)
	inputs[queryInputHighFanoutChildCount] = uint64(11205)

	if filtered {
		for key, value := range finalGateFilteredInputs() {
			inputs[key] = value
		}

		inputs[finalGateTestE2FactsVectorRowsInput] = uint64(0)
	}

	if scenario == finalGateTestE2ScenarioDHCFiltered {
		inputs["expected_true_children"] = []string{
			finalGateHighFanoutParentDir + "child-a/",
			finalGateHighFanoutParentDir + "child-c/",
		}
		inputs["actual_true_children"] = []string{
			finalGateHighFanoutParentDir + "child-a/",
			finalGateHighFanoutParentDir + "child-c/",
		}
	}

	return inputs
}

func finalGateAddE2WhereOps(report *perfreport.Report) {
	inputs := finalGateE2CorrectInputs(
		finalGateTestE2ScenarioFirstRootWhere,
		"/",
		"sha256:e2-root-where-splits-2",
	)
	inputs[queryInputSplitsKey] = uint64(2)
	inputs[queryInputCacheScope] = queryScopeFreshProvider
	finalGateAddE2MeasuredOp(report, queryOpTreeWhereFreshName, inputs, 420, 90, 140, 12_000, 2)

	switchInputs := finalGateE2RESTInputs(
		finalGateTestE2ScenarioSwitch,
		finalGateHighFanoutParentDir,
		"sha256:e2-filter-switch",
		true,
	)
	switchInputs["preceded_by_unfiltered_tree_request"] = true
	switchInputs["used_broad_packet_cache"] = false
	switchInputs[queryInputCacheHitKeysKey] = []string{
		"parent_packet:path=" + finalGateHighFanoutParentDir + ";filter=other;active_set_id=e2;query_version=1",
	}
	finalGateAddE2MeasuredOp(report, finalGateRESTOpTree, switchInputs, 450, 12, 80, 8192, 1)

	finalGateAddE2CLIWhereOp(report, finalGateTestE2ScenarioRealWhere, "/", 430)
	finalGateAddE2CLIWhereOp(report, finalGateTestE2ScenarioRealWhere, finalGateHighFanoutParentDir, 440)
	finalGateAddE2CLIWhereOp(report, finalGateTestE2ScenarioNFSHeavyWhere, finalGateNFSHeavyDir, 450)
}

func finalGateE2SpoolLoadReports() []perfreport.Report {
	reports := make([]perfreport.Report, 0, finalGateMinRepeats)
	for range finalGateMinRepeats {
		report := finalGateE1ImportReport("spool_load_total", 8192, finalGateComparisonStatusSuccess)
		report.Repeat = 1
		report.TableStats[finalGateE2ChildFilterAllTable] = finalGateTableStatsWithBaselines(11205, 35006, 35005)
		reports = append(reports, report)
	}

	return reports
}

func finalGateMutateCandidateOpMatching(
	evidence *FinalGateEvidence,
	name string,
	pred operationPredicate,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]
			if op.Name == name && (pred == nil || pred(*op)) {
				mutate(op)

				return
			}
		}
	}
}

func finalGateRESTPathPredicate(path string) operationPredicate {
	return func(op perfreport.Operation) bool {
		return queryParamInput(op.Inputs, finalGateRESTParamPath) == path
	}
}

func finalGateRemoveCandidateOpMatching(
	evidence *FinalGateEvidence,
	name string,
	pred operationPredicate,
) {
	for reportIndex := range evidence.QueryReports {
		ops := evidence.QueryReports[reportIndex].Operations
		for opIndex := 0; opIndex < len(ops); {
			if ops[opIndex].Name == name && (pred == nil || pred(ops[opIndex])) {
				ops = slices.Delete(ops, opIndex, opIndex+1)

				continue
			}

			opIndex++
		}

		evidence.QueryReports[reportIndex].Operations = ops
	}
}

func finalGateRESTDirPredicate(dir string) operationPredicate {
	return func(op perfreport.Operation) bool {
		return queryParamInput(op.Inputs, queryInputDirKey) == dir
	}
}

func finalGateExactDirPredicate(dir string) operationPredicate {
	return func(op perfreport.Operation) bool {
		return operationRootEquals(op, dir)
	}
}

func finalGateT283WhereCheckPassesAfterRESTParamMutation(mutate func(map[string]string)) bool {
	evidence := finalGateTestEvidence(false, false)
	finalGateMutateT283RESTWhereParams(&evidence, mutate)

	result := ValidateFinalGates(evidence)

	return finalGateTestCheck(result, "E3 t283 where vs Bolt").Passed
}

func finalGateTestEvidence(ageAllSelected, virtualCacheSelected bool) FinalGateEvidence {
	finalGateReport := finalGateE1Report()
	importReports := finalGateImportReports(ageAllSelected, virtualCacheSelected)
	queryReports := []perfreport.Report{finalGateQueryReport(), finalGateJ6MatrixReport()}
	baselineQueryReports := []perfreport.Report{finalGateQueryReport(), finalGateJ6MatrixReport()}

	evidence := FinalGateEvidence{
		ImportReports:         importReports,
		BaselineImportReports: finalGateImportReports(ageAllSelected, virtualCacheSelected),
		QueryReports:          queryReports,
		BaselineQueryReports:  baselineQueryReports,
		BoltQueryReports:      []perfreport.Report{finalGateBoltQueryReport()},
		RequiredQueryRoots:    []string{queryOpTestRootDir},
		ResultEquivalence:     finalGateResultEquivalence(),
		FinalGateReport:       &finalGateReport,
		T283FilteredRESTOrder: &FinalGateT283FilteredRESTOrderEvidence{
			DirectDigest:      finalGateT283Digest,
			WarmedDigest:      finalGateT283Digest,
			DirectResultCount: 87,
			WarmedResultCount: 87,
			WarmedCacheHitKeys: []string{
				queryTestE2CacheHitKey,
			},
		},
	}

	finalGateAttachE2Evidence(&evidence)

	return evidence
}

func finalGateImportReports(ageAllSelected, virtualCacheSelected bool) []perfreport.Report {
	reports := make([]perfreport.Report, 0, finalGateMinRepeats*2)
	for range finalGateMinRepeats {
		reports = append(reports, finalGateImportReport(finalGateImportCap100K, ageAllSelected, virtualCacheSelected))
	}

	for range finalGateMinRepeats {
		reports = append(reports, finalGateImportReport(finalGateImportCap250K, ageAllSelected, virtualCacheSelected))
	}

	return reports
}

func finalGateImportReport(rowCap uint64, ageAllSelected, virtualCacheSelected bool) perfreport.Report {
	report := perfreport.NewReport("clickhouse", "/home/ubuntu/output/nfs", 1, 0)
	report.MaxRSSBytes = 402_000 * 1024
	rootCount := uint64(len(finalGateRequiredImportRoots()))
	filesRows := rowCap * rootCount
	catalogRows := rowCap * rootCount * 35 / 100
	dirFactsRows := rowCap * rootCount * 35 / 100
	report.SelectedTables = []string{
		tableFiles,
		tableCatalog,
		tableDirSummary,
		tableDirSummarySets,
		tableDirFilterAgeAll,
		tableChildFilterAll,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
		finalGateE2DirFilterAllTable,
		finalGateE2Schema3SnapshotSetsTable,
	}
	report.TableStats = map[string]perfreport.TableStats{
		tableFiles:                          finalGateTableStatsWithBaselines(filesRows, dirFactsRows, catalogRows),
		tableCatalog:                        finalGateTableStatsWithBaselines(catalogRows, dirFactsRows, catalogRows),
		tableDirSummary:                     finalGateTableStatsWithBaselines(dirFactsRows, dirFactsRows, catalogRows),
		tableDirSummarySets:                 finalGateTableStatsWithBaselines(1, dirFactsRows, catalogRows),
		tableDirFilterAgeAll:                finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, catalogRows),
		tableChildFilterAll:                 finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, catalogRows),
		tableActivePrefixRollups:            finalGateTableStatsWithBaselines(3, dirFactsRows, catalogRows),
		tableActivePrefixFilterAgeAll:       finalGateTableStatsWithBaselines(rowCap/3, dirFactsRows, catalogRows),
		tableActivePrefixRollupSets:         finalGateTableStatsWithBaselines(1, dirFactsRows, catalogRows),
		finalGateE2DirFilterAllTable:        finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, catalogRows),
		finalGateE2Schema3SnapshotSetsTable: finalGateTableStatsWithBaselines(1, dirFactsRows, catalogRows),
	}
	finalGateAddImportFileTotals(&report, rowCap)

	if ageAllSelected {
		report.TableStats[tableDirFilterAgeAll] = finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, catalogRows)
	}

	if virtualCacheSelected {
		report.SelectedTables = append(report.SelectedTables, tableTreeDGUTA)
		report.TableStats[tableTreeDGUTA] = finalGateTableStatsWithBaselines(50, dirFactsRows, catalogRows)
	}

	report.AddOperation("import_total", map[string]any{
		importInputRecords:         rowCap * rootCount,
		importInputRowCap:          rowCap,
		importInputTotalCPUMS:      uint64(2400),
		importInputPeakRSSBytes:    report.MaxRSSBytes,
		finalGateE2InputSpoolBytes: uint64(0),
		finalGateE2InputPartCounts: importActivePartCounts(report.TableStats),
	}, []float64{4350})
	finalGateAddJ6StorageAudit(&report)
	finalGateAddD4Decisions(&report)

	return report
}

func finalGateFindQueryOpIndex(report perfreport.Report, name string) int {
	for i, op := range report.Operations {
		if op.Name == name {
			return i
		}
	}

	return -1
}

func finalGateSetReportInfoCountVector(report *perfreport.Report, counts []uint64) {
	opIndex := finalGateFindQueryOpIndex(*report, queryOpInfoName)
	So(opIndex, ShouldBeGreaterThanOrEqualTo, 0)

	op := &report.Operations[opIndex]
	op.ResultCount = slices.Clone(counts)
	op.Inputs[queryInputInfoCountFieldsKey] = db.InfoCountFieldNames()
	op.Inputs[queryInputResultDigest] = "sha256:info-counts"
}

func finalGateQueryReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", finalGateMinRepeats, 1)

	finalGateAddOp(&report, queryOpTreeWhereName, nil, 8)
	finalGateAddOp(&report, queryOpTreeWhereColdName, finalGateFilteredWhereInputs("/", "sha256:root-filtered-where"), 40)
	finalGateAddOp(&report, queryOpTreeWhereColdName,
		finalGateFilteredWhereInputs(finalGateT283Dir, "sha256:t283-filtered-where"), 80)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName, finalGateCacheScopeInputs(queryScopeColdProvider), 110)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName,
		finalGateFilteredCacheScopeInputs(queryScopeColdProvider), 100)
	finalGateAddOp(&report, queryOpTreeDiskTreeColdProviderName, nil, 100)
	finalGateAddOp(&report, queryOpTreeDiskTreeColdProviderName, finalGateFilteredInputs(), 90)
	finalGateAddOp(&report, queryOpTreeDiskTreeEndName, nil, 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeEndName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeProviderUpdateName, nil, 20)
	finalGateAddOp(&report, queryOpTreeDiskTreeProviderUpdateName, finalGateFilteredInputs(), 27)
	finalGateAddOp(&report, queryOpTreeDirInfoName, nil, 1)
	finalGateAddOp(&report, queryOpDirInfoBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirInfoFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpDirInfosBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirInfosFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpDirsHaveChildrenBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirsHaveChildrenFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpWhereWholeMountName, nil, 1)
	finalGateAddOp(&report, queryOpWhereFilteredWholeMountName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeVisibleChildName, nil, 0.04)
	finalGateAddOp(&report, queryOpFilesListDirName, nil, 10)
	finalGateAddOp(&report, queryOpFilesStatPathName, nil, 10)
	finalGateAddOp(&report, queryOpGlobCaseAName, nil, 10)
	finalGateAddOp(&report, queryOpGlobCaseBName, nil, 10)
	finalGateAddOp(&report, queryOpFindGlobExtensionDotfileName, nil, 10)
	finalGateAddOp(&report, queryOpVirtualChildrenName, finalGateFilteredInputs(), 10)
	finalGateAddOp(&report, queryOpVirtualDirInfoName, finalGateFilteredInputs(), 10)
	finalGateAddE3RESTOps(&report)
	finalGateAddD2AuthComparisonOps(&report)
	finalGateAddD4Decisions(&report)

	return report
}

func finalGateMutateT283RESTWhereParams(
	evidence *FinalGateEvidence,
	mutate func(map[string]string),
) {
	finalGateMutateCandidateOpMatching(
		evidence,
		finalGateRESTOpWhere,
		finalGateRESTDirPredicate(finalGateT283Dir),
		func(op *perfreport.Operation) {
			params, ok := op.Inputs[finalGateRESTInputQueryParams].(map[string]string)
			So(ok, ShouldBeTrue)

			mutate(params)
		},
	)
}

func finalGateTestCheck(result FinalGateResult, name string) FinalGateCheck {
	for _, check := range result.Checks {
		if check.Name == name {
			return check
		}
	}

	return FinalGateCheck{}
}

func finalGateSetCandidateP95(evidence *FinalGateEvidence, name string, p95 float64) {
	finalGateMutateCandidateOp(evidence, name, func(op *perfreport.Operation) {
		op.P95MS = p95
	})
}

func finalGateE2RESTTreeOp(evidence FinalGateEvidence, path string, check func(perfreport.Operation)) {
	finalGateE2ScenarioRootOp(evidence, finalGateTestE2ScenarioRESTFirst, path, check)
}

func finalGateE2ScenarioRootOp(
	evidence FinalGateEvidence,
	scenario string,
	root string,
	check func(perfreport.Operation),
) {
	for _, report := range evidence.QueryReports {
		for _, op := range report.Operations {
			if stringInput(op.Inputs, finalGateTestE2ScenarioKey) != scenario || !operationRootEquals(op, root) {
				continue
			}

			check(op)

			return
		}
	}

	So("missing "+scenario+" "+root, ShouldEqual, "")
}

func finalGateMutateE2OpRoot(
	evidence *FinalGateEvidence,
	scenario string,
	root string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]
			if stringInput(op.Inputs, finalGateTestE2ScenarioKey) != scenario || !operationRootEquals(*op, root) {
				continue
			}

			mutate(op)

			return
		}
	}
}

func finalGateE2ScenarioOp(
	evidence FinalGateEvidence,
	scenario string,
	check func(perfreport.Operation),
) {
	for _, report := range evidence.QueryReports {
		for _, op := range report.Operations {
			if stringInput(op.Inputs, finalGateTestE2ScenarioKey) == scenario {
				check(op)

				return
			}
		}
	}

	So("missing "+scenario, ShouldEqual, "")
}

func finalGateMutateE2Op(
	evidence *FinalGateEvidence,
	scenario string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]
			if stringInput(op.Inputs, finalGateTestE2ScenarioKey) == scenario {
				mutate(op)

				return
			}
		}
	}
}

func finalGateE2ImportBudgetOp(
	report perfreport.Report,
	opName string,
	check func(perfreport.Operation),
) {
	op, ok := firstOperation(report, opName, nil)
	So(ok, ShouldBeTrue)
	check(op)
}

func finalGateMutateImportBudgetOp(
	report *perfreport.Report,
	opName string,
	mutate func(*perfreport.Operation),
) {
	for opIndex := range report.Operations {
		op := &report.Operations[opIndex]
		if op.Name == opName {
			mutate(op)

			return
		}
	}
}

func finalGateAddE2MeasuredOp(
	report *perfreport.Report,
	name string,
	inputs map[string]any,
	duration float64,
	resultCount uint64,
	readRows uint64,
	readBytes uint64,
	readMarks uint64,
) {
	durations := []float64{duration, duration, duration, duration, duration}
	counts := []uint64{resultCount, resultCount, resultCount, resultCount, resultCount}
	reads := []uint64{readRows, readRows, readRows, readRows, readRows}
	bytes := []uint64{readBytes, readBytes, readBytes, readBytes, readBytes}
	marks := []uint64{readMarks, readMarks, readMarks, readMarks, readMarks}

	report.AddOperationWithCounters(name, inputs, durations, reads, bytes, marks, counts)
}

func finalGateE2RESTInputs(
	scenario string,
	path string,
	digest string,
	filtered bool,
) map[string]any {
	inputs := finalGateE2CorrectInputs(scenario, path, digest)
	inputs["endpoint"] = "/rest/v1/auth/tree"
	inputs[finalGateRESTInputQueryParams] = map[string]string{finalGateRESTParamPath: path}
	inputs[finalGateRESTInputStatusCodes] = []uint64{200, 200, 200, 200, 200}
	inputs[finalGateRESTInputJSONBytes] = []uint64{8192, 8192, 8192, 8192, 8192}
	inputs[finalGateRESTInputGzipBytes] = []uint64{2048, 2048, 2048, 2048, 2048}
	inputs[finalGateRESTInputCacheHits] = []uint64{0, 0, 0, 0, 0}
	inputs[finalGateRESTInputCacheMisses] = []uint64{1, 1, 1, 1, 1}
	inputs["query_count"] = []uint64{1, 1, 1, 1, 1}

	if filtered {
		for key, value := range finalGateFilteredInputs() {
			inputs[key] = value
		}
	}

	return inputs
}

func finalGateAddE2CLIWhereOp(
	report *perfreport.Report,
	scenario string,
	dir string,
	duration float64,
) {
	inputs := finalGateE2CorrectInputs(scenario, dir, "sha256:e2-cli-where-"+dir)
	inputs[queryInputCacheScope] = queryScopeFreshProvider
	inputs["command"] = []string{
		finalGateWrstatUICommand,
		finalGateWhereCommandName,
		"--dir",
		dir,
		"--json",
	}

	finalGateAddE2MeasuredOp(report, finalGateRESTOpCLIWhere, inputs, duration, 11, 120, 10_000, 2)
}

func finalGateE2CorrectInputs(scenario string, dir string, digest string) map[string]any {
	return map[string]any{
		finalGateTestE2ScenarioKey:           scenario,
		queryInputDirKey:                     dir,
		queryInputCacheScope:                 queryScopeColdProvider,
		finalGateTestE2ExpectedDigestInput:   digest,
		queryInputResultDigest:               digest,
		finalGateCorrectnessStatusInput:      finalGateComparisonStatusSuccess,
		finalGateTestE2ProactiveWarmingInput: false,
	}
}

func finalGateAddJ6StorageAudit(report *perfreport.Report) {
	report.AddOperation(finalGateJ6StorageAuditOpName, map[string]any{
		finalGateJ6HotRowPathStringTablesInput:       []string{},
		finalGateJ6PathTextCopiesPerDirSnapshotInput: float64(1),
		finalGateJ6PathTextCatalogTableInput:         tableCatalog,
	}, []float64{1})
}

func finalGateAddOp(
	report *perfreport.Report,
	name string,
	inputs map[string]any,
	duration float64,
) {
	if inputs == nil {
		inputs = map[string]any{}
	}

	if _, ok := inputs[queryInputDirKey]; !ok {
		inputs[queryInputDirKey] = queryOpTestRootDir
	}

	if finalGateBoltComparableOp(name) {
		inputs[queryInputResultDigest] = "sha256:" + name
	}

	durations := []float64{duration, duration, duration, duration, duration}
	counts := []uint64{7, 7, 7, 7, 7}
	report.AddOperationWithCounters(name, inputs, durations, nil, nil, nil, counts)
}

func finalGateBoltComparableOp(name string) bool {
	return slices.Contains(finalGateBoltComparableOps(), name)
}

func finalGateFilteredWhereInputs(dir string, digest string) map[string]any {
	inputs := finalGateFilteredInputs()
	inputs[queryInputDirKey] = dir
	inputs[queryInputResultDigest] = digest

	return inputs
}

func finalGateFilteredInputs() map[string]any {
	return map[string]any{
		queryInputFilterGIDsKey:         []uint32{14976},
		queryInputFilterUIDsKey:         []uint32{20155},
		queryInputFilterFileTypeMaskKey: 32768,
	}
}

func finalGateTableStats(rows uint64) perfreport.TableStats {
	return finalGateTableStatsWithBaselines(rows, 35006, 35005)
}

func finalGateResultEquivalence() []FinalGateResultEquivalence {
	operations := []struct {
		name     string
		kind     string
		scenario string
	}{
		{queryOpTreeWhereName, finalGateEquivalenceResultSet, finalGateScenarioTreeWhereNoAncestor},
		{queryOpTreeWhereColdProviderName, finalGateEquivalenceResultSet, finalGateScenarioTreeWhereColdProvider},
		{queryOpTreeWhereColdProviderName, finalGateEquivalenceResultSet, finalGateScenarioTreeWhereFilteredProvider},
		{queryOpTreeDiskTreeColdProviderName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeBroad},
		{queryOpTreeDiskTreeEndName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeBroad},
		{queryOpTreeDiskTreeProviderUpdateName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeBroad},
		{queryOpTreeDiskTreeColdProviderName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeFiltered},
		{queryOpTreeDiskTreeEndName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeFiltered},
		{queryOpTreeDiskTreeProviderUpdateName, finalGateEquivalenceResultTree, finalGateScenarioDisktreeFiltered},
		{queryOpDirInfoBroadName, finalGateEquivalenceSummary, finalGateScenarioDirInfo},
		{queryOpDirInfoFilteredName, finalGateEquivalenceSummary, finalGateScenarioDirInfo},
		{queryOpDirInfosBroadName, finalGateEquivalenceSummary, finalGateScenarioDirInfos},
		{queryOpDirInfosFilteredName, finalGateEquivalenceSummary, finalGateScenarioDirInfos},
		{queryOpDirsHaveChildrenBroadName, finalGateEquivalenceBooleanMap, finalGateScenarioDirsHaveChildrenBroad},
		{queryOpDirsHaveChildrenFilteredName, finalGateEquivalenceBooleanMap, finalGateScenarioDirsHaveChildrenFiltered},
		{queryOpWhereWholeMountName, finalGateEquivalenceResultSet, finalGateScenarioWhereFrontierBroad},
		{queryOpWhereFilteredWholeMountName, finalGateEquivalenceResultSet, finalGateScenarioWhereFrontierFiltered},
		{queryOpFilesListDirName, finalGateEquivalenceResultSet, finalGateScenarioFiles},
		{queryOpFilesStatPathName, finalGateEquivalenceResultSet, finalGateScenarioFiles},
		{queryOpGlobCaseAName, finalGateEquivalenceResultSet, finalGateScenarioFiles},
		{queryOpGlobCaseBName, finalGateEquivalenceResultSet, finalGateScenarioFiles},
		{queryOpFindGlobExtensionDotfileName, finalGateEquivalenceResultSet, finalGateScenarioFiles},
		{queryOpVirtualChildrenName, finalGateEquivalenceSummary, finalGateScenarioVirtualAncestor},
		{queryOpVirtualDirInfoName, finalGateEquivalenceSummary, finalGateScenarioVirtualAncestor},
	}

	evidence := make([]FinalGateResultEquivalence, 0, len(operations))
	for _, op := range operations {
		evidence = append(evidence, FinalGateResultEquivalence{
			Root:              queryOpTestRootDir,
			Operation:         op.name,
			Kind:              op.kind,
			Scenario:          op.scenario,
			BaselineArtifact:  "baseline.json",
			CandidateArtifact: "schema-v1.json",
			BaselineDigest:    "sha256:match",
			CandidateDigest:   "sha256:match",
			MatchedInputs:     true,
			Passed:            true,
		})
	}

	return evidence
}

func finalGateAddE3RESTOps(report *perfreport.Report) {
	finalGateAddRESTTreeOp(report, "/", 900)
	finalGateAddRESTTreeOp(report, finalGateLustreRootDir, 400)
	finalGateAddRESTTreeOp(report, finalGateNFSRootDir, 400)
	finalGateAddRESTTreeOp(report, finalGateT283Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch120Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch122Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch127Dir, 400)
	finalGateAddRESTWhereOp(report, "/", 450, 7)
	finalGateAddT283RESTWhereOp(report)
	finalGateAddCLIWhereOp(report, finalGateT283Dir, 80, 7)
}

func finalGateAddD2AuthComparisonOps(report *perfreport.Report) {
	finalGateAddD2AuthComparisonOp(report, queryOpInfoName, map[string]any{
		queryInputResultDigest: "sha256:info",
		queryInputInfoCountFieldsKey: []string{
			"files",
			"directories",
			"mounts",
		},
	})

	finalGateAddD2AuthComparisonOp(report, queryOpPermissionCheckName, map[string]any{
		queryInputPermissionChecksKey: []string{
			queryPermissionCheckAnyInDir,
			queryPermissionCheckPath,
		},
		queryInputPermissionPathKey: queryOpTestRootDir + "sample.bam",
	})

	finalGateAddD2AuthComparisonOp(report, queryOpAuthTreeName, map[string]any{
		queryInputResultDigest: "sha256:auth-tree",
		queryInputNoAuthFlagsKey: map[string]bool{
			queryOpTestRootDir:   false,
			queryOpTestChildADir: false,
			queryOpTestChildBDir: true,
		},
	})

	finalGateAddD2AuthComparisonOp(report, queryOpAuthWhereRestrictedName, map[string]any{
		queryInputResultDigest:  "sha256:auth-where-restricted",
		queryInputFilterGIDsKey: []uint32{14976},
	})

	finalGateAddD2AuthComparisonOp(report, queryOpNoAuthWhereName, map[string]any{
		queryInputResultDigest: "sha256:noauth-where",
	})
}

func finalGateAddD2AuthComparisonOp(
	report *perfreport.Report,
	name string,
	inputs map[string]any,
) {
	inputs[queryInputDirKey] = queryOpTestRootDir
	inputs[queryInputStatusCodeKey] = 200
	inputs["status_codes"] = []uint64{200, 200, 200, 200, 200}
	inputs["json_bytes"] = []uint64{8192, 8192, 8192, 8192, 8192}
	inputs["gzip_bytes"] = []uint64{2048, 2048, 2048, 2048, 2048}
	inputs["cache_hits"] = []uint64{3, 3, 3, 3, 3}
	inputs["cache_misses"] = []uint64{1, 1, 1, 1, 1}

	finalGateAddOp(report, name, inputs, 10)
}

func finalGateAddD4Decisions(report *perfreport.Report) {
	p95ByPattern := map[string]float64{
		finalGateJ6D4PatternFilteredExact:    20,
		finalGateJ6D4PatternFilteredChildren: 30,
		finalGateJ6D4PatternFilteredSubtree:  180,
	}

	for _, spec := range queryD4DecisionSpecs() {
		measuredOps := slices.Clone(spec.operations)
		report.AddOperation(finalGateJ6D4DecisionOpName, map[string]any{
			finalGateJ6D4PatternInput:         spec.pattern,
			finalGateJ6D4MaterialisationInput: spec.materialisation,
			finalGateJ6D4DecisionInput:        finalGateJ6D4DecisionRetained,
			finalGateJ6D4CitationInput:        "query_report:" + strings.Join(measuredOps, ","),
			finalGateJ6D4MeasuredP95Input:     p95ByPattern[spec.pattern],
			finalGateJ6D4LatencyGateInput:     float64(500),
			"measured_operations":             measuredOps,
		}, []float64{p95ByPattern[spec.pattern]})
	}
}

func finalGateSetCandidateInput(
	evidence *FinalGateEvidence,
	name string,
	key string,
	value any,
) {
	finalGateMutateCandidateOp(evidence, name, func(op *perfreport.Operation) {
		op.Inputs[key] = value
	})
}

func finalGateMutateCandidateOp(
	evidence *FinalGateEvidence,
	name string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]
			if op.Name == name {
				mutate(op)

				return
			}
		}
	}
}

func finalGateAddRESTTreeOp(
	report *perfreport.Report,
	path string,
	duration float64,
) {
	inputs := finalGateRESTInputs(
		"/rest/v1/auth/tree",
		map[string]string{finalGateRESTParamPath: path},
		"sha256:rest-tree-"+path,
	)
	inputs["first_run_wall_ms"] = duration
	finalGateAddMeasuredOp(report, finalGateRESTOpTree, inputs, duration, 3)
}

func finalGateAddRESTWhereOp(
	report *perfreport.Report,
	dir string,
	duration float64,
	resultCount uint64,
) {
	inputs := finalGateRESTInputs(
		"/rest/v1/where",
		map[string]string{
			queryInputDirKey:         dir,
			finalGateRESTParamGroups: finalGateSelectedTreeGIDs,
			finalGateRESTParamUsers:  finalGateSelectedTreeUIDs,
			finalGateRESTParamTypes:  finalGateOtherType,
			queryInputSplitsKey:      "4",
		},
		"sha256:rest-where-"+dir,
	)
	inputs["first_run_wall_ms"] = duration
	finalGateAddMeasuredOp(report, finalGateRESTOpWhere, inputs, duration, resultCount)
}

func finalGateAddT283RESTWhereOp(report *perfreport.Report) {
	inputs := finalGateRESTInputs(
		"/rest/v1/where",
		map[string]string{
			queryInputDirKey:         finalGateT283Dir,
			finalGateRESTParamGroups: finalGateSelectedTreeGIDs,
			finalGateRESTParamUsers:  finalGateSelectedTreeUIDs,
			finalGateRESTParamTypes:  finalGateOtherType,
			queryInputSplitsKey:      "4",
		},
		finalGateT283Digest,
	)
	inputs["first_run_wall_ms"] = 80.0
	inputs["order"] = "direct"
	finalGateAddMeasuredOp(report, finalGateRESTOpWhere, inputs, 80, 87)
}

func finalGateAddCLIWhereOp(
	report *perfreport.Report,
	dir string,
	duration float64,
	resultCount uint64,
) {
	inputs := finalGateRESTInputs(
		"/rest/v1/where",
		map[string]string{
			queryInputDirKey:         dir,
			finalGateRESTParamGroups: finalGateSelectedTreeGIDs,
			finalGateRESTParamUsers:  finalGateSelectedTreeUIDs,
			finalGateRESTParamTypes:  finalGateOtherType,
			queryInputSplitsKey:      "4",
		},
		"sha256:cli-where-"+dir,
	)
	inputs["command"] = []string{
		finalGateWrstatUICommand,
		finalGateWhereCommandName,
		"--dir",
		dir,
		"--groups",
		finalGateSelectedTreeGIDs,
		"--users",
		finalGateSelectedTreeUIDs,
		"--types",
		finalGateOtherType,
		"--json",
	}
	inputs["first_run_wall_ms"] = duration
	finalGateAddMeasuredOp(report, finalGateRESTOpCLIWhere, inputs, duration, resultCount)
}

func finalGateRESTInputs(endpoint string, params map[string]string, digest string) map[string]any {
	return map[string]any{
		"endpoint":                    endpoint,
		finalGateRESTInputQueryParams: params,
		finalGateRESTInputStatusCodes: []uint64{200, 200, 200, 200, 200},
		finalGateRESTInputJSONBytes:   []uint64{8192, 8192, 8192, 8192, 8192},
		finalGateRESTInputGzipBytes:   []uint64{2048, 2048, 2048, 2048, 2048},
		finalGateRESTInputCacheHits:   []uint64{1, 1, 1, 1, 1},
		finalGateRESTInputCacheMisses: []uint64{1, 1, 1, 1, 1},
		queryInputResultDigest:        digest,
		queryInputCacheHitKeysKey:     []string{queryTestE2CacheHitKey},
		"query_count":                 []uint64{1, 1, 1, 1, 1},
		"query_count_source":          "test.system.events.Query_delta",
		"cache_counter_source":        "test.tree_query_cache_delta",
	}
}

func finalGateAddMeasuredOp(
	report *perfreport.Report,
	name string,
	inputs map[string]any,
	duration float64,
	resultCount uint64,
) {
	durations := []float64{duration, duration, duration, duration, duration}
	counts := []uint64{resultCount, resultCount, resultCount, resultCount, resultCount}
	report.AddOperationWithCounters(name, inputs, durations, nil, nil, nil, counts)
}

func finalGateMutateD4Decision(
	evidence *FinalGateEvidence,
	pattern string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.QueryReports {
		for opIndex := range evidence.QueryReports[reportIndex].Operations {
			op := &evidence.QueryReports[reportIndex].Operations[opIndex]

			matched := op.Name == finalGateJ6D4DecisionOpName &&
				stringInput(op.Inputs, finalGateJ6D4PatternInput) == pattern
			if !matched {
				continue
			}

			mutate(op)

			return
		}
	}
}

func finalGateDeleteQueryD4Decisions(evidence *FinalGateEvidence) {
	for reportIndex := range evidence.QueryReports {
		evidence.QueryReports[reportIndex].Operations = slices.DeleteFunc(
			evidence.QueryReports[reportIndex].Operations,
			func(op perfreport.Operation) bool {
				return op.Name == finalGateJ6D4DecisionOpName
			},
		)
	}
}

func finalGateDeleteReportInput(reports []perfreport.Report, name string, key string) {
	for reportIndex := range reports {
		for opIndex := range reports[reportIndex].Operations {
			op := &reports[reportIndex].Operations[opIndex]
			if op.Name == name {
				delete(op.Inputs, key)

				return
			}
		}
	}
}

func finalGateMutateBoltOp(
	evidence *FinalGateEvidence,
	name string,
	mutate func(*perfreport.Operation),
) {
	for reportIndex := range evidence.BoltQueryReports {
		for opIndex := range evidence.BoltQueryReports[reportIndex].Operations {
			op := &evidence.BoltQueryReports[reportIndex].Operations[opIndex]
			if op.Name == name {
				mutate(op)

				return
			}
		}
	}
}

func finalGateT283RESTReport(order string, cacheHits uint64, cacheHitKeys []string) perfreport.Report {
	report := perfreport.NewReport("clickhouse_rest", "", 1, 0)
	inputs := map[string]any{
		"order": order,
		finalGateRESTInputQueryParams: map[string]string{
			queryInputDirKey:         finalGateT283Dir,
			finalGateRESTParamGroups: finalGateSelectedTreeGIDs,
			finalGateRESTParamUsers:  finalGateSelectedTreeUIDs,
			finalGateRESTParamTypes:  finalGateOtherType,
		},
		finalGateRESTInputStatusCodes: []uint64{200, 200, 200, 200, 200},
		finalGateRESTInputCacheHits:   []uint64{cacheHits},
		finalGateRESTInputCacheMisses: []uint64{1 - cacheHits},
		"cache_counter_source":        "clickhouse.process_tree_query_cache_delta",
		queryInputResultDigest:        finalGateT283Digest,
		queryInputCacheHitKeysKey:     cacheHitKeys,
	}

	report.AddOperationWithCounters(
		"rest_where",
		inputs,
		[]float64{1, 1, 1, 1, 1},
		nil,
		nil,
		nil,
		[]uint64{87, 87, 87, 87, 87},
	)

	return report
}

func finalGateAssemblyCommand(root string, status string) []string {
	if status == finalGateComparisonInfeasible {
		return []string{finalGateE1OldWrstatUI, finalGateWhereCommandName}
	}

	return []string{finalGateE1WrstatUI, finalGateE1BoltPerfCommand, finalGateE1QueryCommand, root}
}

func finalGateOutputArtifactPath(path string, status string) string {
	if status == finalGateComparisonInfeasible {
		return ""
	}

	return path
}

func finalGateE1ArtifactClickHouseReport() perfreport.Report {
	report := finalGateE1ClickHouseReport()
	delete(report.Operations[0].Inputs, finalGateCorrectnessStatusInput)

	return report
}

func finalGateE1ArtifactRESTReport() perfreport.Report {
	report := finalGateE1RESTReport()
	for i := range report.Operations {
		delete(report.Operations[i].Inputs, finalGateCorrectnessStatusInput)
	}

	return report
}

func finalGateE1BoltArtifactReport() perfreport.Report {
	report := perfreport.NewReport("bolt", "/fixtures/mixed8", finalGateMinRepeats, 0)
	report.GitCommit = "bolt-artefact-revision"
	report.ToolVersion = "v1.2.3-e1"
	inputs := map[string]any{
		queryInputResultDigest:   finalGateE1Digest("comparison"),
		"schema3_fallback_count": []uint64{0, 1, 0, 0, 0},
	}
	report.AddOperationWithCounters(
		queryOpTreeWhereName,
		inputs,
		[]float64{1, 2, 3, 4, 5},
		nil,
		nil,
		nil,
		finalGateE1Counts(7),
	)

	return report
}

func finalGateWriteExpectedDigestManifest(
	t *testing.T,
	path string,
	key string,
	digest string,
) {
	t.Helper()

	data, err := json.Marshal(map[string]any{
		"expected_digests": map[string]string{key: digest},
	})
	So(err, ShouldBeNil)
	So(os.WriteFile(path, data, 0o600), ShouldBeNil)
}

func finalGateWritePerfReport(t *testing.T, path string, report perfreport.Report) {
	t.Helper()

	So(perfreport.WriteReport(path, report), ShouldBeNil)
}

func finalGateWriteFile(t *testing.T, path string, contents string) {
	t.Helper()

	So(os.WriteFile(path, []byte(contents), 0o600), ShouldBeNil)
}
