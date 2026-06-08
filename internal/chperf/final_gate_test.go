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
	"slices"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const finalGateT283Digest = "sha256:t283"

const finalGateOtherType = finalGateSelectedTreeTypes

func TestValidateFinalGates(t *testing.T) {
	Convey("ValidateFinalGates covers all 25 final acceptance gates", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(result.Checks, ShouldHaveLength, 25)

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
		finalGateSetCandidateInput(&evidence, queryOpAuthWhereRestrictedName, navigationInputResultDigest, "sha256:different")

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
			op.Inputs[navigationInputResultDigest] = "sha256:different"
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
			finalGateRESTPathPredicate("/nfs/"),
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
				evidence.ResultEquivalence[i].CandidateDigest = "sha256:different"
			}
		}

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(finalGateTestCheck(result, "E3 facts digest equivalence").Passed, ShouldBeFalse)
	})

	Convey("E3.7 final gate fails when new table stats lack memory and amplification evidence", t, func() {
		evidence := finalGateTestEvidence(false, false)
		stats := evidence.ImportReports[0].TableStats[tableParentFacts]
		stats.ImportMemoryBytes = 0
		stats.RowAmplificationVsDirFacts = 0
		stats.RowAmplificationVsChildren = 0
		evidence.ImportReports[0].TableStats[tableParentFacts] = stats

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
		finalGateSetCandidateInput(&evidence, queryOpInfoName, navigationInputResultDigest, "sha256:different")

		result := ValidateFinalGates(evidence)

		So(result.Passed, ShouldBeFalse)
		So(result.Checks[15].Passed, ShouldBeFalse)
		So(result.Checks[15].Detail, ShouldContainSubstring, queryOpInfoName)
	})
}

func finalGateTableStatsWithBaselines(
	rows uint64,
	dirFactsRows uint64,
	childrenRows uint64,
) perfreport.TableStats {
	return perfreport.TableStats{
		Rows:                       rows,
		ActiveParts:                1,
		CompressedBytes:            rows * 10,
		UncompressedBytes:          rows * 20,
		ImportMemoryBytes:          402_000 * 1024,
		RowAmplificationVsDirFacts: float64(rows) / float64(dirFactsRows),
		RowAmplificationVsChildren: float64(rows) / float64(childrenRows),
		ImportPhaseDurationsMS: map[string]float64{
			phaseDirProjectionWrite: 1,
		},
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
				tableChildren:   rowCap / 2,
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
	report.Backend = "bolt"
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
		for opIndex := range ops {
			if ops[opIndex].Name == name && (pred == nil || pred(ops[opIndex])) {
				evidence.QueryReports[reportIndex].Operations = slices.Delete(ops, opIndex, opIndex+1)

				return
			}
		}
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
	return FinalGateEvidence{
		ImportReports:        finalGateImportReports(ageAllSelected, virtualCacheSelected),
		QueryReports:         []perfreport.Report{finalGateQueryReport()},
		BaselineQueryReports: []perfreport.Report{finalGateQueryReport()},
		BoltQueryReports:     []perfreport.Report{finalGateBoltQueryReport()},
		RequiredQueryRoots:   []string{queryOpTestRootDir},
		ResultEquivalence:    finalGateResultEquivalence(),
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
	childrenRows := rowCap * rootCount * 35 / 100
	dirFactsRows := rowCap * rootCount * 35 / 100
	report.SelectedTables = []string{
		tableFiles,
		tableChildren,
		tableDirSummary,
		tableParentFacts,
		tableDirSummarySets,
		tableDirFilterAgeAll,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
	}
	report.TableStats = map[string]perfreport.TableStats{
		tableFiles:                    finalGateTableStatsWithBaselines(filesRows, dirFactsRows, childrenRows),
		tableChildren:                 finalGateTableStatsWithBaselines(childrenRows, dirFactsRows, childrenRows),
		tableDirSummary:               finalGateTableStatsWithBaselines(dirFactsRows, dirFactsRows, childrenRows),
		tableParentFacts:              finalGateTableStatsWithBaselines(rowCap, dirFactsRows, childrenRows),
		tableDirSummarySets:           finalGateTableStatsWithBaselines(1, dirFactsRows, childrenRows),
		tableDirFilterAgeAll:          finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, childrenRows),
		tableActivePrefixRollups:      finalGateTableStatsWithBaselines(3, dirFactsRows, childrenRows),
		tableActivePrefixFilterAgeAll: finalGateTableStatsWithBaselines(rowCap/3, dirFactsRows, childrenRows),
		tableActivePrefixRollupSets:   finalGateTableStatsWithBaselines(1, dirFactsRows, childrenRows),
	}
	finalGateAddImportFileTotals(&report, rowCap)
	report.AddOperation("import_total", map[string]any{
		importInputRecords: rowCap * rootCount,
		importInputRowCap:  rowCap,
	}, []float64{4350})

	if ageAllSelected {
		report.TableStats[tableDirFilterAgeAll] = finalGateTableStatsWithBaselines(rowCap/2, dirFactsRows, childrenRows)
	}

	if virtualCacheSelected {
		report.SelectedTables = append(report.SelectedTables, tableTreeDGUTA)
		report.TableStats[tableTreeDGUTA] = finalGateTableStatsWithBaselines(50, dirFactsRows, childrenRows)
	}

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
	op.Inputs[navigationInputResultDigest] = "sha256:info-counts"
}

func finalGateQueryReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", finalGateMinRepeats, 1)

	finalGateAddOp(&report, queryOpTreeWhereName, nil, 8)
	finalGateAddOp(&report, queryOpTreeWhereColdName, finalGateFilteredWhereInputs("/", "sha256:root-filtered-where"), 40)
	finalGateAddOp(&report, queryOpTreeWhereColdName,
		finalGateFilteredWhereInputs(finalGateT283Dir, "sha256:t283-filtered-where"), 80)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName, nil, 110)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName, finalGateFilteredInputs(), 100)
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
		inputs[navigationInputResultDigest] = "sha256:" + name
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
	inputs[navigationInputResultDigest] = digest

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
	finalGateAddRESTTreeOp(report, "/lustre/", 400)
	finalGateAddRESTTreeOp(report, "/nfs/", 400)
	finalGateAddRESTTreeOp(report, finalGateT283Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch120Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch122Dir, 400)
	finalGateAddRESTTreeOp(report, finalGateScratch127Dir, 400)
	finalGateAddRESTWhereOp(report, "/", 900, 7)
	finalGateAddT283RESTWhereOp(report)
	finalGateAddCLIWhereOp(report, finalGateT283Dir, 80, 7)
}

func finalGateAddD2AuthComparisonOps(report *perfreport.Report) {
	finalGateAddD2AuthComparisonOp(report, queryOpInfoName, map[string]any{
		navigationInputResultDigest: "sha256:info",
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
		navigationInputResultDigest: "sha256:auth-tree",
		queryInputNoAuthFlagsKey: map[string]bool{
			queryOpTestRootDir:   false,
			queryOpTestChildADir: false,
			queryOpTestChildBDir: true,
		},
	})

	finalGateAddD2AuthComparisonOp(report, queryOpAuthWhereRestrictedName, map[string]any{
		navigationInputResultDigest: "sha256:auth-where-restricted",
		queryInputFilterGIDsKey:     []uint32{14976},
	})

	finalGateAddD2AuthComparisonOp(report, queryOpNoAuthWhereName, map[string]any{
		navigationInputResultDigest: "sha256:noauth-where",
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
		"./wrstat-ui",
		"where",
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
		navigationInputResultDigest:   digest,
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
		navigationInputResultDigest:   finalGateT283Digest,
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
