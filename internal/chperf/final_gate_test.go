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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

func TestValidateFinalGates(t *testing.T) {
	Convey("ValidateFinalGates covers all 15 E2 acceptance gates", t, func() {
		result := ValidateFinalGates(finalGateTestEvidence(false, false))

		So(result.Passed, ShouldBeTrue)
		So(result.Checks, ShouldHaveLength, 15)

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

	Convey("ValidateFinalGates enforces the accepted directory facts row baseline", t, func() {
		evidence := finalGateTestEvidence(false, false)
		evidence.ImportReports[0].TableStats[tableDirSummary] = finalGateTableStats(35007)

		result := ValidateFinalGates(evidence)

		rows, ok := finalGateNonOptionalRowBaseline(tableDirSummary)
		So(ok, ShouldBeTrue)
		So(rows, ShouldEqual, 35006)
		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
	})
}

func finalGateTestEvidence(ageAllSelected, virtualCacheSelected bool) FinalGateEvidence {
	return FinalGateEvidence{
		ImportReports:      finalGateImportReports(ageAllSelected, virtualCacheSelected),
		QueryReports:       []perfreport.Report{finalGateQueryReport()},
		RequiredQueryRoots: []string{queryOpTestRootDir},
		ResultEquivalence:  finalGateResultEquivalence(),
	}
}

func finalGateImportReports(ageAllSelected, virtualCacheSelected bool) []perfreport.Report {
	reports := make([]perfreport.Report, 0, finalGateMinRepeats)
	for range finalGateMinRepeats {
		reports = append(reports, finalGateImportReport(ageAllSelected, virtualCacheSelected))
	}

	return reports
}

func finalGateImportReport(ageAllSelected, virtualCacheSelected bool) perfreport.Report {
	report := perfreport.NewReport("clickhouse", "/home/ubuntu/output/nfs", 1, 0)
	report.MaxRSSBytes = 402_000 * 1024
	report.SelectedTables = []string{
		tableFiles,
		tableChildren,
		tableDirSummary,
		tableDirSummarySets,
	}
	report.TableStats = map[string]perfreport.TableStats{
		tableFiles:          finalGateTableStats(100000),
		tableChildren:       finalGateTableStats(35005),
		tableDirSummary:     finalGateTableStats(35006),
		tableDirSummarySets: finalGateTableStats(1),
	}
	report.AddOperation("import_total", map[string]any{"records": uint64(100000)}, []float64{4350})

	if ageAllSelected {
		report.SelectedTables = append(report.SelectedTables, tableDirFilterAgeAll)
		report.TableStats[tableDirFilterAgeAll] = finalGateTableStats(120)
	}

	if virtualCacheSelected {
		report.SelectedTables = append(report.SelectedTables, tableTreeDGUTA)
		report.TableStats[tableTreeDGUTA] = finalGateTableStats(50)
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

func finalGateQueryReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", finalGateMinRepeats, 1)

	finalGateAddOp(&report, queryOpTreeWhereName, nil, 8)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName, nil, 110)
	finalGateAddOp(&report, queryOpTreeWhereColdProviderName, finalGateFilteredInputs(), 100)
	finalGateAddOp(&report, queryOpTreeDiskTreeColdProviderName, nil, 100)
	finalGateAddOp(&report, queryOpTreeDiskTreeColdProviderName, finalGateFilteredInputs(), 90)
	finalGateAddOp(&report, queryOpTreeDiskTreeEndName, nil, 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeEndName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeProviderUpdateName, nil, 20)
	finalGateAddOp(&report, queryOpTreeDiskTreeProviderUpdateName, finalGateFilteredInputs(), 27)
	finalGateAddOp(&report, queryOpDirInfoBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirInfoFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpDirInfosBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirInfosFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpDirsHaveChildrenBroadName, nil, 1)
	finalGateAddOp(&report, queryOpDirsHaveChildrenFilteredName, finalGateFilteredInputs(), 1)
	finalGateAddOp(&report, queryOpTreeDiskTreeVisibleChildName, nil, 0.04)
	finalGateAddOp(&report, queryOpFilesListDirName, nil, 10)
	finalGateAddOp(&report, queryOpFilesStatPathName, nil, 10)
	finalGateAddOp(&report, queryOpGlobCaseAName, nil, 10)
	finalGateAddOp(&report, queryOpGlobCaseBName, nil, 10)
	finalGateAddOp(&report, queryOpFindGlobExtensionDotfileName, nil, 10)
	finalGateAddOp(&report, queryOpVirtualChildrenName, finalGateFilteredInputs(), 10)
	finalGateAddOp(&report, queryOpVirtualDirInfoName, finalGateFilteredInputs(), 10)

	return report
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

	durations := []float64{duration, duration, duration, duration, duration}
	counts := []uint64{7, 7, 7, 7, 7}
	report.AddOperationWithCounters(name, inputs, durations, nil, nil, nil, counts)
}

func finalGateFilteredInputs() map[string]any {
	return map[string]any{
		queryInputFilterGIDsKey:         []uint32{14976},
		queryInputFilterUIDsKey:         []uint32{20155},
		queryInputFilterFileTypeMaskKey: 32768,
	}
}

func finalGateTableStats(rows uint64) perfreport.TableStats {
	return perfreport.TableStats{
		Rows:              rows,
		ActiveParts:       1,
		CompressedBytes:   rows * 10,
		UncompressedBytes: rows * 20,
		ImportPhaseDurationsMS: map[string]float64{
			phaseDirProjectionWrite: 1,
		},
	}
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
