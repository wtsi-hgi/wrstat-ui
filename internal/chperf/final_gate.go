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
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	finalGateMinRepeats          = 5
	finalGateT283ImportWallMS    = 4390
	finalGateT283ImportRSSBytes  = 425_000 * 1024
	finalGateSmallTreeWhereMaxMS = 8.8
	finalGateVisibleChildP50MS   = 0.05
	finalGateVisibleChildP99MS   = 0.1
	finalGateDisktreeAncestorMS  = 118.506

	finalGateEquivalenceBooleanMap = "boolean_map"
	finalGateEquivalenceResultSet  = "result_set"
	finalGateEquivalenceResultTree = "result_tree"
	finalGateEquivalenceSummary    = "summary"

	finalGateScenarioDisktreeBroad             = "disktree_broad"
	finalGateScenarioDisktreeFiltered          = "disktree_filtered"
	finalGateScenarioDirsHaveChildrenBroad     = "dirshavechildren_broad"
	finalGateScenarioDirsHaveChildrenFiltered  = "dirshavechildren_filtered"
	finalGateScenarioDirInfo                   = "dirinfo"
	finalGateScenarioDirInfos                  = "dirinfos"
	finalGateScenarioFiles                     = "files"
	finalGateScenarioTreeWhereColdProvider     = "tree_where_cold_provider"
	finalGateScenarioTreeWhereFilteredProvider = "tree_where_filtered_cold_provider"
	finalGateScenarioTreeWhereNoAncestor       = "tree_where_no_ancestor"
	finalGateScenarioVirtualAncestor           = "virtual_ancestor"
)

// FinalGateResultEquivalence captures paired current-branch/schema-v1 evidence.
type FinalGateResultEquivalence struct {
	Root              string `json:"root"`
	Operation         string `json:"operation"`
	Kind              string `json:"kind"`
	Scenario          string `json:"scenario,omitempty"`
	BaselineArtifact  string `json:"baseline_artefact"`
	CandidateArtifact string `json:"candidate_artefact"`
	BaselineDigest    string `json:"baseline_digest"`
	CandidateDigest   string `json:"candidate_digest"`
	MatchedInputs     bool   `json:"matched_inputs"`
	Passed            bool   `json:"passed"`
}

func operationEquivalencePasses(
	e FinalGateEvidence,
	operation string,
	kind string,
	scenario string,
) bool {
	if len(e.RequiredQueryRoots) == 0 {
		return slices.ContainsFunc(e.ResultEquivalence, func(eq FinalGateResultEquivalence) bool {
			return equivalenceMatches(eq, operation, kind, scenario)
		})
	}

	for _, root := range e.RequiredQueryRoots {
		if !rootEquivalencePasses(e.ResultEquivalence, root, operation, kind, scenario) {
			return false
		}
	}

	return true
}

func equivalenceIdentityMatches(
	eq FinalGateResultEquivalence,
	operation string,
	kind string,
	scenario string,
) bool {
	return eq.Operation == operation &&
		eq.Kind == kind &&
		eq.Scenario == scenario
}

func equivalenceArtifactsMatch(eq FinalGateResultEquivalence) bool {
	return eq.Passed &&
		eq.MatchedInputs &&
		eq.BaselineArtifact != "" &&
		eq.CandidateArtifact != "" &&
		eq.BaselineDigest != "" &&
		eq.BaselineDigest == eq.CandidateDigest
}

func rootEquivalencePasses(
	evidence []FinalGateResultEquivalence,
	root string,
	operation string,
	kind string,
	scenario string,
) bool {
	return slices.ContainsFunc(evidence, func(eq FinalGateResultEquivalence) bool {
		return pathWithinRoot(eq.Root, root) && equivalenceMatches(eq, operation, kind, scenario)
	})
}

func pathWithinRoot(path string, root string) bool {
	path = normalizeRootPath(path)

	root = normalizeRootPath(root)
	if path == "" || root == "" {
		return false
	}

	return path == root || strings.HasPrefix(path, root)
}

func normalizeRootPath(path string) string {
	if path == "" || path == "/" || strings.HasSuffix(path, "/") {
		return path
	}

	return path + "/"
}

func equivalenceMatches(
	eq FinalGateResultEquivalence,
	operation string,
	kind string,
	scenario string,
) bool {
	return equivalenceIdentityMatches(eq, operation, kind, scenario) &&
		equivalenceArtifactsMatch(eq)
}

// FinalGateEvidence contains the raw perf reports used for E2 validation.
type FinalGateEvidence struct {
	ImportReports      []perfreport.Report
	QueryReports       []perfreport.Report
	RequiredQueryRoots []string
	ResultEquivalence  []FinalGateResultEquivalence
}

func opsPass(e FinalGateEvidence, pred operationPredicate, gates map[string][2]float64) bool {
	for name, maxes := range gates {
		if !opPasses(e, name, pred, maxes[0], maxes[1]) {
			return false
		}
	}

	return true
}

func opPasses(
	e FinalGateEvidence,
	name string,
	pred operationPredicate,
	p95Max float64,
	p99Max float64,
) bool {
	ops := operationsInReports(e.QueryReports, name, pred)
	if len(ops) == 0 {
		return false
	}

	for _, op := range ops {
		if !operationPasses(op, p95Max, p99Max) {
			return false
		}
	}

	return requiredRootsCovered(e.RequiredQueryRoots, ops)
}

func operationsInReports(
	reports []perfreport.Report,
	name string,
	pred operationPredicate,
) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			ops = appendOperationIfMatch(ops, op, name, pred)
		}
	}

	return ops
}

func appendOperationIfMatch(
	ops []perfreport.Operation,
	op perfreport.Operation,
	name string,
	pred operationPredicate,
) []perfreport.Operation {
	if op.Name != name {
		return ops
	}

	if pred != nil && !pred(op) {
		return ops
	}

	return append(ops, op)
}

func operationPasses(op perfreport.Operation, p95Max float64, p99Max float64) bool {
	return len(op.DurationsMS) >= finalGateMinRepeats &&
		op.P95MS <= p95Max &&
		op.P99MS <= p99Max &&
		resultCountsStable(op)
}

func resultCountsStable(op perfreport.Operation) bool {
	if len(op.ResultCount) == 0 {
		return false
	}

	return !slices.ContainsFunc(op.ResultCount, func(count uint64) bool {
		return count != op.ResultCount[0]
	})
}

func requiredRootsCovered(requiredRoots []string, ops []perfreport.Operation) bool {
	if len(requiredRoots) == 0 || !hasNonRootPathEvidence(ops) {
		return true
	}

	for _, root := range requiredRoots {
		if !rootCovered(root, ops) {
			return false
		}
	}

	return true
}

func hasNonRootPathEvidence(ops []perfreport.Operation) bool {
	return slices.ContainsFunc(ops, func(op perfreport.Operation) bool {
		root := operationEvidenceRoot(op)

		return root != "" && root != "/"
	})
}

func operationEvidenceRoot(op perfreport.Operation) string {
	for _, key := range finalGateRootInputKeys() {
		if value := stringInput(op.Inputs, key); value != "" {
			return value
		}
	}

	return ""
}

func finalGateRootInputKeys() [4]string {
	return [4]string{
		queryInputDirKey,
		queryInputStartDirKey,
		queryInputParentDirKey,
		"path",
	}
}

func stringInput(inputs map[string]any, key string) string {
	v, ok := inputs[key]
	if !ok {
		return ""
	}

	typed, ok := v.(string)
	if !ok {
		return ""
	}

	return typed
}

func rootCovered(root string, ops []perfreport.Operation) bool {
	return slices.ContainsFunc(ops, func(op perfreport.Operation) bool {
		return pathWithinRoot(operationEvidenceRoot(op), root)
	})
}

func equivalencePasses(
	e FinalGateEvidence,
	operations []string,
	kind string,
	scenario string,
) bool {
	for _, operation := range operations {
		if !operationEquivalencePasses(e, operation, kind, scenario) {
			return false
		}
	}

	return true
}

// FinalGateCheck captures one E2 acceptance-test result.
type FinalGateCheck struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

func validateFinalGateImport(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(1, "t283 100k import")
	if len(e.ImportReports) < finalGateMinRepeats {
		return check.fail("need at least 5 import report repetitions")
	}

	for _, report := range e.ImportReports {
		if !importReportPasses(report) {
			return check.fail("import wall/RSS/table row gate failed")
		}
	}

	return check.pass("import wall, RSS, row amplification, and table rows passed")
}

func validateFinalGateTreeWhereSmall(e FinalGateEvidence) FinalGateCheck {
	return validateOpGate(e, 2, "t283 tree_where no ancestor", queryOpTreeWhereName,
		unfilteredOperation, finalGateSmallTreeWhereMaxMS, finalGateSmallTreeWhereMaxMS,
		finalGateEquivalenceResultSet, finalGateScenarioTreeWhereNoAncestor)
}

func validateFinalGateTreeWhereCold(e FinalGateEvidence) FinalGateCheck {
	return validateOpGate(e, 3, "cold-provider tree_where", queryOpTreeWhereColdProviderName,
		unfilteredOperation, 116.235, 119.260,
		finalGateEquivalenceResultSet, finalGateScenarioTreeWhereColdProvider)
}

func validateFinalGateFilteredTreeWhere(e FinalGateEvidence) FinalGateCheck {
	return validateOpGate(e, 4, "filtered cold-provider tree_where", queryOpTreeWhereColdProviderName,
		filteredOperation, 103.892, 104.945,
		finalGateEquivalenceResultSet, finalGateScenarioTreeWhereFilteredProvider)
}

func validateFinalGateBroadDisktree(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(5, "broad Disktree")

	gates := map[string][2]float64{
		queryOpTreeDiskTreeColdProviderName:   {104.636, 110.406},
		queryOpTreeDiskTreeEndName:            {1, 1},
		queryOpTreeDiskTreeProviderUpdateName: {22.019, 22.878},
	}

	if !opsPass(e, unfilteredOperation, gates) {
		return check.fail("broad Disktree latency or result-count gate failed")
	}

	if !equivalencePasses(e, mapKeys(gates), finalGateEquivalenceResultTree, finalGateScenarioDisktreeBroad) {
		return check.fail("broad Disktree paired result-tree equivalence evidence failed")
	}

	return check.pass("broad cold, warm, and provider-update Disktree gates passed")
}

func validateFinalGateFilteredDisktree(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(6, "filtered Disktree")

	gates := map[string][2]float64{
		queryOpTreeDiskTreeColdProviderName:   {91.216, 125.348},
		queryOpTreeDiskTreeEndName:            {1, 1},
		queryOpTreeDiskTreeProviderUpdateName: {28.319, 36.933},
	}

	if !opsPass(e, filteredOperation, gates) {
		return check.fail("filtered Disktree latency or result-count gate failed")
	}

	if !equivalencePasses(e, mapKeys(gates), finalGateEquivalenceResultTree, finalGateScenarioDisktreeFiltered) {
		return check.fail("filtered Disktree paired result-tree equivalence evidence failed")
	}

	return check.pass("filtered cold, warm, and provider-update Disktree gates passed")
}

func validateFinalGateAggregateDisktree(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(7, "aggregate Disktree")

	broadPassed := opPasses(e, queryOpTreeDiskTreeColdProviderName, unfilteredOperation, 104.636, 110.406)
	filteredPassed := opPasses(e, queryOpTreeDiskTreeColdProviderName, filteredOperation, 91.216, 125.348)

	if !broadPassed || !filteredPassed {
		return check.fail("aggregate broad or filtered Disktree gate failed")
	}

	return check.pass("documented broad and filtered aggregate Disktree gates passed")
}

func validateFinalGateDirInfo(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(8, "DirInfo")

	broadPassed := opsPass(e, unfilteredOperation, map[string][2]float64{queryOpDirInfoBroadName: {1, 1}})
	filteredPassed := opsPass(e, filteredOperation, map[string][2]float64{queryOpDirInfoFilteredName: {1, 1}})

	if !broadPassed || !filteredPassed {
		return check.fail("DirInfo broad or filtered gate failed")
	}

	dirInfoOps := []string{queryOpDirInfoBroadName, queryOpDirInfoFilteredName}
	if !equivalencePasses(e, dirInfoOps, finalGateEquivalenceSummary, finalGateScenarioDirInfo) {
		return check.fail("DirInfo paired summary equivalence evidence failed")
	}

	return check.pass("DirInfo latency and result-count gates passed")
}

func validateFinalGateDirInfos(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(9, "DirInfos")

	broadPassed := opsPass(e, unfilteredOperation, map[string][2]float64{queryOpDirInfosBroadName: {1, 1}})
	filteredPassed := opsPass(e, filteredOperation, map[string][2]float64{queryOpDirInfosFilteredName: {1, 1}})

	if !broadPassed || !filteredPassed {
		return check.fail("DirInfos broad or filtered gate failed")
	}

	dirInfosOps := []string{queryOpDirInfosBroadName, queryOpDirInfosFilteredName}
	if !equivalencePasses(e, dirInfosOps, finalGateEquivalenceSummary, finalGateScenarioDirInfos) {
		return check.fail("DirInfos paired summary equivalence evidence failed")
	}

	return check.pass("DirInfos latency and result-count gates passed")
}

func validateFinalGateBroadDirsHaveChildren(e FinalGateEvidence) FinalGateCheck {
	return validateOpGate(e, 10, "broad DirsHaveChildren", queryOpDirsHaveChildrenBroadName,
		unfilteredOperation, 1, 1, finalGateEquivalenceBooleanMap, finalGateScenarioDirsHaveChildrenBroad)
}

func validateFinalGateFilteredDirsHaveChildren(e FinalGateEvidence) FinalGateCheck {
	return validateOpGate(e, 11, "filtered DirsHaveChildren", queryOpDirsHaveChildrenFilteredName,
		filteredOperation, 1, 1, finalGateEquivalenceBooleanMap, finalGateScenarioDirsHaveChildrenFiltered)
}

func validateFinalGateVisibleChild(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(12, "visible-child")

	op, ok := firstOperationInReports(e.QueryReports, queryOpTreeDiskTreeVisibleChildName, nil)
	tooFewSamples := len(op.DurationsMS) < finalGateMinRepeats
	tooSlow := op.P50MS >= finalGateVisibleChildP50MS || op.P99MS >= finalGateVisibleChildP99MS

	if !ok || tooFewSamples || tooSlow {
		return check.fail("visible-child p50/p99 gate failed")
	}

	return check.pass("visible-child median and p99 gates passed")
}

func validateFinalGateFiles(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(13, "file list/stat/glob")

	gates := map[string][2]float64{
		queryOpFilesListDirName:             {14.3, 14.3},
		queryOpFilesStatPathName:            {14.3, 14.3},
		queryOpGlobCaseAName:                {14.3, 14.3},
		queryOpGlobCaseBName:                {14.3, 14.3},
		queryOpFindGlobExtensionDotfileName: {14.3, 14.3},
	}

	if !opsPass(e, unfilteredOperation, gates) {
		return check.fail("file list/stat/glob latency or result-count gate failed")
	}

	if !equivalencePasses(e, mapKeys(gates), finalGateEquivalenceResultSet, finalGateScenarioFiles) {
		return check.fail("file list/stat/glob paired result-set equivalence evidence failed")
	}

	return check.pass("file list/stat/glob gates passed")
}

func validateFinalGateVirtualSummary(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(14, "virtual ancestor summary")

	if selectedTable(e.ImportReports, tableTreeDGUTA) {
		if optionalTableStatsPass(e.ImportReports, tableTreeDGUTA) {
			return check.pass("selected virtual-summary cache has table evidence")
		}

		return check.fail("selected virtual-summary cache is missing table evidence")
	}

	gates := map[string][2]float64{
		queryOpVirtualChildrenName: {finalGateDisktreeAncestorMS, finalGateDisktreeAncestorMS},
		queryOpVirtualDirInfoName:  {finalGateDisktreeAncestorMS, finalGateDisktreeAncestorMS},
	}
	if !opsPass(e, filteredOperation, gates) {
		return check.fail("live virtual ancestor summary gate failed")
	}

	if !equivalencePasses(e, mapKeys(gates), finalGateEquivalenceSummary, finalGateScenarioVirtualAncestor) {
		return check.fail("virtual ancestor paired summary equivalence evidence failed")
	}

	return check.pass("live virtual ancestor composition passed without cache selection")
}

func validateFinalGateAgeAllOptional(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(15, "optional AgeAll index")
	if !selectedTable(e.ImportReports, tableDirFilterAgeAll) {
		return check.pass("AgeAll index not selected")
	}

	if !optionalTableStatsPass(e.ImportReports, tableDirFilterAgeAll) || !allImportReportsPass(e.ImportReports) {
		return check.fail("selected AgeAll index is missing table evidence or import gates failed")
	}

	return check.pass("selected AgeAll index table evidence and import caps passed")
}

func validateOpGate(
	e FinalGateEvidence,
	id int,
	name string,
	opName string,
	pred operationPredicate,
	p95Max float64,
	p99Max float64,
	equivalenceKind string,
	equivalenceScenario string,
) FinalGateCheck {
	check := finalGateCheck(id, name)
	if !opPasses(e, opName, pred, p95Max, p99Max) {
		return check.fail("latency or result-count gate failed")
	}

	if !equivalencePasses(e, []string{opName}, equivalenceKind, equivalenceScenario) {
		return check.fail("paired result equivalence evidence failed")
	}

	return check.pass("latency and result-count gates passed")
}

func finalGateCheck(id int, name string) FinalGateCheck {
	return FinalGateCheck{ID: id, Name: name}
}

func (c FinalGateCheck) pass(detail string) FinalGateCheck {
	c.Passed = true
	c.Detail = detail

	return c
}

func (c FinalGateCheck) fail(detail string) FinalGateCheck {
	c.Passed = false
	c.Detail = fmt.Sprintf("%s: %s", strings.TrimSpace(c.Name), detail)

	return c
}

// FinalGateResult is a facts-only summary of the E2 final perf gates.
type FinalGateResult struct {
	Passed bool             `json:"passed"`
	Checks []FinalGateCheck `json:"checks"`
}

// ValidateFinalGates evaluates the documented E2 perf gates from raw reports.
func ValidateFinalGates(e FinalGateEvidence) FinalGateResult {
	result := FinalGateResult{Passed: true}
	for _, check := range []FinalGateCheck{
		validateFinalGateImport(e),
		validateFinalGateTreeWhereSmall(e),
		validateFinalGateTreeWhereCold(e),
		validateFinalGateFilteredTreeWhere(e),
		validateFinalGateBroadDisktree(e),
		validateFinalGateFilteredDisktree(e),
		validateFinalGateAggregateDisktree(e),
		validateFinalGateDirInfo(e),
		validateFinalGateDirInfos(e),
		validateFinalGateBroadDirsHaveChildren(e),
		validateFinalGateFilteredDirsHaveChildren(e),
		validateFinalGateVisibleChild(e),
		validateFinalGateFiles(e),
		validateFinalGateVirtualSummary(e),
		validateFinalGateAgeAllOptional(e),
	} {
		result.Checks = append(result.Checks, check)
		result.Passed = result.Passed && check.Passed
	}

	return result
}

type operationPredicate func(perfreport.Operation) bool

func firstOperationInReports(
	reports []perfreport.Report,
	name string,
	pred operationPredicate,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		if op, ok := firstOperation(report, name, pred); ok {
			return op, true
		}
	}

	return perfreport.Operation{}, false
}

func firstOperation(
	report perfreport.Report,
	name string,
	pred operationPredicate,
) (perfreport.Operation, bool) {
	for _, op := range report.Operations {
		if op.Name != name {
			continue
		}

		if pred != nil && !pred(op) {
			continue
		}

		return op, true
	}

	return perfreport.Operation{}, false
}

func mapKeys(gates map[string][2]float64) []string {
	keys := make([]string, 0, len(gates))
	for key := range gates {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}

func unfilteredOperation(op perfreport.Operation) bool {
	return !filteredOperation(op)
}

func filteredOperation(op perfreport.Operation) bool {
	return uint64SliceInputLen(op.Inputs, queryInputFilterGIDsKey) > 0 ||
		uint64SliceInputLen(op.Inputs, queryInputFilterUIDsKey) > 0 ||
		uint64Input(op.Inputs, queryInputFilterFileTypeMaskKey) > 0
}

func uint64SliceInputLen(inputs map[string]any, key string) int {
	v, ok := inputs[key]
	if !ok || v == nil {
		return 0
	}

	switch typed := v.(type) {
	case []uint32:
		return len(typed)
	case []uint64:
		return len(typed)
	case []any:
		return len(typed)
	default:
		return 0
	}
}

func allImportReportsPass(reports []perfreport.Report) bool {
	if len(reports) < finalGateMinRepeats {
		return false
	}

	for _, report := range reports {
		if !importReportPasses(report) {
			return false
		}
	}

	return true
}

func importReportPasses(report perfreport.Report) bool {
	total, ok := firstOperation(report, "import_total", nil)
	if !ok || total.P50MS > finalGateT283ImportWallMS {
		return false
	}

	if report.MaxRSSBytes == 0 || report.MaxRSSBytes > finalGateT283ImportRSSBytes {
		return false
	}

	return importRowsPass(report) && importRowAmplificationPass(report, total)
}

func importRowsPass(report perfreport.Report) bool {
	for _, table := range finalGateNonOptionalTables() {
		baselineRows, _ := finalGateNonOptionalRowBaseline(table)

		stats, ok := report.TableStats[table]
		if !ok || stats.Rows > baselineRows {
			return false
		}
	}

	return true
}

func importRowAmplificationPass(report perfreport.Report, total perfreport.Operation) bool {
	records := uint64Input(total.Inputs, "records")
	if records == 0 {
		return false
	}

	var rows uint64
	for _, table := range finalGateNonOptionalTables() {
		rows += report.TableStats[table].Rows
	}

	return float64(rows)/float64(records) <= 1.70059
}

func uint64Input(inputs map[string]any, key string) uint64 {
	v, ok := inputs[key]
	if !ok {
		return 0
	}

	return uint64InputValue(v)
}

func uint64InputValue(v any) uint64 {
	switch typed := v.(type) {
	case uint64:
		return typed
	case uint32:
		return uint64(typed)
	case int:
		return positiveInt64(int64(typed))
	case int64:
		return positiveInt64(typed)
	case float64:
		return wholeFloat64(typed)
	default:
		return 0
	}
}

func positiveInt64(value int64) uint64 {
	if value <= 0 {
		return 0
	}

	return uint64(value)
}

func wholeFloat64(value float64) uint64 {
	if value <= 0 || math.Trunc(value) != value {
		return 0
	}

	return uint64(value)
}

func finalGateNonOptionalTables() []string {
	return []string{tableFiles, tableChildren, tableDirSummary, tableDirSummarySets}
}

func finalGateNonOptionalRowBaseline(table string) (uint64, bool) {
	switch table {
	case tableFiles:
		return 100000, true
	case tableChildren:
		return 35005, true
	case tableDirSummary:
		return 35006, true
	case tableDirSummarySets:
		return 1, true
	default:
		return 0, false
	}
}

func selectedTable(reports []perfreport.Report, table string) bool {
	for _, report := range reports {
		if slices.Contains(report.SelectedTables, table) {
			return true
		}
	}

	return false
}

func optionalTableStatsPass(reports []perfreport.Report, table string) bool {
	for _, report := range reports {
		if slices.Contains(report.SelectedTables, table) && tableStatsEvidencePass(report.TableStats[table]) {
			return true
		}
	}

	return false
}

func tableStatsEvidencePass(stats perfreport.TableStats) bool {
	return stats.Rows > 0 &&
		stats.ActiveParts > 0 &&
		stats.CompressedBytes > 0 &&
		stats.UncompressedBytes > 0 &&
		len(stats.ImportPhaseDurationsMS) > 0
}
