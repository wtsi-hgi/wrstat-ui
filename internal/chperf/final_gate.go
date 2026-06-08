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
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	finalGateMinRepeats          = 5
	finalGateBaselineMedianMS    = 5
	finalGatePermissionAuthRatio = 1.10
	finalGateNoisyRunRatio       = 1.10
	finalGateT283ImportWallMS    = 4390
	finalGateT283ImportRSSBytes  = 425_000 * 1024
	finalGateImportCap100K       = 100_000
	finalGateImportCap250K       = 250_000
	finalGateRootRefreshMaxMS    = 1000
	finalGateMountClickMaxMS     = 500
	finalGateRootFilterMaxMS     = 1000
	finalGateT283WhereMaxMS      = 86.388
	finalGateT283BoltP95MS       = 78.534
	finalGateRootWhereMaxMS      = 44.716
	finalGateRootBoltP95MS       = 40.651
	finalGateSmallTreeWhereMaxMS = 8.8
	finalGateVisibleChildP50MS   = 0.05
	finalGateVisibleChildP99MS   = 0.1
	finalGateDisktreeAncestorMS  = 118.506

	finalGateEquivalenceBooleanMap = "boolean_map"
	finalGateEquivalenceResultSet  = "result_set"
	finalGateEquivalenceResultTree = "result_tree"
	finalGateEquivalenceSummary    = "summary"

	finalGateResultCountMismatch  = "result-count mismatch"
	finalGateResultDigestMismatch = "result digest mismatch"

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
	finalGateScenarioWhereFrontierBroad        = "where_frontier_broad"
	finalGateScenarioWhereFrontierFiltered     = "where_frontier_filtered"

	finalGateRESTInputCacheHits   = "cache_hits"
	finalGateRESTInputCacheMisses = "cache_misses"
	finalGateRESTInputGzipBytes   = "gzip_bytes"
	finalGateRESTInputJSONBytes   = "json_bytes"
	finalGateRESTInputStatusCodes = "status_codes"
	finalGateRESTOpTree           = "rest_tree"
	finalGateRESTOpWhere          = "rest_where"
	finalGateRESTOpCLIWhere       = "cli_where"
	finalGateRESTParamGroups      = "groups"
	finalGateRESTParamPath        = "path"
	finalGateRESTParamTypes       = "types"
	finalGateRESTParamUsers       = "users"
	finalGateRESTInputQueryParams = "query_params"

	finalGateT283FilteredRESTOrderAnomaly = "t283_filtered_rest_order_anomaly"
	finalGateT283Dir                      = "/nfs/t283_imaging/"
	finalGateSelectedTreeGIDs             = "14976"
	finalGateSelectedTreeUIDs             = "20155"
	finalGateSelectedTreeTypes            = "other"
	finalGateScratch120Dir                = "/lustre/scratch120/"
	finalGateScratch122Dir                = "/lustre/scratch122/"
	finalGateScratch127Dir                = "/lustre/scratch127/"
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

// FinalGateT283FilteredRESTOrderEvidence captures the E2 REST order regression proof.
type FinalGateT283FilteredRESTOrderEvidence struct {
	DirectDigest       string
	WarmedDigest       string
	DirectResultCount  uint64
	WarmedResultCount  uint64
	WarmedCacheHitKeys []string
}

func deriveT283FilteredRESTOrderEvidence(
	reports []perfreport.Report,
) *FinalGateT283FilteredRESTOrderEvidence {
	direct, warmed := t283RESTOrderCandidates(reports)
	if direct == nil || warmed == nil {
		return nil
	}

	return &FinalGateT283FilteredRESTOrderEvidence{
		DirectDigest:       direct.digest,
		WarmedDigest:       warmed.digest,
		DirectResultCount:  direct.resultCount,
		WarmedResultCount:  warmed.resultCount,
		WarmedCacheHitKeys: warmed.cacheKeys,
	}
}

func t283FilteredRESTOrderEvidencePasses(order *FinalGateT283FilteredRESTOrderEvidence) bool {
	return order != nil &&
		t283RESTOrderDigestsMatch(order) &&
		t283RESTOrderCountsMatch(order) &&
		cacheHitKeysHaveE2Scope(order.WarmedCacheHitKeys)
}

func t283RESTOrderDigestsMatch(order *FinalGateT283FilteredRESTOrderEvidence) bool {
	return order.DirectDigest != "" && order.DirectDigest == order.WarmedDigest
}

func t283RESTOrderCountsMatch(order *FinalGateT283FilteredRESTOrderEvidence) bool {
	return order.DirectResultCount > 0 &&
		order.DirectResultCount == order.WarmedResultCount
}

// FinalGateEvidence contains the raw perf reports used for E2 validation.
type FinalGateEvidence struct {
	ImportReports         []perfreport.Report
	QueryReports          []perfreport.Report
	BaselineQueryReports  []perfreport.Report
	BoltQueryReports      []perfreport.Report
	RequiredQueryRoots    []string
	ResultEquivalence     []FinalGateResultEquivalence
	T283FilteredRESTOrder *FinalGateT283FilteredRESTOrderEvidence
}

func finalGateEvidenceWithDerivedT283(e FinalGateEvidence) FinalGateEvidence {
	if e.T283FilteredRESTOrder != nil {
		return e
	}

	e.T283FilteredRESTOrder = deriveT283FilteredRESTOrderEvidence(e.QueryReports)

	return e
}

func finalGatePermissionAuthOperationFailure(e FinalGateEvidence, name string) string {
	if len(e.RequiredQueryRoots) == 0 {
		return finalGatePermissionAuthPairFailure(e, name, "")
	}

	for _, root := range e.RequiredQueryRoots {
		if reason := finalGatePermissionAuthPairFailure(e, name, root); reason != "" {
			return fmt.Sprintf("%s %s", root, reason)
		}
	}

	return ""
}

func finalGatePermissionAuthPairFailure(
	e FinalGateEvidence,
	name string,
	root string,
) string {
	baseline, baselineOK := finalGateComparisonOperation(e.BaselineQueryReports, name, root)

	candidate, candidateOK := finalGateComparisonOperation(e.QueryReports, name, root)
	if !baselineOK || !candidateOK {
		return "missing baseline/candidate evidence"
	}

	return finalGateComparePermissionAuthOperation(name, baseline, candidate)
}

func finalGateComparisonOperation(
	reports []perfreport.Report,
	name string,
	root string,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		for _, op := range report.Operations {
			if !finalGateComparisonOperationMatches(op, name, root) {
				continue
			}

			return op, true
		}
	}

	return perfreport.Operation{}, false
}

func finalGateComparisonOperationMatches(
	op perfreport.Operation,
	name string,
	root string,
) bool {
	if op.Name != name {
		return false
	}

	return root == "" || pathWithinRoot(operationEvidenceRoot(op), root)
}

func queryParamInput(inputs map[string]any, key string) string {
	params, ok := inputs[finalGateRESTInputQueryParams]
	if !ok {
		return ""
	}

	switch typed := params.(type) {
	case map[string]string:
		return typed[key]
	case map[string]any:
		return stringInput(typed, key)
	default:
		return ""
	}
}

func finalGateComparePermissionAuthOperation(
	name string,
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) string {
	if !finalGateP95WithinPermissionAuthLimit(baseline, candidate) {
		return "p95 regression"
	}

	if !finalGateResultCountsMatch(baseline, candidate) {
		return finalGateResultCountMismatch
	}

	digestMismatch := finalGateOperationRequiresDigest(name, baseline, candidate) &&
		!finalGateStringInputMatches(baseline, candidate, navigationInputResultDigest)
	if digestMismatch {
		return finalGateResultDigestMismatch
	}

	noAuthMismatch := finalGateOperationRequiresNoAuthFlags(name, baseline, candidate) &&
		!finalGateInputEvidenceMatches(baseline, candidate, queryInputNoAuthFlagsKey)
	if noAuthMismatch {
		return "NoAuth flags mismatch"
	}

	return finalGateRESTEvidenceFailure(name, baseline, candidate)
}

func finalGateP95WithinPermissionAuthLimit(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	return len(baseline.DurationsMS) >= finalGateMinRepeats &&
		len(candidate.DurationsMS) >= finalGateMinRepeats &&
		baseline.P95MS > 0 &&
		candidate.P95MS > 0 &&
		candidate.P95MS <= baseline.P95MS*finalGatePermissionAuthRatio
}

func finalGateResultCountsMatch(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	if finalGateInfoCountVectorsMatch(baseline, candidate) {
		return true
	}

	return resultCountsStable(baseline) &&
		resultCountsStable(candidate) &&
		slices.Equal(baseline.ResultCount, candidate.ResultCount)
}

func finalGateInfoCountVectorsMatch(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	if !finalGateInfoCountFieldsPresent(baseline, candidate) {
		return false
	}

	return len(baseline.ResultCount) > 0 &&
		slices.Equal(baseline.ResultCount, candidate.ResultCount) &&
		finalGateInputEvidenceMatches(baseline, candidate, queryInputInfoCountFieldsKey)
}

func finalGateInfoCountFieldsPresent(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	return finalGateInputPresent(baseline, queryInputInfoCountFieldsKey) &&
		finalGateInputPresent(candidate, queryInputInfoCountFieldsKey)
}

func finalGateOperationRequiresDigest(
	name string,
	ops ...perfreport.Operation,
) bool {
	if finalGateAnyInputPresent([]string{navigationInputResultDigest}, ops...) {
		return true
	}

	return name == queryOpAuthTreeName ||
		name == queryOpInfoName ||
		name == queryOpAuthWhereRestrictedName ||
		name == queryOpNoAuthWhereName
}

func finalGateAnyInputPresent(keys []string, ops ...perfreport.Operation) bool {
	for _, op := range ops {
		for _, key := range keys {
			if finalGateInputPresent(op, key) {
				return true
			}
		}
	}

	return false
}

func finalGateInputPresent(op perfreport.Operation, key string) bool {
	_, ok := op.Inputs[key]

	return ok
}

func finalGateStringInputMatches(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	key string,
) bool {
	baselineValue := stringInput(baseline.Inputs, key)
	candidateValue := stringInput(candidate.Inputs, key)

	return baselineValue != "" && baselineValue == candidateValue
}

func finalGateOperationRequiresNoAuthFlags(
	name string,
	ops ...perfreport.Operation,
) bool {
	return name == queryOpAuthTreeName ||
		finalGateAnyInputPresent([]string{queryInputNoAuthFlagsKey}, ops...)
}

func finalGateInputEvidenceMatches(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	key string,
) bool {
	baselineValue, baselineOK := baseline.Inputs[key]

	candidateValue, candidateOK := candidate.Inputs[key]
	if !baselineOK || !candidateOK {
		return false
	}

	baselineJSON, baselineJSONOK := finalGateComparableJSON(baselineValue)
	candidateJSON, candidateJSONOK := finalGateComparableJSON(candidateValue)

	return baselineJSONOK && candidateJSONOK && baselineJSON == candidateJSON
}

func finalGateComparableJSON(value any) (string, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", false
	}

	return string(data), true
}

func finalGateRESTEvidenceFailure(
	name string,
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) string {
	restArrayEvidenceRequired := finalGateAnyRESTEvidencePresent(baseline, candidate)
	if !finalGateRESTEvidenceRequired(name, baseline, candidate) {
		return ""
	}

	if !finalGateStatusEvidenceMatches(baseline, candidate, restArrayEvidenceRequired) {
		return "status code mismatch"
	}

	if failure := finalGateRESTByteEvidenceFailure(
		baseline,
		candidate,
		restArrayEvidenceRequired,
	); failure != "" {
		return failure
	}

	return finalGateRESTCacheEvidenceFailure(baseline, candidate, restArrayEvidenceRequired)
}

func finalGateAnyRESTEvidencePresent(ops ...perfreport.Operation) bool {
	return finalGateAnyInputPresent(finalGateRESTArrayEvidenceKeys(), ops...)
}

func finalGateRESTArrayEvidenceKeys() []string {
	return []string{
		finalGateRESTInputStatusCodes,
		finalGateRESTInputJSONBytes,
		finalGateRESTInputGzipBytes,
		finalGateRESTInputCacheHits,
		finalGateRESTInputCacheMisses,
	}
}

func finalGateRESTEvidenceRequired(
	name string,
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	return finalGateOperationRequiresStatusEvidence(name) ||
		finalGateAnyRESTEvidencePresent(baseline, candidate)
}

func finalGateOperationRequiresStatusEvidence(name string) bool {
	return name == queryOpInfoName ||
		name == queryOpAuthTreeName ||
		name == queryOpAuthWhereRestrictedName ||
		name == queryOpNoAuthWhereName
}

func finalGateStatusEvidenceMatches(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	statusCodesRequired bool,
) bool {
	if statusCodesRequired {
		return finalGateInputEvidenceMatches(baseline, candidate, finalGateRESTInputStatusCodes)
	}

	matched := false

	for _, key := range []string{queryInputStatusCodeKey, finalGateRESTInputStatusCodes} {
		if !finalGateInputPresent(baseline, key) && !finalGateInputPresent(candidate, key) {
			continue
		}

		matched = true

		if !finalGateInputEvidenceMatches(baseline, candidate, key) {
			return false
		}
	}

	return matched
}

func finalGateRESTByteEvidenceFailure(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	restArrayEvidenceRequired bool,
) string {
	for _, key := range []string{finalGateRESTInputJSONBytes, finalGateRESTInputGzipBytes} {
		if finalGateOptionalRESTInputAbsent(restArrayEvidenceRequired, baseline, candidate, key) {
			continue
		}

		if !finalGateInputEvidenceMatches(baseline, candidate, key) {
			return finalGateRESTByteMismatch(key)
		}
	}

	return ""
}

func finalGateOptionalRESTInputAbsent(
	restArrayEvidenceRequired bool,
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	key string,
) bool {
	return !restArrayEvidenceRequired &&
		!finalGateInputPresent(baseline, key) &&
		!finalGateInputPresent(candidate, key)
}

func finalGateRESTByteMismatch(key string) string {
	if key == finalGateRESTInputJSONBytes {
		return "JSON byte mismatch"
	}

	return "gzip byte mismatch"
}

func finalGateRESTCacheEvidenceFailure(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
	restArrayEvidenceRequired bool,
) string {
	for _, key := range []string{finalGateRESTInputCacheHits, finalGateRESTInputCacheMisses} {
		if finalGateOptionalRESTInputAbsent(restArrayEvidenceRequired, baseline, candidate, key) {
			continue
		}

		if !finalGateInputEvidenceMatches(baseline, candidate, key) {
			return "cache counter mismatch"
		}
	}

	return ""
}

func finalGateBoltOperationFailure(e FinalGateEvidence, name string) string {
	if len(e.RequiredQueryRoots) == 0 {
		return finalGateBoltOperationRootFailure(e, name, "")
	}

	for _, root := range e.RequiredQueryRoots {
		if reason := finalGateBoltOperationRootFailure(e, name, root); reason != "" {
			return fmt.Sprintf("%s %s", root, reason)
		}
	}

	return ""
}

func finalGateBoltOperationRootFailure(
	e FinalGateEvidence,
	name string,
	root string,
) string {
	clickHouseOp, clickHouseOK := finalGateComparisonOperation(e.QueryReports, name, root)

	boltOp, boltOK := finalGateComparisonOperation(e.BoltQueryReports, name, root)
	if !clickHouseOK || !boltOK {
		return "missing ClickHouse/Bolt evidence"
	}

	if !finalGateResultCountsMatch(clickHouseOp, boltOp) {
		return finalGateResultCountMismatch
	}

	if !finalGateStringInputMatches(clickHouseOp, boltOp, navigationInputResultDigest) {
		return finalGateResultDigestMismatch
	}

	if !finalGateComparableRouteInputsMatch(clickHouseOp, boltOp) {
		return "matched input metadata mismatch"
	}

	return ""
}

func finalGateComparableRouteInputsMatch(a, b perfreport.Operation) bool {
	for _, key := range []string{
		queryInputDirKey,
		queryInputFilterGIDsKey,
		queryInputFilterUIDsKey,
		queryInputFilterFileTypeMaskKey,
		queryInputSplitsKey,
		"endpoint",
		finalGateRESTInputQueryParams,
	} {
		if finalGateInputPresent(a, key) || finalGateInputPresent(b, key) {
			if !finalGateInputEvidenceMatches(a, b, key) {
				return false
			}
		}
	}

	return true
}

func t283FilteredRESTOrderAnomalyFailure(e FinalGateEvidence) string {
	if !t283FilteredRESTOrderEvidencePasses(e.T283FilteredRESTOrder) {
		return finalGateT283FilteredRESTOrderAnomaly
	}

	return ""
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

	for _, key := range []string{finalGateRESTParamPath, queryInputDirKey} {
		if value := queryParamInput(op.Inputs, key); value != "" {
			return value
		}
	}

	return ""
}

func finalGateRootInputKeys() []string {
	return []string{
		queryInputDirKey,
		queryInputStartDirKey,
		queryInputParentDirKey,
		finalGateRESTParamPath,
		importInputMountPath,
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

func finalGateRESTP95Below(
	e FinalGateEvidence,
	name string,
	path string,
	p95Max float64,
) bool {
	op, ok := finalGateExactOperation(e.QueryReports, name, path, nil)

	return ok && operationP95BelowPasses(op, p95Max)
}

func finalGateExactOperation(
	reports []perfreport.Report,
	name string,
	root string,
	pred operationPredicate,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		for _, op := range report.Operations {
			if finalGateExactOperationMatches(op, name, root, pred) {
				return op, true
			}
		}
	}

	return perfreport.Operation{}, false
}

func finalGateExactOperationMatches(
	op perfreport.Operation,
	name string,
	root string,
	pred operationPredicate,
) bool {
	return op.Name == name &&
		operationRootEquals(op, root) &&
		(pred == nil || pred(op))
}

func operationRootEquals(op perfreport.Operation, root string) bool {
	return normalizeRootPath(operationEvidenceRoot(op)) == normalizeRootPath(root)
}

func operationP95BelowPasses(op perfreport.Operation, p95Max float64) bool {
	return len(op.DurationsMS) >= finalGateMinRepeats &&
		op.P95MS < p95Max &&
		resultCountsStable(op) &&
		finalGateStatusCodesPass(op)
}

func finalGateStatusCodesPass(op perfreport.Operation) bool {
	statusCodes := uint64SliceInput(op.Inputs, finalGateRESTInputStatusCodes)
	if len(statusCodes) == 0 {
		return true
	}

	return !slices.ContainsFunc(statusCodes, func(code uint64) bool {
		return code != 200
	})
}

func uint64SliceInput(inputs map[string]any, key string) []uint64 {
	v, ok := inputs[key]
	if !ok || v == nil {
		return nil
	}

	switch typed := v.(type) {
	case []uint32:
		out := make([]uint64, len(typed))
		for i, value := range typed {
			out[i] = uint64(value)
		}

		return out
	case []uint64:
		return slices.Clone(typed)
	case []any:
		return uint64AnySliceInput(typed)
	default:
		return nil
	}
}

func uint64AnySliceInput(values []any) []uint64 {
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if uintValue, ok := uint64SliceValue(value); ok {
			out = append(out, uintValue)
		}
	}

	return out
}

func uint64SliceValue(value any) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, true
	case uint32:
		return uint64(typed), true
	case int:
		return nonNegativeIntSliceValue(int64(typed))
	case int64:
		return nonNegativeIntSliceValue(typed)
	case float64:
		return wholeFloat64SliceValue(typed)
	default:
		return 0, false
	}
}

func nonNegativeIntSliceValue(value int64) (uint64, bool) {
	if value < 0 {
		return 0, false
	}

	return uint64(value), true
}

func wholeFloat64SliceValue(value float64) (uint64, bool) {
	if value < 0 || math.Trunc(value) != value {
		return 0, false
	}

	return uint64(value), true
}

func finalGateRESTWhereFilterP95Below(e FinalGateEvidence, dir string, p95Max float64) bool {
	op, ok := finalGateExactOperation(e.QueryReports, finalGateRESTOpWhere, dir, finalGateRESTWhereFilterPresent)

	return ok && operationP95BelowPasses(op, p95Max)
}

func finalGateT283RESTWhereP95Below(e FinalGateEvidence, p95Max float64) bool {
	ops := operationsInReports(e.QueryReports, finalGateRESTOpWhere, t283FilteredRESTWhereOperation)
	if len(ops) == 0 {
		return false
	}

	return !slices.ContainsFunc(ops, func(op perfreport.Operation) bool {
		return !restOperationP95StatusResultPasses(op, p95Max)
	})
}

func restOperationP95StatusResultPasses(op perfreport.Operation, p95Max float64) bool {
	return operationP95BelowPasses(op, p95Max) &&
		finalGateInputPresent(op, finalGateRESTInputStatusCodes)
}

func finalGateCLIWhereP95Below(e FinalGateEvidence, dir string, p95Max float64) bool {
	op, ok := finalGateExactOperation(e.QueryReports, finalGateRESTOpCLIWhere, dir, finalGateRESTWhereFilterPresent)

	return ok &&
		operationP95BelowPasses(op, p95Max) &&
		finalGateCLIWhereCommandPasses(op, dir)
}

func finalGateCLIWhereCommandPasses(op perfreport.Operation, dir string) bool {
	command := stringSliceInput(op.Inputs, "command")
	if len(command) == 0 {
		return false
	}

	return len(command) >= 4 &&
		command[0] == "./wrstat-ui" &&
		command[1] == "where" &&
		finalGateCommandFlagValue(command, "--dir") == dir &&
		slices.Contains(command, "--json")
}

func stringSliceInput(inputs map[string]any, key string) []string {
	v, ok := inputs[key]
	if !ok || v == nil {
		return nil
	}

	switch typed := v.(type) {
	case []string:
		return slices.Clone(typed)
	case []any:
		return stringAnySliceInput(typed)
	default:
		return nil
	}
}

func stringAnySliceInput(values []any) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if stringValue, ok := value.(string); ok {
			out = append(out, stringValue)
		}
	}

	return out
}

func finalGateCommandFlagValue(command []string, flag string) string {
	for i, value := range command {
		if value == flag && i+1 < len(command) {
			return command[i+1]
		}
	}

	return ""
}

func finalGateFilteredWhereVsBoltFailure(
	e FinalGateEvidence,
	dir string,
	maxP95MS float64,
	boltReferenceP95MS float64,
) string {
	clickHouseOp, boltOp, reason := finalGateFilteredWhereOps(e, dir)
	if reason != "" {
		return reason
	}

	if reason := finalGateBoltReferenceP95Failure(boltOp, boltReferenceP95MS); reason != "" {
		return reason
	}

	if reason := finalGateClickHouseWhereP95Failure(clickHouseOp, maxP95MS); reason != "" {
		return reason
	}

	return finalGateWhereBoltEvidenceFailure(clickHouseOp, boltOp)
}

func finalGateBoltReferenceP95Failure(op perfreport.Operation, referenceP95MS float64) string {
	if op.P95MS <= referenceP95MS {
		return ""
	}

	return "Bolt reference p95 evidence exceeded documented baseline"
}

func finalGateClickHouseWhereP95Failure(op perfreport.Operation, maxP95MS float64) string {
	if op.P95MS <= maxP95MS {
		return ""
	}

	return "ClickHouse p95 exceeded documented Bolt tolerance"
}

func finalGateWhereBoltEvidenceFailure(
	clickHouseOp perfreport.Operation,
	boltOp perfreport.Operation,
) string {
	if !finalGateResultCountsMatch(clickHouseOp, boltOp) {
		return finalGateResultCountMismatch
	}

	digestMismatch := finalGateAnyInputPresent([]string{navigationInputResultDigest}, clickHouseOp, boltOp) &&
		!finalGateStringInputMatches(clickHouseOp, boltOp, navigationInputResultDigest)
	if digestMismatch {
		return finalGateResultDigestMismatch
	}

	return ""
}

func finalGateFilteredWhereOps(
	e FinalGateEvidence,
	dir string,
) (perfreport.Operation, perfreport.Operation, string) {
	clickHouseOp, clickHouseOK := finalGateExactOperation(
		e.QueryReports,
		queryOpTreeWhereColdName,
		dir,
		filteredOperation,
	)

	boltOp, boltOK := finalGateExactOperation(
		e.BoltQueryReports,
		queryOpTreeWhereColdName,
		dir,
		filteredOperation,
	)
	if !clickHouseOK || !boltOK {
		return perfreport.Operation{}, perfreport.Operation{}, "missing ClickHouse/Bolt filtered where evidence"
	}

	return clickHouseOp, boltOp, ""
}

type t283RESTOrderCandidate struct {
	digest      string
	resultCount uint64
	cacheKeys   []string
	warmed      bool
}

func t283RESTOrderCandidates(
	reports []perfreport.Report,
) (*t283RESTOrderCandidate, *t283RESTOrderCandidate) {
	var pair t283RESTOrderCandidatePair

	for _, report := range reports {
		for _, op := range report.Operations {
			pair.add(report, op)
		}
	}

	return pair.direct, pair.warmed
}

func t283RESTOrderCandidateFromOperation(
	report perfreport.Report,
	op perfreport.Operation,
) (t283RESTOrderCandidate, bool) {
	if !t283FilteredRESTWhereOperation(op) {
		return t283RESTOrderCandidate{}, false
	}

	return t283RESTOrderCandidate{
		digest:      stringInput(op.Inputs, navigationInputResultDigest),
		resultCount: firstResultCount(op),
		cacheKeys:   stringSliceInput(op.Inputs, queryInputCacheHitKeysKey),
		warmed:      t283RESTOperationWarmed(report, op),
	}, true
}

type t283RESTOrderCandidatePair struct {
	direct *t283RESTOrderCandidate
	warmed *t283RESTOrderCandidate
}

func (p *t283RESTOrderCandidatePair) add(report perfreport.Report, op perfreport.Operation) {
	candidate, ok := t283RESTOrderCandidateFromOperation(report, op)
	if !ok {
		return
	}

	if candidate.warmed {
		p.setWarmed(candidate)

		return
	}

	p.setDirect(candidate)
}

func (p *t283RESTOrderCandidatePair) setWarmed(candidate t283RESTOrderCandidate) {
	if p.warmed == nil {
		p.warmed = &candidate
	}
}

func (p *t283RESTOrderCandidatePair) setDirect(candidate t283RESTOrderCandidate) {
	if p.direct == nil {
		p.direct = &candidate
	}
}

// FinalGateCheck captures one E2 acceptance-test result.
type FinalGateCheck struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

func validateFinalGateImport(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(1, "E3 capped import coverage")
	if reason := finalGateImportCoverageFailure(e.ImportReports); reason != "" {
		return check.fail(reason)
	}

	return check.pass("100k and 250k import reports cover required roots, caps, RSS, rows, and amplification")
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
	if reason := t283FilteredRESTOrderAnomalyFailure(e); reason != "" {
		return finalGateCheck(4, "filtered cold-provider tree_where").fail(reason)
	}

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
	if reason := t283FilteredRESTOrderAnomalyFailure(e); reason != "" {
		return check.fail(reason)
	}

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

func validateFinalGatePermissionAuthComparison(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(16, "permission/auth baseline comparison")
	if len(e.BaselineQueryReports) == 0 {
		return check.fail("missing baseline query reports")
	}

	if len(e.QueryReports) == 0 {
		return check.fail("missing candidate query reports")
	}

	for _, name := range finalGatePermissionAuthOps() {
		if reason := finalGatePermissionAuthOperationFailure(e, name); reason != "" {
			return check.fail(fmt.Sprintf("%s %s", name, reason))
		}
	}

	return check.pass("permission/auth baseline comparison passed")
}

func validateFinalGateBoltQueryComparison(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(17, "Bolt query report comparison")
	if len(e.BoltQueryReports) == 0 {
		return check.fail("missing Bolt query reports")
	}

	if len(e.QueryReports) == 0 {
		return check.fail("missing ClickHouse query reports")
	}

	for _, name := range finalGateBoltComparableOps() {
		if reason := finalGateBoltOperationFailure(e, name); reason != "" {
			return check.fail(fmt.Sprintf("%s %s", name, reason))
		}
	}

	return check.pass("Bolt query reports include comparable result counts and digests")
}

func validateFinalGateE3RootRefresh(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(18, "E3 root refresh")
	if !finalGateRESTP95Below(e, finalGateRESTOpTree, "/", finalGateRootRefreshMaxMS) {
		return check.fail("first root page refresh server-side p95 gate failed")
	}

	return check.pass("first root page refresh p95 passed")
}

func validateFinalGateE3MountClicks(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(19, "E3 first mount clicks")

	for _, path := range finalGateE3RESTTreeClickPaths() {
		if !finalGateRESTP95Below(e, finalGateRESTOpTree, path, finalGateMountClickMaxMS) {
			return check.fail(path + " first-click server-side p95 gate failed")
		}
	}

	return check.pass("lustre, nfs, selected mount-root, and representative scratch first-click p95 gates passed")
}

func validateFinalGateE3RootFilterSwitch(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(20, "E3 root filter switch")
	if !finalGateRESTWhereFilterP95Below(e, "/", finalGateRootFilterMaxMS) {
		return check.fail("first root filter switch server-side p95 gate failed")
	}

	return check.pass("first root filter switch p95 passed")
}

func validateFinalGateE3T283WhereVsBolt(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(21, "E3 t283 where vs Bolt")
	if !finalGateT283RESTWhereP95Below(e, finalGateT283WhereMaxMS) {
		return check.fail("t283 rest_where p95, status, or result-count gate failed")
	}

	if !finalGateCLIWhereP95Below(e, finalGateT283Dir, finalGateT283WhereMaxMS) {
		return check.fail("t283 cli_where p95, command, or result-count gate failed")
	}

	if reason := finalGateFilteredWhereVsBoltFailure(
		e,
		finalGateT283Dir,
		finalGateT283WhereMaxMS,
		finalGateT283BoltP95MS,
	); reason != "" {
		return check.fail(reason)
	}

	return check.pass("t283 filtered where p95 passed ClickHouse/Bolt gate")
}

func validateFinalGateE3RootWhereVsBolt(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(22, "E3 root where vs Bolt")
	if reason := finalGateFilteredWhereVsBoltFailure(
		e,
		"/",
		finalGateRootWhereMaxMS,
		finalGateRootBoltP95MS,
	); reason != "" {
		return check.fail(reason)
	}

	return check.pass("root filtered where p95 passed ClickHouse/Bolt gate")
}

func validateFinalGateE3FactsDigestEquivalence(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(23, "E3 facts digest equivalence")

	summaryDigestsPass := equivalencePasses(e, []string{
		queryOpDirInfoBroadName,
		queryOpDirInfoFilteredName,
	}, finalGateEquivalenceSummary, finalGateScenarioDirInfo)
	if !summaryDigestsPass {
		return check.fail("broad/filtered summary facts digests failed")
	}

	childSummaryDigestsPass := equivalencePasses(e, []string{
		queryOpDirInfosBroadName,
		queryOpDirInfosFilteredName,
	}, finalGateEquivalenceSummary, finalGateScenarioDirInfos)
	if !childSummaryDigestsPass {
		return check.fail("child summary facts digests failed")
	}

	hasChildrenDigestsPass := operationEquivalencePasses(
		e,
		queryOpDirsHaveChildrenBroadName,
		finalGateEquivalenceBooleanMap,
		finalGateScenarioDirsHaveChildrenBroad,
	) && operationEquivalencePasses(
		e,
		queryOpDirsHaveChildrenFilteredName,
		finalGateEquivalenceBooleanMap,
		finalGateScenarioDirsHaveChildrenFiltered,
	)
	if !hasChildrenDigestsPass {
		return check.fail("has_children facts digests failed")
	}

	whereDigestsPass := operationEquivalencePasses(
		e,
		queryOpWhereWholeMountName,
		finalGateEquivalenceResultSet,
		finalGateScenarioWhereFrontierBroad,
	) && operationEquivalencePasses(
		e,
		queryOpWhereFilteredWholeMountName,
		finalGateEquivalenceResultSet,
		finalGateScenarioWhereFrontierFiltered,
	)
	if !whereDigestsPass {
		return check.fail("where frontier facts digests failed")
	}

	return check.pass("summary, child, has_children, and where-frontier facts digests passed")
}

func validateFinalGateE3TableStatsRowAmplification(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(24, "E3 table stats row amplification evidence")

	for _, report := range e.ImportReports {
		if !finalGateNewObjectTableStatsPass(report) {
			return check.fail("new table stats missing rows, bytes, phase, memory, or amplification evidence")
		}
	}

	return check.pass("new table stats include rows, parts, bytes, phase, memory, and amplification evidence")
}

func validateFinalGateE3BaselineRegression(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(25, "E3 non-targeted baseline regression")
	if len(e.BaselineQueryReports) == 0 || len(e.QueryReports) == 0 {
		return check.fail("missing baseline or candidate query reports")
	}

	for _, candidate := range finalGateBaselineRegressionCandidates(e.QueryReports) {
		baseline, ok := finalGateMatchingBaselineOperation(e.BaselineQueryReports, candidate)
		if !ok {
			return check.fail(candidate.Name + " missing baseline evidence")
		}

		if reason := finalGateBaselineRegressionFailure(baseline, candidate); reason != "" {
			return check.fail(candidate.Name + " " + reason)
		}
	}

	return check.pass("non-targeted p95/p99 baseline comparison passed")
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
	e = finalGateEvidenceWithDerivedT283(e)

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
		validateFinalGatePermissionAuthComparison(e),
		validateFinalGateBoltQueryComparison(e),
		validateFinalGateE3RootRefresh(e),
		validateFinalGateE3MountClicks(e),
		validateFinalGateE3RootFilterSwitch(e),
		validateFinalGateE3T283WhereVsBolt(e),
		validateFinalGateE3RootWhereVsBolt(e),
		validateFinalGateE3FactsDigestEquivalence(e),
		validateFinalGateE3TableStatsRowAmplification(e),
		validateFinalGateE3BaselineRegression(e),
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

func finalGateBaselineRegressionCandidates(reports []perfreport.Report) []perfreport.Operation {
	var candidates []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if !finalGateBaselineRegressionIgnored(op) {
				candidates = append(candidates, op)
			}
		}
	}

	return candidates
}

func finalGateBaselineRegressionIgnored(op perfreport.Operation) bool {
	if slices.Contains(finalGatePermissionAuthOps(), op.Name) {
		return true
	}

	return finalGateTargetedRESTOperation(op) || finalGateTargetedFilteredWhereOperation(op)
}

func finalGatePermissionAuthOps() []string {
	return []string{
		queryOpInfoName,
		queryOpPermissionCheckName,
		queryOpAuthTreeName,
		queryOpAuthWhereRestrictedName,
		queryOpNoAuthWhereName,
	}
}

func finalGateTargetedRESTOperation(op perfreport.Operation) bool {
	switch op.Name {
	case finalGateRESTOpTree:
		return finalGateTargetedRESTTreeOperation(op)
	case finalGateRESTOpWhere:
		return finalGateTargetedRESTWhereOperation(op)
	case finalGateRESTOpCLIWhere:
		return finalGateTargetedCLIWhereOperation(op)
	default:
		return false
	}
}

func finalGateTargetedRESTTreeOperation(op perfreport.Operation) bool {
	if operationRootEquals(op, "/") {
		return true
	}

	return slices.ContainsFunc(finalGateE3RESTTreeClickPaths(), func(path string) bool {
		return operationRootEquals(op, path)
	})
}

func finalGateE3RESTTreeClickPaths() []string {
	return []string{
		"/lustre/",
		"/nfs/",
		finalGateT283Dir,
		finalGateScratch120Dir,
		finalGateScratch122Dir,
		finalGateScratch127Dir,
	}
}

func finalGateTargetedRESTWhereOperation(op perfreport.Operation) bool {
	return operationRootEquals(op, "/") || t283FilteredRESTWhereOperation(op)
}

func t283FilteredRESTWhereOperation(op perfreport.Operation) bool {
	return op.Name == "rest_where" &&
		normalizeRootPath(queryParamInput(op.Inputs, queryInputDirKey)) == finalGateT283Dir &&
		t283RESTWhereFilterPresent(op.Inputs)
}

func t283RESTWhereFilterPresent(inputs map[string]any) bool {
	return queryParamInput(inputs, finalGateRESTParamGroups) == finalGateSelectedTreeGIDs &&
		queryParamInput(inputs, finalGateRESTParamUsers) == finalGateSelectedTreeUIDs &&
		queryParamInput(inputs, finalGateRESTParamTypes) == finalGateSelectedTreeTypes
}

func finalGateTargetedCLIWhereOperation(op perfreport.Operation) bool {
	return operationRootEquals(op, finalGateT283Dir) && finalGateRESTWhereFilterPresent(op)
}

func finalGateRESTWhereFilterPresent(op perfreport.Operation) bool {
	for _, key := range []string{finalGateRESTParamGroups, finalGateRESTParamUsers, finalGateRESTParamTypes} {
		if queryParamInput(op.Inputs, key) != "" {
			return true
		}
	}

	return filteredOperation(op)
}

func finalGateTargetedFilteredWhereOperation(op perfreport.Operation) bool {
	return op.Name == queryOpTreeWhereColdName &&
		filteredOperation(op) &&
		(operationRootEquals(op, "/") || operationRootEquals(op, finalGateT283Dir))
}

func finalGateImportCoverageFailure(reports []perfreport.Report) string {
	for _, capRows := range finalGateRequiredImportCaps() {
		group := finalGateImportReportsForCap(reports, capRows)
		if len(group) < finalGateMinRepeats {
			return fmt.Sprintf("need at least %d %dk import report repetitions", finalGateMinRepeats, capRows/1000)
		}

		for _, report := range group {
			if reason := importReportFailure(report, capRows); reason != "" {
				return fmt.Sprintf("%dk import report failed: %s", capRows/1000, reason)
			}
		}
	}

	return ""
}

func finalGateRequiredImportCaps() []uint64 {
	return []uint64{finalGateImportCap100K, finalGateImportCap250K}
}

func finalGateImportReportsForCap(reports []perfreport.Report, capRows uint64) []perfreport.Report {
	var matches []perfreport.Report

	for _, report := range reports {
		if finalGateImportReportCap(report) == capRows {
			matches = append(matches, report)
		}
	}

	return matches
}

func finalGateImportReportCap(report perfreport.Report) uint64 {
	total, ok := firstOperation(report, "import_total", nil)
	if ok {
		return uint64Input(total.Inputs, importInputRowCap)
	}

	return 0
}

func importReportFailure(report perfreport.Report, capRows uint64) string {
	total, ok := firstOperation(report, "import_total", nil)
	if reason := importReportRuntimeFailure(report, total, ok); reason != "" {
		return reason
	}

	if reason := importDatasetCoverageFailure(report, capRows, total); reason != "" {
		return reason
	}

	rootCount := uint64(len(importFileTotalOperations(report)))
	if !importRowsPass(report, capRows, rootCount) {
		return "table row cap gate failed"
	}

	if !importRowAmplificationPass(report, total) {
		return "row amplification gate failed"
	}

	return ""
}

func importReportRuntimeFailure(report perfreport.Report, total perfreport.Operation, found bool) string {
	if !found || total.P50MS > finalGateT283ImportWallMS {
		return "import wall gate failed"
	}

	if report.MaxRSSBytes == 0 || report.MaxRSSBytes > finalGateT283ImportRSSBytes {
		return "import RSS gate failed"
	}

	return ""
}

func importDatasetCoverageFailure(
	report perfreport.Report,
	capRows uint64,
	total perfreport.Operation,
) string {
	if uint64Input(total.Inputs, importInputRowCap) != capRows {
		return "import_total row_cap mismatch"
	}

	ops := importFileTotalOperations(report)
	if len(ops) == 0 {
		return "missing import_file_total dataset evidence"
	}

	if uint64Input(total.Inputs, importInputRecords) > capRows*uint64(len(ops)) {
		return "import_total records exceeded row cap"
	}

	if reason := importDatasetRowsFailure(ops, capRows); reason != "" {
		return reason
	}

	if !finalGateImportRootsCovered(ops) {
		return "missing required NFS/Lustre import roots"
	}

	return ""
}

func importFileTotalOperations(report perfreport.Report) []perfreport.Operation {
	return operationsInReports([]perfreport.Report{report}, "import_file_total", nil)
}

func importDatasetRowsFailure(ops []perfreport.Operation, capRows uint64) string {
	for _, op := range ops {
		if uint64Input(op.Inputs, importInputRowCap) != capRows {
			return "dataset row_cap mismatch"
		}

		lines := uint64Input(op.Inputs, "lines")
		if lines == 0 || lines > capRows {
			return "dataset lines exceeded row cap"
		}

		rows := uint64MapInput(op.Inputs, "rows_per_table")
		if rows[tableFiles] > capRows {
			return "dataset wrstat_files rows exceeded row cap"
		}
	}

	return ""
}

func uint64MapInput(inputs map[string]any, key string) map[string]uint64 {
	v, ok := inputs[key]
	if !ok || v == nil {
		return map[string]uint64{}
	}

	switch typed := v.(type) {
	case map[string]uint64:
		return mapsCloneUint64(typed)
	case map[string]any:
		return uint64AnyMapInput(typed)
	default:
		return map[string]uint64{}
	}
}

func mapsCloneUint64(src map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(src))
	for key, value := range src {
		out[key] = value
	}

	return out
}

func uint64AnyMapInput(src map[string]any) map[string]uint64 {
	out := make(map[string]uint64, len(src))
	for key, value := range src {
		if uintValue := uint64InputValue(value); uintValue > 0 {
			out[key] = uintValue
		}
	}

	return out
}

func finalGateImportRootsCovered(ops []perfreport.Operation) bool {
	for _, root := range finalGateRequiredImportRoots() {
		if !rootCovered(root, ops) {
			return false
		}
	}

	return true
}

func finalGateRequiredImportRoots() []string {
	return []string{finalGateT283Dir, finalGateScratch120Dir, finalGateScratch122Dir, finalGateScratch127Dir}
}

func finalGateNonOptionalRowCap(table string, capRows uint64, rootCount uint64) (uint64, bool) {
	switch table {
	case tableFiles:
		return capRows * rootCount, true
	case tableChildren:
		return capRows * rootCount, true
	case tableDirSummary:
		return capRows * rootCount, true
	case tableDirSummarySets:
		return 1, true
	default:
		return 0, false
	}
}

func finalGateNewObjectTableStatsPass(report perfreport.Report) bool {
	if report.TableStats[tableDirSummary].Rows == 0 || report.TableStats[tableChildren].Rows == 0 {
		return false
	}

	for _, table := range finalGateNewObjectTables(report) {
		if !tableStatsEvidencePass(report.TableStats[table]) {
			return false
		}
	}

	return true
}

func finalGateNewObjectTables(report perfreport.Report) []string {
	tables := []string{
		tableParentFacts,
		tableDirSummarySets,
		tableDirFilterAgeAll,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
	}

	for _, optional := range []string{tableTreeDGUTA} {
		if slices.Contains(report.SelectedTables, optional) {
			tables = append(tables, optional)
		}
	}

	return tables
}

func tableStatsSizeEvidencePass(stats perfreport.TableStats) bool {
	return stats.Rows > 0 &&
		stats.ActiveParts > 0 &&
		stats.CompressedBytes > 0 &&
		stats.UncompressedBytes > 0 &&
		len(stats.ImportPhaseDurationsMS) > 0
}

func tableStatsDerivedEvidencePass(stats perfreport.TableStats) bool {
	return stats.ImportMemoryBytes > 0 &&
		stats.RowAmplificationVsDirFacts > 0 &&
		stats.RowAmplificationVsChildren > 0
}

func finalGateMatchingBaselineOperation(
	reports []perfreport.Report,
	candidate perfreport.Operation,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		for _, baseline := range report.Operations {
			if finalGateBaselineOperationMatches(baseline, candidate) {
				return baseline, true
			}
		}
	}

	return perfreport.Operation{}, false
}

func finalGateBaselineOperationMatches(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	return baseline.Name == candidate.Name &&
		operationRootEquals(baseline, operationEvidenceRoot(candidate)) &&
		finalGateComparableRouteInputsMatch(baseline, candidate)
}

func finalGateBaselineRegressionFailure(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) string {
	if !finalGateBaselineStatsPresent(baseline, candidate) {
		return "missing p50/p95/p99 evidence"
	}

	p95Worse := candidate.P95MS > baseline.P95MS
	p99Worse := candidate.P99MS > baseline.P99MS

	if !p95Worse && !p99Worse {
		return ""
	}

	if finalGateNoisyRunRegressionAllowed(baseline, candidate) {
		return ""
	}

	return "p95/p99 regression exceeded current ClickHouse baseline"
}

func finalGateBaselineStatsPresent(ops ...perfreport.Operation) bool {
	for _, op := range ops {
		if len(op.DurationsMS) < finalGateMinRepeats || op.P50MS <= 0 || op.P95MS <= 0 || op.P99MS <= 0 {
			return false
		}
	}

	return true
}

func finalGateNoisyRunRegressionAllowed(
	baseline perfreport.Operation,
	candidate perfreport.Operation,
) bool {
	return candidate.P50MS < finalGateBaselineMedianMS &&
		candidate.P95MS <= baseline.P95MS*finalGateNoisyRunRatio &&
		candidate.P99MS <= baseline.P99MS*finalGateNoisyRunRatio
}

func t283RESTOperationWarmed(report perfreport.Report, op perfreport.Operation) bool {
	switch stringInput(op.Inputs, "order") {
	case "warmed":
		return true
	case "direct":
		return false
	}

	return report.Warmup > 0 ||
		uint64SliceInputSum(op.Inputs, finalGateRESTInputCacheHits) > 0 ||
		len(stringSliceInput(op.Inputs, queryInputCacheHitKeysKey)) > 0
}

func uint64SliceInputSum(inputs map[string]any, key string) uint64 {
	var sum uint64

	for _, value := range uint64SliceInput(inputs, key) {
		sum += value
	}

	return sum
}

func firstResultCount(op perfreport.Operation) uint64 {
	if len(op.ResultCount) == 0 {
		return 0
	}

	return op.ResultCount[0]
}

func finalGateBoltComparableOps() []string {
	return []string{
		queryOpTreeWhereName,
		queryOpTreeDirInfoName,
	}
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
	return len(uint64SliceInput(inputs, key))
}

func allImportReportsPass(reports []perfreport.Report) bool {
	for _, report := range reports {
		if !importReportPasses(report) {
			return false
		}
	}

	return true
}

func importReportPasses(report perfreport.Report) bool {
	capRows := finalGateImportReportCap(report)
	if capRows == 0 {
		return false
	}

	return importReportFailure(report, capRows) == ""
}

func importRowsPass(report perfreport.Report, capRows uint64, rootCount uint64) bool {
	if rootCount == 0 {
		return false
	}

	for _, table := range finalGateNonOptionalTables() {
		maxRows, ok := finalGateNonOptionalRowCap(table, capRows, rootCount)
		if !ok {
			return false
		}

		stats, ok := report.TableStats[table]
		if !ok || stats.Rows == 0 || stats.Rows > maxRows {
			return false
		}
	}

	return true
}

func importRowAmplificationPass(report perfreport.Report, total perfreport.Operation) bool {
	records := uint64Input(total.Inputs, importInputRecords)
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
	return finalGateNonOptionalRowCap(table, finalGateImportCap100K, 1)
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
	return tableStatsSizeEvidencePass(stats) &&
		tableStatsDerivedEvidencePass(stats)
}
