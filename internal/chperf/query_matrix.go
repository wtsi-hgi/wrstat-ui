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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	queryInputQueryTypeKey    = "query_type"
	queryInputQueryVariantKey = "query_variant"

	j4MissingOperation = "missing before/after operation"

	queryAuditSurfaceLegacyChildren    = "wrstat_children"
	queryAuditSurfaceLegacyParentFacts = "wrstat_parent_facts"

	j4QueryTypeExactDirectory = "Exact directory"
	j4QueryTypeBatchDirectory = "Batch directory"
	j4QueryTypeChildren       = "Children/presence"
	j4QueryTypeSubtree        = "Subtree/recursive"
	j4QueryTypeDisktree       = "Disktree"
	j4QueryTypeFileAPI        = "File API"
	j4QueryTypeGlob           = "Glob/full-text"
	j4QueryTypeVirtual        = "Virtual/active"
	j4QueryTypeBasedirs       = "Basedirs/quota"
	j4QueryTypeMaintenance    = "Maintenance"
)

var errJ4MatrixCoverage = errors.New("j4 matrix coverage failed")

func j4Inputs(queryType string, variant string, inputs map[string]any) map[string]any {
	if inputs == nil {
		inputs = map[string]any{}
	}

	inputs[queryInputQueryTypeKey] = queryType
	inputs[queryInputQueryVariantKey] = variant

	return inputs
}

func j4FirstOperationNamed(
	reports []perfreport.Report,
	name string,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		for _, op := range report.Operations {
			if op.Name == name {
				return op, true
			}
		}
	}

	return perfreport.Operation{}, false
}

type j4MatrixDelta struct {
	QueryType      string
	Operation      string
	QueryVariant   string
	BaselineP50MS  float64
	CandidateP50MS float64
	DeltaP50MS     float64
	BaselineP95MS  float64
	CandidateP95MS float64
	DeltaP95MS     float64
	BaselineP99MS  float64
	CandidateP99MS float64
	DeltaP99MS     float64
}

func j4MatrixDeltas(
	baseline []perfreport.Report,
	candidate []perfreport.Report,
) ([]j4MatrixDelta, error) {
	if reason := j4MatrixCoverageFailure(baseline, candidate); reason != "" {
		return nil, fmt.Errorf("%w: %s", errJ4MatrixCoverage, reason)
	}

	if reason := j4MatrixCorrectnessFailure(baseline, candidate); reason != "" {
		return nil, fmt.Errorf("%w: %s", errJ4MatrixCoverage, reason)
	}

	required := j4RequiredMatrixOperations()
	deltas := make([]j4MatrixDelta, 0, len(required))

	for _, spec := range required {
		before, _ := j4FirstOperationMatching(baseline, spec)
		after, _ := j4FirstOperationMatching(candidate, spec)
		deltas = append(deltas, j4MatrixDelta{
			QueryType:      spec.QueryType,
			Operation:      spec.Operation,
			QueryVariant:   spec.QueryVariant,
			BaselineP50MS:  before.P50MS,
			CandidateP50MS: after.P50MS,
			DeltaP50MS:     after.P50MS - before.P50MS,
			BaselineP95MS:  before.P95MS,
			CandidateP95MS: after.P95MS,
			DeltaP95MS:     after.P95MS - before.P95MS,
			BaselineP99MS:  before.P99MS,
			CandidateP99MS: after.P99MS,
			DeltaP99MS:     after.P99MS - before.P99MS,
		})
	}

	return deltas, nil
}

func j4MatrixCoverageFailure(baseline []perfreport.Report, candidate []perfreport.Report) string {
	for _, spec := range j4RequiredMatrixOperations() {
		if _, ok, beforeReason := j4FirstValidOperationMatching(baseline, spec); !ok {
			return fmt.Sprintf("%s baseline %s", j4MatrixOperationLabel(spec), beforeReason)
		}

		if _, ok, afterReason := j4FirstValidOperationMatching(candidate, spec); !ok {
			return fmt.Sprintf("%s candidate %s", j4MatrixOperationLabel(spec), afterReason)
		}
	}

	return ""
}

func j4CanonicalQueryTypes() []string {
	return []string{
		j4QueryTypeExactDirectory,
		j4QueryTypeBatchDirectory,
		j4QueryTypeChildren,
		j4QueryTypeSubtree,
		j4QueryTypeDisktree,
		j4QueryTypeFileAPI,
		j4QueryTypeGlob,
		j4QueryTypeVirtual,
		j4QueryTypeBasedirs,
		j4QueryTypeMaintenance,
	}
}

func j4RequiredMatrixOperations() []j4RequiredMatrixOperation {
	return []j4RequiredMatrixOperation{
		{j4QueryTypeExactDirectory, queryOpTreeDirInfoName, "DirInfo selected directory"},
		{j4QueryTypeExactDirectory, queryOpDirInfoBroadName, "DirInfo broad"},
		{j4QueryTypeExactDirectory, queryOpDirInfoFilteredName, "DirInfo filtered"},
		{j4QueryTypeExactDirectory, queryOpAuthTreeName, "DirInfo auth restricted"},
		{j4QueryTypeBatchDirectory, queryOpDirInfosBroadName, "DirInfos broad"},
		{j4QueryTypeBatchDirectory, queryOpDirInfosFilteredName, "DirInfos filtered"},
		{j4QueryTypeChildren, queryOpChildrenName, "Children"},
		{j4QueryTypeChildren, queryOpDirsHaveChildrenBroadName, "DirsHaveChildren broad"},
		{j4QueryTypeChildren, queryOpDirsHaveChildrenFilteredName, "DirsHaveChildren filtered"},
		{j4QueryTypeSubtree, queryOpTreeWhereName, "Where same provider directory"},
		{j4QueryTypeSubtree, queryOpTreeWhereColdName, "Where cold then cached"},
		{j4QueryTypeSubtree, queryOpTreeWhereColdProviderName, "Where cold provider"},
		{j4QueryTypeSubtree, queryOpTreeWhereProviderUpdateName, "Where provider update cold cache"},
		{j4QueryTypeSubtree, queryOpAuthWhereRestrictedName, "Where auth restricted"},
		{j4QueryTypeSubtree, queryOpNoAuthWhereName, "Where no auth"},
		{j4QueryTypeSubtree, queryOpTreeWhereFreshName, "Where fresh provider"},
		{j4QueryTypeSubtree, queryOpWhereWholeMountName, queryOpWhereWholeMountName},
		{j4QueryTypeSubtree, queryOpWhereFilteredWholeMountName, queryOpWhereFilteredWholeMountName},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeEndName, "Disktree same provider directory"},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeColdProviderName, "Disktree cold provider"},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeProviderUpdateName, "Disktree provider update cold cache"},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeNewName, "Disktree new directories"},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeAncName, "Disktree ancestor directories"},
		{j4QueryTypeDisktree, queryOpTreeDiskTreeVisibleChildName, "Disktree visible child directories"},
		{j4QueryTypeFileAPI, queryOpFilesListDirName, "ListDir"},
		{j4QueryTypeFileAPI, queryOpFilesIsDirName, "IsDir"},
		{j4QueryTypeFileAPI, queryOpFilesStatPathName, "StatPath"},
		{j4QueryTypeFileAPI, queryOpPermissionCheckName, "PermissionPath and PermissionAnyInDir"},
		{j4QueryTypeGlob, queryOpGlobCaseAName, "FindByGlob case A"},
		{j4QueryTypeGlob, queryOpGlobCaseBName, "FindByGlob case B"},
		{j4QueryTypeGlob, "glob_case_C", "FindByGlob case C"},
		{j4QueryTypeGlob, "glob_case_D", "FindByGlob case D"},
		{j4QueryTypeGlob, "glob_case_E", "FindByGlob case E"},
		{j4QueryTypeGlob, "glob_case_F", "FindByGlob case F"},
		{j4QueryTypeGlob, "glob_case_G", "FindByGlob case G"},
		{j4QueryTypeGlob, "glob_case_H", "FindByGlob case H"},
		{j4QueryTypeGlob, queryOpCountGlobCaseAName, "CountByGlob case A"},
		{j4QueryTypeGlob, queryOpFindGlobExtensionDotfileName, "FindByGlob extension dotfile"},
		{j4QueryTypeVirtual, queryOpVirtualChildrenName, "virtual children filtered"},
		{j4QueryTypeVirtual, queryOpVirtualDirInfoName, "active virtual root summary filtered"},
		{j4QueryTypeVirtual, queryOpVirtualActivePrefixRollupName, "active-prefix rollups"},
		{j4QueryTypeBasedirs, queryOpBasedirsGroupUsageName, "GroupUsage"},
		{j4QueryTypeBasedirs, queryOpBasedirsUserUsageName, "UserUsage"},
		{j4QueryTypeBasedirs, queryOpBasedirsGroupSubDirsName, "GroupSubDirs"},
		{j4QueryTypeBasedirs, queryOpBasedirsUserSubDirsName, "UserSubDirs"},
		{j4QueryTypeBasedirs, queryOpBasedirsHistoryName, "history"},
		{j4QueryTypeMaintenance, queryOpImportReadinessPublishName, "import readiness/publish"},
		{j4QueryTypeMaintenance, queryOpActiveSnapshotCleanupName, "active-snapshot cleanup"},
		{j4QueryTypeMaintenance, queryOpInfoName, "Info"},
		{j4QueryTypeMaintenance, queryOpMountTimestampsName, "active mount freshness"},
		{j4QueryTypeMaintenance, queryOpBasedirsInfoName, "basedirs Info"},
	}
}

func j4FirstValidOperationMatching(
	reports []perfreport.Report,
	spec j4RequiredMatrixOperation,
) (perfreport.Operation, bool, string) {
	ops := j4OperationsMatching(reports, spec)
	firstReason := j4MissingOperation

	for _, op := range ops {
		reason := j4OperationMetricsFailure(op)
		if reason == "" {
			return op, true, ""
		}

		if firstReason == j4MissingOperation {
			firstReason = reason
		}
	}

	return perfreport.Operation{}, false, firstReason
}

func j4OperationsMatching(
	reports []perfreport.Report,
	spec j4RequiredMatrixOperation,
) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if j4OperationMatchesSpec(op, spec) {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func j4OperationMatchesSpec(op perfreport.Operation, spec j4RequiredMatrixOperation) bool {
	return op.Name == spec.Operation &&
		stringInput(op.Inputs, queryInputQueryTypeKey) == spec.QueryType &&
		stringInput(op.Inputs, queryInputQueryVariantKey) == spec.QueryVariant
}

func j4OperationMetricsFailure(op perfreport.Operation) string {
	if reason := j4FirstFailedOperationMetricCheck(j4OperationBaseMetricChecks(op)); reason != "" {
		return reason
	}

	if reason := j4OperationDurationSourceFailure(op); reason != "" {
		return reason
	}

	if reason := j4OperationReadMetricsFailure(op); reason != "" {
		return reason
	}

	return j4OperationEvidenceFailure(op)
}

func j4FirstFailedOperationMetricCheck(checks []j4OperationMetricCheck) string {
	for _, check := range checks {
		if check.missing {
			return check.reason
		}
	}

	return ""
}

func j4OperationBaseMetricChecks(op perfreport.Operation) []j4OperationMetricCheck {
	return []j4OperationMetricCheck{
		{"missing duration samples", len(op.DurationsMS) == 0},
		{"missing result rows", len(op.ResultCount) == 0},
		{"missing result digest", stringInput(op.Inputs, queryInputResultDigest) == ""},
		{"invalid p50/p95/p99", op.P50MS < 0 || op.P95MS < 0 || op.P99MS < 0},
	}
}

func j4OperationDurationSourceFailure(op perfreport.Operation) string {
	if !j4OperationRequiresClickHouseSource(op.Name) {
		return ""
	}

	if stringInput(op.Inputs, queryInputDurationSource) != querySourceClickHouseLog {
		return "duration_source must be " + querySourceClickHouseLog
	}

	return ""
}

func j4OperationRequiresClickHouseSource(name string) bool {
	switch name {
	case queryOpImportReadinessPublishName,
		queryOpActiveSnapshotCleanupName,
		queryOpVirtualActivePrefixRollupName:
		return true
	default:
		return false
	}
}

func j4OperationReadMetricsFailure(op perfreport.Operation) string {
	if !j4OperationQueryMetricsRequired(op) {
		return ""
	}

	return j4FirstFailedOperationMetricCheck([]j4OperationMetricCheck{
		{"missing ReadRows", len(op.ReadRows) == 0},
		{"missing ReadBytes", len(op.ReadBytes) == 0},
		{"missing ReadMarks", len(op.ReadMarks) == 0},
	})
}

func j4OperationQueryMetricsRequired(op perfreport.Operation) bool {
	return stringInput(op.Inputs, queryInputDurationSource) != querySourceWall
}

func j4OperationEvidenceFailure(op perfreport.Operation) string {
	switch op.Name {
	case queryOpImportReadinessPublishName:
		return j4AuditEvidenceFailure(op, j4ImportReadinessPublishAuditSurfaces())
	case queryOpActiveSnapshotCleanupName:
		return j4ActiveSnapshotCleanupEvidenceFailure(op)
	case queryOpVirtualActivePrefixRollupName:
		return j4ActivePrefixRollupEvidenceFailure(op)
	default:
		return ""
	}
}

func j4AuditEvidenceFailure(op perfreport.Operation, requiredSurfaces []string) string {
	counts := uint64MapInput(op.Inputs, queryInputAuditCounts)
	if len(counts) == 0 {
		return "missing " + queryInputAuditCounts
	}

	surfaces := stringSliceInput(op.Inputs, queryInputAuditSurfaces)
	if len(surfaces) == 0 {
		return "missing " + queryInputAuditSurfaces
	}

	for _, surface := range requiredSurfaces {
		if counts[surface] == 0 {
			return fmt.Sprintf("missing positive %s[%s]", queryInputAuditCounts, surface)
		}

		if !slices.Contains(surfaces, surface) {
			return "missing audit surface " + surface
		}
	}

	return ""
}

func j4ImportReadinessPublishAuditSurfaces() []string {
	return []string{
		tableSchema3SnapshotSets,
		queryAuditSurfaceActiveVirtualReady,
		tableActivePrefixRollupSets,
		queryAuditSurfaceMountEventsPublish,
	}
}

func j4ActiveSnapshotCleanupEvidenceFailure(op perfreport.Operation) string {
	_, reason := activeSnapshotCleanupRoleDigestFromInputs(op.Inputs)

	return reason
}

func activeSnapshotCleanupRoleDigestFromInputs(inputs map[string]any) (string, string) {
	counts := uint64MapInput(inputs, queryInputAuditCounts)
	if len(counts) == 0 {
		return "", "missing " + queryInputAuditCounts
	}

	surfaces := stringSliceInput(inputs, queryInputAuditSurfaces)
	if len(surfaces) == 0 {
		return "", "missing " + queryInputAuditSurfaces
	}

	schemas := activeSnapshotCleanupSchemasForInputs(inputs)
	firstReason := ""

	for _, schema := range schemas {
		reason := activeSnapshotCleanupSchemaFailure(counts, surfaces, schema)
		if reason == "" {
			return activeSnapshotCleanupRoleDigest(schema.roles), ""
		}

		if firstReason == "" {
			firstReason = reason
		}
	}

	return "", firstReason
}

func activeSnapshotCleanupSchemasForInputs(inputs map[string]any) []activeSnapshotCleanupSchema {
	current := activeSnapshotCleanupCurrentSchema()

	baseline := activeSnapshotCleanupBaselineSchema()
	if activeSnapshotCleanupInputsPreferBaseline(inputs) {
		return []activeSnapshotCleanupSchema{baseline, current}
	}

	return []activeSnapshotCleanupSchema{current, baseline}
}

func activeSnapshotCleanupCurrentSchema() activeSnapshotCleanupSchema {
	return activeSnapshotCleanupSchema{
		roles: activeSnapshotCleanupSchemaRoles(
			tableCatalog,
			tableCatalog,
			tableActiveVirtualDirs,
		),
	}
}

func activeSnapshotCleanupSchemaRoles(
	directoryCatalogSurface string,
	parentFactsSurface string,
	activeVirtualCatalogSurface string,
) []activeSnapshotCleanupRoleRequirement {
	return []activeSnapshotCleanupRoleRequirement{
		{"mount_events", tableMountEvents},
		{"directory_catalog", directoryCatalogSurface},
		{"files", tableFiles},
		{"snapshot_sets", tableSchema3SnapshotSets},
		{"directory_facts", tableDirFacts},
		{"parent_directory_facts", parentFactsSurface},
		{"directory_projection_sets", tableDirSummarySets},
		{"directory_age_filter", tableDirFilterAgeAll},
		{"child_filter_all", tableChildFilterAll},
		{"directory_filter_all", tableDirFilterAll},
		{"basedirs_group_usage", tableBasedirsGroupUsage},
		{"basedirs_user_usage", tableBasedirsUserUsage},
		{"basedirs_group_subdirs", tableBasedirsGroupSubdirs},
		{"basedirs_user_subdirs", tableBasedirsUserSubdirs},
		{"active_virtual_catalog", activeVirtualCatalogSurface},
		{"active_virtual_summaries", tableActiveVirtualSummaries},
		{"active_virtual_filter_all", tableActiveVirtualFilterAll},
		{"active_virtual_children", tableActiveVirtualChildren},
		{"active_virtual_sets", tableActiveVirtualSets},
		{"active_prefix_rollups", tableActivePrefixRollups},
		{"active_prefix_filter_ageall", tableActivePrefixFilterAgeAll},
		{"active_prefix_rollup_sets", tableActivePrefixRollupSets},
	}
}

func activeSnapshotCleanupBaselineSchema() activeSnapshotCleanupSchema {
	return activeSnapshotCleanupSchema{
		roles: activeSnapshotCleanupSchemaRoles(
			queryAuditSurfaceLegacyChildren,
			queryAuditSurfaceLegacyParentFacts,
			tableActiveVirtualSummaries,
		),
	}
}

func activeSnapshotCleanupInputsPreferBaseline(inputs map[string]any) bool {
	counts := uint64MapInput(inputs, queryInputAuditCounts)
	if counts[queryAuditSurfaceLegacyChildren] > 0 || counts[queryAuditSurfaceLegacyParentFacts] > 0 {
		return true
	}

	surfaces := stringSliceInput(inputs, queryInputAuditSurfaces)

	return slices.Contains(surfaces, queryAuditSurfaceLegacyChildren) ||
		slices.Contains(surfaces, queryAuditSurfaceLegacyParentFacts)
}

func activeSnapshotCleanupSchemaFailure(
	counts map[string]uint64,
	surfaces []string,
	schema activeSnapshotCleanupSchema,
) string {
	for _, role := range schema.roles {
		if counts[role.surface] == 0 {
			return fmt.Sprintf(
				"missing positive %s[%s] for cleanup role %s",
				queryInputAuditCounts,
				role.surface,
				role.name,
			)
		}

		if !slices.Contains(surfaces, role.surface) {
			return fmt.Sprintf("missing audit surface %s for cleanup role %s", role.surface, role.name)
		}
	}

	return ""
}

func activeSnapshotCleanupRoleDigest(roles []activeSnapshotCleanupRoleRequirement) string {
	roleNames := make([]string, len(roles))
	for index, role := range roles {
		roleNames[index] = role.name
	}

	return digestValue(roleNames)
}

func j4ActivePrefixRollupEvidenceFailure(op perfreport.Operation) string {
	if stringInput(op.Inputs, queryInputActivePrefixRouteProof) != queryActivePrefixRollupRouteProofRead {
		return queryInputActivePrefixRouteProof + " must be " + queryActivePrefixRollupRouteProofRead
	}

	if uint64Input(op.Inputs, queryInputActivePrefixScalarRootRows) == 0 {
		return "missing " + queryInputActivePrefixScalarRootRows
	}

	return ""
}

func j4MatrixOperationLabel(spec j4RequiredMatrixOperation) string {
	return fmt.Sprintf("%s %s (%s)", spec.QueryType, spec.QueryVariant, spec.Operation)
}

func j4MatrixCorrectnessFailure(baseline []perfreport.Report, candidate []perfreport.Report) string {
	for _, spec := range j4RequiredMatrixOperations() {
		before, _ := j4FirstOperationMatching(baseline, spec)
		after, _ := j4FirstOperationMatching(candidate, spec)
		label := j4MatrixOperationLabel(spec)

		if !j4ResultCountsEquivalent(before.ResultCount, after.ResultCount) {
			return label + " result rows mismatch"
		}

		beforeDigest, beforeDigestReason := j4MatrixComparableResultDigest(before)
		if beforeDigestReason != "" {
			return label + " " + beforeDigestReason
		}

		afterDigest, afterDigestReason := j4MatrixComparableResultDigest(after)
		if afterDigestReason != "" {
			return label + " " + afterDigestReason
		}

		if beforeDigest != afterDigest {
			return label + " result digest mismatch"
		}
	}

	return ""
}

func j4FirstOperationMatching(
	reports []perfreport.Report,
	spec j4RequiredMatrixOperation,
) (perfreport.Operation, bool) {
	op, ok, _ := j4FirstValidOperationMatching(reports, spec)

	return op, ok
}

func j4ResultCountsEquivalent(before []uint64, after []uint64) bool {
	if len(before) == 0 || len(after) == 0 {
		return false
	}

	return slices.Equal(before, after)
}

func allUint64Equal(values []uint64, want uint64) bool {
	for _, value := range values {
		if value != want {
			return false
		}
	}

	return true
}

func j4MatrixComparableResultDigest(op perfreport.Operation) (string, string) {
	if op.Name != queryOpActiveSnapshotCleanupName {
		return stringInput(op.Inputs, queryInputResultDigest), ""
	}

	digest, reason := activeSnapshotCleanupRoleDigestFromInputs(op.Inputs)
	if reason != "" {
		return "", reason
	}

	return digest, ""
}

type j4OperationMetricCheck struct {
	reason  string
	missing bool
}

type activeSnapshotCleanupRoleRequirement struct {
	name    string
	surface string
}

type activeSnapshotCleanupSchema struct {
	roles []activeSnapshotCleanupRoleRequirement
}

type j4RequiredMatrixOperation struct {
	QueryType    string
	Operation    string
	QueryVariant string
}

// ExplainFindByGlobAvoidsFilePathScan reports whether a full-path glob plan
// uses the directory catalog and reads files by dir_id rather than file path
// text.
func ExplainFindByGlobAvoidsFilePathScan(explain string) bool {
	normal := strings.ToLower(strings.Join(strings.Fields(explain), " "))
	if !strings.Contains(normal, "wrstat_dirs") {
		return false
	}

	filesReadByDirID := strings.Contains(normal, "wrstat_files") &&
		strings.Contains(normal, "keys: mount_path snapshot_id dir_id")
	if !filesReadByDirID {
		return false
	}

	filePathNeedles := []string{
		"f.path",
		"wrstat_files.path",
		"wrstat_files.full_path",
		"match(path",
	}

	return !containsAny(normal, filePathNeedles)
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}

	return false
}
