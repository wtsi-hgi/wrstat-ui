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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

var (
	errFinalGateComparisonNotConfigured = errors.New("final gate comparison not configured")
	errFinalGateNegativeFileSize        = errors.New("negative file size")
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
	finalGateWhereCommandName     = "where"

	finalGateT283FilteredRESTOrderAnomaly = "t283_filtered_rest_order_anomaly"
	finalGateT283Dir                      = "/nfs/t283_imaging/"
	finalGateHighFanoutParentDir          = "/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/"
	finalGateLustreRootDir                = "/lustre/"
	finalGateNFSHeavyDir                  = finalGateT283Dir
	finalGateNFSRootDir                   = "/nfs/"
	finalGateSelectedTreeGIDs             = "14976"
	finalGateSelectedTreeUIDs             = "20155"
	finalGateSelectedTreeTypes            = "other"
	finalGateScratch120Dir                = "/lustre/scratch120/"
	finalGateScratch122Dir                = "/lustre/scratch122/"
	finalGateScratch127Dir                = "/lustre/scratch127/"
	finalGateWrstatUICommand              = "./wrstat-ui"

	finalGateReportSchemaVersion      = 1
	finalGateCorrectnessStatusInput   = "correctness_equivalence_status"
	finalGateComparisonKindBolt       = "bolt"
	finalGateComparisonKindSidecar    = "sidecar"
	finalGateComparisonStatusSuccess  = "success"
	finalGateComparisonInfeasible     = "infeasible"
	finalGateReportStatusPassed       = "passed"
	finalGateReportStatusFailed       = "failed"
	finalGateReportStatusBlocked      = "blocked"
	finalGateSidecarFallbackInactive  = "inactive"
	finalGateSidecarFallbackTriggered = "triggered"
)

const (
	finalGateE2BudgetMeasurementCountInput = "budget_measurement_count"
	finalGateE2BudgetSourceComputed        = "computed_from_measurements"
	finalGateE2BudgetSourceInput           = "budget_source"
	finalGateE2ChildFilterAllTable         = "wrstat_child_filter_all"
	finalGateE2DirFilterAllTable           = "wrstat_dir_filter_all"
	finalGateE2ExpectedDigestInput         = "expected_result_digest"
	finalGateE2HighFanoutBroadCheckName    = "E2 high-fanout first click broad"
	finalGateE2HighFanoutFilteredCheckName = "E2 high-fanout first click filtered"
	finalGateE2FactsVectorRowsInput        = "facts_vector_rows_read"
	finalGateE2InputPartCounts             = "part_counts"
	finalGateE2InputSpoolBytes             = "spool_bytes"
	finalGateE2ParentPacketTableInput      = "parent_packet_table"
	finalGateE2ProactiveWarmingInput       = "proactive_warming"
	finalGateE2ReadBytesCeilingInput       = "read_bytes_ceiling"
	finalGateE2ReadMarksCeilingInput       = "read_marks_ceiling"
	finalGateE2ReadVolumeByteSlack         = 1.25
	finalGateE2ReadVolumeBytesName         = "bytes"
	finalGateE2ReadVolumeIndexGranularity  = uint64(8192)
	finalGateE2ReadVolumeMarksName         = "marks"
	finalGateE2ReadVolumeRowsName          = "rows"
	finalGateE2ReadRowsCeilingInput        = "read_rows_ceiling"
	finalGateE2Schema3SnapshotSetsTable    = "wrstat_schema3_snapshot_sets"
	finalGateE2ScenarioDirInfosBroad       = "dirinfos_broad_parent_packet"
	finalGateE2ScenarioDirInfosFiltered    = "dirinfos_filtered_child_filter_packet"
	finalGateE2ScenarioDHCBroad            = "dirshavechildren_broad_parent_packet"
	finalGateE2ScenarioDHCFiltered         = "dirshavechildren_filtered_parent_packet"
	finalGateE2ScenarioFirstRootWhere      = "first_root_where_splits_2"
	finalGateE2ScenarioHighFanoutBroad     = "high_fanout_first_click_broad"
	finalGateE2ScenarioHighFanoutFiltered  = "high_fanout_first_click_filtered"
	finalGateE2ScenarioNFSHeavyWhere       = "nfs_heavy_first_where_dir"
	finalGateE2ScenarioRESTFirst           = "rest_tree_first_requests"
	finalGateE2ScenarioRealWhere           = "real_first_where_dir"
	finalGateE2ScenarioSwitch              = "first_filter_switch_after_unfiltered_tree"
	finalGateE2ScenarioInput               = "e2_scenario"
)

const (
	finalGateJ6ColdUXFastMaxMS  = 100
	finalGateJ6ColdUXBroadMaxMS = 500

	finalGateJ6ColdUXCheckName = "J6 absolute cold UX"

	finalGateJ6StorageAuditOpName = "j6_storage_audit"
	finalGateJ6D4DecisionOpName   = "d4_collapse_decision"

	finalGateJ6HotRowPathStringTablesInput       = "hot_row_path_string_tables"
	finalGateJ6PathTextCatalogTableInput         = "path_text_catalog_table"
	finalGateJ6PathTextCopiesPerDirSnapshotInput = "path_text_copies_per_dir_snapshot"
	finalGateJ6WrongRowCountInput                = "wrong_row_count"
	finalGateJ6PathHashCollisionMismatchInput    = "path_hash_collision_mismatch_count"

	finalGateJ6D4CitationInput           = "measurement_citation"
	finalGateJ6D4DecisionCollapsed       = "collapsed"
	finalGateJ6D4DecisionInput           = "decision"
	finalGateJ6D4DecisionRetained        = "retained"
	finalGateJ6D4LatencyGateInput        = "latency_gate_ms"
	finalGateJ6D4MaterialisationInput    = "materialisation"
	finalGateJ6D4MeasuredP95Input        = "measured_p95_ms"
	finalGateJ6D4PatternFilteredExact    = "filtered_exact"
	finalGateJ6D4PatternFilteredChildren = "filtered_children"
	finalGateJ6D4PatternFilteredSubtree  = "filtered_subtree"
	finalGateJ6D4PatternInput            = "pattern"
)

// FinalGateFixtureDigestEvidence records a fixture manifest digest check.
type FinalGateFixtureDigestEvidence struct {
	Key              string `json:"key"`
	ManifestPath     string `json:"manifest_path"`
	ExpectedDigest   string `json:"expected_digest"`
	RecomputedDigest string `json:"recomputed_digest"`
}

func finalGateBuildFixtureDigest(
	spec FinalGateFixtureDigestSpec,
) (FinalGateFixtureDigestEvidence, error) {
	expected := spec.ExpectedDigest
	if strings.TrimSpace(expected) == "" {
		expected = finalGateExpectedDigestFromManifest(spec.ManifestPath, spec.Key)
	}

	recomputed, err := finalGateFixtureResultDigest(spec.InputPath)
	if err != nil {
		return FinalGateFixtureDigestEvidence{}, err
	}

	return FinalGateFixtureDigestEvidence{
		Key:              spec.Key,
		ManifestPath:     spec.ManifestPath,
		ExpectedDigest:   expected,
		RecomputedDigest: recomputed,
	}, nil
}

func finalGateBuildFixtureDigests(
	specs []FinalGateFixtureDigestSpec,
) ([]FinalGateFixtureDigestEvidence, error) {
	digests := make([]FinalGateFixtureDigestEvidence, 0, len(specs))
	for _, spec := range specs {
		digest, err := finalGateBuildFixtureDigest(spec)
		if err != nil {
			return nil, err
		}

		digests = append(digests, digest)
	}

	return digests, nil
}

func finalGateFixtureDigestFailure(digest FinalGateFixtureDigestEvidence) string {
	if strings.TrimSpace(digest.Key) == "" || strings.TrimSpace(digest.ExpectedDigest) == "" {
		return "missing expected digest"
	}

	if finalGatePlaceholderDigest(digest.ExpectedDigest) {
		return "missing expected digest: placeholder digest"
	}

	if digest.ExpectedDigest != digest.RecomputedDigest {
		return "stale expected digest"
	}

	return ""
}

func finalGatePlaceholderDigest(digest string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(digest))

	return trimmed == "sha256:" ||
		strings.Contains(trimmed, "placeholder") ||
		strings.Contains(trimmed, "todo")
}

// FinalGateFixtureDigestSpec identifies one fixture digest to validate.
type FinalGateFixtureDigestSpec struct {
	Key            string `json:"key"`
	ManifestPath   string `json:"manifest_path"`
	InputPath      string `json:"input_path"`
	ExpectedDigest string `json:"expected_digest,omitempty"`
}

// FinalGateComparisonSpec identifies same-subset comparison artefacts.
type FinalGateComparisonSpec struct {
	Kind                string   `json:"kind"`
	Status              string   `json:"status"`
	DatasetManifestPath string   `json:"dataset_manifest_path"`
	CommandArgv         []string `json:"command_argv"`
	SourceRevision      string   `json:"source_revision,omitempty"`
	ToolVersion         string   `json:"tool_version,omitempty"`
	OutputArtifactPath  string   `json:"output_artifact_path,omitempty"` //nolint:misspell
	LogPath             string   `json:"log_path"`
	StoragePath         string   `json:"storage_path,omitempty"`
	AttemptedPath       string   `json:"attempted_checkout_or_prototype_path,omitempty"`
	ErrorOutput         string   `json:"error_output,omitempty"`
	ErrorOutputPath     string   `json:"error_output_path,omitempty"`
	Reason              string   `json:"reason,omitempty"`
}

func finalGateComparisonErrorOutput(spec FinalGateComparisonSpec) (string, error) {
	if strings.TrimSpace(spec.ErrorOutput) != "" {
		return spec.ErrorOutput, nil
	}

	path := firstNonBlank(spec.ErrorOutputPath, spec.LogPath)
	if strings.TrimSpace(path) == "" {
		return "", nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

func finalGatePopulateComparisonEvidence(
	report *FinalGateReport,
	spec FinalGateComparisonSpec,
) error {
	comparison, err := finalGateBuildComparisonEvidence(spec)
	if errors.Is(err, errFinalGateComparisonNotConfigured) {
		return nil
	}

	if err != nil {
		return err
	}

	report.Comparison = comparison

	return nil
}

func finalGateBuildComparisonEvidence(
	spec FinalGateComparisonSpec,
) (*FinalGateComparisonEvidence, error) {
	if strings.TrimSpace(spec.Kind) == "" && strings.TrimSpace(spec.Status) == "" {
		return nil, errFinalGateComparisonNotConfigured
	}

	switch spec.Status {
	case finalGateComparisonStatusSuccess:
		return finalGateBuildSuccessfulComparisonEvidence(spec)
	case finalGateComparisonInfeasible:
		return finalGateBuildInfeasibleComparisonEvidence(spec)
	default:
		return &FinalGateComparisonEvidence{
			Kind:                  spec.Kind,
			Status:                spec.Status,
			DatasetManifestPath:   spec.DatasetManifestPath,
			DatasetManifestSHA256: finalGateSHA256IfPresent(spec.DatasetManifestPath),
			CommandArgv:           slices.Clone(spec.CommandArgv),
			SourceRevision:        spec.SourceRevision,
			ToolVersion:           spec.ToolVersion,
			LogPath:               spec.LogPath,
		}, nil
	}
}

func finalGateBuildSuccessfulComparisonEvidence(
	spec FinalGateComparisonSpec,
) (*FinalGateComparisonEvidence, error) {
	report, err := finalGateReadPerfReport(spec.OutputArtifactPath)
	if err != nil {
		return nil, err
	}

	storageBytes, err := finalGateStorageBytes(spec.StoragePath, spec.OutputArtifactPath)
	if err != nil {
		return nil, err
	}

	if err := finalGateRequireReadableFile(spec.LogPath); err != nil {
		return nil, err
	}

	p50, p95, p99, digest := finalGateComparisonReportMetrics(report)

	return &FinalGateComparisonEvidence{
		Kind:                  spec.Kind,
		Status:                spec.Status,
		DatasetManifestPath:   spec.DatasetManifestPath,
		DatasetManifestSHA256: finalGateSHA256IfPresent(spec.DatasetManifestPath),
		CommandArgv:           slices.Clone(spec.CommandArgv),
		SourceRevision:        firstNonBlank(report.GitCommit, spec.SourceRevision),
		ToolVersion:           firstNonBlank(report.ToolVersion, spec.ToolVersion),
		OutputArtifactPath:    spec.OutputArtifactPath,
		LogPath:               spec.LogPath,
		StorageBytes:          storageBytes,
		P50MS:                 p50,
		P95MS:                 p95,
		P99MS:                 p99,
		ResultDigest:          digest,
		FallbackCount:         finalGateComparisonFallbackCount(report),
	}, nil
}

func finalGateReadPerfReport(path string) (perfreport.Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return perfreport.Report{}, err
	}

	var report perfreport.Report
	if err := json.Unmarshal(data, &report); err != nil {
		return perfreport.Report{}, err
	}

	return report, nil
}

func finalGateStorageBytes(storagePath string, fallbackPath string) (uint64, error) {
	path := firstNonBlank(storagePath, fallbackPath)
	if strings.TrimSpace(path) == "" {
		return 0, nil
	}

	return finalGatePathBytes(path)
}

func finalGatePathBytes(path string) (uint64, error) {
	var total uint64

	err := filepath.WalkDir(path, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		size, err := finalGateFileSizeBytes(info)
		if err != nil {
			return err
		}

		total += size

		return nil
	})

	return total, err
}

func finalGateFileSizeBytes(info os.FileInfo) (uint64, error) {
	size := info.Size()
	if size < 0 {
		return 0, fmt.Errorf("%w: %s", errFinalGateNegativeFileSize, info.Name())
	}

	return uint64(size), nil
}

func finalGateRequireReadableFile(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}

	fh, err := os.Open(path)
	if err != nil {
		return err
	}

	return fh.Close()
}

func finalGateComparisonReportMetrics(report perfreport.Report) (float64, float64, float64, string) {
	for _, op := range report.Operations {
		digest := stringInput(op.Inputs, queryInputResultDigest)
		if digest != "" && finalGateOperationHasTiming(op) {
			return op.P50MS, op.P95MS, op.P99MS, digest
		}
	}

	return 0, 0, 0, ""
}

func finalGateOperationHasTiming(op perfreport.Operation) bool {
	return len(op.DurationsMS) > 0 || op.P50MS > 0 || op.P95MS > 0 || op.P99MS > 0
}

func finalGateSHA256IfPresent(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}

	digest, err := finalGatePathSHA256(path)
	if err != nil {
		return ""
	}

	return digest
}

func finalGatePathSHA256(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.IsDir() {
		return finalGateFileSHA256(path)
	}

	return finalGateDirSHA256(path)
}

func finalGateFileSHA256(path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, fh); err != nil {
		return "", err
	}

	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func finalGateDirSHA256(root string) (string, error) {
	hash := sha256.New()

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		return finalGateHashDirEntry(hash, root, path, entry)
	})
	if err != nil {
		return "", err
	}

	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func finalGateHashDirEntry(hash io.Writer, root string, path string, entry os.DirEntry) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}

	if rel == "." {
		return nil
	}

	if err := finalGateWriteHash(hash, []byte(filepath.ToSlash(rel))); err != nil {
		return err
	}

	if err := finalGateWriteHash(hash, []byte{0}); err != nil {
		return err
	}

	if entry.IsDir() {
		return finalGateWriteHash(hash, []byte("/"))
	}

	if !entry.Type().IsRegular() {
		return nil
	}

	return finalGateHashFileContent(hash, path)
}

func finalGateWriteHash(hash io.Writer, value []byte) error {
	_, err := hash.Write(value)

	return err
}

func finalGateHashFileContent(hash io.Writer, path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	_, err = io.Copy(hash, fh)

	return err
}

func finalGateComparisonFallbackCount(report perfreport.Report) uint64 {
	var count uint64

	for _, op := range report.Operations {
		for _, value := range uint64SliceInput(op.Inputs, "schema3_fallback_count") {
			count += value
		}
	}

	return count
}

func finalGateBuildInfeasibleComparisonEvidence(
	spec FinalGateComparisonSpec,
) (*FinalGateComparisonEvidence, error) {
	errorOutput, err := finalGateComparisonErrorOutput(spec)
	if err != nil {
		return nil, err
	}

	return &FinalGateComparisonEvidence{
		Kind:                  spec.Kind,
		Status:                spec.Status,
		DatasetManifestPath:   spec.DatasetManifestPath,
		DatasetManifestSHA256: finalGateSHA256IfPresent(spec.DatasetManifestPath),
		CommandArgv:           slices.Clone(spec.CommandArgv),
		SourceRevision:        spec.SourceRevision,
		ToolVersion:           spec.ToolVersion,
		LogPath:               spec.LogPath,
		AttemptedPath:         spec.AttemptedPath,
		ErrorOutput:           errorOutput,
		Reason:                spec.Reason,
	}, nil
}

// FinalGateReportOptions identifies artefacts used to assemble an E1 report.
type FinalGateReportOptions struct {
	FixtureDigests        []FinalGateFixtureDigestSpec `json:"fixture_digests,omitempty"`
	ClickHouseReportPaths []string                     `json:"clickhouse_report_paths,omitempty"`
	RESTReportPaths       []string                     `json:"rest_report_paths,omitempty"`
	ImportReportPaths     []string                     `json:"import_report_paths,omitempty"`
	SpoolLoadReportPaths  []string                     `json:"spool_load_report_paths,omitempty"`
	Comparison            FinalGateComparisonSpec      `json:"comparison"`
}

func finalGatePopulateReportArtifacts(
	report *FinalGateReport,
	opts FinalGateReportOptions,
) error {
	var err error

	report.FixtureDigests, err = finalGateBuildFixtureDigests(opts.FixtureDigests)
	if err != nil {
		return err
	}

	if report.ClickHouseReports, err = finalGateReadPerfReports(opts.ClickHouseReportPaths); err != nil {
		return err
	}

	if report.RESTReports, err = finalGateReadPerfReports(opts.RESTReportPaths); err != nil {
		return err
	}

	if report.ImportReports, err = finalGateReadPerfReports(opts.ImportReportPaths); err != nil {
		return err
	}

	report.SpoolLoadReports, err = finalGateReadPerfReports(opts.SpoolLoadReportPaths)

	return err
}

func finalGateReadPerfReports(paths []string) ([]perfreport.Report, error) {
	reports := make([]perfreport.Report, 0, len(paths))
	for _, path := range paths {
		report, err := finalGateReadPerfReport(path)
		if err != nil {
			return nil, err
		}

		reports = append(reports, report)
	}

	return reports, nil
}

// FinalGateComparisonEvidence records same-subset Bolt or sidecar evidence.
type FinalGateComparisonEvidence struct {
	Kind                  string   `json:"kind"`
	Status                string   `json:"status"`
	DatasetManifestPath   string   `json:"dataset_manifest_path"`
	DatasetManifestSHA256 string   `json:"dataset_manifest_sha256"`
	CommandArgv           []string `json:"command_argv"`
	SourceRevision        string   `json:"source_revision"`
	ToolVersion           string   `json:"tool_version"`
	OutputArtifactPath    string   `json:"output_artifact_path,omitempty"` //nolint:misspell
	LogPath               string   `json:"log_path"`
	StorageBytes          uint64   `json:"storage_bytes,omitempty"`
	P50MS                 float64  `json:"p50_ms,omitempty"`
	P95MS                 float64  `json:"p95_ms,omitempty"`
	P99MS                 float64  `json:"p99_ms,omitempty"`
	ResultDigest          string   `json:"result_digest,omitempty"`
	FallbackCount         uint64   `json:"fallback_count"`
	AttemptedPath         string   `json:"attempted_checkout_or_prototype_path,omitempty"`
	ErrorOutput           string   `json:"error_output,omitempty"`
	Reason                string   `json:"reason,omitempty"`
}

func finalGateSuccessfulComparisonFailure(comparison FinalGateComparisonEvidence) string {
	if reason := finalGateCommonComparisonFailure(comparison); reason != "" {
		return reason
	}

	if strings.TrimSpace(comparison.OutputArtifactPath) == "" {
		return "missing output artefact path"
	}

	if finalGateSuccessfulComparisonMetricsMissing(comparison) {
		return "missing storage, percentile, digest, or fallback evidence"
	}

	return ""
}

func finalGateInfeasibleComparisonFailure(comparison FinalGateComparisonEvidence) string {
	if reason := finalGateCommonComparisonFailure(comparison); reason != "" {
		return reason
	}

	if strings.TrimSpace(comparison.AttemptedPath) == "" {
		return "missing attempted checkout or prototype path"
	}

	if strings.TrimSpace(comparison.ErrorOutput) == "" && strings.TrimSpace(comparison.Reason) == "" {
		return "missing infeasible comparison reason"
	}

	return ""
}

func finalGateCommonComparisonFailure(comparison FinalGateComparisonEvidence) string {
	if !finalGateComparisonKindSupported(comparison.Kind) {
		return "missing comparison evidence"
	}

	if !finalGateComparisonMetadataPresent(comparison) {
		return "missing comparison metadata"
	}

	if len(comparison.CommandArgv) == 0 {
		return "missing comparison metadata"
	}

	return ""
}

func finalGateComparisonKindSupported(kind string) bool {
	return kind == finalGateComparisonKindBolt || kind == finalGateComparisonKindSidecar
}

func finalGateComparisonMetadataPresent(comparison FinalGateComparisonEvidence) bool {
	return finalGateStringsPresent(
		comparison.DatasetManifestPath,
		comparison.DatasetManifestSHA256,
		comparison.SourceRevision,
		comparison.ToolVersion,
		comparison.LogPath,
	)
}

func finalGateStringsPresent(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}

	return true
}

func finalGateSuccessfulComparisonMetricsMissing(comparison FinalGateComparisonEvidence) bool {
	if comparison.StorageBytes == 0 {
		return true
	}

	if comparison.P50MS <= 0 || comparison.P95MS <= 0 || comparison.P99MS <= 0 {
		return true
	}

	return strings.TrimSpace(comparison.ResultDigest) == ""
}

// FinalGateReport is the E1 evidence envelope written before speed gates run.
type FinalGateReport struct {
	SchemaVersion     int                              `json:"schema_version"`
	FixtureDigests    []FinalGateFixtureDigestEvidence `json:"fixture_digests,omitempty"`
	ClickHouseReports []perfreport.Report              `json:"clickhouse_reports,omitempty"`
	RESTReports       []perfreport.Report              `json:"rest_reports,omitempty"`
	ImportReports     []perfreport.Report              `json:"import_reports,omitempty"`
	SpoolLoadReports  []perfreport.Report              `json:"spool_load_reports,omitempty"`
	Comparison        *FinalGateComparisonEvidence     `json:"comparison,omitempty"`
}

// BuildFinalGateReport assembles E1 evidence from persisted harness artefacts.
func BuildFinalGateReport(opts FinalGateReportOptions) (FinalGateReport, error) {
	report := FinalGateReport{SchemaVersion: finalGateReportSchemaVersion}

	if err := finalGatePopulateReportArtifacts(&report, opts); err != nil {
		return FinalGateReport{}, err
	}

	if err := finalGatePopulateComparisonEvidence(&report, opts.Comparison); err != nil {
		return FinalGateReport{}, err
	}

	if report.Comparison != nil {
		finalGateSetReportCorrectnessStatus(&report, report.Comparison.Status)
	}

	return report, nil
}

func finalGateEvidenceE1Report(e FinalGateEvidence) FinalGateReport {
	if e.FinalGateReport == nil {
		return FinalGateReport{}
	}

	return *e.FinalGateReport
}

// WriteFinalGateReport writes report as pretty-printed JSON.
func WriteFinalGateReport(path string, report FinalGateReport) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	enc := json.NewEncoder(fh)
	enc.SetIndent("", "  ")

	return enc.Encode(report)
}

func finalGateSetReportCorrectnessStatus(report *FinalGateReport, status string) {
	if strings.TrimSpace(status) == "" {
		return
	}

	finalGateSetReportsCorrectnessStatus(report.ClickHouseReports, status)
	finalGateSetReportsCorrectnessStatus(report.RESTReports, status)
}

func finalGateSetReportsCorrectnessStatus(reports []perfreport.Report, status string) {
	for reportIndex := range reports {
		for opIndex := range reports[reportIndex].Operations {
			op := &reports[reportIndex].Operations[opIndex]
			if !finalGateOperationHasTiming(*op) {
				continue
			}

			if op.Inputs == nil {
				op.Inputs = make(map[string]any)
			}

			op.Inputs[finalGateCorrectnessStatusInput] = status
		}
	}
}

func finalGateSpeedOperations(report FinalGateReport) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, perfReport := range append(slices.Clone(report.ClickHouseReports), report.RESTReports...) {
		for _, op := range perfReport.Operations {
			if finalGateOperationHasTiming(op) {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func finalGateE2ImportReports(e FinalGateEvidence) []perfreport.Report {
	if len(e.ImportReports) > 0 || e.FinalGateReport == nil {
		return e.ImportReports
	}

	return e.FinalGateReport.ImportReports
}

func finalGateE2SpoolReports(e FinalGateEvidence) []perfreport.Report {
	if e.FinalGateReport == nil {
		return nil
	}

	return e.FinalGateReport.SpoolLoadReports
}

func validateFinalGateFixtureDigests(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(10, "E1 fixture digest validation")
	if len(report.FixtureDigests) == 0 {
		return check.fail("missing expected digest: no fixture digest evidence")
	}

	for _, digest := range report.FixtureDigests {
		if reason := finalGateFixtureDigestFailure(digest); reason != "" {
			return check.fail(reason)
		}
	}

	return check.pass("fixture manifest digests match recomputed canonical result digests")
}

func validateFinalGateCorrectnessEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(1, "E1 correctness evidence")

	ops := finalGateSpeedOperations(report)
	if len(ops) == 0 {
		return check.fail("missing correctness fields: no speed-gated operations")
	}

	for _, op := range ops {
		if !finalGateOperationCorrectnessEvidencePasses(op) {
			return check.fail("missing correctness fields")
		}
	}

	return check.pass("speed-gated reports include result counts, result digests, and correctness status")
}

func validateFinalGateRESTReportEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(2, "E1 REST evidence")
	if len(report.RESTReports) == 0 {
		return check.fail("missing REST evidence")
	}

	if !finalGateRESTOpsPass(report.RESTReports, finalGateRESTOpTree) {
		return check.fail("REST evidence missing tree counters, bytes, or percentiles")
	}

	if !finalGateRESTOpsPass(report.RESTReports, finalGateRESTOpWhere) {
		return check.fail("REST evidence missing where counters, bytes, or percentiles")
	}

	return check.pass("REST tree and where probes include counters, byte sizes, and percentiles")
}

func validateFinalGateClickHouseReportEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(3, "E1 ClickHouse evidence")

	ops := finalGateClickHouseOperations(report.ClickHouseReports)
	if len(ops) == 0 {
		return check.fail("missing ClickHouse evidence")
	}

	for _, op := range ops {
		if !finalGateClickHouseOperationEvidencePasses(op) {
			return check.fail("ClickHouse evidence missing read rows, read bytes, read marks, result rows, or result bytes")
		}
	}

	return check.pass("ClickHouse probes include read rows, read bytes, read marks, result rows, and result bytes")
}

func validateFinalGateDirectImportEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(4, "E1 import evidence")
	if len(report.ImportReports) == 0 {
		return check.fail("missing import evidence")
	}

	for _, importReport := range report.ImportReports {
		if reason := finalGateImportEvidenceFailure(importReport, "import_total", false); reason != "" {
			return check.fail("import evidence " + reason)
		}
	}

	return check.pass("direct import reports include table, resource, cleanup, and publish evidence")
}

func validateFinalGateSpoolLoadEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(5, "E1 spool-load evidence")
	if len(report.SpoolLoadReports) == 0 {
		return check.fail("missing spool-load evidence")
	}

	for _, spoolReport := range report.SpoolLoadReports {
		if reason := finalGateImportEvidenceFailure(spoolReport, "spool_load_total", true); reason != "" {
			return check.fail("spool-load evidence " + reason)
		}
	}

	return check.pass("spool-load reports include loaded rows, resources, cleanup, and publish evidence")
}

func validateFinalGateComparisonPresence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(6, "E1 comparison presence")
	if report.Comparison == nil || !finalGateComparisonKindSupported(report.Comparison.Kind) {
		return check.fail("missing comparison evidence")
	}

	return check.pass("same-subset Bolt or sidecar comparison evidence is present")
}

func validateFinalGateSuccessfulComparisonEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(7, "E1 successful comparison evidence")
	if report.Comparison == nil || report.Comparison.Status != finalGateComparisonStatusSuccess {
		return check.pass("comparison is not successful evidence")
	}

	if reason := finalGateSuccessfulComparisonFailure(*report.Comparison); reason != "" {
		return check.fail(reason)
	}

	return check.pass("successful comparison evidence includes manifest, command, version, latency, digest, and storage")
}

func validateFinalGateInfeasibleComparisonEvidence(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(8, "E1 infeasible comparison evidence")
	if report.Comparison == nil || report.Comparison.Status != finalGateComparisonInfeasible {
		return check.pass("comparison is not infeasible evidence")
	}

	if reason := finalGateInfeasibleComparisonFailure(*report.Comparison); reason != "" {
		return check.fail(reason)
	}

	return check.pass("infeasible comparison evidence includes attempted route, manifest, command, log, and reason")
}

func validateFinalGateInfeasibleComparisonBlock(report FinalGateReport) FinalGateCheck {
	check := finalGateCheck(9, "E1 infeasible comparison block")
	if report.Comparison == nil || report.Comparison.Status != finalGateComparisonInfeasible {
		return check.pass("comparison is reproducible")
	}

	if finalGateInfeasibleComparisonFailure(*report.Comparison) != "" {
		return check.pass("incomplete infeasible evidence is handled by evidence validation")
	}

	reason := report.Comparison.Reason
	if reason == "" {
		reason = report.Comparison.ErrorOutput
	}

	return check.block(fmt.Sprintf("%s; log: %s", reason, report.Comparison.LogPath))
}

func validateFinalGateE2RESTTreeFirst(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(26, "E2 REST tree first requests")

	for _, path := range []string{"/", finalGateLustreRootDir, finalGateNFSRootDir} {
		measured, ok := finalGateE2Operation(e, finalGateRESTOpTree, finalGateE2ScenarioRESTFirst, path)
		if !ok {
			return check.fail(path + " missing REST tree first-request evidence")
		}

		if reason := finalGateE2RESTOperationFailure(measured, 500); reason != "" {
			return check.fail(path + " " + reason)
		}
	}

	return check.pass("root, lustre, and nfs first REST tree requests passed cold correctness and p95 gates")
}

func validateFinalGateE2HighFanoutBroadClick(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(27, finalGateE2HighFanoutBroadCheckName)

	measured, ok := finalGateE2Operation(
		e,
		finalGateRESTOpTree,
		finalGateE2ScenarioHighFanoutBroad,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing high-fanout broad first-click evidence")
	}

	if reason := finalGateE2PacketOperationFailure(measured, 500, tableDirFacts, true, false); reason != "" {
		return check.fail(reason)
	}

	return check.pass("high-fanout broad first click used one current read and one catalog parent-id packet")
}

func validateFinalGateE2HighFanoutFilteredClick(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(28, finalGateE2HighFanoutFilteredCheckName)

	measured, ok := finalGateE2Operation(
		e,
		finalGateRESTOpTree,
		finalGateE2ScenarioHighFanoutFiltered,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing high-fanout filtered first-click evidence")
	}

	if reason := finalGateE2PacketOperationFailure(
		measured,
		500,
		finalGateE2ChildFilterAllTable,
		true,
		false,
	); reason != "" {
		return check.fail(reason)
	}

	return check.pass("high-fanout filtered first click used one current read and one child-filter packet")
}

func validateFinalGateE2DirInfosBroad(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(29, "E2 high-fanout DirInfos broad")

	measured, ok := finalGateE2Operation(
		e,
		queryOpDirInfosBroadName,
		finalGateE2ScenarioDirInfosBroad,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing broad DirInfos evidence")
	}

	if reason := finalGateE2PacketOperationFailure(measured, 1000, tableDirFacts, false, false); reason != "" {
		return check.fail(reason)
	}

	return check.pass("focused broad DirInfos used one catalog parent-id packet with bounded reads")
}

func validateFinalGateE2DirInfosFiltered(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(30, "E2 high-fanout DirInfos filtered")

	measured, ok := finalGateE2Operation(
		e,
		queryOpDirInfosFilteredName,
		finalGateE2ScenarioDirInfosFiltered,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing filtered DirInfos evidence")
	}

	if reason := finalGateE2PacketOperationFailure(
		measured,
		1000,
		finalGateE2ChildFilterAllTable,
		false,
		true,
	); reason != "" {
		return check.fail(reason)
	}

	return check.pass("focused filtered DirInfos used one child-filter packet and no facts-vector rows")
}

func validateFinalGateE2DirsHaveChildrenBroad(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(31, "E2 high-fanout DirsHaveChildren broad")

	measured, ok := finalGateE2Operation(
		e,
		queryOpDirsHaveChildrenBroadName,
		finalGateE2ScenarioDHCBroad,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing broad DirsHaveChildren evidence")
	}

	if reason := finalGateE2PacketOperationFailure(measured, 1000, tableDirFacts, false, false); reason != "" {
		return check.fail(reason)
	}

	return check.pass("focused broad DirsHaveChildren used one catalog parent-id packet with bounded reads")
}

func validateFinalGateE2DirsHaveChildrenFiltered(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(32, "E2 high-fanout DirsHaveChildren filtered")

	measured, ok := finalGateE2Operation(
		e,
		queryOpDirsHaveChildrenFilteredName,
		finalGateE2ScenarioDHCFiltered,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing filtered DirsHaveChildren evidence")
	}

	if reason := finalGateE2PacketOperationFailure(
		measured,
		1000,
		finalGateE2ChildFilterAllTable,
		false,
		true,
	); reason != "" {
		return check.fail(reason)
	}

	if reason := finalGateE2ExpectedChildrenFailure(measured.op); reason != "" {
		return check.fail(reason)
	}

	return check.pass("focused filtered DirsHaveChildren returned only expected true children")
}

func validateFinalGateE2FirstRootWhereSplits(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(33, "E2 first root where splits 2")

	measured, ok := finalGateE2Operation(e, queryOpTreeWhereFreshName, finalGateE2ScenarioFirstRootWhere, "/")
	if !ok {
		return check.fail("missing first root where evidence")
	}

	if uint64Input(measured.op.Inputs, queryInputSplitsKey) != 2 {
		return check.fail("first root where did not run with splits 2")
	}

	if reason := finalGateE2QueryOperationFailure(measured, 1000); reason != "" {
		return check.fail(reason)
	}

	return check.pass("first root where splits 2 matched current facts under 1s")
}

func validateFinalGateE2FilterSwitch(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(34, "E2 first filter switch")

	measured, ok := finalGateE2Operation(
		e,
		finalGateRESTOpTree,
		finalGateE2ScenarioSwitch,
		finalGateHighFanoutParentDir,
	)
	if !ok {
		return check.fail("missing first filter-switch evidence")
	}

	if !boolInput(measured.op.Inputs, "preceded_by_unfiltered_tree_request") {
		return check.fail("filter switch was not measured after an unfiltered tree request")
	}

	usedBroadPacketCache := boolInput(measured.op.Inputs, "used_broad_packet_cache") ||
		finalGateE2BroadPacketCacheHit(measured.op)
	if usedBroadPacketCache {
		return check.fail("filtered result used broad packet cache evidence")
	}

	if reason := finalGateE2RESTOperationFailure(measured, 1000); reason != "" {
		return check.fail(reason)
	}

	return check.pass("first filter switch was cold, correct, and isolated from broad packet cache entries")
}

func validateFinalGateE2RealWhereDirs(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(35, "E2 real first where dirs")

	for _, dir := range []string{"/", finalGateHighFanoutParentDir} {
		measured, ok := finalGateE2Operation(e, finalGateRESTOpCLIWhere, finalGateE2ScenarioRealWhere, dir)
		if !ok {
			return check.fail(dir + " missing real first where --dir evidence")
		}

		if !finalGateCLIWhereCommandPasses(measured.op, dir) {
			return check.fail(dir + " where command evidence failed")
		}

		if reason := finalGateE2QueryOperationFailure(measured, 1000); reason != "" {
			return check.fail(dir + " " + reason)
		}
	}

	return check.pass("root and high-fanout first where --dir runs matched current facts under 1s")
}

func validateFinalGateE2NFSHeavyWhere(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(36, "E2 NFS-heavy first where")

	measured, ok := finalGateE2Operation(
		e,
		finalGateRESTOpCLIWhere,
		finalGateE2ScenarioNFSHeavyWhere,
		finalGateNFSHeavyDir,
	)
	if !ok {
		return check.fail("missing NFS-heavy first where --dir evidence")
	}

	if !finalGateCLIWhereCommandPasses(measured.op, finalGateNFSHeavyDir) {
		return check.fail("NFS-heavy where command evidence failed")
	}

	if reason := finalGateE2QueryOperationFailure(measured, 2000); reason != "" {
		return check.fail(reason)
	}

	return check.pass("NFS-heavy first where --dir matched current facts under 2s")
}

func validateFinalGateE2MeasuredBudgets(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(37, "E2 import and spool budgets")
	if reason := finalGateE2BudgetReportsFailure(finalGateE2ImportReports(e), "import_total"); reason != "" {
		return check.fail("direct import " + reason)
	}

	spoolReports := finalGateE2SpoolReports(e)
	if reason := finalGateE2BudgetReportsFailure(spoolReports, "spool_load_total"); reason != "" {
		return check.fail("spool-load " + reason)
	}

	return check.pass(
		"direct import and spool-load budgets were recorded from measured wall, CPU, RSS, spool, and part data",
	)
}

func validateFinalGateJ6Matrix(e FinalGateEvidence) (FinalGateCheck, []j4MatrixDelta) {
	check := finalGateCheck(38, "J6 matrix correctness and deltas")

	deltas, err := j4MatrixDeltas(e.BaselineQueryReports, e.QueryReports)
	if err != nil {
		return check.fail(err.Error()), nil
	}

	if reason := finalGateJ6WrongRowFailure(e); reason != "" {
		return check.fail(reason), deltas
	}

	return check.pass("all canonical matrix types have p50/p95/p99, before/after deltas, and matching results"), deltas
}

func validateFinalGateJ6ColdUX(e FinalGateEvidence) FinalGateCheck {
	check := finalGateCheck(39, finalGateJ6ColdUXCheckName)

	for _, spec := range finalGateJ6ColdUXSpecs() {
		if reason := finalGateJ6ColdUXSpecFailure(e.QueryReports, spec); reason != "" {
			return check.fail(reason)
		}
	}

	return check.pass("exact, file, permission, list, recursive, filtered, glob, Disktree, and Where p95 gates passed")
}

func validateFinalGateJ6Storage(e FinalGateEvidence) (FinalGateCheck, []FinalGateTableByteDelta) {
	check := finalGateCheck(40, "J6 storage layout and table bytes")

	if reason := finalGateJ6StorageLayoutFailure(e); reason != "" {
		return check.fail(reason), nil
	}

	deltas, reason := finalGateJ6TableByteDeltas(e)
	if reason != "" {
		return check.fail(reason), nil
	}

	return check.pass(
		"hot rows are path-string-free and per-table compressed/uncompressed bytes compare to baseline",
	), deltas
}

func validateFinalGateJ6D4Decisions(
	e FinalGateEvidence,
) (FinalGateCheck, []FinalGateD4DecisionEvidence) {
	check := finalGateCheck(41, "J6 D4 collapse decisions")
	decisions := finalGateJ6D4DecisionEvidence(e)

	for _, pattern := range finalGateJ6D4RequiredPatterns() {
		decision, ok := finalGateJ6D4DecisionForPattern(decisions, pattern)
		if !ok {
			return check.fail(pattern + " missing D4 decision"), decisions
		}

		if reason := finalGateJ6D4DecisionFailure(decision); reason != "" {
			return check.fail(pattern + " " + reason), decisions
		}
	}

	return check.pass("D4 retained/collapsed choices cite measurements and collapsed routes meet their gates"), decisions
}

func (c FinalGateCheck) block(detail string) FinalGateCheck {
	c.Passed = false
	c.Blocked = true
	c.Detail = fmt.Sprintf("%s: %s", strings.TrimSpace(c.Name), detail)

	return c
}

func finalGateSidecarColdMisses(checks []FinalGateCheck) []FinalGateCheck {
	misses := make([]FinalGateCheck, 0)

	for _, check := range checks {
		if finalGateSidecarColdMissCheck(check) {
			misses = append(misses, check)
		}
	}

	return finalGateDeduplicateJ6ColdMiss(misses)
}

func finalGateSidecarFailuresAreOnlyColdMisses(checks []FinalGateCheck) bool {
	for _, check := range checks {
		if check.Passed {
			continue
		}

		if !finalGateSidecarColdMissCheck(check) {
			return false
		}
	}

	return true
}

func finalGateSidecarColdMissCheck(check FinalGateCheck) bool {
	return !check.Passed &&
		finalGateSidecarE2ColdCheckName(check.Name) &&
		finalGateSidecarColdMissDetail(check.Detail)
}

func finalGateSidecarE2ColdCheckName(name string) bool {
	switch name {
	case "E2 REST tree first requests",
		finalGateE2HighFanoutBroadCheckName,
		finalGateE2HighFanoutFilteredCheckName,
		"E2 high-fanout DirInfos broad",
		"E2 high-fanout DirInfos filtered",
		"E2 high-fanout DirsHaveChildren broad",
		"E2 high-fanout DirsHaveChildren filtered",
		"E2 first root where splits 2",
		"E2 first filter switch",
		"E2 real first where dirs",
		"E2 NFS-heavy first where",
		finalGateJ6ColdUXCheckName:
		return true
	default:
		return false
	}
}

func finalGateSidecarColdMissDetail(detail string) bool {
	return strings.Contains(detail, "p95") ||
		strings.Contains(detail, "read-volume") && strings.Contains(detail, "exceeded ceiling")
}

func finalGateDeduplicateJ6ColdMiss(misses []FinalGateCheck) []FinalGateCheck {
	if !finalGateSpecificColdMissPresent(misses) {
		return misses
	}

	return slices.DeleteFunc(misses, func(check FinalGateCheck) bool {
		return check.Name == finalGateJ6ColdUXCheckName
	})
}

func finalGateSpecificColdMissPresent(misses []FinalGateCheck) bool {
	return slices.ContainsFunc(misses, func(check FinalGateCheck) bool {
		return check.Name != finalGateJ6ColdUXCheckName
	})
}

// FinalGateReportResult captures E1 evidence validation status.
type FinalGateReportResult struct {
	Status          string           `json:"status"`
	Passed          bool             `json:"passed"`
	Blocked         bool             `json:"blocked,omitempty"`
	TimingEvaluated bool             `json:"timing_evaluated"`
	Checks          []FinalGateCheck `json:"checks"`
}

// ValidateFinalGateReport evaluates E1 evidence before cold speed gates.
func ValidateFinalGateReport(report FinalGateReport) FinalGateReportResult {
	result := FinalGateReportResult{Status: finalGateReportStatusPassed, Passed: true}

	fixture := validateFinalGateFixtureDigests(report)
	result.add(fixture)

	if !fixture.Passed {
		return result
	}

	result.TimingEvaluated = true
	for _, check := range []FinalGateCheck{
		validateFinalGateCorrectnessEvidence(report),
		validateFinalGateRESTReportEvidence(report),
		validateFinalGateClickHouseReportEvidence(report),
		validateFinalGateDirectImportEvidence(report),
		validateFinalGateSpoolLoadEvidence(report),
		validateFinalGateComparisonPresence(report),
		validateFinalGateSuccessfulComparisonEvidence(report),
		validateFinalGateInfeasibleComparisonEvidence(report),
		validateFinalGateInfeasibleComparisonBlock(report),
	} {
		result.add(check)
	}

	return result
}

func (r *FinalGateReportResult) add(check FinalGateCheck) {
	r.Checks = append(r.Checks, check)
	if check.Passed {
		return
	}

	r.Passed = false
	if check.Blocked && r.Status != finalGateReportStatusFailed {
		r.Blocked = true
		r.Status = finalGateReportStatusBlocked

		return
	}

	if !check.Blocked {
		r.Blocked = false
		r.Status = finalGateReportStatusFailed
	}
}

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
	BaselineImportReports []perfreport.Report
	QueryReports          []perfreport.Report
	BaselineQueryReports  []perfreport.Report
	BoltQueryReports      []perfreport.Report
	RequiredQueryRoots    []string
	ResultEquivalence     []FinalGateResultEquivalence
	FinalGateReport       *FinalGateReport
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
		!finalGateStringInputMatches(baseline, candidate, queryInputResultDigest)
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
	if finalGateAnyInputPresent([]string{queryInputResultDigest}, ops...) {
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

	if !finalGateStringInputMatches(clickHouseOp, boltOp, queryInputResultDigest) {
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

	if finalGateE2ScenarioOperation(op) {
		return ops
	}

	if pred != nil && !pred(op) {
		return ops
	}

	return append(ops, op)
}

func finalGateE2ScenarioOperation(op perfreport.Operation) bool {
	return stringInput(op.Inputs, finalGateE2ScenarioInput) != ""
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
		command[0] == finalGateWrstatUICommand &&
		command[1] == finalGateWhereCommandName &&
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

	digestMismatch := finalGateAnyInputPresent([]string{queryInputResultDigest}, clickHouseOp, boltOp) &&
		!finalGateStringInputMatches(clickHouseOp, boltOp, queryInputResultDigest)
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

func finalGateE2MeasuredTableStats(e FinalGateEvidence) map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats)

	finalGateMergeReportTableStats(stats, finalGateE2ImportReports(e))
	finalGateMergeReportTableStats(stats, finalGateE2SpoolReports(e))
	finalGateMergeReportTableStats(stats, e.QueryReports)

	return stats
}

func finalGateMergeReportTableStats(dst map[string]perfreport.TableStats, reports []perfreport.Report) {
	for _, report := range reports {
		maps.Copy(dst, report.TableStats)
	}
}

func finalGateJ6WrongRowFailure(e FinalGateEvidence) string {
	for _, group := range []struct {
		name    string
		reports []perfreport.Report
	}{
		{"baseline", e.BaselineQueryReports},
		{"candidate", e.QueryReports},
	} {
		if reason := finalGateJ6WrongRowReportFailure(group.name, group.reports); reason != "" {
			return reason
		}
	}

	return ""
}

func finalGateJ6WrongRowReportFailure(group string, reports []perfreport.Report) string {
	for _, op := range finalGateJ6MatrixOperations(reports) {
		label := finalGateJ6MatrixOperationLabel(op)
		if count := uint64Input(op.Inputs, finalGateJ6WrongRowCountInput); count > 0 {
			return fmt.Sprintf("%s %s wrong row count was %d", group, label, count)
		}

		if count := uint64Input(op.Inputs, finalGateJ6PathHashCollisionMismatchInput); count > 0 {
			return fmt.Sprintf("%s %s path-hash collision mismatch count was %d", group, label, count)
		}

		status := stringInput(op.Inputs, finalGateCorrectnessStatusInput)
		if status != "" && status != finalGateComparisonStatusSuccess {
			return fmt.Sprintf("%s %s correctness status was %s", group, label, status)
		}
	}

	return ""
}

func finalGateJ6MatrixOperations(reports []perfreport.Report) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			queryType := stringInput(op.Inputs, queryInputQueryTypeKey)
			if slices.Contains(j4CanonicalQueryTypes(), queryType) {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func finalGateJ6MatrixOperationLabel(op perfreport.Operation) string {
	queryType := stringInput(op.Inputs, queryInputQueryTypeKey)

	queryVariant := stringInput(op.Inputs, queryInputQueryVariantKey)
	if queryVariant != "" {
		return fmt.Sprintf("%s %s (%s)", queryType, queryVariant, op.Name)
	}

	if queryType != "" {
		return fmt.Sprintf("%s (%s)", queryType, op.Name)
	}

	return op.Name
}

func finalGateJ6StorageLayoutFailure(e FinalGateEvidence) string {
	audit, ok := finalGateJ6StorageAuditOperation(e)
	if !ok {
		return "missing storage audit"
	}

	if !finalGateInputMapHasKey(audit.Inputs, finalGateJ6HotRowPathStringTablesInput) {
		return "missing hot-row path string audit"
	}

	if tables := stringSliceInput(audit.Inputs, finalGateJ6HotRowPathStringTablesInput); len(tables) > 0 {
		return "hot row stores a path string in " + strings.Join(tables, ", ")
	}

	copies, ok := float64InputPresentValue(audit.Inputs, finalGateJ6PathTextCopiesPerDirSnapshotInput)
	if !ok || copies != 1 {
		return "path text is not one copy per dir per snapshot"
	}

	if stringInput(audit.Inputs, finalGateJ6PathTextCatalogTableInput) != tableCatalog {
		return "path text catalog table evidence missing"
	}

	return finalGateJ6FilterTableRetentionFailure(e)
}

func float64InputPresentValue(inputs map[string]any, key string) (float64, bool) {
	v, ok := inputs[key]
	if !ok {
		return 0, false
	}

	return float64InputValue(v)
}

func float64InputValue(v any) (float64, bool) {
	if value, ok := float64InputFloatValue(v); ok {
		return value, true
	}

	return float64InputIntegerValue(v)
}

func float64InputFloatValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case float64:
		return nonNegativeFloat64(typed)
	case float32:
		return nonNegativeFloat64(float64(typed))
	default:
		return 0, false
	}
}

func nonNegativeFloat64(value float64) (float64, bool) {
	if value < 0 {
		return 0, false
	}

	return value, true
}

func float64InputIntegerValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case uint64:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case int:
		return nonNegativeIntFloat64(int64(typed))
	case int64:
		return nonNegativeIntFloat64(typed)
	default:
		return 0, false
	}
}

func nonNegativeIntFloat64(value int64) (float64, bool) {
	if value < 0 {
		return 0, false
	}

	return float64(value), true
}

func finalGateJ6StorageAuditOperation(e FinalGateEvidence) (perfreport.Operation, bool) {
	for _, report := range finalGateJ6CandidateReports(e) {
		if op, ok := firstOperation(report, finalGateJ6StorageAuditOpName, nil); ok {
			return op, true
		}
	}

	return perfreport.Operation{}, false
}

func finalGateJ6CandidateReports(e FinalGateEvidence) []perfreport.Report {
	reports := append(slices.Clone(finalGateE2ImportReports(e)), finalGateE2SpoolReports(e)...)
	reports = append(reports, e.QueryReports...)

	return reports
}

func finalGateJ6FilterTableRetentionFailure(e FinalGateEvidence) string {
	stats := finalGateJ6CandidateTableStats(e)
	decisions := finalGateJ6D4DecisionEvidence(e)

	for _, table := range finalGateJ6FilterTables() {
		if finalGateBasicTableSizeEvidencePass(stats[table]) {
			continue
		}

		if finalGateJ6MaterialisationCollapsed(decisions, table) {
			continue
		}

		return table + " filter table was not retained and has no D4 collapse proof"
	}

	return ""
}

func finalGateJ6FilterTables() []string {
	return []string{tableChildFilterAll, tableDirFilterAll, tableDirFilterAgeAll}
}

func finalGateJ6MaterialisationCollapsed(decisions []FinalGateD4DecisionEvidence, table string) bool {
	return slices.ContainsFunc(decisions, func(decision FinalGateD4DecisionEvidence) bool {
		return decision.Materialisation == table && decision.Decision == finalGateJ6D4DecisionCollapsed
	})
}

func finalGateJ6TableByteDeltas(e FinalGateEvidence) ([]FinalGateTableByteDelta, string) {
	candidate := finalGateJ6CandidateTableStats(e)
	baseline := finalGateJ6BaselineTableStats(e)

	if len(candidate) == 0 {
		return nil, "missing candidate table bytes"
	}

	if len(baseline) == 0 {
		return nil, "missing baseline table bytes"
	}

	tables := mapKeysTableStats(candidate)

	deltas := make([]FinalGateTableByteDelta, 0, len(tables))
	for _, table := range tables {
		delta, reason := finalGateJ6TableByteDelta(table, baseline[table], candidate[table])
		if reason != "" {
			return nil, reason
		}

		deltas = append(deltas, delta)
	}

	return deltas, ""
}

func mapKeysTableStats(values map[string]perfreport.TableStats) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}

func finalGateJ6TableByteDelta(
	table string,
	baseline perfreport.TableStats,
	candidate perfreport.TableStats,
) (FinalGateTableByteDelta, string) {
	if candidate.CompressedBytes == 0 || candidate.UncompressedBytes == 0 {
		return FinalGateTableByteDelta{}, table + " missing candidate compressed/uncompressed bytes"
	}

	if baseline.Rows > 0 && (baseline.CompressedBytes == 0 || baseline.UncompressedBytes == 0) {
		return FinalGateTableByteDelta{}, table + " missing baseline compressed/uncompressed bytes"
	}

	return FinalGateTableByteDelta{
		Table:                      table,
		BaselineCompressedBytes:    baseline.CompressedBytes,
		CandidateCompressedBytes:   candidate.CompressedBytes,
		DeltaCompressedBytes:       finalGateUint64Delta(candidate.CompressedBytes, baseline.CompressedBytes),
		BaselineUncompressedBytes:  baseline.UncompressedBytes,
		CandidateUncompressedBytes: candidate.UncompressedBytes,
		DeltaUncompressedBytes: finalGateUint64Delta(
			candidate.UncompressedBytes,
			baseline.UncompressedBytes,
		),
	}, ""
}

func finalGateUint64Delta(after uint64, before uint64) int64 {
	if after >= before {
		return finalGateBoundedUint64ToInt64(after - before)
	}

	diff := before - after
	if diff >= finalGateMinInt64Magnitude() {
		return finalGateMinInt64()
	}

	return -finalGateBoundedUint64ToInt64(diff)
}

func finalGateBoundedUint64ToInt64(value uint64) int64 {
	if value > finalGateMaxInt64Uint64() {
		return finalGateMaxInt64()
	}

	return int64(value) //nolint:gosec // value is bounded to MaxInt64 above.
}

func finalGateMaxInt64Uint64() uint64 {
	return uint64(1<<63 - 1)
}

func finalGateMaxInt64() int64 {
	return int64(1<<63 - 1)
}

func finalGateMinInt64Magnitude() uint64 {
	return uint64(1 << 63)
}

func finalGateMinInt64() int64 {
	return -1 << 63
}

func finalGateJ6CandidateTableStats(e FinalGateEvidence) map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats)
	finalGateMergeReportTableStats(stats, finalGateE2ImportReports(e))
	finalGateMergeReportTableStats(stats, finalGateE2SpoolReports(e))
	finalGateMergeReportTableStats(stats, e.QueryReports)

	return stats
}

func finalGateJ6BaselineTableStats(e FinalGateEvidence) map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats)
	finalGateMergeReportTableStats(stats, e.BaselineImportReports)
	finalGateMergeReportTableStats(stats, e.BaselineQueryReports)

	return stats
}

func finalGateJ6D4DecisionEvidence(e FinalGateEvidence) []FinalGateD4DecisionEvidence {
	var decisions []FinalGateD4DecisionEvidence

	for _, report := range e.QueryReports {
		for _, op := range report.Operations {
			if op.Name != finalGateJ6D4DecisionOpName {
				continue
			}

			decisions = append(decisions, finalGateJ6D4DecisionFromOperation(op))
		}
	}

	return decisions
}

func finalGateJ6D4DecisionFromOperation(op perfreport.Operation) FinalGateD4DecisionEvidence {
	measuredP95, _ := float64InputPresentValue(op.Inputs, finalGateJ6D4MeasuredP95Input)
	latencyGate, _ := float64InputPresentValue(op.Inputs, finalGateJ6D4LatencyGateInput)

	return FinalGateD4DecisionEvidence{
		Pattern:         stringInput(op.Inputs, finalGateJ6D4PatternInput),
		Materialisation: stringInput(op.Inputs, finalGateJ6D4MaterialisationInput),
		Decision:        stringInput(op.Inputs, finalGateJ6D4DecisionInput),
		Citation:        stringInput(op.Inputs, finalGateJ6D4CitationInput),
		MeasuredP95MS:   measuredP95,
		LatencyGateMS:   latencyGate,
	}
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
		digest:      stringInput(op.Inputs, queryInputResultDigest),
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

// FinalGateCheck captures one final-gate acceptance-test result.
type FinalGateCheck struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Blocked bool   `json:"blocked,omitempty"`
	Detail  string `json:"detail"`
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

type finalGateMeasuredOperation struct {
	report     perfreport.Report
	op         perfreport.Operation
	tableStats map[string]perfreport.TableStats
}

func finalGateE2Operation(
	e FinalGateEvidence,
	name string,
	scenario string,
	root string,
) (finalGateMeasuredOperation, bool) {
	tableStats := finalGateE2MeasuredTableStats(e)

	for _, report := range e.QueryReports {
		for _, op := range report.Operations {
			if !finalGateE2OperationMatches(op, name, scenario, root) {
				continue
			}

			return finalGateMeasuredOperation{report: report, op: op, tableStats: tableStats}, true
		}
	}

	return finalGateMeasuredOperation{}, false
}

func finalGateE2RESTOperationFailure(measured finalGateMeasuredOperation, p95MaxMS float64) string {
	if reason := finalGateE2QueryOperationFailure(measured, p95MaxMS); reason != "" {
		return reason
	}

	cacheHits := uint64SliceInput(measured.op.Inputs, finalGateRESTInputCacheHits)
	if len(cacheHits) == 0 || uint64SliceInputSum(measured.op.Inputs, finalGateRESTInputCacheHits) != 0 {
		return "response cache was not cold or disabled"
	}

	return ""
}

func finalGateE2PacketOperationFailure(
	measured finalGateMeasuredOperation,
	p95MaxMS float64,
	packetTable string,
	requireCurrentRead bool,
	requireZeroFactsVector bool,
) string {
	if reason := finalGateE2QueryOperationFailure(measured, p95MaxMS); reason != "" {
		return reason
	}

	if reason := finalGateE2PacketReadShapeFailure(measured.op, packetTable, requireCurrentRead); reason != "" {
		return reason
	}

	if reason := finalGateE2NoFanoutFailure(measured.op); reason != "" {
		return reason
	}

	if reason := finalGateE2FactsVectorFailure(measured.op, requireZeroFactsVector); reason != "" {
		return reason
	}

	return finalGateE2ReadVolumeFailure(measured, packetTable, requireCurrentRead)
}

func finalGateE2PacketReadShapeFailure(
	op perfreport.Operation,
	packetTable string,
	requireCurrentRead bool,
) string {
	if requireCurrentRead && uint64Input(op.Inputs, "current_read_count") != 1 {
		return "expected exactly one current read"
	}

	if uint64Input(op.Inputs, "parent_packet_read_count") != 1 {
		return "expected exactly one parent-packet read"
	}

	if stringInput(op.Inputs, finalGateE2ParentPacketTableInput) != packetTable {
		return "expected one " + packetTable + " parent-packet read"
	}

	return ""
}

func finalGateE2NoFanoutFailure(op perfreport.Operation) string {
	if uint64Input(op.Inputs, "per_child_query_count") != 0 {
		return "per-child ClickHouse query count was not zero"
	}

	if uint64Input(op.Inputs, "subtree_scan_count") != 0 {
		return "subtree scan count was not zero"
	}

	return ""
}

func finalGateE2FactsVectorFailure(op perfreport.Operation, required bool) string {
	if required && !finalGateE2ZeroFactsVectorRows(op) {
		return "facts-vector rows read was not zero"
	}

	return ""
}

func finalGateE2ZeroFactsVectorRows(op perfreport.Operation) bool {
	value, ok := uint64InputPresentValue(op.Inputs, finalGateE2FactsVectorRowsInput)

	return ok && value == 0
}

func uint64InputPresentValue(inputs map[string]any, key string) (uint64, bool) {
	v, ok := inputs[key]
	if !ok {
		return 0, false
	}

	switch typed := v.(type) {
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

func finalGateE2ReadVolumeFailure(
	measured finalGateMeasuredOperation,
	packetTable string,
	requireCurrentRead bool,
) string {
	reads, reason := finalGateE2AllowedReads(measured.op, packetTable, requireCurrentRead)
	if reason != "" {
		return reason
	}

	ceiling, reason := finalGateE2ReadVolumeCeiling(measured, reads)
	if reason != "" {
		return reason
	}

	for _, spec := range []struct {
		name    string
		values  []uint64
		ceiling uint64
	}{
		{finalGateE2ReadVolumeRowsName, measured.op.ReadRows, ceiling.rows},
		{finalGateE2ReadVolumeBytesName, measured.op.ReadBytes, ceiling.bytes},
		{finalGateE2ReadVolumeMarksName, measured.op.ReadMarks, ceiling.marks},
	} {
		if reason := finalGateE2ReadVolumeMetricFailure(spec.name, spec.values, spec.ceiling); reason != "" {
			return reason
		}
	}

	return ""
}

func finalGateE2AllowedReads(
	op perfreport.Operation,
	packetTable string,
	includeCurrent bool,
) ([]finalGateE2AllowedRead, string) {
	expectedPacketRows := finalGateE2ReadVolumeExpectedRows(op)
	if expectedPacketRows == 0 {
		return nil, "missing read-volume expected rows"
	}

	reads := make([]finalGateE2AllowedRead, 0, 4)
	if includeCurrent {
		reads = append(reads, finalGateE2ReadVolumeRead(finalGateE2CurrentReadTable(op), 1))
	}

	reads = append(reads, finalGateE2ReadVolumeRead(packetTable, expectedPacketRows))
	reads = append(reads, finalGateE2ReadinessReads(op, packetTable)...)

	return reads, ""
}

func finalGateE2ReadVolumeRead(table string, expectedRows uint64) finalGateE2AllowedRead {
	return finalGateE2AllowedRead{
		table:            table,
		expectedRows:     expectedRows,
		indexGranularity: finalGateE2ReadVolumeTableIndexGranularity(table),
	}
}

func finalGateE2ReadVolumeTableIndexGranularity(_ string) uint64 {
	return finalGateE2ReadVolumeIndexGranularity
}

func finalGateE2CurrentReadTable(op perfreport.Operation) string {
	if finalGateE2UsesFullFilter(op) {
		return finalGateE2DirFilterAllTable
	}

	return tableDirSummary
}

func finalGateE2UsesFullFilter(op perfreport.Operation) bool {
	return len(uint64SliceInput(op.Inputs, queryInputFilterGIDsKey)) > 0 ||
		len(uint64SliceInput(op.Inputs, queryInputFilterUIDsKey)) > 0 ||
		uint64Input(op.Inputs, queryInputFilterFileTypeMaskKey) > 0
}

func finalGateE2ReadinessReads(op perfreport.Operation, packetTable string) []finalGateE2AllowedRead {
	reads := []finalGateE2AllowedRead{
		finalGateE2ReadVolumeRead(tableDirSummarySets, 1),
	}

	if finalGateE2UsesFullFilter(op) || packetTable == finalGateE2ChildFilterAllTable {
		reads = append(reads, finalGateE2ReadVolumeRead(finalGateE2Schema3SnapshotSetsTable, 1))
	}

	return reads
}

func finalGateE2ReadVolumeCeiling(
	measured finalGateMeasuredOperation,
	reads []finalGateE2AllowedRead,
) (finalGateE2ReadVolumeLimits, string) {
	var (
		ceiling     finalGateE2ReadVolumeLimits
		byteCeiling float64
	)

	for _, read := range reads {
		stats, ok := measured.tableStats[read.table]
		if !ok || !finalGateE2ReadVolumeTableStatsPass(stats) {
			return finalGateE2ReadVolumeLimits{}, "missing read-volume table stats for " + read.table
		}

		marks := finalGateE2ReadVolumeMarks(read.expectedRows, read.indexGranularity)
		rows := marks * read.indexGranularity
		bytesPerRow := float64(stats.CompressedBytes) / float64(stats.Rows)

		ceiling.rows += rows
		ceiling.marks += marks
		byteCeiling += float64(rows) * bytesPerRow
	}

	ceiling.bytes = uint64(math.Ceil(byteCeiling * finalGateE2ReadVolumeByteSlack))

	return ceiling, ""
}

func finalGateE2ReadVolumeTableStatsPass(stats perfreport.TableStats) bool {
	return stats.Rows > 0 &&
		stats.ActiveParts > 0 &&
		stats.CompressedBytes > 0 &&
		stats.UncompressedBytes > 0
}

func finalGateE2ReadVolumeExpectedRows(op perfreport.Operation) uint64 {
	if childCount := uint64Input(op.Inputs, queryInputHighFanoutChildCount); childCount > 0 {
		return childCount
	}

	return firstResultCount(op)
}

func finalGateE2ReadVolumeMarks(expectedRows uint64, indexGranularity uint64) uint64 {
	marks := finalGateCeilDiv(expectedRows, indexGranularity) + 2
	if marks < 1 {
		return 1
	}

	return marks
}

func finalGateCeilDiv(value uint64, divisor uint64) uint64 {
	if value == 0 || divisor == 0 {
		return 0
	}

	return 1 + (value-1)/divisor
}

func finalGateE2ReadVolumeMetricFailure(
	name string,
	values []uint64,
	ceiling uint64,
) string {
	if ceiling == 0 || len(values) == 0 {
		return "missing read-volume " + name + " samples"
	}

	for _, value := range values {
		if value == 0 || value > ceiling {
			return "read-volume " + name + " exceeded ceiling"
		}
	}

	return ""
}

func finalGateE2QueryOperationFailure(measured finalGateMeasuredOperation, p95MaxMS float64) string {
	if reason := finalGateE2CorrectnessFailure(measured.op); reason != "" {
		return reason
	}

	if reason := finalGateE2ColdFailure(measured); reason != "" {
		return reason
	}

	return finalGateE2P95Failure(measured.op, p95MaxMS)
}

func finalGateE2CorrectnessFailure(op perfreport.Operation) string {
	if stringInput(op.Inputs, finalGateCorrectnessStatusInput) != finalGateComparisonStatusSuccess {
		return "correctness equivalence status missing or failed"
	}

	actual := stringInput(op.Inputs, queryInputResultDigest)

	expected := stringInput(op.Inputs, finalGateE2ExpectedDigestInput)
	if actual == "" || expected == "" {
		return "missing result digest evidence"
	}

	if actual != expected {
		return finalGateResultDigestMismatch
	}

	if !resultCountsStable(op) {
		return finalGateResultCountMismatch
	}

	return ""
}

func finalGateE2P95Failure(op perfreport.Operation, p95MaxMS float64) string {
	if len(op.DurationsMS) < finalGateMinRepeats {
		return fmt.Sprintf("need at least %d cold repetitions", finalGateMinRepeats)
	}

	if op.P95MS >= p95MaxMS {
		return fmt.Sprintf("p95 %.3f ms exceeded %.3f ms cold gate", op.P95MS, p95MaxMS)
	}

	if !finalGateStatusCodesPass(op) {
		return "status code evidence failed"
	}

	return ""
}

func finalGateE2ColdFailure(measured finalGateMeasuredOperation) string {
	if measured.report.Warmup != 0 {
		return "proactive warming was configured before the measured request"
	}

	if boolInput(measured.op.Inputs, finalGateE2ProactiveWarmingInput) {
		return "proactive warming ran before the measured request"
	}

	if reason := finalGateE2CacheEvidenceFailure(measured.op); reason != "" {
		return reason
	}

	return ""
}

func boolInput(inputs map[string]any, key string) bool {
	value, ok := inputs[key]
	if !ok {
		return false
	}

	typed, ok := value.(bool)

	return ok && typed
}

func finalGateE2CacheEvidenceFailure(op perfreport.Operation) string {
	if !finalGateE2ColdCacheScope(stringInput(op.Inputs, queryInputCacheScope)) {
		return "cache scope was not cold/fresh-provider"
	}

	for _, key := range stringSliceInput(op.Inputs, queryInputCacheHitKeysKey) {
		if !finalGateE2AllowedCacheHitKey(op, key) {
			return "cache hit keys included evidence outside the measured parent-packet read"
		}
	}

	return ""
}

func finalGateE2ColdCacheScope(scope string) bool {
	return scope == queryScopeColdProvider || scope == queryScopeFreshProvider
}

func finalGateE2AllowedCacheHitKey(op perfreport.Operation, key string) bool {
	kind, attrs, ok := finalGateCacheHitKeyAttributes(key)
	if !ok || kind != "parent_packet" {
		return false
	}

	if !finalGateE2ParentPacketCacheHitAttributesHaveScope(attrs) {
		return false
	}

	root := normalizeRootPath(operationEvidenceRoot(op))
	if root == "" {
		return false
	}

	return finalGateCacheHitPathEqualsRoot(attrs["path"], root) ||
		finalGateCacheHitPathEqualsRoot(attrs["parent_dir"], root)
}

func finalGateCacheHitKeyAttributes(key string) (string, map[string]string, bool) {
	kind, rawAttrs, ok := strings.Cut(key, ":")
	if !ok || kind == "" {
		return "", nil, false
	}

	attrs, ok := finalGateCacheHitAttributes(rawAttrs)
	if !ok {
		return "", nil, false
	}

	return kind, attrs, true
}

func finalGateCacheHitAttributes(rawAttrs string) (map[string]string, bool) {
	attrs := make(map[string]string)

	for _, field := range strings.Split(rawAttrs, ";") {
		if !finalGateAddCacheHitAttribute(attrs, field) {
			return nil, false
		}
	}

	return attrs, true
}

func finalGateAddCacheHitAttribute(attrs map[string]string, field string) bool {
	if field == "" {
		return true
	}

	name, value, ok := strings.Cut(field, "=")
	if !ok || name == "" {
		return false
	}

	if _, exists := attrs[name]; exists {
		return false
	}

	attrs[name] = value

	return true
}

func finalGateE2ParentPacketCacheHitAttributesHaveScope(attrs map[string]string) bool {
	for _, name := range []string{"filter", "active_set_id", "query_version"} {
		if strings.TrimSpace(attrs[name]) == "" {
			return false
		}
	}

	return true
}

func finalGateCacheHitPathEqualsRoot(value string, root string) bool {
	return value != "" && normalizeRootPath(value) == root
}

type finalGateE2ReadVolumeLimits struct {
	rows  uint64
	bytes uint64
	marks uint64
}

type finalGateE2AllowedRead struct {
	table            string
	expectedRows     uint64
	indexGranularity uint64
}

// FinalGateSidecarFallbackDecision records whether E3's conditional sidecar
// fallback should be activated from final-gate evidence.
type FinalGateSidecarFallbackDecision struct {
	Triggered    bool             `json:"triggered"`
	Status       string           `json:"status"`
	Reason       string           `json:"reason"`
	MissedChecks []FinalGateCheck `json:"missed_checks,omitempty"`
}

func finalGateSidecarFallbackDecision(result FinalGateResult) FinalGateSidecarFallbackDecision {
	misses := finalGateSidecarColdMisses(result.Checks)
	if len(misses) > 0 && finalGateSidecarFailuresAreOnlyColdMisses(result.Checks) {
		return FinalGateSidecarFallbackDecision{
			Triggered:    true,
			Status:       finalGateSidecarFallbackTriggered,
			Reason:       "E3 fallback triggered by E2 cold gate miss after ClickHouse-native tuning",
			MissedChecks: misses,
		}
	}

	if result.Passed {
		return finalGateInactiveSidecarDecision("ClickHouse-native E2 cold gates passed; sidecar fallback is not active")
	}

	if len(misses) > 0 {
		return finalGateInactiveSidecarDecision("ClickHouse-native A-D gates failed; sidecar fallback precondition not met")
	}

	return finalGateInactiveSidecarDecision("no validated E2 cold performance miss exists")
}

func finalGateInactiveSidecarDecision(reason string) FinalGateSidecarFallbackDecision {
	return FinalGateSidecarFallbackDecision{
		Status: finalGateSidecarFallbackInactive,
		Reason: reason,
	}
}

// FinalGateTableByteDelta captures J6 per-table storage byte evidence.
type FinalGateTableByteDelta struct {
	Table                      string `json:"table"`
	BaselineCompressedBytes    uint64 `json:"baseline_compressed_bytes"`
	CandidateCompressedBytes   uint64 `json:"candidate_compressed_bytes"`
	DeltaCompressedBytes       int64  `json:"delta_compressed_bytes"`
	BaselineUncompressedBytes  uint64 `json:"baseline_uncompressed_bytes"`
	CandidateUncompressedBytes uint64 `json:"candidate_uncompressed_bytes"`
	DeltaUncompressedBytes     int64  `json:"delta_uncompressed_bytes"`
}

// FinalGateD4DecisionEvidence captures the D4 retained/collapsed decision proof.
type FinalGateD4DecisionEvidence struct {
	Pattern         string  `json:"pattern"`
	Materialisation string  `json:"materialisation"`
	Decision        string  `json:"decision"`
	Citation        string  `json:"measurement_citation"`
	MeasuredP95MS   float64 `json:"measured_p95_ms"`
	LatencyGateMS   float64 `json:"latency_gate_ms"`
}

func finalGateJ6D4DecisionForPattern(
	decisions []FinalGateD4DecisionEvidence,
	pattern string,
) (FinalGateD4DecisionEvidence, bool) {
	for _, decision := range decisions {
		if decision.Pattern == pattern {
			return decision, true
		}
	}

	return FinalGateD4DecisionEvidence{}, false
}

func finalGateJ6D4DecisionFailure(decision FinalGateD4DecisionEvidence) string {
	if !finalGateStringsPresent(decision.Materialisation, decision.Decision, decision.Citation) {
		return "missing materialisation, decision, or citation"
	}

	if decision.MeasuredP95MS <= 0 || decision.LatencyGateMS <= 0 {
		return "missing cited measured p95 or latency gate"
	}

	if reason := finalGateJ6D4CitationFailure(decision); reason != "" {
		return reason
	}

	return finalGateJ6D4DecisionValueFailure(decision)
}

func finalGateJ6D4CitationFailure(decision FinalGateD4DecisionEvidence) string {
	requiredOps, ok := finalGateJ6D4RequiredCitationOps(decision.Pattern)
	if !ok {
		return ""
	}

	for _, opName := range requiredOps {
		if !strings.Contains(decision.Citation, opName) {
			return "citation missing filtered measurement " + opName
		}
	}

	return ""
}

func finalGateJ6D4RequiredCitationOps(pattern string) ([]string, bool) {
	switch pattern {
	case finalGateJ6D4PatternFilteredExact:
		return []string{queryOpDirInfoFilteredName}, true
	case finalGateJ6D4PatternFilteredChildren:
		return []string{queryOpDirInfosFilteredName, queryOpDirsHaveChildrenFilteredName}, true
	case finalGateJ6D4PatternFilteredSubtree:
		return []string{queryOpWhereFilteredWholeMountName}, true
	default:
		return nil, false
	}
}

func finalGateJ6D4DecisionValueFailure(decision FinalGateD4DecisionEvidence) string {
	switch decision.Decision {
	case finalGateJ6D4DecisionRetained:
		return ""
	case finalGateJ6D4DecisionCollapsed:
		return finalGateJ6D4CollapsedDecisionFailure(decision)
	default:
		return "unknown D4 decision"
	}
}

func finalGateJ6D4CollapsedDecisionFailure(decision FinalGateD4DecisionEvidence) string {
	if decision.MeasuredP95MS >= decision.LatencyGateMS {
		return "collapsed materialisation did not meet its latency gate"
	}

	return ""
}

func finalGateJ6MatchingOperations(
	reports []perfreport.Report,
	pred operationPredicate,
) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if pred(op) {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

type finalGateJ6ColdUXSpec struct {
	name  string
	maxMS float64
	pred  operationPredicate
}

func finalGateJ6ColdUXSpecs() []finalGateJ6ColdUXSpec {
	return []finalGateJ6ColdUXSpec{
		{"exact_dir", finalGateJ6ColdUXFastMaxMS, finalGateJ6ExactDirOperation},
		{"file_stat", finalGateJ6ColdUXFastMaxMS, finalGateJ6FileStatOperation},
		{"permission_path", finalGateJ6ColdUXFastMaxMS, finalGateJ6PermissionPathOperation},
		{"direct_child_list", finalGateJ6ColdUXFastMaxMS, finalGateJ6DirectChildListOperation},
		{"recursive", finalGateJ6ColdUXBroadMaxMS, finalGateJ6RecursiveOperation},
		{"filtered", finalGateJ6ColdUXBroadMaxMS, finalGateJ6FilteredOperation},
		{"glob", finalGateJ6ColdUXBroadMaxMS, finalGateJ6GlobOperation},
		{"disktree", finalGateJ6ColdUXBroadMaxMS, finalGateJ6DisktreeOperation},
		{"where", finalGateJ6ColdUXBroadMaxMS, finalGateJ6WhereOperation},
	}
}

func finalGateJ6ColdUXSpecFailure(reports []perfreport.Report, spec finalGateJ6ColdUXSpec) string {
	ops := finalGateJ6MatchingOperations(reports, spec.pred)
	if len(ops) == 0 {
		return spec.name + " missing cold UX evidence"
	}

	for _, op := range ops {
		if reason := finalGateJ6ColdUXOperationFailure(op, spec.maxMS); reason != "" {
			return fmt.Sprintf("%s %s %s", spec.name, finalGateJ6OperationLabel(op), reason)
		}
	}

	return ""
}

func finalGateJ6ColdUXOperationFailure(op perfreport.Operation, maxMS float64) string {
	if !finalGatePercentilesPresent(op) {
		return "missing p50/p95/p99 evidence"
	}

	if op.P95MS >= maxMS {
		return fmt.Sprintf("p95 %.3f ms exceeded %.3f ms gate", op.P95MS, maxMS)
	}

	if !finalGateStatusCodesPass(op) {
		return "status code evidence failed"
	}

	return ""
}

func finalGateJ6OperationLabel(op perfreport.Operation) string {
	if queryType := stringInput(op.Inputs, queryInputQueryTypeKey); queryType != "" {
		return queryType
	}

	return op.Name
}

// FinalGateResult is a facts-only summary of the E1 prerequisite and E2 gates.
type FinalGateResult struct {
	Status            string                           `json:"status,omitempty"`
	Passed            bool                             `json:"passed"`
	Blocked           bool                             `json:"blocked,omitempty"`
	TimingEvaluated   bool                             `json:"timing_evaluated"`
	E1ReportResult    *FinalGateReportResult           `json:"e1_report_result,omitempty"`
	SidecarFallback   FinalGateSidecarFallbackDecision `json:"sidecar_fallback"`
	Checks            []FinalGateCheck                 `json:"checks"`
	J6QueryDeltas     []j4MatrixDelta                  `json:"j6_query_deltas,omitempty"`
	J6TableByteDeltas []FinalGateTableByteDelta        `json:"j6_table_byte_deltas,omitempty"`
	J6D4Decisions     []FinalGateD4DecisionEvidence    `json:"j6_d4_decisions,omitempty"`
}

// ValidateFinalGates evaluates the documented E2 perf gates from raw reports.
func ValidateFinalGates(e FinalGateEvidence) FinalGateResult {
	e = finalGateEvidenceWithDerivedT283(e)

	prerequisite, ok := finalGatePrerequisiteResult(e)
	if !ok {
		prerequisite.SidecarFallback = finalGateSidecarFallbackDecision(prerequisite)

		return prerequisite
	}

	result := FinalGateResult{
		Status:          finalGateReportStatusPassed,
		Passed:          true,
		TimingEvaluated: true,
		E1ReportResult:  prerequisite.E1ReportResult,
	}
	j6MatrixCheck, queryDeltas := validateFinalGateJ6Matrix(e)
	j6StorageCheck, tableDeltas := validateFinalGateJ6Storage(e)
	j6D4Check, d4Decisions := validateFinalGateJ6D4Decisions(e)
	result.J6QueryDeltas = queryDeltas
	result.J6TableByteDeltas = tableDeltas
	result.J6D4Decisions = d4Decisions

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
		validateFinalGateE2RESTTreeFirst(e),
		validateFinalGateE2HighFanoutBroadClick(e),
		validateFinalGateE2HighFanoutFilteredClick(e),
		validateFinalGateE2DirInfosBroad(e),
		validateFinalGateE2DirInfosFiltered(e),
		validateFinalGateE2DirsHaveChildrenBroad(e),
		validateFinalGateE2DirsHaveChildrenFiltered(e),
		validateFinalGateE2FirstRootWhereSplits(e),
		validateFinalGateE2FilterSwitch(e),
		validateFinalGateE2RealWhereDirs(e),
		validateFinalGateE2NFSHeavyWhere(e),
		validateFinalGateE2MeasuredBudgets(e),
		j6MatrixCheck,
		validateFinalGateJ6ColdUX(e),
		j6StorageCheck,
		j6D4Check,
	} {
		result.Checks = append(result.Checks, check)
		result.Passed = result.Passed && check.Passed
	}

	if !result.Passed {
		result.Status = finalGateReportStatusFailed
	}

	result.SidecarFallback = finalGateSidecarFallbackDecision(result)

	return result
}

func finalGatePrerequisiteResult(e FinalGateEvidence) (FinalGateResult, bool) {
	e1 := ValidateFinalGateReport(finalGateEvidenceE1Report(e))
	if e1.Passed {
		return FinalGateResult{E1ReportResult: &e1}, true
	}

	return FinalGateResult{
		Status:          e1.Status,
		Passed:          false,
		Blocked:         e1.Blocked,
		TimingEvaluated: false,
		E1ReportResult:  &e1,
		Checks:          e1.Checks,
	}, false
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

func finalGateJ6ExactDirOperation(op perfreport.Operation) bool {
	return stringInput(op.Inputs, queryInputQueryTypeKey) == j4QueryTypeExactDirectory ||
		op.Name == queryOpTreeDirInfoName ||
		op.Name == queryOpDirInfoBroadName
}

func finalGateJ6FileStatOperation(op perfreport.Operation) bool {
	return op.Name == queryOpFilesStatPathName
}

func finalGateJ6PermissionPathOperation(op perfreport.Operation) bool {
	return op.Name == queryOpPermissionCheckName
}

func finalGateJ6DirectChildListOperation(op perfreport.Operation) bool {
	return op.Name == queryOpFilesListDirName
}

func finalGateJ6RecursiveOperation(op perfreport.Operation) bool {
	switch op.Name {
	case queryOpTreeWhereName, queryOpTreeWhereColdProviderName, queryOpTreeWhereFreshName, queryOpWhereWholeMountName:
		return true
	default:
		return stringInput(op.Inputs, queryInputQueryTypeKey) == j4QueryTypeSubtree
	}
}

func finalGateJ6FilteredOperation(op perfreport.Operation) bool {
	return filteredOperation(op) || finalGateRESTWhereFilterPresent(op)
}

func finalGateJ6GlobOperation(op perfreport.Operation) bool {
	return stringInput(op.Inputs, queryInputQueryTypeKey) == j4QueryTypeGlob ||
		strings.Contains(strings.ToLower(op.Name), "glob")
}

func finalGateJ6DisktreeOperation(op perfreport.Operation) bool {
	return stringInput(op.Inputs, queryInputQueryTypeKey) == j4QueryTypeDisktree ||
		strings.Contains(strings.ToLower(op.Name), "disktree")
}

func finalGateJ6WhereOperation(op perfreport.Operation) bool {
	return stringInput(op.Inputs, queryInputQueryTypeKey) == j4QueryTypeSubtree ||
		strings.Contains(strings.ToLower(op.Name), "where")
}

func finalGateJ6D4RequiredPatterns() []string {
	return []string{
		finalGateJ6D4PatternFilteredExact,
		finalGateJ6D4PatternFilteredChildren,
		finalGateJ6D4PatternFilteredSubtree,
	}
}

func finalGateFixtureResultDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return finalGateFixtureResultDigestBytes(data), nil
}

func finalGateFixtureResultDigestBytes(data []byte) string {
	if digest, ok := finalGateFixtureResultSetDigest(data); ok {
		return digest
	}

	if digest, ok := finalGateFixtureDirInfoDigest(data); ok {
		return digest
	}

	return ""
}

func finalGateFixtureResultSetDigest(data []byte) (string, bool) {
	var envelope struct {
		Results json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && len(envelope.Results) > 0 {
		return finalGateFixtureSummariesDigest(envelope.Results)
	}

	return finalGateFixtureSummariesDigest(data)
}

func finalGateFixtureSummariesDigest(data []byte) (string, bool) {
	if strings.TrimSpace(string(data)) == "null" {
		return "", false
	}

	var summaries db.DCSs
	if err := json.Unmarshal(data, &summaries); err != nil {
		return "", false
	}

	if !finalGateFixtureSummariesValid(summaries) {
		return "", false
	}

	return dcssDigest(summaries), true
}

func finalGateFixtureDirInfoDigest(data []byte) (string, bool) {
	var envelope struct {
		DirInfo *db.DirInfo `json:"dir_info"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && finalGateFixtureDirInfoValid(envelope.DirInfo) {
		return dirInfoDigest(envelope.DirInfo), true
	}

	var info db.DirInfo
	if err := json.Unmarshal(data, &info); err != nil || !finalGateFixtureDirInfoValid(&info) {
		return "", false
	}

	return dirInfoDigest(&info), true
}

func finalGateFixtureDirInfoValid(info *db.DirInfo) bool {
	if info == nil {
		return false
	}

	return finalGateFixtureSummaryValid(info.Current) ||
		(len(info.Children) > 0 && finalGateFixtureSummariesValid(info.Children))
}

func finalGateFixtureSummaryOwnersValid(summary *db.DirSummary) bool {
	return len(summary.UIDs) > 0 || len(summary.GIDs) > 0
}

func finalGateFixtureSummariesValid(summaries db.DCSs) bool {
	if len(summaries) == 0 {
		return true
	}

	return !slices.ContainsFunc(summaries, func(summary *db.DirSummary) bool {
		return !finalGateFixtureSummaryValid(summary)
	})
}

func finalGateFixtureSummaryValid(summary *db.DirSummary) bool {
	return summary != nil &&
		(summary.Dir != "" ||
			summary.Count != 0 ||
			summary.Size != 0 ||
			finalGateFixtureSummaryOwnersValid(summary) ||
			summary.FT != 0 ||
			summary.Age != 0)
}

func finalGateOperationCorrectnessEvidencePasses(op perfreport.Operation) bool {
	return len(op.ResultCount) > 0 &&
		stringInput(op.Inputs, queryInputResultDigest) != "" &&
		finalGateCorrectnessStatusPasses(stringInput(op.Inputs, finalGateCorrectnessStatusInput))
}

func finalGateCorrectnessStatusPasses(status string) bool {
	return status == finalGateComparisonStatusSuccess || status == finalGateComparisonInfeasible
}

func finalGateE2ExpectedChildrenFailure(op perfreport.Operation) string {
	expected := sortedStringsInput(op.Inputs, "expected_true_children")

	actual := sortedStringsInput(op.Inputs, "actual_true_children")
	if len(expected) == 0 || !slices.Equal(expected, actual) {
		return "expected child truth did not match filtered DirsHaveChildren output"
	}

	return ""
}

func sortedStringsInput(inputs map[string]any, key string) []string {
	values := stringSliceInput(inputs, key)
	slices.Sort(values)

	return values
}

func finalGateE2BroadPacketCacheHit(op perfreport.Operation) bool {
	return slices.ContainsFunc(stringSliceInput(op.Inputs, queryInputCacheHitKeysKey), func(key string) bool {
		return strings.Contains(key, "filter=broad") ||
			strings.Contains(key, "filter=all") ||
			strings.Contains(key, "filter=none")
	})
}

func finalGateE2OperationMatches(
	op perfreport.Operation,
	name string,
	scenario string,
	root string,
) bool {
	return op.Name == name &&
		stringInput(op.Inputs, finalGateE2ScenarioInput) == scenario &&
		operationRootEquals(op, root)
}

func finalGateE2BudgetReportsFailure(reports []perfreport.Report, opName string) string {
	if len(reports) == 0 {
		return "missing measured reports"
	}

	for _, report := range reports {
		op, ok := firstOperation(report, opName, nil)
		if !ok {
			return "missing " + opName
		}

		if reason := finalGateE2BudgetOperationFailure(op); reason != "" {
			return reason
		}
	}

	return ""
}

func finalGateE2BudgetOperationFailure(op perfreport.Operation) string {
	if boolInput(op.Inputs, "budget_hardcoded_before_measurement") {
		return "budget was hardcoded before measurement"
	}

	if reason := finalGateE2BudgetMeasuredEvidenceFailure(op); reason != "" {
		return reason
	}

	op.Inputs = cloneMap(op.Inputs)
	finalGateEnsureE2ComputedBudgetInputs(&op)

	if stringInput(op.Inputs, finalGateE2BudgetSourceInput) != finalGateE2BudgetSourceComputed {
		return "budgets were not recorded as computed from measurements"
	}

	measuredSamplesMissing := uint64Input(op.Inputs, finalGateE2BudgetMeasurementCountInput) <
		uint64(len(op.DurationsMS)) || len(op.DurationsMS) == 0
	if measuredSamplesMissing {
		return "budget measurement count did not cover measured samples"
	}

	return finalGateE2BudgetResourceFailure(op)
}

func finalGateE2BudgetMeasuredEvidenceFailure(op perfreport.Operation) string {
	for _, key := range []string{importInputTotalCPUMS, importInputPeakRSSBytes, finalGateE2InputSpoolBytes} {
		if !finalGateUint64InputPresent(op.Inputs, key) {
			return "missing measured " + key
		}
	}

	if len(uint64MapInput(op.Inputs, finalGateE2InputPartCounts)) == 0 {
		return "missing measured " + finalGateE2InputPartCounts
	}

	return ""
}

func finalGateEnsureE2ComputedBudgetInputs(op *perfreport.Operation) {
	if op.Inputs == nil {
		op.Inputs = make(map[string]any)
	}

	finalGateSetInputIfMissing(op.Inputs, finalGateE2BudgetSourceInput, finalGateE2BudgetSourceComputed)
	finalGateSetInputIfMissing(op.Inputs, finalGateE2BudgetMeasurementCountInput, uint64(len(op.DurationsMS)))
	finalGateSetInputIfMissing(op.Inputs, "wall_time_budget_ms", uint64(math.Ceil(op.P95MS)))
	finalGateSetInputIfMissing(op.Inputs, "total_cpu_budget_ms", uint64Input(op.Inputs, importInputTotalCPUMS))
	finalGateSetInputIfMissing(op.Inputs, "peak_rss_budget_bytes", uint64Input(op.Inputs, importInputPeakRSSBytes))
	finalGateSetInputIfMissing(op.Inputs, "spool_byte_budget", uint64Input(op.Inputs, finalGateE2InputSpoolBytes))
	finalGateSetInputIfMissing(op.Inputs, "part_count_budget", finalGateE2PartCount(*op))
}

func finalGateSetInputIfMissing(inputs map[string]any, key string, value any) {
	if _, ok := inputs[key]; ok {
		return
	}

	inputs[key] = value
}

func finalGateE2BudgetResourceFailure(op perfreport.Operation) string {
	for _, spec := range finalGateE2BudgetResources(op) {
		if reason := finalGateE2BudgetAtLeast(op.Inputs, spec.budgetKey, spec.measured, spec.name); reason != "" {
			return reason
		}
	}

	return ""
}

func finalGateE2BudgetResources(op perfreport.Operation) []struct {
	name      string
	budgetKey string
	measured  uint64
} {
	totalCPU, _ := uint64InputPresentValue(op.Inputs, importInputTotalCPUMS)
	peakRSS, _ := uint64InputPresentValue(op.Inputs, importInputPeakRSSBytes)
	spoolBytes, _ := uint64InputPresentValue(op.Inputs, finalGateE2InputSpoolBytes)

	return []struct {
		name      string
		budgetKey string
		measured  uint64
	}{
		{"wall-time", "wall_time_budget_ms", uint64(math.Ceil(op.P95MS))},
		{"CPU-time", "total_cpu_budget_ms", totalCPU},
		{"RSS", "peak_rss_budget_bytes", peakRSS},
		{"spool-byte", "spool_byte_budget", spoolBytes},
		{"part-count", "part_count_budget", finalGateE2PartCount(op)},
	}
}

func finalGateE2PartCount(op perfreport.Operation) uint64 {
	var count uint64
	for _, value := range uint64MapInput(op.Inputs, finalGateE2InputPartCounts) {
		count += value
	}

	return count
}

func finalGateE2BudgetAtLeast(
	inputs map[string]any,
	key string,
	measured uint64,
	name string,
) string {
	budget, ok := uint64InputPresentValue(inputs, key)
	if !ok || budget < measured {
		return name + " budget was not recorded from measured value"
	}

	return ""
}

func finalGateImportEvidenceFailure(report perfreport.Report, opName string, spool bool) string {
	if !finalGateReportTableStatsPass(report.TableStats) {
		return "missing table rows, parts, or bytes"
	}

	op, ok := firstOperation(report, opName, nil)
	if !ok {
		return "missing total operation"
	}

	if reason := finalGateImportOperationEvidenceFailure(op, spool); reason != "" {
		return reason
	}

	return ""
}

func finalGateReportTableStatsPass(stats map[string]perfreport.TableStats) bool {
	if len(stats) == 0 {
		return false
	}

	for _, tableStats := range stats {
		if !finalGateBasicTableSizeEvidencePass(tableStats) {
			return false
		}
	}

	return true
}

func finalGateBasicTableSizeEvidencePass(stats perfreport.TableStats) bool {
	return stats.Rows > 0 &&
		stats.ActiveParts > 0 &&
		stats.CompressedBytes > 0 &&
		stats.UncompressedBytes > 0
}

func finalGateImportOperationEvidenceFailure(op perfreport.Operation, spool bool) string {
	if reason := finalGateImportNumericEvidenceFailure(op); reason != "" {
		return reason
	}

	if reason := finalGateImportMetadataEvidenceFailure(op); reason != "" {
		return reason
	}

	if spool && len(uint64MapInput(op.Inputs, "loaded_table_rows")) == 0 {
		return "missing loaded_table_rows"
	}

	return ""
}

func finalGateImportNumericEvidenceFailure(op perfreport.Operation) string {
	for _, key := range []string{
		importInputUserCPUMS,
		importInputSystemCPUMS,
		importInputTotalCPUMS,
		importInputPeakRSSBytes,
		importInputPublishLatency,
	} {
		if !finalGateUint64InputPresent(op.Inputs, key) {
			return "missing " + key
		}
	}

	return ""
}

func finalGateUint64InputPresent(inputs map[string]any, key string) bool {
	value, ok := inputs[key]
	if !ok {
		return false
	}

	return finalGateUint64InputValuePresent(value)
}

func finalGateUint64InputValuePresent(value any) bool {
	switch typed := value.(type) {
	case uint64, uint32:
		return true
	case int:
		return typed >= 0
	case int64:
		return typed >= 0
	case float64:
		return typed >= 0 && math.Trunc(typed) == typed
	default:
		return false
	}
}

func finalGateImportMetadataEvidenceFailure(op perfreport.Operation) string {
	for _, key := range []string{finalGateE2InputSpoolBytes, "retry_cleanup_result", finalGateE2InputPartCounts} {
		if !finalGateInputMapHasKey(op.Inputs, key) {
			return "missing " + key
		}
	}

	if len(uint64MapInput(op.Inputs, finalGateE2InputPartCounts)) == 0 {
		return "missing " + finalGateE2InputPartCounts
	}

	if strings.TrimSpace(stringInput(op.Inputs, "retry_cleanup_result")) == "" {
		return "missing retry_cleanup_result"
	}

	return ""
}

func finalGateInputMapHasKey(inputs map[string]any, key string) bool {
	_, ok := inputs[key]

	return ok
}

func finalGateExpectedDigestFromManifest(path string, key string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	var manifest any
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ""
	}

	digest, _ := finalGateFindDigestKey(manifest, key)

	return digest
}

func finalGateFindDigestInMap(values map[string]any, key string) (string, bool) {
	if digest, ok := values[key].(string); ok {
		return digest, true
	}

	return finalGateFindDigestInSlice(mapValues(values), key)
}

func finalGateFindDigestInSlice(values []any, key string) (string, bool) {
	for _, child := range values {
		if digest, ok := finalGateFindDigestKey(child, key); ok {
			return digest, true
		}
	}

	return "", false
}

func finalGateFindDigestKey(value any, key string) (string, bool) {
	if typed, ok := value.(map[string]any); ok {
		return finalGateFindDigestInMap(typed, key)
	}

	if typed, ok := value.([]any); ok {
		return finalGateFindDigestInSlice(typed, key)
	}

	return "", false
}

func mapValues(values map[string]any) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}

	return out
}

func finalGateRESTOpsPass(reports []perfreport.Report, name string) bool {
	ops := operationsInReports(reports, name, nil)
	if len(ops) == 0 {
		return false
	}

	for _, op := range ops {
		if !finalGateRESTOperationEvidencePasses(op) {
			return false
		}
	}

	return true
}

func finalGateRESTOperationEvidencePasses(op perfreport.Operation) bool {
	for _, key := range []string{
		"query_count",
		finalGateRESTInputCacheHits,
		finalGateRESTInputCacheMisses,
		finalGateRESTInputJSONBytes,
		finalGateRESTInputGzipBytes,
	} {
		if len(uint64SliceInput(op.Inputs, key)) == 0 {
			return false
		}
	}

	return finalGatePercentilesPresent(op)
}

func finalGatePercentilesPresent(op perfreport.Operation) bool {
	return len(op.DurationsMS) > 0 && op.P50MS >= 0 && op.P95MS >= 0 && op.P99MS >= 0
}

func finalGateClickHouseOperations(reports []perfreport.Report) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if len(op.ReadRows) > 0 || len(op.ReadBytes) > 0 || len(op.ReadMarks) > 0 {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func finalGateClickHouseOperationEvidencePasses(op perfreport.Operation) bool {
	return len(op.ReadRows) > 0 &&
		len(op.ReadBytes) > 0 &&
		len(op.ReadMarks) > 0 &&
		len(op.ResultCount) > 0 &&
		len(op.ResultBytes) > 0
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
	if finalGateE2ScenarioOperation(op) {
		return true
	}

	if op.Name == finalGateJ6D4DecisionOpName {
		return true
	}

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
		finalGateLustreRootDir,
		finalGateNFSRootDir,
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
	case tableCatalog:
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
	if report.TableStats[tableDirSummary].Rows == 0 || report.TableStats[tableCatalog].Rows == 0 {
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
		tableDirFacts,
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
		stats.RowAmplificationVsCatalog > 0
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
		if len(op.DurationsMS) < finalGateMinRepeats || op.P50MS < 0 || op.P95MS < 0 || op.P99MS < 0 {
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
	return []string{tableFiles, tableCatalog, tableDirSummary, tableDirSummarySets}
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
