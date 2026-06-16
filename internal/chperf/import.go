/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	statsGZBasename   = "stats.gz"
	lineReaderBufSize = 32 * 1024
	maxImportParallel = 4

	phasePartitionDropReset  = "partition_drop_reset"
	phaseFilesInsert         = "wrstat_files_insert"
	phaseFilesFlush          = "wrstat_files_flush"
	phaseDGUTAInsert         = "wrstat_dguta_insert"
	phaseCatalogInsert       = "wrstat_dirs_insert"
	phaseDirFactsInsert      = "wrstat_dir_facts_insert"
	phaseDirProjectionWrite  = "wrstat_dir_projection_insert"
	phaseFullFilterAllInsert = "wrstat_filter_all_insert"
	phaseSchema3Ready        = "wrstat_schema3_snapshot_ready"
	phaseActiveVirtualInsert = "wrstat_active_virtual_insert"
	phaseActiveVirtualReady  = "wrstat_active_virtual_ready"
	phaseMountSwitch         = "mount_switch"
	phaseTreeSummaryRefresh  = "wrstat_tree_summary_refresh"
	phaseActivePrefixRefresh = "wrstat_active_prefix_rollup_refresh"
	phaseOldSnapshotDrop     = "old_snapshot_partition_drop"
	phaseBasedirsReset       = "wrstat_basedirs_reset"
	phaseBasedirsGroupUsage  = "wrstat_basedirs_group_usage_insert"
	phaseBasedirsUserUsage   = "wrstat_basedirs_user_usage_insert"
	phaseBasedirsGroupSubs   = "wrstat_basedirs_group_subdirs_insert"
	phaseBasedirsUserSubs    = "wrstat_basedirs_user_subdirs_insert"
	phaseBasedirsHistory     = "wrstat_basedirs_history_insert"
	phaseBasedirsFinalise    = "wrstat_basedirs_finalise"
	phaseBasedirsFlush       = "wrstat_basedirs_flush"

	tableFiles                    = "wrstat_files"
	tableDGUTA                    = "wrstat_dguta"
	tableCatalog                  = "wrstat_dirs"
	tableDirFacts                 = "wrstat_dir_facts"
	tableDirSummary               = "wrstat_dir_facts"
	tableDirSummarySets           = "wrstat_dir_projection_sets"
	tableDirDGUTAVector           = "wrstat_dir_facts"
	tableTreeSummarySets          = "wrstat_tree_summary_sets"
	tableTreeDGUTA                = "wrstat_virtual_summary_cache"
	tableTreeDirSummary           = "wrstat_tree_dir_summary"
	tableTreeChildren             = "wrstat_tree_children"
	tableBasedirsGroupUsage       = "wrstat_basedirs_group_usage"
	tableBasedirsUserUsage        = "wrstat_basedirs_user_usage"
	tableBasedirsGroupSubdirs     = "wrstat_basedirs_group_subdirs"
	tableBasedirsUserSubdirs      = "wrstat_basedirs_user_subdirs"
	tableBasedirsHistory          = "wrstat_basedirs_history"
	tableDirFilterAgeAll          = "wrstat_dir_filter_ageall"
	tableChildFilterAll           = "wrstat_child_filter_all"
	tableDirFilterAll             = "wrstat_dir_filter_all"
	tableSchema3SnapshotSets      = "wrstat_schema3_snapshot_sets"
	tableActiveVirtualDirs        = "wrstat_active_virtual_dirs"
	tableActiveVirtualSummaries   = "wrstat_active_virtual_summaries"
	tableActiveVirtualFilterAll   = "wrstat_active_virtual_filter_all"
	tableActiveVirtualChildren    = "wrstat_active_virtual_children"
	tableActiveVirtualSets        = "wrstat_active_virtual_sets"
	tableMountEvents              = "wrstat_mount_events"
	tableSchemaVersion            = "wrstat_schema_version"
	tableActivePrefixRollups      = "wrstat_active_prefix_rollups"
	tableActivePrefixFilterAgeAll = "wrstat_active_prefix_filter_ageall"
	tableActivePrefixRollupSets   = "wrstat_active_prefix_rollup_sets"
)

const (
	importGuardrailOperation                = "import_guardrail"
	importGuardrailStatusObserved           = "observed"
	importGuardrailStatusMissing            = "missing"
	importGuardrailRawFileIngest            = "raw_file_ingest"
	importGuardrailActiveSnapshotPublish    = "active_snapshot_publish"
	importGuardrailMaintainedDirProjection  = "maintained_dir_projection"
	importGuardrailActiveTreeSummaryRefresh = "active_tree_summary_refresh"
)

const (
	importInputDataset   = "dataset"
	importInputStatsPath = "stats_path"
	importInputMountPath = "mount_path"
	importInputRecords   = "records"
	importInputRowCap    = "row_cap"

	importInputUserCPUMS      = "user_cpu_ms"
	importInputSystemCPUMS    = "system_cpu_ms"
	importInputTotalCPUMS     = "total_cpu_ms"
	importInputPeakRSSBytes   = "peak_rss_bytes"
	importInputPublishLatency = "publish_latency_ms"

	importInputRowsPerTable                = "rows_per_table"
	importInputSpoolBytes                  = "spool_bytes"
	importInputPartCounts                  = "part_counts"
	importInputMaxDirsPerSnapshot          = "max_dirs_per_snapshot"
	importInputDirIDUInt32Justified        = "dir_id_uint32_justified"
	importInputDirIDUInt32WarningThreshold = "dir_id_uint32_warning_threshold"
	importInputPathTextBytesBefore         = "path_text_bytes_before"
	importInputPathTextBytesAfter          = "path_text_bytes_after"
	importInputPathTextBytesReduction      = "path_text_bytes_reduction"
	importInputPathTextBytesReductionPct   = "path_text_bytes_reduction_percent"

	dirIDUInt32WarningThreshold uint64 = 1 << 31
)

// ErrNoDatasets indicates no dataset directories were found.
var ErrNoDatasets = errors.New("no dataset directories found")

// PrintfFunc matches fmt.Printf-style output.
type PrintfFunc = perfreport.PrintfFunc

// Import discovers stats.gz datasets under inputDir, ingests them into
// ClickHouse, and returns a Report with timing information.
func Import(
	api ImportAPI,
	inputDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (perfreport.Report, error) {
	datasetDirs, err := findDatasets(inputDir)
	if err != nil {
		return perfreport.Report{}, err
	}

	report := perfreport.NewReport("clickhouse", inputDir, 1, 0)
	usageBefore := importProcessCPUUsage()
	startAll := time.Now()

	results, err := importDatasets(api, datasetDirs, opts, printf)
	if err != nil {
		return perfreport.Report{}, err
	}

	usage := importProcessCPUUsageDelta(usageBefore, importProcessCPUUsage())

	addImportReportOperations(
		&report,
		results,
		effectiveParallelism(opts.Parallelism),
		time.Since(startAll),
		opts.MaxLines,
	)

	if err := enrichImportReport(context.Background(), &report, api, results); err != nil {
		return perfreport.Report{}, err
	}

	addImportFinalGateEvidence(&report, results, usage)

	return report, nil
}

func findDatasets(baseDir string) ([]string, error) {
	dirs, err := datasets.FindLatestDatasetDirs(baseDir, statsGZBasename)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNoDatasets, baseDir)
	}

	slices.Sort(dirs)

	return dirs, nil
}

func importProcessCPUUsage() importCPUUsage {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return importCPUUsage{}
	}

	return importCPUUsage{
		userMS:   timevalMS(usage.Utime),
		systemMS: timevalMS(usage.Stime),
	}
}

func timevalMS(value syscall.Timeval) uint64 {
	if value.Sec < 0 || value.Usec < 0 {
		return 0
	}

	return uint64(value.Sec)*1000 + uint64(value.Usec)/1000
}

func importProcessCPUUsageDelta(before, after importCPUUsage) importCPUUsage {
	return importCPUUsage{
		userMS:   uint64SaturatingSub(after.userMS, before.userMS),
		systemMS: uint64SaturatingSub(after.systemMS, before.systemMS),
	}
}

func uint64SaturatingSub(after, before uint64) uint64 {
	if after < before {
		return 0
	}

	return after - before
}

func addImportReportOperations(
	report *perfreport.Report,
	results []datasetImportResult,
	parallelism int,
	totalDuration time.Duration,
	rowCap int,
) {
	totalRecords := totalImportRecords(results)
	rowCapInput := importRowCapInput(rowCap)

	for _, result := range results {
		inputs := map[string]any{
			importInputDataset:           result.dataset,
			importInputStatsPath:         result.statsPath,
			importInputMountPath:         result.mountPath,
			"lines":                      result.lines,
			"rows_per_table":             importRowsByPhysicalTable(result.rows),
			"throughput_records_per_sec": throughputPerSecond(result.records(), result.elapsed),
		}
		addImportRowCapInput(inputs, rowCapInput)

		report.AddOperationWithCounters(
			"import_file_total",
			inputs,
			[]float64{durationMS(result.elapsed)},
			nil,
			nil,
			nil,
			[]uint64{result.records()},
		)

		for _, phase := range sortedImportPhases(result.phases) {
			inputs := map[string]any{
				importInputDataset:   result.dataset,
				importInputStatsPath: result.statsPath,
				importInputMountPath: result.mountPath,
				"phase":              phase,
			}
			addImportRowCapInput(inputs, rowCapInput)
			addImportPhaseInputs(inputs, result, phase)

			report.AddOperationWithCounters(
				"import_phase",
				inputs,
				[]float64{durationMS(result.phases[phase])},
				nil,
				nil,
				nil,
				[]uint64{importPhaseResultCount(result, phase)},
			)
		}

		addImportGuardrailOperations(report, result, rowCapInput)
	}

	inputs := map[string]any{
		"datasets":                   len(results),
		importInputRecords:           totalRecords,
		"parallelism":                parallelism,
		"mode":                       importMode(parallelism),
		"throughput_records_per_sec": throughputPerSecond(totalRecords, totalDuration),
	}
	addImportRowCapInput(inputs, rowCapInput)

	report.AddOperationWithCounters(
		"import_total",
		inputs,
		[]float64{durationMS(totalDuration)},
		nil,
		nil,
		nil,
		[]uint64{totalRecords},
	)
}

func importRowCapInput(rowCap int) uint64 {
	if rowCap <= 0 {
		return 0
	}

	return uint64(rowCap)
}

func addImportRowCapInput(inputs map[string]any, rowCap uint64) {
	if rowCap > 0 {
		inputs[importInputRowCap] = rowCap
	}
}

func importSnapshotMultiTablePhase(phase string) ([]string, bool) {
	switch phase {
	case phasePartitionDropReset:
		return []string{
			tableDGUTA,
			tableCatalog,
			tableDirFacts,
			tableFiles,
			tableDirSummary,
			tableDirSummarySets,
			tableDirDGUTAVector,
			tableBasedirsGroupUsage,
			tableBasedirsUserUsage,
			tableBasedirsGroupSubdirs,
			tableBasedirsUserSubdirs,
			tableDirFilterAgeAll,
			tableChildFilterAll,
			tableDirFilterAll,
			tableSchema3SnapshotSets,
			tableActiveVirtualDirs,
			tableActiveVirtualSummaries,
			tableActiveVirtualFilterAll,
			tableActiveVirtualChildren,
			tableActiveVirtualSets,
		}, true
	case phaseDirProjectionWrite:
		return []string{tableDirSummary, tableDirSummarySets, tableDirDGUTAVector, tableDirFilterAgeAll}, true
	case phaseFullFilterAllInsert:
		return []string{tableChildFilterAll, tableDirFilterAll}, true
	case phaseSchema3Ready:
		return []string{tableSchema3SnapshotSets}, true
	case phaseActiveVirtualInsert:
		return []string{
			tableActiveVirtualDirs,
			tableActiveVirtualSummaries,
			tableActiveVirtualFilterAll,
			tableActiveVirtualChildren,
			tableActiveVirtualSets,
		}, true
	case phaseActiveVirtualReady:
		return []string{tableActiveVirtualSets}, true
	default:
		return nil, false
	}
}

func importBasedirsMultiTablePhase(phase string) ([]string, bool) {
	switch phase {
	case phaseBasedirsReset, phaseBasedirsFlush:
		return []string{
			tableBasedirsGroupUsage,
			tableBasedirsUserUsage,
			tableBasedirsGroupSubdirs,
			tableBasedirsUserSubdirs,
		}, true
	case phaseBasedirsFinalise:
		return []string{tableBasedirsGroupUsage, tableBasedirsHistory}, true
	default:
		return nil, false
	}
}

func importRefreshMultiTablePhase(phase string) ([]string, bool) {
	switch phase {
	case phaseTreeSummaryRefresh:
		return []string{tableTreeSummarySets, tableTreeDGUTA, tableTreeDirSummary, tableTreeChildren}, true
	case phaseActivePrefixRefresh:
		return []string{tableActivePrefixRollups, tableActivePrefixFilterAgeAll, tableActivePrefixRollupSets}, true
	default:
		return nil, false
	}
}

func addImportDerivedTableEvidence(stats map[string]perfreport.TableStats, memoryBytes uint64) {
	dirFactsRows := stats[tableDirSummary].Rows
	catalogRows := stats[tableCatalog].Rows

	for table, tableStats := range stats {
		tableStats.ImportMemoryBytes = memoryBytes
		tableStats.RowAmplificationVsDirFacts = rowAmplification(tableStats.Rows, dirFactsRows)
		tableStats.RowAmplificationVsCatalog = rowAmplification(tableStats.Rows, catalogRows)
		stats[table] = tableStats
	}
}

func rowAmplification(rows uint64, baselineRows uint64) float64 {
	if rows == 0 || baselineRows == 0 {
		return 0
	}

	return float64(rows) / float64(baselineRows)
}

func addImportJ6StorageAudit(
	ctx context.Context,
	report *perfreport.Report,
	api ImportAPI,
	selectedTables []string,
) error {
	auditAPI, ok := api.(ImportStorageAuditAPI)
	if !ok {
		return nil
	}

	auditTables := j6HotRowAuditTables(selectedTables)

	hotPathTables, err := auditAPI.ImportHotRowPathStringTables(ctx, auditTables)
	if err != nil {
		return err
	}

	if hotPathTables == nil {
		hotPathTables = []string{}
	}

	report.AddOperation(finalGateJ6StorageAuditOpName, map[string]any{
		finalGateJ6HotRowPathStringTablesInput:       hotPathTables,
		finalGateJ6PathTextCatalogTableInput:         tableCatalog,
		finalGateJ6PathTextCopiesPerDirSnapshotInput: float64(1),
		"audited_hot_tables":                         auditTables,
	}, nil)

	return nil
}

func j6HotRowAuditTables(selectedTables []string) []string {
	tables := make([]string, 0, len(selectedTables))
	for _, table := range selectedTables {
		if !j6HotRowAuditTable(table) {
			continue
		}

		tables = append(tables, table)
	}

	return tables
}

func j6HotRowAuditTable(table string) bool {
	switch table {
	case "",
		tableCatalog,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets:
		return false
	default:
		return true
	}
}

func addImportFinalGateEvidence(
	report *perfreport.Report,
	results []datasetImportResult,
	usage importCPUUsage,
) {
	total := importTotalOperation(report)
	if total == nil {
		return
	}

	if total.Inputs == nil {
		total.Inputs = make(map[string]any)
	}

	total.Inputs[importInputUserCPUMS] = usage.userMS
	total.Inputs[importInputSystemCPUMS] = usage.systemMS
	total.Inputs[importInputTotalCPUMS] = usage.userMS + usage.systemMS
	total.Inputs[importInputPeakRSSBytes] = report.MaxRSSBytes
	total.Inputs[importInputSpoolBytes] = uint64(0)
	total.Inputs[importInputRowsPerTable] = importRowsByReportedTableStats(report.TableStats, results)
	total.Inputs[importInputPartCounts] = importActivePartCounts(report.TableStats)
	total.Inputs[importInputMaxDirsPerSnapshot] = importMaxDirsPerSnapshot(results)
	total.Inputs[importInputDirIDUInt32WarningThreshold] = dirIDUInt32WarningThreshold
	total.Inputs[importInputDirIDUInt32Justified] = importDirIDUInt32Justified(results)
	addImportPathTextReductionInputs(total, results)
	total.Inputs["retry_cleanup_result"] = importRetryCleanupResult(results)
	total.Inputs[importInputPublishLatency] = importPublishLatencyMS(results)
	finalGateEnsureE2ComputedBudgetInputs(total)
}

func importTotalOperation(report *perfreport.Report) *perfreport.Operation {
	for i := range report.Operations {
		if report.Operations[i].Name == "import_total" {
			return &report.Operations[i]
		}
	}

	return nil
}

func importRowsByReportedTableStats(
	stats map[string]perfreport.TableStats,
	results []datasetImportResult,
) map[string]uint64 {
	if len(stats) == 0 {
		return totalImportRowsByPhysicalTable(results)
	}

	rows := make(map[string]uint64, len(stats))
	for table, tableStats := range stats {
		rows[table] = tableStats.Rows
	}

	return rows
}

func totalImportRowsByPhysicalTable(results []datasetImportResult) map[string]uint64 {
	rows := make(map[string]uint64)

	for _, result := range results {
		for table, count := range result.rows {
			rows[importPhysicalTable(table)] += count
		}
	}

	return rows
}

func importActivePartCounts(stats map[string]perfreport.TableStats) map[string]uint64 {
	counts := make(map[string]uint64, len(stats))
	for table, tableStats := range stats {
		counts[table] = tableStats.ActiveParts
	}

	return counts
}

func importMaxDirsPerSnapshot(results []datasetImportResult) uint64 {
	var maxDirs uint64

	for _, result := range results {
		maxDirs = max(maxDirs, result.rows[tableCatalog])
	}

	return maxDirs
}

func importDirIDUInt32Justified(results []datasetImportResult) bool {
	return importMaxDirsPerSnapshot(results) < dirIDUInt32WarningThreshold
}

func addImportPathTextReductionInputs(total *perfreport.Operation, results []datasetImportResult) {
	before, after := importPathTextBytes(results)
	reduction := uint64SaturatingSub(before, after)

	total.Inputs[importInputPathTextBytesBefore] = before
	total.Inputs[importInputPathTextBytesAfter] = after
	total.Inputs[importInputPathTextBytesReduction] = reduction
	total.Inputs[importInputPathTextBytesReductionPct] = importPathTextReductionPercent(before, reduction)
}

func importPathTextBytes(results []datasetImportResult) (uint64, uint64) {
	var (
		before uint64
		after  uint64
	)

	for _, result := range results {
		before += result.pathTextBytesBefore
		after += result.pathTextBytesAfter
	}

	return before, after
}

func importPathTextReductionPercent(before, reduction uint64) float64 {
	if before == 0 {
		return 0
	}

	return 100 * float64(reduction) / float64(before)
}

func importRetryCleanupResult(results []datasetImportResult) string {
	if importPhaseDuration(results, phaseOldSnapshotDrop) > 0 {
		return finalGateComparisonStatusSuccess
	}

	return "not_attempted"
}

func importPhaseDuration(results []datasetImportResult, phase string) time.Duration {
	var total time.Duration
	for _, result := range results {
		total += result.phases[phase]
	}

	return total
}

func importPublishLatencyMS(results []datasetImportResult) uint64 {
	return uint64(durationMS(importPhaseDuration(results, phaseMountSwitch)))
}

type importCPUUsage struct {
	userMS   uint64
	systemMS uint64
}

func (m *datasetImportMetrics) addPathTextBytes(before, after uint64) {
	if m == nil {
		return
	}

	m.pathTextBytesBefore += before
	m.pathTextBytesAfter += after
}

func cloneMap[M ~map[K]V, K comparable, V any](src M) M {
	if src == nil {
		return make(M)
	}

	return maps.Clone(src)
}

func throughputPerSecond(records uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}

	return float64(records) / elapsed.Seconds()
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func sortedImportPhases(phases map[string]time.Duration) []string {
	return slices.Sorted(maps.Keys(phases))
}

func addImportPhaseInputs(inputs map[string]any, result datasetImportResult, phase string) {
	if table, rows, ok := importSingleTablePhase(result, phase); ok {
		inputs["table"] = importPhysicalTable(table)
		inputs["rows"] = rows

		return
	}

	if tables, ok := importMultiTablePhase(phase); ok {
		inputs["tables"] = importPhysicalTables(tables)
	}
}

func importSingleTablePhase(result datasetImportResult, phase string) (string, uint64, bool) {
	if table, ok := importMainTablePhase(phase); ok {
		return table, result.rows[table], true
	}

	if table, ok := importBasedirsTablePhase(phase); ok {
		return table, result.rows[table], true
	}

	return "", 0, false
}

func importMainTablePhase(phase string) (string, bool) {
	switch phase {
	case phaseFilesInsert, phaseFilesFlush:
		return tableFiles, true
	case phaseDGUTAInsert:
		return tableDGUTA, true
	case phaseCatalogInsert:
		return tableCatalog, true
	case phaseDirFactsInsert:
		return tableDirFacts, true
	case phaseMountSwitch:
		return tableMountEvents, true
	default:
		return "", false
	}
}

func importBasedirsTablePhase(phase string) (string, bool) {
	switch phase {
	case phaseBasedirsGroupUsage:
		return tableBasedirsGroupUsage, true
	case phaseBasedirsUserUsage:
		return tableBasedirsUserUsage, true
	case phaseBasedirsGroupSubs:
		return tableBasedirsGroupSubdirs, true
	case phaseBasedirsUserSubs:
		return tableBasedirsUserSubdirs, true
	case phaseBasedirsHistory:
		return tableBasedirsHistory, true
	default:
		return "", false
	}
}

func importMultiTablePhase(phase string) ([]string, bool) {
	if tables, ok := importSnapshotMultiTablePhase(phase); ok {
		return tables, true
	}

	if tables, ok := importBasedirsMultiTablePhase(phase); ok {
		return tables, true
	}

	if tables, ok := importRefreshMultiTablePhase(phase); ok {
		return tables, true
	}

	return nil, false
}

func totalImportRecords(results []datasetImportResult) uint64 {
	var total uint64

	for _, result := range results {
		total += result.records()
	}

	return total
}

func importPhaseResultCount(result datasetImportResult, phase string) uint64 {
	if table, _, ok := importSingleTablePhase(result, phase); ok {
		return result.rows[table]
	}

	if tables, ok := importMultiTablePhase(phase); ok {
		return importTablesResultCount(result, tables)
	}

	return 1
}

func importTablesResultCount(result datasetImportResult, tables []string) uint64 {
	var total uint64
	for _, table := range tables {
		total += result.rows[table]
	}

	return total
}

func addImportGuardrailOperations(report *perfreport.Report, result datasetImportResult, rowCap uint64) {
	addRawFileIngestGuardrail(report, result, rowCap)
	addPhaseImportGuardrail(report, result, importGuardrailActiveSnapshotPublish, phaseMountSwitch, nil, rowCap)
	addPhaseImportGuardrail(
		report,
		result,
		importGuardrailMaintainedDirProjection,
		phaseDirProjectionWrite,
		importGuardrailTables(phaseDirProjectionWrite),
		rowCap,
	)
	addPhaseImportGuardrail(
		report,
		result,
		importGuardrailActiveTreeSummaryRefresh,
		phaseTreeSummaryRefresh,
		importGuardrailTables(phaseTreeSummaryRefresh),
		rowCap,
	)
}

func addRawFileIngestGuardrail(report *perfreport.Report, result datasetImportResult, rowCap uint64) {
	status, duration := importGuardrailStatusAndDuration(rawFileIngestObserved(result), result.elapsed)
	inputs := importGuardrailInputs(result, importGuardrailRawFileIngest, status)
	addImportRowCapInput(inputs, rowCap)
	inputs["table"] = tableFiles
	inputs["rows"] = result.rows[tableFiles]
	inputs["lines"] = result.lines
	inputs["phases"] = []string{phaseFilesInsert, phaseFilesFlush}
	inputs["throughput_records_per_sec"] = throughputPerSecond(result.records(), result.elapsed)

	report.AddOperationWithCounters(
		importGuardrailOperation,
		inputs,
		[]float64{durationMS(duration)},
		nil,
		nil,
		nil,
		[]uint64{result.rows[tableFiles]},
	)
}

func importGuardrailStatusAndDuration(observed bool, duration time.Duration) (string, time.Duration) {
	if !observed {
		return importGuardrailStatusMissing, 0
	}

	return importGuardrailStatusObserved, duration
}

func rawFileIngestObserved(result datasetImportResult) bool {
	return result.rows[tableFiles] > 0 ||
		hasImportPhase(result, phaseFilesInsert) ||
		hasImportPhase(result, phaseFilesFlush)
}

func hasImportPhase(result datasetImportResult, phase string) bool {
	_, ok := result.phases[phase]

	return ok
}

func importGuardrailInputs(result datasetImportResult, guardrail string, status string) map[string]any {
	return map[string]any{
		importInputDataset:   result.dataset,
		importInputStatsPath: result.statsPath,
		importInputMountPath: result.mountPath,
		"guardrail":          guardrail,
		"status":             status,
	}
}

func addPhaseImportGuardrail(
	report *perfreport.Report,
	result datasetImportResult,
	guardrail string,
	phase string,
	tables []string,
	rowCap uint64,
) {
	phaseDuration, observed := result.phases[phase]
	status, duration := importGuardrailStatusAndDuration(observed, phaseDuration)
	inputs := importGuardrailInputs(result, guardrail, status)
	addImportRowCapInput(inputs, rowCap)
	inputs["phase"] = phase

	if len(tables) > 0 {
		inputs["tables"] = slices.Clone(tables)
	}

	report.AddOperationWithCounters(
		importGuardrailOperation,
		inputs,
		[]float64{durationMS(duration)},
		nil,
		nil,
		nil,
		[]uint64{importGuardrailResultCount(result, phase, observed)},
	)
}

func importGuardrailResultCount(result datasetImportResult, phase string, observed bool) uint64 {
	if !observed {
		return 0
	}

	return importPhaseResultCount(result, phase)
}

func importGuardrailTables(phase string) []string {
	tables, ok := importMultiTablePhase(phase)
	if !ok {
		return nil
	}

	return importPhysicalTables(tables)
}

func importMode(parallelism int) string {
	if parallelism > 1 {
		return "parallel"
	}

	return "serial"
}

func effectiveParallelism(parallelism int) int {
	return min(max(parallelism, 1), maxImportParallel)
}

func enrichImportReport(
	ctx context.Context,
	report *perfreport.Report,
	api ImportAPI,
	results []datasetImportResult,
) error {
	selectedTables := selectedImportTables(nil)
	tableStats := fallbackImportTableStats(results, selectedTables)

	if statsAPI, ok := api.(ImportReportStatsAPI); ok {
		if err := collectImportReportStats(ctx, report, statsAPI, &selectedTables, tableStats); err != nil {
			return err
		}
	}

	addImportPhaseDurations(tableStats, results)

	report.MaxRSSBytes = maxRSSBytes()
	addImportDerivedTableEvidence(tableStats, report.MaxRSSBytes)

	report.SelectedTables = selectedTables
	report.TableStats = tableStatsForSelectedTables(tableStats, selectedTables)

	if err := addImportJ6StorageAudit(ctx, report, api, selectedTables); err != nil {
		return err
	}

	return nil
}

func selectedImportTables(collected map[string]perfreport.TableStats) []string {
	if collected == nil {
		return baseImportSelectedTables()
	}

	selected := make([]string, 0, len(collected))

	for _, table := range slices.Sorted(maps.Keys(collected)) {
		skip := !strings.HasPrefix(table, "wrstat_") ||
			!importTableSelected(collected[table])
		if skip {
			continue
		}

		selected = append(selected, table)
	}

	return selected
}

func baseImportSelectedTables() []string {
	return []string{
		tableFiles,
		tableDirSummary,
		tableCatalog,
		tableDirSummarySets,
		tableBasedirsGroupUsage,
		tableBasedirsUserUsage,
		tableBasedirsGroupSubdirs,
		tableBasedirsUserSubdirs,
		tableBasedirsHistory,
		tableDirFilterAgeAll,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
	}
}

func importTableSelected(stats perfreport.TableStats) bool {
	return stats.Rows > 0 ||
		stats.ActiveParts > 0 ||
		stats.CompressedBytes > 0 ||
		stats.UncompressedBytes > 0
}

func fallbackImportTableStats(
	results []datasetImportResult,
	tables []string,
) map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats, len(tables))
	for _, table := range tables {
		stats[table] = perfreport.TableStats{Rows: importRowsForPhysicalTable(results, table)}
	}

	return stats
}

func importRowsForPhysicalTable(results []datasetImportResult, table string) uint64 {
	var rows uint64

	for _, result := range results {
		switch table {
		case tableDirSummary:
			rows += result.rows[tableDGUTA]
		default:
			rows += result.rows[table]
		}
	}

	return rows
}

func collectImportReportStats(
	ctx context.Context,
	report *perfreport.Report,
	statsAPI ImportReportStatsAPI,
	selectedTables *[]string,
	tableStats map[string]perfreport.TableStats,
) error {
	collected, err := statsAPI.ImportTableStats(ctx, nil)
	if err != nil {
		return err
	}

	*selectedTables = selectedImportTables(collected)
	maps.Copy(tableStats, collected)

	vector, buckets, err := statsAPI.ImportFactsStats(ctx)
	if err != nil {
		return err
	}

	report.FactsVectorStats = &vector
	report.FactsBucketStats = &buckets

	return nil
}

func addImportPhaseDurations(
	stats map[string]perfreport.TableStats,
	results []datasetImportResult,
) {
	for table, phases := range importPhaseDurationsByTable(results) {
		tableStats := stats[table]
		tableStats.ImportPhaseDurationsMS = phases
		stats[table] = tableStats
	}
}

func importPhaseDurationsByTable(results []datasetImportResult) map[string]map[string]float64 {
	byTable := make(map[string]map[string]float64)

	for _, result := range results {
		for phase, duration := range result.phases {
			addImportPhaseDuration(byTable, phase, duration)
		}
	}

	return byTable
}

func addImportPhaseDuration(
	byTable map[string]map[string]float64,
	phase string,
	duration time.Duration,
) {
	for _, table := range importPhasePhysicalTables(phase) {
		if byTable[table] == nil {
			byTable[table] = make(map[string]float64)
		}

		byTable[table][phase] += durationMS(duration)
	}
}

func importPhasePhysicalTables(phase string) []string {
	if table, ok := importMainTablePhase(phase); ok {
		return []string{importPhysicalTable(table)}
	}

	if table, ok := importBasedirsTablePhase(phase); ok {
		return []string{table}
	}

	tables, ok := importMultiTablePhase(phase)
	if !ok {
		return nil
	}

	return importPhysicalTables(tables)
}

func importPhysicalTables(tables []string) []string {
	seen := make(map[string]struct{}, len(tables))
	physical := make([]string, 0, len(tables))

	for _, table := range tables {
		table = importPhysicalTable(table)
		if _, ok := seen[table]; ok {
			continue
		}

		seen[table] = struct{}{}
		physical = append(physical, table)
	}

	return physical
}

func importPhysicalTable(table string) string {
	if table == tableDGUTA {
		return tableDirSummary
	}

	return table
}

func importRowsByPhysicalTable(rows map[string]uint64) map[string]uint64 {
	physical := make(map[string]uint64, len(rows))
	for table, count := range rows {
		physical[importPhysicalTable(table)] += count
	}

	return physical
}

func tableStatsForSelectedTables(
	stats map[string]perfreport.TableStats,
	selectedTables []string,
) map[string]perfreport.TableStats {
	selected := make(map[string]perfreport.TableStats, len(selectedTables))
	for _, table := range selectedTables {
		selected[table] = stats[table]
	}

	return selected
}

func maxRSSBytes() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}

	if usage.Maxrss <= 0 {
		return 0
	}

	return uint64(usage.Maxrss) * 1024
}

func importDatasets(
	api ImportAPI,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) ([]datasetImportResult, error) {
	if effectiveParallelism(opts.Parallelism) == 1 {
		return importSerial(api, datasetDirs, opts, printf)
	}

	return importParallel(api, datasetDirs, opts, printf)
}

func importSerial(
	api ImportAPI,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) ([]datasetImportResult, error) {
	results := make([]datasetImportResult, 0, len(datasetDirs))

	for _, dir := range datasetDirs {
		result, err := importOneDataset(api, dir, opts, printf)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

func importOneDataset(
	api ImportAPI,
	datasetDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (_ datasetImportResult, err error) {
	mountPath, err := mountpath.FromOutputDir(datasetDir)
	if err != nil {
		return datasetImportResult{}, err
	}

	statsPath := filepath.Join(datasetDir, statsGZBasename)

	st, err := os.Stat(statsPath)
	if err != nil {
		return datasetImportResult{}, err
	}

	updatedAt := st.ModTime()
	start := time.Now()
	dataset := filepath.Base(datasetDir)
	metrics := newDatasetImportMetrics(dataset, statsPath, mountPath)

	records, err := ingestStatsGZ(api, statsPath, mountPath, updatedAt, opts, metrics)
	if err != nil {
		return datasetImportResult{}, err
	}

	printf("import dataset=%s mount=%s records=%d seconds=%.3f\n",
		dataset, mountPath, records, time.Since(start).Seconds())

	return metrics.result(records, time.Since(start)), nil
}

func newDatasetImportMetrics(dataset, statsPath, mountPath string) *datasetImportMetrics {
	return &datasetImportMetrics{
		dataset:   dataset,
		statsPath: statsPath,
		mountPath: mountPath,
		rows:      make(map[string]uint64),
		phases:    make(map[string]time.Duration),
	}
}

func ingestStatsGZ(
	api ImportAPI,
	statsPath, mountPath string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (_ uint64, err error) {
	gz, err := openStatsGZReader(statsPath)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := gz.Close(); err == nil {
			err = cerr
		}
	}()

	return summariseReader(gz, api, mountPath, updatedAt, opts, metrics)
}

func openStatsGZReader(path string) (*statsGZReader, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	gz, err := pgzip.NewReader(fh)
	if err != nil {
		return nil, errors.Join(err, fh.Close())
	}

	return &statsGZReader{Reader: gz, file: fh}, nil
}

func summariseReader(
	r io.Reader,
	api ImportAPI,
	mountPath string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (_ uint64, err error) {
	lr := newLineCountingReader(r, opts.MaxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	allClosers, err := addAllSummarisers(ss, api, mountPath, updatedAt, opts, metrics)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := allClosers(err == nil); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	if err := ss.Summarise(); err != nil {
		return 0, err
	}

	return lr.linesRead(), nil
}

func newLineCountingReader(r io.Reader, maxLines int) *lineCountingReader {
	var ml uint64
	if maxLines > 0 {
		ml = uint64(maxLines)
	}

	return &lineCountingReader{
		underlying: r,
		maxLines:   ml,
		buf:        make([]byte, lineReaderBufSize),
	}
}

func addAllSummarisers(
	ss *summary.Summariser,
	api ImportAPI,
	mountPath string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (func(bool) error, error) {
	idAllocator := summary.NewDirIDAllocator()
	if err := idAllocator.SetMountPath(mountPath); err != nil {
		return nil, fmt.Errorf("failed to reserve directory ids: %w", err)
	}

	dw, err := api.NewDGUTAWriter()
	if err != nil {
		return nil, err
	}

	trackedDW := newTrackedDGUTAWriter(dw, metrics)

	trackedDW.SetMountPath(mountPath)
	trackedDW.SetUpdatedAt(updatedAt)

	fi, fiCloser, err := api.NewFileIngestOperation(mountPath, updatedAt, idAllocator)
	if err != nil {
		return nil, errors.Join(err, trackedDW.Abort())
	}

	summariseutil.SetBatchSize(opts.BatchSize, trackedDW, fiCloser)

	setImportPhaseRecorder(fiCloser, metrics)

	trackedFI := trackFileIngestOperation(fi, metrics)
	timedFICloser := timedImportCloser{Closer: fiCloser, metrics: metrics, phase: phaseFilesFlush}

	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(
		trackedDW.directorySink(),
		idAllocator,
	))
	ss.AddGlobalOperation(trackedFI)

	bsCloser, err := addBasedirsSummariser(ss, api, mountPath, updatedAt, opts, metrics)
	if err != nil {
		return nil, errors.Join(err, composeImportCloser(timedFICloser, nil, trackedDW)(false))
	}

	return composeImportCloser(timedFICloser, bsCloser, trackedDW), nil
}

func newTrackedDGUTAWriter(
	dw db.DGUTAWriter,
	metrics *datasetImportMetrics,
) *trackedDGUTAWriter {
	timed := &trackedDGUTAWriter{DGUTAWriter: dw, metrics: metrics}

	setImportPhaseRecorder(dw, metrics)

	return timed
}

func setImportPhaseRecorder(target any, metrics *datasetImportMetrics) {
	if metrics == nil {
		return
	}

	recorder, ok := target.(importPhaseRecorderSetter)
	if !ok {
		return
	}

	recorder.SetImportPhaseRecorder(metrics.addPhase)
}

func trackFileIngestOperation(
	gen summary.OperationGenerator,
	metrics *datasetImportMetrics,
) summary.OperationGenerator {
	return func() summary.Operation {
		return &trackedFileOperation{Operation: gen(), metrics: metrics}
	}
}

func addBasedirsSummariser(
	ss *summary.Summariser,
	api ImportAPI,
	mountPath string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (func(bool) error, error) {
	if opts.QuotaPath == "" || opts.ConfigPath == "" {
		return noopPublishCloser, nil
	}

	bs, err := api.NewBaseDirsStore()
	if err != nil {
		return nil, err
	}

	summariseutil.SetBatchSize(opts.BatchSize, bs)

	trackedBS := &trackedBasedirsStore{Store: bs, metrics: metrics}

	trackedBS.SetMountPath(mountPath)
	trackedBS.SetUpdatedAt(updatedAt)

	closer := func(publish bool) error {
		return summariseutil.CloseOrAbort(trackedBS, publish)
	}

	if err := addBasedirsOp(ss, trackedBS, updatedAt, opts); err != nil {
		return nil, errors.Join(err, trackedBS.Abort())
	}

	return closer, nil
}

func importMountpoints(opts ImportOptions) ([]string, error) {
	if opts.MountPoints != nil {
		return opts.MountPoints, nil
	}

	return summariseutil.ParseMountpointsFromFile(opts.MountsPath)
}

type statsGZReader struct {
	*pgzip.Reader
	file *os.File
}

func (r *statsGZReader) Close() error {
	return errors.Join(r.Reader.Close(), r.file.Close())
}

func (w *trackedDGUTAWriter) SetMountPath(mountPath string) {
	w.mountPath = mountPath
	w.DGUTAWriter.SetMountPath(mountPath)
}

func (w *trackedDGUTAWriter) directorySink() dirguta.DB {
	return w
}

func countCatalogRows(record db.RecordDGUTA) uint64 {
	if record.Dir == nil {
		return 0
	}

	return 1
}

func countDirFactRows(record db.RecordDGUTA) uint64 {
	if record.Dir == nil {
		return 0
	}

	return 1
}

func importPathTextBytesForRows(path string, rows uint64) uint64 {
	if path == "" || rows == 0 {
		return 0
	}

	return uint64(len(path)) * rows
}

func recordDirPath(record db.RecordDGUTA) string {
	if record.Dir == nil {
		return ""
	}

	return string(record.Dir.AppendTo(nil))
}

func importFileParentPathTextBytes(info *summary.FileInfo) uint64 {
	if info == nil || info.Path == nil {
		return 0
	}

	return importPathTextBytesForRows(string(info.Path.AppendTo(nil)), 1)
}

func noopPublishCloser(bool) error {
	return nil
}

func addBasedirsOp(
	ss *summary.Summariser,
	store basedirs.Store,
	modtime time.Time,
	opts ImportOptions,
) error {
	quotas, config, mountpoints, err := parseBasedirsInputs(opts)
	if err != nil {
		return err
	}

	bd, err := summariseutil.NewBaseDirsCreator(store, quotas, mountpoints, modtime)
	if err != nil {
		return err
	}

	ss.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func parseBasedirsInputs(opts ImportOptions) (*basedirs.Quotas, basedirs.Config, []string, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(opts.QuotaPath, opts.ConfigPath)
	if err != nil {
		return nil, nil, nil, err
	}

	mountpoints, err := importMountpoints(opts)
	if err != nil {
		return nil, nil, nil, err
	}

	return quotas, config, mountpoints, nil
}

func composeImportCloser(
	fileCloser io.Closer,
	basedirsCloser func(bool) error,
	dgutaCloser abortableCloser,
) func(bool) error {
	return summariseutil.ComposePublishCloser(fileCloser, basedirsCloser, dgutaCloser)
}

func importParallel(
	api ImportAPI,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) ([]datasetImportResult, error) {
	results := runParallel(api, datasetDirs, opts, printf)
	if _, err := sumResults(results); err != nil {
		return nil, err
	}

	return collectImportResults(results), nil
}

func sumResults(results []importResult) (uint64, error) {
	var total uint64

	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}

		total += r.records
	}

	return total, nil
}

func collectImportResults(results []importResult) []datasetImportResult {
	imports := make([]datasetImportResult, 0, len(results))

	for _, r := range results {
		imports = append(imports, r.dataset)
	}

	return imports
}

func runParallel(
	api ImportAPI,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) []importResult {
	parallelism := effectiveParallelism(opts.Parallelism)
	sem := make(chan struct{}, parallelism)
	results := make([]importResult, len(datasetDirs))

	var wg sync.WaitGroup

	for idx, dir := range datasetDirs {
		wg.Add(1)

		go func(i int, d string) {
			defer wg.Done()

			sem <- struct{}{}

			defer func() { <-sem }()

			result, err := importOneDataset(api, d, opts, printf)
			results[i] = importResult{dataset: result, records: result.records(), err: err}
		}(idx, dir)
	}

	wg.Wait()

	return results
}

type abortableCloser interface {
	io.Closer
	Abort() error
}

// ImportOptions configures the import operation.
type ImportOptions struct {
	MaxLines    int
	BatchSize   int
	Parallelism int
	QuotaPath   string
	ConfigPath  string
	MountsPath  string
	MountPoints []string
}

type datasetImportResult struct {
	dataset   string
	statsPath string
	mountPath string
	lines     uint64
	elapsed   time.Duration
	rows      map[string]uint64
	phases    map[string]time.Duration

	pathTextBytesBefore uint64
	pathTextBytesAfter  uint64
}

func (r datasetImportResult) records() uint64 {
	return r.lines
}

type importResult struct {
	dataset datasetImportResult
	records uint64
	err     error
}

type datasetImportMetrics struct {
	dataset   string
	statsPath string
	mountPath string
	rows      map[string]uint64
	phases    map[string]time.Duration

	pathTextBytesBefore uint64
	pathTextBytesAfter  uint64
}

func (m *datasetImportMetrics) addRows(table string, rows uint64) {
	if m == nil || rows == 0 {
		return
	}

	m.rows[table] += rows
}

func (m *datasetImportMetrics) addPhase(phase string, d time.Duration) {
	if m == nil || d <= 0 {
		return
	}

	m.phases[phase] += d
}

func (m *datasetImportMetrics) result(lines uint64, elapsed time.Duration) datasetImportResult {
	return datasetImportResult{
		dataset:             m.dataset,
		statsPath:           m.statsPath,
		mountPath:           m.mountPath,
		lines:               lines,
		elapsed:             elapsed,
		rows:                cloneMap(m.rows),
		phases:              cloneMap(m.phases),
		pathTextBytesBefore: m.pathTextBytesBefore,
		pathTextBytesAfter:  m.pathTextBytesAfter,
	}
}

func (m *datasetImportMetrics) timePhases(
	run func() error,
	phases ...string,
) error {
	start := time.Now()
	err := run()
	duration := time.Since(start)

	for _, phase := range phases {
		m.addPhase(phase, duration)
	}

	return err
}

type timedImportCloser struct {
	io.Closer
	metrics *datasetImportMetrics
	phase   string
}

func (c timedImportCloser) Close() error {
	return c.metrics.timePhases(c.Closer.Close, c.phase)
}

type trackedDGUTAWriter struct {
	db.DGUTAWriter
	metrics *datasetImportMetrics

	mountPath string
}

func (w *trackedDGUTAWriter) Add(record db.RecordDGUTA) error {
	err := w.DGUTAWriter.Add(record)
	if err == nil {
		dgutaRows := countDGUTARows(record, w.mountPath)
		catalogRows := countCatalogRows(record)

		w.metrics.addRows(tableDGUTA, dgutaRows)
		w.metrics.addRows(tableCatalog, catalogRows)
		w.metrics.addRows(tableDirFacts, countDirFactRows(record))
		w.metrics.addPathTextBytes(
			importPathTextBytesForRows(recordDirPath(record), dgutaRows),
			importPathTextBytesForRows(recordDirPath(record), catalogRows),
		)
	}

	return err
}

func countDGUTARows(record db.RecordDGUTA, mountPath string) uint64 {
	var rows uint64

	dir := string(record.Dir.AppendTo(nil))
	compactAges := compactInternalDGUTAAgesForImportMetrics(mountPath, dir)

	for _, guta := range record.GUTAs {
		if guta != nil && (!compactAges || guta.Age == db.DGUTAgeAll) {
			rows++
		}
	}

	return rows
}

func (w *trackedDGUTAWriter) Close() error {
	return w.DGUTAWriter.Close()
}

func (w *trackedDGUTAWriter) Abort() error {
	return summariseutil.CloseOrAbort(w.DGUTAWriter, false)
}

type importPhaseRecorderSetter interface {
	SetImportPhaseRecorder(recorder func(phase string, duration time.Duration))
}

type historyAppendInsertReporter interface {
	LastHistoryAppendInserted() bool
}

type trackedFileOperation struct {
	summary.Operation
	metrics *datasetImportMetrics
}

func (o *trackedFileOperation) Add(info *summary.FileInfo) error {
	err := o.metrics.timePhases(func() error {
		return o.Operation.Add(info)
	}, phaseFilesInsert)

	if err == nil && info != nil {
		o.metrics.addRows(tableFiles, 1)
		o.metrics.addPathTextBytes(importFileParentPathTextBytes(info), 0)
	}

	return err
}

type trackedBasedirsStore struct {
	basedirs.Store
	metrics *datasetImportMetrics
}

func (s *trackedBasedirsStore) Abort() error {
	return summariseutil.CloseOrAbort(s.Store, false)
}

func (s *trackedBasedirsStore) Reset() error {
	return s.metrics.timePhases(
		s.Store.Reset,
		phaseBasedirsReset,
		phasePartitionDropReset,
	)
}

func (s *trackedBasedirsStore) PutGroupUsage(u *basedirs.Usage) error {
	err := s.metrics.timePhases(func() error {
		return s.Store.PutGroupUsage(u)
	}, phaseBasedirsGroupUsage)

	if err == nil && u != nil {
		s.metrics.addRows(tableBasedirsGroupUsage, 1)
	}

	return err
}

func (s *trackedBasedirsStore) PutUserUsage(u *basedirs.Usage) error {
	err := s.metrics.timePhases(func() error {
		return s.Store.PutUserUsage(u)
	}, phaseBasedirsUserUsage)

	if err == nil && u != nil {
		s.metrics.addRows(tableBasedirsUserUsage, 1)
	}

	return err
}

func (s *trackedBasedirsStore) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	err := s.metrics.timePhases(func() error {
		return s.Store.PutGroupSubDirs(key, subdirs)
	}, phaseBasedirsGroupSubs)
	if err == nil {
		s.metrics.addRows(tableBasedirsGroupSubdirs, countNonNilSubDirs(subdirs))
	}

	return err
}

func countNonNilSubDirs(subdirs []*basedirs.SubDir) uint64 {
	var rows uint64

	for _, subdir := range subdirs {
		if subdir != nil {
			rows++
		}
	}

	return rows
}

func (s *trackedBasedirsStore) PutUserSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	err := s.metrics.timePhases(func() error {
		return s.Store.PutUserSubDirs(key, subdirs)
	}, phaseBasedirsUserSubs)
	if err == nil {
		s.metrics.addRows(tableBasedirsUserSubdirs, countNonNilSubDirs(subdirs))
	}

	return err
}

func (s *trackedBasedirsStore) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	err := s.metrics.timePhases(func() error {
		return s.Store.AppendGroupHistory(key, point)
	}, phaseBasedirsHistory)

	if err == nil && historyAppendInserted(s.Store) {
		s.metrics.addRows(tableBasedirsHistory, 1)
	}

	return err
}

func historyAppendInserted(store basedirs.Store) bool {
	reporter, ok := store.(historyAppendInsertReporter)
	if !ok {
		return true
	}

	return reporter.LastHistoryAppendInserted()
}

func (s *trackedBasedirsStore) Finalise() error {
	return s.metrics.timePhases(s.Store.Finalise, phaseBasedirsFinalise)
}

func (s *trackedBasedirsStore) Close() error {
	return s.metrics.timePhases(s.Store.Close, phaseBasedirsFlush)
}

type lineCountingReader struct {
	underlying io.Reader
	maxLines   uint64

	buf        []byte
	pending    []byte
	seenLines  uint64
	reachedMax bool
}

func (l *lineCountingReader) linesRead() uint64 {
	return l.seenLines
}

func (l *lineCountingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if n, ok, err := l.readPendingOrDone(p); ok {
		return n, err
	}

	if l.reachedMax {
		return 0, io.EOF
	}

	n, err := l.underlying.Read(l.buf)
	if n == 0 {
		return 0, err
	}

	chunk := l.buf[:n]
	allowed := l.limitChunk(chunk)

	nn := copy(p, allowed)
	if nn < len(allowed) {
		l.pending = append(l.pending[:0], allowed[nn:]...)
	}

	return nn, l.errAfterRead(err)
}

func (l *lineCountingReader) readPendingOrDone(p []byte) (int, bool, error) {
	n := l.readPending(p)
	if n == 0 {
		return 0, false, nil
	}

	if l.reachedMax && len(l.pending) == 0 {
		return n, true, io.EOF
	}

	return n, true, nil
}

func (l *lineCountingReader) errAfterRead(underlyingErr error) error {
	if l.reachedMax {
		if len(l.pending) == 0 {
			return io.EOF
		}

		return nil
	}

	return underlyingErr
}

func (l *lineCountingReader) readPending(p []byte) int {
	if len(l.pending) == 0 {
		return 0
	}

	n := copy(p, l.pending)
	l.pending = l.pending[n:]

	return n
}

func (l *lineCountingReader) limitChunk(chunk []byte) []byte {
	if l.maxLines == 0 {
		l.seenLines += countNewLines(chunk)

		return chunk
	}

	for i, b := range chunk {
		if b != '\n' {
			continue
		}

		l.seenLines++
		if l.seenLines >= l.maxLines {
			l.reachedMax = true

			return chunk[:i+1]
		}
	}

	return chunk
}

func countNewLines(b []byte) uint64 {
	var lines uint64

	for _, c := range b {
		if c == '\n' {
			lines++
		}
	}

	return lines
}

func compactInternalDGUTAAgesForImportMetrics(mountPath, dir string) bool {
	mountPath = normalizeImportMountPathForMetrics(mountPath)

	return mountPath != "" &&
		mountPath != "/" &&
		dir != mountPath &&
		strings.HasPrefix(dir, mountPath)
}

func normalizeImportMountPathForMetrics(mountPath string) string {
	if mountPath == "" || mountPath == "/" || strings.HasSuffix(mountPath, "/") {
		return mountPath
	}

	return mountPath + "/"
}
