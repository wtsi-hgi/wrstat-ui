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
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
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

	phasePartitionDropReset = "partition_drop_reset"
	phaseFilesInsert        = "wrstat_files_insert"
	phaseFilesFlush         = "wrstat_files_flush"
	phaseDGUTAInsert        = "wrstat_dguta_insert"
	phaseChildrenInsert     = "wrstat_children_insert"
	phaseDirProjectionWrite = "wrstat_dir_projection_insert"
	phaseMountSwitch        = "mount_switch"
	phaseTreeSummaryRefresh = "wrstat_tree_summary_refresh"
	phaseOldSnapshotDrop    = "old_snapshot_partition_drop"
	phaseBasedirsReset      = "wrstat_basedirs_reset"
	phaseBasedirsGroupUsage = "wrstat_basedirs_group_usage_insert"
	phaseBasedirsUserUsage  = "wrstat_basedirs_user_usage_insert"
	phaseBasedirsGroupSubs  = "wrstat_basedirs_group_subdirs_insert"
	phaseBasedirsUserSubs   = "wrstat_basedirs_user_subdirs_insert"
	phaseBasedirsHistory    = "wrstat_basedirs_history_insert"
	phaseBasedirsFinalise   = "wrstat_basedirs_finalise"
	phaseBasedirsFlush      = "wrstat_basedirs_flush"

	tableFiles                = "wrstat_files"
	tableDGUTA                = "wrstat_dguta"
	tableChildren             = "wrstat_children"
	tableDirSummary           = "wrstat_dir_summary"
	tableDirSummarySets       = "wrstat_dir_summary_sets"
	tableDirDGUTAVector       = "wrstat_dir_dguta_vector"
	tableTreeSummarySets      = "wrstat_tree_summary_sets"
	tableTreeDGUTA            = "wrstat_tree_dguta"
	tableTreeDirSummary       = "wrstat_tree_dir_summary"
	tableTreeChildren         = "wrstat_tree_children"
	tableBasedirsGroupUsage   = "wrstat_basedirs_group_usage"
	tableBasedirsUserUsage    = "wrstat_basedirs_user_usage"
	tableBasedirsGroupSubdirs = "wrstat_basedirs_group_subdirs"
	tableBasedirsUserSubdirs  = "wrstat_basedirs_user_subdirs"
	tableBasedirsHistory      = "wrstat_basedirs_history"
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
)

// ErrNoDatasets indicates no dataset directories were found.
var ErrNoDatasets = errors.New("no dataset directories found")

// PrintfFunc matches fmt.Printf-style output.
type PrintfFunc = boltperf.PrintfFunc

// Import discovers stats.gz datasets under inputDir, ingests them into
// ClickHouse, and returns a Report with timing information.
func Import(
	api ImportAPI,
	inputDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (boltperf.Report, error) {
	datasetDirs, err := findDatasets(inputDir)
	if err != nil {
		return boltperf.Report{}, err
	}

	report := boltperf.NewReport("clickhouse", inputDir, 1, 0)
	startAll := time.Now()

	results, err := importDatasets(api, datasetDirs, opts, printf)
	if err != nil {
		return boltperf.Report{}, err
	}

	addImportReportOperations(&report, results, effectiveParallelism(opts.Parallelism), time.Since(startAll))

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

func addImportReportOperations(
	report *boltperf.Report,
	results []datasetImportResult,
	parallelism int,
	totalDuration time.Duration,
) {
	totalRecords := totalImportRecords(results)

	for _, result := range results {
		report.AddOperation("import_file_total", map[string]any{
			importInputDataset:           result.dataset,
			importInputStatsPath:         result.statsPath,
			importInputMountPath:         result.mountPath,
			"lines":                      result.lines,
			"rows_per_table":             cloneMap(result.rows),
			"throughput_records_per_sec": throughputPerSecond(result.records(), result.elapsed),
		}, []float64{durationMS(result.elapsed)})

		for _, phase := range sortedImportPhases(result.phases) {
			inputs := map[string]any{
				importInputDataset:   result.dataset,
				importInputStatsPath: result.statsPath,
				importInputMountPath: result.mountPath,
				"phase":              phase,
			}
			addImportPhaseInputs(inputs, result, phase)

			report.AddOperation("import_phase", inputs, []float64{durationMS(result.phases[phase])})
		}

		addImportGuardrailOperations(report, result)
	}

	report.AddOperation("import_total", map[string]any{
		"datasets":                   len(results),
		"records":                    totalRecords,
		"parallelism":                parallelism,
		"mode":                       importMode(parallelism),
		"throughput_records_per_sec": throughputPerSecond(totalRecords, totalDuration),
	}, []float64{durationMS(totalDuration)})
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
		inputs["table"] = table
		inputs["rows"] = rows

		return
	}

	if tables, ok := importMultiTablePhase(phase); ok {
		inputs["tables"] = tables
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
	case phaseChildrenInsert:
		return tableChildren, true
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
	switch phase {
	case phasePartitionDropReset:
		return []string{
			tableDGUTA,
			tableChildren,
			tableFiles,
			tableDirSummary,
			tableDirSummarySets,
			tableDirDGUTAVector,
			tableBasedirsGroupUsage,
			tableBasedirsUserUsage,
			tableBasedirsGroupSubdirs,
			tableBasedirsUserSubdirs,
		}, true
	case phaseDirProjectionWrite:
		return []string{tableDirSummary, tableDirSummarySets, tableDirDGUTAVector}, true
	case phaseBasedirsReset, phaseBasedirsFlush:
		return []string{
			tableBasedirsGroupUsage,
			tableBasedirsUserUsage,
			tableBasedirsGroupSubdirs,
			tableBasedirsUserSubdirs,
		}, true
	case phaseBasedirsFinalise:
		return []string{tableBasedirsGroupUsage, tableBasedirsHistory}, true
	case phaseTreeSummaryRefresh:
		return []string{tableTreeSummarySets, tableTreeDGUTA, tableTreeDirSummary, tableTreeChildren}, true
	default:
		return nil, false
	}
}

func totalImportRecords(results []datasetImportResult) uint64 {
	var total uint64

	for _, result := range results {
		total += result.records()
	}

	return total
}

func addImportGuardrailOperations(report *boltperf.Report, result datasetImportResult) {
	addRawFileIngestGuardrail(report, result)
	addPhaseImportGuardrail(report, result, importGuardrailActiveSnapshotPublish, phaseMountSwitch, nil)
	addPhaseImportGuardrail(
		report,
		result,
		importGuardrailMaintainedDirProjection,
		phaseDirProjectionWrite,
		importGuardrailTables(phaseDirProjectionWrite),
	)
	addPhaseImportGuardrail(
		report,
		result,
		importGuardrailActiveTreeSummaryRefresh,
		phaseTreeSummaryRefresh,
		importGuardrailTables(phaseTreeSummaryRefresh),
	)
}

func addRawFileIngestGuardrail(report *boltperf.Report, result datasetImportResult) {
	status, duration := importGuardrailStatusAndDuration(rawFileIngestObserved(result), result.elapsed)
	inputs := importGuardrailInputs(result, importGuardrailRawFileIngest, status)
	inputs["table"] = tableFiles
	inputs["rows"] = result.rows[tableFiles]
	inputs["lines"] = result.lines
	inputs["phases"] = []string{phaseFilesInsert, phaseFilesFlush}
	inputs["throughput_records_per_sec"] = throughputPerSecond(result.records(), result.elapsed)

	report.AddOperation(importGuardrailOperation, inputs, []float64{durationMS(duration)})
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
	report *boltperf.Report,
	result datasetImportResult,
	guardrail string,
	phase string,
	tables []string,
) {
	phaseDuration, observed := result.phases[phase]
	status, duration := importGuardrailStatusAndDuration(observed, phaseDuration)
	inputs := importGuardrailInputs(result, guardrail, status)
	inputs["phase"] = phase

	if len(tables) > 0 {
		inputs["tables"] = slices.Clone(tables)
	}

	report.AddOperation(importGuardrailOperation, inputs, []float64{durationMS(duration)})
}

func importGuardrailTables(phase string) []string {
	tables, ok := importMultiTablePhase(phase)
	if !ok {
		return nil
	}

	return tables
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
	dw, err := api.NewDGUTAWriter()
	if err != nil {
		return nil, err
	}

	trackedDW := newTrackedDGUTAWriter(dw, metrics)

	trackedDW.SetMountPath(mountPath)
	trackedDW.SetUpdatedAt(updatedAt)

	fi, fiCloser, err := api.NewFileIngestOperation(mountPath, updatedAt)
	if err != nil {
		return nil, errors.Join(err, trackedDW.Abort())
	}

	summariseutil.SetBatchSize(opts.BatchSize, trackedDW, fiCloser)

	setImportPhaseRecorder(fiCloser, metrics)

	trackedFI := trackFileIngestOperation(fi, metrics)
	timedFICloser := timedImportCloser{Closer: fiCloser, metrics: metrics, phase: phaseFilesFlush}

	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(trackedDW))
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
		dataset:   m.dataset,
		statsPath: m.statsPath,
		mountPath: m.mountPath,
		lines:     lines,
		elapsed:   elapsed,
		rows:      cloneMap(m.rows),
		phases:    cloneMap(m.phases),
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
		w.metrics.addRows(tableDGUTA, countDGUTARows(record, w.mountPath))
		w.metrics.addRows(tableChildren, countChildrenRows(record.Children))
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

func countChildrenRows(children []string) uint64 {
	var rows uint64

	for _, child := range children {
		if strings.TrimSuffix(child, "/") != "" {
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
