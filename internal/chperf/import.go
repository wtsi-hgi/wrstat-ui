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
	"os"
	"path/filepath"
	"sort"
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
	phaseMountSwitch        = "mount_switch"
	phaseOldSnapshotDrop    = "old_snapshot_partition_drop"
	phaseBasedirsReset      = "wrstat_basedirs_reset"
	phaseBasedirsGroupUsage = "wrstat_basedirs_group_usage_insert"
	phaseBasedirsUserUsage  = "wrstat_basedirs_user_usage_insert"
	phaseBasedirsGroupSubs  = "wrstat_basedirs_group_subdirs_insert"
	phaseBasedirsUserSubs   = "wrstat_basedirs_user_subdirs_insert"
	phaseBasedirsHistory    = "wrstat_basedirs_history_insert"
	phaseBasedirsFinalise   = "wrstat_basedirs_finalise"
	phaseBasedirsFlush      = "wrstat_basedirs_flush"
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

	sort.Strings(dirs)

	return dirs, nil
}

func addImportReportOperations(
	report *boltperf.Report,
	results []datasetImportResult,
	parallelism int,
	totalDuration time.Duration,
) {
	for _, result := range results {
		report.AddOperation("import_file_total", map[string]any{
			"dataset":                    result.dataset,
			"stats_path":                 result.statsPath,
			"mount_path":                 result.mountPath,
			"lines":                      result.lines,
			"rows_per_table":             cloneUint64Map(result.rows),
			"throughput_records_per_sec": throughputPerSecond(result.records(), result.elapsed),
		}, []float64{durationMS(result.elapsed)})

		for _, phase := range sortedImportPhases(result.phases) {
			inputs := map[string]any{
				"dataset":    result.dataset,
				"stats_path": result.statsPath,
				"mount_path": result.mountPath,
				"phase":      phase,
			}
			addImportPhaseInputs(inputs, result, phase)

			report.AddOperation("import_phase", inputs, []float64{durationMS(result.phases[phase])})
		}
	}

	report.AddOperation("import_total", map[string]any{
		"datasets":                   len(results),
		"records":                    totalImportRecords(results),
		"parallelism":                parallelism,
		"mode":                       importMode(parallelism),
		"throughput_records_per_sec": throughputPerSecond(totalImportRecords(results), totalDuration),
	}, []float64{durationMS(totalDuration)})
}

func cloneUint64Map(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return map[string]uint64{}
	}

	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}

	return dst
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
	names := make([]string, 0, len(phases))
	for phase := range phases {
		names = append(names, phase)
	}

	sort.Strings(names)

	return names
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
	table, ok := importMainTablePhase(phase)
	if !ok {
		table, ok = importBasedirsTablePhase(phase)
		if !ok {
			return "", 0, false
		}
	}

	return table, result.rows[table], true
}

func importMainTablePhase(phase string) (string, bool) {
	switch phase {
	case phaseFilesInsert, phaseFilesFlush:
		return "wrstat_files", true
	case phaseDGUTAInsert:
		return "wrstat_dguta", true
	case phaseChildrenInsert:
		return "wrstat_children", true
	default:
		return "", false
	}
}

func importBasedirsTablePhase(phase string) (string, bool) {
	switch phase {
	case phaseBasedirsGroupUsage:
		return "wrstat_basedirs_group_usage", true
	case phaseBasedirsUserUsage:
		return "wrstat_basedirs_user_usage", true
	case phaseBasedirsGroupSubs:
		return "wrstat_basedirs_group_subdirs", true
	case phaseBasedirsUserSubs:
		return "wrstat_basedirs_user_subdirs", true
	case phaseBasedirsHistory:
		return "wrstat_basedirs_history", true
	default:
		return "", false
	}
}

func importMultiTablePhase(phase string) ([]string, bool) {
	switch phase {
	case phasePartitionDropReset:
		return []string{
			"wrstat_dguta",
			"wrstat_children",
			"wrstat_files",
			"wrstat_basedirs_group_usage",
			"wrstat_basedirs_user_usage",
			"wrstat_basedirs_group_subdirs",
			"wrstat_basedirs_user_subdirs",
		}, true
	case phaseBasedirsReset, phaseBasedirsFlush:
		return []string{
			"wrstat_basedirs_group_usage",
			"wrstat_basedirs_user_usage",
			"wrstat_basedirs_group_subdirs",
			"wrstat_basedirs_user_subdirs",
		}, true
	case phaseBasedirsFinalise:
		return []string{"wrstat_basedirs_group_usage", "wrstat_basedirs_history"}, true
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

func importMode(parallelism int) string {
	if parallelism > 1 {
		return "parallel"
	}

	return "serial"
}

func effectiveParallelism(parallelism int) int {
	if parallelism < 1 {
		return 1
	}

	if parallelism > maxImportParallel {
		return maxImportParallel
	}

	return parallelism
}

func importDatasets(
	api ImportAPI,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) ([]datasetImportResult, error) {
	if opts.Parallelism <= 1 {
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

func newDatasetImportMetrics(dataset, statsPath, mountPath string) *datasetImportMetrics {
	return &datasetImportMetrics{
		dataset:   dataset,
		statsPath: statsPath,
		mountPath: mountPath,
		rows:      make(map[string]uint64),
		phases:    make(map[string]time.Duration),
	}
}

func ingestStatsGZWithMetrics(
	api ImportAPI,
	statsPath, mp string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (_ uint64, err error) {
	gz, closeFn, err := openStatsGZReader(statsPath)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := closeFn(); err == nil {
			err = cerr
		}
	}()

	return summariseReader(gz, api, mp, updatedAt, opts, metrics)
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

func setImportBatchSize(batchSize int, targets ...any) {
	if batchSize <= 0 {
		return
	}

	for _, target := range targets {
		setter, ok := target.(batchSizeSetter)
		if !ok {
			continue
		}

		setter.SetBatchSize(batchSize)
	}
}

func trackFileIngestOperation(
	gen summary.OperationGenerator,
	metrics *datasetImportMetrics,
) summary.OperationGenerator {
	return func() summary.Operation {
		return &trackedFileOperation{Operation: gen(), metrics: metrics}
	}
}

func closeImportBasedirsStore(store basedirs.Store, publish bool) error {
	if store == nil {
		return nil
	}

	if publish {
		return store.Close()
	}

	aborter, ok := store.(interface{ Abort() error })
	if ok {
		return aborter.Abort()
	}

	return store.Close()
}

func composeImportCloser(
	fileCloser io.Closer,
	basedirsCloser func(bool) error,
	dgutaCloser abortableCloser,
) func(bool) error {
	return func(publish bool) error {
		fileErr := closeImportFile(fileCloser)
		shouldPublishBasedirs := publish && fileErr == nil

		basedirsErr := closeImportBasedirs(basedirsCloser, shouldPublishBasedirs)
		dgutaErr := closeImportDGUTA(
			dgutaCloser,
			shouldPublishBasedirs && basedirsErr == nil,
		)

		if shouldPublishBasedirs && (basedirsErr != nil || dgutaErr != nil) {
			basedirsErr = errors.Join(basedirsErr, closeImportBasedirs(basedirsCloser, false))
		}

		return errors.Join(fileErr, basedirsErr, dgutaErr)
	}
}

func closeImportFile(fileCloser io.Closer) error {
	if fileCloser == nil {
		return nil
	}

	return fileCloser.Close()
}

func closeImportBasedirs(basedirsCloser func(bool) error, publish bool) error {
	if basedirsCloser == nil {
		return nil
	}

	return basedirsCloser(publish)
}

func closeImportDGUTA(dgutaCloser abortableCloser, publish bool) error {
	if dgutaCloser == nil {
		return nil
	}

	if publish {
		return dgutaCloser.Close()
	}

	return dgutaCloser.Abort()
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

	return collectImportResults(results)
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

func collectImportResults(results []importResult) ([]datasetImportResult, error) {
	imports := make([]datasetImportResult, 0, len(results))

	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}

		imports = append(imports, r.dataset)
	}

	return imports, nil
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

func importOneDataset(
	api ImportAPI,
	datasetDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (_ datasetImportResult, err error) {
	mp, err := mountpath.FromOutputDir(datasetDir)
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
	metrics := newDatasetImportMetrics(filepath.Base(datasetDir), statsPath, mp)

	records, err := ingestStatsGZ(api, statsPath, mp, updatedAt, opts, metrics)
	if err != nil {
		return datasetImportResult{}, err
	}

	printf("import dataset=%s mount=%s records=%d seconds=%.3f\n",
		filepath.Base(datasetDir), mp, records, time.Since(start).Seconds())

	return metrics.result(records, time.Since(start)), nil
}

func ingestStatsGZ(
	api ImportAPI,
	statsPath, mp string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (_ uint64, err error) {
	return ingestStatsGZWithMetrics(api, statsPath, mp, updatedAt, opts, metrics)
}

func openStatsGZReader(path string) (*pgzip.Reader, func() error, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	gz, err := pgzip.NewReader(fh)
	if err != nil {
		_ = fh.Close()

		return nil, nil, err
	}

	closeFn := func() error {
		gzErr := gz.Close()
		fhErr := fh.Close()

		return errors.Join(gzErr, fhErr)
	}

	return gz, closeFn, nil
}

func summariseReader(
	r io.Reader,
	api ImportAPI,
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (_ uint64, err error) {
	lr := newLineCountingReader(r, opts.MaxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	allClosers, err := addAllSummarisers(ss, api, mp, updatedAt, opts, metrics)
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
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (func(bool) error, error) {
	dw, err := api.NewDGUTAWriter()
	if err != nil {
		return nil, err
	}

	timedDW := newTrackedDGUTAWriter(dw, metrics)

	timedDW.SetMountPath(mp)
	timedDW.SetUpdatedAt(updatedAt)

	fi, fiCloser, err := api.NewFileIngestOperation(mp, updatedAt)
	if err != nil {
		return nil, errors.Join(err, timedDW.Abort())
	}

	setImportBatchSize(opts.BatchSize, timedDW, fiCloser)

	setImportPhaseRecorder(fiCloser, metrics)

	timedFI := trackFileIngestOperation(fi, metrics)
	timedFICloser := timedImportCloser{Closer: fiCloser, metrics: metrics, phase: phaseFilesFlush}

	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(timedDW))
	ss.AddGlobalOperation(timedFI)

	bsCloser, err := addBasedirsSummariser(ss, api, mp, updatedAt, opts, metrics)
	if err != nil {
		return nil, errors.Join(err, composeImportCloser(timedFICloser, nil, timedDW)(false))
	}

	return composeImportCloser(timedFICloser, bsCloser, timedDW), nil
}

func addBasedirsSummariser(
	ss *summary.Summariser,
	api ImportAPI,
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
	metrics *datasetImportMetrics,
) (func(bool) error, error) {
	if opts.QuotaPath == "" || opts.ConfigPath == "" {
		return func(bool) error { return nil }, nil
	}

	bs, err := api.NewBaseDirsStore()
	if err != nil {
		return nil, err
	}

	setImportBatchSize(opts.BatchSize, bs)

	timedBS := &trackedBasedirsStore{Store: bs, metrics: metrics}

	timedBS.SetMountPath(mp)
	timedBS.SetUpdatedAt(updatedAt)

	closer := func(publish bool) error {
		return closeImportBasedirsStore(timedBS, publish)
	}

	if err := addBasedirsOp(ss, timedBS, updatedAt, opts); err != nil {
		_ = timedBS.Close()

		return nil, err
	}

	return closer, nil
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

	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		return err
	}

	if len(mountpoints) > 0 {
		bd.SetMountPoints(mountpoints)
	}

	bd.SetModTime(modtime)
	ss.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func parseBasedirsInputs(opts ImportOptions) (*basedirs.Quotas, basedirs.Config, []string, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(opts.QuotaPath, opts.ConfigPath)
	if err != nil {
		return nil, nil, nil, err
	}

	var mountpoints []string

	if opts.MountsPath != "" {
		mountpoints, err = summariseutil.ParseMountpointsFromFile(opts.MountsPath)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return quotas, config, mountpoints, nil
}

type batchSizeSetter interface {
	SetBatchSize(batchSize int)
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
		rows:      cloneUint64Map(m.rows),
		phases:    cloneDurationMap(m.phases),
	}
}

func cloneDurationMap(src map[string]time.Duration) map[string]time.Duration {
	if len(src) == 0 {
		return map[string]time.Duration{}
	}

	dst := make(map[string]time.Duration, len(src))
	for k, v := range src {
		dst[k] = v
	}

	return dst
}

type timedImportCloser struct {
	io.Closer
	metrics *datasetImportMetrics
	phase   string
}

func (c timedImportCloser) Close() error {
	start := time.Now()
	err := c.Closer.Close()
	c.metrics.addPhase(c.phase, time.Since(start))

	return err
}

type trackedDGUTAWriter struct {
	db.DGUTAWriter
	metrics *datasetImportMetrics
}

func (w *trackedDGUTAWriter) Add(record db.RecordDGUTA) error {
	err := w.DGUTAWriter.Add(record)
	if err == nil {
		w.metrics.addRows("wrstat_dguta", countDGUTARows(record))
		w.metrics.addRows("wrstat_children", countChildrenRows(record.Children))
	}

	return err
}

func countDGUTARows(record db.RecordDGUTA) uint64 {
	var rows uint64

	for _, guta := range record.GUTAs {
		if guta != nil {
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
	aborter, ok := w.DGUTAWriter.(interface{ Abort() error })
	if ok {
		return aborter.Abort()
	}

	return w.DGUTAWriter.Close()
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
	start := time.Now()
	err := o.Operation.Add(info)
	o.metrics.addPhase(phaseFilesInsert, time.Since(start))

	if err == nil && info != nil {
		o.metrics.addRows("wrstat_files", 1)
	}

	return err
}

type trackedBasedirsStore struct {
	basedirs.Store
	metrics *datasetImportMetrics
}

func (s *trackedBasedirsStore) Abort() error {
	aborter, ok := s.Store.(interface{ Abort() error })
	if ok {
		return aborter.Abort()
	}

	return s.Store.Close()
}

func (s *trackedBasedirsStore) Reset() error {
	start := time.Now()
	err := s.Store.Reset()
	duration := time.Since(start)
	s.metrics.addPhase(phaseBasedirsReset, duration)
	s.metrics.addPhase(phasePartitionDropReset, duration)

	return err
}

func (s *trackedBasedirsStore) PutGroupUsage(u *basedirs.Usage) error {
	start := time.Now()
	err := s.Store.PutGroupUsage(u)
	s.metrics.addPhase(phaseBasedirsGroupUsage, time.Since(start))

	if err == nil && u != nil {
		s.metrics.addRows("wrstat_basedirs_group_usage", 1)
	}

	return err
}

func (s *trackedBasedirsStore) PutUserUsage(u *basedirs.Usage) error {
	start := time.Now()
	err := s.Store.PutUserUsage(u)
	s.metrics.addPhase(phaseBasedirsUserUsage, time.Since(start))

	if err == nil && u != nil {
		s.metrics.addRows("wrstat_basedirs_user_usage", 1)
	}

	return err
}

func (s *trackedBasedirsStore) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	start := time.Now()
	err := s.Store.PutGroupSubDirs(key, subdirs)
	s.metrics.addPhase(phaseBasedirsGroupSubs, time.Since(start))

	if err == nil {
		s.metrics.addRows("wrstat_basedirs_group_subdirs", countNonNilSubDirs(subdirs))
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
	start := time.Now()
	err := s.Store.PutUserSubDirs(key, subdirs)
	s.metrics.addPhase(phaseBasedirsUserSubs, time.Since(start))

	if err == nil {
		s.metrics.addRows("wrstat_basedirs_user_subdirs", countNonNilSubDirs(subdirs))
	}

	return err
}

func (s *trackedBasedirsStore) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	start := time.Now()
	err := s.Store.AppendGroupHistory(key, point)
	s.metrics.addPhase(phaseBasedirsHistory, time.Since(start))

	if err == nil && historyAppendInserted(s.Store) {
		s.metrics.addRows("wrstat_basedirs_history", 1)
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
	start := time.Now()
	err := s.Store.Finalise()
	s.metrics.addPhase(phaseBasedirsFinalise, time.Since(start))

	return err
}

func (s *trackedBasedirsStore) Close() error {
	start := time.Now()
	err := s.Store.Close()
	s.metrics.addPhase(phaseBasedirsFlush, time.Since(start))

	return err
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
	var n uint64

	for _, c := range b {
		if c == '\n' {
			n++
		}
	}

	return n
}
