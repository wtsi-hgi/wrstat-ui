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

package cmd

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirbuild"
)

const (
	summariseRecentPhaseLimit = 6
	summariseBaseFieldCount   = 4
	summariseStateFieldCount  = 13
	unknownRSS                = "unknown"
	redactedSecret            = "xxxxx"
)

var summariseProcessRSSMiB = processRSSMiB

type summarisePhaseDuration struct {
	phase    string
	duration time.Duration
}

type summariseDiagnostics struct {
	mu sync.Mutex

	input     string
	outputDir string
	target    *clickHouseSummariseTarget
	started   time.Time

	currentPhase                          string
	lastRecords                           uint64
	inputRows                             uint64
	directoryNodes                        uint64
	impliedDirectories                    uint64
	maximumDepth                          uint64
	depthHistogram                        []uint64
	heavyPrefixes                         []dirbuild.PrefixCount
	sqliteBytes                           uint64
	spoolBytes                            uint64
	rowsPerSecond                         float64
	phaseElapsed                          time.Duration
	clickHouseRowsSent                    uint64
	clickHouseBytesSent                   uint64
	clickHouseBytesAvailable              bool
	clickHouseBatchCount                  uint64
	clickHouseEstimatedUncompressedBytes  uint64
	clickHouseLastBatchEstimatedBytes     uint64
	clickHouseServerParts                 uint64
	clickHouseServerPartsAvailable        bool
	clickHouseServerMerges                uint64
	clickHouseServerMergesAvailable       bool
	clickHouseServerMemoryBytes           uint64
	clickHouseServerMemoryAvailable       bool
	clickHouseServerQueryLatency          time.Duration
	clickHouseServerQueryLatencyAvailable bool
	clickHouseServerPressureBackoff       bool
	currentCheckpoint                     string

	phaseTotals  map[string]time.Duration
	recentPhases []summarisePhaseDuration

	signalStop func()
}

func newSummariseDiagnostics(input string) *summariseDiagnostics {
	return &summariseDiagnostics{
		input:        input,
		started:      time.Now(),
		currentPhase: "initialising",
		phaseTotals:  make(map[string]time.Duration),
		recentPhases: make([]summarisePhaseDuration, 0, summariseRecentPhaseLimit),
		signalStop:   func() {},
	}
}

func (d *summariseDiagnostics) setOutputDir(outputDir string) {
	if d == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.outputDir = outputDir
}

func (d *summariseDiagnostics) setTarget(target *clickHouseSummariseTarget) {
	if d == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.target = target
	if target != nil {
		d.outputDir = target.outputDir
	}
}

func (d *summariseDiagnostics) setCurrentPhase(phase string) {
	if d == nil || phase == "" {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.currentPhase = phase
}

func (d *summariseDiagnostics) beginPhase(phase string) {
	if d == nil || phase == "" {
		return
	}

	d.setCurrentPhase(phase)
	d.logState("summarise phase start")
}

func (d *summariseDiagnostics) observeDirbuildTelemetry(snapshot dirbuild.Telemetry, spoolBytes uint64) {
	if d == nil {
		return
	}

	d.mu.Lock()
	if snapshot.Phase != "" {
		d.currentPhase = snapshot.Phase
	}

	d.lastRecords = snapshot.InputRows
	d.inputRows = snapshot.InputRows
	d.directoryNodes = snapshot.DirectoryNodes
	d.impliedDirectories = snapshot.ImpliedDirectories
	d.maximumDepth = snapshot.MaximumDepth
	d.depthHistogram = append(d.depthHistogram[:0], snapshot.DepthHistogram...)
	d.heavyPrefixes = append(d.heavyPrefixes[:0], snapshot.HeavyPrefixes...)
	d.sqliteBytes = snapshot.SQLiteBytes
	d.spoolBytes = spoolBytes
	d.phaseElapsed = snapshot.PhaseElapsed
	d.rowsPerSecond = dirbuildRowsPerSecond(snapshot)
	d.mu.Unlock()

	d.logState("summarise phase progress")
}

func dirbuildRowsPerSecond(snapshot dirbuild.Telemetry) float64 {
	if snapshot.Phase != dirbuild.PhaseDirectoryScan && snapshot.Phase != dirbuild.PhasePass2Aggregation {
		return 0
	}

	return diagnosticRowsPerSecond(snapshot.PhaseRows, snapshot.PhaseElapsed)
}

func (d *summariseDiagnostics) setShapeTelemetry(snapshot dirbuild.Telemetry) {
	if d == nil {
		return
	}

	d.mu.Lock()
	d.directoryNodes = snapshot.DirectoryNodes
	d.impliedDirectories = snapshot.ImpliedDirectories
	d.maximumDepth = snapshot.MaximumDepth
	d.depthHistogram = append(d.depthHistogram[:0], snapshot.DepthHistogram...)
	d.heavyPrefixes = append(d.heavyPrefixes[:0], snapshot.HeavyPrefixes...)
	d.mu.Unlock()
}

func (d *summariseDiagnostics) recordImportPhase(phase string, duration time.Duration) {
	if d == nil || phase == "" || duration <= 0 {
		return
	}

	d.mu.Lock()

	d.currentPhase = phase
	d.phaseTotals[phase] += duration

	d.recentPhases = append(d.recentPhases, summarisePhaseDuration{
		phase:    phase,
		duration: duration,
	})
	if len(d.recentPhases) > summariseRecentPhaseLimit {
		d.recentPhases = d.recentPhases[len(d.recentPhases)-summariseRecentPhaseLimit:]
	}
	d.mu.Unlock()
}

func (d *summariseDiagnostics) observeClickHouseTelemetry(snapshot clickhouse.SummariseImportTelemetry) {
	if d == nil {
		return
	}

	d.mu.Lock()
	if snapshot.Phase != "" {
		d.currentPhase = snapshot.Phase
	}

	d.currentCheckpoint = snapshot.CurrentCheckpoint
	d.clickHouseRowsSent = snapshot.RowsSent
	d.clickHouseBytesSent = snapshot.BytesSent
	d.clickHouseBytesAvailable = snapshot.BytesSentAvailable
	d.clickHouseBatchCount = snapshot.BatchCount
	d.clickHouseEstimatedUncompressedBytes = snapshot.EstimatedUncompressedBytesSent
	d.clickHouseLastBatchEstimatedBytes = snapshot.LastBatchEstimatedUncompressedBytes
	d.phaseElapsed = snapshot.PhaseElapsed
	d.rowsPerSecond = diagnosticRowsPerSecond(snapshot.PhaseRows, snapshot.PhaseElapsed)
	d.observeClickHousePressureTelemetryLocked(snapshot)
	d.mu.Unlock()
	d.logState("summarise clickhouse progress")
}

func diagnosticRowsPerSecond(rows uint64, elapsed time.Duration) float64 {
	if rows == 0 || elapsed <= 0 {
		return 0
	}

	return float64(rows) / elapsed.Seconds()
}

func (d *summariseDiagnostics) observeClickHousePressureTelemetryLocked(
	snapshot clickhouse.SummariseImportTelemetry,
) {
	d.clickHouseServerParts = snapshot.ServerPartCount
	d.clickHouseServerPartsAvailable = snapshot.ServerPartCountAvailable
	d.clickHouseServerMerges = snapshot.ServerActiveMerges
	d.clickHouseServerMergesAvailable = snapshot.ServerActiveMergesAvailable
	d.clickHouseServerMemoryBytes = snapshot.ServerMemoryBytes
	d.clickHouseServerMemoryAvailable = snapshot.ServerMemoryBytesAvailable
	d.clickHouseServerQueryLatency = snapshot.ServerQueryLatency
	d.clickHouseServerQueryLatencyAvailable = snapshot.ServerQueryLatencyAvailable
	d.clickHouseServerPressureBackoff = snapshot.ServerPressureBackoff
}

func (d *summariseDiagnostics) setSpoolBytes(bytes uint64) {
	if d == nil {
		return
	}

	d.mu.Lock()
	d.spoolBytes = bytes
	d.mu.Unlock()
}

func (d *summariseDiagnostics) setProgress(s *summary.Summariser) {
	if d == nil || s == nil {
		return
	}

	s.SetProgress(summariseProgressEveryRows, d.logProgress)
}

func (d *summariseDiagnostics) logStart() {
	if d == nil {
		return
	}

	d.setCurrentPhase("preflight")

	fields := d.baseFields()
	fields = append(fields,
		"signals="+quoteForDiagnostics("SIGTERM,SIGINT,SIGQUIT"),
		"note="+quoteForDiagnostics("SIGKILL cannot be caught; use the last progress/RSS breadcrumb before an LSF kill"),
	)

	info("summarise start %s", strings.Join(fields, " "))
}

func (d *summariseDiagnostics) logProgress(records uint64, elapsed time.Duration) {
	if d == nil {
		return
	}

	currentPhase, recent, telemetry := d.recordProgress(records, elapsed)

	mem := memorySnapshot()

	info(
		"summarise progress input=%s records=%d elapsed=%s current_phase=%s "+
			"heap_alloc_mb=%d heap_sys_mb=%d rss_mb=%s goroutines=%d gc_count=%d recent_import_phases=%s %s",
		quoteForDiagnostics(d.input),
		records,
		elapsed.Round(time.Second),
		currentPhase,
		mem.heapAllocMiB,
		mem.heapSysMiB,
		mem.rssMiB,
		mem.goroutines,
		mem.gcCount,
		quoteForDiagnostics(recent),
		strings.Join(telemetry, " "),
	)
}

func memorySnapshot() summariseMemorySnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	rss := unknownRSS
	if mb, ok := summariseProcessRSSMiB(); ok {
		rss = strconv.FormatUint(mb, 10)
	}

	return summariseMemorySnapshot{
		heapAllocMiB: bytesToMiB(mem.HeapAlloc),
		heapSysMiB:   bytesToMiB(mem.HeapSys),
		rssMiB:       rss,
		goroutines:   runtime.NumGoroutine(),
		gcCount:      mem.NumGC,
	}
}

func (d *summariseDiagnostics) logInputProgress(records uint64, elapsed time.Duration) {
	if d == nil {
		return
	}

	d.logProgress(records, elapsed)
}

func (d *summariseDiagnostics) telemetryFieldsLocked() []string {
	return []string{
		fmt.Sprintf("input_rows=%d", d.inputRows),
		fmt.Sprintf("directory_nodes=%d", d.directoryNodes),
		fmt.Sprintf("implied_directories=%d", d.impliedDirectories),
		fmt.Sprintf("maximum_depth=%d", d.maximumDepth),
		fmt.Sprintf("sqlite_bytes=%d", d.sqliteBytes),
		fmt.Sprintf("spool_bytes=%d", d.spoolBytes),
		fmt.Sprintf("rows_per_second=%.2f", d.rowsPerSecond),
		"phase_elapsed=" + formatDiagnosticDuration(d.phaseElapsed),
		"depth_histogram=" + quoteForDiagnostics(formatDepthHistogram(d.depthHistogram)),
		"heavy_prefixes=" + quoteForDiagnostics(formatHeavyPrefixes(d.heavyPrefixes)),
		fmt.Sprintf("clickhouse_rows_sent=%d", d.clickHouseRowsSent),
		"clickhouse_bytes_sent=" + diagnosticOptionalUint64(d.clickHouseBytesSent, d.clickHouseBytesAvailable),
		fmt.Sprintf("clickhouse_batch_count=%d", d.clickHouseBatchCount),
		fmt.Sprintf("clickhouse_estimated_uncompressed_bytes_sent=%d", d.clickHouseEstimatedUncompressedBytes),
		fmt.Sprintf("clickhouse_last_batch_estimated_uncompressed_bytes=%d", d.clickHouseLastBatchEstimatedBytes),
		"clickhouse_server_part_count=" +
			diagnosticOptionalUint64(d.clickHouseServerParts, d.clickHouseServerPartsAvailable),
		"clickhouse_server_active_merges=" +
			diagnosticOptionalUint64(d.clickHouseServerMerges, d.clickHouseServerMergesAvailable),
		"clickhouse_server_memory_bytes=" +
			diagnosticOptionalUint64(d.clickHouseServerMemoryBytes, d.clickHouseServerMemoryAvailable),
		"clickhouse_server_query_latency=" +
			diagnosticOptionalDuration(d.clickHouseServerQueryLatency, d.clickHouseServerQueryLatencyAvailable),
		fmt.Sprintf("clickhouse_server_pressure_backoff=%t", d.clickHouseServerPressureBackoff),
		"current_checkpoint=" + quoteForDiagnostics(d.currentCheckpoint),
	}
}

func quoteForDiagnostics(value string) string {
	return strconv.Quote(value)
}

func formatDepthHistogram(histogram []uint64) string {
	parts := make([]string, 0, len(histogram))
	for depth, count := range histogram {
		if count == 0 {
			continue
		}

		parts = append(parts, fmt.Sprintf("%d:%d", depth, count))
	}

	return strings.Join(parts, ",")
}

func formatHeavyPrefixes(prefixes []dirbuild.PrefixCount) string {
	parts := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		parts = append(parts, fmt.Sprintf("%s:%d", prefix.Prefix, prefix.Count))
	}

	return strings.Join(parts, ",")
}

func diagnosticOptionalUint64(value uint64, available bool) string {
	if !available {
		return unknownRSS
	}

	return strconv.FormatUint(value, 10)
}

func diagnosticOptionalDuration(value time.Duration, available bool) string {
	if !available {
		return unknownRSS
	}

	return formatDiagnosticDuration(value)
}

func (d *summariseDiagnostics) logParseResult(records uint64, err error) {
	if d == nil {
		return
	}

	d.setRecordCount(records)

	if err != nil {
		d.setCurrentPhase("parse_failed")
		d.logState("summarise parse failure", "err="+quoteForDiagnostics(err.Error()))

		return
	}

	d.setCurrentPhase("parse_complete")
	d.logState("summarise parse complete")
}

func (d *summariseDiagnostics) recordProgress(
	records uint64,
	elapsed time.Duration,
) (string, string, []string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastRecords = records
	d.inputRows = records
	d.rowsPerSecond = diagnosticRowsPerSecond(records, elapsed)
	d.phaseElapsed = elapsed

	return d.currentPhase, d.recentPhaseSummaryLocked(), d.telemetryFieldsLocked()
}

func (d *summariseDiagnostics) setRecordCount(records uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastRecords = records
}

func (d *summariseDiagnostics) logCloseStart(publish bool) {
	if d == nil {
		return
	}

	d.setCurrentPhase("clickhouse_close")
	d.logState("summarise close start", fmt.Sprintf("publish=%t", publish))
}

func (d *summariseDiagnostics) logCloseResult(publish bool, err error) {
	if d == nil {
		return
	}

	if err != nil {
		d.setCurrentPhase("clickhouse_close_failed")
		d.logState(
			"summarise close failure",
			fmt.Sprintf("publish=%t", publish),
			"err="+quoteForDiagnostics(err.Error()),
		)

		return
	}

	d.setCurrentPhase("clickhouse_close_complete")
	d.logState("summarise close complete", fmt.Sprintf("publish=%t", publish))
}

func (d *summariseDiagnostics) logCompletionMarkerResult(err error) {
	if d == nil {
		return
	}

	if err != nil {
		d.setCurrentPhase("completion_marker_failed")
		d.logState("summarise completion marker failure", "err="+quoteForDiagnostics(err.Error()))

		return
	}

	d.setCurrentPhase("complete")
	d.logState("summarise complete")
}

func (d *summariseDiagnostics) logFailure(err error) {
	if d == nil || err == nil {
		return
	}

	d.logState("summarise failed", "err="+quoteForDiagnostics(err.Error()))
}

func (d *summariseDiagnostics) logSignal(sig os.Signal) {
	if d == nil {
		return
	}

	d.logState("summarise signal", "signal="+summariseSignalName(sig))
}

func (d *summariseDiagnostics) logState(message string, extra ...string) {
	fields := d.stateFields()
	fields = append(fields, extra...)

	info("%s %s", message, strings.Join(fields, " "))
}

func (d *summariseDiagnostics) baseFields() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	chFields := d.clickHouseFieldsLocked()
	fields := make([]string, 0, summariseBaseFieldCount+len(chFields))
	fields = append(fields,
		fmt.Sprintf("pid=%d", os.Getpid()),
		"input="+quoteForDiagnostics(d.input),
		"output="+quoteForDiagnostics(d.outputDir),
		"current_phase="+d.currentPhase,
	)

	fields = append(fields, chFields...)

	return fields
}

func (d *summariseDiagnostics) stateFields() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	mem := memorySnapshot()
	chFields := d.clickHouseFieldsLocked()
	fields := make([]string, 0, summariseStateFieldCount+len(chFields))
	fields = append(fields,
		fmt.Sprintf("pid=%d", os.Getpid()),
		"input="+quoteForDiagnostics(d.input),
		"output="+quoteForDiagnostics(d.outputDir),
		fmt.Sprintf("records=%d", d.lastRecords),
		"elapsed="+d.elapsed().Round(time.Second).String(),
		"current_phase="+d.currentPhase,
		fmt.Sprintf("heap_alloc_mb=%d", mem.heapAllocMiB),
		fmt.Sprintf("heap_sys_mb=%d", mem.heapSysMiB),
		"rss_mb="+mem.rssMiB,
		fmt.Sprintf("goroutines=%d", mem.goroutines),
		fmt.Sprintf("gc_count=%d", mem.gcCount),
		"recent_import_phases="+quoteForDiagnostics(d.recentPhaseSummaryLocked()),
		"total_import_phases="+quoteForDiagnostics(d.totalPhaseSummaryLocked()),
	)
	fields = append(fields, d.telemetryFieldsLocked()...)

	fields = append(fields, chFields...)

	return fields
}

func (d *summariseDiagnostics) elapsed() time.Duration {
	return time.Since(d.started)
}

func (d *summariseDiagnostics) clickHouseFieldsLocked() []string {
	if d.target == nil {
		return nil
	}

	return []string{
		"mount_path=" + quoteForDiagnostics(d.target.mountPath),
		"mountpoints=" + quoteForDiagnostics(d.target.mountpointsPath),
		"snapshot_id=" + clickhouse.SnapshotID(d.target.mountPath, d.target.modtime),
		"updated_at=" + quoteForDiagnostics(d.target.modtime.UTC().Format(time.RFC3339Nano)),
		"clickhouse_database=" + quoteForDiagnostics(d.target.cfg.Database),
		"clickhouse_dsn=" + quoteForDiagnostics(redactClickHouseDSN(d.target.cfg.DSN)),
		"query_timeout=" + d.target.cfg.QueryTimeout.String(),
	}
}

func redactClickHouseDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "redacted_invalid_dsn"
	}

	redactUserinfoPassword(u)
	redactQuerySecrets(u)

	return u.String()
}

func (d *summariseDiagnostics) recentPhaseSummaryLocked() string {
	if len(d.recentPhases) == 0 {
		return ""
	}

	parts := make([]string, 0, len(d.recentPhases))
	for _, recent := range d.recentPhases {
		parts = append(parts, recent.phase+":"+formatDiagnosticDuration(recent.duration))
	}

	return strings.Join(parts, ",")
}

func formatDiagnosticDuration(duration time.Duration) string {
	if duration >= time.Second {
		return duration.Round(time.Second).String()
	}

	if duration >= time.Millisecond {
		return duration.Round(time.Millisecond).String()
	}

	return duration.String()
}

func (d *summariseDiagnostics) totalPhaseSummaryLocked() string {
	if len(d.phaseTotals) == 0 {
		return ""
	}

	keys := make([]string, 0, len(d.phaseTotals))
	for phase := range d.phaseTotals {
		keys = append(keys, phase)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, phase := range keys {
		parts = append(parts, phase+":"+formatDiagnosticDuration(d.phaseTotals[phase]))
	}

	return strings.Join(parts, ",")
}

type summariseMemorySnapshot struct {
	heapAllocMiB uint64
	heapSysMiB   uint64
	rssMiB       string
	goroutines   int
	gcCount      uint32
}

type summariseImportPhaseRecorder interface {
	SetImportPhaseRecorder(recorder func(phase string, duration time.Duration))
}

func setSummariseImportPhaseRecorder(recorder func(string, time.Duration), targets ...any) {
	if recorder == nil {
		return
	}

	for _, target := range targets {
		setter, ok := target.(summariseImportPhaseRecorder)
		if !ok {
			continue
		}

		setter.SetImportPhaseRecorder(recorder)
	}
}

type summariseParseCounter struct {
	records uint64
}

func addSummariseParseCounter(s *summary.Summariser) *summariseParseCounter {
	counter := &summariseParseCounter{}
	if s == nil {
		return counter
	}

	s.AddGlobalOperation(func() summary.Operation {
		return counter
	})

	return counter
}

func (c *summariseParseCounter) Add(*summary.FileInfo) error {
	c.records++

	return nil
}

func (c *summariseParseCounter) Output() error {
	return nil
}

func (c *summariseParseCounter) Count() uint64 {
	if c == nil {
		return 0
	}

	return c.records
}

func processRSSMiB() (uint64, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}

	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}

	return rssMiBFromProcStatm(data, os.Getpagesize())
}

func rssMiBFromProcStatm(data []byte, pageSize int) (uint64, bool) {
	fields := bytes.Fields(data)
	if len(fields) < 2 || pageSize <= 0 {
		return 0, false
	}

	pages, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0, false
	}

	return bytesToMiB(pages * uint64(pageSize)), true
}

func redactUserinfoPassword(u *url.URL) {
	if u == nil || u.User == nil {
		return
	}

	username := u.User.Username()
	if _, hasPassword := u.User.Password(); hasPassword {
		u.User = url.UserPassword(username, redactedSecret)

		return
	}

	u.User = url.User(username)
}

func redactQuerySecrets(u *url.URL) {
	if u == nil {
		return
	}

	q := u.Query()
	for key, values := range q {
		if !diagnosticQueryKeyIsSecret(key) {
			continue
		}

		for i := range values {
			values[i] = redactedSecret
		}

		q[key] = values
	}

	u.RawQuery = q.Encode()
}

func diagnosticQueryKeyIsSecret(key string) bool {
	lower := strings.ToLower(key)

	return strings.Contains(lower, "password") ||
		strings.Contains(lower, "pass") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "key")
}
