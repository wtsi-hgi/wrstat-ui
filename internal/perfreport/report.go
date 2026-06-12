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

package perfreport

import (
	"encoding/json"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"
)

// SchemaVersion is the current JSON report schema version.
const SchemaVersion = 1

// PrintfFunc matches fmt.Printf-style output and is used by perf harnesses.
type PrintfFunc func(string, ...any)

// Operation represents a single measured operation in a perf report.
type Operation struct {
	Name        string         `json:"name"`
	Inputs      map[string]any `json:"inputs"`
	DurationsMS []float64      `json:"durations_ms"`
	ReadRows    []uint64       `json:"read_rows,omitempty"`
	ReadBytes   []uint64       `json:"read_bytes,omitempty"`
	ReadMarks   []uint64       `json:"read_marks,omitempty"`
	MemoryBytes []uint64       `json:"memory_bytes,omitempty"`
	ResultBytes []uint64       `json:"result_bytes,omitempty"`
	ResultCount []uint64       `json:"result_counts,omitempty"`
	P50MS       float64        `json:"p50_ms"`
	P95MS       float64        `json:"p95_ms"`
	P99MS       float64        `json:"p99_ms"`
}

// TableStats captures physical ClickHouse table size evidence.
type TableStats struct {
	Rows                       uint64             `json:"rows"`
	ActiveParts                uint64             `json:"active_parts"`
	CompressedBytes            uint64             `json:"compressed_bytes"`
	UncompressedBytes          uint64             `json:"uncompressed_bytes"`
	ImportMemoryBytes          uint64             `json:"import_memory_bytes,omitempty"`
	RowAmplificationVsDirFacts float64            `json:"row_amplification_vs_wrstat_dir_facts,omitempty"`
	RowAmplificationVsChildren float64            `json:"row_amplification_vs_wrstat_children,omitempty"`
	ImportPhaseDurationsMS     map[string]float64 `json:"import_phase_durations_ms,omitempty"`
}

// FactsVectorStats captures wrstat_dir_facts vector-density evidence.
type FactsVectorStats struct {
	Rows                 uint64  `json:"rows"`
	TotalEntries         uint64  `json:"total_entries"`
	AverageEntriesPerDir float64 `json:"average_entries_per_dir"`
	MaxEntriesPerDir     uint64  `json:"max_entries_per_dir"`
}

// FactsBucketStats captures wrstat_dir_facts bucket-shape evidence.
type FactsBucketStats struct {
	Rows                 uint64 `json:"rows"`
	NonEmptyRows         uint64 `json:"non_empty_rows"`
	MaxBuckets           uint64 `json:"max_buckets"`
	MismatchedBucketRows uint64 `json:"mismatched_bucket_rows"`
}

// Report is the top-level JSON report written by perf harnesses.
type Report struct {
	SchemaVersion    int                   `json:"schema_version"`
	Backend          string                `json:"backend"`
	GitCommit        string                `json:"git_commit"`
	ToolVersion      string                `json:"tool_version"`
	GoVersion        string                `json:"go_version"`
	OS               string                `json:"os"`
	Arch             string                `json:"arch"`
	StartedAt        string                `json:"started_at"`
	InputDir         string                `json:"input_dir"`
	Repeat           int                   `json:"repeat"`
	Warmup           int                   `json:"warmup"`
	SelectedTables   []string              `json:"selected_tables,omitempty"`
	TableStats       map[string]TableStats `json:"table_stats,omitempty"`
	FactsVectorStats *FactsVectorStats     `json:"facts_vector_stats,omitempty"`
	FactsBucketStats *FactsBucketStats     `json:"facts_bucket_stats,omitempty"`
	MaxRSSBytes      uint64                `json:"max_rss_bytes,omitempty"`
	Operations       []Operation           `json:"operations"`
}

// NewReport constructs a new report with build and environment metadata.
func NewReport(backend, inputDir string, repeat, warmup int) Report {
	return Report{
		SchemaVersion: SchemaVersion,
		Backend:       backend,
		GitCommit:     gitCommitFromBuildInfo(),
		ToolVersion:   toolVersionFromBuildInfo(),
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		StartedAt:     time.Now().UTC().Format(time.RFC3339),
		InputDir:      inputDir,
		Repeat:        repeat,
		Warmup:        warmup,
		Operations:    make([]Operation, 0),
	}
}

// AddOperation appends a measured operation and computes p50/p95/p99 from
// the provided durations.
func (r *Report) AddOperation(name string, inputs map[string]any, durationsMS []float64) {
	r.AddOperationWithCounters(name, inputs, durationsMS, nil, nil, nil, nil)
}

// AddOperationWithCounters appends a measured operation with per-repeat
// storage counters and computes p50/p95/p99 from the provided durations.
func (r *Report) AddOperationWithCounters(
	name string,
	inputs map[string]any,
	durationsMS []float64,
	readRows []uint64,
	readBytes []uint64,
	readMarks []uint64,
	resultCounts []uint64,
) {
	r.AddOperationWithFullCounters(
		name,
		inputs,
		durationsMS,
		readRows,
		readBytes,
		readMarks,
		nil,
		nil,
		resultCounts,
	)
}

// AddOperationWithFullCounters appends a measured operation with per-repeat
// storage counters, optional memory counters, and computed percentiles.
func (r *Report) AddOperationWithFullCounters(
	name string,
	inputs map[string]any,
	durationsMS []float64,
	readRows []uint64,
	readBytes []uint64,
	readMarks []uint64,
	memoryBytes []uint64,
	resultBytes []uint64,
	resultCounts []uint64,
) {
	p50, p95, p99 := PercentilesMS(durationsMS)

	r.Operations = append(r.Operations, Operation{
		Name:        name,
		Inputs:      inputs,
		DurationsMS: durationsMS,
		ReadRows:    readRows,
		ReadBytes:   readBytes,
		ReadMarks:   readMarks,
		MemoryBytes: memoryBytes,
		ResultBytes: resultBytes,
		ResultCount: resultCounts,
		P50MS:       p50,
		P95MS:       p95,
		P99MS:       p99,
	})
}

// PercentilesMS returns the p50, p95, and p99 percentiles of values.
func PercentilesMS(values []float64) (float64, float64, float64) {
	return percentileMS(values, 0.50), percentileMS(values, 0.95), percentileMS(values, 0.99)
}

// WriteReport writes report as pretty-printed JSON to the given path.
func WriteReport(path string, report Report) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	enc := json.NewEncoder(fh)
	enc.SetIndent("", "  ")

	return enc.Encode(report)
}

func percentileMS(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := slices.Clone(values)
	slices.Sort(sorted)

	if p <= 0 {
		return sorted[0]
	}

	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}

	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

func toolVersionFromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || strings.TrimSpace(info.Main.Version) == "" {
		return "(devel)"
	}

	return info.Main.Version
}

func gitCommitFromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}

	return ""
}
