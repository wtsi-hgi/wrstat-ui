package boltperf

import (
	"encoding/json"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"time"
)

// SchemaVersion is the current JSON report schema version.
const SchemaVersion = 1

// Operation represents a single measured operation in a perf report.
type Operation struct {
	Name        string         `json:"name"`
	Inputs      map[string]any `json:"inputs"`
	DurationsMS []float64      `json:"durations_ms"`
	P50MS       float64        `json:"p50_ms"`
	P95MS       float64        `json:"p95_ms"`
	P99MS       float64        `json:"p99_ms"`
}

// Report is the top-level JSON report written by the perf harness.
type Report struct {
	SchemaVersion int         `json:"schema_version"`
	Backend       string      `json:"backend"`
	GitCommit     string      `json:"git_commit"`
	GoVersion     string      `json:"go_version"`
	OS            string      `json:"os"`
	Arch          string      `json:"arch"`
	StartedAt     string      `json:"started_at"`
	InputDir      string      `json:"input_dir"`
	Repeat        int         `json:"repeat"`
	Warmup        int         `json:"warmup"`
	Operations    []Operation `json:"operations"`
}

// NewReport constructs a new report with build and environment metadata.
func NewReport(backend, inputDir string, repeat, warmup int) Report {
	return Report{
		SchemaVersion: SchemaVersion,
		Backend:       backend,
		GitCommit:     gitCommitFromBuildInfo(),
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
	p50, p95, p99 := PercentilesMS(durationsMS)

	r.Operations = append(r.Operations, Operation{
		Name:        name,
		Inputs:      inputs,
		DurationsMS: durationsMS,
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
