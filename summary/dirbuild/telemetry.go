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

package dirbuild

import (
	"slices"
	"time"
)

const (
	// TelemetryDepthBins bounds the exact depth histogram; the last bin is overflow.
	TelemetryDepthBins = 65
	// TelemetryHeavyPrefixCapacity bounds the approximate heavy-prefix report.
	TelemetryHeavyPrefixCapacity = 16
	telemetryProgressEvery       = 100_000

	// PhaseDirectoryScan is pass 1 directory topology construction.
	PhaseDirectoryScan = "directory_scan_index_construction"
	// PhaseIDAssignment finalises deterministic IDs and the searchable index.
	PhaseIDAssignment = "id_assignment_index_finalisation"
	// PhasePass2Aggregation is pass 2 fact aggregation.
	PhasePass2Aggregation = "pass2_fact_aggregation" //nolint:gosec // operational phase name, not a credential.
	// PhaseSQLiteRollupSpoolEmission rolls SQLite summaries up while emitting the spool.
	PhaseSQLiteRollupSpoolEmission = "sqlite_rollup_spool_emission"
	// PhaseSpoolEmission rolls memory summaries up while emitting the spool.
	PhaseSpoolEmission = "spool_emission"
)

// PrefixCount is one bounded heavy-prefix estimate.
type PrefixCount struct {
	Prefix string
	Count  uint64
}

// Telemetry is a bounded live snapshot of dirbuild work and input shape.
type Telemetry struct {
	Phase              string
	InputRows          uint64
	DirectoryNodes     uint64
	ImpliedDirectories uint64
	MaximumDepth       uint64
	DepthHistogram     []uint64
	HeavyPrefixes      []PrefixCount
	SQLiteBytes        uint64
	PhaseRows          uint64
	PhaseElapsed       time.Duration
}

type buildTelemetry struct {
	record   func(Telemetry)
	metrics  *DiskMetrics
	snapshot Telemetry
	started  time.Time
}

func newBuildTelemetry(opts Options) *buildTelemetry {
	if opts.Progress == nil {
		return nil
	}

	return &buildTelemetry{record: opts.Progress, metrics: opts.DiskMetrics}
}

func (t *buildTelemetry) begin(phase string) {
	if t == nil || t.record == nil {
		return
	}

	t.snapshot.Phase = phase
	t.snapshot.PhaseRows = 0
	t.snapshot.PhaseElapsed = 0
	t.started = time.Now()
	t.emit()
}

func (t *buildTelemetry) progress(rows uint64) {
	if t == nil || t.record == nil {
		return
	}

	t.snapshot.PhaseRows = rows
	if rows%telemetryProgressEvery != 0 {
		return
	}

	t.snapshot.PhaseElapsed = time.Since(t.started)
	t.emit()
}

func (t *buildTelemetry) progressShape(rows uint64, paths *pathBuilder) {
	if t == nil || t.record == nil {
		return
	}

	t.snapshot.InputRows = rows

	t.snapshot.PhaseRows = rows
	if rows%telemetryProgressEvery != 0 {
		return
	}

	t.setShape(paths)
	t.snapshot.PhaseElapsed = time.Since(t.started)
	t.emit()
}

func (t *buildTelemetry) finish(rows uint64) {
	if t == nil || t.record == nil {
		return
	}

	t.snapshot.PhaseRows = rows
	t.snapshot.PhaseElapsed = time.Since(t.started)
	t.emit()
}

func (t *buildTelemetry) setInputRows(rows uint64) {
	if t != nil && t.record != nil {
		t.snapshot.InputRows = rows
	}
}

func (t *buildTelemetry) inputRows() uint64 {
	if t == nil || t.record == nil {
		return 0
	}

	return t.snapshot.InputRows
}

func (t *buildTelemetry) setShape(paths *pathBuilder) {
	if t == nil || t.record == nil || paths == nil || paths.shape == nil {
		return
	}

	// paths.count is a non-negative in-memory node count.
	t.snapshot.DirectoryNodes = uint64(paths.count) //nolint:gosec
	// rawCount cannot exceed the non-negative in-memory node count.
	t.snapshot.ImpliedDirectories = uint64(paths.count - paths.shape.rawCount) //nolint:gosec
	t.snapshot.MaximumDepth = paths.shape.maximumDepth
	t.snapshot.DepthHistogram = paths.shape.depthHistogram[:]
	t.snapshot.HeavyPrefixes = paths.shape.heavyPrefixes.snapshot()
}

func (t *buildTelemetry) emit() {
	if t == nil || t.record == nil {
		return
	}

	if t.metrics != nil {
		if t.metrics.readDatabaseBytes != nil {
			t.metrics.DatabaseBytes = t.metrics.readDatabaseBytes()
		}

		t.snapshot.SQLiteBytes = t.metrics.DatabaseBytes
	}

	snapshot := t.snapshot
	snapshot.DepthHistogram = slices.Clone(snapshot.DepthHistogram)
	snapshot.HeavyPrefixes = slices.Clone(snapshot.HeavyPrefixes)
	t.record(snapshot)
}

type heavyPrefixCounter struct {
	entries [TelemetryHeavyPrefixCapacity]PrefixCount
	length  int
}

func (h *heavyPrefixCounter) add(prefix string) {
	if prefix == "" {
		return
	}

	for idx := range h.length {
		if h.entries[idx].Prefix == prefix {
			h.entries[idx].Count++

			return
		}
	}

	if h.length < TelemetryHeavyPrefixCapacity {
		h.entries[h.length] = PrefixCount{Prefix: prefix, Count: 1}
		h.length++

		return
	}

	h.reduce()
}

func (h *heavyPrefixCounter) reduce() {
	kept := 0

	for idx := range h.length {
		entry := h.entries[idx]

		entry.Count--

		if entry.Count > 0 {
			h.entries[kept] = entry
			kept++
		}
	}

	clear(h.entries[kept:h.length])
	h.length = kept
}

func (h *heavyPrefixCounter) snapshot() []PrefixCount {
	result := slices.Clone(h.entries[:h.length])

	slices.SortFunc(result, func(a, b PrefixCount) int {
		if a.Count != b.Count {
			return -compareUint64(a.Count, b.Count)
		}

		return compareStrings(a.Prefix, b.Prefix)
	})

	return result
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
