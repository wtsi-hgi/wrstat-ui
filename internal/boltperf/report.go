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

package boltperf

import "github.com/wtsi-hgi/wrstat-ui/internal/perfreport"

// SchemaVersion is the current JSON report schema version.
const SchemaVersion = perfreport.SchemaVersion

// Operation represents a single measured operation in a perf report.
type Operation = perfreport.Operation

// Report is the top-level JSON report written by the perf harness.
type Report = perfreport.Report

// NewReport constructs a new report with build and environment metadata.
func NewReport(backend, inputDir string, repeat, warmup int) Report {
	return perfreport.NewReport(backend, inputDir, repeat, warmup)
}

// PercentilesMS returns the p50, p95, and p99 percentiles of values.
func PercentilesMS(values []float64) (float64, float64, float64) {
	return perfreport.PercentilesMS(values)
}

// WriteReport writes report as pretty-printed JSON to the given path.
func WriteReport(path string, report Report) error {
	return perfreport.WriteReport(path, report)
}
