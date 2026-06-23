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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	// A5BuildOperationName is the operation emitted by summarise build reports.
	A5BuildOperationName = "summarise_build_total"

	// A5BuildScratchBytesLimit is the hard cap for non-contiguous build scratch.
	A5BuildScratchBytesLimit uint64 = 100 * 1024 * 1024

	A5BuildRoleScratch127    = "scratch127"
	A5BuildRoleT283          = "t283"
	A5BuildRoleHealthyLustre = "healthy_lustre"

	A5BuildInputContiguous    = "contiguous"
	A5BuildInputNonContiguous = "non_contiguous"
	A5BuildInputLargeUnprobed = "large_unprobed"

	A5BuildPathContiguousFast = "contiguous_fast_path"
	A5BuildPathDirbuild       = "dirbuild"

	A5BuildInputRole              = "a5_role"
	A5BuildInputShape             = "input_shape"
	A5BuildInputPath              = "build_path"
	A5BuildInputCompleted         = "completed"
	A5BuildInputRowCap            = "row_cap"
	A5BuildInputBuildScratchBytes = "build_phase_bytes_written"
	A5BuildInputSpoolBytes        = "spool_bytes"

	a5BuildExpectedRowCap        uint64  = 1_500_000
	a5HealthyTolerance           float64 = 0.10
	a5NonContiguousWallTolerance float64 = 1.50
)

var errA5BuildOperationMissing = errors.New("A5 build operation missing")

// A5BuildGateOptions points at persisted summarise_build_report.json artefacts.
type A5BuildGateOptions struct {
	Scratch127ReportPath string `json:"scratch127_report_path"`
	T283ReportPath       string `json:"t283_report_path"`
	HealthyBeforePath    string `json:"healthy_before_report_path"`
	HealthyAfterPath     string `json:"healthy_after_report_path"`
}

// A5BuildGateMetrics captures the numeric values used by the gate.
type A5BuildGateMetrics struct {
	Scratch127BuildScratchBytes uint64  `json:"scratch127_build_scratch_bytes"`
	T283BuildScratchBytes       uint64  `json:"t283_build_scratch_bytes"`
	Scratch127WallMS            float64 `json:"scratch127_wall_ms"`
	T283WallMS                  float64 `json:"t283_wall_ms"`
	HealthyBeforeWallMS         float64 `json:"healthy_before_wall_ms"`
	HealthyAfterWallMS          float64 `json:"healthy_after_wall_ms"`
	Scratch127WallRatio         float64 `json:"scratch127_wall_ratio"`
	T283WallRatio               float64 `json:"t283_wall_ratio"`
	HealthyWallChangeRatio      float64 `json:"healthy_wall_change_ratio"`
	HealthyRSSChangeRatio       float64 `json:"healthy_rss_change_ratio"`
	HealthySpoolBytesRatio      float64 `json:"healthy_spool_bytes_ratio"`
}

// A5BuildGateResult is the machine-readable A5 build-gate outcome.
type A5BuildGateResult struct {
	Passed  bool               `json:"passed"`
	Checks  []FinalGateCheck   `json:"checks"`
	Metrics A5BuildGateMetrics `json:"metrics"`
}

// ValidateA5BuildPerfGate validates Phase 3/A5 build performance evidence.
func ValidateA5BuildPerfGate(opts A5BuildGateOptions) (A5BuildGateResult, error) {
	scratch127, err := readA5BuildSummary(opts.Scratch127ReportPath)
	if err != nil {
		return A5BuildGateResult{}, err
	}

	t283, err := readA5BuildSummary(opts.T283ReportPath)
	if err != nil {
		return A5BuildGateResult{}, err
	}

	healthyBefore, err := readA5BuildSummary(opts.HealthyBeforePath)
	if err != nil {
		return A5BuildGateResult{}, err
	}

	healthyAfter, err := readA5BuildSummary(opts.HealthyAfterPath)
	if err != nil {
		return A5BuildGateResult{}, err
	}

	healthyWallBaseline := a5NonContiguousWallBaseline(healthyBefore, healthyAfter)
	result := A5BuildGateResult{
		Passed: true,
		Metrics: A5BuildGateMetrics{
			Scratch127BuildScratchBytes: scratch127.buildScratchBytes,
			T283BuildScratchBytes:       t283.buildScratchBytes,
			Scratch127WallMS:            scratch127.wallMS,
			T283WallMS:                  t283.wallMS,
			HealthyBeforeWallMS:         healthyBefore.wallMS,
			HealthyAfterWallMS:          healthyAfter.wallMS,
			Scratch127WallRatio:         a5Ratio(scratch127.wallMS, healthyWallBaseline.wallMS),
			T283WallRatio:               a5Ratio(t283.wallMS, healthyWallBaseline.wallMS),
			HealthyWallChangeRatio:      a5Ratio(healthyAfter.wallMS, healthyBefore.wallMS),
			HealthyRSSChangeRatio:       a5Ratio(float64(healthyAfter.maxRSSBytes), float64(healthyBefore.maxRSSBytes)),
			HealthySpoolBytesRatio:      a5Ratio(float64(healthyAfter.spoolBytes), float64(healthyBefore.spoolBytes)),
		},
	}

	result.Checks = []FinalGateCheck{
		validateA5NonContiguousReport(1, "A5 scratch127 build scratch", scratch127, A5BuildRoleScratch127),
		validateA5NonContiguousReport(2, "A5 t283 build scratch bytes-only", t283, A5BuildRoleT283),
		validateA5NonContiguousWall(scratch127, healthyWallBaseline),
		validateA5HealthyComparison(healthyBefore, healthyAfter),
	}

	for _, check := range result.Checks {
		result.Passed = result.Passed && check.Passed
	}

	return result, nil
}

// WriteA5BuildGateResult writes an A5 gate result as pretty-printed JSON.
func WriteA5BuildGateResult(path string, result A5BuildGateResult) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	enc := json.NewEncoder(fh)
	enc.SetIndent("", "  ")

	return enc.Encode(result)
}

type a5BuildSummary struct {
	path              string
	role              string
	inputShape        string
	buildPath         string
	completed         bool
	rowCap            uint64
	buildScratchBytes uint64
	spoolBytes        uint64
	maxRSSBytes       uint64
	wallMS            float64
}

func readA5BuildSummary(path string) (a5BuildSummary, error) {
	report, err := finalGateReadPerfReport(path)
	if err != nil {
		return a5BuildSummary{}, err
	}

	op, ok := a5BuildOperation(report)
	if !ok {
		return a5BuildSummary{}, fmt.Errorf("%w: %s missing %s", errA5BuildOperationMissing, path, A5BuildOperationName)
	}

	return a5BuildSummary{
		path:              path,
		role:              stringInput(op.Inputs, A5BuildInputRole),
		inputShape:        stringInput(op.Inputs, A5BuildInputShape),
		buildPath:         stringInput(op.Inputs, A5BuildInputPath),
		completed:         boolInput(op.Inputs, A5BuildInputCompleted),
		rowCap:            uint64Input(op.Inputs, A5BuildInputRowCap),
		buildScratchBytes: uint64Input(op.Inputs, A5BuildInputBuildScratchBytes),
		spoolBytes:        uint64Input(op.Inputs, A5BuildInputSpoolBytes),
		maxRSSBytes:       report.MaxRSSBytes,
		wallMS:            op.P95MS,
	}, nil
}

func a5NonContiguousWallBaseline(before, after a5BuildSummary) a5BuildSummary {
	contiguousFast := a5BuildRoute{
		inputShape: A5BuildInputContiguous,
		buildPath:  A5BuildPathContiguousFast,
	}
	if a5RouteMatches(after, contiguousFast) {
		return after
	}

	return before
}

func validateA5NonContiguousReport(
	id int,
	name string,
	summary a5BuildSummary,
	role string,
) FinalGateCheck {
	check := finalGateCheck(id, name)
	if reason := a5CommonReportFailure(summary, role, a5KnownNonContiguousRoutes()...); reason != "" {
		return check.fail(reason)
	}

	if summary.buildScratchBytes >= A5BuildScratchBytesLimit {
		return check.fail(fmt.Sprintf("%s build scratch %d bytes >= %d bytes",
			role, summary.buildScratchBytes, A5BuildScratchBytesLimit))
	}

	detail := fmt.Sprintf("%s completed via %s with %d build scratch bytes",
		role, a5RouteDetail(summary), summary.buildScratchBytes)
	if role == A5BuildRoleT283 {
		detail += "; t283 wall is recorded as bytes-only evidence and not gated"
	}

	return check.pass(detail)
}

func a5KnownNonContiguousRoutes() []a5BuildRoute {
	return []a5BuildRoute{
		{inputShape: A5BuildInputNonContiguous, buildPath: A5BuildPathDirbuild},
		{inputShape: A5BuildInputLargeUnprobed, buildPath: A5BuildPathDirbuild},
	}
}

func validateA5NonContiguousWall(scratch127, healthyBaseline a5BuildSummary) FinalGateCheck {
	check := finalGateCheck(3, "A5 scratch127 non-contiguous wall")
	if reason := a5CommonReportFailure(scratch127, A5BuildRoleScratch127,
		a5KnownNonContiguousRoutes()...); reason != "" {
		return check.fail(reason)
	}

	if reason := a5CommonReportFailure(healthyBaseline, A5BuildRoleHealthyLustre,
		a5HealthyBaselineRoutes()...); reason != "" {
		return check.fail(reason)
	}

	limit := healthyBaseline.wallMS * a5NonContiguousWallTolerance
	if scratch127.wallMS > limit {
		return check.fail(fmt.Sprintf("scratch127 wall %.3fms exceeds 1.5x contiguous fast path %.3fms",
			scratch127.wallMS, healthyBaseline.wallMS))
	}

	return check.pass(fmt.Sprintf("scratch127 %s build wall is within 1.5x healthy %s",
		a5RouteDetail(scratch127), a5RouteDetail(healthyBaseline)))
}

func a5HealthyBaselineRoutes() []a5BuildRoute {
	return []a5BuildRoute{
		{inputShape: A5BuildInputContiguous, buildPath: A5BuildPathContiguousFast},
	}
}

func validateA5HealthyComparison(before, after a5BuildSummary) FinalGateCheck {
	check := finalGateCheck(4, "A5 healthy build before-after")

	if reason := a5CommonReportFailure(before, A5BuildRoleHealthyLustre,
		a5HealthyBaselineRoutes()...); reason != "" {
		return check.fail("before " + reason)
	}

	if reason := a5CommonReportFailure(after, A5BuildRoleHealthyLustre,
		a5HealthyCurrentRoutes()...); reason != "" {
		return check.fail("after " + reason)
	}

	failures := a5HealthyComparisonFailures(before, after)
	if len(failures) > 0 {
		return check.fail(strings.Join(failures, "; "))
	}

	return check.pass(fmt.Sprintf("healthy current %s wall, RSS, and spool bytes are within +/-10%%",
		a5RouteDetail(after)))
}

func a5HealthyCurrentRoutes() []a5BuildRoute {
	return []a5BuildRoute{
		{inputShape: A5BuildInputContiguous, buildPath: A5BuildPathContiguousFast},
		{inputShape: A5BuildInputLargeUnprobed, buildPath: A5BuildPathDirbuild},
	}
}

func a5CommonReportFailure(summary a5BuildSummary, role string, routes ...a5BuildRoute) string {
	if reason := a5CompletionFailure(summary); reason != "" {
		return reason
	}

	if reason := a5IdentityFailure(summary, role); reason != "" {
		return reason
	}

	if reason := a5RouteFailure(summary, routes...); reason != "" {
		return reason
	}

	return a5MeasurementFailure(summary)
}

func a5CompletionFailure(summary a5BuildSummary) string {
	if !summary.completed {
		return "build did not complete"
	}

	return ""
}

func a5IdentityFailure(summary a5BuildSummary, role string) string {
	if summary.role != role {
		return fmt.Sprintf("role %q != %q", summary.role, role)
	}

	if summary.rowCap != a5BuildExpectedRowCap {
		return fmt.Sprintf("row_cap %d != %d", summary.rowCap, a5BuildExpectedRowCap)
	}

	return ""
}

func a5RouteFailure(summary a5BuildSummary, routes ...a5BuildRoute) string {
	for _, route := range routes {
		if a5RouteMatches(summary, route) {
			return ""
		}
	}

	if len(routes) == 1 {
		return a5SingleRouteFailure(summary, routes[0])
	}

	return fmt.Sprintf("input_shape/build_path %q not in %s",
		a5RouteDetail(summary), a5RouteList(routes))
}

func a5RouteList(routes []a5BuildRoute) string {
	labels := make([]string, 0, len(routes))
	for _, route := range routes {
		labels = append(labels, route.inputShape+"/"+route.buildPath)
	}

	return strings.Join(labels, ", ")
}

func a5RouteMatches(summary a5BuildSummary, route a5BuildRoute) bool {
	return summary.inputShape == route.inputShape && summary.buildPath == route.buildPath
}

func a5SingleRouteFailure(summary a5BuildSummary, route a5BuildRoute) string {
	if summary.inputShape != route.inputShape {
		return fmt.Sprintf("input_shape %q != %q", summary.inputShape, route.inputShape)
	}

	if summary.buildPath != route.buildPath {
		return fmt.Sprintf("build_path %q != %q", summary.buildPath, route.buildPath)
	}

	return fmt.Sprintf("input_shape/build_path %q != %s/%s",
		a5RouteDetail(summary), route.inputShape, route.buildPath)
}

func a5MeasurementFailure(summary a5BuildSummary) string {
	if summary.wallMS <= 0 || summary.maxRSSBytes == 0 || summary.spoolBytes == 0 {
		return "missing wall, MaxRSSBytes, or spool bytes"
	}

	return ""
}

func a5HealthyComparisonFailures(before, after a5BuildSummary) []string {
	checks := []struct {
		name   string
		before float64
		after  float64
	}{
		{name: "wall", before: before.wallMS, after: after.wallMS},
		{name: "MaxRSSBytes", before: float64(before.maxRSSBytes), after: float64(after.maxRSSBytes)},
		{name: "spool bytes", before: float64(before.spoolBytes), after: float64(after.spoolBytes)},
	}

	failures := make([]string, 0)

	for _, check := range checks {
		if !a5WithinTolerance(check.before, check.after, a5HealthyTolerance) {
			failures = append(failures, fmt.Sprintf("%s ratio %.4f outside +/-10%%",
				check.name, a5Ratio(check.after, check.before)))
		}
	}

	return failures
}

func a5WithinTolerance(before, after, tolerance float64) bool {
	if before <= 0 {
		return false
	}

	ratio := after / before

	return ratio >= 1-tolerance && ratio <= 1+tolerance
}

func a5Ratio(after, before float64) float64 {
	if before == 0 {
		return 0
	}

	return after / before
}

func a5RouteDetail(summary a5BuildSummary) string {
	return summary.inputShape + "/" + summary.buildPath
}

type a5BuildRoute struct {
	inputShape string
	buildPath  string
}

func a5BuildOperation(report perfreport.Report) (perfreport.Operation, bool) {
	for _, op := range report.Operations {
		if op.Name == A5BuildOperationName {
			return op, true
		}
	}

	return perfreport.Operation{}, false
}
