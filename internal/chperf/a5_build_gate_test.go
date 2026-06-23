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
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

type a5BuildGateFixtureOptions struct {
	scratch127BuildScratchBytes uint64
	scratch127WallMS            float64
	healthyAfterMS              float64
	healthyAfterSpoolBytes      uint64
}

func TestA5BuildPerfGates(t *testing.T) {
	Convey("A5 accepts non-contiguous scratch reports and a healthy contiguous before-after pair", t, func() {
		root := t.TempDir()
		opts := a5BuildGateFixture(root, a5BuildGateFixtureOptions{})

		result, err := ValidateA5BuildPerfGate(opts)

		So(err, ShouldBeNil)
		So(result.Passed, ShouldBeTrue)
		So(result.Checks, ShouldHaveLength, 4)

		for _, check := range result.Checks {
			So(check.Passed, ShouldBeTrue)
		}

		So(result.Metrics.Scratch127BuildScratchBytes, ShouldEqual, uint64(0))
		So(result.Metrics.T283BuildScratchBytes, ShouldEqual, uint64(0))
		So(result.Checks[1].Detail, ShouldContainSubstring, "bytes-only")
		So(result.Checks[2].Name, ShouldContainSubstring, "scratch127")
		So(result.Metrics.T283WallRatio, ShouldBeGreaterThan, 4.0)
		So(result.Metrics.HealthyWallChangeRatio, ShouldEqual, 1.05)
	})

	Convey("A5 rejects non-contiguous build scratch at the 100 MB cap", t, func() {
		root := t.TempDir()
		opts := a5BuildGateFixture(root, a5BuildGateFixtureOptions{
			scratch127BuildScratchBytes: A5BuildScratchBytesLimit,
		})

		result, err := ValidateA5BuildPerfGate(opts)

		So(err, ShouldBeNil)
		So(result.Passed, ShouldBeFalse)
		So(result.Checks[0].Passed, ShouldBeFalse)
		So(result.Checks[0].Detail, ShouldContainSubstring, "scratch127")
		So(result.Checks[0].Detail, ShouldContainSubstring, "build scratch")
	})

	Convey("A5 rejects scratch127 wall beyond 1.5x the contiguous fast path", t, func() {
		root := t.TempDir()
		opts := a5BuildGateFixture(root, a5BuildGateFixtureOptions{
			scratch127WallMS: 16_000,
			healthyAfterMS:   10_000,
		})

		result, err := ValidateA5BuildPerfGate(opts)

		So(err, ShouldBeNil)
		So(result.Passed, ShouldBeFalse)
		So(result.Checks[2].Passed, ShouldBeFalse)
		So(result.Checks[2].Detail, ShouldContainSubstring, "1.5x")
	})

	Convey("A5 rejects healthy contiguous metric changes beyond the 10 percent band", t, func() {
		root := t.TempDir()
		opts := a5BuildGateFixture(root, a5BuildGateFixtureOptions{
			healthyAfterSpoolBytes: 67_000_001,
		})

		result, err := ValidateA5BuildPerfGate(opts)

		So(err, ShouldBeNil)
		So(result.Passed, ShouldBeFalse)
		So(result.Checks[3].Passed, ShouldBeFalse)
		So(result.Checks[3].Detail, ShouldContainSubstring, "spool")
	})
}

func a5BuildGateFixture(root string, opts a5BuildGateFixtureOptions) A5BuildGateOptions {
	scratchBytes := opts.scratch127BuildScratchBytes

	scratchWallMS := opts.scratch127WallMS
	if scratchWallMS == 0 {
		scratchWallMS = 12_000
	}

	healthyAfterMS := opts.healthyAfterMS
	if healthyAfterMS == 0 {
		healthyAfterMS = 10_500
	}

	healthyAfterSpool := opts.healthyAfterSpoolBytes
	if healthyAfterSpool == 0 {
		healthyAfterSpool = 62_000_000
	}

	paths := A5BuildGateOptions{
		Scratch127ReportPath: filepath.Join(root, "scratch127.json"),
		T283ReportPath:       filepath.Join(root, "t283.json"),
		HealthyBeforePath:    filepath.Join(root, "healthy-before.json"),
		HealthyAfterPath:     filepath.Join(root, "healthy-after.json"),
	}

	a5WriteBuildReportForTest(paths.Scratch127ReportPath,
		a5BuildReportForTest(A5BuildRoleScratch127, A5BuildInputNonContiguous, A5BuildPathDirbuild,
			scratchBytes, 51_000_000, 900_000_000, scratchWallMS))
	a5WriteBuildReportForTest(paths.T283ReportPath,
		a5BuildReportForTest(A5BuildRoleT283, A5BuildInputNonContiguous, A5BuildPathDirbuild,
			0, 134_000_000, 7_300_000_000, 48_000))
	a5WriteBuildReportForTest(paths.HealthyBeforePath,
		a5BuildReportForTest(A5BuildRoleHealthyLustre, A5BuildInputContiguous, A5BuildPathContiguousFast,
			0, 60_000_000, 1_000_000_000, 10_000))
	a5WriteBuildReportForTest(paths.HealthyAfterPath,
		a5BuildReportForTest(A5BuildRoleHealthyLustre, A5BuildInputContiguous, A5BuildPathContiguousFast,
			0, healthyAfterSpool, 1_040_000_000, healthyAfterMS))

	return paths
}

func a5WriteBuildReportForTest(path string, report perfreport.Report) {
	So(perfreport.WriteReport(path, report), ShouldBeNil)
}

func a5BuildReportForTest(
	role string,
	inputShape string,
	buildPath string,
	buildScratchBytes uint64,
	spoolBytes uint64,
	maxRSSBytes uint64,
	durationMS float64,
) perfreport.Report {
	report := perfreport.NewReport("clickhouse_summarise_build", "/input/stats.gz", 1, 0)
	report.MaxRSSBytes = maxRSSBytes
	report.AddOperation(A5BuildOperationName, map[string]any{
		A5BuildInputRole:              role,
		A5BuildInputShape:             inputShape,
		A5BuildInputPath:              buildPath,
		A5BuildInputCompleted:         true,
		A5BuildInputRowCap:            uint64(1_500_000),
		A5BuildInputBuildScratchBytes: buildScratchBytes,
		A5BuildInputSpoolBytes:        spoolBytes,
	}, []float64{durationMS})

	return report
}
