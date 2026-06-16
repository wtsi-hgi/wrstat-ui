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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

func TestJ5CacheScopes(t *testing.T) {
	Convey("required cache scopes match J5", t, func() {
		So(j5RequiredCacheScopes(), ShouldResemble, []string{
			queryScopeFreshProvider,
			queryScopeColdProvider,
			queryScopeSameProviderCold,
			queryScopeSameQueryClient,
			queryScopeAncestorDirs,
			queryScopeNewDirEachRepeat,
			queryScopeVisibleChildDirs,
			queryScopeStartupAudit,
			queryScopeProviderUpdateCold,
			queryScopeSameProviderDir,
		})
	})

	Convey("buildOps labels every required J5 cache scope", t, func() {
		ops := buildOps(queryMatrixTestContext(), queryMatrixTestOptions(), func(string, ...any) {})
		covered := make(map[string]int)

		for _, op := range ops {
			if scope := stringInput(op.inputs, queryInputCacheScope); scope != "" {
				covered[scope]++
			}
		}

		for _, scope := range j5RequiredCacheScopes() {
			So(covered[scope], ShouldBeGreaterThan, 0)
		}
	})

	Convey("validation fails when a required scope is missing", t, func() {
		report := j5CompleteReport()
		missingScope := cloneJ4Report(report)
		missingScope.Operations = missingScope.Operations[:len(missingScope.Operations)-1]

		So(
			j5CacheScopeCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingScope}),
			ShouldContainSubstring,
			queryScopeSameProviderDir,
		)
	})

	Convey("validation requires high-fanout repeated-read evidence before and after", t, func() {
		report := j5CompleteReport()

		So(j5CacheScopeCoverageFailure(
			[]perfreport.Report{report},
			[]perfreport.Report{report},
		), ShouldEqual, "")

		missingFanout := cloneJ4Report(report)
		for i := range missingFanout.Operations {
			delete(missingFanout.Operations[i].Inputs, queryInputHighFanoutChildCount)
		}

		So(
			j5CacheScopeCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingFanout}),
			ShouldContainSubstring,
			"candidate missing high-fanout repeated-read evidence",
		)

		singleSample := cloneJ4Report(report)
		delete(singleSample.Operations[0].Inputs, queryInputHighFanoutChildCount)
		singleSample.AddOperationWithCounters(
			"j5_high_fanout_single_sample",
			map[string]any{
				queryInputCacheScope:           queryScopeFreshProvider,
				queryInputHighFanoutChildCount: uint64(11205),
			},
			[]float64{12},
			nil,
			nil,
			nil,
			[]uint64{4},
		)

		So(
			j5CacheScopeCoverageFailure([]perfreport.Report{singleSample}, []perfreport.Report{report}),
			ShouldContainSubstring,
			"baseline missing high-fanout repeated-read evidence",
		)
	})

	Convey("cache-scope deltas report before and after evidence by scope", t, func() {
		baseline := j5CompleteReport()
		candidate := j5CompleteReport()
		candidate.Operations[0].P95MS = baseline.Operations[0].P95MS + 7

		deltas, err := j5CacheScopeDeltas(
			[]perfreport.Report{baseline},
			[]perfreport.Report{candidate},
		)

		So(err, ShouldBeNil)
		So(deltas, ShouldHaveLength, len(j5RequiredCacheScopes()))
		So(deltas[0].CacheScope, ShouldEqual, queryScopeFreshProvider)
		So(deltas[0].BaselineRepeats, ShouldEqual, 3)
		So(deltas[0].CandidateRepeats, ShouldEqual, 3)
		So(deltas[0].BaselineHighFanoutChildCount, ShouldEqual, uint64(11205))
		So(deltas[0].CandidateHighFanoutChildCount, ShouldEqual, uint64(11205))
		So(deltas[0].DeltaP95MS, ShouldEqual, float64(7))
	})
}

func j5CompleteReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", 3, 0)

	for i, scope := range j5RequiredCacheScopes() {
		durations := []float64{float64(i + 1), float64(i + 2), float64(i + 3)}
		if scope == queryScopeStartupAudit {
			durations = []float64{float64(i + 1)}
		}

		inputs := map[string]any{
			queryInputCacheScope:   scope,
			queryInputResultDigest: "sha256:j5",
		}
		if i == 0 {
			inputs[queryInputHighFanoutChildCount] = uint64(11205)
		}

		report.AddOperationWithCounters(
			"j5_"+scope,
			inputs,
			durations,
			[]uint64{10, 10, 10},
			[]uint64{20, 20, 20},
			[]uint64{1, 1, 1},
			repeatUint64(4, len(durations)),
		)
	}

	return report
}

func repeatUint64(value uint64, count int) []uint64 {
	values := make([]uint64, count)
	for i := range count {
		values[i] = value
	}

	return values
}
