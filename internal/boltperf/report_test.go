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

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

func TestPercentilesMS(t *testing.T) {
	Convey("PercentilesMS computes p50/p95/p99", t, func() {
		p50, p95, p99 := PercentilesMS([]float64{10, 1, 5, 20})
		So(p50, ShouldEqual, 5)
		So(p95, ShouldEqual, 20)
		So(p99, ShouldEqual, 20)
	})

	Convey("PercentilesMS on empty slice", t, func() {
		p50, p95, p99 := PercentilesMS(nil)
		So(p50, ShouldEqual, 0)
		So(p95, ShouldEqual, 0)
		So(p99, ShouldEqual, 0)
	})
}

func TestBoltQueryReportEvidence(t *testing.T) {
	Convey("E1 bolt query reports include stable result counts and summary digests", t, func() {
		report := NewReport("bolt", "", 2, 0)
		ctx := queryContext{
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Repeat: 2, Splits: 1})
		whereOp := findQuerySuiteTestOp(ops, queryOpTreeWhereName)
		So(whereOp, ShouldNotBeNil)

		err := timeAndReportQueryOp(&report, QueryOptions{Repeat: 2}, func(string, ...any) {}, *whereOp)

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].ResultCount, ShouldResemble, []uint64{3, 3})
		So(report.Operations[0].Inputs[queryInputResultDigest], ShouldNotBeBlank)
		So(report.Operations[0].P50MS, ShouldBeGreaterThanOrEqualTo, 0.0)
		So(report.Operations[0].P95MS, ShouldBeGreaterThanOrEqualTo, 0.0)
		So(report.Operations[0].P99MS, ShouldBeGreaterThanOrEqualTo, 0.0)
	})
}
