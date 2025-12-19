/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

package summary

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSummary(t *testing.T) {
	Convey("Given a summary", t, func() {
		s := &Summary{}

		Convey("You can add sizes to it", func() {
			s.Add(10)
			So(s.Count, ShouldEqual, 1)
			So(s.Size, ShouldEqual, 10)

			s.Add(20)
			So(s.Count, ShouldEqual, 2)
			So(s.Size, ShouldEqual, 30)
		})
	})

	Convey("Given a summaryWithAtime", t, func() {
		s := &SummaryWithTimes{}
		now := time.Now().Unix()

		Convey("You can add sizes and atime/mtimes to it", func() {
			s.Add(10, 12, 24, now)
			So(s.Count, ShouldEqual, 1)
			So(s.Size, ShouldEqual, 10)
			So(s.Atime, ShouldEqual, 12)
			So(s.Mtime, ShouldEqual, 24)

			s.Add(20, -5, -10, now)
			So(s.Count, ShouldEqual, 2)
			So(s.Size, ShouldEqual, 30)
			So(s.Atime, ShouldEqual, 12)
			So(s.Mtime, ShouldEqual, 24)

			s.Add(30, 1, 30, now)
			So(s.Count, ShouldEqual, 3)
			So(s.Size, ShouldEqual, 60)
			So(s.Atime, ShouldEqual, 1)
			So(s.Mtime, ShouldEqual, 30)
		})
	})
}

func TestSummaryWithTimes(t *testing.T) {
	Convey("Given a SummaryWithTimes for multiple scenarios", t, func() {
		month := int64(30 * 24 * 3600)
		year := int64(365 * 24 * 3600)
		now := time.Now().Unix()

		Convey("Adding files in each bucket range works correctly", func() {
			s := &SummaryWithTimes{}

			s.Add(1, now-(month/2), now-(month/2), now)
			s.Add(2, now-month, now-month, now)
			s.Add(3, now-(2*month+1), now-(2*month+1), now)
			s.Add(4, now-(6*month+1), now-(6*month+1), now)
			s.Add(5, now-year, now-year, now)
			s.Add(6, now-(2*year+1), now-(2*year+1), now)
			s.Add(7, now-(3*year+1), now-(3*year+1), now)
			s.Add(8, now-(5*year+1), now-(5*year+1), now)
			s.Add(9, now-(7*year+1), now-(7*year+1), now)

			for _, b := range s.AtimeBuckets {
				So(b, ShouldEqual, 1)
			}

			So(s.Atime, ShouldEqual, now-(7*year+1))
			So(s.Mtime, ShouldEqual, now-(month/2))

			So(MostCommonBucket(s.AtimeBuckets), ShouldEqual, RangeLess1Month)
			So(MostCommonBucket(s.MtimeBuckets), ShouldEqual, RangeLess1Month)
		})

		Convey("Tie-breaking in most common bucket", func() {
			s := &SummaryWithTimes{}

			s.Add(10, now-(month/2), now-(month/2), now)
			s.Add(20, now-(month/3), now-(month/3), now)

			s.Add(5, now-(month+1), now-(month+1), now)
			s.Add(6, now-(month+2), now-(month+2), now)

			So(s.AtimeBuckets[RangeLess1Month], ShouldEqual, 2)
			So(s.AtimeBuckets[Range1Month], ShouldEqual, 2)

			So(MostCommonBucket(s.AtimeBuckets), ShouldEqual, RangeLess1Month)
		})

		Convey("Boundary ages", func() {
			s := &SummaryWithTimes{}

			s.Add(1, now-month, now-month, now)
			s.Add(1, now-(2*month), now-(2*month), now)
			s.Add(1, now-year, now-year, now)

			So(s.AtimeBuckets[Range1Month], ShouldEqual, 1)
			So(s.AtimeBuckets[Range2Months], ShouldEqual, 1)
			So(s.AtimeBuckets[Range1Year], ShouldEqual, 1)
		})

		Convey("Zero or negative atime/mtime does not affect buckets", func() {
			s := &SummaryWithTimes{}
			s.Add(10, 0, -10, now)
			s.Add(5, -100, 0, now)

			for _, b := range s.AtimeBuckets {
				So(b, ShouldEqual, 0)
			}

			for _, b := range s.MtimeBuckets {
				So(b, ShouldEqual, 0)
			}

			So(s.Atime, ShouldEqual, 0)
			So(s.Mtime, ShouldEqual, 0)
		})

		Convey("Merging summaries with overlapping buckets sums counts correctly", func() {
			s1 := &SummaryWithTimes{}
			s2 := &SummaryWithTimes{}

			s1.Add(10, now-(month/2), now-(month/2), now)
			s2.Add(20, now-(month/3), now-(month/3), now)
			s2.Add(5, now-(2*month+1), now-(2*month+1), now)

			s1.AddSummary(s2)

			So(s1.AtimeBuckets[RangeLess1Month], ShouldEqual, 2)
			So(s1.AtimeBuckets[Range2Months], ShouldEqual, 1)

			So(MostCommonBucket(s1.AtimeBuckets), ShouldEqual, RangeLess1Month)
		})
	})
}
