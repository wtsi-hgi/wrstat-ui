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

		Convey("You can add sizes and atime/mtimes to it", func() {
			s.Add(10, 12, 24)
			So(s.Count, ShouldEqual, 1)
			So(s.Size, ShouldEqual, 10)
			So(s.Atime, ShouldEqual, 12)
			So(s.Mtime, ShouldEqual, 24)

			s.Add(20, -5, -10)
			So(s.Count, ShouldEqual, 2)
			So(s.Size, ShouldEqual, 30)
			So(s.Atime, ShouldEqual, 12)
			So(s.Mtime, ShouldEqual, 24)

			s.Add(30, 1, 30)
			So(s.Count, ShouldEqual, 3)
			So(s.Size, ShouldEqual, 60)
			So(s.Atime, ShouldEqual, 1)
			So(s.Mtime, ShouldEqual, 30)
		})
	})
}
