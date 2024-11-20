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
		s := &summary{}

		Convey("You can add sizes to it", func() {
			s.add(10)
			So(s.count, ShouldEqual, 1)
			So(s.size, ShouldEqual, 10)

			s.add(20)
			So(s.count, ShouldEqual, 2)
			So(s.size, ShouldEqual, 30)
		})
	})

	Convey("Given a summaryWithAtime", t, func() {
		s := &summaryWithTimes{}

		Convey("You can add sizes and atime/mtimes to it", func() {
			s.add(10, 12, 24)
			So(s.count, ShouldEqual, 1)
			So(s.size, ShouldEqual, 10)
			So(s.atime, ShouldEqual, 12)
			So(s.mtime, ShouldEqual, 24)

			s.add(20, -5, -10)
			So(s.count, ShouldEqual, 2)
			So(s.size, ShouldEqual, 30)
			So(s.atime, ShouldEqual, 12)
			So(s.mtime, ShouldEqual, 24)

			s.add(30, 1, 30)
			So(s.count, ShouldEqual, 3)
			So(s.size, ShouldEqual, 60)
			So(s.atime, ShouldEqual, 1)
			So(s.mtime, ShouldEqual, 30)
		})
	})
}
