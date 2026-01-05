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
	"io"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLineCountingReader(t *testing.T) {
	Convey("lineCountingReader", t, func() {
		Convey("reads all when maxLines is zero", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\n"), 0)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})

		Convey("stops after maxLines", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\nc\n"), 2)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})
	})
}
