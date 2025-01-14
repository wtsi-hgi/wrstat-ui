/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

type testGlobalOperator struct {
	dirCounts  map[string]int
	totalCount int
}

func (t *testGlobalOperator) Add(s *FileInfo) error {
	t.totalCount++

	if s.EntryType == 'f' {
		dir := s.Path.AppendTo(nil)

		t.dirCounts[string(dir)]++
	}

	return nil
}

func (testGlobalOperator) Output() error {
	return nil
}

type testDirectoryOperator struct {
	outputMap map[string]int64
	path      string
	size      int64
}

func (t *testDirectoryOperator) Add(s *FileInfo) error {
	if t.path == "" {
		t.path = string(s.Path.AppendTo(nil))
	}

	t.size += s.Size

	return nil
}

func (t *testDirectoryOperator) Output() error {
	t.outputMap[t.path] = t.size

	t.path = ""
	t.size = 0

	return nil
}

func TestParse(t *testing.T) {
	Convey("Given some stats data and a parser, you can make a summariser", t, func() {
		refTime := time.Now().Unix()
		f := statsdata.TestStats(5, 5, "/opt/", refTime).AsReader()
		p := stats.NewStatsParser(f)
		s := NewSummariser(p)

		Convey("You can add an operation and have it apply over every line of data", func() {
			so := &testGlobalOperator{dirCounts: make(map[string]int)}

			s.AddGlobalOperation(func() Operation { return so })

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(so.dirCounts["/opt/dir1/"], ShouldEqual, 4)
		})

		Convey("You can add multiple operations and they run sequentially", func() {
			so := &testGlobalOperator{dirCounts: make(map[string]int)}
			so2 := &testGlobalOperator{dirCounts: make(map[string]int)}

			s.AddGlobalOperation(func() Operation { return so })
			s.AddGlobalOperation(func() Operation { return so2 })

			outputMap := make(map[string]int64)
			outputMap2 := make(map[string]int64)

			s.AddDirectoryOperation(func() Operation {
				return &testDirectoryOperator{outputMap: outputMap}
			})
			s.AddDirectoryOperation(func() Operation {
				return &testDirectoryOperator{outputMap: outputMap2}
			})

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(so2.totalCount, ShouldEqual, 651)
			So(outputMap["/opt/dir0/dir0/dir0/dir0/dir0/"], ShouldEqual, 4096)
			So(outputMap["/opt/dir0/dir0/dir0/dir1/"], ShouldEqual, 8193)
			So(outputMap2["/opt/dir0/dir0/dir0/dir0/dir0/"], ShouldEqual, 4096)
			So(outputMap2["/opt/dir0/dir0/dir0/dir1/"], ShouldEqual, 8193)
		})
	})
}
