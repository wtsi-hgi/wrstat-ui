/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package group

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type testHandler map[*name][]string

func (t testHandler) Handle(file *summary.FileInfo, group *name) error {
	if file.EntryType != 'f' {
		return nil
	}

	t[group] = append(t[group], string(append(file.Path.AppendTo(nil), file.Name...)))

	return nil
}

func TestGrouper(t *testing.T) {
	Convey("Given a Statemachine and stats file data", t, func() {
		sm, err := NewStatemachine(testGroups[:])
		So(err, ShouldBeNil)

		f := statsdata.NewRoot("/", 0)
		f.AddFile("some/file")
		f.AddFile("some/path/a-file")
		f.AddFile("some/path/b-file")
		f.AddFile("some/path/temp-file")

		sp := stats.NewStatsParser(f.AsReader())

		Convey("You can match files to their correct group", func() {
			output := make(testHandler)

			s := summary.NewSummariser(sp)
			s.AddGlobalOperation(NewGrouper(sm, output))

			So(s.Summarise(), ShouldBeNil)
			So(output, ShouldResemble, testHandler{
				nil:                 {"/some/file"},
				testGroups[0].Group: {"/some/path/a-file", "/some/path/b-file"},
				testGroups[1].Group: {"/some/path/temp-file"},
			})
		})
	})
}
