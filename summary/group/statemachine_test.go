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
)

func (s StateMachine[T]) getGroup(path string) *T {
	return s[s.getState(1, []byte(path))].Group
}

type name struct {
	name string
}

var testGroups = []PathGroup[name]{ //nolint:gochecknoglobals
	{
		Path:  []byte("/some/path/*"),
		Group: &name{"testGroup[0]"},
	},
	{
		Path:  []byte("/some/path/temp-*"),
		Group: &name{"testGroup[1]"},
	},
	{
		Path:  []byte("/some/path/noBackup/*"),
		Group: &name{"testGroup[2]"},
	},
	{
		Path:  []byte("/some/other/path/*.txt"),
		Group: &name{"testGroup[3]"},
	},
	{
		Path:  []byte("/some/other/path/*.tsv*"),
		Group: &name{"testGroup[4]"},
	},
}

func TestStateMachine(t *testing.T) {
	Convey("With a compiled state machine", t, func() {
		sm, err := NewStatemachine(testGroups)
		So(err, ShouldBeNil)

		Convey("You can get the correct lines for given paths", func() {
			So(sm.getGroup("/some/path"), ShouldEqual, nil)
			So(sm.getGroup("/some/otherPath/"), ShouldEqual, nil)
			So(sm.getGroup("/some/path/"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/file"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/temp-file"), ShouldEqual, testGroups[1].Group)
			So(sm.getGroup("/some/path/noBacku"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/noBackup/"), ShouldEqual, testGroups[2].Group)
			So(sm.getGroup("/some/path/noBackup/someFile"), ShouldEqual, testGroups[2].Group)
			So(sm.getGroup("/some/other/path/file"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/file.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/file.txta.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txts"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/subdir/.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsvs"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.tsv.txt"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txta.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/file.tx.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/file.txt.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt.tsv"), ShouldEqual, testGroups[4].Group)
		})
	})
}
