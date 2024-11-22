/*******************************************************************************
 * Copyright (c) 2021, 2024 Genome Research Ltd.
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
	"fmt"
	"os/user"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGroupUser(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		t.Fatal(err.Error())
	}

	cuidI, err := strconv.Atoi(usr.Uid)
	if err != nil {
		t.Fatal(err.Error())
	}

	cuid := int64(cuidI)

	gidI, err := strconv.Atoi(usr.Gid)
	if err != nil {
		t.Fatal(err.Error())
	}

	gid := int64(gidI)

	g, err := user.LookupGroupId(strconv.FormatInt(gid, 10))
	if err != nil {
		t.Fatal(err.Error())
	}

	uname := usr.Username
	gname := g.Name

	tim := time.Now().Unix()

	Convey("GroupUser Operation accumulates count and size by group and username", t, func() {
		var w stringBuilder

		ugGenerator := NewByGroupUser(&w)
		So(ugGenerator, ShouldNotBeNil)

		ug := ugGenerator().(*GroupUser)

		Convey("You can add file info to it which accumulates the info into the output", func() {
			ug.Add(newMockInfoWithTimes("/a/b/d/file3.txt", 0, gid, 3, false, tim))
			ug.Add(newMockInfoWithTimes("/a/b/c/file1.txt", cuid, gid, 1, false, tim))
			ug.Add(newMockInfoWithTimes("/a/b/d/file2.txt", cuid, gid, 2, false, tim))
			ug.Add(newMockInfoWithTimes("/a/b/d/file4.txt", cuid, 0, 4, false, tim))
			ug.Add(newMockInfoWithTimes("/a/e/file5.txt", 0, 0, 5, false, tim))
			ug.Add(newMockInfoWithTimes("/a/", 0, 0, 4096, true, tim))

			err = ug.Output()
			So(err, ShouldBeNil)

			output := w.String()

			So(output, ShouldContainSubstring, fmt.Sprintf("%s\t%s\t2\t3\n", gname, uname))
			So(output, ShouldContainSubstring, fmt.Sprintf("%s\troot\t1\t3\n", gname))
			So(output, ShouldContainSubstring, fmt.Sprintf("root\t%s\t1\t4\n", uname))
			So(output, ShouldContainSubstring, "root\troot\t1\t5\n")

			So(checkDataIsSorted(output, 2), ShouldBeTrue)
		})

		Convey("Output handles bad uids", func() {
			err = ug.Add(newMockInfo("/a/b/c/7.txt", 999999999, 2, 1, false))
			testBadIds(err, ug, &w)
		})

		Convey("Output handles bad gids", func() {
			err = ug.Add(newMockInfo("/a/b/c/8.txt", 1, 999999999, 1, false))
			testBadIds(err, ug, &w)
		})

		Convey("Output fails if we can't write to the output file", func() {
			ug.w = badWriter{}

			err = ug.Output()
			So(err, ShouldNotBeNil)
		})
	})
}
