/*******************************************************************************
 * Copyright (c) 2021,2024 Genome Research Ltd.
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

package usergroup

import (
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

func TestUsergroup(t *testing.T) {
	gid, uid, gname, uname, err := internaluser.RealGIDAndUID()
	if err != nil {
		t.Fatal(err)
	}

	Convey("UserGroup Operation accumulates count and size by username, group and directory", t, func() {
		var w internaltest.StringBuilder

		ugGenerator := NewByUserGroup(&w)
		So(ugGenerator, ShouldNotBeNil)

		Convey("You can add file info to it which accumulates the info into the output", func() {
			f := statsdata.NewRoot("/opt/", 0)
			f.UID = uid
			f.GID = gid

			ud := f.AddDirectory("userDir")
			ud.AddFile("file1.txt").Size = 1
			ud.AddFile("file2.txt").Size = 2
			ud.AddDirectory("subDir").AddDirectory("subsubDir").AddFile("file3.txt").Size = 3

			otherDir := f.AddDirectory("other")
			otherDir.UID = 0
			otherDir.GID = 0
			otherDir.AddDirectory("someDir").AddFile("someFile").Size = 50
			otherDir.AddFile("miscFile").Size = 51

			p := stats.NewStatsParser(f.AsReader())
			s := summary.NewSummariser(p)
			s.AddDirectoryOperation(ugGenerator)

			err = s.Summarise()
			So(err, ShouldBeNil)

			output := w.String()

			So(output, ShouldContainSubstring, uname+"\t"+
				gname+"\t"+strconv.Quote("/opt/")+"\t3\t6\n")

			So(output, ShouldContainSubstring, uname+"\t"+
				gname+"\t"+strconv.Quote("/opt/userDir/")+"\t3\t6\n")

			So(output, ShouldContainSubstring, uname+"\t"+
				gname+"\t"+strconv.Quote("/opt/userDir/subDir/")+"\t1\t3\n")

			So(output, ShouldContainSubstring, uname+"\t"+
				gname+"\t"+strconv.Quote("/opt/userDir/subDir/subsubDir/")+"\t1\t3\n")

			So(output, ShouldNotContainSubstring, "root\troot\t"+
				strconv.Quote("/opt/userDir/"))

			So(output, ShouldNotContainSubstring, uname+"\t"+
				gname+"\t"+strconv.Quote("/opt/other/"))

			So(output, ShouldContainSubstring, "root\troot\t"+
				strconv.Quote("/opt/")+"\t2\t101\n")

			So(output, ShouldContainSubstring, "root\troot\t"+
				strconv.Quote("/opt/other/")+"\t2\t101\n")

			So(internaltest.CheckDataIsSorted(output, 3), ShouldBeTrue)
		})

		Convey("Output handles bad uids", func() {
			paths := internaltest.NewDirectoryPathCreator()
			ug := ugGenerator()
			err = ug.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/"), 999999999, 2, 1, true))
			So(err, ShouldBeNil)

			err = ug.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/file.txt"), 999999999, 2, 1, false))
			internaltest.TestBadIDs(err, ug, &w)
		})

		Convey("Output handles bad gids", func() {
			paths := internaltest.NewDirectoryPathCreator()
			ug := NewByUserGroup(&w)()
			err = ug.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/"), 999999999, 2, 1, true))
			So(err, ShouldBeNil)

			err = ug.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/8.txt"), 1, 999999999, 1, false))
			internaltest.TestBadIDs(err, ug, &w)
		})

		Convey("Output fails if we can't write to the output file", func() {
			err = NewByUserGroup(internaltest.BadWriter{})().Output()
			So(err, ShouldNotBeNil)
		})
	})
}
