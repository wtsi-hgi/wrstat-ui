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
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

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

	cuid := uint32(cuidI)

	Convey("Given a GroupUser", t, func() {
		ug := NewByGroupUser()
		So(ug, ShouldNotBeNil)

		Convey("You can add file info to it which accumulates the info", func() {
			addTestData(ug, cuid)

			So(ug.store[2], ShouldNotBeNil)
			So(ug.store[3], ShouldNotBeNil)
			So(ug.store[2][cuid], ShouldNotBeNil)
			So(ug.store[2][2], ShouldNotBeNil)
			So(ug.store[3][2], ShouldNotBeNil)
			So(ug.store[3][cuid], ShouldBeNil)

			So(ug.store[2][cuid], ShouldResemble, &summary{3, 60})

			So(ug.store[2][2], ShouldResemble, &summary{1, 5})

			So(ug.store[3][2], ShouldResemble, &summary{1, 6})

			Convey("And then given an output file", func() {
				dir := t.TempDir()
				outPath := filepath.Join(dir, "out")
				out, err := os.Create(outPath)
				So(err, ShouldBeNil)

				Convey("You can output the summaries to file", func() {
					err = ug.Output(out)
					So(err, ShouldBeNil)
					err = out.Close()
					So(err, ShouldNotBeNil)

					o, errr := os.ReadFile(outPath)
					So(errr, ShouldBeNil)
					output := string(o)

					g, errl := user.LookupGroupId(strconv.Itoa(2))
					So(errl, ShouldBeNil)

					So(output, ShouldContainSubstring, g.Name+"\t"+os.Getenv("USER")+"\t3\t60\n")

					So(checkGroupUserFileIsSorted(outPath), ShouldBeTrue)
				})

				Convey("Output handles bad uids", func() {
					err = ug.Add("/a/b/c/7.txt", newMockInfo(999999999, 2, 1, false))
					testBadIds(err, ug, out, outPath)
				})

				Convey("Output handles bad gids", func() {
					err = ug.Add("/a/b/c/8.txt", newMockInfo(1, 999999999, 1, false))
					testBadIds(err, ug, out, outPath)
				})

				Convey("Output fails if we can't write to the output file", func() {
					err = out.Close()
					So(err, ShouldBeNil)

					err = ug.Output(out)
					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can't Add() on non-unix-like systems'", func() {
			err := ug.Add("/a/b/c/1.txt", &badInfo{})
			So(err, ShouldNotBeNil)
		})
	})
}

func checkGroupUserFileIsSorted(path string) bool {
	return checkFileIsSorted(path, "-k1,1", "-k2,2", "-k3,3n", "-k4,4n")
}
