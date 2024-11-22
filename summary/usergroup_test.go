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

package summary

import (
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"golang.org/x/exp/slices"
)

func TestUsergroup(t *testing.T) {
	_, cuid, _, _, err := internaluser.RealGIDAndUID()
	if err != nil {
		t.Fatal(err)
	}

	Convey("Given stats data, a Usergroup and a writer", t, func() {
		var w stringBuilder
		ugGen := NewByUserGroup(&w)
		So(ugGen, ShouldNotBeNil)

		ug := ugGen().(*Usergroup)

		Convey("You can add file info to it which accumulates the info", func() {
			addTestData(ug, cuid)

			So(ug.store[cuid], ShouldNotBeNil)
			So(ug.store[2], ShouldNotBeNil)
			So(ug.store[3], ShouldBeNil)
			So(ug.store[cuid][2], ShouldNotBeNil)
			So(ug.store[cuid][3], ShouldBeNil)

			So(len(ug.store[cuid][2]), ShouldEqual, 4)
			So(ug.store[cuid][2]["/a/b/c"], ShouldResemble, &summary{2, 30})
			So(ug.store[cuid][2]["/a/b"], ShouldResemble, &summary{3, 60})
			So(ug.store[cuid][2]["/a"], ShouldResemble, &summary{3, 60})
			So(ug.store[cuid][2]["/"], ShouldResemble, &summary{3, 60})

			So(len(ug.store[2][2]), ShouldEqual, 4)
			So(ug.store[2][2]["/a/b/c"], ShouldResemble, &summary{1, 5})
			So(ug.store[2][2]["/a/b"], ShouldResemble, &summary{1, 5})
			So(ug.store[2][2]["/a"], ShouldResemble, &summary{1, 5})
			So(ug.store[2][2]["/"], ShouldResemble, &summary{1, 5})

			So(len(ug.store[2][3]), ShouldEqual, 4)
			So(ug.store[2][3]["/a/b/c"], ShouldResemble, &summary{1, 6})
			So(ug.store[2][3]["/a/b"], ShouldResemble, &summary{1, 6})
			So(ug.store[2][3]["/a"], ShouldResemble, &summary{1, 6})
			So(ug.store[2][3]["/"], ShouldResemble, &summary{1, 6})

			Convey("You can output the summaries to file", func() {
				err = ug.Output()
				So(err, ShouldBeNil)

				output := w.String()

				g, errl := user.LookupGroupId(strconv.Itoa(2))
				So(errl, ShouldBeNil)

				So(output, ShouldContainSubstring, os.Getenv("USER")+"\t"+
					g.Name+"\t"+strconv.Quote("/a/b/c")+"\t2\t30\n")

				So(checkDataIsSorted(output, 3), ShouldBeTrue)
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
	})
}

type stringBuilder struct {
	strings.Builder
}

func (stringBuilder) Close() error {
	return nil
}

type badWriter struct{}

func (badWriter) Write([]byte) (int, error) {
	return 0, fs.ErrClosed
}

func (badWriter) Close() error {
	return fs.ErrClosed
}

// byColumnAdder describes one of our New* types.
type byColumnAdder interface {
	Add(string, fs.FileInfo) error
	Output(output io.WriteCloser) error
}

func addTestData(a Operation, cuid uint32) {
	err := a.Add(newMockInfo("/a/b/6.txt", cuid, 2, 30, false))
	So(err, ShouldBeNil)
	err = a.Add(newMockInfo("/a/b/c/1.txt", cuid, 2, 10, false))
	So(err, ShouldBeNil)
	err = a.Add(newMockInfo("/a/b/c/2.txt", cuid, 2, 20, false))
	So(err, ShouldBeNil)
	err = a.Add(newMockInfo("/a/b/c/3.txt", 2, 2, 5, false))
	So(err, ShouldBeNil)
	err = a.Add(newMockInfo("/a/b/c/4.txt", 2, 3, 6, false))
	So(err, ShouldBeNil)
	err = a.Add(newMockInfo("/a/b/c/5", 2, 3, 1, true))
	So(err, ShouldBeNil)
}

func newMockInfo(path string, uid, gid uint32, size int64, dir bool) *stats.FileInfo {
	entryType := stats.FileType

	if dir {
		entryType = stats.DirType
	}

	return &stats.FileInfo{
		Path:      []byte(path),
		UID:       uid,
		GID:       gid,
		Size:      size,
		EntryType: byte(entryType),
	}
}

func newMockInfoWithAtime(path string, uid, gid uint32, size int64, dir bool, atime int64) *stats.FileInfo {
	mi := newMockInfo(path, uid, gid, size, dir)
	mi.ATime = atime

	return mi
}

func newMockInfoWithTimes(path string, uid, gid uint32, size int64, dir bool, tim int64) *stats.FileInfo {
	mi := newMockInfo(path, uid, gid, size, dir)
	mi.ATime = tim
	mi.MTime = tim
	mi.CTime = tim

	return mi
}

func testBadIds(err error, a Operation, w *stringBuilder) {
	So(err, ShouldBeNil)

	err = a.Output()
	So(err, ShouldBeNil)

	output := w.String()

	So(output, ShouldContainSubstring, "id999999999")
}

func checkFileIsSorted(path string, args ...string) bool {
	cmd := exec.Command("sort", append(append([]string{"-C"}, args...), path)...) //nolint:gosec
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "LC_ALL=C")

	err := cmd.Run()

	return err == nil
}

func checkDataIsSorted(data string, textCols int) bool {
	lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
	splitLines := make([][]string, len(lines))

	for n, line := range lines {
		splitLines[n] = strings.Split(line, "\t")
	}

	return slices.IsSortedFunc(splitLines, func(a, b []string) int {
		for n, col := range a {
			if n < textCols {
				if cmp := strings.Compare(col, b[n]); cmp != 0 {
					return cmp
				}

				continue
			}

			colA, _ := strconv.ParseInt(col, 10, 0)
			colB, _ := strconv.ParseInt(b[n], 10, 0)

			if dx := colA - colB; dx != 0 {
				return int(dx)
			}
		}

		return 0
	})
}
