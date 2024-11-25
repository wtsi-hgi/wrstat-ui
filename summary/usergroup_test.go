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
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"golang.org/x/exp/slices"
)

func TestUsergroup(t *testing.T) {
	gid, uid, gname, uname, err := internaluser.RealGIDAndUID()
	if err != nil {
		t.Fatal(err)
	}

	Convey("UserGroup Operation accumulates count and size by username, group and directory", t, func() {
		var w stringBuilder

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
			s := NewSummariser(p)
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

			So(checkDataIsSorted(output, 3), ShouldBeTrue)
		})

		Convey("Output handles bad uids", func() {
			paths := NewDirectoryPathCreator()
			ug := ugGenerator()
			err = ug.Add(newMockInfo(paths.ToDirectoryPath("/a/b/c/"), 999999999, 2, 1, true))
			So(err, ShouldBeNil)

			err = ug.Add(newMockInfo(paths.ToDirectoryPath("/a/b/c/file.txt"), 999999999, 2, 1, false))
			testBadIds(err, ug, &w)
		})

		Convey("Output handles bad gids", func() {
			paths := NewDirectoryPathCreator()
			ug := NewByUserGroup(&w)()
			err = ug.Add(newMockInfo(paths.ToDirectoryPath("/a/b/c/"), 999999999, 2, 1, true))
			So(err, ShouldBeNil)

			err = ug.Add(newMockInfo(paths.ToDirectoryPath("/a/b/c/8.txt"), 1, 999999999, 1, false))
			testBadIds(err, ug, &w)
		})

		Convey("Output fails if we can't write to the output file", func() {
			err = NewByUserGroup(badWriter{})().Output()
			So(err, ShouldNotBeNil)
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

func newMockInfo(path *DirectoryPath, uid, gid uint32, size int64, dir bool) *FileInfo {
	entryType := stats.FileType

	if dir {
		entryType = stats.DirType
	}

	return &FileInfo{
		Path:      path,
		UID:       uid,
		GID:       gid,
		Size:      size,
		EntryType: byte(entryType),
	}
}

func newMockInfoWithAtime(path *DirectoryPath, uid, gid uint32, size int64, dir bool, atime int64) *FileInfo {
	mi := newMockInfo(path, uid, gid, size, dir)
	mi.ATime = atime

	return mi
}

func newMockInfoWithTimes(path *DirectoryPath, uid, gid uint32, size int64, dir bool, tim int64) *FileInfo {
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

type DirectoryPathCreator map[string]*DirectoryPath

func (d DirectoryPathCreator) ToDirectoryPath(p string) *DirectoryPath {
	pos := strings.LastIndexByte(p[:len(p)-1], '/')
	dir := p[:pos+1]
	base := p[pos+1:]

	if dp, ok := d[p]; ok {
		dp.Name = base

		return dp
	}

	parent := d.ToDirectoryPath(dir)

	dp := &DirectoryPath{
		Name:   base,
		Depth:  strings.Count(p, "/"),
		Parent: parent,
	}

	d[p] = dp

	return dp
}

func NewDirectoryPathCreator() DirectoryPathCreator {
	d := make(DirectoryPathCreator)

	d["/"] = &DirectoryPath{
		Name:  "/",
		Depth: -1,
	}

	return d
}
