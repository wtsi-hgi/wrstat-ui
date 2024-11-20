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
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUsergroup(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		t.Fatal(err.Error())
	}

	cuidI, err := strconv.Atoi(usr.Uid)
	if err != nil {
		t.Fatal(err.Error())
	}

	cuid := uint32(cuidI)

	Convey("Given a Usergroup", t, func() {
		ug := NewByUserGroup()
		So(ug, ShouldNotBeNil)

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

					So(output, ShouldContainSubstring, os.Getenv("USER")+"\t"+
						g.Name+"\t"+strconv.Quote("/a/b/c")+"\t2\t30\n")

					So(checkUserGroupFileIsSorted(outPath), ShouldBeTrue)
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

// byColumnAdder describes one of our New* types.
type byColumnAdder interface {
	Add(string, fs.FileInfo) error
	Output(output io.WriteCloser) error
}

func addTestData(a byColumnAdder, cuid uint32) {
	err := a.Add("/a/b/c/1.txt", newMockInfo(cuid, 2, 10, false))
	So(err, ShouldBeNil)
	err = a.Add("/a/b/c/2.txt", newMockInfo(cuid, 2, 20, false))
	So(err, ShouldBeNil)
	err = a.Add("/a/b/c/3.txt", newMockInfo(2, 2, 5, false))
	So(err, ShouldBeNil)
	err = a.Add("/a/b/c/4.txt", newMockInfo(2, 3, 6, false))
	So(err, ShouldBeNil)
	err = a.Add("/a/b/c/5", newMockInfo(2, 3, 1, true))
	So(err, ShouldBeNil)
	err = a.Add("/a/b/6.txt", newMockInfo(cuid, 2, 30, false))
	So(err, ShouldBeNil)
}

// mockInfo is an fs.FileInfo that has given data.
type mockInfo struct {
	uid   uint32
	gid   uint32
	size  int64
	isDir bool
	atime int64
	mtime int64
	ctime int64
}

func newMockInfo(uid, gid uint32, size int64, dir bool) *mockInfo {
	return &mockInfo{
		uid:   uid,
		gid:   gid,
		size:  size,
		isDir: dir,
	}
}

func newMockInfoWithAtime(uid, gid uint32, size int64, dir bool, atime int64) *mockInfo {
	mi := newMockInfo(uid, gid, size, dir)
	mi.atime = atime

	return mi
}

func (m *mockInfo) Name() string { return "" }

func (m *mockInfo) Size() int64 { return m.size }

func (m *mockInfo) Mode() fs.FileMode {
	return os.ModePerm
}

func (m *mockInfo) ModTime() time.Time { return time.Now() }

func (m *mockInfo) IsDir() bool { return m.isDir }

func (m *mockInfo) Sys() interface{} {
	return &syscall.Stat_t{
		Uid:  m.uid,
		Gid:  m.gid,
		Atim: syscall.Timespec{Sec: m.atime},
		Mtim: syscall.Timespec{Sec: m.mtime},
		Ctim: syscall.Timespec{Sec: m.ctime},
	}
}

// badInfo is a mockInfo that has a Sys() that returns nonsense.
type badInfo struct {
	mockInfo
}

func (b *badInfo) Sys() interface{} {
	return "foo"
}

func testBadIds(err error, a byColumnAdder, out *os.File, outPath string) {
	So(err, ShouldBeNil)

	err = a.Output(out)
	So(err, ShouldBeNil)

	o, errr := os.ReadFile(outPath)
	So(errr, ShouldBeNil)

	output := string(o)

	So(output, ShouldContainSubstring, "id999999999")
}

func checkFileIsSorted(path string, args ...string) bool {
	cmd := exec.Command("sort", append(append([]string{"-C"}, args...), path)...) //nolint:gosec
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "LC_ALL=C")

	err := cmd.Run()

	return err == nil
}

func checkUserGroupFileIsSorted(path string) bool {
	return checkFileIsSorted(path, "-k1,1", "-k2,2", "-k3,3", "-k4,4n", "-k5,5n")
}
