/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package internaldata

import (
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wtsi-ssg/wrstat/v4/summary"
)

type stringBuilderCloser struct {
	strings.Builder
}

func (s stringBuilderCloser) Close() error {
	return nil
}

type TestFile struct {
	Path           string
	UID, GID       int
	NumFiles       int
	SizeOfEachFile int
	ATime, MTime   int
}

func CreateDefaultTestData(gidA, gidB, gidC, uidA, uidB int) []TestFile {
	dir := "/"
	abdf := filepath.Join(dir, "a", "b", "d", "f")
	abdg := filepath.Join(dir, "a", "b", "d", "g")
	abehtmp := filepath.Join(dir, "a", "b", "e", "h", "tmp")
	acd := filepath.Join(dir, "a", "c", "d")
	files := []TestFile{
		{
			Path:           filepath.Join(abdf, "file.cram"),
			NumFiles:       1,
			SizeOfEachFile: 10,
			GID:            gidA,
			UID:            uidA,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(abdg, "file.cram"),
			NumFiles:       2,
			SizeOfEachFile: 10,
			GID:            gidA,
			UID:            uidA,
			ATime:          60,
			MTime:          60,
		},
		{
			Path:           filepath.Join(abdg, "file.cram"),
			NumFiles:       4,
			SizeOfEachFile: 10,
			GID:            gidA,
			UID:            uidB,
			ATime:          75,
			MTime:          75,
		},
		{
			Path:           filepath.Join(dir, "a", "b", "e", "h", "file.bam"),
			NumFiles:       1,
			SizeOfEachFile: 5,
			GID:            gidA,
			UID:            uidA,
			ATime:          100,
			MTime:          30,
		},
		{
			Path:           filepath.Join(abehtmp, "file.bam"),
			NumFiles:       1,
			SizeOfEachFile: 5,
			GID:            gidA,
			UID:            uidA,
			ATime:          80,
			MTime:          80,
		},
		{Path: filepath.Join(acd, "file.cram"),
			NumFiles:       5,
			SizeOfEachFile: 1,
			GID:            gidB,
			UID:            uidB,
			ATime:          90,
			MTime:          90,
		},
	}

	if gidC == 0 {
		files = append(files,
			TestFile{
				Path:           filepath.Join(dir, "a", "file.cram"),
				NumFiles:       1,
				SizeOfEachFile: 1,
				GID:            gidC,
				UID:            uidB,
				ATime:          50,
				MTime:          50,
			},
			TestFile{Path: filepath.Join(abdg, "file.cram"),
				NumFiles:       4,
				SizeOfEachFile: 10,
				GID:            gidA,
				UID:            uidB,
				ATime:          50,
				MTime:          75,
			},
		)
	}

	return files
}

func TestDGUTData(t *testing.T, files []TestFile) string {
	t.Helper()

	dgut := summary.NewByDirGroupUserType()
	doneDirs := make(map[string]bool)

	for _, file := range files {
		addTestFileInfo(t, dgut, doneDirs, file.Path, file.NumFiles,
			file.SizeOfEachFile, file.GID, file.UID, file.ATime, file.MTime)
	}

	var sb stringBuilderCloser

	err := dgut.Output(&sb)
	if err != nil {
		t.Fatal(err)
	}

	return sb.String()
}

type fakeFileInfo struct {
	dir  bool
	stat *syscall.Stat_t
}

func (f *fakeFileInfo) Name() string       { return "" }
func (f *fakeFileInfo) Size() int64        { return f.stat.Size }
func (f *fakeFileInfo) Mode() fs.FileMode  { return 0 }
func (f *fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f *fakeFileInfo) IsDir() bool        { return f.dir }
func (f *fakeFileInfo) Sys() any           { return f.stat }

func addTestFileInfo(t *testing.T, dgut *summary.DirGroupUserType, doneDirs map[string]bool,
	path string, numFiles, sizeOfEachFile, gid, uid, atime, mtime int) {
	t.Helper()

	dir, basename := filepath.Split(path)

	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(dir, strconv.FormatInt(int64(i), 10)+basename)

		info := &fakeFileInfo{
			stat: &syscall.Stat_t{
				Uid:  uint32(uid),
				Gid:  uint32(gid),
				Size: int64(sizeOfEachFile),
				Atim: syscall.Timespec{Sec: int64(atime)},
				Mtim: syscall.Timespec{Sec: int64(mtime)},
			},
		}

		err := dgut.Add(filePath, info)
		if err != nil {
			t.Fatal(err)
		}
	}

	addTestDirInfo(t, dgut, doneDirs, filepath.Dir(path), gid, uid)
}

func addTestDirInfo(t *testing.T, dgut *summary.DirGroupUserType, doneDirs map[string]bool,
	dir string, gid, uid int) {
	t.Helper()

	for {
		if doneDirs[dir] {
			return
		}

		info := &fakeFileInfo{
			dir: true,
			stat: &syscall.Stat_t{
				Uid:  uint32(uid),
				Gid:  uint32(gid),
				Size: int64(1024),
				Mtim: syscall.Timespec{Sec: int64(1)},
			},
		}

		err := dgut.Add(dir, info)
		if err != nil {
			t.Fatal(err)
		}

		doneDirs[dir] = true

		dir = filepath.Dir(dir)
		if dir == "/" {
			return
		}
	}
}
