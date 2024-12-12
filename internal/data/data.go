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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const filePerms = 0644

type stringBuilderCloser struct {
	strings.Builder
}

func (s stringBuilderCloser) Close() error {
	return nil
}

type TestFile struct {
	Path           string
	UID, GID       uint32
	NumFiles       int
	SizeOfEachFile int
	ATime, MTime   int
}

var i int //nolint:gochecknoglobals

func addFiles(d *statsdata.Directory, directory, suffix string, numFiles int,
	sizeOfEachFile, atime, mtime int64, gid, uid uint32) {
	for range numFiles {
		statsdata.AddFile(d, filepath.Join(directory, strconv.Itoa(i)+suffix), uid, gid, sizeOfEachFile, atime, mtime)

		i++
	}
}

func CreateDefaultTestData(gidA, gidB, gidC, uidA, uidB uint32, refTime int64) *statsdata.Directory {
	dir := statsdata.NewRoot("/", 0)
	dir.ATime = refTime
	// dir.MTime = refTime
	dir.GID = gidA
	dir.UID = uidA

	ac := dir.AddDirectory("a").AddDirectory("c")
	ac.GID = gidB
	ac.UID = uidB

	addFiles(dir, "a/b/d/f", "file.cram", 1, 10, 50, 50, gidA, uidA)
	addFiles(dir, "a/b/d/g", "file.cram", 2, 10, 60, 60, gidA, uidA)
	addFiles(dir, "a/b/d/g", "file.cram", 4, 10, 75, 75, gidA, uidB)
	addFiles(dir, "a/b/e/h", "file.bam", 1, 5, 100, 30, gidA, uidA)
	addFiles(dir, "a/b/e/h/tmp", "file.bam", 1, 5, 80, 80, gidA, uidA)
	addFiles(dir, "a/c/d", "file.cram", 5, 1, 90, 90, gidB, uidB)

	if gidC == 0 {
		addFiles(dir, "a/b/d/i/j", "file.cram", 1, 1, 50, 50, gidC, uidB)
		addFiles(dir, "a/b/d/g", "file.cram", 4, 10, 50, 75, gidA, uidB)
		addFiles(dir, "k", "file1.cram", 1, 1, refTime-(db.SecondsInAYear*3), refTime-(db.SecondsInAYear*7), gidB, uidA)
		addFiles(dir, "k", "file2.cram", 1, 2, refTime-(db.SecondsInAYear*1), refTime-(db.SecondsInAYear*2), gidB, uidA)
		addFiles(dir, "k", "file3.cram", 1, 3, refTime-(db.SecondsInAMonth)-10, refTime-(db.SecondsInAMonth*2), gidB, uidA)
		addFiles(dir, "k", "file4.cram", 1, 4, refTime-(db.SecondsInAMonth*6), refTime-(db.SecondsInAYear), gidB, uidA)
		addFiles(dir, "k", "file5.cram", 1, 5, refTime, refTime, gidB, uidA)
	} else {
		addFiles(dir, "a/c/d", "file.cram", 7, 1, refTime-db.SecondsInAYear, refTime-(db.SecondsInAYear*3), 3, 103)
	}

	return dir
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

func addTestFileInfo(t *testing.T, dguta *dirguta.DirGroupUserTypeAge, doneDirs map[string]bool,
	path string, numFiles, sizeOfEachFile int, gid, uid uint32, atime, mtime int,
) {
	t.Helper()

	paths := NewDirectoryPathCreator()
	dir, basename := filepath.Split(path)

	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(dir, strconv.FormatInt(int64(i), 10)+basename)

		info := &summary.FileInfo{
			Path:      paths.ToDirectoryPath(filePath),
			UID:       uid,
			GID:       gid,
			Size:      int64(sizeOfEachFile),
			ATime:     int64(atime),
			MTime:     int64(mtime),
			EntryType: stats.FileType,
		}

		err := dguta.Add(info)
		if err != nil {
			t.Fatal(err)
		}
	}

	addTestDirInfo(t, dguta, doneDirs, filepath.Dir(path), gid, uid)
}

func addTestDirInfo(t *testing.T, dguta *dirguta.DirGroupUserTypeAge, doneDirs map[string]bool,
	dir string, gid, uid uint32,
) {
	t.Helper()

	for {
		if doneDirs[dir] {
			return
		}

		info := &summary.FileInfo{
			Path:      nil,
			EntryType: stats.DirType,
			UID:       uid,
			GID:       gid,
			Size:      int64(1024),
			MTime:     1,
		}

		err := dguta.Add(info)
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

func FakeFilesForDGUTADBForBasedirsTesting(gid, uid uint32) ([]string, []TestFile) {
	projectA := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "A")
	projectB125 := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "B")
	projectB123 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt1", "projects", "B")
	projectC1 := filepath.Join("/", "lustre", "scratch123", "hgi", "m0")
	projectC2 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt0")
	user2 := filepath.Join("/", "lustre", "scratch125", "humgen", "teams", "102")
	projectD := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "D")
	projectDSub1 := filepath.Join(projectD, "sub1")
	projectDSub2 := filepath.Join(projectD, "sub2")

	files := []TestFile{
		{
			Path:           filepath.Join(projectA, "a.bam"),
			NumFiles:       1,
			SizeOfEachFile: 10,
			GID:            1,
			UID:            101,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectA, "sub", "a.bam"),
			NumFiles:       1,
			SizeOfEachFile: 11,
			GID:            1,
			UID:            101,
			ATime:          50,
			MTime:          100,
		},
		{
			Path:           filepath.Join(projectB125, "b.bam"),
			NumFiles:       1,
			SizeOfEachFile: 20,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectB123, "b.bam"),
			NumFiles:       1,
			SizeOfEachFile: 30,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectC1, "c.bam"),
			NumFiles:       1,
			SizeOfEachFile: 40,
			GID:            2,
			UID:            88888,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectC2, "c.bam"),
			NumFiles:       1,
			SizeOfEachFile: 40,
			GID:            2,
			UID:            88888,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(user2, "d.bam"),
			NumFiles:       1,
			SizeOfEachFile: 60,
			GID:            77777,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
	}

	files = append(files,
		TestFile{
			Path:           filepath.Join(projectDSub1, "a.bam"),
			NumFiles:       1,
			SizeOfEachFile: 1,
			GID:            gid,
			UID:            uid,
			ATime:          50,
			MTime:          50,
		},
		TestFile{
			Path:           filepath.Join(projectDSub1, "temp", "a.sam"),
			NumFiles:       1,
			SizeOfEachFile: 2,
			GID:            gid,
			UID:            uid,
			ATime:          50,
			MTime:          50,
		},
		TestFile{
			Path:           filepath.Join(projectDSub1, "a.cram"),
			NumFiles:       1,
			SizeOfEachFile: 3,
			GID:            gid,
			UID:            uid,
			ATime:          50,
			MTime:          50,
		},
		TestFile{
			Path:           filepath.Join(projectDSub2, "a.bed"),
			NumFiles:       1,
			SizeOfEachFile: 4,
			GID:            gid,
			UID:            uid,
			ATime:          50,
			MTime:          50,
		},
		TestFile{
			Path:           filepath.Join(projectDSub2, "b.bed"),
			NumFiles:       1,
			SizeOfEachFile: 5,
			GID:            gid,
			UID:            uid,
			ATime:          50,
			MTime:          50,
		},
	)

	return []string{projectA, projectB125, projectB123, projectC1, projectC2, user2, projectD}, files
}

const ExampleQuotaCSV = `1,/disk/1,10,20
1,/disk/2,11,21
2,/disk/1,12,22
`

// CreateQuotasCSV creates a quotas csv file in a temp directory. Returns its
// path. You can use ExampleQuotaCSV as the csv data.
func CreateQuotasCSV(t *testing.T, csv string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "quotas.csv")

	if err := os.WriteFile(path, []byte(csv), filePerms); err != nil {
		t.Fatalf("could not write test csv file: %s", err)
	}

	return path
}

const ExampleOwnersCSV = `1,Alan
2,Barbara
4,Dellilah`

// CreateOwnersCSV creates an owners csv files in a temp directory. Returns its
// path. You can use ExampleOwnersCSV as the csv data.
func CreateOwnersCSV(t *testing.T, csv string) (string, error) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "quotas.csv")

	err := writeFile(path, csv)

	return path, err
}

func writeFile(path, contents string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	_, err = io.WriteString(f, contents)
	if err != nil {
		return err
	}

	return f.Close()
}

type DirectoryPathCreator map[string]*summary.DirectoryPath

func (d DirectoryPathCreator) ToDirectoryPath(p string) *summary.DirectoryPath {
	pos := strings.LastIndexByte(p[:len(p)-1], '/')
	dir := p[:pos+1]
	base := p[pos+1:]

	if dp, ok := d[p]; ok {
		dp.Name = base

		return dp
	}

	parent := d.ToDirectoryPath(dir)

	dp := &summary.DirectoryPath{
		Name:   base,
		Depth:  strings.Count(p, "/"),
		Parent: parent,
	}

	d[p] = dp

	return dp
}

func NewDirectoryPathCreator() DirectoryPathCreator {
	d := make(DirectoryPathCreator)

	d["/"] = &summary.DirectoryPath{
		Name:  "/",
		Depth: -1,
	}

	return d
}
