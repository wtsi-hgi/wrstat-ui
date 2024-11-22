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

	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
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

func CreateDefaultTestData(gidA, gidB, gidC, uidA, uidB uint32, refUnixTime int64) []TestFile {
	refTime := int(refUnixTime)
	dir := "/"
	abdf := filepath.Join(dir, "a", "b", "d", "f")
	abdg := filepath.Join(dir, "a", "b", "d", "g")
	abehtmp := filepath.Join(dir, "a", "b", "e", "h", "tmp")
	acd := filepath.Join(dir, "a", "c", "d")
	abdij := filepath.Join(dir, "a", "b", "d", "i", "j")
	k := filepath.Join(dir, "k")
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
		{
			Path:           filepath.Join(acd, "file.cram"),
			NumFiles:       5,
			SizeOfEachFile: 1,
			GID:            gidB,
			UID:            uidB,
			ATime:          90,
			MTime:          90,
		},
		{
			Path:           filepath.Join(k, "file1.cram"),
			NumFiles:       1,
			SizeOfEachFile: 1,
			GID:            gidB,
			UID:            uidA,
			ATime:          refTime - (summary.SecondsInAYear * 3),
			MTime:          refTime - (summary.SecondsInAYear * 7),
		},
		{
			Path:           filepath.Join(k, "file2.cram"),
			NumFiles:       1,
			SizeOfEachFile: 2,
			GID:            gidB,
			UID:            uidA,
			ATime:          refTime - (summary.SecondsInAYear * 1),
			MTime:          refTime - (summary.SecondsInAYear * 2),
		},
		{
			Path:           filepath.Join(k, "file3.cram"),
			NumFiles:       1,
			SizeOfEachFile: 3,
			GID:            gidB,
			UID:            uidA,
			ATime:          refTime - (summary.SecondsInAMonth) - 10,
			MTime:          refTime - (summary.SecondsInAMonth * 2),
		},
		{
			Path:           filepath.Join(k, "file4.cram"),
			NumFiles:       1,
			SizeOfEachFile: 4,
			GID:            gidB,
			UID:            uidA,
			ATime:          refTime - (summary.SecondsInAMonth * 6),
			MTime:          refTime - (summary.SecondsInAYear),
		},
		{
			Path:           filepath.Join(k, "file5.cram"),
			NumFiles:       1,
			SizeOfEachFile: 5,
			GID:            gidB,
			UID:            uidA,
			ATime:          refTime,
			MTime:          refTime,
		},
	}

	if gidC == 0 {
		files = append(files,
			TestFile{
				Path:           filepath.Join(abdij, "file.cram"),
				NumFiles:       1,
				SizeOfEachFile: 1,
				GID:            gidC,
				UID:            uidB,
				ATime:          50,
				MTime:          50,
			},
			TestFile{
				Path:           filepath.Join(abdg, "file.cram"),
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

func TestDGUTAData(t *testing.T, files []TestFile) string {
	t.Helper()

	var sb stringBuilderCloser

	dgutaGen := summary.NewDirGroupUserTypeAge(&sb)
	dguta := dgutaGen().(*summary.DirGroupUserTypeAge)
	doneDirs := make(map[string]bool)

	for _, file := range files {
		addTestFileInfo(t, dguta, doneDirs, file.Path, file.NumFiles,
			file.SizeOfEachFile, file.GID, file.UID, file.ATime, file.MTime)
	}

	err := dguta.Output()
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

func addTestFileInfo(t *testing.T, dguta *summary.DirGroupUserTypeAge, doneDirs map[string]bool,
	path string, numFiles, sizeOfEachFile int, gid, uid uint32, atime, mtime int,
) {
	t.Helper()

	dir, basename := filepath.Split(path)

	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(dir, strconv.FormatInt(int64(i), 10)+basename)

		info := &stats.FileInfo{
			Path:      []byte(filePath),
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

func addTestDirInfo(t *testing.T, dguta *summary.DirGroupUserTypeAge, doneDirs map[string]bool,
	dir string, gid, uid uint32,
) {
	t.Helper()

	for {
		if doneDirs[dir] {
			return
		}

		info := &stats.FileInfo{
			Path:      []byte(dir),
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
