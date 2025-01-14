/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
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

package internaltest

import (
	"io/fs"
	"slices"
	"strconv"
	"strings"

	. "github.com/smartystreets/goconvey/convey" //nolint:revive,stylecheck
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// DirectoryPathCreator stores cached summary.DirectoryPaths based on their full
// path.
type DirectoryPathCreator map[string]*summary.DirectoryPath

// ToDirectoryPath creates a summary.DirectoryPath for the given full path,
// reusing existing parents.
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
		Depth:  parent.Depth + 1,
		Parent: parent,
	}

	d[p] = dp

	return dp
}

// NewDirectoryPathCreator allows for the simple creation of
// summary.DirectoryPath structures.
func NewDirectoryPathCreator() DirectoryPathCreator {
	d := make(DirectoryPathCreator)

	d["/"] = &summary.DirectoryPath{
		Name:  "/",
		Depth: 0,
	}

	return d
}

// StringBuilder gives the strings.Builder type a no-op Close method.
type StringBuilder struct {
	strings.Builder
}

// Close is a no-op Close method to satisfy the io.Closer interface.
func (StringBuilder) Close() error {
	return nil
}

// BadWriter is a io.riteCloser that always returns that it is closed.
type BadWriter struct{}

// Write is a method that always returns 0, fs.ErrClosed.
func (BadWriter) Write([]byte) (int, error) {
	return 0, fs.ErrClosed
}

// Close is a method that always returns fs.ErrClosed.
func (BadWriter) Close() error {
	return fs.ErrClosed
}

// NewMockInfo is a function that creates a summary.FileInfo from a
// summary.DirectoryPath and additional info.
func NewMockInfo(path *summary.DirectoryPath, uid, gid uint32, size int64, dir bool) *summary.FileInfo {
	entryType := stats.FileType

	if dir {
		entryType = stats.DirType
	}

	var name string

	if path != nil {
		name = path.Name
	}

	return &summary.FileInfo{
		Path:      path,
		Name:      []byte(name),
		UID:       uid,
		GID:       gid,
		Size:      size,
		EntryType: byte(entryType),
	}
}

// NewMockInfoWithAtime is similar to NewMockInfo but you can also provide an
// atime.
func NewMockInfoWithAtime(path *summary.DirectoryPath, uid, gid uint32,
	size int64, dir bool, atime int64) *summary.FileInfo {
	mi := NewMockInfo(path, uid, gid, size, dir)
	mi.ATime = atime

	return mi
}

// NewMockInfoWithTimes is similar to NewMockInfo but you can also provide an
// atime, mtime, and ctime.
func NewMockInfoWithTimes(path *summary.DirectoryPath, uid, gid uint32,
	size int64, dir bool, tim int64) *summary.FileInfo {
	mi := NewMockInfo(path, uid, gid, size, dir)
	mi.ATime = tim
	mi.MTime = tim
	mi.CTime = tim

	return mi
}

// CheckDataIsSorted returns true if the data provided is sorted. The first
// textCols columns will be sorted as text, the remaining as integers.
func CheckDataIsSorted(data string, textCols int) bool { //nolint:gocognit
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

			colA, _ := strconv.ParseInt(col, 10, 0)  //nolint:errcheck
			colB, _ := strconv.ParseInt(b[n], 10, 0) //nolint:errcheck

			if dx := colA - colB; dx != 0 {
				return int(dx)
			}
		}

		return 0
	})
}

// TestBadIDs tests how a summary.Operation responds to invalid ids.
func TestBadIDs(err error, a summary.Operation, w *StringBuilder) {
	So(err, ShouldBeNil)

	err = a.Output()
	So(err, ShouldBeNil)

	output := w.String()

	So(output, ShouldContainSubstring, "id999999999")
}
