package internaltest

import (
	"io/fs"
	"slices"
	"strconv"
	"strings"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

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

type StringBuilder struct {
	strings.Builder
}

func (StringBuilder) Close() error {
	return nil
}

type BadWriter struct{}

func (BadWriter) Write([]byte) (int, error) {
	return 0, fs.ErrClosed
}

func (BadWriter) Close() error {
	return fs.ErrClosed
}

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

func NewMockInfoWithAtime(path *summary.DirectoryPath, uid, gid uint32, size int64, dir bool, atime int64) *summary.FileInfo {
	mi := NewMockInfo(path, uid, gid, size, dir)
	mi.ATime = atime

	return mi
}

func NewMockInfoWithTimes(path *summary.DirectoryPath, uid, gid uint32, size int64, dir bool, tim int64) *summary.FileInfo {
	mi := NewMockInfo(path, uid, gid, size, dir)
	mi.ATime = tim
	mi.MTime = tim
	mi.CTime = tim

	return mi
}

func CheckDataIsSorted(data string, textCols int) bool {
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

func TestBadIds(err error, a summary.Operation, w *StringBuilder) {
	So(err, ShouldBeNil)

	err = a.Output()
	So(err, ShouldBeNil)

	output := w.String()

	So(output, ShouldContainSubstring, "id999999999")
}
