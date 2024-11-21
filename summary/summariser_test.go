package summary

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

type testGlobalOperator struct {
	dirCounts  map[string]int
	totalCount int
}

func (testGlobalOperator) New() Operation {
	return new(testGlobalOperator)
}

func (t *testGlobalOperator) Add(s *stats.FileInfo) error {
	if t.dirCounts == nil {
		return nil
	}

	t.totalCount++

	if s.EntryType == 'f' {
		dir := parentDir(s.Path)

		t.dirCounts[string(dir)] = t.dirCounts[string(dir)] + 1
	}

	return nil
}

func (testGlobalOperator) Output() error {
	return nil
}

type testDirectoryOperator struct {
	outputMap map[string]int64
	path      string
	size      int64
}

func (t *testDirectoryOperator) New() Operation {
	return &testDirectoryOperator{
		outputMap: t.outputMap,
	}
}

func (t *testDirectoryOperator) Add(s *stats.FileInfo) error {
	if t.path == "" {
		t.path = string(s.Path)
	}

	t.size += s.Size

	return nil
}

func (t *testDirectoryOperator) Output() error {
	t.outputMap[t.path] = t.size

	return nil
}

func TestParse(t *testing.T) {
	Convey("Given some stats data and a parser, you can make a summariser", t, func() {
		refTime := time.Now().Unix()
		f := statsdata.TestStats(5, 5, "/opt/", refTime).AsReader()
		p := stats.NewStatsParser(f)
		s := NewSummariser(p)

		Convey("You can add an operation and have it apply over every line of data", func() {
			so := &testGlobalOperator{dirCounts: make(map[string]int)}
			s.AddOperation(so)

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(so.dirCounts["/opt/dir1/"], ShouldEqual, 4)
		})

		Convey("You can add multiple operations and they run sequentially", func() {
			so := &testGlobalOperator{dirCounts: make(map[string]int)}
			s.AddOperation(so)

			do := &testDirectoryOperator{outputMap: make(map[string]int64)}
			s.AddOperation(do)

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(do.outputMap["/opt/dir0/dir0/dir0/dir0/dir0/"], ShouldEqual, 4096)
			So(do.outputMap["/opt/dir0/dir0/dir0/dir1/"], ShouldEqual, 8193)
		})
	})
}
