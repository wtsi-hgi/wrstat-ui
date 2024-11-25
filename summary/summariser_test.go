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

func (t *testGlobalOperator) Add(s *FileInfo) error {
	t.totalCount++

	if s.EntryType == 'f' {
		dir := s.Path.appendTo(nil)

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

func (t *testDirectoryOperator) Add(s *FileInfo) error {
	if t.path == "" {
		t.path = string(s.Path.appendTo(nil))
	}

	t.size += s.Size

	return nil
}

func (t *testDirectoryOperator) Output() error {
	t.outputMap[t.path] = t.size

	t.path = ""
	t.size = 0

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
			s.AddGlobalOperation(func() Operation { return so })

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(so.dirCounts["/opt/dir1/"], ShouldEqual, 4)
		})

		Convey("You can add multiple operations and they run sequentially", func() {
			so := &testGlobalOperator{dirCounts: make(map[string]int)}
			so2 := &testGlobalOperator{dirCounts: make(map[string]int)}
			s.AddGlobalOperation(func() Operation { return so })
			s.AddGlobalOperation(func() Operation { return so2 })

			outputMap := make(map[string]int64)
			outputMap2 := make(map[string]int64)

			s.AddDirectoryOperation(func() Operation {
				return &testDirectoryOperator{outputMap: outputMap}
			})
			s.AddDirectoryOperation(func() Operation {
				return &testDirectoryOperator{outputMap: outputMap2}
			})

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(so.totalCount, ShouldEqual, 651)
			So(so2.totalCount, ShouldEqual, 651)
			So(outputMap["/opt/dir0/dir0/dir0/dir0/dir0/"], ShouldEqual, 4096)
			So(outputMap["/opt/dir0/dir0/dir0/dir1/"], ShouldEqual, 8193)
			So(outputMap2["/opt/dir0/dir0/dir0/dir0/dir0/"], ShouldEqual, 4096)
			So(outputMap2["/opt/dir0/dir0/dir0/dir1/"], ShouldEqual, 8193)
		})
	})
}
