package group

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type testHandler map[*name][]string

func (t testHandler) Handle(file *summary.FileInfo, group *name) error {
	if file.EntryType != 'f' {
		return nil
	}

	t[group] = append(t[group], string(append(file.Path.AppendTo(nil), file.Name...)))

	return nil
}

func TestGrouper(t *testing.T) {
	Convey("Given a Statemachine and stats file data", t, func() {
		sm, err := NewStatemachine(testGroups)
		So(err, ShouldBeNil)

		f := statsdata.NewRoot("/", 0)
		f.AddFile("some/file")
		f.AddFile("some/path/a-file")
		f.AddFile("some/path/b-file")
		f.AddFile("some/path/temp-file")

		sp := stats.NewStatsParser(f.AsReader())

		Convey("You can match files to their correct group", func() {
			output := make(testHandler)

			s := summary.NewSummariser(sp)
			s.AddGlobalOperation(NewGrouper(sm, output))

			So(s.Summarise(), ShouldBeNil)
			So(output, ShouldResemble, testHandler{
				nil:                 {"/some/file"},
				testGroups[0].Group: {"/some/path/a-file", "/some/path/b-file"},
				testGroups[1].Group: {"/some/path/temp-file"},
			})
		})
	})
}
