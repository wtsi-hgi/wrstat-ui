package group

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func (s StateMachine[T]) getGroup(path string) *T {
	return s[s.getState(1, []byte(path))].Group
}

type name struct {
	name string
}

func TestStateMachine(t *testing.T) {
	Convey("With a compiled state machine", t, func() {
		lines := []PathGroup[name]{
			{
				Path:  []byte("/some/path/*"),
				Group: &name{"line[0]"},
			},
			{
				Path:  []byte("/some/path/temp-*"),
				Group: &name{"line[1]"},
			},
			{
				Path:  []byte("/some/path/noBackup/*"),
				Group: &name{"line[2]"},
			},
			{
				Path:  []byte("/some/other/path/*.txt"),
				Group: &name{"line[3]"},
			},
			{
				Path:  []byte("/some/other/path/*.tsv*"),
				Group: &name{"line[4]"},
			},
		}

		sm, err := NewStatemachine(lines)
		So(err, ShouldBeNil)

		Convey("You can get the correct lines for given paths", func() {
			So(sm.getGroup("/some/path"), ShouldEqual, nil)
			So(sm.getGroup("/some/otherPath/"), ShouldEqual, nil)
			So(sm.getGroup("/some/path/"), ShouldEqual, lines[0].Group)
			So(sm.getGroup("/some/path/file"), ShouldEqual, lines[0].Group)
			So(sm.getGroup("/some/path/temp-file"), ShouldEqual, lines[1].Group)
			So(sm.getGroup("/some/path/noBacku"), ShouldEqual, lines[0].Group)
			So(sm.getGroup("/some/path/noBackup/"), ShouldEqual, lines[2].Group)
			So(sm.getGroup("/some/path/noBackup/someFile"), ShouldEqual, lines[2].Group)
			So(sm.getGroup("/some/other/path/file"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/file.txt"), ShouldEqual, lines[3].Group)
			So(sm.getGroup("/some/other/path/file.txta.txt"), ShouldEqual, lines[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt"), ShouldEqual, lines[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txts"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/subdir/.tsv"), ShouldEqual, lines[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsv"), ShouldEqual, lines[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsvs"), ShouldEqual, lines[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.tsv.txt"), ShouldEqual, lines[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txta.tsv"), ShouldEqual, lines[4].Group)
			So(sm.getGroup("/some/other/path/file.tx.txt"), ShouldEqual, lines[3].Group)
			So(sm.getGroup("/some/other/path/file.txt.txt"), ShouldEqual, lines[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt.tsv"), ShouldEqual, lines[4].Group)
		})
	})
}
