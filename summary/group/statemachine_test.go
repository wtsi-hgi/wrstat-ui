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

var testGroups = []PathGroup[name]{
	{
		Path:  []byte("/some/path/*"),
		Group: &name{"testGroup[0]"},
	},
	{
		Path:  []byte("/some/path/temp-*"),
		Group: &name{"testGroup[1]"},
	},
	{
		Path:  []byte("/some/path/noBackup/*"),
		Group: &name{"testGroup[2]"},
	},
	{
		Path:  []byte("/some/other/path/*.txt"),
		Group: &name{"testGroup[3]"},
	},
	{
		Path:  []byte("/some/other/path/*.tsv*"),
		Group: &name{"testGroup[4]"},
	},
}

func TestStateMachine(t *testing.T) {
	Convey("With a compiled state machine", t, func() {
		sm, err := NewStatemachine(testGroups)
		So(err, ShouldBeNil)

		Convey("You can get the correct lines for given paths", func() {
			So(sm.getGroup("/some/path"), ShouldEqual, nil)
			So(sm.getGroup("/some/otherPath/"), ShouldEqual, nil)
			So(sm.getGroup("/some/path/"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/file"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/temp-file"), ShouldEqual, testGroups[1].Group)
			So(sm.getGroup("/some/path/noBacku"), ShouldEqual, testGroups[0].Group)
			So(sm.getGroup("/some/path/noBackup/"), ShouldEqual, testGroups[2].Group)
			So(sm.getGroup("/some/path/noBackup/someFile"), ShouldEqual, testGroups[2].Group)
			So(sm.getGroup("/some/other/path/file"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/file.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/file.txta.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txts"), ShouldEqual, nil)
			So(sm.getGroup("/some/other/path/subdir/.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/b.tsvs"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.tsv.txt"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txta.tsv"), ShouldEqual, testGroups[4].Group)
			So(sm.getGroup("/some/other/path/file.tx.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/file.txt.txt"), ShouldEqual, testGroups[3].Group)
			So(sm.getGroup("/some/other/path/subdir/file.txt.tsv"), ShouldEqual, testGroups[4].Group)
		})
	})
}
