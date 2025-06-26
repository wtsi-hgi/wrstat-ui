package backups

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStateMachine(t *testing.T) {
	Convey("With a compiled state machine", t, func() {
		lines := []*Line{
			{
				Path:   []byte("/some/path/*"),
				action: actionBackup,
			},
			{
				Path:   []byte("/some/path/temp-*"),
				action: actionTempBackup,
			},
			{
				Path:   []byte("/some/path/noBackup/*"),
				action: actionNoBackup,
			},
			{
				Path:   []byte("/some/other/path/*.txt"),
				action: actionBackup,
			},
			{
				Path:   []byte("/some/other/path/*.tsv*"),
				action: actionTempBackup,
			},
		}

		sm, err := NewStatemachine(lines)
		So(err, ShouldBeNil)

		Convey("You can get the correct lines for given paths", func() {
			So(sm.GetLine([]byte("/some/path")), ShouldEqual, nil)
			So(sm.GetLine([]byte("/some/otherPath/")), ShouldEqual, nil)
			So(sm.GetLine([]byte("/some/path/")), ShouldEqual, lines[0])
			So(sm.GetLine([]byte("/some/path/file")), ShouldEqual, lines[0])
			So(sm.GetLine([]byte("/some/path/temp-file")), ShouldEqual, lines[1])
			So(sm.GetLine([]byte("/some/path/noBacku")), ShouldEqual, lines[0])
			So(sm.GetLine([]byte("/some/path/noBackup/")), ShouldEqual, lines[2])
			So(sm.GetLine([]byte("/some/path/noBackup/someFile")), ShouldEqual, lines[2])
			So(sm.GetLine([]byte("/some/other/path/file")), ShouldEqual, nil)
			So(sm.GetLine([]byte("/some/other/path/file.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txts")), ShouldEqual, nil)
			So(sm.GetLine([]byte("/some/other/path/subdir/.tsv")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/b.tsv")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/b.tsvs")), ShouldEqual, lines[4])
		})
	})
}
