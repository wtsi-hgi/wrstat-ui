package backups

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStateMachine(t *testing.T) {
	Convey("With a compiled state machine", t, func() {
		lines := []*Line{
			{
				Path: []byte("/some/path/*"),
				name: "line[0]",
			},
			{
				Path: []byte("/some/path/temp-*"),
				name: "line[1]",
			},
			{
				Path: []byte("/some/path/noBackup/*"),
				name: "line[2]",
			},
			{
				Path: []byte("/some/other/path/*.txt"),
				name: "line[3]",
			},
			{
				Path: []byte("/some/other/path/*.tsv*"),
				name: "line[4]",
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
			So(sm.GetLine([]byte("/some/other/path/file.txta.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txts")), ShouldEqual, nil)
			So(sm.GetLine([]byte("/some/other/path/subdir/.tsv")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/b.tsv")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/b.tsvs")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.tsv.txt")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txta.tsv")), ShouldEqual, lines[4])
			So(sm.GetLine([]byte("/some/other/path/file.tx.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/file.txt.txt")), ShouldEqual, lines[3])
			So(sm.GetLine([]byte("/some/other/path/subdir/file.txt.tsv")), ShouldEqual, lines[4])
		})
	})
}
