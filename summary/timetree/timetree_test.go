package timetree

import (
	"bytes"
	"io"
	"maps"
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"vimagination.zapto.org/byteio"
	"vimagination.zapto.org/tree"
)

func TestTimeTree(t *testing.T) {
	Convey("With a summarised time tree", t, func() {
		f := statsdata.NewRoot("/", 12345)

		f.AddDirectory("opt").AddDirectory("userDir").SetMeta(1, 1, 98765)
		statsdata.AddFile(f, "opt/userDir/file1.txt", 1, 1, 0, 0, 98766)
		statsdata.AddFile(f, "opt/userDir/file2.txt", 1, 2, 0, 0, 98767)
		statsdata.AddFile(f, "opt/subDir/subsubDir/file3.txt", 1, 2, 0, 0, 98000)

		f.AddDirectory("opt").AddDirectory("other").SetMeta(2, 1, 12349)
		statsdata.AddFile(f, "opt/other/someDir/someFile", 2, 1, 0, 0, 12346)
		statsdata.AddFile(f, "opt/other/someDir/someFile", 2, 1, 0, 0, 12346)

		p := stats.NewStatsParser(f.AsReader())
		s := summary.NewSummariser(p)

		var treeDB bytes.Buffer

		complete := make(chan struct{})

		s.AddDirectoryOperation(NewTimeTree(&treeDB, complete))

		So(s.Summarise(), ShouldBeNil)

		<-complete

		tr, err := tree.OpenMem(treeDB.Bytes())
		So(err, ShouldBeNil)

		Convey("You can read a root summary", func() {
			r := bytes.NewReader(tr.Data())

			userSummary, groupSummary := readSummary(r)
			So(userSummary, ShouldResemble, []IDTime{
				{1, 98767},
				{2, 12346},
			})
			So(groupSummary, ShouldResemble, []IDTime{
				{1, 98766},
				{2, 98767},
			})

			So(slices.Sorted(maps.Keys(maps.Collect(tr.Children()))), ShouldResemble, []string{
				"opt/",
			})
		})

		Convey("You can read a subdirectory summary", func() {
			tr, err = tr.Child("opt/")
			So(err, ShouldBeNil)

			r := bytes.NewReader(tr.Data())

			userSummary, groupSummary := readSummary(r)
			So(userSummary, ShouldResemble, []IDTime{
				{1, 98767},
				{2, 12346},
			})
			So(groupSummary, ShouldResemble, []IDTime{
				{1, 98766},
				{2, 98767},
			})

			So(slices.Sorted(maps.Keys(maps.Collect(tr.Children()))), ShouldResemble, []string{
				"other/", "subDir/", "userDir/",
			})
		})

		Convey("You can read the ownership and mtime for a file", func() {
			tr, err = tr.Child("opt/")
			So(err, ShouldBeNil)

			tr, err = tr.Child("userDir/")
			So(err, ShouldBeNil)

			tr, err = tr.Child("file1.txt")
			So(err, ShouldBeNil)

			r := bytes.NewReader(tr.Data())

			rootUID, rootGID, rootMtime := readMeta(r)
			So(rootUID, ShouldEqual, 1)
			So(rootGID, ShouldEqual, 1)
			So(rootMtime, ShouldEqual, 98766)
		})
	})
}

func readMeta(r io.Reader) (uint32, uint32, int64) {
	lr := byteio.StickyLittleEndianReader{Reader: r}

	return lr.ReadUint32(), lr.ReadUint32(), int64(lr.ReadUint32())
}

func readSummary(r io.Reader) ([]IDTime, []IDTime) {
	lr := byteio.StickyLittleEndianReader{Reader: r}

	return readArray(&lr), readArray(&lr)
}

func readArray(lr *byteio.StickyLittleEndianReader) []IDTime {
	idts := make([]IDTime, lr.ReadUintX())

	for n := range idts {
		idts[n].ID = lr.ReadUint32()
		idts[n].Time = lr.ReadUint32()
	}

	return idts
}
