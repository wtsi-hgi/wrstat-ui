package datatree

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
		statsdata.AddFile(f, "opt/userDir/file1.txt", 1, 1, 9, 0, 98766)
		statsdata.AddFile(f, "opt/userDir/file2.txt", 1, 2, 8, 0, 98767)
		statsdata.AddFile(f, "opt/subDir/subsubDir/file3.txt", 1, 2, 7, 0, 98000)

		f.AddDirectory("opt").AddDirectory("other").SetMeta(2, 1, 12349)
		statsdata.AddFile(f, "opt/other/someDir/someFile", 2, 1, 6, 0, 12346)
		statsdata.AddFile(f, "opt/other/someDir/someFile", 2, 1, 5, 0, 12346)

		p := stats.NewStatsParser(f.AsReader())
		s := summary.NewSummariser(p)

		var treeDB bytes.Buffer

		s.AddDirectoryOperation(NewTree(&treeDB))

		So(s.Summarise(), ShouldBeNil)

		tr, err := tree.OpenMem(treeDB.Bytes())
		So(err, ShouldBeNil)

		Convey("You can read a root summary", func() {
			r := bytes.NewReader(tr.Data())

			userSummary, groupSummary := readSummary(r)
			So(userSummary, ShouldResemble, []IDData{
				{1, &Meta{MTime: 98767, Files: 3, Bytes: 24}},
				{2, &Meta{MTime: 12346, Files: 1, Bytes: 5}},
			})
			So(groupSummary, ShouldResemble, []IDData{
				{1, &Meta{MTime: 98766, Files: 2, Bytes: 14}},
				{2, &Meta{MTime: 98767, Files: 2, Bytes: 15}},
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
			So(userSummary, ShouldResemble, []IDData{
				{1, &Meta{MTime: 98767, Files: 3, Bytes: 24}},
				{2, &Meta{MTime: 12346, Files: 1, Bytes: 5}},
			})
			So(groupSummary, ShouldResemble, []IDData{
				{1, &Meta{MTime: 98766, Files: 2, Bytes: 14}},
				{2, &Meta{MTime: 98767, Files: 2, Bytes: 15}},
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

			rootUID, rootGID, rootMtime, rootBytes := readMeta(r)
			So(rootUID, ShouldEqual, 1)
			So(rootGID, ShouldEqual, 1)
			So(rootMtime, ShouldEqual, 98766)
			So(rootBytes, ShouldEqual, 9)
		})
	})
}

func readMeta(r io.Reader) (uint32, uint32, int64, uint64) {
	lr := byteio.StickyLittleEndianReader{Reader: r}

	return lr.ReadUint32(), lr.ReadUint32(), int64(lr.ReadUint32()), lr.ReadUint64()
}

func readSummary(r io.Reader) ([]IDData, []IDData) {
	lr := byteio.StickyLittleEndianReader{Reader: r}

	return readArray(&lr), readArray(&lr)
}

func readArray(lr *byteio.StickyLittleEndianReader) []IDData {
	idts := make([]IDData, lr.ReadUintX())

	for n := range idts {
		idts[n].ID = lr.ReadUint32()
		idts[n].Meta = new(Meta)
		idts[n].MTime = lr.ReadUint32()
		idts[n].Files = lr.ReadUint32()
		idts[n].Bytes = lr.ReadUint64()
	}

	return idts
}
