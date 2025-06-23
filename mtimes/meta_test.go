package mtimes

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"vimagination.zapto.org/tree"
)

func TestMeta(t *testing.T) {
	Convey("With a mtime tree", t, func() {
		data := statsdata.NewRoot("/some/path/", 12345)

		data.AddDirectory("userDir").SetMeta(1, 1, 98765)
		statsdata.AddFile(data, "userDir/file1.txt", 1, 1, 0, 0, 98766)
		statsdata.AddFile(data, "userDir/file2.txt", 1, 2, 0, 0, 98767)
		statsdata.AddFile(data, "subDir/subsubDir/file3.txt", 1, 2, 0, 0, 98000)

		tmp := t.TempDir()
		file := filepath.Join(tmp, "data")
		out := filepath.Join(tmp, "tree.db")

		f, err := os.Create(file)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, data.AsReader())
		So(err, ShouldBeNil)

		So(Build([]string{file}, out), ShouldBeNil)

		Convey("You can retrieve the metadata of files and directories", func() {
			tree, err := tree.OpenFile(out)
			So(err, ShouldBeNil)

			m, err := GetMeta(tree, "/some/path/userDir/file1.txt")
			So(err, ShouldBeNil)
			So(m, ShouldResemble, Meta{UIDs: []IDTime{{1, 98766}}, GIDs: []IDTime{{1, 98766}}})

			m, err = GetMeta(tree, "/some/path/subDir/")
			So(err, ShouldBeNil)
			So(m, ShouldResemble, Meta{UIDs: []IDTime{{1, 98000}}, GIDs: []IDTime{{2, 98000}}})

			m, err = GetMeta(tree, "/some/path/")
			So(err, ShouldBeNil)
			So(m, ShouldResemble, Meta{UIDs: []IDTime{{1, 98767}}, GIDs: []IDTime{{1, 98766}, {2, 98767}}})
		})
	})
}
