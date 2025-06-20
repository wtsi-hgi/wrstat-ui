package mtimes

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

func TestMTimes(t *testing.T) {
	Convey("Given input files", t, func() {
		a := statsdata.NewRoot("/lustre/scratch120/", 12345)

		a.AddDirectory("userDir").SetMeta(1, 1, 98765)
		statsdata.AddFile(a, "userDir/file1.txt", 1, 1, 0, 0, 98766)
		statsdata.AddFile(a, "userDir/file2.txt", 1, 2, 0, 0, 98767)
		statsdata.AddFile(a, "subDir/subsubDir/file3.txt", 1, 2, 0, 0, 98000)

		a.AddDirectory("other").SetMeta(2, 1, 12349)
		statsdata.AddFile(a, "other/someDir/someFile", 2, 1, 0, 0, 12346)
		statsdata.AddFile(a, "other/someDir/someFile", 2, 1, 0, 0, 12346)

		b := statsdata.NewRoot("/usr/lib/stuff/", 123)
		statsdata.AddFile(b, "fileA.txt", 1, 1, 0, 0, 987)

		Convey("You can build a mtime tree", func() {
			tmp := t.TempDir()
			fileA := filepath.Join(tmp, "fileA")
			fileB := filepath.Join(tmp, "fileB")
			out := filepath.Join(tmp, "out")

			f, err := os.Create(fileA)
			So(err, ShouldBeNil)

			_, err = io.Copy(f, a.AsReader())
			So(err, ShouldBeNil)

			f, err = os.Create(fileB)
			So(err, ShouldBeNil)

			_, err = io.Copy(f, b.AsReader())
			So(err, ShouldBeNil)

			So(Build([]string{fileA, fileB}, out), ShouldBeNil)
		})
	})
}
