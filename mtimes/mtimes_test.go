package mtimes

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

func TestMTimes(t *testing.T) {
	Convey("Given input files", t, func() {
		a := statsdata.NewRoot("/opt/", 12345)

		a.AddDirectory("userDir").SetMeta(1, 1, 98765)
		statsdata.AddFile(a, "userDir/file1.txt", 1, 1, 0, 0, 98766)
		statsdata.AddFile(a, "userDir/file2.txt", 1, 2, 0, 0, 98767)
		statsdata.AddFile(a, "subDir/subsubDir/file3.txt", 1, 2, 0, 0, 98000)

		a.AddDirectory("other").SetMeta(2, 1, 12349)
		statsdata.AddFile(a, "other/someDir/someFile", 2, 1, 0, 0, 12346)
		statsdata.AddFile(a, "other/someDir/someFile", 2, 1, 0, 0, 12346)

		b := statsdata.NewRoot("/usr/lib/stuff/", 123)
		statsdata.AddFile(b, "fileA.txt", 1, 1, 0, 0, 987)

		Convey("The parent directories are filled in", func() {
			var (
				buf bytes.Buffer
				err error
			)

			files := make([]timeFile, 2)

			files[0].path, err = readFirstPath(a.AsReader())
			So(err, ShouldBeNil)

			files[1].path, err = readFirstPath(b.AsReader())
			So(err, ShouldBeNil)

			files[0].Reader = a.AsReader()
			files[1].Reader = b.AsReader()

			sortFiles(files)

			r, err := mergeFiles(files)
			So(err, ShouldBeNil)

			io.Copy(&buf, r)

			So(buf.String(), ShouldEqual,
				"\"/\"\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n"+
					"\"/opt/\"\t4096\t0\t0\t12345\t12345\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/other/\"\t4096\t2\t1\t12345\t12349\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/other/someDir/\"\t4096\t2\t1\t12345\t12349\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/other/someDir/someFile\"\t0\t2\t1\t0\t12346\t12345\tf\t0\t1\t1\t0\n"+
					"\"/opt/subDir/\"\t4096\t0\t0\t12345\t12345\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/subDir/subsubDir/\"\t4096\t0\t0\t12345\t12345\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/subDir/subsubDir/file3.txt\"\t0\t1\t2\t0\t98000\t12345\tf\t0\t1\t1\t0\n"+
					"\"/opt/userDir/\"\t4096\t1\t1\t12345\t98765\t12345\td\t0\t1\t1\t4096\n"+
					"\"/opt/userDir/file1.txt\"\t0\t1\t1\t0\t98766\t12345\tf\t0\t1\t1\t0\n"+
					"\"/opt/userDir/file2.txt\"\t0\t1\t2\t0\t98767\t12345\tf\t0\t1\t1\t0\n"+
					"\"/usr/\"\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n"+
					"\"/usr/lib/\"\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n"+
					"\"/usr/lib/stuff/\"\t4096\t0\t0\t123\t123\t123\td\t0\t1\t1\t4096\n"+
					"\"/usr/lib/stuff/fileA.txt\"\t0\t1\t1\t0\t987\t123\tf\t0\t1\t1\t0\n")
		})

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
