package discovery

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testRequired = "testDIR"

func TestFindDatasetDirs(t *testing.T) {
	Convey("You can find new dataset dirs", t, func() {
		tmp := t.TempDir()

		createFakeDataset(t, tmp, "123_abc")
		b := createFakeDataset(t, tmp, "124_abc")
		c := createFakeDataset(t, tmp, "123_def")
		createFakeDataset(t, tmp, ".124_def")

		found, toDelete, err := findDBDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{c, b})
		So(toDelete, ShouldResemble, []string{"123_abc"})
	})

	Convey("Deletion list is correct when lexicographic order is misleading", t, func() {
		tmp := t.TempDir()

		// os.ReadDir sorts by name; "10_" comes before "9_" lexicographically.
		createFakeDataset(t, tmp, "10_abc")
		older := createFakeDataset(t, tmp, "9_abc")

		found, toDelete, err := findDBDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{filepath.Join(tmp, "10_abc")})
		So(toDelete, ShouldResemble, []string{filepath.Base(older)})
	})
}

func createFakeDataset(t *testing.T, base, name string) string {
	t.Helper()

	p := filepath.Join(base, name)

	So(os.Mkdir(p, 0o700), ShouldBeNil)
	So(os.Mkdir(filepath.Join(p, testRequired), 0o700), ShouldBeNil)

	return p
}
