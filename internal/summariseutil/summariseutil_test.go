package summariseutil

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseMountpointsFromFile(t *testing.T) {
	Convey("ParseMountpointsFromFile parses quoted mountpoints", t, func() {
		tmp := t.TempDir()
		p := filepath.Join(tmp, "mounts.txt")

		data := []byte("\"/nfs/\"\n\"/lustre/\"\n\n")
		So(os.WriteFile(p, data, 0o600), ShouldBeNil)

		mps, err := ParseMountpointsFromFile(p)
		So(err, ShouldBeNil)
		So(mps, ShouldResemble, []string{"/nfs/", "/lustre/"})
	})

	Convey("ParseMountpointsFromFile returns nil for empty path", t, func() {
		mps, err := ParseMountpointsFromFile("")
		So(err, ShouldBeNil)
		So(mps, ShouldBeNil)
	})
}
