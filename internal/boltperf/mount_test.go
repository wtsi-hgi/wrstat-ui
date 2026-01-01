package boltperf

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDeriveMountPathFromDatasetDirName(t *testing.T) {
	Convey("DeriveMountPathFromDatasetDirName normalises fullwidth slashes and adds trailing slash", t, func() {
		m, err := DeriveMountPathFromDatasetDirName("1_／lustre")
		So(err, ShouldBeNil)
		So(m, ShouldEqual, "/lustre/")
	})

	Convey("DeriveMountPathFromDatasetDirName preserves existing trailing slash", t, func() {
		m, err := DeriveMountPathFromDatasetDirName("2_/nfs/")
		So(err, ShouldBeNil)
		So(m, ShouldEqual, "/nfs/")
	})

	Convey("DeriveMountPathFromDatasetDirName errors without underscore", t, func() {
		_, err := DeriveMountPathFromDatasetDirName("bad")
		So(err, ShouldNotBeNil)
		So(err, ShouldWrap, ErrDatasetDirMissingUnderscore)
	})
}
