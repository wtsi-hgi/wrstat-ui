package bolt

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

func TestDGUTAWriter(t *testing.T) {
	Convey("NewDGUTAWriter validates output directory", t, func() {
		Convey("it errors when outputDir does not exist", func() {
			outputDir := filepath.Join(t.TempDir(), "missing")
			w, err := NewDGUTAWriter(outputDir)
			So(err, ShouldNotBeNil)
			So(w, ShouldBeNil)
		})
	})

	Convey("DGUTAWriter enforces required metadata", t, func() {
		outputDir := filepath.Join(t.TempDir(), "dguta.dbs")
		So(os.MkdirAll(outputDir, 0o755), ShouldBeNil)

		w, err := NewDGUTAWriter(outputDir)
		So(err, ShouldBeNil)
		So(w, ShouldNotBeNil)

		paths := internaltest.NewDirectoryPathCreator()
		err = w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/"), GUTAs: nil})
		So(err, ShouldNotBeNil)

		w.SetMountPath("/a/")
		w.SetUpdatedAt(time.Unix(123, 0))
		err = w.Add(db.RecordDGUTA{Dir: paths.ToDirectoryPath("/"), GUTAs: nil})
		So(err, ShouldBeNil)

		So(w.Close(), ShouldBeNil)
	})
}
