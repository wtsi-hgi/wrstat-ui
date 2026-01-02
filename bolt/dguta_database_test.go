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

func TestDGUTADatabase(t *testing.T) {
	Convey("DGUTA database uses persisted updatedAt", t, func() {
		dgutaDir := filepath.Join(t.TempDir(), "dguta.dbs")
		So(os.MkdirAll(dgutaDir, 0o755), ShouldBeNil)

		w, err := NewDGUTAWriter(dgutaDir)
		So(err, ShouldBeNil)

		updatedAt := time.Unix(999, 0)

		w.SetMountPath("/a/")
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		rec := db.RecordDGUTA{
			Dir: paths.ToDirectoryPath("/"),
			GUTAs: db.GUTAs{
				&db.GUTA{GID: 1, UID: 2, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll, Count: 1, Size: 1, Atime: 1, Mtime: 2},
			},
		}
		So(w.Add(rec), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		database, err := openDGUTADatabase([]string{dgutaDir})
		So(err, ShouldBeNil)

		defer database.Close()

		ds, err := database.DirInfo("/", nil)
		So(err, ShouldBeNil)
		So(ds, ShouldNotBeNil)
		So(ds.Modtime, ShouldResemble, updatedAt)
	})
}
