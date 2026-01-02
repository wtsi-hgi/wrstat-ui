package bolt

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

func TestOpenMultiBaseDirsReader(t *testing.T) {
	Convey("OpenMultiBaseDirsReader returns a basedirs.Reader aggregating across DBs", t, func() {
		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		pathA := dir + "/a.basedirs.db"
		pathB := dir + "/b.basedirs.db"

		tA := time.Unix(100, 0)
		tB := time.Unix(200, 0)

		storeA, err := NewBaseDirsStore(pathA, "")
		So(err, ShouldBeNil)
		storeA.SetMountPath("/mnt/a/")
		storeA.SetUpdatedAt(tA)
		So(storeA.Reset(), ShouldBeNil)
		So(storeA.PutGroupUsage(&basedirs.Usage{GID: 1, BaseDir: "/mnt/a/projects/A", Age: db.DGUTAgeAll, UsageSize: 1}), ShouldBeNil)
		So(storeA.AppendGroupHistory(basedirs.HistoryKey{GID: 1, MountPath: "/mnt/a/"}, basedirs.History{Date: tA}), ShouldBeNil)
		So(storeA.PutGroupSubDirs(basedirs.SubDirKey{ID: 1, BaseDir: "/mnt/a/projects/A", Age: db.DGUTAgeAll}, []*basedirs.SubDir{{SubDir: "x"}}), ShouldBeNil)
		So(storeA.Finalise(), ShouldBeNil)
		So(storeA.Close(), ShouldBeNil)

		storeB, err := NewBaseDirsStore(pathB, "")
		So(err, ShouldBeNil)
		storeB.SetMountPath("/mnt/b/")
		storeB.SetUpdatedAt(tB)
		So(storeB.Reset(), ShouldBeNil)
		So(storeB.PutGroupUsage(&basedirs.Usage{GID: 2, BaseDir: "/mnt/b/projects/B", Age: db.DGUTAgeAll, UsageSize: 2}), ShouldBeNil)
		So(storeB.AppendGroupHistory(basedirs.HistoryKey{GID: 2, MountPath: "/mnt/b/"}, basedirs.History{Date: tB}), ShouldBeNil)
		So(storeB.PutGroupSubDirs(basedirs.SubDirKey{ID: 2, BaseDir: "/mnt/b/projects/B", Age: db.DGUTAgeAll}, []*basedirs.SubDir{{SubDir: "y"}}), ShouldBeNil)
		So(storeB.Finalise(), ShouldBeNil)
		So(storeB.Close(), ShouldBeNil)

		r, err := OpenMultiBaseDirsReader(ownersPath, pathA, pathB)
		So(err, ShouldBeNil)
		defer r.Close()

		Convey("usage concatenates across sources", func() {
			gu, err := r.GroupUsage(db.DGUTAgeAll)
			So(err, ShouldBeNil)
			So(len(gu), ShouldEqual, 2)
		})

		Convey("subdirs returns first successful result", func() {
			// gid=2 exists only in B.
			s, err := r.GroupSubDirs(2, "/mnt/b/projects/B", db.DGUTAgeAll)
			So(err, ShouldBeNil)
			So(len(s), ShouldEqual, 1)
			So(s[0].SubDir, ShouldEqual, "y")
		})

		Convey("timestamps merge per mount key", func() {
			ts, err := r.MountTimestamps()
			So(err, ShouldBeNil)
			So(ts["／mnt／a／"], ShouldEqual, tA)
			So(ts["／mnt／b／"], ShouldEqual, tB)
		})

		Convey("Info sums counts", func() {
			info, err := r.Info()
			So(err, ShouldBeNil)
			So(info.GroupDirCombos, ShouldEqual, 2)
		})
	})

	Convey("OpenMultiBaseDirsReader errors with no db paths", t, func() {
		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		r, err := OpenMultiBaseDirsReader(ownersPath)
		So(err, ShouldNotBeNil)
		So(r, ShouldBeNil)
	})
}
