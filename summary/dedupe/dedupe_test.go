package dedupe

import (
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

func TestDedupe(t *testing.T) {
	Convey("Dedupe should be able to sort stats.gz data by size, and inode-mountpoint", t, func() {
		f := statsdata.NewRoot("/", 0)
		statsdata.AddFile(f, "opt/teams/teamA/user1/aFile.txt", 0, 0, 300, 0, 0).Inode = 1
		statsdata.AddFile(f, "opt/teams/teamA/user1/bFile.txt", 0, 0, 200, 0, 0)
		statsdata.AddFile(f, "opt/teams/teamA/user2/aFile.txt", 0, 0, 300, 0, 0).Inode = 3
		statsdata.AddFile(f, "opt/teams/teamA/user3/cFile.txt", 0, 0, 300, 0, 0).Inode = 1

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))

		d := &Deduper{}

		s.AddGlobalOperation(d.Operation())

		So(s.Summarise(), ShouldBeNil)

		paths := internaltest.NewDirectoryPathCreator()

		So(slices.Collect(d.Iter), ShouldResemble, []*Node{
			{Path: paths.ToDirectoryPath("/opt/teams/teamA/user1/"), Name: "bFile.txt", Size: 200, Inode: 0},
			{Path: paths.ToDirectoryPath("/opt/teams/teamA/user1/"), Name: "aFile.txt", Size: 300, Inode: 1},
			{Path: paths.ToDirectoryPath("/opt/teams/teamA/user3/"), Name: "cFile.txt", Size: 300, Inode: 1},
			{Path: paths.ToDirectoryPath("/opt/teams/teamA/user2/"), Name: "aFile.txt", Size: 300, Inode: 3},
		})
	})
}
