package basedirs

import (
	"maps"
	"slices"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type mockDB struct {
	users, groups basedirs.IDAgeDirs
}

func (m *mockDB) Output(users, groups basedirs.IDAgeDirs) error {
	m.users = users
	m.groups = groups

	return nil
}

func ageDirs(all []basedirs.SummaryWithChildren, a *basedirs.AgeDirs, m *basedirs.AgeDirs) *basedirs.AgeDirs {
	a[0] = all
	copy(a[len(db.AgeThresholds):], m[len(db.AgeThresholds):])

	return a
}

func a(dirs ...[]basedirs.SummaryWithChildren) *basedirs.AgeDirs {
	return setdirs(dirs, 1)
}

func m(dirs ...[]basedirs.SummaryWithChildren) *basedirs.AgeDirs {
	return setdirs(dirs, len(db.AgeThresholds)+1)
}

func setdirs(dirs [][]basedirs.SummaryWithChildren, offset int) *basedirs.AgeDirs {
	a := new(basedirs.AgeDirs)

	for n, dir := range dirs {
		d := slices.Clone(dir)

		for m := range d {
			d[m].Age = db.DirGUTAge(n + offset)
		}

		a[n+offset] = d
	}

	return a
}

func dir(name string, lastMod int64, numFiles int64, files basedirs.UsageBreakdownByType) *basedirs.SubDir {
	var size uint64

	for ft, s := range files {
		if ft != db.DGUTAFileTypeTemp {
			size += s
		}
	}

	return &basedirs.SubDir{
		SubDir:       name,
		NumFiles:     uint64(numFiles),
		SizeFiles:    size,
		LastModified: time.Unix(lastMod, 0),
		FileUsage:    files,
	}
}

func userSummary(path string, uid uint32, gids []uint32, atime int64, children ...*basedirs.SubDir) basedirs.SummaryWithChildren {
	return dirsummary(path, []uint32{uid}, gids, atime, children)
}

func dirsummary(path string, uids []uint32, gids []uint32, atime int64, children []*basedirs.SubDir) basedirs.SummaryWithChildren {
	ftsMap := make(map[db.DirGUTAFileType]struct{})

	var size, num uint64
	var mod time.Time

	for _, c := range children {
		size += c.SizeFiles
		num += c.NumFiles

		if mod.IsZero() || mod.Before(c.LastModified) {
			mod = c.LastModified
		}

		for ft := range c.FileUsage {
			ftsMap[ft] = struct{}{}
		}
	}

	fts := slices.Collect(maps.Keys(ftsMap))

	slices.Sort(fts)

	s := basedirs.SummaryWithChildren{
		DirSummary: db.DirSummary{
			Dir:   path,
			Count: num,
			Size:  size,
			UIDs:  uids,
			GIDs:  gids,
			Atime: time.Unix(atime, 0),
			Mtime: mod,
			FTs:   fts,
		},
		Children: children,
	}

	return s
}

func groupSummary(path string, uids []uint32, gid uint32, atime int64, children ...*basedirs.SubDir) basedirs.SummaryWithChildren {
	return dirsummary(path, uids, []uint32{gid}, atime, children)
}

func ids(ids ...uint32) []uint32 {
	return ids
}

type files = basedirs.UsageBreakdownByType

func TestBaseDirs(t *testing.T) {
	Convey("With a basedirs summariser", t, func() {
		const dt = db.SecondsInAMonth >> 1

		var times [len(db.AgeThresholds)]int64

		now := time.Now().Unix()

		for n := range times {
			times[n] = now + dt - db.AgeThresholds[n]
		}

		f := statsdata.NewRoot("/", 0)
		statsdata.AddFile(f, "opt/teams/teamA/user1/aFile.txt", 1, 10, 3, times[3], times[1])
		statsdata.AddFile(f, "opt/teams/teamA/user2/aDir/aFile.bam", 2, 11, 5, times[2], times[1])
		statsdata.AddFile(f, "opt/teams/teamA/user2/bDir/bFile.gz", 2, 11, 7, times[3], times[1])
		statsdata.AddFile(f, "opt/teams/teamB/user3/aDir/bDir/cDir/aFile.vcf", 3, 12, 11, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user3/eDir/tmp.cram", 3, 12, 13, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user3/fDir/aFile", 3, 12, 17, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user4/aDir/bDir/cDir/aFile", 4, 12, 19, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user4/aDir/dDir/eDir/aFile", 4, 12, 23, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamC/user4/aDir/bDir/cDir/aFile", 4, 12, 29, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamC/user4/aDir/dDir/eDir/aFile", 4, 12, 31, times[0], times[0])

		user1Dir := []basedirs.SummaryWithChildren{
			userSummary("/opt/teams/teamA/user1", 1, ids(10), times[3],
				dir(".", times[1], 1, files{db.DGUTAFileTypeText: 3}),
			),
		}

		user2DirA := []basedirs.SummaryWithChildren{
			userSummary("/opt/teams/teamA/user2", 2, ids(11), times[3],
				dir("aDir", times[1], 1, files{db.DGUTAFileTypeBam: 5}),
				dir("bDir", times[1], 1, files{db.DGUTAFileTypeCompressed: 7}),
			),
		}

		user2DirB := []basedirs.SummaryWithChildren{
			userSummary("/opt/teams/teamA/user2/bDir", 2, ids(11), times[3],
				dir(".", times[1], 1, files{db.DGUTAFileTypeCompressed: 7}),
			),
		}

		user3Dir := []basedirs.SummaryWithChildren{
			userSummary("/opt/teams/teamB/user3", 3, ids(12), times[0],
				dir("aDir", times[0], 1, files{db.DGUTAFileTypeVCF: 11}),
				dir("eDir", times[0], 1, files{db.DGUTAFileTypeTemp: 13, db.DGUTAFileTypeCram: 13}),
				dir("fDir", times[0], 1, files{db.DGUTAFileTypeOther: 17}),
			),
		}

		user4Dir := []basedirs.SummaryWithChildren{
			userSummary("/opt/teams/teamB/user4/aDir", 4, ids(12), times[0],
				dir("bDir", times[0], 1, files{db.DGUTAFileTypeOther: 19}),
				dir("dDir", times[0], 1, files{db.DGUTAFileTypeOther: 23}),
			),
			userSummary("/opt/teams/teamC/user4/aDir", 4, ids(12), times[0],
				dir("bDir", times[0], 1, files{db.DGUTAFileTypeOther: 29}),
				dir("dDir", times[0], 1, files{db.DGUTAFileTypeOther: 31}),
			),
		}

		group10Dir := []basedirs.SummaryWithChildren{
			groupSummary("/opt/teams/teamA/user1", ids(1), 10, times[3],
				dir(".", times[1], 1, files{db.DGUTAFileTypeText: 3}),
			),
		}

		group11DirA := []basedirs.SummaryWithChildren{
			groupSummary("/opt/teams/teamA/user2", ids(2), 11, times[3],
				dir("aDir", times[1], 1, files{db.DGUTAFileTypeBam: 5}),
				dir("bDir", times[1], 1, files{db.DGUTAFileTypeCompressed: 7}),
			),
		}

		group11DirB := []basedirs.SummaryWithChildren{
			groupSummary("/opt/teams/teamA/user2/bDir", ids(2), 11, times[3],
				dir(".", times[1], 1, files{db.DGUTAFileTypeCompressed: 7}),
			),
		}

		group12Dir := []basedirs.SummaryWithChildren{
			groupSummary("/opt/teams/teamB", ids(3, 4), 12, times[0],
				dir("user3", times[0], 3, files{db.DGUTAFileTypeOther: 17, db.DGUTAFileTypeTemp: 13, db.DGUTAFileTypeVCF: 11, db.DGUTAFileTypeCram: 13}),
				dir("user4", times[0], 2, files{db.DGUTAFileTypeOther: 42}),
			),
			groupSummary("/opt/teams/teamC/user4/aDir", ids(4), 12, times[0],
				dir("bDir", times[0], 1, files{db.DGUTAFileTypeOther: 29}),
				dir("dDir", times[0], 1, files{db.DGUTAFileTypeOther: 31}),
			),
		}

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		mdb := &mockDB{users: make(basedirs.IDAgeDirs), groups: make(basedirs.IDAgeDirs)}

		Convey("with a simple output func", func() {
			s.AddDirectoryOperation(NewBaseDirs(func(dp *summary.DirectoryPath) bool { return dp.Depth == 3 }, mdb))

			err := s.Summarise()
			So(err, ShouldBeNil)
			So(mdb.users, ShouldResemble, basedirs.IDAgeDirs{
				1: ageDirs(
					user1Dir,
					a(user1Dir, user1Dir, user1Dir),
					m(user1Dir),
				),
				2: ageDirs(
					user2DirA,
					a(user2DirA, user2DirA, user2DirB),
					m(user2DirA),
				),
				3: ageDirs(
					user3Dir,
					a(),
					m(),
				),
				4: ageDirs(
					user4Dir,
					a(),
					m(),
				),
			})
			So(mdb.groups, ShouldResemble, basedirs.IDAgeDirs{
				10: ageDirs(
					group10Dir,
					a(group10Dir, group10Dir, group10Dir),
					m(group10Dir),
				),
				11: ageDirs(
					group11DirA,
					a(group11DirA, group11DirA, group11DirB),
					m(group11DirA),
				),
				12: ageDirs(
					group12Dir,
					a(),
					m(),
				),
			})
		})
	})
}
