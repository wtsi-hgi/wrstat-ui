package basedirs

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type mockBaseDirsMap map[uint32][]string

type mockDB struct {
	users, groups mockBaseDirsMap
}

func (m *mockDB) Output(users, groups basedirs.IDAgeDirs) error {
	add(m.users, users)
	add(m.groups, groups)

	return nil
}

func add(m mockBaseDirsMap, i basedirs.IDAgeDirs) {
	for id, ad := range i {
		for age, dcss := range ad {
			for _, dcs := range dcss {
				m[id] = append(m[id], dcs.Dir+string(byte(age)))
			}
		}
	}
}

func pathAge(path string, age db.DirGUTAge) string {
	return path + string(byte(age))
}

func TestBaseDirs(t *testing.T) {
	Convey("", t, func() {
		const dt = db.SecondsInAMonth >> 1

		var times [len(db.AgeThresholds)]int64

		now := time.Now().Unix()

		for n := range times {
			times[n] = now + dt - db.AgeThresholds[n]
		}

		f := statsdata.NewRoot("/", 0)
		statsdata.AddFile(f, "opt/teams/teamA/user1/aFile.txt", 1, 10, 0, times[3], times[1])
		statsdata.AddFile(f, "opt/teams/teamA/user2/aDir/aFile.txt", 2, 11, 0, times[2], times[1])
		statsdata.AddFile(f, "opt/teams/teamA/user2/bDir/bFile.txt", 2, 11, 0, times[3], times[1])
		statsdata.AddFile(f, "opt/teams/teamB/user3/aDir/bDir/cDir/aFile.txt", 3, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user3/eDir/aFile.txt", 3, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user3/fDir/aFile.txt", 3, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user4/aDir/bDir/cDir/aFile.txt", 4, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamB/user4/aDir/dDir/eDir/aFile.txt", 4, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamC/user4/aDir/bDir/cDir/aFile.txt", 4, 12, 0, times[0], times[0])
		statsdata.AddFile(f, "opt/teams/teamC/user4/aDir/dDir/eDir/aFile.txt", 4, 12, 0, times[0], times[0])

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{users: make(mockBaseDirsMap), groups: make(mockBaseDirsMap)}
		s.AddDirectoryOperation(NewBaseDirs(func(dp *summary.DirectoryPath) bool { return dp.Depth == 3 }, m))

		err := s.Summarise()
		So(err, ShouldBeNil)
		So(m.users, ShouldResemble, mockBaseDirsMap{
			1: []string{
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeM1M),
			},
			2: []string{
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user2/bDir/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeM1M),
			},
			3: []string{
				pathAge("/opt/teams/teamB/user3/", db.DGUTAgeAll),
			},
			4: []string{
				pathAge("/opt/teams/teamB/user4/aDir/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamC/user4/aDir/", db.DGUTAgeAll),
			},
		})
		So(m.groups, ShouldResemble, mockBaseDirsMap{
			10: []string{
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user1/", db.DGUTAgeM1M),
			},
			11: []string{
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user2/bDir/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeM1M),
			},
			12: []string{
				pathAge("/opt/teams/teamB/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamC/user4/aDir/", db.DGUTAgeAll),
			},
		})
	})
}
