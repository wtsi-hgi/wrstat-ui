package basedirs

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type mockBaseDirsMap map[uint32][]string

type mockDB struct {
	users, groups mockBaseDirsMap
}

func (m *mockDB) AddUserBase(uid uint32, path *summary.DirectoryPath, age db.DirGUTAge) error {
	return add(m.users, uid, path, age)
}

func add(m mockBaseDirsMap, id uint32, path *summary.DirectoryPath, age db.DirGUTAge) error {
	m[id] = append(m[id], string(append(path.AppendTo(nil), byte(age))))

	return nil
}

func (m *mockDB) AddGroupBase(gid uint32, path *summary.DirectoryPath, age db.DirGUTAge) error {
	return add(m.groups, gid, path, age)
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

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{users: make(mockBaseDirsMap), groups: make(mockBaseDirsMap)}
		s.AddDirectoryOperation(NewBaseDirs(func(_ *summary.DirectoryPath) int { return 3 }, m))

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
				pathAge("/opt/teams/teamA/user2/bDir/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeM1M),
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
				pathAge("/opt/teams/teamA/user2/bDir/", db.DGUTAgeA6M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeAll),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA1M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeA2M),
				pathAge("/opt/teams/teamA/user2/", db.DGUTAgeM1M),
			},
		})
	})
}
