package backups

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

func TestBackups(t *testing.T) {
	Convey("Given valid CSV and stats.gz files", t, func() {
		csv, err := ParseCSV(strings.NewReader(testHeaders +
			"projectA,user1,facultyA,/some/path/,/some/path/to/backup/,backup,*.sh,\n" +
			"projectA,user1,facultyA,/some/path/,/some/path/to/not/backup/,nobackup,,\n" +
			"projectB,user3,facultyB,/some/other/path/,/some/other/path/,tempbackup,*,*.log\n" +
			"projectB,user3,facultyB,/mnt/data/,/mnt/data/stuff/,backup,,\n"))
		So(err, ShouldBeNil)

		sproot := statsdata.NewRoot("/", 0)
		statsdata.AddFile(sproot, "some/file", 5, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/backup/a.sh", 1, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/backup/a.txt", 1, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/backup/a.zip", 6, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/backup/b.zip", 6, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/not/backup/a.ignore", 1, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/path/to/not/backup/a.other", 1, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/other/path/b", 3, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/other/path/c", 3, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/other/path/d.log", 3, 0, 100, 0, 100)
		statsdata.AddFile(sproot, "some/zzz/e", 3, 0, 100, 0, 100)

		mntroot := statsdata.NewRoot("/", 0)
		statsdata.AddFile(mntroot, "mnt/data/stuff/file", 3, 0, 100, 0, 100)

		roots := []string{"/some/", "/mnt/"}

		Convey("You can create a new Backup summariser", func() {
			b, err := New(csv, roots...)
			So(err, ShouldBeNil)

			Convey("And summarise the backup status of files in the stats.gz files", func() {
				tmp := t.TempDir()

				So(b.Process(sproot.AsReader(), tmp), ShouldBeNil)

				entries, _ := filepath.Glob(filepath.Join(tmp, "*"))
				So(entries, ShouldResemble, []string{
					filepath.Join(tmp, "user1_projectA"),
					filepath.Join(tmp, "user3_projectB"),
				})

				data, err := os.ReadFile(filepath.Join(tmp, "user1_projectA"))
				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, "\"/some/path/to/backup/a.sh\"\n")

				data, err = os.ReadFile(filepath.Join(tmp, "user3_projectB"))
				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, "\"/some/other/path/b\"\n\"/some/other/path/c\"\n")

				var buf bytes.Buffer

				So(b.Summarise(&buf), ShouldBeNil)

				So(buf.String(), ShouldEqual, "[{\"Root\":\"/some/\",\"Action\":\"warn\",\"UserID\":3,\"Base\":\"/some/zzz/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Root\":\"/some/\",\"Action\":\"warn\",\"UserID\":5,\"Base\":\"/some/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"warn\",\"UserID\":1,\"Base\":\"/some/path/to/backup/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"nobackup\",\"UserID\":1,\"Base\":\"/some/path/to/not/backup/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":1,\"Base\":\"/some/path/to/backup/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"warn\",\"UserID\":6,\"Base\":\"/some/path/to/backup/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyB\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/some/other/path\",\"Action\":\"nobackup\",\"UserID\":3,\"Base\":\"/some/other/path/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyB\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/some/other/path\",\"Action\":\"tempbackup\",\"UserID\":3,\"Base\":\"/some/other/path/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					"]")

				tmp = t.TempDir()

				So(b.Process(mntroot.AsReader(), tmp), ShouldBeNil)

				entries, _ = filepath.Glob(filepath.Join(tmp, "*"))
				So(entries, ShouldResemble, []string{
					filepath.Join(tmp, "user3_projectB"),
				})

				data, err = os.ReadFile(filepath.Join(tmp, "user3_projectB"))
				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, "\"/mnt/data/stuff/file\"\n")

				buf.Reset()

				So(b.Summarise(&buf), ShouldBeNil)

				So(buf.String(), ShouldEqual, "[{\"Root\":\"/some/\",\"Action\":\"warn\",\"UserID\":3,\"Base\":\"/some/zzz/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Root\":\"/some/\",\"Action\":\"warn\",\"UserID\":5,\"Base\":\"/some/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"warn\",\"UserID\":1,\"Base\":\"/some/path/to/backup/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"nobackup\",\"UserID\":1,\"Base\":\"/some/path/to/not/backup/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":1,\"Base\":\"/some/path/to/backup/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"warn\",\"UserID\":6,\"Base\":\"/some/path/to/backup/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyB\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/mnt/data\",\"Action\":\"backup\",\"UserID\":3,\"Base\":\"/mnt/data/stuff/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyB\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/some/other/path\",\"Action\":\"nobackup\",\"UserID\":3,\"Base\":\"/some/other/path/\",\"Size\":100,\"Count\":1,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					",{\"Faculty\":\"facultyB\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/some/other/path\",\"Action\":\"tempbackup\",\"UserID\":3,\"Base\":\"/some/other/path/\",\"Size\":200,\"Count\":2,\"OldestMTime\":100,\"NewestMTime\":100}\n"+
					"]")
			})
		})
	})
}
