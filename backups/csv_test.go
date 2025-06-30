package backups

import (
	"io"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func ShouldMatchError(actual any, expected ...any) string {
	if expected[0] == nil {
		return ShouldBeNil(actual)
	}

	return ShouldWrap(actual, expected...)
}

func TestParseCSV(t *testing.T) {
	const (
		headers = "reporting_name," +
			"requestor," +
			"faculty," +
			"reporting_root," +
			"directory," +
			"instruction ['backup' or 'nobackup' or 'tempbackup']," +
			"file_types_backup," +
			"file_types_ignore" +
			"\n"
		altHeaders = "requestor," +
			"instruction ['backup' or 'nobackup' or 'tempbackup']," +
			"directory," +
			"file_types_ignore," +
			"reporting_name," +
			"file_types_backup," +
			"faculty," +
			"reporting_root" +
			"\n"
	)

	Convey("You can parse a CSV files into line entries", t, func() {
		for _, test := range [...]struct {
			Test   string
			Input  string
			Err    error
			Output []*ReportLine
		}{
			{
				Test: "Empty file produces ErrUnexpectedEOF error",
				Err:  io.ErrUnexpectedEOF,
			},
			{
				Test:  "Invalid title line produces error",
				Input: "title",
				Err:   ErrHeaderNotFound,
			},
			{
				Test:  "Missing titles produces error",
				Input: "reporting_name,requestor,directory,instruction ['backup' or 'nobackup' or 'tempbackup'],file_types_backup,file_types_ignore",
				Err:   ErrHeaderNotFound,
			},
			{
				Test:   "Valid titles, but otherwise empty file produces no lines or errors",
				Input:  headers,
				Output: []*ReportLine{},
			},
			{
				Test:   "Title order doesn't matter",
				Input:  altHeaders,
				Output: []*ReportLine{},
			},
			{
				Test: "You can specify a path to back-up",
				Input: headers +
					"projectA,user1,facultyA,/some/path/,/some/path/to/backup/,backup,,",
				Output: []*ReportLine{
					{
						Path:      []byte("/some/path/to/backup/*"),
						action:    actionBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path/",
					},
				},
			},
			{
				Test: "You can specify a path to back-up, with specific filetypes to backup and ignore, with a different column order",
				Input: altHeaders +
					"user1,backup,/some/path/to/backup/,log* *.log,projectA,*.txt,facultyA,/some/path/",
				Output: []*ReportLine{
					{
						Path:      []byte("/some/path/to/backup/log*"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path/",
					},
					{
						Path:      []byte("/some/path/to/backup/*.log"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path/",
					},
					{
						Path:      []byte("/some/path/to/backup/*.txt"),
						action:    actionBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path/",
					},
				},
			},
			{
				Test: "You can specify many paths to backup or ignore",
				Input: headers +
					"projectA,user1,facultyA,/some/path/,/some/path/to/backup/,backup,*.sh,\n" +
					"projectA,user1,facultyB,/some/path/,/some/path/to/not/backup/,nobackup,*.ignore,\n" +
					"projectB,user2,facultyA,/some/other/path/,/some/other/path/,tempbackup,*,*.log\n",
				Output: []*ReportLine{
					{
						Path:      []byte("/some/path/to/backup/*.sh"),
						action:    actionBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path/",
					},
					{
						Path:      []byte("/some/path/to/not/backup/*"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyB",
						root:      "/some/path/",
					},
					{
						Path:      []byte("/some/other/path/*.log"),
						action:    actionNoBackup,
						requestor: "user2",
						name:      "projectB",
						faculty:   "facultyA",
						root:      "/some/other/path/",
					},
					{
						Path:      []byte("/some/other/path/*"),
						action:    actionTempBackup,
						requestor: "user2",
						name:      "projectB",
						faculty:   "facultyA",
						root:      "/some/other/path/",
					},
				},
			},
		} {
			Convey(test.Test, func() {
				lines, err := ParseCSV(strings.NewReader(test.Input))
				So(err, ShouldMatchError, test.Err)
				So(lines, ShouldResemble, test.Output)
			})
		}
	})
}
