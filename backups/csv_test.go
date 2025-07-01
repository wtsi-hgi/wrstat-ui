/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package backups

import (
	"io"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	testHeaders = "reporting_name," +
		"requestor," +
		"faculty," +
		"reporting_root," +
		"directory," +
		"instruction ['backup' or 'nobackup' or 'tempbackup']," +
		"file_types_backup," +
		"file_types_ignore" +
		"\n"
	testAltHeaders = "requestor," +
		"instruction ['backup' or 'nobackup' or 'tempbackup']," +
		"directory," +
		"file_types_ignore," +
		"reporting_name," +
		"file_types_backup," +
		"faculty," +
		"reporting_root" +
		"\n"
)

func ShouldMatchError(actual any, expected ...any) string {
	if expected[0] == nil {
		return ShouldBeNil(actual)
	}

	return ShouldWrap(actual, expected...)
}

func TestParseCSV(t *testing.T) {

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
				Input:  testHeaders,
				Output: []*ReportLine{},
			},
			{
				Test:   "Title order doesn't matter",
				Input:  testAltHeaders,
				Output: []*ReportLine{},
			},
			{
				Test: "You can specify a path to back-up",
				Input: testHeaders +
					"projectA,user1,facultyA,/some/path/,/some/path/to/backup/,backup,,",
				Output: []*ReportLine{
					{
						Path:      []byte("/some/path/to/backup/*"),
						action:    actionBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path",
					},
				},
			},
			{
				Test: "You can specify a path to back-up, with specific filetypes to backup and ignore, with a different column order",
				Input: testAltHeaders +
					"user1,backup,/some/path/to/backup/,log* *.log,projectA,*.txt,facultyA,/some/path/",
				Output: []*ReportLine{
					{
						Path:      []byte("/some/path/to/backup/log*"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path",
					},
					{
						Path:      []byte("/some/path/to/backup/*.log"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path",
					},
					{
						Path:      []byte("/some/path/to/backup/*.txt"),
						action:    actionBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyA",
						root:      "/some/path",
					},
				},
			},
			{
				Test: "You can specify many paths to backup or ignore",
				Input: testHeaders +
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
						root:      "/some/path",
					},
					{
						Path:      []byte("/some/path/to/not/backup/*"),
						action:    actionNoBackup,
						requestor: "user1",
						name:      "projectA",
						faculty:   "facultyB",
						root:      "/some/path",
					},
					{
						Path:      []byte("/some/other/path/*.log"),
						action:    actionNoBackup,
						requestor: "user2",
						name:      "projectB",
						faculty:   "facultyA",
						root:      "/some/other/path",
					},
					{
						Path:      []byte("/some/other/path/*"),
						action:    actionTempBackup,
						requestor: "user2",
						name:      "projectB",
						faculty:   "facultyA",
						root:      "/some/other/path",
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
