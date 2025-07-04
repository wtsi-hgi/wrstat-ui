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

//nolint:lll
package backups

import (
	"bytes"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

func TestSummary(t *testing.T) {
	Convey("Given a parsed CSV file", t, func() {
		lines, err := ParseCSV(strings.NewReader(testHeaders +
			"projectA,user1,facultyA,/some/path/,/some/path/to/backup/,backup,*.sh,\n" +
			"projectA,user1,facultyB,/some/path/,/some/path/to/not/backup/,nobackup,*.ignore,\n" +
			"projectB,user3,facultyC,/some/other/path/,/some/other/path/,tempbackup,*,*.log\n"))
		So(err, ShouldBeNil)

		actions := createActions(lines, []string{"/some/"})

		var (
			s   = make(backupSummary)
			buf bytes.Buffer
		)

		summaryJSON := func() string {
			defer buf.Reset()

			s.WriteTo(&buf) //nolint:errcheck

			return buf.String()
		}

		Convey("You can create a Summary and write JSON", func() {
			So(summaryJSON(), ShouldEqual, "[]")

			paths := internaltest.NewDirectoryPathCreator()

			s.addFile(
				internaltest.NewMockInfoWithTimes(paths.ToDirectoryPath("/some/path/to/backup/some/child/dir/"), 100, 200, 300, false, 400),
				actions[0].Group,
			)

			So(summaryJSON(), ShouldEqual, "["+
				"{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":100,\"Base\":\"/some/path/to/backup/some/child/dir/\",\"Size\":300,\"Count\":1,\"OldestMTime\":400,\"NewestMTime\":400}\n"+
				"]")

			s.addFile(
				internaltest.NewMockInfoWithTimes(paths.ToDirectoryPath("/some/path/to/backup/some/child/dir2/"), 100, 200, 400, false, 500),
				actions[0].Group,
			)

			So(summaryJSON(), ShouldEqual, "["+
				"{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":100,\"Base\":\"/some/path/to/backup/some/child/\",\"Size\":700,\"Count\":2,\"OldestMTime\":400,\"NewestMTime\":500}\n"+
				"]")

			s.addFile(
				internaltest.NewMockInfoWithTimes(paths.ToDirectoryPath("/some/path/to/backup/some/child/dir2/"), 101, 200, 400, false, 500),
				actions[0].Group,
			)

			So(summaryJSON(), ShouldEqual, "["+
				"{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":100,\"Base\":\"/some/path/to/backup/some/child/\",\"Size\":700,\"Count\":2,\"OldestMTime\":400,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":101,\"Base\":\"/some/path/to/backup/some/child/dir2/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				"]")

			s.addFile(
				internaltest.NewMockInfoWithTimes(paths.ToDirectoryPath("/some/path/to/not/backup/"), 101, 200, 400, false, 500),
				actions[1].Group,
			)

			So(summaryJSON(), ShouldEqual, "["+
				"{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":100,\"Base\":\"/some/path/to/backup/some/child/\",\"Size\":700,\"Count\":2,\"OldestMTime\":400,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":101,\"Base\":\"/some/path/to/backup/some/child/dir2/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyB\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"nobackup\",\"UserID\":101,\"Base\":\"/some/path/to/not/backup/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				"]")

			s.addFile(
				internaltest.NewMockInfoWithTimes(paths.ToDirectoryPath("/some/other/path/"), 105, 200, 400, false, 500),
				actions[2].Group,
			)

			So(summaryJSON(), ShouldEqual, "["+
				"{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":100,\"Base\":\"/some/path/to/backup/some/child/\",\"Size\":700,\"Count\":2,\"OldestMTime\":400,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyA\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"backup\",\"UserID\":101,\"Base\":\"/some/path/to/backup/some/child/dir2/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyB\",\"Name\":\"projectA\",\"Requestor\":\"user1\",\"Root\":\"/some/path\",\"Action\":\"nobackup\",\"UserID\":101,\"Base\":\"/some/path/to/not/backup/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				",{\"Faculty\":\"facultyC\",\"Name\":\"projectB\",\"Requestor\":\"user3\",\"Root\":\"/some/other/path\",\"Action\":\"nobackup\",\"UserID\":105,\"Base\":\"/some/other/path/\",\"Size\":400,\"Count\":1,\"OldestMTime\":500,\"NewestMTime\":500}\n"+
				"]")
		})
	})
}
