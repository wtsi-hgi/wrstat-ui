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
