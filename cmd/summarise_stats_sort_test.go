/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const summariseStatsSortTestBDirLine = "\"/mnt/test/b/\"\tb-dir"

func TestSummariseStatsSort(t *testing.T) {
	Convey("sorted stats file uses unquoted path keys to restore subtree contiguity", t, func() {
		dir := t.TempDir()
		statsPath := filepath.Join(dir, "stats")
		input := strings.Join([]string{
			"\"/mnt/test/b/two.dat\"\tb",
			"\"/mnt/test/a/one.dat\"\ta-file",
			"\"/mnt/test/a/deep/three.txt\"\ta-deep-file",
			"\"/mnt/test/a/\"\ta-dir",
			summariseStatsSortTestBDirLine,
		}, "\n") + "\n"

		So(os.WriteFile(statsPath, []byte(input), 0o600), ShouldBeNil)

		sortedPath, err := summariseWriteSortedStatsFile(statsPath, filepath.Join(dir, "scratch"))
		So(err, ShouldBeNil)

		data, err := os.ReadFile(sortedPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			"\"/mnt/test/a/\"\ta-dir",
			"\"/mnt/test/a/deep/three.txt\"\ta-deep-file",
			"\"/mnt/test/a/one.dat\"\ta-file",
			summariseStatsSortTestBDirLine,
			"\"/mnt/test/b/two.dat\"\tb",
		})
	})

	Convey("stats sort merge orders multiple chunks and preserves equal-key input order", t, func() {
		dir := t.TempDir()
		chunkA, err := summariseFlushStatsSortChunk([]summariseStatsSortRecord{
			{key: "/mnt/test/b/", line: []byte(summariseStatsSortTestBDirLine), seq: 3},
			{key: "/mnt/test/a/file-2", line: []byte("\"/mnt/test/a/file-2\"\ta2"), seq: 2},
		}, dir, 0)
		So(err, ShouldBeNil)

		chunkB, err := summariseFlushStatsSortChunk([]summariseStatsSortRecord{
			{key: "/mnt/test/a/file-1", line: []byte("\"/mnt/test/a/file-1\"\ta1"), seq: 1},
			{key: "/mnt/test/a/file-1", line: []byte("\"/mnt/test/a/file-1\"\ta1-second"), seq: 4},
		}, dir, 1)
		So(err, ShouldBeNil)

		outPath := filepath.Join(dir, "merged")
		So(summariseMergeStatsSortChunks([]string{chunkA, chunkB}, outPath), ShouldBeNil)

		data, err := os.ReadFile(outPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			"\"/mnt/test/a/file-1\"\ta1",
			"\"/mnt/test/a/file-1\"\ta1-second",
			"\"/mnt/test/a/file-2\"\ta2",
			summariseStatsSortTestBDirLine,
		})
	})
}
