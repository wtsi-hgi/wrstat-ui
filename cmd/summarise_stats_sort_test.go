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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const summariseStatsSortTestBDirLine = "\"/mnt/test/b/\"\tb-dir"

const summariseStatsSortFixtureStamp int64 = 1_717_000_000

const summariseStatsSortTestCommandTimeout = 2 * time.Minute

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

	Convey("mount sorted stats deduplicates directory boundaries and synthesises missing parents", t, func() {
		dir := t.TempDir()
		statsPath := filepath.Join(dir, "stats")
		input := strings.Join([]string{
			summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1),
			summariseStatsSortFixtureRow("/mnt/test/a/one.dat", 'f', 10, 12, 22, 302, 2),
			summariseStatsSortFixtureRow("/mnt/test/b/two.dat", 'f', 20, 14, 24, 304, 3),
			summariseStatsSortFixtureRow("/mnt/test/a/", 'd', 4096, 11, 21, 301, 1),
			summariseStatsSortFixtureRow("/mnt/test/a/", 'd', 4096, 11, 21, 301, 1),
		}, "\n") + "\n"

		So(os.WriteFile(statsPath, []byte(input), 0o600), ShouldBeNil)

		sortedPath, err := summariseWriteSortedStatsFileForMount(statsPath, filepath.Join(dir, "scratch"), "/mnt/test/")
		So(err, ShouldBeNil)

		data, err := os.ReadFile(sortedPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1),
			summariseStatsSortFixtureRow("/mnt/test/a/", 'd', 4096, 11, 21, 301, 1),
			summariseStatsSortFixtureRow("/mnt/test/a/one.dat", 'f', 10, 12, 22, 302, 2),
			fmt.Sprintf("%q\t0\t%d\t%d\t%d\t%d\t%d\td\t0\t%d\t1\t0",
				"/mnt/test/b/", uint32(14), uint32(24), summariseStatsSortFixtureStamp,
				summariseStatsSortFixtureStamp, summariseStatsSortFixtureStamp, int64(3)),
			summariseStatsSortFixtureRow("/mnt/test/b/two.dat", 'f', 20, 14, 24, 304, 3),
		})
	})

	Convey("mount sorted stats uses component preorder when sibling names share a prefix", t, func() {
		dir := t.TempDir()
		statsPath := filepath.Join(dir, "stats")
		input := strings.Join([]string{
			summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1),
			summariseStatsSortFixtureRow("/mnt/test/project.v2/", 'd', 4096, 11, 21, 301, 1),
			summariseStatsSortFixtureRow("/mnt/test/project.v2/result.dat", 'f', 20, 12, 22, 302, 1),
			summariseStatsSortFixtureRow("/mnt/test/project/", 'd', 4096, 13, 23, 303, 1),
			summariseStatsSortFixtureRow("/mnt/test/project/root.dat", 'f', 10, 14, 24, 304, 1),
		}, "\n") + "\n"

		So(os.WriteFile(statsPath, []byte(input), 0o600), ShouldBeNil)

		sortedPath, err := summariseWriteSortedStatsFileForMount(statsPath, filepath.Join(dir, "scratch"), "/mnt/test/")
		So(err, ShouldBeNil)

		data, err := os.ReadFile(sortedPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1),
			summariseStatsSortFixtureRow("/mnt/test/project/", 'd', 4096, 13, 23, 303, 1),
			summariseStatsSortFixtureRow("/mnt/test/project/root.dat", 'f', 10, 14, 24, 304, 1),
			summariseStatsSortFixtureRow("/mnt/test/project.v2/", 'd', 4096, 11, 21, 301, 1),
			summariseStatsSortFixtureRow("/mnt/test/project.v2/result.dat", 'f', 20, 12, 22, 302, 1),
		})
	})

	Convey("mount sorted stats keeps escaped unicode siblings after the plain-prefix subtree", t, func() {
		dir := t.TempDir()
		statsPath := filepath.Join(dir, "stats")
		root := summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1)
		chr1Dir := summariseStatsSortFixtureRow("/mnt/test/chr1/", 'd', 4096, 11, 21, 301, 1)
		chr1File := summariseStatsSortFixtureRow("/mnt/test/chr1/file.dat", 'f', 10, 12, 22, 302, 1)
		chr1NBSPDir := summariseStatsSortFixtureRow("/mnt/test/chr1\u00a0/", 'd', 4096, 13, 23, 303, 1)
		chr1NBSPFile := summariseStatsSortFixtureRow("/mnt/test/chr1\u00a0/leaf.dat", 'f', 20, 14, 24, 304, 1)
		chr2Dir := summariseStatsSortFixtureRow("/mnt/test/chr2/", 'd', 4096, 15, 25, 305, 1)
		chr2File := summariseStatsSortFixtureRow("/mnt/test/chr2/two.dat", 'f', 30, 16, 26, 306, 1)
		input := strings.Join([]string{
			root,
			chr1NBSPDir,
			chr1NBSPFile,
			chr2File,
			chr1File,
			chr2Dir,
			chr1Dir,
		}, "\n") + "\n"

		So(os.WriteFile(statsPath, []byte(input), 0o600), ShouldBeNil)

		sortedPath, err := summariseWriteSortedStatsFileForMount(statsPath, filepath.Join(dir, "scratch"), "/mnt/test/")
		So(err, ShouldBeNil)

		data, err := os.ReadFile(sortedPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			root,
			chr1Dir,
			chr1File,
			chr1NBSPDir,
			chr1NBSPFile,
			chr2Dir,
			chr2File,
		})
	})

	Convey("mount sorted stats keeps a same-name directory subtree before the file entry", t, func() {
		dir := t.TempDir()
		statsPath := filepath.Join(dir, "stats")
		root := summariseStatsSortFixtureRow("/mnt/test/", 'd', 4096, 10, 20, 300, 1)
		clashDir := summariseStatsSortFixtureRow("/mnt/test/clash/", 'd', 4096, 11, 21, 301, 1)
		clashFile := summariseStatsSortFixtureRow("/mnt/test/clash", 'f', 10, 12, 22, 302, 1)
		clashLeaf := summariseStatsSortFixtureRow("/mnt/test/clash/leaf.dat", 'f', 20, 13, 23, 303, 1)
		nextDir := summariseStatsSortFixtureRow("/mnt/test/next/", 'd', 4096, 14, 24, 304, 1)
		input := strings.Join([]string{
			root,
			clashDir,
			nextDir,
			clashFile,
			clashLeaf,
		}, "\n") + "\n"

		So(os.WriteFile(statsPath, []byte(input), 0o600), ShouldBeNil)

		sortedPath, err := summariseWriteSortedStatsFileForMount(statsPath, filepath.Join(dir, "scratch"), "/mnt/test/")
		So(err, ShouldBeNil)

		data, err := os.ReadFile(sortedPath)
		So(err, ShouldBeNil)
		So(strings.Split(strings.TrimSpace(string(data)), "\n"), ShouldResemble, []string{
			root,
			clashDir,
			clashLeaf,
			clashFile,
			nextDir,
		})
	})
}

func summariseStatsSortFixtureRow(
	path string,
	entryType byte,
	size int64,
	uid uint32,
	gid uint32,
	inode uint64,
	nlink uint64,
) string {
	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, path, entryType, size, uid, gid, summariseStatsSortFixtureStamp, inode, nlink)

	return strings.TrimSuffix(buf.String(), "\n")
}

func TestSummariseStatsSortMergeHotPathSymbols(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ELF symbol guard is Linux-only")
	}

	Convey("stats sort merge hot path omits the async-preemption wrapper named in the runtime fatal", t, func() {
		var buf bytes.Buffer
		So(summariseWriteMergedStatsSortChunks(bufio.NewWriter(&buf), nil), ShouldBeNil)

		testBinary := filepath.Join(t.TempDir(), "cmd.test")
		So(summariseStatsSortBuildTestBinary(testBinary), ShouldBeNil)

		present, err := summariseStatsSortTestExecutableHasSymbol(
			testBinary,
			"github.com/wtsi-hgi/wrstat-ui/cmd.summariseWriteNextMergedStatsSortChunk",
		)
		So(err, ShouldBeNil)
		So(present, ShouldBeFalse)
	})
}

func summariseStatsSortBuildTestBinary(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), summariseStatsSortTestCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "test", "-tags", "netgo", "-c", ".", "-o", path)

	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func summariseStatsSortTestExecutableHasSymbol(path string, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), summariseStatsSortTestCommandTimeout)
	defer cancel()

	output, err := exec.CommandContext(ctx, "go", "tool", "nm", path).CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}

	for _, line := range bytes.Split(output, []byte{'\n'}) {
		if strings.HasSuffix(string(line), " "+name) {
			return true, nil
		}
	}

	return false, nil
}
