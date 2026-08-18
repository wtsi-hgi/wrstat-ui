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

package dirbuild

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const dirbuildRefUnix = int64(1779120209)

var dirbuildRefTime = time.Unix(dirbuildRefUnix, 0).UTC() //nolint:gochecknoglobals

const (
	a4TotalEntries               = 1_000_000
	a4DirectoryRows              = 50_000
	a4DirectoryOnlyRows          = 250_000
	a4AgedFanoutDirs             = 80_000
	a4CompactFanoutDirs          = 100_000
	a4LongPathDirs               = 80_000
	a4LongPathNameBytes          = 1024
	a4AssignmentOverlapDirs      = 300_000
	a4AssignmentOverlapNameBytes = 128
	// A4's heap budget scales with directory rows, not file rows.
	a4HeapBaseBudgetBytes          = 64 * 1024 * 1024
	a4HeapPerDirectoryBudgetBytes  = 8 * 1024
	a4IndexHeapBaseBudgetBytes     = 48 * 1024 * 1024
	a4IndexHeapPerDirectoryBytes   = 430
	a4RollupHeapBaseBudgetBytes    = 128 * 1024 * 1024
	a4RollupHeapPerDirectoryBytes  = 6 * 1024
	a4CompactHeapBaseBudgetBytes   = 96 * 1024 * 1024
	a4CompactHeapPerDirectoryBytes = 3 * 1024
	a4LongPathHeapBaseBudgetBytes  = 64 * 1024 * 1024
	a4LongPathHeapPerDirectoryByte = 700
	a4AssignHeapBaseBudgetBytes    = 48 * 1024 * 1024
	a4AssignHeapPerDirectoryByte   = 360
	a4HeapSamplerInterval          = 10 * time.Millisecond
	a4LargeNonContiguousStatsInput = "stats.tsv"
)

type captureDB struct {
	records []db.RecordDGUTA
}

func (c *captureDB) Add(record db.RecordDGUTA) error {
	c.records = append(c.records, record)

	return nil
}

func TestDirbuildTelemetry(t *testing.T) {
	Convey("recorder-free index builds avoid telemetry allocations and callbacks", t, func() {
		var input strings.Builder
		input.WriteString(statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2))

		for i := range 64 {
			input.WriteString(statsRow(fmt.Sprintf("/prefix-%03d/file", i), stats.FileType,
				1, dirbuildRefUnix, dirbuildRefUnix, int64(3000+i), 1))
		}

		data := input.String()
		withoutRecorder := testing.AllocsPerRun(5, func() {
			index, err := buildDirectoryIndex(func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(data)), nil
			}, "/", dirbuildRefUnix)
			So(err, ShouldBeNil)
			So(index.nodes, ShouldHaveLength, 65)
		})

		callbacks := 0
		withRecorder := testing.AllocsPerRun(5, func() {
			telemetry := newBuildTelemetry(Options{Progress: func(Telemetry) { callbacks++ }})
			index, err := buildDirectoryIndex(func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(data)), nil
			}, "/", dirbuildRefUnix, telemetry)
			So(err, ShouldBeNil)
			So(index.nodes, ShouldHaveLength, 65)
		})

		withoutShape := testing.AllocsPerRun(100, func() {
			paths := newPathBuilder(false)
			runtime.KeepAlive(paths)
		})
		withShape := testing.AllocsPerRun(100, func() {
			paths := newPathBuilder(true)
			runtime.KeepAlive(paths)
		})

		So(callbacks, ShouldBeGreaterThan, 0)
		So(withoutRecorder, ShouldBeLessThan, withRecorder)
		So(withoutShape, ShouldBeLessThan, withShape)
	})

	Convey("multi-pass builds expose live phase, shape, and bounded prefix telemetry", t, func() {
		input := strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/alpha/deep/file.txt", stats.FileType, 11, dirbuildRefUnix-30, dirbuildRefUnix-20, 3001, 1),
			statsRow("/beta/file.txt", stats.FileType, 12, dirbuildRefUnix-30, dirbuildRefUnix-20, 3002, 1),
		}, "")

		var snapshots []Telemetry

		err := BuildWithFilesOptions(func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(input)), nil
		}, "/", &captureDB{}, dirbuildRefTime, nil, Options{
			DiskNodeThreshold: 1,
			TempDir:           t.TempDir(),
			Progress: func(snapshot Telemetry) {
				snapshots = append(snapshots, snapshot)
			},
		})

		So(err, ShouldBeNil)
		So(telemetryPhases(snapshots), ShouldResemble, []string{
			PhaseDirectoryScan,
			PhaseIDAssignment,
			PhasePass2Aggregation,
			PhaseSQLiteRollupSpoolEmission,
		})
		last := snapshots[len(snapshots)-1]
		So(last.InputRows, ShouldEqual, uint64(3))
		So(last.DirectoryNodes, ShouldEqual, uint64(4))
		So(last.ImpliedDirectories, ShouldEqual, uint64(3))
		So(last.MaximumDepth, ShouldEqual, uint64(2))
		So(len(last.DepthHistogram), ShouldBeLessThanOrEqualTo, TelemetryDepthBins)
		So(len(last.HeavyPrefixes), ShouldBeLessThanOrEqualTo, TelemetryHeavyPrefixCapacity)
		So(last.SQLiteBytes, ShouldBeGreaterThan, uint64(0))
	})

	Convey("shape telemetry remains bounded as prefix variety grows", t, func() {
		var input strings.Builder
		input.WriteString(statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2))

		for i := range TelemetryHeavyPrefixCapacity * 10 {
			input.WriteString(statsRow(fmt.Sprintf("/prefix-%03d/file", i), stats.FileType,
				1, dirbuildRefUnix, dirbuildRefUnix, int64(3000+i), 1))
		}

		var last Telemetry

		err := BuildWithFilesOptions(func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(input.String())), nil
		}, "/", &captureDB{}, dirbuildRefTime, nil, Options{
			Progress: func(snapshot Telemetry) { last = snapshot },
		})

		So(err, ShouldBeNil)
		So(len(last.HeavyPrefixes), ShouldBeLessThanOrEqualTo, TelemetryHeavyPrefixCapacity)
		So(len(last.DepthHistogram), ShouldBeLessThanOrEqualTo, TelemetryDepthBins)
	})

	Convey("heavy prefixes and counts are relative to a non-root selected mount", t, func() {
		input := strings.Join([]string{
			statsRow("/mnt/test/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/mnt/test/alpha/deep/file.txt", stats.FileType,
				11, dirbuildRefUnix-30, dirbuildRefUnix-20, 3001, 1),
			statsRow("/mnt/test/beta/file.txt", stats.FileType,
				12, dirbuildRefUnix-30, dirbuildRefUnix-20, 3002, 1),
		}, "")

		var last Telemetry

		err := BuildWithFilesOptions(func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(input)), nil
		}, "/mnt/test/", &captureDB{}, dirbuildRefTime, nil, Options{
			Progress: func(snapshot Telemetry) { last = snapshot },
		})

		So(err, ShouldBeNil)
		So(last.HeavyPrefixes, ShouldResemble, []PrefixCount{
			{Prefix: "alpha/", Count: 2},
			{Prefix: "beta/", Count: 1},
		})
	})
}

func telemetryPhases(snapshots []Telemetry) []string {
	phases := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(phases) == 0 || phases[len(phases)-1] != snapshot.Phase {
			phases = append(phases, snapshot.Phase)
		}
	}

	return phases
}

func summariseWithDirGUTA(data string, mountPath string) ([]db.RecordDGUTA, error) {
	sink := new(captureDB)

	alloc := summary.NewDirIDAllocator()
	if err := alloc.SetMountPath(mountPath); err != nil {
		return nil, err
	}

	s := summary.NewSummariser(stats.NewStatsParser(strings.NewReader(data)))
	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAgeAt(sink, dirbuildRefTime, alloc))

	return sink.records, s.Summarise()
}

func buildWithDirBuildOptions(
	data string,
	mountPath string,
	opts Options,
) ([]db.RecordDGUTA, []comparableFileRow, int, error) {
	sink := new(captureDB)

	var files []comparableFileRow

	openCalls := 0

	err := BuildWithFilesOptions(func() (io.ReadCloser, error) {
		openCalls++

		return io.NopCloser(strings.NewReader(data)), nil
	}, mountPath, sink, dirbuildRefTime, func(dirID uint32, info summary.FileInfo) error {
		files = append(files, comparableFileRow{
			dirID:     dirID,
			name:      string(info.Name),
			size:      info.Size,
			entryType: info.EntryType,
		})

		return nil
	}, opts)

	return sink.records, files, openCalls, err
}

type comparableFileRow struct {
	dirID     uint32
	name      string
	size      int64
	entryType byte
}

func diskBackedRoutingFixtureStats() string {
	return strings.Join([]string{
		statsRow("/project/b/deep/linked.bam", stats.FileType, 100, dirbuildRefUnix-90, dirbuildRefUnix-10, 9001, 2),
		statsRow("/project/a/early.txt", stats.FileType, 11, dirbuildRefUnix-30, dirbuildRefUnix-20, 3001, 1),
		statsRow("/project/b/deep/other.cram", stats.FileType, 17, dirbuildRefUnix-50, dirbuildRefUnix-15, 3002, 1),
		statsRow("/project/a/linked.bam", stats.FileType, 100, dirbuildRefUnix-90, dirbuildRefUnix-10, 9001, 2),
		statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
		statsRow("/project/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
		statsRow("/project/b/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2002, 2),
		statsRow("/project/a/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2003, 2),
		statsRow("/project/b/deep/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2004, 2),
	}, "")
}

func writeLongPathDirectoryStats(path string, dirs int, nameBytes int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 1024*1024)
	writeErr := writeLongPathDirectoryStatsRows(writer, dirs, nameBytes)
	flushErr := writer.Flush()
	closeErr := file.Close()

	return errors.Join(writeErr, flushErr, closeErr)
}

func writeLongPathDirectoryStatsRows(writer io.Writer, dirs int, nameBytes int) error {
	longName := strings.Repeat("x", nameBytes)

	if _, err := writer.Write([]byte(statsRow(
		"/",
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		99,
		2,
	))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(statsRow(
		"/long/",
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		100,
		2,
	))); err != nil {
		return err
	}

	longRoot := "/long/" + longName + "/"
	if _, err := writer.Write([]byte(statsRow(
		longRoot,
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		101,
		2,
	))); err != nil {
		return err
	}

	for dirIndex := range dirs {
		if _, err := writer.Write([]byte(statsRow(
			fmt.Sprintf("%sd%06d/", longRoot, dirIndex),
			stats.DirType,
			4096,
			dirbuildRefUnix,
			dirbuildRefUnix,
			int64(4_000_000+dirIndex),
			2,
		))); err != nil {
			return err
		}
	}

	return nil
}

func assertDirectoryIndexMemory(input string, expectedNodes int, heapBudget uint64) {
	restoreGC := useDirbuildGCPercent()
	defer restoreGC()

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	stopSampling := startHeapInuseSampler(before.HeapInuse)
	index, err := buildDirectoryIndex(func() (io.ReadCloser, error) {
		return os.Open(input)
	}, "/", dirbuildRefUnix)
	peakGrowth := stopSampling()

	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	retainedGrowth := heapInuseGrowth(before.HeapInuse, after.HeapInuse)

	So(err, ShouldBeNil)
	So(index.nodes, ShouldHaveLength, expectedNodes)
	So(peakGrowth, ShouldBeLessThan, heapBudget)
	So(retainedGrowth, ShouldBeLessThan, heapBudget)
}

func a4LongPathIndexHeapBudget(dirRows uint64) uint64 {
	return a4LongPathHeapBaseBudgetBytes + dirRows*a4LongPathHeapPerDirectoryByte
}

func writeWideDirectoryStats(path string, dirs int, nameBytes int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 1024*1024)
	writeErr := writeWideDirectoryStatsRows(writer, dirs, nameBytes)
	flushErr := writer.Flush()
	closeErr := file.Close()

	return errors.Join(writeErr, flushErr, closeErr)
}

func writeWideDirectoryStatsRows(writer io.Writer, dirs int, nameBytes int) error {
	if _, err := writer.Write([]byte(statsRow(
		"/",
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		99,
		2,
	))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(statsRow(
		"/wide/",
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		100,
		2,
	))); err != nil {
		return err
	}

	namePrefix := strings.Repeat("w", nameBytes)
	for dirIndex := range dirs {
		if _, err := writer.Write([]byte(statsRow(
			fmt.Sprintf("/wide/%s%06d/", namePrefix, dirIndex),
			stats.DirType,
			4096,
			dirbuildRefUnix,
			dirbuildRefUnix,
			int64(5_000_000+dirIndex),
			2,
		))); err != nil {
			return err
		}
	}

	return nil
}

func a4AssignmentIndexHeapBudget(dirRows uint64) uint64 {
	return a4AssignHeapBaseBudgetBytes + dirRows*a4AssignHeapPerDirectoryByte
}

func buildWithDirBuild(data string, mountPath string) ([]db.RecordDGUTA, error) {
	records, _, _, err := buildWithDirBuildOptions(data, mountPath, Options{})

	return records, err
}

type countingDB struct {
	records int
}

func (c *countingDB) Add(db.RecordDGUTA) error {
	c.records++

	return nil
}

func TestBuildA4BoundedMemoryAndBytesWritten(t *testing.T) {
	Convey("A4.0 directory-only index build does not preallocate per-directory accumulators", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeDirectoryOnlyStats(input, a4DirectoryOnlyRows), ShouldBeNil)

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		stopSampling := startHeapInuseSampler(before.HeapInuse)
		index, err := buildDirectoryIndex(func() (io.ReadCloser, error) {
			return os.Open(input)
		}, "/", dirbuildRefUnix)
		peakGrowth := stopSampling()

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBudget := a4DirectoryIndexHeapBudget(a4DirectoryOnlyRows)
		retainedGrowth := heapInuseGrowth(before.HeapInuse, after.HeapInuse)

		So(err, ShouldBeNil)
		So(index.nodes, ShouldHaveLength, a4DirectoryOnlyRows)
		So(peakGrowth, ShouldBeLessThan, heapBudget)
		So(retainedGrowth, ShouldBeLessThan, heapBudget)
	})

	Convey("A4.1 one million non-contiguous entries keep heap growth scaled to directory count", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeLargeNonContiguousStats(input), ShouldBeNil)
		So(a4DirectoryRows*20, ShouldBeLessThanOrEqualTo, a4TotalEntries)

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		stopSampling := startHeapInuseSampler(before.HeapInuse)
		sink := new(countingDB)
		err := Build(func() (io.ReadCloser, error) {
			return os.Open(input)
		}, "/", sink, dirbuildRefTime)
		peakGrowth := stopSampling()

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBudget := a4DirCountScaledHeapBudget(a4DirectoryRows)
		retainedGrowth := heapInuseGrowth(before.HeapInuse, after.HeapInuse)
		files, errFiles := regularFilesBelow(dir)

		So(err, ShouldBeNil)
		So(sink.records, ShouldEqual, a4DirectoryRows)
		So(peakGrowth, ShouldBeLessThan, heapBudget)
		So(retainedGrowth, ShouldBeLessThan, heapBudget)
		So(errFiles, ShouldBeNil)
		So(files, ShouldResemble, []string{input})
	})

	Convey("A4.2 the same build creates no chunk or sort scratch artefacts", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeLargeNonContiguousStats(input), ShouldBeNil)

		sink := new(countingDB)
		err := Build(func() (io.ReadCloser, error) {
			return os.Open(input)
		}, "/", sink, dirbuildRefTime)
		artefacts, errArtifacts := chunkSortArtifactsBelow(dir)

		So(err, ShouldBeNil)
		So(sink.records, ShouldEqual, a4DirectoryRows)
		So(errArtifacts, ShouldBeNil)
		So(artefacts, ShouldBeEmpty)
	})

	Convey("A4.3 aged per-directory rollup emits without retaining every materialised output row", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeAgedDirectoryFanoutStats(input, a4AgedFanoutDirs), ShouldBeNil)

		assertAgedFanoutBuildMemory(input, a4AgedFanoutDirs, a4RollupHeapBudget(a4AgedFanoutDirs))
	})

	Convey("A4.4 aged fanout avoids per-directory GUTA age-key maps", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeAgedDirectoryFanoutStats(input, a4CompactFanoutDirs), ShouldBeNil)

		assertAgedFanoutBuildMemory(input, a4CompactFanoutDirs, a4CompactHeapBudget(a4CompactFanoutDirs))
	})

	Convey("A4.5 extreme directory counts route to disk summaries before pass 2", t, func() {
		input := diskBackedRoutingFixtureStats()
		expected, expectedFiles, _, expectedErr := buildWithDirBuildOptions(input, "/", Options{
			DiskNodeThreshold: 1_000_000,
		})
		got, gotFiles, openCalls, gotErr := buildWithDirBuildOptions(input, "/", Options{
			DiskNodeThreshold: 2,
			TempDir:           t.TempDir(),
		})

		So(expectedErr, ShouldBeNil)
		So(gotErr, ShouldBeNil)
		So(openCalls, ShouldEqual, 2)
		So(comparableRecords(got), ShouldResemble, comparableRecords(expected))
		So(gotFiles, ShouldResemble, expectedFiles)
	})

	Convey("A4.6 long path topology does not retain every full stats path", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeLongPathDirectoryStats(input, a4LongPathDirs, a4LongPathNameBytes), ShouldBeNil)
		assertDirectoryIndexMemory(input, a4LongPathDirs+3, a4LongPathIndexHeapBudget(a4LongPathDirs))
	})

	Convey("A4.7 index assignment does not retain the full builder trie", t, func() {
		dir := t.TempDir()
		input := filepath.Join(dir, a4LargeNonContiguousStatsInput)

		So(writeWideDirectoryStats(input, a4AssignmentOverlapDirs, a4AssignmentOverlapNameBytes), ShouldBeNil)
		assertDirectoryIndexMemory(
			input,
			a4AssignmentOverlapDirs+2,
			a4AssignmentIndexHeapBudget(a4AssignmentOverlapDirs),
		)
	})
}

func writeDirectoryOnlyStats(path string, dirRows int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 1024*1024)
	writeErr := writeDirectoryOnlyStatsRows(writer, dirRows)
	flushErr := writer.Flush()
	closeErr := file.Close()

	return errors.Join(writeErr, flushErr, closeErr)
}

func writeDirectoryOnlyStatsRows(writer io.Writer, dirRows int) error {
	if dirRows <= 0 {
		return nil
	}

	if _, err := writer.Write([]byte(statsRow(
		"/",
		stats.DirType,
		4096,
		dirbuildRefUnix,
		dirbuildRefUnix,
		99,
		2,
	))); err != nil {
		return err
	}

	for dirIndex := 1; dirIndex < dirRows; dirIndex++ {
		if _, err := writer.Write([]byte(statsRow(
			fmt.Sprintf("/dir%06d/", dirIndex),
			stats.DirType,
			4096,
			dirbuildRefUnix,
			dirbuildRefUnix,
			int64(2_000_000+dirIndex),
			2,
		))); err != nil {
			return err
		}
	}

	return nil
}

func a4DirectoryIndexHeapBudget(dirRows uint64) uint64 {
	return a4IndexHeapBaseBudgetBytes + dirRows*a4IndexHeapPerDirectoryBytes
}

func writeLargeNonContiguousStats(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 1024*1024)
	writeErr := writeLargeNonContiguousStatsRows(writer)
	flushErr := writer.Flush()
	closeErr := file.Close()

	return errors.Join(writeErr, flushErr, closeErr)
}

func writeLargeNonContiguousStatsRows(writer io.Writer) error {
	leafDirs := a4DirectoryRows - 1
	fileRows := a4TotalEntries - a4DirectoryRows

	for fileIndex := range fileRows {
		dirIndex := fileIndex % leafDirs

		path := fmt.Sprintf("/d%05d/f%06d.txt", dirIndex, fileIndex)
		if _, err := fmt.Fprintf(
			writer,
			"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t%d\t1\t%d\n",
			path,
			int64(1),
			uint32(10),
			uint32(20),
			dirbuildRefUnix-100,
			dirbuildRefUnix-50,
			dirbuildRefUnix-50,
			stats.FileType,
			int64(1_000_000+fileIndex),
			int64(1),
			int64(1),
		); err != nil {
			return err
		}
	}

	rootRow := statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 99, 2)
	if _, err := writer.Write([]byte(rootRow)); err != nil {
		return err
	}

	for offset := range leafDirs {
		dirIndex := leafDirs - 1 - offset
		if _, err := writer.Write([]byte(statsRow(
			fmt.Sprintf("/d%05d/", dirIndex),
			stats.DirType,
			4096,
			dirbuildRefUnix,
			dirbuildRefUnix,
			int64(2_000_000+dirIndex),
			2,
		))); err != nil {
			return err
		}
	}

	return nil
}

func statsRow(
	path string,
	entryType byte,
	size int64,
	atime int64,
	mtime int64,
	inode int64,
	nlink int64,
) string {
	return fmt.Sprintf(
		"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t%d\t1\t%d\n",
		path,
		size,
		uint32(10),
		uint32(20),
		atime,
		mtime,
		mtime,
		entryType,
		inode,
		nlink,
		size,
	)
}

func startHeapInuseSampler(baseline uint64) func() uint64 {
	done := make(chan struct{})
	peak := make(chan uint64, 1)

	go func() {
		ticker := time.NewTicker(a4HeapSamplerInterval)
		defer ticker.Stop()

		maxGrowth := uint64(0)
		sample := func() {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)

			maxGrowth = max(maxGrowth, heapInuseGrowth(baseline, mem.HeapInuse))
		}

		sample()

		for {
			select {
			case <-ticker.C:
				sample()
			case <-done:
				sample()

				peak <- maxGrowth

				return
			}
		}
	}()

	return func() uint64 {
		close(done)

		return <-peak
	}
}

func heapInuseGrowth(before uint64, after uint64) uint64 {
	if after > before {
		return after - before
	}

	return 0
}

func a4DirCountScaledHeapBudget(dirRows uint64) uint64 {
	return a4HeapBaseBudgetBytes + dirRows*a4HeapPerDirectoryBudgetBytes
}

func regularFilesBelow(root string) ([]string, error) {
	files := []string{}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !entry.Type().IsRegular() {
			return nil
		}

		files = append(files, path)

		return nil
	})

	slices.Sort(files)

	return files, err
}

func chunkSortArtifactsBelow(root string) ([]string, error) {
	artefacts := []string{}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		name := entry.Name()
		if entry.Type().IsRegular() && (name == "stats.sorted" || strings.HasSuffix(name, ".bin")) {
			artefacts = append(artefacts, path)
		}

		return nil
	})

	slices.Sort(artefacts)

	return artefacts, err
}

func writeAgedDirectoryFanoutStats(path string, dirs int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 1024*1024)
	writeErr := writeAgedDirectoryFanoutStatsRows(writer, dirs)
	flushErr := writer.Flush()
	closeErr := file.Close()

	return errors.Join(writeErr, flushErr, closeErr)
}

func writeAgedDirectoryFanoutStatsRows(writer io.Writer, dirs int) error {
	old := dirbuildRefUnix - 8*db.SecondsInAYear

	for dirIndex := range dirs {
		if _, err := fmt.Fprintf(
			writer,
			"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t%d\t1\t%d\n",
			fmt.Sprintf("/fanout/d%06d/old.bam", dirIndex),
			int64(1),
			uint32(10),
			uint32(20),
			old,
			old,
			old,
			stats.FileType,
			int64(3_000_000+dirIndex),
			int64(1),
			int64(1),
		); err != nil {
			return err
		}
	}

	return nil
}

func a4RollupHeapBudget(dirRows uint64) uint64 {
	return a4RollupHeapBaseBudgetBytes + dirRows*a4RollupHeapPerDirectoryBytes
}

func a4CompactHeapBudget(dirRows uint64) uint64 {
	return a4CompactHeapBaseBudgetBytes + dirRows*a4CompactHeapPerDirectoryBytes
}

func assertAgedFanoutBuildMemory(input string, dirs int, heapBudget uint64) {
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	stopSampling := startHeapInuseSampler(before.HeapInuse)
	sink := new(countingDB)
	err := Build(func() (io.ReadCloser, error) {
		return os.Open(input)
	}, "/", sink, dirbuildRefTime)
	peakGrowth := stopSampling()

	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	retainedGrowth := heapInuseGrowth(before.HeapInuse, after.HeapInuse)

	So(err, ShouldBeNil)
	So(sink.records, ShouldEqual, dirs+2)
	So(peakGrowth, ShouldBeLessThan, heapBudget)
	So(retainedGrowth, ShouldBeLessThan, heapBudget)
}

type comparableGUTA struct {
	gid         uint32
	uid         uint32
	ft          db.DirGUTAFileType
	age         db.DirGUTAge
	count       uint64
	size        uint64
	atime       int64
	mtime       int64
	atimeRanges summary.AgeBuckets
	mtimeRanges summary.AgeBuckets
}

func comparableRecords(records []db.RecordDGUTA) []comparableRecord {
	out := make([]comparableRecord, 0, len(records))

	for _, record := range records {
		gutas := make([]comparableGUTA, 0, len(record.GUTAs))
		for _, guta := range record.GUTAs {
			gutas = append(gutas, comparableGUTA{
				gid:         guta.GID,
				uid:         guta.UID,
				ft:          guta.FT,
				age:         guta.Age,
				count:       guta.Count,
				size:        guta.Size,
				atime:       guta.Atime,
				mtime:       guta.Mtime,
				atimeRanges: guta.ATimeRanges,
				mtimeRanges: guta.MTimeRanges,
			})
		}

		out = append(out, comparableRecord{
			path:           string(record.Dir.AppendTo(nil)),
			dirID:          record.DirID,
			parentID:       record.ParentID,
			subtreeEnd:     record.SubtreeEnd,
			depth:          record.Depth,
			gutas:          gutas,
			children:       slices.Clone(record.Children),
			childCount:     record.ChildCount,
			childFileCount: record.ChildFileCount,
		})
	}

	slices.SortFunc(out, func(a, b comparableRecord) int {
		return int(a.dirID) - int(b.dirID)
	})

	return out
}

func comparableGUTAMatches(
	guta comparableGUTA,
	gid uint32,
	uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count uint64,
	size uint64,
) bool {
	return guta.gid == gid &&
		guta.uid == uid &&
		guta.ft == ft &&
		guta.age == age &&
		guta.count == count &&
		guta.size == size
}

type comparableRecord struct {
	path           string
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	gutas          []comparableGUTA
	children       []string
	childCount     uint64
	childFileCount uint64
}

func recordsByPath(records []db.RecordDGUTA) map[string]comparableRecord {
	byPath := make(map[string]comparableRecord, len(records))

	for _, record := range comparableRecords(records) {
		byPath[record.path] = record
	}

	return byPath
}

func recordHasGUTA(
	record comparableRecord,
	ft db.DirGUTAFileType,
	size uint64,
) bool {
	for _, guta := range record.gutas {
		if comparableGUTAMatches(guta, 20, 10, ft, db.DGUTAgeAll, 1, size) {
			return true
		}
	}

	return false
}

type overflowAllocator struct {
	entered int
}

func (o *overflowAllocator) SetMountPath(string) error {
	return nil
}

func (o *overflowAllocator) Enter(*summary.DirectoryPath) (uint32, error) {
	o.entered++
	if o.entered > 1 {
		return 0, summary.ErrTooManyDirs
	}

	return 0, nil
}

func (o *overflowAllocator) Leave(*summary.DirectoryPath) (uint32, error) {
	return 0, nil
}

func TestBuild(t *testing.T) {
	Convey("A2.1 permuted input emits the same sorted RecordDGUTA set as the DFS dirguta path", t, func() {
		contiguous := dirbuildFixtureStats("/")
		permuted := permuteStatsLines(contiguous)

		expected, err := summariseWithDirGUTA(contiguous, "/")
		So(err, ShouldBeNil)

		got, err := buildWithDirBuild(permuted, "/")
		So(err, ShouldBeNil)

		So(comparableRecords(got), ShouldResemble, comparableRecords(expected))
	})

	Convey("A2.2 files before parent directory rows resolve to their assigned leaf directories", t, func() {
		input := strings.Join([]string{
			statsRow("/mnt/root/child/early.txt", stats.FileType, 11, dirbuildRefUnix-10, dirbuildRefUnix-5, 1001, 1),
			statsRow("/mnt/other/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
			statsRow("/mnt/other/later.txt", stats.FileType, 7, dirbuildRefUnix-9, dirbuildRefUnix-4, 1002, 1),
			statsRow("/mnt/root/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2002, 2),
			statsRow("/mnt/root/child/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2003, 2),
			statsRow("/mnt/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2004, 2),
		}, "")

		records, err := buildWithDirBuild(input, "/mnt/")
		So(err, ShouldBeNil)

		byPath := recordsByPath(records)
		child := byPath["/mnt/root/child/"]
		other := byPath["/mnt/other/"]

		So(child.parentID, ShouldEqual, byPath["/mnt/root/"].dirID)
		So(other.parentID, ShouldEqual, byPath["/mnt/"].dirID)
		So(child.childFileCount, ShouldEqual, uint64(1))
		So(other.childFileCount, ShouldEqual, uint64(1))
		So(recordHasGUTA(child, db.DGUTAFileTypeText, 11), ShouldBeTrue)
		So(recordHasGUTA(other, db.DGUTAFileTypeText, 7), ShouldBeTrue)
	})

	Convey("A2.3 cross-subdir hardlinks are counted once in the rolled-up parent", t, func() {
		input := strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/parent/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
			statsRow("/parent/a/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2002, 2),
			statsRow("/parent/a/linked.bam", stats.FileType, 100, dirbuildRefUnix-30, dirbuildRefUnix-20, 7777, 2),
			statsRow("/parent/b/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2003, 2),
			statsRow("/parent/b/linked.bam", stats.FileType, 100, dirbuildRefUnix-30, dirbuildRefUnix-20, 7777, 2),
		}, "")

		records, err := buildWithDirBuild(input, "/")
		So(err, ShouldBeNil)

		parent := recordsByPath(records)["/parent/"]
		So(recordHasGUTA(parent, db.DGUTAFileTypeBam, 100), ShouldBeTrue)
	})

	Convey("A2.4 uint32 preorder overflow is returned as ErrTooManyDirs", t, func() {
		input := strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/overflow/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
		}, "")

		restore := replaceDirIDAllocatorForTest(&overflowAllocator{})
		defer restore()

		_, err := buildWithDirBuild(input, "/")
		So(errors.Is(err, summary.ErrTooManyDirs), ShouldBeTrue)
	})

	Convey("A2.5 mount-root ids match the reserved above-root chain and DFS path", t, func() {
		const mountPath = "/lustre/scratch127"

		input := dirbuildFixtureStats(mountPath + "/")
		expected, err := summariseWithDirGUTA(input, mountPath)
		So(err, ShouldBeNil)

		got, err := buildWithDirBuild(input, mountPath)
		So(err, ShouldBeNil)

		expectedByPath := recordsByPath(expected)
		gotByPath := recordsByPath(got)
		mount := gotByPath[mountPath+"/"]

		expectedDirID, err := summary.ReservedDirIDForDepth(2)
		So(err, ShouldBeNil)

		expectedParentID, err := summary.ReservedParentIDForDepth(2)
		So(err, ShouldBeNil)

		So(mount.dirID, ShouldEqual, expectedDirID)
		So(mount.parentID, ShouldEqual, expectedParentID)
		So(mount.dirID, ShouldEqual, expectedByPath[mountPath+"/"].dirID)
		So(mount.parentID, ShouldEqual, expectedByPath[mountPath+"/"].parentID)
		So(gotByPath["/"].subtreeEnd, ShouldEqual, expectedByPath["/"].subtreeEnd)
	})

	Convey("A2.6 file rows imply missing leaf and intermediate directory facts", t, func() {
		input := strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/project/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
			statsRow("/project/missing/leaf/file.txt", stats.FileType, 37, dirbuildRefUnix-30, dirbuildRefUnix-20, 3001, 1),
		}, "")

		expected, err := summariseWithDirGUTA(input, "/")
		So(err, ShouldBeNil)

		got, err := buildWithDirBuild(input, "/")
		So(err, ShouldBeNil)
		So(comparableRecords(got), ShouldResemble, comparableRecords(expected))

		expectedByPath := recordsByPath(expected)
		gotByPath := recordsByPath(got)

		project, ok := gotByPath["/project/"]
		So(ok, ShouldBeTrue)
		missing, ok := gotByPath["/project/missing/"]
		So(ok, ShouldBeTrue)
		leaf, ok := gotByPath["/project/missing/leaf/"]
		So(ok, ShouldBeTrue)

		expectedProject := expectedByPath["/project/"]
		expectedMissing := expectedByPath["/project/missing/"]
		expectedLeaf := expectedByPath["/project/missing/leaf/"]

		So(project.children, ShouldResemble, expectedProject.children)
		So(project.childCount, ShouldEqual, expectedProject.childCount)
		So(project.childFileCount, ShouldEqual, expectedProject.childFileCount)
		So(missing.children, ShouldResemble, expectedMissing.children)
		So(missing.childCount, ShouldEqual, expectedMissing.childCount)
		So(missing.childFileCount, ShouldEqual, expectedMissing.childFileCount)
		So(leaf.parentID, ShouldEqual, expectedLeaf.parentID)
		So(leaf.childCount, ShouldEqual, expectedLeaf.childCount)
		So(leaf.childFileCount, ShouldEqual, expectedLeaf.childFileCount)
		So(recordHasGUTA(leaf, db.DGUTAFileTypeText, 37), ShouldBeTrue)
		So(recordHasGUTA(missing, db.DGUTAFileTypeText, 37), ShouldBeTrue)
		So(recordHasGUTA(project, db.DGUTAFileTypeText, 37), ShouldBeTrue)
	})
}

func dirbuildFixtureStats(rootPath string) string {
	root := statsdata.NewRoot(rootPath, dirbuildRefUnix)
	statsdata.AddFile(root, "alpha/bravo/file.txt", 10, 20, 11, dirbuildRefUnix-30, dirbuildRefUnix-20)
	statsdata.AddFile(root, "alpha/top.bam", 11, 20, 13, dirbuildRefUnix-40, dirbuildRefUnix-10)
	statsdata.AddFile(root, "beta/tmp/temp.cram", 12, 21, 17, dirbuildRefUnix-50, dirbuildRefUnix-5)
	statsdata.AddFileWithInode(root, "beta/link-a.bam", 13, 22, 19, dirbuildRefUnix-60, dirbuildRefUnix-6, 9001, 2)
	statsdata.AddFileWithInode(root, "gamma/link-b.bam", 13, 22, 19, dirbuildRefUnix-60, dirbuildRefUnix-6, 9001, 2)

	reader := root.AsReader()
	defer reader.Close()

	data, err := io.ReadAll(reader)
	So(err, ShouldBeNil)

	return string(data)
}

func permuteStatsLines(data string) string {
	lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
	slices.Reverse(lines)

	return strings.Join(lines, "\n") + "\n"
}

func TestBuildDiskHardlinkParity(t *testing.T) {
	Convey("forced disk summaries emit the same hardlink aggregates as memory summaries", t, func() {
		for name, input := range diskHardlinkFixtures() {
			Convey(name, func() {
				expected, _, _, expectedErr := buildWithDirBuildOptions(input, "/", Options{
					DiskNodeThreshold: 1_000_000,
				})
				got, _, _, gotErr := buildWithDirBuildOptions(input, "/", Options{
					DiskNodeThreshold: 1,
					TempDir:           t.TempDir(),
				})

				So(expectedErr, ShouldBeNil)
				So(gotErr, ShouldBeNil)
				So(comparableRecords(got), ShouldResemble, comparableRecords(expected))
				So(inexactAggregateRows(got), ShouldEqual, 0)
				So(inexactAggregateRows(expected), ShouldEqual, 0)
			})
		}
	})
}

func diskHardlinkFixtures() map[string]string {
	return map[string]string{
		"same directory changes aggregate key and age mask": strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2000, 2),
			statsRow("/same/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2001, 2),
			statsRow("/same/old.txt", stats.FileType, 10,
				dirbuildRefUnix-8*db.SecondsInAYear, dirbuildRefUnix-8*db.SecondsInAYear, 3001, 2),
			statsRow("/same/recent.bam", stats.FileType, 20,
				dirbuildRefUnix-db.SecondsInAMonth, dirbuildRefUnix-db.SecondsInAMonth, 3001, 2),
			statsRow("/same/ordinary.bam", stats.FileType, 7,
				dirbuildRefUnix-2*db.SecondsInAYear, dirbuildRefUnix-db.SecondsInAYear, 4001, 1),
		}, ""),
		"sibling and nested directories merge exact inode summaries": strings.Join([]string{
			statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2100, 2),
			statsRow("/tree/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2101, 2),
			statsRow("/tree/left/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2102, 2),
			statsRow("/tree/right/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2103, 2),
			statsRow("/tree/right/nested/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2104, 2),
			statsRow("/tree/left/sibling.cram", stats.FileType, 31,
				dirbuildRefUnix-5*db.SecondsInAYear, dirbuildRefUnix-3*db.SecondsInAYear, 3101, 2),
			statsRow("/tree/right/sibling.txt", stats.FileType, 37,
				dirbuildRefUnix-db.SecondsInAYear, dirbuildRefUnix-db.SecondsInAMonth, 3101, 2),
			statsRow("/tree/left/nested.bam", stats.FileType, 41,
				dirbuildRefUnix-7*db.SecondsInAYear, dirbuildRefUnix-2*db.SecondsInAYear, 3102, 2),
			statsRow("/tree/right/nested/nested.cram", stats.FileType, 43,
				dirbuildRefUnix-2*db.SecondsInAYear, dirbuildRefUnix-db.SecondsInAMonth, 3102, 2),
		}, ""),
		"every age bucket remains exact": diskHardlinkAgeBucketFixture(),
	}
}

func diskHardlinkAgeBucketFixture() string {
	offsets := [...]int64{
		8 * db.SecondsInAYear,
		6 * db.SecondsInAYear,
		4 * db.SecondsInAYear,
		2*db.SecondsInAYear + db.SecondsInAMonth,
		db.SecondsInAYear + db.SecondsInAMonth,
		7 * db.SecondsInAMonth,
		3 * db.SecondsInAMonth,
		db.SecondsInAMonth + 24*60*60,
		24 * 60 * 60,
	}

	var input strings.Builder
	input.WriteString(statsRow("/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2200, 2))
	input.WriteString(statsRow("/buckets/", stats.DirType, 4096, dirbuildRefUnix, dirbuildRefUnix, 2201, 2))

	for index, offset := range offsets {
		inode := int64(3200 + index)
		stamp := dirbuildRefUnix - offset
		input.WriteString(statsRow(
			fmt.Sprintf("/buckets/link-%d-a.txt", index), stats.FileType, int64(index+1), stamp, stamp, inode, 2,
		))
		input.WriteString(statsRow(
			fmt.Sprintf("/buckets/link-%d-b.txt", index), stats.FileType, int64(index+1), stamp, stamp, inode, 2,
		))
	}

	return input.String()
}

func inexactAggregateRows(records []db.RecordDGUTA) int {
	inexact := 0

	for _, record := range records {
		for _, guta := range record.GUTAs {
			atimeBucketCount := uint64(0)
			mtimeBucketCount := uint64(0)

			for index := range guta.ATimeRanges {
				atimeBucketCount += guta.ATimeRanges[index]
				mtimeBucketCount += guta.MTimeRanges[index]
			}

			staleTimes := guta.Count == 0 && (guta.Atime != 0 || guta.Mtime != 0)
			incorrectBuckets := atimeBucketCount != guta.Count || mtimeBucketCount != guta.Count

			if staleTimes || incorrectBuckets {
				inexact++
			}
		}
	}

	return inexact
}
