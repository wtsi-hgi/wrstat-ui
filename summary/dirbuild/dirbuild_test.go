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
	a4TotalEntries      = 1_000_000
	a4DirectoryRows     = 50_000
	a4DirectoryOnlyRows = 250_000
	a4AgedFanoutDirs    = 80_000
	// A4's heap budget scales with directory rows, not file rows.
	a4HeapBaseBudgetBytes          = 64 * 1024 * 1024
	a4HeapPerDirectoryBudgetBytes  = 8 * 1024
	a4IndexHeapBaseBudgetBytes     = 48 * 1024 * 1024
	a4IndexHeapPerDirectoryBytes   = 430
	a4RollupHeapBaseBudgetBytes    = 128 * 1024 * 1024
	a4RollupHeapPerDirectoryBytes  = 6 * 1024
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

func buildWithDirBuild(data string, mountPath string) ([]db.RecordDGUTA, error) {
	sink := new(captureDB)
	err := Build(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(data)), nil
	}, mountPath, sink, dirbuildRefTime)

	return sink.records, err
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

		stopSampling := startHeapInuseSampler(before.HeapInuse, a4HeapSamplerInterval)
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

		stopSampling := startHeapInuseSampler(before.HeapInuse, a4HeapSamplerInterval)
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

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		stopSampling := startHeapInuseSampler(before.HeapInuse, a4HeapSamplerInterval)
		sink := new(countingDB)
		err := Build(func() (io.ReadCloser, error) {
			return os.Open(input)
		}, "/", sink, dirbuildRefTime)
		peakGrowth := stopSampling()

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBudget := a4RollupHeapBudget(a4AgedFanoutDirs)
		retainedGrowth := heapInuseGrowth(before.HeapInuse, after.HeapInuse)

		So(err, ShouldBeNil)
		So(sink.records, ShouldEqual, a4AgedFanoutDirs+2)
		So(peakGrowth, ShouldBeLessThan, heapBudget)
		So(retainedGrowth, ShouldBeLessThan, heapBudget)
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

func startHeapInuseSampler(baseline uint64, interval time.Duration) func() uint64 {
	done := make(chan struct{})
	peak := make(chan uint64, 1)

	go func() {
		ticker := time.NewTicker(interval)
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
