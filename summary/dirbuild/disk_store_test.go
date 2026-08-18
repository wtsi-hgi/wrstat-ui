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
	"runtime"
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

func TestSQLiteDiskSummarySingleReadRollup(t *testing.T) {
	Convey("disk roll-up selects each compact directory row set once and reports phase I/O", t, func() {
		metrics := new(DiskMetrics)
		metrics.readProcessWriteBytes = processWriteByteSequence(100, 460, 500, 1_200)
		records, _, _, err := buildWithDirBuildOptions(diskBackedRoutingFixtureStats(), "/", Options{
			DiskNodeThreshold: 1,
			TempDir:           t.TempDir(),
			DiskMetrics:       metrics,
		})

		So(err, ShouldBeNil)
		So(metrics.SelectStatements, ShouldEqual, uint64(len(records)))
		So(metrics.SQLiteStatements, ShouldBeGreaterThan, metrics.SelectStatements)
		So(metrics.DatabaseBytes, ShouldBeGreaterThan, uint64(0))
		So(metrics.ProcessWriteBytesAvailable, ShouldBeTrue)
		So(metrics.Pass2ProcessWriteBytes, ShouldEqual, uint64(360))
		So(metrics.RollupProcessWriteBytes, ShouldEqual, uint64(700))
		So(metrics.ProcessWriteBytes, ShouldEqual,
			metrics.Pass2ProcessWriteBytes+metrics.RollupProcessWriteBytes)
		So(metrics.Pass2Elapsed, ShouldBeGreaterThan, 0)
		So(metrics.RollupElapsed, ShouldBeGreaterThan, 0)
	})
}

func processWriteByteSequence(values ...uint64) func() (uint64, error) {
	index := 0

	return func() (uint64, error) {
		value := values[index]
		index++

		return value, nil
	}
}

func TestSQLiteDiskSummaryWriteBehind(t *testing.T) {
	Convey("repeated facts combine within a fixed byte limit and flush in key order", t, func() {
		metrics := new(DiskMetrics)
		store, err := openSQLiteDiskSummaryStore(t.TempDir(), dirbuildRefUnix, false)
		So(err, ShouldBeNil)

		store.metrics = metrics
		store.writeBehindLimit = 3 * sqliteDiskSummaryAccumulatorEntryBytes

		var flushed [][]diskSummaryKey

		store.observeFlush = func(keys []diskSummaryKey) {
			flushed = append(flushed, slices.Clone(keys))
		}

		keys := []dirguta.GUTAKey{
			{GID: 3, UID: 2, FileType: db.DGUTAFileTypeText, Age: db.DGUTAgeAll},
		}
		for range 5 {
			So(store.addFile(9, keys, 1, dirbuildRefUnix, dirbuildRefUnix), ShouldBeNil)
		}

		for gid := uint32(8); gid > 3; gid-- {
			keys[0].GID = gid
			So(store.addFile(gid, keys, 1, dirbuildRefUnix, dirbuildRefUnix), ShouldBeNil)
		}

		So(store.flush(), ShouldBeNil)
		So(store.close(true), ShouldBeNil)

		So(metrics.WriteBehindLimitBytes, ShouldEqual, uint64(3*sqliteDiskSummaryAccumulatorEntryBytes))
		So(metrics.MaxWriteBehindBytes, ShouldBeLessThanOrEqualTo, metrics.WriteBehindLimitBytes)
		So(metrics.RowsReceived, ShouldEqual, uint64(10))
		So(metrics.RowsCombined, ShouldBeGreaterThan, uint64(0))
		So(metrics.MaxRowsCombinedPerFlush, ShouldBeGreaterThan, uint64(0))
		So(metrics.RowsWritten, ShouldBeLessThan, metrics.RowsReceived)
		So(flushed, ShouldNotBeEmpty)
		So(unsortedDiskSummaryFlushes(flushed), ShouldEqual, 0)
	})
}

func unsortedDiskSummaryFlushes(flushes [][]diskSummaryKey) int {
	unsorted := 0

	for _, keys := range flushes {
		if !slices.IsSortedFunc(keys, compareDiskSummaryKeys) {
			unsorted++
		}
	}

	return unsorted
}

func TestSQLiteDiskSummaryAccumulatorHeap(t *testing.T) {
	Convey("retained accumulator heap stays fixed as directory cardinality increases", t, func() {
		const (
			smallerDirectoryCount = 20_000
			largerDirectoryCount  = 200_000
			retainedHeapBudget    = 12 * 1024 * 1024
			scaleGrowthBudget     = 4 * 1024 * 1024
		)

		smallerGrowth := retainedAccumulatorHeapGrowth(t, smallerDirectoryCount)
		largerGrowth := retainedAccumulatorHeapGrowth(t, largerDirectoryCount)
		scaleGrowth := heapInuseGrowth(smallerGrowth, largerGrowth)
		t.Logf("retained accumulator heap: %d dirs=%d bytes, %d dirs=%d bytes, scale delta=%d bytes",
			smallerDirectoryCount, smallerGrowth, largerDirectoryCount, largerGrowth, scaleGrowth)

		So(smallerGrowth, ShouldBeLessThan, uint64(retainedHeapBudget))
		So(largerGrowth, ShouldBeLessThan, uint64(retainedHeapBudget))
		So(scaleGrowth, ShouldBeLessThan, uint64(scaleGrowthBudget))
	})
}

func retainedAccumulatorHeapGrowth(t *testing.T, directoryCount int) uint64 {
	t.Helper()

	store, err := openSQLiteDiskSummaryStore(t.TempDir(), dirbuildRefUnix, false)
	So(err, ShouldBeNil)

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	keys := []dirguta.GUTAKey{{FileType: db.DGUTAFileTypeText, Age: db.DGUTAgeAll}}

	var addErr error

	for dirID := range directoryCount {
		addErr = store.addFile(uint32(dirID), keys, 1, dirbuildRefUnix, dirbuildRefUnix)
		if addErr != nil {
			break
		}
	}

	So(addErr, ShouldBeNil)

	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(store)

	So(store.close(true), ShouldBeNil)

	return heapInuseGrowth(before.HeapInuse, after.HeapInuse)
}
