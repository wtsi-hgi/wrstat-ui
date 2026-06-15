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

package dirguta

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const idAssignRefTime = int64(1779120209)

type idRecord struct {
	path       string
	dirID      uint32
	parentID   uint32
	subtreeEnd uint32
	depth      uint16
}

func summariseIDStats(root *statsdata.Directory) ([]idRecord, error) {
	reader := root.AsReader()
	defer reader.Close()

	return summariseIDReader(reader)
}

func assertGapFreePreorder(records []idRecord) {
	seenIDs := make(map[uint32]struct{}, len(records))

	var maxID uint32

	So(records, ShouldNotBeEmpty)

	for _, record := range records {
		seenIDs[record.dirID] = struct{}{}
		maxID = max(maxID, record.dirID)
	}

	So(len(seenIDs), ShouldEqual, len(records))

	var expectedID uint32
	for range records {
		_, ok := seenIDs[expectedID]
		So(ok, ShouldBeTrue)

		expectedID++
	}

	So(maxID, ShouldEqual, expectedID-1)
}

func idRecordsByPath(records []idRecord) map[string]idRecord {
	byPath := make(map[string]idRecord, len(records))

	for _, record := range records {
		byPath[record.path] = record
	}

	return byPath
}

func serializeIDRecords(records []idRecord) string {
	var b strings.Builder

	for _, record := range records {
		fmt.Fprintf(
			&b,
			"%s\t%d\t%d\t%d\n",
			record.path,
			record.dirID,
			record.parentID,
			record.subtreeEnd,
		)
	}

	return b.String()
}

func intervalInvariantViolations(records []idRecord) int {
	var violations int

	for _, parent := range records {
		intervalPaths := make(map[string]struct{})
		descendantPaths := make(map[string]struct{})

		for _, child := range records {
			if child.dirID >= parent.dirID && child.dirID < parent.subtreeEnd {
				intervalPaths[child.path] = struct{}{}
			}

			if descendantOrSelf(parent.path, child.path) {
				descendantPaths[child.path] = struct{}{}
			}
		}

		if !maps.Equal(intervalPaths, descendantPaths) {
			violations++
		}
	}

	return violations
}

func descendantOrSelf(parent string, child string) bool {
	return parent == "/" || child == parent || strings.HasPrefix(child, parent)
}

func summariseIDReader(r io.Reader) ([]idRecord, error) {
	s := summary.NewSummariser(stats.NewStatsParser(r))
	sink := new(idCaptureDB)

	s.AddDirectoryOperation(newDirGroupUserTypeAge(sink, idAssignRefTime, idAssignRefTime))

	return sink.records, s.Summarise()
}

func countPath(records []idRecord, path string) int {
	var count int

	for _, record := range records {
		if record.path == path {
			count++
		}
	}

	return count
}

func maxSubtreeEnd(records []idRecord) uint32 {
	var next uint32

	for _, record := range records {
		next = max(next, record.subtreeEnd)
	}

	return next
}

type idCaptureDB struct {
	records []idRecord
}

func (d *idCaptureDB) Add(dguta db.RecordDGUTA) error {
	d.records = append(d.records, idRecord{
		path:       string(dguta.Dir.AppendTo(nil)),
		dirID:      dguta.DirID,
		parentID:   dguta.ParentID,
		subtreeEnd: dguta.SubtreeEnd,
		depth:      dguta.Depth,
	})

	return nil
}

func TestDirGUTAIDAssignment(t *testing.T) {
	Convey("B1.1 assigns gap-free preorder ids on directory boundaries", t, func() {
		root := statsdata.NewRoot("/", idAssignRefTime)
		statsdata.AddFile(root, "a/b/f1", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "a/b_mid.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "a/c/f2", 10, 20, 1, idAssignRefTime, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)
		assertGapFreePreorder(records)

		byPath := idRecordsByPath(records)
		a := byPath["/a/"]
		b := byPath["/a/b/"]
		c := byPath["/a/c/"]

		So(byPath["/"].parentID, ShouldEqual, parentSentinel)
		So(a.dirID, ShouldBeLessThan, b.dirID)
		So(b.subtreeEnd, ShouldBeLessThanOrEqualTo, c.dirID)
		So(a.subtreeEnd, ShouldEqual, max(b.subtreeEnd, c.subtreeEnd))
		So(a.depth, ShouldEqual, uint16(1))
		So(b.parentID, ShouldEqual, a.dirID)
		So(c.parentID, ShouldEqual, a.dirID)
	})

	Convey("B1.2 assigns byte-for-byte deterministic id tuples", t, func() {
		buildFixture := func() *statsdata.Directory {
			root := statsdata.NewRoot("/", idAssignRefTime)
			statsdata.AddFile(root, "a/b/f1", 10, 20, 1, idAssignRefTime, idAssignRefTime)
			statsdata.AddFile(root, "a/b_mid.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
			statsdata.AddFile(root, "a/c/f2", 10, 20, 1, idAssignRefTime, idAssignRefTime)

			return root
		}

		first, err := summariseIDStats(buildFixture())
		So(err, ShouldBeNil)

		second, err := summariseIDStats(buildFixture())
		So(err, ShouldBeNil)

		So(serializeIDRecords(first), ShouldEqual, serializeIDRecords(second))
	})

	Convey("B1.3 interleaved files do not consume directory ids", t, func() {
		root := statsdata.NewRoot("/", idAssignRefTime)
		statsdata.AddFile(root, "a/a/leaf.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "a/b.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "a/c/leaf.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)
		assertGapFreePreorder(records)

		byPath := idRecordsByPath(records)
		So(byPath["/a/a/"].subtreeEnd, ShouldEqual, byPath["/a/c/"].dirID)
		So(byPath["/a/c/"].dirID, ShouldEqual, byPath["/a/a/"].dirID+1)
		So(len(records), ShouldEqual, 4)
	})

	Convey("B1.4 assigned intervals contain exactly each directory and its descendants", t, func() {
		root := internaldata.CreateDefaultTestData(1, 2, 0, 3, 4, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)

		So(intervalInvariantViolations(records), ShouldEqual, 0)
	})

	Convey("B1.5 counter overflow returns ErrTooManyDirs without emitting the sentinel id", t, func() {
		sink := new(idCaptureDB)
		generator := newDirGroupUserTypeAge(sink, idAssignRefTime, idAssignRefTime)
		rootOp, ok := generator().(*DirGroupUserTypeAge)
		So(ok, ShouldBeTrue)

		childOp, ok := generator().(*DirGroupUserTypeAge)
		So(ok, ShouldBeTrue)

		rootOp.idAssigner.next = parentSentinel - 1

		paths := internaltest.NewDirectoryPathCreator()
		rootInfo := &summary.FileInfo{
			Path:      paths.ToDirectoryPath("/"),
			Name:      []byte("/"),
			EntryType: stats.DirType,
		}
		nextInfo := &summary.FileInfo{
			Path:      paths.ToDirectoryPath("/overflow/"),
			Name:      []byte("overflow/"),
			EntryType: stats.DirType,
		}

		So(rootOp.Add(rootInfo), ShouldBeNil)

		err := childOp.Add(nextInfo)
		So(errors.Is(err, ErrTooManyDirs), ShouldBeTrue)

		sentinelRows := 0

		for _, record := range sink.records {
			if record.dirID == parentSentinel {
				sentinelRows++
			}
		}

		So(sentinelRows, ShouldEqual, 0)
		So(sink.records, ShouldBeEmpty)
	})

	Convey("B1.6 re-entering a closed directory boundary returns ErrNonContiguousInput", t, func() {
		var input strings.Builder
		writeDirectoryStatsRow(&input, "/", 10, 20, idAssignRefTime, idAssignRefTime, 1, 1)
		writeDirectoryStatsRow(&input, "/a/", 10, 20, idAssignRefTime, idAssignRefTime, 1, 1)
		writeDirectoryStatsRow(&input, "/a/b/", 10, 20, idAssignRefTime, idAssignRefTime, 2, 1)
		writeDirectoryStatsRow(&input, "/a/", 10, 20, idAssignRefTime, idAssignRefTime, 3, 1)

		records, err := summariseIDReader(strings.NewReader(input.String()))
		So(errors.Is(err, ErrNonContiguousInput), ShouldBeTrue)
		So(countPath(records, "/a/"), ShouldEqual, 1)
	})

	Convey("B2.1 reserves low ids for the above-root chain", t, func() {
		root := statsdata.NewRoot("/lustre/scratch125/teamX/", idAssignRefTime)
		statsdata.AddFile(root, "alpha/file.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)

		byPath := idRecordsByPath(records)
		So(byPath["/"].dirID, ShouldEqual, uint32(0))
		So(byPath["/lustre/"].dirID, ShouldEqual, uint32(1))
		So(byPath["/lustre/scratch125/"].dirID, ShouldEqual, uint32(2))
		So(byPath["/lustre/scratch125/teamX/"].dirID, ShouldEqual, uint32(3))
		So(byPath["/lustre/scratch125/teamX/"].parentID, ShouldEqual, uint32(2))
		So(byPath["/lustre/scratch125/teamX/alpha/"].dirID, ShouldBeGreaterThanOrEqualTo, uint32(4))
	})

	Convey("B2.2 backfills ancestor subtree_end to the final counter", t, func() {
		root := statsdata.NewRoot("/lustre/scratch125/teamX/", idAssignRefTime)
		statsdata.AddFile(root, "alpha/file.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "beta/file.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)

		next := maxSubtreeEnd(records)
		byPath := idRecordsByPath(records)
		So(byPath["/"].subtreeEnd, ShouldEqual, next)
		So(byPath["/lustre/"].subtreeEnd, ShouldEqual, next)
		So(byPath["/lustre/scratch125/"].subtreeEnd, ShouldEqual, next)
		So(intervalInvariantViolations(records), ShouldEqual, 0)
	})

	Convey("B2.3 root sentinel interval spans every catalog row", t, func() {
		root := statsdata.NewRoot("/lustre/scratch125/teamX/", idAssignRefTime)
		statsdata.AddFile(root, "alpha/file.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)
		statsdata.AddFile(root, "beta/file.txt", 10, 20, 1, idAssignRefTime, idAssignRefTime)

		records, err := summariseIDStats(root)
		So(err, ShouldBeNil)

		next := maxSubtreeEnd(records)
		rootRecord := idRecordsByPath(records)["/"]
		So(rootRecord.dirID, ShouldEqual, uint32(0))
		So(rootRecord.parentID, ShouldEqual, parentSentinel)
		So(rootRecord.subtreeEnd, ShouldEqual, next)

		var spannedRows int

		for _, record := range records {
			if record.dirID >= rootRecord.dirID && record.dirID < rootRecord.subtreeEnd {
				spannedRows++
			}
		}

		So(spannedRows, ShouldEqual, len(records))
	})
}
