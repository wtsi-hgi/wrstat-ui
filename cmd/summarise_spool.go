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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	clickHouseSpoolDirName        = ".wrstat-ui-clickhouse-spool"
	clickHouseSpoolLoadReportName = "spool_load_report.json"
	clickHouseSpoolSchemaMark     = "wrstat-ui-clickhouse-summarise-spool-v2"
	clickHouseSpoolSchema3Version = 1
)

var (
	loadSummariseClickHouseSpool = clickhouse.LoadSummariseSpoolReport
	summariseSpoolNow            = time.Now
	summariseSpoolDirGUTANow     = time.Now
)

var (
	errSummariseSpoolFileDirRequired       = errors.New("clickhouse spool: file row requires directory path")
	errSummariseSpoolFileNameRequired      = errors.New("clickhouse spool: file row requires entry name")
	errSummariseSpoolFileNonNegative       = errors.New("clickhouse spool: file row requires non-negative numeric fields")
	errSummariseSpoolDirFactsDir           = errors.New("clickhouse spool: dir facts require dir")
	errSummariseSpoolBasedirsNotReset      = errors.New("clickhouse spool: basedirs store not reset")
	errSummariseSpoolSubdirPositionInvalid = errors.New("clickhouse spool: basedirs subdir position overflows UInt32")
)

type summariseFullFilterTupleKey struct {
	age uint8
	gid uint32
	uid uint32
	ft  uint16
}

func summariseFullFilterKeyForRow(row chspool.DirFilterAllRow) summariseFullFilterTupleKey {
	return summariseFullFilterTupleKey{
		age: row.Age,
		gid: row.GID,
		uid: row.UID,
		ft:  row.FT,
	}
}

func summariseActiveVirtualFilterRowsForSummary(
	summary summariseActiveVirtualSummaryRow,
	contributors []string,
	rootFilterRows map[string][]chspool.DirFilterAllRow,
	refreshedAt time.Time,
) []summariseActiveVirtualFilterAllRow {
	aggregates := make(map[summariseFullFilterTupleKey]summariseActiveVirtualFilterAggregate)
	keys := make(map[summariseFullFilterTupleKey]chspool.DirFilterAllRow)

	for _, mountPath := range contributors {
		for _, row := range rootFilterRows[mountPath] {
			key := summariseFullFilterKeyForRow(row)
			keys[key] = row
			aggregates[key] = summariseAddActiveVirtualFilterAggregate(aggregates[key], row)
		}
	}

	rows := make([]summariseActiveVirtualFilterAllRow, 0, len(aggregates))
	for key, aggregate := range aggregates {
		row := keys[key]
		rows = append(rows, summariseActiveVirtualFilterRow(summary, row, aggregate, refreshedAt))
	}

	return rows
}

func summariseAddActiveVirtualFilterAggregate(
	aggregate summariseActiveVirtualFilterAggregate,
	row chspool.DirFilterAllRow,
) summariseActiveVirtualFilterAggregate {
	aggregate.count += row.Count
	aggregate.size += row.Size
	aggregate.atimeMin = summariseMinNonZeroInt64(aggregate.atimeMin, row.AtimeMin)
	aggregate.mtimeMax = max(aggregate.mtimeMax, row.MtimeMax)
	aggregate.atimeBuckets = summariseSumUint64Slices(aggregate.atimeBuckets, row.AtimeBuckets)
	aggregate.mtimeBuckets = summariseSumUint64Slices(aggregate.mtimeBuckets, row.MtimeBuckets)

	return aggregate
}

func summariseMinNonZeroInt64(a int64, b int64) int64 {
	if a == 0 {
		return b
	}

	if b == 0 {
		return a
	}

	return min(a, b)
}

func summariseSumUint64Slices(a []uint64, b []uint64) []uint64 {
	if len(a) == 0 {
		return append([]uint64(nil), b...)
	}

	for idx, count := range b {
		if idx >= len(a) {
			a = append(a, count)

			continue
		}

		a[idx] += count
	}

	return a
}

func summariseActiveVirtualFilterRow(
	summary summariseActiveVirtualSummaryRow,
	row chspool.DirFilterAllRow,
	aggregate summariseActiveVirtualFilterAggregate,
	refreshedAt time.Time,
) summariseActiveVirtualFilterAllRow {
	return summariseActiveVirtualFilterAllRow{
		ActiveSetID:      summary.ActiveSetID,
		Dir:              summary.Dir,
		Age:              row.Age,
		GID:              row.GID,
		UID:              row.UID,
		FT:               row.FT,
		Count:            aggregate.count,
		Size:             aggregate.size,
		AtimeMin:         aggregate.atimeMin,
		MtimeMax:         aggregate.mtimeMax,
		AtimeBuckets:     aggregate.atimeBuckets,
		MtimeBuckets:     aggregate.mtimeBuckets,
		FilterChildCount: summary.ChildCount,
		ChildCount:       summary.ChildCount,
		RefreshedAt:      refreshedAt,
	}
}

type summariseFullFilterChildTupleKey struct {
	childDir string
	tuple    summariseFullFilterTupleKey
}

type summariseFullFilterPendingDir struct {
	dir       string
	rows      []chspool.DirFilterAllRow
	seenChild map[summariseFullFilterChildTupleKey]struct{}
}

type summariseMountActiveRow struct {
	mountPath  string
	snapshotID string
	updatedAt  time.Time
}

func summariseVirtualChildRowsForMounts(activeRows []summariseMountActiveRow) []summariseVirtualChildRow {
	byKey := make(map[string]summariseVirtualChildRow)

	for _, activeRow := range activeRows {
		for _, row := range summariseVirtualChildRowsForMount(activeRow.mountPath) {
			key := row.parentDir + "\x00" + row.childDir
			if existing, ok := byKey[key]; ok {
				byKey[key] = summariseMergeVirtualChildRows(existing, row)

				continue
			}

			byKey[key] = row
		}
	}

	rows := make([]summariseVirtualChildRow, 0, len(byKey))
	for _, row := range byKey {
		rows = append(rows, row)
	}

	slices.SortFunc(rows, func(a, b summariseVirtualChildRow) int {
		if cmpParent := strings.Compare(a.parentDir, b.parentDir); cmpParent != 0 {
			return cmpParent
		}

		return strings.Compare(a.childDir, b.childDir)
	})

	return rows
}

func summariseMergeVirtualChildRows(a, b summariseVirtualChildRow) summariseVirtualChildRow {
	if !b.childIsMountRoot {
		return a
	}

	a.childIsMountRoot = true
	a.mountPath = b.mountPath

	return a
}

func summariseSeedActiveVirtualChildSummaryRows(
	activeSetID string,
	childRows []summariseActiveVirtualChildRow,
	dirs map[string]summariseActiveVirtualSummaryRow,
	childCounts map[string]uint64,
) {
	for _, row := range childRows {
		childCounts[row.ParentDir]++
		dirs[row.ParentDir] = summariseActiveVirtualSummaryRow{ActiveSetID: activeSetID, Dir: row.ParentDir}
		childDir := summariseEnsureTrailingSlash(row.ChildDir)
		dirs[childDir] = summariseActiveVirtualSummaryRow{
			ActiveSetID:    activeSetID,
			Dir:            childDir,
			MountPath:      row.MountPath,
			IsMountRootBox: row.IsMountRootBox,
		}
	}
}

func summariseSeedActiveVirtualMountRootRows(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	dirs map[string]summariseActiveVirtualSummaryRow,
) {
	for _, activeRow := range activeRows {
		dir := summariseEnsureTrailingSlash(activeRow.mountPath)
		row := dirs[dir]
		row.ActiveSetID = activeSetID
		row.Dir = dir
		row.MountPath = dir
		row.IsMountRootBox = 1
		dirs[dir] = row
	}
}

func summariseMaxUpdatedAtForActiveRows(activeRows []summariseMountActiveRow) time.Time {
	var updatedAt time.Time

	for _, row := range activeRows {
		rowUpdatedAt := summariseActiveSetUpdatedAt(row.updatedAt)
		if rowUpdatedAt.After(updatedAt) {
			updatedAt = rowUpdatedAt
		}
	}

	return updatedAt
}

func summariseActiveSetUpdatedAt(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}

	return t.UTC().Truncate(time.Second)
}

func summariseActiveVirtualContributors(
	summaryRows []summariseActiveVirtualSummaryRow,
	activeRows []summariseMountActiveRow,
) map[string][]string {
	contributors := make(map[string][]string, len(summaryRows))

	for _, summary := range summaryRows {
		if summary.IsMountRootBox == 1 {
			contributors[summary.Dir] = []string{summary.MountPath}

			continue
		}

		for _, activeRow := range activeRows {
			if strings.HasPrefix(activeRow.mountPath, summary.Dir) {
				contributors[summary.Dir] = append(contributors[summary.Dir], activeRow.mountPath)
			}
		}
	}

	return contributors
}

type summariseActiveVirtualSummaryRow struct {
	ActiveSetID     string
	Dir             string
	MountPath       string
	IsMountRootBox  uint8
	UpdatedAt       time.Time
	AllCount        uint64
	AllSize         uint64
	AllAtimeMin     int64
	AllMtimeMax     int64
	AllAtimeBuckets []uint64
	AllMtimeBuckets []uint64
	AllUIDs         []uint32
	AllGIDs         []uint32
	AllFT           uint16
	FileCount       uint64
	FileSize        uint64
	ChildCount      uint64
	RefreshedAt     time.Time
}

type summariseActiveVirtualFilterAllRow struct {
	ActiveSetID      string
	Dir              string
	Age              uint8
	GID              uint32
	UID              uint32
	FT               uint16
	Count            uint64
	Size             uint64
	AtimeMin         int64
	MtimeMax         int64
	AtimeBuckets     []uint64
	MtimeBuckets     []uint64
	FilterChildCount uint64
	ChildCount       uint64
	RefreshedAt      time.Time
}

type summariseActiveVirtualChildRow struct {
	ActiveSetID    string
	ParentDir      string
	ChildDir       string
	MountPath      string
	IsMountRootBox uint8
	ChildCount     uint64
	RefreshedAt    time.Time
}

type summariseActiveVirtualFilterAggregate struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

type summariseActiveVirtualSummaryAggregate struct {
	allCount        uint64
	allSize         uint64
	allAtimeMin     int64
	allMtimeMax     int64
	allAtimeBuckets []uint64
	allMtimeBuckets []uint64
	allUIDs         []uint32
	allGIDs         []uint32
	allFT           uint16
	fileCount       uint64
	fileSize        uint64
}

func summariseActiveVirtualSummaryAggregateForContributors(
	contributors []string,
	rootFacts map[string]chspool.DirFactRow,
) summariseActiveVirtualSummaryAggregate {
	var aggregate summariseActiveVirtualSummaryAggregate

	for _, mountPath := range contributors {
		row, ok := rootFacts[mountPath]
		if !ok {
			continue
		}

		aggregate = summariseAddActiveVirtualSummaryAggregate(aggregate, row)
	}

	return aggregate
}

func summariseAddActiveVirtualSummaryAggregate(
	aggregate summariseActiveVirtualSummaryAggregate,
	row chspool.DirFactRow,
) summariseActiveVirtualSummaryAggregate {
	aggregate.allCount += row.AllCount
	aggregate.allSize += row.AllSize
	aggregate.allAtimeMin = summariseMinNonZeroInt64(aggregate.allAtimeMin, row.AllAtimeMin)
	aggregate.allMtimeMax = max(aggregate.allMtimeMax, row.AllMtimeMax)
	aggregate.allAtimeBuckets = summariseSumUint64Slices(aggregate.allAtimeBuckets, row.AllAtimeBuckets)
	aggregate.allMtimeBuckets = summariseSumUint64Slices(aggregate.allMtimeBuckets, row.AllMtimeBuckets)
	aggregate.allUIDs = summariseAppendUniqueUint32Slice(aggregate.allUIDs, row.AllUIDs)
	aggregate.allGIDs = summariseAppendUniqueUint32Slice(aggregate.allGIDs, row.AllGIDs)
	aggregate.allFT |= row.AllFT
	aggregate.fileCount += row.FileCount
	aggregate.fileSize += row.FileSize

	return aggregate
}

func summariseFillActiveVirtualSummaries(
	rows []summariseActiveVirtualSummaryRow,
	contributors map[string][]string,
	rootFacts map[string]chspool.DirFactRow,
	mountChildCounts map[string]uint64,
) {
	for idx := range rows {
		aggregate := summariseActiveVirtualSummaryAggregateForContributors(contributors[rows[idx].Dir], rootFacts)
		summariseApplyActiveVirtualSummaryAggregate(&rows[idx], aggregate)

		if rows[idx].IsMountRootBox == 1 {
			rows[idx].ChildCount = mountChildCounts[rows[idx].MountPath]
		} else if contributorCount := uint64(len(contributors[rows[idx].Dir])); contributorCount > rows[idx].ChildCount {
			rows[idx].ChildCount = contributorCount
		}
	}
}

func summariseApplyActiveVirtualSummaryAggregate(
	row *summariseActiveVirtualSummaryRow,
	aggregate summariseActiveVirtualSummaryAggregate,
) {
	if aggregate.allCount > 0 {
		row.AllCount = aggregate.allCount
		row.AllSize = aggregate.allSize
		row.AllAtimeMin = aggregate.allAtimeMin
		row.AllMtimeMax = aggregate.allMtimeMax
		row.AllAtimeBuckets = aggregate.allAtimeBuckets
		row.AllMtimeBuckets = aggregate.allMtimeBuckets
		row.AllUIDs = summariseSortedUint32Slice(aggregate.allUIDs)
		row.AllGIDs = summariseSortedUint32Slice(aggregate.allGIDs)
		row.AllFT = aggregate.allFT
	}

	if aggregate.fileCount == 0 {
		return
	}

	row.FileCount = aggregate.fileCount
	row.FileSize = aggregate.fileSize
}

type summariseDGUTARecordContext struct {
	rawDir       string
	canonicalDir string
	dirID        uint32
	parentID     uint32
	subtreeEnd   uint32
	depth        uint16
}

func summariseHasChildrenValue(childCount uint64) uint8 {
	if childCount > 0 {
		return 1
	}

	return 0
}

func (w *summariseDGUTASpoolWriter) writeSchema3FullFilterRows(
	record summariseDGUTARecordContext,
	gutas db.GUTAs,
	childCount uint64,
) error {
	if err := w.flushCompletedSchema3FullFilterRows(record.canonicalDir); err != nil {
		return err
	}

	tuples := summariseFullFilterTupleKeys(gutas)
	w.noteSchema3DirectChildTuples(summariseParentDirForPath(record.canonicalDir), record.canonicalDir, tuples)

	rows := summariseFullFilterRowsForGUTAs(w, record, gutas, childCount)
	if len(rows) == 0 {
		return nil
	}

	w.fullFilterPending = append(w.fullFilterPending, summariseFullFilterPendingDir{
		dir:       summariseEnsureTrailingSlash(record.canonicalDir),
		rows:      rows,
		seenChild: make(map[summariseFullFilterChildTupleKey]struct{}),
	})

	return nil
}

func summariseFullFilterTupleKeys(gutas db.GUTAs) map[summariseFullFilterTupleKey]struct{} {
	keys := make(map[summariseFullFilterTupleKey]struct{}, len(gutas))

	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		keys[summariseFullFilterTupleKey{
			age: uint8(guta.Age),
			gid: guta.GID,
			uid: guta.UID,
			ft:  uint16(guta.FT),
		}] = struct{}{}
	}

	return keys
}

func summariseParentDirForPath(dir string) string {
	dir = summariseNormalizeImportMountPath(dir)
	if dir == "/" {
		return "/"
	}

	trimmed := strings.TrimSuffix(dir, "/")
	idx := strings.LastIndex(trimmed, "/")

	if idx <= 0 {
		return "/"
	}

	return trimmed[:idx+1]
}

func summariseFullFilterRowsForGUTAs(
	w *summariseDGUTASpoolWriter,
	record summariseDGUTARecordContext,
	gutas db.GUTAs,
	childCount uint64,
) []chspool.DirFilterAllRow {
	rows := make([]chspool.DirFilterAllRow, 0, len(gutas))
	record.canonicalDir = summariseEnsureTrailingSlash(record.canonicalDir)

	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		rows = append(rows, summariseFullFilterRowForGUTA(w, record, guta, childCount))
	}

	return rows
}

func summariseEnsureTrailingSlash(dir string) string {
	if dir == "" || strings.HasSuffix(dir, "/") {
		return dir
	}

	return dir + "/"
}

func (w *summariseDGUTASpoolWriter) noteSchema3DirectChildTuples(
	parentDir string,
	childDir string,
	tuples map[summariseFullFilterTupleKey]struct{},
) {
	if len(tuples) == 0 {
		return
	}

	parentDir = summariseEnsureTrailingSlash(parentDir)
	childDir = summariseEnsureTrailingSlash(childDir)

	for idx := range w.fullFilterPending {
		if w.fullFilterPending[idx].dir != parentDir {
			continue
		}

		summariseNotePendingDirectChildTuples(&w.fullFilterPending[idx], childDir, tuples)
	}
}

func summariseNotePendingDirectChildTuples(
	pending *summariseFullFilterPendingDir,
	childDir string,
	tuples map[summariseFullFilterTupleKey]struct{},
) {
	for rowIdx := range pending.rows {
		tuple := summariseFullFilterKeyForRow(pending.rows[rowIdx])
		if _, ok := tuples[tuple]; !ok {
			continue
		}

		childKey := summariseFullFilterChildTupleKey{childDir: childDir, tuple: tuple}
		if _, seen := pending.seenChild[childKey]; seen {
			continue
		}

		pending.seenChild[childKey] = struct{}{}
		pending.rows[rowIdx].FilterChildCount++
	}
}

func (w *summariseDGUTASpoolWriter) flushCompletedSchema3FullFilterRows(currentDir string) error {
	currentDir = summariseEnsureTrailingSlash(currentDir)

	for len(w.fullFilterPending) > 0 &&
		!summariseFullFilterAncestorOrSame(w.fullFilterPending[len(w.fullFilterPending)-1].dir, currentDir) {
		if err := w.flushLastSchema3FullFilterPending(); err != nil {
			return err
		}
	}

	return nil
}

func summariseFullFilterAncestorOrSame(ancestor, dir string) bool {
	ancestor = summariseEnsureTrailingSlash(ancestor)
	dir = summariseEnsureTrailingSlash(dir)

	return ancestor == dir || ancestor == "/" || strings.HasPrefix(dir, ancestor)
}

func (w *summariseDGUTASpoolWriter) flushSchema3FullFilterRows() error {
	for len(w.fullFilterPending) > 0 {
		if err := w.flushLastSchema3FullFilterPending(); err != nil {
			return err
		}
	}

	return nil
}

func (w *summariseDGUTASpoolWriter) flushLastSchema3FullFilterPending() error {
	lastIdx := len(w.fullFilterPending) - 1
	pending := w.fullFilterPending[lastIdx]
	w.fullFilterPending = w.fullFilterPending[:lastIdx]
	flushedRows := make([]chspool.DirFilterAllRow, 0, len(pending.rows))

	for _, row := range pending.rows {
		row.HasFilterChildren = summariseHasChildrenValue(row.FilterChildCount)
		if err := w.writeSchema3FullFilterRow(row); err != nil {
			return err
		}

		flushedRows = append(flushedRows, row)
	}

	w.noteActiveVirtualRootFilterRows(pending.dir, flushedRows)

	return nil
}

func (w *summariseDGUTASpoolWriter) writeSchema3FullFilterRow(row chspool.DirFilterAllRow) error {
	if err := w.ds.set.WriteDirFilterAll(row); err != nil {
		return err
	}

	return w.ds.set.WriteChildFilterAll(summariseChildFilterAllRowForDirFilterAll(row))
}

func summariseChildFilterAllRowForDirFilterAll(row chspool.DirFilterAllRow) chspool.ChildFilterAllRow {
	return chspool.ChildFilterAllRow{
		MountPath:         row.MountPath,
		SnapshotID:        row.SnapshotID,
		ParentID:          0,
		Age:               row.Age,
		GID:               row.GID,
		UID:               row.UID,
		FT:                row.FT,
		DirID:             row.DirID,
		Count:             row.Count,
		Size:              row.Size,
		AtimeMin:          row.AtimeMin,
		MtimeMax:          row.MtimeMax,
		AtimeBuckets:      row.AtimeBuckets,
		MtimeBuckets:      row.MtimeBuckets,
		FilterChildCount:  row.FilterChildCount,
		ChildCount:        row.ChildCount,
		HasFilterChildren: row.HasFilterChildren,
		HasChildren:       row.HasChildren,
		RefreshedAt:       row.RefreshedAt,
	}
}

func (w *summariseDGUTASpoolWriter) noteActiveVirtualRootFilterRows(
	dir string,
	rows []chspool.DirFilterAllRow,
) {
	if dir != w.mountPath {
		return
	}

	if w.activeVirtualRootFilterRows == nil {
		w.activeVirtualRootFilterRows = make(map[string][]chspool.DirFilterAllRow)
	}

	w.activeVirtualRootFilterRows[w.mountPath] = append([]chspool.DirFilterAllRow(nil), rows...)
}

func (w *summariseDGUTASpoolWriter) writeSchema3ReadinessRows() error {
	counts := summariseSchema3SnapshotRowCounts(w.ds.set.TableManifests())
	if err := w.ds.set.WriteSchema3SnapshotSet(chspool.Schema3SnapshotSetRow{
		MountPath:          w.mountPath,
		SnapshotID:         w.snapshotID,
		Schema3Version:     clickHouseSpoolSchema3Version,
		DirsRows:           counts.dirsRows,
		DirFactsRows:       counts.dirFactsRows,
		ChildFilterAllRows: counts.childFilterAllRows,
		DirFilterAllRows:   counts.dirFilterAllRows,
		ManifestSHA256:     summariseSchema3SnapshotManifestSHA256(w.mountPath, w.snapshotID, counts),
		RefreshedAt:        w.refreshedAt,
	}); err != nil {
		return err
	}

	return w.writeActiveVirtualRows()
}

func summariseSchema3SnapshotRowCounts(tables map[string]chspool.TableManifest) summariseSchema3SnapshotCounts {
	return summariseSchema3SnapshotCounts{
		dirsRows:           tables[chspool.TableDirs].Rows,
		dirFactsRows:       tables[chspool.TableDirFacts].Rows,
		childFilterAllRows: tables[chspool.TableChildFilterAll].Rows,
		dirFilterAllRows:   tables[chspool.TableDirFilterAll].Rows,
	}
}

func summariseSchema3SnapshotManifestSHA256(
	mountPath string,
	snapshotID string,
	counts summariseSchema3SnapshotCounts,
) string {
	return summariseSHA256Hex(fmt.Sprintf(
		"%s|%s|%d|%d|%d|%d|%d",
		mountPath,
		snapshotID,
		clickHouseSpoolSchema3Version,
		counts.dirsRows,
		counts.dirFactsRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
	))
}

func (w *summariseDGUTASpoolWriter) writeActiveVirtualRows() error {
	activeRows := []summariseMountActiveRow{{
		mountPath:  w.mountPath,
		snapshotID: w.snapshotID,
		updatedAt:  summariseActiveSetUpdatedAt(w.updatedAt),
	}}
	activeSetID := summariseFingerprintForMountsActive(activeRows)
	summaryRows, filterRows, childRows := summariseActiveVirtualRowsFromCanonicalData(
		activeSetID,
		activeRows,
		w.activeVirtualRootFacts,
		w.activeVirtualRootFilterRows,
		w.activeVirtualMountChildCounts,
		w.refreshedAt,
	)

	if err := w.writeActiveVirtualDataRows(summaryRows, filterRows, childRows); err != nil {
		return err
	}

	return w.ds.set.WriteActiveVirtualSet(summariseActiveVirtualSetRow(
		activeRows,
		activeSetID,
		summaryRows,
		filterRows,
		childRows,
		w.refreshedAt,
	))
}

func summariseFingerprintForMountsActive(rows []summariseMountActiveRow) string {
	if len(rows) == 0 {
		return ""
	}

	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		updatedAt := summariseActiveSetUpdatedAt(row.updatedAt)
		parts = append(parts, row.mountPath+"|"+row.snapshotID+"|"+updatedAt.Format(time.RFC3339Nano))
	}

	slices.Sort(parts)

	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}

	return hex.EncodeToString(hash.Sum(nil))
}

func summariseActiveVirtualChildRows(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	refreshedAt time.Time,
) []summariseActiveVirtualChildRow {
	virtualRows := summariseVirtualChildRowsForMounts(activeRows)
	rows := make([]summariseActiveVirtualChildRow, 0, len(virtualRows))

	for _, row := range virtualRows {
		rows = append(rows, summariseActiveVirtualChildRow{
			ActiveSetID:    activeSetID,
			ParentDir:      row.parentDir,
			ChildDir:       row.childDir,
			MountPath:      row.mountPath,
			IsMountRootBox: summariseBoolAsUInt8(row.childIsMountRoot),
			RefreshedAt:    refreshedAt,
		})
	}

	return rows
}

func summariseActiveVirtualSummaryRows(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	childRows []summariseActiveVirtualChildRow,
	refreshedAt time.Time,
) []summariseActiveVirtualSummaryRow {
	dirs, childCounts := summariseActiveVirtualSummarySeeds(activeSetID, activeRows, childRows)

	return summariseActiveVirtualSummaryRowsFromSeeds(
		dirs,
		childCounts,
		summariseMaxUpdatedAtForActiveRows(activeRows),
		refreshedAt,
	)
}

func summariseActiveVirtualRowsFromCanonicalData(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	rootFacts map[string]chspool.DirFactRow,
	rootFilterRows map[string][]chspool.DirFilterAllRow,
	mountChildCounts map[string]uint64,
	refreshedAt time.Time,
) ([]summariseActiveVirtualSummaryRow, []summariseActiveVirtualFilterAllRow, []summariseActiveVirtualChildRow) {
	childRows := summariseActiveVirtualChildRows(activeSetID, activeRows, refreshedAt)
	summaryRows := summariseActiveVirtualSummaryRows(activeSetID, activeRows, childRows, refreshedAt)
	contributors := summariseActiveVirtualContributors(summaryRows, activeRows)

	summariseFillActiveVirtualSummaries(summaryRows, contributors, rootFacts, mountChildCounts)
	filterRows := summariseActiveVirtualFilterRows(summaryRows, contributors, rootFilterRows, refreshedAt)
	summariseFillActiveVirtualChildCounts(childRows, summaryRows)

	return summaryRows, filterRows, childRows
}

func (w *summariseDGUTASpoolWriter) noteActiveVirtualMountChildRows(parentDir string, count uint64) {
	if parentDir != w.mountPath || count == 0 {
		return
	}

	if w.activeVirtualMountChildCounts == nil {
		w.activeVirtualMountChildCounts = make(map[string]uint64)
	}

	w.activeVirtualMountChildCounts[w.mountPath] += count
}

func (w *summariseDGUTASpoolWriter) noteActiveVirtualRootFact(dir string, row chspool.DirFactRow) {
	if dir != w.mountPath {
		return
	}

	if w.activeVirtualRootFacts == nil {
		w.activeVirtualRootFacts = make(map[string]chspool.DirFactRow)
	}

	w.activeVirtualRootFacts[row.MountPath] = row
}

func summariseActiveVirtualFilterRows(
	summaryRows []summariseActiveVirtualSummaryRow,
	contributors map[string][]string,
	rootFilterRows map[string][]chspool.DirFilterAllRow,
	refreshedAt time.Time,
) []summariseActiveVirtualFilterAllRow {
	rows := make([]summariseActiveVirtualFilterAllRow, 0, len(summaryRows)*len(rootFilterRows))

	for _, summary := range summaryRows {
		rows = append(rows, summariseActiveVirtualFilterRowsForSummary(
			summary,
			contributors[summary.Dir],
			rootFilterRows,
			refreshedAt,
		)...)
	}

	slices.SortFunc(rows, func(a, b summariseActiveVirtualFilterAllRow) int {
		return strings.Compare(summariseActiveVirtualFilterSortKey(a), summariseActiveVirtualFilterSortKey(b))
	})

	return rows
}

func summariseActiveVirtualSetRow(
	activeRows []summariseMountActiveRow,
	activeSetID string,
	summaryRows []summariseActiveVirtualSummaryRow,
	filterRows []summariseActiveVirtualFilterAllRow,
	childRows []summariseActiveVirtualChildRow,
	refreshedAt time.Time,
) chspool.ActiveVirtualSetRow {
	manifest := summariseActiveVirtualManifestSHA256(
		activeSetID,
		len(summaryRows),
		len(filterRows),
		len(childRows),
	)

	return chspool.ActiveVirtualSetRow{
		ActiveSetID:      activeSetID,
		Schema3Version:   clickHouseSpoolSchema3Version,
		MountsSHA256:     activeSetID,
		ActiveMountCount: uint64(len(activeRows)),
		SummaryRows:      uint64(len(summaryRows)),
		FilterRows:       uint64(len(filterRows)),
		ChildRows:        uint64(len(childRows)),
		ManifestSHA256:   manifest,
		Ready:            1,
		RefreshedAt:      refreshedAt,
	}
}

func (w *summariseDGUTASpoolWriter) writeActiveVirtualDataRows(
	summaryRows []summariseActiveVirtualSummaryRow,
	filterRows []summariseActiveVirtualFilterAllRow,
	childRows []summariseActiveVirtualChildRow,
) error {
	for _, row := range summaryRows {
		if err := w.ds.set.WriteActiveVirtualSummary(summariseActiveVirtualSummarySpoolRow(row)); err != nil {
			return err
		}
	}

	for _, row := range filterRows {
		if err := w.ds.set.WriteActiveVirtualFilterAll(summariseActiveVirtualFilterSpoolRow(row)); err != nil {
			return err
		}
	}

	for _, row := range childRows {
		if err := w.ds.set.WriteActiveVirtualChild(summariseActiveVirtualChildSpoolRow(row)); err != nil {
			return err
		}
	}

	return nil
}

func summariseFullFilterRowForGUTA(
	w *summariseDGUTASpoolWriter,
	record summariseDGUTARecordContext,
	guta *db.GUTA,
	childCount uint64,
) chspool.DirFilterAllRow {
	return chspool.DirFilterAllRow{
		MountPath:    w.mountPath,
		SnapshotID:   w.snapshotID,
		Age:          uint8(guta.Age),
		GID:          guta.GID,
		UID:          guta.UID,
		FT:           uint16(guta.FT),
		DirID:        record.dirID,
		SubtreeEnd:   record.subtreeEnd,
		Count:        guta.Count,
		Size:         guta.Size,
		AtimeMin:     guta.Atime,
		MtimeMax:     guta.Mtime,
		AtimeBuckets: summariseAgeBucketsSlice(&guta.ATimeRanges),
		MtimeBuckets: summariseAgeBucketsSlice(&guta.MTimeRanges),
		ChildCount:   childCount,
		HasChildren:  summariseHasChildrenValue(childCount),
		RefreshedAt:  w.refreshedAt,
	}
}

func summariseActiveVirtualSummarySpoolRow(row summariseActiveVirtualSummaryRow) chspool.ActiveVirtualSummaryRow {
	return chspool.ActiveVirtualSummaryRow{
		ActiveSetID:     row.ActiveSetID,
		VirtualID:       summariseVirtualIDForDir(row.Dir),
		MountPath:       row.MountPath,
		MountRootDirID:  summariseMountRootDirID(row.MountPath),
		IsMountRootBox:  row.IsMountRootBox,
		UpdatedAt:       row.UpdatedAt,
		AllCount:        row.AllCount,
		AllSize:         row.AllSize,
		AllAtimeMin:     row.AllAtimeMin,
		AllMtimeMax:     row.AllMtimeMax,
		AllAtimeBuckets: row.AllAtimeBuckets,
		AllMtimeBuckets: row.AllMtimeBuckets,
		AllUIDs:         row.AllUIDs,
		AllGIDs:         row.AllGIDs,
		AllFT:           row.AllFT,
		FileCount:       row.FileCount,
		FileSize:        row.FileSize,
		ChildCount:      row.ChildCount,
		RefreshedAt:     row.RefreshedAt,
	}
}

func summariseActiveVirtualFilterSpoolRow(row summariseActiveVirtualFilterAllRow) chspool.ActiveVirtualFilterAllRow {
	return chspool.ActiveVirtualFilterAllRow{
		ActiveSetID:      row.ActiveSetID,
		VirtualID:        summariseVirtualIDForDir(row.Dir),
		Age:              row.Age,
		GID:              row.GID,
		UID:              row.UID,
		FT:               row.FT,
		Count:            row.Count,
		Size:             row.Size,
		AtimeMin:         row.AtimeMin,
		MtimeMax:         row.MtimeMax,
		AtimeBuckets:     row.AtimeBuckets,
		MtimeBuckets:     row.MtimeBuckets,
		FilterChildCount: row.FilterChildCount,
		ChildCount:       row.ChildCount,
		RefreshedAt:      row.RefreshedAt,
	}
}

func summariseActiveVirtualChildSpoolRow(row summariseActiveVirtualChildRow) chspool.ActiveVirtualChildRow {
	return chspool.ActiveVirtualChildRow{
		ActiveSetID:     row.ActiveSetID,
		ParentVirtualID: summariseVirtualIDForDir(row.ParentDir),
		ChildVirtualID:  summariseVirtualIDForDir(row.ChildDir),
		MountPath:       row.MountPath,
		MountRootDirID:  summariseMountRootDirID(row.MountPath),
		IsMountRootBox:  row.IsMountRootBox,
		ChildCount:      row.ChildCount,
		RefreshedAt:     row.RefreshedAt,
	}
}

func (w *summariseDGUTASpoolWriter) writeCatalogRow(
	dguta db.RecordDGUTA,
	record summariseDGUTARecordContext,
) error {
	return w.ds.set.WriteDir(chspool.DirRow{
		MountPath:     w.mountPath,
		SnapshotID:    w.snapshotID,
		DirID:         record.dirID,
		ParentID:      record.parentID,
		SubtreeEnd:    record.subtreeEnd,
		Depth:         record.depth,
		Name:          summariseCatalogNameForFullPath(record.canonicalDir),
		FullPath:      summariseEnsureTrailingSlash(record.canonicalDir),
		ChildDirCount: summariseSafeUint32(dguta.ChildCount),
		PathHash:      summariseCatalogPathHash(record.canonicalDir),
	})
}

func summariseCatalogNameForFullPath(fullPath string) string {
	fullPath = summariseEnsureTrailingSlash(fullPath)
	if fullPath == "/" {
		return "/"
	}

	trimmed := strings.TrimSuffix(fullPath, "/")

	idx := strings.LastIndexByte(trimmed, '/')
	if idx < 0 {
		return trimmed + "/"
	}

	return trimmed[idx+1:] + "/"
}

func summariseSafeUint32(value uint64) uint32 {
	if value > uint64(^uint32(0)) {
		return ^uint32(0)
	}

	return uint32(value)
}

func summariseCatalogPathHash(fullPath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(summariseEnsureTrailingSlash(fullPath)))

	return h.Sum64()
}

type summariseSpoolDataset struct {
	set                *chspool.Set
	mountPath          string
	updatedAt          time.Time
	snapshotID         string
	refreshedAt        time.Time
	dirgutaReferenceAt time.Time
	idAllocator        *summary.DirIDAllocator
	dgutaWriter        *summariseDGUTASpoolWriter
}

type summariseFileSpoolOperation struct {
	ds *summariseSpoolDataset
}

type summariseDGUTASpoolWriter struct {
	ds                            *summariseSpoolDataset
	mountPath                     string
	updatedAt                     time.Time
	snapshotID                    string
	refreshedAt                   time.Time
	previousDGUTARows             summariseDGUTARecordRows
	fullFilterPending             []summariseFullFilterPendingDir
	activeVirtualRootFacts        map[string]chspool.DirFactRow
	activeVirtualRootFilterRows   map[string][]chspool.DirFilterAllRow
	activeVirtualMountChildCounts map[string]uint64
	closed                        bool
	projectionSetWritten          bool
}

func (w *summariseDGUTASpoolWriter) writeSchema2Rows(
	record summariseDGUTARecordContext,
	gutas db.GUTAs,
) error {
	return w.writeDirFilterAgeAllRows(record, gutas)
}

func (w *summariseDGUTASpoolWriter) writeDirFilterAgeAllRows(record summariseDGUTARecordContext, gutas db.GUTAs) error {
	for _, guta := range gutas {
		if guta == nil || guta.Age != db.DGUTAgeAll {
			continue
		}

		if err := w.ds.set.WriteDirFilterAgeAll(chspool.DirFilterAgeAllRow{
			MountPath:    w.mountPath,
			SnapshotID:   w.snapshotID,
			GID:          guta.GID,
			UID:          guta.UID,
			FT:           uint16(guta.FT),
			DirID:        record.dirID,
			SubtreeEnd:   record.subtreeEnd,
			Count:        guta.Count,
			Size:         guta.Size,
			AtimeMin:     guta.Atime,
			MtimeMax:     guta.Mtime,
			AtimeBuckets: summariseAgeBucketsSlice(&guta.ATimeRanges),
			MtimeBuckets: summariseAgeBucketsSlice(&guta.MTimeRanges),
			RefreshedAt:  w.refreshedAt,
		}); err != nil {
			return err
		}
	}

	return nil
}

func summariseFileIngestDirIDPath(info *summary.FileInfo) *summary.DirectoryPath {
	if info.IsDir() {
		return info.Path.Parent
	}

	return info.Path
}

type summariseBasedirsSpoolStore struct {
	ds                       *summariseSpoolDataset
	mountPath                string
	updatedAt                time.Time
	snapshotID               string
	reset                    bool
	bufferedAgeAllGroupUsage map[uint32][]*basedirs.Usage
}

type summariseDGUTARowKey struct {
	dir         string
	gid         uint32
	uid         uint32
	ft          uint16
	age         uint8
	count       uint64
	size        uint64
	atime       int64
	mtime       int64
	aTimeRanges [9]uint64
	mTimeRanges [9]uint64
}

type summariseDGUTARecordRows struct {
	rawDir       string
	canonicalDir string
	keys         map[summariseDGUTARowKey]struct{}
}

type summariseMountDirRecordSummary struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets summary.AgeBuckets
	mtimeBuckets summary.AgeBuckets
	uids         []uint32
	gids         []uint32
	ft           db.DirGUTAFileType
}

type summariseMountDirProjectionVectorColumns struct {
	gids         []uint32
	uids         []uint32
	fts          []uint16
	ages         []uint8
	counts       []uint64
	sizes        []uint64
	atimeMins    []int64
	mtimeMaxs    []int64
	atimeBuckets [][]uint64
	mtimeBuckets [][]uint64
}

type summariseSchema3SnapshotCounts struct {
	dirsRows           uint64
	dirFactsRows       uint64
	childFilterAllRows uint64
	dirFilterAllRows   uint64
}

type summariseVirtualChildRow struct {
	parentDir        string
	childDir         string
	childIsMountRoot bool
	mountPath        string
}

func summariseVirtualChildRowsForMount(mountPath string) []summariseVirtualChildRow {
	parent := "/"
	mountPath = summariseEnsureTrailingSlash(mountPath)
	rows := make([]summariseVirtualChildRow, 0, strings.Count(mountPath, "/"))

	for {
		child, ok := summariseImmediateChildForMount(parent, mountPath)
		if !ok {
			return rows
		}

		childIsMountRoot := summariseEnsureTrailingSlash(child) == mountPath

		row := summariseVirtualChildRow{
			parentDir:        parent,
			childDir:         child,
			childIsMountRoot: childIsMountRoot,
		}
		if childIsMountRoot {
			row.mountPath = mountPath
		}

		rows = append(rows, row)
		if childIsMountRoot {
			return rows
		}

		parent = summariseEnsureTrailingSlash(child)
	}
}

func summariseImmediateChildForMount(parentDir, mountPath string) (string, bool) {
	parentDir = summariseEnsureTrailingSlash(parentDir)
	mountPath = summariseEnsureTrailingSlash(mountPath)

	if parentDir == mountPath || !strings.HasPrefix(mountPath, parentDir) {
		return "", false
	}

	relative := strings.TrimPrefix(mountPath, parentDir)

	part, _, _ := strings.Cut(relative, "/")
	if part == "" {
		return "", false
	}

	if parentDir == "/" {
		return "/" + part, true
	}

	return strings.TrimSuffix(parentDir, "/") + "/" + part, true
}

func summariseActiveVirtualSummarySeeds(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	childRows []summariseActiveVirtualChildRow,
) (map[string]summariseActiveVirtualSummaryRow, map[string]uint64) {
	dirs := make(map[string]summariseActiveVirtualSummaryRow, len(childRows)+1)
	childCounts := make(map[string]uint64)

	summariseSeedActiveVirtualChildSummaryRows(activeSetID, childRows, dirs, childCounts)
	summariseSeedActiveVirtualMountRootRows(activeSetID, activeRows, dirs)

	if len(dirs) == 0 && activeSetID != "" {
		dirs["/"] = summariseActiveVirtualSummaryRow{ActiveSetID: activeSetID, Dir: "/"}
	}

	return dirs, childCounts
}

func summariseActiveVirtualSummaryRowsFromSeeds(
	dirs map[string]summariseActiveVirtualSummaryRow,
	childCounts map[string]uint64,
	updatedAt time.Time,
	refreshedAt time.Time,
) []summariseActiveVirtualSummaryRow {
	out := make([]summariseActiveVirtualSummaryRow, 0, len(dirs))
	for _, row := range dirs {
		row.UpdatedAt = updatedAt
		row.ChildCount = childCounts[row.Dir]
		row.AllAtimeBuckets = summariseAgeBucketsSlice(nil)
		row.AllMtimeBuckets = summariseAgeBucketsSlice(nil)
		row.RefreshedAt = refreshedAt
		out = append(out, row)
	}

	slices.SortFunc(out, func(a, b summariseActiveVirtualSummaryRow) int {
		return strings.Compare(a.Dir, b.Dir)
	})

	return out
}

func summariseActiveVirtualFilterSortKey(row summariseActiveVirtualFilterAllRow) string {
	return fmt.Sprintf("%s\x00%03d\x00%010d\x00%010d\x00%05d", row.Dir, row.Age, row.GID, row.UID, row.FT)
}

func summariseAppendUniqueUint32Slice(values []uint32, more []uint32) []uint32 {
	for _, value := range more {
		values = summariseAppendUniqueUint32(values, value)
	}

	return values
}

func summariseFillActiveVirtualChildCounts(
	childRows []summariseActiveVirtualChildRow,
	summaryRows []summariseActiveVirtualSummaryRow,
) {
	childCounts := make(map[string]uint64, len(summaryRows))
	for _, row := range summaryRows {
		childCounts[row.Dir] = row.ChildCount
	}

	for idx := range childRows {
		childRows[idx].ChildCount = childCounts[summariseEnsureTrailingSlash(childRows[idx].ChildDir)]
	}
}

func summariseVirtualIDForDir(dir string) uint32 {
	return uint32(summariseCatalogPathHash(dir)) //nolint:gosec // Virtual IDs intentionally use low hash bits.
}

func summariseMountRootDirID(mountPath string) uint32 {
	mountPath = summariseEnsureTrailingSlash(mountPath)
	if mountPath == "/" || mountPath == "" {
		return 0
	}

	trimmed := strings.Trim(mountPath, "/")
	if trimmed == "" {
		return 0
	}

	return uint32(strings.Count(trimmed, "/") + 1) //nolint:gosec // Mount path segment counts are bounded.
}

func summariseActiveVirtualManifestSHA256(activeSetID string, summaryRows, filterRows, childRows int) string {
	return summariseSHA256Hex(fmt.Sprintf(
		"%s|%d|%d|%d|%d",
		activeSetID,
		clickHouseSpoolSchema3Version,
		summaryRows,
		filterRows,
		childRows,
	))
}

func summariseSHA256Hex(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
}

func maybeRunClickHouseSpoolSummarise( //nolint:funlen
	statsPath string,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (bool, error) {
	if target == nil {
		return false, nil
	}

	diag.setTarget(target)
	diag.logStart()
	diag.startSignalHandler()

	spoolDir := summariseClickHouseSpoolDir(target.outputDir)

	expected, err := newSummariseSpoolManifest(statsPath, target)
	if err != nil {
		return true, err
	}

	manifest, hasCompleteSpool := completeSummariseSpool(spoolDir, expected)

	err = preflightClickHouseActiveSnapshotForSpool(*target, spoolDir, manifest)
	if err != nil {
		if errors.Is(err, errSummariseClickHouseSnapshotAlreadyActive) {
			return true, nil
		}

		return true, err
	}

	if hasCompleteSpool {
		return true, publishSummariseSpool(spoolDir, manifest, target, diag)
	}

	manifest, err = buildSummariseSpool(statsPath, spoolDir, expected, target, diag)
	if err != nil {
		diag.logFailure(err)

		return true, err
	}

	return true, publishSummariseSpool(spoolDir, manifest, target, diag)
}

func summariseClickHouseSpoolDir(outputDir string) string {
	return filepath.Join(outputDir, clickHouseSpoolDirName)
}

func completeSummariseSpool(spoolDir string, expected chspool.Manifest) (*chspool.Manifest, bool) {
	manifest, err := chspool.ReadManifest(spoolDir)
	if err != nil {
		return nil, false
	}

	if err := chspool.VerifyManifest(spoolDir, manifest, expected); err != nil {
		return nil, false
	}

	return manifest, true
}

func newSummariseSpoolManifest( //nolint:funlen
	statsPath string,
	target *clickHouseSummariseTarget,
) (chspool.Manifest, error) {
	statsIdentity := chspool.FileIdentity{Path: statsPath}
	if statsPath != "-" {
		var err error

		statsIdentity, err = chspool.IdentifyExistingPath(statsPath, false)
		if err != nil {
			return chspool.Manifest{}, err
		}
	}

	mountsIdentity, err := chspool.IdentifyExistingPath(target.mountpointsPath, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	quotaIdentity, err := chspool.IdentifyExistingPath(quotaPath, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	configIdentity, err := chspool.IdentifyExistingPath(basedirsConfig, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	return chspool.Manifest{
		Version:         chspool.Version,
		Format:          chspool.Format,
		State:           chspool.Complete,
		MountPath:       target.mountPath,
		SnapshotID:      clickhouse.SnapshotID(target.mountPath, target.modtime),
		UpdatedAt:       target.modtime.UTC().Format(time.RFC3339Nano),
		OutputDir:       target.outputDir,
		SchemaMarker:    clickHouseSpoolSchemaMark,
		Stats:           statsIdentity,
		Mounts:          mountsIdentity,
		Quota:           quotaIdentity,
		BasedirsConfig:  configIdentity,
		BasedirsEnabled: basedirsDB != "",
	}, nil
}

func buildSummariseSpool( //nolint:funlen,gocyclo
	statsPath string,
	spoolDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*chspool.Manifest, error) {
	partialDir := spoolDir + ".partial"

	if err := os.RemoveAll(partialDir); err != nil {
		return nil, err
	}

	set, err := chspool.CreateSet(partialDir)
	if err != nil {
		return nil, err
	}

	ds := &summariseSpoolDataset{
		set:                set,
		mountPath:          target.mountPath,
		updatedAt:          target.modtime.UTC(),
		snapshotID:         expected.SnapshotID,
		refreshedAt:        summariseSpoolNow().UTC(),
		dirgutaReferenceAt: summariseSpoolDirGUTANow().UTC(),
		idAllocator:        summary.NewDirIDAllocator(),
	}
	if mountErr := ds.idAllocator.SetMountPath(target.mountPath); mountErr != nil {
		_ = os.RemoveAll(partialDir)

		return nil, mountErr
	}

	err = parseSummariseToSpool(statsPath, ds, diag)

	closeErr := set.Close()
	if err != nil || closeErr != nil {
		_ = os.RemoveAll(partialDir)

		return nil, errors.Join(err, closeErr)
	}

	manifest := expected
	manifest.Tables = set.TableManifests()
	manifest.CompletedAt = summariseSpoolNow().UTC().Format(time.RFC3339Nano)

	if err := chspool.WriteManifestAtomic(partialDir, &manifest); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	if err := os.RemoveAll(spoolDir); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	if err := os.Rename(partialDir, spoolDir); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	return &manifest, nil
}

func parseSummariseToSpool( //nolint:funlen
	statsPath string,
	ds *summariseSpoolDataset,
	diag *summariseDiagnostics,
) (err error) {
	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	s := summary.NewSummariser(stats.NewStatsParser(r))
	setSummariseProgress(s, diag)
	addSummariseSpoolOperations(s, ds)

	if addErr := addSummariseSpoolBasedirs(s, ds); addErr != nil {
		return addErr
	}

	if addErr := addOutputSummarisers(s); addErr != nil {
		return addErr
	}

	diag.setCurrentPhase("parse")

	err = s.Summarise()
	diag.logParseResult(err)

	if err != nil {
		return err
	}

	return closeSummariseSpoolOperations(ds)
}

func addSummariseSpoolOperations(s *summary.Summariser, ds *summariseSpoolDataset) {
	dw := &summariseDGUTASpoolWriter{ds: ds}
	dw.SetMountPath(ds.mountPath)
	dw.SetUpdatedAt(ds.updatedAt)
	ds.dgutaWriter = dw

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAgeAt(dw, ds.dirgutaReferenceAt, ds.idAllocator))
	s.AddGlobalOperation((&summariseFileSpoolOperation{ds: ds}).operation)
}

func addSummariseSpoolBasedirs(s *summary.Summariser, ds *summariseSpoolDataset) error {
	if basedirsDB == "" {
		return nil
	}

	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return err
	}

	mountpoints, err := summariseutil.ParseMountpointsFromFile(mounts)
	if err != nil {
		return err
	}

	store := &summariseBasedirsSpoolStore{ds: ds}
	store.SetMountPath(ds.mountPath)
	store.SetUpdatedAt(ds.updatedAt)

	creator, err := summariseutil.NewBaseDirsCreator(store, quotas, mountpoints, ds.updatedAt)
	if err != nil {
		return err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, creator))

	return nil
}

func closeSummariseSpoolOperations(ds *summariseSpoolDataset) error {
	if ds.dgutaWriter == nil {
		return nil
	}

	return ds.dgutaWriter.Close()
}

func publishSummariseSpool(
	spoolDir string,
	manifest *chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) error {
	diag.logCloseStart(true)
	report, err := loadSummariseClickHouseSpool(
		context.Background(),
		target.cfg,
		spoolDir,
		manifest,
		diag.recordImportPhase,
	)
	diag.logCloseResult(true, err)

	if err != nil {
		diag.logFailure(err)

		return err
	}

	if err := writeSummariseSpoolLoadReport(spoolDir, report, diag); err != nil {
		return err
	}

	return writeSummariseCompletionMarkerWithDiagnostics(
		&summariseRunHooks{completionTarget: target},
		diag,
	)
}

func (f *summariseFileSpoolOperation) operation() summary.Operation {
	return f
}

func (f *summariseFileSpoolOperation) Add(info *summary.FileInfo) error { //nolint:funlen
	if info == nil {
		return nil
	}

	if err := validateSummariseFileInfo(info); err != nil {
		return err
	}

	size, apparentSize, inode, nlink, err := summariseSpoolUnsignedFileInfoValues(info)
	if err != nil {
		return err
	}

	parentDir := string(info.Path.AppendTo(make([]byte, 0, info.Path.Len())))
	name := string(info.Name)

	_, name, keep := summariseCanonicalFileIngestPath(f.ds.mountPath, parentDir, name)
	if !keep {
		return nil
	}

	dirID, err := f.ds.idAllocator.DirID(summariseFileIngestDirIDPath(info))
	if err != nil {
		return err
	}

	return f.ds.set.WriteFile(chspool.FileRow{
		MountPath:    f.ds.mountPath,
		SnapshotID:   f.ds.snapshotID,
		DirID:        dirID,
		Name:         name,
		Ext:          summariseExtFromName(name),
		EntryType:    info.EntryType,
		Size:         size,
		ApparentSize: apparentSize,
		UID:          info.UID,
		GID:          info.GID,
		ATime:        time.Unix(info.ATime, 0),
		MTime:        time.Unix(info.MTime, 0),
		CTime:        time.Unix(info.CTime, 0),
		Inode:        inode,
		Nlink:        nlink,
	})
}

func validateSummariseFileInfo(info *summary.FileInfo) error {
	if info.Path == nil {
		return errSummariseSpoolFileDirRequired
	}

	if len(info.Name) == 0 {
		return errSummariseSpoolFileNameRequired
	}

	if info.Size < 0 || info.ApparentSize < 0 || info.Inode < 0 || info.Nlink < 0 {
		return errSummariseSpoolFileNonNegative
	}

	return nil
}

func summariseSpoolUnsignedFileInfoValues(info *summary.FileInfo) (uint64, uint64, uint64, uint64, error) {
	size, err := summariseNonNegativeInt64ToUint64(info.Size, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	apparentSize, err := summariseNonNegativeInt64ToUint64(info.ApparentSize, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	inode, err := summariseNonNegativeInt64ToUint64(info.Inode, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	nlink, err := summariseNonNegativeInt64ToUint64(info.Nlink, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	return size, apparentSize, inode, nlink, nil
}

func writeSummariseSpoolLoadReport(
	spoolDir string,
	report perfreport.Report,
	diag *summariseDiagnostics,
) error {
	if err := perfreport.WriteReport(summariseSpoolLoadReportPath(spoolDir), report); err != nil {
		diag.logFailure(err)

		return err
	}

	return nil
}

func summariseSpoolLoadReportPath(spoolDir string) string {
	return filepath.Join(spoolDir, clickHouseSpoolLoadReportName)
}

func summariseNonNegativeInt64ToUint64(value int64, negativeErr error) (uint64, error) {
	if value < 0 {
		return 0, negativeErr
	}

	return uint64(value), nil
}

func (f *summariseFileSpoolOperation) Output() error {
	return nil
}

func (w *summariseDGUTASpoolWriter) SetBatchSize(_ int) {}

func (w *summariseDGUTASpoolWriter) SetMountPath(mountPath string) {
	w.mountPath = summariseNormalizeImportMountPath(mountPath)
	w.refreshIDs()
}

func (w *summariseDGUTASpoolWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
	w.refreshIDs()
}

func (w *summariseDGUTASpoolWriter) refreshIDs() {
	if w.mountPath != "" && !w.updatedAt.IsZero() {
		w.snapshotID = clickhouse.SnapshotID(w.mountPath, w.updatedAt)
	}

	if w.refreshedAt.IsZero() && w.ds != nil {
		w.refreshedAt = w.ds.refreshedAt
	}
}

func (w *summariseDGUTASpoolWriter) Add(dguta db.RecordDGUTA) error { //nolint:funlen,gocyclo,gocognit
	if dguta.Dir == nil {
		return errSummariseSpoolDirFactsDir
	}

	w.refreshIDs()

	rawParentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))
	parentDir := summariseCanonicalPathForMount(w.mountPath, rawParentDir)
	childCount := max(dguta.ChildCount, uint64(len(dguta.Children)))
	record := summariseDGUTARecordContext{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		dirID:        dguta.DirID,
		parentID:     dguta.ParentID,
		subtreeEnd:   dguta.SubtreeEnd,
		depth:        dguta.Depth,
	}
	appendedGUTAs := make(db.GUTAs, 0, len(dguta.GUTAs))

	if err := w.writeCatalogRow(dguta, record); err != nil {
		return err
	}

	tracker := w.newDGUTARecordTracker(len(dguta.GUTAs))
	for _, guta := range dguta.GUTAs {
		key, keep, project := w.appendDGUTARow(rawParentDir, parentDir, guta, tracker.keys != nil)
		if keep && tracker.keys != nil {
			tracker.keys[key] = struct{}{}
		}

		if project {
			appendedGUTAs = append(appendedGUTAs, guta)
		}
	}

	if err := w.writeDirFactRow(record, appendedGUTAs, childCount); err != nil {
		return err
	}

	if err := w.writeSchema2Rows(record, appendedGUTAs); err != nil {
		return err
	}

	if err := w.writeSchema3FullFilterRows(record, appendedGUTAs, childCount); err != nil {
		return err
	}

	w.noteActiveVirtualMountChildRows(parentDir, childCount)

	w.previousDGUTARows = summariseDGUTARecordRows{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		keys:         tracker.keys,
	}

	return nil
}

func (w *summariseDGUTASpoolWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	if err := w.flushSchema3FullFilterRows(); err != nil {
		return err
	}

	if err := w.writeSchema3ReadinessRows(); err != nil {
		return err
	}

	return w.writeProjectionSetRow()
}

func (w *summariseDGUTASpoolWriter) Abort() error {
	w.closed = true

	return nil
}

func (w *summariseDGUTASpoolWriter) newDGUTARecordTracker(numGUTAs int) summariseDGUTARecordRows {
	if w.mountPath != "/" {
		return summariseDGUTARecordRows{}
	}

	return summariseDGUTARecordRows{keys: make(map[summariseDGUTARowKey]struct{}, numGUTAs)}
}

func (w *summariseDGUTASpoolWriter) appendDGUTARow(
	rawParentDir string,
	parentDir string,
	guta *db.GUTA,
	trackDuplicateKeys bool,
) (summariseDGUTARowKey, bool, bool) {
	if guta == nil {
		return summariseDGUTARowKey{}, false, false
	}

	if !trackDuplicateKeys {
		return summariseDGUTARowKey{}, true, true
	}

	rowKey := newSummariseDGUTARowKey(parentDir, guta)
	if w.isConsecutiveCanonicalDGUTADuplicate(rawParentDir, parentDir, rowKey) {
		return rowKey, true, false
	}

	return rowKey, true, true
}

func (w *summariseDGUTASpoolWriter) isConsecutiveCanonicalDGUTADuplicate(
	rawDir string,
	canonicalDir string,
	key summariseDGUTARowKey,
) bool {
	prev := w.previousDGUTARows
	if prev.keys == nil || prev.rawDir == rawDir || prev.canonicalDir != canonicalDir {
		return false
	}

	if prev.rawDir == prev.canonicalDir && rawDir == canonicalDir {
		return false
	}

	_, ok := prev.keys[key]

	return ok
}

func newSummariseDGUTARowKey(dir string, guta *db.GUTA) summariseDGUTARowKey {
	return summariseDGUTARowKey{
		dir:         dir,
		gid:         guta.GID,
		uid:         guta.UID,
		ft:          uint16(guta.FT),
		age:         uint8(guta.Age),
		count:       guta.Count,
		size:        guta.Size,
		atime:       guta.Atime,
		mtime:       guta.Mtime,
		aTimeRanges: guta.ATimeRanges,
		mTimeRanges: guta.MTimeRanges,
	}
}

func (w *summariseDGUTASpoolWriter) writeDirFactRow( //nolint:funlen
	record summariseDGUTARecordContext,
	gutas db.GUTAs,
	childCount uint64,
) error {
	allSummary, fileSummary := summariseMountDirRecordSummaries(gutas)
	if allSummary == nil {
		allSummary = newSummariseMountDirRecordSummary()
	}

	columns := summariseMountDirProjectionVectorColumnsFor(gutas)

	row := chspool.DirFactRow{
		MountPath:        w.mountPath,
		SnapshotID:       w.snapshotID,
		DirID:            record.dirID,
		ParentID:         record.parentID,
		SubtreeEnd:       record.subtreeEnd,
		UpdatedAt:        w.updatedAt,
		AllCount:         allSummary.count,
		AllSize:          allSummary.size,
		AllAtimeMin:      allSummary.atimeMin,
		AllMtimeMax:      allSummary.mtimeMax,
		AllAtimeBuckets:  summariseAgeBucketsSlice(&allSummary.atimeBuckets),
		AllMtimeBuckets:  summariseAgeBucketsSlice(&allSummary.mtimeBuckets),
		AllUIDs:          allSummary.sortedUIDs(),
		AllGIDs:          allSummary.sortedGIDs(),
		AllFT:            uint16(allSummary.ft),
		FileCount:        summariseRecordSummaryCount(fileSummary),
		FileSize:         summariseRecordSummarySize(fileSummary),
		FileAtimeMin:     summariseRecordSummaryAtimeMin(fileSummary),
		FileMtimeMax:     summariseRecordSummaryMtimeMax(fileSummary),
		FileAtimeBuckets: summariseRecordSummaryATimeBuckets(fileSummary),
		FileMtimeBuckets: summariseRecordSummaryMTimeBuckets(fileSummary),
		FileUIDs:         summariseRecordSummaryUIDs(fileSummary),
		FileGIDs:         summariseRecordSummaryGIDs(fileSummary),
		FileFT:           summariseRecordSummaryFT(fileSummary),
		GIDs:             columns.gids,
		UIDs:             columns.uids,
		FTs:              columns.fts,
		Ages:             columns.ages,
		Counts:           columns.counts,
		Sizes:            columns.sizes,
		AtimeMins:        columns.atimeMins,
		MtimeMaxs:        columns.mtimeMaxs,
		AtimeBuckets:     columns.atimeBuckets,
		MtimeBuckets:     columns.mtimeBuckets,
		ChildCount:       childCount,
		RefreshedAt:      w.refreshedAt,
	}

	w.noteActiveVirtualRootFact(record.canonicalDir, row)

	return w.ds.set.WriteDirFact(row)
}

func (w *summariseDGUTASpoolWriter) writeProjectionSetRow() error {
	if w.projectionSetWritten {
		return nil
	}

	w.projectionSetWritten = true

	return w.ds.set.WriteDirProjectionSet(chspool.DirProjectionSetRow{
		MountPath:   w.mountPath,
		SnapshotID:  w.snapshotID,
		UpdatedAt:   w.updatedAt,
		RefreshedAt: w.refreshedAt,
	})
}

func (s *summariseBasedirsSpoolStore) SetMountPath(mountPath string) {
	s.mountPath = summariseNormalizeImportMountPath(mountPath)
	s.refreshIDs()
}

func (s *summariseBasedirsSpoolStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
	s.refreshIDs()
}

func (s *summariseBasedirsSpoolStore) refreshIDs() {
	if s.mountPath != "" && !s.updatedAt.IsZero() {
		s.snapshotID = clickhouse.SnapshotID(s.mountPath, s.updatedAt)
	}
}

func (s *summariseBasedirsSpoolStore) Reset() error {
	s.refreshIDs()
	s.reset = true
	s.bufferedAgeAllGroupUsage = map[uint32][]*basedirs.Usage{}

	return nil
}

func (s *summariseBasedirsSpoolStore) ensureReady() error {
	if !s.reset {
		return errSummariseSpoolBasedirsNotReset
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) PutGroupUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	if u.Age == db.DGUTAgeAll {
		s.bufferedAgeAllGroupUsage[u.GID] = append(s.bufferedAgeAllGroupUsage[u.GID], u)

		return nil
	}

	return s.writeGroupUsage(u, summariseUnixEpochUTC(), summariseUnixEpochUTC())
}

func (s *summariseBasedirsSpoolStore) PutUserUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	return s.ds.set.WriteBasedirsUserUsage(chspool.BasedirsUserUsageRow{
		MountPath:       s.mountPath,
		SnapshotID:      s.snapshotID,
		UID:             u.UID,
		BaseDirID:       0,
		BaseDirExternal: u.BaseDir,
		Age:             uint8(u.Age),
		GIDs:            summariseEnsureNonNilUInt32s(u.GIDs),
		UsageSize:       u.UsageSize,
		QuotaSize:       u.QuotaSize,
		UsageInodes:     u.UsageInodes,
		QuotaInodes:     u.QuotaInodes,
		Mtime:           u.Mtime,
	})
}

func (s *summariseBasedirsSpoolStore) PutGroupSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.writeSubDirs(chspool.TableBasedirsGroupSubdirs, key, subdirs)
}

func (s *summariseBasedirsSpoolStore) PutUserSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.writeSubDirs(chspool.TableBasedirsUserSubdirs, key, subdirs)
}

func (s *summariseBasedirsSpoolStore) writeSubDirs( //nolint:funlen,gocognit,gocyclo
	table string,
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	for pos, sd := range subdirs {
		if sd == nil {
			continue
		}

		if uint64(pos) > math.MaxUint32 {
			return errSummariseSpoolSubdirPositionInvalid
		}

		row := chspool.BasedirsSubdirRow{
			MountPath:       s.mountPath,
			SnapshotID:      s.snapshotID,
			ID:              key.ID,
			BaseDirID:       key.ID,
			SubDirID:        0,
			BaseDirExternal: key.BaseDir,
			SubDirExternal:  sd.SubDir,
			Age:             uint8(key.Age),
			Pos:             uint32(pos),
			NumFiles:        sd.NumFiles,
			SizeFiles:       sd.SizeFiles,
			LastModified:    sd.LastModified,
			FileUsage:       summariseUsageBreakdownToCHMap(sd.FileUsage),
		}

		if table == chspool.TableBasedirsGroupSubdirs { //nolint:nestif
			if err := s.ds.set.WriteBasedirsGroupSubdir(row); err != nil {
				return err
			}
		} else if err := s.ds.set.WriteBasedirsUserSubdir(row); err != nil {
			return err
		}
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) AppendGroupHistory(
	key basedirs.HistoryKey,
	point basedirs.History,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	return s.ds.set.WriteBasedirsHistory(chspool.BasedirsHistoryRow{
		MountPath:   key.MountPath,
		GID:         key.GID,
		Date:        point.Date,
		UsageSize:   point.UsageSize,
		QuotaSize:   point.QuotaSize,
		UsageInodes: point.UsageInodes,
		QuotaInodes: point.QuotaInodes,
	})
}

func (s *summariseBasedirsSpoolStore) Finalise() error { //nolint:gocognit
	if err := s.ensureReady(); err != nil {
		return err
	}

	for _, usages := range s.bufferedAgeAllGroupUsage {
		for _, u := range usages {
			if u == nil {
				continue
			}

			if err := s.writeGroupUsage(u, time.Time{}, time.Time{}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) Close() error {
	return nil
}

func (s *summariseBasedirsSpoolStore) writeGroupUsage(
	u *basedirs.Usage,
	dateNoSpace time.Time,
	dateNoFiles time.Time,
) error {
	return s.ds.set.WriteBasedirsGroupUsage(chspool.BasedirsGroupUsageRow{
		MountPath:       s.mountPath,
		SnapshotID:      s.snapshotID,
		GID:             u.GID,
		BaseDirID:       0,
		BaseDirExternal: u.BaseDir,
		Age:             uint8(u.Age),
		UIDs:            summariseEnsureNonNilUInt32s(u.UIDs),
		UsageSize:       u.UsageSize,
		QuotaSize:       u.QuotaSize,
		UsageInodes:     u.UsageInodes,
		QuotaInodes:     u.QuotaInodes,
		Mtime:           u.Mtime,
		DateNoSpace:     dateNoSpace,
		DateNoFiles:     dateNoFiles,
	})
}

func summariseMountDirRecordSummaries(
	gutas db.GUTAs,
) (*summariseMountDirRecordSummary, *summariseMountDirRecordSummary) {
	var allSummary, fileSummary *summariseMountDirRecordSummary

	for _, guta := range gutas {
		allSummary, fileSummary = addSummariseAgeAllMountDirRecordSummary(allSummary, fileSummary, guta)
	}

	return allSummary, fileSummary
}

func addSummariseAgeAllMountDirRecordSummary(
	allSummary *summariseMountDirRecordSummary,
	fileSummary *summariseMountDirRecordSummary,
	guta *db.GUTA,
) (*summariseMountDirRecordSummary, *summariseMountDirRecordSummary) {
	if guta == nil || guta.Age != db.DGUTAgeAll {
		return allSummary, fileSummary
	}

	if allSummary == nil {
		allSummary = newSummariseMountDirRecordSummary()
	}

	allSummary.add(guta)

	if guta.FT&db.AllTypesExceptDirectories == 0 {
		return allSummary, fileSummary
	}

	if fileSummary == nil {
		fileSummary = newSummariseMountDirRecordSummary()
	}

	fileSummary.add(guta)

	return allSummary, fileSummary
}

func newSummariseMountDirRecordSummary() *summariseMountDirRecordSummary {
	return &summariseMountDirRecordSummary{}
}

func (s *summariseMountDirRecordSummary) add(guta *db.GUTA) {
	s.count += guta.Count
	s.size += guta.Size

	if guta.Atime != 0 && (s.atimeMin == 0 || guta.Atime < s.atimeMin) {
		s.atimeMin = guta.Atime
	}

	if guta.Mtime > s.mtimeMax {
		s.mtimeMax = guta.Mtime
	}

	for i, count := range guta.ATimeRanges {
		s.atimeBuckets[i] += count
	}

	for i, count := range guta.MTimeRanges {
		s.mtimeBuckets[i] += count
	}

	s.uids = summariseAppendUniqueUint32(s.uids, guta.UID)
	s.gids = summariseAppendUniqueUint32(s.gids, guta.GID)
	s.ft |= guta.FT
}

func summariseAppendUniqueUint32(values []uint32, value uint32) []uint32 {
	if slices.Contains(values, value) {
		return values
	}

	return append(values, value)
}

func (s *summariseMountDirRecordSummary) sortedUIDs() []uint32 {
	return summariseSortedUint32Slice(s.uids)
}

func (s *summariseMountDirRecordSummary) sortedGIDs() []uint32 {
	return summariseSortedUint32Slice(s.gids)
}

func summariseSortedUint32Slice(values []uint32) []uint32 {
	slices.Sort(values)

	return values
}

//nolint:funlen
func summariseMountDirProjectionVectorColumnsFor(
	gutas db.GUTAs,
) summariseMountDirProjectionVectorColumns {
	slices.SortFunc(gutas, summariseCompareProjectionGUTAs)

	columns := summariseMountDirProjectionVectorColumns{
		gids:         make([]uint32, 0, len(gutas)),
		uids:         make([]uint32, 0, len(gutas)),
		fts:          make([]uint16, 0, len(gutas)),
		ages:         make([]uint8, 0, len(gutas)),
		counts:       make([]uint64, 0, len(gutas)),
		sizes:        make([]uint64, 0, len(gutas)),
		atimeMins:    make([]int64, 0, len(gutas)),
		mtimeMaxs:    make([]int64, 0, len(gutas)),
		atimeBuckets: make([][]uint64, 0, len(gutas)),
		mtimeBuckets: make([][]uint64, 0, len(gutas)),
	}

	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		columns.gids = append(columns.gids, guta.GID)
		columns.uids = append(columns.uids, guta.UID)
		columns.fts = append(columns.fts, uint16(guta.FT))
		columns.ages = append(columns.ages, uint8(guta.Age))
		columns.counts = append(columns.counts, guta.Count)
		columns.sizes = append(columns.sizes, guta.Size)
		columns.atimeMins = append(columns.atimeMins, guta.Atime)
		columns.mtimeMaxs = append(columns.mtimeMaxs, guta.Mtime)
		columns.atimeBuckets = append(columns.atimeBuckets, summariseAgeBucketsSlice(&guta.ATimeRanges))
		columns.mtimeBuckets = append(columns.mtimeBuckets, summariseAgeBucketsSlice(&guta.MTimeRanges))
	}

	return columns
}

func summariseCompareProjectionGUTAs(a, b *db.GUTA) int { //nolint:gocyclo
	if a == nil && b == nil {
		return 0
	}

	if a == nil {
		return -1
	}

	if b == nil {
		return 1
	}

	if diff := cmp.Compare(a.Age, b.Age); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.GID, b.GID); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.UID, b.UID); diff != 0 {
		return diff
	}

	return cmp.Compare(a.FT, b.FT)
}

func summariseRecordSummaryCount(summary *summariseMountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.count
}

func summariseRecordSummarySize(summary *summariseMountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.size
}

func summariseRecordSummaryAtimeMin(summary *summariseMountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.atimeMin
}

func summariseRecordSummaryMtimeMax(summary *summariseMountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.mtimeMax
}

func summariseRecordSummaryATimeBuckets(summary *summariseMountDirRecordSummary) []uint64 {
	if summary == nil {
		return summariseAgeBucketsSlice(nil)
	}

	return summariseAgeBucketsSlice(&summary.atimeBuckets)
}

func summariseRecordSummaryMTimeBuckets(summary *summariseMountDirRecordSummary) []uint64 {
	if summary == nil {
		return summariseAgeBucketsSlice(nil)
	}

	return summariseAgeBucketsSlice(&summary.mtimeBuckets)
}

func summariseRecordSummaryUIDs(summary *summariseMountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedUIDs()
}

func summariseRecordSummaryGIDs(summary *summariseMountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedGIDs()
}

func summariseRecordSummaryFT(summary *summariseMountDirRecordSummary) uint16 {
	if summary == nil {
		return 0
	}

	return uint16(summary.ft)
}

func summariseAgeBucketsSlice(buckets *summary.AgeBuckets) []uint64 {
	if buckets == nil {
		return make([]uint64, len(summary.AgeBuckets{}))
	}

	return buckets[:]
}

func summariseCanonicalFileIngestPath(mountPath, parentDir, name string) (string, string, bool) {
	parentDir = summariseCanonicalPathForMount(mountPath, parentDir)

	selfNamedDir := strings.HasSuffix(name, "/") && strings.HasSuffix(parentDir, name)
	if !selfNamedDir {
		return parentDir, name, true
	}

	if parentDir == "/" && name == "/" {
		return "", "", false
	}

	return strings.TrimSuffix(parentDir, name), name, true
}

func summariseCanonicalPathForMount(mountPath, path string) string {
	if mountPath != "/" || path == "" {
		return path
	}

	trimmed := strings.TrimLeft(path, "/")
	if trimmed == "" {
		return "/"
	}

	return "/" + trimmed
}

func summariseNormalizeImportMountPath(mountPath string) string {
	if mountPath == "" || mountPath == "/" || strings.HasSuffix(mountPath, "/") {
		return mountPath
	}

	return mountPath + "/"
}

func summariseExtFromName(name string) string {
	if strings.HasSuffix(name, "/") {
		return ""
	}

	idx := strings.LastIndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return ""
	}

	return strings.ToLower(name[idx+1:])
}

func summariseEnsureNonNilUInt32s(values []uint32) []uint32 {
	if values == nil {
		return []uint32{}
	}

	return values
}

func summariseUsageBreakdownToCHMap(in basedirs.UsageBreakdownByType) map[uint16]uint64 {
	if in == nil {
		return map[uint16]uint64{}
	}

	out := make(map[uint16]uint64, len(in))
	for ft, v := range in {
		out[uint16(ft)] = v
	}

	return out
}

func summariseUnixEpochUTC() time.Time {
	return time.Unix(0, 0).UTC()
}

func summariseSpoolOutputDir() (string, error) {
	if defaultDir != "" {
		return defaultDir, nil
	}

	if basedirsDB != "" {
		return filepath.Dir(basedirsDB), nil
	}

	if dirgutaDB != "" {
		return filepath.Dir(dirgutaDB), nil
	}

	return "", errNoOutputDir
}

func statsFileModtime(statsPath string) (time.Time, error) {
	if statsPath == "-" {
		return time.Now(), nil
	}

	st, err := os.Stat(statsPath)
	if err != nil {
		return time.Time{}, err
	}

	return st.ModTime(), nil
}

func summariseBoolAsUInt8(v bool) uint8 {
	if v {
		return 1
	}

	return 0
}
