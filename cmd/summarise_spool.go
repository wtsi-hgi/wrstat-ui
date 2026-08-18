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
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirbuild"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	clickHouseSpoolDirName         = ".wrstat-ui-clickhouse-spool"
	clickHouseSpoolBuildReportName = "summarise_build_report.json"
	clickHouseSpoolLoadReportName  = "spool_load_report.json"
	clickHouseSpoolSchemaMark      = "wrstat-ui-clickhouse-summarise-spool-v3"
	clickHouseSpoolSchema3Version  = 1

	summariseActiveVirtualRootID       uint32 = 1
	summariseActiveVirtualNoParentID   uint32 = 0
	summariseActiveVirtualZeroSnapshot        = "00000000-0000-0000-0000-000000000000"
	summariseBuildBytesPerKiB                 = 1024
)

var (
	loadSummariseClickHouseSpool = clickhouse.LoadSummariseSpoolReport
	summariseSpoolNow            = time.Now
	summariseSpoolDirGUTANow     = time.Now
	openSummariseSpoolStats      = openSummariseSpoolStatsFile
	buildSummariseSpoolDirbuild  = dirbuild.BuildWithFilesOptions
)

var (
	errSummariseSpoolFileDirRequired       = errors.New("clickhouse spool: file row requires directory path")
	errSummariseSpoolFileNameRequired      = errors.New("clickhouse spool: file row requires entry name")
	errSummariseSpoolFileNonNegative       = errors.New("clickhouse spool: file row requires non-negative numeric fields")
	errSummariseSpoolDirFactsDir           = errors.New("clickhouse spool: dir facts require dir")
	errSummariseSpoolBasedirsNotReset      = errors.New("clickhouse spool: basedirs store not reset")
	errSummariseSpoolSubdirPositionInvalid = errors.New("clickhouse spool: basedirs subdir position overflows UInt32")
	errSummariseSpoolNonContiguousStdin    = errors.New(
		"clickhouse spool: non-contiguous stdin requires a reopenable input",
	)
)

type summariseSpoolDirbuildFirstReason uint8

const (
	summariseSpoolDirbuildFirstNone summariseSpoolDirbuildFirstReason = iota
	summariseSpoolDirbuildFirstNonContiguous
)

func openSummariseSpoolStatsFile(statsPath string) (io.ReadCloser, error) {
	r, _, err := openStatsFile(statsPath)

	return r, err
}

type summariseSpoolContiguityResult struct {
	contiguous         bool
	violationRow       uint64
	violationPathDepth uint64
}

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
		VirtualID:        summary.VirtualID,
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

type summariseFullFilterPendingDir struct {
	dir              string
	rows             []chspool.DirFilterAllRow
	lastChildByTuple map[summariseFullFilterTupleKey]string
}

type summariseFullFilterDirectChildCounts map[summariseFullFilterTupleKey]uint64

type summariseMountActiveRow struct {
	mountPath  string
	snapshotID string
	updatedAt  time.Time
}

func summariseActiveVirtualCatalogRows(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	rootFacts map[string]chspool.DirFactRow,
	refreshedAt time.Time,
) []summariseActiveVirtualCatalogRow {
	namespace := newSummariseActiveVirtualNamespace(activeSetID, refreshedAt)

	for _, activeRow := range summariseSortedMountActiveRows(activeRows) {
		namespace.addMount(activeRow, rootFacts[summariseEnsureTrailingSlash(activeRow.mountPath)])
	}

	return namespace.rows
}

func newSummariseActiveVirtualNamespace(activeSetID string, refreshedAt time.Time) summariseActiveVirtualNamespace {
	root := summariseActiveVirtualCatalogRow{
		ActiveSetID: activeSetID,
		VirtualID:   summariseActiveVirtualRootID,
		ParentID:    summariseActiveVirtualNoParentID,
		Name:        "/",
		FullPath:    "/",
		SnapshotID:  summariseActiveVirtualZeroSnapshot,
		RefreshedAt: refreshedAt,
	}

	return summariseActiveVirtualNamespace{
		rows:   []summariseActiveVirtualCatalogRow{root},
		byPath: map[string]int{"/": 0},
	}
}

func summariseSortedMountActiveRows(rows []summariseMountActiveRow) []summariseMountActiveRow {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b summariseMountActiveRow) int {
		return strings.Compare(summariseEnsureTrailingSlash(a.mountPath), summariseEnsureTrailingSlash(b.mountPath))
	})

	return out
}

func summariseActiveVirtualCatalogChildCounts(rows []summariseActiveVirtualCatalogRow) map[uint32]uint64 {
	out := make(map[uint32]uint64, len(rows))
	for _, row := range rows {
		if row.ParentID != summariseActiveVirtualNoParentID {
			out[row.ParentID]++
		}
	}

	return out
}

func summariseActiveVirtualSummaryRowsFromCatalog(
	activeSetID string,
	catalogRows []summariseActiveVirtualCatalogRow,
	childCounts map[uint32]uint64,
	updatedAt time.Time,
	refreshedAt time.Time,
) []summariseActiveVirtualSummaryRow {
	out := make([]summariseActiveVirtualSummaryRow, 0, len(catalogRows))
	for _, row := range catalogRows {
		out = append(out, summariseActiveVirtualSummaryRow{
			ActiveSetID:     activeSetID,
			VirtualID:       row.VirtualID,
			Dir:             row.FullPath,
			MountPath:       row.MountPath,
			SnapshotID:      row.SnapshotID,
			MountRootDirID:  row.MountRootDirID,
			IsMountRootBox:  row.IsMountRootBox,
			UpdatedAt:       updatedAt,
			AllAtimeBuckets: summariseAgeBucketsSlice(nil),
			AllMtimeBuckets: summariseAgeBucketsSlice(nil),
			ChildCount:      childCounts[row.VirtualID],
			RefreshedAt:     refreshedAt,
		})
	}

	slices.SortFunc(out, func(a, b summariseActiveVirtualSummaryRow) int {
		return cmp.Compare(a.VirtualID, b.VirtualID)
	})

	return out
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
	VirtualID       uint32
	Dir             string
	MountPath       string
	SnapshotID      string
	MountRootDirID  uint32
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
	VirtualID        uint32
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
	ActiveSetID     string
	ParentDir       string
	ChildDir        string
	ParentVirtualID uint32
	ChildVirtualID  uint32
	MountPath       string
	SnapshotID      string
	MountRootDirID  uint32
	IsMountRootBox  uint8
	ChildCount      uint64
	RefreshedAt     time.Time
}

func summariseActiveVirtualCatalogRowsByID(
	rows []summariseActiveVirtualCatalogRow,
) map[uint32]summariseActiveVirtualCatalogRow {
	out := make(map[uint32]summariseActiveVirtualCatalogRow, len(rows))
	for _, row := range rows {
		out[row.VirtualID] = row
	}

	return out
}

type summariseActiveVirtualCatalogRow struct {
	ActiveSetID    string
	VirtualID      uint32
	ParentID       uint32
	Name           string
	FullPath       string
	MountPath      string
	SnapshotID     string
	MountRootDirID uint32
	IsMountRootBox uint8
	RefreshedAt    time.Time
}

type summariseActiveVirtualNamespace struct {
	rows   []summariseActiveVirtualCatalogRow
	byPath map[string]int
}

func (n *summariseActiveVirtualNamespace) addMount(
	activeRow summariseMountActiveRow,
	rootFact chspool.DirFactRow,
) {
	parent := "/"
	mountPath := summariseEnsureTrailingSlash(activeRow.mountPath)

	for {
		child, ok := summariseImmediateChildForMount(parent, mountPath)
		if !ok {
			return
		}

		child = summariseEnsureTrailingSlash(child)
		childIsMountRoot := child == mountPath

		row := n.ensureChild(parent, child)
		if childIsMountRoot {
			n.markMountRoot(row, activeRow, rootFact)

			return
		}

		parent = child
	}
}

func (n *summariseActiveVirtualNamespace) ensureChild(
	parentPath string,
	childPath string,
) *summariseActiveVirtualCatalogRow {
	childPath = summariseEnsureTrailingSlash(childPath)
	if idx, ok := n.byPath[childPath]; ok {
		return &n.rows[idx]
	}

	parentID := n.idForPath(parentPath)
	n.rows = append(n.rows, summariseActiveVirtualCatalogRow{
		ActiveSetID: n.rows[0].ActiveSetID,
		VirtualID:   uint32(len(n.rows) + 1), //nolint:gosec // Namespace rows are bounded by active mount path segments.
		ParentID:    parentID,
		Name:        summariseCatalogNameForFullPath(childPath),
		FullPath:    childPath,
		SnapshotID:  summariseActiveVirtualZeroSnapshot,
		RefreshedAt: n.rows[0].RefreshedAt,
	})

	n.byPath[childPath] = len(n.rows) - 1

	return &n.rows[len(n.rows)-1]
}

func (n *summariseActiveVirtualNamespace) markMountRoot(
	row *summariseActiveVirtualCatalogRow,
	activeRow summariseMountActiveRow,
	rootFact chspool.DirFactRow,
) {
	row.MountPath = summariseEnsureTrailingSlash(activeRow.mountPath)
	row.SnapshotID = activeRow.snapshotID
	row.MountRootDirID = rootFact.DirID
	row.IsMountRootBox = 1
}

func (n summariseActiveVirtualNamespace) idForPath(path string) uint32 {
	if idx, ok := n.byPath[summariseEnsureTrailingSlash(path)]; ok {
		return n.rows[idx].VirtualID
	}

	return 0
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

type summariseDirbuildBasedirsDirectKey struct {
	id  uint32
	age db.DirGUTAge
	dir string
}

type summariseDirbuildBasedirsDirectSummary struct {
	summary  *basedirs.SummaryWithChildren
	children map[string]*basedirs.SubDir
}

func (s *summariseDirbuildBasedirsDirectSummary) child(childDir string) *basedirs.SubDir {
	if s.children == nil {
		s.children = make(map[string]*basedirs.SubDir)
	}

	child, ok := s.children[childDir]
	if !ok {
		child = &basedirs.SubDir{
			SubDir:    childDir,
			FileUsage: make(basedirs.UsageBreakdownByType),
		}
		s.children[childDir] = child
		s.summary.Children = append(s.summary.Children, child)
	}

	return child
}

type summariseDirbuildBasedirsDirectMap map[summariseDirbuildBasedirsDirectKey]*summariseDirbuildBasedirsDirectSummary

func (m summariseDirbuildBasedirsDirectMap) summary(
	id uint32,
	age db.DirGUTAge,
	dir string,
) *summariseDirbuildBasedirsDirectSummary {
	key := summariseDirbuildBasedirsDirectKey{id: id, age: age, dir: dir}

	entry, ok := m[key]
	if !ok {
		entry = &summariseDirbuildBasedirsDirectSummary{
			summary: summariseNewDirbuildBasedirsSummary(dir, age),
		}
		m[key] = entry
	}

	return entry
}

type summariseSpoolContiguityProbeState struct {
	parser             *stats.StatsParser
	violationRow       uint64
	violationPathDepth uint64
}

type summariseSpoolContiguityProbe struct {
	alloc   *summary.DirIDAllocator
	parent  *summariseSpoolContiguityProbe
	state   *summariseSpoolContiguityProbeState
	dir     *summary.DirectoryPath
	entered bool
}

func (p *summariseSpoolContiguityProbe) Add(info *summary.FileInfo) error {
	if p.entered {
		return nil
	}

	p.dir = info.Path

	if err := p.validateParent(); err != nil {
		p.recordViolation(err)

		return err
	}

	_, err := p.alloc.Enter(p.dir)
	if err != nil {
		p.recordViolation(err)

		return err
	}

	p.entered = true

	return nil
}

func (p *summariseSpoolContiguityProbe) validateParent() error {
	if p.parent == nil {
		_, err := summary.ReservedParentIDForDepth(p.dir.Depth)

		return err
	}

	if p.parent.dir != p.dir.Parent || !p.parent.entered {
		return summary.ErrNonContiguousInput
	}

	return nil
}

func (p *summariseSpoolContiguityProbe) Output() error {
	if !p.entered {
		return nil
	}

	_, err := p.alloc.Leave(p.dir)
	p.recordViolation(err)
	p.dir = nil
	p.entered = false

	return err
}

func newSummariseSpoolContiguityProbe(
	mountPath string,
) (summary.OperationGenerator, *summariseSpoolContiguityProbeState, error) {
	alloc := summary.NewDirIDAllocator()
	if err := alloc.SetMountPath(mountPath); err != nil {
		return nil, nil, err
	}

	var (
		last  *summariseSpoolContiguityProbe
		state = new(summariseSpoolContiguityProbeState)
	)

	return func() summary.Operation {
		last = &summariseSpoolContiguityProbe{alloc: alloc, parent: last, state: state}

		return last
	}, state, nil
}

func (p *summariseSpoolContiguityProbe) recordViolation(err error) {
	if !errors.Is(err, summary.ErrNonContiguousInput) || p.state.violationRow != 0 {
		return
	}

	depth, depthErr := summary.ReservedDirIDForDepth(p.dir.Depth)
	if depthErr != nil {
		return
	}

	p.state.violationRow = p.state.parser.InputRow()
	p.state.violationPathDepth = uint64(depth)
}

func (b *summariseDirbuildBasedirsBuilder) addFileSummaries(
	info *summary.FileInfo,
	size uint64,
	baseDir string,
	childDir string,
	tempDir bool,
) {
	for _, age := range db.DirGUTAges {
		if !age.FitsAgeInterval(info.ATime, info.MTime, b.refUnix) {
			continue
		}

		summariseDirbuildBasedirsAddDirectFile(b.groupSums.summary(info.GID, age, baseDir), childDir, info, size, tempDir)
		summariseDirbuildBasedirsAddDirectFile(b.userSums.summary(info.UID, age, baseDir), childDir, info, size, tempDir)
	}
}

func summariseDirbuildBasedirsAddDirectFile(
	entry *summariseDirbuildBasedirsDirectSummary,
	childDir string,
	info *summary.FileInfo,
	size uint64,
	tempDir bool,
) {
	summariseDirbuildBasedirsAddFile(entry.summary, info, size, tempDir)

	if childDir == "." {
		return
	}

	child := entry.child(childDir)
	summariseDirbuildBasedirsAddSubdirFile(child, info, size, tempDir)
}

func (b *summariseDirbuildBasedirsBuilder) seenHardlink(dirID uint32, info *summary.FileInfo) bool {
	if info.Nlink <= 1 || info.Inode == 0 {
		return false
	}

	seenInodes := b.seenInodes[dirID]
	if seenInodes == nil {
		seenInodes = make(map[int64]struct{})
		b.seenInodes[dirID] = seenInodes
	}

	if _, seen := seenInodes[info.Inode]; seen {
		return true
	}

	seenInodes[info.Inode] = struct{}{}

	return false
}

func (b *summariseDirbuildBasedirsBuilder) baseAndChildDir(info *summary.FileInfo) (string, string, bool) {
	base := summariseDirbuildBasedirsBasePath(b.config, info.Path)
	if base == nil {
		return "", "", false
	}

	baseDir := string(base.AppendTo(make([]byte, 0, base.Len())))
	baseDir = summariseEnsureTrailingSlash(summariseCanonicalPathForMount(b.mountPath, baseDir))

	return baseDir, summariseDirbuildBasedirsImmediateChild(base, info.Path), true
}

func summariseDirbuildBasedirsBasePath(
	config basedirs.Config,
	path *summary.DirectoryPath,
) *summary.DirectoryPath {
	for path != nil {
		if config.PathShouldOutput(path) {
			return path
		}

		path = path.Parent
	}

	return nil
}

func summariseDirbuildBasedirsImmediateChild(
	base *summary.DirectoryPath,
	dir *summary.DirectoryPath,
) string {
	if base == nil || dir == nil || dir.Depth <= base.Depth {
		return "."
	}

	child := dir
	for child.Parent != base {
		child = child.Parent
	}

	return child.Name
}

func (b *summariseDirbuildBasedirsBuilder) addDirectOutputMap(
	out basedirs.IDAgeDirs,
	in summariseDirbuildBasedirsDirectMap,
) {
	for key, entry := range in {
		summariseDirbuildBasedirsAddOutputSummary(out, key.id, key.age, entry.summary)
		delete(in, key)
	}
}

func (w *summariseDGUTASpoolWriter) noteSchema3FutureDirectChildTuples(
	parentDir string,
	tuples map[summariseFullFilterTupleKey]struct{},
) {
	if w.fullFilterFutureChildren == nil {
		w.fullFilterFutureChildren = make(map[string]summariseFullFilterDirectChildCounts)
	}

	counts := w.fullFilterFutureChildren[parentDir]
	if counts == nil {
		counts = make(summariseFullFilterDirectChildCounts, len(tuples))
		w.fullFilterFutureChildren[parentDir] = counts
	}

	for tuple := range tuples {
		counts[tuple]++
	}
}

func (w *summariseDGUTASpoolWriter) applySchema3FutureDirectChildTuples(
	dir string,
	rows []chspool.DirFilterAllRow,
) {
	dir = summariseEnsureTrailingSlash(dir)

	counts := w.fullFilterFutureChildren[dir]
	if len(counts) == 0 {
		return
	}

	delete(w.fullFilterFutureChildren, dir)

	for idx := range rows {
		tuple := summariseFullFilterKeyForRow(rows[idx])
		rows[idx].FilterChildCount += counts[tuple]
	}
}

func finishSummariseSpoolScratch(partialDir string, manifest *chspool.Manifest) (uint64, error) {
	scratchBytes, err := summariseBuildPhaseBytesWritten(partialDir, manifest)
	if err != nil {
		return scratchBytes, err
	}

	return scratchBytes, cleanupSummariseBuildPhaseScratch(partialDir, manifest)
}

func cleanupSummariseBuildPhaseScratch(partialDir string, manifest *chspool.Manifest) error {
	canonical := summariseBuildCanonicalArtifactPaths(manifest)

	entries, err := os.ReadDir(partialDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if summariseBuildCanonicalTopLevel(entry.Name(), canonical) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(partialDir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

func summariseBuildCanonicalTopLevel(name string, canonical map[string]bool) bool {
	cleanName := filepath.Clean(name)
	for path := range canonical {
		cleanPath := filepath.Clean(path)
		if cleanPath == cleanName || strings.HasPrefix(cleanPath, cleanName+string(os.PathSeparator)) {
			return true
		}
	}

	return false
}

func summariseSpoolDirbuildOptions(partialDir string) dirbuild.Options {
	return dirbuild.Options{
		TempDir:       partialDir,
		RetainTempDir: true,
	}
}

func summariseSpoolProbeContiguity(statsPath string, mountPath string) (summariseSpoolContiguityResult, error) {
	reader, err := openSummariseSpoolStats(statsPath)
	if err != nil {
		return summariseSpoolContiguityResult{}, err
	}

	generator, state, err := newSummariseSpoolContiguityProbe(mountPath)
	if err != nil {
		_ = reader.Close()

		return summariseSpoolContiguityResult{}, err
	}

	parser := stats.NewStatsParser(reader)
	state.parser = parser
	s := summary.NewSummariser(parser)
	s.AddDirectoryOperation(generator)

	scanErr := s.Summarise()
	closeErr := reader.Close()

	result := summariseSpoolContiguityResult{
		contiguous:         scanErr == nil && closeErr == nil,
		violationRow:       state.violationRow,
		violationPathDepth: state.violationPathDepth,
	}
	if errors.Is(scanErr, summary.ErrNonContiguousInput) {
		return result, closeErr
	}

	return result, errors.Join(scanErr, closeErr)
}

func summariseSpoolDirbuildFirstInputShape(_ summariseSpoolDirbuildFirstReason) string {
	return chperf.A5BuildInputNonContiguous
}

func handleSummariseSpoolFastPathViolation(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
	records uint64,
	scratchBytes uint64,
) summariseSpoolBuildResult {
	if statsPath == "-" {
		return summariseSpoolBuildResult{
			records:      records,
			scratchBytes: scratchBytes,
			err:          errSummariseSpoolNonContiguousStdin,
		}
	}

	return retrySummariseSpoolWithDirbuild(statsPath, partialDir, expected, target, diag, scratchBytes)
}

func summariseBuildReportInputs(
	target *clickHouseSummariseTarget,
	manifest *chspool.Manifest,
	inputShape string,
	buildPath string,
	records uint64,
	buildScratchBytes uint64,
	contiguityViolationRow uint64,
	contiguityViolationDepth uint64,
) map[string]any {
	inputs := map[string]any{
		chperf.A5BuildInputRole:              summariseBuildA5Role(target.mountPath),
		chperf.A5BuildInputShape:             inputShape,
		chperf.A5BuildInputPath:              buildPath,
		chperf.A5BuildInputCompleted:         true,
		chperf.A5BuildInputRowCap:            records,
		chperf.A5BuildInputBuildScratchBytes: buildScratchBytes,
		chperf.A5BuildInputSpoolBytes:        summariseBuildManifestBytes(manifest),
		"mount_path":                         target.mountPath,
	}
	if contiguityViolationRow != 0 {
		inputs["contiguity_violation_row"] = contiguityViolationRow
		inputs["contiguity_violation_path_depth"] = contiguityViolationDepth
	}

	return inputs
}

func summariseDirbuildBasedirsMergeSummary(
	parent *basedirs.SummaryWithChildren,
	child *basedirs.SummaryWithChildren,
	parentDir string,
) {
	if parent == nil || child == nil {
		return
	}

	parentDot := summariseDirbuildBasedirsDot(parent)
	childDot := summariseDirbuildBasedirsDot(child)

	for ft, size := range childDot.FileUsage {
		parentDot.FileUsage[ft] += size
	}

	parent.UIDs = summariseAppendUniqueUint32Slice(parent.UIDs, child.UIDs)
	parent.GIDs = summariseAppendUniqueUint32Slice(parent.GIDs, child.GIDs)
	summariseDirbuildBasedirsSetTimes(parent, child.Atime, child.Mtime)

	parentDot.NumFiles += childDot.NumFiles
	parentDot.SizeFiles += childDot.SizeFiles
	childDot.SubDir = summariseDirbuildBasedirsChildName(parentDir, child.Dir)
	parent.Children = append(parent.Children, childDot)
}

func summariseDirbuildBasedirsDot(summary *basedirs.SummaryWithChildren) *basedirs.SubDir {
	if len(summary.Children) == 0 {
		summary.Children = []*basedirs.SubDir{{FileUsage: make(basedirs.UsageBreakdownByType)}}
	}

	if summary.Children[0].FileUsage == nil {
		summary.Children[0].FileUsage = make(basedirs.UsageBreakdownByType)
	}

	return summary.Children[0]
}

func summariseDirbuildBasedirsSetTimes(
	summary *basedirs.SummaryWithChildren,
	atime time.Time,
	mtime time.Time,
) {
	if atime.Before(summary.Atime) || summary.Atime.IsZero() {
		summary.Atime = atime
	}

	if mtime.After(summary.Mtime) {
		summary.Mtime = mtime
		summariseDirbuildBasedirsDot(summary).LastModified = mtime
	}
}

func summariseDirbuildBasedirsChildName(parentDir string, childDir string) string {
	parentDir = summariseEnsureTrailingSlash(parentDir)
	childDir = summariseEnsureTrailingSlash(childDir)

	relative := strings.TrimPrefix(childDir, parentDir)

	part, _, _ := strings.Cut(relative, "/")
	if part == "" {
		return "."
	}

	return part + "/"
}

func summariseNewDirbuildBasedirsSummary(
	dir string,
	age db.DirGUTAge,
) *basedirs.SummaryWithChildren {
	return &basedirs.SummaryWithChildren{
		DirSummary: db.DirSummary{
			Dir: summariseEnsureTrailingSlash(dir),
			Age: age,
		},
		Children: []*basedirs.SubDir{{
			FileUsage: make(basedirs.UsageBreakdownByType),
		}},
	}
}

type summariseDirbuildBasedirsBuilder struct {
	enabled    bool
	creator    *basedirs.BaseDirs
	config     basedirs.Config
	mountPath  string
	refUnix    int64
	seenInodes map[uint32]map[int64]struct{}
	userSums   summariseDirbuildBasedirsDirectMap
	groupSums  summariseDirbuildBasedirsDirectMap
	users      basedirs.IDAgeDirs
	groups     basedirs.IDAgeDirs
}

func newSummariseDirbuildBasedirsBuilder(
	ds *summariseSpoolDataset,
) (*summariseDirbuildBasedirsBuilder, error) {
	if basedirsDB == "" {
		return &summariseDirbuildBasedirsBuilder{}, nil
	}

	creator, config, err := newSummariseDirbuildBasedirsCreator(ds)
	if err != nil {
		return nil, err
	}

	return &summariseDirbuildBasedirsBuilder{
		enabled:    true,
		creator:    creator,
		config:     config,
		mountPath:  ds.mountPath,
		refUnix:    time.Now().Unix(),
		seenInodes: make(map[uint32]map[int64]struct{}),
		userSums:   make(summariseDirbuildBasedirsDirectMap),
		groupSums:  make(summariseDirbuildBasedirsDirectMap),
		users:      make(basedirs.IDAgeDirs),
		groups:     make(basedirs.IDAgeDirs),
	}, nil
}

func (b *summariseDirbuildBasedirsBuilder) addDir(_ summariseDGUTARecordContext) {
	if !b.enabled {
		return
	}
}

func (b *summariseDirbuildBasedirsBuilder) addFile(
	dirID uint32,
	info *summary.FileInfo,
	size uint64,
) error {
	if !b.enabled || info == nil || info.IsDir() {
		return nil
	}

	if b.seenHardlink(dirID, info) {
		return nil
	}

	baseDir, childDir, ok := b.baseAndChildDir(info)
	if !ok {
		return nil
	}

	tempDir := summariseDirbuildBasedirsPathIsTemp(info.Path)
	b.addFileSummaries(info, size, baseDir, childDir, tempDir)

	return nil
}

func summariseDirbuildBasedirsPathIsTemp(path *summary.DirectoryPath) bool {
	for path != nil {
		if dirguta.IsTemp([]byte(path.Name)) {
			return true
		}

		path = path.Parent
	}

	return false
}

func summariseDirbuildBasedirsAddFile(
	summary *basedirs.SummaryWithChildren,
	info *summary.FileInfo,
	size uint64,
	tempDir bool,
) {
	dot := summariseDirbuildBasedirsDot(summary)
	dot.NumFiles++
	dot.SizeFiles += size

	atime := time.Unix(info.ATime, 0)
	mtime := time.Unix(info.MTime, 0)
	summariseDirbuildBasedirsSetTimes(summary, atime, mtime)

	ft := dirguta.FilenameToType(info.Name)

	dot.FileUsage[ft] += size
	if tempDir || dirguta.IsTemp(info.Name) {
		dot.FileUsage[db.DGUTAFileTypeTemp] += size
	}

	summary.UIDs = summariseAppendUniqueUint32(summary.UIDs, info.UID)
	summary.GIDs = summariseAppendUniqueUint32(summary.GIDs, info.GID)
}

func (b *summariseDirbuildBasedirsBuilder) output() error {
	if !b.enabled {
		return nil
	}

	b.addDirectOutputMap(b.groups, b.groupSums)
	b.addDirectOutputMap(b.users, b.userSums)

	summariseDirbuildBasedirsNormalize(b.users)
	summariseDirbuildBasedirsNormalize(b.groups)

	return b.creator.Output(b.users, b.groups)
}

func summariseDirbuildBasedirsNormalize(idAgeDirs basedirs.IDAgeDirs) {
	for _, ageDirs := range idAgeDirs {
		if ageDirs == nil {
			continue
		}

		for age := range ageDirs {
			for idx := range ageDirs[age] {
				summariseDirbuildBasedirsNormalizeSummary(&ageDirs[age][idx])
			}
		}
	}
}

func newSummariseDGUTASpoolWriter(ds *summariseSpoolDataset) *summariseDGUTASpoolWriter {
	dw := &summariseDGUTASpoolWriter{ds: ds}
	dw.SetMountPath(ds.mountPath)
	dw.SetUpdatedAt(ds.updatedAt)
	ds.dgutaWriter = dw

	return dw
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
	w.applySchema3FutureDirectChildTuples(record.canonicalDir, rows)

	if len(rows) == 0 {
		return nil
	}

	w.fullFilterPending = append(w.fullFilterPending, summariseFullFilterPendingDir{
		dir:  summariseEnsureTrailingSlash(record.canonicalDir),
		rows: rows,
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
	if parentDir == childDir {
		return
	}

	foundPendingParent := false

	for idx := range w.fullFilterPending {
		if w.fullFilterPending[idx].dir != parentDir {
			continue
		}

		foundPendingParent = true

		summariseNotePendingDirectChildTuples(&w.fullFilterPending[idx], childDir, tuples)
	}

	if !foundPendingParent {
		w.noteSchema3FutureDirectChildTuples(parentDir, tuples)
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

		if pending.lastChildByTuple != nil && pending.lastChildByTuple[tuple] == childDir {
			continue
		}

		if pending.lastChildByTuple == nil {
			pending.lastChildByTuple = make(map[summariseFullFilterTupleKey]string, len(pending.rows))
		}

		pending.lastChildByTuple[tuple] = childDir
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
	return w.ds.set.WriteDirFilterAll(row)
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
		dirFilterAllRows:   tables[chspool.TableDirFilterAll].Rows,
		childFilterAllRows: tables[chspool.TableDirFilterAll].Rows,
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
	catalogRows, summaryRows, filterRows, childRows := summariseActiveVirtualRowsFromCanonicalData(
		activeSetID,
		activeRows,
		w.activeVirtualRootFacts,
		w.activeVirtualRootFilterRows,
		w.activeVirtualMountChildCounts,
		w.refreshedAt,
	)

	if err := w.writeActiveVirtualDataRows(catalogRows, summaryRows, filterRows, childRows); err != nil {
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
	catalogRows []summariseActiveVirtualCatalogRow,
	refreshedAt time.Time,
) []summariseActiveVirtualChildRow {
	byID := summariseActiveVirtualCatalogRowsByID(catalogRows)
	rows := make([]summariseActiveVirtualChildRow, 0, len(catalogRows))

	for _, row := range catalogRows {
		if row.ParentID == summariseActiveVirtualNoParentID {
			continue
		}

		parent := byID[row.ParentID]
		rows = append(rows, summariseActiveVirtualChildRow{
			ActiveSetID:     activeSetID,
			ParentDir:       parent.FullPath,
			ChildDir:        row.FullPath,
			ParentVirtualID: row.ParentID,
			ChildVirtualID:  row.VirtualID,
			MountPath:       row.MountPath,
			SnapshotID:      row.SnapshotID,
			MountRootDirID:  row.MountRootDirID,
			IsMountRootBox:  row.IsMountRootBox,
			RefreshedAt:     refreshedAt,
		})
	}

	return rows
}

func summariseActiveVirtualSummaryRows(
	activeSetID string,
	activeRows []summariseMountActiveRow,
	catalogRows []summariseActiveVirtualCatalogRow,
	refreshedAt time.Time,
) []summariseActiveVirtualSummaryRow {
	childCounts := summariseActiveVirtualCatalogChildCounts(catalogRows)

	return summariseActiveVirtualSummaryRowsFromCatalog(
		activeSetID,
		catalogRows,
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
) (
	[]summariseActiveVirtualCatalogRow,
	[]summariseActiveVirtualSummaryRow,
	[]summariseActiveVirtualFilterAllRow,
	[]summariseActiveVirtualChildRow,
) {
	catalogRows := summariseActiveVirtualCatalogRows(activeSetID, activeRows, rootFacts, refreshedAt)
	childRows := summariseActiveVirtualChildRows(activeSetID, catalogRows, refreshedAt)
	summaryRows := summariseActiveVirtualSummaryRows(activeSetID, activeRows, catalogRows, refreshedAt)
	contributors := summariseActiveVirtualContributors(summaryRows, activeRows)

	summariseFillActiveVirtualSummaries(summaryRows, contributors, rootFacts, mountChildCounts)
	filterRows := summariseActiveVirtualFilterRows(summaryRows, contributors, rootFilterRows, refreshedAt)
	summariseFillActiveVirtualChildCounts(childRows, summaryRows)

	return catalogRows, summaryRows, filterRows, childRows
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
	catalogRows []summariseActiveVirtualCatalogRow,
	summaryRows []summariseActiveVirtualSummaryRow,
	filterRows []summariseActiveVirtualFilterAllRow,
	childRows []summariseActiveVirtualChildRow,
) error {
	if err := w.writeActiveVirtualCatalogRows(catalogRows); err != nil {
		return err
	}

	if err := w.writeActiveVirtualSummaryRows(summaryRows); err != nil {
		return err
	}

	if err := w.writeActiveVirtualFilterRows(filterRows); err != nil {
		return err
	}

	return w.writeActiveVirtualChildRows(childRows)
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
		ParentID:     record.parentID,
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

func (w *summariseDGUTASpoolWriter) writeActiveVirtualCatalogRows(
	rows []summariseActiveVirtualCatalogRow,
) error {
	for _, row := range rows {
		if err := w.ds.set.WriteActiveVirtualDir(summariseActiveVirtualDirSpoolRow(row)); err != nil {
			return err
		}
	}

	return nil
}

func summariseActiveVirtualDirSpoolRow(row summariseActiveVirtualCatalogRow) chspool.ActiveVirtualDirRow {
	return chspool.ActiveVirtualDirRow{
		ActiveSetID:    row.ActiveSetID,
		VirtualID:      row.VirtualID,
		ParentID:       row.ParentID,
		Name:           row.Name,
		FullPath:       row.FullPath,
		MountPath:      row.MountPath,
		SnapshotID:     row.SnapshotID,
		MountRootDirID: row.MountRootDirID,
		IsMountRootBox: row.IsMountRootBox,
		RefreshedAt:    row.RefreshedAt,
	}
}

func (w *summariseDGUTASpoolWriter) writeActiveVirtualSummaryRows(
	rows []summariseActiveVirtualSummaryRow,
) error {
	for _, row := range rows {
		if err := w.ds.set.WriteActiveVirtualSummary(summariseActiveVirtualSummarySpoolRow(row)); err != nil {
			return err
		}
	}

	return nil
}

func summariseActiveVirtualSummarySpoolRow(row summariseActiveVirtualSummaryRow) chspool.ActiveVirtualSummaryRow {
	return chspool.ActiveVirtualSummaryRow{
		ActiveSetID:     row.ActiveSetID,
		VirtualID:       row.VirtualID,
		MountPath:       row.MountPath,
		SnapshotID:      row.SnapshotID,
		MountRootDirID:  row.MountRootDirID,
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

func (w *summariseDGUTASpoolWriter) writeActiveVirtualFilterRows(
	rows []summariseActiveVirtualFilterAllRow,
) error {
	for _, row := range rows {
		if err := w.ds.set.WriteActiveVirtualFilterAll(summariseActiveVirtualFilterSpoolRow(row)); err != nil {
			return err
		}
	}

	return nil
}

func summariseActiveVirtualFilterSpoolRow(row summariseActiveVirtualFilterAllRow) chspool.ActiveVirtualFilterAllRow {
	return chspool.ActiveVirtualFilterAllRow{
		ActiveSetID:      row.ActiveSetID,
		VirtualID:        row.VirtualID,
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

func (w *summariseDGUTASpoolWriter) writeActiveVirtualChildRows(
	rows []summariseActiveVirtualChildRow,
) error {
	for _, row := range rows {
		if err := w.ds.set.WriteActiveVirtualChild(summariseActiveVirtualChildSpoolRow(row)); err != nil {
			return err
		}
	}

	return nil
}

func summariseActiveVirtualChildSpoolRow(row summariseActiveVirtualChildRow) chspool.ActiveVirtualChildRow {
	return chspool.ActiveVirtualChildRow{
		ActiveSetID:     row.ActiveSetID,
		ParentVirtualID: row.ParentVirtualID,
		ChildVirtualID:  row.ChildVirtualID,
		MountPath:       row.MountPath,
		SnapshotID:      row.SnapshotID,
		MountRootDirID:  row.MountRootDirID,
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
		MountPath:      w.mountPath,
		SnapshotID:     w.snapshotID,
		DirID:          record.dirID,
		ParentID:       record.parentID,
		SubtreeEnd:     record.subtreeEnd,
		Depth:          record.depth,
		Name:           summariseCatalogNameForFullPath(record.canonicalDir),
		FullPath:       summariseEnsureTrailingSlash(record.canonicalDir),
		ChildDirCount:  summariseSafeUint32(dguta.ChildCount),
		ChildFileCount: summariseSafeUint32(dguta.ChildFileCount),
		PathHash:       summariseCatalogPathHash(record.canonicalDir),
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

func (w *summariseDGUTASpoolWriter) noteDirbuildBasedirsDir(record summariseDGUTARecordContext) {
	if w.dirbuildBasedirs != nil {
		w.dirbuildBasedirs.addDir(record)
	}
}

func newSummariseDirbuildBasedirsCreator(
	ds *summariseSpoolDataset,
) (*basedirs.BaseDirs, basedirs.Config, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return nil, nil, err
	}

	mountpoints, err := summariseutil.ParseMountpointsFromFile(mounts)
	if err != nil {
		return nil, nil, err
	}

	store := &summariseBasedirsSpoolStore{ds: ds}
	store.SetMountPath(ds.mountPath)
	store.SetUpdatedAt(ds.updatedAt)

	creator, err := summariseutil.NewBaseDirsCreator(store, quotas, mountpoints, ds.updatedAt)
	if err != nil {
		return nil, nil, err
	}

	return creator, config, nil
}

func runAndCloseSummariseSpoolDirbuild(
	statsPath string,
	partialDir string,
	ds *summariseSpoolDataset,
	dw *summariseDGUTASpoolWriter,
	fileWriter *summariseFileSpoolOperation,
	diag *summariseDiagnostics,
) (uint64, error) {
	diag.setCurrentPhase("parse")

	records, err := runSummariseSpoolDirbuild(statsPath, partialDir, ds, dw, fileWriter)
	diag.logParseResult(records, err)

	if err == nil {
		err = fileWriter.outputDirbuildBasedirs()
	}

	if err == nil {
		err = closeSummariseSpoolOperations(ds)
	}

	return records, err
}

func buildSummariseSpoolWithDirbuild(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*chspool.Manifest, uint64, uint64, error) {
	set, ds, err := createSummariseSpoolPartial(partialDir, expected, target)
	if err != nil {
		return nil, 0, 0, err
	}

	dw, fileWriter, err := newSummariseDirbuildSpoolWriters(ds)
	if err != nil {
		manifest, scratchBytes, finishErr := finishSummariseSpoolPartial(partialDir, expected, set, err)

		return manifest, 0, scratchBytes, finishErr
	}

	records, err := runAndCloseSummariseSpoolDirbuild(statsPath, partialDir, ds, dw, fileWriter, diag)
	manifest, scratchBytes, err := finishSummariseSpoolPartial(partialDir, expected, set, err)

	return manifest, records, scratchBytes, err
}

func createSummariseSpoolPartial(
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
) (*chspool.Set, *summariseSpoolDataset, error) {
	if err := os.RemoveAll(partialDir); err != nil {
		return nil, nil, err
	}

	set, err := chspool.CreateSet(partialDir)
	if err != nil {
		return nil, nil, err
	}

	ds, err := newSummariseSpoolDataset(set, expected, target)
	if err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, nil, err
	}

	return set, ds, nil
}

func newSummariseSpoolDataset(
	set *chspool.Set,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
) (*summariseSpoolDataset, error) {
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
		return nil, mountErr
	}

	return ds, nil
}

func finishSummariseSpoolPartial(
	partialDir string,
	expected chspool.Manifest,
	set *chspool.Set,
	err error,
) (*chspool.Manifest, uint64, error) {
	closeErr := set.Close()
	if err != nil || closeErr != nil {
		scratchBytes, cleanupErr := cleanupSummariseSpoolPartial(partialDir, nil, errors.Join(err, closeErr))

		return nil, scratchBytes, cleanupErr
	}

	manifest := summariseCompletedSpoolManifest(expected, set)

	writeErr := chspool.WriteManifestAtomic(partialDir, &manifest)
	if writeErr != nil {
		scratchBytes, cleanupErr := cleanupSummariseSpoolPartial(partialDir, &manifest, writeErr)

		return nil, scratchBytes, cleanupErr
	}

	scratchBytes, err := finishSummariseSpoolScratch(partialDir, &manifest)
	if err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, scratchBytes, err
	}

	return &manifest, scratchBytes, nil
}

func runSummariseSpoolDirbuild(
	statsPath string,
	partialDir string,
	ds *summariseSpoolDataset,
	dw *summariseDGUTASpoolWriter,
	fileWriter *summariseFileSpoolOperation,
) (uint64, error) {
	open := func() (io.ReadCloser, error) {
		return openSummariseSpoolStats(statsPath)
	}

	var records uint64

	err := buildSummariseSpoolDirbuild(
		open,
		ds.mountPath,
		dw,
		ds.dirgutaReferenceAt,
		func(dirID uint32, info summary.FileInfo) error {
			records++

			return fileWriter.addWithDirID(&info, dirID)
		},
		summariseSpoolDirbuildOptions(partialDir),
	)

	return records, err
}

func cleanupSummariseSpoolPartial(partialDir string, manifest *chspool.Manifest, err error) (uint64, error) {
	scratchBytes, scratchErr := summariseBuildPhaseBytesWritten(partialDir, manifest)
	_ = os.RemoveAll(partialDir)

	return scratchBytes, errors.Join(err, scratchErr)
}

func summariseBuildPhaseBytesWritten(partialDir string, manifest *chspool.Manifest) (uint64, error) {
	canonical := summariseBuildCanonicalArtifactPaths(manifest)

	var total uint64

	err := filepath.WalkDir(partialDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		size, err := summariseBuildPhaseFileBytes(partialDir, path, entry, canonical)
		if err != nil {
			return err
		}

		total += size

		return nil
	})

	return total, err
}

func summariseBuildCanonicalArtifactPaths(manifest *chspool.Manifest) map[string]bool {
	paths := map[string]bool{
		chspool.ManifestName:                        true,
		chspool.ManifestName + ".partial":           true,
		clickHouseSpoolBuildReportName:              true,
		clickHouseSpoolBuildReportName + ".partial": true,
		clickHouseSpoolLoadReportName:               true,
		clickHouseSpoolLoadReportName + ".partial":  true,
	}

	for _, table := range chspool.TableOrder() {
		paths[table+".gob.gz"] = true
	}

	if manifest != nil {
		for _, table := range manifest.Tables {
			paths[filepath.Clean(table.Path)] = true
		}
	}

	return paths
}

func summariseBuildPhaseFileBytes(
	partialDir string,
	path string,
	entry fs.DirEntry,
	canonical map[string]bool,
) (uint64, error) {
	size, ok, err := summariseBuildPhasePositiveFileSize(entry)
	if err != nil || !ok {
		return 0, err
	}

	rel, err := filepath.Rel(partialDir, path)
	if err != nil {
		return 0, err
	}

	if canonical[filepath.Clean(rel)] {
		return 0, nil
	}

	return size, nil
}

func summariseBuildPhasePositiveFileSize(entry fs.DirEntry) (uint64, bool, error) {
	if entry.IsDir() {
		return 0, false, nil
	}

	info, err := entry.Info()
	if err != nil {
		return 0, false, err
	}

	size := info.Size()
	if !info.Mode().IsRegular() || size <= 0 {
		return 0, false, nil
	}

	return uint64(size), true, nil
}

func summariseCompletedSpoolManifest(expected chspool.Manifest, set *chspool.Set) chspool.Manifest {
	manifest := expected
	manifest.Tables = set.TableManifests()
	manifest.CompletedAt = summariseSpoolNow().UTC().Format(time.RFC3339Nano)

	return manifest
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

func buildSummariseSpoolAttempt(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*chspool.Manifest, uint64, uint64, error) {
	set, ds, err := createSummariseSpoolPartial(partialDir, expected, target)
	if err != nil {
		return nil, 0, 0, err
	}

	records, err := parseSummariseToSpool(statsPath, ds, diag)
	manifest, scratchBytes, err := finishSummariseSpoolPartial(partialDir, expected, set, err)

	return manifest, records, scratchBytes, err
}

func newSummariseDirbuildSpoolWriters(
	ds *summariseSpoolDataset,
) (*summariseDGUTASpoolWriter, *summariseFileSpoolOperation, error) {
	dw := newSummariseDGUTASpoolWriter(ds)

	basedirsBuilder, err := newSummariseDirbuildBasedirsBuilder(ds)
	if err != nil {
		return nil, nil, err
	}

	dw.dirbuildBasedirs = basedirsBuilder

	return dw, &summariseFileSpoolOperation{ds: ds, dirbuildBasedirs: basedirsBuilder}, nil
}

type summariseFileSpoolOperation struct {
	ds               *summariseSpoolDataset
	dirbuildBasedirs *summariseDirbuildBasedirsBuilder
}

type summariseDGUTASpoolWriter struct {
	ds                            *summariseSpoolDataset
	dirbuildBasedirs              *summariseDirbuildBasedirsBuilder
	mountPath                     string
	updatedAt                     time.Time
	snapshotID                    string
	refreshedAt                   time.Time
	previousDGUTARows             summariseDGUTARecordRows
	fullFilterPending             []summariseFullFilterPendingDir
	fullFilterFutureChildren      map[string]summariseFullFilterDirectChildCounts
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

func (f *summariseFileSpoolOperation) addWithDirID(info *summary.FileInfo, dirID uint32) error { //nolint:funlen
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

	if err := f.ds.set.WriteFile(chspool.FileRow{
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
	}); err != nil {
		return err
	}

	if f.dirbuildBasedirs != nil {
		return f.dirbuildBasedirs.addFile(dirID, info, size)
	}

	return nil
}

func (f *summariseFileSpoolOperation) outputDirbuildBasedirs() error {
	if f.dirbuildBasedirs == nil {
		return nil
	}

	return f.dirbuildBasedirs.output()
}

func summariseFileIngestDirIDPath(info *summary.FileInfo) *summary.DirectoryPath {
	if info.IsDir() {
		if info.Path.Parent == nil {
			return info.Path
		}

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

func summariseActiveVirtualFilterSortKey(row summariseActiveVirtualFilterAllRow) string {
	prefix := row.Dir
	if row.VirtualID != 0 {
		prefix = fmt.Sprintf("%010d", row.VirtualID)
	}

	return fmt.Sprintf("%s\x00%03d\x00%010d\x00%010d\x00%05d", prefix, row.Age, row.GID, row.UID, row.FT)
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

type summariseSpoolBuildResult struct {
	manifest                 *chspool.Manifest
	inputShape               string
	buildPath                string
	records                  uint64
	scratchBytes             uint64
	contiguityViolationRow   uint64
	contiguityViolationDepth uint64
	err                      error
}

func runSummariseSpoolBuild(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) summariseSpoolBuildResult {
	dirbuildFirstReason, probe, err := summariseSpoolShouldUseDirbuildFirst(statsPath, target.mountPath, diag)
	if err != nil {
		return summariseSpoolBuildResult{err: err}
	}

	if dirbuildFirstReason != summariseSpoolDirbuildFirstNone {
		return runSummariseSpoolDirbuildFirst(
			statsPath, partialDir, expected, target, diag, dirbuildFirstReason, probe,
		)
	}

	manifest, records, scratchBytes, err := buildSummariseSpoolAttempt(statsPath, partialDir, expected, target, diag)
	if !errors.Is(err, summary.ErrNonContiguousInput) {
		return summariseSpoolContiguousFastResult(manifest, records, scratchBytes, err)
	}

	return handleSummariseSpoolFastPathViolation(
		statsPath, partialDir, expected, target, diag, records, scratchBytes,
	)
}

func runSummariseSpoolDirbuildFirst(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
	reason summariseSpoolDirbuildFirstReason,
	probe summariseSpoolContiguityResult,
) summariseSpoolBuildResult {
	manifest, records, scratchBytes, err := buildSummariseSpoolWithDirbuild(
		statsPath, partialDir, expected, target, diag,
	)

	result := summariseSpoolDirbuildResult(
		manifest,
		summariseSpoolDirbuildFirstInputShape(reason),
		records,
		scratchBytes,
		err,
	)
	result.contiguityViolationRow = probe.violationRow
	result.contiguityViolationDepth = probe.violationPathDepth

	return result
}

func retrySummariseSpoolWithDirbuild(
	statsPath string,
	partialDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
	firstAttemptScratchBytes uint64,
) summariseSpoolBuildResult {
	manifest, records, scratchBytes, err := buildSummariseSpoolWithDirbuild(
		statsPath, partialDir, expected, target, diag,
	)
	result := summariseSpoolDirbuildResult(
		manifest,
		chperf.A5BuildInputNonContiguous,
		records,
		scratchBytes,
		err,
	)
	result.scratchBytes += firstAttemptScratchBytes

	return result
}

func summariseSpoolContiguousFastResult(
	manifest *chspool.Manifest,
	records uint64,
	scratchBytes uint64,
	err error,
) summariseSpoolBuildResult {
	return summariseSpoolBuildResult{
		manifest:     manifest,
		inputShape:   chperf.A5BuildInputContiguous,
		buildPath:    chperf.A5BuildPathContiguousFast,
		records:      records,
		scratchBytes: scratchBytes,
		err:          err,
	}
}

func summariseSpoolDirbuildResult(
	manifest *chspool.Manifest,
	inputShape string,
	records uint64,
	scratchBytes uint64,
	err error,
) summariseSpoolBuildResult {
	return summariseSpoolBuildResult{
		manifest:     manifest,
		inputShape:   inputShape,
		buildPath:    chperf.A5BuildPathDirbuild,
		records:      records,
		scratchBytes: scratchBytes,
		err:          err,
	}
}

func summariseDirbuildBasedirsAddSubdirFile(
	child *basedirs.SubDir,
	info *summary.FileInfo,
	size uint64,
	tempDir bool,
) {
	child.NumFiles++
	child.SizeFiles += size

	mtime := time.Unix(info.MTime, 0)
	if mtime.After(child.LastModified) {
		child.LastModified = mtime
	}

	ft := dirguta.FilenameToType(info.Name)

	child.FileUsage[ft] += size
	if tempDir || dirguta.IsTemp(info.Name) {
		child.FileUsage[db.DGUTAFileTypeTemp] += size
	}
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

func buildSummariseSpool( //nolint:funlen
	statsPath string,
	spoolDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*chspool.Manifest, error) {
	partialDir := spoolDir + ".partial"
	started := time.Now()

	build := runSummariseSpoolBuild(statsPath, partialDir, expected, target, diag)
	if build.err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, build.err
	}

	report := summariseBuildReport(
		statsPath,
		target,
		build.manifest,
		build.inputShape,
		build.buildPath,
		build.records,
		build.scratchBytes,
		build.contiguityViolationRow,
		build.contiguityViolationDepth,
		time.Since(started),
	)
	if err := writeSummariseSpoolBuildReport(partialDir, report); err != nil {
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

	return build.manifest, nil
}

func parseSummariseToSpool( //nolint:funlen
	statsPath string,
	ds *summariseSpoolDataset,
	diag *summariseDiagnostics,
) (_ uint64, err error) {
	r, err := openSummariseSpoolStats(statsPath)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	s := summary.NewSummariser(stats.NewStatsParser(r))
	parseCounter := addSummariseParseCounter(s)
	setSummariseProgress(s, diag)
	addSummariseSpoolOperations(s, ds)

	if addErr := addSummariseSpoolBasedirs(s, ds); addErr != nil {
		return parseCounter.Count(), addErr
	}

	if addErr := addOutputSummarisers(s); addErr != nil {
		return parseCounter.Count(), addErr
	}

	diag.setCurrentPhase("parse")

	err = s.Summarise()
	if !errors.Is(err, summary.ErrNonContiguousInput) {
		diag.logParseResult(parseCounter.Count(), err)
	}

	if err != nil {
		return parseCounter.Count(), err
	}

	return parseCounter.Count(), closeSummariseSpoolOperations(ds)
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

func (f *summariseFileSpoolOperation) Add(info *summary.FileInfo) error {
	if info == nil {
		return nil
	}

	dirID, err := f.ds.idAllocator.DirID(summariseFileIngestDirIDPath(info))
	if err != nil {
		return err
	}

	return f.addWithDirID(info, dirID)
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

func summariseBuildReport(
	statsPath string,
	target *clickHouseSummariseTarget,
	manifest *chspool.Manifest,
	inputShape string,
	buildPath string,
	records uint64,
	buildScratchBytes uint64,
	contiguityViolationRow uint64,
	contiguityViolationDepth uint64,
	elapsed time.Duration,
) perfreport.Report {
	report := perfreport.NewReport("clickhouse_summarise_build", statsPath, 1, 0)
	report.MaxRSSBytes = summariseBuildMaxRSSBytes()
	inputs := summariseBuildReportInputs(
		target, manifest, inputShape, buildPath, records, buildScratchBytes,
		contiguityViolationRow, contiguityViolationDepth,
	)
	inputs["stats_path"] = statsPath
	report.AddOperation(
		chperf.A5BuildOperationName,
		inputs,
		[]float64{float64(elapsed) / float64(time.Millisecond)},
	)

	return report
}

func summariseBuildMaxRSSBytes() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}

	if usage.Maxrss <= 0 {
		return 0
	}

	return uint64(usage.Maxrss) * summariseBuildBytesPerKiB
}

func summariseBuildA5Role(mountPath string) string {
	mountPath = summariseEnsureTrailingSlash(mountPath)
	switch mountPath {
	case "/lustre/scratch127/":
		return chperf.A5BuildRoleScratch127
	case "/nfs/t283_imaging/":
		return chperf.A5BuildRoleT283
	default:
		if strings.HasPrefix(mountPath, "/lustre/") {
			return chperf.A5BuildRoleHealthyLustre
		}

		return "other"
	}
}

func summariseBuildManifestBytes(manifest *chspool.Manifest) uint64 {
	if manifest == nil {
		return 0
	}

	var total uint64

	for _, table := range manifest.Tables {
		if table.Bytes > 0 {
			total += uint64(table.Bytes)
		}
	}

	return total
}

func writeSummariseSpoolBuildReport(spoolDir string, report perfreport.Report) error {
	return perfreport.WriteReport(summariseSpoolBuildReportPath(spoolDir), report)
}

func summariseSpoolBuildReportPath(spoolDir string) string {
	return filepath.Join(spoolDir, clickHouseSpoolBuildReportName)
}

func summariseSpoolShouldUseDirbuildFirst(
	statsPath string,
	mountPath string,
	diag *summariseDiagnostics,
) (summariseSpoolDirbuildFirstReason, summariseSpoolContiguityResult, error) {
	if !summariseSpoolShouldProbeBeforeFastPath(statsPath) {
		return summariseSpoolDirbuildFirstNone, summariseSpoolContiguityResult{}, nil
	}

	diag.setCurrentPhase("parse")

	probe, err := summariseSpoolProbeContiguity(statsPath, mountPath)
	if err != nil {
		return summariseSpoolDirbuildFirstNone, probe, err
	}

	if !probe.contiguous {
		return summariseSpoolDirbuildFirstNonContiguous, probe, nil
	}

	return summariseSpoolDirbuildFirstNone, probe, nil
}

func summariseSpoolShouldProbeBeforeFastPath(statsPath string) bool {
	return statsPath != "-"
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

	w.noteDirbuildBasedirsDir(record)

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

func summariseDirbuildBasedirsAddOutputSummary(
	out basedirs.IDAgeDirs,
	id uint32,
	age db.DirGUTAge,
	summary *basedirs.SummaryWithChildren,
) {
	if summary == nil {
		return
	}

	next := summariseDirbuildBasedirsCloneSummary(summary)
	ageDirs := out.Get(id)
	kept := ageDirs[age][:0]

	for _, existing := range ageDirs[age] {
		if strings.HasPrefix(existing.Dir, next.Dir) {
			summariseDirbuildBasedirsMergeSummary(&next, &existing, next.Dir)

			continue
		}

		kept = append(kept, existing)
	}

	ageDirs[age] = append(kept, next)
}

func summariseDirbuildBasedirsCloneSummary(
	summary *basedirs.SummaryWithChildren,
) basedirs.SummaryWithChildren {
	out := *summary
	out.UIDs = slices.Clone(summary.UIDs)
	out.GIDs = slices.Clone(summary.GIDs)
	out.Children = make([]*basedirs.SubDir, 0, len(summary.Children))

	for _, child := range summary.Children {
		out.Children = append(out.Children, summariseDirbuildBasedirsCloneSubdir(child))
	}

	return out
}

func summariseDirbuildBasedirsCloneSubdir(in *basedirs.SubDir) *basedirs.SubDir {
	if in == nil {
		return nil
	}

	out := *in

	out.FileUsage = make(basedirs.UsageBreakdownByType, len(in.FileUsage))
	for ft, size := range in.FileUsage {
		out.FileUsage[ft] = size
	}

	return &out
}

func summariseDirbuildBasedirsNormalizeSummary(summary *basedirs.SummaryWithChildren) {
	dot := summariseDirbuildBasedirsDot(summary)
	dot.SubDir = "."
	summary.Count = dot.NumFiles
	summary.Size = dot.SizeFiles
	summary.Dir = strings.TrimSuffix(summary.Dir, "/")
	summary.UIDs = summariseSortedUint32Slice(summary.UIDs)
	summary.GIDs = summariseSortedUint32Slice(summary.GIDs)

	for ft, size := range dot.FileUsage {
		if size > 0 {
			summary.FT |= ft
		}
	}

	summariseDirbuildBasedirsRemoveChildFilesFromDot(summary)
	summariseDirbuildBasedirsCleanDot(summary)
}

func summariseDirbuildBasedirsRemoveChildFilesFromDot(summary *basedirs.SummaryWithChildren) {
	dot := summariseDirbuildBasedirsDot(summary)

	for _, child := range summary.Children[1:] {
		if child == nil {
			continue
		}

		child.SubDir = strings.TrimSuffix(child.SubDir, "/")
		dot.NumFiles -= child.NumFiles
		dot.SizeFiles -= child.SizeFiles

		for ft, size := range child.FileUsage {
			dot.FileUsage[ft] -= size
		}
	}
}

func summariseDirbuildBasedirsCleanDot(summary *basedirs.SummaryWithChildren) {
	dot := summariseDirbuildBasedirsDot(summary)
	if dot.NumFiles == 0 {
		summary.Children = summary.Children[1:]

		return
	}

	for ft, size := range dot.FileUsage {
		if size == 0 {
			delete(dot.FileUsage, ft)
		}
	}
}
