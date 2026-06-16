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

package clickhouse

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var errDirProjectionBatchNotPrepared = errors.New("clickhouse: dir projection batch is not prepared")

var zeroSummaryAgeBuckets summary.AgeBuckets //nolint:gochecknoglobals

type mountDirRecordSummary struct {
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

func mountDirRecordSummaries(gutas db.GUTAs) (*mountDirRecordSummary, *mountDirRecordSummary) {
	var allSummary, fileSummary *mountDirRecordSummary

	for _, guta := range gutas {
		allSummary, fileSummary = addAgeAllMountDirRecordSummary(allSummary, fileSummary, guta)
	}

	return allSummary, fileSummary
}

func addAgeAllMountDirRecordSummary(
	allSummary *mountDirRecordSummary,
	fileSummary *mountDirRecordSummary,
	guta *db.GUTA,
) (*mountDirRecordSummary, *mountDirRecordSummary) {
	if guta == nil || guta.Age != db.DGUTAgeAll {
		return allSummary, fileSummary
	}

	if allSummary == nil {
		allSummary = newMountDirRecordSummary()
	}

	allSummary.add(guta)

	if guta.FT&db.AllTypesExceptDirectories == 0 {
		return allSummary, fileSummary
	}

	if fileSummary == nil {
		fileSummary = newMountDirRecordSummary()
	}

	fileSummary.addFile(guta)

	return allSummary, fileSummary
}

func newMountDirRecordSummary() *mountDirRecordSummary {
	return &mountDirRecordSummary{}
}

func (s *mountDirRecordSummary) add(guta *db.GUTA) {
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

	s.uids = appendUniqueUint32(s.uids, guta.UID)
	s.gids = appendUniqueUint32(s.gids, guta.GID)
	s.ft |= guta.FT
}

func appendUniqueUint32(values []uint32, value uint32) []uint32 {
	if slices.Contains(values, value) {
		return values
	}

	return append(values, value)
}

func (s *mountDirRecordSummary) addFile(guta *db.GUTA) {
	s.add(guta)
	s.ft &= db.AllTypesExceptDirectories
}

func (s *mountDirRecordSummary) sortedUIDs() []uint32 {
	return sortedUint32Slice(s.uids)
}

func sortedUint32Slice(values []uint32) []uint32 {
	slices.Sort(values)

	return values
}

func (s *mountDirRecordSummary) sortedGIDs() []uint32 {
	return sortedUint32Slice(s.gids)
}

func appendMountDirFactRecordValues(
	batch driver.Batch,
	mount activeMount,
	record dgutaRecordContext,
	allSummary *mountDirRecordSummary,
	fileSummary *mountDirRecordSummary,
	columns mountDirProjectionVectorColumns,
	childCount uint64,
	refreshedAt time.Time,
) error {
	err := batch.Append(
		mount.mountPath, mount.snapshotID, record.dirID, record.parentID, record.subtreeEnd, mount.updatedAt,
		allSummary.count, allSummary.size, allSummary.atimeMin, allSummary.mtimeMax,
		ageBucketsSlice(&allSummary.atimeBuckets), ageBucketsSlice(&allSummary.mtimeBuckets),
		allSummary.sortedUIDs(), allSummary.sortedGIDs(), uint16(allSummary.ft),
		recordSummaryCount(fileSummary), recordSummarySize(fileSummary),
		recordSummaryAtimeMin(fileSummary), recordSummaryMtimeMax(fileSummary),
		recordSummaryATimeBuckets(fileSummary), recordSummaryMTimeBuckets(fileSummary),
		recordSummaryUIDs(fileSummary), recordSummaryGIDs(fileSummary), recordSummaryFT(fileSummary),
		columns.gids, columns.uids, columns.fts, columns.ages, columns.counts, columns.sizes,
		columns.atimeMins, columns.mtimeMaxs, columns.atimeBuckets, columns.mtimeBuckets,
		childCount, refreshedAt,
	)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to append dir facts row: %w", err)
	}

	return nil
}

func recordSummaryCount(summary *mountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.count
}

func recordSummarySize(summary *mountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.size
}

func recordSummaryAtimeMin(summary *mountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.atimeMin
}

func recordSummaryMtimeMax(summary *mountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.mtimeMax
}

func recordSummaryATimeBuckets(summary *mountDirRecordSummary) []uint64 {
	if summary == nil {
		return ageBucketsSlice(nil)
	}

	return ageBucketsSlice(&summary.atimeBuckets)
}

func recordSummaryMTimeBuckets(summary *mountDirRecordSummary) []uint64 {
	if summary == nil {
		return ageBucketsSlice(nil)
	}

	return ageBucketsSlice(&summary.mtimeBuckets)
}

func recordSummaryUIDs(summary *mountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedUIDs()
}

func recordSummaryGIDs(summary *mountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedGIDs()
}

func recordSummaryFT(summary *mountDirRecordSummary) uint16 {
	if summary == nil {
		return 0
	}

	return uint16(summary.ft)
}

func summaryAccumulatorOrZero(acc *mountDirSummaryAccumulator) *mountDirSummaryAccumulator {
	if acc != nil {
		return acc
	}

	return newMountDirSummaryAccumulator(db.DGUTAgeAll)
}

func mountDirProjectionVectorValues(gutas db.GUTAs) []any {
	columns := mountDirProjectionVectorColumnsFor(gutas)

	return []any{
		columns.gids,
		columns.uids,
		columns.fts,
		columns.ages,
		columns.counts,
		columns.sizes,
		columns.atimeMins,
		columns.mtimeMaxs,
		columns.atimeBuckets,
		columns.mtimeBuckets,
	}
}

func (w *mountDirProjectionWriter) appendFactRowWithContext(
	ctx context.Context,
	mount activeMount,
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
	batchSize int,
) error {
	blockWriter := w.blockWriter(
		&w.summaryBatch,
		&w.summaryOpenedAt,
		insertMountDirSummaryQuery,
		"dir facts",
		batchSize,
	)

	return blockWriter.append(ctx, func(batch driver.Batch) error {
		return appendMountDirFactRowValuesForRecord(batch, mount, record, gutas, childCount, w.refreshedAt)
	})
}

func appendMountDirFactRowValuesForRecord(
	batch driver.Batch,
	mount activeMount,
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
	refreshedAt time.Time,
) error {
	allSummary, fileSummary := mountDirRecordSummaries(gutas)
	columns := mountDirProjectionVectorColumnsFor(gutas)

	if allSummary == nil {
		allSummary = newMountDirRecordSummary()
	}

	return appendMountDirFactRecordValues(
		batch,
		mount,
		record,
		allSummary,
		fileSummary,
		columns,
		childCount,
		refreshedAt,
	)
}

func (a *mountDirSummaryAccumulator) addFile(guta *db.GUTA) {
	a.add(guta)
	a.ft &= db.AllTypesExceptDirectories
}

func (s *mountDirProjectionState) summaryAccumulatorForDir(
	accumulators map[string]*mountDirSummaryAccumulator,
	dir string,
) *mountDirSummaryAccumulator {
	acc := accumulators[dir]
	if acc != nil {
		return acc
	}

	acc = newMountDirSummaryAccumulator(db.DGUTAgeAll)
	accumulators[dir] = acc

	return acc
}

func (w *mountDirProjectionWriter) appendRecordWithContext(
	ctx context.Context,
	mount activeMount,
	record dgutaRecordContext,
	gutas db.GUTAs,
	childCount uint64,
	_ []db.DirGUTAge,
	_ bool,
	batchSize int,
) error {
	return w.appendFactRowWithContext(ctx, mount, record, gutas, childCount, batchSize)
}

func (w *mountDirProjectionWriter) blockWriter(
	batch *driver.Batch,
	openedAt *time.Time,
	query, name string,
	batchSize int,
) *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       query,
		name:        name,
		batch:       batch,
		openedAt:    openedAt,
		writeErr:    &w.writeErr,
		batchSize:   batchSize,
		notPrepared: fmt.Errorf("%w: %s", errDirProjectionBatchNotPrepared, name),
		now:         w.importBatchNow,
	}
}

func compareProjectionGUTAs(a, b *db.GUTA) int {
	if diff, ok := compareNilProjectionGUTAs(a, b); ok {
		return diff
	}

	return compareProjectionGUTAValues(a, b)
}

type mountDirSummaryKey struct {
	dir string
	age db.DirGUTAge
}

func abortProjectionBatch(batch driver.Batch, name string) error {
	if err := batch.Abort(); err != nil {
		return fmt.Errorf("clickhouse: failed to abort %s batch: %w", name, err)
	}

	return nil
}

func sendOrAbortProjectionBatch(batch driver.Batch, name string) error {
	if batch.Rows() == 0 {
		if err := batch.Abort(); err != nil {
			return fmt.Errorf("clickhouse: failed to abort empty %s batch: %w", name, err)
		}

		return nil
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to send %s batch: %w", name, err)
	}

	return nil
}

func summaryCount(acc *mountDirSummaryAccumulator) uint64 {
	if acc == nil {
		return 0
	}

	return acc.count
}

func summarySize(acc *mountDirSummaryAccumulator) uint64 {
	if acc == nil {
		return 0
	}

	return acc.size
}

func ageBucketsSlice(buckets *summary.AgeBuckets) []uint64 {
	if buckets == nil {
		return zeroSummaryAgeBuckets[:]
	}

	return buckets[:]
}

func sortedUint32Set(set map[uint32]struct{}) []uint32 {
	out := make([]uint32, 0, len(set))
	for value := range set {
		out = append(out, value)
	}

	slices.Sort(out)

	return out
}

type mountDirProjectionWriter struct {
	conn            ch.Conn
	summaryBatch    driver.Batch
	summaryOpenedAt time.Time
	batchNow        func() time.Time
	summaryFlushed  bool
	refreshedAt     time.Time
	writeErr        error
}

func prepareMountDirProjectionWriter(
	_ context.Context,
	conn ch.Conn,
) mountDirProjectionWriter {
	writer := mountDirProjectionWriter{refreshedAt: time.Now().UTC()}
	writer.prepare(conn)

	return writer
}

func (w *mountDirProjectionWriter) prepare(conn ch.Conn) {
	w.conn = conn
	if w.refreshedAt.IsZero() {
		w.refreshedAt = w.importBatchNow().UTC()
	}
}

func (w *mountDirProjectionWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}

func (w *mountDirProjectionWriter) sendFullBatchIfFull(
	batch *driver.Batch,
	openedAt *time.Time,
	batchSize int,
	name string,
) error {
	return w.blockWriter(batch, openedAt, "", name, batchSize).sendIfFull()
}

func (w *mountDirProjectionWriter) abortAll() error {
	return abortBatch(&w.summaryBatch, "dir facts")
}

type mountDirSummaryAccumulator struct {
	age          db.DirGUTAge
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets summary.AgeBuckets
	mtimeBuckets summary.AgeBuckets
	uids         map[uint32]struct{}
	gids         map[uint32]struct{}
	ft           db.DirGUTAFileType
}

func newMountDirSummaryAccumulator(age db.DirGUTAge) *mountDirSummaryAccumulator {
	return &mountDirSummaryAccumulator{
		age:  age,
		uids: make(map[uint32]struct{}),
		gids: make(map[uint32]struct{}),
	}
}

func (a *mountDirSummaryAccumulator) add(guta *db.GUTA) {
	a.count += guta.Count
	a.size += guta.Size

	if guta.Atime != 0 && (a.atimeMin == 0 || guta.Atime < a.atimeMin) {
		a.atimeMin = guta.Atime
	}

	if guta.Mtime > a.mtimeMax {
		a.mtimeMax = guta.Mtime
	}

	for i, count := range guta.ATimeRanges {
		a.atimeBuckets[i] += count
	}

	for i, count := range guta.MTimeRanges {
		a.mtimeBuckets[i] += count
	}

	a.uids[guta.UID] = struct{}{}
	a.gids[guta.GID] = struct{}{}
	a.ft |= guta.FT
}

func (a *mountDirSummaryAccumulator) addScanned(s treeDirSummaryScanned) {
	a.count += s.count
	a.size += s.size
	a.addScannedTimes(s)
	addAgeBucketSlice(&a.atimeBuckets, s.atimeBuckets)
	addAgeBucketSlice(&a.mtimeBuckets, s.mtimeBuckets)
	addUint32Slice(a.uids, s.uids)
	addUint32Slice(a.gids, s.gids)
	a.ft |= db.DirGUTAFileType(s.ft)
}

func addAgeBucketSlice(dst *summary.AgeBuckets, src []uint64) {
	for i, count := range src {
		if i >= len(dst) {
			return
		}

		dst[i] += count
	}
}

func addUint32Slice(dst map[uint32]struct{}, src []uint32) {
	for _, value := range src {
		dst[value] = struct{}{}
	}
}

func (a *mountDirSummaryAccumulator) addScannedTimes(s treeDirSummaryScanned) {
	if s.atimeMin != 0 && (a.atimeMin == 0 || s.atimeMin < a.atimeMin) {
		a.atimeMin = s.atimeMin
	}

	if s.mtimeMax > a.mtimeMax {
		a.mtimeMax = s.mtimeMax
	}
}

func (a *mountDirSummaryAccumulator) summary(updatedAt time.Time) *db.DirSummary {
	if a == nil || a.count == 0 {
		return nil
	}

	return &db.DirSummary{
		Count:       a.count,
		Size:        a.size,
		Atime:       time.Unix(a.atimeMin, 0),
		CommonATime: summary.MostCommonBucket(a.atimeBuckets),
		Mtime:       time.Unix(a.mtimeMax, 0),
		CommonMTime: summary.MostCommonBucket(a.mtimeBuckets),
		UIDs:        sortedUint32Set(a.uids),
		GIDs:        sortedUint32Set(a.gids),
		FT:          a.ft,
		Age:         a.age,
		Modtime:     updatedAt.UTC(),
	}
}

type mountDirProjectionState struct {
	dirs             map[string]struct{}
	allSummaries     map[string]*mountDirSummaryAccumulator
	allFileSummaries map[string]*mountDirSummaryAccumulator
	summaries        map[mountDirSummaryKey]*mountDirSummaryAccumulator
	fileSummaries    map[mountDirSummaryKey]*mountDirSummaryAccumulator
	vectors          map[string]db.GUTAs
	childCounts      map[string]uint64
}

func newMountDirProjectionState() mountDirProjectionState {
	return mountDirProjectionState{
		dirs:             make(map[string]struct{}),
		allSummaries:     make(map[string]*mountDirSummaryAccumulator),
		allFileSummaries: make(map[string]*mountDirSummaryAccumulator),
		summaries:        make(map[mountDirSummaryKey]*mountDirSummaryAccumulator),
		fileSummaries:    make(map[mountDirSummaryKey]*mountDirSummaryAccumulator),
		vectors:          make(map[string]db.GUTAs),
		childCounts:      make(map[string]uint64),
	}
}

func (s *mountDirProjectionState) addGUTAs(dir string, gutas db.GUTAs) {
	if len(gutas) == 0 {
		return
	}

	s.ensure()

	for _, guta := range gutas {
		s.addGUTA(dir, guta)
	}
}

func (s *mountDirProjectionState) addGUTA(dir string, guta *db.GUTA) {
	if guta == nil {
		return
	}

	s.summaryAccumulatorForDir(s.allSummaries, dir).add(guta)

	key := mountDirSummaryKey{dir: dir, age: guta.Age}
	s.summaryAccumulator(s.summaries, key).add(guta)

	if guta.FT&db.AllTypesExceptDirectories > 0 {
		s.summaryAccumulatorForDir(s.allFileSummaries, dir).addFile(guta)
		s.summaryAccumulator(s.fileSummaries, key).addFile(guta)
	}

	s.vectors[dir] = append(s.vectors[dir], guta)
}

func (s *mountDirProjectionState) addChildren(dir string, count uint64) {
	if count == 0 {
		return
	}

	s.ensure()
	s.childCounts[dir] += count
}

func (s *mountDirProjectionState) ensure() {
	if s.summaries == nil {
		*s = newMountDirProjectionState()
	}
}

func (s *mountDirProjectionState) summaryAccumulator(
	accumulators map[mountDirSummaryKey]*mountDirSummaryAccumulator,
	key mountDirSummaryKey,
) *mountDirSummaryAccumulator {
	acc := accumulators[key]
	if acc != nil {
		return acc
	}

	acc = newMountDirSummaryAccumulator(key.age)
	accumulators[key] = acc

	return acc
}

func writeMountDirSummarySetRow(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	refreshedAt time.Time,
) error {
	batch, err := prepareImportBatch(ctx, conn, insertMountDirSummarySetQuery)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare dir summary set batch: %w", err)
	}

	if err := batch.Append(
		mount.mountPath,
		mount.snapshotID,
		mount.updatedAt,
		refreshedAt,
	); err != nil {
		return errors.Join(
			fmt.Errorf("clickhouse: failed to append dir summary set row: %w", err),
			abortProjectionBatch(batch, "dir summary set"),
		)
	}

	return sendOrAbortProjectionBatch(batch, "dir summary set")
}

func mountDirProjectionVectorColumnsFor(gutas db.GUTAs) mountDirProjectionVectorColumns {
	slices.SortFunc(gutas, compareProjectionGUTAs)

	columns := mountDirProjectionVectorColumns{
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

		columns.append(guta)
	}

	return columns
}

type mountDirProjectionVectorColumns struct {
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

func (c *mountDirProjectionVectorColumns) append(guta *db.GUTA) {
	c.appendIDs(guta)
	c.appendStats(guta)
	c.appendBuckets(guta)
}

func (c *mountDirProjectionVectorColumns) appendIDs(guta *db.GUTA) {
	c.gids = append(c.gids, guta.GID)
	c.uids = append(c.uids, guta.UID)
	c.fts = append(c.fts, uint16(guta.FT))
	c.ages = append(c.ages, uint8(guta.Age))
}

func (c *mountDirProjectionVectorColumns) appendStats(guta *db.GUTA) {
	c.counts = append(c.counts, guta.Count)
	c.sizes = append(c.sizes, guta.Size)
	c.atimeMins = append(c.atimeMins, guta.Atime)
	c.mtimeMaxs = append(c.mtimeMaxs, guta.Mtime)
}

func (c *mountDirProjectionVectorColumns) appendBuckets(guta *db.GUTA) {
	c.atimeBuckets = append(c.atimeBuckets, ageBucketsSlice(&guta.ATimeRanges))
	c.mtimeBuckets = append(c.mtimeBuckets, ageBucketsSlice(&guta.MTimeRanges))
}

func compareNilProjectionGUTAs(a, b *db.GUTA) (int, bool) {
	switch {
	case a == nil && b == nil:
		return 0, true
	case a == nil:
		return -1, true
	case b == nil:
		return 1, true
	default:
		return 0, false
	}
}

func compareProjectionGUTAValues(a, b *db.GUTA) int {
	if diff := cmp.Compare(a.Age, b.Age); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.GID, b.GID); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.UID, b.UID); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.FT, b.FT); diff != 0 {
		return diff
	}

	return 0
}
