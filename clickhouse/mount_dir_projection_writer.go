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

func mountDirFactRowValues(
	mount activeMount,
	dir string,
	state mountDirProjectionState,
	refreshedAt time.Time,
) []any {
	values := mountDirSummaryBaseValues(
		mount,
		mountDirSummaryKey{dir: dir, age: db.DGUTAgeAll},
		summaryAccumulatorOrZero(state.allSummaries[dir]),
	)
	values = append(values, mountDirFileSummaryValues(state.allFileSummaries[dir])...)
	values = append(values, mountDirProjectionVectorValues(state.vectors[dir])...)
	values = append(values, state.childCounts[dir], refreshedAt)

	return values
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

func (s mountDirProjectionState) summaryKeysFor(compactAges bool) []mountDirSummaryKey {
	keys := make([]mountDirSummaryKey, 0, len(s.summaries))
	for key := range s.summaries {
		if compactAges && key.age != db.DGUTAgeAll {
			continue
		}

		keys = append(keys, key)
	}

	slices.SortFunc(keys, func(a, b mountDirSummaryKey) int {
		if dirCmp := cmp.Compare(a.dir, b.dir); dirCmp != 0 {
			return dirCmp
		}

		return cmp.Compare(a.age, b.age)
	})

	return keys
}

func (s mountDirProjectionState) factDirs(compactAges bool) []string {
	set := make(map[string]struct{}, len(s.dirs)+len(s.summaries)+len(s.vectors)+len(s.childCounts))

	for dir := range s.dirs {
		set[dir] = struct{}{}
	}

	for _, key := range s.summaryKeysFor(compactAges) {
		set[key.dir] = struct{}{}
	}

	for dir := range s.vectors {
		set[dir] = struct{}{}
	}

	for dir, count := range s.childCounts {
		if count > 0 {
			set[dir] = struct{}{}
		}
	}

	dirs := make([]string, 0, len(set))
	for dir := range set {
		dirs = append(dirs, dir)
	}

	slices.Sort(dirs)

	return dirs
}

func (s *mountDirProjectionState) addDir(dir string) {
	if dir == "" {
		return
	}

	s.ensure()
	s.dirs[dir] = struct{}{}
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

func writeMountDirFactRows(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	state mountDirProjectionState,
	refreshedAt time.Time,
	batchSize int,
) error {
	return writeProjectionRows(
		ctx,
		conn,
		insertMountDirSummaryQuery,
		"dir facts",
		state.factDirs(false),
		batchSize,
		func(batch driver.Batch, dir string) error {
			return appendMountDirFactRow(batch, mount, dir, state, refreshedAt)
		},
	)
}

func newProjectionRowsBlockWriter(
	conn ch.Conn,
	query, name string,
	batch *driver.Batch,
	openedAt *time.Time,
	writeErr *error,
	batchSize int,
	now func() time.Time,
) *importBlockWriter {
	return &importBlockWriter{
		conn:      conn,
		query:     query,
		name:      name,
		batch:     batch,
		openedAt:  openedAt,
		writeErr:  writeErr,
		batchSize: batchSize,
		now:       now,
	}
}

func appendProjectionRowValues[T any](
	ctx context.Context,
	writer *importBlockWriter,
	values []T,
	appendRow func(driver.Batch, T) error,
) error {
	for _, value := range values {
		err := writer.append(ctx, func(batch driver.Batch) error {
			return appendRow(batch, value)
		})
		if err != nil {
			return err
		}
	}

	return writer.close()
}

func appendMountDirFactRow(
	batch driver.Batch,
	mount activeMount,
	dir string,
	state mountDirProjectionState,
	refreshedAt time.Time,
) error {
	if err := batch.Append(mountDirFactRowValues(mount, dir, state, refreshedAt)...); err != nil {
		return fmt.Errorf("clickhouse: failed to append dir facts row: %w", err)
	}

	return nil
}

func (w *mountDirProjectionWriter) appendFactRowsWithContext(
	ctx context.Context,
	mount activeMount,
	state mountDirProjectionState,
	compactAges bool,
	batchSize int,
) error {
	return appendTrackedProjectionRows(
		ctx,
		w,
		state.factDirs(compactAges),
		&w.summaryBatch,
		&w.summaryOpenedAt,
		insertMountDirSummaryQuery,
		"dir facts",
		batchSize,
		func(batch driver.Batch, dir string) error {
			return appendMountDirFactRow(batch, mount, dir, state, w.refreshedAt)
		},
	)
}

func (w *mountDirProjectionWriter) appendRecordWithContext(
	ctx context.Context,
	mount activeMount,
	dir string,
	gutas db.GUTAs,
	childCount uint64,
	recordAges []db.DirGUTAge,
	compactAges bool,
	batchSize int,
) error {
	state := newMountDirProjectionState()
	state.addDir(dir)
	state.addGUTAs(dir, gutas)
	state.addChildren(dir, childCount)
	state.addChildOnlySummaryAges(dir, recordAges)

	return w.appendFactRowsWithContext(ctx, mount, state, compactAges, batchSize)
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

func writeProjectionRows[T any](
	ctx context.Context,
	conn ch.Conn,
	query, name string,
	values []T,
	batchSize int,
	appendRow func(driver.Batch, T) error,
) error {
	return writeProjectionRowsWithClock(ctx, conn, query, name, values, batchSize, appendRow, time.Now)
}

func writeProjectionRowsWithClock[T any](
	ctx context.Context,
	conn ch.Conn,
	query, name string,
	values []T,
	batchSize int,
	appendRow func(driver.Batch, T) error,
	now func() time.Time,
) error {
	var (
		batch    driver.Batch
		openedAt time.Time
		writeErr error
	)

	writer := newProjectionRowsBlockWriter(
		conn,
		query,
		name,
		&batch,
		&openedAt,
		&writeErr,
		batchSize,
		now,
	)

	return appendProjectionRowValues(ctx, writer, values, appendRow)
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

func mountDirFileSummaryValues(acc *mountDirSummaryAccumulator) []any {
	return []any{
		summaryCount(acc), summarySize(acc), summaryAtimeMin(acc), summaryMtimeMax(acc),
		summaryATimeBuckets(acc), summaryMTimeBuckets(acc), summaryUIDs(acc),
		summaryGIDs(acc), summaryFT(acc),
	}
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

func summaryAtimeMin(acc *mountDirSummaryAccumulator) int64 {
	if acc == nil {
		return 0
	}

	return acc.atimeMin
}

func summaryMtimeMax(acc *mountDirSummaryAccumulator) int64 {
	if acc == nil {
		return 0
	}

	return acc.mtimeMax
}

func summaryATimeBuckets(acc *mountDirSummaryAccumulator) []uint64 {
	if acc == nil {
		return ageBucketsSlice(nil)
	}

	return ageBucketsSlice(&acc.atimeBuckets)
}

func ageBucketsSlice(buckets *summary.AgeBuckets) []uint64 {
	if buckets == nil {
		return zeroSummaryAgeBuckets[:]
	}

	return buckets[:]
}

func summaryMTimeBuckets(acc *mountDirSummaryAccumulator) []uint64 {
	if acc == nil {
		return ageBucketsSlice(nil)
	}

	return ageBucketsSlice(&acc.mtimeBuckets)
}

func summaryUIDs(acc *mountDirSummaryAccumulator) []uint32 {
	if acc == nil {
		return nil
	}

	return sortedUint32Set(acc.uids)
}

func sortedUint32Set(set map[uint32]struct{}) []uint32 {
	out := make([]uint32, 0, len(set))
	for value := range set {
		out = append(out, value)
	}

	slices.Sort(out)

	return out
}

func summaryGIDs(acc *mountDirSummaryAccumulator) []uint32 {
	if acc == nil {
		return nil
	}

	return sortedUint32Set(acc.gids)
}

func summaryFT(acc *mountDirSummaryAccumulator) uint16 {
	if acc == nil {
		return 0
	}

	return uint16(acc.ft)
}

func mountDirSummaryBaseValues(
	mount activeMount,
	key mountDirSummaryKey,
	acc *mountDirSummaryAccumulator,
) []any {
	return []any{
		mount.mountPath, mount.snapshotID, key.dir, mount.updatedAt,
		acc.count, acc.size, acc.atimeMin, acc.mtimeMax,
		ageBucketsSlice(&acc.atimeBuckets), ageBucketsSlice(&acc.mtimeBuckets),
		sortedUint32Set(acc.uids), sortedUint32Set(acc.gids), uint16(acc.ft),
	}
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

func appendTrackedProjectionRows[T any](
	ctx context.Context,
	writer *mountDirProjectionWriter,
	values []T,
	batch *driver.Batch,
	openedAt *time.Time,
	query, name string,
	batchSize int,
	appendRow func(driver.Batch, T) error,
) error {
	for _, value := range values {
		blockWriter := writer.blockWriter(batch, openedAt, query, name, batchSize)
		if err := blockWriter.append(ctx, func(batch driver.Batch) error {
			return appendRow(batch, value)
		}); err != nil {
			return err
		}
	}

	return nil
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
		s.summaryAccumulatorForDir(s.allFileSummaries, dir).add(guta)
		s.summaryAccumulator(s.fileSummaries, key).add(guta)
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

func (s *mountDirProjectionState) addChildOnlySummaryAges(dir string, ages []db.DirGUTAge) {
	if len(ages) == 0 || s.childCounts[dir] == 0 {
		return
	}

	s.ensure()

	for _, age := range ages {
		key := mountDirSummaryKey{dir: dir, age: age}
		if s.summaries[key] != nil {
			continue
		}

		s.summaries[key] = newMountDirSummaryAccumulator(age)
	}
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

func writeMountDirProjectionRows(
	ctx context.Context,
	conn ch.Conn,
	mount activeMount,
	state mountDirProjectionState,
	batchSize int,
) error {
	if mount.mountPath == "" || mount.snapshotID == "" {
		return nil
	}

	refreshedAt := time.Now().UTC()

	if err := dropMountDirProjectionPartitions(ctx, conn, mount); err != nil {
		return err
	}

	if err := writeMountDirFactRows(ctx, conn, mount, state, refreshedAt, batchSize); err != nil {
		return err
	}

	return writeMountDirSummarySetRow(ctx, conn, mount, refreshedAt)
}

func dropMountDirProjectionPartitions(ctx context.Context, conn ch.Conn, mount activeMount) error {
	if err := dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummarySetPartitionQuery,
	); err != nil {
		return err
	}

	if err := dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirSummaryPartitionQuery,
	); err != nil {
		return err
	}

	return dropPartitionIgnoreUnknown(
		ctx, conn, mount.mountPath, mount.snapshotID, dropDirDGUTAVectorPartitionQuery,
	)
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
