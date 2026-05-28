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

func compareProjectionGUTAs(a, b *db.GUTA) int {
	if a == nil && b == nil {
		return 0
	}

	if a == nil {
		return -1
	}

	if b == nil {
		return 1
	}

	for _, diff := range []int{
		cmp.Compare(a.Age, b.Age),
		cmp.Compare(a.GID, b.GID),
		cmp.Compare(a.UID, b.UID),
		cmp.Compare(a.FT, b.FT),
	} {
		if diff != 0 {
			return diff
		}
	}

	return 0
}

type mountDirSummaryKey struct {
	dir string
	age db.DirGUTAge
}

func writeMountDirSummaryRows(
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
		"dir summary",
		state.summaryKeys(),
		batchSize,
		func(batch driver.Batch, key mountDirSummaryKey) error {
			return appendMountDirSummaryRow(batch, mount, key, state, refreshedAt)
		},
	)
}

func writeProjectionRows[T any](
	ctx context.Context,
	conn ch.Conn,
	query, name string,
	values []T,
	batchSize int,
	appendRow func(driver.Batch, T) error,
) error {
	batch, err := prepareBatchWithRelease(ctx, conn, query)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare %s batch: %w", name, err)
	}

	for _, value := range values {
		appendErr := appendRow(batch, value)
		if appendErr != nil {
			return errors.Join(appendErr, abortProjectionBatch(batch, name))
		}

		if batch.Rows() < batchSize {
			continue
		}

		batch, err = sendAndReplaceProjectionBatch(ctx, conn, batch, query, name)
		if err != nil {
			return err
		}
	}

	return sendOrAbortProjectionBatch(batch, name)
}

func abortProjectionBatch(batch driver.Batch, name string) error {
	if err := batch.Abort(); err != nil {
		return fmt.Errorf("clickhouse: failed to abort %s batch: %w", name, err)
	}

	return nil
}

func sendAndReplaceProjectionBatch(
	ctx context.Context,
	conn ch.Conn,
	batch driver.Batch,
	query, name string,
) (driver.Batch, error) {
	if err := batch.Send(); err != nil {
		return nil, fmt.Errorf("clickhouse: failed to send %s batch: %w", name, err)
	}

	next, err := prepareBatchWithRelease(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to prepare %s batch: %w", name, err)
	}

	return next, nil
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

func appendMountDirSummaryRow(
	batch driver.Batch,
	mount activeMount,
	key mountDirSummaryKey,
	state mountDirProjectionState,
	refreshedAt time.Time,
) error {
	if err := batch.Append(mountDirSummaryRowValues(mount, key, state, refreshedAt)...); err != nil {
		return fmt.Errorf("clickhouse: failed to append dir summary row: %w", err)
	}

	return nil
}

func mountDirSummaryRowValues(
	mount activeMount,
	key mountDirSummaryKey,
	state mountDirProjectionState,
	refreshedAt time.Time,
) []any {
	values := mountDirSummaryBaseValues(mount, key, state.summaries[key])
	values = append(values, mountDirFileSummaryValues(state.fileSummaries[key])...)
	values = append(values, state.childCounts[key.dir], refreshedAt)

	return values
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
		return make([]uint64, len(summary.AgeBuckets{}))
	}

	return ageBucketsSlice(acc.atimeBuckets)
}

func ageBucketsSlice(buckets summary.AgeBuckets) []uint64 {
	return buckets[:]
}

func summaryMTimeBuckets(acc *mountDirSummaryAccumulator) []uint64 {
	if acc == nil {
		return make([]uint64, len(summary.AgeBuckets{}))
	}

	return ageBucketsSlice(acc.mtimeBuckets)
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
		uint8(key.age), acc.count, acc.size, acc.atimeMin, acc.mtimeMax,
		ageBucketsSlice(acc.atimeBuckets), ageBucketsSlice(acc.mtimeBuckets),
		sortedUint32Set(acc.uids), sortedUint32Set(acc.gids), uint16(acc.ft),
	}
}

type mountDirProjectionWriter struct {
	conn           ch.Conn
	summaryBatch   driver.Batch
	vectorBatch    driver.Batch
	summaryFlushed bool
	vectorFlushed  bool
	refreshedAt    time.Time
	writeErr       error
}

func prepareMountDirProjectionWriter(
	ctx context.Context,
	conn ch.Conn,
) (mountDirProjectionWriter, error) {
	writer := mountDirProjectionWriter{refreshedAt: time.Now().UTC()}
	if err := writer.prepare(ctx, conn); err != nil {
		return mountDirProjectionWriter{}, err
	}

	return writer, nil
}

func (w *mountDirProjectionWriter) prepare(ctx context.Context, conn ch.Conn) error {
	w.conn = conn

	summaryBatch, err := prepareBatchWithRelease(ctx, conn, insertMountDirSummaryQuery)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare dir summary batch: %w", err)
	}

	vectorBatch, err := prepareBatchWithRelease(ctx, conn, insertMountDirDGUTAVectorQuery)
	if err != nil {
		if abortErr := summaryBatch.Abort(); abortErr != nil {
			return fmt.Errorf(
				"clickhouse: failed to prepare dir dguta vector batch and abort dir summary batch: %w",
				errors.Join(err, abortErr),
			)
		}

		return fmt.Errorf("clickhouse: failed to prepare dir dguta vector batch: %w", err)
	}

	w.summaryBatch = summaryBatch
	w.vectorBatch = vectorBatch
	w.summaryFlushed = false
	w.vectorFlushed = false

	return nil
}

func (w *mountDirProjectionWriter) appendRecord(
	mount activeMount,
	dir string,
	gutas db.GUTAs,
	childCount uint64,
	recordAges []db.DirGUTAge,
	batchSize int,
) error {
	state := newMountDirProjectionState()
	state.addGUTAs(dir, gutas)
	state.addChildren(dir, childCount)
	state.addChildOnlySummaryAges(dir, recordAges)

	if err := w.appendSummaryRows(mount, state, batchSize); err != nil {
		return err
	}

	return w.appendVectorRows(mount, state, batchSize)
}

func (w *mountDirProjectionWriter) appendSummaryRows(
	mount activeMount,
	state mountDirProjectionState,
	batchSize int,
) error {
	if err := w.batchReady(w.summaryBatch, "dir summary"); err != nil {
		return err
	}

	for _, key := range state.summaryKeys() {
		if err := appendMountDirSummaryRow(w.summaryBatch, mount, key, state, w.refreshedAt); err != nil {
			return err
		}

		if err := w.sendFullBatchIfFull(
			&w.summaryBatch,
			insertMountDirSummaryQuery,
			batchSize,
			"dir summary",
		); err != nil {
			return err
		}
	}

	return nil
}

func (w *mountDirProjectionWriter) appendVectorRows(
	mount activeMount,
	state mountDirProjectionState,
	batchSize int,
) error {
	if err := w.batchReady(w.vectorBatch, "dir dguta vector"); err != nil {
		return err
	}

	for _, vectorDir := range state.vectorDirs() {
		if err := appendMountDirDGUTAVectorRow(w.vectorBatch, mount, vectorDir, state, w.refreshedAt); err != nil {
			return err
		}

		if err := w.sendFullBatchIfFull(
			&w.vectorBatch,
			insertMountDirDGUTAVectorQuery,
			batchSize,
			"dir dguta vector",
		); err != nil {
			return err
		}
	}

	return nil
}

func (w *mountDirProjectionWriter) batchReady(batch driver.Batch, name string) error {
	if w.writeErr != nil {
		return w.writeErr
	}

	if batch != nil {
		return nil
	}

	w.writeErr = fmt.Errorf("%w: %s", errDirProjectionBatchNotPrepared, name)

	return w.writeErr
}

func (w *mountDirProjectionWriter) sendFullBatchIfFull(
	batch *driver.Batch,
	query string,
	batchSize int,
	name string,
) error {
	if batch == nil || *batch == nil || batchSize <= 0 || (*batch).Rows() < batchSize {
		return nil
	}

	return w.sendFullBatch(batch, query, name)
}

func (w *mountDirProjectionWriter) sendFullBatch(
	batch *driver.Batch,
	query, name string,
) error {
	if err := (*batch).Send(); err != nil {
		*batch = nil
		w.writeErr = fmt.Errorf("clickhouse: failed to send %s batch: %w", name, err)

		return w.writeErr
	}

	*batch = nil

	next, err := prepareBatchWithRelease(context.Background(), w.conn, query)
	if err != nil {
		w.writeErr = fmt.Errorf("clickhouse: failed to prepare %s batch: %w", name, err)

		return w.writeErr
	}

	*batch = next

	return nil
}

func (w *mountDirProjectionWriter) abortAll() error {
	return errors.Join(
		abortBatch(&w.summaryBatch, "dir summary"),
		abortBatch(&w.vectorBatch, "dir dguta vector"),
	)
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
	summaries     map[mountDirSummaryKey]*mountDirSummaryAccumulator
	fileSummaries map[mountDirSummaryKey]*mountDirSummaryAccumulator
	vectors       map[string]db.GUTAs
	childCounts   map[string]uint64
}

func newMountDirProjectionState() mountDirProjectionState {
	return mountDirProjectionState{
		summaries:     make(map[mountDirSummaryKey]*mountDirSummaryAccumulator),
		fileSummaries: make(map[mountDirSummaryKey]*mountDirSummaryAccumulator),
		vectors:       make(map[string]db.GUTAs),
		childCounts:   make(map[string]uint64),
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

	key := mountDirSummaryKey{dir: dir, age: guta.Age}
	s.summaryAccumulator(s.summaries, key).add(guta)

	if guta.FT&db.AllTypesExceptDirectories > 0 {
		s.summaryAccumulator(s.fileSummaries, key).add(guta)
	}

	s.vectors[dir] = append(s.vectors[dir], cloneGUTA(guta))
}

func cloneGUTA(guta *db.GUTA) *db.GUTA {
	if guta == nil {
		return nil
	}

	cloned := *guta

	return &cloned
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

func (s mountDirProjectionState) summaryKeys() []mountDirSummaryKey {
	keys := make([]mountDirSummaryKey, 0, len(s.summaries))
	for key := range s.summaries {
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

func (s mountDirProjectionState) vectorDirs() []string {
	dirs := make([]string, 0, len(s.vectors))
	for dir := range s.vectors {
		dirs = append(dirs, dir)
	}

	slices.Sort(dirs)

	return dirs
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

	if err := writeMountDirSummaryRows(ctx, conn, mount, state, refreshedAt, batchSize); err != nil {
		return err
	}

	if err := writeMountDirDGUTAVectorRows(ctx, conn, mount, state, refreshedAt, batchSize); err != nil {
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
	batch, err := prepareBatchWithRelease(ctx, conn, insertMountDirSummarySetQuery)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare dir summary set batch: %w", err)
	}

	if err := batch.Append(
		mount.mountPath,
		mount.snapshotID,
		mount.updatedAt,
		mountDirSummaryVersion,
		refreshedAt,
	); err != nil {
		return errors.Join(
			fmt.Errorf("clickhouse: failed to append dir summary set row: %w", err),
			abortProjectionBatch(batch, "dir summary set"),
		)
	}

	return sendOrAbortProjectionBatch(batch, "dir summary set")
}

func writeMountDirDGUTAVectorRows(
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
		insertMountDirDGUTAVectorQuery,
		"dir dguta vector",
		state.vectorDirs(),
		batchSize,
		func(batch driver.Batch, dir string) error {
			return appendMountDirDGUTAVectorRow(batch, mount, dir, state, refreshedAt)
		},
	)
}

func appendMountDirDGUTAVectorRow(
	batch driver.Batch,
	mount activeMount,
	dir string,
	state mountDirProjectionState,
	refreshedAt time.Time,
) error {
	columns := mountDirProjectionVectorColumnsFor(state.vectors[dir])

	if err := batch.Append(
		mount.mountPath,
		mount.snapshotID,
		dir,
		mount.updatedAt,
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
		state.childCounts[dir],
		refreshedAt,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to append dir dguta vector row: %w", err)
	}

	return nil
}

func mountDirProjectionVectorColumnsFor(gutas db.GUTAs) mountDirProjectionVectorColumns {
	gutas = cloneGUTAs(gutas)
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
	c.atimeBuckets = append(c.atimeBuckets, ageBucketsSlice(guta.ATimeRanges))
	c.mtimeBuckets = append(c.mtimeBuckets, ageBucketsSlice(guta.MTimeRanges))
}
