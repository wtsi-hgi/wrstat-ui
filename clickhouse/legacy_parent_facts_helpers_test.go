//go:build legacy_parent_facts
// +build legacy_parent_facts

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
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const dirFilterAgeAllTableName = "wrstat_dir_filter_ageall"

func newIsolatedClickHouseTestHarness(t *testing.T) *clickHouseTestHarness {
	t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		refuseNonLocalhostDSN(t, envDSN)

		return &clickHouseTestHarness{t: t, tcpPort: 0, httpPort: 0, baseDir: "", binPath: ""}
	}

	binPath := findClickHouseBinary(t)
	baseDir := t.TempDir()
	tcpPort := pickFreePort(t)

	httpPort := pickFreePort(t)
	for httpPort == tcpPort {
		httpPort = pickFreePort(t)
	}

	proc := startClickHouseServerProcess(t, binPath, baseDir, tcpPort, httpPort)
	t.Cleanup(proc.stop)

	th := &clickHouseTestHarness{
		t:        t,
		tcpPort:  tcpPort,
		httpPort: httpPort,
		binPath:  binPath,
		baseDir:  baseDir,
		doneCh:   proc.doneCh,
		exitErr:  proc.exitErr,
		stdout:   proc.stdout,
		stderr:   proc.stderr,
	}
	th.waitUntilReady()

	return th
}

func newClickHouseDatabase(cfg Config, conn ch.Conn) *clickHouseDatabase {
	return newClickHouseDatabaseWithSnapshot(cfg, conn, nil)
}

func mapAllDirsHandled(dirs []string) map[string]bool {
	handled := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		handled[ensureTrailingSlash(dir)] = true
	}

	return handled
}

func resetSharedTreeQueryCachesForTesting() {
	ResetTreeQueryCaches()
}

func activePrefixRollupMisses() uint64 {
	return activePrefixRollupMissCounter.Load()
}

func resetActivePrefixRollupMissesForTest() {
	activePrefixRollupMissCounter.Store(0)
}

func activePrefixScalarRollupOrFallback(
	ctx context.Context,
	conn ch.Conn,
	activeSetID, dir string,
	fallback func() (*db.DirSummary, error),
) (*db.DirSummary, error) {
	ready, err := activePrefixRollupsReady(ctx, conn, activeSetID)
	if err != nil {
		return nil, err
	}

	if !ready {
		return activePrefixRollupFallback(fallback)
	}

	sum, found, err := readActivePrefixScalarRollup(ctx, conn, activeSetID, dir)
	if err != nil {
		return nil, err
	}

	if !found {
		return activePrefixRollupFallback(fallback)
	}

	return sum, nil
}

func activeVirtualRowsForMounts(
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
) ([]activeVirtualSummaryRow, []activeVirtualFilterAllRow, []activeVirtualChildRow) {
	childRows := activeVirtualChildRowsForMounts(activeSetID, mounts, refreshedAt)
	summaryRows := activeVirtualSummaryRowsForChildren(activeSetID, mounts, childRows, refreshedAt)
	filterRows := activeVirtualFilterRowsForSummaries(summaryRows)

	return summaryRows, filterRows, childRows
}

func activeVirtualFilterRowsForSummaries(summaryRows []activeVirtualSummaryRow) []activeVirtualFilterAllRow {
	rows := make([]activeVirtualFilterAllRow, 0, len(summaryRows))
	for _, summary := range summaryRows {
		rows = append(rows, activeVirtualFilterAllRow{
			ActiveSetID:      summary.ActiveSetID,
			Dir:              summary.Dir,
			Age:              uint8(db.DGUTAgeAll),
			AtimeBuckets:     emptyAgeBuckets(),
			MtimeBuckets:     emptyAgeBuckets(),
			FilterChildCount: summary.ChildCount,
			ChildCount:       summary.ChildCount,
			RefreshedAt:      summary.RefreshedAt,
		})
	}

	return rows
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
	return appendMountDirFactRowValues(batch, mountDirFactRowValues(mount, dir, state, refreshedAt))
}

func appendMountDirFactRowValues(batch driver.Batch, values []any) error {
	if err := batch.Append(values...); err != nil {
		return fmt.Errorf("clickhouse: failed to append dir facts row: %w", err)
	}

	return nil
}

func mountDirFactRowValues(
	mount activeMount,
	dir string,
	state mountDirProjectionState,
	refreshedAt time.Time,
) []any {
	values := mountDirSummaryBaseValues(
		mount,
		summaryAccumulatorOrZero(state.allSummaries[dir]),
		dgutaRecordContext{canonicalDir: dir},
	)
	values = append(values, mountDirFileSummaryValues(state.allFileSummaries[dir])...)
	values = append(values, mountDirProjectionVectorValues(state.vectors[dir])...)
	values = append(values, state.childCounts[dir], refreshedAt)

	return values
}

func mountDirSummaryBaseValues(
	mount activeMount,
	acc *mountDirSummaryAccumulator,
	record dgutaRecordContext,
) []any {
	return []any{
		mount.mountPath, mount.snapshotID, record.dirID, record.parentID, record.subtreeEnd, mount.updatedAt,
		acc.count, acc.size, acc.atimeMin, acc.mtimeMax,
		ageBucketsSlice(&acc.atimeBuckets), ageBucketsSlice(&acc.mtimeBuckets),
		sortedUint32Set(acc.uids), sortedUint32Set(acc.gids), uint16(acc.ft),
	}
}

func mountDirFileSummaryValues(acc *mountDirSummaryAccumulator) []any {
	return []any{
		summaryCount(acc), summarySize(acc), summaryAtimeMin(acc), summaryMtimeMax(acc),
		summaryATimeBuckets(acc), summaryMTimeBuckets(acc), summaryUIDs(acc),
		summaryGIDs(acc), summaryFT(acc),
	}
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

func (d *clickHouseDatabase) filteredMountWhereSummaries(
	mount activeMount,
	queryDir string,
	filter *db.Filter,
) (map[string]*db.DirSummary, error) {
	ctx, cancel := configQueryContext(d.cfg)
	defer cancel()

	summaries, ok, err := d.filteredMountWhereTraversalSummaries(ctx, mount, queryDir, filter)
	if err != nil || ok {
		return summaries, err
	}

	return d.filteredMountWhereFactsSummaries(ctx, mount, filter)
}

func (c *treeQueryCache) dirSummaryEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.dirSummaries)
}

func (c *treeQueryCache) activePrefixSummaryEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.activePrefixSummaries)
}

func (w *dgutaWriter) flushAllBatches() error {
	return w.flushAllBatchesWithContext(context.Background())
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
