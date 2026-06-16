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
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	navIndexCatalogQuery = "SELECT dir_id, parent_id, subtree_end, name, " +
		"child_dir_count, child_file_count FROM wrstat_dirs " +
		"WHERE mount_path = ? AND snapshot_id = ? ORDER BY dir_id"
	navIndexEstimateFormula = "4 dir_id implicit + 4 parent_id + 4 subtree_end + " +
		"4 counts + len(name) bytes per dir plus map overhead"
	navIndexEstimateNote = "10-100M directories are expected to land in the low-GB range " +
		"before workload-specific map overhead"
	navIndexPerDirFixedEstimateBytes = 16
	navIndexMicrosPerMillis          = 1000
	navIndexReadyPollInterval        = 10 * time.Millisecond

	navIndexInputReady           = "ready"
	navIndexInputEstimateFormula = "estimate_formula"
	navIndexInputEstimateNote    = "estimate_note"
)

var errNavIndexDirIDOverflow = errors.New("clickhouse: navigation index dir_id overflows int")

// NavIndexStats reports asynchronous in-process navigation index build cost.
type NavIndexStats struct {
	ActiveSetID       string        `json:"active_set_id"`
	MountPath         string        `json:"mount_path"`
	SnapshotID        string        `json:"snapshot_id"`
	DirCount          uint64        `json:"dir_count"`
	EstimatedBytes    uint64        `json:"estimated_bytes"`
	MeasuredHeapBytes uint64        `json:"measured_heap_bytes"`
	BuildDuration     time.Duration `json:"build_duration"`
	Ready             bool          `json:"ready"`
	Error             string        `json:"error,omitempty"`
}

func navIndexStatFor(stats []NavIndexStats, mountPath, snapshotID string) (NavIndexStats, bool) {
	mountPath = ensureTrailingSlash(mountPath)

	for _, stat := range stats {
		if stat.MountPath == mountPath && stat.SnapshotID == snapshotID {
			return stat, true
		}
	}

	return NavIndexStats{}, false
}

func navIndexBuiltStats(
	stat NavIndexStats,
	idx *navCatalogIndex,
	before runtime.MemStats,
	after runtime.MemStats,
	start time.Time,
) NavIndexStats {
	stat.DirCount = idx.dirCount
	stat.EstimatedBytes = idx.estimatedBytes
	stat.MeasuredHeapBytes = measuredHeapBytes(before, after)
	stat.BuildDuration = time.Since(start)
	stat.Ready = true

	return stat
}

func (s NavIndexStats) inputMap() map[string]any {
	out := map[string]any{
		"active_set_id":              s.ActiveSetID,
		"mount_path":                 s.MountPath,
		"snapshot_id":                s.SnapshotID,
		"dir_count":                  s.DirCount,
		"estimated_bytes":            s.EstimatedBytes,
		"measured_heap_bytes":        s.MeasuredHeapBytes,
		"build_duration_ms":          float64(s.BuildDuration.Microseconds()) / navIndexMicrosPerMillis,
		navIndexInputReady:           s.Ready,
		navIndexInputEstimateFormula: navIndexEstimateFormula,
		navIndexInputEstimateNote:    navIndexEstimateNote,
	}
	if s.Error != "" {
		out["error"] = s.Error
	}

	return out
}

// NavIndexStats returns build-cost evidence for the current provider readers.
func (p *chProvider) NavIndexStats() []NavIndexStats {
	dbImpl := p.currentClickHouseDatabase()
	if dbImpl == nil {
		return nil
	}

	return dbImpl.NavIndexStats()
}

// NavIndexStats returns build-cost evidence for this reader.
func (d *clickHouseDatabase) NavIndexStats() []NavIndexStats {
	return d.navIndex.statsSnapshot()
}

type navIndexKey struct {
	mountPath  string
	snapshotID string
}

type navIndexChildKey struct {
	parentID uint32
	name     string
}

type navIndexRow struct {
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	name           string
	childDirCount  uint32
	childFileCount uint32
}

func scanNavIndexRow(rows rowsScanner) (navIndexRow, error) {
	var row navIndexRow
	if err := rows.Scan(
		&row.dirID,
		&row.parentID,
		&row.subtreeEnd,
		&row.name,
		&row.childDirCount,
		&row.childFileCount,
	); err != nil {
		return navIndexRow{}, fmt.Errorf("clickhouse: failed to scan navigation index row: %w", err)
	}

	return row, nil
}

type navCatalogIndex struct {
	key navIndexKey

	parentIDs       []uint32
	subtreeEnds     []uint32
	childDirCounts  []uint32
	childFileCounts []uint32
	names           []string
	present         []bool

	childrenByParent map[uint32][]uint32
	childByName      map[navIndexChildKey]uint32

	dirCount       uint64
	estimatedBytes uint64
}

func newNavCatalogIndex(mount activeMount) *navCatalogIndex {
	return &navCatalogIndex{
		key: navIndexKey{
			mountPath:  ensureTrailingSlash(mount.mountPath),
			snapshotID: mount.snapshotID,
		},
		childrenByParent: make(map[uint32][]uint32),
		childByName:      make(map[navIndexChildKey]uint32),
	}
}

func (i *navCatalogIndex) add(row navIndexRow) error {
	if uint64(row.dirID) > uint64(math.MaxInt) {
		return fmt.Errorf("%w: %d", errNavIndexDirIDOverflow, row.dirID)
	}

	i.grow(row.dirID)

	i.parentIDs[row.dirID] = row.parentID
	i.subtreeEnds[row.dirID] = row.subtreeEnd
	i.childDirCounts[row.dirID] = row.childDirCount
	i.childFileCounts[row.dirID] = row.childFileCount
	i.names[row.dirID] = row.name

	if !i.present[row.dirID] {
		i.dirCount++
		i.estimatedBytes += navIndexPerDirFixedEstimateBytes + uint64(len(row.name))
	}

	i.present[row.dirID] = true

	if row.dirID != row.parentID {
		i.childrenByParent[row.parentID] = append(i.childrenByParent[row.parentID], row.dirID)
		i.childByName[navIndexChildKey{parentID: row.parentID, name: row.name}] = row.dirID
	}

	return nil
}

func (i *navCatalogIndex) grow(dirID uint32) {
	needed := int(dirID) + 1
	if needed <= len(i.parentIDs) {
		return
	}

	i.parentIDs = append(i.parentIDs, make([]uint32, needed-len(i.parentIDs))...)
	i.subtreeEnds = append(i.subtreeEnds, make([]uint32, needed-len(i.subtreeEnds))...)
	i.childDirCounts = append(i.childDirCounts, make([]uint32, needed-len(i.childDirCounts))...)
	i.childFileCounts = append(i.childFileCounts, make([]uint32, needed-len(i.childFileCounts))...)
	i.names = append(i.names, make([]string, needed-len(i.names))...)
	i.present = append(i.present, make([]bool, needed-len(i.present))...)
}

func (i *navCatalogIndex) finalise() {
	for parent, children := range i.childrenByParent {
		sort.Slice(children, func(a, b int) bool {
			left, leftOK := i.fullPathForID(children[a])
			right, rightOK := i.fullPathForID(children[b])

			return leftOK && (!rightOK || left < right)
		})

		i.childrenByParent[parent] = children
	}
}

func (i *navCatalogIndex) refForPath(dir string) (treeCatalogDirRef, bool, error) {
	dirID, ok := i.idForPath(dir)
	if !ok {
		return treeCatalogDirRef{}, false, nil
	}

	return i.refForID(dirID)
}

func (i *navCatalogIndex) refForID(dirID uint32) (treeCatalogDirRef, bool, error) {
	if !i.hasID(dirID) {
		return treeCatalogDirRef{}, false, nil
	}

	fullPath, ok := i.fullPathForID(dirID)
	if !ok {
		return treeCatalogDirRef{}, false, nil
	}

	return treeCatalogDirRef{
		dirID:      dirID,
		parentID:   i.parentIDs[dirID],
		subtreeEnd: i.subtreeEnds[dirID],
		fullPath:   fullPath,
	}, true, nil
}

func (i *navCatalogIndex) children(parentDir string) ([]string, error) {
	ref, ok, err := i.refForPath(parentDir)
	if err != nil || !ok {
		return nil, err
	}

	children := make([]string, 0, len(i.childrenByParent[ref.dirID]))
	for _, childID := range i.childrenByParent[ref.dirID] {
		child, childOK := i.fullPathForID(childID)
		if childOK {
			children = append(children, child)
		}
	}

	return children, nil
}

func (i *navCatalogIndex) childrenForParents(parentDirs []string) (map[string][]string, error) {
	out := make(map[string][]string, len(parentDirs))
	for _, parentDir := range parentDirs {
		key := ensureTrailingSlash(parentDir)

		children, err := i.children(key)
		if err != nil {
			return nil, err
		}

		out[key] = children
	}

	return out, nil
}

func (i *navCatalogIndex) parentsWithChildren(dirs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(dirs))

	for _, dir := range uniqueQueryDirs(dirs) {
		ref, ok, err := i.refForPath(dir)
		if err != nil {
			return nil, err
		}

		if ok && i.childDirCounts[ref.dirID] > 0 {
			out[ref.fullPath] = true
		}
	}

	return out, nil
}

func (i *navCatalogIndex) idForPath(dir string) (uint32, bool) {
	dir = ensureTrailingSlash(dir)
	if dir == "/" {
		return 0, i.hasID(0)
	}

	current := uint32(0)
	for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
		next, ok := i.childByName[navIndexChildKey{
			parentID: current,
			name:     part + "/",
		}]
		if !ok {
			return 0, false
		}

		current = next
	}

	return current, i.hasID(current)
}

func (i *navCatalogIndex) fullPathForID(dirID uint32) (string, bool) {
	if !i.hasID(dirID) {
		return "", false
	}

	ids, ok := i.pathIDs(dirID)
	if !ok {
		return "", false
	}

	var b strings.Builder

	for idx := len(ids) - 1; idx >= 0; idx-- {
		id := ids[idx]

		name := i.names[id]
		if id == 0 && name == "/" {
			b.WriteString("/")

			continue
		}

		b.WriteString(name)
	}

	return b.String(), true
}

func (i *navCatalogIndex) pathIDs(dirID uint32) ([]uint32, bool) {
	ids := []uint32{dirID}
	seen := map[uint32]bool{dirID: true}

	for dirID != 0 {
		parentID := i.parentIDs[dirID]
		if !i.hasID(parentID) || seen[parentID] {
			return nil, false
		}

		ids = append(ids, parentID)
		seen[parentID] = true
		dirID = parentID
	}

	return ids, true
}

func (i *navCatalogIndex) hasID(dirID uint32) bool {
	return int(dirID) < len(i.present) && i.present[dirID]
}

func (i *navCatalogIndex) addRows(rows rowsScanner) error {
	for rows.Next() {
		row, scanErr := scanNavIndexRow(rows)
		if scanErr != nil {
			return scanErr
		}

		if addErr := i.add(row); addErr != nil {
			return addErr
		}
	}

	if err := rowsErr(rows); err != nil {
		return fmt.Errorf("clickhouse: navigation index iteration error: %w", err)
	}

	return nil
}

type navIndexManager struct {
	cfg      Config
	conn     ch.Conn
	snapshot *activeMountsSnapshot

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.RWMutex
	indexes map[navIndexKey]*navCatalogIndex
	stats   []NavIndexStats
	ready   bool
}

func newNavIndexManager(cfg Config, conn ch.Conn, snapshot *activeMountsSnapshot) *navIndexManager {
	if !cfg.NavIndex || conn == nil || snapshot == nil {
		return nil
	}

	return &navIndexManager{
		cfg:      cfg,
		conn:     conn,
		snapshot: snapshot,
	}
}

func (m *navIndexManager) start(parent context.Context) {
	if m == nil {
		return
	}

	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()

		if err := m.build(ctx); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
			m.publishBuildError(err)
		}
	}()
}

func (m *navIndexManager) close() {
	if m == nil {
		return
	}

	if m.cancel != nil {
		m.cancel()
	}

	m.wg.Wait()
}

func (m *navIndexManager) publishBuildError(err error) {
	if err == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.stats) == 0 {
		m.stats = []NavIndexStats{{Error: err.Error()}}
	}
}

func (m *navIndexManager) build(ctx context.Context) error {
	if m == nil {
		return nil
	}

	mounts := m.snapshot.all()
	indexes := make(map[navIndexKey]*navCatalogIndex, len(mounts))
	stats := make([]NavIndexStats, 0, len(mounts))

	for _, mount := range mounts {
		idx, stat, err := m.buildMount(ctx, mount)
		if err != nil {
			stat.Error = err.Error()
			stats = append(stats, stat)
			m.publish(indexes, stats, false)

			return err
		}

		indexes[idx.key] = idx

		stats = append(stats, stat)
	}

	m.publish(indexes, stats, true)

	return nil
}

func (m *navIndexManager) buildMount(
	ctx context.Context,
	mount activeMount,
) (*navCatalogIndex, NavIndexStats, error) {
	idx := newNavCatalogIndex(mount)
	stat := NavIndexStats{
		ActiveSetID: mount.activeSetID,
		MountPath:   mount.mountPath,
		SnapshotID:  mount.snapshotID,
	}

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()

	if err := m.loadMountIndex(ctx, mount, idx); err != nil {
		return idx, stat, err
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	stat = navIndexBuiltStats(stat, idx, before, after, start)

	return idx, stat, nil
}

func (m *navIndexManager) loadMountIndex(
	ctx context.Context,
	mount activeMount,
	idx *navCatalogIndex,
) error {
	rows, err := m.conn.Query(ctx, navIndexCatalogQuery, mount.mountPath, mount.snapshotID)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to build navigation index: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if err := idx.addRows(rows); err != nil {
		return err
	}

	idx.finalise()

	return nil
}

func (m *navIndexManager) publish(
	indexes map[navIndexKey]*navCatalogIndex,
	stats []NavIndexStats,
	ready bool,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.indexes = indexes
	m.stats = stats
	m.ready = ready
}

func (m *navIndexManager) index(mountPath, snapshotID string) (*navCatalogIndex, bool, bool) {
	if m == nil {
		return nil, false, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return nil, false, true
	}

	idx, ok := m.indexes[navIndexKey{
		mountPath:  ensureTrailingSlash(mountPath),
		snapshotID: snapshotID,
	}]

	return idx, ok, true
}

func (m *navIndexManager) statsSnapshot() []NavIndexStats {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return append([]NavIndexStats(nil), m.stats...)
}

func (m *navIndexManager) waitReady(ctx context.Context) {
	if m == nil {
		return
	}

	ticker := time.NewTicker(navIndexReadyPollInterval)
	defer ticker.Stop()

	for !m.readyOrFailed() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *navIndexManager) readyOrFailed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ready || len(m.stats) > 0 && m.stats[len(m.stats)-1].Error != ""
}

func (m *navIndexManager) benchmarkEvidence(
	ctx context.Context,
	mountPath, snapshotID, parentDir string,
) map[string]any {
	stats := m.statsSnapshot()
	if len(stats) == 0 {
		return map[string]any{
			"ready":            false,
			"estimate_formula": navIndexEstimateFormula,
			"estimate_note":    navIndexEstimateNote,
		}
	}

	evidence := stats[0].inputMap()
	if stat, ok := navIndexStatFor(stats, mountPath, snapshotID); ok {
		evidence = stat.inputMap()
	}

	indexLatency := m.measureIndexChildren(mountPath, snapshotID, parentDir)
	clickHouseLatency := m.measureClickHouseChildren(ctx, mountPath, snapshotID, parentDir)

	evidence["index_latency_ns"] = indexLatency.Nanoseconds()
	evidence["clickhouse_latency_ns"] = clickHouseLatency.Nanoseconds()
	evidence["latency_delta_ns"] = clickHouseLatency.Nanoseconds() - indexLatency.Nanoseconds()

	return evidence
}

func (m *navIndexManager) measureIndexChildren(mountPath, snapshotID, parentDir string) time.Duration {
	start := time.Now()

	if idx, ok, ready := m.index(mountPath, snapshotID); ready && ok {
		if _, err := idx.children(parentDir); err != nil {
			return time.Since(start)
		}
	}

	return time.Since(start)
}

func (m *navIndexManager) measureClickHouseChildren(
	ctx context.Context,
	mountPath, snapshotID, parentDir string,
) time.Duration {
	start := time.Now()

	if m != nil && m.conn != nil {
		if _, err := navIndexClickHouseChildren(ctx, m.conn, mountPath, snapshotID, parentDir); err != nil {
			return time.Since(start)
		}
	}

	return time.Since(start)
}

func navIndexClickHouseChildren(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID, parentDir string,
) ([]string, error) {
	queryDir := ensureTrailingSlash(parentDir)

	rows, err := conn.Query(ctx, dirForFullPathQuery, mountPath, snapshotID, catalogPathHash(queryDir), queryDir)
	if err != nil {
		return nil, err
	}

	ref, ok, err := scanTreeCatalogDirRef(rows, "navigation index benchmark dir resolution")
	closeErr := rows.Close()

	if err != nil || !ok {
		return nil, err
	}

	if closeErr != nil {
		return nil, closeErr
	}

	rows, err = conn.Query(ctx, childrenForDirIDQuery, mountPath, snapshotID, ref.dirID)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	return scanChildrenRows(rows)
}

func measuredHeapBytes(before, after runtime.MemStats) uint64 {
	if after.HeapAlloc > before.HeapAlloc {
		return after.HeapAlloc - before.HeapAlloc
	}

	if after.TotalAlloc > before.TotalAlloc {
		return after.TotalAlloc - before.TotalAlloc
	}

	return 0
}

func navIndexUnavailableEvidence() map[string]any {
	return map[string]any{
		"ready":            false,
		"estimate_formula": navIndexEstimateFormula,
		"estimate_note":    navIndexEstimateNote,
	}
}

// NavIndexBenchmarkEvidence reports nav-index build memory and latency delta.
func (p *chProvider) NavIndexBenchmarkEvidence(ctx context.Context, parentDir string) map[string]any {
	dbImpl := p.currentClickHouseDatabase()
	if dbImpl == nil {
		return navIndexUnavailableEvidence()
	}

	return dbImpl.NavIndexBenchmarkEvidence(ctx, parentDir)
}

func (p *chProvider) currentClickHouseDatabase() *clickHouseDatabase {
	p.mu.RLock()
	defer p.mu.RUnlock()

	dbImpl, ok := p.db.(*clickHouseDatabase)
	if !ok {
		return nil
	}

	return dbImpl
}

// NavIndexBenchmarkEvidence reports nav-index build memory and latency delta.
func (d *clickHouseDatabase) NavIndexBenchmarkEvidence(
	ctx context.Context,
	parentDir string,
) map[string]any {
	if d.navIndex == nil {
		return navIndexUnavailableEvidence()
	}

	waitCtx, cancel := queryContext(ctx, queryTimeout(d.cfg))
	defer cancel()

	d.navIndex.waitReady(waitCtx)

	mount, ok := d.snapshot.resolve(parentDir)
	if !ok {
		return navIndexUnavailableEvidence()
	}

	return d.navIndex.benchmarkEvidence(ctx, mount.mountPath, mount.snapshotID, parentDir)
}

func (d *clickHouseDatabase) navIndexCatalogDirForMount(
	mountPath, snapshotID string,
	dir string,
) (treeCatalogDirRef, bool, bool, error) {
	idx, ok, ready := d.navIndex.index(mountPath, snapshotID)
	if !ready || !ok {
		return treeCatalogDirRef{}, false, false, nil
	}

	ref, found, err := idx.refForPath(dir)

	return ref, found, true, err
}

func (d *clickHouseDatabase) navIndexCatalogDirForID(
	mountPath, snapshotID string,
	dirID uint32,
) (treeCatalogDirRef, bool, bool, error) {
	idx, ok, ready := d.navIndex.index(mountPath, snapshotID)
	if !ready || !ok {
		return treeCatalogDirRef{}, false, false, nil
	}

	ref, found, err := idx.refForID(dirID)

	return ref, found, true, err
}

func (d *clickHouseDatabase) navIndexChildrenForMount(
	mountPath, snapshotID string,
	parentDir string,
) ([]string, bool, error) {
	idx, ok, ready := d.navIndex.index(mountPath, snapshotID)
	if !ready || !ok {
		return nil, false, nil
	}

	children, err := idx.children(parentDir)

	return children, true, err
}

func (d *clickHouseDatabase) navIndexChildrenForParents(
	mountPath, snapshotID string,
	parentDirs []string,
) (map[string][]string, bool, error) {
	idx, ok, ready := d.navIndex.index(mountPath, snapshotID)
	if !ready || !ok {
		return nil, false, nil
	}

	children, err := idx.childrenForParents(parentDirs)

	return children, true, err
}

func (d *clickHouseDatabase) navIndexParentsWithChildren(
	mountPath, snapshotID string,
	dirs []string,
) (map[string]bool, bool, error) {
	idx, ok, ready := d.navIndex.index(mountPath, snapshotID)
	if !ready || !ok {
		return nil, false, nil
	}

	parents, err := idx.parentsWithChildren(dirs)

	return parents, true, err
}
