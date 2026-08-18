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
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	summariseSpoolLoadPhasePrefix                  = "spool_load_"
	spoolHistoryDeleteChunk                        = 512
	summariseSpoolBytesPerKiB                      = 1024
	spoolLoadReportOperation                       = "spool_load_total"
	spoolLoadReportSuccess                         = "success"
	spoolLoadReportNotAttempted                    = "not_attempted"
	spoolLoadTableStatsAvailable                   = "available"
	spoolLoadTableStatsUnavailable                 = "unavailable"
	spoolLoadTableStatsNotRequested                = "not_requested"
	clickHouseInsufficientPrivilegeCode            = 497
	summariseSpoolFilterAmplificationWarnThreshold = 5.0
	summariseSpoolPartTelemetryTimeout             = 2 * time.Second
	summariseSpoolLiveTelemetryInterval            = 30 * time.Second

	selectReadyActiveVirtualSetCountsQuery = "SELECT summary_rows, filter_rows, child_rows " +
		"FROM wrstat_active_virtual_sets WHERE active_set_id = ? AND ready = 1 LIMIT 1"
	selectActiveVirtualSummariesForDirsQuery = "SELECT d.full_path, s.virtual_id, s.mount_path, " +
		"toString(s.snapshot_id), s.mount_root_dir_id, s.is_mount_root_box, s.updated_at, " +
		"all_count, all_size, all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, " +
		"all_uids, all_gids, all_ft, file_count, file_size, child_count " +
		"FROM wrstat_active_virtual_summaries AS s INNER JOIN wrstat_active_virtual_dirs AS d " +
		"ON d.active_set_id = s.active_set_id AND d.virtual_id = s.virtual_id " +
		"PREWHERE s.active_set_id = ? WHERE d.full_path IN (%s) ORDER BY s.virtual_id"
	selectActiveVirtualFiltersForDirsQuery = "SELECT d.full_path, v.virtual_id, age, gid, uid, ft, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, child_count " +
		"FROM wrstat_active_virtual_filter_all AS v INNER JOIN wrstat_active_virtual_dirs AS d " +
		"ON d.active_set_id = v.active_set_id AND d.virtual_id = v.virtual_id " +
		"PREWHERE v.active_set_id = ? WHERE d.full_path IN (%s) ORDER BY v.virtual_id, age, gid, uid, ft"
)

var (
	// ErrFilterAmplificationStatsUnavailable is returned when the mandatory
	// pre-publish amplification gate cannot load or derive row-count evidence.
	ErrFilterAmplificationStatsUnavailable = errors.New(
		"clickhouse: full-filter row amplification stats unavailable",
	)
	errSummariseSpoolManifestRequired          = errors.New("clickhouse: summarise spool manifest is required")
	errInvalidSummariseSpoolManifest           = errors.New("clickhouse: invalid summarise spool manifest")
	errSpoolDecodedRowsMismatch                = errors.New("clickhouse: spool decoded row count mismatch")
	errUnknownSpoolLoadTable                   = errors.New("clickhouse: no loader query for spool table")
	errSpoolLoadedRowsMismatch                 = errors.New("clickhouse: spool loaded row count mismatch")
	errSummariseSpoolAmplificationStatsMissing = errors.New(
		"clickhouse: required row-count evidence missing or zero",
	)
	errSummariseSpoolPublishMissingSwitchPlan = errors.New(
		"clickhouse: summarise spool publish state is missing switch plan",
	)
	errUnsupportedSummariseSpoolPublishStateVersion = errors.New(
		"clickhouse: unsupported summarise spool publish state version",
	)
)

type activeVirtualCompositionCounts struct {
	summaryRows uint64
	filterRows  uint64
	childRows   uint64
}

func activeVirtualManifestSHA256ForCounts(
	activeSetID string,
	counts activeVirtualCompositionCounts,
) string {
	return sha256Hex(fmt.Sprintf(
		"%s|%d|%d|%d|%d",
		activeSetID,
		currentSchemaVersion,
		counts.summaryRows,
		counts.filterRows,
		counts.childRows,
	))
}

type historyLastDateKey struct {
	mountPath string
	gid       uint32
}

type summariseSpoolTelemetryTicker interface {
	channel() <-chan time.Time
	stop()
}

func newSummariseSpoolTelemetryTicker(every time.Duration) summariseSpoolTelemetryTicker {
	return &summariseSpoolRealTelemetryTicker{Ticker: time.NewTicker(every)}
}

func parseSummariseSpoolIdentity(manifest *chspool.Manifest) (uuid.UUID, time.Time, error) {
	snapshot, err := uuid.Parse(manifest.SnapshotID)
	if err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("clickhouse: invalid summarise spool snapshot id: %w", err)
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, manifest.UpdatedAt)
	if err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("clickhouse: invalid summarise spool updated_at: %w", err)
	}

	return snapshot, updatedAt, nil
}

type summariseSpoolLoader struct {
	cfg                 Config
	conn                driver.Conn
	dir                 string
	manifest            *chspool.Manifest
	snapshot            uuid.UUID
	updatedAt           time.Time
	importPhaseRecorder func(string, time.Duration)
	loadedRows          map[string]uint64
	groupUsageDates     map[uint32]finaliseQuotaDates
	telemetryRecorder   func(SummariseImportTelemetry)
	telemetry           SummariseImportTelemetry
	telemetryNow        func() time.Time
	telemetryStarted    time.Time
	telemetryMu         sync.Mutex
	telemetryEmitMu     sync.Mutex
	telemetryTicker     func(time.Duration) summariseSpoolTelemetryTicker
	pressureProbe       func(context.Context, string) summariseServerPressure
	pressurePollTimer   func(time.Duration) summarisePressurePollTimer
	batchMeasurements   map[string][]importBatchMeasurement
	amplificationGated  bool
}

func newSummariseSpoolLoader(
	cfg Config,
	conn driver.Conn,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
	telemetryRecorder ...func(SummariseImportTelemetry),
) (*summariseSpoolLoader, error) {
	snapshot, updatedAt, err := parseSummariseSpoolIdentity(manifest)
	if err != nil {
		return nil, err
	}

	loader := &summariseSpoolLoader{
		cfg:                 cfg,
		conn:                conn,
		dir:                 spoolDir,
		manifest:            manifest,
		snapshot:            snapshot,
		updatedAt:           updatedAt,
		importPhaseRecorder: recorder,
		loadedRows:          map[string]uint64{},
		groupUsageDates:     map[uint32]finaliseQuotaDates{},
		telemetryNow:        time.Now,
		telemetryTicker:     newSummariseSpoolTelemetryTicker,
		batchMeasurements:   make(map[string][]importBatchMeasurement),
	}
	loader.setTelemetryRecorder(telemetryRecorder)

	return loader, nil
}

func (l *summariseSpoolLoader) setTelemetryRecorder(recorders []func(SummariseImportTelemetry)) {
	if len(recorders) > 0 {
		l.telemetryRecorder = recorders[0]
	}
}

func (l *summariseSpoolLoader) load(parent context.Context) error {
	return l.loadWithAfterPublish(parent, nil)
}

func (l *summariseSpoolLoader) loadWithAfterPublish(
	parent context.Context,
	afterPublish summariseSpoolAfterPublish,
) error {
	defer func() { _ = l.conn.Close() }()

	parent = loadParentContext(parent)

	tracker, err := l.newPublishTracker()
	if err != nil {
		return err
	}

	if err := l.ensureTablesLoaded(parent, tracker); err != nil {
		return err
	}

	if err := l.enforceFullFilterAmplificationGateBeforePublish(parent, tracker); err != nil {
		return err
	}

	if err := l.publishWithTracker(parent, tracker); err != nil {
		return err
	}

	if afterPublish == nil {
		return nil
	}

	return afterPublish(parent, l)
}

func loadParentContext(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}

	return parent
}

func (l *summariseSpoolLoader) newPublishTracker() (*summariseSpoolPublishTracker, error) {
	tracker, err := newSummariseSpoolPublishTracker(l.dir, l.manifest)
	if err != nil {
		return nil, err
	}

	tracker.onMark = l.recordCheckpointTelemetry
	l.recordCheckpointTelemetry(tracker.currentCheckpoint())

	return tracker, nil
}

func (l *summariseSpoolLoader) enforceFullFilterAmplificationGateBeforePublish(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhasePostSpoolPublishComplete) {
		return nil
	}

	return l.enforceFullFilterAmplificationGate(parent)
}

func (l *summariseSpoolLoader) ensureTablesLoaded(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
) error {
	retry, err := l.ensureSnapshotPrepared(parent, tracker)
	if err != nil {
		return err
	}

	if err := l.loadTablesWithTracker(parent, tracker, retry); err != nil {
		return err
	}

	tracker.legacyCoarseTables = false
	if tracker.done(summariseSpoolPublishPhaseTablesLoaded) {
		return nil
	}

	return tracker.mark(summariseSpoolPublishPhaseTablesLoaded)
}

func (l *summariseSpoolLoader) ensureSnapshotPrepared(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
) (bool, error) {
	if tracker.done(summariseSpoolPublishPhaseSnapshotPrepared) {
		return true, nil
	}

	if err := l.prepareSnapshot(parent); err != nil {
		return false, err
	}

	return false, tracker.mark(summariseSpoolPublishPhaseSnapshotPrepared)
}

func (l *summariseSpoolLoader) queryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return queryContext(parent, queryTimeout(l.cfg))
}

func (l *summariseSpoolLoader) prepareSnapshot(parent context.Context) error {
	ctx, cancel := l.queryContext(parent)
	defer cancel()

	if err := refuseActiveSnapshotRewrite(ctx, l.conn, l.manifest.MountPath, l.snapshot); err != nil {
		return err
	}

	return l.timeImportPhaseContext(parent, importPhasePartitionDropReset, func() error {
		if err := dropSnapshotPartitionsForMount(
			parent,
			l.conn,
			l.manifest.MountPath,
			l.snapshot.String(),
			allPartitionDropQueries(),
		); err != nil {
			return err
		}

		return l.dropSpoolActiveVirtualPartitions(parent)
	})
}

func (l *summariseSpoolLoader) dropSpoolActiveVirtualPartitions(parent context.Context) error {
	activeSetIDs, err := l.spoolActiveSetIDs()
	if err != nil {
		return err
	}

	for _, activeSetID := range activeSetIDs {
		for _, query := range activeVirtualPartitionDropQueries() {
			if err := dropActiveSetPartition(parent, l.conn, query, activeSetID, "active-virtual"); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *summariseSpoolLoader) spoolActiveSetIDs() ([]string, error) {
	ids := make(map[string]struct{})
	if err := decodeSpoolActiveVirtualSetIDs(l.dir, ids); err != nil {
		return nil, err
	}

	return sortedSpoolActiveSetIDs(ids), nil
}

func decodeSpoolActiveVirtualSetIDs(dir string, ids map[string]struct{}) error {
	decoders := []func() error{
		func() error {
			return decodeSpoolActiveSetIDs(dir, ids, chspool.TableActiveVirtualDirs,
				func(row chspool.ActiveVirtualDirRow) string { return row.ActiveSetID })
		},
		func() error {
			return decodeSpoolActiveSetIDs(dir, ids, chspool.TableActiveVirtualSummaries,
				func(row chspool.ActiveVirtualSummaryRow) string { return row.ActiveSetID })
		},
		func() error {
			return decodeSpoolActiveSetIDs(dir, ids, chspool.TableActiveVirtualFilterAll,
				func(row chspool.ActiveVirtualFilterAllRow) string { return row.ActiveSetID })
		},
		func() error {
			return decodeSpoolActiveSetIDs(dir, ids, chspool.TableActiveVirtualChildren,
				func(row chspool.ActiveVirtualChildRow) string { return row.ActiveSetID })
		},
		func() error {
			return decodeSpoolActiveSetIDs(dir, ids, chspool.TableActiveVirtualSets,
				func(row chspool.ActiveVirtualSetRow) string { return row.ActiveSetID })
		},
	}

	for _, decode := range decoders {
		if err := decode(); err != nil {
			return err
		}
	}

	return nil
}

func sortedSpoolActiveSetIDs(ids map[string]struct{}) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}

	sort.Strings(out)

	return out
}

func (l *summariseSpoolLoader) validateSpoolActiveVirtualCatalog() error {
	catalog, err := l.spoolActiveVirtualCatalogIDs()
	if err != nil {
		return err
	}

	if err := l.validateSpoolActiveVirtualSummaryCatalog(catalog); err != nil {
		return err
	}

	if err := l.validateSpoolActiveVirtualFilterCatalog(catalog); err != nil {
		return err
	}

	return l.validateSpoolActiveVirtualChildCatalog(catalog)
}

func (l *summariseSpoolLoader) spoolActiveVirtualCatalogIDs() (map[string]map[uint32]struct{}, error) {
	catalog := make(map[string]map[uint32]struct{})
	err := chspool.DecodeRows[chspool.ActiveVirtualDirRow](
		l.dir,
		chspool.TableActiveVirtualDirs,
		func(row chspool.ActiveVirtualDirRow) error {
			if catalog[row.ActiveSetID] == nil {
				catalog[row.ActiveSetID] = make(map[uint32]struct{})
			}

			catalog[row.ActiveSetID][row.VirtualID] = struct{}{}

			return nil
		},
	)

	return catalog, err
}

func (l *summariseSpoolLoader) validateSpoolActiveVirtualSummaryCatalog(
	catalog map[string]map[uint32]struct{},
) error {
	return chspool.DecodeRows[chspool.ActiveVirtualSummaryRow](
		l.dir,
		chspool.TableActiveVirtualSummaries,
		func(row chspool.ActiveVirtualSummaryRow) error {
			return validateSpoolActiveVirtualCatalogID(
				catalog,
				row.ActiveSetID,
				row.VirtualID,
				chspool.TableActiveVirtualSummaries,
			)
		},
	)
}

func validateSpoolActiveVirtualCatalogID(
	catalog map[string]map[uint32]struct{},
	activeSetID string,
	virtualID uint32,
	table string,
) error {
	if _, ok := catalog[activeSetID][virtualID]; ok {
		return nil
	}

	return fmt.Errorf(
		"%w: %s active_set_id=%s virtual_id=%d missing wrstat_active_virtual_dirs row",
		errInvalidSummariseSpoolManifest,
		table,
		activeSetID,
		virtualID,
	)
}

func (l *summariseSpoolLoader) validateSpoolActiveVirtualFilterCatalog(
	catalog map[string]map[uint32]struct{},
) error {
	return chspool.DecodeRows[chspool.ActiveVirtualFilterAllRow](
		l.dir,
		chspool.TableActiveVirtualFilterAll,
		func(row chspool.ActiveVirtualFilterAllRow) error {
			return validateSpoolActiveVirtualCatalogID(
				catalog,
				row.ActiveSetID,
				row.VirtualID,
				chspool.TableActiveVirtualFilterAll,
			)
		},
	)
}

func (l *summariseSpoolLoader) validateSpoolActiveVirtualChildCatalog(
	catalog map[string]map[uint32]struct{},
) error {
	return chspool.DecodeRows[chspool.ActiveVirtualChildRow](
		l.dir,
		chspool.TableActiveVirtualChildren,
		func(row chspool.ActiveVirtualChildRow) error {
			if err := validateSpoolActiveVirtualCatalogID(
				catalog,
				row.ActiveSetID,
				row.ParentVirtualID,
				chspool.TableActiveVirtualChildren,
			); err != nil {
				return err
			}

			return validateSpoolActiveVirtualCatalogID(
				catalog,
				row.ActiveSetID,
				row.ChildVirtualID,
				chspool.TableActiveVirtualChildren,
			)
		},
	)
}

func (l *summariseSpoolLoader) loadActiveVirtualDirs(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableActiveVirtualDirs, func(
		batch driver.Batch,
		row chspool.ActiveVirtualDirRow,
	) error {
		return batch.Append(
			row.ActiveSetID,
			row.VirtualID,
			row.ParentID,
			row.Name,
			row.FullPath,
			row.MountPath,
			row.SnapshotID,
			row.MountRootDirID,
			row.IsMountRootBox,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) deriveChildFilterAll(parent context.Context) error {
	return l.timeImportPhaseContext(parent, importPhaseChildFilterAllInsert, func() error {
		ctx, cancel := l.queryContext(parent)
		defer cancel()

		if err := deriveChildFilterAll(ctx, l.conn, l.manifest.MountPath, l.snapshot.String()); err != nil {
			return err
		}

		return l.verifyDerivedChildFilterAllRows(parent)
	})
}

func (l *summariseSpoolLoader) verifyDerivedChildFilterAllRows(parent context.Context) error {
	dirRows, err := l.countLoadedRows(parent, chspool.TableDirFilterAll)
	if err != nil {
		return err
	}

	childRows, err := l.countLoadedRows(parent, chspool.TableChildFilterAll)
	if err != nil {
		return err
	}

	if childRows != dirRows {
		return fmt.Errorf(
			"%w: table=%s got=%d expected=%d",
			errSpoolLoadedRowsMismatch,
			chspool.TableChildFilterAll,
			childRows,
			dirRows,
		)
	}

	l.recordLoadedRows(chspool.TableChildFilterAll, childRows)

	return nil
}

func (l *summariseSpoolLoader) enforceFullFilterAmplificationGate(parent context.Context) error {
	if l.amplificationGated {
		return nil
	}

	if !l.requiresFullFilterAmplificationGate() {
		return nil
	}

	stats, err := l.loadScopedFullFilterAmplificationStats(parent)
	if err != nil {
		return summariseSpoolAmplificationStatsUnavailable(l.manifest, err)
	}

	if err := summariseSpoolCheckFullFilterAmplification(parent, l.manifest, stats); err != nil {
		return err
	}

	l.amplificationGated = true

	return nil
}

func summariseSpoolAmplificationStatsUnavailable(manifest *chspool.Manifest, err error) error {
	var mountPath, snapshotID string
	if manifest != nil {
		mountPath = manifest.MountPath
		snapshotID = manifest.SnapshotID
	}

	return fmt.Errorf(
		"%w: mount_path=%s snapshot_id=%s: %w",
		ErrFilterAmplificationStatsUnavailable,
		mountPath,
		snapshotID,
		err,
	)
}

func summariseSpoolCheckFullFilterAmplification(
	parent context.Context,
	manifest *chspool.Manifest,
	stats map[string]perfreport.TableStats,
) error {
	amplification := summariseSpoolFullFilterAmplificationFromStats(stats)
	if !amplification.exceedsWarnThreshold() {
		return nil
	}

	summariseSpoolLogFullFilterAmplification(parent, manifest, amplification)

	return nil
}

func (l *summariseSpoolLoader) loadScopedFullFilterAmplificationStats(
	parent context.Context,
) (map[string]perfreport.TableStats, error) {
	counts, ok := l.loadedFullFilterAmplificationRows()
	if !ok {
		var err error

		counts, err = l.countScopedFullFilterAmplificationRows(parent)
		if err != nil {
			return nil, err
		}
	}

	stats := summariseSpoolFullFilterAmplificationStatsFromRows(counts)
	if err := summariseSpoolValidateFullFilterAmplificationStats(stats); err != nil {
		return nil, err
	}

	summariseSpoolAddRowAmplification(stats)

	return stats, nil
}

func summariseSpoolFullFilterAmplificationStatsFromRows(
	counts map[string]uint64,
) map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats, len(counts))
	for table, rows := range counts {
		stats[table] = perfreport.TableStats{Rows: rows}
	}

	return stats
}

func summariseSpoolValidateFullFilterAmplificationStats(stats map[string]perfreport.TableStats) error {
	for _, table := range summariseSpoolFullFilterAmplificationTables() {
		tableStats, ok := stats[table]
		if !ok || tableStats.Rows == 0 {
			return fmt.Errorf("%w: table=%s", errSummariseSpoolAmplificationStatsMissing, table)
		}
	}

	return nil
}

func summariseSpoolAddRowAmplification(stats map[string]perfreport.TableStats) {
	dirFactsRows := stats[chspool.TableDirFacts].Rows
	catalogRows := stats[chspool.TableDirs].Rows

	for table, tableStats := range stats {
		tableStats.RowAmplificationVsDirFacts = summariseSpoolRowAmplification(tableStats.Rows, dirFactsRows)
		tableStats.RowAmplificationVsCatalog = summariseSpoolRowAmplification(tableStats.Rows, catalogRows)
		stats[table] = tableStats
	}
}

func (l *summariseSpoolLoader) loadedFullFilterAmplificationRows() (map[string]uint64, bool) {
	counts := make(map[string]uint64, len(summariseSpoolFullFilterAmplificationTables()))
	for _, table := range summariseSpoolFullFilterAmplificationTables() {
		rows := l.loadedRows[table]
		if rows == 0 {
			return nil, false
		}

		counts[table] = rows
	}

	return counts, true
}

func summariseSpoolFullFilterAmplificationTables() []string {
	return []string{
		chspool.TableDirs,
		chspool.TableDirFacts,
		chspool.TableDirFilterAll,
		chspool.TableChildFilterAll,
	}
}

func (l *summariseSpoolLoader) countScopedFullFilterAmplificationRows(
	parent context.Context,
) (map[string]uint64, error) {
	counts := make(map[string]uint64, len(summariseSpoolFullFilterAmplificationTables()))
	for _, table := range summariseSpoolFullFilterAmplificationTables() {
		rows, err := l.countLoadedRows(parent, table)
		if err != nil {
			return nil, err
		}

		counts[table] = rows
	}

	return counts, nil
}

func (l *summariseSpoolLoader) requiresFullFilterAmplificationGate() bool {
	return l.loadedRows[chspool.TableDirFilterAll] > 0
}

func (l *summariseSpoolLoader) beginImportTelemetry(phase string) {
	if l.telemetryRecorder == nil {
		return
	}

	now := l.telemetryTime()
	l.telemetryMu.Lock()
	l.telemetry.Phase = phase
	l.telemetry.PhaseRows = 0
	l.telemetry.PhaseElapsed = 0
	l.telemetry.ServerPartCountAvailable = false
	l.telemetry.ServerActiveMergesAvailable = false
	l.telemetry.ServerMemoryBytesAvailable = false
	l.telemetry.ServerQueryLatencyAvailable = false
	l.telemetry.ServerPressureBackoff = false
	l.telemetryStarted = now
	l.telemetryMu.Unlock()
	l.emitImportTelemetryAt(now)
}

func (l *summariseSpoolLoader) recordBatchTelemetry(
	phase string,
	measurement importBatchMeasurement,
) {
	table := strings.TrimPrefix(phase, summariseSpoolLoadPhasePrefix)

	if l.batchMeasurements == nil {
		l.batchMeasurements = make(map[string][]importBatchMeasurement)
	}

	l.batchMeasurements[table] = append(l.batchMeasurements[table], measurement)

	if l.telemetryRecorder == nil {
		return
	}

	l.telemetryMu.Lock()
	l.telemetry.Phase = phase
	l.telemetry.RowsSent += measurement.Rows
	l.telemetry.BatchCount++
	l.telemetry.PhaseRows += measurement.Rows
	l.telemetry.EstimatedUncompressedBytesSent = saturatingAddUint64(
		l.telemetry.EstimatedUncompressedBytesSent,
		measurement.EstimatedUncompressedBytes,
	)
	l.telemetry.LastBatchEstimatedUncompressedBytes = measurement.EstimatedUncompressedBytes
	l.telemetryMu.Unlock()
	l.emitImportTelemetry()
}

func (l *summariseSpoolLoader) waitForServerPressure(parent context.Context, table string) error {
	parent = loadParentContext(parent)
	if !summarisePressureEnabled(l.cfg) {
		return parent.Err()
	}

	for {
		pressure, err := l.readServerPressure(parent, table)
		if err != nil {
			return err
		}

		l.recordPressureTelemetry(pressure)

		if !pressure.exceeds(l.cfg) {
			return nil
		}

		if err := waitForSummarisePressurePoll(
			parent,
			summarisePressurePollInterval(l.cfg),
			l.pressurePollTimer,
		); err != nil {
			return err
		}
	}
}

func (l *summariseSpoolLoader) readServerPressure(
	parent context.Context,
	table string,
) (summariseServerPressure, error) {
	if err := parent.Err(); err != nil {
		return summariseServerPressure{}, err
	}

	var pressure summariseServerPressure
	if l.pressureProbe != nil {
		pressure = l.pressureProbe(parent, table)
	} else {
		pressure = l.queryServerPressure(parent, table)
	}

	return pressure, parent.Err()
}

func (l *summariseSpoolLoader) queryServerPressure(
	parent context.Context,
	table string,
) summariseServerPressure {
	ctx, cancel := queryContext(parent, min(queryTimeout(l.cfg), summariseSpoolPartTelemetryTimeout))
	defer cancel()

	const query = "SELECT " +
		"(SELECT toUInt64(count()) FROM system.parts WHERE database = ? AND table = ? AND active), " +
		"(SELECT toUInt64(count()) FROM system.merges WHERE database = ? AND table = ?), " +
		"toUInt64(ifNull((SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'), 0))"

	started := l.telemetryTime()
	pressure := summariseServerPressure{}

	err := l.conn.QueryRow(ctx, query, l.cfg.Database, table, l.cfg.Database, table).Scan(
		&pressure.ActiveParts,
		&pressure.ActiveMerges,
		&pressure.MemoryBytes,
	)
	if err != nil {
		return pressure
	}

	pressure.ActivePartsAvailable = true
	pressure.ActiveMergesAvailable = true
	pressure.MemoryBytesAvailable = true
	pressure.QueryLatency = l.telemetryTime().Sub(started)
	pressure.QueryLatencyAvailable = true

	return pressure
}

func (l *summariseSpoolLoader) recordPressureTelemetry(pressure summariseServerPressure) {
	if l.telemetryRecorder == nil {
		return
	}

	l.telemetryMu.Lock()
	l.telemetry.ServerPartCount = pressure.ActiveParts
	l.telemetry.ServerPartCountAvailable = pressure.ActivePartsAvailable
	l.telemetry.ServerActiveMerges = pressure.ActiveMerges
	l.telemetry.ServerActiveMergesAvailable = pressure.ActiveMergesAvailable
	l.telemetry.ServerMemoryBytes = pressure.MemoryBytes
	l.telemetry.ServerMemoryBytesAvailable = pressure.MemoryBytesAvailable
	l.telemetry.ServerQueryLatency = pressure.QueryLatency
	l.telemetry.ServerQueryLatencyAvailable = pressure.QueryLatencyAvailable
	l.telemetry.ServerPressureBackoff = pressure.exceeds(l.cfg)
	l.telemetryMu.Unlock()
	l.emitImportTelemetry()
}

func (l *summariseSpoolLoader) recordCheckpointTelemetry(checkpoint string) {
	if l.telemetryRecorder == nil {
		return
	}

	l.telemetryMu.Lock()
	l.telemetry.CurrentCheckpoint = checkpoint
	l.telemetryMu.Unlock()
	l.emitImportTelemetry()
}

func (l *summariseSpoolLoader) recordServerProgress(progress *proto.Progress) {
	if l.telemetryRecorder == nil || progress == nil {
		return
	}

	if progress.WroteBytes == 0 {
		return
	}

	l.telemetryMu.Lock()
	l.telemetry.BytesSentAvailable = true
	l.telemetry.BytesSent += progress.WroteBytes
	l.telemetryMu.Unlock()
}

func (l *summariseSpoolLoader) emitImportTelemetry() {
	l.emitImportTelemetryAt(l.telemetryTime())
}

func (l *summariseSpoolLoader) emitImportTelemetryAt(now time.Time) {
	if l.telemetryRecorder == nil {
		return
	}

	l.telemetryMu.Lock()
	if !l.telemetryStarted.IsZero() {
		elapsed := now.Sub(l.telemetryStarted)
		if elapsed > l.telemetry.PhaseElapsed {
			l.telemetry.PhaseElapsed = elapsed
		}
	}

	snapshot := l.telemetry
	l.telemetryMu.Unlock()

	l.telemetryEmitMu.Lock()
	l.telemetryRecorder(snapshot)
	l.telemetryEmitMu.Unlock()
}

func (l *summariseSpoolLoader) telemetryTime() time.Time {
	if l.telemetryNow != nil {
		return l.telemetryNow()
	}

	return time.Now()
}

func (l *summariseSpoolLoader) recordServerPartCount(parent context.Context, table string) {
	if l.telemetryRecorder == nil {
		return
	}

	ctx, cancel := queryContext(parent, min(queryTimeout(l.cfg), summariseSpoolPartTelemetryTimeout))
	defer cancel()

	const query = "SELECT toUInt64(count()) FROM system.parts WHERE database = ? AND table = ? AND active"

	var count uint64
	if err := l.conn.QueryRow(ctx, query, l.cfg.Database, table).Scan(&count); err == nil {
		l.telemetryMu.Lock()
		l.telemetry.ServerPartCount = count
		l.telemetry.ServerPartCountAvailable = true
		l.telemetryMu.Unlock()
	}

	l.emitImportTelemetry()
}

func (l *summariseSpoolLoader) timeImportPhaseWithTelemetry(
	parent context.Context,
	phase string,
	telemetryPhase string,
	fn func() error,
) error {
	l.beginImportTelemetry(telemetryPhase)
	stopLiveTelemetry := l.startLiveImportTelemetry(parent)

	err := timeImportPhase(func(p string, d time.Duration) {
		recordImportPhase(l.importPhaseRecorder, p, d)
	}, phase, fn)

	stopLiveTelemetry()
	l.emitImportTelemetry()

	return err
}

func (l *summariseSpoolLoader) startLiveImportTelemetry(parent context.Context) func() {
	if l.telemetryRecorder == nil {
		return func() {}
	}

	ticker := l.telemetryTicker(summariseSpoolLiveTelemetryInterval)
	stop := make(chan struct{})
	done := make(chan struct{})

	go l.runLiveImportTelemetry(parent, ticker, stop, done)

	return func() {
		close(stop)
		<-done
	}
}

func (l *summariseSpoolLoader) runLiveImportTelemetry(
	parent context.Context,
	ticker summariseSpoolTelemetryTicker,
	stop <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)
	defer ticker.stop()

	for {
		select {
		case now, ok := <-ticker.channel():
			if !ok {
				return
			}

			l.emitImportTelemetryAt(now)
		case <-parent.Done():
			return
		case <-stop:
			return
		}
	}
}

func (l *summariseSpoolLoader) timeImportPhaseContext(
	parent context.Context,
	phase string,
	fn func() error,
) error {
	return l.timeImportPhaseWithTelemetry(parent, phase, phase, fn)
}

func (l *summariseSpoolLoader) newDGUTAWriter(parent context.Context) *dgutaWriter {
	return &dgutaWriter{
		cfg:                 l.cfg,
		conn:                l.conn,
		mountPath:           l.manifest.MountPath,
		updatedAt:           l.updatedAt,
		snapshot:            l.snapshot,
		importPhaseRecorder: l.importPhaseRecorder,
		importPhaseRunner: func(phase string, fn func() error) error {
			return l.timeImportPhaseContext(parent, phase, fn)
		},
	}
}

func (l *summariseSpoolLoader) loadTablesWithTracker( //nolint:funlen,gocyclo,gocognit
	ctx context.Context,
	tracker *summariseSpoolPublishTracker,
	retry bool,
) error {
	tableLoads := []struct {
		table string
		load  func(context.Context) error
	}{
		{chspool.TableDirs, l.loadDirs},
		{chspool.TableDirFacts, l.loadDirFacts},
		{chspool.TableDirFilterAgeAll, l.loadDirFilterAgeAll},
		{chspool.TableDirFilterAll, l.loadDirFilterAll},
	}
	for _, tableLoad := range tableLoads {
		if err := l.loadCheckpointedTable(ctx, tracker, tableLoad.table, retry, tableLoad.load); err != nil {
			return err
		}
	}

	if err := l.loadCheckpointedChildFilterAll(ctx, tracker, retry); err != nil {
		return err
	}

	if err := l.enforceFullFilterAmplificationGate(ctx); err != nil {
		return err
	}

	tableLoads = []struct {
		table string
		load  func(context.Context) error
	}{
		{chspool.TableFiles, l.loadFiles},
		{chspool.TableDirProjectionSets, l.loadDirProjectionSets},
		{chspool.TableSchema3SnapshotSets, l.loadSchema3SnapshotSets},
		{chspool.TableActiveVirtualDirs, l.loadActiveVirtualDirs},
		{chspool.TableActiveVirtualSummaries, l.loadActiveVirtualSummaries},
		{chspool.TableActiveVirtualFilterAll, l.loadActiveVirtualFilterAll},
		{chspool.TableActiveVirtualChildren, l.loadActiveVirtualChildren},
	}
	for _, tableLoad := range tableLoads {
		if err := l.loadCheckpointedTable(ctx, tracker, tableLoad.table, retry, tableLoad.load); err != nil {
			return err
		}
	}

	if err := l.validateSpoolActiveVirtualCatalog(); err != nil {
		return err
	}

	tableLoads = []struct {
		table string
		load  func(context.Context) error
	}{
		{chspool.TableActiveVirtualSets, l.loadActiveVirtualSets},
		{chspool.TableBasedirsHistory, l.loadBasedirsHistory},
		{chspool.TableBasedirsGroupUsage, l.loadBasedirsGroupUsage},
		{chspool.TableBasedirsUserUsage, l.loadBasedirsUserUsage},
		{chspool.TableBasedirsGroupSubdirs, func(parent context.Context) error {
			return l.loadBasedirsSubdirs(parent, chspool.TableBasedirsGroupSubdirs)
		}},
		{chspool.TableBasedirsUserSubdirs, func(parent context.Context) error {
			return l.loadBasedirsSubdirs(parent, chspool.TableBasedirsUserSubdirs)
		}},
	}
	for _, tableLoad := range tableLoads {
		if err := l.loadCheckpointedTable(ctx, tracker, tableLoad.table, retry, tableLoad.load); err != nil {
			return err
		}
	}

	return nil
}

func (l *summariseSpoolLoader) loadCheckpointedTable(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
	table string,
	retry bool,
	load func(context.Context) error,
) error {
	expected := l.manifest.Tables[table].Rows

	verified, err := l.checkPersistedTableCheckpoint(parent, tracker, table, expected)
	if err != nil || verified {
		return err
	}

	if err := l.resetIncompleteSpoolTable(parent, table, retry); err != nil {
		return err
	}

	if expected == 0 {
		return tracker.markTable(table, 0)
	}

	if err := load(parent); err != nil {
		return err
	}

	if err := l.verifyLoadedCount(parent, table, expected); err != nil {
		return err
	}

	return tracker.markTable(table, expected)
}

func (l *summariseSpoolLoader) resetIncompleteSpoolTable(
	parent context.Context,
	table string,
	retry bool,
) error {
	if !retry || table == chspool.TableBasedirsHistory {
		return nil
	}

	return l.resetSpoolTable(parent, table)
}

func (l *summariseSpoolLoader) checkPersistedTableCheckpoint(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
	table string,
	expected uint64,
) (bool, error) {
	checkpointed := tracker.tableDone(table, expected)
	if !checkpointed && !tracker.legacyCoarseTables {
		return false, nil
	}

	got, err := l.countLoadedRows(parent, table)
	if err != nil {
		return false, err
	}

	if got != expected {
		if !checkpointed {
			return false, nil
		}

		return false, tracker.clearTable(table)
	}

	l.recordLoadedRows(table, got)

	if checkpointed {
		return true, nil
	}

	return true, tracker.markTable(table, expected)
}

func (l *summariseSpoolLoader) checkPersistedChildFilterAllCheckpoint(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
	expected uint64,
) (bool, error) {
	checkpointed := tracker.operationDone(expected)
	if !checkpointed && !tracker.legacyCoarseTables {
		return false, nil
	}

	got, err := l.countLoadedRows(parent, chspool.TableChildFilterAll)
	if err != nil {
		return false, err
	}

	if got != expected {
		if !checkpointed {
			return false, nil
		}

		return false, tracker.clearOperation(summariseSpoolPublishOperationChildFilterAll)
	}

	l.recordLoadedRows(chspool.TableChildFilterAll, got)

	if checkpointed {
		return true, nil
	}

	return true, tracker.markOperation(expected)
}

func (l *summariseSpoolLoader) loadCheckpointedChildFilterAll(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
	retry bool,
) error {
	expected := l.manifest.Tables[chspool.TableDirFilterAll].Rows

	verified, err := l.checkPersistedChildFilterAllCheckpoint(parent, tracker, expected)
	if err != nil || verified {
		return err
	}

	if retry {
		if err := l.resetSpoolTable(parent, chspool.TableChildFilterAll); err != nil {
			return err
		}
	}

	if expected == 0 {
		return tracker.markOperation(0)
	}

	if err := l.deriveChildFilterAll(parent); err != nil {
		return err
	}

	return tracker.markOperation(expected)
}

func (l *summariseSpoolLoader) verifyLoadedCount(
	parent context.Context,
	table string,
	expected uint64,
) error {
	got, err := l.countLoadedRows(parent, table)
	if err != nil {
		return err
	}

	if got != expected {
		return fmt.Errorf(
			"%w: table=%s got=%d expected=%d",
			errSpoolLoadedRowsMismatch,
			table,
			got,
			expected,
		)
	}

	l.recordLoadedRows(table, got)

	return nil
}

func (l *summariseSpoolLoader) resetSpoolTable(parent context.Context, table string) error {
	if query, ok := summariseSpoolSnapshotDropQueries()[table]; ok {
		return dropPartitionIgnoreUnknown(parent, l.conn, l.manifest.MountPath, l.snapshot.String(), query)
	}

	query, ok := summariseSpoolActiveVirtualDropQueries()[table]
	if !ok {
		return fmt.Errorf("%w: %s", errUnknownSpoolLoadTable, table)
	}

	activeSetIDs, err := l.spoolActiveSetIDs()
	if err != nil {
		return err
	}

	for _, activeSetID := range activeSetIDs {
		if err := dropActiveSetPartition(parent, l.conn, query, activeSetID, table); err != nil {
			return err
		}
	}

	return nil
}

func summariseSpoolSnapshotDropQueries() map[string]string {
	return map[string]string{
		chspool.TableDirs:                 dropDirsPartitionQuery,
		chspool.TableFiles:                dropFilesPartitionQuery,
		chspool.TableDirFacts:             dropDirSummaryPartitionQuery,
		chspool.TableDirFilterAgeAll:      dropDirFilterAgeAllPartitionQuery,
		chspool.TableChildFilterAll:       dropChildFilterAllPartitionQuery,
		chspool.TableDirFilterAll:         dropDirFilterAllPartitionQuery,
		chspool.TableSchema3SnapshotSets:  dropSchema3SnapshotSetPartitionQuery,
		chspool.TableDirProjectionSets:    dropDirSummarySetPartitionQuery,
		chspool.TableBasedirsGroupUsage:   dropBasedirsGroupUsagePartitionQuery,
		chspool.TableBasedirsUserUsage:    dropBasedirsUserUsagePartitionQuery,
		chspool.TableBasedirsGroupSubdirs: dropBasedirsGroupSubdirsPartitionQuery,
		chspool.TableBasedirsUserSubdirs:  dropBasedirsUserSubdirsPartitionQuery,
	}
}

func summariseSpoolActiveVirtualDropQueries() map[string]string {
	return map[string]string{
		chspool.TableActiveVirtualDirs:      dropActiveVirtualDirsPartitionQuery,
		chspool.TableActiveVirtualSummaries: dropActiveVirtualSummariesPartitionQuery,
		chspool.TableActiveVirtualFilterAll: dropActiveVirtualFilterAllPartitionQuery,
		chspool.TableActiveVirtualChildren:  dropActiveVirtualChildrenPartitionQuery,
		chspool.TableActiveVirtualSets:      dropActiveVirtualSetsPartitionQuery,
	}
}

func (l *summariseSpoolLoader) countLoadedBasedirsHistoryRows(parent context.Context) (uint64, error) {
	rows, err := l.readBasedirsHistoryRows()
	if err != nil {
		return 0, err
	}

	uniqueRows := compactHistoryDeleteRows(rows)

	var total uint64

	for _, mountRows := range historyRowsGroupedByMountPath(uniqueRows) {
		for chunk := range slices.Chunk(mountRows, spoolHistoryDeleteChunk) {
			count, err := l.countLoadedBasedirsHistoryChunk(parent, chunk)
			if err != nil {
				return 0, err
			}

			total += count
		}
	}

	return total, nil
}

func (l *summariseSpoolLoader) countLoadedBasedirsHistoryChunk(
	parent context.Context,
	rows []chspool.BasedirsHistoryRow,
) (uint64, error) {
	query, args := summariseSpoolHistoryCountRowsQuery(rows)

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	var count uint64

	if err := l.conn.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"clickhouse: failed to count loaded spool table %s: %w",
			chspool.TableBasedirsHistory,
			err,
		)
	}

	return count, nil
}

func summariseSpoolHistoryCountRowsQuery(rows []chspool.BasedirsHistoryRow) (string, []any) {
	query, args := summariseSpoolHistoryDeleteQuery(rows)
	query = strings.TrimSuffix(query, " SETTINGS mutations_sync = 1")
	query = strings.Replace(
		query,
		"ALTER TABLE wrstat_basedirs_history DELETE",
		"SELECT count() FROM wrstat_basedirs_history",
		1,
	)

	return query, args
}

type summariseSpoolRealTelemetryTicker struct {
	*time.Ticker
}

func (t *summariseSpoolRealTelemetryTicker) channel() <-chan time.Time {
	return t.C
}

func (t *summariseSpoolRealTelemetryTicker) stop() {
	t.Stop()
}

type summariseSpoolFullFilterAmplification struct {
	dirVsDirFacts   float64
	childVsDirFacts float64
	dirVsCatalog    float64
	childVsCatalog  float64
}

func summariseSpoolFullFilterAmplificationFromStats(
	stats map[string]perfreport.TableStats,
) summariseSpoolFullFilterAmplification {
	dirStats := stats[chspool.TableDirFilterAll]
	childStats := stats[chspool.TableChildFilterAll]

	return summariseSpoolFullFilterAmplification{
		dirVsDirFacts:   dirStats.RowAmplificationVsDirFacts,
		childVsDirFacts: childStats.RowAmplificationVsDirFacts,
		dirVsCatalog:    dirStats.RowAmplificationVsCatalog,
		childVsCatalog:  childStats.RowAmplificationVsCatalog,
	}
}

func (a summariseSpoolFullFilterAmplification) exceedsWarnThreshold() bool {
	return a.maxVsDirFacts() > summariseSpoolFilterAmplificationWarnThreshold
}

func (a summariseSpoolFullFilterAmplification) maxVsDirFacts() float64 {
	return max(a.dirVsDirFacts, a.childVsDirFacts)
}

func summariseSpoolLogFullFilterAmplification(
	parent context.Context,
	manifest *chspool.Manifest,
	amplification summariseSpoolFullFilterAmplification,
) {
	slog.WarnContext(loadParentContext(parent), "clickhouse full-filter row amplification exceeds warning threshold",
		"mount_path", manifest.MountPath,
		"snapshot_id", manifest.SnapshotID,
		"basis", "per_table_vs_wrstat_dir_facts",
		"dir_filter_table", chspool.TableDirFilterAll,
		"dir_filter_amplification_vs_dir_facts", amplification.dirVsDirFacts,
		"dir_filter_amplification_vs_catalog", amplification.dirVsCatalog,
		"child_filter_table", chspool.TableChildFilterAll,
		"child_filter_amplification_vs_dir_facts", amplification.childVsDirFacts,
		"child_filter_amplification_vs_catalog", amplification.childVsCatalog,
		"combined_amplification_vs_dir_facts", amplification.dirVsDirFacts+amplification.childVsDirFacts,
		"warn_threshold", summariseSpoolFilterAmplificationWarnThreshold,
	)
}

func (b *summariseSpoolLoadReportBuilder) captureLoadState(loader *summariseSpoolLoader) {
	b.loadedRows = summariseSpoolLoadLoadedRows(loader, b.manifest)
	b.batchMeasurements = cloneImportBatchMeasurements(loader.batchMeasurements)
}

func cloneImportBatchMeasurements(
	measurements map[string][]importBatchMeasurement,
) map[string][]importBatchMeasurement {
	out := make(map[string][]importBatchMeasurement, len(measurements))
	for table, batches := range measurements {
		out[table] = slices.Clone(batches)
	}

	return out
}

func summariseSpoolAddImportPhaseDurations(
	stats map[string]perfreport.TableStats,
	phaseDurations map[string]time.Duration,
) {
	for phase, duration := range phaseDurations {
		for _, table := range summariseSpoolImportPhaseTables(phase) {
			tableStats, ok := stats[table]
			if !ok {
				continue
			}

			if tableStats.ImportPhaseDurationsMS == nil {
				tableStats.ImportPhaseDurationsMS = make(map[string]float64)
			}

			tableStats.ImportPhaseDurationsMS[phase] += summariseSpoolDurationMS(duration)
			stats[table] = tableStats
		}
	}
}

func importBatchEstimatedBytes(
	measurements map[string][]importBatchMeasurement,
) map[string][]uint64 {
	out := make(map[string][]uint64, len(measurements))
	for table, batches := range measurements {
		bytes := make([]uint64, 0, len(batches))
		for _, batch := range batches {
			bytes = append(bytes, batch.EstimatedUncompressedBytes)
		}

		out[table] = bytes
	}

	return out
}

// LoadSummariseSpoolReportWithTelemetry loads a spool while reporting live
// client/server publication counters in addition to completed phase timings.
func LoadSummariseSpoolReportWithTelemetry(
	ctx context.Context,
	cfg Config,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
	telemetryRecorder func(SummariseImportTelemetry),
) (perfreport.Report, error) {
	builder := newSummariseSpoolLoadReportBuilder(spoolDir, manifest, recorder)

	err := runSummariseSpoolLoad(
		ctx, cfg, spoolDir, manifest, builder.record, builder.collect, telemetryRecorder,
	)
	if err != nil {
		return builder.report, err
	}

	builder.finish()

	return builder.report, nil
}

func summariseSpoolRowAmplification(rows uint64, baselineRows uint64) float64 {
	if rows == 0 || baselineRows == 0 {
		return 0
	}

	return float64(rows) / float64(baselineRows)
}

func decodeSpoolActiveSetIDs[T any](
	dir string,
	ids map[string]struct{},
	table string,
	activeSetID func(T) string,
) error {
	return chspool.DecodeRows[T](dir, table, func(row T) error {
		addSpoolActiveSetID(ids, activeSetID(row))

		return nil
	})
}

func (l *summariseSpoolLoader) loadTables(ctx context.Context) error {
	tracker := newEmptySummariseSpoolPublishTracker("", l.manifest, summariseSpoolPublishManifestKey(l.manifest))

	return l.loadTablesWithTracker(ctx, tracker, false)
}

func (l *summariseSpoolLoader) loadDirs(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableDirs, func(batch driver.Batch, row chspool.DirRow) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.DirID,
			row.ParentID,
			row.SubtreeEnd,
			row.Depth,
			row.Name,
			row.FullPath,
			row.ChildDirCount,
			row.ChildFileCount,
			row.PathHash,
		)
	})
}

func (l *summariseSpoolLoader) loadFiles(parent context.Context) error { //nolint:funlen
	return l.loadTable(parent, chspool.TableFiles, func(ctx context.Context, writer *importBlockWriter) (uint64, error) {
		sid := l.snapshot

		var rows uint64

		err := chspool.DecodeRows[chspool.FileRow](l.dir, chspool.TableFiles, func(row chspool.FileRow) error {
			rows++

			return writer.append(ctx, func(batch driver.Batch) error {
				return batch.Append(
					row.MountPath,
					sid,
					row.DirID,
					row.Name,
					row.Ext,
					row.EntryType,
					row.Size,
					row.ApparentSize,
					row.UID,
					row.GID,
					row.ATime,
					row.MTime,
					row.CTime,
					row.Inode,
					row.Nlink,
				)
			})
		})

		return rows, errors.Join(err, writer.close())
	})
}

func (l *summariseSpoolLoader) loadDirFacts(parent context.Context) error { //nolint:funlen
	return l.loadTable(parent, chspool.TableDirFacts, func(
		ctx context.Context,
		writer *importBlockWriter,
	) (uint64, error) {
		var rows uint64

		err := chspool.DecodeRows[chspool.DirFactRow](l.dir, chspool.TableDirFacts, func(row chspool.DirFactRow) error {
			rows++

			return writer.append(ctx, func(batch driver.Batch) error {
				return batch.Append(
					row.MountPath,
					row.SnapshotID,
					row.DirID,
					row.ParentID,
					row.SubtreeEnd,
					row.UpdatedAt,
					row.AllCount,
					row.AllSize,
					row.AllAtimeMin,
					row.AllMtimeMax,
					row.AllAtimeBuckets,
					row.AllMtimeBuckets,
					row.AllUIDs,
					row.AllGIDs,
					row.AllFT,
					row.FileCount,
					row.FileSize,
					row.FileAtimeMin,
					row.FileMtimeMax,
					row.FileAtimeBuckets,
					row.FileMtimeBuckets,
					row.FileUIDs,
					row.FileGIDs,
					row.FileFT,
					row.GIDs,
					row.UIDs,
					row.FTs,
					row.Ages,
					row.Counts,
					row.Sizes,
					row.AtimeMins,
					row.MtimeMaxs,
					row.AtimeBuckets,
					row.MtimeBuckets,
					row.ChildCount,
					row.RefreshedAt,
				)
			})
		})

		return rows, errors.Join(err, writer.close())
	})
}

func loadSimpleSpoolTable[T any](
	parent context.Context,
	l *summariseSpoolLoader,
	table string,
	appendRow func(driver.Batch, T) error,
) error {
	return l.loadTable(parent, table, func(ctx context.Context, writer *importBlockWriter) (uint64, error) {
		var rows uint64

		err := chspool.DecodeRows[T](l.dir, table, func(row T) error {
			rows++

			return writer.append(ctx, func(batch driver.Batch) error {
				return appendRow(batch, row)
			})
		})

		return rows, errors.Join(err, writer.close())
	})
}

func (l *summariseSpoolLoader) loadDirFilterAgeAll(parent context.Context) error {
	return loadSimpleSpoolTable(
		parent,
		l,
		chspool.TableDirFilterAgeAll,
		func(batch driver.Batch, row chspool.DirFilterAgeAllRow) error {
			return batch.Append(
				row.MountPath,
				row.SnapshotID,
				row.GID,
				row.UID,
				row.FT,
				row.DirID,
				row.SubtreeEnd,
				row.Count,
				row.Size,
				row.AtimeMin,
				row.MtimeMax,
				row.AtimeBuckets,
				row.MtimeBuckets,
				row.RefreshedAt,
			)
		},
	)
}

func (l *summariseSpoolLoader) loadDirFilterAll(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableDirFilterAll, func(
		batch driver.Batch,
		row chspool.DirFilterAllRow,
	) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.Age,
			row.GID,
			row.UID,
			row.FT,
			row.DirID,
			row.SubtreeEnd,
			row.Count,
			row.Size,
			row.AtimeMin,
			row.MtimeMax,
			row.AtimeBuckets,
			row.MtimeBuckets,
			row.FilterChildCount,
			row.ChildCount,
			row.HasFilterChildren,
			row.HasChildren,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadDirProjectionSets(parent context.Context) error {
	return loadSimpleSpoolTable(
		parent,
		l,
		chspool.TableDirProjectionSets,
		func(batch driver.Batch, row chspool.DirProjectionSetRow) error {
			return batch.Append(row.MountPath, row.SnapshotID, row.UpdatedAt, row.RefreshedAt)
		},
	)
}

func (l *summariseSpoolLoader) loadSchema3SnapshotSets(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableSchema3SnapshotSets, func(
		batch driver.Batch,
		row chspool.Schema3SnapshotSetRow,
	) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.Schema3Version,
			row.DirsRows,
			row.DirFactsRows,
			row.ChildFilterAllRows,
			row.DirFilterAllRows,
			row.ManifestSHA256,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadActiveVirtualSummaries(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableActiveVirtualSummaries, func(
		batch driver.Batch,
		row chspool.ActiveVirtualSummaryRow,
	) error {
		return batch.Append(
			row.ActiveSetID,
			row.VirtualID,
			row.MountPath,
			row.SnapshotID,
			row.MountRootDirID,
			row.IsMountRootBox,
			row.UpdatedAt,
			row.AllCount,
			row.AllSize,
			row.AllAtimeMin,
			row.AllMtimeMax,
			row.AllAtimeBuckets,
			row.AllMtimeBuckets,
			row.AllUIDs,
			row.AllGIDs,
			row.AllFT,
			row.FileCount,
			row.FileSize,
			row.ChildCount,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadActiveVirtualFilterAll(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableActiveVirtualFilterAll, func(
		batch driver.Batch,
		row chspool.ActiveVirtualFilterAllRow,
	) error {
		return batch.Append(
			row.ActiveSetID,
			row.VirtualID,
			row.Age,
			row.GID,
			row.UID,
			row.FT,
			row.Count,
			row.Size,
			row.AtimeMin,
			row.MtimeMax,
			row.AtimeBuckets,
			row.MtimeBuckets,
			row.FilterChildCount,
			row.ChildCount,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadActiveVirtualChildren(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableActiveVirtualChildren, func(
		batch driver.Batch,
		row chspool.ActiveVirtualChildRow,
	) error {
		return batch.Append(
			row.ActiveSetID,
			row.ParentVirtualID,
			row.ChildVirtualID,
			row.MountPath,
			row.SnapshotID,
			row.MountRootDirID,
			row.IsMountRootBox,
			row.ChildCount,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadActiveVirtualSets(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableActiveVirtualSets, func(
		batch driver.Batch,
		row chspool.ActiveVirtualSetRow,
	) error {
		return batch.Append(
			row.ActiveSetID,
			row.Schema3Version,
			row.MountsSHA256,
			row.ActiveMountCount,
			row.SummaryRows,
			row.FilterRows,
			row.ChildRows,
			row.ManifestSHA256,
			row.Ready,
			row.RefreshedAt,
		)
	})
}

func (l *summariseSpoolLoader) loadBasedirsHistory(parent context.Context) error {
	rows, err := l.readBasedirsHistoryRows()
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	if err := l.deleteManifestHistoryRows(parent, rows); err != nil {
		return err
	}

	if err := l.insertEligibleHistoryRows(parent, rows); err != nil {
		return err
	}

	l.recordLoadedRows(chspool.TableBasedirsHistory, uint64(len(rows)))

	return nil
}

func (l *summariseSpoolLoader) readBasedirsHistoryRows() ([]chspool.BasedirsHistoryRow, error) {
	rows := make([]chspool.BasedirsHistoryRow, 0, l.manifest.Tables[chspool.TableBasedirsHistory].Rows)

	err := chspool.DecodeRows[chspool.BasedirsHistoryRow](
		l.dir,
		chspool.TableBasedirsHistory,
		func(row chspool.BasedirsHistoryRow) error {
			rows = append(rows, row)

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MountPath != rows[j].MountPath {
			return rows[i].MountPath < rows[j].MountPath
		}

		if rows[i].GID != rows[j].GID {
			return rows[i].GID < rows[j].GID
		}

		return rows[i].Date.Before(rows[j].Date)
	})

	return rows, nil
}

func (l *summariseSpoolLoader) deleteManifestHistoryRows(
	parent context.Context,
	rows []chspool.BasedirsHistoryRow,
) error {
	uniqueRows := compactHistoryDeleteRows(rows)

	for _, mountRows := range historyRowsGroupedByMountPath(uniqueRows) {
		for chunk := range slices.Chunk(mountRows, spoolHistoryDeleteChunk) {
			query, args := summariseSpoolHistoryDeleteQuery(chunk)

			ctx, cancel := l.cleanupContext(parent)
			if err := l.conn.Exec(ctx, query, args...); err != nil {
				cancel()

				return fmt.Errorf("clickhouse: failed to delete retry basedirs history rows: %w", err)
			}

			cancel()
		}
	}

	return nil
}

func compactHistoryDeleteRows(rows []chspool.BasedirsHistoryRow) []chspool.BasedirsHistoryRow {
	if len(rows) == 0 {
		return nil
	}

	out := rows[:0]

	for _, row := range rows {
		if len(out) > 0 && sameHistoryDeleteRow(out[len(out)-1], row) {
			continue
		}

		out = append(out, row)
	}

	return out
}

func historyRowsGroupedByMountPath(rows []chspool.BasedirsHistoryRow) [][]chspool.BasedirsHistoryRow {
	groupIndexes := make(map[string]int)
	groups := make([][]chspool.BasedirsHistoryRow, 0)

	for _, row := range rows {
		index, ok := groupIndexes[row.MountPath]
		if !ok {
			index = len(groups)
			groupIndexes[row.MountPath] = index

			groups = append(groups, nil)
		}

		groups[index] = append(groups[index], row)
	}

	return groups
}

func summariseSpoolHistoryDeleteQuery(rows []chspool.BasedirsHistoryRow) (string, []any) {
	var b strings.Builder
	b.WriteString("ALTER TABLE wrstat_basedirs_history DELETE WHERE mount_path = ? AND (gid, date) IN (")

	const historyDeleteArgsPerRow = 2

	args := make([]any, 0, 1+len(rows)*historyDeleteArgsPerRow)
	args = append(args, rows[0].MountPath)

	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, ?)")

		args = append(args, row.GID, row.Date)
	}

	b.WriteString(") SETTINGS mutations_sync = 1")

	return b.String(), args
}

func (l *summariseSpoolLoader) cleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return queryContext(context.WithoutCancel(loadParentContext(parent)), activeSnapshotCleanupTimeout)
}

func (l *summariseSpoolLoader) insertEligibleHistoryRows( //nolint:funlen,gocognit
	parent context.Context,
	rows []chspool.BasedirsHistoryRow,
) error {
	return l.timeImportPhaseContext(parent, importPhaseBasedirsHistory, func() error {
		lastDates, err := l.historyLastDatesByKey(parent, rows)
		if err != nil {
			return err
		}

		var (
			batch    driver.Batch
			openedAt time.Time
			writeErr error
		)

		writer := &importBlockWriter{
			conn:       l.conn,
			query:      insertBasedirsHistoryPoint,
			name:       chspool.TableBasedirsHistory,
			batch:      &batch,
			openedAt:   &openedAt,
			writeErr:   &writeErr,
			batchSize:  defaultBatchSize,
			batchBytes: summariseSpoolBatchBytesFor(l.cfg, chspool.TableBasedirsHistory),
			onSendMeasurement: func(measurement importBatchMeasurement) {
				l.recordBatchTelemetry(
					summariseSpoolLoadPhasePrefix+chspool.TableBasedirsHistory,
					measurement,
				)
			},
			beforeBatch: func(ctx context.Context) error {
				return l.waitForServerPressure(ctx, chspool.TableBasedirsHistory)
			},
		}

		for _, row := range rows {
			key := historyLastDateKey{mountPath: row.MountPath, gid: row.GID}
			if !historyRowAfterLastDate(row, lastDates[key]) {
				continue
			}

			ctx, cancel := l.queryContext(parent)
			err := writer.append(ctx, func(batch driver.Batch) error {
				return batch.Append(
					row.MountPath,
					row.GID,
					row.Date,
					row.UsageSize,
					row.QuotaSize,
					row.UsageInodes,
					row.QuotaInodes,
				)
			})

			cancel()

			if err != nil {
				return err
			}

			lastDates[key] = row.Date
		}

		return writer.close()
	})
}

func historyRowAfterLastDate(row chspool.BasedirsHistoryRow, last time.Time) bool {
	return last.IsZero() || row.Date.After(last)
}

func (l *summariseSpoolLoader) historyLastDatesByKey(
	parent context.Context,
	rows []chspool.BasedirsHistoryRow,
) (map[historyLastDateKey]time.Time, error) {
	out := make(map[historyLastDateKey]time.Time)

	for _, mountRows := range historyRowsGroupedByMountPath(rows) {
		mountPath := mountRows[0].MountPath

		datesByGID, err := l.historyLastDatesForMountPath(parent, mountPath, uniqueHistoryGIDs(mountRows))
		if err != nil {
			return nil, err
		}

		for gid, date := range datesByGID {
			out[historyLastDateKey{mountPath: mountPath, gid: gid}] = date
		}
	}

	return out, nil
}

func uniqueHistoryGIDs(rows []chspool.BasedirsHistoryRow) []uint32 {
	seen := make(map[uint32]struct{})
	gids := make([]uint32, 0, len(rows))

	for _, row := range rows {
		if _, ok := seen[row.GID]; ok {
			continue
		}

		seen[row.GID] = struct{}{}
		gids = append(gids, row.GID)
	}

	slices.Sort(gids)

	return gids
}

func (l *summariseSpoolLoader) historyLastDatesForMountPath(
	parent context.Context,
	mountPath string,
	gids []uint32,
) (map[uint32]time.Time, error) {
	if len(gids) == 0 {
		return map[uint32]time.Time{}, nil
	}

	query, args := summariseSpoolHistoryLastDatesQuery(mountPath, gids)

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	result, err := l.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query basedirs history last dates: %w", err)
	}

	defer func() { _ = result.Close() }()

	return scanHistoryLastDatesByGID(result)
}

func summariseSpoolHistoryLastDatesQuery(mountPath string, gids []uint32) (string, []any) {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(gids)), ",")
	query := "SELECT gid, max(date) FROM wrstat_basedirs_history WHERE mount_path = ? AND gid IN (" +
		placeholders + ") GROUP BY gid"

	args := make([]any, 0, 1+len(gids))

	args = append(args, mountPath)
	for _, gid := range gids {
		args = append(args, gid)
	}

	return query, args
}

func scanHistoryLastDatesByGID(rows driver.Rows) (map[uint32]time.Time, error) {
	out := make(map[uint32]time.Time)

	for rows.Next() {
		var (
			gid  uint32
			last time.Time
		)
		if err := rows.Scan(&gid, &last); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan basedirs history last date: %w", err)
		}

		out[gid] = last
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: basedirs history last dates iteration error: %w", err)
	}

	return out, nil
}

func (l *summariseSpoolLoader) loadBasedirsGroupUsage(parent context.Context) error { //nolint:funlen
	return l.loadTable(parent, chspool.TableBasedirsGroupUsage, func(
		ctx context.Context,
		writer *importBlockWriter,
	) (uint64, error) {
		var rows uint64

		err := chspool.DecodeRows[chspool.BasedirsGroupUsageRow](
			l.dir,
			chspool.TableBasedirsGroupUsage,
			func(row chspool.BasedirsGroupUsageRow) error {
				rows++

				if db.DirGUTAge(row.Age) == db.DGUTAgeAll {
					dates, err := l.finaliseDatesForGID(ctx, row.GID)
					if err != nil {
						return err
					}

					row.DateNoSpace = dates.noSpace
					row.DateNoFiles = dates.noFiles
				}

				return writer.append(ctx, func(batch driver.Batch) error {
					return batch.Append(
						row.MountPath,
						row.SnapshotID,
						row.GID,
						row.BaseDirID,
						row.BaseDirExternal,
						row.Age,
						ensureNonNilUInt32s(row.UIDs),
						row.UsageSize,
						row.QuotaSize,
						row.UsageInodes,
						row.QuotaInodes,
						row.Mtime,
						row.DateNoSpace,
						row.DateNoFiles,
					)
				})
			},
		)

		return rows, errors.Join(err, writer.close())
	})
}

func (l *summariseSpoolLoader) finaliseDatesForGID(
	ctx context.Context,
	gid uint32,
) (finaliseQuotaDates, error) {
	dates, ok := l.groupUsageDates[gid]
	if ok {
		return dates, nil
	}

	history, err := l.readHistorySeries(ctx, gid)
	if err != nil {
		return finaliseQuotaDates{}, err
	}

	dateNoSpace, dateNoFiles := basedirs.DateQuotaFull(history)
	dates = finaliseQuotaDates{
		noSpace: unixEpochIfZero(dateNoSpace),
		noFiles: unixEpochIfZero(dateNoFiles),
	}
	l.groupUsageDates[gid] = dates

	return dates, nil
}

func (l *summariseSpoolLoader) readHistorySeries(
	parent context.Context,
	gid uint32,
) ([]basedirs.History, error) {
	ctx, cancel := l.queryContext(parent)
	defer cancel()

	rows, err := l.conn.Query(ctx, queryBasedirsHistorySeries, l.manifest.MountPath, gid)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query basedirs history series: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return collectHistoryRows(rows)
}

func (l *summariseSpoolLoader) loadBasedirsUserUsage(parent context.Context) error { //nolint:funlen
	return l.loadTable(parent, chspool.TableBasedirsUserUsage, func(
		ctx context.Context,
		writer *importBlockWriter,
	) (uint64, error) {
		var rows uint64

		err := chspool.DecodeRows[chspool.BasedirsUserUsageRow](
			l.dir,
			chspool.TableBasedirsUserUsage,
			func(row chspool.BasedirsUserUsageRow) error {
				rows++

				return writer.append(ctx, func(batch driver.Batch) error {
					return batch.Append(
						row.MountPath,
						row.SnapshotID,
						row.UID,
						row.BaseDirID,
						row.BaseDirExternal,
						row.Age,
						ensureNonNilUInt32s(row.GIDs),
						row.UsageSize,
						row.QuotaSize,
						row.UsageInodes,
						row.QuotaInodes,
						row.Mtime,
					)
				})
			},
		)

		return rows, errors.Join(err, writer.close())
	})
}

func (l *summariseSpoolLoader) loadBasedirsSubdirs(parent context.Context, table string) error { //nolint:funlen
	query := insertBasedirsGroupSubdirsQuery
	phase := importPhaseBasedirsGroupSubs

	if table == chspool.TableBasedirsUserSubdirs {
		query = insertBasedirsUserSubdirsQuery
		phase = importPhaseBasedirsUserSubs
	}

	return l.loadTableWithQuery(parent, table, query, phase, func(
		ctx context.Context,
		writer *importBlockWriter,
	) (uint64, error) {
		var rows uint64

		err := chspool.DecodeRows[chspool.BasedirsSubdirRow](l.dir, table, func(row chspool.BasedirsSubdirRow) error {
			rows++

			return writer.append(ctx, func(batch driver.Batch) error {
				return batch.Append(
					row.MountPath,
					row.SnapshotID,
					row.ID,
					row.BaseDirID,
					row.BaseDirExternal,
					row.Age,
					row.Pos,
					row.SubDirID,
					row.SubDirExternal,
					row.NumFiles,
					row.SizeFiles,
					row.LastModified,
					row.FileUsage,
				)
			})
		})

		return rows, errors.Join(err, writer.close())
	})
}

func (l *summariseSpoolLoader) loadTable(
	parent context.Context,
	table string,
	load func(context.Context, *importBlockWriter) (uint64, error),
) error {
	query, phase, err := summariseSpoolTableQuery(table)
	if err != nil {
		return err
	}

	return l.loadTableWithQuery(parent, table, query, phase, load)
}

func summariseSpoolTableQuery(table string) (string, string, error) { //nolint:gocyclo,cyclop,funlen
	switch table {
	case chspool.TableDirs:
		return insertDirsQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableFiles:
		return insertFilesBatchQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableDirFacts:
		return insertMountDirSummaryQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableDirFilterAgeAll:
		return insertDirFilterAgeAllQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableChildFilterAll:
		return insertChildFilterAllQuery, importPhaseChildFilterAllInsert, nil
	case chspool.TableDirFilterAll:
		return insertDirFilterAllQuery, importPhaseDirFilterAllInsert, nil
	case chspool.TableSchema3SnapshotSets:
		return insertSchema3SnapshotSetQuery, importPhaseSchema3Ready, nil
	case chspool.TableActiveVirtualDirs:
		return insertActiveVirtualDirQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualSummaries:
		return insertActiveVirtualSummaryQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualFilterAll:
		return insertActiveVirtualFilterAllQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualChildren:
		return insertActiveVirtualChildQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualSets:
		return insertActiveVirtualSetQuery, importPhaseActiveVirtualReady, nil
	case chspool.TableDirProjectionSets:
		return insertMountDirSummarySetQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableBasedirsGroupUsage:
		return insertBasedirsGroupUsageQuery, importPhaseBasedirsGroupUsage, nil
	case chspool.TableBasedirsUserUsage:
		return insertBasedirsUserUsageQuery, importPhaseBasedirsUserUsage, nil
	default:
		return "", "", fmt.Errorf("%w: %s", errUnknownSpoolLoadTable, table)
	}
}

func (l *summariseSpoolLoader) loadTableWithQuery( //nolint:funlen
	parent context.Context,
	table string,
	query string,
	phase string,
	load func(context.Context, *importBlockWriter) (uint64, error),
) error {
	expectedRows := l.manifest.Tables[table].Rows
	if expectedRows == 0 {
		return nil
	}

	telemetryPhase := summariseSpoolLoadPhasePrefix + table

	err := l.timeImportPhaseWithTelemetry(parent, phase, telemetryPhase, func() error {
		var (
			batch    driver.Batch
			openedAt time.Time
			writeErr error
		)

		writer := &importBlockWriter{
			conn:       l.conn,
			query:      query,
			name:       table,
			batch:      &batch,
			openedAt:   &openedAt,
			writeErr:   &writeErr,
			batchSize:  summariseSpoolBatchSizeFor(table),
			batchBytes: summariseSpoolBatchBytesFor(l.cfg, table),
			onSendMeasurement: func(measurement importBatchMeasurement) {
				l.recordBatchTelemetry(telemetryPhase, measurement)
			},
			onProgress: l.recordServerProgress,
			beforeBatch: func(ctx context.Context) error {
				return l.waitForServerPressure(ctx, table)
			},
		}

		rows, err := load(parent, writer)
		if err != nil {
			return err
		}

		if rows != expectedRows {
			return fmt.Errorf(
				"%w: table=%s decoded=%d expected=%d",
				errSpoolDecodedRowsMismatch,
				table,
				rows,
				expectedRows,
			)
		}

		return nil
	})
	if err != nil {
		return err
	}

	l.recordServerPartCount(parent, table)

	return nil
}

func (l *summariseSpoolLoader) tryStageZeroContributionActiveVirtualRows( //nolint:gocyclo,funlen
	parent context.Context,
	writer *dgutaWriter,
	nextActiveSetID string,
) (bool, error) {
	if !l.zeroContributionActiveVirtualCandidate() {
		return false, nil
	}

	ctx, cancel := writer.activeVirtualPublishContext(parent)
	defer cancel()

	previousRows, err := queryMountsActiveRows(ctx, l.conn)
	if err != nil {
		return false, err
	}

	if activeRowsContainMount(previousRows, l.manifest.MountPath) {
		return false, nil
	}

	previousActiveSetID := fingerprintForMountsActive(previousRows)
	if previousActiveSetID == "" {
		return false, nil
	}

	previousSet, ok, err := l.readReadyActiveVirtualSetRow(ctx, previousActiveSetID)
	if err != nil || !ok {
		return false, err
	}

	activeRows := stagedMountsActiveRows(previousRows, mountsActiveRow(writer.activeMount()))
	if fingerprintForMountsActive(activeRows) != nextActiveSetID {
		return false, nil
	}

	return true, l.writeZeroContributionActiveVirtualRows(
		ctx,
		nextActiveSetID,
		previousActiveSetID,
		previousSet,
		activeRows,
	)
}

func activeRowsContainMount(rows []mountsActiveRow, mountPath string) bool {
	mountPath = ensureTrailingSlash(mountPath)
	for _, row := range rows {
		if ensureTrailingSlash(row.mountPath) == mountPath {
			return true
		}
	}

	return false
}

func (l *summariseSpoolLoader) zeroContributionActiveVirtualCandidate() bool {
	if l.manifest == nil {
		return false
	}

	tables := l.manifest.Tables

	return tables[chspool.TableDirFacts].Rows == 0 &&
		tables[chspool.TableDirFilterAll].Rows == 0
}

func (l *summariseSpoolLoader) writeZeroContributionActiveVirtualRows( //nolint:gocyclo,funlen
	ctx context.Context,
	nextActiveSetID string,
	previousActiveSetID string,
	previousSet activeVirtualSetRow,
	activeRows []mountsActiveRow,
) error {
	refreshedAt := time.Now().UTC()
	mounts := newActiveMountsSnapshot(activeRows).all()
	namespace, summaryRows, _, childRows := activeVirtualRowsForMountsFromDataWithLinks(
		nextActiveSetID,
		mounts,
		refreshedAt,
		nil,
		nil,
		nil,
	)
	affectedDirs := activeVirtualAffectedDirsForMount(l.manifest.MountPath, summaryRows)
	affected := activeVirtualAffectedDirSet(affectedDirs)
	affectedChildren := affectedActiveVirtualChildRows(childRows, affected)
	summaryDirs := activeVirtualSummaryDirsForChildCounts(affectedDirs, affectedChildren)

	previousSummaries, err := l.readActiveVirtualSummariesForDirs(ctx, previousActiveSetID, summaryDirs)
	if err != nil {
		return err
	}

	previousFilters, err := l.readActiveVirtualFiltersForDirs(ctx, previousActiveSetID, affectedDirs)
	if err != nil {
		return err
	}

	affectedSummaries := composeZeroContributionSummaryRows(summaryRows, previousSummaries, affected)
	summaryByDir := activeVirtualSummaryRowsByDir(affectedSummaries)
	affectedFilters := composeZeroContributionFilterRows(
		previousFilters,
		summaryByDir,
		nextActiveSetID,
		refreshedAt,
	)
	childCountSummaries := activeVirtualSummariesForChildCounts(affectedSummaries, previousSummaries, affectedChildren)
	fillActiveVirtualChildCounts(affectedChildren, childCountSummaries)

	if err := dropActiveVirtualPartitionsForSet(ctx, l.conn, nextActiveSetID); err != nil {
		return err
	}

	updatedAt := maxUpdatedAtForMounts(mounts)
	if err := l.copyUnchangedActiveVirtualRows(
		ctx,
		previousActiveSetID,
		nextActiveSetID,
		affectedDirs,
		updatedAt,
		refreshedAt,
	); err != nil {
		return err
	}

	writer := newActiveVirtualOverlayWriter(l.conn, summariseSpoolBatchSizeFor(chspool.TableActiveVirtualSummaries))
	if err := appendActiveVirtualOverlayRows(
		ctx,
		writer,
		namespace.rows,
		affectedSummaries,
		affectedFilters,
		affectedChildren,
	); err != nil {
		return err
	}

	if err := writer.flush(ctx); err != nil {
		return err
	}

	counts := activeVirtualCompositionCounts{
		summaryRows: uint64(len(summaryRows)),
		filterRows:  previousSet.FilterRows,
		childRows:   uint64(len(childRows)),
	}
	if err := l.validateActiveVirtualCompositionCounts(ctx, nextActiveSetID, counts); err != nil {
		return err
	}

	return newActiveVirtualOverlayWriter(l.conn, defaultBatchSize).appendSet(
		ctx,
		activeVirtualSetRowForCounts(nextActiveSetID, activeRows, counts, refreshedAt),
	)
}

func activeVirtualAffectedDirsForMount(
	mountPath string,
	summaryRows []activeVirtualSummaryRow,
) []string {
	mountPath = ensureTrailingSlash(mountPath)
	dirs := make([]string, 0, len(summaryRows))

	for _, row := range summaryRows {
		dir := ensureTrailingSlash(row.Dir)
		if strings.HasPrefix(mountPath, dir) {
			dirs = append(dirs, dir)
		}
	}

	slices.Sort(dirs)

	return slices.Compact(dirs)
}

func activeVirtualAffectedDirSet(dirs []string) map[string]struct{} {
	out := make(map[string]struct{}, len(dirs))
	for _, dir := range dirs {
		out[ensureTrailingSlash(dir)] = struct{}{}
	}

	return out
}

func affectedActiveVirtualChildRows(
	rows []activeVirtualChildRow,
	affected map[string]struct{},
) []activeVirtualChildRow {
	out := make([]activeVirtualChildRow, 0, len(rows))

	for _, row := range rows {
		if activeVirtualChildRowAffected(row, affected) {
			out = append(out, row)
		}
	}

	return out
}

func activeVirtualSummaryDirsForChildCounts(
	affectedDirs []string,
	childRows []activeVirtualChildRow,
) []string {
	dirs := append([]string(nil), affectedDirs...)
	seen := activeVirtualAffectedDirSet(affectedDirs)

	for _, row := range childRows {
		dir := activeVirtualSummaryDirForChild(row)
		if _, ok := seen[dir]; ok {
			continue
		}

		seen[dir] = struct{}{}
		dirs = append(dirs, dir)
	}

	slices.Sort(dirs)

	return slices.Compact(dirs)
}

func composeZeroContributionSummaryRows(
	rows []activeVirtualSummaryRow,
	previous map[string]activeVirtualSummaryRow,
	affected map[string]struct{},
) []activeVirtualSummaryRow {
	out := make([]activeVirtualSummaryRow, 0, len(affected))

	for _, row := range rows {
		if _, ok := affected[ensureTrailingSlash(row.Dir)]; !ok {
			continue
		}

		if previousRow, ok := previous[ensureTrailingSlash(row.Dir)]; ok {
			row = copyActiveVirtualSummaryAggregates(row, previousRow)
		}

		out = append(out, row)
	}

	return out
}

func activeVirtualSummaryRowsByDir(rows []activeVirtualSummaryRow) map[string]activeVirtualSummaryRow {
	out := make(map[string]activeVirtualSummaryRow, len(rows))
	for _, row := range rows {
		out[ensureTrailingSlash(row.Dir)] = row
	}

	return out
}

func composeZeroContributionFilterRows(
	rows []activeVirtualFilterAllRow,
	summaries map[string]activeVirtualSummaryRow,
	nextActiveSetID string,
	refreshedAt time.Time,
) []activeVirtualFilterAllRow {
	out := make([]activeVirtualFilterAllRow, 0, len(rows))

	for _, row := range rows {
		summary, ok := summaries[ensureTrailingSlash(row.Dir)]
		if !ok {
			continue
		}

		row.ActiveSetID = nextActiveSetID
		row.FilterChildCount = summary.ChildCount
		row.ChildCount = summary.ChildCount
		row.RefreshedAt = refreshedAt
		out = append(out, row)
	}

	return out
}

func activeVirtualSummariesForChildCounts(
	affectedSummaries []activeVirtualSummaryRow,
	previous map[string]activeVirtualSummaryRow,
	childRows []activeVirtualChildRow,
) []activeVirtualSummaryRow {
	out := append([]activeVirtualSummaryRow(nil), affectedSummaries...)
	seen := activeVirtualSummaryRowsByDir(out)

	for _, row := range childRows {
		dir := activeVirtualSummaryDirForChild(row)
		if _, ok := seen[dir]; ok {
			continue
		}

		previousRow, ok := previous[dir]
		if !ok {
			continue
		}

		seen[dir] = previousRow
		out = append(out, previousRow)
	}

	return out
}

func dropActiveVirtualPartitionsForSet(ctx context.Context, conn driver.Conn, activeSetID string) error {
	for _, query := range activeVirtualPartitionDropQueries() {
		if err := dropActiveSetPartition(ctx, conn, query, activeSetID, "active-virtual"); err != nil {
			return err
		}
	}

	return nil
}

func summariseSpoolBatchSizeFor(table string) int {
	switch table {
	case chspool.TableDirs,
		chspool.TableDirFacts,
		chspool.TableDirFilterAgeAll,
		chspool.TableChildFilterAll,
		chspool.TableDirFilterAll,
		chspool.TableActiveVirtualSummaries,
		chspool.TableActiveVirtualFilterAll:
		return projectionBatchSizeFor(defaultBatchSize)
	case chspool.TableActiveVirtualChildren:
		return defaultBatchSize
	default:
		return defaultBatchSize
	}
}

func activeVirtualSetRowForCounts(
	activeSetID string,
	rows []mountsActiveRow,
	counts activeVirtualCompositionCounts,
	refreshedAt time.Time,
) activeVirtualSetRow {
	return activeVirtualSetRow{
		ActiveSetID:      activeSetID,
		Schema3Version:   currentSchemaVersion,
		MountsSHA256:     activeSetID,
		ActiveMountCount: countActiveMountRows(rows),
		SummaryRows:      counts.summaryRows,
		FilterRows:       counts.filterRows,
		ChildRows:        counts.childRows,
		ManifestSHA256:   activeVirtualManifestSHA256ForCounts(activeSetID, counts),
		Ready:            1,
		RefreshedAt:      refreshedAt,
	}
}

func (l *summariseSpoolLoader) recordLoadedRows(table string, rows uint64) {
	if l.loadedRows == nil {
		l.loadedRows = make(map[string]uint64)
	}

	l.loadedRows[table] = rows
}

func (l *summariseSpoolLoader) countLoadedRows(parent context.Context, table string) (uint64, error) {
	if table == chspool.TableBasedirsHistory {
		return l.countLoadedBasedirsHistoryRows(parent)
	}

	if isActiveVirtualSpoolTable(table) {
		return l.countLoadedActiveVirtualRows(parent, table)
	}

	query, ok := summariseSpoolSnapshotCountQueries()[table]
	if !ok {
		return 0, fmt.Errorf("%w: %s", errUnknownSpoolLoadTable, table)
	}

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	var got uint64
	if err := l.conn.QueryRow(ctx, query, l.manifest.MountPath, l.snapshot.String()).Scan(&got); err != nil {
		return 0, fmt.Errorf("clickhouse: failed to count loaded spool table %s: %w", table, err)
	}

	return got, nil
}

func isActiveVirtualSpoolTable(table string) bool {
	_, ok := summariseSpoolActiveVirtualCountQueries()[table]

	return ok
}

func summariseSpoolSnapshotCountQueries() map[string]string { //nolint:funlen
	return map[string]string{
		chspool.TableDirs:  countLoadedSpoolRowsQuery(chspool.TableDirs),
		chspool.TableFiles: countLoadedSpoolRowsQuery(chspool.TableFiles),
		chspool.TableDirFacts: countLoadedSpoolRowsQuery(
			chspool.TableDirFacts,
		),
		chspool.TableDirFilterAgeAll: countLoadedSpoolRowsQuery(
			chspool.TableDirFilterAgeAll,
		),
		chspool.TableChildFilterAll: countLoadedSpoolRowsQuery(
			chspool.TableChildFilterAll,
		),
		chspool.TableDirFilterAll: countLoadedSpoolRowsQuery(
			chspool.TableDirFilterAll,
		),
		chspool.TableSchema3SnapshotSets: countLoadedSpoolRowsQuery(
			chspool.TableSchema3SnapshotSets,
		),
		chspool.TableDirProjectionSets: countLoadedSpoolRowsQuery(
			chspool.TableDirProjectionSets,
		),
		chspool.TableBasedirsGroupUsage: countLoadedSpoolRowsQuery(
			chspool.TableBasedirsGroupUsage,
		),
		chspool.TableBasedirsUserUsage: countLoadedSpoolRowsQuery(
			chspool.TableBasedirsUserUsage,
		),
		chspool.TableBasedirsGroupSubdirs: countLoadedSpoolRowsQuery(
			chspool.TableBasedirsGroupSubdirs,
		),
		chspool.TableBasedirsUserSubdirs: countLoadedSpoolRowsQuery(
			chspool.TableBasedirsUserSubdirs,
		),
	}
}

func (l *summariseSpoolLoader) countLoadedActiveVirtualRows( //nolint:funlen
	parent context.Context,
	table string,
) (uint64, error) {
	activeSetIDs, err := l.spoolActiveSetIDs()
	if err != nil {
		return 0, err
	}

	if len(activeSetIDs) == 0 {
		return 0, nil
	}

	query, ok := summariseSpoolActiveVirtualCountQueries()[table]
	if !ok {
		return 0, fmt.Errorf("%w: %s", errUnknownSpoolLoadTable, table)
	}

	var total uint64

	for _, activeSetID := range activeSetIDs {
		ctx, cancel := l.queryContext(parent)

		var got uint64
		if err := l.conn.QueryRow(ctx, query, activeSetID).Scan(&got); err != nil {
			cancel()

			return 0, fmt.Errorf("clickhouse: failed to count loaded spool table %s: %w", table, err)
		}

		cancel()

		total += got
	}

	return total, nil
}

func summariseSpoolActiveVirtualCountQueries() map[string]string {
	return map[string]string{
		chspool.TableActiveVirtualDirs: activeVirtualSpoolRowsQuery(
			chspool.TableActiveVirtualDirs,
		),
		chspool.TableActiveVirtualSummaries: activeVirtualSpoolRowsQuery(
			chspool.TableActiveVirtualSummaries,
		),
		chspool.TableActiveVirtualFilterAll: activeVirtualSpoolRowsQuery(
			chspool.TableActiveVirtualFilterAll,
		),
		chspool.TableActiveVirtualChildren: activeVirtualSpoolRowsQuery(
			chspool.TableActiveVirtualChildren,
		),
		chspool.TableActiveVirtualSets: activeVirtualSpoolRowsQuery(
			chspool.TableActiveVirtualSets,
		),
	}
}

func (l *summariseSpoolLoader) publish(parent context.Context) error {
	tracker, err := newSummariseSpoolPublishTracker(l.dir, l.manifest)
	if err != nil {
		return err
	}

	return l.publishWithTracker(parent, tracker)
}

func (l *summariseSpoolLoader) publishWithTracker(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhasePostSpoolPublishComplete) {
		return nil
	}

	writer := l.newDGUTAWriter(parent)

	if err := l.ensureFreshPreSwitchResumeState(parent, writer, tracker); err != nil {
		return err
	}

	if err := l.ensurePostSwitchActiveVirtualRows(parent, writer, tracker); err != nil {
		return err
	}

	if err := l.switchSnapshotAndFinalise(parent, writer, tracker); err != nil {
		return err
	}

	return tracker.mark(summariseSpoolPublishPhasePostSpoolPublishComplete)
}

func (l *summariseSpoolLoader) ensureFreshPreSwitchResumeState(
	parent context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	if !tracker.reusesPreSwitchState() {
		return nil
	}

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	stale, err := l.preSwitchResumeStateIsStale(ctx, tracker)
	if err != nil {
		return writer.closeWithNewSnapshotCleanup(ctx, err)
	}

	if !stale {
		return nil
	}

	return tracker.clearPreSwitchPlan()
}

func (l *summariseSpoolLoader) preSwitchResumeStateIsStale(
	ctx context.Context,
	tracker *summariseSpoolPublishTracker,
) (bool, error) {
	if l.currentSnapshotIsActive(ctx) {
		return false, nil
	}

	plan, ok := tracker.switchPlan()
	if !ok {
		return true, nil
	}

	activeRows, err := queryMountsActiveRows(ctx, l.conn)
	if err != nil {
		return false, err
	}

	return fingerprintForMountsActive(activeRows) != plan.PreviousActiveSetID, nil
}

func (l *summariseSpoolLoader) ensurePostSwitchActiveVirtualRows(
	parent context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhaseActiveVirtualReady) {
		writer.stagedActiveSetID = tracker.nextActiveSetID()

		return nil
	}

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	activeSetID, err := l.stagePostSwitchActiveVirtualRows(ctx, writer)
	if err != nil {
		return writer.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := tracker.setNextActiveSetID(activeSetID); err != nil {
		return err
	}

	return tracker.mark(summariseSpoolPublishPhaseActiveVirtualReady)
}

func (l *summariseSpoolLoader) stagePostSwitchActiveVirtualRows( //nolint:gocyclo,funlen
	ctx context.Context,
	writer *dgutaWriter,
) (string, error) {
	spoolActiveSetIDs, err := l.spoolActiveSetIDs()
	if err != nil {
		return "", err
	}

	if len(spoolActiveSetIDs) == 0 {
		return "", nil
	}

	postPublishActiveSetID, err := l.postPublishActiveSetID(ctx, writer)
	if err != nil {
		return "", err
	}

	writer.stagedActiveSetID = postPublishActiveSetID

	if slices.Contains(spoolActiveSetIDs, postPublishActiveSetID) {
		return postPublishActiveSetID, nil
	}

	composed, err := l.tryStageZeroContributionActiveVirtualRows(ctx, writer, postPublishActiveSetID)
	if err != nil {
		return "", err
	}

	if composed {
		return postPublishActiveSetID, l.dropSpoolActiveVirtualPartitions(ctx)
	}

	if err := writer.writeActiveVirtualReadiness(ctx); err != nil {
		return "", err
	}

	return postPublishActiveSetID, l.dropSpoolActiveVirtualPartitions(ctx)
}

func (l *summariseSpoolLoader) readReadyActiveVirtualSetRow(
	ctx context.Context,
	activeSetID string,
) (activeVirtualSetRow, bool, error) {
	rows, err := l.conn.Query(ctx, selectReadyActiveVirtualSetCountsQuery, activeSetID)
	if err != nil {
		return activeVirtualSetRow{}, false, fmt.Errorf("clickhouse: failed to query active virtual set: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return activeVirtualSetRow{}, false, rowIterationErr(rows, "clickhouse: active virtual set iteration error")
	}

	row := activeVirtualSetRow{ActiveSetID: activeSetID, Ready: 1}
	if err := rows.Scan(&row.SummaryRows, &row.FilterRows, &row.ChildRows); err != nil {
		return activeVirtualSetRow{}, false, fmt.Errorf("clickhouse: failed to scan active virtual set: %w", err)
	}

	return row, true, rowIterationErr(rows, "clickhouse: active virtual set iteration error")
}

func (l *summariseSpoolLoader) readActiveVirtualSummariesForDirs(
	ctx context.Context,
	activeSetID string,
	dirs []string,
) (map[string]activeVirtualSummaryRow, error) {
	if len(dirs) == 0 {
		return map[string]activeVirtualSummaryRow{}, nil
	}

	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(selectActiveVirtualSummariesForDirsQuery, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := l.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query previous active virtual summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActiveVirtualSummaryRows(rows, activeSetID)
}

func scanActiveVirtualSummaryRows( //nolint:funlen
	rows driver.Rows,
	activeSetID string,
) (map[string]activeVirtualSummaryRow, error) {
	out := make(map[string]activeVirtualSummaryRow)

	for rows.Next() {
		row := activeVirtualSummaryRow{ActiveSetID: activeSetID}
		if err := rows.Scan(
			&row.Dir,
			&row.VirtualID,
			&row.MountPath,
			&row.SnapshotID,
			&row.MountRootDirID,
			&row.IsMountRootBox,
			&row.UpdatedAt,
			&row.AllCount,
			&row.AllSize,
			&row.AllAtimeMin,
			&row.AllMtimeMax,
			&row.AllAtimeBuckets,
			&row.AllMtimeBuckets,
			&row.AllUIDs,
			&row.AllGIDs,
			&row.AllFT,
			&row.FileCount,
			&row.FileSize,
			&row.ChildCount,
		); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan previous active virtual summary: %w", err)
		}

		out[ensureTrailingSlash(row.Dir)] = row
	}

	return out, rowIterationErr(rows, "clickhouse: previous active virtual summary iteration error")
}

func (l *summariseSpoolLoader) readActiveVirtualFiltersForDirs(
	ctx context.Context,
	activeSetID string,
	dirs []string,
) ([]activeVirtualFilterAllRow, error) {
	if len(dirs) == 0 {
		return nil, nil
	}

	query, args := activeVirtualDirsQuery(
		fmt.Sprintf(selectActiveVirtualFiltersForDirsQuery, placeholders(len(dirs))),
		activeSetID,
		dirs,
	)

	rows, err := l.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query previous active virtual filters: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanActiveVirtualFilterRows(rows, activeSetID)
}

func scanActiveVirtualFilterRows(
	rows driver.Rows,
	activeSetID string,
) ([]activeVirtualFilterAllRow, error) {
	out := []activeVirtualFilterAllRow{}

	for rows.Next() {
		row := activeVirtualFilterAllRow{ActiveSetID: activeSetID}
		if err := rows.Scan(
			&row.Dir,
			&row.VirtualID,
			&row.Age,
			&row.GID,
			&row.UID,
			&row.FT,
			&row.Count,
			&row.Size,
			&row.AtimeMin,
			&row.MtimeMax,
			&row.AtimeBuckets,
			&row.MtimeBuckets,
			&row.FilterChildCount,
			&row.ChildCount,
		); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan previous active virtual filter: %w", err)
		}

		out = append(out, row)
	}

	return out, rowIterationErr(rows, "clickhouse: previous active virtual filter iteration error")
}

func (l *summariseSpoolLoader) copyUnchangedActiveVirtualRows(
	ctx context.Context,
	previousActiveSetID string,
	nextActiveSetID string,
	affectedDirs []string,
	updatedAt time.Time,
	refreshedAt time.Time,
) error {
	for _, build := range []func(string, string, []string, time.Time, time.Time) (string, []any){
		copyUnchangedActiveVirtualSummariesQuery,
		copyUnchangedActiveVirtualFiltersQuery,
		copyUnchangedActiveVirtualChildrenQuery,
	} {
		query, args := build(previousActiveSetID, nextActiveSetID, affectedDirs, updatedAt, refreshedAt)
		if err := l.conn.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("clickhouse: failed to copy unchanged active virtual rows: %w", err)
		}
	}

	return nil
}

func (l *summariseSpoolLoader) validateActiveVirtualCompositionCounts(
	ctx context.Context,
	activeSetID string,
	counts activeVirtualCompositionCounts,
) error {
	expected := map[string]uint64{
		chspool.TableActiveVirtualSummaries: counts.summaryRows,
		chspool.TableActiveVirtualFilterAll: counts.filterRows,
		chspool.TableActiveVirtualChildren:  counts.childRows,
	}

	for table, want := range expected {
		got, err := l.countActiveVirtualRowsForSet(ctx, table, activeSetID)
		if err != nil {
			return err
		}

		if got != want {
			return fmt.Errorf("%w: table=%s got=%d expected=%d", errSpoolLoadedRowsMismatch, table, got, want)
		}
	}

	return nil
}

func (l *summariseSpoolLoader) countActiveVirtualRowsForSet(
	ctx context.Context,
	table string,
	activeSetID string,
) (uint64, error) {
	query, ok := summariseSpoolActiveVirtualCountQueries()[table]
	if !ok {
		return 0, fmt.Errorf("%w: %s", errUnknownSpoolLoadTable, table)
	}

	var got uint64
	if err := l.conn.QueryRow(ctx, query, activeSetID).Scan(&got); err != nil {
		return 0, fmt.Errorf("clickhouse: failed to count composed active virtual table %s: %w", table, err)
	}

	return got, nil
}

func (l *summariseSpoolLoader) postPublishActiveSetID(
	ctx context.Context,
	writer *dgutaWriter,
) (string, error) {
	activeRows, err := queryMountsActiveRows(ctx, l.conn)
	if err != nil {
		return "", err
	}

	postPublishRows := stagedMountsActiveRows(activeRows, mountsActiveRow(writer.activeMount()))

	return fingerprintForMountsActive(postPublishRows), nil
}

func (l *summariseSpoolLoader) switchSnapshotAndFinalise(
	parent context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	ctx, cancel := l.queryContext(parent)
	defer cancel()

	plan, err := l.ensureSwitchPlan(ctx, writer, tracker)
	if err != nil {
		return writer.closeWithNewSnapshotCleanup(ctx, err)
	}

	if err := l.ensureMountSwitched(ctx, writer, tracker); err != nil {
		return err
	}

	if err := l.ensureOldSnapshotDropped(ctx, writer, tracker, plan); err != nil {
		return err
	}

	if err := l.ensureOldActiveVirtualDropped(ctx, writer, tracker, plan); err != nil {
		return err
	}

	if err := l.ensureActiveTreeSummariesRefreshed(ctx, writer, tracker); err != nil {
		return err
	}

	return l.ensureActivePrefixRollupsRefreshed(ctx, writer, tracker)
}

func (l *summariseSpoolLoader) ensureSwitchPlan(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) (summariseSpoolSwitchPlan, error) {
	if plan, ok := tracker.switchPlan(); ok {
		writer.stagedActiveSetID = plan.NextActiveSetID

		return plan, nil
	}

	if tracker.done(summariseSpoolPublishPhaseMountSwitched) {
		return summariseSpoolSwitchPlan{}, errSummariseSpoolPublishMissingSwitchPlan
	}

	previousSID, hasPrevious, err := writer.readPreviousActiveSnapshotID(ctx)
	if err != nil {
		return summariseSpoolSwitchPlan{}, err
	}

	plan, err := l.switchPlanFromCurrentState(ctx, writer, previousSID, hasPrevious)
	if err != nil {
		return summariseSpoolSwitchPlan{}, err
	}

	if err := tracker.setSwitchPlan(plan); err != nil {
		return summariseSpoolSwitchPlan{}, err
	}

	return plan, nil
}

func (l *summariseSpoolLoader) switchPlanFromCurrentState(
	ctx context.Context,
	writer *dgutaWriter,
	previousSID string,
	hasPrevious bool,
) (summariseSpoolSwitchPlan, error) {
	previousActiveSetID, nextActiveSetID, err := writer.activeSetIDsForSwitch(ctx)
	if err != nil {
		return summariseSpoolSwitchPlan{}, err
	}

	return summariseSpoolSwitchPlan{
		HasPrevious:         hasPrevious,
		PreviousSnapshotID:  previousSID,
		PreviousActiveSetID: previousActiveSetID,
		NextActiveSetID:     nextActiveSetID,
	}, nil
}

func (l *summariseSpoolLoader) ensureMountSwitched(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhaseMountSwitched) {
		return nil
	}

	if l.currentSnapshotIsActive(ctx) {
		return tracker.mark(summariseSpoolPublishPhaseMountSwitched)
	}

	err := writer.timeImportPhase(importPhaseMountSwitch, func() error {
		return writer.switchActiveSnapshot(ctx)
	})
	if err == nil {
		return tracker.mark(summariseSpoolPublishPhaseMountSwitched)
	}

	if l.currentSnapshotIsActive(ctx) {
		return tracker.mark(summariseSpoolPublishPhaseMountSwitched)
	}

	return writer.closeWithNewSnapshotCleanup(ctx, err)
}

func (l *summariseSpoolLoader) currentSnapshotIsActive(ctx context.Context) bool {
	activeSID, hasActive, err := readActiveSnapshotID(ctx, l.conn, l.manifest.MountPath)

	return err == nil && hasActive && activeSID == l.snapshot.String()
}

func (l *summariseSpoolLoader) ensureOldSnapshotDropped(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
	plan summariseSpoolSwitchPlan,
) error {
	if tracker.done(summariseSpoolPublishPhaseOldSnapshotDropped) {
		return nil
	}

	if plan.HasPrevious {
		if err := writer.dropPreviousSnapshotPartitions(ctx, plan.PreviousSnapshotID); err != nil {
			return err
		}
	}

	return tracker.mark(summariseSpoolPublishPhaseOldSnapshotDropped)
}

func (l *summariseSpoolLoader) ensureOldActiveVirtualDropped(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
	plan summariseSpoolSwitchPlan,
) error {
	if tracker.done(summariseSpoolPublishPhaseOldActiveVirtualDropped) {
		return nil
	}

	if err := writer.dropPreviousActiveVirtualPartitions(
		ctx,
		plan.PreviousActiveSetID,
		plan.NextActiveSetID,
	); err != nil {
		return err
	}

	return tracker.mark(summariseSpoolPublishPhaseOldActiveVirtualDropped)
}

func (l *summariseSpoolLoader) ensureActiveTreeSummariesRefreshed(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhaseTreeSummaryRefreshed) {
		return nil
	}

	writer.refreshActiveTreeSummariesBestEffort(ctx)

	return tracker.mark(summariseSpoolPublishPhaseTreeSummaryRefreshed)
}

func (l *summariseSpoolLoader) ensureActivePrefixRollupsRefreshed(
	ctx context.Context,
	writer *dgutaWriter,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhaseActivePrefixRefreshed) {
		return nil
	}

	writer.refreshActivePrefixRollupsBestEffort(ctx)

	return tracker.mark(summariseSpoolPublishPhaseActivePrefixRefreshed)
}

func (l *summariseSpoolLoader) loadReport(parent context.Context) (perfreport.Report, error) {
	builder := newSummariseSpoolLoadReportBuilder(l.dir, l.manifest, l.importPhaseRecorder)
	originalRecorder := l.importPhaseRecorder

	l.importPhaseRecorder = builder.record
	defer func() { l.importPhaseRecorder = originalRecorder }()

	err := l.loadWithAfterPublish(parent, builder.collect)
	if err != nil {
		return builder.report, err
	}

	builder.finish()

	return builder.report, nil
}

func newSummariseSpoolLoadReportBuilder(
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
) *summariseSpoolLoadReportBuilder {
	return &summariseSpoolLoadReportBuilder{
		report:         perfreport.NewReport("clickhouse", spoolDir, 1, 0),
		manifest:       manifest,
		recorder:       recorder,
		phaseDurations: make(map[string]time.Duration),
		started:        time.Now(),
		usageBefore:    summariseSpoolProcessCPUUsage(),
	}
}

func (l *summariseSpoolLoader) loadReportTableStats(
	parent context.Context,
) (map[string]perfreport.TableStats, error) {
	tables := summariseSpoolLoadReportTables(l)
	if len(tables) == 0 {
		return map[string]perfreport.TableStats{}, nil
	}

	query, args := summariseSpoolLoadTableStatsQuery(l.cfg.Database, tables)

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	rows, err := l.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	return scanSummariseSpoolLoadTableStats(rows)
}

func summariseSpoolLoadReportTables(loader *summariseSpoolLoader) []string {
	tables := make([]string, 0, len(loader.loadedRows))
	for table, rows := range loader.loadedRows {
		if rows > 0 {
			tables = append(tables, table)
		}
	}

	slices.Sort(tables)

	return tables
}

func summariseSpoolLoadTableStatsQuery(database string, tables []string) (string, []any) {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(tables)), ",")
	query := "SELECT table, toUInt64(sum(rows)), toUInt64(count()), " +
		"toUInt64(sum(data_compressed_bytes)), toUInt64(sum(data_uncompressed_bytes)) " +
		"FROM system.parts WHERE database = ? AND active AND table IN (" +
		placeholders + ") GROUP BY table"

	args := make([]any, 0, len(tables)+1)

	args = append(args, database)
	for _, table := range tables {
		args = append(args, table)
	}

	return query, args
}

func scanSummariseSpoolLoadTableStats(rows driver.Rows) (map[string]perfreport.TableStats, error) {
	stats := make(map[string]perfreport.TableStats)

	for rows.Next() {
		var (
			table string
			s     perfreport.TableStats
		)

		if err := rows.Scan(
			&table,
			&s.Rows,
			&s.ActiveParts,
			&s.CompressedBytes,
			&s.UncompressedBytes,
		); err != nil {
			return nil, err
		}

		stats[table] = s
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

func runSummariseSpoolLoad(
	ctx context.Context,
	cfg Config,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
	afterPublish summariseSpoolAfterPublish,
	telemetryRecorder ...func(SummariseImportTelemetry),
) error {
	if err := validateSummariseSpoolLoad(cfg, manifest); err != nil {
		return err
	}

	connectCtx, connectCancel := queryContext(ctx, queryTimeout(cfg))
	defer connectCancel()

	conn, err := connectForImportFromConfigContext(connectCtx, cfg)
	if err != nil {
		return err
	}

	loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, recorder, telemetryRecorder...)
	if err != nil {
		_ = conn.Close()

		return err
	}

	return loader.loadWithAfterPublish(ctx, afterPublish)
}

type summariseSpoolAfterPublish func(context.Context, *summariseSpoolLoader) error

type summariseSpoolCPUUsage struct {
	userMS   uint64
	systemMS uint64
}

func summariseSpoolProcessCPUUsage() summariseSpoolCPUUsage {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return summariseSpoolCPUUsage{}
	}

	return summariseSpoolCPUUsage{
		userMS:   summariseSpoolTimevalMS(usage.Utime),
		systemMS: summariseSpoolTimevalMS(usage.Stime),
	}
}

func summariseSpoolCPUUsageDelta(before, after summariseSpoolCPUUsage) summariseSpoolCPUUsage {
	return summariseSpoolCPUUsage{
		userMS:   summariseSpoolSaturatingSub(after.userMS, before.userMS),
		systemMS: summariseSpoolSaturatingSub(after.systemMS, before.systemMS),
	}
}

type summariseSpoolLoadReportBuilder struct {
	report            perfreport.Report
	manifest          *chspool.Manifest
	recorder          func(string, time.Duration)
	phaseDurations    map[string]time.Duration
	loadedRows        map[string]uint64
	batchMeasurements map[string][]importBatchMeasurement
	tableStatsStatus  string
	tableStatsError   string
	started           time.Time
	usageBefore       summariseSpoolCPUUsage
}

func (b *summariseSpoolLoadReportBuilder) record(phase string, duration time.Duration) {
	if b.recorder != nil {
		b.recorder(phase, duration)
	}

	if phase == "" || duration <= 0 {
		return
	}

	b.phaseDurations[phase] += duration
}

func (b *summariseSpoolLoadReportBuilder) collect(
	parent context.Context,
	loader *summariseSpoolLoader,
) error {
	b.captureLoadState(loader)

	stats, err := loader.loadReportTableStats(parent)
	if err != nil {
		if isSummariseSpoolOptionalTableStatsError(err) {
			b.report.TableStats = map[string]perfreport.TableStats{}
			b.tableStatsStatus = spoolLoadTableStatsUnavailable
			b.tableStatsError = "insufficient_privileges: " + err.Error()

			return nil
		}

		return err
	}

	summariseSpoolAddImportPhaseDurations(stats, b.phaseDurations)
	summariseSpoolAddRowAmplification(stats)

	b.report.TableStats = stats
	b.report.SelectedTables = summariseSpoolLoadReportTableNames(stats)

	b.tableStatsStatus = spoolLoadTableStatsAvailable
	if len(stats) == 0 {
		b.tableStatsStatus = spoolLoadTableStatsNotRequested
	}

	return nil
}

func summariseSpoolLoadLoadedRows(
	loader *summariseSpoolLoader,
	manifest *chspool.Manifest,
) map[string]uint64 {
	rows := make(map[string]uint64, len(loader.loadedRows))
	for table, count := range loader.loadedRows {
		if count > 0 {
			rows[table] = count
		}
	}

	if manifest == nil {
		return rows
	}

	for table, tableManifest := range manifest.Tables {
		if _, ok := rows[table]; !ok && tableManifest.Rows > 0 {
			rows[table] = tableManifest.Rows
		}
	}

	return rows
}

func isSummariseSpoolOptionalTableStatsError(err error) bool {
	var ex *proto.Exception
	if errors.As(err, &ex) {
		return ex.Code == clickHouseInsufficientPrivilegeCode
	}

	msg := err.Error()

	return strings.Contains(msg, "code: 497") &&
		strings.Contains(msg, "Not enough privileges")
}

func summariseSpoolLoadReportTableNames(stats map[string]perfreport.TableStats) []string {
	tables := make([]string, 0, len(stats))
	for table := range stats {
		tables = append(tables, table)
	}

	slices.Sort(tables)

	return tables
}

func (b *summariseSpoolLoadReportBuilder) finish() {
	usage := summariseSpoolCPUUsageDelta(b.usageBefore, summariseSpoolProcessCPUUsage())
	b.report.MaxRSSBytes = summariseSpoolMaxRSSBytes()
	b.report.AddOperation(
		spoolLoadReportOperation,
		b.inputs(usage),
		[]float64{summariseSpoolDurationMS(time.Since(b.started))},
	)

	summariseSpoolAddE2ComputedBudgetInputs(&b.report.Operations[len(b.report.Operations)-1])
}

func summariseSpoolMaxRSSBytes() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}

	if usage.Maxrss <= 0 {
		return 0
	}

	return uint64(usage.Maxrss) * summariseSpoolBytesPerKiB
}

func summariseSpoolDurationMS(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func summariseSpoolAddE2ComputedBudgetInputs(op *perfreport.Operation) {
	if op.Inputs == nil {
		op.Inputs = make(map[string]any)
	}

	op.Inputs["budget_source"] = "computed_from_measurements"
	op.Inputs["budget_measurement_count"] = uint64(len(op.DurationsMS))
	op.Inputs["wall_time_budget_ms"] = uint64(math.Ceil(op.P95MS))
	op.Inputs["total_cpu_budget_ms"] = summariseSpoolBudgetUint64Input(op.Inputs, "total_cpu_ms")
	op.Inputs["peak_rss_budget_bytes"] = summariseSpoolBudgetUint64Input(op.Inputs, "peak_rss_bytes")
	op.Inputs["spool_byte_budget"] = summariseSpoolBudgetUint64Input(op.Inputs, "spool_bytes")
	op.Inputs["part_count_budget"] = summariseSpoolPartCountBudget(op.Inputs)
}

func (b *summariseSpoolLoadReportBuilder) inputs(usage summariseSpoolCPUUsage) map[string]any {
	inputs := map[string]any{
		"loaded_table_rows":                  b.loadedRows,
		"user_cpu_ms":                        usage.userMS,
		"system_cpu_ms":                      usage.systemMS,
		"total_cpu_ms":                       usage.userMS + usage.systemMS,
		"peak_rss_bytes":                     b.report.MaxRSSBytes,
		"spool_bytes":                        summariseSpoolManifestBytes(b.manifest),
		"part_counts":                        summariseSpoolPartCounts(b.report.TableStats),
		"retry_cleanup_result":               b.retryCleanupResult(),
		"publish_latency_ms":                 summariseSpoolDurationMSUint64(b.phaseDurations[importPhaseMountSwitch]),
		"batch_estimated_uncompressed_bytes": importBatchEstimatedBytes(b.batchMeasurements),
	}

	if b.tableStatsStatus != "" {
		inputs["table_stats_status"] = b.tableStatsStatus
	}

	if b.tableStatsError != "" {
		inputs["table_stats_error"] = b.tableStatsError
	}

	return inputs
}

func summariseSpoolManifestBytes(manifest *chspool.Manifest) uint64 {
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

func summariseSpoolPartCounts(stats map[string]perfreport.TableStats) map[string]uint64 {
	counts := make(map[string]uint64, len(stats))
	for table, tableStats := range stats {
		counts[table] = tableStats.ActiveParts
	}

	return counts
}

func summariseSpoolDurationMSUint64(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}

	return uint64(duration / time.Millisecond)
}

func (b *summariseSpoolLoadReportBuilder) retryCleanupResult() string {
	if b.phaseDurations[importPhasePartitionDropReset] > 0 {
		return spoolLoadReportSuccess
	}

	return spoolLoadReportNotAttempted
}

// LoadSummariseSpoolReport loads a completed local summarise spool into
// ClickHouse and returns final-gate evidence for the measured load.
func LoadSummariseSpoolReport(
	ctx context.Context,
	cfg Config,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
) (perfreport.Report, error) {
	builder := newSummariseSpoolLoadReportBuilder(spoolDir, manifest, recorder)

	err := runSummariseSpoolLoad(
		ctx,
		cfg,
		spoolDir,
		manifest,
		builder.record,
		builder.collect,
		summariseImportTelemetryFromContext(ctx),
	)
	if err != nil {
		return builder.report, err
	}

	builder.finish()

	return builder.report, nil
}

// LoadSummariseSpool loads a completed local summarise spool into ClickHouse
// and publishes it only after all table loads and count checks pass.
func LoadSummariseSpool(
	ctx context.Context,
	cfg Config,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
) error {
	return runSummariseSpoolLoad(ctx, cfg, spoolDir, manifest, recorder, nil)
}

func validateSummariseSpoolLoad(cfg Config, manifest *chspool.Manifest) error { //nolint:gocyclo
	if err := validateConfig(cfg); err != nil {
		return err
	}

	if err := validateSummariseInsertLimits(cfg); err != nil {
		return err
	}

	if manifest == nil {
		return errSummariseSpoolManifestRequired
	}

	if manifest.Version != chspool.Version || manifest.Format != chspool.Format || manifest.State != chspool.Complete {
		return errInvalidSummariseSpoolManifest
	}

	if manifest.MountPath == "" {
		return errMountPathRequired
	}

	if manifest.SnapshotID == "" || manifest.UpdatedAt == "" {
		return errUpdatedAtRequired
	}

	return validateSummariseSpoolManifestTables(manifest)
}

func validateSummariseSpoolManifestTables(manifest *chspool.Manifest) error {
	for _, table := range chspool.TableOrder() {
		if _, ok := manifest.Tables[table]; ok {
			continue
		}

		return fmt.Errorf("%w: missing table %s", chspool.ErrManifestMismatch, table)
	}

	return nil
}

func summariseSpoolImportPhaseTables(phase string) []string {
	if table, ok := strings.CutPrefix(phase, summariseSpoolLoadPhasePrefix); ok {
		return []string{table}
	}

	switch phase {
	case importPhaseDirFilterAllInsert:
		return []string{chspool.TableDirFilterAll}
	case importPhaseChildFilterAllInsert:
		return []string{chspool.TableChildFilterAll}
	case importPhaseSchema3Ready:
		return []string{chspool.TableSchema3SnapshotSets}
	case importPhaseActiveVirtualInsert:
		return []string{
			chspool.TableActiveVirtualDirs,
			chspool.TableActiveVirtualSummaries,
			chspool.TableActiveVirtualFilterAll,
			chspool.TableActiveVirtualChildren,
			chspool.TableActiveVirtualSets,
		}
	case importPhaseActiveVirtualReady:
		return []string{chspool.TableActiveVirtualSets}
	default:
		return nil
	}
}

func copyActiveVirtualSummaryAggregates(
	row activeVirtualSummaryRow,
	previous activeVirtualSummaryRow,
) activeVirtualSummaryRow {
	row.AllCount = previous.AllCount
	row.AllSize = previous.AllSize
	row.AllAtimeMin = previous.AllAtimeMin
	row.AllMtimeMax = previous.AllMtimeMax
	row.AllAtimeBuckets = previous.AllAtimeBuckets
	row.AllMtimeBuckets = previous.AllMtimeBuckets
	row.AllUIDs = previous.AllUIDs
	row.AllGIDs = previous.AllGIDs
	row.AllFT = previous.AllFT
	row.FileCount = previous.FileCount
	row.FileSize = previous.FileSize

	if row.IsMountRootBox == 1 && previous.IsMountRootBox == 1 {
		row.ChildCount = previous.ChildCount
	}

	return row
}

func activeVirtualChildRowAffected(row activeVirtualChildRow, affected map[string]struct{}) bool {
	if _, ok := affected[ensureTrailingSlash(row.ParentDir)]; ok {
		return true
	}

	_, ok := affected[ensureTrailingSlash(row.ChildDir)]

	return ok
}

func copyUnchangedActiveVirtualSummariesQuery(
	previousActiveSetID string,
	nextActiveSetID string,
	affectedDirs []string,
	updatedAt time.Time,
	refreshedAt time.Time,
) (string, []any) {
	query := "INSERT INTO wrstat_active_virtual_summaries " +
		"(active_set_id, virtual_id, mount_path, snapshot_id, mount_root_dir_id, is_mount_root_box, " +
		"updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, " +
		"all_ft, file_count, file_size, child_count, refreshed_at) SELECT ?, virtual_id, mount_path, " +
		"snapshot_id, mount_root_dir_id, is_mount_root_box, ?, all_count, all_size, all_atime_min, " +
		"all_mtime_max, all_atime_buckets, " +
		"all_mtime_buckets, all_uids, all_gids, all_ft, file_count, file_size, child_count, ? " +
		"FROM wrstat_active_virtual_summaries PREWHERE active_set_id = ? WHERE virtual_id NOT IN (" +
		"SELECT virtual_id FROM wrstat_active_virtual_dirs PREWHERE active_set_id = ? WHERE full_path IN (" +
		placeholders(len(affectedDirs)) + "))"
	args := []any{nextActiveSetID, updatedAt, refreshedAt, previousActiveSetID, previousActiveSetID}

	return query, appendActiveVirtualDirArgs(args, affectedDirs)
}

func copyUnchangedActiveVirtualFiltersQuery(
	previousActiveSetID string,
	nextActiveSetID string,
	affectedDirs []string,
	_ time.Time,
	refreshedAt time.Time,
) (string, []any) {
	query := "INSERT INTO wrstat_active_virtual_filter_all " +
		"(active_set_id, virtual_id, age, gid, uid, ft, count, size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, filter_child_count, child_count, refreshed_at) " +
		"SELECT ?, virtual_id, age, gid, uid, ft, count, size, atime_min, mtime_max, atime_buckets, " +
		"mtime_buckets, filter_child_count, child_count, ? FROM wrstat_active_virtual_filter_all " +
		"PREWHERE active_set_id = ? WHERE virtual_id NOT IN (" +
		"SELECT virtual_id FROM wrstat_active_virtual_dirs PREWHERE active_set_id = ? WHERE full_path IN (" +
		placeholders(len(affectedDirs)) + "))"
	args := []any{nextActiveSetID, refreshedAt, previousActiveSetID, previousActiveSetID}

	return query, appendActiveVirtualDirArgs(args, affectedDirs)
}

func copyUnchangedActiveVirtualChildrenQuery(
	previousActiveSetID string,
	nextActiveSetID string,
	affectedDirs []string,
	_ time.Time,
	refreshedAt time.Time,
) (string, []any) {
	query := "INSERT INTO wrstat_active_virtual_children " +
		"(active_set_id, parent_virtual_id, child_virtual_id, mount_path, snapshot_id, mount_root_dir_id, " +
		"is_mount_root_box, child_count, refreshed_at) " +
		"SELECT ?, parent_virtual_id, child_virtual_id, mount_path, snapshot_id, mount_root_dir_id, " +
		"is_mount_root_box, child_count, ? FROM wrstat_active_virtual_children " +
		"PREWHERE active_set_id = ? WHERE parent_virtual_id NOT IN (" +
		"SELECT virtual_id FROM wrstat_active_virtual_dirs PREWHERE active_set_id = ? WHERE full_path IN (" +
		placeholders(len(affectedDirs)) + ")) AND child_virtual_id NOT IN (" +
		"SELECT virtual_id FROM wrstat_active_virtual_dirs PREWHERE active_set_id = ? WHERE full_path IN (" +
		placeholders(len(affectedDirs)) + "))"
	args := []any{nextActiveSetID, refreshedAt, previousActiveSetID, previousActiveSetID}
	args = appendActiveVirtualDirArgs(args, affectedDirs)
	args = append(args, previousActiveSetID)

	return query, appendActiveVirtualDirArgs(args, affectedDirs)
}

func appendActiveVirtualDirArgs(args []any, dirs []string) []any {
	for _, dir := range dirs {
		args = append(args, ensureTrailingSlash(dir))
	}

	return args
}

func summariseSpoolBudgetUint64Input(inputs map[string]any, key string) uint64 {
	value, ok := inputs[key].(uint64)
	if !ok {
		return 0
	}

	return value
}

func summariseSpoolPartCountBudget(inputs map[string]any) uint64 {
	var count uint64
	for _, value := range summariseSpoolPartCountsInput(inputs) {
		count += value
	}

	return count
}

func summariseSpoolPartCountsInput(inputs map[string]any) map[string]uint64 {
	values, ok := inputs["part_counts"].(map[string]uint64)
	if !ok {
		return nil
	}

	return values
}

func summariseSpoolTimevalMS(value syscall.Timeval) uint64 {
	if value.Sec < 0 || value.Usec < 0 {
		return 0
	}

	return uint64(value.Sec)*1000 + uint64(value.Usec)/1000
}

func summariseSpoolSaturatingSub(after uint64, before uint64) uint64 {
	if after < before {
		return 0
	}

	return after - before
}

func addSpoolActiveSetID(ids map[string]struct{}, activeSetID string) {
	if activeSetID != "" {
		ids[activeSetID] = struct{}{}
	}
}

func sameHistoryDeleteRow(a, b chspool.BasedirsHistoryRow) bool {
	return a.MountPath == b.MountPath &&
		a.GID == b.GID &&
		a.Date.Equal(b.Date)
}

func countLoadedSpoolRowsQuery(table string) string {
	return "SELECT count() FROM " + table + " WHERE mount_path = ? AND snapshot_id = toUUID(?)"
}

func activeVirtualSpoolRowsQuery(table string) string {
	return "SELECT count() FROM " + table + " WHERE active_set_id = ?"
}
