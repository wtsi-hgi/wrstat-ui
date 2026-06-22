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
	"slices"
	"sort"
	"strings"
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
	summariseSpoolLoadPhasePrefix       = "spool_load_"
	spoolHistoryDeleteChunk             = 512
	summariseSpoolBytesPerKiB           = 1024
	spoolLoadReportOperation            = "spool_load_total"
	spoolLoadReportSuccess              = "success"
	spoolLoadReportNotAttempted         = "not_attempted"
	spoolLoadTableStatsAvailable        = "available"
	spoolLoadTableStatsUnavailable      = "unavailable"
	spoolLoadTableStatsNotRequested     = "not_requested"
	clickHouseInsufficientPrivilegeCode = 497

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
	errSummariseSpoolManifestRequired         = errors.New("clickhouse: summarise spool manifest is required")
	errInvalidSummariseSpoolManifest          = errors.New("clickhouse: invalid summarise spool manifest")
	errSpoolDecodedRowsMismatch               = errors.New("clickhouse: spool decoded row count mismatch")
	errUnknownSpoolLoadTable                  = errors.New("clickhouse: no loader query for spool table")
	errSpoolLoadedRowsMismatch                = errors.New("clickhouse: spool loaded row count mismatch")
	errSummariseSpoolPublishMissingSwitchPlan = errors.New(
		"clickhouse: summarise spool publish state is missing switch plan",
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
}

func newSummariseSpoolLoader(
	cfg Config,
	conn driver.Conn,
	spoolDir string,
	manifest *chspool.Manifest,
	recorder func(string, time.Duration),
) (*summariseSpoolLoader, error) {
	snapshot, err := uuid.Parse(manifest.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid summarise spool snapshot id: %w", err)
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, manifest.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid summarise spool updated_at: %w", err)
	}

	return &summariseSpoolLoader{
		cfg:                 cfg,
		conn:                conn,
		dir:                 spoolDir,
		manifest:            manifest,
		snapshot:            snapshot,
		updatedAt:           updatedAt,
		importPhaseRecorder: recorder,
		loadedRows:          map[string]uint64{},
		groupUsageDates:     map[uint32]finaliseQuotaDates{},
	}, nil
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

	tracker, err := newSummariseSpoolPublishTracker(l.dir, l.manifest)
	if err != nil {
		return err
	}

	if err := l.ensureTablesLoaded(parent, tracker); err != nil {
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

func (l *summariseSpoolLoader) ensureTablesLoaded(
	parent context.Context,
	tracker *summariseSpoolPublishTracker,
) error {
	if tracker.done(summariseSpoolPublishPhaseTablesLoaded) {
		l.recordLoadedRowsFromManifest()

		return nil
	}

	if err := l.prepareSnapshot(parent); err != nil {
		return err
	}

	if err := l.loadTables(parent); err != nil {
		return err
	}

	l.recordLoadedRowsFromManifest()

	return tracker.mark(summariseSpoolPublishPhaseTablesLoaded)
}

func (l *summariseSpoolLoader) recordLoadedRowsFromManifest() {
	if l.manifest == nil {
		return
	}

	for table, manifest := range l.manifest.Tables {
		if manifest.Rows > 0 {
			l.recordLoadedRows(table, manifest.Rows)
		}
	}

	if manifest, ok := l.manifest.Tables[chspool.TableDirFilterAll]; ok && manifest.Rows > 0 {
		l.recordLoadedRows(chspool.TableChildFilterAll, manifest.Rows)
	}
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

	return l.timeImportPhase(importPhasePartitionDropReset, func() error {
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
	return l.timeImportPhase(importPhaseChildFilterAllInsert, func() error {
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

func (l *summariseSpoolLoader) loadTables(ctx context.Context) error { //nolint:funlen,gocyclo,gocognit,cyclop
	if err := l.loadDirs(ctx); err != nil {
		return err
	}

	if err := l.loadFiles(ctx); err != nil {
		return err
	}

	if err := l.loadDirFacts(ctx); err != nil {
		return err
	}

	if err := l.loadDirFilterAgeAll(ctx); err != nil {
		return err
	}

	if err := l.loadDirFilterAll(ctx); err != nil {
		return err
	}

	if err := l.deriveChildFilterAll(ctx); err != nil {
		return err
	}

	if err := l.verifyLoadedCounts(ctx, summariseSpoolSnapshotStageTables()); err != nil {
		return err
	}

	if err := l.loadDirProjectionSets(ctx); err != nil {
		return err
	}

	if err := l.loadSchema3SnapshotSets(ctx); err != nil {
		return err
	}

	readinessTables := []string{chspool.TableDirProjectionSets, chspool.TableSchema3SnapshotSets}
	if err := l.verifyLoadedCounts(ctx, readinessTables); err != nil {
		return err
	}

	if err := l.loadActiveVirtualDirs(ctx); err != nil {
		return err
	}

	if err := l.loadActiveVirtualSummaries(ctx); err != nil {
		return err
	}

	if err := l.loadActiveVirtualFilterAll(ctx); err != nil {
		return err
	}

	if err := l.loadActiveVirtualChildren(ctx); err != nil {
		return err
	}

	if err := l.verifyLoadedCounts(ctx, summariseSpoolActiveVirtualStageTables()); err != nil {
		return err
	}

	if err := l.validateSpoolActiveVirtualCatalog(); err != nil {
		return err
	}

	if err := l.loadActiveVirtualSets(ctx); err != nil {
		return err
	}

	if err := l.verifyLoadedCounts(ctx, []string{chspool.TableActiveVirtualSets}); err != nil {
		return err
	}

	if err := l.loadBasedirsHistory(ctx); err != nil {
		return err
	}

	if err := l.loadBasedirsGroupUsage(ctx); err != nil {
		return err
	}

	if err := l.loadBasedirsUserUsage(ctx); err != nil {
		return err
	}

	if err := l.loadBasedirsSubdirs(ctx, chspool.TableBasedirsGroupSubdirs); err != nil {
		return err
	}

	if err := l.loadBasedirsSubdirs(ctx, chspool.TableBasedirsUserSubdirs); err != nil {
		return err
	}

	return l.verifyLoadedCounts(ctx, summariseSpoolBasedirsTables())
}

func summariseSpoolSnapshotStageTables() []string {
	return []string{
		chspool.TableDirs,
		chspool.TableFiles,
		chspool.TableDirFacts,
		chspool.TableDirFilterAgeAll,
		chspool.TableDirFilterAll,
	}
}

func summariseSpoolActiveVirtualStageTables() []string {
	return []string{
		chspool.TableActiveVirtualDirs,
		chspool.TableActiveVirtualSummaries,
		chspool.TableActiveVirtualFilterAll,
		chspool.TableActiveVirtualChildren,
	}
}

func summariseSpoolBasedirsTables() []string {
	return []string{
		chspool.TableBasedirsGroupUsage,
		chspool.TableBasedirsUserUsage,
		chspool.TableBasedirsGroupSubdirs,
		chspool.TableBasedirsUserSubdirs,
	}
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
	return l.timeImportPhase(importPhaseBasedirsHistory, func() error {
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
			conn:      l.conn,
			query:     insertBasedirsHistoryPoint,
			name:      chspool.TableBasedirsHistory,
			batch:     &batch,
			openedAt:  &openedAt,
			writeErr:  &writeErr,
			batchSize: defaultBatchSize,
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

	return l.timeImportPhase(phase, func() error {
		var (
			batch    driver.Batch
			openedAt time.Time
			writeErr error
		)

		writer := &importBlockWriter{
			conn:      l.conn,
			query:     query,
			name:      table,
			batch:     &batch,
			openedAt:  &openedAt,
			writeErr:  &writeErr,
			batchSize: summariseSpoolBatchSizeFor(table),
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

func (l *summariseSpoolLoader) verifyLoadedCounts(parent context.Context, tables []string) error {
	for _, table := range tables {
		expected := l.manifest.Tables[table].Rows
		if expected == 0 {
			continue
		}

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
	}

	return nil
}

func (l *summariseSpoolLoader) recordLoadedRows(table string, rows uint64) {
	if l.loadedRows == nil {
		l.loadedRows = make(map[string]uint64)
	}

	l.loadedRows[table] = rows
}

func (l *summariseSpoolLoader) countLoadedRows(parent context.Context, table string) (uint64, error) {
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

	writer := &dgutaWriter{
		cfg:                 l.cfg,
		conn:                l.conn,
		mountPath:           l.manifest.MountPath,
		updatedAt:           l.updatedAt,
		snapshot:            l.snapshot,
		importPhaseRecorder: l.importPhaseRecorder,
	}

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

func (l *summariseSpoolLoader) timeImportPhase(phase string, fn func() error) error {
	return timeImportPhase(func(p string, d time.Duration) {
		recordImportPhase(l.importPhaseRecorder, p, d)
	}, phase, fn)
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

	loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, recorder)
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
	report           perfreport.Report
	manifest         *chspool.Manifest
	recorder         func(string, time.Duration)
	phaseDurations   map[string]time.Duration
	loadedRows       map[string]uint64
	tableStatsStatus string
	tableStatsError  string
	started          time.Time
	usageBefore      summariseSpoolCPUUsage
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
	b.loadedRows = summariseSpoolLoadLoadedRows(loader, b.manifest)

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
		"loaded_table_rows":    b.loadedRows,
		"user_cpu_ms":          usage.userMS,
		"system_cpu_ms":        usage.systemMS,
		"total_cpu_ms":         usage.userMS + usage.systemMS,
		"peak_rss_bytes":       b.report.MaxRSSBytes,
		"spool_bytes":          summariseSpoolManifestBytes(b.manifest),
		"part_counts":          summariseSpoolPartCounts(b.report.TableStats),
		"retry_cleanup_result": b.retryCleanupResult(),
		"publish_latency_ms":   summariseSpoolDurationMSUint64(b.phaseDurations[importPhaseMountSwitch]),
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

	err := runSummariseSpoolLoad(ctx, cfg, spoolDir, manifest, builder.record, builder.collect)
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
