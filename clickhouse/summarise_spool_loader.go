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
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
)

const (
	summariseSpoolLoadPhasePrefix = "spool_load_"
	spoolHistoryDeleteChunk       = 512
)

var (
	errSummariseSpoolManifestRequired = errors.New("clickhouse: summarise spool manifest is required")
	errInvalidSummariseSpoolManifest  = errors.New("clickhouse: invalid summarise spool manifest")
	errSpoolDecodedRowsMismatch       = errors.New("clickhouse: spool decoded row count mismatch")
	errUnknownSpoolLoadTable          = errors.New("clickhouse: no loader query for spool table")
	errSpoolLoadedRowsMismatch        = errors.New("clickhouse: spool loaded row count mismatch")
)

type summariseSpoolLoader struct {
	cfg                 Config
	conn                driver.Conn
	dir                 string
	manifest            *chspool.Manifest
	snapshot            uuid.UUID
	updatedAt           time.Time
	importPhaseRecorder func(string, time.Duration)
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
		groupUsageDates:     map[uint32]finaliseQuotaDates{},
	}, nil
}

func (l *summariseSpoolLoader) load(parent context.Context) error {
	defer func() { _ = l.conn.Close() }()

	parent = loadParentContext(parent)

	if err := l.prepareSnapshot(parent); err != nil {
		return err
	}

	if err := l.loadTables(parent); err != nil {
		return err
	}

	return l.publish(parent)
}

func loadParentContext(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}

	return parent
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

	if err := decodeSpoolActiveSetIDs(l.dir, ids, chspool.TableActiveVirtualSummaries,
		func(row chspool.ActiveVirtualSummaryRow) string { return row.ActiveSetID }); err != nil {
		return nil, err
	}

	if err := decodeSpoolActiveSetIDs(l.dir, ids, chspool.TableActiveVirtualFilterAll,
		func(row chspool.ActiveVirtualFilterAllRow) string { return row.ActiveSetID }); err != nil {
		return nil, err
	}

	if err := decodeSpoolActiveSetIDs(l.dir, ids, chspool.TableActiveVirtualChildren,
		func(row chspool.ActiveVirtualChildRow) string { return row.ActiveSetID }); err != nil {
		return nil, err
	}

	if err := decodeSpoolActiveSetIDs(l.dir, ids, chspool.TableActiveVirtualSets,
		func(row chspool.ActiveVirtualSetRow) string { return row.ActiveSetID }); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}

	sort.Strings(out)

	return out, nil
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
	if err := l.loadFiles(ctx); err != nil {
		return err
	}

	if err := l.loadDirFacts(ctx); err != nil {
		return err
	}

	if err := l.loadChildren(ctx); err != nil {
		return err
	}

	if err := l.loadParentFacts(ctx); err != nil {
		return err
	}

	if err := l.loadDirFilterAgeAll(ctx); err != nil {
		return err
	}

	if err := l.loadChildFilterAll(ctx); err != nil {
		return err
	}

	if err := l.loadDirFilterAll(ctx); err != nil {
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
		chspool.TableFiles,
		chspool.TableDirFacts,
		chspool.TableChildren,
		chspool.TableParentFacts,
		chspool.TableDirFilterAgeAll,
		chspool.TableChildFilterAll,
		chspool.TableDirFilterAll,
	}
}

func summariseSpoolActiveVirtualStageTables() []string {
	return []string{
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
					row.ParentDir,
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
					row.Dir,
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

func (l *summariseSpoolLoader) loadChildren(parent context.Context) error {
	return loadSimpleSpoolTable(parent, l, chspool.TableChildren, func(batch driver.Batch, row chspool.ChildRow) error {
		return batch.Append(row.MountPath, row.SnapshotID, row.ParentDir, row.Child)
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
				row.Dir,
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

func (l *summariseSpoolLoader) loadChildFilterAll(parent context.Context) error { //nolint:dupl
	return loadSimpleSpoolTable(parent, l, chspool.TableChildFilterAll, func(
		batch driver.Batch,
		row chspool.ChildFilterAllRow,
	) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.ParentDir,
			row.Age,
			row.GID,
			row.UID,
			row.FT,
			row.Dir,
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

func (l *summariseSpoolLoader) loadDirFilterAll(parent context.Context) error { //nolint:dupl
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
			row.Dir,
			row.ParentDir,
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

func (l *summariseSpoolLoader) loadParentFacts(parent context.Context) error { //nolint:funlen
	return loadSimpleSpoolTable(parent, l, chspool.TableParentFacts, func(
		batch driver.Batch,
		row chspool.ParentFactRow,
	) error {
		return batch.Append(
			row.MountPath,
			row.SnapshotID,
			row.ParentDir,
			row.Dir,
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
			row.DirFactsRows,
			row.ParentFactsRows,
			row.ChildrenRows,
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
			row.Dir,
			row.MountPath,
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
			row.Dir,
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
			row.ParentDir,
			row.ChildDir,
			row.MountPath,
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

	return l.insertEligibleHistoryRows(parent, rows)
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

	for chunk := range slices.Chunk(uniqueRows, spoolHistoryDeleteChunk) {
		query, args := summariseSpoolHistoryDeleteQuery(chunk)

		ctx, cancel := l.cleanupContext(parent)
		if err := l.conn.Exec(ctx, query, args...); err != nil {
			cancel()

			return fmt.Errorf("clickhouse: failed to delete retry basedirs history rows: %w", err)
		}

		cancel()
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

func summariseSpoolHistoryDeleteQuery(rows []chspool.BasedirsHistoryRow) (string, []any) {
	var b strings.Builder
	b.WriteString("ALTER TABLE wrstat_basedirs_history DELETE WHERE (mount_path, gid, date) IN (")

	const historyDeleteArgsPerRow = 3

	args := make([]any, 0, len(rows)*historyDeleteArgsPerRow)

	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, ?, ?)")

		args = append(args, row.MountPath, row.GID, row.Date)
	}

	b.WriteString(") SETTINGS mutations_sync = 2")

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
		for _, row := range rows {
			skip, err := l.historyAlreadyRecorded(parent, row)
			if err != nil {
				return err
			}

			if skip {
				continue
			}

			ctx, cancel := l.queryContext(parent)
			if err := l.conn.Exec(
				ctx,
				insertBasedirsHistoryPoint,
				row.MountPath,
				row.GID,
				row.Date,
				row.UsageSize,
				row.QuotaSize,
				row.UsageInodes,
				row.QuotaInodes,
			); err != nil {
				cancel()

				return fmt.Errorf("clickhouse: failed to insert basedirs history point: %w", err)
			}

			cancel()
		}

		return nil
	})
}

func (l *summariseSpoolLoader) historyAlreadyRecorded(
	parent context.Context,
	row chspool.BasedirsHistoryRow,
) (bool, error) {
	ctx, cancel := l.queryContext(parent)
	defer cancel()

	result, err := l.conn.Query(ctx, queryBasedirsHistoryLastDate, row.MountPath, row.GID)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query basedirs history last date: %w", err)
	}

	defer func() { _ = result.Close() }()

	return scanHistoryLastDate(result, row.Date)
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
						row.BaseDir,
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
						row.BaseDir,
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
					row.BaseDir,
					row.Age,
					row.Pos,
					row.SubDir,
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
	case chspool.TableFiles:
		return insertFilesBatchQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableDirFacts:
		return insertMountDirSummaryQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableDirFilterAgeAll:
		return insertDirFilterAgeAllQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableParentFacts:
		return insertParentFactsQuery, importPhaseParentFactsInsert, nil
	case chspool.TableChildFilterAll:
		return insertChildFilterAllQuery, importPhaseFullFilterAllInsert, nil
	case chspool.TableDirFilterAll:
		return insertDirFilterAllQuery, importPhaseFullFilterAllInsert, nil
	case chspool.TableSchema3SnapshotSets:
		return insertSchema3SnapshotSetQuery, importPhaseSchema3Ready, nil
	case chspool.TableActiveVirtualSummaries:
		return insertActiveVirtualSummaryQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualFilterAll:
		return insertActiveVirtualFilterAllQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualChildren:
		return insertActiveVirtualChildQuery, importPhaseActiveVirtualInsert, nil
	case chspool.TableActiveVirtualSets:
		return insertActiveVirtualSetQuery, importPhaseActiveVirtualReady, nil
	case chspool.TableChildren:
		return insertChildrenQuery, summariseSpoolLoadPhasePrefix + table, nil
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

func summariseSpoolBatchSizeFor(table string) int {
	switch table {
	case chspool.TableDirFacts,
		chspool.TableDirFilterAgeAll,
		chspool.TableParentFacts,
		chspool.TableChildFilterAll,
		chspool.TableDirFilterAll,
		chspool.TableActiveVirtualSummaries,
		chspool.TableActiveVirtualFilterAll:
		return projectionBatchSizeFor(defaultBatchSize)
	case chspool.TableChildren, chspool.TableActiveVirtualChildren:
		return childrenBatchSizeFor(defaultBatchSize)
	default:
		return defaultBatchSize
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
	}

	return nil
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
		chspool.TableFiles: countLoadedSpoolRowsQuery(chspool.TableFiles),
		chspool.TableDirFacts: countLoadedSpoolRowsQuery(
			chspool.TableDirFacts,
		),
		chspool.TableDirFilterAgeAll: countLoadedSpoolRowsQuery(
			chspool.TableDirFilterAgeAll,
		),
		chspool.TableParentFacts: countLoadedSpoolRowsQuery(
			chspool.TableParentFacts,
		),
		chspool.TableChildren: countLoadedSpoolRowsQuery(
			chspool.TableChildren,
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
	writer := &dgutaWriter{
		cfg:                 l.cfg,
		conn:                l.conn,
		mountPath:           l.manifest.MountPath,
		updatedAt:           l.updatedAt,
		snapshot:            l.snapshot,
		importPhaseRecorder: l.importPhaseRecorder,
	}

	ctx, cancel := l.queryContext(parent)
	defer cancel()

	return writer.switchSnapshotAndDropOld(ctx)
}

func (l *summariseSpoolLoader) timeImportPhase(phase string, fn func() error) error {
	return timeImportPhase(func(p string, d time.Duration) {
		recordImportPhase(l.importPhaseRecorder, p, d)
	}, phase, fn)
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

	return loader.load(ctx)
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
