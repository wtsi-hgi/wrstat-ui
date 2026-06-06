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

	if err := l.verifyLoadedCounts(parent); err != nil {
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
		return dropSnapshotPartitionsForMount(
			parent,
			l.conn,
			l.manifest.MountPath,
			l.snapshot.String(),
			allPartitionDropQueries(),
		)
	})
}

func (l *summariseSpoolLoader) loadTables(ctx context.Context) error { //nolint:funlen,gocyclo
	if err := l.loadFiles(ctx); err != nil {
		return err
	}

	if err := l.loadDirFacts(ctx); err != nil {
		return err
	}

	if err := l.loadChildren(ctx); err != nil {
		return err
	}

	if err := l.loadDirProjectionSets(ctx); err != nil {
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

	return l.loadBasedirsSubdirs(ctx, chspool.TableBasedirsUserSubdirs)
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

		ctx, cancel := l.queryContext(parent)
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

func summariseSpoolTableQuery(table string) (string, string, error) {
	switch table {
	case chspool.TableFiles:
		return insertFilesBatchQuery, summariseSpoolLoadPhasePrefix + table, nil
	case chspool.TableDirFacts:
		return insertMountDirSummaryQuery, summariseSpoolLoadPhasePrefix + table, nil
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
			batchSize: defaultBatchSize,
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

func (l *summariseSpoolLoader) verifyLoadedCounts(parent context.Context) error {
	for table, query := range summariseSpoolCountQueries() {
		expected := l.manifest.Tables[table].Rows
		if expected == 0 {
			continue
		}

		ctx, cancel := l.queryContext(parent)

		var got uint64
		if err := l.conn.QueryRow(ctx, query, l.manifest.MountPath, l.snapshot.String()).Scan(&got); err != nil {
			cancel()

			return fmt.Errorf("clickhouse: failed to count loaded spool table %s: %w", table, err)
		}

		cancel()

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

func summariseSpoolCountQueries() map[string]string {
	return map[string]string{
		chspool.TableFiles: countLoadedSpoolRowsQuery("wrstat_files"),
		chspool.TableDirFacts: countLoadedSpoolRowsQuery(
			"wrstat_dir_facts",
		),
		chspool.TableChildren: countLoadedSpoolRowsQuery(
			"wrstat_children",
		),
		chspool.TableDirProjectionSets: countLoadedSpoolRowsQuery(
			"wrstat_dir_projection_sets",
		),
		chspool.TableBasedirsGroupUsage: countLoadedSpoolRowsQuery(
			"wrstat_basedirs_group_usage",
		),
		chspool.TableBasedirsUserUsage: countLoadedSpoolRowsQuery(
			"wrstat_basedirs_user_usage",
		),
		chspool.TableBasedirsGroupSubdirs: countLoadedSpoolRowsQuery(
			"wrstat_basedirs_group_subdirs",
		),
		chspool.TableBasedirsUserSubdirs: countLoadedSpoolRowsQuery(
			"wrstat_basedirs_user_subdirs",
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

	return nil
}

func sameHistoryDeleteRow(a, b chspool.BasedirsHistoryRow) bool {
	return a.MountPath == b.MountPath &&
		a.GID == b.GID &&
		a.Date.Equal(b.Date)
}

func countLoadedSpoolRowsQuery(table string) string {
	return "SELECT count() FROM " + table + " WHERE mount_path = ? AND snapshot_id = toUUID(?)"
}
