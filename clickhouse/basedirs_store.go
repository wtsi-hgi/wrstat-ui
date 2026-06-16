/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	insertBasedirsGroupUsageQuery = "INSERT INTO wrstat_basedirs_group_usage " +
		"(mount_path, snapshot_id, gid, basedir_id, basedir_external, age, uids, " +
		"usage_size, quota_size, usage_inodes, quota_inodes, " +
		"mtime, date_no_space, date_no_files) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsUserUsageQuery = "INSERT INTO wrstat_basedirs_user_usage " +
		"(mount_path, snapshot_id, uid, basedir_id, basedir_external, age, gids, " +
		"usage_size, quota_size, usage_inodes, quota_inodes, " +
		"mtime) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsGroupSubdirsQuery = "INSERT INTO wrstat_basedirs_group_subdirs " +
		"(mount_path, snapshot_id, gid, basedir_id, basedir_external, age, pos, " +
		"subdir_id, subdir_external, num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsUserSubdirsQuery = "INSERT INTO wrstat_basedirs_user_subdirs " +
		"(mount_path, snapshot_id, uid, basedir_id, basedir_external, age, pos, " +
		"subdir_id, subdir_external, num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	queryBasedirsHistoryLastDate = "SELECT max(date) FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ?"
	insertBasedirsHistoryPoint   = "INSERT INTO wrstat_basedirs_history " +
		"(mount_path, gid, date, usage_size, quota_size, usage_inodes, quota_inodes) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?)"
	queryBasedirsHistorySeries = "SELECT date, usage_size, quota_size, usage_inodes, quota_inodes " +
		"FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ? ORDER BY date ASC"
	queryBasedirsDirID = "SELECT dir_id FROM wrstat_dirs " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?) AND full_path = ? LIMIT 1"
)

const (
	importPhaseBasedirsReset      = "wrstat_basedirs_reset"
	importPhaseBasedirsGroupUsage = "wrstat_basedirs_group_usage_insert"
	importPhaseBasedirsUserUsage  = "wrstat_basedirs_user_usage_insert"
	importPhaseBasedirsGroupSubs  = "wrstat_basedirs_group_subdirs_insert"
	importPhaseBasedirsUserSubs   = "wrstat_basedirs_user_subdirs_insert"
	importPhaseBasedirsHistory    = "wrstat_basedirs_history_insert"
	importPhaseBasedirsFinalise   = "wrstat_basedirs_finalise"
	importPhaseBasedirsFlush      = "wrstat_basedirs_flush"
)

var (
	errStoreNotReset          = errors.New("clickhouse: basedirs store not reset")
	errSubDirPositionOverflow = errors.New("clickhouse: basedirs subdir position overflows UInt32")
)

const historyCap = 64

type historyRollbackEntry struct {
	date time.Time
	key  basedirs.HistoryKey
}

type finaliseQuotaDates struct {
	noSpace time.Time
	noFiles time.Time
}

type basedirsResolvedDirID struct {
	id    uint32
	found bool
}

func scanBasedirsDirID(rows iterRows) (basedirsResolvedDirID, error) {
	resolved := basedirsResolvedDirID{}
	if rows.Next() {
		if err := rows.Scan(&resolved.id); err != nil {
			return basedirsResolvedDirID{}, fmt.Errorf("clickhouse: failed to scan basedirs dir_id: %w", err)
		}

		resolved.found = true
	}

	if err := rowsErr(rows); err != nil {
		return basedirsResolvedDirID{}, fmt.Errorf("clickhouse: basedirs dir_id iteration error: %w", err)
	}

	return resolved, nil
}

type basedirsSubdirColumns struct {
	basedirID       uint32
	basedirExternal string
	subdirID        uint32
	subdirExternal  string
}

type batchSlot struct {
	batch    *driver.Batch
	openedAt *time.Time
	query    string
	name     string
}

type chBaseDirsStore struct {
	cfg Config

	conn ch.Conn

	batchSize int

	importPhaseRecorder func(string, time.Duration)

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	reset bool

	groupUsageBatch driver.Batch
	userUsageBatch  driver.Batch
	groupSubBatch   driver.Batch
	userSubBatch    driver.Batch

	groupUsageOpenedAt time.Time
	userUsageOpenedAt  time.Time
	groupSubOpenedAt   time.Time
	userSubOpenedAt    time.Time
	writeErr           error
	writePhase         string
	batchNow           func() time.Time

	bufferedAgeAllGroupUsage  map[uint32][]*basedirs.Usage
	insertedHistory           []historyRollbackEntry
	lastHistoryAppendInserted bool
	dirIDCache                map[string]basedirsResolvedDirID

	closed bool
}

func (s *chBaseDirsStore) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		s.batchSize = batchSize
	}
}

func (s *chBaseDirsStore) SetImportPhaseRecorder(recorder func(string, time.Duration)) {
	if s == nil {
		return
	}

	s.importPhaseRecorder = recorder
}

func (s *chBaseDirsStore) recordImportPhase(phase string, d time.Duration) {
	if s == nil {
		return
	}

	recordImportPhase(s.importPhaseRecorder, phase, d)
}

func (s *chBaseDirsStore) timeImportPhase(phase string, fn func() error) error {
	return timeImportPhase(s.recordImportPhase, phase, fn)
}

func (s *chBaseDirsStore) ensureSnapshotID() {
	if s.snapshot != uuid.Nil {
		return
	}

	s.snapshot = snapshotID(s.mountPath, s.updatedAt)
}

func (s *chBaseDirsStore) recordInsertedHistory(key basedirs.HistoryKey, date time.Time) {
	s.insertedHistory = append(s.insertedHistory, historyRollbackEntry{
		date: date.UTC(),
		key:  key,
	})
}

func (s *chBaseDirsStore) Abort() error {
	if s == nil {
		return nil
	}

	err := s.abortPendingBatches()
	if !s.abortNeedsCleanup() {
		s.clearRollbackState()

		return errors.Join(err, s.closeStoreConn())
	}

	err = errors.Join(err, s.abortWithCleanup())
	s.clearRollbackState()

	return err
}

func (s *chBaseDirsStore) abortPendingBatches() error {
	if s.conn == nil || s.closed {
		return nil
	}

	return s.abortExistingBatches()
}

func (s *chBaseDirsStore) abortNeedsCleanup() bool {
	return s.hasSnapshotCleanup() || len(s.insertedHistory) > 0
}

func (s *chBaseDirsStore) abortWithCleanup() error {
	rollbackConn, closeConn, connErr := s.rollbackConn()
	if connErr != nil {
		return connErr
	}

	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	return errors.Join(s.cleanupAbortedRun(ctx, rollbackConn), closeConn())
}

func (s *chBaseDirsStore) hasSnapshotCleanup() bool {
	return s.mountPath != "" && !s.updatedAt.IsZero()
}

func (s *chBaseDirsStore) rollbackConn() (ch.Conn, func() error, error) {
	if s.conn != nil && !s.closed {
		return s.conn, s.closeStoreConn, nil
	}

	conn, err := connectFromConfig(s.cfg)
	if err != nil {
		return nil, nil, err
	}

	return conn, conn.Close, nil
}

func (s *chBaseDirsStore) cleanupAbortedRun(ctx context.Context, conn ch.Conn) error {
	published, publishErr := s.snapshotPublished(ctx, conn)
	if publishErr != nil {
		return publishErr
	}

	if published {
		return nil
	}

	var err error

	if s.hasSnapshotCleanup() {
		s.ensureSnapshotID()
		err = errors.Join(err, s.dropSnapshotPartitionsWithConn(ctx, conn))
	}

	err = errors.Join(err, s.rollbackInsertedHistory(ctx, conn))

	return err
}

func (s *chBaseDirsStore) snapshotPublished(
	ctx context.Context,
	conn ch.Conn,
) (bool, error) {
	if !s.hasSnapshotCleanup() {
		return false, nil
	}

	s.ensureSnapshotID()

	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, s.mountPath)
	if err != nil {
		return false, err
	}

	return hasActive && activeSID == s.snapshot.String(), nil
}

func (s *chBaseDirsStore) rollbackInsertedHistory(ctx context.Context, conn ch.Conn) error {
	if len(s.insertedHistory) == 0 {
		return nil
	}

	rollbackByDate := make(map[time.Time]map[basedirs.HistoryKey]struct{})

	for _, entry := range s.insertedHistory {
		keys := rollbackByDate[entry.date]
		if keys == nil {
			keys = make(map[basedirs.HistoryKey]struct{})
			rollbackByDate[entry.date] = keys
		}

		keys[entry.key] = struct{}{}
	}

	var err error

	for date, keySet := range rollbackByDate {
		keys := make([]basedirs.HistoryKey, 0, len(keySet))
		for key := range keySet {
			keys = append(keys, key)
		}

		query, args := deleteBasedirsHistoryRollbackQuery(date, keys)
		err = errors.Join(err, rollbackHistoryRows(ctx, conn, query, args...))
	}

	return err
}

func deleteBasedirsHistoryRollbackQuery(date time.Time, keys []basedirs.HistoryKey) (string, []any) {
	var b strings.Builder
	b.WriteString("ALTER TABLE wrstat_basedirs_history DELETE WHERE date = ? AND (mount_path, gid) IN (")

	args := make([]any, 0, 1+len(keys)*2)
	args = append(args, date)

	for i, key := range keys {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, ?)")

		args = append(args, key.MountPath, key.GID)
	}

	b.WriteString(") SETTINGS mutations_sync = 2")

	return b.String(), args
}

func rollbackHistoryRows(ctx context.Context, conn ch.Conn, query string, args ...any) error {
	if err := conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("clickhouse: failed to rollback basedirs history: %w", err)
	}

	return nil
}

func (s *chBaseDirsStore) clearRollbackState() {
	s.insertedHistory = nil
	s.bufferedAgeAllGroupUsage = nil
	s.lastHistoryAppendInserted = false
	s.dirIDCache = nil
	s.reset = false
	s.writePhase = ""
}

func (s *chBaseDirsStore) closeStoreConn() error {
	if s == nil || s.conn == nil {
		return nil
	}

	conn := s.conn
	s.conn = nil
	s.closed = true

	return conn.Close()
}

func (s *chBaseDirsStore) dropSnapshotPartitionsWithConn(ctx context.Context, conn ch.Conn) error {
	return dropSnapshotPartitionsForMount(ctx, conn, s.mountPath, s.snapshot.String(), basedirsPartitionDropQueries())
}

func (s *chBaseDirsStore) SetMountPath(mountPath string) {
	s.mountPath = mountPath
}

func (s *chBaseDirsStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
}

func (s *chBaseDirsStore) Reset() error {
	return s.timeImportPhase(importPhaseBasedirsReset, s.resetStore)
}

func (s *chBaseDirsStore) resetStore() error {
	if err := s.validateReadyForReset(); err != nil {
		return err
	}

	if err := s.abortExistingBatches(); err != nil {
		return err
	}

	s.resetSnapshotState()

	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	if err := refuseActiveSnapshotRewrite(ctx, s.conn, s.mountPath, s.snapshot); err != nil {
		return err
	}

	if err := s.dropSnapshotPartitions(ctx); err != nil {
		return err
	}

	s.reset = true

	return nil
}

func (s *chBaseDirsStore) resetSnapshotState() {
	s.snapshot = snapshotID(s.mountPath, s.updatedAt)
	s.bufferedAgeAllGroupUsage = map[uint32][]*basedirs.Usage{}
	s.insertedHistory = nil
	s.lastHistoryAppendInserted = false
	s.dirIDCache = map[string]basedirsResolvedDirID{}
	s.reset = false
	s.writePhase = ""
}

func (s *chBaseDirsStore) validateReadyForReset() error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}

	if s.mountPath == "" {
		return errMountPathRequired
	}

	if s.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	return nil
}

func (s *chBaseDirsStore) ensureReady() error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}

	if s.writeErr != nil {
		return s.writeErr
	}

	if !s.reset {
		return errStoreNotReset
	}

	return nil
}

func (s *chBaseDirsStore) PutGroupUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	// Per spec: delay insertion for age=all to compute quota dates in Finalise.
	if u.Age == db.DGUTAgeAll {
		s.bufferedAgeAllGroupUsage[u.GID] = append(s.bufferedAgeAllGroupUsage[u.GID], u)

		return nil
	}

	epoch := unixEpochUTC()

	return s.appendGroupUsage(u, epoch, epoch)
}

func unixEpochUTC() time.Time {
	return time.Unix(0, 0).UTC()
}

func (s *chBaseDirsStore) PutUserUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	return s.appendUserUsage(u)
}

func (s *chBaseDirsStore) basedirsPathColumns(catalogPath, external string) (uint32, string, error) {
	id, found, err := s.resolveBasedirsDirID(catalogPath)
	if err != nil {
		return 0, "", err
	}

	if found {
		return id, "", nil
	}

	return 0, external, nil
}

func (s *chBaseDirsStore) resolveBasedirsDirID(path string) (uint32, bool, error) {
	if !basedirsCanResolvePath(path) {
		return 0, false, nil
	}

	path = ensureTrailingSlash(path)
	if cached, ok := s.cachedBasedirsDirID(path); ok {
		return cached.id, cached.found, nil
	}

	resolved, err := s.queryBasedirsDirID(path)
	if err != nil {
		return 0, false, err
	}

	s.dirIDCache[path] = resolved

	return resolved.id, resolved.found, nil
}

func basedirsCanResolvePath(path string) bool {
	return path != "" && strings.HasPrefix(path, "/")
}

func (s *chBaseDirsStore) cachedBasedirsDirID(path string) (basedirsResolvedDirID, bool) {
	if s.dirIDCache == nil {
		s.dirIDCache = map[string]basedirsResolvedDirID{}
	}

	if cached, ok := s.dirIDCache[path]; ok {
		return cached, true
	}

	return basedirsResolvedDirID{}, false
}

func (s *chBaseDirsStore) queryBasedirsDirID(path string) (basedirsResolvedDirID, error) {
	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	rows, err := s.conn.Query(ctx, queryBasedirsDirID, s.mountPath, s.snapshot.String(), path)
	if err != nil {
		return basedirsResolvedDirID{}, fmt.Errorf("clickhouse: failed to resolve basedirs dir_id: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanBasedirsDirID(rows)
}

func (s *chBaseDirsStore) appendUserUsage(u *basedirs.Usage) error {
	basedirID, basedirExternal, resolveErr := s.basedirsPathColumns(u.BaseDir, u.BaseDir)
	if resolveErr != nil {
		return resolveErr
	}

	return s.timeImportPhase(importPhaseBasedirsUserUsage, func() error {
		if phaseErr := s.transitionWritePhase(importPhaseBasedirsUserUsage); phaseErr != nil {
			return phaseErr
		}

		return s.appendToBatch(s.userUsageSlot(), func(batch driver.Batch) error {
			return appendBasedirsUserUsageBatch(batch, s, u, basedirID, basedirExternal)
		})
	})
}

func appendBasedirsUserUsageBatch(
	batch driver.Batch,
	s *chBaseDirsStore,
	u *basedirs.Usage,
	basedirID uint32,
	basedirExternal string,
) error {
	if err := batch.Append(
		s.mountPath, s.snapshot.String(), u.UID, basedirID, basedirExternal,
		uint8(u.Age), ensureNonNilUInt32s(u.GIDs), u.UsageSize, u.QuotaSize,
		u.UsageInodes, u.QuotaInodes, u.Mtime,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to append basedirs user usage: %w", err)
	}

	return nil
}

func (s *chBaseDirsStore) PutGroupSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.appendSubDirs(&s.groupSubBatch, key, subdirs, "group", importPhaseBasedirsGroupSubs)
}

func (s *chBaseDirsStore) PutUserSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.appendSubDirs(&s.userSubBatch, key, subdirs, "user", importPhaseBasedirsUserSubs)
}

func (s *chBaseDirsStore) appendSubDirs(
	batch *driver.Batch,
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
	kind string,
	phase string,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	for pos, sd := range subdirs {
		if sd == nil {
			continue
		}

		if uint64(pos) > math.MaxUint32 {
			return fmt.Errorf("%w: %s position %d", errSubDirPositionOverflow, kind, pos)
		}

		if err := s.appendOneSubDir(batch, key, sd, uint32(pos), kind, phase); err != nil {
			return err
		}
	}

	return nil
}

func (s *chBaseDirsStore) appendOneSubDir(
	batch *driver.Batch,
	key basedirs.SubDirKey,
	sd *basedirs.SubDir,
	pos uint32,
	kind string,
	phase string,
) error {
	cols, resolveErr := s.basedirsSubdirColumns(key, sd)
	if resolveErr != nil {
		return resolveErr
	}

	return s.timeImportPhase(phase, func() error {
		if phaseErr := s.transitionWritePhase(phase); phaseErr != nil {
			return phaseErr
		}

		return s.appendToBatch(s.subdirSlot(batch, kind), func(prepared driver.Batch) error {
			return appendBasedirsSubDirBatch(
				prepared, s.mountPath, s.snapshot.String(),
				key, sd, cols, pos, kind,
			)
		})
	})
}

func appendBasedirsSubDirBatch(
	batch driver.Batch,
	mountPath, snapshot string,
	key basedirs.SubDirKey,
	sd *basedirs.SubDir,
	cols basedirsSubdirColumns,
	pos uint32,
	kind string,
) error {
	if err := batch.Append(
		mountPath, snapshot, key.ID, cols.basedirID, cols.basedirExternal, uint8(key.Age), pos,
		cols.subdirID, cols.subdirExternal, sd.NumFiles, sd.SizeFiles, sd.LastModified,
		usageBreakdownToCHMap(sd.FileUsage),
	); err != nil {
		return fmt.Errorf(
			"clickhouse: failed to append basedirs %s subdir: %w",
			kind, err,
		)
	}

	return nil
}

func (s *chBaseDirsStore) basedirsSubdirColumns(
	key basedirs.SubDirKey,
	sd *basedirs.SubDir,
) (basedirsSubdirColumns, error) {
	basedirID, basedirExternal, err := s.basedirsPathColumns(key.BaseDir, key.BaseDir)
	if err != nil {
		return basedirsSubdirColumns{}, err
	}

	subdirPath := basedirsSubdirCatalogPath(key.BaseDir, sd.SubDir)

	subdirID, subdirExternal, err := s.basedirsPathColumns(subdirPath, sd.SubDir)
	if err != nil {
		return basedirsSubdirColumns{}, err
	}

	return basedirsSubdirColumns{basedirID, basedirExternal, subdirID, subdirExternal}, nil
}

func basedirsSubdirCatalogPath(basedir, subdir string) string {
	if subdir == "." {
		return basedir
	}

	if strings.HasPrefix(subdir, "/") {
		return subdir
	}

	return ensureTrailingSlash(basedir) + strings.TrimSuffix(subdir, "/") + "/"
}

func (s *chBaseDirsStore) AppendGroupHistory(
	key basedirs.HistoryKey,
	point basedirs.History,
) error {
	return s.timeImportPhase(importPhaseBasedirsHistory, func() error {
		return s.appendGroupHistory(key, point)
	})
}

func (s *chBaseDirsStore) appendGroupHistory(
	key basedirs.HistoryKey,
	point basedirs.History,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	s.lastHistoryAppendInserted = false

	if err := s.transitionWritePhase(importPhaseBasedirsHistory); err != nil {
		return err
	}

	skip, err := s.historyAlreadyRecorded(key, point.Date)
	if err != nil {
		return err
	}

	if skip {
		return nil
	}

	err = s.insertHistoryPoint(key, point)

	s.lastHistoryAppendInserted = err == nil
	if err == nil {
		s.recordInsertedHistory(key, point.Date)
	}

	return err
}

func (s *chBaseDirsStore) finalise() error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if err := s.transitionWritePhase(importPhaseBasedirsFinalise); err != nil {
		return err
	}

	datesByGID, err := s.readFinaliseQuotaDates()
	if err != nil {
		return err
	}

	for gid, usages := range s.bufferedAgeAllGroupUsage {
		dates := datesByGID[gid]
		if err := s.appendFinaliseGIDUsages(usages, dates.noSpace, dates.noFiles); err != nil {
			return err
		}
	}

	return nil
}

func (s *chBaseDirsStore) close() error {
	s.closed = true

	if s.conn == nil {
		return nil
	}

	flushErr := s.flushAllBatches()
	closeErr := s.closeStoreConn()

	return errors.Join(flushErr, closeErr)
}

func sendAndReprepareBatch(
	ctx context.Context,
	conn ch.Conn,
	batch driver.Batch,
	query string,
) (driver.Batch, error) {
	if err := batch.Send(); err != nil {
		return batch, fmt.Errorf("clickhouse: failed to send batch: %w", err)
	}

	b, err := prepareImportBatch(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to reprepare batch: %w", err)
	}

	return b, nil
}

func (s *chBaseDirsStore) groupUsageSlot() batchSlot {
	return batchSlot{&s.groupUsageBatch, &s.groupUsageOpenedAt, insertBasedirsGroupUsageQuery, "group usage"}
}

func (s *chBaseDirsStore) userUsageSlot() batchSlot {
	return batchSlot{&s.userUsageBatch, &s.userUsageOpenedAt, insertBasedirsUserUsageQuery, "user usage"}
}

func (s *chBaseDirsStore) subdirSlot(batch *driver.Batch, kind string) batchSlot {
	if batch == &s.groupSubBatch || kind == "group" {
		return batchSlot{&s.groupSubBatch, &s.groupSubOpenedAt, insertBasedirsGroupSubdirsQuery, "group subdirs"}
	}

	return batchSlot{&s.userSubBatch, &s.userSubOpenedAt, insertBasedirsUserSubdirsQuery, "user subdirs"}
}

func (s *chBaseDirsStore) batchWriter(slot batchSlot) *importBlockWriter {
	return &importBlockWriter{
		conn:      s.conn,
		query:     slot.query,
		name:      "basedirs " + slot.name,
		batch:     slot.batch,
		openedAt:  slot.openedAt,
		writeErr:  &s.writeErr,
		batchSize: s.batchSize,
		now:       s.importBatchNow,
	}
}

func (s *chBaseDirsStore) transitionWritePhase(phase string) error {
	if s.writePhase == "" || s.writePhase == phase {
		s.writePhase = phase

		return nil
	}

	if err := s.flushAllBatches(); err != nil {
		return err
	}

	s.writePhase = phase

	return nil
}

func (s *chBaseDirsStore) importBatchNow() time.Time {
	if s != nil && s.batchNow != nil {
		return s.batchNow()
	}

	return time.Now()
}

func (s *chBaseDirsStore) appendToBatch(slot batchSlot, appendRow func(driver.Batch) error) error {
	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	return s.batchWriter(slot).append(ctx, appendRow)
}

func (s *chBaseDirsStore) closeBatches() error {
	var out error

	for _, slot := range s.batchSlots() {
		out = errors.Join(out, s.batchWriter(slot).close())
	}

	if out == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to flush basedirs batches: %w", out)
}

func appendBasedirsGroupUsageBatch(
	batch driver.Batch,
	s *chBaseDirsStore,
	u *basedirs.Usage,
	basedirID uint32,
	basedirExternal string,
	dateNoSpace time.Time,
	dateNoFiles time.Time,
) error {
	if err := batch.Append(
		s.mountPath, s.snapshot.String(), u.GID, basedirID, basedirExternal,
		uint8(u.Age), ensureNonNilUInt32s(u.UIDs), u.UsageSize, u.QuotaSize,
		u.UsageInodes, u.QuotaInodes, u.Mtime, dateNoSpace, dateNoFiles,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to append basedirs group usage: %w", err)
	}

	return nil
}

func usageBreakdownToCHMap(in basedirs.UsageBreakdownByType) map[uint16]uint64 {
	if in == nil {
		return map[uint16]uint64{}
	}

	out := make(map[uint16]uint64, len(in))
	for ft, v := range in {
		out[uint16(ft)] = v
	}

	return out
}

func (s *chBaseDirsStore) LastHistoryAppendInserted() bool {
	return s != nil && s.lastHistoryAppendInserted
}

func (s *chBaseDirsStore) historyAlreadyRecorded(
	key basedirs.HistoryKey,
	date time.Time,
) (bool, error) {
	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	rows, err := s.conn.Query(
		ctx, queryBasedirsHistoryLastDate, key.MountPath, key.GID,
	)
	if err != nil {
		return false, fmt.Errorf(
			"clickhouse: failed to query basedirs history last date: %w",
			err,
		)
	}

	defer func() { _ = rows.Close() }()

	return scanHistoryLastDate(rows, date)
}

func scanHistoryLastDate(
	rows driver.Rows,
	date time.Time,
) (bool, error) {
	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: basedirs history last date iteration error"); iterErr != nil {
			return false, iterErr
		}

		return false, nil
	}

	var last *time.Time

	if err := rows.Scan(&last); err != nil {
		return false, fmt.Errorf(
			"clickhouse: failed to scan basedirs history last date: %w",
			err,
		)
	}

	return last != nil && !date.After(*last), nil
}

func (s *chBaseDirsStore) insertHistoryPoint(
	key basedirs.HistoryKey,
	point basedirs.History,
) error {
	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	if err := s.conn.Exec(
		ctx,
		insertBasedirsHistoryPoint,
		key.MountPath,
		key.GID,
		point.Date,
		point.UsageSize,
		point.QuotaSize,
		point.UsageInodes,
		point.QuotaInodes,
	); err != nil {
		return fmt.Errorf(
			"clickhouse: failed to insert basedirs history point: %w",
			err,
		)
	}

	return nil
}

func (s *chBaseDirsStore) Finalise() error {
	return s.timeImportPhase(importPhaseBasedirsFinalise, s.finalise)
}

func (s *chBaseDirsStore) readFinaliseQuotaDates() (map[uint32]finaliseQuotaDates, error) {
	out := make(map[uint32]finaliseQuotaDates, len(s.bufferedAgeAllGroupUsage))

	for gid := range s.bufferedAgeAllGroupUsage {
		history, err := s.readHistorySeries(gid)
		if err != nil {
			return nil, err
		}

		dateNoSpace, dateNoFiles := basedirs.DateQuotaFull(history)
		out[gid] = finaliseQuotaDates{
			noSpace: unixEpochIfZero(dateNoSpace),
			noFiles: unixEpochIfZero(dateNoFiles),
		}
	}

	return out, nil
}

func (s *chBaseDirsStore) appendFinaliseGIDUsages(
	usages []*basedirs.Usage,
	dateNoSpace time.Time,
	dateNoFiles time.Time,
) error {
	for _, u := range usages {
		if u == nil {
			continue
		}

		if err := s.appendGroupUsage(u, dateNoSpace, dateNoFiles); err != nil {
			return err
		}
	}

	return nil
}

func unixEpochIfZero(t time.Time) time.Time {
	if t.IsZero() {
		return unixEpochUTC()
	}

	return t
}

func (s *chBaseDirsStore) Close() error {
	if s == nil || s.closed {
		return nil
	}

	return s.timeImportPhase(importPhaseBasedirsFlush, s.close)
}

func (s *chBaseDirsStore) appendGroupUsage(u *basedirs.Usage, dateNoSpace, dateNoFiles time.Time) error {
	basedirID, basedirExternal, resolveErr := s.basedirsPathColumns(u.BaseDir, u.BaseDir)
	if resolveErr != nil {
		return resolveErr
	}

	return s.timeImportPhase(importPhaseBasedirsGroupUsage, func() error {
		if phaseErr := s.transitionWritePhase(importPhaseBasedirsGroupUsage); phaseErr != nil {
			return phaseErr
		}

		return s.appendToBatch(s.groupUsageSlot(), func(batch driver.Batch) error {
			return appendBasedirsGroupUsageBatch(
				batch, s, u, basedirID, basedirExternal, dateNoSpace, dateNoFiles,
			)
		})
	})
}

func (s *chBaseDirsStore) readHistorySeries(
	gid uint32,
) ([]basedirs.History, error) {
	ctx, cancel := configQueryContext(s.cfg)
	defer cancel()

	rows, err := s.conn.Query(
		ctx, queryBasedirsHistorySeries, s.mountPath, gid,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"clickhouse: failed to query basedirs history series: %w",
			err,
		)
	}

	defer func() { _ = rows.Close() }()

	return collectHistoryRows(rows)
}

func collectHistoryRows(
	rows driver.Rows,
) ([]basedirs.History, error) {
	history := make([]basedirs.History, 0, historyCap)

	for rows.Next() {
		var h basedirs.History

		if err := rows.Scan(
			&h.Date, &h.UsageSize, &h.QuotaSize,
			&h.UsageInodes, &h.QuotaInodes,
		); err != nil {
			return nil, fmt.Errorf(
				"clickhouse: failed to scan basedirs history series: %w",
				err,
			)
		}

		history = append(history, h)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"clickhouse: basedirs history series iteration error: %w",
			err,
		)
	}

	return history, nil
}

func (s *chBaseDirsStore) dropSnapshotPartitions(ctx context.Context) error {
	return s.dropSnapshotPartitionsWithConn(ctx, s.conn)
}

func (s *chBaseDirsStore) batchSlots() [4]batchSlot {
	return [...]batchSlot{
		s.groupUsageSlot(),
		s.userUsageSlot(),
		{&s.groupSubBatch, &s.groupSubOpenedAt, insertBasedirsGroupSubdirsQuery, "group subdirs"},
		{&s.userSubBatch, &s.userSubOpenedAt, insertBasedirsUserSubdirsQuery, "user subdirs"},
	}
}

func (s *chBaseDirsStore) flushAllBatches() error {
	return s.closeBatches()
}

func (s *chBaseDirsStore) abortExistingBatches() error {
	var out error

	for _, slot := range s.batchSlots() {
		out = errors.Join(out, s.batchWriter(slot).abort())
	}

	if out == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to abort existing basedirs batches: %w", out)
}

// NewBaseDirsStore returns a ClickHouse-backed basedirs.Store.
func NewBaseDirsStore(cfg Config) (basedirs.Store, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	conn, err := connectForImportFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &chBaseDirsStore{cfg: cfg, conn: conn, batchSize: defaultBatchSize}, nil
}

func dropPartitionIgnoreUnknown(
	ctx context.Context,
	conn ch.Conn,
	mountPath, snapshotID, query string,
) error {
	dropCtx, cancel := partitionDropContext(ctx)
	defer cancel()

	err := conn.Exec(dropCtx, query, mountPath, snapshotID)
	if err == nil {
		return nil
	}

	if isUnknownPartition(err) || isUnknownTable(err) {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to drop partition: %w", err)
}

func partitionDropContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	return queryContext(context.WithoutCancel(parent), activeSnapshotCleanupTimeout)
}

func isUnknownPartition(err error) bool {
	var ex *proto.Exception
	if !errors.As(err, &ex) {
		return false
	}

	// ClickHouse returns UNKNOWN_PARTITION for first-time snapshots.
	return strings.Contains(ex.Message, "UNKNOWN_PARTITION") ||
		strings.Contains(ex.Message, "Unknown partition")
}
