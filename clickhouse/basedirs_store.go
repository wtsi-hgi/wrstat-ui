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
		"(mount_path, snapshot_id, gid, basedir, age, uids, usage_size, quota_size, usage_inodes, quota_inodes, " +
		"mtime, date_no_space, date_no_files) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsUserUsageQuery = "INSERT INTO wrstat_basedirs_user_usage " +
		"(mount_path, snapshot_id, uid, basedir, age, gids, usage_size, quota_size, usage_inodes, quota_inodes, " +
		"mtime) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsGroupSubdirsQuery = "INSERT INTO wrstat_basedirs_group_subdirs " +
		"(mount_path, snapshot_id, gid, basedir, age, pos, subdir, num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	insertBasedirsUserSubdirsQuery = "INSERT INTO wrstat_basedirs_user_subdirs " +
		"(mount_path, snapshot_id, uid, basedir, age, pos, subdir, num_files, size_files, last_modified, file_usage) " +
		"VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	queryBasedirsHistoryLastDate = "SELECT max(date) FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ?"
	insertBasedirsHistoryPoint   = "INSERT INTO wrstat_basedirs_history " +
		"(mount_path, gid, date, usage_size, quota_size, usage_inodes, quota_inodes) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?)"
	queryBasedirsHistorySeries = "SELECT date, usage_size, quota_size, usage_inodes, quota_inodes " +
		"FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ? ORDER BY date ASC"
)

var errStoreNotReset = errors.New("clickhouse: basedirs store not reset")

type chBaseDirsStore struct {
	cfg Config

	conn ch.Conn

	batchSize int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	reset bool

	groupUsageBatch driver.Batch
	userUsageBatch  driver.Batch
	groupSubBatch   driver.Batch
	userSubBatch    driver.Batch

	bufferedAgeAllGroupUsage map[uint32][]*basedirs.Usage

	closed bool
}

func (s *chBaseDirsStore) SetMountPath(mountPath string) {
	s.mountPath = mountPath
}

func (s *chBaseDirsStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
}

func (s *chBaseDirsStore) Reset() error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}

	if s.mountPath == "" {
		return errMountPathRequired
	}

	if s.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	if err := s.abortExistingBatches(); err != nil {
		return err
	}

	s.snapshot = snapshotID(s.mountPath, s.updatedAt)
	s.bufferedAgeAllGroupUsage = map[uint32][]*basedirs.Usage{}
	s.reset = false

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(s.cfg))
	defer cancel()

	if err := s.dropSnapshotPartitions(ctx); err != nil {
		return err
	}

	batchCtx := context.WithoutCancel(ctx)
	if err := s.prepareBatches(batchCtx); err != nil {
		return err
	}

	s.reset = true

	return nil
}

func (s *chBaseDirsStore) PutGroupUsage(u *basedirs.Usage) error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}
	if u == nil {
		return nil
	}

	// Per spec: delay insertion for age=all to compute quota dates in Finalise.
	if u.Age == db.DGUTAgeAll {
		s.bufferedAgeAllGroupUsage[u.GID] = append(s.bufferedAgeAllGroupUsage[u.GID], u)
		return nil
	}

	return s.appendGroupUsage(u, unixEpochUTC(), unixEpochUTC())
}

func (s *chBaseDirsStore) PutUserUsage(u *basedirs.Usage) error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}
	if u == nil {
		return nil
	}

	if err := s.userUsageBatch.Append(
		s.mountPath,
		s.snapshot.String(),
		u.UID,
		u.BaseDir,
		uint8(u.Age),
		ensureNonNilUInt32s(u.GIDs),
		u.UsageSize,
		u.QuotaSize,
		u.UsageInodes,
		u.QuotaInodes,
		u.Mtime,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to append basedirs user usage: %w", err)
	}

	return s.flushFullBatches()
}

func (s *chBaseDirsStore) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}

	for pos, sd := range subdirs {
		if sd == nil {
			continue
		}

		if err := s.groupSubBatch.Append(
			s.mountPath,
			s.snapshot.String(),
			key.ID,
			key.BaseDir,
			uint8(key.Age),
			uint32(pos),
			sd.SubDir,
			sd.NumFiles,
			sd.SizeFiles,
			sd.LastModified,
			usageBreakdownToCHMap(sd.FileUsage),
		); err != nil {
			return fmt.Errorf("clickhouse: failed to append basedirs group subdir: %w", err)
		}

		if err := s.flushFullBatches(); err != nil {
			return err
		}
	}

	return nil
}

func (s *chBaseDirsStore) PutUserSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}

	for pos, sd := range subdirs {
		if sd == nil {
			continue
		}

		if err := s.userSubBatch.Append(
			s.mountPath,
			s.snapshot.String(),
			key.ID,
			key.BaseDir,
			uint8(key.Age),
			uint32(pos),
			sd.SubDir,
			sd.NumFiles,
			sd.SizeFiles,
			sd.LastModified,
			usageBreakdownToCHMap(sd.FileUsage),
		); err != nil {
			return fmt.Errorf("clickhouse: failed to append basedirs user subdir: %w", err)
		}

		if err := s.flushFullBatches(); err != nil {
			return err
		}
	}

	return nil
}

func (s *chBaseDirsStore) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(s.cfg))
	defer cancel()

	rows, err := s.conn.Query(ctx, queryBasedirsHistoryLastDate, key.MountPath, key.GID)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query basedirs history last date: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var last *time.Time
	if rows.Next() {
		var scanned *time.Time
		if err := rows.Scan(&scanned); err != nil {
			return fmt.Errorf("clickhouse: failed to scan basedirs history last date: %w", err)
		}
		last = scanned
	}

	if last != nil && !point.Date.After(*last) {
		return nil
	}

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
		return fmt.Errorf("clickhouse: failed to insert basedirs history point: %w", err)
	}

	return nil
}

func (s *chBaseDirsStore) Finalise() error {
	if s == nil || s.conn == nil {
		return errClientClosed
	}
	if !s.reset {
		return errStoreNotReset
	}

	for gid, usages := range s.bufferedAgeAllGroupUsage {
		history, err := s.readHistorySeries(gid)
		if err != nil {
			return err
		}

		dateNoSpace, dateNoFiles := basedirs.DateQuotaFull(history)
		if dateNoSpace.IsZero() {
			dateNoSpace = unixEpochUTC()
		}
		if dateNoFiles.IsZero() {
			dateNoFiles = unixEpochUTC()
		}

		for _, u := range usages {
			if u == nil {
				continue
			}

			if err := s.appendGroupUsage(u, dateNoSpace, dateNoFiles); err != nil {
				return err
			}
		}
	}

	// Do not auto-flush here; Close() is the canonical flush point.
	return nil
}

func (s *chBaseDirsStore) Close() error {
	if s == nil || s.closed {
		return nil
	}

	s.closed = true

	if s.conn == nil {
		return nil
	}

	flushErr := s.flushAllBatches()
	closeErr := s.conn.Close()

	return errors.Join(flushErr, closeErr)
}

func (s *chBaseDirsStore) appendGroupUsage(u *basedirs.Usage, dateNoSpace, dateNoFiles time.Time) error {
	if err := s.groupUsageBatch.Append(
		s.mountPath,
		s.snapshot.String(),
		u.GID,
		u.BaseDir,
		uint8(u.Age),
		ensureNonNilUInt32s(u.UIDs),
		u.UsageSize,
		u.QuotaSize,
		u.UsageInodes,
		u.QuotaInodes,
		u.Mtime,
		dateNoSpace,
		dateNoFiles,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to append basedirs group usage: %w", err)
	}

	return s.flushFullBatches()
}

func (s *chBaseDirsStore) readHistorySeries(gid uint32) ([]basedirs.History, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(s.cfg))
	defer cancel()

	rows, err := s.conn.Query(ctx, queryBasedirsHistorySeries, s.mountPath, gid)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query basedirs history series: %w", err)
	}
	defer func() { _ = rows.Close() }()

	history := make([]basedirs.History, 0, 64)
	for rows.Next() {
		var h basedirs.History
		if err := rows.Scan(&h.Date, &h.UsageSize, &h.QuotaSize, &h.UsageInodes, &h.QuotaInodes); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan basedirs history series: %w", err)
		}

		history = append(history, h)
	}

	return history, nil
}

func (s *chBaseDirsStore) dropSnapshotPartitions(ctx context.Context) error {
	sid := s.snapshot.String()

	queries := [...]string{
		dropBasedirsGroupUsagePartitionQuery,
		dropBasedirsUserUsagePartitionQuery,
		dropBasedirsGroupSubdirsPartitionQuery,
		dropBasedirsUserSubdirsPartitionQuery,
	}

	for _, query := range queries {
		if err := dropPartitionIgnoreUnknown(ctx, s.conn, s.mountPath, sid, query); err != nil {
			return err
		}
	}

	return nil
}

func (s *chBaseDirsStore) prepareBatches(ctx context.Context) error {
	groupUsage, err := s.conn.PrepareBatch(ctx, insertBasedirsGroupUsageQuery, driver.WithReleaseConnection())
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare basedirs group usage batch: %w", err)
	}

	userUsage, err := s.conn.PrepareBatch(ctx, insertBasedirsUserUsageQuery, driver.WithReleaseConnection())
	if err != nil {
		_ = groupUsage.Abort()
		return fmt.Errorf("clickhouse: failed to prepare basedirs user usage batch: %w", err)
	}

	groupSub, err := s.conn.PrepareBatch(ctx, insertBasedirsGroupSubdirsQuery, driver.WithReleaseConnection())
	if err != nil {
		_ = userUsage.Abort()
		_ = groupUsage.Abort()
		return fmt.Errorf("clickhouse: failed to prepare basedirs group subdirs batch: %w", err)
	}

	userSub, err := s.conn.PrepareBatch(ctx, insertBasedirsUserSubdirsQuery, driver.WithReleaseConnection())
	if err != nil {
		_ = groupSub.Abort()
		_ = userUsage.Abort()
		_ = groupUsage.Abort()
		return fmt.Errorf("clickhouse: failed to prepare basedirs user subdirs batch: %w", err)
	}

	s.groupUsageBatch = groupUsage
	s.userUsageBatch = userUsage
	s.groupSubBatch = groupSub
	s.userSubBatch = userSub

	return nil
}

func (s *chBaseDirsStore) flushFullBatches() error {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(s.cfg))
	defer cancel()

	if s.groupUsageBatch != nil && s.groupUsageBatch.Rows() >= s.batchSize {
		if err := s.groupUsageBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send basedirs group usage batch: %w", err)
		}

		b, err := s.conn.PrepareBatch(context.WithoutCancel(ctx), insertBasedirsGroupUsageQuery, driver.WithReleaseConnection())
		if err != nil {
			return fmt.Errorf("clickhouse: failed to reprepare basedirs group usage batch: %w", err)
		}
		s.groupUsageBatch = b
	}

	if s.userUsageBatch != nil && s.userUsageBatch.Rows() >= s.batchSize {
		if err := s.userUsageBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send basedirs user usage batch: %w", err)
		}

		b, err := s.conn.PrepareBatch(context.WithoutCancel(ctx), insertBasedirsUserUsageQuery, driver.WithReleaseConnection())
		if err != nil {
			return fmt.Errorf("clickhouse: failed to reprepare basedirs user usage batch: %w", err)
		}
		s.userUsageBatch = b
	}

	if s.groupSubBatch != nil && s.groupSubBatch.Rows() >= s.batchSize {
		if err := s.groupSubBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send basedirs group subdirs batch: %w", err)
		}

		b, err := s.conn.PrepareBatch(context.WithoutCancel(ctx), insertBasedirsGroupSubdirsQuery, driver.WithReleaseConnection())
		if err != nil {
			return fmt.Errorf("clickhouse: failed to reprepare basedirs group subdirs batch: %w", err)
		}
		s.groupSubBatch = b
	}

	if s.userSubBatch != nil && s.userSubBatch.Rows() >= s.batchSize {
		if err := s.userSubBatch.Send(); err != nil {
			return fmt.Errorf("clickhouse: failed to send basedirs user subdirs batch: %w", err)
		}

		b, err := s.conn.PrepareBatch(context.WithoutCancel(ctx), insertBasedirsUserSubdirsQuery, driver.WithReleaseConnection())
		if err != nil {
			return fmt.Errorf("clickhouse: failed to reprepare basedirs user subdirs batch: %w", err)
		}
		s.userSubBatch = b
	}

	return nil
}

func (s *chBaseDirsStore) flushAllBatches() error {
	var out error

	if s.groupUsageBatch != nil {
		out = errors.Join(out, s.groupUsageBatch.Send())
		s.groupUsageBatch = nil
	}
	if s.userUsageBatch != nil {
		out = errors.Join(out, s.userUsageBatch.Send())
		s.userUsageBatch = nil
	}
	if s.groupSubBatch != nil {
		out = errors.Join(out, s.groupSubBatch.Send())
		s.groupSubBatch = nil
	}
	if s.userSubBatch != nil {
		out = errors.Join(out, s.userSubBatch.Send())
		s.userSubBatch = nil
	}

	if out == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to flush basedirs batches: %w", out)
}

func (s *chBaseDirsStore) abortExistingBatches() error {
	var out error

	if s.groupUsageBatch != nil {
		out = errors.Join(out, s.groupUsageBatch.Abort())
		s.groupUsageBatch = nil
	}
	if s.userUsageBatch != nil {
		out = errors.Join(out, s.userUsageBatch.Abort())
		s.userUsageBatch = nil
	}
	if s.groupSubBatch != nil {
		out = errors.Join(out, s.groupSubBatch.Abort())
		s.groupSubBatch = nil
	}
	if s.userSubBatch != nil {
		out = errors.Join(out, s.userSubBatch.Abort())
		s.userSubBatch = nil
	}

	if out == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to abort existing basedirs batches: %w", out)
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

func unixEpochUTC() time.Time {
	return time.Unix(0, 0).UTC()
}

func dropPartitionIgnoreUnknown(ctx context.Context, conn ch.Conn, mountPath, snapshotID, query string) error {
	err := conn.Exec(ctx, query, mountPath, snapshotID)
	if err == nil {
		return nil
	}

	var ex *proto.Exception
	if errors.As(err, &ex) {
		// ClickHouse returns UNKNOWN_PARTITION for first-time snapshots.
		if strings.Contains(ex.Message, "UNKNOWN_PARTITION") || strings.Contains(ex.Message, "Unknown partition") {
			return nil
		}
	}

	return fmt.Errorf("clickhouse: failed to drop partition: %w", err)
}

// NewBaseDirsStore returns a ClickHouse-backed basedirs.Store.
func NewBaseDirsStore(cfg Config) (basedirs.Store, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg))
	defer cancel()

	conn, err := connectAndBootstrap(ctx, opts, cfg.Database)
	if err != nil {
		return nil, err
	}

	return &chBaseDirsStore{cfg: cfg, conn: conn, batchSize: defaultBatchSize}, nil
}
