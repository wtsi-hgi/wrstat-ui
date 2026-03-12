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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const groupUsageQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	gid, basedir, uids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, date_no_space, date_no_files, age
FROM wrstat_basedirs_group_usage u
ANY INNER JOIN active a
ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
ORDER BY gid ASC, basedir ASC
`

const userUsageQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	uid, basedir, gids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, age
FROM wrstat_basedirs_user_usage u
ANY INNER JOIN active a
ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
ORDER BY uid ASC, basedir ASC
`

const groupSubDirsQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	subdir, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_group_subdirs s
ANY INNER JOIN active a
ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.gid = ? AND s.basedir = ? AND s.age = ?
ORDER BY s.pos ASC
`

const userSubDirsQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	subdir, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_user_subdirs s
ANY INNER JOIN active a
ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.uid = ? AND s.basedir = ? AND s.age = ?
ORDER BY s.pos ASC
`

const historyQuery = `
SELECT date, usage_size, quota_size, usage_inodes, quota_inodes
FROM wrstat_basedirs_history
WHERE mount_path = ? AND gid = ?
ORDER BY date ASC
`

const mountTimestampsQuery = "SELECT mount_path, updated_at FROM wrstat_mounts_active"

const groupUsageSnapshotQuery = `
SELECT
	gid, basedir, uids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, date_no_space, date_no_files, age
FROM wrstat_basedirs_group_usage u
WHERE u.age = ? AND %s
ORDER BY gid ASC, basedir ASC
`

const userUsageSnapshotQuery = `
SELECT
	uid, basedir, gids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, age
FROM wrstat_basedirs_user_usage u
WHERE u.age = ? AND %s
ORDER BY uid ASC, basedir ASC
`

const groupSubDirsSnapshotQuery = `
SELECT
	subdir, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_group_subdirs s
WHERE s.gid = ? AND s.basedir = ? AND s.age = ? AND %s
ORDER BY s.pos ASC
`

const userSubDirsSnapshotQuery = `
SELECT
	subdir, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_user_subdirs s
WHERE s.uid = ? AND s.basedir = ? AND s.age = ? AND %s
ORDER BY s.pos ASC
`

const infoGroupUsageSnapshotQuery = `
	SELECT count()
	FROM wrstat_basedirs_group_usage u
	WHERE u.age = ? AND %s
`

const infoUserUsageSnapshotQuery = `
	SELECT count()
	FROM wrstat_basedirs_user_usage u
	WHERE u.age = ? AND %s
`

const infoGroupHistorySnapshotQuery = `
	SELECT
		countDistinct((mount_path, gid)) AS group_mount_combos,
		count() AS group_histories
	FROM wrstat_basedirs_history
	WHERE %s
`

const infoGroupSubDirsSnapshotQuery = `
	SELECT
		countDistinct((gid, basedir)) AS group_subdir_combos,
		count() AS group_subdirs
	FROM wrstat_basedirs_group_subdirs s
	WHERE s.age = ? AND %s
`

const infoUserSubDirsSnapshotQuery = `
	SELECT
		countDistinct((uid, basedir)) AS user_subdir_combos,
		count() AS user_subdirs
	FROM wrstat_basedirs_user_subdirs s
	WHERE s.age = ? AND %s
`

type chBaseDirsReader struct {
	cfg Config

	conn ch.Conn

	snapshot *activeMountsSnapshot
	closed   atomic.Bool

	owners map[uint32]string

	groupCache *basedirs.GroupCache
	userCache  *basedirs.UserCache

	mountPoints basedirs.MountPoints
}

type iterRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

type groupUsageScanned struct {
	gid         uint32
	basedir     string
	uids        []uint32
	usageSize   uint64
	quotaSize   uint64
	usageInodes uint64
	quotaInodes uint64
	mtime       time.Time
	dateNoSpace time.Time
	dateNoFiles time.Time
	ageU8       uint8
}

func (s *groupUsageScanned) scanFrom(rows iterRows) error {
	return rows.Scan(
		&s.gid,
		&s.basedir,
		&s.uids,
		&s.usageSize,
		&s.quotaSize,
		&s.usageInodes,
		&s.quotaInodes,
		&s.mtime,
		&s.dateNoSpace,
		&s.dateNoFiles,
		&s.ageU8,
	)
}

func (s *groupUsageScanned) toUsage(r *chBaseDirsReader) *basedirs.Usage {
	return &basedirs.Usage{
		GID:         s.gid,
		UIDs:        s.uids,
		Name:        r.groupCache.GroupName(s.gid),
		Owner:       r.owners[s.gid],
		BaseDir:     s.basedir,
		UsageSize:   s.usageSize,
		QuotaSize:   s.quotaSize,
		UsageInodes: s.usageInodes,
		QuotaInodes: s.quotaInodes,
		Mtime:       s.mtime,
		DateNoSpace: s.dateNoSpace,
		DateNoFiles: s.dateNoFiles,
		Age:         db.DirGUTAge(s.ageU8),
	}
}

type userUsageScanned struct {
	uid         uint32
	basedir     string
	gids        []uint32
	usageSize   uint64
	quotaSize   uint64
	usageInodes uint64
	quotaInodes uint64
	mtime       time.Time
	ageU8       uint8
}

func (s *userUsageScanned) scanFrom(rows iterRows) error {
	return rows.Scan(
		&s.uid,
		&s.basedir,
		&s.gids,
		&s.usageSize,
		&s.quotaSize,
		&s.usageInodes,
		&s.quotaInodes,
		&s.mtime,
		&s.ageU8,
	)
}

func (s *userUsageScanned) toUsage(r *chBaseDirsReader) *basedirs.Usage {
	return &basedirs.Usage{
		UID:         s.uid,
		GIDs:        s.gids,
		Name:        r.userCache.UserName(s.uid),
		BaseDir:     s.basedir,
		UsageSize:   s.usageSize,
		QuotaSize:   s.quotaSize,
		UsageInodes: s.usageInodes,
		QuotaInodes: s.quotaInodes,
		Mtime:       s.mtime,
		Age:         db.DirGUTAge(s.ageU8),
	}
}

type subDirScanned struct {
	subdir       string
	numFiles     uint64
	sizeFiles    uint64
	lastModified time.Time
	usageMap     map[uint16]uint64
}

func (s *subDirScanned) scanFrom(rows iterRows) error {
	return rows.Scan(&s.subdir, &s.numFiles, &s.sizeFiles, &s.lastModified, &s.usageMap)
}

func (s *subDirScanned) toSubDir() *basedirs.SubDir {
	return &basedirs.SubDir{
		SubDir:       s.subdir,
		NumFiles:     s.numFiles,
		SizeFiles:    s.sizeFiles,
		LastModified: s.lastModified,
		FileUsage:    convertUsageMap(s.usageMap),
	}
}

func (r *chBaseDirsReader) snapshotGroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return r.snapshotUsage(
		age,
		groupUsageSnapshotQuery,
		"group",
		"u.mount_path",
		"u.snapshot_id",
		r.scanGroupUsageRows,
	)
}

func (r *chBaseDirsReader) snapshotUserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return r.snapshotUsage(
		age,
		userUsageSnapshotQuery,
		"user",
		"u.mount_path",
		"u.snapshot_id",
		r.scanUserUsageRows,
	)
}

func (r *chBaseDirsReader) snapshotUsage(
	age db.DirGUTAge,
	queryFmt, what, mountColumn, snapshotColumn string,
	scan func(iterRows) ([]*basedirs.Usage, error),
) ([]*basedirs.Usage, error) {
	mounts := r.snapshot.all()
	if len(mounts) == 0 {
		return []*basedirs.Usage{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	query, args := activeMountsQuery(
		queryFmt,
		mountColumn,
		snapshotColumn,
		mounts,
		uint8(age),
	)

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s usage: %w", what, err)
	}

	return scan(rows)
}

func (r *chBaseDirsReader) snapshotGroupSubDirs(
	gid uint32,
	basedir string,
	age db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	mounts := r.snapshot.all()
	if len(mounts) == 0 {
		return nil, basedirs.ErrNoSuchUserOrGroup
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	query, args := activeMountsQuery(
		groupSubDirsSnapshotQuery,
		"s.mount_path",
		"s.snapshot_id",
		mounts,
		gid,
		basedir,
		uint8(age),
	)

	return r.subDirs(ctx, "group", query, args...)
}

func (r *chBaseDirsReader) snapshotUserSubDirs(
	uid uint32,
	basedir string,
	age db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	mounts := r.snapshot.all()
	if len(mounts) == 0 {
		return nil, basedirs.ErrNoSuchUserOrGroup
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	query, args := activeMountsQuery(
		userSubDirsSnapshotQuery,
		"s.mount_path",
		"s.snapshot_id",
		mounts,
		uid,
		basedir,
		uint8(age),
	)

	return r.subDirs(ctx, "user", query, args...)
}

func (r *chBaseDirsReader) liveMountTimestamps(ctx context.Context) (map[string]time.Time, error) {
	rows, err := r.conn.Query(ctx, mountTimestampsQuery)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query mount timestamps: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanMountTimestamps(rows)
}

func scanMountTimestamps(rows iterRows) (map[string]time.Time, error) {
	out := make(map[string]time.Time)

	for rows.Next() {
		var (
			mountPath string
			updatedAt time.Time
		)

		if err := rows.Scan(&mountPath, &updatedAt); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan mount timestamps: %w", err)
		}

		mountKey := strings.ReplaceAll(mountPath, "/", "／")
		out[mountKey] = updatedAt
	}

	return out, nil
}

func (r *chBaseDirsReader) queryCountForSnapshot(
	ctx context.Context,
	queryFmt, mountColumn, snapshotColumn string,
	dest *int,
	mounts []activeMount,
	args ...any,
) error {
	if len(mounts) == 0 {
		*dest = 0

		return nil
	}

	query, queryArgs := activeMountsQuery(
		queryFmt,
		mountColumn,
		snapshotColumn,
		mounts,
		args...,
	)

	return r.queryCount(ctx, query, dest, queryArgs...)
}

func (r *chBaseDirsReader) queryCountPairForSnapshot(
	ctx context.Context,
	queryFmt, mountColumn, snapshotColumn string,
	destA, destB *int,
	mounts []activeMount,
	args ...any,
) error {
	if len(mounts) == 0 {
		*destA = 0
		*destB = 0

		return nil
	}

	query, queryArgs := activeMountsQuery(
		queryFmt,
		mountColumn,
		snapshotColumn,
		mounts,
		args...,
	)

	return r.queryCountPair(ctx, query, destA, destB, queryArgs...)
}

func (r *chBaseDirsReader) queryCountPairForActiveMounts(
	ctx context.Context,
	queryFmt, mountColumn string,
	destA, destB *int,
	mounts []activeMount,
	args ...any,
) error {
	if len(mounts) == 0 {
		*destA = 0
		*destB = 0

		return nil
	}

	query, queryArgs := activeMountPathsQuery(
		queryFmt,
		mountColumn,
		mounts,
		args...,
	)

	return r.queryCountPair(ctx, query, destA, destB, queryArgs...)
}

func (r *chBaseDirsReader) ensureOpen() error {
	if r == nil || r.closed.Load() {
		return errReaderClosed
	}

	return nil
}

func (r *chBaseDirsReader) GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotGroupUsage(age)
	}

	rows, err := r.queryUsageRows(groupUsageQuery, "group", age)
	if err != nil {
		return nil, err
	}

	return r.scanGroupUsageRows(rows)
}

func (r *chBaseDirsReader) queryUsageRows(
	query, what string,
	age db.DirGUTAge,
) (iterRows, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	rows, err := r.conn.Query(ctx, query, uint8(age))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s usage: %w", what, err)
	}

	return rows, nil
}

func (r *chBaseDirsReader) scanGroupUsageRows(rows iterRows) ([]*basedirs.Usage, error) {
	defer func() { _ = rows.Close() }()

	out := make([]*basedirs.Usage, 0)

	for rows.Next() {
		var s groupUsageScanned
		if err := s.scanFrom(rows); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan group usage: %w", err)
		}

		out = append(out, s.toUsage(r))
	}

	return out, nil
}

func (r *chBaseDirsReader) UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotUserUsage(age)
	}

	rows, err := r.queryUsageRows(userUsageQuery, "user", age)
	if err != nil {
		return nil, err
	}

	return r.scanUserUsageRows(rows)
}

func (r *chBaseDirsReader) scanUserUsageRows(rows iterRows) ([]*basedirs.Usage, error) {
	defer func() { _ = rows.Close() }()

	out := make([]*basedirs.Usage, 0)

	for rows.Next() {
		var s userUsageScanned
		if err := s.scanFrom(rows); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan user usage: %w", err)
		}

		out = append(out, s.toUsage(r))
	}

	return out, nil
}

func (r *chBaseDirsReader) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotGroupSubDirs(gid, basedir, age)
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	return r.subDirs(ctx, "group", groupSubDirsQuery, gid, basedir, uint8(age))
}

func (r *chBaseDirsReader) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotUserSubDirs(uid, basedir, age)
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	return r.subDirs(ctx, "user", userSubDirsQuery, uid, basedir, uint8(age))
}

func (r *chBaseDirsReader) subDirs(ctx context.Context, what, query string, args ...any) ([]*basedirs.SubDir, error) {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s subdirs: %w", what, err)
	}

	defer func() { _ = rows.Close() }()

	out := make([]*basedirs.SubDir, 0)

	for rows.Next() {
		var s subDirScanned
		if err := s.scanFrom(rows); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan %s subdirs: %w", what, err)
		}

		out = append(out, s.toSubDir())
	}

	if len(out) == 0 {
		return nil, basedirs.ErrNoSuchUserOrGroup
	}

	return out, nil
}

func convertUsageMap(m map[uint16]uint64) basedirs.UsageBreakdownByType {
	out := make(basedirs.UsageBreakdownByType, len(m))
	for k, v := range m {
		out[db.DirGUTAFileType(k)] = v
	}

	return out
}

func (r *chBaseDirsReader) History(gid uint32, path string) ([]basedirs.History, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	mp := r.mountPoints.PrefixOf(path)
	if mp == "" {
		return nil, basedirs.ErrInvalidBasePath
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	rows, err := r.conn.Query(ctx, historyQuery, mp, gid)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query history: %w", err)
	}

	return r.scanHistoryRows(rows)
}

func (r *chBaseDirsReader) scanHistoryRows(rows iterRows) ([]basedirs.History, error) {
	defer func() { _ = rows.Close() }()

	out := make([]basedirs.History, 0)

	for rows.Next() {
		var h basedirs.History
		if err := rows.Scan(&h.Date, &h.UsageSize, &h.QuotaSize, &h.UsageInodes, &h.QuotaInodes); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan history: %w", err)
		}

		out = append(out, h)
	}

	if len(out) == 0 {
		return nil, basedirs.ErrNoBaseDirHistory
	}

	return out, nil
}

func (r *chBaseDirsReader) SetMountPoints(mountpoints []string) {
	r.mountPoints = basedirs.ValidateMountPoints(mountpoints)
}

func (r *chBaseDirsReader) SetCachedGroup(gid uint32, name string) {
	r.groupCache.SetCached(gid, name)
}

func (r *chBaseDirsReader) SetCachedUser(uid uint32, name string) {
	r.userCache.SetCached(uid, name)
}

func (r *chBaseDirsReader) MountTimestamps() (map[string]time.Time, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshot.mountTimestamps(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	return r.liveMountTimestamps(ctx)
}

func (r *chBaseDirsReader) Info() (*basedirs.DBInfo, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(r.cfg))
	defer cancel()

	info := &basedirs.DBInfo{}

	if err := r.fillInfo(ctx, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (r *chBaseDirsReader) fillInfo(ctx context.Context, info *basedirs.DBInfo) error {
	ageAll := uint8(db.DGUTAgeAll)

	if err := r.fillInfoGroupUsage(ctx, info, ageAll); err != nil {
		return err
	}

	if err := r.fillInfoUserUsage(ctx, info, ageAll); err != nil {
		return err
	}

	if err := r.fillInfoGroupHistory(ctx, info); err != nil {
		return err
	}

	if err := r.fillInfoGroupSubDirs(ctx, info, ageAll); err != nil {
		return err
	}

	return r.fillInfoUserSubDirs(ctx, info, ageAll)
}

func (r *chBaseDirsReader) fillInfoGroupUsage(ctx context.Context, info *basedirs.DBInfo, ageAll uint8) error {
	if r.snapshot != nil {
		return r.queryCountForSnapshot(
			ctx,
			infoGroupUsageSnapshotQuery,
			"u.mount_path",
			"u.snapshot_id",
			&info.GroupDirCombos,
			r.snapshot.all(),
			ageAll,
		)
	}

	query := `
		WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
		SELECT count()
		FROM wrstat_basedirs_group_usage u
		ANY INNER JOIN active a
		ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
		WHERE u.age = ?
	`

	return r.queryCount(ctx, query, &info.GroupDirCombos, ageAll)
}

func (r *chBaseDirsReader) fillInfoUserUsage(ctx context.Context, info *basedirs.DBInfo, ageAll uint8) error {
	if r.snapshot != nil {
		return r.queryCountForSnapshot(
			ctx,
			infoUserUsageSnapshotQuery,
			"u.mount_path",
			"u.snapshot_id",
			&info.UserDirCombos,
			r.snapshot.all(),
			ageAll,
		)
	}

	query := `
		WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
		SELECT count()
		FROM wrstat_basedirs_user_usage u
		ANY INNER JOIN active a
		ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
		WHERE u.age = ?
	`

	return r.queryCount(ctx, query, &info.UserDirCombos, ageAll)
}

func (r *chBaseDirsReader) fillInfoGroupHistory(ctx context.Context, info *basedirs.DBInfo) error {
	if r.snapshot != nil {
		return r.queryCountPairForActiveMounts(
			ctx,
			infoGroupHistorySnapshotQuery,
			"mount_path",
			&info.GroupMountCombos,
			&info.GroupHistories,
			r.snapshot.all(),
		)
	}

	query := `
		WITH active AS (SELECT DISTINCT mount_path FROM wrstat_mounts_active)
		SELECT
			countDistinct((h.mount_path, h.gid)) AS group_mount_combos,
			count() AS group_histories
		FROM wrstat_basedirs_history h
		ANY INNER JOIN active a
		ON h.mount_path = a.mount_path
	`

	return r.queryCountPair(ctx, query, &info.GroupMountCombos, &info.GroupHistories)
}

func (r *chBaseDirsReader) fillInfoGroupSubDirs(ctx context.Context, info *basedirs.DBInfo, ageAll uint8) error {
	if r.snapshot != nil {
		return r.queryCountPairForSnapshot(
			ctx,
			infoGroupSubDirsSnapshotQuery,
			"s.mount_path",
			"s.snapshot_id",
			&info.GroupSubDirCombos,
			&info.GroupSubDirs,
			r.snapshot.all(),
			ageAll,
		)
	}

	query := `
		WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
		SELECT
			countDistinct((gid, basedir)) AS group_subdir_combos,
			count() AS group_subdirs
		FROM wrstat_basedirs_group_subdirs s
		ANY INNER JOIN active a
		ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
		WHERE s.age = ?
	`

	return r.queryCountPair(ctx, query, &info.GroupSubDirCombos, &info.GroupSubDirs, ageAll)
}

func (r *chBaseDirsReader) fillInfoUserSubDirs(ctx context.Context, info *basedirs.DBInfo, ageAll uint8) error {
	if r.snapshot != nil {
		return r.queryCountPairForSnapshot(
			ctx,
			infoUserSubDirsSnapshotQuery,
			"s.mount_path",
			"s.snapshot_id",
			&info.UserSubDirCombos,
			&info.UserSubDirs,
			r.snapshot.all(),
			ageAll,
		)
	}

	query := `
		WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
		SELECT
			countDistinct((uid, basedir)) AS user_subdir_combos,
			count() AS user_subdirs
		FROM wrstat_basedirs_user_subdirs s
		ANY INNER JOIN active a
		ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
		WHERE s.age = ?
	`

	return r.queryCountPair(ctx, query, &info.UserSubDirCombos, &info.UserSubDirs, ageAll)
}

func (r *chBaseDirsReader) queryCount(ctx context.Context, query string, dest *int, args ...any) error {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query basedirs info: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		*dest = 0

		return nil
	}

	n, err := scanUint64(rows)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to scan basedirs info: %w", err)
	}

	i, err := safeUint64ToInt(n)
	if err != nil {
		return err
	}

	*dest = i

	return nil
}

func (r *chBaseDirsReader) queryCountPair(ctx context.Context, query string, destA, destB *int, args ...any) error {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query basedirs info: %w", err)
	}

	defer func() { _ = rows.Close() }()

	a, b, ok, err := scanUint64Pair(rows)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to scan basedirs info: %w", err)
	}

	if !ok {
		*destA = 0
		*destB = 0

		return nil
	}

	return setIntPairFromUint64(destA, destB, a, b)
}

func setIntPairFromUint64(destA, destB *int, a, b uint64) error {
	ai, err := safeUint64ToInt(a)
	if err != nil {
		return err
	}

	bi, err := safeUint64ToInt(b)
	if err != nil {
		return err
	}

	*destA = ai
	*destB = bi

	return nil
}

func scanUint64(rows iterRows) (uint64, error) {
	var n uint64
	if err := rows.Scan(&n); err != nil {
		return 0, err
	}

	return n, nil
}

func scanUint64Pair(rows iterRows) (uint64, uint64, bool, error) {
	if !rows.Next() {
		return 0, 0, false, nil
	}

	var a, b uint64
	if err := rows.Scan(&a, &b); err != nil {
		return 0, 0, false, err
	}

	return a, b, true, nil
}

func (r *chBaseDirsReader) Close() error {
	if r == nil {
		return nil
	}

	r.closed.Store(true)

	return nil
}

func newClickHouseBaseDirsReaderWithSnapshot(
	cfg Config,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) (basedirs.Reader, error) {
	mountPoints, err := mountPointsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	r := &chBaseDirsReader{
		cfg:         cfg,
		conn:        conn,
		snapshot:    snapshot,
		owners:      map[uint32]string{},
		groupCache:  basedirs.NewGroupCache(),
		userCache:   basedirs.NewUserCache(),
		mountPoints: mountPoints,
	}

	if cfg.OwnersCSVPath != "" {
		owners, err := basedirs.ParseOwners(cfg.OwnersCSVPath)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: failed to parse owners csv: %w", err)
		}

		r.owners = owners
	}

	return r, nil
}
