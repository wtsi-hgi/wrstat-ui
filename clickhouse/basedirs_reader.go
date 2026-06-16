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
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// ErrIDUnresolved is returned when an active in-snapshot path cannot be resolved
// to a wrstat_dirs dir_id during basedirs readiness checks.
var ErrIDUnresolved = errors.New("clickhouse: directory id unresolved")

const groupUsageQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	gid, if(u.basedir_external != '', u.basedir_external, d.full_path) AS basedir,
	uids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, date_no_space, date_no_files, age
FROM wrstat_basedirs_group_usage u
ANY INNER JOIN active a
ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
LEFT JOIN wrstat_dirs d
ON d.mount_path = u.mount_path AND d.snapshot_id = u.snapshot_id AND d.dir_id = u.basedir_id
WHERE u.age = ?
ORDER BY gid ASC, basedir ASC
`

const userUsageQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	uid, if(u.basedir_external != '', u.basedir_external, d.full_path) AS basedir,
	gids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, age
FROM wrstat_basedirs_user_usage u
ANY INNER JOIN active a
ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
LEFT JOIN wrstat_dirs d
ON d.mount_path = u.mount_path AND d.snapshot_id = u.snapshot_id AND d.dir_id = u.basedir_id
WHERE u.age = ?
ORDER BY uid ASC, basedir ASC
`

const groupSubDirsQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	if(s.basedir_external != '', s.basedir_external, base.full_path) AS basedir,
	if(s.subdir_external != '', s.subdir_external, sub.full_path) AS subdir,
	s.subdir_external, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_group_subdirs s
ANY INNER JOIN active a
ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
LEFT JOIN wrstat_dirs base
ON base.mount_path = s.mount_path AND base.snapshot_id = s.snapshot_id AND base.dir_id = s.basedir_id
LEFT JOIN wrstat_dirs sub
ON sub.mount_path = s.mount_path AND sub.snapshot_id = s.snapshot_id AND sub.dir_id = s.subdir_id
WHERE s.gid = ? AND (
	(s.basedir_external != '' AND s.basedir_external = ?)
	OR (s.basedir_external = '' AND base.full_path = ?)
) AND s.age = ?
ORDER BY s.pos ASC
`

const userSubDirsQuery = `
WITH active AS (
	SELECT mount_path, snapshot_id
	FROM wrstat_mounts_active
)
SELECT
	if(s.basedir_external != '', s.basedir_external, base.full_path) AS basedir,
	if(s.subdir_external != '', s.subdir_external, sub.full_path) AS subdir,
	s.subdir_external, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_user_subdirs s
ANY INNER JOIN active a
ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
LEFT JOIN wrstat_dirs base
ON base.mount_path = s.mount_path AND base.snapshot_id = s.snapshot_id AND base.dir_id = s.basedir_id
LEFT JOIN wrstat_dirs sub
ON sub.mount_path = s.mount_path AND sub.snapshot_id = s.snapshot_id AND sub.dir_id = s.subdir_id
WHERE s.uid = ? AND (
	(s.basedir_external != '' AND s.basedir_external = ?)
	OR (s.basedir_external = '' AND base.full_path = ?)
) AND s.age = ?
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
	gid, if(u.basedir_external != '', u.basedir_external, d.full_path) AS basedir,
	uids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, date_no_space, date_no_files, age
FROM wrstat_basedirs_group_usage u
LEFT JOIN wrstat_dirs d
ON d.mount_path = u.mount_path AND d.snapshot_id = u.snapshot_id AND d.dir_id = u.basedir_id
WHERE u.age = ? AND %s
ORDER BY gid ASC, basedir ASC
`

const userUsageSnapshotQuery = `
SELECT
	uid, if(u.basedir_external != '', u.basedir_external, d.full_path) AS basedir,
	gids, usage_size, quota_size, usage_inodes, quota_inodes,
	mtime, age
FROM wrstat_basedirs_user_usage u
LEFT JOIN wrstat_dirs d
ON d.mount_path = u.mount_path AND d.snapshot_id = u.snapshot_id AND d.dir_id = u.basedir_id
WHERE u.age = ? AND %s
ORDER BY uid ASC, basedir ASC
`

const groupSubDirsSnapshotQuery = `
SELECT
	if(s.basedir_external != '', s.basedir_external, base.full_path) AS basedir,
	if(s.subdir_external != '', s.subdir_external, sub.full_path) AS subdir,
	s.subdir_external, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_group_subdirs s
LEFT JOIN wrstat_dirs base
ON base.mount_path = s.mount_path AND base.snapshot_id = s.snapshot_id AND base.dir_id = s.basedir_id
LEFT JOIN wrstat_dirs sub
ON sub.mount_path = s.mount_path AND sub.snapshot_id = s.snapshot_id AND sub.dir_id = s.subdir_id
WHERE s.gid = ? AND (
	(s.basedir_external != '' AND s.basedir_external = ?)
	OR (s.basedir_external = '' AND base.full_path = ?)
) AND s.age = ? AND %s
ORDER BY s.pos ASC
`

const userSubDirsSnapshotQuery = `
SELECT
	if(s.basedir_external != '', s.basedir_external, base.full_path) AS basedir,
	if(s.subdir_external != '', s.subdir_external, sub.full_path) AS subdir,
	s.subdir_external, num_files, size_files, last_modified, file_usage
FROM wrstat_basedirs_user_subdirs s
LEFT JOIN wrstat_dirs base
ON base.mount_path = s.mount_path AND base.snapshot_id = s.snapshot_id AND base.dir_id = s.basedir_id
LEFT JOIN wrstat_dirs sub
ON sub.mount_path = s.mount_path AND sub.snapshot_id = s.snapshot_id AND sub.dir_id = s.subdir_id
WHERE s.uid = ? AND (
	(s.basedir_external != '' AND s.basedir_external = ?)
	OR (s.basedir_external = '' AND base.full_path = ?)
) AND s.age = ? AND %s
ORDER BY s.pos ASC
`

const infoGroupUsageSnapshotQuery = `
	SELECT count()
	FROM wrstat_basedirs_group_usage u
	WHERE u.age = ? AND %s
`

const infoGroupUsageQuery = `
	WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
	SELECT count()
	FROM wrstat_basedirs_group_usage u
	ANY INNER JOIN active a
	ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
	WHERE u.age = ?
`

const infoUserUsageSnapshotQuery = `
	SELECT count()
	FROM wrstat_basedirs_user_usage u
	WHERE u.age = ? AND %s
`

const infoUserUsageQuery = `
	WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
	SELECT count()
	FROM wrstat_basedirs_user_usage u
	ANY INNER JOIN active a
	ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
	WHERE u.age = ?
`

const infoGroupHistorySnapshotQuery = `
	SELECT
		countDistinct((mount_path, gid)) AS group_mount_combos,
		count() AS group_histories
	FROM wrstat_basedirs_history
	WHERE %s
`

const infoGroupHistoryQuery = `
	WITH active AS (SELECT DISTINCT mount_path FROM wrstat_mounts_active)
	SELECT
		countDistinct((h.mount_path, h.gid)) AS group_mount_combos,
		count() AS group_histories
	FROM wrstat_basedirs_history h
	ANY INNER JOIN active a
	ON h.mount_path = a.mount_path
`

const infoGroupSubDirsSnapshotQuery = `
	SELECT
		countDistinct((gid, basedir_id, basedir_external)) AS group_subdir_combos,
		count() AS group_subdirs
	FROM wrstat_basedirs_group_subdirs s
	WHERE s.age = ? AND %s
`

const infoGroupSubDirsQuery = `
	WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
	SELECT
		countDistinct((gid, basedir_id, basedir_external)) AS group_subdir_combos,
		count() AS group_subdirs
	FROM wrstat_basedirs_group_subdirs s
	ANY INNER JOIN active a
	ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
	WHERE s.age = ?
`

const infoUserSubDirsSnapshotQuery = `
	SELECT
		countDistinct((uid, basedir_id, basedir_external)) AS user_subdir_combos,
		count() AS user_subdirs
	FROM wrstat_basedirs_user_subdirs s
	WHERE s.age = ? AND %s
`

const infoUserSubDirsQuery = `
	WITH active AS (SELECT mount_path, snapshot_id FROM wrstat_mounts_active)
	SELECT
		countDistinct((uid, basedir_id, basedir_external)) AS user_subdir_combos,
		count() AS user_subdirs
	FROM wrstat_basedirs_user_subdirs s
	ANY INNER JOIN active a
	ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
	WHERE s.age = ?
`

const basedirsUsageUnresolvedQuery = `
SELECT count()
FROM %s u
LEFT JOIN wrstat_dirs id_dir
ON id_dir.mount_path = u.mount_path
AND id_dir.snapshot_id = u.snapshot_id
AND id_dir.dir_id = u.basedir_id
LEFT JOIN wrstat_dirs external_dir
ON external_dir.mount_path = u.mount_path
AND external_dir.snapshot_id = u.snapshot_id
AND external_dir.full_path = if(endsWith(u.basedir_external, '/'), u.basedir_external, concat(u.basedir_external, '/'))
WHERE %s AND (
	(u.basedir_external = '' AND id_dir.full_path = '')
	OR (
		u.basedir_external != ''
		AND startsWith(
			if(endsWith(u.basedir_external, '/'), u.basedir_external, concat(u.basedir_external, '/')),
			u.mount_path
		)
		AND external_dir.full_path = ''
	)
)
`

const basedirsSubdirsUnresolvedQuery = `
WITH
	if(endsWith(s.basedir_external, '/'), s.basedir_external, concat(s.basedir_external, '/')) AS basedir_external_path,
	if(s.basedir_external != '', basedir_external_path, base_id.full_path) AS basedir_path,
	if(
		s.subdir_external = '.',
		basedir_path,
		if(
			startsWith(s.subdir_external, '/'),
			if(endsWith(s.subdir_external, '/'), s.subdir_external, concat(s.subdir_external, '/')),
			concat(if(endsWith(basedir_path, '/'), basedir_path, concat(basedir_path, '/')), s.subdir_external, '/')
		)
	) AS subdir_external_path
SELECT count()
FROM %s s
LEFT JOIN wrstat_dirs base_id
ON base_id.mount_path = s.mount_path
AND base_id.snapshot_id = s.snapshot_id
AND base_id.dir_id = s.basedir_id
LEFT JOIN wrstat_dirs base_external
ON base_external.mount_path = s.mount_path
AND base_external.snapshot_id = s.snapshot_id
AND base_external.full_path = basedir_external_path
LEFT JOIN wrstat_dirs sub_id
ON sub_id.mount_path = s.mount_path
AND sub_id.snapshot_id = s.snapshot_id
AND sub_id.dir_id = s.subdir_id
LEFT JOIN wrstat_dirs sub_external
ON sub_external.mount_path = s.mount_path
AND sub_external.snapshot_id = s.snapshot_id
AND sub_external.full_path = subdir_external_path
WHERE %s AND (
	(s.basedir_external = '' AND base_id.full_path = '')
	OR (
		s.basedir_external != ''
		AND startsWith(basedir_external_path, s.mount_path)
		AND base_external.full_path = ''
	)
	OR (s.subdir_external = '' AND sub_id.full_path = '')
	OR (
		s.subdir_external != ''
		AND startsWith(subdir_external_path, s.mount_path)
		AND sub_external.full_path = ''
	)
)
`

type iterRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

type basedirsReadinessCheck struct {
	name  string
	table string
	query string
	alias string
}

func (r *chBaseDirsReader) ensureBasedirsIDsResolved(ctx context.Context) error {
	if r == nil || r.snapshot == nil {
		return nil
	}

	mounts := r.snapshot.all()
	if len(mounts) == 0 {
		return nil
	}

	for _, check := range basedirsReadinessChecks() {
		unresolved, err := r.countUnresolvedBasedirsRows(
			ctx, check.query, check.table, check.alias, mounts,
		)
		if err != nil {
			return err
		}

		if unresolved > 0 {
			return fmt.Errorf("%w: %s has %d unresolved active rows", ErrIDUnresolved, check.name, unresolved)
		}
	}

	return nil
}

func basedirsReadinessChecks() []basedirsReadinessCheck {
	return []basedirsReadinessCheck{
		{"basedirs group usage", "wrstat_basedirs_group_usage", basedirsUsageUnresolvedQuery, "u"},
		{"basedirs user usage", "wrstat_basedirs_user_usage", basedirsUsageUnresolvedQuery, "u"},
		{"basedirs group subdirs", "wrstat_basedirs_group_subdirs", basedirsSubdirsUnresolvedQuery, "s"},
		{"basedirs user subdirs", "wrstat_basedirs_user_subdirs", basedirsSubdirsUnresolvedQuery, "s"},
	}
}

func (r *chBaseDirsReader) countUnresolvedBasedirsRows(
	parent context.Context,
	queryFmt string,
	table string,
	alias string,
	mounts []activeMount,
) (uint64, error) {
	condition, args := activeMountsTupleCondition(alias+".mount_path", alias+".snapshot_id", mounts)
	query := fmt.Sprintf(queryFmt, table, condition)

	ctx, cancel := queryContext(parent, queryTimeout(r.cfg))
	defer cancel()

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: failed to query basedirs id readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanBasedirsUnresolvedCount(rows)
}

func scanBasedirsUnresolvedCount(rows iterRows) (uint64, error) {
	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: basedirs id readiness iteration error"); iterErr != nil {
			return 0, iterErr
		}

		return 0, nil
	}

	var count uint64
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("clickhouse: failed to scan basedirs id readiness: %w", err)
	}

	if err := rowsErr(rows); err != nil {
		return 0, fmt.Errorf("clickhouse: basedirs id readiness iteration error: %w", err)
	}

	return count, nil
}

func (r *chBaseDirsReader) loadOwners(path string) error {
	if path == "" {
		return nil
	}

	owners, err := basedirs.ParseOwners(path)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to parse owners csv: %w", err)
	}

	r.owners = owners

	return nil
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

	ctx, cancel := configQueryContext(r.cfg)
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
	return r.snapshotSubDirs(
		groupSubDirsSnapshotQuery,
		"group",
		gid,
		basedir,
		age,
	)
}

func (r *chBaseDirsReader) snapshotUserSubDirs(
	uid uint32,
	basedir string,
	age db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	return r.snapshotSubDirs(
		userSubDirsSnapshotQuery,
		"user",
		uid,
		basedir,
		age,
	)
}

func (r *chBaseDirsReader) snapshotSubDirs(
	queryFmt, what string,
	id uint32,
	basedir string,
	age db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	mounts := r.snapshot.all()
	if len(mounts) == 0 {
		return nil, basedirs.ErrNoSuchUserOrGroup
	}

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	query, args := activeMountsQuery(
		queryFmt,
		"s.mount_path",
		"s.snapshot_id",
		mounts,
		id,
		basedir,
		basedir,
		uint8(age),
	)

	return r.subDirs(ctx, what, query, args...)
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

		out[mountTimestampKey(mountPath)] = updatedAt
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: mount timestamp iteration error: %w", err)
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

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	rows, err := r.conn.Query(ctx, query, uint8(age))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s usage: %w", what, err)
	}

	return rows, nil
}

func (r *chBaseDirsReader) scanGroupUsageRows(rows iterRows) ([]*basedirs.Usage, error) {
	return scanUsageRows(rows, "group", func(rows iterRows) (*basedirs.Usage, error) {
		var row groupUsageScanned
		if err := row.scanFrom(rows); err != nil {
			return nil, err
		}

		return row.toUsage(r), nil
	})
}

func scanUsageRows(
	rows iterRows,
	what string,
	scan func(iterRows) (*basedirs.Usage, error),
) ([]*basedirs.Usage, error) {
	defer func() { _ = rows.Close() }()

	out := make([]*basedirs.Usage, 0)

	for rows.Next() {
		usage, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan %s usage: %w", what, err)
		}

		out = append(out, usage)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: %s usage iteration error: %w", what, err)
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
	return scanUsageRows(rows, "user", func(rows iterRows) (*basedirs.Usage, error) {
		var row userUsageScanned
		if err := row.scanFrom(rows); err != nil {
			return nil, err
		}

		return row.toUsage(r), nil
	})
}

func (r *chBaseDirsReader) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotGroupSubDirs(gid, basedir, age)
	}

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	return r.subDirs(ctx, "group", groupSubDirsQuery, gid, basedir, basedir, uint8(age))
}

func (r *chBaseDirsReader) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	if r.snapshot != nil {
		return r.snapshotUserSubDirs(uid, basedir, age)
	}

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	return r.subDirs(ctx, "user", userSubDirsQuery, uid, basedir, basedir, uint8(age))
}

func (r *chBaseDirsReader) subDirs(ctx context.Context, what, query string, args ...any) ([]*basedirs.SubDir, error) {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s subdirs: %w", what, err)
	}

	return scanSubDirRows(rows, what)
}

func scanSubDirRows(rows iterRows, what string) ([]*basedirs.SubDir, error) {
	defer func() { _ = rows.Close() }()

	out := make([]*basedirs.SubDir, 0)

	for rows.Next() {
		var s subDirScanned
		if err := s.scanFrom(rows); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan %s subdirs: %w", what, err)
		}

		out = append(out, s.toSubDir())
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: %s subdirs iteration error: %w", what, err)
	}

	if len(out) == 0 {
		return nil, basedirs.ErrNoSuchUserOrGroup
	}

	return out, nil
}

func (r *chBaseDirsReader) History(gid uint32, path string) ([]basedirs.History, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	mountPath := r.mountPoints.PrefixOf(path)
	if mountPath == "" {
		return nil, basedirs.ErrInvalidBasePath
	}

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	rows, err := r.conn.Query(ctx, historyQuery, mountPath, gid)
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

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: history iteration error: %w", err)
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

	ctx, cancel := configQueryContext(r.cfg)
	defer cancel()

	return r.liveMountTimestamps(ctx)
}

func (r *chBaseDirsReader) Info() (*basedirs.DBInfo, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	ctx, cancel := configQueryContext(r.cfg)
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

	return r.queryCount(ctx, infoGroupUsageQuery, &info.GroupDirCombos, ageAll)
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

	return r.queryCount(ctx, infoUserUsageQuery, &info.UserDirCombos, ageAll)
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

	return r.queryCountPair(ctx, infoGroupHistoryQuery, &info.GroupMountCombos, &info.GroupHistories)
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

	return r.queryCountPair(ctx, infoGroupSubDirsQuery, &info.GroupSubDirCombos, &info.GroupSubDirs, ageAll)
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

	return r.queryCountPair(ctx, infoUserSubDirsQuery, &info.UserSubDirCombos, &info.UserSubDirs, ageAll)
}

func (r *chBaseDirsReader) queryCount(ctx context.Context, query string, dest *int, args ...any) error {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to query basedirs info: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: basedirs info iteration error"); iterErr != nil {
			return iterErr
		}

		*dest = 0

		return nil
	}

	var n uint64
	if scanErr := rows.Scan(&n); scanErr != nil {
		return fmt.Errorf("clickhouse: failed to scan basedirs info: %w", scanErr)
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

	if !rows.Next() {
		if iterErr := rowIterationErr(rows, "clickhouse: basedirs info iteration error"); iterErr != nil {
			return iterErr
		}

		*destA = 0
		*destB = 0

		return nil
	}

	var a, b uint64
	if scanErr := rows.Scan(&a, &b); scanErr != nil {
		return fmt.Errorf("clickhouse: failed to scan basedirs info: %w", scanErr)
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

func (r *chBaseDirsReader) Close() error {
	if r == nil {
		return nil
	}

	r.closed.Store(true)

	return nil
}

func newClickHouseBaseDirsReaderWithSnapshot(
	ctx context.Context,
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

	if err := r.loadOwners(cfg.OwnersCSVPath); err != nil {
		return nil, err
	}

	if err := r.ensureBasedirsIDsResolved(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

type subDirScanned struct {
	basedir        string
	subdir         string
	subdirExternal string
	numFiles       uint64
	sizeFiles      uint64
	lastModified   time.Time
	usageMap       map[uint16]uint64
}

func (s *subDirScanned) scanFrom(rows iterRows) error {
	return rows.Scan(
		&s.basedir,
		&s.subdir,
		&s.subdirExternal,
		&s.numFiles,
		&s.sizeFiles,
		&s.lastModified,
		&s.usageMap,
	)
}

func (s *subDirScanned) toSubDir() *basedirs.SubDir {
	return &basedirs.SubDir{
		SubDir:       basedirsSubDirDisplay(s.basedir, s.subdir, s.subdirExternal),
		NumFiles:     s.numFiles,
		SizeFiles:    s.sizeFiles,
		LastModified: s.lastModified,
		FileUsage:    convertUsageMap(s.usageMap),
	}
}

func basedirsSubDirDisplay(basedir, subdir, external string) string {
	if external != "" {
		return subdir
	}

	base := ensureTrailingSlash(basedir)
	child := ensureTrailingSlash(subdir)

	if child == base {
		return "."
	}

	if rel, ok := strings.CutPrefix(child, base); ok {
		return strings.TrimSuffix(rel, "/")
	}

	return strings.TrimSuffix(subdir, "/")
}

func convertUsageMap(m map[uint16]uint64) basedirs.UsageBreakdownByType {
	out := make(basedirs.UsageBreakdownByType, len(m))
	for k, v := range m {
		out[db.DirGUTAFileType(k)] = v
	}

	return out
}
