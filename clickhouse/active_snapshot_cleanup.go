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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	activeSnapshotCleanupTimeout = 30 * time.Minute

	deleteActiveSnapshotMountRowsQuery = "ALTER TABLE wrstat_mounts DELETE WHERE mount_path = ? " +
		"AND active_snapshot = toUUID(?) SETTINGS mutations_sync = 2"
	latestSnapshotMountSwitchedAtQuery = "SELECT switched_at FROM wrstat_mounts " +
		"WHERE mount_path = ? ORDER BY switched_at DESC LIMIT 1"
	previousSnapshotMountRowQuery = "SELECT toString(active_snapshot), updated_at " +
		"FROM wrstat_mounts WHERE mount_path = ? AND active_snapshot != toUUID(?) " +
		"ORDER BY switched_at DESC LIMIT 1"
	rollbackActiveSnapshotMountRowQuery = "INSERT INTO wrstat_mounts " +
		"(mount_path, switched_at, active_snapshot, updated_at) " +
		"SELECT ?, ?, toUUID(?), ? WHERE EXISTS (" +
		"SELECT 1 FROM wrstat_mounts_active WHERE mount_path = ? AND snapshot_id = toUUID(?))"
)

var (
	errActiveSnapshotStillActive        = errors.New("clickhouse: failed to clean active snapshot mount row")
	errLatestSnapshotMountSwitchMissing = errors.New("clickhouse: latest snapshot mount switch time was not found")
)

type activeSnapshotMountRow struct {
	snapshotID string
	updatedAt  time.Time
}

func readPreviousSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
) (activeSnapshotMountRow, bool, error) {
	rows, err := conn.Query(ctx, previousSnapshotMountRowQuery, mountPath, sid)
	if err != nil {
		return activeSnapshotMountRow{}, false, fmt.Errorf("clickhouse: failed to read previous snapshot mount row: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		rowErr := rows.Err()
		if rowErr != nil {
			return activeSnapshotMountRow{}, false, fmt.Errorf(
				"clickhouse: previous snapshot mount row iteration error: %w",
				rowErr,
			)
		}

		return activeSnapshotMountRow{}, false, nil
	}

	row, err := scanActiveSnapshotMountRow(rows)
	if err != nil {
		return activeSnapshotMountRow{}, false, err
	}

	return row, true, nil
}

func scanActiveSnapshotMountRow(rows interface {
	Scan(dest ...any) error
}) (activeSnapshotMountRow, error) {
	var row activeSnapshotMountRow
	if err := rows.Scan(&row.snapshotID, &row.updatedAt); err != nil {
		return activeSnapshotMountRow{}, fmt.Errorf("clickhouse: failed to scan snapshot mount row: %w", err)
	}

	return row, nil
}

func cleanActiveSnapshotAttempt(ctx context.Context, conn ch.Conn, mountPath string, sid string) error {
	previous, hasPrevious, err := readPreviousSnapshotMountRow(ctx, conn, mountPath, sid)
	if err != nil {
		return err
	}

	if hasPrevious {
		err = rollbackActiveSnapshotMountRow(ctx, conn, mountPath, sid, previous)
	} else {
		err = deleteActiveSnapshotMountRows(ctx, conn, mountPath, sid)
	}

	if err != nil {
		return err
	}

	if err := ensureSnapshotInactive(ctx, conn, mountPath, sid); err != nil {
		return err
	}

	return dropSnapshotPartitionsForMount(ctx, conn, mountPath, sid, allPartitionDropQueries())
}

func ensureSnapshotInactive(ctx context.Context, conn ch.Conn, mountPath string, sid string) error {
	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
	if err != nil {
		return err
	}

	if hasActive && activeSID == sid {
		return fmt.Errorf(
			"%w: mount_path=%s snapshot_id=%s is still active",
			errActiveSnapshotStillActive,
			mountPath,
			sid,
		)
	}

	return nil
}

func rollbackActiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	failedSID string,
	previous activeSnapshotMountRow,
) error {
	switchedAt, err := nextSnapshotMountSwitchedAt(ctx, conn, mountPath)
	if err != nil {
		return err
	}

	if err := conn.Exec(
		ctx,
		rollbackActiveSnapshotMountRowQuery,
		mountPath,
		switchedAt,
		previous.snapshotID,
		previous.updatedAt,
		mountPath,
		failedSID,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to roll back active snapshot mount row: %w", err)
	}

	return nil
}

func nextSnapshotMountSwitchedAt(ctx context.Context, conn ch.Conn, mountPath string) (time.Time, error) {
	rows, err := conn.Query(ctx, latestSnapshotMountSwitchedAtQuery, mountPath)
	if err != nil {
		return time.Time{}, fmt.Errorf("clickhouse: failed to read latest snapshot mount switch time: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		rowErr := rows.Err()
		if rowErr != nil {
			return time.Time{}, fmt.Errorf("clickhouse: latest snapshot mount switch time iteration error: %w", rowErr)
		}

		return time.Time{}, errLatestSnapshotMountSwitchMissing
	}

	var latest time.Time
	if err := rows.Scan(&latest); err != nil {
		return time.Time{}, fmt.Errorf("clickhouse: failed to scan latest snapshot mount switch time: %w", err)
	}

	return nextMillisecondAfter(latest, time.Now().UTC()), nil
}

func nextMillisecondAfter(latest time.Time, now time.Time) time.Time {
	next := latest.Add(time.Millisecond)
	if now.Before(next) {
		return next
	}

	return now
}

// CleanActiveSnapshotAttempt removes an abandoned active deterministic snapshot
// for mountPath and updatedAt while preserving older snapshots for that mount.
func CleanActiveSnapshotAttempt(cfg Config, mountPath string, updatedAt time.Time) error {
	if err := validateSnapshotCleanupInputs(cfg, mountPath, updatedAt); err != nil {
		return err
	}

	conn, err := connectFromConfig(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	return cleanActiveSnapshotAttemptWithConn(cfg, conn, mountPath, updatedAt)
}

func validateSnapshotCleanupInputs(cfg Config, mountPath string, updatedAt time.Time) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	if mountPath == "" {
		return errMountPathRequired
	}

	if updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	return nil
}

func deleteActiveSnapshotMountRows(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
) error {
	if err := conn.Exec(ctx, deleteActiveSnapshotMountRowsQuery, mountPath, sid); err != nil {
		return fmt.Errorf("clickhouse: failed to delete active snapshot mount row: %w", err)
	}

	return nil
}

func cleanActiveSnapshotAttemptWithConn(
	cfg Config,
	conn ch.Conn,
	mountPath string,
	updatedAt time.Time,
) error {
	ctx, cancel := configQueryContext(cfg)
	defer cancel()

	sid := snapshotID(mountPath, updatedAt).String()

	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
	if err != nil {
		return err
	}

	if !hasActive || activeSID != sid {
		return nil
	}

	cleanupCtx, cleanupCancel := activeSnapshotCleanupContext()
	defer cleanupCancel()

	return cleanActiveSnapshotAttempt(cleanupCtx, conn, mountPath, sid)
}

func activeSnapshotCleanupContext() (context.Context, context.CancelFunc) {
	return queryContext(context.Background(), activeSnapshotCleanupTimeout)
}
