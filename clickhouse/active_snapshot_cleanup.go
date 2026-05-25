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

	previousSnapshotMountRowQuery = "SELECT toString(active_snapshot), updated_at " +
		"FROM wrstat_mounts WHERE mount_path = ? AND active_snapshot != toUUID(?) " +
		"AND active = 1 ORDER BY switched_at DESC LIMIT 1"
	rollbackActiveSnapshotMountRowQuery = "INSERT INTO wrstat_mounts " +
		"(mount_path, switched_at, active_snapshot, updated_at, active) " +
		"SELECT ?, next_switched_at, toUUID(?), ?, 1 FROM (" +
		"SELECT greatest(max(switched_at) + toIntervalMillisecond(1), now64(3)) AS next_switched_at " +
		"FROM wrstat_mounts WHERE mount_path = ?) WHERE EXISTS (" +
		"SELECT 1 FROM wrstat_mounts_active_v2 WHERE mount_path = ? AND snapshot_id = toUUID(?))"
	insertInactiveSnapshotMountRowQuery = "INSERT INTO wrstat_mounts " +
		"(mount_path, switched_at, active_snapshot, updated_at, active) " +
		"SELECT ?, next_switched_at, toUUID(?), latest_updated_at, 0 FROM (" +
		"SELECT greatest(max(switched_at) + toIntervalMillisecond(1), now64(3)) AS next_switched_at, " +
		"argMax(updated_at, switched_at) AS latest_updated_at FROM wrstat_mounts WHERE mount_path = ?) " +
		"WHERE EXISTS (" +
		"SELECT 1 FROM wrstat_mounts_active_v2 WHERE mount_path = ? AND snapshot_id = toUUID(?))"
)

var (
	errActiveSnapshotStillActive = errors.New("clickhouse: failed to clean active snapshot mount row")
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

	if err := cleanActiveSnapshotMountRow(ctx, conn, mountPath, sid, previous, hasPrevious); err != nil {
		return err
	}

	return dropSnapshotPartitionsForMount(ctx, conn, mountPath, sid, allPartitionDropQueries())
}

func cleanActiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	previous activeSnapshotMountRow,
	hasPrevious bool,
) error {
	if !hasPrevious {
		return insertInactiveSnapshotMountRowAndEnsure(ctx, conn, mountPath, sid)
	}

	if err := rollbackActiveSnapshotMountRow(ctx, conn, mountPath, sid, previous); err != nil {
		return err
	}

	inactive, err := snapshotInactive(ctx, conn, mountPath, sid)
	if err != nil || inactive {
		return err
	}

	return insertInactiveSnapshotMountRowAndEnsure(ctx, conn, mountPath, sid)
}

func insertInactiveSnapshotMountRowAndEnsure(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
) error {
	if err := insertInactiveSnapshotMountRow(ctx, conn, mountPath, sid); err != nil {
		return err
	}

	return ensureSnapshotInactive(ctx, conn, mountPath, sid)
}

func insertInactiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
) error {
	if err := conn.Exec(ctx, insertInactiveSnapshotMountRowQuery, mountPath, sid, mountPath, mountPath, sid); err != nil {
		return fmt.Errorf("clickhouse: failed to mark active snapshot mount row inactive: %w", err)
	}

	return nil
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

func snapshotInactive(ctx context.Context, conn ch.Conn, mountPath string, sid string) (bool, error) {
	err := ensureSnapshotInactive(ctx, conn, mountPath, sid)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, errActiveSnapshotStillActive) {
		return false, nil
	}

	return false, err
}

func rollbackActiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	failedSID string,
	previous activeSnapshotMountRow,
) error {
	if err := conn.Exec(
		ctx,
		rollbackActiveSnapshotMountRowQuery,
		mountPath,
		previous.snapshotID,
		previous.updatedAt,
		mountPath,
		mountPath,
		failedSID,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to roll back active snapshot mount row: %w", err)
	}

	return nil
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
