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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	activeSnapshotCleanupTimeout  = 30 * time.Minute
	activeSnapshotCleanupRepairs  = 3
	latestMountMetadataRowsLimit  = 5
	latestActiveSnapshotQueryArgs = 3

	previousSnapshotMountRowQuery = "SELECT toString(snapshot_id), updated_at, event_type " +
		"FROM wrstat_mount_events WHERE mount_path = ? " +
		"AND NOT (snapshot_id = toUUID(?) AND event_type = 1) " +
		"AND event_at <= fromUnixTimestamp64Milli(?) " +
		"ORDER BY event_at DESC, if(event_type = 0, 1, 0) DESC, " +
		"updated_at DESC, toString(snapshot_id) DESC LIMIT 1"
	mountMaxSwitchedAtQuery             = "SELECT maxOrNull(event_at) FROM wrstat_mount_events WHERE mount_path = ?"
	latestActiveSnapshotSwitchedAtQuery = "SELECT maxOrNull(event_at) FROM wrstat_mount_events " +
		"WHERE mount_path = ? AND event_type = 1 AND event_at > fromUnixTimestamp64Milli(?)"
	latestUnexpectedActiveSnapshotSwitchedAtQuery = "SELECT maxOrNull(event_at) FROM wrstat_mount_events " +
		"WHERE mount_path = ? AND event_type = 1 AND event_at > fromUnixTimestamp64Milli(?) " +
		"AND NOT (snapshot_id = toUUID(?) AND reason = 'rollback')"
	latestMountMetadataRowsQuery = "SELECT toString(snapshot_id), updated_at, event_at, event_type " +
		"FROM wrstat_mount_events WHERE mount_path = ? ORDER BY event_at DESC, " +
		"if(event_type = 0, 1, 0) DESC, updated_at DESC, toString(snapshot_id) DESC LIMIT 5"
	rollbackActiveSnapshotMountRowQuery = "INSERT INTO wrstat_mount_events " +
		"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, next_event_at, 1, toUUID(?), ?, 'rollback' FROM (" +
		"SELECT greatest(coalesce(max(event_at) + toIntervalMillisecond(1), now64(3)), now64(3)) AS next_event_at " +
		"FROM wrstat_mount_events WHERE mount_path = ?) WHERE EXISTS (" +
		"SELECT 1 FROM wrstat_mounts_active WHERE mount_path = ? AND snapshot_id = toUUID(?))"
	insertInactiveSnapshotMountRowQuery = "INSERT INTO wrstat_mount_events " +
		"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, next_event_at, 0, toUUID(?), latest_updated_at, 'cleanup' FROM (" +
		"SELECT greatest(coalesce(max(event_at) + toIntervalMillisecond(1), now64(3)), now64(3)) AS next_event_at, " +
		"argMax(updated_at, tuple(event_at, if(event_type = 0, 1, 0), updated_at, toString(snapshot_id))) " +
		"AS latest_updated_at FROM wrstat_mount_events WHERE mount_path = ?) " +
		"WHERE EXISTS (" +
		"SELECT 1 FROM wrstat_mounts_active WHERE mount_path = ? AND snapshot_id = toUUID(?))"
	repairInactiveSnapshotMountRowQuery = "INSERT INTO wrstat_mount_events " +
		"(mount_path, event_at, event_type, snapshot_id, updated_at, reason) " +
		"SELECT ?, greatest(latest_event_at + toIntervalMillisecond(1), now64(3)), " +
		"0, toUUID(?), latest_updated_at, 'cleanup_repair' FROM (" +
		"SELECT max(event_at) AS latest_event_at, " +
		"argMax(updated_at, tuple(event_at, if(event_type = 0, 1, 0), updated_at, toString(snapshot_id))) " +
		"AS latest_updated_at FROM wrstat_mount_events WHERE mount_path = ?) " +
		"WHERE latest_event_at <= fromUnixTimestamp64Milli(?)"
)

var (
	errActiveSnapshotStillActive = errors.New("clickhouse: failed to clean active snapshot mount row")
)

type activeSnapshotMountRow struct {
	snapshotID string
	updatedAt  time.Time
	eventType  uint8
}

func readPreviousSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baseline activeSnapshotCleanupBaseline,
) (activeSnapshotMountRow, bool, error) {
	if !baseline.hasRows {
		return activeSnapshotMountRow{}, false, nil
	}

	row, found, err := queryPreviousSnapshotMountRow(ctx, conn, mountPath, sid, baseline.maxSwitchedAt)
	if err != nil || !found || row.eventType != 1 {
		return activeSnapshotMountRow{}, false, err
	}

	return row, true, nil
}

func queryPreviousSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baselineMaxSwitchedAt time.Time,
) (activeSnapshotMountRow, bool, error) {
	rows, err := conn.Query(
		ctx,
		previousSnapshotMountRowQuery,
		mountPath,
		sid,
		snapshotCleanupUnixMillis(baselineMaxSwitchedAt),
	)
	if err != nil {
		return activeSnapshotMountRow{}, false, fmt.Errorf("clickhouse: failed to read previous snapshot mount row: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanOptionalPreviousSnapshotMountRow(rows)
}

func scanOptionalPreviousSnapshotMountRow(rows driverRows) (activeSnapshotMountRow, bool, error) {
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
	if err := rows.Scan(&row.snapshotID, &row.updatedAt, &row.eventType); err != nil {
		return activeSnapshotMountRow{}, fmt.Errorf("clickhouse: failed to scan snapshot mount row: %w", err)
	}

	return row, nil
}

func cleanActiveSnapshotAttempt(ctx context.Context, conn ch.Conn, mountPath string, sid string) error {
	baseline, err := readMountMaxSwitchedAt(ctx, conn, mountPath)
	if err != nil {
		return err
	}

	previous, hasPrevious, err := readPreviousSnapshotMountRow(ctx, conn, mountPath, sid, baseline)
	if err != nil {
		return err
	}

	if err := dropCurrentActiveSnapshotPartitions(ctx, conn, mountPath, sid); err != nil {
		return err
	}

	if err := cleanActiveSnapshotMountRow(ctx, conn, mountPath, sid, previous, hasPrevious, baseline); err != nil {
		return err
	}

	return ensureNoNewerActiveSnapshot(ctx, conn, mountPath, sid, baseline, previous, hasPrevious)
}

func readMountMaxSwitchedAt(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
) (activeSnapshotCleanupBaseline, error) {
	rows, err := conn.Query(ctx, mountMaxSwitchedAtQuery, mountPath)
	if err != nil {
		return activeSnapshotCleanupBaseline{}, fmt.Errorf("clickhouse: failed to read mount max switch time: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := mountMaxSwitchedAtRowsErr(rows.Err()); err != nil {
			return activeSnapshotCleanupBaseline{}, err
		}

		return activeSnapshotCleanupBaseline{}, nil
	}

	var maxSwitchedAt *time.Time
	if err := rows.Scan(&maxSwitchedAt); err != nil {
		return activeSnapshotCleanupBaseline{}, fmt.Errorf("clickhouse: failed to scan mount max switch time: %w", err)
	}

	if maxSwitchedAt == nil {
		return activeSnapshotCleanupBaseline{}, nil
	}

	return activeSnapshotCleanupBaseline{maxSwitchedAt: *maxSwitchedAt, hasRows: true}, nil
}

func mountMaxSwitchedAtRowsErr(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("clickhouse: mount max switch time iteration error: %w", err)
}

func dropCurrentActiveSnapshotPartitions(ctx context.Context, conn ch.Conn, mountPath string, sid string) error {
	if err := dropSnapshotPartitionsForMount(ctx, conn, mountPath, sid, allPartitionDropQueries()); err != nil {
		return fmt.Errorf("clickhouse: current_snapshot_partition_drop: %w", err)
	}

	return nil
}

func ensureNoNewerActiveSnapshot(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baseline activeSnapshotCleanupBaseline,
	allowedRollback activeSnapshotMountRow,
	hasAllowedRollback bool,
) error {
	latestActiveSwitchedAt, hasLatestActive, err := readLatestActiveSnapshotSwitchedAt(
		ctx,
		conn,
		mountPath,
		baseline.maxSwitchedAt,
		allowedRollback,
		hasAllowedRollback,
	)
	if err != nil {
		return err
	}

	if !hasLatestActive {
		return nil
	}

	reason := newerActiveSnapshotReason(latestActiveSwitchedAt, baseline)

	return activeSnapshotStillActiveError(ctx, conn, mountPath, sid, reason)
}

func newerActiveSnapshotReason(
	latestActiveSwitchedAt time.Time,
	baseline activeSnapshotCleanupBaseline,
) string {
	return fmt.Sprintf(
		"latest_active_switched_at=%s newer than cleanup_baseline=%s",
		formatSnapshotCleanupTime(latestActiveSwitchedAt),
		formatSnapshotCleanupTime(baseline.maxSwitchedAt),
	)
}

func readLatestActiveSnapshotSwitchedAt(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	baselineMaxSwitchedAt time.Time,
	allowedRollback activeSnapshotMountRow,
	hasAllowedRollback bool,
) (time.Time, bool, error) {
	query, args := latestActiveSnapshotSwitchedAtArgs(
		mountPath,
		baselineMaxSwitchedAt,
		allowedRollback,
		hasAllowedRollback,
	)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse: failed to read latest active snapshot switch time: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := latestActiveSnapshotSwitchedAtRowsErr(rows.Err()); err != nil {
			return time.Time{}, false, err
		}

		return time.Time{}, false, nil
	}

	return scanLatestActiveSnapshotSwitchedAt(rows)
}

func snapshotCleanupUnixMillis(t time.Time) int64 {
	return t.UTC().UnixMilli()
}

func latestActiveSnapshotSwitchedAtRowsErr(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"clickhouse: latest active snapshot switch time iteration error: %w",
		err,
	)
}

func scanLatestActiveSnapshotSwitchedAt(rows interface {
	Scan(dest ...any) error
}) (time.Time, bool, error) {
	var switchedAt *time.Time
	if err := rows.Scan(&switchedAt); err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse: failed to scan latest active snapshot switch time: %w", err)
	}

	if switchedAt == nil {
		return time.Time{}, false, nil
	}

	return *switchedAt, true, nil
}

func formatSnapshotCleanupTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func activeSnapshotStillActiveError(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	reason string,
) error {
	detail, err := activeSnapshotStillActiveDetail(ctx, conn, mountPath, reason)
	if err != nil {
		detail = strings.TrimSpace(reason + " latest_mount_rows_error=" + err.Error())
	}

	return fmt.Errorf(
		"%w: mount_path=%s snapshot_id=%s is still active; %s",
		errActiveSnapshotStillActive,
		mountPath,
		sid,
		detail,
	)
}

func activeSnapshotStillActiveDetail(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	reason string,
) (string, error) {
	rows, err := readLatestMountMetadataRows(ctx, conn, mountPath)
	if err != nil {
		return "", err
	}

	detail := "latest_mount_rows=" + formatLatestMountMetadataRows(rows)
	if reason != "" {
		return reason + "; " + detail, nil
	}

	return detail, nil
}

func readLatestMountMetadataRows(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
) ([]activeSnapshotMountMetadataRow, error) {
	rows, err := conn.Query(ctx, latestMountMetadataRowsQuery, mountPath)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to read latest mount metadata rows: %w", err)
	}

	defer func() { _ = rows.Close() }()

	metadata := make([]activeSnapshotMountMetadataRow, 0, latestMountMetadataRowsLimit)

	for rows.Next() {
		row, err := scanActiveSnapshotMountMetadataRow(rows)
		if err != nil {
			return nil, err
		}

		metadata = append(metadata, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: latest mount metadata row iteration error: %w", err)
	}

	return metadata, nil
}

func scanActiveSnapshotMountMetadataRow(rows interface {
	Scan(dest ...any) error
}) (activeSnapshotMountMetadataRow, error) {
	var row activeSnapshotMountMetadataRow
	if err := rows.Scan(&row.snapshotID, &row.updatedAt, &row.switchedAt, &row.active); err != nil {
		return activeSnapshotMountMetadataRow{}, fmt.Errorf("clickhouse: failed to scan latest mount metadata row: %w", err)
	}

	return row, nil
}

func formatLatestMountMetadataRows(rows []activeSnapshotMountMetadataRow) string {
	if len(rows) == 0 {
		return "[]"
	}

	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf(
			"{snapshot_id=%s switched_at=%s updated_at=%s active=%d}",
			row.snapshotID,
			formatSnapshotCleanupTime(row.switchedAt),
			formatSnapshotCleanupTime(row.updatedAt),
			row.active,
		))
	}

	return "[" + strings.Join(parts, " ") + "]"
}

func cleanActiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	previous activeSnapshotMountRow,
	hasPrevious bool,
	baseline activeSnapshotCleanupBaseline,
) error {
	if !hasPrevious {
		return insertInactiveSnapshotMountRowAndEnsure(ctx, conn, mountPath, sid, baseline)
	}

	if err := rollbackActiveSnapshotMountRow(ctx, conn, mountPath, sid, previous); err != nil {
		return err
	}

	inactive, err := snapshotInactive(ctx, conn, mountPath, sid)
	if err != nil || inactive {
		return err
	}

	return insertInactiveSnapshotMountRowAndEnsure(ctx, conn, mountPath, sid, baseline)
}

func insertInactiveSnapshotMountRowAndEnsure(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baseline activeSnapshotCleanupBaseline,
) error {
	if err := insertInactiveSnapshotMountRow(ctx, conn, mountPath, sid); err != nil {
		return err
	}

	return ensureSnapshotInactiveWithRepair(ctx, conn, mountPath, sid, baseline)
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

func ensureSnapshotInactiveWithRepair(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baseline activeSnapshotCleanupBaseline,
) error {
	for range activeSnapshotCleanupRepairs {
		inactive, err := snapshotInactive(ctx, conn, mountPath, sid)
		if err != nil {
			return err
		}

		if inactive {
			return nil
		}

		if err := repairInactiveSnapshotMountRow(ctx, conn, mountPath, sid, baseline); err != nil {
			return err
		}
	}

	return activeSnapshotStillActiveError(ctx, conn, mountPath, sid, "")
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

func repairInactiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baseline activeSnapshotCleanupBaseline,
) error {
	if !baseline.hasRows {
		return activeSnapshotStillActiveError(ctx, conn, mountPath, sid, "cleanup_baseline=<none>")
	}

	if err := ensureNoNewerActiveSnapshot(
		ctx,
		conn,
		mountPath,
		sid,
		baseline,
		activeSnapshotMountRow{},
		false,
	); err != nil {
		return err
	}

	return execRepairInactiveSnapshotMountRow(ctx, conn, mountPath, sid, baseline.maxSwitchedAt)
}

func execRepairInactiveSnapshotMountRow(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
	baselineMaxSwitchedAt time.Time,
) error {
	if err := conn.Exec(
		ctx,
		repairInactiveSnapshotMountRowQuery,
		mountPath,
		sid,
		mountPath,
		snapshotCleanupUnixMillis(baselineMaxSwitchedAt),
	); err != nil {
		return fmt.Errorf("clickhouse: failed to repair active snapshot mount row: %w", err)
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

func latestActiveSnapshotSwitchedAtArgs(
	mountPath string,
	baselineMaxSwitchedAt time.Time,
	allowedRollback activeSnapshotMountRow,
	hasAllowedRollback bool,
) (string, []any) {
	args := make([]any, 0, latestActiveSnapshotQueryArgs)

	args = append(args, mountPath, snapshotCleanupUnixMillis(baselineMaxSwitchedAt))
	if !hasAllowedRollback {
		return latestActiveSnapshotSwitchedAtQuery, args
	}

	return latestUnexpectedActiveSnapshotSwitchedAtQuery, append(args, allowedRollback.snapshotID)
}

type driverRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...any) error
}

type activeSnapshotCleanupBaseline struct {
	maxSwitchedAt time.Time
	hasRows       bool
}

type activeSnapshotMountMetadataRow struct {
	snapshotID string
	updatedAt  time.Time
	switchedAt time.Time
	active     uint8
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

	defer invalidateActiveMetadataCache(cfg)

	cleanupCtx, cleanupCancel := activeSnapshotCleanupContext()
	defer cleanupCancel()

	return cleanActiveSnapshotAttempt(cleanupCtx, conn, mountPath, sid)
}

func activeSnapshotCleanupContext() (context.Context, context.CancelFunc) {
	return queryContext(context.Background(), activeSnapshotCleanupTimeout)
}
