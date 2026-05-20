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
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const deleteActiveSnapshotMountRowsQuery = "ALTER TABLE wrstat_mounts DELETE WHERE mount_path = ? " +
	"AND active_snapshot = toUUID(?) SETTINGS mutations_sync = 2"

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

	ctx, cancel := configQueryContext(cfg)
	defer cancel()

	sid := snapshotID(mountPath, updatedAt).String()

	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
	if err != nil || !hasActive || activeSID != sid {
		return err
	}

	if err := deleteActiveSnapshotMountRows(ctx, conn, mountPath, sid); err != nil {
		return err
	}

	return dropAllSnapshotPartitions(ctx, conn, mountPath, sid)
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

func dropAllSnapshotPartitions(
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	sid string,
) error {
	return dropSnapshotPartitionsForMount(ctx, conn, mountPath, sid, allPartitionDropQueries())
}
