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
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	virtualChildrenReadyQuery = "SELECT 1 FROM wrstat_virtual_children_sets " +
		"WHERE active_set_id = ? LIMIT 1"
	virtualChildrenQuery = "SELECT child FROM wrstat_virtual_children " +
		"WHERE active_set_id = ? AND parent_dir = ? ORDER BY child"
	insertVirtualChildQuery = "INSERT INTO wrstat_virtual_children " +
		"(active_set_id, parent_dir, child, child_is_mount_root, mount_path, refreshed_at) " +
		"VALUES (?, ?, ?, ?, ?, ?)"
	insertVirtualChildrenSetQuery = "INSERT INTO wrstat_virtual_children_sets " +
		"(active_set_id, active_mount_count, refreshed_at) VALUES (?, ?, ?)"
	virtualChildrenSetIDsQuery = "SELECT DISTINCT active_set_id FROM wrstat_virtual_children_sets " +
		"WHERE active_set_id != ?"
	dropVirtualChildrenPartitionQuery     = "ALTER TABLE wrstat_virtual_children DROP PARTITION ?"
	dropVirtualChildrenSetPartitionQuery  = "ALTER TABLE wrstat_virtual_children_sets DROP PARTITION ?"
	dropVirtualSummaryCachePartitionQuery = "ALTER TABLE wrstat_virtual_summary_cache DROP PARTITION ?"
	dropVirtualSummarySetPartitionQuery   = "ALTER TABLE wrstat_virtual_summary_sets DROP PARTITION ?"
)

type virtualChildRow struct {
	parentDir        string
	child            string
	childIsMountRoot bool
	mountPath        string
}

func mergeVirtualChildRows(a, b virtualChildRow) virtualChildRow {
	if !b.childIsMountRoot {
		return a
	}

	a.childIsMountRoot = true
	a.mountPath = b.mountPath

	return a
}

func virtualChildRowsForMounts(mounts []activeMount) []virtualChildRow {
	byKey := make(map[string]virtualChildRow)

	for _, mount := range mounts {
		for _, row := range virtualChildRowsForMount(mount.mountPath) {
			key := row.parentDir + "\x00" + row.child
			if existing, ok := byKey[key]; ok {
				byKey[key] = mergeVirtualChildRows(existing, row)

				continue
			}

			byKey[key] = row
		}
	}

	rows := make([]virtualChildRow, 0, len(byKey))
	for _, row := range byKey {
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].parentDir == rows[j].parentDir {
			return rows[i].child < rows[j].child
		}

		return rows[i].parentDir < rows[j].parentDir
	})

	return rows
}

func virtualChildRowsForMount(mountPath string) []virtualChildRow {
	parent := "/"
	mountPath = ensureTrailingSlash(mountPath)
	rows := make([]virtualChildRow, 0, strings.Count(mountPath, "/"))

	for {
		child, ok := immediateChildForMount(parent, mountPath)
		if !ok {
			return rows
		}

		childIsMountRoot := ensureTrailingSlash(child) == mountPath

		row := virtualChildRow{
			parentDir:        parent,
			child:            child,
			childIsMountRoot: childIsMountRoot,
		}
		if childIsMountRoot {
			row.mountPath = mountPath
		}

		rows = append(rows, row)
		if childIsMountRoot {
			return rows
		}

		parent = ensureTrailingSlash(child)
	}
}

func appendVirtualChildRows(
	batch driver.Batch,
	activeSetID string,
	rows []virtualChildRow,
	refreshedAt time.Time,
) error {
	for _, row := range rows {
		err := batch.Append(
			activeSetID,
			row.parentDir,
			row.child,
			boolToUInt8(row.childIsMountRoot),
			row.mountPath,
			refreshedAt,
		)
		if err != nil {
			return fmt.Errorf("clickhouse: failed to append virtual child: %w", err)
		}
	}

	return nil
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}

	return 0
}

func refreshActiveVirtualChildrenForActiveSet(ctx context.Context, conn ch.Conn, activeSetID string) error {
	rows, err := queryMountsActiveRows(ctx, conn)
	if err != nil || activeSetID == "" {
		return err
	}

	if fingerprintForMountsActive(rows) != activeSetID {
		return nil
	}

	return refreshActiveVirtualChildren(ctx, conn, rows)
}

func refreshActiveVirtualChildren(ctx context.Context, conn ch.Conn, rows []mountsActiveRow) error {
	activeSetID := fingerprintForMountsActive(rows)
	if activeSetID == "" {
		return nil
	}

	ready, err := virtualChildrenReady(ctx, conn, activeSetID)
	if err != nil || ready {
		return err
	}

	mounts := newActiveMountsSnapshot(rows).all()

	return insertVirtualChildrenSet(ctx, conn, activeSetID, mounts)
}

func virtualChildrenReady(ctx context.Context, conn ch.Conn, activeSetID string) (bool, error) {
	rows, err := conn.Query(ctx, virtualChildrenReadyQuery, activeSetID)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query virtual children readiness: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: virtual children readiness iteration error: %w", err)
	}

	return false, nil
}

func insertVirtualChildrenSet(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	mounts []activeMount,
) error {
	refreshedAt := time.Now().UTC()

	if err := insertVirtualChildRows(ctx, conn, activeSetID, mounts, refreshedAt); err != nil {
		return err
	}

	if err := conn.Exec(ctx, insertVirtualChildrenSetQuery, activeSetID, uint64(len(mounts)), refreshedAt); err != nil {
		return fmt.Errorf("clickhouse: failed to insert virtual children set: %w", err)
	}

	return nil
}

func insertVirtualChildRows(
	ctx context.Context,
	conn ch.Conn,
	activeSetID string,
	mounts []activeMount,
	refreshedAt time.Time,
) error {
	rows := virtualChildRowsForMounts(mounts)
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, insertVirtualChildQuery)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to prepare virtual children batch: %w", err)
	}

	if err := appendVirtualChildRows(batch, activeSetID, rows, refreshedAt); err != nil {
		return err
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: failed to insert virtual children: %w", err)
	}

	return nil
}

func cleanupOldVirtualChildrenSets(ctx context.Context, conn ch.Conn, keepActiveSetID string) error {
	oldSetIDs, err := virtualChildrenSetIDsExcept(ctx, conn, keepActiveSetID)
	if err != nil {
		return err
	}

	for _, activeSetID := range oldSetIDs {
		if err := dropVirtualActiveSetPartitions(ctx, conn, activeSetID); err != nil {
			return err
		}
	}

	return nil
}

func virtualChildrenSetIDsExcept(ctx context.Context, conn ch.Conn, keepActiveSetID string) ([]string, error) {
	rows, err := conn.Query(ctx, virtualChildrenSetIDsQuery, keepActiveSetID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query old virtual children sets: %w", err)
	}

	defer func() { _ = rows.Close() }()

	setIDs := make([]string, 0)

	for rows.Next() {
		var activeSetID string
		if err := rows.Scan(&activeSetID); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan virtual children set: %w", err)
		}

		setIDs = append(setIDs, activeSetID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: virtual children set iteration error: %w", err)
	}

	return setIDs, nil
}

func dropVirtualActiveSetPartitions(ctx context.Context, conn ch.Conn, activeSetID string) error {
	for _, query := range []string{
		dropVirtualChildrenPartitionQuery,
		dropVirtualChildrenSetPartitionQuery,
		dropVirtualSummaryCachePartitionQuery,
		dropVirtualSummarySetPartitionQuery,
	} {
		if err := dropActiveSetPartition(ctx, conn, query, activeSetID, "virtual"); err != nil {
			return err
		}
	}

	return nil
}

func dropActiveSetPartition(
	ctx context.Context,
	conn ch.Conn,
	query string,
	activeSetID string,
	label string,
) error {
	dropCtx, cancel := partitionDropContext(ctx)
	defer cancel()

	err := conn.Exec(dropCtx, query, activeSetID)
	if err == nil || isUnknownPartition(err) || isUnknownTable(err) {
		return nil
	}

	return fmt.Errorf("clickhouse: failed to drop %s active-set partition: %w", label, err)
}

func isUnknownTable(err error) bool {
	msg := err.Error()

	return strings.Contains(msg, "UNKNOWN_TABLE") ||
		strings.Contains(msg, "Unknown table expression identifier") ||
		strings.Contains(msg, "Could not find table") ||
		strings.Contains(msg, "doesn't exist") ||
		strings.Contains(msg, "does not exist")
}

func virtualChildrenForMounts(parentDir string, children []string, mounts []activeMount) []string {
	if len(children) == 0 || len(mounts) == 0 {
		return nil
	}

	allowed := make(map[string]bool, len(mounts))
	for _, child := range immediateChildrenForMounts(parentDir, mounts) {
		allowed[child] = true
	}

	filtered := make([]string, 0, len(children))
	for _, child := range children {
		if allowed[child] {
			filtered = append(filtered, child)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return filtered
}

func (d *clickHouseDatabase) virtualChildrenForAncestor(
	ctx context.Context,
	parentDir string,
	mounts []activeMount,
) ([]string, bool, error) {
	if d.conn == nil || len(mounts) == 0 {
		return nil, false, nil
	}

	activeSetID, activeMounts, err := d.currentVirtualChildrenActiveSet(ctx)
	if err != nil {
		return nil, false, err
	}

	if activeSetID == "" {
		return nil, false, nil
	}

	readyErr := d.ensureVirtualChildrenReady(ctx, activeSetID, activeMounts)
	if readyErr != nil {
		return nil, false, readyErr
	}

	children, err := d.queryChildren(ctx, virtualChildrenQuery, "virtual children", activeSetID, parentDir)
	if err != nil {
		return nil, false, err
	}

	return virtualChildrenForMounts(parentDir, children, mounts), true, nil
}

func (d *clickHouseDatabase) currentVirtualChildrenActiveSet(
	ctx context.Context,
) (string, []activeMount, error) {
	return d.currentActiveMountsSet(ctx)
}

func (d *clickHouseDatabase) ensureVirtualChildrenReady(
	ctx context.Context,
	activeSetID string,
	mounts []activeMount,
) error {
	ready, err := virtualChildrenReady(ctx, d.conn, activeSetID)
	if err != nil || ready {
		return err
	}

	return insertVirtualChildrenSet(ctx, d.conn, activeSetID, mounts)
}
