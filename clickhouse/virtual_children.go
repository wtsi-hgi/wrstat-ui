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
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

//nolint:unused
const activeVirtualSetIDsExceptQuery = "SELECT DISTINCT active_set_id FROM wrstat_active_virtual_sets " +
	"WHERE active_set_id != ?"

func virtualIDForDir(dir string) uint32 {
	return uint32(catalogPathHash(ensureTrailingSlash(dir))) //nolint:gosec // Virtual IDs intentionally use low hash bits.
}

//nolint:unused
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

//nolint:funlen,gocyclo,unused
func refreshActiveVirtualChildren(ctx context.Context, conn ch.Conn, rows []mountsActiveRow) error {
	activeSetID := fingerprintForMountsActive(rows)
	if activeSetID == "" {
		return nil
	}

	db := &clickHouseDatabase{conn: conn}

	ready, err := db.activeVirtualSetReady(ctx, activeSetID)
	if err != nil || ready {
		return err
	}

	mounts := newActiveMountsSnapshot(rows).all()
	refreshedAt := time.Now().UTC()
	writer := newActiveVirtualOverlayWriter(conn, defaultBatchSize)

	rootGUTAs, err := queryActiveVirtualRootGUTAs(ctx, conn, mounts)
	if err != nil {
		return err
	}

	rootFilterRows, err := queryActiveVirtualRootFilterRows(ctx, conn, mounts)
	if err != nil {
		return err
	}

	mountRootLinks, err := queryActiveVirtualMountRootLinks(ctx, conn, mounts)
	if err != nil {
		return err
	}

	namespace, summaryRows, filterRows, childRows := activeVirtualRowsForMountsFromDataWithLinks(
		activeSetID,
		mounts,
		refreshedAt,
		rootGUTAs,
		rootFilterRows,
		mountRootLinks,
	)
	if err := appendActiveVirtualOverlayRows(ctx, writer, namespace.rows, summaryRows, filterRows, childRows); err != nil {
		return err
	}

	if err := writer.flush(ctx); err != nil {
		return err
	}

	setRow := activeVirtualSetRowForRows(activeSetID, rows, summaryRows, filterRows, childRows, refreshedAt)

	return writer.appendSet(ctx, setRow)
}

//nolint:unused
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

//nolint:unused
func virtualChildrenSetIDsExcept(ctx context.Context, conn ch.Conn, keepActiveSetID string) ([]string, error) {
	rows, err := conn.Query(ctx, activeVirtualSetIDsExceptQuery, keepActiveSetID)
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

//nolint:unused
func dropVirtualActiveSetPartitions(ctx context.Context, conn ch.Conn, activeSetID string) error {
	for _, query := range activeVirtualPartitionDropQueries() {
		if err := dropActiveSetPartition(ctx, conn, query, activeSetID, "active-virtual"); err != nil {
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

//nolint:cyclop,funlen,gocognit,gocyclo
func (d *clickHouseDatabase) virtualChildrenForAncestor(
	ctx context.Context,
	parentDir string,
	mounts []activeMount,
) ([]string, bool, error) {
	if d.conn == nil || len(mounts) == 0 {
		return nil, false, nil
	}

	activeSetID, _, err := d.currentVirtualChildrenActiveSet(ctx)
	if err != nil {
		return nil, false, err
	}

	if activeSetID == "" {
		return nil, false, nil
	}

	ready, err := d.activeVirtualSetReadyCached(ctx, activeSetID)
	if err != nil || !ready {
		return nil, false, err
	}

	rows, err := d.conn.Query(ctx, activeVirtualCatalogChildrenQuery, activeSetID, ensureTrailingSlash(parentDir))
	if err != nil {
		if isUnknownTable(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("clickhouse: failed to query virtual catalog children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	children, err := scanChildrenRows(rows)
	if err != nil {
		return nil, false, err
	}

	if len(children) > 0 {
		return children, true, nil
	}

	handled, err := d.activeVirtualExistingDirs(ctx, activeSetID, []string{parentDir})
	if err != nil {
		return nil, false, err
	}

	return nil, handled[ensureTrailingSlash(parentDir)], nil
}

func (d *clickHouseDatabase) currentVirtualChildrenActiveSet(
	ctx context.Context,
) (string, []activeMount, error) {
	return d.currentActiveMountsSet(ctx)
}
