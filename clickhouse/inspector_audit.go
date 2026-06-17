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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const importReadinessPublishAuditQuery = `
SELECT surface, rows FROM (
	SELECT 'wrstat_schema3_snapshot_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_schema3_snapshot_sets
	UNION ALL
	SELECT 'wrstat_active_virtual_sets_ready' AS surface, toUInt64(countIf(ready = 1)) AS rows
	FROM wrstat_active_virtual_sets
	UNION ALL
	SELECT 'wrstat_active_prefix_rollup_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_rollup_sets
	UNION ALL
	SELECT 'wrstat_mount_events_publish' AS surface, toUInt64(countIf(event_type = 1)) AS rows
	FROM wrstat_mount_events
) ORDER BY surface`

const activeSnapshotCleanupAuditQuery = `
SELECT surface, rows FROM (
	SELECT 'wrstat_mount_events' AS surface, toUInt64(count()) AS rows FROM wrstat_mount_events
	UNION ALL
	SELECT 'wrstat_dirs' AS surface, toUInt64(count()) AS rows FROM wrstat_dirs
	UNION ALL
	SELECT 'wrstat_files' AS surface, toUInt64(count()) AS rows FROM wrstat_files
	UNION ALL
	SELECT 'wrstat_dir_facts' AS surface, toUInt64(count()) AS rows FROM wrstat_dir_facts
	UNION ALL
	SELECT 'wrstat_dir_projection_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_dir_projection_sets
	UNION ALL
	SELECT 'wrstat_dir_filter_ageall' AS surface, toUInt64(count()) AS rows
	FROM wrstat_dir_filter_ageall
	UNION ALL
	SELECT 'wrstat_child_filter_all' AS surface, toUInt64(count()) AS rows
	FROM wrstat_child_filter_all
	UNION ALL
	SELECT 'wrstat_dir_filter_all' AS surface, toUInt64(count()) AS rows
	FROM wrstat_dir_filter_all
	UNION ALL
	SELECT 'wrstat_schema3_snapshot_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_schema3_snapshot_sets
	UNION ALL
	SELECT 'wrstat_basedirs_group_usage' AS surface, toUInt64(count()) AS rows
	FROM wrstat_basedirs_group_usage
	UNION ALL
	SELECT 'wrstat_basedirs_user_usage' AS surface, toUInt64(count()) AS rows
	FROM wrstat_basedirs_user_usage
	UNION ALL
	SELECT 'wrstat_basedirs_group_subdirs' AS surface, toUInt64(count()) AS rows
	FROM wrstat_basedirs_group_subdirs
	UNION ALL
	SELECT 'wrstat_basedirs_user_subdirs' AS surface, toUInt64(count()) AS rows
	FROM wrstat_basedirs_user_subdirs
	UNION ALL
	SELECT 'wrstat_active_virtual_dirs' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_virtual_dirs
	UNION ALL
	SELECT 'wrstat_active_virtual_summaries' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_virtual_summaries
	UNION ALL
	SELECT 'wrstat_active_virtual_filter_all' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_virtual_filter_all
	UNION ALL
	SELECT 'wrstat_active_virtual_children' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_virtual_children
	UNION ALL
	SELECT 'wrstat_active_virtual_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_virtual_sets
	UNION ALL
	SELECT 'wrstat_active_prefix_rollups' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_rollups
	UNION ALL
	SELECT 'wrstat_active_prefix_filter_ageall' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_filter_ageall
	UNION ALL
	SELECT 'wrstat_active_prefix_rollup_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_rollup_sets
) ORDER BY surface`

const activePrefixRollupAuditQuery = `
SELECT surface, rows FROM (
	SELECT 'wrstat_active_prefix_rollup_sets' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_rollup_sets
	UNION ALL
	SELECT 'wrstat_active_prefix_rollups' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_rollups
	UNION ALL
	SELECT 'wrstat_active_prefix_filter_ageall' AS surface, toUInt64(count()) AS rows
	FROM wrstat_active_prefix_filter_ageall
	UNION ALL
	SELECT 'wrstat_active_prefix_scalar_root_read' AS surface, toUInt64(count()) AS rows
	FROM (
		SELECT r.active_set_id
		FROM wrstat_active_prefix_rollups AS r
		INNER JOIN wrstat_active_virtual_dirs AS d
		ON d.active_set_id = r.active_set_id AND d.virtual_id = r.virtual_id
		WHERE r.active_set_id IN (
			SELECT active_set_id FROM wrstat_active_prefix_rollup_sets
			ORDER BY refreshed_at DESC LIMIT 1
		)
		AND d.full_path = '/'
		LIMIT 1
	)
) ORDER BY surface`

// InspectorAuditRow captures a read-only maintenance audit surface and row
// count.
type InspectorAuditRow struct {
	Surface string
	Rows    uint64
}

// ImportReadinessPublishAudit returns read-only row-count evidence for import
// readiness and publish state tables.
func (i *Inspector) ImportReadinessPublishAudit(ctx context.Context) ([]InspectorAuditRow, error) {
	return i.auditRows(ctx, importReadinessPublishAuditQuery, "import readiness/publish")
}

// ActiveSnapshotCleanupAudit returns read-only row-count evidence for active
// snapshot cleanup state tables.
func (i *Inspector) ActiveSnapshotCleanupAudit(ctx context.Context) ([]InspectorAuditRow, error) {
	return i.auditRows(ctx, activeSnapshotCleanupAuditQuery, "active snapshot cleanup")
}

// ActivePrefixRollupAudit returns read-only row-count evidence for active-prefix
// rollup state and a scalar root rollup read.
func (i *Inspector) ActivePrefixRollupAudit(ctx context.Context) ([]InspectorAuditRow, error) {
	return i.auditRows(ctx, activePrefixRollupAuditQuery, "active-prefix rollup")
}

func (i *Inspector) auditRows(
	ctx context.Context,
	query string,
	label string,
) ([]InspectorAuditRow, error) {
	qctx, cancel := i.queryContext(ctx)
	defer cancel()

	rows, err := i.conn.Query(qctx, query)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s audit: %w", label, err)
	}

	defer func() { _ = rows.Close() }()

	return scanInspectorAuditRows(rows, label)
}

func scanInspectorAuditRows(rows driver.Rows, label string) ([]InspectorAuditRow, error) {
	var out []InspectorAuditRow

	for rows.Next() {
		var row InspectorAuditRow
		if err := rows.Scan(&row.Surface, &row.Rows); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan %s audit row: %w", label, err)
		}

		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: %s audit row iteration error: %w", label, err)
	}

	return out, nil
}
