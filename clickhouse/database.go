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

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	childrenInitialCap = 16

	childrenQuery = "SELECT DISTINCT child FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND parent_dir = ? " +
		"ORDER BY child"

	resolveMountQuery = "SELECT mount_path, snapshot_id FROM wrstat_mounts_active " +
		"WHERE startsWith(?, mount_path) " +
		"ORDER BY length(mount_path) DESC LIMIT 1"
)

type clickHouseDatabase struct {
	cfg  Config
	conn ch.Conn
}

func newClickHouseDatabase(cfg Config, conn ch.Conn) *clickHouseDatabase {
	return &clickHouseDatabase{cfg: cfg, conn: conn}
}

func (d *clickHouseDatabase) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	_ = dir
	_ = filter

	return nil, db.ErrDirNotFound
}

func (d *clickHouseDatabase) Children(dir string) ([]string, error) {
	mountPath, snapshotID, ok, err := d.resolveMountAndSnapshot(dir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	parentDir := ensureTrailingSlash(dir)

	return d.childrenForMount(mountPath, snapshotID, parentDir)
}

func ensureTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}

	return dir + "/"
}

func (d *clickHouseDatabase) childrenForMount(mountPath, snapshotID, parentDir string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(d.cfg))
	defer cancel()

	rows, err := d.conn.Query(
		ctx,
		childrenQuery,
		mountPath,
		snapshotID,
		parentDir,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query children: %w", err)
	}

	defer func() { _ = rows.Close() }()

	children, err := scanChildrenRows(rows)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, nil
	}

	return children, nil
}

func scanChildrenRows(rows rowsScanner) ([]string, error) {
	children := make([]string, 0, childrenInitialCap)

	for rows.Next() {
		var child string
		if err := rows.Scan(&child); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan child: %w", err)
		}

		children = append(children, child)
	}

	return children, nil
}

func (d *clickHouseDatabase) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *clickHouseDatabase) Close() error {
	return nil
}

func (d *clickHouseDatabase) resolveMountAndSnapshot(dir string) (string, string, bool, error) {
	dirForMatch := ensureTrailingSlash(dir)

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(d.cfg))
	defer cancel()

	rows, err := d.conn.Query(
		ctx,
		resolveMountQuery,
		dirForMatch,
	)
	if err != nil {
		return "", "", false, fmt.Errorf("clickhouse: failed to resolve active mount: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return "", "", false, nil
	}

	var (
		mountPath  string
		snapshotID string
	)
	if err := rows.Scan(&mountPath, &snapshotID); err != nil {
		return "", "", false, fmt.Errorf("clickhouse: failed to scan active mount: %w", err)
	}

	return mountPath, snapshotID, true, nil
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
}
