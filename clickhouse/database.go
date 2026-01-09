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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	childrenInitialCap = 16

	dgutaInitialCap = 8

	childrenQuery = "SELECT DISTINCT child FROM wrstat_children " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND parent_dir = ? " +
		"ORDER BY child"

	dgutaQuery = "SELECT gid, uid, ft, age, count, size, atime_min, mtime_max, atime_buckets, mtime_buckets " +
		"FROM wrstat_dguta PREWHERE mount_path = ? AND snapshot_id = ? AND dir = ?"

	infoDGUTAQuery = "SELECT " +
		"uniqExact(dir) AS num_dirs, " +
		"count() AS num_dgutas " +
		"FROM wrstat_dguta " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	infoChildrenQuery = "SELECT " +
		"uniqExact(parent_dir) AS num_parents, " +
		"count() AS num_children " +
		"FROM wrstat_children " +
		"WHERE (mount_path, snapshot_id) IN (" +
		"SELECT mount_path, snapshot_id FROM wrstat_mounts_active" +
		")"

	resolveMountQuery = "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active " +
		"WHERE startsWith(?, mount_path) " +
		"ORDER BY length(mount_path) DESC LIMIT 1"
)

var errIntOverflow = errors.New("value overflows int")

type clickHouseDatabase struct {
	cfg  Config
	conn ch.Conn
}

func newClickHouseDatabase(cfg Config, conn ch.Conn) *clickHouseDatabase {
	return &clickHouseDatabase{cfg: cfg, conn: conn}
}

func (d *clickHouseDatabase) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	mountPath, snapshotID, updatedAt, ok, err := d.resolveMountAndSnapshot(dir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &db.DirSummary{}, db.ErrDirNotFound
	}

	gutas, err := d.gutasForDir(mountPath, snapshotID, ensureTrailingSlash(dir))
	if err != nil {
		return nil, err
	}

	if len(gutas) == 0 {
		return &db.DirSummary{Modtime: updatedAt}, db.ErrDirNotFound
	}

	sum := gutas.Summary(filter)
	if sum != nil {
		sum.Modtime = updatedAt
	}

	return sum, nil
}

func (d *clickHouseDatabase) Children(dir string) ([]string, error) {
	mountPath, snapshotID, _, ok, err := d.resolveMountAndSnapshot(dir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	parentDir := ensureTrailingSlash(dir)

	return d.childrenForMount(mountPath, snapshotID, parentDir)
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
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(d.cfg))
	defer cancel()

	numDirs, numDGUTAs, err := d.infoCounts(ctx, infoDGUTAQuery, "dguta")
	if err != nil {
		return nil, err
	}

	numParents, numChildren, err := d.infoCounts(ctx, infoChildrenQuery, "children")
	if err != nil {
		return nil, err
	}

	info, err := makeDBInfo(numDirs, numDGUTAs, numParents, numChildren)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func makeDBInfo(numDirs, numDGUTAs, numParents, numChildren uint64) (*db.Info, error) {
	dirs, err := safeUint64ToInt(numDirs)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid num_dirs: %w", err)
	}

	dgutas, err := safeUint64ToInt(numDGUTAs)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid num_dgutas: %w", err)
	}

	parents, err := safeUint64ToInt(numParents)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid num_parents: %w", err)
	}

	children, err := safeUint64ToInt(numChildren)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid num_children: %w", err)
	}

	return &db.Info{
		NumDirs:     dirs,
		NumDGUTAs:   dgutas,
		NumParents:  parents,
		NumChildren: children,
	}, nil
}

func (d *clickHouseDatabase) infoCounts(ctx context.Context, query, desc string) (uint64, uint64, error) {
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("clickhouse: failed to query %s counts: %w", desc, err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return 0, 0, nil
	}

	var a, b uint64
	if err := rows.Scan(&a, &b); err != nil {
		return 0, 0, fmt.Errorf("clickhouse: failed to scan %s counts: %w", desc, err)
	}

	return a, b, nil
}

func (d *clickHouseDatabase) Close() error {
	return nil
}

func (d *clickHouseDatabase) resolveMountAndSnapshot(dir string) (string, string, time.Time, bool, error) {
	dirForMatch := ensureTrailingSlash(dir)

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(d.cfg))
	defer cancel()

	rows, err := d.conn.Query(ctx, resolveMountQuery, dirForMatch)
	if err != nil {
		return "", "", time.Time{}, false, fmt.Errorf("clickhouse: failed to resolve active mount: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return "", "", time.Time{}, false, nil
	}

	mountPath, snapshotID, updatedAt, err := scanActiveMountRow(rows)
	if err != nil {
		return "", "", time.Time{}, false, err
	}

	return mountPath, snapshotID, updatedAt.UTC(), true, nil
}

func ensureTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}

	return dir + "/"
}

func scanActiveMountRow(rows rowsScanner) (string, string, time.Time, error) {
	var (
		mountPath, snapshotID string
		updatedAt             time.Time
	)

	if err := rows.Scan(&mountPath, &snapshotID, &updatedAt); err != nil {
		return "", "", time.Time{}, fmt.Errorf("clickhouse: failed to scan active mount: %w", err)
	}

	return mountPath, snapshotID, updatedAt, nil
}

func (d *clickHouseDatabase) gutasForDir(mountPath, snapshotID, dir string) (db.GUTAs, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(d.cfg))
	defer cancel()

	rows, err := d.conn.Query(ctx, dgutaQuery, mountPath, snapshotID, dir)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query dguta: %w", err)
	}

	defer func() { _ = rows.Close() }()

	gutas := make(db.GUTAs, 0, dgutaInitialCap)

	for rows.Next() {
		g, err := scanDGUTARow(rows)
		if err != nil {
			return nil, err
		}

		gutas = append(gutas, g)
	}

	return gutas, nil
}

func scanDGUTARow(rows rowsScanner) (*db.GUTA, error) {
	var s dgutaScanned
	if err := s.scanFrom(rows); err != nil {
		return nil, err
	}

	g := &db.GUTA{
		GID:   s.gid,
		UID:   s.uid,
		FT:    db.DirGUTAFileType(s.ft),
		Age:   db.DirGUTAge(s.age),
		Count: s.count,
		Size:  s.size,
		Atime: s.atimeMin,
		Mtime: s.mtimeMax,
	}

	g.ATimeRanges = sliceToAgeBuckets(s.atimeBuckets)
	g.MTimeRanges = sliceToAgeBuckets(s.mtimeBuckets)

	return g, nil
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
}

type dgutaScanned struct {
	gid          uint32
	uid          uint32
	ft           uint16
	age          uint8
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
}

func (s *dgutaScanned) scanFrom(rows rowsScanner) error {
	if err := rows.Scan(
		&s.gid,
		&s.uid,
		&s.ft,
		&s.age,
		&s.count,
		&s.size,
		&s.atimeMin,
		&s.mtimeMax,
		&s.atimeBuckets,
		&s.mtimeBuckets,
	); err != nil {
		return fmt.Errorf("clickhouse: failed to scan dguta: %w", err)
	}

	return nil
}

func safeUint64ToInt(v uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if v > maxInt {
		return 0, fmt.Errorf("%w: %d", errIntOverflow, v)
	}

	return int(v), nil
}

func sliceToAgeBuckets(in []uint64) summary.AgeBuckets {
	var out summary.AgeBuckets

	for i := 0; i < len(out) && i < len(in); i++ {
		out[i] = in[i]
	}

	return out
}
