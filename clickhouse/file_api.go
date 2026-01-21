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
	"math"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	errClientClosed = errors.New("clickhouse: client is closed")
	errPathNotFound = errors.New("clickhouse: path not found")
	errInvalidPath  = errors.New("clickhouse: invalid path")
)

// FileRow represents a file or directory from wrstat_files.
type FileRow struct {
	Path         string
	ParentDir    string
	Name         string
	Ext          string
	EntryType    byte
	Size         int64
	ApparentSize int64
	UID          uint32
	GID          uint32
	ATime        time.Time
	MTime        time.Time
	CTime        time.Time
	Inode        int64
	Nlink        int64
}

// ListOptions controls ListDir behaviour.
type ListOptions struct {
	Fields []string
	Limit  int64
	Offset int64
}

// StatOptions controls StatPath behaviour.
type StatOptions struct {
	Fields []string
}

// FindOptions controls FindByGlob behaviour.
type FindOptions struct {
	Fields       []string
	Limit        int64
	Offset       int64
	RequireOwner bool
	UID          uint32
	GIDs         []uint32
}

const (
	fileRowSelectAll = "f.path, f.parent_dir, f.name, f.ext, f.entry_type, f.size, " +
		"f.apparent_size, f.uid, f.gid, f.atime, f.mtime, f.ctime, f.inode, f.nlink"
)

func defaultFileRowFields() []string {
	return []string{
		"path",
		"parent_dir",
		"name",
		"ext",
		"entry_type",
		"size",
		"apparent_size",
		"uid",
		"gid",
		"atime",
		"mtime",
		"ctime",
		"inode",
		"nlink",
	}
}

const statPathQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? AND f.name = ? LIMIT 1"

const listDirQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? ORDER BY f.name ASC LIMIT ? OFFSET ?"

const isDirQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT f.entry_type FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? AND f.name = ? LIMIT 1"

const defaultFileLimit = 1_000_000

// StatPath returns metadata for an exact file path over the active snapshot of
// the mount containing the path.
func (c *Client) StatPath(ctx context.Context, path string, opts StatOptions) (*FileRow, error) {
	if c == nil || c.conn == nil {
		return nil, errClientClosed
	}

	mountPath, parentDir, name, err := c.resolveMountParentName(path)
	if err != nil {
		return nil, err
	}

	q, fields, err := statPathQuery(opts)
	if err != nil {
		return nil, err
	}

	qctx, cancel := context.WithTimeout(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, q, mountPath, mountPath, parentDir, name)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query StatPath: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return firstFileRow(rows, fields)
}

// ListDir lists direct children (by parent_dir) for the given directory.
func (c *Client) ListDir(ctx context.Context, dir string, opts ListOptions) ([]FileRow, error) {
	if c == nil || c.conn == nil {
		return nil, errClientClosed
	}

	mountPath, parentDir, err := c.resolveMountAndDir(dir)
	if err != nil {
		return nil, err
	}

	q, fields, err := listDirQuery(opts)
	if err != nil {
		return nil, err
	}

	limit := listLimit(opts.Limit)

	qctx, cancel := context.WithTimeout(ctx, queryTimeout(c.cfg))
	defer cancel()

	return c.listDirRows(qctx, q, fields, mountPath, parentDir, limit, opts.Offset)
}

func (c *Client) listDirRows(
	ctx context.Context,
	query string,
	fields []string,
	mountPath string,
	parentDir string,
	limit int64,
	offset int64,
) ([]FileRow, error) {
	rows, err := c.conn.Query(ctx, query, mountPath, mountPath, parentDir, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query ListDir: %w", err)
	}

	defer func() { _ = rows.Close() }()

	out := make([]FileRow, 0)

	for rows.Next() {
		var row FileRow
		if err := scanFileRow(rows, fields, &row); err != nil {
			return nil, err
		}

		out = append(out, row)
	}

	return out, nil
}

func (c *Client) resolveMountAndDir(dir string) (string, string, error) {
	mountPath := c.mountPoints.PrefixOf(dir)
	if mountPath == "" {
		return "", "", basedirs.ErrInvalidBasePath
	}

	return mountPath, ensureTrailingSlash(dir), nil
}

func listLimit(limit int64) int64 {
	if limit > 0 {
		return limit
	}

	return defaultFileLimit
}

func listDirQuery(opts ListOptions) (string, []string, error) {
	selectList, fields, err := fileRowSelectList(opts.Fields)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(listDirQueryTemplate, selectList), fields, nil
}

func statPathQuery(opts StatOptions) (string, []string, error) {
	selectList, fields, err := fileRowSelectList(opts.Fields)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(statPathQueryTemplate, selectList), fields, nil
}

// IsDir reports whether the given path exists and is a directory.
func (c *Client) IsDir(ctx context.Context, path string) (bool, error) {
	if c == nil || c.conn == nil {
		return false, errClientClosed
	}

	mountPath, parentDir, name, err := c.resolveMountParentName(path)
	if err != nil {
		return false, err
	}

	qctx, cancel := context.WithTimeout(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, isDirQuery, mountPath, mountPath, parentDir, name)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query IsDir: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return false, nil
	}

	var entryType uint8
	if err := rows.Scan(&entryType); err != nil {
		return false, fmt.Errorf("clickhouse: failed to scan IsDir row: %w", err)
	}

	return entryType == uint8(stats.DirType), nil
}

func (c *Client) resolveMountParentName(path string) (string, string, string, error) {
	mountPath := c.mountPoints.PrefixOf(path)
	if mountPath == "" {
		return "", "", "", basedirs.ErrInvalidBasePath
	}

	parentDir, name, ok := splitPathParentAndName(path)
	if !ok {
		return "", "", "", errInvalidPath
	}

	return mountPath, parentDir, name, nil
}

type UnknownFileFieldError struct {
	Field string
}

func (e UnknownFileFieldError) Error() string {
	return fmt.Sprintf("clickhouse: unknown file field %q", e.Field)
}

func fileRowSelectList(fields []string) (string, []string, error) {
	if len(fields) == 0 {
		all := defaultFileRowFields()

		return fileRowSelectAll, all, nil
	}

	out := make([]string, 0, len(fields))
	for _, f := range fields {
		col, ok := fileRowColumnForField(f)
		if !ok {
			return "", nil, UnknownFileFieldError{Field: f}
		}

		out = append(out, col)
	}

	return strings.Join(out, ", "), fields, nil
}

func fileRowColumnForField(field string) (string, bool) {
	cols := map[string]string{
		"path":          "f.path",
		"parent_dir":    "f.parent_dir",
		"name":          "f.name",
		"ext":           "f.ext",
		"entry_type":    "f.entry_type",
		"size":          "f.size",
		"apparent_size": "f.apparent_size",
		"uid":           "f.uid",
		"gid":           "f.gid",
		"atime":         "f.atime",
		"mtime":         "f.mtime",
		"ctime":         "f.ctime",
		"inode":         "f.inode",
		"nlink":         "f.nlink",
	}

	col, ok := cols[field]

	return col, ok
}

type fileRowScanner interface {
	Scan(dest ...any) error
}

type fileRowIterator interface {
	fileRowScanner
	Next() bool
}

func firstFileRow(rows fileRowIterator, fields []string) (*FileRow, error) {
	if !rows.Next() {
		return nil, errPathNotFound
	}

	row := &FileRow{}
	if err := scanFileRow(rows, fields, row); err != nil {
		return nil, err
	}

	return row, nil
}

func scanFileRow(rows fileRowScanner, fields []string, out *FileRow) error {
	state := &fileRowScanState{}

	dests, err := state.destsFor(fields)
	if err != nil {
		return err
	}

	if err := rows.Scan(dests...); err != nil {
		return fmt.Errorf("clickhouse: failed to scan file row: %w", err)
	}

	return state.applyTo(out)
}

type fileRowScanState struct {
	path      string
	parentDir string
	name      string
	ext       string

	entryType uint8
	uid       uint32
	gid       uint32

	size         uint64
	apparentSize uint64
	inode        uint64
	nlink        uint64

	atime time.Time
	mtime time.Time
	ctime time.Time
}

func (s *fileRowScanState) destsFor(fields []string) ([]any, error) {
	if len(fields) == 0 {
		fields = defaultFileRowFields()
	}

	dests := make([]any, 0, len(fields))
	for _, f := range fields {
		dest, ok := s.destForField(f)
		if !ok {
			return nil, UnknownFileFieldError{Field: f}
		}

		dests = append(dests, dest)
	}

	return dests, nil
}

func (s *fileRowScanState) destForField(field string) (any, bool) {
	dests := map[string]any{
		"path":          &s.path,
		"parent_dir":    &s.parentDir,
		"name":          &s.name,
		"ext":           &s.ext,
		"entry_type":    &s.entryType,
		"size":          &s.size,
		"apparent_size": &s.apparentSize,
		"uid":           &s.uid,
		"gid":           &s.gid,
		"atime":         &s.atime,
		"mtime":         &s.mtime,
		"ctime":         &s.ctime,
		"inode":         &s.inode,
		"nlink":         &s.nlink,
	}

	dest, ok := dests[field]

	return dest, ok
}

func (s *fileRowScanState) applyTo(out *FileRow) error {
	converted, err := s.convertUints()
	if err != nil {
		return err
	}

	s.applyScalars(out, converted)

	return nil
}

type fileRowConverted struct {
	size         int64
	apparentSize int64
	inode        int64
	nlink        int64
}

func (s *fileRowScanState) convertUints() (*fileRowConverted, error) {
	size, err := uint64ToInt64(s.size)
	if err != nil {
		return nil, err
	}

	apparentSize, err := uint64ToInt64(s.apparentSize)
	if err != nil {
		return nil, err
	}

	inode, err := uint64ToInt64(s.inode)
	if err != nil {
		return nil, err
	}

	nlink, err := uint64ToInt64(s.nlink)
	if err != nil {
		return nil, err
	}

	return &fileRowConverted{size: size, apparentSize: apparentSize, inode: inode, nlink: nlink}, nil
}

func (s *fileRowScanState) applyScalars(out *FileRow, converted *fileRowConverted) {
	out.Path = s.path
	out.ParentDir = s.parentDir
	out.Name = s.name
	out.Ext = s.ext
	out.EntryType = s.entryType
	out.Size = converted.size
	out.ApparentSize = converted.apparentSize
	out.UID = s.uid
	out.GID = s.gid
	out.ATime = s.atime
	out.MTime = s.mtime
	out.CTime = s.ctime
	out.Inode = converted.inode
	out.Nlink = converted.nlink
}

func uint64ToInt64(v uint64) (int64, error) {
	if v > uint64(math.MaxInt64) {
		return 0, errInvalidPath
	}

	return int64(v), nil
}

func splitPathParentAndName(path string) (string, string, bool) {
	if path == "" {
		return "", "", false
	}

	idx := lastComponentSlashIndex(path)
	if idx < 0 {
		return "", "", false
	}

	parentDir := path[:idx+1]
	name := path[idx+1:]

	if parentDir == "" || name == "" {
		return "", "", false
	}

	return parentDir, name, true
}

func lastComponentSlashIndex(path string) int {
	if strings.HasSuffix(path, "/") && path != "/" {
		return strings.LastIndex(path[:len(path)-1], "/")
	}

	return strings.LastIndex(path, "/")
}
