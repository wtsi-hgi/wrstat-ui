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
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const (
	findByGlobAlphaMount          = "/alpha/"
	findByGlobBetaMount           = "/beta/"
	findByGlobAlphaOne            = "/alpha/one"
	findByGlobAlphaOneDir         = "/alpha/one/"
	findByGlobAlphaOneDirNext     = "/alpha/one0"
	findByGlobAlphaTwo            = "/alpha/two"
	findByGlobBetaTwo             = "/beta/two"
	findByGlobRecursiveBamPattern = "**/*.bam"
	findByGlobBamExt              = "bam"
	findByGlobDotBam              = ".bam"
	findByGlobRecursiveBamRegex   = "^/alpha/one/(?:[^/]+/)*[^/]*\\.bam$"
	c2ListBName                   = "b.txt"
	c2MountRootName               = "teamX/"
	c2OwnedFileName               = "owned.txt"
)

func TestUnknownFileFieldErrors(t *testing.T) {
	Convey("file row helpers preserve unknown field error behaviour", t, func() {
		_, _, err := fileRowSelectList([]string{"bogus"})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `clickhouse: unknown file field "bogus"`)

		var selectErr unknownFileFieldError
		So(errors.As(err, &selectErr), ShouldBeTrue)
		So(selectErr.Field, ShouldEqual, "bogus")

		_, err = (&fileRowScanState{}).destsFor([]string{"bogus"})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `clickhouse: unknown file field "bogus"`)

		var scanErr unknownFileFieldError
		So(errors.As(err, &scanErr), ShouldBeTrue)
		So(scanErr.Field, ShouldEqual, "bogus")
	})
}

func TestC2FilesSchemaHasNoDirectoryPathColumn(t *testing.T) {
	Convey("C2 wrstat_files stores dir_id and basename only", t, func() {
		src, err := os.ReadFile("schema/011_files.sql")
		So(err, ShouldBeNil)

		ddl := strings.Join(strings.Fields(string(src)), " ")
		So(ddl, ShouldContainSubstring, "dir_id UInt32")
		So(ddl, ShouldContainSubstring, "name String")
		So(ddl, ShouldNotContainSubstring, "parent_dir")
		So(ddl, ShouldNotContainSubstring, "full_path")
		So(ddl, ShouldNotContainSubstring, " dir String")
		So(ddl, ShouldContainSubstring, "ORDER BY (mount_path, snapshot_id, dir_id, name)")
	})
}

func TestC2FindByGlobUsesDirIDRanges(t *testing.T) {
	Convey("C2 recursive file enumeration uses catalog dir_id subtree ranges", t, func() {
		base := catalogDirRef{
			dirID:      42,
			subtreeEnd: 48,
			fullPath:   findByGlobAlphaOneDir,
		}

		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{base},
			[]string{findByGlobRecursiveBamPattern},
			1,
			10,
			[]uint32{20},
			100,
			0,
		)

		So(q, ShouldContainSubstring, "f.dir_id >= ? AND f.dir_id < ?")
		So(q, ShouldContainSubstring, "match(concat(d.full_path, f.name), ?)")
		So(q, ShouldNotContainSubstring, "d.full_path >= ? AND d.full_path < ?")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			uint32(48),
			findByGlobBamExt,
			findByGlobDotBam,
			findByGlobRecursiveBamRegex,
			int64(1),
			uint32(10),
			[]uint32{20},
			int64(100),
			int64(0),
		})
	})
}

func c2Arg[T any](args []any, idx int) T {
	var zero T
	if idx >= len(args) {
		return zero
	}

	v, ok := args[idx].(T)
	if !ok {
		return zero
	}

	return v
}

type c2CatalogRow struct {
	dirID      uint32
	subtreeEnd uint32
	fullPath   string
}

func TestC2PathHashResolutionVerifiesFullPath(t *testing.T) {
	Convey("C2 catalog resolver rejects path-hash hits with the wrong full_path", t, func() {
		conn := &c2CatalogResolveConn{
			row: c2CatalogRow{
				dirID:      44,
				subtreeEnd: 45,
				fullPath:   "/alpha/collision/",
			},
		}
		client := &Client{
			cfg:  Config{QueryTimeout: time.Second},
			conn: conn,
		}

		ref, err := client.resolveCatalogDir(context.Background(), findByGlobAlphaMount, findByGlobAlphaOneDir)
		So(errors.Is(err, errPathNotFound), ShouldBeTrue)
		So(ref.dirID, ShouldEqual, uint32(0))
		So(conn.args, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			catalogPathHash(findByGlobAlphaOneDir),
			findByGlobAlphaOneDir,
		})
		So(conn.query, ShouldContainSubstring, "path_hash = ?")
		So(conn.query, ShouldContainSubstring, "full_path = ?")
	})

	Convey("C2 catalog resolver returns the verified catalog dir_id", t, func() {
		conn := &c2CatalogResolveConn{
			row: c2CatalogRow{
				dirID:      42,
				subtreeEnd: 48,
				fullPath:   findByGlobAlphaOneDir,
			},
		}
		client := &Client{
			cfg:  Config{QueryTimeout: time.Second},
			conn: conn,
		}

		ref, err := client.resolveCatalogDir(context.Background(), findByGlobAlphaMount, findByGlobAlphaOneDir)
		So(err, ShouldBeNil)
		So(ref.dirID, ShouldEqual, uint32(42))
		So(ref.subtreeEnd, ShouldEqual, uint32(48))
		So(ref.fullPath, ShouldEqual, findByGlobAlphaOneDir)
	})
}

func TestC2FileAPIUsesResolvedDirIDs(t *testing.T) {
	Convey("C2 StatPath resolves parent dir then point-lookups by dir_id and name", t, func() {
		conn := newC2FileAPIConn()
		conn.catalog[findByGlobAlphaOneDir] = c2CatalogRow{
			dirID:      42,
			subtreeEnd: 48,
			fullPath:   findByGlobAlphaOneDir,
		}
		conn.fileRows[c2FileKey{dirID: 42, name: c2OwnedFileName}] = c2FileAPIResult{
			fields: []string{fileFieldPath, fileFieldParentDir, fileFieldName},
			values: [][]any{{
				findByGlobAlphaOneDir + c2OwnedFileName,
				findByGlobAlphaOneDir,
				c2OwnedFileName,
			}},
		}
		client := c2FileAPIClient(conn, findByGlobAlphaMount)

		row, err := client.StatPath(
			context.Background(),
			findByGlobAlphaOneDir+c2OwnedFileName,
			StatOptions{Fields: []string{fileFieldPath, fileFieldParentDir, fileFieldName}},
		)
		So(err, ShouldBeNil)
		So(row.Path, ShouldEqual, findByGlobAlphaOneDir+c2OwnedFileName)
		So(row.ParentDir, ShouldEqual, findByGlobAlphaOneDir)
		So(row.Name, ShouldEqual, c2OwnedFileName)
		So(conn.queries, ShouldHaveLength, 2)
		So(conn.queries[1], ShouldContainSubstring, "FROM wrstat_files")
		So(conn.queries[1], ShouldContainSubstring, "f.dir_id = ?")
		So(conn.queries[1], ShouldContainSubstring, "f.name = ?")
		So(conn.queries[1], ShouldNotContainSubstring, "WHERE d.full_path = ?")
		So(conn.args[1], ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			c2OwnedFileName,
		})
	})

	Convey("C2 ListDir resolves dir then orders and pages by name under dir_id", t, func() {
		conn := newC2FileAPIConn()
		conn.catalog[findByGlobAlphaOneDir] = c2CatalogRow{
			dirID:      42,
			subtreeEnd: 48,
			fullPath:   findByGlobAlphaOneDir,
		}
		conn.listRows[uint32(42)] = c2FileAPIResult{
			fields: []string{fileFieldParentDir, fileFieldName},
			values: [][]any{
				{findByGlobAlphaOneDir, c2ListBName},
				{findByGlobAlphaOneDir, "z/"},
			},
		}
		client := c2FileAPIClient(conn, findByGlobAlphaMount)

		rows, err := client.ListDir(
			context.Background(),
			findByGlobAlphaOneDir,
			ListOptions{Fields: []string{fileFieldParentDir, fileFieldName}, Limit: 2, Offset: 1},
		)
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So(rows[0].ParentDir, ShouldEqual, findByGlobAlphaOneDir)
		So(rows[0].Name, ShouldEqual, c2ListBName)
		So(rows[1].Name, ShouldEqual, "z/")
		So(conn.queries[1], ShouldContainSubstring, "f.dir_id = ?")
		So(conn.queries[1], ShouldContainSubstring, "ORDER BY f.name ASC LIMIT ? OFFSET ?")
		So(conn.args[1], ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			int64(2),
			int64(1),
		})
	})

	Convey("C2 IsDir and permissions use resolved dir_id lookups", t, func() {
		conn := newC2FileAPIConn()
		conn.catalog[findByGlobAlphaOneDir] = c2CatalogRow{dirID: 42, subtreeEnd: 48, fullPath: findByGlobAlphaOneDir}
		conn.entryTypes[c2FileKey{dirID: 42, name: "z/"}] = uint8(stats.DirType)
		conn.permissionRows[c2PermissionKey{dirID: 42, name: c2OwnedFileName, uid: 10, gids: "20"}] = true
		conn.permissionAnyRows[c2PermissionAnyKey{dirID: 42, uid: 11, gids: "30"}] = true
		client := c2FileAPIClient(conn, findByGlobAlphaMount)

		isDir, err := client.IsDir(context.Background(), findByGlobAlphaOneDir+"z/")
		So(err, ShouldBeNil)
		So(isDir, ShouldBeTrue)

		ok, err := client.PermissionPath(context.Background(), findByGlobAlphaOneDir+c2OwnedFileName, 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = client.PermissionAnyInDir(context.Background(), findByGlobAlphaOneDir, 11, []uint32{30})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		So(conn.queries[1], ShouldContainSubstring, "f.dir_id = ?")
		So(conn.queries[3], ShouldContainSubstring, "f.dir_id = ?")
		So(conn.queries[5], ShouldContainSubstring, "FROM wrstat_dir_facts f")
		So(conn.queries[5], ShouldContainSubstring, "f.dir_id = ?")
		So(conn.queries[5], ShouldNotContainSubstring, "INNER JOIN wrstat_dirs")
	})
}

func newC2FileAPIConn() *c2FileAPIConn {
	return &c2FileAPIConn{
		catalog:           make(map[string]c2CatalogRow),
		fileRows:          make(map[c2FileKey]c2FileAPIResult),
		listRows:          make(map[uint32]c2FileAPIResult),
		entryTypes:        make(map[c2FileKey]uint8),
		permissionRows:    make(map[c2PermissionKey]bool),
		permissionAnyRows: make(map[c2PermissionAnyKey]bool),
	}
}

func c2FileAPIClient(conn *c2FileAPIConn, mountPath string) *Client {
	return &Client{
		cfg:         Config{QueryTimeout: time.Second},
		conn:        conn,
		mountPoints: basedirs.ValidateMountPoints([]string{mountPath}),
	}
}

func TestC2MountRootFileAPIUsesReservedChain(t *testing.T) {
	Convey("C2 mount-root file API resolves through reserved parent and data-root ids", t, func() {
		const (
			mountPath = "/m/teamX/"
			parentDir = "/m/"
			dataRoot  = uint32(7)
		)

		conn := newC2FileAPIConn()
		conn.catalog[parentDir] = c2CatalogRow{dirID: dataRoot - 1, subtreeEnd: dataRoot + 4, fullPath: parentDir}
		conn.catalog[mountPath] = c2CatalogRow{dirID: dataRoot, subtreeEnd: dataRoot + 4, fullPath: mountPath}
		conn.fileRows[c2FileKey{dirID: dataRoot - 1, name: c2MountRootName}] = c2FileAPIResult{
			fields: []string{
				fileFieldPath,
				fileFieldParentDir,
				fileFieldName,
				fileFieldEntryType,
				fileFieldUID,
				fileFieldGID,
			},
			values: [][]any{{
				mountPath,
				parentDir,
				c2MountRootName,
				uint8(stats.DirType),
				uint32(30),
				uint32(40),
			}},
		}
		conn.listRows[dataRoot] = c2FileAPIResult{
			fields: []string{fileFieldParentDir, fileFieldName},
			values: [][]any{
				{mountPath, c2ListBName},
				{mountPath, "z/"},
			},
		}
		conn.permissionRows[c2PermissionKey{dirID: dataRoot - 1, name: c2MountRootName, uid: 30, gids: ""}] = true
		conn.permissionRows[c2PermissionKey{dirID: dataRoot - 1, name: c2MountRootName, uid: 31, gids: "40"}] = true
		conn.permissionAnyRows[c2PermissionAnyKey{dirID: dataRoot, uid: 31, gids: "40"}] = true

		client := c2FileAPIClient(conn, mountPath)
		ctx := context.Background()

		row, err := client.StatPath(ctx, mountPath, StatOptions{Fields: []string{
			fileFieldPath,
			fileFieldParentDir,
			fileFieldName,
			fileFieldEntryType,
			fileFieldUID,
			fileFieldGID,
		}})
		So(err, ShouldBeNil)
		So(row.Path, ShouldEqual, mountPath)
		So(row.ParentDir, ShouldEqual, parentDir)
		So(row.Name, ShouldEqual, c2MountRootName)
		So(row.EntryType, ShouldEqual, byte(stats.DirType))
		So(row.UID, ShouldEqual, uint32(30))
		So(row.GID, ShouldEqual, uint32(40))

		rows, err := client.ListDir(
			ctx,
			mountPath,
			ListOptions{Fields: []string{fileFieldParentDir, fileFieldName}, Limit: 2, Offset: 1},
		)
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So(rows[0].Name, ShouldEqual, c2ListBName)
		So(rows[1].Name, ShouldEqual, "z/")
		So(rows[0].ParentDir, ShouldEqual, mountPath)

		ok, err := client.PermissionPath(ctx, mountPath, 30, nil)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = client.PermissionPath(ctx, mountPath, 31, []uint32{40})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		ok, err = client.PermissionPath(ctx, mountPath, 31, []uint32{41})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)

		ok, err = client.PermissionAnyInDir(ctx, mountPath, 31, []uint32{40})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(conn.sawPermissionAnyDirID(dataRoot), ShouldBeTrue)
	})
}

type c2CatalogResolveConn struct {
	bootstrapTestConn

	query string
	args  []any
	row   c2CatalogRow
}

func (c *c2CatalogResolveConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.query = query

	c.args = append([]any(nil), args...)

	return newC2Rows([][]any{{
		c.row.dirID,
		c.row.subtreeEnd,
		c.row.fullPath,
	}}), nil
}

func newC2Rows(rows [][]any) *c2Rows {
	return &c2Rows{rows: rows}
}

type c2FileKey struct {
	dirID uint32
	name  string
}

type c2PermissionKey struct {
	dirID uint32
	name  string
	uid   uint32
	gids  string
}

type c2PermissionAnyKey struct {
	dirID uint32
	uid   uint32
	gids  string
}

type c2FileAPIResult struct {
	fields []string
	values [][]any
}

type c2FileAPIConn struct {
	bootstrapTestConn

	catalog           map[string]c2CatalogRow
	fileRows          map[c2FileKey]c2FileAPIResult
	listRows          map[uint32]c2FileAPIResult
	entryTypes        map[c2FileKey]uint8
	permissionRows    map[c2PermissionKey]bool
	permissionAnyRows map[c2PermissionAnyKey]bool
	permissionAnyIDs  []uint32

	queries []string
	args    [][]any
}

func (c *c2FileAPIConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)
	c.args = append(c.args, append([]any(nil), args...))

	switch {
	case strings.Contains(query, "FROM wrstat_dirs") && strings.Contains(query, "path_hash"):
		return c.catalogRows(args), nil
	case strings.Contains(query, "SELECT f.entry_type"):
		return c.entryTypeRows(args), nil
	case strings.Contains(query, "FROM wrstat_dir_facts"):
		return c.permissionAnyResultRows(args), nil
	case strings.Contains(query, "AND (f.uid = ? OR has(?, f.gid))"):
		return c.permissionPathResultRows(args), nil
	case strings.Contains(query, "ORDER BY f.name ASC"):
		return c.listResultRows(args), nil
	default:
		return c.fileResultRows(args), nil
	}
}

func (c *c2FileAPIConn) catalogRows(args []any) driver.Rows {
	fullPath := c2Arg[string](args, 3)

	row, ok := c.catalog[fullPath]
	if !ok {
		return newC2Rows(nil)
	}

	return newC2Rows([][]any{{row.dirID, row.subtreeEnd, row.fullPath}})
}

func (c *c2FileAPIConn) fileResultRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)
	name := c2Arg[string](args, 3)

	result, ok := c.fileRows[c2FileKey{dirID: dirID, name: name}]
	if !ok {
		return newC2Rows(nil)
	}

	return newC2Rows(result.values)
}

func (c *c2FileAPIConn) listResultRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)

	result, ok := c.listRows[dirID]
	if !ok {
		return newC2Rows(nil)
	}

	return newC2Rows(result.values)
}

func (c *c2FileAPIConn) entryTypeRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)
	name := c2Arg[string](args, 3)

	entryType, ok := c.entryTypes[c2FileKey{dirID: dirID, name: name}]
	if !ok {
		return newC2Rows(nil)
	}

	return newC2Rows([][]any{{entryType}})
}

func (c *c2FileAPIConn) permissionPathResultRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)
	name := c2Arg[string](args, 3)
	uid := c2Arg[uint32](args, 4)

	gids := c2Arg[[]uint32](args, 5)
	if !c.permissionRows[c2PermissionKey{dirID: dirID, name: name, uid: uid, gids: c2GIDKey(gids)}] {
		return newC2Rows(nil)
	}

	return newC2Rows([][]any{{}})
}

func c2GIDKey(gids []uint32) string {
	parts := make([]string, 0, len(gids))
	for _, gid := range gids {
		parts = append(parts, strconv.FormatUint(uint64(gid), 10))
	}

	return strings.Join(parts, ",")
}

func (c *c2FileAPIConn) permissionAnyResultRows(args []any) driver.Rows {
	dirID := c2Arg[uint32](args, 2)
	uid := c2Arg[uint32](args, 4)
	gids := c2Arg[[]uint32](args, 5)

	c.permissionAnyIDs = append(c.permissionAnyIDs, dirID)

	if !c.permissionAnyRows[c2PermissionAnyKey{dirID: dirID, uid: uid, gids: c2GIDKey(gids)}] {
		return newC2Rows(nil)
	}

	return newC2Rows([][]any{{}})
}

func (c *c2FileAPIConn) sawPermissionAnyDirID(dirID uint32) bool {
	for _, seen := range c.permissionAnyIDs {
		if seen == dirID {
			return true
		}
	}

	return false
}

type c2Rows struct {
	rows [][]any
	pos  int
}

func (r *c2Rows) Next() bool {
	if r.pos >= len(r.rows) {
		return false
	}

	r.pos++

	return true
}

func (r *c2Rows) HasData() bool {
	return len(r.rows) > 0
}

func (r *c2Rows) Scan(dests ...any) error {
	row := r.rows[r.pos-1]
	if len(dests) != len(row) {
		return errBootstrapTestUnexpectedScanDestinationN
	}

	for i, dest := range dests {
		if err := c2AssignScanDest(dest, row[i]); err != nil {
			return err
		}
	}

	return nil
}

func c2AssignScanDest(dest any, value any) error {
	switch d := dest.(type) {
	case *string:
		v, ok := value.(string)
		if !ok {
			return errBootstrapTestUnexpectedScanDestination
		}

		*d = v
	case *uint8:
		v, ok := value.(uint8)
		if !ok {
			return errBootstrapTestUnexpectedScanDestination
		}

		*d = v
	case *uint32:
		v, ok := value.(uint32)
		if !ok {
			return errBootstrapTestUnexpectedScanDestination
		}

		*d = v
	case *uint64:
		v, ok := value.(uint64)
		if !ok {
			return errBootstrapTestUnexpectedScanDestination
		}

		*d = v
	case *time.Time:
		v, ok := value.(time.Time)
		if !ok {
			return errBootstrapTestUnexpectedScanDestination
		}

		*d = v
	default:
		return errBootstrapTestUnexpectedScanDestination
	}

	return nil
}

func (r *c2Rows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *c2Rows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *c2Rows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *c2Rows) Columns() []string {
	return nil
}

func (r *c2Rows) Close() error {
	return nil
}

func (r *c2Rows) Err() error {
	return nil
}

func TestFileAPISelectFields(t *testing.T) {
	Convey("file API query builders keep full-row default selects", t, func() {
		listQuery, listFields, err := listDirQuery(ListOptions{})
		So(err, ShouldBeNil)
		So(listQuery, ShouldContainSubstring, "SELECT "+fileRowSelectAll+" FROM")
		So(listFields, ShouldResemble, defaultFileRowFields())

		statQuery, statFields, err := statPathQuery(StatOptions{})
		So(err, ShouldBeNil)
		So(statQuery, ShouldContainSubstring, "SELECT "+fileRowSelectAll+" FROM")
		So(statFields, ShouldResemble, defaultFileRowFields())

		selectList, fields, err := fileRowSelectList(nil)
		So(err, ShouldBeNil)
		So(selectList, ShouldEqual, fileRowSelectAll)
		So(fields, ShouldResemble, defaultFileRowFields())
	})

	Convey("file API query builders select only requested columns", t, func() {
		listQuery, listFields, err := listDirQuery(ListOptions{
			Fields: []string{fileFieldPath, fileFieldExt, fileFieldEntryType},
		})
		So(err, ShouldBeNil)
		So(listQuery, ShouldContainSubstring, "SELECT concat(d.full_path, f.name), f.ext, f.entry_type FROM")
		So(listQuery, ShouldNotContainSubstring, "SELECT concat(d.full_path, f.name), f.ext, f.entry_type, d.full_path")
		So(listQuery, ShouldNotContainSubstring, "f.size")
		So(listFields, ShouldResemble, []string{fileFieldPath, fileFieldExt, fileFieldEntryType})

		statQuery, statFields, err := statPathQuery(StatOptions{
			Fields: []string{fileFieldPath},
		})
		So(err, ShouldBeNil)
		So(statQuery, ShouldContainSubstring, "SELECT concat(d.full_path, f.name) FROM")
		So(statQuery, ShouldNotContainSubstring, "SELECT concat(d.full_path, f.name), d.full_path")
		So(statQuery, ShouldNotContainSubstring, "f.size")
		So(statFields, ShouldResemble, []string{fileFieldPath})

		selectList, fields, err := fileRowSelectList([]string{fileFieldPath})
		So(err, ShouldBeNil)
		So(selectList, ShouldEqual, "concat(d.full_path, f.name)")
		So(fields, ShouldResemble, []string{fileFieldPath})

		globQuery, _ := buildFindByGlobQueryAndParams(
			selectList,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{"*"},
			0,
			0,
			nil,
			defaultFileLimit,
			0,
		)
		So(globQuery, ShouldContainSubstring, "SELECT concat(d.full_path, f.name) FROM")
		So(globQuery, ShouldNotContainSubstring, "SELECT concat(d.full_path, f.name), d.full_path")
		So(globQuery, ShouldNotContainSubstring, "f.size")
	})
}

type findByGlobEmptyRows struct{}

func (r *findByGlobEmptyRows) Next() bool {
	return false
}

func (r *findByGlobEmptyRows) HasData() bool {
	return false
}

func (r *findByGlobEmptyRows) Scan(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *findByGlobEmptyRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) Columns() []string {
	return nil
}

func (r *findByGlobEmptyRows) Close() error {
	return nil
}

func (r *findByGlobEmptyRows) Err() error {
	return nil
}

type findByGlobQueryCountConn struct {
	bootstrapTestConn

	mu         sync.Mutex
	queryCount int
}

func (c *findByGlobQueryCountConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if strings.Contains(query, "FROM wrstat_dirs") && strings.Contains(query, "path_hash") {
		fullPath := c2Arg[string](args, 3)
		dirID := uint32(catalogPathHash(fullPath)) //nolint:gosec // Test IDs are deterministic fixture values.

		return newC2Rows([][]any{{dirID, dirID + 4, fullPath}}), nil
	}

	c.queryCount++

	return &findByGlobEmptyRows{}, nil
}

func (c *findByGlobQueryCountConn) queryCountValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.queryCount
}

func TestClientFindByGlobQueryGrouping(t *testing.T) {
	Convey("Client.FindByGlob issues one query per mount group and pattern chunk", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		mountPoints := basedirs.ValidateMountPoints([]string{
			findByGlobAlphaMount,
			findByGlobBetaMount,
		})

		newClient := func(conn *findByGlobQueryCountConn) *Client {
			return &Client{
				cfg:         Config{QueryTimeout: time.Second},
				conn:        conn,
				mountPoints: mountPoints,
			}
		}

		Convey("same-mount base dirs share one query", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			rows, err := client.FindByGlob(
				ctx,
				[]string{findByGlobAlphaOne, findByGlobAlphaTwo},
				[]string{"*"},
				FindOptions{},
			)
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 1)
		})

		Convey("pattern chunks still split per mount group", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			patterns := make([]string, 33)
			for i := range 33 {
				patterns[i] = "*"
			}

			rows, err := client.FindByGlob(
				ctx,
				[]string{findByGlobAlphaOne, findByGlobAlphaTwo},
				patterns,
				FindOptions{},
			)
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 2)
		})

		Convey("different mounts still query once each", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			rows, err := client.FindByGlob(
				ctx,
				[]string{findByGlobAlphaOne, findByGlobBetaTwo},
				[]string{"*"},
				FindOptions{},
			)
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 2)
		})
	})
}

func TestFindByGlobQueryShape(t *testing.T) {
	Convey("FindByGlob uses catalog parent equality for direct child patterns", t, func() {
		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{"*.txt"},
			1,
			222,
			[]uint32{111},
			100,
			0,
		)

		So(q, ShouldContainSubstring, "f.dir_id = ?")
		So(q, ShouldContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "match(f.name, ?)")
		So(q, ShouldNotContainSubstring, "match(f.path, ?)")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			"txt",
			".txt",
			"^[^/]*\\.txt$",
			int64(1),
			uint32(222),
			[]uint32{111},
			int64(100),
			int64(0),
		})
	})

	Convey("FindByGlob adds exact-safe extension pruning and keeps recursive regex authority", t, func() {
		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{findByGlobRecursiveBamPattern},
			1,
			10,
			[]uint32{20},
			100,
			0,
		)

		So(q, ShouldContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "f.name = ?")
		So(q, ShouldContainSubstring, "match(concat(d.full_path, f.name), ?)")
		So(q, ShouldContainSubstring, "f.uid = ? OR has(?, f.gid)")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			uint32(48),
			findByGlobBamExt,
			findByGlobDotBam,
			findByGlobRecursiveBamRegex,
			int64(1),
			uint32(10),
			[]uint32{20},
			int64(100),
			int64(0),
		})
	})

	Convey("FindByGlob does not add extension pruning for non-exact extension globs", t, func() {
		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{"*.[bc]am"},
			0,
			0,
			nil,
			100,
			0,
		)

		So(q, ShouldNotContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "match(f.name, ?)")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			"^[^/]*\\.\\[bc\\]am$",
			int64(0),
			uint32(0),
			[]uint32(nil),
			int64(100),
			int64(0),
		})
	})

	Convey("FindByGlob prunes multi-dot exact globs by final extension segment", t, func() {
		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{"*.tar.gz"},
			0,
			0,
			nil,
			100,
			0,
		)

		So(q, ShouldContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "match(f.name, ?)")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			"gz",
			".gz",
			"^[^/]*\\.tar\\.gz$",
			int64(0),
			uint32(0),
			[]uint32(nil),
			int64(100),
			int64(0),
		})
	})

	Convey("FindByGlob omits redundant path regexes for recursive match-all patterns", t, func() {
		q, params := buildFindByGlobQueryAndParams(
			fileRowSelectAll,
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{"**"},
			0,
			0,
			nil,
			100,
			0,
		)

		So(q, ShouldContainSubstring, "f.dir_id >= ? AND f.dir_id < ?")
		So(strings.Count(q, "match("), ShouldEqual, 0)
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			uint32(48),
			int64(0),
			uint32(0),
			[]uint32(nil),
			int64(100),
			int64(0),
		})
	})

	Convey("CountByGlob reuses glob predicates without row materialisation", t, func() {
		q, params := buildCountByGlobQueryAndParams(
			findByGlobAlphaMount,
			[]catalogDirRef{findByGlobAlphaOneRef()},
			[]string{findByGlobRecursiveBamPattern},
			1,
			10,
			[]uint32{20},
		)

		So(q, ShouldContainSubstring, "SELECT count() FROM")
		So(q, ShouldContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "f.name = ?")
		So(q, ShouldContainSubstring, "match(concat(d.full_path, f.name), ?)")
		So(q, ShouldContainSubstring, "f.uid = ? OR has(?, f.gid)")
		So(q, ShouldNotContainSubstring, "ORDER BY")
		So(q, ShouldNotContainSubstring, "LIMIT ? OFFSET ?")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			uint32(48),
			findByGlobBamExt,
			findByGlobDotBam,
			findByGlobRecursiveBamRegex,
			int64(1),
			uint32(10),
			[]uint32{20},
		})
	})
}

type permissionPathRows struct {
	ok   bool
	seen bool
}

func (r *permissionPathRows) Next() bool {
	if r.seen || !r.ok {
		return false
	}

	r.seen = true

	return true
}

func (r *permissionPathRows) HasData() bool {
	return r.ok
}

func (r *permissionPathRows) Scan(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *permissionPathRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *permissionPathRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *permissionPathRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *permissionPathRows) Columns() []string {
	return nil
}

func (r *permissionPathRows) Close() error {
	return nil
}

func (r *permissionPathRows) Err() error {
	return nil
}

type permissionPathSpyConn struct {
	bootstrapTestConn

	query string
	args  []any
	ok    bool
}

func (c *permissionPathSpyConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM wrstat_dirs") && strings.Contains(query, "path_hash") {
		return newC2Rows([][]any{{uint32(42), uint32(48), findByGlobAlphaOneDir}}), nil
	}

	c.query = query

	c.args = append([]any(nil), args...)

	return &permissionPathRows{ok: c.ok}, nil
}

func TestClientPermissionPathQueryShape(t *testing.T) {
	Convey("Client.PermissionPath reads exact wrstat_files rows with owner predicates", t, func() {
		conn := &permissionPathSpyConn{ok: true}
		client := &Client{
			cfg:         Config{QueryTimeout: time.Second},
			conn:        conn,
			mountPoints: basedirs.ValidateMountPoints([]string{findByGlobAlphaMount}),
		}

		ok, err := client.PermissionPath(context.Background(), findByGlobAlphaOneDir+c2OwnedFileName, 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(conn.query, ShouldContainSubstring, "FROM wrstat_files")
		So(conn.query, ShouldNotContainSubstring, "wrstat_dguta")
		So(conn.query, ShouldNotContainSubstring, "wrstat_dir_facts")
		So(conn.query, ShouldNotContainSubstring, "wrstat_children")
		So(conn.query, ShouldContainSubstring, "f.dir_id = ?")
		So(conn.query, ShouldContainSubstring, "f.name = ?")
		So(conn.query, ShouldNotContainSubstring, "WHERE d.full_path = ?")
		So(conn.query, ShouldContainSubstring, "f.uid = ? OR has(?, f.gid)")
		So(conn.args, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			uint32(42),
			c2OwnedFileName,
			uint32(10),
			[]uint32{20},
		})
	})
}

func findByGlobAlphaOneRef() catalogDirRef {
	return catalogDirRef{
		dirID:      42,
		subtreeEnd: 48,
		fullPath:   findByGlobAlphaOneDir,
	}
}
