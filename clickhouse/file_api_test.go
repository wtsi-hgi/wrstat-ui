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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
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

		client := &Client{mountPoints: basedirs.ValidateMountPoints([]string{findByGlobAlphaMount})}
		prepared, err := client.prepareFindByGlob(
			[]string{findByGlobAlphaOneDir},
			[]string{"*"},
			FindOptions{},
		)
		So(err, ShouldBeNil)
		So(prepared.selectList, ShouldEqual, fileRowSelectAll)
		So(prepared.fields, ShouldResemble, defaultFileRowFields())
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

		client := &Client{mountPoints: basedirs.ValidateMountPoints([]string{findByGlobAlphaMount})}
		prepared, err := client.prepareFindByGlob(
			[]string{findByGlobAlphaOneDir},
			[]string{"*"},
			FindOptions{Fields: []string{fileFieldPath}},
		)
		So(err, ShouldBeNil)
		So(prepared.selectList, ShouldEqual, "concat(d.full_path, f.name)")
		So(prepared.fields, ShouldResemble, []string{fileFieldPath})

		spec := prepared.plan.queries[0]
		globQuery, _ := buildFindByGlobQueryAndParams(
			prepared.selectList,
			spec.mountPath,
			spec.baseDirs,
			spec.patternChunk,
			prepared.ownerEnabled,
			prepared.uid,
			prepared.gids,
			prepared.queryLimit,
			prepared.queryOffset,
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

func (c *findByGlobQueryCountConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
			[]string{findByGlobAlphaOneDir},
			[]string{"*.txt"},
			1,
			222,
			[]uint32{111},
			100,
			0,
		)

		So(q, ShouldContainSubstring, "d.full_path = ?")
		So(q, ShouldContainSubstring, "f.ext = ?")
		So(q, ShouldContainSubstring, "match(f.name, ?)")
		So(q, ShouldNotContainSubstring, "match(f.path, ?)")
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			findByGlobAlphaOneDir,
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
			[]string{findByGlobAlphaOneDir},
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
			findByGlobAlphaOneDir,
			findByGlobAlphaOneDirNext,
			"bam",
			".bam",
			"^/alpha/one/(?:[^/]+/)*[^/]*\\.bam$",
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
			[]string{findByGlobAlphaOneDir},
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
			findByGlobAlphaOneDir,
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
			[]string{findByGlobAlphaOneDir},
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
			findByGlobAlphaOneDir,
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
			[]string{findByGlobAlphaOneDir},
			[]string{"**"},
			0,
			0,
			nil,
			100,
			0,
		)

		So(q, ShouldContainSubstring, "d.full_path >= ? AND d.full_path < ?")
		So(strings.Count(q, "match("), ShouldEqual, 0)
		So(params, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			findByGlobAlphaOneDir,
			findByGlobAlphaOneDirNext,
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
			[]string{findByGlobAlphaOneDir},
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
			findByGlobAlphaOneDir,
			findByGlobAlphaOneDirNext,
			"bam",
			".bam",
			"^/alpha/one/(?:[^/]+/)*[^/]*\\.bam$",
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

		ok, err := client.PermissionPath(context.Background(), findByGlobAlphaOneDir+"owned.txt", 10, []uint32{20})
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(conn.query, ShouldContainSubstring, "FROM wrstat_files")
		So(conn.query, ShouldNotContainSubstring, "wrstat_dguta")
		So(conn.query, ShouldNotContainSubstring, "wrstat_dir_facts")
		So(conn.query, ShouldNotContainSubstring, "wrstat_children")
		So(conn.query, ShouldContainSubstring, "d.full_path = ? AND f.name = ?")
		So(conn.query, ShouldContainSubstring, "f.uid = ? OR has(?, f.gid)")
		So(conn.args, ShouldResemble, []any{
			findByGlobAlphaMount,
			findByGlobAlphaMount,
			findByGlobAlphaOneDir,
			"owned.txt",
			uint32(10),
			[]uint32{20},
		})
	})
}
