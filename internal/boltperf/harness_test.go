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

package boltperf

import (
	"io"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	querySuiteTestChildADir = "/root/a/"
	querySuiteTestChildBDir = "/root/b/"
	querySuiteTestGrandDir  = "/root/a/grand/"
	querySuiteTestRootDir   = "/root/"
)

func TestLineCountingReader(t *testing.T) {
	Convey("lineCountingReader", t, func() {
		Convey("reads all when maxLines is zero", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\n"), 0)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})

		Convey("stops after maxLines", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\nc\n"), 2)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})
	})
}

type querySuiteTestDB struct {
	children      map[string][]string
	summaries     map[string]*db.DirSummary
	childrenCalls []string
}

func newQuerySuiteTestDB() *querySuiteTestDB {
	return &querySuiteTestDB{
		children: map[string][]string{
			querySuiteTestRootDir:   {querySuiteTestChildADir, querySuiteTestChildBDir},
			querySuiteTestChildADir: {querySuiteTestGrandDir},
			querySuiteTestChildBDir: {},
		},
		summaries: map[string]*db.DirSummary{
			querySuiteTestRootDir:   {Count: 3},
			querySuiteTestChildADir: {Count: 2},
			querySuiteTestChildBDir: {Count: 1},
			querySuiteTestGrandDir:  {Count: 1},
		},
	}
}

func (d *querySuiteTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	cp := *summary

	return &cp, nil
}

func (d *querySuiteTestDB) Children(dir string) ([]string, error) {
	d.childrenCalls = append(d.childrenCalls, dir)

	return d.children[dir], nil
}

func (d *querySuiteTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *querySuiteTestDB) Close() error {
	return nil
}

func TestQuerySuiteOps(t *testing.T) {
	Convey("buildQuerySuiteOps reports DiskTree endpoint", t, func() {
		ctx := queryContext{
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Splits: 4})
		names := make([]string, 0, len(ops))

		for _, op := range ops {
			names = append(names, op.name)
		}

		So(names, ShouldContain, "tree_disktree_endpoint")
	})

	Convey("tree_disktree_endpoint checks all child has_children values via Tree fallback", t, func() {
		database := newQuerySuiteTestDB()
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
		}

		err := opTreeDiskTreeEndpoint(ctx)

		So(err, ShouldBeNil)
		So(database.childrenCalls, ShouldResemble, []string{
			querySuiteTestRootDir,
			querySuiteTestChildADir,
			querySuiteTestChildBDir,
		})
	})
}
