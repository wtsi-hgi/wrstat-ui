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
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
)

const (
	querySuiteTestChildADir  = "/root/a/"
	querySuiteTestChildBDir  = "/root/b/"
	querySuiteTestGrandDir   = "/root/a/grand/"
	querySuiteTestRootDir    = "/root/"
	querySuiteTestNoMatchGID = 404
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

func TestDirsForRepeats(t *testing.T) {
	Convey("uniqueDirsForRepeats caps timings at the discovered dirs", t, func() {
		dirs := []string{"/a/", "/b/"}

		So(uniqueDirsForRepeats(dirs, 3), ShouldResemble, dirs)
		So(uniqueDirsForRepeats(dirs, 0), ShouldBeNil)
		So(uniqueDirsForRepeats(nil, 3), ShouldBeNil)
	})

	Convey("cycledDirsForRepeats repeats ancestor clicks up to the requested count", t, func() {
		dirs := []string{"/", querySuiteTestRootDir}

		So(cycledDirsForRepeats(dirs, 5), ShouldResemble, []string{
			"/",
			querySuiteTestRootDir,
			"/",
			querySuiteTestRootDir,
			"/",
		})
		So(cycledDirsForRepeats(dirs, 0), ShouldBeNil)
		So(cycledDirsForRepeats(nil, 5), ShouldBeNil)
	})
}

type noMatchFilteredTreeDB struct {
	dirInfoCalls []string
}

func (d *noMatchFilteredTreeDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	d.dirInfoCalls = append(d.dirInfoCalls, dir)

	if hasNoMatchGID(filter) {
		return nil, nil //nolint:nilnil
	}

	return &db.DirSummary{Count: dirPickMinCount}, nil
}

func hasNoMatchGID(filter *db.Filter) bool {
	if filter == nil {
		return false
	}

	for _, gid := range filter.GIDs {
		if gid == querySuiteTestNoMatchGID {
			return true
		}
	}

	return false
}

func (*noMatchFilteredTreeDB) Children(string) ([]string, error) {
	return []string{querySuiteTestChildADir}, nil
}

func (*noMatchFilteredTreeDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (*noMatchFilteredTreeDB) Close() error {
	return nil
}

func TestPickRepresentativeDirFromTree(t *testing.T) {
	Convey("pickRepresentativeDirFromTree stops at the current directory when a filter has no matching rows", t, func() {
		database := &noMatchFilteredTreeDB{}
		filter := &db.Filter{GIDs: []uint32{querySuiteTestNoMatchGID}, Age: db.DGUTAgeAll}

		dir := pickRepresentativeDirFromTree(db.NewTree(database), querySuiteTestRootDir, filter)

		So(dir, ShouldEqual, querySuiteTestRootDir)
		So(database.dirInfoCalls, ShouldResemble, []string{querySuiteTestRootDir})
	})
}

type querySuiteTestDB struct {
	children       map[string][]string
	summaries      map[string]*db.DirSummary
	dirInfoCalls   []string
	dirInfoFilters []*db.Filter
	childrenCalls  []string
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

func (d *querySuiteTestDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	d.dirInfoCalls = append(d.dirInfoCalls, dir)
	d.dirInfoFilters = append(d.dirInfoFilters, cloneQuerySuiteTestFilter(filter))

	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	cp := *summary

	return &cp, nil
}

func cloneQuerySuiteTestFilter(filter *db.Filter) *db.Filter {
	if filter == nil {
		return nil
	}

	return &db.Filter{
		GIDs: append([]uint32(nil), filter.GIDs...),
		UIDs: append([]uint32(nil), filter.UIDs...),
		FT:   filter.FT,
		Age:  filter.Age,
	}
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
			datasetDirs: []string{
				filepath.Join("/tmp", "20260101_"+mountpath.EncodeKey(querySuiteTestChildADir)),
				filepath.Join("/tmp", "20260101_"+mountpath.EncodeKey("/nfs/team/")),
			},
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
			openFreshTree: func() (*db.Tree, func() error, error) {
				return db.NewTree(newQuerySuiteTestDB()), func() error { return nil }, nil
			},
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{
			Repeat:        5,
			Splits:        4,
			WalkDepth:     1,
			WalkLimit:     2,
			AncestorLimit: 5,
		})
		names := make([]string, 0, len(ops))

		for _, op := range ops {
			names = append(names, op.name)
		}

		coldCachedWhereOp := findQuerySuiteTestOp(ops, queryOpTreeWhereColdName)
		So(coldCachedWhereOp, ShouldNotBeNil)
		So(coldCachedWhereOp.inputs["dir"], ShouldEqual, querySuiteTestRootDir)
		So(coldCachedWhereOp.inputs["cache_scope"], ShouldEqual, "same_provider_cold_then_warm")
		So(coldCachedWhereOp.inputs["duration_source"], ShouldEqual, "wall")
		So(coldCachedWhereOp.inputs["splits"], ShouldEqual, 4)
		So(coldCachedWhereOp.skipWarmup, ShouldBeTrue)

		So(names, ShouldContain, queryOpTreeDiskTreeEndName)
		So(names, ShouldContain, queryOpTreeDiskTreeNewName)
		So(names, ShouldContain, queryOpTreeDiskTreeVisibleChildName)
		So(names, ShouldContain, queryOpTreeDiskTreeAncName)
		So(names, ShouldContain, queryOpTreeWhereColdName)
		So(names, ShouldContain, queryOpTreeWhereFreshName)
		So(querySuiteTestOpIndex(names, queryOpTreeWhereColdName), ShouldBeLessThan,
			querySuiteTestOpIndex(names, queryOpTreeDiskTreeNewName))
		So(querySuiteTestOpIndex(names, queryOpTreeWhereColdName), ShouldBeLessThan,
			querySuiteTestOpIndex(names, queryOpTreeDirInfoName))
		So(querySuiteTestOpIndex(names, queryOpTreeWhereColdName), ShouldBeLessThan,
			querySuiteTestOpIndex(names, queryOpTreeDiskTreeEndName))
		So(querySuiteTestOpIndex(names, queryOpTreeWhereColdName), ShouldBeLessThan,
			querySuiteTestOpIndex(names, queryOpTreeWhereName))

		newDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeNewName)
		So(newDirsOp, ShouldNotBeNil)
		So(newDirsOp.inputs["start_dir"], ShouldEqual, querySuiteTestRootDir)
		So(newDirsOp.inputs["dirs"], ShouldResemble, []string{querySuiteTestChildADir, querySuiteTestChildBDir})
		So(newDirsOp.inputs["cache_scope"], ShouldEqual, "new_directory_each_repeat")
		So(newDirsOp.skipWarmup, ShouldBeTrue)
		So(newDirsOp.repeatOverride, ShouldEqual, 2)

		visibleChildDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		So(visibleChildDirsOp, ShouldNotBeNil)
		So(visibleChildDirsOp.inputs["parent_dir"], ShouldEqual, querySuiteTestRootDir)
		So(visibleChildDirsOp.inputs["cache_scope"], ShouldEqual, queryScopeVisibleChildDirs)
		So(visibleChildDirsOp.inputs["duration_source"], ShouldEqual, "wall")
		So(visibleChildDirsOp.inputs[queryInputAgeKey], ShouldEqual, int(db.DGUTAgeAll))
		So(visibleChildDirsOp.skipWarmup, ShouldBeTrue)

		ancestorOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeAncName)
		So(ancestorOp, ShouldNotBeNil)
		So(ancestorOp.inputs["start_dir"], ShouldEqual, "/")
		So(ancestorOp.inputs["dirs"], ShouldResemble, []string{
			"/",
			"/nfs/",
			"/nfs/team/",
			querySuiteTestRootDir,
			querySuiteTestChildADir,
		})
		So(ancestorOp.inputs["cache_scope"], ShouldEqual, queryScopeAncestorDirs)
		So(ancestorOp.skipWarmup, ShouldBeTrue)
		So(ancestorOp.repeatOverride, ShouldEqual, 5)

		freshWhereOp := findQuerySuiteTestOp(ops, queryOpTreeWhereFreshName)
		So(freshWhereOp, ShouldNotBeNil)
		So(freshWhereOp.inputs["dir"], ShouldEqual, querySuiteTestRootDir)
		So(freshWhereOp.inputs["cache_scope"], ShouldEqual, "fresh_provider_per_repeat")
		So(freshWhereOp.skipWarmup, ShouldBeTrue)
	})

	Convey("tree_where uses the configured tree filter and records it", t, func() {
		database := newQuerySuiteTestDB()
		filter := querySuiteTestTreeFilter()
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Splits: 2, TreeFilter: filter})
		whereOp := findQuerySuiteTestOp(ops, queryOpTreeWhereName)
		So(whereOp, ShouldNotBeNil)
		assertQuerySuiteTestTreeFilterInputs(whereOp.inputs, filter)

		So(whereOp.op(), ShouldBeNil)
		So(database.dirInfoFilters, ShouldNotBeEmpty)
		So(database.dirInfoFilters[0], ShouldResemble, filter)
	})

	Convey("tree_disktree_endpoint uses the configured tree filter and records it", t, func() {
		database := newQuerySuiteTestDB()
		filter := querySuiteTestTreeFilter()
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{TreeFilter: filter})
		disktreeOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeEndName)
		So(disktreeOp, ShouldNotBeNil)
		assertQuerySuiteTestTreeFilterInputs(disktreeOp.inputs, filter)

		So(disktreeOp.op(), ShouldBeNil)
		So(database.dirInfoFilters, ShouldNotBeEmpty)
		So(database.dirInfoFilters[0], ShouldResemble, filter)
	})

	Convey("tree_disktree_endpoint_new_dirs times walked dirs instead of the selected warm dir", t, func() {
		database := newQuerySuiteTestDB()
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
			openFreshTree: func() (*db.Tree, func() error, error) {
				return db.NewTree(newQuerySuiteTestDB()), func() error { return nil }, nil
			},
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Repeat: 2, WalkDepth: 1, WalkLimit: 2})
		newDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeNewName)
		So(newDirsOp, ShouldNotBeNil)

		database.childrenCalls = nil

		So(newDirsOp.op(), ShouldBeNil)
		So(newDirsOp.op(), ShouldBeNil)
		So(database.childrenCalls, ShouldNotContain, querySuiteTestRootDir)
		So(database.childrenCalls, ShouldContain, querySuiteTestChildADir)
		So(database.childrenCalls, ShouldContain, querySuiteTestChildBDir)
	})

	Convey("tree_disktree_endpoint_new_dirs discovers candidates with a fresh tree", t, func() {
		timingDB := newQuerySuiteTestDB()
		discoveryDB := newQuerySuiteTestDB()
		closeCalls := 0
		ctx := queryContext{
			tree:     db.NewTree(timingDB),
			queryDir: querySuiteTestRootDir,
			openFreshTree: func() (*db.Tree, func() error, error) {
				return db.NewTree(discoveryDB), func() error {
					closeCalls++

					return nil
				}, nil
			},
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Repeat: 3, WalkDepth: 2, WalkLimit: 3})
		newDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeNewName)
		So(newDirsOp, ShouldNotBeNil)
		So(newDirsOp.inputs["dirs"], ShouldResemble, []string{
			querySuiteTestChildBDir,
			querySuiteTestGrandDir,
		})
		So(closeCalls, ShouldEqual, 1)
		So(discoveryDB.dirInfoCalls, ShouldContain, querySuiteTestRootDir)
		So(discoveryDB.dirInfoCalls, ShouldContain, querySuiteTestChildADir)
		So(timingDB.dirInfoCalls, ShouldBeEmpty)
		So(timingDB.childrenCalls, ShouldBeEmpty)

		So(newDirsOp.op(), ShouldBeNil)
		So(newDirsOp.op(), ShouldBeNil)
		So(timingDB.dirInfoCalls, ShouldNotContain, querySuiteTestRootDir)
		So(timingDB.dirInfoCalls, ShouldNotContain, querySuiteTestChildADir)
		So(timingDB.dirInfoCalls, ShouldContain, querySuiteTestChildBDir)
		So(timingDB.dirInfoCalls, ShouldContain, querySuiteTestGrandDir)
	})

	Convey("tree_disktree_endpoint_visible_child_dirs loads parent before timing visible children", t, func() {
		database := newQuerySuiteTestDB()
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Repeat: 5})
		visibleChildDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		report := NewReport("bolt", "", 5, 3)

		So(visibleChildDirsOp, ShouldNotBeNil)
		So(timeAndReportQueryOp(
			&report,
			QueryOptions{Repeat: 5, Warmup: 3},
			func(string, ...any) {},
			*visibleChildDirsOp,
		), ShouldBeNil)

		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDiskTreeVisibleChildName)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 2)
		So(report.Operations[0].Inputs["parent_dir"], ShouldEqual, querySuiteTestRootDir)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{
			querySuiteTestChildADir,
			querySuiteTestChildBDir,
		})
		So(report.Operations[0].Inputs["child_count"], ShouldEqual, 2)
		So(report.Operations[0].Inputs["cache_scope"], ShouldEqual, queryScopeVisibleChildDirs)
		So(report.Operations[0].Inputs["duration_source"], ShouldEqual, "wall")
		So(report.Operations[0].Inputs[queryInputAgeKey], ShouldEqual, int(db.DGUTAgeAll))
		So(countQuerySuiteTestDir(database.dirInfoCalls, querySuiteTestRootDir), ShouldEqual, 1)
		So(database.dirInfoCalls, ShouldContain, querySuiteTestChildADir)
		So(database.dirInfoCalls, ShouldContain, querySuiteTestChildBDir)
	})

	Convey("tree_disktree_endpoint_visible_child_dirs falls back to the parent when it has no children", t, func() {
		database := newQuerySuiteTestDB()
		database.children[querySuiteTestRootDir] = nil
		database.summaries = map[string]*db.DirSummary{
			querySuiteTestRootDir: {Count: 1},
		}
		ctx := queryContext{
			tree:     db.NewTree(database),
			queryDir: querySuiteTestRootDir,
		}

		ops := buildQuerySuiteOps(ctx, QueryOptions{Repeat: 5})
		visibleChildDirsOp := findQuerySuiteTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		report := NewReport("bolt", "", 5, 0)

		So(visibleChildDirsOp, ShouldNotBeNil)
		So(timeAndReportQueryOp(
			&report,
			QueryOptions{Repeat: 5},
			func(string, ...any) {},
			*visibleChildDirsOp,
		), ShouldBeNil)

		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 1)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{querySuiteTestRootDir})
		So(report.Operations[0].Inputs["child_count"], ShouldEqual, 1)
		So(report.Operations[0].Inputs["fallback_to_parent_dir"], ShouldBeTrue)
	})

	Convey("tree_where_fresh_provider opens and closes a fresh tree for each run", t, func() {
		var (
			openCalls  int
			closeCalls int
		)

		ctx := queryContext{
			queryDir: querySuiteTestRootDir,
			openFreshTree: func() (*db.Tree, func() error, error) {
				openCalls++

				return db.NewTree(newQuerySuiteTestDB()), func() error {
					closeCalls++

					return nil
				}, nil
			},
		}

		So(opTreeWhereFreshProvider(ctx, 1), ShouldBeNil)
		So(opTreeWhereFreshProvider(ctx, 1), ShouldBeNil)
		So(openCalls, ShouldEqual, 2)
		So(closeCalls, ShouldEqual, 2)
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

func findQuerySuiteTestOp(ops []querySuiteOp, name string) *querySuiteOp {
	for i := range ops {
		if ops[i].name == name {
			return &ops[i]
		}
	}

	return nil
}

func querySuiteTestOpIndex(names []string, name string) int {
	for i, candidate := range names {
		if candidate == name {
			return i
		}
	}

	return -1
}

func querySuiteTestTreeFilter() *db.Filter {
	return &db.Filter{
		GIDs: []uint32{7, 8},
		UIDs: []uint32{9, 10},
		FT:   db.DGUTAFileTypeBam | db.DGUTAFileTypeCram,
		Age:  db.DGUTAgeAll,
	}
}

func assertQuerySuiteTestTreeFilterInputs(inputs map[string]any, filter *db.Filter) {
	So(inputs[queryInputAgeKey], ShouldEqual, int(filter.Age))
	So(inputs[queryInputFilterGIDsKey], ShouldResemble, filter.GIDs)
	So(inputs[queryInputFilterUIDsKey], ShouldResemble, filter.UIDs)
	So(inputs[queryInputFilterFileTypeMaskKey], ShouldEqual, int(filter.FT))
	So(inputs[queryInputFilterFileTypesKey], ShouldEqual, filter.FT.String())
}

func countQuerySuiteTestDir(dirs []string, target string) int {
	count := 0

	for _, dir := range dirs {
		if dir == target {
			count++
		}
	}

	return count
}

func TestRunQuerySuiteOperationSelection(t *testing.T) {
	Convey("runQuerySuite only runs selected operations in default operation order", t, func() {
		ctx := queryContext{
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
		}
		report := NewReport("bolt", "", 1, 0)

		err := runQuerySuite(&report, ctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{queryOpTreeDiskTreeEndName, queryOpTreeDirInfoName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 2)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDirInfoName)
		So(report.Operations[1].Name, ShouldEqual, queryOpTreeDiskTreeEndName)
	})

	Convey("runQuerySuite can select visible child directory timings", t, func() {
		ctx := queryContext{
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
		}
		report := NewReport("bolt", "", 1, 0)

		err := runQuerySuite(&report, ctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{queryOpTreeDiskTreeVisibleChildName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDiskTreeVisibleChildName)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{querySuiteTestChildADir})
	})

	Convey("runQuerySuite reports unknown selected operations with available names", t, func() {
		ctx := queryContext{
			tree:     db.NewTree(newQuerySuiteTestDB()),
			queryDir: querySuiteTestRootDir,
		}
		report := NewReport("bolt", "", 1, 0)

		err := runQuerySuite(&report, ctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{"missing_op", queryOpTreeDirInfoName},
		}, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "unknown query ops: missing_op")
		So(err.Error(), ShouldContainSubstring, "available ops:")
		So(err.Error(), ShouldContainSubstring, queryOpTreeDiskTreeEndName)
		So(report.Operations, ShouldHaveLength, 0)
	})
}
