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

package chperf

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

var (
	errQueryTestMeasure = errors.New("measure failed")
	errQueryTestRun     = errors.New("query failed")
)

const (
	explainPruningOutput = "mount_path partition pruning\nparent_dir key condition"
	queryTestNFSTeamPath = "/nfs/team/"
	queryOpTestChildADir = "/root/a/"
	queryOpTestChildBDir = "/root/b/"
	queryOpTestGrandDir  = "/root/a/grand/"
	queryOpTestRootDir   = "/root/"
)

func TestDecodeMountPaths(t *testing.T) {
	Convey("DecodeMountPaths converts fullwidth solidus and adds trailing slash", t, func() {
		mt := map[string]time.Time{
			"／lustre／scratch123": time.Now(),
			queryTestNFSTeamPath: time.Now(),
		}

		paths := DecodeMountPaths(mt)
		So(paths, ShouldHaveLength, 2)
		So(paths[0], ShouldEqual, "/lustre/scratch123/")
		So(paths[1], ShouldEqual, queryTestNFSTeamPath)
	})

	Convey("DecodeMountPaths returns empty for nil map", t, func() {
		paths := DecodeMountPaths(nil)
		So(paths, ShouldHaveLength, 0)
	})
}

func TestExplainHasPruning(t *testing.T) {
	Convey("ExplainHasPruning returns true when both indices appear", t, func() {
		explain := "ReadFromMergeTree\n  Indexes:\n    mount_path partition pruning\n    parent_dir key condition"
		So(ExplainHasPruning(explain), ShouldBeTrue)
	})

	Convey("ExplainHasPruning returns false when mount_path is missing", t, func() {
		explain := "ReadFromMergeTree\n  parent_dir key condition"
		So(ExplainHasPruning(explain), ShouldBeFalse)
	})

	Convey("ExplainHasPruning returns false when parent_dir is missing", t, func() {
		explain := "ReadFromMergeTree\n  mount_path partition pruning"
		So(ExplainHasPruning(explain), ShouldBeFalse)
	})

	Convey("ExplainHasPruning returns false for empty string", t, func() {
		So(ExplainHasPruning(""), ShouldBeFalse)
	})
}

func TestNormaliseDirPath(t *testing.T) {
	Convey("normaliseDirPath normalises directory paths", t, func() {
		Convey("returns empty for empty or whitespace input", func() {
			So(normaliseDirPath(""), ShouldEqual, "")
			So(normaliseDirPath("  "), ShouldEqual, "")
		})

		Convey("adds leading slash if missing", func() {
			So(normaliseDirPath("dir/"), ShouldEqual, "/dir/")
		})

		Convey("adds trailing slash if missing", func() {
			So(normaliseDirPath("/dir"), ShouldEqual, "/dir/")
		})

		Convey("returns already-normalised path unchanged", func() {
			So(normaliseDirPath("/dir/sub/"), ShouldEqual, "/dir/sub/")
		})
	})
}

func TestDirsForRepeats(t *testing.T) {
	Convey("uniqueDirsForRepeats caps timings at the discovered dirs", t, func() {
		dirs := []string{queryOpTestChildADir, queryOpTestChildBDir}

		So(uniqueDirsForRepeats(dirs, 3), ShouldResemble, dirs)
		So(uniqueDirsForRepeats(dirs, 0), ShouldBeNil)
		So(uniqueDirsForRepeats(nil, 3), ShouldBeNil)
	})

	Convey("cycledDirsForRepeats repeats ancestor clicks up to the requested count", t, func() {
		dirs := []string{"/", queryOpTestRootDir}

		So(cycledDirsForRepeats(dirs, 5), ShouldResemble, []string{
			"/",
			queryOpTestRootDir,
			"/",
			queryOpTestRootDir,
			"/",
		})
		So(cycledDirsForRepeats(dirs, 0), ShouldBeNil)
		So(cycledDirsForRepeats(nil, 5), ShouldBeNil)
	})
}

func TestPickLargestChild(t *testing.T) {
	Convey("pickLargestChild", t, func() {
		Convey("returns nil for empty slice", func() {
			So(pickLargestChild(nil), ShouldBeNil)
		})

		Convey("returns the single child", func() {
			children := []*db.DirSummary{{Dir: "/a/", Count: 5}}
			So(pickLargestChild(children).Dir, ShouldEqual, "/a/")
		})

		Convey("returns the child with the highest Count", func() {
			children := []*db.DirSummary{
				{Dir: "/a/", Count: 5},
				{Dir: "/b/", Count: 20},
				{Dir: "/c/", Count: 10},
			}

			best := pickLargestChild(children)
			So(best.Dir, ShouldEqual, "/b/")
		})
	})
}

type fakeQueryInspector struct {
	explainListDir  func(ctx context.Context, mountPath, dir string, limit, offset int64) (string, error)
	explainStatPath func(ctx context.Context, mountPath, path string) (string, error)
	measure         func(ctx context.Context, run func(ctx context.Context) error) (*QueryMetrics, error)
	closeFn         func() error
}

func (f fakeQueryInspector) ExplainListDir(
	ctx context.Context,
	mountPath string,
	dir string,
	limit int64,
	offset int64,
) (string, error) {
	if f.explainListDir != nil {
		return f.explainListDir(ctx, mountPath, dir, limit, offset)
	}

	return "", nil
}

func (f fakeQueryInspector) ExplainStatPath(
	ctx context.Context,
	mountPath string,
	path string,
) (string, error) {
	if f.explainStatPath != nil {
		return f.explainStatPath(ctx, mountPath, path)
	}

	return "", nil
}

func (f fakeQueryInspector) Measure(
	ctx context.Context,
	run func(ctx context.Context) error,
) (*QueryMetrics, error) {
	return f.measure(ctx, run)
}

func (f fakeQueryInspector) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}

	return nil
}

func TestRunOp(t *testing.T) {
	Convey("runOp executes warmups without recording measured durations", t, func() {
		var (
			runCalls      int
			measuredCalls uint64
		)

		report := boltperf.NewReport("clickhouse", "", 2, 1)

		err := runOp(
			&report,
			queryContext{inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					measuredCalls++

					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 10 + measuredCalls}, nil
				},
			}},
			op{
				name: queryOpTreeWhereName,
				run: func(context.Context) error {
					runCalls++

					return nil
				},
			},
			QueryOptions{Repeat: 2, Warmup: 1},
			func(string, ...any) {},
		)

		So(err, ShouldBeNil)
		So(runCalls, ShouldEqual, 3)
		So(measuredCalls, ShouldEqual, uint64(2))
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldResemble, []float64{11, 12})
	})

	Convey("runOp returns an error when Measure fails", t, func() {
		report := boltperf.NewReport("clickhouse", "", 2, 0)

		err := runOp(
			&report,
			queryContext{inspector: fakeQueryInspector{
				measure: func(_ context.Context, _ func(context.Context) error) (*QueryMetrics, error) {
					return nil, errQueryTestMeasure
				},
			}},
			op{
				name:   "files_listdir",
				inputs: map[string]any{queryInputDirKey: "/tmp/"},
				run:    func(context.Context) error { return nil },
			},
			QueryOptions{Repeat: 3},
			func(string, ...any) {},
		)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errQueryTestMeasure.Error())
		So(err.Error(), ShouldContainSubstring, "files_listdir")
		So(report.Operations, ShouldHaveLength, 0)
	})

	Convey("runOp returns an error when the measured operation fails", t, func() {
		var calls int

		err := runOp(
			&boltperf.Report{},
			queryContext{inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					calls++

					return nil, run(ctx)
				},
			}},
			op{
				name:   "permission_check",
				inputs: map[string]any{queryInputDirKey: "/tmp/"},
				run:    func(context.Context) error { return errQueryTestRun },
			},
			QueryOptions{Repeat: 2},
			func(string, ...any) {},
		)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errQueryTestRun.Error())
		So(calls, ShouldEqual, 1)
	})

	Convey("runOp records timings and metrics for successful runs", t, func() {
		report := boltperf.NewReport("clickhouse", "", 2, 0)

		var out strings.Builder

		err := runOp(
			&report,
			queryContext{inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 12, ReadRows: 34, ResultRows: 1}, nil
				},
			}},
			op{name: "mount_timestamps", inputs: map[string]any{}, run: func(context.Context) error { return nil }},
			QueryOptions{Repeat: 2},
			func(format string, args ...any) { _, _ = out.WriteString(strings.TrimSpace(format)) },
		)

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldResemble, []float64{12, 12})
		So(out.String(), ShouldContainSubstring, "metrics")
	})

	Convey("runOp records active mount updated_at values for mount_timestamps", t, func() {
		updatedAtA := time.Date(2026, 3, 8, 13, 14, 15, 0, time.UTC)
		updatedAtB := time.Date(2026, 3, 9, 10, 11, 12, 0, time.UTC)

		report := boltperf.NewReport("clickhouse", "", 1, 0)
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
				"／lustre":            updatedAtA,
				queryTestNFSTeamPath: updatedAtB,
			}}},
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 1}, nil
				},
			},
		}

		err := runOp(
			&report,
			qctx,
			opMountTimestamps(qctx),
			QueryOptions{Repeat: 1},
			func(string, ...any) {},
		)

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Inputs["mount_count"], ShouldEqual, 2)

		activeMounts, ok := report.Operations[0].Inputs["active_mounts"].([]activeMountFreshness)
		So(ok, ShouldBeTrue)
		So(activeMounts, ShouldResemble, []activeMountFreshness{
			{MountPath: "/lustre/", UpdatedAt: updatedAtA.Format(time.RFC3339Nano)},
			{MountPath: queryTestNFSTeamPath, UpdatedAt: updatedAtB.Format(time.RFC3339Nano)},
		})
	})

	Convey("runOp records wall durations for operations that should include full provider work", t, func() {
		var runCalls int

		report := boltperf.NewReport("clickhouse", "", 2, 0)
		qctx := queryContext{inspector: fakeQueryInspector{
			measure: func(_ context.Context, _ func(context.Context) error) (*QueryMetrics, error) {
				return nil, errQueryTestMeasure
			},
		}}

		err := runOp(
			&report,
			qctx,
			op{
				name:        "tree_where_fresh_provider",
				inputs:      map[string]any{},
				useWallTime: true,
				run: func(context.Context) error {
					runCalls++

					return nil
				},
			},
			QueryOptions{Repeat: 2},
			func(string, ...any) {},
		)

		So(err, ShouldBeNil)
		So(runCalls, ShouldEqual, 2)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 2)
	})
}

func TestRunSuiteOperationSelection(t *testing.T) {
	Convey("runSuite only runs selected operations in default operation order", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client:   &fakeQueryClient{},
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 1}, nil
				},
			},
			dir: queryOpTestRootDir,
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{queryOpTreeDiskTreeEndName, queryOpTreeDirInfoName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 2)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDirInfoName)
		So(report.Operations[1].Name, ShouldEqual, queryOpTreeDiskTreeEndName)
	})

	Convey("runSuite can select visible child directory timings", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{queryOpTreeDiskTreeVisibleChildName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDiskTreeVisibleChildName)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{queryOpTestChildADir})
	})

	Convey("runSuite reports unknown selected operations with available names", t, func() {
		qctx := queryContext{
			provider:  fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client:    &fakeQueryClient{},
			inspector: fakeQueryInspector{},
			dir:       queryOpTestRootDir,
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{"not_real", queryOpTreeDirInfoName},
		}, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "unknown query ops: not_real")
		So(err.Error(), ShouldContainSubstring, "available ops:")
		So(err.Error(), ShouldContainSubstring, queryOpTreeDiskTreeEndName)
		So(report.Operations, ShouldHaveLength, 0)
	})
}

func TestBuildQueryContext(t *testing.T) {
	Convey("buildQueryContext closes injected dependencies when selecting a dir fails", t, func() {
		providerClosed := false
		clientClosed := false
		inspectorClosed := false

		api := fakeQueryAPI{
			provider: fakeMountTimestampsProvider{
				bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{}},
				closeHook: func() error {
					providerClosed = true

					return nil
				},
			},
			client: &fakeQueryClient{closeHook: func() error {
				clientClosed = true

				return nil
			}},
			inspector: fakeQueryInspector{closeFn: func() error {
				inspectorClosed = true

				return nil
			}},
		}

		_, err := buildQueryContext(api, QueryOptions{}, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, ErrNoDatasets.Error())
		So(providerClosed, ShouldBeTrue)
		So(clientClosed, ShouldBeTrue)
		So(inspectorClosed, ShouldBeTrue)
	})
}

func TestVerifyPlans(t *testing.T) {
	Convey("verifyPlans fails when ExplainStatPath lacks pruning", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
				"／mnt／test": time.Now(),
			}}},
			client: &fakeQueryClient{rows: []QueryRow{{Path: "/mnt/test/project/file.txt"}}},
			inspector: fakeQueryInspector{
				explainListDir: func(context.Context, string, string, int64, int64) (string, error) {
					return explainPruningOutput, nil
				},
				explainStatPath: func(context.Context, string, string) (string, error) {
					return "mount_path partition pruning", nil
				},
			},
			dir: "/mnt/test/project/",
		}

		err := verifyPlans(qctx, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, ErrExplainMissingIndex.Error())
		So(err.Error(), ShouldContainSubstring, "mount_path partition pruning")
	})

	Convey("verifyPlans accepts ExplainStatPath when pruning is present", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
				"／mnt／test": time.Now(),
			}}},
			client: &fakeQueryClient{rows: []QueryRow{{Path: "/mnt/test/project/file.txt"}}},
			inspector: fakeQueryInspector{
				explainListDir: func(context.Context, string, string, int64, int64) (string, error) {
					return explainPruningOutput, nil
				},
				explainStatPath: func(context.Context, string, string) (string, error) {
					return explainPruningOutput, nil
				},
			},
			dir: "/mnt/test/project/",
		}

		So(verifyPlans(qctx, func(string, ...any) {}), ShouldBeNil)
	})
}

type fakeQueryClient struct {
	rows      []QueryRow
	rowsByDir map[string][]QueryRow
	closeHook func() error
}

func (c *fakeQueryClient) ListDir(
	_ context.Context,
	dir string,
	_ int64,
) ([]QueryRow, error) {
	if c.rowsByDir != nil {
		return c.rowsByDir[dir], nil
	}

	return c.rows, nil
}

func (*fakeQueryClient) StatPath(context.Context, string) error { return nil }

func (*fakeQueryClient) PermissionAnyInDir(
	context.Context,
	string,
	uint32,
	[]uint32,
) error {
	return nil
}

func (*fakeQueryClient) FindByGlob(
	context.Context,
	[]string,
	[]string,
	bool,
	uint32,
	[]uint32,
) error {
	return nil
}

func (c *fakeQueryClient) Close() error {
	if c.closeHook != nil {
		return c.closeHook()
	}

	return nil
}

func TestBuildOps(t *testing.T) {
	Convey("buildOps reports DiskTree endpoint and tree_where operations", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Splits: 2}, func(string, ...any) {})
		names := make([]string, 0, len(ops))

		var whereInputs map[string]any

		for _, op := range ops {
			names = append(names, op.name)
			if op.name == queryOpTreeWhereName {
				whereInputs = op.inputs
			}
		}

		So(names, ShouldContain, queryOpTreeDiskTreeEndName)
		So(names, ShouldContain, queryOpTreeWhereName)
		So(whereInputs, ShouldNotBeNil)
		So(whereInputs[queryInputDirKey], ShouldEqual, queryOpTestRootDir)
		So(whereInputs["splits"], ShouldEqual, 2)
	})

	Convey("buildOps reports cold/new directory and fresh-provider tree coverage", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{
				tree: db.NewTree(newQueryOpTestDB()),
				bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
					mountpath.EncodeKey(queryOpTestChildADir): time.Unix(1, 0),
					mountpath.EncodeKey(queryTestNFSTeamPath): time.Unix(2, 0),
				}},
			},
			client: &fakeQueryClient{rowsByDir: map[string][]QueryRow{
				queryOpTestRootDir: {
					{Path: queryOpTestChildADir, EntryType: 'd'},
					{Path: queryOpTestChildBDir, EntryType: 'd'},
				},
			}},
			dir: queryOpTestRootDir,
			openProvider: func() (provider.Provider, error) {
				return fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())}, nil
			},
		}

		ops := buildOps(qctx, QueryOptions{
			Repeat:        5,
			Splits:        2,
			WalkDepth:     1,
			WalkLimit:     2,
			AncestorLimit: 5,
		}, func(string, ...any) {})
		names := make([]string, 0, len(ops))

		for _, op := range ops {
			names = append(names, op.name)
		}

		coldCachedWhereOp := findQueryTestOp(ops, "tree_where_cold_then_cached")
		So(coldCachedWhereOp, ShouldNotBeNil)
		So(coldCachedWhereOp.inputs[queryInputDirKey], ShouldEqual, queryOpTestRootDir)
		So(coldCachedWhereOp.inputs["cache_scope"], ShouldEqual, "same_provider_cold_then_warm")
		So(coldCachedWhereOp.inputs["duration_source"], ShouldEqual, "wall")
		So(coldCachedWhereOp.inputs["splits"], ShouldEqual, 2)
		So(coldCachedWhereOp.skipWarmup, ShouldBeTrue)
		So(coldCachedWhereOp.useWallTime, ShouldBeTrue)

		So(names, ShouldContain, "tree_disktree_endpoint_new_dirs")
		So(names, ShouldContain, queryOpTreeDiskTreeVisibleChildName)
		So(names, ShouldContain, queryOpTreeDiskTreeAncName)
		So(names, ShouldContain, "tree_where_cold_then_cached")
		So(names, ShouldContain, "tree_where_fresh_provider")
		So(queryTestOpIndex(names, "tree_where_cold_then_cached"), ShouldBeLessThan,
			queryTestOpIndex(names, "tree_disktree_endpoint_new_dirs"))
		So(queryTestOpIndex(names, "tree_where_cold_then_cached"), ShouldBeLessThan,
			queryTestOpIndex(names, queryOpTreeDirInfoName))
		So(queryTestOpIndex(names, "tree_where_cold_then_cached"), ShouldBeLessThan,
			queryTestOpIndex(names, queryOpTreeDiskTreeEndName))
		So(queryTestOpIndex(names, "tree_where_cold_then_cached"), ShouldBeLessThan,
			queryTestOpIndex(names, queryOpTreeWhereName))

		newDirsOp := findQueryTestOp(ops, "tree_disktree_endpoint_new_dirs")
		So(newDirsOp, ShouldNotBeNil)
		So(newDirsOp.inputs["start_dir"], ShouldEqual, queryOpTestRootDir)
		So(newDirsOp.inputs["dirs"], ShouldResemble, []string{queryOpTestChildADir, queryOpTestChildBDir})
		So(newDirsOp.inputs["cache_scope"], ShouldEqual, "new_directory_each_repeat")
		So(newDirsOp.inputs["duration_source"], ShouldEqual, "wall")
		So(newDirsOp.skipWarmup, ShouldBeTrue)
		So(newDirsOp.repeatOverride, ShouldEqual, 2)

		visibleChildDirsOp := findQueryTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		So(visibleChildDirsOp, ShouldNotBeNil)
		So(visibleChildDirsOp.inputs["parent_dir"], ShouldEqual, queryOpTestRootDir)
		So(visibleChildDirsOp.inputs["cache_scope"], ShouldEqual, queryScopeVisibleChildDirs)
		So(visibleChildDirsOp.inputs["duration_source"], ShouldEqual, "wall")
		So(visibleChildDirsOp.inputs[queryInputAgeKey], ShouldEqual, int(db.DGUTAgeAll))
		So(visibleChildDirsOp.skipWarmup, ShouldBeTrue)
		So(visibleChildDirsOp.useWallTime, ShouldBeTrue)

		ancestorOp := findQueryTestOp(ops, queryOpTreeDiskTreeAncName)
		So(ancestorOp, ShouldNotBeNil)
		So(ancestorOp.inputs["start_dir"], ShouldEqual, "/")
		So(ancestorOp.inputs["dirs"], ShouldResemble, []string{
			"/",
			"/nfs/",
			queryTestNFSTeamPath,
			queryOpTestRootDir,
			queryOpTestChildADir,
		})
		So(ancestorOp.inputs["cache_scope"], ShouldEqual, queryScopeAncestorDirs)
		So(ancestorOp.inputs["duration_source"], ShouldEqual, "wall")
		So(ancestorOp.skipWarmup, ShouldBeTrue)
		So(ancestorOp.repeatOverride, ShouldEqual, 5)

		freshWhereOp := findQueryTestOp(ops, "tree_where_fresh_provider")
		So(freshWhereOp, ShouldNotBeNil)
		So(freshWhereOp.inputs[queryInputDirKey], ShouldEqual, queryOpTestRootDir)
		So(freshWhereOp.inputs["cache_scope"], ShouldEqual, "fresh_provider_per_repeat")
		So(freshWhereOp.inputs["duration_source"], ShouldEqual, "wall")
		So(freshWhereOp.skipWarmup, ShouldBeTrue)
		So(freshWhereOp.useWallTime, ShouldBeTrue)
	})

	Convey("tree_disktree_endpoint_new_dirs times walked dirs instead of the selected warm dir", t, func() {
		database := newQueryOpTestDB()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client: &fakeQueryClient{rowsByDir: map[string][]QueryRow{
				queryOpTestRootDir: {
					{Path: queryOpTestChildADir, EntryType: 'd'},
					{Path: queryOpTestChildBDir, EntryType: 'd'},
				},
			}},
			dir: queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Repeat: 2, WalkDepth: 1, WalkLimit: 2}, func(string, ...any) {})
		newDirsOp := findQueryTestOp(ops, "tree_disktree_endpoint_new_dirs")

		So(newDirsOp, ShouldNotBeNil)
		So(newDirsOp.run(context.Background()), ShouldBeNil)
		So(newDirsOp.run(context.Background()), ShouldBeNil)
		So(database.childrenCalls, ShouldNotContain, queryOpTestRootDir)
		So(database.childrenCalls, ShouldContain, queryOpTestChildADir)
		So(database.childrenCalls, ShouldContain, queryOpTestChildBDir)
	})

	Convey("tree_disktree_endpoint_new_dirs skips ancestors that would warm descendants", t, func() {
		database := newQueryOpTestDB()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client: &fakeQueryClient{rowsByDir: map[string][]QueryRow{
				queryOpTestRootDir: {
					{Path: queryOpTestChildADir, EntryType: 'd'},
					{Path: queryOpTestChildBDir, EntryType: 'd'},
				},
				queryOpTestChildADir: {
					{Path: queryOpTestGrandDir, EntryType: 'd'},
				},
			}},
			dir: queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Repeat: 3, WalkDepth: 2, WalkLimit: 3}, func(string, ...any) {})
		newDirsOp := findQueryTestOp(ops, "tree_disktree_endpoint_new_dirs")
		So(newDirsOp, ShouldNotBeNil)
		So(newDirsOp.inputs["dirs"], ShouldResemble, []string{
			queryOpTestChildBDir,
			queryOpTestGrandDir,
		})
		So(newDirsOp.repeatOverride, ShouldEqual, 2)

		So(newDirsOp.run(context.Background()), ShouldBeNil)
		So(newDirsOp.run(context.Background()), ShouldBeNil)
		So(database.childrenCalls, ShouldNotContain, queryOpTestRootDir)
		So(database.childrenCalls, ShouldNotContain, queryOpTestChildADir)
		So(database.childrenCalls, ShouldContain, queryOpTestChildBDir)
		So(database.childrenCalls, ShouldContain, queryOpTestGrandDir)
		So(database.dirInfoCalls, ShouldNotContain, queryOpTestRootDir)
		So(database.dirInfoCalls, ShouldNotContain, queryOpTestChildADir)
	})

	Convey("tree_disktree_endpoint_visible_child_dirs loads parent before timing visible children", t, func() {
		database := newQueryOpTestDB()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Repeat: 5}, func(string, ...any) {})
		visibleChildDirsOp := findQueryTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		report := boltperf.NewReport("clickhouse", "", 5, 3)

		So(visibleChildDirsOp, ShouldNotBeNil)
		So(runOp(
			&report,
			qctx,
			*visibleChildDirsOp,
			QueryOptions{Repeat: 5, Warmup: 3},
			func(string, ...any) {},
		), ShouldBeNil)

		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Name, ShouldEqual, queryOpTreeDiskTreeVisibleChildName)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 2)
		So(report.Operations[0].Inputs["parent_dir"], ShouldEqual, queryOpTestRootDir)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{
			queryOpTestChildADir,
			queryOpTestChildBDir,
		})
		So(report.Operations[0].Inputs["child_count"], ShouldEqual, 2)
		So(report.Operations[0].Inputs["cache_scope"], ShouldEqual, queryScopeVisibleChildDirs)
		So(report.Operations[0].Inputs["duration_source"], ShouldEqual, "wall")
		So(report.Operations[0].Inputs[queryInputAgeKey], ShouldEqual, int(db.DGUTAgeAll))
		So(countQueryTestDir(database.dirInfoCalls, queryOpTestRootDir), ShouldEqual, 1)
		So(database.dirInfoCalls, ShouldContain, queryOpTestChildADir)
		So(database.dirInfoCalls, ShouldContain, queryOpTestChildBDir)
	})

	Convey("tree_disktree_endpoint_visible_child_dirs falls back to the parent when it has no children", t, func() {
		database := newQueryOpTestDB()
		database.children[queryOpTestRootDir] = nil
		database.summaries = map[string]*db.DirSummary{
			queryOpTestRootDir: {Count: 1},
		}
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Repeat: 5}, func(string, ...any) {})
		visibleChildDirsOp := findQueryTestOp(ops, queryOpTreeDiskTreeVisibleChildName)
		report := boltperf.NewReport("clickhouse", "", 5, 0)

		So(visibleChildDirsOp, ShouldNotBeNil)
		So(runOp(
			&report,
			qctx,
			*visibleChildDirsOp,
			QueryOptions{Repeat: 5},
			func(string, ...any) {},
		), ShouldBeNil)

		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 1)
		So(report.Operations[0].Inputs["child_dirs"], ShouldResemble, []string{queryOpTestRootDir})
		So(report.Operations[0].Inputs["child_count"], ShouldEqual, 1)
		So(report.Operations[0].Inputs["fallback_to_parent_dir"], ShouldBeTrue)
	})

	Convey("tree_where_fresh_provider opens and closes a provider for each run", t, func() {
		var (
			openCalls  int
			closeCalls int
		)

		qctx := queryContext{
			dir: queryOpTestRootDir,
			openProvider: func() (provider.Provider, error) {
				openCalls++

				return fakeMountTimestampsProvider{
					tree: db.NewTree(newQueryOpTestDB()),
					closeHook: func() error {
						closeCalls++

						return nil
					},
				}, nil
			},
		}

		o := opTreeWhereFreshProvider(qctx, 1)

		So(o.run(context.Background()), ShouldBeNil)
		So(o.run(context.Background()), ShouldBeNil)
		So(openCalls, ShouldEqual, 2)
		So(closeCalls, ShouldEqual, 2)
	})

	Convey("tree_disktree_endpoint checks all child has_children values via Tree fallback", t, func() {
		database := newQueryOpTestDB()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			dir:      queryOpTestRootDir,
		}

		err := opTreeDiskTreeEndpoint(qctx).run(context.Background())

		So(err, ShouldBeNil)
		So(database.childrenCalls, ShouldResemble, []string{
			queryOpTestRootDir,
			queryOpTestChildADir,
			queryOpTestChildBDir,
		})
	})
}

func newQueryOpTestDB() *queryOpTestDB {
	return &queryOpTestDB{
		children: map[string][]string{
			queryOpTestRootDir:   {queryOpTestChildADir, queryOpTestChildBDir},
			queryOpTestChildADir: {queryOpTestGrandDir},
			queryOpTestChildBDir: {},
		},
		summaries: map[string]*db.DirSummary{
			queryOpTestRootDir:   {Count: 3},
			queryOpTestChildADir: {Count: 2},
			queryOpTestChildBDir: {Count: 1},
			queryOpTestGrandDir:  {Count: 1},
		},
	}
}

func findQueryTestOp(ops []op, name string) *op {
	for i := range ops {
		if ops[i].name == name {
			return &ops[i]
		}
	}

	return nil
}

func queryTestOpIndex(names []string, name string) int {
	for i, candidate := range names {
		if candidate == name {
			return i
		}
	}

	return -1
}

func countQueryTestDir(dirs []string, target string) int {
	count := 0

	for _, dir := range dirs {
		if dir == target {
			count++
		}
	}

	return count
}

type fakeQueryAPI struct {
	provider  provider.Provider
	client    QueryClient
	inspector QueryInspector
}

func (a fakeQueryAPI) OpenProvider() (provider.Provider, error) {
	return a.provider, nil
}

func (a fakeQueryAPI) NewQueryClient() (QueryClient, error) {
	return a.client, nil
}

func (a fakeQueryAPI) NewQueryInspector() (QueryInspector, error) {
	return a.inspector, nil
}

type fakeMountTimestampsReader struct {
	basedirs.Reader

	mountTimestamps    map[string]time.Time
	mountTimestampsErr error
}

func (r fakeMountTimestampsReader) MountTimestamps() (map[string]time.Time, error) {
	return r.mountTimestamps, r.mountTimestampsErr
}

type fakeMountTimestampsProvider struct {
	provider.Provider

	bd        basedirs.Reader
	tree      *db.Tree
	closeHook func() error
}

func (p fakeMountTimestampsProvider) Tree() *db.Tree {
	return p.tree
}

func (p fakeMountTimestampsProvider) BaseDirs() basedirs.Reader {
	return p.bd
}

func (p fakeMountTimestampsProvider) Close() error {
	if p.closeHook != nil {
		return p.closeHook()
	}

	return nil
}

type queryOpTestDB struct {
	children      map[string][]string
	summaries     map[string]*db.DirSummary
	dirInfoCalls  []string
	childrenCalls []string
}

func (d *queryOpTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	d.dirInfoCalls = append(d.dirInfoCalls, dir)

	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	cp := *summary

	return &cp, nil
}

func (d *queryOpTestDB) Children(dir string) ([]string, error) {
	d.childrenCalls = append(d.childrenCalls, dir)

	return d.children[dir], nil
}

func (d *queryOpTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *queryOpTestDB) Close() error {
	return nil
}
