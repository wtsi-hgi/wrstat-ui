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
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

var (
	errQueryTestMeasure = errors.New("measure failed")
	errQueryTestRun     = errors.New("query failed")
)

const (
	explainPruningOutput   = "mount_path partition pruning\ndir_id key condition"
	queryTestExplainDir    = "/mnt/test/project/"
	queryTestExplainFile   = queryTestExplainDir + "file.txt"
	queryTestExplainMount  = "／mnt／test"
	queryTestNFSTeamPath   = "/nfs/team/"
	queryOpTestChildADir   = "/root/a/"
	queryOpTestChildBDir   = "/root/b/"
	queryOpTestGrandDir    = "/root/a/grand/"
	queryOpTestRootDir     = "/root/"
	queryTestBamExt        = "bam"
	queryTestE2CacheHitKey = "active_prefix_summary:path=/nfs/t283_imaging/;filter=ft:32768;" +
		"active_set_id=e2;query_version=1"
	queryTestNoMatchGID = 404
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
		explain := "ReadFromMergeTree\n  Indexes:\n    mount_path partition pruning\n    dir_id key condition"
		So(ExplainHasPruning(explain), ShouldBeTrue)
	})

	Convey("ExplainHasPruning returns false when mount_path is missing", t, func() {
		explain := "ReadFromMergeTree\n  dir_id key condition"
		So(ExplainHasPruning(explain), ShouldBeFalse)
	})

	Convey("ExplainHasPruning returns false when dir_id is missing", t, func() {
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

func TestE2CacheHitKeyScope(t *testing.T) {
	Convey("E2.3 scoped cache hit keys include path, filter, active set id, and query version", t, func() {
		keys := []string{
			queryTestE2CacheHitKey,
		}

		So(cacheHitKeysHaveE2Scope(keys), ShouldBeTrue)
		So(cacheHitKeysHaveE2Scope([]string{
			"active_prefix_summary:path=/nfs/t283_imaging/;filter=ft:32768;query_version=1",
		}), ShouldBeFalse)
		So(cacheHitKeysHaveE2Scope([]string{
			"active_prefix_summary:path=/nfs/t283_imaging/;active_set_id=e2;query_version=1",
		}), ShouldBeFalse)
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
		if gid == queryTestNoMatchGID {
			return true
		}
	}

	return false
}

func (*noMatchFilteredTreeDB) Children(string) ([]string, error) {
	return []string{queryOpTestChildADir}, nil
}

func (*noMatchFilteredTreeDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (*noMatchFilteredTreeDB) Close() error {
	return nil
}

func TestPickDir(t *testing.T) {
	Convey("pickDir stops at the current directory when a filter has no matching rows", t, func() {
		database := &noMatchFilteredTreeDB{}
		filter := &db.Filter{GIDs: []uint32{queryTestNoMatchGID}, Age: db.DGUTAgeAll}

		dir := pickDir(db.NewTree(database), queryOpTestRootDir, filter)

		So(dir, ShouldEqual, queryOpTestRootDir)
		So(database.dirInfoCalls, ShouldResemble, []string{queryOpTestRootDir})
	})
}

type fakeQueryInspector struct {
	explainFindByGlob func(ctx context.Context, baseDirs, patterns []string) (string, error)
	explainListDir    func(ctx context.Context, mountPath, dir string, limit, offset int64) (string, error)
	explainStatPath   func(ctx context.Context, mountPath, path string) (string, error)
	measure           func(ctx context.Context, run func(ctx context.Context) error) (*QueryMetrics, error)
	closeFn           func() error
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

func (f fakeQueryInspector) ExplainFindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
) (string, error) {
	if f.explainFindByGlob != nil {
		return f.explainFindByGlob(ctx, baseDirs, patterns)
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
				name:   queryOpFilesListDirName,
				inputs: map[string]any{queryInputDirKey: "/tmp/"},
				run:    func(context.Context) error { return nil },
			},
			QueryOptions{Repeat: 3},
			func(string, ...any) {},
		)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errQueryTestMeasure.Error())
		So(err.Error(), ShouldContainSubstring, queryOpFilesListDirName)
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

					return &QueryMetrics{
						DurationMs:  12,
						ReadRows:    34,
						ReadBytes:   56,
						ReadMarks:   7,
						MemoryBytes: 89,
						ResultRows:  1,
					}, nil
				},
			}},
			op{name: "mount_timestamps", inputs: map[string]any{}, run: func(context.Context) error { return nil }},
			QueryOptions{Repeat: 2},
			func(format string, args ...any) { _, _ = out.WriteString(strings.TrimSpace(format)) },
		)

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].DurationsMS, ShouldResemble, []float64{12, 12})
		So(report.Operations[0].ReadRows, ShouldResemble, []uint64{34, 34})
		So(report.Operations[0].ReadBytes, ShouldResemble, []uint64{56, 56})
		So(report.Operations[0].ReadMarks, ShouldResemble, []uint64{7, 7})
		So(report.Operations[0].MemoryBytes, ShouldResemble, []uint64{89, 89})
		So(report.Operations[0].ResultCount, ShouldResemble, []uint64{1, 1})
		So(out.String(), ShouldContainSubstring, "metrics")
		So(out.String(), ShouldContainSubstring, "memory_bytes=%d")
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

	Convey("runOp runs setup and teardown around each timed repeat", t, func() {
		var calls []string

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
				name:        "tree_where_provider_update_cold_cache",
				inputs:      map[string]any{},
				useWallTime: true,
				setup: func(context.Context) error {
					calls = append(calls, "setup")

					return nil
				},
				run: func(context.Context) error {
					calls = append(calls, "run")

					return nil
				},
				teardown: func(context.Context) error {
					calls = append(calls, "teardown")

					return nil
				},
			},
			QueryOptions{Repeat: 2},
			func(string, ...any) {},
		)

		So(err, ShouldBeNil)
		So(calls, ShouldResemble, []string{
			"setup", "run", "teardown",
			"setup", "run", "teardown",
		})
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

	Convey("runSuite records result counts for general final-gate operations", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client: &fakeQueryClient{rows: []QueryRow{{
				Path:      queryOpTestRootDir + "sample.bam",
				Ext:       queryTestBamExt,
				EntryType: 'f',
			}}},
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
			Ops: []string{
				queryOpTreeWhereName,
				queryOpTreeDiskTreeEndName,
				queryOpTreeDirInfoName,
				queryOpFilesListDirName,
				queryOpFilesStatPathName,
				"glob_case_E",
			},
			Splits: 1,
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 6)
		So(resultCountsByName(report)[queryOpTreeWhereName], ShouldResemble, []uint64{3})
		So(resultCountsByName(report)[queryOpTreeDiskTreeEndName], ShouldResemble, []uint64{2})
		So(resultCountsByName(report)[queryOpTreeDirInfoName], ShouldResemble, []uint64{3})
		So(resultCountsByName(report)[queryOpFilesListDirName], ShouldResemble, []uint64{1})
		So(resultCountsByName(report)[queryOpFilesStatPathName], ShouldResemble, []uint64{1})
		So(resultCountsByName(report)["glob_case_E"], ShouldResemble, []uint64{2})
		So(operationByName(report, queryOpTreeDiskTreeEndName).Inputs[queryInputHighFanoutChildCount], ShouldEqual, uint64(2))
		So(operationByName(report, queryOpTreeWhereName).Inputs[queryInputResultDigest], ShouldNotBeBlank)
		So(operationByName(report, queryOpTreeDirInfoName).Inputs[queryInputResultDigest], ShouldNotBeBlank)
	})

	Convey("D2.6 permission_check records directory and path permission checks", t, func() {
		client := &fakeQueryClient{rows: []QueryRow{{
			Path:      queryOpTestRootDir + "sample.bam",
			Ext:       queryTestBamExt,
			EntryType: 'f',
		}}}
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client:   client,
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 2, ReadRows: 3, ReadBytes: 4, ReadMarks: 5}, nil
				},
			},
			dir:  queryOpTestRootDir,
			uid:  20155,
			gids: []uint32{14976},
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{queryOpPermissionCheckName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		op := report.Operations[0]
		So(op.Name, ShouldEqual, queryOpPermissionCheckName)
		So(op.ResultCount, ShouldResemble, []uint64{2})
		So(op.ReadRows, ShouldResemble, []uint64{3})
		So(op.ReadBytes, ShouldResemble, []uint64{4})
		So(op.ReadMarks, ShouldResemble, []uint64{5})
		So(op.Inputs[queryInputPermissionChecksKey], ShouldResemble, []string{
			queryPermissionCheckAnyInDir,
			queryPermissionCheckPath,
		})
		So(op.Inputs[queryInputPermissionPathKey], ShouldEqual, queryOpTestRootDir+"sample.bam")
		So(client.permissionAnyCalls, ShouldResemble, []permissionAnyCall{{
			dir:  queryOpTestRootDir,
			uid:  20155,
			gids: []uint32{14976},
		}})
		So(client.permissionPathCalls, ShouldResemble, []permissionPathCall{{
			path: queryOpTestRootDir + "sample.bam",
			uid:  20155,
			gids: []uint32{14976},
		}})
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

	Convey("runSuite can select focused final-gate operations with counts and query counters", t, func() {
		database := newQueryOpTestDB()
		database.children["/"] = []string{queryOpTestRootDir}
		database.summaries["/"] = &db.DirSummary{Count: 3}
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{
				tree: db.NewTree(database),
				bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
					mountpath.EncodeKey(queryOpTestRootDir): time.Unix(1, 0),
				}},
			},
			client: &fakeQueryClient{rows: []QueryRow{{
				Path:      queryOpTestRootDir + ".hidden.bam",
				Ext:       queryTestBamExt,
				EntryType: 'f',
			}}},
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{
						DurationMs: 3,
						ReadRows:   4,
						ReadBytes:  5,
						ReadMarks:  6,
					}, nil
				},
			},
			dir:        queryOpTestRootDir,
			treeFilter: queryTestTreeFilter(),
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)
		focusedOps := []string{
			queryOpDirInfoBroadName,
			queryOpDirInfoFilteredName,
			queryOpDirInfosBroadName,
			queryOpDirInfosFilteredName,
			queryOpDirsHaveChildrenBroadName,
			queryOpDirsHaveChildrenFilteredName,
			queryOpWhereWholeMountName,
			queryOpWhereFilteredWholeMountName,
			queryOpVirtualChildrenName,
			queryOpVirtualDirInfoName,
			queryOpFindGlobExtensionDotfileName,
		}

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    focusedOps,
			Splits: 1,
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, len(focusedOps)+len(finalGateJ6D4RequiredPatterns()))

		names := make([]string, 0, len(focusedOps))

		for _, op := range report.Operations {
			if op.Name == finalGateJ6D4DecisionOpName {
				continue
			}

			names = append(names, op.Name)
			So(op.DurationsMS, ShouldResemble, []float64{3})
			So(op.P50MS, ShouldEqual, float64(3))
			So(op.P95MS, ShouldEqual, float64(3))
			So(op.P99MS, ShouldEqual, float64(3))
			So(op.ReadRows, ShouldResemble, []uint64{4})
			So(op.ReadBytes, ShouldResemble, []uint64{5})
			So(op.ReadMarks, ShouldResemble, []uint64{6})
			So(op.ResultCount, ShouldHaveLength, 1)
			So(op.ResultCount[0], ShouldBeGreaterThanOrEqualTo, uint64(0))
		}

		for _, name := range focusedOps {
			So(names, ShouldContain, name)
		}

		decisions := queryTestD4Decisions(report)
		So(decisions, ShouldHaveLength, len(finalGateJ6D4RequiredPatterns()))

		for _, pattern := range finalGateJ6D4RequiredPatterns() {
			decision, ok := finalGateJ6D4DecisionForPattern(decisions, pattern)
			So(ok, ShouldBeTrue)
			So(decision.Decision, ShouldEqual, finalGateJ6D4DecisionRetained)
			So(decision.Citation, ShouldNotBeBlank)
			So(decision.MeasuredP95MS, ShouldEqual, float64(3))
			So(decision.LatencyGateMS, ShouldEqual, float64(finalGateJ6ColdUXBroadMaxMS))
		}

		expectedMeasuredOps := map[string][]string{
			finalGateJ6D4PatternFilteredExact: {
				queryOpDirInfoFilteredName,
			},
			finalGateJ6D4PatternFilteredChildren: {
				queryOpDirInfosFilteredName,
				queryOpDirsHaveChildrenFilteredName,
			},
			finalGateJ6D4PatternFilteredSubtree: {
				queryOpWhereFilteredWholeMountName,
			},
		}

		for _, op := range report.Operations {
			if op.Name != finalGateJ6D4DecisionOpName {
				continue
			}

			pattern := stringInput(op.Inputs, finalGateJ6D4PatternInput)
			measuredOps, ok := op.Inputs["measured_operations"].([]string)
			So(ok, ShouldBeTrue)
			So(measuredOps, ShouldResemble, expectedMeasuredOps[pattern])
			So(op.Inputs[finalGateJ6D4CitationInput], ShouldEqual,
				"query_report:"+strings.Join(expectedMeasuredOps[pattern], ","))
		}
	})

	Convey("D1.6 runSuite records info counts and query counters for final perf reports", t, func() {
		database := newQueryOpTestDB()
		database.info = &db.Info{
			NumDirs:     3,
			NumDGUTAs:   6,
			NumParents:  1,
			NumChildren: 2,
		}
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{
						DurationMs: 3,
						ReadRows:   4,
						ReadBytes:  5,
						ReadMarks:  6,
					}, nil
				},
			},
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops:    []string{"info"},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(database.infoCalls, ShouldEqual, 1)
		So(report.Operations, ShouldHaveLength, 1)
		op := report.Operations[0]
		So(op.Name, ShouldEqual, "info")
		So(op.DurationsMS, ShouldResemble, []float64{3})
		So(op.ReadRows, ShouldResemble, []uint64{4})
		So(op.ReadBytes, ShouldResemble, []uint64{5})
		So(op.ReadMarks, ShouldResemble, []uint64{6})
		So(op.ResultCount, ShouldResemble, []uint64{3, 6, 1, 2})
		So(op.Inputs["count_fields"], ShouldResemble, []string{
			"num_dirs",
			"num_dgutas",
			"num_parents",
			"num_children",
		})
	})

	Convey("D2.6 runSuite records auth tree and auth/no-auth where correctness metadata", t, func() {
		database := newQueryOpTestDB()
		database.summaries[queryOpTestRootDir].GIDs = []uint32{7, 8}
		database.summaries[queryOpTestChildADir].GIDs = []uint32{7}
		database.summaries[queryOpTestChildBDir].GIDs = []uint32{8}
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			inspector: fakeQueryInspector{
				measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
					if err := run(ctx); err != nil {
						return nil, err
					}

					return &QueryMetrics{DurationMs: 3, ReadRows: 4, ReadBytes: 5, ReadMarks: 6}, nil
				},
			},
			dir:        queryOpTestRootDir,
			gids:       []uint32{7},
			treeFilter: &db.Filter{FT: db.DGUTAFileTypeOther, Age: db.DGUTAgeAll},
		}
		report := boltperf.NewReport("clickhouse", "", 1, 0)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 1,
			Ops: []string{
				queryOpAuthTreeName,
				queryOpAuthWhereRestrictedName,
				queryOpNoAuthWhereName,
			},
			Splits: 1,
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 3)

		authTree := report.Operations[0]
		So(authTree.Name, ShouldEqual, queryOpAuthTreeName)
		So(authTree.Inputs[queryInputAllowedGIDsKey], ShouldResemble, []uint32{7})
		So(authTree.Inputs[queryInputStatusCodeKey], ShouldEqual, 200)
		So(authTree.Inputs[queryInputResultDigest], ShouldNotBeBlank)
		So(authTree.Inputs[queryInputNoAuthFlagsKey], ShouldResemble, map[string]bool{
			queryOpTestRootDir:   false,
			queryOpTestChildADir: false,
			queryOpTestChildBDir: true,
		})

		restrictedWhere := report.Operations[1]
		So(restrictedWhere.Name, ShouldEqual, queryOpAuthWhereRestrictedName)
		So(restrictedWhere.Inputs[queryInputFilterGIDsKey], ShouldResemble, []uint32{7})
		So(restrictedWhere.Inputs[queryInputResultDigest], ShouldNotBeBlank)
		So(restrictedWhere.ResultCount, ShouldResemble, []uint64{3})

		noAuthWhere := report.Operations[2]
		So(noAuthWhere.Name, ShouldEqual, queryOpNoAuthWhereName)
		So(noAuthWhere.Inputs[queryInputFilterGIDsKey], ShouldBeEmpty)
		So(noAuthWhere.Inputs[queryInputResultDigest], ShouldNotBeBlank)
		So(noAuthWhere.ResultCount, ShouldResemble, []uint64{3})
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

func resultCountsByName(report boltperf.Report) map[string][]uint64 {
	counts := make(map[string][]uint64, len(report.Operations))
	for _, op := range report.Operations {
		counts[op.Name] = op.ResultCount
	}

	return counts
}

func operationByName(report boltperf.Report, name string) perfreport.Operation {
	for _, op := range report.Operations {
		if op.Name == name {
			return op
		}
	}

	return perfreport.Operation{}
}

func queryTestTreeFilter() *db.Filter {
	return &db.Filter{
		GIDs: []uint32{7, 8},
		UIDs: []uint32{9, 10},
		FT:   db.DGUTAFileTypeBam | db.DGUTAFileTypeCram,
		Age:  db.DGUTAgeAll,
	}
}

func queryTestD4Decisions(report boltperf.Report) []FinalGateD4DecisionEvidence {
	decisions := make([]FinalGateD4DecisionEvidence, 0)

	for _, op := range report.Operations {
		if op.Name == finalGateJ6D4DecisionOpName {
			decisions = append(decisions, finalGateJ6D4DecisionFromOperation(op))
		}
	}

	return decisions
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
				queryTestExplainMount: time.Now(),
			}}},
			client: &fakeQueryClient{rows: []QueryRow{{Path: queryTestExplainFile}}},
			inspector: fakeQueryInspector{
				explainListDir: func(context.Context, string, string, int64, int64) (string, error) {
					return explainPruningOutput, nil
				},
				explainStatPath: func(context.Context, string, string) (string, error) {
					return "mount_path partition pruning", nil
				},
				explainFindByGlob: func(context.Context, []string, []string) (string, error) {
					return f3GlobCatalogProofExplain, nil
				},
			},
			dir: queryTestExplainDir,
		}

		err := verifyPlans(qctx, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, ErrExplainMissingIndex.Error())
		So(err.Error(), ShouldContainSubstring, "mount_path partition pruning")
	})

	Convey("verifyPlans accepts ExplainStatPath when pruning is present", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
				queryTestExplainMount: time.Now(),
			}}},
			client: &fakeQueryClient{rows: []QueryRow{{Path: queryTestExplainFile}}},
			inspector: fakeQueryInspector{
				explainListDir: func(context.Context, string, string, int64, int64) (string, error) {
					return explainPruningOutput, nil
				},
				explainStatPath: func(context.Context, string, string) (string, error) {
					return explainPruningOutput, nil
				},
				explainFindByGlob: func(context.Context, []string, []string) (string, error) {
					return "ReadFromMergeTree wrstat_files match(f.path, ?) dir_id", nil
				},
			},
			dir: queryTestExplainDir,
		}

		err := verifyPlans(qctx, func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, ErrExplainGlobScansFilesPath.Error())
	})

	Convey("verifyPlans accepts full-path glob proof when files are read by dir_id", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{bd: fakeMountTimestampsReader{mountTimestamps: map[string]time.Time{
				queryTestExplainMount: time.Now(),
			}}},
			client: &fakeQueryClient{rows: []QueryRow{{Path: queryTestExplainFile}}},
			inspector: fakeQueryInspector{
				explainListDir: func(context.Context, string, string, int64, int64) (string, error) {
					return explainPruningOutput, nil
				},
				explainStatPath: func(context.Context, string, string) (string, error) {
					return explainPruningOutput, nil
				},
				explainFindByGlob: func(context.Context, []string, []string) (string, error) {
					return f3GlobCatalogProofExplain, nil
				},
			},
			dir: queryTestExplainDir,
		}

		So(verifyPlans(qctx, func(string, ...any) {}), ShouldBeNil)
	})
}

type permissionAnyCall struct {
	dir  string
	uid  uint32
	gids []uint32
}

type permissionPathCall struct {
	path string
	uid  uint32
	gids []uint32
}

type fakeQueryClient struct {
	rows                []QueryRow
	rowsByDir           map[string][]QueryRow
	globRows            []QueryRow
	permissionAnyCalls  []permissionAnyCall
	permissionPathCalls []permissionPathCall
	closeHook           func() error
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

func (*fakeQueryClient) StatPath(_ context.Context, path string) (*QueryRow, error) {
	return &QueryRow{Path: path, EntryType: 'f'}, nil
}

func (*fakeQueryClient) IsDir(context.Context, string) (bool, error) {
	return true, nil
}

func (c *fakeQueryClient) PermissionAnyInDir(
	_ context.Context,
	dir string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	c.permissionAnyCalls = append(c.permissionAnyCalls, permissionAnyCall{
		dir:  dir,
		uid:  uid,
		gids: append([]uint32(nil), gids...),
	})

	return true, nil
}

func (c *fakeQueryClient) PermissionPath(
	_ context.Context,
	path string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	c.permissionPathCalls = append(c.permissionPathCalls, permissionPathCall{
		path: path,
		uid:  uid,
		gids: append([]uint32(nil), gids...),
	})

	return true, nil
}

func (c *fakeQueryClient) FindByGlob(
	_ context.Context,
	_ []string,
	_ []string,
	_ bool,
	_ uint32,
	_ []uint32,
) ([]QueryRow, error) {
	if c.globRows != nil {
		return append([]QueryRow(nil), c.globRows...), nil
	}

	return []QueryRow{
		{Path: queryOpTestRootDir + "glob-a.bam", Ext: queryTestBamExt, EntryType: 'f'},
		{Path: queryOpTestRootDir + "glob-b.bam", Ext: queryTestBamExt, EntryType: 'f'},
	}, nil
}

func (c *fakeQueryClient) CountByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	requireOwner bool,
	uid uint32,
	gids []uint32,
) (int, error) {
	rows, err := c.FindByGlob(ctx, baseDirs, patterns, requireOwner, uid, gids)
	if err != nil {
		return 0, err
	}

	return len(rows), nil
}

func (c *fakeQueryClient) Close() error {
	if c.closeHook != nil {
		return c.closeHook()
	}

	return nil
}

func TestStartupCacheWarmingAudit(t *testing.T) {
	Convey("runSuite reports startup and cache-warming timing metadata", t, func() {
		qctx := queryContext{
			client: &fakeQueryClient{},
			dir:    queryOpTestRootDir,
		}
		report := boltperf.NewReport("clickhouse", "", 5, 3)

		err := runSuite(&report, qctx, QueryOptions{
			Repeat: 5,
			Warmup: 3,
			Ops:    []string{queryOpStartupCacheWarmingAuditName},
		}, func(string, ...any) {})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 1)
		So(report.Operations[0].Name, ShouldEqual, queryOpStartupCacheWarmingAuditName)
		So(report.Operations[0].DurationsMS, ShouldHaveLength, 1)

		inputs := report.Operations[0].Inputs
		So(inputs["initial_provider_readers_timing"], ShouldEqual, queryStartupStageSynchronousInitial)
		So(inputs["server_basedirs_cache_timing"], ShouldEqual, queryStartupStageSynchronousInitial)
		So(inputs["query_cache_warmup_timing"], ShouldEqual, queryStartupStageLazyInteraction)
		So(inputs["provider_polling_timing"], ShouldEqual, queryStartupStageBackgroundProvider)
		So(inputs["provider_update_refresh_timing"], ShouldEqual, queryStartupStageBackgroundProvider)
		So(inputs["filtered_where_gate_source"], ShouldEqual, queryOpTreeWhereColdProviderName)
		So(inputs["filtered_where_gate_cache_scope"], ShouldEqual, queryScopeColdProvider)
		So(inputs["filtered_where_gate_requires_cache_reset"], ShouldBeTrue)
		So(inputs["startup_warming_supports_gate_only"], ShouldBeTrue)
		So(inputs["warmed_request_output_reuse"], ShouldEqual, "forbidden_for_cold_filtered_where_gate")

		providerWork, ok := inputs["initial_provider_work"].([]string)
		So(ok, ShouldBeTrue)
		So(providerWork, ShouldContain, "build_initial_readers")

		serverWork, ok := inputs["server_cache_work"].([]string)
		So(ok, ShouldBeTrue)
		So(serverWork, ShouldContain, "prewarm_basedirs_caches")

		queryWork, ok := inputs["query_cache_work"].([]string)
		So(ok, ShouldBeTrue)
		So(queryWork, ShouldContain, "lazy_query_cache_warmup_from_interactions")

		updateWork, ok := inputs["provider_update_work"].([]string)
		So(ok, ShouldBeTrue)
		So(updateWork, ShouldContain, "server_refresh_provider_from_update_callback")

		timingOps, ok := inputs["timing_ops"].(map[string][]string)
		So(ok, ShouldBeTrue)
		So(timingOps["lazy_query_cache_warmup"], ShouldResemble, []string{
			queryOpTreeWhereColdName,
			queryOpTreeDiskTreeVisibleChildName,
		})
		So(timingOps["cold_provider_startup_replay"], ShouldResemble, []string{
			queryOpTreeWhereColdProviderName,
			queryOpTreeDiskTreeColdProviderName,
		})
		So(timingOps["provider_update_cold_cache"], ShouldResemble, []string{
			queryOpTreeWhereProviderUpdateName,
			queryOpTreeDiskTreeProviderUpdateName,
		})
	})
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

	Convey("tree_where uses the configured tree filter and records it", t, func() {
		database := newQueryOpTestDB()
		filter := queryTestTreeFilter()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{Splits: 2, TreeFilter: filter}, func(string, ...any) {})
		whereOp := findQueryTestOp(ops, queryOpTreeWhereName)
		So(whereOp, ShouldNotBeNil)
		assertQueryTestTreeFilterInputs(whereOp.inputs, filter)

		So(whereOp.run(context.Background()), ShouldBeNil)
		So(database.dirInfoFilters, ShouldNotBeEmpty)
		So(database.dirInfoFilters[0], ShouldResemble, filter)
	})

	Convey("tree_disktree_endpoint uses the configured tree filter and records it", t, func() {
		database := newQueryOpTestDB()
		filter := queryTestTreeFilter()
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(database)},
			client:   &fakeQueryClient{},
			dir:      queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{TreeFilter: filter}, func(string, ...any) {})
		disktreeOp := findQueryTestOp(ops, queryOpTreeDiskTreeEndName)
		So(disktreeOp, ShouldNotBeNil)
		assertQueryTestTreeFilterInputs(disktreeOp.inputs, filter)

		So(disktreeOp.run(context.Background()), ShouldBeNil)
		So(database.dirInfoFilters, ShouldNotBeEmpty)
		So(database.dirInfoFilters[0], ShouldResemble, filter)
	})

	Convey("tree ops mark AgeAll owner/type filters as optional-index gated", t, func() {
		filter := queryTestTreeFilter()
		inputs := treeOpInputs(filter, nil)

		So(inputs[queryInputTreeFilterRouteKey], ShouldEqual, queryTreeFilterRouteOptionalAgeAll)
		So(inputs[queryInputFilterIndexGateKey], ShouldEqual, queryFilterIndexGatePerfRequired)
	})

	Convey("tree ops mark age-specific filters as facts-vector reads", t, func() {
		filter := queryTestTreeFilter()
		filter.Age = db.DGUTAgeM1Y
		inputs := treeOpInputs(filter, nil)

		So(inputs[queryInputTreeFilterRouteKey], ShouldEqual, queryTreeFilterRouteFactsVectors)
		So(inputs[queryInputFilterIndexGateKey], ShouldEqual, queryFilterIndexGateInapplicable)
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
			resetQueryCaches: func() {},
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
		So(names, ShouldContain, queryOpTreeWhereColdProviderName)
		So(names, ShouldContain, queryOpTreeDiskTreeColdProviderName)
		So(names, ShouldContain, queryOpTreeWhereProviderUpdateName)
		So(names, ShouldContain, queryOpTreeDiskTreeProviderUpdateName)
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
		So(newDirsOp.inputs[queryInputStartDirKey], ShouldEqual, queryOpTestRootDir)
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
		So(ancestorOp.inputs[queryInputStartDirKey], ShouldEqual, "/")
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

		coldProviderWhereOp := findQueryTestOp(ops, queryOpTreeWhereColdProviderName)
		So(coldProviderWhereOp, ShouldNotBeNil)
		So(coldProviderWhereOp.inputs["cache_scope"], ShouldEqual, queryScopeColdProvider)
		So(coldProviderWhereOp.inputs["duration_source"], ShouldEqual, "wall")
		So(coldProviderWhereOp.skipWarmup, ShouldBeTrue)
		So(coldProviderWhereOp.useWallTime, ShouldBeTrue)

		updateWhereOp := findQueryTestOp(ops, queryOpTreeWhereProviderUpdateName)
		So(updateWhereOp, ShouldNotBeNil)
		So(updateWhereOp.inputs["cache_scope"], ShouldEqual, queryScopeProviderUpdateCold)
		So(updateWhereOp.inputs["duration_source"], ShouldEqual, "wall")
		So(updateWhereOp.skipWarmup, ShouldBeTrue)
		So(updateWhereOp.useWallTime, ShouldBeTrue)

		coldProviderDiskTreeOp := findQueryTestOp(ops, queryOpTreeDiskTreeColdProviderName)
		So(coldProviderDiskTreeOp, ShouldNotBeNil)
		So(coldProviderDiskTreeOp.inputs["cache_scope"], ShouldEqual, queryScopeColdProvider)
		So(coldProviderDiskTreeOp.inputs["duration_source"], ShouldEqual, "wall")
		So(coldProviderDiskTreeOp.skipWarmup, ShouldBeTrue)
		So(coldProviderDiskTreeOp.useWallTime, ShouldBeTrue)

		updateDiskTreeOp := findQueryTestOp(ops, queryOpTreeDiskTreeProviderUpdateName)
		So(updateDiskTreeOp, ShouldNotBeNil)
		So(updateDiskTreeOp.inputs["cache_scope"], ShouldEqual, queryScopeProviderUpdateCold)
		So(updateDiskTreeOp.inputs["duration_source"], ShouldEqual, "wall")
		So(updateDiskTreeOp.skipWarmup, ShouldBeTrue)
		So(updateDiskTreeOp.useWallTime, ShouldBeTrue)
	})

	Convey("provider-update cold-cache tree ops open providers outside timed runs", t, func() {
		var (
			closeCalls int
			openCalls  int
			resetCalls int
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
			resetQueryCaches: func() {
				resetCalls++
			},
		}

		whereOp := opTreeWhereProviderUpdateColdCache(qctx, 1)
		So(whereOp.setup(context.Background()), ShouldBeNil)
		So(openCalls, ShouldEqual, 1)
		So(resetCalls, ShouldEqual, 1)
		So(whereOp.run(context.Background()), ShouldBeNil)
		So(openCalls, ShouldEqual, 1)
		So(whereOp.teardown(context.Background()), ShouldBeNil)
		So(closeCalls, ShouldEqual, 1)

		diskTreeOp := opTreeDiskTreeProviderUpdateColdCache(qctx)
		So(diskTreeOp.setup(context.Background()), ShouldBeNil)
		So(openCalls, ShouldEqual, 2)
		So(resetCalls, ShouldEqual, 2)
		So(diskTreeOp.run(context.Background()), ShouldBeNil)
		So(openCalls, ShouldEqual, 2)
		So(diskTreeOp.teardown(context.Background()), ShouldBeNil)
		So(closeCalls, ShouldEqual, 2)
	})

	Convey("buildOps records cache metadata for file-level APIs", t, func() {
		qctx := queryContext{
			provider: fakeMountTimestampsProvider{tree: db.NewTree(newQueryOpTestDB())},
			client: &fakeQueryClient{rows: []QueryRow{{
				Path:      queryOpTestRootDir + "file.bam",
				Ext:       queryTestBamExt,
				EntryType: 'f',
			}}},
			dir: queryOpTestRootDir,
		}

		ops := buildOps(qctx, QueryOptions{}, func(string, ...any) {})

		for _, name := range []string{queryOpFilesListDirName, queryOpFilesStatPathName, queryOpGlobCaseAName} {
			fileOp := findQueryTestOp(ops, name)
			So(fileOp, ShouldNotBeNil)
			So(fileOp.inputs[queryInputCacheScope], ShouldEqual, queryScopeSameQueryClient)
			So(fileOp.inputs[queryInputDurationSource], ShouldEqual, querySourceClickHouseLog)
		}
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

func findQueryTestOp(ops []op, name string) *op {
	for i := range ops {
		if ops[i].name == name {
			return &ops[i]
		}
	}

	return nil
}

func assertQueryTestTreeFilterInputs(inputs map[string]any, filter *db.Filter) {
	So(inputs[queryInputAgeKey], ShouldEqual, int(filter.Age))
	So(inputs[queryInputFilterGIDsKey], ShouldResemble, filter.GIDs)
	So(inputs[queryInputFilterUIDsKey], ShouldResemble, filter.UIDs)
	So(inputs[queryInputFilterFileTypeMaskKey], ShouldEqual, int(filter.FT))
	So(inputs[queryInputFilterFileTypesKey], ShouldEqual, filter.FT.String())
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
	children       map[string][]string
	summaries      map[string]*db.DirSummary
	info           *db.Info
	dirInfoCalls   []string
	dirInfoFilters []*db.Filter
	childrenCalls  []string
	infoCalls      int
}

func (d *queryOpTestDB) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	d.dirInfoCalls = append(d.dirInfoCalls, dir)
	d.dirInfoFilters = append(d.dirInfoFilters, cloneQueryTestFilter(filter))

	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	cp := *summary

	return &cp, nil
}

func cloneQueryTestFilter(filter *db.Filter) *db.Filter {
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

func (d *queryOpTestDB) Children(dir string) ([]string, error) {
	d.childrenCalls = append(d.childrenCalls, dir)

	return d.children[dir], nil
}

func (d *queryOpTestDB) Info() (*db.Info, error) {
	d.infoCalls++
	if d.info != nil {
		cp := *d.info

		return &cp, nil
	}

	return &db.Info{}, nil
}

func (d *queryOpTestDB) Close() error {
	return nil
}
