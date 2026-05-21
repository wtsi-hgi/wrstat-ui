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
	closeHook func() error
}

func (c *fakeQueryClient) ListDir(
	context.Context,
	string,
	int64,
) ([]QueryRow, error) {
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
	childrenCalls []string
}

func (d *queryOpTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
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
