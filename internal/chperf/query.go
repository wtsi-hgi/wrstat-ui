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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

const (
	dirPickMinCount     = 1000
	dirPickMaxCount     = 20000
	dirPickMaxSteps     = 64
	defaultExplainLimit = 1_000_000
)

var (
	// ErrExplainMissingIndex is returned when EXPLAIN output does not
	// mention both mount_path and parent_dir pruning.
	ErrExplainMissingIndex = errors.New(
		"EXPLAIN output does not mention both mount_path and parent_dir pruning",
	)

	// ErrEmptyDir is returned when the selected directory has no files
	// for StatPath testing.
	ErrEmptyDir = errors.New("directory is empty, skipping StatPath")
)

// QueryOptions configures the query timing suite.
type QueryOptions struct {
	Dir    string
	UID    uint32
	GIDs   []uint32
	Repeat int
}

// Query runs a repeatable timing suite against ClickHouse and returns
// a Report with per-query latency percentiles.
func Query(
	api QueryAPI,
	opts QueryOptions,
	printf PrintfFunc,
) (_ boltperf.Report, err error) {
	qctx, err := buildQueryContext(api, opts, printf)
	if err != nil {
		return boltperf.Report{}, err
	}

	defer func() {
		if cerr := qctx.close(); err == nil {
			err = cerr
		}
	}()

	if err := verifyPlans(qctx, printf); err != nil {
		return boltperf.Report{}, err
	}

	report := boltperf.NewReport("clickhouse", "", opts.Repeat, 0)

	if err := runSuite(&report, qctx, opts, printf); err != nil {
		return boltperf.Report{}, err
	}

	return report, nil
}

func buildQueryContext(
	api QueryAPI,
	opts QueryOptions,
	printf PrintfFunc,
) (queryContext, error) {
	p, client, inspector, err := openAll(api)
	if err != nil {
		return queryContext{}, err
	}

	dir, err := selectDir(p, opts.Dir, printf)
	if err != nil {
		_ = inspector.Close()
		_ = client.Close()
		_ = p.Close()

		return queryContext{}, err
	}

	return queryContext{
		provider:  p,
		client:    client,
		inspector: inspector,
		dir:       dir,
		uid:       opts.UID,
		gids:      opts.GIDs,
	}, nil
}

func openAll(
	api QueryAPI,
) (provider.Provider, QueryClient, QueryInspector, error) {
	p, err := api.OpenProvider()
	if err != nil {
		return nil, nil, nil, err
	}

	client, err := api.NewQueryClient()
	if err != nil {
		_ = p.Close()

		return nil, nil, nil, err
	}

	inspector, err := api.NewQueryInspector()
	if err != nil {
		_ = client.Close()
		_ = p.Close()

		return nil, nil, nil, err
	}

	return p, client, inspector, nil
}

func selectDir(
	p provider.Provider,
	explicitDir string,
	printf PrintfFunc,
) (string, error) {
	if d := normaliseDirPath(explicitDir); d != "" {
		printf("query: using dir=%s\n", d)

		return d, nil
	}

	startDir, err := firstMountPath(p.BaseDirs())
	if err != nil {
		return "", err
	}

	dir := pickDir(p.Tree(), startDir)
	printf("query: auto-selected dir=%s\n", dir)

	return dir, nil
}

func normaliseDirPath(dir string) string {
	d := strings.TrimSpace(dir)
	if d == "" {
		return ""
	}

	if !strings.HasPrefix(d, "/") {
		d = "/" + d
	}

	if !strings.HasSuffix(d, "/") {
		d += "/"
	}

	return d
}

func firstMountPath(bd basedirs.Reader) (string, error) {
	mt, err := bd.MountTimestamps()
	if err != nil {
		return "", err
	}

	if len(mt) == 0 {
		return "", fmt.Errorf("%w: no active mounts", ErrNoDatasets)
	}

	paths := DecodeMountPaths(mt)

	return paths[0], nil
}

// DecodeMountPaths converts mount-timestamp keys into normalised mount
// paths by replacing fullwidth solidus (U+FF0F) with '/' and ensuring
// a trailing slash.
func DecodeMountPaths(mt map[string]time.Time) []string {
	paths := make([]string, 0, len(mt))

	for key := range mt {
		paths = append(paths, decodeMountPath(key))
	}

	sort.Strings(paths)

	return paths
}

func decodeMountPath(mountKey string) string {
	mountPath := strings.ReplaceAll(mountKey, "／", "/")
	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath
}

func pickDir(tree *db.Tree, startDir string) string {
	current := startDir

	for range dirPickMaxSteps {
		next, done := nextDir(tree, current)
		if done {
			return next
		}

		current = next
	}

	return current
}

func nextDir(tree *db.Tree, current string) (string, bool) {
	filter := &db.Filter{Age: db.DGUTAgeAll}

	info, err := tree.DirInfo(current, filter)
	if err != nil {
		return current, true
	}

	count := info.Current.Count
	if count >= dirPickMinCount && count <= dirPickMaxCount {
		return current, true
	}

	if len(info.Children) == 0 {
		return current, true
	}

	best := pickLargestChild(info.Children)
	if best == nil || best.Dir == "" {
		return current, true
	}

	return best.Dir, false
}

func pickLargestChild(children []*db.DirSummary) *db.DirSummary {
	var best *db.DirSummary

	for _, child := range children {
		if best == nil || child.Count > best.Count {
			best = child
		}
	}

	return best
}

func verifyPlans(qctx queryContext, printf PrintfFunc) error {
	ctx := context.Background()
	mountPath := mountPathForDir(qctx)

	explainLD, err := qctx.inspector.ExplainListDir(
		ctx, mountPath, qctx.dir,
		defaultExplainLimit, 0,
	)
	if err != nil {
		return fmt.Errorf("ExplainListDir failed: %w", err)
	}

	printf("ExplainListDir:\n%s\n\n", explainLD)

	if !ExplainHasPruning(explainLD) {
		return fmt.Errorf("%w:\n%s", ErrExplainMissingIndex, explainLD)
	}

	pickedPath := pickPath(qctx.client, qctx.dir)
	if pickedPath == "" {
		return nil
	}

	explainSP, spErr := qctx.inspector.ExplainStatPath(ctx, mountPath, pickedPath)
	if spErr != nil {
		return fmt.Errorf("ExplainStatPath failed: %w", spErr)
	}

	printf("ExplainStatPath:\n%s\n\n", explainSP)

	if !ExplainHasPruning(explainSP) {
		return fmt.Errorf("%w:\n%s", ErrExplainMissingIndex, explainSP)
	}

	return nil
}

func mountPathForDir(qctx queryContext) string {
	mt, err := qctx.provider.BaseDirs().MountTimestamps()
	if err != nil {
		return qctx.dir
	}

	mountPaths := DecodeMountPaths(mt)

	for _, mp := range mountPaths {
		if strings.HasPrefix(qctx.dir, mp) {
			return mp
		}
	}

	if len(mountPaths) > 0 {
		return mountPaths[0]
	}

	return qctx.dir
}

// ExplainHasPruning reports whether the EXPLAIN output mentions both
// mount_path and parent_dir index pruning.
func ExplainHasPruning(explain string) bool {
	return strings.Contains(explain, "mount_path") &&
		strings.Contains(explain, "parent_dir")
}

func pickPath(client QueryClient, dir string) string {
	ctx := context.Background()

	rows, err := client.ListDir(ctx, dir, 1)
	if err != nil || len(rows) == 0 {
		return ""
	}

	return rows[0].Path
}

func runSuite(
	report *boltperf.Report,
	qctx queryContext,
	opts QueryOptions,
	printf PrintfFunc,
) error {
	ops := buildOps(qctx, printf)

	for _, o := range ops {
		if err := runOp(report, qctx, o, opts, printf); err != nil {
			return err
		}
	}

	return nil
}

func buildOps(qctx queryContext, printf PrintfFunc) []op {
	ops := []op{
		opMountTimestamps(qctx),
		opTreeDirInfo(qctx),
		opGroupUsage(qctx),
		opListDir(qctx),
	}

	ops = append(ops, opStatPath(qctx, printf)...)
	ops = append(ops, opPermission(qctx))
	ops = append(ops, globOps(qctx)...)

	return ops
}

func opMountTimestamps(qctx queryContext) op {
	inputs := map[string]any{}

	return op{
		name:   "mount_timestamps",
		inputs: inputs,
		run: func(_ context.Context) error {
			ts, err := qctx.provider.BaseDirs().MountTimestamps()
			if err != nil {
				return err
			}

			inputs["mount_count"] = len(ts)
			inputs["active_mounts"] = activeMountsFreshness(ts)

			return nil
		},
	}
}

func activeMountsFreshness(mt map[string]time.Time) []activeMountFreshness {
	freshness := make([]activeMountFreshness, 0, len(mt))

	for mountKey, updatedAt := range mt {
		freshness = append(freshness, activeMountFreshness{
			MountPath: decodeMountPath(mountKey),
			UpdatedAt: updatedAt.UTC().Format(time.RFC3339Nano),
		})
	}

	sort.Slice(freshness, func(i, j int) bool {
		return freshness[i].MountPath < freshness[j].MountPath
	})

	return freshness
}

func opTreeDirInfo(qctx queryContext) op {
	return op{
		name:   "tree_dirinfo",
		inputs: map[string]any{"dir": qctx.dir},
		run: func(_ context.Context) error {
			filter := &db.Filter{Age: db.DGUTAgeAll}
			_, err := qctx.provider.Tree().DirInfo(qctx.dir, filter)

			return err
		},
	}
}

func opGroupUsage(qctx queryContext) op {
	return op{
		name:   "basedirs_group_usage",
		inputs: map[string]any{},
		run: func(_ context.Context) error {
			_, err := qctx.provider.BaseDirs().GroupUsage(db.DGUTAgeAll)

			return err
		},
	}
}

func opListDir(qctx queryContext) op {
	return op{
		name:   "files_listdir",
		inputs: map[string]any{"dir": qctx.dir},
		run: func(ctx context.Context) error {
			_, err := qctx.client.ListDir(ctx, qctx.dir, 0)

			return err
		},
	}
}

func opStatPath(qctx queryContext, printf PrintfFunc) []op {
	pickedPath := pickPath(qctx.client, qctx.dir)
	if pickedPath == "" {
		printf("query: %v\n", ErrEmptyDir)

		return nil
	}

	return []op{{
		name:   "files_statpath",
		inputs: map[string]any{"path": pickedPath},
		run: func(ctx context.Context) error {
			return qctx.client.StatPath(ctx, pickedPath)
		},
	}}
}

func opPermission(qctx queryContext) op {
	return op{
		name: "permission_check",
		inputs: map[string]any{
			"dir":  qctx.dir,
			"uid":  qctx.uid,
			"gids": qctx.gids,
		},
		run: func(ctx context.Context) error {
			return qctx.client.PermissionAnyInDir(ctx, qctx.dir, qctx.uid, qctx.gids)
		},
	}
}

func globOps(qctx queryContext) []op {
	ext := pickExt(qctx.client, qctx.dir)
	baseDirs := []string{qctx.dir}

	ops := []op{
		globOp(qctx, baseDirs, "A", []string{"*"}, false),
		globOp(qctx, baseDirs, "B", []string{"*"}, true),
		globOp(qctx, baseDirs, "C", []string{"**"}, false),
		globOp(qctx, baseDirs, "D", []string{"**"}, true),
	}

	if ext != "" {
		ops = append(ops,
			globOp(qctx, baseDirs, "E", []string{"*." + ext}, false),
			globOp(qctx, baseDirs, "F", []string{"*." + ext}, true),
			globOp(qctx, baseDirs, "G", []string{"**/*." + ext}, false),
			globOp(qctx, baseDirs, "H", []string{"**/*." + ext}, true),
		)
	}

	return ops
}

func pickExt(client QueryClient, dir string) string {
	ctx := context.Background()

	rows, err := client.ListDir(ctx, dir, 0)
	if err != nil {
		return ""
	}

	for _, r := range rows {
		if r.Ext != "" && r.EntryType != 'd' {
			return r.Ext
		}
	}

	return ""
}

func globOp(
	qctx queryContext,
	baseDirs []string,
	caseName string,
	patterns []string,
	requireOwner bool,
) op {
	return op{
		name: "glob_case_" + caseName,
		inputs: map[string]any{
			"patterns":      patterns,
			"require_owner": requireOwner,
		},
		run: func(ctx context.Context) error {
			return qctx.client.FindByGlob(
				ctx, baseDirs, patterns, requireOwner, qctx.uid, qctx.gids,
			)
		},
	}
}

func runOp(
	report *boltperf.Report,
	qctx queryContext,
	o op,
	opts QueryOptions,
	printf PrintfFunc,
) error {
	durations, err := timingLoop(qctx, o, opts.Repeat, printf)
	if err != nil {
		return err
	}

	report.AddOperation(o.name, o.inputs, durations)

	p50, p95, p99 := boltperf.PercentilesMS(durations)
	printf("%s repeats=%d p50=%.3f p95=%.3f p99=%.3f ms\n",
		o.name, len(durations), p50, p95, p99)

	return nil
}

func timingLoop(
	qctx queryContext,
	o op,
	repeat int,
	printf PrintfFunc,
) ([]float64, error) {
	ctx := context.Background()
	durations := make([]float64, 0, repeat)

	for i := range repeat {
		start := time.Now()

		metrics, err := qctx.inspector.Measure(ctx, o.run)
		if err != nil {
			return nil, fmt.Errorf("%s repeat %d/%d: %w", o.name, i+1, repeat, err)
		}

		durations = append(durations, measuredQueryDurationMS(metrics, time.Since(start)))

		printMetrics(printf, o.name, metrics)
	}

	return durations, nil
}

func measuredQueryDurationMS(metrics *QueryMetrics, wall time.Duration) float64 {
	if metrics != nil {
		return float64(metrics.DurationMs)
	}

	return durationMS(wall)
}

func printMetrics(
	printf PrintfFunc,
	name string,
	m *QueryMetrics,
) {
	if m == nil {
		return
	}

	printf("  %s metrics: duration_ms=%d read_rows=%d "+
		"read_bytes=%d result_rows=%d result_bytes=%d\n",
		name, m.DurationMs, m.ReadRows, m.ReadBytes,
		m.ResultRows, m.ResultBytes)
}

type activeMountFreshness struct {
	MountPath string `json:"mount_path"`
	UpdatedAt string `json:"updated_at"`
}

type op struct {
	name   string
	inputs map[string]any
	run    func(ctx context.Context) error
}

type queryContext struct {
	provider  provider.Provider
	client    QueryClient
	inspector QueryInspector
	dir       string
	uid       uint32
	gids      []uint32
}

func (q *queryContext) close() error {
	return errors.Join(
		q.inspector.Close(),
		q.client.Close(),
		q.provider.Close(),
	)
}
