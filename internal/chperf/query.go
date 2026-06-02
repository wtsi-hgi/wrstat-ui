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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

const (
	dirPickMinCount                       = 1000
	dirPickMaxCount                       = 20000
	dirPickMaxSteps                       = 64
	defaultExplainLimit                   = 1_000_000
	queryInputAgeKey                      = "age"
	queryInputDirKey                      = "dir"
	queryInputDurationSource              = "duration_source"
	queryInputCacheScope                  = "cache_scope"
	queryInputFilterFileTypeMaskKey       = "filter_file_type_mask"
	queryInputFilterFileTypesKey          = "filter_file_types"
	queryInputFilterGIDsKey               = "filter_gids"
	queryInputFilterIndexGateKey          = "filter_index_gate"
	queryInputFilterUIDsKey               = "filter_uids"
	queryInputParentDirKey                = "parent_dir"
	queryInputSplitsKey                   = "splits"
	queryInputStartDirKey                 = "start_dir"
	queryInputTreeFilterRouteKey          = "tree_filter_route"
	queryOpFilesListDirName               = "files_listdir"
	queryOpStartupCacheWarmingAuditName   = "startup_cache_warming_audit"
	queryOpTreeDiskTreeAncName            = "tree_disktree_endpoint_ancestor_dirs"
	queryOpTreeDiskTreeColdProviderName   = "tree_disktree_endpoint_cold_provider"
	queryOpTreeDiskTreeEndName            = "tree_disktree_endpoint"
	queryOpTreeDiskTreeNewName            = "tree_disktree_endpoint_new_dirs"
	queryOpTreeDiskTreeProviderUpdateName = "tree_disktree_endpoint_provider_update_cold_cache"
	queryOpTreeDiskTreeVisibleChildName   = "tree_disktree_endpoint_visible_child_dirs"
	queryOpTreeDirInfoName                = "tree_dirinfo"
	queryOpDirInfoBroadName               = "dirinfo_broad"
	queryOpDirInfoFilteredName            = "dirinfo_filtered"
	queryOpDirInfosBroadName              = "dirinfos_broad"
	queryOpDirInfosFilteredName           = "dirinfos_filtered"
	queryOpDirsHaveChildrenBroadName      = "dirshavechildren_broad"
	queryOpDirsHaveChildrenFilteredName   = "dirshavechildren_filtered"
	queryOpFindGlobExtensionDotfileName   = "find_glob_extension_dotfile"
	queryOpFilesStatPathName              = "files_statpath"
	queryOpGlobCaseAName                  = "glob_case_A"
	queryOpGlobCaseBName                  = "glob_case_B"
	queryOpTreeWhereColdName              = "tree_where_cold_then_cached"
	queryOpTreeWhereColdProviderName      = "tree_where_cold_provider"
	queryOpTreeWhereFreshName             = "tree_where_fresh_provider"
	queryOpTreeWhereName                  = "tree_where"
	queryOpTreeWhereProviderUpdateName    = "tree_where_provider_update_cold_cache"
	queryOpVirtualChildrenName            = "virtual_children"
	queryOpVirtualDirInfoName             = "virtual_dirinfo"
	queryOpWhereFilteredWholeMountName    = "where_filtered_whole_mount"
	queryOpWhereWholeMountName            = "where_whole_mount" //nolint:gosec // Operation name, not a credential.
	queryScopeFreshProvider               = "fresh_provider_per_repeat"
	queryScopeAncestorDirs                = "ancestor_directory_each_repeat"
	queryScopeColdProvider                = "cold_provider_with_cold_query_cache"
	queryScopeNewDirEachRepeat            = "new_directory_each_repeat"
	queryScopeProviderUpdateCold          = "provider_update_cold_cache"
	queryScopeSameProviderCold            = "same_provider_cold_then_warm"
	queryScopeSameProviderDir             = "same_provider_same_dir"
	queryScopeSameQueryClient             = "same_query_client"
	queryScopeStartupAudit                = "startup_cache_warming_contract"
	queryScopeVisibleChildDirs            = "visible_child_directory_each_repeat"
	querySourceClickHouseLog              = "clickhouse_query_log"
	querySourceWall                       = "wall"
	queryTreeFilterRouteFactsVectors      = "wrstat_dir_facts_vectors"
	queryTreeFilterRouteOptionalAgeAll    = "wrstat_dir_facts_default_optional_wrstat_dir_filter_ageall"
	queryFilterIndexGateInapplicable      = "not_applicable"
	queryFilterIndexGatePerfRequired      = "requires_failed_final_perf_gate_and_ready_index"
	queryStartupStageBackgroundProvider   = "background_provider_polling_or_update_after_initial_readers"
	queryStartupStageLazyInteraction      = "lazy_during_user_or_perf_interactions"
	queryStartupStageSynchronousInitial   = "synchronous_before_server_started"
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

	errUnknownQueryOps      = errors.New("unknown query ops")
	errOpenProviderRequired = errors.New("OpenProvider is required")
)

// QueryOptions configures the query timing suite.
type QueryOptions struct {
	Dir           string
	AncestorDir   string
	Ops           []string
	UID           uint32
	GIDs          []uint32
	TreeFilter    *db.Filter
	Repeat        int
	Warmup        int
	Splits        int
	WalkDepth     int
	WalkLimit     int
	AncestorLimit int
}

// Query runs a repeatable timing suite against ClickHouse and returns
// a Report with per-query latency percentiles.
func Query(
	api QueryAPI,
	opts QueryOptions,
	printf PrintfFunc,
) (_ perfreport.Report, err error) {
	qctx, err := buildQueryContext(api, opts, printf)
	if err != nil {
		return perfreport.Report{}, err
	}

	defer func() {
		if cerr := qctx.close(); err == nil {
			err = cerr
		}
	}()

	if err := verifyPlans(qctx, printf); err != nil {
		return perfreport.Report{}, err
	}

	report := perfreport.NewReport("clickhouse", "", opts.Repeat, opts.Warmup)

	if err := runSuite(&report, qctx, opts, printf); err != nil {
		return perfreport.Report{}, err
	}

	return report, nil
}

func buildQueryContext(
	api QueryAPI,
	opts QueryOptions,
	printf PrintfFunc,
) (queryContext, error) {
	qctx, err := openQueryContext(api)
	if err != nil {
		return queryContext{}, err
	}

	qctx.treeFilter = treeFilterFromOptions(opts.TreeFilter)

	dir, err := selectDir(qctx.provider, opts.Dir, qctx.treeFilter, printf)
	if err != nil {
		return queryContext{}, errors.Join(err, qctx.close())
	}

	qctx.dir = dir
	qctx.uid = opts.UID
	qctx.gids = opts.GIDs

	return qctx, nil
}

func openQueryContext(api QueryAPI) (queryContext, error) {
	var qctx queryContext

	qctx.openProvider = api.OpenProvider
	qctx.resetQueryCaches = queryCacheResetter(api)

	p, err := qctx.openProvider()
	if err != nil {
		return queryContext{}, err
	}

	qctx.provider = p

	client, err := api.NewQueryClient()
	if err != nil {
		return queryContext{}, errors.Join(err, qctx.close())
	}

	qctx.client = client

	inspector, err := api.NewQueryInspector()
	if err != nil {
		return queryContext{}, errors.Join(err, qctx.close())
	}

	qctx.inspector = inspector

	return qctx, nil
}

func queryCacheResetter(api QueryAPI) func() {
	resetter, ok := api.(QueryCacheResetter)
	if !ok {
		return nil
	}

	return resetter.ResetQueryCaches
}

func treeFilterFromOptions(filter *db.Filter) *db.Filter {
	if filter == nil {
		return &db.Filter{Age: db.DGUTAgeAll}
	}

	return &db.Filter{
		GIDs: slices.Clone(filter.GIDs),
		UIDs: slices.Clone(filter.UIDs),
		FT:   filter.FT,
		Age:  filter.Age,
	}
}

func missingDirInfo(info *db.DirInfo) bool {
	return info == nil || info.Current == nil
}

func representativeDirInfo(info *db.DirInfo) bool {
	count := info.Current.Count

	return (count >= dirPickMinCount && count <= dirPickMaxCount) || len(info.Children) == 0
}

func largestChildDir(children []*db.DirSummary) string {
	best := pickLargestChild(children)
	if best == nil {
		return ""
	}

	return best.Dir
}

func selectOps(ops []op, names []string) ([]op, error) {
	wanted, wantedOrder := opNameSet(names)
	if len(wanted) == 0 {
		return ops, nil
	}

	available := make([]string, 0, len(ops))
	availableSet := make(map[string]struct{}, len(ops))
	selected := make([]op, 0, len(wanted))

	for _, candidate := range ops {
		available = append(available, candidate.name)
		availableSet[candidate.name] = struct{}{}

		if _, ok := wanted[candidate.name]; ok {
			selected = append(selected, candidate)
		}
	}

	unknown := unknownOpNames(wantedOrder, availableSet)
	if len(unknown) > 0 {
		return nil, fmt.Errorf(
			"%w: %s; available ops: %s",
			errUnknownQueryOps,
			strings.Join(unknown, ", "),
			strings.Join(available, ", "),
		)
	}

	return selected, nil
}

func opNameSet(names []string) (map[string]struct{}, []string) {
	wanted := make(map[string]struct{}, len(names))
	wantedOrder := make([]string, 0, len(names))

	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		if _, ok := wanted[name]; ok {
			continue
		}

		wanted[name] = struct{}{}
		wantedOrder = append(wantedOrder, name)
	}

	return wanted, wantedOrder
}

func unknownOpNames(wanted []string, available map[string]struct{}) []string {
	unknown := make([]string, 0)

	for _, name := range wanted {
		if _, ok := available[name]; ok {
			continue
		}

		unknown = append(unknown, name)
	}

	return unknown
}

func opStartupCacheWarmingAudit() op {
	return op{
		name: queryOpStartupCacheWarmingAuditName,
		inputs: map[string]any{
			queryInputCacheScope:     queryScopeStartupAudit,
			queryInputDurationSource: querySourceWall,
			"server_started_contract": "server_started_is_logged_after_clickhouse_provider_open_" +
				"and_server_set_provider_complete",
			"initial_provider_readers_timing": queryStartupStageSynchronousInitial,
			"server_basedirs_cache_timing":    queryStartupStageSynchronousInitial,
			"query_cache_warmup_timing":       queryStartupStageLazyInteraction,
			"provider_polling_timing":         queryStartupStageBackgroundProvider,
			"provider_update_refresh_timing":  queryStartupStageBackgroundProvider,
			"initial_provider_work": []string{
				"validate_clickhouse_config",
				"open_clickhouse_connection",
				"build_initial_readers",
				"publish_initial_tree_and_basedirs_readers",
			},
			"server_cache_work": []string{
				"validate_provider",
				"read_basedirs",
				"read_mount_timestamps",
				"prewarm_basedirs_caches",
				"assign_provider_fields",
			},
			"query_cache_work": []string{
				"lazy_query_cache_warmup_from_interactions",
				"measured_by_cold_then_warm_and_visible_child_ops",
			},
			"provider_update_work": []string{
				"poll_active_mounts",
				"rebuild_readers_when_active_mounts_change",
				"server_refresh_provider_from_update_callback",
				"prewarm_basedirs_caches_before_field_swap",
			},
			"timing_ops": map[string][]string{
				"lazy_query_cache_warmup": {
					queryOpTreeWhereColdName,
					queryOpTreeDiskTreeVisibleChildName,
				},
				"cold_provider_startup_replay": {
					queryOpTreeWhereColdProviderName,
					queryOpTreeDiskTreeColdProviderName,
				},
				"provider_update_cold_cache": {
					queryOpTreeWhereProviderUpdateName,
					queryOpTreeDiskTreeProviderUpdateName,
				},
			},
		},
		run: func(context.Context) error {
			return nil
		},
		useWallTime:       true,
		skipWarmup:        true,
		hasRepeatOverride: true,
		repeatOverride:    1,
	}
}

func opTreeWhereColdThenCached(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeWhereColdName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderCold,
			queryInputDurationSource: querySourceWall,
			queryInputSplitsKey:      splits,
		}),
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func treeOpInputs(filter *db.Filter, inputs map[string]any) map[string]any {
	if inputs == nil {
		inputs = map[string]any{}
	}

	filter = treeFilterFromOptions(filter)
	inputs[queryInputAgeKey] = int(filter.Age)
	inputs[queryInputFilterGIDsKey] = slices.Clone(filter.GIDs)
	inputs[queryInputFilterUIDsKey] = slices.Clone(filter.UIDs)
	inputs[queryInputFilterFileTypeMaskKey] = int(filter.FT)
	inputs[queryInputFilterFileTypesKey] = filter.FT.String()
	inputs[queryInputTreeFilterRouteKey] = treeFilterRoute(filter)
	inputs[queryInputFilterIndexGateKey] = treeFilterIndexGate(filter)

	return inputs
}

func treeFilterRoute(filter *db.Filter) string {
	if treeFilterAgeAllIndexEligible(filter) {
		return queryTreeFilterRouteOptionalAgeAll
	}

	return queryTreeFilterRouteFactsVectors
}

func treeFilterAgeAllIndexEligible(filter *db.Filter) bool {
	if filter == nil || filter.Age != db.DGUTAgeAll {
		return false
	}

	if treeFilterHasEmptyIDPredicate(filter) {
		return false
	}

	return treeFilterHasOwnerOrTypePredicate(filter)
}

func treeFilterHasEmptyIDPredicate(filter *db.Filter) bool {
	return (filter.GIDs != nil && len(filter.GIDs) == 0) ||
		(filter.UIDs != nil && len(filter.UIDs) == 0)
}

func treeFilterHasOwnerOrTypePredicate(filter *db.Filter) bool {
	return filter.GIDs != nil || filter.UIDs != nil || filter.FT != 0
}

func treeFilterIndexGate(filter *db.Filter) string {
	if treeFilterAgeAllIndexEligible(filter) {
		return queryFilterIndexGatePerfRequired
	}

	return queryFilterIndexGateInapplicable
}

func opTreeWhereColdProvider(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeWhereColdProviderName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeColdProvider,
			queryInputDurationSource: querySourceWall,
			queryInputSplitsKey:      splits,
		}),
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			return nil
		},
		run: func(_ context.Context) error {
			count, err := runTreeWhereFreshProvider(qctx, splits, filter)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func opTreeDiskTreeEndpointColdProvider(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeDiskTreeColdProviderName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeColdProvider,
			queryInputDurationSource: querySourceWall,
		}),
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			return nil
		},
		run: func(_ context.Context) error {
			count, err := runTreeDiskTreeFreshProvider(qctx, filter)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func runTreeDiskTreeFreshProvider(qctx queryContext, filter *db.Filter) (uint64, error) {
	if qctx.openProvider == nil {
		return 0, errOpenProviderRequired
	}

	p, err := qctx.openProvider()
	if err != nil {
		return 0, err
	}

	count, runErr := runTreeDiskTreeEndpoint(p.Tree(), qctx.dir, filter)
	closeErr := p.Close()

	return count, errors.Join(runErr, closeErr)
}

func opTreeWhereProviderUpdateColdCache(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var (
		p           provider.Provider
		resultCount uint64
	)

	return op{
		name: queryOpTreeWhereProviderUpdateName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeProviderUpdateCold,
			queryInputDurationSource: querySourceWall,
			queryInputSplitsKey:      splits,
		}),
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			var err error

			p, err = openProviderForRepeat(qctx)

			return err
		},
		run: func(_ context.Context) error {
			results, err := p.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))

			return err
		},
		teardown: func(_ context.Context) error {
			err := p.Close()
			p = nil

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func openProviderForRepeat(qctx queryContext) (provider.Provider, error) {
	if qctx.openProvider == nil {
		return nil, errOpenProviderRequired
	}

	return qctx.openProvider()
}

func opTreeDiskTreeProviderUpdateColdCache(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var (
		p           provider.Provider
		resultCount uint64
	)

	return op{
		name: queryOpTreeDiskTreeProviderUpdateName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeProviderUpdateCold,
			queryInputDurationSource: querySourceWall,
		}),
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			var err error

			p, err = openProviderForRepeat(qctx)

			return err
		},
		run: func(_ context.Context) error {
			count, err := runTreeDiskTreeEndpoint(p.Tree(), qctx.dir, filter)
			resultCount = count

			return err
		},
		teardown: func(_ context.Context) error {
			err := p.Close()
			p = nil

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func opTreeDiskTreeEndpointNewDirs(qctx queryContext, opts QueryOptions) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	dirs, fallback := disktreeClickDirs(qctx, opts)
	timedDirs := uniqueDirsForRepeats(dirs, opts.Repeat)
	i := 0

	var resultCount uint64

	return op{
		name: queryOpTreeDiskTreeNewName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputStartDirKey:    qctx.dir,
			"dirs":                   timedDirs,
			"dir_count":              len(timedDirs),
			"walk_depth":             opts.WalkDepth,
			"walk_limit":             opts.WalkLimit,
			"fallback_to_start_dir":  fallback,
			queryInputCacheScope:     queryScopeNewDirEachRepeat,
			queryInputDurationSource: querySourceWall,
		}),
		run: func(_ context.Context) error {
			if i >= len(timedDirs) {
				return nil
			}

			dir := timedDirs[i]
			i++

			count, err := runTreeDiskTreeEndpoint(qctx.provider.Tree(), dir, filter)
			resultCount = count

			return err
		},
		resultCount:       func() uint64 { return resultCount },
		useWallTime:       true,
		skipWarmup:        true,
		hasRepeatOverride: true,
		repeatOverride:    len(timedDirs),
	}
}

func loadTreeDiskTreeEndpoint(tree *db.Tree, dir string, filter *db.Filter) ([]string, error) {
	filter = treeFilterFromOptions(filter)

	di, err := tree.DirInfo(dir, filter)
	if err != nil || di == nil {
		return nil, err
	}

	childPaths := make([]string, 0, len(di.Children))
	for _, child := range di.Children {
		childPaths = append(childPaths, child.Dir)
	}

	_ = tree.DirsHaveChildren(childPaths, filter)

	return childPaths, nil
}

func opTreeDiskTreeEndpointAncestorDirs(qctx queryContext, opts QueryOptions) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	dirs := ancestorDisktreeDirs(qctx, opts)
	timedDirs := cycledDirsForRepeats(dirs, opts.Repeat)
	i := 0

	var resultCount uint64

	return op{
		name: queryOpTreeDiskTreeAncName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputStartDirKey:    ancestorStartDir(opts),
			"dirs":                   timedDirs,
			"dir_count":              len(timedDirs),
			"ancestor_limit":         opts.AncestorLimit,
			queryInputCacheScope:     queryScopeAncestorDirs,
			queryInputDurationSource: querySourceWall,
		}),
		run: func(_ context.Context) error {
			if i >= len(timedDirs) {
				return nil
			}

			dir := timedDirs[i]
			i++

			count, err := runTreeDiskTreeEndpoint(qctx.provider.Tree(), dir, filter)
			resultCount = count

			return err
		},
		resultCount:       func() uint64 { return resultCount },
		useWallTime:       true,
		skipWarmup:        true,
		hasRepeatOverride: true,
		repeatOverride:    len(timedDirs),
	}
}

func uniqueDirsForRepeats(dirs []string, repeat int) []string {
	if repeat <= 0 || len(dirs) == 0 {
		return nil
	}

	n := min(repeat, len(dirs))

	return slices.Clone(dirs[:n])
}

func cycledDirsForRepeats(dirs []string, repeat int) []string {
	if repeat <= 0 || len(dirs) == 0 {
		return nil
	}

	timedDirs := make([]string, 0, repeat)
	for i := range repeat {
		timedDirs = append(timedDirs, dirs[i%len(dirs)])
	}

	return timedDirs
}

func opTreeDiskTreeEndpoint(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeDiskTreeEndName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			count, err := runTreeDiskTreeEndpoint(qctx.provider.Tree(), qctx.dir, filter)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func runTreeDiskTreeEndpoint(tree *db.Tree, dir string, filter *db.Filter) (uint64, error) {
	childPaths, err := loadTreeDiskTreeEndpoint(tree, dir, filter)

	return uint64(len(childPaths)), err
}

func opTreeDiskTreeEndpointVisibleChildDirs(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	inputs := treeOpInputs(filter, map[string]any{
		queryInputParentDirKey:   qctx.dir,
		"child_dirs":             []string{},
		"child_count":            0,
		"fallback_to_parent_dir": false,
		queryInputCacheScope:     queryScopeVisibleChildDirs,
		queryInputDurationSource: querySourceWall,
	})

	var timedDirs []string

	i := 0

	var resultCount uint64

	return op{
		name:   queryOpTreeDiskTreeVisibleChildName,
		inputs: inputs,
		prepare: func(repeat int) (int, error) {
			childDirs, err := loadTreeDiskTreeEndpoint(qctx.provider.Tree(), qctx.dir, filter)
			if err != nil {
				return 0, err
			}

			var fallback bool

			timedDirs, fallback = visibleChildDirsForRepeats(childDirs, qctx.dir, repeat)
			inputs["child_dirs"] = timedDirs
			inputs["child_count"] = len(timedDirs)
			inputs["fallback_to_parent_dir"] = fallback
			i = 0

			return len(timedDirs), nil
		},
		run: func(_ context.Context) error {
			if i >= len(timedDirs) {
				return nil
			}

			dir := timedDirs[i]
			i++

			count, err := runTreeDiskTreeEndpoint(qctx.provider.Tree(), dir, filter)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func visibleChildDirsForRepeats(childDirs []string, parentDir string, repeat int) ([]string, bool) {
	timedDirs := uniqueDirsForRepeats(childDirs, repeat)
	if len(timedDirs) > 0 || repeat <= 0 {
		return timedDirs, false
	}

	return []string{parentDir}, true
}

func opTreeWhere(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeWhereName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
			queryInputSplitsKey:      splits,
		}),
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opTreeWhereFreshProvider(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeWhereFreshName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeFreshProvider,
			queryInputDurationSource: querySourceWall,
			queryInputSplitsKey:      splits,
		}),
		run: func(_ context.Context) error {
			count, err := runTreeWhereFreshProvider(qctx, splits, filter)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func runTreeWhereFreshProvider(qctx queryContext, splits int, filter *db.Filter) (uint64, error) {
	if qctx.openProvider == nil {
		return 0, errOpenProviderRequired
	}

	p, err := qctx.openProvider()
	if err != nil {
		return 0, err
	}

	results, whereErr := p.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
	closeErr := p.Close()

	return uint64(len(results)), errors.Join(whereErr, closeErr)
}

func opResultCount(o op) uint64 {
	if o.resultCount == nil {
		return 0
	}

	return o.resultCount()
}

func querySampleDurations(samples []queryRepeatSample) []float64 {
	durations := make([]float64, len(samples))
	for i, sample := range samples {
		durations[i] = sample.durationMS
	}

	return durations
}

func querySampleReadRows(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.ReadRows })
	if !ok {
		return nil
	}

	return values
}

func querySampleMetricValues(
	samples []queryRepeatSample,
	value func(*QueryMetrics) uint64,
) ([]uint64, bool) {
	values := make([]uint64, 0, len(samples))
	for _, sample := range samples {
		if sample.metrics == nil {
			continue
		}

		values = append(values, value(sample.metrics))
	}

	return values, len(values) > 0
}

func querySampleReadBytes(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.ReadBytes })
	if !ok {
		return nil
	}

	return values
}

func querySampleReadMarks(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.ReadMarks })
	if !ok {
		return nil
	}

	return values
}

func querySampleResultCounts(samples []queryRepeatSample, o op) []uint64 {
	if o.resultCount == nil {
		return nil
	}

	counts := make([]uint64, len(samples))
	for i, sample := range samples {
		counts[i] = sample.resultCount
	}

	return counts
}

func focusedQueryOps(qctx queryContext, opts QueryOptions) []op {
	broadFilter := treeFilterFromOptions(nil)
	filtered := buildTreeFilter(qctx, opts)

	return []op{
		opFocusedDirInfo(qctx, queryOpDirInfoBroadName, broadFilter),
		opFocusedDirInfo(qctx, queryOpDirInfoFilteredName, filtered),
		opFocusedDirInfos(qctx, queryOpDirInfosBroadName, broadFilter),
		opFocusedDirInfos(qctx, queryOpDirInfosFilteredName, filtered),
		opFocusedDirsHaveChildren(qctx, queryOpDirsHaveChildrenBroadName, broadFilter),
		opFocusedDirsHaveChildren(qctx, queryOpDirsHaveChildrenFilteredName, filtered),
		opFocusedWhere(qctx, queryOpWhereWholeMountName, broadFilter, opts.Splits),
		opFocusedWhere(qctx, queryOpWhereFilteredWholeMountName, filtered, opts.Splits),
		opFocusedVirtualChildren(qctx, filtered),
		opFocusedVirtualDirInfo(qctx, filtered),
		opFocusedGlobExtensionDotfile(qctx),
	}
}

func opFocusedDirInfo(qctx queryContext, name string, filter *db.Filter) op {
	var resultCount uint64

	return op{
		name: name,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().DirInfo(qctx.dir, filter)
			resultCount = dirInfoResultCount(info)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func dirInfoResultCount(info *db.DirInfo) uint64 {
	if info == nil || info.Current == nil {
		return 0
	}

	return uint64(1 + len(info.Children))
}

func opFocusedDirInfos(qctx queryContext, name string, filter *db.Filter) op {
	var resultCount uint64

	return op{
		name: name,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputParentDirKey:   qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			dirs, err := focusedDirInfoDirs(qctx.provider.Tree(), qctx.dir, filter)
			if err != nil {
				return err
			}

			resultCount = uint64(len(dirs))

			return nil
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func focusedDirInfoDirs(tree *db.Tree, dir string, filter *db.Filter) ([]string, error) {
	info, err := tree.DirInfo(dir, filter)
	if err != nil || info == nil {
		return nil, err
	}

	dirs := make([]string, 0, 1+len(info.Children))

	dirs = append(dirs, dir)
	for _, child := range info.Children {
		dirs = append(dirs, child.Dir)
	}

	for _, candidate := range dirs {
		if _, err := tree.DirInfo(candidate, filter); err != nil {
			return nil, err
		}
	}

	return dirs, nil
}

func opFocusedDirsHaveChildren(qctx queryContext, name string, filter *db.Filter) op {
	var resultCount uint64

	return op{
		name: name,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputParentDirKey:   qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			dirs, err := focusedDirInfoDirs(qctx.provider.Tree(), qctx.dir, filter)
			if err != nil {
				return err
			}

			hasChildren := qctx.provider.Tree().DirsHaveChildren(dirs, filter)
			resultCount = countTrue(hasChildren)

			return nil
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func countTrue(values map[string]bool) uint64 {
	var count uint64

	for _, value := range values {
		if value {
			count++
		}
	}

	return count
}

func opFocusedWhere(qctx queryContext, name string, filter *db.Filter, splits int) op {
	var resultCount uint64

	return op{
		name: name,
		inputs: treeOpInputs(filter, map[string]any{
			"mount_path":             mountPathForDir(qctx),
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
			queryInputSplitsKey:      splits,
		}),
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(
				mountPathForDir(qctx),
				filter,
				split.SplitsToSplitFn(splits),
			)
			resultCount = uint64(len(results))

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opFocusedVirtualChildren(qctx queryContext, filter *db.Filter) op {
	var resultCount uint64

	return op{
		name: queryOpVirtualChildrenName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         "/",
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			hasChildren := qctx.provider.Tree().DirsHaveChildren([]string{"/", qctx.dir}, filter)
			resultCount = countTrue(hasChildren)

			return nil
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opFocusedVirtualDirInfo(qctx queryContext, filter *db.Filter) op {
	var resultCount uint64

	return op{
		name: queryOpVirtualDirInfoName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         "/",
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().DirInfo("/", filter)
			resultCount = dirInfoResultCount(info)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opFocusedGlobExtensionDotfile(qctx queryContext) op {
	ext := pickExt(qctx.client, qctx.dir)
	if ext == "" {
		ext = "*"
	}

	pattern := ".*." + ext

	var resultCount uint64

	return op{
		name: queryOpFindGlobExtensionDotfileName,
		inputs: map[string]any{
			"patterns":               []string{pattern},
			"require_owner":          false,
			queryInputCacheScope:     queryScopeSameQueryClient,
			queryInputDurationSource: querySourceClickHouseLog,
		},
		run: func(ctx context.Context) error {
			count, err := qctx.client.FindByGlob(
				ctx,
				[]string{qctx.dir},
				[]string{pattern},
				false,
				qctx.uid,
				qctx.gids,
			)
			resultCount = intToUint64(count)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func intToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}

	return uint64(value)
}

func buildTreeFilter(qctx queryContext, opts QueryOptions) *db.Filter {
	if opts.TreeFilter != nil {
		return treeFilterFromOptions(opts.TreeFilter)
	}

	return treeFilterFromOptions(qctx.treeFilter)
}

func warmupOp(ctx context.Context, o op, warmup int) error {
	for i := range warmup {
		if err := runOpCycle(ctx, o, func(context.Context) error {
			return o.run(ctx)
		}); err != nil {
			return fmt.Errorf("%s warmup %d/%d: %w", o.name, i+1, warmup, err)
		}
	}

	return nil
}

func runOpCycle(ctx context.Context, o op, run func(context.Context) error) error {
	if o.setup != nil {
		if err := o.setup(ctx); err != nil {
			return err
		}
	}

	runErr := run(ctx)
	teardownErr := teardownOp(ctx, o)

	return errors.Join(runErr, teardownErr)
}

func teardownOp(ctx context.Context, o op) error {
	if o.teardown == nil {
		return nil
	}

	return o.teardown(ctx)
}

func timeOpRepeat(
	ctx context.Context,
	qctx queryContext,
	o op,
	printf PrintfFunc,
) (queryRepeatSample, error) {
	if o.useWallTime {
		return timeWallRepeat(ctx, o)
	}

	return timeMeasuredRepeat(ctx, qctx, o, printf)
}

func timeWallRepeat(ctx context.Context, o op) (queryRepeatSample, error) {
	var duration float64

	err := runOpCycle(ctx, o, func(context.Context) error {
		var runErr error

		duration, runErr = timeWallOp(ctx, o.run)

		return runErr
	})

	return queryRepeatSample{
		durationMS:  duration,
		resultCount: opResultCount(o),
	}, err
}

func timeWallOp(ctx context.Context, run func(context.Context) error) (float64, error) {
	start := time.Now()

	if err := run(ctx); err != nil {
		return 0, err
	}

	return durationMS(time.Since(start)), nil
}

func timeMeasuredRepeat(
	ctx context.Context,
	qctx queryContext,
	o op,
	printf PrintfFunc,
) (queryRepeatSample, error) {
	var (
		duration float64
		metrics  *QueryMetrics
	)

	err := runOpCycle(ctx, o, func(context.Context) error {
		start := time.Now()

		var runErr error

		metrics, runErr = qctx.inspector.Measure(ctx, o.run)
		duration = measuredQueryDurationMS(metrics, time.Since(start))

		return runErr
	})
	if err != nil {
		return queryRepeatSample{}, err
	}

	printMetrics(printf, o.name, metrics)

	return queryRepeatSample{
		durationMS:  duration,
		metrics:     metrics,
		resultCount: opResultCount(o),
	}, nil
}

func disktreeClickDirs(qctx queryContext, opts QueryOptions) ([]string, bool) {
	dirs := leafDisktreeDirs(collectDisktreeDirsFromFileAPI(qctx.client, qctx.dir, opts.WalkDepth, opts.WalkLimit))
	if len(dirs) > 0 || opts.WalkLimit <= 0 {
		return dirs, false
	}

	return []string{qctx.dir}, true
}

func ancestorDisktreeDirs(qctx queryContext, opts QueryOptions) []string {
	startDir := ancestorStartDir(opts)
	if opts.AncestorLimit <= 0 {
		return nil
	}

	mountPaths := activeMountPaths(qctx.provider)
	if len(mountPaths) == 0 {
		return []string{startDir}
	}

	return ancestorDirsForMountPaths(startDir, mountPaths, opts.AncestorLimit)
}

func ancestorStartDir(opts QueryOptions) string {
	if dir := normaliseDirPath(opts.AncestorDir); dir != "" {
		return dir
	}

	return "/"
}

func activeMountPaths(p provider.Provider) []string {
	if p == nil || p.BaseDirs() == nil {
		return nil
	}

	mt, err := p.BaseDirs().MountTimestamps()
	if err != nil {
		return nil
	}

	return DecodeMountPaths(mt)
}

func ancestorDirsForMountPaths(startDir string, mountPaths []string, limit int) []string {
	dirs := make([]string, 0, min(limit, len(mountPaths)+1))
	seen := make(map[string]bool, len(mountPaths)+1)

	addAncestorDir(&dirs, seen, startDir, limit)

	for _, mountPath := range mountPaths {
		for _, dir := range prefixDirsForMount(startDir, mountPath) {
			addAncestorDir(&dirs, seen, dir, limit)
		}
	}

	return dirs
}

func prefixDirsForMount(startDir, mountPath string) []string {
	mountPath = normaliseDirPath(mountPath)
	if mountPath == "" || !strings.HasPrefix(mountPath, startDir) {
		return nil
	}

	parts := strings.Split(strings.Trim(mountPath, "/"), "/")
	dirs := make([]string, 0, len(parts)+1)
	current := "/"

	for _, part := range parts {
		if part == "" {
			continue
		}

		current += part + "/"
		if strings.HasPrefix(current, startDir) {
			dirs = append(dirs, current)
		}
	}

	return dirs
}

func addAncestorDir(dirs *[]string, seen map[string]bool, dir string, limit int) {
	if len(*dirs) >= limit || seen[dir] {
		return
	}

	seen[dir] = true
	*dirs = append(*dirs, dir)
}

func leafDisktreeDirs(dirs []string) []string {
	leaves := make([]string, 0, len(dirs))

	for _, dir := range dirs {
		if hasDisktreeDescendant(dir, dirs) {
			continue
		}

		leaves = append(leaves, dir)
	}

	return leaves
}

func hasDisktreeDescendant(dir string, dirs []string) bool {
	for _, candidate := range dirs {
		if candidate != dir && strings.HasPrefix(candidate, dir) {
			return true
		}
	}

	return false
}

func collectDisktreeDirsFromFileAPI(
	client QueryClient,
	startDir string,
	depth int,
	limit int,
) []string {
	if client == nil || depth <= 0 || limit <= 0 {
		return nil
	}

	ctx := context.Background()
	queue := []disktreeWalkDir{{dir: startDir}}
	seen := map[string]struct{}{startDir: {}}
	dirs := make([]string, 0, limit)

	for len(queue) > 0 && len(dirs) < limit {
		item := queue[0]
		queue = queue[1:]

		if item.depth >= depth {
			continue
		}

		children := listChildDirs(ctx, client, item.dir)
		queue = appendDisktreeWalkChildren(queue, &dirs, seen, limit, item.depth+1, children)
	}

	return dirs
}

func listChildDirs(ctx context.Context, client QueryClient, dir string) []string {
	rows, err := client.ListDir(ctx, dir, 0)
	if err != nil {
		return nil
	}

	dirs := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.EntryType != 'd' {
			continue
		}

		if child := normaliseDirPath(row.Path); child != "" {
			dirs = append(dirs, child)
		}
	}

	return dirs
}

func appendDisktreeWalkChildren(
	queue []disktreeWalkDir,
	dirs *[]string,
	seen map[string]struct{},
	limit int,
	childDepth int,
	children []string,
) []disktreeWalkDir {
	for _, child := range children {
		if len(*dirs) >= limit {
			return queue
		}

		if _, ok := seen[child]; ok {
			continue
		}

		seen[child] = struct{}{}
		*dirs = append(*dirs, child)
		queue = append(queue, disktreeWalkDir{dir: child, depth: childDepth})
	}

	return queue
}

type disktreeWalkDir struct {
	dir   string
	depth int
}

type queryRepeatSample struct {
	durationMS  float64
	metrics     *QueryMetrics
	resultCount uint64
}

func closeQueryResources(closers ...io.Closer) error {
	var err error

	for _, closer := range closers {
		if closer == nil {
			continue
		}

		err = errors.Join(err, closer.Close())
	}

	return err
}

func selectDir(
	p provider.Provider,
	explicitDir string,
	filter *db.Filter,
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

	dir := pickDir(p.Tree(), startDir, filter)
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
	return mountpath.DecodeSortedKeys(mt)
}

func pickDir(tree *db.Tree, startDir string, filter *db.Filter) string {
	filter = treeFilterFromOptions(filter)
	current := startDir

	for range dirPickMaxSteps {
		next, done := nextDir(tree, current, filter)
		if done {
			return next
		}

		current = next
	}

	return current
}

func nextDir(tree *db.Tree, current string, filter *db.Filter) (string, bool) {
	info, err := tree.DirInfo(current, filter)
	if err != nil || missingDirInfo(info) {
		return current, true
	}

	if representativeDirInfo(info) {
		return current, true
	}

	next := largestChildDir(info.Children)
	if next == "" {
		return current, true
	}

	return next, false
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
	if qctx.provider == nil || qctx.provider.BaseDirs() == nil {
		return qctx.dir
	}

	mt, err := qctx.provider.BaseDirs().MountTimestamps()
	if err != nil {
		return qctx.dir
	}

	mountPaths := DecodeMountPaths(mt)

	for _, mountPath := range mountPaths {
		if strings.HasPrefix(qctx.dir, mountPath) {
			return mountPath
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
	report *perfreport.Report,
	qctx queryContext,
	opts QueryOptions,
	printf PrintfFunc,
) error {
	ops := buildOps(qctx, opts, printf)

	ops, err := selectOps(ops, opts.Ops)
	if err != nil {
		return err
	}

	for _, o := range ops {
		if err := runOp(report, qctx, o, opts, printf); err != nil {
			return err
		}
	}

	return nil
}

func buildOps(qctx queryContext, opts QueryOptions, printf PrintfFunc) []op {
	qctx.treeFilter = buildTreeFilter(qctx, opts)

	ops := []op{
		opStartupCacheWarmingAudit(),
		opMountTimestamps(qctx),
		opTreeWhereColdThenCached(qctx, opts.Splits),
		opTreeWhereColdProvider(qctx, opts.Splits),
		opTreeDiskTreeEndpointColdProvider(qctx),
		opTreeWhereProviderUpdateColdCache(qctx, opts.Splits),
		opTreeDiskTreeProviderUpdateColdCache(qctx),
	}

	if opts.WalkDepth > 0 && opts.WalkLimit > 0 {
		ops = append(ops, opTreeDiskTreeEndpointNewDirs(qctx, opts))
	}

	if opts.AncestorLimit > 0 {
		ops = append(ops, opTreeDiskTreeEndpointAncestorDirs(qctx, opts))
	}

	ops = append(ops,
		focusedQueryOps(qctx, opts)...,
	)

	ops = append(ops,
		opTreeDirInfo(qctx),
		opTreeDiskTreeEndpoint(qctx),
		opTreeDiskTreeEndpointVisibleChildDirs(qctx),
		opTreeWhere(qctx, opts.Splits),
		opTreeWhereFreshProvider(qctx, opts.Splits),
		opGroupUsage(qctx),
		opListDir(qctx),
	)

	ops = append(ops, opStatPath(qctx, printf)...)
	ops = append(ops, opPermission(qctx))

	return append(ops, globOps(qctx)...)
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
			MountPath: mountpath.DecodeKey(mountKey),
			UpdatedAt: updatedAt.UTC().Format(time.RFC3339Nano),
		})
	}

	slices.SortFunc(freshness, func(a, b activeMountFreshness) int {
		return cmp.Compare(a.MountPath, b.MountPath)
	})

	return freshness
}

func opTreeDirInfo(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	var resultCount uint64

	return op{
		name: queryOpTreeDirInfoName,
		inputs: treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
		}),
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().DirInfo(qctx.dir, filter)
			resultCount = dirInfoResultCount(info)

			return err
		},
		resultCount: func() uint64 { return resultCount },
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
	var resultCount uint64

	return op{
		name: queryOpFilesListDirName,
		inputs: map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameQueryClient,
			queryInputDurationSource: querySourceClickHouseLog,
		},
		run: func(ctx context.Context) error {
			rows, err := qctx.client.ListDir(ctx, qctx.dir, 0)
			resultCount = uint64(len(rows))

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opStatPath(qctx queryContext, printf PrintfFunc) []op {
	pickedPath := pickPath(qctx.client, qctx.dir)
	if pickedPath == "" {
		printf("query: %v\n", ErrEmptyDir)

		return nil
	}

	return []op{{
		name: queryOpFilesStatPathName,
		inputs: map[string]any{
			"path":                   pickedPath,
			queryInputCacheScope:     queryScopeSameQueryClient,
			queryInputDurationSource: querySourceClickHouseLog,
		},
		run: func(ctx context.Context) error {
			if err := qctx.client.StatPath(ctx, pickedPath); err != nil {
				return err
			}

			return nil
		},
		resultCount: func() uint64 { return 1 },
	}}
}

func opPermission(qctx queryContext) op {
	return op{
		name: "permission_check",
		inputs: map[string]any{
			queryInputDirKey: qctx.dir,
			"uid":            qctx.uid,
			"gids":           qctx.gids,
		},
		run: func(ctx context.Context) error {
			return qctx.client.PermissionAnyInDir(ctx, qctx.dir, qctx.uid, qctx.gids)
		},
	}
}

func globOps(qctx queryContext) []op {
	type globCase struct {
		name         string
		pattern      string
		requireOwner bool
	}

	ext := pickExt(qctx.client, qctx.dir)
	baseDirs := []string{qctx.dir}
	cases := []globCase{
		{name: "A", pattern: "*"},
		{name: "B", pattern: "*", requireOwner: true},
		{name: "C", pattern: "**"},
		{name: "D", pattern: "**", requireOwner: true},
	}

	if ext != "" {
		cases = append(cases,
			globCase{name: "E", pattern: "*." + ext},
			globCase{name: "F", pattern: "*." + ext, requireOwner: true},
			globCase{name: "G", pattern: "**/*." + ext},
			globCase{name: "H", pattern: "**/*." + ext, requireOwner: true},
		)
	}

	ops := make([]op, 0, len(cases))
	for _, c := range cases {
		ops = append(
			ops,
			globOp(qctx, baseDirs, c.name, []string{c.pattern}, c.requireOwner),
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
	var resultCount uint64

	return op{
		name: "glob_case_" + caseName,
		inputs: map[string]any{
			"patterns":               patterns,
			"require_owner":          requireOwner,
			queryInputCacheScope:     queryScopeSameQueryClient,
			queryInputDurationSource: querySourceClickHouseLog,
		},
		run: func(ctx context.Context) error {
			count, err := qctx.client.FindByGlob(
				ctx, baseDirs, patterns, requireOwner, qctx.uid, qctx.gids,
			)
			resultCount = intToUint64(count)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func runOp(
	report *perfreport.Report,
	qctx queryContext,
	o op,
	opts QueryOptions,
	printf PrintfFunc,
) error {
	warmup := opts.Warmup
	if o.skipWarmup {
		warmup = 0
	}

	repeat := opts.Repeat
	if o.prepare != nil {
		preparedRepeat, err := o.prepare(repeat)
		if err != nil {
			return fmt.Errorf("%s prepare: %w", o.name, err)
		}

		repeat = preparedRepeat
	}

	if o.hasRepeatOverride {
		repeat = o.repeatOverride
	}

	samples, err := timingLoop(qctx, o, warmup, repeat, printf)
	if err != nil {
		return err
	}

	durations := querySampleDurations(samples)
	report.AddOperationWithCounters(
		o.name,
		o.inputs,
		durations,
		querySampleReadRows(samples),
		querySampleReadBytes(samples),
		querySampleReadMarks(samples),
		querySampleResultCounts(samples, o),
	)

	p50, p95, p99 := perfreport.PercentilesMS(durations)
	printf("%s repeats=%d p50=%.3f p95=%.3f p99=%.3f ms\n",
		o.name, len(durations), p50, p95, p99)

	return nil
}

func timingLoop(
	qctx queryContext,
	o op,
	warmup int,
	repeat int,
	printf PrintfFunc,
) ([]queryRepeatSample, error) {
	ctx := context.Background()
	if err := warmupOp(ctx, o, warmup); err != nil {
		return nil, err
	}

	samples := make([]queryRepeatSample, 0, repeat)

	for i := range repeat {
		sample, err := timeOpRepeat(ctx, qctx, o, printf)
		if err != nil {
			return nil, fmt.Errorf("%s repeat %d/%d: %w", o.name, i+1, repeat, err)
		}

		samples = append(samples, sample)
	}

	return samples, nil
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
		"read_bytes=%d read_marks=%d result_rows=%d result_bytes=%d\n",
		name, m.DurationMs, m.ReadRows, m.ReadBytes,
		m.ReadMarks, m.ResultRows, m.ResultBytes)
}

type activeMountFreshness struct {
	MountPath string `json:"mount_path"`
	UpdatedAt string `json:"updated_at"`
}

type op struct {
	name              string
	inputs            map[string]any
	setup             func(ctx context.Context) error
	prepare           func(repeat int) (int, error)
	run               func(ctx context.Context) error
	teardown          func(ctx context.Context) error
	resultCount       func() uint64
	useWallTime       bool
	skipWarmup        bool
	hasRepeatOverride bool
	repeatOverride    int
}

type queryContext struct {
	provider         provider.Provider
	client           QueryClient
	inspector        QueryInspector
	openProvider     func() (provider.Provider, error)
	dir              string
	uid              uint32
	gids             []uint32
	treeFilter       *db.Filter
	resetQueryCaches func()
}

func (q *queryContext) close() error {
	return closeQueryResources(q.inspector, q.client, q.provider)
}

func (q queryContext) resetCaches() {
	if q.resetQueryCaches != nil {
		q.resetQueryCaches()
	}
}
