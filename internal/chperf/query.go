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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
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
	queryInputInfoCountFieldsKey          = "count_fields"
	queryInputPermissionChecksKey         = "permission_checks"
	queryInputPermissionPathKey           = "permission_path"
	queryInputParentDirKey                = "parent_dir"
	queryInputSplitsKey                   = "splits"
	queryInputStartDirKey                 = "start_dir"
	queryInputTreeFilterRouteKey          = "tree_filter_route"
	queryInputAllowedGIDsKey              = "allowed_gids"
	queryInputCacheHitKeysKey             = "cache_hit_keys"
	queryInputNoAuthFlagsKey              = "noauth_flags"
	queryInputStatusCodeKey               = "status_code"
	queryInputSurfaceKey                  = "surface"
	queryInputSurfaceInProcessEquivalent  = "in_process_equivalent"
	queryPermissionCheckAnyInDir          = "any_in_dir"
	queryPermissionCheckPath              = clickHouseFileFieldPath
	queryOpAuthTreeName                   = "auth_tree"
	queryOpAuthWhereRestrictedName        = "auth_where_restricted"
	queryOpFilesListDirName               = "files_listdir"
	queryOpInfoName                       = "info"
	queryOpNoAuthWhereName                = "noauth_where"
	queryOpPermissionCheckName            = "permission_check"
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
	navigationShapeParentFacts            = string(clickhouse.NavigationObjectParentFacts)
	navigationShapeChildFacts             = string(clickhouse.NavigationObjectChildFacts)
	navigationShapeProjection             = string(clickhouse.NavigationObjectProjection)
	navigationScenarioHighFanout          = "high_fanout_305_children"
	navigationScenarioFiltered            = "filtered_ageall_owner_type"
	navigationInputShape                  = "navigation_shape"
	navigationInputScenario               = "navigation_scenario"
	navigationInputExplainOutput          = "explain_indexes_1_output"
	navigationInputProjectionName         = "projection_name"
	navigationInputChildCount             = "high_fanout_child_count"
	navigationInputParentRangeReads       = "parent_range_reads"
	navigationInputAgeAllCompanionRead    = "ageall_companion_read"
	navigationInputResultDigest           = "result_digest"
	navigationMinHighFanoutChildren       = 305
	navigationChildFactsImprovement       = 0.15
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

func finalDirSummary(tree *db.Tree, current string, filter *db.Filter) bool {
	summary, err := tree.DirSummary(current, filter)

	return err != nil || summary == nil || representativeDirSummary(summary)
}

func representativeDirSummary(summary *db.DirSummary) bool {
	if summary == nil {
		return false
	}

	count := summary.Count

	return count >= dirPickMinCount && count <= dirPickMaxCount
}

func nextDirInfo(tree *db.Tree, current string, filter *db.Filter) (*db.DirInfo, bool) {
	info, err := tree.DirInfo(current, filter)
	if err != nil || missingDirInfo(info) {
		return nil, false
	}

	return info, true
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
			"initial_provider_readers_timing":          queryStartupStageSynchronousInitial,
			"server_basedirs_cache_timing":             queryStartupStageSynchronousInitial,
			"query_cache_warmup_timing":                queryStartupStageLazyInteraction,
			"provider_polling_timing":                  queryStartupStageBackgroundProvider,
			"provider_update_refresh_timing":           queryStartupStageBackgroundProvider,
			"filtered_where_gate_source":               queryOpTreeWhereColdProviderName,
			"filtered_where_gate_cache_scope":          queryScopeColdProvider,
			"filtered_where_gate_requires_cache_reset": true,
			"startup_warming_supports_gate_only":       true,
			"warmed_request_output_reuse":              "forbidden_for_cold_filtered_where_gate",
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
		resultCount:       func() uint64 { return 1 },
	}
}

func opInfo(qctx queryContext) op {
	var resultCounts []uint64

	inputs := map[string]any{
		queryInputDirKey:             qctx.dir,
		queryInputInfoCountFieldsKey: db.InfoCountFieldNames(),
		queryInputCacheScope:         queryScopeSameProviderDir,
		queryInputDurationSource:     querySourceClickHouseLog,
		queryInputStatusCodeKey:      200,
		queryInputSurfaceKey:         queryInputSurfaceInProcessEquivalent,
	}

	return op{
		name:   queryOpInfoName,
		inputs: inputs,
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().Info()
			if err != nil {
				return err
			}

			resultCounts = info.CountValues()
			inputs[navigationInputResultDigest] = digestValue(resultCounts)

			return nil
		},
		resultCounts: func() []uint64 { return slices.Clone(resultCounts) },
	}
}

func digestValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func opTreeWhereColdThenCached(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeSameProviderCold,
		queryInputDurationSource: querySourceWall,
		queryInputSplitsKey:      splits,
	})

	var resultCount uint64

	return op{
		name:   queryOpTreeWhereColdName,
		inputs: inputs,
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))
			inputs[navigationInputResultDigest] = dcssDigest(results)

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
	if filter == nil {
		return false
	}

	return len(filter.GIDs) > 0 || len(filter.UIDs) > 0 || filter.FT != 0
}

func treeFilterIndexGate(filter *db.Filter) string {
	if treeFilterAgeAllIndexEligible(filter) {
		return queryFilterIndexGatePerfRequired
	}

	return queryFilterIndexGateInapplicable
}

func dcssDigest(dcss db.DCSs) string {
	summaries := make([]*db.DirSummary, len(dcss))
	copy(summaries, dcss)

	return digestValue(digestSummaries(summaries))
}

func digestSummaries(summaries []*db.DirSummary) []digestDirSummary {
	out := make([]digestDirSummary, 0, len(summaries))
	for _, summary := range summaries {
		if summary != nil {
			out = append(out, digestSummary(summary))
		}
	}

	return out
}

func digestSummary(summary *db.DirSummary) digestDirSummary {
	if summary == nil {
		return digestDirSummary{}
	}

	return digestDirSummary{
		Dir:   summary.Dir,
		Count: summary.Count,
		Size:  summary.Size,
		UIDs:  slices.Clone(summary.UIDs),
		GIDs:  slices.Clone(summary.GIDs),
		FT:    uint16(summary.FT),
		Age:   uint8(summary.Age),
	}
}

func opTreeWhereColdProvider(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeColdProvider,
		queryInputDurationSource: querySourceWall,
		queryInputSplitsKey:      splits,
	})

	var resultCount uint64

	return op{
		name:   queryOpTreeWhereColdProviderName,
		inputs: inputs,
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			return nil
		},
		run: func(_ context.Context) error {
			count, err := runTreeWhereFreshProvider(qctx, splits, filter, inputs)
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
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeProviderUpdateCold,
		queryInputDurationSource: querySourceWall,
		queryInputSplitsKey:      splits,
	})

	var (
		p           provider.Provider
		resultCount uint64
	)

	return op{
		name:   queryOpTreeWhereProviderUpdateName,
		inputs: inputs,
		setup: func(_ context.Context) error {
			qctx.resetCaches()

			var err error

			p, err = openProviderForRepeat(qctx)

			return err
		},
		run: func(_ context.Context) error {
			results, err := p.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))
			inputs[navigationInputResultDigest] = dcssDigest(results)

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

func dirInfoDigest(info *db.DirInfo) string {
	if info == nil {
		return digestValue(nil)
	}

	return digestValue(struct {
		Current  digestDirSummary   `json:"current"`
		Children []digestDirSummary `json:"children"`
	}{
		Current:  digestSummary(info.Current),
		Children: digestSummaries(info.Children),
	})
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
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeSameProviderDir,
		queryInputDurationSource: querySourceClickHouseLog,
		queryInputSplitsKey:      splits,
	})

	var resultCount uint64

	return op{
		name:   queryOpTreeWhereName,
		inputs: inputs,
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			resultCount = uint64(len(results))
			inputs[navigationInputResultDigest] = dcssDigest(results)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opAuthTree(qctx queryContext) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputAllowedGIDsKey: slices.Clone(qctx.gids),
		queryInputCacheScope:     queryScopeSameProviderDir,
		queryInputDurationSource: querySourceClickHouseLog,
		queryInputStatusCodeKey:  200,
		queryInputSurfaceKey:     queryInputSurfaceInProcessEquivalent,
	})

	var resultCount uint64

	return op{
		name:   queryOpAuthTreeName,
		inputs: inputs,
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().DirInfo(qctx.dir, filter)
			if err != nil {
				return err
			}

			resultCount = dirInfoResultCount(info)
			inputs[navigationInputResultDigest] = dirInfoDigest(info)
			inputs[queryInputNoAuthFlagsKey] = dirInfoNoAuthFlags(info, qctx.gids)

			return nil
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func dirInfoNoAuthFlags(info *db.DirInfo, allowedGIDs []uint32) map[string]bool {
	if info == nil {
		return nil
	}

	flags := make(map[string]bool, 1+len(info.Children))
	addNoAuthFlag(flags, info.Current, allowedGIDs)

	for _, child := range info.Children {
		addNoAuthFlag(flags, child, allowedGIDs)
	}

	return flags
}

func addNoAuthFlag(flags map[string]bool, summary *db.DirSummary, allowedGIDs []uint32) {
	if summary == nil {
		return
	}

	flags[summary.Dir] = noAuthForGIDs(allowedGIDs, summary.GIDs)
}

func noAuthForGIDs(allowedGIDs []uint32, summaryGIDs []uint32) bool {
	if len(allowedGIDs) == 0 {
		return false
	}

	allowed := make(map[uint32]bool, len(allowedGIDs))
	for _, gid := range allowedGIDs {
		allowed[gid] = true
	}

	return !slices.ContainsFunc(summaryGIDs, func(gid uint32) bool {
		return allowed[gid]
	})
}

func opAuthWhereRestricted(qctx queryContext, splits int) op {
	filter := restrictedAuthWhereFilter(qctx.treeFilter, qctx.gids)

	return opWhereWithDigest(
		queryOpAuthWhereRestrictedName,
		qctx,
		filter,
		treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputAllowedGIDsKey: slices.Clone(qctx.gids),
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
			queryInputSplitsKey:      splits,
			queryInputStatusCodeKey:  200,
			queryInputSurfaceKey:     queryInputSurfaceInProcessEquivalent,
		}),
		splits,
	)
}

func restrictedAuthWhereFilter(filter *db.Filter, allowedGIDs []uint32) *db.Filter {
	restricted := treeFilterFromOptions(filter)
	if len(allowedGIDs) == 0 {
		return restricted
	}

	if len(restricted.GIDs) == 0 {
		restricted.GIDs = slices.Clone(allowedGIDs)

		return restricted
	}

	restricted.GIDs = intersectUint32s(restricted.GIDs, allowedGIDs)

	return restricted
}

func intersectUint32s(values []uint32, allowed []uint32) []uint32 {
	allowedSet := make(map[uint32]bool, len(allowed))
	for _, value := range allowed {
		allowedSet[value] = true
	}

	out := make([]uint32, 0, len(values))
	for _, value := range values {
		if allowedSet[value] {
			out = append(out, value)
		}
	}

	return out
}

func opWhereWithDigest(
	name string,
	qctx queryContext,
	filter *db.Filter,
	inputs map[string]any,
	splits int,
) op {
	var resultCount uint64

	return op{
		name:   name,
		inputs: inputs,
		run: func(_ context.Context) error {
			results, err := qctx.provider.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
			if err != nil {
				return err
			}

			resultCount = uint64(len(results))
			inputs[navigationInputResultDigest] = dcssDigest(results)

			return nil
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opNoAuthWhere(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)

	return opWhereWithDigest(
		queryOpNoAuthWhereName,
		qctx,
		filter,
		treeOpInputs(filter, map[string]any{
			queryInputDirKey:         qctx.dir,
			queryInputCacheScope:     queryScopeSameProviderDir,
			queryInputDurationSource: querySourceClickHouseLog,
			queryInputSplitsKey:      splits,
			queryInputStatusCodeKey:  200,
			queryInputSurfaceKey:     queryInputSurfaceInProcessEquivalent,
		}),
		splits,
	)
}

func opTreeWhereFreshProvider(qctx queryContext, splits int) op {
	filter := treeFilterFromOptions(qctx.treeFilter)
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeFreshProvider,
		queryInputDurationSource: querySourceWall,
		queryInputSplitsKey:      splits,
	})

	var resultCount uint64

	return op{
		name:   queryOpTreeWhereFreshName,
		inputs: inputs,
		run: func(_ context.Context) error {
			count, err := runTreeWhereFreshProvider(qctx, splits, filter, inputs)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
		useWallTime: true,
		skipWarmup:  true,
	}
}

func runTreeWhereFreshProvider(
	qctx queryContext,
	splits int,
	filter *db.Filter,
	inputs map[string]any,
) (uint64, error) {
	if qctx.openProvider == nil {
		return 0, errOpenProviderRequired
	}

	p, err := qctx.openProvider()
	if err != nil {
		return 0, err
	}

	results, whereErr := p.Tree().Where(qctx.dir, filter, split.SplitsToSplitFn(splits))
	if whereErr == nil && inputs != nil {
		inputs[navigationInputResultDigest] = dcssDigest(results)
	}

	closeErr := p.Close()

	return uint64(len(results)), errors.Join(whereErr, closeErr)
}

func permissionPathCandidate(client QueryClient, dir string) (string, permissionPathQueryClient, bool) {
	path := pickPath(client, dir)
	pathClient, ok := client.(permissionPathQueryClient)

	return path, pathClient, ok && path != ""
}

func runPermissionChecks(
	ctx context.Context,
	qctx queryContext,
	pathClient permissionPathQueryClient,
	path string,
	checkPath bool,
) (uint64, error) {
	if err := qctx.client.PermissionAnyInDir(ctx, qctx.dir, qctx.uid, qctx.gids); err != nil {
		return 0, err
	}

	if !checkPath {
		return 1, nil
	}

	if err := pathClient.PermissionPath(ctx, path, qctx.uid, qctx.gids); err != nil {
		return 0, err
	}

	return 2, nil
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

func querySampleMemoryBytes(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.MemoryBytes })
	if !ok {
		return nil
	}

	return values
}

func querySampleResultBytes(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.ResultBytes })
	if !ok {
		return nil
	}

	return values
}

func querySampleResultCounts(samples []queryRepeatSample, o op) []uint64 {
	if o.resultCounts != nil {
		return o.resultCounts()
	}

	if o.resultCount == nil {
		return querySampleResultRows(samples)
	}

	counts := make([]uint64, len(samples))
	for i, sample := range samples {
		counts[i] = sample.resultCount
	}

	return counts
}

func querySampleResultRows(samples []queryRepeatSample) []uint64 {
	values, ok := querySampleMetricValues(samples, func(m *QueryMetrics) uint64 { return m.ResultRows })
	if !ok {
		return nil
	}

	return values
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

type digestDirSummary struct {
	Dir   string   `json:"dir"`
	Count uint64   `json:"count"`
	Size  uint64   `json:"size"`
	UIDs  []uint32 `json:"uids"`
	GIDs  []uint32 `json:"gids"`
	FT    uint16   `json:"ft"`
	Age   uint8    `json:"age"`
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

// NavigationDecisionEvidence contains the query/import report evidence used by
// the C1 bounded navigation decision gate.
type NavigationDecisionEvidence struct {
	ImportReports []perfreport.Report
	QueryReports  []perfreport.Report
}

func navigationProjectionPasses(e NavigationDecisionEvidence) bool {
	if !navigationProjectionExplainPasses(e) {
		return false
	}

	return navigationCandidateAtLeastAsFast(e, navigationShapeProjection, navigationScenarioHighFanout) &&
		navigationCandidateAtLeastAsFast(e, navigationShapeProjection, navigationScenarioFiltered)
}

func navigationProjectionExplainPasses(e NavigationDecisionEvidence) bool {
	return navigationProjectionExplainPassesScenario(e, navigationScenarioHighFanout) &&
		navigationProjectionExplainPassesScenario(e, navigationScenarioFiltered)
}

func navigationProjectionExplainPassesScenario(e NavigationDecisionEvidence, scenario string) bool {
	op, ok := navigationCandidateOperation(e.QueryReports, navigationShapeProjection, scenario)
	if !ok {
		return false
	}

	return ExplainUsesProjectionPruning(
		stringInput(op.Inputs, navigationInputExplainOutput),
		stringInput(op.Inputs, navigationInputProjectionName),
	)
}

func navigationCandidateOperation(
	reports []perfreport.Report,
	shape string,
	scenario string,
) (perfreport.Operation, bool) {
	for _, report := range reports {
		for _, op := range report.Operations {
			if navigationCandidateMatches(op, shape, scenario) {
				return op, true
			}
		}
	}

	return perfreport.Operation{}, false
}

func navigationCandidateMatches(op perfreport.Operation, shape string, scenario string) bool {
	return navigationShape(op) == shape && navigationScenario(op) == scenario
}

func navigationShape(op perfreport.Operation) string {
	return stringInput(op.Inputs, navigationInputShape)
}

func navigationScenario(op perfreport.Operation) string {
	return stringInput(op.Inputs, navigationInputScenario)
}

// ExplainUsesProjectionPruning reports whether EXPLAIN indexes = 1 output
// names the projection and shows mount/parent pruning.
func ExplainUsesProjectionPruning(explain string, projectionName string) bool {
	if !explainIndexes1Output(explain) || !ExplainHasPruning(explain) {
		return false
	}

	normalised := strings.ToLower(explain)
	if !explainNamesProjection(normalised) {
		return false
	}

	projectionName = strings.ToLower(strings.TrimSpace(projectionName))
	if projectionName == "" {
		return false
	}

	return strings.Contains(normalised, projectionName)
}

func explainIndexes1Output(explain string) bool {
	normalised := strings.ToLower(strings.Join(strings.Fields(explain), " "))

	return strings.Contains(normalised, "explain indexes = 1") ||
		strings.Contains(normalised, "explain indexes=1")
}

func explainNamesProjection(explain string) bool {
	return strings.Contains(explain, "projection")
}

func navigationCandidateAtLeastAsFast(
	e NavigationDecisionEvidence,
	shape string,
	scenario string,
) bool {
	candidate := navigationCandidateP95(e, shape, scenario)
	parent := navigationCandidateP95(e, navigationShapeParentFacts, scenario)

	return candidate > 0 && parent > 0 && candidate <= parent
}

func navigationChildFactsPasses(e NavigationDecisionEvidence) bool {
	return navigationChildFactsSpeedPasses(e) &&
		navigationChildFactsResultsMatch(e) &&
		navigationChildFactsReadShapePasses(e) &&
		navigationChildFactsFilteredAgeAllPasses(e) &&
		navigationImportGatesPass(e.ImportReports)
}

func navigationImportGatesPass(reports []perfreport.Report) bool {
	if len(reports) == 0 {
		return false
	}

	for _, report := range reports {
		if !navigationImportReportPasses(report) {
			return false
		}
	}

	return true
}

func navigationImportReportPasses(report perfreport.Report) bool {
	return report.MaxRSSBytes > 0 &&
		report.MaxRSSBytes <= finalGateT283ImportRSSBytes &&
		navigationRowAmplificationPasses(report)
}

func navigationRowAmplificationPasses(report perfreport.Report) bool {
	total, ok := firstOperation(report, "import_total", nil)
	if !ok {
		return false
	}

	records := uint64Input(total.Inputs, importInputRecords)
	if records == 0 {
		return false
	}

	stats, ok := report.TableStats[navigationShapeChildFacts]
	if !ok || stats.Rows == 0 {
		return false
	}

	return float64(stats.Rows)/float64(records) <= 1.70059
}

func navigationChildFactsSpeedPasses(e NavigationDecisionEvidence) bool {
	return navigationCandidateBeatsParentBy(
		e,
		navigationShapeChildFacts,
		navigationScenarioHighFanout,
		navigationChildFactsImprovement,
	) &&
		navigationCandidateBeatsParentBy(
			e,
			navigationShapeChildFacts,
			navigationScenarioFiltered,
			navigationChildFactsImprovement,
		)
}

func navigationCandidateBeatsParentBy(
	e NavigationDecisionEvidence,
	shape string,
	scenario string,
	improvement float64,
) bool {
	candidate := navigationCandidateP95(e, shape, scenario)
	parent := navigationCandidateP95(e, navigationShapeParentFacts, scenario)

	return candidate > 0 && parent > 0 && candidate <= parent*(1-improvement)
}

func navigationChildFactsResultsMatch(e NavigationDecisionEvidence) bool {
	for _, scenario := range []string{navigationScenarioHighFanout, navigationScenarioFiltered} {
		parent, parentOK := navigationCandidateOperation(e.QueryReports, navigationShapeParentFacts, scenario)

		child, childOK := navigationCandidateOperation(e.QueryReports, navigationShapeChildFacts, scenario)
		if !parentOK || !childOK || !navigationResultDigestMatches(parent, child) {
			return false
		}
	}

	return true
}

func navigationResultDigestMatches(a, b perfreport.Operation) bool {
	aDigest := stringInput(a.Inputs, navigationInputResultDigest)
	bDigest := stringInput(b.Inputs, navigationInputResultDigest)

	return aDigest != "" && aDigest == bDigest && resultCountsStable(a) && resultCountsStable(b)
}

func navigationChildFactsReadShapePasses(e NavigationDecisionEvidence) bool {
	for _, scenario := range []string{navigationScenarioHighFanout, navigationScenarioFiltered} {
		op, ok := navigationCandidateOperation(e.QueryReports, navigationShapeChildFacts, scenario)
		if !ok || !navigationReadShapePasses(op, scenario) {
			return false
		}
	}

	return true
}

func navigationReadShapePasses(op perfreport.Operation, scenario string) bool {
	reads := uint64Input(op.Inputs, navigationInputParentRangeReads)
	if reads == 1 {
		return true
	}

	if scenario != navigationScenarioFiltered {
		return false
	}

	if stringInput(op.Inputs, navigationInputAgeAllCompanionRead) == "" {
		return false
	}

	return navigationOperationHasFilteredAgeAllOwnerOrTypePredicate(op)
}

func navigationOperationHasFilteredAgeAllOwnerOrTypePredicate(op perfreport.Operation) bool {
	if uint64Input(op.Inputs, queryInputAgeKey) != uint64(db.DGUTAgeAll) {
		return false
	}

	ft, ok := uint16Input(op.Inputs, queryInputFilterFileTypeMaskKey)
	if !ok {
		return false
	}

	return treeFilterHasOwnerOrTypePredicate(&db.Filter{
		GIDs: uint32SliceInput(op.Inputs, queryInputFilterGIDsKey),
		UIDs: uint32SliceInput(op.Inputs, queryInputFilterUIDsKey),
		FT:   db.DirGUTAFileType(ft),
		Age:  db.DGUTAgeAll,
	})
}

func uint16Input(inputs map[string]any, key string) (uint16, bool) {
	value := uint64Input(inputs, key)
	if value > uint64(^uint16(0)) {
		return 0, false
	}

	return uint16(value), true
}

func uint32SliceInput(inputs map[string]any, key string) []uint32 {
	v, ok := inputs[key]
	if !ok {
		return nil
	}

	switch typed := v.(type) {
	case []uint32:
		return typed
	case []uint64:
		return uint64SliceToUint32(typed)
	case []any:
		return anySliceToUint32(typed)
	default:
		return nil
	}
}

func uint64SliceToUint32(values []uint64) []uint32 {
	converted := make([]uint32, 0, len(values))
	for _, value := range values {
		if value <= uint64(^uint32(0)) {
			converted = append(converted, uint32(value))
		}
	}

	return converted
}

func anySliceToUint32(values []any) []uint32 {
	converted := make([]uint32, 0, len(values))
	for _, value := range values {
		if parsed := uint64InputValue(value); parsed <= uint64(^uint32(0)) {
			converted = append(converted, uint32(parsed))
		}
	}

	return converted
}

func navigationChildFactsFilteredAgeAllPasses(e NavigationDecisionEvidence) bool {
	op, ok := navigationCandidateOperation(e.QueryReports, navigationShapeChildFacts, navigationScenarioFiltered)
	if !ok {
		return false
	}

	return navigationOperationHasFilteredAgeAllOwnerOrTypePredicate(op)
}

func navigationSelectedObject(
	e NavigationDecisionEvidence,
	projectionPass bool,
	childFactsPass bool,
) string {
	if projectionPass && childFactsPass {
		if navigationChildFactsPreferred(e) {
			return navigationShapeChildFacts
		}
	}

	return string(clickhouse.ChooseNavigationObject(projectionPass, childFactsPass))
}

func navigationChildFactsPreferred(e NavigationDecisionEvidence) bool {
	childP95 := navigationCandidateP95(e, navigationShapeChildFacts, navigationScenarioFiltered)

	projectionP95 := navigationCandidateP95(e, navigationShapeProjection, navigationScenarioFiltered)

	return childP95 < projectionP95
}

func navigationCandidateP95(
	e NavigationDecisionEvidence,
	shape string,
	scenario string,
) float64 {
	op, ok := navigationCandidateOperation(e.QueryReports, shape, scenario)
	if !ok {
		return 0
	}

	return op.P95MS
}

func navigationCandidateScenarioComplete(
	e NavigationDecisionEvidence,
	shape string,
	scenario string,
) bool {
	op, ok := navigationCandidateOperation(e.QueryReports, shape, scenario)

	return ok && navigationCandidateOperationComplete(op, scenario)
}

func navigationCandidateOperationComplete(op perfreport.Operation, scenario string) bool {
	return op.P50MS > 0 &&
		op.P95MS > 0 &&
		op.P99MS > 0 &&
		navigationCandidateCountersComplete(op) &&
		explainIndexes1Output(stringInput(op.Inputs, navigationInputExplainOutput)) &&
		navigationHighFanoutInputPasses(op, scenario)
}

func navigationCandidateCountersComplete(op perfreport.Operation) bool {
	return len(op.ReadRows) > 0 &&
		len(op.ReadBytes) > 0 &&
		len(op.ReadMarks) > 0 &&
		len(op.ResultCount) > 0
}

func navigationHighFanoutInputPasses(op perfreport.Operation, scenario string) bool {
	if scenario != navigationScenarioHighFanout {
		return true
	}

	return uint64Input(op.Inputs, navigationInputChildCount) >= navigationMinHighFanoutChildren
}

// NavigationDecisionCheck captures one C1 acceptance result.
type NavigationDecisionCheck struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

func validateNavigationCandidateReport(e NavigationDecisionEvidence) NavigationDecisionCheck {
	check := navigationDecisionCheck(1, "candidate report evidence")

	for _, shape := range []string{
		navigationShapeParentFacts,
		navigationShapeChildFacts,
		navigationShapeProjection,
	} {
		for _, scenario := range []string{navigationScenarioHighFanout, navigationScenarioFiltered} {
			if !navigationCandidateScenarioComplete(e, shape, scenario) {
				return check.fail(shape + " is missing complete " + scenario + " query evidence")
			}
		}
	}

	return check.pass("all navigation candidates include percentiles, counters, results, and EXPLAIN output")
}

func validateNavigationProjection(
	e NavigationDecisionEvidence,
	projectionPass bool,
) NavigationDecisionCheck {
	check := navigationDecisionCheck(2, "projection evidence")
	if projectionPass {
		return check.pass("projection broad and filtered endpoint evidence passed")
	}

	if !navigationProjectionExplainPasses(e) {
		return check.fail("projection was rejected because EXPLAIN did not prove projection pruning")
	}

	return check.fail("projection was rejected because it did not beat parent facts")
}

func validateNavigationChildFacts(
	e NavigationDecisionEvidence,
	childFactsPass bool,
) NavigationDecisionCheck {
	check := navigationDecisionCheck(3, "child facts evidence")
	if childFactsPass {
		return check.pass("child facts passed speed, exact result, parent-range, and import gates")
	}

	if !navigationChildFactsResultsMatch(e) {
		return check.fail("child facts were rejected because filtered results were not exact")
	}

	if !navigationChildFactsReadShapePasses(e) {
		return check.fail("child facts were rejected because read-shape evidence was missing")
	}

	if !navigationChildFactsFilteredAgeAllPasses(e) {
		return check.fail("child facts were rejected because filtered AgeAll owner/type evidence was missing")
	}

	if !navigationImportGatesPass(e.ImportReports) {
		return check.fail("child facts were rejected because import memory or row amplification evidence failed")
	}

	return check.fail("child facts were rejected because p95 did not improve on parent facts by 15%")
}

func validateNavigationParentDefault(selected string) NavigationDecisionCheck {
	check := navigationDecisionCheck(4, "parent facts default")
	if selected == navigationShapeParentFacts {
		return check.pass("wrstat_parent_facts is the implemented object")
	}

	return check.fail("wrstat_parent_facts was not selected")
}

func navigationDecisionCheck(id int, name string) NavigationDecisionCheck {
	return NavigationDecisionCheck{ID: id, Name: name}
}

func (c NavigationDecisionCheck) pass(detail string) NavigationDecisionCheck {
	c.Passed = true
	c.Detail = detail

	return c
}

func (c NavigationDecisionCheck) fail(detail string) NavigationDecisionCheck {
	c.Passed = false
	c.Detail = detail

	return c
}

// NavigationDecisionResult describes the selected Disktree navigation object.
type NavigationDecisionResult struct {
	SelectedObject string                    `json:"selected_object"`
	Checks         []NavigationDecisionCheck `json:"checks"`
}

// ValidateNavigationDecisionGate applies the C1 evidence rules. The default is
// wrstat_parent_facts unless projection or child facts satisfy every required
// proof item.
func ValidateNavigationDecisionGate(e NavigationDecisionEvidence) NavigationDecisionResult {
	projectionPass := navigationProjectionPasses(e)
	childFactsPass := navigationChildFactsPasses(e)
	selected := navigationSelectedObject(e, projectionPass, childFactsPass)

	return NavigationDecisionResult{
		SelectedObject: selected,
		Checks: []NavigationDecisionCheck{
			validateNavigationCandidateReport(e),
			validateNavigationProjection(e, projectionPass),
			validateNavigationChildFacts(e, childFactsPass),
			validateNavigationParentDefault(selected),
		},
	}
}

type permissionPathQueryClient interface {
	PermissionPath(ctx context.Context, path string, uid uint32, gids []uint32) error
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
	if finalDirSummary(tree, current, filter) {
		return current, true
	}

	info, ok := nextDirInfo(tree, current, filter)
	if !ok || representativeDirInfo(info) {
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
		opInfo(qctx),
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
		opAuthTree(qctx),
		opAuthWhereRestricted(qctx, opts.Splits),
		opNoAuthWhere(qctx, opts.Splits),
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

	var resultCount uint64

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
			resultCount = uint64(len(ts))

			return nil
		},
		resultCount: func() uint64 { return resultCount },
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
	inputs := treeOpInputs(filter, map[string]any{
		queryInputDirKey:         qctx.dir,
		queryInputCacheScope:     queryScopeSameProviderDir,
		queryInputDurationSource: querySourceClickHouseLog,
	})

	var resultCount uint64

	return op{
		name:   queryOpTreeDirInfoName,
		inputs: inputs,
		run: func(_ context.Context) error {
			info, err := qctx.provider.Tree().DirInfo(qctx.dir, filter)
			resultCount = dirInfoResultCount(info)
			inputs[navigationInputResultDigest] = dirInfoDigest(info)

			return err
		},
		resultCount: func() uint64 { return resultCount },
	}
}

func opGroupUsage(qctx queryContext) op {
	var resultCount uint64

	return op{
		name:   "basedirs_group_usage",
		inputs: map[string]any{},
		run: func(_ context.Context) error {
			rows, err := qctx.provider.BaseDirs().GroupUsage(db.DGUTAgeAll)
			resultCount = uint64(len(rows))

			return err
		},
		resultCount: func() uint64 { return resultCount },
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
			clickHouseFileFieldPath:  pickedPath,
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
	inputs := map[string]any{
		queryInputDirKey:         qctx.dir,
		"uid":                    qctx.uid,
		"gids":                   qctx.gids,
		queryInputCacheScope:     queryScopeSameQueryClient,
		queryInputDurationSource: querySourceClickHouseLog,
	}
	path, pathClient, hasPermissionPath := permissionPathCandidate(qctx.client, qctx.dir)
	checks := []string{queryPermissionCheckAnyInDir}

	if hasPermissionPath {
		inputs[queryInputPermissionPathKey] = path

		checks = append(checks, queryPermissionCheckPath)
	}

	inputs[queryInputPermissionChecksKey] = checks

	var resultCount uint64

	return op{
		name:   queryOpPermissionCheckName,
		inputs: inputs,
		run: func(ctx context.Context) error {
			count, err := runPermissionChecks(ctx, qctx, pathClient, path, hasPermissionPath)
			resultCount = count

			return err
		},
		resultCount: func() uint64 { return resultCount },
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
	report.AddOperationWithFullCounters(
		o.name,
		o.inputs,
		durations,
		querySampleReadRows(samples),
		querySampleReadBytes(samples),
		querySampleReadMarks(samples),
		querySampleMemoryBytes(samples),
		querySampleResultBytes(samples),
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
		"read_bytes=%d read_marks=%d memory_bytes=%d result_rows=%d result_bytes=%d\n",
		name, m.DurationMs, m.ReadRows, m.ReadBytes,
		m.ReadMarks, m.MemoryBytes, m.ResultRows, m.ResultBytes)
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
	resultCounts      func() []uint64
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

func cacheHitKeysHaveE2Scope(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	for _, key := range keys {
		if !cacheHitKeyHasE2Scope(key) {
			return false
		}
	}

	return true
}

func cacheHitKeyHasE2Scope(key string) bool {
	return strings.Contains(key, "path=") &&
		strings.Contains(key, "filter=") &&
		strings.Contains(key, "active_set_id=") &&
		strings.Contains(key, "query_version=")
}
