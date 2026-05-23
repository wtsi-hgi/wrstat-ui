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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	defaultDirPerm       = 0o755
	summariseDBBatchSize = 10000

	dgutaDBsSuffix    = "dguta.dbs"
	basedirsBasename  = "basedirs.db"
	statsGZBasename   = "stats.gz"
	lineReaderBufSize = 32 * 1024

	dirPickMinCount      = 1000
	dirPickMaxCount      = 20000
	dirPickMaxIterations = 128

	queryInputAgeKey           = "age"
	queryInputBaseDirKey       = "basedir"
	queryInputCacheScope       = "cache_scope"
	queryInputDirKey           = "dir"
	queryInputDurationSource   = "duration_source"
	queryInputSplitsKey        = "splits"
	queryOpTreeDiskTreeAncName = "tree_disktree_endpoint_ancestor_dirs"
	queryOpTreeDiskTreeEndName = "tree_disktree_endpoint"
	queryOpTreeDiskTreeNewName = "tree_disktree_endpoint_new_dirs"
	queryOpTreeDirInfoName     = "tree_dirinfo"
	queryOpTreeWhereColdName   = "tree_where_cold_then_cached"
	queryOpTreeWhereFreshName  = "tree_where_fresh_provider"
	queryOpTreeWhereName       = "tree_where"
	queryScopeAncestorDirs     = "ancestor_directory_each_repeat"
	queryScopeFreshProvider    = "fresh_provider_per_repeat"
	queryScopeNewDir           = "new_directory_each_repeat"
	queryScopeSameProviderCold = "same_provider_cold_then_warm"
	queryScopeSameProvider     = "same_provider_same_dir"
	querySourceWall            = "wall"
)

var (
	// ErrUnknownBackend indicates an unsupported backend name was provided.
	ErrUnknownBackend = errors.New("unknown backend")
	// ErrNoDatasets indicates no matching dataset directories were discovered.
	ErrNoDatasets = errors.New("no datasets found")
	// ErrNewBaseDirsStoreRequired indicates the import options must supply NewBaseDirsStore.
	ErrNewBaseDirsStoreRequired = errors.New("NewBaseDirsStore is required")
	// ErrNewDGUTAWriterRequired indicates the import options must supply NewDGUTAWriter.
	ErrNewDGUTAWriterRequired = errors.New("NewDGUTAWriter is required")
	// ErrOpenDatabaseRequired indicates the query options must supply OpenDatabase.
	ErrOpenDatabaseRequired = errors.New("OpenDatabase is required")
	// ErrOpenMultiBaseDirsReaderRequired indicates the query options must supply OpenMultiBaseDirsReader.
	ErrOpenMultiBaseDirsReaderRequired = errors.New("OpenMultiBaseDirsReader is required")

	errUnknownQueryOps = errors.New("unknown query ops")
)

// PrintfFunc matches fmt.Printf-style output and is used by the harness
// to emit human-readable timing summaries.
type PrintfFunc func(string, ...any)

// Import runs the in-process import harness over all discovered datasets in
// inputDir and writes a JSON report to opts.JSONOut.
func Import(inputDir string, opts ImportOptions, printf PrintfFunc) error {
	if printf == nil {
		printf = func(string, ...any) {}
	}

	if err := validateBackend(opts.Backend); err != nil {
		return err
	}

	datasetDirs, err := findDatasetDirs(inputDir, statsGZBasename)
	if err != nil {
		return err
	}

	report := NewReport(opts.Backend, inputDir, opts.Repeat, opts.Warmup)
	startAll := time.Now()

	totalRecords, err := importDatasets(datasetDirs, opts, printf)
	if err != nil {
		return err
	}

	report.AddOperation(
		"import_total",
		map[string]any{"datasets": len(datasetDirs), "records": totalRecords},
		[]float64{durationMS(time.Since(startAll))},
	)

	return WriteReport(opts.JSONOut, report)
}

func validateBackend(backend string) error {
	switch backend {
	case "bolt", "bolt_interfaces":
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnknownBackend, backend)
	}
}

func findDatasetDirs(baseDir string, required ...string) ([]string, error) {
	dirs, err := datasets.FindLatestDatasetDirs(baseDir, required...)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNoDatasets, baseDir)
	}

	sort.Strings(dirs)

	return dirs, nil
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func importDatasets(datasetDirs []string, opts ImportOptions, printf PrintfFunc) (uint64, error) {
	var total uint64

	for _, datasetDir := range datasetDirs {
		records, err := importDataset(datasetDir, opts, printf)
		if err != nil {
			return 0, err
		}

		total += records
	}

	return total, nil
}

func importDataset(datasetDir string, opts ImportOptions, printf PrintfFunc) (uint64, error) {
	base := filepath.Base(datasetDir)
	statsGZPath := filepath.Join(datasetDir, statsGZBasename)

	st, err := os.Stat(statsGZPath)
	if err != nil {
		return 0, err
	}

	outDatasetDir := filepath.Join(opts.OutDir, base)
	if mkErr := os.MkdirAll(outDatasetDir, defaultDirPerm); mkErr != nil {
		return 0, mkErr
	}

	spec := importSpec{
		statsGZPath:    statsGZPath,
		basedirsDBPath: filepath.Join(outDatasetDir, basedirsBasename),
		dgutaDBDir:     filepath.Join(outDatasetDir, dgutaDBsSuffix),
		modtime:        st.ModTime(),
		maxLines:       opts.MaxLines,
	}

	start := time.Now()

	records, err := importOneDataset(spec, opts)
	if err != nil {
		return 0, err
	}

	printf("import dataset=%s records=%d seconds=%.3f\n", base, records, time.Since(start).Seconds())

	return records, nil
}

func importOneDataset(spec importSpec, opts ImportOptions) (_ uint64, err error) {
	gz, closeStatsFn, err := openStatsGZReader(spec.statsGZPath)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := closeStatsFn(); err == nil {
			err = cerr
		}
	}()

	lr := newLineCountingReader(gz, spec.maxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	closeSummFn, err := addSummarisers(
		ss,
		importTargets{basedirsDBPath: spec.basedirsDBPath, dgutaDBDir: spec.dgutaDBDir},
		spec.modtime,
		opts,
	)
	if err != nil {
		return 0, err
	}

	if closeSummFn != nil {
		defer func() {
			if cerr := closeSummFn(); err == nil {
				err = cerr
			}
		}()
	}

	if err := ss.Summarise(); err != nil {
		return 0, err
	}

	return lr.linesRead(), nil
}

func openStatsGZReader(path string) (*pgzip.Reader, func() error, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	gz, err := pgzip.NewReader(fh)
	if err != nil {
		_ = fh.Close()

		return nil, nil, err
	}

	closeFn := func() error {
		gzErr := gz.Close()
		fhErr := fh.Close()

		return errors.Join(gzErr, fhErr)
	}

	return gz, closeFn, nil
}

func newLineCountingReader(r io.Reader, maxLines int) *lineCountingReader {
	var ml uint64
	if maxLines > 0 {
		ml = uint64(maxLines)
	}

	return &lineCountingReader{
		underlying: r,
		maxLines:   ml,
		buf:        make([]byte, lineReaderBufSize),
	}
}

func addSummarisers(
	ss *summary.Summariser,
	targets importTargets,
	modtime time.Time,
	opts ImportOptions,
) (func() error, error) {
	store, err := addBasedirsSummariser(ss, targets.basedirsDBPath, modtime, opts)
	if err != nil {
		return nil, err
	}

	writer, err := addDGUTASummariser(ss, targets.dgutaDBDir, modtime, opts)
	if err != nil {
		_ = store.Close()

		return nil, err
	}

	return func() error {
		return errors.Join(store.Close(), writer.Close())
	}, nil
}

func addBasedirsSummariser(
	ss *summary.Summariser,
	basedirsDBPath string,
	modtime time.Time,
	opts ImportOptions,
) (basedirs.Store, error) {
	store, bd, shouldOutput, err := buildBasedirsSummariser(basedirsDBPath, modtime, opts)
	if err != nil {
		return nil, err
	}

	ss.AddDirectoryOperation(sbasedirs.NewBaseDirs(shouldOutput, bd))

	return store, nil
}

func buildBasedirsSummariser(
	basedirsDBPath string,
	modtime time.Time,
	opts ImportOptions,
) (basedirs.Store, *basedirs.BaseDirs, func(*summary.DirectoryPath) bool, error) {
	quotas, shouldOutput, mps, err := parseBasedirsInputs(opts)
	if err != nil {
		return nil, nil, nil, err
	}

	store, err := openBasedirsStoreForImport(basedirsDBPath, modtime, opts)
	if err != nil {
		return nil, nil, nil, err
	}

	bd, err := createBaseDirsCreatorForImport(store, quotas, mps, modtime)
	if err != nil {
		_ = store.Close()

		return nil, nil, nil, err
	}

	return store, bd, shouldOutput, nil
}

func parseBasedirsInputs(
	opts ImportOptions,
) (*basedirs.Quotas, func(*summary.DirectoryPath) bool, []string, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(opts.Quota, opts.Config)
	if err != nil {
		return nil, nil, nil, err
	}

	mps, err := summariseutil.ParseMountpointsFromFile(opts.Mounts)
	if err != nil {
		return nil, nil, nil, err
	}

	return quotas, config.PathShouldOutput, mps, nil
}

func openBasedirsStoreForImport(basedirsDBPath string, modtime time.Time, opts ImportOptions) (basedirs.Store, error) {
	if opts.NewBaseDirsStore == nil {
		return nil, ErrNewBaseDirsStoreRequired
	}

	removeErr := os.Remove(basedirsDBPath)
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, removeErr
	}

	store, err := opts.NewBaseDirsStore(basedirsDBPath, "")
	if err != nil {
		return nil, err
	}

	mountPath := summariseutil.DeriveMountPathFromOutputDir(basedirsDBPath)
	store.SetMountPath(mountPath)
	store.SetUpdatedAt(modtime)

	return store, nil
}

func createBaseDirsCreatorForImport(
	store basedirs.Store,
	quotas *basedirs.Quotas,
	mountpoints []string,
	modtime time.Time,
) (*basedirs.BaseDirs, error) {
	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		return nil, err
	}

	if len(mountpoints) > 0 {
		bd.SetMountPoints(mountpoints)
	}

	bd.SetModTime(modtime)

	return bd, nil
}

func addDGUTASummariser(
	ss *summary.Summariser,
	dgutaDBDir string,
	modtime time.Time,
	opts ImportOptions,
) (db.DGUTAWriter, error) {
	if opts.NewDGUTAWriter == nil {
		return nil, ErrNewDGUTAWriterRequired
	}

	removeErr := os.RemoveAll(dgutaDBDir)
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, removeErr
	}

	if mkErr := os.MkdirAll(dgutaDBDir, defaultDirPerm); mkErr != nil {
		return nil, mkErr
	}

	writer, err := opts.NewDGUTAWriter(dgutaDBDir)
	if err != nil {
		return nil, err
	}

	writer.SetMountPath(summariseutil.DeriveMountPathFromOutputDir(dgutaDBDir))
	writer.SetUpdatedAt(modtime)
	writer.SetBatchSize(summariseDBBatchSize)
	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))

	return writer, nil
}

// Query runs the in-process query timing harness against Bolt DBs discovered
// under inputDir and writes a JSON report to opts.JSONOut.
func Query(inputDir string, opts QueryOptions, printf PrintfFunc) (err error) {
	if printf == nil {
		printf = func(string, ...any) {}
	}

	if validateErr := validateBackend(opts.Backend); validateErr != nil {
		return validateErr
	}

	ctx, err := buildQueryContext(inputDir, opts, printf)
	if err != nil {
		return err
	}

	defer func() {
		if cerr := ctx.closeFn(); err == nil {
			err = cerr
		}
	}()

	report := NewReport(opts.Backend, inputDir, opts.Repeat, opts.Warmup)

	if err := runQuerySuite(&report, ctx, opts, printf); err != nil {
		return err
	}

	return WriteReport(opts.JSONOut, report)
}

func buildQueryContext(inputDir string, opts QueryOptions, printf PrintfFunc) (queryContext, error) {
	datasetDirs, datasetDir, err := discoverQueryDatasets(inputDir, printf)
	if err != nil {
		return queryContext{}, err
	}

	mountPath, err := DeriveMountPathFromDatasetDirName(filepath.Base(datasetDir))
	if err != nil {
		return queryContext{}, err
	}

	tree, mr, closeFn, err := openQueryDBs(opts, datasetDir, opts.Owners)
	if err != nil {
		return queryContext{}, err
	}

	queryDir := resolveQueryDir(tree, mountPath, opts.Dir)
	if strings.TrimSpace(opts.Dir) == "" {
		printf("query: auto-selected dir=%s\n", queryDir)
	}

	prepErr := prepareMultiReader(mr, opts.Mounts)
	if prepErr != nil {
		return queryContext{}, closeAndJoinErr(closeFn, prepErr)
	}

	ids, err := pickRepresentativeIDs(mr, mountPath)
	if err != nil {
		return queryContext{}, closeAndJoinErr(closeFn, err)
	}

	return queryContext{
		datasetDirs: datasetDirs,
		tree:        tree,
		mr:          mr,
		closeFn:     closeFn,
		openFreshTree: func() (*db.Tree, func() error, error) {
			return openFreshQueryTree(opts, datasetDir)
		},
		queryDir: queryDir,
		ids:      ids,
	}, nil
}

func openQueryDBs(
	opts QueryOptions,
	datasetDir string,
	ownersPath string,
) (*db.Tree, basedirs.Reader, func() error, error) {
	if opts.OpenDatabase == nil {
		return nil, nil, nil, ErrOpenDatabaseRequired
	}

	if opts.OpenMultiBaseDirsReader == nil {
		return nil, nil, nil, ErrOpenMultiBaseDirsReaderRequired
	}

	dgutaPath := filepath.Join(datasetDir, dgutaDBsSuffix)
	basedirsPath := filepath.Join(datasetDir, basedirsBasename)

	database, err := opts.OpenDatabase(dgutaPath)
	if err != nil {
		return nil, nil, nil, err
	}

	tree := db.NewTree(database)

	mr, err := opts.OpenMultiBaseDirsReader(ownersPath, basedirsPath)
	if err != nil {
		tree.Close()

		return nil, nil, nil, err
	}

	closeFn := func() error {
		err := mr.Close()
		tree.Close()

		return err
	}

	return tree, mr, closeFn, nil
}

func resolveQueryDir(tree *db.Tree, mountPath, override string) string {
	queryDir := normaliseDirPath(override)
	if queryDir != "" {
		return queryDir
	}

	return pickRepresentativeDirFromTree(tree, mountPath)
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

func pickRepresentativeDirFromTree(tree *db.Tree, mountPath string) string {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	current := mountPath

	for i := 0; i < dirPickMaxIterations; i++ {
		next, done := nextRepresentativeDir(tree, current, filter)
		if done {
			return next
		}

		current = next
	}

	return current
}

func nextRepresentativeDir(tree *db.Tree, current string, filter *db.Filter) (string, bool) {
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

func prepareMultiReader(mr basedirs.Reader, mountsPath string) error {
	if mountsPath == "" {
		return prewarmBasedirsCaches(mr)
	}

	mounts, err := summariseutil.ParseMountpointsFromFile(mountsPath)
	if err != nil {
		return err
	}

	if len(mounts) > 0 {
		mr.SetMountPoints(mounts)
	}

	return prewarmBasedirsCaches(mr)
}

func prewarmBasedirsCaches(mr basedirs.Reader) error {
	for _, age := range db.DirGUTAges {
		if _, err := mr.GroupUsage(age); err != nil {
			return err
		}

		if _, err := mr.UserUsage(age); err != nil {
			return err
		}
	}

	return nil
}

func closeAndJoinErr(closeFn func() error, err error) error {
	if closeFn == nil {
		return err
	}

	if cerr := closeFn(); cerr != nil {
		return errors.Join(err, cerr)
	}

	return err
}

func pickRepresentativeIDs(mr basedirs.Reader, fallbackDir string) (queryIDs, error) {
	ids := queryIDs{groupBD: fallbackDir, userBD: fallbackDir}

	groups, err := mr.GroupUsage(db.DGUTAgeAll)
	if err != nil {
		return queryIDs{}, err
	}

	if g := pickLargestUsage(groups); g != nil {
		ids.gid = g.GID
		ids.groupBD = g.BaseDir
	}

	users, err := mr.UserUsage(db.DGUTAgeAll)
	if err != nil {
		return queryIDs{}, err
	}

	if u := pickLargestUsage(users); u != nil {
		ids.uid = u.UID
		ids.userBD = u.BaseDir
	}

	return ids, nil
}

func pickLargestUsage(usages []*basedirs.Usage) *basedirs.Usage {
	var best *basedirs.Usage
	for _, u := range usages {
		if best == nil || u.UsageSize > best.UsageSize {
			best = u
		}
	}

	return best
}

func openFreshQueryTree(opts QueryOptions, datasetDir string) (*db.Tree, func() error, error) {
	if opts.OpenDatabase == nil {
		return nil, nil, ErrOpenDatabaseRequired
	}

	dgutaPath := filepath.Join(datasetDir, dgutaDBsSuffix)

	database, err := opts.OpenDatabase(dgutaPath)
	if err != nil {
		return nil, nil, err
	}

	tree := db.NewTree(database)
	closeFn := func() error {
		tree.Close()

		return nil
	}

	return tree, closeFn, nil
}

func runQuerySuite(report *Report, ctx queryContext, opts QueryOptions, printf PrintfFunc) error {
	ops, err := selectQuerySuiteOps(buildQuerySuiteOps(ctx, opts), opts.Ops)
	if err != nil {
		return err
	}

	for _, op := range ops {
		if err := timeAndReportQueryOp(report, opts, printf, op); err != nil {
			return err
		}
	}

	return nil
}

func selectQuerySuiteOps(ops []querySuiteOp, names []string) ([]querySuiteOp, error) {
	wanted, wantedOrder := queryOpNameSet(names)
	if len(wanted) == 0 {
		return ops, nil
	}

	available := make([]string, 0, len(ops))
	availableSet := make(map[string]struct{}, len(ops))
	selected := make([]querySuiteOp, 0, len(wanted))

	for _, candidate := range ops {
		available = append(available, candidate.name)
		availableSet[candidate.name] = struct{}{}

		if _, ok := wanted[candidate.name]; ok {
			selected = append(selected, candidate)
		}
	}

	unknown := unknownQueryOpNames(wantedOrder, availableSet)
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

func queryOpNameSet(names []string) (map[string]struct{}, []string) {
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

func unknownQueryOpNames(wanted []string, available map[string]struct{}) []string {
	unknown := make([]string, 0)

	for _, name := range wanted {
		if _, ok := available[name]; ok {
			continue
		}

		unknown = append(unknown, name)
	}

	return unknown
}

func buildQuerySuiteOps(ctx queryContext, opts QueryOptions) []querySuiteOp {
	ops := []querySuiteOp{
		{
			name:   "mount_timestamps",
			inputs: map[string]any{"datasets": len(ctx.datasetDirs)},
			op:     func() error { return opMountTimestamps(ctx) },
		},
		opTreeWhereColdThenCached(ctx, opts.Splits),
	}

	if opts.WalkDepth > 0 && opts.WalkLimit > 0 {
		ops = append(ops, opTreeDiskTreeEndpointNewDirs(ctx, opts))
	}

	if opts.AncestorLimit > 0 {
		ops = append(ops, opTreeDiskTreeEndpointAncestorDirs(ctx, opts))
	}

	ops = append(ops, []querySuiteOp{
		{
			name: queryOpTreeDirInfoName,
			inputs: map[string]any{
				queryInputDirKey:         ctx.queryDir,
				queryInputAgeKey:         int(db.DGUTAgeAll),
				queryInputCacheScope:     queryScopeSameProvider,
				queryInputDurationSource: querySourceWall,
			},
			op: func() error { return opTreeDirInfo(ctx) },
		},
		{
			name: queryOpTreeDiskTreeEndName,
			inputs: map[string]any{
				queryInputDirKey:         ctx.queryDir,
				queryInputAgeKey:         int(db.DGUTAgeAll),
				queryInputCacheScope:     queryScopeSameProvider,
				queryInputDurationSource: querySourceWall,
			},
			op: func() error { return opTreeDiskTreeEndpoint(ctx) },
		},
		{
			name: queryOpTreeWhereName,
			inputs: map[string]any{
				queryInputDirKey:         ctx.queryDir,
				queryInputAgeKey:         int(db.DGUTAgeAll),
				queryInputSplitsKey:      opts.Splits,
				queryInputCacheScope:     queryScopeSameProvider,
				queryInputDurationSource: querySourceWall,
			},
			op: func() error { return opTreeWhere(ctx, opts.Splits) },
		},
		{
			name: queryOpTreeWhereFreshName,
			inputs: map[string]any{
				queryInputDirKey:         ctx.queryDir,
				queryInputAgeKey:         int(db.DGUTAgeAll),
				queryInputSplitsKey:      opts.Splits,
				queryInputCacheScope:     queryScopeFreshProvider,
				queryInputDurationSource: querySourceWall,
			},
			op:         func() error { return opTreeWhereFreshProvider(ctx, opts.Splits) },
			skipWarmup: true,
		},
		{
			name:   "basedirs_group_usage",
			inputs: map[string]any{queryInputAgeKey: int(db.DGUTAgeAll)},
			op:     func() error { return opBasedirsGroupUsage(ctx) },
		},
		{
			name:   "basedirs_user_usage",
			inputs: map[string]any{queryInputAgeKey: int(db.DGUTAgeAll)},
			op:     func() error { return opBasedirsUserUsage(ctx) },
		},
		{
			name: "basedirs_group_subdirs",
			inputs: map[string]any{
				"gid":                ctx.ids.gid,
				queryInputBaseDirKey: ctx.ids.groupBD,
				queryInputAgeKey:     int(db.DGUTAgeAll),
			},
			op: func() error { return opBasedirsGroupSubDirs(ctx) },
		},
		{
			name: "basedirs_user_subdirs",
			inputs: map[string]any{
				"uid":                ctx.ids.uid,
				queryInputBaseDirKey: ctx.ids.userBD,
				queryInputAgeKey:     int(db.DGUTAgeAll),
			},
			op: func() error { return opBasedirsUserSubDirs(ctx) },
		},
		{
			name:   "basedirs_history",
			inputs: map[string]any{"gid": ctx.ids.gid, queryInputBaseDirKey: ctx.ids.groupBD},
			op:     func() error { return opBasedirsHistory(ctx) },
		},
	}...)

	return ops
}

func opMountTimestamps(ctx queryContext) error {
	for _, datasetDir := range ctx.datasetDirs {
		base := filepath.Base(datasetDir)

		_, err := DeriveMountPathFromDatasetDirName(base)
		if err != nil {
			return err
		}

		_, err = os.Stat(filepath.Join(datasetDir, dgutaDBsSuffix))
		if err != nil {
			return err
		}
	}

	return nil
}

func opTreeWhereColdThenCached(ctx queryContext, splits int) querySuiteOp {
	return querySuiteOp{
		name: queryOpTreeWhereColdName,
		inputs: map[string]any{
			queryInputDirKey:         ctx.queryDir,
			queryInputAgeKey:         int(db.DGUTAgeAll),
			queryInputSplitsKey:      splits,
			queryInputCacheScope:     queryScopeSameProviderCold,
			queryInputDurationSource: querySourceWall,
		},
		op:         func() error { return opTreeWhere(ctx, splits) },
		skipWarmup: true,
	}
}

func opTreeDiskTreeEndpointNewDirs(ctx queryContext, opts QueryOptions) querySuiteOp {
	dirs, fallback := disktreeClickDirs(ctx, opts)
	timedDirs := uniqueDirsForRepeats(dirs, opts.Repeat)
	i := 0

	return querySuiteOp{
		name: queryOpTreeDiskTreeNewName,
		inputs: map[string]any{
			"start_dir":              ctx.queryDir,
			"dirs":                   timedDirs,
			"dir_count":              len(timedDirs),
			"walk_depth":             opts.WalkDepth,
			"walk_limit":             opts.WalkLimit,
			"fallback_to_start_dir":  fallback,
			queryInputCacheScope:     queryScopeNewDir,
			queryInputDurationSource: querySourceWall,
			queryInputAgeKey:         int(db.DGUTAgeAll),
		},
		op: func() error {
			if i >= len(timedDirs) {
				return nil
			}

			dir := timedDirs[i]
			i++

			return runTreeDiskTreeEndpoint(ctx.tree, dir)
		},
		skipWarmup:        true,
		hasRepeatOverride: true,
		repeatOverride:    len(timedDirs),
	}
}

func opTreeDiskTreeEndpointAncestorDirs(ctx queryContext, opts QueryOptions) querySuiteOp {
	dirs := ancestorDisktreeDirs(ctx, opts)
	timedDirs := cycledDirsForRepeats(dirs, opts.Repeat)
	i := 0

	return querySuiteOp{
		name: queryOpTreeDiskTreeAncName,
		inputs: map[string]any{
			"start_dir":              ancestorStartDir(opts),
			"dirs":                   timedDirs,
			"dir_count":              len(timedDirs),
			"ancestor_limit":         opts.AncestorLimit,
			queryInputCacheScope:     queryScopeAncestorDirs,
			queryInputDurationSource: querySourceWall,
			queryInputAgeKey:         int(db.DGUTAgeAll),
		},
		op: func() error {
			if i >= len(timedDirs) {
				return nil
			}

			dir := timedDirs[i]
			i++

			return runTreeDiskTreeEndpoint(ctx.tree, dir)
		},
		skipWarmup:        true,
		hasRepeatOverride: true,
		repeatOverride:    len(timedDirs),
	}
}

func disktreeClickDirs(ctx queryContext, opts QueryOptions) ([]string, bool) {
	dirs := leafDisktreeDirs(collectDisktreeDirsFromFreshTree(ctx, opts.WalkDepth, opts.WalkLimit))
	if len(dirs) > 0 || opts.WalkLimit <= 0 {
		return dirs, false
	}

	return []string{ctx.queryDir}, true
}

func ancestorDisktreeDirs(ctx queryContext, opts QueryOptions) []string {
	startDir := ancestorStartDir(opts)
	if opts.AncestorLimit <= 0 {
		return nil
	}

	mountPaths := mountPathsFromDatasetDirs(ctx.datasetDirs)
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

func mountPathsFromDatasetDirs(datasetDirs []string) []string {
	mountPaths := make([]string, 0, len(datasetDirs))
	for _, datasetDir := range datasetDirs {
		mountPath, err := DeriveMountPathFromDatasetDirName(filepath.Base(datasetDir))
		if err != nil {
			continue
		}

		mountPaths = append(mountPaths, mountPath)
	}

	sort.Strings(mountPaths)

	return mountPaths
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

func collectDisktreeDirsFromFreshTree(ctx queryContext, depth int, limit int) (dirs []string) {
	if ctx.openFreshTree == nil {
		return nil
	}

	tree, closeFn, err := ctx.openFreshTree()
	if err != nil {
		return nil
	}

	if closeFn != nil {
		defer func() {
			if err := closeFn(); err != nil {
				dirs = nil
			}
		}()
	}

	return collectDisktreeDirsFromTree(tree, ctx.queryDir, depth, limit)
}

func collectDisktreeDirsFromTree(tree *db.Tree, startDir string, depth int, limit int) []string {
	if tree == nil || depth <= 0 || limit <= 0 {
		return nil
	}

	queue := []disktreeWalkDir{{dir: startDir}}
	seen := map[string]struct{}{startDir: {}}
	dirs := make([]string, 0, limit)

	for len(queue) > 0 && len(dirs) < limit {
		item := queue[0]
		queue = queue[1:]

		if item.depth >= depth {
			continue
		}

		children := childDirsFromTree(tree, item.dir)
		queue = appendDisktreeWalkChildren(queue, &dirs, seen, limit, item.depth+1, children)
	}

	return dirs
}

func childDirsFromTree(tree *db.Tree, dir string) []string {
	filter := &db.Filter{Age: db.DGUTAgeAll}

	di, err := tree.DirInfo(dir, filter)
	if err != nil || di == nil {
		return nil
	}

	children := make([]string, 0, len(di.Children))
	for _, child := range di.Children {
		children = append(children, child.Dir)
	}

	return children
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

func uniqueDirsForRepeats(dirs []string, repeat int) []string {
	if repeat <= 0 || len(dirs) == 0 {
		return nil
	}

	n := min(repeat, len(dirs))

	return append([]string(nil), dirs[:n]...)
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

func runTreeDiskTreeEndpoint(tree *db.Tree, dir string) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}

	di, err := tree.DirInfo(dir, filter)
	if err != nil || di == nil {
		return err
	}

	childPaths := make([]string, 0, len(di.Children))
	for _, child := range di.Children {
		childPaths = append(childPaths, child.Dir)
	}

	_ = tree.DirsHaveChildren(childPaths, filter)

	return nil
}

func opTreeDirInfo(ctx queryContext) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	_, err := ctx.tree.DirInfo(ctx.queryDir, filter)

	return err
}

func opTreeDiskTreeEndpoint(ctx queryContext) error {
	return runTreeDiskTreeEndpoint(ctx.tree, ctx.queryDir)
}

func opTreeWhere(ctx queryContext, splits int) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	splitFn := split.SplitsToSplitFn(splits)
	_, err := ctx.tree.Where(ctx.queryDir, filter, splitFn)

	return err
}

func opTreeWhereFreshProvider(ctx queryContext, splits int) error {
	if ctx.openFreshTree == nil {
		return ErrOpenDatabaseRequired
	}

	tree, closeFn, err := ctx.openFreshTree()
	if err != nil {
		return err
	}

	filter := &db.Filter{Age: db.DGUTAgeAll}
	splitFn := split.SplitsToSplitFn(splits)
	_, whereErr := tree.Where(ctx.queryDir, filter, splitFn)

	return closeAndJoinErr(closeFn, whereErr)
}

func opBasedirsGroupUsage(ctx queryContext) error {
	_, err := ctx.mr.GroupUsage(db.DGUTAgeAll)

	return err
}

func opBasedirsUserUsage(ctx queryContext) error {
	_, err := ctx.mr.UserUsage(db.DGUTAgeAll)

	return err
}

func opBasedirsGroupSubDirs(ctx queryContext) error {
	_, err := ctx.mr.GroupSubDirs(ctx.ids.gid, ctx.ids.groupBD, db.DGUTAgeAll)

	return err
}

func opBasedirsUserSubDirs(ctx queryContext) error {
	_, err := ctx.mr.UserSubDirs(ctx.ids.uid, ctx.ids.userBD, db.DGUTAgeAll)

	return err
}

func opBasedirsHistory(ctx queryContext) error {
	_, err := ctx.mr.History(ctx.ids.gid, ctx.ids.groupBD)

	return err
}

func timeAndReportQueryOp(
	report *Report,
	opts QueryOptions,
	printf PrintfFunc,
	op querySuiteOp,
) error {
	warmup := opts.Warmup
	if op.skipWarmup {
		warmup = 0
	}

	repeat := opts.Repeat
	if op.hasRepeatOverride {
		repeat = op.repeatOverride
	}

	durations, err := measureOperation(warmup, repeat, op.op)
	if err != nil {
		return err
	}

	report.AddOperation(op.name, op.inputs, durations)

	p50, p95, p99 := PercentilesMS(durations)
	printf("%s repeats=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n", op.name, len(durations), p50, p95, p99)

	return nil
}

func measureOperation(warmup, repeat int, op func() error) ([]float64, error) {
	for i := 0; i < warmup; i++ {
		if err := op(); err != nil {
			return nil, err
		}
	}

	durations := make([]float64, 0, repeat)
	for i := 0; i < repeat; i++ {
		start := time.Now()

		if err := op(); err != nil {
			return nil, err
		}

		durations = append(durations, durationMS(time.Since(start)))
	}

	return durations, nil
}

func discoverQueryDatasets(inputDir string, printf PrintfFunc) ([]string, string, error) {
	datasetDirs, err := findDatasetDirs(inputDir, dgutaDBsSuffix, basedirsBasename)
	if err != nil {
		return nil, "", err
	}

	datasetDir := datasetDirs[0]
	if len(datasetDirs) > 1 {
		printf("query: %d datasets found; using %s\n", len(datasetDirs), filepath.Base(datasetDir))
	}

	return datasetDirs, datasetDir, nil
}

type disktreeWalkDir struct {
	dir   string
	depth int
}

// ImportOptions configures the bolt-perf import harness.
type ImportOptions struct {
	Backend  string
	Owners   string
	Mounts   string
	JSONOut  string
	OutDir   string
	Quota    string
	Config   string
	MaxLines int
	Repeat   int
	Warmup   int

	NewDGUTAWriter   func(outputDir string) (db.DGUTAWriter, error)
	NewBaseDirsStore func(dbPath, previousDBPath string) (basedirs.Store, error)
}

// QueryOptions configures the bolt-perf query harness.
type QueryOptions struct {
	Backend string
	Owners  string
	Mounts  string
	JSONOut string

	Dir           string
	AncestorDir   string
	Ops           []string
	Repeat        int
	Warmup        int
	Splits        int
	WalkDepth     int
	WalkLimit     int
	AncestorLimit int

	OpenDatabase            func(paths ...string) (db.Database, error)
	OpenMultiBaseDirsReader func(ownersPath string, dbPaths ...string) (basedirs.Reader, error)
}

type importSpec struct {
	statsGZPath    string
	basedirsDBPath string
	dgutaDBDir     string
	modtime        time.Time
	maxLines       int
}

type importTargets struct {
	basedirsDBPath string
	dgutaDBDir     string
}

type queryIDs struct {
	gid     uint32
	groupBD string
	uid     uint32
	userBD  string
}

type queryContext struct {
	datasetDirs   []string
	tree          *db.Tree
	mr            basedirs.Reader
	closeFn       func() error
	openFreshTree func() (*db.Tree, func() error, error)
	queryDir      string
	ids           queryIDs
}

type querySuiteOp struct {
	name              string
	inputs            map[string]any
	op                func() error
	skipWarmup        bool
	hasRepeatOverride bool
	repeatOverride    int
}

// lineCountingReader is used to optionally cap stats parsing at a number of lines.
// This is testable and shared by bolt-perf import.
type lineCountingReader struct {
	underlying io.Reader
	maxLines   uint64

	buf        []byte
	pending    []byte
	seenLines  uint64
	reachedMax bool
}

func (l *lineCountingReader) linesRead() uint64 {
	return l.seenLines
}

func (l *lineCountingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if n, ok, err := l.readPendingOrDone(p); ok {
		return n, err
	}

	if l.reachedMax {
		return 0, io.EOF
	}

	n, err := l.underlying.Read(l.buf)
	if n == 0 {
		return 0, err
	}

	chunk := l.buf[:n]
	allowed := l.limitChunk(chunk)

	nn := copy(p, allowed)
	if nn < len(allowed) {
		l.pending = append(l.pending[:0], allowed[nn:]...)
	}

	return nn, l.errAfterRead(err)
}

func (l *lineCountingReader) readPendingOrDone(p []byte) (int, bool, error) {
	n := l.readPending(p)
	if n == 0 {
		return 0, false, nil
	}

	if l.reachedMax && len(l.pending) == 0 {
		return n, true, io.EOF
	}

	return n, true, nil
}

func (l *lineCountingReader) errAfterRead(underlyingErr error) error {
	if l.reachedMax {
		if len(l.pending) == 0 {
			return io.EOF
		}

		return nil
	}

	return underlyingErr
}

func (l *lineCountingReader) readPending(p []byte) int {
	if len(l.pending) == 0 {
		return 0
	}

	n := copy(p, l.pending)
	l.pending = l.pending[n:]

	return n
}

func (l *lineCountingReader) limitChunk(chunk []byte) []byte {
	if l.maxLines == 0 {
		l.seenLines += countNewLines(chunk)

		return chunk
	}

	for i, b := range chunk {
		if b != '\n' {
			continue
		}

		l.seenLines++
		if l.seenLines >= l.maxLines {
			l.reachedMax = true

			return chunk[:i+1]
		}
	}

	return chunk
}

func countNewLines(b []byte) uint64 {
	var n uint64

	for _, c := range b {
		if c == '\n' {
			n++
		}
	}

	return n
}
