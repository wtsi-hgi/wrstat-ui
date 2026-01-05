package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	perfSchemaVersion        = 1
	perfDefaultDirPerm       = 0o755
	perfSummariseDBBatchSize = 10000
	perfDatasetSplitParts    = 2
	perfP50                  = 0.50
	perfP95                  = 0.95
	perfP99                  = 0.99

	perfDGUTADBsSuffix   = "dguta.dbs"
	perfBasedirsBasename = "basedirs.db"
	perfStatsGZBasename  = "stats.gz"

	perfLineReaderBufSize = 32 * 1024

	perfDirPickMinCount      = 1000
	perfDirPickMaxCount      = 20000
	perfDirPickMaxIterations = 128
)

var (
	errUnknownBackend              = errors.New("unknown backend")
	errNoDatasets                  = errors.New("no datasets found")
	errDatasetDirMissingUnderscore = errors.New("dataset dir name missing '_' separator")
)

type perfPrintfFunc func(string, ...any)

func runBoltPerfImportInterfaces(inputDir string, printf perfPrintfFunc) error {
	if printf == nil {
		printf = func(string, ...any) {}
	}

	if err := validatePerfBackend(boltPerf.backend); err != nil {
		return err
	}

	report, err := boltPerfImportInterfacesReport(inputDir, printf)
	if err != nil {
		return err
	}

	return writePerfReport(boltPerf.jsonOut, report)
}

func validatePerfBackend(backend string) error {
	switch backend {
	case "bolt", "bolt_interfaces":
		return nil
	default:
		return fmt.Errorf("%w: %q", errUnknownBackend, backend)
	}
}

func boltPerfImportInterfacesReport(inputDir string, printf perfPrintfFunc) (perfReport, error) {
	datasetDirs, err := boltPerfIFindDatasetDirs(inputDir, perfStatsGZBasename)
	if err != nil {
		return perfReport{}, err
	}

	report := newPerfReport(boltPerf.backend, inputDir, boltPerf.repeat, boltPerf.warmup)
	startAll := time.Now()

	totalRecords, err := boltPerfImportInterfacesDatasets(datasetDirs, printf)
	if err != nil {
		return perfReport{}, err
	}

	report.addOperation(
		"import_total",
		map[string]any{"datasets": len(datasetDirs), "records": totalRecords},
		[]float64{durationMS(time.Since(startAll))},
	)

	return report, nil
}

func boltPerfIFindDatasetDirs(baseDir string, required ...string) ([]string, error) {
	dirs, err := boltPerfIFindLatestDatasetDirs(baseDir, required...)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, fmt.Errorf("%w: %q", errNoDatasets, baseDir)
	}

	sort.Strings(dirs)

	return dirs, nil
}

func boltPerfIFindLatestDatasetDirs(baseDir string, required ...string) ([]string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	latest := make(map[string]perfDatasetNameVersion)

	for _, entry := range entries {
		boltPerfIConsiderDatasetDirEntry(latest, baseDir, entry, required)
	}

	dirs := make([]string, 0, len(latest))
	for _, entry := range latest {
		dirs = append(dirs, filepath.Join(baseDir, entry.name))
	}

	sort.Strings(dirs)

	return dirs, nil
}

func boltPerfIConsiderDatasetDirEntry(
	latest map[string]perfDatasetNameVersion,
	baseDir string,
	entry fs.DirEntry,
	required []string,
) {
	if !isValidDatasetDir(entry, baseDir, required...) {
		return
	}

	key, version, ok := datasetKeyAndVersion(entry.Name())
	if !ok {
		return
	}

	if previous, ok := latest[key]; ok && previous.version > version {
		return
	}

	latest[key] = perfDatasetNameVersion{name: entry.Name(), version: version}
}

func isValidDatasetDir(entry fs.DirEntry, baseDir string, required ...string) bool {
	name := entry.Name()
	if !entry.IsDir() {
		return false
	}

	if name == "" || strings.HasPrefix(name, ".") {
		return false
	}

	if !strings.Contains(name, "_") {
		return false
	}

	for _, req := range required {
		if _, err := os.Stat(filepath.Join(baseDir, name, req)); err != nil {
			return false
		}
	}

	return true
}

func datasetKeyAndVersion(name string) (key, version string, ok bool) {
	parts := strings.SplitN(name, "_", perfDatasetSplitParts)
	if len(parts) != perfDatasetSplitParts {
		return "", "", false
	}

	return parts[1], parts[0], true
}

func newPerfReport(backend, inputDir string, repeat, warmup int) perfReport {
	return perfReport{
		SchemaVersion: perfSchemaVersion,
		Backend:       backend,
		GitCommit:     gitCommitFromBuildInfo(),
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		StartedAt:     time.Now().UTC().Format(time.RFC3339),
		InputDir:      inputDir,
		Repeat:        repeat,
		Warmup:        warmup,
		Operations:    make([]perfOperation, 0),
	}
}

func gitCommitFromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}

	return ""
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func writePerfReport(path string, report perfReport) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	enc := json.NewEncoder(fh)
	enc.SetIndent("", "  ")

	return enc.Encode(report)
}

func boltPerfImportInterfacesDatasets(datasetDirs []string, printf perfPrintfFunc) (uint64, error) {
	var total uint64

	for _, datasetDir := range datasetDirs {
		records, err := importOneDatasetInterfaces(datasetDir, printf)
		if err != nil {
			return 0, err
		}

		total += records
	}

	return total, nil
}

func importOneDatasetInterfaces(datasetDir string, printf perfPrintfFunc) (_ uint64, err error) {
	base := filepath.Base(datasetDir)
	statsGZPath := filepath.Join(datasetDir, perfStatsGZBasename)

	st, err := os.Stat(statsGZPath)
	if err != nil {
		return 0, err
	}

	outDatasetDir := filepath.Join(boltPerf.outDir, base)
	if mkErr := os.MkdirAll(outDatasetDir, perfDefaultDirPerm); mkErr != nil {
		return 0, mkErr
	}

	start := time.Now()

	records, err := importDatasetFromStatsGZ(
		statsGZPath,
		filepath.Join(outDatasetDir, perfBasedirsBasename),
		filepath.Join(outDatasetDir, perfDGUTADBsSuffix),
		st.ModTime(),
		boltPerf.maxLines,
	)
	if err != nil {
		return 0, err
	}

	printf("import dataset=%s records=%d seconds=%.3f\n", base, records, time.Since(start).Seconds())

	return records, nil
}

func importDatasetFromStatsGZ(
	statsGZPath, basedirsDBPath, dgutaDBDir string,
	modtime time.Time,
	maxLines int,
) (_ uint64, err error) {
	gz, closeStatsFn, err := openStatsGZReader(statsGZPath)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := closeStatsFn(); err == nil {
			err = cerr
		}
	}()

	return importDatasetFromReader(gz, basedirsDBPath, dgutaDBDir, modtime, maxLines)
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

func importDatasetFromReader(
	r io.Reader,
	basedirsDBPath, dgutaDBDir string,
	modtime time.Time,
	maxLines int,
) (_ uint64, err error) {
	lr := newLineCountingReader(r, maxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	closeFn, err := addSummarisersInterfaces(ss, basedirsDBPath, dgutaDBDir, modtime)
	if err != nil {
		return 0, err
	}

	if closeFn != nil {
		defer func() {
			if cerr := closeFn(); err == nil {
				err = cerr
			}
		}()
	}

	if err := ss.Summarise(); err != nil {
		return 0, err
	}

	return lr.linesRead(), nil
}

func newLineCountingReader(r io.Reader, maxLines int) *lineCountingReader {
	var ml uint64
	if maxLines > 0 {
		ml = uint64(maxLines)
	}

	return &lineCountingReader{
		underlying: r,
		maxLines:   ml,
		buf:        make([]byte, perfLineReaderBufSize),
	}
}

func addSummarisersInterfaces(
	ss *summary.Summariser,
	basedirsDBPath, dgutaDBDir string,
	modtime time.Time,
) (func() error, error) {
	store, err := addBasedirsSummariserInterfaces(ss, basedirsDBPath, modtime)
	if err != nil {
		return nil, err
	}

	writer, err := addDGUTASummariserInterfaces(ss, dgutaDBDir, modtime)
	if err != nil {
		_ = store.Close()

		return nil, err
	}

	return func() error {
		return errors.Join(store.Close(), writer.Close())
	}, nil
}

func addBasedirsSummariserInterfaces(
	ss *summary.Summariser,
	basedirsDBPath string,
	modtime time.Time,
) (basedirs.Store, error) {
	quotas, shouldOutput, mountpoints, err := boltPerfParseBasedirsInputs()
	if err != nil {
		return nil, err
	}

	store, err := boltPerfOpenBasedirsStoreForImport(basedirsDBPath, modtime)
	if err != nil {
		return nil, err
	}

	bd, err := boltPerfCreateBaseDirsCreator(store, quotas, mountpoints, modtime)
	if err != nil {
		_ = store.Close()

		return nil, err
	}

	ss.AddDirectoryOperation(sbasedirs.NewBaseDirs(shouldOutput, bd))

	return store, nil
}

func boltPerfParseBasedirsInputs() (*basedirs.Quotas, func(*summary.DirectoryPath) bool, []string, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(boltPerf.quota, boltPerf.config)
	if err != nil {
		return nil, nil, nil, err
	}

	mountpoints, err := summariseutil.ParseMountpointsFromFile(boltPerf.mounts)
	if err != nil {
		return nil, nil, nil, err
	}

	return quotas, config.PathShouldOutput, mountpoints, nil
}

func boltPerfOpenBasedirsStoreForImport(basedirsDBPath string, modtime time.Time) (basedirs.Store, error) {
	if removeErr := os.Remove(basedirsDBPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		return nil, removeErr
	}

	store, err := bolt.NewBaseDirsStore(basedirsDBPath, "")
	if err != nil {
		return nil, err
	}

	mountPath := summariseutil.DeriveMountPathFromOutputDir(basedirsDBPath)
	store.SetMountPath(mountPath)
	store.SetUpdatedAt(modtime)

	return store, nil
}

func boltPerfCreateBaseDirsCreator(
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

func addDGUTASummariserInterfaces(
	ss *summary.Summariser,
	dgutaDBDir string,
	modtime time.Time,
) (db.DGUTAWriter, error) {
	if removeErr := os.RemoveAll(dgutaDBDir); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		return nil, removeErr
	}

	if mkErr := os.MkdirAll(dgutaDBDir, perfDefaultDirPerm); mkErr != nil {
		return nil, mkErr
	}

	writer, err := bolt.NewDGUTAWriter(dgutaDBDir)
	if err != nil {
		return nil, err
	}

	writer.SetMountPath(summariseutil.DeriveMountPathFromOutputDir(dgutaDBDir))
	writer.SetUpdatedAt(modtime)
	writer.SetBatchSize(perfSummariseDBBatchSize)
	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))

	return writer, nil
}

func runBoltPerfQueryInterfaces(inputDir string, printf perfPrintfFunc) (err error) {
	if printf == nil {
		printf = func(string, ...any) {}
	}

	if validateErr := validatePerfBackend(boltPerf.backend); validateErr != nil {
		return validateErr
	}

	ctx, err := buildPerfQueryContext(inputDir, printf)
	if err != nil {
		return err
	}

	defer func() {
		if cerr := ctx.closeFn(); err == nil {
			err = cerr
		}
	}()

	report := newPerfReport(boltPerf.backend, inputDir, boltPerf.repeat, boltPerf.warmup)
	if err := runPerfQuerySuite(&report, ctx, printf); err != nil {
		return err
	}

	return writePerfReport(boltPerf.jsonOut, report)
}

func buildPerfQueryContext(inputDir string, printf perfPrintfFunc) (perfQueryContext, error) {
	datasetDirs, err := boltPerfIFindDatasetDirs(inputDir, perfDGUTADBsSuffix, perfBasedirsBasename)
	if err != nil {
		return perfQueryContext{}, err
	}

	if len(datasetDirs) > 1 {
		printf("query: %d datasets found; using provider aggregation\n", len(datasetDirs))
	}

	cfg, cfgErr := boltPerfQueryProviderConfig(inputDir)
	if cfgErr != nil {
		return perfQueryContext{}, cfgErr
	}

	p, err := bolt.OpenProvider(cfg)
	if err != nil {
		return perfQueryContext{}, err
	}

	closeFn := func() error { return p.Close() }

	ctx, ctxErr := boltPerfBuildQueryContextWithProvider(datasetDirs, p.Tree(), p.BaseDirs(), closeFn, inputDir, printf)
	if ctxErr != nil {
		return perfQueryContext{}, ctxErr
	}

	return ctx, nil
}

func boltPerfQueryProviderConfig(basePath string) (bolt.Config, error) {
	cfg := bolt.Config{
		BasePath:      basePath,
		DGUTADBName:   perfDGUTADBsSuffix,
		BaseDirDBName: perfBasedirsBasename,
		OwnersCSVPath: boltPerf.owners,
		PollInterval:  0,
	}

	if boltPerf.mounts != "" {
		mountpoints, parseErr := summariseutil.ParseMountpointsFromFile(boltPerf.mounts)
		if parseErr != nil {
			return bolt.Config{}, parseErr
		}

		cfg.MountPoints = mountpoints
	}

	return cfg, nil
}

func boltPerfBuildQueryContextWithProvider(
	datasetDirs []string,
	tree *db.Tree,
	bd basedirs.Reader,
	closeFn func() error,
	inputDir string,
	printf perfPrintfFunc,
) (perfQueryContext, error) {
	if prewarmErr := boltPerfPrewarmBasedirsCaches(bd); prewarmErr != nil {
		return perfQueryContext{}, closeAndJoinErr(closeFn, prewarmErr)
	}

	mountPath, err := boltPerfMountPath(bd, inputDir)
	if err != nil {
		return perfQueryContext{}, closeAndJoinErr(closeFn, err)
	}

	queryDir, autoSelected := boltPerfSelectQueryDir(tree, mountPath)
	if autoSelected {
		printf("query: auto-selected dir=%s\n", queryDir)
	}

	ids, err := boltPerfPickRepresentativeIDs(bd, mountPath)
	if err != nil {
		return perfQueryContext{}, closeAndJoinErr(closeFn, err)
	}

	return boltPerfNewQueryContext(datasetDirs, tree, bd, closeFn, queryDir, ids), nil
}

func boltPerfPrewarmBasedirsCaches(bd basedirs.Reader) error {
	for _, age := range db.DirGUTAges {
		if _, usageErr := bd.GroupUsage(age); usageErr != nil {
			return usageErr
		}

		if _, usageErr := bd.UserUsage(age); usageErr != nil {
			return usageErr
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

func boltPerfMountPath(bd basedirs.Reader, inputDir string) (string, error) {
	mountTimestamps, err := bd.MountTimestamps()
	if err != nil {
		return "", err
	}

	if len(mountTimestamps) == 0 {
		return "", fmt.Errorf("%w: %q", errNoDatasets, inputDir)
	}

	return boltPerfDeriveMountPath(mountTimestamps), nil
}

func boltPerfDeriveMountPath(mountTimestamps map[string]time.Time) string {
	mountKeys := make([]string, 0, len(mountTimestamps))
	for k := range mountTimestamps {
		mountKeys = append(mountKeys, k)
	}

	sort.Strings(mountKeys)

	mountPath := strings.ReplaceAll(mountKeys[0], "／", "/")
	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath
}

func boltPerfSelectQueryDir(tree *db.Tree, mountPath string) (string, bool) {
	queryDir := normaliseDirPath(boltPerf.dir)
	if queryDir == "" {
		return pickRepresentativeDirFromTree(tree, mountPath), true
	}

	return queryDir, false
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

	for i := 0; i < perfDirPickMaxIterations; i++ {
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
	if count >= perfDirPickMinCount && count <= perfDirPickMaxCount {
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

func boltPerfPickRepresentativeIDs(bd basedirs.Reader, mountPath string) (perfQueryIDs, error) {
	return pickRepresentativeIDs(bd, mountPath)
}

func pickRepresentativeIDs(r basedirs.Reader, fallbackDir string) (perfQueryIDs, error) {
	ids := perfQueryIDs{groupBD: fallbackDir, userBD: fallbackDir}

	groups, err := r.GroupUsage(db.DGUTAgeAll)
	if err != nil {
		return perfQueryIDs{}, err
	}

	if g := pickLargestUsage(groups); g != nil {
		ids.gid = g.GID
		ids.groupBD = g.BaseDir
	}

	users, err := r.UserUsage(db.DGUTAgeAll)
	if err != nil {
		return perfQueryIDs{}, err
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

func boltPerfNewQueryContext(
	datasetDirs []string,
	tree *db.Tree,
	bd basedirs.Reader,
	closeFn func() error,
	queryDir string,
	ids perfQueryIDs,
) perfQueryContext {
	return perfQueryContext{
		datasetDirs: datasetDirs,
		tree:        tree,
		bd:          bd,
		closeFn:     closeFn,
		queryDir:    queryDir,
		ids:         ids,
	}
}

func runPerfQuerySuite(report *perfReport, ctx perfQueryContext, printf perfPrintfFunc) error {
	for _, op := range boltPerfQueryOps(ctx) {
		durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, op.op)
		if err != nil {
			return err
		}

		report.addOperation(op.name, op.inputs, durations)

		p50, p95, p99 := percentilesMS(durations)
		printf("%s repeats=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n", op.name, len(durations), p50, p95, p99)
	}

	return nil
}

func boltPerfQueryOps(ctx perfQueryContext) []perfQueryOp {
	return []perfQueryOp{
		boltPerfOpMountTimestamps(ctx),
		boltPerfOpTreeDirInfo(ctx),
		boltPerfOpTreeWhere(ctx),
		boltPerfOpBasedirsGroupUsage(ctx),
		boltPerfOpBasedirsUserUsage(ctx),
		boltPerfOpBasedirsGroupSubdirs(ctx),
		boltPerfOpBasedirsUserSubdirs(ctx),
		boltPerfOpBasedirsHistory(ctx),
	}
}

func boltPerfOpMountTimestamps(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name:   "mount_timestamps",
		inputs: map[string]any{"datasets": len(ctx.datasetDirs)},
		op: func() error {
			if _, err := ctx.bd.MountTimestamps(); err != nil {
				return err
			}

			for _, datasetDir := range ctx.datasetDirs {
				base := filepath.Base(datasetDir)
				if _, err := deriveMountPathFromDatasetDirName(base); err != nil {
					return err
				}
			}

			return nil
		},
	}
}

func deriveMountPathFromDatasetDirName(dirName string) (string, error) {
	parts := strings.SplitN(dirName, "_", perfDatasetSplitParts)
	if len(parts) != perfDatasetSplitParts {
		return "", fmt.Errorf("%w: %q", errDatasetDirMissingUnderscore, dirName)
	}

	mountKey := parts[1]

	mountPath := strings.ReplaceAll(mountKey, "／", "/")
	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath, nil
}

func boltPerfOpTreeDirInfo(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name:   "tree_dirinfo",
		inputs: map[string]any{"dir": ctx.queryDir, "age": int(db.DGUTAgeAll)},
		op: func() error {
			filter := &db.Filter{Age: db.DGUTAgeAll}
			_, err := ctx.tree.DirInfo(ctx.queryDir, filter)

			return err
		},
	}
}

func boltPerfOpTreeWhere(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name: "tree_where",
		inputs: map[string]any{
			"dir":    ctx.queryDir,
			"age":    int(db.DGUTAgeAll),
			"splits": boltPerf.splits,
		},
		op: func() error {
			filter := &db.Filter{Age: db.DGUTAgeAll}
			splitFn := split.SplitsToSplitFn(boltPerf.splits)
			_, err := ctx.tree.Where(ctx.queryDir, filter, splitFn)

			return err
		},
	}
}

func boltPerfOpBasedirsGroupUsage(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name:   "basedirs_group_usage",
		inputs: map[string]any{"age": int(db.DGUTAgeAll)},
		op: func() error {
			_, err := ctx.bd.GroupUsage(db.DGUTAgeAll)

			return err
		},
	}
}

func boltPerfOpBasedirsUserUsage(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name:   "basedirs_user_usage",
		inputs: map[string]any{"age": int(db.DGUTAgeAll)},
		op: func() error {
			_, err := ctx.bd.UserUsage(db.DGUTAgeAll)

			return err
		},
	}
}

func boltPerfOpBasedirsGroupSubdirs(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name: "basedirs_group_subdirs",
		inputs: map[string]any{
			"gid":     ctx.ids.gid,
			"basedir": ctx.ids.groupBD,
			"age":     int(db.DGUTAgeAll),
		},
		op: func() error {
			_, err := ctx.bd.GroupSubDirs(ctx.ids.gid, ctx.ids.groupBD, db.DGUTAgeAll)

			return err
		},
	}
}

func boltPerfOpBasedirsUserSubdirs(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name: "basedirs_user_subdirs",
		inputs: map[string]any{
			"uid":     ctx.ids.uid,
			"basedir": ctx.ids.userBD,
			"age":     int(db.DGUTAgeAll),
		},
		op: func() error {
			_, err := ctx.bd.UserSubDirs(ctx.ids.uid, ctx.ids.userBD, db.DGUTAgeAll)

			return err
		},
	}
}

func boltPerfOpBasedirsHistory(ctx perfQueryContext) perfQueryOp {
	return perfQueryOp{
		name:   "basedirs_history",
		inputs: map[string]any{"gid": ctx.ids.gid, "basedir": ctx.ids.groupBD},
		op: func() error {
			_, err := ctx.bd.History(ctx.ids.gid, ctx.ids.groupBD)

			return err
		},
	}
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

func percentilesMS(values []float64) (float64, float64, float64) {
	return percentileMS(values, perfP50), percentileMS(values, perfP95), percentileMS(values, perfP99)
}

func percentileMS(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := slices.Clone(values)
	slices.Sort(sorted)

	if p <= 0 {
		return sorted[0]
	}

	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}

	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

type perfQueryOp struct {
	name   string
	inputs map[string]any
	op     func() error
}

// perfOperation represents a single measured operation in a perf report.
type perfOperation struct {
	Name        string         `json:"name"`
	Inputs      map[string]any `json:"inputs"`
	DurationsMS []float64      `json:"durations_ms"`
	P50MS       float64        `json:"p50_ms"`
	P95MS       float64        `json:"p95_ms"`
	P99MS       float64        `json:"p99_ms"`
}

// perfReport is the top-level JSON report written by the perf harness.
type perfReport struct {
	SchemaVersion int             `json:"schema_version"`
	Backend       string          `json:"backend"`
	GitCommit     string          `json:"git_commit"`
	GoVersion     string          `json:"go_version"`
	OS            string          `json:"os"`
	Arch          string          `json:"arch"`
	StartedAt     string          `json:"started_at"`
	InputDir      string          `json:"input_dir"`
	Repeat        int             `json:"repeat"`
	Warmup        int             `json:"warmup"`
	Operations    []perfOperation `json:"operations"`
}

func (r *perfReport) addOperation(name string, inputs map[string]any, durationsMS []float64) {
	p50, p95, p99 := percentilesMS(durationsMS)
	r.Operations = append(r.Operations, perfOperation{
		Name:        name,
		Inputs:      inputs,
		DurationsMS: durationsMS,
		P50MS:       p50,
		P95MS:       p95,
		P99MS:       p99,
	})
}

type perfDatasetNameVersion struct {
	name    string
	version string
}

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

type perfQueryIDs struct {
	gid     uint32
	groupBD string
	uid     uint32
	userBD  string
}

type perfQueryContext struct {
	datasetDirs []string
	tree        *db.Tree
	bd          basedirs.Reader
	closeFn     func() error
	queryDir    string
	ids         perfQueryIDs
}
