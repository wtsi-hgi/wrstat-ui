package cmd

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
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/server"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	boltPerfDefaultRepeat = 20
	boltPerfDefaultWarmup = 1
	boltPerfDefaultSplits = 4

	defaultDirPerm = 0o755

	lineReaderBufSize = 32 * 1024

	dirPickMinCount      = 1000
	dirPickMaxCount      = 20000
	dirPickMaxIterations = 128
)

var (
	errBoltPerfUnknownBackend = errors.New("unknown backend")
	errBoltPerfNoDatasets     = errors.New("no datasets found")
)

var boltPerfCmd = &cobra.Command{
	Use:   "bolt-perf",
	Short: "Run in-process performance harness against Bolt-backed databases",
}

var boltPerfImportCmd = &cobra.Command{
	Use:   "import <inputDir>",
	Short: "Import stats.gz datasets into Bolt databases",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		return runBoltPerfImport(args[0])
	},
}

var boltPerfQueryCmd = &cobra.Command{
	Use:   "query <inputDir>",
	Short: "Run query timing suite against imported Bolt databases",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		return runBoltPerfQuery(args[0])
	},
}

type boltPerfFlags struct {
	backend string
	owners  string
	mounts  string
	jsonOut string

	outDir   string
	quota    string
	config   string
	maxLines int

	dir    string
	repeat int
	warmup int
	splits int
}

var boltPerf boltPerfFlags

type lineCountingReader struct {
	underlying io.Reader
	maxLines   uint64

	buf        []byte
	pending    []byte
	seenLines  uint64
	reachedMax bool
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

func (l *lineCountingReader) linesRead() uint64 {
	return l.seenLines
}

func (l *lineCountingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if n := l.readPending(p); n > 0 {
		return n, nil
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

	if l.reachedMax {
		return nn, nil
	}

	return nn, err
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

func importOneDataset(spec boltPerfImportSpec) (_ uint64, err error) {
	gz, closeStatsFn, err := openStatsGZReader(spec.statsGZPath)
	if err != nil {
		return 0, err
	}
	defer setErrOnClose(&err, closeStatsFn)

	lr := newLineCountingReader(gz, spec.maxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	closeSummFn, err := addBoltPerfSummarisers(
		ss,
		boltPerfImportTargets{basedirsDBPath: spec.basedirsDBPath, dgutaDBDir: spec.dgutaDBDir},
		spec.modtime,
	)
	if err != nil {
		return 0, err
	}

	if closeSummFn != nil {
		defer setErrOnClose(&err, closeSummFn)
	}

	if err := ss.Summarise(); err != nil {
		return 0, err
	}

	return lr.linesRead(), nil
}

func openStatsGZReader(path string) (*pgzip.Reader, errCloseFunc, error) {
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

type errCloseFunc = func() error

func setErrOnClose(errp *error, closeFn errCloseFunc) {
	if errp == nil || *errp != nil {
		return
	}

	if cerr := closeFn(); cerr != nil {
		*errp = cerr
	}
}

func addBoltPerfSummarisers(
	ss *summary.Summariser,
	targets boltPerfImportTargets,
	modtime time.Time,
) (errCloseFunc, error) {
	if err := addBasedirsSummariser(
		ss,
		targets.basedirsDBPath,
		"",
		boltPerf.quota,
		boltPerf.config,
		boltPerf.mounts,
		modtime,
	); err != nil {
		return nil, err
	}

	return addDirgutaSummariser(ss, targets.dgutaDBDir)
}

type boltPerfImportSpec struct {
	statsGZPath    string
	basedirsDBPath string
	dgutaDBDir     string
	modtime        time.Time
	maxLines       int
}

func importDataset(datasetDir string) (uint64, error) {
	base, spec, err := buildBoltPerfImportSpec(datasetDir)
	if err != nil {
		return 0, err
	}

	records, seconds, err := timedImportOneDataset(spec)
	if err != nil {
		return 0, err
	}

	cliPrint("import dataset=%s records=%d seconds=%.3f\n", base, records, seconds)

	return records, nil
}

func buildBoltPerfImportSpec(datasetDir string) (string, boltPerfImportSpec, error) {
	base := filepath.Base(datasetDir)
	statsGZPath := filepath.Join(datasetDir, inputStatsFile)

	st, err := os.Stat(statsGZPath)
	if err != nil {
		return "", boltPerfImportSpec{}, err
	}

	outDatasetDir := filepath.Join(boltPerf.outDir, base)
	if mkErr := os.MkdirAll(outDatasetDir, defaultDirPerm); mkErr != nil {
		return "", boltPerfImportSpec{}, mkErr
	}

	spec := boltPerfImportSpec{
		statsGZPath:    statsGZPath,
		basedirsDBPath: filepath.Join(outDatasetDir, basedirBasename),
		dgutaDBDir:     filepath.Join(outDatasetDir, dgutaDBsSuffix),
		modtime:        st.ModTime(),
		maxLines:       boltPerf.maxLines,
	}

	return base, spec, nil
}

func timedImportOneDataset(spec boltPerfImportSpec) (uint64, float64, error) {
	start := time.Now()

	records, err := importOneDataset(spec)
	if err != nil {
		return 0, 0, err
	}

	return records, time.Since(start).Seconds(), nil
}

type boltPerfImportTargets struct {
	basedirsDBPath string
	dgutaDBDir     string
}

type queryIDs struct {
	gid     uint32
	groupBD string
	uid     uint32
	userBD  string
}

func pickRepresentativeIDs(mr basedirs.MultiReader, fallbackDir string) (queryIDs, error) {
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

type boltPerfQueryContext struct {
	datasetDirs []string
	tree        *db.Tree
	mr          basedirs.MultiReader
	closeFn     func() error
	queryDir    string
	ids         queryIDs
}

func buildBoltPerfQueryContext(inputDir string) (boltPerfQueryContext, error) {
	datasetDirs, datasetDir, err := discoverBoltPerfQueryDatasets(inputDir)
	if err != nil {
		return boltPerfQueryContext{}, err
	}

	return buildBoltPerfQueryContextForDataset(datasetDirs, datasetDir)
}

func buildBoltPerfQueryContextForDataset(datasetDirs []string, datasetDir string) (boltPerfQueryContext, error) {
	mountPath, queryDir, err := deriveMountAndQueryDir(datasetDir)
	if err != nil {
		return boltPerfQueryContext{}, err
	}

	tree, mr, closeFn, err := openBoltQueryDBs(datasetDir)
	if err != nil {
		return boltPerfQueryContext{}, err
	}

	prepErr := prepareBoltQueryMultiReader(mr, boltPerf.mounts)
	if prepErr != nil {
		return boltPerfQueryContext{}, closeAndJoinErr(closeFn, prepErr)
	}

	ids, err := pickRepresentativeIDs(mr, mountPath)
	if err != nil {
		return boltPerfQueryContext{}, closeAndJoinErr(closeFn, err)
	}

	return boltPerfQueryContext{
		datasetDirs: datasetDirs,
		tree:        tree,
		mr:          mr,
		closeFn:     closeFn,
		queryDir:    queryDir,
		ids:         ids,
	}, nil
}

func deriveMountAndQueryDir(datasetDir string) (string, string, error) {
	mountPath, err := deriveMountPathFromDatasetDirName(filepath.Base(datasetDir))
	if err != nil {
		return "", "", err
	}

	return mountPath, resolveQueryDir(datasetDir, mountPath), nil
}

func prepareBoltQueryMultiReader(mr basedirs.MultiReader, mountsFile string) error {
	if err := maybeSetMountPointsFromFile(mr, mountsFile); err != nil {
		return err
	}

	return prewarmBasedirsCaches(mr)
}

func runBoltPerfQuery(inputDir string) (err error) {
	validateErr := validateBoltPerfBackend(boltPerf.backend)
	if validateErr != nil {
		return validateErr
	}

	ctx, err := buildBoltPerfQueryContext(inputDir)
	if err != nil {
		return err
	}
	defer setErrOnClose(&err, ctx.closeFn)

	report := newPerfReport(boltPerf.backend, inputDir, boltPerf.repeat, boltPerf.warmup)
	if err := runBoltQuerySuite(&report, ctx); err != nil {
		return err
	}

	return writePerfReport(boltPerf.jsonOut, report)
}

func validateBoltPerfBackend(backend string) error {
	switch backend {
	case "bolt", "bolt_interfaces":
		return nil
	default:
		return fmt.Errorf("%w: %q", errBoltPerfUnknownBackend, backend)
	}
}

func runBoltQuerySuite(report *perfReport, ctx boltPerfQueryContext) error {
	ops := []func() error{
		func() error { return runMountTimestampsOp(report, ctx.datasetDirs) },
		func() error { return runTreeDirInfoOp(report, ctx.tree, ctx.queryDir) },
		func() error { return runTreeWhereOp(report, ctx.tree, ctx.queryDir) },
		func() error { return runBasedirsGroupUsageOp(report, ctx.mr) },
		func() error { return runBasedirsUserUsageOp(report, ctx.mr) },
		func() error { return runBasedirsGroupSubDirsOp(report, ctx.mr, ctx.ids.gid, ctx.ids.groupBD) },
		func() error { return runBasedirsUserSubDirsOp(report, ctx.mr, ctx.ids.uid, ctx.ids.userBD) },
		func() error { return runBasedirsHistoryOp(report, ctx.mr, ctx.ids.gid, ctx.ids.groupBD) },
	}

	for _, op := range ops {
		if err := op(); err != nil {
			return err
		}
	}

	return nil
}

func runMountTimestampsOp(report *perfReport, datasetDirs []string) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		for _, datasetDir := range datasetDirs {
			base := filepath.Base(datasetDir)

			_, err := deriveMountPathFromDatasetDirName(base)
			if err != nil {
				return err
			}

			_, err = os.Stat(filepath.Join(datasetDir, dgutaDBsSuffix))
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	addOperation(report, "mount_timestamps", map[string]any{"datasets": len(datasetDirs)}, durations)
	cliPrintOperation("mount_timestamps", durations)

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

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func cliPrintOperation(name string, durations []float64) {
	cliPrint("%s repeats=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n",
		name, len(durations),
		percentileMS(durations, percentileP50),
		percentileMS(durations, percentileP95),
		percentileMS(durations, percentileP99),
	)
}

func runTreeDirInfoOp(report *perfReport, tree *db.Tree, dir string) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}

	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := tree.DirInfo(dir, filter)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "tree_dirinfo", map[string]any{"dir": dir}, durations)
	cliPrintOperation("tree_dirinfo", durations)

	return nil
}

func runTreeWhereOp(report *perfReport, tree *db.Tree, dir string) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	splitFn := split.SplitsToSplitFn(boltPerf.splits)

	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := tree.Where(dir, filter, splitFn)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "tree_where", map[string]any{"dir": dir, "splits": boltPerf.splits}, durations)
	cliPrintOperation("tree_where", durations)

	return nil
}

func runBasedirsGroupUsageOp(report *perfReport, mr basedirs.MultiReader) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := mr.GroupUsage(db.DGUTAgeAll)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "basedirs_group_usage", map[string]any{"age": int(db.DGUTAgeAll)}, durations)
	cliPrintOperation("basedirs_group_usage", durations)

	return nil
}

func runBasedirsUserUsageOp(report *perfReport, mr basedirs.MultiReader) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := mr.UserUsage(db.DGUTAgeAll)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "basedirs_user_usage", map[string]any{"age": int(db.DGUTAgeAll)}, durations)
	cliPrintOperation("basedirs_user_usage", durations)

	return nil
}

func runBasedirsGroupSubDirsOp(report *perfReport, mr basedirs.MultiReader, gid uint32, basedir string) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := mr.GroupSubDirs(gid, basedir, db.DGUTAgeAll)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "basedirs_group_subdirs", map[string]any{"gid": gid, "basedir": basedir}, durations)
	cliPrintOperation("basedirs_group_subdirs", durations)

	return nil
}

func runBasedirsUserSubDirsOp(report *perfReport, mr basedirs.MultiReader, uid uint32, basedir string) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := mr.UserSubDirs(uid, basedir, db.DGUTAgeAll)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "basedirs_user_subdirs", map[string]any{"uid": uid, "basedir": basedir}, durations)
	cliPrintOperation("basedirs_user_subdirs", durations)

	return nil
}

func runBasedirsHistoryOp(report *perfReport, mr basedirs.MultiReader, gid uint32, path string) error {
	durations, err := measureOperation(boltPerf.warmup, boltPerf.repeat, func() error {
		_, err := mr.History(gid, path)

		return err
	})
	if err != nil {
		return err
	}

	addOperation(report, "basedirs_history", map[string]any{"gid": gid, "path": path}, durations)
	cliPrintOperation("basedirs_history", durations)

	return nil
}

func resolveQueryDir(datasetDir, mountPath string) string {
	queryDir := normaliseDirPath(boltPerf.dir)
	if queryDir != "" {
		return queryDir
	}

	return pickRepresentativeDir(datasetDir, mountPath)
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

func pickRepresentativeDir(datasetDir, mountPath string) string {
	tree, err := db.NewTree(filepath.Join(datasetDir, dgutaDBsSuffix))
	if err != nil {
		return mountPath
	}

	defer func() {
		tree.Close()
	}()

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

func closeAndJoinErr(closeFn func() error, err error) error {
	if closeFn == nil {
		return err
	}

	if cerr := closeFn(); cerr != nil {
		return errors.Join(err, cerr)
	}

	return err
}

func discoverBoltPerfQueryDatasets(inputDir string) ([]string, string, error) {
	datasetDirs, err := findDatasetDirs(inputDir, dgutaDBsSuffix, basedirBasename)
	if err != nil {
		return nil, "", err
	}

	datasetDir := datasetDirs[0]
	if len(datasetDirs) > 1 {
		cliPrint("query: %d datasets found; using %s\n", len(datasetDirs), filepath.Base(datasetDir))
	}

	return datasetDirs, datasetDir, nil
}

func runBoltPerfImport(inputDir string) error {
	if err := validateBoltPerfBackend(boltPerf.backend); err != nil {
		return err
	}

	datasetDirs, err := findDatasetDirs(inputDir, inputStatsFile)
	if err != nil {
		return err
	}

	report := newPerfReport(boltPerf.backend, inputDir, boltPerf.repeat, boltPerf.warmup)

	startAll := time.Now()

	totalRecords, err := importDatasets(datasetDirs)
	if err != nil {
		return err
	}

	addOperation(
		&report,
		"import_total",
		map[string]any{"datasets": len(datasetDirs), "records": totalRecords},
		[]float64{durationMS(time.Since(startAll))},
	)

	return writePerfReport(boltPerf.jsonOut, report)
}

func findDatasetDirs(baseDir string, required ...string) ([]string, error) {
	dirs, err := server.FindDBDirs(baseDir, required...)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, fmt.Errorf("%w: %q", errBoltPerfNoDatasets, baseDir)
	}

	sort.Strings(dirs)

	return dirs, nil
}

func importDatasets(datasetDirs []string) (uint64, error) {
	var totalRecords uint64

	for _, datasetDir := range datasetDirs {
		records, err := importDataset(datasetDir)
		if err != nil {
			return 0, err
		}

		totalRecords += records
	}

	return totalRecords, nil
}

func maybeSetMountPointsFromFile(mr basedirs.MultiReader, mountsPath string) error {
	if mountsPath == "" {
		return nil
	}

	mounts, err := parseMountpointsFromFile(mountsPath)
	if err != nil {
		return err
	}

	if len(mounts) == 0 {
		return nil
	}

	mr.SetMountPoints(mounts)

	return nil
}

func openBoltQueryDBs(datasetDir string) (*db.Tree, basedirs.MultiReader, func() error, error) {
	dgutaPath := filepath.Join(datasetDir, dgutaDBsSuffix)
	basedirsPath := filepath.Join(datasetDir, basedirBasename)

	tree, err := db.NewTree(dgutaPath)
	if err != nil {
		return nil, nil, nil, err
	}

	mr, err := basedirs.OpenMulti(boltPerf.owners, basedirsPath)
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

func prewarmBasedirsCaches(mr basedirs.MultiReader) error {
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

func init() {
	RootCmd.AddCommand(boltPerfCmd)

	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.backend, "backend", "bolt",
		"backend name: bolt or bolt_interfaces")
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.owners, "owners", "", "path to gid,owner CSV")
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.mounts, "mounts", "", "path to mounts file")
	boltPerfCmd.PersistentFlags().IntVar(&boltPerf.repeat, "repeat", boltPerfDefaultRepeat,
		"number of timed repeats per operation")
	boltPerfCmd.PersistentFlags().IntVar(&boltPerf.warmup, "warmup", boltPerfDefaultWarmup,
		"number of warmup runs per operation")
	boltPerfCmd.PersistentFlags().IntVar(&boltPerf.splits, "splits", boltPerfDefaultSplits,
		"splits for tree_where")
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.jsonOut, "json", "", "write JSON report to this path")

	boltPerfCmd.AddCommand(boltPerfImportCmd)
	boltPerfCmd.AddCommand(boltPerfQueryCmd)

	boltPerfImportCmd.Flags().StringVar(&boltPerf.outDir, "out", "", "output dir")
	boltPerfImportCmd.Flags().StringVar(&boltPerf.quota, "quota", "", "quota CSV")
	boltPerfImportCmd.Flags().StringVar(&boltPerf.config, "config", "", "basedirs config TSV")
	boltPerfImportCmd.Flags().IntVar(&boltPerf.maxLines, "maxLines", 0, "max stats lines to parse")

	boltPerfQueryCmd.Flags().StringVar(&boltPerf.dir, "dir", "", "directory to use for tree operations")

	mustMarkRequired(boltPerfCmd, "owners")
	mustMarkRequired(boltPerfCmd, "json")
	mustMarkRequired(boltPerfImportCmd, "out")
	mustMarkRequired(boltPerfImportCmd, "quota")
	mustMarkRequired(boltPerfImportCmd, "config")
}

func mustMarkRequired(cmd *cobra.Command, name string) {
	if err := cmd.MarkFlagRequired(name); err == nil {
		return
	}

	err := cmd.MarkPersistentFlagRequired(name)
	if err == nil {
		return
	}

	panic(err)
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
