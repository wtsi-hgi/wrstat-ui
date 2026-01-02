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
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/server"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	defaultDirPerm = 0o755

	dgutaDBsSuffix    = "dguta.dbs"
	basedirsBasename  = "basedirs.db"
	statsGZBasename   = "stats.gz"
	lineReaderBufSize = 32 * 1024

	dirPickMinCount      = 1000
	dirPickMaxCount      = 20000
	dirPickMaxIterations = 128
)

var (
	// ErrUnknownBackend indicates an unsupported backend name was provided.
	ErrUnknownBackend = errors.New("unknown backend")
	// ErrNoDatasets indicates no matching dataset directories were discovered.
	ErrNoDatasets = errors.New("no datasets found")
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
	dirs, err := server.FindDBDirs(baseDir, required...)
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
	bdClose, err := summariseutil.AddBasedirsSummariser(
		ss,
		targets.basedirsDBPath,
		"",
		opts.Quota,
		opts.Config,
		opts.Mounts,
		modtime,
	)
	if err != nil {
		return nil, err
	}

	dgClose, err := summariseutil.AddDirgutaSummariser(ss, targets.dgutaDBDir)
	if err != nil {
		if bdClose != nil {
			return nil, errors.Join(err, bdClose())
		}

		return nil, err
	}

	return func() error {
		var err error
		if bdClose != nil {
			err = errors.Join(err, bdClose())
		}

		if dgClose != nil {
			err = errors.Join(err, dgClose())
		}

		return err
	}, nil
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

	queryDir := resolveQueryDir(datasetDir, mountPath, opts.Dir)
	if strings.TrimSpace(opts.Dir) == "" {
		printf("query: auto-selected dir=%s\n", queryDir)
	}

	tree, mr, closeFn, err := openQueryDBs(datasetDir, opts.Owners)
	if err != nil {
		return queryContext{}, err
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
		queryDir:    queryDir,
		ids:         ids,
	}, nil
}

func resolveQueryDir(datasetDir, mountPath, override string) string {
	queryDir := normaliseDirPath(override)
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
	database, err := bolt.OpenDatabase(filepath.Join(datasetDir, dgutaDBsSuffix))
	if err != nil {
		return mountPath
	}

	defer func() {
		_ = database.Close()
	}()

	tree := db.NewTree(database)
	defer tree.Close()

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

func openQueryDBs(datasetDir string, ownersPath string) (*db.Tree, basedirs.Reader, func() error, error) {
	dgutaPath := filepath.Join(datasetDir, dgutaDBsSuffix)
	basedirsPath := filepath.Join(datasetDir, basedirsBasename)

	database, err := bolt.OpenDatabase(dgutaPath)
	if err != nil {
		return nil, nil, nil, err
	}

	tree := db.NewTree(database)

	mr, err := bolt.OpenMultiBaseDirsReader(ownersPath, basedirsPath)
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

func runQuerySuite(report *Report, ctx queryContext, opts QueryOptions, printf PrintfFunc) error {
	for _, op := range buildQuerySuiteOps(ctx, opts) {
		if err := timeAndReportQueryOp(report, opts, printf, op.name, op.inputs, op.op); err != nil {
			return err
		}
	}

	return nil
}

func buildQuerySuiteOps(ctx queryContext, opts QueryOptions) []querySuiteOp {
	return []querySuiteOp{
		{
			name:   "mount_timestamps",
			inputs: map[string]any{"datasets": len(ctx.datasetDirs)},
			op:     func() error { return opMountTimestamps(ctx) },
		},
		{
			name:   "tree_dirinfo",
			inputs: map[string]any{"dir": ctx.queryDir, "age": int(db.DGUTAgeAll)},
			op:     func() error { return opTreeDirInfo(ctx) },
		},
		{
			name: "tree_where",
			inputs: map[string]any{
				"dir":    ctx.queryDir,
				"age":    int(db.DGUTAgeAll),
				"splits": opts.Splits,
			},
			op: func() error { return opTreeWhere(ctx, opts.Splits) },
		},
		{
			name:   "basedirs_group_usage",
			inputs: map[string]any{"age": int(db.DGUTAgeAll)},
			op:     func() error { return opBasedirsGroupUsage(ctx) },
		},
		{
			name:   "basedirs_user_usage",
			inputs: map[string]any{"age": int(db.DGUTAgeAll)},
			op:     func() error { return opBasedirsUserUsage(ctx) },
		},
		{
			name: "basedirs_group_subdirs",
			inputs: map[string]any{
				"gid":     ctx.ids.gid,
				"basedir": ctx.ids.groupBD,
				"age":     int(db.DGUTAgeAll),
			},
			op: func() error { return opBasedirsGroupSubDirs(ctx) },
		},
		{
			name: "basedirs_user_subdirs",
			inputs: map[string]any{
				"uid":     ctx.ids.uid,
				"basedir": ctx.ids.userBD,
				"age":     int(db.DGUTAgeAll),
			},
			op: func() error { return opBasedirsUserSubDirs(ctx) },
		},
		{
			name:   "basedirs_history",
			inputs: map[string]any{"gid": ctx.ids.gid, "basedir": ctx.ids.groupBD},
			op:     func() error { return opBasedirsHistory(ctx) },
		},
	}
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

func opTreeDirInfo(ctx queryContext) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	_, err := ctx.tree.DirInfo(ctx.queryDir, filter)

	return err
}

func opTreeWhere(ctx queryContext, splits int) error {
	filter := &db.Filter{Age: db.DGUTAgeAll}
	splitFn := split.SplitsToSplitFn(splits)
	_, err := ctx.tree.Where(ctx.queryDir, filter, splitFn)

	return err
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
	name string,
	inputs map[string]any,
	op func() error,
) error {
	durations, err := measureOperation(opts.Warmup, opts.Repeat, op)
	if err != nil {
		return err
	}

	report.AddOperation(name, inputs, durations)

	p50, p95, p99 := PercentilesMS(durations)
	printf("%s repeats=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n", name, len(durations), p50, p95, p99)

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
}

// QueryOptions configures the bolt-perf query harness.
type QueryOptions struct {
	Backend string
	Owners  string
	Mounts  string
	JSONOut string

	Dir    string
	Repeat int
	Warmup int
	Splits int
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
	datasetDirs []string
	tree        *db.Tree
	mr          basedirs.Reader
	closeFn     func() error
	queryDir    string
	ids         queryIDs
}

type querySuiteOp struct {
	name   string
	inputs map[string]any
	op     func() error
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
