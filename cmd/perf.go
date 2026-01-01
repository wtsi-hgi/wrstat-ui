package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

var (
	perfOwners string
	perfJSON   string
	perfRepeat int
	perfWarmup int
	perfOut    string
	perfQuota  string
	perfConfig string
	perfMax    int
	perfDir    string
	perfSplits int
)

var (
	errNoGroupUsageEntries = errors.New("no group usage entries")
	errNoUserUsageEntries  = errors.New("no user usage entries")
	errInvalidDirInfo      = errors.New("invalid DirInfo")
	errEmptyGroupUsage     = errors.New("empty group usage")
	errEmptyUserUsage      = errors.New("empty user usage")
)

const (
	defaultRepeat = 20
	defaultWarmup = 1
	dirPerm       = 0o755
	splitParts    = 2
	pct50         = 50
	pct95         = 95
	pct99         = 99
	pctMax        = 100
	dirCountMin   = 1000
	dirCountMax   = 20000
)

var perfCmd = &cobra.Command{
	Use:   "bolt-perf",
	Short: "Performance harness for bolt backend (import/query)",
}

var perfQueryCmd = &cobra.Command{
	Use:   "query <inputDir>",
	Short: "Run query suite against bolt DB output directory",
	Args:  cobra.MinimumNArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		inputDir := args[0]

		if perfJSON == "" {
			die("--json is required")
		}

		// minimal implementation: discover dataset dirs and produce a
		// JSON report with required top-level fields.
		absInput, err := filepath.Abs(inputDir)
		if err != nil {
			die("%s", err)
		}

		// build full report by running query suite
		report, err := runQuerySuite(absInput)
		if err != nil {
			die("%s", err)
		}

		// ensure top-level metadata fields are present/updated
		report["go_version"] = runtime.Version()
		report["os"] = runtime.GOOS
		report["arch"] = runtime.GOARCH
		report["started_at"] = time.Now().UTC().Format(time.RFC3339)
		report["input_dir"] = absInput
		report["repeat"] = perfRepeat
		report["warmup"] = perfWarmup

		// mark unused flags as used to satisfy linters
		_ = perfQuota
		_ = perfConfig

		f, err := os.Create(perfJSON)
		if err != nil {
			die("%s", err)
		}

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")

		if err = enc.Encode(report); err != nil {
			f.Close()
			die("%s", err)
		}

		f.Close()
		fmt.Fprintf(os.Stdout, "Wrote %s\n", perfJSON)
	},
}

var perfImportCmd = &cobra.Command{
	Use:   "import <inputDir>",
	Short: "Import stats.gz into bolt DB files",
	Args:  cobra.MinimumNArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		inputDir := args[0]

		if perfOut == "" {
			die("--out is required")
		}

		dirs, err := server.FindDBDirs(inputDir, "stats.gz")
		if err != nil {
			die("%s", err)
		}

		ops := make([]any, 0, len(dirs))

		for _, d := range dirs {
			base := filepath.Base(d)
			outDir := filepath.Join(perfOut, base)
			if errMk := os.MkdirAll(outDir, dirPerm); errMk != nil {
				die("%s", errMk)
			}
			// use real summariser to build DBs
			statsPath := filepath.Join(d, "stats.gz")

			// count records by scanning the (possibly gzipped) file
			rf, errOpen := os.Open(statsPath)
			if errOpen != nil {
				die("%s", errOpen)
			}

			var rcount int
			var scanner *bufio.Scanner
			if strings.HasSuffix(statsPath, ".gz") {
				gr, errr := pgzip.NewReader(rf)
				if errr != nil {
					rf.Close()
					die("%s", errr)
				}

				scanner = bufio.NewScanner(gr)
				for scanner.Scan() {
					rcount++
				}
				gr.Close()
			} else {
				scanner = bufio.NewScanner(rf)
				for scanner.Scan() {
					rcount++
				}
			}

			rf.Close()

			// open stats file for summariser and get modtime
			r, modtime, errOpen2 := openStatsFile(statsPath)
			if errOpen2 != nil {
				die("%s", errOpen2)
			}

			s := summary.NewSummariser(stats.NewStatsParser(r))

			// prepare output paths
			dirgutaPath := filepath.Join(outDir, "dguta.dbs")
			basedirsPath := filepath.Join(outDir, "basedirs.db")

			if errRem := os.Remove(basedirsPath); errRem != nil && !os.IsNotExist(errRem) {
				die("%s", errRem)
			}

			// add basedirs summariser
			if errAdd := addBasedirsSummariser(s, basedirsPath, "", perfQuota, perfConfig, "", modtime); errAdd != nil {
				die("%s", errAdd)
			}

			// add dirguta summariser and get closer
			closeFn, errAdd2 := addDirgutaSummariser(s, dirgutaPath)
			if errAdd2 != nil {
				die("%s", errAdd2)
			}

			start := time.Now()
			if errSumm := s.Summarise(); errSumm != nil {
				if errc := closeFn(); errc != nil {
					die("%s", errc)
				}

				die("%s", errSumm)
			}
			if errc := closeFn(); errc != nil {
				die("%s", errc)
			}

			// set basedirs file mtime to stats file mtime
			if errCht := os.Chtimes(basedirsPath, modtime, modtime); errCht != nil {
				die("%s", errCht)
			}

			dur := time.Since(start)

			mount := deriveMountPath(base)

			op := map[string]any{
				"name": "import_total",
				"inputs": map[string]any{
					"dataset_dir": base,
					"mount_path":  mount,
					"max_lines":   perfMax,
					"records":     rcount,
				},
				"durations_ms": []float64{float64(dur.Milliseconds())},
				"p50_ms":       float64(dur.Milliseconds()),
				"p95_ms":       float64(dur.Milliseconds()),
				"p99_ms":       float64(dur.Milliseconds()),
			}

			ops = append(ops, op)
		}

		// write JSON report
		report := map[string]any{
			"schema_version": 1,
			"backend":        "bolt",
			"git_commit":     "",
			"go_version":     runtime.Version(),
			"os":             runtime.GOOS,
			"arch":           runtime.GOARCH,
			"started_at":     time.Now().UTC().Format(time.RFC3339),
			"input_dir":      inputDir,
			"repeat":         perfRepeat,
			"warmup":         perfWarmup,
			"operations":     ops,
		}

		f, err := os.Create(perfJSON)
		if err != nil {
			die("%s", err)
		}

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			f.Close()
			die("%s", err)
		}

		f.Close()
		fmt.Fprintf(os.Stdout, "Wrote %s\n", perfJSON)
	},
}

func init() {
	RootCmd.AddCommand(perfCmd)

	perfCmd.AddCommand(perfQueryCmd)
	perfCmd.AddCommand(perfImportCmd)

	perfQueryCmd.Flags().StringVar(&perfOwners, "owners", "", "owners CSV for basedirs")
	perfQueryCmd.Flags().StringVar(&perfJSON, "json", "", "path to write JSON report")
	perfQueryCmd.Flags().IntVar(&perfRepeat, "repeat", defaultRepeat, "number of repeats")
	perfQueryCmd.Flags().IntVar(&perfWarmup, "warmup", defaultWarmup, "number of warmups")

	perfImportCmd.Flags().StringVar(&perfOut, "out", "", "output directory for bolt DBs")
	perfImportCmd.Flags().StringVar(&perfQuota, "quota", "", "quota CSV for import")
	perfImportCmd.Flags().StringVar(&perfConfig, "config", "", "basedirs config file")
	perfImportCmd.Flags().IntVar(&perfMax, "maxLines", 0, "max parsed stat records per stats.gz")
	perfImportCmd.Flags().StringVar(&perfOwners, "owners", "", "owners CSV for basedirs")
	perfImportCmd.Flags().StringVar(&perfJSON, "json", "", "path to write JSON report")
}

// runQuerySuite executes the timed query operations and returns the report.
func runQuerySuite(inputDir string) (map[string]any, error) {
	dirs, err := server.FindDBDirs(inputDir, "dguta.dbs", "basedirs.db")
	if err != nil {
		return nil, err
	}

	dgutaPaths := make([]string, 0, len(dirs))

	basedirsPaths := make([]string, 0, len(dirs))
	for _, d := range dirs {
		dgutaPaths = append(dgutaPaths, filepath.Join(d, "dguta.dbs"))
		basedirsPaths = append(basedirsPaths, filepath.Join(d, "basedirs.db"))
	}

	tree, err := db.NewTree(dgutaPaths...)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	bdrs, err := basedirs.OpenMulti(perfOwners, basedirsPaths...)
	if err != nil {
		return nil, err
	}
	defer bdrs.Close()

	// prewarm caches
	if e := prewarmBdrs(bdrs); e != nil {
		return nil, e
	}

	dir, err := chooseRepresentativeDir(tree, dirs)
	if err != nil {
		return nil, err
	}

	gid, baseDir, uid, userBaseDir, err := pickRepresentativeBasedirs(bdrs)
	if err != nil {
		return nil, err
	}

	ops, err := runAllOps(tree, bdrs, dirs, dir, gid, baseDir, uid, userBaseDir)
	if err != nil {
		return nil, err
	}

	report := map[string]any{
		"schema_version": 1,
		"backend":        "bolt",
		"git_commit":     "",
		"operations":     ops,
	}

	return report, nil
}

func prewarmBdrs(bdrs basedirs.MultiReader) error {
	for _, age := range db.DirGUTAges {
		if _, err := bdrs.GroupUsage(age); err != nil {
			return err
		}

		if _, err := bdrs.UserUsage(age); err != nil {
			return err
		}
	}

	return nil
}

// chooseRepresentativeDir picks a mount path to target for tree queries.
func chooseRepresentativeDir(tree *db.Tree, dirs []string) (string, error) {
	if perfDir != "" {
		return perfDir, nil
	}

	mounts := make([]string, 0, len(dirs))
	for _, d := range dirs {
		mounts = append(mounts, deriveMountPath(filepath.Base(d)))
	}

	sort.Strings(mounts)
	dir := mounts[0]

	for i := 0; i < 64; i++ {
		info, err := tree.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})
		if err != nil {
			return "", err
		}

		if info == nil {
			break
		}

		if isAcceptableDir(info) {
			break
		}

		child := bestChild(info)
		if child == nil {
			break
		}

		dir = child.Dir
	}

	return dir, nil
}

func pickRepresentativeBasedirs(bdrs basedirs.MultiReader) (uint32, string, uint32, string, error) {
	gids, err := bdrs.GroupUsage(db.DGUTAgeAll)
	if err != nil {
		return 0, "", 0, "", err
	}

	if len(gids) == 0 {
		return 0, "", 0, "", errNoGroupUsageEntries
	}

	sort.Slice(gids, func(i, j int) bool { return gids[i].UsageSize > gids[j].UsageSize })
	pickedGID := gids[0].GID
	pickedBaseDir := gids[0].BaseDir

	uids, err := bdrs.UserUsage(db.DGUTAgeAll)
	if err != nil {
		return 0, "", 0, "", err
	}

	if len(uids) == 0 {
		return 0, "", 0, "", errNoUserUsageEntries
	}

	sort.Slice(uids, func(i, j int) bool { return uids[i].UsageSize > uids[j].UsageSize })
	pickedUID := uids[0].UID
	pickedUserBaseDir := uids[0].BaseDir

	return pickedGID, pickedBaseDir, pickedUID, pickedUserBaseDir, nil
}

func runAllOps(
	tree *db.Tree,
	bdrs basedirs.MultiReader,
	dirs []string,
	dir string,
	gid uint32,
	baseDir string,
	uid uint32,
	userBaseDir string,
) ([]any, error) {
	ops := make([]any, 0)

	fns := []func() (map[string]any, error){
		func() (map[string]any, error) { return opMountTimestamps(dirs) },
		func() (map[string]any, error) { return opTreeDirInfo(tree, dir) },
		func() (map[string]any, error) { return opTreeWhere(tree, dir, perfSplits) },
		func() (map[string]any, error) { return opBasedirsGroupUsage(bdrs) },
		func() (map[string]any, error) { return opBasedirsUserUsage(bdrs) },
		func() (map[string]any, error) { return opBasedirsGroupSubdirs(bdrs, gid, baseDir) },
		func() (map[string]any, error) { return opBasedirsUserSubdirs(bdrs, uid, userBaseDir) },
		func() (map[string]any, error) { return opBasedirsHistory(bdrs, gid, baseDir) },
	}

	for _, fn := range fns {
		rec, err := fn()
		if err != nil {
			return nil, err
		}

		ops = append(ops, rec)
	}

	return ops, nil
}

func opMountTimestamps(dirs []string) (map[string]any, error) {
	mtInputs := map[string]any{"mount_paths": func() []string {
		mps := make([]string, 0, len(dirs))
		for _, d := range dirs {
			mps = append(mps, deriveMountPath(filepath.Base(d)))
		}

		sort.Strings(mps)

		return mps
	}()}

	return runOp("mount_timestamps", mtInputs, func() error {
		for _, d := range dirs {
			if _, err := os.Stat(d); err != nil {
				return err
			}
		}

		return nil
	})
}

func deriveMountPath(basename string) string {
	// split at first '_'
	parts := strings.SplitN(basename, "_", splitParts)
	if len(parts) < splitParts {
		return "/"
	}

	mount := parts[1]
	// replace fullwidth solidus with '/'
	mount = strings.ReplaceAll(mount, "／", "/")

	if !strings.HasSuffix(mount, "/") {
		mount += "/"
	}

	return mount
}

func opTreeDirInfo(tree *db.Tree, dir string) (map[string]any, error) {
	tdInputs := map[string]any{"dir": dir, "age": db.DGUTAgeAll}

	return runOp("tree_dirinfo", tdInputs, func() error {
		_, err := tree.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})

		return err
	})
}

func opTreeWhere(tree *db.Tree, dir string, splits int) (map[string]any, error) {
	twInputs := map[string]any{"dir": dir, "age": db.DGUTAgeAll, "splits": splits}

	return runOp("tree_where", twInputs, func() error {
		_, err := tree.Where(dir, &db.Filter{Age: db.DGUTAgeAll}, split.SplitsToSplitFn(splits))

		return err
	})
}

func opBasedirsGroupUsage(bdrs basedirs.MultiReader) (map[string]any, error) {
	bguInputs := map[string]any{"age": db.DGUTAgeAll}

	return runOp("basedirs_group_usage", bguInputs, func() error {
		u, err := bdrs.GroupUsage(db.DGUTAgeAll)
		if err != nil {
			return err
		}

		if len(u) == 0 {
			return errEmptyGroupUsage
		}

		return nil
	})
}

func opBasedirsUserUsage(bdrs basedirs.MultiReader) (map[string]any, error) {
	buuInputs := map[string]any{"age": db.DGUTAgeAll}

	return runOp("basedirs_user_usage", buuInputs, func() error {
		u, err := bdrs.UserUsage(db.DGUTAgeAll)
		if err != nil {
			return err
		}

		if len(u) == 0 {
			return errEmptyUserUsage
		}

		return nil
	})
}

func opBasedirsGroupSubdirs(bdrs basedirs.MultiReader, gid uint32, basedir string) (map[string]any, error) {
	bgsInputs := map[string]any{"gid": gid, "basedir": basedir, "age": db.DGUTAgeAll}

	return runOp("basedirs_group_subdirs", bgsInputs, func() error {
		_, err := bdrs.GroupSubDirs(gid, basedir, db.DGUTAgeAll)

		return err
	})
}

func opBasedirsUserSubdirs(bdrs basedirs.MultiReader, uid uint32, basedir string) (map[string]any, error) {
	busInputs := map[string]any{"uid": uid, "basedir": basedir, "age": db.DGUTAgeAll}

	return runOp("basedirs_user_subdirs", busInputs, func() error {
		_, err := bdrs.UserSubDirs(uid, basedir, db.DGUTAgeAll)

		return err
	})
}

func opBasedirsHistory(bdrs basedirs.MultiReader, gid uint32, basedir string) (map[string]any, error) {
	bhInputs := map[string]any{"gid": gid, "basedir": basedir}

	return runOp("basedirs_history", bhInputs, func() error {
		_, err := bdrs.History(gid, basedir)

		return err
	})
}

func runOp(name string, inputs map[string]any, fn func() error) (map[string]any, error) {
	for i := 0; i < perfWarmup; i++ {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	durations := make([]float64, 0, perfRepeat)
	for i := 0; i < perfRepeat; i++ {
		start := time.Now()

		if err := fn(); err != nil {
			return nil, err
		}

		durations = append(durations, float64(time.Since(start).Milliseconds()))
	}

	return map[string]any{
		"name":         name,
		"inputs":       inputs,
		"durations_ms": durations,
		"p50_ms":       percentile(durations, pct50),
		"p95_ms":       percentile(durations, pct95),
		"p99_ms":       percentile(durations, pct99),
	}, nil
}

// percentile computes the integer percentile (e.g. 50,95,99) from a
// slice of durations in milliseconds. Returns 0 for empty input.
func percentile(durations []float64, p int) float64 {
	if len(durations) == 0 {
		return 0
	}

	ds := make([]float64, len(durations))
	copy(ds, durations)
	sort.Float64s(ds)

	if p <= 0 {
		return ds[0]
	}

	if p >= pctMax {
		return ds[len(ds)-1]
	}

	rank := int((float64(p) / float64(pctMax)) * float64(len(ds)))
	if rank <= 0 {
		rank = 1
	}

	if rank > len(ds) {
		rank = len(ds)
	}

	return ds[rank-1]
}

func isAcceptableDir(info *db.DirInfo) bool {
	if info == nil || info.Current == nil {
		return false
	}

	return info.Current.Count >= dirCountMin && info.Current.Count <= dirCountMax
}

func bestChild(info *db.DirInfo) *db.DirSummary {
	if info == nil || len(info.Children) == 0 {
		return nil
	}

	best := info.Children[0]
	for _, c := range info.Children {
		if c.Count > best.Count {
			best = c
		}
	}

	return best
}
