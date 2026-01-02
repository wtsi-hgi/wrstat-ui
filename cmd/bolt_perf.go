package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
)

const (
	boltPerfDefaultRepeat = 20
	boltPerfDefaultWarmup = 1
	boltPerfDefaultSplits = 4
)

var boltPerfCmd = &cobra.Command{
	Use:   "bolt-perf",
	Short: "Run in-process performance harness against Bolt-backed databases",
	Long: `bolt-perf runs an in-process timing harness against Bolt-backed databases.

Use the import subcommand to create Bolt databases from a stats.gz dataset.
Use the query subcommand to run a repeatable timing suite and write a JSON report.
`,
}

var boltPerfImportCmd = &cobra.Command{
	Use:   "import <inputDir>",
	Short: "Import stats.gz datasets into Bolt databases",
	Long: `Import reads one or more datasets under <inputDir> and creates Bolt
databases for each dataset.

The positional argument <inputDir> is a directory containing dataset
subdirectories (as produced by 'wrstat multi'), each named:

	<version>_<mountKey>

For import, each dataset directory must contain a 'stats.gz'.

This command writes output under --out, creating:

	<out>/<version>_<mountKey>/dguta.dbs/
	<out>/<version>_<mountKey>/basedirs.db

Examples:

	# Import all discovered stats.gz datasets into Bolt DBs.
	wrstat-ui bolt-perf import /path/to/stats-input \
		--out /path/to/bolt-out \
		--owners /path/to/owners.csv \
		--quota /path/to/quota.csv \
		--config /path/to/basedirs.config \
		--json /tmp/bolt_import.json

	# Import only the first 1,000,000 lines of each stats.gz (for quick trials).
	wrstat-ui bolt-perf import /path/to/stats-input \
		--out /path/to/bolt-out \
		--owners /path/to/owners.csv \
		--quota /path/to/quota.csv \
		--config /path/to/basedirs.config \
		--max-lines 1000000 \
		--json /tmp/bolt_import_1m.json
`,
	Args: cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		return runBoltPerfImport(args[0])
	},
}

var boltPerfQueryCmd = &cobra.Command{
	Use:   "query <inputDir>",
	Short: "Run query timing suite against imported Bolt databases",
	Long: `Query runs a repeatable timing suite against Bolt databases.

The positional argument <inputDir> is a directory containing dataset
subdirectories created by 'bolt-perf import' (or by 'wrstat summarise'), each
named:

	<version>_<mountKey>

For query, each dataset directory must contain both:

	dguta.dbs/
	basedirs.db

The report is always written to --json, and a human-readable summary is printed
to stdout.

Examples:

	# Run the timing suite against the imported Bolt DBs.
	wrstat-ui bolt-perf query /path/to/bolt-out \
		--owners /path/to/owners.csv \
		--json /tmp/bolt_query.json

	# Pin the tree queries to a specific directory and adjust where() splits.
	wrstat-ui bolt-perf query /path/to/bolt-out \
		--owners /path/to/owners.csv \
		--dir /lustre/some/project/ \
		--splits 4 \
		--json /tmp/bolt_query_dir.json
`,
	Args: cobra.ExactArgs(1),
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

func runBoltPerfImport(inputDir string) error {
	opts := boltperf.ImportOptions{
		Backend:          boltPerf.backend,
		Owners:           boltPerf.owners,
		Mounts:           boltPerf.mounts,
		JSONOut:          boltPerf.jsonOut,
		OutDir:           boltPerf.outDir,
		Quota:            boltPerf.quota,
		Config:           boltPerf.config,
		MaxLines:         boltPerf.maxLines,
		Repeat:           boltPerf.repeat,
		Warmup:           boltPerf.warmup,
		NewDGUTAWriter:   bolt.NewDGUTAWriter,
		NewBaseDirsStore: bolt.NewBaseDirsStore,
	}

	return boltperf.Import(inputDir, opts, cliPrint)
}

func runBoltPerfQuery(inputDir string) error {
	opts := boltperf.QueryOptions{
		Backend:                 boltPerf.backend,
		Owners:                  boltPerf.owners,
		Mounts:                  boltPerf.mounts,
		JSONOut:                 boltPerf.jsonOut,
		Dir:                     boltPerf.dir,
		Repeat:                  boltPerf.repeat,
		Warmup:                  boltPerf.warmup,
		Splits:                  boltPerf.splits,
		OpenDatabase:            bolt.OpenDatabase,
		OpenMultiBaseDirsReader: bolt.OpenMultiBaseDirsReader,
	}

	return boltperf.Query(inputDir, opts, cliPrint)
}

func init() {
	RootCmd.AddCommand(boltPerfCmd)
	boltPerfCmd.AddCommand(boltPerfImportCmd)
	boltPerfCmd.AddCommand(boltPerfQueryCmd)

	addBoltPerfFlags()
	markBoltPerfRequiredFlags()
}

func addBoltPerfFlags() {
	boltPerfCmd.PersistentFlags().StringVar(
		&boltPerf.backend,
		"backend",
		"bolt",
		"backend: bolt or bolt_interfaces",
	)
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.owners, "owners", "", "owners csv")
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.mounts, "mounts", "", "mountpoints file")
	boltPerfCmd.PersistentFlags().StringVar(&boltPerf.jsonOut, "json", "", "write JSON report to this file")

	boltPerfImportCmd.Flags().StringVar(&boltPerf.outDir, "out", "", "output directory")
	boltPerfImportCmd.Flags().StringVar(&boltPerf.quota, "quota", "", "quota csv")
	boltPerfImportCmd.Flags().StringVar(&boltPerf.config, "config", "", "basedirs config")
	boltPerfImportCmd.Flags().IntVar(&boltPerf.maxLines, "max-lines", 0, "max lines of stats.gz to import (0 for all)")
	boltPerfImportCmd.Flags().IntVar(&boltPerf.repeat, "repeat", boltPerfDefaultRepeat, "repeat count")
	boltPerfImportCmd.Flags().IntVar(&boltPerf.warmup, "warmup", boltPerfDefaultWarmup, "warmup iterations")

	boltPerfQueryCmd.Flags().StringVar(&boltPerf.dir, "dir", "", "directory to query (default: auto)")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.repeat, "repeat", boltPerfDefaultRepeat, "repeat count")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.warmup, "warmup", boltPerfDefaultWarmup, "warmup iterations")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.splits, "splits", boltPerfDefaultSplits, "where() splits")
}

func markBoltPerfRequiredFlags() {
	mustMarkPersistentRequired(boltPerfCmd, "owners")
	mustMarkPersistentRequired(boltPerfCmd, "json")

	mustMarkRequired(boltPerfImportCmd, "out")
	mustMarkRequired(boltPerfImportCmd, "quota")
	mustMarkRequired(boltPerfImportCmd, "config")
}

func mustMarkPersistentRequired(cmd *cobra.Command, name string) {
	if err := cmd.MarkPersistentFlagRequired(name); err != nil {
		panic(err)
	}
}

func mustMarkRequired(cmd *cobra.Command, name string) {
	if err := cmd.MarkFlagRequired(name); err != nil {
		panic(err)
	}
}
