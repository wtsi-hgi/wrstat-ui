package cmd

import (
	"github.com/spf13/cobra"
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

func mustMarkRequired(cmd *cobra.Command, name string) {
	if err := cmd.MarkFlagRequired(name); err != nil {
		panic(err)
	}
}

func mustMarkPersistentRequired(cmd *cobra.Command, name string) {
	if err := cmd.MarkPersistentFlagRequired(name); err != nil {
		panic(err)
	}
}

func runBoltPerfImport(inputDir string) error {
	opts := boltperf.ImportOptions{
		Backend:  boltPerf.backend,
		Owners:   boltPerf.owners,
		Mounts:   boltPerf.mounts,
		JSONOut:  boltPerf.jsonOut,
		OutDir:   boltPerf.outDir,
		Quota:    boltPerf.quota,
		Config:   boltPerf.config,
		MaxLines: boltPerf.maxLines,
		Repeat:   boltPerf.repeat,
		Warmup:   boltPerf.warmup,
	}

	return boltperf.Import(inputDir, opts, cliPrint)
}

func runBoltPerfQuery(inputDir string) error {
	opts := boltperf.QueryOptions{
		Backend: boltPerf.backend,
		Owners:  boltPerf.owners,
		Mounts:  boltPerf.mounts,
		JSONOut: boltPerf.jsonOut,
		Dir:     boltPerf.dir,
		Repeat:  boltPerf.repeat,
		Warmup:  boltPerf.warmup,
		Splits:  boltPerf.splits,
	}

	return boltperf.Query(inputDir, opts, cliPrint)
}
