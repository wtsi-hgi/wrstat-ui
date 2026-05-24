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

package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
)

const (
	boltPerfDefaultRepeat    = 20
	boltPerfDefaultWarmup    = 1
	boltPerfDefaultSplits    = 4
	boltPerfDefaultWalkDepth = 2
	boltPerfDefaultWalkLimit = 20
	boltPerfDefaultAncLimit  = 16

	boltPerfBackendInterfaces = "bolt_interfaces"
)

var errBoltPerfInterfacesTreeFilters = errors.New(
	"bolt_interfaces backend does not support tree filter flags",
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

	dir       string
	ancDir    string
	ops       []string
	treeGIDs  string
	treeUIDs  string
	treeFT    string
	treeTypes string
	repeat    int
	warmup    int
	splits    int
	walkDepth int
	walkLimit int
	ancLimit  int
}

var boltPerf boltPerfFlags

func runBoltPerfImport(inputDir string) error {
	if boltPerf.backend == boltPerfBackendInterfaces {
		return runBoltPerfImportInterfaces(inputDir, cliPrint)
	}

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
	if boltPerf.backend == boltPerfBackendInterfaces {
		if err := rejectBoltPerfInterfacesTreeFilters(); err != nil {
			return err
		}

		return runBoltPerfQueryInterfaces(inputDir, cliPrint)
	}

	treeFilter, err := parsePerfTreeFilter(boltPerf.treeGIDs, boltPerf.treeUIDs, boltPerf.treeFT, boltPerf.treeTypes)
	if err != nil {
		return err
	}

	opts := boltperf.QueryOptions{
		Backend:                 boltPerf.backend,
		Owners:                  boltPerf.owners,
		Mounts:                  boltPerf.mounts,
		JSONOut:                 boltPerf.jsonOut,
		Dir:                     boltPerf.dir,
		AncestorDir:             boltPerf.ancDir,
		Ops:                     boltPerf.ops,
		TreeFilter:              treeFilter,
		Repeat:                  boltPerf.repeat,
		Warmup:                  boltPerf.warmup,
		Splits:                  boltPerf.splits,
		WalkDepth:               boltPerf.walkDepth,
		WalkLimit:               boltPerf.walkLimit,
		AncestorLimit:           boltPerf.ancLimit,
		OpenDatabase:            bolt.OpenDatabase,
		OpenMultiBaseDirsReader: bolt.OpenMultiBaseDirsReader,
	}

	return boltperf.Query(inputDir, opts, cliPrint)
}

func rejectBoltPerfInterfacesTreeFilters() error {
	flags := boltPerfInterfacesTreeFilterFlags()
	if len(flags) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: %s",
		errBoltPerfInterfacesTreeFilters,
		strings.Join(flags, ", "),
	)
}

func boltPerfInterfacesTreeFilterFlags() []string {
	var flags []string

	if strings.TrimSpace(boltPerf.treeGIDs) != "" {
		flags = append(flags, "--tree-gids")
	}

	if strings.TrimSpace(boltPerf.treeUIDs) != "" {
		flags = append(flags, "--tree-uids")
	}

	if strings.TrimSpace(boltPerf.treeTypes) != "" {
		flags = append(flags, "--tree-types")
	}

	if strings.TrimSpace(boltPerf.treeFT) != "" {
		flags = append(flags, "--tree-ft")
	}

	return flags
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
	boltPerfQueryCmd.Flags().StringVar(&boltPerf.ancDir, "ancestor-dir", "/",
		"ancestor directory for root/click-through Disktree timings")
	boltPerfQueryCmd.Flags().StringSliceVar(&boltPerf.ops, "ops", nil,
		"comma-separated query operation names to run (default: all)")
	boltPerfQueryCmd.Flags().StringVar(&boltPerf.treeGIDs, "tree-gids", "",
		"comma-separated GIDs for tree query filter")
	boltPerfQueryCmd.Flags().StringVar(&boltPerf.treeUIDs, "tree-uids", "",
		"comma-separated UIDs for tree query filter")
	boltPerfQueryCmd.Flags().StringVar(&boltPerf.treeTypes, "tree-types", "",
		"comma-separated file type names for tree query filter (for example bam,cram,temp)")
	boltPerfQueryCmd.Flags().StringVar(&boltPerf.treeFT, "tree-ft", "",
		"file type bitmask for tree query filter (decimal or 0x; ORed with --tree-types)")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.repeat, "repeat", boltPerfDefaultRepeat, "repeat count")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.warmup, "warmup", boltPerfDefaultWarmup, "warmup iterations")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.splits, "splits", boltPerfDefaultSplits, "where() splits")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.walkDepth, "walk-depth", boltPerfDefaultWalkDepth,
		"max depth for unique directory tree walk timings")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.walkLimit, "walk-limit", boltPerfDefaultWalkLimit,
		"max unique directories to time in tree walk operations")
	boltPerfQueryCmd.Flags().IntVar(&boltPerf.ancLimit, "ancestor-limit", boltPerfDefaultAncLimit,
		"max root/ancestor directories to time in Disktree click-through operations")
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
