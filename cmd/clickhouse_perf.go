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
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	chPerfDefaultRepeat    = 20
	chPerfDefaultWarmup    = 1
	chPerfDefaultSplits    = 4
	chPerfDefaultWalkDepth = 2
	chPerfDefaultWalkLimit = 20
	chPerfDefaultAncLimit  = 16
	chPerfDefaultBatchSize = 100_000
	chPerfDefaultParallel  = 1
)

var chPerfCmd = &cobra.Command{
	Use:   "clickhouse-perf",
	Short: "Run performance harness against ClickHouse-backed storage",
	Long: `clickhouse-perf runs an in-process timing harness against ClickHouse.

Use the import subcommand to ingest stats.gz datasets into ClickHouse.
Use the query subcommand to run a repeatable timing suite and report latency.
`,
}

var chPerfImportCmd = &cobra.Command{
	Use:   "import <inputDir>",
	Short: "Import stats.gz datasets into ClickHouse",
	Long: `Import reads one or more datasets under <inputDir> and ingests them
into ClickHouse. Each dataset subdirectory must contain a 'stats.gz' file and
follow the <version>_<mountKey> naming convention.
`,
	Args: cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		return runCHPerfImport(args[0])
	},
}

var chPerfQueryCmd = &cobra.Command{
	Use:   "query",
	Short: "Run query timing suite against ClickHouse",
	Long: `Query runs a repeatable timing suite against ClickHouse and reports
per-query latency with p50/p95/p99 percentiles.
`,
	RunE: func(_ *cobra.Command, _ []string) error {
		return runCHPerfQuery()
	},
}

type chPerfFlags struct {
	dsn         string
	database    string
	queryTO     string
	owners      string
	mountpoints string
	jsonOut     string

	maxLines    int
	batchSize   int
	parallelism int
	quota       string
	config      string

	dir       string
	ancDir    string
	ops       []string
	uid       uint32
	gids      string
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

var chPerf chPerfFlags

func init() {
	RootCmd.AddCommand(chPerfCmd)
	chPerfCmd.AddCommand(chPerfImportCmd)
	chPerfCmd.AddCommand(chPerfQueryCmd)

	addCHPerfFlags()
}

func addCHPerfFlags() {
	addCHPerfPersistentFlags()
	addCHPerfImportFlags()
	addCHPerfQueryFlags()
}

func addCHPerfPersistentFlags() {
	pf := chPerfCmd.PersistentFlags()

	addClickhouseConnectionFlags(pf, &chPerf.dsn, &chPerf.database)
	addClickhouseQueryTimeoutFlag(pf, &chPerf.queryTO)
	pf.StringVar(&chPerf.owners, "owners", "", "gid,owner csv file")
	addMountpointsFlag(pf, &chPerf.mountpoints)
	pf.StringVar(&chPerf.jsonOut, "json", "", "write JSON report to this file")
}

func addCHPerfImportFlags() {
	f := chPerfImportCmd.Flags()

	f.IntVar(&chPerf.maxLines, "maxLines", 0,
		"max lines per stats.gz to import (0 for all)")
	f.IntVar(&chPerf.batchSize, "batchSize", chPerfDefaultBatchSize,
		"ClickHouse insert batch size")
	f.IntVar(&chPerf.parallelism, "parallelism", chPerfDefaultParallel,
		"number of concurrent dataset ingests (capped at 4)")
	f.StringVar(&chPerf.quota, "quota", "", "quota csv for basedirs")
	f.StringVar(&chPerf.config, "config", "", "basedirs config file")
}

func addCHPerfQueryFlags() {
	f := chPerfQueryCmd.Flags()

	f.StringVar(&chPerf.dir, "dir", "",
		"directory to query (default: auto-select)")
	f.StringVar(&chPerf.ancDir, "ancestor-dir", "/",
		"ancestor directory for root/click-through Disktree timings")
	f.StringSliceVar(&chPerf.ops, "ops", nil,
		"comma-separated query operation names to run (default: all)")
	f.Uint32Var(&chPerf.uid, "uid", 0, "UID for permission query")
	f.StringVar(&chPerf.gids, "gids", "", "comma-separated GIDs for permission query")
	f.StringVar(&chPerf.treeGIDs, "tree-gids", "", "comma-separated GIDs for tree query filter")
	f.StringVar(&chPerf.treeUIDs, "tree-uids", "", "comma-separated UIDs for tree query filter")
	f.StringVar(&chPerf.treeTypes, "tree-types", "",
		"comma-separated file type names for tree query filter (for example bam,cram,temp)")
	f.StringVar(&chPerf.treeFT, "tree-ft", "",
		"file type bitmask for tree query filter (decimal or 0x; ORed with --tree-types)")
	f.IntVar(&chPerf.repeat, "repeat", chPerfDefaultRepeat, "number of timed repeats")
	f.IntVar(&chPerf.warmup, "warmup", chPerfDefaultWarmup, "warmup iterations")
	f.IntVar(&chPerf.splits, "splits", chPerfDefaultSplits, "where() splits")
	f.IntVar(&chPerf.walkDepth, "walk-depth", chPerfDefaultWalkDepth,
		"max depth for unique directory tree walk timings")
	f.IntVar(&chPerf.walkLimit, "walk-limit", chPerfDefaultWalkLimit,
		"max unique directories to time in tree walk operations")
	f.IntVar(&chPerf.ancLimit, "ancestor-limit", chPerfDefaultAncLimit,
		"max root/ancestor directories to time in Disktree click-through operations")
}

func runCHPerfImport(inputDir string) error {
	cfg, err := chPerfConfig()
	if err != nil {
		return err
	}

	api := chperf.NewClickHouseAPI(cfg)

	report, err := chperf.Import(api, inputDir, chperf.ImportOptions{
		MaxLines:    chPerf.maxLines,
		BatchSize:   chPerf.batchSize,
		Parallelism: chPerf.parallelism,
		QuotaPath:   chPerf.quota,
		ConfigPath:  chPerf.config,
		MountPoints: cfg.MountPoints,
	}, cliPrint)
	if err != nil {
		return err
	}

	return chPerfWriteReport(report)
}

func runCHPerfQuery() error {
	cfg, err := chPerfConfig()
	if err != nil {
		return err
	}

	opts, err := chPerfQueryOptions()
	if err != nil {
		return err
	}

	api := chperf.NewClickHouseAPI(cfg)

	report, err := chperf.Query(api, opts, cliPrint)
	if err != nil {
		return err
	}

	return chPerfWriteReport(report)
}

func chPerfConfig() (clickhouse.Config, error) {
	return loadClickhouseConfigWithMountpoints(clickhouseConfigInput{
		dsnFlag:          chPerf.dsn,
		databaseFlag:     chPerf.database,
		ownersPath:       chPerf.owners,
		queryTimeoutFlag: chPerf.queryTO,
	}, chPerf.mountpoints)
}

func chPerfQueryOptions() (chperf.QueryOptions, error) {
	treeFilter, err := parsePerfTreeFilter(chPerf.treeGIDs, chPerf.treeUIDs, chPerf.treeFT, chPerf.treeTypes)
	if err != nil {
		return chperf.QueryOptions{}, err
	}

	return chperf.QueryOptions{
		Dir:           chPerf.dir,
		AncestorDir:   chPerf.ancDir,
		Ops:           chPerf.ops,
		UID:           chPerf.uid,
		GIDs:          parseGIDs(chPerf.gids),
		TreeFilter:    treeFilter,
		Repeat:        chPerf.repeat,
		Warmup:        chPerf.warmup,
		Splits:        chPerf.splits,
		WalkDepth:     chPerf.walkDepth,
		WalkLimit:     chPerf.walkLimit,
		AncestorLimit: chPerf.ancLimit,
	}, nil
}

func parsePerfTreeFilter(gidsRaw, uidsRaw, ftRaw, typesRaw string) (*db.Filter, error) {
	gids, err := parsePerfTreeFilterIDs(gidsRaw, "tree-gids")
	if err != nil {
		return nil, err
	}

	uids, err := parsePerfTreeFilterIDs(uidsRaw, "tree-uids")
	if err != nil {
		return nil, err
	}

	ft, err := parsePerfTreeFilterFileTypes(ftRaw, typesRaw)
	if err != nil {
		return nil, err
	}

	return &db.Filter{
		GIDs: gids,
		UIDs: uids,
		FT:   ft,
		Age:  db.DGUTAgeAll,
	}, nil
}

func parsePerfTreeFilterIDs(raw, flagName string) ([]uint32, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	ids := make([]uint32, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}

		id, err := strconv.ParseUint(trimmed, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid --%s value %q: %w", flagName, trimmed, err)
		}

		ids = append(ids, uint32(id))
	}

	return ids, nil
}

func parsePerfTreeFilterFileTypes(ftRaw, typesRaw string) (db.DirGUTAFileType, error) {
	var ft db.DirGUTAFileType

	if trimmed := strings.TrimSpace(ftRaw); trimmed != "" {
		mask, err := strconv.ParseUint(trimmed, 0, 16)
		if err != nil {
			return 0, fmt.Errorf("invalid --tree-ft value %q: %w", trimmed, err)
		}

		ft |= db.DirGUTAFileType(mask)
	}

	if strings.TrimSpace(typesRaw) == "" {
		return ft, nil
	}

	for _, part := range strings.Split(typesRaw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}

		fileType, err := db.FileTypeStringToDirGUTAFileType(name)
		if err != nil {
			return 0, fmt.Errorf("invalid --tree-types value %q: %w", name, err)
		}

		ft |= fileType
	}

	return ft, nil
}

func parseGIDs(raw string) []uint32 {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	gids := make([]uint32, 0, len(parts))

	for _, p := range parts {
		if gid, ok := parseGID(p); ok {
			gids = append(gids, gid)
		}
	}

	return gids
}

func parseGID(raw string) (uint32, bool) {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return 0, false
	}

	return uint32(v), true
}

func chPerfWriteReport(report perfreport.Report) error {
	if chPerf.jsonOut == "" {
		return nil
	}

	return perfreport.WriteReport(chPerf.jsonOut, report)
}
