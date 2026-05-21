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
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/chperf"
)

const (
	chPerfDefaultRepeat    = 20
	chPerfDefaultWarmup    = 1
	chPerfDefaultSplits    = 4
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

	dir    string
	uid    uint32
	gids   string
	repeat int
	warmup int
	splits int
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
	f.Uint32Var(&chPerf.uid, "uid", 0, "UID for permission query")
	f.StringVar(&chPerf.gids, "gids", "", "comma-separated GIDs for permission query")
	f.IntVar(&chPerf.repeat, "repeat", chPerfDefaultRepeat, "number of timed repeats")
	f.IntVar(&chPerf.warmup, "warmup", chPerfDefaultWarmup, "warmup iterations")
	f.IntVar(&chPerf.splits, "splits", chPerfDefaultSplits, "where() splits")
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

	api := chperf.NewClickHouseAPI(cfg)

	report, err := chperf.Query(api, chperf.QueryOptions{
		Dir:    chPerf.dir,
		UID:    chPerf.uid,
		GIDs:   parseGIDs(chPerf.gids),
		Repeat: chPerf.repeat,
		Warmup: chPerf.warmup,
		Splits: chPerf.splits,
	}, cliPrint)
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

func chPerfWriteReport(report boltperf.Report) error {
	if chPerf.jsonOut == "" {
		return nil
	}

	return boltperf.WriteReport(chPerf.jsonOut, report)
}
