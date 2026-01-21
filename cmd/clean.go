/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

var (
	prefix   string
	viewOnly bool
)

var cleancmd = &cobra.Command{
	Use:   "clean",
	Short: "clean basedir history of invalid history",
	Long: `clean filters basedir history to only have paths with a certain prefix

Legacy basedirectory databases may have history relating to other mountpoints
which is no longer valid with the new database structures.

This command will remove all history except for those whose paths match the
prefix supplied in the --prefix/-p flag.

The --view/-v flag can be used to view the keys that would have be removed if
the flag were not supplied.
`,
	Run: func(_ *cobra.Command, args []string) {
		loadClickhouseDotEnv()

		if len(args) > 0 {
			warn("clean: ignoring legacy basedirs DB path argument")
		}

		if prefix == "" {
			die("need to specify a path prefix to keep")
		}

		cfg, err := clickhouseConfigFromEnvAndFlags(
			clickhouseDSN,
			clickhouseDatabase,
			"",
			nil,
			"",
			0,
			clickhouseQueryTO,
			defaultQueryTimeout,
		)
		if err != nil {
			die("failed to build ClickHouse config: %s", err)
		}

		m, err := clickhouse.NewHistoryMaintainer(cfg)
		if err != nil {
			die("failed to open clickhouse history maintainer: %s", err)
		}

		if viewOnly {
			toRemove, err := m.FindInvalidHistory(prefix)
			if err != nil {
				die("failed to read clickhouse history: %s", err)
			}

			for _, k := range toRemove {
				fmt.Printf("%d:%s\n", k.GID, k.MountPath)
			}
		} else {
			if err := m.CleanHistoryForMount(prefix); err != nil {
				die("error cleaning clickhouse history: %s", err)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(cleancmd)

	cleancmd.Flags().StringVarP(&prefix, "prefix", "p", "", "path prefix to keep in history")
	cleancmd.Flags().BoolVarP(&viewOnly, "view", "v", false, "show the keys that will be removed without deleting them")
	cleancmd.Flags().StringVarP(&clickhouseDSN, "clickhouse-dsn", "C", "",
		"ClickHouse DSN (default $WRSTAT_CLICKHOUSE_DSN)")
	cleancmd.Flags().StringVarP(&clickhouseDatabase, "clickhouse-database", "D", "",
		"ClickHouse database name (default $WRSTAT_CLICKHOUSE_DATABASE)")
	cleancmd.Flags().StringVar(&clickhouseQueryTO, "query-timeout", "",
		"per-query timeout (default $WRSTAT_QUERY_TIMEOUT or 30s)")
}
