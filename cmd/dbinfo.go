/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

// dbinfoCmd represents the server command.
var dbinfoCmd = &cobra.Command{
	Use:   "dbinfo",
	Short: "Get summary information on the databases",
	Long: `Get summary information on the databases.

This sub-command reports some summary information about the databases used by
the server.

NB: for large databases, this can take hours to run.
`,
	Run: func(_ *cobra.Command, args []string) {
		loadClickhouseDotEnv()

		if len(args) > 0 {
			warn("dbinfo: ignoring legacy output directory argument")
		}

		cfg, err := clickhouseConfigFromEnvAndFlags(
			clickhouseDSN,
			clickhouseDatabase,
			ownersPath,
			nil,
			"",
			0,
			clickhouseQueryTO,
			defaultQueryTimeout,
		)
		if err != nil {
			die("failed to build ClickHouse config: %s", err)
		}

		p, err := clickhouse.OpenProvider(cfg)
		if err != nil {
			die("failed to open clickhouse provider: %s", err)
		}
		defer p.Close()

		dbInfo, err := p.Tree().Info()
		if err != nil {
			die("failed to get dguta db info: %s", err)
		}

		cliPrint("\nDirs: %d\nDGUTAs: %d\nParents: %d\nChildren: %d\n\n",
			dbInfo.NumDirs, dbInfo.NumDGUTAs, dbInfo.NumParents, dbInfo.NumChildren)

		bdInfo, err := p.BaseDirs().Info()
		if err != nil {
			die("failed to get basedirs db info: %s", err)
		}

		cliPrint("GroupDirCombos: %d\nGroupHistories: %d\nGroupMountCombos: %d\n"+
			"GroupSubDirCombos: %d\nGroupSubDirs: %d\n"+
			"UserDirCombos: %d\nUserSubDirCombos: %d\nUserSubDirs: %d\n",
			bdInfo.GroupDirCombos, bdInfo.GroupHistories, bdInfo.GroupMountCombos,
			bdInfo.GroupSubDirCombos, bdInfo.GroupSubDirs,
			bdInfo.UserDirCombos, bdInfo.UserSubDirCombos, bdInfo.UserSubDirs)
	},
}

func init() {
	RootCmd.AddCommand(dbinfoCmd)
	dbinfoCmd.Flags().StringVarP(&clickhouseDSN, "clickhouse-dsn", "C", "",
		"ClickHouse DSN (default $WRSTAT_CLICKHOUSE_DSN)")
	dbinfoCmd.Flags().StringVarP(&clickhouseDatabase, "clickhouse-database", "D", "",
		"ClickHouse database name (default $WRSTAT_CLICKHOUSE_DATABASE)")
	dbinfoCmd.Flags().StringVar(&clickhouseQueryTO, "query-timeout", "",
		"per-query timeout (default $WRSTAT_QUERY_TIMEOUT or 30s)")
	dbinfoCmd.Flags().StringVarP(&ownersPath, "owners", "o", "", "path to owners csv file (optional)")
}
