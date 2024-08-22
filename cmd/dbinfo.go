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
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/v4/server"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

// dbinfoCmd represents the server command.
var dbinfoCmd = &cobra.Command{
	Use:   "dbinfo",
	Short: "Get summary information on the databases",
	Long: `Get summary information on the databases.

This sub-command reports some summary information about the databases used by
the server. Provide the path to your 'wrstat multi -f' output directory.

NB: for large databases, this can take hours to run.
`,
	Run: func(_ *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your 'wrstat multi -f' output directory")
		}

		dbPaths, err := server.FindLatestDgutDirs(args[0], dgutDBsSuffix)
		if err != nil {
			die("failed to find database paths: %s", err)
		}

		basedirsDBPath, err := server.FindLatestBasedirsDB(args[0], basedirBasename)
		if err != nil {
			die("failed to find basedirs database path: %s", err)
		}

		slog.SetLogLoggerLevel(slog.LevelDebug)

		info("opening dgut databases...")
		dgutDB := dgut.NewDB(dbPaths...)
		dbInfo, err := dgutDB.Info()
		if err != nil {
			die("failed to get dgut db info: %s", err)
		}

		cliPrint("\nDirs: %d\nDGUTs: %d\nParents: %d\nChildren: %d\n\n",
			dbInfo.NumDirs, dbInfo.NumDGUTs, dbInfo.NumParents, dbInfo.NumChildren)

		info("opening basedir database...\n")

		basedirsInfo, err := basedirs.Info(basedirsDBPath)
		if err != nil {
			die("failed to get basedirs db info: %s", err)
		}

		cliPrint("Group usage group-dir combinations: %d\n", basedirsInfo.GroupDirCombos)
		cliPrint("Group history group-mount combinations: %d\n", basedirsInfo.GroupMountCombos)
		cliPrint("Group histories: %d\n", basedirsInfo.GroupHistories)
		cliPrint("Group subdir group-dir combinations: %d\n", basedirsInfo.GroupSubDirCombos)
		cliPrint("Group subdirs: %d\n", basedirsInfo.GroupSubDirs)
		cliPrint("User usage user-dir combinations: %d\n", basedirsInfo.UserDirCombos)
		cliPrint("User subdir user-dir combinations: %d\n", basedirsInfo.UserSubDirCombos)
		cliPrint("User subdirs: %d\n", basedirsInfo.UserSubDirs)
	},
}

func init() {
	RootCmd.AddCommand(dbinfoCmd)
}
