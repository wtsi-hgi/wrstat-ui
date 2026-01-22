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
	"errors"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/watch"
)

var group string

var watchcmd = &cobra.Command{
	Use:   "watch",
	Short: "watch summarises new wrstat output",
	Long: `watch watches a wrstat output directory for new results and summarises them.

wr manager must have been started before running this. If the manager can run
commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on.

This subcommand polls the given directory for new subdirectories matching the
following name format:

^[^_]+_.

That is, starting with some version string, an underscore, and then at least one
character of key data.

Once found, a summarise task will be launched via wr and it will be given the
stats.gz file in that directory.

The --output flag determines where the summarised data will be written. A new
subdirectory, named the same as the subdirectory containing the stats.gz file,
will be created to contain the new files.

The --quota, --config, and --mount flags act the same as in the summarise
subcommand and will be passed along to it.

The --group flag can be specified to override the unix group with which the
summarise subcommands will be run.
`,
	Run: func(_ *cobra.Command, args []string) {
		if err := checkWatchArgs(args); err != nil {
			die("%s", err)
		}

		if err := watch.Watch(args, group, defaultDir, quotaPath, basedirsConfig, mounts, appLogger); err != nil {
			die("%s", err)
		}
	},
}

var (
	errWatchNeedInput        = errors.New("at least 1 input directory should be provided")
	errWatchNoOutput         = errors.New("no output files specified")
	errWatchNoQuota          = errors.New("no quota file specified")
	errWatchNoBasedirsConfig = errors.New("no basedirs config file specified")
)

func checkWatchArgs(args []string) error {
	if len(args) < 1 {
		return errWatchNeedInput
	}

	if defaultDir == "" {
		return errWatchNoOutput
	}

	if quotaPath == "" {
		return errWatchNoQuota
	}

	if basedirsConfig == "" {
		return errWatchNoBasedirsConfig
	}

	return nil
}

func init() {
	RootCmd.AddCommand(watchcmd)

	watchcmd.Flags().StringVarP(&defaultDir, "output", "o", "", "output all summariser data to here")
	watchcmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	watchcmd.Flags().StringVarP(&basedirsConfig, "config", "c", "", "path to basedirs config file")
	watchcmd.Flags().StringVarP(&mounts, "mounts", "m", "", "path to a file containing a list of quoted mountpoints")
	watchcmd.Flags().StringVarP(&group, "group", "g", "", "unix group to run the summarisers with")
}
