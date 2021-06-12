/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

// these variables are accessible by all subcommands.
var deployment string

const connectTimeout = 10 * time.Second

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "wrstat",
	Short: "wrstat gets stats on all files in a filesystem directory tree.",
	Long: `wrstat gets stats on all files in a filesystem directory tree.

It uses wr to queue getting the stats for subsets of the tree, so enabling the
work to be done in parallel and potentially distributed over many nodes.

Before doing anything else, the wr manager must be running. If the manager can
run commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on. Or run commands from the same node that you
started the manager on.

For raw stats on a directory and all its sub contents:
$ wrstat walk -o [/output/location] -d [dependency_group] [/location/of/interest]

Combine all the above output files:
$ wrstat combine [/output/location]

Or more easily work on multiple locations of interest at once by doing the
above 2 steps on each location and moving the final results to a final location:
$ wrstat multi -w [/working/directory] -f [/final/output/dir] [/a /b /c]`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die(err.Error())
	}
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().StringVar(&deployment,
		"deployment",
		"production",
		"the deployment your wr manager was started with")
}

// warn is a convenience to log a message at the Warn level.
func warn(msg string, a ...interface{}) {
	appLogger.Warn(fmt.Sprintf(msg, a...))
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(msg string, a ...interface{}) {
	appLogger.Error(fmt.Sprintf(msg, a...))
	os.Exit(1)
}
