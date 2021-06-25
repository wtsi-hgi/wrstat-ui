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
	"path/filepath"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/scheduler"
)

const userOnlyPerm = 0700

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

// newScheduler returns a new Scheduler, exiting on error. It also returns a
// function you should defer.
func newScheduler() (*scheduler.Scheduler, func()) {
	s, err := scheduler.New(deployment, connectTimeout, appLogger)
	if err != nil {
		die("%s", err)
	}

	return s, func() {
		err = s.Disconnect()
		if err != nil {
			warn("failed to disconnect from wr manager: %s", err)
		}
	}
}

// repGrp returns a rep_grp that can be used for a wrstat job we will create.
func repGrp(cmd, dir, unique string) string {
	return fmt.Sprintf("wrstat-%s-%s-%s-%s", cmd, filepath.Base(dir), dateStamp(), unique)
}

// dateStamp returns today's date in the form YYYYMMDD.
func dateStamp() string {
	t := time.Now()

	return t.Format("20060102")
}

// addJobsToQueue adds the jobs to wr's queue.
func addJobsToQueue(s *scheduler.Scheduler, jobs []*jobqueue.Job) {
	if err := s.SubmitJobs(jobs); err != nil {
		die("failed to add jobs to wr's queue: %s", err)
	}
}
