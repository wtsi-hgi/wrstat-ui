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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/server"
)

const inputStatsFile = "stats.gz"
const testOutputFD = 3
const dirPerms = 0750

var runJobs string

var watch = &cobra.Command{
	Use:   "watch",
	Short: "watch summarises new wrstat output",
	Long: `watch watches a wrstat output directory for new results and summarises them.

wr manager must have been started before running this. If the manager can run
commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on.

This subcommand polls the given directory for new subdirectories matching the
following name format:

^\d+_.

That is, starting with some string of digits, an underscore, and then at least one
character of key data.

Once found, a summarise task will be launched via wr and it will be given the
stats.gz file in that directory.

The --output flag determines where the summarised data will be written. A new
subdirectory, named the same as the subdirectory containing the stats.gz file,
will be created to contain the new files.

The --quota and --config flags act the same as in the summarise subcommand and
will be passed along to it.
`,
	Run: func(_ *cobra.Command, args []string) {
		if err := checkWatchArgs(args); err != nil {
			die("%s", err)
		}

		for {
			inputPaths, err := server.FindDBDirs(args[0], "stats.gz")
			if err != nil {
				die("error getting input DB paths: %s", err)
			}

			for _, p := range inputPaths {
				base := filepath.Base(p)

				if entryExists(filepath.Join(defaultDir, base)) || entryExists(filepath.Join(defaultDir, "."+base)) {
					continue
				}

				if err := scheduleSummarise(args[0], defaultDir, base); err != nil {
					warn("error scheduling summarise (%s): %s", base, err)
				}
			}

			time.Sleep(time.Minute)
		}
	},
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func checkWatchArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("exactly 1 input directory should be provided") //nolint:err113
	}

	if defaultDir == "" {
		return errors.New("no output files specified") //nolint:err113
	}

	if quotaPath == "" {
		return errors.New("no quota file specified") //nolint:err113
	}

	if basedirsConfig == "" {
		return errors.New("no basedirs config file specified") //nolint:err113
	}

	return nil
}

func scheduleSummarise(inputDir, outputDir, base string) error { //nolint:funlen
	dotOutputBase := filepath.Join(outputDir, "."+base)

	if err := os.MkdirAll(dotOutputBase, dirPerms); err != nil {
		return err
	}

	previousBasedirsDB, err := getPreviousBasedirsDB(outputDir, base)
	if err != nil {
		return err
	}

	cmdFormat := "%[1]q summarise -d %[2]q"

	if previousBasedirsDB != "" {
		cmdFormat += " -s %[3]q"
	}

	cmdFormat += " -q %[4]q -c %[5]q %[6]q && touch -r %[7]q %[2]q && mv %[2]q %[8]q"

	input := strings.NewReader(`{"cmd":` + strconv.Quote(fmt.Sprintf(cmdFormat,
		os.Args[0], dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig,
		filepath.Join(inputDir, base, inputStatsFile),
		filepath.Join(inputDir, base),
		filepath.Join(outputDir, base),
	)) + `,"req_grp":"wrstat-ui-summarise"}`)

	if runJobs != "" {
		io.Copy(os.NewFile(uintptr(testOutputFD), "/dev/stdout"), input) //nolint:errcheck

		os.Exit(0)
	}

	cmd := exec.Command("wr", "add")
	cmd.Stdin = input

	return cmd.Run()
}

func getPreviousBasedirsDB(outputDir, base string) (string, error) {
	possibleBasedirs, err := server.FindDBDirs(outputDir, basedirBasename)
	if err != nil {
		return "", err
	}

	splitBase := strings.Split(base, "_")

	for _, possibleBasedirDB := range possibleBasedirs {
		key := strings.SplitN(filepath.Base(possibleBasedirDB), "_", 2) //nolint:mnd

		if key[1] == splitBase[1] {
			return filepath.Join(possibleBasedirDB, basedirBasename), nil
		}
	}

	return "", nil
}

func init() {
	RootCmd.AddCommand(watch)

	watch.Flags().StringVarP(&defaultDir, "output", "o", "", "output all summariser data to here")
	watch.Flags().StringVarP(&quotaPath, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	watch.Flags().StringVarP(&basedirsConfig, "config", "c", "", "path to basedirs config file")
}
