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
package watch

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/server"
)

const (
	inputStatsFile  = "stats.gz"
	dirPerms        = 0750
	basedirBasename = "basedirs.db"
)

var (
	testOutputFD = 3                                  //nolint:gochecknoglobals
	exit         = func() { os.Exit(0) }              //nolint:gochecknoglobals
	runJobs      string                               //nolint:gochecknoglobals
	delay        = func() { time.Sleep(time.Minute) } //nolint:gochecknoglobals
)

// Watch watches an input directory (which should be the output directory of a
// wrstat multi run) for new stats.gz files, upon which it will run the
// summarise subcommand on that data, if it has not already been run.
//
// The scheduled summarise subcommands will be given the output directory, quota
// path and basedirs config path.
func Watch(inputDir, outputDir, quotaPath, basedirsConfig string) error {
	for {
		inputPaths, err := server.FindDBDirs(inputDir, "stats.gz")
		if err != nil {
			return fmt.Errorf("error getting input DB paths: %w", err)
		}

		inputPaths = slices.DeleteFunc(inputPaths, func(p string) bool {
			base := filepath.Base(p)

			return entryExists(filepath.Join(outputDir, base)) || entryExists(filepath.Join(outputDir, "."+base))
		})

		if err := scheduleSummarisers(inputDir, outputDir, quotaPath, basedirsConfig, inputPaths); err != nil {
			return err
		}

		delay()
	}
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func scheduleSummarisers(inputDir, outputDir, quotaPath, basedirsConfig string, inputPaths []string) error {
	for _, p := range inputPaths {
		base := filepath.Base(p)

		if err := scheduleSummarise(inputDir, outputDir, base, quotaPath, basedirsConfig); err != nil {
			return fmt.Errorf("error scheduling summarise (%s): %w", base, err)
		}
	}

	return nil
}

func scheduleSummarise(inputDir, outputDir, base, quotaPath, basedirsConfig string) error {
	dotOutputBase := filepath.Join(outputDir, "."+base)

	if err := os.MkdirAll(dotOutputBase, dirPerms); err != nil {
		return err
	}

	previousBasedirsDB, err := getPreviousBasedirsDB(outputDir, base)
	if err != nil {
		return err
	}

	return addJob(getWRJSON(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig, inputDir, base, outputDir))
}

func getWRJSON(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig, inputDir, base, outputDir string) string {
	cmdFormat := "%[1]q summarise -d %[2]q"

	if previousBasedirsDB != "" {
		cmdFormat += " -s %[3]q"
	}

	cmdFormat += " -q %[4]q -c %[5]q %[6]q && touch -r %[7]q %[2]q && mv %[2]q %[8]q"

	return `{"cmd":` + strconv.Quote(fmt.Sprintf(cmdFormat,
		os.Args[0], dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig,
		filepath.Join(inputDir, base, inputStatsFile),
		filepath.Join(inputDir, base),
		filepath.Join(outputDir, base),
	)) + `,"req_grp":"wrstat-ui-summarise"}`
}

func addJob(wrJSON string) error {
	if runJobs != "" {
		fakeRunJobs(wrJSON)

		return nil
	}

	cmd := exec.Command("wr", "add")
	cmd.Stdin = strings.NewReader(wrJSON)

	return cmd.Run()
}

func fakeRunJobs(wrJSON string) {
	os.NewFile(uintptr(testOutputFD), "").WriteString(wrJSON) //nolint:errcheck

	exit()
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
