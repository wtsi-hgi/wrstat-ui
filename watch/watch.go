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
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/inconshreveable/log15"
	"github.com/wtsi-hgi/wrstat-ui/server"
)

const (
	inputStatsFile  = "stats.gz"
	dirPerms        = 0750
	basedirBasename = "basedirs.db"
	summariseCPU    = 2
	summariseMem    = 8192
)

// Watch watches an input directory (which should be the output directory of a
// wrstat multi run) for new stats.gz files, upon which it will run the
// summarise subcommand on that data, if it has not already been run.
//
// The scheduled summarise subcommands will be given the output directory, quota
// path and basedirs config path.
func Watch(inputDirs []string, outputDir, quotaPath, basedirsConfig string, logger log15.Logger) error {
	for {
		if err := watch(inputDirs, outputDir, quotaPath, basedirsConfig, logger); err != nil {
			return err
		}

		if client.PretendSubmissions != "" {
			return nil
		}

		time.Sleep(time.Minute)
	}
}

func watch(inputDirs []string, outputDir, quotaPath, basedirsConfig string, logger log15.Logger) error { //nolint:gocognit,gocyclo,lll
	var err error

	for n := range inputDirs {
		if inputDirs[n], err = filepath.Abs(inputDirs[n]); err != nil {
			return err
		}
	}

	if outputDir, err = filepath.Abs(outputDir); err != nil {
		return err
	}

	for _, inputDir := range inputDirs {
		inputPaths, err := server.FindDBDirs(inputDir, "stats.gz")
		if err != nil {
			return fmt.Errorf("error getting input DB paths: %w", err)
		}

		inputPaths = slices.DeleteFunc(inputPaths, func(p string) bool {
			base := filepath.Base(p)

			return entryExists(filepath.Join(outputDir, base)) || entryExists(filepath.Join(outputDir, "."+base))
		})

		if err := scheduleSummarisers(inputDir, outputDir, quotaPath, basedirsConfig, inputPaths, logger); err != nil {
			return err
		}
	}

	return nil
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func scheduleSummarisers(inputDir, outputDir, quotaPath, basedirsConfig string,
	inputPaths []string, logger log15.Logger) error {
	s, err := client.New(client.SchedulerSettings{
		Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create wr client: %w", err)
	}

	jobs := make([]*jobqueue.Job, 0, len(inputPaths))

	for _, p := range inputPaths {
		base := filepath.Base(p)

		job, errr := createSummariseJob(inputDir, outputDir, base, quotaPath, basedirsConfig, s)
		if errr != nil {
			return fmt.Errorf("error scheduling summarise (%s): %w", base, errr)
		}

		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return nil
	}

	if err = s.SubmitJobs(jobs); err != nil {
		return fmt.Errorf("error submitting jobs to wr: %w", err)
	}

	return nil
}

func createSummariseJob(inputDir, outputDir, base, quotaPath, basedirsConfig string,
	s *client.Scheduler) (*jobqueue.Job, error) {
	dotOutputBase := filepath.Join(outputDir, "."+base)

	if err := os.MkdirAll(dotOutputBase, dirPerms); err != nil {
		return nil, err
	}

	previousBasedirsDB, err := getPreviousBasedirsDB(outputDir, base)
	if err != nil {
		return nil, err
	}

	reqs := client.DefaultRequirements()
	reqs.Cores = summariseCPU
	reqs.RAM = summariseMem

	return s.NewJob(
		getJobCommand(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig,
			inputDir, base, outputDir),
		"wrstat-ui-summarise-"+time.Now().Format("20060102150405"),
		"wrstat-ui-summarise",
		"",
		"",
		reqs,
	), nil
}

func getJobCommand(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig,
	inputDir, base, outputDir string) string {
	cmdFormat := "%[1]q summarise -d %[2]q"

	if previousBasedirsDB != "" {
		cmdFormat += " -s %[3]q"
	}

	cmdFormat += " -q %[4]q -c %[5]q %[6]q && touch -r %[7]q %[2]q && mv %[2]q %[8]q"

	return fmt.Sprintf(cmdFormat,
		os.Args[0], dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig,
		filepath.Join(inputDir, base, inputStatsFile),
		filepath.Join(inputDir, base),
		filepath.Join(outputDir, base),
	)
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
