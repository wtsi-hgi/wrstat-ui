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
	"github.com/wtsi-hgi/wrstat-ui/datasets"
)

const (
	inputStatsFile        = "stats.gz"
	dirPerms              = 0750
	basedirBasename       = "basedirs.db"
	hiddenOutputPrefix    = "."
	summariseCores        = 2
	summariseMinRuntime   = 30 * time.Minute
	defaultSummariseRAMMB = 8192
	mbPerGB               = 1024
	jobTimestampLayout    = "20060102150405"
	clickhouseRecoverFlag = "--clickhouse-recover"
	summariseReqGroupBase = "wrstat-ui-summarise"
)

var connectTimeout = 10 * time.Second //nolint:gochecknoglobals

// Watch watches input directories (which should be output directories of wrstat
// multi runs) for new stats.gz files, upon which it will run the summarise
// subcommand on that data, if it has not already been run.
//
// The scheduled summarise subcommands will be given the output directory, quota
// path and basedirs config path. minMemGB is in GB and acts as the minimum
// requested memory for summarise jobs, with values below the default 8GB floor
// clamped upward. Higher learned or historical requirements remain unchanged.
// The queue and queuesAvoid values are passed to wr so scheduler submission can
// target or avoid specific queues.
func Watch(inputDirs []string, group, outputDir, quotaPath, basedirsConfig,
	mounts string, minMemGB int, queue, queuesAvoid string, logger log15.Logger) error {
	for {
		if err := watch(inputDirs, group, outputDir, quotaPath, basedirsConfig,
			mounts, minMemGB, queue, queuesAvoid, logger); err != nil {
			return err
		}

		if client.PretendSubmissions != "" {
			return nil
		}

		time.Sleep(time.Minute)
	}
}

func watch(inputDirs []string, group, outputDir, quotaPath, basedirsConfig, mounts string,
	minMemGB int, queue, queuesAvoid string, logger log15.Logger) error {
	s, err := client.New(client.SchedulerSettings{
		Logger:      logger,
		Timeout:     connectTimeout,
		Queue:       queue,
		QueuesAvoid: queuesAvoid,
	})
	if err != nil {
		return fmt.Errorf("failed to create wr client: %w", err)
	}

	defer s.Disconnect() //nolint:errcheck

	inputDirs, outputDir, err = absoluteWatchPaths(inputDirs, outputDir)
	if err != nil {
		return err
	}

	for _, inputDir := range inputDirs {
		inputPaths, err := pendingInputPaths(inputDir, outputDir)
		if err != nil {
			return err
		}

		if err := scheduleSummarisers(s, group, inputDir, outputDir, quotaPath,
			basedirsConfig, mounts, minMemGB, inputPaths); err != nil {
			return err
		}
	}

	return nil
}

func absoluteWatchPaths(inputDirs []string, outputDir string) ([]string, string, error) {
	absInputDirs := make([]string, len(inputDirs))

	for i, inputDir := range inputDirs {
		absInputDir, err := filepath.Abs(inputDir)
		if err != nil {
			return nil, "", err
		}

		absInputDirs[i] = absInputDir
	}

	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return nil, "", err
	}

	return absInputDirs, absOutputDir, nil
}

func pendingInputPaths(inputDir, outputDir string) ([]string, error) {
	inputPaths, err := datasets.FindLatestDatasetDirs(inputDir, inputStatsFile)
	if err != nil {
		return nil, fmt.Errorf("error getting input DB paths: %w", err)
	}

	return slices.DeleteFunc(inputPaths, func(p string) bool {
		base := filepath.Base(p)

		return entryExists(filepath.Join(outputDir, base)) || entryExists(hiddenOutputDir(outputDir, base))
	}), nil
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func scheduleSummarisers(s *client.Scheduler, group, inputDir, outputDir, quotaPath, basedirsConfig, mounts string,
	minMemGB int, inputPaths []string) error {
	jobs := make([]*jobqueue.Job, 0, len(inputPaths))

	for _, p := range inputPaths {
		base := filepath.Base(p)

		job, err := createSummariseJob(group, inputDir, outputDir, base,
			quotaPath, basedirsConfig, mounts, minMemGB, s)
		if err != nil {
			return fmt.Errorf("error scheduling summarise (%s): %w", base, err)
		}

		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return nil
	}

	if err := s.SubmitJobs(jobs); err != nil {
		return fmt.Errorf("error submitting jobs to wr: %w", err)
	}

	return nil
}

func createSummariseJob(group, inputDir, outputDir, base, quotaPath, basedirsConfig, mounts string,
	minMemGB int, s *client.Scheduler) (*jobqueue.Job, error) {
	dotOutputBase := hiddenOutputDir(outputDir, base)

	if err := os.MkdirAll(dotOutputBase, dirPerms); err != nil {
		return nil, err
	}

	previousBasedirsDB, err := getPreviousBasedirsDB(outputDir, base)
	if err != nil {
		return nil, err
	}

	req := client.DefaultRequirements()
	req.Cores = summariseCores
	req.RAM = summariseMinRAMMB(minMemGB)
	req.Time = summariseMinRuntime

	job := s.NewJob(
		getJobCommand(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig, mounts,
			inputDir, base, outputDir),
		summariseJobName(),
		summariseReqGroup(base),
		"",
		"",
		req,
	)
	job.Group = group

	return job, nil
}

func summariseMinRAMMB(minMemGB int) int {
	if minMemGB <= 0 {
		return defaultSummariseRAMMB
	}

	requestedRAM := minMemGB * mbPerGB
	if requestedRAM < defaultSummariseRAMMB {
		return defaultSummariseRAMMB
	}

	return requestedRAM
}

func summariseJobName() string {
	return "wrstat-ui-summarise-" + time.Now().Format(jobTimestampLayout)
}

func getPreviousBasedirsDB(outputDir, base string) (string, error) {
	possibleBasedirs, err := datasets.FindLatestDatasetDirs(outputDir, basedirBasename)
	if err != nil {
		return "", err
	}

	baseKey, ok := datasetKey(base)
	if !ok {
		return "", nil
	}

	for _, possibleBasedirDB := range possibleBasedirs {
		key, ok := datasetKey(filepath.Base(possibleBasedirDB))
		if ok && key == baseKey {
			return filepath.Join(possibleBasedirDB, basedirBasename), nil
		}
	}

	return "", nil
}

func summariseReqGroup(base string) string {
	key, ok := datasetKey(base)
	if !ok {
		return summariseReqGroupBase
	}

	return summariseReqGroupBase + "-" + key
}

func datasetKey(name string) (string, bool) {
	_, key, ok := datasets.SplitDatasetDirName(name)

	return key, ok
}

func hiddenOutputDir(outputDir, base string) string {
	return filepath.Join(outputDir, hiddenOutputPrefix+base)
}

func getJobCommand(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig, mounts,
	inputDir, base, outputDir string) string {
	inputBase := filepath.Join(inputDir, base)
	finalOutputBase := filepath.Join(outputDir, base)
	parts := []string{
		shellQuote(os.Args[0]),
		"summarise",
		clickhouseRecoverFlag,
		"-d",
		shellQuote(dotOutputBase),
	}

	if previousBasedirsDB != "" {
		parts = append(parts, "-s", shellQuote(previousBasedirsDB))
	}

	if mounts != "" {
		parts = append(parts, "-m", shellQuote(mounts))
	}

	parts = append(parts,
		"-q", shellQuote(quotaPath),
		"-c", shellQuote(basedirsConfig),
		shellQuote(filepath.Join(inputBase, inputStatsFile)),
		"&&", "touch", "-r", shellQuote(inputBase), shellQuote(dotOutputBase),
		"&&", "mv", shellQuote(dotOutputBase), shellQuote(finalOutputBase),
	)

	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	return fmt.Sprintf("%q", value)
}
