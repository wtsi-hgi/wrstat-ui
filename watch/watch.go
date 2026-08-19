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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/internal/watchenv"
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
	clickhouseBuildFlag   = "--clickhouse-build-only"
	clickhousePublishFlag = "--clickhouse-publish-only"
	summariseReqGroupBase = "wrstat-ui-summarise"
	summariseLimitGroup   = "wrstat-ui-clickhouse-import"
	defaultConcurrency    = 1
	summariseJobsPerInput = 2
)

var connectTimeout = 10 * time.Second //nolint:gochecknoglobals

var errSummariseConcurrency = errors.New("summarise concurrency cannot be negative")

// Options configures watch job scheduling.
type Options struct {
	// SummariseConcurrency limits the number of watch-scheduled ClickHouse
	// publish jobs that may run globally. Zero uses the default of one.
	SummariseConcurrency int
}

// Watch watches input directories (which should be output directories of wrstat
// multi runs) for new stats.gz files, upon which it will run the summarise
// subcommand on that data, if it has not already been run.
//
// The scheduled summarise subcommands will be given the output directory, quota
// path and basedirs config path. minMemGB is in GB and acts as the minimum
// requested memory for summarise jobs, with values below the default 8GB floor
// clamped upward. Higher learned or historical requirements remain unchanged.
// The queue and queuesAvoid values are passed to wr so scheduler submission can
// target or avoid specific queues. Spool builds are unrestricted, while Watch
// uses a global ClickHouse publish concurrency limit of one; use WithOptions
// to configure it.
func Watch(inputDirs []string, group, outputDir, quotaPath, basedirsConfig,
	mounts string, minMemGB int, queue, queuesAvoid string, logger log15.Logger) error {
	return WithOptions(inputDirs, group, outputDir, quotaPath, basedirsConfig,
		mounts, minMemGB, queue, queuesAvoid, Options{}, logger)
}

// WithOptions watches input directories and schedules summarise jobs with
// the supplied options.
func WithOptions(inputDirs []string, group, outputDir, quotaPath, basedirsConfig,
	mounts string, minMemGB int, queue, queuesAvoid string, options Options,
	logger log15.Logger) error {
	summariseConcurrency := options.SummariseConcurrency
	if summariseConcurrency < 0 {
		return errSummariseConcurrency
	}

	if summariseConcurrency == 0 {
		summariseConcurrency = defaultConcurrency
	}

	for {
		if err := watch(inputDirs, group, outputDir, quotaPath, basedirsConfig,
			mounts, minMemGB, queue, queuesAvoid, summariseConcurrency, logger); err != nil {
			return err
		}

		if client.PretendSubmissions != "" {
			return nil
		}

		time.Sleep(time.Minute)
	}
}

func watch(inputDirs []string, group, outputDir, quotaPath, basedirsConfig, mounts string,
	minMemGB int, queue, queuesAvoid string, summariseConcurrency int, logger log15.Logger) error {
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
			basedirsConfig, mounts, minMemGB, summariseConcurrency, inputPaths); err != nil {
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
	minMemGB, summariseConcurrency int, inputPaths []string) error {
	jobs := make([]*jobqueue.Job, 0, summariseJobsPerInput*len(inputPaths))

	for _, p := range inputPaths {
		base := filepath.Base(p)

		pair, err := createSummariseJobs(group, inputDir, outputDir, base,
			quotaPath, basedirsConfig, mounts, minMemGB, summariseConcurrency, s)
		if err != nil {
			return fmt.Errorf("error scheduling summarise (%s): %w", base, err)
		}

		jobs = append(jobs, pair...)
	}

	if len(jobs) == 0 {
		return nil
	}

	if err := s.SubmitJobs(jobs); err != nil {
		return fmt.Errorf("error submitting jobs to wr: %w", err)
	}

	return nil
}

func createSummariseJobs(group, inputDir, outputDir, base, quotaPath, basedirsConfig, mounts string,
	minMemGB, summariseConcurrency int, s *client.Scheduler) ([]*jobqueue.Job, error) {
	return createSummariseJobsAt(
		group, inputDir, outputDir, base, quotaPath, basedirsConfig, mounts,
		minMemGB, summariseConcurrency, time.Now(), client.UniqueString(), s,
	)
}

func createSummariseJobsAt(group, inputDir, outputDir, base, quotaPath, basedirsConfig, mounts string,
	minMemGB, summariseConcurrency int, createdAt time.Time, uniqueID string,
	s *client.Scheduler) ([]*jobqueue.Job, error) {
	dotOutputBase, previousBasedirsDB, repGroup, req, err := prepareSummariseJobs(
		outputDir, base, minMemGB, createdAt, uniqueID,
	)
	if err != nil {
		return nil, err
	}

	reqGroup := summariseReqGroup(base)

	buildJob, err := createSummarisePhaseJob(
		s, group, getSummariseJobCommand(dotOutputBase, previousBasedirsDB, quotaPath,
			basedirsConfig, mounts, inputDir, base, outputDir, clickhouseBuildFlag, false),
		repGroup, reqGroup+"-build", repGroup+"-build", req,
	)
	if err != nil {
		return nil, err
	}

	publishJob, err := createSummarisePhaseJob(
		s, group, getSummariseJobCommand(dotOutputBase, previousBasedirsDB, quotaPath,
			basedirsConfig, mounts, inputDir, base, outputDir, clickhousePublishFlag, true),
		repGroup, reqGroup+"-publish", "", req,
	)
	if err != nil {
		return nil, err
	}

	return configureSummariseJobPair(buildJob, publishJob, repGroup, summariseConcurrency), nil
}

func prepareSummariseJobs(
	outputDir string,
	base string,
	minMemGB int,
	createdAt time.Time,
	uniqueID string,
) (string, string, string, *scheduler.Requirements, error) {
	dotOutputBase := hiddenOutputDir(outputDir, base)
	if err := os.MkdirAll(dotOutputBase, dirPerms); err != nil {
		return "", "", "", nil, err
	}

	previousBasedirsDB, err := getPreviousBasedirsDB(outputDir, base)
	if err != nil {
		return "", "", "", nil, err
	}

	req := client.DefaultRequirements()
	req.Cores = summariseCores
	req.RAM = summariseMinRAMMB(minMemGB)
	req.Time = summariseMinRuntime

	return dotOutputBase, previousBasedirsDB, summariseJobName(createdAt, uniqueID), req, nil
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

func createSummarisePhaseJob(
	s *client.Scheduler,
	group string,
	command string,
	repGroup string,
	reqGroup string,
	depGroup string,
	req *scheduler.Requirements,
) (*jobqueue.Job, error) {
	job := s.NewJob(command, repGroup, reqGroup, depGroup, "", req)
	job.Group = group

	if err := job.EnvAddOverride([]string{watchenv.Name + "=" + watchenv.Value}); err != nil {
		return nil, fmt.Errorf("failed to mark watch-scheduled summarise job: %w", err)
	}

	return job, nil
}

func getSummariseJobCommand(dotOutputBase, previousBasedirsDB, quotaPath, basedirsConfig, mounts,
	inputDir, base, outputDir, phaseFlag string, promote bool) string {
	inputBase := filepath.Join(inputDir, base)
	finalOutputBase := filepath.Join(outputDir, base)

	parts := summariseJobCommandPrefix(dotOutputBase, phaseFlag)
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
		">", `"$summarise_log"`, "2>&1",
	)
	parts = appendSummarisePromotion(parts, inputBase, dotOutputBase, finalOutputBase, promote)

	return strings.Join(parts, " ")
}

func summariseJobCommandPrefix(dotOutputBase, phaseFlag string) []string {
	parts := []string{
		summariseLogAssignment(dotOutputBase, phaseFlag),
		"&&",
		shellQuote(os.Args[0]),
		"summarise",
		clickhouseRecoverFlag,
	}
	if phaseFlag != "" {
		parts = append(parts, phaseFlag)
	}

	parts = append(parts, "-d", shellQuote(dotOutputBase))

	return parts
}

func summariseLogAssignment(dotOutputBase, phaseFlag string) string {
	phase := "run"

	switch phaseFlag {
	case clickhouseBuildFlag:
		phase = "build"
	case clickhousePublishFlag:
		phase = "publish"
	}

	template := filepath.Join(dotOutputBase, fmt.Sprintf("summarise-%s.log-XXXXXXXXXX", phase))

	return fmt.Sprintf("summarise_log=$(mktemp %s)", shellQuote(template))
}

func summariseJobName(createdAt time.Time, uniqueID string) string {
	return fmt.Sprintf("%s-%s-%s", summariseReqGroupBase, createdAt.Format(jobTimestampLayout), uniqueID)
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

func appendSummarisePromotion(parts []string, inputBase, dotOutputBase, finalOutputBase string,
	promote bool) []string {
	if !promote {
		return parts
	}

	return append(parts,
		"&&", "touch", "-r", shellQuote(inputBase), shellQuote(dotOutputBase),
		"&&", "mv", shellQuote(dotOutputBase), shellQuote(finalOutputBase),
	)
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

func configureSummariseJobPair(buildJob, publishJob *jobqueue.Job, repGroup string,
	summariseConcurrency int) []*jobqueue.Job {
	publishJob.Dependencies = jobqueue.Dependencies{jobqueue.NewDepGroupDependency(repGroup + "-build")}
	publishJob.LimitGroups = []string{fmt.Sprintf("%s:%d", summariseLimitGroup, summariseConcurrency)}

	return []*jobqueue.Job{buildJob, publishJob}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
