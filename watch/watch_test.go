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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	wrtesting "github.com/VertebrateResequencing/wr/client/testing"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/watchenv"
)

var errTimedOut = errors.New("timed out")

const (
	attemptLogTestBase                = "20260517-200015_／nfs／t283_imaging"
	defaultSummariseLimitGroupForTest = "wrstat-ui-clickhouse-import:1"
	watchTestMountPath                = "/watch/test/"
)

func TestWatchSummariseResourceMinimums(t *testing.T) {
	Convey("Watch creates summarise jobs with resource minimums that wr can only raise", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		inputBase := "20260517-200015_／lustre／scratch127"
		lowMemBase := "20260517-200015_／lustre／scratch128"
		highMemBase := "20260517-200015_／lustre／scratch129"

		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		defer pr.Close()
		defer pw.Close()

		pretendSubmissions := client.PretendSubmissions

		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)
		defer func() {
			client.PretendSubmissions = pretendSubmissions
		}()

		s, err := client.New(client.SchedulerSettings{})
		So(err, ShouldBeNil)

		defer s.Disconnect() //nolint:errcheck

		jobs, err := createSummariseJobs("", inputDir, outputDir, inputBase, "", "", "", 0, 1, s)
		So(err, ShouldBeNil)
		So(jobs, ShouldHaveLength, 2)
		job := jobs[0]
		So(job.Requirements.RAM, ShouldEqual, 8192)
		So(job.Requirements.Time, ShouldEqual, 30*time.Minute)
		So(job.Requirements.Cores, ShouldEqual, 2)
		So(job.Override, ShouldEqual, 1)
		So(job.ReqGroup, ShouldEqual, "wrstat-ui-summarise-／lustre／scratch127-build")

		lowMemJobs, err := createSummariseJobs("", inputDir, outputDir, lowMemBase, "", "", "", 4, 1, s)
		So(err, ShouldBeNil)

		lowMemJob := lowMemJobs[0]
		So(lowMemJob.Requirements.RAM, ShouldEqual, 8192)
		So(lowMemJob.Requirements.Time, ShouldEqual, 30*time.Minute)
		So(lowMemJob.Override, ShouldEqual, 1)

		highMemJobs, err := createSummariseJobs("", inputDir, outputDir, highMemBase, "", "", "", 16, 1, s)
		So(err, ShouldBeNil)

		highMemJob := highMemJobs[0]
		So(highMemJob.Requirements.RAM, ShouldEqual, 16384)
		So(highMemJob.Requirements.Time, ShouldEqual, 30*time.Minute)
		So(highMemJob.Override, ShouldEqual, 1)
	})
}

func TestWatchRejectsInvalidSummariseConcurrency(t *testing.T) {
	Convey("Watch rejects a negative summarise concurrency before connecting to wr", t, func() {
		err := WithOptions(
			[]string{"input"}, "", "output", "quota", "basedirs", "", 0, "", "",
			Options{SummariseConcurrency: -1}, nil,
		)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "summarise concurrency cannot be negative")
	})
}

func TestWatchSummariseCommandFailsWhenAttemptLogCannotBeAllocated(t *testing.T) {
	Convey("A summarise command does not run when its attempt log cannot be allocated", t, func() {
		tempDir := t.TempDir()
		inputDir := filepath.Join(tempDir, "input")
		outputDir := filepath.Join(tempDir, "output")
		base := attemptLogTestBase
		dotOutputBase := hiddenOutputDir(outputDir, base)
		fakeBinDir := filepath.Join(tempDir, "bin")
		fakeWrstatUI := filepath.Join(fakeBinDir, "wrstat-ui")
		marker := filepath.Join(tempDir, "summarise-ran")

		So(os.MkdirAll(filepath.Join(inputDir, base), 0755), ShouldBeNil)
		So(os.MkdirAll(dotOutputBase, 0755), ShouldBeNil)
		So(os.MkdirAll(fakeBinDir, 0755), ShouldBeNil)
		fakeMktemp := filepath.Join(fakeBinDir, "mktemp")
		So(os.WriteFile(fakeMktemp, []byte("#!/bin/sh\nexit 73\n"), 0600), ShouldBeNil)
		So(os.Chmod(fakeMktemp, 0700), ShouldBeNil)
		So(os.WriteFile(fakeWrstatUI, []byte("#!/bin/sh\nprintf ran > \"$FAKE_SUMMARISE_MARKER\"\n"), 0600), ShouldBeNil)
		So(os.Chmod(fakeWrstatUI, 0700), ShouldBeNil)

		oldArgs := os.Args

		os.Args = []string{fakeWrstatUI}
		defer func() { os.Args = oldArgs }()

		command := getSummariseJobCommand(
			dotOutputBase, "", "", "", "", inputDir, base, outputDir, clickhouseBuildFlag, false,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		attempt := exec.CommandContext(ctx, "sh", "-c", command)

		attempt.Env = append(os.Environ(), "PATH="+fakeBinDir, "FAKE_SUMMARISE_MARKER="+marker)
		So(attempt.Run(), ShouldNotBeNil)
		So(entryExists(marker), ShouldBeFalse)

		logs, err := filepath.Glob(filepath.Join(dotOutputBase, "summarise-*"))
		So(err, ShouldBeNil)
		So(logs, ShouldBeEmpty)
	})
}

func TestWatchSummariseCommandUsesPortableAttemptLogTemplates(t *testing.T) {
	Convey("Summarise commands allocate phase-labelled logs with portable mktemp templates", t, func() {
		tempDir := t.TempDir()
		inputDir := filepath.Join(tempDir, "input")
		outputDir := filepath.Join(tempDir, "output")
		base := attemptLogTestBase
		dotOutputBase := hiddenOutputDir(outputDir, base)
		fakeBinDir := filepath.Join(tempDir, "bin")
		fakeWrstatUI := filepath.Join(fakeBinDir, "wrstat-ui")

		So(os.MkdirAll(filepath.Join(inputDir, base), 0755), ShouldBeNil)
		So(os.MkdirAll(dotOutputBase, 0755), ShouldBeNil)
		So(os.MkdirAll(fakeBinDir, 0755), ShouldBeNil)

		fakeMktemp := filepath.Join(fakeBinDir, "mktemp")
		So(os.WriteFile(fakeMktemp, []byte(`#!/bin/sh
template=$1
case "${template##*/}" in
*XXXXXX) ;;
*) exit 64 ;;
esac
path=$template
while [ "${path%X}" != "$path" ]; do
path=${path%X}
done
path=${path}portable
(umask 077; set -C; : > "$path") || exit 65
printf '%s\n' "$path"
`), 0600), ShouldBeNil)
		So(os.Chmod(fakeMktemp, 0700), ShouldBeNil)
		So(os.WriteFile(fakeWrstatUI, []byte("#!/bin/sh\nprintf '%s\\n' \"$2\"\n"), 0600), ShouldBeNil)
		So(os.Chmod(fakeWrstatUI, 0700), ShouldBeNil)

		oldArgs := os.Args

		os.Args = []string{fakeWrstatUI}
		defer func() { os.Args = oldArgs }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for _, phaseFlag := range []string{clickhouseBuildFlag, clickhousePublishFlag} {
			command := getSummariseJobCommand(
				dotOutputBase, "", "", "", "", inputDir, base, outputDir, phaseFlag, false,
			)
			attempt := exec.CommandContext(ctx, "sh", "-c", command)

			attempt.Env = append(os.Environ(), "PATH="+fakeBinDir)

			So(attempt.Run(), ShouldBeNil)
		}

		logs, err := filepath.Glob(filepath.Join(dotOutputBase, "summarise-*.log-*"))
		So(err, ShouldBeNil)
		So(logs, ShouldResemble, []string{
			filepath.Join(dotOutputBase, "summarise-build.log-portable"),
			filepath.Join(dotOutputBase, "summarise-publish.log-portable"),
		})
	})
}

func TestWatchSummariseConcurrencyLimit(t *testing.T) {
	Convey("Watch assigns only publish jobs to the default shared concurrency limit", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		defer pr.Close()
		defer pw.Close()

		pretendSubmissions := client.PretendSubmissions
		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		defer func() {
			client.PretendSubmissions = pretendSubmissions
		}()

		s, err := client.New(client.SchedulerSettings{})
		So(err, ShouldBeNil)

		defer s.Disconnect() //nolint:errcheck

		jobsA, err := createSummariseJobs("", inputDir, outputDir, "12345_a", "", "", "", 0, 1, s)
		So(err, ShouldBeNil)

		jobsB, err := createSummariseJobs("", inputDir, outputDir, "12345_b", "", "", "", 0, 1, s)
		So(err, ShouldBeNil)

		So(jobsA[0].LimitGroups, ShouldBeEmpty)
		So(jobsB[0].LimitGroups, ShouldBeEmpty)
		So(jobsA[1].LimitGroups, ShouldResemble, []string{defaultSummariseLimitGroupForTest})
		So(jobsB[1].LimitGroups, ShouldResemble, jobsA[1].LimitGroups)
		assertWatchScheduledEnvironment(jobsA[0])
		assertWatchScheduledEnvironment(jobsA[1])
		assertWatchScheduledEnvironment(jobsB[0])
		assertWatchScheduledEnvironment(jobsB[1])
	})

	Convey("Watch applies a configured summarise concurrency limit", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		defer pr.Close()
		defer pw.Close()

		pretendSubmissions := client.PretendSubmissions
		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		defer func() {
			client.PretendSubmissions = pretendSubmissions
		}()

		s, err := client.New(client.SchedulerSettings{})
		So(err, ShouldBeNil)

		defer s.Disconnect() //nolint:errcheck

		jobsA, err := createSummariseJobs("", inputDir, outputDir, "12345_a", "", "", "", 0, 4, s)
		So(err, ShouldBeNil)

		jobsB, err := createSummariseJobs("", inputDir, outputDir, "12345_b", "", "", "", 0, 4, s)
		So(err, ShouldBeNil)

		So(jobsA[0].LimitGroups, ShouldBeEmpty)
		So(jobsB[0].LimitGroups, ShouldBeEmpty)
		So(jobsA[1].LimitGroups, ShouldResemble, []string{"wrstat-ui-clickhouse-import:4"})
		So(jobsB[1].LimitGroups, ShouldResemble, jobsA[1].LimitGroups)
		assertWatchScheduledEnvironment(jobsA[0])
		assertWatchScheduledEnvironment(jobsA[1])
		assertWatchScheduledEnvironment(jobsB[0])
		assertWatchScheduledEnvironment(jobsB[1])
	})
}

func assertWatchScheduledEnvironment(job *jobqueue.Job) {
	job.EnvCRetrieved = true

	So(job.Getenv(watchenv.Name), ShouldEqual, watchenv.Value)
}

func TestWatchSplitsSummariseBuildFromPublish(t *testing.T) {
	Convey("Watch schedules an unlimited build before a ClickHouse-limited publish", t, func() {
		jobs := capturePublicWatchJobs(t, func(inputDir, outputDir string) error {
			return Watch([]string{inputDir}, "", outputDir, "quota", "basedirs", "", 0, "", "", nil)
		})

		So(jobs, ShouldHaveLength, 2)

		buildJob := jobs[0]
		publishJob := jobs[1]

		So(buildJob.Cmd, ShouldContainSubstring, "summarise --clickhouse-recover --clickhouse-build-only")
		So(buildJob.LimitGroups, ShouldBeEmpty)
		So(buildJob.Dependencies, ShouldBeEmpty)
		So(buildJob.Cmd, ShouldNotContainSubstring, "&& touch -r")
		So(buildJob.Cmd, ShouldNotContainSubstring, "&& mv")

		So(publishJob.Cmd, ShouldContainSubstring, "summarise --clickhouse-recover --clickhouse-publish-only")
		So(publishJob.LimitGroups, ShouldResemble, []string{defaultSummariseLimitGroupForTest})
		So(publishJob.RepGroup, ShouldEqual, buildJob.RepGroup)
		So(buildJob.DepGroups, ShouldResemble, []string{buildJob.RepGroup + "-build"})
		assertPublishGroupDependency(publishJob, buildJob.DepGroups[0])
		So(publishJob.Cmd, ShouldContainSubstring, "&& touch -r")
		So(publishJob.Cmd, ShouldContainSubstring, "&& mv")
	})
}

func TestWatchIsolatesSummarisePhaseScheduling(t *testing.T) {
	Convey("Distinct submissions for one target at the same instant use isolated build groups", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		createdAt := time.Date(2026, time.August, 19, 12, 34, 56, 789, time.UTC)
		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		defer pr.Close()
		defer pw.Close()

		previousPretendSubmissions := client.PretendSubmissions
		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		defer func() { client.PretendSubmissions = previousPretendSubmissions }()

		s, err := client.New(client.SchedulerSettings{})
		So(err, ShouldBeNil)

		defer s.Disconnect() //nolint:errcheck

		jobsA, err := createSummariseJobsAt(
			"", inputDir, outputDir, "12345_a", "quota-a", "", "", 0, 1, createdAt,
			"d7akoe4rvimc9pmg5t00", s,
		)
		So(err, ShouldBeNil)

		jobsB, err := createSummariseJobsAt(
			"", inputDir, outputDir, "12345_a", "quota-a", "", "", 0, 1, createdAt,
			"d7akoe4rvimc9pmg5t01", s,
		)
		So(err, ShouldBeNil)

		So(jobsA[0].ReqGroup, ShouldEqual, "wrstat-ui-summarise-a-build")
		So(jobsA[1].ReqGroup, ShouldEqual, "wrstat-ui-summarise-a-publish")
		So(jobsB[0].ReqGroup, ShouldEqual, "wrstat-ui-summarise-a-build")
		So(jobsB[1].ReqGroup, ShouldEqual, "wrstat-ui-summarise-a-publish")
		So(jobsA[0].RepGroup, ShouldEqual, jobsA[1].RepGroup)
		So(jobsB[0].RepGroup, ShouldEqual, jobsB[1].RepGroup)
		So(jobsA[0].Cmd, ShouldEqual, jobsB[0].Cmd)
		So(jobsA[1].Cmd, ShouldEqual, jobsB[1].Cmd)
		So(jobsA[0].RepGroup, ShouldNotEqual, jobsB[0].RepGroup)

		buildGroupA := jobsA[0].RepGroup + "-build"
		buildGroupB := jobsB[0].RepGroup + "-build"

		So(jobsA[0].DepGroups, ShouldResemble, []string{buildGroupA})
		So(jobsB[0].DepGroups, ShouldResemble, []string{buildGroupB})
		So(buildGroupA, ShouldNotEqual, buildGroupB)
		assertPublishGroupDependency(jobsA[1], buildGroupA)
		assertPublishGroupDependency(jobsB[1], buildGroupB)
		So(jobsA[1].Dependencies[0].DepGroup, ShouldNotEqual, buildGroupB)
		So(jobsB[1].Dependencies[0].DepGroup, ShouldNotEqual, buildGroupA)
	})

	Convey("Completing one build releases only its publisher", t, func() {
		previousPretendSubmissions := client.PretendSubmissions
		client.PretendSubmissions = ""

		defer func() { client.PretendSubmissions = previousPretendSubmissions }()

		config, restoreCWD := wrtesting.PrepareWrConfig(t)
		defer restoreCWD()

		t.Setenv("WR_DEPLOYMENT", "development")

		server := wrtesting.Serve(t, config)
		defer server.Stop(context.Background(), true)

		s, err := client.New(client.SchedulerSettings{Logger: log15.New()})
		So(err, ShouldBeNil)

		defer s.Disconnect() //nolint:errcheck

		inputDir := t.TempDir()
		outputDir := t.TempDir()
		createdAt := time.Date(2026, time.August, 19, 12, 34, 56, 789, time.UTC)
		jobsA, err := createSummariseJobsAt(
			"", inputDir, outputDir, "12345_a", "quota-a", "", "", 0, 1, createdAt,
			"d7akoe4rvimc9pmg5t00", s,
		)
		So(err, ShouldBeNil)

		jobsB, err := createSummariseJobsAt(
			"", inputDir, outputDir, "12345_a", "quota-b", "", "", 0, 1, createdAt,
			"d7akoe4rvimc9pmg5t01", s,
		)
		So(err, ShouldBeNil)

		So(s.SubmitJobs(append(jobsA, jobsB...)), ShouldBeNil)

		jq, err := jobqueue.ConnectUsingConfig(context.Background(), "development", 2*time.Second)
		So(err, ShouldBeNil)

		defer jq.Disconnect() //nolint:errcheck

		reserved := make(map[string]*jobqueue.Job, 2)

		for range 2 {
			job, reserveErr := jq.Reserve(time.Second)
			So(reserveErr, ShouldBeNil)

			reserved[job.Cmd] = job
		}

		So(reserved, ShouldContainKey, jobsA[0].Cmd)
		So(reserved, ShouldContainKey, jobsB[0].Cmd)
		So(reserved, ShouldNotContainKey, jobsA[1].Cmd)
		So(reserved, ShouldNotContainKey, jobsB[1].Cmd)

		archiveSuccessfulWatchJob(jq, reserved[jobsA[0].Cmd])

		publishA, err := jq.Reserve(time.Second)
		So(err, ShouldBeNil)
		So(publishA.Cmd, ShouldEqual, jobsA[1].Cmd)
		archiveSuccessfulWatchJob(jq, publishA)

		archiveSuccessfulWatchJob(jq, reserved[jobsB[0].Cmd])

		publishB, err := jq.Reserve(time.Second)
		So(err, ShouldBeNil)
		So(publishB.Cmd, ShouldEqual, jobsB[1].Cmd)
	})
}

func TestWatchRestartDoesNotDuplicatePendingPublish(t *testing.T) {
	Convey("A watch restart preserves a built spool and its already-pending publish", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		base := "12345_a"
		inputBase := filepath.Join(inputDir, base)
		statsPath := filepath.Join(inputBase, inputStatsFile)

		So(os.Mkdir(inputBase, 0o755), ShouldBeNil)
		So(createFile(statsPath), ShouldBeNil)

		previousPretendSubmissions := client.PretendSubmissions

		client.PretendSubmissions = ""
		defer func() { client.PretendSubmissions = previousPretendSubmissions }()

		config, restoreCWD := wrtesting.PrepareWrConfig(t)
		defer restoreCWD()

		t.Setenv("WR_DEPLOYMENT", "development")

		server := wrtesting.Serve(t, config)
		defer server.Stop(context.Background(), true)

		logger := log15.New()
		So(watch([]string{inputDir}, "", outputDir, "", "", "", 0, "", "", 1, logger), ShouldBeNil)

		jq, err := jobqueue.ConnectUsingConfig(context.Background(), "development", 2*time.Second)
		So(err, ShouldBeNil)

		defer jq.Disconnect() //nolint:errcheck

		buildJob, err := jq.Reserve(time.Second)
		So(err, ShouldBeNil)
		So(buildJob.Cmd, ShouldContainSubstring, clickhouseBuildFlag)
		So(buildJob.LimitGroups, ShouldBeEmpty)

		spoolDir := filepath.Join(hiddenOutputDir(outputDir, base), ".wrstat-ui-clickhouse-spool")
		manifest := writeVerifiedWatchSpool(t, spoolDir, hiddenOutputDir(outputDir, base), statsPath)
		spoolBeforeRestart := watchSpoolHashes(t, spoolDir)

		So(jq.Started(buildJob, os.Getpid()), ShouldBeNil)
		So(jq.Archive(buildJob, &jobqueue.JobEndState{
			Exited:  true,
			EndTime: time.Now(),
		}), ShouldBeNil)
		completedBuild, err := jq.GetByEssence(buildJob.ToEssense(), false, false)
		So(err, ShouldBeNil)
		So(completedBuild.State, ShouldEqual, jobqueue.JobStateComplete)

		pendingBeforeRestart := incompleteWatchJobs(t, jq)
		assertOnlyPendingPublish(pendingBeforeRestart)
		publishKey := pendingBeforeRestart[0].Key()

		So(watch([]string{inputDir}, "", outputDir, "", "", "", 0, "", "", 1, logger), ShouldBeNil)

		pendingAfterRestart := incompleteWatchJobs(t, jq)
		assertOnlyPendingPublish(pendingAfterRestart)
		So(pendingAfterRestart[0].Key(), ShouldEqual, publishKey)
		So(watchSpoolHashes(t, spoolDir), ShouldResemble, spoolBeforeRestart)

		gotManifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)
		So(chspool.VerifyManifest(spoolDir, gotManifest, *manifest), ShouldBeNil)
	})
}

func TestWatchPublicConcurrencyAPI(t *testing.T) {
	Convey("The established Watch API keeps its exact signature and defaults to one", t, func() {
		type legacyWatchFunc func(
			[]string, string, string, string, string, string, int, string, string, log15.Logger,
		) error

		legacyWatch := legacyWatchFunc(Watch)

		jobs := capturePublicWatchJobs(t, func(inputDir, outputDir string) error {
			return legacyWatch([]string{inputDir}, "", outputDir, "quota", "basedirs", "", 0, "", "", nil)
		})

		So(jobs, ShouldHaveLength, 2)
		So(jobs[0].LimitGroups, ShouldBeEmpty)
		So(jobs[1].LimitGroups, ShouldResemble, []string{defaultSummariseLimitGroupForTest})
	})

	Convey("WithOptions applies a configured concurrency", t, func() {
		jobs := capturePublicWatchJobs(t, func(inputDir, outputDir string) error {
			options := Options{SummariseConcurrency: 4}

			return WithOptions(
				[]string{inputDir}, "", outputDir, "quota", "basedirs", "", 0, "", "", options, nil,
			)
		})

		So(jobs, ShouldHaveLength, 2)
		So(jobs[0].LimitGroups, ShouldBeEmpty)
		So(jobs[1].LimitGroups, ShouldResemble, []string{"wrstat-ui-clickhouse-import:4"})
	})
}

func capturePublicWatchJobs(t *testing.T, run func(inputDir, outputDir string) error) []*jobqueue.Job {
	t.Helper()

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	inputBase := filepath.Join(inputDir, "12345_a")
	So(os.Mkdir(inputBase, 0o755), ShouldBeNil)
	So(createFile(filepath.Join(inputBase, inputStatsFile)), ShouldBeNil)

	pr, pw, err := os.Pipe()
	So(err, ShouldBeNil)

	defer pw.Close()

	previousPretendSubmissions := client.PretendSubmissions

	client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)
	defer func() { client.PretendSubmissions = previousPretendSubmissions }()

	So(run(inputDir, outputDir), ShouldBeNil)

	var jobs []*jobqueue.Job
	So(json.NewDecoder(pr).Decode(&jobs), ShouldBeNil)
	So(pr.Close(), ShouldBeNil)

	return jobs
}

func assertPublishGroupDependency(job *jobqueue.Job, buildGroup string) {
	So(job.DepGroups, ShouldBeEmpty)
	So(job.Dependencies, ShouldHaveLength, 1)
	So(job.Dependencies[0].DepGroup, ShouldEqual, buildGroup)
	So(job.Dependencies[0].Essence, ShouldBeNil)
}

func archiveSuccessfulWatchJob(jq *jobqueue.Client, job *jobqueue.Job) {
	So(jq.Started(job, os.Getpid()), ShouldBeNil)
	So(jq.Archive(job, &jobqueue.JobEndState{Exited: true, EndTime: time.Now()}), ShouldBeNil)
}

func TestWatchSummariseCommandPreservesAttemptLogs(t *testing.T) {
	Convey("Repeated concurrent runs preserve distinct phase-labelled attempt logs", t, func() {
		tempDir := t.TempDir()
		outputDir := filepath.Join(tempDir, "output-$WRSTAT_UI_UNSAFE_TOKEN-`touch BACKTICK_MARKER`-$(touch DOLLAR_MARKER)")
		inputDir := filepath.Join(tempDir, "input")
		base := attemptLogTestBase
		inputBase := filepath.Join(inputDir, base)
		dotOutputBase := hiddenOutputDir(outputDir, base)
		finalOutputBase := filepath.Join(outputDir, base)
		fakeWrstatUI := filepath.Join(
			tempDir,
			"bin-$WRSTAT_UI_UNSAFE_TOKEN-`touch EXE_BACKTICK_MARKER`-$(touch EXE_DOLLAR_MARKER)",
			"wrstat-ui",
		)

		So(os.MkdirAll(inputBase, 0755), ShouldBeNil)
		So(os.MkdirAll(dotOutputBase, 0755), ShouldBeNil)
		So(createFile(filepath.Join(inputBase, inputStatsFile)), ShouldBeNil)
		So(os.MkdirAll(filepath.Dir(fakeWrstatUI), 0755), ShouldBeNil)
		So(os.WriteFile(fakeWrstatUI, []byte(`#!/bin/sh
printf 'stdout:%s\n' "$FAKE_SUMMARISE_ATTEMPT"
printf 'stderr:%s\n' "$FAKE_SUMMARISE_ATTEMPT" >&2
exit "$FAKE_SUMMARISE_EXIT"
`), 0600), ShouldBeNil)
		So(os.Chmod(fakeWrstatUI, 0700), ShouldBeNil)

		oldArgs := os.Args

		os.Args = []string{fakeWrstatUI}
		defer func() {
			os.Args = oldArgs
		}()

		buildCommand := getSummariseJobCommand(
			dotOutputBase, "", "", "", "", inputDir, base, outputDir, clickhouseBuildFlag, false,
		)
		publishCommand := getSummariseJobCommand(
			dotOutputBase, "", "", "", "", inputDir, base, outputDir, clickhousePublishFlag, true,
		)

		So(buildCommand, ShouldContainSubstring, `> "$summarise_log" 2>&1`)
		So(publishCommand, ShouldContainSubstring, `> "$summarise_log" 2>&1`)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const attempts = 32

		results := make(chan error, attempts)

		var wg sync.WaitGroup

		for attempt := range attempts {
			wg.Add(1)

			go func() {
				defer wg.Done()

				failedAttempt := exec.CommandContext(ctx, "sh", "-c", buildCommand)
				failedAttempt.Dir = tempDir

				failedAttempt.Env = append(
					os.Environ(), "FAKE_SUMMARISE_EXIT=7", "WRSTAT_UI_UNSAFE_TOKEN=expanded",
					fmt.Sprintf("FAKE_SUMMARISE_ATTEMPT=%02d", attempt),
				)
				results <- failedAttempt.Run()
			}()
		}

		wg.Wait()
		close(results)

		failed := 0

		for err := range results {
			if err != nil {
				failed++
			}
		}

		So(failed, ShouldEqual, attempts)

		stagingLogs, err := filepath.Glob(filepath.Join(dotOutputBase, "summarise-build.log-*"))
		So(err, ShouldBeNil)
		So(stagingLogs, ShouldHaveLength, attempts)
		So(entryExists(finalOutputBase), ShouldBeFalse)

		contents, err := attemptLogContents(stagingLogs)
		So(err, ShouldBeNil)

		expectedContents := make(map[string]int, attempts)
		for attempt := range attempts {
			expectedContents[fmt.Sprintf("stdout:%02d\nstderr:%02d\n", attempt, attempt)] = 1
		}

		So(contents, ShouldResemble, expectedContents)

		successfulRetry := exec.CommandContext(ctx, "sh", "-c", publishCommand)

		successfulRetry.Dir = tempDir

		successfulRetry.Env = append(
			os.Environ(), "FAKE_SUMMARISE_EXIT=0", "FAKE_SUMMARISE_ATTEMPT=publish",
			"WRSTAT_UI_UNSAFE_TOKEN=expanded",
		)
		So(successfulRetry.Run(), ShouldBeNil)

		finalBuildLogs, err := filepath.Glob(filepath.Join(finalOutputBase, "summarise-build.log-*"))
		So(err, ShouldBeNil)
		So(finalBuildLogs, ShouldHaveLength, attempts)

		finalPublishLogs, err := filepath.Glob(filepath.Join(finalOutputBase, "summarise-publish.log-*"))
		So(err, ShouldBeNil)
		So(finalPublishLogs, ShouldHaveLength, 1)
		So(entryExists(dotOutputBase), ShouldBeFalse)

		publishLog, err := os.ReadFile(finalPublishLogs[0])
		So(err, ShouldBeNil)
		So(string(publishLog), ShouldEqual, "stdout:publish\nstderr:publish\n")

		So(entryExists(filepath.Join(tempDir, "BACKTICK_MARKER")), ShouldBeFalse)
		So(entryExists(filepath.Join(tempDir, "DOLLAR_MARKER")), ShouldBeFalse)
		So(entryExists(filepath.Join(tempDir, "EXE_BACKTICK_MARKER")), ShouldBeFalse)
		So(entryExists(filepath.Join(tempDir, "EXE_DOLLAR_MARKER")), ShouldBeFalse)
	})
}

func TestWatch(t *testing.T) {
	Convey("Given the expected setup", t, func() {
		const reqGroupABC = "wrstat-ui-summarise-abc-publish"

		inputDir := t.TempDir()
		outputDir := t.TempDir()
		testInputA := filepath.Join(inputDir, "12345_abc")
		testInputB := filepath.Join(inputDir, "12345_def")
		wrWrittenCh := make(chan bool)

		cwd, err := os.Getwd()
		So(err, ShouldBeNil)

		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		var jobs []*jobqueue.Job

		go func() {
			defer pr.Close()
			defer close(wrWrittenCh)

			j := json.NewDecoder(pr)

			for {
				var js []*jobqueue.Job

				if errr := j.Decode(&js); errr != nil {
					return
				}

				jobs = append(jobs, js...)
			}
		}()

		So(os.Mkdir(testInputA, 0755), ShouldBeNil)
		So(os.Mkdir(testInputB, 0755), ShouldBeNil)
		So(createFile(filepath.Join(testInputA, inputStatsFile)), ShouldBeNil)

		Convey("Watch will spot a new directory and schedule a summarise", func() {
			err = watch(
				[]string{inputDir},
				"",
				outputDir,
				"/path/to/quota",
				"/path/to/basedirs.config",
				"/path/to/mounts",
				0,
				"",
				"",
				1,
				nil,
			)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			assertSummariseRepGroup(jobs[0].RepGroup)
			So(publishJobsWithoutDynamicSchedulingFields(jobs), ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(expectedSummariseLogAssignment(`'%[2]s/.12345_abc'`)+
						`'%[1]s' summarise --clickhouse-recover --clickhouse-publish-only `+
						`-d '%[2]s/.12345_abc' `+
						`-m '/path/to/mounts' `+
						`-q '/path/to/quota' -c '/path/to/basedirs.config' `+
						`'%[3]s/stats.gz' > "$summarise_log" 2>&1 `+
						`&& touch -r '%[3]s' '%[2]s/.12345_abc' `+
						`&& mv '%[2]s/.12345_abc' '%[2]s/12345_abc'`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					ReqGroup:   reqGroupABC,
					LimitGroups: []string{
						defaultSummariseLimitGroupForTest,
					},
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  summariseMinRuntime,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch overrides summarise RAM when min mem is set", func() {
			err = watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "/path/to/mounts", 16, "", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			So(jobs[0].Requirements, ShouldNotBeNil)
			So(jobs[0].Requirements.RAM, ShouldEqual, 16384)
			So(jobs[0].Override, ShouldEqual, 1)
		})

		Convey("Watch leaves job requirements other nil when no queue settings are provided", func() {
			err = watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			So(jobs[0].Requirements, ShouldNotBeNil)
			So(jobs[0].Requirements.Other, ShouldBeNil)
		})

		Convey("Watch passes queue through to job requirements other", func() {
			err = watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "myq", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			So(jobs[0].Requirements, ShouldNotBeNil)
			So(jobs[0].Requirements.Other, ShouldResemble, map[string]string{
				"scheduler_queue": "myq",
			})
		})

		Convey("Watch passes queues to avoid through to job requirements other", func() {
			err = watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "badq", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			So(jobs[0].Requirements, ShouldNotBeNil)
			So(jobs[0].Requirements.Other, ShouldResemble, map[string]string{
				"scheduler_queues_avoid": "badq",
			})
		})

		Convey("Watch passes queue and queues to avoid through to job requirements other", func() {
			err = watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "q1,q2", "q3", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			So(jobs[0].Requirements, ShouldNotBeNil)
			So(jobs[0].Requirements.Other, ShouldResemble, map[string]string{
				"scheduler_queue":        "q1,q2",
				"scheduler_queues_avoid": "q3",
			})
		})

		Convey("Watch will provide absolute paths to summarise given relative paths", func() {
			parentDir := filepath.Dir(inputDir)

			relInput := filepath.Base(inputDir)
			relOutput := filepath.Base(outputDir)

			err = os.Chdir(parentDir)
			So(err, ShouldBeNil)

			err := watch([]string{relInput}, "myGroup", relOutput, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "", 1, nil)

			errr := os.Chdir(cwd)
			So(errr, ShouldBeNil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			assertSummariseRepGroup(jobs[0].RepGroup)
			So(publishJobsWithoutDynamicSchedulingFields(jobs), ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(expectedSummariseLogAssignment(`'%[2]s/.12345_abc'`)+
						`'%[1]s' summarise --clickhouse-recover --clickhouse-publish-only `+
						`-d '%[2]s/.12345_abc' `+
						`-q '/path/to/quota' -c '/path/to/basedirs.config' `+
						`'%[3]s/stats.gz' > "$summarise_log" 2>&1 `+
						`&& touch -r '%[3]s' '%[2]s/.12345_abc' `+
						`&& mv '%[2]s/.12345_abc' '%[2]s/12345_abc'`,
						os.Args[0], outputDir, testInputA),
					Cwd:        parentDir,
					CwdMatters: true,
					ReqGroup:   reqGroupABC,
					Group:      "myGroup",
					LimitGroups: []string{
						defaultSummariseLimitGroupForTest,
					},
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  summariseMinRuntime,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch will not reschedule a summarise if one has already started", func() {
			So(os.Mkdir(filepath.Join(outputDir, ".12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(len(jobs), ShouldEqual, 0)
		})

		Convey("Watch will not reschedule a summarise if one has already completed", func() {
			So(os.Mkdir(filepath.Join(outputDir, "12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(len(jobs), ShouldEqual, 0)
		})

		Convey("Watch will recognise existing basedir history in the output path", func() {
			existingOutput := filepath.Join(outputDir, "00001_abc")
			So(os.Mkdir(existingOutput, 0755), ShouldBeNil)
			So(createFile(filepath.Join(existingOutput, basedirBasename)), ShouldBeNil)

			err := watch([]string{inputDir}, "", outputDir, "/path/to/quota",
				"/path/to/basedirs.config", "", 0, "", "", 1, nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 2)
			assertSummariseRepGroup(jobs[0].RepGroup)
			So(publishJobsWithoutDynamicSchedulingFields(jobs), ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(expectedSummariseLogAssignment(`'%[2]s/.12345_abc'`)+
						`'%[1]s' summarise --clickhouse-recover --clickhouse-publish-only `+
						`-d '%[2]s/.12345_abc' `+
						`-s '%[2]s/00001_abc/basedirs.db' `+
						`-q '/path/to/quota' -c '/path/to/basedirs.config' `+
						`'%[3]s/stats.gz' > "$summarise_log" 2>&1 `+
						`&& touch -r '%[3]s' '%[2]s/.12345_abc' `+
						`&& mv '%[2]s/.12345_abc' '%[2]s/12345_abc'`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					ReqGroup:   reqGroupABC,
					LimitGroups: []string{
						defaultSummariseLimitGroupForTest,
					},
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  summariseMinRuntime,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch can watch multiple directories", func() {
			inputDir2 := t.TempDir()
			testInputC := filepath.Join(inputDir2, "98765_c")
			So(os.Mkdir(testInputC, 0755), ShouldBeNil)
			So(createFile(filepath.Join(testInputC, inputStatsFile)), ShouldBeNil)

			err := watch(
				[]string{inputDir, inputDir2},
				"",
				outputDir,
				"/path/to/quota",
				"/path/to/basedirs.config",
				"",
				0,
				"",
				"",
				1,
				nil,
			)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldHaveLength, 4)
			assertSummariseRepGroup(jobs[0].RepGroup)
			assertSummariseRepGroup(jobs[2].RepGroup)
			So(publishJobsWithoutDynamicSchedulingFields(jobs), ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(expectedSummariseLogAssignment(`'%[2]s/.12345_abc'`)+
						`'%[1]s' summarise --clickhouse-recover --clickhouse-publish-only `+
						`-d '%[2]s/.12345_abc' `+
						`-q '/path/to/quota' -c '/path/to/basedirs.config' `+
						`'%[3]s/stats.gz' > "$summarise_log" 2>&1 `+
						`&& touch -r '%[3]s' '%[2]s/.12345_abc' `+
						`&& mv '%[2]s/.12345_abc' '%[2]s/12345_abc'`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					ReqGroup:   reqGroupABC,
					LimitGroups: []string{
						defaultSummariseLimitGroupForTest,
					},
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  summariseMinRuntime,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
				{
					Cmd: fmt.Sprintf(expectedSummariseLogAssignment(`'%[2]s/.98765_c'`)+
						`'%[1]s' summarise --clickhouse-recover --clickhouse-publish-only `+
						`-d '%[2]s/.98765_c' `+
						`-q '/path/to/quota' -c '/path/to/basedirs.config' `+
						`'%[3]s/stats.gz' > "$summarise_log" 2>&1 `+
						`&& touch -r '%[3]s' '%[2]s/.98765_c' `+
						`&& mv '%[2]s/.98765_c' '%[2]s/98765_c'`,
						os.Args[0], outputDir, testInputC),
					Cwd:        cwd,
					CwdMatters: true,
					ReqGroup:   "wrstat-ui-summarise-c-publish",
					LimitGroups: []string{
						defaultSummariseLimitGroupForTest,
					},
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  summariseMinRuntime,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("watch errors if can't connect to manager", func() {
			tempDir := t.TempDir()

			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			tokenPath := filepath.Join(tempDir, "token")
			So(os.WriteFile(tokenPath, []byte("token"), 0600), ShouldBeNil)

			os.Setenv("WR_MANAGERTOKENFILE", tokenPath)
			os.Setenv("WR_MANAGERCAFILE", certPath)
			os.Setenv("WR_MANAGERCERTFILE", certPath)
			os.Setenv("WR_MANAGERKEYFILE", keyPath)

			client.PretendSubmissions = ""
			logger := log15.New()

			errCh := make(chan error, 1)

			connectTimeout = time.Second

			go func() {
				time.Sleep(3 * connectTimeout)
				errCh <- errTimedOut
			}()

			go func() {
				errCh <- watch([]string{inputDir}, "", outputDir, "/path/to/quota",
					"/path/to/basedirs.config", "", 0, "", "", 1, logger)
			}()

			err = <-errCh
			So(err, ShouldNotBeNil)
			So(err, ShouldNotEqual, errTimedOut)
			So(err.Error(), ShouldContainSubstring, "could not reach the server")
		})
	})
}

func TestWatchSummariseReqGroupIncludesMountKey(t *testing.T) {
	Convey("Watch sets summarise requirements group per mount key", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		inputBase := "20260517-200015_／lustre／scratch127"
		testInput := filepath.Join(inputDir, inputBase)
		wrWrittenCh := make(chan bool)

		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		var jobs []*jobqueue.Job

		go func() {
			defer pr.Close()
			defer close(wrWrittenCh)

			j := json.NewDecoder(pr)

			for {
				var js []*jobqueue.Job

				if errr := j.Decode(&js); errr != nil {
					return
				}

				jobs = append(jobs, js...)
			}
		}()

		So(os.Mkdir(testInput, 0755), ShouldBeNil)
		So(createFile(filepath.Join(testInput, inputStatsFile)), ShouldBeNil)

		err = watch([]string{inputDir}, "", outputDir, "", "", "", 0, "", "", 1, nil)
		So(err, ShouldBeNil)

		pw.Close()

		<-wrWrittenCh

		So(jobs, ShouldHaveLength, 2)
		So(jobs[0].ReqGroup, ShouldEqual, "wrstat-ui-summarise-／lustre／scratch127-build")
		So(jobs[1].ReqGroup, ShouldEqual, "wrstat-ui-summarise-／lustre／scratch127-publish")
	})
}

func createFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	return f.Close()
}

func assertSummariseRepGroup(repGroup string) {
	So(repGroup, ShouldStartWith, summariseReqGroupBase+"-")

	remainder := strings.TrimPrefix(repGroup, summariseReqGroupBase+"-")
	timestamp, targetID, found := strings.Cut(remainder, "-")
	So(found, ShouldBeTrue)
	So(targetID, ShouldHaveLength, 20)

	_, err := time.Parse(jobTimestampLayout, timestamp)
	So(err, ShouldBeNil)
}

func publishJobsWithoutDynamicSchedulingFields(jobs []*jobqueue.Job) []*jobqueue.Job {
	publishJobs := make([]*jobqueue.Job, 0, len(jobs)/2)
	for idx, job := range jobs {
		if idx%2 == 0 {
			continue
		}

		job.RepGroup = ""
		job.EnvOverride = nil
		job.EnvCRetrieved = false
		job.Dependencies = nil
		publishJobs = append(publishJobs, job)
	}

	return publishJobs
}

func expectedSummariseLogAssignment(quotedDotOutput string) string {
	return `summarise_log=$(mktemp ` + strings.TrimSuffix(quotedDotOutput, "'") +
		`/summarise-publish.log-XXXXXXXXXX') && `
}

func attemptLogContents(paths []string) (map[string]int, error) {
	contents := make(map[string]int, len(paths))

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		contents[string(data)]++
	}

	return contents, nil
}

func writeVerifiedWatchSpool(t *testing.T, spoolDir, outputDir, statsPath string) *chspool.Manifest {
	t.Helper()

	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)
	So(set.WriteDir(chspool.DirRow{
		MountPath:  watchTestMountPath,
		SnapshotID: "00000000-0000-0000-0000-000000000001",
		DirID:      1,
		FullPath:   watchTestMountPath,
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	stats, err := chspool.IdentifyExistingPath(statsPath, false)
	So(err, ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    watchTestMountPath,
		SnapshotID:   "00000000-0000-0000-0000-000000000001",
		UpdatedAt:    time.Unix(1, 0).UTC().Format(time.RFC3339Nano),
		OutputDir:    outputDir,
		SchemaMarker: "watch-restart-test",
		Stats:        stats,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Unix(2, 0).UTC().Format(time.RFC3339Nano),
	}
	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	got, err := chspool.ReadManifest(spoolDir)
	So(err, ShouldBeNil)
	So(chspool.VerifyManifest(spoolDir, got, *manifest), ShouldBeNil)

	return manifest
}

func watchSpoolHashes(t *testing.T, spoolDir string) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(spoolDir)
	So(err, ShouldBeNil)

	hashes := make(map[string]string, len(entries))
	for _, entry := range entries {
		_, hash, hashErr := chspool.HashFile(filepath.Join(spoolDir, entry.Name()))
		So(hashErr, ShouldBeNil)

		hashes[entry.Name()] = hash
	}

	return hashes
}

func incompleteWatchJobs(t *testing.T, jq *jobqueue.Client) []*jobqueue.Job {
	t.Helper()

	jobs, err := jq.GetIncomplete(0, "", false, false)
	So(err, ShouldBeNil)

	return jobs
}

func assertOnlyPendingPublish(jobs []*jobqueue.Job) {
	So(jobs, ShouldHaveLength, 1)
	So(jobs[0].Cmd, ShouldContainSubstring, clickhousePublishFlag)
	So(jobs[0].Cmd, ShouldNotContainSubstring, clickhouseBuildFlag)
	So(jobs[0].State, ShouldEqual, jobqueue.JobStateReady)
	So(jobs[0].LimitGroups, ShouldResemble, []string{summariseLimitGroup})
}
