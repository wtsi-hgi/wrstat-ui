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
package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	_ "unsafe"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
)

const app = "wrstat-ui_test"

const (
	clickHousePerfPhasePartitionDropReset = "partition_drop_reset"
	clickHousePerfPhaseDGUTAInsert        = "wrstat_dguta_insert"
	clickHousePerfPhaseChildrenInsert     = "wrstat_children_insert"
	clickHousePerfPhaseMountSwitch        = "mount_switch"
	clickHousePerfPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"
)

var (
	errSummariseTestFiles    = errors.New("summarise files")
	errSummariseTestBasedirs = errors.New("summarise basedirs")
	errSummariseTestDGUTA    = errors.New("summarise dguta")
)

type clickHouseCLIEnv struct {
	DSN      string
	Database string
}

func setupClickHouseCLIEnv(t *testing.T) clickHouseCLIEnv {
	t.Helper()

	if envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN"); envDSN != "" {
		refuseNonLocalhostDSN(t, envDSN)

		db := newTestDatabaseName(t)
		dsn := withDatabaseInDSN(t, envDSN, db)

		t.Setenv("WRSTAT_CLICKHOUSE_DSN", dsn)
		t.Setenv("WRSTAT_CLICKHOUSE_DATABASE", db)

		return clickHouseCLIEnv{DSN: dsn, Database: db}
	}

	binPath := findClickHouseBinary(t)
	baseDir := t.TempDir()

	tcpPort := pickFreePort(t)

	httpPort := pickFreePort(t)
	for httpPort == tcpPort {
		httpPort = pickFreePort(t)
	}

	startClickHouseServer(t, binPath, baseDir, tcpPort, httpPort)
	waitForTCPPort(t, "127.0.0.1", tcpPort)

	db := newTestDatabaseName(t)
	dsn := fmt.Sprintf(
		"clickhouse://default@127.0.0.1:%d/default?database=%s&dial_timeout=1s",
		tcpPort,
		url.QueryEscape(db),
	)

	t.Setenv("WRSTAT_CLICKHOUSE_DSN", dsn)
	t.Setenv("WRSTAT_CLICKHOUSE_DATABASE", db)

	return clickHouseCLIEnv{DSN: dsn, Database: db}
}

func TestSummarise(t *testing.T) {
	Convey("summarise produces the correct output", t, func() {
		chEnv := setupClickHouseCLIEnv(t)

		gid, uid, _, _, err := internaluser.RealGIDAndUID()
		So(err, ShouldBeNil)

		refTime := time.Now().Truncate(time.Second)
		yesterday := refTime.Add(-24 * time.Hour)

		_, root := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"lustre", 1, 1<<29, 1<<31, true, yesterday.Unix())

		inputDir := t.TempDir()
		outputDir := t.TempDir()
		quotaFile := filepath.Join(inputDir, "quota.csv")
		basedirsConfig := filepath.Join(inputDir, "basedirs.config")
		mounts := filepath.Join(inputDir, "mounts")

		So(os.WriteFile(quotaFile, []byte(`1,/lustre/scratch125,4000000000,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
77777,/lustre/scratch125,500,50
1,/nfs/scratch125,4000000000,20
2,/nfs/scratch125,300,30
2,/nfs/scratch123,400,40
77777,/nfs/scratch125,500,50
3,/lustre/scratch125,300,30
`), 0600), ShouldBeNil)

		So(os.WriteFile(basedirsConfig, []byte(`/lustre/scratch123/hgi/mdt	5	5
/nfs/scratch123/hgi/mdt	5	5
/	4	4`), 0600), ShouldBeNil)

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		So(os.WriteFile(mounts, []byte(`"/nfs/"
"/lustre/"`), 0600), ShouldBeNil)

		inputA := filepath.Join(inputDir, "inputA")
		inputB := filepath.Join(inputDir, "inputB")

		datasetDir := filepath.Join(outputDir, "2_／lustre")
		err = os.MkdirAll(datasetDir, 0755)
		So(err, ShouldBeNil)

		outputA := filepath.Join(datasetDir, "A")
		outputB := filepath.Join(datasetDir, "B")

		err = os.Mkdir(outputA, 0755)
		So(err, ShouldBeNil)

		err = os.Mkdir(outputB, 0755)
		So(err, ShouldBeNil)

		f, err := os.Create(inputA)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, root.AsReader())
		So(err, ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		So(os.Chtimes(inputA, yesterday, yesterday), ShouldBeNil)

		stdout, stderr, _, err := runWRStat("summarise", "-d", outputA, "-q", quotaFile,
			"-c", basedirsConfig, "-m", mounts, inputA)
		if err != nil {
			t.Logf("stdout: %s", stdout)
			t.Logf("stderr: %s", stderr)
		}
		So(err, ShouldBeNil)

		compareFileContents(t, filepath.Join(outputA, "bygroup"), sortLines(fmt.Sprintf("%[1]s\t%[2]s\t2\t2684354560\n"+
			"%[3]s\t%[4]s\t2\t80\n"+
			"%[3]s\t%[5]s\t2\t50\n"+
			"%[6]s\t%[5]s\t1\t60\n"+
			"%[7]s\t%[8]s\t5\t15\n"+
			"%[9]s\t%[10]s\t2\t100\n",
			internaluser.GetGroupName(t, "1"), internaluser.GetUsername(t, "101"),
			internaluser.GetGroupName(t, "2"), internaluser.GetUsername(t, "88888"),
			internaluser.GetUsername(t, "102"), internaluser.GetGroupName(t, "77777"),
			internaluser.GetGroupName(t, strconv.Itoa(int(gid))), internaluser.GetUsername(t, strconv.Itoa(int(uid))),
			internaluser.GetGroupName(t, "3"), internaluser.GetUsername(t, "103"),
		)))

		compareFileContents(t, filepath.Join(outputA, "byusergroup.gz"),
			sortLines(fmt.Sprintf("%[1]s\t%[2]s\t\"/\"\t2\t100\n"+
				"%[1]s\t%[2]s\t\"/lustre/\"\t2\t100\n"+
				"%[1]s\t%[2]s\t\"/lustre/scratch125/\"\t2\t100\n"+
				"%[1]s\t%[2]s\t\"/lustre/scratch125/humgen/\"\t2\t100\n"+
				"%[1]s\t%[2]s\t\"/lustre/scratch125/humgen/projects/\"\t2\t100\n"+
				"%[1]s\t%[2]s\t\"/lustre/scratch125/humgen/projects/A/\"\t2\t100\n"+
				"%[3]s\t%[4]s\t\"/\"\t2\t80\n"+
				"%[3]s\t%[4]s\t\"/lustre/\"\t2\t80\n"+
				"%[3]s\t%[4]s\t\"/lustre/scratch123/\"\t2\t80\n"+
				"%[3]s\t%[4]s\t\"/lustre/scratch123/hgi/\"\t2\t80\n"+
				"%[3]s\t%[4]s\t\"/lustre/scratch123/hgi/m0/\"\t1\t40\n"+
				"%[3]s\t%[4]s\t\"/lustre/scratch123/hgi/mdt0/\"\t1\t40\n"+
				"%[5]s\t%[6]s\t\"/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/scratch125/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/scratch125/humgen/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/scratch125/humgen/projects/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/scratch125/humgen/projects/A/\"\t2\t2684354560\n"+
				"%[5]s\t%[6]s\t\"/lustre/scratch125/humgen/projects/A/sub/\"\t1\t2147483648\n"+
				"%[7]s\t%[8]s\t\"/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/projects/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/projects/D/\"\t5\t15\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/projects/D/sub1/\"\t3\t6\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/projects/D/sub1/temp/\"\t1\t2\n"+
				"%[7]s\t%[8]s\t\"/lustre/scratch125/humgen/projects/D/sub2/\"\t2\t9\n"+
				"%[9]s\t%[4]s\t\"/\"\t2\t50\n"+
				"%[9]s\t%[4]s\t\"/lustre/\"\t2\t50\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch123/\"\t1\t30\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch123/hgi/\"\t1\t30\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch123/hgi/mdt1/\"\t1\t30\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch123/hgi/mdt1/projects/\"\t1\t30\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch123/hgi/mdt1/projects/B/\"\t1\t30\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch125/\"\t1\t20\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch125/humgen/\"\t1\t20\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch125/humgen/projects/\"\t1\t20\n"+
				"%[9]s\t%[4]s\t\"/lustre/scratch125/humgen/projects/B/\"\t1\t20\n"+
				"%[9]s\t%[10]s\t\"/\"\t1\t60\n"+
				"%[9]s\t%[10]s\t\"/lustre/\"\t1\t60\n"+
				"%[9]s\t%[10]s\t\"/lustre/scratch125/\"\t1\t60\n"+
				"%[9]s\t%[10]s\t\"/lustre/scratch125/humgen/\"\t1\t60\n"+
				"%[9]s\t%[10]s\t\"/lustre/scratch125/humgen/teams/\"\t1\t60\n"+
				"%[9]s\t%[10]s\t\"/lustre/scratch125/humgen/teams/102/\"\t1\t60\n",
				internaluser.GetUsername(t, "103"), internaluser.GetGroupName(t, "3"),
				internaluser.GetUsername(t, "88888"), internaluser.GetGroupName(t, "2"),
				internaluser.GetUsername(t, "101"), internaluser.GetGroupName(t, "1"),
				internaluser.GetUsername(t, strconv.Itoa(int(uid))), internaluser.GetGroupName(t, strconv.Itoa(int(gid))),
				internaluser.GetUsername(t, "102"), internaluser.GetGroupName(t, "77777"))))

		p, err := clickhouse.OpenProvider(clickhouse.Config{
			DSN:           chEnv.DSN,
			Database:      chEnv.Database,
			OwnersCSVPath: ownersPath,
			MountPoints: []string{
				"/nfs/",
				"/lustre/",
			},
		})
		So(err, ShouldBeNil)

		defer p.Close()

		p.BaseDirs().SetMountPoints([]string{
			"/nfs/",
			"/lustre/",
		})

		h, err := p.BaseDirs().History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)

		fixTZs(h)

		So(h, ShouldResemble, []basedirs.History{
			{Date: yesterday.In(time.UTC), UsageSize: 15, UsageInodes: 5},
		})

		_, root = internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"lustre", 2, 1<<29, 1<<31, true, refTime.Unix())

		f, err = os.Create(inputB)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, root.AsReader())
		So(err, ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		So(os.Chtimes(inputB, refTime, refTime), ShouldBeNil)

		_, _, _, err = runWRStat("summarise",
			"-d", outputB, "-q", quotaFile, "-c", basedirsConfig, "-m", mounts, inputB)
		So(err, ShouldBeNil)

		h, err = p.BaseDirs().History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)

		fixTZs(h)

		So(h, ShouldResemble, []basedirs.History{
			{Date: yesterday.In(time.UTC), UsageSize: 15, UsageInodes: 5},
			{Date: refTime.In(time.UTC), UsageSize: 15, UsageInodes: 5},
		})
	})
}

func runWRStat(args ...string) (string, string, []*jobqueue.Job, error) {
	var (
		stdout, stderr strings.Builder
		jobs           []*jobqueue.Job
	)

	pr, pw, err := os.Pipe()
	if err != nil {
		return "", "", nil, err
	}

	cmd := exec.CommandContext(context.Background(), "./"+app, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = append(cmd.ExtraFiles, pw)

	jd := json.NewDecoder(pr)
	done := make(chan struct{})

	go func() {
		for {
			var j []*jobqueue.Job

			if errr := jd.Decode(&j); errr != nil {
				break
			}

			jobs = append(jobs, j...)
		}

		close(done)
	}()

	err = cmd.Run()

	pw.Close()

	<-done

	return stdout.String(), stderr.String(), jobs, err
}

func compareFileContents(t *testing.T, path, expectation string) {
	t.Helper()

	f, err := os.Open(path)
	So(err, ShouldBeNil)

	defer f.Close()

	var r io.Reader = f

	if strings.HasSuffix(path, ".gz") {
		r, err = gzip.NewReader(f)
		So(err, ShouldBeNil)
	}

	contents, err := io.ReadAll(r)
	So(err, ShouldBeNil)

	So(string(contents), ShouldEqual, expectation)
}

func sortLines(data string) string {
	nl := strings.HasSuffix(data, "\n")
	if nl {
		data = data[:len(data)-1]
	}

	lines := strings.Split(data, "\n")

	slices.Sort(lines)

	data = strings.Join(lines, "\n")

	if nl {
		data += "\n"
	}

	return data
}

func fixTZs(h []basedirs.History) {
	for n := range h {
		h[n].Date = h[n].Date.In(time.UTC)
	}
}

func TestDBInfo(t *testing.T) {
	Convey("dbinfo prints the correct information", t, func() {
		chEnv := setupClickHouseCLIEnv(t)

		tmpDir := t.TempDir()
		ownersPath := filepath.Join(tmpDir, "owners.csv")
		err := os.WriteFile(ownersPath, []byte("1,owner1\n"), 0600)
		So(err, ShouldBeNil)

		cfg := clickhouse.Config{DSN: chEnv.DSN, Database: chEnv.Database, OwnersCSVPath: ownersPath}

		w, err := clickhouse.NewDGUTAWriter(cfg)
		So(err, ShouldBeNil)
		w.SetMountPath("/mount/")
		w.SetUpdatedAt(time.Now().Truncate(time.Second))

		dpc := internaltest.NewDirectoryPathCreator()
		dirPath := dpc.ToDirectoryPath("/mount/")

		err = w.Add(db.RecordDGUTA{
			Dir:   dirPath,
			GUTAs: db.GUTAs{{GID: 1, UID: 1, FT: 1, Age: 0, Size: 100, Count: 1}},
		})
		So(err, ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		// Run command
		output, stderr, _, err := runWRStat("dbinfo", "--owners", ownersPath)
		So(err, ShouldBeNil)
		So(stderr, ShouldBeBlank)

		So(output, ShouldContainSubstring, "Dirs: 1")
		So(output, ShouldContainSubstring, "DGUTAs: 1")
	})
}

func TestClickHousePerfImport(t *testing.T) {
	Convey("clickhouse-perf import ingests a dataset and writes a useful JSON report", t, func() {
		chEnv := setupClickHouseCLIEnv(t)
		fixture := newClickHousePerfFixture(t)
		importJSON := filepath.Join(t.TempDir(), "clickhouse_import.json")

		stdout, stderr, err := runClickHousePerfImport(t, fixture, importJSON)
		if err != nil {
			t.Fatalf("clickhouse-perf import failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		So(stderr, ShouldBeBlank)
		So(stdout, ShouldContainSubstring, "import dataset="+fixture.datasetName)
		So(stdout, ShouldContainSubstring, "mount=/lustre/")

		report := readPerfReport(t, importJSON)
		So(report.SchemaVersion, ShouldEqual, 1)
		So(report.Backend, ShouldEqual, "clickhouse")
		So(report.GoVersion, ShouldNotBeBlank)
		So(report.OS, ShouldNotBeBlank)
		So(report.Arch, ShouldNotBeBlank)
		So(report.StartedAt, ShouldNotBeBlank)
		So(report.InputDir, ShouldEqual, fixture.statsInputDir)
		So(report.Repeat, ShouldEqual, 1)
		So(report.Warmup, ShouldEqual, 0)
		So(len(report.Operations), ShouldBeGreaterThan, 2)

		fileTotal := findReportOperation(report.Operations, "import_file_total")
		So(fileTotal, ShouldNotBeNil)
		So(fileTotal.Inputs["dataset"], ShouldEqual, fixture.datasetName)
		So(fileTotal.Inputs["mount_path"], ShouldEqual, "/lustre/")

		rowsPerTable, ok := fileTotal.Inputs["rows_per_table"].(map[string]any)
		So(ok, ShouldBeTrue)
		So(rowsPerTable["wrstat_files"], ShouldBeGreaterThan, float64(0))

		partitionReset := findReportPhaseOperation(report.Operations, clickHousePerfPhasePartitionDropReset)
		So(partitionReset, ShouldNotBeNil)
		So(partitionReset.DurationsMS[0], ShouldBeGreaterThan, 0)

		tables, ok := partitionReset.Inputs["tables"].([]any)
		So(ok, ShouldBeTrue)

		tableNames := make([]string, 0, len(tables))
		for _, raw := range tables {
			name, ok := raw.(string)
			So(ok, ShouldBeTrue)

			tableNames = append(tableNames, name)
		}

		So(tableNames, ShouldContain, "wrstat_dguta")
		So(tableNames, ShouldContain, "wrstat_children")
		So(tableNames, ShouldContain, "wrstat_files")
		So(tableNames, ShouldContain, "wrstat_basedirs_group_usage")

		dgutaInsert := findReportPhaseOperation(report.Operations, clickHousePerfPhaseDGUTAInsert)
		So(dgutaInsert, ShouldNotBeNil)
		So(dgutaInsert.Inputs["table"], ShouldEqual, "wrstat_dguta")
		So(dgutaInsert.Inputs["rows"], ShouldBeGreaterThan, float64(0))

		childrenInsert := findReportPhaseOperation(report.Operations, clickHousePerfPhaseChildrenInsert)
		So(childrenInsert, ShouldNotBeNil)
		So(childrenInsert.Inputs["table"], ShouldEqual, "wrstat_children")
		So(childrenInsert.Inputs["rows"], ShouldBeGreaterThan, float64(0))

		mountSwitch := findReportPhaseOperation(report.Operations, clickHousePerfPhaseMountSwitch)
		So(mountSwitch, ShouldNotBeNil)
		So(mountSwitch.DurationsMS[0], ShouldBeGreaterThan, 0)

		So(findReportPhaseOperation(report.Operations, clickHousePerfPhaseOldSnapshotDrop), ShouldBeNil)

		total := findReportOperation(report.Operations, "import_total")
		So(total, ShouldNotBeNil)
		So(total.Inputs["datasets"], ShouldEqual, float64(1))
		So(total.Inputs["parallelism"], ShouldEqual, float64(1))
		So(total.Inputs["mode"], ShouldEqual, "serial")
		So(total.Inputs["records"], ShouldBeGreaterThan, float64(0))

		client, err := clickhouse.NewClient(clickhouse.Config{
			DSN:           chEnv.DSN,
			Database:      chEnv.Database,
			OwnersCSVPath: fixture.ownersPath,
			MountPoints:   fixture.queryMounts,
			QueryTimeout:  5 * time.Second,
		})
		So(err, ShouldBeNil)

		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rows, err := client.ListDir(ctx, fixture.queryDir, clickhouse.ListOptions{})
		So(err, ShouldBeNil)
		So(len(rows), ShouldBeGreaterThan, 0)
		So(rows[0].ParentDir, ShouldEqual, fixture.queryDir)
	})

	Convey("clickhouse-perf import failure does not advance the active snapshot", t, func() {
		chEnv := setupClickHouseCLIEnv(t)
		fixture := newClickHousePerfFixture(t)
		goodJSON := filepath.Join(t.TempDir(), "clickhouse_import_good.json")
		badJSON := filepath.Join(t.TempDir(), "clickhouse_import_bad.json")

		stdout, stderr, err := runClickHousePerfImport(t, fixture, goodJSON)
		if err != nil {
			t.Fatalf("clickhouse-perf import failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		initialTS := activeMountTimestamp(t, chEnv, "/lustre/")
		So(initialTS, ShouldEqual, fixture.updatedAt)

		failedUpdatedAt := fixture.updatedAt.Add(time.Hour)
		corruptPerfFixtureStats(t, fixture, failedUpdatedAt)

		_, stderr, err = runClickHousePerfImport(t, fixture, badJSON)
		So(err, ShouldNotBeNil)
		So(stderr, ShouldNotBeBlank)

		finalTS := activeMountTimestamp(t, chEnv, "/lustre/")
		So(finalTS, ShouldEqual, initialTS)
		So(finalTS, ShouldNotEqual, failedUpdatedAt)
	})
}

func newClickHousePerfFixture(t *testing.T) clickHousePerfFixture {
	t.Helper()

	gid, uid, _, _, err := internaluser.RealGIDAndUID()
	So(err, ShouldBeNil)

	refTime := time.Now().Truncate(time.Second)
	paths, root := internaldata.FakeFilesForDGUTADBForBasedirsTesting(
		gid,
		uid,
		"lustre",
		2,
		1<<20,
		1<<19,
		true,
		refTime.Unix(),
	)

	statsInputDir := t.TempDir()
	datasetDir := filepath.Join(statsInputDir, "1_／lustre")
	So(os.MkdirAll(datasetDir, 0o755), ShouldBeNil)

	statsGZPath := filepath.Join(datasetDir, "stats.gz")
	fh, err := os.Create(statsGZPath)
	So(err, ShouldBeNil)

	gz := gzip.NewWriter(fh)
	_, err = io.Copy(gz, root.AsReader())
	So(err, ShouldBeNil)
	So(gz.Close(), ShouldBeNil)
	So(fh.Close(), ShouldBeNil)
	So(os.Chtimes(statsGZPath, refTime, refTime), ShouldBeNil)

	metaDir := t.TempDir()
	quotaFile := filepath.Join(metaDir, "quota.csv")
	basedirsConfig := filepath.Join(metaDir, "basedirs.config")
	mountsFile := filepath.Join(metaDir, "mounts.txt")

	So(os.WriteFile(quotaFile, []byte(""), 0o600), ShouldBeNil)
	So(os.WriteFile(basedirsConfig, []byte("\t1\t1\n"), 0o600), ShouldBeNil)
	So(os.WriteFile(mountsFile, []byte("\"/lustre\"\n\"/\"\n"), 0o600), ShouldBeNil)

	ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
	So(err, ShouldBeNil)

	return clickHousePerfFixture{
		statsInputDir:   statsInputDir,
		statsGZPath:     statsGZPath,
		datasetName:     filepath.Base(datasetDir),
		ownersPath:      ownersPath,
		quotaFile:       quotaFile,
		basedirsConfig:  basedirsConfig,
		mountsFile:      mountsFile,
		updatedAt:       refTime,
		queryDir:        paths[0] + "/",
		queryMounts:     []string{"/lustre/", "/"},
		queryReportUID:  "101",
		queryReportGIDs: "1,3",
	}
}

func runClickHousePerfImport(
	t *testing.T,
	fixture clickHousePerfFixture,
	jsonPath string,
) (string, string, error) {
	t.Helper()

	stdout, stderr, err := runWRStatWithTimeout(
		t,
		45*time.Second,
		"clickhouse-perf",
		"import",
		fixture.statsInputDir,
		"--owners",
		fixture.ownersPath,
		"--mounts",
		fixture.mountsFile,
		"--quota",
		fixture.quotaFile,
		"--config",
		fixture.basedirsConfig,
		"--batchSize",
		"512",
		"--parallelism",
		"1",
		"--query-timeout",
		"5s",
		"--json",
		jsonPath,
	)

	return stdout, stderr, err
}

func runWRStatWithTimeout(
	t *testing.T,
	timeout time.Duration,
	args ...string,
) (string, string, error) {
	t.Helper()

	var (
		stdout, stderr strings.Builder
		jobs           []*jobqueue.Job
	)

	pr, pw, err := os.Pipe()
	if err != nil {
		return "", "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "./"+app, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = append(cmd.ExtraFiles, pw)

	jd := json.NewDecoder(pr)
	done := make(chan struct{})

	go func() {
		for {
			var j []*jobqueue.Job

			if errr := jd.Decode(&j); errr != nil {
				break
			}

			jobs = append(jobs, j...)
		}

		close(done)
	}()

	err = cmd.Run()

	pw.Close()

	<-done

	return stdout.String(), stderr.String(), err
}

func readPerfReport(t *testing.T, path string) boltperf.Report {
	t.Helper()

	data, err := os.ReadFile(path)
	So(err, ShouldBeNil)

	var report boltperf.Report

	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	So(dec.Decode(&report), ShouldBeNil)

	return report
}

func findReportOperation(ops []boltperf.Operation, name string) *boltperf.Operation {
	for i := range ops {
		if ops[i].Name == name {
			return &ops[i]
		}
	}

	return nil
}

func findReportPhaseOperation(ops []boltperf.Operation, phase string) *boltperf.Operation {
	for i := range ops {
		if ops[i].Name != "import_phase" {
			continue
		}

		if ops[i].Inputs["phase"] == phase {
			return &ops[i]
		}
	}

	return nil
}

func corruptPerfFixtureStats(t *testing.T, fixture clickHousePerfFixture, updatedAt time.Time) {
	t.Helper()

	raw := readGzipFile(t, fixture.statsGZPath)
	raw = append(raw, []byte("\"/broken\"\t1\n")...)

	writeGzipFile(t, fixture.statsGZPath, raw)
	So(os.Chtimes(fixture.statsGZPath, updatedAt, updatedAt), ShouldBeNil)
}

func readGzipFile(t *testing.T, path string) []byte {
	t.Helper()

	fh, err := os.Open(path)
	So(err, ShouldBeNil)

	defer func() { So(fh.Close(), ShouldBeNil) }()

	gz, err := gzip.NewReader(fh)
	So(err, ShouldBeNil)

	defer func() { So(gz.Close(), ShouldBeNil) }()

	data, err := io.ReadAll(gz)
	So(err, ShouldBeNil)

	return data
}

func writeGzipFile(t *testing.T, path string, data []byte) {
	t.Helper()

	fh, err := os.Create(path)
	So(err, ShouldBeNil)

	gz := gzip.NewWriter(fh)
	_, err = gz.Write(data)
	So(err, ShouldBeNil)
	So(gz.Close(), ShouldBeNil)
	So(fh.Close(), ShouldBeNil)
}

func activeMountTimestamp(t *testing.T, chEnv clickHouseCLIEnv, mountPath string) time.Time {
	t.Helper()

	p, err := clickhouse.OpenProvider(clickhouse.Config{
		DSN:          chEnv.DSN,
		Database:     chEnv.Database,
		QueryTimeout: 5 * time.Second,
	})
	So(err, ShouldBeNil)

	defer func() { So(p.Close(), ShouldBeNil) }()

	timestamps, err := p.BaseDirs().MountTimestamps()
	So(err, ShouldBeNil)

	ts, ok := timestamps[strings.ReplaceAll(mountPath, "/", "／")]
	So(ok, ShouldBeTrue)

	return ts
}

func TestClickHousePerfQuery(t *testing.T) {
	Convey("clickhouse-perf query reports timed operations and metrics against imported data", t, func() {
		chEnv := setupClickHouseCLIEnv(t)
		fixture := newClickHousePerfFixture(t)
		importJSON := filepath.Join(t.TempDir(), "clickhouse_import.json")
		queryJSON := filepath.Join(t.TempDir(), "clickhouse_query.json")

		stdout, stderr, err := runClickHousePerfImport(t, fixture, importJSON)
		if err != nil {
			t.Fatalf("clickhouse-perf import failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		queryDir := findClickHousePerfQueryableDir(t, chEnv, fixture)

		stdout, stderr, err = runClickHousePerfQuery(t, 45*time.Second, fixture, queryDir, queryJSON)
		if err != nil {
			t.Fatalf("clickhouse-perf query failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		So(stderr, ShouldBeBlank)
		So(stdout, ShouldContainSubstring, "query: using dir="+queryDir)
		So(stdout, ShouldContainSubstring, "ExplainListDir:")
		So(stdout, ShouldContainSubstring, "ExplainStatPath:")
		So(stdout, ShouldContainSubstring, "files_listdir metrics:")
		So(stdout, ShouldContainSubstring, "glob_case_A repeats=2")

		report := readPerfReport(t, queryJSON)
		So(report.SchemaVersion, ShouldEqual, 1)
		So(report.Backend, ShouldEqual, "clickhouse")
		So(report.GoVersion, ShouldNotBeBlank)
		So(report.OS, ShouldNotBeBlank)
		So(report.Arch, ShouldNotBeBlank)
		So(report.StartedAt, ShouldNotBeBlank)
		So(report.InputDir, ShouldEqual, "")
		So(report.Repeat, ShouldEqual, 2)
		So(report.Warmup, ShouldEqual, 0)
		So(len(report.Operations), ShouldBeGreaterThanOrEqualTo, 10)

		opNames := make([]string, 0, len(report.Operations))
		for _, op := range report.Operations {
			opNames = append(opNames, op.Name)
			So(len(op.DurationsMS), ShouldEqual, 2)
		}

		So(opNames, ShouldContain, "mount_timestamps")
		So(opNames, ShouldContain, "tree_dirinfo")
		So(opNames, ShouldContain, "basedirs_group_usage")
		So(opNames, ShouldContain, "files_listdir")
		So(opNames, ShouldContain, "permission_check")
		So(opNames, ShouldContain, "glob_case_A")

		treeDirInfo := findReportOperation(report.Operations, "tree_dirinfo")
		So(treeDirInfo, ShouldNotBeNil)
		So(treeDirInfo.Inputs["dir"], ShouldNotBeBlank)

		filesListDir := findReportOperation(report.Operations, "files_listdir")
		So(filesListDir, ShouldNotBeNil)
		So(filesListDir.Inputs["dir"], ShouldEqual, treeDirInfo.Inputs["dir"])
	})
}

func runClickHousePerfQuery(
	t *testing.T,
	timeout time.Duration,
	fixture clickHousePerfFixture,
	queryDir string,
	jsonPath string,
) (string, string, error) {
	t.Helper()

	stdout, stderr, err := runWRStatWithTimeout(
		t,
		timeout,
		"clickhouse-perf",
		"query",
		"--owners",
		fixture.ownersPath,
		"--mounts",
		fixture.mountsFile,
		"--dir",
		queryDir,
		"--uid",
		fixture.queryReportUID,
		"--gids",
		fixture.queryReportGIDs,
		"--repeat",
		"2",
		"--query-timeout",
		"5s",
		"--json",
		jsonPath,
	)

	return stdout, stderr, err
}

func findClickHousePerfQueryableDir(
	t *testing.T,
	chEnv clickHouseCLIEnv,
	fixture clickHousePerfFixture,
) string {
	t.Helper()

	cfg := clickhouse.Config{
		DSN:           chEnv.DSN,
		Database:      chEnv.Database,
		OwnersCSVPath: fixture.ownersPath,
		MountPoints:   fixture.queryMounts,
		QueryTimeout:  5 * time.Second,
	}

	p, err := clickhouse.OpenProvider(cfg)
	So(err, ShouldBeNil)

	defer p.Close()

	client, err := clickhouse.NewClient(cfg)
	So(err, ShouldBeNil)

	defer client.Close()

	filter := &db.Filter{Age: db.DGUTAgeAll}
	candidates := []string{fixture.queryDir}
	seen := make(map[string]struct{})

	for len(candidates) > 0 {
		dir := candidates[0]
		candidates = candidates[1:]

		if _, ok := seen[dir]; ok {
			continue
		}

		seen[dir] = struct{}{}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rows, err := client.ListDir(ctx, dir, clickhouse.ListOptions{})

		cancel()

		if err != nil || len(rows) == 0 {
			if parent := clickHousePerfParentDir(dir); parent != "" {
				candidates = append(candidates, parent)
			}

			continue
		}

		if _, err := p.Tree().DirInfo(dir, filter); err == nil {
			return dir
		}

		for _, row := range rows {
			if row.EntryType == 'd' {
				candidates = append(candidates, row.Path)
			}
		}

		if parent := clickHousePerfParentDir(dir); parent != "" {
			candidates = append(candidates, parent)
		}
	}

	t.Fatalf("failed to find a clickhouse-perf query dir that works for both tree and file APIs")

	return ""
}

func clickHousePerfParentDir(dir string) string {
	trimmed := strings.TrimSuffix(dir, "/")
	if trimmed == "" || trimmed == "/" {
		return ""
	}

	pos := strings.LastIndexByte(trimmed, '/')
	if pos <= 0 {
		return "/"
	}

	return trimmed[:pos+1]
}

type summariseOrderedCloser struct {
	name  string
	calls *[]string
	err   error
}

func (c summariseOrderedCloser) Close() error {
	if c.calls != nil {
		*c.calls = append(*c.calls, c.name)
	}

	return c.err
}

func TestCloseSummariseDGUTAWriter(t *testing.T) {
	Convey("closeSummariseDGUTAWriter publishes with Close", t, func() {
		calls := make([]string, 0, 1)

		err := closeSummariseDGUTAWriter(summariseAbortTrackingCloser{
			closeName: "close",
			abortName: "abort",
			calls:     &calls,
		}, true)

		So(err, ShouldBeNil)
		So(calls, ShouldResemble, []string{"close"})
	})

	Convey("closeSummariseDGUTAWriter aborts when not publishing and Abort is available", t, func() {
		calls := make([]string, 0, 1)

		err := closeSummariseDGUTAWriter(summariseAbortTrackingCloser{
			closeName: "close",
			abortName: "abort",
			calls:     &calls,
		}, false)

		So(err, ShouldBeNil)
		So(calls, ShouldResemble, []string{"abort"})
	})

	Convey("closeSummariseDGUTAWriter falls back to Close when Abort is unavailable", t, func() {
		calls := make([]string, 0, 1)

		err := closeSummariseDGUTAWriter(summariseOrderedCloser{
			name:  "close",
			calls: &calls,
		}, false)

		So(err, ShouldBeNil)
		So(calls, ShouldResemble, []string{"close"})
	})

	Convey("closeSummariseDGUTAWriter returns the writer error", t, func() {
		err := closeSummariseDGUTAWriter(summariseAbortTrackingCloser{
			abortErr: errSummariseTestDGUTA,
		}, false)

		So(err, ShouldEqual, errSummariseTestDGUTA)
	})
}

//go:linkname closeSummariseDGUTAWriter github.com/wtsi-hgi/wrstat-ui/cmd.closeSummariseDGUTAWriter
func closeSummariseDGUTAWriter(writer io.Closer, publish bool) error

func TestComposeSummariseCloser(t *testing.T) {
	Convey("composeSummariseCloser closes resources in summarise order on success", t, func() {
		calls := make([]string, 0, 3)

		closer := composeSummariseCloser(
			summariseOrderedCloser{name: "files", calls: &calls},
			func() error {
				calls = append(calls, "basedirs")

				return nil
			},
			summariseAbortTrackingCloser{
				closeName: "dguta-close",
				abortName: "dguta-abort",
				calls:     &calls,
			},
		)

		So(closer(true), ShouldBeNil)
		So(calls, ShouldResemble, []string{"files", "basedirs", "dguta-close"})
	})

	Convey("composeSummariseCloser aborts dguta publishing when not publishing", t, func() {
		calls := make([]string, 0, 3)

		closer := composeSummariseCloser(
			summariseOrderedCloser{name: "files", calls: &calls},
			func() error {
				calls = append(calls, "basedirs")

				return nil
			},
			summariseAbortTrackingCloser{
				closeName: "dguta-close",
				abortName: "dguta-abort",
				calls:     &calls,
			},
		)

		So(closer(false), ShouldBeNil)
		So(calls, ShouldResemble, []string{"files", "basedirs", "dguta-abort"})
	})

	Convey("composeSummariseCloser aborts dguta publishing when file close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeSummariseCloser(
			summariseOrderedCloser{name: "files", calls: &calls, err: errSummariseTestFiles},
			func() error {
				calls = append(calls, "basedirs")

				return nil
			},
			summariseAbortTrackingCloser{
				closeName: "dguta-close",
				abortName: "dguta-abort",
				calls:     &calls,
			},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errSummariseTestFiles.Error())
		So(calls, ShouldResemble, []string{"files", "basedirs", "dguta-abort"})
	})

	Convey("composeSummariseCloser aborts dguta publishing when basedirs close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeSummariseCloser(
			summariseOrderedCloser{name: "files", calls: &calls},
			func() error {
				calls = append(calls, "basedirs")

				return errSummariseTestBasedirs
			},
			summariseAbortTrackingCloser{
				closeName: "dguta-close",
				abortName: "dguta-abort",
				calls:     &calls,
			},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errSummariseTestBasedirs.Error())
		So(calls, ShouldResemble, []string{"files", "basedirs", "dguta-abort"})
	})

	Convey("composeSummariseCloser joins cleanup errors", t, func() {
		err := composeSummariseCloser(
			summariseOrderedCloser{err: errSummariseTestFiles},
			func() error { return errSummariseTestBasedirs },
			summariseAbortTrackingCloser{abortErr: errSummariseTestDGUTA},
		)(false)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errSummariseTestFiles.Error())
		So(err.Error(), ShouldContainSubstring, errSummariseTestBasedirs.Error())
		So(err.Error(), ShouldContainSubstring, errSummariseTestDGUTA.Error())
	})
}

//go:linkname composeSummariseCloser github.com/wtsi-hgi/wrstat-ui/cmd.composeSummariseCloser
func composeSummariseCloser(
	fileCloser io.Closer,
	basedirsCloser func() error,
	dgutaCloser io.Closer,
) func(bool) error

type summariseAbortTrackingCloser struct {
	closeName string
	abortName string
	calls     *[]string
	closeErr  error
	abortErr  error
}

func (c summariseAbortTrackingCloser) Close() error {
	if c.calls != nil && c.closeName != "" {
		*c.calls = append(*c.calls, c.closeName)
	}

	return c.closeErr
}

func (c summariseAbortTrackingCloser) Abort() error {
	if c.calls != nil && c.abortName != "" {
		*c.calls = append(*c.calls, c.abortName)
	}

	return c.abortErr
}

type clickHousePerfFixture struct {
	statsInputDir   string
	statsGZPath     string
	datasetName     string
	ownersPath      string
	quotaFile       string
	basedirsConfig  string
	mountsFile      string
	updatedAt       time.Time
	queryDir        string
	queryMounts     []string
	queryReportUID  string
	queryReportGIDs string
}

func TestMain(m *testing.M) {
	d1 := buildSelf()
	if d1 == nil {
		return
	}

	defer os.Exit(m.Run())
	defer d1()
}

func buildSelf() func() {
	cmd := exec.CommandContext(
		context.Background(),
		"go", "build", "-tags", "netgo",
		"-ldflags=-X github.com/VertebrateResequencing/wr/client.PretendSubmissions=3 "+
			"-X github.com/wtsi-hgi/wrstat-ui/cmd.Version=TESTVERSION",
		"-o", app,
	)

	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() {
		os.Remove(app)
	}
}

func failMainTest(err string) {
	fmt.Println(err) //nolint:forbidigo
}

func TestVersion(t *testing.T) {
	Convey("wrstat-ui prints the correct version", t, func() {
		output, stderr, _, err := runWRStat("version")
		So(err, ShouldBeNil)
		So(strings.TrimSpace(output), ShouldEqual, "TESTVERSION")
		So(stderr, ShouldBeBlank)
	})
}

func TestBoltPerf(t *testing.T) {
	Convey("bolt-perf can import and query and write a schema v1 report", t, func() {
		gid, uid, _, _, err := internaluser.RealGIDAndUID()
		So(err, ShouldBeNil)

		refTime := time.Now().Truncate(time.Second)
		_, root := internaldata.FakeFilesForDGUTADBForBasedirsTesting(
			gid,
			uid,
			"lustre",
			1,
			1<<20,
			1<<20,
			true,
			refTime.Unix(),
		)

		statsInputDir := t.TempDir()
		datasetDir := filepath.Join(statsInputDir, "1_／lustre")
		So(os.MkdirAll(datasetDir, 0755), ShouldBeNil)

		statsGZPath := filepath.Join(datasetDir, "stats.gz")
		fh, err := os.Create(statsGZPath)
		So(err, ShouldBeNil)

		gz := gzip.NewWriter(fh)
		_, err = io.Copy(gz, root.AsReader())
		So(err, ShouldBeNil)
		So(gz.Close(), ShouldBeNil)
		So(fh.Close(), ShouldBeNil)

		So(os.Chtimes(statsGZPath, refTime, refTime), ShouldBeNil)

		metaDir := t.TempDir()
		quotaFile := filepath.Join(metaDir, "quota.csv")
		basedirsConfig := filepath.Join(metaDir, "basedirs.config")
		mountsFile := filepath.Join(metaDir, "mounts.txt")

		So(os.WriteFile(quotaFile, []byte(""), 0600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("\t1\t1\n"), 0600), ShouldBeNil)
		So(os.WriteFile(mountsFile, []byte("\"/lustre\"\n\"/\"\n"), 0600), ShouldBeNil)

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		boltOutDir := t.TempDir()
		importJSON := filepath.Join(metaDir, "bolt_import.json")
		queryJSON := filepath.Join(metaDir, "bolt_query.json")

		stdout, stderr, _, err := runWRStat(
			"bolt-perf",
			"import",
			statsInputDir,
			"--out",
			boltOutDir,
			"--quota",
			quotaFile,
			"--config",
			basedirsConfig,
			"--mounts",
			mountsFile,
			"--owners",
			ownersPath,
			"--json",
			importJSON,
		)
		if err != nil {
			t.Fatalf("bolt-perf import failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		_, _, _, err = runWRStat(
			"bolt-perf",
			"query",
			boltOutDir,
			"--mounts",
			mountsFile,
			"--owners",
			ownersPath,
			"--dir",
			"/lustre/",
			"--repeat",
			"2",
			"--warmup",
			"0",
			"--splits",
			"2",
			"--json",
			queryJSON,
		)
		So(err, ShouldBeNil)

		data, err := os.ReadFile(queryJSON)
		So(err, ShouldBeNil)

		var report struct {
			SchemaVersion int    `json:"schema_version"`
			Backend       string `json:"backend"`
			GitCommit     string `json:"git_commit"`
			GoVersion     string `json:"go_version"`
			OS            string `json:"os"`
			Arch          string `json:"arch"`
			StartedAt     string `json:"started_at"`
			InputDir      string `json:"input_dir"`
			Repeat        int    `json:"repeat"`
			Warmup        int    `json:"warmup"`
			Operations    []struct {
				Name        string                 `json:"name"`
				Inputs      map[string]any         `json:"inputs"`
				DurationsMS []float64              `json:"durations_ms"`
				P50MS       float64                `json:"p50_ms"`
				P95MS       float64                `json:"p95_ms"`
				P99MS       float64                `json:"p99_ms"`
				Extra       map[string]interface{} `json:"-"`
			} `json:"operations"`
		}

		dec := json.NewDecoder(strings.NewReader(string(data)))
		dec.DisallowUnknownFields()
		So(dec.Decode(&report), ShouldBeNil)

		So(report.SchemaVersion, ShouldEqual, 1)
		So(report.Backend, ShouldEqual, "bolt")
		So(report.GoVersion, ShouldNotBeBlank)
		So(report.OS, ShouldNotBeBlank)
		So(report.Arch, ShouldNotBeBlank)
		So(report.StartedAt, ShouldNotBeBlank)
		So(report.InputDir, ShouldEqual, boltOutDir)
		So(report.Repeat, ShouldEqual, 2)
		So(report.Warmup, ShouldEqual, 0)

		opNames := make([]string, 0, len(report.Operations))
		for _, op := range report.Operations {
			opNames = append(opNames, op.Name)
			So(len(op.DurationsMS), ShouldEqual, 2)
		}

		So(opNames, ShouldResemble, []string{
			"mount_timestamps",
			"tree_dirinfo",
			"tree_where",
			"basedirs_group_usage",
			"basedirs_user_usage",
			"basedirs_group_subdirs",
			"basedirs_user_subdirs",
			"basedirs_history",
		})
		So(report.GitCommit, ShouldNotBeNil)

		Convey("bolt-perf can run in bolt_interfaces mode", func() {
			boltOutDir2 := t.TempDir()
			importJSON2 := filepath.Join(metaDir, "bolt_import_interfaces.json")
			queryJSON2 := filepath.Join(metaDir, "bolt_query_interfaces.json")

			stdout, stderr, _, err := runWRStat(
				"bolt-perf",
				"import",
				statsInputDir,
				"--backend",
				"bolt_interfaces",
				"--out",
				boltOutDir2,
				"--quota",
				quotaFile,
				"--config",
				basedirsConfig,
				"--mounts",
				mountsFile,
				"--owners",
				ownersPath,
				"--json",
				importJSON2,
			)
			if err != nil {
				t.Fatalf("bolt-perf import (bolt_interfaces) failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
			}

			_, _, _, err = runWRStat(
				"bolt-perf",
				"query",
				boltOutDir2,
				"--backend",
				"bolt_interfaces",
				"--mounts",
				mountsFile,
				"--owners",
				ownersPath,
				"--dir",
				"/lustre/",
				"--repeat",
				"2",
				"--warmup",
				"0",
				"--splits",
				"2",
				"--json",
				queryJSON2,
			)
			So(err, ShouldBeNil)

			data, err := os.ReadFile(queryJSON2)
			So(err, ShouldBeNil)

			var report2 struct {
				SchemaVersion int    `json:"schema_version"`
				Backend       string `json:"backend"`
				GitCommit     string `json:"git_commit"`
				GoVersion     string `json:"go_version"`
				OS            string `json:"os"`
				Arch          string `json:"arch"`
				StartedAt     string `json:"started_at"`
				InputDir      string `json:"input_dir"`
				Repeat        int    `json:"repeat"`
				Warmup        int    `json:"warmup"`
				Operations    []struct {
					Name        string                 `json:"name"`
					Inputs      map[string]any         `json:"inputs"`
					DurationsMS []float64              `json:"durations_ms"`
					P50MS       float64                `json:"p50_ms"`
					P95MS       float64                `json:"p95_ms"`
					P99MS       float64                `json:"p99_ms"`
					Extra       map[string]interface{} `json:"-"`
				} `json:"operations"`
			}

			dec := json.NewDecoder(strings.NewReader(string(data)))
			dec.DisallowUnknownFields()
			So(dec.Decode(&report2), ShouldBeNil)

			So(report2.SchemaVersion, ShouldEqual, 1)
			So(report2.Backend, ShouldEqual, "bolt_interfaces")
			So(report2.GoVersion, ShouldNotBeBlank)
			So(report2.OS, ShouldNotBeBlank)
			So(report2.Arch, ShouldNotBeBlank)
			So(report2.StartedAt, ShouldNotBeBlank)
			So(report2.InputDir, ShouldEqual, boltOutDir2)
			So(report2.Repeat, ShouldEqual, 2)
			So(report2.Warmup, ShouldEqual, 0)

			opNames2 := make([]string, 0, len(report2.Operations))
			for _, op := range report2.Operations {
				opNames2 = append(opNames2, op.Name)
				So(len(op.DurationsMS), ShouldEqual, 2)
			}

			So(opNames2, ShouldResemble, []string{
				"mount_timestamps",
				"tree_dirinfo",
				"tree_where",
				"basedirs_group_usage",
				"basedirs_user_usage",
				"basedirs_group_subdirs",
				"basedirs_user_subdirs",
				"basedirs_history",
			})
			So(report2.GitCommit, ShouldNotBeNil)
		})
	})
}

func TestWatch(t *testing.T) {
	Convey("watch starts the correct jobs", t, func() {
		tmp := t.TempDir()
		output := t.TempDir()

		runA := filepath.Join(tmp, "12345_A")
		dotA := filepath.Join(output, ".12345_A")
		finalA := filepath.Join(output, "12345_A")
		statsA := filepath.Join(runA, "stats.gz")

		const (
			cpus = 2
			ram  = 8192
		)

		cwd, err := os.Getwd()
		So(err, ShouldBeNil)

		So(os.Mkdir(runA, 0755), ShouldBeNil)
		So(os.WriteFile(statsA, nil, 0600), ShouldBeNil)

		_, _, jobs, err := runWRStat("watch", "-o", output, "-q", "/some/quota.file", "-c", "basedirs.config", tmp)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldBeGreaterThan, 0)
		So(jobs[0].RepGroup, ShouldStartWith, "wrstat-ui-summarise-")
		So(jobs, ShouldResemble, []*jobqueue.Job{
			{
				Cmd: fmt.Sprintf(`"./wrstat-ui_test" summarise -d %[1]q -q `+
					`"/some/quota.file" -c "basedirs.config" %[2]q && touch -r %[3]q %[1]q && mv %[1]q %[4]q`,
					dotA, statsA, runA, finalA,
				),
				Cwd:        cwd,
				CwdMatters: true,
				ReqGroup:   "wrstat-ui-summarise",
				RepGroup:   jobs[0].RepGroup,
				Requirements: &scheduler.Requirements{
					Cores: cpus,
					RAM:   ram,
					Time:  10 * time.Second,
					Disk:  1,
				},
				Override: 1,
				Retries:  30,
				State:    jobqueue.JobStateDelayed,
			},
		})

		So(os.Remove(dotA), ShouldBeNil)

		previous := filepath.Join(output, "12344_A")
		previousBasedirs := filepath.Join(previous, "basedirs.db")

		So(os.Mkdir(previous, 0700), ShouldBeNil)
		So(os.WriteFile(previousBasedirs, nil, 0600), ShouldBeNil)

		_, _, jobs, err = runWRStat("watch", "-o", output, "-q", "/some/quota.file", "-c", "basedirs.config", tmp)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldBeGreaterThan, 0)
		So(jobs[0].RepGroup, ShouldStartWith, "wrstat-ui-summarise-")
		So(jobs, ShouldResemble, []*jobqueue.Job{
			{
				Cmd: fmt.Sprintf(`"./wrstat-ui_test" summarise -d %[1]q `+
					`-s %[2]q -q "/some/quota.file" -c "basedirs.config" %[3]q && touch -r %[4]q %[1]q && mv %[1]q %[5]q`,
					dotA, previousBasedirs, statsA, runA, finalA,
				),
				Cwd:        cwd,
				CwdMatters: true,
				ReqGroup:   "wrstat-ui-summarise",
				RepGroup:   jobs[0].RepGroup,
				Requirements: &scheduler.Requirements{
					Cores: cpus,
					RAM:   ram,
					Time:  10 * time.Second,
					Disk:  1,
				},
				Override: 1,
				Retries:  30,
				State:    jobqueue.JobStateDelayed,
			},
		})
	})
}

func TestDupes(t *testing.T) {
	Convey("dupes correctly matches same-sized files", t, func() {
		tmp := t.TempDir()
		statsA := filepath.Join(tmp, "statsA")
		statsB := filepath.Join(tmp, "statsB.gz")

		fa := statsdata.NewRoot("/mount/A/", 0)
		statsdata.AddFileWithInode(fa, "someDir/not_displayed", 0, 0, 1, 0, 0, 0, 6)

		statsdata.AddFileWithInode(fa, "someDir/not_displayed_2", 0, 0, 1, 0, 0, 1, 2)
		statsdata.AddFileWithInode(fa, "someDir/not_displayed_3", 0, 0, 1, 0, 0, 1, 2)

		statsdata.AddFileWithInode(fa, "someDir/big_files/1", 0, 0, 100, 0, 0, 0, 6)
		statsdata.AddFileWithInode(fa, "someDir/big_files/2", 0, 0, 101, 0, 0, 0, 6)

		statsdata.AddFileWithInode(fa, "someDir/big_files/3", 0, 0, 101, 0, 0, 2, 3)
		statsdata.AddFileWithInode(fa, "someDir/big_files/4", 0, 0, 101, 0, 0, 2, 3)

		statsdata.AddFileWithInode(fa, "someDir/big_files/5", 0, 0, 102, 0, 0, 0, 6)

		fb := statsdata.NewRoot("/mount/B/", 0)
		statsdata.AddFileWithInode(fb, "anotherDir/big_files/1", 0, 0, 100, 0, 0, 0, 6)

		statsdata.AddFileWithInode(fb, "anotherDir/big_files/2", 0, 0, 101, 0, 0, 2, 3)

		statsdata.AddFileWithInode(fb, "anotherDir/big_files/3", 0, 0, 103, 0, 0, 0, 6)

		f, err := os.Create(statsA)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, fa.AsReader())

		So(err, ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		f, err = os.Create(statsB)
		So(err, ShouldBeNil)

		gf := gzip.NewWriter(f)

		_, err = io.Copy(gf, fb.AsReader())
		So(err, ShouldBeNil)
		So(gf.Close(), ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		stdout, _, _, err := runWRStat("dupes", "-m", "10", statsA, statsB)
		So(err, ShouldBeNil)
		So(stdout, ShouldEqual, ``+
			`Size: 100
"/mount/A/someDir/big_files/1"
"/mount/B/anotherDir/big_files/1"
Size: 101
"/mount/A/someDir/big_files/2"
"/mount/A/someDir/big_files/3"
	"/mount/A/someDir/big_files/4"
"/mount/B/anotherDir/big_files/2"
`)
	})
}

func waitForTCPPort(t *testing.T, host string, port int) {
	t.Helper()

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	deadline := time.Now().Add(30 * time.Second)

	for {
		dialer := net.Dialer{Timeout: 200 * time.Millisecond}

		conn, err := dialer.DialContext(
			context.Background(), "tcp", addr,
		)
		if err == nil {
			_ = conn.Close()

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("clickhouse server did not become ready at %s: %v", addr, err)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func refuseNonLocalhostDSN(t *testing.T, dsn string) {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("invalid DSN: %v", err)
	}

	host := u.Hostname()
	if host == "" {
		t.Fatalf("invalid DSN host")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		t.Fatalf("failed to resolve DSN host %q: %v", host, err)
	}

	for _, ip := range ips {
		if !ip.IP.IsLoopback() {
			t.Fatalf("refusing non-localhost DSN host %q (%v)", host, ip.IP)
		}
	}
}

func withDatabaseInDSN(t *testing.T, dsn string, database string) string {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("invalid DSN: %v", err)
	}

	q := u.Query()
	q.Set("database", database)
	u.RawQuery = q.Encode()

	return u.String()
}

func findClickHouseBinary(t *testing.T) string {
	t.Helper()

	bin, err := exec.LookPath("clickhouse")
	if err == nil {
		return bin
	}

	fallback := "/software/hgi/installs/clickhouse/clickhouse"
	if _, statErr := os.Stat(fallback); statErr == nil {
		return fallback
	}

	t.Skip("clickhouse binary not found")

	return ""
}

func pickFreePort(t *testing.T) int {
	t.Helper()

	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to pick free port: %v", err)
	}

	defer func() { _ = l.Close() }()

	addr := l.Addr().String()

	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("failed to parse listener addr %q: %v", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse listener port %q: %v", portStr, err)
	}

	return port
}

func startClickHouseServer(
	t *testing.T,
	binPath string,
	baseDir string,
	tcpPort int,
	httpPort int,
) {
	t.Helper()

	dataPath := filepath.Join(baseDir, "data")
	stdoutPath := filepath.Join(baseDir, "clickhouse.stdout.log")
	stderrPath := filepath.Join(baseDir, "clickhouse.stderr.log")

	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("failed to create clickhouse data dir: %v", err)
	}

	crtPath, keyPath := writeSelfSignedTLSCertPair(t, baseDir)

	args := []string{
		"server",
		"--",
		"--listen_host=127.0.0.1",
		"--tcp_port=" + strconv.Itoa(tcpPort),
		"--tcp_port_secure=0",
		"--http_port=" + strconv.Itoa(httpPort),
		"--https_port=0",
		"--mysql_port=0",
		"--postgresql_port=0",
		"--grpc_port=0",
		"--openSSL.server.certificateFile=" + crtPath,
		"--openSSL.server.privateKeyFile=" + keyPath,
		"--path=" + dataPath + string(os.PathSeparator),
	}

	cmdCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, binPath, args...)
	cmd.Dir = baseDir

	cmd.Env = append(os.Environ(), "CLICKHOUSE_WATCHDOG_ENABLE=0")

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		cancel()
		t.Fatalf("failed to create clickhouse stdout log: %v", err)
	}

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		_ = stdoutFile.Close()

		cancel()
		t.Fatalf("failed to create clickhouse stderr log: %v", err)
	}

	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()

		cancel()
		t.Fatalf("failed to start clickhouse: %v", err)
	}

	doneCh := make(chan struct{})

	go func() {
		_ = cmd.Wait() //nolint:errcheck

		close(doneCh)

		_ = stdoutFile.Close()
		_ = stderrFile.Close()
	}()

	t.Cleanup(func() {
		defer cancel()

		if cmd.Process == nil {
			return
		}

		_ = cmd.Process.Signal(os.Interrupt) //nolint:errcheck

		select {
		case <-doneCh:
			return
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill() //nolint:errcheck

			<-doneCh
		}
	})
}

func writeSelfSignedTLSCertPair(t *testing.T, dir string) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate TLS key: %v", err)
	}

	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create TLS certificate: %v", err)
	}

	crtPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	if err := os.WriteFile(crtPath, crtPEM, 0o600); err != nil {
		t.Fatalf("failed to write TLS certificate: %v", err)
	}

	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("failed to write TLS private key: %v", err)
	}

	return crtPath, keyPath
}

func newTestDatabaseName(t *testing.T) string {
	t.Helper()

	rnd := make([]byte, 6)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatalf("failed to generate random suffix: %v", err)
	}

	randHex := hex.EncodeToString(rnd)

	// Keep it short; ClickHouse DB names are identifiers.
	base := fmt.Sprintf("wrstat_ui_test_%d_%s", os.Getpid(), randHex)

	base = strings.ReplaceAll(base, "-", "_")
	base = strings.ReplaceAll(base, ".", "_")
	base = strings.ReplaceAll(base, "@", "_")

	return base
}
