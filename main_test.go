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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
)

const app = "wrstat-ui_test"

func TestMain(m *testing.M) {
	d1 := buildSelf()
	if d1 == nil {
		return
	}

	defer os.Exit(m.Run())
	defer d1()
}

func buildSelf() func() {
	cmd := exec.Command(
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

func TestSummarise(t *testing.T) {
	Convey("summarise produces the correct output", t, func() {
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
		outputA := filepath.Join(outputDir, "A")
		outputB := filepath.Join(outputDir, "B")

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

		_, _, _, err = runWRStat("summarise", "-d", outputA, "-q", quotaFile, "-c", basedirsConfig, "-m", mounts, inputA)
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

		bddb, err := bolt.OpenBaseDirsReader(filepath.Join(outputA, "basedirs.db"), ownersPath)
		So(err, ShouldBeNil)

		bddb.SetMountPoints([]string{
			"/nfs/",
			"/lustre/",
		})

		h, err := bddb.History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)

		fixTZs(h)

		So(h, ShouldResemble, []basedirs.History{
			{Date: yesterday.In(time.UTC), UsageSize: 15, UsageInodes: 5},
		})

		bddb.Close()

		database, err := bolt.OpenDatabase(filepath.Join(outputA, "dguta.dbs"))
		So(err, ShouldBeNil)

		tree := db.NewTree(database)

		childrenExist := tree.DirHasChildren("/", nil)
		So(childrenExist, ShouldBeTrue)

		tree.Close()

		_, root = internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"lustre", 2, 1<<29, 1<<31, true, refTime.Unix())

		f, err = os.Create(inputB)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, root.AsReader())
		So(err, ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		So(os.Chtimes(inputB, refTime, refTime), ShouldBeNil)

		_, _, _, err = runWRStat("summarise", "-s", filepath.Join(outputA, "basedirs.db"),
			"-d", outputB, "-q", quotaFile, "-c", basedirsConfig, "-m", mounts, inputB)
		So(err, ShouldBeNil)

		bddb, err = bolt.OpenBaseDirsReader(filepath.Join(outputB, "basedirs.db"), ownersPath)
		So(err, ShouldBeNil)

		bddb.SetMountPoints([]string{
			"/nfs/",
			"/lustre/",
		})

		h, err = bddb.History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)

		fixTZs(h)

		So(h, ShouldResemble, []basedirs.History{
			{Date: yesterday.In(time.UTC), UsageSize: 15, UsageInodes: 5},
			{Date: refTime.In(time.UTC), UsageSize: 15, UsageInodes: 5},
		})

		bddb.Close()
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
