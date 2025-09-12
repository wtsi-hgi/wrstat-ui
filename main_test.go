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
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/cmd"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
)

const app = "wrstat-ui_test"

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

func TestMain(m *testing.M) {
	d1 := buildSelf()
	if d1 == nil {
		return
	}

	defer os.Exit(m.Run())
	defer d1()
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

	cmd := exec.Command("./"+app, args...)
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

		bddb, err := basedirs.NewReader(filepath.Join(outputA, "basedirs.db"), ownersPath)
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

		tree, err := db.NewTree(filepath.Join(outputA, "dguta.dbs"))
		So(err, ShouldBeNil)

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

		bddb, err = basedirs.NewReader(filepath.Join(outputB, "basedirs.db"), ownersPath)
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

func fixTZs(h []basedirs.History) {
	for n := range h {
		h[n].Date = h[n].Date.In(time.UTC)
	}
}

func TestSummariseClickHouse(t *testing.T) {
	// Check if TEST_CLICKHOUSE_HOST environment variable is set
	// If not, skip the test
	chHost := os.Getenv("TEST_CLICKHOUSE_HOST")
	if chHost == "" {
		chHost = "127.0.0.1" // default host
	}

	chPort := os.Getenv("TEST_CLICKHOUSE_PORT")
	if chPort == "" {
		chPort = "9000" // default port
	}

	chUsername := os.Getenv("TEST_CLICKHOUSE_USERNAME")
	if chUsername == "" {
		chUsername = "default" // default username
	}

	chPassword := os.Getenv("TEST_CLICKHOUSE_PASSWORD")
	// No default password

	// Create a unique test database name based on the current username
	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("Failed to get current user: %v", err)
	}
	testDatabase := fmt.Sprintf("test_wrstatui_%s", currentUser.Username)

	// Create a test connection
	ctx := context.Background()

	// Connect to default database first for management operations
	adminConn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
		Auth:        clickhouse.Auth{Database: "default", Username: chUsername, Password: chPassword},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		t.Skipf("Skipping TestSummariseClickHouse - could not connect to ClickHouse: %v", err)
		return
	}

	// First drop the test database if it exists
	if err := adminConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDatabase)); err != nil {
		t.Logf("Warning: failed to drop existing test DB: %v", err)
	}

	// Create a fresh test database
	err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDatabase))
	if err != nil {
		adminConn.Close()
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Close admin connection
	adminConn.Close()

	// Clean up the test database after the test
	defer func() {
		// Create a new connection to default database for cleanup
		cleanupConn, err := clickhouse.Open(&clickhouse.Options{
			Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
			Auth:        clickhouse.Auth{Database: "default", Username: chUsername, Password: chPassword},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Errorf("Failed to connect for cleanup: %v", err)
			return
		}

		if err := cleanupConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDatabase)); err != nil {
			t.Errorf("Failed to drop test database during cleanup: %v", err)
		}

		cleanupConn.Close()
	}()

	Convey("summarise with ClickHouse integration", t, func() {
		// Prepare test data - use fixed UIDs and GIDs for testing
		uid := uint32(1000) // standard test user ID
		gid := uint32(1000) // standard test group ID

		refTime := time.Now().Truncate(time.Second)
		unixTime := refTime.Unix()
		root := statsdata.NewRoot("/lustre/scratch125/", unixTime)
		statsdata.AddFile(root, "humgen/projects/A/file1", uid, gid, 1000, unixTime, unixTime)
		statsdata.AddFile(root, "humgen/projects/A/file2", uid, gid, 2000, unixTime, unixTime)
		statsdata.AddFile(root, "humgen/projects/B/file3", uid, gid, 3000, unixTime, unixTime)

		// Create temporary input file
		tmpDir := t.TempDir()
		statsPath := filepath.Join(tmpDir, "test_stats")

		f, err := os.Create(statsPath)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, root.AsReader())
		So(err, ShouldBeNil)
		So(f.Close(), ShouldBeNil)

		// Run the test directly using the exported functions
		mountPath := "/lustre/scratch125/"

		// Create a fresh connection to the test database
		testConn, err := clickhouse.Open(&clickhouse.Options{
			Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
			Auth:        clickhouse.Auth{Database: testDatabase, Username: chUsername, Password: chPassword},
			DialTimeout: 10 * time.Second,
			Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
			Settings: clickhouse.Settings{
				"max_insert_block_size":       1000000,
				"min_insert_block_size_rows":  100000,
				"min_insert_block_size_bytes": 10485760, // 10MB
			},
		})
		So(err, ShouldBeNil)
		defer testConn.Close()

		// Create schema first
		err = cmd.CreateSchema(ctx, testConn)
		So(err, ShouldBeNil)

		// Open the stats file
		r, _, err := cmd.OpenStatsFile(statsPath)
		So(err, ShouldBeNil)
		defer r.Close()

		// Update the ClickHouse database
		err = cmd.UpdateClickhouse(ctx, testConn, mountPath, r)
		So(err, ShouldBeNil)

		// The implementation inserts a ready scan row; no manual fixup needed.

		// Verify data was inserted by querying ClickHouse
		chConn, err := clickhouse.Open(&clickhouse.Options{
			Addr:        []string{fmt.Sprintf("%s:%s", chHost, chPort)},
			Auth:        clickhouse.Auth{Database: testDatabase, Username: chUsername, Password: chPassword},
			DialTimeout: 5 * time.Second,
		})
		So(err, ShouldBeNil)
		defer chConn.Close()

		// Check scans table
		var scanCount uint64
		err = chConn.QueryRow(ctx, "SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?", mountPath).Scan(&scanCount)
		So(err, ShouldBeNil)
		So(scanCount, ShouldBeGreaterThanOrEqualTo, 1)

		// Check fs_entries table - we should have more than 3 entries due to directory entries
		var fileCount uint64
		err = chConn.QueryRow(ctx, "SELECT count() FROM fs_entries_current WHERE mount_path = ?", mountPath).Scan(&fileCount)
		So(err, ShouldBeNil)
		So(fileCount, ShouldBeGreaterThan, 3) // Should have at least our 3 files plus directories

		// Check ancestor_rollups_raw table
		var rollupCount uint64
		err = chConn.QueryRow(ctx, "SELECT count() FROM ancestor_rollups_current WHERE mount_path = ?", mountPath).Scan(&rollupCount)
		So(err, ShouldBeNil)
		So(rollupCount, ShouldBeGreaterThan, 3) // Should have multiple rollups per file

		// Check total size calculation in rollups - we expect at least 6000 (our 3 files),
		// but there may be additional bytes for directory entries
		var totalSize uint64
		err = chConn.QueryRow(ctx, `
			SELECT total_size 
			FROM ancestor_rollups_current 
			WHERE mount_path = ? AND ancestor = ?`,
			mountPath, mountPath).Scan(&totalSize)
		So(err, ShouldBeNil)
		So(totalSize, ShouldBeGreaterThanOrEqualTo, 6000) // At least 1000 + 2000 + 3000

		// Test querying by path
		var fileSize uint64
		err = chConn.QueryRow(ctx, `
			SELECT size FROM fs_entries_current 
			WHERE path = ?`,
			mountPath+"humgen/projects/A/file1").Scan(&fileSize)
		So(err, ShouldBeNil)
		So(fileSize, ShouldEqual, 1000)

		// Test getting a subtree summary
		summary, err := cmd.OptimizedSubtreeSummary(ctx, chConn, mountPath, mountPath+"humgen/projects/A/", cmd.Filters{})
		So(err, ShouldBeNil)
		So(summary.TotalSize, ShouldBeGreaterThanOrEqualTo, 3000) // At least 1000 + 2000
		So(summary.FileCount, ShouldBeGreaterThanOrEqualTo, 2)    // At least 2 files in directory A

		// Test directory listing
		entries, err := cmd.ListImmediateChildren(ctx, chConn, mountPath, mountPath+"humgen/projects/")
		So(err, ShouldBeNil)
		So(len(entries), ShouldBeGreaterThanOrEqualTo, 2)

		// Check the search functionality
		paths, err := cmd.SearchGlobPaths(ctx, chConn, mountPath, "*/projects/*/file*", 10, false)
		So(err, ShouldBeNil)
		So(len(paths), ShouldEqual, 3) // All 3 files match the pattern
	})
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
		statsdata.AddFile(fa, "someDir/not_displayed", 0, 0, 1, 0, 0)

		statsdata.AddFile(fa, "someDir/not_displayed_2", 0, 0, 1, 0, 0).Inode = 1
		statsdata.AddFile(fa, "someDir/not_displayed_3", 0, 0, 1, 0, 0).Inode = 1

		statsdata.AddFile(fa, "someDir/big_files/1", 0, 0, 100, 0, 0)
		statsdata.AddFile(fa, "someDir/big_files/2", 0, 0, 101, 0, 0)

		statsdata.AddFile(fa, "someDir/big_files/3", 0, 0, 101, 0, 0).Inode = 2
		statsdata.AddFile(fa, "someDir/big_files/4", 0, 0, 101, 0, 0).Inode = 2

		statsdata.AddFile(fa, "someDir/big_files/5", 0, 0, 102, 0, 0)

		fb := statsdata.NewRoot("/mount/B/", 0)
		statsdata.AddFile(fb, "anotherDir/big_files/1", 0, 0, 100, 0, 0)

		statsdata.AddFile(fb, "anotherDir/big_files/2", 0, 0, 101, 0, 0).Inode = 2

		statsdata.AddFile(fb, "anotherDir/big_files/3", 0, 0, 103, 0, 0)

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
