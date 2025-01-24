package main

import (
	"compress/gzip"
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
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
)

const app = "wrstat-ui_test"

func buildSelf() func() {
	cmd := exec.Command(
		"go", "build", "-tags", "netgo",
		"-ldflags=-X github.com/wtsi-hgi/wrstat-ui/cmd.runJobs=0 -X github.com/wtsi-hgi/wrstat-ui/cmd.Version=TESTVERSION",
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

func runWRStat(args ...string) (string, string, []*jobqueue.JobViaJSON, error) {
	var (
		stdout, stderr strings.Builder
		jobs           []*jobqueue.JobViaJSON
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
			var j *jobqueue.JobViaJSON

			if errr := jd.Decode(&j); errr != nil {
				break
			}

			jobs = append(jobs, j)
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

		err = os.WriteFile(quotaFile, []byte(`1,/lustre/scratch125,4000000000,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
77777,/lustre/scratch125,500,50
1,/nfs/scratch125,4000000000,20
2,/nfs/scratch125,300,30
2,/nfs/scratch123,400,40
77777,/nfs/scratch125,500,50
3,/lustre/scratch125,300,30
`), 0600)
		So(err, ShouldBeNil)

		err = os.WriteFile(basedirsConfig, []byte(`/lustre/scratch123/hgi/mdt	5	5
/nfs/scratch123/hgi/mdt	5	5
/	4	4`), 0600)
		So(err, ShouldBeNil)

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

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

		_, _, _, err = runWRStat("summarise", "-d", outputA, "-q", quotaFile, "-c", basedirsConfig, inputA)
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

		h, err := bddb.History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)
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
			"-d", outputB, "-q", quotaFile, "-c", basedirsConfig, inputB)
		So(err, ShouldBeNil)

		bddb, err = basedirs.NewReader(filepath.Join(outputB, "basedirs.db"), ownersPath)
		So(err, ShouldBeNil)

		h, err = bddb.History(gid, "/lustre/scratch125/humgen/projects/D")
		So(err, ShouldBeNil)
		So(h, ShouldResemble, []basedirs.History{
			{Date: yesterday.In(time.UTC), UsageSize: 15, UsageInodes: 5},
			{Date: refTime.In(time.UTC), UsageSize: 15, UsageInodes: 5},
		})

		bddb.Close()
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

		So(os.Mkdir(runA, 0755), ShouldBeNil)
		So(os.WriteFile(statsA, nil, 0600), ShouldBeNil)

		_, _, jobs, err := runWRStat("watch", "-o", output, "-q", "/some/quota.file", "-c", "basedirs.config", tmp)
		So(err, ShouldBeNil)

		So(jobs, ShouldResemble, []*jobqueue.JobViaJSON{
			{
				Cmd: fmt.Sprintf(`"./wrstat-ui_test" summarise -d %[1]q -q `+
					`"/some/quota.file" -c "basedirs.config" %[2]q && touch -r %[3]q %[1]q && mv %[1]q %[4]q`,
					dotA, statsA, runA, finalA,
				),
				ReqGrp: "wrstat-ui-summarise",
			},
		})

		So(os.Remove(dotA), ShouldBeNil)

		previous := filepath.Join(output, "12344_A")
		previousBasedirs := filepath.Join(previous, "basedirs.db")

		So(os.Mkdir(previous, 0700), ShouldBeNil)
		So(os.WriteFile(previousBasedirs, nil, 0600), ShouldBeNil)

		_, _, jobs, err = runWRStat("watch", "-o", output, "-q", "/some/quota.file", "-c", "basedirs.config", tmp)
		So(err, ShouldBeNil)

		So(jobs, ShouldResemble, []*jobqueue.JobViaJSON{
			{
				Cmd: fmt.Sprintf(`"./wrstat-ui_test" summarise -d %[1]q `+
					`-s %[2]q -q "/some/quota.file" -c "basedirs.config" %[3]q && touch -r %[4]q %[1]q && mv %[1]q %[5]q`,
					dotA, previousBasedirs, statsA, runA, finalA,
				),
				ReqGrp: "wrstat-ui-summarise",
			},
		})
	})
}
