/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package chperf

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	errImportTestFiles    = errors.New("files")
	errImportTestBasedirs = errors.New("basedirs")
	errImportTestDGUTA    = errors.New("dguta")
)

const (
	expectedPhasePartitionDropReset = "partition_drop_reset"
	expectedPhaseDGUTAInsert        = "wrstat_dguta_insert"
	expectedPhaseChildrenInsert     = "wrstat_children_insert"
	expectedPhaseMountSwitch        = "mount_switch"
	expectedPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"
)

func TestSumResults(t *testing.T) {
	Convey("sumResults", t, func() {
		Convey("totals records from successful results", func() {
			results := []importResult{
				{records: 100, err: nil},
				{records: 200, err: nil},
			}

			total, err := sumResults(results)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 300)
		})

		Convey("returns zero for empty results", func() {
			total, err := sumResults(nil)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 0)
		})

		Convey("returns first error encountered", func() {
			results := []importResult{
				{records: 100, err: nil},
				{records: 0, err: ErrNoDatasets},
			}

			total, err := sumResults(results)
			So(err, ShouldEqual, ErrNoDatasets)
			So(total, ShouldEqual, 0)
		})
	})
}

func TestEffectiveParallelism(t *testing.T) {
	Convey("effectiveParallelism clamps into the supported range", t, func() {
		So(effectiveParallelism(0), ShouldEqual, 1)
		So(effectiveParallelism(1), ShouldEqual, 1)
		So(effectiveParallelism(4), ShouldEqual, 4)
		So(effectiveParallelism(9), ShouldEqual, 4)
	})
}

func TestLineCountingReader(t *testing.T) {
	Convey("lineCountingReader", t, func() {
		Convey("reads all when maxLines is zero", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\n"), 0)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})

		Convey("stops after maxLines", func() {
			lr := newLineCountingReader(strings.NewReader("a\nb\nc\n"), 2)

			b, err := io.ReadAll(lr)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "a\nb\n")
			So(lr.linesRead(), ShouldEqual, 2)
		})
	})
}

func TestDGUTARowCounting(t *testing.T) {
	Convey("countDGUTARows ignores nil gutas", t, func() {
		record := db.RecordDGUTA{GUTAs: db.GUTAs{nil, &db.GUTA{}, nil, &db.GUTA{}}}
		So(countDGUTARows(record), ShouldEqual, 2)
	})

	Convey("countChildrenRows ignores blank child names", t, func() {
		children := []string{"/a", "", "/b/", "/"}
		So(countChildrenRows(children), ShouldEqual, 2)
	})
}

type fakeImportDGUTAWriter struct {
	batchSize int
	mountPath string
	updatedAt time.Time
	closed    bool
	aborted   bool
}

func (*fakeImportDGUTAWriter) Add(db.RecordDGUTA) error { return nil }

func (w *fakeImportDGUTAWriter) SetBatchSize(batchSize int) {
	w.batchSize = batchSize
}

func (w *fakeImportDGUTAWriter) SetMountPath(mountPath string) {
	w.mountPath = mountPath
}

func (w *fakeImportDGUTAWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
}

func (w *fakeImportDGUTAWriter) Close() error {
	w.closed = true

	return nil
}

func (w *fakeImportDGUTAWriter) Abort() error {
	w.aborted = true

	return nil
}

func TestAddAllSummarisers(t *testing.T) {
	Convey("addAllSummarisers uses injected import dependencies", t, func() {
		api := &fakeImportAPI{
			dgutaWriter: &fakeImportDGUTAWriter{},
			fileCloser:  &fakeImportCloser{},
		}
		updatedAt := time.Date(2026, 3, 9, 12, 34, 56, 0, time.UTC)
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")

		closer, err := addAllSummarisers(
			summary.NewSummariser(nil),
			api,
			"/mnt/scratch/",
			updatedAt,
			ImportOptions{BatchSize: 17},
			metrics,
		)

		So(err, ShouldBeNil)
		So(api.fileMountPath, ShouldEqual, "/mnt/scratch/")
		So(api.fileUpdatedAt, ShouldEqual, updatedAt)
		So(api.dgutaWriter.batchSize, ShouldEqual, 17)
		So(api.fileCloser.batchSize, ShouldEqual, 17)
		So(api.dgutaWriter.mountPath, ShouldEqual, "/mnt/scratch/")
		So(api.dgutaWriter.updatedAt, ShouldEqual, updatedAt)
		So(api.baseDirsCalls, ShouldEqual, 0)

		So(closer(true), ShouldBeNil)
		So(api.fileCloser.closed, ShouldBeTrue)
		So(api.dgutaWriter.closed, ShouldBeTrue)
		So(api.dgutaWriter.aborted, ShouldBeFalse)
	})
}

func trackedImportBasedirsCloser(closer abortTrackingCloser) func(bool) error {
	return func(publish bool) error {
		if publish {
			return closer.Close()
		}

		return closer.Abort()
	}
}

type fakeImportCloser struct {
	closed    bool
	batchSize int
}

func (c *fakeImportCloser) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

func (c *fakeImportCloser) Close() error {
	c.closed = true

	return nil
}

type fakeImportOperation struct{}

func (fakeImportOperation) Add(*summary.FileInfo) error { return nil }

func (fakeImportOperation) Output() error { return nil }

type historyTrackingBasedirsStore struct {
	appendErr      error
	appendInserted bool
	batchSize      int
	aborted        bool
}

func (*historyTrackingBasedirsStore) SetMountPath(string) {}

func (*historyTrackingBasedirsStore) SetUpdatedAt(time.Time) {}

func (s *historyTrackingBasedirsStore) SetBatchSize(batchSize int) {
	s.batchSize = batchSize
}

func (*historyTrackingBasedirsStore) Reset() error { return nil }

func (*historyTrackingBasedirsStore) PutGroupUsage(*basedirs.Usage) error { return nil }

func (*historyTrackingBasedirsStore) PutUserUsage(*basedirs.Usage) error { return nil }

func (*historyTrackingBasedirsStore) PutGroupSubDirs(
	basedirs.SubDirKey,
	[]*basedirs.SubDir,
) error {
	return nil
}

func (*historyTrackingBasedirsStore) PutUserSubDirs(
	basedirs.SubDirKey,
	[]*basedirs.SubDir,
) error {
	return nil
}

func (s *historyTrackingBasedirsStore) AppendGroupHistory(
	basedirs.HistoryKey,
	basedirs.History,
) error {
	return s.appendErr
}

func (s *historyTrackingBasedirsStore) LastHistoryAppendInserted() bool {
	return s.appendInserted
}

func (*historyTrackingBasedirsStore) Finalise() error { return nil }

func (*historyTrackingBasedirsStore) Close() error { return nil }

func (s *historyTrackingBasedirsStore) Abort() error {
	s.aborted = true

	return nil
}

func TestAddBasedirsSummariserPropagatesBatchSize(t *testing.T) {
	Convey("addBasedirsSummariser propagates the configured batch size to the basedirs store", t, func() {
		tmpDir := t.TempDir()
		quotaPath := filepath.Join(tmpDir, "quota.csv")
		configPath := filepath.Join(tmpDir, "basedirs.config")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/scratch,100,10\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(configPath, []byte("/\t1\t1\n"), 0o600), ShouldBeNil)

		store := &historyTrackingBasedirsStore{}
		api := &fakeImportAPI{baseDirsStore: store}
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")

		closer, err := addBasedirsSummariser(
			summary.NewSummariser(nil),
			api,
			"/mnt/scratch/",
			time.Date(2026, 3, 9, 12, 34, 56, 0, time.UTC),
			ImportOptions{BatchSize: 23, QuotaPath: quotaPath, ConfigPath: configPath},
			metrics,
		)

		So(err, ShouldBeNil)
		So(closer, ShouldNotBeNil)
		So(store.batchSize, ShouldEqual, 23)
		So(api.baseDirsCalls, ShouldEqual, 1)
		So(closer(true), ShouldBeNil)
	})
}

func TestTrackedBasedirsStoreHistoryRows(t *testing.T) {
	Convey("trackedBasedirsStore records reset time in the spec-level partition drop/reset phase", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{},
			metrics: metrics,
		}

		So(store.Reset(), ShouldBeNil)
		So(metrics.phases[phaseBasedirsReset], ShouldBeGreaterThan, time.Duration(0))
		So(metrics.phases[expectedPhasePartitionDropReset], ShouldBeGreaterThan, time.Duration(0))
	})

	Convey("trackedBasedirsStore counts successful history appends in report rows", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendInserted: true},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: "/mnt/scratch/"},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldBeNil)

		metrics.phases[phaseBasedirsHistory] = time.Millisecond

		result := metrics.result(0, time.Second)
		So(result.rows["wrstat_basedirs_history"], ShouldEqual, 1)

		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		addImportReportOperations(&report, []datasetImportResult{result}, 1, time.Second)

		historyPhase := findImportOperation(report.Operations, "import_phase", phaseBasedirsHistory)
		So(historyPhase, ShouldNotBeNil)
		So(historyPhase.Inputs["table"], ShouldEqual, "wrstat_basedirs_history")
		So(historyPhase.Inputs["rows"], ShouldEqual, uint64(1))
	})

	Convey("trackedBasedirsStore ignores history appends skipped without insertion", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendInserted: false},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: "/mnt/scratch/"},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldBeNil)
		So(metrics.rows["wrstat_basedirs_history"], ShouldEqual, uint64(0))
	})

	Convey("trackedBasedirsStore ignores failed history appends", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", "/mnt/scratch/")
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendErr: errImportTestBasedirs},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: "/mnt/scratch/"},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldEqual, errImportTestBasedirs)
		So(metrics.rows["wrstat_basedirs_history"], ShouldEqual, uint64(0))
	})
}

func findImportOperation(ops []boltperf.Operation, name, phase string) *boltperf.Operation {
	for i := range ops {
		if ops[i].Name != name {
			continue
		}

		if phase == "" || ops[i].Inputs["phase"] == phase {
			return &ops[i]
		}
	}

	return nil
}

type fakeImportAPI struct {
	dgutaWriter   *fakeImportDGUTAWriter
	fileCloser    *fakeImportCloser
	baseDirsStore *historyTrackingBasedirsStore
	fileMountPath string
	fileUpdatedAt time.Time
	baseDirsCalls int
}

func (a *fakeImportAPI) NewDGUTAWriter() (db.DGUTAWriter, error) {
	if a.dgutaWriter == nil {
		a.dgutaWriter = &fakeImportDGUTAWriter{}
	}

	return a.dgutaWriter, nil
}

func (a *fakeImportAPI) NewFileIngestOperation(
	mountPath string,
	updatedAt time.Time,
) (summary.OperationGenerator, io.Closer, error) {
	a.fileMountPath = mountPath
	a.fileUpdatedAt = updatedAt

	if a.fileCloser == nil {
		a.fileCloser = &fakeImportCloser{}
	}

	return func() summary.Operation { return fakeImportOperation{} }, a.fileCloser, nil
}

func (a *fakeImportAPI) NewBaseDirsStore() (basedirs.Store, error) {
	a.baseDirsCalls++

	if a.baseDirsStore == nil {
		a.baseDirsStore = &historyTrackingBasedirsStore{}
	}

	return a.baseDirsStore, nil
}

type orderedCloser struct {
	name  string
	calls *[]string
	err   error
}

func (c orderedCloser) Close() error {
	if c.calls != nil {
		*c.calls = append(*c.calls, c.name)
	}

	return c.err
}

func TestComposeImportCloser(t *testing.T) {
	Convey("composeImportCloser closes resources in summarise order on success", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: "files", calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: "basedirs-close",
				abortName: "basedirs-abort",
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: "dguta-close", abortName: "dguta-abort", calls: &calls},
		)

		So(closer(true), ShouldBeNil)
		So(calls, ShouldResemble, []string{"files", "basedirs-close", "dguta-close"})
	})

	Convey("composeImportCloser aborts dguta publishing on failure", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: "files", calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: "basedirs-close",
				abortName: "basedirs-abort",
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: "dguta-close", abortName: "dguta-abort", calls: &calls},
		)

		So(closer(false), ShouldBeNil)
		So(calls, ShouldResemble, []string{"files", "basedirs-abort", "dguta-abort"})
	})

	Convey("composeImportCloser aborts dguta publishing when file close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: "files", calls: &calls, err: errImportTestFiles},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: "basedirs-close",
				abortName: "basedirs-abort",
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: "dguta-close", abortName: "dguta-abort", calls: &calls},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestFiles.Error())
		So(calls, ShouldResemble, []string{"files", "basedirs-abort", "dguta-abort"})
	})

	Convey("composeImportCloser aborts dguta publishing when basedirs close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: "files", calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: "basedirs-close",
				abortName: "basedirs-abort",
				calls:     &calls,
				closeErr:  errImportTestBasedirs,
			}),
			abortTrackingCloser{closeName: "dguta-close", abortName: "dguta-abort", calls: &calls},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestBasedirs.Error())
		So(calls, ShouldResemble, []string{"files", "basedirs-close", "dguta-abort", "basedirs-abort"})
	})

	Convey("composeImportCloser aborts basedirs when dguta publish fails after basedirs flush", t, func() {
		calls := make([]string, 0, 4)

		closer := composeImportCloser(
			orderedCloser{name: "files", calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: "basedirs-close",
				abortName: "basedirs-abort",
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: "dguta-close", abortName: "dguta-abort", calls: &calls, closeErr: errImportTestDGUTA},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestDGUTA.Error())
		So(calls, ShouldResemble, []string{"files", "basedirs-close", "dguta-close", "basedirs-abort"})
	})

	Convey("composeImportCloser joins cleanup errors", t, func() {
		err := composeImportCloser(
			orderedCloser{name: "files", err: errImportTestFiles},
			func(bool) error { return errImportTestBasedirs },
			abortTrackingCloser{abortErr: errImportTestDGUTA},
		)(false)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestFiles.Error())
		So(err.Error(), ShouldContainSubstring, errImportTestBasedirs.Error())
		So(err.Error(), ShouldContainSubstring, errImportTestDGUTA.Error())
	})
}

type abortTrackingCloser struct {
	closeName string
	abortName string
	calls     *[]string
	closeErr  error
	abortErr  error
}

func (c abortTrackingCloser) Close() error {
	if c.calls != nil && c.closeName != "" {
		*c.calls = append(*c.calls, c.closeName)
	}

	return c.closeErr
}

func (c abortTrackingCloser) Abort() error {
	if c.calls != nil && c.abortName != "" {
		*c.calls = append(*c.calls, c.abortName)
	}

	return c.abortErr
}

func TestImportReportOperations(t *testing.T) {
	Convey("addImportReportOperations emits per-file and per-phase detail", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   "v1_／mnt／scratch",
			statsPath: "/input/v1_／mnt／scratch/stats.gz",
			mountPath: "/mnt/scratch/",
			lines:     42,
			elapsed:   1500 * time.Millisecond,
			rows: map[string]uint64{
				"wrstat_files":                  42,
				"wrstat_dguta":                  7,
				"wrstat_children":               5,
				"wrstat_basedirs_group_usage":   3,
				"wrstat_basedirs_user_usage":    2,
				"wrstat_basedirs_group_subdirs": 4,
			},
			phases: map[string]time.Duration{
				expectedPhasePartitionDropReset: 160 * time.Millisecond,
				phaseFilesInsert:                500 * time.Millisecond,
				phaseFilesFlush:                 50 * time.Millisecond,
				expectedPhaseDGUTAInsert:        200 * time.Millisecond,
				expectedPhaseChildrenInsert:     100 * time.Millisecond,
				expectedPhaseMountSwitch:        120 * time.Millisecond,
				expectedPhaseOldSnapshotDrop:    80 * time.Millisecond,
				phaseBasedirsReset:              100 * time.Millisecond,
				phaseBasedirsGroupUsage:         75 * time.Millisecond,
				phaseBasedirsFinalise:           25 * time.Millisecond,
				phaseBasedirsFlush:              10 * time.Millisecond,
			},
		}

		addImportReportOperations(&report, []datasetImportResult{result}, 2, 2*time.Second)

		So(report.Operations, ShouldHaveLength, 13)

		fileTotal := findImportOperation(report.Operations, "import_file_total", "")
		So(fileTotal, ShouldNotBeNil)
		So(fileTotal.Inputs["dataset"], ShouldEqual, result.dataset)
		So(fileTotal.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(fileTotal.Inputs["lines"], ShouldEqual, result.lines)

		rows, ok := fileTotal.Inputs["rows_per_table"].(map[string]uint64)
		So(ok, ShouldBeTrue)
		So(rows, ShouldResemble, result.rows)

		partitionReset := findImportOperation(report.Operations, "import_phase", expectedPhasePartitionDropReset)
		So(partitionReset, ShouldNotBeNil)

		tables, ok := partitionReset.Inputs["tables"].([]string)
		So(ok, ShouldBeTrue)
		So(tables, ShouldResemble, []string{
			"wrstat_dguta",
			"wrstat_children",
			"wrstat_files",
			"wrstat_basedirs_group_usage",
			"wrstat_basedirs_user_usage",
			"wrstat_basedirs_group_subdirs",
			"wrstat_basedirs_user_subdirs",
		})
		So(partitionReset.DurationsMS, ShouldResemble, []float64{160})

		filesInsert := findImportOperation(report.Operations, "import_phase", phaseFilesInsert)
		So(filesInsert, ShouldNotBeNil)
		So(filesInsert.Inputs["dataset"], ShouldEqual, result.dataset)
		So(filesInsert.Inputs["phase"], ShouldEqual, phaseFilesInsert)
		So(filesInsert.Inputs["rows"], ShouldEqual, uint64(42))
		So(filesInsert.DurationsMS, ShouldResemble, []float64{500})

		dgutaInsert := findImportOperation(report.Operations, "import_phase", expectedPhaseDGUTAInsert)
		So(dgutaInsert, ShouldNotBeNil)
		So(dgutaInsert.Inputs["table"], ShouldEqual, "wrstat_dguta")
		So(dgutaInsert.Inputs["rows"], ShouldEqual, uint64(7))
		So(dgutaInsert.DurationsMS, ShouldResemble, []float64{200})

		childrenInsert := findImportOperation(report.Operations, "import_phase", expectedPhaseChildrenInsert)
		So(childrenInsert, ShouldNotBeNil)
		So(childrenInsert.Inputs["table"], ShouldEqual, "wrstat_children")
		So(childrenInsert.Inputs["rows"], ShouldEqual, uint64(5))
		So(childrenInsert.DurationsMS, ShouldResemble, []float64{100})

		mountSwitch := findImportOperation(report.Operations, "import_phase", expectedPhaseMountSwitch)
		So(mountSwitch, ShouldNotBeNil)
		So(mountSwitch.DurationsMS, ShouldResemble, []float64{120})

		oldSnapshotDrop := findImportOperation(report.Operations, "import_phase", expectedPhaseOldSnapshotDrop)
		So(oldSnapshotDrop, ShouldNotBeNil)
		So(oldSnapshotDrop.DurationsMS, ShouldResemble, []float64{80})

		total := findImportOperation(report.Operations, "import_total", "")
		So(total, ShouldNotBeNil)
		So(total.Inputs["datasets"], ShouldEqual, 1)
		So(total.Inputs["records"], ShouldEqual, uint64(42))
		So(total.Inputs["parallelism"], ShouldEqual, 2)
		So(total.Inputs["mode"], ShouldEqual, "parallel")
		So(total.Inputs["throughput_records_per_sec"], ShouldEqual, 21.0)
	})
}
