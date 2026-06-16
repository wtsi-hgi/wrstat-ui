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
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	expectedPhasePartitionDropReset = "partition_drop_reset"
	expectedPhaseDGUTAInsert        = "wrstat_dguta_insert"
	expectedPhaseCatalogInsert      = "wrstat_dirs_insert"
	expectedPhaseDirFactsInsert     = "wrstat_dir_facts_insert"
	expectedPhaseMountSwitch        = "mount_switch"
	expectedPhaseOldSnapshotDrop    = "old_snapshot_partition_drop"

	importTestFilesClose    = "files"
	importTestBasedirsClose = "basedirs-close"
	importTestBasedirsAbort = "basedirs-abort"
	importTestDGUTAClose    = "dguta-close"
	importTestDGUTAAbort    = "dguta-abort"
	importTestMountScratch  = "/mnt/scratch/"
	importTestDataset       = "v1_／mnt／scratch"
	importTestStatsPath     = "/input/v1_／mnt／scratch/stats.gz"
)

var (
	errImportTestFiles    = errors.New(importTestFilesClose)
	errImportTestBasedirs = errors.New("basedirs")
	errImportTestDGUTA    = errors.New("dguta")
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
		So(countDGUTARows(record, ""), ShouldEqual, 2)
	})

	Convey("countDGUTARows reports compacted internal mount rows", t, func() {
		paths := internaltest.NewDirectoryPathCreator()
		record := db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(importTestMountScratch + "dir/"),
			GUTAs: db.GUTAs{
				{Age: db.DGUTAgeAll},
				{Age: db.DGUTAgeA1M},
				nil,
				{Age: db.DGUTAgeM1Y},
			},
		}

		So(countDGUTARows(record, importTestMountScratch), ShouldEqual, 1)
		So(countDGUTARows(record, strings.TrimSuffix(importTestMountScratch, "/")), ShouldEqual, 1)
		So(countDGUTARows(record, "/"), ShouldEqual, 3)

		mountRoot := record
		mountRoot.Dir = paths.ToDirectoryPath(importTestMountScratch)
		So(countDGUTARows(mountRoot, strings.TrimSuffix(importTestMountScratch, "/")), ShouldEqual, 3)

		sibling := record
		sibling.Dir = paths.ToDirectoryPath(strings.TrimSuffix(importTestMountScratch, "/") + "2/dir/")
		So(countDGUTARows(sibling, strings.TrimSuffix(importTestMountScratch, "/")), ShouldEqual, 3)
	})

	Convey("tracked DGUTA writer counts one catalog and dir-facts row per imported directory", t, func() {
		paths := internaltest.NewDirectoryPathCreator()
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		tracked := newTrackedDGUTAWriter(&fakeImportDGUTAWriter{}, metrics)
		tracked.SetMountPath(importTestMountScratch)

		So(tracked.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(importTestMountScratch + "a/"),
			GUTAs: db.GUTAs{
				&db.GUTA{Age: db.DGUTAgeAll},
				&db.GUTA{Age: db.DGUTAgeA1M},
			},
		}), ShouldBeNil)

		So(metrics.rows[tableCatalog], ShouldEqual, uint64(1))
		So(metrics.rows[tableDirFacts], ShouldEqual, uint64(1))
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
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)

		reader := statsdata.TestStats(1, 1, importTestMountScratch, updatedAt.Unix()).AsReader()
		defer func() { So(reader.Close(), ShouldBeNil) }()

		ss := summary.NewSummariser(stats.NewStatsParser(reader))
		closer, err := addAllSummarisers(
			ss,
			api,
			importTestMountScratch,
			updatedAt,
			ImportOptions{BatchSize: 17},
			metrics,
		)

		So(err, ShouldBeNil)
		So(api.fileMountPath, ShouldEqual, importTestMountScratch)
		So(api.fileUpdatedAt, ShouldEqual, updatedAt)
		So(api.dgutaWriter.batchSize, ShouldEqual, 17)
		So(api.fileCloser.batchSize, ShouldEqual, 17)
		So(api.dgutaWriter.mountPath, ShouldEqual, importTestMountScratch)
		So(api.dgutaWriter.updatedAt, ShouldEqual, updatedAt)
		So(api.baseDirsCalls, ShouldEqual, 0)
		So(api.fileAllocator, ShouldNotBeNil)
		So(ss.Summarise(), ShouldBeNil)
		So(api.fileDirIDs, ShouldNotBeEmpty)
		So(countZeroDirIDs(api.fileDirIDs), ShouldEqual, 0)

		So(closer(true), ShouldBeNil)
		So(api.fileCloser.closed, ShouldBeTrue)
		So(api.dgutaWriter.closed, ShouldBeTrue)
		So(api.dgutaWriter.aborted, ShouldBeFalse)
	})
}

func countZeroDirIDs(dirIDs []uint32) int {
	var count int

	for _, dirID := range dirIDs {
		if dirID == 0 {
			count++
		}
	}

	return count
}

func TestImportPathTextByteMetrics(t *testing.T) {
	Convey("tracked DGUTA writer measures old duplicated and new catalog path text bytes", t, func() {
		paths := internaltest.NewDirectoryPathCreator()
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		tracked := newTrackedDGUTAWriter(&fakeImportDGUTAWriter{}, metrics)
		tracked.SetMountPath(importTestMountScratch)

		dir := importTestMountScratch + "alpha/"
		So(tracked.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(dir),
			GUTAs: db.GUTAs{
				&db.GUTA{Age: db.DGUTAgeAll},
				&db.GUTA{Age: db.DGUTAgeA1M},
			},
		}), ShouldBeNil)

		So(metrics.pathTextBytesBefore, ShouldEqual, importPathTextBytesForRows(dir, metrics.rows[tableDGUTA]))
		So(metrics.pathTextBytesAfter, ShouldEqual, uint64(len(dir)))
	})

	Convey("tracked file operation measures old duplicated parent path bytes", t, func() {
		paths := internaltest.NewDirectoryPathCreator()
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		parentDir := importTestMountScratch + "alpha/"
		op := &trackedFileOperation{
			Operation: fakeImportOperation{},
			metrics:   metrics,
		}

		So(op.Add(&summary.FileInfo{
			Path:      paths.ToDirectoryPath(parentDir),
			Name:      []byte("file.dat"),
			EntryType: stats.FileType,
		}), ShouldBeNil)

		So(metrics.pathTextBytesBefore, ShouldEqual, uint64(len(parentDir)))
		So(metrics.pathTextBytesAfter, ShouldEqual, uint64(0))
	})
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

type fakeImportOperation struct {
	allocator *summary.DirIDAllocator
	dirIDs    *[]uint32
}

func (o fakeImportOperation) Add(info *summary.FileInfo) error {
	if info.IsDir() || o.dirIDs == nil {
		return nil
	}

	if o.allocator == nil {
		*o.dirIDs = append(*o.dirIDs, 0)

		return nil
	}

	dirID, err := o.allocator.DirID(info.Path)
	if err != nil {
		return err
	}

	*o.dirIDs = append(*o.dirIDs, dirID)

	return nil
}

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
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)

		closer, err := addBasedirsSummariser(
			summary.NewSummariser(nil),
			api,
			importTestMountScratch,
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
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{},
			metrics: metrics,
		}

		So(store.Reset(), ShouldBeNil)
		So(metrics.phases[phaseBasedirsReset], ShouldBeGreaterThan, time.Duration(0))
		So(metrics.phases[expectedPhasePartitionDropReset], ShouldBeGreaterThan, time.Duration(0))
	})

	Convey("trackedBasedirsStore counts successful history appends in report rows", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendInserted: true},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: importTestMountScratch},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldBeNil)

		metrics.phases[phaseBasedirsHistory] = time.Millisecond

		result := metrics.result(0, time.Second)
		So(result.rows[tableBasedirsHistory], ShouldEqual, 1)

		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		addImportReportOperations(&report, []datasetImportResult{result}, 1, time.Second, 0)

		historyPhase := findImportOperation(report.Operations, "import_phase", phaseBasedirsHistory)
		So(historyPhase, ShouldNotBeNil)
		So(historyPhase.Inputs["table"], ShouldEqual, tableBasedirsHistory)
		So(historyPhase.Inputs["rows"], ShouldEqual, uint64(1))
	})

	Convey("trackedBasedirsStore ignores history appends skipped without insertion", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendInserted: false},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: importTestMountScratch},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldBeNil)
		So(metrics.rows[tableBasedirsHistory], ShouldEqual, uint64(0))
	})

	Convey("trackedBasedirsStore ignores failed history appends", t, func() {
		metrics := newDatasetImportMetrics("dataset", "/input/stats.gz", importTestMountScratch)
		store := &trackedBasedirsStore{
			Store:   &historyTrackingBasedirsStore{appendErr: errImportTestBasedirs},
			metrics: metrics,
		}

		So(store.AppendGroupHistory(
			basedirs.HistoryKey{GID: 12, MountPath: importTestMountScratch},
			basedirs.History{Date: time.Unix(1700000000, 0).UTC()},
		), ShouldEqual, errImportTestBasedirs)
		So(metrics.rows[tableBasedirsHistory], ShouldEqual, uint64(0))
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
	dgutaWriter        *fakeImportDGUTAWriter
	fileCloser         *fakeImportCloser
	baseDirsStore      *historyTrackingBasedirsStore
	tableStats         map[string]perfreport.TableStats
	tableStatsTables   []string
	vectorStats        perfreport.FactsVectorStats
	bucketStats        perfreport.FactsBucketStats
	hotPathTables      []string
	hotPathAuditTables []string
	fileMountPath      string
	fileUpdatedAt      time.Time
	fileAllocator      *summary.DirIDAllocator
	fileDirIDs         []uint32
	baseDirsCalls      int
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
	alloc *summary.DirIDAllocator,
) (summary.OperationGenerator, io.Closer, error) {
	a.fileMountPath = mountPath
	a.fileUpdatedAt = updatedAt
	a.fileAllocator = alloc

	if a.fileCloser == nil {
		a.fileCloser = &fakeImportCloser{}
	}

	return func() summary.Operation {
		return fakeImportOperation{
			allocator: a.fileAllocator,
			dirIDs:    &a.fileDirIDs,
		}
	}, a.fileCloser, nil
}

func (a *fakeImportAPI) NewBaseDirsStore() (basedirs.Store, error) {
	a.baseDirsCalls++

	if a.baseDirsStore == nil {
		a.baseDirsStore = &historyTrackingBasedirsStore{}
	}

	return a.baseDirsStore, nil
}

func (a *fakeImportAPI) ImportTableStats(
	_ context.Context,
	tables []string,
) (map[string]perfreport.TableStats, error) {
	a.tableStatsTables = append([]string(nil), tables...)

	return a.tableStats, nil
}

func (a *fakeImportAPI) ImportFactsStats(
	context.Context,
) (perfreport.FactsVectorStats, perfreport.FactsBucketStats, error) {
	return a.vectorStats, a.bucketStats, nil
}

func (a *fakeImportAPI) ImportHotRowPathStringTables(_ context.Context, tables []string) ([]string, error) {
	a.hotPathAuditTables = append([]string(nil), tables...)

	return append([]string(nil), a.hotPathTables...), nil
}

func TestImportReportEnrichment(t *testing.T) {
	Convey("enrichImportReport records selected table stats, facts stats, and optional tables", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   importTestDataset,
			statsPath: importTestStatsPath,
			mountPath: importTestMountScratch,
			rows: map[string]uint64{
				tableFiles:                42,
				tableDGUTA:                3,
				tableCatalog:              2,
				tableDirFacts:             3,
				tableBasedirsGroupUsage:   1,
				tableBasedirsUserUsage:    1,
				tableBasedirsGroupSubdirs: 1,
				tableBasedirsUserSubdirs:  1,
				tableBasedirsHistory:      1,
			},
			phases: map[string]time.Duration{
				phaseFilesInsert:         10 * time.Millisecond,
				phaseDirProjectionWrite:  20 * time.Millisecond,
				phaseCatalogInsert:       30 * time.Millisecond,
				phaseDirFactsInsert:      50 * time.Millisecond,
				phaseBasedirsGroupUsage:  40 * time.Millisecond,
				phaseActivePrefixRefresh: 60 * time.Millisecond,
			},
		}
		api := &fakeImportAPI{
			tableStats: map[string]perfreport.TableStats{
				tableFiles: {
					Rows:              42,
					ActiveParts:       2,
					CompressedBytes:   100,
					UncompressedBytes: 200,
				},
				tableDirSummary: {
					Rows:              3,
					ActiveParts:       1,
					CompressedBytes:   80,
					UncompressedBytes: 160,
				},
				tableCatalog: {
					Rows:              2,
					ActiveParts:       1,
					CompressedBytes:   50,
					UncompressedBytes: 100,
				},
				tableDirSummarySets: {
					Rows:              3,
					ActiveParts:       1,
					CompressedBytes:   30,
					UncompressedBytes: 60,
				},
				tableBasedirsGroupUsage: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
				tableBasedirsUserUsage: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
				tableBasedirsGroupSubdirs: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
				tableBasedirsUserSubdirs: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
				tableBasedirsHistory: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
				tableDirFilterAgeAll: {
					Rows:              5,
					ActiveParts:       1,
					CompressedBytes:   70,
					UncompressedBytes: 140,
				},
				tableTreeDGUTA: {
					Rows:              6,
					ActiveParts:       1,
					CompressedBytes:   90,
					UncompressedBytes: 180,
				},
				tableActivePrefixRollups: {
					Rows:              3,
					ActiveParts:       1,
					CompressedBytes:   30,
					UncompressedBytes: 60,
				},
				tableActivePrefixFilterAgeAll: {
					Rows:              9,
					ActiveParts:       1,
					CompressedBytes:   90,
					UncompressedBytes: 180,
				},
				tableActivePrefixRollupSets: {
					Rows:              1,
					ActiveParts:       1,
					CompressedBytes:   10,
					UncompressedBytes: 20,
				},
			},
			vectorStats: perfreport.FactsVectorStats{
				Rows:                 3,
				TotalEntries:         12,
				AverageEntriesPerDir: 4,
				MaxEntriesPerDir:     9,
			},
			bucketStats: perfreport.FactsBucketStats{
				Rows:                 3,
				NonEmptyRows:         2,
				MaxBuckets:           10,
				MismatchedBucketRows: 0,
			},
		}

		So(enrichImportReport(context.Background(), &report, api, []datasetImportResult{result}), ShouldBeNil)

		So(report.SelectedTables, ShouldContain, tableFiles)
		So(report.SelectedTables, ShouldContain, tableDirSummary)
		So(report.SelectedTables, ShouldContain, tableCatalog)
		So(report.SelectedTables, ShouldContain, tableDirSummarySets)
		So(report.SelectedTables, ShouldContain, tableBasedirsGroupUsage)
		So(report.SelectedTables, ShouldContain, tableBasedirsUserUsage)
		So(report.SelectedTables, ShouldContain, tableBasedirsGroupSubdirs)
		So(report.SelectedTables, ShouldContain, tableBasedirsUserSubdirs)
		So(report.SelectedTables, ShouldContain, tableBasedirsHistory)
		So(report.SelectedTables, ShouldContain, tableDirFilterAgeAll)
		So(report.SelectedTables, ShouldContain, tableDirFacts)
		So(report.SelectedTables, ShouldContain, tableTreeDGUTA)
		So(report.SelectedTables, ShouldContain, tableActivePrefixRollups)
		So(report.SelectedTables, ShouldContain, tableActivePrefixFilterAgeAll)
		So(report.SelectedTables, ShouldContain, tableActivePrefixRollupSets)

		So(api.tableStatsTables, ShouldBeNil)

		files := report.TableStats[tableFiles]
		So(files.Rows, ShouldEqual, uint64(42))
		So(files.ActiveParts, ShouldEqual, uint64(2))
		So(files.CompressedBytes, ShouldEqual, uint64(100))
		So(files.UncompressedBytes, ShouldEqual, uint64(200))
		So(files.ImportPhaseDurationsMS[phaseFilesInsert], ShouldEqual, float64(10))

		dirFacts := report.TableStats[tableDirSummary]
		So(dirFacts.Rows, ShouldEqual, uint64(3))
		So(dirFacts.ImportPhaseDurationsMS[phaseDirProjectionWrite], ShouldEqual, float64(20))
		So(dirFacts.ImportPhaseDurationsMS[phaseDirFactsInsert], ShouldEqual, float64(50))

		catalog := report.TableStats[tableCatalog]
		So(catalog.Rows, ShouldEqual, uint64(2))
		So(catalog.ImportPhaseDurationsMS[phaseCatalogInsert], ShouldEqual, float64(30))

		basedirs := report.TableStats[tableBasedirsGroupUsage]
		So(basedirs.Rows, ShouldEqual, uint64(1))
		So(basedirs.ImportPhaseDurationsMS[phaseBasedirsGroupUsage], ShouldEqual, float64(40))

		activePrefix := report.TableStats[tableActivePrefixRollups]
		So(activePrefix.Rows, ShouldEqual, uint64(3))
		So(activePrefix.ImportPhaseDurationsMS[phaseActivePrefixRefresh], ShouldEqual, float64(60))

		So(report.FactsVectorStats, ShouldResemble, &api.vectorStats)
		So(report.FactsBucketStats, ShouldResemble, &api.bucketStats)
		So(report.MaxRSSBytes, ShouldBeGreaterThan, uint64(0))
	})

	Convey("enrichImportReport discovers every active wrstat table from ClickHouse stats", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			rows: map[string]uint64{tableFiles: 7},
			phases: map[string]time.Duration{
				phaseFullFilterAllInsert: 21 * time.Millisecond,
				phaseSchema3Ready:        22 * time.Millisecond,
				phaseActiveVirtualInsert: 23 * time.Millisecond,
				phaseActiveVirtualReady:  24 * time.Millisecond,
				phaseMountSwitch:         25 * time.Millisecond,
			},
		}
		api := &fakeImportAPI{
			tableStats: map[string]perfreport.TableStats{
				tableFiles:                   importTestTableStats(7),
				tableChildFilterAll:          importTestTableStats(11),
				tableDirFilterAll:            importTestTableStats(13),
				tableSchema3SnapshotSets:     importTestTableStats(2),
				tableActiveVirtualDirs:       importTestTableStats(5),
				tableActiveVirtualSummaries:  importTestTableStats(5),
				tableActiveVirtualFilterAll:  importTestTableStats(17),
				tableActiveVirtualChildren:   importTestTableStats(4),
				tableActiveVirtualSets:       importTestTableStats(1),
				tableMountEvents:             importTestTableStats(2),
				tableSchemaVersion:           importTestTableStats(1),
				"scratch_non_wrstat_summary": importTestTableStats(99),
			},
		}

		So(enrichImportReport(context.Background(), &report, api, []datasetImportResult{result}), ShouldBeNil)

		So(api.tableStatsTables, ShouldBeNil)

		for _, table := range []string{
			tableChildFilterAll,
			tableDirFilterAll,
			tableSchema3SnapshotSets,
			tableActiveVirtualDirs,
			tableActiveVirtualSummaries,
			tableActiveVirtualFilterAll,
			tableActiveVirtualChildren,
			tableActiveVirtualSets,
			tableMountEvents,
			tableSchemaVersion,
		} {
			So(report.SelectedTables, ShouldContain, table)
			So(report.TableStats[table].Rows, ShouldBeGreaterThan, uint64(0))
		}

		So(report.SelectedTables, ShouldNotContain, "scratch_non_wrstat_summary")
		So(report.TableStats[tableChildFilterAll].ImportPhaseDurationsMS[phaseFullFilterAllInsert],
			ShouldEqual, float64(21))
		So(report.TableStats[tableDirFilterAll].ImportPhaseDurationsMS[phaseFullFilterAllInsert],
			ShouldEqual, float64(21))
		So(report.TableStats[tableSchema3SnapshotSets].ImportPhaseDurationsMS[phaseSchema3Ready],
			ShouldEqual, float64(22))
		So(report.TableStats[tableActiveVirtualFilterAll].ImportPhaseDurationsMS[phaseActiveVirtualInsert],
			ShouldEqual, float64(23))
		So(report.TableStats[tableActiveVirtualSets].ImportPhaseDurationsMS[phaseActiveVirtualReady],
			ShouldEqual, float64(24))
		So(report.TableStats[tableMountEvents].ImportPhaseDurationsMS[phaseMountSwitch],
			ShouldEqual, float64(25))
	})

	Convey("enrichImportReport excludes absent base tables from active ClickHouse stats", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			rows: map[string]uint64{
				tableFiles:      7,
				tableDirSummary: 3,
			},
			phases: map[string]time.Duration{
				phaseDirProjectionWrite:  20 * time.Millisecond,
				phaseActivePrefixRefresh: 60 * time.Millisecond,
			},
		}
		api := &fakeImportAPI{
			tableStats: map[string]perfreport.TableStats{
				tableFiles:      importTestTableStats(7),
				tableDirSummary: importTestTableStats(3),
			},
		}

		So(enrichImportReport(context.Background(), &report, api, []datasetImportResult{result}), ShouldBeNil)

		So(api.tableStatsTables, ShouldBeNil)
		So(report.SelectedTables, ShouldResemble, []string{tableDirSummary, tableFiles})
		So(report.TableStats, ShouldHaveLength, 2)
		So(report.TableStats[tableFiles].Rows, ShouldEqual, uint64(7))
		So(report.TableStats[tableDirSummary].Rows, ShouldEqual, uint64(3))

		for _, table := range []string{
			tableDirSummarySets,
			tableActivePrefixRollups,
			tableActivePrefixFilterAgeAll,
			tableActivePrefixRollupSets,
		} {
			So(report.SelectedTables, ShouldNotContain, table)
			_, ok := report.TableStats[table]
			So(ok, ShouldBeFalse)
		}
	})
}

func importTestTableStats(rows uint64) perfreport.TableStats {
	return perfreport.TableStats{
		Rows:              rows,
		ActiveParts:       1,
		CompressedBytes:   rows * 10,
		UncompressedBytes: rows * 20,
	}
}

func TestImportProductionReportEvidence(t *testing.T) {
	Convey("Import emits E1 resource, cleanup, publish, spool, and part evidence on the real import path", t, func() {
		inputDir := t.TempDir()
		datasetDir := filepath.Join(inputDir, importTestDataset)
		So(os.MkdirAll(datasetDir, 0o755), ShouldBeNil)
		writeImportTestStatsGZ(t, filepath.Join(datasetDir, statsGZBasename))

		api := &fakeImportAPI{tableStats: importE1TableStats()}

		report, err := Import(api, inputDir, ImportOptions{Parallelism: 1}, func(string, ...any) {})

		So(err, ShouldBeNil)

		total := findImportOperation(report.Operations, "import_total", "")
		So(total, ShouldNotBeNil)
		So(total.DurationsMS, ShouldHaveLength, 1)
		So(total.Inputs["spool_bytes"], ShouldEqual, uint64(0))
		So(total.Inputs["retry_cleanup_result"], ShouldEqual, "not_attempted")
		So(total.Inputs[importInputDirIDUInt32Justified], ShouldEqual, true)

		for _, key := range []string{
			importInputUserCPUMS,
			importInputSystemCPUMS,
			importInputTotalCPUMS,
			importInputPeakRSSBytes,
			importInputPublishLatency,
			importInputRowsPerTable,
			importInputMaxDirsPerSnapshot,
			importInputPathTextBytesBefore,
			importInputPathTextBytesAfter,
			importInputPathTextBytesReduction,
			importInputPathTextBytesReductionPct,
		} {
			_, ok := total.Inputs[key]
			So(ok, ShouldBeTrue)
		}

		So(uint64Input(total.Inputs, importInputUserCPUMS), ShouldBeGreaterThanOrEqualTo, uint64(0))
		So(uint64Input(total.Inputs, importInputSystemCPUMS), ShouldBeGreaterThanOrEqualTo, uint64(0))
		So(uint64Input(total.Inputs, importInputTotalCPUMS), ShouldBeGreaterThanOrEqualTo, uint64(0))
		So(uint64Input(total.Inputs, importInputPeakRSSBytes), ShouldBeGreaterThan, uint64(0))
		So(uint64Input(total.Inputs, importInputPublishLatency), ShouldBeGreaterThanOrEqualTo, uint64(0))
		partCounts := uint64MapInput(total.Inputs, "part_counts")

		var totalParts uint64
		for _, count := range partCounts {
			totalParts += count
		}

		So(partCounts[tableFiles], ShouldEqual, uint64(2))
		So(total.Inputs["budget_source"], ShouldEqual, "computed_from_measurements")
		So(uint64Input(total.Inputs, "budget_measurement_count"), ShouldEqual, uint64(len(total.DurationsMS)))
		So(uint64Input(total.Inputs, "wall_time_budget_ms"), ShouldEqual, uint64(math.Ceil(total.P95MS)))
		So(uint64Input(total.Inputs, "total_cpu_budget_ms"),
			ShouldEqual, uint64Input(total.Inputs, importInputTotalCPUMS))
		So(uint64Input(total.Inputs, "peak_rss_budget_bytes"),
			ShouldEqual, uint64Input(total.Inputs, importInputPeakRSSBytes))
		So(uint64Input(total.Inputs, "spool_byte_budget"), ShouldEqual, uint64(0))
		So(uint64Input(total.Inputs, "part_count_budget"), ShouldEqual, totalParts)
		So(report.TableStats[tableFiles].Rows, ShouldEqual, uint64(17))
		So(report.TableStats[tableFiles].ActiveParts, ShouldEqual, uint64(2))
		So(report.TableStats[tableFiles].CompressedBytes, ShouldEqual, uint64(170))
		So(report.TableStats[tableFiles].UncompressedBytes, ShouldEqual, uint64(340))

		audit := findImportOperation(report.Operations, finalGateJ6StorageAuditOpName, "")
		So(audit, ShouldNotBeNil)
		So(audit.Inputs[finalGateJ6HotRowPathStringTablesInput], ShouldResemble, []string{})
		So(audit.Inputs[finalGateJ6PathTextCatalogTableInput], ShouldEqual, tableCatalog)
		So(audit.Inputs[finalGateJ6PathTextCopiesPerDirSnapshotInput], ShouldEqual, float64(1))
		So(api.hotPathAuditTables, ShouldContain, tableDirFilterAgeAll)
		So(api.hotPathAuditTables, ShouldNotContain, tableCatalog)
		So(api.hotPathAuditTables, ShouldContain, tableActivePrefixRollups)
		So(api.hotPathAuditTables, ShouldContain, tableActivePrefixFilterAgeAll)
		So(audit.Inputs["audited_hot_tables"], ShouldResemble, api.hotPathAuditTables)
	})
}

func writeImportTestStatsGZ(t *testing.T, path string) {
	t.Helper()

	fh, err := os.Create(path)
	So(err, ShouldBeNil)

	gz := gzip.NewWriter(fh)
	reader := statsdata.TestStats(1, 1, importTestMountScratch, 1).AsReader()
	_, err = io.Copy(gz, reader)
	So(err, ShouldBeNil)
	So(reader.Close(), ShouldBeNil)
	So(gz.Close(), ShouldBeNil)
	So(fh.Close(), ShouldBeNil)
}

func importE1TableStats() map[string]perfreport.TableStats {
	stats := make(map[string]perfreport.TableStats, len(baseImportSelectedTables()))
	for i, table := range baseImportSelectedTables() {
		rows := uint64(i + 1)
		stats[table] = perfreport.TableStats{
			Rows:              rows,
			ActiveParts:       1,
			CompressedBytes:   rows * 10,
			UncompressedBytes: rows * 20,
		}
	}

	stats[tableFiles] = perfreport.TableStats{
		Rows:              17,
		ActiveParts:       2,
		CompressedBytes:   170,
		UncompressedBytes: 340,
	}

	return stats
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
			orderedCloser{name: importTestFilesClose, calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: importTestBasedirsClose,
				abortName: importTestBasedirsAbort,
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: importTestDGUTAClose, abortName: importTestDGUTAAbort, calls: &calls},
		)

		So(closer(true), ShouldBeNil)
		So(calls, ShouldResemble, []string{importTestFilesClose, importTestBasedirsClose, importTestDGUTAClose})
	})

	Convey("composeImportCloser aborts dguta publishing on failure", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: importTestFilesClose, calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: importTestBasedirsClose,
				abortName: importTestBasedirsAbort,
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: importTestDGUTAClose, abortName: importTestDGUTAAbort, calls: &calls},
		)

		So(closer(false), ShouldBeNil)
		So(calls, ShouldResemble, []string{importTestFilesClose, importTestBasedirsAbort, importTestDGUTAAbort})
	})

	Convey("composeImportCloser aborts dguta publishing when file close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: importTestFilesClose, calls: &calls, err: errImportTestFiles},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: importTestBasedirsClose,
				abortName: importTestBasedirsAbort,
				calls:     &calls,
			}),
			abortTrackingCloser{closeName: importTestDGUTAClose, abortName: importTestDGUTAAbort, calls: &calls},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestFiles.Error())
		So(calls, ShouldResemble, []string{importTestFilesClose, importTestBasedirsAbort, importTestDGUTAAbort})
	})

	Convey("composeImportCloser aborts dguta publishing when basedirs close fails", t, func() {
		calls := make([]string, 0, 3)

		closer := composeImportCloser(
			orderedCloser{name: importTestFilesClose, calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: importTestBasedirsClose,
				abortName: importTestBasedirsAbort,
				calls:     &calls,
				closeErr:  errImportTestBasedirs,
			}),
			abortTrackingCloser{closeName: importTestDGUTAClose, abortName: importTestDGUTAAbort, calls: &calls},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestBasedirs.Error())
		So(calls, ShouldResemble, []string{
			importTestFilesClose,
			importTestBasedirsClose,
			importTestDGUTAAbort,
			importTestBasedirsAbort,
		})
	})

	Convey("composeImportCloser aborts basedirs when dguta publish fails after basedirs flush", t, func() {
		calls := make([]string, 0, 4)

		closer := composeImportCloser(
			orderedCloser{name: importTestFilesClose, calls: &calls},
			trackedImportBasedirsCloser(abortTrackingCloser{
				closeName: importTestBasedirsClose,
				abortName: importTestBasedirsAbort,
				calls:     &calls,
			}),
			abortTrackingCloser{
				closeName: importTestDGUTAClose,
				abortName: importTestDGUTAAbort,
				calls:     &calls,
				closeErr:  errImportTestDGUTA,
			},
		)

		err := closer(true)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestDGUTA.Error())
		So(calls, ShouldResemble, []string{
			importTestFilesClose,
			importTestBasedirsClose,
			importTestDGUTAClose,
			importTestBasedirsAbort,
		})
	})

	Convey("composeImportCloser joins cleanup errors", t, func() {
		err := composeImportCloser(
			orderedCloser{name: importTestFilesClose, err: errImportTestFiles},
			func(bool) error { return errImportTestBasedirs },
			abortTrackingCloser{abortErr: errImportTestDGUTA},
		)(false)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errImportTestFiles.Error())
		So(err.Error(), ShouldContainSubstring, errImportTestBasedirs.Error())
		So(err.Error(), ShouldContainSubstring, errImportTestDGUTA.Error())
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

func TestImportJ3AggregateEvidence(t *testing.T) {
	Convey("addImportFinalGateEvidence records J3 import and storage aggregate inputs", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		report.TableStats = map[string]perfreport.TableStats{
			tableFiles: {
				Rows:        11,
				ActiveParts: 2,
			},
			tableCatalog: {
				Rows:        7,
				ActiveParts: 3,
			},
			tableDirSummary: {
				Rows:        5,
				ActiveParts: 1,
			},
		}
		report.AddOperation("import_total", map[string]any{}, []float64{125})

		results := []datasetImportResult{
			{
				rows: map[string]uint64{
					tableFiles:   11,
					tableDGUTA:   2,
					tableCatalog: 7,
				},
				pathTextBytesBefore: 1_000,
				pathTextBytesAfter:  250,
			},
			{
				rows: map[string]uint64{
					tableFiles:   3,
					tableCatalog: 4,
				},
				pathTextBytesBefore: 50,
				pathTextBytesAfter:  20,
			},
		}

		addImportFinalGateEvidence(&report, results, importCPUUsage{userMS: 12, systemMS: 8})

		total := findImportOperation(report.Operations, "import_total", "")
		So(total, ShouldNotBeNil)
		So(total.Inputs[importInputRowsPerTable], ShouldResemble, map[string]uint64{
			tableFiles:      11,
			tableCatalog:    7,
			tableDirSummary: 5,
		})
		So(total.Inputs[importInputMaxDirsPerSnapshot], ShouldEqual, uint64(7))
		So(total.Inputs[importInputDirIDUInt32Justified], ShouldEqual, true)
		So(total.Inputs[importInputDirIDUInt32WarningThreshold], ShouldEqual, uint64(1<<31))
		So(total.Inputs[importInputPathTextBytesBefore], ShouldEqual, uint64(1_050))
		So(total.Inputs[importInputPathTextBytesAfter], ShouldEqual, uint64(270))
		So(total.Inputs[importInputPathTextBytesReduction], ShouldEqual, uint64(780))
		So(total.Inputs[importInputPathTextBytesReductionPct], ShouldAlmostEqual, 74.28571428571429)
	})

	Convey("addImportFinalGateEvidence warns when max dirs approach the UInt32 widening threshold", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		report.AddOperation("import_total", map[string]any{}, []float64{1})

		addImportFinalGateEvidence(&report, []datasetImportResult{
			{rows: map[string]uint64{tableCatalog: 1 << 31}},
		}, importCPUUsage{})

		total := findImportOperation(report.Operations, "import_total", "")
		So(total.Inputs[importInputMaxDirsPerSnapshot], ShouldEqual, uint64(1<<31))
		So(total.Inputs[importInputDirIDUInt32Justified], ShouldEqual, false)
	})
}

func TestImportReportOperations(t *testing.T) {
	Convey("addImportReportOperations emits per-file and per-phase detail", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   importTestDataset,
			statsPath: importTestStatsPath,
			mountPath: importTestMountScratch,
			lines:     42,
			elapsed:   1500 * time.Millisecond,
			rows: map[string]uint64{
				tableFiles:                42,
				tableDGUTA:                7,
				tableCatalog:              5,
				tableDirFacts:             7,
				tableBasedirsGroupUsage:   3,
				tableBasedirsUserUsage:    2,
				tableBasedirsGroupSubdirs: 4,
			},
			phases: map[string]time.Duration{
				expectedPhasePartitionDropReset: 160 * time.Millisecond,
				phaseFilesInsert:                500 * time.Millisecond,
				phaseFilesFlush:                 50 * time.Millisecond,
				expectedPhaseDGUTAInsert:        200 * time.Millisecond,
				expectedPhaseCatalogInsert:      100 * time.Millisecond,
				expectedPhaseDirFactsInsert:     110 * time.Millisecond,
				phaseDirProjectionWrite:         90 * time.Millisecond,
				expectedPhaseMountSwitch:        120 * time.Millisecond,
				expectedPhaseOldSnapshotDrop:    80 * time.Millisecond,
				phaseBasedirsReset:              100 * time.Millisecond,
				phaseBasedirsGroupUsage:         75 * time.Millisecond,
				phaseBasedirsFinalise:           25 * time.Millisecond,
				phaseBasedirsFlush:              10 * time.Millisecond,
				phaseActivePrefixRefresh:        60 * time.Millisecond,
			},
		}

		addImportReportOperations(&report, []datasetImportResult{result}, 2, 2*time.Second, 100_000)

		So(report.Operations, ShouldHaveLength, 20)

		fileTotal := findImportOperation(report.Operations, "import_file_total", "")
		So(fileTotal, ShouldNotBeNil)
		So(fileTotal.Inputs["dataset"], ShouldEqual, result.dataset)
		So(fileTotal.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(fileTotal.Inputs["lines"], ShouldEqual, result.lines)
		So(fileTotal.Inputs[importInputRowCap], ShouldEqual, uint64(100_000))

		rows, ok := fileTotal.Inputs["rows_per_table"].(map[string]uint64)
		So(ok, ShouldBeTrue)
		So(rows, ShouldResemble, map[string]uint64{
			tableFiles:                42,
			tableDirSummary:           14,
			tableCatalog:              5,
			tableBasedirsGroupUsage:   3,
			tableBasedirsUserUsage:    2,
			tableBasedirsGroupSubdirs: 4,
		})

		partitionReset := findImportOperation(report.Operations, "import_phase", expectedPhasePartitionDropReset)
		So(partitionReset, ShouldNotBeNil)

		tables, ok := partitionReset.Inputs["tables"].([]string)
		So(ok, ShouldBeTrue)
		So(tables, ShouldResemble, []string{
			tableDirSummary,
			tableCatalog,
			tableFiles,
			tableDirSummarySets,
			tableBasedirsGroupUsage,
			tableBasedirsUserUsage,
			tableBasedirsGroupSubdirs,
			tableBasedirsUserSubdirs,
			tableDirFilterAgeAll,
			tableChildFilterAll,
			tableDirFilterAll,
			tableSchema3SnapshotSets,
			tableActiveVirtualDirs,
			tableActiveVirtualSummaries,
			tableActiveVirtualFilterAll,
			tableActiveVirtualChildren,
			tableActiveVirtualSets,
		})
		So(partitionReset.DurationsMS, ShouldResemble, []float64{160})

		dirProjectionWrite := findImportOperation(report.Operations, "import_phase", phaseDirProjectionWrite)
		So(dirProjectionWrite, ShouldNotBeNil)
		So(dirProjectionWrite.Inputs["tables"], ShouldResemble,
			[]string{tableDirSummary, tableDirSummarySets, tableDirFilterAgeAll})
		So(dirProjectionWrite.DurationsMS, ShouldResemble, []float64{90})

		activePrefixRefresh := findImportOperation(report.Operations, "import_phase", phaseActivePrefixRefresh)
		So(activePrefixRefresh, ShouldNotBeNil)
		So(activePrefixRefresh.Inputs["tables"], ShouldResemble,
			[]string{tableActivePrefixRollups, tableActivePrefixFilterAgeAll, tableActivePrefixRollupSets})
		So(activePrefixRefresh.DurationsMS, ShouldResemble, []float64{60})

		filesInsert := findImportOperation(report.Operations, "import_phase", phaseFilesInsert)
		So(filesInsert, ShouldNotBeNil)
		So(filesInsert.Inputs["dataset"], ShouldEqual, result.dataset)
		So(filesInsert.Inputs["phase"], ShouldEqual, phaseFilesInsert)
		So(filesInsert.Inputs["rows"], ShouldEqual, uint64(42))
		So(filesInsert.DurationsMS, ShouldResemble, []float64{500})

		dgutaInsert := findImportOperation(report.Operations, "import_phase", expectedPhaseDGUTAInsert)
		So(dgutaInsert, ShouldNotBeNil)
		So(dgutaInsert.Inputs["table"], ShouldEqual, tableDirSummary)
		So(dgutaInsert.Inputs["rows"], ShouldEqual, uint64(7))
		So(dgutaInsert.DurationsMS, ShouldResemble, []float64{200})

		catalogInsert := findImportOperation(report.Operations, "import_phase", expectedPhaseCatalogInsert)
		So(catalogInsert, ShouldNotBeNil)
		So(catalogInsert.Inputs["table"], ShouldEqual, tableCatalog)
		So(catalogInsert.Inputs["rows"], ShouldEqual, uint64(5))
		So(catalogInsert.DurationsMS, ShouldResemble, []float64{100})

		dirFactsInsert := findImportOperation(report.Operations, "import_phase", expectedPhaseDirFactsInsert)
		So(dirFactsInsert, ShouldNotBeNil)
		So(dirFactsInsert.Inputs["table"], ShouldEqual, tableDirFacts)
		So(dirFactsInsert.Inputs["rows"], ShouldEqual, uint64(7))
		So(dirFactsInsert.DurationsMS, ShouldResemble, []float64{110})

		mountSwitch := findImportOperation(report.Operations, "import_phase", expectedPhaseMountSwitch)
		So(mountSwitch, ShouldNotBeNil)
		So(mountSwitch.DurationsMS, ShouldResemble, []float64{120})

		oldSnapshotDrop := findImportOperation(report.Operations, "import_phase", expectedPhaseOldSnapshotDrop)
		So(oldSnapshotDrop, ShouldNotBeNil)
		So(oldSnapshotDrop.DurationsMS, ShouldResemble, []float64{80})

		total := findImportOperation(report.Operations, "import_total", "")
		So(total, ShouldNotBeNil)
		So(total.Inputs["datasets"], ShouldEqual, 1)
		So(total.Inputs[importInputRecords], ShouldEqual, uint64(42))
		So(total.Inputs[importInputRowCap], ShouldEqual, uint64(100_000))
		So(total.Inputs["parallelism"], ShouldEqual, 2)
		So(total.Inputs["mode"], ShouldEqual, "parallel")
		So(total.Inputs["throughput_records_per_sec"], ShouldEqual, 21.0)
	})
}

func TestImportGuardrailOperations(t *testing.T) {
	Convey("addImportReportOperations emits observed import guardrails with required metadata", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   importTestDataset,
			statsPath: importTestStatsPath,
			mountPath: importTestMountScratch,
			lines:     42,
			elapsed:   1500 * time.Millisecond,
			rows: map[string]uint64{
				tableFiles: 42,
			},
			phases: map[string]time.Duration{
				phaseFilesInsert:        500 * time.Millisecond,
				phaseFilesFlush:         50 * time.Millisecond,
				phaseDirProjectionWrite: 90 * time.Millisecond,
				phaseMountSwitch:        120 * time.Millisecond,
				phaseTreeSummaryRefresh: 70 * time.Millisecond,
			},
		}

		addImportReportOperations(&report, []datasetImportResult{result}, 1, 2*time.Second, 0)

		rawIngest := findImportGuardrailOperation(report.Operations, "raw_file_ingest")
		So(rawIngest, ShouldNotBeNil)
		So(rawIngest.Inputs["dataset"], ShouldEqual, result.dataset)
		So(rawIngest.Inputs["stats_path"], ShouldEqual, result.statsPath)
		So(rawIngest.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(rawIngest.Inputs["status"], ShouldEqual, "observed")
		So(rawIngest.Inputs["table"], ShouldEqual, tableFiles)
		So(rawIngest.Inputs["rows"], ShouldEqual, uint64(42))
		So(rawIngest.Inputs["lines"], ShouldEqual, uint64(42))
		So(rawIngest.Inputs["throughput_records_per_sec"], ShouldEqual, 28.0)
		So(rawIngest.DurationsMS, ShouldResemble, []float64{1500})

		mountSwitch := findImportGuardrailOperation(report.Operations, "active_snapshot_publish")
		So(mountSwitch, ShouldNotBeNil)
		So(mountSwitch.Inputs["dataset"], ShouldEqual, result.dataset)
		So(mountSwitch.Inputs["stats_path"], ShouldEqual, result.statsPath)
		So(mountSwitch.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(mountSwitch.Inputs["status"], ShouldEqual, "observed")
		So(mountSwitch.Inputs["phase"], ShouldEqual, phaseMountSwitch)
		So(mountSwitch.DurationsMS, ShouldResemble, []float64{120})

		dirProjection := findImportGuardrailOperation(report.Operations, "maintained_dir_projection")
		So(dirProjection, ShouldNotBeNil)
		So(dirProjection.Inputs["dataset"], ShouldEqual, result.dataset)
		So(dirProjection.Inputs["stats_path"], ShouldEqual, result.statsPath)
		So(dirProjection.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(dirProjection.Inputs["status"], ShouldEqual, "observed")
		So(dirProjection.Inputs["phase"], ShouldEqual, phaseDirProjectionWrite)
		So(dirProjection.Inputs["tables"], ShouldResemble,
			[]string{tableDirSummary, tableDirSummarySets, tableDirFilterAgeAll})
		So(dirProjection.DurationsMS, ShouldResemble, []float64{90})

		treeSummary := findImportGuardrailOperation(report.Operations, "active_tree_summary_refresh")
		So(treeSummary, ShouldNotBeNil)
		So(treeSummary.Inputs["dataset"], ShouldEqual, result.dataset)
		So(treeSummary.Inputs["stats_path"], ShouldEqual, result.statsPath)
		So(treeSummary.Inputs["mount_path"], ShouldEqual, result.mountPath)
		So(treeSummary.Inputs["status"], ShouldEqual, "observed")
		So(treeSummary.Inputs["phase"], ShouldEqual, phaseTreeSummaryRefresh)
		So(treeSummary.Inputs["tables"], ShouldResemble,
			[]string{tableTreeSummarySets, tableTreeDGUTA, tableTreeDirSummary, tableTreeChildren})
		So(treeSummary.DurationsMS, ShouldResemble, []float64{70})
	})

	Convey("addImportReportOperations emits missing import guardrails instead of hiding absent phases", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   importTestDataset,
			statsPath: importTestStatsPath,
			mountPath: importTestMountScratch,
			rows:      map[string]uint64{},
			phases:    map[string]time.Duration{},
		}

		addImportReportOperations(&report, []datasetImportResult{result}, 1, time.Second, 0)

		rawIngest := findImportGuardrailOperation(report.Operations, "raw_file_ingest")
		So(rawIngest, ShouldNotBeNil)
		So(rawIngest.Inputs["status"], ShouldEqual, "missing")
		So(rawIngest.Inputs["rows"], ShouldEqual, uint64(0))
		So(rawIngest.Inputs["lines"], ShouldEqual, uint64(0))
		So(rawIngest.DurationsMS, ShouldResemble, []float64{0})

		mountSwitch := findImportGuardrailOperation(report.Operations, "active_snapshot_publish")
		So(mountSwitch, ShouldNotBeNil)
		So(mountSwitch.Inputs["status"], ShouldEqual, "missing")
		So(mountSwitch.Inputs["phase"], ShouldEqual, phaseMountSwitch)
		So(mountSwitch.DurationsMS, ShouldResemble, []float64{0})

		dirProjection := findImportGuardrailOperation(report.Operations, "maintained_dir_projection")
		So(dirProjection, ShouldNotBeNil)
		So(dirProjection.Inputs["status"], ShouldEqual, "missing")
		So(dirProjection.Inputs["phase"], ShouldEqual, phaseDirProjectionWrite)
		So(dirProjection.Inputs["tables"], ShouldResemble,
			[]string{tableDirSummary, tableDirSummarySets, tableDirFilterAgeAll})
		So(dirProjection.DurationsMS, ShouldResemble, []float64{0})

		treeSummary := findImportGuardrailOperation(report.Operations, "active_tree_summary_refresh")
		So(treeSummary, ShouldNotBeNil)
		So(treeSummary.Inputs["status"], ShouldEqual, "missing")
		So(treeSummary.Inputs["phase"], ShouldEqual, phaseTreeSummaryRefresh)
		So(treeSummary.Inputs["tables"], ShouldResemble,
			[]string{tableTreeSummarySets, tableTreeDGUTA, tableTreeDirSummary, tableTreeChildren})
		So(treeSummary.DurationsMS, ShouldResemble, []float64{0})
	})

	Convey("raw file ingest remains missing when only parser lines were counted", t, func() {
		report := boltperf.NewReport("clickhouse", "/input", 1, 0)
		result := datasetImportResult{
			dataset:   importTestDataset,
			statsPath: importTestStatsPath,
			mountPath: importTestMountScratch,
			lines:     42,
			elapsed:   1500 * time.Millisecond,
			rows:      map[string]uint64{},
			phases:    map[string]time.Duration{},
		}

		addImportReportOperations(&report, []datasetImportResult{result}, 1, time.Second, 0)

		rawIngest := findImportGuardrailOperation(report.Operations, "raw_file_ingest")
		So(rawIngest, ShouldNotBeNil)
		So(rawIngest.Inputs["status"], ShouldEqual, "missing")
		So(rawIngest.Inputs["rows"], ShouldEqual, uint64(0))
		So(rawIngest.Inputs["lines"], ShouldEqual, uint64(42))
		So(rawIngest.DurationsMS, ShouldResemble, []float64{0})
	})
}

func findImportGuardrailOperation(ops []boltperf.Operation, guardrail string) *boltperf.Operation {
	for i := range ops {
		if ops[i].Name != "import_guardrail" {
			continue
		}

		if ops[i].Inputs["guardrail"] == guardrail {
			return &ops[i]
		}
	}

	return nil
}
