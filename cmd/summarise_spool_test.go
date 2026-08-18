/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirbuild"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const summariseSpoolVirtualNamespaceDir = "/mnt/"

const b5SpoolMountPath = "/m/teamX/"

const summariseSpoolOldCompressedSizeThreshold int64 = 32 * 1024 * 1024

const reinsertOldDirFactSentinelQuery = `
INSERT INTO wrstat_dir_facts
SELECT
  mount_path,
  toUUID(?),
  dir_id,
  parent_id,
  subtree_end,
  updated_at,
  all_count,
  all_size,
  all_atime_min,
  all_mtime_max,
  all_atime_buckets,
  all_mtime_buckets,
  all_uids,
  all_gids,
  all_ft,
  file_count,
  file_size,
  file_atime_min,
  file_mtime_max,
  file_atime_buckets,
  file_mtime_buckets,
  file_uids,
  file_gids,
  file_ft,
  gids,
  uids,
  fts,
  ages,
  counts,
  sizes,
  atime_mins,
  mtime_maxs,
  atime_buckets,
  mtime_buckets,
  child_count,
  refreshed_at
FROM wrstat_dir_facts
WHERE mount_path = ? AND snapshot_id = toUUID(?)
LIMIT 1`

const reinsertOldActiveVirtualSetSentinelQuery = `
INSERT INTO wrstat_active_virtual_sets
SELECT
  ?,
  schema3_version,
  ?,
  active_mount_count,
  summary_rows,
  filter_rows,
  child_rows,
  manifest_sha256,
  ready,
  refreshed_at
FROM wrstat_active_virtual_sets
WHERE active_set_id = ?
LIMIT 1`

const insertMountActiveSnapshotEventForTestQuery = `
INSERT INTO wrstat_mount_events
  (mount_path, event_at, event_type, snapshot_id, updated_at, reason)
VALUES (?, ?, 1, toUUID(?), ?, ?)`

func TestSummariseFileIngestDirIDPath(t *testing.T) {
	Convey("summarise file spool resolves / mount top-level directory rows against /", t, func() {
		rootDir := &summary.DirectoryPath{Name: "/", Depth: 0}
		info := &summary.FileInfo{
			Path:      rootDir,
			Name:      []byte("boot/"),
			EntryType: stats.DirType,
		}

		dirIDPath := summariseFileIngestDirIDPath(info)

		So(dirIDPath, ShouldEqual, rootDir)

		alloc := summary.NewDirIDAllocator()
		So(alloc.SetMountPath("/"), ShouldBeNil)

		dirID, err := alloc.DirID(dirIDPath)
		So(err, ShouldBeNil)
		So(dirID, ShouldEqual, uint32(0))
	})
}

type summariseCountingReadCloser struct {
	io.Reader
	bytesRead *int
}

func (r *summariseCountingReadCloser) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	*r.bytesRead += n

	return n, err
}

func (r *summariseCountingReadCloser) Close() error {
	return nil
}

func writeEarlyNonContiguousSpoolFixtureStats(
	t *testing.T,
	statsPath string,
	updatedAt time.Time,
	tailDirs int,
) []byte {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/one.dat", 'f', 10, 12, 22,
		updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/implied/deep/file.dat", 'f', 11, 12, 22,
		updatedAt.Unix(), 307, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/two.dat", 'f', 20, 14, 24,
		updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/", 'd', 4096, 15, 25,
		updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/three.txt", 'f', 30, 16, 26,
		updatedAt.Unix(), 306, 1)

	for idx := range tailDirs {
		dirPath := fmt.Sprintf("%stail/dir%05d/", summariseTestMountPath, idx)
		writeSpoolFixtureStatsRow(&buf, dirPath, 'd', 4096, 17, 27, updatedAt.Unix(), uint64(1_000+idx*2), 1)
		writeSpoolFixtureStatsRow(&buf, dirPath+"file.dat", 'f', int64(idx+1), 18, 28,
			updatedAt.Unix(), uint64(1_001+idx*2), 1)
	}

	rawStats := bytes.Clone(buf.Bytes())
	writeGzipStats(t, statsPath, rawStats)
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)

	return rawStats
}

func writeLargeContiguousSpoolFixtureStats(
	t *testing.T,
	statsPath string,
	updatedAt time.Time,
	dirRows int,
) {
	t.Helper()

	root := statsdata.NewRoot(summariseTestMountPath, updatedAt.Unix())
	for dirIndex := range dirRows {
		dir := root.AddDirectory(fmt.Sprintf("project%05d", dirIndex))
		file := dir.AddFile("file.dat")
		file.Size = int64(1 + dirIndex%17)
		file.UID = uint32(10 + dirIndex%11)
		file.GID = uint32(20 + dirIndex%13)
		file.ATime = updatedAt.Unix()
		file.MTime = updatedAt.Unix()
		file.CTime = updatedAt.Unix()
		file.Inode = uint64(10_000_001 + dirIndex*2)
		file.Nlink = 1
	}

	var buf bytes.Buffer

	_, err := root.WriteTo(&buf)
	So(err, ShouldBeNil)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func padSummariseSpoolFixturePastOldThreshold(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	So(os.Truncate(statsPath, summariseSpoolOldCompressedSizeThreshold+1), ShouldBeNil)
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func startSummariseHeapInuseSampler(baseline uint64, interval time.Duration) func() uint64 {
	done := make(chan struct{})
	peak := make(chan uint64, 1)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		maxGrowth := uint64(0)
		sample := func() {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)

			maxGrowth = max(maxGrowth, summariseHeapInuseGrowth(baseline, mem.HeapInuse))
		}

		sample()

		for {
			select {
			case <-ticker.C:
				sample()
			case <-done:
				sample()

				peak <- maxGrowth

				return
			}
		}
	}()

	return func() uint64 {
		close(done)

		return <-peak
	}
}

func summariseHeapInuseGrowth(before uint64, after uint64) uint64 {
	if after > before {
		return after - before
	}

	return 0
}

func writeNestedFullFilterSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"zz_unordered/", 'd', 4096, 17, 7,
		updatedAt.Unix(), 306, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/", 'd', 4096, 17, 7,
		updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/alpha/", 'd', 4096, 17, 7,
		updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/alpha/one.bam", 'f', 10, 17, 7,
		updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/beta/", 'd', 4096, 17, 7,
		updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/beta/two.bam", 'f', 20, 17, 7,
		updatedAt.Unix(), 305, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func d2DirFilterRowsByDirPathAndTuple(
	spoolDir string,
	fullPath string,
) map[summariseFullFilterTupleKey]chspool.DirFilterAllRow {
	out := make(map[summariseFullFilterTupleKey]chspool.DirFilterAllRow)
	dirID := d2DirIDForPath(spoolDir, fullPath)

	So(chspool.DecodeRows[chspool.DirFilterAllRow](
		spoolDir,
		chspool.TableDirFilterAll,
		func(row chspool.DirFilterAllRow) error {
			if row.DirID == dirID {
				out[summariseFullFilterKeyForRow(row)] = row
			}

			return nil
		},
	), ShouldBeNil)
	So(len(out), ShouldBeGreaterThan, 0)

	return out
}

func summariseSpoolHasDirbuildScratchForTest(t *testing.T, spoolDir string) bool {
	t.Helper()

	hasScratch := false
	err := filepath.WalkDir(spoolDir, func(path string, entry os.DirEntry, err error) error {
		So(err, ShouldBeNil)

		name := entry.Name()
		if strings.HasPrefix(name, "wrstat-dirbuild-summary-") || name == summariseSQLiteScratchName {
			hasScratch = true
		}

		return nil
	})
	So(err, ShouldBeNil)

	return hasScratch
}

func writeHighFanoutNonContiguousSpoolFixtureStats(
	t *testing.T,
	statsPath string,
	updatedAt time.Time,
	childDirs int,
	tuplesPerChild int,
) {
	t.Helper()

	var buf bytes.Buffer

	writeRawStatsFixtureLine(
		&buf, summariseTestMountPath+"zz_non_contiguous_probe/",
		0, 7, 7, updatedAt, stats.DirType, 10, 2,
	)

	nextInode := int64(100)

	for childIdx := range childDirs {
		dirPath := fmt.Sprintf("%sproject%05d/", summariseTestMountPath, childIdx)
		writeRawStatsFixtureLine(&buf, dirPath, 0, 7, 7, updatedAt, stats.DirType, nextInode, 2)
		nextInode++

		for tupleIdx := range tuplesPerChild {
			filePath := fmt.Sprintf("%sfile%02d.dat", dirPath, tupleIdx)
			writeRawStatsFixtureLine(
				&buf,
				filePath,
				int64(1+tupleIdx),
				uint32(1000+tupleIdx),
				uint32(2000+tupleIdx%4),
				updatedAt,
				stats.FileType,
				nextInode,
				1,
			)
			nextInode++
		}
	}

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeRawStatsFixtureLine(
	buf *bytes.Buffer,
	path string,
	size int64,
	uid uint32,
	gid uint32,
	updatedAt time.Time,
	entryType byte,
	inode int64,
	nlink int64,
) {
	fmt.Fprintf(
		buf,
		"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t%d\t0\t%d\n",
		path,
		size,
		uid,
		gid,
		updatedAt.Unix(),
		updatedAt.Unix(),
		updatedAt.Unix(),
		entryType,
		inode,
		nlink,
		size,
	)
}

type failingDirbuildError struct{}

func (failingDirbuildError) Error() string {
	return "forced dirbuild emission failure"
}

type failingDirbuildDatabase struct{}

func (failingDirbuildDatabase) Add(db.RecordDGUTA) error {
	return failingDirbuildError{}
}

type summariseSpoolSwitchPlanForTest struct {
	HasPrevious         bool   `json:"has_previous"`
	PreviousSnapshotID  string `json:"previous_snapshot_id"`
	PreviousActiveSetID string `json:"previous_active_set_id"`
	NextActiveSetID     string `json:"next_active_set_id"`
}

func rewriteSummariseSpoolStateToPostSwitchCrash(
	t *testing.T,
	statePath string,
) summariseSpoolSwitchPlanForTest {
	t.Helper()

	var state map[string]any

	data, err := os.ReadFile(statePath)
	So(err, ShouldBeNil)
	So(json.Unmarshal(data, &state), ShouldBeNil)

	plan := summariseSpoolSwitchPlanFromStateForTest(t, state)
	So(plan.HasPrevious, ShouldBeTrue)

	phases, ok := state["completed_phases"].(map[string]any)
	So(ok, ShouldBeTrue)

	for _, phase := range []string{
		"mount_switched",
		"old_snapshot_dropped",
		"old_active_virtual_dropped",
		"tree_summary_refreshed",
		"active_prefix_refreshed",
		"post_spool_publish_complete",
	} {
		delete(phases, phase)
	}

	data, err = json.MarshalIndent(state, "", "  ")
	So(err, ShouldBeNil)

	data = append(data, '\n')
	So(os.WriteFile(statePath, data, 0o600), ShouldBeNil)

	return plan
}

func summariseSpoolSwitchPlanFromStateForTest(
	t *testing.T,
	state map[string]any,
) summariseSpoolSwitchPlanForTest {
	t.Helper()

	raw, err := json.Marshal(state["switch_plan"])
	So(err, ShouldBeNil)

	var plan summariseSpoolSwitchPlanForTest
	So(json.Unmarshal(raw, &plan), ShouldBeNil)

	return plan
}

func TestSummariseClickHouseSpoolRetry(t *testing.T) {
	Convey("summarise retry reuses a completed spool after ClickHouse publish fails", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
			So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThan, uint64(0))

			if loadCalls == 1 {
				return perfreport.Report{}, errSummariseTestClose
			}

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		err = run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise retry rebuilds a completed spool with an old schema marker", t, func() {
		const previousClickHouseSpoolSchemaMark = "wrstat-ui-clickhouse-summarise-spool-v2"

		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
			So(manifest.SchemaMarker, ShouldEqual, clickHouseSpoolSchemaMark)

			if loadCalls == 1 {
				return perfreport.Report{}, errSummariseTestClose
			}

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		manifest.SchemaMarker = previousClickHouseSpoolSchemaMark
		So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		err = run([]string{fixture.statsPath})
		So(err, ShouldNotBeNil)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil)

		err = run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)

		manifest, err = chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)
		So(manifest.SchemaMarker, ShouldEqual, clickHouseSpoolSchemaMark)
		So(manifest.SchemaMarker, ShouldNotEqual, previousClickHouseSpoolSchemaMark)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise retry can fail twice then succeed without rereading stats.gz", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
			So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThan, uint64(0))

			if loadCalls <= 2 {
				return perfreport.Report{}, errSummariseTestClose
			}

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		err = run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 2)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		err = run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(loadCalls, ShouldEqual, 3)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise rebuilds instead of loading a corrupt completed spool", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadCalls := 0
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++

			return perfreport.Report{}, errSummariseTestClose
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)

		manifestPath := chspool.ManifestPath(summariseClickHouseSpoolDir(fixture.outputDir))
		So(os.WriteFile(manifestPath, []byte(`{"state":"complete","mount_path":"wrong"}`), 0o600), ShouldBeNil)
		writeGzipStats(t, fixture.statsPath, []byte("not a valid wrstat row\n"))
		So(os.Chtimes(fixture.statsPath, fixture.updatedAt, fixture.updatedAt), ShouldBeNil)

		err = run([]string{fixture.statsPath})
		So(err, ShouldNotBeNil)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)
	})

	Convey("summarise retry rebuilds stale pre-switch active virtual readiness after another mount publishes", t, func() {
		const otherMountPath = "/mnt/other/"

		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()
		cfg.QueryTimeout = 10 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{summariseTestMountPath, otherMountPath}

		baseDir := t.TempDir()
		oldUpdatedAt := time.Unix(1_710_000_000, 0).UTC()
		updatedAt := oldUpdatedAt.Add(time.Hour)
		otherUpdatedAt := updatedAt.Add(30 * time.Minute)

		fixture := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)
		otherFixture := newSummariseActiveSnapshotFixtureForMount(t, baseDir, otherMountPath, otherUpdatedAt)

		sharedDir := t.TempDir()
		quotaFile := filepath.Join(sharedDir, "quota.csv")
		basedirsFile := filepath.Join(sharedDir, "basedirs.tsv")
		mountsFile := filepath.Join(sharedDir, "mounts.txt")

		So(os.WriteFile(quotaFile, []byte("7,/mnt/test,1000,100\n7,/mnt/other,1000,100\n"), 0o600),
			ShouldBeNil)
		So(os.WriteFile(basedirsFile, []byte("/mnt/test\t1\t3\n/mnt/other\t1\t3\n"), 0o600),
			ShouldBeNil)
		So(os.WriteFile(mountsFile, []byte("\"/mnt/test/\"\n\"/mnt/other/\"\n"), 0o600), ShouldBeNil)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		summariseSpoolNow = func() time.Time {
			return updatedAt.Add(2 * time.Hour)
		}
		summariseSpoolDirGUTANow = d2Schema3DirGUTAReferenceTime

		configureSummariseSpoolRetryFixture(fixture, cfg, quotaFile, basedirsFile, mountsFile)
		writeBasedirsSpoolFixtureStatsForMount(t, fixture.statsPath, summariseTestMountPath, oldUpdatedAt)
		So(run([]string{fixture.statsPath}), ShouldBeNil)

		oldSnapshotID := clickhouse.SnapshotID(summariseTestMountPath, oldUpdatedAt)
		writeBasedirsSpoolFixtureStatsForMount(t, fixture.statsPath, summariseTestMountPath, updatedAt)
		So(run([]string{fixture.statsPath}), ShouldBeNil)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		statePath := filepath.Join(spoolDir, "post_spool_publish_state.json")
		plan := rewriteSummariseSpoolStateToPostSwitchCrash(t, statePath)
		So(plan.PreviousSnapshotID, ShouldEqual, oldSnapshotID)
		So(plan.PreviousActiveSetID, ShouldNotBeBlank)
		So(plan.NextActiveSetID, ShouldNotBeBlank)
		So(os.Remove(summariseCompletionMarkerPath(fixture.outputDir)), ShouldBeNil)

		conn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer verifyCancel()

		insertMountActiveSnapshotEventForTest(
			t,
			verifyCtx,
			conn,
			summariseTestMountPath,
			time.Now().UTC().Add(time.Second),
			oldSnapshotID,
			oldUpdatedAt,
			"pre-switch crash rollback",
		)

		configureSummariseSpoolRetryFixture(otherFixture, cfg, quotaFile, basedirsFile, mountsFile)
		writeBasedirsSpoolFixtureStatsForMount(t, otherFixture.statsPath, otherMountPath, otherUpdatedAt)
		So(run([]string{otherFixture.statsPath}), ShouldBeNil)

		otherSnapshotID := clickhouse.SnapshotID(otherMountPath, otherUpdatedAt)
		combinedActiveSetID := d2ExpectedActiveSetIDForRows([]summariseActiveSetRowForTest{
			{mountPath: otherMountPath, snapshotID: otherSnapshotID, updatedAt: otherUpdatedAt},
			{mountPath: summariseTestMountPath, snapshotID: manifest.SnapshotID, updatedAt: updatedAt},
		})
		So(combinedActiveSetID, ShouldNotEqual, plan.NextActiveSetID)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		configureSummariseSpoolRetryFixture(fixture, cfg, quotaFile, basedirsFile, mountsFile)
		So(run([]string{fixture.statsPath}), ShouldBeNil)

		So(countActiveVirtualSetRows(verifyCtx, conn, combinedActiveSetID), ShouldEqual, uint64(1))
		So(activeVirtualSetMountCountForTest(verifyCtx, conn, combinedActiveSetID), ShouldEqual, uint64(2))
		So(d3CountRows(
			verifyCtx,
			conn,
			"SELECT count() FROM wrstat_active_virtual_summaries WHERE active_set_id = ?",
			combinedActiveSetID,
		), ShouldBeGreaterThan, uint64(0))
	})

	Convey("summarise retry resumes post-spool publish phases without rereading stats.gz", t, func() {
		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()

		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		configureSummariseActiveSnapshotTest(fixture.outputDir, true)

		clickhouseDSN = cfg.DSN
		clickhouseDatabase = cfg.Database
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}
		dirgutaReferenceAt := d2Schema3DirGUTAReferenceTime()
		summariseSpoolDirGUTANow = func() time.Time {
			return dirgutaReferenceAt
		}

		loadCalls := 0
		secondRunMountSwitches := 0
		loadSummariseClickHouseSpool = func(
			ctx context.Context,
			cfg clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			recorder func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++
			loadCtx := ctx

			cancel := func() {}
			if loadCalls == 1 {
				loadCtx, cancel = context.WithCancel(ctx)
			}
			defer cancel()

			wrappedRecorder := func(phase string, duration time.Duration) {
				recorder(phase, duration)

				if loadCalls == 1 && phase == "mount_switch" {
					cancel()

					return
				}

				if loadCalls == 2 && phase == "mount_switch" {
					secondRunMountSwitches++
				}
			}

			return clickhouse.LoadSummariseSpoolReport(loadCtx, cfg, spoolDir, manifest, wrappedRecorder)
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, context.Canceled), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)
		So(secondRunMountSwitches, ShouldEqual, 0)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		report := readSummariseSpoolLoadReport(t, spoolDir)
		So(report.TableStats[chspool.TableDirFacts].Rows, ShouldBeGreaterThan, uint64(0))

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise retry resumes after a mount switch crash using the stored switch plan", t, func() {
		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()

		fixture := newSummariseActiveSnapshotFixture(t)
		oldUpdatedAt := fixture.updatedAt.Add(-time.Hour)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, oldUpdatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		configureSummariseActiveSnapshotTest(fixture.outputDir, true)

		clickhouseDSN = cfg.DSN
		clickhouseDatabase = cfg.Database
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}
		dirgutaReferenceAt := d2Schema3DirGUTAReferenceTime()
		summariseSpoolDirGUTANow = func() time.Time {
			return dirgutaReferenceAt
		}

		loadCalls := 0

		var retryPhases []string

		loadSummariseClickHouseSpool = func(
			ctx context.Context,
			cfg clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			recorder func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++
			call := loadCalls
			wrappedRecorder := func(phase string, duration time.Duration) {
				recorder(phase, duration)

				if call == 3 {
					retryPhases = append(retryPhases, phase)
				}
			}

			return clickhouse.LoadSummariseSpoolReport(ctx, cfg, spoolDir, manifest, wrappedRecorder)
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(loadCalls, ShouldEqual, 1)

		oldSnapshotID := clickhouse.SnapshotID(summariseTestMountPath, oldUpdatedAt)

		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)
		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		statePath := filepath.Join(spoolDir, "post_spool_publish_state.json")
		plan := rewriteSummariseSpoolStateToPostSwitchCrash(t, statePath)
		So(plan.PreviousSnapshotID, ShouldEqual, oldSnapshotID)
		So(plan.PreviousActiveSetID, ShouldNotBeBlank)
		So(plan.NextActiveSetID, ShouldNotBeBlank)

		So(os.Remove(summariseCompletionMarkerPath(fixture.outputDir)), ShouldBeNil)

		conn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer verifyCancel()

		reinsertOldPublishSentinels(
			t,
			verifyCtx,
			conn,
			manifest.SnapshotID,
			plan.PreviousSnapshotID,
			plan.PreviousActiveSetID,
			plan.NextActiveSetID,
		)
		So(countDirFactSnapshotRows(verifyCtx, conn, plan.PreviousSnapshotID), ShouldBeGreaterThan, uint64(0))
		So(countActiveVirtualSetRows(verifyCtx, conn, plan.PreviousActiveSetID), ShouldBeGreaterThan, uint64(0))

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		cleanupCalls := 0
		clickHouseCleanActiveSnapshotAttempt = func(clickhouse.Config, string, time.Time) error {
			cleanupCalls++

			return errSummariseTestClose
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(loadCalls, ShouldEqual, 3)
		So(cleanupCalls, ShouldEqual, 0)
		So(summariseSpoolPhaseCount(retryPhases, "mount_switch"), ShouldEqual, 0)
		So(summariseSpoolPhaseCount(retryPhases, "old_snapshot_partition_drop"), ShouldBeGreaterThanOrEqualTo, 2)
		So(summariseSpoolPhaseCount(retryPhases, "wrstat_tree_summary_refresh"), ShouldBeGreaterThanOrEqualTo, 1)
		So(summariseSpoolPhaseCount(retryPhases, "wrstat_active_prefix_rollup_refresh"),
			ShouldBeGreaterThanOrEqualTo, 1)
		So(countDirFactSnapshotRows(verifyCtx, conn, plan.PreviousSnapshotID), ShouldEqual, uint64(0))
		So(countActiveVirtualSetRows(verifyCtx, conn, plan.PreviousActiveSetID), ShouldEqual, uint64(0))

		phases := readSummariseSpoolStatePhasesForTest(t, statePath)
		So(phases["post_spool_publish_complete"], ShouldNotBeBlank)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})
}

func d2DecodedRowFingerprints[T any](spoolDir string, table string) []string {
	rows := []string{}

	So(chspool.DecodeRows[T](spoolDir, table, func(row T) error {
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}

		rows = append(rows, string(data))

		return nil
	}), ShouldBeNil)
	slices.Sort(rows)

	return rows
}

func newSummariseActiveSnapshotFixtureForMount(
	t *testing.T,
	baseDir string,
	mountPath string,
	updatedAt time.Time,
) summariseActiveSnapshotFixture {
	t.Helper()

	mountKey := mountpath.EncodeKey(mountPath)
	outputDir := filepath.Join(baseDir, "12345_"+mountKey)
	So(os.MkdirAll(outputDir, summariseDirPerm), ShouldBeNil)

	statsPath := filepath.Join(baseDir, mountKey+".stats.gz")
	writeGzipStats(t, statsPath, []byte("not a valid wrstat row\n"))
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)

	groupUserPath := filepath.Join(outputDir, "bygroup")
	userGroupPath := filepath.Join(outputDir, "byusergroup.gz")
	groupUserContent := []byte("existing group output\n")
	userGroupContent := []byte("existing user output\n")

	So(os.WriteFile(groupUserPath, groupUserContent, 0o600), ShouldBeNil)
	So(os.WriteFile(userGroupPath, userGroupContent, 0o600), ShouldBeNil)

	return summariseActiveSnapshotFixture{
		outputDir:        outputDir,
		statsPath:        statsPath,
		updatedAt:        updatedAt,
		groupUserPath:    groupUserPath,
		userGroupPath:    userGroupPath,
		groupUserContent: groupUserContent,
		userGroupContent: userGroupContent,
	}
}

func configureSummariseSpoolRetryFixture(
	fixture summariseActiveSnapshotFixture,
	cfg clickhouse.Config,
	quotaFile string,
	basedirsFile string,
	mountsFile string,
) {
	configureSummariseActiveSnapshotTest(fixture.outputDir, true)

	clickhouseDSN = cfg.DSN
	clickhouseDatabase = cfg.Database
	quotaPath = quotaFile
	basedirsConfig = basedirsFile
	basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
	mounts = mountsFile
}

func writeBasedirsSpoolFixtureStatsForMount(
	t *testing.T,
	statsPath string,
	mountPath string,
	updatedAt time.Time,
) {
	t.Helper()

	root := statsdata.NewRoot(mountPath, updatedAt.Unix())
	project := root.AddDirectory("project")
	file := project.AddFile("file.bam")
	file.Size = 50
	file.UID = 17
	file.GID = 7
	file.ATime = updatedAt.Unix()
	file.MTime = updatedAt.Unix()
	file.CTime = updatedAt.Unix()
	file.Inode = 99
	file.Nlink = 1

	var buf bytes.Buffer

	_, err := root.WriteTo(&buf)
	So(err, ShouldBeNil)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func TestSummariseClickHouseSpoolB2Retry(t *testing.T) {
	Convey("B2 completed-spool retry derives child rows without rereading stats.gz", t, func() {
		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()

		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickhouseDSN = cfg.DSN
		clickhouseDatabase = cfg.Database
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}
		dirgutaReferenceAt := d2Schema3DirGUTAReferenceTime()
		summariseSpoolDirGUTANow = func() time.Time {
			return dirgutaReferenceAt
		}

		loadCalls := 0

		var retryPhases []string

		loadSummariseClickHouseSpool = func(
			ctx context.Context,
			cfg clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			recorder func(string, time.Duration),
		) (perfreport.Report, error) {
			loadCalls++
			call := loadCalls
			loadCtx := ctx
			cancel := func() {}

			if call == 1 {
				loadCtx, cancel = context.WithCancel(ctx)
			}

			defer cancel()

			wrappedRecorder := func(phase string, duration time.Duration) {
				if recorder != nil {
					recorder(phase, duration)
				}

				if call == 2 {
					retryPhases = append(retryPhases, phase)
				}

				if call == 1 && phase == "wrstat_child_filter_all_insert" {
					cancel()
				}
			}

			return clickhouse.LoadSummariseSpoolReport(loadCtx, cfg, spoolDir, manifest, wrappedRecorder)
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, context.Canceled), ShouldBeTrue)
		So(loadCalls, ShouldEqual, 1)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		assertChildFilterAllSpoolAbsent(spoolDir)
		assertSummariseSpoolPhaseIncomplete(t, spoolDir, "tables_loaded")

		So(os.Chmod(fixture.statsPath, 0), ShouldBeNil)
		Reset(func() { So(os.Chmod(fixture.statsPath, 0o600), ShouldBeNil) })

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(loadCalls, ShouldEqual, 2)
		So(summariseSpoolPhaseCount(retryPhases, "wrstat_child_filter_all_insert"), ShouldEqual, 1)

		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		dirRows := manifest.Tables[chspool.TableDirFilterAll].Rows
		So(dirRows, ShouldBeGreaterThan, uint64(0))

		report := readSummariseSpoolLoadReport(t, spoolDir)
		loadedRows := summariseSpoolReportUint64MapInputForTest(report, "spool_load_total", "loaded_table_rows")
		So(loadedRows[chspool.TableChildFilterAll], ShouldEqual, dirRows)
		So(report.TableStats[chspool.TableChildFilterAll].Rows, ShouldEqual, dirRows)
		So(report.TableStats[chspool.TableChildFilterAll].ActiveParts, ShouldBeGreaterThan, uint64(0))

		conn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer verifyCancel()

		So(d3CountRows(
			verifyCtx,
			conn,
			"SELECT count() FROM wrstat_child_filter_all WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			manifest.MountPath,
			manifest.SnapshotID,
		), ShouldEqual, dirRows)
	})
}

func TestSummariseClickHouseSpoolB1(t *testing.T) {
	Convey("B1 summarise spool writes dir full-filter as the only canonical full-filter spool file", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		spoolDir, _ := buildSummariseSpoolFixtureForTest(t, fixture)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		dirRows := manifest.Tables[chspool.TableDirFilterAll].Rows
		So(dirRows, ShouldBeGreaterThan, uint64(0))
		So(d2DecodedRowsForTable(spoolDir, chspool.TableDirFilterAll), ShouldEqual, dirRows)

		_, hasChildFilterAll := manifest.Tables[chspool.TableChildFilterAll]
		So(hasChildFilterAll, ShouldBeFalse)

		_, err = os.Stat(filepath.Join(spoolDir, chspool.TableChildFilterAll+".gob.gz"))
		So(errors.Is(err, os.ErrNotExist), ShouldBeTrue)
	})
}

func buildSummariseSpoolFixtureForTest(
	t *testing.T,
	fixture summariseActiveSnapshotFixture,
) (string, *chspool.Manifest) {
	t.Helper()

	target := &clickHouseSummariseTarget{
		cfg:       clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
		mountPath: summariseTestMountPath,
		modtime:   fixture.updatedAt,
		outputDir: fixture.outputDir,
	}
	expected, err := newSummariseSpoolManifest(fixture.statsPath, target)
	So(err, ShouldBeNil)

	spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
	manifest, err := buildSummariseSpool(
		fixture.statsPath,
		spoolDir,
		expected,
		target,
		newSummariseDiagnostics(fixture.statsPath),
	)
	So(err, ShouldBeNil)

	return spoolDir, manifest
}

func readSummariseSpoolBuildReport(t *testing.T, spoolDir string) perfreport.Report {
	t.Helper()

	data, err := os.ReadFile(summariseSpoolBuildReportPath(spoolDir))
	So(err, ShouldBeNil)

	var report perfreport.Report
	So(json.Unmarshal(data, &report), ShouldBeNil)

	return report
}

func summariseSpoolBuildReportOperationForTest(report perfreport.Report) perfreport.Operation {
	for _, op := range report.Operations {
		if op.Name == "summarise_build_total" {
			return op
		}
	}

	So("summarise_build_total", ShouldEqual, "missing")

	return perfreport.Operation{}
}

func uint64InputForTest(inputs map[string]any, key string) uint64 {
	switch value := inputs[key].(type) {
	case float64:
		return uint64(value)
	case uint64:
		return value
	default:
		return 0
	}
}

func writeContiguousSubtreeRevisitSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/one.dat", 'f', 10, 12, 22, updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/", 'd', 4096, 15, 25, updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/three.txt", 'f', 30, 16, 26, updatedAt.Unix(), 306, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/two.dat", 'f', 20, 14, 24, updatedAt.Unix(), 304, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func assertSummariseManifestTableRowsMatch(expected, actual *chspool.Manifest) {
	So(actual, ShouldNotBeNil)
	So(expected, ShouldNotBeNil)
	So(actual.Tables, ShouldHaveLength, len(expected.Tables))

	for table, expectedManifest := range expected.Tables {
		actualManifest, ok := actual.Tables[table]
		So(ok, ShouldBeTrue)
		So(actualManifest.Rows, ShouldEqual, expectedManifest.Rows)
	}
}

func writeNonContiguousBasedirsSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 7, 7, updatedAt.Unix(), 98, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/file.bam", 'f', 50, 17, 7,
		updatedAt.Unix(), 99, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/", 'd', 4096, 7, 7,
		updatedAt.Unix(), 100, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func assertSummariseSpoolDecodedRowsMatch(expectedSpoolDir, actualSpoolDir string) {
	for _, table := range []string{
		chspool.TableDirs,
		chspool.TableFiles,
		chspool.TableDirFacts,
		chspool.TableDirFilterAgeAll,
		chspool.TableDirFilterAll,
		chspool.TableSchema3SnapshotSets,
		chspool.TableActiveVirtualDirs,
		chspool.TableActiveVirtualSummaries,
		chspool.TableActiveVirtualFilterAll,
		chspool.TableActiveVirtualChildren,
		chspool.TableActiveVirtualSets,
		chspool.TableDirProjectionSets,
	} {
		So(
			d2DecodedRowFingerprintsForTable(actualSpoolDir, table),
			ShouldResemble,
			d2DecodedRowFingerprintsForTable(expectedSpoolDir, table),
		)
	}
}

func assertChildFilterAllSpoolAbsent(spoolDir string) {
	manifest, err := chspool.ReadManifest(spoolDir)
	So(err, ShouldBeNil)

	_, hasChildFilterAll := manifest.Tables[chspool.TableChildFilterAll]
	So(hasChildFilterAll, ShouldBeFalse)

	_, err = os.Stat(filepath.Join(spoolDir, chspool.TableChildFilterAll+".gob.gz"))
	So(errors.Is(err, os.ErrNotExist), ShouldBeTrue)
}

func writeB5SpoolFixtureStats(t *testing.T, statsPath string, mountPath string, updatedAt time.Time) {
	t.Helper()

	root := statsdata.NewRoot(mountPath, updatedAt.Unix())
	root.UID = 30
	root.GID = 40
	root.Inode = 200
	root.Nlink = 1

	rootFile := root.AddFile("root.bam")
	rootFile.Size = 50
	rootFile.UID = 31
	rootFile.GID = 40
	rootFile.ATime = updatedAt.Unix()
	rootFile.MTime = updatedAt.Unix()
	rootFile.CTime = updatedAt.Unix()
	rootFile.Inode = 201
	rootFile.Nlink = 1

	sub := root.AddDirectory("sub")
	sub.UID = 32
	sub.GID = 41
	sub.Inode = 202
	sub.Nlink = 1

	nested := sub.AddFile("nested.cram")
	nested.Size = 75
	nested.UID = 33
	nested.GID = 42
	nested.ATime = updatedAt.Unix()
	nested.MTime = updatedAt.Unix()
	nested.CTime = updatedAt.Unix()
	nested.Inode = 203
	nested.Nlink = 1

	deep := sub.AddDirectory("deep")
	deep.UID = 34
	deep.GID = 43
	deep.Inode = 204
	deep.Nlink = 1

	leaf := deep.AddFile("leaf.txt")
	leaf.Size = 25
	leaf.UID = 35
	leaf.GID = 44
	leaf.ATime = updatedAt.Unix()
	leaf.MTime = updatedAt.Unix()
	leaf.CTime = updatedAt.Unix()
	leaf.Inode = 205
	leaf.Nlink = 1

	var buf bytes.Buffer

	_, err := root.WriteTo(&buf)
	So(err, ShouldBeNil)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func b5CatalogRows(spoolDir string) (map[string]chspool.DirRow, map[uint32]chspool.DirRow) {
	byPath := make(map[string]chspool.DirRow)
	byID := make(map[uint32]chspool.DirRow)

	So(chspool.DecodeRows[chspool.DirRow](spoolDir, chspool.TableDirs, func(row chspool.DirRow) error {
		byPath[row.FullPath] = row
		byID[row.DirID] = row

		return nil
	}), ShouldBeNil)

	return byPath, byID
}

func b5FileRowsByFullPath(
	spoolDir string,
	dirsByID map[uint32]chspool.DirRow,
) map[string]chspool.FileRow {
	files := make(map[string]chspool.FileRow)

	So(chspool.DecodeRows[chspool.FileRow](spoolDir, chspool.TableFiles, func(row chspool.FileRow) error {
		parent, ok := dirsByID[row.DirID]
		So(ok, ShouldBeTrue)
		So(row.DirID, ShouldBeGreaterThan, uint32(0))

		files[parent.FullPath+row.Name] = row

		return nil
	}), ShouldBeNil)

	return files
}

func b5AssertFileCatalogAgreement(
	dirsByPath map[string]chspool.DirRow,
	dirsByID map[uint32]chspool.DirRow,
	filesByPath map[string]chspool.FileRow,
	mountPath string,
) {
	for fullPath, file := range filesByPath {
		parent, ok := dirsByID[file.DirID]
		So(ok, ShouldBeTrue)
		So(parent.FullPath+file.Name, ShouldEqual, fullPath)

		if file.EntryType != 'd' {
			continue
		}

		dir, ok := dirsByPath[fullPath]
		So(ok, ShouldBeTrue)
		So(file.DirID, ShouldEqual, dir.ParentID)
	}

	for fullPath, dir := range dirsByPath {
		if !strings.HasPrefix(fullPath, mountPath) {
			continue
		}

		file, ok := filesByPath[fullPath]
		So(ok, ShouldBeTrue)
		So(file.EntryType, ShouldEqual, uint8('d'))
		So(file.DirID, ShouldEqual, dir.ParentID)
	}
}

func insertMountActiveSnapshotEventForTest(
	t *testing.T,
	ctx context.Context,
	conn ch.Conn,
	mountPath string,
	eventAt time.Time,
	snapshotID string,
	updatedAt time.Time,
	reason string,
) {
	t.Helper()

	So(conn.Exec(
		ctx,
		insertMountActiveSnapshotEventForTestQuery,
		mountPath,
		eventAt,
		snapshotID,
		updatedAt,
		reason,
	), ShouldBeNil)
}

func d2ExpectedActiveSetIDForRows(rows []summariseActiveSetRowForTest) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		updatedAt := summariseActiveSetUpdatedAt(row.updatedAt)
		parts = append(parts, row.mountPath+"|"+row.snapshotID+"|"+updatedAt.Format(time.RFC3339Nano))
	}

	slices.Sort(parts)

	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}

	return hex.EncodeToString(hash.Sum(nil))
}

func countActiveVirtualSetRows(ctx context.Context, conn ch.Conn, activeSetID string) uint64 {
	return d3CountRows(ctx, conn, "SELECT count() FROM wrstat_active_virtual_sets WHERE active_set_id = ?", activeSetID)
}

func activeVirtualSetMountCountForTest(ctx context.Context, conn ch.Conn, activeSetID string) uint64 {
	return d3CountRows(
		ctx,
		conn,
		"SELECT active_mount_count FROM wrstat_active_virtual_sets WHERE active_set_id = ?",
		activeSetID,
	)
}

func reinsertOldPublishSentinels(
	t *testing.T,
	ctx context.Context,
	conn ch.Conn,
	sourceSnapshotID string,
	previousSnapshotID string,
	previousActiveSetID string,
	nextActiveSetID string,
) {
	t.Helper()

	So(conn.Exec(ctx, reinsertOldDirFactSentinelQuery, previousSnapshotID, summariseTestMountPath, sourceSnapshotID),
		ShouldBeNil)
	So(conn.Exec(ctx, reinsertOldActiveVirtualSetSentinelQuery, previousActiveSetID, previousActiveSetID,
		nextActiveSetID), ShouldBeNil)
}

func countDirFactSnapshotRows(ctx context.Context, conn ch.Conn, snapshotID string) uint64 {
	return d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_dir_facts WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		summariseTestMountPath,
		snapshotID,
	)
}

func summariseSpoolPhaseCount(phases []string, want string) int {
	count := 0

	for _, phase := range phases {
		if phase == want {
			count++
		}
	}

	return count
}

func readSummariseSpoolStatePhasesForTest(t *testing.T, statePath string) map[string]string {
	t.Helper()

	var state struct {
		CompletedPhases map[string]string `json:"completed_phases"`
	}

	data, err := os.ReadFile(statePath)
	So(err, ShouldBeNil)
	So(json.Unmarshal(data, &state), ShouldBeNil)

	return state.CompletedPhases
}

type summariseActiveSetRowForTest struct {
	mountPath  string
	snapshotID string
	updatedAt  time.Time
}

type d2ActiveVirtualSummaryFacts struct {
	AllCount        uint64
	AllSize         uint64
	AllAtimeMin     int64
	AllMtimeMax     int64
	AllAtimeBuckets []uint64
	AllMtimeBuckets []uint64
	AllUIDs         []uint32
	AllGIDs         []uint32
	AllFT           uint16
	FileCount       uint64
	FileSize        uint64
}

func d2RootDirFactSummaryDigest(row chspool.DirFactRow) string {
	return d2SpoolDigest(d2ActiveVirtualSummaryFacts{
		AllCount:        row.AllCount,
		AllSize:         row.AllSize,
		AllAtimeMin:     row.AllAtimeMin,
		AllMtimeMax:     row.AllMtimeMax,
		AllAtimeBuckets: row.AllAtimeBuckets,
		AllMtimeBuckets: row.AllMtimeBuckets,
		AllUIDs:         row.AllUIDs,
		AllGIDs:         row.AllGIDs,
		AllFT:           row.AllFT,
		FileCount:       row.FileCount,
		FileSize:        row.FileSize,
	})
}

func d2SpoolDigest(value any) string {
	data, err := json.Marshal(value)
	So(err, ShouldBeNil)

	return d2SHA256Hex(string(data))
}

func d2ActiveVirtualSummaryDigest(row chspool.ActiveVirtualSummaryRow) string {
	return d2SpoolDigest(d2ActiveVirtualSummaryFacts{
		AllCount:        row.AllCount,
		AllSize:         row.AllSize,
		AllAtimeMin:     row.AllAtimeMin,
		AllMtimeMax:     row.AllMtimeMax,
		AllAtimeBuckets: row.AllAtimeBuckets,
		AllMtimeBuckets: row.AllMtimeBuckets,
		AllUIDs:         row.AllUIDs,
		AllGIDs:         row.AllGIDs,
		AllFT:           row.AllFT,
		FileCount:       row.FileCount,
		FileSize:        row.FileSize,
	})
}

type d2ActiveVirtualFilterFacts struct {
	Age          uint8
	GID          uint32
	UID          uint32
	FT           uint16
	Count        uint64
	Size         uint64
	AtimeMin     int64
	MtimeMax     int64
	AtimeBuckets []uint64
	MtimeBuckets []uint64
}

func d2ActiveVirtualFilterDigest(row chspool.ActiveVirtualFilterAllRow) string {
	return d2SpoolDigest(d2ActiveVirtualFilterFacts{
		Age:          row.Age,
		GID:          row.GID,
		UID:          row.UID,
		FT:           row.FT,
		Count:        row.Count,
		Size:         row.Size,
		AtimeMin:     row.AtimeMin,
		MtimeMax:     row.MtimeMax,
		AtimeBuckets: row.AtimeBuckets,
		MtimeBuckets: row.MtimeBuckets,
	})
}

func d2DirFilterDigest(row chspool.DirFilterAllRow) string {
	return d2SpoolDigest(d2ActiveVirtualFilterFacts{
		Age:          row.Age,
		GID:          row.GID,
		UID:          row.UID,
		FT:           row.FT,
		Count:        row.Count,
		Size:         row.Size,
		AtimeMin:     row.AtimeMin,
		MtimeMax:     row.MtimeMax,
		AtimeBuckets: row.AtimeBuckets,
		MtimeBuckets: row.MtimeBuckets,
	})
}

type d3SummaryFacts struct {
	Count uint64
	Size  uint64
	UIDs  []uint32
	GIDs  []uint32
	FT    uint16
	Age   uint8
}

func d3RootBroadFactDigest(ctx context.Context, conn ch.Conn, manifest *chspool.Manifest) string {
	row := conn.QueryRow(
		ctx,
		"SELECT f.all_count, f.all_size, f.all_uids, f.all_gids, f.all_ft "+
			"FROM wrstat_dir_facts f INNER JOIN wrstat_dirs d "+
			"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id "+
			"WHERE f.mount_path = ? AND f.snapshot_id = toUUID(?) AND d.full_path = ? LIMIT 1",
		manifest.MountPath,
		manifest.SnapshotID,
		manifest.MountPath,
	)

	facts := d3SummaryFacts{Age: uint8(db.DGUTAgeAll)}
	So(row.Scan(&facts.Count, &facts.Size, &facts.UIDs, &facts.GIDs, &facts.FT), ShouldBeNil)

	return d3SummaryDigest(facts)
}

func d3RootFullFilterDigest(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	filter *db.Filter,
) string {
	row := conn.QueryRow(
		ctx,
		"SELECT f.count, f.size, f.uid, f.gid, f.ft "+
			"FROM wrstat_dir_filter_all f INNER JOIN wrstat_dirs d "+
			"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id "+
			"WHERE f.mount_path = ? AND f.snapshot_id = toUUID(?) AND d.full_path = ? "+
			"AND f.age = ? AND f.gid = ? AND f.uid = ? AND f.ft = ? LIMIT 1",
		manifest.MountPath,
		manifest.SnapshotID,
		manifest.MountPath,
		uint8(filter.Age),
		filter.GIDs[0],
		filter.UIDs[0],
		uint16(filter.FT),
	)

	var (
		uid uint32
		gid uint32
		ft  uint16
	)

	facts := d3SummaryFacts{Age: uint8(filter.Age)}
	So(row.Scan(&facts.Count, &facts.Size, &uid, &gid, &ft), ShouldBeNil)
	facts.UIDs = []uint32{uid}
	facts.GIDs = []uint32{gid}
	facts.FT = ft

	return d3SummaryDigest(facts)
}

func d3DirSummaryDigest(summary *db.DirSummary) string {
	So(summary, ShouldNotBeNil)

	return d3SummaryDigest(d3SummaryFacts{
		Count: summary.Count,
		Size:  summary.Size,
		UIDs:  summary.UIDs,
		GIDs:  summary.GIDs,
		FT:    uint16(summary.FT),
		Age:   uint8(summary.Age),
	})
}

func d3SummaryDigest(facts d3SummaryFacts) string {
	facts.UIDs = append([]uint32(nil), facts.UIDs...)
	facts.GIDs = append([]uint32(nil), facts.GIDs...)
	slices.Sort(facts.UIDs)
	slices.Sort(facts.GIDs)

	return d2SpoolDigest(facts)
}

func TestSummariseClickHouseSpoolRows(t *testing.T) {
	Convey("A3.1 contiguous spool build probes then uses the fast path without dirbuild", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		openCalls := 0
		dirbuildCalls := 0
		originalOpen := openSummariseSpoolStats
		originalDirbuild := buildSummariseSpoolDirbuild

		openSummariseSpoolStats = func(statsPath string) (io.ReadCloser, error) {
			openCalls++

			return originalOpen(statsPath)
		}
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++

			return originalDirbuild(open, mountPath, database, refTime, files, opts)
		}

		_, manifest := buildSummariseSpoolFixtureForTest(t, fixture)

		So(manifest.Tables[chspool.TableDirs].Rows, ShouldBeGreaterThan, uint64(0))
		So(openCalls, ShouldEqual, 2)
		So(dirbuildCalls, ShouldEqual, 0)

		report := readSummariseSpoolBuildReport(t, summariseClickHouseSpoolDir(fixture.outputDir))
		op := summariseSpoolBuildReportOperationForTest(report)
		So(op.Inputs["input_shape"], ShouldEqual, "contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "contiguous_fast_path")
		So(op.Inputs["completed"], ShouldEqual, true)
		So(uint64InputForTest(op.Inputs, "build_phase_bytes_written"), ShouldEqual, uint64(0))
		So(uint64InputForTest(op.Inputs, "spool_bytes"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "row_cap"), ShouldBeGreaterThan, uint64(0))
		So(report.MaxRSSBytes, ShouldBeGreaterThan, uint64(0))
	})

	Convey("A3.1b an early-unordered spool stops the exact probe and records the violation", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		rawStats := writeEarlyNonContiguousSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt, 20_000)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		openCalls := 0
		probeBytesRead := 0
		dirbuildCalls := 0
		originalOpen := openSummariseSpoolStats
		originalDirbuild := buildSummariseSpoolDirbuild

		openSummariseSpoolStats = func(statsPath string) (io.ReadCloser, error) {
			openCalls++
			if openCalls == 1 {
				return &summariseCountingReadCloser{
					Reader:    bytes.NewReader(rawStats),
					bytesRead: &probeBytesRead,
				}, nil
			}

			return originalOpen(statsPath)
		}
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++

			return originalDirbuild(open, mountPath, database, refTime, files, opts)
		}

		_, manifest := buildSummariseSpoolFixtureForTest(t, fixture)

		So(manifest.Tables[chspool.TableDirs].Rows, ShouldBeGreaterThan, uint64(0))
		So(openCalls, ShouldEqual, 3)
		So(dirbuildCalls, ShouldEqual, 1)
		So(probeBytesRead, ShouldBeLessThan, len(rawStats))

		report := readSummariseSpoolBuildReport(t, summariseClickHouseSpoolDir(fixture.outputDir))
		op := summariseSpoolBuildReportOperationForTest(report)
		So(op.Inputs["input_shape"], ShouldEqual, "non_contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "dirbuild")
		So(uint64InputForTest(op.Inputs, "contiguity_violation_row"), ShouldEqual, uint64(7))
		So(uint64InputForTest(op.Inputs, "contiguity_violation_path_depth"), ShouldEqual, uint64(3))
		So(op.Inputs["completed"], ShouldEqual, true)
	})

	Convey("A3.1c input large by the old threshold is exactly probed and uses the contiguous fast path", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeLargeContiguousSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt, 30_000)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		padSummariseSpoolFixturePastOldThreshold(t, fixture.statsPath, fixture.updatedAt)

		openCalls := 0
		dirbuildCalls := 0
		originalOpen := openSummariseSpoolStats
		originalDirbuild := buildSummariseSpoolDirbuild

		openSummariseSpoolStats = func(statsPath string) (io.ReadCloser, error) {
			openCalls++

			return originalOpen(statsPath)
		}
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++

			return originalDirbuild(open, mountPath, database, refTime, files, opts)
		}

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		stopSampling := startSummariseHeapInuseSampler(before.HeapInuse, 10*time.Millisecond)
		_, manifest := buildSummariseSpoolFixtureForTest(t, fixture)
		peakGrowth := stopSampling()

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		So(manifest.Tables[chspool.TableDirs].Rows, ShouldEqual, uint64(30_003))
		So(dirbuildCalls, ShouldEqual, 0)
		So(openCalls, ShouldEqual, 2)
		So(peakGrowth, ShouldBeLessThan, uint64(220*1024*1024))
		So(summariseHeapInuseGrowth(before.HeapInuse, after.HeapInuse), ShouldBeLessThan, uint64(120*1024*1024))

		report := readSummariseSpoolBuildReport(t, summariseClickHouseSpoolDir(fixture.outputDir))
		op := summariseSpoolBuildReportOperationForTest(report)
		So(op.Inputs["input_shape"], ShouldEqual, "contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "contiguous_fast_path")
		So(op.Inputs["completed"], ShouldEqual, true)
	})

	Convey("A3.1c2 dirbuild full-filter rows retain direct-child tuple counts", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeNestedFullFilterSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		spoolDir, _ := buildSummariseSpoolFixtureForTest(t, fixture)

		rows := d2DirFilterRowsByDirPathAndTuple(spoolDir, summariseTestMountPath+"project/")
		tuple := summariseFullFilterTupleKey{
			age: uint8(db.DGUTAgeAll),
			gid: 7,
			uid: 17,
			ft:  uint16(db.DGUTAFileTypeBam),
		}
		row, ok := rows[tuple]

		So(ok, ShouldBeTrue)
		So(row.FilterChildCount, ShouldEqual, uint64(2))
		So(row.HasFilterChildren, ShouldEqual, uint8(1))
		So(row.ChildCount, ShouldEqual, uint64(2))
		So(row.HasChildren, ShouldEqual, uint8(1))
	})

	Convey("A3.1c3 disk-backed dirbuild keeps full-filter direct-child counts", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeNestedFullFilterSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		dirbuildCalls := 0
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++
			opts.DiskNodeThreshold = 2

			return dirbuild.BuildWithFilesOptions(open, mountPath, database, refTime, files, opts)
		}

		spoolDir, _ := buildSummariseSpoolFixtureForTest(t, fixture)

		rows := d2DirFilterRowsByDirPathAndTuple(spoolDir, summariseTestMountPath+"project/")
		tuple := summariseFullFilterTupleKey{
			age: uint8(db.DGUTAgeAll),
			gid: 7,
			uid: 17,
			ft:  uint16(db.DGUTAFileTypeBam),
		}
		row, ok := rows[tuple]

		So(dirbuildCalls, ShouldEqual, 1)
		So(ok, ShouldBeTrue)
		So(row.FilterChildCount, ShouldEqual, uint64(2))
		So(row.HasFilterChildren, ShouldEqual, uint8(1))
		So(row.ChildCount, ShouldEqual, uint64(2))
		So(row.HasChildren, ShouldEqual, uint8(1))
	})

	Convey("A3.1c4 disk-backed dirbuild scratch is reported and cleaned", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeNestedFullFilterSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			opts.DiskNodeThreshold = 2

			return dirbuild.BuildWithFilesOptions(open, mountPath, database, refTime, files, opts)
		}

		spoolDir, _ := buildSummariseSpoolFixtureForTest(t, fixture)
		report := readSummariseSpoolBuildReport(t, spoolDir)
		op := summariseSpoolBuildReportOperationForTest(report)

		So(op.Inputs["input_shape"], ShouldEqual, "non_contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "dirbuild")
		So(op.Inputs["completed"], ShouldEqual, true)
		So(uint64InputForTest(op.Inputs, "build_phase_bytes_written"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "sqlite_write_behind_limit_bytes"), ShouldEqual, uint64(4*1024*1024))
		So(uint64InputForTest(op.Inputs, "sqlite_max_write_behind_bytes"),
			ShouldBeLessThanOrEqualTo, uint64InputForTest(op.Inputs, "sqlite_write_behind_limit_bytes"))
		So(uint64InputForTest(op.Inputs, "sqlite_rows_received"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "sqlite_rows_written"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "sqlite_statements"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "sqlite_select_statements"), ShouldBeGreaterThan, uint64(0))
		So(uint64InputForTest(op.Inputs, "sqlite_database_bytes"), ShouldBeGreaterThan, uint64(0))
		So(op.Inputs, ShouldNotContainKey, "sqlite_write_bytes")
		So(op.Inputs["dirbuild_process_write_bytes_source"], ShouldEqual, "/proc/self/io write_bytes")
		So(op.Inputs["dirbuild_process_write_bytes_available"], ShouldEqual, true)
		So(uint64InputForTest(op.Inputs, "dirbuild_process_write_bytes"), ShouldEqual,
			uint64InputForTest(op.Inputs, "dirbuild_pass2_process_write_bytes")+
				uint64InputForTest(op.Inputs, "dirbuild_rollup_process_write_bytes"))
		So(op.Inputs["dirbuild_pass2_elapsed_ms"], ShouldBeGreaterThan, float64(0))
		So(op.Inputs["dirbuild_rollup_elapsed_ms"], ShouldBeGreaterThan, float64(0))
		So(summariseSpoolHasDirbuildScratchForTest(t, spoolDir), ShouldBeFalse)
	})

	Convey("A3.1c4b failed disk-backed dirbuild retains SQLite scratch for diagnosis", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeNestedFullFilterSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			_ dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			opts.DiskNodeThreshold = 2

			return dirbuild.BuildWithFilesOptions(open, mountPath, failingDirbuildDatabase{}, refTime, files, opts)
		}

		target := &clickHouseSummariseTarget{
			cfg:       clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath: summariseTestMountPath,
			modtime:   fixture.updatedAt,
			outputDir: fixture.outputDir,
		}
		expected, err := newSummariseSpoolManifest(fixture.statsPath, target)
		So(err, ShouldBeNil)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		_, err = buildSummariseSpool(
			fixture.statsPath, spoolDir, expected, target, newSummariseDiagnostics(fixture.statsPath),
		)

		So(err, ShouldNotBeNil)
		So(summariseSpoolHasDirbuildScratchForTest(t, spoolDir+".partial"), ShouldBeTrue)
	})

	Convey("A3.1c5 non-contiguous stdin is rejected instead of attempting a second pass", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		rawStats := writeEarlyNonContiguousSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt, 0)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		stdinPath := filepath.Join(t.TempDir(), "stdin.stats")
		So(os.WriteFile(stdinPath, rawStats, 0o600), ShouldBeNil)

		stdin, err := os.Open(stdinPath)
		So(err, ShouldBeNil)

		originalStdin := os.Stdin
		os.Stdin = stdin

		Reset(func() {
			os.Stdin = originalStdin

			So(stdin.Close(), ShouldBeNil)
		})

		dirbuildCalls := 0
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++

			return dirbuild.BuildWithFilesOptions(open, mountPath, database, refTime, files, opts)
		}

		target := fixture.clickHouseTarget()
		expected, err := newSummariseSpoolManifest("-", target)
		So(err, ShouldBeNil)

		result := runSummariseSpoolBuild(
			"-",
			filepath.Join(t.TempDir(), "spool.partial"),
			expected,
			target,
			newSummariseDiagnostics("-"),
		)

		So(errors.Is(result.err, errSummariseSpoolNonContiguousStdin), ShouldBeTrue)
		So(dirbuildCalls, ShouldEqual, 0)
	})

	Convey("A3.1d small non-contiguous high-fanout basedirs build is bounded", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeHighFanoutNonContiguousSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt, 2_000, 12)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		openCalls := 0
		dirbuildCalls := 0
		originalOpen := openSummariseSpoolStats
		originalDirbuild := buildSummariseSpoolDirbuild

		openSummariseSpoolStats = func(statsPath string) (io.ReadCloser, error) {
			openCalls++

			return originalOpen(statsPath)
		}
		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			dirbuildCalls++

			return originalDirbuild(open, mountPath, database, refTime, files, opts)
		}

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		stopSampling := startSummariseHeapInuseSampler(before.HeapInuse, 10*time.Millisecond)
		_, manifest := buildSummariseSpoolFixtureForTest(t, fixture)
		peakGrowth := stopSampling()

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		So(manifest.Tables[chspool.TableDirs].Rows, ShouldEqual, uint64(2_004))
		So(dirbuildCalls, ShouldEqual, 1)
		So(openCalls, ShouldEqual, 3)
		So(peakGrowth, ShouldBeLessThan, uint64(700*1024*1024))
		So(summariseHeapInuseGrowth(before.HeapInuse, after.HeapInuse), ShouldBeLessThan, uint64(120*1024*1024))

		report := readSummariseSpoolBuildReport(t, summariseClickHouseSpoolDir(fixture.outputDir))
		op := summariseSpoolBuildReportOperationForTest(report)
		So(op.Inputs["input_shape"], ShouldEqual, "non_contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "dirbuild")
		So(op.Inputs["completed"], ShouldEqual, true)
	})

	Convey("A3.2 non-contiguous spool build removes stale partial rows and matches contiguous row counts", t, func() {
		baseDir := t.TempDir()
		updatedAt := time.Unix(1_710_000_000, 123).UTC()
		contiguous := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)
		nonContiguous := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)

		writeContiguousSubtreeRevisitSpoolFixtureStats(t, contiguous.statsPath, updatedAt)
		writeNonContiguousSpoolFixtureStats(t, nonContiguous.statsPath, updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		configureSummariseActiveSnapshotTest(contiguous.outputDir, false)

		_, contiguousManifest := buildSummariseSpoolFixtureForTest(t, contiguous)

		configureSummariseActiveSnapshotTest(nonContiguous.outputDir, false)

		stalePartial := filepath.Join(summariseClickHouseSpoolDir(nonContiguous.outputDir)+".partial", "stale")
		So(os.MkdirAll(filepath.Dir(stalePartial), 0o700), ShouldBeNil)
		So(os.WriteFile(stalePartial, []byte("discard me"), 0o600), ShouldBeNil)

		originalDirbuild := buildSummariseSpoolDirbuild
		buildScratchPath := filepath.Join(filepath.Dir(stalePartial), "dirbuild-scratch.tmp")
		buildScratch := []byte("scratch proof")

		buildSummariseSpoolDirbuild = func(
			open func() (io.ReadCloser, error),
			mountPath string,
			database dirguta.DB,
			refTime time.Time,
			files dirbuild.FileSink,
			opts dirbuild.Options,
		) error {
			_, err := os.Stat(stalePartial)
			So(errors.Is(err, os.ErrNotExist), ShouldBeTrue)
			So(os.WriteFile(buildScratchPath, buildScratch, 0o600), ShouldBeNil)

			return originalDirbuild(open, mountPath, database, refTime, files, opts)
		}

		_, nonContiguousManifest := buildSummariseSpoolFixtureForTest(t, nonContiguous)

		assertSummariseManifestTableRowsMatch(contiguousManifest, nonContiguousManifest)

		report := readSummariseSpoolBuildReport(t, summariseClickHouseSpoolDir(nonContiguous.outputDir))
		op := summariseSpoolBuildReportOperationForTest(report)
		So(op.Inputs["input_shape"], ShouldEqual, "non_contiguous")
		So(op.Inputs["build_path"], ShouldEqual, "dirbuild")
		So(op.Inputs["completed"], ShouldEqual, true)
		So(uint64InputForTest(op.Inputs, "build_phase_bytes_written"), ShouldEqual, uint64(len(buildScratch)))
	})

	Convey("A3.3 non-contiguous spool build keeps basedirs row counts", t, func() {
		baseDir := t.TempDir()
		updatedAt := time.Unix(1_710_000_000, 123).UTC()
		contiguous := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)
		nonContiguous := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)

		writeBasedirsSpoolFixtureStats(t, contiguous.statsPath, updatedAt)
		writeNonContiguousBasedirsSpoolFixtureStats(t, nonContiguous.statsPath, updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		configureSummariseActiveSnapshotTest(contiguous.outputDir, false)
		quotaPath = filepath.Join(contiguous.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(contiguous.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(contiguous.outputDir, basedirBasename)
		mounts = filepath.Join(contiguous.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		_, contiguousManifest := buildSummariseSpoolFixtureForTest(t, contiguous)

		configureSummariseActiveSnapshotTest(nonContiguous.outputDir, false)
		quotaPath = filepath.Join(nonContiguous.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(nonContiguous.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(nonContiguous.outputDir, basedirBasename)
		mounts = filepath.Join(nonContiguous.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		_, nonContiguousManifest := buildSummariseSpoolFixtureForTest(t, nonContiguous)

		for _, table := range []string{
			chspool.TableBasedirsHistory,
			chspool.TableBasedirsGroupUsage,
			chspool.TableBasedirsUserUsage,
			chspool.TableBasedirsGroupSubdirs,
			chspool.TableBasedirsUserSubdirs,
		} {
			So(contiguousManifest.Tables[table].Rows, ShouldBeGreaterThan, uint64(0))
			So(nonContiguousManifest.Tables[table].Rows, ShouldEqual, contiguousManifest.Tables[table].Rows)
		}
	})

	Convey("A3.4 non-contiguity regression fixtures match their contiguous spool rows", t, func() {
		type fixtureCase struct {
			name       string
			unordered  func(*testing.T, string, time.Time)
			contiguous func(*testing.T, string, time.Time)
		}

		cases := []fixtureCase{
			{
				name:       "unordered subtree revisit",
				unordered:  writeNonContiguousSpoolFixtureStats,
				contiguous: writeContiguousSubtreeRevisitSpoolFixtureStats,
			},
			{
				name:       "prefix-sharing siblings",
				unordered:  writePrefixSharingDirectorySiblingSpoolFixtureStats,
				contiguous: writeContiguousPrefixSharingDirectorySiblingSpoolFixtureStats,
			},
			{
				name:       "same-name file and directory",
				unordered:  writeSameNameFileDirectorySpoolFixtureStats,
				contiguous: writeContiguousSameNameFileDirectorySpoolFixtureStats,
			},
			{
				name:       "slashless directory rows",
				unordered:  writeSlashlessDirectoryBoundarySpoolFixtureStats,
				contiguous: writeContiguousSlashlessDirectoryBoundarySpoolFixtureStats,
			},
			{
				name:       "unicode non-breaking-space path",
				unordered:  writeEscapedUnicodeSiblingSpoolFixtureStats,
				contiguous: writeContiguousEscapedUnicodeSiblingSpoolFixtureStats,
			},
		}

		for _, tc := range cases {
			Convey(tc.name, func() {
				baseDir := t.TempDir()
				updatedAt := time.Unix(1_710_000_000, 123).UTC()
				contiguous := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)
				unordered := newSummariseActiveSnapshotFixtureForMount(t, baseDir, summariseTestMountPath, updatedAt)

				tc.contiguous(t, contiguous.statsPath, updatedAt)
				tc.unordered(t, unordered.statsPath, updatedAt)

				restore := snapshotSummariseGlobals()
				Reset(restore)

				summariseSpoolNow = func() time.Time {
					return updatedAt.Add(time.Hour)
				}
				summariseSpoolDirGUTANow = d2Schema3DirGUTAReferenceTime

				configureSummariseActiveSnapshotTest(contiguous.outputDir, false)
				contiguousSpoolDir, contiguousManifest := buildSummariseSpoolFixtureForTest(t, contiguous)

				configureSummariseActiveSnapshotTest(unordered.outputDir, false)
				unorderedSpoolDir, unorderedManifest := buildSummariseSpoolFixtureForTest(t, unordered)

				assertSummariseManifestTableRowsMatch(contiguousManifest, unorderedManifest)
				assertSummariseSpoolDecodedRowsMatch(contiguousSpoolDir, unorderedSpoolDir)
			})
		}
	})

	Convey("summarise spool records small-fixture rows for files, tree facts and basedirs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)

		target := &clickHouseSummariseTarget{
			cfg:       clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath: summariseTestMountPath,
			modtime:   fixture.updatedAt,
			outputDir: fixture.outputDir,
		}
		expected, err := newSummariseSpoolManifest(fixture.statsPath, target)
		So(err, ShouldBeNil)

		manifest, err := buildSummariseSpool(
			fixture.statsPath,
			summariseClickHouseSpoolDir(fixture.outputDir),
			expected,
			target,
			newSummariseDiagnostics(fixture.statsPath),
		)
		So(err, ShouldBeNil)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)

		_, hasChildrenStream := manifest.Tables["wrstat_"+"children"]
		_, hasParentFactsStream := manifest.Tables["wrstat_"+"parent_facts"]

		So(hasChildrenStream, ShouldBeFalse)
		So(hasParentFactsStream, ShouldBeFalse)
		So(manifest.Tables[chspool.TableDirs].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThanOrEqualTo, uint64(3))
		So(manifest.Tables[chspool.TableDirFacts].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirFilterAgeAll].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirProjectionSets].Rows, ShouldEqual, uint64(1))
		So(manifest.Tables[chspool.TableBasedirsHistory].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsGroupUsage].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsUserUsage].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsGroupSubdirs].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableBasedirsUserSubdirs].Rows, ShouldBeGreaterThan, uint64(0))

		var files []chspool.FileRow

		So(chspool.DecodeRows[chspool.FileRow](spoolDir, chspool.TableFiles, func(row chspool.FileRow) error {
			files = append(files, row)

			return nil
		}), ShouldBeNil)
		So(files[0].MountPath, ShouldEqual, summariseTestMountPath)
		So(files[0].DirID, ShouldBeGreaterThan, uint32(0))
		So(files[0].Name, ShouldNotBeBlank)
		So(files[len(files)-1].SnapshotID, ShouldEqual, manifest.SnapshotID)

		var dirs []chspool.DirRow

		So(chspool.DecodeRows[chspool.DirRow](spoolDir, chspool.TableDirs, func(row chspool.DirRow) error {
			dirs = append(dirs, row)

			return nil
		}), ShouldBeNil)
		So(dirs[0].MountPath, ShouldEqual, summariseTestMountPath)
		So(dirs[0].DirID, ShouldBeGreaterThan, uint32(0))
		So(dirs[0].FullPath, ShouldNotBeBlank)

		var facts []chspool.DirFactRow

		So(chspool.DecodeRows[chspool.DirFactRow](spoolDir, chspool.TableDirFacts, func(row chspool.DirFactRow) error {
			facts = append(facts, row)

			return nil
		}), ShouldBeNil)
		So(facts[0].DirID, ShouldBeGreaterThan, uint32(0))
		So(facts[0].SubtreeEnd, ShouldBeGreaterThanOrEqualTo, facts[0].DirID)
		So(facts[0].GIDs, ShouldContain, uint32(7))
		So(facts[0].UIDs, ShouldContain, uint32(17))

		assertChildFilterAllSpoolAbsent(spoolDir)
	})

	Convey("B5 summarise spool file rows share catalog directory ids for /m/teamX/", t, func() {
		baseDir := t.TempDir()
		updatedAt := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
		fixture := newSummariseActiveSnapshotFixtureForMount(t, baseDir, b5SpoolMountPath, updatedAt)
		writeB5SpoolFixtureStats(t, fixture.statsPath, b5SpoolMountPath, updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)

		So(os.WriteFile(quotaPath, []byte("40,/m/teamX,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/m/teamX\t1\t3\n"), 0o600), ShouldBeNil)

		target := &clickHouseSummariseTarget{
			cfg:       clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath: b5SpoolMountPath,
			modtime:   fixture.updatedAt,
			outputDir: fixture.outputDir,
		}
		expected, err := newSummariseSpoolManifest(fixture.statsPath, target)
		So(err, ShouldBeNil)

		manifest, err := buildSummariseSpool(
			fixture.statsPath,
			summariseClickHouseSpoolDir(fixture.outputDir),
			expected,
			target,
			newSummariseDiagnostics(fixture.statsPath),
		)
		So(err, ShouldBeNil)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		dirsByPath, dirsByID := b5CatalogRows(spoolDir)
		filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

		mountParent := dirsByPath["/m/"]
		mountRoot := dirsByPath[b5SpoolMountPath]
		subDir := dirsByPath[b5SpoolMountPath+"sub/"]

		So(mountParent.DirID, ShouldEqual, uint32(1))
		So(mountRoot.DirID, ShouldEqual, uint32(2))
		So(mountRoot.ParentID, ShouldEqual, mountParent.DirID)
		So(subDir.ParentID, ShouldEqual, mountRoot.DirID)
		So(mountParent.ChildFileCount, ShouldEqual, uint32(0))
		So(mountRoot.ChildDirCount, ShouldEqual, uint32(1))
		So(mountRoot.ChildFileCount, ShouldEqual, uint32(1))
		So(subDir.ChildDirCount, ShouldEqual, uint32(1))
		So(subDir.ChildFileCount, ShouldEqual, uint32(1))

		So(filesByPath[b5SpoolMountPath].DirID, ShouldEqual, mountParent.DirID)
		So(filesByPath[b5SpoolMountPath+"sub/"].DirID, ShouldEqual, mountRoot.DirID)
		So(filesByPath[b5SpoolMountPath+"root.bam"].DirID, ShouldEqual, mountRoot.DirID)
		So(filesByPath[b5SpoolMountPath+"sub/nested.cram"].DirID, ShouldEqual, subDir.DirID)
		So(filesByPath[b5SpoolMountPath+"sub/deep/leaf.txt"].DirID,
			ShouldEqual, dirsByPath[b5SpoolMountPath+"sub/deep/"].DirID)

		b5AssertFileCatalogAgreement(dirsByPath, dirsByID, filesByPath, b5SpoolMountPath)
		So(filesByPath[b5SpoolMountPath].SnapshotID, ShouldEqual, manifest.SnapshotID)
	})

	Convey("summarise command spools raw stats whose directory subtree is revisited later", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeNonContiguousSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			mountRoot := dirsByPath[summariseTestMountPath]
			alpha := dirsByPath[summariseTestMountPath+"a/"]
			alphaDeep := dirsByPath[summariseTestMountPath+"a/deep/"]
			beta := dirsByPath[summariseTestMountPath+"b/"]

			So(alpha.ParentID, ShouldEqual, mountRoot.DirID)
			So(alphaDeep.ParentID, ShouldEqual, alpha.DirID)
			So(beta.ParentID, ShouldEqual, mountRoot.DirID)
			So(alpha.DirID, ShouldBeLessThan, alphaDeep.DirID)
			So(alphaDeep.DirID, ShouldBeLessThan, alpha.SubtreeEnd)
			So(beta.DirID, ShouldBeGreaterThanOrEqualTo, alpha.SubtreeEnd)
			So(mountRoot.SubtreeEnd, ShouldEqual, beta.SubtreeEnd)

			So(filesByPath[summariseTestMountPath+"a/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"a/one.dat"].DirID, ShouldEqual, alpha.DirID)
			So(filesByPath[summariseTestMountPath+"a/deep/"].DirID, ShouldEqual, alpha.DirID)
			So(filesByPath[summariseTestMountPath+"a/deep/three.txt"].DirID, ShouldEqual, alphaDeep.DirID)
			So(filesByPath[summariseTestMountPath+"b/two.dat"].DirID, ShouldEqual, beta.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("summarise command spools unordered stats with repeated directory boundaries", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeRepeatedDirectoryBoundarySpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			mountRoot := dirsByPath[summariseTestMountPath]
			alpha := dirsByPath[summariseTestMountPath+"a/"]
			alphaDeep := dirsByPath[summariseTestMountPath+"a/deep/"]
			beta := dirsByPath[summariseTestMountPath+"b/"]

			So(alpha.ParentID, ShouldEqual, mountRoot.DirID)
			So(alphaDeep.ParentID, ShouldEqual, alpha.DirID)
			So(beta.ParentID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"a/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"a/one.dat"].DirID, ShouldEqual, alpha.DirID)
			So(filesByPath[summariseTestMountPath+"a/deep/"].DirID, ShouldEqual, alpha.DirID)
			So(filesByPath[summariseTestMountPath+"a/deep/three.txt"].DirID, ShouldEqual, alphaDeep.DirID)
			So(filesByPath[summariseTestMountPath+"b/two.dat"].DirID, ShouldEqual, beta.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("summarise command spools unordered stats with prefix-sharing directory siblings", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writePrefixSharingDirectorySiblingSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			mountRoot := dirsByPath[summariseTestMountPath]
			project := dirsByPath[summariseTestMountPath+"project/"]
			projectV2 := dirsByPath[summariseTestMountPath+"project.v2/"]

			So(project.ParentID, ShouldEqual, mountRoot.DirID)
			So(projectV2.ParentID, ShouldEqual, mountRoot.DirID)
			So(project.DirID, ShouldBeLessThan, projectV2.DirID)
			So(project.SubtreeEnd, ShouldBeLessThanOrEqualTo, projectV2.DirID)
			So(mountRoot.SubtreeEnd, ShouldEqual, projectV2.SubtreeEnd)

			So(filesByPath[summariseTestMountPath+"project/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"project/root.dat"].DirID, ShouldEqual, project.DirID)
			So(filesByPath[summariseTestMountPath+"project.v2/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"project.v2/result.dat"].DirID, ShouldEqual, projectV2.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("summarise command spools same-name file and directory entries without reopening the directory", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeSameNameFileDirectorySpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			mountRoot := dirsByPath[summariseTestMountPath]
			clash := dirsByPath[summariseTestMountPath+"clash/"]
			next := dirsByPath[summariseTestMountPath+"next/"]

			So(clash.ParentID, ShouldEqual, mountRoot.DirID)
			So(next.ParentID, ShouldEqual, mountRoot.DirID)
			So(clash.DirID, ShouldBeLessThan, next.DirID)
			So(clash.SubtreeEnd, ShouldBeLessThanOrEqualTo, next.DirID)

			So(filesByPath[summariseTestMountPath+"clash"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"clash/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"clash/leaf.dat"].DirID, ShouldEqual, clash.DirID)
			So(filesByPath[summariseTestMountPath+"next/"].DirID, ShouldEqual, mountRoot.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("summarise command spools escaped unicode siblings without reopening a prefix subtree", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeEscapedUnicodeSiblingSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			nbspDirPath := summariseTestMountPath + "chr1\u00a0/"
			nbspFilePath := nbspDirPath + "leaf.dat"
			_, hasNULDir := dirsByPath[summariseTestMountPath+"chr1\x00/"]
			mountRoot := dirsByPath[summariseTestMountPath]
			chr1 := dirsByPath[summariseTestMountPath+"chr1/"]
			chr1NBSP := dirsByPath[nbspDirPath]
			chr2 := dirsByPath[summariseTestMountPath+"chr2/"]

			So(hasNULDir, ShouldBeFalse)
			So(chr1.ParentID, ShouldEqual, mountRoot.DirID)
			So(chr1NBSP.ParentID, ShouldEqual, mountRoot.DirID)
			So(chr2.ParentID, ShouldEqual, mountRoot.DirID)
			So(chr1.DirID, ShouldBeLessThan, chr1NBSP.DirID)
			So(chr1.SubtreeEnd, ShouldBeLessThanOrEqualTo, chr1NBSP.DirID)
			So(chr1NBSP.SubtreeEnd, ShouldBeLessThanOrEqualTo, chr2.DirID)

			So(filesByPath[summariseTestMountPath+"chr1/file.dat"].DirID, ShouldEqual, chr1.DirID)
			So(filesByPath[nbspDirPath].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[nbspFilePath].DirID, ShouldEqual, chr1NBSP.DirID)
			So(filesByPath[summariseTestMountPath+"chr2/two.dat"].DirID, ShouldEqual, chr2.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("summarise command spools slashless explicit directory rows after prefix-sharing siblings", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeSlashlessDirectoryBoundarySpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			dirsByPath, dirsByID := b5CatalogRows(spoolDir)
			filesByPath := b5FileRowsByFullPath(spoolDir, dirsByID)

			mountRoot := dirsByPath[summariseTestMountPath]
			bins := dirsByPath[summariseTestMountPath+"bins/"]
			prior := dirsByPath[summariseTestMountPath+"bins/maxbin2.004_sub/"]
			normalised := dirsByPath[summariseTestMountPath+"bins/maxbin2.006/"]
			_, hasSlashlessDir := dirsByPath[summariseTestMountPath+"bins/maxbin2.006"]

			So(hasSlashlessDir, ShouldBeFalse)
			So(bins.ParentID, ShouldEqual, mountRoot.DirID)
			So(prior.ParentID, ShouldEqual, bins.DirID)
			So(normalised.ParentID, ShouldEqual, bins.DirID)
			So(prior.DirID, ShouldBeLessThan, normalised.DirID)
			So(prior.SubtreeEnd, ShouldBeLessThanOrEqualTo, normalised.DirID)

			So(filesByPath[summariseTestMountPath+"bins/"].DirID, ShouldEqual, mountRoot.DirID)
			So(filesByPath[summariseTestMountPath+"bins/maxbin2.004_sub/"].DirID, ShouldEqual, bins.DirID)
			So(filesByPath[summariseTestMountPath+"bins/maxbin2.004_sub/hmmer.tree.txt"].DirID,
				ShouldEqual, prior.DirID)
			So(filesByPath[summariseTestMountPath+"bins/maxbin2.006/"].DirID, ShouldEqual, bins.DirID)
			So(filesByPath[summariseTestMountPath+"bins/maxbin2.006/genes.faa"].DirID,
				ShouldEqual, normalised.DirID)

			assertSpoolCatalogIntervals(dirsByPath)
			assertChildFilterAllSpoolAbsent(spoolDir)

			return perfreport.NewReport("clickhouse", spoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
	})

	Convey("D2.7 actual summarise command path writes and verifies every schema3 spool table", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}
		dirgutaReferenceAt := d2Schema3DirGUTAReferenceTime()
		summariseSpoolDirGUTANow = func() time.Time {
			return dirgutaReferenceAt
		}

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}

		expectedTarget := &clickHouseSummariseTarget{
			cfg:             clickhouse.Config{DSN: summariseTestClickHouseDSN, Database: summariseTestClickHouseDatabase},
			mountPath:       summariseTestMountPath,
			mountpointsPath: mounts,
			modtime:         fixture.updatedAt,
			outputDir:       fixture.outputDir,
		}
		expectedManifest, err := newSummariseSpoolManifest(fixture.statsPath, expectedTarget)
		So(err, ShouldBeNil)

		// Build expected table bytes and hashes from an independent canonical
		// fixture spool before the command path under test writes its own spool.
		canonicalSpoolDir := filepath.Join(fixture.outputDir, "d2-schema3-canonical-spool")
		canonicalManifest, err := buildSummariseSpool(
			fixture.statsPath,
			canonicalSpoolDir,
			expectedManifest,
			expectedTarget,
			newSummariseDiagnostics(fixture.statsPath),
		)
		So(err, ShouldBeNil)

		var (
			manifest *chspool.Manifest
			spoolDir string
		)

		loadSummariseClickHouseSpool = func(
			_ context.Context,
			_ clickhouse.Config,
			gotSpoolDir string,
			gotManifest *chspool.Manifest,
			_ func(string, time.Duration),
		) (perfreport.Report, error) {
			spoolDir = gotSpoolDir
			manifest = gotManifest

			return perfreport.NewReport("clickhouse", gotSpoolDir, 1, 0), nil
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(manifest, ShouldNotBeNil)
		So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
		So(chspool.VerifyManifest(spoolDir, manifest, expectedManifest), ShouldBeNil)

		expectedRows := d2Schema3ExpectedRowCounts()
		expectedTables := d2Schema3ExpectedTablesFromCanonicalManifest(canonicalManifest, expectedRows)
		assertD2Schema3ManifestTables(spoolDir, manifest, expectedRows, expectedTables, canonicalSpoolDir)
		assertD2Schema3CanonicalCounts(manifest, expectedManifest, fixture.updatedAt, expectedRows)
		assertD2ActiveVirtualRowsFactsEquivalent(spoolDir, expectedManifest.MountPath)
	})

	Convey("D3.6 actual summarise command path loads and publishes schema3 spool rows", t, func() {
		harness := newB3CLIClickHouseHarness(t)
		cfg := harness.newConfig()
		cfg.QueryTimeout = 10 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{summariseTestMountPath}

		fixture := newSummariseActiveSnapshotFixture(t)
		writeBasedirsSpoolFixtureStats(t, fixture.statsPath, fixture.updatedAt)

		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() { clickhouse.ResetTreeQueryCaches() })

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickhouseDSN = cfg.DSN
		clickhouseDatabase = cfg.Database
		quotaPath = filepath.Join(fixture.outputDir, "quota.csv")
		basedirsConfig = filepath.Join(fixture.outputDir, "basedirs.tsv")
		basedirsDB = filepath.Join(fixture.outputDir, basedirBasename)
		mounts = filepath.Join(fixture.outputDir, "mounts.txt")

		So(os.WriteFile(quotaPath, []byte("7,/mnt/test,1000,100\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(basedirsConfig, []byte("/mnt/test\t1\t3\n"), 0o600), ShouldBeNil)
		So(os.WriteFile(mounts, []byte("\"/mnt/test/\"\n"), 0o600), ShouldBeNil)

		refreshedAt := fixture.updatedAt.Add(time.Hour)
		summariseSpoolNow = func() time.Time {
			return refreshedAt
		}
		dirgutaReferenceAt := d2Schema3DirGUTAReferenceTime()
		summariseSpoolDirGUTANow = func() time.Time {
			return dirgutaReferenceAt
		}

		orderingConn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(orderingConn.Close(), ShouldBeNil) }()

		orderingCtx, orderingCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer orderingCancel()

		sawReadinessBeforeActiveMount := false
		loadSummariseClickHouseSpool = func(
			ctx context.Context,
			cfg clickhouse.Config,
			spoolDir string,
			manifest *chspool.Manifest,
			recorder func(string, time.Duration),
		) (perfreport.Report, error) {
			wrappedRecorder := func(phase string, duration time.Duration) {
				recorder(phase, duration)

				if phase != "wrstat_active_virtual_ready" {
					return
				}

				activeSetID := d2ExpectedActiveSetID(*manifest, fixture.updatedAt)
				assertD3ReadinessVisibleBeforeActiveMount(orderingCtx, orderingConn, manifest, activeSetID)

				sawReadinessBeforeActiveMount = true
			}

			return clickhouse.LoadSummariseSpoolReport(ctx, cfg, spoolDir, manifest, wrappedRecorder)
		}

		So(run([]string{fixture.statsPath}), ShouldBeNil)
		So(sawReadinessBeforeActiveMount, ShouldBeTrue)

		spoolDir := summariseClickHouseSpoolDir(fixture.outputDir)
		manifest, err := chspool.ReadManifest(spoolDir)
		So(err, ShouldBeNil)

		report := readSummariseSpoolLoadReport(t, spoolDir)
		So(summariseSpoolReportHasOperation(report, "spool_load_total"), ShouldBeTrue)

		conn := openB3CLIClickHouseConn(t, cfg.DSN)
		defer func() { So(conn.Close(), ShouldBeNil) }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		expectedRows := d2Schema3ExpectedRowCounts()
		activeSetID := d2ExpectedActiveSetID(*manifest, fixture.updatedAt)
		assertD3LoadedSchema3Counts(ctx, conn, manifest, activeSetID, expectedRows)
		assertD3LoadedSchema3Readiness(ctx, conn, manifest, fixture.updatedAt, activeSetID, expectedRows)
		assertD3ColdSchema3Probes(ctx, conn, cfg, manifest)
	})
}

func writeBasedirsSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	writeBasedirsSpoolFixtureStatsForMount(t, statsPath, summariseTestMountPath, updatedAt)
}

func d2Schema3DirGUTAReferenceTime() time.Time {
	return time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC)
}

func writeNonContiguousSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/one.dat", 'f', 10, 12, 22, updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/two.dat", 'f', 20, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/", 'd', 4096, 15, 25, updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/three.txt", 'f', 30, 16, 26, updatedAt.Unix(), 306, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeRepeatedDirectoryBoundarySpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/one.dat", 'f', 10, 12, 22, updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"b/two.dat", 'f', 20, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/", 'd', 4096, 15, 25, updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"a/deep/three.txt", 'f', 30, 16, 26, updatedAt.Unix(), 306, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writePrefixSharingDirectorySiblingSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project.v2/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"project.v2/result.dat", 'f', 20, 12, 22, updatedAt.Unix(), 302, 1,
	)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/root.dat", 'f', 10, 14, 24, updatedAt.Unix(), 304, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeSameNameFileDirectorySpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"next/", 'd', 4096, 12, 22, updatedAt.Unix(), 302, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash/leaf.dat", 'f', 20, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash", 'f', 10, 13, 23, updatedAt.Unix(), 303, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeEscapedUnicodeSiblingSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1\u00a0/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"chr1\u00a0/leaf.dat", 'f', 20, 12, 22, updatedAt.Unix(), 302, 1,
	)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr2/two.dat", 'f', 30, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1/file.dat", 'f', 10, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr2/", 'd', 4096, 15, 25, updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1/", 'd', 4096, 16, 26, updatedAt.Unix(), 306, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeSlashlessDirectoryBoundarySpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"bins/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.004_sub/", 'd', 4096, 12, 22, updatedAt.Unix(), 302, 1,
	)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.004_sub/hmmer.tree.txt", 'f', 10, 13, 23,
		updatedAt.Unix(), 303, 1,
	)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"bins/maxbin2.006", 'd', 4096, 14, 24,
		updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.006/genes.faa", 'f', 20, 15, 25, updatedAt.Unix(), 305, 1,
	)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func assertSummariseSpoolPhaseIncomplete(t *testing.T, spoolDir string, phase string) {
	t.Helper()

	statePath := filepath.Join(spoolDir, "post_spool_publish_state.json")
	if _, err := os.Stat(statePath); errors.Is(err, os.ErrNotExist) {
		return
	}

	phases := readSummariseSpoolStatePhasesForTest(t, statePath)
	So(phases[phase], ShouldBeBlank)
}

func summariseSpoolReportUint64MapInputForTest(
	report perfreport.Report,
	operationName string,
	inputName string,
) map[string]uint64 {
	for _, op := range report.Operations {
		if op.Name != operationName {
			continue
		}

		raw, ok := op.Inputs[inputName].(map[string]any)
		So(ok, ShouldBeTrue)

		out := make(map[string]uint64, len(raw))
		for key, value := range raw {
			switch typed := value.(type) {
			case float64:
				out[key] = uint64(typed)
			case uint64:
				out[key] = typed
			}
		}

		return out
	}

	return nil
}

func TestSummariseContiguityTelemetry(t *testing.T) {
	Convey("recorder-free probes and dirbuild options allocate no shape recorder", t, func() {
		withoutShape := testing.AllocsPerRun(100, func() {
			_, state, err := newSummariseSpoolContiguityProbe(summariseTestMountPath, false)
			So(err, ShouldBeNil)
			runtime.KeepAlive(state)
		})
		withShape := testing.AllocsPerRun(100, func() {
			_, state, err := newSummariseSpoolContiguityProbe(summariseTestMountPath, true)
			So(err, ShouldBeNil)
			runtime.KeepAlive(state)
		})

		withoutRecorder := summariseSpoolDirbuildOptions(t.TempDir(), new(dirbuild.DiskMetrics), nil)
		So(withoutRecorder.Progress, ShouldBeNil)

		diag := newSummariseDiagnostics("input")
		withRecorder := summariseSpoolDirbuildOptions(t.TempDir(), new(dirbuild.DiskMetrics), diag)

		So(withRecorder.Progress, ShouldNotBeNil)
		So(withoutShape, ShouldBeLessThan, withShape)
	})

	Convey("contiguity telemetry reports distinct heavy descendants relative to a non-root mount", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)

		var input bytes.Buffer
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath, 'd', 4096, 10, 20, fixture.updatedAt.Unix(), 300, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"alpha/", 'd',
			4096, 11, 21, fixture.updatedAt.Unix(), 301, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"alpha/deep/", 'd',
			4096, 12, 22, fixture.updatedAt.Unix(), 302, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"alpha/deep/file.dat", 'f',
			10, 13, 23, fixture.updatedAt.Unix(), 303, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"beta/", 'd',
			4096, 14, 24, fixture.updatedAt.Unix(), 304, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"beta/deep/", 'd',
			4096, 15, 25, fixture.updatedAt.Unix(), 305, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"beta/deep/file.dat", 'f',
			20, 16, 26, fixture.updatedAt.Unix(), 306, 1,
		)
		writeSpoolFixtureStatsRow(
			&input, summariseTestMountPath+"tail.dat", 'f',
			30, 17, 27, fixture.updatedAt.Unix(), 307, 1,
		)
		writeGzipStats(t, fixture.statsPath, input.Bytes())

		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		result, err := summariseSpoolProbeContiguity(
			fixture.statsPath,
			summariseTestMountPath,
			newSummariseDiagnostics(fixture.statsPath),
		)

		So(err, ShouldBeNil)
		So(result.contiguous, ShouldBeTrue)

		So(logs.String(), ShouldContainSubstring,
			"heavy_prefixes="+quoteForDiagnostics("alpha/:2,beta/:2"))
	})
}

func writeContiguousPrefixSharingDirectorySiblingSpoolFixtureStats(
	t *testing.T,
	statsPath string,
	updatedAt time.Time,
) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/", 'd', 4096, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project/root.dat", 'f', 10, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"project.v2/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"project.v2/result.dat", 'f', 20, 12, 22, updatedAt.Unix(), 302, 1,
	)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeContiguousSameNameFileDirectorySpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash/leaf.dat", 'f', 20, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"clash", 'f', 10, 13, 23, updatedAt.Unix(), 303, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"next/", 'd', 4096, 12, 22, updatedAt.Unix(), 302, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeContiguousEscapedUnicodeSiblingSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1/", 'd', 4096, 16, 26, updatedAt.Unix(), 306, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1/file.dat", 'f', 10, 14, 24, updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr1\u00a0/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"chr1\u00a0/leaf.dat", 'f', 20, 12, 22, updatedAt.Unix(), 302, 1,
	)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr2/", 'd', 4096, 15, 25, updatedAt.Unix(), 305, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"chr2/two.dat", 'f', 30, 13, 23, updatedAt.Unix(), 303, 1)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeContiguousSlashlessDirectoryBoundarySpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	var buf bytes.Buffer

	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath, 'd', 4096, 10, 20, updatedAt.Unix(), 300, 1)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"bins/", 'd', 4096, 11, 21, updatedAt.Unix(), 301, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.004_sub/", 'd', 4096, 12, 22, updatedAt.Unix(), 302, 1,
	)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.004_sub/hmmer.tree.txt", 'f', 10, 13, 23,
		updatedAt.Unix(), 303, 1,
	)
	writeSpoolFixtureStatsRow(&buf, summariseTestMountPath+"bins/maxbin2.006", 'd', 4096, 14, 24,
		updatedAt.Unix(), 304, 1)
	writeSpoolFixtureStatsRow(
		&buf, summariseTestMountPath+"bins/maxbin2.006/genes.faa", 'f', 20, 15, 25, updatedAt.Unix(), 305, 1,
	)

	writeGzipStats(t, statsPath, buf.Bytes())
	So(os.Chtimes(statsPath, updatedAt, updatedAt), ShouldBeNil)
}

func writeSpoolFixtureStatsRow(
	buf *bytes.Buffer,
	path string,
	entryType byte,
	size int64,
	uid uint32,
	gid uint32,
	stamp int64,
	inode uint64,
	nlink uint64,
) {
	fmt.Fprintf(
		buf,
		"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t%d\t1\t%d\n",
		path,
		size,
		uid,
		gid,
		stamp,
		stamp,
		stamp,
		entryType,
		inode,
		nlink,
		size,
	)
}

func assertSpoolCatalogIntervals(dirsByPath map[string]chspool.DirRow) {
	for _, parent := range dirsByPath {
		for _, child := range dirsByPath {
			inInterval := child.DirID >= parent.DirID && child.DirID < parent.SubtreeEnd
			isDescendant := spoolCatalogDescendantOrSelf(parent.FullPath, child.FullPath)

			So(inInterval, ShouldEqual, isDescendant)
		}
	}
}

func spoolCatalogDescendantOrSelf(parent string, child string) bool {
	return parent == "/" || child == parent || strings.HasPrefix(child, parent)
}

func d2Schema3ExpectedRowCounts() map[string]uint64 {
	return map[string]uint64{
		chspool.TableDirs:                   4,
		chspool.TableDirFacts:               4,
		chspool.TableDirFilterAll:           34,
		chspool.TableSchema3SnapshotSets:    1,
		chspool.TableActiveVirtualDirs:      3,
		chspool.TableActiveVirtualSummaries: 3,
		chspool.TableActiveVirtualFilterAll: 51,
		chspool.TableActiveVirtualChildren:  2,
		chspool.TableActiveVirtualSets:      1,
	}
}

func d2Schema3ExpectedTablesFromCanonicalManifest(
	canonicalManifest *chspool.Manifest,
	expectedRows map[string]uint64,
) map[string]chspool.TableManifest {
	out := make(map[string]chspool.TableManifest, len(expectedRows))

	for table, expectedRows := range expectedRows {
		tm, ok := canonicalManifest.Tables[table]
		So(ok, ShouldBeTrue)
		So(tm.Rows, ShouldEqual, expectedRows)
		So(tm.Bytes, ShouldBeGreaterThan, int64(0))
		So(tm.SHA256, ShouldNotBeBlank)

		out[table] = tm
	}

	return out
}

func assertD2Schema3ManifestTables(
	spoolDir string,
	manifest *chspool.Manifest,
	expectedRows map[string]uint64,
	expectedTables map[string]chspool.TableManifest,
	expectedSpoolDir string,
) {
	var seen uint64

	for _, table := range chspool.TableOrder() {
		expectedRowCount, ok := expectedRows[table]
		if !ok {
			continue
		}

		seen++
		tm, ok := manifest.Tables[table]
		So(ok, ShouldBeTrue)
		So(tm.Table, ShouldEqual, expectedTables[table].Table)
		So(tm.Path, ShouldEqual, expectedTables[table].Path)
		So(tm.Rows, ShouldEqual, expectedTables[table].Rows)
		So(tm.Bytes, ShouldBeGreaterThan, int64(0))
		So(tm.SHA256, ShouldNotBeBlank)
		So(d2DecodedRowsForTable(spoolDir, table), ShouldEqual, expectedRowCount)
		So(
			d2DecodedRowFingerprintsForTable(spoolDir, table),
			ShouldResemble,
			d2DecodedRowFingerprintsForTable(expectedSpoolDir, table),
		)
	}

	So(seen, ShouldEqual, uint64(len(expectedRows)))
}

func d2DecodedRowsForTable(spoolDir string, table string) uint64 {
	var rows uint64

	switch table {
	case chspool.TableDirs:
		So(chspool.DecodeRows[chspool.DirRow](spoolDir, table, func(chspool.DirRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableFiles:
		So(chspool.DecodeRows[chspool.FileRow](spoolDir, table, func(chspool.FileRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableDirFacts:
		So(chspool.DecodeRows[chspool.DirFactRow](spoolDir, table, func(chspool.DirFactRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableDirFilterAgeAll:
		So(chspool.DecodeRows[chspool.DirFilterAgeAllRow](spoolDir, table, func(chspool.DirFilterAgeAllRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableChildFilterAll:
		So(chspool.DecodeRows[chspool.ChildFilterAllRow](spoolDir, table, func(chspool.ChildFilterAllRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableDirFilterAll:
		So(chspool.DecodeRows[chspool.DirFilterAllRow](spoolDir, table, func(chspool.DirFilterAllRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableSchema3SnapshotSets:
		So(chspool.DecodeRows[chspool.Schema3SnapshotSetRow](spoolDir, table, func(chspool.Schema3SnapshotSetRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualDirs:
		So(chspool.DecodeRows[chspool.ActiveVirtualDirRow](spoolDir, table, func(chspool.ActiveVirtualDirRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualSummaries:
		So(chspool.DecodeRows[chspool.ActiveVirtualSummaryRow](spoolDir, table, func(chspool.ActiveVirtualSummaryRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualFilterAll:
		So(chspool.DecodeRows[chspool.ActiveVirtualFilterAllRow](
			spoolDir,
			table,
			func(chspool.ActiveVirtualFilterAllRow) error {
				rows++

				return nil
			},
		), ShouldBeNil)
	case chspool.TableActiveVirtualChildren:
		So(chspool.DecodeRows[chspool.ActiveVirtualChildRow](spoolDir, table, func(chspool.ActiveVirtualChildRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableActiveVirtualSets:
		So(chspool.DecodeRows[chspool.ActiveVirtualSetRow](spoolDir, table, func(chspool.ActiveVirtualSetRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	case chspool.TableDirProjectionSets:
		So(chspool.DecodeRows[chspool.DirProjectionSetRow](spoolDir, table, func(chspool.DirProjectionSetRow) error {
			rows++

			return nil
		}), ShouldBeNil)
	}

	return rows
}

func d2DecodedRowFingerprintsForTable(spoolDir string, table string) []string {
	switch table {
	case chspool.TableDirs:
		return d2DecodedRowFingerprints[chspool.DirRow](spoolDir, table)
	case chspool.TableFiles:
		return d2DecodedRowFingerprints[chspool.FileRow](spoolDir, table)
	case chspool.TableDirFacts:
		return d2DecodedRowFingerprints[chspool.DirFactRow](spoolDir, table)
	case chspool.TableDirFilterAgeAll:
		return d2DecodedRowFingerprints[chspool.DirFilterAgeAllRow](spoolDir, table)
	case chspool.TableChildFilterAll:
		return d2DecodedRowFingerprints[chspool.ChildFilterAllRow](spoolDir, table)
	case chspool.TableDirFilterAll:
		return d2DecodedRowFingerprints[chspool.DirFilterAllRow](spoolDir, table)
	case chspool.TableSchema3SnapshotSets:
		return d2DecodedRowFingerprints[chspool.Schema3SnapshotSetRow](spoolDir, table)
	case chspool.TableActiveVirtualDirs:
		return d2DecodedRowFingerprints[chspool.ActiveVirtualDirRow](spoolDir, table)
	case chspool.TableActiveVirtualSummaries:
		return d2DecodedRowFingerprints[chspool.ActiveVirtualSummaryRow](spoolDir, table)
	case chspool.TableActiveVirtualFilterAll:
		return d2DecodedRowFingerprints[chspool.ActiveVirtualFilterAllRow](spoolDir, table)
	case chspool.TableActiveVirtualChildren:
		return d2DecodedRowFingerprints[chspool.ActiveVirtualChildRow](spoolDir, table)
	case chspool.TableActiveVirtualSets:
		return d2DecodedRowFingerprints[chspool.ActiveVirtualSetRow](spoolDir, table)
	case chspool.TableDirProjectionSets:
		return d2DecodedRowFingerprints[chspool.DirProjectionSetRow](spoolDir, table)
	}

	return nil
}

func assertD2Schema3CanonicalCounts(
	manifest *chspool.Manifest,
	expectedManifest chspool.Manifest,
	updatedAt time.Time,
	expectedRows map[string]uint64,
) {
	var snapshotSets []chspool.Schema3SnapshotSetRow

	So(chspool.DecodeRows[chspool.Schema3SnapshotSetRow](
		summariseClickHouseSpoolDir(expectedManifest.OutputDir),
		chspool.TableSchema3SnapshotSets,
		func(row chspool.Schema3SnapshotSetRow) error {
			snapshotSets = append(snapshotSets, row)

			return nil
		},
	), ShouldBeNil)
	So(snapshotSets, ShouldHaveLength, 1)

	snapshotCounts := d2Schema3ExpectedSnapshotCounts(expectedRows)
	So(snapshotSets[0].DirsRows, ShouldEqual, snapshotCounts.dirsRows)
	So(snapshotSets[0].DirFactsRows, ShouldEqual, snapshotCounts.dirFactsRows)
	So(snapshotSets[0].ChildFilterAllRows, ShouldEqual, snapshotCounts.childFilterAllRows)
	So(snapshotSets[0].DirFilterAllRows, ShouldEqual, snapshotCounts.dirFilterAllRows)
	So(snapshotSets[0].ManifestSHA256, ShouldEqual, d2ExpectedSchema3SnapshotDigest(expectedManifest, snapshotCounts))

	var activeSets []chspool.ActiveVirtualSetRow

	So(chspool.DecodeRows[chspool.ActiveVirtualSetRow](
		summariseClickHouseSpoolDir(expectedManifest.OutputDir),
		chspool.TableActiveVirtualSets,
		func(row chspool.ActiveVirtualSetRow) error {
			activeSets = append(activeSets, row)

			return nil
		},
	), ShouldBeNil)
	So(activeSets, ShouldHaveLength, 1)

	activeSetID := d2ExpectedActiveSetID(expectedManifest, updatedAt)
	So(activeSets[0].ActiveSetID, ShouldEqual, activeSetID)
	So(activeSets[0].MountsSHA256, ShouldEqual, activeSetID)
	So(activeSets[0].ActiveMountCount, ShouldEqual, uint64(1))
	So(activeSets[0].Ready, ShouldEqual, uint8(1))
	So(activeSets[0].SummaryRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualSummaries])
	So(activeSets[0].FilterRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualFilterAll])
	So(activeSets[0].ChildRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualChildren])
	So(activeSets[0].ManifestSHA256, ShouldEqual, d2ExpectedActiveVirtualDigest(activeSetID, expectedRows))
	So(manifest.Tables[chspool.TableDirFilterAll].Rows, ShouldEqual, expectedRows[chspool.TableDirFilterAll])
}

func assertD2ActiveVirtualRowsFactsEquivalent(spoolDir string, mountPath string) {
	virtualDirsByID := d2ActiveVirtualDirsByID(spoolDir)
	summaries := d2ActiveVirtualSummaryRowsByDir(spoolDir, virtualDirsByID)
	virtualDirs := []string{"/", summariseSpoolVirtualNamespaceDir, mountPath}

	So(d2SortedActiveVirtualSummaryDirs(summaries), ShouldResemble, virtualDirs)
	assertD2ActiveVirtualCatalogRows(spoolDir, virtualDirsByID, mountPath)

	rootFact := d2RootDirFactRow(spoolDir, mountPath)
	expectedSummaryDigest := d2RootDirFactSummaryDigest(rootFact)

	for _, dir := range virtualDirs {
		row := summaries[dir]
		So(d2ActiveVirtualSummaryDigest(row), ShouldEqual, expectedSummaryDigest)
		So(row.ChildCount, ShouldEqual, uint64(1))
	}

	So(summaries["/"].MountPath, ShouldBeBlank)
	So(summaries["/"].IsMountRootBox, ShouldEqual, uint8(0))
	So(summaries[summariseSpoolVirtualNamespaceDir].MountPath, ShouldBeBlank)
	So(summaries[summariseSpoolVirtualNamespaceDir].IsMountRootBox, ShouldEqual, uint8(0))
	So(summaries[mountPath].MountPath, ShouldEqual, mountPath)
	So(summaries[mountPath].IsMountRootBox, ShouldEqual, uint8(1))

	rootFilters := d2RootDirFilterRowsByTuple(spoolDir, mountPath)
	activeFilters := d2ActiveVirtualFilterRowsByDirAndTuple(spoolDir, virtualDirsByID)

	for _, dir := range virtualDirs {
		rowsByTuple := activeFilters[dir]
		So(rowsByTuple, ShouldHaveLength, len(rootFilters))

		for tuple, expected := range rootFilters {
			got, ok := rowsByTuple[tuple]
			So(ok, ShouldBeTrue)
			So(d2ActiveVirtualFilterDigest(got), ShouldEqual, d2DirFilterDigest(expected))
			So(got.FilterChildCount, ShouldEqual, summaries[dir].ChildCount)
			So(got.ChildCount, ShouldEqual, summaries[dir].ChildCount)
		}
	}

	fullTuple := summariseFullFilterTupleKey{
		age: uint8(db.DGUTAgeAll),
		gid: 7,
		uid: 17,
		ft:  uint16(db.DGUTAFileTypeBam),
	}
	for _, dir := range virtualDirs {
		So(activeFilters[dir][fullTuple].Count, ShouldEqual, uint64(1))
		So(activeFilters[dir][fullTuple].Size, ShouldEqual, uint64(50))
	}
}

func d2ActiveVirtualDirsByID(spoolDir string) map[uint32]chspool.ActiveVirtualDirRow {
	out := make(map[uint32]chspool.ActiveVirtualDirRow)

	So(chspool.DecodeRows[chspool.ActiveVirtualDirRow](
		spoolDir,
		chspool.TableActiveVirtualDirs,
		func(row chspool.ActiveVirtualDirRow) error {
			out[row.VirtualID] = row

			return nil
		},
	), ShouldBeNil)

	return out
}

func d2ActiveVirtualSummaryRowsByDir(
	spoolDir string,
	virtualDirsByID map[uint32]chspool.ActiveVirtualDirRow,
) map[string]chspool.ActiveVirtualSummaryRow {
	out := make(map[string]chspool.ActiveVirtualSummaryRow)

	So(chspool.DecodeRows[chspool.ActiveVirtualSummaryRow](
		spoolDir,
		chspool.TableActiveVirtualSummaries,
		func(row chspool.ActiveVirtualSummaryRow) error {
			out[virtualDirsByID[row.VirtualID].FullPath] = row

			return nil
		},
	), ShouldBeNil)

	return out
}

func d2SortedActiveVirtualSummaryDirs(rows map[string]chspool.ActiveVirtualSummaryRow) []string {
	dirs := make([]string, 0, len(rows))
	for dir := range rows {
		dirs = append(dirs, dir)
	}

	slices.Sort(dirs)

	return dirs
}

func assertD2ActiveVirtualCatalogRows(
	spoolDir string,
	rows map[uint32]chspool.ActiveVirtualDirRow,
	mountPath string,
) {
	root := rows[summariseActiveVirtualRootID]
	namespace := rows[2]
	mountRoot := rows[3]

	So(root.FullPath, ShouldEqual, "/")
	So(root.ParentID, ShouldEqual, summariseActiveVirtualNoParentID)
	So(root.SnapshotID, ShouldEqual, summariseActiveVirtualZeroSnapshot)
	So(namespace.FullPath, ShouldEqual, summariseSpoolVirtualNamespaceDir)
	So(namespace.ParentID, ShouldEqual, summariseActiveVirtualRootID)
	So(namespace.SnapshotID, ShouldEqual, summariseActiveVirtualZeroSnapshot)
	So(mountRoot.FullPath, ShouldEqual, mountPath)
	So(mountRoot.ParentID, ShouldEqual, namespace.VirtualID)
	So(mountRoot.MountPath, ShouldEqual, mountPath)
	So(mountRoot.IsMountRootBox, ShouldEqual, uint8(1))
	So(mountRoot.SnapshotID, ShouldNotBeBlank)
	So(mountRoot.MountRootDirID, ShouldEqual, d2DirIDForPath(spoolDir, mountPath))
}

func d2RootDirFactRow(spoolDir string, mountPath string) chspool.DirFactRow {
	var (
		out   chspool.DirFactRow
		found bool
	)

	rootID := d2DirIDForPath(spoolDir, mountPath)

	So(chspool.DecodeRows[chspool.DirFactRow](spoolDir, chspool.TableDirFacts, func(row chspool.DirFactRow) error {
		if row.DirID == rootID {
			out = row
			found = true
		}

		return nil
	}), ShouldBeNil)
	So(found, ShouldBeTrue)

	return out
}

func d2RootDirFilterRowsByTuple(
	spoolDir string,
	mountPath string,
) map[summariseFullFilterTupleKey]chspool.DirFilterAllRow {
	out := make(map[summariseFullFilterTupleKey]chspool.DirFilterAllRow)
	rootID := d2DirIDForPath(spoolDir, mountPath)

	So(chspool.DecodeRows[chspool.DirFilterAllRow](
		spoolDir,
		chspool.TableDirFilterAll,
		func(row chspool.DirFilterAllRow) error {
			if row.DirID == rootID {
				out[summariseFullFilterKeyForRow(row)] = row
			}

			return nil
		},
	), ShouldBeNil)
	So(out, ShouldHaveLength, 17)

	return out
}

func d2DirIDForPath(spoolDir string, fullPath string) uint32 {
	var (
		dirID uint32
		found bool
	)

	So(chspool.DecodeRows[chspool.DirRow](spoolDir, chspool.TableDirs, func(row chspool.DirRow) error {
		if row.FullPath == fullPath {
			dirID = row.DirID
			found = true
		}

		return nil
	}), ShouldBeNil)
	So(found, ShouldBeTrue)

	return dirID
}

func d2ActiveVirtualFilterRowsByDirAndTuple(
	spoolDir string,
	virtualDirsByID map[uint32]chspool.ActiveVirtualDirRow,
) map[string]map[summariseFullFilterTupleKey]chspool.ActiveVirtualFilterAllRow {
	out := make(map[string]map[summariseFullFilterTupleKey]chspool.ActiveVirtualFilterAllRow)

	So(chspool.DecodeRows[chspool.ActiveVirtualFilterAllRow](
		spoolDir,
		chspool.TableActiveVirtualFilterAll,
		func(row chspool.ActiveVirtualFilterAllRow) error {
			dir := virtualDirsByID[row.VirtualID].FullPath
			if out[dir] == nil {
				out[dir] = make(map[summariseFullFilterTupleKey]chspool.ActiveVirtualFilterAllRow)
			}

			out[dir][summariseFullFilterTupleKey{
				age: row.Age,
				gid: row.GID,
				uid: row.UID,
				ft:  row.FT,
			}] = row

			return nil
		},
	), ShouldBeNil)

	return out
}

func assertD3ReadinessVisibleBeforeActiveMount(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	activeSetID string,
) {
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_schema3_snapshot_sets WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	), ShouldEqual, uint64(1))
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_active_virtual_sets WHERE active_set_id = ? AND ready = 1",
		activeSetID,
	), ShouldEqual, uint64(1))
	So(d3CountRows(
		ctx,
		conn,
		"SELECT count() FROM wrstat_mount_events WHERE mount_path = ? AND snapshot_id = toUUID(?) AND event_type = 1",
		manifest.MountPath,
		manifest.SnapshotID,
	), ShouldEqual, uint64(0))
}

func readSummariseSpoolLoadReport(t *testing.T, spoolDir string) perfreport.Report {
	t.Helper()

	data, err := os.ReadFile(summariseSpoolLoadReportPath(spoolDir))
	So(err, ShouldBeNil)

	var report perfreport.Report
	So(json.Unmarshal(data, &report), ShouldBeNil)

	return report
}

func summariseSpoolReportHasOperation(report perfreport.Report, name string) bool {
	for _, op := range report.Operations {
		if op.Name == name {
			return true
		}
	}

	return false
}

func assertD3LoadedSchema3Counts(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	activeSetID string,
	expectedRows map[string]uint64,
) {
	snapshotTables := map[string]uint64{
		chspool.TableChildFilterAll:      expectedRows[chspool.TableDirFilterAll],
		chspool.TableDirFilterAll:        expectedRows[chspool.TableDirFilterAll],
		chspool.TableSchema3SnapshotSets: expectedRows[chspool.TableSchema3SnapshotSets],
	}
	for table, expected := range snapshotTables {
		So(d3CountRows(
			ctx,
			conn,
			"SELECT count() FROM "+table+" WHERE mount_path = ? AND snapshot_id = toUUID(?)",
			manifest.MountPath,
			manifest.SnapshotID,
		), ShouldEqual, expected)

		if table != chspool.TableChildFilterAll {
			So(manifest.Tables[table].Rows, ShouldEqual, expected)
		}
	}

	for table, expected := range map[string]uint64{
		chspool.TableActiveVirtualDirs:      expectedRows[chspool.TableActiveVirtualDirs],
		chspool.TableActiveVirtualSummaries: expectedRows[chspool.TableActiveVirtualSummaries],
		chspool.TableActiveVirtualFilterAll: expectedRows[chspool.TableActiveVirtualFilterAll],
		chspool.TableActiveVirtualChildren:  expectedRows[chspool.TableActiveVirtualChildren],
		chspool.TableActiveVirtualSets:      expectedRows[chspool.TableActiveVirtualSets],
	} {
		So(d3CountRows(ctx, conn, "SELECT count() FROM "+table+" WHERE active_set_id = ?", activeSetID),
			ShouldEqual, expected)
		So(manifest.Tables[table].Rows, ShouldEqual, expected)
	}
}

func d3CountRows(ctx context.Context, conn ch.Conn, query string, args ...any) uint64 {
	row := conn.QueryRow(ctx, query, args...)

	var got uint64
	So(row.Scan(&got), ShouldBeNil)

	return got
}

func assertD3LoadedSchema3Readiness(
	ctx context.Context,
	conn ch.Conn,
	manifest *chspool.Manifest,
	updatedAt time.Time,
	activeSetID string,
	expectedRows map[string]uint64,
) {
	snapshotCounts := d2Schema3ExpectedSnapshotCounts(expectedRows)
	row := conn.QueryRow(
		ctx,
		"SELECT dirs_rows, dir_facts_rows, child_filter_all_rows, "+
			"dir_filter_all_rows, manifest_sha256 FROM wrstat_schema3_snapshot_sets "+
			"WHERE mount_path = ? AND snapshot_id = toUUID(?)",
		manifest.MountPath,
		manifest.SnapshotID,
	)

	var (
		dirsRows           uint64
		dirFactsRows       uint64
		childFilterAllRows uint64
		dirFilterAllRows   uint64
		manifestSHA256     string
	)
	So(row.Scan(
		&dirsRows,
		&dirFactsRows,
		&childFilterAllRows,
		&dirFilterAllRows,
		&manifestSHA256,
	), ShouldBeNil)
	So(dirsRows, ShouldEqual, snapshotCounts.dirsRows)
	So(dirFactsRows, ShouldEqual, snapshotCounts.dirFactsRows)
	So(childFilterAllRows, ShouldEqual, snapshotCounts.childFilterAllRows)
	So(dirFilterAllRows, ShouldEqual, snapshotCounts.dirFilterAllRows)
	So(manifestSHA256, ShouldEqual, d2ExpectedSchema3SnapshotDigest(*manifest, snapshotCounts))

	row = conn.QueryRow(
		ctx,
		"SELECT ready, summary_rows, filter_rows, child_rows, manifest_sha256 "+
			"FROM wrstat_active_virtual_sets WHERE active_set_id = ?",
		activeSetID,
	)

	var (
		ready       uint8
		summaryRows uint64
		filterRows  uint64
		childRows   uint64
		activeSHA   string
	)
	So(row.Scan(&ready, &summaryRows, &filterRows, &childRows, &activeSHA), ShouldBeNil)
	So(ready, ShouldEqual, uint8(1))
	So(summaryRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualSummaries])
	So(filterRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualFilterAll])
	So(childRows, ShouldEqual, expectedRows[chspool.TableActiveVirtualChildren])
	So(activeSHA, ShouldEqual, d2ExpectedActiveVirtualDigest(activeSetID, expectedRows))
	So(activeSetID, ShouldEqual, d2ExpectedActiveSetID(*manifest, updatedAt))
}

func d2Schema3ExpectedSnapshotCounts(expectedRows map[string]uint64) summariseSchema3SnapshotCounts {
	return summariseSchema3SnapshotCounts{
		dirsRows:           expectedRows[chspool.TableDirs],
		dirFactsRows:       expectedRows[chspool.TableDirFacts],
		dirFilterAllRows:   expectedRows[chspool.TableDirFilterAll],
		childFilterAllRows: expectedRows[chspool.TableDirFilterAll],
	}
}

func d2ExpectedSchema3SnapshotDigest(
	expectedManifest chspool.Manifest,
	counts summariseSchema3SnapshotCounts,
) string {
	return d2SHA256Hex(fmt.Sprintf(
		"%s|%s|%d|%d|%d|%d|%d",
		expectedManifest.MountPath,
		expectedManifest.SnapshotID,
		clickHouseSpoolSchema3Version,
		counts.dirsRows,
		counts.dirFactsRows,
		counts.childFilterAllRows,
		counts.dirFilterAllRows,
	))
}

func d2ExpectedActiveVirtualDigest(activeSetID string, expectedRows map[string]uint64) string {
	return d2SHA256Hex(fmt.Sprintf(
		"%s|%d|%d|%d|%d",
		activeSetID,
		clickHouseSpoolSchema3Version,
		expectedRows[chspool.TableActiveVirtualSummaries],
		expectedRows[chspool.TableActiveVirtualFilterAll],
		expectedRows[chspool.TableActiveVirtualChildren],
	))
}

func d2SHA256Hex(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
}

func d2ExpectedActiveSetID(expectedManifest chspool.Manifest, updatedAt time.Time) string {
	return d2ExpectedActiveSetIDForRows([]summariseActiveSetRowForTest{{
		mountPath:  expectedManifest.MountPath,
		snapshotID: expectedManifest.SnapshotID,
		updatedAt:  updatedAt,
	}})
}

func assertD3ColdSchema3Probes(
	ctx context.Context,
	conn ch.Conn,
	cfg clickhouse.Config,
	manifest *chspool.Manifest,
) {
	clickhouse.ResetTreeQueryCaches()
	clickhouse.ResetTreeQueryCacheStats(cfg)
	clickhouse.ResetSchema3FallbackRoutes()

	p, err := clickhouse.OpenProvider(cfg)

	So(err, ShouldBeNil)
	defer func() { So(p.Close(), ShouldBeNil) }()

	tree := p.Tree()
	So(tree, ShouldNotBeNil)

	broadFilter := &db.Filter{Age: db.DGUTAgeAll}
	fullFilter := &db.Filter{
		GIDs: []uint32{7},
		UIDs: []uint32{17},
		FT:   db.DGUTAFileTypeBam,
		Age:  db.DGUTAgeAll,
	}
	virtualDirs := []string{"/", summariseSpoolVirtualNamespaceDir, manifest.MountPath}
	broadDigest := d3RootBroadFactDigest(ctx, conn, manifest)
	fullDigest := d3RootFullFilterDigest(ctx, conn, manifest, fullFilter)

	for _, dir := range virtualDirs {
		clickhouse.ResetTreeQueryCacheStats(cfg)
		clickhouse.ResetSchema3FallbackRoutes()
		d3AssertDirSummaryDigest(tree, dir, broadFilter, broadDigest)
		d3AssertNoFactVectorFallback(cfg, "virtual-summary-broad-"+dir)

		clickhouse.ResetTreeQueryCacheStats(cfg)
		clickhouse.ResetSchema3FallbackRoutes()
		d3AssertDirSummaryDigest(tree, dir, fullFilter, fullDigest)
		d3AssertNoFactVectorFallback(cfg, "virtual-summary-full-"+dir)
	}

	clickhouse.ResetTreeQueryCacheStats(cfg)
	clickhouse.ResetSchema3FallbackRoutes()
	d3AssertChildrenProbe(tree, virtualDirs[:2], broadFilter)
	d3AssertNoFactVectorFallback(cfg, "virtual-broad-children")

	clickhouse.ResetTreeQueryCacheStats(cfg)
	clickhouse.ResetSchema3FallbackRoutes()
	d3AssertChildrenProbe(tree, virtualDirs[:2], fullFilter)
	d3AssertNoFactVectorFallback(cfg, "virtual-full-children")

	projectDir := summariseTestMountPath + "project/"

	clickhouse.ResetTreeQueryCacheStats(cfg)
	clickhouse.ResetSchema3FallbackRoutes()

	info, err := tree.DirInfo(projectDir, fullFilter)
	So(err, ShouldBeNil)
	So(info.Current.Count, ShouldEqual, uint64(1))

	haveChildren := tree.DirsHaveChildren([]string{projectDir}, fullFilter)
	So(haveChildren[projectDir], ShouldBeFalse)

	dcss, err := tree.Where(projectDir, fullFilter, split.SplitsToSplitFn(0))
	So(err, ShouldBeNil)
	So(dcss, ShouldHaveLength, 1)

	So(clickhouse.ReadSchema3FallbackRoutes(), ShouldResemble, map[string]uint64{})
}

func d3AssertDirSummaryDigest(tree *db.Tree, dir string, filter *db.Filter, expectedDigest string) {
	info, err := tree.DirSummary(dir, filter)
	So(err, ShouldBeNil)
	So(d3DirSummaryDigest(info), ShouldEqual, expectedDigest)
}

func d3AssertNoFactVectorFallback(cfg clickhouse.Config, label string) {
	stats := clickhouse.ReadTreeQueryCacheStats(cfg)
	So(fmt.Sprintf("%s fact vector reads=%d", label, stats.FactVectorReads), ShouldEqual,
		label+" fact vector reads=0")
	So(clickhouse.ReadSchema3FallbackRoutes(), ShouldResemble, map[string]uint64{})
}

func d3AssertChildrenProbe(tree *db.Tree, dirs []string, filter *db.Filter) {
	haveChildren := tree.DirsHaveChildren(dirs, filter)

	for _, dir := range dirs {
		So(fmt.Sprintf("%s=%t", dir, haveChildren[dir]), ShouldEqual, dir+"=true")
	}
}
