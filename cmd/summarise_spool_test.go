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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

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
		) error {
			loadCalls++

			So(spoolDir, ShouldEqual, summariseClickHouseSpoolDir(fixture.outputDir))
			So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThan, uint64(0))

			if loadCalls == 1 {
				return errSummariseTestClose
			}

			return nil
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
		) error {
			loadCalls++

			return errSummariseTestClose
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
}

func TestSummariseClickHouseSpoolRows(t *testing.T) {
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

		So(manifest.Tables[chspool.TableFiles].Rows, ShouldBeGreaterThanOrEqualTo, uint64(3))
		So(manifest.Tables[chspool.TableChildren].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirFacts].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableDirFilterAgeAll].Rows, ShouldBeGreaterThan, uint64(0))
		So(manifest.Tables[chspool.TableParentFacts].Rows, ShouldEqual, manifest.Tables[chspool.TableDirFacts].Rows)
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
		So(files[len(files)-1].SnapshotID, ShouldEqual, manifest.SnapshotID)

		var facts []chspool.DirFactRow

		So(chspool.DecodeRows[chspool.DirFactRow](spoolDir, chspool.TableDirFacts, func(row chspool.DirFactRow) error {
			facts = append(facts, row)

			return nil
		}), ShouldBeNil)
		So(facts[0].GIDs, ShouldContain, uint32(7))
		So(facts[0].UIDs, ShouldContain, uint32(17))
	})
}

func writeBasedirsSpoolFixtureStats(t *testing.T, statsPath string, updatedAt time.Time) {
	t.Helper()

	root := statsdata.NewRoot(summariseTestMountPath, updatedAt.Unix())
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
