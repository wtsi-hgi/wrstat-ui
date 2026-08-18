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
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/internal/watchenv"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	summariseTestClickHouseDSN      = "clickhouse://default@127.0.0.1:9000/default"
	summariseTestClickHouseDatabase = "wrstat_ui_test"
	summariseTestMountPath          = "/mnt/test/"
)

var errSummariseTestClose = errors.New("close failed")

func TestSummariseSchedulerGuardWarning(t *testing.T) {
	Convey("A manual summarise emits an operational concurrency warning", t, func() {
		t.Setenv(watchenv.Name, "")

		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		warnIfSummariseUnguarded()

		So(logs.String(), ShouldContainSubstring, "not protected by the watch scheduler concurrency limit")
	})

	Convey("A watch-scheduled summarise emits no concurrency warning", t, func() {
		t.Setenv(watchenv.Name, watchenv.Value)

		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		warnIfSummariseUnguarded()

		So(logs.String(), ShouldBeBlank)
	})

	Convey("A user-supplied lookalike value does not suppress the warning", t, func() {
		t.Setenv(watchenv.Name, "1")

		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		warnIfSummariseUnguarded()

		So(logs.String(), ShouldContainSubstring, "not protected by the watch scheduler concurrency limit")
	})
}

type summariseActiveSnapshotFixture struct {
	outputDir        string
	statsPath        string
	updatedAt        time.Time
	groupUserPath    string
	userGroupPath    string
	groupUserContent []byte
	userGroupContent []byte
}

func newSummariseActiveSnapshotFixture(t *testing.T) summariseActiveSnapshotFixture {
	t.Helper()

	baseDir := t.TempDir()
	outputDir := filepath.Join(baseDir, "12345_\uff0fmnt\uff0ftest")
	So(os.MkdirAll(outputDir, summariseDirPerm), ShouldBeNil)

	statsPath := filepath.Join(baseDir, "stats.gz")
	updatedAt := time.Unix(1_710_000_000, 123).UTC()

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

func (f summariseActiveSnapshotFixture) clickHouseTarget() *clickHouseSummariseTarget {
	return &clickHouseSummariseTarget{
		mountPath: summariseTestMountPath,
		modtime:   f.updatedAt,
		outputDir: f.outputDir,
	}
}

func (f summariseActiveSnapshotFixture) writeValidStats(t *testing.T) {
	t.Helper()

	root := statsdata.NewRoot(summariseTestMountPath, f.updatedAt.Unix())
	file := root.AddFile("file")
	file.Size = 1
	file.Inode = 1
	file.Nlink = 1

	var buf bytes.Buffer

	_, err := root.WriteTo(&buf)
	So(err, ShouldBeNil)

	writeGzipStats(t, f.statsPath, buf.Bytes())
	So(os.Chtimes(f.statsPath, f.updatedAt, f.updatedAt), ShouldBeNil)
}

func TestSummariseClickHouseActiveSnapshotPreflight(t *testing.T) {
	Convey("watch retries clean an already-active ClickHouse snapshot "+
		"without a completion marker and reprocess", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, true)

		activeCheckCalled := false
		cleanupCalled := false
		wireCalled := false
		closeCalled := false

		clickHouseSnapshotIsActive = func(cfg clickhouse.Config, mountPath string, modtime time.Time) (bool, error) {
			activeCheckCalled = true

			So(cfg.DSN, ShouldEqual, summariseTestClickHouseDSN)
			So(cfg.Database, ShouldEqual, summariseTestClickHouseDatabase)
			So(mountPath, ShouldEqual, summariseTestMountPath)
			So(modtime.Equal(fixture.updatedAt), ShouldBeTrue)

			return true, nil
		}
		clickHouseCleanActiveSnapshotAttempt = func(
			cfg clickhouse.Config,
			mountPath string,
			modtime time.Time,
		) error {
			So(cfg.DSN, ShouldEqual, summariseTestClickHouseDSN)
			So(cfg.Database, ShouldEqual, summariseTestClickHouseDatabase)
			So(mountPath, ShouldEqual, summariseTestMountPath)
			So(modtime.Equal(fixture.updatedAt), ShouldBeTrue)

			cleanupCalled = true

			return nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
			_ *summariseDiagnostics,
		) (func(bool) error, error) {
			So(cleanupCalled, ShouldBeTrue)

			wireCalled = true

			return func(publish bool) error {
				So(publish, ShouldBeTrue)

				closeCalled = true

				return nil
			}, nil
		}
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			So(cleanupCalled, ShouldBeTrue)

			wireCalled = true
			closeCalled = true

			return perfreport.NewReport("clickhouse", "", 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(activeCheckCalled, ShouldBeTrue)
		So(cleanupCalled, ShouldBeTrue)
		So(wireCalled, ShouldBeTrue)
		So(closeCalled, ShouldBeTrue)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("watch retries accept an already-active ClickHouse snapshot with a matching completion marker", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, true)
		So(writeSummariseCompletionMarker(fixture.clickHouseTarget()), ShouldBeNil)

		cleanupCalled := false
		wireCalled := false

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return true, nil
		}
		clickHouseCleanActiveSnapshotAttempt = func(clickhouse.Config, string, time.Time) error {
			cleanupCalled = true

			return nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
			_ *summariseDiagnostics,
		) (func(bool) error, error) {
			wireCalled = true

			return func(bool) error { return nil }, nil
		}
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			wireCalled = true

			return perfreport.NewReport("clickhouse", "", 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(cleanupCalled, ShouldBeFalse)
		So(wireCalled, ShouldBeFalse)
		So(readFileBytes(fixture.groupUserPath), ShouldResemble, fixture.groupUserContent)
		So(readFileBytes(fixture.userGroupPath), ShouldResemble, fixture.userGroupContent)
	})

	Convey("manual summarise refuses an already-active ClickHouse snapshot before truncating outputs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)
		So(writeSummariseCompletionMarker(fixture.clickHouseTarget()), ShouldBeNil)

		cleanupCalled := false
		wireCalled := false

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return true, nil
		}
		clickHouseCleanActiveSnapshotAttempt = func(clickhouse.Config, string, time.Time) error {
			cleanupCalled = true

			return nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
			_ *summariseDiagnostics,
		) (func(bool) error, error) {
			wireCalled = true

			return func(bool) error { return nil }, nil
		}
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			wireCalled = true

			return perfreport.NewReport("clickhouse", "", 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "refusing to rewrite active snapshot")
		So(cleanupCalled, ShouldBeFalse)
		So(wireCalled, ShouldBeFalse)
		So(readFileBytes(fixture.groupUserPath), ShouldResemble, fixture.groupUserContent)
		So(readFileBytes(fixture.userGroupPath), ShouldResemble, fixture.userGroupContent)
	})

	Convey("successful summarise writes a completion marker after ClickHouse closing succeeds", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		closeCalled := false

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
			_ *summariseDiagnostics,
		) (func(bool) error, error) {
			return func(publish bool) error {
				So(publish, ShouldBeTrue)
				So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

				closeCalled = true

				return nil
			}, nil
		}
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

			closeCalled = true

			return perfreport.NewReport("clickhouse", "", 1, 0), nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(closeCalled, ShouldBeTrue)

		markerMatches, err := summariseCompletionMarkerMatches(*fixture.clickHouseTarget())
		So(err, ShouldBeNil)
		So(markerMatches, ShouldBeTrue)
	})

	Convey("summarise does not write a completion marker when ClickHouse closing fails", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
			_ *summariseDiagnostics,
		) (func(bool) error, error) {
			return func(bool) error {
				return errSummariseTestClose
			}, nil
		}
		loadSummariseClickHouseSpool = func(
			context.Context,
			clickhouse.Config,
			string,
			*chspool.Manifest,
			func(string, time.Duration),
		) (perfreport.Report, error) {
			return perfreport.Report{}, errSummariseTestClose
		}

		err := run([]string{fixture.statsPath})
		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)
		So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)
	})
}

func snapshotSummariseGlobals() func() {
	origDefaultDir := defaultDir
	origUserGroup := userGroup
	origGroupUser := groupUser
	origBasedirsDB := basedirsDB
	origBasedirsHistoryDB := basedirsHistoryDB
	origDirgutaDB := dirgutaDB
	origQuotaPath := quotaPath
	origBasedirsConfig := basedirsConfig
	origMounts := mounts
	origClickHouseDSN := clickhouseDSN
	origClickHouseDatabase := clickhouseDatabase
	origClickHouseQueryTO := clickhouseQueryTO
	origClickHouseRecover := clickhouseRecover
	origClickHouseSnapshotIsActive := clickHouseSnapshotIsActive
	origClickHouseCleanActiveSnapshotAttempt := clickHouseCleanActiveSnapshotAttempt
	origWireSummariseClickHouseOperations := wireSummariseClickHouseOperations
	origLoadSummariseClickHouseSpool := loadSummariseClickHouseSpool
	origSummariseSpoolNow := summariseSpoolNow
	origSummariseSpoolDirGUTANow := summariseSpoolDirGUTANow
	origOpenSummariseSpoolStats := openSummariseSpoolStats
	origBuildSummariseSpoolDirbuild := buildSummariseSpoolDirbuild

	return func() {
		defaultDir = origDefaultDir
		userGroup = origUserGroup
		groupUser = origGroupUser
		basedirsDB = origBasedirsDB
		basedirsHistoryDB = origBasedirsHistoryDB
		dirgutaDB = origDirgutaDB
		quotaPath = origQuotaPath
		basedirsConfig = origBasedirsConfig
		mounts = origMounts
		clickhouseDSN = origClickHouseDSN
		clickhouseDatabase = origClickHouseDatabase
		clickhouseQueryTO = origClickHouseQueryTO
		clickhouseRecover = origClickHouseRecover
		clickHouseSnapshotIsActive = origClickHouseSnapshotIsActive
		clickHouseCleanActiveSnapshotAttempt = origClickHouseCleanActiveSnapshotAttempt
		wireSummariseClickHouseOperations = origWireSummariseClickHouseOperations
		loadSummariseClickHouseSpool = origLoadSummariseClickHouseSpool
		summariseSpoolNow = origSummariseSpoolNow
		summariseSpoolDirGUTANow = origSummariseSpoolDirGUTANow
		openSummariseSpoolStats = origOpenSummariseSpoolStats
		buildSummariseSpoolDirbuild = origBuildSummariseSpoolDirbuild
	}
}

func configureSummariseActiveSnapshotTest(outputDir string, allowActive bool) {
	defaultDir = ""
	userGroup = filepath.Join(outputDir, "byusergroup.gz")
	groupUser = filepath.Join(outputDir, "bygroup")
	basedirsDB = ""
	basedirsHistoryDB = ""
	dirgutaDB = filepath.Join(outputDir, dgutaDBsSuffix)
	quotaPath = ""
	basedirsConfig = ""
	mounts = ""
	clickhouseDSN = summariseTestClickHouseDSN
	clickhouseDatabase = summariseTestClickHouseDatabase
	clickhouseQueryTO = ""
	clickhouseRecover = allowActive
}

func readFileBytes(path string) []byte {
	data, err := os.ReadFile(path)
	So(err, ShouldBeNil)

	return data
}

func writeGzipStats(t *testing.T, path string, content []byte) {
	t.Helper()

	f, err := os.Create(path)

	So(err, ShouldBeNil)
	defer func() { So(f.Close(), ShouldBeNil) }()

	zw := gzip.NewWriter(f)

	_, err = zw.Write(content)
	So(err, ShouldBeNil)
	So(zw.Close(), ShouldBeNil)
}

func summariseCompletionMarkerExists(outputDir string) bool {
	_, err := os.Stat(summariseCompletionMarkerPath(outputDir))

	return err == nil
}

func TestSummariseClickHouseRecoverFlag(t *testing.T) {
	Convey("summarise exposes only clickhouse recover", t, func() {
		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() {
			So(summariseCmd.Flags().Set("clickhouse-recover", "false"), ShouldBeNil)
		})

		usages := summariseCmd.Flags().FlagUsages()
		So(usages, ShouldContainSubstring, "--clickhouse-recover")

		So(summariseCmd.Flags().Set("clickhouse-recover", "true"), ShouldBeNil)
		So(clickhouseRecover, ShouldBeTrue)

		So(summariseCmd.Flags().Set("clickhouse-recover", "false"), ShouldBeNil)
		So(clickhouseRecover, ShouldBeFalse)
	})
}
