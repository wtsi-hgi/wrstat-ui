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
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	summariseTestClickHouseDSN      = "clickhouse://default@127.0.0.1:9000/default"
	summariseTestClickHouseDatabase = "wrstat_ui_test"
)

var errSummariseTestClose = errors.New("close failed")

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
		mountPath: "/mnt/test/",
		modtime:   f.updatedAt,
		outputDir: f.outputDir,
	}
}

func (f summariseActiveSnapshotFixture) writeValidStats(t *testing.T) {
	t.Helper()

	writeGzipStats(t, f.statsPath, []byte("\"/mnt/test/file\"\t1\t0\t0\t0\t0\t0\tf\t1\t1\t0\t1\n"))
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
			So(mountPath, ShouldEqual, "/mnt/test/")
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
			So(mountPath, ShouldEqual, "/mnt/test/")
			So(modtime.Equal(fixture.updatedAt), ShouldBeTrue)

			cleanupCalled = true

			return nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			_ clickhouse.Config,
			_, _ string,
			_ time.Time,
		) (func(bool) error, error) {
			So(cleanupCalled, ShouldBeTrue)

			wireCalled = true

			return func(publish bool) error {
				So(publish, ShouldBeTrue)

				closeCalled = true

				return nil
			}, nil
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
		) (func(bool) error, error) {
			wireCalled = true

			return func(bool) error { return nil }, nil
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
		) (func(bool) error, error) {
			wireCalled = true

			return func(bool) error { return nil }, nil
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
		) (func(bool) error, error) {
			return func(publish bool) error {
				So(publish, ShouldBeTrue)
				So(summariseCompletionMarkerExists(fixture.outputDir), ShouldBeFalse)

				closeCalled = true

				return nil
			}, nil
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
		) (func(bool) error, error) {
			return func(bool) error {
				return errSummariseTestClose
			}, nil
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
	origClickHouseActiveSnapshotOK := clickhouseActiveSnapshotOK
	origClickHouseSnapshotIsActive := clickHouseSnapshotIsActive
	origClickHouseCleanActiveSnapshotAttempt := clickHouseCleanActiveSnapshotAttempt
	origWireSummariseClickHouseOperations := wireSummariseClickHouseOperations

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
		clickhouseActiveSnapshotOK = origClickHouseActiveSnapshotOK
		clickHouseSnapshotIsActive = origClickHouseSnapshotIsActive
		clickHouseCleanActiveSnapshotAttempt = origClickHouseCleanActiveSnapshotAttempt
		wireSummariseClickHouseOperations = origWireSummariseClickHouseOperations
	}
}

func configureSummariseActiveSnapshotTest(outputDir string, allowActive bool) {
	defaultDir = outputDir
	userGroup = ""
	groupUser = ""
	basedirsDB = ""
	basedirsHistoryDB = ""
	dirgutaDB = ""
	quotaPath = ""
	basedirsConfig = ""
	mounts = ""
	clickhouseDSN = summariseTestClickHouseDSN
	clickhouseDatabase = summariseTestClickHouseDatabase
	clickhouseQueryTO = ""
	clickhouseActiveSnapshotOK = allowActive
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
	Convey("summarise exposes clickhouse recover and hides the old retry flag", t, func() {
		restore := snapshotSummariseGlobals()
		Reset(restore)
		Reset(func() {
			So(summariseCmd.Flags().Set("clickhouse-recover", "false"), ShouldBeNil)
			So(summariseCmd.Flags().Set("clickhouse-active-snapshot-ok", "false"), ShouldBeNil)
		})

		usages := summariseCmd.Flags().FlagUsages()
		So(usages, ShouldContainSubstring, "--clickhouse-recover")
		So(usages, ShouldNotContainSubstring, "--clickhouse-active-snapshot-ok")

		oldFlag := summariseCmd.Flags().Lookup("clickhouse-active-snapshot-ok")
		So(oldFlag, ShouldNotBeNil)
		So(oldFlag.Hidden, ShouldBeTrue)

		So(summariseCmd.Flags().Set("clickhouse-recover", "true"), ShouldBeNil)
		So(clickhouseActiveSnapshotOK, ShouldBeTrue)

		So(summariseCmd.Flags().Set("clickhouse-recover", "false"), ShouldBeNil)
		So(summariseCmd.Flags().Set("clickhouse-active-snapshot-ok", "true"), ShouldBeNil)
		So(clickhouseActiveSnapshotOK, ShouldBeTrue)
	})
}
