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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

const (
	summariseTestClickHouseDSN      = "clickhouse://default@127.0.0.1:9000/default"
	summariseTestClickHouseDatabase = "wrstat_ui_test"
)

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

func TestSummariseClickHouseActiveSnapshotPreflight(t *testing.T) {
	Convey("watch retries treat an already-active ClickHouse snapshot as success before truncating outputs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, true)

		called := false
		clickHouseSnapshotIsActive = func(cfg clickhouse.Config, mountPath string, modtime time.Time) (bool, error) {
			called = true

			So(cfg.DSN, ShouldEqual, summariseTestClickHouseDSN)
			So(cfg.Database, ShouldEqual, summariseTestClickHouseDatabase)
			So(mountPath, ShouldEqual, "/mnt/test/")
			So(modtime.Equal(fixture.updatedAt), ShouldBeTrue)

			return true, nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldBeNil)
		So(called, ShouldBeTrue)
		So(readFileBytes(fixture.groupUserPath), ShouldResemble, fixture.groupUserContent)
		So(readFileBytes(fixture.userGroupPath), ShouldResemble, fixture.userGroupContent)
	})

	Convey("manual summarise refuses an already-active ClickHouse snapshot before truncating outputs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)

		restore := snapshotSummariseGlobals()
		Reset(restore)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return true, nil
		}

		err := run([]string{fixture.statsPath})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "refusing to rewrite active snapshot")
		So(readFileBytes(fixture.groupUserPath), ShouldResemble, fixture.groupUserContent)
		So(readFileBytes(fixture.userGroupPath), ShouldResemble, fixture.userGroupContent)
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
