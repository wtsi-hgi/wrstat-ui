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
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const summariseTestSecretDSN = "clickhouse://diag:secret@127.0.0.1:9000/default?" +
	"database=wrstat_ui_test&password=querysecret"

func TestSummariseProcessRSS(t *testing.T) {
	Convey("Linux proc statm RSS parsing returns MiB", t, func() {
		mb, ok := rssMiBFromProcStatm([]byte("10 512 0 0 0 0 0\n"), 4096)

		So(ok, ShouldBeTrue)
		So(mb, ShouldEqual, 2)
	})

	Convey("RSS parsing falls back safely for malformed proc data", t, func() {
		mb, ok := rssMiBFromProcStatm([]byte("malformed\n"), 4096)

		So(ok, ShouldBeFalse)
		So(mb, ShouldEqual, 0)
	})
}

func TestSummariseDiagnosticsLogging(t *testing.T) {
	Convey("ClickHouse summarise logs safe start and close-failure breadcrumbs", t, func() {
		fixture := newSummariseActiveSnapshotFixture(t)
		fixture.writeValidStats(t)

		restoreGlobals := snapshotSummariseGlobals()
		Reset(restoreGlobals)

		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		configureSummariseActiveSnapshotTest(fixture.outputDir, false)

		clickhouseDSN = summariseTestSecretDSN

		clickHouseSnapshotIsActive = func(clickhouse.Config, string, time.Time) (bool, error) {
			return false, nil
		}
		wireSummariseClickHouseOperations = func(
			_ *summary.Summariser,
			cfg clickhouse.Config,
			mountPath, _ string,
			modtime time.Time,
			diag *summariseDiagnostics,
		) (func(bool) error, error) {
			So(cfg.DSN, ShouldEqual, summariseTestSecretDSN)
			So(mountPath, ShouldEqual, "/mnt/test/")
			So(modtime.Equal(fixture.updatedAt), ShouldBeTrue)
			So(diag, ShouldNotBeNil)

			diag.recordImportPhase("wrstat_files_insert", 37*time.Millisecond)

			return func(publish bool) error {
				So(publish, ShouldBeTrue)

				return errSummariseTestClose
			}, nil
		}

		err := run([]string{fixture.statsPath})

		So(errors.Is(err, errSummariseTestClose), ShouldBeTrue)

		output := logs.String()
		So(output, ShouldContainSubstring, "summarise start")
		So(output, ShouldContainSubstring, "pid=")
		So(output, ShouldContainSubstring, "input="+quoteForDiagnostics(fixture.statsPath))
		So(output, ShouldContainSubstring, "output="+quoteForDiagnostics(fixture.outputDir))
		So(output, ShouldContainSubstring, "mount_path="+quoteForDiagnostics("/mnt/test/"))
		So(output, ShouldContainSubstring, "snapshot_id=")
		So(output, ShouldContainSubstring, "clickhouse_database="+quoteForDiagnostics(summariseTestClickHouseDatabase))
		So(output, ShouldContainSubstring, "clickhouse_dsn="+
			quoteForDiagnostics("clickhouse://diag:xxxxx@127.0.0.1:9000/default?database=wrstat_ui_test&password=xxxxx"))
		So(output, ShouldContainSubstring, "SIGKILL cannot be caught")
		So(output, ShouldContainSubstring, "summarise close start")
		So(output, ShouldContainSubstring, "publish=true")
		So(output, ShouldContainSubstring, "summarise close failure")
		So(output, ShouldContainSubstring, errSummariseTestClose.Error())
		So(output, ShouldContainSubstring, "recent_import_phases="+quoteForDiagnostics("wrstat_files_insert:37ms"))
		So(output, ShouldNotContainSubstring, "secret")
		So(output, ShouldNotContainSubstring, "querysecret")
	})

	Convey("progress breadcrumbs include parsed rows, heap, RSS, goroutines, GC, and recent phases", t, func() {
		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		restoreRSS := stubSummariseProcessRSS(123, true)
		Reset(restoreRSS)

		diag := newSummariseDiagnostics("stats.gz")
		diag.setCurrentPhase("parse")
		diag.recordImportPhase("wrstat_dguta_insert", 20*time.Millisecond)
		diag.recordImportPhase("wrstat_dir_projection_insert", 30*time.Millisecond)

		diag.logProgress(1_000_000, 2*time.Second)

		output := logs.String()
		So(output, ShouldContainSubstring, "summarise progress")
		So(output, ShouldContainSubstring, "records=1000000")
		So(output, ShouldContainSubstring, "elapsed=2s")
		So(output, ShouldContainSubstring, "heap_alloc_mb=")
		So(output, ShouldContainSubstring, "heap_sys_mb=")
		So(output, ShouldContainSubstring, "rss_mb=123")
		So(output, ShouldContainSubstring, "goroutines=")
		So(output, ShouldContainSubstring, "gc_count=")
		So(output, ShouldContainSubstring, "current_phase=wrstat_dir_projection_insert")
		So(output, ShouldContainSubstring, "recent_import_phases="+
			quoteForDiagnostics("wrstat_dguta_insert:20ms,wrstat_dir_projection_insert:30ms"))
	})

	Convey("signal breadcrumbs include the last known state for catchable termination signals", t, func() {
		var logs bytes.Buffer

		restoreLogs := captureSummariseDiagnosticsLogs(&logs)
		Reset(restoreLogs)

		restoreRSS := stubSummariseProcessRSS(456, true)
		Reset(restoreRSS)

		diag := newSummariseDiagnostics("stats.gz")
		diag.setTarget(&clickHouseSummariseTarget{
			cfg: clickhouse.Config{
				DSN:      summariseTestSecretDSN,
				Database: summariseTestClickHouseDatabase,
			},
			mountPath: "/nfs/t283_imaging/",
			modtime:   time.Unix(1_710_000_000, 0).UTC(),
			outputDir: filepath.Join("out", "t283"),
		})
		diag.setCurrentPhase("clickhouse_close")
		diag.recordImportPhase("mount_switch", time.Second)
		diag.logProgress(2_000_000, 3*time.Hour)

		diag.logSignal(syscall.SIGTERM)

		output := logs.String()
		So(output, ShouldContainSubstring, "summarise signal")
		So(output, ShouldContainSubstring, "signal=SIGTERM")
		So(output, ShouldContainSubstring, "records=2000000")
		So(output, ShouldContainSubstring, "rss_mb=456")
		So(output, ShouldContainSubstring, "current_phase=mount_switch")
		So(output, ShouldContainSubstring, "mount_path="+quoteForDiagnostics("/nfs/t283_imaging/"))
		So(output, ShouldContainSubstring, "recent_import_phases="+quoteForDiagnostics("mount_switch:1s"))
		So(output, ShouldNotContainSubstring, "secret")
		So(output, ShouldNotContainSubstring, "querysecret")
	})
}

func captureSummariseDiagnosticsLogs(buf *bytes.Buffer) func() {
	orig := appLogger.GetHandler()
	appLogger.SetHandler(log15.StreamHandler(buf, cliFormat()))

	return func() {
		appLogger.SetHandler(orig)
	}
}

func stubSummariseProcessRSS(mb uint64, ok bool) func() {
	orig := summariseProcessRSSMiB
	summariseProcessRSSMiB = func() (uint64, bool) {
		return mb, ok
	}

	return func() {
		summariseProcessRSSMiB = orig
	}
}
