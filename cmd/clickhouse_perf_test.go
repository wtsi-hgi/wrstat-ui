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
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestClickHousePerfQueryFlags(t *testing.T) {
	Convey("clickhouse-perf query exposes tree benchmark controls", t, func() {
		flags := chPerfQueryCmd.Flags()

		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
		So(flags.Lookup("walk-depth"), ShouldNotBeNil)
		So(flags.Lookup("walk-limit"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-dir"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-limit"), ShouldNotBeNil)
		So(flags.Lookup("ops"), ShouldNotBeNil)
		So(flags.Lookup("tree-gids"), ShouldNotBeNil)
		So(flags.Lookup("tree-uids"), ShouldNotBeNil)
		So(flags.Lookup("tree-types"), ShouldNotBeNil)
		So(flags.Lookup("tree-ft"), ShouldNotBeNil)
	})

	Convey("bolt-perf query exposes tree benchmark controls", t, func() {
		flags := boltPerfQueryCmd.Flags()

		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
		So(flags.Lookup("walk-depth"), ShouldNotBeNil)
		So(flags.Lookup("walk-limit"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-dir"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-limit"), ShouldNotBeNil)
		So(flags.Lookup("ops"), ShouldNotBeNil)
		So(flags.Lookup("tree-gids"), ShouldNotBeNil)
		So(flags.Lookup("tree-uids"), ShouldNotBeNil)
		So(flags.Lookup("tree-types"), ShouldNotBeNil)
		So(flags.Lookup("tree-ft"), ShouldNotBeNil)
	})
}

func TestClickHousePerfRequiresConnectionSettings(t *testing.T) {
	Convey("clickhouse-perf import reports missing DSN and database before input work", t, func() {
		resetClickHousePerfConnectionForTest()

		err := runCHPerfImport("/definitely/not/read")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errClickhouseDSNRequired.Error())
		So(err.Error(), ShouldContainSubstring, errClickhouseDatabaseRequired.Error())
		So(err.Error(), ShouldNotContainSubstring, "definitely/not/read")
	})

	Convey("clickhouse-perf query reports missing DSN and database before running operations", t, func() {
		resetClickHousePerfConnectionForTest()

		err := runCHPerfQuery()

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errClickhouseDSNRequired.Error())
		So(err.Error(), ShouldContainSubstring, errClickhouseDatabaseRequired.Error())
		So(err.Error(), ShouldNotContainSubstring, "active mounts")
	})
}

func resetClickHousePerfConnectionForTest() {
	origDSN := chPerf.dsn
	origDB := chPerf.database
	origMountpoints := chPerf.mountpoints
	origEnvDSN, hadEnvDSN := os.LookupEnv(envClickhouseDSN)
	origEnvDB, hadEnvDB := os.LookupEnv(envClickhouseDatabase)

	chPerf.dsn = ""
	chPerf.database = ""
	chPerf.mountpoints = ""
	_ = os.Unsetenv(envClickhouseDSN)
	_ = os.Unsetenv(envClickhouseDatabase)

	Reset(func() {
		chPerf.dsn = origDSN
		chPerf.database = origDB
		chPerf.mountpoints = origMountpoints

		restoreEnv(envClickhouseDSN, origEnvDSN, hadEnvDSN)
		restoreEnv(envClickhouseDatabase, origEnvDB, hadEnvDB)
	})
}

func restoreEnv(key, value string, ok bool) {
	if ok {
		_ = os.Setenv(key, value)

		return
	}

	_ = os.Unsetenv(key)
}
