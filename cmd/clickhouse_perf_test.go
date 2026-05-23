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
	})

	Convey("bolt-perf query exposes tree benchmark controls", t, func() {
		flags := boltPerfQueryCmd.Flags()

		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
		So(flags.Lookup("walk-depth"), ShouldNotBeNil)
		So(flags.Lookup("walk-limit"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-dir"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-limit"), ShouldNotBeNil)
	})
}
