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

package clickhouse

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestOptionsFromConfig(t *testing.T) {
	Convey("optionsFromConfig enforces the spec connection pool defaults", t, func() {
		cfg := Config{
			DSN:      "clickhouse://localhost:9000/?database=wrstat",
			Database: "wrstat",
		}

		Convey("it defaults MaxOpenConns to 10 and MaxIdleConns to match", func() {
			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.MaxOpenConns, ShouldEqual, 10)
			So(opts.MaxIdleConns, ShouldEqual, 10)
		})

		Convey("it defaults MaxIdleConns to the effective open count", func() {
			cfgWithOpen := cfg
			cfgWithOpen.MaxOpenConns = 23

			opts, err := optionsFromConfig(cfgWithOpen)
			So(err, ShouldBeNil)
			So(opts.MaxOpenConns, ShouldEqual, 23)
			So(opts.MaxIdleConns, ShouldEqual, 23)
		})
	})
}
