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

package clickhouse_test

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

func TestNewClient(t *testing.T) {
	Convey("NewClient validates Config", t, func() {
		Convey("it errors when DSN is empty", func() {
			c, err := clickhouse.NewClient(clickhouse.Config{Database: "wrstat"})
			So(err, ShouldNotBeNil)
			So(c, ShouldBeNil)
		})

		Convey("it errors when Database is empty", func() {
			c, err := clickhouse.NewClient(clickhouse.Config{DSN: "clickhouse://localhost:9000/?database=wrstat"})
			So(err, ShouldNotBeNil)
			So(c, ShouldBeNil)
		})

		Convey("it errors when DSN is missing database=", func() {
			c, err := clickhouse.NewClient(clickhouse.Config{DSN: "clickhouse://localhost:9000/", Database: "wrstat"})
			So(err, ShouldNotBeNil)
			So(c, ShouldBeNil)
		})

		Convey("it errors when DSN database does not match Config.Database", func() {
			c, err := clickhouse.NewClient(clickhouse.Config{
				DSN:      "clickhouse://localhost:9000/?database=other",
				Database: "wrstat",
			})
			So(err, ShouldNotBeNil)
			So(c, ShouldBeNil)
		})

		Convey("it accepts a minimal valid config (even if the server is unreachable)", func() {
			cfg := clickhouse.Config{
				DSN:           "clickhouse://127.0.0.1:65535/?database=wrstat",
				Database:      "wrstat",
				OwnersCSVPath: "",
				MountPoints:   nil,
				PollInterval:  time.Second,
				QueryTimeout:  time.Second,
				MaxOpenConns:  0,
				MaxIdleConns:  0,
			}

			c, err := clickhouse.NewClient(cfg)
			So(err, ShouldBeNil)
			So(c, ShouldNotBeNil)
		})
	})
}
