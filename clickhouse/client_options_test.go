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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestOptionsFromConfig(t *testing.T) {
	Convey("optionsFromConfig enforces the spec connection pool defaults", t, func() {
		cfg := Config{
			DSN:      testNativeDSN,
			Database: testDatabaseName,
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

		Convey("it enforces LZ4 transport compression", func() {
			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.Compression, ShouldNotBeNil)
			So(opts.Compression.Method, ShouldEqual, ch.CompressionLZ4)
		})

		Convey("normal connections preserve explicit short DSN timeouts", func() {
			cfg.DSN = "clickhouse://localhost:9000/?" +
				"database=wrstat&dial_timeout=1s&read_timeout=2s&conn_max_lifetime=1h"

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.DialTimeout, ShouldEqual, time.Second)
			So(opts.ReadTimeout, ShouldEqual, 2*time.Second)
			So(opts.ConnMaxLifetime, ShouldEqual, time.Hour)
		})

		Convey("normal connections preserve explicit low pool limits", func() {
			cfg.MaxOpenConns = 1
			cfg.MaxIdleConns = 1

			opts, err := optionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.MaxOpenConns, ShouldEqual, 1)
			So(opts.MaxIdleConns, ShouldEqual, 1)
		})
	})
}

func TestImportOptionsFromConfig(t *testing.T) {
	Convey("importOptionsFromConfig protects long-running import connection lifecycles", t, func() {
		cfg := Config{
			DSN:      testNativeDSN,
			Database: testDatabaseName,
		}

		Convey("it raises short DSN timeouts and lifetimes to import-safe floors", func() {
			cfg.DSN = "clickhouse://localhost:9000/?" +
				"database=wrstat&dial_timeout=1s&read_timeout=2s&conn_max_lifetime=1h"

			opts, err := importOptionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.DialTimeout, ShouldEqual, minImportDialTimeout)
			So(opts.ReadTimeout, ShouldEqual, minImportReadTimeout)
			So(opts.ConnMaxLifetime, ShouldEqual, minImportConnMaxLifetime)
		})

		Convey("it keeps operator values that are already safe for imports", func() {
			cfg.DSN = "clickhouse://localhost:9000/?" +
				"database=wrstat&dial_timeout=2m&read_timeout=2h&conn_max_lifetime=48h"

			opts, err := importOptionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.DialTimeout, ShouldEqual, 2*time.Minute)
			So(opts.ReadTimeout, ShouldEqual, 2*time.Hour)
			So(opts.ConnMaxLifetime, ShouldEqual, 48*time.Hour)
		})

		Convey("it guarantees basedirs batches still have a side-query connection", func() {
			cfg.MaxOpenConns = 1
			cfg.MaxIdleConns = 1

			opts, err := importOptionsFromConfig(cfg)
			requiredConns := len((&chBaseDirsStore{}).batchSlots()) + 1

			So(err, ShouldBeNil)
			So(opts.MaxOpenConns, ShouldEqual, requiredConns)
			So(opts.MaxIdleConns, ShouldEqual, requiredConns)
		})

		Convey("it keeps operator pool values that are already safe for imports", func() {
			cfg.MaxOpenConns = 8
			cfg.MaxIdleConns = 6

			opts, err := importOptionsFromConfig(cfg)
			So(err, ShouldBeNil)
			So(opts.MaxOpenConns, ShouldEqual, 8)
			So(opts.MaxIdleConns, ShouldEqual, 6)
		})
	})
}
