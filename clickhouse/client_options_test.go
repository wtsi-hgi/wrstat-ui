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
	"context"
	"io"
	"net"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestOptionsFromConfig(t *testing.T) {
	Convey("optionsFromConfig enforces the spec connection pool defaults", t, func() {
		cfg := Config{
			DSN:      "clickhouse://localhost:9000/?database=wrstat",
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

		Convey("maintenance options do not inherit short driver read timeouts", func() {
			cfgWithShortReadTimeout := cfg
			cfgWithShortReadTimeout.DSN += "&read_timeout=100ms"

			foregroundOpts, err := optionsFromConfig(cfgWithShortReadTimeout)
			So(err, ShouldBeNil)
			So(foregroundOpts.ReadTimeout, ShouldEqual, 100*time.Millisecond)

			maintenanceOpts, err := maintenanceOptionsFromConfig(cfgWithShortReadTimeout)
			So(err, ShouldBeNil)
			So(maintenanceOpts.ReadTimeout, ShouldEqual, maintenanceReadTimeout)
			So(maintenanceOpts.DialContext, ShouldNotBeNil)
			So(maintenanceOpts.Auth.Database, ShouldEqual, foregroundOpts.Auth.Database)
			So(maintenanceOpts.DialTimeout, ShouldEqual, foregroundOpts.DialTimeout)
			So(maintenanceOpts.Compression.Method, ShouldEqual, foregroundOpts.Compression.Method)
		})
	})
}

type testAddr string

func (a testAddr) Network() string {
	return string(a)
}

func (a testAddr) String() string {
	return string(a)
}

type recordingDeadlineConn struct {
	readDeadline      time.Time
	deadline          time.Time
	readDeadlineCalls int
	deadlineCalls     int
}

func (*recordingDeadlineConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (*recordingDeadlineConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (*recordingDeadlineConn) Close() error {
	return nil
}

func (*recordingDeadlineConn) LocalAddr() net.Addr {
	return testAddr("local")
}

func (*recordingDeadlineConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}

func (c *recordingDeadlineConn) SetDeadline(t time.Time) error {
	c.deadline = t
	c.deadlineCalls++

	return nil
}

func (c *recordingDeadlineConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	c.readDeadlineCalls++

	return nil
}

func (*recordingDeadlineConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestNoReadDeadlineDialContext(t *testing.T) {
	Convey("maintenance dial context ignores driver read deadlines but preserves context deadlines", t, func() {
		baseConn := &recordingDeadlineConn{}
		dial := noReadDeadlineDialContext(func(context.Context, string) (net.Conn, error) {
			return baseConn, nil
		})

		conn, err := dial(context.Background(), "127.0.0.1:9000")
		So(err, ShouldBeNil)

		readDeadline := time.Now().Add(time.Second)
		So(conn.SetReadDeadline(readDeadline), ShouldBeNil)
		So(baseConn.readDeadlineCalls, ShouldEqual, 0)
		So(baseConn.readDeadline.IsZero(), ShouldBeTrue)

		deadline := time.Now().Add(2 * time.Second)
		So(conn.SetDeadline(deadline), ShouldBeNil)
		So(baseConn.deadlineCalls, ShouldEqual, 1)
		So(baseConn.deadline, ShouldResemble, deadline)
	})
}
