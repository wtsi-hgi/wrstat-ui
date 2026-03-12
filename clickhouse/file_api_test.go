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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

type findByGlobEmptyRows struct{}

func (r *findByGlobEmptyRows) Next() bool {
	return false
}

func (r *findByGlobEmptyRows) Scan(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *findByGlobEmptyRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *findByGlobEmptyRows) Columns() []string {
	return nil
}

func (r *findByGlobEmptyRows) Close() error {
	return nil
}

func (r *findByGlobEmptyRows) Err() error {
	return nil
}

type findByGlobQueryCountConn struct {
	bootstrapTestConn

	mu         sync.Mutex
	queryCount int
}

func (c *findByGlobQueryCountConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.queryCount++

	return &findByGlobEmptyRows{}, nil
}

func (c *findByGlobQueryCountConn) queryCountValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.queryCount
}

func TestClientFindByGlobQueryGrouping(t *testing.T) {
	Convey("Client.FindByGlob issues one query per mount group and pattern chunk", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		mountPoints := basedirs.ValidateMountPoints([]string{"/alpha/", "/beta/"})

		newClient := func(conn *findByGlobQueryCountConn) *Client {
			return &Client{
				cfg:         Config{QueryTimeout: time.Second},
				conn:        conn,
				mountPoints: mountPoints,
			}
		}

		Convey("same-mount base dirs share one query", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			rows, err := client.FindByGlob(ctx, []string{"/alpha/one", "/alpha/two"}, []string{"*"}, FindOptions{})
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 1)
		})

		Convey("pattern chunks still split per mount group", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			patterns := make([]string, 33)
			for i := range 33 {
				patterns[i] = "*"
			}

			rows, err := client.FindByGlob(ctx, []string{"/alpha/one", "/alpha/two"}, patterns, FindOptions{})
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 2)
		})

		Convey("different mounts still query once each", func() {
			conn := &findByGlobQueryCountConn{}
			client := newClient(conn)

			rows, err := client.FindByGlob(ctx, []string{"/alpha/one", "/beta/two"}, []string{"*"}, FindOptions{})
			So(err, ShouldBeNil)
			So(rows, ShouldBeEmpty)
			So(conn.queryCountValue(), ShouldEqual, 2)
		})
	})
}

func TestUnknownFileFieldErrors(t *testing.T) {
	Convey("file row helpers preserve unknown field error behaviour", t, func() {
		_, _, err := fileRowSelectList([]string{"bogus"})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `clickhouse: unknown file field "bogus"`)

		var selectErr unknownFileFieldError
		So(errors.As(err, &selectErr), ShouldBeTrue)
		So(selectErr.Field, ShouldEqual, "bogus")

		_, err = (&fileRowScanState{}).destsFor([]string{"bogus"})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `clickhouse: unknown file field "bogus"`)

		var scanErr unknownFileFieldError
		So(errors.As(err, &scanErr), ShouldBeTrue)
		So(scanErr.Field, ShouldEqual, "bogus")
	})
}
