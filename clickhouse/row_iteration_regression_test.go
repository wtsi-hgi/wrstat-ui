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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

var errRowIterationRegression = errors.New("row iteration failed")

type iterationRegressionRows struct {
	columns []string
	err     error
}

func newIterationRegressionRows() *iterationRegressionRows {
	return &iterationRegressionRows{err: errRowIterationRegression}
}

func (r *iterationRegressionRows) Next() bool {
	return false
}

func (r *iterationRegressionRows) HasData() bool {
	return false
}

func (r *iterationRegressionRows) Scan(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *iterationRegressionRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *iterationRegressionRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *iterationRegressionRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *iterationRegressionRows) Columns() []string {
	return r.columns
}

func (r *iterationRegressionRows) Close() error {
	return nil
}

func (r *iterationRegressionRows) Err() error {
	return r.err
}

func TestClickHouseRowIterationErrors(t *testing.T) {
	Convey("file API helpers propagate ClickHouse iterator errors", t, func() {
		_, err := firstFileRow(newIterationRegressionRows(), defaultFileRowFields())
		assertIterationError(err, "StatPath iteration error")

		client := &Client{
			cfg:  Config{QueryTimeout: time.Second},
			conn: &iterationRegressionConn{rows: newIterationRegressionRows()},
		}

		_, err = client.queryFileRows(context.Background(), "ListDir", "SELECT 1", nil)
		assertIterationError(err, "ListDir iteration error")

		_, err = isDirFromRows(newIterationRegressionRows())
		assertIterationError(err, "IsDir iteration error")
	})

	Convey("PermissionAnyInDir propagates ClickHouse iterator errors", t, func() {
		client := &Client{
			cfg:         Config{QueryTimeout: time.Second},
			conn:        &iterationRegressionConn{rows: newIterationRegressionRows()},
			mountPoints: basedirs.ValidateMountPoints([]string{"/mnt/"}),
		}

		_, err := client.PermissionAnyInDir(context.Background(), "/mnt/project", 1, nil)
		assertIterationError(err, "PermissionAnyInDir iteration error")
	})

	Convey("database helpers propagate ClickHouse iterator errors", t, func() {
		_, err := scanChildrenRows(newIterationRegressionRows())
		assertIterationError(err, "children iteration error")

		_, err = scanDGUTARows(newIterationRegressionRows())
		assertIterationError(err, "dguta iteration error")

		_, err = scanMaxUpdatedAt(newIterationRegressionRows())
		assertIterationError(err, "ancestor max updated_at iteration error")

		db := &clickHouseDatabase{
			cfg:  Config{QueryTimeout: time.Second},
			conn: &iterationRegressionConn{rows: newIterationRegressionRows()},
		}

		_, _, err = db.queryInfoCounts(context.Background(), "SELECT 1", "dguta")
		assertIterationError(err, "dguta counts iteration error")
	})

	Convey("basedirs helpers propagate ClickHouse iterator errors", t, func() {
		_, err := scanMountTimestamps(newIterationRegressionRows())
		assertIterationError(err, "mount timestamp iteration error")

		_, err = scanSubDirRows(newIterationRegressionRows(), "group")
		assertIterationError(err, "group subdirs iteration error")

		_, err = collectHistoryRows(newIterationRegressionRows())
		assertIterationError(err, "basedirs history series iteration error")

		_, err = scanHistoryLastDate(newIterationRegressionRows(), time.Now())
		assertIterationError(err, "basedirs history last date iteration error")

		reader := &chBaseDirsReader{
			cfg:  Config{QueryTimeout: time.Second},
			conn: &iterationRegressionConn{rows: newIterationRegressionRows()},
		}

		var count int

		err = reader.queryCount(context.Background(), "SELECT 1", &count)
		assertIterationError(err, "basedirs info iteration error")
	})

	Convey("active snapshot and provider helpers propagate ClickHouse iterator errors", t, func() {
		conn := &iterationRegressionConn{rows: newIterationRegressionRows()}
		_, _, err := readActiveSnapshotID(context.Background(), conn, "/mnt/")
		assertIterationError(err, "active snapshot iteration error")

		provider := &chProvider{
			cfg:  Config{QueryTimeout: time.Second},
			conn: &iterationRegressionConn{rows: newIterationRegressionRows()},
		}

		_, err = provider.mountsActiveRows(context.Background())
		assertIterationError(err, "mounts_active iteration error")
	})
}

func assertIterationError(err error, message string) {
	So(err, ShouldNotBeNil)
	So(err.Error(), ShouldContainSubstring, message)
	So(errors.Is(err, errRowIterationRegression), ShouldBeTrue)
}

func TestSchemaVersionStatsNoRows(t *testing.T) {
	Convey("schemaVersionStatsFromDB reports an empty stats result distinctly", t, func() {
		conn := &iterationRegressionConn{
			rows: &iterationRegressionRows{
				columns: []string{"count", "min", "max"},
			},
		}

		_, _, _, err := schemaVersionStatsFromDB(context.Background(), conn)
		So(errors.Is(err, errNoSchemaVersionStats), ShouldBeTrue)
	})
}

type iterationRegressionConn struct {
	bootstrapTestConn

	rows driver.Rows
}

func (c *iterationRegressionConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return c.rows, nil
}
