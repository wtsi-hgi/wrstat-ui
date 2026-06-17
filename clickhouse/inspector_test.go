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
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	errTestRun       = errors.New("test error")
	errTestScan      = errors.New("scan failed")
	errTestIteration = errors.New("iteration failed")
)

const (
	testAuditMountEvents         = "wrstat_mount_events"
	testAuditSchema3SnapshotSets = "wrstat_schema3_snapshot_sets"
)

func TestNewInspector(t *testing.T) {
	Convey("NewInspector validates Config", t, func() {
		Convey("it errors when DSN is empty", func() {
			ins, err := NewInspector(Config{Database: testDatabaseName})
			So(err, ShouldNotBeNil)
			So(ins, ShouldBeNil)
		})

		Convey("it errors when Database is empty", func() {
			ins, err := NewInspector(Config{DSN: testNativeDSN})
			So(err, ShouldNotBeNil)
			So(ins, ShouldBeNil)
		})
	})
}

type profileEventsTestRows struct {
	events map[string]uint64
	keys   []string
	pos    int
}

func (r *profileEventsTestRows) Next() bool {
	if r.keys == nil {
		r.keys = []string{
			profileEventSelectedRows,
			profileEventSelectedBytes,
			profileEventSelectedMarks,
		}
	}

	return r.pos < len(r.keys)
}

func (r *profileEventsTestRows) Scan(dest ...any) error {
	if len(dest) != 2 {
		return errBootstrapTestUnexpectedScanDestinationN
	}

	event := r.keys[r.pos]

	eventDest, ok := dest[0].(*string)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	valueDest, ok := dest[1].(*uint64)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	*eventDest = event
	*valueDest = r.events[event]
	r.pos++

	return nil
}

func (*profileEventsTestRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (*profileEventsTestRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (*profileEventsTestRows) Totals(dest ...any) error {
	return errBootstrapTestUnexpectedCall
}

func (*profileEventsTestRows) Columns() []string {
	return []string{"event", "value"}
}

func (*profileEventsTestRows) Close() error {
	return nil
}

func (*profileEventsTestRows) Err() error {
	return nil
}

func (*profileEventsTestRows) HasData() bool {
	return true
}

func TestInspectorAuditRows(t *testing.T) {
	Convey("scanInspectorAuditRows captures named audit counts", t, func() {
		rows := &profileEventsTestRows{
			events: map[string]uint64{
				testAuditMountEvents:         4,
				testAuditSchema3SnapshotSets: 2,
			},
			keys: []string{
				testAuditMountEvents,
				testAuditSchema3SnapshotSets,
			},
		}

		auditRows, err := scanInspectorAuditRows(rows, "test")

		So(err, ShouldBeNil)
		So(auditRows, ShouldResemble, []InspectorAuditRow{
			{Surface: testAuditMountEvents, Rows: 4},
			{Surface: testAuditSchema3SnapshotSets, Rows: 2},
		})
	})
}

func TestInspectorExplainListDir(t *testing.T) {
	Convey("ExplainListDir returns EXPLAIN output", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		ins, err := NewInspector(cfg)
		So(err, ShouldBeNil)
		So(ins, ShouldNotBeNil)

		Reset(func() { So(ins.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		output, err := ins.ExplainListDir(ctx, providerTestMountPath, providerTestMountPath+"dir/", 100, 0)
		So(err, ShouldBeNil)
		So(output, ShouldNotBeEmpty)
	})
}

func TestInspectorExplainStatPath(t *testing.T) {
	Convey("ExplainStatPath returns EXPLAIN output", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second
		cfg.MountPoints = []string{providerTestMountPath}

		ins, err := NewInspector(cfg)
		So(err, ShouldBeNil)
		So(ins, ShouldNotBeNil)

		Reset(func() { So(ins.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		output, err := ins.ExplainStatPath(ctx, providerTestMountPath, providerTestMountPath+"dir/file.txt")
		So(err, ShouldBeNil)
		So(output, ShouldNotBeEmpty)

		Convey("it errors on an invalid path", func() {
			_, err := ins.ExplainStatPath(ctx, providerTestMountPath, "")
			So(err, ShouldNotBeNil)
		})
	})
}

func TestInspectorClose(t *testing.T) {
	Convey("Close is safe to call on nil inspector", t, func() {
		var ins *Inspector

		So(ins.Close(), ShouldBeNil)
	})
}

func TestInspectorServerTime(t *testing.T) {
	Convey("serverTime returns the current server time", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		ins, err := NewInspector(cfg)
		So(err, ShouldBeNil)
		So(ins, ShouldNotBeNil)

		Reset(func() { So(ins.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		before := time.Now().Add(-2 * time.Second)

		t0, err := ins.serverTime(ctx)
		So(err, ShouldBeNil)
		So(t0.After(before), ShouldBeTrue)
		So(t0.Before(time.Now().Add(2*time.Second)), ShouldBeTrue)
	})
}

type inspectorTestRow struct {
	values []any
	err    error
}

func (r inspectorTestRow) Err() error {
	return r.err
}

func (r inspectorTestRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != len(r.values) {
		return errTestScan
	}

	for i, value := range r.values {
		switch d := dest[i].(type) {
		case *time.Time:
			v, ok := value.(time.Time)
			if !ok {
				return errTestScan
			}

			*d = v
		case *uint64:
			v, ok := value.(uint64)
			if !ok {
				return errTestScan
			}

			*d = v
		default:
			return errTestScan
		}
	}

	return nil
}

func (r inspectorTestRow) ScanStruct(any) error {
	return r.err
}

func TestInspectorMeasure(t *testing.T) {
	Convey("Measure returns the error from the run function", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 5 * time.Second

		ins, err := NewInspector(cfg)
		So(err, ShouldBeNil)
		So(ins, ShouldNotBeNil)

		Reset(func() { So(ins.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		m, err := ins.Measure(ctx, func(ctx context.Context) error {
			return errTestRun
		})
		So(err, ShouldEqual, errTestRun)
		So(m, ShouldBeNil)
	})

	Convey("Measure falls back when query logging returns no matching row", t, func() {
		ins := &Inspector{
			cfg: Config{QueryTimeout: time.Second},
			conn: &inspectorTestConn{rows: map[string]driver.Row{
				serverTimeQuery: inspectorTestRow{values: []any{time.Unix(1710000000, 0).UTC()}},
				queryLogQuery:   inspectorTestRow{err: sql.ErrNoRows},
			}, profileEvents: []map[string]uint64{
				{
					profileEventSelectedRows:  10,
					profileEventSelectedBytes: 20,
					profileEventSelectedMarks: 3,
				},
				{
					profileEventSelectedRows:  15,
					profileEventSelectedBytes: 32,
					profileEventSelectedMarks: 5,
				},
			}},
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		m, err := ins.Measure(ctx, func(context.Context) error {
			time.Sleep(20 * time.Millisecond)

			return nil
		})
		So(err, ShouldBeNil)
		So(m, ShouldNotBeNil)
		So(m.DurationMs, ShouldBeGreaterThanOrEqualTo, uint64(10))
		So(m.ReadRows, ShouldEqual, 5)
		So(m.ReadBytes, ShouldEqual, 12)
		So(m.ReadMarks, ShouldEqual, 2)
		So(m.ResultRows, ShouldEqual, 0)
		So(m.ResultBytes, ShouldEqual, 0)
	})

	Convey("Measure returns query-log memory usage when available", t, func() {
		ins := &Inspector{
			cfg: Config{QueryTimeout: time.Second},
			conn: &inspectorTestConn{rows: map[string]driver.Row{
				serverTimeQuery: inspectorTestRow{values: []any{time.Unix(1710000000, 0).UTC()}},
				queryLogQuery: inspectorTestRow{values: []any{
					uint64(12),
					uint64(34),
					uint64(56),
					uint64(7),
					uint64(89),
					uint64(1),
					uint64(2),
				}},
			}},
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		m, err := ins.Measure(ctx, func(context.Context) error { return nil })
		So(err, ShouldBeNil)
		So(m, ShouldNotBeNil)
		So(m.DurationMs, ShouldEqual, 12)
		So(m.ReadRows, ShouldEqual, 34)
		So(m.ReadBytes, ShouldEqual, 56)
		So(m.ReadMarks, ShouldEqual, 7)
		So(m.MemoryBytes, ShouldEqual, 89)
		So(m.ResultRows, ShouldEqual, 1)
		So(m.ResultBytes, ShouldEqual, 2)
	})
}

type inspectorTestConn struct {
	bootstrapTestConn
	rows          map[string]driver.Row
	profileEvents []map[string]uint64
	profilePos    int
}

func (c *inspectorTestConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	if row, ok := c.rows[query]; ok {
		return row
	}

	return bootstrapTestRow{err: errBootstrapTestUnexpectedCall}
}

func (c *inspectorTestConn) Exec(_ context.Context, query string, _ ...any) error {
	if query == flushLogsStmt {
		return nil
	}

	return errBootstrapTestUnexpectedCall
}

func (c *inspectorTestConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if query != profileEventsQuery || c.profilePos >= len(c.profileEvents) {
		return nil, errBootstrapTestUnexpectedCall
	}

	rows := profileEventsTestRows{events: c.profileEvents[c.profilePos]}
	c.profilePos++

	return &rows, nil
}

type mockExplainRows struct {
	lines   []string
	pos     int
	scanErr error
	rowErr  error
}

func (m *mockExplainRows) Next() bool {
	if m.scanErr != nil && m.pos > 0 {
		return false
	}

	return m.pos < len(m.lines)
}

func (m *mockExplainRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}

	if len(dest) > 0 {
		if sp, ok := dest[0].(*string); ok {
			*sp = m.lines[m.pos]
		}
	}

	m.pos++

	return nil
}

func (m *mockExplainRows) Err() error {
	return m.rowErr
}

func TestCollectExplainOutput(t *testing.T) {
	Convey("collectExplainOutput joins rows into output", t, func() {
		mock := &mockExplainRows{
			lines: []string{"line1", "line2", "line3"},
		}

		output, err := collectExplainOutput(mock)
		So(err, ShouldBeNil)
		So(output, ShouldEqual, "line1\nline2\nline3")

		Convey("returns empty string for no rows", func() {
			mock := &mockExplainRows{lines: nil}

			output, err := collectExplainOutput(mock)
			So(err, ShouldBeNil)
			So(output, ShouldEqual, "")
		})

		Convey("returns scan error", func() {
			mock := &mockExplainRows{
				lines:   []string{"ok"},
				scanErr: errTestScan,
			}

			_, err := collectExplainOutput(mock)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "scan")
		})

		Convey("returns iteration error", func() {
			mock := &mockExplainRows{
				lines:  []string{},
				rowErr: errTestIteration,
			}

			_, err := collectExplainOutput(mock)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "iteration")
		})
	})
}
