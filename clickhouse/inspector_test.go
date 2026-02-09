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
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	errTestRun       = errors.New("test error")
	errTestScan      = errors.New("scan failed")
	errTestIteration = errors.New("iteration failed")
)

func TestNewInspector(t *testing.T) {
	Convey("NewInspector validates Config", t, func() {
		Convey("it errors when DSN is empty", func() {
			ins, err := NewInspector(Config{Database: "wrstat"})
			So(err, ShouldNotBeNil)
			So(ins, ShouldBeNil)
		})

		Convey("it errors when Database is empty", func() {
			ins, err := NewInspector(Config{DSN: "clickhouse://localhost:9000/?database=wrstat"})
			So(err, ShouldNotBeNil)
			So(ins, ShouldBeNil)
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
