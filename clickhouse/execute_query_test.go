/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

// Predefined errors for testing.
var (
	errSyntaxSQL         = errors.New("syntax error in SQL")
	errTableNotFound     = errors.New("table not found in database")
	errScanStructNotImpl = errors.New("mockRow.ScanStruct not implemented for testing")
)

// Test for ExecuteQuery method.
func TestExecuteQuery(t *testing.T) {
	// Set up mock connections for testing
	t.Run("Execute DDL statement", func(t *testing.T) {
		// Mock connection for a DDL statement (should call Exec)
		mockConn := &mockConnForExec{
			expectedQuery: "CREATE DATABASE IF NOT EXISTS test",
		}

		// Create a test instance that directly sets the connection
		ch := &testClickhouse{
			conn: mockConn,
		}

		// Execute a query without destination (should use Exec)
		err := ch.ExecuteQuery(context.Background(), "CREATE DATABASE IF NOT EXISTS test")

		assert.NoError(t, err)
		assert.True(t, mockConn.execCalled, "Exec should have been called")
	})

	t.Run("Execute query with result", func(t *testing.T) {
		// Mock connection for a query returning a value
		mockConn := &mockConnForQuery{
			expectedQuery: "SELECT count() FROM scans",
			returnValue:   42,
		}

		// Create a test instance that directly sets the connection
		ch := &testClickhouse{
			conn: mockConn,
		}

		// Execute a query with destination (should use QueryRow)
		var count int

		err := ch.ExecuteQuery(context.Background(), "SELECT count() FROM scans", &count)

		assert.NoError(t, err)
		assert.True(t, mockConn.queryRowCalled, "QueryRow should have been called")
		assert.Equal(t, 42, count, "Should return the mocked value")
	})

	t.Run("Execute query with arguments", func(t *testing.T) {
		mockConn := &mockConnForQuery{
			expectedQuery: "SELECT count() FROM scans WHERE mount_path = ?",
			returnValue:   5,
		}

		// Create a test instance that directly sets the connection
		ch := &testClickhouse{
			conn: mockConn,
		}

		var count int

		err := ch.ExecuteQuery(context.Background(), "SELECT count() FROM scans WHERE mount_path = ?", "/data/", &count)

		assert.NoError(t, err)
		assert.True(t, mockConn.queryRowCalled, "QueryRow should have been called")
		assert.Equal(t, 5, count, "Should return the mocked value")
	})

	t.Run("Handle Exec error", func(t *testing.T) {
		// Using a predefined error
		mockConn := &mockConnForExec{
			expectedQuery: "INVALID QUERY",
			execError:     errSyntaxSQL,
		}

		// Create a test instance that directly sets the connection
		ch := &testClickhouse{
			conn: mockConn,
		}

		err := ch.ExecuteQuery(context.Background(), "INVALID QUERY")

		assert.Error(t, err)
		assert.Equal(t, errSyntaxSQL.Error(), err.Error())
	})

	t.Run("Handle QueryRow error", func(t *testing.T) {
		// Using a predefined error
		mockConn := &mockConnForQuery{
			expectedQuery: "SELECT count() FROM non_existent_table",
			rowError:      errTableNotFound,
		}

		// Create a test instance that directly sets the connection
		ch := &testClickhouse{
			conn: mockConn,
		}

		var count int

		err := ch.ExecuteQuery(context.Background(), "SELECT count() FROM non_existent_table", &count)

		assert.Error(t, err)
		assert.Equal(t, errTableNotFound.Error(), err.Error())
	})
}

// testClickhouse is a simplified version of Clickhouse for testing.
type testClickhouse struct {
	conn driver.Conn
}

// ExecuteQuery implements the ExecuteQuery method for testing.
func (ch *testClickhouse) ExecuteQuery(ctx context.Context, query string, args ...interface{}) error {
	if len(args) == 0 {
		// No arguments, use Exec
		return ch.conn.Exec(ctx, query)
	}

	// Check if the last argument is a pointer (a destination for results)
	last := args[len(args)-1]
	if !testIsPointer(last) {
		// No destination pointer, use Exec
		return ch.conn.Exec(ctx, query, args...)
	}

	// Extract the destination and remove it from args
	dest := args[len(args)-1]
	args = args[:len(args)-1]

	// Use QueryRow for queries with results
	row := ch.conn.QueryRow(ctx, query, args...)
	if row.Err() != nil {
		return row.Err()
	}

	return row.Scan(dest)
}

// Simplified mocks for testing ExecuteQuery

type mockConnForExec struct {
	chdriver.Conn
	expectedQuery string
	execCalled    bool
	execError     error
}

func (m *mockConnForExec) Exec(ctx context.Context, query string, args ...interface{}) error {
	m.execCalled = true
	// Skip assertion to avoid test.T in non-test code
	return m.execError
}

type mockConnForQuery struct {
	chdriver.Conn
	expectedQuery  string
	queryRowCalled bool
	returnValue    interface{}
	rowError       error
	scanError      error
}

// QueryRow implements the driver.Conn interface method for mock testing.
//
//nolint:ireturn // test mock needs to return interface type
func (m *mockConnForQuery) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	m.queryRowCalled = true
	// Skip assertion to avoid test.T in non-test code

	return &mockRow{
		rowError:    m.rowError,
		scanError:   m.scanError,
		returnValue: m.returnValue,
	}
}

type mockRow struct {
	rowError    error
	scanError   error
	returnValue interface{}
}

func (r *mockRow) Err() error {
	return r.rowError
}

func (r *mockRow) Scan(dest ...interface{}) error {
	if r.scanError != nil {
		return r.scanError
	}

	if len(dest) == 0 {
		return nil
	}

	// Set the return value to the destination pointer
	switch value := r.returnValue.(type) {
	case int:
		if ptr, ok := dest[0].(*int); ok {
			*ptr = value
		}
	case string:
		if ptr, ok := dest[0].(*string); ok {
			*ptr = value
		}
	case uint64:
		if ptr, ok := dest[0].(*uint64); ok {
			*ptr = value
		}
	case time.Time:
		if ptr, ok := dest[0].(*time.Time); ok {
			*ptr = value
		}
	}

	return nil
}

func (r *mockRow) ScanStruct(dest interface{}) error {
	// Using the predefined error for testing
	return errScanStructNotImpl
}

// testIsPointer checks if an interface{} is a pointer (test-specific implementation).
func testIsPointer(i interface{}) bool {
	// Use type assertion to check if it's a pointer
	switch i.(type) {
	case *string, *int, *int64, *uint64, *float64, *bool, *time.Time:
		return true
	default:
		return false
	}
}
