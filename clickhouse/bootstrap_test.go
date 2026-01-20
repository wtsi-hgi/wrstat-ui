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
	"os"
	"sort"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testSystemTablesQuery  = "SELECT name FROM system.tables WHERE database = ? ORDER BY name"
	testSystemColumnsQuery = "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY name"
)

func TestNewClientBootstrapsSchema(t *testing.T) {
	Convey("NewClient bootstraps database and schema", t, func() {
		os.Setenv("WRSTAT_ENV", "test")

		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.PollInterval = time.Second
		cfg.QueryTimeout = 5 * time.Second

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		So(c.Close(), ShouldBeNil)

		versions := th.schemaVersions(cfg)
		So(versions, ShouldResemble, []uint32{1})
	})

	Convey("NewClient bootstraps the full schema", t, func() {
		os.Setenv("WRSTAT_ENV", "test")

		Reset(func() { os.Unsetenv("WRSTAT_ENV") })

		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.PollInterval = time.Second
		cfg.QueryTimeout = 5 * time.Second

		c, err := NewClient(cfg)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Reset(func() { So(c.Close(), ShouldBeNil) })

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tables := listTableNames(ctx, t, conn, cfg.Database)
		So(tables, ShouldContain, "wrstat_schema_version")
		So(tables, ShouldContain, "wrstat_mounts")
		So(tables, ShouldContain, "wrstat_mounts_active")
		So(tables, ShouldContain, "wrstat_dguta")
		So(tables, ShouldContain, "wrstat_children")
		So(tables, ShouldContain, "wrstat_basedirs_group_usage")
		So(tables, ShouldContain, "wrstat_basedirs_user_usage")
		So(tables, ShouldContain, "wrstat_basedirs_group_subdirs")
		So(tables, ShouldContain, "wrstat_basedirs_user_subdirs")
		So(tables, ShouldContain, "wrstat_basedirs_history")
		So(tables, ShouldContain, "wrstat_files")

		cols := listColumnNames(ctx, t, conn, cfg.Database, "wrstat_mounts_active")
		So(cols, ShouldContain, "mount_path")
		So(cols, ShouldContain, "snapshot_id")
		So(cols, ShouldContain, "updated_at")
	})
}

func listTableNames(ctx context.Context, t *testing.T, conn ch.Conn, database string) []string {
	t.Helper()

	rows, err := conn.Query(ctx, testSystemTablesQuery, database)
	if err != nil {
		t.Fatalf("failed to query system.tables: %v", err)
	}

	defer func() { _ = rows.Close() }()

	names := make([]string, 0, 16)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan table name: %v", err)
		}

		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func listColumnNames(
	ctx context.Context,
	t *testing.T,
	conn ch.Conn,
	database string,
	table string,
) []string {
	t.Helper()

	rows, err := conn.Query(
		ctx,
		testSystemColumnsQuery,
		database,
		table,
	)
	if err != nil {
		t.Fatalf("failed to query system.columns: %v", err)
	}

	defer func() { _ = rows.Close() }()

	names := make([]string, 0, 16)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan column name: %v", err)
		}

		names = append(names, name)
	}

	sort.Strings(names)

	return names
}
