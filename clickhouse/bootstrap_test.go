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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testSystemTablesQuery      = "SELECT name FROM system.tables WHERE database = ? ORDER BY name"
	testSystemColumnsQuery     = "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY name"
	testSystemTableEngineQuery = "SELECT engine FROM system.tables WHERE database = ? AND name = ? LIMIT 1"

	schemaVersionBootstrapHelperEnv       = "WRSTAT_SCHEMA_BOOTSTRAP_HELPER"
	schemaVersionBootstrapDSNEnv          = "WRSTAT_SCHEMA_BOOTSTRAP_DSN"
	schemaVersionBootstrapDatabaseEnv     = "WRSTAT_SCHEMA_BOOTSTRAP_DATABASE"
	schemaVersionBootstrapQueryTimeoutEnv = "WRSTAT_SCHEMA_BOOTSTRAP_QUERY_TIMEOUT"
	schemaVersionBootstrapHelperIDEnv     = "WRSTAT_SCHEMA_BOOTSTRAP_HELPER_ID"
	schemaVersionBootstrapSyncDirEnv      = "WRSTAT_SCHEMA_BOOTSTRAP_SYNC_DIR"
	schemaVersionBootstrapReleaseFile     = "release"
	schemaVersionBootstrapReadyFilePrefix = "ready-"
)

type bootstrapTestError string

const (
	errBootstrapTestAccessDenied               bootstrapTestError = "access denied"
	errBootstrapTestUnexpectedCall             bootstrapTestError = "unexpected call"
	errBootstrapTestUnexpectedConnection       bootstrapTestError = "unexpected connection attempt"
	errBootstrapTestUnexpectedScanDestination  bootstrapTestError = "unexpected scan destination"
	errBootstrapTestUnexpectedScanDestinationN bootstrapTestError = "unexpected scan destination count"
)

func (e bootstrapTestError) Error() string {
	return string(e)
}

type bootstrapTestRow struct {
	err error
}

func (r bootstrapTestRow) Err() error {
	return r.err
}

func (r bootstrapTestRow) Scan(...any) error {
	return r.err
}

func (r bootstrapTestRow) ScanStruct(any) error {
	return r.err
}

func waitForSchemaVersionBootstrapHelpers(
	t *testing.T,
	ctx context.Context,
	syncDir string,
	helperIDs []string,
) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		allReady := true

		for _, helperID := range helperIDs {
			_, err := os.Stat(schemaVersionBootstrapReadyPath(syncDir, helperID))
			switch {
			case err == nil:
			case os.IsNotExist(err):
				allReady = false
			default:
				t.Fatalf("failed to stat bootstrap helper %s ready file: %v", helperID, err)
			}
		}

		if allReady {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("bootstrap helpers did not reach schema insert barrier: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func schemaVersionBootstrapReadyPath(syncDir, helperID string) string {
	return filepath.Join(
		syncDir,
		schemaVersionBootstrapReadyFilePrefix+helperID,
	)
}

func tableEngine(ctx context.Context, t *testing.T, conn ch.Conn, database, table string) string {
	t.Helper()

	row := conn.QueryRow(ctx, testSystemTableEngineQuery, database, table)

	var engine string
	if err := row.Scan(&engine); err != nil {
		t.Fatalf("failed to scan table engine: %v", err)
	}

	return engine
}

type bootstrapTestConn struct {
	pingErr error
	execErr error

	closed atomic.Bool

	mu       sync.Mutex
	executed []string
}

func (c *bootstrapTestConn) Contributors() []string {
	return nil
}

func (c *bootstrapTestConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}

func (c *bootstrapTestConn) Select(context.Context, any, string, ...any) error {
	return errBootstrapTestUnexpectedCall
}

func (c *bootstrapTestConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errBootstrapTestUnexpectedCall
}

func (c *bootstrapTestConn) QueryRow(context.Context, string, ...any) driver.Row {
	return bootstrapTestRow{err: errBootstrapTestUnexpectedCall}
}

func (c *bootstrapTestConn) PrepareBatch(
	context.Context,
	string,
	...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return nil, errBootstrapTestUnexpectedCall
}

func (c *bootstrapTestConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.executed = append(c.executed, query)

	return c.execErr
}

func (c *bootstrapTestConn) AsyncInsert(context.Context, string, bool, ...any) error {
	return errBootstrapTestUnexpectedCall
}

func (c *bootstrapTestConn) Ping(context.Context) error {
	return c.pingErr
}

func (c *bootstrapTestConn) Stats() driver.Stats {
	return driver.Stats{}
}

func (c *bootstrapTestConn) Close() error {
	c.closed.Store(true)

	return nil
}

func (c *bootstrapTestConn) executedQueries() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	queries := make([]string, len(c.executed))
	copy(queries, c.executed)

	return queries
}

func TestConnectAndBootstrap(t *testing.T) {
	Convey("connectAndBootstrap only falls back when the configured database is missing", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cfg := Config{
			DSN:      "clickhouse://127.0.0.1:9000/default?database=wrstat",
			Database: "wrstat",
		}

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		Convey("it does not reconnect to default when the configured database already exists", func() {
			targetConn := &bootstrapTestConn{}
			openedDatabases := make([]string, 0, 1)

			conn, connectErr := connectAndBootstrapWith(
				ctx,
				opts,
				cfg.Database,
				time.Second,
				func(callOpts *ch.Options) (ch.Conn, error) {
					openedDatabases = append(openedDatabases, callOpts.Auth.Database)
					if callOpts.Auth.Database == defaultDatabaseName {
						return nil, errBootstrapTestAccessDenied
					}

					return targetConn, nil
				},
				func(context.Context, ch.Conn) error { return nil },
			)

			So(connectErr, ShouldBeNil)
			So(conn, ShouldEqual, targetConn)
			So(openedDatabases, ShouldResemble, []string{"wrstat"})
			So(targetConn.closed.Load(), ShouldBeFalse)
		})

		Convey("it reconnects via default only after an unknown database failure", func() {
			missingErr := &chproto.Exception{
				Code:    81,
				Name:    "UNKNOWN_DATABASE",
				Message: "Database wrstat does not exist",
			}

			missingConn := &bootstrapTestConn{pingErr: missingErr}
			adminConn := &bootstrapTestConn{}
			readyConn := &bootstrapTestConn{}

			openedDatabases := make([]string, 0, 3)

			conn, connectErr := connectAndBootstrapWith(
				ctx,
				opts,
				cfg.Database,
				time.Second,
				func(callOpts *ch.Options) (ch.Conn, error) {
					openedDatabases = append(openedDatabases, callOpts.Auth.Database)

					switch len(openedDatabases) {
					case 1:
						return missingConn, nil
					case 2:
						return adminConn, nil
					case 3:
						return readyConn, nil
					default:
						return nil, errBootstrapTestUnexpectedConnection
					}
				},
				func(context.Context, ch.Conn) error { return nil },
			)

			So(connectErr, ShouldBeNil)
			So(conn, ShouldEqual, readyConn)
			So(openedDatabases, ShouldResemble, []string{"wrstat", defaultDatabaseName, "wrstat"})
			So(adminConn.executedQueries(), ShouldResemble, []string{createDatabaseStmtPrefix + "`wrstat`"})
			So(missingConn.closed.Load(), ShouldBeTrue)
			So(adminConn.closed.Load(), ShouldBeTrue)
			So(readyConn.closed.Load(), ShouldBeFalse)
		})
	})
}

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

		So(tableEngine(ctx, t, conn, cfg.Database, "wrstat_schema_version"), ShouldEqual, "TinyLog")
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

func runSchemaVersionBootstrapHelperProcess(t *testing.T) {
	t.Helper()

	helperID := os.Getenv(schemaVersionBootstrapHelperIDEnv)
	syncDir := os.Getenv(schemaVersionBootstrapSyncDirEnv)
	queryTimeout := 5 * time.Second

	if rawTimeout := os.Getenv(schemaVersionBootstrapQueryTimeoutEnv); rawTimeout != "" {
		parsedTimeout, err := time.ParseDuration(rawTimeout)
		if err != nil {
			t.Fatalf("bootstrap helper invalid query timeout %q: %v", rawTimeout, err)
		}

		queryTimeout = parsedTimeout
	}

	cfg := Config{
		DSN:          os.Getenv(schemaVersionBootstrapDSNEnv),
		Database:     os.Getenv(schemaVersionBootstrapDatabaseEnv),
		PollInterval: time.Second,
		QueryTimeout: queryTimeout,
	}

	if helperID == "" || syncDir == "" || cfg.DSN == "" || cfg.Database == "" {
		t.Fatal("bootstrap helper missing configuration")
	}

	if err := os.WriteFile(
		schemaVersionBootstrapReadyPath(syncDir, helperID),
		[]byte("ready"),
		0o600,
	); err != nil {
		t.Fatalf("failed to write bootstrap ready file: %v", err)
	}

	waitForSchemaVersionBootstrapRelease(t, syncDir)

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("bootstrap helper %s failed to create client: %v", helperID, err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("bootstrap helper %s failed to close client: %v", helperID, err)
	}
}

func waitForSchemaVersionBootstrapRelease(t *testing.T, syncDir string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	releasePath := schemaVersionBootstrapReleasePath(syncDir)

	for time.Now().Before(deadline) {
		_, err := os.Stat(releasePath)
		switch {
		case err == nil:
			return
		case os.IsNotExist(err):
			time.Sleep(10 * time.Millisecond)
		default:
			t.Fatalf("failed to stat bootstrap release file: %v", err)
		}
	}

	t.Fatalf("timed out waiting for bootstrap release file %q", releasePath)
}

func schemaVersionBootstrapReleasePath(syncDir string) string {
	return filepath.Join(syncDir, schemaVersionBootstrapReleaseFile)
}

type schemaVersionTestStore struct {
	mu sync.Mutex

	versionRows      uint64
	initialReads     int
	initialReadReady chan struct{}
	initialReadClose sync.Once
}

func newSchemaVersionTestStore() *schemaVersionTestStore {
	return &schemaVersionTestStore{initialReadReady: make(chan struct{})}
}

func (s *schemaVersionTestStore) captureStats() (uint64, *uint32, *uint32) {
	s.mu.Lock()
	count := s.versionRows
	waitForConcurrentRead := false

	if count == 0 {
		s.initialReads++

		switch s.initialReads {
		case 1:
			waitForConcurrentRead = true
		default:
			s.initialReadClose.Do(func() { close(s.initialReadReady) })
		}
	}

	s.mu.Unlock()

	if waitForConcurrentRead {
		select {
		case <-s.initialReadReady:
		case <-time.After(100 * time.Millisecond):
		}
	}

	if count == 0 {
		return 0, nil, nil
	}

	version := uint32(1)

	return count, &version, &version
}

func (s *schemaVersionTestStore) insertVersion() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.versionRows++
}

func TestEnsureSchemaVersion(t *testing.T) {
	Convey("ensureSchemaVersion leaves a fresh database with exactly one version row", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		conn := &schemaVersionTestConn{store: newSchemaVersionTestStore()}
		errs := make(chan error, 2)

		for range 2 {
			go func() {
				errs <- ensureSchemaVersion(ctx, conn)
			}()
		}

		So(<-errs, ShouldBeNil)
		So(<-errs, ShouldBeNil)

		count, minVersion, maxVersion, err := schemaVersionStatsFromDB(ctx, conn)
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 1)
		So(minVersion, ShouldNotBeNil)
		So(maxVersion, ShouldNotBeNil)
		So(*minVersion, ShouldEqual, 1)
		So(*maxVersion, ShouldEqual, 1)
	})
}

type schemaVersionTestRows struct {
	count uint64

	minVersion *uint32
	maxVersion *uint32

	next bool
}

func (r *schemaVersionTestRows) Next() bool {
	if r.next {
		return false
	}

	r.next = true

	return true
}

func (r *schemaVersionTestRows) Scan(dest ...any) error {
	if len(dest) != 3 {
		return errBootstrapTestUnexpectedScanDestinationN
	}

	count, ok := dest[0].(*uint64)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	minVersion, ok := dest[1].(**uint32)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	maxVersion, ok := dest[2].(**uint32)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	*count = r.count
	*minVersion = r.minVersion
	*maxVersion = r.maxVersion

	return nil
}

func (r *schemaVersionTestRows) ScanStruct(any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *schemaVersionTestRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *schemaVersionTestRows) Totals(...any) error {
	return errBootstrapTestUnexpectedCall
}

func (r *schemaVersionTestRows) Columns() []string {
	return []string{"count", "min", "max"}
}

func (r *schemaVersionTestRows) Close() error {
	return nil
}

func (r *schemaVersionTestRows) Err() error {
	return nil
}

type schemaVersionTestConn struct {
	store *schemaVersionTestStore
}

func (c *schemaVersionTestConn) Contributors() []string {
	return nil
}

func (c *schemaVersionTestConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}

func (c *schemaVersionTestConn) Select(context.Context, any, string, ...any) error {
	return errBootstrapTestUnexpectedCall
}

func (c *schemaVersionTestConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if query != schemaVersionStatsQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	count, minVersion, maxVersion := c.store.captureStats()

	return &schemaVersionTestRows{count: count, minVersion: minVersion, maxVersion: maxVersion}, nil
}

func (c *schemaVersionTestConn) QueryRow(context.Context, string, ...any) driver.Row {
	return bootstrapTestRow{err: errBootstrapTestUnexpectedCall}
}

func (c *schemaVersionTestConn) PrepareBatch(
	context.Context,
	string,
	...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return nil, errBootstrapTestUnexpectedCall
}

func (c *schemaVersionTestConn) Exec(_ context.Context, query string, _ ...any) error {
	if query != insertSchemaVersionStmt {
		return errBootstrapTestUnexpectedCall
	}

	c.store.insertVersion()

	return nil
}

func (c *schemaVersionTestConn) AsyncInsert(context.Context, string, bool, ...any) error {
	return errBootstrapTestUnexpectedCall
}

func (c *schemaVersionTestConn) Ping(context.Context) error {
	return nil
}

func (c *schemaVersionTestConn) Stats() driver.Stats {
	return driver.Stats{}
}

func (c *schemaVersionTestConn) Close() error {
	return nil
}

type schemaVersionBootstrapHelper struct {
	id string

	cmd *exec.Cmd

	output  bytes.Buffer
	waitCh  chan struct{}
	waitErr error
}

func startSchemaVersionBootstrapHelper(
	t *testing.T,
	ctx context.Context,
	cfg Config,
	syncDir, helperID string,
) *schemaVersionBootstrapHelper {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to resolve test executable: %v", err)
	}

	helper := &schemaVersionBootstrapHelper{id: helperID}
	helper.cmd = exec.CommandContext(
		ctx,
		exe,
		"-test.run=^TestNewClientBootstrapIsCrossProcessSafe$",
	)
	helper.cmd.Stdout = &helper.output
	helper.cmd.Stderr = &helper.output

	helper.cmd.Env = append(
		os.Environ(),
		"WRSTAT_ENV=test",
		schemaVersionBootstrapHelperEnv+"=1",
		schemaVersionBootstrapDSNEnv+"="+cfg.DSN,
		schemaVersionBootstrapDatabaseEnv+"="+cfg.Database,
		schemaVersionBootstrapQueryTimeoutEnv+"="+cfg.QueryTimeout.String(),
		schemaVersionBootstrapHelperIDEnv+"="+helperID,
		schemaVersionBootstrapSyncDirEnv+"="+syncDir,
	)

	if err := helper.cmd.Start(); err != nil {
		t.Fatalf("failed to start bootstrap helper %s: %v", helperID, err)
	}

	helper.waitCh = make(chan struct{})

	go func() {
		helper.waitErr = helper.cmd.Wait()
		close(helper.waitCh)
	}()

	return helper
}

func (h *schemaVersionBootstrapHelper) wait() error {
	<-h.waitCh

	if h.waitErr != nil {
		return fmt.Errorf(
			"bootstrap helper %s failed: %w\n%s",
			h.id,
			h.waitErr,
			h.output.String(),
		)
	}

	return nil
}

func (h *schemaVersionBootstrapHelper) ensureStillRunning(t *testing.T, d time.Duration) {
	t.Helper()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-h.waitCh:
		if h.waitErr != nil {
			t.Fatalf(
				"bootstrap helper %s exited before schema lock release: %v\n%s",
				h.id,
				h.waitErr,
				h.output.String(),
			)
		}

		t.Fatalf(
			"bootstrap helper %s exited before schema lock release\n%s",
			h.id,
			h.output.String(),
		)
	case <-timer.C:
	}
}

func TestNewClientBootstrapIsCrossProcessSafe(t *testing.T) {
	if os.Getenv(schemaVersionBootstrapHelperEnv) == "1" {
		runSchemaVersionBootstrapHelperProcess(t)

		return
	}

	Convey("NewClient keeps wrstat_schema_version to a single row across helper processes", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.PollInterval = time.Second
		cfg.QueryTimeout = 5 * time.Second

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		lock, err := newSchemaBootstrapLock(opts, cfg.Database)
		So(err, ShouldBeNil)
		So(lock.acquire(ctx), ShouldBeNil)

		released := false

		defer func() {
			if released {
				return
			}

			So(lock.release(), ShouldBeNil)
		}()

		syncDir := t.TempDir()
		helpers := []*schemaVersionBootstrapHelper{
			startSchemaVersionBootstrapHelper(t, ctx, cfg, syncDir, "1"),
			startSchemaVersionBootstrapHelper(t, ctx, cfg, syncDir, "2"),
		}

		waitForSchemaVersionBootstrapHelpers(t, ctx, syncDir, []string{"1", "2"})
		releaseSchemaVersionBootstrapHelpers(t, syncDir)

		for _, helper := range helpers {
			helper.ensureStillRunning(t, 200*time.Millisecond)
		}

		So(lock.release(), ShouldBeNil)

		released = true

		for _, helper := range helpers {
			So(helper.wait(), ShouldBeNil)
		}

		So(th.schemaVersions(cfg), ShouldResemble, []uint32{1})
	})
}

func releaseSchemaVersionBootstrapHelpers(t *testing.T, syncDir string) {
	t.Helper()

	if err := os.WriteFile(
		schemaVersionBootstrapReleasePath(syncDir),
		[]byte("release"),
		0o600,
	); err != nil {
		t.Fatalf("failed to release bootstrap helpers: %v", err)
	}
}

func TestNewClientBootstrapLockWaitDoesNotUseQueryTimeout(t *testing.T) {
	if os.Getenv(schemaVersionBootstrapHelperEnv) == "1" {
		runSchemaVersionBootstrapHelperProcess(t)

		return
	}

	Convey("NewClient can wait on the schema bootstrap lock longer than QueryTimeout", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.PollInterval = time.Second
		cfg.QueryTimeout = 100 * time.Millisecond

		opts, err := optionsFromConfig(cfg)
		So(err, ShouldBeNil)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		lock, err := newSchemaBootstrapLock(opts, cfg.Database)
		So(err, ShouldBeNil)
		So(lock.acquire(ctx), ShouldBeNil)

		released := false

		defer func() {
			if released {
				return
			}

			So(lock.release(), ShouldBeNil)
		}()

		syncDir := t.TempDir()
		helper := startSchemaVersionBootstrapHelper(t, ctx, cfg, syncDir, "timeout")

		waitForSchemaVersionBootstrapHelpers(t, ctx, syncDir, []string{"timeout"})
		releaseSchemaVersionBootstrapHelpers(t, syncDir)

		helper.ensureStillRunning(t, 400*time.Millisecond)

		So(lock.release(), ShouldBeNil)

		released = true

		So(helper.wait(), ShouldBeNil)
		So(th.schemaVersions(cfg), ShouldResemble, []uint32{1})
	})
}
