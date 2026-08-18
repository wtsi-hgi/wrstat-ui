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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

const (
	testDatabaseName        = "wrstat"
	testNativeDSN           = "clickhouse://localhost:9000/?database=wrstat"
	testSchemaVersionsQuery = "SELECT version FROM wrstat_schema_version"
	testPingQuery           = "SELECT 1"
	testInsertMountStmt     = "INSERT INTO wrstat_mount_events (mount_path, event_at, event_type, " +
		"snapshot_id, updated_at, reason) VALUES (?, ?, 1, ?, ?, 'publish')"
	testInsertDGUTAStmt = "INSERT INTO wrstat_dir_facts (mount_path, snapshot_id, dir, updated_at, gids, uids, " +
		"fts, ages, counts, sizes, atime_mins, mtime_maxs, atime_buckets, mtime_buckets, refreshed_at) " +
		"VALUES (?, ?, ?, now(), [?], [?], [?], [?], [?], [?], [?], [?], [?], [?], now())"
)

//nolint:gochecknoglobals // Shared server cuts startup churn; per-test databases still isolate state.
var (
	sharedClickHouseServerOnce sync.Once
	sharedClickHouseServer     *sharedClickHouseTestServer
	errSharedClickHouseServer  error
)

type clickHouseTestHarness struct {
	t        *testing.T
	tcpPort  int
	httpPort int
	binPath  string
	baseDir  string
	doneCh   <-chan struct{}
	exitErr  *error
	stdout   string
	stderr   string
}

type sharedClickHouseTestServer struct {
	tcpPort int
	binPath string
	baseDir string
	server  *internaltest.LocalClickHouseServer
}

func TestMain(m *testing.M) {
	code := m.Run()

	if sharedClickHouseServer != nil {
		sharedClickHouseServer.server.Stop()
		_ = os.RemoveAll(sharedClickHouseServer.baseDir)
	}

	os.Exit(code)
}

func newClickHouseTestHarness(t *testing.T) *clickHouseTestHarness {
	t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		refuseNonLocalhostDSN(t, envDSN)

		return &clickHouseTestHarness{t: t, tcpPort: 0, httpPort: 0, baseDir: "", binPath: ""}
	}

	shared := getSharedClickHouseTestServer(t)

	return &clickHouseTestHarness{
		t:       t,
		tcpPort: shared.tcpPort,
		binPath: shared.binPath,
		baseDir: shared.baseDir,
		doneCh:  nil,
		exitErr: nil,
		stdout:  shared.server.StdoutPath,
		stderr:  shared.server.StderrPath,
	}
}

func getSharedClickHouseTestServer(t *testing.T) *sharedClickHouseTestServer {
	t.Helper()

	sharedClickHouseServerOnce.Do(func() {
		sharedClickHouseServer = startSharedClickHouseTestServer(t)
	})

	if errSharedClickHouseServer != nil {
		t.Fatalf("failed to start shared clickhouse test server: %v", errSharedClickHouseServer)
	}

	if sharedClickHouseServer == nil {
		t.Skip("shared clickhouse test server unavailable")
	}

	return sharedClickHouseServer
}

func startSharedClickHouseTestServer(t *testing.T) *sharedClickHouseTestServer {
	t.Helper()

	binPath := findClickHouseBinary(t)

	server, err := internaltest.StartLocalClickHouseServer(binPath, "")
	if err != nil {
		errSharedClickHouseServer = err

		return nil
	}

	return &sharedClickHouseTestServer{
		tcpPort: server.TCPPort,
		binPath: binPath,
		baseDir: server.BaseDir,
		server:  server,
	}
}

func (h *clickHouseTestHarness) newConfig() Config {
	h.t.Helper()

	db := newTestDatabaseName(h.t)

	return Config{
		DSN:      h.baseDSN(db),
		Database: db,
	}
}

func newTestDatabaseName(t *testing.T) string {
	t.Helper()

	usr := "unknown"
	if u, err := user.Current(); err == nil && u.Username != "" {
		usr = u.Username
	}

	rnd := make([]byte, 6)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatalf("failed to generate random suffix: %v", err)
	}

	randHex := hex.EncodeToString(rnd)

	return fmt.Sprintf(
		"wrstat_ui_test_%s_%d_%s",
		sanitizeDatabaseSuffix(usr),
		os.Getpid(),
		randHex,
	)
}

func (h *clickHouseTestHarness) baseDSN(database string) string {
	h.t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		u, err := url.Parse(envDSN)
		if err != nil {
			h.t.Fatalf("invalid WRSTAT_CLICKHOUSE_DSN: %v", err)
		}

		q := u.Query()
		q.Set("database", database)
		u.RawQuery = q.Encode()

		return u.String()
	}

	return fmt.Sprintf(
		"clickhouse://default@127.0.0.1:%d/default?database=%s&dial_timeout=1s&compress=lz4",
		h.tcpPort,
		url.QueryEscape(database),
	)
}

func (h *clickHouseTestHarness) schemaVersions(cfg Config) []uint32 {
	h.t.Helper()

	conn := h.openConn(cfg.DSN)

	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, testSchemaVersionsQuery)
	if err != nil {
		h.t.Fatalf("failed to query schema versions: %v", err)
	}

	defer func() { _ = rows.Close() }()

	versions := make([]uint32, 0, 1)

	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			h.t.Fatalf("failed to scan schema version: %v", err)
		}

		versions = append(versions, v)
	}

	return versions
}

func (h *clickHouseTestHarness) openConn(dsn string) ch.Conn {
	h.t.Helper()

	refuseNonLocalhostDSN(h.t, dsn)

	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		h.t.Fatalf("failed to parse DSN: %v", err)
	}

	conn, err := ch.Open(opts)
	if err != nil {
		h.t.Fatalf("failed to open clickhouse connection: %v", err)
	}

	return conn
}

func refuseNonLocalhostDSN(t *testing.T, dsn string) {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("invalid DSN: %v", err)
	}

	host := u.Hostname()
	if host == "" {
		t.Fatalf("invalid DSN host")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		t.Fatalf("failed to resolve DSN host %q: %v", host, err)
	}

	for _, ip := range ips {
		if !ip.IP.IsLoopback() {
			t.Fatalf("refusing non-localhost DSN host %q (%v)", host, ip.IP)
		}
	}
}

func findClickHouseBinary(t *testing.T) string {
	t.Helper()

	bin, err := exec.LookPath("clickhouse")
	if err == nil {
		return bin
	}

	t.Skip("clickhouse binary not found")

	return ""
}

func sanitizeDatabaseSuffix(s string) string {
	if runtime.GOOS == "windows" {
		s = strings.ReplaceAll(s, "\\\\", "_")
	}

	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "@", "_")

	return s
}
