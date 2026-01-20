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
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	testSchemaVersionsQuery = "SELECT version FROM wrstat_schema_version"
	testPingQuery           = "SELECT 1"
	testInsertMountStmt     = "INSERT INTO wrstat_mounts (mount_path, switched_at, " +
		"active_snapshot, updated_at) VALUES (?, ?, ?, ?)"
	testInsertChildrenStmt = "INSERT INTO wrstat_children (mount_path, snapshot_id, " +
		"parent_dir, child) VALUES (?, ?, ?, ?)"
	testInsertDGUTAStmt = "INSERT INTO wrstat_dguta (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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

func newClickHouseTestHarness(t *testing.T) *clickHouseTestHarness {
	t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		refuseNonLocalhostDSN(t, envDSN)

		return &clickHouseTestHarness{t: t, tcpPort: 0, httpPort: 0, baseDir: "", binPath: ""}
	}

	binPath := findClickHouseBinary(t)

	baseDir := t.TempDir()
	tcpPort := pickFreePort(t)

	httpPort := pickFreePort(t)
	for httpPort == tcpPort {
		httpPort = pickFreePort(t)
	}

	doneCh, exitErr, stdoutPath, stderrPath := startClickHouseServer(t, binPath, baseDir, tcpPort, httpPort)

	th := &clickHouseTestHarness{
		t:        t,
		tcpPort:  tcpPort,
		httpPort: httpPort,
		binPath:  binPath,
		baseDir:  baseDir,
		doneCh:   doneCh,
		exitErr:  exitErr,
		stdout:   stdoutPath,
		stderr:   stderrPath,
	}

	th.waitUntilReady()

	return th
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
		"clickhouse://default@127.0.0.1:%d/default?database=%s&dial_timeout=1s",
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

func (h *clickHouseTestHarness) waitUntilReady() {
	h.t.Helper()

	dsn := h.baseDSN("default")
	conn := h.openConn(dsn)

	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(30 * time.Second)

	for {
		select {
		case <-h.doneCh:
			stdout := readFileOrEmpty(h.stdout)
			stderr := readFileOrEmpty(h.stderr)
			h.t.Fatalf(
				"clickhouse server exited early: %v\nstdout:\n%s\nstderr:\n%s",
				*h.exitErr,
				stdout,
				stderr,
			)
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := conn.Exec(ctx, testPingQuery)

		cancel()

		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			stdout := readFileOrEmpty(h.stdout)
			stderr := readFileOrEmpty(h.stderr)
			h.t.Fatalf(
				"clickhouse server did not become ready: %v\nstdout:\n%s\nstderr:\n%s",
				err,
				stdout,
				stderr,
			)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func readFileOrEmpty(path string) string {
	if path == "" {
		return ""
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(b)
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

	fallback := "/software/hgi/installs/clickhouse/clickhouse"
	if _, statErr := os.Stat(fallback); statErr == nil {
		return fallback
	}

	t.Skip("clickhouse binary not found")

	return ""
}

func pickFreePort(t *testing.T) int {
	t.Helper()

	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to pick free port: %v", err)
	}

	defer func() { _ = l.Close() }()

	addr := l.Addr().String()

	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("failed to parse listener addr %q: %v", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse listener port %q: %v", portStr, err)
	}

	return port
}

func startClickHouseServer(
	t *testing.T,
	binPath string,
	baseDir string,
	tcpPort int,
	httpPort int,
) (<-chan struct{}, *error, string, string) {
	t.Helper()

	dataPath := filepath.Join(baseDir, "data")
	stdoutPath := filepath.Join(baseDir, "clickhouse.stdout.log")
	stderrPath := filepath.Join(baseDir, "clickhouse.stderr.log")

	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("failed to create clickhouse data dir: %v", err)
	}

	crtPath, keyPath := writeSelfSignedTLSCertPair(t, baseDir)

	args := []string{
		"server",
		"--",
		"--listen_host=127.0.0.1",
		"--tcp_port=" + strconv.Itoa(tcpPort),
		"--tcp_port_secure=0",
		"--http_port=" + strconv.Itoa(httpPort),
		"--https_port=0",
		"--mysql_port=0",
		"--postgresql_port=0",
		"--grpc_port=0",
		"--openSSL.server.certificateFile=" + crtPath,
		"--openSSL.server.privateKeyFile=" + keyPath,
		"--path=" + dataPath + string(os.PathSeparator),
	}

	cmdCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, binPath, args...)
	cmd.Dir = baseDir

	cmd.Env = append(os.Environ(), "CLICKHOUSE_WATCHDOG_ENABLE=0")

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		t.Fatalf("failed to create clickhouse stdout log: %v", err)
	}

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		_ = stdoutFile.Close()

		t.Fatalf("failed to create clickhouse stderr log: %v", err)
	}

	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()

		t.Fatalf("failed to start clickhouse: %v", err)
	}

	doneCh := make(chan struct{})
	exitErr := new(error)

	go func() {
		*exitErr = cmd.Wait()

		close(doneCh)

		_ = stdoutFile.Close()
		_ = stderrFile.Close()
	}()

	t.Cleanup(func() {
		defer cancel()

		if cmd.Process == nil {
			return
		}

		done := make(chan struct{})

		go func() {
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Logf("failed to signal clickhouse server: %v", err)
			}

			close(done)
		}()

		select {
		case <-doneCh:
			return
		case <-time.After(5 * time.Second):
			if err := cmd.Process.Kill(); err != nil {
				t.Logf("failed to kill clickhouse server: %v", err)
			}

			<-doneCh
			<-done
		}
	})

	return doneCh, exitErr, stdoutPath, stderrPath
}

func writeSelfSignedTLSCertPair(t *testing.T, dir string) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate TLS key: %v", err)
	}

	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create TLS certificate: %v", err)
	}

	crtPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	if err := os.WriteFile(crtPath, crtPEM, 0o600); err != nil {
		t.Fatalf("failed to write TLS certificate: %v", err)
	}

	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("failed to write TLS private key: %v", err)
	}

	return crtPath, keyPath
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
