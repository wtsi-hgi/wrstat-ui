/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/provider"
	"github.com/wtsi-hgi/wrstat-ui/server"
)

const (
	b3CLIProjectMount = "/m/"
	b3CLIProjectDir   = "/m/project/"
)

const (
	b3CLICreateSchema3SnapshotSetsTable = "CREATE TABLE IF NOT EXISTS wrstat_schema3_snapshot_sets (" +
		"mount_path LowCardinality(String), snapshot_id UUID, schema3_version UInt32, " +
		"dir_facts_rows UInt64, parent_facts_rows UInt64, children_rows UInt64, " +
		"child_filter_all_rows UInt64, dir_filter_all_rows UInt64, manifest_sha256 String, " +
		"refreshed_at DateTime64(3)) ENGINE = MergeTree " +
		"PARTITION BY (mount_path, snapshot_id) ORDER BY (mount_path, snapshot_id, schema3_version)"
	b3CLIInsertSchema3SnapshotSet = "INSERT INTO wrstat_schema3_snapshot_sets " +
		"(mount_path, snapshot_id, schema3_version, dir_facts_rows, parent_facts_rows, children_rows, " +
		"child_filter_all_rows, dir_filter_all_rows, manifest_sha256, refreshed_at) " +
		"VALUES (?, toUUID(?), 1, 1, 1, 1, ?, ?, 'b3-cli-test', now())"
	b3CLICreateDirFilterAllTable = "CREATE TABLE IF NOT EXISTS wrstat_dir_filter_all (" +
		"mount_path LowCardinality(String), snapshot_id UUID, age UInt8, gid UInt32, uid UInt32, " +
		"ft UInt16, dir String, parent_dir String, count UInt64, size UInt64, atime_min Int64, " +
		"mtime_max Int64, atime_buckets Array(UInt64), mtime_buckets Array(UInt64), " +
		"filter_child_count UInt64, child_count UInt64, has_filter_children UInt8, " +
		"has_children UInt8, refreshed_at DateTime64(3)) ENGINE = MergeTree " +
		"PARTITION BY (mount_path, snapshot_id) " +
		"ORDER BY (mount_path, snapshot_id, age, gid, uid, ft, dir)"
	b3CLIInsertDirFilterAll = "INSERT INTO wrstat_dir_filter_all " +
		"(mount_path, snapshot_id, age, gid, uid, ft, dir, parent_dir, count, size, " +
		"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, " +
		"child_count, has_filter_children, has_children, refreshed_at)"
)

func TestWhereAgeFromUnusedUnchangedB3(t *testing.T) {
	Convey("B3.5 where --unused and --unchanged select the REST age probe", t, func() {
		age, ok := whereAgeFromUnusedUnchanged("1Y", "")
		So(ok, ShouldBeTrue)
		So(age, ShouldEqual, db.DGUTAgeA1Y)

		age, ok = whereAgeFromUnusedUnchanged("", "1Y")
		So(ok, ShouldBeTrue)
		So(age, ShouldEqual, db.DGUTAgeM1Y)

		age, ok = whereAgeFromUnusedUnchanged("", "")
		So(ok, ShouldBeTrue)
		So(age, ShouldEqual, db.DGUTAgeAll)

		_, ok = whereAgeFromUnusedUnchanged("1Y", "1Y")
		So(ok, ShouldBeFalse)
	})
}

type b3CLIWhereEnv struct {
	addr     string
	cert     string
	cfg      clickhouse.Config
	provider provider.Provider
	server   *server.Server
	stop     func() error
}

func newB3CLIWhereEnv(t *testing.T) *b3CLIWhereEnv {
	t.Helper()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("WRSTAT_ENV", "test")
	clickhouse.ResetTreeQueryCaches()

	harness := newB3CLIClickHouseHarness(t)
	cfg := harness.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{b3CLIProjectMount}

	seedB3CLIProjectFixture(t, cfg)
	markB3CLISchema3Ready(t, cfg)
	clickhouse.ResetTreeQueryCaches()
	clickhouse.ResetTreeQueryCacheStats(cfg)

	p, err := clickhouse.OpenProvider(cfg)
	So(err, ShouldBeNil)

	logWriter := gas.NewStringLogger()
	s := server.New(logWriter)
	s.WhiteListGroups(func(string) bool { return true })
	b3CLISeedNameCaches(s)

	cert, key, err := gas.CreateTestCert(t)
	So(err, ShouldBeNil)
	So(s.EnableAuthWithServerToken(cert, key, serverTokenBasename, func(_, _ string) (bool, string) {
		return true, "0"
	}), ShouldBeNil)
	So(s.AddTreePage(), ShouldBeNil)
	So(s.SetProvider(p), ShouldBeNil)

	addr, stop, err := gas.StartTestServer(s, cert, key)
	So(err, ShouldBeNil)

	return &b3CLIWhereEnv{
		addr:     addr,
		cert:     cert,
		cfg:      cfg,
		provider: p,
		server:   s,
		stop:     stop,
	}
}

func (e *b3CLIWhereEnv) close() {
	So(e.stop(), ShouldBeNil)
	So(e.provider.Close(), ShouldBeNil)
	clickhouse.ResetTreeQueryCaches()
}

func TestWhereCommandProjectFixtureB3(t *testing.T) {
	Convey("B3.5 where --dir uses a fresh server provider and uncached JSON subtree rows", t, func() {
		cases := []struct {
			manifestKey string
			flag        string
			value       string
		}{
			{manifestKey: "project_where_unused_1y", flag: "--unused", value: "1Y"},
			{manifestKey: "project_where_unchanged_1y", flag: "--unchanged", value: "1Y"},
		}

		for i := range cases {
			tc := cases[i]
			env := newB3CLIWhereEnv(t)

			clickhouse.ResetTreeQueryCacheStats(env.cfg)
			clickhouse.ResetSchema3FallbackRoutes()

			out := runB3CLIWhereCommand(t, env.addr, env.cert, tc.flag, tc.value)

			var got []*server.DirSummary
			So(json.Unmarshal([]byte(out), &got), ShouldBeNil)

			stats := clickhouse.ReadTreeQueryCacheStats(env.cfg)
			responseCacheHits := env.server.ResponseCacheHits()
			env.close()

			So(b3CLIWhereDigest(got), ShouldEqual, b3CLIProjectManifestDigest(tc.manifestKey))
			So(responseCacheHits, ShouldEqual, uint64(0))
			So(stats.FactVectorReads, ShouldEqual, uint64(0))
			So(clickhouse.ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
		}
	})
}

func runB3CLIWhereCommand(t *testing.T, addr, cert, flag, value string) string {
	t.Helper()
	resetWhereCommandFlagsForTest()

	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	So(err, ShouldBeNil)

	os.Stdout = writer

	RootCmd.SetArgs([]string{
		"where",
		"--dir", "/m/project/",
		flag, value,
		"--json",
		"--cert", cert,
		addr,
	})
	err = RootCmd.Execute()

	So(writer.Close(), ShouldBeNil)

	os.Stdout = stdout

	So(err, ShouldBeNil)

	var buf bytes.Buffer

	_, err = io.Copy(&buf, reader)
	So(err, ShouldBeNil)
	So(reader.Close(), ShouldBeNil)

	return buf.String()
}

func resetWhereCommandFlagsForTest() {
	whereQueryDir = "/"
	whereSplits = defaultWhereSplits
	whereGroups = ""
	whereUsers = ""
	whereTypes = ""
	whereSize = defaultSize
	whereAccess = 0
	whereShowSupergroups = false
	whereSupergroup = ""
	whereCert = ""
	whereJSON = false
	whereOrder = "size"
	whereShowUG = false
	whereUnused = ""
	whereUnchanged = ""

	mustSetWhereFlagForTest("dir", "/")
	mustSetWhereFlagForTest("splits", "2")
	mustSetWhereFlagForTest("groups", "")
	mustSetWhereFlagForTest("users", "")
	mustSetWhereFlagForTest("types", "")
	mustSetWhereFlagForTest("size", defaultSize)
	mustSetWhereFlagForTest("access", "0")
	mustSetWhereFlagForTest("show_areas", "false")
	mustSetWhereFlagForTest("area", "")
	mustSetWhereFlagForTest("cert", "")
	mustSetWhereFlagForTest("json", "false")
	mustSetWhereFlagForTest("order", "size")
	mustSetWhereFlagForTest("show_ug", "false")
	mustSetWhereFlagForTest("unused", "")
	mustSetWhereFlagForTest("unchanged", "")
}

func mustSetWhereFlagForTest(name, value string) {
	So(whereCmd.Flags().Set(name, value), ShouldBeNil)
}

func b3CLIWhereDigest(summaries []*server.DirSummary) string {
	elements := make([]b3CLIWhereDigestElement, len(summaries))
	for i, summary := range summaries {
		elements[i] = b3CLIWhereDigestElement{
			Dir:       summary.Dir,
			Count:     summary.Count,
			Size:      summary.Size,
			Users:     b3CLISortedStrings(summary.Users),
			Groups:    b3CLISortedStrings(summary.Groups),
			FileTypes: b3CLISortedStrings(summary.FileTypes),
			Age:       summary.Age,
		}
	}

	sort.Slice(elements, func(i, j int) bool {
		return elements[i].Dir < elements[j].Dir
	})

	data, err := json.Marshal(elements)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func b3CLISortedStrings(values []string) []string {
	cp := append([]string(nil), values...)
	sort.Strings(cp)

	return cp
}

func b3CLIProjectManifestDigest(key string) string {
	return map[string]string{
		"project_where_unused_1y":    "sha256:20b461c3d947a332c2c6f1f21c6958a10198fbed82c9a6d049e9912d22b65070",
		"project_where_unchanged_1y": "sha256:46f47c20afbca8f779689bc68e3d21d246cc37a16a12559d6f42820d37b8914c",
	}[key]
}

func startB3CLIClickHouseServer(
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

	crtPath, keyPath := writeB3CLISelfSignedTLSCertPair(t, baseDir)

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
		cancel()
		t.Fatalf("failed to create clickhouse stdout log: %v", err)
	}

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		_ = stdoutFile.Close()

		cancel()
		t.Fatalf("failed to create clickhouse stderr log: %v", err)
	}

	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()

		cancel()
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

func writeB3CLISelfSignedTLSCertPair(t *testing.T, dir string) (string, string) {
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

type b3CLIWhereDigestElement struct {
	Dir       string       `json:"dir"`
	Count     uint64       `json:"count"`
	Size      uint64       `json:"size"`
	Users     []string     `json:"users"`
	Groups    []string     `json:"groups"`
	FileTypes []string     `json:"filetypes"`
	Age       db.DirGUTAge `json:"age"`
}

type b3CLIProjectChild struct {
	dir   string
	age   db.DirGUTAge
	gid   uint32
	uid   uint32
	ft    db.DirGUTAFileType
	count uint64
	size  uint64
}

func b3CLIProjectChildrenForAge(age db.DirGUTAge) []b3CLIProjectChild {
	children := make([]b3CLIProjectChild, 0)

	for _, child := range b3CLIProjectAllChildren() {
		if child.age == age {
			children = append(children, child)
		}
	}

	return children
}

func b3CLIProjectAllChildren() []b3CLIProjectChild {
	return []b3CLIProjectChild{
		{
			dir:   b3CLIProjectDir + "alpha/",
			age:   db.DGUTAgeA1Y,
			gid:   7,
			uid:   11,
			ft:    db.DGUTAFileTypeBam,
			count: 10,
			size:  100,
		},
		{dir: b3CLIProjectDir + "beta/", age: db.DGUTAgeA1Y, gid: 8, uid: 12, ft: db.DGUTAFileTypeCram, count: 8, size: 80},
		{dir: b3CLIProjectDir + "gamma/", age: db.DGUTAgeA1Y, gid: 7, uid: 12, ft: db.DGUTAFileTypeBam, count: 6, size: 60},
		{dir: b3CLIProjectDir + "zeta/", age: db.DGUTAgeA1Y, gid: 9, uid: 13, ft: db.DGUTAFileTypeOther, count: 4, size: 40},
		{dir: b3CLIProjectDir + "delta/", age: db.DGUTAgeM1Y, gid: 8, uid: 11, ft: db.DGUTAFileTypeCram, count: 5, size: 50},
		{
			dir:   b3CLIProjectDir + "omega/",
			age:   db.DGUTAgeM1Y,
			gid:   9,
			uid:   13,
			ft:    db.DGUTAFileTypeOther,
			count: 3,
			size:  30,
		},
	}
}

type b3CLIProjectTreeFixture struct {
	age        db.DirGUTAge
	childCount uint64
}

func b3CLIProjectTreeFixtures() []b3CLIProjectTreeFixture {
	return []b3CLIProjectTreeFixture{
		{age: db.DGUTAgeA1Y, childCount: 4},
		{age: db.DGUTAgeM1Y, childCount: 2},
	}
}

type b3CLIClickHouseHarness struct {
	t        *testing.T
	tcpPort  int
	httpPort int
	baseDir  string
	doneCh   <-chan struct{}
	exitErr  *error
	stdout   string
	stderr   string
}

func newB3CLIClickHouseHarness(t *testing.T) *b3CLIClickHouseHarness {
	t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		b3CLIRefuseNonLocalhostDSN(t, envDSN)

		return &b3CLIClickHouseHarness{t: t}
	}

	bin, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse binary not found")
	}

	baseDir := t.TempDir()
	tcpPort := b3CLIPickFreePort(t)

	httpPort := b3CLIPickFreePort(t)
	for httpPort == tcpPort {
		httpPort = b3CLIPickFreePort(t)
	}

	doneCh, exitErr, stdoutPath, stderrPath := startB3CLIClickHouseServer(t, bin, baseDir, tcpPort, httpPort)
	h := &b3CLIClickHouseHarness{
		t:        t,
		tcpPort:  tcpPort,
		httpPort: httpPort,
		baseDir:  baseDir,
		doneCh:   doneCh,
		exitErr:  exitErr,
		stdout:   stdoutPath,
		stderr:   stderrPath,
	}
	h.waitUntilReady()

	return h
}

func (h *b3CLIClickHouseHarness) newConfig() clickhouse.Config {
	h.t.Helper()

	database := b3CLITestDatabaseName(h.t)

	return clickhouse.Config{
		DSN:      h.baseDSN(database),
		Database: database,
	}
}

func b3CLITestDatabaseName(t *testing.T) string {
	t.Helper()

	rnd := make([]byte, 6)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatalf("failed to generate random database suffix: %v", err)
	}

	return fmt.Sprintf("wrstat_ui_test_%d_%s", os.Getpid(), hex.EncodeToString(rnd))
}

func (h *b3CLIClickHouseHarness) baseDSN(database string) string {
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

func (h *b3CLIClickHouseHarness) waitUntilReady() {
	h.t.Helper()

	conn := openB3CLIClickHouseConn(h.t, h.baseDSN("default"))
	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(30 * time.Second)

	for {
		select {
		case <-h.doneCh:
			h.t.Fatalf(
				"clickhouse server exited early: %v\nstdout:\n%s\nstderr:\n%s",
				*h.exitErr,
				b3CLIReadFileOrEmpty(h.stdout),
				b3CLIReadFileOrEmpty(h.stderr),
			)
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := conn.Exec(ctx, "SELECT 1")

		cancel()

		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			h.t.Fatalf(
				"clickhouse server did not become ready: %v\nstdout:\n%s\nstderr:\n%s",
				err,
				b3CLIReadFileOrEmpty(h.stdout),
				b3CLIReadFileOrEmpty(h.stderr),
			)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func openB3CLIClickHouseConn(t *testing.T, dsn string) ch.Conn {
	t.Helper()
	b3CLIRefuseNonLocalhostDSN(t, dsn)

	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse DSN: %v", err)
	}

	conn, err := ch.Open(opts)
	if err != nil {
		t.Fatalf("failed to open clickhouse connection: %v", err)
	}

	return conn
}

func b3CLIReadFileOrEmpty(path string) string {
	if path == "" {
		return ""
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(data)
}

func seedB3CLIProjectFixture(t *testing.T, cfg clickhouse.Config) {
	t.Helper()

	paths := internaltest.NewDirectoryPathCreator()

	w, err := clickhouse.NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(b3CLIProjectMount)
	w.SetUpdatedAt(time.Date(2026, 6, 7, 18, 30, 0, 0, time.UTC))

	So(w.Add(db.RecordDGUTA{
		Dir:      paths.ToDirectoryPath(b3CLIProjectMount),
		Children: []string{"project/"},
		GUTAs: db.GUTAs{
			b3CLIGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	So(w.Add(db.RecordDGUTA{
		Dir:        paths.ToDirectoryPath(b3CLIProjectDir),
		ChildCount: uint64(len(b3CLIProjectAllChildren())),
		Children:   b3CLIProjectChildNames(),
		GUTAs: db.GUTAs{
			b3CLIGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	for _, child := range b3CLIProjectAllChildren() {
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(child.dir),
			GUTAs: db.GUTAs{
				b3CLIGUTA(child.gid, child.uid, child.ft, db.DGUTAgeAll, child.count, child.size),
				b3CLIGUTA(child.gid, child.uid, child.ft, child.age, child.count, child.size),
			},
		}), ShouldBeNil)
	}

	So(w.Close(), ShouldBeNil)
}

func b3CLIGUTA(
	gid, uid uint32,
	ft db.DirGUTAFileType,
	age db.DirGUTAge,
	count, size uint64,
) *db.GUTA {
	return &db.GUTA{
		GID:         gid,
		UID:         uid,
		FT:          ft,
		Age:         age,
		Count:       count,
		Size:        size,
		Atime:       100,
		Mtime:       200,
		ATimeRanges: [9]uint64{count, 0, 0, 0, 0, 0, 0, 0, 0},
		MTimeRanges: [9]uint64{0, count, 0, 0, 0, 0, 0, 0, 0},
	}
}

func b3CLIProjectChildNames() []string {
	children := make([]string, len(b3CLIProjectAllChildren()))
	for i, child := range b3CLIProjectAllChildren() {
		children[i] = child.dir[len(b3CLIProjectDir):]
	}

	return children
}

func markB3CLISchema3Ready(t *testing.T, cfg clickhouse.Config) {
	t.Helper()

	conn := openB3CLIClickHouseConn(t, cfg.DSN)
	defer func() { So(conn.Close(), ShouldBeNil) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(conn.Exec(ctx, b3CLICreateSchema3SnapshotSetsTable), ShouldBeNil)

	row := conn.QueryRow(
		ctx,
		"SELECT toString(snapshot_id) FROM wrstat_mounts_active WHERE mount_path = ?",
		b3CLIProjectMount,
	)

	var snapshotID string
	So(row.Scan(&snapshotID), ShouldBeNil)

	seedB3CLIDirFilterAll(t, conn, snapshotID)
	So(conn.Exec(
		ctx,
		b3CLIInsertSchema3SnapshotSet,
		b3CLIProjectMount,
		snapshotID,
		uint64(len(b3CLIProjectAllChildren())),
		uint64(len(b3CLIProjectAllChildren())*2),
	), ShouldBeNil)
}

func seedB3CLIDirFilterAll(t *testing.T, conn ch.Conn, snapshotID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	So(conn.Exec(ctx, b3CLICreateDirFilterAllTable), ShouldBeNil)

	batch, err := conn.PrepareBatch(ctx, b3CLIInsertDirFilterAll)
	So(err, ShouldBeNil)

	for _, fixture := range b3CLIProjectTreeFixtures() {
		for _, child := range b3CLIProjectChildrenForAge(fixture.age) {
			So(batch.Append(
				b3CLIProjectMount,
				snapshotID,
				uint8(child.age),
				child.gid,
				child.uid,
				uint16(child.ft),
				b3CLIProjectDir,
				b3CLIProjectMount,
				child.count,
				child.size,
				int64(100),
				int64(200),
				[]uint64{child.count, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, child.count, 0, 0, 0, 0, 0, 0, 0},
				fixture.childCount,
				uint64(len(b3CLIProjectAllChildren())),
				uint8(1),
				uint8(1),
				time.Now(),
			), ShouldBeNil)
		}
	}

	for _, child := range b3CLIProjectAllChildren() {
		So(batch.Append(
			b3CLIProjectMount,
			snapshotID,
			uint8(child.age),
			child.gid,
			child.uid,
			uint16(child.ft),
			child.dir,
			b3CLIProjectDir,
			child.count,
			child.size,
			int64(100),
			int64(200),
			[]uint64{child.count, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, child.count, 0, 0, 0, 0, 0, 0, 0},
			uint64(0),
			uint64(0),
			uint8(0),
			uint8(0),
			time.Now(),
		), ShouldBeNil)
	}

	So(batch.Send(), ShouldBeNil)
}

func b3CLISeedNameCaches(s *server.Server) {
	s.SetCachedGroupName(7, "group7")
	s.SetCachedGroupName(8, "group8")
	s.SetCachedGroupName(9, "group9")
	s.SetCachedUserName(11, "user11")
	s.SetCachedUserName(12, "user12")
	s.SetCachedUserName(13, "user13")
}

func b3CLIRefuseNonLocalhostDSN(t *testing.T, dsn string) {
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

func b3CLIPickFreePort(t *testing.T) int {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to pick free port: %v", err)
	}

	defer func() { _ = listener.Close() }()

	_, portStr, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to parse listener addr %q: %v", listener.Addr().String(), err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse port %q: %v", portStr, err)
	}

	return port
}
