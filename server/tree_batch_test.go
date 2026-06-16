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

package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

const (
	treeBatchRoot       = "/root"
	treeBatchRootChildA = "/root/a"
	treeBatchRootChildB = "/root/b"

	c3RESTLustre           = "/lustre"
	c3RESTLustreRoot       = "/lustre/"
	c3RESTLustreScratch120 = "/lustre/scratch120"
	c3RESTLustreScratch127 = "/lustre/scratch127"
	c3RESTNFS              = "/nfs"
	c3RESTNFSRoot          = "/nfs/"
	c3RESTNFST283Imaging   = "/nfs/t283_imaging"
	c3RESTNFSTeamArchive   = "/nfs/team_archive"
	c3RESTWideParent       = "/nfs/t283_imaging/wide"

	c3RESTAuthRepeat = 5

	c3RESTInputChildCount        = "child_count"
	c3RESTInputColdProviderState = "cold_provider_state"
	c3RESTInputEndpoint          = "endpoint"
	c3RESTInputGzipBytes         = "gzip_bytes"
	c3RESTInputJSONBytes         = "json_bytes"
	c3RESTInputPath              = "path"
	c3RESTInputQueryCount        = "query_count"
	c3RESTInputQueryCountSource  = "query_count_source"
	c3RESTInputRoute             = "route"
	c3RESTInputStatusCodes       = "status_codes"
	c3RESTRouteCatalog           = "wrstat_dirs"
)

const (
	c3RESTNFSDirID            = 1
	c3RESTMountDirID          = 2
	c3RESTWideParentDirID     = 3
	c3RESTFirstChildDirID     = 4
	c3RESTHighFanoutLeafDirID = 5
)

type batchHasChildrenTestDB struct {
	hasChildren           map[string]bool
	childrenCalls         int
	dirsHaveChildrenCalls [][]string
}

func (d *batchHasChildrenTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	return &db.DirSummary{Dir: dir, Count: 1}, nil
}

func (d *batchHasChildrenTestDB) Children(string) ([]string, error) {
	d.childrenCalls++

	return []string{}, nil
}

func (d *batchHasChildrenTestDB) DirsHaveChildren(dirs []string, _ *db.Filter) (map[string]bool, error) {
	d.dirsHaveChildrenCalls = append(d.dirsHaveChildrenCalls, dirs)

	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = d.hasChildren[dir]
	}

	return result, nil
}

func (d *batchHasChildrenTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *batchHasChildrenTestDB) Close() error {
	return nil
}

func TestTreeElementUsesBatchedChildExistence(t *testing.T) {
	Convey("DiskTree conversion batches child has_children checks", t, func() {
		database := &batchHasChildrenTestDB{
			hasChildren: map[string]bool{
				treeBatchRootChildA: true,
				treeBatchRootChildB: false,
			},
		}

		s := New(io.Discard)
		s.tree = db.NewTree(database)

		element := s.diToTreeElement(&db.DirInfo{
			Current: &db.DirSummary{Dir: treeBatchRoot, Count: 3},
			Children: []*db.DirSummary{
				{Dir: treeBatchRootChildA, Count: 2},
				{Dir: treeBatchRootChildB, Count: 1},
			},
		}, nil, nil, treeBatchRoot)

		So(element.Children, ShouldHaveLength, 2)
		So(element.Children[0].HasChildren, ShouldBeTrue)
		So(element.Children[1].HasChildren, ShouldBeFalse)
		So(database.dirsHaveChildrenCalls, ShouldResemble, [][]string{{
			treeBatchRootChildA, treeBatchRootChildB,
		}})
		So(database.childrenCalls, ShouldBeZeroValue)
	})
}

func seedC3RESTClickHouseHighFanout(
	t *testing.T,
	cfg clickhouse.Config,
	parentDir string,
	childCount int,
) {
	t.Helper()

	paths := internaltest.NewDirectoryPathCreator()
	mountPath := c3RESTNFST283Imaging + "/"

	w, err := clickhouse.NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(mountPath)
	w.SetUpdatedAt(time.Date(2026, 6, 7, 18, 30, 0, 0, time.UTC))

	snapshotEnd := c3RESTHighFanoutSnapshotEnd(childCount)

	mountDir := paths.ToDirectoryPath(mountPath)
	So(w.Add(db.RecordDGUTA{
		Dir:        mountDir,
		DirID:      c3RESTMountDirID,
		ParentID:   c3RESTNFSDirID,
		SubtreeEnd: snapshotEnd,
		Depth:      schema3FixtureDepth(t, mountDir.Depth),
		Children:   []string{"wide/"},
		GUTAs: db.GUTAs{
			c3RESTGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	children := make([]string, childCount)
	for i := range childCount {
		children[i] = fmt.Sprintf("child%03d/", i)
	}

	parentPath := paths.ToDirectoryPath(parentDir)
	So(w.Add(db.RecordDGUTA{
		Dir:        parentPath,
		DirID:      c3RESTWideParentDirID,
		ParentID:   c3RESTMountDirID,
		SubtreeEnd: snapshotEnd,
		Depth:      schema3FixtureDepth(t, parentPath.Depth),
		ChildCount: c3RESTNonNegativeIntToUint64(t, childCount),
		Children:   children,
		GUTAs: db.GUTAs{
			c3RESTGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	for i, child := range children {
		dir := parentDir + child
		childPath := paths.ToDirectoryPath(dir)

		record := db.RecordDGUTA{
			Dir:        childPath,
			DirID:      c3RESTHighFanoutChildDirID(i),
			ParentID:   c3RESTWideParentDirID,
			SubtreeEnd: c3RESTHighFanoutChildSubtreeEnd(i),
			Depth:      schema3FixtureDepth(t, childPath.Depth),
			GUTAs: db.GUTAs{
				c3RESTGUTA(
					uint32(7+i%3),
					uint32(11+i%5),
					db.DGUTAFileTypeBam,
					db.DGUTAgeAll,
					2,
					uint64(20+i),
				),
				c3RESTGUTA(
					uint32(7+i%3),
					uint32(11+i%5),
					db.DGUTAFileTypeBam,
					db.DGUTAgeA6M,
					1,
					uint64(10+i),
				),
			},
		}

		if i == 0 {
			record.ChildCount = 1
			record.Children = []string{"leaf/"}
		}

		So(w.Add(record), ShouldBeNil)
	}

	leafPath := paths.ToDirectoryPath(parentDir + "child000/leaf/")
	So(w.Add(db.RecordDGUTA{
		Dir:        leafPath,
		DirID:      c3RESTHighFanoutLeafDirID,
		ParentID:   c3RESTHighFanoutChildDirID(0),
		SubtreeEnd: c3RESTHighFanoutLeafDirID + 1,
		Depth:      schema3FixtureDepth(t, leafPath.Depth),
		GUTAs: db.GUTAs{
			c3RESTGUTA(7, 11, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 10),
		},
	}), ShouldBeNil)
	So(w.Close(), ShouldBeNil)
}

func c3RESTHighFanoutSnapshotEnd(childCount int) uint32 {
	return uint32(childCount + int(c3RESTHighFanoutLeafDirID) + 1) //nolint:gosec // Fixture child counts are bounded.
}

func schema3FixtureDepth(t *testing.T, depth int) uint16 {
	t.Helper()

	So(depth, ShouldBeGreaterThanOrEqualTo, 0)
	So(depth, ShouldBeLessThanOrEqualTo, int(^uint16(0)))

	return uint16(depth) //nolint:gosec // Fixture depth range is checked above.
}

func c3RESTGUTA(
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

func c3RESTNonNegativeIntToUint64(t *testing.T, value int) uint64 {
	t.Helper()

	converted, err := strconv.ParseUint(strconv.Itoa(value), 10, 64)
	So(err, ShouldBeNil)

	return converted
}

func c3RESTHighFanoutChildDirID(i int) uint32 {
	if i == 0 {
		return c3RESTFirstChildDirID
	}

	return uint32(i + int(c3RESTHighFanoutLeafDirID) + 1) //nolint:gosec // Fixture child counts are bounded.
}

func c3RESTHighFanoutChildSubtreeEnd(i int) uint32 {
	if i == 0 {
		return c3RESTHighFanoutLeafDirID + 1
	}

	return c3RESTHighFanoutChildDirID(i) + 1
}

func openC3RESTClickHouseConn(t *testing.T, dsn string) ch.Conn {
	t.Helper()
	c3RESTRefuseNonLocalhostDSN(t, dsn)

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

func c3RESTRefuseNonLocalhostDSN(t *testing.T, dsn string) {
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

func c3RESTPickFreePort(t *testing.T) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
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
		t.Fatalf("failed to parse listener port %q: %v", portStr, err)
	}

	return port
}

func startC3RESTClickHouseServer(
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

	crtPath, keyPath := writeC3RESTSelfSignedTLSCertPair(t, baseDir)
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

		if c3RESTStopClickHouseProcess(t, cmd, doneCh) {
			return
		}
	})

	return doneCh, exitErr, stdoutPath, stderrPath
}

func writeC3RESTSelfSignedTLSCertPair(t *testing.T, dir string) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate TLS key: %v", err)
	}

	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Hour),
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

func c3RESTStopClickHouseProcess(t *testing.T, cmd *exec.Cmd, doneCh <-chan struct{}) bool {
	t.Helper()

	done := make(chan struct{})

	go func() {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Logf("failed to signal clickhouse server: %v", err)
		}

		close(done)
	}()

	select {
	case <-doneCh:
		return true
	case <-time.After(5 * time.Second):
		if err := cmd.Process.Kill(); err != nil {
			t.Logf("failed to kill clickhouse server: %v", err)
		}

		<-doneCh
		<-done

		return false
	}
}

func addC3RESTAuthTreeOperation(
	t *testing.T,
	report *perfreport.Report,
	s *Server,
	addr string,
	cert string,
	token string,
	cfg clickhouse.Config,
	parentDir string,
	route string,
) {
	t.Helper()

	conn := openC3RESTClickHouseConn(t, cfg.DSN)
	defer func() { _ = conn.Close() }()

	durations := make([]float64, 0, c3RESTAuthRepeat)
	queryCounts := make([]uint64, 0, c3RESTAuthRepeat)
	jsonBytes := make([]uint64, 0, c3RESTAuthRepeat)
	gzipBytes := make([]uint64, 0, c3RESTAuthRepeat)
	statusCodes := make([]uint64, 0, c3RESTAuthRepeat)
	resultCounts := make([]uint64, 0, c3RESTAuthRepeat)

	for range c3RESTAuthRepeat {
		clickhouse.ResetTreeQueryCaches()

		provider, err := clickhouse.OpenProvider(cfg)
		So(err, ShouldBeNil)
		So(s.SetProvider(provider), ShouldBeNil)

		queryCountBefore := c3RESTQueryEventCount(t, conn)
		start := time.Now()
		status, got, body := requestC3RESTAuthTree(t, addr, cert, token, parentDir)
		elapsed := time.Since(start)

		gzipData, err := compressGzip(body)
		So(err, ShouldBeNil)

		durations = append(durations, float64(elapsed.Microseconds())/1000)
		queryCounts = append(queryCounts, c3RESTQueryEventDelta(t, conn, queryCountBefore))
		jsonBytes = append(jsonBytes, uint64(len(body)))
		gzipBytes = append(gzipBytes, uint64(len(gzipData)))
		statusCodes = append(statusCodes, c3RESTNonNegativeIntToUint64(t, status))
		resultCounts = append(resultCounts, uint64(len(got.Children)))
	}

	report.AddOperationWithCounters(
		"auth_rest_tree_"+route,
		map[string]any{
			c3RESTInputEndpoint:          EndPointAuthTree,
			c3RESTInputPath:              parentDir,
			c3RESTInputRoute:             route,
			c3RESTInputColdProviderState: true,
			c3RESTInputQueryCount:        queryCounts,
			c3RESTInputQueryCountSource:  "system.events.Query_delta",
			c3RESTInputJSONBytes:         jsonBytes,
			c3RESTInputGzipBytes:         gzipBytes,
			c3RESTInputStatusCodes:       statusCodes,
			c3RESTInputChildCount:        uint64(305),
		},
		durations,
		nil,
		nil,
		nil,
		resultCounts,
	)
}

func c3RESTQueryEventCount(t *testing.T, conn ch.Conn) uint64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := conn.QueryRow(ctx, "SELECT toUInt64(sum(value)) FROM system.events WHERE event = 'Query'")

	var count uint64
	So(row.Scan(&count), ShouldBeNil)

	return count
}

func requestC3RESTAuthTree(
	t *testing.T,
	addr string,
	cert string,
	token string,
	path string,
) (int, TreeElement, []byte) {
	t.Helper()

	request := gas.NewAuthenticatedClientRequest(addr, cert, token)
	response, err := request.SetResult(&TreeElement{}).
		ForceContentType("application/json").
		SetQueryParams(map[string]string{c3RESTInputPath: path}).
		Get(EndPointAuthTree)
	So(err, ShouldBeNil)
	So(response.StatusCode(), ShouldEqual, http.StatusOK)

	result, ok := response.Result().(*TreeElement)
	So(ok, ShouldBeTrue)
	So(result, ShouldNotBeNil)

	return response.StatusCode(), *result, response.Body()
}

func c3RESTQueryEventDelta(t *testing.T, conn ch.Conn, before uint64) uint64 {
	t.Helper()

	after := c3RESTQueryEventCount(t, conn)
	if after <= before {
		return 0
	}

	delta := after - before
	if delta > 0 {
		delta--
	}

	return delta
}

type c3RESTTreeTestDB struct {
	summaries             map[string]*db.DirSummary
	children              map[string][]string
	hasChildren           map[string]bool
	dirsHaveChildrenCalls [][]string
	queryCalls            int
}

func newC3RESTTreeTestDB() *c3RESTTreeTestDB {
	children := map[string][]string{
		"/":              {c3RESTLustre, c3RESTNFS},
		c3RESTLustreRoot: {c3RESTLustreScratch120, c3RESTLustreScratch127},
		c3RESTNFSRoot:    {c3RESTNFST283Imaging, c3RESTNFSTeamArchive},
	}

	summaries := make(map[string]*db.DirSummary)
	for dir, count := range map[string]uint64{
		"/":                    30,
		c3RESTLustre:           20,
		c3RESTLustreRoot:       20,
		c3RESTLustreScratch120: 12,
		c3RESTLustreScratch127: 8,
		c3RESTNFS:              10,
		c3RESTNFSRoot:          10,
		c3RESTNFST283Imaging:   7,
		c3RESTNFSTeamArchive:   3,
	} {
		summaries[dir] = c3RESTDirSummary(dir, count)
	}

	return &c3RESTTreeTestDB{
		summaries: summaries,
		children:  children,
		hasChildren: map[string]bool{
			c3RESTLustre:           true,
			c3RESTNFS:              true,
			c3RESTLustreScratch120: false,
			c3RESTLustreScratch127: false,
			c3RESTNFST283Imaging:   false,
			c3RESTNFSTeamArchive:   false,
		},
	}
}

func (d *c3RESTTreeTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	d.queryCalls++

	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	return cloneC3RESTDirSummary(summary), nil
}

func cloneC3RESTDirSummary(summary *db.DirSummary) *db.DirSummary {
	clone := *summary
	clone.GIDs = append([]uint32(nil), summary.GIDs...)
	clone.UIDs = append([]uint32(nil), summary.UIDs...)

	return &clone
}

func (d *c3RESTTreeTestDB) DirInfos(dirs []string, _ *db.Filter) (map[string]*db.DirSummary, error) {
	d.queryCalls++

	summaries := make(map[string]*db.DirSummary, len(dirs))
	for _, dir := range dirs {
		summary := d.summaries[dir]
		if summary != nil {
			summaries[dir] = cloneC3RESTDirSummary(summary)
		}
	}

	return summaries, nil
}

func (d *c3RESTTreeTestDB) Children(dir string) ([]string, error) {
	d.queryCalls++

	return append([]string(nil), d.children[dir]...), nil
}

func (d *c3RESTTreeTestDB) DirsHaveChildren(dirs []string, _ *db.Filter) (map[string]bool, error) {
	d.queryCalls++
	d.dirsHaveChildrenCalls = append(d.dirsHaveChildrenCalls, append([]string(nil), dirs...))

	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = d.hasChildren[dir]
	}

	return result, nil
}

func (d *c3RESTTreeTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *c3RESTTreeTestDB) Close() error {
	return nil
}

func (d *c3RESTTreeTestDB) queryCount() int {
	return d.queryCalls
}

func TestTreeRESTVirtualAncestorRoutesC3(t *testing.T) {
	Convey("REST tree requests expose root and virtual namespace mount roots", t, func() {
		gin.SetMode(gin.TestMode)

		database := newC3RESTTreeTestDB()
		s := New(io.Discard)
		s.tree = db.NewTree(database)

		for requestPath, expectedChildren := range map[string][]string{
			"/":              {c3RESTLustre, c3RESTNFS},
			c3RESTLustreRoot: {c3RESTLustreScratch120, c3RESTLustreScratch127},
			c3RESTNFSRoot:    {c3RESTNFST283Imaging, c3RESTNFSTeamArchive},
		} {
			got := requestC3RESTTree(t, s, requestPath)
			So(got.Path, ShouldEqual, requestPath)
			So(treeElementChildPaths(got.Children), ShouldResemble, expectedChildren)
		}
	})

	Convey("REST auth tree returns the full 305-child high-fanout payload", t, func() {
		gin.SetMode(gin.TestMode)

		database := newC3RESTTreeTestDB()
		seedC3RESTHighFanoutParent(database, c3RESTWideParent, 305)

		s := New(io.Discard)
		s.tree = db.NewTree(database)

		got, jsonBytes, gzipBytes := requestC3RESTTreeWithSizes(t, s, c3RESTWideParent)

		So(got.Path, ShouldEqual, c3RESTWideParent)
		So(got.Children, ShouldHaveLength, 305)
		So(got.Children[0].Path, ShouldEqual, c3RESTWideParent+"/child000")
		So(got.Children[304].Path, ShouldEqual, c3RESTWideParent+"/child304")
		So(database.queryCount(), ShouldEqual, 4)
		So(database.dirsHaveChildrenCalls, ShouldHaveLength, 1)
		So(database.dirsHaveChildrenCalls[0], ShouldHaveLength, 305)
		So(jsonBytes, ShouldBeGreaterThan, 0)
		So(gzipBytes, ShouldBeGreaterThan, 0)
		So(gzipBytes, ShouldBeLessThan, jsonBytes)
	})

	Convey("C3.5 real cold-provider auth REST tree report records catalog-route evidence", t, func() {
		os.Setenv("WRSTAT_ENV", "test")
		Reset(func() { os.Unsetenv("WRSTAT_ENV") })
		clickhouse.ResetTreeQueryCaches()
		Reset(clickhouse.ResetTreeQueryCaches)

		harness := newC3RESTClickHouseHarness(t)
		cfg := harness.newConfig()

		cfg.QueryTimeout = 5 * time.Second
		cfg.PollInterval = 0
		cfg.MountPoints = []string{c3RESTNFST283Imaging + "/"}

		parentDir := c3RESTWideParent + "/"
		seedC3RESTClickHouseHighFanout(t, cfg, parentDir, 305)

		cert, key, err := gas.CreateTestCert(t)
		So(err, ShouldBeNil)

		report := measureC3RESTAuthTreeReport(t, cfg, cert, key, parentDir)
		catalogOp := c3RESTReportOperation(report, "auth_rest_tree_"+c3RESTRouteCatalog)

		So(catalogOp, ShouldNotBeNil)
		assertC3RESTAuthTreeOperation(catalogOp, c3RESTRouteCatalog, parentDir)
		So(catalogOp.ResultCount, ShouldResemble, []uint64{305, 305, 305, 305, 305})
	})
}

func requestC3RESTTree(t *testing.T, s *Server, path string) TreeElement {
	t.Helper()

	got, _, _ := requestC3RESTTreeWithSizes(t, s, path)

	return got
}

func requestC3RESTTreeWithSizes(t *testing.T, s *Server, path string) (TreeElement, int, int) {
	t.Helper()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		EndPointAuthTree+"?"+c3RESTInputPath+"="+url.QueryEscape(path),
		nil,
	)

	s.getTree(c)
	So(w.Code, ShouldEqual, http.StatusOK)

	var got TreeElement
	So(json.Unmarshal(w.Body.Bytes(), &got), ShouldBeNil)

	gzipData, err := compressGzip(w.Body.Bytes())
	So(err, ShouldBeNil)

	return got, w.Body.Len(), len(gzipData)
}

func treeElementChildPaths(children []*TreeElement) []string {
	paths := make([]string, len(children))
	for i, child := range children {
		paths[i] = child.Path
	}

	return paths
}

func newC3RESTClickHouseHarness(t *testing.T) *c3RESTClickHouseHarness {
	t.Helper()

	envDSN := os.Getenv("WRSTAT_CLICKHOUSE_DSN")
	if envDSN != "" {
		c3RESTRefuseNonLocalhostDSN(t, envDSN)

		return &c3RESTClickHouseHarness{t: t}
	}

	bin, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("clickhouse binary not found")
	}

	baseDir := t.TempDir()
	tcpPort := c3RESTPickFreePort(t)

	httpPort := c3RESTPickFreePort(t)

	for httpPort == tcpPort {
		httpPort = c3RESTPickFreePort(t)
	}

	doneCh, exitErr, stdoutPath, stderrPath := startC3RESTClickHouseServer(
		t, bin, baseDir, tcpPort, httpPort,
	)
	h := &c3RESTClickHouseHarness{
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

func measureC3RESTAuthTreeReport(
	t *testing.T,
	cfg clickhouse.Config,
	cert string,
	key string,
	parentDir string,
) perfreport.Report {
	t.Helper()

	s := New(io.Discard)
	So(s.EnableAuth(cert, key, func(_, _ string) (bool, string) {
		return true, "0"
	}), ShouldBeNil)
	s.userToGIDs["user"] = nil
	So(s.AddTreePage(), ShouldBeNil)

	addr, stop, err := gas.StartTestServer(s, cert, key)
	So(err, ShouldBeNil)

	defer func() { So(stop(), ShouldBeNil) }()

	token, err := gas.Login(gas.NewClientRequest(addr, cert), "user", "pass")
	So(err, ShouldBeNil)

	report := perfreport.NewReport("clickhouse", "", c3RESTAuthRepeat, 0)
	addC3RESTAuthTreeOperation(t, &report, s, addr, cert, token, cfg, parentDir, c3RESTRouteCatalog)

	return report
}

func c3RESTReportOperation(report perfreport.Report, name string) *perfreport.Operation {
	for i := range report.Operations {
		if report.Operations[i].Name == name {
			return &report.Operations[i]
		}
	}

	return nil
}

func assertC3RESTAuthTreeOperation(op *perfreport.Operation, route string, parentDir string) {
	So(op.P95MS, ShouldBeGreaterThan, float64(0))
	So(op.ResultCount, ShouldResemble, []uint64{305, 305, 305, 305, 305})
	So(op.Inputs[c3RESTInputColdProviderState], ShouldBeTrue)
	So(op.Inputs[c3RESTInputEndpoint], ShouldEqual, EndPointAuthTree)
	So(op.Inputs[c3RESTInputPath], ShouldEqual, parentDir)
	So(op.Inputs[c3RESTInputRoute], ShouldEqual, route)
	So(op.Inputs[c3RESTInputChildCount], ShouldEqual, uint64(305))
	So(c3RESTPositiveUintInputs(op, c3RESTInputQueryCount), ShouldBeTrue)
	So(c3RESTPositiveUintInputs(op, c3RESTInputJSONBytes), ShouldBeTrue)
	So(c3RESTPositiveUintInputs(op, c3RESTInputGzipBytes), ShouldBeTrue)
	So(c3RESTGzipBytesAreSmaller(op), ShouldBeTrue)
	So(c3RESTStatusInputs(op), ShouldResemble, []uint64{200, 200, 200, 200, 200})
}

func c3RESTPositiveUintInputs(op *perfreport.Operation, key string) bool {
	values := c3RESTUintInputs(op, key)
	if len(values) != c3RESTAuthRepeat {
		return false
	}

	for _, value := range values {
		if value == 0 {
			return false
		}
	}

	return true
}

func c3RESTUintInputs(op *perfreport.Operation, key string) []uint64 {
	values, ok := op.Inputs[key].([]uint64)
	if !ok {
		return nil
	}

	return values
}

func c3RESTGzipBytesAreSmaller(op *perfreport.Operation) bool {
	jsonBytes, ok := op.Inputs[c3RESTInputJSONBytes].([]uint64)
	if !ok {
		return false
	}

	gzipBytes, ok := op.Inputs[c3RESTInputGzipBytes].([]uint64)
	if !ok || len(gzipBytes) != len(jsonBytes) {
		return false
	}

	for i, jsonByteCount := range jsonBytes {
		if gzipBytes[i] >= jsonByteCount {
			return false
		}
	}

	return true
}

func c3RESTStatusInputs(op *perfreport.Operation) []uint64 {
	values, ok := op.Inputs[c3RESTInputStatusCodes].([]uint64)
	if !ok {
		return nil
	}

	return values
}

func seedC3RESTHighFanoutParent(database *c3RESTTreeTestDB, parentDir string, childCount int) {
	children := make([]string, childCount)

	var childTotal uint64

	for i := range childCount {
		child := fmt.Sprintf("%s/child%03d", parentDir, i)
		children[i] = child
		database.summaries[child] = c3RESTDirSummary(child, 1)
		database.hasChildren[child] = false
		childTotal++
	}

	database.summaries[parentDir] = c3RESTDirSummary(parentDir, childTotal)
	database.children[parentDir] = children
}

func c3RESTDirSummary(dir string, count uint64) *db.DirSummary {
	return &db.DirSummary{
		Dir:     dir,
		Count:   count,
		Size:    count * 10,
		Atime:   time.Unix(10, 0),
		Mtime:   time.Unix(20, 0),
		GIDs:    []uint32{7},
		UIDs:    []uint32{11},
		FT:      db.DGUTAFileTypeBam,
		Modtime: time.Unix(30, 0),
	}
}

type c3RESTClickHouseHarness struct {
	t        *testing.T
	tcpPort  int
	httpPort int
	baseDir  string
	doneCh   <-chan struct{}
	exitErr  *error
	stdout   string
	stderr   string
}

func (h *c3RESTClickHouseHarness) newConfig() clickhouse.Config {
	h.t.Helper()

	database := c3RESTTestDatabaseName(h.t)

	return clickhouse.Config{
		DSN:      h.baseDSN(database),
		Database: database,
	}
}

func c3RESTTestDatabaseName(t *testing.T) string {
	t.Helper()

	rnd := make([]byte, 6)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatalf("failed to generate random database suffix: %v", err)
	}

	return fmt.Sprintf("wrstat_ui_test_%d_%s", os.Getpid(), hex.EncodeToString(rnd))
}

func (h *c3RESTClickHouseHarness) baseDSN(database string) string {
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

func (h *c3RESTClickHouseHarness) waitUntilReady() {
	h.t.Helper()

	conn := openC3RESTClickHouseConn(h.t, h.baseDSN("default"))
	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(30 * time.Second)

	for {
		select {
		case <-h.doneCh:
			h.t.Fatalf(
				"clickhouse server exited early: %v\nstdout:\n%s\nstderr:\n%s",
				*h.exitErr,
				c3RESTReadFileOrEmpty(h.stdout),
				c3RESTReadFileOrEmpty(h.stderr),
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
				c3RESTReadFileOrEmpty(h.stdout),
				c3RESTReadFileOrEmpty(h.stderr),
			)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func c3RESTReadFileOrEmpty(path string) string {
	if path == "" {
		return ""
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(data)
}
