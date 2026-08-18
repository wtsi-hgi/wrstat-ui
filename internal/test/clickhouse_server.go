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

package internaltest

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	clickHouseStartupAttempts = 3
	clickHouseStartupTimeout  = 30 * time.Second
)

var (
	errClickHouseStartupTimeout = errors.New("clickhouse server did not become ready")
	errDistinctClickHousePorts  = errors.New("could not pick distinct clickhouse TCP and HTTP ports")
	errUnexpectedListenerAddr   = errors.New("unexpected loopback listener address")
)

type clickHouseStartupError struct {
	cause  error
	stdout string
	stderr string
}

func (e *clickHouseStartupError) Error() string {
	return fmt.Sprintf("clickhouse server exited early: %v\nstdout:\n%s\nstderr:\n%s", e.cause, e.stdout, e.stderr)
}

func (e *clickHouseStartupError) Unwrap() error {
	return e.cause
}

func isLocalClickHouseBindCollision(err error) bool {
	var startupErr *clickHouseStartupError
	if !errors.As(err, &startupErr) {
		return false
	}

	var exitErr *exec.ExitError
	if !errors.As(startupErr.cause, &exitErr) || exitErr.ExitCode() != 210 {
		return false
	}

	return strings.Contains(startupErr.stderr, "Listen [127.0.0.1]:") &&
		strings.Contains(startupErr.stderr, "Address already in use")
}

// LocalClickHouseServer is an isolated ClickHouse server bound only to loopback.
type LocalClickHouseServer struct {
	TCPPort    int
	HTTPPort   int
	BaseDir    string
	StdoutPath string
	StderrPath string

	cancel  context.CancelFunc
	cmd     *exec.Cmd
	doneCh  <-chan struct{}
	exitErr *error
	stop    sync.Once
}

// StartLocalClickHouseServer starts an isolated loopback-only ClickHouse server.
func StartLocalClickHouseServer(binPath string, parentDir string) (*LocalClickHouseServer, error) {
	return startLocalClickHouseServer(binPath, parentDir, pickLocalPort)
}

func startLocalClickHouseServer(
	binPath string,
	parentDir string,
	pickPort func() (int, error),
) (*LocalClickHouseServer, error) {
	var lastErr error

	for range clickHouseStartupAttempts {
		server, err := startLocalClickHouseAttempt(binPath, parentDir, pickPort)
		if err != nil {
			return nil, err
		}

		err = server.waitUntilReady()
		if err == nil {
			return server, nil
		}

		server.Stop()

		if removeErr := os.RemoveAll(server.BaseDir); removeErr != nil {
			return nil, fmt.Errorf("clean failed clickhouse startup directory: %w", removeErr)
		}

		if !isLocalClickHouseBindCollision(err) {
			return nil, err
		}

		lastErr = err
	}

	return nil, fmt.Errorf("clickhouse local bind collision persisted after %d attempts: %w",
		clickHouseStartupAttempts, lastErr)
}

func startLocalClickHouseAttempt(
	binPath string,
	parentDir string,
	pickPort func() (int, error),
) (*LocalClickHouseServer, error) {
	baseDir, err := os.MkdirTemp(parentDir, "wrstat-ui-clickhouse-test-*")
	if err != nil {
		return nil, fmt.Errorf("create clickhouse startup directory: %w", err)
	}

	server, err := prepareLocalClickHouseAttempt(binPath, baseDir, pickPort)
	if err != nil {
		if removeErr := os.RemoveAll(baseDir); removeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("clean clickhouse startup directory: %w", removeErr))
		}

		return nil, err
	}

	return server, nil
}

func prepareLocalClickHouseAttempt(
	binPath string,
	baseDir string,
	pickPort func() (int, error),
) (*LocalClickHouseServer, error) {
	tcpPort, httpPort, err := pickDistinctLocalPorts(pickPort)
	if err != nil {
		return nil, err
	}

	dataPath := filepath.Join(baseDir, "data")
	if mkdirErr := os.MkdirAll(dataPath, 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("create clickhouse data directory: %w", mkdirErr)
	}

	crtPath, keyPath, err := writeLocalClickHouseCertPair(baseDir)
	if err != nil {
		return nil, err
	}

	return runLocalClickHouse(binPath, baseDir, dataPath, crtPath, keyPath, tcpPort, httpPort)
}

func runLocalClickHouse(
	binPath string,
	baseDir string,
	dataPath string,
	crtPath string,
	keyPath string,
	tcpPort int,
	httpPort int,
) (*LocalClickHouseServer, error) {
	stdoutPath := filepath.Join(baseDir, "clickhouse.stdout.log")
	stderrPath := filepath.Join(baseDir, "clickhouse.stderr.log")

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("create clickhouse stdout log: %w", err)
	}

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		_ = stdoutFile.Close()

		return nil, fmt.Errorf("create clickhouse stderr log: %w", err)
	}

	cmdCtx, cancel := context.WithCancel(context.Background())
	//nolint:gosec // This test helper executes the caller-resolved local ClickHouse binary.
	cmd := exec.CommandContext(cmdCtx, binPath, localClickHouseArgs(dataPath, crtPath, keyPath, tcpPort, httpPort)...)
	cmd.Dir = baseDir

	cmd.Env = append(os.Environ(), "CLICKHOUSE_WATCHDOG_ENABLE=0")
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	if err := cmd.Start(); err != nil {
		cancel()

		_ = stdoutFile.Close()
		_ = stderrFile.Close()

		return nil, fmt.Errorf("start clickhouse: %w", err)
	}

	doneCh := make(chan struct{})

	exitErr := new(error)
	go func() {
		*exitErr = cmd.Wait()
		_ = stdoutFile.Close()
		_ = stderrFile.Close()

		close(doneCh)
	}()

	return &LocalClickHouseServer{
		TCPPort: tcpPort, HTTPPort: httpPort, BaseDir: baseDir,
		StdoutPath: stdoutPath, StderrPath: stderrPath,
		cancel: cancel, cmd: cmd, doneCh: doneCh, exitErr: exitErr,
	}, nil
}

func (s *LocalClickHouseServer) waitUntilReady() error {
	opts, err := ch.ParseDSN(fmt.Sprintf(
		"clickhouse://default@127.0.0.1:%d/default?database=default&dial_timeout=1s&compress=lz4",
		s.TCPPort,
	))
	if err != nil {
		return fmt.Errorf("parse local clickhouse DSN: %w", err)
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return fmt.Errorf("open local clickhouse connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	deadline := time.NewTimer(clickHouseStartupTimeout)
	defer deadline.Stop()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		ready, err := s.ready(conn)
		if err != nil {
			return err
		}

		if ready {
			return nil
		}

		if err := s.waitForReadinessPoll(deadline.C, ticker.C); err != nil {
			return err
		}
	}
}

func (s *LocalClickHouseServer) waitForReadinessPoll(deadline, tick <-chan time.Time) error {
	select {
	case <-s.doneCh:
		return s.earlyExitError()
	case <-deadline:
		return fmt.Errorf("%w within %s", errClickHouseStartupTimeout, clickHouseStartupTimeout)
	case <-tick:
		return nil
	}
}

func (s *LocalClickHouseServer) ready(conn ch.Conn) (bool, error) {
	if !strings.Contains(readLocalClickHouseLog(s.StderrPath), "Ready for connections.") {
		return false, nil
	}

	err := s.pingOrExit(conn)
	if err == nil {
		return true, nil
	}

	var startupErr *clickHouseStartupError
	if errors.As(err, &startupErr) {
		return false, startupErr
	}

	return false, nil
}

func readLocalClickHouseLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(data)
}

func (s *LocalClickHouseServer) pingOrExit(conn ch.Conn) error {
	select {
	case <-s.doneCh:
		return s.earlyExitError()
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return conn.Exec(ctx, "SELECT 1")
}

func (s *LocalClickHouseServer) earlyExitError() error {
	return &clickHouseStartupError{
		cause:  *s.exitErr,
		stdout: readLocalClickHouseLog(s.StdoutPath),
		stderr: readLocalClickHouseLog(s.StderrPath),
	}
}

// Stop terminates the local ClickHouse server and waits for it to exit.
func (s *LocalClickHouseServer) Stop() {
	s.stop.Do(s.stopProcess)
}

func (s *LocalClickHouseServer) stopProcess() {
	defer s.cancel()

	if s.processDone() {
		return
	}

	s.signalProcess(os.Interrupt)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-s.doneCh:
		return
	case <-timer.C:
		s.signalProcess(os.Kill)
		<-s.doneCh
	}
}

func (s *LocalClickHouseServer) processDone() bool {
	select {
	case <-s.doneCh:
		return true
	default:
		return false
	}
}

func (s *LocalClickHouseServer) signalProcess(signal os.Signal) {
	if err := s.cmd.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
		s.cancel()
	}
}

func pickDistinctLocalPorts(pickPort func() (int, error)) (int, int, error) {
	tcpPort, err := pickPort()
	if err != nil {
		return 0, 0, fmt.Errorf("pick clickhouse TCP port: %w", err)
	}

	for range clickHouseStartupAttempts {
		httpPort, err := pickPort()
		if err != nil {
			return 0, 0, fmt.Errorf("pick clickhouse HTTP port: %w", err)
		}

		if httpPort != tcpPort {
			return tcpPort, httpPort, nil
		}
	}

	return 0, 0, errDistinctClickHousePorts
}

func pickLocalPort() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("listen on an ephemeral loopback port: %w", err)
	}

	defer func() { _ = listener.Close() }()

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("%w %q", errUnexpectedListenerAddr, listener.Addr())
	}

	return tcpAddr.Port, nil
}

func localClickHouseArgs(dataPath, crtPath, keyPath string, tcpPort, httpPort int) []string {
	return []string{
		"server", "--", "--listen_host=127.0.0.1",
		"--tcp_port=" + strconv.Itoa(tcpPort), "--tcp_port_secure=0",
		"--http_port=" + strconv.Itoa(httpPort), "--https_port=0",
		"--mysql_port=0", "--postgresql_port=0", "--grpc_port=0",
		"--openSSL.server.certificateFile=" + crtPath,
		"--openSSL.server.privateKeyFile=" + keyPath,
		"--path=" + dataPath + string(os.PathSeparator),
	}
}

func writeLocalClickHouseCertPair(dir string) (string, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", fmt.Errorf("generate clickhouse TLS key: %w", err)
	}

	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return "", "", fmt.Errorf("create clickhouse TLS certificate: %w", err)
	}

	crtPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	if err := os.WriteFile(crtPath, crtPEM, 0o600); err != nil {
		return "", "", fmt.Errorf("write clickhouse TLS certificate: %w", err)
	}

	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return "", "", fmt.Errorf("write clickhouse TLS private key: %w", err)
	}

	return crtPath, keyPath, nil
}
