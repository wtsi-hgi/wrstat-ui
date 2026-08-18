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
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const concurrentClickHouseServerCount = 2

type clickHouseStartResult struct {
	server *LocalClickHouseServer
	err    error
}

func TestLocalClickHouseServerConcurrentLaunches(t *testing.T) {
	Convey("concurrent local servers use distinct loopback ports and scoped directories", t, func() {
		binPath, err := exec.LookPath("clickhouse")
		if err != nil {
			t.Skip("clickhouse binary not found")
		}

		parentDir := t.TempDir()
		startCh := make(chan struct{})
		resultCh := make(chan clickHouseStartResult, concurrentClickHouseServerCount)

		for range concurrentClickHouseServerCount {
			go func() {
				<-startCh

				server, startErr := StartLocalClickHouseServer(binPath, parentDir)
				resultCh <- clickHouseStartResult{server: server, err: startErr}
			}()
		}

		close(startCh)

		results := make([]clickHouseStartResult, 0, concurrentClickHouseServerCount)

		servers := make([]*LocalClickHouseServer, 0, concurrentClickHouseServerCount)
		for range concurrentClickHouseServerCount {
			result := <-resultCh
			results = append(results, result)

			if result.server != nil {
				servers = append(servers, result.server)
			}
		}

		defer stopAndRemoveLocalClickHouseServers(servers)

		for _, result := range results {
			So(result.err, ShouldBeNil)
			So(result.server, ShouldNotBeNil)
		}

		So(servers, ShouldHaveLength, concurrentClickHouseServerCount)

		if len(servers) != concurrentClickHouseServerCount {
			return
		}

		ports := make(map[int]struct{}, concurrentClickHouseServerCount*2)
		dirs := make(map[string]struct{}, concurrentClickHouseServerCount)
		nonLoopbackIP := localNonLoopbackIPv4(t)

		for _, server := range servers {
			ports[server.TCPPort] = struct{}{}
			ports[server.HTTPPort] = struct{}{}
			dirs[server.BaseDir] = struct{}{}

			relDir, relErr := filepath.Rel(parentDir, server.BaseDir)
			So(relErr, ShouldBeNil)
			So(relDir, ShouldNotStartWith, "..")
			So(localPortAcceptsConnections("127.0.0.1", server.TCPPort), ShouldBeTrue)
			So(localPortAcceptsConnections("127.0.0.1", server.HTTPPort), ShouldBeTrue)
			So(localPortAcceptsConnections(nonLoopbackIP, server.TCPPort), ShouldBeFalse)
			So(localPortAcceptsConnections(nonLoopbackIP, server.HTTPPort), ShouldBeFalse)
		}

		So(ports, ShouldHaveLength, concurrentClickHouseServerCount*2)
		So(dirs, ShouldHaveLength, concurrentClickHouseServerCount)

		entries, err := os.ReadDir(parentDir)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, concurrentClickHouseServerCount)
	})
}

func stopAndRemoveLocalClickHouseServers(servers []*LocalClickHouseServer) {
	for _, server := range servers {
		server.Stop()
		_ = os.RemoveAll(server.BaseDir)
	}
}

func localNonLoopbackIPv4(t *testing.T) string {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("failed to list local network addresses: %v", err)
	}

	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && ip.To4() != nil && !ip.IsLoopback() {
			return ip.String()
		}
	}

	t.Fatal("no local non-loopback IPv4 address available")

	return ""
}

func localPortAcceptsConnections(host string, port int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	dialer := net.Dialer{Timeout: 200 * time.Millisecond}

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

func TestLocalClickHouseServerStop(t *testing.T) {
	Convey("Stop waits for a successful server to exit and leaves its directory removable", t, func() {
		binPath, err := exec.LookPath("clickhouse")
		if err != nil {
			t.Skip("clickhouse binary not found")
		}

		server, err := StartLocalClickHouseServer(binPath, t.TempDir())
		if server != nil {
			defer server.Stop()
		}

		So(err, ShouldBeNil)
		So(server, ShouldNotBeNil)

		if server == nil {
			return
		}

		So(localPortAcceptsConnections("127.0.0.1", server.TCPPort), ShouldBeTrue)

		server.Stop()
		server.Stop()

		So(server.processDone(), ShouldBeTrue)
		So(server.cmd.ProcessState, ShouldNotBeNil)
		So(server.cmd.ProcessState.Exited(), ShouldBeTrue)
		So(localPortAcceptsConnections("127.0.0.1", server.TCPPort), ShouldBeFalse)
		So(server.cmd.Process.Signal(os.Interrupt), ShouldWrap, os.ErrProcessDone)

		_, err = os.Stat(server.StdoutPath)
		So(err, ShouldBeNil)
		_, err = os.Stat(server.StderrPath)
		So(err, ShouldBeNil)
		So(os.RemoveAll(server.BaseDir), ShouldBeNil)
		_, err = os.Stat(server.BaseDir)
		So(err, ShouldWrap, os.ErrNotExist)
	})
}

func TestLocalClickHouseServerRetriesBindCollision(t *testing.T) {
	Convey("a local bind collision retries with fresh ports and a clean directory", t, func() {
		binPath, err := exec.LookPath("clickhouse")
		if err != nil {
			t.Skip("clickhouse binary not found")
		}

		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")

		So(err, ShouldBeNil)
		defer func() { So(listener.Close(), ShouldBeNil) }()

		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		So(ok, ShouldBeTrue)

		occupiedPort := tcpAddr.Port
		calls := 0
		pickPort := func() (int, error) {
			calls++
			if calls == 1 {
				return occupiedPort, nil
			}

			return pickLocalPort()
		}

		parentDir := t.TempDir()
		server, err := startLocalClickHouseServer(binPath, parentDir, pickPort)
		So(err, ShouldBeNil)

		defer server.Stop()

		So(server.TCPPort, ShouldNotEqual, occupiedPort)
		So(calls, ShouldEqual, 4)

		entries, err := os.ReadDir(parentDir)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 1)
	})

	Convey("a non-bind exit 210 fails fast", t, func() {
		binPath := writeClickHouseExitScript(t, "configuration error", 210)
		calls := 0
		pickPort := func() (int, error) {
			calls++

			return pickLocalPort()
		}

		parentDir := t.TempDir()
		server, err := startLocalClickHouseServer(binPath, parentDir, pickPort)
		So(server, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(calls, ShouldEqual, 2)

		entries, err := os.ReadDir(parentDir)
		So(err, ShouldBeNil)
		So(entries, ShouldBeEmpty)
	})

	Convey("persistent local bind collisions stop after the bounded attempts", t, func() {
		stderr := "Code: 210. Listen [127.0.0.1]:123 failed: Address already in use"
		binPath := writeClickHouseExitScript(t, stderr, 210)
		calls := 0
		pickPort := func() (int, error) {
			calls++

			return pickLocalPort()
		}

		parentDir := t.TempDir()
		server, err := startLocalClickHouseServer(binPath, parentDir, pickPort)
		So(server, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "persisted after 3 attempts")
		So(calls, ShouldEqual, 6)

		entries, err := os.ReadDir(parentDir)
		So(err, ShouldBeNil)
		So(entries, ShouldBeEmpty)
	})
}

func writeClickHouseExitScript(t *testing.T, stderr string, exitCode int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "clickhouse")

	contents := "#!/bin/sh\nprintf '%s\\n' '" + strings.ReplaceAll(stderr, "'", "'\\''") + "' >&2\nexit " +
		fmt.Sprintf("%d\n", exitCode)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write fake clickhouse executable: %v", err)
	}

	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatalf("failed to make fake clickhouse executable runnable: %v", err)
	}

	return path
}
