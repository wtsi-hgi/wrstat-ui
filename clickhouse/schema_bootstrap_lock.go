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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sys/unix"
)

const (
	schemaBootstrapLockDirName    = "wrstat-ui"
	schemaBootstrapLockFilePrefix = "clickhouse-schema-bootstrap-"
	schemaBootstrapLockRetryDelay = 10 * time.Millisecond
	schemaBootstrapLockFileMode   = 0o600
	schemaBootstrapLockDirMode    = 0o700
)

type schemaBootstrapLock struct {
	file *os.File
	held bool
}

func newSchemaBootstrapLock(opts *ch.Options, database string) (*schemaBootstrapLock, error) {
	path, err := schemaBootstrapLockPath(opts, database)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, schemaBootstrapLockFileMode)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to open schema bootstrap lock %q: %w", path, err)
	}

	return &schemaBootstrapLock{file: file}, nil
}

func (l *schemaBootstrapLock) acquire(ctx context.Context) error {
	if l == nil || l.file == nil {
		return nil
	}

	ticker := time.NewTicker(schemaBootstrapLockRetryDelay)
	defer ticker.Stop()

	for {
		err := l.tryAcquire()
		switch {
		case err == nil:
			return nil
		case !schemaBootstrapLockUnavailable(err):
			return fmt.Errorf("clickhouse: failed to acquire schema bootstrap lock: %w", err)
		}

		if waitErr := waitForSchemaBootstrapRetry(ctx, ticker.C); waitErr != nil {
			return waitErr
		}
	}
}

func schemaBootstrapLockUnavailable(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}

func waitForSchemaBootstrapRetry(ctx context.Context, ticks <-chan time.Time) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("clickhouse: timed out waiting for schema bootstrap lock: %w", ctx.Err())
	case <-ticks:
		return nil
	}
}

func (l *schemaBootstrapLock) tryAcquire() error {
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		l.held = true
	}

	return err
}

func (l *schemaBootstrapLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}

	var err error

	if l.held {
		if unlockErr := unix.Flock(int(l.file.Fd()), unix.LOCK_UN); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("clickhouse: failed to unlock schema bootstrap lock: %w", unlockErr))
		}
	}

	if closeErr := l.file.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("clickhouse: failed to close schema bootstrap lock: %w", closeErr))
	}

	l.file = nil
	l.held = false

	return err
}

func ensureSchemaWithBootstrapLock(
	ctx context.Context,
	execer ch.Conn,
	opts *ch.Options,
	database string,
	queryTO time.Duration,
) (err error) {
	lock, err := newSchemaBootstrapLock(opts, database)
	if err != nil {
		return err
	}

	defer func() {
		releaseErr := lock.release()
		if err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()

	if err = lock.acquire(ctx); err != nil {
		return err
	}

	queryCtx, cancel := queryContext(ctx, queryTO)
	defer cancel()

	return ensureSchema(queryCtx, execer)
}

func schemaBootstrapLockPath(opts *ch.Options, database string) (string, error) {
	dir, err := schemaBootstrapLockDir()
	if err != nil {
		return "", err
	}

	key := schemaBootstrapLockKey(opts, database)

	return filepath.Join(dir, schemaBootstrapLockFilePrefix+key+".lock"), nil
}

func schemaBootstrapLockDir() (string, error) {
	baseDir, err := os.UserCacheDir()
	if err != nil || baseDir == "" {
		baseDir = os.TempDir()
	}

	dir := filepath.Join(baseDir, schemaBootstrapLockDirName)
	if mkErr := os.MkdirAll(dir, schemaBootstrapLockDirMode); mkErr != nil {
		return "", fmt.Errorf("clickhouse: failed to create schema bootstrap lock dir %q: %w", dir, mkErr)
	}

	return dir, nil
}

func schemaBootstrapLockKey(opts *ch.Options, database string) string {
	addrs := append([]string(nil), opts.Addr...)
	sort.Strings(addrs)

	sum := sha256.Sum256([]byte(strings.Join(addrs, ",") + "|" + database))

	return hex.EncodeToString(sum[:])
}
