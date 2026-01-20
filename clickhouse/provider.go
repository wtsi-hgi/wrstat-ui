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
	"fmt"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

const mountsActiveRowsInitialCap = 16

const mountsActiveRowsQuery = "SELECT mount_path, updated_at FROM wrstat_mounts_active ORDER BY mount_path"

type mountsActiveRow struct {
	mountPath string
	updatedAt time.Time
}

type chProvider struct {
	cfg Config

	conn ch.Conn

	db   db.Database
	tree *db.Tree
	bd   basedirs.Reader

	buildReaders func() (db.Database, *db.Tree, basedirs.Reader)

	mu       sync.RWMutex
	onUpdate func()
	onError  func(error)

	updateCh chan struct{}
	errCh    chan struct{}

	pendingFingerprint string
	pendingErr         error

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *chProvider) Tree() *db.Tree {
	p.mu.RLock()
	tree := p.tree
	p.mu.RUnlock()

	if tree != nil {
		return tree
	}

	p.mu.Lock()
	p.ensureReadersLocked()
	tree = p.tree
	p.mu.Unlock()

	return tree
}

func (p *chProvider) BaseDirs() basedirs.Reader {
	p.mu.RLock()
	bd := p.bd
	p.mu.RUnlock()

	if bd != nil {
		return bd
	}

	p.mu.Lock()
	p.ensureReadersLocked()
	bd = p.bd
	p.mu.Unlock()

	return bd
}

func (p *chProvider) ensureReadersLocked() {
	if p.tree != nil && p.bd != nil {
		return
	}

	if p.buildReaders == nil {
		p.buildReaders = p.defaultBuildReaders
	}

	dbImpl, tree, bd := p.buildReaders()
	p.db = dbImpl
	p.tree = tree
	p.bd = bd
}

func (p *chProvider) defaultBuildReaders() (db.Database, *db.Tree, basedirs.Reader) {
	dbImpl := newClickHouseDatabase(p.cfg, p.conn)

	return dbImpl, db.NewTree(dbImpl), newClickHouseBaseDirsReader(p.cfg, p.conn)
}

func (p *chProvider) OnUpdate(cb func()) {
	p.mu.Lock()
	p.onUpdate = cb
	p.mu.Unlock()
}

func (p *chProvider) OnError(cb func(error)) {
	p.mu.Lock()
	p.onError = cb
	p.mu.Unlock()
}

func (p *chProvider) Close() error {
	if p == nil {
		return nil
	}

	p.stopPolling()

	bd, dbImpl := p.detachReaders()
	p.closeOldReaders(dbImpl, bd)

	if p.conn == nil {
		return nil
	}

	return p.conn.Close()
}

func (p *chProvider) stopPolling() {
	if p.cancel != nil {
		p.cancel()
	}

	p.wg.Wait()
}

func (p *chProvider) detachReaders() (basedirs.Reader, db.Database) {
	p.mu.Lock()
	defer p.mu.Unlock()

	bd := p.bd
	dbImpl := p.db

	p.bd = nil
	p.db = nil
	p.tree = nil

	return bd, dbImpl
}

func (p *chProvider) startPolling() {
	if p == nil || p.conn == nil {
		return
	}

	if p.cfg.PollInterval <= 0 {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.updateCh = make(chan struct{}, 1)
	p.errCh = make(chan struct{}, 1)

	p.startWorker(ctx, p.pollLoop)
	p.startWorker(ctx, p.updateLoop)
	p.startWorker(ctx, p.errorLoop)
}

func (p *chProvider) startWorker(ctx context.Context, fn func(context.Context)) {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		fn(ctx)
	}()
}

func (p *chProvider) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	last, err := p.mountsActiveFingerprint(ctx)
	if err != nil {
		p.queueError(err)
	}

	for range ticker.C {
		if ctx.Err() != nil {
			return
		}

		fp, err := p.mountsActiveFingerprint(ctx)
		if err != nil {
			p.queueError(err)

			continue
		}

		if fp == last {
			continue
		}

		last = fp
		p.queueUpdate(fp)
	}
}

func (p *chProvider) mountsActiveFingerprint(parent context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(parent, queryTimeout(p.cfg))
	defer cancel()

	rows, err := p.mountsActiveRows(ctx)
	if err != nil {
		return "", err
	}

	return fingerprintForMountsActive(rows), nil
}

func fingerprintForMountsActive(rows []mountsActiveRow) string {
	var b strings.Builder
	for _, row := range rows {
		b.WriteString(row.mountPath)
		b.WriteString("|")
		b.WriteString(row.updatedAt.UTC().Format(time.RFC3339Nano))
		b.WriteString("\n")
	}

	return b.String()
}

func (p *chProvider) mountsActiveRows(ctx context.Context) ([]mountsActiveRow, error) {
	rows, err := p.conn.Query(ctx, mountsActiveRowsQuery)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query mounts_active: %w", err)
	}

	defer func() { _ = rows.Close() }()

	out := make([]mountsActiveRow, 0, mountsActiveRowsInitialCap)

	for rows.Next() {
		var (
			mountPath string
			updatedAt time.Time
		)

		if err := rows.Scan(&mountPath, &updatedAt); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan mounts_active: %w", err)
		}

		out = append(out, mountsActiveRow{mountPath: mountPath, updatedAt: updatedAt})
	}

	return out, nil
}

func (p *chProvider) queueUpdate(fp string) {
	p.mu.Lock()
	p.pendingFingerprint = fp
	p.mu.Unlock()

	select {
	case p.updateCh <- struct{}{}:
	default:
	}
}

func (p *chProvider) queueError(err error) {
	if err == nil {
		return
	}

	p.mu.Lock()
	p.pendingErr = err
	p.mu.Unlock()

	select {
	case p.errCh <- struct{}{}:
	default:
	}
}

func (p *chProvider) updateLoop(ctx context.Context) {
	processed := ""

	for {
		if !p.waitForSignal(ctx, p.updateCh) {
			return
		}

		processed = p.drainUpdates(ctx, processed)
	}
}

func (p *chProvider) drainUpdates(ctx context.Context, processed string) string {
	for {
		if ctx.Err() != nil {
			return processed
		}

		fp, cb := p.pendingUpdate()
		if fp == "" || fp == processed {
			return processed
		}

		processed = fp

		p.swapReadersAndInvoke(cb)
	}
}

func (p *chProvider) waitForSignal(ctx context.Context, ch <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

func (p *chProvider) pendingUpdate() (string, func()) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.pendingFingerprint, p.onUpdate
}

func (p *chProvider) swapReadersAndInvoke(cb func()) {
	newDB, newTree, newBD := p.buildReadersNow()
	oldDB, oldBD := p.publishReaders(newDB, newTree, newBD)

	if cb != nil {
		cb()
	}

	p.closeOldReaders(oldDB, oldBD)
}

func (p *chProvider) buildReadersNow() (db.Database, *db.Tree, basedirs.Reader) {
	p.mu.RLock()
	build := p.buildReaders
	p.mu.RUnlock()

	if build == nil {
		return p.defaultBuildReaders()
	}

	return build()
}

func (p *chProvider) publishReaders(
	newDB db.Database,
	newTree *db.Tree,
	newBD basedirs.Reader,
) (db.Database, basedirs.Reader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldDB, oldBD := p.db, p.bd
	p.db, p.tree, p.bd = newDB, newTree, newBD

	return oldDB, oldBD
}

func (p *chProvider) closeOldReaders(oldDB db.Database, oldBD basedirs.Reader) {
	if oldBD != nil {
		_ = oldBD.Close()
	}

	if oldDB != nil {
		_ = oldDB.Close()
	}
}

func (p *chProvider) errorLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.errCh:
		}

		p.mu.RLock()
		err := p.pendingErr
		cb := p.onError
		p.mu.RUnlock()

		if cb != nil && err != nil {
			cb(err)
		}
	}
}

func OpenProvider(cfg Config) (provider.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg))
	defer cancel()

	conn, err := connectAndBootstrap(ctx, opts, cfg.Database)
	if err != nil {
		return nil, err
	}

	p := &chProvider{cfg: cfg, conn: conn}
	p.buildReaders = p.defaultBuildReaders
	p.startPolling()

	return p, nil
}
