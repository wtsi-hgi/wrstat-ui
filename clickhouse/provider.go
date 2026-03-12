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

const mountsActiveRowsQuery = "SELECT mount_path, toString(snapshot_id), updated_at " +
	"FROM wrstat_mounts_active ORDER BY mount_path"

type mountsActiveRow struct {
	mountPath  string
	snapshotID string
	updatedAt  time.Time
}

type chProvider struct {
	cfg Config

	conn ch.Conn

	db   db.Database
	tree *db.Tree
	bd   basedirs.Reader

	buildReaders    func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error)
	captureSnapshot func(context.Context) (*activeMountsSnapshot, string, error)

	mu       sync.RWMutex
	onUpdate func()
	onError  func(error)

	updateCh chan struct{}
	errCh    chan struct{}

	currentFingerprint string
	pendingFingerprint string
	hasPendingUpdate   bool
	pendingErrs        []error
	pendingErrHead     int

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

	var err error

	p.mu.Lock()
	err = p.ensureReadersLocked()
	tree = p.tree
	p.mu.Unlock()

	if err != nil {
		p.queueError(err)
	}

	return tree
}

func (p *chProvider) BaseDirs() basedirs.Reader {
	p.mu.RLock()
	bd := p.bd
	p.mu.RUnlock()

	if bd != nil {
		return bd
	}

	var err error

	p.mu.Lock()
	err = p.ensureReadersLocked()
	bd = p.bd
	p.mu.Unlock()

	if err != nil {
		p.queueError(err)
	}

	return bd
}

func (p *chProvider) ensureReadersLocked() error {
	if p.tree != nil && p.bd != nil {
		return nil
	}

	build := p.buildReaders
	if build == nil {
		build = p.defaultBuildReaders
	}

	capture := p.captureSnapshot
	if capture == nil {
		capture = p.captureActiveMountsState
	}

	snapshot, fingerprint, err := capture(context.Background())
	if err != nil {
		return err
	}

	dbImpl, tree, bd, err := build(context.Background(), snapshot)
	if err != nil {
		return err
	}

	p.db = dbImpl
	p.tree = tree
	p.bd = bd
	p.currentFingerprint = fingerprint

	return nil
}

func (p *chProvider) defaultBuildReaders(
	_ context.Context,
	snapshot *activeMountsSnapshot,
) (db.Database, *db.Tree, basedirs.Reader, error) {
	dbImpl := newClickHouseDatabaseWithSnapshot(p.cfg, p.conn, snapshot)

	bd, err := newClickHouseBaseDirsReaderWithSnapshot(p.cfg, p.conn, snapshot)
	if err != nil {
		return nil, nil, nil, err
	}

	return dbImpl, db.NewTree(dbImpl), bd, nil
}

func (p *chProvider) captureActiveMountsState(parent context.Context) (*activeMountsSnapshot, string, error) {
	ctx, cancel := context.WithTimeout(parent, queryTimeout(p.cfg))
	defer cancel()

	rows, err := p.mountsActiveRows(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("clickhouse: failed to capture mounts_active snapshot: %w", err)
	}

	return newActiveMountsSnapshot(rows), fingerprintForMountsActive(rows), nil
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

	p.pollOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		p.pollOnce(ctx)
	}
}

func (p *chProvider) pollOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	fp, err := p.mountsActiveFingerprint(ctx)
	if err != nil {
		p.queueError(err)

		return
	}

	if fp == p.currentPublishedFingerprint() {
		return
	}

	p.queueUpdate(fp)
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
		b.WriteString(row.snapshotID)
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
			mountPath  string
			snapshotID string
			updatedAt  time.Time
		)

		if err := rows.Scan(&mountPath, &snapshotID, &updatedAt); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan mounts_active: %w", err)
		}

		out = append(out, mountsActiveRow{
			mountPath:  mountPath,
			snapshotID: snapshotID,
			updatedAt:  updatedAt,
		})
	}

	return out, nil
}

func (p *chProvider) queueUpdate(fp string) {
	p.mu.Lock()
	p.pendingFingerprint = fp
	p.hasPendingUpdate = true
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
	p.pendingErrs = append(p.pendingErrs, err)
	p.mu.Unlock()

	select {
	case p.errCh <- struct{}{}:
	default:
	}
}

func (p *chProvider) updateLoop(ctx context.Context) {
	for {
		if !p.waitForSignal(ctx, p.updateCh) {
			return
		}

		p.drainUpdates(ctx)
	}
}

func (p *chProvider) drainUpdates(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		fp, ok, cb := p.pendingUpdate()
		if !ok || fp == p.currentPublishedFingerprint() {
			return
		}

		if !p.swapReadersAndInvoke(ctx, fp, cb) {
			return
		}
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

func (p *chProvider) pendingUpdate() (string, bool, func()) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.pendingFingerprint, p.hasPendingUpdate, p.onUpdate
}

func (p *chProvider) swapReadersAndInvoke(ctx context.Context, targetFingerprint string, cb func()) bool {
	newDB, newTree, newBD, publishedFingerprint, err := p.buildReadersNow(ctx)
	if err != nil {
		p.queueError(err)

		return false
	}

	oldDB, oldBD := p.publishReaders(
		newDB,
		newTree,
		newBD,
		targetFingerprint,
		publishedFingerprint,
	)

	invokeSerializedCallback(cb)

	p.closeOldReaders(oldDB, oldBD)

	return true
}

func invokeSerializedCallback(cb func()) {
	if cb == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		cb()
	}()

	<-done
}

func (p *chProvider) buildReadersNow(ctx context.Context) (db.Database, *db.Tree, basedirs.Reader, string, error) {
	p.mu.RLock()
	build := p.buildReaders
	capture := p.captureSnapshot
	p.mu.RUnlock()

	if build == nil {
		build = p.defaultBuildReaders
	}

	if capture == nil {
		capture = p.captureActiveMountsState
	}

	snapshot, fingerprint, err := capture(ctx)
	if err != nil {
		return nil, nil, nil, "", err
	}

	dbImpl, tree, bd, err := build(ctx, snapshot)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return dbImpl, tree, bd, fingerprint, nil
}

func (p *chProvider) publishReaders(
	newDB db.Database,
	newTree *db.Tree,
	newBD basedirs.Reader,
	targetFingerprint string,
	publishedFingerprint string,
) (db.Database, basedirs.Reader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldDB, oldBD := p.db, p.bd
	p.db, p.tree, p.bd = newDB, newTree, newBD
	p.currentFingerprint = publishedFingerprint

	if p.hasPendingUpdate && p.pendingFingerprint == targetFingerprint {
		p.pendingFingerprint = publishedFingerprint
	}

	return oldDB, oldBD
}

func (p *chProvider) currentPublishedFingerprint() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.currentFingerprint
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
		if !p.waitForSignal(ctx, p.errCh) {
			return
		}

		p.drainErrors(ctx)
	}
}

func (p *chProvider) drainErrors(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		cb, err := p.pendingError()
		if err == nil {
			return
		}

		invokeSerializedErrorCallback(cb, err)
	}
}

func invokeSerializedErrorCallback(cb func(error), err error) {
	if cb == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		cb(err)
	}()

	<-done
}

func (p *chProvider) pendingError() (func(error), error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pendingErrHead >= len(p.pendingErrs) {
		p.pendingErrs = nil
		p.pendingErrHead = 0

		return p.onError, nil
	}

	err := p.pendingErrs[p.pendingErrHead]
	p.pendingErrHead++

	if p.pendingErrHead >= len(p.pendingErrs) {
		p.pendingErrs = nil
		p.pendingErrHead = 0
	}

	return p.onError, err
}

func OpenProvider(cfg Config) (provider.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := connectAndBootstrap(context.Background(), opts, cfg.Database, queryTimeout(cfg))
	if err != nil {
		return nil, err
	}

	p := &chProvider{cfg: cfg, conn: conn}

	dbImpl, tree, bd, fingerprint, err := p.buildReadersNow(context.Background())
	if err != nil {
		_ = conn.Close()

		return nil, err
	}

	p.db = dbImpl
	p.tree = tree
	p.bd = bd
	p.currentFingerprint = fingerprint
	p.startPolling()

	return p, nil
}
