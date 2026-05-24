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
	"errors"
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

func scanMountsActiveRow(rows rowsScanner) (mountsActiveRow, error) {
	var row mountsActiveRow
	if err := rows.Scan(&row.mountPath, &row.snapshotID, &row.updatedAt); err != nil {
		return mountsActiveRow{}, fmt.Errorf("clickhouse: failed to scan mounts_active: %w", err)
	}

	return row, nil
}

type readerBuilder func(context.Context, *activeMountsSnapshot) (db.Database, *db.Tree, basedirs.Reader, error)

type snapshotCapturer func(context.Context) (*activeMountsSnapshot, string, error)

type chProvider struct {
	cfg Config

	conn ch.Conn

	maintenanceConn    ch.Conn
	connectMaintenance clickHouseConfigConnector

	db   db.Database
	tree *db.Tree
	bd   basedirs.Reader

	buildReaders    readerBuilder
	captureSnapshot snapshotCapturer

	mu        sync.RWMutex
	onUpdate  func()
	onError   func(error)
	onMessage func(string)

	updateCh chan struct{}
	errCh    chan struct{}
	msgCh    chan struct{}

	currentFingerprint     string
	pendingFingerprint     string
	hasPendingUpdate       bool
	pendingErrs            []error
	pendingErrHead         int
	pendingMessages        []string
	pendingMsgHead         int
	treeSummaryJobs        map[string]*treeSummaryRefreshState
	mountDirProjectionJobs map[treeMountCacheKey]*mountDirProjectionRefreshState

	closing        bool
	workersStarted bool
	workerCancels  []context.CancelFunc
	wg             sync.WaitGroup
}

func newChProvider(
	cfg Config,
	conn ch.Conn,
	connectMaintenance clickHouseConfigConnector,
) *chProvider {
	return &chProvider{
		cfg:                cfg,
		conn:               conn,
		connectMaintenance: connectMaintenance,
	}
}

func (p *chProvider) Tree() *db.Tree {
	p.mu.RLock()
	tree := p.tree
	p.mu.RUnlock()

	if tree != nil {
		return tree
	}

	if err := p.ensureReaders(); err != nil {
		p.queueError(err)
	}

	p.mu.RLock()
	tree = p.tree
	p.mu.RUnlock()

	return tree
}

func (p *chProvider) BaseDirs() basedirs.Reader {
	p.mu.RLock()
	bd := p.bd
	p.mu.RUnlock()

	if bd != nil {
		return bd
	}

	if err := p.ensureReaders(); err != nil {
		p.queueError(err)
	}

	p.mu.RLock()
	bd = p.bd
	p.mu.RUnlock()

	return bd
}

func (p *chProvider) ensureReaders() error {
	if p.readersReady() {
		return nil
	}

	dbImpl, tree, bd, fingerprint, err := p.buildReadersNow(context.Background())
	if err != nil {
		return err
	}

	if p.publishLazyReaders(dbImpl, tree, bd, fingerprint) {
		return nil
	}

	p.closeOldReaders(dbImpl, bd)

	return nil
}

func (p *chProvider) readersReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.tree != nil && p.bd != nil
}

func (p *chProvider) publishLazyReaders(
	dbImpl db.Database,
	tree *db.Tree,
	bd basedirs.Reader,
	fingerprint string,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tree != nil && p.bd != nil {
		return false
	}

	p.db = dbImpl
	p.tree = tree
	p.bd = bd
	p.currentFingerprint = fingerprint

	return true
}

func (p *chProvider) readerHooksLocked() (readerBuilder, snapshotCapturer) {
	build := p.buildReaders
	if build == nil {
		build = p.defaultBuildReaders
	}

	capture := p.captureSnapshot
	if capture == nil {
		capture = p.captureActiveMountsState
	}

	return build, capture
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
	ctx, cancel := queryContext(parent, queryTimeout(p.cfg))
	defer cancel()

	rows, err := p.mountsActiveRows(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("clickhouse: failed to capture mounts_active snapshot: %w", err)
	}

	p.scheduleActiveMountDirProjectionRefreshBestEffort(ctx, rows)

	snapshot := newActiveMountsSnapshot(rows)
	if activeTreeSummariesReadyForSnapshot(ctx, p.conn, snapshot) {
		snapshot.markTreeSummaryReady()
	} else {
		p.scheduleTreeSummaryRefresh(context.WithoutCancel(ctx), snapshot, rows)
	}

	return snapshot, snapshot.fingerprint, nil
}

func activeTreeSummariesReadyForSnapshot(
	ctx context.Context,
	conn ch.Conn,
	snapshot *activeMountsSnapshot,
) bool {
	if snapshot == nil {
		return false
	}

	if snapshot.fingerprint == "" {
		return true
	}

	ready, err := treeSummaryReady(ctx, conn, snapshot.fingerprint)

	return err == nil && ready
}

func (p *chProvider) scheduleActiveMountDirProjectionRefreshBestEffort(
	ctx context.Context,
	rows []mountsActiveRow,
) {
	if p == nil || len(rows) == 0 {
		return
	}

	missingRows, err := activeMountDirProjectionRowsNeedingRefresh(ctx, p.conn, rows)
	if err != nil {
		p.queueError(err)

		missingRows = append([]mountsActiveRow(nil), rows...)
	}

	if len(missingRows) == 0 {
		p.markMountDirProjectionsReady(rows)

		return
	}

	p.scheduleMountDirProjectionRefresh(context.WithoutCancel(ctx), missingRows)
}

func (p *chProvider) OnMessage(cb func(message string)) {
	p.mu.Lock()
	p.onMessage = cb
	hasPendingMessages := p.hasPendingMessagesLocked()
	msgCh := p.msgCh
	p.mu.Unlock()

	if cb != nil && hasPendingMessages {
		signal(msgCh)
	}
}

func (p *chProvider) startBackgroundWorkers() {
	ctx, ok := p.newBackgroundContext()
	if !ok {
		return
	}

	p.errCh = make(chan struct{}, 1)
	p.msgCh = make(chan struct{}, 1)

	p.startWorker(ctx, p.errorLoop)
	p.startWorker(ctx, p.messageLoop)
}

func (p *chProvider) cancelWorkers() {
	p.mu.Lock()
	p.closing = true
	cancels := append([]context.CancelFunc(nil), p.workerCancels...)

	for _, job := range p.treeSummaryJobs {
		cancels = append(cancels, job.cancel)
	}

	for _, job := range p.mountDirProjectionJobs {
		cancels = append(cancels, job.cancel)
	}

	p.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (p *chProvider) newBackgroundContext() (context.Context, bool) {
	ctx, cancel := context.WithCancel(context.Background())

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.workersStarted || p.closing {
		cancel()

		return nil, false
	}

	p.workersStarted = true
	p.workerCancels = append(p.workerCancels, cancel)

	return ctx, true
}

func (p *chProvider) newWorkerContext() (context.Context, bool) {
	ctx, cancel := context.WithCancel(context.Background())

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closing {
		cancel()

		return nil, false
	}

	p.workerCancels = append(p.workerCancels, cancel)

	return ctx, true
}

func (p *chProvider) signalPendingCallbacks() {
	p.mu.RLock()
	hasPendingErrs := p.hasPendingErrorsLocked()
	hasPendingMessages := p.hasPendingMessagesLocked()
	p.mu.RUnlock()

	if hasPendingErrs {
		signal(p.errCh)
	}

	if hasPendingMessages {
		signal(p.msgCh)
	}
}

func (p *chProvider) detachMaintenanceConnection() ch.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn := p.maintenanceConn
	p.maintenanceConn = nil

	return conn
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

func (p *chProvider) OnUpdate(cb func()) {
	p.mu.Lock()
	p.onUpdate = cb
	p.mu.Unlock()
}

func (p *chProvider) OnError(cb func(error)) {
	p.mu.Lock()
	p.onError = cb
	hasPendingErrs := p.hasPendingErrorsLocked()
	errCh := p.errCh
	p.mu.Unlock()

	if cb != nil && hasPendingErrs {
		signal(errCh)
	}
}

func (p *chProvider) Close() error {
	if p == nil {
		return nil
	}

	p.stopPolling()

	bd, dbImpl := p.detachReaders()
	p.closeOldReaders(dbImpl, bd)

	var err error

	if maintenanceConn := p.detachMaintenanceConnection(); maintenanceConn != nil {
		err = errors.Join(err, maintenanceConn.Close())
	}

	if p.conn != nil {
		conn := p.conn
		p.conn = nil
		err = errors.Join(err, conn.Close())
	}

	return err
}

func (p *chProvider) stopPolling() {
	p.cancelWorkers()
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

	p.startBackgroundWorkers()

	if p.cfg.PollInterval <= 0 {
		return
	}

	p.updateCh = make(chan struct{}, 1)

	ctx, ok := p.newWorkerContext()
	if !ok {
		return
	}

	p.startWorker(ctx, p.pollLoop)
	p.startWorker(ctx, p.updateLoop)
	p.signalPendingCallbacks()
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
	ctx, cancel := queryContext(parent, queryTimeout(p.cfg))
	defer cancel()

	rows, err := p.mountsActiveRows(ctx)
	if err != nil {
		return "", err
	}

	return fingerprintForMountsActive(rows), nil
}

func (p *chProvider) mountsActiveRows(ctx context.Context) ([]mountsActiveRow, error) {
	return queryMountsActiveRows(ctx, p.conn)
}

func queryMountsActiveRows(ctx context.Context, conn ch.Conn) ([]mountsActiveRow, error) {
	rows, err := conn.Query(ctx, mountsActiveRowsQuery)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query mounts_active: %w", err)
	}

	defer func() { _ = rows.Close() }()

	out := make([]mountsActiveRow, 0, mountsActiveRowsInitialCap)

	for rows.Next() {
		row, err := scanMountsActiveRow(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: mounts_active iteration error: %w", err)
	}

	return out, nil
}

func (p *chProvider) queueUpdate(fp string) {
	p.mu.Lock()
	p.pendingFingerprint = fp
	p.hasPendingUpdate = true
	p.mu.Unlock()

	signal(p.updateCh)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
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

	signal(p.errCh)
}

func (p *chProvider) queueMessage(msg string) {
	if msg == "" {
		return
	}

	p.mu.Lock()
	p.pendingMessages = append(p.pendingMessages, msg)
	p.mu.Unlock()

	signal(p.msgCh)
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

	invokeOnFreshGoroutine(cb)

	p.closeOldReaders(oldDB, oldBD)

	return true
}

func invokeOnFreshGoroutine(fn func()) {
	if fn == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	<-done
}

func (p *chProvider) buildReadersNow(ctx context.Context) (db.Database, *db.Tree, basedirs.Reader, string, error) {
	p.mu.RLock()
	build, capture := p.readerHooksLocked()
	p.mu.RUnlock()

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
		p.pendingFingerprint = ""
		p.hasPendingUpdate = false
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

		cb := p.errorCallback()
		if cb == nil {
			return
		}

		err := p.popPendingError()
		if err == nil {
			return
		}

		invokeOnFreshGoroutine(func() {
			cb(err)
		})
	}
}

func (p *chProvider) errorCallback() func(error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.onError
}

func (p *chProvider) popPendingError() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pendingErrHead >= len(p.pendingErrs) {
		p.pendingErrs = nil
		p.pendingErrHead = 0

		return nil
	}

	err := p.pendingErrs[p.pendingErrHead]
	p.pendingErrHead++

	if p.pendingErrHead >= len(p.pendingErrs) {
		p.pendingErrs = nil
		p.pendingErrHead = 0
	}

	return err
}

func (p *chProvider) hasPendingErrorsLocked() bool {
	return p.pendingErrHead < len(p.pendingErrs)
}

func (p *chProvider) messageLoop(ctx context.Context) {
	for {
		if !p.waitForSignal(ctx, p.msgCh) {
			return
		}

		p.drainMessages(ctx)
	}
}

func (p *chProvider) drainMessages(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		cb := p.messageCallback()
		if cb == nil {
			return
		}

		msg := p.popPendingMessage()
		if msg == "" {
			return
		}

		invokeOnFreshGoroutine(func() {
			cb(msg)
		})
	}
}

func (p *chProvider) messageCallback() func(string) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.onMessage
}

func (p *chProvider) popPendingMessage() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pendingMsgHead >= len(p.pendingMessages) {
		p.pendingMessages = nil
		p.pendingMsgHead = 0

		return ""
	}

	msg := p.pendingMessages[p.pendingMsgHead]
	p.pendingMsgHead++

	if p.pendingMsgHead >= len(p.pendingMessages) {
		p.pendingMessages = nil
		p.pendingMsgHead = 0
	}

	return msg
}

func (p *chProvider) hasPendingMessagesLocked() bool {
	return p.pendingMsgHead < len(p.pendingMessages)
}

func (p *chProvider) buildInitialReaders() error {
	dbImpl, tree, bd, fingerprint, err := p.buildReadersNow(context.Background())
	if err != nil {
		return err
	}

	p.db = dbImpl
	p.tree = tree
	p.bd = bd
	p.currentFingerprint = fingerprint

	return nil
}

func (p *chProvider) maintenanceConnection(ctx context.Context) (ch.Conn, error) {
	p.mu.RLock()
	conn := p.maintenanceConn
	primaryConn := p.conn
	connectMaintenance := p.connectMaintenance
	closing := p.closing
	p.mu.RUnlock()

	if conn != nil {
		return conn, nil
	}

	if connectMaintenance == nil {
		return primaryConn, nil
	}

	if closing {
		return nil, context.Canceled
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	newConn, err := connectMaintenance(ctx, p.cfg)
	if err != nil {
		return nil, err
	}

	return p.storeMaintenanceConnection(newConn)
}

func (p *chProvider) storeMaintenanceConnection(newConn ch.Conn) (ch.Conn, error) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()

		_ = newConn.Close()

		return nil, context.Canceled
	}

	if p.maintenanceConn != nil {
		conn := p.maintenanceConn
		p.mu.Unlock()

		_ = newConn.Close()

		return conn, nil
	}

	p.maintenanceConn = newConn
	p.mu.Unlock()

	return newConn, nil
}

func (p *chProvider) startInitialReaders() error {
	p.startBackgroundWorkers()

	if err := p.buildInitialReaders(); err != nil {
		_ = p.Close()

		return err
	}

	return nil
}

func openProviderWithConnectors(
	cfg Config,
	connect clickHouseConfigConnector,
	connectMaintenance clickHouseConfigConnector,
) (provider.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	conn, err := openProviderConnection(cfg, connect)
	if err != nil {
		return nil, err
	}

	p := newChProvider(cfg, conn, connectMaintenance)
	if err := p.startInitialReaders(); err != nil {
		return nil, err
	}

	p.startPolling()

	return p, nil
}

func openProviderConnection(
	cfg Config,
	connect clickHouseConfigConnector,
) (ch.Conn, error) {
	if connect == nil {
		connect = connectFromConfigContext
	}

	return connect(context.Background(), cfg)
}

func openProviderWithConnector(
	cfg Config,
	connect func(Config) (ch.Conn, error),
) (provider.Provider, error) {
	return openProviderWithConnectors(cfg, legacyProviderConnector(connect), nil)
}

func OpenProvider(cfg Config) (provider.Provider, error) {
	return openProviderWithConnectors(cfg, connectFromConfigContext, connectMaintenanceFromConfig)
}

func legacyProviderConnector(connect func(Config) (ch.Conn, error)) clickHouseConfigConnector {
	return func(_ context.Context, cfg Config) (ch.Conn, error) {
		return connect(cfg)
	}
}
