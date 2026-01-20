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

	mu       sync.RWMutex
	onUpdate func()
	onError  func(error)

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *chProvider) Tree() *db.Tree {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tree != nil {
		return p.tree
	}

	if p.db == nil {
		p.db = newClickHouseDatabase(p.cfg, p.conn)
	}

	p.tree = db.NewTree(p.db)

	return p.tree
}

func (p *chProvider) BaseDirs() basedirs.Reader {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.bd != nil {
		return p.bd
	}

	p.bd = newClickHouseBaseDirsReader(p.cfg, p.conn)

	return p.bd
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

	if p.cancel != nil {
		p.cancel()
	}

	p.wg.Wait()

	if p.conn == nil {
		return nil
	}

	return p.conn.Close()
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

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		p.pollLoop(ctx)
	}()
}

func (p *chProvider) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	last, err := p.mountsActiveFingerprint(ctx)
	if err != nil {
		p.notifyError(err)
	}

	for range ticker.C {
		if ctx.Err() != nil {
			return
		}

		fp, err := p.mountsActiveFingerprint(ctx)
		if err != nil {
			p.notifyError(err)

			continue
		}

		if fp == last {
			continue
		}

		last = fp

		p.notifyUpdate()
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
		b.WriteString(row.snapshotID)
		b.WriteString("|")
		b.WriteString(row.updatedAt.UTC().Format(time.RFC3339Nano))
		b.WriteString("\n")
	}

	return b.String()
}

func (p *chProvider) mountsActiveRows(ctx context.Context) ([]mountsActiveRow, error) {
	query := "SELECT mount_path, snapshot_id, updated_at FROM wrstat_mounts_active ORDER BY mount_path"

	rows, err := p.conn.Query(ctx, query)
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

		out = append(out, mountsActiveRow{mountPath: mountPath, snapshotID: snapshotID, updatedAt: updatedAt})
	}

	return out, nil
}

func (p *chProvider) notifyUpdate() {
	p.mu.RLock()
	cb := p.onUpdate
	p.mu.RUnlock()

	if cb != nil {
		cb()
	}
}

func (p *chProvider) notifyError(err error) {
	p.mu.RLock()
	cb := p.onError
	p.mu.RUnlock()

	if cb != nil {
		cb(err)
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
	p.startPolling()

	return p, nil
}
