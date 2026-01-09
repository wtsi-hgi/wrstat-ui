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
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	defaultBatchSize = 100_000

	activeSnapshotQuery = "SELECT toString(snapshot_id), updated_at FROM wrstat_mounts_active WHERE mount_path = ?"
	switchSnapshotQuery = "INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at) " +
		"VALUES (?, now64(3), toUUID(?), ?)"
)

var (
	errMountPathRequired = errors.New("clickhouse: mount path is required")
	errUpdatedAtRequired = errors.New("clickhouse: updated at is required")
	errDirRequired       = errors.New("clickhouse: record dir is required")
)

type dgutaWriter struct {
	cfg Config

	conn ch.Conn

	batchSize int

	mountPath string
	updatedAt time.Time
	snapshot  uuid.UUID

	closed bool
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		w.batchSize = batchSize
	}
}

func (w *dgutaWriter) SetMountPath(mountPath string) {
	w.mountPath = mountPath
}

func (w *dgutaWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
}

func (w *dgutaWriter) Add(dguta db.RecordDGUTA) error {
	if w.mountPath == "" {
		return errMountPathRequired
	}

	if w.updatedAt.IsZero() {
		return errUpdatedAtRequired
	}

	if dguta.Dir == nil {
		return errDirRequired
	}

	// Future slices: drop partitions, batch insert dguta+children.
	if w.snapshot == uuid.Nil {
		w.snapshot = snapshotID(w.mountPath, w.updatedAt)
	}

	return nil
}

func (w *dgutaWriter) Close() error {
	if w == nil {
		return nil
	}

	if w.closed {
		return nil
	}

	w.closed = true

	if w.conn == nil {
		return nil
	}

	if w.shouldSwitchSnapshot() {
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(w.cfg))
		defer cancel()

		if err := w.switchActiveSnapshot(ctx); err != nil {
			_ = w.conn.Close()

			return err
		}
	}

	return w.conn.Close()
}

func (w *dgutaWriter) shouldSwitchSnapshot() bool {
	return w.mountPath != "" && !w.updatedAt.IsZero()
}

func (w *dgutaWriter) ensureSnapshotID() {
	if w.snapshot != uuid.Nil {
		return
	}

	w.snapshot = snapshotID(w.mountPath, w.updatedAt)
}

func (w *dgutaWriter) switchActiveSnapshot(ctx context.Context) error {
	w.ensureSnapshotID()

	// Read previous snapshot (required by spec). We don't use it yet (future slice drops old partitions).
	rows, err := w.conn.Query(ctx, activeSnapshotQuery, w.mountPath)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to read active snapshot: %w", err)
	}

	_ = rows.Close()

	if err := w.conn.Exec(ctx, switchSnapshotQuery, w.mountPath, w.snapshot.String(), w.updatedAt); err != nil {
		return fmt.Errorf("clickhouse: failed to switch active snapshot: %w", err)
	}

	return nil
}

// NewDGUTAWriter returns a ClickHouse-backed implementation of db.DGUTAWriter.
func NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error) {
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

	return &dgutaWriter{cfg: cfg, conn: conn, batchSize: defaultBatchSize}, nil
}
