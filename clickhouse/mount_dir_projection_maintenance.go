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

package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const mountDirProjectionMaintenanceRetryDelay = time.Second

const (
	mountDirProjectionMountStartedMessage = "clickhouse: active mount dir projection refresh mount started " +
		"active_mounts=%d mount_index=%d total_mounts=%d mount_path=%q snapshot_id=%s fingerprint=%s"
	mountDirProjectionMountSkippedMessage = "clickhouse: active mount dir projection refresh mount skipped ready " +
		"active_mounts=%d mount_index=%d total_mounts=%d mount_path=%q snapshot_id=%s duration=%s fingerprint=%s"
	mountDirProjectionMountCompletedMessage = "clickhouse: active mount dir projection refresh mount completed " +
		"active_mounts=%d mount_index=%d total_mounts=%d mount_path=%q snapshot_id=%s duration=%s fingerprint=%s"
	mountDirProjectionPhaseStartedMessage = "clickhouse: active mount dir projection refresh phase started " +
		"active_mounts=%d mount_index=%d total_mounts=%d mount_path=%q snapshot_id=%s phase=%s fingerprint=%s"
	mountDirProjectionPhaseCompletedMessage = "clickhouse: active mount dir projection refresh phase completed " +
		"active_mounts=%d mount_index=%d total_mounts=%d mount_path=%q snapshot_id=%s phase=%s duration=%s " +
		"fingerprint=%s"
)

func activeMountDirProjectionRowsNeedingRefresh(
	ctx context.Context,
	conn ch.Conn,
	rows []mountsActiveRow,
) ([]mountsActiveRow, error) {
	missing := make([]mountsActiveRow, 0, len(rows))
	for _, row := range rows {
		ready, err := mountDirProjectionsReady(ctx, conn, row.mountPath, row.snapshotID)
		if err != nil {
			return nil, err
		}

		if !ready {
			missing = append(missing, row)
		}
	}

	return missing, nil
}

type mountDirProjectionRefreshJob struct {
	rows             []mountsActiveRow
	fingerprint      string
	activeMountCount int
}

func newMountDirProjectionRefreshJob(rows []mountsActiveRow) mountDirProjectionRefreshJob {
	clonedRows := append([]mountsActiveRow(nil), rows...)

	return mountDirProjectionRefreshJob{
		rows:             clonedRows,
		fingerprint:      fingerprintForMountsActive(clonedRows),
		activeMountCount: len(clonedRows),
	}
}

func mountDirProjectionRefreshScheduledMessage(job mountDirProjectionRefreshJob) string {
	return fmt.Sprintf(
		"clickhouse: active mount dir projection refresh scheduled asynchronously active_mounts=%d fingerprint=%s",
		job.activeMountCount,
		mountDirProjectionFingerprintHash(job.fingerprint),
	)
}

func mountDirProjectionFingerprintHash(fingerprint string) string {
	sum := sha256.Sum256([]byte(fingerprint))

	return hex.EncodeToString(sum[:])[:12]
}

func (p *chProvider) runMountDirProjectionRefresh(ctx context.Context, job mountDirProjectionRefreshJob) {
	defer p.forgetMountDirProjectionRefresh(job.rows)

	if ctx.Err() != nil {
		return
	}

	p.queueMessage(mountDirProjectionRefreshStartedMessage(job))

	for ctx.Err() == nil {
		if p.tryMountDirProjectionRefresh(ctx, job) {
			return
		}

		if !sleepContext(ctx, mountDirProjectionMaintenanceRetryDelay) {
			return
		}
	}
}

func mountDirProjectionRefreshStartedMessage(job mountDirProjectionRefreshJob) string {
	return fmt.Sprintf(
		"clickhouse: active mount dir projection refresh started active_mounts=%d fingerprint=%s",
		job.activeMountCount,
		mountDirProjectionFingerprintHash(job.fingerprint),
	)
}

func (p *chProvider) tryMountDirProjectionRefresh(
	ctx context.Context,
	job mountDirProjectionRefreshJob,
) bool {
	started := time.Now()

	conn, err := p.maintenanceConnection(ctx)
	if err == nil {
		err = ensureActiveMountDirSummariesWithProgress(
			ctx,
			conn,
			job.rows,
			mountDirProjectionMessageProgress{
				job:   job,
				queue: p.queueMessage,
			},
		)
	}

	if err == nil {
		p.completeMountDirProjectionRefresh(job, time.Since(started))

		return true
	}

	if ctx.Err() == nil {
		p.queueError(fmt.Errorf("clickhouse: failed to refresh active mount dir projections asynchronously: %w", err))
	}

	return false
}

func (p *chProvider) completeMountDirProjectionRefresh(
	job mountDirProjectionRefreshJob,
	duration time.Duration,
) {
	p.markMountDirProjectionsReady(job.rows)
	p.queueMessage(mountDirProjectionRefreshCompletedMessage(job, duration))
}

func mountDirProjectionRefreshCompletedMessage(
	job mountDirProjectionRefreshJob,
	duration time.Duration,
) string {
	return fmt.Sprintf(
		"clickhouse: active mount dir projection refresh completed active_mounts=%d duration=%s fingerprint=%s",
		job.activeMountCount,
		duration.Round(time.Millisecond),
		mountDirProjectionFingerprintHash(job.fingerprint),
	)
}

type mountDirProjectionProgress interface {
	mountStarted(row mountsActiveRow, index, total int)
	mountSkipped(row mountsActiveRow, index, total int, duration time.Duration)
	mountCompleted(row mountsActiveRow, index, total int, duration time.Duration)
	phaseStarted(row mountsActiveRow, index, total int, phase string)
	phaseCompleted(row mountsActiveRow, index, total int, phase string, duration time.Duration)
}

type mountDirProjectionMessageProgress struct {
	job   mountDirProjectionRefreshJob
	queue func(string)
}

func (p mountDirProjectionMessageProgress) mountStarted(row mountsActiveRow, index, total int) {
	p.queueProgress(
		mountDirProjectionMountStartedMessage,
		p.job.activeMountCount,
		index,
		total,
		row.mountPath,
		row.snapshotID,
		mountDirProjectionFingerprintHash(p.job.fingerprint),
	)
}

func (p mountDirProjectionMessageProgress) mountSkipped(
	row mountsActiveRow,
	index, total int,
	duration time.Duration,
) {
	p.queueProgress(
		mountDirProjectionMountSkippedMessage,
		p.job.activeMountCount,
		index,
		total,
		row.mountPath,
		row.snapshotID,
		duration.Round(time.Millisecond),
		mountDirProjectionFingerprintHash(p.job.fingerprint),
	)
}

func (p mountDirProjectionMessageProgress) mountCompleted(
	row mountsActiveRow,
	index, total int,
	duration time.Duration,
) {
	p.queueProgress(
		mountDirProjectionMountCompletedMessage,
		p.job.activeMountCount,
		index,
		total,
		row.mountPath,
		row.snapshotID,
		duration.Round(time.Millisecond),
		mountDirProjectionFingerprintHash(p.job.fingerprint),
	)
}

func (p mountDirProjectionMessageProgress) phaseStarted(
	row mountsActiveRow,
	index, total int,
	phase string,
) {
	p.queueProgress(
		mountDirProjectionPhaseStartedMessage,
		p.job.activeMountCount,
		index,
		total,
		row.mountPath,
		row.snapshotID,
		phase,
		mountDirProjectionFingerprintHash(p.job.fingerprint),
	)
}

func (p mountDirProjectionMessageProgress) phaseCompleted(
	row mountsActiveRow,
	index, total int,
	phase string,
	duration time.Duration,
) {
	p.queueProgress(
		mountDirProjectionPhaseCompletedMessage,
		p.job.activeMountCount,
		index,
		total,
		row.mountPath,
		row.snapshotID,
		phase,
		duration.Round(time.Millisecond),
		mountDirProjectionFingerprintHash(p.job.fingerprint),
	)
}

func (p mountDirProjectionMessageProgress) queueProgress(format string, args ...any) {
	if p.queue == nil {
		return
	}

	p.queue(fmt.Sprintf(format, args...))
}

func (p *chProvider) markMountDirProjectionsReady(rows []mountsActiveRow) {
	cache := treeQueryCacheForConfig(p.cfg)

	for _, row := range rows {
		key := newTreeMountCacheKey(row.mountPath, row.snapshotID)
		cache.putMountDirSummaryReady(key)
		cache.putMountDirDGUTAVectorReady(key)
	}
}

func (p *chProvider) scheduleMountDirProjectionRefresh(
	ctx context.Context,
	rows []mountsActiveRow,
) {
	if p == nil || len(rows) == 0 {
		return
	}

	maintenanceCtx, cancel := context.WithCancel(ctx)

	registeredRows := p.registerMountDirProjectionRefresh(rows, cancel)
	if len(registeredRows) == 0 {
		cancel()

		return
	}

	job := newMountDirProjectionRefreshJob(registeredRows)
	p.queueMessage(mountDirProjectionRefreshScheduledMessage(job))
	p.startWorker(maintenanceCtx, func(ctx context.Context) {
		defer cancel()

		p.runMountDirProjectionRefresh(ctx, job)
	})
}

type mountDirProjectionRefreshState struct {
	cancel context.CancelFunc
}

func (p *chProvider) registerMountDirProjectionRefresh(
	rows []mountsActiveRow,
	cancel context.CancelFunc,
) []mountsActiveRow {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.workersStarted || p.closing {
		return nil
	}

	if p.mountDirProjectionJobs == nil {
		p.mountDirProjectionJobs = make(map[treeMountCacheKey]*mountDirProjectionRefreshState)
	}

	registeredRows := make([]mountsActiveRow, 0, len(rows))
	for _, row := range rows {
		key := newTreeMountCacheKey(row.mountPath, row.snapshotID)
		if _, exists := p.mountDirProjectionJobs[key]; exists {
			continue
		}

		p.mountDirProjectionJobs[key] = &mountDirProjectionRefreshState{cancel: cancel}

		registeredRows = append(registeredRows, row)
	}

	return registeredRows
}

func (p *chProvider) forgetMountDirProjectionRefresh(rows []mountsActiveRow) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, row := range rows {
		delete(p.mountDirProjectionJobs, newTreeMountCacheKey(row.mountPath, row.snapshotID))
	}
}
