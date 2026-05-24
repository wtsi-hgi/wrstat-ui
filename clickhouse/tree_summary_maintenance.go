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
	"fmt"
	"time"
)

const treeSummaryMaintenanceRetryDelay = time.Second

type treeSummaryRefreshJob struct {
	rows             []mountsActiveRow
	fingerprint      string
	activeMountCount int
	ancestorDirCount int
}

func newTreeSummaryRefreshJob(rows []mountsActiveRow) treeSummaryRefreshJob {
	clonedRows := append([]mountsActiveRow(nil), rows...)
	snapshot := newActiveMountsSnapshot(clonedRows)

	return treeSummaryRefreshJob{
		rows:             clonedRows,
		fingerprint:      snapshot.fingerprint,
		activeMountCount: len(clonedRows),
		ancestorDirCount: len(activeTreeDirs(snapshot.all())),
	}
}

func treeSummaryRefreshScheduledMessage(job treeSummaryRefreshJob) string {
	return fmt.Sprintf(
		"clickhouse: active tree summary refresh scheduled asynchronously active_mounts=%d ancestor_dirs=%d fingerprint=%s",
		job.activeMountCount,
		job.ancestorDirCount,
		treeSummaryFingerprintHash(job.fingerprint),
	)
}

func (p *chProvider) runTreeSummaryRefresh(ctx context.Context, job treeSummaryRefreshJob) {
	defer p.forgetTreeSummaryRefresh(job.fingerprint)

	if ctx.Err() != nil {
		return
	}

	p.queueMessage(treeSummaryRefreshStartedMessage(job))

	for ctx.Err() == nil {
		if p.tryTreeSummaryRefresh(ctx, job) {
			return
		}

		if !sleepContext(ctx, treeSummaryMaintenanceRetryDelay) {
			return
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func treeSummaryRefreshStartedMessage(job treeSummaryRefreshJob) string {
	return fmt.Sprintf(
		"clickhouse: active tree summary refresh started active_mounts=%d ancestor_dirs=%d fingerprint=%s",
		job.activeMountCount,
		job.ancestorDirCount,
		treeSummaryFingerprintHash(job.fingerprint),
	)
}

func (p *chProvider) tryTreeSummaryRefresh(ctx context.Context, job treeSummaryRefreshJob) bool {
	started := time.Now()

	conn, err := p.maintenanceConnection(ctx)
	if err == nil {
		err = refreshActiveTreeSummaries(ctx, conn, job.rows, job.fingerprint)
	}

	if err == nil {
		p.completeTreeSummaryRefresh(job, time.Since(started))

		return true
	}

	if ctx.Err() == nil {
		p.queueError(fmt.Errorf("clickhouse: failed to refresh active tree summaries asynchronously: %w", err))
	}

	return false
}

func (p *chProvider) completeTreeSummaryRefresh(job treeSummaryRefreshJob, duration time.Duration) {
	for _, snapshot := range p.finishTreeSummaryRefresh(job.fingerprint) {
		snapshot.markTreeSummaryReady()
	}

	p.queueMessage(treeSummaryRefreshCompletedMessage(job, duration))
}

func treeSummaryRefreshCompletedMessage(job treeSummaryRefreshJob, duration time.Duration) string {
	return fmt.Sprintf(
		"clickhouse: active tree summary refresh completed active_mounts=%d ancestor_dirs=%d duration=%s fingerprint=%s",
		job.activeMountCount,
		job.ancestorDirCount,
		duration.Round(time.Millisecond),
		treeSummaryFingerprintHash(job.fingerprint),
	)
}

func treeSummaryFingerprintHash(fingerprint string) string {
	sum := sha256.Sum256([]byte(fingerprint))

	return hex.EncodeToString(sum[:])[:12]
}

func (p *chProvider) scheduleTreeSummaryRefresh(
	ctx context.Context,
	snapshot *activeMountsSnapshot,
	rows []mountsActiveRow,
) {
	if p == nil || len(rows) == 0 {
		return
	}

	job := newTreeSummaryRefreshJob(rows)
	if job.fingerprint == "" {
		return
	}

	maintenanceCtx, cancel := context.WithCancel(ctx)
	if !p.registerTreeSummaryRefresh(snapshot, job.fingerprint, cancel) {
		cancel()

		return
	}

	p.queueMessage(treeSummaryRefreshScheduledMessage(job))
	p.startWorker(maintenanceCtx, func(ctx context.Context) {
		defer cancel()

		p.runTreeSummaryRefresh(ctx, job)
	})
}

type treeSummaryRefreshState struct {
	cancel    context.CancelFunc
	snapshots []*activeMountsSnapshot
}

func (p *chProvider) registerTreeSummaryRefresh(
	snapshot *activeMountsSnapshot,
	fingerprint string,
	cancel context.CancelFunc,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.workersStarted || p.closing {
		return false
	}

	if p.treeSummaryJobs == nil {
		p.treeSummaryJobs = make(map[string]*treeSummaryRefreshState)
	}

	if state, exists := p.treeSummaryJobs[fingerprint]; exists {
		state.snapshots = append(state.snapshots, snapshot)

		return false
	}

	p.treeSummaryJobs[fingerprint] = &treeSummaryRefreshState{
		cancel:    cancel,
		snapshots: []*activeMountsSnapshot{snapshot},
	}

	return true
}

func (p *chProvider) finishTreeSummaryRefresh(fingerprint string) []*activeMountsSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.treeSummaryJobs[fingerprint]
	delete(p.treeSummaryJobs, fingerprint)

	if state == nil {
		return nil
	}

	return append([]*activeMountsSnapshot(nil), state.snapshots...)
}

func (p *chProvider) forgetTreeSummaryRefresh(fingerprint string) {
	p.mu.Lock()
	delete(p.treeSummaryJobs, fingerprint)
	p.mu.Unlock()
}
