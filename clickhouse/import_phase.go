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
	"time"
)

type importPhaseRecorder func(string, time.Duration)

func timeImportPhase(
	recorder importPhaseRecorder,
	phase string,
	fn func() error,
) error {
	start := time.Now()
	err := fn()

	recordImportPhase(recorder, phase, time.Since(start))

	return err
}

func recordImportPhase(recorder importPhaseRecorder, phase string, d time.Duration) {
	if recorder == nil || phase == "" || d <= 0 {
		return
	}

	recorder(phase, d)
}

// SummariseImportTelemetry reports exact client batch counters and optional
// server-reported byte/part evidence during spool publication.
type SummariseImportTelemetry struct {
	Phase                               string
	CurrentCheckpoint                   string
	RowsSent                            uint64
	BytesSent                           uint64
	BytesSentAvailable                  bool
	BatchCount                          uint64
	EstimatedUncompressedBytesSent      uint64
	LastBatchEstimatedUncompressedBytes uint64
	PhaseRows                           uint64
	PhaseElapsed                        time.Duration
	ServerPartCount                     uint64
	ServerPartCountAvailable            bool
	ServerActiveMerges                  uint64
	ServerActiveMergesAvailable         bool
	ServerMemoryBytes                   uint64
	ServerMemoryBytesAvailable          bool
	ServerQueryLatency                  time.Duration
	ServerQueryLatencyAvailable         bool
	ServerPressureBackoff               bool
}

// WithSummariseImportTelemetry attaches a live publication recorder without
// changing the stable spool-loader call signature.
func WithSummariseImportTelemetry(
	ctx context.Context,
	recorder func(SummariseImportTelemetry),
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, summariseImportTelemetryContextKey{}, recorder)
}

func summariseImportTelemetryFromContext(ctx context.Context) func(SummariseImportTelemetry) {
	if ctx == nil {
		return nil
	}

	recorder, ok := ctx.Value(summariseImportTelemetryContextKey{}).(func(SummariseImportTelemetry))
	if !ok {
		return nil
	}

	return recorder
}

type summariseImportTelemetryContextKey struct{}
