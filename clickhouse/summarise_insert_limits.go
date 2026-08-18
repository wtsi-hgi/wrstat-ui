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
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
)

const (
	defaultSummariseFilesInsertBytes  = 16 * 1024 * 1024
	defaultSummariseFilterInsertBytes = 8 * 1024 * 1024
	defaultSummariseOtherInsertBytes  = 32 * 1024 * 1024
	clickHouseArrayOffsetBytes        = 8
	clickHouseDateTimeBytes           = 8
	clickHouseUUIDBytes               = 16
	clickHouseByteBytes               = 1
	clickHouseShortBytes              = 2
	clickHouseWordBytes               = 4
	clickHouseLongBytes               = 8
	maxSummarisePressurePollInterval  = time.Minute
)

var (
	errImportBatchMultipleAppends  = errors.New("clickhouse: import row appended more than once")
	errInvalidSummariseInsertLimit = errors.New("clickhouse: invalid summarise insert limit")
)

func summariseSpoolBatchBytesFor(cfg Config, table string) uint64 {
	configured := cfg.SummariseOtherInsertBytes
	fallback := int64(defaultSummariseOtherInsertBytes)

	switch table {
	case chspool.TableFiles:
		configured = cfg.SummariseFilesInsertBytes
		fallback = defaultSummariseFilesInsertBytes
	case chspool.TableDirFilterAgeAll,
		chspool.TableChildFilterAll,
		chspool.TableDirFilterAll,
		chspool.TableActiveVirtualFilterAll:
		configured = cfg.SummariseFilterInsertBytes
		fallback = defaultSummariseFilterInsertBytes
	}

	if configured <= 0 {
		configured = fallback
	}

	return uint64(configured)
}

func summarisePressureEnabled(cfg Config) bool {
	return cfg.SummarisePressureMaxActiveParts > 0 ||
		cfg.SummarisePressureMaxMerges > 0 ||
		cfg.SummarisePressureMaxMemoryBytes > 0 ||
		cfg.SummarisePressureMaxQueryLatency > 0
}

func summarisePressurePollInterval(cfg Config) time.Duration {
	if cfg.SummarisePressurePollInterval > 0 {
		return cfg.SummarisePressurePollInterval
	}

	return time.Second
}

func validateSummariseInsertLimits(cfg Config) error {
	checks := []struct {
		name  string
		value int64
	}{
		{"files insert bytes", cfg.SummariseFilesInsertBytes},
		{"filter insert bytes", cfg.SummariseFilterInsertBytes},
		{"other insert bytes", cfg.SummariseOtherInsertBytes},
		{"maximum active parts", cfg.SummarisePressureMaxActiveParts},
		{"maximum merges", cfg.SummarisePressureMaxMerges},
		{"maximum memory bytes", cfg.SummarisePressureMaxMemoryBytes},
	}
	for _, check := range checks {
		if check.value < 0 {
			return fmt.Errorf("%w: %s must not be negative", errInvalidSummariseInsertLimit, check.name)
		}
	}

	if cfg.SummarisePressureMaxQueryLatency < 0 {
		return fmt.Errorf("%w: maximum query latency must not be negative", errInvalidSummariseInsertLimit)
	}

	interval := cfg.SummarisePressurePollInterval
	if interval < 0 || interval > maxSummarisePressurePollInterval {
		return fmt.Errorf("%w: pressure poll interval must be between zero and %s",
			errInvalidSummariseInsertLimit, maxSummarisePressurePollInterval)
	}

	return nil
}

type summarisePressurePollTimer interface {
	channel() <-chan time.Time
	stop()
}

func newSummarisePressurePollTimer(interval time.Duration) summarisePressurePollTimer {
	return &summarisePressureRealTimer{Timer: time.NewTimer(interval)}
}

func waitForSummarisePressurePoll(
	ctx context.Context,
	interval time.Duration,
	timerFactory func(time.Duration) summarisePressurePollTimer,
) error {
	if timerFactory == nil {
		timerFactory = newSummarisePressurePollTimer
	}

	timer := timerFactory(interval)
	defer timer.stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.channel():
		return nil
	}
}

type summarisePressureRealTimer struct {
	*time.Timer
}

func (t *summarisePressureRealTimer) channel() <-chan time.Time {
	return t.C
}

func (t *summarisePressureRealTimer) stop() {
	t.Stop()
}

type importBatchMeasurement struct {
	Rows                       uint64
	EstimatedUncompressedBytes uint64
}

type importBatchBufferedBatch struct {
	driver.Batch
	values   []any
	bytes    uint64
	appended bool
}

func (b *importBatchBufferedBatch) Append(values ...any) error {
	if b.appended {
		return errImportBatchMultipleAppends
	}

	b.values = values
	b.bytes = estimateClickHouseUncompressedBytes(values...)
	b.appended = true

	return nil
}

func estimateClickHouseUncompressedBytes(values ...any) uint64 {
	var total uint64
	for _, value := range values {
		total = saturatingAddUint64(total, estimateClickHouseValueBytes(reflect.ValueOf(value)))
	}

	return total
}

type summariseServerPressure struct {
	ActiveParts           uint64
	ActivePartsAvailable  bool
	ActiveMerges          uint64
	ActiveMergesAvailable bool
	MemoryBytes           uint64
	MemoryBytesAvailable  bool
	QueryLatency          time.Duration
	QueryLatencyAvailable bool
}

func (p summariseServerPressure) exceeds(cfg Config) bool {
	return pressureUint64Exceeded(p.ActiveParts, p.ActivePartsAvailable, cfg.SummarisePressureMaxActiveParts) ||
		pressureUint64Exceeded(p.ActiveMerges, p.ActiveMergesAvailable, cfg.SummarisePressureMaxMerges) ||
		pressureUint64Exceeded(p.MemoryBytes, p.MemoryBytesAvailable, cfg.SummarisePressureMaxMemoryBytes) ||
		pressureDurationExceeded(p.QueryLatency, p.QueryLatencyAvailable, cfg.SummarisePressureMaxQueryLatency)
}

func pressureUint64Exceeded(value uint64, available bool, threshold int64) bool {
	return available && threshold > 0 && value > uint64(threshold)
}

func pressureDurationExceeded(value time.Duration, available bool, threshold time.Duration) bool {
	return available && threshold > 0 && value > threshold
}

func saturatingAddUint64(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}

	return a + b
}

func estimateClickHouseValueBytes(value reflect.Value) uint64 {
	if !value.IsValid() {
		return 0
	}

	if bytes, ok := estimateKnownClickHouseValueBytes(value); ok {
		return bytes
	}

	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		return estimateClickHouseIndirectBytes(value)
	case reflect.String:
		return uint64(uint(value.Len()))
	case reflect.Slice, reflect.Array:
		return estimateClickHouseSequenceBytes(value)
	case reflect.Map:
		return estimateClickHouseMapBytes(value)
	default:
		return estimateClickHouseScalarBytes(value)
	}
}

func estimateKnownClickHouseValueBytes(value reflect.Value) (uint64, bool) {
	if value.Type() == reflect.TypeFor[time.Time]() {
		return clickHouseDateTimeBytes, true
	}

	if value.Type() == reflect.TypeFor[uuid.UUID]() {
		return clickHouseUUIDBytes, true
	}

	return 0, false
}

func estimateClickHouseIndirectBytes(value reflect.Value) uint64 {
	if value.IsNil() {
		return 0
	}

	return estimateClickHouseValueBytes(value.Elem())
}

func estimateClickHouseSequenceBytes(value reflect.Value) uint64 {
	total := uint64(clickHouseArrayOffsetBytes)
	for i := range value.Len() {
		total = saturatingAddUint64(total, estimateClickHouseValueBytes(value.Index(i)))
	}

	return total
}

func estimateClickHouseMapBytes(value reflect.Value) uint64 {
	total := uint64(clickHouseArrayOffsetBytes)

	iter := value.MapRange()
	for iter.Next() {
		total = saturatingAddUint64(total, estimateClickHouseValueBytes(iter.Key()))
		total = saturatingAddUint64(total, estimateClickHouseValueBytes(iter.Value()))
	}

	return total
}

func estimateClickHouseScalarBytes(value reflect.Value) uint64 {
	switch value.Kind() {
	case reflect.Bool, reflect.Int8, reflect.Uint8:
		return clickHouseByteBytes
	case reflect.Int16, reflect.Uint16:
		return clickHouseShortBytes
	case reflect.Int32, reflect.Uint32, reflect.Float32:
		return clickHouseWordBytes
	case reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64, reflect.Float64:
		return clickHouseLongBytes
	default:
		return uint64(value.Type().Size())
	}
}
