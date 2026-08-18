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
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const summariseInsertTestQuery = "INSERT"

var (
	errPressureTestDenied           = errors.New("pressure probe denied")
	errPressureTestDestination      = errors.New("unexpected pressure scan destination")
	errPressureTestDestinationCount = errors.New("unexpected pressure scan destination count")
	errReplayAppendTest             = errors.New("replayed append failed")
)

func TestSummariseInsertMeasurementReportingAndValidation(t *testing.T) {
	Convey("Completed batches report their exact deterministic estimates", t, func() {
		var snapshots []SummariseImportTelemetry

		loader := &summariseSpoolLoader{
			telemetryRecorder: func(snapshot SummariseImportTelemetry) {
				snapshots = append(snapshots, snapshot)
			},
			batchMeasurements: make(map[string][]importBatchMeasurement),
		}
		loader.recordBatchTelemetry("spool_load_"+chspool.TableFiles,
			importBatchMeasurement{Rows: 2, EstimatedUncompressedBytes: 101})
		loader.recordBatchTelemetry("spool_load_"+chspool.TableFiles,
			importBatchMeasurement{Rows: 1, EstimatedUncompressedBytes: 73})

		So(snapshots, ShouldHaveLength, 2)
		So(snapshots[1].EstimatedUncompressedBytesSent, ShouldEqual, uint64(174))
		So(snapshots[1].LastBatchEstimatedUncompressedBytes, ShouldEqual, uint64(73))

		builder := &summariseSpoolLoadReportBuilder{
			batchMeasurements: cloneImportBatchMeasurements(loader.batchMeasurements),
			report:            perfreport.NewReport("clickhouse", "test", 1, 0),
		}
		inputs := builder.inputs(summariseSpoolCPUUsage{})
		batchBytes, ok := inputs["batch_estimated_uncompressed_bytes"].(map[string][]uint64)
		So(ok, ShouldBeTrue)
		So(batchBytes[chspool.TableFiles], ShouldResemble, []uint64{101, 73})
	})

	Convey("Negative and overflowing-duration limit settings are rejected", t, func() {
		So(errors.Is(validateSummariseInsertLimits(Config{SummariseFilesInsertBytes: -1}),
			errInvalidSummariseInsertLimit), ShouldBeTrue)
		So(errors.Is(validateSummariseInsertLimits(Config{SummarisePressureMaxActiveParts: -1}),
			errInvalidSummariseInsertLimit), ShouldBeTrue)
		So(errors.Is(validateSummariseInsertLimits(Config{SummarisePressureMaxQueryLatency: -1}),
			errInvalidSummariseInsertLimit), ShouldBeTrue)
		So(errors.Is(validateSummariseInsertLimits(Config{SummarisePressurePollInterval: time.Minute + 1}),
			errInvalidSummariseInsertLimit), ShouldBeTrue)
	})
}

type summarisePressureQueryRow struct {
	activeParts  uint64
	activeMerges uint64
	memoryBytes  uint64
	err          error
}

func (r summarisePressureQueryRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != 3 {
		return errPressureTestDestinationCount
	}

	values := [...]uint64{r.activeParts, r.activeMerges, r.memoryBytes}
	for i, value := range values {
		ptr, ok := dest[i].(*uint64)
		if !ok {
			return errPressureTestDestination
		}

		*ptr = value
	}

	return nil
}

func (r summarisePressureQueryRow) ScanStruct(any) error {
	return r.err
}

func (r summarisePressureQueryRow) Err() error {
	return r.err
}

func TestSummariseInsertPressureGuard(t *testing.T) {
	Convey("Each configured pressure signal pauses until it recovers", t, func() {
		cases := []struct {
			name     string
			pressure summariseServerPressure
		}{
			{"active parts", summariseServerPressure{ActiveParts: 11, ActivePartsAvailable: true}},
			{"merges", summariseServerPressure{ActiveMerges: 3, ActiveMergesAvailable: true}},
			{"memory", summariseServerPressure{MemoryBytes: 101, MemoryBytesAvailable: true}},
			{"latency", summariseServerPressure{QueryLatency: 11 * time.Millisecond, QueryLatencyAvailable: true}},
		}

		for _, tc := range cases {
			loader, calls := pressureGuardTestLoader(tc.pressure, summariseServerPressure{})

			So(loader.waitForServerPressure(context.Background(), chspool.TableFiles), ShouldBeNil)
			So(*calls, ShouldEqual, 2)
		}
	})

	Convey("Unavailable best-effort pressure evidence never invents pressure", t, func() {
		loader, calls := pressureGuardTestLoader(summariseServerPressure{}, summariseServerPressure{})

		So(loader.waitForServerPressure(context.Background(), chspool.TableFiles), ShouldBeNil)
		So(*calls, ShouldEqual, 1)
	})

	Convey("Cancellation interrupts a blocked pressure poll before another batch", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pollTimer := newSummarisePressureTestTimer()

		var pollInterval time.Duration

		conn := &summarisePressureGuardConn{}
		loader := &summariseSpoolLoader{
			cfg: Config{
				SummarisePressureMaxActiveParts: 1,
				SummarisePressurePollInterval:   time.Hour,
			},
			conn: conn,
			pressureProbe: func(context.Context, string) summariseServerPressure {
				return summariseServerPressure{ActiveParts: 2, ActivePartsAvailable: true}
			},
			pressurePollTimer: func(interval time.Duration) summarisePressurePollTimer {
				pollInterval = interval

				return pollTimer
			},
		}
		writer := newRealPressureGuardWriterWithLoader(conn, loader)
		So(writer.append(ctx, appendSizedTestValue("first")), ShouldBeNil)

		appendResult := make(chan error, 1)
		go func() {
			appendResult <- writer.append(ctx, appendSizedTestValue("second"))
		}()

		testDeadline := time.NewTimer(5 * time.Second)
		defer testDeadline.Stop()

		select {
		case <-pollTimer.entered:
			So(pollInterval, ShouldEqual, time.Hour)
		case <-testDeadline.C:
			So("pressure poll did not start", ShouldBeBlank)

			return
		}

		select {
		case <-appendResult:
			So("pressure poll returned before cancellation", ShouldBeBlank)

			return
		default:
		}

		cancel()

		select {
		case err := <-appendResult:
			So(errors.Is(err, context.Canceled), ShouldBeTrue)
		case <-testDeadline.C:
			So("pressure poll did not stop promptly", ShouldBeBlank)

			return
		}

		So(pollTimer.stopCalls, ShouldEqual, 1)
		So(conn.queryCalls, ShouldEqual, 0)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.totalAppends(), ShouldEqual, 1)
		So(conn.totalSends(), ShouldEqual, 1)
	})

	Convey("Parent cancellation around the real probe prevents the next batch", t, func() {
		cases := []struct {
			cancelDuringQuery bool
			expectedQueries   int
		}{
			{false, 0},
			{true, 1},
		}

		for _, tc := range cases {
			ctx, cancel := context.WithCancel(context.Background())

			conn := &summarisePressureGuardConn{}
			if tc.cancelDuringQuery {
				conn.cancelDuringQuery = cancel
			}

			writer := newRealPressureGuardWriter(conn)
			So(writer.append(ctx, appendSizedTestValue("first")), ShouldBeNil)

			if !tc.cancelDuringQuery {
				cancel()
			}

			err := writer.append(ctx, appendSizedTestValue("second"))

			cancel()

			So(errors.Is(err, context.Canceled), ShouldBeTrue)
			So(conn.queryCalls, ShouldEqual, tc.expectedQueries)
			So(conn.prepareCalls, ShouldEqual, 1)
			So(conn.totalAppends(), ShouldEqual, 1)
		}
	})

	Convey("An internal probe deadline remains nonblocking best-effort", t, func() {
		conn := &summarisePressureGuardConn{waitForProbeContext: true}
		writer := newRealPressureGuardWriterWithConfig(conn, Config{
			QueryTimeout:                    time.Nanosecond,
			SummarisePressureMaxActiveParts: 1,
		})

		So(writer.append(context.Background(), appendSizedTestValue("first")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("second")), ShouldBeNil)
		So(conn.queryCalls, ShouldEqual, 1)
		So(conn.prepareCalls, ShouldEqual, 2)
		So(conn.totalAppends(), ShouldEqual, 2)
	})

	Convey("An ordinary real probe error remains nonblocking best-effort", t, func() {
		conn := &summarisePressureGuardConn{probeErr: errPressureTestDenied}
		writer := newRealPressureGuardWriter(conn)

		So(writer.append(context.Background(), appendSizedTestValue("first")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("second")), ShouldBeNil)
		So(conn.queryCalls, ShouldEqual, 1)
		So(conn.prepareCalls, ShouldEqual, 2)
		So(conn.totalAppends(), ShouldEqual, 2)
	})

	Convey("The bounded best-effort probe reports all four server signals honestly", t, func() {
		conn := &summarisePressureQueryConn{row: summarisePressureQueryRow{
			activeParts: 7, activeMerges: 2, memoryBytes: 1234,
		}}
		loader := &summariseSpoolLoader{cfg: Config{Database: testDatabaseName}, conn: conn}
		times := []time.Time{time.Unix(0, 0), time.Unix(0, int64(15*time.Millisecond))}
		loader.telemetryNow = func() time.Time {
			now := times[0]
			times = times[1:]

			return now
		}

		pressure := loader.queryServerPressure(context.Background(), chspool.TableFiles)
		So(pressure.ActiveParts, ShouldEqual, uint64(7))
		So(pressure.ActivePartsAvailable, ShouldBeTrue)
		So(pressure.ActiveMerges, ShouldEqual, uint64(2))
		So(pressure.ActiveMergesAvailable, ShouldBeTrue)
		So(pressure.MemoryBytes, ShouldEqual, uint64(1234))
		So(pressure.MemoryBytesAvailable, ShouldBeTrue)
		So(pressure.QueryLatency, ShouldEqual, 15*time.Millisecond)
		So(pressure.QueryLatencyAvailable, ShouldBeTrue)
		So(conn.sawDeadline, ShouldBeTrue)
	})

	Convey("A failed best-effort probe marks every signal unavailable", t, func() {
		conn := &summarisePressureQueryConn{row: summarisePressureQueryRow{err: errPressureTestDenied}}
		loader := &summariseSpoolLoader{cfg: Config{Database: testDatabaseName}, conn: conn}

		pressure := loader.queryServerPressure(context.Background(), chspool.TableFiles)
		So(pressure.ActivePartsAvailable, ShouldBeFalse)
		So(pressure.ActiveMergesAvailable, ShouldBeFalse)
		So(pressure.MemoryBytesAvailable, ShouldBeFalse)
		So(pressure.QueryLatencyAvailable, ShouldBeFalse)
	})

	Convey("Pressure is checked between batches and not per row", t, func() {
		writer, _, _ := newSizedImportBlockWriter(2, 1024)
		checks := 0
		writer.beforeBatch = func(context.Context) error {
			checks++

			return nil
		}

		So(writer.append(context.Background(), appendSizedTestValue(uint8(1))), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue(uint8(2))), ShouldBeNil)
		So(checks, ShouldEqual, 0)
		So(writer.append(context.Background(), appendSizedTestValue(uint8(3))), ShouldBeNil)
		So(checks, ShouldEqual, 1)
	})

	Convey("No pressure probe is made after the final batch", t, func() {
		writer, _, _ := newSizedImportBlockWriter(2, 1024)
		checks := 0
		writer.beforeBatch = func(context.Context) error {
			checks++

			return nil
		}

		So(writer.append(context.Background(), appendSizedTestValue(uint8(1))), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue(uint8(2))), ShouldBeNil)
		So(writer.close(), ShouldBeNil)
		So(checks, ShouldEqual, 0)
	})

	Convey("Zero disables pressure limits and equality is allowed", t, func() {
		available := summariseServerPressure{
			ActiveParts: 10, ActivePartsAvailable: true,
			ActiveMerges: 2, ActiveMergesAvailable: true,
			MemoryBytes: 100, MemoryBytesAvailable: true,
			QueryLatency: 10 * time.Millisecond, QueryLatencyAvailable: true,
		}

		So(available.exceeds(Config{}), ShouldBeFalse)
		So(available.exceeds(Config{
			SummarisePressureMaxActiveParts:  10,
			SummarisePressureMaxMerges:       2,
			SummarisePressureMaxMemoryBytes:  100,
			SummarisePressureMaxQueryLatency: 10 * time.Millisecond,
		}), ShouldBeFalse)
	})

	Convey("The real pressure query scopes parts and merges to the target table", t, func() {
		conn := &summarisePressureQueryConn{row: summarisePressureQueryRow{}}
		loader := &summariseSpoolLoader{cfg: Config{Database: testDatabaseName}, conn: conn}

		loader.queryServerPressure(context.Background(), chspool.TableFiles)

		So(conn.query, ShouldContainSubstring,
			"FROM system.parts WHERE database = ? AND table = ? AND active")
		So(conn.query, ShouldContainSubstring,
			"FROM system.merges WHERE database = ? AND table = ?")
		So(conn.args, ShouldResemble,
			[]any{testDatabaseName, chspool.TableFiles, testDatabaseName, chspool.TableFiles})
	})

	Convey("Only merges for the target table cause backoff", t, func() {
		unrelated := newSummariseMergeScopeConn([]uint64{2, 0}, []uint64{0})
		loader := summariseMergeScopeLoader(unrelated)
		So(loader.waitForServerPressure(context.Background(), chspool.TableFiles), ShouldBeNil)
		So(unrelated.calls, ShouldEqual, 1)

		target := newSummariseMergeScopeConn([]uint64{2, 0}, []uint64{2, 0})
		loader = summariseMergeScopeLoader(target)
		So(loader.waitForServerPressure(context.Background(), chspool.TableFiles), ShouldBeNil)
		So(target.calls, ShouldEqual, 2)
	})
}

func pressureGuardTestLoader(
	pressures ...summariseServerPressure,
) (*summariseSpoolLoader, *int) {
	calls := 0
	loader := &summariseSpoolLoader{
		cfg: Config{
			SummarisePressureMaxActiveParts:  10,
			SummarisePressureMaxMerges:       2,
			SummarisePressureMaxMemoryBytes:  100,
			SummarisePressureMaxQueryLatency: 10 * time.Millisecond,
			SummarisePressurePollInterval:    time.Nanosecond,
		},
	}
	loader.pressureProbe = func(context.Context, string) summariseServerPressure {
		index := min(calls, len(pressures)-1)
		calls++

		return pressures[index]
	}

	return loader, &calls
}

func newSummarisePressureTestTimer() *summarisePressureTestTimer {
	return &summarisePressureTestTimer{
		c:       make(chan time.Time),
		entered: make(chan struct{}, 1),
	}
}

func newRealPressureGuardWriterWithLoader(
	conn *summarisePressureGuardConn,
	loader *summariseSpoolLoader,
) *importBlockWriter {
	var (
		batch    driver.Batch
		writeErr error
	)

	return &importBlockWriter{
		conn: conn, query: summariseInsertTestQuery, name: "pressure guard", batch: &batch, writeErr: &writeErr,
		batchSize: 1, batchBytes: 1024,
		beforeBatch: func(ctx context.Context) error {
			return loader.waitForServerPressure(ctx, chspool.TableFiles)
		},
	}
}

func appendSizedTestValue(value any) func(driver.Batch) error {
	return func(batch driver.Batch) error { return batch.Append(value) }
}

func newRealPressureGuardWriter(conn *summarisePressureGuardConn) *importBlockWriter {
	return newRealPressureGuardWriterWithConfig(conn, Config{SummarisePressureMaxActiveParts: 1})
}

func newRealPressureGuardWriterWithConfig(
	conn *summarisePressureGuardConn,
	cfg Config,
) *importBlockWriter {
	loader := &summariseSpoolLoader{cfg: cfg, conn: conn}

	return newRealPressureGuardWriterWithLoader(conn, loader)
}

func newSizedImportBlockWriter(
	rowLimit int,
	byteLimit uint64,
) (*importBlockWriter, *importBlockWriterSpyConn, *[]importBatchMeasurement) {
	conn := &importBlockWriterSpyConn{}

	var (
		batch    driver.Batch
		writeErr error
	)

	measurements := make([]importBatchMeasurement, 0)

	return &importBlockWriter{
		conn: conn, query: summariseInsertTestQuery, name: "test", batch: &batch, writeErr: &writeErr,
		batchSize: rowLimit, batchBytes: byteLimit,
		onSendMeasurement: func(measurement importBatchMeasurement) {
			measurements = append(measurements, measurement)
		},
	}, conn, &measurements
}

func newSummariseMergeScopeConn(globalMerges, targetMerges []uint64) *summariseMergeScopeConn {
	return &summariseMergeScopeConn{globalMerges: globalMerges, targetMerges: targetMerges}
}

func summariseMergeScopeLoader(conn *summariseMergeScopeConn) *summariseSpoolLoader {
	return &summariseSpoolLoader{
		cfg: Config{
			Database:                      testDatabaseName,
			SummarisePressureMaxMerges:    1,
			SummarisePressurePollInterval: time.Nanosecond,
		},
		conn: conn,
	}
}

type summarisePressureTestTimer struct {
	c         chan time.Time
	entered   chan struct{}
	stopCalls int
}

func (t *summarisePressureTestTimer) channel() <-chan time.Time {
	t.entered <- struct{}{}

	return t.c
}

func (t *summarisePressureTestTimer) stop() {
	t.stopCalls++
}

type summarisePressureQueryConn struct {
	bootstrapTestConn
	row         summarisePressureQueryRow
	sawDeadline bool
	query       string
	args        []any
}

func (c *summarisePressureQueryConn) QueryRow(
	ctx context.Context,
	query string,
	args ...any,
) driver.Row {
	_, c.sawDeadline = ctx.Deadline()
	c.query = query
	c.args = slices.Clone(args)

	return c.row
}

type summarisePressureGuardRow struct {
	done                <-chan struct{}
	contextErr          func() error
	cancelDuringQuery   context.CancelFunc
	err                 error
	waitForProbeContext bool
}

func (r summarisePressureGuardRow) Scan(...any) error {
	if r.cancelDuringQuery != nil {
		r.cancelDuringQuery()
	}

	if r.waitForProbeContext {
		<-r.done

		return r.contextErr()
	}

	if r.err != nil {
		return r.err
	}

	return r.contextErr()
}

func (r summarisePressureGuardRow) ScanStruct(any) error {
	return r.Scan()
}

func (r summarisePressureGuardRow) Err() error {
	return r.err
}

type summarisePressureGuardConn struct {
	importBlockWriterSpyConn
	cancelDuringQuery   context.CancelFunc
	probeErr            error
	waitForProbeContext bool
	queryCalls          int
}

func (c *summarisePressureGuardConn) QueryRow(
	ctx context.Context,
	_ string,
	_ ...any,
) driver.Row {
	c.queryCalls++

	return summarisePressureGuardRow{
		done:                ctx.Done(),
		contextErr:          ctx.Err,
		cancelDuringQuery:   c.cancelDuringQuery,
		err:                 c.probeErr,
		waitForProbeContext: c.waitForProbeContext,
	}
}

type summariseMergeScopeConn struct {
	bootstrapTestConn
	globalMerges []uint64
	targetMerges []uint64
	calls        int
}

func (c *summariseMergeScopeConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	index := min(c.calls, len(c.targetMerges)-1)
	globalIndex := min(c.calls, len(c.globalMerges)-1)
	c.calls++

	merges := c.globalMerges[globalIndex]

	if summariseMergeScopeMatches(query, args) {
		merges = c.targetMerges[index]
	}

	return summarisePressureQueryRow{activeMerges: merges}
}

func summariseMergeScopeMatches(query string, args []any) bool {
	if !strings.Contains(query, "FROM system.merges WHERE database = ? AND table = ?") {
		return false
	}

	return slices.Equal(args, []any{
		testDatabaseName, chspool.TableFiles, testDatabaseName, chspool.TableFiles,
	})
}

func TestSummariseInsertByteBudgets(t *testing.T) {
	Convey("A buffered replay append failure preserves execute-once and cleanup semantics", t, func() {
		conn := &importBlockWriterSpyConn{appendErr: errReplayAppendTest}

		var (
			batch            driver.Batch
			writeErr         error
			closureCalls     int
			successfulSends  int
			successTelemetry int
		)

		writer := &importBlockWriter{
			conn: conn, query: summariseInsertTestQuery, name: "buffered replay", batch: &batch, writeErr: &writeErr,
			batchSize: 10, batchBytes: 1024,
			onSend: func(uint64) {
				successfulSends++
			},
			onSendMeasurement: func(importBatchMeasurement) {
				successTelemetry++
			},
		}
		expectedArgs := []any{"row", uint64(7)}

		err := writer.append(context.Background(), func(batch driver.Batch) error {
			closureCalls++

			return batch.Append(expectedArgs...)
		})

		So(errors.Is(err, errReplayAppendTest), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "failed to append buffered replay row")
		So(closureCalls, ShouldEqual, 1)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].appendCalls, ShouldEqual, 1)
		So(conn.batches[0].appendArgs, ShouldResemble, [][]any{expectedArgs})
		So(conn.batches[0].appends, ShouldEqual, 0)
		So(conn.totalSends(), ShouldEqual, 0)
		So(successfulSends, ShouldEqual, 0)
		So(successTelemetry, ShouldEqual, 0)

		So(writer.close(), ShouldBeNil)
		So(conn.totalAborts(), ShouldEqual, 1)
		So(errors.Is(conn.batches[0].ctxErr(), context.Canceled), ShouldBeTrue)
	})

	Convey("A row is sent in a new batch when it would cross the byte target", t, func() {
		writer, conn, sends := newSizedImportBlockWriter(10, 5)

		So(writer.append(context.Background(), appendSizedTestValue("123")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("456")), ShouldBeNil)
		So(writer.close(), ShouldBeNil)

		So(conn.batches, ShouldHaveLength, 2)
		So(conn.batches[0].appends, ShouldEqual, 1)
		So(conn.batches[1].appends, ShouldEqual, 1)
		So(*sends, ShouldResemble, []importBatchMeasurement{
			{Rows: 1, EstimatedUncompressedBytes: 3},
			{Rows: 1, EstimatedUncompressedBytes: 3},
		})
	})

	Convey("A row that exactly reaches the byte target stays in the current batch", t, func() {
		writer, conn, sends := newSizedImportBlockWriter(10, 5)

		So(writer.append(context.Background(), appendSizedTestValue("123")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("45")), ShouldBeNil)

		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].appends, ShouldEqual, 2)
		So(*sends, ShouldResemble,
			[]importBatchMeasurement{{Rows: 2, EstimatedUncompressedBytes: 5}})
	})

	Convey("A wide row flushes a spool batch before its row cap", t, func() {
		writer, conn, sends := newSizedImportBlockWriter(10, 5)

		So(writer.append(context.Background(), appendSizedTestValue("123456")), ShouldBeNil)
		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].sends, ShouldEqual, 1)
		So(*sends, ShouldResemble, []importBatchMeasurement{{Rows: 1, EstimatedUncompressedBytes: 6}})
	})

	Convey("The row cap still sends first when it is reached before the byte target", t, func() {
		writer, conn, sends := newSizedImportBlockWriter(2, 1024)

		So(writer.append(context.Background(), appendSizedTestValue("1")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("2")), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue("3")), ShouldBeNil)
		So(writer.close(), ShouldBeNil)

		So(conn.batches, ShouldHaveLength, 2)
		So(*sends, ShouldResemble, []importBatchMeasurement{
			{Rows: 2, EstimatedUncompressedBytes: 2},
			{Rows: 1, EstimatedUncompressedBytes: 1},
		})
	})

	Convey("Narrow rows still flush at the existing row cap", t, func() {
		writer, conn, sends := newSizedImportBlockWriter(2, 1024)

		So(writer.append(context.Background(), appendSizedTestValue(uint8(1))), ShouldBeNil)
		So(writer.append(context.Background(), appendSizedTestValue(uint8(2))), ShouldBeNil)
		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].sends, ShouldEqual, 1)
		So(*sends, ShouldResemble, []importBatchMeasurement{{Rows: 2, EstimatedUncompressedBytes: 2}})
		So(writer.batchSize, ShouldEqual, 2)
		So(writer.batchBytes, ShouldEqual, uint64(1024))
	})

	Convey("File and filter tables have distinct fixed byte targets", t, func() {
		cfg := Config{SummariseFilesInsertBytes: 111, SummariseFilterInsertBytes: 222, SummariseOtherInsertBytes: 333}

		So(summariseSpoolBatchBytesFor(cfg, chspool.TableFiles), ShouldEqual, uint64(111))
		So(summariseSpoolBatchBytesFor(cfg, chspool.TableDirFilterAll), ShouldEqual, uint64(222))
		So(summariseSpoolBatchBytesFor(cfg, chspool.TableDirFacts), ShouldEqual, uint64(333))
		So(summariseSpoolBatchBytesFor(Config{}, chspool.TableFiles), ShouldNotEqual,
			summariseSpoolBatchBytesFor(Config{}, chspool.TableDirFilterAll))
	})

	Convey("The byte estimate is deterministic, width-sensitive, and saturating", t, func() {
		narrow := estimateClickHouseUncompressedBytes(uint8(1), "a", []uint32{1})
		wide := estimateClickHouseUncompressedBytes(uint64(1), "a much wider value", []uint32{1, 2, 3})

		So(narrow, ShouldEqual, estimateClickHouseUncompressedBytes(uint8(1), "a", []uint32{1}))
		So(wide, ShouldBeGreaterThan, narrow)
		So(saturatingAddUint64(math.MaxUint64, 1), ShouldEqual, uint64(math.MaxUint64))
	})
}
