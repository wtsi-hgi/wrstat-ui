# ClickHouse Summarise Speed Recovery Specification

## Overview

Recover production `wrstat-ui summarise` wall time after schema3 and durable
spool changes. The regression is duplicated full-filter work: schema3 writes
and loads identical row payloads into `wrstat_dir_filter_all` and
`wrstat_child_filter_all`.

The fix keeps the visible schema3 query contract unchanged. The spool writes
`wrstat_dir_filter_all` as the canonical full-filter table, loads it first,
then populates `wrstat_child_filter_all` inside ClickHouse with an
`INSERT INTO ... SELECT` before schema3 readiness or mount publish.

Durable spool retry/recovery remains mandatory. Completed-spool retry must not
reparse `stats.gz`, and failed load/publish must leave the old active snapshot
and active virtual set visible.

Recorded `summarise -t` performance gates are tree-only. They do not measure
basedirs work; basedirs retry cleanup/replay is covered by functional
no-regression tests in this spec.

## Architecture

- `cmd/summarise_spool.go`
  - Stop `summariseDGUTASpoolWriter.writeSchema3FullFilterRow` from writing
    `wrstat_child_filter_all`.
  - Keep `wrstat_child_filter_all` in the manifest as an empty spool file.
  - Set schema3 readiness `child_filter_all_rows` from
    `wrstat_dir_filter_all` row count.
- `clickhouse/dir_filter_all.go`
  - Make direct import write only `wrstat_dir_filter_all`.
  - Derive `wrstat_child_filter_all` during `fullFilterAllWriter.flush`.
- `clickhouse/child_filter_all.go`
  - Add the explicit derived insert query. Do not use `SELECT *`.
- `clickhouse/dguta_writer.go`
  - Split `wrstat_filter_all_insert` into:
    - `wrstat_dir_filter_all_insert`;
    - `wrstat_child_filter_all_insert`.
- `clickhouse/summarise_spool_loader.go`
  - Load `wrstat_dir_filter_all` before deriving child rows.
  - Verify derived child row counts before schema3 readiness.
  - Include derived child evidence in spool load reports.
- `clickhouse/summarise_spool_loader.go` and `clickhouse/basedirs_store.go`
  - Keep basedirs retry cleanup/replay behavior unchanged.
  - Keep zero-record active virtual fast path unchanged; normal nonzero
    mounts still materialize and validate the full active virtual overlay.
- `internal/chspool/spool.go`
  - Keep manifest identity, schema/version, file presence, byte size, and
    SHA256 checks.
  - Compare row counts with trusted writer-close manifest evidence. Do not
    decode every row only to recount a completed spool.
- `internal/perfreport/report.go`
  - Add per-table spool bytes, phase row/byte rates, and input-row
    amplification evidence.
- `internal/chperf/import.go`
  - Make `clickhouse-perf import` measure the same derived-child path and
    report both full-filter tables.
- `internal/chperf/final_gate.go`
  - Add bounded summarise-speed gates and full-filter evidence checks.
- `summary/summariser.go` and `cmd/summarise_diagnostics.go`
  - Expose and log the final parsed record count.

Public API signatures:

```go
func LoadSummariseSpool(
    ctx context.Context,
    cfg Config,
    spoolDir string,
    manifest *chspool.Manifest,
    recorder func(string, time.Duration),
) error

func LoadSummariseSpoolReport(
    ctx context.Context,
    cfg Config,
    spoolDir string,
    manifest *chspool.Manifest,
    recorder func(string, time.Duration),
) (perfreport.Report, error)

func VerifyManifest(dir string, got *Manifest, expected Manifest) error

func (s *Summariser) ParsedRecords() uint64
```

Use existing `perfreport.TableStats.Rows`, `CompressedBytes`,
`UncompressedBytes`, and `ImportPhaseDurationsMS`. Add fields to exported
`perfreport.TableStats`:

```go
SpoolBytes uint64 `json:"spool_bytes,omitempty"`
PhaseRows map[string]uint64 `json:"phase_rows,omitempty"`
PhaseBytes map[string]uint64 `json:"phase_bytes,omitempty"`
RowsPerSec map[string]float64 `json:"rows_per_sec,omitempty"`
BytesPerSec map[string]float64 `json:"bytes_per_sec,omitempty"`
RowAmplificationVsInputRecords float64
```

`RowAmplificationVsInputRecords` uses JSON tag
`json:"row_amplification_vs_input_records,omitempty"`.

Derived child insert must map columns explicitly:

```sql
INSERT INTO wrstat_child_filter_all
(
  mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir,
  count, size, atime_min, mtime_max, atime_buckets, mtime_buckets,
  filter_child_count, child_count, has_filter_children, has_children,
  refreshed_at
)
SELECT
  mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir,
  count, size, atime_min, mtime_max, atime_buckets, mtime_buckets,
  filter_child_count, child_count, has_filter_children, has_children,
  refreshed_at
FROM wrstat_dir_filter_all
WHERE mount_path = ? AND snapshot_id = toUUID(?)
```

## Section A: Derived Child Full-Filter Table

### A1: Canonical Dir Spool

As an operator, I want the spool to write full-filter rows once, so that
bounded and production summarise runs avoid duplicated gob output.

`wrstat_dir_filter_all` is the only non-empty full-filter spool table.
`wrstat_child_filter_all` remains in `chspool.TableOrder()` and the manifest,
but has zero rows and only empty-file bytes. Schema3 readiness counts still
record the final physical child table row count, equal to dir rows.

**Package:** `cmd/`
**File:** `cmd/summarise_spool.go`
**Test file:** `cmd/summarise_spool_test.go`

**Acceptance tests:**

1. Given a schema3 spool fixture with 3 dir full-filter rows, when
   `summariseDGUTASpoolWriter.Close` completes, then
   `manifest.Tables["wrstat_dir_filter_all"].Rows == 3`,
   `manifest.Tables["wrstat_child_filter_all"].Rows == 0`, and
   `wrstat_schema3_snapshot_sets.child_filter_all_rows == 3`.
2. Given the same fixture, when the manifest is read, then
   `wrstat_child_filter_all` is present with table name
   `"wrstat_child_filter_all"` and a SHA256 for the empty gob file.
3. Given a non-empty t283-shaped fixture, when the spool writer closes, then
   no call to `Set.WriteChildFilterAll` occurs for full-filter rows.

### A2: Child Derivation Before Readiness

As a query user, I want `wrstat_child_filter_all` to exist before the snapshot
is ready, so that schema3 query behavior is unchanged after publish.

The loader loads `wrstat_dir_filter_all`, derives `wrstat_child_filter_all`,
counts both tables, and only then loads `wrstat_schema3_snapshot_sets`.
Failure in the derived insert or row-count check aborts before readiness,
active virtual readiness, mount switch, cleanup, and completion marker.

**Package:** `clickhouse/`
**File:** `clickhouse/summarise_spool_loader.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`

**Acceptance tests:**

1. Given a manifest with `wrstat_dir_filter_all.Rows == 5` and
   `wrstat_child_filter_all.Rows == 0`, when the loader succeeds, then event
   order is `send wrstat_dir_filter_all`, `derive wrstat_child_filter_all`,
   `count wrstat_child_filter_all`, `send wrstat_schema3_snapshot_sets`,
   `publish`.
2. Given a derived child insert failure, when loading the spool, then the error
   is returned, `wrstat_schema3_snapshot_sets` inserted rows is `0`,
   `activePublishes() == 0`, and the publish state has not marked
   `tables_loaded`.
3. Given `count wrstat_child_filter_all` returns `4` while expected is `5`,
   when loading the spool, then the error wraps `errSpoolLoadedRowsMismatch`,
   schema3 readiness inserted rows is `0`, and active publishes is `0`.
4. Given a completed canonical spool, unreadable `stats.gz`, and
   `post_spool_publish_state.json` with `tables_loaded` complete but no later
   publish flags, when `wrstat-ui summarise --clickhouse-recover` runs, then
   it never opens `stats.gz`, sends zero table spool batches, runs zero
   derived child inserts, and runs this ordered sequence: active virtual
   readiness, mount switch, old snapshot cleanup, old active virtual cleanup,
   tree summary refresh, active-prefix refresh, table evidence/reporting, and
   completion marker creation.

### A3: Direct Import Uses Same Contract

As a performance investigator, I want `clickhouse-perf import` to exercise the
same full-filter path as production summarise, so that reports are comparable.

Direct import writes dir full-filter rows only, derives child rows in
ClickHouse, and records both split phases. It does not append duplicated child
rows through `childFilterAllWriter`.

**Package:** `clickhouse/`, `internal/chperf/`
**File:** `clickhouse/dir_filter_all.go`, `internal/chperf/import.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`internal/chperf/import_test.go`

**Acceptance tests:**

1. Given a direct import fixture with 7 full-filter rows, when
   `fullFilterAllWriter.flush` succeeds, then exactly 7 rows are appended to
   `wrstat_dir_filter_all`, zero rows are batch-appended to
   `wrstat_child_filter_all`, and the derived insert inserts 7 child rows.
2. Given derived child insert fails during direct import close, then close
   returns that error, mount switch does not run, and the old active snapshot
   remains visible.
3. Given a `clickhouse-perf import` report, then `selected_tables` contains
   both `wrstat_dir_filter_all` and `wrstat_child_filter_all`, and
   `table_stats` has non-zero rows for both after a successful import.

### A4: Cleanup Derived Child Rows

As an operator, I want derived child rows cleaned up with other snapshot
tables, so that failed or replaced snapshots leave no visible residue.

Cleanup treats `wrstat_child_filter_all` as a physical snapshot table even
though it is derived from canonical spool rows. Failed load retry, old snapshot
drop, inactive snapshot cleanup, and tombstone cleanup must remove child rows
wherever they remove `wrstat_dir_filter_all`.

**Package:** `clickhouse/`
**File:** `clickhouse/dguta_writer.go`,
`clickhouse/active_snapshot_cleanup.go`,
`clickhouse/summarise_spool_loader.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`clickhouse/active_snapshot_cleanup_test.go`,
`clickhouse/summarise_spool_loader_test.go`

**Acceptance tests:**

1. Given a failed spool load left derived child rows for snapshot `S`, when
   retry starts, then `drop wrstat_child_filter_all` and
   `drop wrstat_dir_filter_all` both occur before reloading `S`.
2. Given a replacement snapshot publishes over an old active snapshot, when old
   snapshot cleanup runs, then it drops partitions for both full-filter tables
   and keeps the replacement snapshot visible.
3. Given `CleanActiveSnapshotAttempt` tombstones a failed snapshot with no
   previous mount row, then both full-filter partitions for that snapshot are
   dropped and no active mount row remains.
4. Given inactive snapshot cleanup removes `wrstat_dir_filter_all` partitions
   for snapshot `S`, when it runs, then it also removes
   `wrstat_child_filter_all` partitions for `S`, records both table names in
   cleanup evidence, and leaves the active snapshot visible.

### A5: Preserve Non-Targeted Publish Behavior

As an operator, I want the speed fix to avoid basedirs and active virtual
behavior changes, so that recent durable-retry fixes stay intact.

Do not change basedirs retry cleanup/replay. Do not change the zero-record
active virtual fast path. Normal nonzero active virtual publishes must still
materialize and validate the full overlay before mount switch.

**Package:** `clickhouse/`
**File:** `clickhouse/summarise_spool_loader.go`,
`clickhouse/basedirs_store.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`,
`clickhouse/basedirs_store_test.go`

**Acceptance tests:**

1. Given completed-spool retry with basedirs history rows already present and
   one missing row, when replay runs, then cleanup uses the existing batched
   `(gid, date)` delete, reinserts only missing rows, and final history row
   count is exactly the expected count with no duplicates.
2. Given zero-record active virtual publish with a previous ready active set,
   when publish runs after child derivation, then it composes the new active
   virtual set from previous rows, preserves summary/filter/child row counts,
   and switches mount state only after active virtual readiness.
3. Given a normal nonzero active virtual publish, when publish runs after child
   derivation, then it inserts and validates all summary/filter/child overlay
   rows before mount switch.

## Section B: Evidence, Diagnostics, and Guards

### B1: Split Full-Filter Telemetry

As an operator, I want per-table full-filter timings and bytes, so that reports
show where summarise time went.

Replace the combined phase in production and perf reports. Existing reports may
keep old phase names only for backward compatibility; new measurements must use
split names. For each full-filter table report:

- `rows` is the final physical table row count;
- `spool_bytes` is set for loaded spool tables and `0` for derived tables;
- `phase_rows[phase]` is rows inserted by that phase;
- `phase_bytes[phase]` is spool bytes for loaded tables and ClickHouse
  uncompressed bytes for derived tables;
- `rows_per_sec[phase]` and `bytes_per_sec[phase]` are positive when phase
  duration and rows/bytes are positive.

**Package:** `clickhouse/`, `internal/chperf/`, `internal/perfreport/`
**File:** `clickhouse/dguta_writer.go`,
`clickhouse/summarise_spool_loader.go`, `internal/chperf/import.go`,
`internal/perfreport/report.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`,
`internal/chperf/import_test.go`

**Acceptance tests:**

1. Given a spool load report with 5 dir rows, then
   `table_stats["wrstat_dir_filter_all"].rows == 5`,
   `import_phase_durations_ms["wrstat_dir_filter_all_insert"] > 0`,
   `phase_rows["wrstat_dir_filter_all_insert"] == 5`,
   `phase_bytes["wrstat_dir_filter_all_insert"] == spool_bytes`,
   `rows_per_sec["wrstat_dir_filter_all_insert"] > 0`,
   `bytes_per_sec["wrstat_dir_filter_all_insert"] > 0`,
   `spool_bytes > 0`, and compressed/uncompressed bytes are greater than `0`.
2. Given the same report, then
   `table_stats["wrstat_child_filter_all"].rows == 5`,
   `import_phase_durations_ms["wrstat_child_filter_all_insert"] > 0`,
   `phase_rows["wrstat_child_filter_all_insert"] == 5`,
   `phase_bytes["wrstat_child_filter_all_insert"] == uncompressed_bytes`,
   `rows_per_sec["wrstat_child_filter_all_insert"] > 0`,
   `bytes_per_sec["wrstat_child_filter_all_insert"] > 0`,
   `spool_bytes == 0`, and compressed/uncompressed bytes are greater than `0`.
3. Given a direct import report, then no operation input has
   `"phase": "wrstat_filter_all_insert"`, and both split phases appear.
4. Given report JSON is decoded by `perfreport.Report`, then all new fields are
   present under `table_stats` without breaking schema version `1`.

### B2: Full-Filter Amplification Guard

As an operator, I want full-filter row amplification reported and guarded, so
that dense mounts are visible before they dominate summarise time.

For each summarise/import report, compute:

- `dir_filter_all.Rows / input_records`;
- `child_filter_all.Rows / input_records`;
- `(dir_filter_all.Rows + child_filter_all.Rows) / input_records`.

Warn when duplicated full-filter rows per input record is `> 5.0`.
Hard-fail only when an explicit debug/perf guard is enabled and the ratio is
`> 10.0`. Production summarise without the explicit guard must warn, not fail.

**Package:** `clickhouse/`, `internal/chperf/`
**File:** `clickhouse/summarise_spool_loader.go`, `internal/chperf/import.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`,
`internal/chperf/import_test.go`

**Acceptance tests:**

1. Given `input_records == 200000`, dir rows `1196726`, and child rows
   `1196726`, when report evidence is built, then duplicated amplification is
   `11.96726`, dir amplification is `5.98363`, child amplification is
   `5.98363`, and warning status is `"warn"`.
2. Given the same values and the explicit hard guard enabled, then the report
   builder returns an error containing `"full-filter amplification"` and
   `"> 10"`.
3. Given `input_records == 200000`, dir rows `26883`, and child rows `26883`,
   then duplicated amplification is `0.26883` and status is `"ok"`.

### B3: Final Parse Count Logging

As an operator, I want parse-complete logs to show actual parsed records, so
that bounded runs below 1M rows do not report `records=0`.

`summary.Summariser` stores the final parsed record count and
`summariseDiagnostics.logParseResult` logs it on success and failure.
Periodic progress behavior is unchanged.

**Package:** `summary/`, `cmd/`
**File:** `summary/summariser.go`, `cmd/summarise_diagnostics.go`
**Test file:** `summary/summariser_test.go`, `cmd/summarise_diagnostics_test.go`

**Acceptance tests:**

1. Given 200 parsed records and progress interval 1,000,000, when summarise
   completes, then the parse-complete log contains `records=200`.
2. Given a parse error after 17 records, when summarise fails, then the
   parse-failure log contains `records=17`.
3. Given existing progress interval 200 and 650 records, when summarise runs,
   periodic progress callbacks remain exactly `[200, 400, 600]`.

## Section C: Fast Completed-Spool Verification

### C1: Hash-Based Manifest Verification

As an operator retrying a completed spool, I want verification to avoid a full
gob decode, so that recovery cost stays bounded by file hashing.

`VerifyManifest` keeps its signature. It verifies identity and every table's
presence, table key, row count, byte size, and SHA256 against `expected`.
`expected.Tables` must be writer-close manifest evidence retained from the
initial successful close, not a copy rebuilt from the disk manifest being
verified. If writer-close table evidence is unavailable, fast verification
must fail closed or run the old decoded verification path before reuse. If any
row/size/hash/schema/identity check fails, the spool is untrusted.

**Package:** `internal/chspool/`
**File:** `internal/chspool/spool.go`
**Test file:** `internal/chspool/spool_test.go`

**Acceptance tests:**

1. Given a completed spool whose writer-close expected manifest has
   `wrstat_dir_filter_all.Rows == 5`, when the disk manifest is tampered to
   `Rows == 4` but file bytes and hashes still match, then `VerifyManifest`
   returns an error wrapping `ErrManifestMismatch` and mentioning row count.
2. Given a completed spool file with one byte appended, when `VerifyManifest`
   runs, then the error wraps `ErrManifestMismatch` and mentions that table.
3. Given expected `wrstat_dir_filter_all` spool file is missing, when
   `VerifyManifest` runs, then the error wraps `ErrManifestMismatch` and
   mentions `wrstat_dir_filter_all`.
4. Given expected zero-row `wrstat_child_filter_all` empty gob file is
   missing, when `VerifyManifest` runs, then the error wraps
   `ErrManifestMismatch` and mentions `wrstat_child_filter_all`.
5. Given manifest `Version`, `Format`, `SchemaMarker`, `SnapshotID`,
   `OutputDir`, or input file identity differs from expected, then the error
   wraps `ErrManifestMismatch` and no table hashing is required.
6. Given `expected.Tables` is empty or lacks `wrstat_dir_filter_all`, when
   fast verification runs, then it returns `ErrManifestMismatch` instead of
   trusting manifest-only row counts.
7. Given a 20.6 MiB t283 derived spool, when fast verification runs as a perf
   gate, then wall time is `<= 0.25s`; full decoded verification baseline was
   `5.25s`.
8. Given a 6.11 MiB scratch127 derived spool, fast verification is
   `<= 0.15s`;
   full decoded verification baseline was `0.70s`.

## Section D: Bounded Performance Gates

### D1: Reproduce Failure Class Before Fix

As an implementor, I want a bounded local proof before changing code, so that
the implementation stops if the regression class cannot be reproduced.

Use bounded inputs under `.tmp/agent/summarise/inputs/` and command shapes from
`.docs/summarise/investigate.md`. Do not run full production mounts.
Recorded `summarise -t` baselines are tree-only and exclude basedirs work.

Required evidence fixtures:

- t283 200k: wall `41.24s`, load `32.21s`, RSS `372.8 MiB`,
  spool `30.4 MiB`, dir/child rows `1196726`, load `dir 11.02s`,
  `child 10.98s`.
- scratch127 200k: wall `3.58s`, load `2.23s`, RSS `146.9 MiB`,
  spool `6.42 MiB`, dir/child rows `26883`, load `dir 0.22s`,
  `child 0.27s`.
- t283 500k: wall `48.35s`, load `38.47s`, RSS `360.6 MiB`,
  spool `42.3 MiB`, dir/child rows `1363994`, collapsed load `25s`.
- scratch127 500k: wall `27.44s`, load `20.09s`, RSS `375.6 MiB`,
  spool `34.2 MiB`, dir/child rows `621557`, collapsed load `12s`.

Mandatory pass/fail guardrail samples are t283 200k, scratch127 200k, and
scratch125 500k or an equivalent dense Lustre sample. t283 500k and
scratch127 500k remain required evidence fixtures for reports and review.

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given recorded production-shaped tree-only spool baselines, final-gate
   input must include exactly the four required evidence fixtures above with
   matching dataset, cap, wall, load, RSS, spool bytes, full-filter rows, and
   full-filter load evidence.
2. Given the recorded direct t283 200k import baseline, final-gate input must
   show wall `22.17s`, max RSS `926.7 MiB`, `dir_facts == 59778`, dir rows
   `1196726`, child rows `1196726`, and full-filter insert evidence
   `dir 6.45s`, `child 5.65s`.
3. Given the recorded direct scratch127 200k import baseline, final-gate input
   must show wall `2.07s`, max RSS `311.9 MiB`, `dir_facts == 2289`, dir rows
   `26883`, child rows `26883`, and full-filter insert evidence `dir 0.18s`,
   `child 0.12s`.
4. Given the recorded direct scratch125 500k import baseline, final-gate input
   must show wall `24.39s`, max RSS `859.2 MiB`, `dir_facts == 87186`, dir
   rows `1255727`, child rows `1255727`, and full-filter insert evidence
   `dir 5.85s`, `child 5.26s`.
5. Given local pre-fix reproduction does not show both full-filter tables with
   equal non-zero rows and combined full-filter load `>= 20s`, then final gate
   status is `"blocked"` with reason `"failure class not reproduced"`.

### D2: Summarise Speed and Spool Size Gates

As an operator, I want bounded summarise runs to prove material speed recovery,
so that the fix attacks wall time, not only a unit benchmark.

Recorded prototype evidence used to set thresholds:

- t283 200k: wall `41.24s -> 27.89s`, load `32.21s -> 22.19s`,
  spool `30.4 MiB -> 20.6 MiB`, child `10.98s -> 1.43s`.
- scratch127 200k: wall `3.58s -> 3.11s`, load `2.23s -> 1.99s`,
  spool `6.42 MiB -> 6.11 MiB`, child `0.27s -> 0.044s`.
- scratch125 500k: wall `36.08s`, load `27.49s`, spool `50.7 MiB`,
  child `1.43s` for `1255727` rows; no spool baseline was recorded.

All `summarise -t` gates below are tree-only. Required pass/fail gates:

- t283 200k production-shaped `summarise -t`:
  - end-to-end wall `<= 30.93s`;
  - spool load wall `<= 24.16s`;
  - child phase `<= 2.50s`;
  - spool bytes `<= 22.80 MiB`.
- scratch127 200k:
  - end-to-end wall `<= 3.94s`;
  - spool load wall `<= 2.46s`;
  - max RSS `<= 180 MiB`;
  - spool bytes `<= 6.75 MiB`.
- scratch125 500k or equivalent dense Lustre sample:
  - derived child rows equal dir rows;
  - child phase `<= 2.50s`;
  - end-to-end wall `<= 40.00s`;
  - spool load wall `<= 30.25s`;
  - max RSS `<= 1.10 GiB`;
  - spool bytes `<= 53.30 MiB`.

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given a final-gate report with t283 200k wall `30.93s`, spool load
   `24.16s`, child phase `2.50s`, and spool bytes `22.80 MiB`, then the t283
   summarise-speed check passes.
2. Given any t283 value above those thresholds by `0.01s` or `1 byte`, then
   the t283 summarise-speed check fails with the failing metric name.
3. Given scratch127 and scratch125 reports at the thresholds above, then the
   Lustre no-regression checks pass.
4. Given scratch127 wall `3.95s` or scratch125 RSS `1.11 GiB`, then the Lustre
   check fails with dataset and metric names.
5. Given prototype evidence fixtures are loaded, then final-gate evidence
   records exactly the three prototype entries above, including t283 child
   phase `10.98s -> 1.43s`, scratch127 child phase `0.27s -> 0.044s`, and
   scratch125 child phase `1.43s` for `1255727` rows.

### D3: Query Correctness and Speed Guardrails

As a query user, I want query results and read volume unchanged, so that
summarise speed recovery does not undo schema3 query improvements.

Run before/after `clickhouse-perf query` on:

- t283 root or mount-root path;
- scratch125 high-fanout VCFS path;
- one normal Lustre path, preferably scratch127.

Result counts and digests must match. For p50, p95, read rows, read bytes, and
read marks, after-fix values must be `<= max(before * 1.10, before + 100ms)`
for time metrics and `<= before * 1.10` for read metrics. The scratch125
60s+ high-fanout path may remain slow, but must not worsen by more than 10%.

**Package:** `internal/chperf/`
**File:** `internal/chperf/query.go`, `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/query_test.go`,
`internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given before/after query reports with identical counts and digests and p95
   increasing from `60000ms` to `66000ms`, then scratch125 guardrail passes.
2. Given the same reports with p95 `66001ms`, then scratch125 guardrail fails.
3. Given any compared operation has a result digest mismatch, final gate fails
   with `result digest mismatch`.
4. Given t283 root queries remain low-ms to sub-second and read metrics are
   within 10%, then the t283 query guard passes.

## Implementation Order

1. Baseline/reproduction gate: encode D1 final-gate evidence and blocked
   behavior. Stop if t283 200k failure class cannot be reproduced locally.
2. Canonical spool: implement A1 with unit tests for manifest rows and schema3
   readiness counts.
3. Loader derivation: implement A2, A4, and A5, including failure ordering,
   retry resume, cleanup, loaded-row evidence, and no-regression publish
   behavior.
4. Direct import parity: implement A3 and split direct import phases.
5. Telemetry and guardrails: implement B1 and B2 in reports and final gates.
6. Fast spool verification: implement C1 after hash/size tests fail first.
7. Parse diagnostics: implement B3.
8. Perf gates: run bounded D2 and D3 samples. Query gates may run in parallel
   after phase 4; final pass is sequential because it consumes all evidence.

## Appendix: Key Decisions

- Keep `wrstat_dir_filter_all` and `wrstat_child_filter_all` visible in the
  final schema. No phase may remove either table without a separate spec that
  proves equivalent or better query behavior.
- Derive child rows before readiness because async post-publish full-filter
  build would change product behavior.
- Keep durable spool semantics: completed-spool retry skips parsing and resumes
  post-spool publish phases through `post_spool_publish_state.json`.
- Trust row counts only when disk manifest table rows match writer-close
  `expected.Tables`; identity, schema/version, byte size, and SHA256 checks
  must also pass before reuse.
- Tests use GoConvey. Each acceptance test needs a corresponding `Convey`
  block with explicit `So()` assertions.
- Performance tests are bounded. Do not run full production mounts or long
  perf jobs during implementation.

Non-goals for this spec:

- Do not replace the two tables with one table plus a ClickHouse projection.
- Do not use fanout-thresholded child rows as the t283 fix.
- Do not blanket-convert full-filter path columns to LowCardinality.
- Do not replace child rows with parent packet rows.
- Do not replace row-per-tuple full-filter tables with per-directory vectors.
- Do not implement an AgeAll-only eager full-filter layer.
- Do not sparse out `wrstat_dir_filter_all` rows without a query contract.
- Do not reintroduce direct ClickHouse loading during parse before this fix.
- Do not publish before full-filter readiness without product approval.
- Do not include memory-budgeted larger full-filter batch caps in this phase.
  Treat them as follow-up candidates only. Recorded child-derivation evidence
  shows t283 200k improving from `27.89s` and `386.8 MiB` at cap `15.3k` to
  `24.54s` and `758.9 MiB` at `25k`, and to `22.89s` and `1026.3 MiB` at
  `50k`; scratch127 200k at `50k` was `2.82s` and `142.3 MiB`. Do not make
  `50k` unconditional.
- Do not include compact child-presence tables, integer directory IDs,
  RowBinary/zstd spool, parallel table loads, high-fanout query side indexes,
  query fallback work, query packet caching, immutable sidecar/external
  navigation artifacts, external sort/reduce navigation artifacts, or adaptive
  per-mount strategy selection in this spec.
