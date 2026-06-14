# Prompt for ClickHouse summarise performance investigation

Use this before writing a follow-up summarise-speed spec in `.docs/summarise`.

The goal is to recover production `summarise` wall time for
`/nfs/t283_imaging/` after schema3, without regressing Lustre summarise speed
or the query-speed gains schema3 was added to provide. The current production
symptom is concrete: summarising
`~/output/nfs/20260517-170004_／nfs／t283_imaging/` has exceeded 21 hours, where
the same mount previously took about 10 hours.

Do not hand wave this. If a bounded local subset cannot reproduce the same
class of failure, stop and say so. A candidate chosen without a local failure
mode is likely to optimize the wrong part of the pipeline.

## What Is Already Implemented

The current branch includes the durable ClickHouse spool path from the recent
summarise fixes:

- `wrstat-ui summarise` builds a durable output-local spool under
  `.wrstat-ui-clickhouse-spool` before loading ClickHouse.
- Completed spool retries skip reparsing `stats.gz` and resume post-spool
  publish phases through `post_spool_publish_state.json`.
- Post-spool publish still runs active virtual readiness, mount switch, old
  snapshot cleanup, old active virtual cleanup, tree summary refresh,
  active-prefix refresh, table evidence/reporting, and completion marker
  creation.
- Zero-record active-virtual publish has a guarded fast path, but normal
  nonzero mounts still materialize the full active virtual overlay.
- Basedirs retry cleanup/replay was grouped and batched, but the timings in
  this investigation use `-t` only and do not include basedirs work.
- Schema3 spool output now includes:
  - `wrstat_child_filter_all`;
  - `wrstat_dir_filter_all`;
  - `wrstat_schema3_snapshot_sets`;
  - `wrstat_active_virtual_summaries`;
  - `wrstat_active_virtual_filter_all`;
  - `wrstat_active_virtual_children`;
  - `wrstat_active_virtual_sets`.

Important current implementation details:

- `cmd/summarise_spool.go` writes each full-filter row twice:
  `writeSchema3FullFilterRow` writes `wrstat_dir_filter_all`, then writes the
  same payload as `wrstat_child_filter_all`.
- `clickhouse/dir_filter_all.go` does the same for direct ClickHouse import:
  `flushLastPending` appends every full-filter row to both `dirWriter` and
  `childWriter`.
- Before this investigation, `clickhouse/summarise_spool_loader.go` loaded the
  two full-filter spool files separately but reported both under one phase
  name, `wrstat_filter_all_insert`; this pass split that telemetry.
- `internal/chspool.VerifyManifest` decodes every table to count rows when
  reusing an existing complete spool. The first successful build/load path
  does not perform that same full-table manifest verification after writing the
  manifest, but retries do.

Do not repeat old fixes as if they are new. In particular, do not propose only:

- increasing the normal query timeout;
- relying on scheduler retries;
- repairing stale prepared batches;
- streaming child names out of `summary/dirguta`;
- making post-spool publish resumable;
- making zero-record active virtual refresh faster;
- making basedirs cleanup/replay more batched;
- adding cache warming as the answer;
- dropping schema3 query tables without proving equivalent query speed.

Those can be ingredients, but the next design must attack the measured
schema3-era row and write amplification.

## Local Proof From This Pass

This file was seeded with bounded proof runs on 2026-06-14. The full production
21-hour run was not attempted. The same class of failure was reproduced locally:
t283 prefixes spend most of their time writing/loading schema3 full-filter
tables, and bounded Lustre samples show the same mechanism can also affect
Lustre when a prefix is directory/filter dense.

Investigation status: complete enough to choose the next implementation target.
The proof did not fail. The corrected pass included temporary prototype code
for the highest-signal candidates, not only passive measurement. The candidate
matrix below records the outcome for each idea: measured now, carried forward
from prior schema3 evidence, rejected with evidence, or not runnable without
becoming a separate feature implementation.

Artifacts:

- Mounts file: `.tmp/agent/summarise/mounts.txt`
- Symlinked dataset dirs: `.tmp/agent/summarise/subsets/`
- Bounded stats prefixes: `.tmp/agent/summarise/inputs/`
- Perf reports: `.tmp/agent/summarise/perf/`
- Transient summarise outputs/spools were created under
  `.tmp/agent/summarise/outputs*` during measurement, then removed after the
  reports needed for final comparisons were copied into `perf/`.
- The temporary binary `.tmp/agent/summarise/wrstat-ui` was removed after
  verification to save space; rebuild it with the command in this file when
  continuing the investigation.

Temporary telemetry/prototype code was used during this investigation, then
removed from the tracked source tree:

- Direct import reports recorded `wrstat_child_filter_all` and
  `wrstat_dir_filter_all` in `rows_per_table` and selected table stats.
- Direct import reports split `wrstat_child_filter_all_insert` and
  `wrstat_dir_filter_all_insert`.
- Spool load reports split `wrstat_child_filter_all_insert` and
  `wrstat_dir_filter_all_insert`.
- `spool_load_total` reports included `table_load_metrics` with per-table
  rows, spool bytes, phase duration, rows/sec, and bytes/sec.
- `WRSTAT_UI_EXPERIMENT_DERIVE_CHILD_FILTER_ALL=1` skipped writing the child
  full-filter spool file, loaded `dir_filter_all`, and populated
  `child_filter_all` with server-side `INSERT INTO ... SELECT`.
- `WRSTAT_UI_EXPERIMENT_FAST_SPOOL_VERIFY=1` skipped gob decode during
  completed-spool manifest verification after byte hash/size checks.
- A temporary batch-cap patch raised `defaultProjectionBatchSize` from 15,300
  to 25k and 50k for bounded timing probes, then was reverted.

### Direct Import Evidence

Direct `clickhouse-perf import` avoids the output-local gob spool, so it
isolates current ClickHouse writer/schema costs.

| Dataset | Cap | Wall | Max RSS | `dir_facts` | `dir_filter_all` | `child_filter_all` | `dir_filter_all_insert` | `child_filter_all_insert` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| t283 | 200k | 22.17s | 926.7 MiB | 59,778 | 1,196,726 | 1,196,726 | 6.45s | 5.65s |
| scratch127 | 200k | 2.07s | 311.9 MiB | 2,289 | 26,883 | 26,883 | 0.18s | 0.12s |
| scratch125 | 500k | 24.39s | 859.2 MiB | 87,186 | 1,255,727 | 1,255,727 | 5.85s | 5.26s |

The 200k t283 direct import is about 10.7x slower than the 200k scratch127
direct import. The full-filter tables are the largest new schema3 cost:
t283 writes about 2.39M duplicated full-filter rows from 200k input records.

The scratch125 500k run matters because it proves this is not only an NFS
issue. A Lustre mount with dense/high-fanout directory structure can also
generate more than 2.5M duplicated full-filter rows from a bounded prefix.

### Production-Shaped Summarise Spool Evidence

These runs used the actual `wrstat-ui summarise` spool path with `-t`, so they
exercise parsing, gob+gzip spool writing, ClickHouse replay, readiness, and
publish. They intentionally omit basedirs to keep the proof focused on
schema3 tree/summarise cost.

| Dataset | Cap | End-to-end wall | Spool load wall | Max RSS | Spool bytes | `dir_filter_all` | `child_filter_all` | `dir_filter_all_insert` | `child_filter_all_insert` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| t283 | 200k | 41.24s | 32.21s | 372.8 MiB | 30.4 MiB | 1,196,726 | 1,196,726 | 11.02s | 10.98s |
| scratch127 | 200k | 3.58s | 2.23s | 146.9 MiB | 6.42 MiB | 26,883 | 26,883 | 0.22s | 0.27s |
| t283 | 500k | 48.35s | 38.47s | 360.6 MiB | 42.3 MiB | 1,363,994 | 1,363,994 | collapsed 25s | collapsed 25s |
| scratch127 | 500k | 27.44s | 20.09s | 375.6 MiB | 34.2 MiB | 621,557 | 621,557 | collapsed 12s | collapsed 12s |

The 200k production-shaped t283 run is about 11.5x slower than the matching
scratch127 run. The 500k results show prefix distribution matters: the first
200k t283 records are especially directory/filter dense, while scratch127
becomes much denser later in the prefix. A fix must therefore be tested on
both t283 and multiple Lustre prefixes, not just on one root.

The progress log reports `records=0` for these 200k/500k spool runs because
progress currently fires every 1M rows and `logParseResult` only reports the
last progress value. Production-sized runs still emit progress every 1M rows,
but parse-complete logs can under-report the final partial interval.

Per-table spool load metrics show why the duplicated child table is a strong
target. In the t283 200k run:

- `wrstat_dir_filter_all`: 1,196,726 rows, 9.42 MB spool bytes, 11.02s load,
  about 108.6k rows/sec.
- `wrstat_child_filter_all`: 1,196,726 rows, 9.82 MB spool bytes, 10.98s load,
  about 109.0k rows/sec.
- `wrstat_parent_facts`: 59,778 rows, 3.25 MB spool bytes, 3.69s load.
- `wrstat_files`: 200,000 rows, 3.37 MB spool bytes, 0.85s load.

### Query Guardrail Evidence

The same datasets were probed with `clickhouse-perf query` so summarise-speed
fixes do not silently discard the query structures schema3 needs.

| Dataset/path | Operation | p50 | p95 | Read evidence |
|---|---|---:|---:|---|
| t283 200k `/nfs/t283_imaging/` | `tree_disktree_endpoint_cold_provider` | 49 ms | 50 ms | fast |
| t283 200k `/nfs/t283_imaging/` | `tree_where_cold_provider` | 395 ms | 406 ms | result count 92 |
| t283 200k `/nfs/t283_imaging/` | `dirinfos_broad` | 9 ms | 31 ms | first sample 23k rows, 2.6 MB |
| t283 200k `/nfs/t283_imaging/` | `dirshavechildren_broad` | 5 ms | 6 ms | 232 rows |
| scratch125 500k high-fanout VCFS | `tree_disktree_endpoint_cold_provider` | 258 ms | 261 ms | 11,205 children |
| scratch125 500k high-fanout VCFS | `tree_where_cold_provider` | 62.0s | 62.7s | result count 22,411 |
| scratch125 500k high-fanout VCFS | `dirinfos_broad` | 64.5s | 115.7s | up to 232M rows, 42.3 GB |
| scratch125 500k high-fanout VCFS | `dirshavechildren_broad` | 60.3s | 61.7s | 59.5M rows, 19.2 GB |

The scratch125 query results reproduce the older schema3 high-fanout query
failure too. That is not the primary summarise-speed regression, but it is a
hard guardrail: any summarise-speed simplification must either preserve
schema3 query correctness/speed where it already works, or explicitly replace
the slow high-fanout route with a faster structure.

### Candidate Probe Results

Cheap manual probes were run first, followed by temporary implementation
prototypes. The implementation prototypes are the meaningful evidence for the
next fix.

**Completed-spool retry verification.** Re-running `summarise
--clickhouse-recover` against already-complete 200k spools, with the active
snapshot already published, measured the cost of `completeSummariseSpool`
manifest verification plus active-snapshot preflight:

| Dataset | Existing spool bytes | Retry/no-op wall | Notes |
|---|---:|---:|---|
| t283 200k | 30.4 MiB | 7.50s | hashes and decodes the full completed spool before no-op |
| scratch127 200k | 6.42 MiB | 0.63s | same path on much smaller full-filter spool |

This supports the candidate to avoid full gob decode during completed-spool
manifest verification. It will not fix first-attempt wall time, but it can
make retries of large t283 spools much cheaper.

**Server-side child-filter derivation.** A manual ClickHouse prototype created
`wrstat_child_filter_all_copy AS wrstat_child_filter_all`, then populated it
from `wrstat_dir_filter_all` with:

```sql
INSERT INTO wrstat_child_filter_all_copy
SELECT mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir, count,
       size, atime_min, mtime_max, atime_buckets, mtime_buckets,
       filter_child_count, child_count, has_filter_children, has_children,
       refreshed_at
FROM wrstat_dir_filter_all
```

| Dataset | Rows copied | Server-side copy wall | Current gob child load |
|---|---:|---:|---:|
| t283 200k | 1,196,726 | 1.59s | 10.98s |
| scratch127 200k | 26,883 | 0.11s | 0.27s |

This is the strongest low-risk candidate from this pass. It preserves the
same final physical child table, but avoids writing, verifying, decoding, and
native-appending a second large gob file. On t283 200k it would plausibly save
about 9s from spool replay and about 9.8 MiB from the spool, before any
manifest-verification savings.

### Temporary Implementation Prototype Results

**Prototype A: derive `wrstat_child_filter_all` from `wrstat_dir_filter_all`
during spool load, and do not write the child gob file.**

This was implemented behind `WRSTAT_UI_EXPERIMENT_DERIVE_CHILD_FILTER_ALL=1`.
The final physical child table was still present before schema3 readiness and
query paths saw the same table shape.

| Dataset | Cap | Baseline wall | Prototype wall | Baseline spool load | Prototype spool load | Spool bytes | Child phase |
|---|---:|---:|---:|---:|---:|---:|---:|
| t283 | 200k | 41.24s | 27.89s | 32.21s | 22.19s | 30.4 MiB -> 20.6 MiB | 10.98s -> 1.43s |
| scratch127 | 200k | 3.58s | 3.11s | 2.23s | 1.99s | 6.42 MiB -> 6.11 MiB | 0.27s -> 0.044s |
| scratch125 | 500k | no spool baseline | 36.08s | no spool baseline | 27.49s | 50.7 MiB | 1.43s for 1,255,727 derived rows |

Prototype A is effective and should be implemented properly. It materially
improves the t283 failure class, does not regress the normal scratch127
sample, and preserves query table shape. The scratch125 run confirms the
child derivation stays cheap even at 1.25M full-filter rows, while the old
high-fanout query path remains slow for unrelated reasons.

Query guardrails after Prototype A:

| Dataset/path | Operation | p50 | p95 | Comparison |
|---|---|---:|---:|---|
| t283 200k `/nfs/t283_imaging/` | `tree_where_cold_provider` | 450 ms | 463 ms | same sub-second class as baseline |
| t283 200k `/nfs/t283_imaging/` | `tree_disktree_endpoint_cold_provider` | 60 ms | 64 ms | same class as baseline |
| scratch127 200k `/lustre/scratch127/` | `tree_disktree_endpoint_cold_provider` | 41 ms | 42 ms | no obvious regression |
| scratch125 500k high-fanout VCFS | `tree_where_cold_provider` | 61.7s | 61.8s | same known slow path as baseline |
| scratch125 500k high-fanout VCFS | `dirinfos_broad` | 61.9s | 116.7s | same known slow path/read volume |
| scratch125 500k high-fanout VCFS | `dirshavechildren_broad` | 62.2s | 62.2s | same known slow path/read volume |

**Prototype B: completed-spool fast verify.**

This was implemented behind `WRSTAT_UI_EXPERIMENT_FAST_SPOOL_VERIFY=1`.
It still checked manifest identity plus file size/SHA256, but skipped gob
decode row counting when reusing an already complete spool.

| Dataset | Spool | Full verify retry | Fast verify retry |
|---|---:|---:|---:|
| t283 200k derived spool | 20.6 MiB | 5.25s | 0.11s |
| scratch127 200k derived spool | 6.11 MiB | 0.70s | 0.06s |

Prototype B is effective for retries, not first-attempt summarise wall time.
It should be paired with a manifest/schema change that makes the row counts
trusted from the initial writer close, rather than a bare environment flag.

**Prototype C: raise full-filter/projection insert batch cap.**

The default full-filter/projection batch cap is 15,300 rows. Temporary patches
tested 25k and 50k while also using Prototype A.

| Dataset | Batch cap | End-to-end wall | Spool load | Max RSS | `dir_filter_all_insert` |
|---|---:|---:|---:|---:|---:|
| t283 200k | 15.3k | 27.89s | 22.19s | 386.8 MiB | 10.82s |
| t283 200k | 25k | 24.54s | 17.85s | 758.9 MiB | 6.48s |
| t283 200k | 50k | 22.89s | 17.12s | 1,026.3 MiB | 5.96s |
| scratch127 200k | 15.3k | 3.11s | 1.99s | 133.3 MiB | 0.206s |
| scratch127 200k | 50k | 2.82s | 1.76s | 142.3 MiB | 0.147s |

Prototype C is a real speed lever but not a free fix. A static 50k default is
too memory-hungry for production safety. A proper implementation could expose
or auto-tune the cap with a memory budget, or apply it only to smaller
bounded tables. Prototype A should land first; batch tuning can be a guarded
follow-up.

### Recommendation

The highest-confidence next implementation is Prototype A, plus Prototype B
for retry ergonomics:

1. Keep writing and loading `wrstat_dir_filter_all` as the canonical
   full-filter spool table.
2. Stop writing `wrstat_child_filter_all` to the gob spool.
3. Populate `wrstat_child_filter_all` inside ClickHouse with
   `INSERT INTO ... SELECT` from `wrstat_dir_filter_all` before schema3
   readiness is made visible.
4. Keep row-count/table evidence for the derived child table so reports still
   prove both physical tables are present.
5. Optimize completed-spool retry verification after that by avoiding a full
   gob decode when the manifest already has trusted row counts and byte
   hashes.
6. Treat batch-cap increases as optional and memory-budgeted, not as an
   unconditional default change.

Do not start with RowBinary, projections, sidecars, or async background
publish. They may be good follow-up ideas, but the server-side child derivation
probe is already measured, preserves the final table shape, and targets the
largest duplicated cost without changing query semantics.

## Ground Rules

- Keep prototype code, scratch SQL, reports, temporary binaries, and notes in
  `.tmp/agent/summarise/`.
- Use a unique ClickHouse database per experiment.
- Do not summarise complete example mounts unless the user explicitly asks for
  a long production-scale run. Use bounded prefixes, selected mounts, or
  synthetic fanout only when the synthetic data preserves the measured row
  amplification.
- Always test at least one t283 subset and at least one Lustre subset. Include
  scratch125 high-fanout query checks whenever a candidate changes full-filter
  or parent/child query tables.
- Measure both direct import and production-shaped `summarise` spool when the
  candidate changes writer/schema shape. Direct import alone can miss gob
  spool/replay costs; spool alone can hide writer costs behind replay.
- Record wall time, user/system CPU, max RSS, spool bytes, table row counts,
  active parts, compressed/uncompressed bytes, per-phase durations, and query
  p50/p95/p99.
- Preserve split telemetry for `wrstat_child_filter_all` and
  `wrstat_dir_filter_all`. The old shared `wrstat_filter_all_insert` bucket
  was too coarse.
- Query-speed proof must use `clickhouse-perf query` or `rest`, not only unit
  tests. Include result counts/digests and read rows/bytes/marks when
  available.
- Do not recommend a candidate that helps t283 by making the known
  scratch125 high-fanout query path worse.
- Do not recommend a candidate that only moves the risky part after publish
  without durable retry semantics.

Useful command shapes:

```bash
timeout 300s env CGO_ENABLED=1 go build -o .tmp/agent/summarise/wrstat-ui .

timeout 20m /usr/bin/time -v .tmp/agent/summarise/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_summarise_<name>&compress=lz4" \
  -D wrstat_summarise_<name> \
  --mounts .tmp/agent/summarise/mounts.txt \
  --query-timeout 180s \
  --json .tmp/agent/summarise/perf/<name>-import.json \
  import .tmp/agent/summarise/subsets/t283-only \
  --maxLines 200000 --batchSize 100000 --parallelism 1

timeout 30m /usr/bin/time -v .tmp/agent/summarise/wrstat-ui summarise \
  -t .tmp/agent/summarise/outputs/<dataset>/dguta.dbs \
  -C "clickhouse://localhost:9000?database=wrstat_summarise_<name>&compress=lz4" \
  -D wrstat_summarise_<name> \
  --query-timeout 180s \
  --mounts .tmp/agent/summarise/mounts.txt \
  .tmp/agent/summarise/inputs/<prefix>/stats.gz

timeout 15m .tmp/agent/summarise/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_summarise_<name>&compress=lz4" \
  -D wrstat_summarise_<name> \
  --mounts .tmp/agent/summarise/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/summarise/perf/<name>-query.json \
  query --dir /lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/ \
  --ancestor-dir /lustre --ancestor-limit 16 --repeat 3 --warmup 0 \
  --ops tree_disktree_endpoint_cold_provider,tree_where_cold_provider,tree_where_cold_then_cached,dirinfos_broad,dirshavechildren_broad
```

## Completed Investigation Checklist

This checklist is the work completed in this investigation pass.

- [x] Read `.docs/schema3/investigate.md` and `.docs/schema3/spec.md`.
  Result: schema3 deliberately added exact full-filter tables to fix cold
  query failures, but the spec left import/summarise wall budget as measured
  rather than known.
- [x] Read recent summarise bug fixes.
  Result: failures moved from stale batches, memory retention, and non-durable
  publish into a durable spool flow. The current issue is now speed, especially
  schema3 full-filter row/write amplification.
- [x] Build current binary in `.tmp/agent/summarise/wrstat-ui`.
  Result: `go build` succeeded.
- [x] Create bounded local t283 and Lustre inputs without full-mount
  summarise.
  Result: symlinked datasets and 200k/500k prefix `stats.gz` files under
  `.tmp/agent/summarise/`.
- [x] Reproduce a local summarise-speed failure class.
  Result: 200k t283 production-shaped spool took 41.24s vs 3.58s for 200k
  scratch127; full-filter rows were 1,196,726 per table vs 26,883 per table.
- [x] Test Lustre speed guardrails.
  Result: scratch125 500k direct import also generated 1,255,727 rows per
  full-filter table and spent 11.11s across
  `wrstat_dir_filter_all_insert` and `wrstat_child_filter_all_insert`;
  scratch127 500k spool generated 621,557 rows per full-filter table and
  spent about 12s in the old collapsed phase.
- [x] Test query-speed guardrails with the perf subcommand.
  Result: t283 200k root-ish tree queries stayed low-ms to sub-second, while
  scratch125 high-fanout `where`, `DirInfos`, and `DirsHaveChildren`
  remained 60s+ and read tens of GB.
- [x] Prototype the highest-confidence summarise-speed fix with temporary
  code.
  Result: derive-child spool load improved t283 200k wall from 41.24s to
  27.89s and scratch127 200k from 3.58s to 3.11s.
- [x] Run high-fanout query guardrail after the prototype.
  Result: scratch125 500k high-fanout query remained in the same known slow
  class; the prototype did not fix it, but did not obviously regress it.
- [x] Split full-filter telemetry by table and by direct writer vs spool
  loader.
  Result: temporary direct import and spool load reports recorded separate
  `wrstat_child_filter_all_insert` and `wrstat_dir_filter_all_insert`
  durations.
- [x] Add per-table spool-load perf report metrics.
  Result: temporary `spool_load_total` reports recorded rows, bytes, phase
  duration, rows/sec, and bytes/sec per manifest table.
- [x] Decide whether 1M t283/Lustre prefixes are needed after telemetry split.
  Result: deferred to conserve disk and wall time. The 200k t283 and 500k
  scratch125 samples already produced more than 1.2M rows per full-filter
  table and were sufficient to prove the duplicated-table failure class plus
  the server-side derivation candidate.

## Candidate Investigation Matrix

This section is not a deferred backlog. Each idea is classified by what was
measured in this pass, what was carried forward from `.docs/schema3`, or why
the idea was not runnable without becoming a separate feature implementation.

| Candidate | Status | Evidence | Decision |
|---|---|---|---|
| Split full-filter telemetry by table | Measured with temporary code | t283 200k spool load split into `dir_filter_all` 11.02s and `child_filter_all` 10.98s; direct import split into 6.45s and 5.65s. | Required instrumentation for the fix. |
| Per-table spool-load metrics | Measured with temporary code | `table_load_metrics` recorded rows, bytes, duration, rows/sec, and bytes/sec for each manifest table. | Keep in the proper implementation. |
| Full-filter row amplification guard | Measured enough to set a budget | t283 200k produced 2.39M duplicated full-filter rows from 200k input rows; scratch125 500k produced 2.51M from 500k. | Add a perf guard; suggested warning budget is duplicated rows/input row > 5, hard-fail or waiver > 10. |
| Final partial progress records | Investigated as diagnostics, not a speed fix | All bounded spool logs report `records=0` because progress is emitted every 1M rows and the final partial interval is not copied into the parse-complete log. | Fix diagnostics, but it will not improve summarise wall time. |
| End-to-end summarise phase report | Partially investigated | `/usr/bin/time`, log phase timestamps, `spool_load_total`, retry probes, and table metrics were enough to isolate the bottleneck. A first-class report would reduce manual parsing. | Useful instrumentation, not required to choose the speed fix. |
| Derive `child_filter_all` from `dir_filter_all` during load | Measured with temporary code | t283 200k wall 41.24s -> 27.89s; spool bytes 30.4 MiB -> 20.6 MiB; child phase 10.98s -> 1.43s. scratch127 200k wall 3.58s -> 3.11s. | Primary fix. |
| Completed-spool fast verification | Measured with temporary code | t283 200k derived-spool no-op recover 5.25s -> 0.11s; scratch127 0.70s -> 0.06s after skipping gob decode and keeping byte hash/size checks. | Implement with manifest semantics that trust initially counted rows. |
| Larger full-filter batch cap | Measured with temporary code | With derive-child, t283 200k wall 27.89s at 15.3k cap, 24.54s at 25k, 22.89s at 50k; RSS rose from 386.8 MiB to 758.9 MiB and 1,026.3 MiB. | Useful only behind memory budget/adaptive cap. Do not make 50k unconditional. |
| One physical table plus ClickHouse projection | Measured in ClickHouse | Parent-ordered projection on t283 200k stored 12.92 MiB base + 5.74 MiB projection, versus 12.90 MiB `dir_filter_all` + 4.52 MiB `child_filter_all`; insert was 2.26s from server-side copy. | Reject as summarise/storage fix; it moves duplication into projection storage and does not beat derive-child. |
| Generate child rows only above fanout threshold | Measured in ClickHouse | t283 200k threshold 32 still keeps 94.05% of child rows; threshold 64 keeps 0 because max fanout is 36. scratch125 500k threshold 64 keeps 32.03%, threshold 1024 keeps 15.83%. | Reject as primary t283 fix; possibly useful only for scratch125-like high-fanout side index. |
| Compact child-presence table | Measured in ClickHouse | t283 presence rows 69,118, 146.9 KiB compressed versus 1,196,726 child rows, 4.52 MiB; scratch125 presence rows 488,541, 4.99 MiB versus 16.98 MiB. | Promising for `DirsHaveChildren`, but not a replacement for full child summaries. Pair with derive-child or query rewrite. |
| LowCardinality path columns | Measured in ClickHouse | On t283, child LC table compressed 4.52 MiB -> 2.82 MiB, but dir LC table worsened 12.90 MiB -> 13.42 MiB. | Do not apply broadly; maybe child-only storage tweak after the primary fix. |
| Integer directory IDs | Measured in ClickHouse | Repeated path bytes/dictionary bytes: t283 39.5x, scratch125 24.7x. ID-coded `dir_filter_all` plus dictionary compressed t283 13.46 MiB vs 12.90 MiB original; scratch125 19.38 MiB vs 38.66 MiB original. | Promising for scratch125/string-heavy cases, mixed for t283. Needs query/schema work and is not the first fix. |
| RowBinary/zstd table-native spool | Measured with ClickHouse export/import | t283 200k `dir_filter_all` RowBinary zstd file 9.36 MiB, import 2.43s; child file 3.45 MiB, import 2.45s. Import RSS was about 1.0 GiB. | Promising load-speed path, but memory-heavy and larger engineering change than derive-child. |
| Parallel independent table loads | Measured with server-side copy approximation | t283 copy of files/children/facts/parent/full-filter tables: sequential 3.74s, parallel 1.70s. This omits gob decode, so it is only a contention probe. | Worth a controlled follow-up after derive-child; not enough proof alone. |
| Parent packet rows instead of child rows | Measured in ClickHouse and compared with schema3 evidence | t283 child rows 1,196,726 -> 3,407 packet rows, but compressed size 4.52 MiB -> 4.09 MiB. scratch125 1,255,727 -> 34,506 rows, size 16.98 MiB -> 16.98 MiB. Prior schema3 packet table did not beat parent facts for query. | Row-count win but not storage/summarise-speed win by itself; keep as query-structure idea, not primary summarise fix. |
| Per-directory filter vectors instead of row-per-tuple tables | Investigated via current table stats and query guardrails | Existing vector tables are already compact (`dir_facts` 59,778 rows/7.84 MiB on t283; 87,186 rows/13.35 MiB on scratch125), but scratch125 high-fanout `DirInfos` still reads tens of GB and takes 60s+. | Vectors alone are not sufficient; query routing/packet caching remains required. |
| Store only AgeAll eagerly | Measured in ClickHouse | Age 0 rows are 10% of t283 full-filter rows and 21.32% of scratch125 rows. Age-specific rows dominate volume. | Large potential write reduction, but it changes age-filter readiness unless age-specific data is lazy or compact; not safe as a standalone fix. |
| Exact subtree summaries only where needed | Partially investigated | `dir_filter_all` is the larger remaining table after derive-child. Age-specific and path-ID probes show where volume comes from, but no safe query-equivalent sparse rule was found. | Do not implement without a query contract proving when omitted subtree rows are safe. |
| High-fanout side index | Measured enough to scope | scratch125 threshold data shows a side index for parents >=64 children would cover 32% of child rows and the 11,205-child VCFS case; t283 thresholding does not help. | Useful for high-fanout query fixes, not the t283 summarise regression. |
| Revisit schema3 query fallbacks | Measured | scratch125 high-fanout remains 60s+ after derive-child, proving summarise-speed simplification did not solve that older query issue. | Separate query-fix stream; do not block primary summarise-speed fix. |
| Row-building allocation cleanup | Investigated by timing shape | Parse phase on t283 200k was about 6-9s, while load/full-filter phases dominated. No allocation prototype was run because the measured bottleneck is ClickHouse load/write amplification. | Lower priority; revisit after row/write amplification is fixed. |
| Share row-building code | Investigated as maintainability | Direct writer and spool writer duplicate logic, but this does not explain the 21h regression. | Do as part of implementation hygiene if touching both paths, not as a performance fix. |
| Block-indexed spool files | Not run; scoped by fast verify and RowBinary probes | Fast verify removes retry decode cost; RowBinary points to a different table-native format. A block index would mainly help resumable partial table loads, which were not the measured bottleneck in bounded first-attempt runs. | Defer unless partial-table resume becomes a production failure mode. |
| Hybrid parse plus direct ClickHouse load | Not run; rejected for this pass on durability semantics | Recent bug fixes deliberately moved to durable spool before ClickHouse load. Reintroducing direct ClickHouse interaction during parse needs rollback/retry design, not a quick timing probe. | Do not pursue before derive-child; risk is high. |
| Async full-filter build after broad publish | Not run; requires product semantics | It can reduce user-visible wall time only if filtered queries may be temporarily unavailable or slower after publish. That is a behavior/product decision, not only a performance experiment. | Do not choose without explicit acceptance of degraded filtered-query readiness. |
| Immutable sidecar / resurrect Bolt as sidecar exporter | Carried forward from schema3 investigation; not rebuilt here | `.docs/schema3/investigate.md` modeled sidecars and prior Bolt evidence, but notes no same-subset high-fanout sidecar run. Building a correct active-set sidecar is a phase-sized feature. | Keep as fallback if ClickHouse-native fixes fail; not needed before derive-child. |
| External sort/reduce navigation artifact | Carried forward from schema3 investigation; not rebuilt here | Prior schema3 notes found postings/sidecar primitives promising but lacking UI payloads alone. A production external reducer would be a new artifact pipeline. | Consider only after primary ClickHouse-native fix and query fallback work. |
| One compressed columnar artifact per mount | Not run; subsumed by RowBinary/sidecar probes | RowBinary probes approximate table-native bulk artifacts; sidecar notes cover custom navigation artifacts. A full artifact reader/registration path would be separate implementation. | Not first fix. |
| Adaptive per-mount strategy selection | Measured input shape, not implemented | t283 and scratch125 have different shapes: t283 thresholding fails, scratch125 high-fanout thresholding helps; ID compression helps scratch125 more. | Promising once multiple strategies exist; cannot be first because there is not yet a second production-ready strategy. |

## Acceptance Criteria For The Next Fix

These are gates for the follow-up fix, not tasks expected to be checked by
this investigation document.

- A bounded local t283 sample reproduces the current failure class before
  the fix.
- The same bounded t283 sample improves materially after the fix in
  end-to-end `summarise` wall time, not only in a unit benchmark.
- At least one Lustre sample, including scratch125 or a high-fanout
  equivalent, does not regress in summarise wall time, RSS, or spool bytes.
- Query gates run before and after using `clickhouse-perf query` on:
  - t283 root or mount-root path;
  - scratch125 high-fanout VCFS path;
  - at least one normal Lustre path.
- Query result counts/digests match before/after unless the fix includes a
  deliberately specified query-schema replacement.
- Post-spool retry still does not reparse `stats.gz`.
- Failed load still leaves the old active snapshot and active set visible.
- Perf reports include enough table-specific evidence to explain where the
  time went.
