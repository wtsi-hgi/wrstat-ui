# Prompt for ClickHouse summarise speed recovery spec

Use this as the source-of-truth input for the spec-writer workflow. It
incorporates the final recommendations from `.docs/summarise/investigate.md`.
The spec-writer should turn these decisions into a cohesive feature spec with
user stories, acceptance tests, implementation phases, and perf gates.

Target spec path:

```text
.docs/summarise/spec.md
```

## Goal

Write a summarise-speed recovery spec for `wrstat-ui`.

After schema3 and the recent durable-spool bug fixes, production summarise wall
time for `/nfs/t283_imaging/` regressed. The concrete production symptom is
that summarising:

```text
~/output/nfs/20260517-170004_／nfs／t283_imaging/
```

has exceeded 21 hours, where the same mount previously took about 10 hours.

The spec must recover production `wrstat-ui summarise` wall time without
regressing:

- Lustre summarise speed;
- the schema3 query-speed improvements;
- durable retry/recovery semantics from the recent summarise bug fixes.

The implementation must not hand wave the regression. It must preserve and
extend the bounded local proof from `.docs/summarise/investigate.md`, and it
must stop if the failure class cannot be reproduced locally.

## Current Implementation Context

The current branch already has the durable ClickHouse spool path from the
recent summarise bug fixes:

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
- Basedirs retry cleanup/replay was grouped and batched, but the investigation
  timings used `-t` only and did not include basedirs work.

Schema3 currently writes these spool outputs:

- `wrstat_child_filter_all`;
- `wrstat_dir_filter_all`;
- `wrstat_schema3_snapshot_sets`;
- `wrstat_active_virtual_summaries`;
- `wrstat_active_virtual_filter_all`;
- `wrstat_active_virtual_children`;
- `wrstat_active_virtual_sets`.

Important implementation details that the spec must account for:

- `cmd/summarise_spool.go` currently writes each full-filter row twice:
  `writeSchema3FullFilterRow` writes `wrstat_dir_filter_all`, then writes the
  same payload as `wrstat_child_filter_all`.
- `clickhouse/dir_filter_all.go` does the same in direct ClickHouse import:
  `flushLastPending` appends every full-filter row to both `dirWriter` and
  `childWriter`.
- `clickhouse/summarise_spool_loader.go` loads the two full-filter spool files
  separately.
- `internal/chspool.VerifyManifest` decodes every table to count rows when
  reusing an existing complete spool. First successful build/load already has
  writer-side row counts, sizes, and hashes; retry verification currently pays
  a second full decode cost.

Do not repeat the old bug fixes as the answer. The new spec may rely on those
fixes, but it must attack the measured schema3-era row and write
amplification.

## Required Local Evidence

The investigation reproduced the same class of failure locally on bounded
subsets. The spec must carry these numbers forward and preserve the same style
of proof in acceptance tests and perf gates.

Production-shaped `summarise -t` spool evidence:

| Dataset | Cap | End-to-end wall | Spool load wall | Max RSS | Spool bytes | `dir_filter_all` rows | `child_filter_all` rows | Full-filter load evidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| t283 | 200k | 41.24s | 32.21s | 372.8 MiB | 30.4 MiB | 1,196,726 | 1,196,726 | dir 11.02s, child 10.98s |
| scratch127 | 200k | 3.58s | 2.23s | 146.9 MiB | 6.42 MiB | 26,883 | 26,883 | dir 0.22s, child 0.27s |
| t283 | 500k | 48.35s | 38.47s | 360.6 MiB | 42.3 MiB | 1,363,994 | 1,363,994 | collapsed 25s |
| scratch127 | 500k | 27.44s | 20.09s | 375.6 MiB | 34.2 MiB | 621,557 | 621,557 | collapsed 12s |

Direct `clickhouse-perf import` evidence:

| Dataset | Cap | Wall | Max RSS | `dir_facts` | `dir_filter_all` | `child_filter_all` | Full-filter insert evidence |
|---|---:|---:|---:|---:|---:|---:|---|
| t283 | 200k | 22.17s | 926.7 MiB | 59,778 | 1,196,726 | 1,196,726 | dir 6.45s, child 5.65s |
| scratch127 | 200k | 2.07s | 311.9 MiB | 2,289 | 26,883 | 26,883 | dir 0.18s, child 0.12s |
| scratch125 | 500k | 24.39s | 859.2 MiB | 87,186 | 1,255,727 | 1,255,727 | dir 5.85s, child 5.26s |

The core failure class is duplicated full-filter work. On the 200k t283
sample, schema3 writes and loads about 2.39M duplicated full-filter rows from
200k input records. On scratch125 500k, a Lustre high-fanout/dense prefix
also generates more than 2.5M duplicated full-filter rows.

Query guardrails from the investigation:

- t283 200k root-ish tree queries were low-ms to sub-second.
- scratch125 500k high-fanout VCFS remains a known slow query path:
  `tree_where_cold_provider`, `dirinfos_broad`, and
  `dirshavechildren_broad` were about 60s+ and read tens of GB.
- The summarise-speed fix is not required to solve the scratch125 query
  problem, but it must not make that path worse or remove schema3 tables
  without an equivalent query replacement.

## Specify For Implementation Now

The spec must require the following implementation work. These items were
shown by the investigation to be required, effective, or directly beneficial.

### 1. Derive `wrstat_child_filter_all` From `wrstat_dir_filter_all`

This is the primary fix.

Implement a production version of the temporary
`WRSTAT_UI_EXPERIMENT_DERIVE_CHILD_FILTER_ALL=1` prototype:

1. Keep `wrstat_dir_filter_all` as the canonical full-filter spool table.
2. Stop writing `wrstat_child_filter_all` to the gob spool.
3. Load `wrstat_dir_filter_all` first.
4. Populate `wrstat_child_filter_all` inside ClickHouse with
   `INSERT INTO ... SELECT` from `wrstat_dir_filter_all`.
5. Do the derived insert before schema3 readiness, snapshot activation, or any
   publish marker that can make the new snapshot visible.
6. Preserve the final physical `wrstat_child_filter_all` table shape and query
   semantics. Query code should see the same table contract after readiness.
7. Record row-count and table evidence for the derived child table in the same
   reports used for normally loaded tables.
8. Make direct import/perf paths consistent with production summarise where
   possible. The spec should not leave `clickhouse-perf import` measuring a
   different duplicated writer path unless it explicitly explains why.

Prototype evidence to carry into the spec:

| Dataset | Cap | Baseline wall | Prototype wall | Baseline spool load | Prototype spool load | Spool bytes | Child phase |
|---|---:|---:|---:|---:|---:|---:|---:|
| t283 | 200k | 41.24s | 27.89s | 32.21s | 22.19s | 30.4 MiB -> 20.6 MiB | 10.98s -> 1.43s |
| scratch127 | 200k | 3.58s | 3.11s | 2.23s | 1.99s | 6.42 MiB -> 6.11 MiB | 0.27s -> 0.044s |
| scratch125 | 500k | no spool baseline | 36.08s | no spool baseline | 27.49s | 50.7 MiB | 1.43s for 1,255,727 rows |

Acceptance tests must prove:

- the child table is populated before readiness;
- row counts match the canonical dir table for the active snapshot;
- query result counts/digests match baseline for t283 and normal Lustre paths;
- failed derived child insert does not publish or switch active state;
- retry/recover can finish from a completed canonical spool without reparsing
  `stats.gz`;
- cleanup removes derived child rows for failed, old, inactive, or tombstoned
  snapshots consistently with other snapshot tables.

Suggested perf gates for the same bounded proof samples:

- t283 200k production-shaped `summarise -t` end-to-end wall improves by at
  least 25% versus the recorded 41.24s baseline.
- t283 200k spool load improves by at least 25% versus the recorded 32.21s
  baseline.
- t283 200k child full-filter phase is in the same class as the prototype
  derived insert, around 1-2s rather than around 11s.
- t283 200k spool bytes drop by at least 25% versus the 30.4 MiB baseline.
- scratch127 200k and at least one Lustre dense/high-fanout sample do not
  regress in end-to-end wall, RSS, spool bytes, or query p50/p95 by more than
  noise thresholds specified in the spec.

### 2. Keep Split Full-Filter Telemetry And Per-Table Load Metrics

The old combined `wrstat_filter_all_insert` phase was too coarse. The spec
must require first-class telemetry for:

- `wrstat_dir_filter_all_insert`;
- derived `wrstat_child_filter_all_insert`;
- rows per table;
- spool bytes per table where applicable;
- phase duration, rows/sec, and bytes/sec per table;
- compressed/uncompressed ClickHouse bytes for relevant tables.

Reports must make it obvious where summarise time went. The acceptance tests
should include at least one report-level assertion or golden-shape check so the
telemetry does not silently disappear later.

### 3. Add A Full-Filter Row Amplification Guard

The spec must require a diagnostic guard that reports duplicated full-filter
amplification relative to input records.

The investigation suggests:

- warn when duplicated full-filter rows/input row is greater than 5;
- hard-fail only behind an explicit perf/debug mode or waiver when it is
  greater than 10, because production data may legitimately be dense.

The exact policy can be refined in the spec, but the implementation must expose
the ratio and include it in perf/report output. It should identify whether the
amplification comes from `dir_filter_all`, derived `child_filter_all`, or both.

### 4. Optimize Completed-Spool Verification Without Weakening Durability

Implement a production version of the temporary
`WRSTAT_UI_EXPERIMENT_FAST_SPOOL_VERIFY=1` prototype.

Completed-spool retry should not decode every gob row merely to count rows when
the manifest already contains trusted writer-side row counts plus byte size and
SHA256 hashes. Verification should still check:

- manifest identity and schema/version compatibility;
- all expected files are present;
- byte size;
- hash;
- trusted row counts from the initial writer close.

If any size/hash/schema check fails, the spool must not be trusted.

Prototype evidence:

| Dataset | Spool | Full verify retry | Fast verify retry |
|---|---:|---:|---:|
| t283 200k derived spool | 20.6 MiB | 5.25s | 0.11s |
| scratch127 200k derived spool | 6.11 MiB | 0.70s | 0.06s |

This is a retry/recover improvement, not the primary first-attempt wall-time
fix. It should be implemented because it protects large production retries
after a completed spool has already been written.

### 5. Fix Misleading Final Partial Progress Logging

The investigation found that bounded 200k/500k spool logs report `records=0`
because progress is emitted every 1M rows and the final partial interval is
not copied into the parse-complete log.

This does not improve summarise speed, but it is beneficial diagnostics and
should be specified with the implementation. Final parse-complete reporting
should include the actual final parsed record count, even when the last
progress interval did not cross 1M rows.

## Do Not Implement These As Fixes

The spec must explicitly reject the following approaches for this summarise
speed recovery. They were measured and either did not help enough, were worse
for this problem, or would solve a different problem while leaving the t283
failure class intact.

### Do Not Replace The Two Tables With A ClickHouse Projection

Do not implement "one physical full-filter table plus parent-ordered
projection" as the summarise/storage fix.

Evidence: on t283 200k, a parent-ordered projection stored 12.92 MiB base plus
5.74 MiB projection, versus 12.90 MiB `dir_filter_all` plus 4.52 MiB
`child_filter_all`. It moved duplication into projection storage and did not
beat server-side child derivation.

### Do Not Generate Child Rows Only Above A Fanout Threshold As The t283 Fix

Do not use fanout-thresholded child rows as the primary t283 summarise fix.

Evidence: on t283 200k, threshold 32 still keeps 94.05% of child rows, while
threshold 64 keeps 0 because max fanout is only 36. This does not address the
t283 regression.

Thresholding may be reconsidered later only as part of a scratch125-style
high-fanout side index, not as this spec's primary summarise-speed change.

### Do Not Apply LowCardinality Path Columns Broadly

Do not blanket-convert both full-filter path columns to LowCardinality as this
fix.

Evidence: on t283, LowCardinality improved a child-table probe from 4.52 MiB
to 2.82 MiB compressed, but worsened the dir-table probe from 12.90 MiB to
13.42 MiB. This is not a broad win and does not attack the main duplicated
write/load cost.

### Do Not Use Parent Packet Rows As A Summarise Storage Replacement

Do not replace child full-filter rows with parent packet rows as the
summarise-speed fix.

Evidence: parent packets reduced t283 child row count from 1,196,726 to 3,407,
but compressed size only improved from 4.52 MiB to 4.09 MiB. On scratch125,
compressed size stayed about 16.98 MiB. Prior schema3 packet evidence also did
not beat existing parent facts for the relevant query path.

### Do Not Rely On Per-Directory Vectors Alone

Do not replace row-per-tuple full-filter tables with per-directory filter
vectors alone.

Evidence: existing vector tables are already compact, but scratch125
high-fanout `DirInfos` still reads tens of GB and takes 60s+. Vectors alone do
not solve the query guardrail and do not directly address the duplicated spool
write/load regression.

### Do Not Store Only AgeAll Eagerly As A Standalone Fix

Do not implement an AgeAll-only eager full-filter layer unless the spec also
defines a safe lazy or compact age-specific path.

Evidence: AgeAll rows are only 10% of t283 full-filter rows and 21.32% of
scratch125 rows, so this could reduce writes, but it changes readiness and
correctness for age-specific filters if done alone.

### Do Not Omit Exact Subtree Rows Without A Query Contract

Do not sparse out `wrstat_dir_filter_all` rows unless the spec proves exactly
which queries no longer need them and how omitted rows are served.

Evidence: `dir_filter_all` is the larger remaining table after child
derivation, but the investigation did not find a safe query-equivalent sparse
rule.

### Do Not Reintroduce Direct ClickHouse Loading During Parse First

Do not pursue a hybrid parse-plus-direct-ClickHouse-load approach before the
canonical child-derivation fix.

The recent bug fixes deliberately moved summarise to a durable spool before
ClickHouse load. Reintroducing direct ClickHouse interaction during parse needs
rollback and retry design and is not justified by the current evidence.

### Do Not Publish Before Full-Filter Readiness Without Product Approval

Do not implement async full-filter build after broad publish in this spec.

It can reduce user-visible publish time only if filtered queries are allowed to
be temporarily unavailable or slower after publish. That is a product behavior
change, not a pure performance fix.

## Out Of Scope For This Spec, But Worth Trying Later

The following ideas may be beneficial after the required work lands. The spec
should mention them only as explicit non-goals/follow-up candidates, not as
phase-one implementation requirements.

### Memory-Budgeted Larger Batch Caps

Raising the full-filter/projection batch cap is a real speed lever, but it is
memory-sensitive.

Evidence with child derivation:

| Dataset | Batch cap | End-to-end wall | Spool load | Max RSS | `dir_filter_all_insert` |
|---|---:|---:|---:|---:|---:|
| t283 200k | 15.3k | 27.89s | 22.19s | 386.8 MiB | 10.82s |
| t283 200k | 25k | 24.54s | 17.85s | 758.9 MiB | 6.48s |
| t283 200k | 50k | 22.89s | 17.12s | 1,026.3 MiB | 5.96s |
| scratch127 200k | 50k | 2.82s | 1.76s | 142.3 MiB | 0.147s |

A later spec may add an adaptive or configured memory-budgeted cap. This spec
must not make 50k unconditional.

### Compact Child-Presence Table

A compact child-presence table is promising for `DirsHaveChildren`, but it is
not a replacement for full child summaries.

Evidence:

- t283 presence rows: 69,118, 146.9 KiB compressed versus 1,196,726 child rows
  and 4.52 MiB compressed;
- scratch125 presence rows: 488,541, 4.99 MiB compressed versus 16.98 MiB.

Try this later as part of a query-route improvement, especially for
`DirsHaveChildren`.

### Integer Directory IDs

Integer directory IDs are promising for scratch/string-heavy cases but mixed
for t283.

Evidence:

- repeated path bytes/dictionary bytes: t283 39.5x, scratch125 24.7x;
- ID-coded `dir_filter_all` plus dictionary compressed t283 13.46 MiB versus
  12.90 MiB original;
- scratch125 improved to 19.38 MiB versus 38.66 MiB original.

This needs schema and query work and should follow the low-risk child
derivation fix.

### RowBinary/zstd Table-Native Spool

RowBinary/zstd could be a future table-native spool/load path, but it is a
larger engineering change and was memory-heavy in the probe.

Evidence on t283 200k:

- `dir_filter_all` RowBinary zstd file: 9.36 MiB, import 2.43s;
- `child_filter_all` RowBinary zstd file: 3.45 MiB, import 2.45s;
- import RSS was about 1.0 GiB.

Try later only with a memory-budgeted design and durable manifest semantics.

### Parallel Independent Table Loads

Parallel table load is worth a controlled follow-up after child derivation.

Evidence: a server-side copy approximation for t283 copied independent tables
in 1.70s parallel versus 3.74s sequential, but this omitted gob decode and is
not enough proof by itself.

### High-Fanout Query Side Index And Schema3 Query Fallbacks

scratch125 high-fanout query slowness is a separate query-fix stream. The
summarise-speed fix must not regress it, but does not need to solve it.

Later work may include:

- high-fanout side indexes;
- compact child-presence routing;
- query packet caching;
- immutable sidecar or external navigation artifacts;
- external sort/reduce navigation artifact.

Evidence: scratch125 threshold data shows a side index for parents with at
least 64 children would cover 32% of child rows and the 11,205-child VCFS case.
t283 thresholding does not help, so this belongs outside the t283 summarise
speed recovery.

### Adaptive Per-Mount Strategy Selection

Adaptive strategy selection may be useful once multiple production-ready
strategies exist. It cannot be this spec's first fix because there is not yet
a second production-ready strategy.

## Required Acceptance Criteria

The spec must include acceptance tests and perf gates covering correctness,
durability, summarise speed, query speed, and diagnostics.

Minimum acceptance criteria:

- A bounded local t283 sample reproduces the current failure class before the
  fix.
- The same bounded t283 sample improves materially after the fix in
  end-to-end `summarise` wall time, not only in a unit benchmark.
- At least one Lustre sample, including scratch125 or a high-fanout
  equivalent, does not regress in summarise wall time, RSS, or spool bytes.
- Query gates run before and after using `clickhouse-perf query` on:
  - t283 root or mount-root path;
  - scratch125 high-fanout VCFS path;
  - at least one normal Lustre path.
- Query result counts/digests match before/after unless a later spec includes
  a deliberately specified query-schema replacement.
- Post-spool retry still does not reparse `stats.gz`.
- Failed load still leaves the old active snapshot and active set visible.
- Perf reports include enough table-specific evidence to explain where the
  time went.
- No implementation phase may remove `wrstat_child_filter_all` or
  `wrstat_dir_filter_all` from the visible schema unless it proves equivalent
  or better query behavior.

The spec should define exact perf thresholds from the investigation evidence.
At minimum, use the t283 200k, scratch127 200k, and scratch125 500k samples
recorded above as required guardrails.

## Useful Existing Artifacts

The investigation left reusable bounded inputs and reports under:

```text
.tmp/agent/summarise/inputs/
.tmp/agent/summarise/perf/
```

The transient summarise outputs, ClickHouse probe databases, and scratch binary
were cleaned up. Rebuild a binary in `.tmp/agent/summarise/wrstat-ui` if
continuing experiments.

Useful command shapes are recorded in `.docs/summarise/investigate.md`.

## Notes For The Spec Writer

The first implementation should be conservative and ClickHouse-native:

1. derive `wrstat_child_filter_all` from `wrstat_dir_filter_all`;
2. keep the final query schema shape unchanged;
3. add the telemetry needed to prove the fix;
4. add fast completed-spool verification using trusted manifest data;
5. keep all more radical schema/artifact changes explicitly out of scope.

Do not let the spec drift into solving the scratch125 high-fanout query problem
unless it is framed as a separate follow-up. That problem is real, but the
current target is the t283 summarise wall-time regression caused by duplicated
schema3 full-filter write/load work.
