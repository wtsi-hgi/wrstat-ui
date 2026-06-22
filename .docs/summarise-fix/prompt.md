# Prompt for ClickHouse summarise speed + reliability recovery spec

Use this as the source-of-truth input for the spec-writer workflow. It carries
the decisions from `.docs/summarise-fix/investigate.md`. Turn these into a
cohesive feature spec with user stories, acceptance tests, implementation
phases, and perf gates.

Target spec path:

```text
.docs/summarise-fix/spec.md
```

## Goal

Recover production `wrstat-ui summarise` wall time AND reliability for the three
mounts that currently cannot be summarised on the overhaul branch:

```text
/lustre/scratch122/   (~/output/lustre/20260517-200015_／lustre／scratch122)
/lustre/scratch127/   (~/output/lustre/20260517-200015_／lustre／scratch127)
/nfs/t283_imaging/    (~/output/nfs/20260517-170004_／nfs／t283_imaging)
```

All other example mounts (scratch120/123/124/125/126) summarise fine. The three
failing mounts have raw `stats.gz` that is not DFS-subtree-contiguous, so they
always fall into the output-local external-sort retry, which is slow, writes
~1 GB of scratch per 1.5M input lines (depth-scaled, tens of GB per full mount),
and has needed six correctness patches and counting.

The production host has ample disk but its output is on **NFS**, so the volume
of scratch+spool bytes written/read is itself a primary cost; do not treat
"disk is big enough" as a fix. The spec must reduce bytes written and CPU, not
just avoid running out of space.

Hard constraints. The fix must NOT regress:

- summarise speed of healthy contiguous Lustre mounts;
- the schema3/overhaul query speed (catalog `dir_id` navigation, full-filter
  query paths, high-fanout);
- durable retry/recovery semantics from the recent bug fixes (durable spool
  before load; completed-spool retry must not reparse `stats.gz`; a failed load
  must leave the previous active snapshot/active set visible).

Do not hand wave. Preserve and extend the bounded local proof in the
investigation; stop if the failure class cannot be reproduced locally.

## Current Implementation Context

- The overhaul schema is live: per-snapshot `wrstat_dirs` catalog with preorder
  `dir_id`/`parent_id`/`subtree_end` nested-set intervals; `wrstat_files`,
  `wrstat_dir_facts`, `wrstat_dir_filter_all` (keyed `dir_id`,`subtree_end`),
  `wrstat_child_filter_all` (keyed `parent_id`,`dir_id`),
  `wrstat_dir_filter_ageall`, plus the active-virtual overlay.
- Preorder id assignment lives in `summary/dirguta` and **requires
  subtree-contiguous DFS input**: re-entering a closed directory returns
  `summary.ErrNonContiguousInput`. This is the root cause of the retry.
- `cmd/summarise_spool.go` builds a durable gob+gzip spool under
  `.wrstat-ui-clickhouse-spool` before any ClickHouse load.
- `cmd/summarise.go` -> `buildSummariseSpool` parses once with `sortInput=false`;
  on `ErrNonContiguousInput` it retries with `sortInput=true`
  (`cmd/summarise_stats_sort.go`), which external-sorts the WHOLE stats stream:
  for every line it also emits a synthetic directory record for EVERY ancestor
  (the depth multiplier), hex-encodes each path component key (2 bytes/byte),
  writes 64 MiB chunks, merge-sorts with duplicate-boundary suppression, then
  re-parses the sorted output. This is the slow/fragile path.
- `cmd/summarise_spool.go:795-801` (`writeSchema3FullFilterRow`) writes every
  full-filter row twice — `WriteDirFilterAll` then `WriteChildFilterAll` with an
  identical payload — and `clickhouse/summarise_spool_loader.go` loads both
  spool files. `clickhouse/dir_filter_all.go` has the same double write in the
  direct `clickhouse-perf import` path. The derive-child fix from
  `.docs/summarise/spec.md` was never carried into the overhaul branch.
- Full-filter load telemetry is collapsed into one `wrstat_filter_all_insert`
  phase (no per-table split).
- `internal/chspool.VerifyManifest` decodes every gob row to count rows when
  reusing a complete spool, although the writer already recorded trusted row
  counts + byte sizes + SHA256 in the manifest.
- `logParseResult` reports the last whole-million progress value, so bounded
  runs log `records=0`/`records=1000000` for 1.5M inputs.

## Required Local Evidence (carry forward; preserve the proof style)

Bounded 1.5M-line prefixes, local ClickHouse, current HEAD `de40e92`:

Sort retry (isolated in the real path) vs plain parse of the same input:

| Mount 1.5M | parse-only | sort wall | slowdown | sort scratch bytes |
|---|---:|---:|---:|---:|
| scratch127 | 1.11s | 32.2s | ~29x | 1.07 GB |
| t283       | 1.21s | 31.8s | ~26x | 1.26 GB |

Directory share + directory-only reorder cost (the enabling fact):

| Mount 1.5M | dirs | % dirs | sort all dir paths |
|---|---:|---:|---:|
| scratch127 | 49,702 | 3.3% | <0.1s |
| t283 | 482,830 | 32.2% | 0.44s |

Full-filter duplication/density (spool manifest, 1.5M):

- scratch127: `dir_filter_all` = `child_filter_all` = 1,122,958 each.
- t283: `dir_filter_all` = `child_filter_all` = 10,367,372 each (20.7M from 1.5M
  files; ~6.9x per table).
- End-to-end scratch127 1.5M load = 30s, of which the combined
  `wrstat_filter_all_insert` = 18s.

Server-side child derivation on the overhaul schema (catalog join for
`parent_id`): `INSERT INTO child SELECT ... FROM wrstat_dir_filter_all d INNER
JOIN wrstat_dirs c ON (mount,snapshot,dir_id)` produced the exact 1,122,958-row
scratch127 child set in **1.16s**, versus writing+loading a second ~1 GB gob
table.

Completed-spool retry verify decoded a 182 MB t283 spool in ~tens of seconds
before publish.

## Specify For Implementation Now (ordered)

### 1. Remove the external-sort retry's cost and fragility (primary)

The interval id assignment needs the directory tree, not contiguous file lines.
Reorder/aggregate by directory only; never spill the file set with per-ancestor
synthetic records again.

Required properties:

1. A non-contiguous mount must summarise to completion with bytes-written bounded
   by roughly the input size (no depth-multiplied synthetic-ancestor blow-up) and
   wall time within a small multiple of a contiguous mount of the same size.
2. Eliminate the synthetic-ancestor records and the 2-byte-per-byte hex
   component key from any reorder that remains.
3. Preferred design: assign preorder `dir_id`/`parent_id`/`subtree_end`/`depth`
   from the directory set alone (collect/sort only directory rows — 3-32% of the
   stream, sub-second even for t283), build a path->`dir_id` map, and resolve
   each file's `dir_id` from it. Keep the existing fast streaming path unchanged
   when the input is already contiguous (healthy mounts must not regress).
4. The per-directory GUTA aggregation must still finalise correctly without
   requiring file contiguity. If files must be grouped, group/sort by parent
   `dir_id` (compact integer key) rather than by hex path string, and without
   synthetic ancestors. Bound and report peak memory; do not introduce an
   unbounded full-stream in-memory aggregation.
5. Detect non-contiguity cheaply (streaming monotonic-boundary check) so the
   code does not first build, then discard, a full failed spool before reordering.
   Avoid paying for two full passes when one reorder pass suffices.
6. Preserve interval correctness exactly: nested-set invariant, gap-free
   preorder, determinism byte-for-byte, the reserved above-root low-id chain, and
   the existing `ErrTooManyDirs` overflow behaviour. Result digests of the final
   catalog/facts/files/full-filter rows must match the contiguous path for an
   input that is merely a permutation of a contiguous one.

Acceptance tests must prove:

- a constructed non-contiguous bounded input for each failing mount summarises to
  completion and yields identical catalog intervals / file `dir_id` references /
  table digests to the contiguous ordering of the same data;
- healthy contiguous mounts take the fast path unchanged (no extra reorder);
- bytes written for a non-contiguous mount are bounded near input size, not
  depth-multiplied;
- the previously-shipped non-contiguity regressions (unordered subtree revisit;
  prefix-sharing siblings `project/` vs `project.v2/`; same-name file/dir;
  slashless `d` rows; ` ` unicode) still pass.

Perf gates (bounded subsets):

- non-contiguous scratch127/t283 1.5M: reorder/build phase bytes-written << the
  current ~1.1-1.3 GB, and end-to-end wall within ~1.5x the contiguous fast path
  of the same size;
- healthy contiguous Lustre 1.5M: no regression in wall, RSS, or spool bytes
  beyond a stated noise threshold.

### 2. Re-land server-side derivation of `wrstat_child_filter_all`

1. Stop writing `wrstat_child_filter_all` to the gob spool (the second
   `WriteChildFilterAll` in `writeSchema3FullFilterRow`); keep
   `wrstat_dir_filter_all` canonical.
2. Populate `wrstat_child_filter_all` with `INSERT INTO ... SELECT` from
   `wrstat_dir_filter_all` joined to `wrstat_dirs` for `parent_id`, before
   schema3 readiness / snapshot activation / any publish marker.
3. Preserve the final physical table shape and query semantics; query code sees
   the same contract after readiness.
4. Make the direct `clickhouse-perf import` path consistent (it double-writes in
   `clickhouse/dir_filter_all.go`).
5. Record row-count/table evidence for the derived child table in the same
   reports as normally loaded tables.

Acceptance tests: child populated before readiness; derived row count matches
the canonical dir count for the snapshot; query result counts/digests match
baseline; a failed derived insert does not publish or switch active state;
completed-spool retry still finishes from the canonical spool without reparsing;
cleanup removes derived child rows for failed/old/inactive/tombstoned snapshots.

Perf gate: end-to-end load drops materially on dense mounts (the ~18s combined
`filter_all_insert` on scratch127 1.5M should fall toward the dir-only insert
plus a ~1-2s derived insert); spool bytes drop by roughly the child file's share.

### 3. Split full-filter telemetry + amplification guard

- Replace the collapsed `wrstat_filter_all_insert` with separate
  `wrstat_dir_filter_all_insert` and derived `wrstat_child_filter_all_insert`
  metrics (rows, bytes, duration, rows/sec) in the spool-load and import reports.
- Expose a full-filter rows/input-row amplification ratio attributed to dir vs
  derived child. Warn above 5; hard-fail only behind an explicit debug/waiver
  above 10 (production data is legitimately dense; t283 was ~6.9x per table).
- Add at least one report-level/golden-shape assertion so the telemetry cannot
  silently disappear again (it was specified once before and never landed).

### 4. Fast completed-spool verification

Trust the writer-recorded manifest row counts plus per-file byte size and SHA256;
skip the full gob decode in `VerifyManifest` when reusing a complete spool. Still
verify manifest identity, schema/version compatibility, file presence, byte size,
and hash. Any size/hash/schema mismatch must reject the spool. This protects
large production retries; it is not the first-attempt fix.

### 5. Fix final partial progress logging

Report the true final parsed record count at parse-complete even when the last
progress interval did not cross 1M. Diagnostics only.

## Do Not Implement These As The Fix

- Do NOT add a seventh `non-contiguous directory input` correctness patch to the
  existing whole-stream synthetic-ancestor sort while leaving its ~26x cost and
  ~1 GB/1.5M scratch writes in place. The sort path itself is the liability.
- Do NOT "just rely on bigger disk" — the production output is NFS; bytes written
  are the cost.
- Do NOT drop or sparsify any schema3/overhaul query table (`wrstat_dir_filter_all`,
  `wrstat_child_filter_all`, facts, catalog) without proving equivalent query
  speed and correctness.
- Do NOT reintroduce direct ClickHouse loading during parse (durability/retry was
  deliberately moved to a durable spool first).
- Do NOT publish before full-filter readiness / async build after publish (that
  is a product behaviour change, not a pure perf fix).
- Do NOT make a large full-filter batch cap unconditional (memory). A
  memory-budgeted/adaptive cap may be a later follow-up, not phase one.
- Do NOT switch to per-directory vectors / parent packets / projections / a
  sidecar as this spec's fix; they are separate query-side streams.

## Out Of Scope (mention as follow-ups only)

Memory-budgeted larger batch caps; parallel independent table loads after child
derivation; integer-id dictionary compression tuning; RowBinary/zstd table-native
spool; high-fanout query side index for scratch125-class queries; upstream
`wrstat` emitting already-contiguous stats (note it as the true root-cause fix
that would make the in-`wrstat-ui` reorder a defensive fallback only).

## Required Acceptance Criteria (summary)

- A bounded local non-contiguous subset of a failing mount reproduces the slow
  sort-retry class before the fix and summarises with bounded bytes-written after.
- At least one healthy contiguous Lustre subset does not regress in wall, RSS, or
  spool bytes.
- Query gates run before/after with `clickhouse-perf query` on a t283 mount-root
  path, a healthy Lustre path, and a high-fanout path; result counts/digests
  match unless a deliberate query-schema replacement is specified. (Establish the
  baseline on a clean single-mount DB; the perf harness has a pre-flight
  glob-routing EXPLAIN assertion that fails on a polluted multi-mount DB.)
- Completed-spool retry does not reparse `stats.gz`; failed load leaves the old
  active snapshot/active set visible.
- Reports include per-table full-filter evidence and the amplification ratio.
- No phase removes `wrstat_child_filter_all` or `wrstat_dir_filter_all` from the
  visible schema without proving equivalent query behaviour.

## Notes For The Spec Writer

Order the work by what removes the failure class:

1. eliminate the synthetic-ancestor whole-stream sort (directory-centric,
   contiguity-tolerant id assignment + grouping; minimise bytes written);
2. re-land server-side child derivation + split telemetry + amplification guard;
3. fast completed-spool verification + the progress-log fix.

The investigation left reusable bounded inputs and reports under
`.tmp/agent/summarise-fix/` (binary, 1.5M prefixes per mount, mounts file, perf
evidence). Rebuild a binary there if continuing experiments, and start a local
ClickHouse with a dummy cert (see the investigation's command shapes).

## Notes

These notes resolve open mechanism decisions (the user delegated all answers).

- Non-contiguous build is a directory-centric two-pass build, not a whole-stream
  file sort. Pass 1 streams the stats reading ONLY directory rows plus
  per-directory GUTA accumulators keyed by directory, builds the directory set,
  and assigns preorder `dir_id`/`parent_id`/`subtree_end`/`depth` from the sorted
  directory set; it also builds a path->`dir_id` map. Pass 2 re-streams the stats,
  resolves each file's `dir_id` from the map, adds into the (directory-set-sized)
  accumulators, and emits rows in `dir_id` order. File rows and synthetic ancestor
  rows are NEVER spilled to disk. Peak memory is bounded by the directory set, not
  the file set. The current `drainInto`-at-directory-close finalisation that relies
  on file contiguity must be replaced for this path by accumulator-map finalisation
  keyed by `dir_id`.
- Keep the existing fast single-pass streaming build unchanged for contiguous
  input (healthy mounts must not regress and must not take any extra reorder
  pass). Detect non-contiguity inline: on the first `summary.ErrNonContiguousInput`
  during the streaming build, switch to the directory-centric two-pass build
  described above. Do NOT build then discard a full failed spool, and do NOT run
  the old whole-stream synthetic-ancestor external sort. The legacy
  `cmd/summarise_stats_sort.go` sort path (synthetic ancestors + hex component
  keys + chunk spill) is removed/retired as the contiguity remedy.
- Memory budget: the directory-centric path targets peak RSS in the same order as
  the current contiguous fast path (about 489 MB at the 1.5M-line bounded proof
  size), scaling with directory count. Report peak RSS via the existing
  `MaxRSSBytes` report field. Exceeding the budget is a perf-gate failure, not a
  runtime fallback. On-disk spill of directory accumulators stays an explicit
  out-of-scope follow-up; do not implement it now.
- Server-side child derivation runs as a distinct, separately-reported phase
  `wrstat_child_filter_all_insert`, immediately after `wrstat_dir_filter_all_insert`
  and before schema3 readiness/snapshot activation, writing into the same staging
  snapshot partition so the existing per-snapshot partition-drop cleanup already
  removes failed/old/inactive/tombstoned derived rows (no new cleanup path). On
  derive-insert error, return before readiness so the previous active snapshot and
  active set stay visible. Stop writing `wrstat_child_filter_all` to the gob spool;
  the derive uses `INSERT INTO ... SELECT ... FROM wrstat_dir_filter_all JOIN
  wrstat_dirs` for `parent_id`.
- Telemetry: reuse the existing `perfreport`/`chperf` structures. Split the
  collapsed `wrstat_filter_all_insert` into named `wrstat_dir_filter_all_insert`
  and `wrstat_child_filter_all_insert` phases (rows, bytes, duration, rows/sec) in
  both the spool-load report and the `clickhouse-perf import` report. Compute the
  dir-vs-child full-filter amplification ratio from the existing per-table
  `TableStats.RowAmplificationVs*` fields. Warn when amplification per input row
  exceeds 5; hard-fail only behind an explicit debug/waiver env or flag when it
  exceeds 10. Add a GoConvey golden-shape assertion (mirroring the existing
  `final_gate` E3 amplification check) that both phase keys and the amplification
  fields are present and non-zero, so the telemetry cannot silently disappear.
- Fast completed-spool verification: persist trusted per-table row counts in the
  manifest at initial writer close, and have `VerifyManifest` trust them plus
  per-file byte size and SHA256 when reusing a complete spool, skipping the full
  gob decode. Any size/hash/schema/identity/version/presence mismatch still rejects
  the spool. This needs a manifest schema/version note for the trusted counts.

### Notes (round 2 — directory-centric build internals)

- Architecture: implement the contiguity-tolerant build in a new exported Go
  package (e.g. `summary/dirbuild`) that owns both passes and the `dir_id`-keyed
  accumulator map. It must REUSE `summary/dirguta`'s per-file GUTA logic
  (file-type classification incl. temp-dir typing, age-bucket fill, and
  subtree-wide hardlink/inode dedup) by extracting those into shared helpers, so
  the directory-centric path and the existing DFS `Operation` path produce
  byte-for-byte identical facts/full-filter/catalog digests. The existing fast
  contiguous path keeps using `dirguta` verbatim (no change to its `Operation`
  lifecycle). The spec must enumerate exactly which `dirguta` internals are shared.
- Subtree (ancestor) aggregation in the directory-centric path: do NOT add each
  file into every ancestor (that re-introduces an O(files x depth) cost). Instead,
  add each file only into its leaf directory's accumulator, then after pass 2
  perform ONE bottom-up roll-up over the directory set in reverse preorder
  (descending `dir_id` / by `subtree_end`), draining each directory's store into
  its parent exactly as `dirguta`'s `addChild`/`drainInto` does today — including
  merging each child's seen-hardlink/inode set into its parent so subtree-wide
  dedup is preserved and inodes are not double-counted. This mirrors `drainInto`
  semantics and bounds CPU independent of tree depth.
- Inline switch-over on non-contiguity: on the first
  `summary.ErrNonContiguousInput` during the streaming build, discard the partial
  `chspool.Set`/partial dir (as today's `os.RemoveAll(partialDir)` retry does) and
  run the directory-centric two-pass build from a fresh re-open of the stats file
  (pass 1 reads dirs + builds accumulators and the path->`dir_id` map; pass 2 reads
  files and resolves `dir_id`). This is up to three cheap sequential reads of the
  stats stream (~1s parse each per 1.5M lines) with ZERO disk spill of file/sort
  data — there is no whole-stream synthetic-ancestor sort and no failed FULL spool
  is produced or re-decoded. Healthy contiguous mounts never trigger this and take
  no extra pass. The spec should state the exact read count and that the
  eliminated waste is the discarded-failed-spool + ~26-29x external sort, not raw
  re-reads.
- Peak-RSS gate source: read `MaxRSSBytes` from the spool-load report. It is a
  whole-process `getrusage(RUSAGE_SELF)` high-water mark, and because build+load
  run in one process it already captures the build-phase peak; no new build report
  is needed. A build-only (non-publishing) test path must still assert memory via
  the go-conventions memory-bounded test pattern.
