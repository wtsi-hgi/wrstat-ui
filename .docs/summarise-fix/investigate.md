# Prompt for ClickHouse summarise speed + reliability investigation

Use this before writing a follow-up summarise spec in `.docs/summarise-fix`.

The goal is to recover production `wrstat-ui summarise` wall time and, just as
importantly, *reliability*, for the three mounts that currently cannot be
summarised at all on the overhaul branch:

```text
~/output/lustre/20260517-200015_／lustre／scratch122
~/output/lustre/20260517-200015_／lustre／scratch127
~/output/nfs/20260517-170004_／nfs／t283_imaging
```

All other example mounts (scratch120, 123, 124, 125, 126) summarise to
completion. These three are both too slow and too buggy: no clean end-to-end
summarise of them has completed yet, even after the `.docs/overhaul` schema
redesign and the `.docs/bugfixes` rounds 260617-1 through 260619-2.

This is a different problem from `.docs/summarise/investigate.md`. That pass
targeted the t283 wall-time regression caused by duplicated schema3 full-filter
writes, on the pre-overhaul string-keyed schema. Since then the overhaul landed
integer `dir_id`/`subtree_end` preorder intervals, and a long tail of
`non-contiguous directory input` bug fixes added an output-local external sort
retry. The sort retry is now the dominant cost and the dominant source of
fragility for exactly these three mounts.

Do not hand wave this. If a bounded local subset cannot reproduce the same
class of failure, stop and say so. A candidate chosen without a local failure
mode is likely to optimise the wrong part of the pipeline.

## What Is Already Implemented

The current branch (clickhouse, HEAD `de40e92` at time of writing) has:

- The full `.docs/overhaul` schema: a per-snapshot `wrstat_dirs` catalog with
  preorder `dir_id`/`parent_id`/`subtree_end` nested-set intervals; `wrstat_files`,
  `wrstat_dir_facts`, `wrstat_dir_filter_all`, `wrstat_child_filter_all`,
  `wrstat_dir_filter_ageall` all keyed by `dir_id` (no path strings on hot rows);
  active-virtual overlay with its own small id space.
- Preorder id assignment in `summary/dirguta` that **requires subtree-contiguous
  DFS input**. Re-entering a closed directory returns
  `summary.ErrNonContiguousInput`; overflow returns `ErrTooManyDirs`
  (`summary/dirguta`, overhaul B1).
- A durable output-local gob+gzip spool under `.wrstat-ui-clickhouse-spool`
  built before any ClickHouse load (`cmd/summarise_spool.go`).
- An external-sort retry: `buildSummariseSpool` runs one parse attempt with
  `sortInput=false`; on `ErrNonContiguousInput` it retries with
  `sortInput=true`, which fully sorts the raw stats stream into DFS-contiguous
  order before re-parsing (`cmd/summarise_stats_sort.go`). The sort emits
  synthetic ancestor directory rows, hex-encodes component path keys, suppresses
  duplicate boundaries, normalises slashless `d` rows, and fixes unicode
  unquoting (bug fixes 260617-1 .. 260619-2).
- Completed-spool retry that skips reparsing and resumes publish, gated by
  `VerifyManifest` (`internal/chspool`).

Important current implementation details the spec must account for:

- `cmd/summarise_spool.go:795-801` (`writeSchema3FullFilterRow`) still writes
  **every** full-filter row twice: `WriteDirFilterAll` then `WriteChildFilterAll`
  with an identical payload. `clickhouse/summarise_spool_loader.go` loads the two
  spool files separately. The derive-child-server-side fix recommended in
  `.docs/summarise/spec.md` was never carried into the overhaul branch.
- Per-table full-filter load telemetry is still collapsed into one
  `wrstat_filter_all_insert` phase. The split recommended by the prior spec was
  not implemented either.
- `internal/chspool.VerifyManifest` decodes every gob row to count rows when
  reusing a complete spool, even though the writer already recorded trusted row
  counts, byte sizes and SHA256 hashes.
- `logParseResult` still reports the last whole-million progress value, so
  bounded runs log `records=0` or `records=1000000` for 1.5M inputs.

Do not repeat the old fixes as the answer. In particular, do not propose only:

- another `non-contiguous directory input` correctness patch to the existing
  sort retry (six fixes deep already; the sort path itself is the liability);
- increasing query timeout or relying on scheduler retries;
- making post-spool publish resumable (already done);
- making zero-record active-virtual refresh faster;
- batching basedirs cleanup/replay (already done);
- cache warming;
- dropping schema3 query tables without proving equivalent query speed.

Those can be ingredients, but the next design must attack the measured
sort-retry cost, the synthetic-ancestor disk blow-up, and the duplicated
full-filter write/load.

## Local Proof From This Pass

Seeded with bounded proof runs on 2026-06-19. The full production mounts were
NOT summarised end-to-end (each `stats.gz` is 2.8-6.1 GB compressed; a full run
takes many hours and would overflow the 26 GB free disk). The same failure
class was reproduced on bounded 1.5M-line prefixes.

Artifacts:

- Binary: `.tmp/agent/summarise-fix/wrstat-ui`
- Inputs: `.tmp/agent/summarise-fix/inputs/{scratch122,scratch127,t283}-1500k.stats.gz`
- Mounts file: `.tmp/agent/summarise-fix/mounts.txt`
- Local ClickHouse: `127.0.0.1:9000`, data under `.tmp/agent/summarise-fix/ch/`
- Evidence summary + reports: `.tmp/agent/summarise-fix/perf/`

### Sort-retry path is the dominant regression

Measured directly inside the real summarise path (env-gated `WRSTAT_FORCE_SORT=1`
on a warm DB so the sort is isolated), and a plain parse of the same input:

| Mount | parse-only wall | sort wall | slowdown | sort scratch bytes (chunks+merged) |
|---|---:|---:|---:|---:|
| `/lustre/scratch127/` 1.5M | 1.11s | 32.2s | ~29x | 1.07 GB |
| `/nfs/t283_imaging/` 1.5M  | 1.21s | 31.8s | ~26x | 1.26 GB |

(An earlier standalone probe reported ~100x and ~5-10 GB; that was inflated by
concurrent Go compilation I/O and a disk watcher that included the build cache.
The numbers above are the clean, in-path figures and should be used.)

The sort output line count equals the input (1.5M); the blow-up is the
*intermediate* chunk files. `summariseStatsSortRecordsForLine` emits a synthetic
directory record for every ancestor of every file (plus a 2-byte-per-byte
hex-encoded component key) before duplicates are suppressed at merge time, so a
depth-`D` tree inflates chunk volume well beyond the input. ~1.1-1.3 GB of
scratch per 1.5M lines extrapolates to tens of GB for a full mount. On the
production host disk does not run out, but it is **NFS**, so writing tens of GB
of sort scratch (then reading it back) is itself a dominant cost; locally (26 GB
free) a full mount would overflow disk. Either way the sort path is the failure.

The retry also runs only *after* a full first parse attempt has already failed
part-way through with `ErrNonContiguousInput`, then re-parses the sorted output.
A non-contiguous mount therefore pays: full parse #1 (fails) + external sort
(~26-29x a parse, ~1 GB writes per 1.5M lines) + full parse #2 of the sorted
stream + spool build + load. The three failing mounts are precisely the ones
whose raw stats are non-contiguous (bug fixes 260618-3 scratch124, 260619-1/2
scratch122, 260618-4 t283), so they always take the slow, fragile path. Healthy
mounts (scratch120/123/124/125/126) are contiguous and skip it.

### Directory-only reorder is ~70x cheaper than the full sort

The catalog needs contiguity, not the files. Directory rows are a small fraction
of the stream, and sorting only them avoids the synthetic-ancestor explosion:

| Mount | total rows | directory rows | % dirs |
|---|---:|---:|---:|
| `/lustre/scratch127/` 1.5M | 1,500,000 | 49,702 | 3.3% |
| `/nfs/t283_imaging/` 1.5M  | 1,500,000 | 482,830 | 32.2% |

Sorting all 482,830 t283 directory paths took **0.44s** (`sort(1)`), versus the
full file+ancestor sort at ~32s. Files then resolve their `dir_id` by an
in-memory parent-path lookup against the sorted directory set — no file rows and
no synthetic ancestors need to be spilled to disk. This is the enabling fact for
the radical fix in section 2.

### End-to-end fast path (contiguous), scratch127 1.5M

Total wall 147s on a cold database:

- schema bootstrap on the empty DB (preflight): ~110s, one-time per database;
- parse + spool build: ~10s;
- ClickHouse spool load: 30.1s, of which `wrstat_filter_all_insert` was 18s.

Loaded rows: `files`=1,500,000; `dirs`=`dir_facts`=49,704;
`dir_filter_all`=`child_filter_all`=1,122,958 (exact duplicate);
`dir_filter_ageall`=122,378. Spool 58 MB, peak RSS 489 MB.

So for a contiguous mount the per-mount cost (warm DB) is ~10s build + ~30s
load; the load is dominated by the duplicated full-filter insert. Contiguous
Lustre mounts are healthy today, which matches the user report.

### Full-filter density and duplication

t283 1.5M-file spool manifest:

- `dir_filter_all` = `child_filter_all` = **10,367,372 rows each** -> 20.7M
  full-filter rows from 1.5M input files (~13.8x), spool 182 MB.
- scratch127 1.5M: 1,122,958 per table, spool 56 MB.

t283 is ~9x denser per input row than scratch127. It is the worst case for BOTH
the sort retry and the duplicated full-filter load. Halving the full-filter
write/load by deriving `child_filter_all` from `dir_filter_all` server-side is
still a real win on top of fixing the sort path.

Overhaul nuance: unlike the pre-overhaul string schema, `child_filter_all`
(keyed `parent_id`,`dir_id`) is NOT a column-rename of `dir_filter_all` (keyed
`dir_id`,`subtree_end`) — the derived insert needs `parent_id` from the
`wrstat_dirs` catalog. Measured: `INSERT INTO child SELECT ... FROM
dir_filter_all d INNER JOIN wrstat_dirs c ON (mount,snapshot,dir_id)` produced
the exact 1,122,958 rows for the scratch127 snapshot in **1.16s**, versus
writing+loading a second ~1 GB gob table. So derive-child still applies; it just
joins the (small) catalog.

### Completed-spool verification

Re-running summarise against an existing complete t283 1.5M spool spent ~53s in
preflight before publish, because `VerifyManifest` decodes every gob row to count
rows. Trusted-manifest fast verify was never implemented.

### Diagnostics

`records=0` / `records=1000000` were logged for 1.5M inputs (the final partial
progress interval is not reported). Unchanged from the prior investigation.

## Ground Rules

- Keep prototype code, scratch SQL, reports, temporary binaries, and notes in
  `.tmp/agent/summarise-fix/`. Use a unique ClickHouse database per experiment.
- Do NOT summarise complete example mounts unless the user explicitly asks for a
  production-scale run. Use bounded prefixes. To reproduce the non-contiguous
  failure specifically, take a window around a known break point (e.g. scratch122
  raw line ~14.96M, scratch124 ~11.35M) or a prefix large enough to contain one.
- Watch disk. The sort retry writes ~1 GB scratch per 1.5M lines (depth-scaled, tens of GB per full mount); on NFS that write/read is itself a dominant cost, and only ~26 GB is
  free. Cap inputs and clean scratch between runs.
- Always test at least one of the three failing mounts AND at least one healthy
  contiguous Lustre mount, so a candidate cannot fix the failing mounts by
  regressing the healthy ones.
- Measure both the spool-build path (parse/sort, no ClickHouse) and the
  end-to-end `summarise` path (spool load + publish). The sort cost is invisible
  to a direct `clickhouse-perf import`.
- Record wall time, user/system CPU, max RSS, peak sort scratch bytes, spool
  bytes, table row counts, part counts, per-phase durations, and query p50/p95.
- Run query guardrails with `clickhouse-perf query`/`rest` (not only unit tests)
  before and after, including a high-fanout path, so a summarise change does not
  regress the schema3 query gains.

Useful command shapes:

```bash
timeout 300s env CGO_ENABLED=1 go build -o .tmp/agent/summarise-fix/wrstat-ui .

# Start a local ClickHouse (needs a dummy cert even with secure ports off):
openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt -days 3 -nodes -subj /CN=localhost
clickhouse server -- --listen_host=127.0.0.1 --tcp_port=9000 --tcp_port_secure=0 \
  --http_port=8123 --https_port=0 --mysql_port=0 --postgresql_port=0 --grpc_port=0 \
  --openSSL.server.certificateFile=$PWD/server.crt --openSSL.server.privateKeyFile=$PWD/server.key \
  --path=$PWD/data/

# Bounded prefix (preserves DFS head -> contiguous; window a break to force the retry):
zcat <mount>/stats.gz | head -n 1500000 | gzip > inputs/<m>-1500k.stats.gz

# End-to-end summarise (-t only, no basedirs); mount derived from the dataset dir name:
/usr/bin/time -v .tmp/agent/summarise-fix/wrstat-ui summarise \
  -t "outputs/<TS>_／<mnt-encoded>/dguta.dbs" \
  -C "clickhouse://localhost:9000?database=<db>&compress=lz4" -D <db> \
  --query-timeout 180s --mounts mounts.txt "outputs/<TS>_／<mnt-encoded>/stats.gz"
```

## Baseline Setup Checklist

- [x] Read `.docs/overhaul/spec.md` phase summaries, `.docs/summarise/investigate.md`,
  `.docs/summarise/prompt.md`, and bug fixes 260617-1 .. 260619-2.
  Result: failures evolved from string-keyed duplicated full-filter writes into
  an interval-id schema whose contiguity requirement spawned an external-sort
  retry. The sort retry is the new dominant cost and fragility.
- [x] Build a current binary in `.tmp/agent/summarise-fix/wrstat-ui`.
- [x] Create bounded 1.5M-line prefixes for scratch122, scratch127, t283.
- [x] Reproduce the sort-retry cost class locally.
  Result: ~100-134x slower than parse, ~5-10 GB scratch per 1.5M lines.
- [x] Measure a healthy contiguous end-to-end fast path (scratch127 1.5M).
  Result: ~10s build + 30s load (18s duplicated full-filter) on a warm DB.
- [x] Confirm the duplicated full-filter write/load and density on t283/scratch127.
  Result: child == dir full-filter exactly; t283 20.7M full-filter rows from 1.5M.
- [x] Reproduce a real end-to-end `summarise` that actually enters the sort retry
  and record wall + scratch. Synthetic re-entry appends did not trip the
  Summariser (it tolerates a lone re-declared dir), so the sort was exercised via
  an env-gated force flag on the real path: scratch127 1.5M sort = 32.2s / 1.07 GB,
  t283 1.5M = 31.8s / 1.26 GB. Completes within free disk at 1.5M; a full mount
  would not.
- [x] Confirm the scratch-bytes-per-line slope.
  Result: ~715-840 bytes of sort scratch per input line (1.07-1.26 GB / 1.5M);
  scales with tree depth via synthetic ancestors. A 60M-line mount ~= 45-50 GB.
- [ ] Record same-subset query guardrails (root, mount-root, high-fanout) before
  and after any candidate, using `clickhouse-perf query`. Harness exists but its
  pre-flight glob-routing EXPLAIN assertion failed on the polluted multi-mount
  experiment DB; establish on a clean single-mount load during spec work.

## Investigation Checklist

### 1. Trace And Quantify The Real Cost Of A Failing Mount

Question: for a mount that enters the sort retry, where does the wall time and
disk actually go?

- [ ] Add phase-level timing/telemetry that separates: first parse attempt (the
  one that fails), external sort (chunk write + merge), second parse of the
  sorted stream, spool build, and each ClickHouse load phase. (Prototyped here
  via a temporary stderr timer; production telemetry still to implement.)
- [x] Record sort wall, sort scratch bytes, and the parse baseline for the
  failing mounts. Result: scratch127 32.2s/1.07 GB, t283 31.8s/1.26 GB vs ~1.1s
  parse. Per-phase end-to-end split: cold-DB scratch127 1.5M = ~110s one-time
  schema bootstrap + ~10s parse/build + 30s load (18s combined filter_all).
- [x] Confirm the double/triple pass over the data.
  Result: yes by construction — failed parse #1 + full external sort (reads input,
  writes/reads ~1 GB chunks+merged) + parse #2 of the sorted stream.
- [x] Quantify synthetic-ancestor amplification in the sort chunks.
  Result: ~715-840 scratch bytes per input line (depth-scaled), the source of the
  ~1.1-1.3 GB per 1.5M lines.
- [ ] A fix candidate must reduce wall time AND bound bytes written (NFS!) for the
  failing mounts, not only lower a micro-benchmark.

### 2. Make Id Assignment Tolerate Non-Contiguous Input (radical, preferred)

Question: can the preorder `dir_id`/`subtree_end` interval be assigned WITHOUT
requiring the raw stats to be DFS-contiguous, removing the need for the sort
retry entirely?

- [x] Establish the enabling fact: the directory set is a small fraction of the
  stream and sorting only it is cheap. Result: scratch127 3.3% dirs (49,702),
  t283 32.2% dirs (482,830); sorting all 482,830 t283 dir paths = 0.44s vs ~32s
  for the full file+ancestor sort.
- [ ] Prototype a two-pass-over-catalog-only assignment: stream the stats once
  accumulating per-directory facts keyed by path (not requiring order), then
  assign preorder intervals from the directory set alone. (Needs implementation.)
- [ ] Assign `dir_id`/`parent_id`/`subtree_end`/`depth` by sorting only the
  DIRECTORY paths in component order, then resolve each file's `dir_id` by an
  in-memory parent-path lookup (no file rows or synthetic ancestors spilled).
- [ ] Keep the existing fast streaming path when input IS contiguous; fall back
  to the directory-only reorder when it is not, instead of sorting every file
  line plus synthetic ancestors.
- [ ] Prove interval correctness (nested-set invariant, gap-free preorder,
  determinism) matches the current allocator on a real fixture.
- [ ] Measure: directory-only reorder should cost O(dirs) sort, not O(files x
  depth) sort. Target: failing-mount spool build within a small multiple of the
  contiguous fast path, with bounded scratch.
- [ ] Decide whether the file table even needs preorder ids at build time or can
  resolve `dir_id` from a path->id map built from the catalog pass.

### 3. Eliminate The Synthetic-Ancestor Disk Blow-Up (if the sort stays)

Question: if an external reorder is still needed, can it avoid materialising a
synthetic record per ancestor per file?

- [ ] Reorder only what must be reordered: sort the explicit directory rows (to
  rebuild contiguous subtrees), and stream files attached to their parent dir
  without emitting per-ancestor synthetic rows into the chunk files.
- [ ] Derive missing-parent synthesis from the sorted directory set after the
  sort, not by duplicating ancestors into every pre-sort chunk.
- [ ] Drop the hex-encoded component key (it doubles key bytes for every record);
  use a compact comparable encoding or sort directory rows by structured
  components without 2-byte-per-byte expansion.
- [ ] Measure peak scratch bytes per input line before/after; target a small
  constant per line, independent of tree depth.

### 4. Avoid The Failed First Parse Attempt

Question: must we always attempt a contiguous parse, fail deep into the stream,
and only then reorder?

- [ ] Detect non-contiguity cheaply (e.g. a streaming check that the directory
  boundary order is monotonic) without building the whole spool first.
- [ ] If detected, go straight to the reorder path instead of discarding a
  full failed spool build.
- [ ] Alternatively, make the reorder the only path and prove it is not slower
  than today's contiguous fast path for healthy mounts (it must not regress
  scratch120/123/124/125/126).
- [ ] Consider whether the upstream `wrstat` walk can emit contiguous stats so
  no reorder is ever needed; if so, record it as the real root-cause fix and
  scope the in-`wrstat-ui` reorder as a defensive fallback only.

### 5. Derive `child_filter_all` From `dir_filter_all` Server-Side

Question: re-land the prior spec's primary fix that the overhaul dropped.

- [ ] Stop writing `wrstat_child_filter_all` to the gob spool
  (`cmd/summarise_spool.go:800`); keep `wrstat_dir_filter_all` canonical.
- [ ] Populate `wrstat_child_filter_all` with `INSERT INTO ... SELECT` from
  `wrstat_dir_filter_all` JOIN `wrstat_dirs` (for `parent_id`) before readiness.
- [ ] Preserve the final physical table shape and query semantics; record row
  counts/evidence for the derived table.
- [x] Confirm the duplication still exists and its size.
  Result: child == dir exactly; scratch127 1.12M each, t283 10.37M each; ~18s of
  a 30s load on scratch127 1.5M is the combined filter_all insert.
- [x] Measure the derived-insert cost on the overhaul schema (catalog join).
  Result: 1.16s for the 1,122,958-row scratch127 snapshot, exact row parity,
  versus writing+loading a second ~1 GB gob table.
- [ ] Make `clickhouse-perf import` consistent with summarise (it has the same
  double write in `clickhouse/dir_filter_all.go`).
- [ ] Gate: failing/derived insert must not publish or switch active state;
  cleanup removes derived rows for failed/old/tombstoned snapshots.

### 6. Split Full-Filter Telemetry And Add An Amplification Guard

- [ ] Replace the collapsed `wrstat_filter_all_insert` phase with separate
  `wrstat_dir_filter_all_insert` and derived `wrstat_child_filter_all_insert`
  metrics (rows, bytes, duration, rows/sec) so reports show where time went.
- [ ] Expose a full-filter rows/input-row amplification ratio in perf/report
  output, attributed to dir vs derived child. Warn above ~5; hard-fail only
  behind a debug/waiver above ~10 (production data is legitimately dense; t283
  was ~6.9x per table).
- [ ] Add at least one report-level assertion so the telemetry cannot silently
  disappear again (it was specified once before and never landed).

### 7. Fast Completed-Spool Verification

- [ ] Trust writer-recorded row counts in the manifest plus per-file byte size
  and SHA256; skip the full gob decode in `VerifyManifest` when reusing a
  complete spool. Still verify identity, schema/version, presence, size, hash.
- [x] Confirm the current cost.
  Result: ~53s preflight verify for a 182 MB t283 spool. This protects large
  production retries; it is not the first-attempt fix.

### 8. Memory-Budgeted Batch Cap And Parallel Independent Loads (follow-up)

- [ ] Revisit the prior spec's batch-cap lever (15.3k -> 25k/50k) only behind a
  memory budget; measure RSS, do not make a large cap unconditional.
- [ ] Consider loading independent spool tables concurrently after child
  derivation; measure contention and gob-decode overlap.

### 9. Fix Misleading Final Partial Progress Logging

- [ ] Report the true final parsed record count at parse-complete even when the
  last interval did not cross 1M. Diagnostics only; no speed change.

### 10. Query Guardrails (must not regress schema3 gains)

- [ ] Before/after `clickhouse-perf query` on a t283 mount-root path, a healthy
  Lustre path, and a high-fanout path. Result counts/digests must match unless a
  deliberate query-schema replacement is specified.
- [ ] A summarise-speed change must not remove or sparsify a schema3 query table
  without an equivalent query path.

## Comparison Matrix

Maintain this table as experiments complete.

| Design | Objects/paths changed | Failing-mount cost | Sort scratch (bytes written) | Healthy-mount wall | Full-filter write/load | Correctness/fragility | Recommendation |
|---|---|---:|---:|---:|---:|---|---|
| Current HEAD | overhaul intervals + sort retry + double full-filter | sort ~26-29x parse / 1.5M; +failed parse #1 +parse #2 | ~1.1-1.3 GB / 1.5M (~45-50 GB on a 60M mount) | ~10s build + 30s load / 1.5M | child == dir, doubled | 6 sort fixes deep; brittle | Baseline only |
| 2. Non-contiguous-tolerant id assignment | directory-only reorder; no full-stream sort | dir sort 0.44s/482k dirs; file dir_id via in-mem lookup | ~0 (catalog only, files not spilled) | must not regress | unchanged here | removes the brittle path entirely | Preferred radical fix |
| 3. Reorder without synthetic ancestors | sort explicit dirs only; compact keys | << current sort | small constant/line, no depth blow-up | n/a | unchanged | simpler than current sort | Strong if 2 is too large |
| 4. Detect + skip failed first parse | streaming contiguity check | removes one full parse | unchanged | unchanged | unchanged | low | Cheap complement |
| 5. Derive child_filter_all server-side | drop child gob; INSERT..SELECT + catalog join | n/a | n/a (removes ~1 GB gob write) | derive 1.16s/1.12M rows vs ~9s gob load | halves write/load | preserves table shape | Re-land; low risk |
| 7. Fast spool verify | trusted manifest counts | n/a | n/a | n/a | n/a | size/hash still checked | Retry ergonomics |
| Another sort correctness patch | tweak existing sort | still ~26x; still writes ~1 GB+/1.5M | still depth-scaled | unchanged | unchanged | 7th patch | Rejected as the answer |

## Final Recommendation (to be confirmed by the spec experiments)

Order the work by what removes the failure class, not just the symptom:

1. **Remove the contiguity requirement (sections 2-4).** The root liability is
   that interval id assignment demands DFS-contiguous raw stats, and production
   stats for these three mounts are not contiguous, forcing a ~26-29x external
   sort that emits a synthetic record per ancestor per file and blows out disk.
   Prefer assigning preorder intervals from the directory set alone (hundreds of
   thousands of dirs, not tens of millions of file lines), keeping the fast
   streaming path when input is already contiguous, and skipping the doomed
   first parse. If a full reorder must remain, at least stop materialising
   per-ancestor synthetic rows and drop the byte-doubling hex key.
2. **Re-land server-side child derivation (section 5)** to halve the full-filter
   write/load that the overhaul reintroduced, with split telemetry and an
   amplification guard (section 6). This is independently beneficial for t283's
   density and is low risk.
3. **Fast completed-spool verification (section 7)** so large production retries
   do not pay a full gob decode, and **fix the partial-progress log (section 9)**.

Hard gates before acceptance, on bounded subsets of the three failing mounts and
at least one healthy Lustre mount:

- a windowed non-contiguous subset of scratch122/scratch127/t283 summarises to
  completion with bounded peak scratch disk (a small constant per input line),
  not a depth-multiplied blow-up;
- failing-mount end-to-end wall within a small multiple of the contiguous fast
  path of comparable size;
- healthy contiguous Lustre mounts do not regress in wall, RSS, or spool bytes;
- full-filter write/load drops materially via child derivation, with split
  telemetry proving it;
- completed-spool retry verify drops from tens of seconds to sub-second while
  still checking size/hash/schema;
- `clickhouse-perf query` result counts/digests and p50/p95 do not regress on
  root, mount-root, and high-fanout paths.

Do not accept a candidate that fixes the failing mounts by regressing the
healthy ones, nor a seventh `non-contiguous directory input` patch that leaves
the ~26-29x sort and the depth-scaled disk writes (tens of GB to NFS) in place.
