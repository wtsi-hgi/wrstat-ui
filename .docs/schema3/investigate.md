# Prompt for ClickHouse schema3 performance investigation

Use this before writing a follow-up schema/query spec in `.docs/schema3`.

The goal is to find a storage and query design that gives consistently fast
cold performance for Disktree navigation and `where`, now that the schema2
work has been implemented and follow-up fixes have improved many hot paths.
The remaining problem is not "make the second request fast". The next design
must make first use fast for broad and filtered queries, virtual ancestors,
mount roots, high-fanout directories, and real `where --dir` subtrees.

Known current symptoms to reproduce before proposing a fix:

- Clicking into previously unseen Disktree folders can still feel sluggish.
- `where` can take about 33s on first run, then less than a second on retry.
- First waits for `where --dir <subdir>` are variable and can rise past 1m,
  apparently depending on how many subdirectories are involved.
- Page refreshes and repeated cached paths are now fast, so process-local and
  response caches can hide the real cold-path cost.

Do not hand wave this. If a bounded local subset cannot reproduce the same
class of cold-path failure, stop and say so. A design chosen without a local
failure mode is likely to optimize the wrong thing.

## What Is Already Implemented

The current branch already includes the main schema2 objects:

- `wrstat_dir_facts`: canonical exact directory facts, ordered by
  `(mount_path, snapshot_id, dir)`.
- `wrstat_children`: direct child edges, ordered by parent.
- `wrstat_parent_facts`: duplicate directory facts ordered by
  `(mount_path, snapshot_id, parent_dir, dir)`.
- `wrstat_dir_filter_ageall`: flattened AgeAll owner/type rows.
- `wrstat_active_prefix_rollups` and
  `wrstat_active_prefix_filter_ageall`: active-set rollups for `/`,
  `/lustre/`, `/nfs/`, and similar prefixes.
- `wrstat_virtual_children`: active-set virtual namespace children.
- Process-local tree query caches and server response caches keyed by active
  set, path, filters, permissions, and query version.

Do not repeat schema2 ideas as if they are new. In particular, do not propose
only:

- scalar active-prefix rollups;
- AgeAll-only filter rows;
- parent-ordered facts without proving repeated child clicks and filters;
- virtual ancestor summary cache that helps only `/`;
- cache warming as the final answer;
- path compression that only helps single-child chains;
- integer directory IDs by themselves;
- file-facts-only subtree scans;
- aggregate-state tables for primary tree summaries.

Those can be ingredients, but schema3 must be comprehensive.

## Comprehensive Means

A candidate is not comprehensive unless the same design covers all of these
without falling back to slow vector scans or repeated parent-range reads:

- root `/`, `/lustre/`, `/nfs/`, ordinary mount roots, and ordinary deep
  directories;
- broad, file-only/default, owner filters, user filters, file-type filters,
  owner+user+type filters, and age-specific filters, not only AgeAll;
- `DirInfo`, `DirInfos`, `Children`, `DirsHaveChildren`, Disktree endpoint
  navigation, and `Tree.Where`;
- one-shot cold provider, provider-update cold cache, repeated same-provider
  warm, and real REST/CLI-shaped `where`;
- high-fanout parents and many-mount active sets, including about 100 small
  NFS mounts above the real large Lustre/NFS mounts;
- correctness for totals, child presence, UID/GID/type/age summaries,
  timestamps, and virtual ancestor boxes.

Partial ideas may still be measured, but they must not be recommended as the
final schema3 answer unless paired with another layer that closes the remaining
gaps.

## Local Proof From This Pass

This file was seeded with a bounded proof run on 2026-06-11. The literal 33s
first `where` symptom was not reproduced on the small root REST case, but the
same class of cold high-fanout failure was reproduced strongly enough to
continue: broad focused child-summary checks hit 63-117s, and a filtered
focused child-summary check hit 459s before the command timed out.

Artifacts:

- Binary: `.tmp/agent/schema3/wrstat-ui`
- Dataset dir: `.tmp/agent/schema3/subset-mixed8`
- Mounts file: `.tmp/agent/schema3/mounts-mixed8.txt`
- Database: `wrstat_schema3_mixed8_094117`
- Import report: `.tmp/agent/schema3/perf/mixed8-import.json`
- Root query report: `.tmp/agent/schema3/perf/mixed8-query-root.json`
- High-fanout report: `.tmp/agent/schema3/perf/mixed8-query-highfanout.json`
- REST report: `.tmp/agent/schema3/perf/mixed8-rest-root.json`

Subset:

| Mount | Input cap | Notes |
|---|---:|---|
| `/nfs/t283_imaging/` | 250,000 | only available large NFS example |
| `/lustre/scratch120/` | 250,000 | large Lustre example |
| `/lustre/scratch122/` | 250,000 | large Lustre example |
| `/lustre/scratch123/` | all available, 1 row | effectively empty |
| `/lustre/scratch124/` | 250,000 | large Lustre example |
| `/lustre/scratch125/` | 250,000 | contains 11,205-child parent |
| `/lustre/scratch126/` | 250,000 | large Lustre example |
| `/lustre/scratch127/` | 250,000 | large Lustre example |

Import evidence:

- 1,750,001 input rows imported in 35.93s wall.
- Max RSS was 831,356 KB.
- Active table sizes after import:
  - `wrstat_files`: 1,750,001 rows, 35.68 MiB compressed.
  - `wrstat_dir_facts`: 197,179 rows, 30.03 MiB compressed,
    801.90 MiB uncompressed.
  - `wrstat_parent_facts`: 197,179 rows, 28.50 MiB compressed,
    822.50 MiB uncompressed.
  - `wrstat_dir_filter_ageall`: 493,588 rows, 11.35 MiB compressed.
  - `wrstat_children`: 197,171 rows, 3.99 MiB compressed.

Root semantics were validated:

| Path | Count | Size | Child count |
|---|---:|---:|---:|
| `/` | 1,750,001 | 61,484,536,134,482 | 8 |
| `/lustre/` | 1,500,001 | 61,176,182,464,512 | 7 |
| `/nfs/` | 250,000 | 308,353,669,970 | 1 |

Cold/warm evidence:

| Case | Timing | Read evidence | Notes |
|---|---:|---:|---|
| Root `tree_where_cold_provider`, splits 4 | p50 1,393 ms | not available from query log | Slower than warm but not production-scale 33s. |
| Root `tree_disktree_endpoint_cold_provider` | p50 62 ms | not available from query log | Root Disktree is not the reproduced bottleneck on this subset. |
| REST tree `/` | first 65 ms, then 1.5 ms and 0.3 ms | query count 16 then 1 and 1 | Response cache/process caches hide cold work. |
| REST `where /`, splits 2 | first 120 ms, then 0.3 ms and 1.6 ms | query count 33 then 1 and 1 | Literal REST root `where` still too small on this subset. |
| High-fanout parent Disktree | p50 199 ms broad; 848 ms type-filtered | endpoint path | Shows filter cost but not minute-scale by itself. |
| High-fanout parent `where` | p50 498 ms broad; 1,097 ms type-filtered | endpoint path | Smaller than reported production symptom. |
| High-fanout focused `DirInfos` broad | p50 63,482 ms, p95 116,770 ms | 66.6M-206.6M rows, 25.9-45.4GB per sample | Reproduces 1m+ cold child-summary failure. |
| High-fanout focused `DirsHaveChildren` broad | p50 63,273 ms | 66.6M rows, 25.9GB per sample | Same cold fanout class. |
| High-fanout focused `DirInfos` type-filtered | 458,951 ms before timeout | 2.90B rows, 898.6GB | Filtered proof is worse; command timed out before filtered `DirsHaveChildren`. |
| Single parent-facts range for same parent | p50 about 12 ms | 11,205 child rows | The single ordered read is cheap; repeated independent reads multiply it. |

The high-fanout parent was:

```text
/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/
```

It had 11,205 direct children and 92,529 vector entries under those child rows.
The evidence suggests the current endpoint can read the parent once, but other
API shapes can repeatedly reread the same parent range and its descendants
while only retaining the one requested child. Schema3 must remove this
possibility, not merely make one endpoint path lucky.

## Ground Rules

- Use writable subagents for bounded design experiments when the investigation
  gets wider than one local probe. Follow `/home/ubuntu/.agents/skills/subagents/SKILL.md`.
- Keep prototype code, scratch SQL, reports, temporary binaries, and notes in
  `.tmp/agent/schema3/`.
- Use a unique ClickHouse database per experiment.
- Do not summarise complete example mounts. Use bounded `--maxLines` subsets,
  selected mounts, or synthetic small-mount copies only when the synthetic data
  preserves the tree/query semantics being tested.
- A root `/` test over a subset must aggregate every selected active Lustre and
  NFS mount and expose separate `/lustre` and `/nfs` boxes with their totals.
- If simulating about 100 small NFS mounts, keep `/nfs` virtual. Do not turn
  `/nfs` itself into the configured mount unless that is the production shape
  being tested.
- Reset process-local query caches between cold measurements. Record whether a
  timing is cold provider, provider-update cold cache, fresh provider per run,
  REST first request, REST repeated request, CLI-shaped cached request, or
  browser/React timing.
- Record commands, row counts, active parts, compressed/uncompressed bytes,
  query counts, cache hits/misses, read rows/bytes/marks, result counts,
  result digests, JSON/gzip bytes, and p50/p95/p99.
- `system.query_log` may be unavailable in local ClickHouse. If so, use
  `clickhouse-perf` inspector metrics, `system.events` deltas, and targeted
  `EXPLAIN indexes=1`/`clickhouse-benchmark` probes.
- Do not recommend a candidate that only wins one filter mode, one endpoint, or
  one warmed-cache state.

Useful command shapes:

```bash
timeout 600s env CGO_ENABLED=1 go test -tags netgo --count 1 ./clickhouse ./cmd ./internal/chperf ./server
timeout 300s env CGO_ENABLED=1 go build -o .tmp/agent/schema3/wrstat-ui .

timeout 1h .tmp/agent/schema3/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_schema3_<name>&compress=lz4" \
  -D wrstat_schema3_<name> \
  --mounts .tmp/agent/schema3/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/schema3/perf/<name>-import.json \
  import .tmp/agent/schema3/subset --maxLines 250000 --batchSize 100000 --parallelism 1

timeout 20m .tmp/agent/schema3/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_schema3_<name>&compress=lz4" \
  -D wrstat_schema3_<name> \
  --mounts .tmp/agent/schema3/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/schema3/perf/<name>-query-root.json \
  query --dir / --ancestor-dir / --ancestor-limit 16 --repeat 3 --warmup 0 \
  --ops tree_disktree_endpoint_cold_provider,tree_disktree_endpoint_provider_update_cold_cache,tree_disktree_endpoint_ancestor_dirs,tree_where_cold_provider,tree_where_cold_then_cached,virtual_dirinfo

timeout 20m .tmp/agent/schema3/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_schema3_<name>&compress=lz4" \
  -D wrstat_schema3_<name> \
  --mounts .tmp/agent/schema3/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/schema3/perf/<name>-query-highfanout.json \
  query --dir "/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/" \
  --ancestor-dir /lustre --ancestor-limit 16 --repeat 3 --warmup 0 \
  --ops tree_disktree_endpoint_cold_provider,tree_where_cold_provider,dirinfos_broad,dirshavechildren_broad

timeout 10m .tmp/agent/schema3/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_schema3_<name>&compress=lz4" \
  -D wrstat_schema3_<name> \
  --mounts .tmp/agent/schema3/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/schema3/perf/<name>-rest-root.json \
  rest --paths /,/lustre,/nfs --where-dir / --repeat 3 --warmup 0 --splits 2
```

## Baseline Setup Checklist

- [x] Read `.docs/schema2/investigate.md`, `.docs/schema2/diff.md`, the
  current schema DDL, and current tree query code.
  Result: current schema2 objects are listed above. The main new evidence is
  not missing scalar rollups; it is repeated cold high-fanout reads and
  filtered child-summary blowups.
- [x] Build a current binary in `.tmp/agent/schema3/wrstat-ui`.
  Result: `env CGO_ENABLED=1 go build -o .tmp/agent/schema3/wrstat-ui .`
  succeeded.
- [x] Create a mixed Lustre+NFS subset from `/home/ubuntu/output/lustre` and
  `/home/ubuntu/output/nfs`.
  Result: `.tmp/agent/schema3/subset-mixed8` contains real dataset directories
  with symlinked `stats.gz` files for all available example mounts.
- [x] Use a quoted mountpoints file listing actual mount roots, not virtual
  `/lustre` or `/nfs`.
  Result: `.tmp/agent/schema3/mounts-mixed8.txt`.
- [x] Import a bounded subset into a fresh ClickHouse database and capture row
  counts, bytes, import time, and memory.
  Result: `wrstat_schema3_mixed8_094117`, 1.75M records, 35.93s wall,
  831,356 KB max RSS.
- [x] Verify root semantics on the subset.
  Result: `/` equals all selected active mount roots; `/lustre/` and `/nfs/`
  active-prefix rollups have the expected separate totals.
- [x] Measure current cold and warm root Disktree and `where`.
  Result: small root REST case is fast after first request and did not
  reproduce 33s; root in-process `tree_where_cold_provider` was about 1.4s.
- [x] Find and measure a high-fanout directory.
  Result: the 11,205-child scratch125 parent reproduced minute-scale broad
  focused `DirInfos`/`DirsHaveChildren` and a 459s filtered focused
  `DirInfos` sample.
- [ ] Reproduce or fail to reproduce literal first `./wrstat-ui where --dir`
  behavior against a larger subset or a production-like server path. Do not
  rely on the REST response cache or a pre-warmed provider.
- [ ] Simulate about 100 small NFS mounts and remeasure `/`, `/nfs/`, first
  click into a small NFS mount, and root `where`. Record whether active-prefix
  rollups still hide the mount-count cost.
- [ ] Increase the NFS-heavy subset only as far as needed to show whether
  first `where` scales toward the reported 33s. Stop before full example
  summarise/import work.
- [ ] Record same-subset Bolt or sidecar target numbers if available. The
  schema2 correction pass measured Bolt as orders of magnitude faster on an
  earlier subset; schema3 needs the same comparison for the new high-fanout
  proof case.

## Investigation Checklist

### 1. Trace The Real Cold Request Graph

Question: which public calls can still multiply a cheap parent read into a
minute-scale request?

- [ ] Add request-scoped tracing for every tree call: `DirInfo`, `DirInfos`,
  `Children`, `DirsHaveChildren`, `Where`, cache hit/miss, SQL shape, read
  rows/bytes, and result count.
- [ ] For root, `/lustre/`, `/nfs/`, mount roots, and the 11,205-child parent,
  record the exact call graph for Disktree, REST `where`, CLI-shaped `where`,
  focused `DirInfos`, and focused `DirsHaveChildren`.
- [ ] Prove whether separate `DirInfo(child)` calls under a high-fanout parent
  reread the same parent-facts range and/or descendant child checks.
- [ ] Record how much of the time is one SQL query, repeated SQL, Go
  traversal, JSON serialization, gzip, or React rendering.
- [ ] A fix candidate must reduce query count and read bytes in the worst
  high-fanout/filter cases, not only lower p50 for the endpoint path.

### 2. Exact-Dir Plus Parent-Packet Routing

Question: can the current schema become safe by changing the serving contract:
exact directory summary by `dir`, child packet by `parent_dir`, never parent
range for a single current summary?

- [ ] Prototype routing `DirInfo(dir)` as:
  1. exact current summary from `wrstat_dir_facts` or a narrow exact-serving
     table keyed by `(mount_path, snapshot_id, dir)`;
  2. child summaries from a parent packet keyed by
     `(mount_path, snapshot_id, parent_dir = dir)`;
  3. `has_children` from the same child packet.
- [ ] When a parent packet is read, cache or return all child summaries and
  child counts as a coherent packet, so a later `DirInfo(child)` does not
  reread the parent's sibling range.
- [ ] Make this work for broad, file-only, owner, user, type, owner+user+type,
  AgeAll, and age-specific filters. If age-specific falls back to arrays, it is
  not comprehensive.
- [ ] Measure high-fanout focused `DirInfos` and `DirsHaveChildren` broad and
  filtered. The 63s/459s proof cases must fall to bounded milliseconds or low
  hundreds of milliseconds cold.
- [ ] Decide whether the current `wrstat_dir_facts` plus `wrstat_parent_facts`
  can support this, or whether a new packet table is needed to avoid accidental
  partial caching.

### 3. Directory Packet Serving Table

Question: should the primary interactive object be one packet per directory
that contains everything needed for `DirInfo`, visible children, child
summaries, and child presence?

- [ ] Prototype `wrstat_tree_packets` or similar, keyed by
  `(active_set_id or mount_path/snapshot_id, dir)`.
- [ ] Each packet must include the directory's exact summary, child names,
  child scalar summaries, child filter vectors or all-dimension filter rows,
  child child-counts, timestamps, and virtual ancestor metadata.
- [ ] Provide both exact lookup by `dir` and parent lookup by `parent_dir`
  without reading unrelated siblings repeatedly.
- [ ] Cover virtual ancestors and 100 small NFS mounts, either by active-set
  packets or by ordinary mount packets plus a virtual packet layer.
- [ ] Test broad and every filter family. A scalar-only packet is rejected.
- [ ] Record packet row size, compressed bytes, max child-array size, memory
  required to scan one packet, import write time, and cleanup behavior.
- [ ] If high-fanout packet rows become too wide, split packet payloads by
  filter family or child pages, but keep the public serving contract one
  bounded packet read per directory.

### 4. All-Filter Child Serving Rows

Question: should schema3 fully flatten child summaries by filter dimensions so
filtered navigation never unpacks vectors or rescans siblings?

- [ ] Prototype a comprehensive child filter table such as
  `wrstat_child_filter_facts` with keys like
  `(mount_path, snapshot_id, parent_dir, gid, uid, ft, age, child_dir)`.
- [ ] Include AgeAll rows and every age-specific bucket needed by current UI
  filters. AgeAll-only is not enough.
- [ ] Include enough summary columns to answer `DirInfo` children,
  `DirsHaveChildren`, and `where` traversal without a second table scan.
- [ ] Test filters that are selective and filters that match most children;
  both must be fast.
- [ ] Measure row amplification, compressed/uncompressed bytes, import CPU,
  spool size, ClickHouse part count, and memory.
- [ ] Decide whether to store `(gid, uid, ft, age)` combined rows, separate
  per-dimension postings with query-time set algebra, or both.
- [ ] Reject the design if it solves type filters but not UID/GID+age or if it
  makes import/summarise regress beyond acceptable limits.

### 5. Active-Set Serving Tree

Question: should all active tree navigation be copied into one active-set
serving layout so root, virtual ancestors, mount roots, and ordinary dirs share
one query model?

- [ ] Prototype active-set keyed serving tables for exact facts, parent
  packets, child facts, and filter facts.
- [ ] Publish them atomically by `active_set_id` after every active mount
  change. Readers should not join `wrstat_mounts_active` on hot paths.
- [ ] Include virtual ancestor rows for `/`, `/lustre/`, `/nfs/`, and all
  intermediate virtual parents above mount roots.
- [ ] Simulate 100 small NFS mounts and measure active-set build time, storage,
  cleanup, and first query latency.
- [ ] Compare incremental rebuild when one mount changes versus rebuilding the
  full active-set serving layer.
- [ ] Reject if active-set rebuild cost makes every publish expensive, unless
  the design has a proven incremental swap plan.

### 6. Where Frontier Facts

Question: does `where` need its own precomputed frontier object rather than
being an emergent behavior of repeated `DirInfo` calls?

- [ ] Trace `where` with splits 0, 1, 2, and 4 for root, `/nfs/`,
  `/nfs/t283_imaging/`, the high-fanout scratch125 parent, and selected deep
  directories.
- [ ] Prototype `wrstat_where_frontiers` storing, for each directory and filter
  dimension set, the next split frontier, count, size, time buckets, and child
  frontier membership.
- [ ] Include broad, file-only, AgeAll, and age-specific filters. Do not build
  a frontier that only works for no filter or only for `types=other`.
- [ ] Measure whether `where --dir X --splits N` can be answered in a bounded
  number of reads independent of subtree size and child fanout.
- [ ] Model row amplification for all directories and filters. If full
  precompute is too large, test frontier pages or lazy persisted packets, but
  keep first cold query bounded.
- [ ] Compare with simply fixing `DirInfo` packet routing. Choose a separate
  frontier only if packet routing cannot make `where` consistently fast.

### 7. Bitmap Or Posting-List Tree Index

Question: would a set-algebra index give comprehensive filter performance with
less duplication than fully expanded child rows?

- [ ] Prototype directory/subtree bitmaps or postings for UID, GID, file type,
  age, and directory descendants using ClickHouse bitmap functions, roaring
  bitmaps, or an embedded side index.
- [ ] Prove exact semantics for arbitrary combinations:
  GID OR sets, UID OR sets, type bitmasks, age buckets, and default
  all-types-except-directories.
- [ ] Measure root, high-fanout, and filtered `where` using set intersections
  and tree boundaries.
- [ ] Record storage size and update/build cost. Bitmap rows per ancestor can
  explode; quantify that before recommending it.
- [ ] Reject if query time is fast only for selective filters but slow for
  broad filters, or vice versa.

### 8. Embedded Navigation Sidecar

Question: if ClickHouse-native serving tables remain too slow or too expensive,
should the interactive tree use a compact Bolt-like sidecar generated from the
same snapshot data?

- [ ] Prototype or model a sidecar containing exact directory packets,
  parent-child packets, all filter vectors/rows, virtual ancestors, and where
  frontiers.
- [ ] Compare Bolt, SQLite, RocksDB/Badger, mmap files, and a compact custom
  binary format. The sidecar must be read-optimized, not a second ad hoc
  database with slow cold scans.
- [ ] Preserve atomic publish: a sidecar version becomes visible only with the
  corresponding active set or active snapshot set.
- [ ] Measure build time, storage, memory mapping cost, first lookup latency,
  cleanup, and operational recovery.
- [ ] Keep ClickHouse as the source of truth for files, history, basedirs, and
  audit unless the sidecar proves it should own more.
- [ ] Recommend this fallback if ClickHouse-native candidates cannot approach
  same-subset Bolt targets for high-fanout focused queries and first `where`.

### 9. Import And Spool Consequences

Question: can the selected serving layer be built during real summarise/spool
without recreating the schema2 import regressions?

- [ ] Map which current entrypoints populate `wrstat_dir_filter_ageall`,
  `wrstat_parent_facts`, and active-prefix rollups: direct writer,
  `clickhouse-perf import`, production summarise spool, retry/resume, and
  cleanup.
- [ ] For each schema3 candidate, define direct-writer rows, spool files,
  spool manifest verification, ClickHouse insert order, readiness rows, active
  publish, and old partition cleanup.
- [ ] Measure import wall time, peak RSS, local spool bytes, ClickHouse parts,
  and retry cleanup on t283 and scratch125 subsets.
- [ ] Reject post-import `INSERT ... SELECT` rebuilds unless they are bounded,
  use cleanup deadlines, and cannot leave partial serving data visible.
- [ ] Preserve deterministic snapshot IDs and active snapshot atomicity.

### 10. Physical Layout And Query-Shape Tuning

Question: are there simple ClickHouse layout fixes that are comprehensive when
combined with packet routing?

- [ ] Test projections versus physical duplicate tables for exact-dir,
  parent-dir, and filter-order access. Count a projection only if
  `EXPLAIN indexes=1` proves the real query uses it reliably.
- [ ] Test smaller `index_granularity` for high-fanout parent/filter tables and
  record storage/import effects.
- [ ] Test codecs and `LowCardinality` on high-cardinality path columns; paths
  compress well but can still hurt marks and read amplification.
- [ ] Compare large `IN` lists, external temporary tables, `Join` engine
  dictionaries, and array binds for any remaining batch query.
- [ ] Test whether query cache settings help cold provider paths only after
  the underlying read amplification is removed.
- [ ] Reject tuning-only changes if high-fanout focused broad/filtered probes
  still read millions or billions of rows.

### 11. Tactical Cache Warming And Response Caching

Question: what is safe as a stopgap while schema3 is built?

- [ ] Prototype warming root, `/lustre/`, `/nfs/`, mount roots, and high-fanout
  parent packets after provider open or active-set publish.
- [ ] Cache whole directory packets, not just the one requested child, when a
  parent range is read.
- [ ] Include filter and age in the cache key. Do not let AgeAll cache entries
  answer age-specific queries.
- [ ] Measure provider-open cost, active-update cost, memory growth, and
  invalidation on snapshot publish/tombstone.
- [ ] Treat this as tactical only. If cold filtered focused `DirInfos` can hit
  459s without cache, the final answer cannot be "warm it first".

### 12. Perf Harness And Gates

Question: what must be measured before schema3 is accepted?

- [ ] Add or preserve operations for high-fanout focused `DirInfos` and
  `DirsHaveChildren` with broad and filtered inputs. These are now required
  gates, not optional diagnostics.
- [ ] Add a real first `./wrstat-ui where --dir` measurement that does not
  run after the REST response cache is already warm.
- [ ] Add browser/React timing for first Disktree click into a high-fanout
  folder and for switching filters.
- [ ] Add result digest/equivalence checks for every optional serving table and
  cache. Fast stale or partial answers are failures.
- [ ] Add per-operation query count, cache hit/miss count, ClickHouse read
  rows/bytes/marks, JSON/gzip bytes, and result counts to every report.
- [ ] Define hard gates on the mixed subset, a larger NFS-heavy subset, a
  100-small-NFS simulation, and the high-fanout scratch125 parent.

Suggested starting gates:

| Scenario | Gate |
|---|---:|
| REST tree `/`, `/lustre/`, `/nfs/` first request | p95 under 500 ms |
| First click into high-fanout parent | p95 under 500 ms broad and filtered |
| Focused `DirInfos` high-fanout broad | p95 under 1s, read bytes bounded |
| Focused `DirInfos` high-fanout filtered | p95 under 1s, read bytes bounded |
| Focused `DirsHaveChildren` high-fanout broad/filtered | p95 under 1s |
| First root `where`, splits 2 | p95 under 1s on bounded subset |
| First `where --dir` high-fanout and NFS-heavy dirs | p95 under 2s |
| Same-subset Bolt/sidecar comparison | no more than 10% slower where feasible; otherwise justify with evidence |
| Import/summarise | no unacceptable wall-time/RSS regression versus current branch |

## Comparison Matrix

Maintain this table as experiments complete.

| Design | Objects added/removed | Broad Disktree | Filtered Disktree | Focused high-fanout `DirInfos` | Focused high-fanout `DirsHaveChildren` | First `where` | 100 small NFS mounts | Import/RSS/spool cost | Storage cost | Correctness risk | Recommendation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| Current schema2 branch | Existing facts, parent facts, AgeAll, active-prefix rollups, caches | root fast; high-fanout endpoint 199 ms | high-fanout endpoint 848 ms for `types=other` | broad p50 63s, filtered 459s | broad p50 63s | root REST fast on small subset; in-process root 1.4s | open | current import 35.9s/831 MB for 1.75M rows | current facts 30 MiB, parent facts 28.5 MiB, AgeAll 11.35 MiB | repeated parent range reads, cache hiding | Baseline only |
| Exact-dir plus parent-packet routing | Maybe no new table; stricter use of exact facts plus packet cache | open | open | open | open | open | open | likely low | low | routing misses could reintroduce slow path | Test first |
| Directory packet table | Add packet/child payload table(s) | open | open | open | open | open | open | open | open | wide rows, packet paging | Candidate |
| All-filter child serving rows | Add comprehensive child filter facts | open | open | open | open | open | open | likely high | high | row explosion | Candidate if measured |
| Active-set serving tree | Add active-set exact/parent/filter rows | open | open | open | open | open | open | active rebuild cost open | open | active-set publish/invalidation | Candidate |
| Where frontier facts | Add frontier table(s) | indirect | indirect | indirect | indirect | open | open | likely high | high | split/filter correctness | Candidate only if packet routing fails |
| Bitmap/posting-list index | Add bitmap/postings layer | open | open | open | open | open | open | open | open | exact set semantics | Radical candidate |
| Embedded navigation sidecar | Add Bolt/SQLite/RocksDB/mmap sidecar | open | open | open | open | open | open | build cost open | open | dual-store publish | Radical fallback |
| Cache warming only | No schema; warm packets/responses | helps repeats | only if warmed | hides but does not fix | hides but does not fix | hides but does not fix | open | provider-update cost | memory | invalidation | Stopgap only |

## Final Recommendation

End the investigation by editing this section in place.

- [ ] State the recommended design direction and why it is comprehensive.
- [ ] State exactly which schema objects are added, removed, or repurposed.
- [ ] State which prior schema2 objects remain canonical and which become
  compatibility/diagnostic only.
- [ ] State rejected alternatives with measured evidence, especially any idea
  that only helps no-filter, AgeAll-only, virtual-root-only, or warm-cache
  cases.
- [ ] State writer, spool, readiness, active publish, cleanup, and cache
  invalidation changes.
- [ ] State the perf gates, datasets, simulated mount count, and commands that
  must pass before implementation is considered complete.
- [ ] If no ClickHouse-native candidate makes cold page refresh, high-fanout
  clicks, filters, and first `where` consistently fast, recommend the least
  risky sidecar design rather than layering more process-local caches over a
  slow cold path.

