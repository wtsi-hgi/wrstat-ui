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
  Result: still missing. Worker A found the `cli_where` record in
  `mixed8-rest-root.json` was handler-shaped and ran after REST `where` had
  warmed response caches, so it is not a real fresh-process CLI measurement.
- [ ] Simulate about 100 small NFS mounts and remeasure `/`, `/nfs/`, first
  click into a small NFS mount, and root `where`. Record whether active-prefix
  rollups still hide the mount-count cost.
  Result: still missing. Worker B modeled active virtual filter overhead as
  small on mixed8, but did not run the 100-small-NFS simulation.
- [ ] Increase the NFS-heavy subset only as far as needed to show whether
  first `where` scales toward the reported 33s. Stop before full example
  summarise/import work.
  Result: still missing.
- [ ] Record same-subset Bolt or sidecar target numbers if available. The
  schema2 correction pass measured Bolt as orders of magnitude faster on an
  earlier subset; schema3 needs the same comparison for the new high-fanout
  proof case.
  Result: still missing for the mixed8 high-fanout proof. Worker C recorded
  prior Bolt targets on the earlier schema2 subset and modeled mixed8 sidecar
  storage, but no same-subset high-fanout sidecar run exists yet.

## Investigation Checklist

### 1. Trace The Real Cold Request Graph

Question: which public calls can still multiply a cheap parent read into a
minute-scale request?

- [ ] Add request-scoped tracing for every tree call: `DirInfo`, `DirInfos`,
  `Children`, `DirsHaveChildren`, `Where`, cache hit/miss, SQL shape, read
  rows/bytes, and result count.
- [x] For root, `/lustre/`, `/nfs/`, mount roots, and the 11,205-child parent,
  record the exact call graph for Disktree, REST `where`, CLI-shaped `where`,
  focused `DirInfos`, and focused `DirsHaveChildren`.
  Result: Worker A traced REST Disktree, REST `where`, focused `DirInfos`, and
  focused `DirsHaveChildren`. A real fresh CLI `where` gate is still needed;
  the cached handler-shaped record is not enough.
- [x] Prove whether separate `DirInfo(child)` calls under a high-fanout parent
  reread the same parent-facts range and/or descendant child checks.
  Result: yes. Focused `DirInfos` loops over 11,205 children with separate
  public `Tree.DirInfo(child)` calls; steady-state samples read 11,211 marks
  and 66.6M rows for 11,206 logical dirs.
- [ ] Record how much of the time is one SQL query, repeated SQL, Go
  traversal, JSON serialization, gzip, or React rendering.
  Result: repeated SQL/read amplification is proven. JSON, gzip, and React
  first-click timing remain unmeasured.
- [x] A fix candidate must reduce query count and read bytes in the worst
  high-fanout/filter cases, not only lower p50 for the endpoint path.
  Result: accepted as a gate. Root REST and the high-fanout endpoint are not
  sufficient because batching/caches already make those paths look fast.

### 2. Exact-Dir Plus Parent-Packet Routing

Question: can the current schema become safe by changing the serving contract:
exact directory summary by `dir`, child packet by `parent_dir`, never parent
range for a single current summary?

- [x] Prototype routing `DirInfo(dir)` as:
  1. exact current summary from `wrstat_dir_facts` or a narrow exact-serving
     table keyed by `(mount_path, snapshot_id, dir)`;
  2. child summaries from a parent packet keyed by
     `(mount_path, snapshot_id, parent_dir = dir)`;
  3. `has_children` from the same child packet.
  Result: Worker A/B proved the read shape with existing tables. Exact target
  dir lookups from `wrstat_dir_facts` were 6-11 ms; one high-parent packet from
  `wrstat_parent_facts` was 8-12 ms scalar or 18-28 ms with vectors.
- [x] When a parent packet is read, cache or return all child summaries and
  child counts as a coherent packet, so a later `DirInfo(child)` does not
  reread the parent's sibling range.
  Result: required serving contract. Current code caches only requested
  parent-fact rows, so schema3 must cache/return complete packets keyed by
  mount, snapshot, parent, filter mode, and age.
- [ ] Make this work for broad, file-only, owner, user, type, owner+user+type,
  AgeAll, and age-specific filters. If age-specific falls back to arrays, it is
  not comprehensive.
  Result: broad/default is covered by parent packets. Comprehensive filtered
  coverage requires the all-filter child/dir serving layer below.
- [ ] Measure high-fanout focused `DirInfos` and `DirsHaveChildren` broad and
  filtered. The 63s/459s proof cases must fall to bounded milliseconds or low
  hundreds of milliseconds cold.
  Result: not end-to-end implemented yet. Direct SQL proves the packet shape is
  cheap; the actual public calls must still be changed and gated.
- [x] Decide whether the current `wrstat_dir_facts` plus `wrstat_parent_facts`
  can support this, or whether a new packet table is needed to avoid accidental
  partial caching.
  Result: current exact facts plus parent facts can support the first step.
  The separate directory packet table was slower and is rejected as primary.

### 3. Directory Packet Serving Table

Question: should the primary interactive object be one packet per directory
that contains everything needed for `DirInfo`, visible children, child
summaries, and child presence?

- [x] Prototype `wrstat_tree_packets` or similar, keyed by
  `(active_set_id or mount_path/snapshot_id, dir)`.
  Result: Worker B built `wrstat_dir_packets` with one nested child packet per
  `(mount_path, snapshot_id, parent_dir)`.
- [ ] Each packet must include the directory's exact summary, child names,
  child scalar summaries, child filter vectors or all-dimension filter rows,
  child child-counts, timestamps, and virtual ancestor metadata.
  Result: not completed because the candidate lost before full coverage; the
  prototype represented child facts/vectors but not full active virtual and
  all-filter serving metadata.
- [ ] Provide both exact lookup by `dir` and parent lookup by `parent_dir`
  without reading unrelated siblings repeatedly.
  Result: parent packet lookup was measured; exact-dir routing remains better
  served by `wrstat_dir_facts`.
- [ ] Cover virtual ancestors and 100 small NFS mounts, either by active-set
  packets or by ordinary mount packets plus a virtual packet layer.
- [ ] Test broad and every filter family. A scalar-only packet is rejected.
- [x] Record packet row size, compressed bytes, max child-array size, memory
  required to scan one packet, import write time, and cleanup behavior.
  Result: prototype had 50,430 packet rows representing 197,179 child rows and
  3,488,307 vector entries, 33.15 MiB compressed, 808.23 MiB uncompressed, and
  max packet children 11,205. Target fetch was p50 21 ms/p95 42 ms; `arrayJoin`
  was p50 22 ms/p95 43 ms.
- [x] If high-fanout packet rows become too wide, split packet payloads by
  filter family or child pages, but keep the public serving contract one
  bounded packet read per directory.
  Result: rejected as primary before paging. It did not beat
  `wrstat_parent_facts` and still needs a separate filter layer.

### 4. All-Filter Child Serving Rows

Question: should schema3 fully flatten child summaries by filter dimensions so
filtered navigation never unpacks vectors or rescans siblings?

- [x] Prototype a comprehensive child filter table such as
  `wrstat_child_filter_facts` with keys like
  `(mount_path, snapshot_id, parent_dir, gid, uid, ft, age, child_dir)`.
  Result: Worker B built `wrstat_child_filter_rows`, keyed by parent and full
  filter dimensions, plus companion `wrstat_dir_filter_all` for subtree probes.
- [x] Include AgeAll rows and every age-specific bucket needed by current UI
  filters. AgeAll-only is not enough.
  Result: full vector expansion from `wrstat_dir_facts` produced 3,488,307 rows
  versus 493,588 AgeAll rows.
- [x] Include enough summary columns to answer `DirInfo` children,
  `DirsHaveChildren`, and `where` traversal without a second table scan.
  Result: prototype rows carried count/size/timestamps in the child-serving
  shape; final schema still needs digest gates for every UI summary field.
- [x] Test filters that are selective and filters that match most children;
  both must be fast.
  Result: target parent timings were 44-77 ms for AgeAll+gid, 37-50 ms for
  age+gid, and 27-38 ms for age+gid+uid+ft. Candidate subtree `where --dir`
  timing was 47-59 ms for age+gid+uid+ft; AgeAll+gid was 77-93 ms and did not
  beat the existing AgeAll table.
- [ ] Measure row amplification, compressed/uncompressed bytes, import CPU,
  spool size, ClickHouse part count, and memory.
  Result: row/storage amplification was measured, but import CPU, spool size,
  part count, and memory were not.
- [x] Decide whether to store `(gid, uid, ft, age)` combined rows, separate
  per-dimension postings with query-time set algebra, or both.
  Result: use full combined child/dir rows for ClickHouse-native schema3;
  reserve postings/bitmaps as a sidecar primitive, not the primary ClickHouse
  table model.
- [x] Reject the design if it solves type filters but not UID/GID+age or if it
  makes import/summarise regress beyond acceptable limits.
  Result: not rejected on filter coverage; it covers age-specific
  owner+user+type. It remains gated on import/spool cost.

### 5. Active-Set Serving Tree

Question: should all active tree navigation be copied into one active-set
serving layout so root, virtual ancestors, mount roots, and ordinary dirs share
one query model?

- [x] Prototype active-set keyed serving tables for exact facts, parent
  packets, child facts, and filter facts.
  Result: Worker B built a physical active-set child-fact copy ordered by
  `(active_set_id, parent_dir, dir)`. Target parent p50/p95 was 19/25 ms, but
  full physical duplication is not recommended as the default.
- [ ] Publish them atomically by `active_set_id` after every active mount
  change. Readers should not join `wrstat_mounts_active` on hot paths.
- [x] Include virtual ancestor rows for `/`, `/lustre/`, `/nfs/`, and all
  intermediate virtual parents above mount roots.
  Result: virtual full-filter overhead was modeled as 6,134 rows on mixed8,
  small enough to keep as an active-set overlay on physical parent/filter
  tables.
- [ ] Simulate 100 small NFS mounts and measure active-set build time, storage,
  cleanup, and first query latency.
- [ ] Compare incremental rebuild when one mount changes versus rebuilding the
  full active-set serving layer.
- [x] Reject if active-set rebuild cost makes every publish expensive, unless
  the design has a proven incremental swap plan.
  Result: reject physical duplication of all parent facts as the primary
  active-set strategy. Recommend only a small virtual overlay unless
  production active-set evidence justifies more.

### 6. Where Frontier Facts

Question: does `where` need its own precomputed frontier object rather than
being an emergent behavior of repeated `DirInfo` calls?

- [ ] Trace `where` with splits 0, 1, 2, and 4 for root, `/nfs/`,
  `/nfs/t283_imaging/`, the high-fanout scratch125 parent, and selected deep
  directories.
- [ ] Prototype `wrstat_where_frontiers` storing, for each directory and filter
  dimension set, the next split frontier, count, size, time buckets, and child
  frontier membership.
- [x] Include broad, file-only, AgeAll, and age-specific filters. Do not build
  a frontier that only works for no filter or only for `types=other`.
  Result: Worker C modeled these requirements and found exact tuple and
  multi-select filter coverage are the hard part; broad-only frontiers are not
  comprehensive.
- [ ] Measure whether `where --dir X --splits N` can be answered in a bounded
  number of reads independent of subtree size and child fanout.
- [x] Model row amplification for all directories and filters. If full
  precompute is too large, test frontier pages or lazy persisted packets, but
  keep first cold query bounded.
  Result: exact AgeAll tuple memberships are about 493k and exact
  age-specific tuple memberships are about 3.49M before frontier payloads.
  Storing all selected-set combinations would grow combinatorially.
- [x] Compare with simply fixing `DirInfo` packet routing. Choose a separate
  frontier only if packet routing cannot make `where` consistently fast.
  Result: standalone where-frontiers are rejected/deferred. Packet routing plus
  all-filter child/dir rows addresses more measured failures with less
  query-version surface area.

### 7. Bitmap Or Posting-List Tree Index

Question: would a set-algebra index give comprehensive filter performance with
less duplication than fully expanded child rows?

- [ ] Prototype directory/subtree bitmaps or postings for UID, GID, file type,
  age, and directory descendants using ClickHouse bitmap functions, roaring
  bitmaps, or an embedded side index.
- [x] Prove exact semantics for arbitrary combinations:
  GID OR sets, UID OR sets, type bitmasks, age buckets, and default
  all-types-except-directories.
  Result: Worker C specified exact tuple and dimension posting semantics with
  DFS intervals and prefix payloads; this is a design proof, not an
  implementation proof.
- [ ] Measure root, high-fanout, and filtered `where` using set intersections
  and tree boundaries.
- [x] Record storage size and update/build cost. Bitmap rows per ancestor can
  explode; quantify that before recommending it.
  Result: modeled raw posting sizes were 10.4 MB for dimension dir-id lists,
  2.0 MB for exact AgeAll tuple lists, and 14.0 MB for exact age-specific tuple
  lists, before summary payloads. Build should target 10-30s extra on mixed8
  if integrated with import.
- [x] Reject if query time is fast only for selective filters but slow for
  broad filters, or vice versa.
  Result: do not make ClickHouse bitmap/postings the primary schema3 answer.
  Use postings as a sidecar primitive because dense filters still need summary
  payloads and virtual active-set handling.

### 8. Embedded Navigation Sidecar

Question: if ClickHouse-native serving tables remain too slow or too expensive,
should the interactive tree use a compact Bolt-like sidecar generated from the
same snapshot data?

- [x] Prototype or model a sidecar containing exact directory packets,
  parent-child packets, all filter vectors/rows, virtual ancestors, and where
  frontiers.
  Result: Worker C modeled Bolt, SQLite, purpose-built mmap/Roaring, and
  RocksDB/Badger/LMDB candidates.
- [x] Compare Bolt, SQLite, RocksDB/Badger, mmap files, and a compact custom
  binary format. The sidecar must be read-optimized, not a second ad hoc
  database with slow cold scans.
  Result: purpose-built immutable mmap/Roaring is the strongest production
  fallback; SQLite is useful for prototype/audit; Bolt is the fastest reuse
  path but needs an active-set aggregate redesign; RocksDB/Badger/LMDB are not
  recommended first.
- [x] Preserve atomic publish: a sidecar version becomes visible only with the
  corresponding active set or active snapshot set.
  Result: Worker C specified staging, checksum validation, content-addressed
  version directories, atomic pointer publish, reader grace period, and cleanup.
- [ ] Measure build time, storage, memory mapping cost, first lookup latency,
  cleanup, and operational recovery.
  Result: estimates exist, but no mixed8 high-fanout sidecar build/run was
  performed.
- [x] Keep ClickHouse as the source of truth for files, history, basedirs, and
  audit unless the sidecar proves it should own more.
- [x] Recommend this fallback if ClickHouse-native candidates cannot approach
  same-subset Bolt targets for high-fanout focused queries and first `where`.
  Result: immutable active-set navigation sidecar is the explicit fallback if
  ClickHouse-native packet/filter gates fail.

### 9. Import And Spool Consequences

Question: can the selected serving layer be built during real summarise/spool
without recreating the schema2 import regressions?

- [ ] Map which current entrypoints populate `wrstat_dir_filter_ageall`,
  `wrstat_parent_facts`, and active-prefix rollups: direct writer,
  `clickhouse-perf import`, production summarise spool, retry/resume, and
  cleanup.
- [x] For each schema3 candidate, define direct-writer rows, spool files,
  spool manifest verification, ClickHouse insert order, readiness rows, active
  publish, and old partition cleanup.
  Result: final recommendation below defines the required publish/spool shape,
  but implementation entrypoint mapping remains open.
- [ ] Measure import wall time, peak RSS, local spool bytes, ClickHouse parts,
  and retry cleanup on t283 and scratch125 subsets.
- [x] Reject post-import `INSERT ... SELECT` rebuilds unless they are bounded,
  use cleanup deadlines, and cannot leave partial serving data visible.
  Result: accepted as a design constraint for all new tables and sidecars.
- [x] Preserve deterministic snapshot IDs and active snapshot atomicity.
  Result: carried forward as a hard requirement.

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
- [x] Compare large `IN` lists, external temporary tables, `Join` engine
  dictionaries, and array binds for any remaining batch query.
  Result: a large `parent_dir IN (...)` probe for 11,205 visible child parents
  pruned to 4/17 granules and ran 17-20 ms scalar or 32-58 ms vector; matching
  `wrstat_children` batched read ran 16-22 ms.
- [ ] Test whether query cache settings help cold provider paths only after
  the underlying read amplification is removed.
- [x] Reject tuning-only changes if high-fanout focused broad/filtered probes
  still read millions or billions of rows.
  Result: tuning-only is rejected. The baseline reads 66.6M-206.6M rows broad
  and 2.90B rows filtered in the focused proof cases.

### 11. Tactical Cache Warming And Response Caching

Question: what is safe as a stopgap while schema3 is built?

- [ ] Prototype warming root, `/lustre/`, `/nfs/`, mount roots, and high-fanout
  parent packets after provider open or active-set publish.
- [x] Cache whole directory packets, not just the one requested child, when a
  parent range is read.
  Result: accepted as a tactical and schema3 serving-contract requirement.
- [x] Include filter and age in the cache key. Do not let AgeAll cache entries
  answer age-specific queries.
  Result: accepted as required for both packet caches and all-filter rows.
- [ ] Measure provider-open cost, active-update cost, memory growth, and
  invalidation on snapshot publish/tombstone.
- [x] Treat this as tactical only. If cold filtered focused `DirInfos` can hit
  459s without cache, the final answer cannot be "warm it first".
  Result: cache warming is only a stopgap; schema3 must make the cold route
  bounded.

### 12. Perf Harness And Gates

Question: what must be measured before schema3 is accepted?

- [x] Add or preserve operations for high-fanout focused `DirInfos` and
  `DirsHaveChildren` with broad and filtered inputs. These are now required
  gates, not optional diagnostics.
  Result: broad focused operations exist and reproduced 63s. The filtered
  operation timed out at 459s without a saved JSON record, so timeout flushing
  remains a harness improvement.
- [ ] Add a real first `./wrstat-ui where --dir` measurement that does not
  run after the REST response cache is already warm.
- [ ] Add browser/React timing for first Disktree click into a high-fanout
  folder and for switching filters.
- [ ] Add result digest/equivalence checks for every optional serving table and
  cache. Fast stale or partial answers are failures.
- [ ] Add per-operation query count, cache hit/miss count, ClickHouse read
  rows/bytes/marks, JSON/gzip bytes, and result counts to every report.
- [x] Define hard gates on the mixed subset, a larger NFS-heavy subset, a
  100-small-NFS simulation, and the high-fanout scratch125 parent.
  Result: starting gates below are retained and expanded by the final
  recommendation.

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
| Current schema2 branch | Existing facts, parent facts, AgeAll, active-prefix rollups, caches | root first 65 ms REST; high-fanout endpoint p50 199 ms | high-fanout endpoint p50 848 ms for `types=other` | broad p50 63s/p95 117s; filtered 459s timeout | broad p50 63s | root REST first 120 ms on small subset; in-process root p50 1.4s; real CLI missing | open | import 35.9s/831 MB for 1.75M rows | facts 30 MiB, parent facts 28.5 MiB, AgeAll 11.35 MiB | repeated parent reads and caches hiding cold work | Baseline only |
| 1. Exact-dir plus parent-packet routing | Reuse `wrstat_dir_facts`, `wrstat_parent_facts`, `wrstat_children`; add request/provider packet cache and multi-parent packet reads | exact dir 6-11 ms; high-parent packet 8-12 ms scalar, 18-28 ms vector | broad vectors available, but arbitrary filters need all-filter layer | expected bounded if implemented; direct batched child-parent probe 17-58 ms instead of 11k rereads | expected bounded from same packets; must gate end-to-end | helps `where` traversal by batching frontiers; not enough for age filters alone | needs virtual overlay gate | low table cost; implementation routing/cache work | no new physical table for broad packets | any missed public path can recreate 63s class | First schema3 step |
| 2. All-filter child/dir serving rows | Add `wrstat_child_filter_all` keyed by parent and `wrstat_dir_filter_all` or projection keyed for subtree `where`; keep AgeAll path until replaced by faster projection | broad/default should stay on parent packets, not filter rows | target parent p50 27-44 ms for age/gid/uid/ft variants; AgeAll+gid p50 44 ms | closes filtered focused case when paired with packet routing; end-to-end gate still required | use same parent-keyed rows for child presence under filters | subtree `where` age+gid+uid+ft p50 47 ms; AgeAll+gid p50 77 ms, slower than current AgeAll | virtual filter rows handled by overlay | import CPU/spool not measured | 3.49M child rows 30.11 MiB plus 3.49M dir rows 75.26 MiB; 105.37 MiB combined | row amplification and exact summary equivalence | Required comprehensive filter layer |
| 3. Small active-set virtual overlay | Add active-set virtual rows for `/`, `/lustre/`, `/nfs/`, intermediate virtual parents, and virtual filter summaries; avoid full physical duplication by default | unifies roots/mount boxes with ordinary packet serving | must include full-filter virtual summaries | not a high-fanout fix by itself | not a high-fanout fix by itself | makes root and virtual `where` first reads route like normal rows | not simulated; modeled virtual full-filter overhead is small | active publish/incremental rebuild still open | modeled 6,134 virtual full-filter rows; physical full copy was 29.13 MiB per active set | active-set pointer and invalidation | Third layer, small overlay only |
| Directory packet table | Add nested `wrstat_dir_packets` style table | target nested packet p50 21 ms | still needs filter layer | did not beat parent facts | did not beat parent facts | no subtree/filter advantage | not covered | more complex grouped import/update | 50,430 rows, 33.15 MiB compressed, 808.23 MiB uncompressed | wide nested rows and alignment/paging | Rejected as primary |
| Standalone where frontier facts | Add frontier table(s) keyed by dir/filter/split/query version | indirect | only if every filter family is precomputed | does not solve measured `DirInfos` failure | does not solve measured `DirsHaveChildren` failure | can bound `where`, but exact tuple/multi-select coverage is large | open | likely high and version-sensitive | at least 493k AgeAll and 3.49M age-specific memberships before frontier payloads | split semantics and combinatorial filters | Rejected/deferred |
| Bitmap/posting-list index | Use postings/bitmaps with DFS intervals and prefix payloads, preferably in a sidecar | weak alone for broad/dense filters | promising for exact age-specific filters | can prevent rereads if paired with packets/payloads | can test child intervals | promising for interval `where` sums | needs active manifest | build modeled at 10-30s extra if integrated | raw lists: 10.4 MB dimensions, 2.0 MB AgeAll tuples, 14.0 MB age tuples before payloads | postings alone lack UI payloads | Sidecar primitive, not primary CH design |
| Immutable navigation sidecar | Add versioned Bolt/SQLite/mmap/Roaring sidecar with active-set manifest and atomic publish | prior Bolt p95 2.5 ms root on earlier subset | prior Bolt filtered direct p95 0.45 ms on earlier subset | mixed8 high-fanout not measured | mixed8 high-fanout not measured | prior Bolt `where` p95 40-79 ms on earlier subset | active-set aggregate design needed | estimated 10-30s extra integrated build; no mixed8 run | Bolt-like mixed8 estimate 0.4-0.5 GB; mmap/Roaring target 100-300 MB | dual-store publish, validation, recovery | Fallback if native gates fail |
| Cache warming only | No schema; warm packets/responses | helps repeats | only if warmed | hides but does not fix | hides but does not fix | hides but does not fix | open | provider-update cost open | memory growth open | invalidation/staleness | Stopgap only |

## Final Recommendation

Schema3 should be a ClickHouse-native hybrid, implemented in this order:

1. **Exact-dir plus parent packet routing first.** Keep `wrstat_dir_facts` as
   the canonical exact directory fact table and `wrstat_parent_facts` as the
   canonical broad/default child packet table. Change the serving contract so
   public `DirInfo`, focused `DirInfos`, `DirsHaveChildren`, Disktree endpoint
   navigation, and `Where` frontiers read each parent packet once per request
   or provider cache, then reuse the whole packet. Do not use a parent packet
   row as the primary exact current summary. Evidence: exact target dir reads
   were 6-11 ms; one high-parent packet was 8-12 ms scalar or 18-28 ms vector;
   the 63-117s failure came from about 11k repeated focused calls reading
   66.6M-206.6M rows.
2. **Add comprehensive all-filter child and dir serving rows.** Add a
   parent-keyed `wrstat_child_filter_all` table for filtered child summaries
   and child presence, and add `wrstat_dir_filter_all` or an equivalent
   projection keyed for `where --dir` subtree scans. Include AgeAll and every
   age-specific `(gid, uid, ft, age)` row needed by current filters. Retain
   `wrstat_dir_filter_ageall` until an AgeAll projection proves it can replace
   it; the all-filter subtree row was slower for the AgeAll+gid case here.
3. **Use a small active-set virtual overlay.** Add active-set keyed rows only
   for virtual ancestors and virtual filter summaries: `/`, `/lustre/`,
   `/nfs/`, intermediate virtual parents, and mount-root boxes. Avoid copying
   all physical facts per active set unless the 100-small-NFS and publish-cost
   gates prove that duplication is needed. The mixed8 virtual full-filter model
   was only 6,134 rows; the physical active-set copy would duplicate about
   29.13 MiB per active set.

No existing schema2 object should be removed in the first schema3 step.
`wrstat_dir_facts`, `wrstat_parent_facts`, and `wrstat_children` remain
canonical. `wrstat_active_prefix_rollups`, `wrstat_active_prefix_filter_ageall`,
and `wrstat_virtual_children` become compatibility or diagnostic objects once
the active-set virtual overlay can serve the same totals.
`wrstat_dir_filter_ageall` remains a canonical fast AgeAll path until the
comprehensive filter layer has a measured replacement.

Rejected/deferred alternatives:

- **Directory packet table as primary storage:** rejected. The nested packet
  prototype was p50 21-22 ms on the target parent, slower than the existing
  `wrstat_parent_facts` packet, and still needs another filter/subtree layer.
- **Standalone `where_frontiers`:** rejected/deferred. It is specialized to
  `where`, does not solve the measured 63s `DirInfos`/`DirsHaveChildren`
  failures, and full filter coverage creates exact tuple and selected-set
  versioning complexity.
- **ClickHouse bitmap/posting tables as the primary answer:** rejected for now.
  Postings are promising, but by themselves they identify candidate dirs rather
  than UI summary payloads. Use postings/bitmaps as a sidecar primitive.
- **Cache warming only:** stopgap only. Response caches hid root REST costs,
  but the cold filtered focused case still reached 459s and 898.6 GB read.

Writer/spool changes must build every schema3 object before publish:
direct-writer rows and production spool manifests need counts/checksums for
exact facts, parent packets, all-filter child rows, all-filter dir rows, and
active virtual rows. Readiness rows must not expose a partial serving layer.
Active publish must switch all objects atomically by snapshot/active-set id;
cleanup must remove old partitions or sidecar versions only after readers drain.
Packet caches must be invalidated by active-set/snapshot/version and keyed by
filter mode and age.

Implementation is not complete until these gates pass on mixed8, a larger
NFS-heavy subset, the 100-small-NFS simulation, and the scratch125 high-fanout
parent:

- high-fanout focused `DirInfos` broad and filtered: p95 under 1s with bounded
  read rows/bytes/marks;
- high-fanout focused `DirsHaveChildren` broad and filtered: p95 under 1s;
- first high-fanout Disktree click broad and filtered: p95 under 500 ms;
- real first `./wrstat-ui where --dir` for root, high-fanout, and NFS-heavy
  dirs: p95 under 1-2s, with no warmed REST response cache;
- result digests for exact summaries, child summaries, `has_children`, filter
  visibility, timestamps, UID/GID/type/age summaries, virtual boxes, and
  `where` result sets;
- import/summarise wall time, peak RSS, spool bytes, part counts, retry
  cleanup, and publish latency must not regress beyond an agreed budget.

If the ClickHouse-native packet plus all-filter plus virtual-overlay design
misses those cold gates, stop adding process-local caches and use an immutable
active-set navigation sidecar. The preferred fallback is a purpose-built
mmap/Roaring sidecar with exact directory packets, parent-child adjacency,
filter postings/prefix payloads, virtual rows, manifest checksums, atomic
publish, and ClickHouse retained as the source of truth for files, history,
basedirs, and audit. SQLite is acceptable as a prototype/audit format; Bolt is
the fastest reuse path but still needs an active-set aggregate redesign.
