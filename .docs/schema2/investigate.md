# Prompt for ClickHouse cold tree performance investigation

Use this before writing a follow-up schema/query spec in `.docs/schema2`.

The goal is to determine what deeper storage, query, cache, or hybrid design is
needed now that the clean ClickHouse schema work in `.docs/schema` is mostly
implemented but the web UI is still cold-slow compared with Bolt. The known
symptom pattern is important:

- After a page refresh, the root Disktree view `/` takes multiple seconds.
- First clicks into virtual mount-parent directories such as `/lustre` and
  `/nfs` take about 1-2 seconds.
- Repeating the same browser-session clicks is fast.
- Switching to a different tree filter makes the slow path return.
- After a mount directory has been visited, repeated clicks into it and first
  clicks into its subdirectories are fast.
- `./wrstat-ui where` is acceptable only after cache warming, roughly 4s, but
  the first run is still too slow, roughly 18s.

Treat this as a cold-path/schema investigation, not as another pass over the
old raw-DGUTA schema. The current branch already has `wrstat_dir_facts`,
`wrstat_children`, append-only `wrstat_mount_events`, `wrstat_virtual_children`,
process-local tree query caches, and optional AgeAll filter-index plumbing.
The next design must make the first uncached interaction fast enough, not just
make the second one fast.

## Ground Rules

- Use writable subagents for bounded design experiments, following
  `/home/ubuntu/.agents/skills/subagents/SKILL.md`.
- Give each subagent the relevant skill paths, the files to inspect, the exact
  hypotheses to test, and timeout requirements for long commands.
- Temporary code, DDL, and instrumentation are allowed for experiments.
- Keep final code changes out of the branch unless the user explicitly asks to
  keep a prototype.
- Use `.tmp/agent/schema2/` for temporary scripts, binaries, worktrees, perf
  JSON, SQL, screenshots, trace logs, and scratch notes.
- Do not write a separate `investigation-results.md` by default. Record
  commands, datasets, row counts, timings, query counts, read rows/bytes, and
  conclusions directly in the relevant sections of this file as the work
  proceeds.
- Mark checklist items as done while working. Do not leave all checkboxes blank
  until the end. When an item is tested, change `[ ]` to `[x]` and add a short
  `Result:` note under the item or section. If an item is deliberately skipped,
  mark it `[x]` and explain why.
- The production-like datasets are under `/home/ubuntu/output/lustre` and
  `/home/ubuntu/output/nfs`.
- Do not summarise the complete example mounts. Full summarise runs can take
  hours. Use sizable subsets: bounded `--maxLines`, selected dataset symlink
  trees under `.tmp/agent/schema2/`, or a few representative mounts imported
  into the same test database.
- A root `/` test over a subset is still expected to aggregate all selected
  Lustre and NFS mounts, and the Disktree result must expose separate `/lustre`
  and `/nfs` boxes with their respective totals. A fast result that samples one
  mount or hides virtual parent directories is wrong.
- If a `--mounts` file is used, list the selected actual mount roots. Do not
  list only `/lustre` or `/nfs` if doing so changes them from virtual ancestors
  into configured mount scopes.
- Use a unique ClickHouse database per experiment so table shape, cache state,
  and optional indexes do not contaminate other runs.
- Reset process-local query caches between cold measurements. Record whether a
  timing is cold browser, cold provider, provider-update cold cache, warm
  provider, repeated same directory, or repeated same filter.

Useful command shapes:

```bash
timeout 600s env CGO_ENABLED=1 go test -tags netgo --count 1 ./clickhouse ./cmd ./internal/chperf
timeout 300s go build -o .tmp/agent/schema2/wrstat-ui .

timeout 1h .tmp/agent/schema2/wrstat-ui clickhouse-perf \
  -C "$CLICKHOUSE_DSN" -D wrstat_schema2_<name> \
  --mounts .tmp/agent/schema2/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/schema2/perf/<name>-import.json \
  import .tmp/agent/schema2/subset --maxLines 200000 --batchSize 100000 --parallelism 1

timeout 10m .tmp/agent/schema2/wrstat-ui clickhouse-perf \
  -C "$CLICKHOUSE_DSN" -D wrstat_schema2_<name> \
  --mounts .tmp/agent/schema2/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/schema2/perf/<name>-query-root.json \
  query --dir / --ancestor-dir / --ancestor-limit 8 --repeat 10 --warmup 0 \
  --ops tree_disktree_endpoint_cold_provider,tree_disktree_endpoint_provider_update_cold_cache,tree_disktree_endpoint_ancestor_dirs,tree_where_cold_provider,tree_where_cold_then_cached,virtual_dirinfo

timeout 10m .tmp/agent/schema2/wrstat-ui clickhouse-perf \
  -C "$CLICKHOUSE_DSN" -D wrstat_schema2_<name> \
  --mounts .tmp/agent/schema2/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/schema2/perf/<name>-query-filtered.json \
  query --dir / --ancestor-dir / --ancestor-limit 8 --repeat 10 --warmup 0 \
  --tree-gids <gid> --tree-uids <uid> --tree-types other \
  --ops tree_disktree_endpoint_cold_provider,tree_disktree_endpoint_provider_update_cold_cache,tree_where_cold_provider,where_filtered_whole_mount,dirinfo_filtered,dirinfos_filtered,dirshavechildren_filtered,virtual_dirinfo
```

Adjust `--maxLines`, repeat counts, selected mounts, and operations upward only
when a candidate looks promising and needs stronger proof.

## Baseline Setup Checklist

- [x] Read `.docs/schema/investigate.md`,
  `.docs/schema/investigation-results.md`, `.docs/schema/prompt.md`, and the
  recent `.docs/bugfixes` entries for Disktree, `where`, projection memory,
  and cold-provider perf. Record the already-tested ideas that must not be
  repeated without new evidence.
  Result: Prior-art/context review captured in
  `.tmp/agent/schema2/baseline-context.md`. Already-tested ideas not to repeat
  without new evidence: raw-DGUTA/mixed fallback schema, file-facts-only tree
  summaries, general directory-id rewrites, full re-expanded filter indexes by
  default, aggregate-state primary facts, post-import projection rebuilds,
  fingerprinted active-tree hot reads with `FINAL`, cache warming as the only
  final answer, ordinary full-subtree `Where` scans, and long-lived or
  normal-timeout ClickHouse import batches.
- [x] Identify the current implemented ClickHouse schema and query routing.
  Confirm whether optional objects such as `wrstat_dir_filter_ageall` and
  virtual summary cache tables are present in base DDL, only in tests, or not
  present at all.
  Result: See `.tmp/agent/schema2/schema-routing.md`. Base DDL creates
  `wrstat_mount_events`, `wrstat_mounts_active`, `wrstat_dir_facts`,
  `wrstat_children`, `wrstat_dir_projection_sets`, `wrstat_virtual_children`,
  and `wrstat_virtual_children_sets`. `wrstat_dir_filter_ageall`,
  `wrstat_virtual_summary_cache`, and `wrstat_virtual_summary_sets` are absent
  from base DDL; production hooks tolerate absence. Disktree routes through
  `/rest/v1/auth/tree` -> `Tree.DirInfo` -> ClickHouse
  `DirInfo`/`Children`/`DirInfos` plus `DirsHaveChildren`; `where` routes
  through `/rest/v1/auth/where` -> `Tree.Where` -> ClickHouse traversal.
- [x] Select a mixed Lustre+NFS subset from `/home/ubuntu/output/lustre` and
  `/home/ubuntu/output/nfs`. Include at least one directory-heavy NFS dataset
  such as `t283_imaging` when feasible, and representative Lustre datasets such
  as `scratch120`, `scratch122`, or `scratch127`.
  Result: Bounded pass on 2026-06-07 used subset DB
  `wrstat_schema2_baseline_084721` and probe DB
  `wrstat_schema2_probe_084918`. Subset dir:
  `.tmp/agent/schema2/subset-084721-statslinks`; mounts file:
  `.tmp/agent/schema2/mounts-084721.txt`; `maxLines=100000` per mount.
- [x] Record selected dataset paths, mount paths, `stats.gz` sizes, `maxLines`,
  imported line counts, and why the subset is large enough to exercise `/`,
  `/lustre`, `/nfs`, filter changes, and mount-root clicks.
  Result:
  | Dataset | Mount | Source `stats.gz` | Compressed bytes | Imported lines | Dir facts | Children |
  |---|---|---|---:|---:|---:|---:|
  | `t283_imaging` | `/nfs/t283_imaging/` | `/home/ubuntu/output/nfs/20260517-170004_／nfs／t283_imaging/stats.gz` | 6,095,109,852 | 100,000 | 70,523 | 35,005 |
  | `scratch120` | `/lustre/scratch120/` | `/home/ubuntu/output/lustre/20260517-200015_／lustre／scratch120/stats.gz` | 1,420,306,103 | 100,000 | 1,508 | 546 |
  | `scratch122` | `/lustre/scratch122/` | `/home/ubuntu/output/lustre/20260517-200015_／lustre／scratch122/stats.gz` | 2,780,894,204 | 100,000 | 32,897 | 9,608 |
  | `scratch127` | `/lustre/scratch127/` | `/home/ubuntu/output/lustre/20260517-200015_／lustre／scratch127/stats.gz` | 5,317,090,708 | 100,000 | 2,490 | 1,144 |
  The set includes one directory-heavy NFS mount plus three Lustre mounts, so
  `/`, `/lustre/`, `/nfs/`, filter changes, and mount-root clicks are meaningful
  within the bounded subset.
- [x] Build a local binary in `.tmp/agent/schema2/wrstat-ui`.
  Result: `timeout 300s env CGO_ENABLED=1 go build -o .tmp/agent/schema2/wrstat-ui .`
  succeeded in about 8.5s.
- [x] Import the subset into a fresh ClickHouse database and capture table row
  counts, active parts, compressed bytes, uncompressed bytes, vector entry
  stats, and import phase timings.
  Result: `timeout 1h .tmp/agent/schema2/wrstat-ui clickhouse-perf ... import`
  succeeded against `wrstat_schema2_baseline_084721` in 5.56s wall, 400,000
  input records, max RSS 407,392,256 bytes. Import phase durations:
  `/nfs/t283_imaging/` 2,992.834 ms, `/lustre/scratch120/` 485.650 ms,
  `/lustre/scratch122/` 1,420.105 ms, `/lustre/scratch127/` 574.117 ms.
  Active table storage: `wrstat_files` 400,000 rows/7.09 MB compressed,
  `wrstat_dir_facts` 46,307 rows/7.28 MB compressed/216.53 MB uncompressed,
  `wrstat_children` 46,303 rows/0.44 MB compressed. Facts vectors:
  915,933 entries, average 19.78 per dir, max 1,030.
- [x] Verify root semantics on the imported subset: `/` total equals the sum of
  selected active Lustre and NFS mounts, and `/lustre` plus `/nfs` children have
  their respective summary data.
  Result: Scratch Go tree probe validated `/` equals the sum of all selected
  active mount roots. Counts/sizes: `/` 3,281,812 and 76,576,914,604,824 bytes;
  `/lustre/` 2,186,003 and 74,219,418,845,184 bytes; `/nfs/` 1,095,809 and
  2,357,495,759,640 bytes. Root API exposed separate `/lustre` and `/nfs`
  children with matching totals.
- [x] Measure current branch cold and warm query baselines with
  `clickhouse-perf`, including root `/`, virtual ancestors `/lustre` and `/nfs`,
  selected mount roots, filter changes, cold provider, provider-update cold
  cache, same-provider warm, and visible-child repeated paths.
  Result: `timeout 10m .tmp/agent/schema2/wrstat-ui clickhouse-perf ... query`
  measured in-process tree paths with `--repeat 5 --warmup 0` in baseline and
  `--repeat 3 --warmup 0` in candidate probes. Baseline subset did not
  reproduce multi-second root Disktree, but did reproduce slow cold filtered
  `where`: `/` broad cold-provider Disktree p50/p95 89.9/93.2 ms, warm p50/p95
  56/58 ms; `/lustre/` cold-provider 68.7/74.8 ms, warm 20/20 ms; `/nfs/`
  cold-provider 50.4/51.1 ms, warm 13/26 ms; `/nfs/t283_imaging/` cold-provider
  35.3/35.5 ms, warm 0/0 ms. Filtered `/` cold-provider Disktree p50/p95
  92.2/95.9 ms, but filtered `tree_where_cold_provider` was 854.1/866.0 ms and
  `/nfs/t283_imaging` filtered `tree_where_cold_provider` reached p50/p95
  1160.6/1554.4 ms in candidate probes.
- [ ] Measure the actual web/API path if possible, not only in-process perf:
  page refresh to `/`, first click `/lustre`, first click `/nfs`, first click
  into a selected mount, repeat click, and switch to a different filter.
  Open: Real browser/HTTP path was not measured in this bounded pass. Current
  `clickhouse-perf` Disktree ops are in-process and do not include browser cache
  clear, HTTP, gzip, auth, JSON decode, or React render time.
- [x] Measure `./wrstat-ui where` first run and cached run against the same
  server/database/filter, and separately measure in-process `tree_where` so CLI,
  HTTP, JSON, and database costs are not conflated.
  Result: Deliberately skipped real CLI timing because the standalone `server`
  command requires `--cert`, `--key`, `--owners`, and Okta issuer/client/secret
  configuration, which this environment did not provide. In-process
  `tree_where` was measured instead: `/` broad cold-provider p50/p95
  296.960/361.848 ms; `/` cold-then-cached first sample 376.764 ms then about
  64-70 ms; filtered `/` first sample 881.146 ms then about 79-84 ms.
- [x] Add temporary query tracing if needed: per request query count, query
  names, cache hit/miss counts, ClickHouse elapsed time, read rows, read bytes,
  and returned rows/bytes.
  Result: Chperf and inspector artifacts captured elapsed/read-row/read-byte
  evidence for representative operations. Examples: root endpoint typical
  17,239 read rows/9.82 MB; `/lustre/` endpoint 4,319 rows/4.46 MB; `/nfs/`
  endpoint 4,631 rows/608 KB; filtered `dirshavechildren` 47,159 rows/25.19 MB;
  t283 filtered whole-mount `where` 35,222-140,219 rows/149-164 MB. Direct
  `system.query_log` capture failed because this local ClickHouse exposed no
  `system.query_log`; query counts and cache hit/miss counts remain open.
- [ ] If Bolt comparison data is available or can be produced on the same
  subset, record Bolt root Disktree and `where` timings as the user-facing
  target.
  Open: No same-subset Bolt baseline was located or produced during this
  bounded pass.

## Investigation Checklist

### 1. Trace The True Cold Path

Question: where are the seconds going on first page refresh and first filter
change?

- [ ] Add or use instrumentation that separates browser/render time, HTTP
  handler time, provider/database time, ClickHouse query time, JSON size, gzip
  size, and React Disktree render time.
  Open: Not measured for real HTTP/browser. Existing chperf is in-process.
- [x] For `/`, `/lustre`, `/nfs`, and one mount root, record the exact backend
  calls made by the Disktree endpoint: `DirInfo`, `DirInfos`, `Children`,
  `DirsHaveChildren`, virtual summary/cache checks, active mount resolution, and
  readiness checks.
  Result: Code/routing report in `.tmp/agent/schema2/schema-routing.md` traces
  `/rest/v1/auth/tree` to `Tree.DirInfo`, then ClickHouse
  `DirInfo`/`Children`/`DirInfos`, with server `DirsHaveChildren` for visible
  children. Virtual ancestors try optional summary cache, then live active-root
  aggregation; children use `wrstat_virtual_children` when ready.
- [ ] Record cache hits and misses for children, GUTAs, scalar summaries,
  readiness, and active metadata on first request, repeated request, and after
  filter changes.
  Open: Process-local cache buckets were identified, but hit/miss counts were
  not captured.
- [x] Use ClickHouse profile events or `system.query_log` to record read rows,
  read bytes, result rows, elapsed time, and memory for each hot SQL shape.
  Result: Chperf/inspector captured read rows/bytes for hot shapes. Direct
  query-log validation was blocked by missing `system.query_log` on local
  ClickHouse; inspector fallback/chperf metrics were used instead.
- [x] Determine whether the root delay is mostly virtual ancestor composition,
  child summary/has-children fanout, active mount/readiness lookups, vector
  filtering, response size, or frontend rendering.
  Result: On this subset, in-process root Disktree was under 102 ms p95 and did
  not reproduce the multi-second browser symptom. The largest measured cold
  signal was filtered `where`/filter vector scanning on t283, with
  `tree_where_cold_provider` p50 1160.6 ms and whole-mount filtered scan p50
  726 ms reading 149-164 MB.
- [x] Add a `Result:` subsection here with a table of cold root, cold
  `/lustre`, cold `/nfs`, cold mount-root, filter-change, and repeated timings.

Result:

| Case | Timing | Read rows/bytes | Conclusion |
|---|---:|---:|---|
| `/` cold-provider Disktree | p50/p95 97.8/101.1 ms | typical root endpoint 17,239 rows/9.82 MB | Not the reproduced bottleneck in-process. |
| `/lustre` cold-provider Disktree | p50/p95 70.7/74.2 ms | 4,319 rows/4.46 MB | Virtual namespace click is measurable but not seconds on subset. |
| `/nfs` cold-provider Disktree | p50/p95 53.9/54.4 ms | 4,631 rows/608 KB | Warm p50 fell to 14 ms. |
| `/nfs/t283_imaging` cold-provider Disktree | p50/p95 94.2/115.9 ms filtered | not captured per request | Endpoint itself is less costly than filtered `where`. |
| `/nfs/t283_imaging` filtered `where` | p50/p95 1160.6/1554.4 ms | whole-mount op 149-164 MB | Strongest cold filter/where signal. |
| Repeated/cached `where` | first 376.8-881.1 ms, repeats about 64-84 ms | not counted | Process caches hide cold cost. |

### 2. First-Class Virtual Ancestor Summary Cache

Question: should the optional virtual summary cache become a real schema table
again, or be replaced by a better ancestor rollup?

- [x] Confirm the current base DDL status of `wrstat_virtual_summary_cache` and
  `wrstat_virtual_summary_sets`; current code has hooks, but base schema may
  intentionally omit the tables.
  Result: Base DDL intentionally omits both tables, and bootstrap tests assert
  absence. Reader/writer hooks exist and tolerate unknown-table absence.
- [x] Prototype a persisted ancestor summary cache keyed by active set id and
  virtual ancestor dir, covering `/`, `/lustre/`, `/nfs/`, and any other parent
  directories above active mount roots.
  Result: Deliberately skipped in this bounded pass. The probe used the smaller
  active-prefix rollup instead because it directly covers `/`, `/lustre/`, and
  `/nfs/` scalar summaries and ranked above restoring the broader cache.
- [ ] Store enough data to answer broad and filtered `DirInfo` for virtual
  ancestors without scanning all active mount-root vectors on first request:
  scalar all/default-file summaries, aligned filter vectors, `child_count`,
  `updated_at`, and readiness.
  Open: If revisited, the cache must store vectors or filter rows; scalar-only
  summaries would not fix cold filter switches.
- [ ] Ensure root `/` and namespace summaries are composed from all selected
  active mount roots and preserve exact UID/GID/file-type/age filter semantics.
  Open: Not measured for the virtual cache; root semantics were validated only
  for current live composition and scalar active-prefix rollup.
- [ ] Measure refresh cost when the active set changes, startup/provider-open
  cost, storage bytes, active-set cleanup, and first request latency.
  Open: No cache refresh/import cost measured.
- [x] Test whether a cache of only virtual ancestors is enough, or whether
  mount-root child summaries still dominate first click into `/lustre` and
  `/nfs`.
  Result: Existing evidence says a broad virtual cache would not directly solve
  t283 filtered whole-mount `where` or ordinary mount child facts. Defer until
  after active-root tuple tuning, prefix rollups, and filter rows.
- [x] Reject the cache if it only helps broad root but not filter changes or
  click-through paths, unless paired with another design that covers those.
  Result: Rejected as the first schema move. It may become a companion only if
  measured root virtual composition remains dominant after smaller rollups.

### 3. One-Query Disktree Navigation Facts

Question: can the Disktree endpoint be answered by one ordered read instead of
the current parent summary, children, child summaries, and has-children sequence?

- [x] Prototype a denormalized navigation table, for example
  `wrstat_tree_nav_facts` or `wrstat_child_facts`, keyed by active set id or
  `(mount_path, snapshot_id, parent_dir, child_dir)`.
  Result: Probe DB `wrstat_schema2_probe_084918` built
  `wrstat_child_facts(mount_path, snapshot_id, parent_dir, child)` from
  `wrstat_children` joined to `wrstat_dir_facts`: 46,303 rows, 1.74 MB
  compressed, 38.94 MB uncompressed.
- [ ] Include each visible child row's display path, child summary scalar
  columns, child filter vectors or a pointer to facts, `child_count`, and
  enough metadata to answer `has_children` without a second query.
  Result: Prototype carried child summary facts and `child_count` from facts.
  Open: Filter vectors, a pointer-to-facts strategy, or a narrow filter
  companion table still need selection.
- [ ] Cover virtual parents (`/`, `/lustre`, `/nfs`) and ordinary mount
  directories. Root `/` must still return separate `/lustre` and `/nfs` boxes.
  Open: Prototype covered ordinary parent edges only; virtual rows still need
  active-prefix rollups or explicit nav rows.
- [ ] Measure broad Disktree first request and repeated request for `/`,
  `/lustre`, `/nfs`, selected mount roots, and visible children.
  Open: Benchmarked SQL shape for one high-fanout parent, not full endpoint
  first/repeated path.
- [ ] Measure filtered Disktree after changing UID/GID/file-type filters. If
  vectors in the navigation table are too wide, test a narrow filter existence
  companion table.
  Open: Filtered navigation was not measured.
- [x] Compare import row amplification and compressed bytes against the current
  `wrstat_children` plus `wrstat_dir_facts` shape.
  Result: Storage was measured, but importer cost was not. Current baseline:
  `wrstat_children` 46,303 rows/0.44 MB compressed plus `wrstat_dir_facts`
  46,307 rows/7.28 MB. Prototype child facts added 46,303 rows/1.74 MB.
- [x] Decide whether this table replaces `wrstat_children`, supplements it only
  for UI paths, or is too expensive.
  Result: Promising for UI navigation after rollups/filter rows. For a 305-child
  high-fanout parent, current `children` plus `dir IN` facts p50 was 6 ms;
  `wrstat_child_facts` used one parent primary-key range and p50 was 4 ms.
  Treat as a candidate supplement/replacement for Disktree paths, not yet a
  selected final table.

### 4. Parent-Ordered Directory Facts

Question: is the current `(mount_path, snapshot_id, dir)` order forcing
expensive `dir IN (...)` lookups for child summaries?

- [x] Prototype adding `parent_dir`, `name`, `depth`, and possibly
  `has_children` to `wrstat_dir_facts`, plus a ClickHouse projection or
  duplicate table ordered by `(mount_path, snapshot_id, parent_dir, name)`.
  Result: Probe DB built duplicate `wrstat_parent_facts` ordered by
  `(mount_path, snapshot_id, parent_dir, name)`: 46,307 rows, 1.84 MB
  compressed, 39.70 MB uncompressed.
- [x] Test whether one parent-range query can replace `wrstat_children` plus
  `DirInfos(children)` for ordinary mount-directory clicks.
  Result: For the 305-child `/lustre/scratch127/.../HE/` parent, parent facts
  answered via one parent-prefix range with p50 5 ms versus 6 ms for the current
  two-shape lookup.
- [x] Verify ClickHouse actually uses the projection/order with
  `EXPLAIN indexes=1`; do not assume projections are selected.
  Result: `EXPLAIN indexes=1` showed a binary-search parent range and 1/6
  granules for the duplicate table. Projection optimizer behavior was not
  proven.
- [ ] Compare a ClickHouse projection, a second physical table, and folding
  child edges into the canonical facts table.
  Open: Only second physical tables were probed. Projection and canonical-table
  folding remain open.
- [ ] Measure first click into high-fanout directories, low-fanout directories,
  and deep single-child chains.
  Open: High-fanout parent measured; low-fanout and deep-chain cases remain
  open.
- [ ] Record import cost, storage cost, and cleanup complexity.
  Open: Storage measured; importer/write and cleanup cost not measured.

### 5. Active-Set Mount-Root Rollups

Question: can root and namespace reads avoid touching one partition per active
mount?

- [x] Prototype an active-set table containing only active mount-root facts in
  one active-set partition, for example `wrstat_active_mount_root_facts`.
  Result: Probe DB built `wrstat_active_prefix_rollups(active_set_id, dir)` with
  scalar rows for `/`, `/lustre/`, and `/nfs/`: 3 rows, 251 compressed bytes.
- [x] Test whether `/`, `/lustre`, and `/nfs` summaries can be answered from
  this small active-set table instead of reading mount-root rows from many
  `(mount_path, snapshot_id)` partitions.
  Result: Rollup matched current scalar totals:
  `all_count=400000`, `file_count=353197`, `child_count=9`. EXPLAIN read 1/1
  granules and benchmark p50/p95 was 2/4 ms.
- [x] Add per-namespace rollup rows if direct mount-root aggregation is still
  too slow, for example `wrstat_active_prefix_rollups(active_set_id, dir, ...)`.
  Result: `/`, `/lustre/`, and `/nfs/` prefix rows were included.
- [ ] Include vectors or re-expanded filter rows so filter changes do not
  rescan all mount-root arrays.
  Open: Prototype was scalar only; filter-aware rollup rows or vectors remain
  open.
- [ ] Measure active-set publish/update cost when one mount changes and the
  active-set id changes.
  Open: Probe table was derived post-import; publish/update cost was not
  measured.
- [x] Compare this design with the virtual ancestor summary cache. Decide
  whether they are the same concept, complementary layers, or competing
  designs.
  Result: Current active-root SQL shaped as `(mount_path, snapshot_id) IN (...)
  AND dir = mount_path` read 20/20 granules. A tuned tuple query
  `(mount_path, snapshot_id, dir) IN ((mount, snapshot, mount), ...)` read 4/20
  granules with p50/p95 5/6 ms. Active-prefix rollups rank above the broader
  virtual summary cache because they are smaller and directly measured.

### 6. Filter Indexes For Cold Filter Changes

Question: which minimal filter-oriented structures make first use of a new
filter fast?

- [x] Verify whether `wrstat_dir_filter_ageall` exists in the tested database,
  whether it is populated during import, and whether query routing uses it for
  the filters that are slow in the UI.
  Result: Baseline DB did not contain `wrstat_dir_filter_ageall`; it is absent
  from base DDL. Code can route AgeAll filters to it only if it exists and has
  active parts.
- [x] Measure facts-vector filtering versus `wrstat_dir_filter_ageall` for
  common and selective UID/GID/type filters on root, `/lustre`, `/nfs`, and
  whole-mount `where`.
  Result: Probe DB built `wrstat_dir_filter_ageall` from existing facts:
  103,980 rows, 1.00 MB compressed, 16.33 MB uncompressed. For
  `/nfs/t283_imaging/`, gid 14976, uid 20155, type `other`, age all, current
  vector and index both returned 34,998 dirs, 764,218 files, and
  1,197,943,849,957 bytes. Simplified p50 improved from 31 ms to 19 ms, and
  EXPLAIN pruned from 13/13 t283 granules to 5/13. Full current
  `where_filtered_whole_mount` remained p50 726 ms, reading 149-164 MB.
- [ ] Prototype a parent-child oriented filter index such as
  `wrstat_child_filter_ageall` keyed by
  `(mount_path, snapshot_id, parent_dir, gid, uid, ft, child_dir)` for
  filter-aware `has_children` and child-summary existence.
  Open: Parent-child filter existence table was not built.
- [ ] Prototype virtual-ancestor filter rollups keyed by
  `(active_set_id, ancestor_dir, gid, uid, ft, age)` so root filter changes
  avoid live aggregation over all mounts.
  Open: Virtual filter rollups were not built; first prove narrow AgeAll rows
  for cold filters and `where`.
- [ ] Evaluate age-specific filters, not only AgeAll. Decide whether
  age-specific support needs vectors, rows, or explicit rejection.
  Open: Only AgeAll was evaluated.
- [ ] Record row amplification, compressed bytes, import time, memory, and p95
  or p99 latency improvements.
  Open: Row count, bytes, and latency were measured; importer time/memory and
  production-scale p95/p99 remain open.
- [x] Reject broad all-dimension indexes unless they beat facts-vectors on
  cold filter changes enough to justify storage and import cost.
  Result: Continue rejecting broad all-dimension indexes by default. The
  measured winner is the narrow AgeAll owner/type row index.

### 7. Path-Compressed `where` Frontiers

Question: can `where` avoid a cold breadth traversal that only becomes tolerable
after process-local cache warming?

- [ ] Trace first-run `where` query count and identify how many levels,
  children batches, summary batches, and filter scans are required before the
  requested split frontier is reached.
  Open: Query counts were not available because `system.query_log` was absent
  and the harness does not yet emit per-request query counts.
- [x] Prototype a path-compression table with per-directory `child_count`,
  `only_child`, `largest_child`, depth, subtree summary scalar columns, and
  possibly vector/filter metadata.
  Result: Deliberately skipped as a table prototype after modeling directory
  shapes. t283 has 35,006 dirs but only 901 single-child dirs (2.57%), while
  the expensive filter matched 34,998 dirs, so compression would skip little in
  the measured bottleneck.
- [ ] Test broad `where` using compressed single-child chains so it can jump to
  the next split point without loading each intermediate directory.
  Open: Broad compressed traversal was not tested.
- [x] Test filtered `where` with owner/type filters. If broad compression is
  not valid under filters, prototype a filter rollup that stores matching child
  counts or next matching descendants.
  Result: Deliberately skipped a filter-rollup prototype because the filtered
  model argues against prioritizing compression for t283: dominant AgeAll
  owner/type filter matches nearly every directory. Scratch122 has 19.86%
  single-child dirs, so compression may help selected deep Lustre chains later.
- [ ] Compare direct SQL recursive alternatives, batched frontier traversal,
  precomputed top-N child summaries, and a stored `where_frontier` table.
  Open: Alternatives were not compared beyond model evidence.
- [x] Measure `./wrstat-ui where` first run, second run, and in-process
  `tree_where` on the same data and filters.
  Result: Real CLI path skipped for missing auth/server setup. In-process
  `tree_where` was measured: `/` broad cold p50 296.960 ms; cached repeats
  about 64-70 ms; filtered `/` first sample 881.146 ms; filtered
  `/nfs/t283_imaging` cold p50/p95 1160.6/1554.4 ms.
- [x] Decide whether `where` needs its own schema object, or whether the
  Disktree navigation facts and filter indexes solve it sufficiently.
  Result: Do not prioritize a standalone `where_frontier` table. First prove
  AgeAll filter rows for cold filters/where, then reuse nav facts if needed.

### 8. Active Metadata And Readiness Lookup Reduction

Question: are cold paths spending too much time re-resolving active mounts and
projection readiness?

- [ ] Count active mount and readiness queries during one root page refresh and
  one filter switch.
  Open: Per-request query counts were not captured.
- [x] Verify whether the provider's active snapshot is used consistently for
  virtual ancestors and mount scopes, or whether requests still query
  `wrstat_mounts_active` repeatedly.
  Result: Code evidence shows provider active snapshots already exist and avoid
  some active view re-queries; `readyActiveMounts` still loops active mounts for
  readiness checks.
- [ ] Prototype a request-scoped active-set object passed through `DirInfo`,
  `DirInfos`, `Children`, and `DirsHaveChildren` so one request resolves active
  metadata once.
  Open: Request-scoped active-set object not prototyped.
- [x] Prototype bulk readiness preloading for all active mounts in a provider
  snapshot or active set.
  Result: Batched tuple readiness query over the four active mounts used
  ClickHouse `_minmax_count_projection` and benchmarked p50/p95 3/3 ms.
- [ ] Measure how much latency remains after removing redundant metadata
  lookups. If the improvement is small, do not overfit this area.
  Open: No code path was changed, so residual latency after batching was not
  measured. Treat metadata/readiness batching as tactical cleanup only.

### 9. Tactical Cache Warming And HTTP Caching

Question: is there a safe tactical fix while deeper schema work is built?

- [ ] Prototype startup/provider-update warming for broad `/`, `/lustre`,
  `/nfs`, and selected mount-root Disktree paths. Measure server startup and
  provider-update impact.
  Open: Startup/provider-update warming was not prototyped in this pass.
- [ ] Prototype warming all active mount-root scalar summaries and child rows
  after provider open so the first browser session does not pay the tree cache
  fill cost.
  Open: Not prototyped. Existing process caches were observed indirectly through
  cold versus warm timings.
- [ ] Prototype an HTTP or server-side response cache keyed by active set id,
  path, filter, user-visible permissions inputs, and endpoint version.
  Open: Browser-side URL response caching already exists, but no server-side
  response cache was prototyped.
- [ ] Verify cache invalidation on active snapshot publish, tombstone, provider
  update, and filter change.
  Open: Invalidation not tested.
- [x] Be explicit whether this is a stopgap. Do not recommend cache warming as
  the only final answer if cold `where` and cold filter changes remain slow.
  Result: Existing caches explain fast repeats: root Disktree moved from
  cold-provider p50 97.8 ms to warm 56 ms, and `/nfs` from 53.9 ms to 14 ms.
  Warming broad paths is useful tactically, but not a final answer while first
  filter switch and first `where` remain cold-slow.

### 10. ClickHouse Query-Shape Tuning

Question: can the current schema be made fast enough with query and table
ordering changes before adding new storage?

- [x] For each hot SQL shape, run `EXPLAIN indexes=1` and profile events.
  Confirm partition pruning, primary-key range pruning, read rows, read marks,
  read bytes, and result bytes.
  Result: `timeout 30s clickhouse-client --query "EXPLAIN indexes = 1 ..."`
  and `timeout 30s clickhouse-benchmark ...` were run for current/prototype
  active roots, parent facts, child facts, and AgeAll filter SQL. Query-log
  profile events were unavailable because `system.query_log` was absent.
- [ ] Compare `IN (...)` lists, external temporary tables, joins, and array
  bind alternatives for large child-summary batches.
  Open: Tuple `IN` and parent-prefix range were tested; external tables,
  joins, and array bind alternatives remain open.
- [ ] Compare `ARRAY JOIN` against ClickHouse array functions such as
  `arrayExists`, `arrayFilter`, `arrayMap`, and `arrayReduce` for filter
  existence and summary aggregation.
  Open: Not compared.
- [ ] Test smaller `index_granularity` or alternate orderings only on
  prototype tables, and record storage/import effects.
  Result: Alternate orderings were tested through duplicate prototype tables:
  parent facts and child facts. Open: smaller `index_granularity` and importer
  effects were not tested.
- [ ] Test whether column codecs or `LowCardinality` choices on path columns
  are helping or hurting current hot reads.
  Open: Not tested.
- [ ] Test whether ClickHouse projections can replace duplicate tables for
  parent-ordered or filter-ordered access. Only count this as viable if the
  optimizer reliably uses the projection in the real query.
  Open: Duplicate physical tables were tested; projection selection remains
  open.
- [x] Record any simple query fix that improves p95/p99 without adding schema.
  Result: Immediate fix candidate: bind full
  `(mount_path, snapshot_id, dir)` tuple for active-root facts. It reduced
  primary-key read from 20/20 to 4/20 granules and improved p50/p95 from 6/10 ms
  to 5/6 ms on the subset. Probe footgun: a SELECT alias named `dir` shadowed
  the source column in `WHERE`, producing an empty rollup until source columns
  were explicitly qualified.

### 11. Hybrid Bolt-Like Navigation Sidecar

Question: if ClickHouse remains too slow for cold interactive tree navigation,
should the system deliberately use a compact Bolt-like sidecar for tree UI
paths while keeping ClickHouse for import, history, and file APIs?

- [ ] Prototype or model a sidecar generated during import or provider update
  that stores the active tree navigation facts needed by Disktree and `where`.
  Open: No sidecar was prototyped or sized in this bounded pass.
- [ ] Compare an embedded Bolt/SQLite/RocksDB sidecar, a memory-mapped file,
  and a ClickHouse table designed to behave like a key/value navigation index.
  Open: Not compared because ClickHouse-native candidates still have promising
  lower-bound signals.
- [ ] Preserve active snapshot atomicity: the sidecar must publish only after
  the corresponding ClickHouse snapshot or active set is ready.
  Open: Atomic publish design not modeled.
- [ ] Measure sidecar build time, storage size, memory, cleanup, and first
  query latency.
  Open: Not measured.
- [x] Decide whether a hybrid design is worth the operational complexity. This
  is a radical fallback, not the preferred path unless ClickHouse-native
  candidates cannot hit the cold UX target.
  Result: Defer sidecar. AgeAll filter rows, active-prefix rollups, tuple query
  tuning, and parent/nav facts should be implemented/proven first.

### 12. Perf Harness And Acceptance Gates

Question: what measurements must exist so this problem does not regress again?

- [x] Add or extend `clickhouse-perf` operations for actual root UI semantics:
  root `/` Disktree, `/lustre`, `/nfs`, mount-root click, repeated visible
  child click, filter switch, and result-shape validation.
  Result: Existing harness already has cold-provider, provider-update
  cold-cache, visible-child, virtual, broad, filtered, and `where` operations.
  No harness code was edited in this recorder pass.
- [ ] Add an operation for first `./wrstat-ui where` through the real server
  path, not only in-process `Tree.Where`.
  Open: Real CLI/server path remains missing; current CLI measurement was
  blocked by auth/cert/Okta requirements.
- [ ] Add query-count, cache-hit/miss, ClickHouse read rows/bytes/marks, JSON
  response bytes, and operation result counts to perf reports.
  Open: Read rows/bytes are partly available, but query count, cache hit/miss,
  JSON bytes, gzip bytes, and robust query-log/profile events remain gaps.
- [ ] Add a paired broad/filtered correctness equivalence check so optional
  indexes and caches cannot return stale or partial summaries.
  Open: Prototype AgeAll index result was manually matched; this still needs a
  reusable correctness gate.
- [ ] Add table evidence for any selected new schema object: row count, active
  parts, compressed bytes, uncompressed bytes, import phase duration, and
  cleanup behavior.
  Result: Prototype table rows/bytes were captured: active prefix rollups 3
  rows/251 compressed bytes; child facts 46,303 rows/1.74 MB; parent facts
  46,307 rows/1.84 MB; AgeAll filter rows 103,980 rows/1.00 MB. Open: import
  phase duration and cleanup behavior remain because tables were derived after
  import and no new schema object is selected yet.
- [x] Define final cold UX gates. Suggested starting targets: first root page
  refresh under 1s server-side, first `/lustre` and `/nfs` clicks under 500ms
  server-side, first filter switch under 1s server-side, and first `where` no
  more than 10% slower than Bolt on the same subset. Adjust only with evidence.
  Result: Keep these gates for the next implementation proof, but require real
  HTTP/browser timing and a same-subset Bolt or accepted replacement baseline
  before declaring production-scale success.

## Comparison Matrix

Maintain this table as experiments complete.

| Design | Tables or caches added/removed | Root `/` cold | `/lustre` and `/nfs` cold | Filter switch cold | `where` first/cached | Import cost | Storage cost | Correctness risks | Complexity | Recommendation |
|---|---|---:|---:|---:|---:|---:|---:|---|---|---|
| Current branch baseline | Existing `wrstat_dir_facts`, `wrstat_children`, virtual children, process caches | `/` in-process cold-provider p50 97.8 ms on subset; real browser open | `/lustre` 70.7 ms, `/nfs` 53.9 ms cold-provider | Filtered `/nfs/t283_imaging` endpoint 94.2 ms but filtered `where` remains slow | Root `where` p50 316 ms; filtered t283 `where` p50 1161 ms; cached repeats about 64-84 ms | Existing import 400k lines in 5.56s | facts 7.28 MB, children 0.44 MB compressed | Query-log unavailable locally; in-process only | Medium | Baseline; insufficient for first filtered `where` |
| First-class virtual ancestor summary cache | Would add `wrstat_virtual_summary_cache` and sets; hooks exist but base DDL omits | Not measured; likely broad root help | May help namespace summaries, not ordinary child facts | Only helps filters if vectors/filter rows stored | Does not directly solve t283 whole-mount filtered `where` | Unknown refresh cost | Unknown | Staleness/readiness/active-set cleanup | Medium-high | Defer; prefer active-prefix rollups first |
| One-query Disktree navigation facts | Prototype `wrstat_child_facts` | Root virtual parents still need active rows/rollups | High-fanout parent p50 4 ms, one parent range | Needs vectors or filter companion rows | Could help frontiers only if reused | Import cost not measured | +1.74 MB compressed | Duplicate summary rows; virtual parent semantics | Medium | Promising for UI nav after rollups/filter index |
| Parent-ordered directory facts/projection | Prototype `wrstat_parent_facts` or projection | Not a root virtual fix | High-fanout parent p50 5 ms, one parent range | Needs vector reads or filter index | Indirect only | Import/projection cost not measured | +1.84 MB compressed | Projection optimizer must be proven | Medium | High-value query-shape candidate |
| Active-set mount-root rollups | Prototype `wrstat_active_prefix_rollups`; tune tuple query | Rollup p50 2 ms; tuple tune reduced 20/20 to 4/20 granules | Directly answers `/lustre`/`/nfs` scalar rollups | Needs vectors/filter rows | Indirect for root namespace only | Publish/update cost not measured | 3 rows, 251 compressed bytes scalar subset | Active-set invalidation/readiness | Low-medium | High priority |
| Filter-oriented index tables | Prototype `wrstat_dir_filter_ageall` | Helps root filters only with virtual/active rollups | Helps filtered child existence if parent-oriented variant added | t283 exact filter matched; p50 19 ms vs vector 31 ms simplified; 5/13 vs 13/13 granules | Strongest signal; current t283 whole-mount p50 726 ms | Import cost not measured | +1.00 MB compressed, 103,980 rows | Age semantics, key order, optional fallback | Medium | Highest priority for cold filters and `where` |
| Path-compressed `where` frontier | None; model only | No root impact | Could help selected deep chains | Poor fit for dominant t283 filter matching 34,998/35,006 dirs | t283 has only 2.57% single-child dirs | Unknown | Unknown | Filter correctness hard | Medium-high | Defer/reject as standalone first fix |
| Metadata/readiness batching only | No new table; batch active readiness | Small overhead reduction | Small overhead reduction | Small overhead reduction | No direct scan reduction | Low | None | Low | Low | Tactical cleanup only |
| Cache warming / HTTP cache only | Startup/provider warming; response cache keyed by active set/filter/path | Warm root p50 56 ms after cold 97.8 ms | `/nfs` warm 14 ms after cold 53.9 ms | New filters still cold unless warmed/computed | First `where` still slow | Startup/update cost unknown | Memory/response cache only | Invalidation and permissions keying | Low-medium | Stopgap, not final |
| Hybrid Bolt-like sidecar | None prototyped | Unknown | Unknown | Unknown | Unknown | Unknown | Unknown | Atomic publish and operational complexity | High | Fallback only after ClickHouse-native gates fail |

## Final Recommendation

End the investigation by editing this section in place.

- [x] State the recommended design direction and the minimal schema objects
  required.
  Result: Recommended direction for the next spec is ClickHouse-native and
  phased: first implement/prove narrow AgeAll filter rows for cold filters and
  `where`; add active-set prefix rollups plus immediate active-root tuple query
  tuning for `/`, `/lustre/`, and `/nfs/`; then prove parent-ordered facts or
  child navigation facts for Disktree click paths.
- [x] State which alternatives were rejected and why, using measured evidence.
  Result: Defer virtual summary cache because active-prefix rollups are smaller
  and measured. Treat metadata batching and warming as tactical only. Defer
  path-compressed `where` because t283 has only 2.57% single-child dirs and the
  dominant filter matches nearly all dirs. Defer sidecar because native
  candidates still have promising lower-bound measurements.
- [x] State what should be implemented first if the final answer has phases.
  Result: Phase 1 should prove `wrstat_dir_filter_ageall` through real importer
  DDL/writes/readiness/cleanup and cold filtered `where` gates. Phase 2 should
  add active-prefix scalar rollups and tuple query tuning. Phase 3 should choose
  between parent facts/projection and child nav facts for Disktree navigation.
- [x] State the expected DDL, writer, reader, readiness, cleanup, and cache
  invalidation changes.
  Result: Expected changes: base or gated DDL for `wrstat_dir_filter_ageall`
  with import-time AgeAll owner/type rows and cleanup by mount/snapshot; DDL for
  `wrstat_active_prefix_rollups` and a readiness/set marker keyed by active set;
  reader routing to AgeAll rows for eligible filters and active-prefix rollups
  for root/namespace scalar summaries; cleanup on inactive/tombstone/active-set
  replacement; process-cache invalidation keyed by active set, path, filter, and
  schema version. Parent/nav facts need either a reliably used projection or a
  duplicate table with importer writes and readiness evidence.
- [x] State the exact perf gates and datasets that must pass before the work is
  considered done.
  Result: Gates: on the 2026-06-07 subset and a larger production-like
  Lustre+NFS subset, first root page refresh under 1s server-side, first
  `/lustre` and `/nfs` clicks under 500 ms server-side, first filter switch
  under 1s server-side, first filtered `where` no more than 10% slower than
  same-subset Bolt or an agreed replacement baseline, and correctness matching
  current facts for broad and filtered summaries. Require row counts, active
  parts, compressed/uncompressed bytes, import time, memory, read rows/bytes,
  and p95/p99 for each new table.
- [x] State any `clickhouse-perf`, server tracing, or browser/API measurement
  improvements needed before implementation.
  Result: Add real HTTP/browser timing, first `./wrstat-ui where` through the
  server path, per-request query count, cache hit/miss counts, JSON/gzip bytes,
  and either enabled `system.query_log` or formal inspector fallback metrics.
- [x] Copy concrete spec decisions into a future `.docs/schema2/prompt.md` or
  implementation-phase prompt when requested.
  Result: Not copied now because this task only requested updating
  `.docs/schema2/investigate.md`. The concrete decisions above should seed the
  future prompt.

If no ClickHouse-native candidate can make cold page refresh, filter switch,
and first `where` fast enough, say that clearly and recommend the least risky
hybrid design rather than layering more process-local cache over a slow cold
path.

Final bounded-pass conclusion: this investigation does not prove every
production-scale question is solved. On the measured subset, the best-ranked
path is: narrow AgeAll filter rows for cold filters/where first; active-set
prefix rollups and tuple query tuning for root/namespace summaries second;
parent-ordered or child navigation facts for Disktree navigation third.
Metadata batching and warming are useful tactical work only. Virtual summary
cache and Bolt-like sidecar should stay deferred until these ClickHouse-native
options fail real HTTP/CLI gates.
