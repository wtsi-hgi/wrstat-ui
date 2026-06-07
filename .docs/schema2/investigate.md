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

- [ ] Read `.docs/schema/investigate.md`,
  `.docs/schema/investigation-results.md`, `.docs/schema/prompt.md`, and the
  recent `.docs/bugfixes` entries for Disktree, `where`, projection memory,
  and cold-provider perf. Record the already-tested ideas that must not be
  repeated without new evidence.
- [ ] Identify the current implemented ClickHouse schema and query routing.
  Confirm whether optional objects such as `wrstat_dir_filter_ageall` and
  virtual summary cache tables are present in base DDL, only in tests, or not
  present at all.
- [ ] Select a mixed Lustre+NFS subset from `/home/ubuntu/output/lustre` and
  `/home/ubuntu/output/nfs`. Include at least one directory-heavy NFS dataset
  such as `t283_imaging` when feasible, and representative Lustre datasets such
  as `scratch120`, `scratch122`, or `scratch127`.
- [ ] Record selected dataset paths, mount paths, `stats.gz` sizes, `maxLines`,
  imported line counts, and why the subset is large enough to exercise `/`,
  `/lustre`, `/nfs`, filter changes, and mount-root clicks.
- [ ] Build a local binary in `.tmp/agent/schema2/wrstat-ui`.
- [ ] Import the subset into a fresh ClickHouse database and capture table row
  counts, active parts, compressed bytes, uncompressed bytes, vector entry
  stats, and import phase timings.
- [ ] Verify root semantics on the imported subset: `/` total equals the sum of
  selected active Lustre and NFS mounts, and `/lustre` plus `/nfs` children have
  their respective summary data.
- [ ] Measure current branch cold and warm query baselines with
  `clickhouse-perf`, including root `/`, virtual ancestors `/lustre` and `/nfs`,
  selected mount roots, filter changes, cold provider, provider-update cold
  cache, same-provider warm, and visible-child repeated paths.
- [ ] Measure the actual web/API path if possible, not only in-process perf:
  page refresh to `/`, first click `/lustre`, first click `/nfs`, first click
  into a selected mount, repeat click, and switch to a different filter.
- [ ] Measure `./wrstat-ui where` first run and cached run against the same
  server/database/filter, and separately measure in-process `tree_where` so CLI,
  HTTP, JSON, and database costs are not conflated.
- [ ] Add temporary query tracing if needed: per request query count, query
  names, cache hit/miss counts, ClickHouse elapsed time, read rows, read bytes,
  and returned rows/bytes.
- [ ] If Bolt comparison data is available or can be produced on the same
  subset, record Bolt root Disktree and `where` timings as the user-facing
  target.

## Investigation Checklist

### 1. Trace The True Cold Path

Question: where are the seconds going on first page refresh and first filter
change?

- [ ] Add or use instrumentation that separates browser/render time, HTTP
  handler time, provider/database time, ClickHouse query time, JSON size, gzip
  size, and React Disktree render time.
- [ ] For `/`, `/lustre`, `/nfs`, and one mount root, record the exact backend
  calls made by the Disktree endpoint: `DirInfo`, `DirInfos`, `Children`,
  `DirsHaveChildren`, virtual summary/cache checks, active mount resolution, and
  readiness checks.
- [ ] Record cache hits and misses for children, GUTAs, scalar summaries,
  readiness, and active metadata on first request, repeated request, and after
  filter changes.
- [ ] Use ClickHouse profile events or `system.query_log` to record read rows,
  read bytes, result rows, elapsed time, and memory for each hot SQL shape.
- [ ] Determine whether the root delay is mostly virtual ancestor composition,
  child summary/has-children fanout, active mount/readiness lookups, vector
  filtering, response size, or frontend rendering.
- [ ] Add a `Result:` subsection here with a table of cold root, cold
  `/lustre`, cold `/nfs`, cold mount-root, filter-change, and repeated timings.

### 2. First-Class Virtual Ancestor Summary Cache

Question: should the optional virtual summary cache become a real schema table
again, or be replaced by a better ancestor rollup?

- [ ] Confirm the current base DDL status of `wrstat_virtual_summary_cache` and
  `wrstat_virtual_summary_sets`; current code has hooks, but base schema may
  intentionally omit the tables.
- [ ] Prototype a persisted ancestor summary cache keyed by active set id and
  virtual ancestor dir, covering `/`, `/lustre/`, `/nfs/`, and any other parent
  directories above active mount roots.
- [ ] Store enough data to answer broad and filtered `DirInfo` for virtual
  ancestors without scanning all active mount-root vectors on first request:
  scalar all/default-file summaries, aligned filter vectors, `child_count`,
  `updated_at`, and readiness.
- [ ] Ensure root `/` and namespace summaries are composed from all selected
  active mount roots and preserve exact UID/GID/file-type/age filter semantics.
- [ ] Measure refresh cost when the active set changes, startup/provider-open
  cost, storage bytes, active-set cleanup, and first request latency.
- [ ] Test whether a cache of only virtual ancestors is enough, or whether
  mount-root child summaries still dominate first click into `/lustre` and
  `/nfs`.
- [ ] Reject the cache if it only helps broad root but not filter changes or
  click-through paths, unless paired with another design that covers those.

### 3. One-Query Disktree Navigation Facts

Question: can the Disktree endpoint be answered by one ordered read instead of
the current parent summary, children, child summaries, and has-children sequence?

- [ ] Prototype a denormalized navigation table, for example
  `wrstat_tree_nav_facts` or `wrstat_child_facts`, keyed by active set id or
  `(mount_path, snapshot_id, parent_dir, child_dir)`.
- [ ] Include each visible child row's display path, child summary scalar
  columns, child filter vectors or a pointer to facts, `child_count`, and
  enough metadata to answer `has_children` without a second query.
- [ ] Cover virtual parents (`/`, `/lustre`, `/nfs`) and ordinary mount
  directories. Root `/` must still return separate `/lustre` and `/nfs` boxes.
- [ ] Measure broad Disktree first request and repeated request for `/`,
  `/lustre`, `/nfs`, selected mount roots, and visible children.
- [ ] Measure filtered Disktree after changing UID/GID/file-type filters. If
  vectors in the navigation table are too wide, test a narrow filter existence
  companion table.
- [ ] Compare import row amplification and compressed bytes against the current
  `wrstat_children` plus `wrstat_dir_facts` shape.
- [ ] Decide whether this table replaces `wrstat_children`, supplements it only
  for UI paths, or is too expensive.

### 4. Parent-Ordered Directory Facts

Question: is the current `(mount_path, snapshot_id, dir)` order forcing
expensive `dir IN (...)` lookups for child summaries?

- [ ] Prototype adding `parent_dir`, `name`, `depth`, and possibly
  `has_children` to `wrstat_dir_facts`, plus a ClickHouse projection or
  duplicate table ordered by `(mount_path, snapshot_id, parent_dir, name)`.
- [ ] Test whether one parent-range query can replace `wrstat_children` plus
  `DirInfos(children)` for ordinary mount-directory clicks.
- [ ] Verify ClickHouse actually uses the projection/order with
  `EXPLAIN indexes=1`; do not assume projections are selected.
- [ ] Compare a ClickHouse projection, a second physical table, and folding
  child edges into the canonical facts table.
- [ ] Measure first click into high-fanout directories, low-fanout directories,
  and deep single-child chains.
- [ ] Record import cost, storage cost, and cleanup complexity.

### 5. Active-Set Mount-Root Rollups

Question: can root and namespace reads avoid touching one partition per active
mount?

- [ ] Prototype an active-set table containing only active mount-root facts in
  one active-set partition, for example `wrstat_active_mount_root_facts`.
- [ ] Test whether `/`, `/lustre`, and `/nfs` summaries can be answered from
  this small active-set table instead of reading mount-root rows from many
  `(mount_path, snapshot_id)` partitions.
- [ ] Add per-namespace rollup rows if direct mount-root aggregation is still
  too slow, for example `wrstat_active_prefix_rollups(active_set_id, dir, ...)`.
- [ ] Include vectors or re-expanded filter rows so filter changes do not
  rescan all mount-root arrays.
- [ ] Measure active-set publish/update cost when one mount changes and the
  active-set id changes.
- [ ] Compare this design with the virtual ancestor summary cache. Decide
  whether they are the same concept, complementary layers, or competing
  designs.

### 6. Filter Indexes For Cold Filter Changes

Question: which minimal filter-oriented structures make first use of a new
filter fast?

- [ ] Verify whether `wrstat_dir_filter_ageall` exists in the tested database,
  whether it is populated during import, and whether query routing uses it for
  the filters that are slow in the UI.
- [ ] Measure facts-vector filtering versus `wrstat_dir_filter_ageall` for
  common and selective UID/GID/type filters on root, `/lustre`, `/nfs`, and
  whole-mount `where`.
- [ ] Prototype a parent-child oriented filter index such as
  `wrstat_child_filter_ageall` keyed by
  `(mount_path, snapshot_id, parent_dir, gid, uid, ft, child_dir)` for
  filter-aware `has_children` and child-summary existence.
- [ ] Prototype virtual-ancestor filter rollups keyed by
  `(active_set_id, ancestor_dir, gid, uid, ft, age)` so root filter changes
  avoid live aggregation over all mounts.
- [ ] Evaluate age-specific filters, not only AgeAll. Decide whether
  age-specific support needs vectors, rows, or explicit rejection.
- [ ] Record row amplification, compressed bytes, import time, memory, and p95
  or p99 latency improvements.
- [ ] Reject broad all-dimension indexes unless they beat facts-vectors on
  cold filter changes enough to justify storage and import cost.

### 7. Path-Compressed `where` Frontiers

Question: can `where` avoid a cold breadth traversal that only becomes tolerable
after process-local cache warming?

- [ ] Trace first-run `where` query count and identify how many levels,
  children batches, summary batches, and filter scans are required before the
  requested split frontier is reached.
- [ ] Prototype a path-compression table with per-directory `child_count`,
  `only_child`, `largest_child`, depth, subtree summary scalar columns, and
  possibly vector/filter metadata.
- [ ] Test broad `where` using compressed single-child chains so it can jump to
  the next split point without loading each intermediate directory.
- [ ] Test filtered `where` with owner/type filters. If broad compression is
  not valid under filters, prototype a filter rollup that stores matching child
  counts or next matching descendants.
- [ ] Compare direct SQL recursive alternatives, batched frontier traversal,
  precomputed top-N child summaries, and a stored `where_frontier` table.
- [ ] Measure `./wrstat-ui where` first run, second run, and in-process
  `tree_where` on the same data and filters.
- [ ] Decide whether `where` needs its own schema object, or whether the
  Disktree navigation facts and filter indexes solve it sufficiently.

### 8. Active Metadata And Readiness Lookup Reduction

Question: are cold paths spending too much time re-resolving active mounts and
projection readiness?

- [ ] Count active mount and readiness queries during one root page refresh and
  one filter switch.
- [ ] Verify whether the provider's active snapshot is used consistently for
  virtual ancestors and mount scopes, or whether requests still query
  `wrstat_mounts_active` repeatedly.
- [ ] Prototype a request-scoped active-set object passed through `DirInfo`,
  `DirInfos`, `Children`, and `DirsHaveChildren` so one request resolves active
  metadata once.
- [ ] Prototype bulk readiness preloading for all active mounts in a provider
  snapshot or active set.
- [ ] Measure how much latency remains after removing redundant metadata
  lookups. If the improvement is small, do not overfit this area.

### 9. Tactical Cache Warming And HTTP Caching

Question: is there a safe tactical fix while deeper schema work is built?

- [ ] Prototype startup/provider-update warming for broad `/`, `/lustre`,
  `/nfs`, and selected mount-root Disktree paths. Measure server startup and
  provider-update impact.
- [ ] Prototype warming all active mount-root scalar summaries and child rows
  after provider open so the first browser session does not pay the tree cache
  fill cost.
- [ ] Prototype an HTTP or server-side response cache keyed by active set id,
  path, filter, user-visible permissions inputs, and endpoint version.
- [ ] Verify cache invalidation on active snapshot publish, tombstone, provider
  update, and filter change.
- [ ] Be explicit whether this is a stopgap. Do not recommend cache warming as
  the only final answer if cold `where` and cold filter changes remain slow.

### 10. ClickHouse Query-Shape Tuning

Question: can the current schema be made fast enough with query and table
ordering changes before adding new storage?

- [ ] For each hot SQL shape, run `EXPLAIN indexes=1` and profile events.
  Confirm partition pruning, primary-key range pruning, read rows, read marks,
  read bytes, and result bytes.
- [ ] Compare `IN (...)` lists, external temporary tables, joins, and array
  bind alternatives for large child-summary batches.
- [ ] Compare `ARRAY JOIN` against ClickHouse array functions such as
  `arrayExists`, `arrayFilter`, `arrayMap`, and `arrayReduce` for filter
  existence and summary aggregation.
- [ ] Test smaller `index_granularity` or alternate orderings only on
  prototype tables, and record storage/import effects.
- [ ] Test whether column codecs or `LowCardinality` choices on path columns
  are helping or hurting current hot reads.
- [ ] Test whether ClickHouse projections can replace duplicate tables for
  parent-ordered or filter-ordered access. Only count this as viable if the
  optimizer reliably uses the projection in the real query.
- [ ] Record any simple query fix that improves p95/p99 without adding schema.

### 11. Hybrid Bolt-Like Navigation Sidecar

Question: if ClickHouse remains too slow for cold interactive tree navigation,
should the system deliberately use a compact Bolt-like sidecar for tree UI
paths while keeping ClickHouse for import, history, and file APIs?

- [ ] Prototype or model a sidecar generated during import or provider update
  that stores the active tree navigation facts needed by Disktree and `where`.
- [ ] Compare an embedded Bolt/SQLite/RocksDB sidecar, a memory-mapped file,
  and a ClickHouse table designed to behave like a key/value navigation index.
- [ ] Preserve active snapshot atomicity: the sidecar must publish only after
  the corresponding ClickHouse snapshot or active set is ready.
- [ ] Measure sidecar build time, storage size, memory, cleanup, and first
  query latency.
- [ ] Decide whether a hybrid design is worth the operational complexity. This
  is a radical fallback, not the preferred path unless ClickHouse-native
  candidates cannot hit the cold UX target.

### 12. Perf Harness And Acceptance Gates

Question: what measurements must exist so this problem does not regress again?

- [ ] Add or extend `clickhouse-perf` operations for actual root UI semantics:
  root `/` Disktree, `/lustre`, `/nfs`, mount-root click, repeated visible
  child click, filter switch, and result-shape validation.
- [ ] Add an operation for first `./wrstat-ui where` through the real server
  path, not only in-process `Tree.Where`.
- [ ] Add query-count, cache-hit/miss, ClickHouse read rows/bytes/marks, JSON
  response bytes, and operation result counts to perf reports.
- [ ] Add a paired broad/filtered correctness equivalence check so optional
  indexes and caches cannot return stale or partial summaries.
- [ ] Add table evidence for any selected new schema object: row count, active
  parts, compressed bytes, uncompressed bytes, import phase duration, and
  cleanup behavior.
- [ ] Define final cold UX gates. Suggested starting targets: first root page
  refresh under 1s server-side, first `/lustre` and `/nfs` clicks under 500ms
  server-side, first filter switch under 1s server-side, and first `where` no
  more than 10% slower than Bolt on the same subset. Adjust only with evidence.

## Comparison Matrix

Maintain this table as experiments complete.

| Design | Tables or caches added/removed | Root `/` cold | `/lustre` and `/nfs` cold | Filter switch cold | `where` first/cached | Import cost | Storage cost | Correctness risks | Complexity | Recommendation |
|---|---|---:|---:|---:|---:|---:|---:|---|---|---|
| Current branch baseline | `wrstat_dir_facts`, `wrstat_children`, virtual children, process cache |  |  |  |  |  |  |  |  |  |
| First-class virtual ancestor summary cache |  |  |  |  |  |  |  |  |  |  |
| One-query Disktree navigation facts |  |  |  |  |  |  |  |  |  |  |
| Parent-ordered directory facts/projection |  |  |  |  |  |  |  |  |  |  |
| Active-set mount-root rollups |  |  |  |  |  |  |  |  |  |  |
| Filter-oriented index tables |  |  |  |  |  |  |  |  |  |  |
| Path-compressed `where` frontier |  |  |  |  |  |  |  |  |  |  |
| Metadata/readiness batching only |  |  |  |  |  |  |  |  |  |  |
| Cache warming / HTTP cache only |  |  |  |  |  |  |  |  |  |  |
| Hybrid Bolt-like sidecar |  |  |  |  |  |  |  |  |  |  |

## Final Recommendation

End the investigation by editing this section in place.

- [ ] State the recommended design direction and the minimal schema objects
  required.
- [ ] State which alternatives were rejected and why, using measured evidence.
- [ ] State what should be implemented first if the final answer has phases.
- [ ] State the expected DDL, writer, reader, readiness, cleanup, and cache
  invalidation changes.
- [ ] State the exact perf gates and datasets that must pass before the work is
  considered done.
- [ ] State any `clickhouse-perf`, server tracing, or browser/API measurement
  improvements needed before implementation.
- [ ] Copy concrete spec decisions into a future `.docs/schema2/prompt.md` or
  implementation-phase prompt when requested.

If no ClickHouse-native candidate can make cold page refresh, filter switch,
and first `where` fast enough, say that clearly and recommend the least risky
hybrid design rather than layering more process-local cache over a slow cold
path.
