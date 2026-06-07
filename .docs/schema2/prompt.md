# Prompt for ClickHouse cold tree performance spec

Use this as the source-of-truth input for the spec-writer workflow. It
incorporates the final recommendations from `.docs/schema2/investigate.md`;
the spec-writer should turn these decisions into a cohesive feature spec with
user stories, acceptance tests, implementation phases, and perf gates.

## Goal

Write a ClickHouse-native cold tree performance spec for `wrstat-ui`.

The existing clean ClickHouse schema work in `.docs/schema` is mostly
implemented, but first uncached web UI interactions and first filtered `where`
paths remain too slow compared with Bolt. The new spec must make the first
uncached interaction fast enough. It must not rely on process-local warming,
browser cache state, or repeated clicks as the final answer.

The spec must implement the investigation's final ClickHouse-native direction:

- narrow AgeAll owner/type filter rows for cold filter changes and `where`
- active-set prefix rollups plus immediate active-root tuple query tuning for
  `/`, `/lustre/`, and `/nfs/`
- a measured Disktree navigation improvement using either parent-ordered
  facts/projection or child navigation facts
- REST/HTTP and Bolt perf gates that prove the user-facing path improved
- correction-pass evidence and new harness coverage for current blind spots
- explicit investigation of the filtered REST `/nfs/t283_imaging/` anomaly

The spec must also state that metadata batching and cache warming are tactical
only, while the virtual summary cache and a hybrid Bolt-like sidecar are
fallback/deferred designs rather than the first implementation path.

## Measured Dataset

The required baseline subset was measured on 2026-06-07 from the production-like
datasets under `/home/ubuntu/output/lustre` and `/home/ubuntu/output/nfs`.
The spec must require this exact subset, plus at least one larger
production-like Lustre+NFS subset, for final perf gates.

| Dataset | Mount | Source family | Compressed `stats.gz` bytes | Imported lines | Dir facts | Children |
|---|---|---|---:|---:|---:|---:|
| `t283_imaging` | `/nfs/t283_imaging/` | `/home/ubuntu/output/nfs` | 6,095,109,852 | 100,000 | 70,523 | 35,005 |
| `scratch120` | `/lustre/scratch120/` | `/home/ubuntu/output/lustre` | 1,420,306,103 | 100,000 | 1,508 | 546 |
| `scratch122` | `/lustre/scratch122/` | `/home/ubuntu/output/lustre` | 2,780,894,204 | 100,000 | 32,897 | 9,608 |
| `scratch127` | `/lustre/scratch127/` | `/home/ubuntu/output/lustre` | 5,317,090,708 | 100,000 | 2,490 | 1,144 |

The subset imported 400,000 file records. Root semantics were validated:
`/` equals all selected active Lustre and NFS mounts, and root Disktree exposes
separate `/lustre` and `/nfs` boxes with their respective totals.

ClickHouse import baseline on this subset:

- database: `wrstat_schema2_baseline_084721`
- import wall time: 5.56s
- max RSS: 407,392,256 bytes
- `wrstat_files`: 400,000 rows, 7.09 MB compressed
- `wrstat_dir_facts`: 46,307 rows, 7.28 MB compressed, 216.53 MB uncompressed
- `wrstat_children`: 46,303 rows, 0.44 MB compressed
- facts vectors: 915,933 entries, average 19.78 per dir, max 1,030

Same-subset Bolt comparison:

- import harness time: 3,677.091 ms; wall time 0:03.71
- max RSS: 193,656 KB
- storage: 106,700,800 bytes DGUTA plus 81,920 bytes basedirs

## Current Timing Evidence

The spec must carry forward both ClickHouse and Bolt numbers as acceptance
targets and regression guards.

Current ClickHouse REST and in-process anchors on the measured subset:

| Case | Timing | Evidence |
|---|---:|---|
| `/` auth REST tree | p50/p95 88.2/89.8 ms | 1,036 JSON bytes, 349 offline gzip bytes |
| `/lustre/` auth REST tree | p50/p95 61.1/71.4 ms | browser-like sequence p50 47.4 ms |
| `/nfs/` auth REST tree | p50/p95 43.9/46.3 ms | browser-like sequence p50 35.6 ms |
| `/nfs/t283_imaging/` auth REST tree | p50/p95 27.2/31.1 ms | browser-like mount-root p50 10.7 ms |
| `/` no-auth REST `where` | p50/p95 306.1/334.9 ms | 200,102 JSON bytes, 16,480 offline gzip bytes |
| `/` type-filter REST `where` | p50/p95 527.3/553.1 ms | 167,214 JSON bytes, 13,713 offline gzip bytes |
| `/nfs/t283_imaging/` filtered `where` | p50/p95 1160.6/1554.4 ms | gid+uid+type chperf; 149-164 MB read |
| repeated cached `where` | about 64-84 ms | process-local caches hide cold cost |

Same-subset Bolt p95 targets from the correction pass:

| Case | Bolt p50/p95 |
|---|---:|
| `/` Disktree | 1.933/2.459 ms |
| `/lustre/` Disktree | 1.681/2.269 ms |
| `/nfs/` Disktree | 0.242/0.261 ms |
| `/nfs/t283_imaging/` Disktree | 0.463/1.052 ms |
| `/` `where_cold_then_cached` | 36.570/40.651 ms |
| `/lustre/` `where_cold_then_cached` | 47.793/49.954 ms |
| `/nfs/` `where_cold_then_cached` | 11.280/13.137 ms |
| `/nfs/t283_imaging/` `where_cold_then_cached` | 13.203/23.621 ms |
| filtered direct t283 `where_cold_then_cached` | 67.355/78.534 ms |

The spec must set server-side gates of:

- first root page refresh under 1s
- first `/lustre/` and `/nfs/` clicks under 500 ms
- first filter switch under 1s
- first filtered `where` no more than 10% slower than same-subset Bolt
- no correctness regression against current ClickHouse facts for broad or
  filtered summaries

These gates are minimum UX gates. They do not replace the Bolt comparison; the
spec must still report Bolt p95 deltas so the remaining gap is visible.

## Required Design Direction

### 1. Narrow AgeAll Filter Rows

The spec must implement and prove a narrow derived AgeAll owner/type row index,
expected to be named `wrstat_dir_filter_ageall`.

This is no longer merely an optional curiosity for the first phase. The
investigation's strongest measured cold signal was filtered `where`, and the
measured winner was the narrow AgeAll owner/type row index rather than a broad
all-dimension index.

The spec must define:

- final DDL for `wrstat_dir_filter_ageall`
- the exact key order and partitioning, starting from
  `(mount_path, snapshot_id, gid, uid, ft, dir)` unless the spec proves another
  order is better
- AgeAll-only semantics; age-specific filters must not accidentally read
  all-age-only rows
- importer writes during deterministic snapshot import, not post-hoc manual
  derivation
- readiness through the same final projection/readiness marker, or a clearly
  named stricter marker if needed
- cleanup by `(mount_path, snapshot_id)` partition on failed, old, inactive, or
  tombstoned snapshots
- reader routing for eligible AgeAll UID/GID/file-type filters
- fallback to `wrstat_dir_facts` when the table is absent, not ready, or
  semantically inapplicable
- acceptance tests proving index results exactly match `wrstat_dir_facts`
  vector results for broad and selective filters

Measured prototype evidence to carry into the spec:

- `wrstat_dir_filter_ageall`: 103,980 rows, 1.00 MB compressed,
  16.33 MB uncompressed on the measured subset
- for `/nfs/t283_imaging/`, gid `14976`, uid `20155`, type `other`, age all:
  current vectors and the prototype index both returned 34,998 dirs,
  764,218 files, and 1,197,943,849,957 bytes
- simplified p50 improved from 31 ms to 19 ms
- EXPLAIN pruned from 13/13 t283 granules to 5/13
- full current `where_filtered_whole_mount` still remained p50 726 ms,
  reading 149-164 MB, so endpoint-level proof is still required

Reject broad all-dimension indexes by default. The spec must not add GID-only,
UID-only, age-specific, or full `(gid, uid, file_type, age, dir)` row indexes
unless it includes production-scale evidence that the narrow AgeAll table
cannot meet the gates.

### 2. Active-Set Prefix Rollups And Tuple Query Tuning

The spec must implement active-set prefix rollups for virtual/root namespace
summaries and tune the current active-root fact query immediately.

The expected new object is a small derived table such as
`wrstat_active_prefix_rollups`, plus a readiness/set marker keyed by a
deterministic active-set id. The active-set id must derive from the sorted
active mount rows, including mount path, snapshot id, and updated-at or an
equivalent active generation input.

The spec must define:

- DDL for active-prefix rollup rows covering at least `/`, `/lustre/`, and
  `/nfs/`
- whether the first version stores scalar summaries only or also stores
  filter-aware rows/vectors
- how active-prefix rollups are refreshed on publish, rollback, tombstone, and
  provider update
- readiness semantics for active-set rollups
- cleanup of replaced active-set partitions
- exact root semantics: `/` must aggregate all active selected Lustre and NFS
  mounts, while `/lustre/` and `/nfs/` remain separate visible boxes
- cache invalidation keyed by active set, path, filter, and schema/query
  version

The spec must also require the simple query-shape fix identified in the
investigation: bind the full `(mount_path, snapshot_id, dir)` tuple for active
mount-root facts instead of relying on a partial `(mount_path, snapshot_id)`
tuple plus `dir = mount_path`.

Measured prototype evidence to carry into the spec:

- scalar `wrstat_active_prefix_rollups(active_set_id, dir)` prototype had 3
  rows and 251 compressed bytes
- scalar rollup matched current totals: `all_count=400000`,
  `file_count=353197`, `child_count=9`
- rollup EXPLAIN read 1/1 granules and benchmarked p50/p95 2/4 ms
- current active-root SQL read 20/20 granules
- full tuple active-root SQL read 4/20 granules and improved p50/p95 from
  6/10 ms to 5/6 ms

The spec must explicitly compare active-prefix rollups with the older virtual
summary cache concept and choose active-prefix rollups first because they are
smaller and directly measured.

### 3. Disktree Navigation Facts

The spec must include a third implementation area for Disktree click-path
navigation. It must choose, or define a strict measured decision gate between,
one of these ClickHouse-native shapes:

- parent-ordered directory facts or a reliably used ClickHouse projection
- child navigation facts, such as `wrstat_child_facts` or
  `wrstat_tree_nav_facts`

The chosen design must cover ordinary mount directories and virtual parents.
Root `/` must still show separate `/lustre` and `/nfs` children with correct
summaries.

The spec must require:

- a single parent-range or projection-backed read for ordinary child summaries
  where practical
- child display path, scalar summary, `child_count`/`has_children`, and either
  filter vectors, a pointer to canonical facts, or a narrow filter companion
  for filtered Disktree
- proof that ClickHouse uses the intended order or projection via
  `EXPLAIN indexes=1`
- import-time writes or projection materialization behavior
- row count, compressed/uncompressed bytes, import phase timing, cleanup, and
  readiness evidence
- tests for high-fanout parents, low-fanout parents, deep chains, virtual
  parents, broad filters, and owner/type filters

Measured prototype evidence to carry into the spec:

- `wrstat_child_facts`: 46,303 rows, 1.74 MB compressed,
  38.94 MB uncompressed
- for a 305-child high-fanout parent, current `children` plus `dir IN` facts
  p50 was 6 ms; `wrstat_child_facts` parent range p50 was 4 ms
- `wrstat_parent_facts`: 46,307 rows, 1.84 MB compressed,
  39.70 MB uncompressed
- parent-ordered facts answered the same high-fanout parent via one
  parent-prefix range with p50 5 ms
- `EXPLAIN indexes=1` showed binary-search parent range and 1/6 granules for
  the duplicate parent table
- ClickHouse projection optimizer behavior was not proven, so a projection
  only counts if the real endpoint query reliably uses it

The spec must not prioritize a standalone `where_frontier` table before these
navigation and AgeAll paths. The investigation modeled t283 as a poor fit for
path compression: only 901 of 35,006 directories were single-child dirs
2.57%, and the dominant filtered case matched 34,998 directories.

## Query Routing Requirements

The spec must require all affected tree readers to use the selected
ClickHouse-native paths:

- `DirInfo`
- `DirInfos`
- `Children`
- `DirsHaveChildren`
- Disktree endpoint paths
- `Where`
- `Info`
- permission checks that depend on tree summaries

Eligible AgeAll owner/type filters must use `wrstat_dir_filter_ageall` where
it is ready and semantically valid. Ineligible filters must use
`wrstat_dir_facts` vectors. No age-specific query may read compacted AgeAll
rows as if they were age-specific rows.

Virtual ancestor summaries must use active-prefix rollups first for `/`,
`/lustre/`, `/nfs/`, and future virtual ancestors covered by the active set.
The older virtual summary cache may be used only as a later fallback if
active-prefix rollups and tuple tuning fail measured many-active-mount gates.

Process-local tree caches may remain as accelerators, but correctness and
acceptance timings must be measured from cold provider or cold request states.

## Import, Readiness, And Cleanup Requirements

The spec must define deterministic snapshot import order for the new objects:

- stream file rows and canonical facts as the existing clean schema requires
- write `wrstat_dir_filter_ageall` during import for the same snapshot
- write any selected parent/nav facts during import, or define the exact
  projection materialization behavior if a projection is selected
- write readiness markers only after facts, filter rows, and selected
  navigation rows are complete
- publish active mount events only after snapshot-scoped data is ready
- refresh active-prefix rollups only after the active set changes

For each new or changed object, specify:

- canonical or derived status
- engine, partitioning, ordering, codecs, and `LowCardinality` choices
- writer path and failure behavior
- reader/query path
- readiness marker
- cleanup for failed snapshot, old snapshot, inactive tombstone, rollback, and
  active-set replacement
- cache invalidation
- row count, active parts, compressed bytes, uncompressed bytes, import phase
  time, memory, read rows/bytes/marks, and response bytes evidence

The spec must keep import memory bounded. It must extend or reuse the existing
bounded block-writer pattern for AgeAll and selected navigation writes rather
than accumulating unbounded per-directory data.

## REST, HTTP, CLI, Browser, And Tracing Gates

The correction pass measured REST handlers and proved that the server/auth/JSON
path tracks in-process Disktree and `where` on the measured subset. The spec
must preserve this evidence and expand the harness so future work cannot hide
costs in unmeasured layers.

The spec must require:

- real REST/HTTP perf gates for auth tree and no-auth or authenticated `where`
  endpoints
- `./wrstat-ui where` first-run timing through the real CLI/server path, not
  only in-process `Tree.Where`
- browser or Playwright timing that separates fetch, JSON decode, React render,
  layout/canvas work, and total first visible update
- per-request query counts
- cache hit/miss counts for children, summaries, readiness, active metadata,
  and provider caches
- ClickHouse read rows, read bytes, read marks, elapsed time, and memory using
  `system.query_log` when available
- a formal inspector/profile-events fallback when `system.query_log` is not
  available
- JSON bytes and gzip bytes for each measured REST operation
- result counts and summary checks in both ClickHouse and Bolt perf JSON
- paired broad/filtered correctness equivalence checks for optional indexes,
  rollups, and caches

The spec must investigate and resolve the filtered REST anomaly before relying
on filtered click-through measurements: isolated type-only REST
`/nfs/t283_imaging/` `where` returned 2 rows, but the same filtered request
after a filtered root `/` request returned 87 rows consistently in the same
provider. Treat this as a possible correctness or cache-state bug until
disproved.

## Acceptance Tests

The final spec must require focused tests for:

- `wrstat_dir_filter_ageall` DDL, ordering, partitioning, and absence from
  age-specific routes unless semantically valid
- importer writes for AgeAll rows, including final partial block flush,
  ambiguous-send cleanup, retry behavior, and bounded memory
- AgeAll readiness marker behavior: misses before rows are complete, hits
  after completion, and fallback to facts when absent or not ready
- exact AgeAll result equivalence with `wrstat_dir_facts` vectors for common
  and selective UID/GID/type filters
- cleanup of AgeAll rows for failed/current/old snapshots
- active-prefix rollup DDL and active-set readiness markers
- root `/`, `/lustre/`, and `/nfs/` scalar correctness after publish,
  rollback, tombstone, provider update, and active-set replacement
- full active-root tuple query routing and a regression test that prevents the
  old broad tuple shape from returning
- cache invalidation on active set, path, filter, and query/schema version
- the selected parent/nav facts design, including high-fanout, low-fanout,
  deep-chain, filtered, and virtual-parent Disktree cases
- proof that a ClickHouse projection is selected by the real endpoint query if
  a projection is used instead of a physical table
- REST tree and REST `where` response correctness, status codes, JSON bytes,
  and gzip bytes
- real CLI `where` first-run timing where server/auth setup is available
- browser/render timing once the browser harness is available
- Bolt comparison JSON result counts and summary equivalence
- the filtered REST `/nfs/t283_imaging/` anomaly, including a regression test
  for request-order independence

Every spec acceptance test that touches Go behavior must have a corresponding
GoConvey test in the implementation plan. Perf gates may be integration tests,
`clickhouse-perf`/`bolt-perf` checks, or documented CI/manual gates, but the
spec must make their command shape, dataset, and required JSON fields explicit.

## Performance Gates

The spec must require final perf runs on:

- the measured 2026-06-07 four-mount subset above
- at least one larger production-like mixed Lustre+NFS subset from
  `/home/ubuntu/output/lustre` and `/home/ubuntu/output/nfs`
- a directory-heavy NFS case including `t283_imaging`
- representative Lustre cases including `scratch120`, `scratch122`, and
  `scratch127`

For each run, the report must include:

- import wall time and phase timings
- max RSS or equivalent memory metric
- table row counts, active parts, compressed bytes, and uncompressed bytes
- new-object row amplification versus `wrstat_dir_facts` and `wrstat_children`
- ClickHouse read rows, read bytes, read marks, elapsed time, and memory
- REST status, JSON bytes, gzip bytes, and p50/p95/p99
- in-process p50/p95/p99 for matching operations
- Bolt p50/p95/p99 and result counts on the same subset
- correctness equivalence for broad summaries, filtered summaries, child
  summaries, `has_children`, and `where` frontiers

At minimum, gate these operations:

- root `/` Disktree first request
- `/lustre/` and `/nfs/` first clicks
- selected mount-root first click
- repeated visible-child click
- broad and filtered `DirInfo`
- broad and filtered `DirInfos`
- broad and filtered `DirsHaveChildren`
- broad and filtered whole-mount `Where`
- root filter switch
- `/nfs/t283_imaging/` filtered `where`
- real REST/auth tree endpoint
- REST `where` endpoint
- real CLI `./wrstat-ui where`
- browser first visible Disktree update, when harness support exists

Reject statistically consistent p95 or p99 regressions. A local noisy-run
tolerance of up to 10% is acceptable only when medians are already very small;
do not use it to hide cold-provider, filtered `where`, import memory, response
size, or row-amplification regressions.

## Tactical Work

The spec may include tactical metadata and warming tasks, but must label them
as tactical and keep them out of the core success criteria unless paired with
cold-path wins.

Allowed tactical tasks:

- request-scoped active-set metadata so one request resolves active metadata
  once
- batched projection/readiness checks for all active mounts
- startup or provider-update warming for broad `/`, `/lustre/`, `/nfs/`, and
  selected mount-root paths
- response caching keyed by active set, path, filter, permissions inputs, and
  endpoint version

These tasks must not be the final answer. Existing caches explain fast repeats,
but first filter switch and first `where` remain cold-slow.

## Non-Goals And Deferred Fallbacks

The spec must explicitly reject or defer these paths for the first
implementation:

- no cache-warming-only solution
- no broad all-dimension filter index by default
- no standalone `where_frontier` or path-compression table before AgeAll and
  navigation facts are proven
- no first-class `wrstat_virtual_summary_cache` as the initial root fix
- no hybrid Bolt-like sidecar as the initial implementation
- no process-local cache layered over a slow cold path as proof of success
- no hidden fallback to Bolt for production ClickHouse tree routes unless the
  spec reaches the explicit sidecar fallback decision
- no re-litigation of raw-DGUTA or file-facts-only tree summaries

The virtual summary cache remains a deferred ClickHouse fallback. Revisit it
only if active-prefix rollups plus tuple tuning fail many-active-mount gates,
and only if the cache stores enough scalar and filter-aware data to fix the
measured cold paths.

The hybrid sidecar remains a concrete fallback, not a vague future idea. If
AgeAll rows, active-prefix rollups, tuple query tuning, and parent/nav facts
cannot approach the same-subset Bolt p95 targets, the spec must say so clearly
and recommend the least risky Bolt-like navigation sidecar rather than adding
more warming. Any sidecar fallback must preserve active snapshot atomicity,
publish only after ClickHouse data is ready, and include build time, storage,
memory, cleanup, and first-query latency gates.

## Final Spec Expectations

The final spec should be implementation-ready and test-driven. It should not
ask implementors to repeat the whole investigation. The only acceptable
measured design fork is the Disktree navigation choice between parent-ordered
facts/projection and child navigation facts, and that fork must have explicit
evidence, a deadline, and acceptance gates.

The implementation order should be:

1. AgeAll filter rows for cold filters and `where`.
2. Active-set prefix rollups and active-root tuple query tuning.
3. Parent-ordered facts/projection or child navigation facts for Disktree.
4. Harness gaps, correction-pass regression tests, and tactical metadata
   batching/warming as supporting work.

Completion means the cold path is genuinely faster under REST/HTTP, CLI,
browser/tracing where available, and same-subset Bolt comparison gates. It does
not mean the second click is fast after the first one already paid the cost.

## Notes

- Final performance gates must first use the 2026-06-07 four-mount
  100k-lines-per-mount subset and a fixed larger capped subset of the same
  named mounts at 250k lines per mount. Any no-line-cap or full-run expectation
  comes after those gates; full runs may be optional/manual evidence only
  because the investigation warns full `summarise` runs can take hours.
- Active-prefix rollups must store scalar summaries plus narrow AgeAll UID/GID
  and type prefix rows for `/`, `/lustre/`, and `/nfs/`. Do not store full
  filter vectors in v1 unless later evidence proves AgeAll rows insufficient.
- Disktree navigation must keep a short measured decision gate comparing
  physical child facts, physical parent facts, and a ClickHouse projection.
  Select the projection only if real endpoint `EXPLAIN indexes=1` proves
  reliable use; otherwise prefer the simpler physical table that meets gates.
- Browser/Playwright timing must not block this spec's implementation. Require
  REST and CLI/server gates now, define browser timing fields and expected
  manual/CI capture, and leave full Playwright automation as follow-up unless
  easy existing infrastructure makes it low-risk.
