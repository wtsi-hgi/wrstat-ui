# Prompt for a clean ClickHouse schema/query spec

Use this as the source-of-truth input for the spec-writer workflow. It
incorporates the decisions from `.docs/schema/investigation-results.md`; the
spec-writer should turn these decisions into a cohesive feature spec with user
stories, acceptance tests, and implementation phases.

## Goal

Write a clean schema-v1 ClickHouse schema and query spec for `wrstat-ui`.

This ClickHouse work is unreleased branch work. It has not shipped as a
production schema, so the final design must read as if it was built this way
from the start:

- schema version remains `1`
- no branch-local migrations
- no compatibility aliases
- no fallback code for earlier branch schemas
- no backfills for old branch shapes
- no comments that explain or preserve superseded branch-local layouts
- no migration ledger whose only purpose is preserving old branch states

The spec must center on the final ClickHouse-native design described below, not
the original Bolt-shaped raw tree schema and not the current mixed branch
schema.

The final design must preserve:

- atomic snapshot publication per mount
- retry/recovery safety for failed deterministic snapshots
- bounded cleanup of old or failed snapshot data
- web-responsive Disktree and `Where` paths
- correctness for UID, GID, file-type, and age filters
- bounded memory during large imports
- current file-level API behavior
- basedirs usage, subdirectory, and history behavior
- no production dependency on Bolt
- no performance regression from the current branch on the datasets used in
  `.docs/bugfixes`, especially `scratch127`, `scratch122`,
  `/lustre/scratch120`, and `t283_imaging`

## Non-Negotiable Schema Decisions

The spec must define `wrstat_dir_facts` as the canonical tree fact table.

`wrstat_dir_facts` replaces all persisted production uses of:

- `wrstat_dguta`
- `wrstat_dir_summary`
- `wrstat_dir_dguta_vector`

The table has one row per mount directory and must include:

- canonical path-string keys: `mount_path`, `snapshot_id`, and `dir`
- scalar all-age/default-file summary columns for hot broad reads
- aligned per-filter vectors for GID, UID, file type, and age dimensions
- aligned count, size, and time-bucket arrays for those vectors
- `child_count` for child-existence and visible-child paths
- readiness through `wrstat_dir_projection_sets`

Any temporary raw DGUTA accumulator used during import is private
implementation detail. It must not be part of the public schema-v1 contract and
must not be a production read source.

Do not add a general directory-id dictionary. Keep canonical path strings in
hot tree and file tables. Deterministic path hashes may be used only inside a
small virtual hierarchy/cache if the spec proves they help; they must not
replace paths as public tree fact keys.

Do not add a full re-expanded `(gid, uid, file_type, age, dir)` filter index by
default. The only allowed filter index is a narrow, derived AgeAll owner/type
index such as `wrstat_dir_filter_ageall`, and only if final larger-prefix perf
gates prove that `wrstat_dir_facts` cannot meet whole-mount filtered `Where` or
high-fanout filtered `DirsHaveChildren` targets. Do not add GID-only, UID-only,
or full-age index variants without production query evidence.

Use append-only active mount metadata:

- define `wrstat_mount_events` as the canonical history table
- define one final active view named `wrstat_mounts_active`
- use publish/rollback events and inactive tombstone events
- inactive events must win same-millisecond ties
- do not define `wrstat_mounts_active_v2` or compatibility active views

Replace fingerprinted active-tree projections with a smaller virtual-active
design:

- remove `wrstat_tree_dguta` and `wrstat_tree_children` from base schema v1
- define a small active mount hierarchy or virtual children table/view for
  paths above mount roots
- compose virtual ancestor summaries live from active mount-root
  `wrstat_dir_facts` rows first
- add a small virtual-summary cache only if many-active-mount perf gates prove
  live composition is too slow
- avoid `FINAL` in hot virtual ancestor reads

Reject aggregate-state tables for primary directory facts and summaries. Normal
tree reads must not depend on background merges, `OPTIMIZE FINAL`, or
query-time state finalization.

Reject file-facts-only tree summaries. `wrstat_files` remains canonical only
for file-level APIs; hot tree `DirInfo`, `Where`, and Disktree reads must not
be implemented as direct subtree scans of `wrstat_files`.

Keep `wrstat_files.ext` and `ext_idx`. Use exact-safe extension predicates for
simple extension globs while retaining regex matching as the authoritative
check. Preserve dotfile semantics, including bare dotfile exceptions such as
`.bam` matching `*.bam`.

Use row-exists readiness markers. `wrstat_dir_projection_sets` is keyed by
`(mount_path, snapshot_id)` and is written only after `wrstat_dir_facts` and
any selected derived filter index are complete. Do not keep branch-local
summary version columns.

## Required DDL Coverage

The spec must define final schema-v1 DDL for:

- `wrstat_schema_version` and bootstrap behavior
- `wrstat_mount_events`
- `wrstat_mounts_active`
- `wrstat_dir_facts`
- `wrstat_dir_projection_sets`
- the final child edge table used for direct children and child summaries
- the optional `wrstat_dir_filter_ageall`, only if perf gates require it
- the active mount hierarchy or virtual children table/view
- the optional virtual-summary cache, only if perf gates require it
- `wrstat_files`, including `ext` and `ext_idx`
- basedirs, subdirectory, and history tables

For each table or view, specify:

- purpose
- canonical or derived status
- engine, partitioning, primary key, and ordering
- compression and `LowCardinality` choices
- lifecycle, cleanup, and partition deletion behavior
- readiness markers, if any
- writer paths
- reader/query paths

The DDL must not define the removed branch shapes as production objects:

- no persisted `wrstat_dguta`
- no separate `wrstat_dir_summary`
- no separate `wrstat_dir_dguta_vector`
- no full default `wrstat_dir_filter_index`
- no general `wrstat_dirs` dictionary
- no `wrstat_mounts_active_v2`
- no fingerprinted `wrstat_tree_dguta` or `wrstat_tree_children`
- no aggregate-state primary summary tables

## Import And Publication Requirements

The spec must define the import pipeline order for deterministic snapshots:

- stream file rows into `wrstat_files`
- build canonical directory facts directly into `wrstat_dir_facts`
- write the final child edge table
- write any selected derived AgeAll filter index
- write the `wrstat_dir_projection_sets` marker last
- publish active mount events only after the deterministic snapshot is ready

The import design must be bounded in memory and must not rely on post-import
`INSERT ... SELECT` rebuilds for normal facts or selected indexes.

Specify constrained import batch writer unification:

- introduce a shared internal bounded block writer for directory facts,
  child edges, and any selected projection/index writers
- preserve lazy prepare, final partial sends, max-block flushing, no eager
  reprepare after send, and abort cleanup
- preserve `context.WithoutCancel` or equivalent timeout detachment needed by
  clickhouse-go prepared batch send behavior
- keep row construction, validation, phase names, rollback, active snapshot
  checks, and table-specific cleanup explicit
- keep file ingest separate unless tests prove lower-level helper sharing
  preserves column buffering and ambiguous-send behavior
- move basedirs onto shared lifecycle only if tests cover eager prepare,
  history rollback, and snapshot partition cleanup semantics

The spec must also define:

- retry/recovery behavior for failed deterministic snapshots
- cleanup when a previous active snapshot exists
- cleanup when no previous active snapshot exists
- failed/current/old snapshot partition cleanup using cleanup timeout
  semantics
- cache boundaries and invalidation rules for active metadata, projection
  readiness, and any optional virtual-summary cache
- failure reporting for best-effort or asynchronous maintenance work

## Query Routing Requirements

The spec must require all tree summary and tree filter readers to use the clean
schema-v1 routes.

`DirInfo`, `DirInfos`, broad Disktree child summaries, `DirsHaveChildren`,
`Children`, `Where`, `Info`, and permission checks must route through
`wrstat_dir_facts`, the child edge table, and the active virtual hierarchy as
appropriate. They must not fall back to raw DGUTA.

`Where` and high-fanout filtered `DirsHaveChildren` must use
`wrstat_dir_facts` vectors by default. If final perf gates require
`wrstat_dir_filter_ageall`, specify the exact filters it serves, its readiness
dependency, and the fallback to facts when the index is absent or not
semantically applicable. Do not let age-specific queries accidentally read
all-age-only facts.

Virtual ancestor paths above active mount roots must use the active mount
hierarchy/virtual children design and live composition from active mount-root
`wrstat_dir_facts` rows. The optional virtual-summary cache is allowed only
for ancestor directories above mount roots and only with active-generation or
fingerprint readiness.

File-level routes must preserve current behavior:

- `ListDir`
- `StatPath`
- `IsDir`
- `FindByGlob`
- file-level permission checks
- basedirs usage, subdirectory, and history behavior

`FindByGlob` may add `ext` predicates only for simple, exact-safe extension
cases such as `*.bam` and `**/*.bam`; regex matching remains the authority and
dotfile exceptions must stay correct.

## Clean-Branch Cleanup Requirements

The spec must require code and DDL cleanup, not migrations:

- fold final columns into final `CREATE TABLE` statements
- remove branch-local ALTER files such as active-column, child-count,
  summary-version, and file-column additions
- remove compatibility active-view aliases
- remove test-only backfill/projection helpers from production code unless the
  final design genuinely uses them
- remove raw-DGUTA routing and raw-DGUTA fallback tests
- remove branch-local summary-version columns and checks
- keep schema version `1`
- keep comments focused on the final design, not on old branch states

## Acceptance Tests

The spec must require focused tests for:

- fresh schema-v1 bootstrap under concurrent constructors
- absence of branch-local compatibility DDL, aliases, backfills, version
  columns, and old-layout comments
- `wrstat_dir_facts` row cardinality: one row per mount directory
- scalar all-age/default-file columns matching expected summaries
- aligned vector and bucket-array lengths and values
- `child_count` correctness
- `wrstat_dir_projection_sets` row-exists readiness misses and hits
- marker rows written only after facts and selected indexes are complete
- no age-specific query reading compacted all-age-only facts
- no raw-DGUTA fallback routing for `DirInfo`, `DirInfos`,
  `DirsHaveChildren`, `Where`, `Info`, or permission checks
- active view tie ordering where inactive wins same-millisecond ties
- append-only active history preserving rollback and audit data
- retry cleanup preserving previous active snapshots when possible
- retry cleanup when no previous active row exists
- failed/current/old snapshot partition cleanup using cleanup timeout
  semantics
- active virtual hierarchy children and ancestor summaries
- optional virtual-summary cache readiness, if the cache is selected
- optional `wrstat_dir_filter_ageall` routing and readiness, if selected
- bounded import memory for direct `wrstat_dir_facts` writing
- unified batch writer lifecycle, timeout detachment, final partial sends,
  abort cleanup, and ambiguous-send behavior
- file glob extension optimization with and without owner filtering
- dotfile extension semantics
- basedirs usage, subdirectory, and history correctness

## Performance Gates

The spec must require final perf runs on representative production-like
prefixes, especially `scratch120`, `scratch122`, `scratch127`, and
`t283_imaging`, using the found dataset roots under `/home/ubuntu/output`.

Use the current branch measurements in
`.docs/schema/investigation-results.md` as regression guards. At minimum, the
spec must set gates for:

- t283-shaped directory-heavy import row amplification and phase timings
- bounded import memory for the folded `wrstat_dir_facts` writer
- Disktree cold-provider, warm-provider, and provider-update timings
- unfiltered `Where`
- filtered `Where` with common and selective UID/GID/type/age filters
- broad and filtered `DirInfo` and `DirInfos`
- high-fanout broad and filtered `DirsHaveChildren`
- virtual ancestor children and summaries with many active mounts
- file list/stat/glob behavior, including extension globs and owner filters
- optional `wrstat_dir_filter_ageall` row count, compressed bytes, and import
  phase cost if the index is selected
- optional virtual-summary cache row count, compressed bytes, refresh time, and
  read latency if the cache is selected

The perf gates must reject regressions against the measured branch baseline.
Important baseline anchors from the investigation include:

- t283 100k import: 4.35s import report, 4.39s wall, 402,892 KB max RSS
- t283 100k no-ancestor `tree_where`: p50/p95/p99 4/8/8 ms
- t283 100k cold-provider `tree_where`: p50/p95/p99
  102.558/115.235/118.260 ms
- t283 100k filtered cold-provider `tree_where`: p50/p95/p99
  84.837/102.892/103.945 ms
- Disktree unfiltered: p50/p95/p99 63.418/103.636/109.406 ms
- Disktree filtered: p50/p95/p99 67.983/90.216/124.348 ms
- visible-child warm path: p50/p95/p99 0.008/0.012/0.017 ms
- file list/stat/glob focused runs: p50 roughly 6-13 ms

The spec must state that facts-only routing is the default. The narrow AgeAll
index may be added only if the final larger-prefix gates show facts-only
filtered `Where` or filtered `DirsHaveChildren` cannot meet the documented
targets. The virtual-summary cache may be added only if many-active-mount gates
show live composition cannot meet the documented targets.

## `clickhouse-perf` Improvements Required

The spec must include implementation work to make `clickhouse-perf` sufficient
for these gates:

- require or clearly surface ClickHouse DSN and database before query/import
  runs begin
- record table row counts, active parts, compressed bytes, uncompressed bytes,
  and selected table list after each import
- record reliable ClickHouse read rows, read bytes, and read marks in JSON
  query reports, using profile events when `system.query_log` is unavailable
- record directory fact vector statistics: rows, total entries, average entries
  per directory, maximum entries per directory, and bucket-array sizes
- add focused ops for broad and filtered `DirInfo`, `DirInfos`,
  `DirsHaveChildren`, whole-mount `Where`, virtual ancestor children and
  summaries, and extension glob dotfile cases
- report import phase metrics for every selected table, including optional
  filter indexes and optional virtual-summary caches
- preserve bounded memory reporting, preferably max RSS from `/usr/bin/time -v`
  or equivalent process metrics

The final spec should be implementation-ready and test-driven. It should not
ask the implementor to re-litigate the schema shape, except for the two
explicit perf-gated additions: the narrow AgeAll filter index and the small
virtual-summary cache.

## Notes

The base schema excludes both `wrstat_dir_filter_ageall` and the
virtual-summary cache. The spec must still define conditional DDL, tests, and
phase steps for each perf-gated addition. Implementation must first build
facts-only filtering and live virtual-summary composition, run larger-prefix
perf gates, and add only the minimal optional object needed when a gate fails.
Any selected optional object is part of clean schema v1, not a migration,
compatibility layer, or branch-local fallback.

The active virtual hierarchy must use a persisted, small derived
`wrstat_virtual_children` table maintained when active mount events publish or
tombstone snapshots. It represents only virtual paths above mount roots and
maps each virtual parent to visible child names/paths and whether the child is
a mount root or another virtual directory. It is derived from
`wrstat_mounts_active`, and readers may deterministically rebuild or refresh
it. Virtual ancestor summaries must compose live from active mount-root
`wrstat_dir_facts` rows by default. Add a virtual-summary cache only if the
many-active-mount perf gate fails.

Preserve the API-facing meaning of `db.Info.NumDGUTAs` by counting active
DGUTA detail entries represented inside `wrstat_dir_facts` vectors, not by
counting directory rows. Keep the existing `NumDGUTAs` field unless a broader
API rename is already underway. The ClickHouse implementation must compute the
value from vector entry cardinality after raw DGUTA removal.

Perf gates should use repeated larger-prefix runs where practical. Reject
statistically consistent p95 or p99 latency regressions, and reject any clear
correctness or memory regression. For bounded noisy local runs, allow up to
10% latency tolerance when medians are already very small, but do not use that
tolerance to hide a repeated cold-provider, filtered `Where`, import memory, or
row-amplification regression.
