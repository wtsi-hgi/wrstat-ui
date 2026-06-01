# ClickHouse Schema Investigation Results

## Executive Recommendation

Build a clean schema v1 around a single vector-first directory fact table,
tentatively named `wrstat_dir_facts`, with one row per mount directory. This
table should replace raw `wrstat_dguta`, `wrstat_dir_summary`, and
`wrstat_dir_dguta_vector` as the canonical tree fact source. It should contain
the scalar all-age/default-file summary columns needed by hot broad reads, the
aligned per-GID/UID/file-type/age vectors needed by filtered reads, and
`child_count` for child-existence paths.

Do not preserve branch-local compatibility shapes. The branch has not shipped a
ClickHouse schema, so schema v1 should be authored as if this design had been
chosen first: no `wrstat_mounts_active_v2`, no summary-version compatibility,
no raw-DGUTA fallback routing, and no migration ledger for older branch states.

Recommended high-level schema direction:

- Canonical tree facts: `wrstat_dir_facts` only, populated directly during
  import in bounded batches.
- Raw DGUTA: remove as a persisted production read source. Any temporary import
  accumulator must not be part of the public schema v1 contract.
- `wrstat_dir_summary`: fold its scalar hot columns into `wrstat_dir_facts`.
- `wrstat_dir_dguta_vector`: replace with `wrstat_dir_facts`.
- Filter indexes: reject the full re-expanded all-age filter index as a default
  table. Add only a narrow, perf-gated index such as
  `wrstat_dir_filter_ageall` if final larger-prefix runs prove whole-mount
  filtered `where` or high-fanout filtered `DirsHaveChildren` cannot meet the
  perf gate from `wrstat_dir_facts`.
- Directory IDs: do not rewrite hot tree/file tables to integer ids. Keep exact
  path strings as canonical keys. Use deterministic path hashes only if a small
  virtual hierarchy table benefits from them.
- Active mount metadata: replace the current `ReplacingMergeTree` active rows
  with append-only `wrstat_mount_events` plus one final active view named
  `wrstat_mounts_active`.
- Active-tree/virtual ancestors: remove or shrink fingerprinted `wrstat_tree_*`
  projections. Prefer a tiny active mount hierarchy and live composition from
  active mount-root facts. Keep one small virtual-summary cache only if final
  perf runs show live composition is too slow.
- Aggregate-state tables: reject for primary directory facts and summaries.
- File facts only: reject as the tree-summary source.
- `ext_idx`: keep it, and use exact-safe `ext` predicates only for simple
  extension globs while retaining regex matching as the authoritative check.
- Batch writers: unify repeated import batch lifecycle mechanics for directory
  fact, child, and projection/index writers, while keeping table-specific
  rollback and ambiguous-send behavior explicit.
- Readiness/version metadata: use explicit projection-set marker rows written
  after all derived outputs for a mount/snapshot have succeeded. Drop
  branch-local summary version columns in schema v1.

## Evidence Scope And Limitations

This report synthesizes the worker artifacts under `.tmp/agent/` and should be
used as input to the final schema spec. It is not full-production proof.

Important constraints:

- The requested production-like dataset roots `~/outputs/lustre` and
  `~/outputs/nfs` were absent. Workers found the datasets under
  `/home/ubuntu/output/lustre` and `/home/ubuntu/output/nfs`.
- `clickhouse-perf` requires an explicit ClickHouse DSN/database in this
  workspace. A default query failed with `Error: clickhouse DSN required`.
  Baseline runs used `-C clickhouse://...` and `-D <database>`.
- Most current-design imports were bounded to 100k input lines. Other existing
  local databases used for file/extension checks had 1m to 6m rows, but those
  were not full fresh imports for every candidate.
- The t283 and scratch127 prefixes had low owner diversity, so filter-index
  selectivity conclusions are directional rather than final.
- `clickhouse-perf` JSON did not expose useful ClickHouse read rows/bytes for
  several query runs. Some workers used `EXPLAIN indexes=1`,
  `clickhouse-client --time`, `clickhouse-benchmark`, or profile events instead.
- Some prototypes used synthetic or bounded shapes, for example `cityHash64`
  directory ids and a full filter index populated from `ARRAY JOIN`. These are
  evidence for tradeoffs, not production DDL.

## Comparison Matrix

| Design name | Tables added/removed | Canonical tree fact source | Import rows per representative dataset | Import wall time | Memory observations | ClickHouse parts/bytes if measured | Unfiltered `where` p50/p95/p99 | Filtered `where` p50/p95/p99 | Disktree cold-provider p50/p95/p99 | Disktree warm/visible-child p50/p95/p99 | File glob/list/stat impact | Complexity notes | Recommendation |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---|---|---|
| Current branch baseline | Existing `wrstat_dguta`, `wrstat_children`, `wrstat_dir_summary`, `wrstat_dir_dguta_vector`, `wrstat_files`, `wrstat_tree_*`, `wrstat_mounts_active_v2` | Mixed: summaries/vectors first, raw DGUTA fallback | t283 100k: files 100,000; DGUTA 70,520; children 35,005; dir summary 35,048; vector 35,006; active tree DGUTA 468 | t283 100k 4.35s import report, 4.39s wall | `/usr/bin/time -v` max RSS 402,892 KB | Baseline `system.parts` captured in `.tmp/agent/baseline/system-parts.tsv` | Baseline no-ancestor `tree_where`: 4/8/8 ms; cold-provider `tree_where`: 102.558/115.235/118.260 ms | Baseline filtered cold-provider `tree_where`: 84.837/102.892/103.945 ms; vector worker filtered `tree_where` sample: t283 442/454/454 ms, scratch127 15/16/16 ms | Baseline unfiltered 63.418/103.636/109.406 ms; filtered 67.983/90.216/124.348 ms | Baseline visible-child 0.008/0.012/0.017 ms; warm endpoint 0/0/0 ms | List/stat/glob p50 roughly 6-13 ms in t283 100k focused runs | Too many canonical/fallback paths; active-tree projections and raw DGUTA keep old design centered | Use only as baseline. Do not preserve as schema v1. |
| Vector-first directory facts | Replace `wrstat_dguta`, `wrstat_dir_summary`, `wrstat_dir_dguta_vector` with `wrstat_dir_facts`; keep `wrstat_children` or final child edge table | `wrstat_dir_facts` one row per dir with scalar hot columns and aligned vectors | Evidence from existing vector rows: t283 100k vector rows 35,006 with 701,282 vector entries; scratch127 100k vector rows 1,145 with 10,695 entries | Current projection phase was costly: t283 `wrstat_dir_projection_insert` 2.652-2.693s of 4.3s import; scratch127 0.180s | Not isolated from current import; baseline total RSS 402,892 KB | t283 vector table 4.15 MiB; scratch127 vector table 97.25 KiB | Direct SQL age-specific common-owner vector `ARRAY JOIN`: 0.053s; selective-owner vector: 0.020s | Same direct SQL; needs final perf harness routing | Expected to keep exact lookup close to current because key remains path | Expected to preserve warm child paths through scalar columns plus child table | No direct file impact | Removes raw fallback routing if active-tree, Info, and permission checks are ported | Adopt as canonical schema v1 direction. |
| Full inverted filter index prototype | Add `wrstat_dir_filter_index` re-expanded by gid, uid, ft, age, dir | Derived from vector facts | t283 100k: 701,282 index rows; scratch127 100k: 10,695 rows | Populated after import for prototype, not measured as stream writer | Not measured separately | t283 index 7.00 MiB; scratch127 index 132 KiB | AgeAll common t283 raw 0.133s vs full index 0.140s; scratch127 raw 0.048s vs index 0.013s | Age-specific selective t283 index 0.007s vs vector 0.020s; common t283 index 0.065s vs vector 0.053s | Rare filtered child join prototype 0.016s | Not measured | No file impact | Helps only selective filters; full-age shape expands rows and bytes | Reject as default schema v1 table. Keep only targeted index option. |
| Narrow AgeAll filter index | Add possible `wrstat_dir_filter_ageall` ordered by `(mount_path, snapshot_id, gid, uid, ft, dir)` | Derived from `wrstat_dir_facts` | Not directly measured; expected smaller than full-age index | Not measured | Not measured | Not measured | Not measured | Intended for whole-mount AgeAll filtered `where` and broad filtered child checks | Not measured | Not measured | No file impact | Needs final larger-prefix proof and direct stream-write measurement | Perf-gated optional. Add only if facts alone fail final gates. |
| Directory id rewrite | Add directory dictionary and id-keyed DGUTA/children/summary/vector prototypes | Same logical facts, keyed by ids | Bounded database: 68,021 dirs; id tables mirrored current row counts | NFS 100k import 4.191s; Lustre bounded 600,001 records across seven mounts 9.939s | Dictionary dedupe memory risk not fully measured | Compressed bytes got worse: vector 9.52 MiB current vs 11.43 MiB id proto; dictionary added 3.67 MiB | Exact summary lookup current 3/7 ms vs id proto 3/7 ms by benchmark | Not materially improved | Child lookup current 3/6 ms vs id proto 3/6 ms | Not improved | Would complicate file prefix/range glob paths | Collision handling, dictionary dedupe, joins, and API path returns add complexity | Reject for hot tables. Keep path strings canonical. |
| Virtual hierarchy instead of fingerprinted tree tables | Replace/shrink `wrstat_tree_children`; maybe one tiny virtual child table and optional virtual summary cache | Mount-root rows from `wrstat_dir_facts`, composed through active mounts | Bounded active mounts after import: 8 | Tree refresh grew from about 129ms to 346ms as active mount count increased in bounded Lustre run | Not measured | Prototype virtual tables small; exact parts not reported | Root active summary current tree 12/25 ms vs virtual summary 5/9 ms | Filtered ancestor production cardinality not measured | Root virtual children current tree 10/18 ms vs hierarchy 4/7 ms | Ancestor endpoint baseline 9.655/57.221 ms unfiltered; 19.274/118.506 ms for GID 1105 | No file impact | Avoids `FINAL` and historical fingerprint accumulation | Replace active-tree children with hierarchy; compose summaries live first, cache only if needed. |
| Append-only active mount events | Replace `wrstat_mounts`/`wrstat_mounts_active_v2` with `wrstat_mount_events` plus `wrstat_mounts_active` view | Not a tree fact source | Prototype inserted 1,000,002 event rows for 10,001 mounts/history events | Active count query 0.079s; point lookup 0.006s | Not measured | Query read 42.9 MB for 1m history active count; point lookup read one granule | n/a | n/a | n/a | n/a | n/a | Preserves rollback history and deterministic tie semantics | Adopt. |
| Aggregate-state summary tables | Add `AggregatingMergeTree` summary states and materialized views | Derived state, not final facts | 1m-row synthetic insert produced 30k state rows; ten 100k inserts produced 150k state rows before merges | Final merge query 0.012s after single insert; 0.022s after ten batches | Not measured | Query-time state rows scale with insert batches until merges | n/a | n/a | n/a | n/a | n/a | Readiness is ambiguous after partial inserts; depends on background merge or query-time finalization | Reject for primary schema v1 summaries/facts. |
| File-facts-only tree summaries | Remove tree facts and derive from `wrstat_files` or exploded ancestors | `wrstat_files` only | Existing DB estimates: 1.67m files -> 16.36m ancestor rows; 6.0m files -> 65.99m ancestor rows | Not a viable import; row explosion 9x to 60x | Would increase import memory/IO risk | Direct scan reads hundreds of MB for root summaries | File scan root summary 0.086-0.094s vs maintained lookup 0.014-0.037s | Not viable for filtered hot tree paths | n/a | n/a | File table remains canonical for file APIs | Makes hot tree reads proportional to subtree file count | Reject. |
| Extension glob optimization | Keep `wrstat_files.ext` and `ext_idx`; add exact-safe query predicates | No tree fact impact | Existing file rows only | n/a | n/a | `ext_idx` storage tiny: 1.35 KiB to 40.98 KiB in measured DBs | n/a | n/a | n/a | n/a | Recursive `**/*.bam`: regex 0.328s vs `ext`+regex 0.196s or PREWHERE 0.063s in examples | Must preserve dotfile and case semantics with regex as authority | Keep `ext_idx`; use narrowly. Consider granularity 1. |
| Batch writer unification | Add internal common lifecycle helper, no schema table | n/a | n/a | No overhead measurement | n/a | n/a | n/a | n/a | n/a | n/a | No query impact | Useful only if table-specific failure semantics stay visible | Include constrained implementation work in final spec. |

## Checklist Results

### 1. Baseline Current Design

Conclusion: the current branch is a useful baseline but should not be carried
forward as the schema-v1 shape.

Evidence:

- Bounded t283 import of 100k rows took 4.350s by import report and 4.39s wall
  time, with max RSS 402,892 KB.
- Rows after t283 100k import: `wrstat_files=100000`, `wrstat_dguta=70520`,
  `wrstat_children=35005`, `wrstat_dir_summary=35048`,
  `wrstat_dir_dguta_vector=35006`, `wrstat_tree_dguta=468`,
  `wrstat_tree_children=4`, `wrstat_tree_dir_summary=45`.
- `wrstat_dir_projection_insert` dominated the t283 bounded import at about
  2.7s of 4.35s.
- Query routing still has layered fallbacks: vector, maintained summary, raw
  DGUTA, active-tree projections, and virtual ancestor synthesis.
- A default all-ops query hit an ancestor click-through failure in the baseline
  run: `tree_disktree_endpoint_ancestor_dirs repeat 2/20: directory not found`.

Spec decision:

- Use baseline numbers only for perf gates and regression checks.
- Remove raw-DGUTA and compatibility fallback routing from schema v1.
- Require final spec tests for cold-provider, warm-provider, provider-update,
  filtered/unfiltered `where`, Disktree, list/stat/glob, and ancestor paths.

### 2. Vector-First Directory Facts

Conclusion: promote the vector shape to the canonical mount-tree fact table.

Evidence:

- Current vector rows already have the desired cardinality: one row per
  directory. In t283 100k, 35,006 vector rows represented 701,282 detail
  entries; in scratch127 100k, 1,145 vector rows represented 10,695 entries.
- Scalar maintained summary rows and vector rows are currently separate, which
  causes extra routing and readiness complexity.
- Direct SQL showed vector `ARRAY JOIN` can be competitive for common filters:
  t283 age-specific common owner 0.053s via vector vs 0.065s via full filter
  index. For selective owners, vector was 0.020s vs 0.007s via index.
- Vector-first routing can cover `DirInfo`, `DirInfos`, broad Disktree reads,
  child summaries, permission checks, and `Info` without raw DGUTA.

Spec decision:

- Define `wrstat_dir_facts` with `(mount_path, snapshot_id, dir)` ordering,
  scalar all-age/default-file columns, aligned arrays for gid/uid/ft/age/counts
  and size/time buckets, `child_count`, and a projection-set readiness marker.
- Route ordinary mount `DirInfo`, `DirInfos`, Disktree child summaries,
  `DirsHaveChildren`, `Where`, `Info`, and permission checks through
  `wrstat_dir_facts` plus the child edge table.
- Build active virtual ancestor summaries from mount-root `wrstat_dir_facts`
  rows, not raw DGUTA.

### 3. Inverted Filter Index Tables

Conclusion: do not add a full re-expanded filter index by default. Keep a
targeted AgeAll index as a perf-gated option.

Evidence:

- The full prototype `wrstat_dir_filter_index` expanded t283 100k to 701,282
  rows and 7.00 MiB, compared with 35,006 vector rows and 4.15 MiB.
- On t283 AgeAll common-owner summary, raw compact DGUTA was 0.133s and the
  full filter index was 0.140s.
- On scratch127 AgeAll, the index was faster, 0.013s vs raw 0.048s.
- On t283 age-specific selective owner, the index won, 0.007s vs vector 0.020s.
- The measured prefixes have low owner diversity, so the selective-filter win
  is not enough proof to pay full-age storage/import cost for all mounts.

Spec decision:

- Omit a full `(gid, uid, ft, age, dir)` re-expanded table from base schema v1.
- Add a final pre-implementation perf gate: if whole-mount filtered `where` or
  broad filtered `DirsHaveChildren` cannot meet thresholds from
  `wrstat_dir_facts`, add one narrow derived table for AgeAll owner/type
  filtering, for example `wrstat_dir_filter_ageall`.
- Do not add separate GID-only, UID-only, or full-age indexes without production
  query evidence.
- If an index is selected, populate it directly during import with bounded
  batches and a readiness marker; do not use post-import `INSERT ... SELECT`
  rebuilds.

### 4. Directory ID Dictionary

Conclusion: reject a wholesale directory-id rewrite for hot tables.

Evidence:

- Exact child and summary lookup benchmarks did not improve: current and id
  prototype were both about p50 3ms, p95 6-7ms.
- Compressed storage worsened because paths compress well and random ids do
  not. Example: `wrstat_dir_dguta_vector` 9.52 MiB current vs 11.43 MiB id
  prototype, plus a 3.67 MiB dictionary.
- Streaming id generation is feasible, but dictionary dedupe, collision
  handling, and API path reconstruction add complexity.
- File APIs benefit from current `(parent_dir, name)` ordering for list, stat,
  and glob. Replacing file parent paths with ids would make prefix/range paths
  harder unless another path table stays in the read path.

Spec decision:

- Keep path strings as canonical keys in `wrstat_dir_facts`, child edges, and
  `wrstat_files`.
- Do not add a general `wrstat_dirs` dictionary to schema v1.
- If virtual hierarchy code uses ids internally, define deterministic ids from
  canonical full paths, store the full hash/path, and test collision detection;
  do not make those ids the public tree fact key.

### 5. Active Mount Metadata

Conclusion: use append-only events, not `ReplacingMergeTree`, for active mount
metadata.

Evidence:

- Current `ReplacingMergeTree(switched_at)` can discard previous active rows
  after merges, which is unsafe for rollback and retry cleanup.
- Equal-version rows can resolve by insertion order after merge, while required
  semantics are that inactive tombstones win same-millisecond ties.
- A prototype event table with 1,000,002 rows for 10,001 mounts/history events
  queried the active count in 0.079s and point lookup in 0.006s.
- Append-only history keeps previous active rows available for cleanup, audit,
  rollback, and retry.

Spec decision:

- Define `wrstat_mount_events` as append-only `MergeTree`.
- Define one final active view named `wrstat_mounts_active`.
- Use `event_type=1` for publish/rollback and `event_type=0` for tombstone.
- Ensure the view orders ties so inactive events win at the same millisecond.
- Remove branch-local `wrstat_mounts_active_v2` compatibility naming.

### 6. Virtual Ancestors And Active-Tree Tables

Conclusion: shrink or remove fingerprinted active-tree projections. Prefer
active mount hierarchy plus live composition from mount-root facts.

Evidence:

- Current `wrstat_tree_*` projections use fingerprinted copies and hot reads use
  `FINAL`; old fingerprints can accumulate.
- In the bounded dirid/ancestor run, root active summary benchmark improved
  from current tree p50/p95 12/25 ms to virtual summary 5/9 ms.
- Root virtual children improved from current tree p50/p95 10/18 ms to
  hierarchy 4/7 ms.
- Tree refresh cost grew with active mount count in bounded Lustre imports,
  from about 129ms to 346ms.

Spec decision:

- Remove `wrstat_tree_dguta` and `wrstat_tree_children` from base schema v1.
- Add a small active mount hierarchy or virtual children table/view for paths
  above mount roots.
- Compose virtual ancestor summaries from active mount-root `wrstat_dir_facts`
  rows first.
- If final perf with many active mounts fails, add a small virtual-summary cache
  containing only ancestor directories above mount roots, keyed by active
  generation/fingerprint and published by a readiness marker.
- Avoid `FINAL` in hot virtual ancestor reads.

### 7. ClickHouse Aggregate-State Tables

Conclusion: reject aggregate-state tables for primary maintained directory
facts in schema v1.

Evidence:

- A small `AggregatingMergeTree` prototype produced correct merged summaries.
- Readiness became ambiguous: rows existed after the first partial insert,
  before the snapshot was complete.
- Ten 100k-row inserts produced 150k state rows for 30k final keys before
  background merging; query-time finalization then read all state rows.
- Directory vector output and `child_count` depend on import-time state across
  multiple sources, not one simple raw table.

Spec decision:

- Keep Go-maintained final directory fact rows and explicit readiness markers.
- Do not depend on background merges, `OPTIMIZE FINAL`, or query-time
  `...Merge` aggregation for normal tree reads.
- Reconsider aggregate-state only for future side indexes with separate
  import-complete readiness.

### 8. File-Facts-Only Challenge

Conclusion: reject deriving tree summaries directly from `wrstat_files`.

Evidence:

- Direct root subtree file scans read hundreds of MB and were slower than
  maintained summaries: 0.086-0.094s for file scans vs 0.014-0.037s for
  maintained root lookup in measured databases.
- Exploding each file to ancestor rows caused large amplification:
  1,668,686 rows became 16,361,663 ancestor rows; 6,000,001 rows became
  65,990,754 ancestor rows.
- Filtered tree queries need owner, file type, age, and bucket dimensions, which
  would make exploded rows wide as well as numerous.

Spec decision:

- Keep `wrstat_files` as the canonical file-level table only.
- Keep tree summaries/facts as import-produced derived data.
- Do not implement hot tree `DirInfo`, `Where`, or Disktree reads as direct
  `wrstat_files` subtree scans.

### 9. File Extension Index

Conclusion: keep `ext` and `ext_idx`; use them only when semantically exact.

Evidence:

- `ext_idx` storage was tiny in measured databases: 1.35 KiB to 40.98 KiB.
- `EXPLAIN indexes=1` confirmed the skip index is used only with an explicit
  `ext` predicate.
- Recursive `**/*.bam` examples improved from regex-only 0.328s to
  `ext`+regex 0.196s, and to PREWHERE 0.063s in one measured case.
- A simple `f.ext='bam'` predicate is not always exact because bare dotfiles
  such as `.bam` have empty `ext` but match `*.bam` under current semantics.

Spec decision:

- Keep `wrstat_files.ext` and `ext_idx`.
- Consider changing skip-index granularity to 1 because storage is negligible.
- For simple extension globs like `*.bam` and `**/*.bam`, add an exact-safe
  optimization branch using lowercased `ext` plus the original regex.
- Preserve dotfile semantics with a separate exception branch such as
  `name = '.bam'`; avoid a single broad `OR` that defeats skip-index pruning.
- Do not use `ext` predicates for mixed or complex glob chunks where suffix
  extraction is not exact.

### 10. Import Batch Writer Unification

Conclusion: include a constrained batch-writer cleanup in the final schema work.

Evidence:

- DGUTA, children, and projection writers duplicate lifecycle mechanics:
  prepare lazily, send when full or too old, avoid immediate reprepare after
  send, flush final partial batches, and abort pending batches during cleanup.
- `prepareImportBatch` intentionally strips normal query cancellation with
  `context.WithoutCancel` because clickhouse-go reuses the prepare context
  during `Send`.
- File ingest has important extra behavior: column buffering and an ambiguous
  `sendErr` guard so `Close` does not retry a possibly-sent batch.
- Basedirs has eager prepare, history rollback, and snapshot partition cleanup
  behavior that should not be hidden in a generic helper without tests.

Spec decision:

- Add an internal bounded block writer for shared lifecycle mechanics in the
  directory fact, child, and selected projection/index writers.
- Keep row construction, validation, phase names, cleanup, active snapshot
  checks, and rollback behavior table-specific.
- Leave file ingest separate except for sharing lower-level prepare/send/abort
  helpers if tests prove behavior is unchanged.
- Only move basedirs onto the common lifecycle if the final tests cover its
  eager prepare and rollback semantics.

### 11. Projection Readiness And Version Metadata

Conclusion: in clean schema v1, projection readiness should be an explicit
row-exists marker per mount/snapshot, written last. Branch-local version
columns are not needed.

Evidence:

- Current summary/vector readiness already relies on writing projection outputs
  before the set marker.
- Retry reset drops the summary-set, summary, and vector partitions before
  deterministic snapshot rewrites.
- A marker row written after all derived outputs protects readers from partial
  writes as long as readers check it before using derived tables.
- The global schema version already protects incompatible database shapes; a
  branch-local `summary_version=4` does not add atomicity in schema v1.
- Active-tree projections, if any remain, still need fingerprint/generation
  readiness because they depend on the active mount set rather than one
  mount/snapshot.

Spec decision:

- Define `wrstat_dir_projection_sets` keyed by `(mount_path, snapshot_id)`,
  with `updated_at` and `refreshed_at`, and write one marker after
  `wrstat_dir_facts` and any selected filter indexes are complete.
- Remove summary-version columns from clean schema v1.
- If a virtual-summary cache remains, key its readiness by active generation or
  fingerprint and verify data existence to avoid empty published caches.

## Commands And Datasets Used

Representative command shapes from the worker artifacts:

```bash
timeout 300s env CGO_ENABLED=1 go build -o .tmp/agent/wrstat-ui .

timeout 1h .tmp/agent/wrstat-ui clickhouse-perf \
  -C '<dsn>' -D wrstat_baseline_t283_20260601_132727 \
  --mounts .tmp/agent/baseline/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/baseline/t283-100k-import.json \
  import /home/ubuntu/output/nfs --maxLines 100000 --batchSize 100000

timeout 10m .tmp/agent/wrstat-ui clickhouse-perf \
  -C '<dsn>' -D wrstat_baseline_t283_20260601_132727 \
  --mounts .tmp/agent/baseline/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/baseline/t283-100k-query-no-ancestor.json \
  query --repeat 20 --warmup 1 --ancestor-limit 0

timeout 10m .tmp/agent/wrstat-ui clickhouse-perf \
  -C '<dsn>' -D wrstat_baseline_t283_20260601_132727 \
  --mounts .tmp/agent/baseline/mounts.txt \
  --query-timeout 30s \
  --json .tmp/agent/baseline/t283-100k-query-filtered.json \
  query --repeat 20 --warmup 1 --ancestor-limit 0 \
  --tree-gids 14976 --tree-uids 20155 --tree-types other
```

Other command families used:

- `clickhouse client --host 127.0.0.1 --port 9000 --query 'SELECT 1'`
- `clickhouse-client --database <db> --multiquery < prototype.sql`
- `EXPLAIN indexes=1` for raw, vector-array, and filter-index query shapes.
- `clickhouse client --time` and `clickhouse-benchmark` for direct SQL checks.
- `clickhouse-client --print-profile-events --profile-events-delay-ms -1` for
  file/glob read rows and bytes where `system.query_log` was unavailable.

Datasets and databases referenced:

- Requested but absent: `/home/ubuntu/outputs/lustre`,
  `/home/ubuntu/outputs/nfs`.
- Found production-like roots: `/home/ubuntu/output/lustre`,
  `/home/ubuntu/output/nfs`.
- Representative `stats.gz` files found under `/home/ubuntu/output`:
  `scratch120`, `scratch122`, `scratch127`, and `t283_imaging`.
- Bounded fresh imports:
  - `t283_imaging`, 100k rows, from `/home/ubuntu/output/nfs`.
  - `scratch127`, 100k rows, from `/home/ubuntu/output/lustre`.
  - A bounded dirid/ancestor database with NFS 100k plus Lustre 600,001
    records across seven mount imports.
- Existing local databases used for file-facts and extension measurements:
  `wrstat_vector_raw1668`, `wrstat_rss_s120`, `wrstat_bench_2m_current`,
  and `wrstat_speed_f0e4a73`.

## Remaining Risks And Blockers

- Final schema work still needs a stronger perf proof on larger prefixes,
  especially `scratch120`, `scratch122`, `scratch127`, and `t283_imaging`.
- Filter index selection remains the largest open schema risk. The evidence
  rejects a full default index, but the final implementation should not remove
  the option for a narrow AgeAll index until larger filtered root runs pass.
- Virtual ancestor summary composition needs production-like active mount count
  evidence before all active-tree summary caching is removed.
- Import memory for a direct `wrstat_dir_facts` writer must be measured after
  the table is folded into one writer. Current measurements include the branch
  projection writer, not final code.
- `clickhouse-perf` currently makes schema investigation harder because JSON
  reports lack reliable read rows/bytes, table bytes/parts, vector length
  stats, and per-table import row counts for new candidate tables.

## Decisions To Copy Into `.docs/schema/prompt.md`

- Schema v1 should define `wrstat_dir_facts` as the canonical tree fact table
  and remove persisted raw `wrstat_dguta`, separate `wrstat_dir_summary`, and
  separate `wrstat_dir_dguta_vector`.
- `wrstat_dir_facts` must include scalar all-age/default-file summary columns,
  aligned filter vectors, bucket arrays, `child_count`, and direct readiness via
  `wrstat_dir_projection_sets`.
- Query routing for `DirInfo`, `DirInfos`, `DirsHaveChildren`, `Where`,
  `Info`, and permission checks must not fall back to raw DGUTA.
- Do not add a full re-expanded filter index by default. Add only a narrow
  AgeAll owner/type index if final larger-prefix perf gates prove it is needed.
- Keep path strings as canonical keys. Do not add a general directory id
  dictionary for hot tree or file APIs.
- Replace active mount metadata with append-only `wrstat_mount_events` and one
  final active view named `wrstat_mounts_active`; inactive events must win
  same-millisecond ties.
- Replace fingerprinted active-tree child projections with a small active mount
  hierarchy or virtual children table/view. Compose ancestor summaries from
  active mount-root facts first; add a small virtual-summary cache only if perf
  gates require it.
- Reject aggregate-state tables for primary directory facts/summaries.
- Reject file-facts-only tree summaries; `wrstat_files` remains canonical only
  for file-level APIs.
- Keep `wrstat_files.ext` and `ext_idx`; add exact-safe extension predicates
  for simple globs while preserving regex semantics and dotfile exceptions.
- Use row-exists projection-set readiness for mount/snapshot derived outputs.
  Do not keep branch-local summary version columns in schema v1.
- Unify import batch lifecycle mechanics only where tests preserve timeout
  detachment, final partial sends, no eager reprepare, abort cleanup, and
  ambiguous-send behavior.
- Keep schema version 1 and remove branch-local compatibility DDL, aliases,
  backfills, and comments.

## `clickhouse-perf` Improvements Needed Before Final Implementation

- Require or clearly surface the ClickHouse DSN and database before starting
  query/import runs; the default failure is currently easy to miss.
- Record table row counts, active parts, compressed/uncompressed bytes, and
  selected table list after each import.
- Add reliable ClickHouse read rows/bytes/marks to JSON query reports, using
  profile events when `system.query_log` is unavailable.
- Add vector statistics for directory fact candidates: rows, total entries,
  average entries per dir, max entries per dir, and bucket-array size.
- Add focused ops for final schema gates:
  - `DirInfo` and `DirInfos` broad and filtered.
  - `DirsHaveChildren` high fanout broad and filtered.
  - Whole-mount `where` with common and selective UID/GID/type/age filters.
  - Virtual ancestor children and summaries with many active mounts.
  - Extension glob cases with dotfile exceptions.
- Add import phase reporting for every selected table, including any optional
  filter index and virtual-summary cache.
- Preserve bounded memory reporting in perf artifacts, preferably max RSS from
  `/usr/bin/time -v` or equivalent process metrics.
