# Prompt for ClickHouse schema investigation

Use this before writing the final spec in `.docs/schema/prompt.md`.

The goal is to determine which ClickHouse schema/query design should be
specced. This is exploratory work: use subagents, write temporary prototype
code, run `clickhouse-perf`, collect evidence, and then recommend a final
schema direction. Do not guess from intuition alone.

## Ground Rules

- Use subagents for the design experiments. If using the local multi-agent
  tooling, follow `/home/ubuntu/.agents/skills/subagents/SKILL.md`.
- Use writable subagents, not read-only exploration agents.
- Give each subagent a bounded task, the relevant skill paths, and explicit
  timeout requirements for long commands.
- Temporary code changes are allowed for experiments.
- Keep final code changes out of the branch unless the user explicitly asks to
  keep a prototype.
- Record commands, datasets, row counts, timings, and conclusions in an
  investigation report, preferably `.docs/schema/investigation-results.md`.
- Use `.tmp/agent/` for temporary scripts, binaries, perf JSON, scratch notes,
  and worktrees if needed.
- Real production-like datasets are available in `~/outputs/lustre` and
  `~/outputs/nfs`. Small fixtures are useful for correctness checks, but they
  are not enough to choose the final schema. At the same time, the real datasets
  are too large to experiement with since they can take hours to summarise, so
  sizable subsets must be used.

This branch has not shipped a ClickHouse schema. Do not spend effort preserving
backwards compatibility with earlier branch-local schema shapes.

## Baseline Setup Checklist

- [ ] Identify available production-like datasets. Look for examples mentioned
  in `.docs/bugfixes`, especially `t283_imaging`, `scratch127`, `scratch122`,
  and `scratch120`.
- [ ] Build a local binary for repeatable perf runs, for example
  `.tmp/agent/wrstat-ui`.
- [ ] Use a unique ClickHouse database per experiment so results do not
  contaminate each other.
- [ ] Capture the current branch baseline before changing code.
- [ ] Run at least one bounded import per representative dataset, using
  `clickhouse-perf import --json ... --maxLines ... --batchSize 100000`.
- [ ] Run query perf against the imported data, using `clickhouse-perf query
  --json ... --repeat ... --warmup ...`.
- [ ] Include filtered tree runs with `--tree-gids`, `--tree-uids`,
  `--tree-types`, or `--tree-ft` when the dataset has representative IDs.
- [ ] Run cold-provider, warm-provider, provider-update, Disktree, `where`, and
  file glob/list/stat operations. Use `--ops` for focused reruns when needed.
- [ ] Collect import phase durations, row counts, memory observations if
  available, query p50/p95/p99, and ClickHouse read rows/bytes from the perf
  reports.
- [ ] Add temporary metrics if needed to count ClickHouse parts and on-disk
  bytes from `system.parts`.

Useful command shapes:

```bash
timeout 600s env CGO_ENABLED=1 go test -tags netgo --count 1 ./clickhouse ./cmd ./internal/chperf
timeout 300s go build -o .tmp/agent/wrstat-ui .
timeout 1h .tmp/agent/wrstat-ui clickhouse-perf --json .tmp/agent/perf/<name>-import.json import <inputDir> --maxLines 1000000 --batchSize 100000
timeout 10m .tmp/agent/wrstat-ui clickhouse-perf --json .tmp/agent/perf/<name>-query.json query --repeat 20 --warmup 1
```

Adjust timeouts and `--maxLines` upward when a candidate looks promising and
needs stronger proof.

## Investigation Checklist

### 1. Baseline Current Design

- [ ] Measure the current branch with no prototype changes.
- [ ] Record row counts for `wrstat_dguta`, `wrstat_children`,
  `wrstat_dir_summary`, `wrstat_dir_dguta_vector`, `wrstat_files`, and
  active-tree tables.
- [ ] Record import time, import guardrails, maximum observed memory if
  available, and query timings.
- [ ] Record whether current query routing depends on raw DGUTA, maintained
  summaries, vectors, active-tree tables, or caches for each hot path.
- [ ] Produce a baseline table that every prototype can be compared against.

### 2. Vector-First Directory Facts

Question: should the per-directory vector shape become the canonical mount tree
fact table, replacing raw `wrstat_dguta` for mount-directory reads?

- [ ] Assign a subagent to build a minimal vector-first prototype.
- [ ] Store one row per directory with aligned arrays or a ClickHouse `Nested`
  structure for GID, UID, file type, age, counts, sizes, and time buckets.
- [ ] Add scalar all-age/default-file summary columns and `child_count` only if
  needed for hot paths.
- [ ] Route `DirInfo`, `DirInfos`, `DirsHaveChildren`, and ordinary Disktree
  paths through the vector-first table.
- [ ] Decide whether raw `wrstat_dguta` can be removed, retained only for
  active-tree/ancestor paths, or replaced by another structure.
- [ ] Measure import row volume and runtime on directory-heavy data.
- [ ] Measure unfiltered and filtered query timings.
- [ ] Check whether whole-mount filtered `where` over vectors is acceptable or
  needs a separate filter index.
- [ ] Record code complexity: did this remove fallback routing, or add more?

### 3. Inverted Filter Index Tables

Question: should filtered searches use separate tables ordered by filter
dimensions instead of scanning directory vectors or raw directory rows?

- [ ] Assign a subagent to prototype one or more filter-oriented tables.
- [ ] Try ordering by dimensions such as
  `(mount_path, snapshot_id, gid, uid, ft, age, dir)` and/or separate GID/UID
  oriented keys.
- [ ] Prototype filtered `where` against the index.
- [ ] Prototype broad filtered `DirsHaveChildren` against the index.
- [ ] Measure added import rows, bytes, parts, CPU, memory, and wall time.
- [ ] Compare filtered p50/p95/p99 against baseline and vector-first results.
- [ ] Decide whether the speedup justifies the extra storage/import cost.

### 4. Directory ID Dictionary

Question: should hot tree tables use integer directory ids instead of repeated
path strings?

- [ ] Assign a subagent to prototype deterministic directory ids and a
  directory dictionary.
- [ ] Include at least directory id, parent id, name, full or mount-relative
  path, depth, and any path hash or ancestor metadata needed.
- [ ] Route child lookups and summary joins through ids.
- [ ] Verify API boundaries still return exact paths.
- [ ] Address deterministic id generation during streaming import.
- [ ] Address hash collision handling if hashes are used.
- [ ] Measure storage size, index size, query latency, and import overhead.
- [ ] Decide whether path-prefix and virtual-ancestor queries become simpler or
  more complex.

### 5. Active Mount Metadata

Question: should active snapshot metadata remain in the current
`ReplacingMergeTree` shape, or become an explicit append-only event log?

- [ ] Assign a subagent to prototype or design an append-only mount event table.
- [ ] Preserve active publish, inactive tombstone, rollback/retry, and
  same-millisecond tie semantics.
- [ ] Decide the single final active-view name.
- [ ] Check whether previous active rows remain available for retry cleanup.
- [ ] Measure active view query cost with many mounts and history rows.
- [ ] Recommend the final engine/order/view shape.

### 6. Virtual Ancestors And Active-Tree Tables

Question: are fingerprinted `wrstat_tree_*` active-tree projections the best
way to support paths above mount roots?

- [ ] Assign a subagent to evaluate mount hierarchy or tree hierarchy tables.
- [ ] Prototype or model a table keyed by path segments or directory ids for
  virtual ancestor children.
- [ ] Evaluate whether virtual ancestor summaries can be composed cheaply from
  mount-root summaries.
- [ ] Compare with current fingerprinted active-tree projection cost and query
  speed.
- [ ] Decide whether active-tree tables stay, shrink, or disappear.

### 7. ClickHouse Aggregate-State Tables

Question: can ClickHouse aggregate-state tables replace some hand-written Go
projection writes?

- [ ] Assign a subagent to prototype `AggregatingMergeTree` or aggregate-state
  materialized view variants for maintained summaries.
- [ ] Ensure the build happens inside the snapshot import lifecycle.
- [ ] Ensure readiness is unambiguous and no long background rebuild is needed.
- [ ] Measure import time, memory, query time, and operational complexity.
- [ ] Reject this option if it reintroduces long `INSERT ... SELECT` rebuilds
  or normal-query-timeout failures.

### 8. File-Facts-Only Challenge

Question: can tree summaries be derived directly from `wrstat_files`, and if
not, can we document why?

- [ ] Assign a subagent to evaluate deriving tree summaries from file-level
  rows.
- [ ] Test or model direct subtree prefix scans.
- [ ] Test or model exploding each file into ancestor rows.
- [ ] Estimate row amplification, import cost, and query cost.
- [ ] Either produce a viable prototype or write a clear rejection with data.

### 9. File Extension Index

Question: should extension-limited glob queries use `f.ext`, or should
`ext_idx` be removed?

- [ ] Assign a subagent to add temporary `f.ext = ?` predicates for patterns
  where it is semantically safe, such as `*.bam` and `**/*.bam`.
- [ ] Preserve exact glob semantics for dotfiles, directories, case handling,
  and tricky names.
- [ ] Compare ClickHouse read rows/bytes and latency with and without the
  predicate.
- [ ] Decide whether to keep, change, or remove `ext_idx`.

### 10. Import Batch Writer Unification

Question: can repeated import writer mechanics be simplified without making
large imports fragile?

- [ ] Assign a subagent to inspect DGUTA, children, file ingest, projection, and
  basedirs batch writers.
- [ ] Prototype a common bounded block writer if it reduces duplication without
  hiding important table-specific behaviour.
- [ ] Verify import timeout detachment, send/reprepare behaviour, final partial
  sends, and abort cleanup.
- [ ] Measure any overhead.

### 11. Projection Readiness And Version Metadata

Question: in a clean schema-v1 world, do projection readiness rows need version
columns?

- [ ] Assign a subagent to map all readiness checks and version columns.
- [ ] Decide whether readiness can mean "row exists for this snapshot" or needs
  a version field because multiple derived outputs must be atomically matched.
- [ ] If version fields remain, define what they protect in schema v1.
- [ ] If version fields are removed, prove readiness still protects readers
  from partial projection writes.

## Comparison Matrix

The final investigation report must include a comparison table with at least:

- design name
- tables added/removed
- canonical tree fact source
- import rows per representative dataset
- import wall time
- memory observations
- ClickHouse parts/bytes if measured
- unfiltered `where` p50/p95/p99
- filtered `where` p50/p95/p99
- Disktree cold-provider p50/p95/p99
- Disktree warm/visible-child p50/p95/p99
- file glob/list/stat impact, if relevant
- complexity notes
- recommendation

## Final Recommendation

End the investigation with:

- the recommended schema direction
- alternatives rejected and why
- risks and unknowns
- concrete spec decisions to copy into `.docs/schema/prompt.md`
- any changes needed to `clickhouse-perf` before final implementation work

If no candidate beats the current design, say that clearly and recommend a
cleaned-up version of the current design. The recommendation still must remove
branch-local backwards compatibility and produce one clean schema version 1.
