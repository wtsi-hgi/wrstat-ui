# Prompt for ClickHouse trie path-ID schema spec

Use this as the source-of-truth input for the spec-writer workflow.
The target output path is `.docs/trie/spec.md`, with implementation phase
documents in `.docs/trie/`.

The spec-writer should turn this feature description into a cohesive feature
spec with user stories, acceptance tests, implementation phases, and perf
comparison gates. Use the Go project conventions and the Go implementor/reviewer
skills for phase planning.

Do not inspect, alter, or rely on `.docs/summarise`; another agent is working
there.

## Goal

Write a full ClickHouse path-trie / numeric-node schema spec for `wrstat-ui`.

The current ClickHouse package stores or repeats full directory path strings in
hot tables and query shapes. `wrstat_files` already avoids one duplicate full
file path by deriving `path` from `parent_dir` and `name`, but it still stores
`parent_dir` as text for every file row, and the directory/fact/filter/navigation
tables repeat `dir` and `parent_dir` strings across many derived rows.

At production scale this means writing more than two billion file or directory
paths, most of them sharing long repeated prefixes. The new design should make
the filesystem tree a first-class indexed structure:

- store each directory path once per mount snapshot;
- assign compact integer directory IDs;
- represent parent/child relationships with integer IDs;
- use preorder/subtree ranges for recursive descendant queries;
- reconstruct full paths from a directory catalog or cache instead of storing
  the same path text in every hot row.

The expected result is a schema and query layer where parent/child lookup,
exact path lookup, recursive subtree scans, and full path reconstruction are
faster and substantially smaller because the hot tables compare integers rather
than long strings.

## Important Product Direction

This is not a compatibility migration. This branch has never been released.
There are no backwards compatibility requirements for old ClickHouse databases,
old schema versions, or old readers.

The spec should describe the full intended implementation, not a minimal
parallel proof-of-concept and not a reversible migration plan. It is acceptable
for the implementation to replace existing ClickHouse tables, tests, and query
paths wholesale if that gives the maximal benefit.

The spec must still require before/after performance evidence. That evidence is
not a go/no-go proof before implementation; it is a required final report so we
know whether the completed trie design is faster, slower, or neutral for every
important query type compared with the current branch.

## Current Schema And Query Shape To Replace

The spec-author should research the codebase, but the core current objects are:

- `wrstat_files`: stores `mount_path`, `snapshot_id`, `parent_dir`, `name`, and
  file metadata. `path` is an alias of `concat(parent_dir, name)`. It is ordered
  by `(mount_path, snapshot_id, parent_dir, name)`.
- `wrstat_dir_facts`: stores directory summary facts keyed by full `dir` string.
- `wrstat_children`: stores direct child directory edges using full
  `parent_dir` and child path strings.
- `wrstat_parent_facts`: duplicates directory fact payloads ordered by
  `(mount_path, snapshot_id, parent_dir, dir)` for direct-child navigation.
- `wrstat_child_filter_all`, `wrstat_dir_filter_all`,
  `wrstat_dir_filter_ageall`, active-prefix rollups, active virtual overlay
  tables, and related derived tables also carry full directory strings in hot
  rows.
- `Client.StatPath`, `Client.ListDir`, `Client.PermissionPath`, and
  `Client.FindByGlob` currently resolve request strings to
  `mount_path + parent_dir + name`, then query `wrstat_files` using string
  predicates on `parent_dir`.
- Tree and `DirInfo` style queries rely heavily on string `dir` and
  `parent_dir` predicates and, in some cases, string prefix ranges.

The trie spec should replace these hot string keys with numeric IDs wherever
possible. Full strings should remain only where they are truly needed:

- once per directory in a directory catalog;
- file or directory basenames where the UI/API must display or sort names;
- request/response boundaries;
- optional lookup hashes or verification fields for exact path resolution.

## Required Core Algorithm

Implement a per-mount-snapshot directory trie.

Each directory in a mount snapshot receives a deterministic integer `dir_id`.
The preferred assignment is DFS/preorder order, because it makes all descendants
of a directory occupy one contiguous numeric interval.

Each directory node must have at least:

- `mount_path`;
- `snapshot_id`;
- `dir_id`;
- `parent_id`;
- basename or display name;
- full directory path stored once;
- path hash for fast exact lookup, with full-path collision verification;
- depth;
- `subtree_end` or equivalent exclusive upper bound for descendant scans;
- direct child counts needed by existing tree semantics.

The spec should decide exact names and types, but a table such as
`wrstat_dirs` or `wrstat_path_nodes` is expected.

Required query primitives:

- exact directory path resolution:
  resolve `(mount_path, snapshot_id, full_path)` to `dir_id` using a hash and
  full-path verification, or an equally fast exact lookup;
- direct child directories:
  `parent_id = ?`;
- direct child files:
  `parent_id = ?`;
- recursive subtree:
  `dir_id >= ? AND dir_id < subtree_end`;
- exact file path:
  resolve the parent directory path to `parent_id`, then look up
  `(parent_id, name)`;
- full directory path:
  lookup `full_path` by `dir_id`;
- full file path:
  lookup or cache the parent directory full path and append `name`.

Full path reconstruction should be fast in practice. For list/stat/tree APIs,
the implementation should batch or cache parent full-path lookups instead of
performing one ClickHouse join per returned row when the application can do it
more cheaply.

## Required Schema Direction

The spec should design a clean trie-native schema. It may rename tables, but it
must cover the current behavioral surface.

Expected table families:

- directory node catalog:
  one row per directory per mount snapshot, with `dir_id`, `parent_id`,
  basename, one stored full path, path hash, depth, and subtree interval;
- file rows:
  one row per file-system entry currently represented by `wrstat_files`, keyed
  by numeric `parent_id` plus `name`, without storing `parent_dir` or full file
  path text;
- exact directory facts:
  summary facts keyed by `dir_id`, without full `dir` strings in hot rows;
- parent/child navigation facts:
  direct-child summary packets keyed by numeric `parent_id` and child `dir_id`,
  replacing the string-heavy `wrstat_parent_facts` shape;
- filtered child facts:
  numeric replacement for `wrstat_child_filter_all`, keyed by `parent_id`,
  filter dimensions, and child `dir_id`;
- filtered subtree/exact facts:
  numeric replacement for `wrstat_dir_filter_all` and/or
  `wrstat_dir_filter_ageall`, keyed for both exact `dir_id` lookups and
  subtree range scans;
- active virtual namespace:
  a numeric active-set overlay for `/`, `/lustre/`, `/nfs/`, intermediate
  virtual parents, and mount-root links, without copying all mount-local
  directory paths into a second full-string namespace;
- readiness, manifest, cleanup, and active snapshot/set marker tables needed
  to keep publish, rollback, tombstone, and failed import behavior correct.

The old `wrstat_children` table should not remain as a duplicated full-string
edge table. Direct child directory relationships should come from the directory
node catalog or from numeric child fact rows.

The old `wrstat_parent_facts` string shape should not remain as the primary
navigation object. If a parent packet table is still needed for performance, it
should be numeric.

The spec should consider ClickHouse projections or secondary serving tables
where one physical ordering cannot serve all important access patterns. The
goal is maximal benefit, not minimal DDL churn.

## Import And ID Assignment Requirements

The importer must build the trie deterministically for each mount snapshot.

The spec should define how IDs and subtree intervals are produced at large
scale, including whether the implementation uses a sorted stream, a stack over
lexicographic paths, an intermediate spool, or another deterministic method.

The design must handle very large datasets without requiring all file rows or
all path strings to be held in memory at once. It is acceptable to use more
upfront import work than the current implementation if it buys meaningful query
and storage wins, but the spec must still measure import wall time, CPU, memory,
spool bytes, ClickHouse bytes, part counts, and cleanup costs.

All derived tables produced during import must use the same directory IDs:

- file rows;
- exact directory summaries;
- parent/child navigation facts;
- filter tables;
- active virtual overlays and active prefix/summary rows;
- manifests and readiness checks.

Failed imports, replaced snapshots, inactive snapshots, tombstones, and active
set refreshes must clean up trie-native partitions consistently.

## Query Behavior Requirements

The trie implementation must preserve the current external API semantics.
Returned paths, parent paths, names, summaries, permissions, filters, and active
virtual root behavior must match the current ClickHouse implementation.

The spec must cover at least these query families:

- `Client.StatPath`;
- `Client.ListDir`;
- `Client.PermissionPath`;
- `Client.PermissionAnyInDir`;
- `Client.FindByGlob`, including direct-child patterns, recursive patterns,
  extension-optimized patterns, dotfiles, and offset/limit behavior;
- `DirInfo`;
- focused and broad `DirInfos`;
- `DirsHaveChildren`;
- Disktree endpoint navigation;
- `Tree.Where`;
- `where --dir`;
- broad and filtered summaries;
- active root `/`;
- active virtual parents such as `/lustre/` and `/nfs/`;
- mount roots and ordinary mount directories;
- high-fanout directories.

For `FindByGlob`, the spec must account for the fact that recursive matching
currently can use a derived full file path. The new design should avoid storing
full file paths in `wrstat_files`, but it still must support equivalent glob
semantics. Acceptable approaches include joining candidate rows to the directory
catalog after numeric subtree pruning, reconstructing candidate paths in the
application for bounded result sets, or another exact approach that preserves
current behavior and is measured.

For subtree queries, the preferred route is numeric interval pruning by
`dir_id`/`subtree_end`, not string prefix ranges.

For direct-child queries, the preferred route is numeric `parent_id` equality,
not full `parent_dir` equality.

For exact path queries, the preferred route is one directory-path resolution
followed by integer predicates.

## Active Virtual Namespace Requirements

The current UI/API has an active virtual namespace that can expose `/`,
`/lustre/`, `/nfs/`, and active mount roots as a single tree. The trie design
must preserve this behavior.

The spec should define a numeric active-set overlay rather than duplicating all
mount-local directory rows as full active paths.

The overlay should represent:

- synthetic virtual nodes such as `/`, `/lustre/`, and `/nfs/`;
- child edges between virtual nodes;
- links from virtual nodes to mount-local root `dir_id` values;
- summary rows for virtual nodes;
- filtered summary rows for virtual nodes when filters are supported;
- readiness and cleanup keyed by active set id.

The active-set id should continue to be deterministic from the active mount
state, including mount path, snapshot id, and an active generation/update input.

## Performance Comparison Requirements

The completed implementation must report whether the trie design is faster or
slower than the current branch for every important query type.

Because this is a clean rewrite, the baseline should be captured before the
schema changes are implemented or from an equivalent preserved current-branch
build. This baseline is required for comparison only. It must not become a
compatibility constraint or a reason to keep the old schema around.

The final evidence should include a clear before/after table with at least:

- import wall time;
- import CPU if available;
- max RSS;
- spool bytes;
- ClickHouse compressed and uncompressed bytes by table;
- ClickHouse part counts;
- rows written by table;
- p50/p95/p99 server-side latency;
- rows read, bytes read, and granules read where available;
- result counts and result digests;
- whether each query became faster, slower, or materially unchanged.

Compare at least these categories:

- exact file stat;
- file list direct children;
- permission path;
- permission any-in-dir;
- direct child glob;
- recursive glob;
- extension glob;
- dotfile glob;
- broad `DirInfo`;
- filtered `DirInfo`;
- broad/focused `DirInfos`;
- broad/filtered `DirsHaveChildren`;
- root Disktree;
- `/lustre/` and `/nfs/` Disktree;
- mount-root Disktree;
- high-fanout parent Disktree;
- broad `Tree.Where`;
- filtered `Tree.Where`;
- `where --dir`;
- active virtual root summaries;
- active virtual filtered summaries;
- import cleanup and active snapshot cleanup.

Use the existing `internal/chperf` and Bolt comparison harnesses where possible,
but extend them if they do not cover a query type needed for the before/after
answer.

The spec should require testing on existing representative subsets used by the
schema2/schema3 work, including mixed Lustre/NFS roots, `/nfs/t283_imaging/`,
and a high-fanout directory. It should also require the largest practical
production-like dataset available in the development environment.

Perf gates should include both absolute UX gates and relative before/after
reporting. The implementation should not be accepted without the comparison
report, even if the code is correct.

## Correctness Requirements

The trie schema must produce the same externally visible results as the current
ClickHouse implementation.

Acceptance tests must prove:

- exact path resolution returns the same file/directory rows;
- list directory returns the same children in the same order;
- full returned paths and parent paths are identical to current behavior;
- recursive subtree queries include exactly the same descendants;
- glob semantics match current behavior;
- permission checks match current behavior;
- broad and filtered directory summaries match current facts;
- active virtual root, `/lustre/`, `/nfs/`, and mount-root semantics match
  current behavior;
- high-fanout parent queries return complete and ordered children;
- cleanup removes all trie-native rows for failed or replaced snapshots;
- manifests/readiness markers prevent serving partially built trie data.

The tests should include collision-safe path-hash behavior: exact path lookup
may use hashes for speed, but it must verify the full path so a hash collision
cannot return the wrong directory.

## Non-Goals

Do not write an old-schema compatibility migration.

Do not keep old string-heavy tables as the primary serving path merely to ease
transition.

Do not solve this only with cache warming, browser cache behavior, or
process-local memoization. Caches may still exist, but the trie-native schema
and query plans must be the main performance improvement.

Do not add an external sidecar as the primary answer unless the spec-author
finds that ClickHouse cannot support the required semantics at all. If that
happens, record it as a blocker or explicit alternative, not as a hidden
fallback.

Do not modify `.docs/summarise`.
