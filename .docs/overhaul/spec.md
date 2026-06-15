# ClickHouse Storage + Ingest + Query Overhaul Specification

## Overview

The `clickhouse` branch stores a directory path as text billions of times across
~18 tables: once per file (`wrstat_files.parent_dir` on ~1.3B rows), again per
directory fact, twice per parent fact, once per child edge, and again multiplied
by every `(age, gid, uid, ft)` filter combination in the exploded filter tables.
This bloats storage, slows ingest (the spool carries the duplicated text), slows
merges, and forces queries to scan and compare long strings.

This overhaul replaces that design wholesale (no migration, no compatibility
shims; the branch has never shipped). The core move is **preorder interval
(nested-set) labelling**: during the summariser's existing depth-first walk every
directory gets a dense integer `dir_id` assigned in first-visit order, recording
`parent_id` and `subtree_end` (smallest `dir_id` greater than every descendant).
A directory's whole subtree is then the contiguous range `[dir_id, subtree_end)`;
its direct children share `parent_id`. The path string is stored exactly once per
`(mount_path, snapshot_id)` in a directory catalog keyed by `dir_id`; every other
table stores `dir_id`, never the path.

This collapses dedup, parent/child navigation, exact lookup, broad subtree scans,
and full-path reconstruction to integer primary-key operations. The filter story
is deliberately conservative: the `dir_id`-keyed numeric filter materialisations
are **kept by default** (the path strings, not the rows, were the duplication);
an in-query vector-filter path is also implemented; a specific materialisation is
collapsed only where the mandatory before/after benchmark proves the in-query
path meets the latency gate for that pattern. The external API
(`db/interfaces.go`, `clickhouse.Client` file API, basedirs readers) is
unchanged.

## Architecture

### Packages and files

- `summary/` (`summariser.go`): unchanged DFS walk; `DirectoryPath{Name, Depth,
  Parent}`, `FileInfo`, `Operation{Add, Output}`, `OperationGenerator`.
- `summary/dirguta/`: assigns `dir_id`/`parent_id`/`subtree_end` during the walk
  and carries them on the emitted record (area B).
- `db/` (`interfaces.go`, `dguta.go`): `RecordDGUTA` gains id fields; the
  `Database`, `DGUTAWriter`, `DirInfoBatcher`, `DirHasChildrenBatcher`, `Whereer`
  interfaces and `DirSummary`/`DirInfo`/`Filter`/`Info` types are unchanged
  externally.
- `clickhouse/schema/*.sql`: rewritten DDL (areas A, C, D, G).
- `clickhouse/` writers and query layer: rewritten to id-keyed rows (areas C-H).
- `clickhouse/catalog.go` (new): catalog write buffer + path<->id resolver.
- `clickhouse/navindex.go` (new, optional, flag-gated): in-memory nav index
  (area I).
- `internal/chspool/`: spool carries one catalog stream + id-keyed rows (area H).
- `internal/chperf/`: extended benchmark harness; baseline capture (gate).

### ID type and keying (Notes: snapshot/mount keying)

- `dir_id`, `parent_id`, `subtree_end`: `UInt32`. Justification: max dirs per
  `(mount_path, snapshot_id)` is bounded well under 2^32 on production trees;
  validate against the largest benchmark dataset and widen to `UInt64` only if
  the catalog row count for any single snapshot approaches 2^31. Record the
  measured max-dirs-per-snapshot in the benchmark report.
- `dir_id` is unique only within `(mount_path, snapshot_id)`. Every id-keyed
  table carries `(mount_path LowCardinality(String), snapshot_id UUID, dir_id
  UInt32)` and keeps today's `PARTITION BY (mount_path, snapshot_id)`. `dir_id`
  replaces the path column in the sort key.
- A compact integer `mount_key`/`snapshot_key` replacing the `(mount_path,
  snapshot_id)` pair in hot rows is adopted **only** if the benchmark shows it
  materially reduces part sizes; default to the simple model.
- Codecs: `dir_id`/`parent_id`/`subtree_end` use `CODEC(Delta, LZ4)` (dense,
  monotonic in sort order). `full_path` is cold: `CODEC(ZSTD(3))`. Validate codec
  choices against measured part sizes (prompt Notes).

### Sentinels

- The mount catalog is rooted at filesystem `/` (area A, Notes: above-root
  ancestor chain). `/`'s `parent_id` is the sentinel `parentSentinel =
  0xFFFFFFFF` (`dir_id` 0 is `/` itself, so the sentinel must not collide with a
  real id).

### Error handling

- Sentinel errors as package `var` with `errors.New`; wrap with `%w`. Reuse the
  existing `ErrDirNotFound`. Add `ErrIDUnresolved` for an in-snapshot path that
  cannot be resolved to a `dir_id` during readiness (area G fallback).
- Path-hash hits MUST verify `full_path` before returning (Notes); a mismatch is
  treated as a miss, never a wrong result.

---

## A. Directory catalog: the single home for path text

### A1: `wrstat_dirs` catalog table

As a storage designer, I want one catalog row per directory per snapshot, so
that path text is stored exactly once and every other table can reference
`dir_id`.

**File:** `clickhouse/schema/004_dirs.sql`

```sql
CREATE TABLE IF NOT EXISTS wrstat_dirs (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir_id UInt32 CODEC(Delta, LZ4),
  parent_id UInt32 CODEC(Delta, LZ4),
  subtree_end UInt32 CODEC(Delta, LZ4),
  depth UInt16 CODEC(Delta, LZ4),
  name String CODEC(ZSTD(3)),
  full_path String CODEC(ZSTD(3)),
  child_dir_count UInt32 CODEC(Delta, LZ4),
  child_file_count UInt32 CODEC(Delta, LZ4),
  path_hash UInt64 CODEC(LZ4),
  INDEX full_path_ngram full_path TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4,
  INDEX full_path_tokens full_path TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 4,
  PROJECTION children_proj (
    SELECT * ORDER BY (mount_path, snapshot_id, parent_id, dir_id)
  ),
  PROJECTION path_hash_proj (
    SELECT * ORDER BY (mount_path, snapshot_id, path_hash)
  )
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir_id)
SETTINGS index_granularity = 8192;
```

- `path_hash` = 64-bit hash of `full_path` (e.g. CityHash64 / `sipHash64`),
  primary path->id resolver; the `path_hash_proj` projection serves it. Every hit
  re-reads `full_path` and compares before returning (collision safety, Notes).
  The benchmark may drop `path_hash` + projection if the `full_path` skip-index /
  ordering alone meets the 100 ms p95 gate.
- `name` is basename only (keeps the trailing `/` convention the summariser uses
  for directories). `full_path` is the single stored copy of the full path.
- `child_dir_count` / `child_file_count` make "has children" a column read.

**Test file:** `clickhouse/dirs_catalog_test.go`

**Acceptance tests:**

1. Given a snapshot with N distinct directories, when the catalog is read, then
   `SELECT count() FROM wrstat_dirs WHERE mount_path=? AND snapshot_id=?` equals N
   and `SELECT count(DISTINCT full_path)` also equals N (no dir stored twice).
2. Given any catalog row R, when its descendants are computed as `dir_id IN
   [R.dir_id, R.subtree_end)`, then that set equals the set reachable by
   recursively following `parent_id = R.dir_id` (interval invariant: descendant
   <=> `dir_id in [root_id, subtree_end)`).
3. Given the mount root `/`, when its row is read, then `parent_id = 0xFFFFFFFF`.
4. Given any `dir_id`, when its full path is reconstructed via stored `full_path`
   and via the `parent_id` walk (concatenating `name` from `/` down), then the two
   byte-strings are identical.

---

## B. ID assignment during summarise (deterministic, streaming)

### B1: preorder id assignment in the DFS walk

As an ingest author, I want `dir_id`/`parent_id`/`subtree_end` assigned during
the existing depth-first walk using only an ancestor stack, so that ids are a
near-free byproduct and the spool can carry ids not strings.

The `stats` parser already emits directories DFS-contiguously (it synthesises
intermediate directory entries, deepest-first, between divergence points), so a
directory and all its descendants form one uninterrupted run. Files interleave
with sibling subdirectories under lexicographic ordering, so **id assignment must
key off directory boundaries (`info.IsDir()` / `DirectoryPath` push/pop), not raw
entry order**.

Algorithm (in `summary/dirguta`, hooked into the operation lifecycle so it sees
each directory's enter and `Output`):

- A monotonic counter `next` (start value set per B2). On the first `Add` for a
  directory (when `thisDir` is set), assign `dir_id = next; next++`, record
  `parent_id` from the parent operation, `depth` from `DirectoryPath.Depth`.
- On that directory's `Output` (subtree complete, popped), set `subtree_end =
  next` (the next id not yet handed out is the smallest id greater than every
  descendant).
- Push the assigned `dir_id` onto the carried `RecordDGUTA` so all writers see it
  (B3).

This requires no full-tree buffering: state is the ancestor chain (depth <= ~128)
plus the counter. Determinism: same input stream -> same `next` sequence -> same
ids (no maps, no time, no concurrency in the id path).

**Package:** `summary/dirguta/`
**File:** `summary/dirguta/dirguta.go`
**Test file:** `summary/dirguta/idassign_test.go`

**Acceptance tests:**

1. Given a tree `/a/{b/{f1}, c/{f2}}` fed in DFS order with files interleaved
   among subdirectories, when summarised, then ids are gap-free preorder: `a`<`b`,
   `b`'s `subtree_end` <= `c`'s `dir_id`, and `a.subtree_end` = max child
   `subtree_end`.
2. Given the same input twice, when summarised twice, then every emitted
   `(full_path, dir_id, parent_id, subtree_end)` tuple is byte-for-byte identical
   (determinism).
3. Given a directory with a file lexicographically between two subdirectories,
   when summarised, then the file does not consume a `dir_id` and the two
   subdirectories receive consecutive-subtree ids.
4. Given any directory D, when ids are assigned, then `[D.dir_id, D.subtree_end)`
   contains exactly D plus all its descendant directories (interval invariant on
   a real fixture dataset).

### B2: above-root ancestor reserved low-id block

As an ingest author, I want the above-root ancestor chain (`/`, `/lustre/`, ...,
`mountPath`'s parent, then `mountPath`) to occupy a reserved low `dir_id` block,
so that single-mount queries that hit the per-mount tables keep seeing those rows
and the interval invariant holds for the whole chain.

`SetMountPath` is called before any `Add`, so `mountPath`'s depth D (slash count)
is known up front. Reserve ids `0..D` for the linear chain in preorder: `/` = 0,
`/lustre/` = 1, ..., `mountPath`'s parent = D-1, the data root `mountPath` = D.
Start the data-root subtree counter at D+1.

The ancestor rows are emitted out of order at end-of-walk (deepest-first, via
`dirguta.outputRoot`), but their ids are fixed by the reservation (each ancestor's
`dir_id` = its depth). Backfill each ancestor's `subtree_end` to the
end-of-snapshot bound `next` (final counter value): the linear chain contains the
entire data-root subtree, so every ancestor's interval spans the whole snapshot.
`/`'s `parent_id` = sentinel.

The handful of duplicated linear-chain rows per snapshot is acceptable and
matches current behaviour. These rows stay in the per-`(mount_path, snapshot_id)`
catalog; they are **not** moved into the `active_set_id` virtual layer.

**File:** `summary/dirguta/dirguta.go` (`outputRoot` path)
**Test file:** `summary/dirguta/idassign_test.go`

**Acceptance tests:**

1. Given `mountPath = /lustre/scratch125/teamX/` (depth D=3), when summarised,
   then `/`=0, `/lustre/`=1, `/lustre/scratch125/`=2, `/lustre/scratch125/teamX/`
   =3, and the first descendant directory under the data root has `dir_id` >= 4.
2. Given the completed snapshot with final counter `next`, when ancestor rows are
   read, then every ancestor (ids 0..D-1) has `subtree_end = next` and the
   interval invariant from A1 holds for each.
3. Given the catalog, when `/`'s row is read, then `parent_id = sentinel` and its
   `[0, next)` interval contains every catalog row.

### B3: ids carried to writers

As a writer author, I want the assigned ids on `RecordDGUTA`, so every table can
emit `dir_id` rows.

`db.RecordDGUTA` gains `DirID, ParentID, SubtreeEnd uint32` and `Depth uint16`.
`db.GUTAs`/`DGUTA`/`Filter`/`DirSummary` are unchanged. The `DGUTAWriter`
interface (`Add, SetBatchSize, SetMountPath, SetUpdatedAt, Close`) is unchanged.

**File:** `db/dguta.go`
**Test file:** `summary/dirguta/dirguta_test.go` (existing, updated)

**Acceptance test:**

1. Given a summarised tree, when each `RecordDGUTA` reaches the writer, then its
   `DirID`/`ParentID`/`SubtreeEnd`/`Depth` match the catalog row for that
   directory's `full_path`.

### B4: import cost measured, not assumed

The headline claim (ids near-free, leaner spool makes ingest faster) MUST be
confirmed by measurement; an import regression is acceptable if reported and
justified by query/storage wins.

**Acceptance test:** the benchmark (gate section) reports summarise wall time,
CPU, and max RSS for the overhaul vs baseline; the import section states the
delta and whether the "near-free" claim held.

---

## C. Files table rewrite: `dir_id` instead of `parent_dir`

### C1: `wrstat_files` keyed by `dir_id`

As a storage designer, I want the largest table to store `dir_id` not
`parent_dir`, so the dominant text-duplication source is removed.

**File:** `clickhouse/schema/011_files.sql`

```sql
CREATE TABLE IF NOT EXISTS wrstat_files (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir_id UInt32 CODEC(Delta, LZ4),
  name String CODEC(LZ4),
  ext LowCardinality(String) CODEC(LZ4),
  entry_type UInt8,
  size UInt64 CODEC(Delta, LZ4),
  apparent_size UInt64 CODEC(Delta, LZ4),
  uid UInt32,
  gid UInt32,
  atime DateTime CODEC(Delta, LZ4),
  mtime DateTime CODEC(Delta, LZ4),
  ctime DateTime CODEC(Delta, LZ4),
  inode UInt64 CODEC(Delta, LZ4),
  nlink UInt64 CODEC(Delta, LZ4),
  INDEX ext_idx ext TYPE set(256) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir_id, name)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
```

- `parent_dir` (String) is removed; the `path` ALIAS is removed. Full path for
  display is the catalog `full_path` + `name` (resolved via a `dir_id` join, F3).

### C2: file API resolves path->dir_id then point-looks-up

As a server, I want `StatPath`/`IsDir`/`PermissionPath`/`ListDir`/recursive
enumeration unchanged externally, served via the catalog.

- `StatPath`/`IsDir`/`PermissionPath`: resolve `(mount, parentDir)` -> `dir_id`
  via the catalog (path_hash + verify, or full_path projection), then `WHERE
  mount_path=? AND snapshot_id=sid AND dir_id=? AND name=? LIMIT 1`.
- `ListDir`: resolve dir -> `dir_id`, then `WHERE dir_id=? ORDER BY name ASC LIMIT
  ? OFFSET ?`.
- Recursive file enumeration: `WHERE dir_id >= ? AND dir_id < ?` over the
  `[dir_id, subtree_end)` range.
- `PermissionAnyInDir`: resolve dir -> `dir_id`, then query `wrstat_dir_facts`
  (D1) by `dir_id` with the same `arrayExists` over `ages/uids/gids`.

Signatures unchanged: `StatPath(ctx, path, opts StatOptions) (*FileRow, error)`,
`ListDir(ctx, dir, opts ListOptions) ([]FileRow, error)`, `IsDir(ctx, path)
(bool, error)`, `PermissionPath(ctx, path, uid, gids) (bool, error)`,
`PermissionAnyInDir(ctx, dir, uid, gids) (bool, error)`. `FileRow` keeps
`Path`/`ParentDir` fields, populated from the catalog at read time.

**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/file_api_test.go` (existing, updated)

**Acceptance tests:**

1. Given a fixture loaded under both old and new schema, when `StatPath`,
   `ListDir`, `IsDir`, `PermissionPath`, `PermissionAnyInDir` are called for the
   same inputs, then results (including returned `Path`/`ParentDir`, ordering,
   pagination) are identical.
2. Given the new `wrstat_files`, when its columns are inspected, then no column
   stores a directory path string (only basename `name`).
3. Given a missing path, when `StatPath` is called, then it returns the same
   not-found behaviour as baseline (nil/`ErrDirNotFound` per current contract).
4. Given a path whose `path_hash` collides with another path's hash (synthetic
   test), when resolved, then the verifier rejects the wrong row and returns the
   correct dir_id or not-found - never a wrong file.

---

## D. Numeric filter tables (default), collapse only where measured

### D1: `wrstat_dir_facts` keyed by `dir_id`

As a query author, I want the facts table keyed by `dir_id` keeping the
parallel-array DGUTA vector, so exact and subtree summaries are integer ops and
the in-query filter path is available.

**File:** `clickhouse/schema/005_dir_facts.sql`

Same columns as today (`all_*`, `file_*` scalar summaries; parallel arrays
`gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, atime_buckets,
mtime_buckets`; `child_count`; `updated_at`; `refreshed_at`) **except** `dir
String` is replaced by `dir_id UInt32 CODEC(Delta, LZ4)` and `subtree_end UInt32
CODEC(Delta, LZ4)` is added (so subtree range scans need no catalog join).
`ORDER BY (mount_path, snapshot_id, dir_id)`. Keep the scalar `all_*`/`file_*`
columns (they serve broad unfiltered `DirInfo` without array work); the benchmark
decides if any are redundant.

`wrstat_parent_facts` is removed: child-fact reads are served by the
`subtree_end`-bearing facts table over the `parent_id` band (E1) or the numeric
child-filter table (D2). The `has_children` need is met by `child_dir_count` on
the catalog.

### D2: numeric filter materialisations (retained by default)

As a query author, I want the filter tables kept but numeric (no path strings),
so the proven low-latency serving model for filtered/high-fanout queries
survives while duplication is removed.

**Files:** `clickhouse/schema/015_child_filter_all.sql`,
`016_dir_filter_all.sql`, `012_dir_filter_ageall.sql`

- `wrstat_child_filter_all`: replace `parent_dir`/`dir` strings with `parent_id`
  and child `dir_id`. `ORDER BY (mount_path, snapshot_id, parent_id, age, gid,
  uid, ft, dir_id)`. Keep `count, size, atime_min, mtime_max, atime_buckets,
  mtime_buckets, filter_child_count, child_count, has_filter_children,
  has_children, refreshed_at`.
- `wrstat_dir_filter_all`: replace `dir`/`parent_dir` with `dir_id` (+
  `subtree_end` for range scans). `ORDER BY (mount_path, snapshot_id, age, gid,
  uid, ft, dir_id)`.
- `wrstat_dir_filter_ageall`: replace `dir` with `dir_id` (+ `subtree_end`).
  `ORDER BY (mount_path, snapshot_id, gid, uid, ft, dir_id)`.

No `dir`/`parent_dir` string in any of these hot rows.

### D3: in-query vector-filter fallback path

As a query author, I want an in-query `arrayFilter`/`arrayReduce` path over a
`dir_id` range against the facts vector, so the benchmark can compare it
head-to-head with the materialised tables per pattern.

Implement a code path that, for a filter `(gids, uids, ft, age)`, reduces the
parallel arrays of one facts row (exact) or a `[dir_id, subtree_end)` band
(subtree/children) to filtered aggregates using ClickHouse array functions -
selectable per query so either route produces identical results.

### D4: per-pattern collapse decision (benchmark-gated)

As a maintainer, I want a documented decision procedure, so each
retained-vs-collapsed choice cites a measurement.

Routes (each benchmarked both ways):

- **Filtered exact** (`DirInfo(dir, filter)`): single facts row in-query filter,
  or one `wrstat_dir_filter_all` lookup. Likeliest safe collapse.
- **Filtered children** (`DirInfos` of children, `DirsHaveChildren`):
  `parent_id`-keyed `wrstat_child_filter_all` band, or the contiguous `parent_id`
  band of facts rows filtered in-query. High-fanout parents (~11k children) read
  one contiguous range. Likeliest table to retain.
- **Filtered subtree** (`Where`, Disktree, `where --dir`): `(dir_id,
  subtree_end)` range over `wrstat_dir_filter_all`/`_ageall`, or facts-table range
  scan emitting per-node filtered aggregates above the recurse threshold.

A materialisation is dropped only when the benchmark proves the in-query path
meets the latency gate for that pattern x dataset x fanout; the collapse cites
the measurement. An unproven collapse is a spec failure. If a bounded local
dataset cannot reproduce a pattern, that is stated explicitly and the
materialisation is retained.

**Test file:** `clickhouse/filter_parity_test.go`

**Acceptance tests:**

1. Given the benchmark filter matrix (each of gid, uid, ft, age, and combos),
   when the materialised-table route and the in-query vector route serve the same
   filtered query, then their results are bit-for-bit identical (parity).
2. Given any filter table, when its columns are inspected, then none stores a
   `dir`/`parent_dir`/path string.
3. Given filtered `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where` against a
   fixture, when compared to baseline output, then results match exactly (same
   summaries, child ordering, full paths).
4. Given the benchmark report, when a materialisation is marked collapsed, then a
   cited measurement justifies it and the in-query route still meets the latency
   gate for that pattern.

---

## E. Parent / child / ancestor / full-path resolution

### E1: `Children` and `DirInfos`/`DirInfo` via integer bands

As a server, I want `Children`, `DirInfo`, `DirInfos` served by integer lookups,
with external semantics unchanged.

- `Children(dir)`: resolve `dir_id`, read the catalog `parent_id = dir_id` band
  (via `children_proj`), return each child's `full_path`, de-duplicated and sorted
  across sources (unchanged semantics).
- `DirInfo(dir, filter)`: resolve `dir_id`, read the facts row, merge across
  sources, apply filter, set `Modtime` to latest `updated_at` across sources;
  `ErrDirNotFound` only if missing from all sources.
- `DirInfos(dirs, filter)`: resolve dirs -> `dir_id IN (...)`, batch facts read.

Signatures unchanged: `DirInfo(dir string, filter *Filter) (*DirSummary,
error)`, `Children(dir string) ([]string, error)`, `DirInfos(dirs []string,
filter *Filter) (map[string]*DirSummary, error)`.

### E2: `DirsHaveChildren`, ancestors, batch full-path

As a server, I want presence/ancestor/full-path resolution as integer reads.

- `DirsHaveChildren(dirs, filter)`: broad uses catalog `child_dir_count > 0`;
  filter-aware uses the `wrstat_child_filter_all` `parent_id` band (or in-query
  band). Signature unchanged.
- Ancestors/breadcrumbs and path->id: `parent_id` walk and/or path_hash/full_path.
- Batch full-path for a page of file rows: one catalog `dir_id IN (...)` resolves
  the whole page (F3).

**File:** `clickhouse/database.go`
**Test file:** `clickhouse/database_test.go` (existing, updated)

**Acceptance tests:**

1. Given a parent with children across multiple sources, when `Children` is
   called, then returned paths are de-duplicated, sorted, and byte-identical to
   baseline.
2. Given a directory present in 2 sources with differing `updated_at`, when
   `DirInfo` is called, then `Modtime` is the latest `updated_at` and the summary
   is the merged-then-filtered result (matches baseline).
3. Given a leaf or missing dir, when `Children` is called, then it returns
   nil/empty (matches baseline).
4. Given a batch of dirs, when `DirsHaveChildren` is called broad and filtered,
   then the boolean map matches baseline for both.
5. Given a dir, when its breadcrumb ancestors are resolved via the `parent_id`
   walk, then the paths match those produced by splitting the baseline full path.

### E3: `Where` over `dir_id` ranges

As a CLI/server, I want `Where` served by `(dir_id, subtree_end)` range scans,
sorted DESC by Size then ASC by Dir.

`Where(dir, filter, recurseCount)`: resolve `dir_id`, range-scan facts/filter
rows in `[dir_id, subtree_end)`, apply the `recurseCount` threshold per node,
return `DCSs` sorted as today. Default filter (FT==0 -> AllTypesExceptDirectories)
preserved. Signature unchanged: `Where(dir string, filter *Filter, recurseCount
func(string) int) (DCSs, error)`.

**Acceptance tests:**

1. Given a subtree, when `Where` is called broad and filtered, then `DCSs` are
   identical to baseline (same dirs, sizes, full paths, DESC-Size/ASC-Dir order).
2. Given an auth-restricted `Where` (uid/gid filter), when called, then only
   permitted dirs appear, matching baseline.

---

## F. Full-text / glob path search

### F1: basename globs on `wrstat_files.name`

As a server, I want direct-child / extension / dotfile globs that match on
basename to filter `wrstat_files.name` directly.

### F2: full-path globs on the catalog skip index

As a server, I want full-path / substring / recursive globs resolved against the
small `wrstat_dirs` catalog using `ngrambf_v1`/`tokenbf_v1` on `full_path`, so the
1.3B-row files table is never scanned for directory-path matching.

- Compile each pattern: basename-only -> `name` filter on files; path-bearing ->
  candidate `dir_id`s (or `[dir_id, subtree_end)` ranges) from the catalog
  skip-index + regex on `full_path`, then intersect with files by `dir_id`.
- Preserve gitignore-style multi-pattern semantics (`*` not crossing `/`, `**`
  crossing dir boundaries, `?`), the `(ownerEnabled=0 OR uid=? OR has(gids, gid))`
  permission filter, ordering, and pagination. `CountByGlob` falls back to
  `FindByGlob` above 32 patterns (unchanged).

Signatures unchanged: `FindByGlob(ctx, baseDirs, patterns, opts FindOptions)
([]FileRow, error)`, `CountByGlob(ctx, baseDirs, patterns, opts FindOptions)
(int, error)`.

### F3: result full-path reconstruction

`FindByGlob`/`ListDir` results carry full `Path`/`ParentDir`: after the files
read, one catalog `dir_id IN (...)` over the result page resolves `full_path`,
then `Path = full_path + name`.

**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/file_api_test.go` (existing, updated)

**Acceptance tests:**

1. Given the existing glob benchmark cases (direct-child, recursive, extension,
   dotfile), when `FindByGlob` runs, then results (paths, ordering, pagination,
   dedup) are identical to baseline.
2. Given the same cases, when `CountByGlob` runs, then counts match baseline.
3. Given a full-path glob, when executed, then the query plan reads
   `wrstat_dirs` (skip-index) for path matching and `wrstat_files` only by
   `dir_id` - the files table is not scanned for path text (verify via
   `EXPLAIN`/read-rows in the benchmark).

---

## G. Basedirs and active virtual namespace in the id world

### G1: basedirs reference `dir_id` with external string fallback

As a basedirs author, I want in-snapshot `basedir`/`subdir` paths referenced by
`dir_id`, with an explicit external string column for paths legitimately outside
the snapshot, so historical/quota data still resolves.

- `wrstat_basedirs_group_subdirs` / `_user_subdirs`: replace `basedir`/`subdir`
  strings with `basedir_id`/`subdir_id UInt32` resolved against the active
  snapshot catalog, plus `basedir_external String`/`subdir_external String`
  (clearly marked, used only when no `dir_id` exists). `wrstat_basedirs_group_usage`
  / `_user_usage`: `basedir_id` + `basedir_external`.
- `wrstat_basedirs_history` keeps strings (history spans snapshots; explicitly
  external).
- Readiness MUST fail (`ErrIDUnresolved`) if an in-snapshot active
  `basedir`/`subdir` cannot be resolved to a `dir_id`.
- Reader signatures unchanged: `GroupUsage(age)`, `UserUsage(age)`,
  `GroupSubDirs(gid, basedir, age)`, `UserSubDirs(uid, basedir, age)`,
  `History(gid, path)` - they return the same `[]*Usage`/`[]*SubDir`/`[]History`
  with the same path strings (resolved from `dir_id` or the external column).

**File:** `clickhouse/schema/006-009_basedirs*.sql`, `clickhouse/basedirs_reader.go`
**Test file:** `clickhouse/basedirs_reader_test.go` (existing, updated)

**Acceptance tests:**

1. Given a snapshot, when `GroupUsage`/`UserUsage`/`GroupSubDirs`/`UserSubDirs`/
   `History` are called, then results (paths, ordering by `pos`, numbers) match
   baseline.
2. Given an in-snapshot active basedir with no resolvable `dir_id`, when
   readiness runs, then it fails with `ErrIDUnresolved` (not a silent bad row).
3. Given an external/historical basedir path absent from the catalog, when
   queried, then it resolves via the external column and is returned correctly.

### G2: per-active_set_id virtual catalog with its own id space

As a server, I want the cross-mount virtual namespace (`/`, `/lustre/`, `/nfs/`,
intermediate virtual parents, mount-root boxes) to keep its own small id space
that links to underlying mount-root `dir_id`s, so virtual navigation works
without copying mount-local paths into a second namespace.

- A per-`active_set_id` virtual catalog (`wrstat_active_virtual_dirs`) numbers
  ONLY the synthetic virtual nodes with `virtual_id` (its own small id space),
  each linking to a `(mount_path, snapshot_id, mount_root_dir_id)` when it is a
  mount-root box. Below a mount root, virtual navigation defers to that mount's
  per-snapshot catalog and `dir_id` ranges - no mount-local dir is copied.
- `wrstat_active_virtual_summaries`/`_filter_all`/`_children`/`_sets` are keyed
  by `active_set_id` + `virtual_id` (replacing `dir`/`parent_dir` strings for
  synthetic nodes; mount-root boxes carry the mount-local `dir_id` link). The
  above-root linear chain stays in the per-mount catalog (B2), NOT here.

**File:** `clickhouse/schema/018_active_virtual_overlay.sql`,
`clickhouse/virtual_namespace.go`
**Test file:** `clickhouse/virtual_namespace_test.go` (new)

**Acceptance tests:**

1. Given selected active Lustre and NFS mounts, when root `/` is summarised, then
   it aggregates every selected mount with separate `/lustre` and `/nfs` boxes and
   correct totals (matches baseline).
2. Given an active set, when virtual children, filtered virtual summaries, and
   active-prefix rollups are read, then they match baseline outputs.
3. Given the virtual catalog, when inspected, then it contains only synthetic
   virtual nodes (no copied mount-local directory rows).

---

## H. Ingest pipeline and spool carrying ids

### H1: spool format carries one catalog stream + id-keyed rows

As an ingest author, I want the spool to carry `dir_id` integers and one catalog
stream per mount snapshot, not repeated path strings, so spool bytes drop sharply.

- Add a `wrstat_dirs` catalog stream to `internal/chspool`: rows `(dir_id,
  parent_id, subtree_end, depth, name, full_path, child_dir_count,
  child_file_count, path_hash)` - `full_path` stored once per directory.
- `FileRow` replaces `ParentDir string` with `DirID uint32` (keeps `Name`).
- `DirFactRow` replaces `Dir string` with `DirID uint32` (+ `SubtreeEnd`).
- `ChildFilterAllRow`/`DirFilterAllRow`/`DirFilterAgeAllRow` replace path strings
  with `ParentID`/`DirID`/`SubtreeEnd`.
- `ParentFactRow` and the `wrstat_children`/`wrstat_parent_facts` streams are
  removed (catalog band replaces them).
- Basedirs/active-virtual rows carry ids per G1/G2 (+ external string fallback).
- Keep `Format`/`Manifest`/`TableManifest` machinery and the deterministic table
  order; add the catalog stream to that order; update per-table `Rows` counters.

**File:** `internal/chspool/spool.go`
**Test file:** `internal/chspool/spool_test.go` (existing, updated)

### H2: writers and loader emit/load id rows

As an ingest author, I want `dguta_writer.go`, `file_ingest_operation.go`,
`mount_dir_projection_writer.go`, `import_block_writer.go`, and
`summarise_spool_loader.go` to write/load the id-based rows, keeping the
resumable/retry and readiness-set machinery.

- A new catalog writer (`clickhouse/catalog.go`) buffers and inserts `wrstat_dirs`
  rows from `RecordDGUTA`'s id fields; the dguta writer drives it.
- File ingest writes `dir_id` (looked up from the current directory's id during
  the walk) + `name`, no `parent_dir`.
- `mount_dir_projection_writer` writes facts rows keyed by `dir_id` + `subtree_end`.
- Derived-index writers emit numeric filter rows (D2).
- `import_block_writer` (batch open-duration, send-on-full/too-old, abort-on-error)
  unchanged in mechanism; row counters updated for the new tables.
- `summarise_spool_loader` loads the catalog stream first, then id-keyed tables;
  the readiness/manifest (`wrstat_schema3_snapshot_sets`,
  `wrstat_dir_projection_sets`) records the new tables' row counts + SHA256.
- `DGUTAWriter` interface (`Add, SetBatchSize, SetMountPath, SetUpdatedAt, Close`)
  is preserved; ids flow via `RecordDGUTA` (no signature change). `SetMountPath`
  still sets D up front for B2.

**Files:** `clickhouse/dguta_writer.go`, `file_ingest_operation.go`,
`mount_dir_projection_writer.go`, `import_block_writer.go`,
`summarise_spool_loader.go`, `catalog.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go` (existing, updated)

**Acceptance tests:**

1. Given a full summarise -> spool -> load cycle, when queries run against the
   loaded data, then all results match baseline (correctness).
2. Given the same input, when the spool is written twice, then per-table byte
   contents are identical (determinism, enabled by B1/B2 ids).
3. Given the benchmark, when spool bytes are measured, then total spool bytes and
   path-text bytes are reported vs baseline with the reduction quantified.
4. Given a write interrupted mid-batch and retried, when resumed, then the
   readiness machinery produces a complete, correct snapshot (resume preserved).

---

## I. (Optional, flag-gated) in-memory navigation index

### I1: compact in-process catalog index

As a server operator, I want an optional in-memory index (`dir_id -> parent_id,
name, subtree_end, child_dir_count, child_file_count`) so parent/child/
has-children/ancestor/full-path navigation answers without a ClickHouse
round-trip, leaving ClickHouse for heavy DGUTA aggregations.

- Flag-gated (`--nav-index`), non-blocking, built asynchronously per active
  snapshot; queries fall back to ClickHouse until it is ready. It MUST NOT gate or
  delay the core schema/benchmark work; the core is complete and correct without
  it.
- Include a memory/build-cost estimate: per dir ~ `4 (dir_id implicit) + 4
  (parent_id) + 4 (subtree_end) + 4 (counts) + len(name)` bytes plus map overhead;
  for 10-100M dirs estimate low-GB; report the measured figure.

**File:** `clickhouse/navindex.go` (new)
**Test file:** `clickhouse/navindex_test.go` (new)

**Acceptance tests:**

1. Given the flag off, when navigation queries run, then behaviour and results are
   identical to the ClickHouse-only path (no regression).
2. Given the flag on and the index built, when `Children`/`DirsHaveChildren`/
   ancestor/full-path queries run, then results match the ClickHouse path exactly
   with no ClickHouse round-trip (verified by query-count assertion).
3. Given the flag on but the index not yet built, when a navigation query runs,
   then it transparently falls back to ClickHouse and returns correct results.
4. Given the benchmark, when the index is enabled, then its memory and build cost
   are reported and its per-query latency delta vs the ClickHouse path is stated.

---

## J. Mandatory before/after benchmark (the gate)

### J1: baseline capture and harness reuse

As a maintainer, I want the current `clickhouse` branch HEAD captured as baseline
before any change, using the existing harness, so the comparison is rigorous.

- Baseline = current `clickhouse` HEAD, captured before any overhaul change
  (preserve a built binary or a separate worktree under `.tmp/agent/overhaul/`).
- Reuse and extend `internal/chperf/` (`import.go`, `query.go`, `final_gate.go`,
  `clickhouse_api.go`) and the Bolt comparison harness. Do NOT invent a new report
  format; keep the existing `perfreport.Report`/`Operation`/`TableStats`/
  `QueryMetrics` structures.

### J2: datasets

At minimum: a mixed Lustre/NFS subset; a directory-heavy NFS tree; a high-fanout
parent (~11k direct children); a many-small-mounts active-virtual simulation
keeping `/nfs` virtual; and the largest practical production-like local dataset.
State which datasets are used and why a bounded subset still reproduces each
access pattern. If a bounded dataset cannot reproduce a pattern, say so.

### J3: import / storage metrics

Report: summarise wall time, CPU, max RSS; spool bytes; ClickHouse bytes per
table (compressed AND uncompressed via `TableStats.CompressedBytes` /
`UncompressedBytes`); active part counts (`ActiveParts`); rows per table; and the
explicit path-text-bytes-before-vs-after duplication reduction. Record the
measured max-dirs-per-snapshot (justifies `UInt32`).

### J4: per-query-type metrics and the canonical matrix

Per query type report server-side p50/p95/p99 latency, rows read, bytes read,
granules/marks read (`ReadRows`/`ReadBytes`/`ReadMarks`), result rows, and a
result digest for correctness. Every type in this matrix MUST be benchmarked with
a per-type before/after delta; missing one is a spec failure:

| Group | Query types |
| --- | --- |
| Exact directory | `DirInfo` broad; filtered (gid/uid/ft/age + combos) |
| Batch directory | `DirInfos` broad; focused/high-fanout; filtered |
| Children/presence | `Children`; `DirsHaveChildren` broad; filtered |
| Subtree/recursive | `Where` broad; filtered; `where --dir`; auth-restricted |
| Disktree | root `/`; `/lustre/`; `/nfs/`; a mount root; high-fanout parent; with type/owner/age filters |
| File API | `StatPath`; `ListDir`; `IsDir`; `PermissionPath`; `PermissionAnyInDir` |
| Glob/full-text | `FindByGlob` direct-child/recursive/extension/dotfile; `CountByGlob` |
| Virtual/active | active virtual root summaries; filtered; virtual children; active-prefix rollups |
| Basedirs/quota | `GroupUsage`; `UserUsage`; `GroupSubDirs`; `UserSubDirs`; history |
| Maintenance | import readiness/publish; active-snapshot cleanup; `Info` |

### J5: cache scopes

Reproduce the existing harness's scopes so the high-fanout repeated-read
pathology is exercised before and after: `fresh_provider_per_repeat`,
`cold_provider_with_cold_query_cache`, `same_provider_cold_then_warm`,
`same_query_client`, `ancestor_directory_each_repeat`, `new_directory_each_repeat`,
`visible_child_directory_each_repeat`, `startup_cache_warming_contract`,
`provider_update_cold_cache`, `same_provider_same_dir`.

### J6: gates

- **Correctness (hard):** every query type returns results identical to baseline
  (paths, child ordering, full paths, filter/permission semantics, multi-source/
  active merge, glob semantics, basedirs/quota numbers, manifests). No wrong row
  ever (path-hash collisions verified, C2.4).
- **Absolute cold UX (hard):** exact dir resolution, exact file stat, permission
  path, direct-child list -> p95 < 100 ms; recursive subtree, filtered, glob,
  Disktree, `Where` -> p95 < 500 ms.
- **Storage (hard):** no hot row in any table stores a `dir`/`parent_dir`/path
  string; path text reduced to one copy per directory per snapshot in the catalog.
  Filter tables remain (numeric/`dir_id`-keyed) unless a pattern's materialisation
  was collapsed under D4 with a cited measurement. Report compressed and
  uncompressed bytes per table vs baseline.
- **Relative performance (report, not pass/fail):** per-query before/after delta
  for every type; regressions quantified and explained, and must still satisfy the
  absolute UX gate to ship.

**File:** `internal/chperf/` (extended), `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/*_test.go` (existing, updated)

**Acceptance tests:**

1. Given baseline and overhaul reports, when the comparison runs, then every
   matrix query type has a recorded p50/p95/p99 and a before/after delta; a
   missing type fails the gate.
2. Given the overhaul report, when storage is inspected, then no hot row stores a
   path string and per-table compressed/uncompressed bytes are reported vs
   baseline.
3. Given the cold UX gate, when the relevant query types run, then p95 latencies
   satisfy the < 100 ms / < 500 ms thresholds.
4. Given any collapsed materialisation, when the report is read, then a cited
   measurement justifies the collapse and the in-query route meets its gate.

---

## Implementation Order

1. **Catalog + ids in the summariser** (A, B): preorder id assignment in the DFS
   walk incl. the above-root reserved low-id block; emit `wrstat_dirs`. Prove
   determinism and the interval invariant. Foundation for everything.
2. **Spool + writers carry ids** (H): rewrite spool format and ClickHouse writers/
   loader to emit the catalog + id-keyed rows; keep readiness/resume.
3. **Files table on `dir_id`** (C): rewrite `wrstat_files` and the file API.
4. **Facts + numeric filter tables on `dir_id`** (D, E): rewrite `wrstat_dir_facts`
   keyed by `dir_id`; numeric filter tables (retained); the in-query vector path;
   rewrite `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where`/`Children`.
5. **Glob / full-text** (F): catalog skip indexes.
6. **Basedirs + active virtual namespace** (G): id world + external fallback +
   virtual id space.
7. **Benchmark study** (J): baseline capture, full matrix, import/storage, gates,
   per-type deltas, per-pattern collapse decisions. Mandatory gate.
8. **(Optional)** in-memory navigation index (I): flag-gated, measured,
   non-blocking.

Phases 3-6 may proceed in parallel once 1-2 land (shared catalog, different
tables/queries). Phase 7 is the acceptance gate. Phase 8 must never delay 1-7.

---

## Appendix: Key Decisions

- **`UInt32` ids by default**, validated against the largest dataset's
  max-dirs-per-snapshot; widen to `UInt64` only if approaching 2^31. Codecs:
  `Delta, LZ4` for ids (dense/monotonic), `ZSTD(3)` for cold `full_path`/`name`.
- **`path_hash` primary resolver with mandatory `full_path` verification on hit**
  (exact resolution is under the hard 100 ms gate); droppable only if the
  `full_path` projection alone meets the gate. Verification makes collisions
  impossible to surface as wrong rows.
- **Filter materialisations retained by default**, made numeric; the in-query
  vector path is implemented for head-to-head benchmarking; collapse only with a
  cited measurement (D4). This is the prompt's explicit safer-than-trie stance.
- **`wrstat_parent_facts`, `wrstat_children`, and `wrstat_virtual_children`
  (string) are removed** - replaced by the catalog `parent_id` band, `subtree_end`
  ranges, and the virtual id space. This is not a filter-table collapse; it is the
  dedup core.
- **Above-root ancestor rows stay in the per-mount catalog** with a reserved
  low-id block (B2), preserving today's single-mount serving behaviour; they are
  not moved into the `active_set_id` layer.
- **Testing strategy:** GoConvey throughout (`So(...)`); `t.TempDir()` for FS;
  fixtures updated to the new schema; new tests prove the interval invariant,
  deterministic ids, path-hash collision safety, and materialised-vs-in-query
  filter parity. Memory-bounded tests for the streaming id assignment and spool.
  Implementors follow go-conventions; reviewers verify every acceptance test maps
  to a real GoConvey test (no stubs, no build-tag exclusions).
- **Error policy:** `ErrDirNotFound` semantics preserved; `ErrIDUnresolved` for an
  in-snapshot path that fails to resolve during readiness (readiness fails rather
  than emit a bad row).
- **Scratch artefacts** (prototype SQL, throwaway binaries, baseline binary,
  reports) live under `.tmp/agent/overhaul/`; baseline artefacts preserved for the
  before/after comparison.
