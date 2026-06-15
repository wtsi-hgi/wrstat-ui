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
(nested-set) labelling**: during the summariser's existing depth-first walk
every directory gets a dense integer `dir_id` assigned in first-visit order,
recording `parent_id` and `subtree_end` (smallest `dir_id` greater than every
descendant). A directory's whole subtree is then the contiguous range `[dir_id,
subtree_end)`; its direct children share `parent_id`. The path string is stored
exactly once per `(mount_path, snapshot_id)` in a directory catalog keyed by
`dir_id`; every other table stores `dir_id`, never the path.

This collapses dedup, parent/child navigation, exact lookup, broad subtree
scans, and full-path reconstruction to integer primary-key operations. The
filter story is deliberately conservative: the `dir_id`-keyed numeric filter
materialisations are **kept by default** (the path strings, not the rows, were
the duplication); an in-query vector-filter path is also implemented; a
specific materialisation is collapsed only where the mandatory before/after
benchmark proves the in-query path meets the latency gate for that pattern. The
external API (`db/interfaces.go`, `clickhouse.Client` file API, basedirs
readers) is unchanged.

## Architecture

### Packages and files

- `summary/` (`summariser.go`): unchanged DFS walk; `DirectoryPath{Name, Depth,
  Parent}`, `FileInfo`, `Operation{Add, Output}`, `OperationGenerator`.
- `summary/dirguta/`: assigns `dir_id`/`parent_id`/`subtree_end` during the
  walk and carries them on the emitted record (area B); drives the shared
  `DirIDAllocator` (B5) on each directory enter/`Output`.
- `summary/idalloc.go` (new): the shared `DirIDAllocator` (B5) - preorder
  counter, reserved low-id block, and current-directory id lookup. Lives in the
  `summary` package (not `clickhouse`/`dirguta`) so the DGUTA directory
  operation and the file-ingest global operation can share one instance with no
  import cycle.
- `cmd/summarise.go`: constructs one `DirIDAllocator` per summarise run, calls
  `SetMountPath` to reserve the above-root block (B2), and injects it into both
  `dirguta.NewDirGroupUserTypeAge` and `clickhouse.NewFileIngestOperation` (B5).
- `db/` (`interfaces.go`, `dguta.go`): `RecordDGUTA` (currently `{Dir
  *summary.DirectoryPath; GUTAs; Children []string; ChildCount uint64}`) gains
  the id fields (B3); `summary.DirectoryPath{Name, Depth, Parent}` is unchanged
  (no id field added to it). The `Database`, `DGUTAWriter`, `DirInfoBatcher`,
  `DirHasChildrenBatcher`, `Whereer` interfaces and
  `DirSummary`/`DirInfo`/`Filter`/`Info` types are unchanged externally.
- `clickhouse/schema/*.sql`: rewritten DDL (areas A, C, D, G). See "Schema
  file set after the rewrite" below for the exact deleted/renumbered files.
- `clickhouse/` writers and query layer: rewritten to id-keyed rows (areas C-H).
- `clickhouse/catalog.go` (new): catalog write buffer + path<->id resolver.
- `clickhouse/navindex.go` (new, optional, flag-gated): in-memory nav index
  (area I).
- `cmd/summarise_spool.go`: the production spool PRODUCER.
  `summariseFileSpoolOperation` and `summariseDGUTASpoolWriter` (wired by
  `addSummariseSpoolOperations` via `dirguta.NewDirGroupUserTypeAgeAt` +
  `summariseFileSpoolOperation.operation`) write the `internal/chspool` row
  structs; rewritten to emit id-keyed rows and drop the deleted
  children/parent-facts plumbing (areas H1, H2, B3).
- `internal/chspool/`: spool carries one catalog stream + id-keyed rows (area
  H).
- `internal/chperf/` (`import.go`): extended benchmark harness; baseline capture
  (gate). The children/parent-facts harness plumbing is removed alongside the
  deleted tables (area J1).

### ID type and keying (Notes: snapshot/mount keying)

- `dir_id`, `parent_id`, `subtree_end`: `UInt32`. Justification: max dirs per
  `(mount_path, snapshot_id)` is bounded well under 2^32 on production trees;
  validate against the largest benchmark dataset and widen to `UInt64` only if
  the catalog row count for any single snapshot approaches 2^31. Record the
  measured max-dirs-per-snapshot in the benchmark report.
- Overflow is a hard runtime guard, not only a design-time check: the allocator
  (B5) MUST return `ErrTooManyDirs` and abort the import if the preorder counter
  would reach `parentSentinel` (`0xFFFFFFFF`). Real ids therefore never exceed
  `0xFFFFFFFE` and can never collide with the sentinel, even at the `UInt32`
  maximum. Widening to `UInt64` raises the ceiling but does not remove the
  guard.
- `dir_id` is unique only within `(mount_path, snapshot_id)`. Every id-keyed
  table carries `(mount_path LowCardinality(String), snapshot_id UUID, dir_id
  UInt32)` and keeps today's `PARTITION BY (mount_path, snapshot_id)`. `dir_id`
  replaces the path column in the sort key.
- A compact integer `mount_key`/`snapshot_key` replacing the `(mount_path,
  snapshot_id)` pair in hot rows is adopted **only** if the benchmark shows it
  materially reduces part sizes; default to the simple model.
- Codecs: `dir_id`/`parent_id`/`subtree_end` use `CODEC(Delta, LZ4)` (dense,
  monotonic in sort order). `full_path` is cold: `CODEC(ZSTD(3))`. Validate
  codec choices against measured part sizes (prompt Notes).

### Sentinels

- The mount catalog is rooted at filesystem `/` (area A, Notes: above-root
  ancestor chain). `/`'s `parent_id` is the sentinel `parentSentinel =
  0xFFFFFFFF` (`dir_id` 0 is `/` itself, so the sentinel must not collide with a
  real id). The B5 overflow guard enforces this: ids are handed out from 0 and
  the import aborts before any id reaches `0xFFFFFFFF`, so `parentSentinel` is
  never a valid `dir_id`.

### Error handling

- Sentinel errors as package `var` with `errors.New`; wrap with `%w`. Reuse the
  existing `ErrDirNotFound`. Add `ErrIDUnresolved` for an in-snapshot path that
  cannot be resolved to a `dir_id` during readiness (area G fallback). Add
  `ErrTooManyDirs` for `dir_id` overflow (B5, ID type and keying) and
  `ErrNonContiguousInput` for a re-entered directory boundary that would break
  the preorder interval invariant (B1).
- Path-hash hits MUST verify `full_path` before returning (Notes); a mismatch is
  treated as a miss, never a wrong result.

### Schema file set after the rewrite

The current `clickhouse/schema/` is (verified):

```text
001_schema_version.sql        010_basedirs_history.sql
002_mount_events.sql          011_files.sql
003_mounts_active.sql         012_dir_filter_ageall.sql
004_dir_facts.sql             013_active_prefix_rollups.sql
005_children.sql              014_parent_facts.sql
006_basedirs_group_usage.sql  015_child_filter_all.sql
006_dir_projection_sets.sql   016_dir_filter_all.sql
007_basedirs_user_usage.sql   017_schema3_snapshot_sets.sql
007_virtual_children.sql      018_active_virtual_overlay.sql
008_basedirs_group_subdirs.sql
008_virtual_children_sets.sql
009_basedirs_user_subdirs.sql
```

(The repo currently has duplicate numbers `006`/`007`/`008`; the rewrite also
fixes that so no two files share a number.) After the rewrite the set is
exactly:

- **Deleted** (string-keyed tables removed by the dedup core, not a filter
  collapse): `005_children.sql` (`wrstat_children`), `014_parent_facts.sql`
  (`wrstat_parent_facts`), `007_virtual_children.sql` +
  `008_virtual_children_sets.sql` (`wrstat_virtual_children` and its set table -
  the string virtual-children edges; replaced by the virtual id space, G2).
- **New:** `004_dirs.sql` (the `wrstat_dirs` catalog, A1). This is why
  `wrstat_dir_facts` moves off `004` to `005`. Also new: the per-`active_set_id`
  virtual catalog `wrstat_active_virtual_dirs` (its own `virtual_id` space,
  G2) - appended into the rewritten `018_active_virtual_overlay.sql` (which
  currently defines `wrstat_active_virtual_summaries`/`_filter_all`/`_children`/
  `_sets`, not the deleted string table `wrstat_virtual_children`). It needs no
  new file number; if an implementor prefers a separate file it takes the next
  free number (e.g. `020`).
- **Rewritten in place, same filename/number:** `011_files.sql` (C1),
  `012_dir_filter_ageall.sql`, `015_child_filter_all.sql`,
  `016_dir_filter_all.sql` (D2), `006_basedirs_group_usage.sql`,
  `007_basedirs_user_usage.sql`, `008_basedirs_group_subdirs.sql`,
  `009_basedirs_user_subdirs.sql` (G1), `018_active_virtual_overlay.sql` (G2),
  `013_active_prefix_rollups.sql` (G2). `010_basedirs_history.sql` keeps
  strings (G1).
- **Renumbered:** the old `004_dir_facts.sql` (`wrstat_dir_facts`) becomes
  `005_dir_facts.sql` (D1), freeing `004` for the new catalog. The bookkeeping
  files (`006_dir_projection_sets.sql`, `017_schema3_snapshot_sets.sql`) and
  `001`-`003` are unchanged in number; `006_dir_projection_sets.sql` keeps its
  number (the `006` clash with `006_basedirs_group_usage.sql` is resolved by
  renumbering the projection-sets file - implementor's choice of free number,
  e.g. `019`, as long as no two files share a number).

Net result: exactly one file per number, `004_dirs.sql` is the catalog and
`005_dir_facts.sql` is the facts table; no `004`/`005` conflict remains.

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
  primary path->id resolver; the `path_hash_proj` projection serves it. Every
  hit re-reads `full_path` and compares before returning (collision safety,
  Notes). The benchmark may drop `path_hash` + projection if the `full_path`
  skip-index / ordering alone meets the 100 ms p95 gate.
- `name` is basename only (keeps the trailing `/` convention the summariser uses
  for directories). `full_path` is the single stored copy of the full path.
- `child_dir_count` / `child_file_count` make "has children" a column read.

**Test file:** `clickhouse/dirs_catalog_test.go`

**Acceptance tests:**

1. Given a snapshot with N distinct directories, when the catalog is read, then
   `SELECT count() FROM wrstat_dirs WHERE mount_path=? AND snapshot_id=?` equals
   N and `SELECT count(DISTINCT full_path)` also equals N (no dir stored twice).
2. Given any catalog row R, when its descendants are computed as `dir_id IN
   [R.dir_id, R.subtree_end)`, then that set equals the set reachable by
   recursively following `parent_id = R.dir_id` (interval invariant: descendant
   <=> `dir_id in [root_id, subtree_end)`).
3. Given the mount root `/`, when its row is read, then `parent_id =
   0xFFFFFFFF`.
4. Given any `dir_id`, when its full path is reconstructed via stored
   `full_path` and via the `parent_id` walk (concatenating `name` from `/`
   down), then the two byte-strings are identical.

---

## B. ID assignment during summarise (deterministic, streaming)

### B1: preorder id assignment in the DFS walk

As an ingest author, I want `dir_id`/`parent_id`/`subtree_end` assigned during
the existing depth-first walk using only an ancestor stack, so that ids are a
near-free byproduct and the spool can carry ids not strings.

The `stats` parser already emits directories DFS-contiguously (it synthesises
intermediate directory entries, deepest-first, between divergence points), so a
directory and all its descendants form one uninterrupted run. Files interleave
with sibling subdirectories under lexicographic ordering, so **id assignment
must key off directory boundaries (`info.IsDir()` / `DirectoryPath` push/pop),
not raw entry order**.

Algorithm (in `summary/dirguta`, hooked into the operation lifecycle so it sees
each directory's enter and `Output`):

- A monotonic counter `next` (start value set per B2). On the first `Add` for a
  directory (when `thisDir` is set, i.e. at a directory boundary), assign
  `dir_id = next; next++`, record `parent_id` from the parent operation, and
  take `depth` directly from the directory's `summary.DirectoryPath.Depth` (the
  existing linked-list depth field - not a new field).
- On that directory's `Output` (subtree complete, popped), set `subtree_end =
  next` (the next id not yet handed out is the smallest id greater than every
  descendant).
- Carry the assigned `dir_id`/`parent_id`/`subtree_end`/`depth` on the emitted
  `RecordDGUTA` so all writers see them (B3).

These ids are derived purely from the walk (directory enter/`Output`
boundaries and `Dir.Depth`) and the assignment counter; they are not read from
any new field on `DirectoryPath`.

This requires no full-tree buffering: state is the ancestor chain (depth <=
~128) plus the counter. Determinism: same input stream -> same `next` sequence
-> same ids (no maps, no time, no concurrency in the id path).

Precondition (made explicit, and guarded): the assignment relies on the stats
stream being subtree-contiguous - each directory is entered exactly once, with
its whole subtree between its enter and `Output`. This is not a new assumption:
the existing DGUTA roll-up already requires it (a re-entered directory merges
two partial stores into its parent and emits a duplicate record). The new
`subtree_end` ranges raise the cost of a violation from a duplicate row to wrong
results across a whole subtree, so the allocator MUST defend it: if a directory
boundary is entered whose preorder id was already assigned (re-entry), or the
depth sequence is inconsistent with a single DFS, it returns
`ErrNonContiguousInput` and aborts rather than silently mislabel `subtree_end`.

**Package:** `summary/dirguta/`
**File:** `summary/dirguta/dirguta.go`
**Test file:** `summary/dirguta/idassign_test.go`

**Acceptance tests:**

1. Given a tree `/a/{b/{f1}, c/{f2}}` fed in DFS order with files interleaved
   among subdirectories, when summarised, then ids are gap-free preorder:
   `a`<`b`, `b`'s `subtree_end` <= `c`'s `dir_id`, and `a.subtree_end` = max
   child `subtree_end`.
2. Given the same input twice, when summarised twice, then every emitted
   `(full_path, dir_id, parent_id, subtree_end)` tuple is byte-for-byte
   identical (determinism).
3. Given a directory with a file lexicographically between two subdirectories,
   when summarised, then the file does not consume a `dir_id` and the two
   subdirectories receive consecutive-subtree ids.
4. Given any directory D, when ids are assigned, then `[D.dir_id,
   D.subtree_end)` contains exactly D plus all its descendant directories
   (interval invariant on a real fixture dataset).
5. Given an allocator whose counter is one below `parentSentinel`, when the next
   directory would be assigned `0xFFFFFFFF`, then assignment returns
   `ErrTooManyDirs`, the import aborts, and no row is emitted with a
   sentinel-valued `dir_id`.
6. Given a stats stream that re-enters an already-closed directory boundary (a
   non-contiguous subtree), when ids are assigned, then the allocator returns
   `ErrNonContiguousInput` rather than emitting a second `dir_id` for that
   directory or a `subtree_end` that excludes part of its subtree.

### B2: above-root ancestor reserved low-id block

As an ingest author, I want the above-root ancestor chain (`/`, `/lustre/`,
..., `mountPath`'s parent, then `mountPath`) to occupy a reserved low `dir_id`
block, so that single-mount queries that hit the per-mount tables keep seeing
those rows and the interval invariant holds for the whole chain.

`SetMountPath` is called before any `Add`, so `mountPath`'s depth D (slash
count) is known up front. Reserve ids `0..D` for the linear chain in preorder:
`/` = 0, `/lustre/` = 1, ..., `mountPath`'s parent = D-1, the data root
`mountPath` = D. Start the data-root subtree counter at D+1.

The ancestor rows are emitted out of order at end-of-walk (deepest-first, via
`dirguta.outputRoot`), but their ids are fixed by the reservation (each
ancestor's `dir_id` = its depth). Backfill each ancestor's `subtree_end` to the
end-of-snapshot bound `next` (final counter value): the linear chain contains
the entire data-root subtree, so every ancestor's interval spans the whole
snapshot. `/`'s `parent_id` = sentinel.

The handful of duplicated linear-chain rows per snapshot is acceptable and
matches current behaviour. These rows stay in the per-`(mount_path,
snapshot_id)` catalog; they are **not** moved into the `active_set_id` virtual
layer.

**File:** `summary/dirguta/dirguta.go` (`outputRoot` path)
**Test file:** `summary/dirguta/idassign_test.go`

**Acceptance tests:**

1. Given `mountPath = /lustre/scratch125/teamX/` (depth D=3), when summarised,
   then `/`=0, `/lustre/`=1, `/lustre/scratch125/`=2,
   `/lustre/scratch125/teamX/`=3, and the first descendant directory under the
   data root has `dir_id` >= 4.
2. Given the completed snapshot with final counter `next`, when ancestor rows
   are read, then every ancestor (ids 0..D-1) has `subtree_end = next` and the
   interval invariant from A1 holds for each.
3. Given the catalog, when `/`'s row is read, then `parent_id = sentinel` and
   its `[0, next)` interval contains every catalog row.

### B3: ids carried to writers

As a writer author, I want the assigned ids on `RecordDGUTA`, so every table can
emit `dir_id` rows.

`db.RecordDGUTA` gains four fields - `DirID, ParentID, SubtreeEnd uint32` and
`Depth uint16` - alongside the existing `Dir *summary.DirectoryPath`, `GUTAs`,
`Children`, `ChildCount`. The ids are written by the B1 assignment during the
walk (derived from directory boundaries and `Dir.Depth`, see B1), not stored on
`summary.DirectoryPath`, which keeps its existing `{Name, Depth, Parent}` shape.
`Depth` duplicates `Dir.Depth` on the record for writers that do not walk the
`Dir` pointer. `db.GUTAs`/`DGUTA`/`Filter`/`DirSummary` are unchanged. The
`DGUTAWriter` interface (`Add, SetBatchSize, SetMountPath, SetUpdatedAt, Close`)
is unchanged.

The streaming child-edge machinery is **removed**: the optional
`db.DGUTAChildrenWriter` interface (`AddChildren(parent *summary.DirectoryPath,
children []string) error` in `db/dguta.go`), its `summary/dirguta` plumbing
(`childDB`/`streamChildren` fields, `streamChildren`/`flushChildren`, the
`d.(db.DGUTAChildrenWriter)` type assertion), and `dgutaWriter.AddChildren` in
`clickhouse/dguta_writer.go` all existed solely to populate the now-deleted
`wrstat_children` table and are deleted with it. The same deletion applies to
the production spool PRODUCER `summariseDGUTASpoolWriter` in
`cmd/summarise_spool.go`: its `AddChildren` (`AddChildren(parent
*summary.DirectoryPath, children []string)`), `appendChildrenRows`, the
`WriteChild`/`ChildRow` emission, and `writeParentFactRow` (emitting
`WriteParentFact`/`ParentFactRow`) - all feeding the deleted
`wrstat_children`/`wrstat_parent_facts` streams - are removed (H1, H2). The
catalog `parent_id` band (child listings, E1) plus the catalog
`child_dir_count` column (A1, "has children") replace them; no implementor wires
a live interface to a deleted table.

**File:** `db/dguta.go`, `cmd/summarise_spool.go`
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

### B5: shared id allocator visible to file ingest

As an ingest author, I want one `DirIDAllocator` shared by the DGUTA directory
operation and the file-ingest global operation, so the global op (which the
summariser runs before directory ops) can write each file's `dir_id` without
reconstructing a parent path string.

Today `cmd/summarise.go` registers DGUTA as a directory operation
(`AddDirectoryOperation`) and file ingest as a global operation
(`AddGlobalOperation`), and `summary.Summariser` calls `global.Add` before
`dirs.Add` for every entry (verified, `summariser.go`). File ingest currently
derives `parent_dir` from `info.Path` (verified, `fileIngestParentAndName`). The
id assignment (B1) lives in the DGUTA directory op, which runs after file
ingest, so file ingest cannot read an id that DGUTA assigns in the same entry. A
shared allocator resolves this:

- `summary.DirIDAllocator` (new, `summary/idalloc.go`) owns the preorder counter
  and the reserved low-id block. `cmd/summarise.go` creates one per run, calls
  `SetMountPath(mountPath)` to reserve ids `0..D` for the above-root chain (B2,
  counter starts at `D+1`), and injects it into both `NewDirGroupUserTypeAge`
  and `NewFileIngestOperation`. The allocator is optional: the Bolt summarise
  path passes nil and is unchanged (no ids assigned); only the ClickHouse path
  injects a real allocator.
- The DGUTA op drives it: on a directory's first `Add` (enter) it calls
  `alloc.Enter(dirPath)` (returns the `dir_id`: the reserved id when
  `depth <= D`, else `next++`); on `Output` it calls `alloc.Leave(dirPath)`.
  `Enter`/`Leave` maintain a `*summary.DirectoryPath` -> `dir_id` lookup for the
  live ancestor chain, releasing entries on `Leave`, so memory is bounded by
  depth.
- File ingest reads it: the containing directory of an entry is `info.Path` for
  a file and `info.Path.Parent` for a directory entry (matching the existing
  `canonicalFileIngestPath` parent/self-named-dir trim). Its `dir_id` is
  `alloc.DirID(thatPath)`, replacing the `parent_dir` string. `DirID` resolves
  reserved-block ancestors (depth <= D, e.g. `/m/`) by depth per B2; the
  `*DirectoryPath` -> `dir_id` lookup covers only the data-root subtree (depth >
  D).

Ordering safety (why global-before-directory is correct): an entry's containing
directory is always entered earlier in preorder than the entry itself - a
directory is emitted before its children, and a directory entry is filed under
its already-entered parent - so its id is assigned before file ingest reads it.
The above-root chain and data root are reserved at `SetMountPath`, so even the
first entry resolves. File ingest never needs an id that DGUTA assigns later in
the same entry; `DirID` on an unassigned directory errors (a bug guard, never
reached on contiguous input), it does not return a zero id.

**Package:** `summary/`, `clickhouse/`
**File:** `summary/idalloc.go`, `cmd/summarise.go`,
`clickhouse/file_ingest_operation.go`
**Test file:** `summary/idalloc_test.go`,
`clickhouse/file_ingest_operation_test.go` (existing, updated)

**Acceptance tests:**

1. Given a mount `/m/teamX/` with files in the data root and in nested dirs,
   when summarised, then every `wrstat_files` row's `dir_id` equals the catalog
   `dir_id` of its containing directory: data-root files use the reserved data
   root id `D`, nested files use their directory's assigned id.
2. Given a subdirectory entry `/m/teamX/sub/`, when its `wrstat_files` row is
   written, then its `dir_id` is the parent's id (a directory entry is filed
   under its parent), not the subdirectory's own id.
3. Given the mount-root directory entry itself, when its `wrstat_files` row is
   written, then its `dir_id` is the reserved parent id `D-1`, and no row
   carries an unassigned or zero `dir_id` from the global-before-directory
   order.
4. Given one allocator shared by both operations across a full walk, when the
   spool is written, then file-ingest `dir_id`s and DGUTA/catalog `dir_id`s
   agree for every directory (one id space, no divergence).

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
  display is the catalog `full_path` + `name` (resolved via a `dir_id` join,
  F3).

### C2: file API resolves path->dir_id then point-looks-up

As a server, I want `StatPath`/`IsDir`/`PermissionPath`/`ListDir`/recursive
enumeration unchanged externally, served via the catalog.

- `StatPath`/`IsDir`/`PermissionPath`: resolve `(mount, parentDir)` -> `dir_id`
  via the catalog (path_hash + verify, or full_path projection), then `WHERE
  mount_path=? AND snapshot_id=sid AND dir_id=? AND name=? LIMIT 1`.
- `ListDir`: resolve dir -> `dir_id`, then `WHERE dir_id=? ORDER BY name ASC
  LIMIT ? OFFSET ?`.
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
5. Given active mount `/m/teamX/` (data root `dir_id = D`; its directory entry
   is filed under the reserved parent `dir_id = D-1` with `name = "teamX/"`,
   `uid=30`, `gid=40`), when `StatPath(ctx, "/m/teamX/", StatOptions{})` is
   called, then it returns no invalid-path or not-found error and the row has
   `Path="/m/teamX/"`, `ParentDir="/m/"`, `Name="teamX/"`,
   `EntryType=byte(stats.DirType)`, `UID=30`, and `GID=40`.
6. Given the same mount with direct children `a.txt`, `b.txt`, and `z/` filed
   under `parent_id = D`, when `ListDir(ctx, "/m/teamX/", ListOptions{Limit: 2,
   Offset: 1})` is called, then names are exactly `["b.txt", "z/"]`, every row
   has `ParentDir="/m/teamX/"`, and the query uses `dir_id = D`.
7. Given the same mount root, with root directory facts for `dir_id = D`
   including age-all `(uid=30, gid=40)`, when `PermissionPath(ctx, "/m/teamX/",
   30, [])` runs it returns `true`; with `(uid=31, gids=[40])` `true`; with
   `(uid=31, gids=[41])` `false` (the mount-root directory entry row under
   `D-1`, not `wrstat_dir_facts`, supplies the permission fields); and
   `PermissionAnyInDir(ctx, "/m/teamX/", 31, []uint32{40})` returns `true`
   (resolving the mount root to `dir_id = D`) without an invalid-path or
   not-found error.

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
String` is replaced by `dir_id UInt32 CODEC(Delta, LZ4)`, and both `parent_id
UInt32 CODEC(Delta, LZ4)` and `subtree_end UInt32 CODEC(Delta, LZ4)` are added.
`parent_id` makes direct-child fact reads a contiguous band (it is carried on
`RecordDGUTA`, B3); `subtree_end` makes subtree range scans need no catalog
join. `ORDER BY (mount_path, snapshot_id, dir_id)`, plus a benchmark-gated
`children_proj` projection `ORDER BY (mount_path, snapshot_id, parent_id,
dir_id)` to serve the `parent_id` band. Keep the scalar `all_*`/`file_*`
columns (they serve broad unfiltered `DirInfo` without array work); the
benchmark decides if any are redundant, including whether to drop
`children_proj` in favour of resolving child `dir_id`s from the catalog
`children_proj` then batching facts by `dir_id IN (...)`.

`wrstat_parent_facts` is removed: direct-child fact reads are served by this
facts table's `parent_id` band (the added `parent_id` column + `children_proj`,
E1) or the numeric child-filter table (D2); recursive subtree reads use the
`subtree_end` range. The `has_children` need is met by `child_dir_count` on the
catalog.

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
  `parent_id`-keyed `wrstat_child_filter_all` band, or the contiguous
  `parent_id` band of facts rows filtered in-query. High-fanout parents (~11k
  children) read one contiguous range. Likeliest table to retain.
- **Filtered subtree** (`Where`, Disktree, `where --dir`): `(dir_id,
  subtree_end)` range over `wrstat_dir_filter_all`/`_ageall`, or facts-table
  range scan emitting per-node filtered aggregates above the recurse threshold.

A materialisation is dropped only when the benchmark proves the in-query path
meets the latency gate for that pattern x dataset x fanout; the collapse cites
the measurement. An unproven collapse is a spec failure. If a bounded local
dataset cannot reproduce a pattern, that is stated explicitly and the
materialisation is retained.

**Test file:** `clickhouse/filter_parity_test.go`

**Acceptance tests:**

1. Given the benchmark filter matrix (each of gid, uid, ft, age, and combos),
   when the materialised-table route and the in-query vector route serve the
   same filtered query, then their results are bit-for-bit identical (parity).
2. Given any filter table, when its columns are inspected, then none stores a
   `dir`/`parent_dir`/path string.
3. Given filtered `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where` against a
   fixture, when compared to baseline output, then results match exactly (same
   summaries, child ordering, full paths).
4. Given the benchmark report, when a materialisation is marked collapsed, then
   a cited measurement justifies it and the in-query route still meets the
   latency gate for that pattern.

---

## E. Parent / child / ancestor / full-path resolution

### E1: `Children` and `DirInfos`/`DirInfo` via integer bands

As a server, I want `Children`, `DirInfo`, `DirInfos` served by integer lookups,
with external semantics unchanged.

- `Children(dir)`: resolve `dir_id`, read the catalog `parent_id = dir_id` band
  (via the `children_proj` projection, A1), return each child's `full_path`,
  de-duplicated and sorted across sources (unchanged semantics). With
  `wrstat_children` and `wrstat_parent_facts` removed (H1, Key Decisions), this
  catalog `parent_id` band is the **sole** source for child listings within a
  snapshot; the multi-source/active-mount merge still reads each source's band
  and applies the same cross-source de-dup and sort, so external semantics are
  unchanged.
- `DirInfo(dir, filter)`: resolve `dir_id`, read the facts row, merge across
  sources, apply filter, set `Modtime` to latest `updated_at` across sources;
  `ErrDirNotFound` only if missing from all sources.
- `DirInfos(dirs, filter)`: resolve dirs -> `dir_id IN (...)`, batch facts read.

Every `FROM wrstat_children` query-layer READER is **retired** and rewritten to
the catalog `parent_id` band / facts table (no surviving `wrstat_children`
read):

- `clickhouse/database.go`: `childrenQuery` (~line 58),
  `childrenForParentsQuery` (~line 209), `childrenForExternalParentsQuery`
  (~line 215), `activeMountRootChildrenQuery` (~line 221), and
  `dirsHaveMatchingChildrenQuery` (~line 226) - all replaced by the catalog
  `parent_id = dir_id` band (and, for the `dir_facts` join in
  `dirsHaveMatchingChildrenQuery`, by `child_dir_count` on the catalog, E2).
  `infoChildrenQuery` (~line 86) and
  `infoChildrenSnapshotQuery` (~line 243) - which feed `Info` - are rewritten to
  count children from the catalog (`uniqExact(parent_id)` / `count()` over
  `wrstat_dirs`), see Maintenance/`Info` in the J4 matrix.
- `clickhouse/dir_filter_ageall.go`: `dirsHaveMatchingChildrenAgeAllQuery`
  (~line 78) - rewritten to the catalog band joined to
  `wrstat_dir_filter_ageall` by `dir_id` instead of `wrstat_children`.
- `clickhouse/tree_summary.go`: the `addTreeSummaryChildren` query
  `SELECT c.parent_dir, count() FROM wrstat_children c ...` (~line 234) -
  rewritten to count the catalog `parent_id` band per parent.

Signatures unchanged: `DirInfo(dir string, filter *Filter) (*DirSummary,
error)`, `Children(dir string) ([]string, error)`, `DirInfos(dirs []string,
filter *Filter) (map[string]*DirSummary, error)`.

### E2: `DirsHaveChildren`, ancestors, batch full-path

As a server, I want presence/ancestor/full-path resolution as integer reads.

- `DirsHaveChildren(dirs, filter)`: broad uses catalog `child_dir_count > 0`;
  filter-aware uses the `wrstat_child_filter_all` `parent_id` band (or in-query
  band). Signature unchanged.
- Ancestors/breadcrumbs and path->id: `parent_id` walk and/or
  path_hash/full_path.
- Batch full-path for a page of file rows: one catalog `dir_id IN (...)`
  resolves the whole page (F3).

**File:** `clickhouse/database.go`, `clickhouse/dir_filter_ageall.go`,
`clickhouse/tree_summary.go` (E1 query retirements)
**Test file:** `clickhouse/database_test.go` (existing, updated)

**Acceptance tests:**

1. Given a parent with children across multiple sources, when `Children` is
   called, then returned paths are de-duplicated, sorted, and byte-identical to
   baseline.
2. Given a directory present in 2 sources with differing `updated_at`, when
   `DirInfo` is called, then `Modtime` is the latest `updated_at` and the
   summary is the merged-then-filtered result (matches baseline).
3. Given a leaf or missing dir, when `Children` is called, then it returns
   nil/empty (matches baseline).
4. Given a batch of dirs, when `DirsHaveChildren` is called broad and filtered,
   then the boolean map matches baseline for both.
5. Given a dir, when its breadcrumb ancestors are resolved via the `parent_id`
   walk, then the paths match those produced by splitting the baseline full
   path.

### E3: `Where` over `dir_id` ranges

As a CLI/server, I want `Where` served by `(dir_id, subtree_end)` range scans,
sorted DESC by Size then ASC by Dir.

`Where(dir, filter, recurseCount)`: resolve `dir_id`, range-scan facts/filter
rows in `[dir_id, subtree_end)`, apply the `recurseCount` threshold per node,
return `DCSs` sorted as today. Default filter (FT==0 ->
AllTypesExceptDirectories) preserved. Signature unchanged: `Where(dir string,
filter *Filter, recurseCount func(string) int) (DCSs, error)`.

**Acceptance tests:**

1. Given a subtree, when `Where` is called broad and filtered, then `DCSs` are
   identical to baseline (same dirs, sizes, full paths, DESC-Size/ASC-Dir
   order).
2. Given an auth-restricted `Where` (uid/gid filter), when called, then only
   permitted dirs appear, matching baseline.

---

## F. Full-text / glob path search

### F1: basename globs on `wrstat_files.name`

As a server, I want direct-child / extension / dotfile globs that match on
basename to filter `wrstat_files.name` directly.

### F2: full-path globs on the catalog skip index

As a server, I want full-path / substring / recursive globs resolved against
the small `wrstat_dirs` catalog using `ngrambf_v1`/`tokenbf_v1` on `full_path`,
so the 1.3B-row files table is never scanned for directory-path matching.

- Compile each pattern: basename-only -> `name` filter on files; path-bearing ->
  candidate `dir_id`s (or `[dir_id, subtree_end)` ranges) from the catalog
  skip-index + regex on `full_path`, then intersect with files by `dir_id`.
- Preserve gitignore-style multi-pattern semantics (`*` not crossing `/`, `**`
  crossing dir boundaries, `?`), the `(ownerEnabled=0 OR uid=? OR has(gids,
  gid))` permission filter, ordering, and pagination. `CountByGlob` falls back
  to `FindByGlob` above 32 patterns (unchanged).
- The skip index is an optimisation, never the matcher: the `full_path` regex
  (RE2) is always applied to candidate rows, so a pattern with no usable
  ngram/token (e.g. a leading `*`/`?` with no literal run) correctly falls back
  to scanning the small catalog rather than missing matches. The `ext` index
  shortcut likewise only narrows candidates; the glob is still verified against
  the reconstructed name/path, so `ext='bam'` cannot over-match `*.bam`.

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
4. Given `/m/a/.bam`, `/m/a/a.bam`, `/m/a/b.BAM`, `/m/a/a.tar.gz`,
   `/m/a/sub/owned.bam`, and `/m/a/sub/fake.cram` (the last with an adversarial
   stored `ext` of `bam`), when `FindByGlob(["/m/a/"], ["*.bam"], no owner)`
   runs, then results are exactly `["/m/a/.bam", "/m/a/a.bam"]`; `["*.tar.gz"]`
   returns exactly `["/m/a/a.tar.gz"]`; and `["**/*.bam"]` returns exactly
   `["/m/a/.bam", "/m/a/a.bam", "/m/a/sub/owned.bam"]` (the `ext` shortcut
   narrows candidates but the glob decides, so `b.BAM` and `fake.cram` are
   excluded).
5. Given the recursive bam set above, when `FindByGlob(["/m/a/"], ["**/*.bam"])`
   runs with `Limit=2, Offset=1`, then results are exactly `["/m/a/a.bam",
   "/m/a/sub/owned.bam"]`; with `Offset=3` it returns an empty slice.
6. Given `/m/a/.env` (uid=11, gid=21) and `/m/a/file.txt` (uid=10, gid=20), when
   `FindByGlob(["/m/a/"], [".*"], no owner)` runs it returns exactly
   `["/m/a/.env"]`; and owner-required `FindByGlob(["/m/a/"], ["*"])` with
   `uid=10, gids=[30]` returns only `["/m/a/file.txt"]`.
7. Given the extension dataset above, when `CountByGlob` runs for `*.bam`,
   `**/*.bam`, `*.tar.gz`, and `.*`, then it returns `2`, `3`, `1`, and `1`
   respectively (matching `FindByGlob` dedup semantics).

---

## G. Basedirs and active virtual namespace in the id world

### G1: basedirs reference `dir_id` with external string fallback

As a basedirs author, I want in-snapshot `basedir`/`subdir` paths referenced by
`dir_id`, with an explicit external string column for paths legitimately outside
the snapshot, so historical/quota data still resolves.

- `wrstat_basedirs_group_subdirs` / `_user_subdirs`: replace `basedir`/`subdir`
  strings with `basedir_id`/`subdir_id UInt32` resolved against the active
  snapshot catalog, plus `basedir_external String`/`subdir_external String`
  (clearly marked, used only when no `dir_id` exists).
  `wrstat_basedirs_group_usage` / `_user_usage`: `basedir_id` +
  `basedir_external`.
- `wrstat_basedirs_history` keeps strings (history spans snapshots; explicitly
  external).
- Readiness MUST fail (`ErrIDUnresolved`) if an in-snapshot active
  `basedir`/`subdir` cannot be resolved to a `dir_id`.
- Reader signatures unchanged: `GroupUsage(age db.DirGUTAge)`,
  `UserUsage(age db.DirGUTAge)`,
  `GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge)`,
  `UserSubDirs(uid uint32, basedir string, age db.DirGUTAge)`,
  `History(gid uint32, path string)` - they return the same
  `[]*basedirs.Usage`/`[]*basedirs.SubDir`/`[]basedirs.History` with the same
  path strings (resolved from `dir_id` or the external column).

**File:** `clickhouse/schema/006-009_basedirs*.sql`,
`clickhouse/basedirs_reader.go`
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

Precedence (matches baseline routing, prevents double counting): the active
virtual overlay is the sole authority for any path that is a strict prefix of
an active mount root (`/`, `/lustre/`, `/nfs/`, intermediate virtual parents)
and for an exact mount-root box under the same conditions as today. Serving
checks the overlay first; when it handles a path, the per-mount above-root chain
rows (B2) are NEVER also summed into that answer. Those B2 rows exist only to
(a) hold the interval invariant and full-path/ancestor reconstruction inside one
mount's id space and (b) feed the overlay aggregation and act as the documented
fallback when the overlay is not ready - exactly the current
`dirInfoOutsideMount` -> `activeVirtualDirInfo` (handled?) -> ancestor-fallback
order. `DirInfo`, `Children`, `DirsHaveChildren`, and `Where` follow this one
rule for above-root and virtual paths.

Reuse, do not reinvent: the `active_set_id` derivation, active-set readiness,
and active-set cleanup are the existing implementations (`active_mounts.go`,
`active_virtual_overlay.go`, `active_prefix_rollups.go`); this story only swaps
the synthetic nodes' `dir`/`parent_dir` strings for `virtual_id` and the
mount-root `dir_id` link. The active-set identity and readiness contract are
unchanged.

The deleted string `wrstat_virtual_children` (+ `wrstat_virtual_children_sets`)
table and its production WRITER/REFRESHER/READER are **removed** (the virtual
id space above plus the catalog `parent_id` band serve virtual child listings):

- `clickhouse/virtual_children.go`: the `insertVirtualChildQuery` /
  `insertVirtualChildrenSetQuery` / `virtualChildrenSetIDsQuery` /
  `dropVirtualChildrenPartitionQuery` / `dropVirtualChildrenSetPartitionQuery`
  constants; the writers/refreshers `insertVirtualChildrenSet`,
  `insertVirtualChildRows`, `appendVirtualChildRows`,
  `refreshActiveVirtualChildren`, `refreshActiveVirtualChildrenForActiveSet`;
  and the readers `virtualChildrenReadyQuery`/`virtualChildrenReady` and
  `virtualChildrenQuery` (and the `queryChildren(... virtualChildrenQuery ...)`
  call ~line 395). The live READER chain that serves `Children` for
  virtual/ancestor paths via that deleted table is rewritten (not deleted) to
  serve from the per-`active_set_id` virtual catalog
  (`wrstat_active_virtual_dirs`) / catalog `parent_id` band, mirroring how
  `activeVirtualMountChildCountsQuery` below is rewritten off `wrstat_children`:
  `virtualChildrenForAncestor` (~line 372, replace its `queryChildren(...
  virtualChildrenQuery ...)` ~line 395 with the virtual-catalog/`parent_id`-band
  read), `ensureVirtualChildrenReady`
  (~line 409, drop its `virtualChildrenReady` + `insertVirtualChildrenSet` calls
  and use the reused active-set readiness above), and
  `currentVirtualChildrenActiveSet` (~line 403). In `clickhouse/database.go` the
  caller chain is rewritten to match: `virtualChildrenForReadyAncestorMounts`
  (~line 4197) and its two call sites - the call from `readyChildrenForAncestor`
  (~line 4160) and its own `virtualChildrenForAncestor` call (~line 4206).
  These reader functions preserve current `Children` behaviour for `/`,
  `/lustre/`, `/nfs/` and ancestor paths.
- `clickhouse/provider.go`: the `virtualChildrenRefresher` type alias (~line
  78), the `refreshVirtualChildren` field (~line 93), and the
  `refreshVirtualChildrenAsync` / `virtualChildrenRefresherLocked` /
  `refreshVirtualChildrenAndReport` methods plus their call site (~line 664).
- `clickhouse/active_virtual_overlay.go`: the
  `activeVirtualMountChildCountsQuery` reader (~line 71) currently reads
  mount-root child counts from
  `FROM wrstat_children c`; rewritten to read the per-mount catalog
  (`wrstat_dirs`) `parent_id` band at the mount root (the mount-root box's
  child count) - no `wrstat_children` read survives in the overlay path.
- **Retained, do NOT remove:** the geometry helpers `virtualChildRowsForMount`,
  `virtualChildRowsForMounts`, `mergeVirtualChildRows`, and the
  `virtualChildRow` type stay - they are shared by the RETAINED
  `activeVirtualChildRowsForMounts` (`dguta_writer.go` ~line 481) that writes
  the RETAINED `wrstat_active_virtual_children` table. Removing them would break
  that writer. Only the `wrstat_virtual_children`-specific INSERT/refresh/reader
  code above is deleted.

**File:** `clickhouse/schema/018_active_virtual_overlay.sql`,
`clickhouse/virtual_namespace.go`, `clickhouse/virtual_children.go` (pruned),
`clickhouse/provider.go`, `clickhouse/active_virtual_overlay.go`,
`clickhouse/database.go` (reader chain rewrite)
**Test file:** `clickhouse/virtual_namespace_test.go` (new)

**Acceptance tests:**

1. Given selected active Lustre and NFS mounts, when root `/` is summarised,
   then it aggregates every selected mount with separate `/lustre` and `/nfs`
   boxes and correct totals (matches baseline).
2. Given an active set, when virtual children, filtered virtual summaries, and
   active-prefix rollups are read, then they match baseline outputs.
3. Given the virtual catalog, when inspected, then it contains only synthetic
   virtual nodes (no copied mount-local directory rows).
4. Given two active Lustre mounts under `/lustre/` with root counts `10` and
   `20`, when `DirInfo("/lustre/")` runs, then `Count=30` (the overlay
   aggregates once); the per-mount above-root `/lustre/` rows in `wrstat_dirs`
   are not additionally summed, and the result equals baseline.
5. Given the same active set, when `Children("/")`, `DirInfo("/")`,
   `DirsHaveChildren(["/nfs/"], filter)`, and a virtual `Where` run, then each
   is served from the overlay (the per-mount above-root rows are read only on
   the not-ready fallback) and matches baseline.
6. Given an unchanged active mount selection, when the `active_set_id` is
   derived and readiness is evaluated, then both equal the baseline
   implementation's values (derivation and readiness are reused, not redefined).

---

## H. Ingest pipeline and spool carrying ids

### H1: spool format carries one catalog stream + id-keyed rows

As an ingest author, I want the spool to carry `dir_id` integers and one
catalog stream per mount snapshot, not repeated path strings, so spool bytes
drop sharply.

- Add a `wrstat_dirs` catalog stream to `internal/chspool`: rows `(dir_id,
  parent_id, subtree_end, depth, name, full_path, child_dir_count,
  child_file_count, path_hash)` - `full_path` stored once per directory.
- Drop every path-string field on the surviving row structs (verified field
  names in `internal/chspool/spool.go`), replacing each with the integer id:
  - `FileRow`: `ParentDir string` -> `DirID uint32` (keeps `Name`).
  - `DirFactRow`: `Dir string` -> `DirID uint32` (+ `ParentID uint32` and
    `SubtreeEnd uint32`, D1).
  - `ChildFilterAllRow`: drop both `ParentDir string` and `Dir string` ->
    `ParentID uint32` + child `DirID uint32`.
  - `DirFilterAllRow`: drop both `Dir string` and `ParentDir string` ->
    `DirID uint32` + `SubtreeEnd uint32` (no `ParentID` needed; keyed by the
    `(dir_id, subtree_end)` range, D2).
  - `DirFilterAgeAllRow`: drop `Dir string` -> `DirID uint32` + `SubtreeEnd
    uint32`.
  No surviving filter/child/file/fact row struct retains a `Dir`/`ParentDir`
  string field.
- `ChildRow` (and its `ParentDir`/`Child` strings), `ParentFactRow` (and its
  `ParentDir`/`Dir` strings), and the `wrstat_children`/`wrstat_parent_facts`
  streams are removed entirely (the catalog `parent_id` band replaces them, E1).
  In `internal/chspool/spool.go`: the `TableChildren = "wrstat_children"` and
  `TableParentFacts = "wrstat_parent_facts"` constants (~lines 55, 58), their
  entries in the deterministic table-order list (~lines 82-83), the
  `WriteChild`/`WriteParentFact` writer methods (~lines 750, 762), and the
  `case TableChildren:`/`case TableParentFacts:` decode arms (~lines 220, 226)
  are deleted; the loader switch arms for them go too (H2).
- Basedirs/active-virtual rows carry ids per G1/G2 (+ external string fallback).
- Keep `Format`/`Manifest`/`TableManifest` machinery and the deterministic table
  order; add the catalog stream to that order; update per-table `Rows` counters.
- The production spool PRODUCER `cmd/summarise_spool.go` is the writer of these
  structs and is updated to match: `summariseFileSpoolOperation.Add` emits the
  `FileRow` with `DirID` (looked up from the current directory's assigned id,
  B5) + `Name`, carrying no `parent_dir`/`Dir` path string;
  `summariseDGUTASpoolWriter` emits
  `DirFactRow`/`ChildFilterAllRow`/`DirFilterAllRow`/`DirFilterAgeAllRow` with
  `DirID`/`ParentID`/`SubtreeEnd` and no path strings; it also emits the new
  catalog stream rows. Its `WriteChild`/`ChildRow` path (`AddChildren`,
  `appendChildrenRows`) and its `writeParentFactRow` (emitting
  `WriteParentFact`/`ParentFactRow`) are removed with the deleted tables (B3,
  H2).

**File:** `internal/chspool/spool.go`, `cmd/summarise_spool.go`
**Test file:** `internal/chspool/spool_test.go` (existing, updated)

### H2: writers and loader emit/load id rows

As an ingest author, I want `dguta_writer.go`, `file_ingest_operation.go`,
`mount_dir_projection_writer.go`, `import_block_writer.go`, and
`summarise_spool_loader.go` to write/load the id-based rows, keeping the
resumable/retry and readiness-set machinery.

- A new catalog writer (`clickhouse/catalog.go`) buffers and inserts
  `wrstat_dirs` rows from `RecordDGUTA`'s id fields; the dguta writer drives it.
- File ingest writes `dir_id` (read from the shared `DirIDAllocator`, B5) +
  `name`, no `parent_dir`.
- `mount_dir_projection_writer` writes facts rows keyed by `dir_id`, carrying
  `parent_id` and `subtree_end` (D1) from `RecordDGUTA`.
- Derived-index writers emit numeric filter rows (D2).
- `import_block_writer` (batch open-duration, send-on-full/too-old,
  abort-on-error) unchanged in mechanism; row counters updated for the new
  tables.
- `summarise_spool_loader` loads the catalog stream first, then id-keyed tables;
  the readiness/manifest (`wrstat_schema3_snapshot_sets`,
  `wrstat_dir_projection_sets`) records the new tables' row counts + SHA256.
- `DGUTAWriter` interface (`Add, SetBatchSize, SetMountPath, SetUpdatedAt,
  Close`) is preserved; ids flow via `RecordDGUTA` (no signature change).
  `SetMountPath` still sets D up front for B2.
- The streaming child-edge and parent-facts machinery is **removed** (it only
  fed the deleted `wrstat_children`/`wrstat_parent_facts` tables):
  - in `db`/`summary/dirguta`: the `db.DGUTAChildrenWriter` interface and its
    `summary/dirguta` plumbing (`childDB`/`streamChildren` fields,
    `streamChildren`/`flushChildren`, the `d.(db.DGUTAChildrenWriter)`
    assertion);
  - in `clickhouse/dguta_writer.go`: `dgutaWriter.AddChildren` plus its
    child-row writers
    (`appendChildrenRows`/`appendChildRow`/`childrenBlockWriter`), the
    `insertChildrenQuery` constant (~line 105), the `importPhaseChildrenInsert`
    constant (~line 60), the `dropChildrenPartitionQuery` (~line 79), and the
    `{name: "wrstat_children", dest: &counts.childrenRows}` row-counter entry
    (~line 1168);
  - in `cmd/summarise_spool.go` (the production spool PRODUCER): the
    `summariseDGUTASpoolWriter.AddChildren` method, `appendChildrenRows`, the
    `WriteChild`/`ChildRow` emission, the `writeParentFactRow` method
    (emitting `WriteParentFact`/`ParentFactRow`), and the
    `parentFactsRows`/`childrenRows` row-counter fields read from
    `tables[chspool.TableParentFacts].Rows`/`[chspool.TableChildren].Rows`
    (~lines 664-665).
  These are deleted (B3). Child listings come from the catalog `parent_id` band
  (E1) and "has children" from `child_dir_count` (A1); no live interface is
  wired to a deleted table.
- The production `wrstat_parent_facts` derived-index WRITER is **removed**
  (it only fed the deleted `wrstat_parent_facts` table):
  - `clickhouse/parent_facts.go`: the `parentFactsWriter` type,
    `newParentFactsWriter`, `appendRecord`, `appendParentFactRecordValues`, the
    `blockWriter`/`flush`/`abort`/`importPhase`/`importBatchNow` methods, and
    the `insertParentFactsQuery` constant;
  - `clickhouse/dguta_writer.go`: the `selectedNavigationFactWriters()` wiring
    (~line 1912) that appends a `newParentFactsWriter(...)` (~line 1918) to
    `selectedDerivedIndexes` (so the writer is no longer flushed by the
    derived-index path ~line 2243), plus the `importPhaseParentFactsInsert`
    constant (~line 61), the `dropParentFactsPartitionQuery` (~line 85) and the
    `{name: "wrstat_parent_facts", dest: &counts.parentFactsRows}` row-counter
    entry (~line 1167);
  - `clickhouse/summarise_spool_loader.go`: every `TableChildren`/
    `TableParentFacts` load path - the `summariseSpoolTableQuery` switch arms
    `case chspool.TableParentFacts: return insertParentFactsQuery,
    importPhaseParentFactsInsert` and `case chspool.TableChildren: ...` (~lines
    1291, 1307); the dedicated children/parent-facts loaders
    `loadSimpleSpoolTable(... chspool.TableChildren ..., ChildRow ...)` (~line
    526) and `loadSimpleSpoolTable(... chspool.TableParentFacts ...,
    ParentFactRow ...)` (~line 636); their entries in the table-order/readiness
    lists (~lines 412-413, 1685) and per-table row-count queries (~lines
    1790-1794); and the readiness check
    `tables[chspool.TableChildren].Rows == 0` (~line 1433). The loaded set is
    the catalog + surviving id-keyed tables only.
- The `wrstat_parent_facts` derived-index READ machinery (the parent-ordered
  navigation object) is **removed**; with the parent-ordered navigation object
  gone, child listings and child summaries come solely from the catalog
  `parent_id` band (E1) and the facts table's `parent_id` band / numeric
  child-filter table (D1, D2):
  - the whole `NavigationObject` selection apparatus is removed (after the
    overhaul, child navigation is served solely from the catalog `parent_id`
    band / facts `dir_id`/`subtree_end`, so no navigation-object is selected):
    - `clickhouse/parent_facts.go`: the `NavigationObject` type (~line 81), ALL
      THREE constants - `NavigationObjectParentFacts` (~line 86) and
      `NavigationObjectChildFacts` (~line 90) here, and
      `NavigationObjectProjection` (in `mount_dir_projection_writer.go` ~line
      49, see below) - plus `DefaultNavigationObject` (~line 102) and
      `ChooseNavigationObject` (~line 108). `NavigationObjectChildFacts`'s
      string value `"wrstat_tree_nav_facts"` named only a benchmark CANDIDATE:
      no such table, writer, reader, or schema file ever existed (verified - it
      appears only in this doc comment), so there is no child-facts serving path
      or schema file to retire and the "Schema file set after the rewrite"
      section is unaffected. `NavigationObjectParentFacts`'s
      `wrstat_parent_facts` serving path is the parent-facts writer/readers
      deleted above.
    - `clickhouse/mount_dir_projection_writer.go` is RETAINED (it writes the D1
      `wrstat_dir_facts` table); ONLY its `NavigationObjectProjection` const
      (~line 49) and that const's doc comment are removed. The
      `mountDirProjectionWriter` type and all its fact-writing machinery stay.
      `NavigationObjectProjection` likewise named only a benchmark candidate
      (`"clickhouse_projection"`), not a separate serving path.
    - the remaining `wrstat_parent_facts` READ machinery in
      `clickhouse/parent_facts.go`: the `parentFactsChildSummariesQueryScope` /
      `parentFactsAllChildSummariesQuery` / `parentFactsFileChildSummariesQuery`
      / `parentFactsVectorChildSummariesQuery` query constants, and the
      `parentFactChildSummaries` / `parentFactDirInfoChildSummaries` /
      `parentFactReadMode*` readers (~line 103, ~line 137).
    Both `parent_facts.go` and `mount_dir_projection_writer.go` MUST compile
    after this change (no dangling reference to the deleted `NavigationObject`
    type or its constants/functions).
  - `clickhouse/database.go`: the read-side branches that gate on
    `DefaultNavigationObject() != NavigationObjectParentFacts` (~lines 586,
    3224, 3563) are removed, and with them the ENTIRE `whereTraversal`
    parent-fact packet subsystem (its frontier/packet/store helper cluster -
    e.g. `canUseWhereParentPackets`, `loadWhereParentFactPackets`,
    `preloadFrontierParentPackets`, `frontierSummaryPacketDirs` (~line 593,
    which ALSO calls `parentFactsParentDir` at ~line 602),
    `frontierChildPacketDirs`,
    `appendWherePacketDir`, `wherePacketDirInMount`,
    `storeWhereParentFactPacket`, `storeWhereParentFactSummary`,
    `storeWhereParentFactLeafChildren`) plus the
    parent-fact `DirInfo` machinery (`parentFactDirInfoChildSummaries`/
    `parentFactDirInfoRequests`/`parentFactDirInfoRemainingGroup`/
    `parentFactDirInfoFallbackOrError`/`parentFactsCanHandleDirInfoFilter`/
    `parentFactDirInfoFilterUsesParentFacts`) and the `parentFactChildSummary`
    type they consume. This whole subsystem and the parent-facts read/cache
    machinery in `clickhouse/parent_facts.go` and the `database_cache.go`
    parent-packet cache are removed; the `Where`, `DirInfo`, and
    child-navigation behaviour they served is re-expressed on the catalog
    `parent_id` band and facts `(dir_id, subtree_end)` ranges per E1/E3/D2 -
    none read `wrstat_parent_facts` or build `parentFactChildSummary`. The named
    helpers above are illustrative entry points, NOT an exhaustive list: the
    private helpers transitively orphaned by this subsystem removal are covered
    by the build-clean acceptance criterion (J1/H2 acceptance test 7), which an
    implementor resolves mechanically by following compiler errors guided by the
    subsystem story.
    - `clickhouse/database_cache.go`: the parent-fact packet CACHE (the
      `treeParentPacketCacheKey`/`parentPacket*` cache cluster on
      `treeQueryCache` and its snapshot struct, the
      `cloneParentFactChildSummaries` consumer, and the
      `getParentPacket`/`putParentPacket`/`recordParentPacket*` methods) is
      removed or repointed at the catalog/facts band cache that replaces it; no
      cache field retains a `[]parentFactChildSummary`.
    - `clickhouse/perf_counters.go`: the parent-facts fallback-route counter
      (`parentFactsFallbackRoute`/`parentFactsFallbackRouteName`/
      `parentFactsFallbackRoutes`/`recordParentFactsFallbackRoute`/
      `resetParentFactsFallbackRoutesForTest` in the deleted `parent_facts.go`)
      is removed. The PUBLIC `ReadSchema3FallbackRoutes`/
      `ResetSchema3FallbackRoutes` (called from `cmd/clickhouse_perf.go`,
      `cmd/`, and `server/`, so retained) drop the `parent_facts_fallback` map
      entry (the route no longer exists); `ReadSchema3FallbackRoutes` returns
      the surviving fallback-route counters only (an empty map if none remain),
      and callers/tests asserting `["parent_facts_fallback"]` are updated to the
      surviving-route key set.
    - `parentFactsParentDir` (defined in the deleted `parent_facts.go`) has two
      RETAINED callers besides the `whereTraversal` subsystem above
      (`frontierSummaryPacketDirs`, removed with that subsystem): both are in
      `clickhouse/dir_filter_all.go`. Line ~79 derives `ParentDir` for the
      `filterAllRow` that feeds the `wrstat_dir_filter_all` DIR-filter row; line
      ~232 (`noteDirectChildTuples`) derives the parent dir for the CHILD-filter
      writer. With the D2 rewrite both filter tables key on the carried
      `parent_id` (B3) and no longer derive a parent-dir string, so both calls
      are removed; if any string parent-dir derivation is still needed
      transiently it is inlined into its caller rather than left referencing the
      deleted file. After the change `dir_filter_all.go` (and the removed
      `whereTraversal` subsystem) leave NO reference to `parentFactsParentDir`.

**Files:** `clickhouse/dguta_writer.go`, `file_ingest_operation.go`,
`mount_dir_projection_writer.go`, `import_block_writer.go`,
`summarise_spool_loader.go`, `catalog.go`, `parent_facts.go` (deleted),
`database.go`, `database_cache.go`, `dir_filter_all.go`, `perf_counters.go`,
`cmd/summarise_spool.go`, `cmd/summarise.go`, `summary/idalloc.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go` (existing, updated)

**Acceptance tests:**

1. Given a full summarise -> spool -> load cycle, when queries run against the
   loaded data, then all results match baseline (correctness).
2. Given the same input, when the spool is written twice, then per-table byte
   contents are identical (determinism, enabled by B1/B2 ids).
3. Given the benchmark, when spool bytes are measured, then total spool bytes
   and path-text bytes are reported vs baseline with the reduction quantified.
4. Given a write interrupted mid-batch and retried, when resumed, then the
   readiness machinery produces a complete, correct snapshot (resume preserved).
5. Given the production spool produced by `cmd/summarise_spool.go`, when its
   written streams are inspected, then there is no `wrstat_children` or
   `wrstat_parent_facts` stream and every `FileRow`/`DirFactRow`/filter row
   carries `dir_id` (and `parent_id`/`subtree_end` where applicable) with no
   path string.
6. Given the rewritten production tree (all non-test `.go` files, excluding the
   "Current state to replace" descriptive listings and the SQL files that are
   themselves deleted), when a repo-wide search runs (e.g. `rg -n
   'INSERT INTO (wrstat_children|wrstat_parent_facts|wrstat_virtual_children)|
   FROM (wrstat_children|wrstat_parent_facts|wrstat_virtual_children)( |$)'
   --glob '!*_test.go'`), then it finds zero matches: no code path writes,
   refreshes, loads, or selects from `wrstat_children`, `wrstat_parent_facts`,
   `wrstat_virtual_children`, or `wrstat_virtual_children_sets`. (The retained
   `wrstat_active_virtual_children` table, written by `activeVirtualChildRows
   ForMounts`, is a different table and is unaffected.)
7. Given the rewritten production tree, when each affected package is built
   (`go build ./...`) and vetted (`go vet ./...`), then it compiles with NO
   orphaned reference: no retained code refers to a deleted type, constant,
   interface, struct field, or table - specifically the deleted
   `NavigationObject` type and its constants (H2),
   `DefaultNavigationObject`/`ChooseNavigationObject`, the
   `parentFactChildSummary` type and its packet/cache machinery, the
   `parentFactsFallbackRoute*` counters, `db.DGUTAChildrenWriter` (B3), the
   `wrstat_children`/`wrstat_parent_facts`/`wrstat_virtual_children` query
   constants, or any reference to a deleted SQL table. This build-clean test is
   the AUTHORITATIVE backstop for the subsystem removals: the private/internal
   helper functions transitively orphaned by removing the `whereTraversal`
   parent-fact packet subsystem and the parent-facts read/cache machinery
   (H2) are NOT enumerated individually in this spec - they are covered by this
   requirement that `go build ./... && go vet ./...` is clean with no reference
   to any removed type/field/const/interface/table, and an implementor resolves
   each one mechanically by following the compiler errors guided by the
   subsystem story in H2. The post-removal tree builds clean before any new
   feature code is added.

---

## I. (Optional, flag-gated) in-memory navigation index

### I1: compact in-process catalog index

As a server operator, I want an optional in-memory index (`dir_id -> parent_id,
name, subtree_end, child_dir_count, child_file_count`) so parent/child/
has-children/ancestor/full-path navigation answers without a ClickHouse
round-trip, leaving ClickHouse for heavy DGUTA aggregations.

- Flag-gated (`--nav-index`), non-blocking, built asynchronously per active
  snapshot; queries fall back to ClickHouse until it is ready. It MUST NOT gate
  or delay the core schema/benchmark work; the core is complete and correct
  without it.
- Include a memory/build-cost estimate: per dir ~ `4 (dir_id implicit) + 4
  (parent_id) + 4 (subtree_end) + 4 (counts) + len(name)` bytes plus map
  overhead; for 10-100M dirs estimate low-GB; report the measured figure.

**File:** `clickhouse/navindex.go` (new)
**Test file:** `clickhouse/navindex_test.go` (new)

**Acceptance tests:**

1. Given the flag off, when navigation queries run, then behaviour and results
   are identical to the ClickHouse-only path (no regression).
2. Given the flag on and the index built, when `Children`/`DirsHaveChildren`/
   ancestor/full-path queries run, then results match the ClickHouse path
   exactly with no ClickHouse round-trip (verified by query-count assertion).
3. Given the flag on but the index not yet built, when a navigation query runs,
   then it transparently falls back to ClickHouse and returns correct results.
4. Given the benchmark, when the index is enabled, then its memory and build
   cost are reported and its per-query latency delta vs the ClickHouse path is
   stated.

---

## J. Mandatory before/after benchmark (the gate)

### J1: baseline capture and harness reuse

As a maintainer, I want the current `clickhouse` branch HEAD captured as
baseline before any change, using the existing harness, so the comparison is
rigorous.

- Baseline = current `clickhouse` HEAD, captured before any overhaul change
  (preserve a built binary or a separate worktree under `.tmp/agent/overhaul/`).
- Reuse and extend `internal/chperf/` (`import.go`, `query.go`, `final_gate.go`,
  `clickhouse_api.go`) and the Bolt comparison harness. Do NOT invent a new
  report format; keep the existing report structures -
  `perfreport.Report`/`Operation`/`TableStats` (in
  `internal/perfreport/report.go`) and `chperf.QueryMetrics` (in
  `internal/chperf/api.go`).
- The children/parent-facts plumbing in `internal/chperf/import.go` is removed
  or replaced alongside the table deletions, mirroring `dguta_writer.go` (B3,
  H2): the `trackedStreamingDGUTAWriter` type and its `AddChildren` method, the
  `errDGUTAChildrenWriterRequired` sentinel, the
  `tableChildren`/`tableParentFacts` constants and the
  `phaseChildrenInsert`/`phaseParentFactsInsert` phases, the `countChildrenRows`
  helper (and any `countParentFactsRows` companion), and every use of these in
  per-table report lists, phase->table mapping, and the
  `RowAmplificationVsChildren`/`addRows(tableChildren,
  ...)`/`addRows(tableParentFacts, ...)` evidence. The harness reports only the
  surviving id-keyed tables (the catalog and the numeric filter tables); no
  metric references a deleted table.
- The ENTIRE navigation-object selection apparatus in
  `internal/chperf/query.go` is removed alongside the read-side deletion (H2,
  which removes the whole `clickhouse.NavigationObject` type and its
  `NavigationObjectParentFacts`/`NavigationObjectChildFacts`/
  `NavigationObjectProjection` constants and
  `DefaultNavigationObject`/`ChooseNavigationObject`). Removing only
  `navigationShapeParentFacts` would leave the other two shapes and their
  candidate machinery referencing now-deleted constants, so ALL of it goes:
  - the three shape constants `navigationShapeParentFacts` (~line 132),
    `navigationShapeChildFacts` (~line 133), and `navigationShapeProjection`
    (~line 134) - each `= string(clickhouse.NavigationObject*)`, so each
    references a deleted constant - plus the supporting constants
    `navigationScenarioHighFanout`/`navigationScenarioFiltered`/
    `navigationInput*`/`navigationMinHighFanoutChildren`/
    `navigationChildFactsImprovement` (~lines 135-146);
  - the `navigationProjection*` machinery
    (`navigationProjectionPasses`/`navigationProjectionExplainPasses`/
    `navigationProjectionExplainPassesScenario`, `ExplainUsesProjectionPruning`/
    `explainIndexes1Output`/`explainNamesProjection`, ~lines 1973-2056);
  - the `navigationCandidate*` machinery
    (`navigationCandidateOperation`/`navigationCandidateMatches`/
    `navigationShape`/`navigationScenario`/`navigationCandidateAtLeastAsFast`/
    `navigationCandidateBeatsParentBy`/`navigationCandidateP95`/
    `navigationCandidateScenarioComplete`/
    `navigationCandidateOperationComplete`/
    `navigationCandidateCountersComplete`/`navigationHighFanoutInputPasses`,
    ~lines 1999-2334);
  - the child-facts machinery
    (`navigationChildFactsPasses`/`navigationChildFactsSpeedPasses`/
    `navigationChildFactsResultsMatch`/`navigationResultDigestMatches`/
    `navigationChildFactsReadShapePasses`/`navigationReadShapePasses`/
    `navigationOperationHasFilteredAgeAllOwnerOrTypePredicate`/
    `navigationChildFactsFilteredAgeAllPasses`/`navigationChildFactsPreferred`,
    and the import-gate helpers `navigationImportGatesPass`/
    `navigationImportReportPasses`/`navigationRowAmplificationPasses` -
    `navigationRowAmplificationPasses` reads
    `report.TableStats[navigationShapeChildFacts]`, ~line 2108);
  - the C1 decision gate itself: the `NavigationDecisionEvidence` (~line 1968),
    `NavigationDecisionCheck` (~line 2337), and `NavigationDecisionResult`
    (~line 2434) types; `navigationSelectedObject` (~line 2267, which calls the
    deleted `clickhouse.ChooseNavigationObject`, ~line 2278); the per-check
    validators `validateNavigationCandidateReport`/
    `validateNavigationProjection`/`validateNavigationChildFacts`/
    `validateNavigationParentDefault` (the last
    passing when `selected == navigationShapeParentFacts`, ~line 2408) and
    `navigationDecisionCheck`/`(NavigationDecisionCheck).pass`/`.fail`; and the
    exported `ValidateNavigationDecisionGate` (~line 2442). This gate is used
    only by `internal/chperf/query_test.go`'s `TestNavigationDecisionGateC1`
    (verified - no other caller), whose C1 test and its
    `navigationGateTestEvidence` fixture are deleted with it.
  Navigation is served from the catalog `parent_id` band / facts table, so the
  harness no longer benchmarks, selects, or asserts any navigation shape, and
  no metric, candidate, or check references a deleted `NavigationObject*`
  constant. `internal/chperf` MUST compile after this removal.
- `internal/perfreport/report.go`: the `RowAmplificationVsChildren` field
  (`json:"row_amplification_vs_wrstat_children"`, ~line 69) is removed or
  renamed to a surviving table; no report field name references a deleted table.
- `internal/chperf/final_gate.go`: the final-gate assertions keyed on
  `tableChildren`/`tableParentFacts` are removed or repointed at surviving
  tables - the `finalGateE2PacketOperationFailure(..., tableParentFacts, ...)`
  packet gates (~lines 1052, 1098, 1144), the `case tableChildren:` /
  `report.TableStats[tableChildren].Rows == 0` required-rows checks (~lines
  4453, 4465), the `tableParentFacts` entry in the gated-table list (~line
  4480), and `tableChildren` in the required-tables slice (~line 4735). The
  E2-packet latency gate is re-expressed against the catalog `parent_id` band /
  facts-table reads that now serve child packets; no gate references a deleted
  table. A RETAINED gate path also reads the removed field: the
  `stats.RowAmplificationVsChildren > 0` check in
  `tableStatsDerivedEvidencePass` (~line 4508), reached via
  `tableStatsEvidencePass`/`finalGateNewObjectTableStatsPass`. This check is
  removed (or repointed to a surviving amplification metric) alongside the
  `report.go` field removal so `internal/chperf` compiles.

**Acceptance tests:**

1. Given the rewritten `internal/chperf/import.go` and `query.go`, when the
   import and query benchmarks run, then no per-table metric, phase,
   amplification baseline, or navigation-shape candidate references
   `wrstat_children` or `wrstat_parent_facts`, and the `wrstat_dirs` catalog
   plus the surviving id-keyed tables are reported.
2. Given the rewritten `internal/chperf`, when the package and its tests are
   built, then they compile with no reference to `clickhouse.NavigationObject`,
   any `NavigationObject*` constant, `DefaultNavigationObject`,
   `ChooseNavigationObject`, or any `navigationShape*`/`navigationCandidate*`/
   `NavigationDecision*` symbol (all removed); `go vet ./internal/chperf/...`
   reports no unused-symbol or undefined-reference error.

### J2: datasets

At minimum: a mixed Lustre/NFS subset; a directory-heavy NFS tree; a high-fanout
parent (~11k direct children); a many-small-mounts active-virtual simulation
keeping `/nfs` virtual; and the largest practical production-like local dataset.
State which datasets are used and why a bounded subset still reproduces each
access pattern. If a bounded dataset cannot reproduce a pattern, say so.

### J3: import / storage metrics

Report: summarise wall time, CPU, max RSS; spool bytes; ClickHouse bytes per
table (compressed AND uncompressed via `TableStats.CompressedBytes` /
`UncompressedBytes`); active part counts (`ActiveParts`); rows per table; and
the explicit path-text-bytes-before-vs-after duplication reduction. Record the
measured max-dirs-per-snapshot (justifies `UInt32`).

### J4: per-query-type metrics and the canonical matrix

Per query type report server-side p50/p95/p99 latency, rows read, bytes read,
granules/marks read (`ReadRows`/`ReadBytes`/`ReadMarks`), result rows, and a
result digest for correctness. Every type in this matrix MUST be benchmarked
with a per-type before/after delta; missing one is a spec failure:

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
`same_query_client`, `ancestor_directory_each_repeat`,
`new_directory_each_repeat`, `visible_child_directory_each_repeat`,
`startup_cache_warming_contract`, `provider_update_cold_cache`,
`same_provider_same_dir`.

### J6: gates

- **Correctness (hard):** every query type returns results identical to baseline
  (paths, child ordering, full paths, filter/permission semantics, multi-source/
  active merge, glob semantics, basedirs/quota numbers, manifests). No wrong row
  ever (path-hash collisions verified, C2.4).
- **Absolute cold UX (hard):** exact dir resolution, exact file stat, permission
  path, direct-child list -> p95 < 100 ms; recursive subtree, filtered, glob,
  Disktree, `Where` -> p95 < 500 ms.
- **Storage (hard):** no hot row in any table stores a `dir`/`parent_dir`/path
  string; path text reduced to one copy per directory per snapshot in the
  catalog. Filter tables remain (numeric/`dir_id`-keyed) unless a pattern's
  materialisation was collapsed under D4 with a cited measurement. Report
  compressed and uncompressed bytes per table vs baseline.
- **Relative performance (report, not pass/fail):** per-query before/after
  delta for every type; regressions quantified and explained, and must still
  satisfy the absolute UX gate to ship.

**File:** `internal/chperf/` (extended), `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/*_test.go` (existing, updated)

**Acceptance tests:**

1. Given baseline and overhaul reports, when the comparison runs, then every
   matrix query type has a recorded p50/p95/p99 and a before/after delta; a
   missing type fails the gate.
2. Given the overhaul report, when storage is inspected, then no hot row stores
   a path string and per-table compressed/uncompressed bytes are reported vs
   baseline.
3. Given the cold UX gate, when the relevant query types run, then p95 latencies
   satisfy the < 100 ms / < 500 ms thresholds.
4. Given any collapsed materialisation, when the report is read, then a cited
   measurement justifies the collapse and the in-query route meets its gate.

---

## Implementation Order

1. **Catalog + ids in the summariser** (A, B): preorder id assignment in the DFS
   walk incl. the above-root reserved low-id block and the shared
   `DirIDAllocator` (B5); emit `wrstat_dirs`. Prove determinism, the interval
   invariant, and the overflow/non-contiguous guards. Foundation for everything;
   file ingest consumes the allocator when the writers land (phase 2, H2).
2. **Spool + writers carry ids** (H): rewrite spool format and ClickHouse
   writers/loader to emit the catalog + id-keyed rows; keep readiness/resume.
3. **Files table on `dir_id`** (C): rewrite `wrstat_files` and the file API.
4. **Facts + numeric filter tables on `dir_id`** (D, E): rewrite
   `wrstat_dir_facts` keyed by `dir_id`; numeric filter tables (retained); the
   in-query vector path; rewrite
   `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where`/`Children`.
5. **Glob / full-text** (F): catalog skip indexes.
6. **Basedirs + active virtual namespace** (G): id world + external fallback +
   virtual id space.
7. **Benchmark study** (J): baseline capture, full matrix, import/storage,
   gates, per-type deltas, per-pattern collapse decisions. Mandatory gate.
8. **(Optional)** in-memory navigation index (I): flag-gated, measured,
   non-blocking.

Phases 3-6 may proceed in parallel once 1-2 land (shared catalog, different
tables/queries). Phase 7 is the acceptance gate. Phase 8 must never delay 1-7.

---

## Appendix: Key Decisions

- **`UInt32` ids by default**, validated against the largest dataset's
  max-dirs-per-snapshot; widen to `UInt64` only if approaching 2^31. A hard
  `ErrTooManyDirs` overflow guard (B5) keeps real ids below `parentSentinel`, so
  no id ever collides with the sentinel. Codecs: `Delta, LZ4` for ids
  (dense/monotonic), `ZSTD(3)` for cold `full_path`/`name`.
- **`path_hash` primary resolver with mandatory `full_path` verification on
  hit** (exact resolution is under the hard 100 ms gate); droppable only if the
  `full_path` projection alone meets the gate. Verification makes collisions
  impossible to surface as wrong rows.
- **Filter materialisations retained by default**, made numeric; the in-query
  vector path is implemented for head-to-head benchmarking; collapse only with a
  cited measurement (D4). This is the prompt's explicit safer-than-trie stance.
- **`wrstat_parent_facts`, `wrstat_children`, and `wrstat_virtual_children`
  (string) are removed** - replaced by the catalog `parent_id` band,
  `subtree_end` ranges, and the virtual id space. This is not a filter-table
  collapse; it is the dedup core.
- **Above-root ancestor rows stay in the per-mount catalog** with a reserved
  low-id block (B2), preserving today's single-mount serving behaviour; they are
  not moved into the `active_set_id` layer. The active virtual overlay remains
  the sole authority for above-root/virtual paths (any strict prefix of a mount
  root); the B2 rows are never independently summed into those answers (G2
  precedence), matching baseline routing and avoiding double counting.
- **One shared `DirIDAllocator`** (B5) is injected into the DGUTA directory op
  and the file-ingest global op, so the global op (which runs before directory
  ops) writes `dir_id` instead of a parent path string. It guards
  subtree-contiguity (`ErrNonContiguousInput`) and overflow (`ErrTooManyDirs`).
- **Testing strategy:** GoConvey throughout (`So(...)`); `t.TempDir()` for FS;
  fixtures updated to the new schema; new tests prove the interval invariant,
  deterministic ids, path-hash collision safety, and materialised-vs-in-query
  filter parity. Memory-bounded tests for the streaming id assignment and spool.
  Implementors follow go-conventions; reviewers verify every acceptance test
  maps to a real GoConvey test (no stubs, no build-tag exclusions).
- **Error policy:** `ErrDirNotFound` semantics preserved; `ErrIDUnresolved` for
  an in-snapshot path that fails to resolve during readiness (readiness fails
  rather than emit a bad row); `ErrTooManyDirs` on `dir_id` overflow and
  `ErrNonContiguousInput` on a re-entered directory boundary (B1/B5).
- **Scratch artefacts** (prototype SQL, throwaway binaries, baseline binary,
  reports) live under `.tmp/agent/overhaul/`; baseline artefacts preserved for
  the before/after comparison.
