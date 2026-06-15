# ClickHouse Trie Path-ID Schema Specification

## Overview

Replace the ClickHouse path-string serving schema with a trie-native schema.
Each mount snapshot stores each directory path once, assigns deterministic
numeric directory IDs, and serves hot queries by `dir_id`, `parent_id`, and
preorder subtree ranges instead of repeated `dir` and `parent_dir` strings.

This is a clean unreleased-branch rewrite. Do not keep old string-heavy tables
as primary serving paths and do not write compatibility migrations for old
ClickHouse databases. External API strings, display basenames, synthetic active
virtual node paths, and documented fallback rows are the only path strings that
remain outside the directory catalog.

Implementation must not inspect, modify, import from, or depend on
`.docs/summarise`; that directory is outside this feature.

The completed implementation must prove correctness against current external
semantics and must archive before/after performance reports. Slower queries are
allowed only when correctness and absolute cold UX gates pass and the final
report makes the regression visible.

## Architecture

Primary package: `clickhouse/`. Shared trie helpers may live in
`clickhouse/trie_*.go` unless reuse outside ClickHouse justifies `internal/`.
Update `clickhouse/schema/*.sql`, `clickhouse/schema.go`, import writers,
spool rows, query files, basedirs readers/stores, server basedirs auth checks,
cleanup, and perf harnesses.
Because `schema.go` embeds all `clickhouse/schema/*.sql`, replace, remove, or
rewrite old string-heavy schema files as needed. A lone additive migration is
not enough if older embedded DDL still creates removed string tables or
columns.

Preserve these exported APIs:

```go
func (c *Client) StatPath(
  context.Context, string, StatOptions,
) (*FileRow, error)
func (c *Client) ListDir(
  context.Context, string, ListOptions,
) ([]FileRow, error)
func (c *Client) FindByGlob(
  context.Context, []string, []string, FindOptions,
) ([]FileRow, error)
func (c *Client) CountByGlob(
  context.Context, []string, []string, FindOptions,
) (int, error)
func (c *Client) PermissionAnyInDir(
  context.Context, string, uint32, []uint32,
) (bool, error)
func (c *Client) PermissionPath(
  context.Context, string, uint32, []uint32,
) (bool, error)
func (d *clickHouseDatabase) DirInfo(
  string, *db.Filter,
) (*db.DirSummary, error)
func (d *clickHouseDatabase) DirInfos(
  []string, *db.Filter,
) (map[string]*db.DirSummary, error)
func (d *clickHouseDatabase) DirsHaveChildren(
  []string, *db.Filter,
) (map[string]bool, error)
func (d *clickHouseDatabase) Where(
  string, *db.Filter, func(string) int,
) (db.DCSs, error)
func (d *clickHouseDatabase) Children(string) ([]string, error)
```

Add exact helper APIs in `clickhouse/trie_paths.go`:

```go
type TrieDir struct {
  MountPath      string
  SnapshotID     uuid.UUID
  DirID          uint64
  ParentID       uint64
  Name           string
  FullPath       string
  PathHash       uint64
  Depth          uint16
  SubtreeEnd     uint64
  ChildDirCount  uint64
  ChildFileCount uint64
}

func TriePathHash(path string) uint64
func ResolveTrieDir(ctx context.Context, conn driver.Conn, mountPath string,
  snapshotID uuid.UUID, fullPath string) (TrieDir, bool, error)
func ResolveTrieDirs(ctx context.Context, conn driver.Conn, mountPath string,
  snapshotID uuid.UUID, fullPaths []string) (map[string]TrieDir, error)
func TrieFilePath(parentFullPath, name string) string
```

`ResolveTrieDir` must query by `(mount_path, snapshot_id, path_hash)`, verify
`full_path`, and return `found=false` when only hash-collision rows exist.

Directory IDs are per `(mount_path, snapshot_id)`. The mount root has
`dir_id=1`, `parent_id=0`, and `full_path=mount_path`. Sibling order is bytewise
by basename without the trailing slash. `dir_id` is DFS preorder. `subtree_end`
is the exclusive upper bound after the last descendant, so descendants satisfy
`dir_id >= root.dir_id AND dir_id < root.subtree_end`.

Clean trie-native tables:

- `wrstat_dirs`: one row per directory. Columns:
  `mount_path`, `snapshot_id`, `dir_id UInt64`, `parent_id UInt64`,
  `name String`, `full_path String`, `path_hash UInt64`, `depth UInt16`,
  `subtree_end UInt64`, `child_dir_count UInt64`,
  `child_file_count UInt64`, `refreshed_at`. Partition by
  `(mount_path, snapshot_id)`. Order by `(mount_path, snapshot_id, dir_id)`.
  Add projections for `(mount_path, snapshot_id, path_hash, full_path)` and
  `(mount_path, snapshot_id, parent_id, name, dir_id)`.
- `wrstat_files`: same file metadata and `ext` index as today, but replace
  `parent_dir` and `path` with `parent_id UInt64`. Keep `name` and directory
  entry names exactly as today, including trailing `/`. Order by
  `(mount_path, snapshot_id, parent_id, name)`. The mount-root directory
  metadata row uses `parent_id=0` and the mount basename as `name`; direct
  children of the mount root use `parent_id=1`.
- `wrstat_dir_facts`: current summary payload keyed by `dir_id`, no `dir`.
- `wrstat_child_facts`: numeric replacement for `wrstat_parent_facts`, keyed by
  `(parent_id, dir_id)` and carrying the same summary packet payload plus
  `has_children`.
- `wrstat_child_filter_all`: numeric replacement for child filter packets,
  keyed by `(parent_id, age, gid, uid, ft, dir_id)`.
- `wrstat_dir_filter_all` and `wrstat_dir_filter_ageall`: numeric exact/subtree
  fact rows keyed by `dir_id`; add projections for exact `dir_id` lookup and
  filtered subtree range scans.
- `wrstat_active_virtual_nodes`: one row per synthetic active overlay node
  (`/`, `/lustre/`, `/nfs/`, or another virtual parent) or mount link. Columns:
  `active_set_id String`, `node_id UInt64`, `parent_node_id UInt64`,
  `name String`, `node_type String`, `full_path String` populated for synthetic
  nodes only, `(mount_path, snapshot_id, dir_id)` populated for mount links
  only and nullable or zero otherwise, child counts, and `refreshed_at`.
  Synthetic nodes are not rows in `wrstat_dirs`.
- `wrstat_active_virtual_summaries`: numeric replacement for active virtual
  summaries and old `wrstat_active_prefix_rollups`, keyed by
  `(active_set_id, node_id)`.
- `wrstat_active_virtual_filter_all`: numeric replacement for active virtual
  filtered summaries and old `wrstat_active_prefix_filter_ageall`, keyed by
  `(active_set_id, node_id, age, gid, uid, ft)`.
- `wrstat_active_virtual_edges`: one row per overlay edge with
  `active_set_id`, `parent_node_id`, `child_node_id`, `child_name`,
  `child_type`, and `sort_name`; no full path strings.
- `wrstat_active_virtual_sets`: readiness rows keyed by `active_set_id`. Do not
  duplicate all mount-local full paths into active rows.
- `wrstat_trie_snapshot_sets`: readiness manifest with row counts and SHA256
  for all trie-native snapshot tables, including basedirs rows.

The old `wrstat_children`, `wrstat_virtual_children`,
`wrstat_virtual_children_sets`, and string-shaped
`wrstat_active_virtual_children` tables are removed. Direct child directories
come from `wrstat_dirs.parent_id` and numeric active virtual edge rows. The old
string-shaped `wrstat_parent_facts` is removed or replaced by
`wrstat_child_facts`.
The old `wrstat_active_prefix_rollups`,
`wrstat_active_prefix_filter_ageall`, and
`wrstat_active_prefix_rollup_sets` are removed. Their prefix summary payloads
are folded into the numeric active virtual tables above and covered by the
active-set manifest.

`active_set_id` is `hex(SHA256(canonical_active_set_input))`. The canonical
input is `generation\t<generation>\n` plus one line per active mount, sorted
bytewise by mount path:
`<mount_path>\t<snapshot_id>\t<updated_at_unix_nano>\n`.

Active basedirs/quota tables for group usage, user usage, group subdirs, and
user subdirs replace active `basedir` and `subdir` strings with numeric
`basedir_id UInt64` and `subdir_id UInt64` columns when the path is inside the
scanned active snapshot. A fallback row must have `path_kind='external'`,
`basedir_id=0` or `subdir_id=0` as applicable, and an original path string.
Trie readiness fails if an active in-snapshot basedir or subdir cannot resolve
to a node.
`wrstat_basedirs_history` may remain mount-path keyed because
`History(gid, path)` resolves history by active mount, not by a stored basedir
row.

Import must produce one deterministic ID map for files, directory facts, child
facts, filters, active overlays, basedirs rows, manifests, cleanup, direct
streaming imports, and spool publish. It may use ClickHouse staging tables or
local spools, but must not hold all file rows or all paths in memory. Required
large-scale algorithm:

1. Stream candidate mount-local directory paths from file parents, directory
   entries, `RecordDGUTA.Dir`, `RecordDGUTA.Children`, active basedirs, and
   subdirs. Do not add active virtual nodes (`/`, `/lustre/`, `/nfs/`) to
   `wrstat_dirs`.
2. Normalize paths with existing mount rules and inject missing ancestors.
3. External sort and de-duplicate full directory paths per mount snapshot.
4. Assign preorder IDs with an ancestor stack; close stack entries by setting
   `subtree_end` when the next path leaves their prefix.
5. External merge-join sorted files, facts, filters, active prefix rollups, and
   basedirs spools by full directory path to append numeric IDs.
6. Build active virtual overlay rows from ready mount roots and folded active
   prefix rollups. Synthetic overlay node IDs are separate from mount-local
   `dir_id` values.
7. Write readiness only after every trie-native table count and manifest hash
   matches; otherwise drop the new snapshot partitions and active-set
   partitions.

Path reconstruction must be batched and instrumentable. In one API call, load
all distinct parent IDs or result `dir_id` values with one
`wrstat_dirs ... IN (...)` query per mount snapshot, or zero queries on a
snapshot-scoped cache hit. Tests must fail if N returned rows cause N
ClickHouse path lookup queries. Cold gates may use per-request caches, but must
not depend on warmed cross-request process caches.

Errors:

- Missing directory path: existing `db.ErrDirNotFound` or
  `basedirs.ErrInvalidBasePath` semantics remain.
- Missing file path: existing `errPathNotFound` semantics remain.
- Hash collision: no wrong row may be returned; full-path verification decides.
- Not-ready trie snapshot: readers must not serve partial rows.
- Failed import or spool publish: cleanup removes every trie-native partition
  for the new snapshot and active set.

## Section A: Schema And Import

### A1: Clean Trie Schema

As an operator, I want the ClickHouse schema to store hot relationships as
integers, so that repeated full paths are removed from serving tables.

Replace schema SQL and bootstrap tests. Existing table names may be reused only
with numeric columns. No serving query may depend on `wrstat_children`,
`wrstat_virtual_children`, `wrstat_virtual_children_sets`, string-shaped
`wrstat_parent_facts`, string-shaped `wrstat_active_virtual_children`, or hot
`dir`/`parent_dir` keys outside `wrstat_dirs.full_path` and documented
fallback rows.
Do not merely add `019_trie_path_ids.sql` beside old embedded DDL. Remove or
rewrite old SQL that creates string-heavy serving tables.

**Package:** `clickhouse/`
**File:** `clickhouse/schema/*.sql`, `clickhouse/schema.go`
**Test file:** `clickhouse/clean_schema_test.go`

**Acceptance tests:**

1. Given a clean bootstrap, when schema metadata is read, then `wrstat_dirs`
   exists with `dir_id`, `parent_id`, `full_path`, `path_hash`, `depth`, and
   `subtree_end`, and its primary order is
   `(mount_path, snapshot_id, dir_id)`.
2. Given a clean bootstrap, when `wrstat_files` columns are read, then it has
   `parent_id UInt64`, `name`, `ext`, and all existing metadata fields, and it
   has no `parent_dir` column and no `path` alias.
3. Given a clean bootstrap, when table names are listed, then
   `wrstat_children`, `wrstat_parent_facts`, `wrstat_virtual_children`,
   `wrstat_virtual_children_sets`, `wrstat_active_virtual_children`,
   `wrstat_active_prefix_rollups`,
   `wrstat_active_prefix_filter_ageall`, and
   `wrstat_active_prefix_rollup_sets` are absent, and `wrstat_child_facts`,
   `wrstat_active_virtual_nodes`, `wrstat_active_virtual_edges`,
   `wrstat_active_virtual_summaries`,
   `wrstat_active_virtual_filter_all`, and
   `wrstat_active_virtual_sets` are present.
4. Given a clean bootstrap, when `wrstat_child_facts` columns are inspected,
   then it has `parent_id UInt64`, `dir_id UInt64`, and `has_children`, and it
   has no `parent_dir` or `dir` string column.
5. Given active virtual columns are inspected, then
   `wrstat_active_virtual_nodes`, `wrstat_active_virtual_edges`,
   `wrstat_active_virtual_summaries`, and
   `wrstat_active_virtual_filter_all` are keyed by `node_id`,
   `parent_node_id`, `child_node_id`, or mount-link `dir_id`; none has
   `parent_dir`, `child_dir`, `child`, or `dir` string columns.
6. Given all trie-native hot tables, when their columns are inspected, then no
   table except `wrstat_dirs.full_path`,
   `wrstat_active_virtual_nodes.full_path` for synthetic nodes, and explicit
   fallback columns contains `dir`, `parent_dir`, `child_dir`, `basedir`, or
   `subdir` full-path string columns.
7. Given a clean bootstrap, when active virtual columns are inspected, then
   `wrstat_active_virtual_nodes` has `active_set_id`, `node_id`,
   `parent_node_id`, `name`, `node_type`, `full_path`, `mount_path`,
   `snapshot_id`, and `dir_id`; `wrstat_active_virtual_edges` has
   `active_set_id`, `parent_node_id`, `child_node_id`, `child_name`,
   `child_type`, and `sort_name`; and neither table stores mount-local full
   paths; only synthetic nodes use `full_path`.

### A2: Deterministic ID Assignment

As an importer, I want one deterministic trie per mount snapshot, so that all
derived rows agree on IDs and subtree ranges.

Build IDs from an external sorted directory stream. Root is ID 1. Parent IDs,
depth, child counts, and `subtree_end` are computed during the same pass.

**Package:** `clickhouse/`
**File:** `clickhouse/trie_builder.go`
**Test file:** `clickhouse/trie_builder_test.go`

**Acceptance tests:**

1. Given mount `/mnt/` and paths `/mnt/`, `/mnt/a/`, `/mnt/a/b/`, `/mnt/c/`,
   when trie rows are built, then rows are exactly:

   ```text
   full_path  dir_id  parent_id  depth  subtree_end
   /mnt/      1       0          0      5
   /mnt/a/    2       1          1      4
   /mnt/a/b/  3       2          2      4
   /mnt/c/    4       1          1      5
   ```

2. Given the same paths in reverse order and with duplicate `/mnt/a/`, when the
   builder runs twice, then both outputs match the table above byte-for-byte.
3. Given only `/mnt/a/b/` plus root, when ancestors are injected, then
   `/mnt/a/` is present with `parent_id=1` and `/mnt/a/b/` has `parent_id=2`.
4. Given 1,000,000 generated directory paths, when the builder runs with an
   external spool, then heap growth is less than 20 MiB and the row count is
   exactly 1,000,000 plus injected ancestors.
5. Given the paths above plus files `/mnt/a/f.txt`, `/mnt/a/g.txt`, and
   `/mnt/c/z.dat`, when trie rows are built, then `/mnt/` has
   `child_dir_count=2` and `child_file_count=0`, `/mnt/a/` has
   `child_dir_count=1` and `child_file_count=2`, `/mnt/a/b/` has
   `child_dir_count=0` and `child_file_count=0`, and `/mnt/c/` has
   `child_dir_count=0` and `child_file_count=1`.
6. Given mount `/mnt/` and paths `/mnt/`, `/mnt/a/`, `/mnt/a/b/`,
   `/mnt/a/z/`, `/mnt/a-b/`, and `/mnt/a.b/`, when trie rows are built, then
   component-aware preorder rows are exactly:

   ```text
   full_path    dir_id  parent_id  depth  subtree_end
   /mnt/        1       0          0      7
   /mnt/a/      2       1          1      5
   /mnt/a/b/    3       2          2      4
   /mnt/a/z/    4       2          2      5
   /mnt/a-b/    5       1          1      6
   /mnt/a.b/    6       1          1      7
   ```

### A3: Trie-Native Import And Publish

As an importer, I want direct streaming and spool publish to write the same
trie rows, so that retry, readiness, and cleanup remain correct.

Update `fileIngestWriter`, `dgutaWriter`, `chBaseDirsStore`,
`summariseSpoolLoader`, and `internal/chspool` row structs. Every row that
references an active snapshot directory must carry the resolved `dir_id`.

**Package:** `clickhouse/`
**File:** `clickhouse/dguta_writer.go`
**Test file:** `clickhouse/dguta_writer_test.go`

**Acceptance tests:**

1. Given one file `/mnt/a/f.txt` and one directory summary for `/mnt/a/`, when
   direct import closes, then `wrstat_dirs` has `/mnt/` and `/mnt/a/`,
   `wrstat_files.parent_id` equals `/mnt/a/`'s `dir_id`, and
   `wrstat_dir_facts.dir_id` equals the same ID; `/mnt/` has
   `child_dir_count=1` and `child_file_count=0`, and `/mnt/a/` has
   `child_dir_count=0` and `child_file_count=1`.
2. Given the same dataset through `summarise_spool`, when the spool is loaded,
   then sorted `wrstat_dirs`, `wrstat_files`, `wrstat_dir_facts`,
   `wrstat_child_facts`, filter rows, active virtual summary/filter rows, and
   basedirs rows are byte-for-byte equal to direct import after ignoring
   manifest timestamps.
3. Given an import fails after writing files but before readiness, when cleanup
   runs, then row counts for every trie-native snapshot and active virtual table
   are 0 and no `wrstat_mounts_active` row points at the failed snapshot.
4. Given an old active snapshot is replaced, when publish completes, then all
   old trie-native snapshot partitions and old active-set partitions are
   dropped, including folded active-prefix summary/filter rows, and the new
   active snapshot remains queryable.

### A4: Manifest Readiness And Cleanup

As an operator, I want manifests to gate serving, so that partial trie data is
never visible.

Every trie-native snapshot and active-set table has a manifest entry with row
count and SHA256. Readiness is written only when actual counts and hashes match.
Cleanup removes inactive, tombstoned, failed, and replaced trie partitions using
the same table list.

**Package:** `clickhouse/`
**File:** `clickhouse/trie_readiness.go`
**Test file:** `clickhouse/trie_readiness_test.go`

**Acceptance tests:**

1. Given a manifest expecting 2 `wrstat_dirs` rows and ClickHouse contains 1,
   when readiness validation runs, then it returns `ErrManifestMismatch`, no
   ready row is written, and `StatPath` and `DirInfo` do not serve that
   snapshot.
2. Given a manifest whose `wrstat_files` SHA256 is `abc` and the actual table
   hash is `def`, when readiness validation runs, then it returns
   `ErrManifestMismatch`, `wrstat_mounts_active` is unchanged, and the new
   snapshot is not queryable.
3. Given an active-set manifest expecting 3
   `wrstat_active_virtual_summaries` rows and the table contains 2, when
   active readiness validation runs, then `Children("/")`,
   `DirInfo("/")`, and filtered `DirInfo("/nfs/", filter)` do not read that
   active set.
4. Given a snapshot is inactive, tombstoned, or replaced, when cleanup runs,
   then rows for that `(mount_path, snapshot_id)` are 0 in `wrstat_dirs`,
   `wrstat_files`, `wrstat_dir_facts`, `wrstat_child_facts`, all trie filter
   tables, and all trie-normalized basedirs tables.
5. Given an active set is replaced, when cleanup runs, then rows for the old
   `active_set_id` are 0 in active virtual nodes, edges, summaries, filters,
   and sets; the new active set remains ready.

## Section B: File And Path APIs

### B1: Exact Directory Resolution

As a query planner, I want exact path-to-ID lookup to be collision-safe, so that
all downstream integer predicates use the intended directory.

`ResolveTrieDir` and `ResolveTrieDirs` use path hashes only as an index. They
must verify `full_path` before returning a row.

**Package:** `clickhouse/`
**File:** `clickhouse/trie_paths.go`
**Test file:** `clickhouse/trie_paths_test.go`

**Acceptance tests:**

1. Given `wrstat_dirs` has `/mnt/` as `dir_id=1` and `/mnt/a/` as `dir_id=2`,
   when `ResolveTrieDir(ctx, conn, "/mnt/", sid, "/mnt/a/")` runs, then it
   returns `found=true`, `DirID=2`, `ParentID=1`, and `FullPath="/mnt/a/"`.
2. Given the same rows, when `ResolveTrieDir` runs for `/mnt/missing/`, then it
   returns `found=false`, a zero `TrieDir`, and `err=nil`.
3. Given rows `/mnt/a/` with `dir_id=2` and `/mnt/c/` with `dir_id=3`, when
   `ResolveTrieDirs` runs for `["/mnt/c/","/mnt/missing/","/mnt/a/",
   "/mnt/a/"]`, then the map has exactly keys `"/mnt/a/"` and `"/mnt/c/"`,
   with IDs `2` and `3`.
4. Given rows `/mnt/a/` with `dir_id=2` and `/mnt/collision/` with `dir_id=9`
   both store `path_hash=TriePathHash("/mnt/a/")`, when resolving `/mnt/a/`,
   then the returned row is `dir_id=2` and never `dir_id=9`.
5. Given only `/mnt/collision/` stores
   `path_hash=TriePathHash("/mnt/missing/")`, when resolving
   `/mnt/missing/`, then `found=false` and the collision row is not returned.

### B2: Exact Path, List, And Reconstruction

As an API caller, I want file and directory paths to look unchanged, so that the
schema rewrite is invisible at request and response boundaries.

Path resolution is: resolve mount, resolve parent full path to `parent_id` via
`wrstat_dirs`, query `wrstat_files` by `(parent_id, name)`, and reconstruct
`FileRow.Path` and `FileRow.ParentDir` from the directory catalog. Mount-root
paths are valid: `ListDir` resolves them to `dir_id=1`, while `StatPath` reads
the root metadata row keyed by `parent_id=0`.

**Package:** `clickhouse/`
**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/file_api_test.go`

**Acceptance tests:**

1. Given active mount `/mnt/`, directory `/mnt/a/`, and file row
   `parent_id=id(/mnt/a/)`, `name=f.txt`, `size=7`, when
   `StatPath(ctx, "/mnt/a/f.txt", StatOptions{})` is called, then the row has
   `Path="/mnt/a/f.txt"`, `ParentDir="/mnt/a/"`, `Name="f.txt"`, and
   `Size=7`.
2. Given directory entry `parent_id=id(/mnt/)`, `name="a/"`, when
   `StatPath(ctx, "/mnt/a/", StatOptions{})` is called, then the row has
   `Path="/mnt/a/"`, `ParentDir="/mnt/"`, and `Name="a/"`.
3. Given children `b.txt`, `a.txt`, and `z/` under `/mnt/a/`, when
   `ListDir(ctx, "/mnt/a/", ListOptions{Limit: 2, Offset: 1})` is called,
   then names are exactly `["b.txt", "z/"]`.
4. Given a missing parent path `/mnt/missing/`, when `ListDir` or `StatPath`
   resolves it, then the existing not-found error is returned and no
   `wrstat_files` scan is issued.
5. Given query recording is enabled and `StatPath(ctx, "/mnt/a/f.txt",
   StatOptions{})` returns one file, when queries are inspected, then there is
   one parent directory resolution query, one `wrstat_files` query by
   `(parent_id, name)`, zero `wrstat_files` joins to `wrstat_dirs`, and no
   extra parent-path lookup after the file row is read.
6. Given 50 files and 50 directory entries under `/mnt/a/`, when
   `ListDir(ctx, "/mnt/a/", ListOptions{})` returns 100 rows, then parent path
   reconstruction uses at most one `wrstat_dirs` batch lookup for returned
   `parent_id` values, or zero on a snapshot cache hit; it never issues one
   ClickHouse lookup per returned row, and every row has
   `ParentDir="/mnt/a/"`.
7. Given active mount `/mnt/root/` has root metadata row `parent_id=0`,
   `name="root/"`, `uid=30`, and `gid=40`, when
   `StatPath(ctx, "/mnt/root/", StatOptions{})` is called, then it returns no
   invalid-path or not-found error and the row has `Path="/mnt/root/"`,
   `ParentDir="/mnt/"`, `Name="root/"`, `EntryType=byte(stats.DirType)`,
   `UID=30`, and `GID=40`.
8. Given active mount `/mnt/root/` has `dir_id=1` and direct children
   `a.txt`, `b.txt`, and `z/` with `parent_id=1`, when
   `ListDir(ctx, "/mnt/root/", ListOptions{Limit: 2, Offset: 1})` is called,
   then names are exactly `["b.txt", "z/"]`, every row has
   `ParentDir="/mnt/root/"`, and the query uses `parent_id=1`.

### B3: Glob Find And Count

As a file API caller, I want glob matching to match current behavior without
stored full file paths, so that searches remain exact.

Direct-child glob uses `parent_id = ?` and `name` matches. Recursive glob first
resolves base directories, prunes by `dir_id >= base_id AND dir_id <
subtree_end`, then reconstructs candidate paths for exact RE2 matching. The
extension shortcut may use `ext`, but must still verify the glob. `CountByGlob`
must return the exact count after the same de-duplication semantics as
`FindByGlob`.

**Package:** `clickhouse/`
**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/client_file_api_test.go`

**Acceptance tests:**

1. Given `/mnt/a/file.txt`, `/mnt/a/.env`, `/mnt/a/b/file.txt`, and
   `/mnt/a/b/file.dat`, when `FindByGlob(["/mnt/a/"], ["*"], no owner)` runs,
   then returned paths are exactly `["/mnt/a/.env", "/mnt/a/file.txt"]`.
2. Given the same rows, when `FindByGlob(["/mnt/a/"], ["**/*.txt"], no owner)`
   runs, then returned paths are exactly
   `["/mnt/a/b/file.txt", "/mnt/a/file.txt"]`.
3. Given the same rows, when `FindByGlob(["/mnt/a/"], [".*"], no owner)` runs,
   then returned paths are exactly `["/mnt/a/.env"]`.
4. Given `file.txt` has `uid=10,gid=20` and `.env` has `uid=11,gid=21`, when
   owner-required glob runs with `uid=10,gids=[30]`, then only
   `/mnt/a/file.txt` is returned.
5. Given `/mnt/a/.bam`, `/mnt/a/a.bam`, `/mnt/a/b.BAM`,
   `/mnt/a/a.tar.gz`, `/mnt/a/sub/owned.bam`, and
   `/mnt/a/sub/fake.cram` whose stored `ext` is `bam`, when
   `FindByGlob(["/mnt/a/"], ["*.bam"], no owner)` runs, then returned paths are
   exactly `["/mnt/a/.bam", "/mnt/a/a.bam"]`; when `["*.tar.gz"]` runs, then
   returned paths are exactly `["/mnt/a/a.tar.gz"]`; when `["**/*.bam"]` runs,
   then returned paths are exactly `["/mnt/a/.bam", "/mnt/a/a.bam",
   "/mnt/a/sub/owned.bam"]`.
6. Given the recursive bam match set above, when `FindByGlob` runs with
   `Limit=2, Offset=1`, then returned paths are exactly
   `["/mnt/a/a.bam", "/mnt/a/sub/owned.bam"]`; with `Offset=3`, it returns an
   empty slice.
7. Given the recursive txt case above, when `CountByGlob` runs with the same
   inputs, then it returns `2`; with `Limit=1, Offset=1`, it returns `1`.
8. Given the extension dataset above, when `CountByGlob` runs for `*.bam`,
   `**/*.bam`, `*.tar.gz`, and `.*`, then it returns `2`, `3`, `1`, and `1`
   respectively.

### B4: Permissions

As an auth caller, I want permission checks to use IDs but keep current truth
values, so that protected routes do not change behavior.

`PermissionPath` resolves a parent ID and checks the exact file row. Exact
directory permissions use the directory entry row when the requested path names
a directory. Mount-root exact permissions use the root metadata row
`parent_id=0`. `PermissionAnyInDir` resolves `dir_id` and checks numeric
directory facts, including `dir_id=1` at a mount root.

**Package:** `clickhouse/`
**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/file_api_test.go`

**Acceptance tests:**

1. Given `/mnt/a/file.txt` has `uid=10,gid=20`, when `PermissionPath` is called
   with `(uid=10,gids=[])`, then it returns `true`.
2. Given the same file, when `PermissionPath` is called with
   `(uid=11,gids=[20])`, then it returns `true`.
3. Given the same file, when `PermissionPath` is called with
   `(uid=11,gids=[21])`, then it returns `false`.
4. Given directory entry `/mnt/a/` has `parent_id=id(/mnt/)`, `name="a/"`,
   `uid=30`, and `gid=40`, when `PermissionPath(ctx, "/mnt/a/", 30, [])` runs,
   then it returns `true`; with `(uid=31,gids=[40])` it returns `true`; with
   `(uid=31,gids=[41])` it returns `false`; and the exact directory entry row,
   not `wrstat_dir_facts`, supplies the permission fields.
5. Given `/mnt/a/` directory facts include age-all `(uid=30,gid=40)`, when
   `PermissionAnyInDir(ctx, "/mnt/a/", 31, []uint32{40})` runs, then it returns
   `true`; with `uid=31,gids=[41]`, it returns `false`.
6. Given active mount `/mnt/root/` has root metadata row `parent_id=0`,
   `name="root/"`, `uid=30`, and `gid=40`, and root directory facts for
   `dir_id=1` include age-all `(uid=30,gid=40)`, when
   `PermissionPath(ctx, "/mnt/root/", 30, [])` runs, then it returns `true`;
   with `(uid=31,gids=[40])` it returns `true`; with `(uid=31,gids=[41])` it
   returns `false`; and `PermissionAnyInDir(ctx, "/mnt/root/", 31,
   []uint32{40})` returns `true` without an invalid-path or not-found error.

## Section C: Tree, Summaries, And Active Namespace

### C1: Directory Facts And Tree Queries

As a tree API caller, I want `DirInfo`, `DirInfos`, `DirsHaveChildren`,
`Children`, and `Where` to return unchanged paths and summaries from numeric
tables, so that UI and CLI behavior is preserved.

Exact summaries use `dir_id`. Direct children use `parent_id`. Recursive
subtrees use `[dir_id, subtree_end)`. `Where` must prefer numeric subtree range
scans, not string prefix ranges. Returned `DirSummary.Dir` and child paths are
reconstructed from `wrstat_dirs.full_path`; `Children` returns child paths
without a trailing slash, matching current `wrstat_children.child`.

**Package:** `clickhouse/`
**File:** `clickhouse/database.go`
**Test file:** `clickhouse/database_dirinfo_test.go`

**Acceptance tests:**

1. Given `/mnt/a/` has summary count `3`, size `30`, child dirs `b/` and `c/`,
   when `DirInfo("/mnt/a/", nil)` runs, then `Current.Dir="/mnt/a/"`,
   `Current.Count=3`, `Current.Size=30`, and children are `["/mnt/a/b",
   "/mnt/a/c"]`.
2. Given filter rows for `/mnt/a/b/` with age all, gid `7`, uid `8`, ft
   `regular`, count `2`, size `20`, when `DirInfo("/mnt/a/b/",
   &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{8}, FT: regular,
   Age: db.DGUTAgeAll})` runs, then `Count=2` and `Size=20`.
3. Given dirs `/mnt/a/`, `/mnt/a/b/`, and `/mnt/a/c/`, when
   `DirsHaveChildren([]string{"/mnt/a/","/mnt/a/b/"}, nil)` runs, then the map
   is `{"/mnt/a/": true, "/mnt/a/b/": false}`.
4. Given `/mnt/a/` has direct children `/mnt/a/hit/` and `/mnt/a/miss/`,
   child filter rows give `/mnt/a/hit/` age all, gid `7`, uid `8`, ft
   `regular`, count `4`, and `/mnt/a/miss/` gid `9`, when
   `DirsHaveChildren([]string{"/mnt/a/","/mnt/a/hit/","/mnt/missing/"},
   &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{8}, FT: regular,
   Age: db.DGUTAgeAll})` runs, then the map is `{"/mnt/a/": true,
   "/mnt/a/hit/": false, "/mnt/missing/": false}`.
5. Given the same tree, when `DirsHaveChildren([]string{"/mnt/a/"},
   &db.Filter{GIDs: []uint32{42}, Age: db.DGUTAgeAll})` runs, then the map is
   `{"/mnt/a/": false}`; with `&db.Filter{GIDs: []uint32{},
   Age: db.DGUTAgeAll}` it is also `{"/mnt/a/": false}`; with `nil` filter it
   is `{"/mnt/a/": true}`. Filtered checks read numeric child filter facts by
   `(parent_id, age, gid, uid, ft, dir_id)`, not by `parent_dir`.
6. Given the same tree, when `Where("/mnt/a/", nil, split.SplitsToSplitFn(1))`
   runs, then every result `Dir` starts with `/mnt/a/`, no result outside
   `dir_id >= id(/mnt/a/) AND dir_id < subtree_end(/mnt/a/)` is read, and the
   sorted result digest equals the current generic tree digest for the fixture.
7. Given summaries for `/mnt/a/`, `/mnt/a/b/`, and `/mnt/z/`, when focused
   `DirInfos([]string{"/mnt/z/","/mnt/a/"}, nil)` runs, then the map has
   exactly those two keys with their exact `Dir`, `Count`, `Size`, `GIDs`, and
   `UIDs`.
8. Given 1,000 children under `/mnt/wide/` with counts equal to their numeric
   suffix, when broad `DirInfos(childPaths, nil)` runs, then it returns exactly
   1,000 summaries, every requested child path is present once, and the sorted
   result digest equals the current generic tree digest for the fixture.
9. Given filter rows under `/mnt/a/` where `/mnt/a/hit/` has gid `7`, uid `8`,
   ft `regular`, count `4`, size `40`, and `/mnt/a/miss/` has gid `9`, when
   `Where("/mnt/a/", &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{8},
   FT: regular, Age: db.DGUTAgeAll}, split.SplitsToSplitFn(1))` runs, then
   results are exactly under
   `/mnt/a/hit/`, `Count=4`, `Size=40`, and no row outside
   `[id(/mnt/a/), subtree_end(/mnt/a/))` is read.
10. Given query recording is enabled and broad `DirInfos(childPaths, nil)`
   returns 1,000 child summaries, when queries are inspected, then full path
   reconstruction uses at most one `wrstat_dirs` batch lookup for the 1,000
   result `dir_id` values, or the summary query returns all full paths itself;
   it never issues one ClickHouse lookup per summary row, and the result digest
   matches the current generic tree digest.

### C2: Active Virtual Namespace

As a UI user, I want `/`, `/lustre/`, `/nfs/`, and active mount roots to behave
as one tree, so that Disktree and summaries are unchanged.

Represent synthetic overlay nodes, mount links, and folded active-prefix
summaries numerically. `/`, `/lustre/`, `/nfs/`, and intermediate virtual
parents live only in `wrstat_active_virtual_nodes`, never in per-mount
`wrstat_dirs`. A mount link points to `(mount_path, snapshot_id, dir_id=1)` for
that active mount. Active set ID uses the canonical SHA256 input defined in
Architecture.

**Package:** `clickhouse/`
**File:** `clickhouse/active_virtual_overlay.go`
**Test file:** `clickhouse/database_test.go`

**Acceptance tests:**

1. Given active mounts `/lustre/scratch120/` and `/nfs/t283_imaging/`, when
   `Children("/")` runs, then it returns exactly `["/lustre", "/nfs"]`.
2. Given the same mounts, when `Children("/nfs/")` runs, then it returns exactly
   `["/nfs/t283_imaging"]`.
3. Given mount root summaries count `10` for Lustre and `20` for NFS, when
   `DirInfo("/")` runs, then `Count=30`; when `DirInfo("/nfs/")` runs, then
   `Count=20`.
4. Given active mounts `/lustre/scratch120/` and `/nfs/t283_imaging/`, when the
   overlay is built, then `wrstat_active_virtual_nodes` contains synthetic rows
   for `/`, `/lustre/`, and `/nfs/`, mount-link rows for `scratch120` and
   `t283_imaging`, and no `wrstat_dirs` row for either mount snapshot has
   `full_path` equal to `/`, `/lustre/`, or `/nfs/`.
5. Given generation `42` and active mounts
   `/lustre/scratch120/` snapshot
   `11111111-1111-1111-1111-111111111111` updated at
   `1700000000000000001`, and `/nfs/t283_imaging/` snapshot
   `22222222-2222-2222-2222-222222222222` updated at
   `1700000000000000002`, when active-set ID is generated from either input
   order, then it is
   `6579df4070110db34bb41c729d7c40a7247b5d3b39f44c589adc38498364b74c`; when
   generation changes to `43`, the ID is different.
6. Given that active set, when overlay tables are inspected, then nodes are:

   ```text
   node_id  parent_node_id  node_type   name          full_path  dir_id
   1        0               synthetic   ""            /          0
   2        1               synthetic   lustre        /lustre/   0
   3        2               mount_link  scratch120    ""         1
   4        1               synthetic   nfs           /nfs/      0
   5        4               mount_link  t283_imaging  ""         1
   ```

   and edges are exactly `(1,2,"lustre")`, `(2,3,"scratch120")`,
   `(1,4,"nfs")`, and `(4,5,"t283_imaging")` keyed by the same
   `active_set_id`.
7. Given a refreshed active set with one mount removed, when cleanup completes,
   then old `active_set_id` rows in every active virtual trie table are 0 and
   the new active set remains ready.
8. Given active virtual summaries for `/`, `/lustre/`, and `/nfs/`, when their
   stored rows are inspected, then they are keyed by `node_id`, have no `dir`
   string column, and contain counts `30`, `10`, and `20`.
9. Given `wrstat_active_virtual_filter_all` has a `/nfs/` node row for age all,
   gid `7`, uid `8`, ft `regular`, count `5`, and size `50`, when
   `DirInfo("/nfs/", &db.Filter{GIDs: []uint32{7}, UIDs: []uint32{8},
   FT: regular, Age: db.DGUTAgeAll})` runs, then it returns `Count=5`,
   `Size=50`, and reads active virtual filters by `(active_set_id, node_id)`.
10. Given `/nfs/` has one mount-link child with active virtual filter count
   `5` for age all, gid `7`, uid `8`, ft `regular`, when
   `DirsHaveChildren([]string{"/nfs/"}, &db.Filter{GIDs: []uint32{7},
   UIDs: []uint32{8}, FT: regular, Age: db.DGUTAgeAll})` runs, then the map is
   `{"/nfs/": true}`; with gid `42` it is `{"/nfs/": false}`; and the check
   uses `(active_set_id, node_id)` facts or numeric overlay edges, not
   `wrstat_active_virtual_children`.
11. Given a folded active-prefix summary row has a manifest hash mismatch, when
   active-set readiness is checked, then `DirInfo("/")` and
   `DirInfo("/nfs/", filter)` do not serve that active set.

### C3: Basedirs And Quota Readers

As a basedirs API caller, I want active basedir and subdir rows normalized to
directory IDs while responses keep path strings, so that quota views shrink
without changing REST output.

Resolve active `Usage.BaseDir`, `SubDirKey.BaseDir`, and `SubDir.SubDir` to
`dir_id` before writing. Readers batch-load required full paths from
`wrstat_dirs`. Historical rows that are not active trie directories may keep
their explicit fallback path. Final perf evidence for readers that can mix
both path kinds must split active-trie, fallback, and mixed cases.

**Package:** `clickhouse/`, `server/`
**File:** `clickhouse/basedirs_store.go`, `server/basedirs.go`
**Test file:** `clickhouse/basedirs_reader_test.go`, `server/server_test.go`

**Acceptance tests:**

1. Given active basedir `/mnt/projects/p1/`, when `PutGroupUsage` writes it,
   then `wrstat_basedirs_group_usage.basedir_id` equals
   `id(/mnt/projects/p1/)`, fallback path is empty, and `GroupUsage` returns
   `BaseDir="/mnt/projects/p1/"`.
2. Given active basedir `/mnt/projects/p1/`, when `PutUserUsage` writes it,
   then `wrstat_basedirs_user_usage.basedir_id` equals
   `id(/mnt/projects/p1/)`, fallback path is empty, and `UserUsage` returns
   `BaseDir="/mnt/projects/p1/"`.
3. Given active subdir `/mnt/projects/p1/run1/`, when `PutGroupSubDirs` writes
   it, then `subdir_id=id(/mnt/projects/p1/run1/)` and `GroupSubDirs` returns
   `SubDir="/mnt/projects/p1/run1/"` in the original `pos` order.
4. Given active subdir `/mnt/projects/p1/run2/`, when `PutUserSubDirs` writes
   it, then `subdir_id=id(/mnt/projects/p1/run2/)` and `UserSubDirs` returns
   `SubDir="/mnt/projects/p1/run2/"` in the original `pos` order.
5. Given an active basedir or subdir inside `/mnt/` is absent from
   `wrstat_dirs`, when readiness is written, then readiness fails with an error
   containing `unresolved active basedir` or `unresolved active subdir` and the
   new snapshot is not served.
6. Given a history row for gid `7` on mount `/mnt/`, when `History(7,
   "/mnt/any/path")` runs, then it still returns that mount history without
   requiring a `dir_id`.
7. Given active row `/mnt/projects/p1/` and fallback row `/external/archive/`,
   when `GroupUsage`, `UserUsage`, `GroupSubDirs`, and `UserSubDirs` run, then
   active responses use paths loaded from `wrstat_dirs`, fallback responses use
   the original fallback string, and test evidence shows active ClickHouse rows
   have `path_kind='trie'` with empty fallback path.
8. Given basedirs auth is enabled and the caller has allowed GIDs `[7]`, when
   `GET /auth/basedirs/group/subdirs?id=8&basedir=/mnt/projects/p1/` runs, then
   it returns `[]` without calling `GroupSubDirs`; with `id=7`, it returns the
   exact trie-reconstructed subdir rows.
9. Given basedirs auth is enabled and `DirInfo("/mnt/projects/p1/")` has GIDs
   `[9]`, when the caller with allowed GIDs `[7]` requests
   `GET /auth/basedirs/user/subdirs?id=101&basedir=/mnt/projects/p1/`, then it
   returns `[]`; when `DirInfo` has GIDs `[7]`, it returns the exact
   trie-reconstructed user subdir rows.

### C4: Server And CLI Query Surfaces

As a UI or CLI caller, I want Disktree and `where --dir` to match database
semantics, so that external navigation is unchanged.

REST tree endpoints, auth tree endpoints, REST where, and CLI `where` must call
the trie-native database paths and preserve current JSON fields, status codes,
ordering, auth flags, and digests.

**Package:** `server/`, `cmd/`
**File:** `server/tree.go`, `server/where.go`, `cmd/where.go`
**Test file:** `server/server_test.go`, `cmd/where_test.go`

**Acceptance tests:**

1. Given active mounts `/lustre/scratch120/` and `/nfs/t283_imaging/`, when the
   Disktree REST endpoint opens `/`, `/lustre/`, `/nfs/`, and
   `/nfs/t283_imaging/`, then each response is HTTP 200 and its path list,
   child order, summaries, and result digest equal the current tree fixture.
2. Given high-fanout parent `/mnt/wide/` with children `child000/`,
   `child001/`, `child010/`, and 997 more bytewise sorted children, when the
   Disktree endpoint opens `/mnt/wide/`, then all 1,000 children are returned
   once in bytewise path order and no child outside `parent_id=id(/mnt/wide/)`
   is present.
3. Given `where --dir /mnt/a/ --groups g7 --json` against a fixture with hits
   only under `/mnt/a/hit/`, when the command runs, then JSON contains exactly
   the same dirs, counts, sizes, and atime values as
   `Where("/mnt/a/", &db.Filter{GIDs: []uint32{7}},
   split.SplitsToSplitFn(defaultWhereSplits))`.
4. Given auth tree marks `/mnt/closed/` as disallowed and `/mnt/open/` as
   allowed, when the auth Disktree and auth where endpoints run, then `NoAuth`
   flags and restricted result digests match the current auth fixture.

## Section D: Evidence And Gates

### D1: Baseline And Final Comparison Reports

As a maintainer, I want mandatory before/after evidence, so that correctness
and performance changes are visible for every important query.

Capture a current-branch baseline before trie schema edits begin, using a
separate worktree or preserved binary when needed. Extend `internal/chperf`,
`cmd/clickhouse_perf.go`, and Bolt comparison harnesses instead of inventing a
new report format.

**Package:** `internal/chperf/`
**File:** `internal/chperf/query.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given baseline and trie reports, when final comparison validation runs, then
   it fails unless both reports contain import wall time, import cleanup wall
   time, active snapshot cleanup wall time, CPU where available, max RSS,
   spool bytes, ClickHouse compressed and uncompressed bytes by table, part
   counts, rows written, cache mode, p50/p95/p99 latency, rows read, bytes
   read, granules read or read marks, result counts, result digests, and query
   delta status `faster`, `slower`, or `neutral`.
2. Given final reports, when operation names are checked, then all categories
   are present: exact directory resolution, exact file stat, list direct
   children, permission path, permission any-in-dir, direct-child glob,
   recursive glob, extension glob, dotfile glob, `CountByGlob` for those glob
   shapes, broad and filtered `DirInfo`, broad and focused `DirInfos`, broad
   and filtered
   `DirsHaveChildren`, root Disktree, `/lustre/` Disktree, `/nfs/` Disktree,
   mount-root Disktree, high-fanout Disktree, broad and filtered `Tree.Where`,
   `where --dir`, active virtual root summaries, active virtual filtered
   summaries, folded active-prefix rollup publish/readiness/cleanup, basedirs
   group usage, user usage, group subdirs, user subdirs, history, basedirs auth
   allow/deny checks, import cleanup, and active snapshot cleanup.
3. Given a query is slower than baseline, when correctness and absolute gates
   pass, then the report status is not failed solely for that delta, but the
   operation row records `slower` and includes a non-empty cause note.
4. Given missing baseline evidence and no documented infeasible reason, when
   final validation runs, then it fails.
5. Given final reports for each query operation, when validation runs, then
   both `cold_uncached` and `warmed` variants are present, their latencies,
   read metrics, result counts, and result digests are separate, and validation
   fails if warmed/cache-assisted results are mixed into cold aggregates.
6. Given any trie import or cleanup metric is at least 2.0x the baseline, when
   validation runs, then it fails unless the report flags
   `extreme_import_regression` with metric name, baseline value, trie value,
   ratio, and a non-empty cause note.
7. Given final storage reports, when hot snapshot table bytes are compared,
   then each hot table has compressed and uncompressed byte deltas classified
   as `improved`, `regressed`, or `neutral`, and every `regressed` byte metric
   has a non-empty cause note. Hot tables include `wrstat_dirs`,
   `wrstat_files`, `wrstat_dir_facts`, `wrstat_child_facts`, trie filter
   tables, and trie-normalized basedirs tables.
8. Given basedirs/quota reader reports containing active trie rows and
   historical or external fallback rows, when validation runs, then operation
   rows are separated by `path_kind_case` values `active_trie`, `fallback`,
   and `mixed` as applicable, with separate p50/p95/p99 latency, read metrics,
   result counts, digests, and delta status. Validation fails if mixed results
   are reported only as one aggregate.

### D2: Absolute Cold UX Gates

As a maintainer, I want cold latency gates for the trie design, so that the new
schema is usable without warmed process caches.

Cold means no warmed cross-request process cache and no browser cache. A
request may use per-request path, `dir_id`, and parent-path caches. Provider or
active-snapshot caches are allowed only when keyed by snapshot or active set and
fully invalidated on refresh.

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given representative subset reports, when exact directory resolution, exact
   file stat, permission path, and direct-child list p95 are checked, then each
   `cold_uncached` p95 is less than 100 ms.
2. Given representative subset reports, when bounded recursive subtree,
   filtered summary, glob, Disktree, and `where` p95 are checked, then each
   `cold_uncached` p95 is less than 500 ms.
3. Given a large-result query exceeds 500 ms, when server query time is less
   than 500 ms and serialization or result volume dominates, then the report
   records server time, serialization time, rows returned, bytes returned, and
   current-branch delta separately.
4. Given fixed final datasets, when validation runs, then reports include the
   schema3 mixed Lustre/NFS subset, `/nfs/t283_imaging/`, the known high-fanout
   parent, a 100-small-NFS active virtual namespace simulation, and the largest
   practical local production-like dataset, each with reproducible manifest
   detail.
5. Given a cold gate would pass only by using `warmed` rows, when validation
   runs, then it fails and reports the missing or failing `cold_uncached`
   operation.

## Implementation Order

1. Baseline and fixtures. Capture current-branch import/query/basedirs reports
   with cleanup timing and separate cold/warmed rows, and add missing
   `internal/chperf` operation names and digest fields. This must happen
   before schema edits.
2. Schema and trie builder (`A1`, `A2`, `A4`, `B1`). Add trie DDL, resolver
   helpers, deterministic external-sort builder tests, manifest/readiness
   tests, and clean schema tests.
3. Import rewrite (`A3`). Convert direct file, DGUTA, filter, child packet,
   readiness, cleanup, folded active-prefix rollups, basedirs, and spool paths
   to write numeric IDs. This phase is sequential after the builder; subareas
   can then proceed in parallel.
4. Query rewrite (`B2`, `B3`, `B4`, `C1`). Convert file APIs, tree APIs,
   glob/count, permissions, and path reconstruction to numeric plans. Keep
   string outputs unchanged.
5. Active virtual, server endpoints, and basedirs readers (`C2`, `C3`, `C4`).
   Convert active overlay, Disktree/where surfaces, and basedirs
   reader/store/query paths to numeric IDs and documented fallback semantics.
6. Final evidence (`D1`, `D2`). Run correctness, GoConvey tests, perf
   harnesses, storage comparison, cleanup validation, and final gate report
   generation.

## Appendix: Key Decisions

- No compatibility migration: this branch is unreleased.
- `wrstat_dirs` is the only normal full directory path catalog for active
  scanned snapshots.
- Preorder IDs make recursive subtree queries integer range scans.
- Exact lookup uses a hash plus full-path verification; collision tests are
  mandatory.
- Active-prefix rollups are folded into numeric active virtual
  summary/filter tables, not kept as string-keyed serving tables.
- Store basenames where needed for sorting/display; do not store repeated full
  paths in hot rows.
- Query latency and storage reduction are the design goal. Import may cost more
  if measured and reported.
- Correctness and absolute cold UX gates determine pass/fail. Storage reduction
  and baseline deltas are required evidence, not standalone pass/fail gates.
- Tests must use GoConvey per `go-conventions`; every acceptance test here must
  have a concrete GoConvey test. Implement with `go-implementor`; review with
  `go-reviewer`.
