# Schema3 ClickHouse Cold Tree Performance Specification

## Overview

Make first-use ClickHouse tree navigation and `where` fast without browser
cache state, process-local warming, or a second click. Schema3 keeps schema2
objects, then adds bounded serving contracts and full-filter rows so cold
routes read the needed parent/subtree data once.

The design is ClickHouse-native:

- exact current summaries from `wrstat_dir_facts` or full-filter dir rows;
- parent-packet child serving from `wrstat_parent_facts` and
  `wrstat_child_filter_all`;
- full-filter dir/subtree serving from `wrstat_dir_filter_all`;
- small active-set virtual overlay for `/`, `/lustre/`, `/nfs/`,
  intermediate virtual parents, and mount-root boxes;
- immutable sidecar fallback only if this design misses cold gates.

No schema2 object is removed in the first schema3 step.
`wrstat_dir_facts`, `wrstat_parent_facts`, and `wrstat_children` remain
canonical. `wrstat_dir_filter_ageall` remains the preferred AgeAll path only
while it is exact and strictly faster than exact `wrstat_dir_filter_all`.
Schema3 does not enable proactive broad warming. Required cache behaviour is
only whole-packet caching caused by real parent-packet reads.

## Architecture

**Packages and files**

- `clickhouse/schema/*.sql`: add schema3 tables.
- `clickhouse/schema.go`: bootstrap schema3 DDL at schema version `1`.
- `clickhouse/dguta_writer.go`: direct writer order, readiness, publish,
  retry cleanup, old snapshot cleanup.
- `clickhouse/import_block_writer.go`: bounded insert writers.
- `clickhouse/parent_facts.go`: broad/default parent packet readers.
- `clickhouse/child_filter_all.go`: full-filter parent packet writers/readers.
- `clickhouse/dir_filter_all.go`: full-filter dir/subtree writers/readers.
- `clickhouse/database.go`: route `DirInfo`, `DirInfos`, `Children`,
  `DirsHaveChildren`, `Where`, and `Info`.
- `clickhouse/database_cache.go`: parent-packet cache keys and counters.
- `clickhouse/active_virtual_overlay.go`: active virtual summary/filter rows.
- `clickhouse/active_mounts.go`: deterministic active-set id input.
- `clickhouse/virtual_children.go`: active virtual child rows and cleanup.
- `clickhouse/summarise_spool_loader.go`: load schema3 spool tables.
- `internal/chspool/spool.go`: schema3 spool row types, order, manifest.
- `internal/chperf/*`, `cmd/clickhouse_perf.go`: correctness and perf gates.
- `internal/boltperf/*`, `cmd/bolt_perf.go`: Bolt/sidecar comparison.
- `server/tree.go`, `server/where.go`, `cmd/where.go`: REST/CLI probes.

**Public API compatibility**

No caller-visible API changes are required. Preserve these signatures:

```go
func NewClient(cfg Config) (*Client, error)
func NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error)
func OpenProvider(cfg Config) (provider.Provider, error)
func NewInspector(cfg Config) (*Inspector, error)

type Database interface {
    DirInfo(dir string, filter *Filter) (*DirSummary, error)
    Children(dir string) ([]string, error)
    Info() (*Info, error)
    Close() error
}

type DirInfoBatcher interface {
    DirInfos(dirs []string, filter *Filter) (map[string]*DirSummary, error)
}

type DirHasChildrenBatcher interface {
    DirsHaveChildren(dirs []string, filter *Filter) (map[string]bool, error)
}

type Whereer interface {
    Where(
        dir string,
        filter *Filter,
        recurseCount func(string) int,
    ) (DCSs, error)
}

type DGUTAWriter interface {
    Add(dguta RecordDGUTA) error
    SetBatchSize(batchSize int)
    SetMountPath(mountPath string)
    SetUpdatedAt(updatedAt time.Time)
    Close() error
}
```

**Data model**

Add `wrstat_child_filter_all`. One row is an exact
`(age, gid, uid, ft)` summary for one child directory under one parent.
`filter_child_count` is the number of direct child directories of `dir` that
have at least one row for the same tuple.

```sql
CREATE TABLE IF NOT EXISTS wrstat_child_filter_all (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
  age UInt8,
  gid UInt32,
  uid UInt32,
  ft UInt16,
  dir String CODEC(LZ4),
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  filter_child_count UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  has_filter_children UInt8,
  has_children UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir)
SETTINGS index_granularity = 8192;
```

Add `wrstat_dir_filter_all`. One row is an exact filtered subtree summary for
one directory. It serves exact filtered `DirInfo`, `DirInfos`, and
`where --dir` subtree scans.

```sql
CREATE TABLE IF NOT EXISTS wrstat_dir_filter_all (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  age UInt8,
  gid UInt32,
  uid UInt32,
  ft UInt16,
  dir String CODEC(LZ4),
  parent_dir String CODEC(LZ4),
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  filter_child_count UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  has_filter_children UInt8,
  has_children UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, age, gid, uid, ft, dir)
SETTINGS index_granularity = 8192;
```

Add snapshot readiness after all schema3 snapshot rows are inserted and
validated.

```sql
CREATE TABLE IF NOT EXISTS wrstat_schema3_snapshot_sets (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  schema3_version UInt32,
  dir_facts_rows UInt64 CODEC(Delta, ZSTD(3)),
  parent_facts_rows UInt64 CODEC(Delta, ZSTD(3)),
  children_rows UInt64 CODEC(Delta, ZSTD(3)),
  child_filter_all_rows UInt64 CODEC(Delta, ZSTD(3)),
  dir_filter_all_rows UInt64 CODEC(Delta, ZSTD(3)),
  manifest_sha256 String CODEC(ZSTD(3)),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, schema3_version);
```

Add small active virtual overlay tables. They contain only virtual dirs and
mount-root boxes, never ordinary non-root directory facts.

```sql
CREATE TABLE IF NOT EXISTS wrstat_active_virtual_summaries (
  active_set_id String CODEC(ZSTD(3)),
  dir String CODEC(LZ4),
  mount_path LowCardinality(String) CODEC(LZ4),
  is_mount_root_box UInt8,
  updated_at DateTime CODEC(Delta, ZSTD(3)),
  all_count UInt64 CODEC(Delta, LZ4),
  all_size UInt64 CODEC(Delta, LZ4),
  all_atime_min Int64 CODEC(Delta, LZ4),
  all_mtime_max Int64 CODEC(Delta, LZ4),
  all_atime_buckets Array(UInt64) CODEC(LZ4),
  all_mtime_buckets Array(UInt64) CODEC(LZ4),
  all_uids Array(UInt32) CODEC(LZ4),
  all_gids Array(UInt32) CODEC(LZ4),
  all_ft UInt16,
  file_count UInt64 CODEC(Delta, LZ4),
  file_size UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, dir);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_filter_all (
  active_set_id String CODEC(ZSTD(3)),
  dir String CODEC(LZ4),
  age UInt8,
  gid UInt32,
  uid UInt32,
  ft UInt16,
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  filter_child_count UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, dir, age, gid, uid, ft);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_children (
  active_set_id String CODEC(ZSTD(3)),
  parent_dir String CODEC(LZ4),
  child_dir String CODEC(LZ4),
  mount_path LowCardinality(String) CODEC(LZ4),
  is_mount_root_box UInt8,
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, parent_dir, child_dir);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_sets (
  active_set_id String CODEC(ZSTD(3)),
  schema3_version UInt32 CODEC(Delta, ZSTD(3)),
  mounts_sha256 String CODEC(ZSTD(3)),
  active_mount_count UInt64 CODEC(Delta, ZSTD(3)),
  summary_rows UInt64 CODEC(Delta, ZSTD(3)),
  filter_rows UInt64 CODEC(Delta, ZSTD(3)),
  child_rows UInt64 CODEC(Delta, ZSTD(3)),
  manifest_sha256 String CODEC(ZSTD(3)),
  ready UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY active_set_id;
```

`wrstat_schema3_snapshot_sets` row presence is snapshot readiness.
`wrstat_active_virtual_sets.ready = 1` row presence is active-set overlay
readiness. Readers may use schema3 rows only when all active snapshots and the
computed active-set id are ready.
`wrstat_active_virtual_children` is the schema3 replacement active child table.
`wrstat_virtual_children` remains compatibility/diagnostic only and is not a
required schema3 serving or spool target.

Full-filter rows encode exact tuples only. Multi-select filters use `IN` or
bitmask predicates and aggregate rows. Mandatory ages are all values in
`db.DirGUTAges`: AgeAll, all access-time buckets, and all mtime buckets.
Broad/default routes and scalar file-only routes remain canonical on
`wrstat_dir_facts` and `wrstat_parent_facts` unless a measured replacement is
exact and equal-or-better on E2 gates. Full-filter rows are mandatory for
arbitrary filter combinations, owner/user/type filters, and age-specific
routes.

Parent packet cache key:

```go
type parentPacketCacheKey struct {
    MountPath    string
    SnapshotID   string
    ParentDir    string
    FilterMode   parentPacketFilterMode
    Age          db.DirGUTAge
    FilterKey    treeFilterCacheKey
    ActiveSetID  string
    SchemaVer    uint32
    QueryVer     uint32
}
```

`AgeAll` packet entries must not answer age-specific requests. Entries for
different active-set ids, query versions, filters, and snapshot ids are
distinct.

Canonical expected digests:

- `where` result digest: clone `db.DCSs`, sort by `Dir` then `Age`, sort each
  summary's `UIDs` and `GIDs`, encode non-nil summaries as compact JSON array
  of `{dir,count,size,uids,gids,ft,age}`, and return
  `sha256:<lower-hex>`.
- Tree child digest: apply the same encoding to `DirInfo.Children` sorted by
  `Dir` then `Age`. `child_count` is `len(DirInfo.Children)`.
- Fixture manifests must store expected digests for each fixture query. Values
  are generated from fixture input via the canonical summariser before schema3
  candidate code runs. Literal placeholder SHA-256 values are forbidden.
  Tests fail when an expected digest is missing or recomputation from fixture
  input differs from the manifest value.

Read-volume ceiling formula:

- For each allowed ClickHouse read `i`, define `E_i` expected rows. For a
  parent packet, `E_i` is the exact packet row count. For an exact current
  read, `E_i = 1`. For readiness reads, `E_i` is the exact readiness row
  count required by the active mount/snapshot set.
- `G_i` is the table `index_granularity`.
- `B_i = sum(data_compressed_bytes) / sum(rows)` from active `system.parts`
  for the table. Missing or zero part rows fail the gate.
- `M_i = max(1, ceil(E_i / G_i) + 2)` read granules.
- `rows_ceiling = sum(M_i * G_i)`.
- `marks_ceiling = sum(M_i)`.
- `bytes_ceiling = ceil(sum(M_i * G_i * B_i) * 1.25)`.

Parent-packet `DirInfos` and `DirsHaveChildren` gates allow exactly one
parent-packet table read for the target parent, zero per-child ClickHouse
queries, zero subtree scans, zero facts-vector reads, and readiness reads only.
Their rows, marks, and bytes must be no greater than the formula above.

## Section A: Parent Packet Serving

### A1: Exact current summaries stay canonical

As a UI reader, I want exact `DirInfo` current summaries to come from the
canonical exact-dir table, so that child packet rows cannot corrupt the
clicked directory total.

For broad/default filters, exact current summary uses `wrstat_dir_facts`.
For full filters, exact current summary uses `wrstat_dir_filter_all` when the
schema3 snapshot is ready. Do not use a `wrstat_parent_facts` row as the
primary exact summary for the requested directory.

**Package:** `clickhouse/`, `cmd/`, `server/`
**File:** `clickhouse/database.go`, `cmd/where.go`, `server/where.go`
**Test file:** `clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`,
`server/server_test.go`

**Acceptance tests:**

1. Given `/m/a/` has `wrstat_dir_facts.all_count = 3` and its
   `wrstat_parent_facts` duplicate has `all_count = 999`, when
   `DirInfo("/m/a/", &db.Filter{Age: db.DGUTAgeAll})` runs, then it returns
   `Count = 3`, `Size` from `wrstat_dir_facts`, one exact-dir read, and zero
   parent-packet reads for the current summary.
2. Given ready `wrstat_dir_filter_all` rows for
   `(age=1,gid=7,uid=11,ft=32,dir="/m/a/")` with `count=5`, when
   `DirInfo("/m/a/", filter)` uses that exact filter, then it returns
   `Count = 5`, `GIDs = []uint32{7}`, `UIDs = []uint32{11}`,
   `FT = db.DGUTAFileTypeBam`, and no `arrayJoin(gids, uids, fts, ages, ...)`
   facts-vector query is executed.

### A2: Parent packets are coherent request units

As a Disktree user, I want one parent packet read to serve all visible child
summaries, so that high-fanout folders do not reread the same sibling range.

When a parent packet is read, all child summaries, broad `child_count`, and
filtered `filter_child_count` values from that packet are returned or cached
together. Later `DirInfo(child)` in the same provider cache scope must reuse
the packet result.

**Package:** `clickhouse/`
**File:** `clickhouse/parent_facts.go`
**Test file:** `clickhouse/database_dirinfo_test.go`

**Acceptance tests:**

1. Given a mount with high parent
   `/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/` and
   11,205 direct child dirs, when `Tree.DirInfo(parent, AgeAll)` runs from a
   cold provider, then it returns 11,205 child summaries and performs exactly
   one `wrstat_parent_facts` range read for that parent.
2. Given the result from test 1 is in the provider cache, when
   `DirInfo(firstChild, AgeAll)` runs, then it performs zero ClickHouse reads
   and returns the cached summary and child count for `firstChild`.
3. Given `DirInfos(all11205ChildPaths, AgeAll)` runs from a cold provider,
   then it performs exactly one distinct parent-packet read, zero
   `wrstat_dir_facts` vector reads, and returns 11,205 keyed summaries.

### A3: Child presence uses packet child counts

As a tree renderer, I want `DirsHaveChildren` to use maintained packet child
counts, so that focused broad and filtered checks stay bounded.

For broad/default filters, use `has_children` from `wrstat_parent_facts`.
For full filters, use `has_filter_children` from
`wrstat_child_filter_all`. Do not query each requested child's own child
range unless its parent packet is unavailable and the fallback is recorded.

**Package:** `clickhouse/`, `cmd/`, `server/`
**File:** `clickhouse/database.go`, `cmd/where.go`, `server/where.go`
**Test file:** `clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`,
`server/server_test.go`

**Acceptance tests:**

1. Given the 11,205 child dirs from A2 and alternating broad `has_children`
   values, when `DirsHaveChildren(childPaths, AgeAll)` runs cold, then it
   returns the exact alternating map and performs one parent-packet read, zero
   `wrstat_children` reads for child dirs, and zero `DirInfo(child)` calls.
2. Given ready `wrstat_child_filter_all` rows where only child indexes
   `0, 10, 20` have `has_filter_children = 1` for
   `age=1,gid=7,uid=11,ft=32`, when `DirsHaveChildren(childPaths, filter)`
   runs cold, then only those three child paths are true and the query reads
   one filtered parent packet.
3. Given the filtered packet table is missing, when the same request runs,
   then it records the named fallback route, preserves current results, and
   the perf gate marks the fallback as a schema3 failure.

### A4: `Tree.Where` traverses packet frontiers once

As a CLI user, I want first `where` traversal to reuse packet summaries and
children, so that the first run is as bounded as retries.

For each traversal level, group frontier dirs by mount and parent packet. Load
each distinct packet once, store current and child summaries together, and
avoid recursive `DirInfo(child)` fanout.

**Package:** `clickhouse/`
**File:** `clickhouse/database.go`
**Test file:** `clickhouse/database_dirinfo_test.go`

**Acceptance tests:**

1. Given a three-level synthetic tree with two frontier dirs sharing the same
   parent, when `Tree.Where(root, AgeAll, split.SplitsToSplitFn(2))` runs,
   then the number of parent-packet reads equals the number of distinct
   traversed parent dirs and the sorted result digest equals the generic
   `db.Tree` fallback digest.
2. Given the high-fanout parent and a type filter `other`, when
   `Tree.Where(parent, filter, split.SplitsToSplitFn(2))` runs cold, then it
   reads each distinct filtered parent packet at most once and does not issue
   one query per child.

### A5: REST tree endpoint reuses one packet

As a Disktree user, I want one endpoint request to share `DirInfo` and
`DirsHaveChildren` packet data, so that the first click is bounded.

The endpoint `GET /rest/v1/auth/tree?path=<dir>` must not depend on the
response cache. With response caching disabled or cold, endpoint assembly must
use the same provider packet for the clicked directory's child summaries and
child `HasChildren` values.

**Package:** `server/`
**File:** `server/tree.go`
**Test file:** `server/server_test.go`

**Acceptance tests:**

1. Given the high-fanout parent from A2, a fresh provider, and disabled
   response cache, when `GET /rest/v1/auth/tree` runs with `path` set to the
   high-fanout parent and broad/default filter, then the JSON has 11,205
   children, each child `HasChildren` equals the packet `has_children` value,
   and ClickHouse performs exactly one `wrstat_parent_facts` range read for
   that parent.
2. Given the same request with a full filter, when the endpoint runs from a
   fresh provider and cold response cache, then the JSON children and
   `HasChildren` flags come from one `wrstat_child_filter_all` parent packet,
   with zero rereads of the high-fanout sibling range.
3. Given ordinary fixture dir `/m/project/` has tree response expectations:

   ```text
   manifest_key                 query                   child_count
   project_tree_unused_1y       path=/m/project/&age=4  4
   project_tree_unchanged_1y    path=/m/project/&age=12 2
   ```

   When `GET /rest/v1/auth/tree` runs with each query from a fresh provider
   and cold or disabled response cache, then HTTP status is `200`, the JSON
   child count equals the table value, the canonical child digest equals the
   manifest digest for `manifest_key`, `age=4` is the `--unused 1Y`
   equivalent, `age=12` is the `--unchanged 1Y` equivalent, facts-vector
   reads are `0`, and fallback count is `0`.

## Section B: Comprehensive All-Filter Rows

### B1: Full-filter row construction is exact

As an importer, I want schema3 full-filter rows for every existing GUTA tuple,
so that arbitrary filters avoid vector scans.

Write one row per exact `(age, gid, uid, ft, dir)` tuple into both
`wrstat_dir_filter_all` and `wrstat_child_filter_all`. Include AgeAll and all
age-specific rows. Multi-select filters are answered by aggregating exact
rows, not by precomputing every multi-select combination.

**Package:** `clickhouse/`
**File:** `clickhouse/dir_filter_all.go`
**Test file:** `clickhouse/dguta_writer_test.go`

```go
type filterAllRow struct {
    MountPath          string
    SnapshotID         string
    ParentDir          string
    Dir                string
    Age                db.DirGUTAge
    GID                uint32
    UID                uint32
    FT                 db.DirGUTAFileType
    Count              uint64
    Size               uint64
    AtimeMin           int64
    MtimeMax           int64
    AtimeBuckets       []uint64
    MtimeBuckets       []uint64
    FilterChildCount   uint64
    ChildCount         uint64
    HasFilterChildren  uint8
    HasChildren        uint8
    RefreshedAt        time.Time
}
```

**Acceptance tests:**

1. Given a dir with GUTAs
   `(AgeAll, gid=7, uid=11, ft=bam, count=3, size=30)` and
   `(AgeA1M, gid=7, uid=11, ft=bam, count=2, size=20)`, when imported, then
   `wrstat_dir_filter_all` has exactly those two rows for the dir and
   `wrstat_child_filter_all` has exactly those two rows under its parent.
2. Given a parent with two direct children where only child A has a matching
   `(age=1,gid=7,uid=11,ft=32)` row, when child rows are written, then the
   parent packet row for child A has `has_filter_children = 0` and the row for
   the parent directory in its own parent packet has
   `filter_child_count = 1`.
3. Given all `db.DirGUTAges`, when import writes schema3 rows, then each
   age present in input GUTAs appears with the same numeric age in both
   schema3 tables.

### B2: All-filter readers replace vector scans

As an API reader, I want all current filter families to use full-filter rows,
so that cold filtered navigation stays bounded.

Supported filters: broad, file-only/default, UID, GID, file-type bitmask,
owner+user+type, AgeAll, age-specific, `--unused`, and `--unchanged`.
Broad/default and scalar file-only requests stay on `wrstat_dir_facts` and
`wrstat_parent_facts` unless an exact replacement is measured equal-or-better
on E2. Full-filter rows are mandatory for arbitrary filter combinations,
owner/user/type, and age-specific routes. Empty UID/GID lists return no
summaries without scanning full-filter tables.

**Package:** `clickhouse/`
**File:** `clickhouse/dir_filter_all.go`
**Test file:** `clickhouse/database_dirinfo_test.go`

**Acceptance tests:**

1. Given facts-vector summaries and ready full-filter rows for the same mount,
   when `DirInfos` runs with each filter family, then every returned
   `DirSummary` has identical `Count`, `Size`, `Atime`, `Mtime`,
   `CommonATime`, `CommonMTime`, sorted `UIDs`, sorted `GIDs`, `FT`, and
   `Age` compared with the facts-vector path.
2. Given `GIDs = []uint32{7, 9}`, `UIDs = nil`,
   `FT = bam|cram`, and `Age = db.DGUTAgeA1M`, when `DirInfo` runs, then it
   sums only rows with age `1`, gid `7` or `9`, and `ft & filter.FT != 0`,
   and returns the exact aggregated UID/GID/type sets.
3. Given `GIDs = []uint32{}` or `UIDs = []uint32{}`, when `DirInfos` or
   `Where` runs, then the result is empty and ClickHouse read rows are `0`
   for `wrstat_child_filter_all` and `wrstat_dir_filter_all`.
4. Given an age-specific filter, when `DirInfos` runs, then no
   `wrstat_dir_filter_ageall` query and no facts-vector `arrayJoin` query is
   executed.
5. Given ready full-filter rows and no measured replacement record, when
   broad/default or scalar file-only `DirInfos` runs, then it reads
   `wrstat_dir_facts` or `wrstat_parent_facts` and performs zero
   `wrstat_dir_filter_all` and `wrstat_child_filter_all` reads.
6. Given ready full-filter rows, when owner+user+type or age-specific
   `DirInfos` runs, then it reads only `wrstat_dir_filter_all` or
   `wrstat_child_filter_all` for summaries and performs zero facts-vector
   `arrayJoin` reads.

### B3: `where --dir` uses subtree-serving rows

As a CLI user, I want `where --dir <subdir>` to scan filtered subtree rows
directly, so that high-fanout and NFS-heavy subtrees do not unpack vectors.

`Tree.Where` may prefer `wrstat_dir_filter_ageall` for AgeAll owner/type only
while it is exact and its p95 is strictly lower than
`wrstat_dir_filter_all`. When `dir_filter_all.p95` is less than or equal to
`dir_filter_ageall.p95`, route to `wrstat_dir_filter_all`. All other
supported filters use `wrstat_dir_filter_all` when schema3 readiness exists.

**Package:** `clickhouse/`, `cmd/`, `server/`
**File:** `clickhouse/database.go`, `cmd/where.go`, `server/where.go`
**Test file:** `clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`,
`server/server_test.go`

**Acceptance tests:**

1. Given a subtree with five matching filtered dirs and three non-matching
   dirs, when `Tree.Where(subdir, age+gid+uid+ft, splits=2)` runs, then the
   result contains only the five matching dirs, sorted by size, and the digest
   matches the facts-vector implementation.
2. Given ready `wrstat_dir_filter_ageall` and ready
   `wrstat_dir_filter_all`, when AgeAll+gid is measured, then the route keeps
   `wrstat_dir_filter_ageall` only if both sources are exact and
   `dir_filter_ageall.p95 < dir_filter_all.p95`; when
   `dir_filter_all.p95 <= dir_filter_ageall.p95`, including equal p95, it
   routes to `wrstat_dir_filter_all`.
3. Given the high-fanout parent, when `where --dir` runs with
   age+gid+uid+ft, then ClickHouse read rows, bytes, and marks are bounded by
   the read-volume formula for one `wrstat_dir_filter_all` subtree read over
   matching rows plus any distinct frontier packet reads and readiness reads;
   facts-vector rows read is `0`.
4. Given ordinary fixture dir `/m/project/` has manifest digest keys:

   ```text
   project_where_unused_1y
   project_where_unchanged_1y
   ```

   When `Tree.Where("/m/project/", &db.Filter{Age: db.DGUTAgeA1Y}, splits=2)`
   and `Tree.Where("/m/project/", &db.Filter{Age: db.DGUTAgeM1Y}, splits=2)`
   run cold, then canonical result digests match the manifest values for
   those keys, `wrstat_dir_filter_all` is the subtree source, zero
   facts-vector `arrayJoin` queries run, and fallback count is `0`.
5. Given the same fixture, when
   `wrstat-ui where --dir /m/project/ --unused 1Y --json` and
   `wrstat-ui where --dir /m/project/ --unchanged 1Y --json` run against a
   fresh server provider, then JSON result digests are exactly the two
   manifest values from test 4, response cache hits are `0`, facts-vector
   reads are `0`, and fallback count is `0`.
6. Given the same fixture, when REST calls
   `/rest/v1/where?dir=/m/project/&age=A1Y&splits=2` and
   `/rest/v1/where?dir=/m/project/&age=M1Y&splits=2` run with a cold or
   disabled response cache, then HTTP status is `200`, result digests are
   exactly the two manifest values from test 4, facts-vector reads are `0`,
   and fallback count is `0`.

## Section C: Small Active-Set Virtual Overlay

### C1: Active-set ids are deterministic

As a provider, I want active-set ids to change only when the active mount set
changes, so that virtual rows and packet caches invalidate correctly.

The id is the SHA-256 hex digest of sorted active rows. Each row input is
`mount_path|snapshot_id|updated_at.UTC().Format(time.RFC3339Nano)`.

**Package:** `clickhouse/`
**File:** `clickhouse/provider.go`
**Test file:** `clickhouse/provider_test.go`

**Acceptance tests:**

1. Given the same active rows in reverse order, when
   `fingerprintForMountsActive` runs, then both calls return the same
   non-empty id.
2. Given one row has a different `snapshot_id` or `updated_at`, when the id is
   recomputed, then the new id differs from the previous id.
3. Given a parent packet cache entry for active set A, when active set B is
   published, then reads under B miss A's packet entry and do not reuse stale
   virtual summaries.

### C2: Virtual rows cover roots and mount boxes

As a Disktree user, I want virtual roots to use the same serving model as
ordinary directories, so that `/`, `/lustre/`, and `/nfs/` are cold-fast.

The overlay must include:

- `/`;
- `/lustre/`;
- `/nfs/`;
- intermediate virtual parents above configured mount roots;
- mount-root boxes under those parents;
- full-filter virtual rows for every filter family in B2.

Do not duplicate ordinary non-root directory facts into active-set tables.
Existing `wrstat_active_prefix_*` and `wrstat_virtual_children` may remain
compatibility/diagnostic objects.

**Package:** `clickhouse/`
**File:** `clickhouse/active_virtual_overlay.go`
**Test file:** `clickhouse/database_dirinfo_test.go`

**Acceptance tests:**

1. Given the mixed8 active set, when querying virtual summaries, then:

   | Path | Count | Size | Child count |
   |---|---:|---:|---:|
   | `/` | 1750001 | 61484536134482 | 8 |
   | `/lustre/` | 1500001 | 61176182464512 | 7 |
   | `/nfs/` | 250000 | 308353669970 | 1 |

2. Given the same active set and a full filter, when `DirInfo("/")`,
   `DirInfo("/lustre/")`, and `DirInfo("/nfs/")` run cold, then they read
   `wrstat_active_virtual_filter_all`, return facts-equivalent summaries, and
   read zero ordinary mount-root facts for virtual totals.
3. Given a synthetic 100-small-NFS active set with configured mounts
   `/nfs/project000/` through `/nfs/project099/` and `/nfs/` not configured,
   when the overlay is built, then `/nfs/` is virtual, `Children("/nfs/")`
   returns exactly 100 project boxes, summary rows equal 102,
   `wrstat_active_virtual_children` rows equal 101, and no ordinary non-root
   directory is copied into the overlay.
4. Given a deterministic active-set fixture with expected summaries for `/`,
   `/lustre/`, `/nfs/`, and every mount-root box, when `DirInfo` runs cold for
   each path with broad/default, file-only, UID, GID, type bitmask,
   owner+user+type, AgeAll, age-specific, `--unused`, and `--unchanged`
   filters, then each result matches the fixture's exact count, size,
   atime/mtime extrema, bucket arrays, UID/GID/type sets, child count, and
   digest.
5. Given test 4 runs, then all virtual-path filter queries read only
   `wrstat_active_virtual_summaries`,
   `wrstat_active_virtual_filter_all`, and
   `wrstat_active_virtual_children`; they perform zero ordinary mount-root
   fact reads for `/`, `/lustre/`, `/nfs/`, and mount-root box totals.
6. Given the same fixture has virtual-root where digest keys:

   ```text
   root_where_unused_1y
   nfs_where_unchanged_1y
   ```

   When `Tree.Where("/", &db.Filter{Age: db.DGUTAgeA1Y}, splits=2)` and
   `Tree.Where("/nfs/", &db.Filter{Age: db.DGUTAgeM1Y}, splits=2)` run cold,
   then canonical result digests equal the manifest values for those keys,
   only active virtual and schema3 packet tables are read, zero facts-vector
   `arrayJoin` queries run, and fallback count is `0`.
7. Given the same fixture, when
   `wrstat-ui where --dir / --unused 1Y --json`,
   `wrstat-ui where --dir /nfs/ --unchanged 1Y --json`,
   `/rest/v1/where?dir=/&age=A1Y&splits=2`, and
   `/rest/v1/where?dir=/nfs/&age=M1Y&splits=2` run from fresh providers,
   then each result digest matches test 6, response cache hits are `0`, no
   ordinary mount-root fact or facts-vector reads occur, and fallback count is
   `0`.
8. Given the same fixture has virtual tree response expectations:

   ```text
   manifest_key            query              child_count
   root_tree_unused_1y     path=/&age=4       8
   nfs_tree_unchanged_1y   path=/nfs/&age=12  1
   ```

   When `GET /rest/v1/auth/tree` runs with each query from a fresh provider
   and cold or disabled response cache, then HTTP status is `200`, child count
   equals the table value, canonical child digest equals the manifest value
   for `manifest_key`, `age=4` is the `--unused 1Y` equivalent, `age=12` is
   the `--unchanged 1Y` equivalent, only active virtual and schema3 packet
   tables are read, facts-vector reads are `0`, and fallback count is `0`.

## Section D: Writer, Spool, Readiness, Cleanup

### D1: Direct import stages every schema3 object before publish

As an operator, I want readers to see schema3 rows only after all derived data
is complete, so that partial serving layers are impossible.

The active mount event is the only visible switch. Compute the next active set
from current active rows plus the candidate snapshot, but do not write
`wrstat_mount_events` until that snapshot and its active virtual overlay are
ready. Readers opened before the switch keep the previous active set; readers
opened after the switch must find the new active-set readiness row.

Direct import order:

1. Drop inactive stale partitions for `(mount_path, snapshot_id)`.
2. Stream `wrstat_files`.
3. Stream `wrstat_dir_facts`, `wrstat_children`, and `wrstat_parent_facts`.
4. Stream `wrstat_dir_filter_ageall`.
5. Stream `wrstat_child_filter_all` and `wrstat_dir_filter_all`.
6. Validate row counts and required checksums.
7. Write `wrstat_dir_projection_sets`.
8. Write `wrstat_schema3_snapshot_sets`.
9. Compute staged next active-set id without publishing `wrstat_mount_events`.
10. Stream active virtual summaries, filter rows, and
   `wrstat_active_virtual_children` rows for that next active set.
11. Validate active virtual summary, filter, and child row counts and
   checksums.
12. Write `wrstat_active_virtual_sets` with `ready = 1`.
13. Publish `wrstat_mount_events` active row.
14. Drop replaced inactive snapshot and old active-set partitions after the
   existing reader grace period.

Direct active virtual writer row structs are unexported, one per active
virtual table, and have the same field names and types as D2
`ActiveVirtualSummaryRow`, `ActiveVirtualFilterAllRow`,
`ActiveVirtualChildRow`, and `ActiveVirtualSetRow`. Add bounded direct writer
methods:

```go
func (w *activeVirtualOverlayWriter) appendSummary(
    ctx context.Context,
    row activeVirtualSummaryRow,
) error
func (w *activeVirtualOverlayWriter) appendFilterAll(
    ctx context.Context,
    row activeVirtualFilterAllRow,
) error
func (w *activeVirtualOverlayWriter) appendChild(
    ctx context.Context,
    row activeVirtualChildRow,
) error
func (w *activeVirtualOverlayWriter) appendSet(
    ctx context.Context,
    row activeVirtualSetRow,
) error
```

**Package:** `clickhouse/`
**File:** `clickhouse/dguta_writer.go`
**Test file:** `clickhouse/dguta_writer_test.go`

**Acceptance tests:**

1. Given an import succeeds, when the active mount row is visible, then
   `wrstat_schema3_snapshot_sets` already has one row with exact row counts
   for facts, children, parent facts, child-filter rows, and dir-filter rows,
   and `wrstat_active_virtual_sets` already has `ready = 1`, exact summary,
   filter, and child row counts for the active set produced by that row.
2. Given the all-filter writer returns an error after facts were inserted,
   when import exits, then no active mount row is published, no schema3
   readiness row is written, partial schema3 partitions are dropped on retry,
   and readers keep serving the previous active snapshot or not found.
3. Given active-set virtual refresh fails before mount publish, when import
   exits, then no active mount row is published, no active-set readiness row is
   written, staged active virtual partitions are dropped on retry, and old
   readers keep serving the previous active set.
4. Given reader A opens on active set A and import stages active set B, when B
   has partial virtual rows but no active mount event, then reader A and any
   new provider still compute active set A. After B's active event is visible,
   new providers compute B and require `wrstat_active_virtual_sets.ready = 1`.

### D2: Summarise spool carries schema3 tables

As a production summarise user, I want spool output to contain every schema3
table with verified counts and checksums, so that retry/resume is deterministic.

Spool files are `<table>.gob.gz` and manifest keys are table names.
`CreateSet` must create all schema3 files in `TableOrder`. Extend
`internal/chspool` with:

```go
const TableChildFilterAll = "wrstat_child_filter_all"
const TableDirFilterAll = "wrstat_dir_filter_all"
const TableSchema3SnapshotSets = "wrstat_schema3_snapshot_sets"
const TableActiveVirtualSummaries = "wrstat_active_virtual_summaries"
const TableActiveVirtualFilterAll = "wrstat_active_virtual_filter_all"
const TableActiveVirtualChildren = "wrstat_active_virtual_children"
const TableActiveVirtualSets = "wrstat_active_virtual_sets"

func (s *Set) WriteChildFilterAll(row ChildFilterAllRow) error
func (s *Set) WriteDirFilterAll(row DirFilterAllRow) error
func (s *Set) WriteSchema3SnapshotSet(row Schema3SnapshotSetRow) error
func (s *Set) WriteActiveVirtualSummary(row ActiveVirtualSummaryRow) error
func (s *Set) WriteActiveVirtualFilterAll(
    row ActiveVirtualFilterAllRow,
) error
func (s *Set) WriteActiveVirtualChild(row ActiveVirtualChildRow) error
func (s *Set) WriteActiveVirtualSet(row ActiveVirtualSetRow) error

type ActiveVirtualSummaryRow struct {
    ActiveSetID      string
    Dir              string
    MountPath        string
    IsMountRootBox   uint8
    UpdatedAt        time.Time
    AllCount         uint64
    AllSize          uint64
    AllAtimeMin      int64
    AllMtimeMax      int64
    AllAtimeBuckets  []uint64
    AllMtimeBuckets  []uint64
    AllUIDs          []uint32
    AllGIDs          []uint32
    AllFT            uint16
    FileCount        uint64
    FileSize         uint64
    ChildCount       uint64
    RefreshedAt      time.Time
}

type ActiveVirtualFilterAllRow struct {
    ActiveSetID       string
    Dir               string
    Age               uint8
    GID               uint32
    UID               uint32
    FT                uint16
    Count             uint64
    Size              uint64
    AtimeMin          int64
    MtimeMax          int64
    AtimeBuckets      []uint64
    MtimeBuckets      []uint64
    FilterChildCount  uint64
    ChildCount        uint64
    RefreshedAt       time.Time
}

type ActiveVirtualChildRow struct {
    ActiveSetID     string
    ParentDir       string
    ChildDir        string
    MountPath       string
    IsMountRootBox  uint8
    ChildCount      uint64
    RefreshedAt     time.Time
}

type ActiveVirtualSetRow struct {
    ActiveSetID       string
    Schema3Version    uint32
    MountsSHA256      string
    ActiveMountCount  uint64
    SummaryRows       uint64
    FilterRows        uint64
    ChildRows         uint64
    ManifestSHA256    string
    Ready             uint8
    RefreshedAt       time.Time
}
```

**Package:** `cmd/`, `internal/chspool/`
**File:** `cmd/summarise_spool.go`, `internal/chspool/spool.go`
**Test file:** `cmd/summarise_spool_test.go`,
`internal/chspool/spool_test.go`

**Acceptance tests:**

1. Given a spool set is closed, when the manifest is read, then all schema3
   tables appear in table order with files named `<table>.gob.gz`, exact
   decoded row counts, byte counts, and SHA-256 hashes.
2. Given a schema3 spool file is modified after manifest write, when
   `VerifyManifest` runs, then it returns `ErrManifestMismatch` and names the
   changed table.
3. Given a spool manifest is missing `wrstat_child_filter_all`, when
   `LoadSummariseSpool` runs, then it returns a manifest mismatch and writes
   no active mount event.
4. Given the manifest is missing `wrstat_active_virtual_summaries`,
   `wrstat_active_virtual_filter_all`, `wrstat_active_virtual_children`, or
   `wrstat_active_virtual_sets`, when `LoadSummariseSpool` runs, then it
   returns `ErrManifestMismatch`, writes no active-set readiness row, and
   writes no active mount event.
5. Given `wrstat_active_virtual_sets.gob.gz` decodes a row count different
   from the manifest count, when `VerifyManifest` runs, then it returns
   `ErrManifestMismatch` and names `wrstat_active_virtual_sets`.
6. Given `wrstat_active_virtual_children.gob.gz` decodes a row count different
   from the manifest count, when `VerifyManifest` runs, then it returns
   `ErrManifestMismatch` and names `wrstat_active_virtual_children`.
7. Given a fixture stats file, mountpoints file, quota/config inputs, and
   ClickHouse spool target, when the actual summarise command path runs through
   `cmd/summarise_spool.go`, then the produced spool manifest contains every
   schema3 table in `TableOrder`, each schema3 file decodes successfully, each
   decoded row count equals the manifest count, and row counts match the
   canonical fixture summariser output. The test must not call schema3
   `Set.Write*` methods directly except through the summarise command path.

### D3: Spool loader preserves atomic publish

As an operator, I want loaded spools to publish exactly like direct imports,
so that direct and production paths are equivalent.

`LoadSummariseSpool` loads schema3 tables before readiness. It must verify
decoded rows equal inserted rows for each table, including all active virtual
tables. It may not use post-import `INSERT ... SELECT` rebuilds unless the
rebuild is bounded, deadline-limited, and hidden until readiness. Missing or
mismatched active virtual spool data, including
`wrstat_active_virtual_children`, blocks `wrstat_active_virtual_sets` and
`wrstat_mount_events`.

**Package:** `clickhouse/`, `cmd/`
**File:** `clickhouse/summarise_spool_loader.go`, `cmd/summarise_spool.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`,
`cmd/summarise_spool_test.go`

**Acceptance tests:**

1. Given a valid schema3 spool, when loaded, then table insert order matches
   D1, loaded row counts equal manifest counts, and the active mount row is
   written only after `wrstat_schema3_snapshot_sets` and
   `wrstat_active_virtual_sets.ready = 1`.
2. Given `wrstat_dir_filter_all` load inserts one fewer row than the manifest,
   when verification runs, then it returns `errSpoolLoadedRowsMismatch`,
   writes no readiness row, and writes no active mount event.
3. Given a retry after failed load, when the same spool is loaded again, then
   stale inactive partitions are dropped first and the final active snapshot id
   is deterministic from mount path and `updated_at`.
4. Given `wrstat_active_virtual_filter_all` load inserts one fewer row than
   the manifest, when verification runs, then it returns
   `errSpoolLoadedRowsMismatch`, writes no active-set readiness row, writes no
   active mount event, and preserves the previous active set for readers.
5. Given `wrstat_active_virtual_children` load inserts one fewer row than the
   manifest, when verification runs, then it returns
   `errSpoolLoadedRowsMismatch`, writes no active-set readiness row, writes no
   active mount event, and deletes the partial child partition before retry.
6. Given the same command fixture as D2 test 7 and an empty ClickHouse test
   database, when the actual summarise command path builds and publishes the
   spool, then every schema3 table has the manifest row count in ClickHouse,
   `wrstat_schema3_snapshot_sets` contains the exact manifest counts and
   checksum, `wrstat_active_virtual_sets.ready = 1`, the active mount event is
   visible only after both readiness rows, and cold `DirInfo`,
   `DirsHaveChildren`, and `Where` probes read schema3 rows with zero fallback,
   zero facts-vector reads, and fixture-manifest digests equal to the canonical
   summariser output.

### D4: Cleanup removes old partitions and active sets

As an operator, I want old schema3 data removed after safe publish, so that
storage does not grow per retry or active-set change.

Cleanup covers all snapshot-scoped schema3 tables and active-set overlay
tables. It must never remove the current active snapshot or current active
set. Cleanup failures are reported as import warnings/errors according to the
existing cleanup policy; they must not make partial new data visible.

**Package:** `clickhouse/`
**File:** `clickhouse/active_snapshot_cleanup.go`
**Test file:** `clickhouse/active_snapshot_cleanup_test.go`

**Acceptance tests:**

1. Given active snapshot B replaces snapshot A for the same mount, when
   cleanup completes, then all schema3 partitions for A are gone and all
   schema3 partitions for B remain.
2. Given active set B replaces active set A, when virtual cleanup completes,
   then `wrstat_active_virtual_summaries`,
   `wrstat_active_virtual_filter_all`, `wrstat_active_virtual_children`, and
   `wrstat_active_virtual_sets` partitions for A are gone and B remains.
3. Given cleanup races with a newer active publish, when the guard detects the
   newer active row, then it aborts deletion of the newer active snapshot and
   returns the existing active-cleanup error.

## Section E: Perf Gates and Fallback

### E1: Perf harness records correctness and cold metrics

As a reviewer, I want final reports to prove correctness before speed, so that
fast wrong answers fail.

Datasets:

- mixed8 subset from `.docs/schema3/investigate.md`;
- larger NFS-heavy subset that can expose the reported 33s first `where`;
- 100-small-NFS simulation with `/nfs/` kept virtual;
- high-fanout parent
  `/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/`;
- same-subset Bolt or sidecar comparison as a required final-gate input.

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given any perf report, when a speed gate is evaluated, then the report also
   contains result count, result digest, and correctness-equivalence status;
   missing correctness fields fail the gate.
2. Given REST tree and REST where probes, when measured, then reports include
   query count, cache hits/misses, JSON bytes, gzip bytes, p50, p95, and p99.
3. Given ClickHouse query probes, when measured, then reports include read
   rows, read bytes, read marks, result rows, and result bytes when available.
4. Given direct import probes, when measured, then reports include table rows,
   active parts, compressed/uncompressed bytes, wall time, user CPU time,
   system CPU time, total CPU time, peak RSS, spool bytes, part counts, retry
   cleanup result, and publish latency.
5. Given summarise spool-load probes, when measured, then reports include
   loaded table rows, active parts, compressed/uncompressed bytes, wall time,
   user CPU time, system CPU time, total CPU time, peak RSS, spool bytes, part
   counts, retry cleanup result, and publish latency.
6. Given the final gate evaluates a report without same-subset Bolt or
   sidecar comparison evidence, then the gate fails with
   `missing comparison evidence`.
7. Given a successful same-subset comparison, when the report is written, then
   `comparison.status = "success"` and it contains exact dataset/subset
   manifest path and SHA-256, command argv, source revision, tool version,
   output artifact path, log path, storage bytes, p50, p95, p99, result
   digest, and fallback count.
8. Given comparison reproduction is infeasible, when the report is written,
   then `comparison.status = "infeasible"` and it contains the exact attempted
   pre-ClickHouse checkout path or prototype path, command argv, dataset
   manifest path and SHA-256, source revision, tool version, log path, and
   either the failing error output or a specific evidence-backed reason the
   route cannot run.
9. Given `comparison.status = "infeasible"` has all required fields, when the
   final gate evaluates it, then the gate is blocked, not passed, and the
   report names the comparison reason and log path.
10. Given a fixture manifest omits an expected digest key used by a schema3
   acceptance test, or its stored digest differs from recomputation from the
   fixture input, when fixture validation runs, then the gate fails with
   `missing expected digest` or `stale expected digest` before timing is
   evaluated.

### E2: Cold performance gates pass without warming

As a product owner, I want schema3 to pass cold gates from a reset provider,
so that first user interactions are consistently fast.

All gates require cold process query caches and no warmed REST response cache.
No proactive broad warming may run in schema3 gates. The only allowed cache
entries are whole parent packets read by the measured request. Correctness
equivalence is mandatory before timing wins are accepted.

| Scenario | Gate |
|---|---:|
| REST tree `/`, `/lustre/`, `/nfs/` first request | p95 < 500 ms |
| First click into high-fanout parent, broad and filtered | p95 < 500 ms |
| Focused high-fanout `DirInfos`, broad and filtered | p95 < 1 s |
| Focused high-fanout `DirsHaveChildren`, broad and filtered | p95 < 1 s |
| First root `where`, splits 2 | p95 < 1 s on bounded subset |
| Real first `where --dir`, root and high-fanout | p95 < 1 s |
| Real first `where --dir`, NFS-heavy dirs | p95 < 2 s |
| First filter switch | p95 < 1 s |
| Import/summarise | within measured wall/CPU/RSS budget from E2.12 |

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given mixed8, when REST tree first requests run for `/`, `/lustre/`, and
   `/nfs/` with fresh provider and cold or disabled response cache, then each
   path has p95 under 500 ms, correct result digest, and no proactive warming.
2. Given mixed8, when first click into the high-fanout parent runs with
   broad/default filter, then p95 is under 500 ms, it performs one exact
   current read and exactly one `wrstat_parent_facts` parent-packet read, zero
   per-child queries, zero subtree scans, and rows/bytes/marks are no greater
   than the read-volume ceiling for those reads plus readiness reads.
3. Given mixed8, when first click into the high-fanout parent runs with a full
   filter, then p95 is under 500 ms, it performs one exact current read and
   exactly one `wrstat_child_filter_all` parent-packet read, zero per-child
   queries, zero subtree scans, and rows/bytes/marks are no greater than the
   read-volume ceiling for those reads plus readiness reads.
4. Given mixed8, when the high-fanout focused `DirInfos` broad gate runs, then
   p95 is under 1s, it performs exactly one `wrstat_parent_facts`
   parent-packet read, zero per-child queries, zero subtree scans, and
   rows/bytes/marks are no greater than the read-volume ceiling for that
   packet plus required readiness reads.
5. Given mixed8, when the high-fanout focused `DirInfos` filtered gate runs,
   then p95 is under 1s, it performs exactly one
   `wrstat_child_filter_all` parent-packet read for the high-fanout parent,
   per-child ClickHouse query count is `0`, `wrstat_dir_filter_all` subtree
   scan count is `0`, facts-vector rows read is `0`, and rows/bytes/marks are
   no greater than the read-volume ceiling for that packet plus required
   readiness reads.
6. Given mixed8, when the high-fanout focused `DirsHaveChildren` broad gate
   runs, then p95 is under 1s, it performs exactly one
   `wrstat_parent_facts` parent-packet read, zero per-child queries, zero
   subtree scans, and rows/bytes/marks are no greater than the read-volume
   ceiling for that packet plus required readiness reads.
7. Given mixed8, when the high-fanout focused `DirsHaveChildren` filtered gate
   runs, then p95 is under 1s, it performs exactly one
   `wrstat_child_filter_all` parent-packet read, zero per-child queries, zero
   subtree scans, only the expected child paths are true, facts-vector rows
   read is `0`, and rows/bytes/marks are no greater than the read-volume
   ceiling for that packet plus required readiness reads.
8. Given mixed8, when first root `where` with splits 2 runs from a fresh
   provider, then p95 is under 1s and the digest matches current facts.
9. Given mixed8, when the first filter switch runs after an unfiltered tree
   request, then p95 is under 1s, the filtered digest is correct, and existing
   broad packet cache entries are not used for filtered results.
10. Given mixed8, when real first `where --dir /` and first
   `where --dir` for the high-fanout parent run from fresh providers, then
   both p95 values are under 1s and both digests match current facts.
11. Given the NFS-heavy subset, when first `where --dir` runs from a fresh
   provider, then p95 is under 2s and the digest matches current facts.
12. Given direct import and spool-load reports before and after schema3, when
   the regression budget is computed, then accepted wall-time, CPU-time, RSS,
   spool-byte, and part-count budgets are recorded from measurements rather
   than hardcoded before measurement.

### E3: Sidecar fallback is explicit and immutable

As a maintainer, I want a clear fallback if ClickHouse-native schema3 misses
the gates, so that we stop adding process-local caches to mask cold cost.

Trigger fallback if A-D are correct but any E2 cold gate misses after bounded
query tuning. Broad warming is deferred to a later measured feature and is not
enabled in schema3.

Preferred fallback: immutable mmap/Roaring active-set navigation sidecar with
exact directory packets, parent-child adjacency, filter postings plus summary
payloads, virtual rows, manifest checksums, atomic publish, reader grace
period, and cleanup of old versions. ClickHouse remains source of truth for
files, history, basedirs, and audit. SQLite is acceptable for prototype/audit.
Bolt is acceptable for reuse only with active-set aggregate redesign.

**Package:** `clickhouse/`
**File:** `clickhouse/navigation_sidecar.go`
**Test file:** `clickhouse/navigation_sidecar_test.go`

**Acceptance tests:**

1. Given a sidecar manifest with checksum mismatch, when a provider opens,
   then the sidecar is rejected and ClickHouse fallback is used with a recorded
   sidecar error.
2. Given sidecar version B is published while readers hold version A, when the
   grace period elapses, then A is closed and cleaned only after no reader can
   use it.
3. Given mixed8, when a sidecar candidate is measured, then storage size,
   p50/p95/p99, correctness digest, and ClickHouse fallback count are recorded.
   The target size is 100-300 MB; Bolt-like 0.4-0.5 GB is acceptable only as
   measured fallback evidence.

## Implementation Order

1. Parent packet contract: A1-A5. Add packet cache/counters and tests proving
   high-fanout `DirInfos`, `DirsHaveChildren`, Disktree, and `Where` read each
   parent packet once per request/provider cache scope.
2. Full-filter schema and direct writer: B1 plus D1 readiness for direct
   import. Keep existing schema2 tables and routes working.
3. Full-filter readers: B2-B3. Route arbitrary and age-specific filters to
   schema3 rows. Keep `wrstat_dir_filter_ageall` only while it is exact and
   strictly faster on measured AgeAll p95.
4. Spool path and cleanup: D2-D4. Production summarise and direct import must
   have identical publish/readiness semantics before perf acceptance.
5. Active virtual overlay: C1-C2. Build small active-set rows and prove mixed8
   plus 100-small-NFS semantics before replacing active-prefix routes.
6. Perf gates: E1-E2. Run mixed8, NFS-heavy, high-fanout, virtual simulation,
   and same-subset Bolt/sidecar comparisons. Correctness failures block speed
   acceptance.
7. Sidecar fallback: E3 only if ClickHouse-native A-D are correct and any cold
   E2 gate still misses.

Phases 1 and 2 can proceed in parallel after DDL names are fixed. Reader route
changes in phase 3 must wait for phase 2 readiness tests. Perf gates must run
after phases 1-5.

## Appendix: Key Decisions

**Measured evidence**

Mixed8 on 2026-06-11 imported 1,750,001 rows in 35.93s, max RSS
831,356 KB. Active compressed sizes were:

- `wrstat_files`: 35.68 MiB;
- `wrstat_dir_facts`: 30.03 MiB;
- `wrstat_parent_facts`: 28.50 MiB;
- `wrstat_dir_filter_ageall`: 11.35 MiB;
- `wrstat_children`: 3.99 MiB.

High-fanout evidence:

- parent path:
  `/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/`;
- 11,205 direct children and 92,529 vector entries;
- one parent scalar packet: 8-12 ms;
- one parent vector packet: 18-28 ms;
- focused `DirInfos` broad: p50 63,482 ms, p95 116,770 ms;
- focused `DirsHaveChildren` broad: p50 63,273 ms;
- focused filtered sample: 458,951 ms, 2.90B rows, 898.6 GB.

All-filter prototype evidence:

- full expansion: 3,488,307 rows vs 493,588 AgeAll rows;
- child prototype: 30.11 MiB compressed, 1.49 GiB uncompressed;
- dir prototype: 75.26 MiB compressed, 1.12 GiB uncompressed;
- combined prototype: 105.37 MiB compressed;
- parent AgeAll+gid p50/p95: 44/77 ms;
- parent age+gid p50/p95: 37/50 ms;
- parent age+gid+uid+ft p50/p95: 27/38 ms;
- subtree age+gid+uid+ft p50/p95: 47/59 ms;
- subtree AgeAll+gid p50/p95: 77/93 ms, slower than AgeAll table.

**Rejected alternatives**

- Directory packet table as primary storage. Prototype had 50,430 packets for
  197,179 child rows and 3,488,307 vector entries, 33.15 MiB compressed,
  max packet 11,205 children, p50/p95 21/42 ms fetch and 22/43 ms
  `arrayJoin`. It did not beat `wrstat_parent_facts` and still needed
  full-filter/subtree rows.
- Standalone `where_frontiers`. They solve only `where`, not measured 63s
  `DirInfos`/`DirsHaveChildren`, and add tuple/multi-select/split/query
  version complexity.
- ClickHouse bitmap/posting tables as primary answer. They identify candidate
  dirs but not UI summary payloads. Mixed8 modeled raw postings were 10.4 MB
  dimension lists, 2.0 MB exact AgeAll tuple lists, and 14.0 MB
  age-specific tuple lists before payloads.
- Cache warming only. A cold filtered focused case hit 459s and 898.6 GB
  read. Proactive broad warming is not enabled in schema3; only whole-packet
  caching from real reads is required.
- Physical active-set duplication. Mixed8 modeled duplication was about
  29.13 MiB compressed per active set; the virtual full-filter overlay was
  modeled at 6,134 rows. Use the small overlay first.

**Testing strategy**

Use GoConvey. Every acceptance test maps to a real test. Use
`t.TempDir()` for files and deterministic fake ClickHouse connections for
query-count tests. Perf commands must be bounded with `timeout`.

Recommended targeted commands:

```bash
timeout 600s env CGO_ENABLED=1 go test -tags netgo --count 1 \
  ./clickhouse ./internal/chspool ./internal/chperf ./cmd ./server -v

OPS=tree_disktree_endpoint_cold_provider,tree_where_cold_provider
OPS=${OPS},dirinfos_broad,dirshavechildren_broad

timeout 20m .tmp/agent/schema3/wrstat-ui clickhouse-perf \
  -C "clickhouse://localhost:9000?database=wrstat_schema3_<name>&compress=lz4" \
  -D wrstat_schema3_<name> \
  --mounts .tmp/agent/schema3/mounts.txt \
  --query-timeout 120s \
  --json .tmp/agent/schema3/perf/<name>-query-highfanout.json \
  query \
  --dir "/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/" \
  --repeat 3 --warmup 0 \
  --ops "$OPS"
```

**Error policy**

Unknown schema3 tables may fall back only for compatibility and must record a
named fallback route. Fallback routes fail final schema3 perf gates. Readers
must never use schema3 rows unless corresponding snapshot or active-set
readiness exists. Partial table answers are correctness failures even when
fast.

Implement with the `go-implementor` skill and review with `go-reviewer`.
