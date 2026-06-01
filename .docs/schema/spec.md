# Clean ClickHouse Schema V1 Specification

## Overview

Define the unreleased ClickHouse schema as clean schema version 1. The schema
centers tree reads on `wrstat_dir_facts`, one row per mount directory, with
scalar hot summaries, aligned filter vectors, `child_count`, direct child
edges, and row-exists readiness. It removes branch-local compatibility shapes,
migrations, compatibility aliases, backfills, and raw-DGUTA read fallbacks.

Snapshot import stays deterministic and atomic per mount: write all snapshot
partitions, write the readiness marker last, then publish an active mount
event. Failed deterministic snapshots remain retryable and cleanable without
dropping a previous active snapshot.

Tree, Disktree, `Where`, `Info`, permission, file, basedirs, subdirectory, and
history APIs keep current caller-visible behavior. Bolt is not a production
dependency for ClickHouse schema v1.

## Architecture

**Packages and files**

- `clickhouse/schema/*.sql`: final schema-v1 `CREATE TABLE` and `CREATE VIEW`
  statements only.
- `clickhouse/schema.go`: bootstrap schema version 1 and reject incompatible
  databases.
- `clickhouse/dguta_writer.go`: deterministic snapshot import, facts writer,
  child writer, readiness, active publication, retry cleanup.
- `clickhouse/import_block_writer.go`: internal bounded block writer shared by
  directory facts, child edges, and selected derived writers.
- `clickhouse/database.go`, `clickhouse/dir_facts.go`: tree readers over
  `wrstat_dir_facts`, child edges, and virtual ancestors.
- `clickhouse/active_mounts.go`, `clickhouse/active_snapshot_cleanup.go`:
  append-only active events, rollback/tombstone cleanup.
- `clickhouse/virtual_children.go`: active virtual hierarchy above mount roots.
- `clickhouse/file_api.go`: file APIs over `wrstat_files`, including exact-safe
  extension predicates.
- `clickhouse/basedirs_store.go`, `clickhouse/basedirs_reader.go`: existing
  basedirs usage, subdirectory, and history semantics.
- `internal/chperf/*`, `cmd/clickhouse_perf.go`: perf gates and JSON reports.

**Public APIs**

These signatures remain caller-compatible:

```go
func NewClient(cfg Config) (*Client, error)
func NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error)
func NewFileIngestOperation(
    cfg Config,
    mountPath string,
    updatedAt time.Time,
) (summary.OperationGenerator, io.Closer, error)
func NewBaseDirsStore(cfg Config) (basedirs.Store, error)
func OpenProvider(cfg Config) (provider.Provider, error)
func ActiveSnapshotMatches(
    cfg Config,
    mountPath string,
    updatedAt time.Time,
) (bool, error)
func CleanActiveSnapshotAttempt(
    cfg Config,
    mountPath string,
    updatedAt time.Time,
) error
func (c *Client) ListDir(
    ctx context.Context,
    dir string,
    opts ListOptions,
) ([]FileRow, error)
func (c *Client) StatPath(
    ctx context.Context,
    path string,
    opts StatOptions,
) (*FileRow, error)
func (c *Client) IsDir(ctx context.Context, path string) (bool, error)
func (c *Client) FindByGlob(
    ctx context.Context,
    baseDirs []string,
    patterns []string,
    opts FindOptions,
) ([]FileRow, error)
func (c *Client) PermissionAnyInDir(
    ctx context.Context,
    dir string,
    uid uint32,
    gids []uint32,
) (bool, error)
```

**Schema DDL**

All SQL files contain only final v1 objects. Snapshot tables partition by
`(mount_path, snapshot_id)` and cleanup drops those partitions with the cleanup
timeout. `ORDER BY` is the ClickHouse primary key unless an explicit primary
key is added.

```sql
CREATE TABLE IF NOT EXISTS wrstat_schema_version (
  singleton UInt8 DEFAULT 1,
  version UInt32,
  inserted_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY singleton;
```

Bootstrap inserts version `1` when no version exists. Validation reads
`FINAL`, requires exactly singleton `1`, and requires min/max version `1`.

```sql
CREATE TABLE IF NOT EXISTS wrstat_mount_events (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  event_at DateTime64(3) CODEC(Delta, ZSTD(3)),
  event_type UInt8,
  snapshot_id UUID,
  updated_at DateTime CODEC(Delta, ZSTD(3)),
  reason LowCardinality(String) CODEC(ZSTD(3))
) ENGINE = MergeTree
ORDER BY (mount_path, event_at, event_type, updated_at, snapshot_id);

CREATE VIEW IF NOT EXISTS wrstat_mounts_active AS
SELECT
  mount_path,
  tupleElement(latest, 1) AS snapshot_id,
  tupleElement(latest, 2) AS updated_at
FROM (
  SELECT
    mount_path,
    argMax(
      tuple(snapshot_id, updated_at, event_type),
      tuple(
        event_at,
        if(event_type = 0, 1, 0),
        updated_at,
        toString(snapshot_id)
      )
    ) AS latest
  FROM wrstat_mount_events
  GROUP BY mount_path
)
WHERE tupleElement(latest, 3) = 1;
```

`event_type=1` means publish or rollback to active. `event_type=0` means
inactive tombstone. Inactive wins same-millisecond ties. Active reads never use
`FINAL`.

```sql
CREATE TABLE IF NOT EXISTS wrstat_dir_facts (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir String CODEC(LZ4),
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
  file_atime_min Int64 CODEC(Delta, LZ4),
  file_mtime_max Int64 CODEC(Delta, LZ4),
  file_atime_buckets Array(UInt64) CODEC(LZ4),
  file_mtime_buckets Array(UInt64) CODEC(LZ4),
  file_uids Array(UInt32) CODEC(LZ4),
  file_gids Array(UInt32) CODEC(LZ4),
  file_ft UInt16,
  gids Array(UInt32) CODEC(LZ4),
  uids Array(UInt32) CODEC(LZ4),
  fts Array(UInt16) CODEC(LZ4),
  ages Array(UInt8) CODEC(LZ4),
  counts Array(UInt64) CODEC(Delta, LZ4),
  sizes Array(UInt64) CODEC(Delta, LZ4),
  atime_mins Array(Int64) CODEC(Delta, LZ4),
  mtime_maxs Array(Int64) CODEC(Delta, LZ4),
  atime_buckets Array(Array(UInt64)) CODEC(LZ4),
  mtime_buckets Array(Array(UInt64)) CODEC(LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_children (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
  child String CODEC(LZ4)
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, parent_dir, child)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_dir_projection_sets (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  updated_at DateTime CODEC(Delta, ZSTD(3)),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id)
SETTINGS index_granularity = 8192;
```

`wrstat_dir_facts` is canonical for tree facts. `wrstat_children` is canonical
for direct mount children. `wrstat_dir_projection_sets` is derived readiness,
written only after facts, children, and any selected derived filter index are
complete.

```sql
CREATE TABLE IF NOT EXISTS wrstat_dir_filter_ageall (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
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
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, gid, uid, ft, dir)
SETTINGS index_granularity = 8192;
```

`wrstat_dir_filter_ageall` is optional. Add it only if final larger-prefix
gates prove facts-only routing misses filtered whole-mount `Where` or
high-fanout filtered `DirsHaveChildren` targets. It serves only
`Age == db.DGUTAgeAll` owner/type filters and falls back to facts when absent,
not ready, or semantically inapplicable.

```sql
CREATE TABLE IF NOT EXISTS wrstat_virtual_children (
  active_set_id String CODEC(ZSTD(3)),
  parent_dir String CODEC(LZ4),
  child String CODEC(LZ4),
  child_is_mount_root UInt8,
  mount_path LowCardinality(String) CODEC(LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, parent_dir, child)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_virtual_children_sets (
  active_set_id String CODEC(ZSTD(3)),
  active_mount_count UInt64 CODEC(Delta, ZSTD(3)),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY active_set_id;
```

`active_set_id` is the deterministic hash of sorted
`wrstat_mounts_active` rows. Virtual children contain only paths above mount
roots. Readers may rebuild missing sets from active mounts, then query the
ready set without `FINAL`.

```sql
CREATE TABLE IF NOT EXISTS wrstat_virtual_summary_cache (
  active_set_id String CODEC(ZSTD(3)),
  dir String CODEC(LZ4),
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
  gids Array(UInt32) CODEC(LZ4),
  uids Array(UInt32) CODEC(LZ4),
  fts Array(UInt16) CODEC(LZ4),
  ages Array(UInt8) CODEC(LZ4),
  counts Array(UInt64) CODEC(Delta, LZ4),
  sizes Array(UInt64) CODEC(Delta, LZ4),
  atime_mins Array(Int64) CODEC(Delta, LZ4),
  mtime_maxs Array(Int64) CODEC(Delta, LZ4),
  atime_buckets Array(Array(UInt64)) CODEC(LZ4),
  mtime_buckets Array(Array(UInt64)) CODEC(LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, dir)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_virtual_summary_sets (
  active_set_id String CODEC(ZSTD(3)),
  active_mount_count UInt64 CODEC(Delta, ZSTD(3)),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY active_set_id;
```

`wrstat_virtual_summary_cache` is optional. Add it only if many-active-mount
gates prove live composition from mount-root facts is too slow. It contains
only ancestor directories above active mount roots.

```sql
CREATE TABLE IF NOT EXISTS wrstat_files (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
  name String CODEC(LZ4),
  path String ALIAS concat(parent_dir, name),
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
ORDER BY (mount_path, snapshot_id, parent_dir, name)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
```

`wrstat_files` is canonical only for file-level APIs.

```sql
CREATE TABLE IF NOT EXISTS wrstat_basedirs_group_usage (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  snapshot_id UUID,
  gid UInt32,
  basedir String CODEC(ZSTD(3)),
  age UInt8,
  uids Array(UInt32),
  usage_size UInt64 CODEC(Delta, ZSTD(3)),
  quota_size UInt64 CODEC(Delta, ZSTD(3)),
  usage_inodes UInt64 CODEC(Delta, ZSTD(3)),
  quota_inodes UInt64 CODEC(Delta, ZSTD(3)),
  mtime DateTime CODEC(Delta, ZSTD(3)),
  date_no_space DateTime CODEC(Delta, ZSTD(3)),
  date_no_files DateTime CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, gid, age, basedir);

CREATE TABLE IF NOT EXISTS wrstat_basedirs_user_usage (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  snapshot_id UUID,
  uid UInt32,
  basedir String CODEC(ZSTD(3)),
  age UInt8,
  gids Array(UInt32),
  usage_size UInt64 CODEC(Delta, ZSTD(3)),
  quota_size UInt64 CODEC(Delta, ZSTD(3)),
  usage_inodes UInt64 CODEC(Delta, ZSTD(3)),
  quota_inodes UInt64 CODEC(Delta, ZSTD(3)),
  mtime DateTime CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, uid, age, basedir);

CREATE TABLE IF NOT EXISTS wrstat_basedirs_group_subdirs (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  snapshot_id UUID,
  gid UInt32,
  basedir String CODEC(ZSTD(3)),
  age UInt8,
  pos UInt32,
  subdir String CODEC(ZSTD(3)),
  num_files UInt64 CODEC(Delta, ZSTD(3)),
  size_files UInt64 CODEC(Delta, ZSTD(3)),
  last_modified DateTime CODEC(Delta, ZSTD(3)),
  file_usage Map(UInt16, UInt64)
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, gid, age, basedir, pos);

CREATE TABLE IF NOT EXISTS wrstat_basedirs_user_subdirs (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  snapshot_id UUID,
  uid UInt32,
  basedir String CODEC(ZSTD(3)),
  age UInt8,
  pos UInt32,
  subdir String CODEC(ZSTD(3)),
  num_files UInt64 CODEC(Delta, ZSTD(3)),
  size_files UInt64 CODEC(Delta, ZSTD(3)),
  last_modified DateTime CODEC(Delta, ZSTD(3)),
  file_usage Map(UInt16, UInt64)
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, uid, age, basedir, pos);

CREATE TABLE IF NOT EXISTS wrstat_basedirs_history (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  gid UInt32,
  date DateTime CODEC(Delta, ZSTD(3)),
  usage_size UInt64 CODEC(Delta, ZSTD(3)),
  quota_size UInt64 CODEC(Delta, ZSTD(3)),
  usage_inodes UInt64 CODEC(Delta, ZSTD(3)),
  quota_inodes UInt64 CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY mount_path
ORDER BY (mount_path, gid, date);
```

Basedirs tables remain canonical for basedirs APIs. Snapshot-scoped basedirs
tables use snapshot partition cleanup; history is append-only by mount.

**Object contracts**

- `wrstat_schema_version`: canonical bootstrap metadata. Writer:
  `ensureSchema`. Readers: constructors. Cleanup: none.
- `wrstat_mount_events`: canonical active history. Writers: snapshot publish,
  rollback, inactive cleanup. Readers: active view, cleanup audit. Cleanup:
  retain append-only history.
- `wrstat_mounts_active`: derived active view. Readers: provider snapshots,
  mount resolution, file APIs, tree APIs. Readiness: latest event ordering.
- `wrstat_dir_facts`: canonical tree facts. Writer: DGUTA import close path.
  Readers: tree summaries, filters, `Info`, permissions. Cleanup: snapshot
  partition drops. Readiness: `wrstat_dir_projection_sets`.
- `wrstat_children`: canonical direct child edges. Writer: DGUTA import.
  Readers: `Children`, Disktree, `Where`, child checks. Cleanup: snapshot
  partition drops. Readiness: active publish after projection-set marker.
- `wrstat_dir_projection_sets`: derived readiness. Writer: last import step.
  Readers: readiness caches and fact routes. Cleanup: snapshot partition drops.
- `wrstat_dir_filter_ageall`: optional derived AgeAll owner/type index.
  Writer: facts import only when selected. Readers: whole-mount filtered
  `Where` and high-fanout filtered child checks. Cleanup: snapshot partition
  drops. Readiness: same projection-set marker.
- `wrstat_virtual_children`: derived active hierarchy. Writer: active-set
  refresh after publish/tombstone. Readers: virtual ancestor `Children` and
  Disktree. Cleanup: exact old `active_set_id` partition drops.
- `wrstat_virtual_children_sets`: derived active-hierarchy readiness. Writer:
  active-set refresh after all `wrstat_virtual_children` rows are written.
  Readers: virtual children routes and provider readiness cache. Cleanup:
  exact old `active_set_id` partition drops with virtual children. Readiness:
  row exists for `active_set_id`; absent rows trigger deterministic refresh.
- `wrstat_virtual_summary_cache`: optional derived ancestor cache. Writer:
  active-set refresh only when selected. Readers: virtual ancestor `DirInfo`.
  Cleanup: exact old `active_set_id` partition drops. Readiness: summary-set
  row.
- `wrstat_virtual_summary_sets`: optional derived ancestor-cache readiness.
  Writer: summary-cache refresh after all cache rows are written. Readers:
  virtual ancestor `DirInfo` cache route. Cleanup: exact old `active_set_id`
  partition drops with summary cache. Readiness: row exists for
  `active_set_id`; absent rows force live composition.
- `wrstat_files`: canonical file facts. Writer: `NewFileIngestOperation`.
  Readers: `ListDir`, `StatPath`, `IsDir`, `FindByGlob`, file-level
  permissions. Cleanup: snapshot partition drops.
- Basedirs tables: canonical basedirs data. Writer: `NewBaseDirsStore`.
  Readers: basedirs usage, subdirectory, and history APIs. Cleanup: snapshot
  partitions for usage/subdirs; history retained by mount.

## Section A: Clean Schema And Active Mounts

### A1: Bootstrap Clean Schema V1

As an operator, I want a fresh ClickHouse database to bootstrap directly into
the final schema, so that new installs do not carry unreleased branch history.

`ensureSchema()` applies only final `CREATE TABLE` and `CREATE VIEW`
statements. Schema version remains `1`. No `ALTER` files, migrations,
compatibility views, test-only backfills, or comments about superseded branch
layouts are part of production DDL, production Go, or embedded resources.
Temporary raw DGUTA accumulation is allowed only as unexported import-local
state; readers never call it or expose it.

**Package:** `clickhouse/`
**File:** `clickhouse/schema.go`, `clickhouse/schema/*.sql`,
production Go under `clickhouse/`, `cmd/`, and `internal/`
**Test file:** `clickhouse/bootstrap_test.go`,
`clickhouse/clean_schema_test.go`

**Acceptance tests:**

1. Given an empty database and 8 concurrent `NewClient(cfg)` calls, when all
   constructors return, then every constructor succeeds and
   `wrstat_schema_version FINAL` contains one row with `version = 1`.
2. Given embedded schema SQL, when tests scan `clickhouse/schema/*.sql`, then
   no file contains `ALTER TABLE`, `wrstat_mounts_active_v2`,
   `wrstat_dguta`, `wrstat_dir_summary`,
   `wrstat_dir_dguta_vector`, `wrstat_dir_filter_index`,
   `wrstat_dirs`, `wrstat_tree_dguta`, `wrstat_tree_children`,
   `AggregatingMergeTree`, `summary_version`, `backfill`, or
   `compatibility`.
3. Given production non-test Go and embedded resources under `clickhouse/`,
   `cmd/`, and `internal/`, when the clean-schema denylist scan runs, then no
   file contains `wrstat_mounts_active_v2`, `wrstat_dir_summary`,
   `wrstat_dir_dguta_vector`, `wrstat_dir_filter_index`,
   `wrstat_tree_dguta`, `summary_version`, `backfill`, `compatibility`,
   `old layout`, or `projection helper`.
4. Given production tree-reader SQL strings and `_test.go` files outside the
   clean-schema denylist tests, when they are scanned, then no reader SQL reads
   `wrstat_dguta` and no test name or string literal contains both
   `wrstat_dguta` and `fallback`.
5. Given any raw DGUTA import accumulator remains, when exported identifiers
   are inspected, then it has no exported type, function, method, interface,
   or field; only `NewDGUTAWriter` construction reaches it, and no reader file
   imports or calls it.
6. Given production ClickHouse schema-v1 Go paths (`clickhouse/`,
   `internal/chperf/`, and command files that import the ClickHouse package or
   call `loadClickhouseConfig`, excluding `_test.go` and `cmd/bolt_perf*.go`),
   when `go list -deps` or an AST import-graph scan runs and constructor calls
   are scanned, then no dependency path reaches
   `github.com/wtsi-hgi/wrstat-ui/bolt`,
   `github.com/wtsi-hgi/wrstat-ui/internal/boltperf`, or `go.etcd.io/bbolt`,
   and no file calls `bolt.NewDGUTAWriter`, `bolt.NewBaseDirsStore`,
   `bolt.OpenDatabase`, `bolt.OpenMultiBaseDirsReader`, or
   `bolt.OpenProvider`.
7. Given a database with `wrstat_schema_version.version = 2`, when
   `NewClient(cfg)` runs, then it returns
   `clickhouse: unexpected schema versions` and does not create readers.

### A2: Append-Only Active Mount Events

As an importer, I want active mount metadata as ordered events, so that publish,
rollback, tombstone, audit, and retry cleanup are deterministic.

`wrstat_mount_events` is append-only. `wrstat_mounts_active` is the only active
view. Publish and rollback append `event_type=1`; inactive cleanup appends
`event_type=0`. The active view orders inactive above publish for equal
`event_at` milliseconds and does not use `FINAL`.

**Package:** `clickhouse/`
**File:** `clickhouse/active_mounts.go`,
`clickhouse/active_snapshot_cleanup.go`, `clickhouse/dguta_writer.go`
**Test file:** `clickhouse/snapshot_test.go`,
`clickhouse/active_snapshot_cleanup_test.go`

**Acceptance tests:**

1. Given publish and inactive events for `/mnt/` with the same `event_at` and
   snapshot, when `wrstat_mounts_active` is queried, then `/mnt/` is absent.
2. Given publish events for `/mnt/` snapshots `A` then `B`, when
   `wrstat_mount_events` is queried, then both rows remain and
   `wrstat_mounts_active` returns only snapshot `B`.
3. Given an active failed snapshot `B` and previous active snapshot `A`, when
   `CleanActiveSnapshotAttempt(cfg, "/mnt/", updatedAtB)` succeeds, then an
   active event for `A` is appended, snapshot `B` partitions are dropped using
   `activeSnapshotCleanupTimeout`, and snapshot `A` partitions remain.
4. Given an active failed snapshot `B` and no previous active row, when cleanup
   succeeds, then an inactive event for `B` is appended, snapshot `B`
   partitions are dropped using `activeSnapshotCleanupTimeout`, and
   `wrstat_mounts_active` has no `/mnt/` row.
5. Given current active snapshot `B` partition drop blocks past
   `activeSnapshotCleanupTimeout`, when `CleanActiveSnapshotAttempt` runs,
   then it returns an error containing `current_snapshot_partition_drop`, no
   rollback or inactive event is appended, and `B` remains the active row.
6. Given cleanup races with a newer active event after the cleanup baseline,
   when cleanup runs, then it returns
   `clickhouse: failed to clean active snapshot mount row` and includes the
   latest five mount event details.

## Section B: Directory Facts Import And Readiness

### B1: Stream Canonical Directory Facts

As an importer, I want to build directory facts directly during import, so that
tree reads need one canonical table and import memory stays bounded.

The deterministic snapshot pipeline is:

1. Drop stale partitions for the target inactive deterministic snapshot.
2. Stream file rows into `wrstat_files`.
3. Stream one `wrstat_dir_facts` row per mount directory.
4. Stream `wrstat_children` rows.
5. Stream selected `wrstat_dir_filter_ageall` rows, if the gate selected it.
6. Write one `wrstat_dir_projection_sets` row.
7. Publish `wrstat_mount_events`.
8. Drop previous inactive snapshot partitions.

Normal facts and selected indexes are not built by post-import
`INSERT ... SELECT`.

**Package:** `clickhouse/`
**File:** `clickhouse/dguta_writer.go`, `clickhouse/dir_facts.go`,
`clickhouse/import_block_writer.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`clickhouse/import_context_test.go`

**Acceptance tests:**

1. Given a fixture with directories `/mnt/`, `/mnt/a/`, `/mnt/a/b/`, and
   `/mnt/c/`, when import closes successfully, then `wrstat_dir_facts` has
   exactly 4 rows for that `(mount_path, snapshot_id)`.
2. Given fixture files with `(gid, uid, ft, age, count, size)` rows
   `(7, 11, bam, 0, 2, 20)`, `(7, 12, other, 0, 1, 5)`, and
   `(8, 11, bam, 3, 4, 40)` under `/mnt/a/`, when `/mnt/a/` is read from
   `wrstat_dir_facts`, then `gids`, `uids`, `fts`, `ages`, `counts`,
   `sizes`, `atime_mins`, `mtime_maxs`, `atime_buckets`, and
   `mtime_buckets` all have length 3 and matching index values.
3. Given the same fixture, when `/mnt/a/` is read, then `all_count = 7`,
   `all_size = 65`, `all_uids = [11,12]`, `all_gids = [7,8]`, and
   `bitAnd(all_ft, bam) > 0`.
4. Given `/mnt/file-scalars/` has AgeAll rows:
   `(gid=7, uid=11, ft=bam, count=2, size=20, atime_min=100,
   mtime_max=200, atime_buckets=[2,0,0,0,0,0,0,0,0],
   mtime_buckets=[0,2,0,0,0,0,0,0,0])`,
   `(gid=8, uid=12, ft=dir, count=1, size=5, atime_min=80,
   mtime_max=250, atime_buckets=[0,1,0,0,0,0,0,0,0],
   mtime_buckets=[1,0,0,0,0,0,0,0,0])`, and
   `(gid=8, uid=13, ft=cram, count=3, size=30, atime_min=120,
   mtime_max=180, atime_buckets=[0,0,3,0,0,0,0,0,0],
   mtime_buckets=[0,0,3,0,0,0,0,0,0])`, when that row is read from
   `wrstat_dir_facts`, then `file_count = 5`, `file_size = 50`,
   `file_atime_min = 100`, `file_mtime_max = 200`,
   `file_atime_buckets = [2,0,3,0,0,0,0,0,0]`,
   `file_mtime_buckets = [0,2,3,0,0,0,0,0,0]`,
   `file_uids = [11,13]`, `file_gids = [7,8]`, and
   `file_ft = bam|cram`.
5. Given `/mnt/a/` has direct child `/mnt/a/b/` and no other direct child,
   when import closes, then `/mnt/a/` has `child_count = 1` in
   `wrstat_dir_facts` and `wrstat_children` has one matching edge.
6. Given a generated t283-shaped 1,000,000-record import with about
   230,000 directories, when the facts writer runs in an integration gate,
   then max RSS is less than 1.5 GiB and rows are flushed in bounded blocks.
7. Given an import SQL spy and spy block-writer factories for facts, children,
   and any selected AgeAll index, when `Close()` succeeds, then each selected
   writer has non-zero append and send counts, and no normalized executed SQL
   is an `INSERT INTO ... SELECT` targeting `wrstat_dir_facts`,
   `wrstat_children`, or selected `wrstat_dir_filter_ageall`.

### B2: Publish Readiness Last

As a tree reader, I want a row-exists readiness marker, so that partial
snapshot facts are never visible through active routes.

`wrstat_dir_projection_sets` is written only after facts, child edges, and any
selected derived index are complete. Readers check readiness before using
snapshot facts. No branch-local summary version column exists.

**Package:** `clickhouse/`
**File:** `clickhouse/dir_facts.go`, `clickhouse/dguta_writer.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`clickhouse/snapshot_test.go`

**Acceptance tests:**

1. Given facts and children for snapshot `S` but no
   `wrstat_dir_projection_sets` row, when `DirInfo("/mnt/", nil)` runs, then
   it does not read `S` and returns `db.ErrDirNotFound` if no older active
   ready snapshot exists.
2. Given the readiness marker for snapshot `S`, when
   `DirInfo("/mnt/", &db.Filter{Age: db.DGUTAgeAll})` runs, then it returns
   the scalar summary from `wrstat_dir_facts`.
3. Given an injected facts-writer failure before child edge completion, when
   `Close()` returns, then no readiness marker or active event exists for the
   failed snapshot.
4. Given the optional AgeAll index is selected and index writing fails, when
   `Close()` returns, then no readiness marker exists and facts partitions for
   the failed snapshot are dropped during cleanup.

### B3: Retry And Partition Cleanup

As an operator, I want failed deterministic snapshots to be retryable, so that
reruns can repair imports without corrupting active data.

Retry reset drops the target inactive snapshot partitions for all snapshot
tables. Rewriting an already active deterministic snapshot is refused. Cleanup
uses `activeSnapshotCleanupTimeout` semantics for failed, current, and old
partition drops. Best-effort or asynchronous maintenance failures are reported
through provider error callbacks or import diagnostics.

**Package:** `clickhouse/`, `cmd/`
**File:** `clickhouse/dguta_writer.go`, `clickhouse/active_snapshot_cleanup.go`,
`clickhouse/provider.go`, `cmd/summarise_diagnostics.go`
**Test file:** `clickhouse/active_snapshot_cleanup_test.go`,
`clickhouse/snapshot_test.go`, `clickhouse/provider_test.go`,
`cmd/summarise_diagnostics_test.go`

**Acceptance tests:**

1. Given a failed inactive snapshot `S` with rows in `wrstat_files`,
   `wrstat_dir_facts`, `wrstat_children`, basedirs snapshot tables, and the
   optional index if selected, when import retries `S`, then retry reset drops
   all those partitions with `activeSnapshotCleanupTimeout` before appending
   new rows.
2. Given failed inactive snapshot `S` partition drop blocks past
   `activeSnapshotCleanupTimeout`, when import retries `S`, then it returns an
   error containing `failed_snapshot_partition_drop`, appends no new snapshot
   rows, and does not change active mount metadata.
3. Given `S` is the active snapshot for `/mnt/`, when import tries to rewrite
   deterministic snapshot `S`, then it returns
   `clickhouse: refusing to rewrite active snapshot`.
4. Given old active snapshot `A` and successful new snapshot `B`, when publish
   completes, then `B` is active, `A` snapshot partitions are dropped with the
   `activeSnapshotCleanupTimeout`, and `wrstat_mount_events` keeps both
   active-event rows.
5. Given old snapshot partition drop fails, when import returns, then the
   returned error contains `old_snapshot_partition_drop` context and the new
   active event is not hidden or rolled back.
6. Given provider asynchronous virtual-children refresh is injected to fail
   with `errRefresh` and an `OnError` callback is registered, when the provider
   observes an active-set change, then the callback receives exactly one error
   containing `virtual_children_refresh`, `active_set_id`, and `errRefresh`.
7. Given post-publish best-effort old-snapshot cleanup is injected to fail and
   summarise diagnostics are enabled, when import returns, then diagnostics
   contain `old_snapshot_partition_drop`, mount path, snapshot ID, and the
   underlying error string.

### B4: Shared Bounded Block Writer

As a maintainer, I want one tested block-writer lifecycle for related import
tables, so that timeout and flush fixes apply consistently.

The internal helper covers lazy prepare, max-block flush, open-too-long flush,
final partial send, empty-batch abort, no eager reprepare after send, and
`context.WithoutCancel` prepare/send detachment. Row validation, phase names,
rollback, active snapshot checks, and table-specific cleanup stay explicit.
File ingest remains separate unless lower-level sharing preserves column
buffering and ambiguous-send behavior. Basedirs moves only with coverage for
eager prepare, history rollback, and snapshot cleanup.

**Package:** `clickhouse/`
**File:** `clickhouse/import_block_writer.go`,
`clickhouse/dguta_writer.go`
**Test file:** `clickhouse/import_context_test.go`,
`clickhouse/dguta_writer_test.go`, `clickhouse/file_ingest_operation_test.go`,
`clickhouse/basedirs_store_test.go`

**Acceptance tests:**

1. Given a new writer and spy batch factory, when `Close()` runs before any
   row is appended, then prepare, append, send, and abort counts are all `0`.
2. Given a spy batch factory, when the writer is constructed and then the
   first valid row is appended, then prepare count is `0` before append and
   `1` after append.
3. Given block size 2 and 5 fact rows, when the writer closes, then it sends
   blocks of 2, 2, and 1 rows and never sends an empty block.
4. Given a prepared non-empty block older than
   `importBatchMaxOpenDuration`, when another row is appended, then the old
   block sends before the new row is appended.
5. Given block size 2 and exactly 2 rows, when automatic flush sends the full
   block and no more rows are appended before `Close()`, then prepare count is
   `1`, send count is `1`, and no second prepare occurs after send.
6. Given the helper aborts a prepared batch with zero appended rows, when the
   abort path runs, then spy batch abort count is `1` and send count is `0`.
7. Given a send error after ClickHouse may have accepted a block, when close
   runs, then the writer does not retry that block and cleanup drops the
   deterministic snapshot partitions.
8. Given the parent import context is canceled during `Send`, when the
   clickhouse-go prepared batch uses its prepare context, then `Send` uses the
   detached timeout context and returns according to the import send timeout.
9. Given file ingest receives an ambiguous send error, when `Close()` runs,
   then existing file-ingest tests still prove the batch is not resent.

## Section C: Query Routing And API Semantics

### C1: Route Tree Reads Through Facts

As a web user, I want tree pages to read one clean facts source, so that
Disktree and `Where` stay responsive and correct for every filter.

`DirInfo`, `DirInfos`, broad Disktree child summaries, `Children`,
`DirsHaveChildren`, `Where`, `Info`, and tree permission checks use
`wrstat_dir_facts`, `wrstat_children`, `wrstat_mounts_active`, readiness, and
virtual hierarchy tables. They do not read raw DGUTA or `wrstat_files`;
`wrstat_files` is only for file-level APIs. Scalar columns serve unfiltered
and default-file AgeAll reads. Vector columns serve UID, GID, file-type, and
age filters using existing `db.GUTAs.Summary` semantics, including
temporary-file and empty-filter behavior.

**Package:** `clickhouse/`
**File:** `clickhouse/database.go`, `clickhouse/dir_facts.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`clickhouse/database_test.go`

**Acceptance tests:**

1. Given a ready snapshot and SQL query spy, when `DirInfo`, `DirInfos`,
   broad Disktree child summaries, `Children`, `DirsHaveChildren`, `Where`,
   `Info`, and tree `PermissionAnyInDir` run, then no query string contains
   `wrstat_dguta` or `wrstat_files`.
2. Given the same spy traces, when tree-route table names are extracted, then
   each table is one of `wrstat_dir_facts`, `wrstat_children`,
   `wrstat_dir_projection_sets`, `wrstat_mounts_active`,
   `wrstat_virtual_children`, `wrstat_virtual_children_sets`,
   `wrstat_virtual_summary_cache`, or `wrstat_virtual_summary_sets`, except
   C2 may use `wrstat_dir_filter_ageall` only for ready AgeAll owner/type
   filters.
3. Given `/mnt/a/` has vector entries for GID 7 and GID 8, when
   `DirInfo("/mnt/a/", &db.Filter{GIDs: []uint32{7}, Age: 0})` runs, then the
   result contains only GID 7 counts and sorted UIDs from matching entries.
4. Given a non-empty UID filter and an empty GID filter, when `DirInfo` runs,
   then it returns nil summary with no ClickHouse error.
5. Given an age-specific filter `Age = db.DGUTAgeA6M`, when `Where` runs, then
   it filters vector entries where `ages = 3` and does not use scalar
   `all_*` or `file_*` columns.
6. Given a default `Where(nil)` call, when the filter is normalized, then
   `FT = db.AllTypesExceptDirectories` and `Age = db.DGUTAgeAll` are applied.

### C2: Optional AgeAll Filter Index

As an implementor, I want an explicit gate for the only allowed filter index,
so that schema v1 does not add unnecessary row amplification.

Facts-only routing is the default. `wrstat_dir_filter_ageall` may be added only
after final larger-prefix perf runs show facts cannot meet whole-mount
filtered `Where` or high-fanout filtered `DirsHaveChildren` gates. The index
serves only `Age == db.DGUTAgeAll` filters with GID, UID, or file-type
predicates. Age-specific reads always use facts vectors.

**Package:** `clickhouse/`, `internal/chperf/`
**File:** `clickhouse/dir_filter_ageall.go`,
`internal/chperf/query.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`internal/chperf/query_test.go`

**Acceptance tests:**

1. Given the optional table is absent, when a filtered whole-mount `Where`
   runs, then it uses `wrstat_dir_facts` and returns the expected summaries.
2. Given the optional table is absent, when high-fanout
   `DirsHaveChildren("/mnt/", parents, &db.Filter{GIDs: []uint32{7},
   Age: 0})` runs, then it uses `wrstat_dir_facts` and returns the same
   boolean map as the facts-vector reference.
3. Given the optional table is present and ready, when
   `Where("/mnt/", &db.Filter{GIDs: []uint32{7}, Age: 0})` runs, then the
   query uses `wrstat_dir_filter_ageall` and returns the same summaries as the
   facts-vector route.
4. Given the optional table is present and ready, when high-fanout filtered
   `DirsHaveChildren` runs with the same AgeAll owner/type filter, then the
   query uses `wrstat_dir_filter_ageall` and returns the same boolean map as
   the facts-vector route.
5. Given the optional table is present but no readiness marker exists, when the
   same filtered `Where` runs, then it falls back to facts and does not read
   the index.
6. Given the optional table is present but no readiness marker exists, when the
   same filtered `DirsHaveChildren` runs, then it falls back to facts and does
   not read the index.
7. Given `Age = db.DGUTAgeM1Y`, when filtered `Where` and
   `DirsHaveChildren` run, then no query reads `wrstat_dir_filter_ageall`.
8. Given final gates pass with facts-only routing, when schema SQL is scanned,
   then no `wrstat_dir_filter_ageall` file exists.

### C3: Virtual Active Ancestors

As a web user, I want `/`, `/lustre/`, and `/nfs/` navigation to work without
large active-tree projections, so that ancestor pages stay responsive.

`wrstat_virtual_children` stores only virtual paths above mount roots for a
ready active set. Ancestor summaries compose live from active mount-root
`wrstat_dir_facts` rows. Add `wrstat_virtual_summary_cache` only if
many-active-mount gates fail live composition. Hot ancestor reads avoid
`FINAL`.

**Package:** `clickhouse/`
**File:** `clickhouse/virtual_children.go`, `clickhouse/database.go`
**Test file:** `clickhouse/database_test.go`, `clickhouse/provider_test.go`

**Acceptance tests:**

1. Given active mounts `/lustre/scratch120/`, `/lustre/scratch127/`, and
   `/nfs/t283_imaging/`, when virtual children refreshes, then parent `/`
   has children `/lustre` and `/nfs`, and `/lustre/` has children
   `/lustre/scratch120` and `/lustre/scratch127`.
2. Given those active mounts, when `DirInfo("/")` runs, then the result count,
   size, UID set, GID set, file-type mask, and bucket summaries equal the sum
   of the active mount-root `wrstat_dir_facts` rows.
3. Given active event `B` replaces active event `A`, when provider update
   publishes, then active metadata, projection readiness, virtual children,
   and optional virtual summary cache entries for `A` are not reused for `B`.
4. Given optional virtual cache is selected and the cache rows exist without a
   `wrstat_virtual_summary_sets` marker, when `DirInfo("/")` runs, then it
   ignores the cache and composes live facts.
5. Given hot virtual ancestor reads are traced, when `Children("/")` and
   `DirInfo("/")` run, then no traced query contains `FINAL`.
6. Given active set `A` is replaced by active set `B`, when old active-set
   cleanup drops `A` partitions from `wrstat_virtual_children` and the
   optional summary cache, then all rows and readiness markers for `B` remain
   and `Children("/")` plus `DirInfo("/")` return only `B` data.
7. Given many-active-mount gates pass with live composition, when base schema
   SQL is scanned, then no file or `CREATE` statement defines
   `wrstat_virtual_summary_cache` or `wrstat_virtual_summary_sets`.

### C4: Info And Permission Counts

As a CLI user, I want `Info` and permission checks to keep their meanings, so
that removing raw DGUTA does not change API results.

`db.Info.NumDirs` counts active directory fact rows. `db.Info.NumDGUTAs` keeps
its API meaning by summing active vector entry counts from `wrstat_dir_facts`.
`NumParents` and `NumChildren` come from `wrstat_children`. Permission checks
use AgeAll vector entries or the optional AgeAll index when applicable.

**Package:** `clickhouse/`
**File:** `clickhouse/database.go`, `clickhouse/file_api.go`
**Test file:** `clickhouse/database_info_test.go`,
`clickhouse/file_api_test.go`

**Acceptance tests:**

1. Given active facts with 3 directories and vector lengths 2, 0, and 5, when
   `Info()` runs, then `NumDirs = 3` and `NumDGUTAs = 7`.
2. Given 2 parent dirs and 4 child edge rows, when `Info()` runs, then
   `NumParents = 2` and `NumChildren = 4`.
3. Given `/mnt/a/` AgeAll vectors include UID 11 or GID 7, when
   `PermissionAnyInDir(ctx, "/mnt/a/", 11, []uint32{9})` runs, then it returns
   true; for UID 99 and GID 98 it returns false.
4. Given a query spy, when `Info()` and tree `PermissionAnyInDir` run, then no
   query contains `wrstat_dguta` or `wrstat_files`.

### C5: File-Level APIs And Extension Predicates

As a file API caller, I want file behavior preserved while simple extension
globs get safe pruning, so that optimizations do not change matches.

`wrstat_files` remains canonical for `ListDir`, `StatPath`, `IsDir`,
`FindByGlob`, and file-level permission checks. `FindByGlob` may add `ext`
predicates only for exact-safe extension globs such as `*.bam` and
`**/*.bam`. Regex matching remains authoritative. Bare dotfile exceptions such
as `.bam` matching `*.bam` remain correct. File-level permission checks use
exact `wrstat_files` rows and the same owner predicate as file owner filters:
`uid == opts.UID OR gid IN opts.GIDs`.

**Package:** `clickhouse/`
**File:** `clickhouse/file_api.go`
**Test file:** `clickhouse/file_api_test.go`,
`clickhouse/client_file_api_test.go`

**Acceptance tests:**

1. Given files `a.bam`, `.bam`, `b.BAM`, and `c.cram`, when
   `FindByGlob(ctx, []string{"/mnt/"}, []string{"*.bam"}, opts)` runs, then
   results include `a.bam` and `.bam`, exclude `c.cram`, and match existing
   case semantics for `b.BAM`.
2. Given pattern `**/*.bam`, when the query is inspected, then it contains an
   exact-safe `ext` predicate and still contains the regex `match` predicate.
3. Given pattern `*.[bc]am`, when the query is inspected, then it does not add
   an `ext` predicate.
4. Given `RequireOwner=true`, `UID=10`, and `GIDs=[20]`, when glob results are
   returned, then every row has `uid = 10` or `gid = 20`.
5. Given rows `/mnt/a/owned.bam` with `ext="bam",uid=10,gid=30`,
   `/mnt/a/group.bam` with `ext="bam",uid=11,gid=20`,
   `/mnt/a/denied.bam` with `ext="bam",uid=11,gid=30`, and
   `/mnt/a/fake.cram` with `ext="bam",uid=10,gid=20`, when
   `FindByGlob(ctx, []string{"/mnt/"}, []string{"**/*.bam"},
   FindOptions{RequireOwner: true, UID: 10, GIDs: []uint32{20}})` runs under
   a SQL query spy, then the query contains an exact-safe `f.ext` predicate
   for `"bam"`, still contains the regex `match` predicate, contains the
   owner predicate for `UID=10` or `GIDs=[20]`, and results are exactly
   `/mnt/a/owned.bam` and `/mnt/a/group.bam`.
6. Given `ListDir`, `StatPath`, and `IsDir` fixtures from current tests, when
   run against schema v1, then returned rows, sorting, limits, offsets, missing
   path errors, and invalid path errors match current behavior exactly.
7. Given active `wrstat_files` rows `/mnt/a/owned.txt` with `uid=10,gid=30`,
   `/mnt/a/group.txt` with `uid=11,gid=20`, and `/mnt/a/denied.txt` with
   `uid=11,gid=30`, when the file-level permission checker evaluates exact
   paths for `UID=10` and `GIDs=[20]`, then it returns true, true, and false
   in path order.
8. Given a SQL query spy, when the file-level permission checker runs, then
   every query reads `wrstat_files` and no query reads `wrstat_dguta`,
   `wrstat_dir_facts`, or `wrstat_children`.

## Section D: Basedirs, Subdirectories, And History

### D1: Preserve Basedirs Behavior

As a basedirs user, I want usage, subdirectory, and history reads unchanged, so
that schema cleanup does not affect quota workflows.

Basedirs snapshot tables are written for the same deterministic snapshot and
published through active mount events. Snapshot tables participate in retry and
old-snapshot partition cleanup. History remains append-only by mount. Basedirs
writer lifecycle is not generalized unless tests cover eager prepare, flush,
history rollback, and partition cleanup semantics.

**Package:** `clickhouse/`
**File:** `clickhouse/basedirs_store.go`, `clickhouse/basedirs_reader.go`
**Test file:** `clickhouse/basedirs_store_test.go`,
`clickhouse/basedirs_reader_test.go`, `clickhouse/history_maintainer_test.go`

**Acceptance tests:**

1. Given a successful import with basedirs data, when group usage, user usage,
   group subdirs, user subdirs, and history readers run, then returned rows
   equal the existing ClickHouse fixture expectations.
2. Given retry reset for deterministic snapshot `S`, when basedirs snapshot
   tables contain rows for `S`, then all four basedirs snapshot partitions for
   `S` are dropped before retry writes new rows.
3. Given a basedirs import fails after usage rows but before finalise, when
   cleanup runs, then active mount metadata is not advanced and partial
   snapshot rows are dropped.
4. Given history rows for dates `2026-05-01` and `2026-05-02`, when a later
   snapshot is dropped, then history rows remain queryable.

## Section E: Perf Harness And Gates

### E1: Extend ClickHouse Perf Reports

As an implementor, I want `clickhouse-perf` to expose schema-v1 evidence, so
that optional tables are selected only with measured proof.

`clickhouse-perf` must require or clearly report DSN and database before runs.
Import reports include table row counts, active parts, compressed bytes,
uncompressed bytes, selected table list, facts vector stats, max RSS where
available, and phase metrics for every selected table. Query reports include
reliable read rows, read bytes, and read marks using profile events when
`system.query_log` is unavailable.

**Package:** `internal/chperf/`, `cmd/`
**File:** `internal/chperf/import.go`, `internal/chperf/query.go`,
`internal/chperf/clickhouse_api.go`, `cmd/clickhouse_perf.go`
**Test file:** `internal/chperf/import_test.go`,
`internal/chperf/query_test.go`, `cmd/clickhouse_perf_test.go`

**Acceptance tests:**

1. Given no DSN or database, when `clickhouse-perf import` starts, then it
   returns before reading input with an error containing both missing settings.
2. Given no DSN or database, when `clickhouse-perf query` starts, then it
   returns before running operations with an error containing both missing
   settings.
3. Given a successful import, when the JSON report is decoded, then
   `selected_tables` contains `wrstat_files`, `wrstat_dir_facts`,
   `wrstat_children`, `wrstat_dir_projection_sets`, and all basedirs tables.
4. Given the same report, when selected table stats are decoded, then every
   selected table has `rows`, `active_parts`, `compressed_bytes`,
   `uncompressed_bytes`, and import phase duration fields.
5. Given facts rows with vector lengths 0, 3, and 9, when the import report is
   decoded, then vector stats are `rows=3`, `total_entries=12`,
   `average_entries_per_dir=4`, and `max_entries_per_dir=9`.
6. Given facts bucket arrays with lengths 10, 10, and 0, when the import report
   is decoded, then bucket stats are `rows=3`, `non_empty_rows=2`,
   `max_buckets=10`, and `mismatched_bucket_rows=0`.
7. Given a query run with query-log access disabled, when the JSON report is
   decoded, then each measured operation has non-negative `read_rows`,
   `read_bytes`, and `read_marks` fields sourced from profile events.
8. Given focused query ops are requested, when the JSON report is decoded, then
   it contains ops named `dirinfo_broad`, `dirinfo_filtered`,
   `dirinfos_broad`, `dirinfos_filtered`, `dirshavechildren_broad`,
   `dirshavechildren_filtered`, `where_whole_mount`,
   `where_filtered_whole_mount`, `virtual_children`, `virtual_dirinfo`, and
   `find_glob_extension_dotfile`, each with result count and latency fields.
9. Given optional AgeAll index or virtual summary cache is selected, when
   import report is decoded, then its rows, bytes, active parts, and phase
   durations are present under its table name.

### E2: Run Final Perf Gates

As a reviewer, I want final gates on production-like prefixes, so that clean
schema v1 does not regress the current branch.

Run repeated larger-prefix gates on roots found under `/home/ubuntu/output`,
especially `scratch120`, `scratch122`, `scratch127`, and `t283_imaging`.
Facts-only routing is the required first implementation. Select the narrow
AgeAll index or virtual-summary cache only after its gate fails.
Each gate compares matched roots, filters, limits, and operations. Collect at
least five measured repetitions after warmup and store raw samples. Pass
requires identical results and:

- p95 and p99 reject statistically consistent regressions. A regression is
  consistent when schema-v1 is slower in at least 4 of 5 paired repetitions
  and the median p95 or p99 delta exceeds the baseline noise band. The noise
  band is `max(1 ms, 3 * baseline_mad)`, where `baseline_mad` is median
  absolute deviation of baseline samples, when baseline p50 is at least
  `10 ms`.
- Small-median latency tolerance applies only when baseline p50 is below
  `10 ms`: p95 and p99 may be up to `max(0.1 ms, 10%)` above baseline. Do not
  apply this tolerance to cold-provider runs or filtered `Where`.
- Import wall and phase durations use the same latency rule. Import max RSS,
  total row amplification, and per-table row counts never use the 10% latency
  tolerance.
- t283 100k import max RSS must be at most `425,000 KB`. Larger-prefix import
  RSS fails on any repeatable increase above `max(32 MiB, 5%)` over baseline.
- Total written rows per input record and non-optional per-table row counts
  must not exceed current-branch baselines in any successful repeated run.

**Package:** `internal/chperf/`, `clickhouse/`
**File:** `internal/chperf/query.go`, `internal/chperf/import.go`
**Test file:** `internal/chperf/query_test.go`, final perf artifacts under
`.tmp/agent/`

**Acceptance tests:**

1. Given t283 100k import baselines `4.35s` report, `4.39s` wall,
   `402,892 KB` max RSS, and current-branch row/phase metrics, when schema-v1
   t283 100k import runs, then wall and selected table phases pass the
   latency rule, max RSS is at most `425,000 KB`, and row amplification plus
   non-optional per-table row counts do not exceed baseline.
2. Given t283 100k no-ancestor `tree_where` baseline p50/p95/p99 `4/8/8 ms`,
   when schema-v1 no-ancestor `tree_where` runs, then p95 and p99 are at most
   `8.8 ms` and result sets are identical.
3. Given cold-provider `tree_where` baseline p50/p95/p99
   `102.558/115.235/118.260 ms`, when schema-v1 cold-provider `tree_where`
   runs, then p95 and p99 pass the no-regression latency rule and result sets
   are identical.
4. Given filtered cold-provider `tree_where` baseline p50/p95/p99
   `84.837/102.892/103.945 ms`, when common and selective UID/GID/type/age
   filters run, then p95 and p99 pass the no-regression latency rule and
   result sets are identical; if facts-only repeatedly fails for AgeAll
   owner/type filters, select
   `wrstat_dir_filter_ageall`.
5. Given current-branch Disktree cold-provider, warm-provider, and
   provider-update baselines for broad reads on the same roots, when schema-v1
   broad Disktree gates run, then p95 and p99 pass the latency rule for each
   matching baseline and result trees are identical.
6. Given current-branch Disktree cold-provider, warm-provider, and
   provider-update baselines for filtered reads on the same roots, when
   schema-v1 filtered Disktree gates run, then p95 and p99 pass the latency
   rule for each matching baseline and result trees are identical.
7. Given Disktree unfiltered baseline p50/p95/p99
   `63.418/103.636/109.406 ms` and filtered baseline
   `67.983/90.216/124.348 ms`, when schema-v1 aggregate Disktree gates run,
   then p95 and p99 pass the no-regression latency rule.
8. Given current-branch broad and filtered `DirInfo` baselines for
   `scratch120`, `scratch122`, `scratch127`, and `t283_imaging`, when
   schema-v1 `DirInfo` gates run, then p95 and p99 pass the latency rule and
   count, size, owner, type, and bucket summaries are identical.
9. Given current-branch broad and filtered `DirInfos` baselines for high-count
   directory batches, when schema-v1 `DirInfos` gates run, then p95 and p99
   pass the latency rule and all returned summaries are identical.
10. Given current-branch high-fanout broad `DirsHaveChildren` baselines, when
   schema-v1 broad `DirsHaveChildren` gates run, then p95 and p99 pass the
   latency rule and the boolean map is identical.
11. Given current-branch high-fanout filtered `DirsHaveChildren` baselines,
   when schema-v1 filtered `DirsHaveChildren` gates run, then p95 and p99
   pass the latency rule and the boolean map is identical; if
   facts-only repeatedly fails for AgeAll owner/type filters, select
   `wrstat_dir_filter_ageall`.
12. Given visible-child warm baseline p50/p95/p99 `0.008/0.012/0.017 ms`, when
   schema-v1 visible-child gates run, then median stays below `0.05 ms` and
   p99 stays below `0.1 ms`.
13. Given file list/stat/glob focused baselines with p50 roughly `6-13 ms`,
   when schema-v1 file gates run with extension globs and owner filters, then
   p95 and p99 pass the applicable latency rule and result sets are
   identical.
14. Given many active mounts, when virtual ancestor children and summaries run,
   then live composition passes the documented Disktree ancestor target; if it
   repeatedly fails, select `wrstat_virtual_summary_cache` and record its row
   count, bytes, refresh time, and read latency.
15. Given optional `wrstat_dir_filter_ageall` is selected, when final import
   gates run, then its row count, compressed bytes, uncompressed bytes, active
   parts, and import phase cost are reported, import latency gates pass, and
   RSS plus non-optional row-amplification caps remain satisfied.

## Implementation Order

1. A1 - Clean DDL and bootstrap. Replace schema files with final v1 objects,
   update schema-version bootstrap, add absence tests, and keep version `1`.
2. A2 - Active events. Implement `wrstat_mount_events`, final
   `wrstat_mounts_active`, publish/rollback/tombstone writes, cleanup queries,
   and active metadata cache invalidation.
3. B1, B2, B3, B4 - Facts import. Add the bounded block writer, stream
   `wrstat_dir_facts`, stream child edges, write readiness last, and remove
   raw DGUTA, summary, and vector production writers.
4. C1, C2, C4 - Facts query routing. Route `DirInfo`, `DirInfos`, broad
   Disktree child summaries, `DirsHaveChildren`, `Where`, `Info`, and tree
   permission checks through facts and child edges. Query-spy tests must reject
   both raw-DGUTA and `wrstat_files` tree-summary/filter reads. Remove
   raw-DGUTA fallback tests.
5. C3 - Virtual active hierarchy. Add `wrstat_virtual_children`, live ancestor
   composition from mount-root facts, active-set invalidation, and no-`FINAL`
   hot reads.
6. C5, D1 - File and basedirs preservation. Keep file APIs on `wrstat_files`,
   add exact-safe extension predicates, and verify basedirs, subdirectory,
   history semantics, and cleanup.
7. E1 - Perf harness. Extend `clickhouse-perf` reports and add focused
   operations needed by the final gates.
8. E2 - Final gates and optional objects. Run facts-only gates first. Add only
   the AgeAll index or virtual-summary cache if its gate fails, then rerun
   gates and keep selected objects as clean schema-v1 DDL.

Phases 1-3 are sequential. Phases 4-7 can proceed after phase 3 has stable
fixtures. Phase 8 is last.

## Appendix: Key Decisions

- `wrstat_dir_facts` is canonical for tree facts; any raw accumulator is a
  private import detail and never a production read source.
- Path strings stay canonical in tree and file tables. No general directory-id
  dictionary is part of schema v1.
- Aggregate-state tables are rejected for primary facts because normal reads
  must not depend on background merges, `OPTIMIZE FINAL`, or query-time state
  finalization.
- `wrstat_files` remains canonical only for file-level APIs. Hot tree reads do
  not scan file subtrees.
- Readiness is row-exists and written last. Branch-local summary versions are
  not part of schema v1.
- The only allowed filter index is the perf-gated AgeAll owner/type index.
  There are no GID-only, UID-only, or full-age default index variants.
- The optional virtual-summary cache is only for ancestor directories above
  active mount roots and only after many-active-mount gates require it.
- Tests use GoConvey per `go-conventions`; every acceptance test maps to
  explicit `So()` assertions. Memory and perf gates report real measurements,
  not mocked success.
