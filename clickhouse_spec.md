# ClickHouse backend spec (replacement for Bolt)

This document specifies the ClickHouse backend that replaces the Bolt backend
previously described in `interface_spec.md`.

The goal is that another agent can implement a new root package `clickhouse`
that satisfies the storage-neutral interfaces defined in `interface_spec.md`
(`db.Database`, `db.DGUTAWriter`, `basedirs.Store`, `basedirs.Reader`,
`basedirs.HistoryMaintainer`, and `server.Provider`), and then update `cmd/*`,
`main.go`, and tests to use `clickhouse` constructors.

Hard constraints (must hold when implementation is complete):

- Nothing outside the `clickhouse` package imports `github.com/ClickHouse/
  clickhouse-go/v2`.
- The `clickhouse` package does not re-export *any* clickhouse-go types.
  The only public API is:
  - constructors used by `cmd/*` and `main.go` to obtain interface instances
  - methods required by the interfaces in `interface_spec.md`
  - extra-goal query methods defined in this spec
- Only tests, `main.go`, and packages under `cmd/` import
  `github.com/wtsi-hgi/wrstat-ui/clickhouse`.

----------------------------------------------------------------------

## Package surface

The new root package is `github.com/wtsi-hgi/wrstat-ui/clickhouse`.

It exports only:

- `OpenProvider(cfg Config) (server.Provider, error)`
- `NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error)`
- `NewBaseDirsStore(cfg Config) (basedirs.Store, error)`
- `NewHistoryMaintainer(cfg Config) (basedirs.HistoryMaintainer, error)`

Plus the extra-goal file APIs:

- `NewClient(cfg Config) (*Client, error)`
- `NewFileIngestOperation(cfg Config, mountPath string, updatedAt time.Time)
  (summary.OperationGenerator, error)`

The extra-goal APIs require a small number of exported helper types
(`Client`, `FileRow`, and options structs). These types must not expose
clickhouse-go types.

----------------------------------------------------------------------

## Configuration

### `clickhouse.Config`

`Config` is a plain Go struct (no clickhouse-go types) and is passed into all
constructors. It must contain exactly the following fields:

- `DSN string`
  - A ClickHouse DSN for clickhouse-go v2, using the native protocol.
  - Must include `database=`.
- `Database string`
  - The ClickHouse database name that *this app* owns and may create.
  - Must match the `database` in `DSN`.
- `OwnersCSVPath string`
  - Required by the basedirs reader for owner display.
- `MountPoints []string`
  - Optional override for mountpoint auto-discovery (same semantics as today).
  - Empty means auto-discover using the current mountinfo logic.
- `PollInterval time.Duration`
  - How often `OpenProvider` polls for mount updates.
  - Zero or negative disables polling and therefore disables `OnUpdate`.
- `QueryTimeout time.Duration`
  - Per-query timeout applied inside the clickhouse package.

No other knobs are exported.

Bootstrap rule (normative):

- All constructors must ensure `cfg.Database` exists.
- If connecting with `cfg.DSN` fails because the database does not exist, the
  clickhouse package must:
  1. reconnect to the same server using the same DSN but with
     `database=default`
  2. `CREATE DATABASE IF NOT EXISTS {cfg.Database}`
  3. reconnect using `cfg.DSN` again

### `.env` loading convention (cmd only)

The clickhouse package itself must not read `.env` files.

For developer convenience, `cmd/server` and `cmd/summarise` should load:

- `.env` (defaults)
- `.env.local` (developer overrides)

using `github.com/joho/godotenv`.

Tests should not require `.env` files.

----------------------------------------------------------------------

## Data lifecycle model (snapshots + atomic swap)

ClickHouse stores multiple snapshots per mountpoint. Reads always use the
*active snapshot* per mountpoint.

Terms:

- `mount_path`: the mount directory path (absolute, ends with `/`). This is the
  same key used by `/rest/v1/auth/dbsUpdated`.
- `updated_at`: the dataset snapshot time (stats.gz mtime).
- `snapshot_id`: a UUID identifying one ingested snapshot for one mount.

Snapshot id derivation (normative):

- `snapshot_id` is deterministic for a given `(mount_path, updated_at)`.
- Use UUIDv5 of `mount_path + "|" + updated_at.UTC().Format(time.RFC3339Nano)`.

Write-side rule:

- Each summarise run for one mount creates exactly one `snapshot_id`.
- All rows written by that run are tagged with that `(mount_path, snapshot_id)`.
- The new snapshot is made visible by appending a new row in `wrstat_mounts`
  (see schema) which becomes the active snapshot by `argMax()`.
- Old snapshot data is deleted after the switch by dropping the old snapshot
  partitions.

This provides an atomic read-side swap without requiring ClickHouse
transactions: partial ingests are never visible because readers only read
through the active snapshot pointer.

Performance note:

- Use the native protocol with LZ4 compression (via DSN) and `PrepareBatch`.
- Prefer fewer, larger batches (eg 50k-200k rows) over many small inserts.
- All read queries must `ANY INNER JOIN` to `wrstat_mounts_active` and filter
  by directory/path in `PREWHERE` when possible.

A mountpoint may be missing for some days. The system must always return the
latest available snapshot per mount, which may mean different `updated_at`
values across mounts.

----------------------------------------------------------------------

## Schema

All tables are created inside `cfg.Database`.

Schema creation is the responsibility of the clickhouse package. It must:

- Create tables/views if they do not exist.
- Verify an internal schema version table matches the expected version.
- Run in all constructors (provider + writers) so any entrypoint can bootstrap.

Embed all DDL as `.sql` files in the clickhouse package using `//go:embed`.
Do not build SQL dynamically.

### 1. Schema version

- Table: `wrstat_schema_version`

DDL:

- `version UInt32`
- Engine: `TinyLog`

Exactly one row is stored. Current version is `1`.

### 2. Active snapshot pointer

- Table: `wrstat_mounts`

DDL:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `switched_at DateTime64(3) CODEC(Delta, ZSTD(3))`
- `active_snapshot UUID`
- `updated_at DateTime CODEC(Delta, ZSTD(3))`

Engine:

- `ReplacingMergeTree(switched_at)`
- `ORDER BY mount_path`

View:

- `wrstat_mounts_active`

DDL:

- `mount_path`
- `snapshot_id` = `argMax(active_snapshot, switched_at)`
- `updated_at` = `argMax(updated_at, switched_at)`

This view is the single source of truth for `MountTimestamps()`.

### 3. DGUTA aggregates (tree/where)

- Table: `wrstat_dguta`

DDL:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `snapshot_id UUID`
- `dir String CODEC(ZSTD(3))`
- `gid UInt32`
- `uid UInt32`
- `ft UInt16`
- `age UInt8`
- `count UInt64 CODEC(Delta, ZSTD(3))`
- `size UInt64 CODEC(Delta, ZSTD(3))`
- `atime_min Int64 CODEC(Delta, ZSTD(3))`
- `mtime_max Int64 CODEC(Delta, ZSTD(3))`

Engine:

- `MergeTree`
- `PARTITION BY (mount_path, snapshot_id)`
- `ORDER BY (mount_path, snapshot_id, dir, age, gid, uid, ft)`

### 4. Children edges

- Table: `wrstat_children`

DDL:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `snapshot_id UUID`
- `parent_dir String CODEC(ZSTD(3))`
- `child String CODEC(ZSTD(3))`

Engine:

- `MergeTree`
- `PARTITION BY (mount_path, snapshot_id)`
- `ORDER BY (mount_path, snapshot_id, parent_dir, child)`

`child` is the full child path, without a trailing `/` (matching the current
Bolt behaviour).

### 5. Basedirs usage

Two tables, mirroring `basedirs.Usage` without name/owner fields.

- Table: `wrstat_basedirs_group_usage`

Columns:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `snapshot_id UUID`
- `gid UInt32`
- `basedir String CODEC(ZSTD(3))`
- `age UInt8`
- `uids Array(UInt32)`
- `usage_size UInt64 CODEC(Delta, ZSTD(3))`
- `quota_size UInt64 CODEC(Delta, ZSTD(3))`
- `usage_inodes UInt64 CODEC(Delta, ZSTD(3))`
- `quota_inodes UInt64 CODEC(Delta, ZSTD(3))`
- `mtime DateTime CODEC(Delta, ZSTD(3))`
- `date_no_space DateTime CODEC(Delta, ZSTD(3))`
- `date_no_files DateTime CODEC(Delta, ZSTD(3))`

Engine:

- `MergeTree`
- `PARTITION BY (mount_path, snapshot_id)`
- `ORDER BY (mount_path, snapshot_id, gid, age, basedir)`

- Table: `wrstat_basedirs_user_usage`

Same as above but with:

- `uid UInt32`
- `gids Array(UInt32)`

and `ORDER BY (mount_path, snapshot_id, uid, age, basedir)`.

### 6. Basedirs subdirs

Subdirs are stored row-per-subdir to avoid large encoded blobs.

- Table: `wrstat_basedirs_group_subdirs`

Columns:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `snapshot_id UUID`
- `gid UInt32`
- `basedir String CODEC(ZSTD(3))`
- `age UInt8`
- `pos UInt32`
- `subdir String CODEC(ZSTD(3))`
- `num_files UInt64 CODEC(Delta, ZSTD(3))`
- `size_files UInt64 CODEC(Delta, ZSTD(3))`
- `last_modified DateTime CODEC(Delta, ZSTD(3))`
- `file_usage Map(UInt16, UInt64)`

Engine:

- `MergeTree`
- `PARTITION BY (mount_path, snapshot_id)`
- `ORDER BY (mount_path, snapshot_id, gid, age, basedir, pos)`

`pos` preserves the slice ordering passed to `PutGroupSubDirs()`.

- Table: `wrstat_basedirs_user_subdirs`

Same as above but keyed by `uid UInt32`.

### 7. Basedirs history

- Table: `wrstat_basedirs_history`

DDL:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `gid UInt32`
- `date DateTime CODEC(Delta, ZSTD(3))`
- `usage_size UInt64 CODEC(Delta, ZSTD(3))`
- `quota_size UInt64 CODEC(Delta, ZSTD(3))`
- `usage_inodes UInt64 CODEC(Delta, ZSTD(3))`
- `quota_inodes UInt64 CODEC(Delta, ZSTD(3))`

Engine:

- `MergeTree`
- `PARTITION BY mount_path`
- `ORDER BY (mount_path, gid, date)`

History is append-only with a strict-newer rule (see Store semantics below).

----------------------------------------------------------------------

## Interface implementation mapping

This section is normative: implement exactly this behaviour.

### `server.Provider` (`clickhouse.OpenProvider`)

`OpenProvider(cfg)` returns an object that:

- Creates and owns one clickhouse connection pool.
- Lazily (re)builds:
  - a `db.Tree` backed by a clickhouse `db.Database` implementation
  - a `basedirs.Reader` backed by ClickHouse
- Implements `OnUpdate(cb func())` by polling `wrstat_mounts_active`:
  - On each poll, query `mount_path -> updated_at`.
  - If the map differs from the previous poll, swap internal readers and call
    the callback on a new goroutine.
  - Callbacks must not run concurrently with themselves.

The provider must not expose any ClickHouse concepts. It returns:

- `Tree() *db.Tree` (constructed as `db.NewTree(dbImpl)` after the refactor)
- `BaseDirs() basedirs.Reader`

### `db.Database` (read-side tree)

The clickhouse implementation must:

- Merge across all mountpoints by querying all rows for active snapshots.
- Return `ErrDirNotFound` only when the directory is absent from *all* active
  snapshots.
- Return `nil, nil` (no error) when the directory exists but the filter removes
  all results.
- Set `DirSummary.Modtime` to the maximum `updated_at` across contributing
  mounts for the returned summary.

Normalization:

- When querying by directory, normalize `dir` to have a trailing `/`.

Queries:

- Existence check (unfiltered):

  - `SELECT 1 FROM wrstat_dguta d
     ANY INNER JOIN wrstat_mounts_active a
       ON d.mount_path = a.mount_path AND d.snapshot_id = a.snapshot_id
     WHERE d.dir = {dir}
     LIMIT 1`

- Summary query (filtered):

  - Apply filters only if they are non-empty / non-zero, matching current Go
    semantics:

    - GIDs: `d.gid IN (...)` when `filter.GIDs != nil`
    - UIDs: `d.uid IN (...)` when `filter.UIDs != nil`
    - Age:  always apply `d.age = {filter.Age}` (callers set this)
    - FT:   apply only when `filter.FT != 0`:
      `bitAnd(d.ft, {filter.FT}) != 0`

  - Aggregate:

    - `Count` = `sum(d.count)`
    - `Size`  = `sum(d.size)`
    - `Atime` = `toDateTime(min(d.atime_min))`
    - `Mtime` = `toDateTime(max(d.mtime_max))`
    - `UIDs`  = `arraySort(groupUniqArray(d.uid))`
    - `GIDs`  = `arraySort(groupUniqArray(d.gid))`
    - `FT`    = `bitOr(d.ft)`
    - `Age`   = `filter.Age`
    - `Modtime` = `max(a.updated_at)`

### `db.Database.Children(dir string) ([]string, error)`

Behaviour must match current Bolt behaviour:

- Returns a de-duplicated, sorted list across mounts.
- Returns `nil`/empty when there are no children or the dir is missing.

Normalization:

- Normalize `dir` to end with `/` before matching `parent_dir`.

Query:

- `SELECT DISTINCT c.child
   FROM wrstat_children c
   ANY INNER JOIN wrstat_mounts_active a
     ON c.mount_path = a.mount_path AND c.snapshot_id = a.snapshot_id
   WHERE c.parent_dir = {dir}
   ORDER BY c.child ASC`

### `db.Database.Info()`

Return values must match the existing `db.DBInfo` meaning:

- `NumDirs`: number of directory keys present
- `NumDGUTAs`: number of dguta rows (dir+gid+uid+ft+age combos)
- `NumParents`: number of distinct parent_dir entries in children
- `NumChildren`: number of child edges

Compute over active snapshots only.

### `db.DGUTAWriter` (write-side tree ingest)

The writer streams `db.RecordDGUTA` into ClickHouse.

Required behaviour:

- `SetMountPath()` and `SetUpdatedAt()` must be called before the first `Add`.
- The writer generates one `snapshot_id` on first use.
- Before inserting any rows, the writer must ensure the target partitions for
  `(mount_path, snapshot_id)` are empty by dropping those partitions in
  `wrstat_dguta` and `wrstat_children` (idempotent retry behaviour).
- `Add()` writes:
  - one row per `GUTA` in `RecordDGUTA.GUTAs` into `wrstat_dguta`
  - one row per child in `RecordDGUTA.Children` into `wrstat_children`
- Batching:
  - Implement batching using clickhouse-go v2 `PrepareBatch`.
  - A `SetBatchSize` value of `10_000` must work without OOM.
- Close:
  - Flush all batches.
  - Switch the active snapshot by inserting into `wrstat_mounts`.
  - Drop old snapshot partitions for this mount in `wrstat_dguta`,
    `wrstat_children`, and all basedirs snapshot tables (usage/subdirs/files).
    The old snapshot id is read from `wrstat_mounts_active`.
  - If Close fails before switching the snapshot, the new snapshot must be
    dropped (cleanup) so it is not leaked.

Caller responsibility (normative):

- `cmd/summarise` must only switch the active snapshot after *all* writes for
  that snapshot have completed (DGUTA, children, basedirs usage/subdirs, and
  file rows).
- Therefore, the clickhouse implementation must make `DGUTAWriter.Close()` the
  single place that performs the `wrstat_mounts` switch, and `cmd/summarise`
  must close the basedirs store and file ingest operation before closing the
  DGUTA writer.

----------------------------------------------------------------------

## Basedirs store + reader

### `basedirs.Store`

The store writes one mount's basedirs snapshot.

Mapping:

- `SetMountPath(mountPath)` sets `mount_path` for all subsequent writes.
- `SetUpdatedAt(updatedAt)` is stored in `wrstat_mounts` on snapshot switch.
- `Reset()` deletes any prior *staged* data for the current in-progress
- `Reset()` must ensure the snapshot partitions for `(mount_path, snapshot_id)`
  are empty by dropping those partitions in:
  - `wrstat_basedirs_group_usage`
  - `wrstat_basedirs_user_usage`
  - `wrstat_basedirs_group_subdirs`
  - `wrstat_basedirs_user_subdirs`

  This makes reruns with the same `(mount_path, updated_at)` safe.
- `PutGroupUsage` inserts into `wrstat_basedirs_group_usage`.
- `PutUserUsage` inserts into `wrstat_basedirs_user_usage`.
- `PutGroupSubDirs` inserts one row per subdir into
  `wrstat_basedirs_group_subdirs`, with `pos` equal to the slice index.
- `PutUserSubDirs` similarly.

History append rule:

- `AppendGroupHistory(key, point)` must append only if `point.Date` is strictly
  after the last stored date for `(key.GID, key.MountPath)`.

Implementation requirement:

- Implement the append rule inside ClickHouse without reading the full series.
  Do exactly:

  - Query the last date:

    `SELECT max(date) FROM wrstat_basedirs_history
     WHERE mount_path = {mount} AND gid = {gid}`

  - If max(date) is NULL or < point.Date, insert the new row.

Finalize:

- `Finalize()` must ensure that every inserted group usage row where
  `age == DGUTAgeAll` has correct `date_no_space` and `date_no_files` values.
- Compute these in Go using the existing `basedirs.DateQuotaFull` algorithm,
  by reading the full history series for `(gid, mount_path)`.

Update-in-place is not permitted. Instead, the store must delay insertion of
`age == DGUTAgeAll` group usage rows until it can compute the quota dates.

Required insertion strategy:

- In `PutGroupUsage(u)`:
  - if `u.Age != DGUTAgeAll`, insert the row immediately (with
    `date_no_space`/`date_no_files` as zero values)
  - if `u.Age == DGUTAgeAll`, buffer the row in memory
- In `Finalize()`:
  1. For each buffered gid, query the full history series for the gid.
  2. Compute quota dates.
  3. Insert the buffered `DGUTAgeAll` rows with computed quota dates.

This buffering is bounded by (num group usage rows in one mount) and is
required to keep read-path fast.

### `basedirs.Reader`

The reader queries across all active mount snapshots.

Name/owner filling:

- `Usage.Owner` is filled from the owners CSV mapping (gid -> owner).
- `Usage.Name` is filled via the existing user/group caches.
- The ClickHouse tables do not store names.

Ordering:

- `GroupUsage` and `UserUsage` must return results ordered by the same logical
  key order as Bolt: `(id, basedir)`.
- Subdirs are ordered by `pos`.
- History is ordered by `date` ascending.

`MountTimestamps()`:

- Returns `mount_path -> updated_at` from `wrstat_mounts_active`.

`Info()`:

Return the same counts as the existing `basedirs.DBInfo`, computed over active
snapshots only.

### `basedirs.HistoryMaintainer`

`CleanHistoryForMount(prefix)`:

- Delete all history rows whose `mount_path` does not start with `prefix`.
- This is a safety tool; it must run only on the configured database.

`FindInvalidHistory(prefix)`:

- Return distinct `(gid, mount_path)` pairs that would be deleted by
  `CleanHistoryForMount(prefix)`.

----------------------------------------------------------------------

## Extra-goal file APIs

To support other apps, the clickhouse package must also store file-level rows
from stats.gz and expose query helpers.

### Schema: `wrstat_files`

DDL:

- `mount_path LowCardinality(String) CODEC(ZSTD(3))`
- `snapshot_id UUID`
- `path String CODEC(ZSTD(3))`
- `parent_dir String CODEC(ZSTD(3))`
- `name String CODEC(ZSTD(3))`
- `ext LowCardinality(String) CODEC(ZSTD(3))`
- `entry_type UInt8`
- `size Int64 CODEC(Delta, ZSTD(3))`
- `apparent_size Int64 CODEC(Delta, ZSTD(3))`
- `uid UInt32`
- `gid UInt32`
- `atime Int64 CODEC(Delta, ZSTD(3))`
- `mtime Int64 CODEC(Delta, ZSTD(3))`
- `ctime Int64 CODEC(Delta, ZSTD(3))`
- `inode Int64`
- `nlink Int64`

Engine:

- `MergeTree`
- `PARTITION BY (mount_path, snapshot_id)`
- `ORDER BY (mount_path, snapshot_id, parent_dir, name, path)`

Projection (required):

- Add a projection to accelerate point lookups by absolute path:

  - `PROJECTION by_path (SELECT * ORDER BY (mount_path, snapshot_id, path))`

Indexes:

- `INDEX path_bf path TYPE bloom_filter(0.01) GRANULARITY 8`
- `INDEX name_bf name TYPE bloom_filter(0.01) GRANULARITY 8`
- `INDEX ext_bf ext TYPE bloom_filter(0.01) GRANULARITY 8`

Conventions:

- `path` is stored exactly as seen in stats.gz (directories end with `/`).
- `parent_dir` ends with `/`.
- `name` for directories includes trailing `/` (matching stats.gz).
- `ext` is empty for directories and for names without an extension.

### Ingestion changes

`cmd/summarise` must register an additional *global* summariser operation when
using ClickHouse:

- `NewFileIngestOperation(cfg, mountPath, updatedAt)`

This operation streams every file and directory from stats.gz into
`wrstat_files` for the same `(mount_path, snapshot_id)` as the DGUTA writer.

Before inserting any rows, it must drop the `wrstat_files` partition for
`(mount_path, snapshot_id)` to make reruns safe.

`snapshot_id` derivation is defined in the lifecycle section above. All
ClickHouse writers for a mount must use that same derived id.

### Public query helpers

These are additional public methods on a new exported type
`clickhouse.Client` returned by:

- `NewClient(cfg Config) (*Client, error)`

`Client` must not expose clickhouse-go types.

Required methods:

- `ListDir(ctx, dir string, opts ListOptions) ([]FileRow, error)`
- `StatPath(ctx, path string, opts StatOptions) (*FileRow, error)`
- `IsDir(ctx, path string) (bool, error)`
- `FindByGlob(ctx, baseDirs []string, patterns []string,
  opts FindOptions) ([]FileRow, error)`
- `PermissionAnyInDir(ctx, dir string, uid uint32, gids []uint32) (bool, error)`

Semantics:

- All methods operate over the active snapshot for the mount containing the
  queried path, using `wrstat_mounts_active`.
- Permission checks are ownership-based (uid/gid matching), because stats.gz
  does not contain POSIX mode bits.

Selection/paging:

- `opts` must allow selecting which metadata fields are returned so callers can
  avoid transferring unused columns.
- `opts` must support `Limit` and `Offset`.

Implementation guidance:

- For glob matching, translate gitignore-style patterns into ClickHouse
  predicates using:
  - prefix filtering on `parent_dir` where possible
  - `like` for `*` and `?`
  - `match` for patterns that require `**`
  - always restrict by mount active snapshot first

----------------------------------------------------------------------

## Testing (clickhouse package)

All public methods of the clickhouse package must be tested.

Tests must run against a real ClickHouse server started automatically.

Required approach:

- Use `testcontainers-go` to start ClickHouse `25.11.2.24` for tests.
- Create a unique database per test package run:

  `wrstat_ui_test_${USER}_${PID}_${RAND}`

- The tests must never connect to a non-local host.
- The clickhouse package must refuse to run destructive schema operations when
  `Database` does not start with `wrstat_ui_test_` while `WRSTAT_ENV=test`.

"Destructive schema operations" here means `DROP DATABASE` and any deletion of
history across mounts (eg, `CleanHistoryForMount`). Snapshot partition drops are
part of normal operation and are permitted in all environments.

This ensures:

- no conflicts between developers on the same machine
- no possibility of tests wiping or mutating production

----------------------------------------------------------------------

## Performance hooks

To enable iterative schema/perf tuning, the clickhouse package must include
benchmarks (in `_test.go`) that:

- measure `DGUTAWriter.Add` throughput (records/sec) with configurable batch
  sizes
- measure `db.Database.DirInfo` latency under a loaded dataset

Benchmarks must log:

- total rows inserted
- wall time
- rows/sec

No new CLI commands are added for performance testing.

----------------------------------------------------------------------

## Repository wiring (what other packages must do)

After the interfaces in `interface_spec.md` are in place, update only
constructors/wiring in:

- `cmd/server`: call `clickhouse.OpenProvider(cfg)` and pass the returned
  `server.Provider` into the server.
- `cmd/summarise`: replace Bolt writer/store constructors with the ClickHouse
  ones, ensure the close order matches the rule in the DGUTAWriter section, and
  add `NewFileIngestOperation` as a global summariser operation.
- `cmd/clean`: use `clickhouse.NewHistoryMaintainer(cfg)`.
- Tests: use `clickhouse` constructors; no other production package imports it.

When finished, there must be no remaining `go.etcd.io/bbolt` imports anywhere
in the repository.
