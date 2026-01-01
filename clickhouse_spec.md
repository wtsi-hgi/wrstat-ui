# ClickHouse backend spec (replacement for Bolt)

This document specifies the ClickHouse backend that replaces the Bolt backend.
This spec supersedes the Bolt-related sections and the earlier "ClickHouse
backend notes" section of `interface_spec.md`. The `bolt` package may remain
in the repository as unused code.

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
  - performance inspection helpers defined in this spec (Inspector API)
- Only tests, `main.go`, and packages under `cmd/` import
  `github.com/wtsi-hgi/wrstat-ui/clickhouse`.

----------------------------------------------------------------------

## Package surface

The new root package is `github.com/wtsi-hgi/wrstat-ui/clickhouse`.

It exports only:

- `type Config struct { ... }` (see Configuration section)
- `OpenProvider(cfg Config) (server.Provider, error)`
- `NewDGUTAWriter(cfg Config) (db.DGUTAWriter, error)`
- `NewBaseDirsStore(cfg Config) (basedirs.Store, error)`
- `NewHistoryMaintainer(cfg Config) (basedirs.HistoryMaintainer, error)`

Plus the extra-goal file APIs:

- `NewClient(cfg Config) (*Client, error)`
- `NewFileIngestOperation(cfg Config, mountPath string, updatedAt time.Time)
  (summary.OperationGenerator, io.Closer, error)`

Plus performance inspection helpers (used only by `cmd/clickhouse-perf`):

- `type QueryMetrics struct { ... }` (see Performance hooks)
- `NewInspector(cfg Config) (*Inspector, error)`
- `(*Inspector) ExplainListDir(ctx, mountPath, dir string,
  limit, offset int64) (string, error)`
- `(*Inspector) ExplainStatPath(ctx, mountPath, path string) (string, error)`
- `(*Inspector) Measure(ctx, run func(ctx context.Context) error)
  (*QueryMetrics, error)`

`Client` is exported and its query methods are part of the public API:

- `(*Client) ListDir(ctx, dir string, opts ListOptions) ([]FileRow, error)`
- `(*Client) StatPath(ctx, path string, opts StatOptions) (*FileRow, error)`
- `(*Client) IsDir(ctx, path string) (bool, error)`
- `(*Client) FindByGlob(ctx, baseDirs []string, patterns []string,
  opts FindOptions) ([]FileRow, error)`
- `(*Client) PermissionAnyInDir(ctx, dir string, uid uint32,
  gids []uint32) (bool, error)`
- `(*Client) Close() error`

The extra-goal APIs require a small number of exported helper types
(`Client`, `FileRow`, `ListOptions`, `StatOptions`, `FindOptions`). These
types must not expose clickhouse-go types.

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
- `MaxOpenConns int`
  - Optional. Maximum open connections in the pool.
  - Default: 10. For high-throughput ingest, set to the number of concurrent
    batch writers.
- `MaxIdleConns int`
  - Optional. Maximum idle connections to keep in the pool.
  - Default: same as MaxOpenConns. Should match MaxOpenConns to avoid churn.

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

For developer convenience, `cmd/server` and `cmd/summarise` must load `.env`
files using `github.com/joho/godotenv` (in order, later files override earlier):

1. `.env` (defaults, committed to repo for development)
2. `.env.local` (developer overrides, gitignored)

Environment variables for ClickHouse configuration:

- `WRSTAT_CLICKHOUSE_DSN` (required): ClickHouse DSN for native protocol, eg
  `clickhouse://user:pass@localhost:9000/wrstat?dial_timeout=5s&compress=lz4`
- `WRSTAT_CLICKHOUSE_DATABASE` (required): database name, must match DSN
- `WRSTAT_POLL_INTERVAL` (optional): duration string for mount update polling,
  eg `1m`. Default `1m` for server, disabled for summarise.
- `WRSTAT_QUERY_TIMEOUT` (optional): per-query timeout, default `30s`.

CLI flags (must override env vars if specified):

- `--clickhouse-dsn` / `-C`
- `--clickhouse-database` / `-D`
- `--poll-interval` (server only)
- `--query-timeout`

Existing flags like `--owners`, `--mounts` remain unchanged and populate
`cfg.OwnersCSVPath` and `cfg.MountPoints`.

Tests must not require `.env` files; they construct `Config` directly.

----------------------------------------------------------------------

## Data lifecycle model (snapshots + atomic swap)

ClickHouse stores multiple snapshots per mountpoint. Reads always use the
*active snapshot* per mountpoint.

Terms:

- `mount_path`: the decoded mount directory path (absolute, ends with `/`).
  This is the canonical identifier used inside ClickHouse tables and queries.
- `mount_key`: the external mount identifier used by the existing
  `/rest/v1/auth/dbsUpdated` endpoint.
  - It corresponds to the `<mountKey>` suffix of dataset output directories
    named `<version>_<mountKey>`.
  - It is typically derived from `mount_path` by replacing every `/` with `／`
    (U+FF0F FULLWIDTH SOLIDUS).
  - ClickHouse implementations SHOULD compute `mount_key` from `mount_path` at
    the API boundary rather than storing it in tables.
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
- Target batch sizes of 100,000–500,000 rows for optimal ingest throughput.
  - With 1.3 billion rows and a 1-hour target, we need ~360,000 rows/second.
  - Larger batches reduce per-batch overhead and part creation frequency.
- All read queries must restrict results to the active snapshot(s) using
  `wrstat_mounts_active`.
  - For single-mount queries, prefer resolving the active snapshot into a
    constant and filtering by it in `PREWHERE`:
    `WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid`
  - For multi-mount queries (ancestor-scope), join to a filtered set of active
    snapshots (often written as `WITH active AS (...)` then `ANY INNER JOIN active`).
  - Always place partition keys early (ideally in `PREWHERE`) when possible.
- For `wrstat_files` (the largest table), the implementation MUST use columnar
  inserts (`batch.Column(i).Append(slice)`) to avoid the reflection overhead of
  row-based inserts. This is critical to meeting the 1-hour ingest requirement.

Ingest throughput target (normative):

- Given ~1.3 billion file/dir entries across ~7 main mountpoints, the pipeline
  must sustain at least 400,000 rows/second aggregate write throughput.
- The bottleneck tables are `wrstat_dguta`, `wrstat_children`, and
  `wrstat_files`.
- Recommended batch size for all tables: 100,000 rows minimum.
- If using parallel ingest per mount, limit concurrent batch flushes to avoid
  "too many parts" errors; use at most 3–4 concurrent writers per table.

Driver guidance (normative):

- Use clickhouse-go v2's native API (not `database/sql`) for performance.
- Enable native protocol compression via DSN (`compress=lz4`).
  - LZ4 is required for the main tables (`wrstat_files`, `wrstat_dguta`,
    `wrstat_children`) to maximize ingest throughput.
- Do not share a prepared batch between goroutines; always `Close()` batches
  to avoid leaking connections.
- Connection pool settings for high-throughput ingest:
  - `MaxOpenConns`: at least equal to the number of concurrent batch writers.
  - `MaxIdleConns`: same as MaxOpenConns to avoid connection churn.
  - `ConnMaxLifetime`: 30 minutes minimum to avoid reconnection during ingest.
- For batch inserts, call `batch.Send()` rather than relying on auto-flush;
  this ensures deterministic part creation timing.

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

Schema versioning (normative):

- The expected schema version is `1`.
- `wrstat_schema_version` must contain exactly one row.
- On startup, the clickhouse package must:
  - `SELECT count(), min(version), max(version)` from `wrstat_schema_version`.
  - If count is 0, `INSERT INTO wrstat_schema_version (version) VALUES (1)`.
  - If count is 1 and version is 1, continue.
  - Otherwise, return an error instructing the operator to migrate or drop the
    database.

Embed all DDL as `.sql` files in the clickhouse package using `//go:embed`.
Do not build SQL dynamically.

### Complete embedded DDL (normative)

The following CREATE statements must be embedded in the clickhouse package.
Each statement should be in a separate `.sql` file under `clickhouse/schema/`.

```sql
-- schema/001_schema_version.sql
CREATE TABLE IF NOT EXISTS wrstat_schema_version (
  version UInt32
) ENGINE = TinyLog;

-- schema/002_mounts.sql
CREATE TABLE IF NOT EXISTS wrstat_mounts (
  mount_path LowCardinality(String) CODEC(ZSTD(3)),
  switched_at DateTime64(3) CODEC(Delta, ZSTD(3)),
  active_snapshot UUID,
  updated_at DateTime CODEC(Delta, ZSTD(3))
) ENGINE = ReplacingMergeTree(switched_at)
ORDER BY mount_path;

-- schema/003_mounts_active.sql
CREATE VIEW IF NOT EXISTS wrstat_mounts_active AS
SELECT
  mount_path,
  argMax(active_snapshot, switched_at) AS snapshot_id,
  argMax(updated_at, switched_at) AS updated_at
FROM wrstat_mounts
GROUP BY mount_path;

-- schema/004_dguta.sql
CREATE TABLE IF NOT EXISTS wrstat_dguta (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir String CODEC(LZ4),
  gid UInt32,
  uid UInt32,
  -- ft is a bitmask (db.DirGUTAFileType). Multiple bits may be set (e.g.
  -- temp|bam). Readers must treat it as a set of flags.
  ft UInt16,
  age UInt8,
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  -- Per-age-bucket counts used to compute DirSummary.CommonATime/CommonMTime.
  -- Each array MUST have length 9 and the bucket index mapping MUST match
  -- summary.AgeRange (0..8).
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4)
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir, age, gid, uid, ft)
SETTINGS index_granularity = 8192;

-- schema/005_children.sql
CREATE TABLE IF NOT EXISTS wrstat_children (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
  child String CODEC(LZ4)
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, parent_dir, child)
SETTINGS index_granularity = 8192;

-- schema/006_basedirs_group_usage.sql
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

-- schema/007_basedirs_user_usage.sql
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

-- schema/008_basedirs_group_subdirs.sql
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

-- schema/009_basedirs_user_subdirs.sql
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

-- schema/010_basedirs_history.sql
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

-- schema/011_files.sql
CREATE TABLE IF NOT EXISTS wrstat_files (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
  name String CODEC(LZ4),
  -- path is derived from (parent_dir, name) so we don't store it twice.
  -- This keeps directory lookups fast (via ORDER BY) while avoiding
  -- redundant storage at ~1.3B rows.
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
  INDEX ext_idx ext TYPE set(256) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, parent_dir, name)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
```

### Schema field reference (for prose)

The following sections describe the schema tables for reference. The definitive
DDL is in the "Complete embedded DDL" section above.

Conventions:

- `child` in `wrstat_children` is the full child path, without a trailing `/`
  (matching the current Bolt behaviour).
- User usage does not have `date_no_space` or `date_no_files` columns because
  quota projections are only computed for groups.
- `pos` in subdir tables preserves the slice ordering passed to `PutSubDirs()`.
- `path` in `wrstat_files` is derived as `parent_dir + name` and must match the
  exact stats.gz representation (directories end with `/`). `parent_dir` ends
  with `/`. `name` for directories includes trailing `/`. `ext` is derived from
  the filename: for files, it is the part after the last `.` in the name
  (lowercased), or empty if there is no `.` or the name starts with `.` and has
  no other `.`. For directories, `ext` is always empty.

History notes:

- History is append-only with a strict-newer rule (see Store semantics).
- History is partitioned by `mount_path` only (not snapshot_id) because it
  persists across snapshots.

## Query optimization requirements (normative)

All read queries must follow these patterns for millisecond performance:

### Partition pruning

Every query must ensure ClickHouse can prune to the active snapshot partition:

1. **Resolve the active snapshot first** - For single-mount queries, resolve
  the active `snapshot_id` into a constant using a scalar subquery:

  `WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid`

  This avoids relying on join-dependent predicate pushdown.

2. **Use partition keys in PREWHERE** - When querying within a single mount,
  include both `mount_path` and `snapshot_id = sid` in PREWHERE.

  This is important during the brief window after a snapshot switch and before
  old partitions are dropped: without `snapshot_id` in PREWHERE, ClickHouse can
  scan both the new and old partitions for that mount.

3. **Subquery pattern for ancestor queries** - When querying across mounts
   (e.g., browsing `/`), use a subquery on `wrstat_mounts_active` with
   `startsWith(mount_path, ?)` to limit the join to relevant mounts.

### ORDER BY prefix matching

Queries that filter on directory paths benefit from ORDER BY prefix matching:

- `wrstat_dguta` is ordered by `(mount_path, snapshot_id, dir, ...)`.
- `wrstat_children` is ordered by `(mount_path, snapshot_id, parent_dir, ...)`.
- `wrstat_files` is ordered by `(mount_path, snapshot_id, parent_dir, name)`.

Filtering on the leading columns (e.g., `dir = ?` or `parent_dir = ?`) enables
ClickHouse to seek directly to the relevant rows without scanning.

### Projection usage

The `wrstat_files` table is ordered by `(mount_path, snapshot_id, parent_dir, name)`.
Queries that filter by exact `path = ?` (like `StatPath`) must split the path
into `parent_dir` and `name` in the client to use the primary key index.

- `parent_dir` is the path up to and including the last `/`.
- `name` is the remainder.
- Example: `/a/b/` -> parent=`/a/`, name=`b/`.
- Example: `/a/b` -> parent=`/a/`, name=`b`.

This avoids the need for a secondary projection, saving write overhead.

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

Update swap semantics (normative):

- When a change is detected, the provider must build new reader instances,
  publish them (so subsequent `Tree()`/`BaseDirs()` calls use the new data),
  then invoke the callback.
- Any old reader instances must remain usable until the callback returns.
- After the callback returns, the provider must close the old readers.

The provider must not expose any ClickHouse concepts. It returns:

- `Tree() *db.Tree` (constructed as `db.NewTree(dbImpl)` after the refactor)
- `BaseDirs() basedirs.Reader`

### `db.Database` (read-side tree)

The clickhouse implementation must:

- Preserve the current multi-mount semantics:
  - For directories that are "above" mountpoints (eg `/` or `/lustre/`), merge
    results across all mounts that are under that directory.
  - For directories that are within a single mount, query only that mount.
- Return `ErrDirNotFound` only when the directory is absent from *all* active
  snapshots.
- Return `nil, nil` (no error) when the directory exists but the filter removes
  all results.
- Set `DirSummary.Modtime` to the maximum `updated_at` across contributing
  mounts for the returned summary.

Normalization:

- When querying by directory, normalize `dir` to have a trailing `/`.

Mount scoping (normative):

Given a normalized `dir`:

1. If `dir` is within a mountpoint, use that single mountpoint:
   - Resolve `mount_path` by longest-prefix match of `dir` against the
     configured/auto-discovered mountpoint list (same algorithm as
     basedirs mount resolution).
   - Query only that `mount_path`.
2. Otherwise, `dir` is treated as an ancestor directory:
   - Query all mounts where `startsWith(mount_path, dir)`.

This is required for performance: it ensures that almost all UI queries (which
are typically within one mount) prune to a single `(mount_path, snapshot_id)`
partition, while still allowing browsing from `/`.

Queries:

Existence check (unfiltered):

- If `dir` is within a mountpoint (single-mount scope):

  - ```sql
    WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
    SELECT 1
    FROM wrstat_dguta d
    PREWHERE d.mount_path = ?
      AND d.snapshot_id = sid
      AND d.dir = ?
    LIMIT 1
    ```

  Parameter order:

  1. mount_path (for snapshot lookup)
  2. mount_path
  3. dir

- Otherwise (ancestor scope):

  - ```sql
    WITH active AS (
      SELECT mount_path, snapshot_id
      FROM wrstat_mounts_active
      WHERE startsWith(mount_path, ?)
    )
    SELECT 1
    FROM wrstat_dguta d
    ANY INNER JOIN active a ON d.mount_path = a.mount_path AND d.snapshot_id = a.snapshot_id
    WHERE d.dir = ?
    LIMIT 1
    ```

  Parameter order:

  1. dir (as ancestor prefix)
  2. dir

- Summary query (filtered):

  - Apply filters only if they are non-empty / non-zero, matching current Go
    semantics:

    - GIDs: apply only when `filter.GIDs != nil` (membership check against an
      Array parameter; see the `has(?, d.gid)` pattern in the SQL below)
    - UIDs: apply only when `filter.UIDs != nil` (membership check against an
      Array parameter; see the `has(?, d.uid)` pattern in the SQL below)
    - Age:  always apply `d.age = {filter.Age}` (callers set this)
    - FT:   apply only when `filter.FT != 0`:
      `bitAnd(d.ft, {filter.FT}) != 0`

  - Aggregate:

    - `Count` = `sum(d.count)`
    - `Size`  = `sum(d.size)`
    - `Atime` = `toDateTime(min(d.atime_min))`
    - `Mtime` = `toDateTime(max(d.mtime_max))`
    - `CommonATime` and `CommonMTime` must match the Go behaviour of
      `summary.MostCommonBucket` on the *summed* bucket arrays across all rows
      included by the query.
      - Tie-breaking is required: if multiple buckets share the maximum count,
        choose the highest index (newest bucket).
      - Suggested SQL pattern (normative output semantics; implementation may
        compute in Go instead of SQL if it yields the same result):
        - `atime_ranges := sumForEach(d.atime_buckets)`
        - `common_atime := 8 - (arrayMaxIndex(arrayReverse(atime_ranges)) - 1)`
        - same for mtime.
    - `UIDs`  = `arraySort(groupUniqArray(d.uid))`
    - `GIDs`  = `arraySort(groupUniqArray(d.gid))`
    - `FT`    = `bitOr(d.ft)`
    - `Age`   = `filter.Age`
    - `Modtime` = `max(a.updated_at)`

Summary query SQL (normative):

- If `dir` is within a mountpoint (single-mount scope):

  - ```sql
    WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
    SELECT
      sum(d.count) AS count,
      sum(d.size) AS size,
      toDateTime(min(d.atime_min)) AS atime,
      toUInt8(8 - (arrayMaxIndex(arrayReverse(sumForEach(d.atime_buckets))) - 1)) AS common_atime,
      toDateTime(max(d.mtime_max)) AS mtime,
      toUInt8(8 - (arrayMaxIndex(arrayReverse(sumForEach(d.mtime_buckets))) - 1)) AS common_mtime,
      arraySort(groupUniqArray(d.uid)) AS uids,
      arraySort(groupUniqArray(d.gid)) AS gids,
      bitOr(d.ft) AS ft,
      (SELECT updated_at FROM wrstat_mounts_active WHERE mount_path = ?) AS modtime
    FROM wrstat_dguta d
    PREWHERE d.mount_path = ?
      AND d.snapshot_id = sid
      AND d.dir = ?
    WHERE d.age = ?
      AND (? = 0 OR bitAnd(d.ft, ?) != 0)
      AND (? = 0 OR has(?, d.gid))
      AND (? = 0 OR has(?, d.uid))
    ```

  Parameter order:

  1. mount_path (for snapshot lookup)
  2. mount_path (for updated_at lookup)
  3. mount_path
  4. dir
  5. age
  6. ft_enabled (0/1)
  7. ft_mask
  8. gids_enabled (0/1)
  9. gids Array(UInt32)
  10. uids_enabled (0/1)
  11. uids Array(UInt32)

- Otherwise (ancestor scope):

  - ```sql
    WITH active AS (
      SELECT mount_path, snapshot_id, updated_at
      FROM wrstat_mounts_active
      WHERE startsWith(mount_path, ?)
    )
    SELECT
      sum(d.count) AS count,
      sum(d.size) AS size,
      toDateTime(min(d.atime_min)) AS atime,
      toUInt8(8 - (arrayMaxIndex(arrayReverse(sumForEach(d.atime_buckets))) - 1)) AS common_atime,
      toDateTime(max(d.mtime_max)) AS mtime,
      toUInt8(8 - (arrayMaxIndex(arrayReverse(sumForEach(d.mtime_buckets))) - 1)) AS common_mtime,
      arraySort(groupUniqArray(d.uid)) AS uids,
      arraySort(groupUniqArray(d.gid)) AS gids,
      bitOr(d.ft) AS ft,
      max(a.updated_at) AS modtime
    FROM wrstat_dguta d
    ANY INNER JOIN active a ON d.mount_path = a.mount_path AND d.snapshot_id = a.snapshot_id
    WHERE d.dir = ?
      AND d.age = ?
      AND (? = 0 OR bitAnd(d.ft, ?) != 0)
      AND (? = 0 OR has(?, d.gid))
      AND (? = 0 OR has(?, d.uid))
    ```

  Parameter order:

  1. dir (as ancestor prefix)
  2. dir
  3. age
  4. ft_enabled (0/1)
  5. ft_mask
  6. gids_enabled (0/1)
  7. gids Array(UInt32)
  8. uids_enabled (0/1)
  9. uids Array(UInt32)

Filter binding (normative):

- `ft_enabled` is 0 if `filter.FT == 0`, else 1.
- `gids_enabled` is 0 if `filter.GIDs == nil`, else 1.
- `uids_enabled` is 0 if `filter.UIDs == nil`, else 1.

### `db.Database.Children(dir string) ([]string, error)`

Behaviour must match current Bolt behaviour:

- Returns a de-duplicated, sorted list across mounts.
- Returns `nil`/empty when there are no children or the dir is missing.

Normalization:

- Normalize `dir` to end with `/` before matching `parent_dir`.

Query:

- If `dir` is within a mountpoint (single-mount scope):

  - ```sql
    WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
    SELECT DISTINCT c.child
    FROM wrstat_children c
    PREWHERE c.mount_path = ?
      AND c.snapshot_id = sid
      AND c.parent_dir = ?
    ORDER BY c.child ASC
    ```

  Parameter order:

  1. mount_path (for snapshot lookup)
  2. mount_path
  3. parent_dir

- Otherwise (ancestor scope):

  - ```sql
    WITH active AS (
      SELECT mount_path, snapshot_id
      FROM wrstat_mounts_active
      WHERE startsWith(mount_path, ?)
    )
    SELECT DISTINCT c.child
    FROM wrstat_children c
    ANY INNER JOIN active a ON c.mount_path = a.mount_path AND c.snapshot_id = a.snapshot_id
    WHERE c.parent_dir = ?
    ORDER BY c.child ASC
    ```

  Parameter order:

  1. dir (as ancestor prefix)
  2. parent_dir

### `db.Database.Info()`

Return values must match the existing `db.DBInfo` meaning:

- `NumDirs`: number of directory keys present
- `NumDGUTAs`: number of dguta rows (dir+gid+uid+ft+age combos)
- `NumParents`: number of distinct parent_dir entries in children
- `NumChildren`: number of child edges

Compute over active snapshots only.

Info SQL statements (normative)

DGUTA counts:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  countDistinct(d.dir) AS num_dirs,
  count() AS num_dgutas
FROM wrstat_dguta d
ANY INNER JOIN active a
  ON d.mount_path = a.mount_path AND d.snapshot_id = a.snapshot_id
```

Children counts:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  countDistinct(c.parent_dir) AS num_parents,
  count() AS num_children
FROM wrstat_children c
ANY INNER JOIN active a
  ON c.mount_path = a.mount_path AND c.snapshot_id = a.snapshot_id
```

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
  - Default batch size should be 100,000 rows for optimal throughput.
  - A `SetBatchSize` value of `10_000` must work without OOM (for testing).
  - For production ingest, batch sizes of 100,000–500,000 are recommended.
- Close:
  - Flush all batches.
  - Read the *previous* active snapshot id for this mount (if any) from
    `wrstat_mounts_active`.
  - Switch the active snapshot by inserting into `wrstat_mounts`.
  - Drop the *previous* snapshot partitions for this mount in `wrstat_dguta`,
    `wrstat_children`, and all other snapshot tables (basedirs usage/subdirs
    and files).
    - This drop must use the snapshot id read *before* the switch.
    - If there was no previous snapshot, skip the old-partition drop.
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

Write-side SQL statements (normative)

Partition drop syntax:

- All tables that use `PARTITION BY (mount_path, snapshot_id)` must drop
  partitions using:

  ```sql
  ALTER TABLE <table>
  DROP PARTITION tuple(?, toUUID(?))
  ```

  Parameter order:
  1. mount_path (String)
  2. snapshot_id (UUID string)

Active snapshot read:

This query is used to find the previous snapshot id and updated_at for a mount.
It MUST be executed before inserting the new `wrstat_mounts` row.

```sql
SELECT a.snapshot_id, a.updated_at
FROM wrstat_mounts_active a
WHERE a.mount_path = ?
```

Switch active snapshot (must be executed only once per run, in
`DGUTAWriter.Close()`):

```sql
INSERT INTO wrstat_mounts (mount_path, switched_at, active_snapshot, updated_at)
VALUES (?, now64(3), toUUID(?), ?)
```

Insert DGUTA rows (batch):

```sql
INSERT INTO wrstat_dguta
  (mount_path, snapshot_id, dir, gid, uid, ft, age, count, size,
  atime_min, mtime_max, atime_buckets, mtime_buckets)
VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

Insert children rows (batch):

```sql
INSERT INTO wrstat_children
  (mount_path, snapshot_id, parent_dir, child)
VALUES (?, toUUID(?), ?, ?)
```

----------------------------------------------------------------------

## Basedirs store + reader

### `basedirs.Store`

The store writes one mount's basedirs snapshot.

Mapping:

- `SetMountPath(mountPath)` sets `mount_path` for all subsequent writes.
- `SetUpdatedAt(updatedAt)` is stored in `wrstat_mounts` on snapshot switch.
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

    ```sql
    SELECT max(date)
    FROM wrstat_basedirs_history
    WHERE mount_path = ?
      AND gid = ?
    ```

  - If max(date) is NULL or < point.Date, insert the new row.

Basedirs SQL statements (normative)

Insert group usage (batch):

```sql
INSERT INTO wrstat_basedirs_group_usage
  (mount_path, snapshot_id, gid, basedir, age, uids, usage_size, quota_size,
   usage_inodes, quota_inodes, mtime, date_no_space, date_no_files)
VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

Insert user usage (batch):

```sql
INSERT INTO wrstat_basedirs_user_usage
  (mount_path, snapshot_id, uid, basedir, age, gids, usage_size, quota_size,
   usage_inodes, quota_inodes, mtime)
VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

Insert group subdir rows (batch):

```sql
INSERT INTO wrstat_basedirs_group_subdirs
  (mount_path, snapshot_id, gid, basedir, age, pos, subdir, num_files,
   size_files, last_modified, file_usage)
VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

Insert user subdir rows (batch):

```sql
INSERT INTO wrstat_basedirs_user_subdirs
  (mount_path, snapshot_id, uid, basedir, age, pos, subdir, num_files,
   size_files, last_modified, file_usage)
VALUES (?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

History last-point lookup:

```sql
SELECT max(date)
FROM wrstat_basedirs_history
WHERE mount_path = ?
  AND gid = ?
```

History append:

```sql
INSERT INTO wrstat_basedirs_history
  (mount_path, gid, date, usage_size, quota_size, usage_inodes, quota_inodes)
VALUES (?, ?, ?, ?, ?, ?, ?)
```

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

Required methods from `basedirs.Reader` (normative):

- `SetMountPoints(mountpoints []string)` must override mount auto-discovery and
  must affect mount resolution for `History(gid, path)` in the reader.

Mount resolution for the extra-goal `Client` APIs is configured independently:

- `Client` mount resolution must use `cfg.MountPoints` when non-empty;
  otherwise it uses the same mount auto-discovery logic as the reader.
- There is no requirement that calling `basedirs.Reader.SetMountPoints()`
  mutates already-constructed `Client` instances.
- `SetCachedGroup(gid, name)` and `SetCachedUser(uid, name)` must populate the
  same caches used for filling `Usage.Name` so tests that pre-seed names remain
  stable. These must not be no-ops.

Reader SQL statements (normative)

Group usage:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  gid,
  basedir,
  uids,
  usage_size,
  quota_size,
  usage_inodes,
  quota_inodes,
  mtime,
  date_no_space,
  date_no_files,
  age
FROM wrstat_basedirs_group_usage u
ANY INNER JOIN active a
  ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
ORDER BY gid ASC, basedir ASC
```

User usage:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  uid,
  basedir,
  gids,
  usage_size,
  quota_size,
  usage_inodes,
  quota_inodes,
  mtime,
  age
FROM wrstat_basedirs_user_usage u
ANY INNER JOIN active a
  ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
ORDER BY uid ASC, basedir ASC
```

Group subdirs:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  subdir,
  num_files,
  size_files,
  last_modified,
  file_usage
FROM wrstat_basedirs_group_subdirs s
ANY INNER JOIN active a
  ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.gid = ?
  AND s.basedir = ?
  AND s.age = ?
ORDER BY s.pos ASC
```

User subdirs:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  subdir,
  num_files,
  size_files,
  last_modified,
  file_usage
FROM wrstat_basedirs_user_subdirs s
ANY INNER JOIN active a
  ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.uid = ?
  AND s.basedir = ?
  AND s.age = ?
ORDER BY s.pos ASC
```

History:

- Resolve mount_path for the input `path` using the same longest-prefix mount
  resolution as current basedirs.


```sql
SELECT
  date,
  usage_size,
  quota_size,
  usage_inodes,
  quota_inodes
FROM wrstat_basedirs_history
WHERE mount_path = ?
  AND gid = ?
ORDER BY date ASC
```

Mount timestamps:

```sql
SELECT mount_path, updated_at
FROM wrstat_mounts_active
```

Info SQL statements (normative)

For the following, `age_all` is the numeric value of `db.DGUTAgeAll`.

Group usage combos:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT count()
FROM wrstat_basedirs_group_usage u
ANY INNER JOIN active a
  ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
```

User usage combos:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT count()
FROM wrstat_basedirs_user_usage u
ANY INNER JOIN active a
  ON u.mount_path = a.mount_path AND u.snapshot_id = a.snapshot_id
WHERE u.age = ?
```

Group history series + points:

```sql
SELECT
  countDistinct((mount_path, gid)) AS group_mount_combos,
  count() AS group_histories
FROM wrstat_basedirs_history
```

Group subdir combos + subdir rows:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  countDistinct((gid, basedir)) AS group_subdir_combos,
  count() AS group_subdirs
FROM wrstat_basedirs_group_subdirs s
ANY INNER JOIN active a
  ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.age = ?
```

User subdir combos + subdir rows:

```sql
WITH active AS (
  SELECT mount_path, snapshot_id
  FROM wrstat_mounts_active
)
SELECT
  countDistinct((uid, basedir)) AS user_subdir_combos,
  count() AS user_subdirs
FROM wrstat_basedirs_user_subdirs s
ANY INNER JOIN active a
  ON s.mount_path = a.mount_path AND s.snapshot_id = a.snapshot_id
WHERE s.age = ?
```

### `basedirs.HistoryMaintainer`

`CleanHistoryForMount(prefix)`:

- Delete all history rows whose `mount_path` does not start with `prefix`.
- This is a safety tool; it must run only on the configured database.

`FindInvalidHistory(prefix)`:

- Return distinct `(gid, mount_path)` pairs that would be deleted by
  `CleanHistoryForMount(prefix)`.

History maintenance SQL statements (normative)

`FindInvalidHistory(prefix)`:

```sql
SELECT DISTINCT
  gid,
  mount_path
FROM wrstat_basedirs_history
WHERE NOT startsWith(mount_path, ?)
ORDER BY mount_path ASC, gid ASC
```

`CleanHistoryForMount(prefix)` must run a synchronous mutation:

```sql
ALTER TABLE wrstat_basedirs_history
DELETE WHERE NOT startsWith(mount_path, ?)
SETTINGS mutations_sync = 2
```

----------------------------------------------------------------------

## Extra-goal file APIs

To support other apps, the clickhouse package must also store file-level rows
from stats.gz and expose query helpers. The `wrstat_files` table DDL is defined
in the "Complete embedded DDL" section above.

### Ingestion changes

`cmd/summarise` must register an additional *global* summariser operation when
using ClickHouse:

- `NewFileIngestOperation(cfg, mountPath, updatedAt) (summary.OperationGenerator,
  io.Closer, error)`

This operation streams every file and directory from stats.gz into
`wrstat_files` for the same `(mount_path, snapshot_id)` as the DGUTA writer.

Before inserting any rows, it must drop the `wrstat_files` partition for
`(mount_path, snapshot_id)` to make reruns safe.

`snapshot_id` derivation is defined in the lifecycle section above. All
ClickHouse writers for a mount must use that same derived id.

Close order in `cmd/summarise` (normative):

After `Summariser.Summarise()` returns, close resources in this order:
1. Close the file ingest operation (flushes file batches)
2. Close the basedirs store (flushes basedirs batches)
3. Close the DGUTA writer (flushes DGUTA batches, switches active snapshot,
   drops old partitions)

This ensures all data is written before the snapshot switch makes it visible.

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

Mount resolution (normative):

- For any API taking an absolute `path`/`dir`, first resolve the mount by
  longest-prefix match against the reader's mountpoint list (either the
  configured override or auto-discovered, identical to current basedirs
  mount resolution).
- The resolved mount path is the `mount_path` used in SQL. If no mount matches,
  return the existing domain error `basedirs.ErrInvalidBasePath`.

Active snapshot resolution (required):

All file queries must restrict results to the active snapshot for the resolved
mount.

For single-mount queries, implementations MUST resolve the active snapshot id
into a constant using a scalar subquery and then filter by it:

  `WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid`

This avoids accidental scans of multiple snapshot partitions.

SQL parameter style (required):

- Use positional parameters (`?`) with clickhouse-go v2.
- For `gids []uint32`, pass as `Array(UInt32)`.

Semantics:

- All methods operate over the active snapshot for the mount containing the
  queried path, using `wrstat_mounts_active`.
- Permission checks are ownership-based (uid/gid matching), because stats.gz
  does not contain POSIX mode bits.

### Option types (exported)

```go
// FileRow represents a file or directory from wrstat_files.
type FileRow struct {
  Path         string
  ParentDir    string
  Name         string
  Ext          string
  EntryType    byte      // stats.FileType, stats.DirType, etc.
  Size         int64
  ApparentSize int64
  UID          uint32
  GID          uint32
  ATime        time.Time
  MTime        time.Time
  CTime        time.Time
  Inode        int64
  Nlink        int64
}

// ListOptions controls ListDir behaviour.
type ListOptions struct {
  // Fields to retrieve; empty means all fields.
  Fields []string
  // Limit must be > 0.
  // If 0, implementations MUST substitute a large internal cap (e.g. 1_000_000)
  // to avoid accidental unbounded reads.
  Limit  int64
  Offset int64
}

// StatOptions controls StatPath behaviour.
type StatOptions struct {
  // Fields to retrieve; empty means all fields.
  Fields []string
}

// FindOptions controls FindByGlob behaviour.
type FindOptions struct {
  // Fields to retrieve; empty means all fields.
  Fields       []string
  // Limit must be > 0.
  // If 0, implementations MUST substitute a large internal cap (e.g. 1_000_000)
  // to avoid accidental unbounded reads.
  Limit        int64
  Offset       int64
  // RequireOwner filters results to files where uid matches UID or gid matches
  // one of GIDs.
  RequireOwner bool
  UID          uint32
  GIDs         []uint32
}
```

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

### SQL statements (normative)

The following statements must be used. The SQL text and semantics in this
section must not change.

`ListDir(ctx, dir, opts)`

Normalize `dir` to end with `/`.

Required SQL (columns list depends on opts, but WHERE/ORDER/LIMIT must match):

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT
  f.path,
  f.parent_dir,
  f.name,
  f.ext,
  f.entry_type,
  f.size,
  f.apparent_size,
  f.uid,
  f.gid,
  f.atime,
  f.mtime,
  f.ctime,
  f.inode,
  f.nlink
FROM wrstat_files f
PREWHERE f.mount_path = ?
  AND f.snapshot_id = sid
  AND f.parent_dir = ?
ORDER BY f.name ASC
LIMIT ? OFFSET ?
```

Parameter order:

1. mount_path (for snapshot lookup)
2. mount_path
3. dir (as parent_dir)
4. limit
5. offset

`StatPath(ctx, path, opts)`

Required SQL:

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT
  f.path,
  f.parent_dir,
  f.name,
  f.ext,
  f.entry_type,
  f.size,
  f.apparent_size,
  f.uid,
  f.gid,
  f.atime,
  f.mtime,
  f.ctime,
  f.inode,
  f.nlink
FROM wrstat_files f
PREWHERE f.mount_path = ?
  AND f.snapshot_id = sid
  AND f.parent_dir = ?
  AND f.name = ?
LIMIT 1
```

Parameter order:

1. mount_path (for snapshot lookup)
2. mount_path
3. parent_dir (derived from path)
4. name (derived from path)

`IsDir(ctx, path)`

Normalize `path` using the same parent/name split as `StatPath` so the query
hits the `(parent_dir, name)` prefix of the primary key instead of scanning a
full mount partition.

Required SQL:

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT f.entry_type
FROM wrstat_files f
PREWHERE f.mount_path = ?
  AND f.snapshot_id = sid
  AND f.parent_dir = ?
  AND f.name = ?
LIMIT 1
```

Return true iff `entry_type == stats.DirType`.

`FindByGlob(ctx, baseDirs, patterns, opts)`

Pattern translation (required):

- Translate gitignore-style patterns into RE2 regex and query using `match`.
- `**` => `.*`
- `*`  => `[^/]*`
- `?`  => `[^/]`
- Escape all other regex metacharacters.
- Anchor to the provided base dir by prefixing the regex with
  `^<escaped_base_dir>`.

Mount grouping rule (clarification):

- `baseDirs` may contain directories on different mounts.
- Resolve the mount for each base dir using the mount resolution rule.
- Group the `(baseDir, pattern)` work by `mount_path` and execute one SQL query
  per mount group.
- In the common case where all baseDirs are on one mount (eg you pass a single
  directory), this means exactly one query.

Required SQL (normative):

- Note: The `match(f.path, ?)` OR-list must contain exactly one placeholder per
  compiled regex.
- If `patterns` is empty, return an empty result without querying.
- If `patterns` is longer than 32, split into multiple queries (per mount
  group) and concatenate results, preserving overall `ORDER BY f.path ASC`.

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT
  f.path,
  f.parent_dir,
  f.name,
  f.ext,
  f.entry_type,
  f.size,
  f.apparent_size,
  f.uid,
  f.gid,
  f.atime,
  f.mtime,
  f.ctime,
  f.inode,
  f.nlink
FROM wrstat_files f
PREWHERE f.mount_path = ?
  AND f.snapshot_id = sid
WHERE f.parent_dir >= ?
  AND f.parent_dir < ?
  AND (
    match(f.path, ?) OR match(f.path, ?)
  )
  AND (? = 0 OR f.uid = ? OR has(?, f.gid))
ORDER BY f.parent_dir ASC, f.name ASC
LIMIT ? OFFSET ?
```

Parameter notes:

- The parent_dir range parameters are:
  - lower bound: base directory path (normalized, ends with `/`)
  - upper bound: the smallest string strictly greater than all strings with that
    prefix (compute in Go; sometimes called `prefixNext(baseDir)`).
- The `match` parameters are the precomputed regex strings.
- The permission clause is required:
  - if `opts.RequireOwner` is false, pass `0` for the first `?` and still pass
    placeholder values for the remaining parameters
  - if true, pass `1`, then `uid`, then `gids` as Array(UInt32)

`PermissionAnyInDir(ctx, dir, uid, gids)`

Normalize `dir` to end with `/`.

Required SQL:

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT 1
FROM wrstat_dguta d
PREWHERE d.mount_path = ?
  AND d.snapshot_id = sid
  AND d.dir = ?
  AND d.age = ?
  AND (d.uid = ? OR has(?, d.gid))
LIMIT 1
```

Parameter order:

1. mount_path (for snapshot lookup)
2. mount_path
3. dir
4. age_all (numeric value of db.DGUTAgeAll)
5. uid
6. gids

----------------------------------------------------------------------

## Testing (clickhouse package)

All public methods of the clickhouse package must be tested.

Tests must run against a real ClickHouse server started automatically.

Local development must not require root.

### Local test runner (required)

By default, clickhouse package tests must start a local ClickHouse server using
the `clickhouse` binary available in `PATH`.

Skip behaviour (normative):

- If `clickhouse` is not in PATH, tests must call `t.Skip("clickhouse binary
  not found in PATH")`.
- If the environment variable `WRSTAT_CLICKHOUSE_DSN` is set, tests must use
  that DSN instead of starting a local server (this allows CI to provide a
  pre-started server).

Required behaviour when starting a local server:

- Start the server with a generated config under `t.TempDir()` so it is fully
  isolated (data path, logs, tmp, etc).
- Bind only to `127.0.0.1`.
- Pick a free TCP port at runtime and write it into the generated config.
- Wait until the server answers `SELECT 1` before running tests (max 30s
  timeout, then fail).
- Stop the server and clean up the temp dir when the test completes.

Required database name convention:

- Create a unique database per test run:
  `wrstat_ui_test_${USER}_${PID}_${RAND}`.

Hard safety checks (normative):

- Tests must refuse to connect to any DSN that does not resolve to localhost.
- When `WRSTAT_ENV=test`, the clickhouse package must refuse to execute:
  - `DROP DATABASE`
  - any deletion of history across mounts (eg `CleanHistoryForMount`)
  unless `cfg.Database` starts with `wrstat_ui_test_`.

Snapshot partition drops are part of normal operation and are permitted in all
environments.

### CI runner (normative)

In GitHub Actions, run tests against a ClickHouse service container. Add a
ClickHouse service to the workflow and set `WRSTAT_CLICKHOUSE_DSN` to point to
it. Tests must use this DSN instead of starting a local server.

Example workflow snippet:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.11.2.24
    ports:
      - 9000:9000
    options: >-
      --health-cmd "clickhouse-client --query 'SELECT 1'"
      --health-interval 5s
      --health-timeout 2s
      --health-retries 10
env:
  WRSTAT_CLICKHOUSE_DSN: clickhouse://default@localhost:9000/default
```

CI must not skip any tests; all tests must run.

### README update requirement

The implementation agent must update `README.md` with:

- how to install a local ClickHouse binary suitable for running tests
- how to run a ClickHouse server on a custom port (so multiple developers can
  run test servers on the same machine)
- which env vars/flags control the test server port/DSN

----------------------------------------------------------------------

## Performance hooks

To enable iterative schema/perf tuning, add a CLI command that can:

1. import one or more stats.gz files into ClickHouse (optionally only the first N lines)
2. run a fixed suite of read queries and report latencies

Command name (normative):

- `wrstat-ui clickhouse-perf`

This command lives under `cmd/` and may import the `clickhouse` package.

Instrumentation constraint (normative):

- `cmd/clickhouse-perf` must not import clickhouse-go.
- To support plan/IO verification, the clickhouse package must provide the
  `Inspector` API defined below.

### `clickhouse.Inspector` (normative)

The clickhouse package must export:

```go
// QueryMetrics are pulled from ClickHouse query logs.
type QueryMetrics struct {
  DurationMs  uint64
  ReadRows    uint64
  ReadBytes   uint64
  ResultRows  uint64
  ResultBytes uint64
}

// Inspector can run EXPLAIN and query system.query_log without exposing
// clickhouse-go types.
type Inspector struct{ /* unexported */ }

func NewInspector(cfg Config) (*Inspector, error)

// ExplainListDir returns EXPLAIN output for the ListDir SQL statement.
// It must use the same SQL text as Client.ListDir.
func (i *Inspector) ExplainListDir(ctx context.Context,
  mountPath, dir string, limit, offset int64) (string, error)

// ExplainStatPath returns EXPLAIN output for the StatPath SQL statement.
// It must use the same SQL text as Client.StatPath.
func (i *Inspector) ExplainStatPath(ctx context.Context,
  mountPath, path string) (string, error)

// Measure runs the provided function, then returns metrics for the last
// completed query executed on the configured server after the run started.
// This is best-effort and assumes the perf CLI runs queries serially.
func (i *Inspector) Measure(ctx context.Context,
  run func(ctx context.Context) error) (*QueryMetrics, error)

func (i *Inspector) Close() error
```

Implementation requirements (normative):

- `Measure` must:
  - record a start time `t0` using the ClickHouse server clock
  - run `run(ctx)`
  - execute `SYSTEM FLUSH LOGS`
  - query `system.query_log` for the most recent finished query with
    `event_time >= t0` and exclude the `SYSTEM FLUSH LOGS` statement itself
  - return metrics from that row

Server time SQL (normative):

```sql
SELECT now()
```

Flush logs SQL (normative):

```sql
SYSTEM FLUSH LOGS
```

Query-log SQL (normative):

```sql
SELECT
  toUInt64(query_duration_ms) AS duration_ms,
  toUInt64(read_rows) AS read_rows,
  toUInt64(read_bytes) AS read_bytes,
  toUInt64(result_rows) AS result_rows,
  toUInt64(result_bytes) AS result_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= ?
  AND NOT startsWith(trimLeft(query), 'SYSTEM FLUSH LOGS')
ORDER BY event_time DESC
LIMIT 1
```

EXPLAIN requirements (normative):

- `ExplainListDir` and `ExplainStatPath` must execute the underlying query
  prefixed by `EXPLAIN indexes = 1`.
- The SQL body must be identical to the corresponding Client query (same
  WHERE/JOIN shape and parameters).

### Configuration

Connection details are supplied the same way as the server/summarise commands
(same env + flags conventions used by the implementer for other ClickHouse
commands). The clickhouse package itself does not load `.env`.

### Subcommands and flags

`clickhouse-perf import <inputDir>`

- `<inputDir>` (required; positional argument): base directory containing
  `<version>_<mountKey>` subdirectories with `stats.gz` files (same structure
  as `wrstat multi` outputs)
- `--maxLines <n>` (optional; 0 means all lines; applies per input file)
- `--batchSize <n>` (optional; defaults to 10000)
- `--parallelism <n>` (optional; default 1)
- `--quota <file>` (optional; CSV for basedirs quotas)
- `--config <file>` (optional; basedirs config file)

Behaviour:

- Discovers subdirectories using `server.FindDBDirs(inputDir, "stats.gz")`.
- For each discovered subdirectory, performs the same ingestion as
  `cmd/summarise` would for ClickHouse:
  - DGUTA rows + children rows
  - file rows (`wrstat_files`)
  - basedirs snapshot rows (only if both `--quota` and `--config` are provided)

Mount path derivation (normative):

- Mount path is derived from each discovered subdirectory name.
- The subdirectory follows the `<version>_<mountKey>` naming convention.
- Use the shared `mountpath.FromOutputDir()` helper to derive `mount_path` from
  the subdirectory path.
- `updated_at` is taken from the `stats.gz` file mtime.

Serial vs parallel ingest (normative):

- When `--parallelism=1`, ingest the discovered subdirectories serially in
  lexicographic order.
- When `--parallelism>1`, ingest up to N subdirectories concurrently.
- Each stats file ingest is still internally streamed and must not load the
  entire file into memory.

Reports (normative):

- total lines processed per file
- inferred `mount_path` per file
- rows inserted per table per file
- wall time for each phase per file:
  - partition drop / reset
  - insert time per table
  - mount switch time
  - old snapshot partition drop time
- overall wall time and effective throughput (serial vs parallel)

`clickhouse-perf query`

- `--dir <abs/>` (optional; when omitted, a directory is chosen automatically)
- `--uid <n>` and `--gids <csv>` (optional; used for permission query)
- `--repeat <n>` (optional; default 20)

Behaviour:

- Queries run across all active mounts (same as the real server).
- Runs the fixed query suite below and reports per-query latency for each run
  and summary percentiles p50/p95/p99.

Plan + IO verification (normative):

- Before the timing loop begins, the perf CLI must verify pruning for
  `ListDir` and `StatPath` using `clickhouse.NewInspector(cfg)`.
- It must run `ExplainListDir` and `ExplainStatPath` for the chosen `dir` and
  `pickedPath` and fail the command if:
  - `ExplainListDir` output does not mention both `mount_path` and
    `parent_dir`.
- For every timed operation (each repeat), the perf CLI must also print the
  `QueryMetrics` returned by `Inspector.Measure` (duration/read/result rows
  and bytes).

### Query suite (normative)

The perf CLI must not re-specify SQL statements.

Instead, it must exercise the exact same query methods used by the rest of
the program (so the SQL is defined once, in the clickhouse implementation).

Implementation rule (normative):

- The perf CLI must not embed any SQL text in `cmd/`.
- For tree + basedirs queries, it must call the storage-neutral interfaces
  via `clickhouse.OpenProvider(cfg)`:
  - `provider.Tree()` for tree queries
  - `provider.BaseDirs()` for basedirs queries
- For file-level queries, it must call the public ClickHouse query helpers via
  `clickhouse.NewClient(cfg)` (see the Client section).

Fixed query suite (normative):

Queries run across all active mounts, matching how the real server operates.

Directory selection (normative):

- If `--dir` is supplied, use it.
- Otherwise, automatically pick a "representative" directory by:
  1. Call `provider.BaseDirs().MountTimestamps()` to get the list of active
    mounts.
    - Note: this call returns a map keyed by `mount_key` for compatibility.
  2. Decode `mount_key` to a `mount_path` by replacing `／` with `/` and
    ensuring it ends with `/`.
  3. Pick the first decoded mount path (sorted lexicographically) as `startDir`.
  3. Walk the directory tree using the storage-neutral tree API (no filesystem
     calls):
     a. Call `provider.Tree().DirInfo(startDir, filter)` (Age=all, no UID/GID
        restriction).
     b. If `DirInfo.Current.Count` is between 1,000 and 20,000 inclusive, stop
        and use this directory.
     c. Otherwise, choose the child directory with the largest `Count` and
        repeat up to 64 steps.
     d. If no children exist before reaching the target range, use the last
        directory.

The chosen `dir` is used for the file-glob tests and the list/stat tests.

1) Active snapshot freshness:

- Call `provider.BaseDirs().MountTimestamps()` and report the `updated_at`
  values for all active mounts.

2) Tree directory summary:

- Call `provider.Tree().DirInfo(dir, filter)` with an unfiltered filter
  (no UID/GID restriction) and with Age set to the equivalent of
  `DGUTAgeAll`.

3) Basedirs group usage:

- Call `provider.BaseDirs().GroupUsage(DGUTAgeAll)`.

4) Files list directory:

- Call `client.ListDir(ctx, dir, opts)`.

5) Files stat a path:

- First call `client.ListDir(ctx, dir, opts)` once (outside the timing loop)
  and pick the first returned `FileRow.Path` (sorted order).
- Then call `client.StatPath(ctx, pickedPath, opts)`.
- If the directory is empty, skip this operation.

6) Permission check:

- Call `client.PermissionAnyInDir(ctx, dir, --uid, --gids)`.

7) Glob query matrix:

- Exercise `client.FindByGlob(ctx, baseDirs, patterns, opts)` with a fixed set
  of gitignore-style patterns to cover `*` and `**` cases, and to compare
  extension-limited vs non-limited matches, with and without permission
  filtering.

Pattern and option generation (normative):

- Let `baseDirs = []string{dir}`.
- Determine an extension candidate `ext` as follows:
  - Call `client.ListDir(ctx, dir, opts)` once (outside timing loops).
  - Pick the first returned file row where `ext != ""` and `entry_type` is not
    a directory.
  - If no such row exists, set `ext = ""` and skip the ext-limited cases.

Run these cases (normative):

- Case A: patterns = [`*`], RequireOwner=false
- Case B: patterns = [`*`], RequireOwner=true
- Case C: patterns = [`**`], RequireOwner=false
- Case D: patterns = [`**`], RequireOwner=true
- Case E (only if ext != ""): patterns = [`*.${ext}`], RequireOwner=false
- Case F (only if ext != ""): patterns = [`*.${ext}`], RequireOwner=true
- Case G (only if ext != ""): patterns = [`**/*.${ext}`], RequireOwner=false
- Case H (only if ext != ""): patterns = [`**/*.${ext}`], RequireOwner=true

The CLI must report latency per case separately.

Reporting (normative):

- Print per-operation latency and p50/p95/p99 across repeats.
- Print the operation name and the key inputs.
- If the clickhouse package provides debug logging of SQL text, the perf CLI
  may enable it for reproducibility, but it must not contain its own SQL.

----------------------------------------------------------------------

## Repository wiring (what other packages must do)

After the interfaces in `interface_spec.md` are in place, update only
constructors/wiring in:

### `cmd/server`

Call `clickhouse.OpenProvider(cfg)` and pass the returned `server.Provider`
into the server via `server.SetProvider()`.

### `cmd/summarise`

Keep the existing mount path derivation mechanism. The output directory path
follows the naming convention `<version>_<mountKey>`, where `<mountKey>` is
the mount path with `/` replaced by `／` (U+FF0F FULLWIDTH SOLIDUS).

Add a shared helper function (e.g. in `internal/mountpath/mountpath.go`):

```go
package mountpath

// FromOutputDir derives the mount path from an output directory path.
// The directory basename must be `<version>_<mountKey>` where mountKey
// uses ／ (U+FF0F) instead of /.
// Returns the mount path ending with /.
func FromOutputDir(outputDir string) (string, error)
```

Replace Bolt writer/store constructors:

```go
// Derive mount path from output directory
mountPath, err := mountpath.FromOutputDir(dirgutaDB)

// Create writers
dw, err := clickhouse.NewDGUTAWriter(cfg)
dw.SetMountPath(mountPath)
dw.SetUpdatedAt(modtime)
dw.SetBatchSize(dbBatchSize)

bs, err := clickhouse.NewBaseDirsStore(cfg)
bs.SetMountPath(mountPath)
bs.SetUpdatedAt(modtime)

fi, closer, err := clickhouse.NewFileIngestOperation(cfg, mountPath, modtime)

// Register summarisers
s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dw))
s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))
s.AddGlobalOperation(fi)

// After Summarise() returns, close in order:
closer.Close()
bs.Close()
dw.Close()
```

### `cmd/dbinfo`

Call `clickhouse.OpenProvider(cfg)` and use `provider.Tree().Info()` and
`provider.BaseDirs().Info()` to print stats.

### `cmd/clean`

Use `clickhouse.NewHistoryMaintainer(cfg)`.

### Tests

Use `clickhouse` constructors; no other production package imports it.

Note: The `bolt` package may remain in the repository as unused code. The
requirement is that no production code *uses* bolt, not that bolt must be
deleted.
