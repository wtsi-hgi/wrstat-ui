# ClickHouse Cold Tree Performance Specification

## Overview

Make first uncached ClickHouse tree interactions fast enough for `wrstat-ui`.
The fix must not rely on process-local warming, browser cache state, or a
second click. It must improve cold REST/HTTP, CLI/server, and in-process paths
while preserving current ClickHouse facts correctness and reporting the
remaining gap to same-subset Bolt.

The implementation path is ClickHouse-native:

- mandatory narrow AgeAll owner/type filter rows for eligible cold filters and
  `Where`
- active-set prefix rollups for `/`, `/lustre/`, and `/nfs/`, plus immediate
  active mount-root tuple query tuning
- parent-ordered facts by default for Disktree navigation, with one short
  measured fork for child facts or a proven ClickHouse projection
- harness coverage for REST, CLI, Bolt deltas, query counters, cache counters,
  response bytes, correction-pass regressions, and the t283 REST anomaly

Required baseline data is the 2026-06-07 four-mount subset, imported at
100,000 lines per mount from `/home/ubuntu/output/lustre` and
`/home/ubuntu/output/nfs`, plus the same named mounts capped at 250,000 lines
per mount. Full no-cap production runs are optional manual evidence.

Baseline subset rows:

- `t283_imaging`, `/nfs/t283_imaging/`, NFS, `stats.gz` 6,095,109,852 bytes,
  100,000 imported lines, 70,523 dir facts, 35,005 children
- `scratch120`, `/lustre/scratch120/`, Lustre, 1,420,306,103 bytes,
  100,000 lines, 1,508 dir facts, 546 children
- `scratch122`, `/lustre/scratch122/`, Lustre, 2,780,894,204 bytes,
  100,000 lines, 32,897 dir facts, 9,608 children
- `scratch127`, `/lustre/scratch127/`, Lustre, 5,317,090,708 bytes,
  100,000 lines, 2,490 dir facts, 1,144 children

Baseline ClickHouse database `wrstat_schema2_baseline_084721` imported
400,000 file records in 5.56 s, max RSS 407,392,256 bytes. Table sizes:
`wrstat_files` 400,000 rows and 7.09 MB compressed, `wrstat_dir_facts`
46,307 rows, 7.28 MB compressed, 216.53 MB uncompressed, and
`wrstat_children` 46,303 rows, 0.44 MB compressed. Fact vectors had
915,933 entries, average 19.78 per dir, max 1,030. Same-subset Bolt import
was 3,677.091 ms wall 0:03.71, max RSS 193,656 KB, storage 106,700,800 bytes
for DGUTA plus 81,920 bytes for basedirs.

Current ClickHouse REST anchors on this subset:

- `/` auth tree p50/p95 88.2/89.8 ms, 1,036 JSON bytes,
  349 offline gzip bytes
- `/lustre/` auth tree p50/p95 61.1/71.4 ms, browser-like p50 47.4 ms
- `/nfs/` auth tree p50/p95 43.9/46.3 ms, browser-like p50 35.6 ms
- `/nfs/t283_imaging/` auth tree p50/p95 27.2/31.1 ms,
  browser-like mount-root p50 10.7 ms
- `/` no-auth REST `where` p50/p95 306.1/334.9 ms, 200,102 JSON bytes,
  16,480 gzip bytes
- `/` type-filter REST `where` p50/p95 527.3/553.1 ms, 167,214 JSON bytes,
  13,713 gzip bytes
- `/nfs/t283_imaging/` gid+uid+type filtered `where` p50/p95
  1160.6/1554.4 ms, reading 149-164 MB
- repeated cached `where` about 64-84 ms, proving process caches hide cold cost

Same-subset Bolt p50/p95 comparison targets:

- `/` Disktree 1.933/2.459 ms
- `/lustre/` Disktree 1.681/2.269 ms
- `/nfs/` Disktree 0.242/0.261 ms
- `/nfs/t283_imaging/` Disktree 0.463/1.052 ms
- `/` `where_cold_then_cached` 36.570/40.651 ms
- `/lustre/` `where_cold_then_cached` 47.793/49.954 ms
- `/nfs/` `where_cold_then_cached` 11.280/13.137 ms
- `/nfs/t283_imaging/` `where_cold_then_cached` 13.203/23.621 ms
- direct filtered t283 `where_cold_then_cached` 67.355/78.534 ms

Final reports must show ClickHouse p95 deltas from these values.

Prototype evidence to preserve:

- `wrstat_dir_filter_ageall`: 103,980 rows, 1.00 MB compressed,
  16.33 MB uncompressed; simplified t283 filter p50 improved from
  31 ms to 19 ms and pruned 13/13 granules to 5/13.
- Full current `where_filtered_whole_mount` still had p50 726 ms and read
  149-164 MB, so endpoint-level proof is required.
- Scalar `wrstat_active_prefix_rollups(active_set_id, dir)` had 3 rows,
  251 compressed bytes, matched `all_count=400000`, `file_count=353197`,
  `child_count=9`, read 1/1 granules, and benchmarked p50/p95 2/4 ms.
- Active-root tuple tuning changed granules from 20/20 to 4/20 and
  p50/p95 from 6/10 ms to 5/6 ms.
- `wrstat_child_facts` prototype had 46,303 rows, 1.74 MB compressed,
  38.94 MB uncompressed, and high-fanout p50 4 ms.
- `wrstat_parent_facts` prototype had 46,307 rows, 1.84 MB compressed,
  39.70 MB uncompressed, high-fanout p50 5 ms, and 1/6 granules. Projection
  optimizer use was not proven.

## Architecture

**Packages and files**

- `clickhouse/schema/*.sql`: add final DDL for new physical tables.
- `clickhouse/schema.go`: bootstrap all final DDL at schema version `1`.
- `clickhouse/dguta_writer.go`: import order, readiness, publish, cleanup.
- `clickhouse/import_block_writer.go`: bounded writers for new rows.
- `clickhouse/dir_filter_ageall.go`: AgeAll filter routing and readiness.
- `clickhouse/active_prefix_rollups.go`: active-set rollup refresh/readers.
- `clickhouse/parent_facts.go`: parent-ordered navigation fact writers/readers.
- `clickhouse/database.go`: route `DirInfo`, `DirInfos`, `Children`,
  `DirsHaveChildren`, `Where`, and `Info`.
- `clickhouse/file_api.go`: preserve permission summary checks.
- `clickhouse/active_mounts.go`: active-set id and full tuple helper.
- `clickhouse/virtual_children.go`: virtual children plus active-set cleanup.
- `clickhouse/database_cache.go`: cache keys and cache hit/miss counters.
- `clickhouse/inspector.go`: query metrics and EXPLAIN support.
- `internal/chperf/*`, `cmd/clickhouse_perf.go`: import/query/REST gates.
- `internal/boltperf/*`, `cmd/bolt_perf.go`: Bolt comparison fields.
- `server/tree.go`, `server/where.go`: REST status and response byte probes.
- `cmd/where.go`: real CLI/server timing harness target.

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

**Import order**

Deterministic snapshot import order is:

1. Drop stale inactive partitions for the target `(mount_path, snapshot_id)`.
2. Stream file rows and canonical `wrstat_dir_facts`/`wrstat_children`.
3. Stream `wrstat_dir_filter_ageall` during the same import.
4. Stream selected parent/nav facts during the same import, or materialize the
   selected projection before readiness if projection wins C1.
5. Write `wrstat_dir_projection_sets` only after all snapshot-scoped facts,
   filter rows, and navigation rows are complete.
6. Publish the active mount event.
7. Refresh active-prefix rollups only after the active set changes.
8. Drop replaced inactive snapshot partitions.

**AgeAll filter DDL**

`wrstat_dir_filter_ageall` is derived, snapshot-scoped, and mandatory for new
imports. It stores only AgeAll owner/type rows.

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

**Active-prefix DDL**

`active_set_id` is a deterministic hash of sorted active rows. Each input row
includes `mount_path`, `snapshot_id`, and `updated_at` UTC. Rollups cover at
least `/`, `/lustre/`, and `/nfs/`.

```sql
CREATE TABLE IF NOT EXISTS wrstat_active_prefix_rollups (
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
  file_count UInt64 CODEC(Delta, LZ4),
  file_size UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, dir)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_active_prefix_filter_ageall (
  active_set_id String CODEC(ZSTD(3)),
  dir String CODEC(LZ4),
  gid UInt32,
  uid UInt32,
  ft UInt16,
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, dir, gid, uid, ft)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS wrstat_active_prefix_rollup_sets (
  active_set_id String CODEC(ZSTD(3)),
  active_mount_count UInt64 CODEC(Delta, ZSTD(3)),
  prefix_count UInt64 CODEC(Delta, ZSTD(3)),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY active_set_id;
```

V1 stores scalar summaries plus narrow AgeAll UID/GID/type prefix rows. It
does not store full filter vectors unless later evidence proves AgeAll prefix
rows cannot meet the gates.

**Navigation DDL**

Default navigation object is a physical parent-ordered facts table. A
ClickHouse projection may replace it only if the real endpoint query proves
projection use with `EXPLAIN indexes = 1`. Physical child facts may replace it
only if the C1 decision gate proves a better filtered endpoint result.

```sql
CREATE TABLE IF NOT EXISTS wrstat_parent_facts (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  parent_dir String CODEC(LZ4),
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
  has_children UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, parent_dir, dir)
SETTINGS index_granularity = 8192;
```

**Readiness, cleanup, and caches**

- `wrstat_dir_projection_sets` remains the snapshot readiness marker. A row
  means `wrstat_dir_facts`, `wrstat_children`, `wrstat_dir_filter_ageall`,
  and the selected navigation object are complete for that snapshot.
- `wrstat_active_prefix_rollup_sets` is the active-set readiness marker.
  Readers fall back to live facts only when the set is absent, refresh fails,
  or the table is absent during rolling development.
- Snapshot cleanup drops new snapshot partitions by `(mount_path,
  snapshot_id)` for failed, old, inactive, tombstoned, and rollback snapshots.
- Active-set cleanup drops replaced `active_set_id` partitions for virtual
  children, active-prefix rollups, and active-prefix AgeAll rows.
- Cache keys include active set id, path, filter fingerprint, permission
  inputs, endpoint/schema query version, mount path, and snapshot id where
  applicable. Publish, rollback, tombstone, and provider update invalidate
  affected keys.

**Object contracts**

- `wrstat_dir_filter_ageall`: derived snapshot table. Writer:
  `dgutaWriter` AgeAll derived writer. Readers: eligible AgeAll filters in
  tree summaries, `Where`, and child checks. Readiness:
  `wrstat_dir_projection_sets`. Cleanup: snapshot partition drop.
- `wrstat_active_prefix_rollups`: derived active-set scalar table. Writer:
  active-set refresh after active changes. Readers: virtual ancestor
  summaries. Readiness: `wrstat_active_prefix_rollup_sets`. Cleanup:
  active-set partition drop.
- `wrstat_active_prefix_filter_ageall`: derived active-set AgeAll table.
  Writer and cleanup match scalar rollups. Readers: eligible filtered virtual
  ancestor summaries.
- `wrstat_parent_facts`: derived snapshot navigation table. Writer:
  `dgutaWriter` selected nav writer. Readers: Disktree child summaries and
  parent-range checks. Readiness: `wrstat_dir_projection_sets`. Cleanup:
  snapshot partition drop.
- `wrstat_active_prefix_rollup_sets`: derived readiness table. Writer:
  active-prefix refresh after rollup rows are complete. Readers: rollup
  routing. Cleanup: active-set partition drop.

## Section A: AgeAll Filter Rows

### A1: Import Narrow AgeAll Rows

As a ClickHouse importer, I want deterministic AgeAll owner/type filter rows,
so that cold filtered summaries and `Where` avoid full facts-vector scans.

Write `wrstat_dir_filter_ageall` rows during snapshot import, not by manual
post-hoc derivation. Append one row for each AgeAll `(gid, uid, ft, dir)`
summary present in a `wrstat_dir_facts` row. Exclude age-specific facts.
Flush final partial blocks, abort empty batches, and keep memory bounded by
the existing `importBlockWriter` pattern.

**Package:** `clickhouse/`
**File:** `clickhouse/schema/012_dir_filter_ageall.sql`,
`clickhouse/dguta_writer.go`, `clickhouse/dir_filter_ageall.go`,
`clickhouse/import_block_writer.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`clickhouse/bootstrap_test.go`, `clickhouse/clean_schema_test.go`

**Acceptance tests:**

1. Given an empty database, when schema bootstrap completes, then
   `wrstat_dir_filter_ageall` exists with partition key
   `(mount_path, snapshot_id)` and order
   `(mount_path, snapshot_id, gid, uid, ft, dir)`.
2. Given `/mnt/a/` has AgeAll GUTAs `(7,11,bam,count=2,size=20)`,
   `(7,12,other,1,5)`, and age-specific `(8,11,bam,A2M,4,40)`, when import
   closes, then the table has exactly 2 rows for `/mnt/a/`, total count 3,
   total size 25, and no value from the age-specific row.
3. Given batch size 2 and 5 AgeAll rows, when import closes, then all 5 rows
   are queryable and the final partial block was sent once.
4. Given `PrepareBatch` succeeds and `Send` returns an ambiguous error, when
   `Close` fails, then the new snapshot partitions for files, children,
   facts, AgeAll, parent facts, and readiness are dropped, and the previous
   active snapshot remains active.
5. Given 1,000,000 generated facts in `t.TempDir()`, when AgeAll import runs,
   then heap growth is less than 20 MB using the GoConvey memory-bounded
   pattern from `go-conventions`.

### A2: Route Eligible Filters To AgeAll Rows

As a tree reader, I want eligible AgeAll owner/type filters to use the narrow
rows, so that first filtered `Where`, `DirInfo`, `DirInfos`, and
`DirsHaveChildren` are fast and correct.

Eligibility requires `filter != nil`, `filter.Age == db.DGUTAgeAll`, and at
least one UID, GID, or file-type predicate. Empty UID/GID lists are no-match
filters. Age-specific filters must never read AgeAll rows as age-specific
rows. If the table is absent, not ready for `(mount_path, snapshot_id)`, or
semantically inapplicable, fall back to `wrstat_dir_facts`.

**Package:** `clickhouse/`
**File:** `clickhouse/dir_filter_ageall.go`, `clickhouse/database.go`,
`clickhouse/dir_summary.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`clickhouse/dguta_writer_test.go`, `clickhouse/clean_schema_test.go`

```go
func dirFilterAgeAllCanHandleFilter(filter *db.Filter) bool
func dirFilterAgeAllFilterExpression(filter *db.Filter) (string, []any)
```

**Acceptance tests:**

1. Given filter `{Age: DGUTAgeAll, GIDs: []uint32{7}}`, when routing is
   evaluated, then `dirFilterAgeAllCanHandleFilter` returns `true`.
2. Given filter `{Age: DGUTAgeA2M, GIDs: []uint32{7}}`, when routing is
   evaluated, then it returns `false` and the generated SQL reads
   `wrstat_dir_facts`, not `wrstat_dir_filter_ageall`.
3. Given empty GID list `[]uint32{}` and AgeAll, when `DirInfo` runs, then it
   returns no summary and performs no AgeAll table scan.
4. Given imported facts and complete AgeAll rows, when broad facts-vector
   filtering and AgeAll filtering run for GID 7, UID 11, type `bam`, then both
   return identical `DirSummary` values for count, size, atime, mtime,
   buckets, UID list, GID list, file-type mask, and dir set.
5. Given the 2026-06-07 subset and filter `/nfs/t283_imaging/`,
   gid `14976`, uid `20155`, type `other`, AgeAll, when facts vectors and
   AgeAll rows are compared, then both return 34,998 dirs, 764,218 files, and
   1,197,943,849,957 bytes.
6. Given the same subset, when `EXPLAIN indexes = 1` runs for the AgeAll route,
   then the report shows at most 5 of 13 t283 granules read for the simplified
   query and records the endpoint read rows/bytes for the full `Where`.

### A3: AgeAll Readiness And Cleanup

As an operator, I want AgeAll rows to share snapshot atomicity, so that active
readers never see partial derived filter data.

`wrstat_dir_projection_sets` is written after facts, children, AgeAll rows,
and selected navigation rows. Readiness checks are scoped to
`(mount_path, snapshot_id)`, not to active table row counts. Cleanup removes
AgeAll partitions on failed imports, old inactive snapshots, tombstones, and
rollbacks.

**Package:** `clickhouse/`
**File:** `clickhouse/dguta_writer.go`,
`clickhouse/active_snapshot_cleanup.go`, `clickhouse/dir_filter_ageall.go`
**Test file:** `clickhouse/active_snapshot_cleanup_test.go`,
`clickhouse/dguta_writer_test.go`

**Acceptance tests:**

1. Given AgeAll rows exist but no `wrstat_dir_projection_sets` row exists for
   the snapshot, when an eligible filter runs, then the reader uses facts
   fallback and returns the same summary as facts.
2. Given readiness exists for snapshot A but not snapshot B, when snapshot B
   is active during a failed retry, then AgeAll routing is disabled for B.
3. Given successful import of snapshot B after snapshot A, when old snapshot
   cleanup completes, then AgeAll partition A is gone and partition B remains.
4. Given tombstone cleanup of `/mnt/`, when cleanup completes, then no
   `wrstat_dir_filter_ageall` active part remains for the tombstoned snapshot.
5. Given retry import writes duplicate deterministic snapshot rows, when
   partition reset runs first, then AgeAll row count equals the fixture count,
   not twice the fixture count.

## Section B: Active-Set Prefix Rollups

### B1: Build Active Prefix Rollups

As a tree reader, I want ready active-prefix summaries for virtual ancestors,
so that `/`, `/lustre/`, and `/nfs/` do not compose cold root state from many
mount fact rows.

Refresh active-prefix rollups after publish, rollback, tombstone, or provider
update changes the active set. Write scalar rows and AgeAll prefix rows first,
then write `wrstat_active_prefix_rollup_sets`. Rollups are derived, not
canonical. Canonical data remains `wrstat_dir_facts` and
`wrstat_dir_filter_ageall`.

**Package:** `clickhouse/`
**File:** `clickhouse/schema/013_active_prefix_rollups.sql`,
`clickhouse/active_prefix_rollups.go`, `clickhouse/active_mounts.go`,
`clickhouse/dguta_writer.go`, `clickhouse/provider.go`
**Test file:** `clickhouse/provider_test.go`,
`clickhouse/dguta_writer_test.go`, `clickhouse/bootstrap_test.go`

```go
func fingerprintForMountsActive(rows []mountsActiveRow) string
```

**Acceptance tests:**

1. Given active rows sorted differently but containing the same mount path,
   snapshot id, and updated-at values, when `fingerprintForMountsActive` runs,
   then both orders produce the same non-empty active set id.
2. Given two active sets differing only by one mount `updated_at`, when ids are
   computed, then the ids differ.
3. Given active mounts `/lustre/scratch120/`, `/lustre/scratch122/`,
   `/lustre/scratch127/`, and `/nfs/t283_imaging/`, when rollups refresh, then
   rows exist for `/`, `/lustre/`, and `/nfs/`, and the set row has
   `prefix_count = 3`.
4. Given the 2026-06-07 subset, when `/` scalar rollup is read, then
   `all_count = 400000`, `file_count = 353197`, and `child_count = 9`.
5. Given the prototype scalar rollup query, when `EXPLAIN indexes = 1` and the
   benchmark run complete, then the report records 1 of 1 granules read and
   p50/p95 at or below 2/4 ms on the 2026-06-07 subset.
6. Given refresh fails before the set row is written, when `/` is queried, then
   the reader falls back to live facts and increments the rollup miss counter.

### B2: Route Virtual Ancestors And Tune Root Tuples

As a web user, I want first root and namespace clicks to be fast, so that the
root page and `/lustre/` or `/nfs/` clicks do not pay cold active-root scans.

`DirInfo` and `DirInfos` use active-prefix rollups first for `/`, `/lustre/`,
`/nfs/`, and later covered virtual ancestors. `Children("/")` still exposes
separate `/lustre` and `/nfs` boxes. `/` aggregates all active selected Lustre
and NFS mounts. `/lustre/` and `/nfs/` aggregate only their own active mounts.

The active mount-root fact query must bind the full
`(mount_path, snapshot_id, dir)` tuple for each active mount root instead of a
partial `(mount_path, snapshot_id)` tuple plus `dir = mount_path`.

**Package:** `clickhouse/`
**File:** `clickhouse/database.go`, `clickhouse/active_mounts.go`,
`clickhouse/active_prefix_rollups.go`, `clickhouse/virtual_children.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`clickhouse/provider_test.go`, `clickhouse/clean_schema_test.go`

**Acceptance tests:**

1. Given the four-mount subset, when `DirInfo("/")` runs with no filter, then
   the current summary equals the active-prefix row for `/`, and children are
   exactly `/lustre/` and `/nfs/` with correct summaries.
2. Given the same subset, when `DirInfo("/lustre/")` and `DirInfo("/nfs/")`
   run, then each summary equals only its active mounts and neither includes
   the other namespace.
3. Given AgeAll prefix rows and filter gid 14976 uid 20155 type `other`, when
   `/nfs/` filtered `DirInfo` runs, then the summary equals the sum of
   eligible `wrstat_dir_filter_ageall` rows under active NFS mounts.
4. Given active-prefix tables are absent, when `/` is queried, then the reader
   returns the same summary through facts fallback and records route
   `active_prefix_fallback`.
5. Given SQL strings in production readers, when the tuple regression test
   scans active mount-root facts SQL, then no query contains
   `d.dir = d.mount_path` with only a two-column active tuple.
6. Given the tuned active-root SQL, when `EXPLAIN indexes = 1` runs on the
   subset, then it reads at most 4 of 20 granules and p50/p95 is no worse than
   5/6 ms for the in-process root fact query.

### B3: Active-Set Cleanup And Cache Invalidation

As an operator, I want active-prefix data to follow active-set changes, so that
rollbacks, tombstones, and provider updates cannot serve stale root summaries.

Cleanup drops replaced active-set partitions for `wrstat_virtual_children`,
`wrstat_virtual_children_sets`, `wrstat_active_prefix_rollups`,
`wrstat_active_prefix_filter_ageall`, and
`wrstat_active_prefix_rollup_sets`. Cache invalidation uses active set id,
path, filter, permission inputs, and schema/query version.

**Package:** `clickhouse/`
**File:** `clickhouse/virtual_children.go`,
`clickhouse/active_prefix_rollups.go`, `clickhouse/database_cache.go`
**Test file:** `clickhouse/provider_test.go`,
`clickhouse/active_snapshot_cleanup_test.go`

**Acceptance tests:**

1. Given active set A then publish creates active set B, when cleanup runs,
   then all A active-prefix and virtual-child partitions are dropped and B
   partitions remain.
2. Given rollback from B to A, when `/` is queried after invalidation, then the
   cache key contains A and returns A totals, not B totals.
3. Given the same path and filter but different schema/query versions, when
   both are cached, then they occupy distinct entries and counters show one
   miss per version.
4. Given provider update refreshes active metadata, when first `/nfs/` request
   runs, then active metadata is resolved once for the request and cache
   counters report active metadata hit/miss counts.

## Section C: Disktree Navigation Facts

### C1: Choose The Navigation Shape

As an implementor, I want one bounded measured decision for Disktree
navigation, so that the final route is based on endpoint evidence and does not
delay AgeAll or active-prefix work.

The default is physical `wrstat_parent_facts`. Before wiring readers, run a
two-day maximum decision gate comparing:

- physical parent facts ordered by `(mount_path, snapshot_id, parent_dir, dir)`
- physical child facts or `wrstat_tree_nav_facts`
- a ClickHouse projection on the real endpoint query

Select projection only if real endpoint `EXPLAIN indexes = 1` proves reliable
projection use for broad and filtered queries. Select child facts only if they
are at least 15% faster at p95 than parent facts for high-fanout and filtered
Disktree, preserve one parent-range read, and do not fail import memory or row
amplification gates. Otherwise implement parent facts.

**Package:** `clickhouse/`, `internal/chperf/`
**File:** `clickhouse/parent_facts.go`,
`clickhouse/mount_dir_projection_writer.go`, `internal/chperf/query.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`clickhouse/database_dirinfo_test.go`, `internal/chperf/query_test.go`

**Acceptance tests:**

1. Given the 305-child high-fanout parent from the subset, when candidate
   queries run, then the report includes p50/p95/p99, read rows, read bytes,
   read marks, result counts, and `EXPLAIN indexes = 1` output for all three
   shapes.
2. Given projection is selected, when the real REST tree endpoint query is
   explained for broad and filtered child summaries, then the output names the
   projection and shows parent-range pruning; otherwise projection is rejected.
3. Given child facts are selected, when filtered Disktree runs for AgeAll
   owner/type filters, then it uses one parent-range read or a documented
   narrow AgeAll companion read and matches parent facts results exactly.
4. Given no candidate beats parent facts by the required evidence, when the
   decision gate closes, then `wrstat_parent_facts` is the implemented object.

### C2: Import Parent-Ordered Facts

As a Disktree reader, I want parent-ordered facts ready with each snapshot, so
that ordinary child summaries use one parent-range read.

`wrstat_parent_facts` duplicates `wrstat_dir_facts` data with `parent_dir`
first in the order key. The importer writes one row per directory fact during
the same deterministic snapshot import. For mount roots, `parent_dir` is the
virtual parent such as `/lustre/` or `/nfs/`; root `/` is handled by active
prefix rollups.

**Package:** `clickhouse/`
**File:** `clickhouse/schema/014_parent_facts.sql`,
`clickhouse/parent_facts.go`, `clickhouse/dguta_writer.go`
**Test file:** `clickhouse/dguta_writer_test.go`,
`clickhouse/active_snapshot_cleanup_test.go`

**Acceptance tests:**

1. Given fixture dirs `/mnt/`, `/mnt/a/`, `/mnt/a/b/`, and `/mnt/c/`, when
   import closes, then `wrstat_parent_facts` has 4 rows and parent dirs are
   `/`, `/mnt/`, `/mnt/a/`, and `/mnt/`.
2. Given a high-fanout parent with 305 children, when parent facts are queried,
   then one parent-prefix range returns exactly 305 child summaries ordered by
   `dir`.
3. Given a low-fanout parent with one child and deep chain `/mnt/a/b/c/`, when
   parent facts are queried for each parent, then each result contains exactly
   the direct child and correct `has_children`.
4. Given filtered fixture data, when parent facts vector filtering runs for
   GID 7 UID 11 type `bam`, then returned child summaries equal
   `wrstat_dir_facts` vector results for the same child dirs.
5. Given failed import after parent facts rows are written but before
   readiness, when cleanup completes, then parent facts for the failed
   snapshot are gone and no active reader uses them.
6. Given the subset prototype, when parent facts table stats are reported, then
   the report records row count 46,307, compressed bytes, uncompressed bytes,
   import phase duration, and p50 no worse than 5 ms for the high-fanout query.

### C3: Route Disktree Through Navigation Facts

As a web user, I want ordinary directory clicks to avoid child list plus
`dir IN` fact lookups, so that Disktree navigation improves from cold state.

For ordinary mount directories, `DirInfo` child summaries use parent facts
where ready. `Children` may keep using `wrstat_children` as canonical child
edges, but Disktree endpoint paths must use parent facts for child summaries.
For virtual parents, use virtual children plus active-prefix rollups and
mount-root parent facts. Eligible AgeAll filters still prefer
`wrstat_dir_filter_ageall`.

**Package:** `clickhouse/`, `server/`
**File:** `clickhouse/database.go`, `clickhouse/parent_facts.go`,
`server/tree.go`
**Test file:** `clickhouse/database_dirinfo_test.go`,
`server/tree_batch_test.go`, `server/server_test.go`

**Acceptance tests:**

1. Given parent facts are ready, when `DirInfo("/nfs/t283_imaging/")` runs
   broadly, then child summaries come from `wrstat_parent_facts` and match the
   old children plus facts route exactly.
2. Given parent facts are not ready, when the same `DirInfo` runs, then the
   reader falls back to `wrstat_children` plus `wrstat_dir_facts` and records
   route `parent_facts_fallback`.
3. Given `/`, `/lustre/`, and `/nfs/`, when REST tree requests run, then `/`
   shows `/lustre` and `/nfs`, `/lustre/` shows active Lustre mount roots, and
   `/nfs/` shows active NFS mount roots with correct summaries.
4. Given broad, file-only, owner/type, and age-specific filters, when
   Disktree child summaries run, then broad and file-only use parent scalar
   columns, owner/type AgeAll uses AgeAll rows, and age-specific filters use
   facts vectors without reading AgeAll rows.
5. Given the 305-child parent, when the real auth REST tree endpoint runs from
   cold provider state, then parent facts endpoint p95 improves over the
   current children plus `dir IN` route and the report records query count,
   JSON bytes, gzip bytes, and result child count 305.

## Section D: Info And Permission/Auth Summaries

### D1: Preserve Info() Counts

As an operator, I want `Info()` to keep reporting canonical active counts, so
that new derived tables do not change `dbinfo` or perf baselines.

`Info()` counts only canonical active `wrstat_dir_facts` and
`wrstat_children` rows. It does not count AgeAll filter rows, active-prefix
rollups, parent facts, virtual children, or readiness rows. Snapshot-scoped
databases count only their pinned snapshot. Publish, rollback, tombstone, and
provider update must not serve stale counts.

**Package:** `clickhouse/`, `db/`
**File:** `clickhouse/database.go`, `db/info.go`
**Test file:** `clickhouse/database_info_test.go`,
`internal/chperf/query_test.go`

**Acceptance tests:**

1. Given active snapshot B for `/mnt/` has `wrstat_dir_facts` rows
   `/mnt/`, `/mnt/a/`, and `/mnt/b/` with vector lengths 2, 1, and 3, and
   children `/mnt/ -> /mnt/a/` and `/mnt/ -> /mnt/b/`, when `Info()` runs,
   then `NumDirs = 3`, `NumDGUTAs = 6`, `NumParents = 1`, and
   `NumChildren = 2`.
2. Given stale snapshot A has 99 facts rows and derived rows exist in
   `wrstat_dir_filter_ageall`, `wrstat_parent_facts`, and
   `wrstat_active_prefix_rollups`, when active snapshot B from test 1 is
   queried, then `Info()` still returns 3, 6, 1, and 2.
3. Given a snapshot-scoped database pinned to snapshot A with one facts row of
   vector length 4 and one child edge, when active snapshot B has the test 1
   rows, then pinned `Info()` returns `NumDirs = 1`, `NumDGUTAs = 4`,
   `NumParents = 1`, and `NumChildren = 1`.
4. Given active set A returns 3, 6, 1, and 2, and publish creates active set B
   with one facts row of vector length 2 and no children, when `Info()` runs
   after publish and after rollback, then it returns 1, 2, 0, 0 for B and
   3, 6, 1, 2 for A.
5. Given query counters are enabled, when `Info()` runs after new routes are
   added, then it issues only canonical facts and children count queries and
   records zero reads from AgeAll, active-prefix, parent facts, and virtual
   child tables.
6. Given final perf reports include `info`, when gates compare current
   baseline and candidate reports on the same dataset, then candidate p95 is
   no more than 10% slower and the report records query count, read rows,
   read bytes, read marks, and result counts 3, 6, 1, and 2 for the fixture
   run.

### D2: Preserve Permission And Auth Summary Checks

As a web or CLI user, I want permission checks and restricted summaries to stay
correct after the new routes, so that faster summaries never reveal hidden
data or reject allowed data.

`PermissionAnyInDir`, `PermissionPath`, tree `NoAuth`, restricted `where`, and
auth tree filters must keep current Unix name-based server/CLI semantics.
Numeric IDs are allowed in perf-harness fields such as `--tree-gids`,
`--tree-uids`, `--uid`, and `--gids`; real `where --groups` and
`where --users` examples use Unix group names and usernames.

**Package:** `clickhouse/`, `server/`, `internal/chperf/`
**File:** `clickhouse/file_api.go`, `clickhouse/database.go`,
`server/tree.go`, `server/filter.go`, `server/where.go`,
`internal/chperf/query.go`
**Test file:** `clickhouse/client_file_api_test.go`,
`clickhouse/database_info_test.go`, `server/server_test.go`,
`internal/chperf/query_test.go`

```go
func (c *Client) PermissionAnyInDir(
    ctx context.Context,
    dir string,
    uid uint32,
    gids []uint32,
) (bool, error)

func (c *Client) PermissionPath(
    ctx context.Context,
    path string,
    uid uint32,
    gids []uint32,
) (bool, error)
```

**Acceptance tests:**

1. Given active `/mnt/a/` facts include an AgeAll entry for uid 11 gid 7 and
   stale facts include uid 99 gid 99, when `PermissionAnyInDir(ctx,
   "/mnt/a/", 11, []uint32{8})`, `PermissionAnyInDir(ctx, "/mnt/a/", 99,
   []uint32{7})`, and `PermissionAnyInDir(ctx, "/mnt/a/", 99, []uint32{8})`
   run, then the results are `true`, `true`, and `false`.
2. Given active file `/mnt/a/file.bam` is uid 11 gid 7 and stale snapshot A
   has uid 99 gid 99 for the same path, when `PermissionPath` runs with the
   same three uid/gid inputs as test 1, then the results are `true`, `true`,
   and `false`.
3. Given an authenticated tree user allowed only gid 7, and `/mnt/` has child
   summaries `/mnt/open/` with GIDs `[7]` and `/mnt/closed/` with GIDs `[8]`,
   when GET `/rest/v1/auth/tree?path=/mnt/` runs, then `/mnt/open/` has
   `NoAuth = false` and `HasChildren = true`, while `/mnt/closed/` has
   `NoAuth = true` and no `Children` field.
4. Given the same user requests disallowed Unix group `group8`, when GET
   `/rest/v1/auth/tree?path=/mnt/&groups=group8` or auth REST `where` runs,
   then the server returns HTTP 400 with `ErrBadQuery` and performs no
   summary route read.
5. Given a no-auth REST `where` server has Unix group `wrstat_perf_g14976`
   mapped to gid 14976 and user `wrstat_perf_u20155` mapped to uid 20155,
   when `groups=wrstat_perf_g14976&users=wrstat_perf_u20155&types=other`
   runs, then the filter is gids `[14976]`, uids `[20155]`, type `other`,
   status is 200, and the digest matches direct `Tree.Where`.
6. Given final perf reports include `permission_check`, auth tree, restricted
   auth `where`, and no-auth `where`, when gates compare current baseline and
   candidate reports on the same dataset, then candidate p95 is no more than
   10% slower for permission/auth checks and all result digests, `NoAuth`
   flags, status codes, JSON bytes, gzip bytes, and cache counters match.

## Section E: Harness, Anomaly, And Perf Gates

### E1: Expand Perf And Tracing Harnesses

As a performance reviewer, I want measured REST, CLI, query, cache, and Bolt
evidence, so that cold-path cost cannot hide outside in-process tests.

Extend current reports instead of creating a parallel format. Required fields
are `schema_version`, `backend`, `git_commit`, `input_dir`, `repeat`,
`warmup`, `selected_tables`, `table_stats`, `facts_vector_stats`,
`max_rss_bytes`, and `operations`. Each operation records `name`, `inputs`,
`durations_ms`, `read_rows`, `read_bytes`, `read_marks`, `result_counts`,
`p50_ms`, `p95_ms`, and `p99_ms`.

Add REST/CLI/browser fields where applicable: `status_codes`, `json_bytes`,
`gzip_bytes`, `query_count`, `cache_hits`, `cache_misses`, `fetch_ms`,
`json_decode_ms`, `react_render_ms`, `layout_canvas_ms`, and
`first_visible_update_ms`. Browser/Playwright automation is follow-up work
unless existing infrastructure makes it low risk; REST and CLI/server gates are
required now.

**Package:** `internal/chperf/`, `internal/boltperf/`, `cmd/`, `server/`
**File:** `internal/perfreport/report.go`, `internal/chperf/query.go`,
`internal/chperf/final_gate.go`, `cmd/clickhouse_perf.go`,
`cmd/bolt_perf.go`, `cmd/where.go`, `server/tree.go`, `server/where.go`
**Test file:** `internal/chperf/query_test.go`,
`internal/chperf/final_gate_test.go`, `internal/boltperf/report_test.go`,
`cmd/clickhouse_perf_test.go`, `server/server_test.go`

**Acceptance tests:**

1. Given `system.query_log` is available, when a measured ClickHouse query
   runs, then report fields include non-empty `read_rows`, `read_bytes`,
   `read_marks`, elapsed duration, memory when available, and result count.
2. Given `system.query_log` is absent, when a measured query runs, then the
   inspector fallback records wall duration and profile-event deltas for rows,
   bytes, and marks without failing the run.
3. Given REST tree and REST `where` requests, when the REST harness runs, then
   each operation records HTTP status `200`, JSON bytes, gzip bytes, query
   count, cache hit/miss counts, result count, p50/p95/p99, and route inputs.
4. Given the test server maps Unix group `wrstat_perf_g14976` to gid 14976
   and user `wrstat_perf_u20155` to uid 20155, when the CLI/server harness
   runs this command, then it records first-run wall time, REST status, result
   count, JSON bytes, and gzip bytes.
   ```bash
   timeout 60s ./wrstat-ui where \
     --dir /nfs/t283_imaging/ \
     --groups wrstat_perf_g14976 \
     --users wrstat_perf_u20155 \
     --types other --json
   ```
5. Given Bolt query reports, when final gates read them, then each matching
   operation has stable `result_counts` and summary digests comparable with
   ClickHouse reports.
6. Given any optional index, rollup, or cache route, when paired correctness
   checks run, then broad and filtered result digests match canonical facts
   and include matched input metadata.

### E2: Resolve The t283 Filtered REST Anomaly

As a reviewer, I want request-order-independent filtered REST results, so that
filtered click-through timings are not accepted with a cache-state bug.

Current anomaly: isolated type-only REST `/nfs/t283_imaging/` `where` returned
2 rows, but the same filtered request after a filtered root `/` request
returned 87 rows in the same provider. Treat this as a correctness or
cache-state bug until disproved.

**Package:** `server/`, `clickhouse/`, `internal/chperf/`
**File:** `server/where.go`, `clickhouse/database.go`,
`clickhouse/database_cache.go`, `internal/chperf/query.go`
**Test file:** `server/server_test.go`, `clickhouse/database_dirinfo_test.go`,
`internal/chperf/query_test.go`

**Acceptance tests:**

1. Given a fresh provider, when filtered REST `where` for
   `/nfs/t283_imaging/` type `other` runs before any root request, then the
   result digest equals the digest from the same request after filtered root
   `/` warming.
2. Given the same two request orders, when result counts are compared, then
   counts are identical; `2` versus `87` is a failing regression.
3. Given cache counters are enabled, when the warmed-order request runs, then
   every cache hit key includes path, filter, active set id, and query version.
4. Given a failed anomaly regression, when final perf gates run, then filtered
   Disktree and filtered `where` gates are blocked with detail
   `t283_filtered_rest_order_anomaly`.

### E3: Final Performance Gates

As a product owner, I want cold user-facing paths to pass fixed gates on fixed
datasets, so that completion means first interactions are genuinely faster.

Run final gates on:

- 2026-06-07 four-mount subset at 100,000 lines per mount
- same named mounts at 250,000 lines per mount
- directory-heavy NFS case including `/nfs/t283_imaging/`
- representative Lustre cases `scratch120`, `scratch122`, and `scratch127`

Command shapes use current ClickHouse flag names. Short aliases `-C`, `-D`,
and `-m` are valid but examples use long names.

```bash
timeout 30m ./wrstat-ui clickhouse-perf import <subset-dir> \
  --clickhouse-dsn "$CH_DSN" \
  --clickhouse-database <candidate-db> \
  --owners <owners.csv> --mounts <mountpoints.txt> --maxLines 100000 \
  --batchSize 100000 --parallelism 4 --json <ch-import.json>

timeout 10m ./wrstat-ui clickhouse-perf query \
  --clickhouse-dsn "$CH_DSN" \
  --clickhouse-database <candidate-db> \
  --owners <owners.csv> --mounts <mountpoints.txt> \
  --repeat 20 --warmup 0 --uid 20155 --gids 14976 \
  --dir /nfs/t283_imaging/ --tree-gids 14976 --tree-uids 20155 \
  --tree-types other --json <ch-query.json>

timeout 10m ./wrstat-ui clickhouse-perf rest \
  --base-url <server-url> --repeat 20 --warmup 0 \
  --paths /,/lustre/,/nfs/,/nfs/t283_imaging/ \
  --where-dir /nfs/t283_imaging/ --tree-gids 14976 \
  --tree-uids 20155 --tree-types other --json <ch-rest.json>

timeout 30m ./wrstat-ui bolt-perf import <subset-dir> --out <bolt-out> \
  --quota <quota.csv> --config <basedirs-config> \
  --owners <owners.csv> --mounts <mountpoints.txt> \
  --max-lines 100000 --json <bolt-import.json>

timeout 10m ./wrstat-ui bolt-perf query <bolt-out> --owners <owners.csv> \
  --mounts <mountpoints.txt> --repeat 20 --warmup 0 \
  --dir /nfs/t283_imaging/ --tree-gids 14976 --tree-uids 20155 \
  --tree-types other --json <bolt-query.json>
```

Repeat commands with `--maxLines 250000` or `--max-lines 250000` for the
larger capped subset.

**Package:** `internal/chperf/`, `internal/boltperf/`, `cmd/`
**File:** `internal/chperf/final_gate.go`, `cmd/clickhouse_perf.go`,
`cmd/bolt_perf.go`
**Test file:** `internal/chperf/final_gate_test.go`,
`cmd/clickhouse_perf_test.go`

**Acceptance tests:**

1. Given final reports for the 100k subset, when gates run, then first root
   page refresh server-side p95 is less than 1,000 ms.
2. Given final reports, when `/lustre/` and `/nfs/` first clicks are checked,
   then each server-side p95 is less than 500 ms.
3. Given final reports, when first root filter switch is checked, then
   server-side p95 is less than 1,000 ms.
4. Given final ClickHouse and Bolt reports, when direct filtered
   `/nfs/t283_imaging/` `where` p95 is compared, then ClickHouse p95 is at
   most 86.388 ms, which is 10% slower than Bolt p95 78.534 ms.
5. Given final ClickHouse and Bolt reports, when root filtered `where` p95 is
   compared, then ClickHouse p95 is at most 44.716 ms, which is 10% slower
   than Bolt p95 40.651 ms.
6. Given broad and filtered summaries, child summaries, `has_children`, and
   `where` frontiers, when paired digests are compared, then ClickHouse
   candidate digests match current ClickHouse facts digests.
7. Given table stats, when row amplification is checked, then new objects
   include rows, active parts, compressed bytes, uncompressed bytes, import
   phase time, memory, and amplification versus `wrstat_dir_facts` and
   `wrstat_children`.
8. Given p95 or p99 is statistically worse than the current ClickHouse
   baseline for a non-targeted operation, when final gates run, then the gate
   fails unless the median is already below 5 ms and the regression is within
   the documented 10% noisy-run tolerance.
9. Given `info`, `permission_check`, auth tree, restricted auth `where`, and
   no-auth `where` reports, when final gates run, then correctness digests,
   `NoAuth` flags, and status codes match the current ClickHouse baseline and
   p95 is no more than 10% slower.

Gate these operations at minimum: root `/` Disktree first request, `/lustre/`
and `/nfs/` first clicks, selected mount-root first click, repeated visible
child click, broad and filtered `DirInfo`, broad and filtered `DirInfos`,
broad and filtered `DirsHaveChildren`, broad and filtered whole-mount `Where`,
`Info`, `PermissionAnyInDir`, `PermissionPath`, root filter switch,
`/nfs/t283_imaging/` filtered `where`, real REST/auth tree, restricted auth
tree/`where`, REST `where`, real CLI `./wrstat-ui where`, and browser first
visible Disktree update when harness support exists.

### E4: Tactical Work Is Supporting Only

As an operator, I want metadata batching and warming to reduce incidental
latency, so that they help but do not mask slow cold queries.

Allowed tactical tasks:

- request-scoped active-set metadata, resolved once per request
- batched projection/readiness checks for all active mounts
- startup or provider-update warming for broad `/`, `/lustre/`, `/nfs/`, and
  selected mount-root paths
- response caching keyed by active set, path, filter, permission inputs, and
  endpoint version

These tasks are not success criteria unless paired with cold-path wins from
AgeAll rows, active-prefix rollups, tuple tuning, and navigation facts.

**Package:** `clickhouse/`, `server/`
**File:** `clickhouse/database.go`, `clickhouse/provider.go`,
`clickhouse/database_cache.go`, `server/tree.go`, `server/where.go`
**Test file:** `clickhouse/provider_test.go`, `server/server_test.go`,
`internal/chperf/query_test.go`

**Acceptance tests:**

1. Given startup warming is enabled, when the first measured filtered `where`
   runs from a cold provider state with caches reset, then it still passes the
   same p95 gate without relying on prior warmed request output.
2. Given response caching is enabled, when active set id or filter changes,
   then the cached response is missed and the new response matches facts.
3. Given metadata batching is implemented, when one REST tree request runs,
   then active metadata and readiness checks are counted once per active set,
   not once per child.

## Implementation Order

1. Implement AgeAll rows first (A1-A3). Add DDL, importer writes, readiness,
   cleanup, exact facts equivalence, eligible reader routing, t283 proof, and
   cold filtered `Where`/REST evidence. This is sequential and blocks later
   gates.
2. Implement active-prefix rollups and active-root tuple tuning (B1-B3). Add
   DDL, active-set id, refresh on active changes, readiness, cleanup, root
   routing, AgeAll prefix rows, cache invalidation, and tuple regression tests.
3. Run the navigation decision gate, then implement the selected Disktree
   shape (C1-C3). Default to `wrstat_parent_facts` unless projection or child
   facts meet C1. Add import, readiness, cleanup, reader routing, and endpoint
   proof.
4. Preserve Info and permission/auth behavior, then fill harness gaps and
   tactical support (D1-D2, E1-E4). REST/CLI/Bolt/result-count/query metrics
   are required before final acceptance. Browser fields are specified now; full
   Playwright automation may follow if no low-risk harness exists.

Do not create phase files. Every Go behavior acceptance test above must be a
GoConvey test. Perf gates may be integration tests, `clickhouse-perf` and
`bolt-perf` checks, or documented CI/manual gates, but reports must include the
required command shape and JSON fields.

## Appendix: Key Decisions

- Narrow AgeAll rows are mandatory. Broad all-dimension, GID-only, UID-only,
  age-specific, or full `(gid, uid, file_type, age, dir)` row indexes are
  rejected unless production-scale evidence shows the narrow table cannot meet
  gates.
- Active-prefix rollups are first for virtual/root summaries. The older
  `wrstat_virtual_summary_cache` is deferred because the measured scalar
  active-prefix prototype was 3 rows, 251 compressed bytes, and p50/p95
  2/4 ms.
- Active-prefix rollups must include scalar rows and narrow AgeAll prefix rows
  for `/`, `/lustre/`, and `/nfs/`; they must not store full filter vectors in
  V1 without evidence that AgeAll prefix rows are insufficient.
- Parent facts are the default navigation shape because they are simple,
  import-time, and measured at p50 5 ms with 1 of 6 granules for the
  high-fanout prototype. A projection only counts if real endpoint
  `EXPLAIN indexes = 1` proves reliable use.
- Standalone `where_frontier` and path compression are deferred. The t283 case
  is a poor fit: only 901 of 35,006 directories were single-child dirs
  (2.57%), and the dominant filtered case matched 34,998 dirs.
- Metadata batching, response caching, and warming are tactical only. They may
  reduce incidental work, but cannot be proof that first filter switch or first
  `where` is fixed.
- Hybrid Bolt-like sidecar is a concrete fallback, not the initial path. If
  AgeAll rows, active-prefix rollups, tuple tuning, and navigation facts cannot
  approach same-subset Bolt p95 targets, recommend the least risky sidecar with
  active snapshot atomicity, build time, storage, memory, cleanup, and
  first-query latency gates.
- No hidden Bolt fallback is allowed for production ClickHouse tree routes
  unless the explicit sidecar fallback decision is reached.
- Do not re-litigate raw-DGUTA or file-facts-only tree summaries in this
  implementation path.
- Error policy follows `go-conventions`: wrap errors with `%w`, use
  context-bound queries, keep goroutines cancellable, and keep import memory
  bounded.
