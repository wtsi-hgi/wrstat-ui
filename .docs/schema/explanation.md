# ClickHouse schema explanation

This note explains the ClickHouse backend design for someone who is new to
ClickHouse. It is based on the original `clickhouse3` branch spec and the
current `clickhouse` branch implementation as of 2026-06-01.

The original spec is here:
https://github.com/wtsi-hgi/wrstat-ui/blob/clickhouse3/clickhouse_spec.md

## The ClickHouse ideas the spec relied on

ClickHouse is not being used like Bolt.

Bolt is a local key/value store. It is good at opening one on-disk database and
doing point lookups or cursor walks in process. ClickHouse is a columnar,
append-oriented analytical database. It is strongest when rows are inserted in
large batches, sorted into table "parts", and queried with predicates that let
ClickHouse skip whole partitions, index ranges, and unused columns.

The initial schema was built around a few ClickHouse principles:

- Partition by coarse lifecycle units, so an old snapshot can be dropped
  cheaply without deleting individual rows.
- Sort by the columns used in common point/range lookups. In ClickHouse this
  `ORDER BY` is the primary sparse index; it is not a uniqueness constraint.
- Put high-selectivity snapshot and path predicates as early as possible,
  often in `PREWHERE`, so ClickHouse can read fewer granules and fewer columns.
- Keep writes append-only. ClickHouse can do mutations, but large deletes and
  updates are expensive compared with inserting a new row or dropping a
  partition.
- Use compression and low-cardinality encodings where many rows repeat the same
  values, such as mount paths and extensions.
- Use the native protocol and explicit batches. Row-at-a-time writes would lose
  the main benefit of moving to ClickHouse.

Those ideas explain most of the original design.

## The initial spec

The first design had one main objective: preserve the existing Bolt-backed API
semantics while storing production-scale filesystem summaries in ClickHouse.

It did that by making every daily import a snapshot. A summarise run for one
mount derives a deterministic `snapshot_id` from `(mount_path, updated_at)`.
All snapshot-owned tables include `mount_path` and `snapshot_id`, and are
partitioned by that pair. The new snapshot only becomes visible when a row is
inserted into `wrstat_mounts`. Readers go through an active-snapshot view, so a
partial import is invisible.

That gives an atomic swap without ClickHouse transactions:

1. Write all rows for a new `(mount_path, snapshot_id)` into inactive
   partitions.
2. Insert a new `wrstat_mounts` row pointing that mount at the new snapshot.
3. Drop old snapshot partitions.

The original raw tree schema was deliberately close to the Bolt model:

- `wrstat_dguta` stored one row per directory summary bucket:
  `(dir, gid, uid, file-type bitmask, age)` plus count, size, min/max times,
  and age-bucket arrays.
- `wrstat_children` stored parent-to-child edges.
- `wrstat_basedirs_*` stored basedirs group/user usage and subdirectory rows.
- `wrstat_basedirs_history` was not snapshot-partitioned because history
  persists across snapshots.
- `wrstat_files` stored file-level rows for the extra ClickHouse-only APIs.
  It used `(parent_dir, name)` rather than a stored full path as the lookup key,
  with `path` as an alias, because exact file and directory queries can then
  follow the table ordering.

The original query shapes follow directly from the table ordering.

Single-mount queries resolve the active snapshot first, then filter by
`mount_path`, `snapshot_id`, and the directory key:

```sql
WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid
SELECT ...
FROM wrstat_dguta
PREWHERE mount_path = ? AND snapshot_id = sid AND dir = ?
```

Ancestor queries, such as browsing `/` when many mountpoints live below it,
cannot use a single mount. They join against the active mount list restricted
by `startsWith(mount_path, ?)`.

File queries split exact paths into `parent_dir` and `name` so `StatPath` and
`IsDir` can use the `wrstat_files` sort key instead of scanning a whole mount
partition. `ListDir` uses `parent_dir = ?`. Recursive glob queries use
`parent_dir` prefix ranges plus `match(...)` only where needed.

In short: the initial spec used ClickHouse well for the problem it thought it
had. It made the daily snapshot the partition boundary, made the UI's exact
directory/file lookups match table sort keys, and kept write-side visibility
append-only.

## Current state

The current implementation still keeps the core snapshot model. The active
snapshot pointer, `(mount_path, snapshot_id)` partitioning, native batch writes,
columnar file ingest, and path-oriented sort keys are all still central.

What changed is that production data showed the raw schema was not enough for
all UI paths. The expensive cases were not simple `DirInfo("/some/mount/dir")`
lookups. They were cold `where` runs, Disktree folder clicks, broad
`DirsHaveChildren` checks, arbitrary UID/GID/file-type filters, and very
directory-heavy imports such as `t283_imaging`.

### Active snapshots now have tombstones

The original active view just picked the latest `wrstat_mounts` row. Failed
imports and retries made that too simple. A deterministic snapshot can be left
active even though the local completion marker was never written.

`wrstat_mounts` now has an `active UInt8` column. Cleanup writes inactive
tombstone rows instead of deleting active rows. Production code reads
`wrstat_mounts_active_v2`, whose `argMax` ordering handles inactive rows and
same-millisecond `switched_at` ties. The old `wrstat_mounts_active` view is
still created or preserved for compatibility, but current Go queries use
`wrstat_mounts_active_v2`.

This is still a ClickHouse-friendly design because recovery is append-only:
publish and cleanup are both represented by inserted metadata rows.

### Maintained tree projections became core

The biggest design change is that raw `wrstat_dguta` is no longer the only fast
tree query source.

The current schema has per-active-mount projections:

- `wrstat_dir_summary`
- `wrstat_dir_summary_sets`
- `wrstat_dir_dguta_vector`

`wrstat_dir_summary` stores one row per mount directory and age for the broad
filters the UI uses constantly. It also stores a file-only summary and
`child_count`. That lets common unfiltered/default-file tree requests and broad
`DirsHaveChildren` checks avoid scanning raw DGUTA rows or joining children to
DGUTA.

`wrstat_dir_dguta_vector` stores one row per mount directory, with aligned
arrays for gids, uids, file types, ages, counts, sizes, times, and age buckets.
It is effectively a compact per-directory DGUTA bundle. The Go reader can scan
one row and evaluate arbitrary filters in process, which is often much cheaper
than asking ClickHouse to aggregate many raw rows for every Disktree click.

`wrstat_dir_summary_sets` is the readiness marker. The writer inserts it only
after the streamed projection rows have been flushed successfully. Readers only
trust the projection when the marker exists at the expected
`summary_version`.

There are also active-tree projection tables:

- `wrstat_tree_summary_sets`
- `wrstat_tree_dguta`
- `wrstat_tree_children`
- `wrstat_tree_dir_summary`

These are keyed by a fingerprint of the active mount set. They support
ancestor/virtual tree paths above mount roots, where a single `(mount_path,
snapshot_id)` projection cannot answer the question by itself.

### Projection writes moved out of ClickHouse backfills

One intermediate design rebuilt projections with large
`INSERT ... SELECT ... FROM wrstat_dguta` queries. That was coherent from a SQL
point of view, but it was too expensive on production data and could fail under
normal query timeouts.

The current design writes `wrstat_dir_summary` and `wrstat_dir_dguta_vector`
directly from the Go `RecordDGUTA` stream during import, before the snapshot is
published. This turns the projection tables into first-class import outputs,
not background repairs.

The first direct-write version accumulated too much state in Go. The current
writer streams projection rows per record and sends capped batches during
`Add`. The readiness marker still waits until `Close`, so readers never see a
half-built projection as ready.

### Raw DGUTA rows are now compacted for internal mount directories

The latest performance change is important: for internal directories within a
non-root mount, raw `wrstat_dguta` no longer stores every age bucket row. It
keeps the `DGUTAgeAll` row, while `wrstat_dir_dguta_vector` keeps the full age
detail. The maintained `wrstat_dir_summary` rows for those internal directories
are compacted in the same spirit: the all-age summary remains the cheap broad
path, while age-specific detail is served from the vector.

This was done because `t283_imaging` has a very directory-heavy shape. It was
generating many age rows per logical directory in both raw DGUTA and maintained
summary output. For that dataset, age-row amplification was the performance
bug.

The design consequence is that `wrstat_dguta` and `wrstat_dir_summary` are no
longer complete sources for every age-specific mount-directory query. Query
routing must use the vector projection for non-all-age and arbitrary-filter
paths whenever internal age compaction may apply. The current code does that
for the main `DirInfo`, `DirInfos`, `DirsHaveChildren`, and filtered
mount-root `where` paths.

This still makes good use of ClickHouse because it reduces write volume and
keeps common reads on snapshot-scoped, ordered, narrow tables. The cost is that
the read-routing rules are now part of the schema design, not just an
implementation detail.

### Queries became adaptive

The original query model was mostly one query shape per API method. The current
tree reader chooses among several paths:

- maintained summary for broad all/default-file filters
- maintained DGUTA vector for arbitrary filters and age-specific mount queries
- grouped raw DGUTA summaries for large fallback batches
- raw DGUTA point lookups for small fallback cases
- children-only checks for broad `has_children`
- child-count projection checks when available
- active-tree summary tables for ancestor paths
- virtual ancestor fallbacks when no stored ancestor row exists

There is also a bounded process-wide query cache keyed by ClickHouse config,
mount path, snapshot id, directory, filter age, and summary mode. This matters
because repeated web interactions happen in one server process, while CLI
`where` runs need cold-provider perf coverage.

The adaptive strategy is less elegant than the initial spec, but it reflects a
real ClickHouse lesson: tiny indexed point lookups are fast, large analytical
aggregates are fast, but thousands of small round trips or repeated raw
aggregations over hot UI paths are not.

### Import behaviour changed for reliability

The spec recommended 100,000 to 500,000 row batches. In production, some native
insert batches were too long-lived or too large for directory-heavy rows and
long imports.

The current code still exposes a 100,000 default batch size at the top level,
but raw DGUTA, children, and projection inserts are capped to smaller effective
blocks, currently 10,000 rows. Batches are sent discretely and freshly prepared
again. Import batch contexts are detached from normal foreground query
timeouts, and import connections get safer dial/read/lifetime/open/idle floors.

This is still aligned with ClickHouse. The important principle is not "always
100k rows"; it is "write bounded blocks through the native protocol, avoid
row-at-a-time inserts, and avoid creating pathological part or connection
lifetimes".

### File APIs remain close to the original spec

`wrstat_files` still follows the original layout: partition by active snapshot,
order by `(mount_path, snapshot_id, parent_dir, name)`, derive `path` as an
alias, and use columnar inserts.

The query layer has improved field selection. Callers can request narrow
columns for perf-sensitive file operations, so ClickHouse can avoid reading and
returning unused metadata columns.

One caveat: the table defines a secondary skip index on `ext`, but current glob
queries do not filter on `f.ext`. Extension-limited glob cases still use regex
matching against `name` or `path`. That index therefore looks unused in the
current query set.

## Are we still making good use of ClickHouse?

Yes, mostly.

The current design still uses ClickHouse's strengths:

- append-only snapshot publication instead of transactional rewrites
- partition drops instead of row-by-row old snapshot cleanup
- ordered tables for exact directory, parent, and file lookups
- columnar reads and narrow field selection
- native protocol batch ingest
- compression on repeated strings and numeric time/count columns
- precomputed projection tables for hot aggregate shapes

The parts that feel less cohesive are not anti-ClickHouse so much as signs of
evolution under pressure. `wrstat_dguta` started as the canonical raw truth,
then became partly compacted. `wrstat_dir_summary` and
`wrstat_dir_dguta_vector` started as performance projections, then became
required for correctness under age compaction. `wrstat_mounts_active_v2` fixed
real cleanup races, but left a legacy active view beside it. Import batch sizes
were tuned away from the simple spec recommendation.

The next design pass should make those evolved rules explicit: projections are
part of the canonical read model, raw DGUTA has a documented compacted shape,
active tombstones are first-class, and schema migrations are intentional rather
than a chain of narrowly named fixes.

See `.docs/schema/prompt.md` for suggested follow-up work.
