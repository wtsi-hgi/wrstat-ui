# Phase 4: Facts + numeric filter tables on `dir_id`

Ref: [spec.md](spec.md) sections D1, D2, D3, D4, E1, E2, E3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

Rewrite `wrstat_dir_facts` keyed by `dir_id`; keep the numeric filter
tables (made numeric, retained by default); implement the in-query
vector-filter path and the per-pattern collapse decision procedure;
rewrite `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where`/`Children`
over integer bands and ranges. Depends on Phases 1-2; runs in
parallel with Phases 3, 5, 6.

The schema items D1/D2 are independent of each other and form a
parallel batch; D3, D4 and the query rewrites E1-E3 depend on the
facts/filter schema being in place.

## Items

### Batch 1 (parallel, after Phase 2 is reviewed)

#### Item 4.1: D1 - `wrstat_dir_facts` keyed by `dir_id` [parallel with 4.2]

spec.md section: D1

Rewrite `clickhouse/schema/005_dir_facts.sql`: keep all current
columns (`all_*`/`file_*` scalar summaries; parallel arrays `gids,
uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, atime_buckets,
mtime_buckets`; `child_count`; `updated_at`; `refreshed_at`) except
replace `dir String` with `dir_id UInt32 CODEC(Delta, LZ4)` and add
`parent_id` + `subtree_end UInt32 CODEC(Delta, LZ4)`; `ORDER BY
(mount_path, snapshot_id, dir_id)` plus a benchmark-gated
`children_proj` ordered by `(mount_path, snapshot_id, parent_id,
dir_id)` for the direct-child fact band. Remove `wrstat_parent_facts`
(direct-child fact reads served by the facts `parent_id` band or the
numeric child-filter table; recursive subtree reads use the
`subtree_end` range; "has children" met by catalog `child_dir_count`).
Verified jointly via D4 / E-section acceptance tests.

- [ ] implemented
- [ ] reviewed

#### Item 4.2: D2 - numeric filter materialisations (retained by default) [parallel with 4.1]

spec.md section: D2

Rewrite `clickhouse/schema/015_child_filter_all.sql`,
`016_dir_filter_all.sql`, `012_dir_filter_ageall.sql` to replace all
`dir`/`parent_dir` strings with `parent_id`/`dir_id` (+ `subtree_end`
for range scans), keeping the existing measure/flag columns;
`wrstat_child_filter_all` `ORDER BY (mount_path, snapshot_id,
parent_id, age, gid, uid, ft, dir_id)`, `wrstat_dir_filter_all`
`ORDER BY (..., age, gid, uid, ft, dir_id)`, `wrstat_dir_filter_ageall`
`ORDER BY (..., gid, uid, ft, dir_id)`. No path string in any hot
row. Verified jointly via D4.2 (no path/dir/parent_dir column).

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 4.3: D3 - in-query vector-filter fallback path [parallel with 4.4]

spec.md section: D3

Implement a code path that reduces the parallel DGUTA arrays of one
facts row (exact) or a `[dir_id, subtree_end)` band (subtree/children)
to filtered aggregates for `(gids, uids, ft, age)` via ClickHouse
`arrayFilter`/`arrayReduce`, selectable per query so it produces
results identical to the materialised-table route. Depends on Items
4.1, 4.2.

- [ ] implemented
- [ ] reviewed

#### Item 4.4: E1 - `Children`/`DirInfo`/`DirInfos` via integer bands [parallel with 4.3]

spec.md section: E1

In `clickhouse/database.go` serve `Children(dir)` (resolve `dir_id`,
read catalog `parent_id = dir_id` band via `children_proj`, return
de-duplicated sorted `full_path`s), `DirInfo(dir, filter)` (facts row,
merge across sources, apply filter, `Modtime` = latest `updated_at`,
`ErrDirNotFound` only if missing from all sources), `DirInfos(dirs,
filter)` (`dir_id IN (...)` batch facts read). Signatures unchanged.
Verified jointly with E2 in `clickhouse/database_test.go`. Depends on
Items 4.1, 4.2.

- [ ] implemented
- [ ] reviewed

### Batch 3 (parallel, after batch 2 is reviewed)

#### Item 4.5: E2 - `DirsHaveChildren`, ancestors, batch full-path [parallel with 4.6, 4.7]

spec.md section: E2

In `clickhouse/database.go`: `DirsHaveChildren(dirs, filter)` (broad
uses catalog `child_dir_count > 0`; filter-aware uses the
`wrstat_child_filter_all` `parent_id` band or in-query band);
ancestors/breadcrumbs + path->id via `parent_id` walk and/or
path_hash/full_path; batch full-path for a page via one catalog
`dir_id IN (...)`. Signatures unchanged. Update existing test file
`clickhouse/database_test.go`. Covers all 5 acceptance tests from E2
(Children dedup/sort/byte-identical, DirInfo merge + latest Modtime,
leaf/missing returns nil/empty, DirsHaveChildren broad+filtered map
matches baseline, breadcrumb ancestors via parent_id walk). Note: E1
acceptance is exercised by these same tests. Depends on Items 4.3,
4.4.

- [ ] implemented
- [ ] reviewed

#### Item 4.6: E3 - `Where` over `dir_id` ranges [parallel with 4.5, 4.7]

spec.md section: E3

In `clickhouse/database.go` serve `Where(dir, filter, recurseCount)`:
resolve `dir_id`, range-scan facts/filter rows in `[dir_id,
subtree_end)`, apply the `recurseCount` threshold per node, return
`DCSs` sorted DESC by Size then ASC by Dir; preserve the default
filter (FT==0 -> AllTypesExceptDirectories). Signature unchanged.
Covers both acceptance tests from E3 (broad+filtered `DCSs` identical
to baseline incl. order; auth-restricted `Where` shows only permitted
dirs). Depends on Items 4.3, 4.4.

- [ ] implemented
- [ ] reviewed

#### Item 4.7: D4 - per-pattern collapse decision (benchmark-gated) [parallel with 4.5, 4.6]

spec.md section: D4

Implement the documented decision procedure and the parity machinery:
for the three routes (filtered exact, filtered children, filtered
subtree) make both the materialised-table route and the in-query
vector route selectable and provably equal; a materialisation is
dropped only when the benchmark (phase 7) proves the in-query path
meets the latency gate for that pattern (unproven collapse is a
failure; retain if a bounded dataset cannot reproduce a pattern).
Test file `clickhouse/filter_parity_test.go`. Covers all 4 acceptance
tests from D4 (bit-for-bit materialised-vs-in-query parity across the
filter matrix, no path/dir/parent_dir column in any filter table,
filtered `DirInfo`/`DirInfos`/`DirsHaveChildren`/`Where` match
baseline, any collapse cites a measurement meeting the gate). Depends
on Items 4.3, 4.4.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill (review all
items in the batch together in a single review pass).
