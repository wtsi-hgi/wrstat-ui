# Prompt for ClickHouse schema3 cold tree performance spec

Use this as the source-of-truth input for the spec-writer workflow. It
incorporates the final recommendations from `.docs/schema3/investigate.md`.
The spec-writer should turn these decisions into a cohesive feature spec with
user stories, acceptance tests, implementation phases, and perf gates.

## Goal

Write a schema3 ClickHouse cold tree performance spec for `wrstat-ui`.

The current ClickHouse implementation has already replaced Bolt for filesystem
tree information and includes the schema2 performance objects: canonical
directory facts, children, parent facts, AgeAll filter rows, active-prefix
rollups, virtual children, process-local tree query caches, and server response
caches.

Despite these improvements, first-use cold paths can still be too slow:

- unseen Disktree folders can feel sluggish;
- first `where` can be much slower than retry;
- first `where --dir <subdir>` waits are variable and can rise past 1m;
- repeated requests are fast because caches hide the underlying cold cost.

The schema3 spec must make cold performance consistently fast. It must not
depend on browser cache state, process-local warming, or repeated clicks as the
final answer.

The recommended schema3 design is a ClickHouse-native hybrid:

1. exact-dir plus parent-packet routing over existing `wrstat_dir_facts` and
   `wrstat_parent_facts`;
2. comprehensive all-filter child and dir serving rows for arbitrary and
   age-specific filters;
3. a small active-set virtual overlay for `/`, `/lustre/`, `/nfs/`,
   intermediate virtual parents, and virtual filter summaries;
4. an immutable navigation sidecar fallback only if the ClickHouse-native
   design misses the cold gates.

## Current Schema State

The current branch already includes these schema2 objects:

- `wrstat_dir_facts`: canonical exact directory facts, ordered by
  `(mount_path, snapshot_id, dir)`;
- `wrstat_children`: direct child edges, ordered by
  `(mount_path, snapshot_id, parent_dir, child)`;
- `wrstat_parent_facts`: duplicate directory facts ordered by
  `(mount_path, snapshot_id, parent_dir, dir)`;
- `wrstat_dir_filter_ageall`: flattened AgeAll owner/type rows;
- `wrstat_active_prefix_rollups` and
  `wrstat_active_prefix_filter_ageall`: active-set prefix rollups;
- `wrstat_virtual_children`: active-set virtual namespace children;
- process-local tree query caches and server response caches keyed by active
  set, path, filters, permissions, and query version.

No existing schema2 object should be removed in the first schema3 step.
`wrstat_dir_facts`, `wrstat_parent_facts`, and `wrstat_children` remain
canonical. `wrstat_dir_filter_ageall` remains the canonical fast AgeAll path
until the comprehensive filter layer has a measured replacement.

`wrstat_active_prefix_rollups`, `wrstat_active_prefix_filter_ageall`, and
`wrstat_virtual_children` may become compatibility or diagnostic objects once
the active-set virtual overlay can serve the same totals and child rows.

## Measured Evidence

The schema3 investigation used a bounded mixed Lustre/NFS subset on
2026-06-11.

Artifacts:

- binary: `.tmp/agent/schema3/wrstat-ui`;
- dataset dir: `.tmp/agent/schema3/subset-mixed8`;
- mounts file: `.tmp/agent/schema3/mounts-mixed8.txt`;
- database: `wrstat_schema3_mixed8_094117`;
- import report: `.tmp/agent/schema3/perf/mixed8-import.json`;
- root query report: `.tmp/agent/schema3/perf/mixed8-query-root.json`;
- high-fanout report: `.tmp/agent/schema3/perf/mixed8-query-highfanout.json`;
- REST report: `.tmp/agent/schema3/perf/mixed8-rest-root.json`.

Subset:

| Mount | Input cap |
|---|---:|
| `/nfs/t283_imaging/` | 250,000 |
| `/lustre/scratch120/` | 250,000 |
| `/lustre/scratch122/` | 250,000 |
| `/lustre/scratch123/` | all available, 1 row |
| `/lustre/scratch124/` | 250,000 |
| `/lustre/scratch125/` | 250,000 |
| `/lustre/scratch126/` | 250,000 |
| `/lustre/scratch127/` | 250,000 |

Import evidence:

- 1,750,001 input rows imported in 35.93s wall.
- Max RSS was 831,356 KB.
- `wrstat_files`: 1,750,001 rows, 35.68 MiB compressed.
- `wrstat_dir_facts`: 197,179 rows, 30.03 MiB compressed,
  801.90 MiB uncompressed.
- `wrstat_parent_facts`: 197,179 rows, 28.50 MiB compressed,
  822.50 MiB uncompressed.
- `wrstat_dir_filter_ageall`: 493,588 rows, 11.35 MiB compressed.
- `wrstat_children`: 197,171 rows, 3.99 MiB compressed.

Root semantics were validated:

| Path | Count | Size | Child count |
|---|---:|---:|---:|
| `/` | 1,750,001 | 61,484,536,134,482 | 8 |
| `/lustre/` | 1,500,001 | 61,176,182,464,512 | 7 |
| `/nfs/` | 250,000 | 308,353,669,970 | 1 |

Important cold-path evidence:

| Case | Timing / reads |
|---|---:|
| Root `tree_where_cold_provider` | p50 1,393 ms, p95 1,403 ms |
| REST tree `/` | first 65 ms, then 1.5 ms and 0.3 ms |
| REST `where /`, splits 2 | first 120 ms, then 0.3 ms and 1.6 ms |
| High-fanout parent endpoint | p50 199 ms broad |
| High-fanout parent endpoint, `types=other` | p50 848 ms |
| High-fanout `dirinfos_broad` | p50 63,482 ms, p95 116,770 ms |
| High-fanout `dirshavechildren_broad` | p50 63,273 ms |
| `dirinfos_broad` read volume | 66.6M to 206.6M rows, 25.9GB to 45.4GB |
| `dirinfos_filtered` timeout sample | 458,951 ms, 2.90B rows, 898.6GB |

The high-fanout parent is:

```text
/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/
```

That parent had 11,205 direct children and 92,529 vector entries under those
child rows.

The key finding is that a single parent packet is cheap, but the current public
call shape can reread equivalent parent/child packet work thousands of times:

- exact target dir from `wrstat_dir_facts`: 6-11 ms;
- one high-parent scalar packet from `wrstat_parent_facts`: 8-12 ms;
- one high-parent vector packet from `wrstat_parent_facts`: 18-28 ms;
- focused public child calls can multiply this into 63-117s broad and 459s
  filtered probes.

The spec must address that repeated-read class directly.

## Required Design

### 1. Exact-Dir Plus Parent-Packet Routing

The first schema3 implementation area is a serving-contract change over the
existing canonical tables.

Keep `wrstat_dir_facts` as the canonical exact directory fact table. Use it
for exact current summaries by `(mount_path, snapshot_id, dir)`.

Keep `wrstat_parent_facts` as the canonical broad/default child packet table.
Use it for child summaries and child presence by
`(mount_path, snapshot_id, parent_dir)`.

Do not use a parent packet row as the primary exact current summary for the
requested directory.

The spec must require that these public shapes read each parent packet at most
once per request or provider cache scope:

- `DirInfo`;
- focused or storage-level `DirInfos`;
- `DirsHaveChildren`;
- Disktree endpoint navigation;
- `Tree.Where` frontier traversal.

When a parent packet is read, all child summaries and child counts from that
packet must be cached or returned as a coherent packet. A later
`DirInfo(child)` must not reread the parent sibling range merely to recover one
requested child row.

The packet cache key must include at least:

- mount path;
- snapshot id;
- parent dir;
- filter mode;
- age;
- schema/query version;
- active set id or equivalent active generation when serving virtual rows.

The spec must include acceptance tests proving that the high-fanout focused
`DirInfos` and `DirsHaveChildren` broad paths no longer perform thousands of
independent ClickHouse reads.

### 2. Comprehensive All-Filter Child And Dir Rows

The exact-dir plus parent-packet fix is not comprehensive by itself. Schema3
must add full-filter serving rows so arbitrary filters and age-specific
filters do not fall back to slow vector scans or repeated array unpacking.

Add a parent-keyed table, expected name:

```text
wrstat_child_filter_all
```

It should be keyed for filtered child summaries and child presence, starting
from:

```text
(mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir)
```

unless the spec proves a different ordering is better.

Add a dir/subtree-serving table or reliable projection, expected name:

```text
wrstat_dir_filter_all
```

It should be keyed for `where --dir` subtree scans and exact filtered
directory summaries.

The all-filter layer must include:

- AgeAll rows;
- every age-specific bucket needed by current UI/API filters;
- GID filters;
- UID filters;
- file-type bitmask filters;
- owner+user+type combinations;
- count, size, atime/mtime, and bucket payload needed to match existing
  `DirSummary` behavior.

The design may keep `wrstat_dir_filter_ageall` as the preferred AgeAll path
until the all-filter layer has a measured AgeAll replacement. In the
investigation, the full all-filter subtree row did not beat the existing
AgeAll table for the AgeAll+gid case.

Measured prototype evidence to carry into the spec:

- full vector expansion produced 3,488,307 filter rows, compared with
  493,588 AgeAll rows;
- prototype `wrstat_child_filter_rows`: 3,488,307 rows, 30.11 MiB compressed,
  1.49 GiB uncompressed;
- prototype `wrstat_dir_filter_all`: 3,488,307 rows, 75.26 MiB compressed,
  1.12 GiB uncompressed;
- combined prototype storage: 105.37 MiB compressed;
- target parent AgeAll+gid child-serving p50/p95: 44/77 ms;
- target parent age+gid p50/p95: 37/50 ms;
- target parent age+gid+uid+ft p50/p95: 27/38 ms;
- target subtree `where --dir` age+gid+uid+ft p50/p95: 47/59 ms;
- target subtree AgeAll+gid p50/p95: 77/93 ms, slower than the existing
  AgeAll table.

The spec must measure import CPU, spool bytes, ClickHouse parts, memory, and
cleanup behavior before accepting the new all-filter layer.

### 3. Small Active-Set Virtual Overlay

Schema3 must make virtual roots and mount boxes use the same serving model as
ordinary directories.

Add active-set keyed virtual rows for:

- `/`;
- `/lustre/`;
- `/nfs/`;
- intermediate virtual parents above configured mount roots;
- mount-root boxes shown under those virtual parents;
- full-filter virtual summaries for every filter family supported by the
  all-filter layer.

The active-set id must derive deterministically from the active mount set,
including mount path, snapshot id, and updated-at or an equivalent active
generation input.

Do not physically duplicate every ordinary directory fact into an active-set
serving tree by default. The investigation measured physical active-set
duplication as about 29.13 MiB compressed per active set on mixed8, while the
modeled virtual full-filter overlay was only 6,134 rows.

The spec must require a 100-small-NFS simulation before deciding whether the
small overlay is sufficient. Keep `/nfs` virtual in that simulation; do not
turn `/nfs` itself into the configured mount unless that is the production
shape being tested.

### 4. Import, Spool, Readiness, And Cleanup

Every schema3 object must be built before publish.

The spec must define:

- direct-writer row construction for parent packets, all-filter child rows,
  all-filter dir rows, and active virtual rows;
- production summarise spool files for the new schema3 objects;
- spool manifest counts and checksums;
- ClickHouse insert order;
- readiness rows or active-set markers;
- active publish ordering;
- retry/resume cleanup;
- old partition and old active-set cleanup;
- failure behavior when one derived object cannot be written.

Readers must never see a partial serving layer. Readiness rows must be written
only after all corresponding data for the mount snapshot or active set is
complete and validated.

Avoid post-import `INSERT ... SELECT` rebuilds unless the spec proves they are
bounded, use cleanup deadlines, and cannot leave partial serving data visible.

Snapshot ids and active publish semantics must remain deterministic and atomic.

### 5. Tactical Caching

Cache warming may be implemented only as a tactical improvement. It is not the
final answer.

If a parent range is read, cache the whole directory packet, not only the one
requested child. Cache keys must include filter mode and age; AgeAll cache
entries must not answer age-specific queries.

The spec must measure provider-open cost, active-update cost, memory growth,
and invalidation on snapshot publish/tombstone before enabling broad warming.

## Rejected Or Deferred Designs

The spec must state these design decisions explicitly.

### Directory Packet Table As Primary Storage

Reject as the primary schema3 physical design.

The nested `wrstat_dir_packets` prototype:

- had 50,430 packet rows representing 197,179 child rows and 3,488,307 vector
  entries;
- used 33.15 MiB compressed and 808.23 MiB uncompressed;
- had a max packet size of 11,205 children;
- fetched the target packet at p50/p95 21/42 ms;
- `arrayJoin`ed the target packet at p50/p95 22/43 ms;
- did not beat the existing `wrstat_parent_facts` parent packet;
- still needed a separate filter/subtree serving layer.

### Standalone Where Frontiers

Reject or defer standalone `where_frontiers`.

They are specialized to `where`, do not solve the measured 63s
`DirInfos`/`DirsHaveChildren` failures, and full filter support creates exact
tuple, multi-select, split-version, and query-version complexity.

Schema3 should first make `where` fast through parent-packet routing and
all-filter child/dir rows. Add a separate frontier table only if those gates
fail and a bounded frontier design is proven.

### ClickHouse Bitmap/Postings As Primary Answer

Reject ClickHouse bitmap/posting tables as the primary schema3 answer.

Postings are promising for a sidecar primitive, but by themselves they identify
candidate directories rather than UI summary payloads. Dense broad filters are
also common.

Modeled raw posting sizes on mixed8:

- dimension dir-id lists: 10.4 MB;
- exact AgeAll tuple lists: 2.0 MB;
- exact age-specific tuple lists: 14.0 MB;
- all before summary payloads.

### Cache Warming Only

Reject cache warming as the final solution. The cold filtered focused case hit
459s and 898.6 GB read without cache. Schema3 must make cold routes bounded.

## Sidecar Fallback

If the ClickHouse-native packet plus all-filter plus virtual-overlay design
misses the cold gates, stop adding process-local caches and use an immutable
active-set navigation sidecar.

The preferred fallback is a purpose-built mmap/Roaring sidecar with:

- exact directory packets;
- parent-child adjacency;
- filter postings and prefix payloads;
- virtual rows;
- manifest checksums;
- atomic publish;
- reader grace period;
- cleanup of old versions;
- ClickHouse retained as the source of truth for files, history, basedirs, and
  audit.

SQLite is acceptable as a prototype or audit format. Bolt is the fastest reuse
path, but still needs an active-set aggregate redesign.

Estimated mixed8 sidecar storage from prior Bolt-like evidence is about
0.4-0.5 GB. A purpose-built mmap/Roaring sidecar should target 100-300 MB, but
this must be measured before selection.

## Required Perf Gates

The spec must include these gates and require all new tables/caches to pass
correctness equivalence checks before accepting performance wins.

Datasets/gates must include:

- the mixed8 subset from `.docs/schema3/investigate.md`;
- a larger NFS-heavy subset that can show whether first `where` scales toward
  the reported 33s symptom;
- a 100-small-NFS simulation with `/nfs` kept virtual;
- the scratch125 high-fanout parent
  `/lustre/scratch125/casm/restricted/dbGaP-team219-43354/VCFS/`;
- same-subset Bolt or sidecar comparison where feasible.

Performance gates:

| Scenario | Gate |
|---|---:|
| REST tree `/`, `/lustre/`, `/nfs/` first request | p95 under 500 ms |
| First click into high-fanout parent, broad and filtered | p95 under 500 ms |
| Focused high-fanout `DirInfos`, broad and filtered | p95 under 1s with bounded read rows/bytes/marks |
| Focused high-fanout `DirsHaveChildren`, broad and filtered | p95 under 1s |
| First root `where`, splits 2 | p95 under 1s on bounded subset |
| Real first `./wrstat-ui where --dir` for root, high-fanout, and NFS-heavy dirs | p95 under 1-2s with no warmed REST response cache |
| First filter switch | p95 under 1s |
| Import/summarise | no unacceptable wall-time/RSS/spool regression |

Correctness gates:

- exact summaries match current facts;
- child summaries match current facts;
- `has_children` matches current behavior;
- broad, file-only, UID, GID, type, owner+user+type, AgeAll, and age-specific
  filters match current behavior;
- timestamps, UID/GID/type/age summaries, and time buckets match current
  behavior;
- `/`, `/lustre/`, `/nfs/`, and mount-root virtual boxes keep exact totals;
- `where` result sets and digests match current ClickHouse facts;
- stale or partial cache/table answers are failures, even when fast.

Perf reports must record:

- query count;
- cache hit/miss count;
- ClickHouse read rows/bytes/marks;
- operation result counts;
- result digests;
- JSON and gzip bytes for REST paths;
- p50/p95/p99;
- table rows, active parts, compressed/uncompressed bytes;
- import wall time, peak RSS, spool bytes, part counts, retry cleanup, and
  publish latency.

## Notes

- The import/summarise wall-time and peak RSS regression budget is unknown.
  The spec must determine the acceptable budget through measurement and
  acceptance gates rather than guessing it.
- `wrstat_child_filter_all` and `wrstat_dir_filter_all` are mandatory base
  schema3 tables, not optional candidates or phase-gated alternatives.
- All-filter rows must encode every age-bucket semantic needed to answer all
  current and expected web UI queries and `where` queries exactly, including
  `--unused` and `--unchanged`.
- `wrstat_dir_filter_ageall` must be replaced once the comprehensive filter
  layer proves exact equivalence and better-or-equal acceptance-gate results.
  Do not leave it as an indefinite specialized table.
- Same-subset Bolt or sidecar comparison must be reproduced. If necessary,
  check out the pre-ClickHouse code in scratch under `.tmp/agent/schema3` and
  write temporary bounded code to obtain the comparison.
