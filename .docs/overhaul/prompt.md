# Prompt for ClickHouse storage + ingest + query overhaul spec

Use this as the source-of-truth input for the spec-writer skill workflow. The
target output is `.docs/overhaul/spec.md`, with numbered implementation-phase
documents in `.docs/overhaul/` (`phase1.md`, `phase2.md`, ...).

This is a **radical, full** overhaul, not a migration. The `clickhouse` branch
has never been released. There are **no backwards-compatibility concerns**: no
data to migrate, no on-disk format to preserve, no API consumers outside this
repo. The spec should pursue the maximal-benefit design end to end, accepting
substantial up-front rework, and then **prove with measurement** whether it is
faster or slower than the current implementation across every query type.

---

## Goal

Today the `clickhouse` package persists **path text as strings, billions of
times, across ~18 tables**. The same logical directory path is written once per
file (as `parent_dir`), once per directory fact, twice per parent fact, once per
child edge, and then again multiplied by every `(age, gid, uid, ft)` filter
combination in the derived filter tables. The result is well over 2 billion
stored path strings, dominated by (a) `wrstat_files.parent_dir` repeated on every
one of ~1.3B file rows and (b) the multi-billion-row `wrstat_dir_filter_all` /
`wrstat_child_filter_all` / `wrstat_dir_filter_ageall` tables. This bloats
storage, slows ingest (the spool carries the same duplicated text), slows merges,
and forces queries to scan and compare long strings.

The spec must design a single coherent system in which **a directory path is
stored exactly once per mount snapshot**, every other table refers to directories
by a compact integer id, and the three core hard problems become near-instant:

1. **Deduplication** — no path string is stored more than once per snapshot.
2. **Parent / child relationship finding** — listing children, testing "has
   children", and walking ancestors are integer lookups, not string scans.
3. **Full-path / full-text determination** — reconstructing a directory's full
   path is an O(1) catalog read, and full-text path search is a small-table skip
   index, not a scan over billions of rows.

The same redesign aims to make **ingest (summarise) faster too**: the new
structure should be generated as a near-free byproduct of the existing
depth-first tree walk, and the spool stops carrying duplicated path text. This is
a goal to be confirmed by measurement, not an assumption (see area B) — an import
regression is acceptable if it is reported and the query/storage wins justify it.

### The core idea: preorder interval labelling (an immutable nested-set trie)

The data for a mount snapshot is immutable once written, read-heavy, and
naturally a tree. The optimal known representation for that shape is **interval
(nested-set / preorder) labelling**:

- During the summariser's depth-first walk, assign each directory a dense
  integer `dir_id` in **preorder** (first-visit order), starting at the mount
  root. Record `parent_id` and `subtree_end` (the smallest `dir_id` greater than
  every descendant's id; equivalently `dir_id` of the next preorder node that is
  not a descendant).
- Then, for any directory, **its entire subtree is the contiguous integer range
  `[dir_id, subtree_end)`**. Its direct children all share `parent_id = dir_id`.
- Store the actual path string **once**, in a directory catalog keyed by
  `dir_id`. Every other table (files, facts, basedirs, etc.) stores `dir_id`
  (a `UInt32`/`UInt64`), never the path.

This single move makes all of the following collapse to integer primary-key
operations on a `MergeTree` ordered by `dir_id`:

| Access pattern | Today | With interval labelling |
| --- | --- | --- |
| Exact directory summary | `WHERE dir = '...'` string match | `WHERE dir_id = ?` |
| Subtree / recursive (`where`, Disktree) | `startsWith(dir, prefix)` scan + string compare, or a precomputed exploded filter table | `WHERE dir_id >= ? AND dir_id < ?` contiguous granule range |
| List direct children | `wrstat_children` string edges | `WHERE parent_id = ?` (ordered projection) |
| "Has children" | join/scan children | `child_dir_count > 0` column on the catalog row |
| Ancestors / breadcrumbs | split string, repeated lookups | walk `parent_id` chain in the catalog |
| Full path of a dir | stored redundantly everywhere | one `full_path` copy in the catalog (O(1)) |
| Files under a dir | `WHERE parent_dir = '...'` | `WHERE dir_id = ?` (direct) or range (recursive) |

Interval labelling also changes the *filter* story, but here the design is
deliberately conservative. Directory summaries in wrstat are already **recursive
roll-ups** (the `wrstat_dir_facts` row for `/a/b/` already aggregates everything
beneath it, with a per-`(gid,uid,ft,age)` breakdown held in parallel arrays), and
the existing exploded filter tables (`*_filter_all`, `*_filter_ageall`) exist to
make filtered subtree scans and high-fanout child listings fast. There is a
tempting hypothesis that integer ranges make those tables unnecessary — that a
filtered subtree query can be served by an integer-range scan filtering the
per-directory vector in-query. **That hypothesis is exactly the bet that the
earlier schema work already found too slow over string keys, so this overhaul
does not bet the design on it.** Instead:

- **Default:** keep the filter materialisations, but make them `dir_id`-keyed and
  numeric (filter dimensions in the sort key, child/dir `dir_id` instead of path
  strings). This preserves the proven low-latency serving model for the
  filtered/high-fanout query class while still removing all path-string
  duplication.
- **Optimisation, per pattern, benchmark-gated:** where — and *only* where — the
  before/after study proves that in-query vector filtering over an integer range
  meets the latency gate for a specific filtered pattern, collapse that
  materialisation away. Every such collapse must cite the measurement that
  justified it; an unproven collapse is a spec failure.

The big, *unconditional* wins (dedup, parent/child, exact lookup, broad subtree,
full-path) come from interval labelling regardless. The filter-table collapse is
treated as an upside to be earned by measurement, not a foundational assumption.

---

## Important product direction

- **No migration, no compatibility shims, no dual-read/dual-write.** Replace the
  schema and the writers wholesale. Old SQL files under `clickhouse/schema/` that
  no longer apply should be removed or rewritten, not retained for fallback.
- **Do the full thing.** Prefer the maximal-benefit design even where it means
  rewriting the summariser output path, the spool format, the writers, and the
  query layer together. Do not stage it as a cautious "prove the riskiest 5%
  first" migration. We accept that some up-front work may turn out not to pay off;
  the deliverable includes finding that out by measuring.
- **But measurement is mandatory.** The one non-negotiable acceptance gate is a
  rigorous **before/after benchmark across every query type** (and ingest, and
  storage), comparing the overhaul to the current `clickhouse` branch HEAD.
  Slower outcomes are acceptable to ship *only* if correctness holds and the
  regression is quantified, explained, and within the absolute UX gates below;
  any per-query regression must be reported, not hidden.
- This overhaul **supersedes and unifies** the incremental `.docs/schema`,
  `.docs/schema2`, and `.docs/schema3` layering (which kept *adding* string-keyed
  tables and thus *added* duplication) and the `.docs/summarise` write-dedup fix.
  A parallel `.docs/trie` effort proposes the same integer-id / preorder-interval
  core for the query schema. This overhaul **shares that core deliberately** and
  adopts its safer serving stance — *keep* numeric `dir_id`-keyed filter
  materialisations rather than betting the design on collapsing them — while
  adding three things the trie sketch does not commit to: (1) `dir_id` assignment
  integrated into the summariser's existing walk, (2) a spool/write path that
  carries ids instead of repeated path text, and (3) catalog-side text
  skip-indexes for full-path/glob search. **Treat this prompt as self-contained**;
  the spec writer should not depend on those other documents, but where this
  prompt and the trie design agree, that agreement is intentional.

---

## Current state to replace

The spec must open with an accurate description of what exists, so the rewrite is
unambiguous. Key facts established by investigation (verify against code):

### Tables and where path text is duplicated

- `wrstat_files` — `ENGINE = MergeTree`, `PARTITION BY (mount_path, snapshot_id)`,
  `ORDER BY (mount_path, snapshot_id, parent_dir, name)`, ~1.3B rows. Columns
  `parent_dir String` and `name String`; `path` is a non-stored
  `ALIAS concat(parent_dir, name)`. **`parent_dir` (a full directory path) is
  repeated on every file row** — the single largest source of duplicated text.
  See [clickhouse/schema/011_files.sql](clickhouse/schema/011_files.sql).
- `wrstat_dir_facts` — one row per directory; `dir String` holds the full path;
  carries the **compact recursive DGUTA vector** as parallel arrays
  (`gids`, `uids`, `fts`, `ages`, `counts`, `sizes`, `atime_mins`, `mtime_maxs`,
  `atime_buckets`, `mtime_buckets`) plus scalar `all_*`/`file_*` summaries and
  `child_count`. `ORDER BY (mount_path, snapshot_id, dir)`. See
  [clickhouse/schema/004_dir_facts.sql](clickhouse/schema/004_dir_facts.sql).
- `wrstat_parent_facts` — duplicates dir facts keyed by `(parent_dir, dir)` for
  fast child-packet reads. Stores **both** `parent_dir` and `dir` as full paths.
- `wrstat_children` — `(parent_dir, child)` edges; ~one row per directory-child
  pair (estimated multi-billion rows on large filesystems).
- `wrstat_dir_filter_all`, `wrstat_child_filter_all`, `wrstat_dir_filter_ageall`
  — **the row-explosion tables**: one row per directory per `(age, gid, uid, ft)`
  combination, each storing `dir` (and `parent_dir`) as full strings. These are
  the multi-billion-row tables and the second-largest duplication source.
- `wrstat_basedirs_*` — `(basedir, subdir)` string pairs (ZSTD(3)).
- Active-set overlay/rollup tables (`wrstat_active_virtual_*`,
  `wrstat_active_prefix_*`, `wrstat_virtual_children`) — `dir` / `parent_dir`
  strings keyed by `active_set_id`, for the cross-mount virtual namespace
  (`/`, `/lustre/`, `/nfs/`).

### Ingest pipeline

- `stats` parses the wrstat stat file into a `FileInfo` stream.
- `summary.Summariser` walks the stream **depth-first**, maintaining a
  `DirectoryPath{Name, Depth, Parent *DirectoryPath}` linked list
  ([summary/summariser.go](summary/summariser.go)). Full paths are materialised
  lazily by `AppendTo` walking parent pointers. **There is no existing trie or id
  assignment** — paths are passed as strings/bytes.
- `summary/dirguta` accumulates per-directory DGUTA roll-ups and rolls children
  into parents on `Output()`.
- `clickhouse` writers (`dguta_writer.go`, `file_ingest_operation.go`,
  `mount_dir_projection_writer.go`, `import_block_writer.go`) write the tables
  above; the spool (`internal/chspool`) gob+gzips rows **with full path strings**,
  one file per table; `summarise_spool_loader.go` replays them into ClickHouse.

### Query surface (the contract to preserve)

`db.Database` and its extensions ([db/interfaces.go](db/interfaces.go)):
`DirInfo`, `Children`, `Info`, `DirInfos` (batch), `DirsHaveChildren` (batch),
`Where` (filtered subtree). Plus the `clickhouse.Client` file API
([clickhouse/file_api.go](clickhouse/file_api.go)): `StatPath`, `ListDir`,
`FindByGlob`, `CountByGlob`, `IsDir`, `PermissionPath`, `PermissionAnyInDir`. Plus
basedirs readers (`GroupUsage`, `UserUsage`, `GroupSubDirs`, `UserSubDirs`,
history). **External behaviour of all of these must be unchanged** (same results,
ordering, filter semantics, multi-source/active-mount merge, full paths returned).

---

## Required design areas

The spec must address each area below with: the concrete schema/DDL or Go design,
the algorithm, and BDD-style acceptance criteria. Where a choice is empirical
(e.g. integer width, whether to keep a helper projection), state the hypothesis
and require the benchmark to settle it.

### A. Directory catalog — the single home for path text

Design `wrstat_dirs` (name negotiable), one row per directory per
`(mount_path, snapshot_id)`:

- `dir_id` (preorder integer; choose `UInt32` vs `UInt64` based on max dirs per
  snapshot — justify), `parent_id`, `subtree_end`, `depth`.
- `name` (basename only) and `full_path` (the **one** stored copy of the path).
- `child_dir_count`, `child_file_count` (so "has children" is a column read).
- Optional `path_hash` for O(1) path→id resolution; if used, the spec must
  require full-path verification on hit to defend against collisions.
- `ENGINE = MergeTree`, `ORDER BY (mount_path, snapshot_id, dir_id)`.
- Helper projections / secondary orderings (inside the same table, not new
  tables): `(mount_path, snapshot_id, parent_id, dir_id)` for child listings;
  `(mount_path, snapshot_id, full_path)` or `path_hash` for path→id resolution.

Acceptance: every distinct directory in a snapshot appears exactly once;
preorder + `subtree_end` invariant holds (`descendant ⟺ dir_id ∈ [root_id,
subtree_end)`); `parent_id` of the mount root is a sentinel; full path of any
`dir_id` is reconstructable both via the stored `full_path` (O(1)) and via the
`parent_id` walk (cross-check they agree).

### B. ID assignment during summarise (deterministic, streaming)

The intended approach assigns the preorder `dir_id` as a counter during the
summariser's existing depth-first walk, fixing `subtree_end` when a directory's
subtree completes (when it is popped) — using only an ancestor stack (depth
≤ ~128) and no full-tree buffering. **This rests on an assumption the spec must
validate, not assume: that the input stat stream is already DFS-contiguous**
(every directory's descendants arrive as one uninterrupted run, which the
existing streaming child→parent roll-up in `summary/dirguta` implies but does not
guarantee — note in particular how files can interleave with sibling
subdirectories under lexicographic ordering). The spec must:

- prove the stream order yields correct preorder ids and subtree intervals, or
  define the deterministic step that establishes it (e.g. a bounded reorder, a
  stack over lexicographic paths, or an intermediate sort), per the same concern
  the trie design flags;
- produce ids **deterministically** (same input → same ids, required for
  reproducible spool and tests);
- **measure**, not assume, the import cost. The headline claim is that ids are
  near-free in the existing walk and the leaner spool makes ingest *faster*; the
  benchmark must confirm or refute this. An import regression is acceptable if
  query/storage wins justify it, but it must be reported, not hidden behind a
  "free during the walk" assertion.

Acceptance: correct, deterministic, gap-free ids proven by test (including a case
with files interleaved among subdirectories); the interval invariant from area A
holds on real datasets; assignment integrated into `summary.Summariser` /
`summary/dirguta` output so the id is available to all writers; import wall
time/CPU/RSS reported vs baseline.

### C. Files table rewrite — `dir_id` instead of `parent_dir`

`wrstat_files` keeps `name` and all stat columns but replaces `parent_dir`
(string) with `dir_id` (integer). `ORDER BY (mount_path, snapshot_id, dir_id,
name)`. `StatPath`/`IsDir`/`PermissionPath` resolve path→`dir_id` via the catalog
then do a `(dir_id, name)` point lookup. `ListDir` is `WHERE dir_id = ?`.
Recursive file enumeration is `WHERE dir_id >= ? AND dir_id < ?`. Full path for
display is the catalog `full_path` + `name`.

Acceptance: results identical to current `StatPath`/`ListDir`/etc.; the largest
table no longer stores any directory path string.

### D. Numeric filter tables (default), collapse only where measured

The default is **not** to delete the filter materialisations. The earlier schema
work added `wrstat_dir_filter_all`, `wrstat_child_filter_all`, and
`wrstat_dir_filter_ageall` precisely because serving filtered/high-fanout queries
from the per-directory vector was too slow, and this overhaul does not gamble the
filtered-query latency class on the assumption that integer ranges alone close
that gap. Instead:

- **Keep the filter materialisations, made numeric.** Replace their string keys
  with ids: filtered child facts keyed by `(parent_id, age, gid, uid, ft, child
  dir_id)`; filtered dir/subtree facts keyed by `dir_id` for exact lookups and by
  the `(dir_id, subtree_end)` range for subtree scans, with the filter dimensions
  in the sort key. No `dir`/`parent_dir` strings in any of these hot rows. This
  removes the duplication (the dominant storage problem in these tables was the
  repeated path text) while preserving the proven low-latency serving model.
- **Specify the in-query fallback path too.** The facts table retains the
  parallel-array DGUTA vector, so filtered queries *can* also be answered by
  in-query `arrayFilter`/`arrayReduce` over an integer range. Implement this as an
  available code path so the benchmark can compare it head-to-head against the
  materialised tables per pattern.
- **Collapse per pattern, benchmark-gated.** Where — and only where — the
  before/after study proves the in-query path meets the latency gate for a
  specific filtered pattern (a given filter shape × dataset × fanout), drop that
  materialisation. Each collapse must cite the measurement; an unproven collapse
  is a spec failure. The likeliest safe collapse is filtered *exact* `DirInfo`
  (a single facts row); the likeliest table to retain is the high-fanout filtered
  child/subtree serving rows.

The spec must specify the exact facts-table layout (keep the parallel-array DGUTA
vector; decide whether to also keep narrow scalar `all_*`/`file_*` columns), the
numeric filter-table DDL, the in-query filter expressions, and the per-pattern
decision procedure (which table is kept vs collapsed, and the measurement that
decided it).

Filtered query routes covered:

- **Filtered exact summary** (`DirInfo(dir, filter)`): `dir_id` lookup against the
  numeric dir-filter rows, or in-query vector filter of the single facts row.
- **Filtered child summaries** (`DirInfos` of a parent's children,
  `DirsHaveChildren`): the numeric `parent_id`-keyed child-filter band, or the
  contiguous `parent_id` band of child facts rows filtered in-query. High-fanout
  parents (e.g. 11k children) read one contiguous range, not 11k scattered reads.
- **Filtered subtree** (`Where`, Disktree, `where --dir`): the numeric
  `(dir_id, subtree_end)` range over the dir-filter rows, or a range scan of the
  facts table emitting per-node filtered aggregates above the recurse threshold.

Acceptance: filtered results bit-for-bit match current outputs across the
benchmark filter matrix (gid, uid, ft, age, and combinations); no filter table
stores a path string; every retained-vs-collapsed decision is backed by a cited
measurement; latency gates in the benchmark section met for every filtered
pattern.

### E. Parent / child / ancestor / full-path resolution

Specify the integer algorithms and the supporting Go (caching) layer:

- `Children(dir)`: resolve `dir_id`, read `parent_id = dir_id` band, return
  `full_path` (or basename) per child, sorted — semantics unchanged.
- `DirsHaveChildren`: read `child_dir_count` (filter-aware variant uses the band).
- Ancestors/breadcrumbs and path→id: `parent_id` walk and/or `path_hash`/
  `full_path` projection.
- Full-path determination: O(1) via stored `full_path`; for batches of file rows,
  one catalog join/`dir_id IN (...)` resolves a whole result page.

Acceptance: `Children` de-dup/sort/multi-source semantics preserved; full paths
returned to callers are byte-identical to today.

### F. Full-text / glob path search

`FindByGlob`/`CountByGlob` must stay correct while no longer scanning path text
on the 1.3B-row files table. Design:

- Basename globs filter `wrstat_files.name`.
- Full-path globs/substrings resolve against the **small** directory catalog
  using ClickHouse data-skipping indexes (`ngrambf_v1` / `tokenbf_v1`) on
  `full_path`, producing a set of `dir_id`s (or `[dir_id, subtree_end)` ranges),
  then intersect with the files table by `dir_id`.
- Preserve gitignore-style multi-pattern semantics, the `uid`/`gid` permission
  filter, ordering, and pagination.

Acceptance: glob results and counts identical to current across the existing glob
benchmark cases (direct-child, recursive, extension, dotfile); the files table is
no longer scanned for directory-path matching.

### G. Basedirs and the active virtual namespace in the id world

- Basedirs tables reference `dir_id` for in-snapshot `basedir`/`subdir`; specify
  the fallback for any path not present in the snapshot catalog (explicit
  external/string row, clearly marked) so historical/quota data still resolves.
- The cross-mount virtual namespace (`/`, `/lustre/`, `/nfs/`, intermediate
  virtual parents, mount-root boxes) spans multiple per-mount id spaces. Design a
  per-`active_set_id` virtual catalog with its **own** id space whose nodes link
  to the underlying mount-root `dir_id`s, so virtual navigation and folded
  active-prefix summaries work **without** copying every mount-local path into a
  second string namespace. Preserve the existing virtual/active behaviour.

Acceptance: root `/` aggregates every selected active Lustre and NFS mount with
separate `/lustre` and `/nfs` boxes and correct totals; virtual children,
filtered virtual summaries, and active-prefix rollups match current outputs.

### H. Ingest pipeline and spool carrying ids, not strings

- The spool format carries `dir_id` integers and **one** catalog stream per
  mount snapshot (`dir_id, parent_id, subtree_end, depth, name`, and `full_path`
  once), not repeated path strings in every row. Expect a large reduction in
  spool bytes; the benchmark must quantify it.
- Writers (`dguta_writer.go`, `file_ingest_operation.go`,
  `mount_dir_projection_writer.go`, `import_block_writer.go`,
  `summarise_spool_loader.go`) emit/load the new id-based rows. Keep the
  resumable/retry and readiness-set machinery; update its row counters.
- Preserve the `DGUTAWriter` configuration contract in
  [db/interfaces.go](db/interfaces.go) (`Add`, `SetMountPath`, `SetUpdatedAt`,
  `SetBatchSize`, `Close`) as far as practical; if signatures must change to pass
  ids, change them cleanly (no compatibility layer needed).

Acceptance: a full summarise → spool → load cycle reproduces correct query
results; spool bytes and ingest wall-time reported vs baseline.

### I. (Optional, evaluate) in-memory navigation index

Because the catalog is integer-keyed and modest (≈10–100M dirs ⇒ low-GB), the
spec may *propose and evaluate* loading a compact in-process index
(`dir_id → parent_id, name, subtree_end, child counts`) into the server so that
parent/child/has-children/ancestor/full-path navigation is answered **without a
ClickHouse round-trip**, leaving ClickHouse for the heavy DGUTA aggregations.
Treat this as an optional radical extension: include the design and a memory/build
cost estimate, gate it behind a flag, and let the benchmark decide whether it
earns its place. Do not let it block the core schema work.

---

## Performance comparison requirements (the mandatory gate)

The spec must define a rigorous before/after study. This is the primary
acceptance artefact.

- **Baseline = current `clickhouse` branch HEAD**, captured *before* any overhaul
  change (preserve a built binary or a separate worktree). Re-use and extend the
  existing harness in [internal/chperf/](internal/chperf/) (`import.go`,
  `query.go`, `final_gate.go`, `clickhouse_api.go`) and the Bolt comparison
  harness. Do **not** invent a new report format.
- **Datasets**: at minimum a mixed Lustre/NFS subset; a directory-heavy NFS tree;
  a high-fanout parent (~11k direct children, e.g. a `.../VCFS/`-style dir); a
  many-small-mounts active-virtual simulation keeping `/nfs` virtual; and the
  largest practical production-like local dataset. State exactly which datasets
  are used and why a bounded subset still reproduces each access pattern.
- **Import / storage metrics**: summarise wall time, CPU, max RSS; spool bytes;
  ClickHouse bytes per table (compressed *and* uncompressed); part counts; rows
  written per table. Report the duplication reduction explicitly (path text bytes
  before vs after).
- **Query metrics per type**: server-side p50/p95/p99 latency, rows read, bytes
  read, granules/marks read, result rows, and a result digest for correctness.
- **Every query type must be compared.** The matrix below is the canonical set
  derived from the current query layer; the spec must benchmark each and report a
  per-type before/after delta. Missing a type is a spec failure.

| Group | Query types to benchmark |
| --- | --- |
| Exact directory | `DirInfo` broad; `DirInfo` filtered (gid / uid / ft / age and combos) |
| Batch directory | `DirInfos` broad; `DirInfos` focused/high-fanout; filtered variants |
| Children / presence | `Children`; `DirsHaveChildren` broad; `DirsHaveChildren` filtered |
| Subtree / recursive | `Where` broad; `Where` filtered; `where --dir`; auth-restricted `Where` |
| Disktree navigation | root `/`; `/lustre/`; `/nfs/`; a mount root; a high-fanout parent; with type/owner/age filters |
| File API | `StatPath`; `ListDir`; `IsDir`; `PermissionPath`; `PermissionAnyInDir` |
| Glob / full-text | `FindByGlob` direct-child, recursive, extension, dotfile; `CountByGlob` |
| Virtual / active | active virtual root summaries; active virtual filtered summaries; virtual children; active-prefix rollups |
| Basedirs / quota | `GroupUsage`; `UserUsage`; `GroupSubDirs`; `UserSubDirs`; history |
| Maintenance | import readiness/publish; active-snapshot cleanup; `Info` |

- **Cache scopes**: reproduce the existing harness's cold/warm/fresh-provider
  scopes (fresh provider, cold provider, same-provider-cold, same-query-client,
  ancestor-dirs, new-dir-each-repeat, visible-child-dirs, startup audit) so the
  high-fanout repeated-read pathology is exercised before and after.

### Gates

- **Correctness (hard):** every query type returns results identical to baseline
  — same paths, child ordering, full paths, filter/permission semantics,
  multi-source/active merge, glob semantics, basedirs/quota numbers, and
  manifests. No wrong row may ever be returned (path-hash collisions verified).
- **Absolute cold UX (hard):** exact dir resolution, exact file stat, permission
  path, direct-child list ⇒ p95 < 100 ms; recursive subtree, filtered, glob,
  Disktree, `Where` ⇒ p95 < 500 ms.
- **Storage (hard):** no hot row in any table stores a `dir`/`parent_dir`/path
  string — path text is reduced to one copy per directory per snapshot in the
  catalog. The filter tables remain (now `dir_id`-keyed/numeric) unless a specific
  pattern's materialisation was collapsed under area D with a cited measurement.
  Report compressed and uncompressed bytes per table vs baseline (the numeric
  filter tables should shrink substantially from dropping path strings even when
  retained).
- **Relative performance (report, not pass/fail):** per-query before/after delta
  reported for every type. Net wins expected on dedup-driven storage, ingest, and
  subtree/recursive/high-fanout navigation; the filtered query class is expected
  to be at parity-or-better because its materialisations are retained; any
  regression must be quantified and explained, and must still satisfy the absolute
  UX gate to ship.

---

## Correctness requirements

- The external behaviour of every method in [db/interfaces.go](db/interfaces.go)
  and the `clickhouse.Client` file API is unchanged. Callers (server, REST,
  Disktree, CLI `where`, dbinfo, basedirs) must not observe any difference except
  speed.
- Multi-source/active-mount semantics, `ErrDirNotFound` conditions, modtime
  selection, child de-dup/sort, and filter combination logic are preserved.
- Existing tests across `clickhouse/`, `summary/`, `db/`, and `internal/chperf/`
  must pass (after updating fixtures to the new schema); add tests proving the
  interval invariant, deterministic ids, collision safety, and parity between the
  numeric filter tables and the in-query vector-filter path (so either can serve a
  pattern with identical results).

---

## Non-goals

- No support for mutating a snapshot in place (data is immutable per snapshot;
  interval labelling is rebuilt each snapshot — that is the point, not a
  limitation to work around).
- No backwards compatibility, dual-read/dual-write, or online migration path.
- No change to the upstream wrstat stat-file format or to what `stats` parses.
- No new external/public API surface; this is an internal storage/query rewrite.
- The optional in-memory navigation index (area I) must not gate or delay the core
  schema work; ship it only if it measures well.

---

## Notes for the implementer

- Keep prototype code, scratch SQL, throwaway binaries, measurement reports, and
  notes under `.tmp/agent/overhaul/`. Preserve baseline artefacts for the
  before/after comparison.
- Use writable sub-agents for bounded design experiments (e.g. validating that an
  integer-range subtree scan beats the current exploded-table read on the
  high-fanout dataset) before committing the whole rewrite.
- Do not hand-wave the collapse in area D. If a bounded local dataset cannot
  reproduce a given access pattern's behaviour, say so explicitly rather than
  asserting the win.
- Justify integer widths, codecs (the catalog `full_path` is cold → high ZSTD;
  `dir_id`/`parent_id` benefit from `Delta`), and any projection choices with
  measured part sizes.

---

## Suggested implementation order

1. **Catalog + ids in the summariser.** Add preorder `dir_id` / `parent_id` /
   `subtree_end` assignment to the depth-first walk; emit the directory catalog.
   Prove determinism and the interval invariant. (Areas A, B.)
2. **Spool + writers carry ids.** Rewrite the spool format and ClickHouse writers
   to emit the catalog and id-keyed rows; keep readiness/resume machinery.
   (Area H.)
3. **Files table on `dir_id`.** Rewrite `wrstat_files` and the file API
   (`StatPath`, `ListDir`, `IsDir`, permissions). (Area C.)
4. **Facts + numeric filter tables on `dir_id`.** Rewrite `wrstat_dir_facts`
   keyed by `dir_id`; rewrite `wrstat_dir_filter_all` / `wrstat_child_filter_all`
   / `wrstat_dir_filter_ageall` as numeric `dir_id`-keyed tables (no path strings)
   — these are retained by default. Also implement the in-query vector-filtering
   code path so the benchmark can compare it per pattern and collapse only the
   materialisations it proves redundant (area D). Rewrite `DirInfo`, `DirInfos`,
   `DirsHaveChildren`, `Where`, `Children`. (Areas D, E.)
5. **Glob / full-text search** on the catalog skip indexes. (Area F.)
6. **Basedirs + active virtual namespace** in the id world. (Area G.)
7. **Benchmark study**: baseline capture, full query-type matrix, import/storage,
   gates, per-type deltas report. (Mandatory gate.)
8. **(Optional)** in-memory navigation index, flag-gated, measured. (Area I.)

Steps 3–6 can proceed in parallel once 1–2 land, since they share the catalog but
touch different tables/queries.

---

## Notes

Clarifications resolved during spec Q&A (these refine the areas above):

- **Snapshot/mount keying.** `dir_id` is unique only within a
  `(mount_path, snapshot_id)`. Every id-keyed table carries
  `(mount_path LowCardinality, snapshot_id UUID, dir_id)` and keeps today's
  partitioning — `dir_id` simply replaces the path column in the sort key. A
  further optimisation that replaces the `(mount_path, snapshot_id)` pair in hot
  rows with a compact integer `mount_key`/`snapshot_key` may be adopted **only**
  if the benchmark shows it materially reduces part sizes; default to the simple
  model otherwise.
- **Path → `dir_id` resolution.** Provide a `path_hash`-based resolver that
  **verifies the full path on hit** (so a hash collision can never return the
  wrong directory) and a `full_path` projection/index. Default to including
  `path_hash` as the primary resolver because exact path resolution sits under
  the hard 100 ms p95 gate; the benchmark may drop it if the `full_path`
  projection alone meets the gate.
- **Stream ordering for id assignment.** The `stats` parser already emits
  directories in DFS-contiguous order (it synthesises intermediate directory
  entries), so a directory and all its descendants form one uninterrupted run and
  preorder `dir_id`/`subtree_end` can be assigned with an ancestor stack during
  the existing walk. Note that files interleave with sibling subdirectories under
  lexicographic ordering, so id assignment must key off directory boundaries, not
  raw entry order. The spec must still prove the resulting ids and intervals are
  correct and deterministic with a test that includes interleaved files.
- **Basedirs / quota fallback.** Basedirs and quota tables reference `dir_id` for
  any `basedir`/`subdir` present in the active snapshot catalog. Paths
  legitimately outside the snapshot (historical or external quota records) keep an
  explicit string column, clearly marked external, used only as a fallback.
  Readiness must fail if an in-snapshot active `basedir`/`subdir` cannot be
  resolved to a `dir_id`.
- **Virtual namespace id space.** The active virtual namespace keeps its own
  small id space **only** for the synthetic virtual nodes (`/`, `/lustre/`,
  `/nfs/`, intermediate virtual parents, and mount-root boxes), keyed by
  `active_set_id`. Below a mount root it defers to that mount's per-snapshot
  catalog and `dir_id` ranges — it must not copy mount-local directories into a
  second namespace.
- **Optional in-memory navigation index.** Specify it as a flag-gated,
  explicitly non-blocking phase (the last phase) including a memory/build-cost
  estimate and benchmark hooks. It must never gate or delay the mandatory core
  schema and benchmark work; the core must be complete and correct without it.
- **Above-root ancestor chain in the per-mount catalog.** The summariser today
  emits facts rows for every path component *above* the data root (`mountPath`) —
  `/`, `/lustre/`, `/lustre/scratch125/`, … down to `mountPath`'s parent — into
  the same per-`(mount_path, snapshot_id)` tables that single-mount queries hit
  (via `dirguta`'s `outputRoot`), lazily at end-of-walk and deepest-first. The
  per-mount catalog is therefore rooted at filesystem `/`, not at `mountPath`, and
  this serving behaviour must be preserved. Assign these ancestors a **reserved
  low `dir_id` block in preorder** (`/` = 0, `/lustre/` = 1, …, `mountPath`'s
  parent = D−1, then the data root `mountPath` = D), with the data-root subtree
  numbered contiguously from D+1 onward. `mountPath`'s depth D is known up front
  (it is set before any `Add`), so the reservation is deterministic even though
  the ancestor rows are emitted out of order at the end of the walk; backfill
  their `subtree_end` to the end-of-snapshot bound (the linear chain contains the
  entire data-root subtree, so every ancestor's interval spans the whole
  snapshot). The interval invariant from area A must hold for **every** catalog
  row, ancestors included. `/`'s `parent_id` is the sentinel. The handful of
  duplicated linear-chain rows per snapshot is acceptable and matches current
  behaviour; do not move these rows into the `active_set_id` virtual layer (that
  layer remains only for the cross-mount synthetic namespace).
