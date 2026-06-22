# ClickHouse summarise speed + reliability recovery Specification

## Overview

Production `wrstat-ui summarise` cannot complete on three mounts
(`/lustre/scratch122`, `/lustre/scratch127`, `/nfs/t283_imaging`) because their
raw `stats.gz` is not DFS-subtree-contiguous. The overhaul schema assigns
preorder `dir_id`/`parent_id`/`subtree_end`/`depth` intervals in `summary/dirguta`
and requires contiguous input; non-contiguity returns
`summary.ErrNonContiguousInput`, triggering a whole-stream external-sort retry
(`cmd/summarise_stats_sort.go`). That sort emits a synthetic directory record per
ancestor per file plus a 2-byte-per-byte hex component key, writing ~1.1-1.3 GB
of scratch per 1.5M input lines (tens of GB per full mount) and running ~26-29x a
plain parse. Output is on NFS, so bytes written are themselves a primary cost.

This feature removes that failure class and reduces load cost without regressing
healthy contiguous mounts or schema3 query speed:

1. A contiguity-tolerant directory-centric two-pass build in a new
   `summary/dirbuild` package, reusing `dirguta`'s GUTA logic. The legacy
   whole-stream synthetic-ancestor sort is retired.
2. Server-side derivation of `wrstat_child_filter_all` via `INSERT..SELECT JOIN
   wrstat_dirs`, eliminating the gob double-write.
3. Split full-filter telemetry plus an amplification-ratio gate.
4. Fast completed-spool verification using trusted manifest counts.
5. A final partial-progress log fix.

Key behaviours: healthy contiguous mounts take the existing single-pass `dirguta`
fast path unchanged (no extra read, no reorder). Non-contiguity is detected
inline on the first `ErrNonContiguousInput` and the build switches to the
directory-centric path. Interval correctness, determinism, and query-table shape
are preserved byte-for-byte.

## Architecture

### Packages and files

- New: `summary/dirbuild/` - the contiguity-tolerant directory-centric two-pass
  build. Owns both passes and the `dir_id`-keyed accumulator map.
- Modified: `summary/dirguta/dirguta.go` - extract reusable internals (file-type
  classification, age-bucket fill, hardlink/inode dedup, store drain, seen-set
  merge) into exported shared helpers so both paths produce identical digests.
  The existing `Operation` lifecycle is otherwise unchanged.
- Modified: `cmd/summarise_spool.go` - replace the `ErrNonContiguousInput` retry
  (`buildSummariseSpool`) with the inline switch to `dirbuild`; drop the
  `WriteChildFilterAll` half of `writeSchema3FullFilterRow`.
- Removed: `cmd/summarise_stats_sort.go` and its tests - the whole-stream
  synthetic-ancestor external sort is retired.
- Modified: `cmd/summarise_diagnostics.go` - `logParseResult` reports true final
  count.
- Modified: `clickhouse/summarise_spool_loader.go` - split the collapsed
  `wrstat_filter_all_insert` phase; add a derived `wrstat_child_filter_all`
  `INSERT..SELECT` phase before schema3 readiness; stop loading the child spool
  file; populate split telemetry + amplification.
- Modified: `clickhouse/dir_filter_all.go` - make the direct `clickhouse-perf
  import` path derive child server-side instead of double-writing.
- Modified: `internal/chspool/spool.go` - trusted-manifest fast verify
  (`VerifyTables`); manifest version bump for trusted counts; stop including
  `wrstat_child_filter_all` in the spool table set.
- Modified: `internal/chperf/final_gate.go` (+ test) - golden-shape assertion for
  the split phase keys and amplification fields.

### Key existing types and APIs (reuse, do not redefine)

`summary` package (`summary/summariser.go`, `summary/idalloc.go`):

```go
type DirectoryPath struct { Name string; Depth int; Parent *DirectoryPath }
func (d *DirectoryPath) Less(e *DirectoryPath) bool   // component-order sort
func (d *DirectoryPath) AppendTo(p []byte) []byte

type FileInfo struct {
    Path *DirectoryPath; Name []byte; Size, ApparentSize int64
    UID, GID uint32; MTime, ATime, CTime, Inode, Nlink int64; EntryType byte
}
func (f *FileInfo) IsDir() bool

type DirIDAllocator struct{ ... }
func NewDirIDAllocator() *DirIDAllocator
func (a *DirIDAllocator) SetMountPath(mountPath string) error  // reserves 0..D
func (a *DirIDAllocator) Enter(dir *DirectoryPath) (uint32, error)
func (a *DirIDAllocator) Leave(dir *DirectoryPath) (uint32, error)
func ReservedDirIDForDepth(depth int) (uint32, error)
func ReservedParentIDForDepth(depth int) (uint32, error)

var ErrNonContiguousInput, ErrTooManyDirs error
```

`summary/dirguta/dirguta.go` internals to EXTRACT into exported shared helpers
(currently unexported; the directory-centric path and the DFS `Operation` path
must call the SAME code):

- `FileTypeWithTemp(name []byte, isTempDir bool) db.DirGUTAFileType` (exported
  already), `IsTemp(name []byte) bool` (exported), `FilenameToType` (exported).
- `gutaStore` (map[gutaKey]*summary.SummaryWithTimes + refTime) and its methods
  `add`, `addForEach`, `subtractFromStore`, `sort`, `drainInto`.
- `inodeEntry` + `handleHardlink` (per-file inode dedup) and
  `mergeSeenHardlinks`/`updateExistingHardlink` (subtree seen-set merge).
- `getGUTA` (gutaStore entry -> `db.GUTA`).
- `gutaKey`/`gutaKeys`/`gutaKeysFromEntry`/`gutaKeyPool`.

These move to a shared, exported home (e.g. exported types/functions in
`summary/dirguta` consumed by `summary/dirbuild`, or a small shared subpackage).
The spec mandates: extract exactly the set above; the DFS `Operation`
(`DirGroupUserTypeAge`) keeps calling them with no behavioural change.

`summary/dirguta` emits `db.RecordDGUTA` via the `DB` interface:

```go
type DB interface{ Add(dguta db.RecordDGUTA) error }
type RecordDGUTA struct {
    Dir *summary.DirectoryPath; DirID, ParentID, SubtreeEnd uint32; Depth uint16
    GUTAs GUTAs; Children []string; ChildCount, ChildFileCount uint64
}
```

`internal/chspool/spool.go`:

```go
type Set struct{ ... }
func (s *Set) WriteDir(DirRow) error
func (s *Set) WriteFile(FileRow) error
func (s *Set) WriteDirFact(DirFactRow) error
func (s *Set) WriteDirFilterAll(DirFilterAllRow) error
func (s *Set) WriteChildFilterAll(ChildFilterAllRow) error   // REMOVED for canonical write
func (s *Set) WriteDirFilterAgeAll(DirFilterAgeAllRow) error

type TableManifest struct{ Table, Path string; Rows uint64; Bytes int64; SHA256 string }
type Manifest struct{ Version int; ...; Tables map[string]TableManifest; ... }
func VerifyManifest(dir string, got *Manifest, expected Manifest) error
func VerifyTables(dir string, tables map[string]TableManifest) error
const Version = ...  // bump when trusted-count semantics change
```

`internal/perfreport/report.go`:

```go
type TableStats struct {
    Rows, ActiveParts, CompressedBytes, UncompressedBytes, ImportMemoryBytes uint64
    RowAmplificationVsDirFacts, RowAmplificationVsCatalog float64
    ImportPhaseDurationsMS map[string]float64
}
type Report struct{ ...; TableStats map[string]TableStats; MaxRSSBytes uint64; ... }
```

`MaxRSSBytes` is whole-process `getrusage(RUSAGE_SELF).Maxrss` (KiB), set in
`clickhouse/summarise_spool_loader.go:summariseSpoolMaxRSSBytes`. Build + load run
in one process, so it already captures the build-phase peak.

### Data formats

`wrstat_dirs` (`clickhouse/schema/004_dirs.sql`): keyed `(mount_path,
snapshot_id, dir_id)`; holds `parent_id`, `subtree_end`, `depth`. Partition
`(mount_path, snapshot_id)`.

`wrstat_dir_filter_all` (`clickhouse/schema/016_dir_filter_all.sql`): order
`(mount_path, snapshot_id, age, gid, uid, ft, dir_id)`; has `dir_id,
subtree_end`. CANONICAL spool table.

`wrstat_child_filter_all` (`clickhouse/schema/015_child_filter_all.sql`): order
`(mount_path, snapshot_id, parent_id, age, gid, uid, ft, dir_id)`; has
`parent_id, dir_id`. DERIVED server-side.

Derived insert (measured 1.16s for 1,122,958 scratch127 rows):

```sql
INSERT INTO wrstat_child_filter_all
  (mount_path, snapshot_id, parent_id, age, gid, uid, ft, dir_id, count, size,
   atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count,
   child_count, has_filter_children, has_children, refreshed_at)
SELECT d.mount_path, d.snapshot_id, c.parent_id, d.age, d.gid, d.uid, d.ft,
   d.dir_id, d.count, d.size, d.atime_min, d.mtime_max, d.atime_buckets,
   d.mtime_buckets, d.filter_child_count, d.child_count, d.has_filter_children,
   d.has_children, d.refreshed_at
FROM wrstat_dir_filter_all d
INNER JOIN wrstat_dirs c
  ON d.mount_path = c.mount_path AND d.snapshot_id = c.snapshot_id
     AND d.dir_id = c.dir_id
WHERE d.mount_path = ? AND d.snapshot_id = ?
```

(Confirm the exact `wrstat_child_filter_all` column list against
`clickhouse/schema/015_child_filter_all.sql` at implementation time; the SELECT
must produce byte-identical rows to today's double write.)

### Error handling

- `ErrNonContiguousInput` during the streaming build: caught inline, triggers the
  directory-centric path (no error to caller).
- `ErrTooManyDirs`: propagated unchanged from both paths.
- Derived-child `INSERT..SELECT` failure: return before schema3 readiness; the
  previous active snapshot/active set stay visible. No new cleanup path - the
  existing per-snapshot partition drop removes the staging rows.
- Amplification > waiver threshold without waiver: hard fail the load with a
  named error before publish.
- Trusted-verify mismatch (size/hash/schema/identity/version/presence): reject the
  spool with `ErrManifestMismatch` (existing sentinel).

## A. Contiguity-tolerant directory-centric build

### A1: Extract shared GUTA helpers from dirguta

As a maintainer, I want `dirguta`'s per-file GUTA logic exposed as shared helpers,
so the directory-centric build produces byte-for-byte identical facts/full-filter
digests without duplicating logic.

Extract the exact set listed in Architecture (`gutaStore` + methods, `inodeEntry`,
`handleHardlink`, seen-set merge, `getGUTA`, `gutaKey*`, file-type/age helpers).
The existing `DirGroupUserTypeAge.Operation` lifecycle must keep calling these
with no observable change.

**Package:** `summary/dirguta/`
**File:** `summary/dirguta/dirguta.go` (+ new shared file if needed)
**Test file:** `summary/dirguta/dirguta_test.go`

**Acceptance tests:**

1. Given the current `dirguta` test fixtures, when run after extraction, then all
   existing `dirguta` tests pass unchanged (no `RecordDGUTA` digest changes).
2. Given a single directory with one regular file, one temp-suffixed file, and a
   hardlinked file (Nlink=2) seen twice, when summarised via the DFS `Operation`,
   then the emitted `RecordDGUTA.GUTAs` match the pre-extraction golden bytes (the
   hardlink is counted once; temp typing applied).

### A2: Directory-centric two-pass build

As an operator, I want non-contiguous mounts to summarise via a directory-centric
two-pass build, so they complete with bytes-written bounded near input size and no
synthetic-ancestor blow-up.

Pass 1 streams the stats reading ONLY directory rows: collect the directory set,
sort it in component order (`DirectoryPath.Less`), assign preorder
`dir_id`/`parent_id`/`subtree_end`/`depth` via `summary.DirIDAllocator`
(`SetMountPath` then `Enter`/`Leave` in sorted preorder), and build a
`path->dir_id` map. Pass 2 re-streams the stats; for each file resolve its leaf
`dir_id` from the map and add it ONLY into that leaf directory's accumulator (a
`dir_id`-keyed `gutaStore` map) - never into ancestors. After pass 2, perform ONE
bottom-up roll-up over the directory set in reverse preorder (descending `dir_id`
/ by `subtree_end`), draining each directory's store into its parent via the
shared `drainInto`, and merging each child's seen-hardlink/inode set into its
parent via the shared seen-set merge, so subtree-wide dedup is preserved and
inodes are not double-counted. Emit `RecordDGUTA` rows in ascending `dir_id`
order. File rows and synthetic ancestor rows are NEVER spilled to disk.

Peak memory is bounded by the directory set, not the file set. Use compact integer
`dir_id` keys, never hex path strings. No unbounded full-stream in-memory file
aggregation.

**Package:** `summary/dirbuild/`
**File:** `summary/dirbuild/dirbuild.go`
**Test file:** `summary/dirbuild/dirbuild_test.go`

```go
// Build runs the two-pass directory-centric build over the stats stream,
// emitting RecordDGUTA rows to db in ascending dir_id order. open is called
// once per pass to obtain a fresh reader of the same stats stream.
func Build(open func() (io.ReadCloser, error), mountPath string, db dirguta.DB,
    refTime time.Time) error
```

**Acceptance tests:**

1. Given a contiguous DFS stats stream and the SAME stream randomly permuted
   (a non-contiguous permutation of identical records), when each is built (the
   contiguous one via `dirguta` DFS, the permuted one via `dirbuild.Build`), then
   the full set of emitted `RecordDGUTA` rows is identical after sorting by
   `dir_id`: identical catalog intervals (`dir_id`/`parent_id`/`subtree_end`/
   `depth`), identical per-dir `GUTAs`, identical `Children`/`ChildCount`/
   `ChildFileCount`.
2. Given a tree where a child directory's files appear in the stream before the
   parent directory row and interleaved with an unrelated subtree (revisit), when
   built via `dirbuild.Build`, then it completes without error and each file's
   resolved leaf `dir_id` equals the `dir_id` its parent path was assigned.
3. Given a directory with a hardlinked inode (Nlink=2) whose two links live in
   different subdirectories of the same parent, when built, then the inode is
   counted once in the parent's rolled-up `GUTAs` (subtree dedup preserved
   through the bottom-up seen-set merge).
4. Given more directories than fit in `uint32` preorder space, when built, then
   `Build` returns an error matching `summary.ErrTooManyDirs`.
5. Given the reserved above-root chain (mount path `/lustre/scratch127`,
   depth 2), when built, then the root data directory's `dir_id` and `parent_id`
   match `ReservedDirIDForDepth`/`ReservedParentIDForDepth` for that depth, equal
   to the DFS path's values for the same input.

### A3: Inline switch on non-contiguity; retire the sort

As an operator, I want the build to take the fast single-pass `dirguta` path for
contiguous input and switch to `dirbuild` only on the first
`ErrNonContiguousInput`, so healthy mounts never pay an extra pass and the legacy
sort is gone.

On the first `summary.ErrNonContiguousInput` during the streaming `dirguta`
build, discard the partial `chspool.Set`/partial spool dir (as the current
`os.RemoveAll(partialDir)` retry does), then run `dirbuild.Build` from fresh
re-opens of the stats file. Total reads for a non-contiguous mount: the failed
streaming pass (stops at the break) + pass 1 (dirs) + pass 2 (files) = up to three
cheap sequential reads (~1s parse each per 1.5M lines), with ZERO disk spill of
file/sort data. No whole-stream synthetic-ancestor sort runs and no FULL failed
spool is produced or re-decoded. The eliminated waste is the discarded-failed-spool
plus the ~26-29x external sort, not the raw re-reads.

Delete `cmd/summarise_stats_sort.go` and its tests. Remove the `sortInput`
parameter threading from `buildSummariseSpool`/`buildSummariseSpoolAttempt`.

**Package:** `cmd/`
**File:** `cmd/summarise_spool.go`
**Test file:** `cmd/summarise_spool_test.go`

**Acceptance tests:**

1. Given a contiguous stats input, when `buildSummariseSpool` runs, then it
   completes via the single-pass `dirguta` path and `dirbuild.Build` is NOT
   invoked (assert via a build-path counter/flag in the diagnostics or an
   injected hook), and the stats file is opened exactly once.
2. Given a non-contiguous stats input, when `buildSummariseSpool` runs, then it
   completes successfully, the partial spool dir is removed before the
   directory-centric build starts, and the resulting manifest contains the same
   table set and row counts as the contiguous permutation of the same data.
3. Given the repository after this story, when `grep -r summariseStatsSort` over
   `cmd/`, then there are zero matches (the sort path is fully removed).
4. Given the previously-shipped non-contiguity regression fixtures - unordered
   subtree revisit; prefix-sharing siblings `project/` vs `project.v2/`; a
   same-name file and directory; slashless `d` rows; a non-ASCII (U+00A0
   non-breaking space) unicode path - when each is built via `dirbuild.Build`,
   then all complete and yield catalog intervals and file `dir_id` references
   identical to the contiguous ordering of the same data.

### A4: Bounded memory and bytes-written

As an operator, I want the directory-centric build's peak RSS and bytes-written
bounded, so a non-contiguous full mount does not exhaust NFS scratch or memory.

Peak RSS targets the same order as the contiguous fast path (~489 MB at the 1.5M
bounded proof size), scaling with directory count. Report peak RSS via the
existing `MaxRSSBytes` spool-load report field. Exceeding the budget is a
perf-gate failure, not a runtime fallback. On-disk spill of accumulators is
out of scope.

**Package:** `summary/dirbuild/`
**Test file:** `summary/dirbuild/dirbuild_test.go`

**Acceptance tests:**

1. Given a 1M-entry non-contiguous stats input in `t.TempDir()` with a directory
   set <= 5% of entries, when built via `dirbuild.Build` with no file/sort spill,
   then heap growth (go-conventions memory-bounded pattern: `runtime.GC()` +
   `ReadMemStats` before/after, guarding unsigned underflow) is less than a stated
   threshold scaling with the directory count, and zero temporary files are
   written under `t.TempDir()` beyond the input.
2. Given the same input, when built, then no chunk/sort scratch file is created
   (assert the build writes no `*.bin`/`stats.sorted` artifacts).

### A5: Perf gates - non-contiguous build vs contiguous fast path

As a reviewer, I want quantified before/after gates, so the fix provably removes
the sort-retry cost without regressing healthy mounts.

Gates on bounded 1.5M-line prefixes (`scratch127`, `t283` non-contiguous;
one healthy contiguous Lustre mount):

- Non-contiguous reorder/build phase bytes-written: << current ~1.07-1.26 GB;
  target O(input size), no per-ancestor/depth multiplier. Concretely: build-phase
  scratch bytes for non-contiguous scratch127/t283 1.5M MUST be under 100 MB
  (vs ~1.07-1.26 GB today).
- Non-contiguous end-to-end wall: within ~1.5x the contiguous fast path of the
  same size (contiguous scratch127 1.5M build ~10s; gate the non-contiguous build
  phase under ~3x a plain parse, i.e. well under the ~26-29x sort).
- Healthy contiguous Lustre 1.5M: no regression in wall, peak RSS, or spool bytes
  beyond a stated +/-10% noise threshold vs the pre-change baseline.

**Package:** `cmd/` (or `internal/chperf` harness driver)
**Test file:** integration / `clickhouse-perf` harness; recorded in
`.tmp/agent/summarise-fix/perf/`

**Acceptance tests:**

1. Given the non-contiguous scratch127 1.5M prefix, when summarised after the
   fix, then total build-phase bytes written under `t.TempDir()`/spool-partial is
   below 100 MB and the build completes (vs the ~1.07 GB sort scratch baseline).
2. Given the healthy contiguous Lustre 1.5M prefix, when summarised before and
   after the fix, then wall time, `MaxRSSBytes`, and spool bytes each change by
   no more than +/-10%.

## B. Server-side derivation of wrstat_child_filter_all

### B1: Stop the gob double-write; keep dir canonical

As a maintainer, I want only `wrstat_dir_filter_all` written to the spool, so
spool bytes drop by the child file's share and there is one source of truth.

Drop the `WriteChildFilterAll` call in `cmd/summarise_spool.go`'s
`writeSchema3FullFilterRow` (the second write); remove
`summariseChildFilterAllRowForDirFilterAll`. Remove `wrstat_child_filter_all` from
the spool table set / `Set` writer / manifest `tableOrder` / `VerifyTables` /
`countRows`. The spool loader must no longer load a child spool file.

**Package:** `cmd/`, `internal/chspool/`
**File:** `cmd/summarise_spool.go`, `internal/chspool/spool.go`
**Test file:** `cmd/summarise_spool_test.go`, `internal/chspool/spool_test.go`

**Acceptance tests:**

1. Given any summarise run, when the spool is built, then `manifest.Tables` does
   NOT contain `wrstat_child_filter_all` and no `wrstat_child_filter_all.gob.gz`
   file exists in the spool dir.
2. Given a built spool with N `wrstat_dir_filter_all` rows, when the manifest is
   read, then the dir full-filter row count equals N and the prior duplicate child
   bytes are absent (spool total bytes drop by roughly the child table's former
   share).

### B2: Derive child server-side as a distinct pre-readiness phase

As an operator, I want `wrstat_child_filter_all` populated by `INSERT..SELECT`
joining `wrstat_dirs` for `parent_id`, before schema3 readiness, so the query
contract is unchanged while spool/load cost drops.

Run the derived insert (Architecture SQL) into the same staging snapshot
partition `(mount_path, snapshot_id)`, in a new distinct phase immediately after
the dir full-filter insert and before the schema3 readiness/snapshot-activation
marker. On insert error, return before readiness so the previous active
snapshot/active set stay visible. No new cleanup path: the existing per-snapshot
partition-drop removes failed/old/inactive/tombstoned derived rows.

Make the direct `clickhouse-perf import` path
(`clickhouse/dir_filter_all.go:flushLastPending`) consistent: write only dir, then
derive child server-side the same way.

**Package:** `clickhouse/`
**File:** `clickhouse/summarise_spool_loader.go`, `clickhouse/dir_filter_all.go`
**Test file:** `clickhouse/summarise_spool_loader_test.go`,
`clickhouse/dir_filter_all_test.go`

**Acceptance tests:**

1. Given a loaded snapshot, when the derived insert completes, then the
   `wrstat_child_filter_all` row count for `(mount, snapshot)` equals the
   canonical `wrstat_dir_filter_all` row count for the same snapshot.
2. Given a snapshot loaded via the old double-write path and one via the derived
   path on identical data, when both are queried, then per-`parent_id`
   `wrstat_child_filter_all` row contents and aggregate digests are identical.
3. Given the derived insert is forced to fail (e.g. a missing-catalog stub or
   injected error), when the load runs, then the schema3 readiness marker is NOT
   set, the snapshot is NOT activated, and the previously active
   snapshot/active-set remain visible.
4. Given a completed spool (no child spool file), when summarise is re-run as a
   completed-spool retry, then it finishes without reparsing `stats.gz` and the
   child table is derived server-side.
5. Given a failed/old/tombstoned snapshot partition, when per-snapshot cleanup
   runs, then the derived `wrstat_child_filter_all` rows for that snapshot are
   removed by the existing partition drop (no separate child-cleanup call).
6. Given the derived child table, when the load report is produced, then it
   records the derived child row count and table stats alongside normally loaded
   tables.

### B3: Query equivalence and load perf gate

As a reviewer, I want proof the derived child preserves query behaviour and cuts
load time, so the change is safe and beneficial.

Run `clickhouse-perf query` before/after on a t283 mount-root path, a healthy
Lustre path, and a high-fanout path (establish the baseline on a clean
single-mount DB; the harness pre-flight glob-routing EXPLAIN assertion fails on a
polluted multi-mount DB). Result counts/digests and p50/p95 must match.

**Test file:** `clickhouse-perf` harness; `.tmp/agent/summarise-fix/perf/`

**Acceptance tests:**

1. Given root, t283 mount-root, healthy Lustre, and high-fanout query paths, when
   run before and after the derive change on a clean single-mount DB, then result
   counts and digests match exactly and p50/p95 do not regress beyond noise.
2. Given scratch127 1.5M (combined `filter_all_insert` ~18s today), when loaded
   after the change, then the dir-only insert plus the derived child insert total
   materially less, with the derived insert near ~1-2s (measured 1.16s for
   1,122,958 rows), and spool bytes drop by roughly the child table's former
   share.

## C. Split telemetry + amplification guard

### C1: Split the collapsed full-filter phase

As an operator, I want separate `wrstat_dir_filter_all_insert` and
`wrstat_child_filter_all_insert` metrics, so reports show where load time goes.

Replace the collapsed `importPhaseFullFilterAllInsert`
("wrstat_filter_all_insert") with two named phases:
`wrstat_dir_filter_all_insert` and `wrstat_child_filter_all_insert`, each with
rows, bytes, duration, rows/sec, in BOTH the spool-load report and the
`clickhouse-perf import` report (`ImportPhaseDurationsMS` + `TableStats`).

**Package:** `clickhouse/`, `internal/chperf/`
**File:** `clickhouse/summarise_spool_loader.go`, `clickhouse/dguta_writer.go`,
`internal/chperf/import.go`
**Test file:** corresponding `_test.go`

**Acceptance tests:**

1. Given a spool-load report after a run, when inspected, then it contains phase
   keys `wrstat_dir_filter_all_insert` and `wrstat_child_filter_all_insert` (both
   non-zero duration) and NO `wrstat_filter_all_insert` key.
2. Given the `clickhouse-perf import` report, when inspected, then both phase keys
   are present with rows, bytes, duration, and a computable rows/sec.

### C2: Amplification ratio + waiver gate

As an operator, I want a full-filter rows/input-row amplification ratio attributed
to dir vs derived child, warned above 5 and hard-failed (behind a waiver) above
10, so accidental blow-ups are caught while legitimately dense data (t283 ~6.9x
per table) loads.

Compute the ratio from existing `TableStats.RowAmplificationVs*` fields. Warn
(slog) when amplification per input row > 5. Hard-fail the load before publish
when > 10 unless an explicit debug/waiver env var or flag is set
(e.g. `WRSTAT_FILTER_AMPLIFICATION_WAIVER=1`).

**Package:** `clickhouse/` (or `internal/chperf/`)
**Test file:** corresponding `_test.go`

**Acceptance tests:**

1. Given amplification 6 (between 5 and 10), when the load runs without a waiver,
   then it completes and a warn-level log records the ratio attributed to dir vs
   child.
2. Given amplification 11 and no waiver set, when the load runs, then it returns a
   named error before publish and does not activate the snapshot.
3. Given amplification 11 and the waiver set, when the load runs, then it
   completes (warn logged) and publishes.
4. Given t283-shaped density (~6.9x per table, ~13.8x combined), when the load
   runs, then under the chosen ratio basis t283 stays within the hard-fail
   threshold on the waiver-free path: the gate must not block legitimate t283
   loads by default.

### C3: Golden-shape telemetry assertion

As a maintainer, I want a report-level assertion that the split phase keys and
amplification fields exist and are non-zero, so the telemetry cannot silently
disappear again (it was specified once before and never landed).

Mirror the existing `final_gate` E3 amplification check
(`validateFinalGateE3TableStatsRowAmplification` /
`tableStatsDerivedEvidencePass`).

**Package:** `internal/chperf/`
**File:** `internal/chperf/final_gate.go`
**Test file:** `internal/chperf/final_gate_test.go`

**Acceptance tests:**

1. Given a report missing `wrstat_dir_filter_all_insert` or
   `wrstat_child_filter_all_insert`, when the golden-shape check runs, then it
   FAILS with a message naming the missing phase.
2. Given a report whose `wrstat_dir_filter_all`/`wrstat_child_filter_all`
   `TableStats` have zero `RowAmplificationVsDirFacts`/`RowAmplificationVsCatalog`,
   when the check runs, then it FAILS.
3. Given a report with both phase keys non-zero and both full-filter tables'
   amplification fields populated, when the check runs, then it PASSES.

## D. Fast completed-spool verification

### D1: Trust manifest counts on completed-spool reuse

As an operator, I want completed-spool verification to trust recorded row counts
plus byte size and SHA256, so large production retries do not pay a full gob
decode (~53s for a 182 MB t283 spool).

Persist trusted per-table row counts in the manifest at initial writer close
(already in `TableManifest.Rows`). Bump the manifest `Version` to mark
trusted-count semantics. When reusing a COMPLETE spool, `VerifyTables` verifies
identity, schema/version compatibility, file presence, byte size, and SHA256, and
TRUSTS `TableManifest.Rows` instead of decoding every gob row. Any
size/hash/schema/identity/version/presence mismatch still rejects the spool
(`ErrManifestMismatch`).

**Package:** `internal/chspool/`
**File:** `internal/chspool/spool.go`
**Test file:** `internal/chspool/spool_test.go`

**Acceptance tests:**

1. Given a complete spool with a valid manifest, when `VerifyManifest` runs, then
   it returns nil WITHOUT gob-decoding any table file (asserted via an injected
   decode hook that must not fire).
2. Given a spool whose table file bytes were modified (size or SHA256 differ from
   the manifest), when `VerifyManifest` runs, then it returns an error matching
   `ErrManifestMismatch`.
3. Given a manifest with a `Version` older than the trusted-count version, when
   verified against the current expected manifest, then it returns an error
   matching `ErrManifestMismatch` (version mismatch), forcing a safe rebuild.
4. Given a spool missing a required table file, when `VerifyManifest` runs, then
   it returns `ErrManifestMismatch` (missing table/presence).

## E. Final partial progress logging

### E1: Report true final parsed record count

As an operator, I want the true final parsed record count logged at
parse-complete, so bounded runs do not log `records=0`/`records=1000000` for 1.5M
inputs. Diagnostics only; no speed change.

`logParseResult` must report the actual final record count (not the last
whole-million progress value) when parse completes.

**Package:** `cmd/`
**File:** `cmd/summarise_diagnostics.go`
**Test file:** `cmd/summarise_diagnostics_test.go`

**Acceptance tests:**

1. Given a parse of exactly 1,500,000 records with a 1M progress interval, when
   `logParseResult` is called on success, then the logged record count is
   1,500,000 (not 1,000,000).
2. Given a parse of 0 records, when `logParseResult` is called on success, then
   the logged record count is 0 and no spurious progress line is emitted.

## Implementation Order

Phases build on tested foundations from prior phases.

1. **Phase 1 - Shared GUTA helpers (A1).** Extract `dirguta` internals; prove
   existing tests unchanged. Sequential foundation for Phase 2.

2. **Phase 2 - Directory-centric build + inline switch + retire sort (A2, A3,
   A4).** New `summary/dirbuild` package, two-pass build, bottom-up roll-up with
   seen-set merge, bounded memory; wire the inline switch in `buildSummariseSpool`
   and delete `cmd/summarise_stats_sort.go`. Depends on Phase 1.

3. **Phase 3 - Build perf gates (A5).** Bounded before/after gates for
   non-contiguous vs contiguous. Depends on Phase 2.

4. **Phase 4 - Server-side child derivation (B1, B2).** Drop the gob double-write
   (summarise + `clickhouse-perf import`); add the pre-readiness derived insert
   phase. Independent of Phases 1-3; can run in parallel after Phase 1 if desired,
   but sequence after Phase 2 to avoid conflicting `summarise_spool.go` edits.

5. **Phase 5 - Split telemetry + amplification guard + golden-shape (C1, C2,
   C3).** Depends on Phase 4 (the derived phase must exist to be named/measured).

6. **Phase 6 - Query + load perf gates (B3).** Depends on Phases 4-5.

7. **Phase 7 - Fast spool verify (D1) + progress log fix (E1).** Independent of
   the build/derive work; can run in parallel with Phases 4-6. D1 needs the
   manifest version bump.

## Appendix: Key Decisions

- **Why directory-centric, not a better sort.** The catalog needs the directory
  tree's contiguity, not file contiguity. Directories are 3-32% of the stream and
  sort sub-second even for t283 (0.44s for 482,830 dirs). Adding a 7th correctness
  patch to the whole-stream synthetic-ancestor sort while leaving its ~26-29x cost
  and ~1 GB/1.5M scratch is explicitly rejected (Do Not Implement).
- **Why bottom-up roll-up, not per-file ancestor adds.** Adding each file into
  every ancestor is O(files x depth). One reverse-preorder drain (mirroring
  `dirguta`'s `addChild`/`drainInto` + seen-set merge) bounds CPU independent of
  depth and preserves subtree inode dedup.
- **Why reuse, not reimplement, GUTA logic.** Byte-for-byte identical
  facts/full-filter/catalog digests between the DFS and directory-centric paths
  require the SAME classification/age/dedup code; hence the A1 extraction.
- **Why server-side child derivation.** `child_filter_all` (keyed `parent_id`) is
  NOT a column rename of `dir_filter_all` (keyed `subtree_end`) on the overhaul
  schema; the derive joins the small `wrstat_dirs` catalog for `parent_id`
  (measured 1.16s/1.12M rows) vs writing+loading a second ~1 GB gob table.
- **Why a waiver, not a hard cap.** Production data is legitimately dense (t283
  ~6.9x per table); warn>5 catches accidents, hard-fail>10-behind-waiver protects
  while allowing known-dense loads.
- **Out of scope (follow-ups only):** memory-budgeted larger batch caps; parallel
  independent table loads after child derivation; integer-id dictionary
  compression; RowBinary/zstd native spool; high-fanout query-side index;
  on-disk spill of directory accumulators; upstream `wrstat` emitting contiguous
  stats (the true root-cause fix that would make the in-`wrstat-ui` reorder a
  defensive fallback only).
- **Do NOT:** reintroduce direct ClickHouse loading during parse; publish before
  full-filter readiness; build then discard a full failed spool; run the
  whole-stream synthetic-ancestor sort; drop or sparsify any schema3 query table
  without proving equivalent query speed.
- **Testing strategy:** GoConvey (`So`) per go-conventions; `t.TempDir()` for fs;
  memory-bounded pattern for A4/D1; integration gates recorded under
  `.tmp/agent/summarise-fix/perf/`. See go-implementor and go-reviewer skills for
  TDD workflow and review against these acceptance tests.
