# Phase 1: Catalog + ids in the summariser

Ref: [spec.md](spec.md) sections A1, B1, B2, B3, B4, B5

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

This phase is the foundation for everything: preorder interval
(nested-set) id assignment in the existing DFS walk, the above-root
reserved low-id block, the `wrstat_dirs` catalog table, and carrying
the ids to writers. It must land before any table that keys on
`dir_id` (phases 2-6).

## Items

### Item 1.1: A1 - `wrstat_dirs` catalog table

spec.md section: A1

Write the catalog DDL `clickhouse/schema/004_dirs.sql` (one row per
directory per snapshot; `dir_id`/`parent_id`/`subtree_end`/`depth`/
`name`/`full_path`/`child_dir_count`/`child_file_count`/`path_hash`;
`children_proj` and `path_hash_proj` projections; `full_path` ngram +
token skip indexes; `PARTITION BY (mount_path, snapshot_id)`,
`ORDER BY (mount_path, snapshot_id, dir_id)`). Test file
`clickhouse/dirs_catalog_test.go`. Covers all 4 acceptance tests from
A1 (count/distinct-full_path invariant, interval-vs-parent_id-walk
invariant, root `parent_id = 0xFFFFFFFF`, full_path-vs-parent-walk
byte equality).

- [ ] implemented
- [ ] reviewed

### Item 1.2: B1 - preorder id assignment in the DFS walk

spec.md section: B1

Implement the monotonic preorder id assignment in `summary/dirguta`
hooked into the operation lifecycle (`summary/dirguta/dirguta.go`):
assign `dir_id`/`parent_id`/`depth` on a directory's first `Add`, set
`subtree_end = next` on its `Output`, keying off `DirectoryPath`
push/pop (not raw entry order so interleaved files consume no id).
No full-tree buffering (ancestor-stack state only); deterministic. The
assignment relies on subtree-contiguous input (an existing pipeline
invariant) and guards it: re-entry returns `ErrNonContiguousInput` and
counter overflow returns `ErrTooManyDirs` rather than colliding with
`parentSentinel`. Test file `summary/dirguta/idassign_test.go`. Covers
all 6 acceptance tests from B1 (gap-free preorder, determinism
byte-for-byte, interleaved file consumes no id, interval invariant on a
real fixture, `ErrTooManyDirs` on overflow, `ErrNonContiguousInput` on a
re-entered directory boundary). Depends on Item 1.1 (catalog schema
target).

- [ ] implemented
- [ ] reviewed

### Item 1.3: B2 - above-root ancestor reserved low-id block

spec.md section: B2

Reserve ids `0..D` for the linear above-root chain (`/`=0 ... data
root `mountPath`=D, D from `SetMountPath` slash count), start the
data-root counter at D+1, emit ancestors out of order at end-of-walk
via `dirguta.outputRoot`, backfill each ancestor `subtree_end = next`
(final counter), set `/`'s `parent_id = parentSentinel = 0xFFFFFFFF`.
Ancestor rows stay in the per-mount catalog (not the active layer).
File `summary/dirguta/dirguta.go`; test file
`summary/dirguta/idassign_test.go`. Covers all 3 acceptance tests
from B2 (reserved chain ids + first descendant >= D+1, ancestor
`subtree_end = next` with interval invariant, root sentinel + `[0,
next)` spans all rows). Depends on Item 1.2.

- [ ] implemented
- [ ] reviewed

### Item 1.4: B3 - ids carried to writers

spec.md section: B3

Add `DirID, ParentID, SubtreeEnd uint32` and `Depth uint16` to
`db.RecordDGUTA` (`db/dguta.go`); leave `db.GUTAs`/`DGUTA`/`Filter`/
`DirSummary` and the `DGUTAWriter` interface unchanged. Update
existing test file `summary/dirguta/dirguta_test.go`. Covers the 1
acceptance test from B3 (each `RecordDGUTA`'s ids match the catalog
row for that directory's `full_path`). Depends on Item 1.2 and 1.3.

- [ ] implemented
- [ ] reviewed

### Item 1.5: B4 - import cost measured, not assumed

spec.md section: B4

B4 has no standalone code; its single acceptance test is satisfied by
the benchmark (phase 7, gate section J) reporting summarise wall
time, CPU, max RSS for overhaul vs baseline and stating whether the
"ids near-free / leaner spool" claim held. Record here as a tracked
dependency the benchmark must honour; no implementation lands in this
phase beyond ensuring the id path adds no maps/time/concurrency
(determinism preserved per B1). Depends on Items 1.2-1.4.

- [ ] implemented
- [ ] reviewed

### Item 1.6: B5 - shared id allocator visible to file ingest

spec.md section: B5

Add `summary.DirIDAllocator` (`summary/idalloc.go`): the preorder
counter, reserved low-id block, and `*summary.DirectoryPath` -> `dir_id`
lookup. `cmd/summarise.go` creates one per run, calls `SetMountPath`
(reserve `0..D`, counter from D+1) and injects it (optional, nil for the
Bolt path) into `dirguta.NewDirGroupUserTypeAge` and
`clickhouse.NewFileIngestOperation`. The DGUTA op calls `Enter`/`Leave`
on directory boundaries; file ingest reads `DirID(info.Path)` (or
`info.Path.Parent` for a directory entry), replacing the `parent_dir`
string. Ordering is safe because a containing directory is entered
earlier in preorder than its entries, and the above-root chain + data
root are reserved at `SetMountPath`. The allocator-level behaviour
(reserved ids, counter, `ErrTooManyDirs`, `ErrNonContiguousInput`) is
unit-tested here in `summary/idalloc_test.go`; the file-ingest
consumption and B5 acceptance tests 1-4 (each `wrstat_files` row's
`dir_id`, including the mount-root entry under `D-1`) are exercised when
the file-ingest writer lands (Phase 2, Item 2.2), since they need the
files/spool rows. Depends on Items 1.2, 1.3.

- [ ] implemented
- [ ] reviewed
