# Phase 2: Directory-centric build + inline switch + retire sort (A2, A3, A4)

Ref: [spec.md](spec.md) sections A2, A3, A4

Depends on Phase 1 (the extracted shared GUTA helpers must exist and be
reviewed before the directory-centric build can reuse them).

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 2.1: A2 - Directory-centric two-pass build

spec.md section: A2

Create the new `summary/dirbuild` package implementing the contiguity-tolerant
two-pass build. Pass 1 streams only directory rows, sorts them in component
order (`DirectoryPath.Less`), assigns preorder
`dir_id`/`parent_id`/`subtree_end`/`depth` via `summary.DirIDAllocator`
(`SetMountPath` then `Enter`/`Leave` in sorted preorder), and builds a
`path->dir_id` map. Pass 2 re-streams, resolves each file's leaf `dir_id` from
the map and adds it ONLY into that leaf's `dir_id`-keyed `gutaStore`
accumulator (never into ancestors). After pass 2, do ONE bottom-up roll-up in
reverse preorder (descending `dir_id` / by `subtree_end`) draining each store
into its parent via the shared `drainInto` and merging each child's
seen-hardlink/inode set into its parent via the shared seen-set merge, then
emit `RecordDGUTA` rows in ascending `dir_id` order. File rows and synthetic
ancestor rows are NEVER spilled to disk. Use compact integer `dir_id` keys,
never hex path strings.

Implement the exported entry point:

```go
func Build(open func() (io.ReadCloser, error), mountPath string, db dirguta.DB,
    refTime time.Time) error
```

Files: `summary/dirbuild/dirbuild.go`. Test file:
`summary/dirbuild/dirbuild_test.go`.

Covering all 5 acceptance tests from A2 (contiguous-vs-permuted identical
sorted RecordDGUTA set; pre-parent/interleaved revisit leaf resolution;
cross-subdir hardlink counted once; `ErrTooManyDirs` on uint32 overflow;
reserved above-root chain `dir_id`/`parent_id` match
`ReservedDirIDForDepth`/`ReservedParentIDForDepth`).

- [x] implemented
- [x] reviewed

### Item 2.2: A3 - Inline switch on non-contiguity; retire the sort

spec.md section: A3

Depends on Item 2.1 (`dirbuild.Build` must exist). Wire the inline switch in
`cmd/summarise_spool.go`: on the first `summary.ErrNonContiguousInput` during
the streaming `dirguta` build, discard the partial `chspool.Set`/partial spool
dir (as the current `os.RemoveAll(partialDir)` retry does) then run
`dirbuild.Build` from fresh re-opens of the stats file. No whole-stream
synthetic-ancestor sort runs and no full failed spool is produced or
re-decoded. Delete `cmd/summarise_stats_sort.go` and its tests, and remove the
`sortInput` parameter threading from
`buildSummariseSpool`/`buildSummariseSpoolAttempt`.

Files: `cmd/summarise_spool.go` (delete `cmd/summarise_stats_sort.go` + its
tests). Test file: `cmd/summarise_spool_test.go`.

Covering all 4 acceptance tests from A3 (contiguous input takes single-pass
`dirguta` and never invokes `dirbuild.Build`, opening the stats file exactly
once; non-contiguous input completes with the partial dir removed first and a
manifest matching the contiguous permutation; `grep -r summariseStatsSort` over
`cmd/` yields zero matches; the previously-shipped non-contiguity regression
fixtures - unordered subtree revisit, prefix-sharing siblings `project/` vs
`project.v2/`, same-name file and directory, slashless `d` rows, U+00A0 unicode
path - all build identically to contiguous ordering).

- [x] implemented
- [x] reviewed

### Item 2.3: A4 - Bounded memory and bytes-written

spec.md section: A4

Depends on Item 2.1. Add memory-bounded and bytes-written tests for
`dirbuild.Build` proving peak heap scales with directory count (not file
count) and that no chunk/sort scratch spill occurs. Peak RSS targets the same
order as the contiguous fast path and is reported via the existing
`MaxRSSBytes` spool-load report field; exceeding the budget is a perf-gate
failure (Phase 3), not a runtime fallback. On-disk spill of accumulators is
out of scope.

Test file: `summary/dirbuild/dirbuild_test.go`.

Covering all 2 acceptance tests from A4 (1M-entry non-contiguous input with
dir set <= 5% of entries: heap growth under a stated dir-count-scaled threshold
via the go-conventions `runtime.GC()` + `ReadMemStats` before/after pattern
guarding unsigned underflow, with zero temp files under `t.TempDir()` beyond
the input; and no `*.bin`/`stats.sorted` artifacts created).

- [x] implemented
- [x] reviewed
