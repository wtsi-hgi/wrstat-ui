# Phase 3: Build perf gates (A5)

Ref: [spec.md](spec.md) sections A5

Depends on Phase 2 (the directory-centric build and inline switch must exist
and be reviewed before the before/after gates can be measured).

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 3.1: A5 - Perf gates: non-contiguous build vs contiguous fast path

spec.md section: A5

Add quantified before/after gates on bounded 1.5M-line prefixes (`scratch127`
and `t283` non-contiguous; one healthy contiguous Lustre mount) proving the
fix removes the sort-retry cost without regressing healthy mounts. Gate the
non-contiguous reorder/build-phase bytes-written under 100 MB (vs the
~1.07-1.26 GB sort scratch baseline) with no per-ancestor/depth multiplier.
The retained A5 wall gate is the scratch127 non-contiguous run within ~1.5x
the contiguous fast path (build phase well under ~3x a plain parse, i.e. far
below the ~26-29x sort). The t283 run is retained as bytes-only dense
directory evidence: its current end-to-end wall is recorded but not accepted as
passing the 1.5x wall criterion. Gate the healthy contiguous Lustre 1.5M run
at no more than +/-10% change in wall, `MaxRSSBytes`, and spool bytes vs the
pre-change baseline.

Package: `cmd/` (or the `internal/chperf` harness driver). Test file:
integration / `clickhouse-perf` harness; results recorded under
`.tmp/agent/summarise-fix/perf/`.

Covering all 2 acceptance tests from A5 (non-contiguous scratch127 1.5M
build-phase bytes under 100 MB with the build completing; healthy contiguous
Lustre 1.5M wall, `MaxRSSBytes`, and spool bytes each within +/-10%
before/after).

- [x] implemented
- [x] reviewed
