# Phase 0: Baseline capture

Ref: [spec.md](spec.md) section J1 (capture half), Implementation Order step 0

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

This phase MUST complete before Phase 1 begins. It captures the
current `clickhouse` HEAD as the before/after benchmark baseline using
the existing, unmodified harness. Once any schema or code edit lands
(Phase 1 onward) an equivalent baseline can no longer be produced, so
this is the very first action of the whole effort. This phase changes
no production code; it only preserves a baseline artefact and its
report.

## Items

### Item 0.1: J1 - capture current HEAD baseline

spec.md section: J1 (capture half)

Before touching the working tree, preserve the current `clickhouse`
HEAD as the baseline: build and keep a baseline binary, or check out a
separate worktree, under `.tmp/agent/overhaul/`. Run the existing
(unmodified) `internal/chperf` import/query harness and the Bolt
comparison harness against a representative dataset to produce and
archive the baseline `perfreport.Report` (import wall time, CPU, max
RSS, spool bytes, per-table compressed/uncompressed bytes, part
counts, rows per table, and the per-query matrix metrics) under
`.tmp/agent/overhaul/`. Do NOT modify the harness or any production
code in this phase - the report must reflect HEAD exactly. The
captured baseline is the comparison target for the Phase 7 gate (J3,
J4, J6). No new test code lands here; the artefact's existence and
completeness are verified when Phase 7 consumes it.

- [x] implemented
- [x] reviewed
