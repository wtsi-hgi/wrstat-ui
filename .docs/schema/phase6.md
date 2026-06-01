# Phase 6: File and basedirs preservation

Ref: [spec.md](spec.md) sections C5, D1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Batch 1 (parallel)

#### Item 6.1: C5 - File-Level APIs And Extension Predicates [parallel with D1]

spec.md section: C5

Keep `ListDir`, `StatPath`, `IsDir`, `FindByGlob`, and file-level permission
checks on `wrstat_files`, add exact-safe extension predicates while preserving
regex authority and owner checks, and cover all 8 acceptance tests from C5 in
`clickhouse/file_api.go`.

- [x] implemented
- [x] reviewed

#### Item 6.2: D1 - Preserve Basedirs Behavior [parallel with C5]

spec.md section: D1

Preserve usage, subdirectory, and history behavior in
`clickhouse/basedirs_store.go` and `clickhouse/basedirs_reader.go`; ensure
basedirs snapshot tables participate in retry and old-snapshot cleanup while
history remains append-only. Cover all 4 acceptance tests from D1.

- [x] implemented
- [x] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
