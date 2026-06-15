# Phase 5: Active virtual, server endpoints, and basedirs readers

Ref: [spec.md](spec.md) sections C2, C3, C4

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Batch 1 (parallel)

#### Item 5.1: C2 - Active Virtual Namespace [parallel with C3]

spec.md section: C2

Implement numeric active virtual overlay building and reading in
`clickhouse/active_virtual_overlay.go`, including synthetic nodes, mount links,
canonical active-set IDs, folded summaries, active filters, readiness gating,
and cleanup. Cover all 11 acceptance tests from C2 in
`clickhouse/database_test.go`. Depends on phase 3 active-set import paths and
phase 4 tree reader behavior.

- [ ] implemented
- [ ] reviewed

#### Item 5.2: C3 - Basedirs And Quota Readers [parallel with C2]

spec.md section: C3

Normalize active basedirs and subdirs to directory IDs and batch-load trie
paths in `clickhouse/basedirs_store.go`, preserve fallback path rows, enforce
readiness failures for unresolved active paths, and keep `server/basedirs.go`
auth behavior unchanged. Cover all 9 acceptance tests from C3 in
`clickhouse/basedirs_reader_test.go` and `server/server_test.go`. Depends on
phase 3 basedirs import paths.

- [ ] implemented
- [ ] reviewed

### Item 5.3: C4 - Server And CLI Query Surfaces

spec.md section: C4

Route REST tree endpoints, auth tree endpoints, REST where, and CLI
`where --dir` through the trie-native database paths in `server/tree.go`,
`server/where.go`, and `cmd/where.go`, preserving JSON fields, status codes,
ordering, auth flags, and result digests. Cover all 4 acceptance tests from C4
in `server/server_test.go` and `cmd/where_test.go`. Depends on item 5.1 and
phase 4 query rewrites.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
