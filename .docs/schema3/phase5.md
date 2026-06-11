# Phase 5: Active virtual overlay

Ref: [spec.md](spec.md) sections C1, C2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 5.1: C1 - Active-set ids are deterministic

spec.md section: C1

Implement deterministic active-set id calculation in `clickhouse/provider.go`
and `clickhouse/active_mounts.go`, then include `ActiveSetID` in parent packet
cache keys and invalidation behavior in `clickhouse/database_cache.go`. Cover
all 3 acceptance tests from C1 in `clickhouse/provider_test.go`.

- [x] implemented
- [x] reviewed

### Item 5.2: C2 - Virtual rows cover roots and mount boxes

spec.md section: C2

Implement active virtual summary rows, full-filter rows, child rows, and reader
routes in `clickhouse/active_virtual_overlay.go`,
`clickhouse/virtual_children.go`, `clickhouse/database.go`, and related
writer paths. The overlay must cover `/`, `/lustre/`, `/nfs/`, intermediate
virtual parents, and mount-root boxes without duplicating ordinary non-root
facts. Cover all 8 acceptance tests from C2 in
`clickhouse/database_dirinfo_test.go`. Depends on item 5.1 active-set ids and
phase 2 active virtual writer hooks.

- [x] implemented
- [x] reviewed
