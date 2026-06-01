# Phase 4: Facts query routing

Ref: [spec.md](spec.md) sections C1, C2, C4

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 4.1: C1 - Route Tree Reads Through Facts

spec.md section: C1

Route `DirInfo`, `DirInfos`, broad Disktree child summaries, `Children`,
`DirsHaveChildren`, `Where`, `Info`, and tree `PermissionAnyInDir` through
`wrstat_dir_facts`, `wrstat_children`, `wrstat_mounts_active`, readiness, and
virtual hierarchy tables in `clickhouse/database.go` and
`clickhouse/dir_facts.go`. Remove raw-DGUTA fallback tests and cover all 6
acceptance tests from C1.

- [ ] implemented
- [ ] reviewed

### Item 4.2: C2 - Optional AgeAll Filter Index

spec.md section: C2

Keep facts-only routing as the default, add the optional
`wrstat_dir_filter_ageall` route only when its readiness and gate conditions
apply, and ensure age-specific reads always use facts vectors. Implement in
`clickhouse/dir_filter_ageall.go` and `internal/chperf/query.go`, covering
all 8 acceptance tests from C2.

- [ ] implemented
- [ ] reviewed

### Item 4.3: C4 - Info And Permission Counts

spec.md section: C4

After C1 facts routing is in place, preserve `Info` counts and tree
permission semantics using facts vector entries or the optional AgeAll route
when applicable in `clickhouse/database.go` and `clickhouse/file_api.go`.
Cover all 4 acceptance tests from C4.

- [ ] implemented
- [ ] reviewed
