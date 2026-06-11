# Phase 3: Full-filter readers

Ref: [spec.md](spec.md) sections B2, B3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 3.1: B2 - All-filter readers replace vector scans

spec.md section: B2

Implement full-filter reader routes in `clickhouse/dir_filter_all.go` and
`clickhouse/database.go` for UID, GID, file-type bitmask, owner+user+type,
AgeAll, age-specific, unused, and unchanged filters. Preserve broad/default
and scalar file-only reads on `wrstat_dir_facts` or `wrstat_parent_facts`
unless an exact replacement is measured equal-or-better. Empty UID/GID lists
must return empty results without reading schema3 filter tables. Cover all 6
acceptance tests from B2 in `clickhouse/database_dirinfo_test.go`. Depends on
phase 2 readiness tests.

- [ ] implemented
- [ ] reviewed

### Item 3.2: B3 - `where --dir` uses subtree-serving rows

spec.md section: B3

Route `Tree.Where`, CLI `where --dir`, and REST where probes through
`wrstat_dir_filter_all` in `clickhouse/database.go`, `cmd/where.go`, and
`server/where.go`, keeping `wrstat_dir_filter_ageall` only while it is exact
and strictly faster on measured AgeAll p95. Cover all 6 acceptance tests from
B3 in `clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`, and
`server/server_test.go`. Depends on item 3.1 filter-row reader routes and
phase 2 readiness tests.

- [ ] implemented
- [ ] reviewed
