# Phase 1: AgeAll Filter Rows

Ref: [spec.md](spec.md) sections A1, A2, A3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 1.1: A1 - Import Narrow AgeAll Rows

spec.md section: A1

Add `wrstat_dir_filter_ageall` DDL and deterministic snapshot import writes in
`clickhouse/schema/012_dir_filter_ageall.sql`,
`clickhouse/dguta_writer.go`, `clickhouse/dir_filter_ageall.go`, and
`clickhouse/import_block_writer.go`, covering all 5 acceptance tests from A1.
This item blocks AgeAll routing and readiness work.

- [ ] implemented
- [ ] reviewed

### Item 1.2: A2 - Route Eligible Filters To AgeAll Rows

spec.md section: A2

Implement eligible AgeAll owner/type filter routing in
`clickhouse/dir_filter_ageall.go`, `clickhouse/database.go`, and
`clickhouse/dir_summary.go`, including
`dirFilterAgeAllCanHandleFilter` and `dirFilterAgeAllFilterExpression`,
covering all 6 acceptance tests from A2. Depends on A1 table/import support.

- [ ] implemented
- [ ] reviewed

### Item 1.3: A3 - AgeAll Readiness And Cleanup

spec.md section: A3

Wire AgeAll readiness and cleanup through
`clickhouse/dguta_writer.go`, `clickhouse/active_snapshot_cleanup.go`, and
`clickhouse/dir_filter_ageall.go`, covering all 5 acceptance tests from A3.
This finalizes snapshot atomicity before later performance gates rely on
AgeAll rows.

- [ ] implemented
- [ ] reviewed
