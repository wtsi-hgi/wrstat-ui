# Phase 2: Full-filter schema and direct writer

Ref: [spec.md](spec.md) sections B1, D1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 2.1: B1 - Full-filter row construction is exact

spec.md section: B1

Add schema3 DDL in `clickhouse/schema/*.sql` and `clickhouse/schema.go`, then
implement exact full-filter row construction and bounded insert writers in
`clickhouse/dir_filter_all.go`, `clickhouse/child_filter_all.go`,
`clickhouse/import_block_writer.go`, and `clickhouse/dguta_writer.go`.
Preserve all schema2 tables and routes. Cover all 3 acceptance tests from B1
in `clickhouse/dguta_writer_test.go`.

- [x] implemented
- [x] reviewed

### Item 2.2: D1 - Direct import stages every schema3 object before publish

spec.md section: D1

Update direct import ordering, validation, retry cleanup, schema3 snapshot
readiness, active virtual staging hooks, and mount-event publish behavior in
`clickhouse/dguta_writer.go`. Add the bounded active virtual writer methods
needed by D1, using row structs that match D2. Cover all 4 acceptance tests
from D1 in `clickhouse/dguta_writer_test.go`. Depends on item 2.1 schema3
tables and full-filter writer counts.

- [x] implemented
- [x] reviewed
