# Phase 4: Direct import parity

Ref: [spec.md](spec.md) sections A3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 4.1: A3 - Direct Import Uses Same Contract

spec.md section: A3

Update `clickhouse/dir_filter_all.go`, `internal/chperf/import.go`,
`clickhouse/dguta_writer_test.go`, and `internal/chperf/import_test.go` so
`fullFilterAllWriter.flush` and `clickhouse-perf import` write dir
full-filter rows only, derive child rows in ClickHouse, and report split
direct-import phases, covering all 3 acceptance tests from A3.

- [ ] implemented
- [ ] reviewed
