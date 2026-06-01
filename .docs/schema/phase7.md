# Phase 7: Perf harness

Ref: [spec.md](spec.md) sections E1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 7.1: E1 - Extend ClickHouse Perf Reports

spec.md section: E1

Extend `clickhouse-perf` reports in `internal/chperf/import.go`,
`internal/chperf/query.go`, `internal/chperf/clickhouse_api.go`, and
`cmd/clickhouse_perf.go` with DSN/database validation, selected table stats,
facts vector stats, phase metrics, profile-event query metrics, and focused
operations needed by final gates. Cover all 9 acceptance tests from E1.

- [ ] implemented
- [ ] reviewed
