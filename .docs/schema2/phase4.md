# Phase 4: Info, Permission/Auth Summaries, Harness, Anomaly, And Perf Gates

Ref: [spec.md](spec.md) sections D1, D2, E1, E2, E4, E3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 4.1: D1 - Preserve Info() Counts

spec.md section: D1

Keep `Info()` on canonical active facts and children counts in
`clickhouse/database.go` and `db/info.go`, covering all 6 acceptance tests
from D1. This confirms derived tables from Phases 1-3 do not alter dbinfo
semantics.

- [x] implemented
- [x] reviewed

### Item 4.2: D2 - Preserve Permission/Auth Checks

spec.md section: D2

Preserve permission checks, auth tree behavior, restricted `where`, and Unix
name-based server/CLI filter semantics in `clickhouse/file_api.go`,
`clickhouse/database.go`, `server/tree.go`, `server/filter.go`,
`server/where.go`, and `internal/chperf/query.go`, covering all 6 acceptance
tests from D2. Sequence after D1 to avoid concurrent
`clickhouse/database.go` edits.

- [x] implemented
- [x] reviewed

### Item 4.3: E1 - Expand Perf And Tracing Harnesses

spec.md section: E1

Extend existing ClickHouse, Bolt, REST, CLI, cache, and report structures in
`internal/perfreport/report.go`, `internal/chperf/query.go`,
`internal/chperf/final_gate.go`, `cmd/clickhouse_perf.go`,
`cmd/bolt_perf.go`, `cmd/where.go`, `server/tree.go`, and `server/where.go`,
covering all 6 acceptance tests from E1. Depends on D1 and D2 preservation
reviews.

- [x] implemented
- [x] reviewed

### Item 4.4: E2 - Resolve The t283 Filtered REST Anomaly

spec.md section: E2

Fix request-order-independent filtered REST behavior and cache key proof in
`server/where.go`, `clickhouse/database.go`,
`clickhouse/database_cache.go`, and `internal/chperf/query.go`, covering all
4 acceptance tests from E2. Depends on cache and auth semantics from prior
items.

- [x] implemented
- [x] reviewed

### Item 4.5: E4 - Tactical Work Is Supporting Only

spec.md section: E4

Add only supporting metadata batching, readiness batching, warming, or
response caching in `clickhouse/database.go`, `clickhouse/provider.go`,
`clickhouse/database_cache.go`, `server/tree.go`, and `server/where.go`,
covering all 3 acceptance tests from E4. Ensure this work does not substitute
for cold-path wins required by Phases 1-3 and E3.

- [x] implemented
- [x] reviewed

### Item 4.6: E3 - Final Performance Gates

spec.md section: E3

Implement final gate evaluation and command coverage in
`internal/chperf/final_gate.go`, `cmd/clickhouse_perf.go`, and
`cmd/bolt_perf.go`, covering all 9 acceptance tests from E3. Depends on all
prior phase items, especially E1 reports, E2 anomaly resolution, D1/D2
behavior preservation, and any E4 tactical support.

- [x] implemented
- [x] reviewed
