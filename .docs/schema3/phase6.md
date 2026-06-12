# Phase 6: Perf gates

Ref: [spec.md](spec.md) sections E1, E2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 6.1: E1 - Perf harness records correctness and cold metrics

spec.md section: E1

Implement final-gate report structures, fixture digest validation,
ClickHouse, REST, import, spool-load, and same-subset Bolt or sidecar
comparison evidence in `internal/chperf/final_gate.go`,
`internal/boltperf/*`, `cmd/clickhouse_perf.go`, and `cmd/bolt_perf.go`.
Cover all 10 acceptance tests from E1 in `internal/chperf/final_gate_test.go`.
Depends on phases 1-5 for the measured schema3 routes.

- [x] implemented
- [x] reviewed

### Item 6.2: E2 - Cold performance gates pass without warming

spec.md section: E2

Implement cold performance gate evaluation in `internal/chperf/final_gate.go`
for mixed8, NFS-heavy, high-fanout, virtual simulation, filter switch, direct
import, and spool-load scenarios. Enforce correctness before timing, bounded
read-volume ceilings, no proactive warming, and measured import budgets.
Cover all 12 acceptance tests from E2 in `internal/chperf/final_gate_test.go`.
Depends on item 6.1 for report validation and correctness evidence, and on
phases 1-5 for complete schema3 behavior.

- [x] implemented
- [x] reviewed
