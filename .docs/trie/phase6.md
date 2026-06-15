# Phase 6: Final evidence

Ref: [spec.md](spec.md) sections D1, D2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 6.1: D1 - Baseline And Final Comparison Reports

spec.md section: D1

Complete final trie report generation and validation in `internal/chperf`,
`cmd/clickhouse_perf.go`, and the existing Bolt comparison harnesses, comparing
against the phase 1 baseline. Include correctness digests, import and cleanup
timing, cold and warmed query rows, storage deltas, regression cause notes, and
separate basedirs `path_kind_case` rows. Cover all 8 acceptance tests from D1
in `internal/chperf/final_gate_test.go`. Depends on phases 1 through 5.

- [ ] implemented
- [ ] reviewed

### Item 6.2: D2 - Absolute Cold UX Gates

spec.md section: D2

Implement absolute cold UX gate validation in `internal/chperf/final_gate.go`
using representative datasets, fixed manifest detail, strict
`cold_uncached` rows, and separate server, serialization, row, byte, and
current-branch delta metrics for large results. Cover all 5 acceptance tests
from D2 in `internal/chperf/final_gate_test.go`. Depends on item 6.1 final
report data.

- [ ] implemented
- [ ] reviewed
