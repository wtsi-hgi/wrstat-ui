# Phase 8: Perf gates

Ref: [spec.md](spec.md) sections D2, D3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Batch 1 (parallel)

#### Item 8.1: D2 - Summarise Speed and Spool Size Gates [parallel with D3]

spec.md section: D2

Update `internal/chperf/final_gate.go` and
`internal/chperf/final_gate_test.go` to run bounded tree-only summarise gates
for t283 200k, scratch127 200k, and scratch125 500k or an equivalent dense
Lustre sample, covering all 5 acceptance tests from D2. The final pass is
sequential because it consumes evidence from all earlier phases.

- [ ] implemented
- [ ] reviewed

#### Item 8.2: D3 - Query Correctness and Speed Guardrails [parallel with D2]

spec.md section: D3

Update `internal/chperf/query.go`, `internal/chperf/final_gate.go`,
`internal/chperf/query_test.go`, and `internal/chperf/final_gate_test.go` to
compare before and after query counts, digests, time metrics, and read metrics
for t283, scratch125, and a normal Lustre path, covering all 4 acceptance
tests from D3. Query gates may run in parallel after phase 4, but the final
pass remains sequential because it consumes all evidence.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
