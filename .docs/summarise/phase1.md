# Phase 1: Baseline/reproduction gate

Ref: [spec.md](spec.md) sections D1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 1.1: D1 - Reproduce Failure Class Before Fix

spec.md section: D1

Encode `internal/chperf/final_gate.go` and
`internal/chperf/final_gate_test.go` evidence fixtures for the bounded
summarise and direct-import baselines, covering all 5 acceptance tests from
D1. The final gate must report blocked status if the t283 200k failure class
cannot be reproduced locally.

- [ ] implemented
- [ ] reviewed
