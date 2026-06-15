# Phase 8: (Optional) in-memory navigation index

Ref: [spec.md](spec.md) section I1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

This phase is OPTIONAL and explicitly non-blocking. It MUST NOT gate
or delay the core schema/benchmark work (Phases 1-7); the core is
complete and correct without it. Implement only after Phase 7 has
passed its gate, or in parallel without blocking it.

## Items

### Item 8.1: I1 - compact in-process catalog index

spec.md section: I1

Add `clickhouse/navindex.go` (new) and `clickhouse/navindex_test.go`
(new): an optional, flag-gated (`--nav-index`) in-memory index
(`dir_id -> parent_id, name, subtree_end, child_dir_count,
child_file_count`) built asynchronously per active snapshot, so
parent/child/has-children/ancestor/full-path navigation answers
without a ClickHouse round-trip; queries fall back transparently to
ClickHouse until the index is ready. Include a memory/build-cost
estimate (~`4 dir_id implicit + 4 parent_id + 4 subtree_end + 4 counts
+ len(name)` bytes per dir plus map overhead; low-GB for 10-100M dirs)
and report the measured figure. Covers all 4 acceptance tests from I1
(flag off -> identical to ClickHouse-only path; flag on + built ->
results match with no ClickHouse round-trip, verified by query-count
assertion; flag on + not yet built -> transparent fallback returning
correct results; benchmark reports memory/build cost and per-query
latency delta vs the ClickHouse path). Depends on Phases 1-2 (catalog)
and the navigation query paths from Phase 4; benchmark hooks from
Phase 7.

- [ ] implemented
- [ ] reviewed
