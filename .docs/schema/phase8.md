# Phase 8: Final gates and optional objects

Ref: [spec.md](spec.md) sections E2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 8.1: E2 - Run Final Perf Gates

spec.md section: E2

Run facts-only final gates first with repeated larger-prefix measurements,
store raw artifacts under `.tmp/agent/`, and compare import, query, RSS, row
count, and result-equivalence metrics against the documented baselines. Add
only `wrstat_dir_filter_ageall` or `wrstat_virtual_summary_cache` if its gate
fails, then rerun gates and keep selected clean schema-v1 DDL. Cover all 15
acceptance tests from E2.

- [x] implemented
- [x] reviewed
