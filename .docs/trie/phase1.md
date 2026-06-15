# Phase 1: Baseline and fixtures

Ref: [spec.md](spec.md) sections D1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 1.1: D1 - Baseline And Final Comparison Reports

spec.md section: D1

Capture current-branch import, query, cleanup, basedirs, and storage reports
before schema edits begin, using a preserved binary or separate worktree when
needed. Extend `internal/chperf/query.go`, `cmd/clickhouse_perf.go`, and the
existing comparison harnesses with the operation names, digest fields, cleanup
timing, and separate `cold_uncached` and `warmed` rows required by D1. This
creates the baseline inputs for all 8 acceptance tests from D1; final trie
validation is completed in phase 6.

- [ ] implemented
- [ ] reviewed
