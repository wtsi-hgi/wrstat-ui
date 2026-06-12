# Phase 7: Sidecar fallback

Ref: [spec.md](spec.md) sections E3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 7.1: E3 - Sidecar fallback is explicit and immutable

spec.md section: E3

Implement `clickhouse/navigation_sidecar.go` only if ClickHouse-native phases
A-D are correct and any cold E2 gate still misses after bounded query tuning.
Build the immutable mmap/Roaring active-set navigation sidecar with exact
directory packets, parent-child adjacency, filter postings plus summary
payloads, virtual rows, manifest checksums, atomic publish, reader grace
period, and cleanup of old versions. Keep ClickHouse as the source of truth
for files, history, basedirs, and audit. Use SQLite only for prototype/audit,
and Bolt only with active-set aggregate redesign. The fallback must reject
checksum mismatches, preserve versioned readers through a grace period, record
ClickHouse fallback counts, and measure storage size, p50, p95, p99, and
correctness digest. Cover all 3 acceptance tests from E3 in
`clickhouse/navigation_sidecar_test.go`. Depends on phase 6 results.

- [x] implemented
- [x] reviewed
