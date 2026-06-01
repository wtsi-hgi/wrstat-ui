# Phase 2: Active events

Ref: [spec.md](spec.md) sections A2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 2.1: A2 - Append-Only Active Mount Events

spec.md section: A2

Implement `wrstat_mount_events`, the final `wrstat_mounts_active` view,
publish, rollback, tombstone, cleanup queries, and active metadata cache
invalidation in `clickhouse/active_mounts.go`,
`clickhouse/active_snapshot_cleanup.go`, and `clickhouse/dguta_writer.go`,
covering all 6 acceptance tests from A2.

- [x] implemented
- [x] reviewed
