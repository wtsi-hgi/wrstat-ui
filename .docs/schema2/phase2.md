# Phase 2: Active-Set Prefix Rollups

Ref: [spec.md](spec.md) sections B1, B2, B3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 2.1: B1 - Build Active Prefix Rollups

spec.md section: B1

Add active-prefix rollup DDL and refresh logic in
`clickhouse/schema/013_active_prefix_rollups.sql`,
`clickhouse/active_prefix_rollups.go`, `clickhouse/active_mounts.go`,
`clickhouse/dguta_writer.go`, and `clickhouse/provider.go`, including
`fingerprintForMountsActive`, covering all 6 acceptance tests from B1.
This item depends on Phase 1 AgeAll rows for AgeAll prefix rows.

- [ ] implemented
- [ ] reviewed

### Item 2.2: B2 - Route Virtual Ancestors And Tune Root Tuples

spec.md section: B2

Route `/`, `/lustre/`, `/nfs/`, and covered virtual ancestors through
active-prefix rollups in `clickhouse/database.go`,
`clickhouse/active_mounts.go`, `clickhouse/active_prefix_rollups.go`, and
`clickhouse/virtual_children.go`, and tune active mount-root tuple SQL,
covering all 6 acceptance tests from B2. Depends on B1 readiness.

- [ ] implemented
- [ ] reviewed

### Item 2.3: B3 - Active-Set Cleanup And Cache Invalidation

spec.md section: B3

Clean replaced active-set partitions and key caches by active set id, path,
filter, permission inputs, and schema/query version in
`clickhouse/virtual_children.go`, `clickhouse/active_prefix_rollups.go`, and
`clickhouse/database_cache.go`, covering all 4 acceptance tests from B3.
Depends on B1 rollup data and B2 reader routing.

- [ ] implemented
- [ ] reviewed
