# Phase 3: Disktree Navigation Facts

Ref: [spec.md](spec.md) sections C1, C2, C3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 3.1: C1 - Choose The Navigation Shape

spec.md section: C1

Run the bounded navigation decision gate in `clickhouse/parent_facts.go`,
`clickhouse/mount_dir_projection_writer.go`, and `internal/chperf/query.go`,
defaulting to `wrstat_parent_facts` unless projection or child facts meet the
evidence requirements, covering all 4 acceptance tests from C1. This gate
blocks final navigation implementation.

- [ ] implemented
- [ ] reviewed

### Item 3.2: C2 - Import Parent-Ordered Facts

spec.md section: C2

Add selected navigation fact import support, defaulting to parent-ordered
facts in `clickhouse/schema/014_parent_facts.sql`,
`clickhouse/parent_facts.go`, and `clickhouse/dguta_writer.go`, covering all
6 acceptance tests from C2. Depends on the C1 decision.

- [ ] implemented
- [ ] reviewed

### Item 3.3: C3 - Route Disktree Through Navigation Facts

spec.md section: C3

Route ordinary Disktree child summaries and REST tree paths through ready
navigation facts in `clickhouse/database.go`, `clickhouse/parent_facts.go`,
and `server/tree.go`, while preserving virtual parent and AgeAll filter
routes, covering all 5 acceptance tests from C3. Depends on C2 import and
readiness.

- [ ] implemented
- [ ] reviewed
