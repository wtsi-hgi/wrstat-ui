# Phase 5: Virtual active hierarchy

Ref: [spec.md](spec.md) sections C3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 5.1: C3 - Virtual Active Ancestors

spec.md section: C3

Add `wrstat_virtual_children`, live ancestor composition from active
mount-root `wrstat_dir_facts`, active-set invalidation, old active-set
cleanup, and no-`FINAL` hot reads in `clickhouse/virtual_children.go` and
`clickhouse/database.go`. Add optional virtual summary cache only if gates
require it, covering all 7 acceptance tests from C3.

- [x] implemented
- [x] reviewed
