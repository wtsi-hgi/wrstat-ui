# Phase 3: Loader derivation

Ref: [spec.md](spec.md) sections A2, A4, A5

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 3.1: A2 - Child Derivation Before Readiness

spec.md section: A2

Update `clickhouse/summarise_spool_loader.go` and
`clickhouse/summarise_spool_loader_test.go` so the loader sends
`wrstat_dir_filter_all`, derives and counts `wrstat_child_filter_all`, and
only then inserts `wrstat_schema3_snapshot_sets`, covering all 4 acceptance
tests from A2. This item is the dependency for cleanup and publish
no-regression work in items 3.2 and 3.3.

- [ ] implemented
- [ ] reviewed

### Batch 1 (parallel, after item 3.1 is reviewed)

#### Item 3.2: A4 - Cleanup Derived Child Rows [parallel with A5]

spec.md section: A4

Update `clickhouse/dguta_writer.go`,
`clickhouse/active_snapshot_cleanup.go`,
`clickhouse/summarise_spool_loader.go`, and their tests so failed-load
retry, old snapshot drop, inactive cleanup, and tombstone cleanup remove
`wrstat_child_filter_all` wherever they remove `wrstat_dir_filter_all`,
covering all 4 acceptance tests from A4.

- [ ] implemented
- [ ] reviewed

#### Item 3.3: A5 - Preserve Non-Targeted Publish Behavior [parallel with A4]

spec.md section: A5

Update `clickhouse/summarise_spool_loader.go`,
`clickhouse/basedirs_store.go`, and their tests to preserve basedirs retry
cleanup/replay, zero-record active virtual behavior, and normal nonzero
active virtual overlay validation after child derivation, covering all 3
acceptance tests from A5.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
