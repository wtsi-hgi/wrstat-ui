# Phase 3: Facts import

Ref: [spec.md](spec.md) sections B1, B2, B3, B4

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 3.1: B4 - Shared Bounded Block Writer

spec.md section: B4

Add `clickhouse/import_block_writer.go` and wire the shared lifecycle into
`clickhouse/dguta_writer.go` for lazy prepare, bounded flush, detached send
contexts, abort, and ambiguous-send behavior. This enables the facts and child
writers and covers all 9 acceptance tests from B4.

- [ ] implemented
- [ ] reviewed

### Item 3.2: B1 - Stream Canonical Directory Facts

spec.md section: B1

Use the bounded writer from B4 to stream `wrstat_dir_facts` rows, stream
`wrstat_children` edges, preserve aligned vector summaries and scalar hot
summaries in `clickhouse/dguta_writer.go`, `clickhouse/dir_facts.go`, and
`clickhouse/import_block_writer.go`, and remove production summary, vector,
and raw-DGUTA writers. Cover all 7 acceptance tests from B1.

- [ ] implemented
- [ ] reviewed

### Batch 1 (parallel, after item 3.2 is reviewed)

#### Item 3.3: B2 - Publish Readiness Last [parallel with B3]

spec.md section: B2

Write `wrstat_dir_projection_sets` only after facts, child edges, and any
selected derived index complete, and make tree readers check row-exists
readiness before using snapshot facts. Cover all 4 acceptance tests from B2.

- [ ] implemented
- [ ] reviewed

#### Item 3.4: B3 - Retry And Partition Cleanup [parallel with B2]

spec.md section: B3

Implement deterministic retry reset, active snapshot rewrite refusal,
partition cleanup across snapshot tables, provider error callbacks, and
diagnostics in `clickhouse/dguta_writer.go`,
`clickhouse/active_snapshot_cleanup.go`, `clickhouse/provider.go`, and
`cmd/summarise_diagnostics.go`, covering all 7 acceptance tests from B3.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
