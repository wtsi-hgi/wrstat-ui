# Phase 4: Spool path and cleanup

Ref: [spec.md](spec.md) sections D2, D3, D4

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 4.1: D2 - Summarise spool carries schema3 tables

spec.md section: D2

Extend `internal/chspool/spool.go` with schema3 table constants, row types,
`TableOrder`, manifest entries, write methods, and verification behavior for
full-filter, schema3 readiness, and active virtual tables. Update
`cmd/summarise_spool.go` so the production summarise command writes every
schema3 spool table through normal summariser operations. Cover D2 acceptance
tests 1, 2, 5, and 6 in `internal/chspool/spool_test.go`; cover D2 acceptance
test 7 in `cmd/summarise_spool_test.go` using the actual command path. D2
acceptance tests 3 and 4 are loader-facing and are covered by item 4.2.

- [ ] implemented
- [ ] reviewed

### Item 4.2: D3 - Spool loader preserves atomic publish

spec.md section: D3

Update `clickhouse/summarise_spool_loader.go` so schema3 spool loads follow
the D1 publish order, verify decoded and inserted rows for every schema3 and
active virtual table, block readiness on missing or mismatched data, and clean
partial partitions before retry. Cover D3 acceptance tests 1-5 in
`clickhouse/summarise_spool_loader_test.go`, plus D2 acceptance tests 3 and 4
for missing schema3 or active virtual manifest entries. Cover D3 acceptance
test 6 in `cmd/summarise_spool_test.go` as an end-to-end command/load/database
test. Depends on item 4.1 spool table manifest support.

- [ ] implemented
- [ ] reviewed

### Item 4.3: D4 - Cleanup removes old partitions and active sets

spec.md section: D4

Extend `clickhouse/active_snapshot_cleanup.go` to remove old snapshot-scoped
schema3 partitions and old active-set overlay partitions only after safe
publish, while guarding the current active snapshot and active set. Cover all
3 acceptance tests from D4 in `clickhouse/active_snapshot_cleanup_test.go`.
Depends on items 4.1 and 4.2 table names and readiness semantics.

- [ ] implemented
- [ ] reviewed
