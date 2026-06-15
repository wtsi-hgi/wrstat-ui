# Phase 3: Import rewrite

Ref: [spec.md](spec.md) sections A3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 3.1: A3 - Trie-Native Import And Publish

spec.md section: A3

Convert direct file import, DGUTA import, filter rows, child facts, readiness,
cleanup, folded active-prefix rollups, basedirs rows, and spool publish paths
to write numeric directory IDs. Update `fileIngestWriter`, `dgutaWriter`,
`chBaseDirsStore`, `summariseSpoolLoader`, and `internal/chspool` row structs
so every active snapshot directory reference carries the resolved `dir_id`.
Cover all 4 acceptance tests from A3 in `clickhouse/dguta_writer_test.go`.
Depends on phase 2 builder, resolver, schema, and readiness work.

- [ ] implemented
- [ ] reviewed
