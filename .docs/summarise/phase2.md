# Phase 2: Canonical spool

Ref: [spec.md](spec.md) sections A1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 2.1: A1 - Canonical Dir Spool

spec.md section: A1

Update `cmd/summarise_spool.go` and `cmd/summarise_spool_test.go` so
`summariseDGUTASpoolWriter.Close` writes full-filter rows only to
`wrstat_dir_filter_all`, keeps `wrstat_child_filter_all` as a zero-row
manifest table, and records schema3 child readiness counts, covering all 3
acceptance tests from A1.

- [ ] implemented
- [ ] reviewed
