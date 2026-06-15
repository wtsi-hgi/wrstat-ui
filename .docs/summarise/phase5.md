# Phase 5: Telemetry and guardrails

Ref: [spec.md](spec.md) sections B1, B2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 5.1: B1 - Split Full-Filter Telemetry

spec.md section: B1

Update `clickhouse/dguta_writer.go`,
`clickhouse/summarise_spool_loader.go`, `internal/chperf/import.go`,
`internal/perfreport/report.go`, and their tests so production and perf
reports expose split full-filter table timing, byte, row, and rate evidence,
covering all 4 acceptance tests from B1.

- [ ] implemented
- [ ] reviewed

### Item 5.2: B2 - Full-Filter Amplification Guard

spec.md section: B2

Update `clickhouse/summarise_spool_loader.go`,
`internal/chperf/import.go`, and their tests to compute dir, child, and
duplicated full-filter amplification, warn above 5.0, and hard-fail above
10.0 only when the explicit debug or perf guard is enabled, covering all 3
acceptance tests from B2. This item depends on the report evidence from item
5.1.

- [ ] implemented
- [ ] reviewed
