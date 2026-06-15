# Phase 7: Parse diagnostics

Ref: [spec.md](spec.md) sections B3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 7.1: B3 - Final Parse Count Logging

spec.md section: B3

Update `summary/summariser.go`, `cmd/summarise_diagnostics.go`,
`summary/summariser_test.go`, and `cmd/summarise_diagnostics_test.go` so
`summary.Summariser` stores the final parsed record count and
`summariseDiagnostics.logParseResult` logs it on parse success and failure
without changing periodic progress callbacks, covering all 3 acceptance tests
from B3.

- [ ] implemented
- [ ] reviewed
