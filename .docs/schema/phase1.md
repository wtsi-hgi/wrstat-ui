# Phase 1: Clean DDL and bootstrap

Ref: [spec.md](spec.md) sections A1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 1.1: A1 - Bootstrap Clean Schema V1

spec.md section: A1

Replace `clickhouse/schema/*.sql` with final schema-v1 objects, update
`clickhouse/schema.go` bootstrap and validation for version `1`, and add
clean-schema absence plus Bolt-dependency tests covering all 7 acceptance
tests from A1.

- [x] implemented
- [x] reviewed
