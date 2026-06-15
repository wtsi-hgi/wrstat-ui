# Phase 6: Fast spool verification

Ref: [spec.md](spec.md) sections C1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 6.1: C1 - Hash-Based Manifest Verification

spec.md section: C1

Update `internal/chspool/spool.go` and `internal/chspool/spool_test.go` so
`VerifyManifest` uses writer-close expected manifest evidence to verify
identity, table presence, table key, row count, byte size, schema, and SHA256
without full gob decode when safe, covering all 8 acceptance tests from C1.
Write the hash and size tests so they fail before the implementation.

- [ ] implemented
- [ ] reviewed
