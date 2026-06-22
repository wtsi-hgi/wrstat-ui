# Phase 7: Fast spool verify (D1) + progress log fix (E1)

Ref: [spec.md](spec.md) sections D1, E1

Independent of the build/derive work (Phases 4-6); this phase can run in
parallel with Phases 4-6. D1 needs the manifest version bump. The two items in
this phase are independent of each other and form a parallel batch.

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Batch 1 (parallel)

#### Item 7.1: D1 - Trust manifest counts on spool reuse [parallel with 7.2]

spec.md section: D1

Make completed-spool verification trust recorded per-table row counts plus byte
size and SHA256 instead of paying a full gob decode. Persist trusted per-table
row counts in the manifest at initial writer close (already in
`TableManifest.Rows`); bump the manifest `Version` to mark trusted-count
semantics. When reusing a COMPLETE spool, `VerifyTables` verifies identity,
schema/version compatibility, file presence, byte size, and SHA256 and TRUSTS
`TableManifest.Rows` rather than decoding every row; any
size/hash/schema/identity/version/presence mismatch still rejects the spool
with `ErrManifestMismatch`.

Files: `internal/chspool/spool.go`. Test file:
`internal/chspool/spool_test.go`.

Covering all 4 acceptance tests from D1 (valid complete spool: `VerifyManifest`
returns nil without gob-decoding any table file, asserted via an injected
decode hook that must not fire; modified table bytes (size or SHA256 differ):
`ErrManifestMismatch`; manifest `Version` older than the trusted-count version:
`ErrManifestMismatch` forcing a safe rebuild; missing required table file:
`ErrManifestMismatch`).

- [x] implemented
- [x] reviewed

#### Item 7.2: E1 - Report true final parsed record count [parallel with 7.1]

spec.md section: E1

Make `logParseResult` report the actual final parsed record count (not the last
whole-million progress value) when parse completes. Diagnostics only; no speed
change.

Files: `cmd/summarise_diagnostics.go`. Test file:
`cmd/summarise_diagnostics_test.go`.

Covering all 2 acceptance tests from E1 (a parse of exactly 1,500,000 records
with a 1M progress interval logs 1,500,000 on success, not 1,000,000; a parse
of 0 records logs 0 with no spurious progress line).

- [x] implemented
- [x] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill (review all
items in the batch together in a single review pass).
