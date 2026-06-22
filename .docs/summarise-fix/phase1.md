# Phase 1: Shared GUTA helpers (A1)

Ref: [spec.md](spec.md) sections A1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 1.1: A1 - Extract shared GUTA helpers from dirguta

spec.md section: A1

Extract `dirguta`'s currently-unexported per-file GUTA logic into shared
exported helpers so the directory-centric build (Phase 2) and the DFS
`Operation` path call the SAME code, producing byte-for-byte identical
digests. Extract exactly the set named in the spec Architecture: `gutaStore`
(map + refTime) and its methods `add`, `addForEach`, `subtractFromStore`,
`sort`, `drainInto`; `inodeEntry` + `handleHardlink`;
`mergeSeenHardlinks`/`updateExistingHardlink` (subtree seen-set merge);
`getGUTA`; `gutaKey`/`gutaKeys`/`gutaKeysFromEntry`/`gutaKeyPool`; plus the
file-type/age helpers (`FileTypeWithTemp`, `IsTemp`, `FilenameToType` are
already exported). The existing `DirGroupUserTypeAge.Operation` lifecycle must
keep calling these with no observable behavioural change.

Files: `summary/dirguta/dirguta.go` (+ a new shared file in `summary/dirguta`
if needed). Test file: `summary/dirguta/dirguta_test.go`.

Covering all 2 acceptance tests from A1 (existing dirguta tests pass unchanged
after extraction; single-dir regular/temp/hardlinked-file digest matches the
pre-extraction golden bytes).

This is the sequential foundation for Phase 2; no behaviour change is
permitted.

- [ ] implemented
- [ ] reviewed
