# Phase 4: Query rewrite

Ref: [spec.md](spec.md) sections B2, B3, B4, C1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Item 4.1: B2 - Exact Path, List, And Reconstruction

spec.md section: B2

Rewrite exact file and directory APIs in `clickhouse/file_api.go` to resolve
parent paths through `wrstat_dirs`, query `wrstat_files` by numeric
`parent_id`, and reconstruct `FileRow.Path` and `FileRow.ParentDir` from the
directory catalog without changing public responses. Cover all 8 acceptance
tests from B2 in `clickhouse/file_api_test.go`. Depends on phase 3 import data
and phase 2 exact directory resolution.

- [ ] implemented
- [ ] reviewed

### Batch 1 (parallel, after item 4.1 is reviewed)

#### Item 4.2: B3 - Glob Find And Count [parallel with C1]

spec.md section: B3

Rewrite `FindByGlob` and `CountByGlob` in `clickhouse/file_api.go` to use
numeric direct-child and recursive subtree plans, including extension shortcut
verification, exact RE2 matching, de-duplication, limits, and offsets. Cover
all 8 acceptance tests from B3 in `clickhouse/client_file_api_test.go`.
Depends on item 4.1 path reconstruction.

- [ ] implemented
- [ ] reviewed

#### Item 4.3: C1 - Directory Facts And Tree Queries [parallel with B3]

spec.md section: C1

Rewrite `DirInfo`, `DirInfos`, `DirsHaveChildren`, `Children`, and `Where` in
`clickhouse/database.go` to read numeric facts by `dir_id`, `parent_id`, and
`[dir_id, subtree_end)` ranges while preserving returned path strings and
digests. Cover all 10 acceptance tests from C1 in
`clickhouse/database_dirinfo_test.go`. Depends on phase 3 imported facts.

- [ ] implemented
- [ ] reviewed

### Item 4.4: B4 - Permissions

spec.md section: B4

Rewrite `PermissionPath` and `PermissionAnyInDir` in `clickhouse/file_api.go`
to resolve numeric parent and directory IDs, read exact file or directory
entry permission rows, and handle mount-root metadata and facts without
changing truth values. Cover all 6 acceptance tests from B4 in
`clickhouse/file_api_test.go`. Depends on item 4.1 and should be coordinated
after the glob edits to avoid overlapping `file_api.go` changes.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
