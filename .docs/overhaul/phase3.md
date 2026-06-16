# Phase 3: Files table on `dir_id`

Ref: [spec.md](spec.md) sections C1, C2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

Rewrite the largest table (`wrstat_files`) to store `dir_id` instead
of `parent_dir`, and serve the file API via the catalog. Depends on
Phases 1-2 (catalog + spool/writers landed). Phases 3, 4, 5 and 6 may
proceed in parallel with each other (shared catalog, different
tables/queries).

## Items

### Item 3.1: C1 - `wrstat_files` keyed by `dir_id`

spec.md section: C1

Rewrite the DDL `clickhouse/schema/011_files.sql`: drop `parent_dir`
(String) and the `path` ALIAS; key rows by `dir_id UInt32 CODEC(Delta,
LZ4)` + `name`; `ORDER BY (mount_path, snapshot_id, dir_id, name)`;
keep `ext_idx` set index; `PARTITION BY (mount_path, snapshot_id)`.
Verified jointly via C2's acceptance tests (esp. C2.2 "no column
stores a directory path string"). Depends on Phases 1-2.

- [x] implemented
- [x] reviewed

### Item 3.2: C2 - file API resolves path->dir_id then point-looks-up

spec.md section: C2

In `clickhouse/file_api.go` serve `StatPath`/`IsDir`/`PermissionPath`
(resolve `(mount, parentDir)` -> `dir_id` via catalog path_hash+verify
or full_path projection, then point lookup by `dir_id, name`),
`ListDir` (`dir_id` then `ORDER BY name LIMIT/OFFSET`), recursive file
enumeration (`dir_id >= ? AND dir_id < ?` over `[dir_id,
subtree_end)`), and `PermissionAnyInDir` (resolve dir -> `dir_id`,
query `wrstat_dir_facts` by `dir_id` with `arrayExists`). Signatures
unchanged; `FileRow` keeps `Path`/`ParentDir` populated from the
catalog at read time; path-hash hits MUST verify `full_path` before
returning. Update existing test file `clickhouse/file_api_test.go`.
Covers all 7 acceptance tests from C2 (old-vs-new parity for all five
calls incl. Path/ParentDir/ordering/pagination, no path-string
column, baseline not-found behaviour, path-hash collision rejected
never wrong file, and mount-root StatPath/ListDir/PermissionPath/
PermissionAnyInDir via the reserved chain - data root `dir_id = D`,
its entry filed under `D-1`). Depends on Item 3.1.

- [x] implemented
- [x] reviewed
