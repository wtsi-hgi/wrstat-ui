# Phase 5: Glob / full-text

Ref: [spec.md](spec.md) sections F1, F2, F3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

Route basename globs to `wrstat_files.name` and full-path globs to
the `wrstat_dirs` catalog skip indexes, so the 1.3B-row files table
is never scanned for directory-path matching; reconstruct full paths
on the result page via the catalog. Depends on Phases 1-3 (catalog
skip indexes + `dir_id`-keyed files table). Runs in parallel with
Phases 4 and 6.

## Items

### Item 5.1: F1+F2 - basename and full-path glob routing

spec.md section: F1, F2

In `clickhouse/file_api.go`, compile each pattern: basename-only
(direct-child / extension / dotfile) -> `name` filter on
`wrstat_files`; path-bearing (full-path / substring / recursive) ->
candidate `dir_id`s or `[dir_id, subtree_end)` ranges from the
`wrstat_dirs` `ngrambf_v1`/`tokenbf_v1` skip index + regex on
`full_path`, then intersect with files by `dir_id`. Preserve
gitignore-style multi-pattern semantics (`*` not crossing `/`, `**`
crossing boundaries, `?`), the `(ownerEnabled=0 OR uid=? OR has(gids,
gid))` permission filter, ordering, pagination, and the `CountByGlob`
-> `FindByGlob` fallback above 32 patterns. Signatures
(`FindByGlob`/`CountByGlob`) unchanged. Verified jointly with F3 in
`clickhouse/file_api_test.go`. Depends on Phases 1-3.

- [ ] implemented
- [ ] reviewed

### Item 5.2: F3 - result full-path reconstruction

spec.md section: F3

In `clickhouse/file_api.go`, after the files read for
`FindByGlob`/`ListDir`, resolve `full_path` for the result page via
one catalog `dir_id IN (...)`, then `Path = full_path + name` and
populate `ParentDir`. Update existing test file
`clickhouse/file_api_test.go`. Covers all 3 acceptance tests from the
F section (FindByGlob direct-child/recursive/extension/dotfile paths
+ ordering + pagination + dedup identical to baseline; CountByGlob
counts match baseline; full-path glob reads `wrstat_dirs` skip-index
and `wrstat_files` only by `dir_id`, verified via EXPLAIN/read-rows
in the benchmark). Depends on Item 5.1.

- [ ] implemented
- [ ] reviewed
