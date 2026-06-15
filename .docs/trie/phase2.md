# Phase 2: Schema and trie builder

Ref: [spec.md](spec.md) sections A1, A2, A4, B1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Batch 1 (parallel)

#### Item 2.1: A1 - Clean Trie Schema [parallel with A2]

spec.md section: A1

Replace the ClickHouse bootstrap DDL in `clickhouse/schema/*.sql` and
`clickhouse/schema.go` so trie-native serving tables use numeric directory
columns and the string-heavy serving tables are removed or rewritten. Add
`clickhouse/clean_schema_test.go` coverage for all 7 acceptance tests from A1.

- [ ] implemented
- [ ] reviewed

#### Item 2.2: A2 - Deterministic ID Assignment [parallel with A1]

spec.md section: A2

Implement `clickhouse/trie_builder.go` so an external sorted directory stream
builds deterministic `dir_id`, `parent_id`, depth, child-count, and
`subtree_end` rows for each mount snapshot. Add
`clickhouse/trie_builder_test.go` coverage for all 6 acceptance tests from A2.

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 2.3: A4 - Manifest Readiness And Cleanup [parallel with B1]

spec.md section: A4

Implement `clickhouse/trie_readiness.go` so trie-native snapshot and active-set
tables are validated by manifest row counts and SHA256 hashes before serving,
and cleanup removes inactive, tombstoned, failed, and replaced trie partitions.
Cover all 5 acceptance tests from A4 in `clickhouse/trie_readiness_test.go`.
Depends on item 2.1 schema table names and numeric columns.

- [ ] implemented
- [ ] reviewed

#### Item 2.4: B1 - Exact Directory Resolution [parallel with A4]

spec.md section: B1

Implement collision-safe `ResolveTrieDir` and `ResolveTrieDirs` helpers in
`clickhouse/trie_paths.go`, using path hashes only as an index and verifying
`wrstat_dirs.full_path` before returning IDs. Cover all 5 acceptance tests from
B1 in `clickhouse/trie_paths_test.go`. Depends on item 2.1 `wrstat_dirs`
schema and item 2.2 ID semantics.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
