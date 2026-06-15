# Phase 2: Spool + writers carry ids

Ref: [spec.md](spec.md) sections H1, H2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

Rewrite the spool format and the ClickHouse writers/loader to emit
the catalog stream plus id-keyed rows (no repeated path strings),
preserving the readiness/resume machinery. Depends on Phase 1
(catalog schema + ids on `RecordDGUTA`). Together with Phase 1 this
unblocks the parallel phases 3-6.

## Items

### Item 2.1: H1 - spool format carries one catalog stream + id-keyed rows

spec.md section: H1

In `internal/chspool/spool.go`: add a `wrstat_dirs` catalog stream
(rows `dir_id, parent_id, subtree_end, depth, name, full_path,
child_dir_count, child_file_count, path_hash` - `full_path` once per
directory); `FileRow` replaces `ParentDir string` with `DirID
uint32` (keeps `Name`); `DirFactRow` replaces `Dir string` with
`DirID uint32` (+ `ParentID` and `SubtreeEnd`, D1);
`ChildFilterAllRow`/`DirFilterAllRow`/
`DirFilterAgeAllRow` replace path strings with `ParentID`/`DirID`/
`SubtreeEnd`; remove `ParentFactRow` and the `wrstat_children`/
`wrstat_parent_facts` streams; basedirs/active-virtual rows carry ids
(+ external string fallback) per G1/G2; keep `Format`/`Manifest`/
`TableManifest` machinery and the deterministic table order, adding
the catalog stream first in that order; update per-table `Rows`
counters. Update existing test file `internal/chspool/spool_test.go`.
Implements H1 (no standalone acceptance list; verified jointly via
H2's tests). Depends on Phase 1.

- [ ] implemented
- [ ] reviewed

### Item 2.2: H2 - writers and loader emit/load id rows

spec.md section: H2

Add new catalog writer `clickhouse/catalog.go` (buffers + inserts
`wrstat_dirs` rows from `RecordDGUTA` id fields; also serves as the
path<->id resolver buffer); the dguta writer drives it. Update
`clickhouse/dguta_writer.go`, `file_ingest_operation.go` (write
`dir_id` from the shared `DirIDAllocator` (B5, injected via
`cmd/summarise.go`) + `name`, no `parent_dir`),
`mount_dir_projection_writer.go` (facts rows keyed by `dir_id`,
carrying `parent_id` + `subtree_end`), derived-index writers (numeric
filter rows), `import_block_writer.go` (mechanism unchanged; row
counters updated), `summarise_spool_loader.go` (load catalog stream
first, then id-keyed tables; readiness/manifest records new tables'
row counts + SHA256). Delete the now-dead child-edge and parent-facts
machinery that only fed the removed `wrstat_children`/
`wrstat_parent_facts` tables: the `db.DGUTAChildrenWriter` interface
(B3) and its `summary/dirguta` plumbing, `dgutaWriter.AddChildren` plus
the parent-facts derived-index writer/readers and the `NavigationObject`
apparatus in `clickhouse/parent_facts.go` (deleted), the parent-fact
`whereTraversal`/`DirInfo` packet subsystem in `clickhouse/database.go`,
its packet cache in `clickhouse/database_cache.go`, the
`parentFactsParentDir` callers in `clickhouse/dir_filter_all.go`, and the
parent-facts fallback-route counter in `clickhouse/perf_counters.go`
(loader child/parent-facts arms drop too); child listings now come from
the catalog `parent_id` band (E1) and "has children" from
`child_dir_count` (A1). Preserve the `DGUTAWriter` interface (`Add,
SetBatchSize, SetMountPath, SetUpdatedAt, Close`); `SetMountPath` still
sets D up front for B2. Update existing test file
`clickhouse/summarise_spool_loader_test.go`. Covers all 7 acceptance
tests from H2 (full summarise->spool->load correctness vs baseline,
twice-written byte-identical determinism, spool-bytes / path-text-bytes
reduction reported, interrupted-mid-batch resume produces a complete
correct snapshot, production-spool streams id-keyed with no
children/parent-facts stream, a repo-wide search finds no reads/writes
of the deleted tables, and `go build`/`go vet` are clean with no
orphaned reference to a deleted symbol or table), and B5 acceptance
tests 1-4 (each `wrstat_files` row's `dir_id`, incl. the mount-root
entry under `D-1`), since file ingest reads the allocator here. Depends
on Item 2.1 and Phase 1 Item 1.6 (`DirIDAllocator`).

- [ ] implemented
- [ ] reviewed
