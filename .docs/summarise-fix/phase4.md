# Phase 4: Server-side child derivation (B1, B2)

Ref: [spec.md](spec.md) sections B1, B2

Independent of Phases 1-3 in principle (could run in parallel after Phase 1),
but per the spec Implementation Order it is sequenced after Phase 2 to avoid
conflicting `cmd/summarise_spool.go` edits.

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 4.1: B1 - Stop the gob double-write; keep dir canonical

spec.md section: B1

Drop the `WriteChildFilterAll` call in `cmd/summarise_spool.go`'s
`writeSchema3FullFilterRow` (the second write) and remove
`summariseChildFilterAllRowForDirFilterAll`. Remove `wrstat_child_filter_all`
from the spool table set / `Set` writer / manifest `tableOrder` /
`VerifyTables` / `countRows`, so only `wrstat_dir_filter_all` (the canonical
table) is written and the spool loader no longer loads a child spool file.

Files: `cmd/summarise_spool.go`, `internal/chspool/spool.go`. Test files:
`cmd/summarise_spool_test.go`, `internal/chspool/spool_test.go`.

Covering all 2 acceptance tests from B1 (`manifest.Tables` does not contain
`wrstat_child_filter_all` and no `wrstat_child_filter_all.gob.gz` exists in the
spool dir; dir full-filter row count equals N with the prior duplicate child
bytes absent).

- [x] implemented
- [x] reviewed

### Item 4.2: B2 - Derive child server-side as a distinct pre-readiness phase

spec.md section: B2

Depends on Item 4.1 (the child spool file/table must be dropped first). Run the
derived `INSERT..SELECT` (Architecture SQL, joining `wrstat_dirs` for
`parent_id`) into the same staging snapshot partition `(mount_path,
snapshot_id)` in a new distinct phase immediately after the dir full-filter
insert and before the schema3 readiness/snapshot-activation marker. On insert
error, return before readiness so the previous active snapshot/active set stay
visible; rely on the existing per-snapshot partition drop for cleanup (no new
cleanup path). Make the direct `clickhouse-perf import` path
(`clickhouse/dir_filter_all.go:flushLastPending`) consistent: write only dir,
then derive child server-side the same way. Confirm the exact
`wrstat_child_filter_all` column list against
`clickhouse/schema/015_child_filter_all.sql` so the SELECT produces
byte-identical rows to today's double write.

Files: `clickhouse/summarise_spool_loader.go`, `clickhouse/dir_filter_all.go`.
Test files: `clickhouse/summarise_spool_loader_test.go`,
`clickhouse/dir_filter_all_test.go`.

Covering all 6 acceptance tests from B2 (derived child row count equals
canonical dir full-filter count per `(mount, snapshot)`; per-`parent_id`
contents and aggregate digests identical between old double-write and derived
paths; forced derive failure leaves readiness marker unset, snapshot
unactivated, prior active set visible; completed-spool retry derives child
without reparsing `stats.gz`; failed/old/tombstoned partition's derived rows
removed by the existing partition drop with no separate child-cleanup call;
load report records the derived child row count and table stats).

- [x] implemented
- [x] reviewed
