# Phase 5: Split telemetry + amplification guard + golden-shape (C1, C2, C3)

Ref: [spec.md](spec.md) sections C1, C2, C3

Depends on Phase 4 (the derived child phase must exist before it can be
named, measured, and asserted).

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 5.1: C1 - Split the collapsed full-filter phase

spec.md section: C1

Replace the collapsed `importPhaseFullFilterAllInsert`
("wrstat_filter_all_insert") with two named phases
`wrstat_dir_filter_all_insert` and `wrstat_child_filter_all_insert`, each
exposing rows, bytes, duration, and rows/sec, in BOTH the spool-load report and
the `clickhouse-perf import` report (`ImportPhaseDurationsMS` + `TableStats`).

Files: `clickhouse/summarise_spool_loader.go`, `clickhouse/dguta_writer.go`,
`internal/chperf/import.go`. Test files: corresponding `_test.go`.

Covering all 2 acceptance tests from C1 (spool-load report contains both new
phase keys with non-zero duration and NO `wrstat_filter_all_insert` key;
`clickhouse-perf import` report has both phase keys with rows, bytes, duration,
and a computable rows/sec).

- [ ] implemented
- [ ] reviewed

### Item 5.2: C2 - Amplification ratio + waiver gate

spec.md section: C2

Depends on Item 5.1 (the split phases/telemetry feed the attribution). Compute
the full-filter rows/input-row amplification ratio from the existing
`TableStats.RowAmplificationVs*` fields, attributed to dir vs derived child.
Warn (slog) when amplification per input row > 5; hard-fail the load before
publish when > 10 unless an explicit debug/waiver env var or flag is set (e.g.
`WRSTAT_FILTER_AMPLIFICATION_WAIVER=1`). The chosen ratio basis must not block
legitimate t283-shaped density (~6.9x per table) on the waiver-free path.

Package: `clickhouse/` (or `internal/chperf/`). Test file: corresponding
`_test.go`.

Covering all 4 acceptance tests from C2 (amplification 6 without waiver
completes with a warn-level log attributing the ratio to dir vs child;
amplification 11 without waiver returns a named error before publish and does
not activate the snapshot; amplification 11 with the waiver completes and
publishes with a warn logged; t283-shaped density stays within the hard-fail
threshold on the default waiver-free path).

- [ ] implemented
- [ ] reviewed

### Item 5.3: C3 - Golden-shape telemetry assertion

spec.md section: C3

Depends on Items 5.1 and 5.2 (the split phase keys and amplification fields
must exist to be asserted). Add a report-level golden-shape check asserting the
split phase keys and the full-filter amplification fields exist and are
non-zero, mirroring the existing `final_gate` E3 amplification check
(`validateFinalGateE3TableStatsRowAmplification` /
`tableStatsDerivedEvidencePass`).

Files: `internal/chperf/final_gate.go`. Test file:
`internal/chperf/final_gate_test.go`.

Covering all 3 acceptance tests from C3 (check FAILS naming the missing phase
when `wrstat_dir_filter_all_insert` or `wrstat_child_filter_all_insert` is
absent; FAILS when either full-filter table's
`RowAmplificationVsDirFacts`/`RowAmplificationVsCatalog` is zero; PASSES when
both phase keys are non-zero and both tables' amplification fields are
populated).

- [ ] implemented
- [ ] reviewed
