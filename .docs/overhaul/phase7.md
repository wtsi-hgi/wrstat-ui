# Phase 7: Benchmark study (the mandatory gate)

Ref: [spec.md](spec.md) sections J1, J2, J3, J4, J5, J6 (also closes
B4, D4, F3.3)

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

This is the acceptance gate and is mandatory. It depends on Phases
1-6 being landed and reviewed. The baseline itself was captured in
Phase 0 (before any overhaul change), so the baseline binary/worktree
and report under `.tmp/agent/overhaul/` already exist; this phase does
the harness-extension half of J1 (removing the children/parent-facts
and navigation-object plumbing), runs the full matrix against the
Phase 0 baseline, and enforces the gates. Do NOT invent a new report
format - reuse the existing
`perfreport.Report`/`Operation`/`TableStats`/`QueryMetrics`
structures and the Bolt comparison harness.

## Items

### Item 7.1: J1 - baseline capture and harness reuse

spec.md section: J1 (harness-extension half; capture done in Phase 0)

The baseline `clickhouse` HEAD was already captured in Phase 0
(preserved built binary or separate worktree under
`.tmp/agent/overhaul/`) before any overhaul change. Here, reuse and
extend `internal/chperf/` (`import.go`, `query.go`, `final_gate.go`,
`clickhouse_api.go`) and the Bolt comparison harness, keeping the
existing report structures. Remove the
children/parent-facts plumbing and the navigation-object selection
apparatus from `import.go`/`query.go` alongside the table/type
deletions (mirroring B3/H2). Update existing test files
`internal/chperf/*_test.go`. Covers both J1 acceptance tests (no
per-table metric / phase / amplification / navigation-shape candidate
references `wrstat_children` or `wrstat_parent_facts`; `internal/chperf`
builds and `go vet`s clean with no `NavigationObject*` or
`navigationShape*` reference). Foundation for the rest of this phase.

- [x] implemented
- [x] reviewed

### Item 7.2: J2 - datasets

spec.md section: J2

Assemble at minimum: a mixed Lustre/NFS subset; a directory-heavy
NFS tree; a high-fanout parent (~11k direct children); a
many-small-mounts active-virtual simulation keeping `/nfs` virtual;
and the largest practical production-like local dataset. State which
datasets are used and why a bounded subset still reproduces each
access pattern; if a bounded dataset cannot reproduce a pattern, say
so explicitly. Depends on Item 7.1.

- [x] implemented
- [x] reviewed

### Item 7.3: J3 - import / storage metrics

spec.md section: J3

Report summarise wall time, CPU, max RSS; spool bytes; ClickHouse
compressed AND uncompressed bytes per table (`TableStats`); active
part counts; rows per table; and the explicit
path-text-bytes-before-vs-after duplication reduction. Record the
measured max-dirs-per-snapshot (justifies `UInt32`, widen to
`UInt64` only if approaching 2^31). Closes B4 (states whether the
"ids near-free / leaner spool" claim held). Depends on Items 7.1,
7.2.

- [x] implemented
- [x] reviewed

### Item 7.4: J4 - per-query-type metrics and the canonical matrix

spec.md section: J4

Benchmark every query type in the spec's matrix (Exact directory,
Batch directory, Children/presence, Subtree/recursive, Disktree,
File API, Glob/full-text, Virtual/active, Basedirs/quota,
Maintenance) with a per-type before/after delta, recording
server-side p50/p95/p99, `ReadRows`/`ReadBytes`/`ReadMarks`, result
rows, and a result digest for correctness. A missing type is a spec
failure. Also satisfies F3 acceptance test 3 (EXPLAIN/read-rows
proof the files table is not scanned for path text). Depends on
Items 7.1, 7.2.

- [x] implemented
- [x] reviewed

### Item 7.5: J5 - cache scopes

spec.md section: J5

Reproduce the existing harness's cache scopes
(`fresh_provider_per_repeat`, `cold_provider_with_cold_query_cache`,
`same_provider_cold_then_warm`, `same_query_client`,
`ancestor_directory_each_repeat`, `new_directory_each_repeat`,
`visible_child_directory_each_repeat`,
`startup_cache_warming_contract`, `provider_update_cold_cache`,
`same_provider_same_dir`) so the high-fanout repeated-read pathology
is exercised before and after. Depends on Items 7.1, 7.4.

- [x] implemented
- [x] reviewed

### Item 7.6: J6 - gates and final report

spec.md section: J6

Enforce the gates in `internal/chperf/final_gate.go`: correctness
(hard - every query type identical to baseline, no wrong row ever);
absolute cold UX (hard - p95 < 100 ms for exact dir / file stat /
permission path / direct-child list, p95 < 500 ms for recursive /
filtered / glob / Disktree / `Where`); storage (hard - no hot row
stores a path string, path text one copy per dir per snapshot,
filter tables retained unless collapsed under D4 with a cited
measurement, compressed+uncompressed bytes per table vs baseline);
relative performance (report-only per-type deltas, regressions
explained but must still satisfy the absolute UX gate). Closes the
D4 collapse decisions with cited measurements. Update existing test
files `internal/chperf/*_test.go`. Covers all 4 acceptance tests
from J6 (every matrix type has p50/p95/p99 + delta or the gate
fails; no hot-row path string + per-table compressed/uncompressed
reported; cold UX p95 thresholds met; any collapsed materialisation
cites a measurement meeting its gate). Depends on Items 7.1-7.5.

- [x] implemented
- [x] reviewed
