# Phase 3 Item 3.1 blocker: missing healthy contiguous 1.5M evidence

## Status

Phase 3/A5 remains blocked on evidence, not on ClickHouse availability.

Fresh current-after reports now exist for scratch127, t283, scratch120,
scratch122, scratch124, and scratch126. Scratch127 and t283 satisfy the
non-contiguous build-scratch portion of A5. The remaining A5 gates still need
one healthy contiguous Lustre 1.5M before/after pair. No tested healthy
candidate currently qualifies.

## Latest candidate rerun on 2026-06-22

New source inputs checked:

- `/home/ubuntu/output/lustre/20260517-200015_／lustre／scratch124/stats.gz`
- `/home/ubuntu/output/lustre/20260517-200015_／lustre／scratch126/stats.gz`

Bounded 1.5M inputs generated:

| candidate | bounded input | bytes | rows | lex drops |
| --- | --- | ---: | ---: | ---: |
| scratch124 | `.tmp/agent/summarise-fix/inputs/scratch124-1500k.stats.gz` | 29,850,771 | 1,500,000 | 0 |
| scratch126 | `.tmp/agent/summarise-fix/inputs/scratch126-1500k.stats.gz` | 32,005,025 | 1,500,000 | 0 |

Current binary:

`.tmp/agent/summarise-fix/phase3-a5-20260622/bin/wrstat-ui-current`

Fresh current reports:

| candidate | report | role | input shape | build path | complete | row cap | build scratch bytes | spool bytes | peak RSS bytes | build p95 ms | whole wall |
| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| scratch124 | `.tmp/agent/summarise-fix/phase3-a5-20260622/outputs/current-scratch124/20260622_／lustre／scratch124/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `healthy_lustre` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 52,869,284 | 908,951,552 | 11,691.564770 | 0:33.02 |
| scratch126 | `.tmp/agent/summarise-fix/phase3-a5-20260622/outputs/current-scratch126/20260622_／lustre／scratch126/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `healthy_lustre` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 63,401,828 | 1,163,177,984 | 13,324.893141 | 0:39.80 |

Both commands exited `0`, but both candidates are disqualified for the healthy
contiguous A5 role because the reports record `input_shape=non_contiguous` and
`build_path=dirbuild`, not `input_shape=contiguous` and
`build_path=contiguous_fast_path`.

Detailed rerun evidence is recorded in:

`.tmp/agent/summarise-fix/perf/a5-healthy-candidates-20260622.md`

## Existing current-after evidence

Run root:

`.tmp/agent/summarise-fix/phase3-a5-20260622/`

Earlier fresh reports:

| candidate | report | role | input shape | build path | complete | row cap | build scratch bytes | spool bytes | peak RSS bytes | build p95 ms | whole wall |
| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| scratch127 | `outputs/current-scratch127/20260622_／lustre／scratch127/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `scratch127` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 51,022,261 | 923,762,688 | 12,704.250846 | 0:33.99 |
| t283 | `outputs/current-t283/20260622_／nfs／t283_imaging/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `t283` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 134,148,611 | 7,712,612,352 | 48,358.287805 | 3:06.24 |
| scratch120 candidate | `outputs/current-scratch120/20260622_／lustre／scratch120/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `healthy_lustre` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 79,643,594 | 3,126,374,400 | 24,574.875232 | 1:20.08 |
| scratch122 candidate | `outputs/current-scratch122c/20260622_／lustre／scratch122/.wrstat-ui-clickhouse-spool/summarise_build_report.json` | `healthy_lustre` | `non_contiguous` | `dirbuild` | true | 1,500,000 | 0 | 89,439,368 | 2,648,641,536 | 21,804.936171 | 1:13.11 |

The scratch127 and t283 non-contiguous build scratch gate is checkable and
passes: both completed with `build_phase_bytes_written=0`, below the A5
100 MB cap.

## Gate validation status

| A5 gate | result | source |
| --- | --- | --- |
| scratch127 non-contiguous completes and build scratch bytes < 100 MB | PASS | `current-scratch127` report: `completed=true`, `row_cap=1500000`, `build_phase_bytes_written=0` |
| t283 non-contiguous completes and build scratch bytes < 100 MB | PASS | `current-t283` report: `completed=true`, `row_cap=1500000`, `build_phase_bytes_written=0` |
| non-contiguous wall within 1.5x healthy contiguous fast path | BLOCKED | no current healthy report has `input_shape=contiguous` and `build_path=contiguous_fast_path` |
| healthy contiguous Lustre wall/RSS/spool bytes within +/-10% before/after | BLOCKED | no qualifying current healthy contiguous report exists, so no comparable before baseline was generated |

## Before-baseline status

The isolated pre-change worktree is:

`.tmp/agent/summarise-fix/worktrees/phase3-before`

The detached commit is:

`0944cb2166ac81d399ec84bf82a80eb228eff104`

The pre-change binary is:

`.tmp/agent/summarise-fix/phase3-a5-20260622/bin/wrstat-ui-before-0944cb2`

No new pre-change baseline was run for scratch124 or scratch126 because neither
current report satisfied the required healthy contiguous identity check.

## Unblock condition

Provide or identify a real 1.5M healthy contiguous Lustre `stats.gz` input, or
explicitly approve a documented synthetic contiguous Lustre fixture as
acceptable for A5. Then rerun the current and pre-change binaries against that
input and validate the A5 wall, RSS, and spool-byte gates from the recorded
reports and time files.
