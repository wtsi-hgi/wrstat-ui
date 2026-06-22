# Phase 6: Query + load perf gates (B3)

Ref: [spec.md](spec.md) sections B3

Depends on Phases 4 and 5 (the server-side child derivation and its split
telemetry must exist and be reviewed before query equivalence and load perf
can be gated).

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 6.1: B3 - Query equivalence and load perf gate

spec.md section: B3

Run `clickhouse-perf query` before/after the derive change on a t283 mount-root
path, a healthy Lustre path, and a high-fanout path, establishing the baseline
on a clean single-mount DB (the harness pre-flight glob-routing EXPLAIN
assertion fails on a polluted multi-mount DB). Prove result counts/digests
match exactly and p50/p95 do not regress beyond noise, and that the dir-only
insert plus the derived child insert total materially less than today's
combined `filter_all_insert` with spool bytes dropping by roughly the child
table's former share.

Test file: `clickhouse-perf` harness; results recorded under
`.tmp/agent/summarise-fix/perf/`.

Covering all 2 acceptance tests from B3 (root, t283 mount-root, healthy Lustre,
and high-fanout query paths before/after on a clean single-mount DB: result
counts and digests match exactly and p50/p95 do not regress beyond noise;
scratch127 1.5M dir-only plus derived child insert total materially less than
the ~18s combined baseline, with the derived insert near ~1-2s (measured 1.16s
for 1,122,958 rows) and spool bytes dropping by roughly the child table's
former share).

- [x] implemented
- [x] reviewed
