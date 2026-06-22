# Blocker: Phase 3 Item 3.1

## Item

Phase 3 Item 3.1 / spec section A5: Perf gates for the non-contiguous
directory-centric build versus the contiguous fast path.

Required acceptance evidence:

- Non-contiguous `scratch127` and `t283` 1.5M prefixes complete with measured
  build-phase bytes written under 100 MB and wall time within the specified
  fast-path relationship.
- Healthy contiguous Lustre 1.5M run stays within +/-10% of the pre-change
  wall time, `MaxRSSBytes`, and spool bytes baseline.

## Impossibility

The local environment cannot produce the required measurements:

- `clickhouse-client --host 127.0.0.1 --port 9000` returned connection refused,
  so the ClickHouse-backed perf harness cannot run here.
- No completed `.wrstat-ui-clickhouse-spool` directories were present under
  `.tmp/agent/summarise-fix/`.
- No structured before-baseline A5 build report was available locally. The
  existing prose evidence is not a machine-checkable before/after gate input.

The implementor added final-gate code/tests for machine-checkable A5 evidence,
but the acceptance measurements themselves cannot be generated or verified in
this environment without the missing service and artifacts.

## Alternatives

1. Provide a running ClickHouse instance plus the bounded 1.5M input prefixes
   and before-baseline reports, then rerun Phase 3.
2. Provide completed spool/build report artifacts for `scratch127`, `t283`, and
   healthy Lustre before/after runs so the final-gate evidence can be verified.
3. Defer Phase 3 until a production-like perf environment is available, while
   continuing independent phases that do not depend on A5.
