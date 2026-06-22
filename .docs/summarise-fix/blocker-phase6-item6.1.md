# Blocker: Phase 6 Item 6.1

## Item

Phase 6 Item 6.1 / spec section B3: Query equivalence and load perf gate for
server-side `wrstat_child_filter_all` derivation.

Required acceptance evidence:

- Root, t283 mount-root, healthy Lustre, and high-fanout query paths compared
  before/after on a clean single-mount DB, with exact result counts/digests and
  no p50/p95 regression beyond noise.
- Scratch127 1.5M load proving dir-only insert plus derived child insert is
  materially faster than the previous combined full-filter baseline, and spool
  bytes drop by roughly the child table's former share.

## Impossibility

The local environment cannot run or verify the required measurements:

- The local ClickHouse harness PID recorded under
  `.tmp/agent/summarise-fix/phase6-20260622-impl/ch/clickhouse.pid` was not a
  running process.
- `clickhouse client --host 127.0.0.1 --port 19000 --query "SELECT 1"` returned
  `Code: 210. DB::NetException: Connection refused`.
- The bounded 1.5M input fixtures exist, but required healthy Lustre before/after
  query JSON artifacts were missing under
  `.tmp/agent/summarise-fix/perf/phase6-20260622-impl/`.

The acceptance tests require real query/load perf measurements. Creating fake
measurements, hardcoded pass data, or narrowing the paths would not satisfy B3.

## Alternatives

1. Restart or recreate the local ClickHouse harness on `127.0.0.1:19000`, then
   rerun Phase 6 from the existing 1.5M fixtures.
2. Provide completed healthy Lustre before/after query JSON artifacts under
   `.tmp/agent/summarise-fix/perf/phase6-20260622-impl/`.
3. Rebuild a fresh clean single-mount DB from the available `scratch127`,
   `scratch122`, and `t283` 1.5M fixtures and record new reports under
   `.tmp/agent/summarise-fix/perf/`.
