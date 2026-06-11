# Phase 1: Parent packet contract

Ref: [spec.md](spec.md) sections A1, A2, A3, A4, A5

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer`
skills.

## Items

### Batch 1 (parallel)

#### Item 1.1: A1 - Exact current summaries stay canonical [parallel with A2]

spec.md section: A1

Implement the exact current summary routes in `clickhouse/database.go`,
`cmd/where.go`, and `server/where.go` so broad/default summaries stay on
`wrstat_dir_facts` and ready full-filter summaries use
`wrstat_dir_filter_all`, without using `wrstat_parent_facts` as the primary
summary for the clicked directory. Cover all 2 acceptance tests from A1 in
`clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`, and
`server/server_test.go`.

- [ ] implemented
- [ ] reviewed

#### Item 1.2: A2 - Parent packets are coherent request units [parallel with A1]

spec.md section: A2

Implement parent packet readers, cache keys, cache counters, and packet result
reuse in `clickhouse/parent_facts.go` and `clickhouse/database_cache.go` so a
high-fanout parent packet serves child summaries and child counts together.
Cover all 3 acceptance tests from A2 in `clickhouse/database_dirinfo_test.go`.

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 1.3: A3 - Child presence uses packet child counts [parallel with A4]

spec.md section: A3

Route `DirsHaveChildren` in `clickhouse/database.go`, `cmd/where.go`, and
`server/where.go` through packet `has_children` and
`has_filter_children` values, recording named fallbacks when filtered packets
are unavailable. Cover all 3 acceptance tests from A3 in
`clickhouse/database_dirinfo_test.go`, `cmd/where_test.go`, and
`server/server_test.go`. Depends on item 1.2 packet cache and counters.

- [ ] implemented
- [ ] reviewed

#### Item 1.4: A4 - `Tree.Where` traverses frontiers once [parallel with A3]

spec.md section: A4

Update `Tree.Where` traversal in `clickhouse/database.go` to group frontier
directories by mount and parent packet, load each distinct packet once, and
reuse current and child summaries without per-child fanout. Cover all 2
acceptance tests from A4 in `clickhouse/database_dirinfo_test.go`. Depends on
item 1.2 packet cache and counters.

- [ ] implemented
- [ ] reviewed

### Item 1.5: A5 - REST tree endpoint reuses one packet

spec.md section: A5

Update `server/tree.go` so one cold endpoint request shares the same provider
packet for child summaries and child `HasChildren` values, including broad,
full-filter, unused, and unchanged queries. Cover all 3 acceptance tests from
A5 in `server/server_test.go`. Depends on items 1.2 and 1.3.

- [ ] implemented
- [ ] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill
(review all items in the batch together in a single review
pass).
