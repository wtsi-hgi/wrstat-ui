# Phase 6: Basedirs + active virtual namespace

Ref: [spec.md](spec.md) sections G1, G2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills. All
implementors follow the `go-conventions` skill.

Move basedirs into the id world with an explicit external-string
fallback, and give the cross-mount active virtual namespace its own
small id space that links to underlying mount-root `dir_id`s. Depends
on Phases 1-2. Runs in parallel with Phases 3, 4, 5. The two items
touch different schema/reader files and form a parallel batch.

## Items

### Batch 1 (parallel, after Phase 2 is reviewed)

#### Item 6.1: G1 - basedirs reference `dir_id` with external string fallback [parallel with 6.2]

spec.md section: G1

Rewrite `clickhouse/schema/006-009_basedirs*.sql` and
`clickhouse/basedirs_reader.go`: `_group_subdirs`/`_user_subdirs`
replace `basedir`/`subdir` strings with `basedir_id`/`subdir_id
UInt32` (resolved against the active snapshot catalog) plus
`basedir_external`/`subdir_external String` (used only when no
`dir_id` exists); `_group_usage`/`_user_usage` get `basedir_id` +
`basedir_external`; `wrstat_basedirs_history` keeps strings (spans
snapshots). Readiness MUST fail with `ErrIDUnresolved` if an
in-snapshot active `basedir`/`subdir` cannot resolve to a `dir_id`.
Reader signatures (`GroupUsage`, `UserUsage`, `GroupSubDirs`,
`UserSubDirs`, `History`) unchanged, returning the same path strings
(from `dir_id` or external column). Update existing test file
`clickhouse/basedirs_reader_test.go`. Covers all 3 acceptance tests
from G1 (readers match baseline paths/order/numbers; unresolvable
in-snapshot active basedir fails with `ErrIDUnresolved`;
external/historical path resolves via external column). Depends on
Phases 1-2.

- [x] implemented
- [x] reviewed

#### Item 6.2: G2 - per-active_set_id virtual catalog with its own id space [parallel with 6.1]

spec.md section: G2

Rewrite `clickhouse/schema/018_active_virtual_overlay.sql` and add
`clickhouse/virtual_namespace.go`: a per-`active_set_id` virtual
catalog (`wrstat_active_virtual_dirs`) numbering ONLY synthetic
virtual nodes with `virtual_id` (its own small id space), each
linking to `(mount_path, snapshot_id, mount_root_dir_id)` for
mount-root boxes; below a mount root, defer to that mount's
per-snapshot catalog and `dir_id` ranges (no mount-local dir copied).
`wrstat_active_virtual_summaries`/`_filter_all`/`_children`/`_sets`
keyed by `active_set_id` + `virtual_id` (mount-root boxes carry the
mount-local `dir_id` link); the above-root linear chain stays in the
per-mount catalog (B2), not here. `wrstat_active_virtual_children` is
rewritten numeric too: it survives the `wrstat_virtual_children`
deletion (a different table) but is NOT kept string-shaped - swap its
`parent_dir`/`child_dir` strings for parent/child `virtual_id` (+ the
mount-root `dir_id` link), and rewrite the retained geometry helpers
(`virtualChildRowsForMount`/`virtualChildRowsForMounts`/
`mergeVirtualChildRows`), the `virtualChildRow` type, and the
`activeVirtualChildRowsForMounts` writer (`dguta_writer.go`) to
populate those numeric fields. No `dir`/`parent_dir`/`child_dir` path
string survives in any active-virtual hot row (J6 storage gate).
Precedence (matches baseline): the overlay is the sole authority for
any path that is a strict prefix of an active mount root; the B2 rows
are never independently summed into those answers (no double
counting). Reuse the existing `active_set_id` derivation and active-set
readiness/cleanup (`active_mounts.go`,
`active_virtual_overlay.go`, `active_prefix_rollups.go`); only swap
string keys for `virtual_id`/`dir_id` links. Prune the deleted string
`wrstat_virtual_children`/`_sets` table and its writer/refresher/reader
(`clickhouse/virtual_children.go`, `clickhouse/provider.go`), rewriting
the live `Children` reader chain for virtual/ancestor paths
(`virtualChildrenForAncestor` plus the `database.go` caller chain
`virtualChildrenForReadyAncestorMounts` and its call sites) to serve
from the virtual catalog / catalog `parent_id` band, and rewrite
`activeVirtualMountChildCountsQuery` in
`clickhouse/active_virtual_overlay.go` to read the catalog `parent_id`
band instead of `wrstat_children`. Add test file
`clickhouse/virtual_namespace_test.go`. Covers all 7 acceptance tests
from G2 (root `/` aggregates selected mounts with separate
`/lustre`/`/nfs` boxes + correct totals; virtual children / filtered
virtual summaries / active-prefix rollups match baseline; virtual
catalog contains only synthetic nodes; `DirInfo("/lustre/")` aggregated
once not double-counted; overlay-is-sole-authority precedence for
above-root/virtual paths; `active_set_id` + readiness reused from
baseline; and no `dir`/`parent_dir`/`child_dir` string column in
`wrstat_active_virtual_children`/`_summaries`/`_filter_all`, which are
keyed by `virtual_id`/`dir_id`). Depends on Phases 1-2.

- [x] implemented
- [x] reviewed

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill (review all
items in the batch together in a single review pass).
