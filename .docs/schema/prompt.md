# Prompt for a cohesive ClickHouse schema/query spec

Use this after completing `.docs/schema/investigate.md`. The investigation
should decide which schema shape to spec and should provide performance
evidence for that choice.

## Goal

Write a cohesive ClickHouse schema and query spec for `wrstat-ui` that turns
the current branch work into one clean design.

This ClickHouse work is still unreleased branch work. It has not been released
or used as a production schema that needs to be preserved. The final design
must therefore remain schema version 1 and must not contain migrations,
backwards-compatibility tables/views, fallback code, tests, or comments that
exist only to support earlier shapes from this branch.

The spec should read as if the chosen design was made this way from the start.

## Known Requirements

The final design must preserve:

- atomic snapshot publication per mount
- retry/recovery safety for failed deterministic snapshots
- bounded cleanup of old or failed snapshot data
- web-responsive Disktree and `where` paths
- correctness for UID, GID, file-type, and age filters
- bounded memory during large summarise imports
- current file-level API behaviour
- basedirs usage, subdirectory, and history behaviour
- no production dependency on Bolt
- no performance regression from the current branch on the datasets used in
  `.docs/bugfixes`, especially `scratch127`, `scratch122`,
  `/lustre/scratch120`, and `t283_imaging`

The spec must not assume the original Bolt-shaped raw tree schema is still the
right centre of the design. Use the investigation result to choose the best
ClickHouse-native layout.

## Required Spec Content

The spec must define the final schema-v1 DDL, including:

- active mount metadata tables/views
- tree fact tables
- tree summary or index tables, if selected
- active-tree or virtual-ancestor support tables, if selected
- file-level tables
- basedirs/history tables
- schema version table and bootstrap behaviour

For each table or view, specify:

- purpose
- whether it is canonical or derived
- engine, partitioning, and ordering
- compression and low-cardinality choices
- lifecycle and cleanup
- readiness markers, if any
- which read/write paths use it

The spec must also define:

- import pipeline ordering
- import batch/block-size rules
- active snapshot publish and retry recovery
- old snapshot cleanup
- query routing for `DirInfo`, `DirInfos`, `Children`, `DirsHaveChildren`, and
  `Where`
- query routing for `ListDir`, `StatPath`, `IsDir`, `FindByGlob`, and
  permission checks
- basedirs reader/store query routing
- cache boundaries and invalidation rules
- failure reporting for any best-effort or asynchronous maintenance work

## Clean-Branch Rules

Because this branch has not shipped a ClickHouse schema, the spec must require
cleanup rather than migrations:

- Fold final columns into their final `CREATE TABLE` statements.
- Remove branch-local ALTER files such as "add active column", "add child
  count", "add summary version", and "add file columns".
- Choose one stable active-view name and remove compatibility aliases.
- Remove test-only backfill/projection helpers from production code unless the
  final design genuinely uses them.
- Remove comments that describe old branch-local layouts or migrations.
- Keep schema version 1 unless there is a reason unrelated to backwards
  compatibility.
- Do not introduce a migration ledger just to preserve old branch states.

## Known Cleanup Targets

The investigation should already have decided these, but the spec must cover
the final outcome:

- Whether raw directory DGUTA rows remain canonical, become compacted import
  facts, or are replaced by a vector-first directory fact table.
- Whether any inverted filter index tables are needed for filtered `where` and
  child-existence queries.
- Whether repeated path strings should be replaced by deterministic directory
  ids and a directory dictionary.
- Whether active mount metadata should remain a `ReplacingMergeTree` row stream
  or become a clearer append-only event table.
- Whether active-tree fingerprint tables remain, are replaced by hierarchy
  tables, or are simplified.
- Whether projection readiness needs a version column in a clean schema-v1
  world.
- Whether `wrstat_files.ext_idx` is useful after extension-limited glob
  experiments.
- Whether `wrstat_tree_summary_sets.active_mount_count` has a purpose.
- Whether import batch writer code should be unified.

## Acceptance Tests And Perf Gates

The spec must require tests or perf gates for:

- fresh schema-v1 bootstrap under concurrent constructors
- active view tie ordering for same-millisecond rows
- retry cleanup preserving previous active snapshots when possible
- retry cleanup behaviour when no previous active row exists
- failed/current/old snapshot partition cleanup using cleanup timeout semantics
- projection or index readiness misses and hits, if readiness markers exist
- no age-specific query accidentally reading compacted all-age-only facts
- Disktree cold-provider, warm-provider, and provider-update timings
- filtered and unfiltered `where` timings
- t283-shaped directory-heavy import row amplification
- bounded import memory
- file glob extension cases with and without owner filtering
- basedirs usage/subdirs/history correctness
- no branch-local compatibility DDL or comments remaining

Use the evidence gathered by `.docs/schema/investigate.md` to set concrete
thresholds. If the investigation did not produce enough evidence to choose a
schema shape, stop and request the missing investigation instead of writing a
spec based on guesswork.
