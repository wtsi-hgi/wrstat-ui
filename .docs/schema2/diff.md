# Schema2 Difference Notes

This compares the original `.docs/schema/spec.md` design with the current
code after the `.docs/schema2/spec.md` work and follow-up fixes.

## Short version

The original schema work made ClickHouse the clean source of truth for wrstat
tree data. It focused on correctness: one canonical directory summary table,
one child-edge table, one file table, clear snapshot readiness markers, and no
hidden fallback to old Bolt/raw data.

Schema2 keeps that foundation, but adds a performance layer for the shapes of
questions the UI asks most often. The main idea is simple: keep the original
truth tables, then add a few extra copies of selected data sorted in ways that
ClickHouse can read quickly.

That tradeoff is very ClickHouse-specific. ClickHouse is excellent when it can
read a few columns from large compressed blocks and skip blocks using the table
sort order. It is much less magical when a query needs to unpack big arrays or
find "all children of this parent" from a table sorted some other way. Schema2
therefore gives ClickHouse tables that match the hot UI questions.

## ClickHouse in plain English

ClickHouse is a columnar database. Instead of storing each row as one little
record, it stores each column separately. If a query needs only `dir`, `count`,
and `size`, ClickHouse can often avoid reading the user/group/type vectors,
timestamps, names, and other columns.

Its "primary key" is mostly a sort order, not a uniqueness rule. If the table is
ordered by `(mount_path, snapshot_id, parent_dir, dir)`, then a query for one
parent directory can jump to a small area of the table. If the same data is
ordered by `(mount_path, snapshot_id, dir)`, that same parent query may have to
look much harder.

ClickHouse is also happiest with big scans and pre-shaped summaries. It is not
a pointer-following tree database. Duplicating a small amount of data into a
second table with a different order is normal in ClickHouse when it turns a
slow interaction into a narrow read.

## What the first schema did

The first schema spec introduced a clean ClickHouse model:

- `wrstat_dir_facts` became the canonical directory summary table. It has one
  row per mount directory, with scalar summaries for common all-files/file-only
  cases and vector columns for detailed group/user/type/age breakdowns.
- `wrstat_children` became the canonical direct-child table.
- `wrstat_dir_projection_sets` became the readiness marker for a snapshot's
  directory summary data.
- `wrstat_files` remained the canonical file API table.
- Active mounts became append-only events, with the active view computed from
  those events instead of in-place mutation.
- Snapshot import became atomic from the UI's point of view: write the snapshot
  rows first, write readiness last, then publish the active mount event.
- Old compatibility paths were removed from production reads: no Bolt fallback,
  no raw DGUTA fallback, and no browser/process-cache dependency for correctness.

That version deliberately avoided extra optimization tables unless performance
proved they were needed.

## What schema2 changed

Schema2 makes three extra ClickHouse structures mandatory.

### 1. AgeAll filter rows

New table: `wrstat_dir_filter_ageall`.

The old canonical facts table stores detailed summaries in arrays: group IDs,
user IDs, file types, ages, counts, sizes, and bucket vectors all line up by
position. That is compact, but a filtered query can force ClickHouse to read and
unpack those arrays for many directories.

The new AgeAll table flattens the most important filter case into ordinary
rows. "AgeAll" means the user is not asking for a specific age bucket. For that
case, the table stores one row per directory/group/user/type combination, with
count, size, and bucket summaries already separated out.

In plain terms: instead of asking ClickHouse to open every box and search
inside, schema2 puts labels on the boxes ClickHouse most often needs.

The current code routes eligible filters to this table when:

- the filter is not empty,
- the age is `DGUTAgeAll`,
- and at least one group, user, or file type filter is present.

Age-specific filters still fall back to the canonical vector table.

### 2. Active-prefix rollups

New tables:

- `wrstat_active_prefix_rollups`
- `wrstat_active_prefix_filter_ageall`
- `wrstat_active_prefix_rollup_sets`

The UI often asks about active top-level prefixes such as `/`, `/lustre/`, and
`/nfs/`. Without a rollup, answering those questions can mean joining or
aggregating across multiple active mounts.

Schema2 records small active-set summaries for those prefixes. The active set is
identified by an `active_set_id`, a fingerprint of the currently active mounts
and snapshots. That ID is also part of the cache key, so cached answers belong
to the exact set of active snapshots that produced them.

There is a scalar rollup for unfiltered/all-file questions and an AgeAll filter
rollup for the same owner/type filters described above.

### 3. Parent-ordered facts

New table: `wrstat_parent_facts`.

The original `wrstat_dir_facts` table is ordered by directory path. That is good
for exact directory lookup, but it is not ideal for "show me the immediate
children of this directory".

`wrstat_parent_facts` duplicates the directory facts and adds `parent_dir` and
`has_children`. Its sort order begins with the parent directory, so navigation
queries can read a tight group of rows instead of searching through the
directory-facts order.

The schema2 investigation considered projections and other child-fact layouts,
but the current code defaults to `wrstat_parent_facts` unless a measured
alternative proves it beats the gates.

## Routing and cache changes

The current code does not replace the canonical tables. It chooses the cheapest
safe route for each query:

- exact scalar directory summaries still use maintained scalar columns when
  they fit;
- eligible AgeAll owner/type filters use `wrstat_dir_filter_ageall`;
- navigation defaults to `wrstat_parent_facts`;
- active prefix summaries use active-prefix rollups when the active set is
  ready;
- unsupported filters fall back to the canonical fact vectors.

Cache keys now include the path, filter, permission inputs where relevant,
`active_set_id`, schema version, and query version. That prevents an answer for
one active snapshot set from being reused for another.

Follow-up fixes tightened this:

- provider swaps clear the response cache;
- AgeAll readiness cache state is reset when active metadata changes;
- performance reports record result counts, not only timings;
- final gates allow the intended `Info()` vector-count behavior;
- summarise spool retry cleanup uses the longer cleanup timeout, avoiding
  retries that fail because cleanup was timed like an ordinary query.

## Import and cleanup changes

For the direct ClickHouse DGUTA writer, schema2 adds derived-table work during
snapshot import:

- write canonical directory facts;
- write child rows;
- write `wrstat_dir_filter_ageall`;
- write `wrstat_parent_facts`;
- write projection readiness;
- publish the active snapshot;
- refresh active tree and active-prefix summaries best-effort;
- drop old partitions from canonical and derived tables.

The production ClickHouse summarise spool path is different. It currently
spools and reloads:

- `wrstat_files`;
- `wrstat_dir_facts`;
- `wrstat_children`;
- `wrstat_dir_projection_sets`;
- basedirs tables.

It does not currently spool/load `wrstat_dir_filter_ageall` or
`wrstat_parent_facts`. Its publish step still uses the shared snapshot-switch
logic, so it drops those derived partitions for the new snapshot and may attempt
active-prefix refresh, but the derived rows themselves are not loaded by the
spool loader.

That distinction matters when interpreting summarise performance: `clickhouse
perf import` and the direct writer exercise more schema2 derived-table writes
than the current production spool path.

## What stayed the same

The canonical truth is still the same:

- directory summaries live in `wrstat_dir_facts`;
- child edges live in `wrstat_children`;
- file APIs read `wrstat_files`;
- basedirs behavior is unchanged;
- snapshot publishing remains event-based and atomic from the reader's view;
- there is still no hidden Bolt fallback in production reads.

The schema version also remains `1`; schema2 is a performance extension of the
ClickHouse schema, not a user-visible data model reset.

## Why t283_imaging summarise got slower

The reported before/after numbers are:

| Run | Max memory | Wall time |
| --- | ---: | ---: |
| Before schema2 work | 36,281 MB | 5h42m |
| Current code | 41,878 MB | 9h34m |

That is an increase of 5,597 MB, about 15.4 percent, and an increase of 3h52m,
about 67.8 percent.

I checked `~/output/nfs`. The t283 artifacts present there are
`stats.gz` at 6,095,109,852 bytes and `logs.gz` at 303,400 bytes. I did not find
the exact full summarise job records behind the two measurements in that
directory, so this section treats the two numbers above as external
measurements and explains the most likely causes from current code and local
performance artifacts.

### The most likely direct cause: current summarise does more I/O

The current ClickHouse summarise command uses a resumable local spool. It first
parses the stats file and writes typed gzip/gob spool files for files,
directory facts, child rows, readiness, and basedirs. Then it reads those spool
files back, inserts them into ClickHouse, verifies counts, and publishes the
snapshot.

That retry safety is valuable, but it changes the cost model. A full t283 run
now has to:

- read and decompress the original 6.1 GB `stats.gz`;
- write a large local spool;
- hash/check the spool for manifest verification;
- read the spool again;
- insert rows into ClickHouse;
- count rows in ClickHouse for verification.

If the earlier run wrote directly to ClickHouse, the 9h34m result is plausibly
paying for an extra full local write/read/check cycle. That would affect wall
time much more than peak memory, which matches the observed shape: time rose
about 68 percent while memory rose about 15 percent.

### The schema2 direct-writer tables are still a cost in some paths

When the direct ClickHouse DGUTA writer is used, schema2 adds real per-directory
write work:

- `wrstat_parent_facts` duplicates directory facts in parent order;
- `wrstat_dir_filter_ageall` adds flattened owner/type rows for AgeAll filters;
- active-prefix rollups are refreshed after publish.

Those tables are intentionally small compared with files, but they are not
free. t283 is exactly the kind of mount where this matters because the tree and
directory summaries are large. Local 2-million-line t283 import artifacts from
the pre-current work show 818,479 `wrstat_dir_facts` rows and 818,478
`wrstat_children` rows; the directory projection insert alone took about
44 seconds out of an 81 second import. That says the directory-summary side of
this dataset is already the expensive part before adding more derived shapes.

The schema2 prototype notes also show that a small four-mount subset produced
103,980 AgeAll filter rows beside 46,307 parent-fact rows. The compressed size
was small, but it is still extra row construction, batching, compression, insert
work, and ClickHouse part management.

For the current production spool path, though, those two derived tables are not
the best explanation because the spool loader does not currently load them.
They are more likely to explain slowdowns seen in the direct import/performance
harness path than in `summarise` itself.

### Why memory increased

The current path keeps more moving pieces alive:

- the summariser still has to build the in-memory directory/GUTA summaries;
- the spool writer builds canonical scalar/vector directory rows;
- file, fact, child, and basedirs spool encoders have buffers and gzip state;
- publish/retry verification has additional manifest and row-count bookkeeping;
- direct-writer runs also maintain AgeAll and parent-fact batches.

The 5.6 GB RSS increase is consistent with more transient row/buffer state and
larger live batches, but the local artifacts do not prove a single allocator or
table is responsible. The shape looks like "more machinery around the same big
tree" rather than one obviously runaway structure.

### Other contributors to check

These are plausible but not proven from the files currently present:

- output location: if the spool is written under the same storage path as the
  large input/output tree, storage latency can dominate;
- ClickHouse part/merge pressure: more insert streams and verification queries
  can block behind merges or slow disks;
- retry cleanup: the current fix gives cleanup a longer timeout, which is safer
  but can make a retry wait longer instead of failing quickly;
- active-prefix refresh: after publish, refresh is best-effort, but it can still
  add work when the needed derived data exists;
- dataset drift: the `stats.gz` file present for t283 is very large, and a
  small change in file/directory distribution can move summarise time more than
  a small change in compressed input size suggests.

## What would prove the cause

The next useful evidence would be a full t283 current run with phase timings and
table row counts preserved. In particular:

- record parse time, spool build time, spool load time, count verification time,
  mount switch time, old partition cleanup time, and active-prefix refresh time;
- record spool table bytes and rows from the manifest;
- record ClickHouse table rows/bytes for `wrstat_files`, `wrstat_dir_facts`,
  `wrstat_children`, `wrstat_dir_filter_ageall`, and `wrstat_parent_facts`;
- compare one run with the production spool path to one direct-writer import on
  the same t283 `stats.gz`.

My current best read is that schema2 improved cold query/read behavior by
adding ClickHouse-shaped lookup tables, while the summarise regression is more
likely dominated by the current resumable spool/retry import path and by extra
directory-summary bookkeeping. The new schema2 tables are a real cost in direct
import paths, but they are not the clearest direct explanation for the
production spool summarise numbers unless that path is changed to populate them.
