# Summarise at scale: immediate improvements and partitioning direction

This is an engineering note, not a feature specification. It records the
small changes that look worth making now and separates them from the larger
subtree-partitioning design that still needs discussion and measurement.

## Constraints and conclusion

The constraints are:

- `stats.gz` cannot be assumed to be in directory-contiguous or DFS order.
- Changing `wrstat` to promise DFS order is not a small or obviously beneficial
  fix.
- Node-local scratch is unavailable. Any scratch is on NFS.
- Writing a raw external sort to NFS is too expensive.
- A large snapshot must not be allowed to monopolise or overload ClickHouse,
  especially if a late phase will fail and cause a retry.

The best near-term shape remains the current two-pass `dirbuild` approach:

1. Read only enough topology in pass 1 to assign deterministic preorder
   `dir_id`, `parent_id`, and `subtree_end` values.
2. Re-read the original compressed input in pass 2 and aggregate rows against
   that topology.
3. Write the durable final spool once.
4. Publish that spool to ClickHouse with bounded concurrency, bounded batches,
   and resumable checkpoints.

This avoids sorting or rewriting every raw input row. The main work now is to
make the disk-backed aggregation NFS-friendly and correct, to make routing
depend on the input's shape rather than compressed bytes, and to isolate
ClickHouse publication from failures and retries.

## What the branch has already established

The history of the fixes is useful because it rules out several tempting
loops:

- Direct long-lived ClickHouse writes were vulnerable to timeouts, memory
  growth, and ambiguous partial state. The durable spool is the right boundary.
- Integer directory intervals are valuable, but the original streaming builder
  required directory-contiguous input.
- A full raw external sort restored correctness but produced excessive NFS
  scratch and parsing overhead.
- `dirbuild` is the better correction: sort/build only directory topology and
  read the original facts again, rather than sort all facts.
- Recent changes have reduced topology and GUTA memory, streamed ID assignment,
  added implied directories, and introduced SQLite-backed summaries.
- The newest risk has therefore moved from obvious heap exhaustion to the
  amount and pattern of NFS I/O, the still-large topology, and the cost/order of
  ClickHouse publication.

The current disk mode is selected after building the directory index and is
triggered at one million nodes. Its SQLite file is created below the partial
spool directory, so in production it is NFS scratch. Pass 2 currently performs
an SQLite upsert for essentially every fact, and roll-up reads each directory's
rows once for emission and again for merging into its parent. That is a large
number of small B-tree operations on NFS.

There is also a correctness risk to resolve before tuning that path. The
SQLite hardlink subtraction updates only `count` and `size`; it does not undo
age buckets or recompute `atime`/`mtime`. A hardlink whose combined file type or
age mask changes can also leave a zero-count old key behind. The existing
cross-subdirectory hardlink test does not force disk mode, so it does not cover
this behaviour.

## Concrete improvements to make now

The order below is intentional: establish correctness and evidence first,
then reduce NFS and ClickHouse load.

### 1. Differentially test and repair disk-backed hardlink aggregation

Add forced-disk tests that run the same fixtures through the in-memory and
SQLite paths and compare every emitted field, including:

- hardlinks repeated within one directory;
- hardlinks crossing sibling and nested subdirectories;
- differing names/file-type classifications for the same inode;
- differing timestamps and age masks;
- counts, sizes, minima/maxima, and every age bucket;
- rows whose aggregate key changes during hardlink merging.

The repair should avoid trying to reverse `min`/`max` values from an aggregate.
The smallest clean design appears to be to use the per-node inode maps that the
disk path already retains, but stop inserting provisional hardlink rows into
SQLite. Materialise a node by combining its ordinary SQLite rows with the
finalised inode entries in its map, then merge the inode map upward separately
while SQLite rolls up only ordinary rows. This uses no more hardlink memory than
the current code and removes the need for lossy subtraction. If that approach
does not fit the existing interfaces, use a separate exact hardlink relation;
signed count/size deltas alone are not sufficient.

Do this before adding a write-behind cache, otherwise caching will make the
existing edge cases harder to reason about.

### 2. Replace the compressed-size routing heuristic with an exact shape probe

At present, a compressed input at or above 32 MiB is routed straight to
`dirbuild`, while smaller reopenable inputs are probed for directory
contiguity. Extend the existing exact contiguity probe to every reopenable
input:

- stop immediately at the first violated directory boundary;
- use the existing fast path if the entire input is contiguous;
- use `dirbuild` if a violation is found;
- continue to route non-reopenable stdin conservatively to a one-pass-capable
  path or reject it explicitly where two passes are required.

For a badly unordered input the probe should normally stop early. The worst
case is one extra sequential read of a valid large gzip, which is preferable to
putting every large-but-well-shaped input through the more expensive disk path.
Record the row number and path depth of the first violation so later decisions
are based on evidence rather than file size.

### 3. Make SQLite aggregation sequential and write-combining where possible

After the hardlink fix, make two contained changes:

1. Add a fixed-memory write-behind accumulator keyed by
   `(dir_id, gid, uid, file_type, age_mask)`. Combine repeated facts in memory
   and flush in sorted key order at a byte-based limit. The limit must be fixed
   and observable; it must not grow with the number of directories.
2. Replace the separate `materialise(child)` and `merge(parent, child)` reads
   with one operation that fetches the child's compact rows once, emits the
   expanded result, upserts those same compact rows into the parent, and deletes
   the child.

The database is already using one transaction, journal and synchronous modes
off, exclusive locking, and a bounded cache. More pragma tuning is unlikely to
compete with eliminating millions of statements and duplicate reads.

Measure rows combined per flush, SQLite statements, database bytes, write
bytes, and time separately for pass 2 and roll-up. Retain the scratch database
on failure for diagnosis, but remove it after a successful canonical spool is
complete.

### 4. Add phase and shape telemetry that works during a multi-hour run

Split the current broad `parse` status into at least:

- contiguity probe;
- directory scan/index construction;
- ID assignment/index finalisation;
- pass-2 fact aggregation;
- SQLite roll-up and spool emission;
- each ClickHouse table or derived-table stage.

Report input rows, directory nodes, implied directories, maximum depth, RSS,
SQLite bytes, spool bytes, rows/second, and elapsed time. During ClickHouse
publication also report rows and bytes sent, batch count, server part count if
cheap to query, and the current checkpoint.

Add a bounded prefix/depth shape report. It need not retain every path: depth
histograms plus fixed-capacity heavy prefixes are enough initially. This will
tell us whether failures correlate with total rows, number of directory nodes,
wide shallow fanout, deep chains, hardlink population, or high GUTA cardinality.
Those measurements are also the input to any future dynamic partitioner.

### 5. Load and validate cheap ClickHouse tables before the huge file table

The loader currently inserts directories and then files before directory facts
and filters. Change the order to approximately:

1. directories;
2. directory facts;
3. age-all and full-filter rows;
4. derived child-filter rows and their validation;
5. files;
6. projection/readiness, active-virtual, and basedir finalisation.

This does not reduce the total successful load, but it makes schema,
amplification, join, and derived-filter failures happen before the largest
table is imported. Snapshot readiness must remain false until every stage has
been verified.

### 6. Checkpoint publication per table, not only after all spool tables

Extend the existing publish-state file with one verified checkpoint per table
and per derived operation. After inserting a table:

- verify the exact row count scoped to `(mount_path, snapshot_id)`;
- persist the completed checkpoint atomically;
- on retry, skip verified tables;
- reset only the incomplete table's snapshot partition before retrying it.

The derived child-filter insert is a single snapshot-wide query today. First
checkpoint it as one operation. If real measurements show it is still too
large, chunk it in primary-key-friendly ranges, most naturally by `age` and
then by `gid` ranges for an oversized age. Checkpoint each range and avoid a
chunk predicate that causes a full table rescan.

### 7. Put an immediate global concurrency cap on summarise jobs

`wr` already supports `Job.LimitGroups`, including a count limit. Add a watch
option that assigns every scheduled summarise job to a shared group such as
`wrstat-ui-clickhouse-import:1`. Defaulting to one concurrent job is the safe
starting point; installations can raise it after observing ClickHouse and NFS
capacity.

This first version caps the whole monolithic job, not just publication. That is
deliberately conservative and small: it protects ClickHouse immediately and
does not depend on NFS advisory locks or stale lease recovery. Direct/manual
summarise invocations should emit a warning when they are outside this
scheduler guard.

The future version should split spool build and publish into separate jobs so
several CPU/NFS-bounded builds can proceed while the ClickHouse importer remains
strictly limited.

### 8. Use byte budgets and server pressure guardrails for ClickHouse inserts

Current batch sizing is primarily a fixed row count, despite wide variation in
row width and filter amplification. Introduce a bounded byte target as well as
a maximum row count:

- flush when either limit is reached;
- configure targets per table class, especially files versus filter rows;
- record actual uncompressed bytes per batch;
- back off or pause between batches when active parts, merges, memory, or query
  latency cross a configured threshold;
- never increase a batch merely because a prior row-count experiment was
  faster if it substantially increases client or server memory.

This is safer than choosing a new globally larger row count. The prior
experiments already showed that a faster filter batch can cost about a
gigabyte of additional RSS.

## Future direction: dynamic subtree decomposition

Subdividing a mount is a credible answer to pathological topology, but a fixed
depth such as five is only a useful first mental model. A single subtree below
depth five may contain nearly the whole mount, while a wide shallow mount may
already be pathological above it. The partition boundary therefore needs to be
selected by measured work, not depth alone.

### 1. Plan a dynamic cut set, with a shallow coordinator

Use a bounded planning pass or upstream manifest to estimate, per path prefix:

- fact rows and path bytes;
- directory count and maximum depth;
- hardlink population;
- approximate distinct `(gid, uid, file type, age mask)` tuples;
- expected spool and ClickHouse row amplification.

Recursively descend only into prefixes that exceed a configured work budget.
The selected subtree roots form a non-overlapping cut set. A shallow job handles
the mount root through the cut boundaries; one job handles each selected
subtree. If a selected subtree is still oversized, apply the same planning rule
inside it. Depth is a guardrail, not the primary budget.

For example, “root through depth four plus each depth-five subtree” would be a
valid plan only when the measurements say every resulting piece is below the
budget.

### 2. Do not make every subtree job scan the whole `stats.gz`

Prefix-filtering the same gzip once per shard multiplies NFS reads by the
number of shards and is not acceptable. There are two plausible input designs:

- During one sequential split pass, route parsed rows into compressed subtree
  members/files and a shallow stream. This incurs one compressed sequential NFS
  write, but avoids a raw external sort and random scratch I/O.
- Preferably, teach `wrstat`'s existing final combine pass to emit a partition
  manifest and independently readable prefix partitions while it is already
  touching every row. Routing by prefix does not require DFS ordering.

The second design avoids adding another full pass to `wrstat-ui`. A splittable
container made of indexed independent gzip members is another variant: the
manifest maps prefixes to member ranges so jobs read only their own ranges.

### 3. Treat `wrstat` ordering and partition production as different questions

`wrstat` currently writes walk paths round-robin to multiple files. Its final
combine is a k-way merge of the current head row from each stat file, using a
lexical path comparison. A k-way merge cannot repair ordering violations within
an individual input stream, and lexical path order is not itself the required
directory-component preorder in all cases.

Therefore changing only the final comparator would not guarantee DFS order.
Guaranteeing it upstream would move the same sorting/buffering problem into
`wrstat`. However, its combine stage is a promising place to emit partitioned
streams and shape metadata because that requires only prefix routing, not a
global DFS sort.

### 4. Define shard merge semantics before implementing jobs

Most summary values compose naturally: counts and sizes add, buckets add,
minimum/maximum times merge, and GUTA maps merge by key. Four global properties
need an explicit design:

- **Directory IDs:** each shard reports its directory count; a coordinator
  prefix-sums deterministic ID ranges, and shards apply an assigned offset so
  global `dir_id`, `parent_id`, and `subtree_end` remain stable.
- **Hardlinks:** the same inode may cross shard roots. A separate boundary
  hardlink artifact or global inode reconciliation step must prevent double
  counting at ancestors.
- **Basedirs and shallow ancestors:** configuration and facts may cross a cut
  boundary and must be resolved by the coordinator.
- **Filter child counts/readiness:** direct-child and full-filter metadata must
  be combined without making a partially merged snapshot visible.

A subtree artifact should therefore contain not only normal spool rows but also
its root aggregate, local directory count/ID information, hardlink boundary
state, and child/filter reconciliation metadata.

### 5. Explore set-oriented SQLite roll-up as a single-process precursor

Before building distributed shard coordination, test whether roll-up can be
done one depth at a time with bulk SQL:

- insert/aggregate pass-2 facts in batches;
- emit all nodes at the deepest depth;
- aggregate them into their parents with `INSERT ... SELECT ... GROUP BY`;
- delete the completed depth and continue upward.

This is logical subdivision inside one process and may eliminate most
per-directory NFS round trips. Hardlinks should remain in a separate exact
relation and be reconciled before each depth is finalised. The observed maximum
depth is small compared with the number of directories, which makes depth-wise
bulk work worth benchmarking.

### 6. Split build from publish and compact the ClickHouse representation

The durable architecture should expose two resumable operations:

1. build and verify a spool on NFS;
2. enqueue a verified spool for a small, globally limited importer pool.

That lets filesystem work run without granting every worker simultaneous
ClickHouse load. It also gives operators a queue that can be paused without
discarding completed builds.

Separately, investigate preserving the compact age-mask representation through
the spool and possibly into ClickHouse. Expanding one logical tuple into many
age/filter rows before insertion creates high amplification and duplicate child
tables. This is a schema-level trade-off, not a quick fix: queries, pruning, and
materialisation cost must be measured before choosing it.

## Recommended execution order

Start with items 1–4 together: they make the currently running implementation
trustworthy and measurable. Items 5–8 are independent, incremental protections
for ClickHouse and retries. Collect the resulting shape and stage data from the
largest known mounts before choosing between depth-wise bulk SQLite and dynamic
subtree artifacts.

Do not restore the full raw external sort as the default fallback. Keep it, if
at all, as an explicit diagnostic/recovery tool with a preflight NFS-space
check, because it solves ordering by recreating the original NFS scratch
problem.
