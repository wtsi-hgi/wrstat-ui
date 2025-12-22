# Performance specification (Bolt baseline → interfaces → ClickHouse)

This document defines a repeatable performance test harness for wrstat-ui.

The harness must allow three comparable runs:

1. **Bolt (current code, before interface refactor)**
2. **Bolt (after interface refactor; behaviour must be identical)**
3. **ClickHouse (after ClickHouse backend is implemented)**

The key requirement is that (2) must not regress relative to (1), and (3)
should be comparable to (1)/(2).

----------------------------------------------------------------------

## Scope (what is measured)

Measure both:

- write-side ingest performance for creating databases from `stats.gz`
- read-side query performance for the operations that power the current
  UI/API

- Tree/Where functionality (DirGUTA queries)
- Basedirs functionality (usage, subdirs, history)
- Dataset freshness timestamps ("dbsUpdated" equivalent)

Do **not** measure ClickHouse file-level queries (ListDir/StatPath/Glob/etc).
Those are outside the current Bolt capability and are covered by
clickhouse-specific tooling.

All measurements are *in-process* (no HTTP server, no JSON encoding), so the
results reflect storage/query performance rather than HTTP overhead.

----------------------------------------------------------------------

## Common dataset input (shared naming + discovery)

All perf commands use the same dataset directory naming convention as
"wrstat multi":

- `<inputDir>/` contains one or more subdirectories.
- Each subdirectory name is `<version>_<mountKey>`.
- The latest numeric `<version>` per `<mountKey>` is considered the active
  dataset for that mount.

There are two different directory types used by perf commands:

1. A *stats input directory* containing dataset dirs with `stats.gz`.
  - Used by `bolt-perf import`.
  - Used by `clickhouse-perf import`.
2. A *Bolt DB output directory* containing dataset dirs with Bolt DB files.
  - Used by `bolt-perf query`.

The directory discovery rules must match server behaviour:

- Use `server.FindDBDirs(inputDir, required...)` to discover the latest
  dataset dirs.

### Mount path derivation (normative)

The perf harness must be able to derive a mount path from `<version>_<mountKey>`
without using filesystem calls.

Rules:

- Split the directory basename at the first `_`.
- Take the remainder as `<mountKey>`.
- Replace all `／` (U+FF0F FULLWIDTH SOLIDUS) with `/`.
- Ensure the returned mount path ends with `/`.

This is used for:

- selecting a representative start directory
- labeling timestamp results consistently across backends

----------------------------------------------------------------------

## Command surface (must exist)

### 1) Bolt performance CLI (must be implemented first)

Command name (normative):

- `wrstat-ui bolt-perf`

Subcommands (normative):

- `bolt-perf import <inputDir>`
- `bolt-perf query <inputDir>`

Flags (normative):

- `--backend <name>` (optional; default `bolt`)
  - Must be one of: `bolt`, `bolt_interfaces`.
  - This exists only so the same CLI can produce distinct JSON reports
    before vs after the interface refactor.
- `--owners <file>` (required)
  - Path to `gid,owner` CSV used by basedirs.
- `--mounts <file>` (optional)
  - Same semantics as `wrstat-ui server --mounts`.
  - When provided, apply to the basedirs reader.
- `--dir <abs/>` (optional)
  - When provided, this directory is used directly by the tree operations.
  - When omitted, the representative directory auto-selection algorithm is
    used.
- `--repeat <n>` (optional; default 20)
  - Number of timed repeats per operation.
- `--warmup <n>` (optional; default 1)
  - Number of warmup runs per operation (not recorded).
- `--splits <n>` (optional; default 2)
  - Used for the Where operation.
- `--json <file>` (required)
  - Write a machine-readable report to this path.
  - Stdout must still print the human-readable summary.

Additional flags for `bolt-perf import` (normative):

- `--out <dir>` (required)
  - Output directory that will receive `<version>_<mountKey>/` directories.
  - For each imported dataset, `bolt-perf import` must write:
    - `dguta.dbs` (dirguta DB directory)
    - `basedirs.db`
- `--quota <file>` (required)
  - CSV of `gid,disk,size_quota,inode_quota`.
- `--config <file>` (required)
  - Basedirs config file.
- `--maxLines <n>` (optional; default 0)
  - When non-zero, cap the number of parsed stat records per `stats.gz`.

`bolt-perf import` dataset discovery (normative):

- Discover dataset dirs using `server.FindDBDirs(inputDir, "stats.gz")`.
- For each discovered dataset dir, the input stats file is:
  `<datasetDir>/stats.gz`.

`bolt-perf query` dataset discovery (normative):

- Discover dataset dirs using
  `server.FindDBDirs(inputDir, "dguta.dbs", "basedirs.db")`.

Required files (normative):

- `bolt-perf query` must discover dataset dirs using:
  `server.FindDBDirs(inputDir, "dguta.dbs", "basedirs.db")`.

Database open behaviour (normative):

- For each discovered dataset directory, open:
  - the dirguta DB via `db.NewTree(paths...)` where `paths` are the full
    file paths to each `dguta.dbs` file.
  - the basedirs DBs via `basedirs.OpenMulti(ownersPath, paths...)` where
    `paths` are the full file paths to each `basedirs.db` file.

Cache prewarming (normative):

- After opening basedirs, prewarm caches by calling, in this exact order:
  - for each age in `db.DirGUTAges`:
    - `GroupUsage(age)`
    - `UserUsage(age)`

If prewarming fails, the command must fail.

Import behaviour (normative):

- `bolt-perf import` must build Bolt DBs using the same in-process codepaths
  used by `wrstat-ui summarise`:
  - Use `stats.NewStatsParser()` + `summary.NewSummariser()`.
  - Add only the dirguta and basedirs summarisers.
  - Do not produce usergroup/groupuser outputs.
- For each dataset dir, the output dataset dir must be:
  `<out>/<basename(datasetDir)>/`.
- The `basedirs` summariser must be configured using `--quota` and `--config`.
- The `basedirs` creator must have its modtime set to the `stats.gz` mtime.

Import timing (normative):

- `bolt-perf import` must report a single end-to-end wall time per dataset dir
  for creating both `dguta.dbs` and `basedirs.db`.
- It must also report overall wall time and effective throughput in
  records/second (based on the number of parsed stats records).

### 2) ClickHouse performance CLI (implemented later)

The ClickHouse performance CLI is specified primarily in `clickhouse_spec.md`.
For the purposes of comparisons, it must also support `--json <file>` and must
write reports in the same JSON schema defined below.

The ClickHouse perf suite used for comparison in this document is the subset
of the ClickHouse "query" suite that overlaps Bolt capabilities:

- Mount timestamps
- Tree DirInfo
- Basedirs GroupUsage

----------------------------------------------------------------------

## Workload selection (directory / ids)

To keep runs repeatable and to avoid hard-coding site-specific paths, the perf
commands must auto-select representative inputs from the dataset itself.

### Selecting a representative directory (normative)

If `--dir` is not supplied, the harness must choose a representative
directory using only storage queries.

Algorithm:

1. Discover active dataset dirs.
2. Derive mount paths from their directory names.
3. Sort mount paths lexicographically and pick the first as `startDir`.
4. Set `dir = startDir`.
5. Repeat up to 64 times:
   - Call `tree.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})`.
   - If the returned `DirInfo` is nil, fail.
   - If `DirInfo.Current.Count` is within `[1_000, 20_000]`, stop.
   - Otherwise, choose the child entry with the largest `Count` from
     `DirInfo.Children` and set `dir = child.Dir`.
   - If there are no children, stop and use the last `dir`.

The chosen `dir` is used by all timed tree operations.

### Selecting representative basedirs ids (normative)

The harness must pick representative ids and basedirs from usage data.

Algorithm:

1. Call `GroupUsage(db.DGUTAgeAll)`.
2. Sort the resulting slice by `UsageSize` descending.
3. Pick the first entry as:
   - `pickedGID = entry.GID`
   - `pickedBaseDir = entry.BaseDir`
4. Call `UserUsage(db.DGUTAgeAll)`.
5. Sort the resulting slice by `UsageSize` descending.
6. Pick the first entry as:
   - `pickedUID = entry.UID`
   - `pickedUserBaseDir = entry.BaseDir`

If either usage slice is empty, the command must fail.

----------------------------------------------------------------------

## Timed operation suite

All timings are wall-clock durations measured with Go's monotonic clock.

For each operation, run:

- `warmup` iterations (not recorded)
- `repeat` iterations (recorded)

The suite is run serially (no parallelism).

### Import suite (normative)

`bolt-perf import` measures the write-side creation cost.

For each discovered dataset dir, record one duration for:

- `import_total`
  - End-to-end time to read/decompress `stats.gz`, parse records, and write
    `dguta.dbs` + `basedirs.db` into the output dataset dir.

### Bolt suite (normative)

Operations must be executed in this order:

1. `mount_timestamps`
   - For each discovered dataset dir, read its directory mtime (Unix seconds)
     and map it to the derived mount path.
   - This mirrors current server behaviour.

2. `tree_dirinfo`
   - Call `tree.DirInfo(dir, &db.Filter{Age: db.DGUTAgeAll})`.

3. `tree_where`
   - Call `tree.Where(dir, &db.Filter{Age: db.DGUTAgeAll}, splitFn)` where
     `splitFn = split.SplitsToSplitFn(--splits)`.

4. `basedirs_group_usage`
   - Call `basedirs.GroupUsage(db.DGUTAgeAll)`.

5. `basedirs_user_usage`
   - Call `basedirs.UserUsage(db.DGUTAgeAll)`.

6. `basedirs_group_subdirs`
   - Call `basedirs.GroupSubDirs(pickedGID, pickedBaseDir, db.DGUTAgeAll)`.

7. `basedirs_user_subdirs`
   - Call `basedirs.UserSubDirs(pickedUID, pickedUserBaseDir, db.DGUTAgeAll)`.

8. `basedirs_history`
   - Call `basedirs.History(pickedGID, pickedBaseDir)`.

All operations must validate basic sanity (normative):

- For `tree_dirinfo`, the returned `DirInfo` must be non-nil and
  `DirInfo.Current.Count` must be > 0.
- For `basedirs_group_usage` and `basedirs_user_usage`, the returned slice must
  be non-empty.
- For subdirs/history operations, the returned slice may be empty, but the call
  must succeed.

If any sanity check fails, the command must fail.

### ClickHouse comparison subset (normative)

When ClickHouse is available, the comparable subset of the ClickHouse query
suite must produce the same operation names (below) so JSON comparisons are
straightforward:

- `mount_timestamps`
- `tree_dirinfo`
- `basedirs_group_usage`

----------------------------------------------------------------------

## Reporting (stdout + JSON file)

The perf commands must produce:

1. A human-readable summary printed to stdout.
2. A machine-readable JSON report written to `--json`.

### Human-readable summary (normative)

For each operation, print:

- operation name
- key inputs (dir, splits, picked ids)
- per-repeat duration (ms)
- p50/p95/p99 (ms)

Percentiles must be computed over the recorded repeats only.

### JSON report schema (normative)

The JSON file must contain a single JSON object with the following fields:

- `schema_version` (number): must be `1`
- `backend` (string): one of `bolt`, `bolt_interfaces`, `clickhouse`
- `git_commit` (string): current commit hash if available, else `""`
- `go_version` (string)
- `os` (string)
- `arch` (string)
- `started_at` (RFC3339 string, UTC)
- `input_dir` (string)
- `repeat` (number)
- `warmup` (number)
- `operations` (array)

Each element of `operations` must be an object with fields:

- `name` (string): one of the operation names above
- `inputs` (object): operation-specific inputs
- `durations_ms` (array of numbers)
  - For query operations, length must equal `repeat`.
  - For `import_total`, length must be `1`.
- `p50_ms` (number)
- `p95_ms` (number)
- `p99_ms` (number)

For `mount_timestamps`, `inputs` must include:

- `mount_paths` (array of strings) sorted lexicographically

For `tree_dirinfo` and `tree_where`, `inputs` must include:

- `dir` (string)
- `age` (number) (must be the numeric value of `db.DGUTAgeAll`)

For `tree_where`, `inputs` must also include:

- `splits` (number)

For basedirs operations, `inputs` must include:

- `age` (number) (must be the numeric value of `db.DGUTAgeAll`)

For `basedirs_group_subdirs` and `basedirs_history`, `inputs` must also
include:

- `gid` (number)
- `basedir` (string)

For `basedirs_user_subdirs`, `inputs` must also include:

- `uid` (number)
- `basedir` (string)

No additional fields are permitted; this keeps reports stable across time.

For `import_total`, `inputs` must include:

- `dataset_dir` (string): basename `<version>_<mountKey>`
- `mount_path` (string)
- `max_lines` (number)
- `records` (number): number of parsed stat records

----------------------------------------------------------------------

## How to use this for regression checking

### Bolt baseline (before interface refactor)

First create Bolt DBs from the stats input directory:

- `wrstat-ui bolt-perf import <statsInputDir> --out <boltOutDir> \
  --quota <quota.csv> --config <basedirs.config> \
  --owners <owners.csv> --json bolt_import_baseline.json`

Then run the query suite against the generated Bolt DB output directory:

- `wrstat-ui bolt-perf query <boltOutDir> --owners <owners.csv> \
  --json bolt_baseline.json`

Store `bolt_baseline.json` as the baseline.

### Bolt after interface refactor

Repeat the same import + query runs, but set:

- `--backend bolt_interfaces`

The workload selection algorithm must remain identical so the results are
comparable.

### ClickHouse comparison

1. Import the same datasets into ClickHouse using the ClickHouse perf import.
2. Run the ClickHouse perf query, writing `clickhouse.json`.
3. Ensure `clickhouse.json.backend = "clickhouse"`.

### Comparison rule (normative)

When comparing two JSON reports for the same backend or different backends:

- Compare p50/p95/p99 per operation.
- A regression is defined as:
  - p95 increasing by more than 10% for any operation, OR
  - p99 increasing by more than 20% for any operation.

If the environment (host, filesystem cache state, dataset size) differs, do not
treat differences as regressions; rerun both reports under the same conditions.
