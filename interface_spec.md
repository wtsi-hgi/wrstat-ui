# wrstat-ui storage abstraction spec (Bolt → ClickHouse)

## Goals (hard constraints)

1. Replace direct use of `go.etcd.io/bbolt` throughout the codebase with a
   storage-neutral design, enabling a future ClickHouse backend.
2. Create a new root package `bolt` that is the **only** production package
   allowed to import `go.etcd.io/bbolt`.
3. No bbolt types may leak across the boundary:
   - Do **not** re-export `*bolt.DB`, `*bolt.Tx`, `*bolt.Bucket`, cursors, or
     transaction semantics.
   - Interfaces must not expose buckets, cursors, `[]byte` values, or
     caller-built keys.
4. Public API surface of the new `bolt` package must be **minimal** and **fully
   tested**.
5. Only these may import `github.com/wtsi-hgi/wrstat-ui/bolt`:
   - `cmd/*`, `main.go`, test files (`*_test.go`), and `internal/*` test helper
     packages.
   - Domain packages like `db`, `basedirs`, and `server` must not import it in
     production code.
6. Must preserve all current user-visible behaviour:
   - Web UI and `wrstat-ui where` filters and results.
   - Multi-mount behaviour today (multiple Bolt “sources”).
   - `dbsUpdated` behaviour, but **timestamps must come from the storage
     layer**.
     - The only identifier exposed to callers is the **mount directory path**.
     - The timestamps map keys are mount paths only (no version prefixes).

## Non-goals

- No behavioural changes to query semantics, sorting, JSON shapes, or filters.
- No UI changes.
- No optimisation work beyond what is necessary to preserve existing performance
  characteristics.

## Current user-visible API contract (must remain)

### Tree/Where

- Endpoint: `GET /rest/v1/auth/tree` (and unauth variant if auth disabled)
  - Query params:
    - `path` (default `/`)
    - `groups` (comma-separated group names)
    - `users` (comma-separated usernames)
    - `types` (comma-separated file type strings; parsed via
      `db.FileTypeStringToDirGUTAFileType`)
    - `age` (string; parsed via `db.AgeStringToDirGUTAge`, `0` means all)

- Endpoint: `GET /rest/v1/auth/where`
  - Query params:
    - `dir` (default server default dir)
    - `splits` (default `2`)
    - plus the same filter params as above (`groups`, `users`, `types`, `age`)
  - Behaviour: returns directory summaries sorted by size.

### Basedirs

- Endpoints (secured under `/rest/v1/auth/…` when auth enabled):
  - `GET /rest/v1/auth/basedirs/usage/groups`
  - `GET /rest/v1/auth/basedirs/usage/users`
    - These are served from server caches; cache is built by calling the
      underlying `basedirs.Reader` across all `db.DirGUTAges`.
  - `GET /rest/v1/auth/basedirs/subdirs/group`
  - `GET /rest/v1/auth/basedirs/subdirs/user`
    - Query params: `id`, `basedir`, optional `age` (default `0`)
  - `GET /rest/v1/auth/basedirs/history`
    - Query params: `id` (gid), `basedir` (a mountpoint or a path under it)

### Database update timestamps

- Endpoint: `GET /rest/v1/auth/dbsUpdated`
- Response shape: JSON object mapping `mountPath` → Unix seconds.
  - `mountPath` is the mount directory path (string) that the stats were created
    for, e.g. `/some/mount/point/`.
  - The server must treat these keys as opaque mount paths; it must not derive
    them from directory names.
- Today: server derives timestamps from filesystem `mtime` of per-mount database
  directories.
- Required change: timestamps must be provided by storage, so ClickHouse can
  return mount-path timestamps too. Server should call
  `provider.BaseDirs().MountTimestamps()`, merge/normalise via the backend,
  and convert to Unix seconds for the response.

## Domain model summary (what storage must support)

### Current Bolt behaviour (reference)

- DirGUTA data is split across two Bolt files per dataset: `dguta.db`
  (bucket `gut`) and `dguta.db.children` (bucket `children`). Keys are the
  directory path with a trailing `0xff` byte; values are codec-encoded GUTAs.
  Children are codec-encoded `[]string` keyed by the parent dir.
- `db.Tree` combines results across all opened datasets. `DirInfo` merges
  GUTAs from every dataset for the requested dir, returns `ErrDirNotFound` if
  the dir is missing from all, and `Children()` de-dupes and sorts children
  from every dataset.
- Basedirs writer stores usage in `groupUsage`/`userUsage`, histories in
  `groupHistorical`, subdirs in `groupSubDirs`/`userSubDirs`. History append is
  skipped if the new timestamp is not strictly newer than the latest entry.
  `CopyHistoryFrom()` seeds histories from a previous DB before appending.
  `storeDateQuotasFill()` precomputes `DateNoSpace`/`DateNoFiles` using
  histories.
- Basedirs reader resolves mountpoints via longest-prefix match of the
  configured mount list (auto-discovered by default, overridable). Usage
  responses populate `Usage.Owner` from the owners CSV and `Usage.Name` via
  cached UID/GID lookups. `MultiReader` concatenates usage across DBs, returns
  the first non-nil subdir/history hit, and exposes `SetCachedGroup/User` for
  test convenience. History cleaning CLIs operate by deleting history keys not
  starting with a supplied prefix.
- Server today scans `<base>/<version>_<mountkey>/` dirs containing
  `dguta.db*` and `basedirs.db`, picks the highest numeric version per mount,
  and sets `/rest/v1/auth/dbsUpdated` from the `mtime` of each mount dir.
  Reloading is incremental via `Tree.OpenFrom` / `MultiReader.OpenFrom` and
  caches are rebuilt across all ages.

### DirGUTA (“tree/where”)

- Write path (summarise): repeated ingestion of `db.RecordDGUTA` values.
- Read path (server/UI/CLI):
  - `DirInfo(dir, filter)` returning summary + immediate children summaries.
  - `DirHasChildren(dir, filter)`.
  - `Where(dir, filter, splits)`.
- Filter semantics are defined in `db.Filter` / `db.GUTAs.Summary()`.

### Basedirs

- Write path (summarise): build a DB containing:
  - user/group usage by basedir
  - child subdir breakdown
  - group history series (time-ordered)
  - History continuity is the responsibility of the backend:
    - ClickHouse: history is stored in the single database and appended
      in-place.
    - Bolt: history is seeded from the previous run’s `basedirs.db` by passing
      the previous DB path to the Bolt writer constructor.
- Read path:
  - group/user usage for a given age
  - group/user subdirs for a given id+basedir+age
  - group history for a given gid+path (path resolved to mountpoint prefix)

## Storage-neutral interfaces

These interfaces must live in the domain packages `db` and `basedirs`.

### 1) Tree query interface (read-side)

`db.Tree` currently wraps `db.DB` which is tightly coupled to Bolt.
`db.DB` should be replaced by `db.Database` interface.

```go
package db

// Database is the low-level storage interface that db.Tree uses internally.
// This is implemented by the bolt package.
type Database interface {
  // GetDGUTA retrieves the DGUTA record for a directory.
  // Returns nil, ErrDirNotFound if the directory is not in the database.
  GetDGUTA(dir string) (*DGUTA, error)

  // GetChildren returns the child directory paths for a directory.
  // Returns empty slice if no children exist.
  GetChildren(dir string) ([]string, error)

  Close() error
}
```

`db.Tree` constructor changes from `NewTree(paths ...string)` to:

```go
package db

// NewTree creates a Tree that queries the given database.
func NewTree(db Database) *Tree
```

The Bolt package provides the `Database` implementation. It must preserve the
current multi-source semantics: combining DGUTA results across all underlying
datasets, returning `ErrDirNotFound` only if a directory is missing from all
sources, and de-duplicating + sorting children across sources.

### 2) Tree ingest interface (write-side)

`summary/dirguta` already depends on a minimal interface:

```go
// package summary/dirguta
// type DB interface { Add(dguta db.RecordDGUTA) error }
```

To make backend selection explicit and testable, define the following in `db`:

```go
package db

type DGUTAWriter interface {
  // Add adds a DGUTA record to the database.
  Add(dguta RecordDGUTA) error

  // SetBatchSize controls flush batching. cmd/summarise sets this to 10_000
  // today. Implementations must keep streaming semantics (no requirement to
  // hold the whole dataset in memory).
  SetBatchSize(batchSize int)

  // SetMountPath is the mount directory path the stats were produced for.
  // It must be an absolute path and must end with '/'.
  // Example: "/lustre/scratch123/".
  SetMountPath(mountPath string)

  // SetUpdatedAt sets the dataset creation time used for mount timestamps.
  // cmd/summarise currently uses the stats.gz file mtime for this purpose.
  SetUpdatedAt(t time.Time)

  Close() error
}
```

Normative requirement:
- Every successful ingest must persist `(mountPath, updatedAt)` so it can be
  returned later via `MountTimestamps()`.
- `cmd/summarise` must set `SetMountPath` and `SetUpdatedAt` on both writers
  using a required `--mount` flag (absolute path with trailing `/`) and the
  stats input `mtime`. Do not attempt to derive mount paths inside the server.

- Implementations must support streaming ingestion: callers never need to hold
  the entire dataset in memory; backends are responsible for batching and
  flushing as needed.

### 3) Basedirs ingest interface (write-side)

This is the storage-neutral contract used by `cmd/summarise` to build basedirs
data and update history.

```go
package basedirs

type Writer interface {
  // SetMountPath sets the mount directory path the stats were produced for.
  // It must be an absolute path and must end with '/'.
  SetMountPath(mountPath string)

  // SetMountPoints overrides mountpoint discovery for history key resolution.
  // When not called, the writer must auto-discover mountpoints from the OS.
  SetMountPoints(mountpoints []string)

  // SetUpdatedAt sets the dataset creation time used for history timestamps.
  // cmd/summarise currently uses the stats.gz file mtime for this purpose.
  SetUpdatedAt(t time.Time)

  // StoreGroupUsage adds a single group usage record.
  StoreGroupUsage(usage *Usage) error

  // StoreUserUsage adds a single user usage record.
  StoreUserUsage(usage *Usage) error

  // StoreGroupSubDirs adds sub-directory information for a specific group and basedir.
  StoreGroupSubDirs(gid uint32, basedir string, age db.DirGUTAge, subdirs []*SubDir) error

  // StoreUserSubDirs adds sub-directory information for a specific user and basedir.
  StoreUserSubDirs(uid uint32, basedir string, age db.DirGUTAge, subdirs []*SubDir) error

  // StoreHistory adds history information for a specific group and mountpoint.
  // The implementation handles appending to existing history.
  StoreHistory(gid uint32, mountpoint string, history []History) error

  Close() error
}
```

Note: `basedirs.BaseDirs` (the domain logic struct) will retain the logic to
calculate `Usage`, `SubDir`, and `History` from `IDAgeDirs`. It will then call
the `Writer` methods to persist them.

History update rule (must match current behaviour):
- For each `(gid, mountPath)` history series, append the new point only if
  `updatedAt` is strictly after the last stored point’s date; otherwise leave
  history unchanged.

Backend-specific history continuity:
- ClickHouse: read the last stored point for `(gid, mountPath)` and
  append/update in-place.
- Bolt: seed the new output DB’s history from a previous `basedirs.db` provided
  to the Bolt writer constructor (see Bolt package section), then apply the
  append rule above.

All backends must implement history updates as logical upserts into a single
history series per `(gid, mountPath)`; there must be no domain-level "copy
entire bucket" operations.

### 4) Basedirs query interface (read-side)

`server` uses these methods (directly or via caching):

```go
package basedirs

type Reader interface {
  // Usage is requested per age; server will call this for all ages.
  GroupUsage(age db.DirGUTAge) ([]*Usage, error)
  UserUsage(age db.DirGUTAge) ([]*Usage, error)

  GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error)
  UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error)

  // Returns history for a group for the mountpoint that contains `path`.
  // The implementation must resolve `path` to a mountpoint.
  History(gid uint32, path string) ([]History, error)

  // Matches current behaviour: used to override mountpoint auto-discovery.
  SetMountPoints(mountpoints []string)

  // Optional caches used in tests and to avoid repeated lookups when caller
  // already knows the names. No-ops are acceptable.
  SetCachedGroup(gid uint32, name string)
  SetCachedUser(uid uint32, name string)

  // Returns mount directory path -> last updated time.
  // Keys MUST be mount paths (absolute) and MUST end with '/'.
  MountTimestamps() (map[string]time.Time, error)

  Close() error
}

// HistoryIssue describes a history entry that would be considered invalid for
// a given mount-path prefix.
type HistoryIssue struct {
  GID       uint32
  MountPath string
}

// HistoryMaintainer defines storage-neutral maintenance operations over
// basedirs history.
type HistoryMaintainer interface {
  // CleanHistoryForMount keeps only history entries whose mount path has the
  // given prefix and removes all others.
  CleanHistoryForMount(prefix string) error

  // FindInvalidHistory returns the list of (gid, mountPath) pairs that would
  // be removed by CleanHistoryForMount for the same prefix. This is used by
  // the `clean` CLI in view-only mode.
  FindInvalidHistory(prefix string) ([]HistoryIssue, error)
}
```

Aggregation behaviour across mounts (current `basedirs.MultiReader`) must be
preserved by the storage implementation:
- Usage methods: concatenate results across all mounts.
- Subdirs/history: return the first successful result.

`MountTimestamps` must merge entries across all physical sources, returning
the latest `updatedAt` per mount path.

Group/user name caching: The `basedirs` types `Usage` includes `Name` which is
resolved from UID/GID. The current implementation uses `GroupCache` and
`UserCache` in `basedirs`. The Bolt backend must continue to populate
`Usage.Name` and `Usage.Owner` fields; name caches should still be honoured if
set via `SetCachedGroup` / `SetCachedUser`.

### 5) Backend bundle interface (required)

`server` must be wired to a single backend bundle, provided by `cmd/server`.
Define this interface in the `db` package as `Provider`.

```go
package db

type Provider interface {
  Tree() Database
  BaseDirs() basedirs.Reader

  // DGUTAInfo returns summary information about all DirGUTA data that this
  // provider serves. Implementations must support aggregating over multiple
  // physical sources (eg. many Bolt files or ClickHouse tables) while
  // presenting a single logical summary to callers.
  DGUTAInfo() (*DBInfo, error)

  // BasedirsInfo returns summary information about all basedirs data that
  // this provider serves.
  BasedirsInfo() (*basedirs.DBInfo, error)

  // OnUpdate registers a callback that will be invoked whenever the
  // underlying data changes (e.g., new databases discovered, ClickHouse
  // tables updated). The callback receives the updated Provider so the
  // server can rebuild caches from the new data.
  //
  // Implementations must:
  // - Call the callback on a separate goroutine (non-blocking).
  // - Guarantee the callback is not called concurrently with itself.
  // - Provide a fully-usable Provider when the callback is invoked
  //   (queries must work; old data connections are still valid until
  //    the callback returns).
  //
  // If cb is nil, any previously registered callback is removed.
  OnUpdate(cb func())

  Close() error
}
```

### 6) Backend reloading (internal implementation detail)

Reloading is an **internal concern** of each backend implementation; neither
`server` nor `cmd/server` should manage reload loops, filesystem scanning, or
incremental open/close logic.

**Bolt implementation:**
- The `bolt.OpenProvider(cfg)` constructor starts an internal goroutine that
  periodically scans the configured base path for new/changed database
  directories.
- When changes are detected, the backend:
  1. Opens the new databases.
  2. Updates its internal `Database` and `BaseDirsReader`.
  3. Invokes the registered `OnUpdate` callback (if any).
  4. Closes obsolete database connections after the callback returns.
- The scan interval is configured via `Config.PollInterval`.
- If `Config.PollInterval` is zero or negative, automatic reloading is disabled.

**ClickHouse implementation (future):**
- Must either poll for changes or hook into database-side notifications and
  invoke `OnUpdate` whenever underlying data changes so the server can rebuild
  caches.

**Server cache invalidation:**
- The server's `groupUsageCache` and `userUsageCache` are pre-serialized
  JSON/gzip payloads for the `/basedirs/usage/*` endpoints.
- On startup, `server` registers a callback via
  `provider.OnUpdate(s.rebuildCaches)`.
- When the backend detects new data and calls the callback, the server rebuilds
  these caches from the (now-updated) `BaseDirsReader`.
- The `uidToNameCache` and `gidToNameCache` are append-only lookups and do not
  require invalidation.

See `bolt.Config` in the "The new `bolt` package" section for configuration
fields including `PollInterval` and `RemoveOldPaths`.

**Write-side constructors are the new `bolt.NewDGUTAWriter` and
`bolt.NewBaseDirsWriter` helpers**; `cmd/summarise` must switch to them and set
mount path / timestamps explicitly.

## Required package boundary changes

### The new `bolt` package

The new root package `bolt` owns:

- All `go.etcd.io/bbolt` imports.
- All on-disk layout knowledge for Bolt:
  - DGUTA uses two bolt files per dataset (`dguta.db`, `dguta.db.children`).
  - Basedirs uses one bolt file (`basedirs.db`).
- Any filesystem layout knowledge currently embedded in `server/db.go`:
  - locating “latest version per mount” directories
  - incremental reload using add/remove lists
  - reading directory `mtime` as a fallback when older Bolt DBs do not contain
    explicit mount timestamps

Timestamp source of truth (Bolt backend):
- Preferred: the backend persists and reads explicit `(mountPath, updatedAt)`
  written by `db.DGUTAWriter`.
- Legacy fallback (only when explicit timestamps are absent):
  - Derive `mountPath` from the dataset directory name using the existing
    on-disk convention from `server/db.go`:
    - split the dataset directory basename on the first underscore (`_`)
    - treat the suffix (the part after the underscore) as the mount path string
  - Normalise by ensuring it is absolute and ends with '/'.
  - Use the dataset directory `mtime` as `updatedAt`.
  - This derivation happens inside the Bolt backend; the server still must not
    derive mount paths or timestamps from filesystem paths.

Minimal public API (required shape; keep as small as possible):

```go
package bolt

// OpenProvider constructs a backend bundle that implements db.Provider.
// When cfg.PollInterval > 0, the backend starts an internal goroutine that
// watches cfg.BasePath for new databases and triggers OnUpdate callbacks.
func OpenProvider(cfg Config) (db.Provider, error)

type Config struct {
    // BasePath is the directory scanned for database subdirectories.
    // Each subdirectory must be named "<version>_<mountpath>" and contain
    // files named DGUTADBName and BaseDirDBName.
    BasePath string

    // DGUTADBName and BaseDirDBName are the filenames expected inside each
    // database directory (e.g., "dguta.db", "basedirs.db").
    DGUTADBName   string
    BaseDirDBName string

    // OwnersCSVPath is required for basedirs name resolution.
    OwnersCSVPath string

    // MountPoints overrides mount auto-discovery for basedirs history resolution.
    // When empty, the backend must auto-discover mountpoints from the OS.
    MountPoints []string

    // PollInterval is how often to scan BasePath for new databases.
    // Zero or negative disables automatic reloading.
    PollInterval time.Duration

    // RemoveOldPaths controls removal of older database directories from
    // BasePath after a successful reload. When true, old paths are deleted;
    // when false, they are retained.
    RemoveOldPaths bool
}
```

Write-side construction is used only by `cmd/summarise`:

```go
package bolt

func NewDGUTAWriter(outputDir string) (db.DGUTAWriter, error)
// previousDBPath is the path to the previous run’s basedirs.db. Empty means
// start with no seeded history.
func NewBaseDirsWriter(dbPath string, quotas *basedirs.Quotas, previousDBPath string) (basedirs.Writer, error)
```

### Domain packages after refactor

- `db/` becomes a pure domain package plus query algorithms.
  - Keeps: `Filter`, `DirSummary`, `DirInfo`, `DCSs`, `DGUTA`, `GUTAs`,
    `RecordDGUTA`, age/filetype enums and parsing, `DBInfo` struct.
  - Keeps: `Tree` type and its methods (`DirInfo`, `DirHasChildren`, `Where`,
    `FileLocations`, `Close`).
  - Removes: all bbolt imports, `DB` type entirely (its write functionality
    moves to bolt), `dbSet` type.
  - Removes: `EncodeToBytes`, `DecodeDGUTAbytes`, and all bolt-specific
    encoding/decoding (these move to bolt).
  - New: `Tree` takes `Database` interface in its constructor. The Bolt
    package provides the `Database` implementation.
  - `db` must not import `go.etcd.io/bbolt` or `github.com/ugorji/go/codec`
    (codec is bolt-specific serialization).

- `basedirs/` becomes domain types + pure interfaces.
  - Keeps: `Usage`, `SubDir`, `History`, `IDAgeDirs`, `AgeDirs`,
    `SummaryWithChildren`, `Quotas`, `Config`, `DBInfo` struct, `DateQuotaFull`
    function, `GroupCache`, `UserCache`, `Error` type, mountpoint utilities.
  - Removes: all bbolt imports, `BaseDirs` writer type (replaced by domain logic
    calling `Writer`), `BaseDirReader` type, `MultiReader` type, `OpenDBRO`,
    `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`, `Info`, all
    encoding/decoding.
  - Removes: `github.com/ugorji/go/codec` import (codec is bolt-specific
    serialization).
  - The Bolt package reimplements the reader/writer using the domain types.

- `server/` must not:
  - open databases from filesystem paths,
  - scan filesystem for “latest version” dirs,
  - stat dirs for timestamps,
  - manage reload loops or polling,
  - import `go.etcd.io/bbolt`.

Instead:
- `server` stores only the `db.Provider` and uses it to serve requests.
- `server.Server` changes its fields from `basedirs.MultiReader` and `*db.Tree`
  to `db.Provider`.
- `server.LoadDBs` becomes `server.SetProvider(provider db.Provider)` which
  takes an already-opened provider.
- On `SetProvider`, the server registers its cache rebuild callback via
  `provider.OnUpdate(s.rebuildCaches)`, rebuilds caches immediately, and sets
  `dataTimeStamp` from `provider.BaseDirs().MountTimestamps()`.
- The server no longer has `EnableDBReloading`; reloading is handled internally
  by the provider.

`cmd/server` becomes simpler:
- Constructs `bolt.Config` with `BasePath`, `PollInterval`, etc.
- Calls `bolt.OpenProvider(cfg)` to get a `db.Provider`.
- Calls `server.SetProvider(provider)` once.
- The provider handles its own reloading and notifies the server via the
  callback.

Maintenance functions (used by `cmd/dbinfo` and `cmd/clean`) move to `bolt`
package:

```go
package bolt

// DGUTAInfo returns summary info about dguta databases at the given paths.
func DGUTAInfo(paths []string) (*db.DBInfo, error)

// BaseDirsInfo returns summary info about a basedirs database.
func BaseDirsInfo(dbPath string) (*basedirs.DBInfo, error)

// CleanInvalidDBHistory removes irrelevant paths from the history bucket.
func CleanInvalidDBHistory(dbPath, prefix string) error

// FindInvalidHistoryKeys returns keys that would be removed by CleanInvalidDBHistory.
func FindInvalidHistoryKeys(dbPath, prefix string) ([][]byte, error)

// MergeDBs merges two basedirs databases into a new output file.
func MergeDBs(pathA, pathB, outputPath string) error
```

Directory discovery helpers currently in `server/db.go` move into
`bolt/internal` and remain **non-public**:
- Implement them only in `bolt/internal` helpers used by tests; do not expose
  them from the public `bolt` package and do not reimplement discovery in
  production code.
- No exported Bolt API must expose database path layout or directory naming
  conventions; those are backend implementation details. CLI commands should
  just pass the bolt DB paths they already take to the bolt constructors. Any
  test-only discovery logic stays in `bolt/internal`.

## ClickHouse backend notes (future work)

ClickHouse must implement the same read/write interfaces.

### Required capabilities

- Ingest `db.RecordDGUTA` at scale.
- Support the same filter semantics:
  - GID list, UID list, file types bitmask, age buckets.
 - Provide `MountTimestamps()` keyed by mount directory paths.

### Suggested schema (guidance, not final)

- `dguta_records` table keyed by `(mount_path, dir, gid, uid, filetype, age)`
  with aggregated sums:
  - `count`, `size`, `min_atime`, `max_mtime`, and any other fields needed to
    reproduce `db.DirSummary`.
- `children` table keyed by `(mount_path, dir)` with `child` entries.
- `mount_updates` table keyed by `(mount_path)` with `updated_at`.
- `basedirs_usage` tables for group/user usage and subdirs, plus
  `basedirs_history` for time series.

### Timestamp semantics for ClickHouse

In bolt mode, timestamps are persisted by the writer as `(mountPath, updatedAt)`
and returned via `MountTimestamps()`. In ClickHouse mode, the backend must
maintain equivalent timestamps in `mount_updates` keyed by `mount_path`. When
summarise ingests data for a mount, it must update that mount’s `updated_at`.

## Migration steps (implementation order)

1. **Create `db.Database` and `db.DGUTAWriter` interfaces** in the `db` package:
   - Define the low-level store interface (`GetDGUTA`, `GetChildren`, `Close`).
   - Refactor `db.Tree` to use `Database` instead of directly opening bolt
     databases.
   - Remove all bbolt imports from `db` package.

2. **Create `basedirs.Reader` and `basedirs.Writer` interfaces** in the
   `basedirs` package:
   - Define the interfaces.
   - Refactor `basedirs` to use these interfaces.
   - Remove all bbolt imports from `basedirs` package.

3. **Create the new root `bolt` package**:
   - Move all bbolt opening/closing, bucket logic, encoding/decoding from `db`
     and `basedirs`.
   - Implement `db.Database` interface.
   - Implement `basedirs.Reader` (current `MultiReader` logic).
   - Implement `db.Provider` bundle with internal reload loop.
   - Implement `db.DGUTAWriter` (current `db.DB.Add` + batching logic).
   - Implement `basedirs.Writer` (current `basedirs.BaseDirs.Output` logic).
   - Move maintenance functions: `DGUTAInfo`, `BaseDirsInfo`,
     `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`.
   - Implement internal reload goroutine that watches `BasePath` and calls
     `OnUpdate` callbacks.

4. **Refactor `cmd/summarise`**:
   - Replace `db.NewDB()` with `bolt.NewDGUTAWriter()`.
   - Replace `basedirs.NewCreator()` with `bolt.NewBaseDirsWriter()`.
   - Require a `--mount` flag; pass it to both writers via `SetMountPath` and
     set `SetUpdatedAt` from the stats input `mtime`.
   - Pass `--basedirsHistoryDB` as `previousDBPath` to the bolt writer.

5. **Refactor `server` package**:
   - Change `Server` fields from `basedirs.MultiReader` and `*db.Tree` to
     `db.Provider`.
   - Replace `LoadDBs(basePaths, ...)` with `SetProvider(provider
     db.Provider)`.
   - In `SetProvider`, register cache rebuild callback via
     `provider.OnUpdate(s.rebuildCaches)`.
   - Remove `EnableDBReloading` entirely; reloading is internal to the provider.
   - Change `dbUpdateTimestamps` handler to call
     `provider.BaseDirs().MountTimestamps()`.
   - Remove bbolt-related imports.

6. **Refactor `cmd/server`**:
   - Build `bolt.Config` with `BasePath`, `PollInterval`, `OwnersCSVPath`, etc.
   - Call `bolt.OpenProvider(cfg)` to get a `db.Provider`.
   - Call `server.SetProvider(provider)` once.
   - No reload loop needed; the backend handles it internally.

7. **Refactor `cmd/dbinfo` and `cmd/clean`**:
   - Keep commands backend-neutral by delegating all layout discovery and Bolt
     details to backend helpers.
   - For DB info: call Bolt helpers that *internally* discover the latest
     datasets for a base path (same logic the provider uses) and return
     `db.DBInfo` / `basedirs.DBInfo`. The CLI should not replicate directory
     scanning logic.
   - For cleaning basedirs history: obtain a backend-specific
     `basedirs.HistoryMaintainer` (Bolt: wrap a single `basedirs.db`; future
     ClickHouse: wrap a connection) and call `CleanHistoryForMount` or
     `FindInvalidHistory`. The CLI supplies the prefix and, for Bolt, the
     database path; the backend owns how that path is opened.

8. **Final verification**:
   - Ensure no package outside `bolt` imports `go.etcd.io/bbolt`.
   - Run all existing tests to verify behaviour is unchanged.

## Testing checklist

- Unit tests in `bolt` package for:
  - `Database` implementation: `GetDGUTA`, `GetChildren`.
  - `basedirs.Reader`: `GroupUsage`, `UserUsage`, `GroupSubDirs`, `UserSubDirs`,
    `History`.
  - `MountTimestampsProvider`: timestamp derivation from directory names and
    mtimes.
  - `DGUTAWriter`: batched writes, children storage.
  - `basedirs.Writer`: usage, subdirs, history with proper update semantics.
  - Internal reload loop: verify `OnUpdate` callback is invoked when new
    databases appear.
  - Maintenance functions: `DGUTAInfo`, `BaseDirsInfo`, `CleanInvalidDBHistory`.
- A repository-wide check (CI or a small test) that fails if any non-`bolt`
  package imports `go.etcd.io/bbolt`.
- Server tests should pass unchanged in behaviour (JSON shapes, auth
  restrictions, caches, and reload semantics).
