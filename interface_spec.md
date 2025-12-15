# wrstat-ui storage abstraction spec (Bolt → ClickHouse)

## Goals (hard constraints)

1. Replace direct use of `go.etcd.io/bbolt` throughout the codebase with a storage-neutral design, enabling a future ClickHouse backend.
2. Create a new root package `bolt` that is the **only** production package allowed to import `go.etcd.io/bbolt`.
3. No bbolt types may leak across the boundary:
   - Do **not** re-export `*bolt.DB`, `*bolt.Tx`, `*bolt.Bucket`, cursors, or transaction semantics.
   - Interfaces must not expose buckets, cursors, `[]byte` values, or caller-built keys.
4. Public API surface of the new `bolt` package must be **minimal** and **fully tested**.
5. Only these may import `github.com/wtsi-hgi/wrstat-ui/bolt`:
   - `cmd/*`, `main.go`, test files (`*_test.go`), and `internal/*` test helper packages.
   - Domain packages like `db`, `basedirs`, and `server` must not import it in production code.
6. Must preserve all current user-visible behaviour:
   - Web UI and `wrstat-ui where` filters and results.
   - Multi-mount behaviour today (multiple Bolt “sources”).
   - `dbsUpdated` behaviour, but **timestamps must come from the storage layer**.
     - The only identifier exposed to callers is the **mount directory path**.
     - The timestamps map keys are mount paths only (no version prefixes).

## Non-goals

- No behavioural changes to query semantics, sorting, JSON shapes, or filters.
- No UI changes.
- No optimisation work beyond what is necessary to preserve existing performance characteristics.

## Current user-visible API contract (must remain)

### Tree/Where

- Endpoint: `GET /rest/v1/auth/tree` (and unauth variant if auth disabled)
  - Query params:
    - `path` (default `/`)
    - `groups` (comma-separated group names)
    - `users` (comma-separated usernames)
    - `types` (comma-separated file type strings; parsed via `db.FileTypeStringToDirGUTAFileType`)
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
    - These are served from server caches; cache is built by calling the underlying `storage.BaseDirsReader` across all `db.DirGUTAges`.
  - `GET /rest/v1/auth/basedirs/subdirs/group`
  - `GET /rest/v1/auth/basedirs/subdirs/user`
    - Query params: `id`, `basedir`, optional `age` (default `0`)
  - `GET /rest/v1/auth/basedirs/history`
    - Query params: `id` (gid), `basedir` (a mountpoint or a path under it)

### Database update timestamps

- Endpoint: `GET /rest/v1/auth/dbsUpdated`
- Response shape: JSON object mapping `mountPath` → Unix seconds.
  - `mountPath` is the mount directory path (string) that the stats were created for, e.g. `/some/mount/point/`.
  - The server must treat these keys as opaque mount paths; it must not derive them from directory names.
- Today: server derives timestamps from filesystem `mtime` of per-mount database directories.
- Required change: timestamps must be provided by storage, so ClickHouse can return mount-path timestamps too.

## Domain model summary (what storage must support)

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
    - ClickHouse: history is stored in the single database and appended in-place.
    - Bolt: history is seeded from the previous run’s `basedirs.db` by passing the previous DB path to the Bolt writer constructor.
- Read path:
  - group/user usage for a given age
  - group/user subdirs for a given id+basedir+age
  - group history for a given gid+path (path resolved to mountpoint prefix)

## Storage-neutral interfaces

These interfaces must live in a new root package `storage`.

Rationale: `server` and `cmd/*` need stable, backend-agnostic types to wire dependencies without importing the Bolt implementation.

### 1) Tree query interface (read-side)

`server` must depend on this interface, not on Bolt paths.

```go
package storage

type TreeQuerier interface {
  DirInfo(dir string, filter *db.Filter) (*db.DirInfo, error)
  DirHasChildren(dir string, filter *db.Filter) bool

  // recurseCount controls how far to descend when computing “where”.
  // Existing behaviour uses internal/split.SplitFn.
  Where(dir string, filter *db.Filter, recurseCount split.SplitFn) (db.DCSs, error)

  Close() error
}
```

Notes:
- `filter == nil` must behave as the current code (no filtering).
- File type defaults (all non-dir types) and age defaults must remain identical.

Internal store interface for `db.Tree` (defined in `db` package, implemented by `bolt`):

```go
package db

// TreeStore is the low-level storage interface that db.Tree uses internally.
// This is not exported from the storage package; it lives in db and is implemented by bolt.
type TreeStore interface {
  // GetDGUTA retrieves the DGUTA record for a directory.
  // Returns nil, ErrDirNotFound if the directory is not in the database.
  GetDGUTA(dir string) (*DGUTA, error)

  // GetChildren returns the child directory paths for a directory.
  // Returns empty slice if no children exist.
  GetChildren(dir string) []string

  // Modtime returns the modification time of the data source.
  Modtime() time.Time

  Close() error
}
```

`db.Tree` constructor changes from `NewTree(paths ...string)` to:

```go
package db

// NewTree creates a Tree that queries the given stores.
// Multiple stores allow combining data from multiple mounts.
func NewTree(stores ...TreeStore) *Tree
```

The Bolt package provides the `TreeStore` implementation and the `storage.TreeQuerier` wraps a `*db.Tree`.

### 2) Tree ingest interface (write-side)

`summary/dirguta` already depends on a minimal interface:

```go
// package summary/dirguta
// type DB interface { Add(dguta db.RecordDGUTA) error }
```

To make backend selection explicit and testable, define the following in `storage`:

```go
package storage

type DGUTAWriter interface {
  // MountPath is the mount directory path the stats were produced for.
  // It must be an absolute path and must end with '/'.
  // Example: "/lustre/scratch123/".
  SetMountPath(mountPath string)

  // SetUpdatedAt sets the dataset creation time used for mount timestamps.
  // cmd/summarise currently uses the stats.gz file mtime for this purpose.
  SetUpdatedAt(t time.Time)

  Add(rec db.RecordDGUTA) error
  Close() error
}
```

Normative requirement:
- Every successful ingest must persist `(mountPath, updatedAt)` so it can be returned later via `MountTimestamps()`.

### 3) Basedirs ingest interface (write-side)

This is the storage-neutral contract used by `cmd/summarise` to build basedirs data and update history. The caller does not provide a “previous DB path” to this interface.

```go
package storage

type BaseDirsWriter interface {
  // SetMountPath sets the mount directory path the stats were produced for.
  // It must be an absolute path and must end with '/'.
  SetMountPath(mountPath string)

  // SetMountPoints can be used to manually set mountpoints for history key resolution.
  // If not called, the writer must use auto-discovery from the OS.
  SetMountPoints(mountpoints []string)

  // SetUpdatedAt sets the dataset creation time used for history timestamps.
  // cmd/summarise currently uses the stats.gz file mtime for this purpose.
  SetUpdatedAt(t time.Time)

  // Output writes the full basedirs snapshot (usage, subdirs) and updates history.
  // It must be safe to call exactly once per writer.
  Output(users, groups basedirs.IDAgeDirs) error

  Close() error
}
```

History update rule (must match current behaviour):
- For each `(gid, mountPath)` history series, append the new point only if `updatedAt` is strictly after the last stored point’s date; otherwise leave history unchanged.

Backend-specific history continuity:
- ClickHouse: read the last stored point for `(gid, mountPath)` and append/update in-place.
- Bolt: seed the new output DB’s history from a previous `basedirs.db` provided to the Bolt writer constructor (see Bolt package section), then apply the append rule above.

### 4) Basedirs query interface (read-side)

`server` uses these methods (directly or via caching):

```go
package storage

type BaseDirsReader interface {
  // Usage is requested per age; server will call this for all ages.
  GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)
  UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)

  GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)
  UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)

  // Returns history for a group for the mountpoint that contains `path`.
  History(gid uint32, path string) ([]basedirs.History, error)

  // Matches current behaviour: used to override mountpoint auto-discovery.
  SetMountPoints(mountpoints []string)

  Close() error
}
```

Aggregation behaviour across mounts (current `basedirs.MultiReader`) must be preserved by the storage implementation:
- Usage methods: concatenate results across all mounts.
- Subdirs/history: return the first successful result.

Group/user name caching: The `basedirs` types `Usage` includes `Name` which is resolved from UID/GID. The current implementation uses `GroupCache` and `UserCache` in `basedirs`. The Bolt backend must continue to populate `Usage.Name` and `Usage.Owner` fields.

### 5) Mount update timestamps (read-side metadata)

Define a small metadata interface so `server` does not infer timestamps:

```go
package storage

type MountTimestampsProvider interface {
  // Returns mount directory path -> last updated time.
  // Keys MUST be mount paths (absolute) and MUST end with '/'.
  MountTimestamps() (map[string]time.Time, error)
}
```

The HTTP handler continues to return Unix seconds, but converts from `time.Time`.

### 6) Backend bundle interface (required)

`server` must be wired to a single backend bundle, provided by `cmd/server`.

```go
package storage

type Backend interface {
  Tree() TreeQuerier
  BaseDirs() BaseDirsReader
  Mounts() MountTimestampsProvider

  // OnUpdate registers a callback that will be invoked whenever the
  // underlying data changes (e.g., new databases discovered, ClickHouse
  // tables updated). The callback receives the updated Backend so the
  // server can rebuild caches from the new data.
  //
  // Implementations must:
  // - Call the callback on a separate goroutine (non-blocking).
  // - Guarantee the callback is not called concurrently with itself.
  // - Provide a fully-usable Backend when the callback is invoked
  //   (queries must work; old data connections are still valid until
  //    the callback returns).
  //
  // If cb is nil, any previously registered callback is removed.
  OnUpdate(cb func())

  Close() error
}
```

### 7) Backend reloading (internal implementation detail)

Reloading is an **internal concern** of each backend implementation; neither `server` nor `cmd/server` should manage reload loops, filesystem scanning, or incremental open/close logic.

**Bolt implementation:**
- The `bolt.OpenBackend(cfg)` constructor starts an internal goroutine that periodically scans the configured base path for new/changed database directories.
- When changes are detected, the backend:
  1. Opens the new databases.
  2. Updates its internal `TreeQuerier`, `BaseDirsReader`, and `MountTimestampsProvider`.
  3. Invokes the registered `OnUpdate` callback (if any).
  4. Closes obsolete database connections after the callback returns.
- The scan interval is configured via `Config.PollInterval`.
- If `Config.PollInterval` is zero or negative, automatic reloading is disabled.

**ClickHouse implementation (future):**
- May use a similar polling approach or rely on database-side change notifications.
- Must invoke `OnUpdate` when underlying data changes so the server can rebuild caches.

**Server cache invalidation:**
- The server's `groupUsageCache` and `userUsageCache` are pre-serialized JSON/gzip payloads for the `/basedirs/usage/*` endpoints.
- On startup, `server` registers a callback via `backend.OnUpdate(s.rebuildCaches)`.
- When the backend detects new data and calls the callback, the server rebuilds these caches from the (now-updated) `BaseDirsReader`.
- The `uidToNameCache` and `gidToNameCache` are append-only lookups and do not require invalidation.

See `bolt.Config` in the "The new `bolt` package" section for configuration fields including `PollInterval` and `RemoveOldPaths`.

**Write-side constructors remain unchanged** (used only by `cmd/summarise`).

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
  - reading directory `mtime` as a fallback when older Bolt DBs do not contain explicit mount timestamps

Timestamp source of truth (Bolt backend):
- Preferred: the backend persists and reads explicit `(mountPath, updatedAt)` written by `storage.DGUTAWriter`.
- Legacy fallback (only when explicit timestamps are absent):
  - Derive `mountPath` from the dataset directory name using the existing on-disk convention from `server/db.go`:
    - split the dataset directory basename on the first underscore (`_`)
    - treat the suffix (the part after the underscore) as the mount path string
  - Normalise by ensuring it is absolute and ends with '/'.
  - Use the dataset directory `mtime` as `updatedAt`.
  - This derivation happens inside the Bolt backend; the server still must not derive mount paths or timestamps from filesystem paths.

Minimal public API (required shape; keep as small as possible):

```go
package bolt

// OpenBackend constructs a backend bundle that implements storage.Backend.
// If cfg.PollInterval > 0, the backend starts an internal goroutine that
// watches cfg.BasePath for new databases and triggers OnUpdate callbacks.
func OpenBackend(cfg Config) (storage.Backend, error)

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
    // If empty, the backend uses auto-discovery from the OS.
    MountPoints []string

    // PollInterval is how often to scan BasePath for new databases.
    // If zero or negative, automatic reloading is disabled.
    PollInterval time.Duration

    // RemoveOldPaths, if true, causes older database directories to be
    // deleted from BasePath after a successful reload.
    RemoveOldPaths bool
}
```

Write-side construction is used only by `cmd/summarise`:

```go
package bolt

func NewDGUTAWriter(outputDir string) (storage.DGUTAWriter, error)
// previousDBPath is the path to the previous run’s basedirs.db.
// It may be empty; if empty, the writer starts with no history.
func NewBaseDirsWriter(dbPath string, quotas *basedirs.Quotas, previousDBPath string) (storage.BaseDirsWriter, error)
```

### Domain packages after refactor

- `db/` becomes a pure domain package plus query algorithms.
  - Keeps: `Filter`, `DirSummary`, `DirInfo`, `DCSs`, `DGUTA`, `GUTAs`, `RecordDGUTA`, `TreeStore` interface, age/filetype enums and parsing, `DBInfo` struct.
  - Keeps: `Tree` type and its methods (`DirInfo`, `DirHasChildren`, `Where`, `FileLocations`, `Close`).
  - Removes: all bbolt imports, `DB` type entirely (its write functionality moves to bolt), `dbSet` type.
  - Removes: `EncodeToBytes`, `DecodeDGUTAbytes`, and all bolt-specific encoding/decoding (these move to bolt).
  - New: `Tree` takes `TreeStore` interfaces in its constructor. The Bolt package provides the `TreeStore` implementation.
  - `db` must not import `go.etcd.io/bbolt` or `github.com/ugorji/go/codec` (codec is bolt-specific serialization).

- `basedirs/` becomes domain types + pure interfaces.
  - Keeps: `Usage`, `SubDir`, `History`, `IDAgeDirs`, `AgeDirs`, `SummaryWithChildren`, `Quotas`, `Config`, `DBInfo` struct, `DateQuotaFull` function, `GroupCache`, `UserCache`, `Error` type, mountpoint utilities.
  - Removes: all bbolt imports, `BaseDirs` writer type, `BaseDirReader` type, `MultiReader` type, `OpenDBRO`, `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`, `Info`, all encoding/decoding.
  - Removes: `github.com/ugorji/go/codec` import (codec is bolt-specific serialization).
  - The Bolt package reimplements the reader/writer using the domain types.

- `server/` must not:
  - open databases from filesystem paths,
  - scan filesystem for “latest version” dirs,
  - stat dirs for timestamps,
  - manage reload loops or polling,
  - import `go.etcd.io/bbolt`.

Instead:
- `server` stores only the `storage.Backend` and uses it to serve requests.
- `server.Server` changes its fields from `basedirs.MultiReader` and `*db.Tree` to `storage.Backend`.
- `server.LoadDBs` becomes `server.SetBackend(backend storage.Backend)` which takes an already-opened backend.
- On `SetBackend`, the server registers its cache rebuild callback via `backend.OnUpdate(s.rebuildCaches)`.
- The server no longer has `EnableDBReloading`; reloading is handled internally by the backend.

`cmd/server` becomes simpler:
- Constructs `bolt.Config` with `BasePath`, `PollInterval`, etc.
- Calls `bolt.OpenBackend(cfg)` to get a `storage.Backend`.
- Calls `server.SetBackend(backend)` once.
- The backend handles its own reloading and notifies the server via the callback.

Maintenance functions (used by `cmd/dbinfo` and `cmd/clean`) move to `bolt` package:

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

Directory discovery helpers (`findDBDirs`, `isValidDBDir`, `joinDBPaths`) move from `server/db.go` to `bolt` but remain **internal** (unexported). They are used by the backend's internal reload loop and `OpenBackend`.

## ClickHouse backend notes (future work)

ClickHouse must implement the same read/write interfaces.

### Required capabilities

- Ingest `db.RecordDGUTA` at scale.
- Support the same filter semantics:
  - GID list, UID list, file types bitmask, age buckets.
 - Provide `MountTimestamps()` keyed by mount directory paths.

### Suggested schema (guidance, not final)

- `dguta_records` table keyed by `(mount_path, dir, gid, uid, filetype, age)` with aggregated sums:
  - `count`, `size`, `min_atime`, `max_mtime`, and any other fields needed to reproduce `db.DirSummary`.
- `children` table keyed by `(mount_path, dir)` with `child` entries.
- `mount_updates` table keyed by `(mount_path)` with `updated_at`.
- `basedirs_usage` tables for group/user usage and subdirs, plus `basedirs_history` for time series.

### Timestamp semantics for ClickHouse

In bolt mode, timestamps are persisted by the writer as `(mountPath, updatedAt)` and returned via `MountTimestamps()`.
In ClickHouse mode, the backend must maintain equivalent timestamps in `mount_updates` keyed by `mount_path`.
When summarise ingests data for a mount, it must update that mount’s `updated_at`.

## Migration steps (implementation order)

1. **Create `storage` package with interfaces**:
   - Define `TreeQuerier`, `BaseDirsReader`, `MountTimestampsProvider`, `DGUTAWriter`, `BaseDirsWriter`, and `Backend`.
   - This package has no implementation, only interface definitions and domain type re-exports if needed.

2. **Create `db.TreeStore` interface** in the `db` package:
   - Define the low-level store interface (`GetDGUTA`, `GetChildren`, `Modtime`, `Close`).
   - Refactor `db.Tree` to use `TreeStore` instead of directly opening bolt databases.
   - Remove all bbolt imports from `db` package.

3. **Create the new root `bolt` package**:
   - Move all bbolt opening/closing, bucket logic, encoding/decoding from `db` and `basedirs`.
   - Implement `db.TreeStore` interface.
   - Implement `storage.TreeQuerier` by wrapping `*db.Tree`.
   - Implement `storage.BaseDirsReader` (current `MultiReader` logic).
   - Implement `storage.MountTimestampsProvider`.
   - Implement `storage.Backend` bundle with internal reload loop.
   - Implement `storage.DGUTAWriter` (current `db.DB.Add` + batching logic).
   - Implement `storage.BaseDirsWriter` (current `basedirs.BaseDirs.Output` logic).
   - Move maintenance functions: `DGUTAInfo`, `BaseDirsInfo`, `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`.
   - Implement internal reload goroutine that watches `BasePath` and calls `OnUpdate` callbacks.

4. **Refactor `basedirs` package**:
   - Keep only domain types: `Usage`, `SubDir`, `History`, `IDAgeDirs`, `Quotas`, `Config`, `DBInfo`, `GroupCache`, `UserCache`, `DateQuotaFull`.
   - Remove: `BaseDirs`, `BaseDirReader`, `MultiReader`, `OpenDBRO`, all bbolt imports.
   - Remove: `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`, `Info`.

5. **Refactor `cmd/summarise`**:
   - Replace `db.NewDB()` with `bolt.NewDGUTAWriter()`.
   - Replace `basedirs.NewCreator()` with `bolt.NewBaseDirsWriter()`.
   - Pass `--basedirsHistoryDB` as `previousDBPath` to the bolt writer.
   - The `summary/dirguta` and `summary/basedirs` packages continue to use their existing interfaces (`DB interface { Add(...) }` and `output interface { Output(...) }`).

6. **Refactor `server` package**:
   - Change `Server` fields from `basedirs.MultiReader` and `*db.Tree` to `storage.Backend`.
   - Replace `LoadDBs(basePaths, ...)` with `SetBackend(backend storage.Backend)`.
   - In `SetBackend`, register cache rebuild callback via `backend.OnUpdate(s.rebuildCaches)`.
   - Remove `EnableDBReloading` entirely; reloading is internal to the backend.
   - Change `dbUpdateTimestamps` handler to call `backend.Mounts().MountTimestamps()`.
   - Remove bbolt-related imports.

7. **Refactor `cmd/server`**:
   - Build `bolt.Config` with `BasePath`, `PollInterval`, `OwnersCSVPath`, etc.
   - Call `bolt.OpenBackend(cfg)` to get a `storage.Backend`.
   - Call `server.SetBackend(backend)` once.
   - No reload loop needed; the backend handles it internally.

8. **Refactor `cmd/dbinfo` and `cmd/clean`**:
   - Use `bolt.DGUTAInfo`, `bolt.BaseDirsInfo` for dbinfo.
   - Use `bolt.CleanInvalidDBHistory`, `bolt.FindInvalidHistoryKeys` for clean.

9. **Final verification**:
   - Ensure no package outside `bolt` imports `go.etcd.io/bbolt`.
   - Run all existing tests to verify behaviour is unchanged.

## Testing checklist

- Unit tests in `bolt` package for:
  - `TreeStore` implementation: `GetDGUTA`, `GetChildren`, `Modtime`.
  - `TreeQuerier` via `db.Tree`: `DirInfo`, `DirHasChildren`, `Where`.
  - `BaseDirsReader`: `GroupUsage`, `UserUsage`, `GroupSubDirs`, `UserSubDirs`, `History`.
  - `MountTimestampsProvider`: timestamp derivation from directory names and mtimes.
  - `DGUTAWriter`: batched writes, children storage.
  - `BaseDirsWriter`: usage, subdirs, history with proper update semantics.
  - Internal reload loop: verify `OnUpdate` callback is invoked when new databases appear.
  - Maintenance functions: `DGUTAInfo`, `BaseDirsInfo`, `CleanInvalidDBHistory`.
- A repository-wide check (CI or a small test) that fails if any non-`bolt` package imports `go.etcd.io/bbolt`.
- Server tests should pass unchanged in behaviour (JSON shapes, auth restrictions, caches, and reload semantics).
