# wrstat-ui storage abstraction spec (Bolt → ClickHouse)

## Goals (hard constraints)

1. Replace direct use of `go.etcd.io/bbolt` throughout the codebase with a storage-neutral design, enabling a future ClickHouse backend.
2. Create a new root package `bolt` that is the **only** production package allowed to import `go.etcd.io/bbolt`.
3. No bbolt types may leak across the boundary:
   - Do **not** re-export `*bolt.DB`, `*bolt.Tx`, `*bolt.Bucket`, cursors, or transaction semantics.
   - Interfaces must not expose buckets, cursors, `[]byte` values, or caller-built keys.
4. Public API surface of the new `bolt` package must be **minimal** and **fully tested**.
5. Only these may import `github.com/wtsi-hgi/wrstat-ui/bolt`:
   - `cmd/*`, `main.go`, and tests.
   - Domain packages like `db`, `basedirs`, and `server` must not import it.
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

### 3) Basedirs query interface (read-side)

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

### 4) Mount update timestamps (read-side metadata)

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

### 5) Backend bundle interface (required)

`server` must be wired to a single backend bundle, provided by `cmd/server`.

```go
package storage

type Backend interface {
  Tree() TreeQuerier
  BaseDirs() BaseDirsReader
  Mounts() MountTimestampsProvider
  Close() error
}
```

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
// It must support multiple Bolt sources (multi-mount) by taking multiple paths.
func OpenBackend(cfg Config) (storage.Backend, error)

type Config struct {
    // OwnersCSVPath is required for basedirs.
    OwnersCSVPath string

    // DirGUTASourcePaths are paths to dguta.dbs datasets (one per mount/run).
    DirGUTASourcePaths []string

    // BaseDirsDBPaths are paths to basedirs.db files (one per mount/run).
    BaseDirsDBPaths []string

    // MountPoints overrides mount auto-discovery for basedirs history resolution.
    // If empty, the backend must use auto-discovery.
    MountPoints []string
}
```

Write-side construction is used only by `cmd/summarise`:

```go
package bolt

func NewDGUTAWriter(outputDir string) (storage.DGUTAWriter, error)
func NewBaseDirsWriter(dbPath string, quotas *basedirs.Quotas) (*basedirs.BaseDirs, error)
```

### Domain packages after refactor


- `db/` becomes a pure domain package plus query algorithms.
  - `db.Tree` remains in `db` and is the implementation used by the Bolt backend.
  - All persistence/IO details are pushed behind an internal (non-bbolt) store interface (defined in `db`), which is implemented in the `bolt` package.
  - `db` must not import `go.etcd.io/bbolt`.

- `basedirs/` becomes domain types + encoding/decoding logic (if still used), and/or pure query interfaces.
  - Any code that directly uses `*bolt.DB` must move behind the `bolt` package boundary.

- `server/` must not:
  - open databases from filesystem paths,
  - scan filesystem for “latest version” dirs,
  - stat dirs for timestamps,
  - call `OpenFrom` / `CloseOnly` on bolt-backed concrete types.

Instead:
- `cmd/server` is responsible for discovery/reloading and for supplying `storage.Backend` to `server`.
- `server` stores only the `storage.Backend` and uses it to serve requests.

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

1. Introduce interfaces (in `storage`): `storage.TreeQuerier`, `storage.BaseDirsReader`, `storage.MountTimestampsProvider`, `storage.DGUTAWriter`, and `storage.Backend`.
2. Create the new root `bolt` package:
   - move all bbolt opening/closing, bucket logic, and bolt-specific helpers into it.
   - ensure no bbolt imports remain outside `bolt`.
3. Refactor `cmd/summarise`:
   - replace `db.NewDB` / `basedirs.NewCreator` usage with constructors from `bolt`.
   - ensure `summary/dirguta` and `summary/basedirs` depend only on the minimal interfaces.
4. Refactor `server` wiring:
   - change `Server.LoadDBs`/`EnableDBReloading` so `server` no longer accepts file paths.
   - move path discovery + reloading into `cmd/server` or `bolt` helpers.
   - change `/dbsUpdated` to call the injected `MountTimestampsProvider`.
5. Refactor bolt-only maintenance helpers (`basedirs/clean.go`, `basedirs/info.go`, any bucket-copy helpers):
  - move them into `bolt`.
6. Delete/replace any remaining bbolt imports outside `bolt`.

## Testing checklist

- Unit tests in `bolt` package for:
  - Opening/reading DGUTA tree (`DirInfo`, `DirHasChildren`, `Where`).
  - Opening/reading basedirs (`GroupUsage`, `UserUsage`, `SubDirs`, `History`).
  - Timestamp derivation for bolt mode.
- A repository-wide check (CI or a small test) that fails if any non-`bolt` package imports `go.etcd.io/bbolt`.
- Server tests should pass unchanged in behaviour (JSON shapes, auth restrictions, and caches).
