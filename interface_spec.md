# wrstat-ui storage abstraction spec (Bolt → ClickHouse)

## Goals (hard constraints)

1. Replace direct use of `go.etcd.io/bbolt` throughout the codebase with a
   storage-neutral design, enabling a future ClickHouse backend.
2. Create a new root package `bolt` (and its subpackages, e.g. `bolt/internal`)
  that are the **only** production packages allowed to import
  `go.etcd.io/bbolt`.
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
   - `dbsUpdated` behaviour.
     - The only identifier exposed to callers is the **mount key** (the
       dataset directory suffix after `<version>_`, with `/` encoded as `／`).
     - The timestamps map keys are mount keys only (no version prefixes).

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
      - Semantics: each string maps to a single filetype bit.
      - The backend ORs all requested bits into a single mask; a record passes
        if `(recordFT & requestedMask) != 0`.
      - `temp` is a bit-flag that may be present *in addition to* another type
        (e.g. a temporary BAM may be both `temp` and `bam`). Filtering by
        `bam` therefore includes both non-temp BAM and temp BAM.
    - `age` (string; parsed via `db.AgeStringToDirGUTAge`, `0` means all)

- Endpoint: `GET /rest/v1/auth/where`
  - Query params:
    - `dir` (default server default dir)
    - `splits` (default `2`)
    - plus the same filter params as above (`groups`, `users`, `types`, `age`)
  - Behaviour: returns directory summaries sorted by size.

Tree JSON additions (must remain):

- The tree endpoint response objects (both the root object and child entries)
  include two additional fields:
  - `common_atime`: numeric age-bucket index representing the most common
    access-time age among files nested under the directory.
  - `common_mtime`: numeric age-bucket index representing the most common
    modification-time age among files nested under the directory.
- These are `summary.AgeRange` values with this mapping (index → bucket):
  - `0`: >= 7 years
  - `1`: 5–7 years
  - `2`: 3–5 years
  - `3`: 2–3 years
  - `4`: 1–2 years
  - `5`: 6–12 months
  - `6`: 2–6 months
  - `7`: 1–2 months
  - `8`: < 1 month
- Tie-breaking rule (normative): if multiple buckets share the maximum count,
  the *newest* bucket (highest index) is chosen.

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
- Response shape: JSON object mapping `mountKey` → Unix seconds.
  - `mountKey` is the dataset directory key suffix from `<version>_<mountKey>`.
  - `mountKey` is typically derived from the true mount path by replacing every
    `/` with `／` (U+FF0F FULLWIDTH SOLIDUS).
  - Keys MUST be treated as opaque by `server` and the frontend.

Notes:

- Today the server derives these timestamps from the dataset directory `mtime`
  and uses the raw `<mountKey>` as the JSON key.
- A future ClickHouse provider should preserve this external shape (mountKey
  keys) even if it stores/queries using decoded mount paths internally.

Bolt mount-key and mount-path derivation (for multi-directory Bolt layouts):
- Dataset directories are named `<version>_<mountKey>`.
- `<mountKey>` is the mount path with `/` replaced by `／` (U+FF0F FULLWIDTH
  SOLIDUS) to avoid nested directories.
- The Bolt backend MUST be able to:
  - return `<mountKey>` exactly as found in the dataset directory name for
    `/rest/v1/auth/dbsUpdated` compatibility.
  - derive the decoded `mountPath` by splitting the directory name at the first
    `_`, taking the remainder as `<mountKey>`, then replacing all `／` with `/`.
  - normalise the derived `mountPath` to end with `/`.
- `updatedAt` is the snapshot time of the dataset, derived from the input
  `stats.gz` file `mtime`.
- Bolt datasets written before this change may not have a persisted
  `updatedAt`; for those, the Bolt backend MUST fall back to dataset directory
  `mtime` to preserve backwards compatibility.

## Domain model summary (what storage must support)

### Current Bolt behaviour (reference)

- DirGUTA data is split across two Bolt files per dataset: `dguta.db`
  (bucket `gut`) and `dguta.db.children` (bucket `children`). Keys are the
  directory path with a trailing `0xff` byte; values are binary-encoded GUTAs
  (via `byteio`). Children are codec-encoded `[]string` keyed by the parent dir.
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
  and sets `/rest/v1/auth/dbsUpdated` from the `mtime` of each dataset dir.
  Reloading is incremental via `Tree.OpenFrom` / `MultiReader.OpenFrom` and
  caches are rebuilt across all ages.

### DirGUTA (“tree/where”)

- Write path (summarise): repeated ingestion of `db.RecordDGUTA` values.
- Read path (server/UI/CLI):
  - `DirInfo(dir, filter)` returning summary + immediate children summaries.
  - `DirHasChildren(dir, filter)`.
  - `Where(dir, filter, splits)`.
- Filter semantics are defined in `db.Filter` / `db.GUTAs.Summary()`.

Hardlink and filetype semantics (current behaviour, must be preserved):

- The summarise pipeline is hardlink-aware.
  - Each stats entry includes `inode` and `nlink` (link count).
  - Hardlinked files (same inode, with `nlink > 1`) must be collapsed so they
    do not double-count `Count` and `Size` within a directory subtree.
- When collapsing hardlinks, the logical “file” contributes:
  - `Count`: incremented once per inode (not per path).
  - `Size`: counted once per inode, using the maximum observed size for that
    inode within the collapsed set.
  - `Atime`: the minimum observed atime for that inode.
  - `Mtime`: the maximum observed mtime for that inode.
  - `FT`: a bitmask union of all observed filetype bits for that inode (so a
    hardlink that appears under different names/suffixes can contribute
    multiple type bits).
- Temporary classification is a bit flag:
  - the `temp` bit may be OR’d with any other type bit, and callers may filter
    by `temp` alone or alongside other types.

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
`db.DB` should be replaced by a `db.Database` interface.

```go
package db

// Database is the storage interface that db.Tree uses internally.
// Implementations MUST NOT expose Bolt concepts (tx/bucket/cursor) or []byte
// values. This is implemented by the bolt package.
type Database interface {
  // DirInfo returns the directory summary for dir, after applying filter.
  // It MUST preserve current multi-source semantics:
  // - return ErrDirNotFound only if dir is missing from all sources
  // - merge GUTA state across sources before applying the filter
  // - set DirSummary.Modtime to the latest dataset updatedAt across sources
  DirInfo(dir string, filter *Filter) (*DirSummary, error)

  // Children returns the immediate child directory paths for dir.
  // It MUST de-duplicate and sort children across all sources.
  // It MUST return nil/empty if no children exist (leaf or missing dir).
  Children(dir string) ([]string, error)

  // Info returns summary information about the database (e.g. counts).
  // Used by cmd/dbinfo.
  Info() (*Info, error)

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
`Database.Info()` must aggregate counts across all mounts the backend has
opened, matching the current `DB.Info()` behaviour.

### 2) Tree ingest interface (write-side)

`summary/dirguta` already depends on a minimal interface:

```go
// package summary/dirguta (unchanged)
type DB interface { Add(dguta db.RecordDGUTA) error }
```

This interface remains unchanged. `cmd/summarise` creates a writer and passes it
to `dirguta.NewDirGroupUserTypeAge()`.

For the full writer lifecycle, define in `db`:

```go
package db

// DGUTAWriter is the full interface for writing DGUTA data.
// cmd/summarise uses this to configure the writer before passing it to
// summary/dirguta (which only uses the Add method).
type DGUTAWriter interface {
  // Add adds a DGUTA record to the database.
  Add(dguta RecordDGUTA) error

  // SetBatchSize controls flush batching. cmd/summarise sets this to 10_000
  // today. Implementations must keep streaming semantics (no requirement to
  // hold the whole dataset in memory).
  SetBatchSize(batchSize int)

  // SetMountPath sets the mount directory path for this dataset.
  // MUST be called before Add(). The path must be absolute and end with '/'.
  // Implementations persist this so MountTimestamps() can return it.
  SetMountPath(mountPath string)

  // SetUpdatedAt sets the dataset snapshot time (typically from stats.gz
  // mtime).
  // MUST be called before Add().
  // Implementations persist this so MountTimestamps() can return it, and
  // DirSummary.Modtime can be populated correctly.
  SetUpdatedAt(updatedAt time.Time)

  Close() error
}
```

The Bolt constructor:

```go
package bolt

// NewDGUTAWriter creates a db.DGUTAWriter backed by Bolt.
// outputDir is the directory where dguta.db and dguta.db.children will be
// created. The directory must already exist.
func NewDGUTAWriter(outputDir string) (db.DGUTAWriter, error)
```

The caller must call `SetMountPath()` and `SetUpdatedAt()` before calling
`Add()`. These are defined in the interface so that any backend implementation
(Bolt, ClickHouse, etc.) is guided to implement them.

Normative requirement:
- Implementations must support streaming ingestion: callers never need to hold
  the entire dataset in memory; backends are responsible for batching and
  flushing as needed.
- Every ingest MUST associate records with exactly one `(mountPath, updatedAt)`
  pair so that:
  - `MountTimestamps()` can return the last `updatedAt` per mount.
  - `DirSummary.Modtime` can be populated as the latest `updatedAt` across all
    mounts contributing to a query (matching current behaviour).

### 3) Basedirs ingest interface (write-side)

The basedirs write path is used by `summary/basedirs.NewBaseDirs()` which
receives directory information from the summariser and calls `basedirs.BaseDirs`
to calculate and store usage data.

The current `basedirs.BaseDirs` struct:
- Takes a db path, quotas, mountpoints, and modTime
- Receives `IDAgeDirs` (computed by summary/basedirs) via `Output(users,
  groups)`
- Internally calculates `Usage`, `SubDir`, and `History` from the IDAgeDirs
- Stores everything in a Bolt transaction

The refactored design keeps the calculation logic in
`basedirs.BaseDirs.Output()` but delegates all persistence to a storage-neutral
`Store` interface.

Hard requirement (streaming + minimal copies): `BaseDirs.Output()` MUST NOT
build a second full in-memory representation of the data (no “OutputData”
containing full slices/maps of everything). Instead, it must call the Store as
it iterates its existing computed structures.

Define in `basedirs`:

```go
package basedirs

// SubDirKey identifies a subdir entry.
type SubDirKey struct {
  ID      uint32 // GID or UID
  BaseDir string
  Age     db.DirGUTAge
}

// HistoryKey identifies a history series.
// MountPath MUST be an absolute mount directory path ending with '/'.
type HistoryKey struct {
  GID       uint32
  MountPath string
}

// Store is the storage backend interface for basedirs persistence.
//
// This interface is domain-oriented (no buckets/tx/cursors/[]byte), and is
// designed so BaseDirs.Output() can stream its writes without materialising
// extra large slices.
type Store interface {
  // SetMountPath sets the mount directory path for this dataset.
  // MUST be called before any write methods.
  SetMountPath(mountPath string)

  // SetUpdatedAt sets the dataset snapshot time (stats.gz mtime).
  // MUST be called before any write methods.
  SetUpdatedAt(updatedAt time.Time)

  // Reset prepares the destination for a new summarise run for this mount.
  //
  // It MUST remove/overwrite all non-history data for this mount (group/user
  // usage and subdir breakdown) so the run produces a full replacement of
  // those views.
  //
  // It MUST NOT delete history; history continuity is append-only and is
  // handled via AppendGroupHistory (and, for Bolt, optional seeding from a
  // previous DB in the constructor).
  //
  // Notes:
  // - For Bolt, which writes to a fresh file per run, Reset will typically be
  //   a no-op beyond internal initialisation.
  // - For ClickHouse (single logical database), Reset corresponds to
  //   truncating/deleting rows for this mount in the relevant tables.
  Reset() error

  // Put* methods persist one logical record.
  PutGroupUsage(u *Usage) error
  PutUserUsage(u *Usage) error
  PutGroupSubDirs(key SubDirKey, subdirs []*SubDir) error
  PutUserSubDirs(key SubDirKey, subdirs []*SubDir) error

  // AppendGroupHistory applies the append-only rule for histories.
  // It MUST append only if point.Date is strictly after the last stored
  // history point for (key.GID, key.MountPath).
  AppendGroupHistory(key HistoryKey, point History) error

  // Finalise is called once after all Put*/Append calls.
  // It MUST update DateNoSpace/DateNoFiles for each stored group usage entry
  // based on the stored history (matching current storeDateQuotasFill).
  //
  // Transaction semantics: the Store MAY implement Reset() as "begin" and
  // Finalise() as "commit" (i.e. keep one internal transaction open across
  // Put*/Append calls) to preserve atomic updates. This is the mechanism by
  // which implementations get a single-transaction write without requiring the
  // caller to pass a monolithic Persist() payload.
  Finalise() error

  Close() error
}
```

`basedirs.BaseDirs` changes from holding a `dbPath string` to holding a `Store`:

```go
package basedirs

// NewCreator returns a BaseDirs that can create basedirs data.
// The store parameter is the backend that will persist the data.
func NewCreator(store Store, quotas *Quotas) (*BaseDirs, error)
```

The `BaseDirs.Output()` method changes from directly using Bolt to:
1. Call `store.Reset()`
2. Iterate the already-computed usage/subdir/history structures (same logic as
  today) and call the appropriate `store.Put*` / `store.AppendGroupHistory`
  methods
3. Call `store.Finalise()`

The Bolt `Store` implementation:
- Opens/creates the basedirs.db file in its constructor
- If `previousDBPath` is provided, seeds history from that file first
- `Reset()` clears usage/subdir data for the destination
- `Put*` and `AppendGroupHistory` store data in Bolt transactions
- `Finalise()` computes and persists DateNoSpace/DateNoFiles
- Handles codec encoding internally

```go
package bolt

// NewBaseDirsStore creates a basedirs.Store backed by Bolt.
// dbPath is the path to the new basedirs.db file.
// previousDBPath, if non-empty, is the path to a previous basedirs.db from
// which history will be seeded (used by cmd/summarise --basedirsHistoryDB).
func NewBaseDirsStore(dbPath, previousDBPath string) (basedirs.Store, error)
```

History update rule (must match current behaviour):
- For each `(gid, mountPath)` history series, append the new point only if
  `point.Date` is strictly after the last stored point’s date; otherwise leave
  history unchanged.

Terminology note:
- In the summarise pipeline, the new point’s `History.Date` is derived from the
  dataset snapshot time (stats input `mtime`). This is also the value passed to
  `Store.SetUpdatedAt(updatedAt)` for the run, so `point.Date` and `updatedAt`
  refer to the same timestamp.

Backend-specific history continuity:
- **ClickHouse:** history is stored in the single database and updated in-place;
  the implementation reads the last stored point for `(gid, mountPath)` and
  appends if the new date is strictly newer.
- **Bolt:** the `bolt.NewBaseDirsStore()` constructor accepts a `previousDBPath`
  parameter. When non-empty, the implementation seeds the new database's history
  bucket by copying all entries from the previous database before any new data
  is written. The `Output()` method then applies the append-only rule.

The Bolt implementation owns all history seeding logic (the current
`CopyHistoryFrom()` functionality); domain code does not manage this.

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

  // Returns mountKey -> last updated time.
  // The mountKey MUST match the dataset directory naming convention used by
  // the server for /rest/v1/auth/dbsUpdated (suffix after '<version>_').
  // Implementations may store/compute this from a decoded mountPath, but the
  // returned map keys must be mount keys for compatibility.
  MountTimestamps() (map[string]time.Time, error)

  // Info returns summary information about the database (e.g. counts).
  // Used by cmd/dbinfo.
  Info() (*Info, error)

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
- Info: sum the counts across all mounts that are open.

`MountTimestamps` must merge entries across all physical sources, returning
the latest `updatedAt` per mount key.

Group/user name caching: The `basedirs` types `Usage` includes `Name` which is
resolved from UID/GID. The current implementation uses `GroupCache` and
`UserCache` in `basedirs`. The Bolt backend must continue to populate
`Usage.Name` and `Usage.Owner` fields; name caches should still be honoured if
set via `SetCachedGroup` / `SetCachedUser`.

### 5) Backend bundle interface (required)

`server` must be wired to a single backend bundle, provided by `cmd/server`.
Define this interface in the dedicated `provider` package as `provider.Provider`.

Note: This interface cannot live in `db` because it references
`basedirs.Reader`, and `basedirs` already imports `db` (for `DirGUTAge` etc.),
which would create an import cycle. Placing it in `provider` keeps the domain
packages (`db`, `basedirs`) decoupled from `server` while avoiding import
cycles.

```go
package provider

type Provider interface {
  // Tree returns a query object used by server endpoints (tree + where).
  // The returned value must already be ready for use.
  Tree() *db.Tree
  BaseDirs() basedirs.Reader

  // OnUpdate registers a callback that will be invoked whenever the
  // underlying data changes (e.g., new databases discovered, ClickHouse
  // tables updated). The server rebuilds caches using the same Provider
  // instance on which the callback was registered.
  //
  // Implementations must:
  // - Call the callback on a separate goroutine (non-blocking).
  // - Guarantee the callback is not called concurrently with itself.
  // - Provide a fully-usable Provider when the callback is invoked. The
  //   provider has already swapped to new data before cb runs; old
  //   connections stay valid until cb returns.
  //
  // If cb is nil, any previously registered callback is removed.
  OnUpdate(cb func())

  // OnError registers a callback that will be invoked whenever the provider
  // encounters an internal asynchronous error (eg. reload cleanup failures).
  //
  // Implementations must:
  // - Call the callback on a separate goroutine (non-blocking).
  // - Guarantee the callback is not called concurrently with itself.
  //
  // If cb is nil, any previously registered callback is removed.
  OnError(cb func(error))

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

**Write-side constructors**: `cmd/summarise` uses `bolt.NewDGUTAWriter()` and
`bolt.NewBaseDirsStore()` to create the writers.
For basedirs, `cmd/summarise` MUST call `store.SetMountPath()` and
`store.SetUpdatedAt()` on the returned `basedirs.Store` before the summariser
invokes `BaseDirs.Output()`.

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

Timestamp source of truth:
- `DGUTAWriter` receives `mountPath` and `updatedAt` via `SetMountPath()` and
  `SetUpdatedAt()` (defined in the interface). Implementations persist these
  so readers can return them via `MountTimestamps()`.
- `basedirs.Store` receives `mountPath` and `updatedAt` via
  `Store.SetMountPath()` and `Store.SetUpdatedAt()` (both required by the
  interface). The store persists these for `MountTimestamps()`.
- Bolt storage mechanism: Each database file stores its `(mountPath, updatedAt)`
  in a metadata bucket (e.g., `_meta` with keys `mountPath` and `updatedAt`).
- Legacy fallback (only when explicit timestamps are absent in older DBs):
  - Derive `mountPath` from the dataset directory name using the existing
    on-disk convention from `server/db.go`:
    - split the dataset directory basename on the first underscore (`_`)
    - treat the suffix (the part after the underscore) as the mount path string
  - Decode the suffix by replacing `／` with `/`, then normalise to ensure it
    ends with '/'.
  - Use the dataset directory `mtime` as `updatedAt`.
  - This derivation happens inside the Bolt backend; the server never derives
    mount paths or timestamps from filesystem paths.

Minimal public API (required shape; keep as small as possible):

```go
package bolt

// OpenProvider constructs a backend bundle that implements provider.Provider.
// When cfg.PollInterval > 0, the backend starts an internal goroutine that
// watches cfg.BasePath for new databases and triggers OnUpdate callbacks.
func OpenProvider(cfg Config) (provider.Provider, error)

type Config struct {
    // BasePath is the directory scanned for database subdirectories.
  // Bolt's provider implementation scans this directory for dataset
  // subdirectories using the existing on-disk naming convention
  // "<version>_<mountKey>" (mount path with '/' replaced by '／'), and expects
  // each dataset directory to contain files named DGUTADBName and
  // BaseDirDBName.
    BasePath string

    // DGUTADBName and BaseDirDBName are the filenames expected inside each
    // database directory (e.g., "dguta.db", "basedirs.db").
    DGUTADBName   string
    BaseDirDBName string

    // OwnersCSVPath is required for basedirs name resolution.
    OwnersCSVPath string

    // MountPoints overrides mount auto-discovery for basedirs history
    // resolution.
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

// NewDGUTAWriter creates a db.DGUTAWriter backed by Bolt.
// outputDir is the directory where dguta.db and dguta.db.children will be
// created. The directory must already exist.
// Caller must call SetMountPath() and SetUpdatedAt() before Add().
func NewDGUTAWriter(outputDir string) (db.DGUTAWriter, error)

// NewBaseDirsStore creates a basedirs.Store backed by Bolt.
// dbPath is the path to the new basedirs.db file.
// previousDBPath, if non-empty, is the path to a previous basedirs.db from
// which history will be seeded.
func NewBaseDirsStore(dbPath, previousDBPath string) (basedirs.Store, error)
```

Maintenance functions (used by `cmd/clean`):

```go
package bolt

// NewHistoryMaintainer returns a basedirs.HistoryMaintainer backed by the
// Bolt database at dbPath.
func NewHistoryMaintainer(dbPath string) (basedirs.HistoryMaintainer, error)
```

### Domain packages after refactor

- `db/` becomes a pure domain package plus query algorithms.
  - Keeps: `Filter`, `DirSummary`, `DirInfo`, `DCSs`, `DGUTA`, `GUTAs`,
    `RecordDGUTA`, age/filetype enums and parsing, `Info` struct,
    `Database` interface (new), `DGUTAWriter` interface (new).
  - Keeps: `Tree` type and its methods (`DirInfo`, `DirHasChildren`, `Where`,
    `FileLocations`, `Info`, `Close`).
  - Keeps: `RecordDGUTA.EncodeToBytes()` and `DecodeDGUTAbytes()` (these use
    simple binary encoding via `byteio`, not bolt-specific; the Bolt package
    calls them when storing/reading).
  - Removes: all bbolt imports, `DB` type entirely (its functionality moves to
    bolt), `dbSet` type.
  - New: `Tree` takes `Database` interface in its constructor. The Bolt
    package provides the `Database` implementation.
  - `db` must not import `go.etcd.io/bbolt` or `github.com/ugorji/go/codec`.

- `basedirs/` becomes domain types + pure interfaces.
  - Keeps: `Usage`, `SubDir`, `History`, `IDAgeDirs`, `AgeDirs`,
    `SummaryWithChildren`, `Quotas`, `Config`, `DBInfo` struct, `DateQuotaFull`
    function, `GroupCache`, `UserCache`, `Error` type, mountpoint utilities,
    `BaseDirs` struct (domain logic for calculating usage from IDAgeDirs),
    `SubDirKey`, `HistoryKey` (new types for Store interface).
  - Keeps: `Store` interface (new), `Reader` interface (new),
    `HistoryMaintainer` interface (new).
  - Removes: all bbolt imports, `BaseDirReader` type, `MultiReader` type,
    `OpenDBRO`, `CleanInvalidDBHistory`, `FindInvalidHistoryKeys`, `MergeDBs`,
    `Info`, `CopyHistoryFrom`, all encoding/decoding.
  - Removes: `github.com/ugorji/go/codec` import (codec is bolt-specific
    serialization).
  - The Bolt package implements the `Store` and `Reader` interfaces using the
    domain types.

- `server/` must not:
  - open databases from filesystem paths,
  - scan filesystem for “latest version” dirs,
  - stat dirs for timestamps,
  - manage reload loops or polling,
  - import `go.etcd.io/bbolt`.

Instead:
- `server` stores only the `provider.Provider` and uses it to serve requests.
- `server.Server` changes its fields from `basedirs.MultiReader` and `*db.Tree`
  to `provider.Provider`.
- `server.LoadDBs` becomes `server.SetProvider(provider provider.Provider)`
  which takes an already-opened provider.
- On `SetProvider`, the server registers its cache rebuild callback via
  `provider.OnUpdate(s.rebuildCaches)`, rebuilds caches immediately, and sets
  `dataTimeStamp` from `provider.BaseDirs().MountTimestamps()`.
- The server no longer has `EnableDBReloading`; reloading is handled internally
  by the provider.

`cmd/server` becomes simpler:
- Constructs `bolt.Config` with `BasePath`, `PollInterval`, etc.
- Calls `bolt.OpenProvider(cfg)` to get a `provider.Provider`.
- Calls `server.SetProvider(provider)` once.
- The provider handles its own reloading and notifies the server via the
  callback.

`cmd/dbinfo` becomes simpler:
- Constructs `bolt.Config` with `BasePath` (from args).
- Calls `bolt.OpenProvider(cfg)` to get a `provider.Provider`.
- Calls `provider.Tree().Info()` and `provider.BaseDirs().Info()` to get stats.
- Prints the stats.

`cmd/clean` uses `bolt` to get the interface:
- Calls `bolt.NewHistoryMaintainer(path)` to obtain a
  `basedirs.HistoryMaintainer`.
- Calls `maintainer.CleanHistoryForMount(prefix)` or
  `maintainer.FindInvalidHistory(prefix)`.

Directory discovery helpers currently in `server/db.go` move into
`bolt/internal` and remain **non-public**. They must not be part of the
public `bolt` API, and no other package should depend on their exact
behaviour.

- No exported Bolt API must expose database path layout or directory naming
  conventions; those are backend implementation details.

  The only exception is `bolt.Config` used by `bolt.OpenProvider`: it describes
  how the provider locates datasets under a base path. Outside of provider
  configuration, CLI commands should just pass the bolt DB paths they already
  take to the bolt constructors. Any test-only discovery logic stays in
  `bolt/internal`.

## ClickHouse backend notes (future work)

ClickHouse must implement the same read/write interfaces.

### Required capabilities

- Ingest `db.RecordDGUTA` at scale.
- Support the same filter semantics:
  - GID list, UID list, file types bitmask, age buckets.
- Provide `MountTimestamps()` keyed by mount keys (for dbsUpdated
  compatibility).

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

In Bolt mode, mount timestamps are derived from dataset directory names (mount
paths) and persisted `updatedAt` (stats snapshot time). Legacy Bolt datasets
fall back to dataset directory `mtime` (see "Bolt mount-path derivation").

In ClickHouse mode, the backend must maintain equivalent timestamps in
`mount_updates` keyed by `mount_path`.

## Migration steps (implementation order)

1. **Create `db.Database` and `db.DGUTAWriter` interfaces** in the `db` package:
   - Define the store interface (`DirInfo`, `Children`, `Info`, `Close`).
   - Refactor `db.Tree` to use `Database` instead of directly opening bolt
     databases.
   - Remove all bbolt imports from `db` package.

2. **Create `basedirs.Reader` and `basedirs.Store` interfaces** in the
   `basedirs` package:
   - Define the `Reader` interface for query operations (including `Info`).
   - Define the `Store` interface for write operations.
   - Refactor `basedirs.BaseDirs` to accept a `Store` in its constructor.
   - Remove all bbolt imports from `basedirs` package.

3. **Create the new root `bolt` package**:
   - Move all bbolt opening/closing, bucket logic, encoding/decoding from `db`
     and `basedirs`.
   - Implement `db.Database` interface.
   - Implement `basedirs.Reader` (current `MultiReader` logic).
   - Implement `provider.Provider` bundle with internal reload loop.
   - Implement `db.DGUTAWriter` (current `db.DB.Add` + batching logic).
   - Implement `basedirs.Store` (current `basedirs.BaseDirs.Output` storage
     logic, including history seeding from a previous DB).
   - Implement `basedirs.HistoryMaintainer` (wrapping the clean logic).
   - Export `NewHistoryMaintainer(dbPath)`.
   - Implement internal reload goroutine that watches `BasePath` and calls
     `OnUpdate` callbacks.

4. **Refactor `cmd/summarise`**:
   - Derive `mountPath` from the output directory name:
     - The `--tree` (treeOutputDir) and `--basedirsDB` paths are inside a
       directory named `<version>_<mountKey>` (e.g.,
       `123_／lustre／scratch123／`).
     - Extract the directory name from the output path, split on the first `_`,
       take the suffix, replace all `／` with `/`, ensure it ends with `/`.
   - Replace `db.NewDB(treeOutputDir)` with:
     ```go
     writer, err := bolt.NewDGUTAWriter(treeOutputDir)
     writer.SetMountPath(mountPath)
     writer.SetUpdatedAt(modtime)
     writer.SetBatchSize(dbBatchSize)
     s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))
     // later: writer.Close()
     ```
   - Replace `basedirs.NewCreator(basedirsDB, quotas)` with:
     ```go
     store, err := bolt.NewBaseDirsStore(basedirsDB, basedirsHistoryDB)
     store.SetMountPath(mountPath)
     store.SetUpdatedAt(modtime)
     bd, err := basedirs.NewCreator(store, quotas)
     bd.SetMountPoints(mps)  // if mounts were provided
     bd.SetModTime(modtime)  // stats.gz mtime, used for History.Date
     ```
   - The `--basedirsHistoryDB` path is passed to `bolt.NewBaseDirsStore()` as
     the `previousDBPath` parameter; no separate `CopyHistoryFrom()` call is
     needed since the Bolt store handles history seeding internally.

5. **Refactor `server` package**:
   - Change `Server` fields from `basedirs.MultiReader` and `*db.Tree` to
     `provider.Provider`.
   - Replace `LoadDBs(basePaths, ...)` with `SetProvider(provider
     provider.Provider)`.
   - In `SetProvider`, register cache rebuild callback via
     `provider.OnUpdate(s.rebuildCaches)`.
   - Remove `EnableDBReloading` entirely; reloading is internal to the provider.
   - Change `dbUpdateTimestamps` handler to call
     `provider.BaseDirs().MountTimestamps()`.
   - Remove bbolt-related imports.

6. **Refactor `cmd/server`**:
   - Build `bolt.Config` with `BasePath`, `PollInterval`, `OwnersCSVPath`, etc.
  - Call `bolt.OpenProvider(cfg)` to get a `provider.Provider`.
   - Call `server.SetProvider(provider)` once.
   - No reload loop needed; the backend handles it internally.

7. **Refactor `cmd/dbinfo` and `cmd/clean`**:
   - `cmd/dbinfo`:
     - Construct `bolt.Config` with `BasePath` (from args).
  - Call `bolt.OpenProvider(cfg)` to get a `provider.Provider`.
     - Call `provider.Tree().Info()` and `provider.BaseDirs().Info()`.
   - `cmd/clean`:
     - Call `bolt.NewHistoryMaintainer(path)`.
     - Use `CleanHistoryForMount` or `FindInvalidHistory`.

8. **Final verification**:
   - Ensure no package outside `bolt` imports `go.etcd.io/bbolt`.
   - Run all existing tests to verify behaviour is unchanged.

## Testing checklist

- Unit tests in `bolt` package for:
  - `Database` implementation: `DirInfo`, `Children`, `Info`.
  - `basedirs.Reader`: `GroupUsage`, `UserUsage`, `GroupSubDirs`, `UserSubDirs`,
    `History`, `MountTimestamps`, `Info`.
  - Timestamp derivation: from explicit persisted values, and legacy fallback
    from directory names and mtimes.
  - `DGUTAWriter`: batched writes, children storage, metadata persistence
    (mountPath, updatedAt).
  - `basedirs.Store`: usage, subdirs, history with proper update semantics,
    history seeding from a previous database, DateNoSpace/DateNoFiles updates.
  - Internal reload loop: verify `OnUpdate` callback is invoked when new
    databases appear.
  - Maintenance functions: `HistoryMaintainer` implementation.
- A repository-wide check (CI or a small test) that fails if any non-`bolt`
  package imports `go.etcd.io/bbolt`.
- Server tests should pass unchanged in behaviour (JSON shapes, auth
  restrictions, caches, and reload semantics).
