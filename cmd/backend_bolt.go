package cmd

import (
    "time"

    "github.com/wtsi-hgi/wrstat-ui/basedirs"
    "github.com/wtsi-hgi/wrstat-ui/db"
    bolt "github.com/wtsi-hgi/wrstat-ui/bolt"
)

// The cmd package should depend only on interfaces. These thin wrappers
// centralise the current bolt-backed implementation so other files in cmd do
// not import bolt directly. Swapping backend later means changing only here.

// openBasedirsReadOnly returns an interface-based basedirs.Store.
func openBasedirsReadOnly(path string) (basedirs.Store, error) { return bolt.OpenReadOnlyBasedirs(path) }

// newBasedirs returns an interface-based basedirs.Store for writes.
func newBasedirs(path string) (basedirs.Store, error) { return bolt.NewBasedirs(path) }

// newDirSource returns a db.Source representing a DGUTA repository source.
func newDirSource(path string) db.Source { return bolt.NewDirSource(path) }

// findDBDirs lists latest versioned DB dirs under basepath that contain required entries.
func findDBDirs(basepath string, required ...string) ([]string, []string, error) {
    return bolt.FindDBDirs(basepath, required...)
}

// startServerReloader wires a backend-specific reloader to the server via Loader.
func startServerReloader(
    s interface{ SetSourceFromPath(func(string) db.Source); LoadDBs([]db.Source, basedirs.MultiReader) error },
    basepath string,
    required []string,
    dgutaDirBasename string,
    basedirBasename string,
    ownersPath string,
    pollFrequency time.Duration,
    removeOld bool,
    extra []string,
) (interface{ Stop() }, error) {
    return bolt.StartServerReloader(s, basepath, required, dgutaDirBasename, basedirBasename, ownersPath, pollFrequency, removeOld, extra)
}
