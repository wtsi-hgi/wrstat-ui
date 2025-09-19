package reloader

import (
	"path/filepath"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	bolt "github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// StartServerReloader wires a bolt.Reloader to a server.Server.
//
// Parameters:
// - s: server to load DBs into on change
// - basepath: directory to scan for versioned DB directories
// - required: names that must exist inside each versioned directory for it to be valid (eg. "dguta.dbs", "basedirs.db" or test fixtures like "dirguta", "basedir.db")
// - dgutaDirBasename: subdirectory name holding the dirguta DBs (eg. "dirguta")
// - basedirBasename: filename of the basedirs DB (eg. "basedirs.db" or "basedir.db")
// - ownersPath: path to the gid,owner csv used by basedirs.OpenMulti
// - pollFrequency: how often to poll for changes
// - removeOld: whether to delete older versioned directories after successful reload
// - mounts: optional mountpoints to attach to basedirs reader
//
// Returns the started reloader; call Stop() on it to terminate.
// Loader is the minimal surface from a server we need to perform reloads.
type Loader interface {
	SetSourceFromPath(func(string) db.Source)
	LoadDBs([]db.Source, basedirs.MultiReader, map[string]int64, ...string) error
}

func StartServerReloader(
	s Loader,
	basepath string,
	required []string,
	dgutaDirBasename string,
	basedirBasename string,
	ownersPath string,
	pollFrequency time.Duration,
	removeOld bool,
	mounts []string,
) (*bolt.Reloader, error) {
	// Ensure the server knows how to convert backend-specific paths to db.Source
	s.SetSourceFromPath(func(p string) db.Source { return bolt.NewDirSource(p) })

	r := bolt.NewReloader()

	onChange := func(dirs, _ []string) bool {
		// Build sources and basedirs reader for the selected dirs
		var srcs []db.Source
		var bdbs []*bolt.DB
		for _, d := range dirs {
			srcs = append(srcs, bolt.NewDirSource(filepath.Join(d, dgutaDirBasename)))
			bdb, err := basedirs.OpenDBRO(filepath.Join(d, basedirBasename))
			if err != nil {
				return false
			}
			bdbs = append(bdbs, bdb)
		}

		bmr, err := basedirs.OpenMulti(ownersPath, bdbs...)
		if err != nil {
			return false
		}

		ts, err := bolt.TimestampsFromDirs(dirs)
		if err != nil {
			return false
		}

		if err := s.LoadDBs(srcs, bmr, ts, mounts...); err != nil {
			return false
		}
		return true
	}

	if err := r.Start(basepath, pollFrequency, removeOld, required, onChange); err != nil {
		return nil, err
	}
	return r, nil
}
