package bolt

import (
	"path/filepath"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Loader is the minimal server surface needed for reloading.
type Loader interface {
	SetSourceFromPath(func(string) db.Source)
	LoadDBs([]db.Source, basedirs.MultiReader, map[string]int64, ...string) error
}

// StartServerReloader wires a Reloader to a server via the Loader interface.
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
) (*Reloader, error) {
	// Ensure the server knows how to convert backend-specific paths to db.Source
	s.SetSourceFromPath(func(p string) db.Source { return NewDirSource(p) })

	r := NewReloader()

	onChange := func(dirs, _ []string) bool {
		var srcs []db.Source
		var stores []basedirs.Store
		for _, d := range dirs {
			srcs = append(srcs, NewDirSource(filepath.Join(d, dgutaDirBasename)))
			bdb, err := OpenReadOnlyBasedirs(filepath.Join(d, basedirBasename))
			if err != nil {
				return false
			}
			stores = append(stores, bdb)
		}

		bmr, err := basedirs.OpenMulti(ownersPath, stores...)
		if err != nil {
			return false
		}

		// Generate timestamps using the MountPoint method from each source
		ts := make(map[string]int64, len(srcs))
		for _, src := range srcs {
			mountPoint := src.MountPoint()
			if mountPoint == "" {
				continue
			}
			ts[mountPoint] = src.ModTime().Unix()
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
