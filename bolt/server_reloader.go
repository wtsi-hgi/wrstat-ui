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
	LoadDBs([]db.Source, basedirs.MultiReader) error
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
			dirSrc := NewDirSource(filepath.Join(d, dgutaDirBasename))
			srcs = append(srcs, dirSrc)
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

		// No need to manually extract timestamps anymore, as they are derived from sources
		if err := s.LoadDBs(srcs, bmr); err != nil {
			return false
		}
		return true
	}

	if err := r.Start(basepath, pollFrequency, removeOld, required, onChange); err != nil {
		return nil, err
	}
	return r, nil
}
