package bolt

import (
	"path/filepath"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Loader is the minimal server surface needed for reloading.
type Loader interface {
	SetSourceFromPath(fn func(path string) db.Source)
	LoadDBs(srcs []db.Source, bd basedirs.MultiReader) error
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
	_ []string,
) (*Reloader, error) {
	// Ensure the server knows how to convert backend-specific paths to db.Source
	s.SetSourceFromPath(func(p string) db.Source { return NewDirSource(p) })

	r := NewReloader()

	// loadForDirs handles loading sources and basedirs for the given directories.
	// Extracted out of StartServerReloader to reduce the cognitive complexity of
	// the parent function.
	loadForDirs := func(loader Loader, dirs []string, dgutaBase, basedirBase, owners string) bool {
		var (
			srcs   []db.Source
			stores []basedirs.Store
		)

		for _, d := range dirs {
			dirSrc := NewDirSource(filepath.Join(d, dgutaBase))
			srcs = append(srcs, dirSrc)

			bdb, err := OpenReadOnlyBasedirs(filepath.Join(d, basedirBase))
			if err != nil {
				return false
			}

			stores = append(stores, bdb)
		}

		bmr, err := basedirs.OpenMulti(owners, stores...)
		if err != nil {
			return false
		}

		// No need to manually extract timestamps anymore, as they are derived from sources
		if err := loader.LoadDBs(srcs, bmr); err != nil {
			return false
		}

		return true
	}

	onChange := func(dirs, _ []string) bool {
		return loadForDirs(s, dirs, dgutaDirBasename, basedirBasename, ownersPath)
	}

	if err := r.Start(basepath, pollFrequency, removeOld, required, onChange); err != nil {
		return nil, err
	}

	return r, nil
}
