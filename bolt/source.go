package bolt

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/internal/dbdirs"
)

// DirSource is a bolt implementation of db.Source backed by a directory
// containing dguta and children db files.
type DirSource struct {
	dir string
}

// NewDirSource creates a DirSource for the given directory path.
func NewDirSource(dir string) *DirSource { return &DirSource{dir: dir} }

// ModTime returns the directory modification time.
func (s *DirSource) ModTime() time.Time {
	fi, err := os.Lstat(s.dir)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// Exists reports whether both database files exist and are non-empty.
func (s *DirSource) Exists() (bool, error) {
	paths := []string{s.dgutaPath(), s.childrenPath()}
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return false, nil
		}
		if fi.Size() == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *DirSource) dgutaPath() string    { return filepath.Join(s.dir, "dguta.db") }
func (s *DirSource) childrenPath() string { return filepath.Join(s.dir, "dguta.db.children") }

// OpenMode is the default file mode used when creating bolt DB files.
const OpenMode = 0640

// DirDBPaths returns the two bolt file paths for a given directory.
func DirDBPaths(dir string) (dgutaPath, childrenPath string) {
	ds := NewDirSource(dir)
	return ds.dgutaPath(), ds.childrenPath()
}

// FindDBDirs finds the latest db directories under basepath that contain the
// required entries (eg, "dguta.dbs" and "basedirs.db"). It returns the full
// paths to the selected directories sorted by name, and a list of older
// directories that can be deleted.
func FindDBDirs(basepath string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}

	latest := make(map[string]nameVersion)
	var toDelete []string

	for _, entry := range entries {
		if !IsValidDBDir(entry, basepath, required...) {
			continue
		}
		toDelete = addEntryToMap(entry, latest, toDelete)
	}

	dirs := make([]string, 0, len(latest))
	for _, nt := range latest {
		dirs = append(dirs, filepath.Join(basepath, nt.name))
	}
	slices.Sort(dirs)

	return dirs, toDelete, nil
}

// IsValidDBDir returns true if the given entry is a directory named with the
// correct format and containing the required files.
func IsValidDBDir(entry fs.DirEntry, basepath string, required ...string) bool {
	return dbdirs.IsValidDBDir(entry, basepath, required...)
}

// entryExists is retained for backward compatibility within this package.
func entryExists(path string) bool { return dbdirs.EntryExists(path) }

type nameVersion struct{ name, version string }

func addEntryToMap(entry fs.DirEntry, latest map[string]nameVersion, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", 2)
	key := parts[1]
	version := parts[0]
	if previous, ok := latest[key]; ok && previous.version > version {
		// Current is older, mark it for deletion
		toDelete = append(toDelete, entry.Name())
	} else {
		if ok {
			toDelete = append(toDelete, previous.name)
		}
		latest[key] = nameVersion{name: entry.Name(), version: version}
	}
	return toDelete
}
