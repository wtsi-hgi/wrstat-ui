package bolt

import (
	"os"
	"path/filepath"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
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

// dirSourceFactory adapts string paths into DirSource.
type dirSourceFactory struct{}

func (dirSourceFactory) FromPath(path string) (db.Source, error) { //nolint:ireturn
	return NewDirSource(path), nil
}

func init() {
	// Ensure the db package knows how to convert string paths to a Source when
	// callers use NewDB(paths...). Also ensure the bolt storage factory is the
	// default one already via dirstore.go's init.
	db.SetDefaultSourceFactory(dirSourceFactory{})
}
