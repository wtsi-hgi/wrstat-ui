package dbdirs

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
)

var validDBDir = regexp.MustCompile(`^[^.][^_]*_.`)

// IsValidDBDir returns true if the given entry is a directory named with the
// correct format and containing the required files.
func IsValidDBDir(entry fs.DirEntry, basepath string, required ...string) bool {
	name := entry.Name()
	if !entry.IsDir() || !validDBDir.MatchString(name) {
		return false
	}

	for _, req := range required {
		if !EntryExists(filepath.Join(basepath, name, req)) {
			return false
		}
	}

	return true
}

// EntryExists returns true if the path exists.
func EntryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}
