package dbdirs

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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

// FindLatestDirs finds the latest versioned subdirectories under basepath that
// contain all required entries. It returns the absolute paths to the selected
// directories sorted by name, and a slice of older directories (by version)
// that may be deleted.
//
// Directories are expected to be named in the form "<version>_<key>", where
// version is a sortable string (eg. timestamp) and key groups multiple versions
// for the same dataset.
func FindLatestDirs(basepath string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}

	type nameVersion struct{ name, version string }

	latest := make(map[string]nameVersion)
	var toDelete []string

	for _, entry := range entries {
		if !IsValidDBDir(entry, basepath, required...) {
			continue
		}

		key, version, ok := parseEntryName(entry.Name())
		if !ok {
			continue
		}

		if prev, ok := latest[key]; !ok {
			latest[key] = nameVersion{name: entry.Name(), version: version}
		} else if prev.version <= version {
			toDelete = append(toDelete, prev.name)
			latest[key] = nameVersion{name: entry.Name(), version: version}
		} else {
			toDelete = append(toDelete, entry.Name())
		}
	}

	dirs := make([]string, 0, len(latest))
	for _, nv := range latest {
		dirs = append(dirs, filepath.Join(basepath, nv.name))
	}

	slices.Sort(dirs)

	return dirs, toDelete, nil
}

var partSep = "_"

// parseEntryName splits an entry name into version and key; returns ok=false
// if the format is invalid.
func parseEntryName(name string) (key, version string, ok bool) {
	parts := regexp.MustCompile("_").Split(name, 2)
	if len(parts) != 2 {
		return "", "", false
	}

	return parts[1], parts[0], true
}
