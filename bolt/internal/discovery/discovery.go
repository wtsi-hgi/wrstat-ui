package discovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	numDatasetDirParts = 2
	keyPart            = 1
)

var validDatasetDir = regexp.MustCompile(`^[^.][^_]*_.`)

type nameVersion struct {
	name    string
	version int64
}

func findDBDirs(basepath string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}

	var toDelete []string

	latest := make(map[string]nameVersion)

	for _, entry := range entries {
		if !isValidDatasetDir(entry, basepath, required...) {
			continue
		}

		toDelete = addEntryToMap(entry, latest, toDelete)
	}

	dirs := make([]string, 0, len(latest))
	for _, nv := range latest {
		dirs = append(dirs, filepath.Join(basepath, nv.name))
	}

	slices.Sort(dirs)

	return dirs, toDelete, nil
}

func isValidDatasetDir(entry fs.DirEntry, basepath string, required ...string) bool {
	name := entry.Name()

	if !entry.IsDir() || !validDatasetDir.MatchString(name) {
		return false
	}

	for _, requiredEntry := range required {
		if !entryExists(filepath.Join(basepath, name, requiredEntry)) {
			return false
		}
	}

	return true
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func addEntryToMap(entry fs.DirEntry, latest map[string]nameVersion, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", numDatasetDirParts)
	key := parts[keyPart]
	version, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		// If the version is not numeric, treat it as very old.
		version = -1
	}

	if previous, ok := latest[key]; previous.version > version {
		toDelete = append(toDelete, entry.Name())
	} else {
		if ok {
			toDelete = append(toDelete, previous.name)
		}

		latest[key] = nameVersion{name: entry.Name(), version: version}
	}

	return toDelete
}

// FindDatasetDirs returns the latest dataset directory for each mount key, and
// a list of older dataset directories that may be removed.
func FindDatasetDirs(basepath string, required ...string) ([]string, []string, error) {
	return findDBDirs(basepath, required...)
}
