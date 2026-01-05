/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package discovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/datasets"
)

const (
	numDatasetDirParts = 2
	keyPart            = 1
)

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

	if !entry.IsDir() || !datasets.IsValidDatasetDirName(name) {
		return false
	}

	return datasets.HasRequiredFiles(filepath.Join(basepath, name), required...)
}

func addEntryToMap(entry fs.DirEntry, latest map[string]nameVersion, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", numDatasetDirParts)
	key := parts[keyPart]

	version, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		// If the version is not numeric, treat it as very old.
		version = -1
	}

	previous, ok := latest[key]
	if ok && previous.version > version {
		toDelete = append(toDelete, entry.Name())

		return toDelete
	}

	if ok {
		toDelete = append(toDelete, previous.name)
	}

	latest[key] = nameVersion{name: entry.Name(), version: version}

	return toDelete
}

// FindDatasetDirs returns the latest dataset directory for each mount key, and
// a list of older dataset directories that may be removed.
func FindDatasetDirs(basepath string, required ...string) ([]string, []string, error) {
	return findDBDirs(basepath, required...)
}
