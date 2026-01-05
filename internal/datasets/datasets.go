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

// Package datasets contains helpers for discovering wrstat-ui dataset
// directories on disk.
package datasets

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type nameVersion struct {
	name    string
	version string
}

// FindLatestDatasetDirs returns the latest dataset directory for each dataset
// key found directly under baseDir.
//
// Dataset directory names are expected to be of the form:
//
//	<version>_<key>
//
// "Latest" is determined by numeric comparison of <version> when possible.
// Numeric versions are always considered newer than non-numeric versions.
// If both versions are non-numeric, lexicographic comparison is used.
// If required is provided, each returned dataset directory must contain all
// those file basenames.
func FindLatestDatasetDirs(baseDir string, required ...string) ([]string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	latest := make(map[string]nameVersion)

	for _, entry := range entries {
		considerDatasetDirEntry(latest, baseDir, entry, required)
	}

	dirs := make([]string, 0, len(latest))
	for _, nv := range latest {
		dirs = append(dirs, filepath.Join(baseDir, nv.name))
	}

	slices.Sort(dirs)

	return dirs, nil
}

func considerDatasetDirEntry(latest map[string]nameVersion, baseDir string, entry fs.DirEntry, required []string) {
	name := entry.Name()
	if !entry.IsDir() || !IsValidDatasetDirName(name) {
		return
	}

	if !HasRequiredFiles(filepath.Join(baseDir, name), required...) {
		return
	}

	version, key, ok := splitDatasetDirName(name)
	if !ok {
		return
	}

	if previous, ok := latest[key]; ok {
		if compareDatasetVersions(previous.version, version) > 0 {
			return
		}
	}

	latest[key] = nameVersion{name: name, version: version}
}

// IsValidDatasetDirName validates a dataset directory name.
//
// It is compatible with the previous regexp used across the codebase:
//
//	`^[^.][^_]*_.`.
func IsValidDatasetDirName(name string) bool {
	if name == "" || strings.HasPrefix(name, ".") {
		return false
	}

	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 {
		return false
	}

	return parts[0] != "" && parts[1] != ""
}

// HasRequiredFiles checks that all required basenames exist within dir.
func HasRequiredFiles(dir string, required ...string) bool {
	for _, req := range required {
		if _, err := os.Stat(filepath.Join(dir, req)); err != nil {
			return false
		}
	}

	return true
}

func splitDatasetDirName(name string) (version, key string, ok bool) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func compareDatasetVersions(a, b string) int {
	ai, aOK := parsePositiveInt(a)
	bi, bOK := parsePositiveInt(b)

	switch {
	case aOK && bOK:
		return ai - bi
	case aOK && !bOK:
		return 1
	case !aOK && bOK:
		return -1
	default:
		return strings.Compare(a, b)
	}
}

func parsePositiveInt(s string) (int, bool) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}

	return v, true
}
