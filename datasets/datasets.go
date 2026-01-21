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

// FindDatasetDirs returns the latest dataset directory for each dataset key
// found directly under baseDir, and a list of older dataset directory names
// that may be removed.
//
// Dataset directory names are expected to be of the form:
//
//	<version>_<key>
//
// "Latest" is determined by numeric comparison of <version> when possible.
// Numeric versions are always considered newer than non-numeric versions.
// If both versions are non-numeric, lexicographic comparison is used.
//
// If required is provided, each considered dataset directory must contain all
// those file or directory basenames.
//
// Returned dataset directories are full paths. Returned toDelete entries are
// directory names (basenames) relative to baseDir.
func FindDatasetDirs(baseDir string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, nil, err
	}

	latest := make(map[string]nameVersion)
	toDelete := make([]string, 0)

	for _, entry := range entries {
		considerDatasetDirEntryWithDeletes(latest, &toDelete, baseDir, entry, required)
	}

	dirs := make([]string, 0, len(latest))
	for _, nv := range latest {
		dirs = append(dirs, filepath.Join(baseDir, nv.name))
	}

	slices.Sort(dirs)

	return dirs, toDelete, nil
}

func considerDatasetDirEntryWithDeletes(latest map[string]nameVersion,
	toDelete *[]string, baseDir string, entry fs.DirEntry, required []string) {
	name := entry.Name()

	if !isValidDatasetEntry(entry, name, baseDir, required) {
		return
	}

	version, key, ok := SplitDatasetDirName(name)
	if !ok {
		return
	}

	updateLatestWithDeletes(latest, toDelete, name, version, key)
}

// SplitDatasetDirName splits a dataset directory name into its <version> and
// <key> parts.
//
// Dataset directory names are expected to be of the form:
//
//	<version>_<key>
//
// It returns ok=false if the name is not a valid dataset directory name.
func SplitDatasetDirName(name string) (version string, key string, ok bool) {
	if !IsValidDatasetDirName(name) {
		return "", "", false
	}

	before, after, ok := strings.Cut(name, "_")
	if !ok {
		return "", "", false
	}

	return before, after, true
}

func isValidDatasetEntry(entry fs.DirEntry, name, baseDir string, required []string) bool {
	if !entry.IsDir() || !IsValidDatasetDirName(name) {
		return false
	}

	if !HasRequiredFiles(filepath.Join(baseDir, name), required...) {
		return false
	}

	return true
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

	before, after, ok := strings.Cut(name, "_")
	if !ok {
		return false
	}

	return before != "" && after != ""
}

// HasRequiredFiles checks that all required basenames exist within dir.
// A required basename may refer to either a file or a directory.
func HasRequiredFiles(dir string, required ...string) bool {
	for _, req := range required {
		if _, err := os.Stat(filepath.Join(dir, req)); err != nil {
			return false
		}
	}

	return true
}

func updateLatestWithDeletes(latest map[string]nameVersion, toDelete *[]string, name, version, key string) {
	previous, ok := latest[key]
	if !ok {
		latest[key] = nameVersion{name: name, version: version}

		return
	}

	if compareDatasetVersions(previous.version, version) > 0 {
		appendToDelete(toDelete, name)

		return
	}

	appendToDelete(toDelete, previous.name)
	latest[key] = nameVersion{name: name, version: version}
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

func appendToDelete(toDelete *[]string, name string) {
	if toDelete == nil {
		return
	}

	*toDelete = append(*toDelete, name)
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
// those file or directory basenames.
func FindLatestDatasetDirs(baseDir string, required ...string) ([]string, error) {
	dirs, _, err := FindDatasetDirs(baseDir, required...)

	return dirs, err
}
