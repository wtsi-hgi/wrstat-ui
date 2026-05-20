/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

// Package mountpath encodes and decodes dataset directory mount-path keys.
package mountpath

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/datasets"
)

const (
	fullwidthSolidus = "／" // U+FF0F FULLWIDTH SOLIDUS
	pathSeparator    = "/"
)

var (
	// ErrEmptyOutputDir is returned when mount path derivation is given an
	// empty output directory.
	ErrEmptyOutputDir = errors.New("empty output dir")
	// ErrDatasetDirBadFormat is returned when neither the output directory nor
	// its parent has a dataset directory basename.
	ErrDatasetDirBadFormat = errors.New("dataset dir basename must be <version>_<mountKey>")
	// ErrDatasetDirBadMountPath is returned when a decoded mount path is not
	// absolute.
	ErrDatasetDirBadMountPath = errors.New("dataset dir mount path must be absolute")
	// ErrDatasetDirEmptyMountKey is returned when the dataset basename has no
	// usable mount key.
	ErrDatasetDirEmptyMountKey = errors.New("dataset dir mount key is empty")
)

// FromOutputDir derives the mount path from an output directory path.
//
// The dataset directory basename must be `<version>_<mountKey>` where mountKey
// uses ／ (U+FF0F) instead of /.
//
// The provided outputDir may be either the dataset directory itself or a
// subpath inside it.
//
// Returns the mount path ending with /.
func FromOutputDir(outputDir string) (string, error) {
	if strings.TrimSpace(outputDir) == "" {
		return "", ErrEmptyOutputDir
	}

	clean := filepath.Clean(outputDir)
	candidates := [...]string{
		filepath.Base(clean),
		filepath.Base(filepath.Dir(clean)),
	}

	for _, base := range candidates {
		mountPath, ok, err := mountPathFromDatasetDirBase(base)
		if err != nil {
			return "", err
		}

		if ok {
			return mountPath, nil
		}
	}

	return "", ErrDatasetDirBadFormat
}

func mountPathFromDatasetDirBase(dirBase string) (mountPath string, ok bool, err error) {
	dirBase = strings.TrimPrefix(dirBase, ".")
	if strings.HasSuffix(dirBase, "_") {
		return "", false, ErrDatasetDirEmptyMountKey
	}

	_, mountKey, ok := datasets.SplitDatasetDirName(dirBase)
	if !ok {
		return "", false, nil
	}

	if !strings.Contains(mountKey, fullwidthSolidus) {
		return "", false, nil
	}

	mountPath = DecodeKey(mountKey)
	if mountPath == "" {
		return "", false, ErrDatasetDirEmptyMountKey
	}

	if !strings.HasPrefix(mountPath, pathSeparator) {
		return "", false, ErrDatasetDirBadMountPath
	}

	return mountPath, true, nil
}

// DecodeKey converts a dataset mount key back to a slash-delimited mount path.
func DecodeKey(mountKey string) string {
	if mountKey == "" {
		return ""
	}

	mountPath := strings.ReplaceAll(mountKey, fullwidthSolidus, pathSeparator)
	if !strings.HasSuffix(mountPath, pathSeparator) {
		mountPath += pathSeparator
	}

	return mountPath
}

// EncodeKey converts a slash-delimited mount path into a dataset mount key.
func EncodeKey(mountPath string) string {
	return strings.ReplaceAll(mountPath, pathSeparator, fullwidthSolidus)
}

// DecodeSortedKeys converts mount-key map keys into sorted mount paths.
func DecodeSortedKeys[M ~map[string]V, V any](mountKeys M) []string {
	paths := make([]string, 0, len(mountKeys))

	for key := range mountKeys {
		paths = append(paths, DecodeKey(key))
	}

	slices.Sort(paths)

	return paths
}
