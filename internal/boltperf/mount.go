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

package boltperf

import (
	"errors"
	"fmt"
	"strings"
)

// ErrDatasetDirMissingUnderscore is returned when a dataset directory name
// does not contain the expected '<version>_<mountKey>' underscore separator.
var ErrDatasetDirMissingUnderscore = errors.New("dataset dir name missing '_' separator")

// DeriveMountPathFromDatasetDirName derives a mount path from a dataset
// directory basename of the form '<version>_<mountKey>'.
//
// It replaces fullwidth solidus characters (U+FF0F '／') with '/', and ensures
// the returned mount path ends with '/'.
func DeriveMountPathFromDatasetDirName(dirName string) (string, error) {
	parts := strings.SplitN(dirName, "_", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("%w: %q", ErrDatasetDirMissingUnderscore, dirName)
	}

	mountKey := parts[1]

	mountPath := strings.ReplaceAll(mountKey, "／", "/")
	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath, nil
}
