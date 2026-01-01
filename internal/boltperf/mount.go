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
