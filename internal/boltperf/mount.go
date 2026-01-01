package boltperf

import (
	"errors"
	"fmt"
	"strings"
)

var ErrDatasetDirMissingUnderscore = errors.New("dataset dir name missing '_' separator")

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
