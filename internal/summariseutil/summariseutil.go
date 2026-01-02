package summariseutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

const (
	numDatasetDirParts   = 2
	fullwidthSolidus     = "／"
	fullwidthReplacement = "/"
)

// ErrDatasetDirMissingUnderscore is returned when a dataset directory name
// does not contain the expected '<version>_<mountKey>' underscore separator.
var ErrDatasetDirMissingUnderscore = errors.New("dataset dir missing '_' separator")

// ParseBasedirConfig parses quotas and basedirs config files.
func ParseBasedirConfig(quotaPath, basedirsConfig string) (*basedirs.Quotas, basedirs.Config, error) {
	quotas, err := basedirs.ParseQuotas(quotaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing quotas file: %w", err)
	}

	cf, err := os.Open(basedirsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening basedirs config: %w", err)
	}
	defer cf.Close()

	config, err := basedirs.ParseConfig(cf)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing basedirs config: %w", err)
	}

	return quotas, config, nil
}

// ParseMountpointsFromFile parses a file containing quoted mountpoints.
//
// Each non-empty line must be a Go-quoted string (as produced by
// 'findmnt ... | sed ...'), and the returned slice preserves file order.
func ParseMountpointsFromFile(mountpoints string) ([]string, error) {
	if mountpoints == "" {
		return nil, nil
	}

	data, err := os.ReadFile(mountpoints)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	mounts := make([]string, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		mountpoint, err := strconv.Unquote(line)
		if err != nil {
			return nil, err
		}

		mounts = append(mounts, mountpoint)
	}

	return mounts, nil
}

// DeriveMountPathFromOutputDir extracts the mount path from the parent
// directory of the output path.
//
// The parent directory is expected to have the form '<version>_<mountKey>'
// where <mountKey> is the mount path with '/' replaced by '／' (fullwidth
// solidus).
//
// If the directory name doesn't match the expected format, "/" is returned
// as a fallback for backwards compatibility.
func DeriveMountPathFromOutputDir(outputPath string) string {
	parentDir := filepath.Base(filepath.Dir(outputPath))

	parts := strings.SplitN(parentDir, "_", numDatasetDirParts)
	if len(parts) != numDatasetDirParts {
		// Fallback to root mount path for backwards compatibility
		return "/"
	}

	mountKey := parts[1]
	mountPath := strings.ReplaceAll(mountKey, fullwidthSolidus, fullwidthReplacement)

	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath
}
