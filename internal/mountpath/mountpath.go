package mountpath

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/datasets"
)

const (
	fullwidthSolidus     = "／" // U+FF0F FULLWIDTH SOLIDUS
	fullwidthReplacement = "/"
)

var (
	ErrEmptyOutputDir          = errors.New("empty output dir")
	ErrDatasetDirBadFormat     = errors.New("dataset dir basename must be <version>_<mountKey>")
	ErrDatasetDirBadMountPath  = errors.New("dataset dir mount path must be absolute")
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
	base := filepath.Base(clean)

	mountPath, ok, err := mountPathFromDatasetDirBase(base)
	if err != nil {
		return "", err
	}

	if ok {
		return mountPath, nil
	}

	parentBase := filepath.Base(filepath.Dir(clean))

	mountPath, ok, err = mountPathFromDatasetDirBase(parentBase)
	if err != nil {
		return "", err
	}

	if ok {
		return mountPath, nil
	}

	return "", ErrDatasetDirBadFormat
}

func mountPathFromDatasetDirBase(dirBase string) (mountPath string, ok bool, err error) {
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

	mountPath = strings.ReplaceAll(mountKey, fullwidthSolidus, fullwidthReplacement)
	if mountPath == "" {
		return "", false, ErrDatasetDirEmptyMountKey
	}

	if !strings.HasPrefix(mountPath, "/") {
		return "", false, ErrDatasetDirBadMountPath
	}

	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	return mountPath, true, nil
}
