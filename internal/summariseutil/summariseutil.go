package summariseutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	dbBatchSize          = 10000
	numDatasetDirParts   = 2
	fullwidthSolidus     = "／"
	fullwidthReplacement = "/"
)

// ErrDatasetDirMissingUnderscore is returned when a dataset directory name
// does not contain the expected '<version>_<mountKey>' underscore separator.
var ErrDatasetDirMissingUnderscore = errors.New("dataset dir missing '_' separator")

// AddDirgutaSummariser adds the dirguta summariser to s and returns a close
// function for the underlying DB.
func AddDirgutaSummariser(s *summary.Summariser, dirgutaDB string,
	modtime time.Time) (func() error, error) {
	if err := os.RemoveAll(dirgutaDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(dirgutaDB, 0o755); err != nil {
		return nil, err
	}

	writer, err := bolt.NewDGUTAWriter(dirgutaDB)
	if err != nil {
		return nil, fmt.Errorf("failed to create dguta writer: %w", err)
	}

	mountPath := deriveMountPathFromOutputDir(dirgutaDB)

	writer.SetMountPath(mountPath)
	writer.SetUpdatedAt(modtime)
	writer.SetBatchSize(dbBatchSize)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))

	return writer.Close, nil
}

// AddBasedirsSummariser adds the basedirs summariser to s and configures it
// from the provided quota/config/mountpoints files.
func AddBasedirsSummariser(
	s *summary.Summariser,
	basedirsDB, basedirsHistoryDB, quotaPath, basedirsConfig, mountpoints string,
	modtime time.Time,
) (func() error, error) {
	bd, config, closeFn, err := newBasedirsCreator(
		basedirsDB,
		basedirsHistoryDB,
		quotaPath,
		basedirsConfig,
		mountpoints,
		modtime,
	)
	if err != nil {
		return nil, err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return closeFn, nil
}

func newBasedirsCreator(
	basedirsDB, basedirsHistoryDB, quotaPath, basedirsConfig, mountpoints string,
	modtime time.Time,
) (*basedirs.BaseDirs, basedirs.Config, func() error, error) {
	quotas, config, err := ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	if err = removeFileIfNotNotExist(basedirsDB); err != nil {
		return nil, nil, nil, err
	}

	mps, err := ParseMountpointsFromFile(mountpoints)
	if err != nil {
		return nil, nil, nil, err
	}

	store, err := bolt.NewBaseDirsStore(basedirsDB, basedirsHistoryDB)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create basedirs store: %w", err)
	}

	// Configure store metadata and create the basedirs creator.
	configureStoreMounts(store, basedirsDB, modtime)

	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		_ = store.Close()

		return nil, nil, nil, fmt.Errorf("failed to create new basedirs creator: %w", err)
	}

	if len(mps) > 0 {
		bd.SetMountPoints(mps)
	}

	bd.SetModTime(modtime)

	return bd, config, store.Close, nil
}

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

func removeFileIfNotNotExist(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
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

func configureStoreMounts(store basedirs.Store, basedirsDB string, modtime time.Time) {
	storeMountPath := deriveMountPathFromOutputDir(basedirsDB)

	store.SetMountPath(storeMountPath)
	store.SetUpdatedAt(modtime)
}

// deriveMountPathFromOutputDir extracts the mount path from the parent
// directory of the output path.
//
// The parent directory is expected to have the form '<version>_<mountKey>'
// where <mountKey> is the mount path with '/' replaced by '／' (fullwidth
// solidus).
//
// If the directory name doesn't match the expected format, "/" is returned
// as a fallback for backwards compatibility.
func deriveMountPathFromOutputDir(outputPath string) string {
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

// CopyHistory copies history entries from an existing basedirs DB into bd.
