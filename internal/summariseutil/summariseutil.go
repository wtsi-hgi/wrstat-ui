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

package summariseutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
)

const rootMountPath = "/"

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

// NewBaseDirsCreator returns a configured basedirs creator.
func NewBaseDirsCreator(
	store basedirs.Store,
	quotas *basedirs.Quotas,
	mountpoints []string,
	modtime time.Time,
) (*basedirs.BaseDirs, error) {
	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		return nil, fmt.Errorf("failed to create new basedirs creator: %w", err)
	}

	if len(mountpoints) > 0 {
		bd.SetMountPoints(mountpoints)
	}

	bd.SetModTime(modtime)

	return bd, nil
}

// SetBatchSize applies batchSize to targets that support SetBatchSize.
func SetBatchSize(batchSize int, targets ...any) {
	if batchSize <= 0 {
		return
	}

	for _, target := range targets {
		setter, ok := target.(batchSizeSetter)
		if !ok {
			continue
		}

		setter.SetBatchSize(batchSize)
	}
}

type batchSizeSetter interface {
	SetBatchSize(batchSize int)
}

// ComposePublishCloser closes file, basedirs and DGUTA resources in the
// publish/abort order shared by summarise and ClickHouse perf imports.
func ComposePublishCloser(
	fileCloser io.Closer,
	basedirsCloser func(bool) error,
	dgutaCloser io.Closer,
) func(bool) error {
	return func(publish bool) error {
		fileErr := closeOptional(fileCloser)
		shouldPublishBasedirs := publish && fileErr == nil

		basedirsErr := closePublishFunc(basedirsCloser, shouldPublishBasedirs)
		dgutaErr := CloseOrAbort(
			dgutaCloser,
			shouldPublishBasedirs && basedirsErr == nil,
		)

		if shouldPublishBasedirs && (basedirsErr != nil || dgutaErr != nil) {
			basedirsErr = errors.Join(basedirsErr, closePublishFunc(basedirsCloser, false))
		}

		return errors.Join(fileErr, basedirsErr, dgutaErr)
	}
}

func closeOptional(closer io.Closer) error {
	if closer == nil {
		return nil
	}

	return closer.Close()
}

// CloseOrAbort closes closer when publishing, or calls Abort when available.
func CloseOrAbort(closer io.Closer, publish bool) error {
	if closer == nil {
		return nil
	}

	if publish {
		return closer.Close()
	}

	aborter, ok := closer.(interface{ Abort() error })
	if ok {
		return aborter.Abort()
	}

	return closer.Close()
}

func closePublishFunc(closer func(bool) error, publish bool) error {
	if closer == nil {
		return nil
	}

	return closer(publish)
}

// DeriveMountPathFromOutputDir extracts the mount path from a dataset output
// directory or from a child path inside it.
//
// If the directory name doesn't match the expected format, "/" is returned
// as a fallback for backwards compatibility.
func DeriveMountPathFromOutputDir(outputPath string) string {
	mountPath, err := mountpath.FromOutputDir(outputPath)
	if err != nil {
		return rootMountPath
	}

	return mountPath
}
