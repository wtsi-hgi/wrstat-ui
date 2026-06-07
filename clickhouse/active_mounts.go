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

package clickhouse

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
)

const (
	noRowsCondition      = "1 = 0"
	activeMountTupleArgs = 2
	activeMountDirArgs   = 3
)

// ActiveSnapshotMatches reports whether mountPath is already active for the
// deterministic snapshot derived from updatedAt.
func ActiveSnapshotMatches(cfg Config, mountPath string, updatedAt time.Time) (bool, error) {
	if err := validateConfig(cfg); err != nil {
		return false, err
	}

	conn, err := connectFromConfig(cfg)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := configQueryContext(cfg)
	defer cancel()

	activeSID, hasActive, err := readActiveSnapshotID(ctx, conn, mountPath)
	if err != nil {
		return false, err
	}

	return hasActive && activeSID == snapshotID(mountPath, updatedAt).String(), nil
}

func invalidateActiveMetadataCache(cfg Config) {
	treeQueryCacheForConfig(cfg).reset()
}

type activeMount struct {
	mountPath  string
	snapshotID string
	updatedAt  time.Time
}

func activeMountsQuery(
	queryFmt, mountColumn, snapshotColumn string,
	mounts []activeMount,
	args ...any,
) (string, []any) {
	condition, activeArgs := activeMountsTupleCondition(
		mountColumn, snapshotColumn, mounts,
	)

	return fmt.Sprintf(queryFmt, condition), append(args, activeArgs...)
}

func activeMountsTupleCondition(
	mountColumn, snapshotColumn string,
	mounts []activeMount,
) (string, []any) {
	if len(mounts) == 0 {
		return noRowsCondition, nil
	}

	var b strings.Builder
	b.WriteString("(")
	b.WriteString(mountColumn)
	b.WriteString(", ")
	b.WriteString(snapshotColumn)
	b.WriteString(") IN (")

	args := make([]any, 0, len(mounts)*activeMountTupleArgs)
	for i, mount := range mounts {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, toUUID(?))")

		args = append(args, mount.mountPath, mount.snapshotID)
	}

	b.WriteString(")")

	return b.String(), args
}

func activeMountRootDirTuplesQuery(mounts []activeMount) (string, []any) {
	condition, args := activeMountRootDirTuplesCondition(
		"d.mount_path",
		"d.snapshot_id",
		"d.dir",
		mounts,
	)

	return fmt.Sprintf(dgutasForActiveMountRootDirsQuery, condition), args
}

func activeMountRootDirTuplesCondition(
	mountColumn, snapshotColumn, dirColumn string,
	mounts []activeMount,
) (string, []any) {
	if len(mounts) == 0 {
		return noRowsCondition, nil
	}

	var b strings.Builder
	b.WriteString("(")
	b.WriteString(mountColumn)
	b.WriteString(", ")
	b.WriteString(snapshotColumn)
	b.WriteString(", ")
	b.WriteString(dirColumn)
	b.WriteString(") IN (")

	args := make([]any, 0, len(mounts)*activeMountDirArgs)
	for i, mount := range mounts {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(?, ?, ?)")

		args = append(args, mount.mountPath, mount.snapshotID, mount.mountPath)
	}

	b.WriteString(")")

	return b.String(), args
}

func activeMountPathsQuery(
	queryFmt, mountColumn string,
	mounts []activeMount,
	args ...any,
) (string, []any) {
	condition, activeArgs := activeMountPathsCondition(mountColumn, mounts)

	return fmt.Sprintf(queryFmt, condition), append(args, activeArgs...)
}

func activeMountPathsCondition(
	mountColumn string,
	mounts []activeMount,
) (string, []any) {
	if len(mounts) == 0 {
		return noRowsCondition, nil
	}

	var b strings.Builder
	b.WriteString(mountColumn)
	b.WriteString(" IN (")

	args := make([]any, 0, len(mounts))
	for i, mount := range mounts {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("?")

		args = append(args, mount.mountPath)
	}

	b.WriteString(")")

	return b.String(), args
}

func (d *clickHouseDatabase) currentActiveMountsSet(
	ctx context.Context,
) (string, []activeMount, error) {
	if d.snapshot != nil {
		return d.cachedSnapshotActiveMounts(d.snapshot)
	}

	if d.conn == nil {
		return "", nil, nil
	}

	d.treeCache.recordActiveMetadataMiss()

	rows, err := queryMountsActiveRows(ctx, d.conn)
	if err != nil {
		return "", nil, err
	}

	snapshot := newActiveMountsSnapshot(rows)

	return snapshot.fingerprint, snapshot.all(), nil
}

func (d *clickHouseDatabase) cachedSnapshotActiveMounts(
	snapshot *activeMountsSnapshot,
) (string, []activeMount, error) {
	if snapshot == nil {
		return "", nil, nil
	}

	key := newTreeActiveMetadataCacheKey(
		snapshot.fingerprint,
		currentSchemaVersion,
		activeMetadataQueryVersion,
	)

	metadata, ok := d.treeCache.getActiveMetadata(key)
	if ok {
		return metadata.activeSetID, metadata.mounts, nil
	}

	metadata = newTreeActiveMetadata(snapshot.fingerprint, snapshot.all())
	d.treeCache.putActiveMetadata(key, metadata)

	return metadata.activeSetID, metadata.mounts, nil
}

type activeMountsSnapshot struct {
	mounts           []activeMount
	fingerprint      string
	treeSummaryReady atomic.Bool
}

func newActiveMountsSnapshot(rows []mountsActiveRow) *activeMountsSnapshot {
	mounts := make([]activeMount, len(rows))

	for i, row := range rows {
		mounts[i] = activeMount{
			mountPath:  row.mountPath,
			snapshotID: row.snapshotID,
			updatedAt:  row.updatedAt.UTC(),
		}
	}

	return &activeMountsSnapshot{
		mounts:      mounts,
		fingerprint: fingerprintForMountsActive(rows),
	}
}

func (s *activeMountsSnapshot) resolve(dir string) (activeMount, bool) {
	if s == nil {
		return activeMount{}, false
	}

	dir = ensureTrailingSlash(dir)

	var (
		best activeMount
		ok   bool
	)

	for _, mount := range s.mounts {
		if !strings.HasPrefix(dir, mount.mountPath) {
			continue
		}

		if ok && len(mount.mountPath) <= len(best.mountPath) {
			continue
		}

		best = mount
		ok = true
	}

	return best, ok
}

func (s *activeMountsSnapshot) mount(mountPath string) (activeMount, bool) {
	if s == nil {
		return activeMount{}, false
	}

	mountPath = ensureTrailingSlash(mountPath)

	for _, mount := range s.mounts {
		if mount.mountPath == mountPath {
			return mount, true
		}
	}

	return activeMount{}, false
}

func (s *activeMountsSnapshot) under(dir string) []activeMount {
	if s == nil {
		return nil
	}

	return activeMountsUnderDir(dir, s.mounts)
}

func activeMountsUnderDir(dir string, mounts []activeMount) []activeMount {
	dir = ensureTrailingSlash(dir)

	out := make([]activeMount, 0, len(mounts))
	for _, mount := range mounts {
		if strings.HasPrefix(mount.mountPath, dir) {
			out = append(out, mount)
		}
	}

	return out
}

func (s *activeMountsSnapshot) all() []activeMount {
	if s == nil {
		return nil
	}

	return slices.Clone(s.mounts)
}

func (s *activeMountsSnapshot) markTreeSummaryReady() {
	if s != nil {
		s.treeSummaryReady.Store(true)
	}
}

func (s *activeMountsSnapshot) treeSummaryFingerprint() (string, bool, error) {
	if s == nil || !s.treeSummaryReady.Load() || s.fingerprint == "" {
		return "", false, nil
	}

	return s.fingerprint, true, nil
}

func (s *activeMountsSnapshot) mountTimestamps() map[string]time.Time {
	if s == nil {
		return nil
	}

	out := make(map[string]time.Time, len(s.mounts))

	for _, mount := range s.mounts {
		out[mountTimestampKey(mount.mountPath)] = mount.updatedAt.UTC()
	}

	return out
}

func mountTimestampKey(mountPath string) string {
	return mountpath.EncodeKey(mountPath)
}
