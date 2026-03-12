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
	"fmt"
	"strings"
	"time"
)

const (
	activeMountTupleArgCount = 2
	activeMountPathArgCount  = 1
)

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

	allArgs := make([]any, 0, len(args)+len(activeArgs))
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, activeArgs...)

	return fmt.Sprintf(queryFmt, condition), allArgs
}

func activeMountsTupleCondition(
	mountColumn, snapshotColumn string,
	mounts []activeMount,
) (string, []any) {
	if len(mounts) == 0 {
		return "1 = 0", nil
	}

	var b strings.Builder
	b.WriteString("(")
	b.WriteString(mountColumn)
	b.WriteString(", ")
	b.WriteString(snapshotColumn)
	b.WriteString(") IN (")

	args := make([]any, 0, len(mounts)*activeMountTupleArgCount)
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

func activeMountPathsQuery(
	queryFmt, mountColumn string,
	mounts []activeMount,
	args ...any,
) (string, []any) {
	condition, activeArgs := activeMountPathsCondition(mountColumn, mounts)

	allArgs := make([]any, 0, len(args)+len(activeArgs))
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, activeArgs...)

	return fmt.Sprintf(queryFmt, condition), allArgs
}

func activeMountPathsCondition(
	mountColumn string,
	mounts []activeMount,
) (string, []any) {
	if len(mounts) == 0 {
		return "1 = 0", nil
	}

	var b strings.Builder
	b.WriteString(mountColumn)
	b.WriteString(" IN (")

	args := make([]any, 0, len(mounts)*activeMountPathArgCount)
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

type activeMountsSnapshot struct {
	mounts []activeMount
}

func newActiveMountsSnapshot(rows []mountsActiveRow) *activeMountsSnapshot {
	mounts := make([]activeMount, 0, len(rows))

	for _, row := range rows {
		mounts = append(mounts, activeMount{
			mountPath:  row.mountPath,
			snapshotID: row.snapshotID,
			updatedAt:  row.updatedAt.UTC(),
		})
	}

	return &activeMountsSnapshot{mounts: mounts}
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

	dir = ensureTrailingSlash(dir)

	mounts := make([]activeMount, 0, len(s.mounts))
	for _, mount := range s.mounts {
		if strings.HasPrefix(mount.mountPath, dir) {
			mounts = append(mounts, mount)
		}
	}

	return mounts
}

func (s *activeMountsSnapshot) all() []activeMount {
	if s == nil {
		return nil
	}

	mounts := make([]activeMount, len(s.mounts))
	copy(mounts, s.mounts)

	return mounts
}

func (s *activeMountsSnapshot) maxUpdatedAt(dir string) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}

	dir = ensureTrailingSlash(dir)

	var (
		latestUpdatedAt time.Time
		ok              bool
	)

	for _, mount := range s.mounts {
		if !strings.HasPrefix(mount.mountPath, dir) {
			continue
		}

		if ok && !mount.updatedAt.After(latestUpdatedAt) {
			continue
		}

		latestUpdatedAt = mount.updatedAt.UTC()
		ok = true
	}

	return latestUpdatedAt, ok
}

func (s *activeMountsSnapshot) mountTimestamps() map[string]time.Time {
	out := make(map[string]time.Time, len(s.mounts))

	for _, mount := range s.mounts {
		out[strings.ReplaceAll(mount.mountPath, "/", "／")] = mount.updatedAt.UTC()
	}

	return out
}
