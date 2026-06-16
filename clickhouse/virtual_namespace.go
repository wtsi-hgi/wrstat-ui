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

package clickhouse

import (
	"slices"
	"sort"
	"time"
)

const (
	activeVirtualRootID       uint32 = 1
	activeVirtualNoParentID   uint32 = 0
	activeVirtualZeroSnapshot        = "00000000-0000-0000-0000-000000000000"
)

type activeVirtualMountRootLink struct {
	SnapshotID string
	DirID      uint32
	ChildCount uint64
}

type activeVirtualDirRow struct {
	ActiveSetID    string
	VirtualID      uint32
	ParentID       uint32
	Name           string
	FullPath       string
	MountPath      string
	SnapshotID     string
	MountRootDirID uint32
	IsMountRootBox uint8
	RefreshedAt    time.Time
}

type activeVirtualNamespace struct {
	rows   []activeVirtualDirRow
	byPath map[string]int
}

func activeVirtualNamespaceForMounts(
	activeSetID string,
	mounts []activeMount,
	links map[string]activeVirtualMountRootLink,
	refreshedAt time.Time,
) activeVirtualNamespace {
	namespace := newActiveVirtualNamespace(activeSetID, refreshedAt)
	for _, mount := range sortedActiveMounts(mounts) {
		namespace.addMount(mount, links[ensureTrailingSlash(mount.mountPath)])
	}

	return namespace
}

func newActiveVirtualNamespace(activeSetID string, refreshedAt time.Time) activeVirtualNamespace {
	root := activeVirtualDirRow{
		ActiveSetID: activeSetID,
		VirtualID:   activeVirtualRootID,
		ParentID:    activeVirtualNoParentID,
		Name:        "/",
		FullPath:    "/",
		SnapshotID:  activeVirtualZeroSnapshot,
		RefreshedAt: refreshedAt,
	}

	return activeVirtualNamespace{
		rows:   []activeVirtualDirRow{root},
		byPath: map[string]int{"/": 0},
	}
}

func (n *activeVirtualNamespace) addMount(mount activeMount, link activeVirtualMountRootLink) {
	parent := "/"
	mountPath := ensureTrailingSlash(mount.mountPath)

	for {
		child, ok := immediateChildForMount(parent, mountPath)
		if !ok {
			return
		}

		child = ensureTrailingSlash(child)
		childIsMountRoot := child == mountPath

		row := n.ensureChild(parent, child)
		if childIsMountRoot {
			n.markMountRoot(row, mount, link)

			return
		}

		parent = child
	}
}

func (n *activeVirtualNamespace) ensureChild(parentPath, childPath string) *activeVirtualDirRow {
	childPath = ensureTrailingSlash(childPath)
	if idx, ok := n.byPath[childPath]; ok {
		return &n.rows[idx]
	}

	parentID := n.idForPath(parentPath)
	n.rows = append(n.rows, activeVirtualDirRow{
		ActiveSetID: n.rows[0].ActiveSetID,
		VirtualID:   uint32(len(n.rows) + 1), //nolint:gosec // Namespace rows are bounded by active mount path segments.
		ParentID:    parentID,
		Name:        catalogNameForFullPath(childPath),
		FullPath:    childPath,
		SnapshotID:  activeVirtualZeroSnapshot,
		RefreshedAt: n.rows[0].RefreshedAt,
	})

	n.byPath[childPath] = len(n.rows) - 1

	return &n.rows[len(n.rows)-1]
}

func (n *activeVirtualNamespace) markMountRoot(
	row *activeVirtualDirRow,
	mount activeMount,
	link activeVirtualMountRootLink,
) {
	row.MountPath = ensureTrailingSlash(mount.mountPath)

	row.SnapshotID = mount.snapshotID
	if link.SnapshotID != "" {
		row.SnapshotID = link.SnapshotID
	}

	row.MountRootDirID = link.DirID
	row.IsMountRootBox = 1
}

func (n activeVirtualNamespace) idForPath(path string) uint32 {
	if idx, ok := n.byPath[ensureTrailingSlash(path)]; ok {
		return n.rows[idx].VirtualID
	}

	return 0
}

func (n activeVirtualNamespace) parentIDForPath(path string) uint32 {
	if idx, ok := n.byPath[ensureTrailingSlash(path)]; ok {
		return n.rows[idx].ParentID
	}

	return 0
}

func (n activeVirtualNamespace) rowForPath(path string) activeVirtualDirRow {
	if idx, ok := n.byPath[ensureTrailingSlash(path)]; ok {
		return n.rows[idx]
	}

	return activeVirtualDirRow{}
}

func (n activeVirtualNamespace) childRows(activeSetID string, refreshedAt time.Time) []activeVirtualChildRow {
	rows := make([]activeVirtualChildRow, 0, len(n.rows)-1)
	for _, row := range n.rows {
		if row.ParentID == activeVirtualNoParentID {
			continue
		}

		parent := n.rowForID(row.ParentID)
		rows = append(rows, activeVirtualChildRow{
			ActiveSetID:     activeSetID,
			ParentDir:       parent.FullPath,
			ChildDir:        row.FullPath,
			ParentVirtualID: row.ParentID,
			ChildVirtualID:  row.VirtualID,
			MountPath:       row.MountPath,
			SnapshotID:      row.SnapshotID,
			MountRootDirID:  row.MountRootDirID,
			IsMountRootBox:  row.IsMountRootBox,
			RefreshedAt:     refreshedAt,
		})
	}

	return rows
}

func (n activeVirtualNamespace) rowForID(virtualID uint32) activeVirtualDirRow {
	if virtualID == 0 {
		return activeVirtualDirRow{}
	}

	idx := int(virtualID - 1)
	if idx < 0 || idx >= len(n.rows) {
		return activeVirtualDirRow{}
	}

	return n.rows[idx]
}

func sortedActiveMounts(mounts []activeMount) []activeMount {
	out := slices.Clone(mounts)
	sort.Slice(out, func(i, j int) bool {
		return ensureTrailingSlash(out[i].mountPath) < ensureTrailingSlash(out[j].mountPath)
	})

	return out
}
