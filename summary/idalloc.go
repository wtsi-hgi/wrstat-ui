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

package summary

import (
	"errors"
	"strings"
)

// ParentSentinel is the parent_id stored for filesystem root.
const ParentSentinel = ^uint32(0)

var (
	// ErrTooManyDirs is returned when the next directory id would collide with
	// the reserved parent sentinel value.
	ErrTooManyDirs = errors.New("too many directories for uint32 dir_id")
	// ErrNonContiguousInput is returned when directory boundaries are not a
	// single contiguous preorder traversal.
	ErrNonContiguousInput = errors.New("non-contiguous directory input")
	// ErrDirIDUnassigned is returned when a path is not in the allocator's
	// reserved block or live directory lookup.
	ErrDirIDUnassigned = errors.New("directory id has not been assigned")
)

// DirIDAllocator assigns preorder dir_id values and exposes the live directory
// id lookup shared by directory and file-ingest summarise operations.
type DirIDAllocator struct {
	next          uint32
	reservedDepth int
	mountSet      bool
	ids           map[*DirectoryPath]uint32
	closedFloor   *DirectoryPath
}

// NewDirIDAllocator returns a ready-to-use directory id allocator.
func NewDirIDAllocator() *DirIDAllocator {
	return &DirIDAllocator{reservedDepth: -1}
}

// SetMountPath reserves ids 0..D for the above-root chain and starts the
// descendant preorder counter at D+1.
func (a *DirIDAllocator) SetMountPath(mountPath string) error {
	if a == nil {
		return nil
	}

	depth := mountPathDepth(mountPath)

	id, err := ReservedDirIDForDepth(depth)
	if err != nil {
		return err
	}

	a.next = id + 1
	a.reservedDepth = depth
	a.mountSet = true
	a.closedFloor = nil
	clear(a.ids)

	return nil
}

func mountPathDepth(mountPath string) int {
	mountPath = strings.TrimRight(mountPath, "/")
	if mountPath == "" {
		return 0
	}

	return strings.Count(mountPath, "/")
}

// ReservedDirIDForDepth returns the reserved low-block id for a directory depth.
func ReservedDirIDForDepth(depth int) (uint32, error) {
	if depth < 0 || uint64(depth) >= uint64(ParentSentinel) {
		return 0, ErrTooManyDirs
	}

	return uint32(depth), nil
}

// Enter assigns or returns the preorder dir_id for a directory boundary.
func (a *DirIDAllocator) Enter(dir *DirectoryPath) (uint32, error) {
	if a == nil || dir == nil {
		return 0, ErrNonContiguousInput
	}

	if err := a.rejectClosedReentry(dir); err != nil {
		return 0, err
	}

	if err := a.ensureReservedFor(dir); err != nil {
		return 0, err
	}

	if dir.Depth <= a.reservedDepth {
		return ReservedDirIDForDepth(dir.Depth)
	}

	return a.enterLive(dir)
}

func (a *DirIDAllocator) enterLive(dir *DirectoryPath) (uint32, error) {
	if _, ok := a.ids[dir]; ok {
		return 0, ErrNonContiguousInput
	}

	if a.next == ParentSentinel {
		return 0, ErrTooManyDirs
	}

	id := a.next
	a.next++
	a.liveIDs()[dir] = id

	return id, nil
}

// Leave closes a directory boundary, releases its live lookup entry, and
// returns the current preorder end bound.
func (a *DirIDAllocator) Leave(dir *DirectoryPath) (uint32, error) {
	if a == nil || dir == nil || !a.mountSet {
		return 0, ErrNonContiguousInput
	}

	if dir.Depth > a.reservedDepth {
		if _, ok := a.ids[dir]; !ok {
			return 0, ErrNonContiguousInput
		}

		delete(a.ids, dir)
	}

	a.close(dir)

	return a.next, nil
}

// DirID returns the id for a reserved or currently live directory path.
func (a *DirIDAllocator) DirID(dir *DirectoryPath) (uint32, error) {
	if a == nil || dir == nil {
		return 0, ErrDirIDUnassigned
	}

	if a.mountSet && dir.Depth <= a.reservedDepth {
		return ReservedDirIDForDepth(dir.Depth)
	}

	id, ok := a.ids[dir]
	if !ok {
		return 0, ErrDirIDUnassigned
	}

	return id, nil
}

// Next returns the next unassigned preorder id.
func (a *DirIDAllocator) Next() uint32 {
	if a == nil {
		return 0
	}

	return a.next
}

func (a *DirIDAllocator) rejectClosedReentry(dir *DirectoryPath) error {
	if a.closedFloor == nil {
		return nil
	}

	if !a.closedFloor.Less(dir) || directoryPathIsAncestorOrSelf(a.closedFloor, dir) {
		return ErrNonContiguousInput
	}

	return nil
}

func directoryPathIsAncestorOrSelf(ancestor *DirectoryPath, dir *DirectoryPath) bool {
	if ancestor.Depth > dir.Depth {
		return false
	}

	for dir.Depth > ancestor.Depth {
		dir = dir.Parent
	}

	return sameDirectoryPath(ancestor, dir)
}

func (a *DirIDAllocator) ensureReservedFor(dir *DirectoryPath) error {
	if a.mountSet {
		return nil
	}

	id, err := ReservedDirIDForDepth(dir.Depth)
	if err != nil {
		return err
	}

	a.next = id + 1
	a.reservedDepth = dir.Depth
	a.mountSet = true

	return nil
}

func (a *DirIDAllocator) liveIDs() map[*DirectoryPath]uint32 {
	if a.ids == nil {
		a.ids = make(map[*DirectoryPath]uint32)
	}

	return a.ids
}

func (a *DirIDAllocator) close(dir *DirectoryPath) {
	if a.closedFloor == nil || a.closedFloor.Less(dir) {
		a.closedFloor = dir
	}
}

// ReservedParentIDForDepth returns the reserved parent id for a directory depth.
func ReservedParentIDForDepth(depth int) (uint32, error) {
	if depth == 0 {
		return ParentSentinel, nil
	}

	return ReservedDirIDForDepth(depth - 1)
}

func sameDirectoryPath(a *DirectoryPath, b *DirectoryPath) bool {
	if a == nil || b == nil {
		return a == b
	}

	return !a.Less(b) && !b.Less(a)
}
