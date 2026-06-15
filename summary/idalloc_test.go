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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type idAllocatorTestPaths map[string]*DirectoryPath

func newIDAllocatorTestPaths() idAllocatorTestPaths {
	return idAllocatorTestPaths{
		"/": {Name: "/", Depth: 0},
	}
}

func (p idAllocatorTestPaths) path(path string) *DirectoryPath {
	if dir, ok := p[path]; ok {
		return dir
	}

	parentPath, name := splitIDAllocatorTestPath(path)
	parent := p.path(parentPath)
	dir := &DirectoryPath{
		Name:   name,
		Depth:  parent.Depth + 1,
		Parent: parent,
	}
	p[path] = dir

	return dir
}

func splitIDAllocatorTestPath(path string) (string, string) {
	for i := len(path) - 2; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i+1], path[i+1:]
		}
	}

	return "/", path
}

func TestDirIDAllocator(t *testing.T) {
	Convey("B5 reserves low ids for the mount chain and starts descendants after it", t, func() {
		paths := newIDAllocatorTestPaths()
		root := paths.path("/")
		lustre := paths.path("/lustre/")
		scratch := paths.path("/lustre/scratch125/")
		mount := paths.path("/lustre/scratch125/teamX/")
		alpha := paths.path("/lustre/scratch125/teamX/alpha/")

		alloc := NewDirIDAllocator()
		So(alloc.SetMountPath("/lustre/scratch125/teamX/"), ShouldBeNil)

		So(mustDirID(alloc, root), ShouldEqual, uint32(0))
		So(mustDirID(alloc, lustre), ShouldEqual, uint32(1))
		So(mustDirID(alloc, scratch), ShouldEqual, uint32(2))
		So(mustDirID(alloc, mount), ShouldEqual, uint32(3))

		mountID, err := alloc.Enter(mount)
		So(err, ShouldBeNil)
		So(mountID, ShouldEqual, uint32(3))
		So(alloc.Next(), ShouldEqual, uint32(4))

		alphaID, err := alloc.Enter(alpha)
		So(err, ShouldBeNil)
		So(alphaID, ShouldEqual, uint32(4))
		So(alloc.Next(), ShouldEqual, uint32(5))
		So(mustDirID(alloc, alpha), ShouldEqual, uint32(4))

		subtreeEnd, err := alloc.Leave(alpha)
		So(err, ShouldBeNil)
		So(subtreeEnd, ShouldEqual, uint32(5))

		_, err = alloc.DirID(alpha)
		So(errors.Is(err, ErrDirIDUnassigned), ShouldBeTrue)
		So(mustDirID(alloc, mount), ShouldEqual, uint32(3))
	})

	Convey("B5 live lookup is keyed by *DirectoryPath", t, func() {
		paths := newIDAllocatorTestPaths()
		mount := paths.path("/m/teamX/")
		alpha := paths.path("/m/teamX/alpha/")
		alphaSamePath := &DirectoryPath{Name: alpha.Name, Depth: alpha.Depth, Parent: alpha.Parent}

		alloc := NewDirIDAllocator()
		So(alloc.SetMountPath("/m/teamX/"), ShouldBeNil)

		_, err := alloc.Enter(mount)
		So(err, ShouldBeNil)
		alphaID, err := alloc.Enter(alpha)
		So(err, ShouldBeNil)
		So(alphaID, ShouldEqual, uint32(3))

		So(mustDirID(alloc, alpha), ShouldEqual, alphaID)
		_, err = alloc.DirID(alphaSamePath)
		So(errors.Is(err, ErrDirIDUnassigned), ShouldBeTrue)
	})

	Convey("B5 returns ErrTooManyDirs before assigning the parent sentinel", t, func() {
		paths := newIDAllocatorTestPaths()
		overflow := paths.path("/overflow/")

		alloc := NewDirIDAllocator()
		alloc.mountSet = true
		alloc.reservedDepth = 0
		alloc.next = ParentSentinel

		_, err := alloc.Enter(overflow)
		So(errors.Is(err, ErrTooManyDirs), ShouldBeTrue)
		_, err = alloc.DirID(overflow)
		So(errors.Is(err, ErrDirIDUnassigned), ShouldBeTrue)
	})

	Convey("B5 returns ErrNonContiguousInput on a re-entered closed directory", t, func() {
		paths := newIDAllocatorTestPaths()
		mount := paths.path("/m/")
		alpha := paths.path("/m/alpha/")
		alphaAgain := &DirectoryPath{Name: alpha.Name, Depth: alpha.Depth, Parent: alpha.Parent}

		alloc := NewDirIDAllocator()
		So(alloc.SetMountPath("/m/"), ShouldBeNil)

		_, err := alloc.Enter(mount)
		So(err, ShouldBeNil)
		_, err = alloc.Enter(alpha)
		So(err, ShouldBeNil)
		_, err = alloc.Leave(alpha)
		So(err, ShouldBeNil)

		_, err = alloc.Enter(alphaAgain)
		So(errors.Is(err, ErrNonContiguousInput), ShouldBeTrue)
	})
}

func mustDirID(alloc *DirIDAllocator, dir *DirectoryPath) uint32 {
	id, err := alloc.DirID(dir)
	So(err, ShouldBeNil)

	return id
}
