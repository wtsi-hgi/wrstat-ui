/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package dedupe

import (
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	nullDirEnt = &Node{Size: -1} //nolint:gochecknoglobals
)

func init() {
	nullDirEnt.left = nullDirEnt
	nullDirEnt.right = nullDirEnt
}

type Deduper struct {
	mountpoint  int32
	MinFileSize int64
	node        *Node
}

func (d *Deduper) Iter(yield func(*Node) bool) {
	d.node.iter(yield)
}

func (d *Deduper) Operation() summary.OperationGenerator {
	d.node = nullDirEnt
	d.mountpoint = -1

	return func() summary.Operation {
		d.mountpoint++

		return d
	}
}

func (d *Deduper) Add(info *summary.FileInfo) error {
	if info.Size >= d.MinFileSize && !info.IsDir() {
		d.node = d.node.insert(&Node{
			left:       nullDirEnt,
			right:      nullDirEnt,
			Mountpoint: d.mountpoint,
			Path:       info.Path,
			Name:       string(info.Name),
			Size:       info.Size,
			Inode:      info.Inode,
		})
	}

	return nil
}

func (d *Deduper) Output() error {
	return nil
}

type Node struct {
	left, right *Node
	depth       int32
	Mountpoint  int32
	Path        *summary.DirectoryPath
	Name        string
	Size        int64
	Inode       int64
}

func (n *Node) compare(e *Node) int64 {
	if n.Size == e.Size {
		if n.Mountpoint == e.Mountpoint {
			return n.Inode - e.Inode
		}

		return int64(n.Mountpoint) - int64(e.Mountpoint)
	}

	return n.Size - e.Size
}

func (n *Node) insert(e *Node) *Node { //nolint:gocyclo
	if n == nullDirEnt {
		return e
	}

	if n.compare(e) > 0 {
		n.left = n.left.insert(e)
	} else {
		n.right = n.right.insert(e)
	}

	n.setDepth()

	switch n.left.depth - n.right.depth {
	case -2:
		if n.right.left.depth > n.right.right.depth {
			n.right = n.right.rotateRight()
		}

		return n.rotateLeft()
	case 2: //nolint:mnd
		if n.left.right.depth > n.left.left.depth {
			n.left = n.left.rotateLeft()
		}

		return n.rotateRight()
	}

	return n
}

func (n *Node) setDepth() {
	if n == nullDirEnt {
		return
	}

	if n.left.depth > n.right.depth {
		n.depth = n.left.depth + 1
	} else {
		n.depth = n.right.depth + 1
	}
}

func (n *Node) rotateLeft() *Node {
	r := n.right
	n.right = r.left
	r.left = n

	n.setDepth()
	r.setDepth()

	return r
}

func (n *Node) rotateRight() *Node {
	l := n.left
	n.left = l.right
	l.right = n

	n.setDepth()
	l.setDepth()

	return l
}

func (n *Node) iter(yield func(*Node) bool) bool {
	if n == nullDirEnt {
		return true
	}

	if !n.left.iter(yield) {
		return false
	}

	right := n.right
	n.left = nil
	n.right = nil
	n.depth = 0

	if !yield(n) {
		return false
	}

	return right.iter(yield)
}
