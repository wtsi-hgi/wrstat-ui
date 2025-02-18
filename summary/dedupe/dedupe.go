package dedupe

import (
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	nullDirEnt = new(Node) //nolint:gochecknoglobals
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

	return func() summary.Operation {
		d.mountpoint++

		return d
	}
}

func (d *Deduper) Add(info *summary.FileInfo) error {
	if info.Size >= d.MinFileSize && !info.IsDir() {
		d.node = d.node.insert(&Node{
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

	switch n.compare(e) {
	case 1:
		n.left = n.left.insert(e)
	case -1:
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

	if !yield(n) {
		return false
	}

	return n.right.iter(yield)
}
