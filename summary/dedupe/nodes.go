package dedupe

import "github.com/wtsi-hgi/wrstat-ui/summary"

var nullNode = &Node{Size: -1} //nolint:gochecknoglobals

func init() { //nolint:gochecknoinits
	nullNode.left = nullNode
	nullNode.right = nullNode
}

// Node represents a single file in the sorted tree.
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

func (n *Node) insert(e *Node) *Node {
	if n == nullNode {
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
	if n == nullNode {
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
	if n == nullNode {
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
