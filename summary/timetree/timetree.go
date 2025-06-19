package timetree

import (
	"bytes"
	"context"
	"io"
	"iter"
	"slices"

	"github.com/wtsi-hgi/wrstat-ui/summary"
	"vimagination.zapto.org/byteio"
	"vimagination.zapto.org/tree"
)

func NewTimeTree(w io.Writer, complete chan struct{}) summary.OperationGenerator {
	ctx, cancel := context.WithCancelCause(context.Background())
	next := newNode(ctx)

	go func(node *Node) {
		cancel(tree.Serialise(w, node))
		close(complete)
	}(next)

	return func() summary.Operation {
		curr := next

		next = newNode(ctx)
		curr.child = next

		return curr
	}
}

type IDTime struct {
	ID, Time uint32
}

type NameChild struct {
	Name  string
	Child tree.Node
}

type Node struct {
	ctx context.Context

	path, currChild *summary.DirectoryPath
	child           *Node

	yield  chan NameChild
	writer chan *byteio.StickyLittleEndianWriter

	uid, gid uint32
	mtime    int64

	users  IDTimes
	groups IDTimes
}

func newNode(ctx context.Context) *Node {
	return &Node{
		ctx:    ctx,
		yield:  make(chan NameChild),
		writer: make(chan *byteio.StickyLittleEndianWriter),
		users:  make(IDTimes),
		groups: make(IDTimes),
	}
}

type IDTimes map[uint32]uint32

func (i IDTimes) Add(id uint32, t int64) {
	i[id] = max(i[id], uint32(t))
}

func (n *Node) Add(info *summary.FileInfo) error {
	if n.path == nil {
		n.path = info.Path
		n.uid = info.UID
		n.gid = info.GID
		n.mtime = info.MTime
	} else if info.Path.Parent == n.path && info.IsDir() {
		if err := n.sendChild(info.Name, n.child); err != nil {
			return err
		}
	} else if info.Path == n.path {
		if err := n.sendChild(info.Name, file(info.UID, info.GID, info.MTime)); err != nil {
			return err
		}
	}

	if !info.IsDir() {
		n.users.Add(info.UID, info.MTime)
		n.groups.Add(info.GID, info.MTime)
	}

	return nil
}

func (n *Node) sendChild(name []byte, child tree.Node) error {
	select {
	case <-n.ctx.Done():
		return n.ctx.Err()
	case n.yield <- NameChild{Name: string(name), Child: child}:
		return nil
	}
}

func (n *Node) Output() error {
	close(n.yield)

	select {
	case <-n.ctx.Done():
		return n.ctx.Err()
	case w := <-n.writer:
		writeIDTimes(w, getSortedIDTimes(n.users))
		writeIDTimes(w, getSortedIDTimes(n.groups))

		n.writer <- nil

		if w.Err != nil {
			return w.Err
		}
	}

	n.path = nil
	clear(n.users)
	clear(n.groups)

	return nil
}

func (n *Node) Children() iter.Seq2[string, tree.Node] {
	return func(yield func(string, tree.Node) bool) {
		for nc := range n.yield {
			if !yield(nc.Name, nc.Child) {
				return
			}
		}

		n.yield = make(chan NameChild)
	}
}

func (n *Node) WriteTo(w io.Writer) (int64, error) {
	lw := &byteio.StickyLittleEndianWriter{Writer: w}

	n.writer <- lw
	<-n.writer

	return lw.Count, lw.Err
}

func getSortedIDTimes(idt IDTimes) []IDTime {
	var idts []IDTime

	for id, mtime := range idt {
		it := IDTime{id, mtime}

		idx, _ := slices.BinarySearchFunc(idts, it, func(a, b IDTime) int {
			return int(a.ID) - int(b.ID)
		})

		idts = slices.Insert(idts, idx, it)
	}

	return idts
}

func writeIDTimes(w *byteio.StickyLittleEndianWriter, idts []IDTime) {
	w.WriteUintX(uint64(len(idts)))

	for _, idt := range idts {
		w.WriteUint32(idt.ID)
		w.WriteUint32(idt.Time)
	}
}

func file(uid, gid uint32, mtime int64) tree.Leaf {
	var buf bytes.Buffer

	w := byteio.StickyLittleEndianWriter{Writer: &buf}

	writeFile(&w, uid, gid, mtime)

	return tree.Leaf(buf.Bytes())
}

func writeFile(w *byteio.StickyLittleEndianWriter, uid, gid uint32, mtime int64) {
	w.WriteUint32(uid)
	w.WriteUint32(gid)
	w.WriteUint32(uint32(mtime))
}
