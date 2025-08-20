package datatree

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"slices"

	"github.com/wtsi-hgi/wrstat-ui/summary"
	"vimagination.zapto.org/byteio"
	"vimagination.zapto.org/tree"
)

func NewTree(w io.Writer) summary.OperationGenerator {
	ctx, cancel := context.WithCancelCause(context.Background())
	next := newNode(ctx)

	go func(node *Node) {
		cancel(tree.Serialise(w, node))
	}(next)

	return func() summary.Operation {
		curr := next

		next = newNode(ctx)
		curr.child = next

		return curr
	}
}

type IDData struct {
	ID uint32
	*Meta
}

type NameChild struct {
	Name  string
	Child tree.Node
}

type Node struct {
	ctx context.Context

	path  *summary.DirectoryPath
	child *Node

	yield  chan NameChild
	writer chan *byteio.StickyLittleEndianWriter

	uid, gid uint32
	mtime    int64

	users  IDMeta
	groups IDMeta
}

func newNode(ctx context.Context) *Node {
	return &Node{
		ctx:    ctx,
		yield:  make(chan NameChild),
		writer: make(chan *byteio.StickyLittleEndianWriter),
		users:  make(IDMeta),
		groups: make(IDMeta),
	}
}

type Meta struct {
	MTime uint32
	Files uint32
	Bytes uint64
}

type IDMeta map[uint32]*Meta

func (i IDMeta) Add(id uint32, t int64, size int64) {
	existing, ok := i[id]
	if !ok {
		existing = new(Meta)

		i[id] = existing
	}

	existing.MTime = max(existing.MTime, uint32(t))
	existing.Files++
	existing.Bytes += uint64(size)
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
		if err := n.sendChild(info.Name, file(info.UID, info.GID, info.MTime, info.Size)); err != nil {
			return err
		}
	}

	if !info.IsDir() {
		n.users.Add(info.UID, info.MTime, info.Size)
		n.groups.Add(info.GID, info.MTime, info.Size)
	}

	return nil
}

func (n *Node) sendChild(name []byte, child tree.Node) error {
	select {
	case <-n.ctx.Done():
		return context.Cause(n.ctx)
	case n.yield <- NameChild{Name: string(name), Child: child}:
		return nil
	}
}

func (n *Node) Output() error {
	close(n.yield)

	select {
	case <-n.ctx.Done():
		return context.Cause(n.ctx)
	case w := <-n.writer:
		writeIDTimes(w, getSortedIDTimes(n.users))
		writeIDTimes(w, getSortedIDTimes(n.groups))

		n.writer <- nil

		if w.Err != nil {
			return w.Err
		}
	}

	var err error

	if n.path.Parent == nil {
		<-n.ctx.Done()

		if err = context.Cause(n.ctx); errors.Is(err, context.Canceled) {
			err = nil
		}
	}

	n.path = nil
	clear(n.users)
	clear(n.groups)

	return err
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

func getSortedIDTimes(idt IDMeta) []IDData {
	var idts []IDData

	for id, meta := range idt {
		it := IDData{id, meta}

		idx, _ := slices.BinarySearchFunc(idts, it, func(a, b IDData) int {
			return int(a.ID) - int(b.ID)
		})

		idts = slices.Insert(idts, idx, it)
	}

	return idts
}

func writeIDTimes(w *byteio.StickyLittleEndianWriter, idts []IDData) {
	w.WriteUintX(uint64(len(idts)))

	for _, idt := range idts {
		w.WriteUint32(idt.ID)
		w.WriteUint32(idt.MTime)
		w.WriteUint32(idt.Files)
		w.WriteUint64(idt.Bytes)
	}
}

func file(uid, gid uint32, mtime, size int64) tree.Leaf {
	var buf bytes.Buffer

	w := byteio.StickyLittleEndianWriter{Writer: &buf}

	writeFile(&w, uid, gid, mtime, size)

	return tree.Leaf(buf.Bytes())
}

func writeFile(w *byteio.StickyLittleEndianWriter, uid, gid uint32, mtime, size int64) {
	w.WriteUint32(uid)
	w.WriteUint32(gid)
	w.WriteUint32(uint32(mtime))
	w.WriteUint64(uint64(size))
}
