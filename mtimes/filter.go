package mtimes

import (
	"bytes"
	"io"
	"iter"
	"slices"
	"strings"
	"unsafe"

	"vimagination.zapto.org/byteio"
	"vimagination.zapto.org/tree"
)

type Tree struct {
	Name        string
	ChildTrees  []*Tree
	LatestMTime int64
}

type Filter interface {
	Filter(path string, meta Meta) bool
}

var slash = []byte{'/'}

func FilterTree(t tree.Node, filter Filter) (*Tree, error) {
	var buf bytes.Buffer

	return filterTree(append(make([]byte, 0, 4100), '/'), t, filter, &buf)
}

func filterTree(path []byte, t tree.Node, filter Filter, buf *bytes.Buffer) (*Tree, error) {
	_, err := t.WriteTo(buf)
	if err != nil {
		return nil, err
	}

	var m Meta

	if bytes.HasSuffix(path, slash) {
		m = readDirMeta(&byteio.StickyLittleEndianReader{Reader: buf})
	} else {
		m = readFileMeta(&byteio.StickyLittleEndianReader{Reader: buf})
	}

	buf.Reset()

	if !filter.Filter(unsafe.String(unsafe.SliceData(path), len(path)), m) {
		return nil, nil
	}

	tree := &Tree{Name: "/"}

	if !bytes.HasSuffix(path, slash) {
		tree.LatestMTime = m.UIDs[0].Time
	}

	if err = tree.addChildren(path, t, filter, buf); err != nil {
		return nil, err
	}

	return tree, nil
}

func (tree *Tree) addChildren(path []byte, t tree.Node, filter Filter, buf *bytes.Buffer) error {
	for name, child := range t.Children() {
		childPath := append(path, name...)

		c, err := filterTree(childPath, child, filter, buf)
		if err != nil {
			return err
		}

		if c != nil {
			tree.ChildTrees = append(tree.ChildTrees, c)
			c.Name = name
			tree.LatestMTime = max(tree.LatestMTime, c.LatestMTime)
		}
	}

	return nil
}

func (t *Tree) Children() iter.Seq2[string, tree.Node] {
	return func(yield func(string, tree.Node) bool) {
		for _, child := range t.ChildTrees {
			if !yield(child.Name, child) {
				break
			}
		}
	}
}

func (t *Tree) WriteTo(w io.Writer) (int64, error) {
	lw := byteio.StickyBigEndianWriter{Writer: w}

	if strings.HasSuffix(t.Name, "/") {
		lw.WriteUintX(1)
		lw.WriteUint32(0)
		lw.WriteUint32(uint32(t.LatestMTime))
		lw.WriteUintX(1)
	}

	lw.WriteUint32(0)
	lw.WriteUint32(uint32(t.LatestMTime))

	return lw.Count, lw.Err
}

type PathTime struct {
	Path  string
	MTime int64
}

func (t *Tree) ListFiles() []PathTime {
	var pt []PathTime

	t.getFiles(nil, &pt)

	return pt
}

func (t *Tree) getFiles(path []byte, pt *[]PathTime) {
	path = append(path, t.Name...)

	if !t.IsDir() {
		*pt = append(*pt, PathTime{
			Path:  string(path),
			MTime: t.LatestMTime,
		})

		return
	}

	for _, child := range t.ChildTrees {
		child.getFiles(path, pt)
	}
}

func (t *Tree) IsDir() bool {
	return strings.HasSuffix(t.Name, "/")
}

func (t *Tree) DataLocations(minDepth int) []PathTime {
	var pt []PathTime

	t.getDirs(nil, &pt, minDepth)

	return pt
}

func (t *Tree) getDirs(path []byte, pt *[]PathTime, minDepth int) {
	path = append(path, t.Name...)

	if minDepth <= 0 && (len(t.ChildTrees) > 1 || len(t.ChildTrees) == 1 && !t.ChildTrees[0].IsDir()) {
		*pt = append(*pt, PathTime{
			Path:  string(path),
			MTime: t.LatestMTime,
		})

		return
	}

	minDepth--

	for _, child := range t.ChildTrees {
		if !child.IsDir() {
			continue
		}

		child.getDirs(path, pt, minDepth)
	}
}

type And []Filter

func (a And) Filter(path string, meta Meta) bool {
	for _, filter := range a {
		if !filter.Filter(path, meta) {
			return false
		}
	}

	return true
}

type Or []Filter

func (o Or) Filter(path string, meta Meta) bool {
	for _, filter := range o {
		if filter.Filter(path, meta) {
			return true
		}
	}

	return false
}

type GID uint32

func (g GID) Filter(path string, meta Meta) bool {
	_, ok := slices.BinarySearchFunc(meta.GIDs, IDTime{ID: uint32(g)}, IDTime.compare)

	return ok
}

type UID uint32

func (u UID) Filter(path string, meta Meta) bool {
	_, ok := slices.BinarySearchFunc(meta.UIDs, IDTime{ID: uint32(u)}, IDTime.compare)

	return ok
}

type Prefix string

func (p Prefix) Filter(path string, meta Meta) bool {
	return strings.HasPrefix(path, string(p)) || strings.HasSuffix(path, "/") && strings.HasPrefix(string(p), path)
}

type ExcludePrefix string

func (e ExcludePrefix) Filter(path string, meta Meta) bool {
	return !strings.HasPrefix(path, string(e))
}

type FileWithSuffixOrDir string

func (f FileWithSuffixOrDir) Filter(path string, meta Meta) bool {
	return strings.HasSuffix(path, "/") || strings.HasSuffix(path, string(f))
}
