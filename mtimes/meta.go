package mtimes

import (
	"bytes"
	"strings"

	"vimagination.zapto.org/byteio"
	"vimagination.zapto.org/tree"
)

func GetMeta(t tree.Node, path string) (Meta, error) {
	childFn := otherChild

	switch at := t.(type) {
	case *tree.MemTree:
		childFn = memChild
	case *tree.Tree:
		childFn = readerChild
	case *tree.TreeCloser:
		t = &at.Tree
		childFn = readerChild
	}

	return getMeta(t, strings.TrimPrefix(path, "/"), childFn)
}

type childFn func(tree.Node, string) (tree.Node, error)

func memChild(t tree.Node, name string) (tree.Node, error) {
	return t.(*tree.MemTree).Child(name)
}

func readerChild(t tree.Node, name string) (tree.Node, error) {
	return t.(*tree.Tree).Child(name)
}

func otherChild(t tree.Node, name string) (tree.Node, error) {
	for child, n := range t.Children() {
		if child == name {
			return n, nil
		}
	}

	return nil, tree.ChildNotFoundError(name)
}

func getMeta(t tree.Node, name string, childFn childFn) (Meta, error) {
	child, err := getChild(t, name, childFn)
	if err != nil {
		return Meta{}, err
	}

	var buf bytes.Buffer

	if _, err = child.WriteTo(&buf); err != nil {
		return Meta{}, err
	}

	if strings.HasSuffix(name, "/") {
		return readDirMeta(&byteio.StickyLittleEndianReader{Reader: &buf}), nil
	}

	return readFileMeta(&byteio.StickyLittleEndianReader{Reader: &buf}), nil
}

func getChild(t tree.Node, name string, childFn childFn) (tree.Node, error) {
	var err error
	for name != "" {
		pos := strings.IndexByte(name, '/')
		if pos == -1 {
			pos = len(name)
		} else {
			pos++
		}

		if t, err = childFn(t, name[:pos]); err != nil {
			return nil, err
		}

		name = name[pos:]
	}

	return t, nil
}

type Meta struct {
	UIDs, GIDs []IDTime
}

func readFileMeta(lr *byteio.StickyLittleEndianReader) Meta {
	uid := lr.ReadUint32()
	gid := lr.ReadUint32()
	mtime := int64(lr.ReadUint32())

	return Meta{
		UIDs: []IDTime{{uid, mtime}},
		GIDs: []IDTime{{gid, mtime}},
	}
}

func readDirMeta(lr *byteio.StickyLittleEndianReader) Meta {
	return Meta{
		UIDs: readArray(lr),
		GIDs: readArray(lr),
	}
}

type IDTime struct {
	ID   uint32
	Time int64
}

func readArray(lr *byteio.StickyLittleEndianReader) []IDTime {
	idts := make([]IDTime, lr.ReadUintX())

	for n := range idts {
		idts[n].ID = lr.ReadUint32()
		idts[n].Time = int64(lr.ReadUint32())
	}

	return idts
}
