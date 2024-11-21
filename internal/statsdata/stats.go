package statsdata

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"

	_ "embed"
)

//go:embed test.stats.gz
var testStats string

func TestStats(width, depth int, rootPath string, refTime int64) *Directory {
	d := NewRoot(rootPath, refTime)

	addChildren(d, width, depth)

	return d
}

func addChildren(d *Directory, width, depth int) {
	for n := range width {
		addChildren(d.AddDirectory(fmt.Sprintf("dir%d", n)), width-1, depth-1)
		d.AddFile(fmt.Sprintf("file%d", n)).Size = 1
	}
}

var ErrNotEnoughGroups = errors.New("not enough groups")

type Directory struct {
	children map[string]io.WriterTo
	File
}

func NewRoot(path string, refTime int64) *Directory {
	return &Directory{
		children: make(map[string]io.WriterTo),
		File: File{
			Path:  path,
			Size:  4096,
			ATime: refTime,
			MTime: refTime,
			CTime: refTime,
			Type:  'd',
		},
	}
}

func (d *Directory) AddDirectory(name string) *Directory {
	if c, ok := d.children[name]; ok {
		if cd, ok := c.(*Directory); ok {
			return cd
		} else {
			return nil
		}
	}

	c := &Directory{
		children: make(map[string]io.WriterTo),
		File:     d.File,
	}

	c.File.Path += name + "/"
	d.children[name] = c

	return c
}

func (d *Directory) AddFile(name string) *File {
	if c, ok := d.children[name]; ok {
		if cf, ok := c.(*File); ok {
			return cf
		} else {
			return nil
		}
	}

	f := d.File

	d.children[name] = &f
	f.Path += name
	f.Size = 0
	f.Type = 'f'

	return &f
}

func (d *Directory) WriteTo(w io.Writer) (int64, error) {
	n, err := d.File.WriteTo(w)
	if err != nil {
		return int64(n), err
	}

	keys := slices.Collect(maps.Keys(d.children))
	sort.Strings(keys)

	for _, k := range keys {
		m, err := d.children[k].WriteTo(w)

		n += m

		if err != nil {
			return int64(n), err
		}
	}

	return int64(n), nil
}

func (d *Directory) AsReader() io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		d.WriteTo(pw)
		pw.Close()
	}()

	return pr
}

type File struct {
	Path                string
	Size                int64
	ATime, MTime, CTime int64
	UID, GID            int
	Type                byte
}

func (f *File) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t1\t1\t1\n",
		f.Path, f.Size, f.UID, f.GID, f.ATime, f.MTime, f.CTime, f.Type)

	return int64(n), err
}
