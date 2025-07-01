/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package statsdata

import (
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// TestStats creates a new Directory filled with pseudo content.
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

// Directory represents the stat information for a directory and its children.
type Directory struct {
	children map[string]io.WriterTo
	File
}

// NewRoot creates a new Directory root with the specified time as the atime,
// mtime, and ctime.
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

// AddDirectory either creates and returns a new directory in the direcory or
// returns an existing one.
func (d *Directory) AddDirectory(name string) *Directory {
	if c, ok := d.children[name]; ok {
		if cd, ok := c.(*Directory); ok {
			return cd
		}

		return nil
	}

	c := &Directory{
		children: make(map[string]io.WriterTo),
		File:     d.File,
	}

	c.File.Path += name + "/"
	d.children[name] = c

	return c
}

// AddFile either creates and returns a new file in the direcory or returns an
// existing one.
func (d *Directory) AddFile(name string) *File {
	if c, ok := d.children[name]; ok {
		if cf, ok := c.(*File); ok {
			return cf
		}

		return nil
	}

	f := d.File

	d.children[name] = &f
	f.Path += name
	f.Size = 0
	f.Type = 'f'

	return &f
}

// WriteTo writes the stats data for the directory.
func (d *Directory) WriteTo(w io.Writer) (int64, error) {
	n, err := d.File.WriteTo(w)
	if err != nil {
		return n, err
	}

	keys := slices.Collect(maps.Keys(d.children))
	sort.Strings(keys)

	for _, k := range keys {
		m, err := d.children[k].WriteTo(w)

		n += m

		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// AsReader returns a ReadCloser that will output a stats file as read by the
// stats package.
func (d *Directory) AsReader() io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		d.WriteTo(pw) //nolint:errcheck
		pw.Close()
	}()

	return pr
}

// File represents a pseudo-file entry.
type File struct {
	Path                string
	Size                int64
	ATime, MTime, CTime int64
	UID, GID            uint32
	Inode               uint64
	Type                byte
}

// WriteTo writes the stats data for a file entry.
func (f *File) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "%q\t%d\t%d\t%d\t%d\t%d\t%d\t%c\t%d\t1\t1\t%d\n",
		f.Path, f.Size, f.UID, f.GID, f.ATime, f.MTime, f.CTime, f.Type, f.Inode, f.Size)

	return int64(n), err
}

// AddFile adds file data to a directory, creating the directory in the tree if
// necessary.
func AddFile(d *Directory, path string, uid, gid uint32, size, atime, mtime int64) *File {
	for part := range strings.SplitSeq(filepath.Dir(path), "/") {
		d = d.AddDirectory(part)
	}

	file := d.AddFile(filepath.Base(path))
	file.UID = uid
	file.GID = gid
	file.Size = size
	file.ATime = atime
	file.MTime = mtime

	return file
}
