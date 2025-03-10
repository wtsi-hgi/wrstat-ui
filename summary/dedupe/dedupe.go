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

// Deduper represents a summary Operation that can be used to match files by
// size.
type Deduper struct {
	mountpoint  int32
	MinFileSize int64
	node        *Node
}

// Iter implements the iter.Seq interface, interating over each file in the
// sorted list.
func (d *Deduper) Iter(yield func(*Node) bool) {
	d.node.iter(yield)
}

// Operation provides a summary.OperationGenerator to be used with a
// summary.Summariser.
func (d *Deduper) Operation() summary.OperationGenerator {
	d.node = nullNode
	d.mountpoint = -1

	return func() summary.Operation {
		d.mountpoint++

		return d
	}
}

// Add is used to implements summary.Operation.
func (d *Deduper) Add(info *summary.FileInfo) error {
	if info.Size >= d.MinFileSize && !info.IsDir() {
		d.node = d.node.insert(&Node{
			left:       nullNode,
			right:      nullNode,
			Mountpoint: d.mountpoint,
			Path:       info.Path,
			Name:       string(info.Name),
			Size:       info.ASize,
			Inode:      info.Inode,
		})
	}

	return nil
}

// Output is used to implements summary.Operation.
func (d *Deduper) Output() error {
	return nil
}
