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
	"fmt"
	"io"
)

const minNodeGroups = 2

func outputNodes(output io.Writer, nodes [][]*Node) error {
	var buffer [4096]byte

	if len(nodes) < minNodeGroups {
		return nil
	}

	if _, err := fmt.Fprintf(output, "Size: %d\n", nodes[0][0].Size); err != nil {
		return err
	}

	for _, hardlinks := range nodes {
		if err := outputHardlinks(output, hardlinks, &buffer); err != nil {
			return err
		}
	}

	return nil
}

func outputHardlinks(output io.Writer, hardlinks []*Node, buffer *[4096]byte) error {
	if err := outputNode(output, hardlinks[0], buffer); err != nil {
		return err
	}

	for _, node := range hardlinks[1:] {
		if _, err := io.WriteString(output, "\t"); err != nil {
			return err
		}

		if err := outputNode(output, node, buffer); err != nil {
			return err
		}
	}

	return nil
}

func outputNode(output io.Writer, node *Node, buffer *[4096]byte) error {
	_, err := fmt.Fprintf(output, "%q\n", append(node.Path.AppendTo(buffer[:0]), node.Name...))

	return err
}

// Print writes sized matched files to the output Writer. If all of the files in
// a group are hardlinks of each other, nothing is printed.
//
// Each group of matched files will print a small header in the following
// format:
//
// Size: %d
//
// …followed by a list of the filenames of the matching files.
//
// Hardlinks to previously printed files are prefixed with a tab character.
func (d *Deduper) Print(output io.Writer) error { //nolint:gocognit
	var (
		lastSize       int64 = -1
		lastMountPoint int32 = -1
		lastInode      int64 = -1

		matches [][]*Node
	)

	for node := range d.Iter {
		if node.Size != lastSize {
			if err := outputNodes(output, matches); err != nil {
				return err
			}

			lastSize = node.Size
			lastMountPoint = -1
			lastInode = -1
			matches = nil
		}

		if node.Mountpoint != lastMountPoint || node.Inode != lastInode {
			matches = append(matches, []*Node{node})
			lastMountPoint = node.Mountpoint
			lastInode = node.Inode
		} else {
			matches[len(matches)-1] = append(matches[len(matches)-1], node)
		}
	}

	return outputNodes(output, matches)
}
