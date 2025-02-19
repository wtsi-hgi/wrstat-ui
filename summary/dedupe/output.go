package dedupe

import (
	"fmt"
	"io"
)

const minNodeGroups = 2

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

func outputNodes(output io.Writer, nodes [][]*Node) error {
	if len(nodes) < minNodeGroups {
		return nil
	}

	if _, err := fmt.Fprintf(output, "Size: %d\n", nodes[0][0].Size); err != nil {
		return err
	}

	for _, hardlinks := range nodes {
		if err := outputHardlinks(output, hardlinks); err != nil {
			return err
		}
	}

	return nil
}

func outputHardlinks(output io.Writer, hardlinks []*Node) error {
	if err := outputNode(output, hardlinks[0]); err != nil {
		return err
	}

	for _, node := range hardlinks[1:] {
		if _, err := io.WriteString(output, "\t"); err != nil {
			return err
		}

		if err := outputNode(output, node); err != nil {
			return err
		}
	}

	return nil
}

var buffer [4096]byte

func outputNode(output io.Writer, node *Node) error {
	_, err := fmt.Fprintf(output, "%q\n", append(node.Path.AppendTo(buffer[:0]), node.Name...))

	return err
}
