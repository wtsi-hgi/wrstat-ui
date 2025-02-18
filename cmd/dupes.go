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
package cmd

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/server"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dedupe"
)

const inputStatsFile = "stats.gz"

var (
	minSize int64
	output  string
)

type statsFile struct {
	io.Reader
}

var dupescmd = &cobra.Command{
	Use:   "dupes",
	Short: "dupes",
	Long:  ``,
	Run: func(_ *cobra.Command, args []string) {
		files, err := parseFiles(args)
		if err != nil {
			die("%s", err)
		}

		if err = findDupes(files, minSize, output); err != nil {
			die("%s", err)
		}
	},
}

func parseFiles(args []string) ([]string, error) {
	var files []string

	if len(args) == 1 {
		fi, err := os.Stat(args[0])
		if err != nil {
			return nil, err
		}

		if fi.IsDir() {
			dirs, err := server.FindDBDirs(args[0], inputStatsFile)
			if err != nil {
				return nil, err
			}

			files = make([]string, len(dirs))

			for n, dir := range dirs {
				files[n] = filepath.Join(dir, inputStatsFile)
			}
		} else {
			files = args
		}
	} else {
		files = args
	}

	return files, nil
}

func findDupes(files []string, minSize int64, output string) error {
	var sf statsFile

	deduper := dedupe.Deduper{MinFileSize: minSize}

	s := summary.NewSummariser(stats.NewStatsParser(&sf))
	s.AddGlobalOperation(deduper.Operation())

	for _, stats := range files {
		f, err := os.Open(stats)
		if err != nil {
			return err
		}

		sf.Reader = f

		if strings.HasSuffix(stats, ".gz") {
			if sf.Reader, err = pgzip.NewReader(f); err != nil {
				return err
			}
		}

		if err = s.Summarise(); err != nil {
			return err
		}

		f.Close()
	}

	return outputDupes(output, deduper.Iter)
}

func outputDupes(output string, nodes iter.Seq[*dedupe.Node]) (err error) {
	var w io.Writer

	if output == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(output)
		if err != nil {
			return err
		}

		defer func() {
			if errr := f.Close(); err == nil {
				err = errr
			}
		}()

		w = f
	}

	b := bufio.NewWriter(w)

	if err = processNodes(b, nodes); err != nil {
		return err
	}

	err = b.Flush()

	return err
}

func processNodes(output io.Writer, nodes iter.Seq[*dedupe.Node]) error {
	var (
		lastSize       int64 = -1
		lastMountPoint int32 = -1
		lastInode      int64 = -1

		matches [][]*dedupe.Node
	)

	for node := range nodes {
		if node.Size != lastSize {
			if err := outputNodes(output, matches); err != nil {
				return err
			}

			lastSize = node.Size
			lastMountPoint = -1
			lastInode = -1
		}

		if node.Mountpoint != lastMountPoint || node.Inode != lastInode {
			matches = append(matches, []*dedupe.Node{node})
			lastMountPoint = node.Mountpoint
			lastInode = node.Inode
		} else {
			matches[len(matches)-1] = append(matches[len(matches)-1], node)
		}
	}

	return outputNodes(output, matches)
}

func outputNodes(output io.Writer, nodes [][]*dedupe.Node) error {
	if len(nodes) < 2 {
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

func outputHardlinks(output io.Writer, hardlinks []*dedupe.Node) error {
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

func outputNode(output io.Writer, node *dedupe.Node) error {
	_, err := fmt.Fprintf(output, "%q\n", append(node.Path.AppendTo(buffer[:0]), node.Name...))

	return err
}

func init() {
	RootCmd.AddCommand(dupescmd)

	dupescmd.Flags().Int64VarP(&minSize, "minsize", "m", 0, "minimum file size to consider a possible duplicate")
	dupescmd.Flags().StringVarP(&output, "minsize", "o", "-", "file to output possible duplicate file data")
}
