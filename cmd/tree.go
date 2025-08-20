/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"os"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/datatree"
)

var treeOut string

var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "Create a tree database",
	Long:  ``,
	RunE: func(_ *cobra.Command, args []string) error {
		statsFiles, err := parseFiles(args)
		if err != nil {
			return err
		}

		r, err := combineStatsFiles(statsFiles)
		if err != nil {
			return err
		}

		s := summary.NewSummariser(stats.NewStatsParser(r))

		f, err := os.Create(treeOut)
		if err != nil {
			return err
		}

		s.AddDirectoryOperation(datatree.NewTree(f))

		if err := s.Summarise(); err != nil {
			return err
		}

		return f.Close()
	},
}

func init() {
	treeCmd.Flags().StringVarP(&treeOut, "tree", "t", "",
		"path to store tree representaion of provided wrstat files")

	RootCmd.AddCommand(treeCmd)
}
