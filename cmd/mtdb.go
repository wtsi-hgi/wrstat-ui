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
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/mtimes"
)

var mtDBCmd = &cobra.Command{
	Use:   "mtdb",
	Short: "generate an mtime tree database",
	Long: `generate an mtime tree database to be queried with the mtquery subcommand

	
Input files can either be specified directly, with the paths to the stats.gz
files created by wrstat, or a directory can be specified that will be searched
for stats.gz files in the same manner as the server and watch commands.

Also requires an output file, specified with the --output/-o flag.
	`,
	Run: func(_ *cobra.Command, args []string) {
		if output == "" {
			die("require --output file")
		}

		files, err := parseFiles(args)
		if err != nil {
			die("%s", err)
		}

		if err := mtimes.Build(files, output); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(mtDBCmd)

	mtDBCmd.Flags().StringVarP(&output, "output", "o", "-", "file to output mtime tree")
}
