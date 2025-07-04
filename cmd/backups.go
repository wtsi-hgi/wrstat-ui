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
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/backups"
)

var (
	backupCSV   string
	reportRoot  string
	roots       []string
	validateCSV bool
)

var backupsCmd = &cobra.Command{
	Use: "backups",
	Short: "backups produces reports on files to be backed-up and what isn't " +
		"under a backup plan",
	Long: `backups produces reports on files to be backed-up and what isn't
under a backup plan.

A CSV file must be provided by the --csv/-c flag, and contain the following
headers in any order:

	reporting_name
	reporting_root
	requestor
	faculty
	directory
	instruction ['backup' or 'nobackup' or 'tempbackup']
	file_types_backup
	file_types_ignore

An output directory must also be specified via the --output/-o flag. This
directory will be used to store the FOFNs (File-of-Filenames) for the files to
be backed-up and the overall summary file.

The FOFNs will have a filename with the following format:

	{requestor}_{reporting_name}

…and contain a single, quoted filename on each line of the file.

The summary file will be a JSON file named 'summary.json'. Each row of the JSON
file will contain the following fields:

	Faculty   // Absent in warn root entries
	Name      // Absent in warn root entries
	Requestor // Absent in warn root entries
	Root
	Action
	UserID
	Base
	Size
	Count
	OldestMTime
	NewestMTime

The --root/-r flag can be used to specify additional directories that will be
used for detecting non-planned for files, flagging them as 'warn'.

Lastly, input files can either be specified directly, with the paths to the
stats.gz files created by wrstat, or a directory can be specified that will be
searched for stats.gz files in the same manner as the server and watch commands.
`,
	Run: func(_ *cobra.Command, args []string) {
		f := must(os.Open(backupCSV))
		csv := must(backups.ParseCSV(f))
		f.Close()

		if validateCSV {
			return
		}

		statsFiles := must(parseFiles(args))
		r := must(combineStatsFiles(statsFiles))

		b := must(backups.New(csv, roots...))

		must("", b.Process(r, reportRoot))

		f = must(os.Create(filepath.Join(reportRoot, "summary.json")))

		must("", b.Summarise(f))
		must("", f.Close())
	},
}

func must[T any](v T, err error) T { //nolint:ireturn
	if err != nil {
		die("%s", err)
	}

	return v
}

func init() {
	RootCmd.AddCommand(backupsCmd)

	backupsCmd.Flags().StringVarP(&backupCSV, "csv", "c", "", "Backup CSV input file")
	backupsCmd.Flags().StringVarP(&reportRoot, "output", "o", "", "output directory")
	backupsCmd.Flags().StringSliceVarP(&roots, "root", "r", nil, "root to add to the warn list")
	backupsCmd.Flags().BoolVarP(&validateCSV, "validate", "v", false, "validate CSV input file only")
}

type pathFile struct {
	path string
	io.Reader
}

func combineStatsFiles(filePaths []string) (io.Reader, error) { //nolint:gocognit,gocyclo,funlen
	files := make([]pathFile, len(filePaths))

	for n, file := range filePaths {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}

		var r io.Reader = f

		if strings.HasSuffix(file, ".gz") {
			if r, err = gzip.NewReader(f); err != nil {
				return nil, err
			}
		}

		path, err := readFirstPath(r)
		if err != nil {
			return nil, err
		}

		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		if strings.HasSuffix(file, ".gz") {
			if r, err = pgzip.NewReader(f); err != nil {
				return nil, err
			}
		}

		files[n] = pathFile{path, r}
	}

	return mergeFiles(files)
}

func readFirstPath(f io.Reader) (string, error) {
	var path string

	if _, err := fmt.Fscanf(f, "%q", &path); err != nil {
		return "", err
	}

	return path, nil
}

func sortFiles(files []pathFile) {
	slices.SortFunc(files, func(a, b pathFile) int {
		return strings.Compare(a.path, b.path)
	})
}

func mergeFiles(files []pathFile) (io.Reader, error) {
	sortFiles(files)

	readers := make([]io.Reader, 0, len(files))

	if files[0].path != "/" {
		readers = append(readers, strings.NewReader("\"/\"\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n"))
	}

	for _, file := range files {
		readers = append(readers, file.Reader)
	}

	return io.MultiReader(readers...), nil
}
