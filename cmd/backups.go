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

The summary file will be a JSON file named 'summary.json'. The file will contain
 an array of objects, each of which has the following fields:

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
stats.gz files created by wrstat, or directoies can be specified that will be
searched for stats.gz files in the same manner as the server and watch commands.
`,
	RunE: func(_ *cobra.Command, args []string) error {
		f, err := os.Open(backupCSV)
		if err != nil {
			return err
		}

		csv, err := backups.ParseCSV(f)
		if err != nil {
			return err
		}

		f.Close()

		if validateCSV {
			return nil
		}

		statsFiles, err := parseFiles(args)
		if err != nil {
			return err
		}

		r, err := combineStatsFiles(statsFiles)
		if err != nil {
			return err
		}

		b, err := backups.New(csv, roots...)
		if err != nil {
			return err
		}

		if err = b.Process(r, reportRoot); err != nil {
			return err
		}

		f, err = os.Create(filepath.Join(reportRoot, "summary.json"))
		if err != nil {
			return err
		}

		if err = b.Summarise(f); err != nil {
			return err
		}

		return f.Close()
	},
}

func init() {
	RootCmd.AddCommand(backupsCmd)

	backupsCmd.Flags().StringVarP(&backupCSV, "csv", "c", "", "backup plan CSV input file")
	backupsCmd.Flags().StringVarP(&reportRoot, "output", "o", "", "output directory")
	backupsCmd.Flags().StringSliceVarP(&roots, "root", "r", nil, "root to add to the warn list, "+
		"can be supplied multuple times")
	backupsCmd.Flags().BoolVarP(&validateCSV, "validate", "v", false, "validate CSV input file only")
}

type pathReader struct {
	path string
	io.Reader
}

func combineStatsFiles(filePaths []string) (io.Reader, error) {
	files := make([]*pathReader, len(filePaths))

	var err error

	for n, file := range filePaths {
		if files[n], err = createPathReader(file); err != nil {
			return nil, err
		}
	}

	return mergeReaders(files)
}

func createPathReader(file string) (*pathReader, error) { //nolint:gocyclo
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	pf := &pathReader{Reader: f}
	isGzip := strings.HasSuffix(file, ".gz")

	if isGzip {
		if pf.Reader, err = gzip.NewReader(f); err != nil {
			return nil, err
		}
	}

	pf.path, err = readFirstPath(pf.Reader)
	if err != nil {
		return nil, err
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if isGzip {
		if pf.Reader, err = pgzip.NewReader(f); err != nil {
			return nil, err
		}
	}

	return pf, nil
}

func readFirstPath(f io.Reader) (string, error) {
	var path string

	if _, err := fmt.Fscanf(f, "%q", &path); err != nil {
		return "", err
	}

	return path, nil
}

func mergeReaders(files []*pathReader) (io.Reader, error) {
	sortPathReaders(files)

	readers := make([]io.Reader, 0, len(files))

	if files[0].path != "/" {
		readers = append(readers, strings.NewReader("\"/\"\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n"))
	}

	for _, file := range files {
		readers = append(readers, file.Reader)
	}

	return io.MultiReader(readers...), nil
}

func sortPathReaders(files []*pathReader) {
	slices.SortFunc(files, func(a, b *pathReader) int {
		return strings.Compare(a.path, b.path)
	})
}
