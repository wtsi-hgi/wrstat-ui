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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	bolt "github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dedupe"
)

const inputStatsFile = "stats.gz"

var (
	minSize int64
	output  string

	ErrNoStatsFiles = errors.New("no stats files specified")
)

var dupescmd = &cobra.Command{
	Use:   "dupes",
	Short: "dupes searches for potentially duplicated files in wrstat output",
	Long: `dupes searches wrstat output for files with the same size,
flagging them as potential duplicates. Useful as a pre-step for a more absolute
check of same-ness.

Input files can either be specified directly, with the paths to the stats.gz
files created by wrstat, or a directory can be specified that will be searched
for stats.gz files in the same manner as the server and watch commands.

The --minsize/-m flag can be used to set the minimum file size to consider as a
duplicate. It is recommended to set this at at-least 10MB to avoid many small
files being discovered where the likeliness of coincidental size matches is
high.

The --output/-o flag can be used to set the output file, instead of the default
stdout. Files ending in '.gz' will be compressed.
`,
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

func collectFilesFromArg(arg string) ([]string, error) {
	fi, err := os.Stat(arg)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return []string{arg}, nil
	}

	dirs, _, err := bolt.FindDBDirs(arg, inputStatsFile)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, dir := range dirs {
		files = append(files, filepath.Join(dir, inputStatsFile))
	}

	return files, nil
}

func parseFiles(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, ErrNoStatsFiles
	}

	var files []string
	for _, arg := range args {
		f, err := collectFilesFromArg(arg)
		if err != nil {
			return nil, err
		}

		files = append(files, f...)
	}

	return files, nil
}

func processStatsFile(statsFile string, sp *stats.StatsParser, s *summary.Summariser) error {
	f, err := os.Open(statsFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(statsFile, ".gz") {
		if r, err = pgzip.NewReader(f); err != nil {
			return err
		}
	}

	*sp = *stats.NewStatsParser(r)

	return s.Summarise()
}

func findDupes(files []string, minSize int64, output string) error {
	sp := stats.NewStatsParser(nil)
	deduper := dedupe.Deduper{MinFileSize: minSize}
	s := summary.NewSummariser(sp)
	s.AddGlobalOperation(deduper.Operation())

	for _, statsFile := range files {
		if err := processStatsFile(statsFile, sp, s); err != nil {
			return err
		}
	}

	return outputDupes(output, &deduper)
}

func outputDupes(output string, d *dedupe.Deduper) (err error) {
	var w io.Writer

	if output == "-" { //nolint:nestif
		w = os.Stdout
	} else {
		f, errr := os.Create(output)
		if errr != nil {
			return errr
		}

		w = f

		defer deferClose(f.Close, &err)
	}

	if strings.HasSuffix(output, ".gz") {
		g := pgzip.NewWriter(w)
		w = g

		defer deferClose(g.Close, &err)
	} else {
		b := bufio.NewWriter(w)
		w = b

		defer deferClose(b.Flush, &err)
	}

	err = d.Print(w)

	return err
}

func deferClose(fn func() error, err *error) {
	if errr := fn(); *err == nil {
		*err = errr
	}
}

func init() {
	RootCmd.AddCommand(dupescmd)

	dupescmd.Flags().Int64VarP(&minSize, "minsize", "m", 0, "minimum file size to consider a possible duplicate")
	dupescmd.Flags().StringVarP(&output, "output", "o", "-", "file to output possible duplicate file data")
}
