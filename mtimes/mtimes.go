package mtimes

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/datatree"
	"golang.org/x/exp/slices"
)

type timeFile struct {
	path string
	io.Reader
}

func Build(filePaths []string, output string) error {
	if len(filePaths) == 0 {
		return ErrNoFiles
	}

	files := make([]timeFile, len(filePaths))

	for n, file := range filePaths {
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		defer f.Close()

		var r io.Reader = f

		if strings.HasSuffix(file, ".gz") {
			if r, err = gzip.NewReader(f); err != nil {
				return err
			}
		}

		path, err := readFirstPath(r)
		if err != nil {
			return err
		}

		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}

		if strings.HasSuffix(file, ".gz") {
			if r, err = pgzip.NewReader(f); err != nil {
				return err
			}
		}

		files[n] = timeFile{path, r}
	}

	return buildTree(files, output)
}

func readFirstPath(f io.Reader) (string, error) {
	var path string

	if _, err := fmt.Fscanf(f, "%q", &path); err != nil {
		return "", err
	}

	return path, nil
}

func buildTree(files []timeFile, output string) error {
	r, err := mergeFiles(files)
	if err != nil {
		return err
	}

	w, err := os.Create(output)
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(w)

	s := summary.NewSummariser(stats.NewStatsParser(r))
	s.AddDirectoryOperation(datatree.NewTree(buf))

	if err = s.Summarise(); err != nil {
		return err
	}

	return buf.Flush()
}

func sortFiles(files []timeFile) {
	slices.SortFunc(files, func(a, b timeFile) int {
		return strings.Compare(a.path, b.path)
	})
}

func mergeFiles(files []timeFile) (io.Reader, error) {
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

var ErrNoFiles = errors.New("no files supplied")
