package mtimes

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/timetree"
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
	sortFiles(files)

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
	s.AddDirectoryOperation(timetree.NewTimeTree(buf))

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
	added := make(map[string]struct{})
	readers := make([]io.Reader, 0, len(files))

	for n, file := range files {
		for _, pfile := range files[:n] {
			if strings.HasPrefix(file.path, pfile.path) {
				return nil, ErrBadMerge
			}
		}

		toAdd := unboundParents(file.path, added)

		if len(toAdd) > 0 {
			var buf bytes.Buffer

			writeLines(&buf, toAdd)

			readers = append(readers, &buf)
		}

		readers = append(readers, file.Reader)
	}

	return io.MultiReader(readers...), nil
}

func unboundParents(path string, added map[string]struct{}) []string {
	toAdd := make([]string, 0)

	for parent := filepath.Dir(strings.TrimSuffix(path, "/")); ; parent = filepath.Dir(parent) {
		if _, ok := added[parent]; ok {
			break
		}

		added[parent] = struct{}{}

		if parent == "/" {
			toAdd = append(toAdd, parent)
		} else {
			toAdd = append(toAdd, parent+"/")
		}
	}

	return toAdd
}

func writeLines(w io.Writer, toAdd []string) {
	slices.Reverse(toAdd)

	for _, line := range toAdd {
		fmt.Fprintf(w, "%q\t0\t0\t0\t0\t0\t0\td\t0\t1\t1\t0\n", line)
	}
}

var (
	ErrBadMerge = errors.New("can not merge file that is a subdirectory of another file")
	ErrNoFiles  = errors.New("no files supplied")
)
