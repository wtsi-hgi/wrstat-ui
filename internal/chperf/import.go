/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package chperf

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/internal/boltperf"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	statsGZBasename   = "stats.gz"
	lineReaderBufSize = 32 * 1024
)

// ErrNoDatasets indicates no dataset directories were found.
var ErrNoDatasets = errors.New("no dataset directories found")

// PrintfFunc matches fmt.Printf-style output.
type PrintfFunc = boltperf.PrintfFunc

// Import discovers stats.gz datasets under inputDir, ingests them into
// ClickHouse, and returns a Report with timing information.
func Import(
	cfg clickhouse.Config,
	inputDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (boltperf.Report, error) {
	datasetDirs, err := findDatasets(inputDir)
	if err != nil {
		return boltperf.Report{}, err
	}

	report := boltperf.NewReport("clickhouse", inputDir, 1, 0)
	startAll := time.Now()

	totalRecords, err := importDatasets(cfg, datasetDirs, opts, printf)
	if err != nil {
		return boltperf.Report{}, err
	}

	report.AddOperation(
		"import_total",
		map[string]any{"datasets": len(datasetDirs), "records": totalRecords},
		[]float64{durationMS(time.Since(startAll))},
	)

	return report, nil
}

func findDatasets(baseDir string) ([]string, error) {
	dirs, err := datasets.FindLatestDatasetDirs(baseDir, statsGZBasename)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNoDatasets, baseDir)
	}

	sort.Strings(dirs)

	return dirs, nil
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func importDatasets(
	cfg clickhouse.Config,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) (uint64, error) {
	if opts.Parallelism <= 1 {
		return importSerial(cfg, datasetDirs, opts, printf)
	}

	return importParallel(cfg, datasetDirs, opts, printf)
}

func importSerial(
	cfg clickhouse.Config,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) (uint64, error) {
	var total uint64

	for _, dir := range datasetDirs {
		n, err := importOneDataset(cfg, dir, opts, printf)
		if err != nil {
			return 0, err
		}

		total += n
	}

	return total, nil
}

func importParallel(
	cfg clickhouse.Config,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) (uint64, error) {
	results := runParallel(cfg, datasetDirs, opts, printf)

	return sumResults(results)
}

func sumResults(results []importResult) (uint64, error) {
	var total uint64

	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}

		total += r.records
	}

	return total, nil
}

func runParallel(
	cfg clickhouse.Config,
	datasetDirs []string,
	opts ImportOptions,
	printf PrintfFunc,
) []importResult {
	sem := make(chan struct{}, opts.Parallelism)
	results := make([]importResult, len(datasetDirs))

	var wg sync.WaitGroup

	for idx, dir := range datasetDirs {
		wg.Add(1)

		go func(i int, d string) {
			defer wg.Done()

			sem <- struct{}{}

			defer func() { <-sem }()

			n, err := importOneDataset(cfg, d, opts, printf)
			results[i] = importResult{records: n, err: err}
		}(idx, dir)
	}

	wg.Wait()

	return results
}

func importOneDataset(
	cfg clickhouse.Config,
	datasetDir string,
	opts ImportOptions,
	printf PrintfFunc,
) (_ uint64, err error) {
	mp, err := mountpath.FromOutputDir(datasetDir)
	if err != nil {
		return 0, err
	}

	statsPath := filepath.Join(datasetDir, statsGZBasename)

	st, err := os.Stat(statsPath)
	if err != nil {
		return 0, err
	}

	updatedAt := st.ModTime()
	start := time.Now()

	records, err := ingestStatsGZ(cfg, statsPath, mp, updatedAt, opts)
	if err != nil {
		return 0, err
	}

	printf("import dataset=%s mount=%s records=%d seconds=%.3f\n",
		filepath.Base(datasetDir), mp, records, time.Since(start).Seconds())

	return records, nil
}

func ingestStatsGZ(
	cfg clickhouse.Config,
	statsPath, mp string,
	updatedAt time.Time,
	opts ImportOptions,
) (_ uint64, err error) {
	gz, closeFn, err := openStatsGZReader(statsPath)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := closeFn(); err == nil {
			err = cerr
		}
	}()

	return summariseReader(gz, cfg, mp, updatedAt, opts)
}

func openStatsGZReader(path string) (*pgzip.Reader, func() error, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	gz, err := pgzip.NewReader(fh)
	if err != nil {
		_ = fh.Close()

		return nil, nil, err
	}

	closeFn := func() error {
		gzErr := gz.Close()
		fhErr := fh.Close()

		return errors.Join(gzErr, fhErr)
	}

	return gz, closeFn, nil
}

func summariseReader(
	r io.Reader,
	cfg clickhouse.Config,
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
) (_ uint64, err error) {
	lr := newLineCountingReader(r, opts.MaxLines)
	ss := summary.NewSummariser(stats.NewStatsParser(lr))

	allClosers, err := addAllSummarisers(ss, cfg, mp, updatedAt, opts)
	if err != nil {
		return 0, err
	}

	defer func() {
		if cerr := allClosers(); err == nil {
			err = cerr
		}
	}()

	if err := ss.Summarise(); err != nil {
		return 0, err
	}

	return lr.linesRead(), nil
}

func newLineCountingReader(r io.Reader, maxLines int) *lineCountingReader {
	var ml uint64
	if maxLines > 0 {
		ml = uint64(maxLines)
	}

	return &lineCountingReader{
		underlying: r,
		maxLines:   ml,
		buf:        make([]byte, lineReaderBufSize),
	}
}

func addAllSummarisers(
	ss *summary.Summariser,
	cfg clickhouse.Config,
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
) (func() error, error) {
	dw, err := clickhouse.NewDGUTAWriter(cfg)
	if err != nil {
		return nil, err
	}

	dw.SetMountPath(mp)
	dw.SetUpdatedAt(updatedAt)
	dw.SetBatchSize(opts.BatchSize)

	fi, fiCloser, err := clickhouse.NewFileIngestOperation(cfg, mp, updatedAt)
	if err != nil {
		_ = dw.Close()

		return nil, err
	}

	closers := func() error {
		return errors.Join(fiCloser.Close(), dw.Close())
	}

	ss.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dw))
	ss.AddGlobalOperation(fi)

	bsCloser, err := addBasedirsSummariser(ss, cfg, mp, updatedAt, opts)
	if err != nil {
		_ = closers() //nolint:errcheck

		return nil, err
	}

	return func() error {
		return errors.Join(closers(), bsCloser())
	}, nil
}

func addBasedirsSummariser(
	ss *summary.Summariser,
	cfg clickhouse.Config,
	mp string,
	updatedAt time.Time,
	opts ImportOptions,
) (func() error, error) {
	if opts.QuotaPath == "" || opts.ConfigPath == "" {
		return func() error { return nil }, nil
	}

	bs, err := clickhouse.NewBaseDirsStore(cfg)
	if err != nil {
		return nil, err
	}

	bs.SetMountPath(mp)
	bs.SetUpdatedAt(updatedAt)

	closer := func() error { return bs.Close() }

	if err := addBasedirsOp(ss, bs, updatedAt, opts); err != nil {
		_ = bs.Close()

		return nil, err
	}

	return closer, nil
}

func addBasedirsOp(
	ss *summary.Summariser,
	store basedirs.Store,
	modtime time.Time,
	opts ImportOptions,
) error {
	quotas, config, mountpoints, err := parseBasedirsInputs(opts)
	if err != nil {
		return err
	}

	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		return err
	}

	if len(mountpoints) > 0 {
		bd.SetMountPoints(mountpoints)
	}

	bd.SetModTime(modtime)
	ss.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func parseBasedirsInputs(opts ImportOptions) (*basedirs.Quotas, basedirs.Config, []string, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(opts.QuotaPath, opts.ConfigPath)
	if err != nil {
		return nil, nil, nil, err
	}

	var mountpoints []string

	if opts.MountsPath != "" {
		mountpoints, err = summariseutil.ParseMountpointsFromFile(opts.MountsPath)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return quotas, config, mountpoints, nil
}

// ImportOptions configures the import operation.
type ImportOptions struct {
	MaxLines    int
	BatchSize   int
	Parallelism int
	QuotaPath   string
	ConfigPath  string
	MountsPath  string
}

type importResult struct {
	records uint64
	err     error
}

type lineCountingReader struct {
	underlying io.Reader
	maxLines   uint64

	buf        []byte
	pending    []byte
	seenLines  uint64
	reachedMax bool
}

func (l *lineCountingReader) linesRead() uint64 {
	return l.seenLines
}

func (l *lineCountingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if n, ok, err := l.readPendingOrDone(p); ok {
		return n, err
	}

	if l.reachedMax {
		return 0, io.EOF
	}

	n, err := l.underlying.Read(l.buf)
	if n == 0 {
		return 0, err
	}

	chunk := l.buf[:n]
	allowed := l.limitChunk(chunk)

	nn := copy(p, allowed)
	if nn < len(allowed) {
		l.pending = append(l.pending[:0], allowed[nn:]...)
	}

	return nn, l.errAfterRead(err)
}

func (l *lineCountingReader) readPendingOrDone(p []byte) (int, bool, error) {
	n := l.readPending(p)
	if n == 0 {
		return 0, false, nil
	}

	if l.reachedMax && len(l.pending) == 0 {
		return n, true, io.EOF
	}

	return n, true, nil
}

func (l *lineCountingReader) errAfterRead(underlyingErr error) error {
	if l.reachedMax {
		if len(l.pending) == 0 {
			return io.EOF
		}

		return nil
	}

	return underlyingErr
}

func (l *lineCountingReader) readPending(p []byte) int {
	if len(l.pending) == 0 {
		return 0
	}

	n := copy(p, l.pending)
	l.pending = l.pending[n:]

	return n
}

func (l *lineCountingReader) limitChunk(chunk []byte) []byte {
	if l.maxLines == 0 {
		l.seenLines += countNewLines(chunk)

		return chunk
	}

	for i, b := range chunk {
		if b != '\n' {
			continue
		}

		l.seenLines++
		if l.seenLines >= l.maxLines {
			l.reachedMax = true

			return chunk[:i+1]
		}
	}

	return chunk
}

func countNewLines(b []byte) uint64 {
	var n uint64

	for _, c := range b {
		if c == '\n' {
			n++
		}
	}

	return n
}
