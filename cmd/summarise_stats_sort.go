/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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
	"bytes"
	"cmp"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	summariseStatsSortChunkBytes   = 64 * bytesPerMiB
	summariseStatsSortMaxLineBytes = 64 * 1024
	summariseStatsSortOutputName   = "stats.sorted"
)

var errSummariseStatsSortRecordTooLarge = errors.New("summarise stats sort record too large")

var errSummariseStatsSortHeapType = errors.New("summarise stats sort heap contained an invalid item")

type summariseStatsSortRecord struct {
	key  string
	line []byte
	seq  uint64
}

func summariseReadStatsSortRecord(r io.Reader) (summariseStatsSortRecord, error) {
	key, err := summariseReadStatsSortBytes(r)
	if err != nil {
		return summariseStatsSortRecord{}, err
	}

	var seq uint64
	if readErr := binary.Read(r, binary.BigEndian, &seq); readErr != nil {
		return summariseStatsSortRecord{}, readErr
	}

	line, err := summariseReadStatsSortBytes(r)
	if err != nil {
		return summariseStatsSortRecord{}, err
	}

	return summariseStatsSortRecord{key: string(key), line: line, seq: seq}, nil
}

func summariseWriteStatsSortRecord(w io.Writer, record summariseStatsSortRecord) error {
	if err := binary.Write(w, binary.BigEndian, uint64(len(record.key))); err != nil {
		return err
	}

	if _, err := io.WriteString(w, record.key); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, record.seq); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint64(len(record.line))); err != nil {
		return err
	}

	_, err := w.Write(record.line)

	return err
}

type summariseStatsSortScanState struct {
	chunks     []string
	records    []summariseStatsSortRecord
	chunkBytes int
	seq        uint64
}

func (s *summariseStatsSortScanState) add(scannerLine []byte) {
	line := slices.Clone(scannerLine)
	key := summariseStatsSortKey(line)

	s.records = append(s.records, summariseStatsSortRecord{key: key, line: line, seq: s.seq})
	s.chunkBytes += len(key) + len(line)
	s.seq++
}

func summariseStatsSortKey(line []byte) string {
	pathColumn := line
	if idx := bytes.IndexByte(line, '\t'); idx >= 0 {
		pathColumn = line[:idx]
	}

	key, err := strconv.Unquote(string(pathColumn))
	if err == nil {
		return key
	}

	return string(pathColumn)
}

func (s *summariseStatsSortScanState) shouldFlush() bool {
	return s.chunkBytes >= summariseStatsSortChunkBytes
}

func (s *summariseStatsSortScanState) flush(scratchDir string) error {
	chunkPath, err := summariseFlushStatsSortChunk(s.records, scratchDir, len(s.chunks))
	if err != nil {
		return err
	}

	s.chunks = append(s.chunks, chunkPath)
	s.records = nil
	s.chunkBytes = 0

	return nil
}

func summariseFlushStatsSortChunk(
	records []summariseStatsSortRecord,
	scratchDir string,
	chunkIndex int,
) (path string, err error) {
	slices.SortStableFunc(records, compareSummariseStatsSortRecords)

	path = filepath.Join(scratchDir, fmt.Sprintf("stats-sort-%06d.bin", chunkIndex))

	fh, err := os.Create(path)
	if err != nil {
		return "", err
	}

	bw := bufio.NewWriter(fh)
	for _, record := range records {
		if err = summariseWriteStatsSortRecord(bw, record); err != nil {
			break
		}
	}

	err = errors.Join(err, bw.Flush(), fh.Close())
	if err != nil {
		return "", err
	}

	return path, nil
}

func (s *summariseStatsSortScanState) finish(scratchDir string) ([]string, error) {
	if len(s.records) == 0 {
		return s.chunks, nil
	}

	if err := s.flush(scratchDir); err != nil {
		return nil, err
	}

	return s.chunks, nil
}

func summariseScanStatsSortChunks(scanner *bufio.Scanner, scratchDir string) ([]string, error) {
	state := summariseStatsSortScanState{}
	for scanner.Scan() {
		state.add(scanner.Bytes())

		if state.shouldFlush() {
			if err := state.flush(scratchDir); err != nil {
				return nil, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return state.finish(scratchDir)
}

type summariseSortedStatsReadCloser struct {
	io.ReadCloser
	scratchDir string
}

func (r *summariseSortedStatsReadCloser) Close() error {
	return errors.Join(r.ReadCloser.Close(), os.RemoveAll(r.scratchDir))
}

func openSortedSummariseStats(statsPath string, scratchDir string) (io.ReadCloser, error) {
	sortedPath, err := summariseWriteSortedStatsFile(statsPath, scratchDir)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(scratchDir))
	}

	fh, err := os.Open(sortedPath)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(scratchDir))
	}

	return &summariseSortedStatsReadCloser{ReadCloser: fh, scratchDir: scratchDir}, nil
}

func summariseWriteSortedStatsFile(statsPath string, scratchDir string) (string, error) {
	if err := os.RemoveAll(scratchDir); err != nil {
		return "", err
	}

	if err := os.MkdirAll(scratchDir, summariseDirPerm); err != nil {
		return "", err
	}

	chunks, err := summariseSortStatsChunks(statsPath, scratchDir)
	if err != nil {
		return "", err
	}

	outPath := filepath.Join(scratchDir, summariseStatsSortOutputName)
	if err := summariseMergeStatsSortChunks(chunks, outPath); err != nil {
		return "", err
	}

	return outPath, nil
}

func summariseSortStatsChunks(statsPath string, scratchDir string) (_ []string, err error) {
	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, summariseStatsSortMaxLineBytes), summariseStatsSortMaxLineBytes)

	return summariseScanStatsSortChunks(scanner, scratchDir)
}

func summariseMergeStatsSortChunks(chunks []string, outPath string) (err error) {
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}

	bw := bufio.NewWriter(out)

	readers, err := summariseStatsSortChunkReaders(chunks)
	if err != nil {
		return errors.Join(err, bw.Flush(), out.Close())
	}

	defer func() {
		err = errors.Join(err, closeSummariseStatsSortChunkReaders(readers))
	}()

	if err = summariseWriteMergedStatsSortChunks(bw, readers); err != nil {
		return errors.Join(err, bw.Flush(), out.Close())
	}

	return errors.Join(bw.Flush(), out.Close())
}

func summariseStatsSortChunkReaders(chunks []string) ([]*summariseStatsSortChunkReader, error) {
	readers := make([]*summariseStatsSortChunkReader, 0, len(chunks))

	for _, chunk := range chunks {
		reader, err := newSummariseStatsSortChunkReader(chunk)
		if errors.Is(err, io.EOF) {
			continue
		}

		if err != nil {
			return nil, errors.Join(err, closeSummariseStatsSortChunkReaders(readers))
		}

		readers = append(readers, reader)
	}

	return readers, nil
}

func newSummariseStatsSortChunkReader(path string) (*summariseStatsSortChunkReader, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := &summariseStatsSortChunkReader{
		file:   fh,
		reader: bufio.NewReader(fh),
	}

	if err := reader.Next(); err != nil {
		return nil, errors.Join(err, reader.Close())
	}

	return reader, nil
}

func closeSummariseStatsSortChunkReaders(readers []*summariseStatsSortChunkReader) error {
	var err error

	for _, reader := range readers {
		err = errors.Join(err, reader.Close())
	}

	return err
}

func summariseWriteMergedStatsSortChunks(
	bw *bufio.Writer,
	readers []*summariseStatsSortChunkReader,
) error {
	h := summariseStatsSortHeap(readers)
	heap.Init(&h)

	for h.Len() > 0 {
		if err := summariseWriteNextMergedStatsSortChunk(bw, &h); err != nil {
			return err
		}
	}

	return nil
}

func summariseWriteNextMergedStatsSortChunk(
	bw *bufio.Writer,
	h *summariseStatsSortHeap,
) error {
	reader, err := popSummariseStatsSortChunkReader(h)
	if err != nil {
		return err
	}

	if err := summariseWriteStatsSortOutputLine(bw, reader.record.line); err != nil {
		return err
	}

	return summariseAdvanceStatsSortChunkReader(h, reader)
}

func popSummariseStatsSortChunkReader(
	h *summariseStatsSortHeap,
) (*summariseStatsSortChunkReader, error) {
	reader, ok := heap.Pop(h).(*summariseStatsSortChunkReader)
	if !ok {
		return nil, errSummariseStatsSortHeapType
	}

	return reader, nil
}

func summariseWriteStatsSortOutputLine(bw *bufio.Writer, line []byte) error {
	if _, err := bw.Write(line); err != nil {
		return err
	}

	return bw.WriteByte('\n')
}

func summariseAdvanceStatsSortChunkReader(
	h *summariseStatsSortHeap,
	reader *summariseStatsSortChunkReader,
) error {
	if err := reader.Next(); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}

	pushSummariseStatsSortChunkReader(h, reader)

	return nil
}

func pushSummariseStatsSortChunkReader(
	h *summariseStatsSortHeap,
	reader *summariseStatsSortChunkReader,
) {
	heap.Push(h, reader)
}

type summariseStatsSortChunkReader struct {
	file   *os.File
	reader *bufio.Reader
	record summariseStatsSortRecord
}

func (r *summariseStatsSortChunkReader) Next() error {
	record, err := summariseReadStatsSortRecord(r.reader)
	if err != nil {
		return err
	}

	r.record = record

	return nil
}

func (r *summariseStatsSortChunkReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}

	err := r.file.Close()
	r.file = nil

	return err
}

type summariseStatsSortHeap []*summariseStatsSortChunkReader

func (h summariseStatsSortHeap) Len() int {
	return len(h)
}

func (h summariseStatsSortHeap) Less(i, j int) bool {
	return compareSummariseStatsSortRecords(h[i].record, h[j].record) < 0
}

func compareSummariseStatsSortRecords(a, b summariseStatsSortRecord) int {
	if diff := strings.Compare(a.key, b.key); diff != 0 {
		return diff
	}

	return cmp.Compare(a.seq, b.seq)
}

func (h summariseStatsSortHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *summariseStatsSortHeap) Push(x any) {
	reader, ok := x.(*summariseStatsSortChunkReader)
	if !ok {
		panic(errSummariseStatsSortHeapType)
	}

	*h = append(*h, reader)
}

func (h *summariseStatsSortHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}

func openSummariseSpoolStats(
	statsPath string,
	partialDir string,
	sortInput bool,
) (io.ReadCloser, error) {
	if sortInput {
		return openSortedSummariseStats(statsPath, summariseStatsSortScratchDir(partialDir))
	}

	r, _, err := openStatsFile(statsPath)

	return r, err
}

func summariseStatsSortScratchDir(partialDir string) string {
	return partialDir + ".stats-sort"
}

func summariseReadStatsSortBytes(r io.Reader) ([]byte, error) {
	var length uint64
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	if length > summariseStatsSortMaxLineBytes {
		return nil, fmt.Errorf("%w: %d bytes", errSummariseStatsSortRecordTooLarge, length)
	}

	out := make([]byte, int(length))
	_, err := io.ReadFull(r, out)

	return out, err
}
