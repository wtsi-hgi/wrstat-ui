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

	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

const (
	summariseStatsSortChunkBytes   = 64 * bytesPerMiB
	summariseStatsSortMaxLineBytes = 64 * 1024
	summariseStatsSortOutputName   = "stats.sorted"
	summariseStatsSortHexByteLen   = 2
	summariseStatsSortPathKeyDir   = byte(0)
	summariseStatsSortPathKeyFile  = byte(1)
)

var errSummariseStatsSortRecordTooLarge = errors.New("summarise stats sort record too large")

var errSummariseStatsSortHeapType = errors.New("summarise stats sort heap contained an invalid item")

const (
	summariseStatsSortExplicitDirPriority = iota
	summariseStatsSortSyntheticDirPriority
	summariseStatsSortFilePriority
)

type summariseStatsSortRecordKind uint8

const (
	summariseStatsSortRecordFile summariseStatsSortRecordKind = iota
	summariseStatsSortRecordExplicitDir
	summariseStatsSortRecordSyntheticDir
)

type summariseStatsSortRecord struct {
	key  string
	line []byte
	seq  uint64
	kind summariseStatsSortRecordKind
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

	var kind summariseStatsSortRecordKind
	if readErr := binary.Read(r, binary.BigEndian, &kind); readErr != nil {
		return summariseStatsSortRecord{}, readErr
	}

	line, err := summariseReadStatsSortBytes(r)
	if err != nil {
		return summariseStatsSortRecord{}, err
	}

	return summariseStatsSortRecord{key: string(key), line: line, seq: seq, kind: kind}, nil
}

func (r summariseStatsSortRecord) priority() int {
	switch r.kind {
	case summariseStatsSortRecordExplicitDir:
		return summariseStatsSortExplicitDirPriority
	case summariseStatsSortRecordSyntheticDir:
		return summariseStatsSortSyntheticDirPriority
	default:
		return summariseStatsSortFilePriority
	}
}

func (r summariseStatsSortRecord) isDir() bool {
	return r.kind == summariseStatsSortRecordExplicitDir || r.kind == summariseStatsSortRecordSyntheticDir
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

	if err := binary.Write(w, binary.BigEndian, record.kind); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint64(len(record.line))); err != nil {
		return err
	}

	_, err := w.Write(record.line)

	return err
}

func summariseStatsSortSyntheticDirRecords(
	info stats.FileInfo,
	mountPath string,
	seq uint64,
) []summariseStatsSortRecord {
	dirPath, ok := summariseStatsSortDirPath(info)
	if !ok {
		return nil
	}

	dirs := summariseStatsSortAncestorDirsForMount(mountPath, dirPath)
	records := make([]summariseStatsSortRecord, 0, len(dirs))

	for _, dir := range dirs {
		records = append(records, summariseStatsSortRecord{
			key:  summariseStatsSortDirPathKey(dir),
			line: summariseStatsSortSyntheticDirLine(dir, info),
			seq:  seq,
			kind: summariseStatsSortRecordSyntheticDir,
		})
	}

	return records
}

func summariseStatsSortDirPath(info stats.FileInfo) (string, bool) {
	path := string(info.Path)
	if path == "" {
		return "", false
	}

	if info.EntryType == stats.DirType {
		return summariseEnsureTrailingSlash(path), true
	}

	idx := strings.LastIndexByte(path, '/')
	if idx < 0 {
		return "", false
	}

	return path[:idx+1], true
}

func summariseStatsSortAncestorDirsForMount(mountPath, dirPath string) []string {
	mountPath = summariseNormalizeImportMountPath(mountPath)
	if mountPath == "" {
		return nil
	}

	dirPath = summariseEnsureTrailingSlash(dirPath)
	if !strings.HasPrefix(dirPath, mountPath) {
		return nil
	}

	dirs := []string{mountPath}
	current := mountPath
	relative := strings.TrimPrefix(dirPath, mountPath)

	for relative != "" {
		part, rest, _ := strings.Cut(relative, "/")
		if part == "" {
			break
		}

		current += part + "/"
		dirs = append(dirs, current)
		relative = rest
	}

	return dirs
}

func summariseStatsSortDirPathKey(path string) string {
	return summariseStatsSortPathKey(path, summariseStatsSortPathKeyDir)
}

func summariseStatsSortPathKey(path string, terminal byte) string {
	parts := split.SplitPath(path)

	var key strings.Builder
	key.Grow(len(path) * summariseStatsSortHexByteLen)

	for idx, part := range parts {
		summariseWriteStatsSortPathKeyPart(&key, strings.TrimSuffix(part, "/"))

		if idx == len(parts)-1 {
			key.WriteByte(terminal)
		} else {
			key.WriteByte(summariseStatsSortPathKeyDir)
		}
	}

	return key.String()
}

func summariseWriteStatsSortPathKeyPart(key *strings.Builder, part string) {
	const hex = "0123456789abcdef"

	for _, b := range []byte(part) {
		key.WriteByte(hex[b>>4])
		key.WriteByte(hex[b&0x0f])
	}
}

func summariseStatsSortSyntheticDirLine(path string, info stats.FileInfo) []byte {
	return []byte(fmt.Sprintf(
		"%q\t0\t%d\t%d\t%d\t%d\t%d\td\t0\t%d\t1\t0",
		path,
		info.UID,
		info.GID,
		info.ATime,
		info.MTime,
		info.CTime,
		info.Nlink,
	))
}

func summariseWriteStatsSortOutputLineIfNeeded(
	bw *bufio.Writer,
	record *summariseStatsSortRecord,
	state *summariseStatsSortMergeState,
) error {
	if record.isDir() {
		if state.hasLastDirKey && state.lastDirKey == record.key {
			return nil
		}

		state.lastDirKey = record.key
		state.hasLastDirKey = true
	}

	return summariseWriteStatsSortOutputLine(bw, record.line)
}

type summariseStatsSortScanState struct {
	chunks     []string
	records    []summariseStatsSortRecord
	chunkBytes int
	seq        uint64
}

func (s *summariseStatsSortScanState) add(scannerLine []byte, mountPath string) {
	records := summariseStatsSortRecordsForLine(scannerLine, mountPath, s.seq)

	for _, record := range records {
		s.records = append(s.records, record)
		s.chunkBytes += len(record.key) + len(record.line)
	}

	s.seq++
}

func summariseStatsSortRecordsForLine(
	scannerLine []byte,
	mountPath string,
	seq uint64,
) []summariseStatsSortRecord {
	line := slices.Clone(scannerLine)
	record := summariseStatsSortRecord{key: summariseStatsSortKey(line), line: line, seq: seq}

	info, ok := summariseStatsSortParseLine(line)
	if !ok {
		return []summariseStatsSortRecord{record}
	}

	if info.EntryType == stats.DirType {
		dirPath := summariseEnsureTrailingSlash(string(info.Path))
		record.key = summariseStatsSortDirPathKey(dirPath)
		record.line = summariseStatsSortDirRecordLine(line, info)
		record.kind = summariseStatsSortRecordExplicitDir
	} else {
		record.key = summariseStatsSortFilePathKey(string(info.Path))
	}

	records := summariseStatsSortSyntheticDirRecords(info, mountPath, seq)
	records = append(records, record)

	return records
}

type summariseStatsSortMergeState struct {
	lastDirKey    string
	hasLastDirKey bool
}

func summariseWriteSortedStatsFileForMount(statsPath string, scratchDir string, mountPath string) (string, error) {
	if err := os.RemoveAll(scratchDir); err != nil {
		return "", err
	}

	if err := os.MkdirAll(scratchDir, summariseDirPerm); err != nil {
		return "", err
	}

	chunks, err := summariseSortStatsChunks(statsPath, scratchDir, mountPath)
	if err != nil {
		return "", err
	}

	outPath := filepath.Join(scratchDir, summariseStatsSortOutputName)
	if err := summariseMergeStatsSortChunks(chunks, outPath); err != nil {
		return "", err
	}

	return outPath, nil
}

func summariseStatsSortDirRecordLine(line []byte, info stats.FileInfo) []byte {
	if len(info.Path) == 0 || info.Path[len(info.Path)-1] == '/' {
		return line
	}

	idx := bytes.IndexByte(line, '\t')
	if idx < 0 {
		return line
	}

	out := make([]byte, 0, len(line)+1)
	out = strconv.AppendQuote(out, summariseEnsureTrailingSlash(string(info.Path)))
	out = append(out, line[idx:]...)

	return out
}

func summariseStatsSortFilePathKey(path string) string {
	return summariseStatsSortPathKey(path, summariseStatsSortPathKeyFile)
}

func summariseStatsSortParseLine(line []byte) (stats.FileInfo, bool) {
	data := make([]byte, len(line)+1)
	copy(data, line)
	data[len(data)-1] = '\n'

	parser := stats.NewStatsParser(bytes.NewReader(data))

	var info stats.FileInfo
	if parser.Scan(&info) != nil {
		return stats.FileInfo{}, false
	}

	return info, true
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

func summariseScanStatsSortChunks(scanner *bufio.Scanner, scratchDir string, mountPath string) ([]string, error) {
	state := summariseStatsSortScanState{}
	for scanner.Scan() {
		state.add(scanner.Bytes(), mountPath)

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

func openSortedSummariseStats(statsPath string, scratchDir string, mountPath string) (io.ReadCloser, error) {
	sortedPath, err := summariseWriteSortedStatsFileForMount(statsPath, scratchDir, mountPath)
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
	return summariseWriteSortedStatsFileForMount(statsPath, scratchDir, "")
}

func summariseSortStatsChunks(statsPath string, scratchDir string, mountPath string) (_ []string, err error) {
	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, summariseStatsSortMaxLineBytes), summariseStatsSortMaxLineBytes)

	return summariseScanStatsSortChunks(scanner, scratchDir, mountPath)
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

	state := summariseStatsSortMergeState{}

	for h.Len() > 0 {
		reader, err := popSummariseStatsSortChunkReader(&h)
		if err != nil {
			return err
		}

		if err := summariseWriteStatsSortOutputLineIfNeeded(bw, &reader.record, &state); err != nil {
			return err
		}

		if err := summariseAdvanceStatsSortChunkReader(&h, reader); err != nil {
			return err
		}
	}

	return nil
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

	if diff := cmp.Compare(a.priority(), b.priority()); diff != 0 {
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
	mountPath string,
	sortInput bool,
) (io.ReadCloser, error) {
	if sortInput {
		return openSortedSummariseStats(statsPath, summariseStatsSortScratchDir(partialDir), mountPath)
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
