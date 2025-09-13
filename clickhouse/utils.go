/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package clickhouse

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

// typeMap maps stats package entry type bytes to our internal FileType representation.
var typeMap = map[byte]FileType{
	stats.FileType:    FileTypeFile,
	stats.DirType:     FileTypeDir,
	stats.SymlinkType: FileTypeSymlink,
	stats.DeviceType:  FileTypeDevice,
	stats.PipeType:    FileTypePipe,
	stats.SocketType:  FileTypeSocket,
	stats.CharType:    FileTypeChar,
}

// Helper to close multiple resources when wrapping readers.
type readMultiCloser struct {
	r       io.Reader
	closers []io.Closer
}

func (m *readMultiCloser) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *readMultiCloser) Close() error {
	var firstErr error
	for i := len(m.closers) - 1; i >= 0; i-- {
		if err := m.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// OpenStatsFile opens a stats file for reading, handling gzip compression if necessary.
func OpenStatsFile(statsFile string) (io.ReadCloser, time.Time, error) {
	if statsFile == "-" {
		return os.Stdin, time.Now(), nil
	}

	f, err := os.Open(statsFile)
	if err != nil {
		return nil, time.Time{}, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()

		return nil, time.Time{}, err
	}

	if strings.HasSuffix(statsFile, ".gz") {
		zr, err := pgzip.NewReader(f)
		if err != nil {
			_ = f.Close()

			return nil, time.Time{}, err
		}

		return &readMultiCloser{r: zr, closers: []io.Closer{zr, f}}, fi.ModTime(), nil
	}

	return f, fi.ModTime(), nil
}

// MapEntryType converts a stats package entry type byte to our internal FileType.
func MapEntryType(b byte) FileType {
	if ft, ok := typeMap[b]; ok {
		return ft
	}

	return FileTypeUnknown
}

// NormalizeMount ensures a mount path ends with a trailing slash.
func NormalizeMount(m string) string {
	if m == "" {
		return m
	}

	if !strings.HasSuffix(m, "/") {
		return m + "/"
	}

	return m
}

// IsDirPath checks if a path represents a directory (ends with slash).
func IsDirPath(path string) bool {
	return strings.HasSuffix(path, "/")
}

// SplitParentAndName splits a path into its parent directory and name components.
func SplitParentAndName(path string) (parent, name string) {
	// Treat directories as having trailing slash in the input.
	p := path
	if IsDirPath(p) {
		p = p[:len(p)-1]
	}

	idx := strings.LastIndexByte(p, '/')
	if idx <= 0 {
		// Root-like; parent is "/" and name is remainder
		return "/", p
	}

	return p[:idx+1], p[idx+1:]
}

// ForEachAncestor calls the provided function for each ancestor directory of the path.
func ForEachAncestor(dir, mountPath string, fn func(a string) bool) {
	// dir must be a directory path ending with '/'
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	// Only iterate ancestors at or below mountPath
	if !strings.HasPrefix(dir, mountPath) {
		return
	}

	for {
		if !fn(dir) {
			return
		}

		if dir == mountPath {
			return
		}

		// Get parent directory by truncating after the last slash before the final slash
		lastSlash := strings.LastIndexByte(dir[:len(dir)-1], '/')
		if lastSlash < 0 {
			return
		}

		dir = dir[:lastSlash+1]
	}
}

// EscapeCHSingleQuotes escapes single quotes for embedding string literals
// into ClickHouse queries.
func EscapeCHSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// isCompressedExtension checks if the given extension is a compressed file extension.
func isCompressedExtension(ext string) bool {
	compressExts := map[string]bool{
		"gz": true, "bz2": true, "xz": true,
		"zst": true, "lz4": true, "lz": true, "br": true,
	}

	return compressExts[ext]
}

// handleCompressedExtension processes compound extensions like .tar.gz, .csv.bz2, etc.
func handleCompressedExtension(base string, dot int, ext string) string {
	prevDot := strings.LastIndexByte(base[:dot], '.')
	if prevDot == -1 {
		return ext
	}

	prevExt := strings.ToLower(base[prevDot+1 : dot])
	if prevExt == "" {
		return ext
	}

	return prevExt + "." + ext
}

// DeriveExtLower extracts the lowercase file extension, handling special cases.
func DeriveExtLower(name string, isDir bool) string {
	// Directories and files without extensions return empty string
	if isDir {
		return ""
	}

	// Hidden files: ignore leading dot for ext purposes
	base := strings.TrimPrefix(name, ".")

	dot := strings.LastIndexByte(base, '.')
	if dot == -1 {
		return ""
	}

	ext := strings.ToLower(base[dot+1:])

	// If not a compressed extension, return as is
	if !isCompressedExtension(ext) {
		return ext
	}

	// Handle compound extensions for compressed files
	return handleCompressedExtension(base, dot, ext)
}

// EnsureDir ensures a path ends with a trailing slash.
func EnsureDir(path string) string {
	if !strings.HasSuffix(path, "/") {
		return path + "/"
	}

	return path
}
