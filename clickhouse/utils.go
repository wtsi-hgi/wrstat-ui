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
	"path"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

// fileTypeMapping maps stats package entry type bytes to our internal FileType representation.
func fileTypeMapping() map[byte]FileType {
	return map[byte]FileType{
		stats.FileType:    FileTypeFile,
		stats.DirType:     FileTypeDir,
		stats.SymlinkType: FileTypeSymlink,
		stats.DeviceType:  FileTypeDevice,
		stats.PipeType:    FileTypePipe,
		stats.SocketType:  FileTypeSocket,
		stats.CharType:    FileTypeChar,
	}
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
	if ft, ok := fileTypeMapping()[b]; ok {
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
func SplitParentAndName(p string) (parent, name string) {
	// Root directory
	if p == "/" {
		return "/", ""
	}

	// Remove trailing slash for directory inputs to get a stable base component
	pp := strings.TrimSuffix(p, "/")
	if pp == "" {
		// Input was "/" or equivalent
		return "/", ""
	}

	dir := path.Dir(pp)
	name = path.Base(pp)

	// Ensure parent ends with a trailing slash
	if dir == "/" || dir == "." {
		parent = "/"
	} else {
		parent = dir + "/"
	}

	return parent, name
}

// ForEachAncestor calls the provided function for each ancestor directory of the path.
// It yields the directory itself (ensured to end with '/'), then walks up to and including mountPath.
func ForEachAncestor(dir, mountPath string, fn func(a string) bool) {
	d := EnsureDir(dir)
	mp := EnsureDir(mountPath)

	// Only iterate ancestors at or below mountPath
	if !strings.HasPrefix(d, mp) {
		return
	}

	for {
		if !fn(d) {
			return
		}

		if d == mp {
			return
		}

		// Trim trailing slash, get parent using slash-separated semantics, and re-ensure '/'
		parent := path.Dir(strings.TrimSuffix(d, "/"))
		if parent == "" || parent == "." {
			return
		}

		d = EnsureDir(parent)
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

// computeDepth returns the directory depth of a path, counting segments.
// Root '/' has depth 0, '/a/' has depth 1, '/a/b/' depth 2, and files
// are measured by their parent directory depth (eg. '/a/b/file' => 2).
func computeDepth(p string) uint16 {
	if p == "/" {
		return 0
	}

	// For files, use parent directory depth
	parent, name := SplitParentAndName(p)
	if name != "" && !IsDirPath(p) {
		p = parent
	}

	s := strings.Trim(p, "/")
	if s == "" {
		return 0
	}

	// Count segments split by '/'
	n := 1

	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			n++
		}
	}

	if n < 0 {
		return 0
	}

	return uint16(n)
}
