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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeMount(t *testing.T) {
	tests := []struct {
		name     string
		mount    string
		expected string
	}{
		{"Empty mount", "", ""},
		{"Mount without trailing slash", "/path/to/mount", "/path/to/mount/"},
		{"Mount with trailing slash", "/path/to/mount/", "/path/to/mount/"},
		{"Root mount", "/", "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeMount(tt.mount)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDirPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"Directory path", "/path/to/dir/", true},
		{"File path", "/path/to/file", false},
		{"Root path without slash", "/", true}, // Root path is considered a directory by IsDirPath implementation
		{"Empty path", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsDirPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitParentAndName(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedParent string
		expectedName   string
	}{
		{"Nested file", "/path/to/file.txt", "/path/to/", "file.txt"},
		{"Nested directory", "/path/to/dir/", "/path/to/", "dir"},
		{"Root", "/", "/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, name := SplitParentAndName(tt.path)
			assert.Equal(t, tt.expectedParent, parent)
			assert.Equal(t, tt.expectedName, name)
		})
	}
}

func TestEscapeCHSingleQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"No quotes", "simple string", "simple string"},
		{"Single quote", "string with ' quote", "string with '' quote"},
		{"Multiple quotes", "string's with 'multiple' quotes", "string''s with ''multiple'' quotes"},
		{"Empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EscapeCHSingleQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeriveExtLower(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		isDir    bool
		expected string
	}{
		{"Directory", "folder", true, ""},
		{"Simple extension", "file.txt", false, "txt"},
		{"Uppercase extension", "file.TXT", false, "txt"},
		{"No extension", "file", false, ""},
		{"Hidden file with extension", ".config", false, ""},
		{"Compound extension - tarball", "archive.tar.gz", false, "tar.gz"},
		{"Compound extension - compressed csv", "data.csv.bz2", false, "csv.bz2"},
		{"Dot in filename, no extension", "version.1", false, "1"},
		{"Double extension", "script.js.map", false, "map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DeriveExtLower(tt.fileName, tt.isDir)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureDir(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"Already has trailing slash", "/path/to/dir/", "/path/to/dir/"},
		{"No trailing slash", "/path/to/dir", "/path/to/dir/"},
		{"Root path", "/", "/"},
		{"Empty string", "", "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureDir(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestForEachAncestor(t *testing.T) {
	tests := []struct {
		name      string
		dir       string
		mountPath string
		expected  []string
	}{
		{
			name:      "Dir equals mount path",
			dir:       "/data/",
			mountPath: "/data/",
			expected:  []string{"/data/"},
		},
		{
			name:      "One level deep",
			dir:       "/data/folder/",
			mountPath: "/data/",
			expected:  []string{"/data/folder/", "/data/"},
		},
		{
			name:      "Multiple levels deep",
			dir:       "/data/folder/subfolder/leaf/",
			mountPath: "/data/",
			expected:  []string{"/data/folder/subfolder/leaf/", "/data/folder/subfolder/", "/data/folder/", "/data/"},
		},
		{
			name:      "Dir outside mount path",
			dir:       "/other/path/",
			mountPath: "/data/",
			expected:  nil,
		},
		{
			name:      "Dir without trailing slash",
			dir:       "/data/folder",
			mountPath: "/data/",
			expected:  []string{"/data/folder/", "/data/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []string
			ForEachAncestor(tt.dir, tt.mountPath, func(a string) bool {
				result = append(result, a)
				return true
			})
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("Early termination", func(t *testing.T) {
		var result []string
		ForEachAncestor("/data/folder/subfolder/", "/data/", func(a string) bool {
			result = append(result, a)
			return len(result) < 2 // Stop after collecting 2 ancestors
		})
		assert.Equal(t, []string{"/data/folder/subfolder/", "/data/folder/"}, result)
	})
}

func TestOpenStatsFile(t *testing.T) {
	t.Run("Read from stdin when file is '-'", func(t *testing.T) {
		reader, modTime, err := OpenStatsFile("-")

		assert.NoError(t, err)
		assert.Equal(t, os.Stdin, reader)
		// Can't directly compare modTime since it's using time.Now()
		assert.False(t, modTime.IsZero())
	})

	t.Run("Open regular file", func(t *testing.T) {
		// Create a temporary file for testing
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "test.txt")

		testData := []byte("test data")
		err := os.WriteFile(tmpFile, testData, 0644)
		require.NoError(t, err)

		// Get file stats for later comparison
		fileInfo, err := os.Stat(tmpFile)
		require.NoError(t, err)

		// Test OpenStatsFile
		reader, modTime, err := OpenStatsFile(tmpFile)
		require.NoError(t, err)

		// Check that modTime matches file's modTime
		assert.Equal(t, fileInfo.ModTime(), modTime)

		// Read and verify content
		buf := make([]byte, len(testData))
		n, err := reader.Read(buf)
		assert.Equal(t, len(testData), n)
		assert.NoError(t, err)
		assert.Equal(t, testData, buf)

		// Close the reader
		err = reader.Close()
		assert.NoError(t, err)
	})

	t.Run("Handle non-existent file", func(t *testing.T) {
		reader, modTime, err := OpenStatsFile("/nonexistent/file.txt")

		assert.Error(t, err)
		assert.Nil(t, reader)
		assert.True(t, modTime.IsZero())
	})
}
