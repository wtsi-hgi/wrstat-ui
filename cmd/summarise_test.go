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

package cmd_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wtsi-hgi/wrstat-ui/cmd"
)

// Tests for helper functions.
func TestNormalizeMount(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", ""},
		{"Already has trailing slash", "/path/to/mount/", "/path/to/mount/"},
		{"Needs trailing slash", "/path/to/mount", "/path/to/mount/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cmd.NormalizeMount(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitParentAndName(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		expectedDir  string
		expectedName string
	}{
		{"Root dir", "/", "/", ""},
		{"File in root", "/file.txt", "/", "/file.txt"},
		{"Dir with trailing slash", "/path/to/dir/", "/path/to/", "dir"},
		{"File in subdir", "/path/to/file.txt", "/path/to/", "file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, name := cmd.SplitParentAndName(tt.path)
			assert.Equal(t, tt.expectedDir, dir)
			assert.Equal(t, tt.expectedName, name)
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
			"Path with ancestors",
			"/mnt/lustre/projects/team/subdir/",
			"/mnt/lustre/",
			[]string{
				"/mnt/lustre/projects/team/subdir/",
				"/mnt/lustre/projects/team/",
				"/mnt/lustre/projects/",
				"/mnt/lustre/",
			},
		},
		{
			"Path at mount",
			"/mnt/lustre/",
			"/mnt/lustre/",
			[]string{"/mnt/lustre/"},
		},
		{
			"Path outside mount",
			"/tmp/other/",
			"/mnt/lustre/",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []string
			cmd.ForEachAncestor(tt.dir, tt.mountPath, func(a string) bool {
				result = append(result, a)

				return true
			})
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeriveExtLower(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		isDir    bool
		expected string
	}{
		{"Directory", "somedir", true, ""},
		{"No extension", "noext", false, ""},
		{"Simple extension", "file.txt", false, "txt"},
		{"Uppercase extension", "file.TXT", false, "txt"},
		{"Double extension compression", "file.tar.gz", false, "tar.gz"},
		{"Double extension noncompression", "file.tar.bam", false, "bam"},
		{"Hidden file", ".hidden", false, ""},
		{"Hidden with ext", ".hidden.txt", false, "txt"},
		{"Multiple dots", "file.name.with.dots.txt", false, "txt"},
		{"Multiple compression", "archive.tar.bz2", false, "tar.bz2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cmd.DeriveExtLower(tt.filename, tt.isDir)
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
		{"Root dir", "/", true},
		{"Subdir with slash", "/path/to/dir/", true},
		{"File path", "/path/to/file.txt", false},
		{"Empty path", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cmd.IsDirPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}
