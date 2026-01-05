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

package datasets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindLatestDatasetDirs(t *testing.T) {
	baseDir := t.TempDir()

	mkDataset := func(name string, requiredFiles ...string) {
		dir := filepath.Join(baseDir, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}

		for _, req := range requiredFiles {
			p := filepath.Join(dir, req)
			if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}
		}
	}

	mkDataset("20240101_alpha", "stats.gz")
	mkDataset("20240102_alpha", "stats.gz")
	mkDataset("20240101_beta", "stats.gz")
	mkDataset("9_delta", "stats.gz")
	mkDataset("10_delta", "stats.gz")
	mkDataset("20240103_beta")                // missing required file
	mkDataset(".hidden_zzz", "stats.gz")      // invalid
	mkDataset("nounderscore", "stats.gz")     // invalid
	mkDataset("_missingversion", "stats.gz")  // invalid
	mkDataset("missingkey_", "stats.gz")      // invalid
	mkDataset("20240104_gamma", "other.file") // missing required

	dirs, err := FindLatestDatasetDirs(baseDir, "stats.gz")
	if err != nil {
		t.Fatalf("FindLatestDatasetDirs: %v", err)
	}

	want := []string{
		filepath.Join(baseDir, "10_delta"),
		filepath.Join(baseDir, "20240101_beta"),
		filepath.Join(baseDir, "20240102_alpha"),
	}

	if len(dirs) != len(want) {
		t.Fatalf("got %d dirs, want %d: %#v", len(dirs), len(want), dirs)
	}

	for i := range want {
		if dirs[i] != want[i] {
			t.Fatalf("got dirs[%d]=%q, want %q (all=%#v)", i, dirs[i], want[i], dirs)
		}
	}
}

func TestIsValidDatasetDirName(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"20240101_a", true},
		{"v_key", true},
		{".hidden_a", false},
		{"no_underscore", true},
		{"nounderscore", false},
		{"_missingversion", false},
		{"missingkey_", false},
		{"", false},
	}

	for _, tc := range cases {
		if got := IsValidDatasetDirName(tc.name); got != tc.ok {
			t.Fatalf("IsValidDatasetDirName(%q)=%v, want %v", tc.name, got, tc.ok)
		}
	}
}
