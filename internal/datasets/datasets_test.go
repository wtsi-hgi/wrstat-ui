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
