package dbdirs

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestIsValidDBDir verifies directory name format and required file presence
// checks for IsValidDBDir.
func TestIsValidDBDir(t *testing.T) {
	base := t.TempDir()

	// helper to make a candidate dir
	mk := func(name string, files ...string) (fs.DirEntry, string) {
		dir := filepath.Join(base, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		for _, f := range files {
			p := filepath.Join(dir, f)
			if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
				t.Fatalf("writefile: %v", err)
			}
		}

		entry, err := os.ReadDir(base)
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}

		for _, e := range entry {
			if e.Name() == name {
				return e, dir
			}
		}

		t.Fatalf("direntry not found for %s", name)

		return nil, ""
	}

	// invalid: starts with dot
	eDot, _ := mk(".bad_abc", "req1")
	if IsValidDBDir(eDot, base, "req1") {
		t.Errorf("expected invalid for dot-prefixed dir")
	}

	// invalid: missing underscore
	eNoUnderscore, _ := mk("20250101bad", "req1")
	if IsValidDBDir(eNoUnderscore, base, "req1") {
		t.Errorf("expected invalid for name without underscore")
	}

	// invalid: not a dir simulated by file entryExists is used only after dir check
	file := filepath.Join(base, "20250101_ok")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("writefile: %v", err)
	}

	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatalf("readdir base: %v", err)
	}

	var fileEntry fs.DirEntry

	for _, e := range entries {
		if e.Name() == filepath.Base(file) {
			fileEntry = e

			break
		}
	}

	if fileEntry == nil {
		t.Fatalf("expected file entry")
	}

	if IsValidDBDir(fileEntry, base, "req1") {
		t.Errorf("expected invalid for non-directory entry")
	}

	// valid if required files exist
	eGood, goodPath := mk("20250101_runA", "req1", "req2")
	if !IsValidDBDir(eGood, base, "req1", "req2") {
		t.Errorf("expected valid dir when required files present")
	}

	// invalid if a required file is missing
	eMissing, _ := mk("20250102_runB", "req1")
	if IsValidDBDir(eMissing, base, "req1", "req2") {
		t.Errorf("expected invalid when a required file is missing")
	}

	// sanity: regex matches expected pattern
	if !validDBDir.MatchString(filepath.Base(goodPath)) {
		t.Errorf("regex should match %s", filepath.Base(goodPath))
	}
}

// TestEntryExists covers positive and negative cases.
func TestEntryExists(t *testing.T) {
	base := t.TempDir()

	p := filepath.Join(base, "a")
	if EntryExists(p) {
		t.Fatalf("expected false when path does not exist")
	}

	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatalf("writefile: %v", err)
	}

	if !EntryExists(p) {
		t.Fatalf("expected true when path exists")
	}
}
