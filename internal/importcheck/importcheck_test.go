package importcheck

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoExternalBBoltImports(t *testing.T) {
	root := moduleRoot(t)

	var offenders []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}

			return nil
		}

		if filepath.Ext(path) != ".go" {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}

		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "bolt/") {
			return nil
		}

		imports, parseErr := parseImports(path)
		if parseErr != nil {
			return parseErr
		}

		for _, imp := range imports {
			if imp == "go.etcd.io/bbolt" {
				offenders = append(offenders, rel)

				break
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	if len(offenders) > 0 {
		t.Fatalf("go.etcd.io/bbolt imported outside bolt/: %v", offenders)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}

		next := filepath.Dir(dir)
		if next == dir {
			t.Fatalf("go.mod not found from %s", dir)
		}

		dir = next
	}
}

func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "vendor", "node_modules", "analytics":
		return true
	default:
		return false
	}
}

func parseImports(path string) ([]string, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}

	imports := make([]string, 0, len(file.Imports))
	for _, imp := range file.Imports {
		imports = append(imports, strings.Trim(imp.Path.Value, "\""))
	}

	return imports, nil
}
