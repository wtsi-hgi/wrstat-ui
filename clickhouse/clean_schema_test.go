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

package clickhouse

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const cleanSchemaClickHouseDir = "clickhouse"

func TestCleanSchemaDDLContainsOnlyFinalV1Objects(t *testing.T) {
	Convey("embedded schema SQL has no legacy DDL, migration, or compatibility objects", t, func() {
		files, err := filepath.Glob(filepath.Join(repoRootForCleanSchemaTest(t), cleanSchemaClickHouseDir, "schema", "*.sql"))
		So(err, ShouldBeNil)
		So(files, ShouldNotBeEmpty)

		offenders := filesContainingAny(files, cleanSchemaSQLDenylist())
		So(offenders, ShouldBeEmpty)
	})

	Convey("wrstat_files extension index matches schema-v1 DDL", t, func() {
		src, err := os.ReadFile(filepath.Join(
			repoRootForCleanSchemaTest(t), cleanSchemaClickHouseDir, "schema", "011_files.sql",
		))
		So(err, ShouldBeNil)
		So(string(src), ShouldContainSubstring, "INDEX ext_idx ext TYPE set(256) GRANULARITY 1")
	})

	Convey("wrstat_dir_filter_ageall is a mandatory snapshot-scoped DDL object", t, func() {
		src, err := os.ReadFile(filepath.Join(
			repoRootForCleanSchemaTest(t), cleanSchemaClickHouseDir, "schema", "012_dir_filter_ageall.sql",
		))
		So(err, ShouldBeNil)

		ddl := strings.Join(strings.Fields(string(src)), " ")
		So(ddl, ShouldContainSubstring, "CREATE TABLE IF NOT EXISTS wrstat_dir_filter_ageall")
		So(ddl, ShouldContainSubstring, "PARTITION BY (mount_path, snapshot_id)")
		So(ddl, ShouldContainSubstring, "ORDER BY (mount_path, snapshot_id, gid, uid, ft, dir)")
	})
}

func TestCleanSchemaProductionCodeHasNoLegacyReferences(t *testing.T) {
	Convey("production Go and embedded resources have no clean-schema denylist references", t, func() {
		files := cleanSchemaProductionFiles(t)

		offenders := filesContainingAny(files, cleanSchemaProductionDenylist())
		So(offenders, ShouldBeEmpty)
	})
}

func cleanSchemaProductionFiles(t *testing.T) []string {
	t.Helper()

	root := repoRootForCleanSchemaTest(t)
	files := cleanSchemaProductionGoFiles(t)
	sqlFiles, err := filepath.Glob(filepath.Join(root, cleanSchemaClickHouseDir, "schema", "*.sql"))
	So(err, ShouldBeNil)

	return append(files, sqlFiles...)
}

func TestCleanSchemaReadersDoNotUseRawDGUTA(t *testing.T) {
	Convey("production tree reader SQL does not read raw DGUTA rows", t, func() {
		files := cleanSchemaProductionGoFiles(t)
		readerFiles := make([]string, 0, len(files))

		for _, file := range files {
			base := filepath.Base(file)
			isReaderFile := strings.Contains(base, "database") ||
				strings.Contains(base, "dir") ||
				strings.Contains(base, "provider") ||
				strings.Contains(base, "file_api")

			if isReaderFile {
				readerFiles = append(readerFiles, file)
			}
		}

		offenders := filesContainingAny(readerFiles, []string{"wrstat_dguta"})
		So(offenders, ShouldBeEmpty)
	})

	Convey("non clean-schema tests do not name DGUTA fallback scenarios", t, func() {
		files := cleanSchemaTestFilesOutsideThisFile(t)
		offenders := make([]string, 0)

		for _, file := range files {
			literals, err := stringLiteralsInGoFile(file)
			So(err, ShouldBeNil)

			for _, lit := range literals {
				lower := strings.ToLower(lit)
				if strings.Contains(lower, "wrstat_dguta") && strings.Contains(lower, "fallback") {
					offenders = append(offenders, file)

					break
				}
			}
		}

		So(offenders, ShouldBeEmpty)
	})
}

func TestCleanSchemaRawDGUTAAccumulatorIsImportLocal(t *testing.T) {
	Convey("raw DGUTA import implementation exposes no exported identifiers", t, func() {
		files := []string{filepath.Join(repoRootForCleanSchemaTest(t), cleanSchemaClickHouseDir, "dguta_writer.go")}
		offenders, err := exportedRawDGUTAIdentifiers(files)

		So(err, ShouldBeNil)
		So(offenders, ShouldBeEmpty)
	})

	Convey("reader files do not call raw DGUTA import helpers", t, func() {
		files := cleanSchemaProductionGoFiles(t)
		readerFiles := make([]string, 0, len(files))

		for _, file := range files {
			base := filepath.Base(file)
			if strings.Contains(base, "writer") {
				continue
			}

			readerFiles = append(readerFiles, file)
		}

		offenders := filesContainingAny(readerFiles, []string{"appendRawDGUTA", "rawDGUTA"})
		So(offenders, ShouldBeEmpty)
	})
}

func TestCleanSchemaProductionPathsDoNotDependOnBolt(t *testing.T) {
	Convey("ClickHouse schema-v1 production packages do not depend on Bolt packages", t, func() {
		for _, pattern := range []string{"./clickhouse", "./internal/chperf"} {
			deps, err := goListDeps(pattern)
			So(err, ShouldBeNil)

			offenders := intersectStrings(deps, cleanSchemaForbiddenImportPaths())
			So(offenders, ShouldBeEmpty)
		}
	})

	Convey("ClickHouse command files do not import Bolt packages or call Bolt constructors", t, func() {
		files := cleanSchemaClickHouseCommandFiles(t)
		importOffenders, callOffenders, err := forbiddenBoltUsageInGoFiles(files)

		So(err, ShouldBeNil)
		So(importOffenders, ShouldBeEmpty)
		So(callOffenders, ShouldBeEmpty)
	})
}

func goListDeps(pattern string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "list", "-deps", "-f", "{{.ImportPath}}", pattern)

	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	if filepath.Base(wd) == cleanSchemaClickHouseDir {
		cmd.Dir = filepath.Dir(wd)
	} else {
		cmd.Dir = wd
	}

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}

	return lines, nil
}

func intersectStrings(values, denied []string) []string {
	offenders := make([]string, 0)

	for _, value := range values {
		if slices.Contains(denied, value) {
			offenders = append(offenders, value)
		}
	}

	return offenders
}

func cleanSchemaClickHouseCommandFiles(t *testing.T) []string {
	t.Helper()

	root := repoRootForCleanSchemaTest(t)
	files := make([]string, 0)

	err := filepath.WalkDir(filepath.Join(root, "cmd"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || strings.HasSuffix(path, "_test.go") || !strings.HasSuffix(path, ".go") {
			return nil
		}

		if strings.HasPrefix(filepath.Base(path), "bolt_perf") {
			return nil
		}

		// #nosec G122 -- test scans repo files discovered from a fixed in-repo root.
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		usesClickhouse := bytes.Contains(src, []byte(`"github.com/wtsi-hgi/wrstat-ui/clickhouse"`)) ||
			bytes.Contains(src, []byte("loadClickhouseConfig"))

		if usesClickhouse {
			files = append(files, path)
		}

		return nil
	})
	So(err, ShouldBeNil)

	return files
}

func forbiddenBoltUsageInGoFiles(files []string) ([]string, []string, error) {
	importOffenders := make([]string, 0)
	callOffenders := make([]string, 0)
	fset := token.NewFileSet()

	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return nil, nil, err
		}

		boltAliases := make(map[string]bool)

		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if !slices.Contains(cleanSchemaForbiddenImportPaths(), path) {
				continue
			}

			importOffenders = append(importOffenders, file+": "+path)

			if path == "github.com/wtsi-hgi/wrstat-ui/bolt" {
				alias := filepath.Base(path)
				if imp.Name != nil {
					alias = imp.Name.Name
				}

				boltAliases[alias] = true
			}
		}

		ast.Inspect(parsed, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !slices.Contains(cleanSchemaForbiddenBoltCalls(), sel.Sel.Name) {
				return true
			}

			ident, ok := sel.X.(*ast.Ident)
			if ok && boltAliases[ident.Name] {
				callOffenders = append(callOffenders, file+": "+ident.Name+"."+sel.Sel.Name)
			}

			return true
		})
	}

	return importOffenders, callOffenders, nil
}

func cleanSchemaForbiddenImportPaths() []string {
	return []string{
		"github.com/wtsi-hgi/wrstat-ui/bolt",
		"github.com/wtsi-hgi/wrstat-ui/internal/boltperf",
		"go.etcd.io/bbolt",
	}
}

func cleanSchemaForbiddenBoltCalls() []string {
	return []string{
		"NewDGUTAWriter",
		"NewBaseDirsStore",
		"OpenDatabase",
		"OpenMultiBaseDirsReader",
		"OpenProvider",
	}
}

func cleanSchemaTestFilesOutsideThisFile(t *testing.T) []string {
	t.Helper()

	root := repoRootForCleanSchemaTest(t)
	files := make([]string, 0)

	err := filepath.WalkDir(
		filepath.Join(root, cleanSchemaClickHouseDir),
		func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !d.IsDir() && strings.HasSuffix(path, "_test.go") && filepath.Base(path) != "clean_schema_test.go" {
				files = append(files, path)
			}

			return nil
		})
	So(err, ShouldBeNil)

	return files
}

func exportedRawDGUTAIdentifiers(files []string) ([]string, error) {
	offenders := make([]string, 0)
	fset := token.NewFileSet()

	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}

		ast.Inspect(parsed, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if !ok || !ident.IsExported() || !strings.Contains(ident.Name, "RawDGUTA") {
				return true
			}

			offenders = append(offenders, file+": "+ident.Name)

			return true
		})
	}

	return offenders, nil
}

func cleanSchemaProductionGoFiles(t *testing.T) []string {
	t.Helper()

	root := repoRootForCleanSchemaTest(t)
	files := make([]string, 0)

	for _, dir := range []string{cleanSchemaClickHouseDir, "cmd", "internal"} {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if d.Name() == "boltperf" {
					return filepath.SkipDir
				}

				return nil
			}

			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				files = append(files, path)
			}

			return nil
		})
		So(err, ShouldBeNil)
	}

	return files
}

func repoRootForCleanSchemaTest(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate clean schema test source")
	}

	return filepath.Dir(filepath.Dir(file))
}

func filesContainingAny(files, denylist []string) []string {
	offenders := make([]string, 0)

	for _, file := range files {
		src, err := os.ReadFile(file)
		if err != nil {
			offenders = append(offenders, file+": "+err.Error())

			continue
		}

		for _, denied := range denylist {
			if bytes.Contains(src, []byte(denied)) {
				offenders = append(offenders, file+": "+denied)

				break
			}
		}
	}

	return offenders
}

func stringLiteralsInGoFile(file string) ([]string, error) {
	fset := token.NewFileSet()

	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	literals := make([]string, 0)

	ast.Inspect(parsed, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if ok && lit.Kind == token.STRING {
			literals = append(literals, lit.Value)
		}

		return true
	})

	return literals, nil
}

func cleanSchemaProductionDenylist() []string {
	return []string{
		"wrstat_mounts_active_v2",
		"wrstat_dir_summary",
		"wrstat_dir_dguta_vector",
		"wrstat_dir_filter_index",
		"wrstat_tree_dguta",
		"summary_version",
		"backfill",
		"compatibility",
		"old layout",
		"projection helper",
	}
}

func cleanSchemaSQLDenylist() []string {
	return []string{
		"ALTER TABLE",
		"wrstat_mounts_active_v2",
		"wrstat_dguta",
		"wrstat_dir_summary",
		"wrstat_dir_dguta_vector",
		"wrstat_dir_filter_index",
		"wrstat_dirs",
		"wrstat_tree_dguta",
		"wrstat_tree_children",
		"AggregatingMergeTree",
		"summary_version",
		"backfill",
		"compatibility",
	}
}
