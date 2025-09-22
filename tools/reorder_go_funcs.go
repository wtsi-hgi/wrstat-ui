package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
)

var dry = flag.Bool("dry", true, "dry run (default true)")

// Minimal, single-file dry-run tool that preserves original byte ranges for functions.
func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: reorder_go_funcs <file.go>\n")
		os.Exit(2)
	}
	fn := args[0]
	fset := token.NewFileSet()
	src, err := os.ReadFile(fn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}
	file, err := parser.ParseFile(fset, fn, src, parser.ParseComments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
		os.Exit(1)
	}

	type block struct{ key string; start, end int }
	var blocks []block
	nameByIdent := map[string]string{}
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok {
			key := fd.Name.Name
			nameByIdent[key] = key
			start := fset.Position(fd.Pos()).Offset
			end := fset.Position(fd.End()).Offset
			if fd.Doc != nil { start = fset.Position(fd.Doc.Pos()).Offset }
			blocks = append(blocks, block{key: key, start: start, end: end})
		}
	}
	if len(blocks) <= 1 { fmt.Println("nothing to do"); return }

	// build call graph
	adj := map[string]map[string]struct{}{}
	indeg := map[string]int{}
	for _, b := range blocks { adj[b.key] = map[string]struct{}{}; indeg[b.key] = 0 }
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
			caller := fd.Name.Name
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ce, ok := n.(*ast.CallExpr); ok {
					switch fun := ce.Fun.(type) {
					case *ast.Ident:
						if callee, ok := nameByIdent[fun.Name]; ok && caller != callee {
							if _, ex := adj[caller][callee]; !ex { adj[caller][callee] = struct{}{}; indeg[callee]++ }
						}
					case *ast.SelectorExpr:
						if callee, ok := nameByIdent[fun.Sel.Name]; ok && caller != callee {
							if _, ex := adj[caller][callee]; !ex { adj[caller][callee] = struct{}{}; indeg[callee]++ }
						}
					}
				}
				return true
			})
		}
	}

	// Kahn
	zero := []string{}
	for k, v := range indeg { if v == 0 { zero = append(zero, k) } }
	sort.Strings(zero)
	var order []string
	for len(zero) > 0 {
		n := zero[0]
		zero = zero[1:]
		order = append(order, n)
		for neigh := range adj[n] { indeg[neigh]--; if indeg[neigh] == 0 { zero = append(zero, neigh) } }
		sort.Strings(zero)
	}
	if len(order) != len(blocks) { fmt.Println("cycle or unresolved; skipping"); return }

	// original order
	var orig []string
	for _, b := range blocks { orig = append(orig, b.key) }
	if equalStringSlices(orig, order) { fmt.Println("already ordered"); return }

	fmt.Printf("file: %s\n", fn)
	fmt.Printf("  original order:\n")
	for i, k := range orig { fmt.Printf("    %2d: %s\n", i, k) }
	fmt.Printf("  proposed order:\n")
	for i, k := range order { fmt.Printf("    %2d: %s\n", i, k) }

	fmt.Printf("\nByte ranges (start..end) for functions (preserved verbatim):\n")
	for _, b := range blocks { fmt.Printf("  %s: %d..%d\n", b.key, b.start, b.end) }

	if !*dry {
		fmt.Println("(writing files not enabled in minimal tool)")
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) { return false }
	for i := range a { if a[i] != b[i] { return false } }
	return true
}

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	dry    = flag.Bool("dry", false, "Do not write files; just show what would change")
	report = flag.Bool("report", false, "Report caller-after-callee occurrences (and do not write unless -dry is false)")
)

func main() {
	flag.Parse()
	fset := token.NewFileSet()

	files := collectGoFiles(flag.Args())

	for _, fn := range files {
		// skip the tool itself
		if filepath.Clean(fn) == filepath.Clean("tools/reorder_go_funcs.go") {
			continue
		}

		if err := processFile(fset, fn); err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", fn, err)
		}
	}
}

func collectGoFiles(args []string) []string {
	var files []string
	if len(args) > 0 {
		for _, a := range args {
			if fi, err := os.Stat(a); err == nil {
				if fi.IsDir() {
					filepath.WalkDir(a, func(path string, d fs.DirEntry, err error) error {
						if err != nil {
							return nil
						}
						if d.IsDir() {
							base := filepath.Base(path)
							if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
								return filepath.SkipDir
							}
							return nil
						}
						if strings.HasSuffix(path, ".go") {
							files = append(files, path)
						}
						return nil
					})
				} else {
					files = append(files, a)
				}
			}
		}
	} else {
		filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				base := filepath.Base(path)
				if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				files = append(files, path)
			}
			return nil
		})
	}
	return files
}

func processFile(fset *token.FileSet, fn string) error {
	src, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	file, err := parser.ParseFile(fset, fn, src, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// collect funcs and their original byte ranges (including doc comments)
	type funcBlock struct{ key string; start, end int; src []byte }
	var blocks []funcBlock
	funcIndex := make(map[string]int)
	var keyByDecl []string
	funcsByName := make(map[string]string)
	var funcDecls []*ast.FuncDecl

	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok {
			key := funcKey(fd)
			funcIndex[key] = len(funcIndex)
			keyByDecl = append(keyByDecl, key)
			funcsByName[fd.Name.Name] = key
			funcDecls = append(funcDecls, fd)

			var startPos token.Pos
			if fd.Doc != nil {
				startPos = fd.Doc.Pos()
			} else {
				startPos = fd.Pos()
			}
			start := fset.Position(startPos).Offset
			end := fset.Position(fd.End()).Offset
			if start < 0 {
				start = 0
			}
			if end > len(src) {
				end = len(src)
			}
			b := make([]byte, end-start)
			copy(b, src[start:end])
			blocks = append(blocks, funcBlock{key: key, start: start, end: end, src: b})
		}
	}

	if len(blocks) <= 1 {
		return nil
	}

	// build call edges (file-local matching only)
	adj := make(map[string]map[string]struct{})
	indegree := make(map[string]int)
	for _, k := range keyByDecl {
		adj[k] = make(map[string]struct{})
		indegree[k] = 0
	}

	for _, fd := range funcDecls {
		caller := funcKey(fd)
		if fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			ce, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := ce.Fun.(type) {
			case *ast.Ident:
				if callee, ok := funcsByName[fun.Name]; ok {
					addEdge(adj, indegree, caller, callee)
				}
			case *ast.SelectorExpr:
				if callee, ok := funcsByName[fun.Sel.Name]; ok {
					addEdge(adj, indegree, caller, callee)
				}
			}
			return true
		})
	}

	order, ok := topoSort(keysFromMap(indegree), adj, indegree, funcIndex)
	if !ok {
		// cycle or unresolved; skip
		return nil
	}

	origKeys := keyByDecl
	if equalStringSlices(origKeys, order) {
		return nil
	}

	fmt.Printf("file: %s\n", fn)
	fmt.Printf("  original order:\n")
	for i, k := range origKeys {
		fmt.Printf("    %2d: %s\n", i, k)
	}
	fmt.Printf("  proposed order:\n")
	for i, k := range order {
		fmt.Printf("    %2d: %s\n", i, k)
	}

	if *dry || *report {
		return nil
	}

	// remove original func ranges from src (descending start index)
	keyToBlock := make(map[string]funcBlock)
	for _, b := range blocks {
		keyToBlock[b.key] = b
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].start > blocks[j].start })
	base := make([]byte, len(src))
	copy(base, src)
	for _, r := range blocks {
		if r.start < 0 || r.end > len(base) || r.start >= r.end {
			continue
		}
		base = append(base[:r.start], base[r.end:]...)
	}
	base = bytesTrimTrailingSpacesAndEnsureNewline(base)

	var out bytes.Buffer
	out.Write(base)
	out.WriteString("\n")
	for _, k := range order {
		if b, ok := keyToBlock[k]; ok {
			out.Write(b.src)
			out.WriteString("\n\n")
		}
	}

	bak := fn + ".bak"
	if err := os.WriteFile(bak, src, 0644); err != nil {
		return fmt.Errorf("failed to write backup %s: %w", bak, err)
	}
	if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil {
		// try to restore
		_ = os.WriteFile(fn, src, 0644)
		return fmt.Errorf("failed to write %s: %w", fn, err)
	}
	fmt.Printf("  wrote %s (backup %s)\n", fn, bak)
	return nil
}

func bytesTrimTrailingSpacesAndEnsureNewline(b []byte) []byte {
	i := len(b)
	for i > 0 {
		ch := b[i-1]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i--
			continue
		}
		break
	}
	nb := make([]byte, i)
	copy(nb, b[:i])
	return append(nb, '\n')
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	var rt string
	switch expr := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := expr.X.(*ast.Ident); ok {
			rt = id.Name
		} else if se, ok := expr.X.(*ast.SelectorExpr); ok {
			rt = typeString(se)
		}
	case *ast.Ident:
		rt = expr.Name
	case *ast.SelectorExpr:
		rt = typeString(expr)
	default:
		rt = "recv"
	}
	return rt + "." + fd.Name.Name
}

func typeString(se *ast.SelectorExpr) string {
	if id, ok := se.X.(*ast.Ident); ok {
		return id.Name + "." + se.Sel.Name
	}
	return "pkg." + se.Sel.Name
}

func addEdge(adj map[string]map[string]struct{}, indeg map[string]int, caller, callee string) {
	if caller == callee {
		return
	}
	if _, ok := adj[caller]; !ok {
		adj[caller] = make(map[string]struct{})
	}
	if _, ok := adj[caller][callee]; ok {
		return
	}
	adj[caller][callee] = struct{}{}
	indeg[callee]++
}

func keysFromMap(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func topoSort(nodes []string, adj map[string]map[string]struct{}, indeg map[string]int, origIndex map[string]int) ([]string, bool) {
	indegree := make(map[string]int, len(indeg))
	for k, v := range indeg {
		indegree[k] = v
	}
	zero := make([]string, 0)
	for _, n := range nodes {
		if indegree[n] == 0 {
			zero = append(zero, n)
		}
	}
	if origIndex == nil {
		origIndex = make(map[string]int)
		for i, k := range nodes {
			origIndex[k] = i
		}
	}
	sort.Slice(zero, func(i, j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	var out []string
	for len(zero) > 0 {
		n := zero[0]
		zero = zero[1:]
		out = append(out, n)
		for neigh := range adj[n] {
			indegree[neigh]--
			if indegree[neigh] == 0 {
				zero = append(zero, neigh)
			}
		}
		sort.Slice(zero, func(i, j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	}
	if len(out) != len(nodes) {
		return nil, false
	}
	return out, true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	dry    = flag.Bool("dry", false, "Do not write files; just show what would change")
	report = flag.Bool("report", false, "Report caller-after-callee occurrences (and do not write unless -dry is false)")
)

func main() {
	flag.Parse()
	fset := token.NewFileSet()

	var files []string
	if flag.NArg() > 0 {
		for _, a := range flag.Args() {
			if fi, err := os.Stat(a); err == nil {
				if fi.IsDir() {
					filepath.WalkDir(a, func(path string, d fs.DirEntry, err error) error {
						if err != nil { return nil }
						if d.IsDir() {
							base := filepath.Base(path)
							if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
								return filepath.SkipDir
							}
							return nil
						}
						if strings.HasSuffix(path, ".go") { files = append(files, path) }
						return nil
					})
				} else {
					files = append(files, a)
				}
			}
		}
	} else {
		filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			if err != nil { return nil }
			if d.IsDir() {
				base := filepath.Base(path)
				if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") { files = append(files, path) }
			return nil
		})
	}

	for _, fn := range files {
		// skip the tool itself
		if filepath.Clean(fn) == filepath.Clean("tools/reorder_go_funcs.go") { continue }

		src, err := os.ReadFile(fn)
		if err != nil { fmt.Fprintf(os.Stderr, "skip %s: read error: %v\n", fn, err); continue }

		file, err := parser.ParseFile(fset, fn, src, parser.ParseComments)
		if err != nil { fmt.Fprintf(os.Stderr, "skip %s: parse error: %v\n", fn, err); continue }

		// collect funcs and their source blocks (including doc comments)
		type funcBlock struct{ key string; start, end int; src []byte }
		var blocks []funcBlock
		funcIndex := make(map[string]int)
		var keyByDecl []string
		funcsByName := make(map[string]string)
		var funcDecls []*ast.FuncDecl

		for _, decl := range file.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok {
				key := funcKey(fd)
				funcIndex[key] = len(funcIndex)
				keyByDecl = append(keyByDecl, key)
				funcsByName[fd.Name.Name] = key
				funcDecls = append(funcDecls, fd)

				var startPos token.Pos
				if fd.Doc != nil { startPos = fd.Doc.Pos() } else { startPos = fd.Pos() }
				start := fset.Position(startPos).Offset
				end := fset.Position(fd.End()).Offset
				if start < 0 { start = 0 }
				if end > len(src) { end = len(src) }
				b := make([]byte, end-start)
				copy(b, src[start:end])
				blocks = append(blocks, funcBlock{key:key, start:start, end:end, src:b})
			}
		}

		if len(blocks) <= 1 { continue }

		// build call edges (file-local matching only)
		adj := make(map[string]map[string]struct{})
		indegree := make(map[string]int)
		for _, k := range keyByDecl { adj[k] = make(map[string]struct{}); indegree[k]=0 }

		for _, fd := range funcDecls {
			caller := funcKey(fd)
			if fd.Body == nil { continue }
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok { return true }
				switch fun := ce.Fun.(type) {
				case *ast.Ident:
					if callee, ok := funcsByName[fun.Name]; ok { addEdge(adj, indegree, caller, callee) }
				case *ast.SelectorExpr:
					if callee, ok := funcsByName[fun.Sel.Name]; ok { addEdge(adj, indegree, caller, callee) }
				}
				return true
			})
		}

		order, ok := topoSort(keysFromMap(indegree), adj, indegree, funcIndex)
		if !ok { continue }

		origKeys := keyByDecl
		if equalStringSlices(origKeys, order) { continue }

		fmt.Printf("file: %s\n", fn)
		fmt.Printf("  original order:\n")
		for i,k := range origKeys { fmt.Printf("    %2d: %s\n", i, k) }
		fmt.Printf("  proposed order:\n")
		for i,k := range order { fmt.Printf("    %2d: %s\n", i, k) }

		if *dry || *report { continue }

		// remove original func ranges from src
		keyToBlock := make(map[string]funcBlock)
		for _, b := range blocks { keyToBlock[b.key] = b }
		// sort desc by start to remove safely
		sort.Slice(blocks, func(i,j int) bool { return blocks[i].start > blocks[j].start })
		base := make([]byte, len(src)); copy(base, src)
		for _, r := range blocks {
			if r.start < 0 || r.end > len(base) || r.start >= r.end { continue }
			base = append(base[:r.start], base[r.end:]...)
		}
		base = bytesTrimTrailingSpacesAndEnsureNewline(base)

		var out bytes.Buffer
		out.Write(base)
		out.WriteString("\n")
		for _, k := range order {
			if b, ok := keyToBlock[k]; ok {
				out.Write(b.src)
				out.WriteString("\n\n")
			}
		}

		bak := fn + ".bak"
		if err := os.WriteFile(bak, src, 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write backup %s: %v\n", bak, err); continue }
		if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", fn, err); os.WriteFile(fn, src, 0644); continue }
		fmt.Printf("  wrote %s (backup %s)\n", fn, bak)
	}
}

func bytesTrimTrailingSpacesAndEnsureNewline(b []byte) []byte {
	i := len(b)
	for i>0 {
		ch := b[i-1]
		if ch==' '||ch=='\t'||ch=='\n'||ch=='\r' { i--; continue }
		break
	}
	nb := make([]byte,i); copy(nb,b[:i])
	return append(nb,'\n')
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv==nil || len(fd.Recv.List)==0 { return fd.Name.Name }
	var rt string
	switch expr := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := expr.X.(*ast.Ident); ok { rt = id.Name } else if se, ok := expr.X.(*ast.SelectorExpr); ok { rt = typeString(se) }
	case *ast.Ident:
		rt = expr.Name
	case *ast.SelectorExpr:
		rt = typeString(expr)
	default:
		rt = "recv"
	}
	return rt+"."+fd.Name.Name
}

func typeString(se *ast.SelectorExpr) string {
	if id, ok := se.X.(*ast.Ident); ok { return id.Name+"."+se.Sel.Name }
	return "pkg."+se.Sel.Name
}

func addEdge(adj map[string]map[string]struct{}, indeg map[string]int, caller, callee string) {
	if caller==callee { return }
	if _, ok := adj[caller]; !ok { adj[caller] = make(map[string]struct{}) }
	if _, ok := adj[caller][callee]; ok { return }
	adj[caller][callee] = struct{}{}
	indeg[callee]++
}

func keysFromMap(m map[string]int) []string { ks := make([]string,0,len(m)); for k := range m { ks = append(ks, k) }; return ks }

func topoSort(nodes []string, adj map[string]map[string]struct{}, indeg map[string]int, origIndex map[string]int) ([]string,bool) {
	indegree := make(map[string]int, len(indeg))
	for k,v := range indeg { indegree[k] = v }
	zero := make([]string,0)
	for _, n := range nodes { if indegree[n] == 0 { zero = append(zero, n) } }
	if origIndex == nil { origIndex = make(map[string]int); for i,k := range nodes { origIndex[k] = i } }
	sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	var out []string
	for len(zero) > 0 {
		n := zero[0]; zero = zero[1:]; out = append(out, n)
		for neigh := range adj[n] { indegree[neigh]--; if indegree[neigh] == 0 { zero = append(zero, neigh) } }
		sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	}
	if len(out) != len(nodes) { return nil, false }
	return out, true
}

func equalStringSlices(a,b []string) bool { if len(a) != len(b) { return false }; for i:=range a { if a[i] != b[i] { return false } }; return true }
							}
							return nil
						}
						if strings.HasSuffix(path, ".go") { files = append(files, path) }
						return nil
					})
				} else {
					files = append(files, a)
				}
			}
		}
	} else {
		filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			if err != nil { return nil }
			if d.IsDir() {
				base := filepath.Base(path)
				if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") { files = append(files, path) }
			return nil
		})
	}

	for _, fn := range files {
		// skip the tool itself
		if filepath.Clean(fn) == filepath.Clean("tools/reorder_go_funcs.go") { continue }

		src, err := os.ReadFile(fn)
		if err != nil { fmt.Fprintf(os.Stderr, "skip %s: read error: %v\n", fn, err); continue }

		file, err := parser.ParseFile(fset, fn, src, parser.ParseComments)
		if err != nil { fmt.Fprintf(os.Stderr, "skip %s: parse error: %v\n", fn, err); continue }

		// collect funcs and their source blocks (including doc comments)
		type funcBlock struct { key string; start, end int; src []byte }
		var blocks []funcBlock
		funcIndex := make(map[string]int)
		var keyByDecl []string
		funcsByName := make(map[string]string)
		var funcDecls []*ast.FuncDecl

		for _, decl := range file.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok {
				key := funcKey(fd)
				funcIndex[key] = len(funcIndex)
				keyByDecl = append(keyByDecl, key)
				funcsByName[fd.Name.Name] = key
				funcDecls = append(funcDecls, fd)

				var startPos token.Pos
				if fd.Doc != nil { startPos = fd.Doc.Pos() } else { startPos = fd.Pos() }
				start := fset.Position(startPos).Offset
				end := fset.Position(fd.End()).Offset
				if start < 0 { start = 0 }
				if end > len(src) { end = len(src) }
				b := make([]byte, end-start)
				copy(b, src[start:end])
				blocks = append(blocks, funcBlock{key:key, start:start, end:end, src:b})
			}
		}

		if len(blocks) <= 1 { continue }

		// build call edges (file-local matching only)
		adj := make(map[string]map[string]struct{})
		indegree := make(map[string]int)
		for _, k := range keyByDecl { adj[k] = make(map[string]struct{}); indegree[k]=0 }

		for _, fd := range funcDecls {
			caller := funcKey(fd)
			if fd.Body == nil { continue }
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok { return true }
				switch fun := ce.Fun.(type) {
				case *ast.Ident:
					if callee, ok := funcsByName[fun.Name]; ok { addEdge(adj, indegree, caller, callee) }
				case *ast.SelectorExpr:
					if callee, ok := funcsByName[fun.Sel.Name]; ok { addEdge(adj, indegree, caller, callee) }
				}
				return true
			})
		}

		order, ok := topoSort(keysFromMap(indegree), adj, indegree, funcIndex)
		if !ok { continue }

		origKeys := keyByDecl
		if equalStringSlices(origKeys, order) { continue }

		fmt.Printf("file: %s\n", fn)
		fmt.Printf("  original order:\n")
		for i,k := range origKeys { fmt.Printf("    %2d: %s\n", i, k) }
		fmt.Printf("  proposed order:\n")
		for i,k := range order { fmt.Printf("    %2d: %s\n", i, k) }

		if *dry || *report { continue }

		// remove original func ranges from src
		keyToBlock := make(map[string]funcBlock)
		for _, b := range blocks { keyToBlock[b.key] = b }
		// sort desc by start to remove safely
		sort.Slice(blocks, func(i,j int) bool { return blocks[i].start > blocks[j].start })
		base := make([]byte, len(src)); copy(base, src)
		for _, r := range blocks {
			if r.start < 0 || r.end > len(base) || r.start >= r.end { continue }
			base = append(base[:r.start], base[r.end:]...)
		}
		base = bytesTrimTrailingSpacesAndEnsureNewline(base)

		var out bytes.Buffer
		out.Write(base)
		out.WriteString("\n")
		for _, k := range order {
			if b, ok := keyToBlock[k]; ok {
				out.Write(b.src)
				out.WriteString("\n\n")
			}
		}

		bak := fn + ".bak"
		if err := os.WriteFile(bak, src, 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write backup %s: %v\n", bak, err); continue }
		if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", fn, err); os.WriteFile(fn, src, 0644); continue }
		fmt.Printf("  wrote %s (backup %s)\n", fn, bak)
	}
}

func bytesTrimTrailingSpacesAndEnsureNewline(b []byte) []byte {
	i := len(b)
	for i>0 {
		ch := b[i-1]
		if ch==' '||ch=='\t'||ch=='\n'||ch=='\r' { i--; continue }
		break
	}
	nb := make([]byte,i); copy(nb,b[:i])
	return append(nb,'\n')
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv==nil || len(fd.Recv.List)==0 { return fd.Name.Name }
	var rt string
	switch expr := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := expr.X.(*ast.Ident); ok { rt = id.Name } else if se, ok := expr.X.(*ast.SelectorExpr); ok { rt = typeString(se) }
	case *ast.Ident:
		rt = expr.Name
	case *ast.SelectorExpr:
		rt = typeString(expr)
	default:
		rt = "recv"
	}
	return rt+"."+fd.Name.Name
}

func typeString(se *ast.SelectorExpr) string {
	if id, ok := se.X.(*ast.Ident); ok { return id.Name+"."+se.Sel.Name }
	return "pkg."+se.Sel.Name
}

func addEdge(adj map[string]map[string]struct{}, indegree map[string]int, caller, callee string) {
	if caller==callee { return }
	if _, ok := adj[caller]; !ok { adj[caller]=make(map[string]struct{}) }
	if _, ok := adj[caller][callee]; ok { return }
	adj[caller][callee]=struct{}{}
	indegree[callee]++
}

func keysFromMap(m map[string]int) []string { ks:=make([]string,0,len(m)); for k:=range m{ ks=append(ks,k) }; return ks }

func topoSort(nodes []string, adj map[string]map[string]struct{}, indeg map[string]int, origIndex map[string]int) ([]string,bool) {
	indegree := make(map[string]int,len(indeg)); for k,v:=range indeg{ indegree[k]=v }
	zero := make([]string,0)
	for _,n:=range nodes{ if indegree[n]==0 { zero=append(zero,n) } }
	sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	var out []string
	for len(zero)>0 {
		n:=zero[0]; zero=zero[1:]; out=append(out,n)
		for neigh:=range adj[n] { indegree[neigh]--; if indegree[neigh]==0 { zero=append(zero,neigh) } }
		sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
	}
	if len(out)!=len(nodes) { return nil,false }
	return out,true
}

func equalStringSlices(a,b []string) bool { if len(a)!=len(b) { return false }; for i:=range a { if a[i]!=b[i] { return false } }; return true }

package main

import (
	"bytes"
	package main

	import (
		"bytes"
		"flag"
		"fmt"
		"go/ast"
		"go/parser"
		"go/token"
		"io/fs"
		"os"
		"path/filepath"
		"sort"
		"strings"
	)

	var (
		dry    = flag.Bool("dry", false, "Do not write files; just show what would change")
		report = flag.Bool("report", false, "Report caller-after-callee occurrences (and do not write unless -dry is false)")
	)

	func main() {
		flag.Parse()

		fset := token.NewFileSet()

		var files []string

		if flag.NArg() > 0 {
			for _, a := range flag.Args() {
				if fi, err := os.Stat(a); err == nil {
					if fi.IsDir() {
						filepath.WalkDir(a, func(path string, d fs.DirEntry, err error) error {
							if err != nil { return nil }
							if d.IsDir() {
								base := filepath.Base(path)
								if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
									return filepath.SkipDir
								}
								return nil
							}
							if strings.HasSuffix(path, ".go") { files = append(files, path) }
							return nil
						})
					} else {
						files = append(files, a)
					}
				}
			}
		} else {
			filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
				if err != nil { return nil }
				if d.IsDir() {
					base := filepath.Base(path)
					if base == ".git" || base == "vendor" || base == "node_modules" || base == "build" {
						return filepath.SkipDir
					}
					return nil
				}
				if strings.HasSuffix(path, ".go") { files = append(files, path) }
				return nil
			})
		}

		for _, fn := range files {
			if filepath.Clean(fn) == filepath.Clean("tools/reorder_go_funcs.go") { continue }

			src, err := os.ReadFile(fn)
			if err != nil { fmt.Fprintf(os.Stderr, "skip %s: read error: %v\n", fn, err); continue }

			file, err := parser.ParseFile(fset, fn, src, parser.ParseComments)
			if err != nil { fmt.Fprintf(os.Stderr, "skip %s: parse error: %v\n", fn, err); continue }

			type funcBlock struct { key string; start, end int; src []byte }
			var blocks []funcBlock
			funcIndex := make(map[string]int)
			var keyByDecl []string
			funcsByName := make(map[string]string)
			var funcDecls []*ast.FuncDecl

			for _, decl := range file.Decls {
				if fd, ok := decl.(*ast.FuncDecl); ok {
					key := funcKey(fd)
					funcIndex[key] = len(funcIndex)
					keyByDecl = append(keyByDecl, key)
					funcsByName[fd.Name.Name] = key
					funcDecls = append(funcDecls, fd)

					var startPos token.Pos
					if fd.Doc != nil { startPos = fd.Doc.Pos() } else { startPos = fd.Pos() }
					start := fset.Position(startPos).Offset
					end := fset.Position(fd.End()).Offset
					if start < 0 { start = 0 }
					if end > len(src) { end = len(src) }
					b := make([]byte, end-start)
					copy(b, src[start:end])
					blocks = append(blocks, funcBlock{key:key, start:start, end:end, src:b})
				}
			}

			if len(blocks) <= 1 { continue }

			adj := make(map[string]map[string]struct{})
			indegree := make(map[string]int)
			for _, k := range keyByDecl { adj[k] = make(map[string]struct{}); indegree[k]=0 }

			for _, fd := range funcDecls {
				caller := funcKey(fd)
				if fd.Body == nil { continue }
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					ce, ok := n.(*ast.CallExpr)
					if !ok { return true }
					switch fun := ce.Fun.(type) {
					case *ast.Ident:
						if callee, ok := funcsByName[fun.Name]; ok { addEdge(adj, indegree, caller, callee) }
					case *ast.SelectorExpr:
						if callee, ok := funcsByName[fun.Sel.Name]; ok { addEdge(adj, indegree, caller, callee) }
					}
					return true
				})
			}

			order, ok := topoSort(keysFromMap(indegree), adj, indegree, funcIndex)
			if !ok { continue }

			origKeys := keyByDecl
			if equalStringSlices(origKeys, order) { continue }

			fmt.Printf("file: %s\n", fn)
			fmt.Printf("  original order:\n")
			for i,k := range origKeys { fmt.Printf("    %2d: %s\n", i, k) }
			fmt.Printf("  proposed order:\n")
			for i,k := range order { fmt.Printf("    %2d: %s\n", i, k) }

			if *dry || *report { continue }

			keyToBlock := make(map[string]funcBlock)
			for _, b := range blocks { keyToBlock[b.key] = b }
			sort.Slice(blocks, func(i,j int) bool { return blocks[i].start > blocks[j].start })
			base := make([]byte, len(src)); copy(base, src)
			for _, r := range blocks {
				if r.start < 0 || r.end > len(base) || r.start >= r.end { continue }
				base = append(base[:r.start], base[r.end:]...)
			}
			base = bytesTrimTrailingSpacesAndEnsureNewline(base)

			var out bytes.Buffer
			out.Write(base)
			out.WriteString("\n")
			for _, k := range order {
				if b, ok := keyToBlock[k]; ok {
					out.Write(b.src)
					out.WriteString("\n\n")
				}
			}

			bak := fn + ".bak"
			if err := os.WriteFile(bak, src, 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write backup %s: %v\n", bak, err); continue }
			if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil { fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", fn, err); os.WriteFile(fn, src, 0644); continue }
			fmt.Printf("  wrote %s (backup %s)\n", fn, bak)
		}
	}

	func bytesTrimTrailingSpacesAndEnsureNewline(b []byte) []byte {
		i := len(b)
		for i>0 {
			ch := b[i-1]
			if ch==' '||ch=='\t'||ch=='\n'||ch=='\r' { i--; continue }
			break
		}
		nb := make([]byte,i); copy(nb,b[:i])
		return append(nb,'\n')
	}

	func funcKey(fd *ast.FuncDecl) string {
		if fd.Recv==nil || len(fd.Recv.List)==0 { return fd.Name.Name }
		var rt string
		switch expr := fd.Recv.List[0].Type.(type) {
		case *ast.StarExpr:
			if id, ok := expr.X.(*ast.Ident); ok { rt = id.Name } else if se, ok := expr.X.(*ast.SelectorExpr); ok { rt = typeString(se) }
		case *ast.Ident:
			rt = expr.Name
		case *ast.SelectorExpr:
			rt = typeString(expr)
		default:
			rt = "recv"
		}
		return rt+"."+fd.Name.Name
	}

	func typeString(se *ast.SelectorExpr) string {
		if id, ok := se.X.(*ast.Ident); ok { return id.Name+"."+se.Sel.Name }
		return "pkg."+se.Sel.Name
	}

	func addEdge(adj map[string]map[string]struct{}, indegree map[string]int, caller, callee string) {
		if caller==callee { return }
		if _, ok := adj[caller]; !ok { adj[caller]=make(map[string]struct{}) }
		if _, ok := adj[caller][callee]; ok { return }
		adj[caller][callee]=struct{}{}
		indegree[callee]++
	}

	func keysFromMap(m map[string]int) []string { ks:=make([]string,0,len(m)); for k:=range m{ ks=append(ks,k) }; return ks }

	func topoSort(nodes []string, adj map[string]map[string]struct{}, indeg map[string]int, origIndex map[string]int) ([]string,bool) {
		indegree := make(map[string]int,len(indeg)); for k,v:=range indeg{ indegree[k]=v }
		zero := make([]string,0)
		for _,n:=range nodes{ if indegree[n]==0 { zero=append(zero,n) } }
		sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
		var out []string
		for len(zero)>0 {
			n:=zero[0]; zero=zero[1:]; out=append(out,n)
			for neigh:=range adj[n] { indegree[neigh]--; if indegree[neigh]==0 { zero=append(zero,neigh) } }
			sort.Slice(zero, func(i,j int) bool { return origIndex[zero[i]] < origIndex[zero[j]] })
		}
		if len(out)!=len(nodes) { return nil,false }
		return out,true
	}

	func equalStringSlices(a,b []string) bool { if len(a)!=len(b) { return false }; for i:=range a { if a[i]!=b[i] { return false } }; return true }
