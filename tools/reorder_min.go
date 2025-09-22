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

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: reorder_min <file.go>\n")
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
