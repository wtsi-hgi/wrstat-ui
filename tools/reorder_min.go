//go:build tools_min

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
)

var (
	dry   = flag.Bool("dry", true, "dry run (default true)")
	apply = flag.Bool("apply", false, "write changes and create .bak backup (overrides -dry)")
)

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

	type block struct {
		key        string
		start, end int
	}
	var funcBlocks []block
	var nonFuncBlocks []block
	nameByIdent := map[string]string{}
	firstDeclStart := -1
	lastDeclEnd := -1
	for _, decl := range file.Decls {
		s := fset.Position(decl.Pos()).Offset
		e := fset.Position(decl.End()).Offset
		if gd, ok := decl.(*ast.GenDecl); ok {
			if gd.Doc != nil {
				s = fset.Position(gd.Doc.Pos()).Offset
			}
		}
		if fd, ok := decl.(*ast.FuncDecl); ok {
			key := fd.Name.Name
			nameByIdent[key] = key
			if fd.Doc != nil {
				s = fset.Position(fd.Doc.Pos()).Offset
			}
			funcBlocks = append(funcBlocks, block{key: key, start: s, end: e})
		} else {
			nonFuncBlocks = append(nonFuncBlocks, block{key: "", start: s, end: e})
		}
		if firstDeclStart == -1 || s < firstDeclStart {
			firstDeclStart = s
		}
		if e > lastDeclEnd {
			lastDeclEnd = e
		}
	}
	if len(funcBlocks) <= 1 {
		fmt.Println("nothing to do")
		return
	}

	adj := map[string]map[string]struct{}{}
	indeg := map[string]int{}
	for _, b := range funcBlocks {
		adj[b.key] = map[string]struct{}{}
		indeg[b.key] = 0
	}
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
			caller := fd.Name.Name
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ce, ok := n.(*ast.CallExpr); ok {
					switch fun := ce.Fun.(type) {
					case *ast.Ident:
						if callee, ok := nameByIdent[fun.Name]; ok && caller != callee {
							if _, ex := adj[caller][callee]; !ex {
								adj[caller][callee] = struct{}{}
								indeg[callee]++
							}
						}
					case *ast.SelectorExpr:
						if callee, ok := nameByIdent[fun.Sel.Name]; ok && caller != callee {
							if _, ex := adj[caller][callee]; !ex {
								adj[caller][callee] = struct{}{}
								indeg[callee]++
							}
						}
					}
				}
				return true
			})
		}
	}

	zero := []string{}
	for k, v := range indeg {
		if v == 0 {
			zero = append(zero, k)
		}
	}
	sort.Strings(zero)
	var order []string
	for len(zero) > 0 {
		n := zero[0]
		zero = zero[1:]
		order = append(order, n)
		for neigh := range adj[n] {
			indeg[neigh]--
			if indeg[neigh] == 0 {
				zero = append(zero, neigh)
			}
		}
		sort.Strings(zero)
	}
	if len(order) != len(funcBlocks) {
		fmt.Println("cycle or unresolved; skipping")
		return
	}

	var orig []string
	for _, b := range funcBlocks {
		orig = append(orig, b.key)
	}
	if equalStringSlices(orig, order) {
		fmt.Println("already ordered")
		return
	}

	fmt.Printf("file: %s\n", fn)
	fmt.Printf("  original order:\n")
	for i, k := range orig {
		fmt.Printf("    %2d: %s\n", i, k)
	}
	fmt.Printf("  proposed order:\n")
	for i, k := range order {
		fmt.Printf("    %2d: %s\n", i, k)
	}

	fmt.Printf("\nByte ranges (start..end) for functions (preserved verbatim):\n")
	for _, b := range funcBlocks {
		fmt.Printf("  %s: %d..%d\n", b.key, b.start, b.end)
	}

	if *apply && !*dry {
		// build new content: keep file prefix before first decl, then non-funcs in original order,
		// then functions in topo order, then file suffix after last decl.
		buf := &bytes.Buffer{}
		if firstDeclStart > 0 {
			buf.Write(src[:firstDeclStart])
		}
		// write non-funcs
		for i, b := range nonFuncBlocks {
			if i > 0 {
				ensureTrailingNewline(buf)
			}
			buf.Write(src[b.start:b.end])
			ensureSingleBlankLine(buf)
		}
		// map func key to block
		fbByKey := map[string]block{}
		for _, b := range funcBlocks {
			fbByKey[b.key] = b
		}
		for idx, k := range order {
			if len(nonFuncBlocks) == 0 && idx == 0 && buf.Len() == 0 && firstDeclStart > 0 {
				// ensure at least one newline after header
				buf.WriteByte('\n')
			}
			b := fbByKey[k]
			buf.Write(src[b.start:b.end])
			ensureSingleBlankLine(buf)
		}
		// append suffix
		if lastDeclEnd >= 0 && lastDeclEnd < len(src) {
			ensureTrailingNewline(buf)
			buf.Write(src[lastDeclEnd:])
		}

		bak := fn + ".bak"
		if err := os.WriteFile(bak, src, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write backup: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(fn, buf.Bytes(), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write updated file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s (backup %s)\n", fn, bak)
	} else {
		fmt.Println("(dry-run; pass -apply=false -dry=false to write)")
	}
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

// ensureSingleBlankLine ensures exactly one trailing blank line after current buffer content.
func ensureSingleBlankLine(buf *bytes.Buffer) {
	bs := buf.Bytes()
	// count trailing newlines
	n := 0
	for i := len(bs) - 1; i >= 0 && (bs[i] == '\n' || bs[i] == '\r'); i-- {
		if bs[i] == '\n' {
			n++
		}
	}
	if n == 0 {
		buf.WriteByte('\n')
	}
	if n <= 1 {
		buf.WriteByte('\n')
	}
}

// ensureTrailingNewline ensures at least one trailing newline.
func ensureTrailingNewline(buf *bytes.Buffer) {
	bs := buf.Bytes()
	if len(bs) == 0 || bs[len(bs)-1] != '\n' {
		buf.WriteByte('\n')
	}
}
