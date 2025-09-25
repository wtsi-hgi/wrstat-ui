package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
)

var (
	dry = flag.Bool("dry", false, "dry run: print full altered file then summary; without, write in place")
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: reorder <file.go>\n")
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
	type declBlock struct {
		start, end int
		isFunc     bool
		key        string
	}
	var funcBlocks []block
	var declBlocks []declBlock
	nameByIdent := map[string]string{}

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
			declBlocks = append(declBlocks, declBlock{start: s, end: e, isFunc: true, key: key})
		} else {
			declBlocks = append(declBlocks, declBlock{start: s, end: e, isFunc: false})
		}
	}
	if len(funcBlocks) <= 1 {
		fmt.Println("nothing to do")
		return
	}

	// Build adjacency (caller->callee) and reverse (callee->callers)
	adj := map[string]map[string]struct{}{}
	for _, fb := range funcBlocks {
		adj[fb.key] = map[string]struct{}{}
	}
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
			caller := fd.Name.Name
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ce, ok := n.(*ast.CallExpr); ok {
					switch fun := ce.Fun.(type) {
					case *ast.Ident:
						if callee, ok := nameByIdent[fun.Name]; ok && caller != callee {
							adj[caller][callee] = struct{}{}
						}
					case *ast.SelectorExpr:
						if callee, ok := nameByIdent[fun.Sel.Name]; ok && caller != callee {
							adj[caller][callee] = struct{}{}
						}
					}
				}
				return true
			})
		}
	}
	if hasCycle(adj) {
		fmt.Println("cycle or unresolved; skipping")
		return
	}

	// Original and working order
	origOrder := make([]string, 0)
	for _, db := range declBlocks {
		if db.isFunc {
			origOrder = append(origOrder, db.key)
		}
	}
	order := make([]string, len(origOrder))
	copy(order, origOrder)
	pos := map[string]int{}
	for i, k := range order {
		pos[k] = i
	}

	// Build reverse map: callee -> callers
	rev := map[string]map[string]struct{}{}
	for c, m := range adj {
		for g := range m {
			if _, ok := rev[g]; !ok {
				rev[g] = map[string]struct{}{}
			}
			rev[g][c] = struct{}{}
		}
	}

	// Minimal-change: move a callee just after its latest caller when needed
	changed := true
	guard := len(order) * len(order) * 2
	for changed && guard > 0 {
		changed = false
		guard--
		for i := 0; i < len(order); i++ {
			g := order[i]
			callers := rev[g]
			if len(callers) == 0 {
				continue
			}
			maxIdx := -1
			for c := range callers {
				if idx, ok := pos[c]; ok && idx > maxIdx {
					maxIdx = idx
				}
			}
			if maxIdx >= 0 && maxIdx >= pos[g] {
				from := pos[g]
				to := maxIdx + 1
				if to > len(order) {
					to = len(order)
				}
				if to-1 == from {
					continue
				}
				// remove from
				order = append(order[:from], order[from+1:]...)
				if to > from {
					to--
				}
				// insert at to
				order = append(order[:to], append([]string{g}, order[to:]...)...)
				for j, k := range order {
					pos[k] = j
				}
				changed = true
			}
		}
	}

	if equalStringSlices(origOrder, order) {
		fmt.Println("already ordered")
		return
	}

	// Build full altered content: keep non-func blocks and gaps verbatim; replace function decls by new order
	// Map func key to block
	fbByKey := map[string]block{}
	for _, b := range funcBlocks {
		fbByKey[b.key] = b
	}

	out := &bytes.Buffer{}
	if len(declBlocks) > 0 && declBlocks[0].start > 0 {
		out.Write(src[:declBlocks[0].start])
	}
	fIdx := 0
	for i := 0; i < len(declBlocks); i++ {
		db := declBlocks[i]
		if db.isFunc {
			key := order[fIdx]
			fb := fbByKey[key]
			out.Write(src[fb.start:fb.end])
			fIdx++
		} else {
			out.Write(src[db.start:db.end])
		}
		if i+1 < len(declBlocks) {
			gapStart := declBlocks[i].end
			gapEnd := declBlocks[i+1].start
			if gapEnd > gapStart {
				out.Write(src[gapStart:gapEnd])
			}
		}
	}
	if len(declBlocks) > 0 {
		tail := declBlocks[len(declBlocks)-1].end
		if tail < len(src) {
			out.Write(src[tail:])
		}
	}

	if *dry {
		os.Stdout.Write(out.Bytes())
		fmt.Println()
		printSummary(fn, origOrder, order)
		return
	}

	if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write updated file: %v\n", err)
		os.Exit(1)
	}
	printSummary(fn, origOrder, order)
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

func printSummary(fn string, orig, order []string) {
	fmt.Printf("file: %s\n", fn)
	fmt.Printf("  original order:\n")
	for i, k := range orig {
		fmt.Printf("    %2d: %s\n", i, k)
	}
	fmt.Printf("  proposed order:\n")
	for i, k := range order {
		fmt.Printf("    %2d: %s\n", i, k)
	}
}

func hasCycle(adj map[string]map[string]struct{}) bool {
	state := map[string]int{} // 0=unseen,1=visiting,2=done
	var dfs func(string) bool
	dfs = func(u string) bool {
		if state[u] == 1 {
			return true
		}
		if state[u] == 2 {
			return false
		}
		state[u] = 1
		for v := range adj[u] {
			if dfs(v) {
				return true
			}
		}
		state[u] = 2
		return false
	}
	for u := range adj {
		if state[u] == 0 {
			if dfs(u) {
				return true
			}
		}
	}
	return false
}
