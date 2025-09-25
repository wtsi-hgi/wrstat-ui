package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

var (
	dry = flag.Bool("dry", false, "dry run: print full altered file then summary; without, write in place")
)

// typeContains recursively checks whether the ast.Expr contains a given identifier name.
func typeContains(t ast.Expr, typeName string) bool {
	if t == nil {
		return false
	}
	switch tt := t.(type) {
	case *ast.Ident:
		return tt.Name == typeName
	case *ast.StarExpr:
		return typeContains(tt.X, typeName)
	case *ast.ArrayType:
		return typeContains(tt.Elt, typeName)
	case *ast.MapType:
		return typeContains(tt.Key, typeName) || typeContains(tt.Value, typeName)
	case *ast.FuncType:
		checkFL := func(fl *ast.FieldList) bool {
			if fl == nil {
				return false
			}
			for _, f := range fl.List {
				if typeContains(f.Type, typeName) {
					return true
				}
			}
			return false
		}
		return checkFL(tt.Params) || checkFL(tt.Results)
	case *ast.IndexExpr:
		return typeContains(tt.X, typeName) || typeContains(tt.Index, typeName)
	case *ast.SelectorExpr:
		return tt.Sel != nil && tt.Sel.Name == typeName
	case *ast.StructType:
		if tt.Fields == nil {
			return false
		}
		for _, f := range tt.Fields.List {
			if typeContains(f.Type, typeName) {
				return true
			}
		}
		return false
	case *ast.InterfaceType:
		if tt.Methods == nil {
			return false
		}
		for _, f := range tt.Methods.List {
			if typeContains(f.Type, typeName) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// usesType checks whether a free function references a type by name in its signature or body.
func usesType(fd *ast.FuncDecl, typeName string) bool {
	hasTypeInFieldList := func(fl *ast.FieldList) bool {
		if fl == nil {
			return false
		}
		for _, f := range fl.List {
			if typeContains(f.Type, typeName) {
				return true
			}
		}
		return false
	}
	if hasTypeInFieldList(fd.Type.Params) || hasTypeInFieldList(fd.Type.Results) {
		return true
	}
	found := false
	if fd.Body != nil {
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == typeName {
				found = true
				return false
			}
			return true
		})
	}
	return found
}

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

	type block struct{ start, end int }
	type funcBlock struct {
		key        string
		start, end int
		recvType   string
		isMethod   bool
	}

	// Collect declarations
	importBlocks := []block{}
	constVarBlocks := []block{}
	typeBlocks := []block{}
	typeNames := []string{}
	typeDeclFor := map[string]block{}

	funcBlocks := []funcBlock{}
	funcByKey := map[string]funcBlock{}

	firstDeclStart := -1
	lastImportEnd := -1
	lastDeclEnd := -1

	// Helper to get receiver type name (base identifier)
	getRecvType := func(fd *ast.FuncDecl) string {
		if fd.Recv == nil || len(fd.Recv.List) == 0 {
			return ""
		}
		t := fd.Recv.List[0].Type
		for {
			switch tt := t.(type) {
			case *ast.StarExpr:
				t = tt.X
				continue
			case *ast.IndexExpr:
				t = tt.X
				continue
			case *ast.Ident:
				return tt.Name
			default:
				return ""
			}
		}
	}

	// Scan decls and build blocks
	for _, decl := range file.Decls {
		s := fset.Position(decl.Pos()).Offset
		e := fset.Position(decl.End()).Offset
		if gd, ok := decl.(*ast.GenDecl); ok {
			if gd.Doc != nil {
				s = fset.Position(gd.Doc.Pos()).Offset
			}
			switch gd.Tok {
			case token.IMPORT:
				importBlocks = append(importBlocks, block{s, e})
				if e > lastImportEnd {
					lastImportEnd = e
				}
			case token.CONST, token.VAR:
				constVarBlocks = append(constVarBlocks, block{s, e})
			case token.TYPE:
				typeBlocks = append(typeBlocks, block{s, e})
				for _, sp := range gd.Specs {
					if ts, ok := sp.(*ast.TypeSpec); ok {
						tn := ts.Name.Name
						typeDeclFor[tn] = block{s, e}
						typeNames = append(typeNames, tn)
					}
				}
			}
		}
		if fd, ok := decl.(*ast.FuncDecl); ok {
			fs := fset.Position(fd.Pos()).Offset
			fe := fset.Position(fd.End()).Offset
			if fd.Doc != nil {
				fs = fset.Position(fd.Doc.Pos()).Offset
			}
			recv := getRecvType(fd)
			key := fd.Name.Name // assume unique in file
			fb := funcBlock{key: key, start: fs, end: fe, recvType: recv, isMethod: recv != ""}
			funcBlocks = append(funcBlocks, fb)
			funcByKey[key] = fb
		}
		if firstDeclStart == -1 || s < firstDeclStart {
			firstDeclStart = s
		}
		if e > lastDeclEnd {
			lastDeclEnd = e
		}
	}

	if len(funcBlocks) <= 0 {
		// nothing to do structurally, but still move const/vars to top if any
	}

	// Build name index for calls: map ident/selector name -> function key
	nameToKey := map[string]string{}
	for _, fb := range funcBlocks {
		nameToKey[fb.key] = fb.key
	}

	// Build call graph (file-local)
	adj := map[string]map[string]struct{}{}
	callersOf := map[string]map[string]struct{}{}
	for _, fb := range funcBlocks {
		adj[fb.key] = map[string]struct{}{}
	}
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		caller := fd.Name.Name
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if ce, ok := n.(*ast.CallExpr); ok {
				switch fun := ce.Fun.(type) {
				case *ast.Ident:
					if callee, ok := nameToKey[fun.Name]; ok && callee != caller {
						adj[caller][callee] = struct{}{}
						if _, ok := callersOf[callee]; !ok {
							callersOf[callee] = map[string]struct{}{}
						}
						callersOf[callee][caller] = struct{}{}
					}
				case *ast.SelectorExpr:
					if callee, ok := nameToKey[fun.Sel.Name]; ok && callee != caller {
						adj[caller][callee] = struct{}{}
						if _, ok := callersOf[callee]; !ok {
							callersOf[callee] = map[string]struct{}{}
						}
						callersOf[callee][caller] = struct{}{}
					}
				}
			}
			return true
		})
	}

	// Detect types declared in this file
	typeSet := map[string]struct{}{}
	for _, tn := range typeNames {
		typeSet[tn] = struct{}{}
	}

	// Constructors by type
	constructors := map[string][]string{}
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv != nil {
			continue
		}
		name := fd.Name.Name
		if !strings.HasPrefix(name, "New") {
			continue
		}
		// results contain target type?
		if fd.Type.Results != nil {
			for _, f := range fd.Type.Results.List {
				t := f.Type
				for {
					switch tt := t.(type) {
					case *ast.StarExpr:
						t = tt.X
						continue
					case *ast.Ident:
						if _, ok := typeSet[tt.Name]; ok {
							constructors[tt.Name] = append(constructors[tt.Name], name)
						}
					}
					break
				}
			}
		}
	}

	// Methods by type
	methods := map[string][]string{}
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv == nil {
			continue
		}
		rt := getRecvType(fd)
		if _, ok := typeSet[rt]; ok {
			methods[rt] = append(methods[rt], fd.Name.Name)
		}
	}

	// Helpers per type: free funcs called by any method of that type
	helpers := map[string]map[string]struct{}{}
	for tn := range typeSet {
		helpers[tn] = map[string]struct{}{}
	}
	for tn, mlist := range methods {
		for _, m := range mlist {
			for callee := range adj[m] {
				if fb, ok := funcByKey[callee]; ok && !fb.isMethod {
					helpers[tn][callee] = struct{}{}
				}
			}
		}
	}

	// Users per type: free funcs that call a constructor of the type or a method of the type, or reference the type in signature/body
	users := map[string]map[string]struct{}{}
	for tn := range typeSet {
		users[tn] = map[string]struct{}{}
	}
	// Build quick sets
	ctorSet := map[string]string{} // funcName -> typeName
	for tn, list := range constructors {
		for _, c := range list {
			ctorSet[c] = tn
		}
	}
	methodSet := map[string]string{} // methodName -> typeName (best-effort)
	for tn, list := range methods {
		for _, m := range list {
			methodSet[m] = tn
		}
	}

	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv != nil {
			continue
		}
		name := fd.Name.Name
		// Check signature/body mentions
		for tn := range typeSet {
			if usesType(fd, tn) {
				users[tn][name] = struct{}{}
			}
		}
		if fd.Body != nil {
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ce, ok := n.(*ast.CallExpr); ok {
					switch fun := ce.Fun.(type) {
					case *ast.Ident:
						if tn, ok := ctorSet[fun.Name]; ok {
							users[tn][name] = struct{}{}
						}
						if tn, ok := methodSet[fun.Name]; ok {
							users[tn][name] = struct{}{}
						}
					case *ast.SelectorExpr:
						if tn, ok := methodSet[fun.Sel.Name]; ok {
							users[tn][name] = struct{}{}
						}
					}
				}
				return true
			})
		}
	}

	// Independent free funcs: not constructors, not helpers of any type, not users of any type
	inHelpers := map[string]struct{}{}
	for _, hs := range helpers {
		for k := range hs {
			inHelpers[k] = struct{}{}
		}
	}
	inUsers := map[string]struct{}{}
	for _, us := range users {
		for k := range us {
			inUsers[k] = struct{}{}
		}
	}

	independent := []string{}
	for _, fb := range funcBlocks {
		if fb.isMethod {
			continue
		}
		name := fb.key
		if strings.HasPrefix(name, "New") {
			continue
		}
		if _, ok := inHelpers[name]; ok {
			continue
		}
		if _, ok := inUsers[name]; ok {
			continue
		}
		independent = append(independent, name)
	}

	// Helper: minimal change reorder for a subset
	minimalReorder := func(keys []string, pinned int) []string {
		order := append([]string(nil), keys...)
		pos := map[string]int{}
		for i, k := range order {
			pos[k] = i
		}
		changed := true
		guard := len(order)*len(order) + 5
		for changed && guard > 0 {
			changed = false
			guard--
			for i := 0; i < len(order); i++ {
				if i < pinned {
					continue
				}
				g := order[i]
				// compute callers of g within the subset
				maxIdx := -1
				for caller, neigh := range adj {
					if _, ok := pos[caller]; !ok {
						continue
					}
					if _, calls := neigh[g]; calls {
						if idx := pos[caller]; idx > maxIdx {
							maxIdx = idx
						}
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
					order = append(order[:from], order[from+1:]...)
					if to > from {
						to--
					}
					order = append(order[:to], append([]string{g}, order[to:]...)...)
					for j, k := range order {
						pos[k] = j
					}
					changed = true
				}
			}
		}
		return order
	}

	// Build output
	out := &bytes.Buffer{}

	// Tracking of written type decls
	writtenDecl := map[int]struct{}{}
	markWritten := func(b block) { writtenDecl[b.start] = struct{}{} }
	isWritten := func(b block) bool { _, ok := writtenDecl[b.start]; return ok }

	// Header up to end of last import (or start of first decl if no imports)
	headerEnd := 0
	if lastImportEnd != -1 {
		headerEnd = lastImportEnd
	} else if firstDeclStart > 0 {
		headerEnd = firstDeclStart
	}
	if headerEnd > 0 {
		out.Write(src[:headerEnd])
	}

	writeNL := func() {
		if out.Len() > 0 && !bytes.HasSuffix(out.Bytes(), []byte("\n\n")) {
			if !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
				out.WriteByte('\n')
			}
			out.WriteByte('\n')
		}
	}

	// Move const/var under imports
	if len(constVarBlocks) > 0 {
		writeNL()
		for i, b := range constVarBlocks {
			if i > 0 {
				writeNL()
			}
			out.Write(src[b.start:b.end])
		}
		writeNL()
	}

	// Track functions already written to avoid duplication across sections
	writtenFunc := map[string]struct{}{}
	writeFuncIfNotWritten := func(name string) {
		if _, ok := writtenFunc[name]; ok {
			return
		}
		if fb, ok := funcByKey[name]; ok {
			writeNL()
			out.Write(src[fb.start:fb.end])
			writtenFunc[name] = struct{}{}
		}
	}

	// Independent free funcs near the top
	if len(independent) > 0 {
		ord := minimalReorder(independent, 0)
		for _, k := range ord {
			writeFuncIfNotWritten(k)
		}
		writeNL()
	}

	// Write type clusters in order of appearance
	for _, tn := range typeNames {
		b, ok := typeDeclFor[tn]
		if !ok {
			continue
		}
		if !isWritten(b) {
			writeNL()
			out.Write(src[b.start:b.end])
			markWritten(b)
		}
		// constructors in original order
		ctors := constructors[tn]
		for _, name := range ctors {
			writeFuncIfNotWritten(name)
		}
		// methods + helpers: start with methods in original order, then helpers in original order
		methodList := methods[tn]
		helperList := []string{}
		for _, fb := range funcBlocks {
			if _, ok := helpers[tn][fb.key]; ok {
				helperList = append(helperList, fb.key)
			}
		}
		cluster := append(append([]string{}, methodList...), helperList...)
		ord := minimalReorder(cluster, len(methodList))
		for _, k := range ord {
			writeFuncIfNotWritten(k)
		}
		// After cluster, write users assigned to this type (in original order, then minimal within subset)
		userList := []string{}
		for _, fb := range funcBlocks {
			if _, ok := users[tn][fb.key]; ok && !fb.isMethod {
				userList = append(userList, fb.key)
			}
		}
		if len(userList) > 0 {
			uord := minimalReorder(userList, 0)
			for _, k := range uord {
				writeFuncIfNotWritten(k)
			}
		}
		writeNL()
	}

	// Append any remaining type blocks not yet written
	for _, b := range typeBlocks {
		if !isWritten(b) {
			writeNL()
			out.Write(src[b.start:b.end])
			markWritten(b)
		}
	}

	// Append any remaining free funcs not yet written: those not in independent, not in ctors/methods/helpers/users
	writtenKeys := map[string]struct{}{}
	for _, k := range independent {
		writtenKeys[k] = struct{}{}
	}
	for tn := range typeSet {
		for _, k := range constructors[tn] {
			writtenKeys[k] = struct{}{}
		}
		for _, k := range methods[tn] {
			writtenKeys[k] = struct{}{}
		}
		for k := range helpers[tn] {
			writtenKeys[k] = struct{}{}
		}
		for k := range users[tn] {
			writtenKeys[k] = struct{}{}
		}
	}
	for _, fb := range funcBlocks {
		if _, ok := writtenKeys[fb.key]; ok {
			continue
		}
		writeFuncIfNotWritten(fb.key)
	}

	// Append any trailing bytes after the last declaration (e.g., file-level comments)
	if lastDeclEnd >= 0 && lastDeclEnd < len(src) {
		// ensure there is at least a newline between last written block and tail if needed
		if out.Len() > 0 && !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
			out.WriteByte('\n')
		}
		out.Write(src[lastDeclEnd:])
	}

	// If dry, print full altered file and summary
	if *dry {
		os.Stdout.Write(out.Bytes())
		fmt.Println()
		printSummaryAdvanced(fn, src, file, fset)
		return
	}

	if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write updated file: %v\n", err)
		os.Exit(1)
	}
	printSummaryAdvanced(fn, src, file, fset)
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

func printSummaryAdvanced(fn string, src []byte, file *ast.File, fset *token.FileSet) {
	fmt.Printf("file: %s\n", fn)
	// list functions in original order
	var funcs []string
	for _, d := range file.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok {
			funcs = append(funcs, fd.Name.Name)
		}
	}
	fmt.Printf("  functions (original order):\n")
	for i, k := range funcs {
		fmt.Printf("    %2d: %s\n", i, k)
	}
}
