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
	"strings"
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

	// Track original order index of functions as they appear in the file
	origFuncIdx := map[string]int{}

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
			// record original index in order of appearance
			if _, seen := origFuncIdx[key]; !seen {
				origFuncIdx[key] = len(origFuncIdx)
			}
		}

		if firstDeclStart == -1 || s < firstDeclStart {
			firstDeclStart = s
		}

		if e > lastDeclEnd {
			lastDeclEnd = e
		}
	}

	// Build name index for calls: map ident/selector name -> function key
	nameToKey := map[string]string{}
	for _, fb := range funcBlocks {
		nameToKey[fb.key] = fb.key
	}

	// Build call graph (file-local) and call sequence order per caller
	adj := map[string]map[string]struct{}{}
	callersOf := map[string]map[string]struct{}{}
	callSeq := map[string][]string{} // caller -> ordered unique callees by first occurrence

	callFirstPos := map[string]map[string]int{} // caller -> callee -> first offset
	for _, fb := range funcBlocks {
		adj[fb.key] = map[string]struct{}{}
	}

	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}

		caller := fd.Name.Name
		if _, ok := callFirstPos[caller]; !ok {
			callFirstPos[caller] = map[string]int{}
		}

		ast.Inspect(fd.Body, func(n ast.Node) bool {
			ce, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			var name string

			switch fun := ce.Fun.(type) {
			case *ast.Ident:
				name = fun.Name
			case *ast.SelectorExpr:
				name = fun.Sel.Name
			}

			if callee, ok := nameToKey[name]; ok && callee != caller {
				adj[caller][callee] = struct{}{}
				if _, ok := callersOf[callee]; !ok {
					callersOf[callee] = map[string]struct{}{}
				}

				callersOf[callee][caller] = struct{}{}
				// record first position
				if _, seen := callFirstPos[caller][callee]; !seen {
					callFirstPos[caller][callee] = fset.Position(ce.Pos()).Offset
				}
			}

			return true
		})

		// build sequence list
		if len(callFirstPos[caller]) > 0 {
			type pair struct {
				name string
				pos  int
			}

			arr := make([]pair, 0, len(callFirstPos[caller]))
			for k, p := range callFirstPos[caller] {
				arr = append(arr, pair{k, p})
			}

			sort.Slice(arr, func(i, j int) bool { return arr[i].pos < arr[j].pos })

			seq := make([]string, len(arr))
			for i := range arr {
				seq[i] = arr[i].name
			}

			callSeq[caller] = seq
		}
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

	// Users per type: free funcs that call a constructor of the type or a
	// method of the type, or reference the type in signature/body
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

	// Independent free funcs: zero-degree (no in/out edges within file)
	independent := []string{}

	for _, fb := range funcBlocks {
		if fb.isMethod {
			continue
		}

		name := fb.key
		if strings.HasPrefix(name, "New") {
			continue
		}

		if len(adj[name]) > 0 {
			continue
		}

		if len(callersOf[name]) > 0 {
			continue
		}

		independent = append(independent, name)
	}

	// Helper to sort a list of function keys by their original order in the
	// file (by byte offset)
	sortByOriginal := func(keys []string) []string {
		out := append([]string(nil), keys...)
		sort.SliceStable(out, func(i, j int) bool { return funcByKey[out[i]].start < funcByKey[out[j]].start })

		return out
	}

	// Helper: minimal change reorder for a subset honouring predecessors
	minimalReorderSubset := func(keys []string, pinned int) []string {
		order := append([]string(nil), keys...)
		pos := map[string]int{}
		keysSet := map[string]struct{}{}

		for i, k := range order {
			pos[k] = i
			keysSet[k] = struct{}{}
		}

		// Build predecessor map for subset
		pred := map[string]map[string]struct{}{}
		getPred := func(k string) map[string]struct{} {
			if m, ok := pred[k]; ok {
				return m
			}

			m := map[string]struct{}{}
			pred[k] = m

			return m
		}

		// 1) real call edges restricted to subset
		for caller, neigh := range adj {
			if _, ok := keysSet[caller]; !ok {
				continue
			}

			for callee := range neigh {
				if _, ok := keysSet[callee]; ok {
					getPred(callee)[caller] = struct{}{}
				}
			}
		}

		// 2) sequence edges among callees by first-use order in any caller
		for _, seq := range callSeq {
			// filter to subset
			filt := make([]string, 0, len(seq))
			for _, c := range seq {
				if _, ok := keysSet[c]; ok {
					filt = append(filt, c)
				}
			}

			for i := 0; i < len(filt); i++ {
				for j := i + 1; j < len(filt); j++ {
					a, b := filt[i], filt[j]
					getPred(b)[a] = struct{}{}
				}
			}
		}

		// Minimal move respecting predecessors
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

				maxIdx := -1
				for p := range getPred(g) {
					if idx, ok := pos[p]; ok && idx > maxIdx {
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

	// Helper: pack callees contiguously beneath each caller, when safe (won't
	// violate predecessors), within a subset Respects a pinned prefix: elements
	// before 'pinned' index are not crossed.
	packWithinSubset := func(order []string, pinned int) []string {
		out := append([]string(nil), order...)
		if len(out) == 0 {
			return out
		}

		keysSet := map[string]struct{}{}
		for _, k := range out {
			keysSet[k] = struct{}{}
		}

		// Build predecessor map restricted to subset, mirroring
		// minimalReorderSubset
		pred := map[string]map[string]struct{}{}
		getPred := func(k string) map[string]struct{} {
			if m, ok := pred[k]; ok {
				return m
			}

			m := map[string]struct{}{}
			pred[k] = m

			return m
		}

		for caller, neigh := range adj {
			if _, ok := keysSet[caller]; !ok {
				continue
			}

			for callee := range neigh {
				if _, ok := keysSet[callee]; ok {
					getPred(callee)[caller] = struct{}{}
				}
			}
		}

		for _, seq := range callSeq {
			filt := make([]string, 0, len(seq))
			for _, c := range seq {
				if _, ok := keysSet[c]; ok {
					filt = append(filt, c)
				}
			}

			for i := 0; i < len(filt); i++ {
				for j := i + 1; j < len(filt); j++ {
					a, b := filt[i], filt[j]
					getPred(b)[a] = struct{}{}
				}
			}
		}

		pos := map[string]int{}
		for i, k := range out {
			pos[k] = i
		}

		// For each caller in current order, pack its callees after it in
		// first-use order
		for ci := 0; ci < len(out); ci++ {
			caller := out[ci]

			seq := callSeq[caller]
			if len(seq) == 0 {
				continue
			}

			// Filter to subset keys and keep unique order
			filtered := make([]string, 0, len(seq))
			seen := map[string]struct{}{}

			for _, v := range seq {
				if _, ok := keysSet[v]; ok {
					if _, s := seen[v]; !s {
						filtered = append(filtered, v)
						seen[v] = struct{}{}
					}
				}
			}

			insertPos := ci + 1
			if insertPos < pinned {
				insertPos = pinned
			}

			for _, v := range filtered {
				// Compute max predecessor index for v within subset
				maxPred := -1
				for p := range getPred(v) {
					if idx, ok := pos[p]; ok && idx > maxPred {
						maxPred = idx
					}
				}

				desired := insertPos
				if maxPred+1 > desired {
					desired = maxPred + 1
				}

				if desired < pinned {
					desired = pinned
				}

				cur := pos[v]
				if cur == desired {
					insertPos = desired + 1

					continue
				}

				// Move v to desired index by splicing
				out = append(out[:cur], out[cur+1:]...)
				if desired > cur {
					desired--
				}

				if desired < 0 {
					desired = 0
				}

				if desired > len(out) {
					desired = len(out)
				}

				out = append(out[:desired], append([]string{v}, out[desired:]...)...)

				// Recompute positions and next insert
				pos = map[string]int{}
				for i, k := range out {
					pos[k] = i
				}

				insertPos = pos[v] + 1
				if insertPos < pinned {
					insertPos = pinned
				}
			}
		}

		return out
	}

	// Build output
	out := &bytes.Buffer{}

	// Tracking of written type decls
	writtenDecl := map[int]struct{}{}
	markWritten := func(b block) { writtenDecl[b.start] = struct{}{} }
	isWritten := func(b block) bool {
		_, ok := writtenDecl[b.start]

		return ok
	}

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
		ord := minimalReorderSubset(sortByOriginal(independent), 0)
		for _, k := range ord {
			writeFuncIfNotWritten(k)
		}

		writeNL()
	}

	// For each type cluster
	for _, tn := range typeNames {
		b, ok := typeDeclFor[tn]
		if !ok {
			continue
		}

		// Primary types are those with methods or constructors
		hasCtors := len(constructors[tn]) > 0

		hasMethods := len(methods[tn]) > 0
		if hasCtors || hasMethods {
			if !isWritten(b) {
				writeNL()
				out.Write(src[b.start:b.end])
				markWritten(b)
			}

			// constructors
			for _, name := range constructors[tn] {
				writeFuncIfNotWritten(name)
			}

			// methods + helpers together, allow full reordering based on
			// constraints
			methodList := methods[tn]
			helperList := []string{}

			for _, fb := range funcBlocks {
				if _, ok := helpers[tn][fb.key]; ok {
					helperList = append(helperList, fb.key)
				}
			}

			cluster := append(append([]string{}, methodList...), helperList...)
			ord := sortByOriginal(cluster)

			// Determine a pinned prefix: methods before the first caller (with
			// callees in this cluster), excluding those callees
			pinned := 0

			// find first caller index in ord
			firstCallerIdx := -1
			firstCaller := ""

			for i, name := range ord {
				if seq, ok := callSeq[name]; ok && len(seq) > 0 {
					// does it call someone in this cluster?
					for _, cal := range seq {
						for _, kk := range ord {
							if kk == cal {
								firstCallerIdx = i
								firstCaller = name

								break
							}
						}

						if firstCallerIdx != -1 {
							break
						}
					}
				}

				if firstCallerIdx != -1 {
					break
				}
			}

			if firstCallerIdx != -1 {
				// compute callee set within cluster for firstCaller
				calSet := map[string]struct{}{}

				for _, c := range callSeq[firstCaller] {
					for _, kk := range ord {
						if kk == c {
							calSet[c] = struct{}{}
						}
					}
				}

				// count pinned as those before firstCallerIdx that are not in
				// callee set
				for i := 0; i < firstCallerIdx; i++ {
					if _, isCal := calSet[ord[i]]; !isCal {
						pinned++
					}
				}
			}

			// Now apply minimal reorder with pinned prefix, then pack
			// respecting pinned
			ord = minimalReorderSubset(ord, pinned)

			ord = packWithinSubset(ord, pinned)
			for _, k := range ord {
				writeFuncIfNotWritten(k)
			}

			// users of this type
			userList := []string{}

			for _, fb := range funcBlocks {
				if _, ok := users[tn][fb.key]; ok && !fb.isMethod {
					userList = append(userList, fb.key)
				}
			}

			if len(userList) > 0 {
				uord := sortByOriginal(userList)
				uord = minimalReorderSubset(uord, 0)

				uord = packWithinSubset(uord, 0)
				for _, k := range uord {
					writeFuncIfNotWritten(k)
				}
			}

			writeNL()
		} else {
			// Non-primary type: just emit the type declaration now; do not
			// cluster users
			if !isWritten(b) {
				writeNL()
				out.Write(src[b.start:b.end])
				markWritten(b)
			}
		}
	}

	// Append any remaining type blocks not yet written
	for _, b := range typeBlocks {
		if !isWritten(b) {
			writeNL()
			out.Write(src[b.start:b.end])
			markWritten(b)
		}
	}

	// Append remaining free-func clusters (connected components), ordered by
	// constraints
	remainingSet := map[string]struct{}{}

	for _, fb := range funcBlocks {
		if fb.isMethod {
			continue
		}

		if _, ok := writtenFunc[fb.key]; !ok {
			remainingSet[fb.key] = struct{}{}
		}
	}

	// Build components via undirected connectivity using adj and callersOf
	for key := range remainingSet {
		if _, ok := remainingSet[key]; !ok {
			continue
		}

		// BFS
		comp := []string{}
		queue := []string{key}
		delete(remainingSet, key)

		for len(queue) > 0 {
			u := queue[0]
			queue = queue[1:]

			comp = append(comp, u)

			// neighbours: outgoing
			for v := range adj[u] {
				if _, ok := remainingSet[v]; ok {
					delete(remainingSet, v)
					queue = append(queue, v)
				}
			}

			// neighbours: incoming
			for v := range callersOf[u] {
				if _, ok := remainingSet[v]; ok {
					delete(remainingSet, v)
					queue = append(queue, v)
				}
			}
		}

		// Order this component starting from original order
		ord := sortByOriginal(comp)
		ord = minimalReorderSubset(ord, 0)

		ord = packWithinSubset(ord, 0)
		for _, k := range ord {
			writeFuncIfNotWritten(k)
		}

		writeNL()
	}

	// Append any remaining free funcs not yet written
	writtenKeys := map[string]struct{}{}

	for _, tn := range typeNames {
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

	for _, k := range independent {
		writtenKeys[k] = struct{}{}
	}

	for _, fb := range funcBlocks {
		if _, ok := writtenKeys[fb.key]; ok {
			continue
		}

		writeFuncIfNotWritten(fb.key)
	}

	// Append any trailing bytes after the last declaration (e.g., file-level
	// comments)
	if lastDeclEnd >= 0 && lastDeclEnd < len(src) {
		// ensure there is at least a newline between last written block and
		// tail if needed
		if out.Len() > 0 && !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
			out.WriteByte('\n')
		}

		out.Write(src[lastDeclEnd:])
	}

	// If dry, print full altered file and summary
	if *dry {
		os.Stdout.Write(out.Bytes())
		fmt.Println()

		return
	}

	if err := os.WriteFile(fn, out.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write updated file: %v\n", err)
		os.Exit(1)
	}
}

// usesType checks whether a free function references a type by name in its
// signature or body.
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

// typeContains recursively checks whether the ast.Expr contains a given
// identifier name.
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



