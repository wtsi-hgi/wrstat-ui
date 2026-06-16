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
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	errClientClosed           = errors.New("clickhouse: client is closed")
	errFileCatalogDirNotFound = errors.New("clickhouse: file catalog dir not found")
	errPathNotFound           = errors.New("clickhouse: path not found")
	errInvalidPath            = errors.New("clickhouse: invalid path")
)

const (
	fileRowSelectAll = "concat(d.full_path, f.name), d.full_path, f.name, f.ext, f.entry_type, f.size, " +
		"f.apparent_size, f.uid, f.gid, f.atime, f.mtime, f.ctime, f.inode, f.nlink"
	fileRowPageSelectAll = "f.dir_id, f.name, f.ext, f.entry_type, f.size, f.apparent_size, f.uid, f.gid, " +
		"f.atime, f.mtime, f.ctime, f.inode, f.nlink"
)

const catalogDirByPathQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT dir_id, subtree_end, full_path FROM wrstat_dirs " +
	"PREWHERE mount_path = ? AND snapshot_id = sid AND path_hash = ? " +
	"WHERE full_path = ? LIMIT 1"

const catalogDirsByIDQueryTemplate = "WITH " +
	"(SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT dir_id, full_path FROM wrstat_dirs " +
	"PREWHERE mount_path = ? AND snapshot_id = sid AND dir_id IN (%s)"

const statPathQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f INNER JOIN wrstat_dirs d " +
	"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid AND f.dir_id = ? " +
	"WHERE f.name = ? LIMIT 1"

const listDirQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid AND f.dir_id = ? " +
	"ORDER BY f.name ASC LIMIT ? OFFSET ?"

const findByGlobQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f INNER JOIN wrstat_dirs d " +
	"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"WHERE (%s) " +
	"AND (? = 0 OR f.uid = ? OR has(?, f.gid)) " +
	"ORDER BY d.full_path ASC, f.name ASC LIMIT ? OFFSET ?"

const countByGlobQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT count() FROM wrstat_files f INNER JOIN wrstat_dirs d " +
	"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"WHERE (%s) " +
	"AND (? = 0 OR f.uid = ? OR has(?, f.gid))"

const isDirQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT f.entry_type FROM wrstat_files f INNER JOIN wrstat_dirs d " +
	"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid AND f.dir_id = ? " +
	"WHERE f.name = ? LIMIT 1"

const permissionAnyInDirQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT 1 FROM wrstat_dir_facts f " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid AND f.dir_id = ? " +
	"WHERE arrayExists((age, uid, gid) -> age = ? AND (uid = ? OR has(?, gid)), " +
	"f.ages, f.uids, f.gids) LIMIT 1"

const permissionPathQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT 1 FROM wrstat_files f INNER JOIN wrstat_dirs d " +
	"ON d.mount_path = f.mount_path AND d.snapshot_id = f.snapshot_id AND d.dir_id = f.dir_id " +
	"PREWHERE f.mount_path = ? AND f.snapshot_id = sid AND f.dir_id = ? " +
	"WHERE f.name = ? AND (f.uid = ? OR has(?, f.gid)) LIMIT 1"

const defaultFileLimit = 1_000_000

const (
	maxGlobPatternsPerQuery       = 32
	findByGlobParamsPerBaseDirCap = 2
	findByGlobParamsSharedCap     = 7
	findByGlobClauseCap           = 2
	catalogDirsByIDBaseParamCount = 2
	minDedupeByPathLen            = 2
	growExtraForAnchors           = 2
	catalogSkipLiteralMinLen      = 4
)

const (
	fileFieldDirID        = "dir_id"
	fileFieldPath         = "path"
	fileFieldParentDir    = "parent_dir"
	fileFieldName         = "name"
	fileFieldExt          = "ext"
	fileFieldEntryType    = "entry_type"
	fileFieldSize         = "size"
	fileFieldApparentSize = "apparent_size"
	fileFieldUID          = "uid"
	fileFieldGID          = "gid"
	fileFieldATime        = "atime"
	fileFieldMTime        = "mtime"
	fileFieldCTime        = "ctime"
	fileFieldInode        = "inode"
	fileFieldNLink        = "nlink"
)

//nolint:gochecknoglobals // Immutable lookup table used on hot file-row scan paths.
var fileRowSpecs = []fileRowFieldSpec{
	{fileFieldPath, "concat(d.full_path, f.name)", func(s *fileRowScanState) any { return &s.path }},
	{fileFieldParentDir, "d.full_path", func(s *fileRowScanState) any { return &s.parentDir }},
	{fileFieldName, "f.name", func(s *fileRowScanState) any { return &s.name }},
	{fileFieldExt, "f.ext", func(s *fileRowScanState) any { return &s.ext }},
	{fileFieldEntryType, "f.entry_type", func(s *fileRowScanState) any { return &s.entryType }},
	{fileFieldSize, "f.size", func(s *fileRowScanState) any { return &s.size }},
	{fileFieldApparentSize, "f.apparent_size", func(s *fileRowScanState) any { return &s.apparentSize }},
	{fileFieldUID, "f.uid", func(s *fileRowScanState) any { return &s.uid }},
	{fileFieldGID, "f.gid", func(s *fileRowScanState) any { return &s.gid }},
	{fileFieldATime, "f.atime", func(s *fileRowScanState) any { return &s.atime }},
	{fileFieldMTime, "f.mtime", func(s *fileRowScanState) any { return &s.mtime }},
	{fileFieldCTime, "f.ctime", func(s *fileRowScanState) any { return &s.ctime }},
	{fileFieldInode, "f.inode", func(s *fileRowScanState) any { return &s.inode }},
	{fileFieldNLink, "f.nlink", func(s *fileRowScanState) any { return &s.nlink }},
}

// FileRow represents a file or directory from wrstat_files.
type FileRow struct {
	Path         string
	ParentDir    string
	Name         string
	Ext          string
	EntryType    byte
	Size         int64
	ApparentSize int64
	UID          uint32
	GID          uint32
	ATime        time.Time
	MTime        time.Time
	CTime        time.Time
	Inode        int64
	Nlink        int64

	dirID uint32
}

func firstFileRow(rows fileRowIterator, fields []string) (*FileRow, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("clickhouse: StatPath iteration error: %w", err)
		}

		return nil, errPathNotFound
	}

	row := &FileRow{}
	if err := scanFileRow(rows, fields, row); err != nil {
		return nil, err
	}

	return row, nil
}

// StatPath returns metadata for an exact file path over the active snapshot of
// the mount containing the path.
func (c *Client) StatPath(ctx context.Context, path string, opts StatOptions) (*FileRow, error) {
	if c == nil || c.conn == nil {
		return nil, errClientClosed
	}

	mountPath, parentDir, name, err := c.resolveMountParentName(path)
	if err != nil {
		return nil, err
	}

	q, fields, err := statPathQuery(opts)
	if err != nil {
		return nil, err
	}

	parent, err := c.resolveCatalogDir(ctx, mountPath, parentDir)
	if err != nil {
		return nil, err
	}

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, q, mountPath, mountPath, parent.dirID, name)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query StatPath: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return firstFileRow(rows, fields)
}

func statPathQuery(opts StatOptions) (string, []string, error) {
	return fileRowQuery(statPathQueryTemplate, opts.Fields)
}

func fileRowQuery(template string, fields []string) (string, []string, error) {
	selectList, selectedFields, err := fileRowSelectList(fields)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(template, selectList), selectedFields, nil
}

func fileRowSelectList(fields []string) (string, []string, error) {
	if len(fields) == 0 {
		return fileRowSelectAll, defaultFileRowFields(), nil
	}

	out := make([]string, 0, len(fields))
	for _, f := range fields {
		spec, ok := fileRowFieldSpecFor(f)
		if !ok {
			return "", nil, unknownFileFieldError{Field: f}
		}

		out = append(out, spec.column)
	}

	return strings.Join(out, ", "), fields, nil
}

func defaultFileRowFields() []string {
	specs := fileRowSpecs
	out := make([]string, 0, len(specs))

	for _, spec := range specs {
		out = append(out, spec.field)
	}

	return out
}

func fileRowFieldSpecFor(field string) (fileRowFieldSpec, bool) {
	for _, spec := range fileRowSpecs {
		if spec.field == field {
			return spec, true
		}
	}

	return fileRowFieldSpec{}, false
}

// ListDir lists direct children for the given directory.
func (c *Client) ListDir(ctx context.Context, dir string, opts ListOptions) ([]FileRow, error) {
	if c == nil || c.conn == nil {
		return nil, errClientClosed
	}

	mountPath, parentDir, err := c.resolveMountAndDir(dir)
	if err != nil {
		return nil, err
	}

	q, fields, err := listDirQuery(opts)
	if err != nil {
		return nil, err
	}

	dirRef, ok, err := c.resolveListDirRef(ctx, mountPath, parentDir)
	if err != nil {
		return nil, err
	}

	if !ok {
		return []FileRow{}, nil
	}

	return c.queryListDirRows(ctx, q, fields, mountPath, dirRef.dirID, opts)
}

func listDirQuery(opts ListOptions) (string, []string, error) {
	return fileRowPageQuery(listDirQueryTemplate, opts.Fields)
}

func fileRowPageQuery(template string, fields []string) (string, []string, error) {
	selectList, selectedFields, err := fileRowPageSelectList(fields)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(template, selectList), selectedFields, nil
}

func fileRowPageSelectList(fields []string) (string, []string, error) {
	if len(fields) == 0 {
		return fileRowPageSelectAll, defaultFileRowPageFields(), nil
	}

	columns := []string{"f.dir_id", "f.name"}
	selected := []string{fileFieldDirID, fileFieldName}

	for _, f := range fields {
		column, selectedField, include, err := fileRowPageColumn(f)
		if err != nil {
			return "", nil, err
		}

		if !include {
			continue
		}

		columns = append(columns, column)
		selected = append(selected, selectedField)
	}

	return strings.Join(columns, ", "), selected, nil
}

func defaultFileRowPageFields() []string {
	fields := defaultFileRowFields()
	out := make([]string, 0, len(fields)-1)
	out = append(out, fileFieldDirID)

	for _, field := range fields {
		switch field {
		case fileFieldPath, fileFieldParentDir:
			continue
		default:
			out = append(out, field)
		}
	}

	return out
}

func fileRowPageColumn(field string) (string, string, bool, error) {
	switch field {
	case fileFieldDirID:
		return "", "", false, nil
	case fileFieldPath, fileFieldParentDir, fileFieldName:
		if _, ok := fileRowFieldSpecFor(field); !ok {
			return "", "", false, unknownFileFieldError{Field: field}
		}

		return "", "", false, nil
	default:
		spec, ok := fileRowFieldSpecFor(field)
		if !ok {
			return "", "", false, unknownFileFieldError{Field: field}
		}

		return spec.column, field, true, nil
	}
}

func (c *Client) queryListDirRows(
	ctx context.Context,
	query string,
	fields []string,
	mountPath string,
	dirID uint32,
	opts ListOptions,
) ([]FileRow, error) {
	limit := listLimit(opts.Limit)

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	return c.queryFileRowsWithPaths(
		qctx,
		"ListDir",
		query,
		fields,
		mountPath,
		mountPath,
		mountPath,
		dirID,
		limit,
		opts.Offset,
	)
}

func listLimit(limit int64) int64 {
	if limit > 0 {
		return limit
	}

	return defaultFileLimit
}

// FindByGlob finds file rows matching gitignore-style patterns under the given
// base directories.
func (c *Client) FindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	opts FindOptions,
) ([]FileRow, error) {
	if c == nil || c.conn == nil {
		return nil, errClientClosed
	}

	if len(patterns) == 0 {
		return []FileRow{}, nil
	}

	prepared, err := c.prepareFindByGlob(ctx, baseDirs, patterns, opts)
	if err != nil {
		return nil, err
	}

	all, err := c.runFindByGlobPlan(ctx, prepared)
	if err != nil {
		return nil, err
	}

	all = finishFindByGlob(all)

	if prepared.useDirectOffset {
		return all, nil
	}

	return sliceLimitOffset(all, prepared.limit, opts.Offset), nil
}

func (c *Client) runFindByGlobPlan(
	ctx context.Context,
	prepared findByGlobPrepared,
) ([]FileRow, error) {
	out := make([]FileRow, 0)

	for _, q := range prepared.plan.queries {
		rows, err := c.findByGlobQuery(ctx, prepared, q)
		if err != nil {
			return nil, err
		}

		out = append(out, rows...)
	}

	return out, nil
}

type findByGlobQuerySpec struct {
	mountPath    string
	baseDirs     []fileCatalogDirRef
	patternChunk []string
}

func (c *Client) findByGlobQuery(
	ctx context.Context,
	prepared findByGlobPrepared,
	spec findByGlobQuerySpec,
) ([]FileRow, error) {
	q, params := buildFindByGlobQueryAndParams(
		prepared.selectList,
		spec.mountPath,
		spec.baseDirs,
		spec.patternChunk,
		prepared.ownerEnabled,
		prepared.uid,
		prepared.gids,
		prepared.queryLimit,
		prepared.queryOffset,
	)

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	return c.queryFileRowsWithPaths(qctx, "FindByGlob", q, prepared.fields, spec.mountPath, params...)
}

func buildFindByGlobQueryAndParams(
	selectList string,
	mountPath string,
	baseDirs []fileCatalogDirRef,
	patterns []string,
	ownerEnabled int64,
	uid uint32,
	gids []uint32,
	limit int64,
	offset int64,
) (string, []any) {
	baseDirClauses := make([]string, 0, len(baseDirs))
	params := make([]any, 0, findByGlobParamsSharedCap+len(baseDirs)*(len(patterns)+findByGlobParamsPerBaseDirCap))
	params = append(params, mountPath, mountPath)

	for _, baseDir := range baseDirs {
		compiled := compileGlobPatterns(baseDir.fullPath, patterns)
		clause, clauseParams := findByGlobBaseDirClause(mountPath, baseDir, compiled)
		baseDirClauses = append(baseDirClauses, clause)
		params = append(params, clauseParams...)
	}

	q := fmt.Sprintf(findByGlobQueryTemplate, selectList, strings.Join(baseDirClauses, " OR "))

	params = append(params, ownerEnabled, uid, gids, limit, offset)

	return q, params
}

func compileGlobPatterns(baseDir string, patterns []string) compiledGlobPatterns {
	escapedBase := regexp.QuoteMeta(baseDir)

	out := compiledGlobPatterns{
		direct:    make([]compiledGlobPattern, 0, len(patterns)),
		recursive: make([]compiledGlobPattern, 0, len(patterns)),
	}

	for _, p := range patterns {
		if globPatternMatchesWholeSubtree(p) {
			out.matchAll = true

			continue
		}

		if isDirectChildGlobPattern(p) {
			out.direct = append(out.direct, compileGlobPattern("", p))

			continue
		}

		out.recursive = append(out.recursive, compilePathGlobPattern(baseDir, escapedBase, p))
	}

	return out
}

func globPatternMatchesWholeSubtree(pattern string) bool {
	return pattern == "**" || pattern == "**/*"
}

func isDirectChildGlobPattern(pattern string) bool {
	return !strings.Contains(pattern, "/") && !strings.Contains(pattern, "**")
}

func compileGlobPattern(escapedBase string, pattern string) compiledGlobPattern {
	return compiledGlobPattern{
		regex: globToRE2(escapedBase, pattern),
		ext:   exactSafeGlobExt(pattern),
	}
}

func globToRE2(escapedBase string, pattern string) string {
	var b strings.Builder
	b.Grow(len(escapedBase) + len(pattern) + growExtraForAnchors)

	b.WriteByte('^')
	b.WriteString(escapedBase)

	for i := 0; i < len(pattern); i++ {
		if writeGlobToken(&b, pattern, &i) {
			continue
		}

		writeRE2LiteralByte(&b, pattern[i])
	}

	b.WriteByte('$')

	return b.String()
}

func writeGlobToken(b *strings.Builder, pattern string, i *int) bool {
	switch pattern[*i] {
	case '*':
		if *i+1 < len(pattern) && pattern[*i+1] == '*' {
			if isZeroOrMoreDirsPattern(pattern, *i) {
				b.WriteString("(?:[^/]+/)*")

				*i += 2

				return true
			}

			b.WriteString(".*")

			(*i)++

			return true
		}

		b.WriteString("[^/]*")

		return true
	case '?':
		b.WriteString("[^/]")

		return true
	default:
		return false
	}
}

func isZeroOrMoreDirsPattern(pattern string, idx int) bool {
	return (idx == 0 || pattern[idx-1] == '/') && idx+2 < len(pattern) && pattern[idx+2] == '/'
}

func writeRE2LiteralByte(b *strings.Builder, c byte) {
	switch c {
	case '.', '+', '(', ')', '|', '[', ']', '{', '}', '^', '$', '\\':
		b.WriteByte('\\')
		b.WriteByte(c)
	default:
		b.WriteByte(c)
	}
}

func exactSafeGlobExt(pattern string) string {
	_, name := splitGlobPatternParentAndName(pattern)

	ext, ok := strings.CutPrefix(name, "*.")
	if !ok {
		ext, ok = strings.CutPrefix(name, "**/*.")
	}

	if !ok || ext == "" || strings.ContainsAny(ext, "*/?[]{}\\") {
		return ""
	}

	if idx := strings.LastIndexByte(ext, '.'); idx >= 0 {
		ext = ext[idx+1:]
	}

	return strings.ToLower(ext)
}

func splitGlobPatternParentAndName(pattern string) (string, string) {
	if strings.HasSuffix(pattern, "/") {
		prefix := strings.TrimSuffix(pattern, "/")

		idx := strings.LastIndexByte(prefix, '/')
		if idx < 0 {
			return "", pattern
		}

		return pattern[:idx+1], pattern[idx+1:]
	}

	idx := strings.LastIndexByte(pattern, '/')
	if idx < 0 {
		return "", pattern
	}

	return pattern[:idx+1], pattern[idx+1:]
}

func compilePathGlobPattern(baseDir string, escapedBase string, pattern string) compiledGlobPattern {
	patternBase := baseDir
	patternEscapedBase := escapedBase

	if strings.HasPrefix(pattern, "/") {
		patternBase = ""
		patternEscapedBase = ""
	}

	parentPattern := globParentDirPattern(pattern)

	return compiledGlobPattern{
		regex:          globToRE2(patternEscapedBase, pattern),
		ext:            exactSafeGlobExt(pattern),
		dirRegex:       globToRE2(patternEscapedBase, parentPattern),
		dirSkipLiteral: longestGlobLiteral(patternBase+parentPattern, catalogSkipLiteralMinLen),
	}
}

func globParentDirPattern(pattern string) string {
	parent, name := splitGlobPatternParentAndName(pattern)
	if parent == "" && !strings.Contains(pattern, "/") && strings.Contains(name, "**") {
		return "**/"
	}

	if strings.Contains(name, "**") {
		return parent + "**/"
	}

	return parent
}

func longestGlobLiteral(pattern string, minLen int) string {
	best := ""
	start := 0

	for i := range len(pattern) {
		if pattern[i] == '*' || pattern[i] == '?' {
			best = longerString(best, pattern[start:i])
			start = i + 1
		}
	}

	best = longerString(best, pattern[start:])
	if len(best) < minLen {
		return ""
	}

	return best
}

func longerString(a string, b string) string {
	if len(b) > len(a) {
		return b
	}

	return a
}

func findByGlobBaseDirClause(
	mountPath string,
	baseDir fileCatalogDirRef,
	compiled compiledGlobPatterns,
) (string, []any) {
	if compiled.matchAll {
		return findByGlobRangeClause(), []any{baseDir.dirID, baseDir.subtreeEnd}
	}

	clauses := make([]string, 0, findByGlobClauseCap)
	params := make([]any, 0, len(compiled.direct)+len(compiled.recursive)+findByGlobParamsPerBaseDirCap)

	if len(compiled.direct) > 0 {
		matchClause, matchParams := matchOrExtList("f.name", compiled.direct)
		clauses = append(clauses, "(f.dir_id = ? AND ("+matchClause+"))")
		params = append(params, baseDir.dirID)
		params = append(params, matchParams...)
	}

	if len(compiled.recursive) > 0 {
		clause, clauseParams := recursiveGlobClause(mountPath, baseDir, compiled.recursive)
		clauses = append(clauses, clause)
		params = append(params, clauseParams...)
	}

	if len(clauses) == 0 {
		return "(0)", nil
	}

	return "(" + strings.Join(clauses, " OR ") + ")", params
}

func findByGlobRangeClause() string {
	return "(f.dir_id >= ? AND f.dir_id < ?)"
}

func matchOrExtList(column string, patterns []compiledGlobPattern) (string, []any) {
	if len(patterns) == 0 {
		return "0", nil
	}

	clauses := make([]string, 0, len(patterns))
	params := make([]any, 0, len(patterns))

	for _, p := range patterns {
		if p.ext == "" {
			clauses = append(clauses, "match("+column+", ?)")
			params = append(params, p.regex)

			continue
		}

		clauses = append(clauses, "((f.ext = ? OR f.name = ?) AND match("+column+", ?))")
		params = append(params, p.ext, "."+p.ext, p.regex)
	}

	return strings.Join(clauses, " OR "), params
}

func recursiveGlobClause(mountPath string, baseDir fileCatalogDirRef, patterns []compiledGlobPattern) (string, []any) {
	dirClause, dirParams := catalogDirCandidateClause(mountPath, baseDir, patterns)
	matchClause, matchParams := matchOrExtList("concat(d.full_path, f.name)", patterns)
	params := make([]any, 0, len(dirParams)+len(matchParams))
	params = append(params, dirParams...)
	params = append(params, matchParams...)

	return dirClause + " AND (" + matchClause + ")", params
}

func catalogDirCandidateClause(
	mountPath string,
	baseDir fileCatalogDirRef,
	patterns []compiledGlobPattern,
) (string, []any) {
	patternClauses := make([]string, 0, len(patterns))
	params := make([]any, 0, findByGlobParamsPerBaseDirCap+len(patterns)*2)
	params = append(params, mountPath, baseDir.dirID, baseDir.subtreeEnd)

	for _, pattern := range patterns {
		clause, clauseParams := catalogDirPatternClause(pattern)
		patternClauses = append(patternClauses, clause)
		params = append(params, clauseParams...)
	}

	return "f.dir_id IN (SELECT cd.dir_id FROM wrstat_dirs cd " +
		"PREWHERE cd.mount_path = ? AND cd.snapshot_id = sid " +
		"AND cd.dir_id >= ? AND cd.dir_id < ? " +
		"WHERE (" + strings.Join(patternClauses, " OR ") + "))", params
}

func catalogDirPatternClause(pattern compiledGlobPattern) (string, []any) {
	if pattern.dirSkipLiteral == "" {
		return "match(cd.full_path, ?)", []any{pattern.dirRegex}
	}

	return "(cd.full_path LIKE ? AND match(cd.full_path, ?))",
		[]any{"%" + pattern.dirSkipLiteral + "%", pattern.dirRegex}
}

func (c *Client) queryFileRowsWithPaths(
	ctx context.Context,
	op string,
	query string,
	fields []string,
	mountPath string,
	params ...any,
) ([]FileRow, error) {
	rows, err := c.queryFileRows(ctx, op, query, fields, params...)
	if err != nil {
		return nil, err
	}

	if err := c.populateFileRowPaths(ctx, op, mountPath, rows); err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *Client) queryFileRows(
	ctx context.Context,
	op string,
	query string,
	fields []string,
	params ...any,
) ([]FileRow, error) {
	rows, err := c.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s: %w", op, err)
	}

	defer func() { _ = rows.Close() }()

	out := make([]FileRow, 0)

	for rows.Next() {
		var row FileRow
		if err := scanFileRow(rows, fields, &row); err != nil {
			return nil, err
		}

		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: %s iteration error: %w", op, err)
	}

	return out, nil
}

func scanFileRow(rows fileRowScanner, fields []string, out *FileRow) error {
	state := &fileRowScanState{}

	dests, err := state.destsFor(fields)
	if err != nil {
		return err
	}

	if err := rows.Scan(dests...); err != nil {
		return fmt.Errorf("clickhouse: failed to scan file row: %w", err)
	}

	return state.applyTo(out)
}

func (c *Client) populateFileRowPaths(ctx context.Context, op string, mountPath string, rows []FileRow) error {
	dirIDs := fileRowDirIDs(rows)
	if len(dirIDs) == 0 {
		return nil
	}

	parents, err := c.fileRowParentDirs(ctx, op, mountPath, dirIDs)
	if err != nil {
		return err
	}

	for i := range rows {
		parentDir, ok := parents[rows[i].dirID]
		if !ok {
			return fmt.Errorf(
				"clickhouse: %s catalog dir_id %d: %w",
				op,
				rows[i].dirID,
				errFileCatalogDirNotFound,
			)
		}

		rows[i].ParentDir = parentDir
		rows[i].Path = parentDir + rows[i].Name
	}

	return nil
}

func fileRowDirIDs(rows []FileRow) []uint32 {
	seen := make(map[uint32]struct{}, len(rows))
	ids := make([]uint32, 0, len(rows))

	for _, row := range rows {
		if _, ok := seen[row.dirID]; ok {
			continue
		}

		seen[row.dirID] = struct{}{}
		ids = append(ids, row.dirID)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

func finishFindByGlob(in []FileRow) []FileRow {
	if len(in) < minDedupeByPathLen {
		return in
	}

	sort.Slice(in, func(i, j int) bool { return in[i].Path < in[j].Path })

	return dedupeByPath(in)
}

func dedupeByPath(in []FileRow) []FileRow {
	if len(in) < minDedupeByPathLen {
		return in
	}

	out := in[:1]
	last := in[0].Path

	for _, row := range in[1:] {
		if row.Path == last {
			continue
		}

		out = append(out, row)
		last = row.Path
	}

	return out
}

func sliceLimitOffset(in []FileRow, limit int64, offset int64) []FileRow {
	if offset > 0 {
		if offset >= int64(len(in)) {
			return []FileRow{}
		}

		in = in[offset:]
	}

	if limit >= int64(len(in)) {
		return in
	}

	return in[:limit]
}

type fileCatalogDirRef struct {
	dirID      uint32
	subtreeEnd uint32
	fullPath   string
}

func scanFileCatalogDirRef(rows fileRowIterator, normalised string) (fileCatalogDirRef, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fileCatalogDirRef{}, fmt.Errorf("clickhouse: catalog dir iteration error: %w", err)
		}

		return fileCatalogDirRef{}, errPathNotFound
	}

	var ref fileCatalogDirRef
	if err := rows.Scan(&ref.dirID, &ref.subtreeEnd, &ref.fullPath); err != nil {
		return fileCatalogDirRef{}, fmt.Errorf("clickhouse: failed to scan catalog dir: %w", err)
	}

	if ref.fullPath != normalised {
		return fileCatalogDirRef{}, errPathNotFound
	}

	return ref, nil
}

func (c *Client) resolveListDirRef(
	ctx context.Context,
	mountPath string,
	dir string,
) (fileCatalogDirRef, bool, error) {
	dirRef, err := c.resolveCatalogDir(ctx, mountPath, dir)
	if errors.Is(err, errPathNotFound) {
		return fileCatalogDirRef{}, false, nil
	}

	if err != nil {
		return fileCatalogDirRef{}, false, err
	}

	return dirRef, true, nil
}

func (c *Client) addBaseDirRef(
	ctx context.Context,
	seen map[string]map[string]struct{},
	out map[string][]fileCatalogDirRef,
	baseDir string,
) error {
	mountPath, normalised, err := c.resolveMountAndDir(baseDir)
	if err != nil {
		return err
	}

	if markSeenBaseDir(seen, mountPath, normalised) {
		return nil
	}

	ref, ok, err := c.resolveBaseDirRef(ctx, mountPath, normalised)
	if err != nil {
		return err
	}

	if ok {
		out[mountPath] = append(out[mountPath], ref)
	}

	return nil
}

func markSeenBaseDir(seen map[string]map[string]struct{}, mountPath string, dir string) bool {
	seenMount := seen[mountPath]
	if seenMount == nil {
		seenMount = make(map[string]struct{})
		seen[mountPath] = seenMount
	}

	if _, ok := seenMount[dir]; ok {
		return true
	}

	seenMount[dir] = struct{}{}

	return false
}

func (c *Client) resolveBaseDirRef(
	ctx context.Context,
	mountPath string,
	dir string,
) (fileCatalogDirRef, bool, error) {
	ref, err := c.resolveCatalogDir(ctx, mountPath, dir)
	if errors.Is(err, errPathNotFound) {
		return fileCatalogDirRef{}, false, nil
	}

	if err != nil {
		return fileCatalogDirRef{}, false, err
	}

	return ref, true, nil
}

func (c *Client) resolveCatalogDir(ctx context.Context, mountPath string, dir string) (fileCatalogDirRef, error) {
	normalised := ensureTrailingSlash(dir)

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(
		qctx,
		catalogDirByPathQuery,
		mountPath,
		mountPath,
		catalogPathHash(normalised),
		normalised,
	)
	if err != nil {
		return fileCatalogDirRef{}, fmt.Errorf("clickhouse: failed to query catalog dir: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanFileCatalogDirRef(rows, normalised)
}

type compiledGlobPattern struct {
	regex          string
	ext            string
	dirRegex       string
	dirSkipLiteral string
}

type unknownFileFieldError struct {
	Field string
}

func (e unknownFileFieldError) Error() string {
	return fmt.Sprintf("clickhouse: unknown file field %q", e.Field)
}

type fileRowInts struct {
	size         int64
	apparentSize int64
	inode        int64
	nlink        int64
}

type fileRowScanState struct {
	dirID     uint32
	path      string
	parentDir string
	name      string
	ext       string

	entryType uint8
	uid       uint32
	gid       uint32

	size         uint64
	apparentSize uint64
	inode        uint64
	nlink        uint64

	atime time.Time
	mtime time.Time
	ctime time.Time
}

func (s *fileRowScanState) destsFor(fields []string) ([]any, error) {
	if len(fields) == 0 {
		fields = defaultFileRowFields()
	}

	dests := make([]any, 0, len(fields))
	for _, f := range fields {
		dest, ok := s.destForField(f)
		if !ok {
			return nil, unknownFileFieldError{Field: f}
		}

		dests = append(dests, dest)
	}

	return dests, nil
}

func (s *fileRowScanState) destForField(field string) (any, bool) {
	if field == fileFieldDirID {
		return &s.dirID, true
	}

	spec, ok := fileRowFieldSpecFor(field)
	if !ok {
		return nil, false
	}

	return spec.scanDest(s), true
}

func (s *fileRowScanState) applyTo(out *FileRow) error {
	ints, err := s.intValues()
	if err != nil {
		return err
	}

	out.Path = s.path
	out.ParentDir = s.parentDir
	out.Name = s.name
	out.Ext = s.ext
	out.EntryType = s.entryType
	out.Size = ints.size
	out.ApparentSize = ints.apparentSize
	out.UID = s.uid
	out.GID = s.gid
	out.ATime = s.atime
	out.MTime = s.mtime
	out.CTime = s.ctime
	out.Inode = ints.inode
	out.Nlink = ints.nlink
	out.dirID = s.dirID

	return nil
}

func (s *fileRowScanState) intValues() (fileRowInts, error) {
	size, err := uint64ToInt64(s.size)
	if err != nil {
		return fileRowInts{}, err
	}

	apparentSize, err := uint64ToInt64(s.apparentSize)
	if err != nil {
		return fileRowInts{}, err
	}

	inode, err := uint64ToInt64(s.inode)
	if err != nil {
		return fileRowInts{}, err
	}

	nlink, err := uint64ToInt64(s.nlink)
	if err != nil {
		return fileRowInts{}, err
	}

	return fileRowInts{
		size:         size,
		apparentSize: apparentSize,
		inode:        inode,
		nlink:        nlink,
	}, nil
}

func uint64ToInt64(v uint64) (int64, error) {
	if v > uint64(math.MaxInt64) {
		return 0, errInvalidPath
	}

	return int64(v), nil
}

type fileRowFieldSpec struct {
	field    string
	column   string
	scanDest func(*fileRowScanState) any
}

type findByGlobExecPlan struct {
	queries []findByGlobQuerySpec
}

func findByGlobPlan(baseDirsByMount map[string][]fileCatalogDirRef, patterns []string) findByGlobExecPlan {
	patternChunks := chunkStrings(patterns, maxGlobPatternsPerQuery)
	queries := make([]findByGlobQuerySpec, 0, len(baseDirsByMount)*len(patternChunks))

	for mountPath, dirs := range baseDirsByMount {
		for _, chunk := range patternChunks {
			queries = append(queries, findByGlobQuerySpec{
				mountPath:    mountPath,
				baseDirs:     dirs,
				patternChunk: chunk,
			})
		}
	}

	return findByGlobExecPlan{queries: queries}
}

type findByGlobPrepared struct {
	selectList      string
	fields          []string
	plan            findByGlobExecPlan
	useDirectOffset bool
	limit           int64
	queryLimit      int64
	queryOffset     int64
	ownerEnabled    int64
	uid             uint32
	gids            []uint32
}

func newFindByGlobPrepared(
	selectList string,
	fields []string,
	plan findByGlobExecPlan,
	opts FindOptions,
) findByGlobPrepared {
	out := findByGlobPrepared{
		selectList:      selectList,
		fields:          fields,
		plan:            plan,
		useDirectOffset: len(plan.queries) == 1,
	}

	out.ownerEnabled, out.uid, out.gids = ownerFilterArgs(opts)
	out.limit = listLimit(opts.Limit)
	out.queryLimit, out.queryOffset = findByGlobQueryLimitOffset(
		out.limit,
		opts.Offset,
		out.useDirectOffset,
	)

	return out
}

func (c *Client) prepareFindByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	opts FindOptions,
) (findByGlobPrepared, error) {
	selectList, fields, err := fileRowPageSelectList(opts.Fields)
	if err != nil {
		return findByGlobPrepared{}, err
	}

	baseDirsByMount, err := c.groupBaseDirsByMount(ctx, baseDirs)
	if err != nil {
		return findByGlobPrepared{}, err
	}

	plan := findByGlobPlan(baseDirsByMount, patterns)

	return newFindByGlobPrepared(selectList, fields, plan, opts), nil
}

func (c *Client) prepareCountByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	opts FindOptions,
) (findByGlobPrepared, error) {
	baseDirsByMount, err := c.groupBaseDirsByMount(ctx, baseDirs)
	if err != nil {
		return findByGlobPrepared{}, err
	}

	return newFindByGlobPrepared("", nil, findByGlobPlan(baseDirsByMount, patterns), opts), nil
}

func (c *Client) runCountByGlobPlan(
	ctx context.Context,
	prepared findByGlobPrepared,
) (int64, error) {
	var total int64

	for _, q := range prepared.plan.queries {
		count, err := c.countByGlobQuery(ctx, prepared, q)
		if err != nil {
			return 0, err
		}

		total += count
	}

	return total, nil
}

func (c *Client) countByGlobQuery(
	ctx context.Context,
	prepared findByGlobPrepared,
	spec findByGlobQuerySpec,
) (int64, error) {
	q, params := buildCountByGlobQueryAndParams(
		spec.mountPath,
		spec.baseDirs,
		spec.patternChunk,
		prepared.ownerEnabled,
		prepared.uid,
		prepared.gids,
	)

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	var count uint64
	if err := c.conn.QueryRow(qctx, q, params...).Scan(&count); err != nil {
		return 0, fmt.Errorf("clickhouse: failed to query CountByGlob: %w", err)
	}

	if count > uint64(math.MaxInt64) {
		return 0, errInvalidPath
	}

	return int64(count), nil
}

func buildCountByGlobQueryAndParams(
	mountPath string,
	baseDirs []fileCatalogDirRef,
	patterns []string,
	ownerEnabled int64,
	uid uint32,
	gids []uint32,
) (string, []any) {
	baseDirClauses := make([]string, 0, len(baseDirs))
	params := make([]any, 0, findByGlobParamsSharedCap+len(baseDirs)*(len(patterns)+findByGlobParamsPerBaseDirCap))
	params = append(params, mountPath, mountPath)

	for _, baseDir := range baseDirs {
		compiled := compileGlobPatterns(baseDir.fullPath, patterns)
		clause, clauseParams := findByGlobBaseDirClause(mountPath, baseDir, compiled)
		baseDirClauses = append(baseDirClauses, clause)
		params = append(params, clauseParams...)
	}

	q := fmt.Sprintf(countByGlobQueryTemplate, strings.Join(baseDirClauses, " OR "))

	params = append(params, ownerEnabled, uid, gids)

	return q, params
}

type compiledGlobPatterns struct {
	direct    []compiledGlobPattern
	recursive []compiledGlobPattern
	matchAll  bool
}

// ListOptions controls ListDir behaviour.
type ListOptions struct {
	Fields []string
	Limit  int64
	Offset int64
}

// StatOptions controls StatPath behaviour.
type StatOptions struct {
	Fields []string
}

// FindOptions controls FindByGlob behaviour.
type FindOptions struct {
	Fields       []string
	Limit        int64
	Offset       int64
	RequireOwner bool
	UID          uint32
	GIDs         []uint32
}

// CountByGlob counts rows matching gitignore-style patterns under base
// directories without materialising file rows when the prepared plan can be
// counted without cross-query de-duplication.
func (c *Client) CountByGlob(
	ctx context.Context,
	baseDirs []string,
	patterns []string,
	opts FindOptions,
) (int, error) {
	if c == nil || c.conn == nil {
		return 0, errClientClosed
	}

	if len(patterns) == 0 {
		return 0, nil
	}

	if len(patterns) > maxGlobPatternsPerQuery {
		rows, err := c.FindByGlob(ctx, baseDirs, patterns, opts)

		return len(rows), err
	}

	prepared, err := c.prepareCountByGlob(ctx, baseDirs, patterns, opts)
	if err != nil {
		return 0, err
	}

	count, err := c.runCountByGlobPlan(ctx, prepared)
	if err != nil {
		return 0, err
	}

	return limitOffsetCount(count, prepared.limit, opts.Offset), nil
}

func limitOffsetCount(count int64, limit int64, offset int64) int {
	if offset >= count {
		return 0
	}

	count -= max(offset, 0)
	if count > limit {
		count = limit
	}

	return int(count)
}

func ownerFilterArgs(opts FindOptions) (int64, uint32, []uint32) {
	ownerEnabled := int64(0)
	if opts.RequireOwner {
		ownerEnabled = 1
	}

	return ownerEnabled, opts.UID, ensureNonNilUInt32s(opts.GIDs)
}

func ensureNonNilUInt32s(in []uint32) []uint32 {
	if in == nil {
		return []uint32{}
	}

	return in
}

type fileRowScanner interface {
	Scan(dest ...any) error
}

type fileRowIterator interface {
	fileRowScanner
	Next() bool
	Err() error
}

func scanFileRowParentDirs(rows fileRowIterator, op string) (map[uint32]string, error) {
	out := make(map[uint32]string)

	for rows.Next() {
		var (
			dirID    uint32
			fullPath string
		)

		if err := rows.Scan(&dirID, &fullPath); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan %s catalog paths: %w", op, err)
		}

		out[dirID] = fullPath
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: %s catalog paths iteration error: %w", op, err)
	}

	return out, nil
}

func isDirFromRows(rows fileRowIterator) (bool, error) {
	entryType, ok, err := isDirEntryType(rows)
	if err != nil || !ok {
		return false, err
	}

	return entryType == uint8(stats.DirType), nil
}

func isDirEntryType(rows fileRowIterator) (uint8, bool, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("clickhouse: IsDir iteration error: %w", err)
		}

		return 0, false, nil
	}

	var entryType uint8
	if err := rows.Scan(&entryType); err != nil {
		return 0, false, fmt.Errorf("clickhouse: failed to scan IsDir row: %w", err)
	}

	return entryType, true, nil
}

func permissionRowsOK(rows fileRowIterator, op string) (bool, error) {
	ok := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: %s iteration error: %w", op, err)
	}

	return ok, nil
}

func catalogDirsByIDQueryAndParams(mountPath string, dirIDs []uint32) (string, []any) {
	params := make([]any, 0, len(dirIDs)+catalogDirsByIDBaseParamCount)
	params = append(params, mountPath, mountPath)

	for _, dirID := range dirIDs {
		params = append(params, dirID)
	}

	return fmt.Sprintf(catalogDirsByIDQueryTemplate, questionPlaceholders(len(dirIDs))), params
}

func questionPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}

	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}

	return strings.Join(parts, ", ")
}

func findByGlobQueryLimitOffset(limit int64, offset int64, useDirectOffset bool) (int64, int64) {
	if useDirectOffset {
		return limit, offset
	}

	if offset <= 0 {
		return limit, 0
	}

	if limit > math.MaxInt64-offset {
		return math.MaxInt64, 0
	}

	return limit + offset, 0
}

func chunkStrings(in []string, maxChunk int) [][]string {
	if maxChunk <= 0 || len(in) == 0 {
		return nil
	}

	if len(in) <= maxChunk {
		return [][]string{in}
	}

	out := make([][]string, 0, (len(in)+maxChunk-1)/maxChunk)
	for start := 0; start < len(in); start += maxChunk {
		end := min(start+maxChunk, len(in))
		out = append(out, in[start:end])
	}

	return out
}

func splitPathParentAndName(path string) (string, string, bool) {
	if path == "" {
		return "", "", false
	}

	idx := lastComponentSlashIndex(path)
	if idx < 0 {
		return "", "", false
	}

	parentDir := path[:idx+1]
	name := path[idx+1:]

	if parentDir == "" || name == "" {
		return "", "", false
	}

	return parentDir, name, true
}

func lastComponentSlashIndex(path string) int {
	if strings.HasSuffix(path, "/") && path != "/" {
		return strings.LastIndex(path[:len(path)-1], "/")
	}

	return strings.LastIndex(path, "/")
}

func permissionAnyInDirArgs(mountPath string, dirID uint32, uid uint32, gids []uint32) []any {
	return []any{
		mountPath,
		mountPath,
		dirID,
		uint8(db.DGUTAgeAll),
		uid,
		gids,
	}
}

func (c *Client) groupBaseDirsByMount(ctx context.Context, baseDirs []string) (map[string][]fileCatalogDirRef, error) {
	if len(baseDirs) == 0 {
		return map[string][]fileCatalogDirRef{}, nil
	}

	seen := make(map[string]map[string]struct{})
	out := make(map[string][]fileCatalogDirRef)

	for _, bd := range baseDirs {
		if err := c.addBaseDirRef(ctx, seen, out, bd); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (c *Client) fileRowParentDirs(
	ctx context.Context,
	op string,
	mountPath string,
	dirIDs []uint32,
) (map[uint32]string, error) {
	query, params := catalogDirsByIDQueryAndParams(mountPath, dirIDs)

	rows, err := c.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query %s catalog paths: %w", op, err)
	}

	defer func() { _ = rows.Close() }()

	return scanFileRowParentDirs(rows, op)
}

func (c *Client) resolveMountAndDir(dir string) (string, string, error) {
	mountPath := c.mountPoints.PrefixOf(dir)
	if mountPath == "" {
		return "", "", basedirs.ErrInvalidBasePath
	}

	return mountPath, ensureTrailingSlash(dir), nil
}

// IsDir reports whether the given path exists and is a directory.
func (c *Client) IsDir(ctx context.Context, path string) (bool, error) {
	if c == nil || c.conn == nil {
		return false, errClientClosed
	}

	mountPath, parentDir, name, err := c.resolveMountParentName(path)
	if err != nil {
		return false, err
	}

	parent, err := c.resolveCatalogDir(ctx, mountPath, parentDir)
	if errors.Is(err, errPathNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, isDirQuery, mountPath, mountPath, parent.dirID, name)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query IsDir: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return isDirFromRows(rows)
}

// PermissionAnyInDir reports whether, in the active snapshot for the mount
// containing dir, there exists any directory summary row indicating ownership by
// uid or any gid in gids.
func (c *Client) PermissionAnyInDir(ctx context.Context, dir string, uid uint32, gids []uint32) (bool, error) {
	if c == nil || c.conn == nil {
		return false, errClientClosed
	}

	mountPath, normalisedDir, err := c.resolveMountAndDir(dir)
	if err != nil {
		return false, err
	}

	gids = ensureNonNilUInt32s(gids)

	dirRef, err := c.resolveCatalogDir(ctx, mountPath, normalisedDir)
	if errors.Is(err, errPathNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return c.permissionAnyInDir(ctx, mountPath, dirRef.dirID, uid, gids)
}

func (c *Client) permissionAnyInDir(
	ctx context.Context,
	mountPath string,
	dirID uint32,
	uid uint32,
	gids []uint32,
) (bool, error) {
	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(
		qctx,
		permissionAnyInDirQuery,
		permissionAnyInDirArgs(mountPath, dirID, uid, gids)...,
	)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query PermissionAnyInDir: %w", err)
	}

	defer func() { _ = rows.Close() }()

	ok := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("clickhouse: PermissionAnyInDir iteration error: %w", err)
	}

	return ok, nil
}

// PermissionPath reports whether the active snapshot row for path is owned by
// uid or by any gid in gids.
func (c *Client) PermissionPath(ctx context.Context, path string, uid uint32, gids []uint32) (bool, error) {
	if c == nil || c.conn == nil {
		return false, errClientClosed
	}

	mountPath, parentDir, name, err := c.resolveMountParentName(path)
	if err != nil {
		return false, err
	}

	parent, err := c.resolveCatalogDir(ctx, mountPath, parentDir)
	if errors.Is(err, errPathNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return c.permissionPath(ctx, mountPath, parent.dirID, name, uid, ensureNonNilUInt32s(gids))
}

func (c *Client) permissionPath(
	ctx context.Context,
	mountPath string,
	dirID uint32,
	name string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(
		qctx,
		permissionPathQuery,
		mountPath,
		mountPath,
		dirID,
		name,
		uid,
		gids,
	)
	if err != nil {
		return false, fmt.Errorf("clickhouse: failed to query PermissionPath: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return permissionRowsOK(rows, "PermissionPath")
}

func (c *Client) resolveMountParentName(path string) (string, string, string, error) {
	mountPath := c.mountPoints.PrefixOf(path)
	if mountPath == "" {
		return "", "", "", basedirs.ErrInvalidBasePath
	}

	parentDir, name, ok := splitPathParentAndName(path)
	if !ok {
		return "", "", "", errInvalidPath
	}

	return mountPath, parentDir, name, nil
}
