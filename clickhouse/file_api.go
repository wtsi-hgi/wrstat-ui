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
	errClientClosed = errors.New("clickhouse: client is closed")
	errPathNotFound = errors.New("clickhouse: path not found")
	errInvalidPath  = errors.New("clickhouse: invalid path")
)

const (
	fileRowSelectAll = "f.path, f.parent_dir, f.name, f.ext, f.entry_type, f.size, " +
		"f.apparent_size, f.uid, f.gid, f.atime, f.mtime, f.ctime, f.inode, f.nlink"
)

const statPathQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? AND f.name = ? LIMIT 1"

const listDirQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? ORDER BY f.name ASC LIMIT ? OFFSET ?"

const findByGlobQueryTemplate = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT %s FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"WHERE (%s) " +
	"AND (? = 0 OR f.uid = ? OR has(?, f.gid)) " +
	"ORDER BY f.parent_dir ASC, f.name ASC LIMIT ? OFFSET ?"

const isDirQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT f.entry_type FROM wrstat_files f PREWHERE f.mount_path = ? AND f.snapshot_id = sid " +
	"AND f.parent_dir = ? AND f.name = ? LIMIT 1"

const permissionAnyInDirQuery = "WITH (SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?) AS sid " +
	"SELECT 1 FROM wrstat_dguta d PREWHERE d.mount_path = ? AND d.snapshot_id = sid " +
	"AND d.dir = ? AND d.age = ? AND (d.uid = ? OR has(?, d.gid)) LIMIT 1"

const defaultFileLimit = 1_000_000

const (
	maxGlobPatternsPerQuery       = 32
	findByGlobParamsPerBaseDirCap = 2
	findByGlobParamsSharedCap     = 7
	findByGlobClauseCap           = 2
	minDedupeByPathLen            = 2
	growExtraForAnchors           = 2
	maxByte                       = 0xFF
)

const (
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

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, q, mountPath, mountPath, parentDir, name)
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
	specs := fileRowFieldSpecs()
	out := make([]string, 0, len(specs))

	for _, spec := range specs {
		out = append(out, spec.field)
	}

	return out
}

func fileRowFieldSpecs() []fileRowFieldSpec {
	return []fileRowFieldSpec{
		{fileFieldPath, "f.path", func(s *fileRowScanState) any { return &s.path }},
		{fileFieldParentDir, "f.parent_dir", func(s *fileRowScanState) any { return &s.parentDir }},
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
}

func fileRowFieldSpecFor(field string) (fileRowFieldSpec, bool) {
	for _, spec := range fileRowFieldSpecs() {
		if spec.field == field {
			return spec, true
		}
	}

	return fileRowFieldSpec{}, false
}

// ListDir lists direct children (by parent_dir) for the given directory.
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

	limit := listLimit(opts.Limit)

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	return c.queryFileRows(
		qctx,
		"ListDir",
		q,
		fields,
		mountPath,
		mountPath,
		parentDir,
		limit,
		opts.Offset,
	)
}

func listDirQuery(opts ListOptions) (string, []string, error) {
	return fileRowQuery(listDirQueryTemplate, opts.Fields)
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

	prepared, err := c.prepareFindByGlob(baseDirs, patterns, opts)
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
	baseDirs     []string
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

	return c.queryFileRows(qctx, "FindByGlob", q, prepared.fields, params...)
}

func buildFindByGlobQueryAndParams(
	selectList string,
	mountPath string,
	baseDirs []string,
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
		compiled := compileGlobPatterns(baseDir, patterns)
		clause, clauseParams := findByGlobBaseDirClause(baseDir, compiled)
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
		direct:    make([]string, 0, len(patterns)),
		recursive: make([]string, 0, len(patterns)),
	}

	for _, p := range patterns {
		if globPatternMatchesWholeSubtree(p) {
			out.matchAll = true

			continue
		}

		if isDirectChildGlobPattern(p) {
			out.direct = append(out.direct, globToRE2("", p))

			continue
		}

		out.recursive = append(out.recursive, globToRE2(escapedBase, p))
	}

	return out
}

func globPatternMatchesWholeSubtree(pattern string) bool {
	return pattern == "**" || pattern == "**/*"
}

func isDirectChildGlobPattern(pattern string) bool {
	return !strings.Contains(pattern, "/") && !strings.Contains(pattern, "**")
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

func findByGlobBaseDirClause(baseDir string, compiled compiledGlobPatterns) (string, []any) {
	if compiled.matchAll {
		return findByGlobRangeClause(), []any{baseDir, prefixNext(baseDir)}
	}

	clauses := make([]string, 0, findByGlobClauseCap)
	params := make([]any, 0, len(compiled.direct)+len(compiled.recursive)+findByGlobParamsPerBaseDirCap)

	if len(compiled.direct) > 0 {
		clauses = append(clauses, "(f.parent_dir = ? AND ("+matchOrList("f.name", len(compiled.direct))+"))")
		params = append(params, baseDir)
		params = appendStringsAsAny(params, compiled.direct)
	}

	if len(compiled.recursive) > 0 {
		clauses = append(clauses, findByGlobRangeClause()+" AND ("+matchOrList("f.path", len(compiled.recursive))+")")
		params = append(params, baseDir, prefixNext(baseDir))
		params = appendStringsAsAny(params, compiled.recursive)
	}

	if len(clauses) == 0 {
		return "(0)", nil
	}

	return "(" + strings.Join(clauses, " OR ") + ")", params
}

func findByGlobRangeClause() string {
	return "(f.parent_dir >= ? AND f.parent_dir < ?)"
}

func prefixNext(prefix string) string {
	if prefix == "" {
		return "\x00"
	}

	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != maxByte {
			b[i]++

			return string(b[:i+1])
		}
	}

	return prefix + "\x00"
}

func matchOrList(column string, n int) string {
	if n <= 0 {
		return "0"
	}

	out := make([]string, 0, n)
	for range n {
		out = append(out, "match("+column+", ?)")
	}

	return strings.Join(out, " OR ")
}

func appendStringsAsAny(out []any, in []string) []any {
	for _, s := range in {
		out = append(out, s)
	}

	return out
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

func findByGlobPlan(baseDirsByMount map[string][]string, patterns []string) findByGlobExecPlan {
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
	baseDirs []string,
	patterns []string,
	opts FindOptions,
) (findByGlobPrepared, error) {
	selectList, fields, err := fileRowSelectList(opts.Fields)
	if err != nil {
		return findByGlobPrepared{}, err
	}

	baseDirsByMount, err := c.groupBaseDirsByMount(baseDirs)
	if err != nil {
		return findByGlobPrepared{}, err
	}

	plan := findByGlobPlan(baseDirsByMount, patterns)

	return newFindByGlobPrepared(selectList, fields, plan, opts), nil
}

type compiledGlobPatterns struct {
	direct    []string
	recursive []string
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

func permissionAnyInDirArgs(mountPath string, dir string, uid uint32, gids []uint32) []any {
	return []any{
		mountPath,
		mountPath,
		dir,
		uint8(db.DGUTAgeAll),
		uid,
		gids,
	}
}

func (c *Client) groupBaseDirsByMount(baseDirs []string) (map[string][]string, error) {
	if len(baseDirs) == 0 {
		return map[string][]string{}, nil
	}

	seen := make(map[string]map[string]struct{})
	out := make(map[string][]string)

	for _, bd := range baseDirs {
		mountPath, normalised, err := c.resolveMountAndDir(bd)
		if err != nil {
			return nil, err
		}

		seenMount := seen[mountPath]
		if seenMount == nil {
			seenMount = make(map[string]struct{})
			seen[mountPath] = seenMount
		}

		if _, ok := seenMount[normalised]; ok {
			continue
		}

		seenMount[normalised] = struct{}{}
		out[mountPath] = append(out[mountPath], normalised)
	}

	return out, nil
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

	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(qctx, isDirQuery, mountPath, mountPath, parentDir, name)
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

	return c.permissionAnyInDir(ctx, mountPath, normalisedDir, uid, gids)
}

func (c *Client) permissionAnyInDir(
	ctx context.Context,
	mountPath string,
	dir string,
	uid uint32,
	gids []uint32,
) (bool, error) {
	qctx, cancel := queryContext(ctx, queryTimeout(c.cfg))
	defer cancel()

	rows, err := c.conn.Query(
		qctx,
		permissionAnyInDirQuery,
		permissionAnyInDirArgs(mountPath, dir, uid, gids)...,
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
