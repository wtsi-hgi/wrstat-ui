/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package dirbuild

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const maxStatsLineLength = 64 * 1024

var newDirIDAllocator = func() dirIDAllocator { //nolint:gochecknoglobals
	return summary.NewDirIDAllocator()
}

const (
	rawStatsColumnCount = 12
	colPath             = 0
	colSize             = 1
	colUID              = 2
	colGID              = 3
	colATime            = 4
	colMTime            = 5
	colCTime            = 6
	colEntryType        = 7
	colInode            = 8
	colNlink            = 9
	colApparentSize     = 11
)

type dirIDAllocator interface {
	SetMountPath(mountPath string) error
	Enter(dir *summary.DirectoryPath) (uint32, error)
	Leave(dir *summary.DirectoryPath) (uint32, error)
}

func replaceDirIDAllocatorForTest(replacement dirIDAllocator) func() {
	previous := newDirIDAllocator
	newDirIDAllocator = func() dirIDAllocator {
		return replacement
	}

	return func() {
		newDirIDAllocator = previous
	}
}

// FileSink receives each stats row during BuildWithFiles pass 2 with the
// catalog dir_id that should own the file-table row.
type FileSink func(dirID uint32, info summary.FileInfo) error

// BuildWithFiles runs Build and also streams pass-2 file-table rows to files.
func BuildWithFiles(
	open func() (io.ReadCloser, error),
	mountPath string,
	database dirguta.DB,
	refTime time.Time,
	files FileSink,
) error {
	index, err := buildDirectoryIndex(open, mountPath, refTime.Unix())
	if err != nil {
		return err
	}

	if err := addStatsRows(open, index, refTime.Unix(), files); err != nil {
		return err
	}

	rollUp(index)

	return emit(index, database)
}

func buildDirectoryIndex(
	open func() (io.ReadCloser, error),
	mountPath string,
	refUnix int64,
) (*directoryIndex, error) {
	paths := newPathBuilder()
	paths.dir(cleanDirPath(mountPath))

	if err := collectDirectoryRows(open, paths); err != nil {
		return nil, err
	}

	nodes, err := assignDirectoryIDs(paths, mountPath, refUnix)
	if err != nil {
		return nil, err
	}

	return indexNodes(nodes), nil
}

func newPathBuilder() *pathBuilder {
	root := &summary.DirectoryPath{Name: "/", Depth: 0}

	return &pathBuilder{
		paths: map[string]*summary.DirectoryPath{"/": root},
		keys:  map[*summary.DirectoryPath]string{root: "/"},
	}
}

func cleanDirPath(path string) string {
	if path == "" {
		return "/"
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return path
}

func collectDirectoryRows(open func() (io.ReadCloser, error), paths *pathBuilder) error {
	return withStatsReader(open, func(reader io.Reader) error {
		return scanRawStats(reader, func(row rawStatsRow) error {
			if !row.isDir() {
				return nil
			}

			paths.dir(row.dirKey())

			return nil
		})
	})
}

func withStatsReader(open func() (io.ReadCloser, error), fn func(io.Reader) error) error {
	reader, err := open()
	if err != nil {
		return err
	}

	fnErr := fn(reader)
	closeErr := reader.Close()

	return errors.Join(fnErr, closeErr)
}

func scanRawStats(reader io.Reader, handle func(rawStatsRow) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, maxStatsLineLength), maxStatsLineLength)

	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}

		row, err := parseRawStatsLine(scanner.Bytes())
		if err != nil {
			return err
		}

		if err := handle(row); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func parseRawStatsLine(line []byte) (rawStatsRow, error) {
	cols := bytes.Split(line, []byte{'\t'})
	if len(cols) < rawStatsColumnCount {
		return rawStatsRow{}, stats.ErrTooFewColumns
	}

	row, err := newRawStatsRow(cols)
	if err != nil {
		return rawStatsRow{}, err
	}

	if err := row.parseNumberColumns(cols); err != nil {
		return rawStatsRow{}, err
	}

	return row, nil
}

func newRawStatsRow(cols [][]byte) (rawStatsRow, error) {
	path, err := strconv.Unquote(string(cols[colPath]))
	if err != nil {
		return rawStatsRow{}, fmt.Errorf("parse stats path: %w", err)
	}

	row := rawStatsRow{path: path, entryType: firstByte(cols[colEntryType])}
	if strings.HasSuffix(row.path, "/") {
		row.entryType = stats.DirType
	}

	return row, nil
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}

	return b[0]
}

func assignDirectoryIDs(paths *pathBuilder, mountPath string, refUnix int64) ([]*dirNode, error) {
	dirs := paths.sorted()

	alloc := newDirIDAllocator()
	if err := alloc.SetMountPath(mountPath); err != nil {
		return nil, err
	}

	state := newIDAssignmentState(alloc, paths, refUnix, len(dirs))

	for _, dir := range dirs {
		if err := state.enterNext(dir); err != nil {
			return nil, err
		}
	}

	if err := state.closeRemaining(); err != nil {
		return nil, err
	}

	populateChildren(state.nodes, state.nodesByPath)

	return state.nodes, nil
}

func newIDAssignmentState(
	alloc dirIDAllocator,
	paths *pathBuilder,
	refUnix int64,
	dirCount int,
) *idAssignmentState {
	return &idAssignmentState{
		alloc:       alloc,
		paths:       paths,
		refUnix:     refUnix,
		nodes:       make([]*dirNode, 0, dirCount),
		stack:       make([]*dirNode, 0, dirCount),
		nodesByPath: make(map[string]*dirNode, dirCount),
	}
}

func populateChildren(nodes []*dirNode, nodesByPath map[string]*dirNode) {
	for _, node := range nodes {
		if node.dir.Parent == nil {
			continue
		}

		parent := nodesByPath[string(node.dir.Parent.AppendTo(nil))]
		if parent == nil {
			continue
		}

		parent.children = append(parent.children, node.dir.Name)
		parent.childCount++
	}
}

func indexNodes(nodes []*dirNode) *directoryIndex {
	index := &directoryIndex{
		nodes:    nodes,
		byID:     make(map[uint32]*dirNode, len(nodes)),
		pathToID: make(map[string]uint32, len(nodes)),
	}

	for _, node := range nodes {
		index.byID[node.dirID] = node
		index.pathToID[node.key] = node.dirID
	}

	return index
}

func rollUp(index *directoryIndex) {
	for i := len(index.nodes) - 1; i >= 0; i-- {
		node := index.nodes[i]
		node.gutas = materializeGUTAs(node.store)

		parent := index.byID[node.parentID]
		if parent == nil {
			continue
		}

		dirguta.MergeSeenHardlinks(&parent.store, parent.seenHardlinks, &node.store, node.seenHardlinks)
		node.store.DrainInto(&parent.store)
	}
}

func materializeGUTAs(store dirguta.GUTAStore) db.GUTAs {
	keys := store.Sort()
	gutas := make(db.GUTAs, 0, len(keys))

	for _, key := range keys {
		gutas = append(gutas, dirguta.GetGUTA(store, key))
	}

	return gutas
}

func emit(index *directoryIndex, database dirguta.DB) error {
	slices.SortFunc(index.nodes, func(a, b *dirNode) int {
		return int(a.dirID) - int(b.dirID)
	})

	for _, node := range index.nodes {
		if err := database.Add(db.RecordDGUTA{
			Dir:            node.dir,
			DirID:          node.dirID,
			ParentID:       node.parentID,
			SubtreeEnd:     node.subtreeEnd,
			Depth:          node.depth,
			GUTAs:          node.gutas,
			Children:       node.children,
			ChildCount:     node.childCount,
			ChildFileCount: node.childFileCount,
		}); err != nil {
			return err
		}

		node.store.Clear()
	}

	return nil
}

func addStatsRows(open func() (io.ReadCloser, error), index *directoryIndex, refUnix int64, files FileSink) error {
	return withStatsReader(open, func(reader io.Reader) error {
		return scanRawStats(reader, func(row rawStatsRow) error {
			return addStatsRow(row, index, refUnix, files)
		})
	})
}

func addStatsRow(row rawStatsRow, index *directoryIndex, refUnix int64, files FileSink) error {
	node, err := index.resolve(row)
	if err != nil {
		return err
	}

	info := row.fileInfo(node.dir)
	if !info.IsDir() {
		node.childFileCount++
	}

	if err := addFileRow(files, node, info); err != nil {
		return err
	}

	addToNode(node, &info, refUnix)

	return nil
}

func addToNode(node *dirNode, info *summary.FileInfo, refUnix int64) {
	ft := dirguta.FileTypeWithTemp(info.Name, node.isTempDir)

	atime := info.ATime
	if info.IsDir() {
		atime = refUnix
	}

	if dirguta.HandleHardlink(&node.store, node.seenHardlinks, info, ft, atime) {
		return
	}

	gutaKeysA := dirguta.GUTAKeyPool.Get().(*[dirguta.MaxNumOfGUTAKeys]dirguta.GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := dirguta.GUTAKeys(gutaKeysA[:0])
	gutaKeys.Append(info.GID, info.UID, ft)
	node.store.AddForEach(gutaKeys, info.Size, atime, max(0, info.MTime))
	dirguta.GUTAKeyPool.Put(gutaKeysA)
}

func addFileRow(files FileSink, node *dirNode, info summary.FileInfo) error {
	if files == nil {
		return nil
	}

	return files(fileRowDirID(node, info), info)
}

func fileRowDirID(node *dirNode, info summary.FileInfo) uint32 {
	if info.IsDir() {
		return node.parentID
	}

	return node.dirID
}

type dirNode struct {
	dir            *summary.DirectoryPath
	key            string
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	store          dirguta.GUTAStore
	seenHardlinks  map[int64]*dirguta.InodeEntry
	gutas          db.GUTAs
	children       []string
	childCount     uint64
	childFileCount uint64
	isTempDir      bool
}

func enterNode(
	alloc dirIDAllocator,
	dir *summary.DirectoryPath,
	key string,
	nodesByPath map[string]*dirNode,
	refUnix int64,
) (*dirNode, error) {
	parentID, err := parentID(dir, nodesByPath)
	if err != nil {
		return nil, err
	}

	dirID, err := alloc.Enter(dir)
	if err != nil {
		return nil, err
	}

	return &dirNode{
		dir:           dir,
		key:           key,
		dirID:         dirID,
		parentID:      parentID,
		depth:         uint16(dir.Depth), //nolint:gosec // stats paths are bounded by max path length.
		store:         dirguta.NewGUTAStore(refUnix),
		seenHardlinks: make(map[int64]*dirguta.InodeEntry),
		isTempDir:     isTempDirectory(dir, nodesByPath),
	}, nil
}

func parentID(dir *summary.DirectoryPath, nodesByPath map[string]*dirNode) (uint32, error) {
	if dir.Parent == nil {
		return summary.ReservedParentIDForDepth(dir.Depth)
	}

	parent, ok := nodesByPath[string(dir.Parent.AppendTo(nil))]
	if !ok {
		return 0, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, dir.Parent.AppendTo(nil))
	}

	return parent.dirID, nil
}

func isTempDirectory(dir *summary.DirectoryPath, nodesByPath map[string]*dirNode) bool {
	if dir == nil {
		return false
	}

	if dirguta.IsTemp([]byte(dir.Name)) {
		return true
	}

	if dir.Parent == nil {
		return false
	}

	parent, ok := nodesByPath[string(dir.Parent.AppendTo(nil))]

	return ok && parent.isTempDir
}

type pathBuilder struct {
	paths map[string]*summary.DirectoryPath
	keys  map[*summary.DirectoryPath]string
}

func (p *pathBuilder) dir(path string) *summary.DirectoryPath {
	key := cleanDirPath(path)
	if dir, ok := p.paths[key]; ok {
		return dir
	}

	parentKey := parentDirKey(key)
	parent := p.dir(parentKey)
	dir := &summary.DirectoryPath{
		Name:   dirName(key),
		Depth:  parent.Depth + 1,
		Parent: parent,
	}

	p.paths[key] = dir
	p.keys[dir] = key

	return dir
}

func parentDirKey(path string) string {
	path = strings.TrimSuffix(cleanDirPath(path), "/")

	idx := strings.LastIndexByte(path, '/')
	if idx <= 0 {
		return "/"
	}

	return path[:idx+1]
}

func dirName(path string) string {
	if path == "/" {
		return "/"
	}

	trimmed := strings.TrimSuffix(path, "/")
	idx := strings.LastIndexByte(trimmed, '/')

	return path[idx+1:]
}

func (p *pathBuilder) sorted() []*summary.DirectoryPath {
	dirs := make([]*summary.DirectoryPath, 0, len(p.paths))
	for _, dir := range p.paths {
		dirs = append(dirs, dir)
	}

	slices.SortFunc(dirs, func(a, b *summary.DirectoryPath) int {
		if a.Less(b) {
			return -1
		}

		if b.Less(a) {
			return 1
		}

		return 0
	})

	return dirs
}

func (p *pathBuilder) key(dir *summary.DirectoryPath) string {
	return p.keys[dir]
}

type idAssignmentState struct {
	alloc       dirIDAllocator
	paths       *pathBuilder
	refUnix     int64
	nodes       []*dirNode
	stack       []*dirNode
	nodesByPath map[string]*dirNode
}

func (s *idAssignmentState) enterNext(dir *summary.DirectoryPath) error {
	if err := s.closeCompletedAncestors(dir); err != nil {
		return err
	}

	node, err := enterNode(s.alloc, dir, s.paths.key(dir), s.nodesByPath, s.refUnix)
	if err != nil {
		return err
	}

	s.nodes = append(s.nodes, node)
	s.nodesByPath[node.key] = node
	s.stack = append(s.stack, node)

	return nil
}

func (s *idAssignmentState) closeCompletedAncestors(dir *summary.DirectoryPath) error {
	for len(s.stack) > 0 && !isAncestor(s.stack[len(s.stack)-1].dir, dir) {
		if err := leaveNode(s.alloc, s.stack[len(s.stack)-1]); err != nil {
			return err
		}

		s.stack = s.stack[:len(s.stack)-1]
	}

	return nil
}

func isAncestor(ancestor *summary.DirectoryPath, dir *summary.DirectoryPath) bool {
	if ancestor == nil || dir == nil || ancestor.Depth >= dir.Depth {
		return false
	}

	for dir.Depth > ancestor.Depth {
		dir = dir.Parent
	}

	return dir == ancestor
}

func leaveNode(alloc dirIDAllocator, node *dirNode) error {
	subtreeEnd, err := alloc.Leave(node.dir)
	if err != nil {
		return err
	}

	node.subtreeEnd = subtreeEnd

	return nil
}

func (s *idAssignmentState) closeRemaining() error {
	for i := len(s.stack) - 1; i >= 0; i-- {
		if err := leaveNode(s.alloc, s.stack[i]); err != nil {
			return err
		}
	}

	return nil
}

type intColumnTarget struct {
	column int
	value  *int64
}

type uint32ColumnTarget struct {
	column int
	value  *uint32
}

type rawStatsRow struct {
	path         string
	size         int64
	apparentSize int64
	uid          uint32
	gid          uint32
	mtime        int64
	atime        int64
	ctime        int64
	entryType    byte
	inode        int64
	nlink        int64
}

func (r *rawStatsRow) parseNumberColumns(cols [][]byte) error {
	if err := parseIntColumns(cols, []intColumnTarget{
		{column: colSize, value: &r.size},
		{column: colATime, value: &r.atime},
		{column: colMTime, value: &r.mtime},
		{column: colCTime, value: &r.ctime},
		{column: colInode, value: &r.inode},
		{column: colNlink, value: &r.nlink},
		{column: colApparentSize, value: &r.apparentSize},
	}); err != nil {
		return err
	}

	return parseUint32Columns(cols, []uint32ColumnTarget{
		{column: colUID, value: &r.uid},
		{column: colGID, value: &r.gid},
	})
}

func parseIntColumns(cols [][]byte, targets []intColumnTarget) error {
	for _, target := range targets {
		value, err := parseIntColumn(cols[target.column])
		if err != nil {
			return err
		}

		*target.value = value
	}

	return nil
}

func parseUint32Columns(cols [][]byte, targets []uint32ColumnTarget) error {
	for _, target := range targets {
		value, err := parseUint32Column(cols[target.column])
		if err != nil {
			return err
		}

		*target.value = value
	}

	return nil
}

func (r rawStatsRow) isDir() bool {
	return r.entryType == stats.DirType || strings.HasSuffix(r.path, "/")
}

func (r rawStatsRow) dirKey() string {
	return cleanDirPath(r.path)
}

func (r rawStatsRow) leafDirKey() string {
	if r.isDir() {
		return r.dirKey()
	}

	path := cleanFilePath(r.path)

	idx := strings.LastIndexByte(path, '/')
	if idx <= 0 {
		return "/"
	}

	return cleanDirPath(path[:idx+1])
}

func cleanFilePath(path string) string {
	if path == "" {
		return "/"
	}

	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}

	return path
}

func (r rawStatsRow) fileInfo(dir *summary.DirectoryPath) summary.FileInfo {
	entryType := r.entryType

	name := fileName(r.path)
	if r.isDir() {
		entryType = stats.DirType
		name = []byte(dir.Name)
	}

	return summary.FileInfo{
		Path:         dir,
		Name:         name,
		Size:         r.size,
		ApparentSize: r.apparentSize,
		UID:          r.uid,
		GID:          r.gid,
		MTime:        r.mtime,
		ATime:        r.atime,
		CTime:        r.ctime,
		Inode:        r.inode,
		Nlink:        r.nlink,
		EntryType:    entryType,
	}
}

func fileName(path string) []byte {
	path = cleanFilePath(path)
	if path == "/" {
		return []byte("/")
	}

	idx := strings.LastIndexByte(strings.TrimSuffix(path, "/"), '/')
	if idx < 0 {
		return []byte(path)
	}

	return []byte(path[idx+1:])
}

type directoryIndex struct {
	nodes    []*dirNode
	byID     map[uint32]*dirNode
	pathToID map[string]uint32
}

func (index *directoryIndex) resolve(row rawStatsRow) (*dirNode, error) {
	dirID, ok := index.pathToID[row.leafDirKey()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, row.leafDirKey())
	}

	return index.byID[dirID], nil
}

// Build runs the two-pass directory-centric build over the stats stream,
// emitting RecordDGUTA rows to db in ascending dir_id order. open is called
// once per pass to obtain a fresh reader of the same stats stream.
func Build(
	open func() (io.ReadCloser, error),
	mountPath string,
	database dirguta.DB,
	refTime time.Time,
) error {
	return BuildWithFiles(open, mountPath, database, refTime, nil)
}

func parseIntColumn(col []byte) (int64, error) {
	value, err := strconv.ParseInt(string(col), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse stats number %q: %w", col, err)
	}

	return value, nil
}

func parseUint32Column(col []byte) (uint32, error) {
	value, err := strconv.ParseUint(string(col), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse stats number %q: %w", col, err)
	}

	return uint32(value), nil
}
