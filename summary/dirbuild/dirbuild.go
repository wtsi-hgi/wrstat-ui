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
	"runtime/debug"
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

const dirbuildGCPercent = 10

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
	restoreGC := useDirbuildGCPercent()
	defer restoreGC()

	index, err := buildDirectoryIndex(open, mountPath, refTime.Unix())
	if err != nil {
		return err
	}

	if err := addStatsRows(open, index, refTime.Unix(), files); err != nil {
		return err
	}

	return rollUpAndEmit(index, database)
}

func useDirbuildGCPercent() func() {
	previous := debug.SetGCPercent(dirbuildGCPercent)
	if previous >= 0 && previous < dirbuildGCPercent {
		debug.SetGCPercent(previous)

		return func() {}
	}

	return func() {
		debug.SetGCPercent(previous)
	}
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
		paths:   map[string]*summary.DirectoryPath{"/": root},
		rawDirs: make(map[string]struct{}),
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
			if row.isDir() {
				paths.rawDir(row.dirKey())

				return nil
			}

			paths.dir(row.leafDirKey())

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

	populateChildren(state.nodes)

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

func populateChildren(nodes []*dirNode) {
	for _, node := range nodes {
		if node.parent == nil {
			continue
		}

		node.parent.children = append(node.parent.children, node.dir.Name)
		node.parent.childCount++
	}
}

func indexNodes(nodes []*dirNode) *directoryIndex {
	index := &directoryIndex{
		nodes:      nodes,
		pathToNode: make(map[string]*dirNode, len(nodes)),
	}

	for _, node := range nodes {
		index.pathToNode[node.key] = node
	}

	return index
}

func rollUpAndEmit(index *directoryIndex, database dirguta.DB) error {
	index.pathToNode = nil

	for i := len(index.nodes) - 1; i >= 0; i-- {
		node := index.nodes[i]
		if err := emitNode(database, node); err != nil {
			return err
		}

		if node.parent == nil {
			node.store.Clear()

			continue
		}

		mergeNodeHardlinks(node.parent, node)
		drainNodeStore(node.parent, node)
	}

	return nil
}

func emitNode(database dirguta.DB, node *dirNode) error {
	err := database.Add(db.RecordDGUTA{
		Dir:            node.dir,
		DirID:          node.dirID,
		ParentID:       node.parentID,
		SubtreeEnd:     node.subtreeEnd,
		Depth:          node.depth,
		GUTAs:          materializeGUTAs(node.store),
		Children:       node.children,
		ChildCount:     node.childCount,
		ChildFileCount: node.childFileCount,
	})
	if err != nil {
		return err
	}

	node.children = nil

	return nil
}

func materializeGUTAs(store dirguta.GUTAStore) db.GUTAs {
	keys := store.Sort()
	if len(keys) == 0 {
		return nil
	}

	values := make([]db.GUTA, len(keys))
	gutas := make(db.GUTAs, len(keys))

	for idx, key := range keys {
		materializeGUTA(&values[idx], store, key)
		gutas[idx] = &values[idx]
	}

	return gutas
}

func materializeGUTA(out *db.GUTA, store dirguta.GUTAStore, key dirguta.GUTAKey) {
	summary := store.Summary(key)
	if summary == nil {
		return
	}

	*out = db.GUTA{
		GID:         key.GID,
		UID:         key.UID,
		FT:          key.FileType,
		Age:         key.Age,
		Count:       uint64(summary.Count), //nolint:gosec
		Size:        uint64(summary.Size),  //nolint:gosec
		Atime:       summary.Atime,
		ATimeRanges: summary.AtimeBuckets,
		Mtime:       summary.Mtime,
		MTimeRanges: summary.MtimeBuckets,
	}
}

func mergeNodeHardlinks(parent *dirNode, node *dirNode) {
	if len(node.seenHardlinks) == 0 {
		return
	}

	parent.ensureStore()
	parent.ensureHardlinks()
	dirguta.MergeSeenHardlinks(&parent.store, parent.seenHardlinks, &node.store, node.seenHardlinks)
	clear(node.seenHardlinks)
}

func drainNodeStore(parent *dirNode, node *dirNode) {
	if node.store.Empty() {
		return
	}

	parent.ensureStore()
	node.store.DrainInto(&parent.store)
}

func addStatsRows(open func() (io.ReadCloser, error), index *directoryIndex, refUnix int64, files FileSink) error {
	return withStatsReader(open, func(reader io.Reader) error {
		return scanRawStats(reader, func(row rawStatsRow) error {
			if err := addSyntheticDirRows(row, index, refUnix); err != nil {
				return err
			}

			return addStatsRow(row, index, refUnix, files)
		})
	})
}

func addSyntheticDirRows(row rawStatsRow, index *directoryIndex, refUnix int64) error {
	nodes, err := index.syntheticDirNodes(row)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		info := row.syntheticDirInfo(node.dir)
		addToNode(node, &info, refUnix)
		node.syntheticStatsRowAdded = true
	}

	return nil
}

func shouldTrackHardlink(info *summary.FileInfo) bool {
	return !info.IsDir() && info.Nlink > 1 && info.Inode != 0
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

	store := node.ensureStore()
	if shouldTrackHardlink(info) {
		node.ensureHardlinks()
	}

	if dirguta.HandleHardlink(store, node.seenHardlinks, info, ft, atime) {
		return
	}

	gutaKeysA := dirguta.GUTAKeyPool.Get().(*[dirguta.MaxNumOfGUTAKeys]dirguta.GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := dirguta.GUTAKeys(gutaKeysA[:0])
	gutaKeys.Append(info.GID, info.UID, ft)
	store.AddForEach(gutaKeys, info.Size, atime, max(0, info.MTime))
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
	dir                    *summary.DirectoryPath
	parent                 *dirNode
	key                    string
	dirID                  uint32
	parentID               uint32
	subtreeEnd             uint32
	depth                  uint16
	store                  dirguta.GUTAStore
	seenHardlinks          map[int64]*dirguta.InodeEntry
	children               []string
	childCount             uint64
	childFileCount         uint64
	isTempDir              bool
	hasRawStatsRow         bool
	syntheticStatsRowAdded bool
}

func enterNode(
	alloc dirIDAllocator,
	dir *summary.DirectoryPath,
	key string,
	nodesByPath map[string]*dirNode,
	refUnix int64,
) (*dirNode, error) {
	parent, parentID, err := parentAndID(dir, nodesByPath)
	if err != nil {
		return nil, err
	}

	dirID, err := alloc.Enter(dir)
	if err != nil {
		return nil, err
	}

	return &dirNode{
		dir:       dir,
		parent:    parent,
		key:       key,
		dirID:     dirID,
		parentID:  parentID,
		depth:     uint16(dir.Depth), //nolint:gosec // stats paths are bounded by max path length.
		store:     dirguta.GUTAStore{RefTime: refUnix},
		isTempDir: isTempDirectory(dir, parent),
	}, nil
}

func parentAndID(dir *summary.DirectoryPath, nodesByPath map[string]*dirNode) (*dirNode, uint32, error) {
	if dir.Parent == nil {
		parentID, err := summary.ReservedParentIDForDepth(dir.Depth)

		return nil, parentID, err
	}

	parent, ok := nodesByPath[string(dir.Parent.AppendTo(nil))]
	if !ok {
		return nil, 0, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, dir.Parent.AppendTo(nil))
	}

	return parent, parent.dirID, nil
}

func (node *dirNode) ensureStore() *dirguta.GUTAStore {
	return &node.store
}

func (node *dirNode) ensureHardlinks() {
	if node.seenHardlinks == nil {
		node.seenHardlinks = make(map[int64]*dirguta.InodeEntry)
	}
}

func isTempDirectory(dir *summary.DirectoryPath, parent *dirNode) bool {
	if dir == nil {
		return false
	}

	if dirguta.IsTemp([]byte(dir.Name)) {
		return true
	}

	return parent != nil && parent.isTempDir
}

type pathEntry struct {
	key string
	dir *summary.DirectoryPath
}

type pathBuilder struct {
	paths   map[string]*summary.DirectoryPath
	rawDirs map[string]struct{}
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

func (p *pathBuilder) rawDir(path string) {
	key := cleanDirPath(path)
	p.dir(key)
	p.rawDirs[key] = struct{}{}
}

func (p *pathBuilder) hasRawDir(path string) bool {
	_, ok := p.rawDirs[path]

	return ok
}

func (p *pathBuilder) sorted() []pathEntry {
	dirs := make([]pathEntry, 0, len(p.paths))
	for key, dir := range p.paths {
		dirs = append(dirs, pathEntry{key: key, dir: dir})
	}

	slices.SortFunc(dirs, func(a, b pathEntry) int {
		if a.dir.Less(b.dir) {
			return -1
		}

		if b.dir.Less(a.dir) {
			return 1
		}

		return 0
	})

	return dirs
}

type idAssignmentState struct {
	alloc       dirIDAllocator
	paths       *pathBuilder
	refUnix     int64
	nodes       []*dirNode
	stack       []*dirNode
	nodesByPath map[string]*dirNode
}

func (s *idAssignmentState) enterNext(entry pathEntry) error {
	if err := s.closeCompletedAncestors(entry.dir); err != nil {
		return err
	}

	node, err := enterNode(s.alloc, entry.dir, entry.key, s.nodesByPath, s.refUnix)
	if err != nil {
		return err
	}

	node.hasRawStatsRow = s.paths.hasRawDir(entry.key)

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

func (r rawStatsRow) syntheticDirInfo(dir *summary.DirectoryPath) summary.FileInfo {
	return summary.FileInfo{
		Path:         dir,
		Name:         []byte(dir.Name),
		Size:         0,
		ApparentSize: 0,
		UID:          r.uid,
		GID:          r.gid,
		MTime:        r.mtime,
		ATime:        r.atime,
		CTime:        r.ctime,
		Inode:        0,
		Nlink:        r.nlink,
		EntryType:    stats.DirType,
	}
}

type directoryIndex struct {
	nodes      []*dirNode
	pathToNode map[string]*dirNode
}

func (index *directoryIndex) resolve(row rawStatsRow) (*dirNode, error) {
	node, ok := index.pathToNode[row.leafDirKey()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, row.leafDirKey())
	}

	return node, nil
}

func (index *directoryIndex) syntheticDirNodes(row rawStatsRow) ([]*dirNode, error) {
	node, err := index.resolve(row)
	if err != nil {
		return nil, err
	}

	if row.isDir() {
		node = node.parent
	}

	nodes := make([]*dirNode, 0, nodeDepth(node))
	for node != nil {
		if node.dir.Parent == nil {
			break
		}

		if !node.hasRawStatsRow && !node.syntheticStatsRowAdded {
			nodes = append(nodes, node)
		}

		node = node.parent
	}

	slices.Reverse(nodes)

	return nodes, nil
}

func nodeDepth(node *dirNode) int {
	if node == nil {
		return 0
	}

	return int(node.depth)
}

// Build runs the two-pass directory-centric build over the stats stream. open
// is called once per pass to obtain a fresh reader of the same stats stream.
// Output order is not part of the contract.
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
