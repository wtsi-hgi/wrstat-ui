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

const defaultDiskNodeThreshold = 1_000_000

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
	return BuildWithFilesOptions(open, mountPath, database, refTime, files, Options{})
}

// BuildWithFilesOptions is BuildWithFiles with explicit hybrid build options.
func BuildWithFilesOptions(
	open func() (io.ReadCloser, error),
	mountPath string,
	database dirguta.DB,
	refTime time.Time,
	files FileSink,
	opts Options,
) error {
	restoreGC := useDirbuildGCPercent()
	defer restoreGC()

	index, err := buildDirectoryIndex(open, mountPath, refTime.Unix())
	if err != nil {
		return err
	}

	if shouldUseDiskBackedSummaries(index, opts) {
		return buildWithDiskBackedSummaries(open, index, database, refTime.Unix(), files, opts)
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
	paths.node(cleanDirPath(mountPath))

	if err := collectDirectoryRows(open, paths); err != nil {
		return nil, err
	}

	nodes, err := assignDirectoryIDs(paths, mountPath, refUnix)
	if err != nil {
		return nil, err
	}

	index := indexNodes(nodes)

	paths.clear()

	return index, nil
}

func newPathBuilder() *pathBuilder {
	root := &summary.DirectoryPath{Name: "/", Depth: 0}

	return &pathBuilder{
		root:  &pathBuilderNode{dir: root},
		count: 1,
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

			paths.node(row.leafDirKey())

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

func assignDirectoryIDs(
	paths *pathBuilder,
	mountPath string,
	refUnix int64,
) ([]*dirNode, error) {
	alloc := newDirIDAllocator()
	if err := alloc.SetMountPath(mountPath); err != nil {
		return nil, err
	}

	state := newIDAssignmentState(alloc, refUnix, paths.count)
	if err := state.assignSubtree(paths.root, nil); err != nil {
		return nil, err
	}

	return state.nodes, nil
}

func newIDAssignmentState(
	alloc dirIDAllocator,
	refUnix int64,
	dirCount int,
) *idAssignmentState {
	return &idAssignmentState{
		alloc:   alloc,
		refUnix: refUnix,
		nodes:   make([]*dirNode, 0, dirCount),
	}
}

func indexNodes(nodes []*dirNode) *directoryIndex {
	index := &directoryIndex{
		nodes: nodes,
	}
	if len(nodes) > 0 {
		index.root = nodes[0]
	}

	return index
}

func shouldUseDiskBackedSummaries(index *directoryIndex, opts Options) bool {
	return len(index.nodes) >= diskNodeThreshold(opts)
}

func diskNodeThreshold(opts Options) int {
	if opts.DiskNodeThreshold > 0 {
		return opts.DiskNodeThreshold
	}

	return defaultDiskNodeThreshold
}

func rollUpAndEmit(index *directoryIndex, database dirguta.DB) error {
	for i := len(index.nodes) - 1; i >= 0; i-- {
		node := index.nodes[i]
		if err := emitNode(database, node); err != nil {
			return err
		}

		if node.parent == nil {
			node.clearStore()
			clear(node.seenHardlinks)
			node.seenHardlinks = nil

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
		GUTAs:          dirguta.MaterializeGUTAs(*node.ensureStore(), node.seenHardlinks),
		Children:       node.childNames(),
		ChildCount:     node.childCount,
		ChildFileCount: node.childFileCount,
	})
	if err != nil {
		return err
	}

	return nil
}

func mergeNodeHardlinks(parent *dirNode, node *dirNode) {
	if len(node.seenHardlinks) == 0 {
		return
	}

	parent.ensureHardlinks()
	dirguta.MergeHardlinks(parent.seenHardlinks, node.seenHardlinks)

	clear(node.seenHardlinks)
	node.seenHardlinks = nil
}

func drainNodeStore(parent *dirNode, node *dirNode) {
	if node.store == nil || node.store.Empty() {
		return
	}

	node.store.DrainInto(parent.ensureStore())
	node.store = nil
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

	if shouldTrackHardlink(info) {
		node.ensureHardlinks()
		dirguta.TrackHardlink(node.seenHardlinks, info, ft, atime)

		return
	}

	store := node.ensureStore()
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

// Options configures the hybrid dirbuild implementation.
type Options struct {
	// DiskNodeThreshold routes builds with at least this many directory nodes to
	// the disk-backed summary store before pass 2 starts. Values <= 0 use the
	// production default.
	DiskNodeThreshold int
	// TempDir optionally selects the parent directory for disk-backed summary
	// scratch data. Empty uses the runtime temporary directory.
	TempDir string
	// RetainTempDir leaves disk-backed summary scratch in place after closing so
	// callers can measure and clean it with surrounding build artefacts.
	RetainTempDir bool
	// DiskMetrics receives bounded-accumulator, SQLite, and phase measurements
	// when the disk-backed summary path is used.
	DiskMetrics *DiskMetrics
}

type dirNode struct {
	dir                    *summary.DirectoryPath
	parent                 *dirNode
	dirID                  uint32
	parentID               uint32
	subtreeEnd             uint32
	depth                  uint16
	store                  *dirguta.GUTAStore
	seenHardlinks          map[int64]*dirguta.InodeEntry
	firstChild             *dirNode
	childByName            map[string]*dirNode
	childCount             uint64
	childFileCount         uint64
	isTempDir              bool
	hasRawStatsRow         bool
	syntheticStatsRowAdded bool
	refUnix                int64
}

func enterNode(
	alloc dirIDAllocator,
	dir *summary.DirectoryPath,
	parent *dirNode,
	refUnix int64,
) (*dirNode, error) {
	parentID, err := parentIDForDir(dir, parent)
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
		dirID:     dirID,
		parentID:  parentID,
		depth:     uint16(dir.Depth), //nolint:gosec // stats paths are bounded by max path length.
		isTempDir: isTempDirectory(dir, parent),
		refUnix:   refUnix,
	}, nil
}

func (node *dirNode) ensureStore() *dirguta.GUTAStore {
	if node.store == nil {
		store := dirguta.NewGUTAStore(node.refUnix)
		node.store = &store
	}

	return node.store
}

func (node *dirNode) clearStore() {
	if node.store == nil {
		return
	}

	node.store.Clear()
	node.store = nil
}

func (node *dirNode) ensureHardlinks() {
	if node.seenHardlinks == nil {
		node.seenHardlinks = make(map[int64]*dirguta.InodeEntry)
	}
}

func (node *dirNode) addChild(child *dirNode) {
	if node.firstChild == nil {
		node.firstChild = child

		return
	}

	if node.childByName == nil {
		node.childByName = map[string]*dirNode{
			node.firstChild.dir.Name: node.firstChild,
		}
	}

	node.childByName[child.dir.Name] = child
}

func (node *dirNode) child(name string) *dirNode {
	if node == nil {
		return nil
	}

	if node.childByName != nil {
		return node.childByName[name]
	}

	if node.firstChild != nil && node.firstChild.dir.Name == name {
		return node.firstChild
	}

	return nil
}

func (node *dirNode) childNames() []string {
	if node.childCount == 0 {
		return nil
	}

	if node.childByName == nil {
		return []string{node.firstChild.dir.Name}
	}

	children := make([]string, 0, len(node.childByName))
	for name := range node.childByName {
		children = append(children, name)
	}

	slices.SortFunc(children, compareDirNames)

	return children
}

func parentIDForDir(dir *summary.DirectoryPath, parent *dirNode) (uint32, error) {
	if dir.Parent == nil {
		return summary.ReservedParentIDForDepth(dir.Depth)
	}

	if parent == nil {
		return 0, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, dir.Parent.AppendTo(nil))
	}

	return parent.dirID, nil
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

type pathBuilderNode struct {
	dir      *summary.DirectoryPath
	parent   *pathBuilderNode
	children map[string]*pathBuilderNode
	raw      bool
}

type pathBuilder struct {
	root  *pathBuilderNode
	count int
}

func (p *pathBuilder) node(path string) *pathBuilderNode {
	key := cleanDirPath(path)
	if key == "/" {
		return p.root
	}

	node := p.root

	start := 1
	for start < len(key) {
		next := strings.IndexByte(key[start:], '/')
		if next < 0 {
			break
		}

		name := key[start : start+next+1]
		node = p.child(node, name)
		start += next + 1
	}

	return node
}

func (p *pathBuilder) child(parent *pathBuilderNode, name string) *pathBuilderNode {
	child := parent.children[name]
	if child != nil {
		return child
	}

	if parent.children == nil {
		parent.children = make(map[string]*pathBuilderNode, 1)
	}

	name = strings.Clone(name)
	child = &pathBuilderNode{
		dir: &summary.DirectoryPath{
			Name:   name,
			Depth:  parent.dir.Depth + 1,
			Parent: parent.dir,
		},
		parent: parent,
	}
	parent.children[name] = child
	p.count++

	return child
}

func (p *pathBuilder) rawDir(path string) {
	p.node(path).raw = true
}

func (p *pathBuilder) clear() {
	p.root = nil
	p.count = 0
}

type idAssignmentState struct {
	alloc   dirIDAllocator
	refUnix int64
	nodes   []*dirNode
}

func (s *idAssignmentState) assignSubtree(builder *pathBuilderNode, parent *dirNode) error {
	node, err := s.enterBuilderNode(builder, parent)
	if err != nil {
		return err
	}

	if err := s.assignBuilderChildren(builder, node); err != nil {
		return err
	}

	clearPathBuilderNode(builder)

	return s.leaveAssignedNode(node)
}

func clearPathBuilderNode(builder *pathBuilderNode) {
	builder.children = nil
	builder.parent = nil
	builder.dir = nil
}

func (s *idAssignmentState) enterBuilderNode(builder *pathBuilderNode, parent *dirNode) (*dirNode, error) {
	node, err := enterNode(s.alloc, builder.dir, parent, s.refUnix)
	if err != nil {
		return nil, err
	}

	node.hasRawStatsRow = builder.raw
	if parent != nil {
		parent.addChild(node)
		parent.childCount++
	}

	s.nodes = append(s.nodes, node)

	return node, nil
}

func (s *idAssignmentState) assignBuilderChildren(builder *pathBuilderNode, node *dirNode) error {
	children := sortedPathBuilderChildren(builder)
	for idx, child := range children {
		childName := child.dir.Name
		if childErr := s.assignSubtree(child, node); childErr != nil {
			return childErr
		}

		delete(builder.children, childName)
		children[idx] = nil
	}

	return nil
}

func sortedPathBuilderChildren(builder *pathBuilderNode) []*pathBuilderNode {
	children := make([]*pathBuilderNode, 0, len(builder.children))
	for _, child := range builder.children {
		children = append(children, child)
	}

	slices.SortFunc(children, func(a, b *pathBuilderNode) int {
		return compareDirNames(a.dir.Name, b.dir.Name)
	})

	return children
}

func (s *idAssignmentState) leaveAssignedNode(node *dirNode) error {
	subtreeEnd, err := s.alloc.Leave(node.dir)
	if err != nil {
		return err
	}

	node.subtreeEnd = subtreeEnd

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
	nodes []*dirNode
	root  *dirNode
}

func (index *directoryIndex) resolve(row rawStatsRow) (*dirNode, error) {
	key := row.leafDirKey()

	node := index.resolveKey(key)
	if node == nil {
		return nil, fmt.Errorf("%w: %s", summary.ErrDirIDUnassigned, key)
	}

	return node, nil
}

func (index *directoryIndex) resolveKey(key string) *dirNode {
	key = cleanDirPath(key)
	if key == "/" {
		return index.root
	}

	node := index.root

	start := 1
	for start < len(key) {
		next := strings.IndexByte(key[start:], '/')
		if next < 0 {
			return nil
		}

		node = node.child(key[start : start+next+1])
		if node == nil {
			return nil
		}

		start += next + 1
	}

	return node
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

func compareDirNames(a string, b string) int {
	return strings.Compare(strings.TrimSuffix(a, "/"), strings.TrimSuffix(b, "/"))
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
