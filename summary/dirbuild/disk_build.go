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
	"errors"
	"io"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

func buildWithDiskBackedSummaries(
	open func() (io.ReadCloser, error),
	index *directoryIndex,
	database dirguta.DB,
	refUnix int64,
	files FileSink,
	opts Options,
) error {
	store, err := newDiskBackedSummaryStore(opts, refUnix)
	if err != nil {
		return err
	}

	buildErr := addStatsRowsDisk(open, index, store, refUnix, files)
	if buildErr == nil {
		buildErr = rollUpAndEmitDisk(index, database, store)
	}

	return errors.Join(buildErr, store.close())
}

func addStatsRowsDisk(
	open func() (io.ReadCloser, error),
	index *directoryIndex,
	store diskSummaryStore,
	refUnix int64,
	files FileSink,
) error {
	return withStatsReader(open, func(reader io.Reader) error {
		return scanRawStats(reader, func(row rawStatsRow) error {
			if err := addSyntheticDirRowsDisk(row, index, store, refUnix); err != nil {
				return err
			}

			return addStatsRowDisk(row, index, store, refUnix, files)
		})
	})
}

func addSyntheticDirRowsDisk(
	row rawStatsRow,
	index *directoryIndex,
	store diskSummaryStore,
	refUnix int64,
) error {
	nodes, err := index.syntheticDirNodes(row)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		info := row.syntheticDirInfo(node.dir)
		if err := addToDiskNode(store, node, &info, refUnix); err != nil {
			return err
		}

		node.syntheticStatsRowAdded = true
	}

	return nil
}

func addStatsRowDisk(
	row rawStatsRow,
	index *directoryIndex,
	store diskSummaryStore,
	refUnix int64,
	files FileSink,
) error {
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

	return addToDiskNode(store, node, &info, refUnix)
}

func addToDiskNode(
	store diskSummaryStore,
	node *dirNode,
	info *summary.FileInfo,
	refUnix int64,
) error {
	ft := dirguta.FileTypeWithTemp(info.Name, node.isTempDir)

	atime := info.ATime
	if info.IsDir() {
		atime = refUnix
	}

	if shouldTrackHardlink(info) {
		return handleDiskHardlink(store, node, info, ft, atime)
	}

	keys := dirguta.GUTAKeyPool.Get().(*[dirguta.MaxNumOfGUTAKeys]dirguta.GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := dirguta.GUTAKeys(keys[:0])
	gutaKeys.Append(info.GID, info.UID, ft)
	err := store.addFile(node.dirID, gutaKeys, info.Size, atime, max(0, info.MTime))

	dirguta.GUTAKeyPool.Put(keys)

	return err
}

func handleDiskHardlink(
	store diskSummaryStore,
	node *dirNode,
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) error {
	node.ensureDiskHardlinks()

	entry, exists := node.diskSeenHardlinks[info.Inode]
	if !exists {
		node.diskSeenHardlinks[info.Inode] = newDiskHardlinkEntry(info, ft, atime)

		return store.addHardlinkEntry(node.dirID, node.diskSeenHardlinks[info.Inode])
	}

	if err := store.subtractHardlinkEntry(node.dirID, entry); err != nil {
		return err
	}

	entry.merge(info, ft, atime)

	return store.addHardlinkEntry(node.dirID, entry)
}

func newDiskHardlinkEntry(
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) *diskHardlinkEntry {
	return &diskHardlinkEntry{
		fileType: ft,
		size:     info.Size,
		atime:    atime,
		mtime:    info.MTime,
		gid:      info.GID,
		uid:      info.UID,
	}
}

func rollUpAndEmitDisk(index *directoryIndex, database dirguta.DB, store diskSummaryStore) error {
	for i := len(index.nodes) - 1; i >= 0; i-- {
		if err := rollUpDiskNode(database, store, index.nodes[i]); err != nil {
			return err
		}
	}

	return nil
}

func rollUpDiskNode(database dirguta.DB, store diskSummaryStore, node *dirNode) error {
	if err := emitDiskNode(database, store, node); err != nil {
		return err
	}

	if node.parent == nil {
		return store.clear(node.dirID)
	}

	if err := mergeDiskNodeHardlinks(store, node.parent, node); err != nil {
		return err
	}

	return store.merge(node.parent.dirID, node.dirID)
}

func emitDiskNode(database dirguta.DB, store diskSummaryStore, node *dirNode) error {
	gutas, err := store.materialise(node.dirID)
	if err != nil {
		return err
	}

	return database.Add(db.RecordDGUTA{
		Dir:            node.dir,
		DirID:          node.dirID,
		ParentID:       node.parentID,
		SubtreeEnd:     node.subtreeEnd,
		Depth:          node.depth,
		GUTAs:          gutas,
		Children:       node.childNames(),
		ChildCount:     node.childCount,
		ChildFileCount: node.childFileCount,
	})
}

func mergeDiskNodeHardlinks(store diskSummaryStore, parent *dirNode, node *dirNode) error {
	if len(node.diskSeenHardlinks) == 0 {
		return nil
	}

	parent.ensureDiskHardlinks()

	for inode, childEntry := range node.diskSeenHardlinks {
		parentEntry, exists := parent.diskSeenHardlinks[inode]
		if !exists {
			parent.diskSeenHardlinks[inode] = childEntry

			continue
		}

		if err := updateExistingDiskHardlink(store, parent, node, parentEntry, childEntry); err != nil {
			return err
		}
	}

	clear(node.diskSeenHardlinks)
	node.diskSeenHardlinks = nil

	return nil
}

func updateExistingDiskHardlink(
	store diskSummaryStore,
	parent *dirNode,
	node *dirNode,
	parentEntry *diskHardlinkEntry,
	childEntry *diskHardlinkEntry,
) error {
	if err := store.subtractHardlinkEntry(parent.dirID, parentEntry); err != nil {
		return err
	}

	if err := store.subtractHardlinkEntry(node.dirID, childEntry); err != nil {
		return err
	}

	parentEntry.mergeEntry(childEntry)

	return store.addHardlinkEntry(node.dirID, parentEntry)
}

func (entry *diskHardlinkEntry) merge(
	info *summary.FileInfo,
	ft db.DirGUTAFileType,
	atime int64,
) {
	entry.fileType |= ft
	entry.size = max(entry.size, info.Size)
	entry.atime = min(entry.atime, atime)
	entry.mtime = max(entry.mtime, info.MTime)
}

func (entry *diskHardlinkEntry) mergeEntry(child *diskHardlinkEntry) {
	entry.fileType |= child.fileType
	entry.size = max(entry.size, child.size)
	entry.atime = min(entry.atime, child.atime)
	entry.mtime = max(entry.mtime, child.mtime)
}

func (node *dirNode) ensureDiskHardlinks() {
	if node.diskSeenHardlinks == nil {
		node.diskSeenHardlinks = make(map[int64]*diskHardlinkEntry)
	}
}
