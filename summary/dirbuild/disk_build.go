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
	"time"

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
	telemetry *buildTelemetry,
) error {
	if opts.DiskMetrics == nil {
		opts.DiskMetrics = new(DiskMetrics)
	}

	store, err := newDiskBackedSummaryStore(opts, refUnix)
	if err != nil {
		return err
	}

	metrics := opts.DiskMetrics

	telemetry.begin(PhasePass2Aggregation)

	buildErr := addStatsRowsDiskMeasured(open, index, store, refUnix, files, metrics, telemetry)
	if buildErr != nil {
		return errors.Join(buildErr, store.close(false))
	}

	telemetry.begin(PhaseSQLiteRollupSpoolEmission)

	return rollUpAndEmitDiskMeasured(index, database, store, metrics, telemetry)
}

func addStatsRowsDiskMeasured(
	open func() (io.ReadCloser, error),
	index *directoryIndex,
	store diskSummaryStore,
	refUnix int64,
	files FileSink,
	metrics *DiskMetrics,
	telemetry *buildTelemetry,
) error {
	started := time.Now()
	writesBefore := startProcessWriteBytesMeasurement(metrics)

	err := addStatsRowsDisk(open, index, store, refUnix, files, telemetry)
	if err == nil {
		err = store.flush()
	}

	metrics.Pass2Elapsed = time.Since(started)

	telemetry.finish(telemetry.inputRows())

	metrics.Pass2ProcessWriteBytes, metrics.Pass2ProcessWriteBytesAvailable =
		finishProcessWriteBytesMeasurement(metrics, writesBefore)

	return err
}

func addStatsRowsDisk(
	open func() (io.ReadCloser, error),
	index *directoryIndex,
	store diskSummaryStore,
	refUnix int64,
	files FileSink,
	telemetry *buildTelemetry,
) error {
	return withStatsReader(open, func(reader io.Reader) error {
		handle := func(row rawStatsRow) error {
			if err := addSyntheticDirRowsDisk(row, index, store, refUnix); err != nil {
				return err
			}

			return addStatsRowDisk(row, index, store, refUnix, files)
		}
		if telemetry == nil {
			return scanRawStatsObserved(reader, handle, nil)
		}

		return scanRawStatsObserved(reader, handle, func(rows uint64) {
			telemetry.setInputRows(rows)
			telemetry.progress(rows)
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
		node.ensureHardlinks()
		dirguta.TrackHardlink(node.seenHardlinks, info, ft, atime)

		return nil
	}

	keys := dirguta.GUTAKeyPool.Get().(*[dirguta.MaxNumOfGUTAKeys]dirguta.GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := dirguta.GUTAKeys(keys[:0])
	gutaKeys.Append(info.GID, info.UID, ft)
	err := store.addFile(node.dirID, gutaKeys, info.Size, atime, max(0, info.MTime))

	dirguta.GUTAKeyPool.Put(keys)

	return err
}

func rollUpAndEmitDiskMeasured(
	index *directoryIndex,
	database dirguta.DB,
	store diskSummaryStore,
	metrics *DiskMetrics,
	telemetry *buildTelemetry,
) error {
	started := time.Now()
	writesBefore := startProcessWriteBytesMeasurement(metrics)
	buildErr := rollUpAndEmitDisk(index, database, store, telemetry)
	closeErr := store.close(buildErr == nil)
	metrics.RollupElapsed = time.Since(started)

	telemetry.finish(uint64(len(index.nodes)))

	metrics.RollupProcessWriteBytes, metrics.RollupProcessWriteBytesAvailable =
		finishProcessWriteBytesMeasurement(metrics, writesBefore)

	metrics.ProcessWriteBytesAvailable = metrics.Pass2ProcessWriteBytesAvailable &&
		metrics.RollupProcessWriteBytesAvailable
	if metrics.ProcessWriteBytesAvailable {
		metrics.ProcessWriteBytes = metrics.Pass2ProcessWriteBytes + metrics.RollupProcessWriteBytes
	}

	return errors.Join(buildErr, closeErr)
}

func rollUpAndEmitDisk(
	index *directoryIndex,
	database dirguta.DB,
	store diskSummaryStore,
	telemetry *buildTelemetry,
) error {
	for i := len(index.nodes) - 1; i >= 0; i-- {
		telemetry.progress(uint64(len(index.nodes) - i)) //nolint:gosec // non-negative loop progress.

		if err := rollUpDiskNode(database, store, index.nodes[i]); err != nil {
			return err
		}
	}

	return nil
}

func rollUpDiskNode(database dirguta.DB, store diskSummaryStore, node *dirNode) error {
	hardlinkStore := dirguta.NewGUTAStore(node.refUnix)
	hardlinks := dirguta.MaterializeGUTAs(hardlinkStore, node.seenHardlinks)

	var parentID *uint32
	if node.parent != nil {
		parentID = &node.parent.dirID
	}

	err := store.rollUp(node.dirID, parentID, hardlinks, func(gutas db.GUTAs) error {
		return emitDiskRecord(database, node, gutas)
	})
	if err != nil {
		return err
	}

	if node.parent == nil {
		clear(node.seenHardlinks)
		node.seenHardlinks = nil

		return nil
	}

	mergeNodeHardlinks(node.parent, node)

	return nil
}

func emitDiskRecord(database dirguta.DB, node *dirNode, gutas db.GUTAs) error {
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
