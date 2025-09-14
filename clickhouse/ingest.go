/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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
	"io"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

// UpdateClickhouse ingests a scan into ClickHouse.
func (c *Clickhouse) UpdateClickhouse(ctx context.Context, mountPath string, r io.Reader) (retErr error) {
	// Use current time as scan ID
	scanID := uint64(time.Now().Unix()) //nolint:gosec // monotonic timestamp scan identifier
	started := time.Now()

	// Register scan as loading
	if err := c.registerScan(ctx, mountPath, scanID, started); err != nil {
		return err
	}

	// Set up rollback handler for cleanup on error
	defer c.setupRollbackHandler(ctx, mountPath, scanID)(retErr)

	if err := c.ingestScan(ctx, mountPath, scanID, r); err != nil {
		return err
	}

	// Promote to ready by inserting a new row (avoids ALTER UPDATE pitfalls)
	finished := time.Now()
	if err := c.promoteScan(ctx, mountPath, scanID, started, finished); err != nil {
		return fmt.Errorf("promote scan (insert ready): %w", err)
	}

	// Drop older scans for this mount
	if err := c.dropOlderScans(ctx, mountPath, scanID); err != nil {
		return fmt.Errorf("retention: %w", err)
	}

	return nil
}

// BatchProcessor handles batched processing of file entries and rollups.
type BatchProcessor struct {
	filesBatch      CHBatch
	rollupsBatch    CHBatch
	conn            chdriver.Conn
	filesCount      int
	rollupsCount    int
	mountPath       string
	scanID          uint64
	filesBatchSQL   string
	rollupsBatchSQL string
}

// NewBatchProcessor creates a new batch processor for files and rollups.
func (c *Clickhouse) newBatchProcessor(ctx context.Context, mountPath string, scanID uint64) (*BatchProcessor, error) {
	filesBatchSQL := `
		INSERT INTO fs_entries 
		(mount_path, scan_id, path, parent_path, name, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime)`

	rollupsBatchSQL := `
		INSERT INTO ancestor_rollups_raw 
		(mount_path, scan_id, ancestor, size, atime, mtime, uid, gid, ext_low)`

	filesBatch, err := c.conn.PrepareBatch(ctx, filesBatchSQL)
	if err != nil {
		return nil, err
	}

	rollupsBatch, err := c.conn.PrepareBatch(ctx, rollupsBatchSQL)
	if err != nil {
		return nil, err
	}

	return &BatchProcessor{
		filesBatch:      filesBatch,
		rollupsBatch:    rollupsBatch,
		conn:            c.conn,
		mountPath:       mountPath,
		scanID:          scanID,
		filesBatchSQL:   filesBatchSQL,
		rollupsBatchSQL: rollupsBatchSQL,
	}, nil
}

// AddFile adds a file entry to the batch.
func (bp *BatchProcessor) AddFile(path string, parent string, name string, ext string,
	ft FileType, inode uint64, size uint64, uid uint32, gid uint32, mtime, atime, ctime time.Time) error {
	if err := bp.filesBatch.Append(
		bp.mountPath, bp.scanID, path, parent, name, ext, uint8(ft),
		inode, size, uid, gid, mtime, atime, ctime,
	); err != nil {
		return err
	}

	bp.filesCount++

	return nil
}

// AddRollup adds an ancestor rollup entry to the batch.
func (bp *BatchProcessor) AddRollup(ancestor string, size uint64,
	atime, mtime time.Time, uid, gid uint32, ext string) error {
	if err := bp.rollupsBatch.Append(
		bp.mountPath, bp.scanID, ancestor, size, atime, mtime, uid, gid, ext); err != nil {
		return err
	}

	bp.rollupsCount++

	return nil
}

// NeedsFlush checks if either batch needs flushing.
func (bp *BatchProcessor) NeedsFlush() bool {
	return bp.filesCount >= FileBatchSize || bp.rollupsCount >= RollupBatchSize
}

// Flush sends both batches if they contain any data.
func (bp *BatchProcessor) Flush(ctx context.Context) error {
	if err := bp.flushFilesBatch(ctx); err != nil {
		return err
	}

	if err := bp.flushRollupsBatch(ctx); err != nil {
		return err
	}

	return nil
}

// flushFilesBatch sends the files batch if it's non-empty.
func (bp *BatchProcessor) flushFilesBatch(ctx context.Context) error {
	if bp.filesCount == 0 {
		return nil
	}

	if err := bp.filesBatch.Send(); err != nil {
		return err
	}

	bp.filesCount = 0

	filesBatch, err := bp.conn.PrepareBatch(ctx, bp.filesBatchSQL)
	if err != nil {
		return err
	}

	bp.filesBatch = filesBatch

	return nil
}

// flushRollupsBatch sends the rollups batch if it's non-empty.
func (bp *BatchProcessor) flushRollupsBatch(ctx context.Context) error {
	if bp.rollupsCount == 0 {
		return nil
	}

	if err := bp.rollupsBatch.Send(); err != nil {
		return err
	}

	bp.rollupsCount = 0

	rollupsBatch, err := bp.conn.PrepareBatch(ctx, bp.rollupsBatchSQL)
	if err != nil {
		return err
	}

	bp.rollupsBatch = rollupsBatch

	return nil
}

// ingestScan processes a stats file and loads it into ClickHouse.
func (c *Clickhouse) ingestScan(ctx context.Context, mountPath string, scanID uint64, r io.Reader) error {
	// Create batch processor
	bp, err := c.newBatchProcessor(ctx, mountPath, scanID)
	if err != nil {
		return err
	}

	if err := scanAndProcessEntries(ctx, bp, r, mountPath); err != nil {
		return err
	}

	// Insert synthetic ancestor directories above the mount to enable global listings
	if err := insertSyntheticAncestorDirs(bp, mountPath); err != nil {
		return err
	}

	// Final flush
	return bp.Flush(ctx)
}

// scanAndProcessEntries scans through the file records and processes each entry.
func scanAndProcessEntries(ctx context.Context, bp *BatchProcessor, r io.Reader, mountPath string) error {
	parser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)

	var parseErr error

	for {
		// Read the next entry
		if parseErr = parser.Scan(fi); parseErr != nil {
			break
		}

		shouldContinue, err := processScanEntry(ctx, bp, fi, mountPath)
		if !shouldContinue || err != nil {
			return err
		}
	}

	// Check for parser errors (excluding EOF which is expected)
	if !errors.Is(parseErr, io.EOF) {
		return fmt.Errorf("parser error: %w", parseErr)
	}

	return nil
}

// processScanEntry processes a single entry during scan ingestion.
// Returns a boolean indicating if we should continue scanning, and any error encountered.
func processScanEntry(ctx context.Context, bp *BatchProcessor, fi *stats.FileInfo, mountPath string) (bool, error) {
	// Process the file entry
	if err := processFileEntry(bp, fi, mountPath); err != nil {
		return false, err
	}

	// Flush batches if needed
	if bp.NeedsFlush() {
		if err := bp.Flush(ctx); err != nil {
			return false, fmt.Errorf("failed to flush batches: %w", err)
		}
	}

	return true, nil
}

// processFileEntry handles a single file entry during scan ingestion.
func processFileEntry(bp *BatchProcessor, fi *stats.FileInfo, mountPath string) error {
	path := string(fi.Path)

	// Ensure all paths are absolute (start with '/') to align with server queries/tests
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	isDir := fi.EntryType == stats.DirType || IsDirPath(path)

	parent, name := SplitParentAndName(path)
	ext := DeriveExtLower(name, isDir)
	ft := MapEntryType(fi.EntryType)
	mtime := time.Unix(fi.MTime, 0)
	atime := time.Unix(fi.ATime, 0)
	ctime := time.Unix(fi.CTime, 0)

	// Handle potential integer overflow by using explicit conversions
	inode := uint64(0)
	if fi.Inode > 0 {
		inode = uint64(fi.Inode) // Values originate from trusted stats parser
	}

	size := uint64(0)
	if fi.Size > 0 {
		size = uint64(fi.Size) // Values originate from trusted stats parser
	}

	// Add file entry to batch
	if err := bp.AddFile(path, parent, name, ext, ft, inode, size,
		fi.UID, fi.GID, mtime, atime, ctime); err != nil {
		return fmt.Errorf("failed to add file entry: %w", err)
	}

	return processAncestorRollups(bp, fi, path, parent, isDir, size, atime, mtime, ext, mountPath)
}

// processAncestorRollups processes rollups for all ancestor directories.
// It calculates rollups for each directory in the path hierarchy.
func processAncestorRollups(bp *BatchProcessor, fi *stats.FileInfo, path, parent string,
	isDir bool, size uint64, atime, mtime time.Time, ext, mountPath string) error {
	// Include the directory itself in its own subtree if the entry is a directory
	base := parent
	if isDir {
		base = path
	}

	if err := addRollupsWithinMount(bp, fi, base, mountPath, size, atime, mtime, ext); err != nil {
		return err
	}

	if err := addRollupsAboveMount(bp, fi, mountPath, size, atime, mtime, ext); err != nil {
		return err
	}

	return nil
}

// addRollupsWithinMount adds rollups for the path and its ancestors inside the mount.
func addRollupsWithinMount(bp *BatchProcessor, fi *stats.FileInfo, base, mountPath string,
	size uint64, atime, mtime time.Time, ext string) error {
	var firstErr error

	ForEachAncestor(base, mountPath, func(a string) bool {
		if err := bp.AddRollup(a, size, atime, mtime, fi.UID, fi.GID, ext); err != nil {
			firstErr = err

			return false
		}

		return true
	})

	if firstErr != nil {
		return fmt.Errorf("failed to add ancestor rollup: %w", firstErr)
	}

	return nil
}

// addRollupsAboveMount adds rollups for ancestors above the mount up to root.
func addRollupsAboveMount(bp *BatchProcessor, fi *stats.FileInfo, mountPath string,
	size uint64, atime, mtime time.Time, ext string) error {
	mp := EnsureDir(mountPath)
	parentOfMount, _ := SplitParentAndName(mp)

	var firstErr error

	ForEachAncestor(parentOfMount, "/", func(a string) bool {
		if err := bp.AddRollup(a, size, atime, mtime, fi.UID, fi.GID, ext); err != nil {
			firstErr = err

			return false
		}

		return true
	})

	if firstErr != nil {
		return fmt.Errorf("failed to add ancestor rollup above mount: %w", firstErr)
	}

	return nil
}

// insertSyntheticAncestorDirs inserts one directory entry per ancestor above the mountPath
// (eg. for "/mnt/b/c/" inserts "/mnt/b/" and "/mnt/") so that global listings at
// high-level directories (including "/") can enumerate down to the real mountpoints.
func insertSyntheticAncestorDirs(bp *BatchProcessor, mountPath string) error {
	mp := EnsureDir(mountPath)
	parent, _ := SplitParentAndName(mp)

	// Nothing to do if already at root
	if parent == "/" && mp == "/" {
		return nil
	}

	// Use the scan time as a consistent timestamp for synthetic dirs.
	// scanID originated from time.Now().Unix() and is safe to cast back.
	t := time.Unix(int64(bp.scanID), 0) //nolint:gosec // scanID comes from Unix seconds

	// Walk ancestors up to root, excluding root itself as an entry
	var firstErr error

	ForEachAncestor(parent, "/", func(a string) bool {
		if a == "/" { // do not insert a synthetic root entry
			return true
		}

		if err := addSyntheticDirAndRollups(bp, a, t); err != nil {
			firstErr = err

			return false
		}

		return true
	})

	return firstErr
}

// addSyntheticDirAndRollups inserts a synthetic directory entry for 'a' and adds
// rollup rows for 'a' and all its ancestors up to and including root.
func addSyntheticDirAndRollups(bp *BatchProcessor, a string, t time.Time) error {
	p, name := SplitParentAndName(a)

	// Insert a single directory entry with zero size/ids; duplicates across mounts will be
	// deduplicated at query time for global listings.
	if err := bp.AddFile(a, p, name, "", FileTypeDir, 0, 0, 0, 0, t, t, t); err != nil {
		return err
	}

	var firstErr error

	ForEachAncestor(a, "/", func(ra string) bool {
		// Zero-size, zero-uid/gid, empty ext to avoid affecting size while counting entries.
		if err := bp.AddRollup(ra, 0, t, t, 0, 0, ""); err != nil {
			firstErr = err

			return false
		}

		return true
	})

	if firstErr != nil {
		return firstErr
	}

	return nil
}
