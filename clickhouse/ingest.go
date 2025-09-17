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
	"sort"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

// UpdateClickhouse ingests a scan into ClickHouse.
func (c *Clickhouse) UpdateClickhouse(ctx context.Context, mountPath string, r io.Reader) (retErr error) {
	// Use UUID as scan_id to guarantee uniqueness
	scanUUID := uuid.New()
	scanTime := time.Now()
	started := scanTime

	// Register scan as loading
	if err := c.registerScan(ctx, mountPath, scanUUID, scanTime, started); err != nil {
		return err
	}

	// Set up rollback handler for cleanup on error
	defer c.setupRollbackHandler(ctx, mountPath, scanTime)(retErr)

	if err := c.ingestScan(ctx, mountPath, scanUUID, scanTime, r); err != nil {
		return err
	}

	// Promote to ready state
	finished := time.Now()
	if err := c.promoteScan(ctx, mountPath, scanUUID, scanTime, started, finished); err != nil {
		return fmt.Errorf("promote scan: %w", err)
	}

	// Drop older scans for this mount
	if err := c.dropOlderScans(ctx, mountPath, finished); err != nil {
		return fmt.Errorf("retention: %w", err)
	}

	return nil
}

// BatchProcessor handles batched processing of file entries and in-memory aggregation for rollups.
type BatchProcessor struct {
	filesBatch    CHBatch
	conn          chdriver.Conn
	filesCount    int
	mountPath     string
	scanID        uuid.UUID
	scanTime      time.Time
	filesBatchSQL string
	dirAgg        map[string]*dirSummaryAgg
}

// dirSummaryAgg aggregates directory level stats (single age bucket for now).
type dirSummaryAgg struct {
	totalSize uint64
	fileCount uint64
	oldestAT  time.Time
	newestAT  time.Time
	oldestMT  time.Time
	newestMT  time.Time
	uids      map[uint32]struct{}
	gids      map[uint32]struct{}
	exts      map[string]struct{}
	ftypes    map[uint8]struct{}
}

func newDirSummaryAgg() *dirSummaryAgg { //nolint:ireturn
	return &dirSummaryAgg{
		uids:   make(map[uint32]struct{}),
		gids:   make(map[uint32]struct{}),
		exts:   make(map[string]struct{}),
		ftypes: make(map[uint8]struct{}),
	}
}

// NewBatchProcessor creates a new batch processor for files and rollups.
func (c *Clickhouse) newBatchProcessor(ctx context.Context, mountPath string, scanID uuid.UUID, scanTime time.Time) (*BatchProcessor, error) { //nolint:lll
	filesBatchSQL := filesInsertSQL
	filesBatch, err := prepareBatch(ctx, c.conn, filesBatchSQL)
	if err != nil {
		return nil, err
	}

	return &BatchProcessor{
		filesBatch:    filesBatch,
		conn:          c.conn,
		mountPath:     mountPath,
		scanID:        scanID,
		scanTime:      scanTime,
		filesBatchSQL: filesBatchSQL,
		dirAgg:        make(map[string]*dirSummaryAgg),
	}, nil
}

// filesInsertSQL is the SQL string used to insert file entries.
const filesInsertSQL = `
	INSERT INTO fs_entries
	(mount_path, scan_id, scan_time, path, parent_path, basename,
	 depth, ext_low, ftype, inode, size, uid, gid, mtime, atime, ctime)`

// rollupsInsertSQL inserts aggregated directory summaries.
const rollupsInsertSQL = `
    INSERT INTO ancestor_rollups
    (mount_path, scan_id, scan_time, ancestor, total_size, file_count, oldest_atime, newest_atime, oldest_mtime, newest_mtime, uids, gids, exts, ftypes, age)`

// prepareBatch prepares a ClickHouse batch from the given SQL string.
// It's acceptable to return the CHBatch interface here as the concrete batch
// type from the driver is not exposed in a stable way.
//
// nolint
func prepareBatch(ctx context.Context, conn chdriver.Conn, sql string) (CHBatch, error) {
	return conn.PrepareBatch(ctx, sql)
}

// AddFile adds a file entry to the batch.
func (bp *BatchProcessor) AddFile(path string, parent string, name string, depth uint16, ext string,
	ft FileType, inode uint64, size uint64, uid uint32, gid uint32, mtime, atime, ctime time.Time) error {
	if err := bp.filesBatch.Append(
		bp.mountPath, bp.scanID, bp.scanTime, path, parent, name, depth, ext, uint8(ft),
		inode, size, uid, gid, mtime, atime, ctime,
	); err != nil {
		return err
	}

	bp.filesCount++

	return nil
}

// aggregateRollup updates in-memory aggregation for a directory.
func (bp *BatchProcessor) aggregateRollup(ancestor string, size uint64, atime, mtime time.Time, uid, gid uint32, ext string, ftype uint8) { //nolint:lll
	// Only aggregate from files; ignore directories and other special entries.
	if ftype != uint8(FileTypeFile) {
		return
	}

	agg := bp.dirAgg[ancestor]
	if agg == nil {
		agg = newDirSummaryAgg()
		bp.dirAgg[ancestor] = agg
	}

	agg.totalSize += size
	agg.fileCount++

	if agg.oldestAT.IsZero() || atime.Before(agg.oldestAT) {
		agg.oldestAT = atime
	}
	if atime.After(agg.newestAT) {
		agg.newestAT = atime
	}
	if agg.oldestMT.IsZero() || mtime.Before(agg.oldestMT) {
		agg.oldestMT = mtime
	}
	if mtime.After(agg.newestMT) {
		agg.newestMT = mtime
	}

	if uid != 0 {
		agg.uids[uid] = struct{}{}
	}
	if gid != 0 {
		agg.gids[gid] = struct{}{}
	}
	if ext != "" {
		agg.exts[ext] = struct{}{}
	}
	// Record the file type for files (eg. regular files only here)
	agg.ftypes[ftype] = struct{}{}
}

// NeedsFlush checks if either batch needs flushing.
func (bp *BatchProcessor) NeedsFlush() bool { return bp.filesCount >= FileBatchSize }

// Flush sends both batches if they contain any data.
func (bp *BatchProcessor) Flush(ctx context.Context) error { return bp.flushFilesBatch(ctx) }

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

// afterScanInsertRollups sends aggregated directory rows to ClickHouse.
func (bp *BatchProcessor) afterScanInsertRollups(ctx context.Context) error { //nolint:funlen
	if len(bp.dirAgg) == 0 {
		return nil
	}

	batch, err := bp.conn.PrepareBatch(ctx, rollupsInsertSQL)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(bp.dirAgg))
	for k := range bp.dirAgg {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		a := bp.dirAgg[k]

		uids := collectUint32(a.uids)
		gids := collectUint32(a.gids)
		exts := collectString(a.exts)
		ftypes := collectUint8(a.ftypes)

		if err := batch.Append(
			bp.mountPath, bp.scanID, bp.scanTime, k,
			a.totalSize, a.fileCount,
			a.oldestAT, a.newestAT, a.oldestMT, a.newestMT,
			uids, gids, exts, ftypes,
			uint8(db.DGUTAgeAll),
		); err != nil { //nolint:lll
			return err
		}
	}

	return batch.Send()
}

func collectUint32(m map[uint32]struct{}) []uint32 { //nolint:ireturn
	out := make([]uint32, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	return out
}

func collectUint8(m map[uint8]struct{}) []uint8 { //nolint:ireturn
	out := make([]uint8, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	return out
}

func collectString(m map[string]struct{}) []string { //nolint:ireturn
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	return out
}

// ingestScan processes a stats file and loads it into ClickHouse.
func (c *Clickhouse) ingestScan(
	ctx context.Context,
	mountPath string,
	scanID uuid.UUID,
	scanTime time.Time,
	r io.Reader,
) error {
	// Create batch processor
	bp, err := c.newBatchProcessor(ctx, mountPath, scanID, scanTime)
	if err != nil {
		return err
	}

	// Track directories we've already inserted to avoid duplicates
	dirsSeen := make(map[string]bool)

	if err := scanAndProcessEntries(ctx, bp, r, mountPath, dirsSeen); err != nil {
		return err
	}

	// Insert synthetic ancestor directories above the mount to enable global listings
	if err := insertSyntheticAncestorDirs(bp, mountPath, dirsSeen); err != nil {
		return err
	}

	// Final flush
	if err := bp.Flush(ctx); err != nil {
		return err
	}

	// Insert aggregated directory rollups after processing all entries
	if err := bp.afterScanInsertRollups(ctx); err != nil {
		return fmt.Errorf("insert aggregated rollups: %w", err)
	}

	return nil
}

// scanAndProcessEntries scans through the file records and processes each entry.
//
//nolint:gocognit,funlen
func scanAndProcessEntries(
	ctx context.Context,
	bp *BatchProcessor,
	r io.Reader,
	mountPath string,
	dirsSeen map[string]bool,
) error {
	parser := stats.NewStatsParser(r)
	fi := new(stats.FileInfo)

	for {
		// Read the next entry
		parseErr := parser.Scan(fi)
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				break
			}

			return fmt.Errorf("parser error: %w", parseErr)
		}

		// Process the file entry
		if err := processFileEntry(bp, fi, mountPath, dirsSeen); err != nil {
			return err
		}

		// Flush batches if needed
		if bp.NeedsFlush() {
			if err := bp.Flush(ctx); err != nil {
				return fmt.Errorf("failed to flush batches: %w", err)
			}
		}
	}

	return nil
}

// processFileEntry handles a single file entry during scan ingestion.
//
//nolint:gocyclo,funlen
func processFileEntry(bp *BatchProcessor, fi *stats.FileInfo, mountPath string, dirsSeen map[string]bool) error {
	path := string(fi.Path)

	// Ensure all paths are absolute
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

	// Handle integer conversions
	inode := uint64(0)
	if fi.Inode > 0 {
		inode = uint64(fi.Inode)
	}

	// Set directory size
	size := uint64(0)
	if fi.Size > 0 {
		size = uint64(fi.Size)
	} else if isDir {
		size = DirectorySize
	}

	// Skip duplicate directory entries
	if isDir && dirsSeen[path] {
		return nil
	}

	if isDir {
		dirsSeen[path] = true
	}

	// Compute depth: number of '/' separators excluding leading root; ensure directories end with '/'
	dpth := computeDepth(path)

	// Add file entry to batch
	if err := bp.AddFile(path, parent, name, dpth, ext, ft, inode, size,
		fi.UID, fi.GID, mtime, atime, ctime); err != nil {
		return fmt.Errorf("failed to add file entry: %w", err)
	}

	return processAncestorRollups(bp, fi, path, parent, isDir, size, atime, mtime, ext, mountPath, dirsSeen)
}

// processAncestorRollups processes rollups for all ancestor directories.
// It calculates rollups for each directory in the path hierarchy.
func processAncestorRollups(bp *BatchProcessor, fi *stats.FileInfo, path, parent string,
	isDir bool, size uint64, atime, mtime time.Time, ext, mountPath string, dirsSeen map[string]bool) error {
	// Include the directory itself in its own subtree if the entry is a directory
	base := parent
	if isDir {
		base = path
	}

	if err := addRollupsWithinMount(bp, fi, base, mountPath, size, atime, mtime, ext, dirsSeen); err != nil {
		return err
	}

	if err := addRollupsAboveMount(bp, fi, mountPath, size, atime, mtime, ext, dirsSeen); err != nil {
		return err
	}

	return nil
}

// addRollupsWithinMount adds rollups for the path and its ancestors inside the mount.
func addRollupsWithinMount(bp *BatchProcessor, fi *stats.FileInfo, base, mountPath string,
	size uint64, atime, mtime time.Time, ext string, dirsSeen map[string]bool) error {
	ForEachAncestor(base, mountPath, func(a string) bool {
		// Track directory
		dirsSeen[a] = true

		ft := MapEntryType(fi.EntryType)
		bp.aggregateRollup(a, size, atime, mtime, fi.UID, fi.GID, ext, uint8(ft))

		return true
	})

	return nil
}

// addRollupsAboveMount adds rollups for ancestors above the mount up to root.
func addRollupsAboveMount(bp *BatchProcessor, fi *stats.FileInfo, mountPath string,
	size uint64, atime, mtime time.Time, ext string, _ map[string]bool) error {
	mp := EnsureDir(mountPath)
	parentOfMount, _ := SplitParentAndName(mp)

	// If there are no ancestors above the mount (eg. mount is "/"), do nothing.
	if parentOfMount == mp {
		return nil
	}

	ForEachAncestor(parentOfMount, "/", func(a string) bool {
		// Do not mark ancestors above the mount as seen in dirsSeen; that map is
		// used to de-duplicate fs_entries for real directories, and marking these
		// would prevent insertSyntheticAncestorDirs() from inserting the synthetic
		// ancestor directory entries (eg. "/lustre/") needed for global listings.
		ft := MapEntryType(fi.EntryType)
		bp.aggregateRollup(a, size, atime, mtime, fi.UID, fi.GID, ext, uint8(ft))

		return true
	})

	return nil
}

// insertSyntheticAncestorDirs inserts one directory entry per ancestor above the mountPath
// (eg. for "/mnt/b/c/" inserts "/mnt/b/" and "/mnt/") so that global listings at
// high-level directories (including "/") can enumerate down to the real mountpoints.
func insertSyntheticAncestorDirs(bp *BatchProcessor, mountPath string, dirsSeen map[string]bool) error {
	mp := EnsureDir(mountPath)
	parent, _ := SplitParentAndName(mp)

	// Nothing to do if already at root
	if parent == "/" && mp == "/" {
		return nil
	}

	// Use the scan time as a consistent timestamp for synthetic dirs.
	t := bp.scanTime

	// Walk ancestors up to root, excluding root itself as an entry
	var firstErr error

	ForEachAncestor(parent, "/", func(a string) bool {
		if a == "/" { // do not insert a synthetic root entry
			return true
		}

		// Skip if we've already seen this directory
		if dirsSeen[a] {
			return true
		}

		if err := addSyntheticDirAndRollups(bp, a, t, dirsSeen); err != nil {
			firstErr = err

			return false
		}

		return true
	})

	return firstErr
}

// addSyntheticDirAndRollups inserts a synthetic directory entry for 'a' and adds
// rollup rows for 'a' and all its ancestors up to and including root.
func addSyntheticDirAndRollups(
	bp *BatchProcessor,
	a string,
	t time.Time,
	dirsSeen map[string]bool,
) error {
	p, name := SplitParentAndName(a)

	// Insert a single directory entry with zero size/ids; duplicates across mounts will be
	// deduplicated at query time for global listings.
	//
	// Use DirectorySize for synthetic directories to ensure consistent directory sizing
	if err := insertSyntheticDir(bp, a, p, name, t); err != nil {
		return err
	}

	// Mark this directory as seen
	dirsSeen[a] = true

	if err := addAncestorRollupsForSynthetic(bp, a, t, dirsSeen); err != nil {
		return err
	}

	return nil
}

// insertSyntheticDir inserts a single synthetic directory entry.
func insertSyntheticDir(bp *BatchProcessor, a, parent, name string, t time.Time) error {
	return bp.AddFile(a, parent, name, computeDepth(a), "", FileTypeDir, 0, DirectorySize, 0, 0, t, t, t)
}

// addAncestorRollupsForSynthetic adds zero-sized rollups for the synthetic directory and its ancestors.
func addAncestorRollupsForSynthetic(bp *BatchProcessor, a string, t time.Time, dirsSeen map[string]bool) error {
	ForEachAncestor(a, "/", func(ra string) bool {
		dirsSeen[ra] = true
		bp.aggregateRollup(ra, 0, t, t, 0, 0, "", uint8(FileTypeDir))
		return true
	})

	return nil
}
