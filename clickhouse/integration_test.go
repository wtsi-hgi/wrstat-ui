/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
	entries, err := ch.ListImmediateChildren(ctx, mountPath+"humgen/projects/")
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

package clickhouse_test

import (
	"context"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

// TestClickHouseIntegration performs integration tests with a real ClickHouse
// instance. This test will be skipped if no ClickHouse connection is available.
func TestClickHouseIntegration(t *testing.T) {
	// Check if TEST_CLICKHOUSE_HOST environment variable is set
	// If not, use default host
	chHost := os.Getenv("TEST_CLICKHOUSE_HOST")
	if chHost == "" {
		chHost = "127.0.0.1" // default host
	}

	chPort := os.Getenv("TEST_CLICKHOUSE_PORT")
	if chPort == "" {
		chPort = "9000" // default port
	}

	chUsername := os.Getenv("TEST_CLICKHOUSE_USERNAME")
	if chUsername == "" {
		chUsername = "default" // default username
	}

	chPassword := os.Getenv("TEST_CLICKHOUSE_PASSWORD")

	// Create a unique test database name based on the current username
	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("Failed to get current user: %v", err)
	}

	testDatabase := "test_wrstatui_" + currentUser.Username

	// Create connection parameters
	params := clickhouse.ConnectionParams{
		Host:     chHost,
		Port:     chPort,
		Database: "default", // connect to default first
		Username: chUsername,
		Password: chPassword,
	}

	// Try connecting
	ctx := context.Background()

	adminCh, err := clickhouse.New(params)
	if err != nil {
		t.Skipf("Skipping ClickHouse integration tests - could not connect to ClickHouse: %v", err)

		return
	}

	defer adminCh.Close()

	// Drop test database if it exists (cleanup from previous failed tests)
	err = adminCh.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+testDatabase)
	if err != nil {
		t.Logf("Warning: failed to drop existing test DB: %v", err)
	}

	// Create a fresh test database
	err = adminCh.ExecuteQuery(ctx, "CREATE DATABASE "+testDatabase)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Clean up the test database after the test
	defer func() {
		err = adminCh.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+testDatabase)
		if err != nil {
			t.Errorf("Failed to drop test database during cleanup: %v", err)
		}
	}()

	// Now connect to the test database
	params.Database = testDatabase
	ch, err := clickhouse.New(params)
	require.NoError(t, err)

	// Ensure schema exists in the test database
	require.NoError(t, ch.CreateSchema(ctx))

	// Prepare test data
	uid := uint32(1000) // standard test user ID
	gid := uint32(1000) // standard test group ID
	mountPath := "/lustre/scratch125/"

	// Create test data
	refTime := time.Now().Truncate(time.Second)
	unixTime := refTime.Unix()
	root := statsdata.NewRoot(mountPath, unixTime)
	statsdata.AddFile(root, "humgen/projects/A/file1", uid, gid, 1000, unixTime, unixTime)
	statsdata.AddFile(root, "humgen/projects/A/file2", uid, gid, 2000, unixTime, unixTime)
	statsdata.AddFile(root, "humgen/projects/B/file3", uid, gid, 3000, unixTime, unixTime)

	// Create temporary input file
	tmpDir := t.TempDir()
	statsPath := filepath.Join(tmpDir, "test_stats")

	f, err := os.Create(statsPath)
	require.NoError(t, err)

	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open the stats file
	r, _, err := clickhouse.OpenStatsFile(statsPath)
	require.NoError(t, err)

	defer r.Close()

	// Update the ClickHouse database
	err = ch.UpdateClickhouse(ctx, mountPath, r)
	require.NoError(t, err)

	// Test various queries
	t.Run("ScanCountCheck", func(t *testing.T) {
		var scanCount uint64

		query := "SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?"
		err = ch.ExecuteQuery(ctx, query, mountPath, &scanCount)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, scanCount, uint64(1))
	})

	t.Run("FileEntriesCheck", func(t *testing.T) {
		var fileCount uint64

		query := "SELECT count() FROM fs_entries_current WHERE mount_path = ?"
		err = ch.ExecuteQuery(ctx, query, mountPath, &fileCount)
		require.NoError(t, err)
		assert.Greater(t, fileCount, uint64(3)) // Should have at least our 3 files plus directories
	})

	t.Run("AncestorRollupsCheck", func(t *testing.T) {
		var rollupCount uint64

		query := "SELECT count() FROM ancestor_rollups_current WHERE mount_path = ?"
		err = ch.ExecuteQuery(ctx, query, mountPath, &rollupCount)
		require.NoError(t, err)
		assert.Greater(t, rollupCount, uint64(3)) // Should have multiple rollups per file
	})

	t.Run("TotalSizeCalculationCheck", func(t *testing.T) {
		var totalSize uint64

		query := `SELECT total_size FROM ancestor_rollups_current WHERE mount_path = ? AND ancestor = ?`
		err = ch.ExecuteQuery(ctx, query, mountPath, mountPath, &totalSize)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, totalSize, uint64(6000)) // At least 1000 + 2000 + 3000
	})

	t.Run("PathQueryCheck", func(t *testing.T) {
		var fileSize uint64

		query := `SELECT size FROM fs_entries_current WHERE path = ?`
		err = ch.ExecuteQuery(ctx, query, mountPath+"humgen/projects/A/file1", &fileSize)
		require.NoError(t, err)
		assert.Equal(t, uint64(1000), fileSize)
	})

	t.Run("SubtreeSummaryCheck", func(t *testing.T) {
		summary, err := ch.SubtreeSummary(ctx, mountPath+"humgen/projects/A/", clickhouse.Filters{})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, summary.TotalSize, uint64(3000)) // At least 1000 + 2000
		assert.GreaterOrEqual(t, summary.FileCount, uint64(2))    // At least 2 files in directory A
	})

	t.Run("ListImmediateChildrenCheck", func(t *testing.T) {
		entries, err := ch.ListImmediateChildren(ctx, mountPath+"humgen/projects/")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 2)
	})

	t.Run("SearchGlobPathsCheck", func(t *testing.T) {
		paths, err := ch.SearchGlobPaths(ctx, "*/projects/*/file*", 10, false)
		require.NoError(t, err)
		assert.Equal(t, 3, len(paths)) // All 3 files match the pattern
	})

	// Verify GetLastScanTimes returns the latest ready scan's finished_at
	t.Run("LastScanTimesCheck", func(t *testing.T) {
		m, err := ch.GetLastScanTimes(ctx)
		require.NoError(t, err)

		got, ok := m[mountPath]
		require.True(t, ok)

		var want time.Time

		q := `SELECT argMax(finished_at, scan_id) FROM scans WHERE state = 'ready' AND mount_path = ?`
		require.NoError(t, ch.ExecuteQuery(ctx, q, mountPath, &want))
		assert.WithinDuration(t, want, got, time.Second)
	})

	// Perform a second scan with some files unchanged, one deleted, and one new
	t.Run("MultiScanLatestOnly", func(t *testing.T) {
		// Ensure the next scan_id (Unix seconds) is strictly greater
		time.Sleep(1 * time.Second)

		// Build second dataset: file1 unchanged (1000), file2 changed (2500), file3 deleted, new file4 (4000)
		refTime2 := time.Now().Truncate(time.Second)
		unixTime2 := refTime2.Unix()
		root2 := statsdata.NewRoot(mountPath, unixTime2)
		statsdata.AddFile(root2, "humgen/projects/A/file1", uid, gid, 1000, unixTime2, unixTime2)
		statsdata.AddFile(root2, "humgen/projects/A/file2", uid, gid, 2500, unixTime2, unixTime2) // modified
		statsdata.AddFile(root2, "humgen/projects/B/file4", uid, gid, 4000, unixTime2, unixTime2) // new

		// Write the second stats to a temp file
		statsPath2 := filepath.Join(tmpDir, "test_stats_2")
		f2, err := os.Create(statsPath2)
		require.NoError(t, err)

		_, err = io.Copy(f2, root2.AsReader())
		require.NoError(t, err)
		require.NoError(t, f2.Close())

		// Ingest second scan
		r2, _, err := clickhouse.OpenStatsFile(statsPath2)
		require.NoError(t, err)

		defer r2.Close()

		require.NoError(t, ch.UpdateClickhouse(ctx, mountPath, r2))

		// Only the latest ready scan should remain recorded for this mount
		var readyCount uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?",
			mountPath,
			&readyCount,
		))
		assert.Equal(t, uint64(1), readyCount)

		// Underlying fs_entries should only have one scan_id for this mount (older partitions dropped)
		var uniqScans uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT uniqExact(scan_id) FROM fs_entries WHERE mount_path = ?",
			mountPath,
			&uniqScans,
		))
		assert.Equal(t, uint64(1), uniqScans)

		// Validate paths presence/absence and no duplicates
		var (
			cnt  uint64
			size uint64
		)

		// file1 unchanged: present once, size 1000
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/A/file1",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT size FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/A/file1",
			&size,
		))
		assert.Equal(t, uint64(1000), size)

		// file2 modified: present once, size 2500
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/A/file2",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT size FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/A/file2",
			&size,
		))
		assert.Equal(t, uint64(2500), size)

		// file3 deleted: absent
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/B/file3",
			&cnt,
		))
		assert.Equal(t, uint64(0), cnt)

		// file4 new: present once, size 4000
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/B/file4",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT size FROM fs_entries_current WHERE path = ?",
			mountPath+"humgen/projects/B/file4",
			&size,
		))
		assert.Equal(t, uint64(4000), size)

		// Rollup totals should reflect new dataset
		var totalSizeRoot uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT total_size FROM ancestor_rollups_current WHERE mount_path = ? AND ancestor = ?",
			mountPath,
			mountPath,
			&totalSizeRoot,
		))
		assert.GreaterOrEqual(t, totalSizeRoot, uint64(1000+2500+4000))

		var totalSizeA uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT total_size FROM ancestor_rollups_current WHERE mount_path = ? AND ancestor = ?",
			mountPath,
			mountPath+"humgen/projects/A/",
			&totalSizeA,
		))
		assert.GreaterOrEqual(t, totalSizeA, uint64(1000+2500))

		var totalSizeB uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT total_size FROM ancestor_rollups_current WHERE mount_path = ? AND ancestor = ?",
			mountPath,
			mountPath+"humgen/projects/B/",
			&totalSizeB,
		))
		assert.GreaterOrEqual(t, totalSizeB, uint64(4000))
	})

	// Ensure retention and latest-only semantics are per-mount (independent mounts)
	t.Run("PerMountRetention", func(t *testing.T) {
		mountPath2 := "/lustre/scratch126/"

		refTimeB := time.Now().Truncate(time.Second)
		unixTimeB := refTimeB.Unix()
		rootB := statsdata.NewRoot(mountPath2, unixTimeB)
		statsdata.AddFile(rootB, "other/projects/X/fileX.txt", uid, gid, 1234, unixTimeB, unixTimeB)

		statsPathB := filepath.Join(tmpDir, "test_stats_mount2")
		fb, err := os.Create(statsPathB)
		require.NoError(t, err)

		_, err = io.Copy(fb, rootB.AsReader())
		require.NoError(t, err)
		require.NoError(t, fb.Close())

		rb, _, err := clickhouse.OpenStatsFile(statsPathB)
		require.NoError(t, err)

		defer rb.Close()

		require.NoError(t, ch.UpdateClickhouse(ctx, mountPath2, rb))

		// Both mounts should have exactly one ready scan each
		var ready1, ready2 uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?",
			mountPath,
			&ready1,
		))
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?",
			mountPath2,
			&ready2,
		))
		assert.Equal(t, uint64(1), ready1)
		assert.Equal(t, uint64(1), ready2)

		// Verify current view returns the file for mount2
		var cnt uint64
		require.NoError(t, ch.ExecuteQuery(
			ctx,
			"SELECT count() FROM fs_entries_current WHERE mount_path = ? AND path = ?",
			mountPath2,
			mountPath2+"other/projects/X/fileX.txt",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)
	})

	// Filters: GIDs, UIDs, Exts, ATimeBucket, MTimeBucket
	t.Run("FilteredSubtreeSummary", func(t *testing.T) {
		mountPath3 := "/lustre/scratch127/"

		refTimeC := time.Now().Truncate(time.Second)
		unixTimeC := refTimeC.Unix()

		oldAT := unixTimeC - 400*24*3600 // >1 year
		oldMT := unixTimeC - 70*24*3600  // >2 months

		rootC := statsdata.NewRoot(mountPath3, unixTimeC)
		// One old file (atime>1y, mtime>2m), ext log, uid/gid 2001/3001
		statsdata.AddFile(rootC, "humgen/projects/C/old1.log", 2001, 3001, 100, oldAT, oldMT)
		// Recent file (0d), ext txt, uid/gid 2002/3002
		statsdata.AddFile(rootC, "humgen/projects/C/recent.txt", 2002, 3002, 200, unixTimeC, unixTimeC)
		// GID-specific file
		statsdata.AddFile(rootC, "humgen/projects/C/gidfile.bin", 2003, 4242, 300, unixTimeC, unixTimeC)
		// UID-specific file
		statsdata.AddFile(rootC, "humgen/projects/C/uidfile.dat", 4242, 3003, 400, unixTimeC, unixTimeC)

		statsPathC := filepath.Join(tmpDir, "test_stats_mount3")
		fc, err := os.Create(statsPathC)
		require.NoError(t, err)

		_, err = io.Copy(fc, rootC.AsReader())
		require.NoError(t, err)
		require.NoError(t, fc.Close())

		rc, _, err := clickhouse.OpenStatsFile(statsPathC)
		require.NoError(t, err)

		defer rc.Close()

		require.NoError(t, ch.UpdateClickhouse(ctx, mountPath3, rc))

		base := mountPath3 + "humgen/projects/C/"

		// ATime bucket >1y selects only old1.log (size 100)
		s, err := ch.SubtreeSummary(ctx, base, clickhouse.Filters{ATimeBucket: ">1y"})
		require.NoError(t, err)
		assert.Equal(t, uint64(100), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)

		// MTime bucket >2m selects only old1.log (size 100)
		s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{MTimeBucket: ">2m"})
		require.NoError(t, err)
		assert.Equal(t, uint64(100), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)

		// Ext filter 'log' -> old1.log (100)
		s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{Exts: []string{"log"}})
		require.NoError(t, err)
		assert.Equal(t, uint64(100), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)

		// Ext filter 'txt' -> recent.txt (200)
		s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{Exts: []string{"txt"}})
		require.NoError(t, err)
		assert.Equal(t, uint64(200), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)

		// GID filter 4242 -> gidfile.bin (300)
		s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{GIDs: []uint32{4242}})
		require.NoError(t, err)
		assert.Equal(t, uint64(300), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)

		// UID filter 4242 -> uidfile.dat (400)
		s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{UIDs: []uint32{4242}})
		require.NoError(t, err)
		assert.Equal(t, uint64(400), s.TotalSize)
		assert.Equal(t, uint64(1), s.FileCount)
	})

	// Concurrent ingests on different mount points should not interfere with each other
	t.Run("ConcurrentMountIngests", func(t *testing.T) {
		// Use a fresh database for this subtest to avoid interference from earlier ingests
		tempDB := testDatabase + "_conc_" + time.Now().Format("150405")
		require.NoError(t, adminCh.ExecuteQuery(ctx, "CREATE DATABASE "+tempDB))

		defer func() {
			require.NoError(t, adminCh.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+tempDB))
		}()

		params2 := params
		params2.Database = tempDB

		ch2, err2 := clickhouse.New(params2)
		require.NoError(t, err2)

		defer ch2.Close()

		require.NoError(t, ch2.CreateSchema(ctx))

		mountA := "/lustre/scratch128/"
		mountB := "/lustre/scratch129/"

		ref := time.Now().Truncate(time.Second).Unix()
		rootA := statsdata.NewRoot(mountA, ref)
		statsdata.AddFile(rootA, "projA/data/fileA1", uid, gid, 111, ref, ref)
		statsdata.AddFile(rootA, "projA/data/fileA2", uid, gid, 222, ref, ref)

		rootB := statsdata.NewRoot(mountB, ref)
		statsdata.AddFile(rootB, "projB/data/fileB1", uid, gid, 333, ref, ref)
		statsdata.AddFile(rootB, "projB/data/fileB2", uid, gid, 444, ref, ref)

		ctx2, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		var wg sync.WaitGroup

		errs := make(chan error, 2)

		wg.Add(2)

		go func() {
			defer wg.Done()

			// Use a dedicated connection in the isolated DB to simulate independent ingesters
			chA, errA := clickhouse.New(params2)
			if errA != nil {
				errs <- errA

				return
			}
			defer chA.Close()

			r := rootA.AsReader()
			defer r.Close()

			errs <- chA.UpdateClickhouse(ctx2, mountA, r)
		}()

		go func() {
			defer wg.Done()

			// Use a dedicated connection in the isolated DB to simulate independent ingesters
			chB, errB := clickhouse.New(params2)
			if errB != nil {
				errs <- errB

				return
			}
			defer chB.Close()

			r := rootB.AsReader()
			defer r.Close()

			errs <- chB.UpdateClickhouse(ctx2, mountB, r)
		}()

		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}

		// Each mount should have exactly one ready scan
		var readyA, readyB uint64
		require.NoError(t, ch2.ExecuteQuery(
			ctx2,
			"SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?",
			mountA,
			&readyA,
		))
		require.NoError(t, ch2.ExecuteQuery(
			ctx2,
			"SELECT count() FROM scans WHERE state = 'ready' AND mount_path = ?",
			mountB,
			&readyB,
		))
		assert.Equal(t, uint64(1), readyA)
		assert.Equal(t, uint64(1), readyB)

		// Verify files present for both mounts
		var cnt uint64
		require.NoError(t, ch2.ExecuteQuery(
			ctx2,
			"SELECT count() FROM fs_entries_current WHERE mount_path = ? AND path = ?",
			mountA,
			mountA+"projA/data/fileA1",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)
		require.NoError(t, ch2.ExecuteQuery(
			ctx2,
			"SELECT count() FROM fs_entries_current WHERE mount_path = ? AND path = ?",
			mountB,
			mountB+"projB/data/fileB1",
			&cnt,
		))
		assert.Equal(t, uint64(1), cnt)

		// Now aggregate across all mounts at root (in the isolated DB) and ensure totals match both ingests combined
		s, err := ch2.SubtreeSummary(ctx2, "/", clickhouse.Filters{})
		require.NoError(t, err)

		// We ingested 2 files under each mount (4 total) plus their ancestor directories. FileCount now includes
		// directories as well as files, so it should be >= 4.
		assert.GreaterOrEqual(t, s.FileCount, uint64(4))

		// Total size is at least 111+222+333+444 == 1110 (includes directories now)
		assert.GreaterOrEqual(t, s.TotalSize, uint64(1110))

		// Additionally, verify root count strictly exceeds the sum of counts at each mount root
		// (due to the presence of synthetic ancestor directories like "/lustre/" etc.).
		sa, err := ch2.SubtreeSummary(ctx2, mountA, clickhouse.Filters{})
		require.NoError(t, err)

		sb, err := ch2.SubtreeSummary(ctx2, mountB, clickhouse.Filters{})
		require.NoError(t, err)

		sumMountCounts := sa.FileCount + sb.FileCount
		assert.Greater(t, s.FileCount, sumMountCounts)

		// SubtreeSummary should also work at root with filters (falls back under the hood).
		// Filtering by GID ensures we only count files (directories have gid=0 by default in test data).
		sFiltered, err := ch2.SubtreeSummary(ctx2, "/", clickhouse.Filters{GIDs: []uint32{gid}})
		require.NoError(t, err)
		assert.Equal(t, uint64(1110), sFiltered.TotalSize)
		assert.Equal(t, uint64(4), sFiltered.FileCount)

		// Global listings should show synthetic ancestors leading to real mounts
		// List at root -> expect at least 'lustre'
		ents, err := ch2.ListImmediateChildren(ctx2, "/")
		require.NoError(t, err)

		var haveMnt bool

		for _, e := range ents {
			if e.Path == "/lustre/" && e.FType == uint8(clickhouse.FileTypeDir) {
				haveMnt = true

				break
			}
		}

		assert.True(t, haveMnt, "expected /lustre in root children")

		// List at /lustre -> expect 'scratch128' and 'scratch129'
		ents, err = ch2.ListImmediateChildren(ctx2, "/lustre/")
		require.NoError(t, err)

		var haveA, haveB bool

		for _, e := range ents {
			if e.Path == "/lustre/scratch128/" {
				haveA = true
			}

			if e.Path == "/lustre/scratch129/" {
				haveB = true
			}
		}

		assert.True(t, haveA && haveB, "expected scratch128 and scratch129 under /lustre")
	})
}
