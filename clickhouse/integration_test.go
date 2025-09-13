/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package clickhouse_test

import (
	"context"
	"io"
	"os"
	"os/user"
	"path/filepath"
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

	defer ch.Close()

	// Create schema
	err = ch.CreateSchema(ctx)
	require.NoError(t, err)

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
		summary, err := ch.OptimizedSubtreeSummary(ctx, mountPath, mountPath+"humgen/projects/A/", clickhouse.Filters{})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, summary.TotalSize, uint64(3000)) // At least 1000 + 2000
		assert.GreaterOrEqual(t, summary.FileCount, uint64(2))    // At least 2 files in directory A
	})

	t.Run("ListImmediateChildrenCheck", func(t *testing.T) {
		entries, err := ch.ListImmediateChildren(ctx, mountPath, mountPath+"humgen/projects/")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 2)
	})

	t.Run("SearchGlobPathsCheck", func(t *testing.T) {
		paths, err := ch.SearchGlobPaths(ctx, mountPath, "*/projects/*/file*", 10, false)
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
}
