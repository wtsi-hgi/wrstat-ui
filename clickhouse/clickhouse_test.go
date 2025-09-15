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

package clickhouse_test

import (
	"context"
	"fmt"
	"os"
	osuser "os/user"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

// Tests for helper functions.
func TestNormalizeMount(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", ""},
		{"Already has trailing slash", "/path/to/mount/", "/path/to/mount/"},
		{"Needs trailing slash", "/path/to/mount", "/path/to/mount/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clickhouse.NormalizeMount(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitParentAndName(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		expectedDir  string
		expectedName string
	}{
		{"Root dir", "/", "/", ""},
		{"File in root", "/file.txt", "/", "file.txt"},
		{"Dir with trailing slash", "/path/to/dir/", "/path/to/", "dir"},
		{"File in subdir", "/path/to/file.txt", "/path/to/", "file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, name := clickhouse.SplitParentAndName(tt.path)
			assert.Equal(t, tt.expectedDir, dir)
			assert.Equal(t, tt.expectedName, name)
		})
	}
}

func TestForEachAncestor(t *testing.T) {
	tests := []struct {
		name      string
		dir       string
		mountPath string
		expected  []string
	}{
		{
			"Path with ancestors",
			"/mnt/lustre/projects/team/subdir/",
			"/mnt/lustre/",
			[]string{
				"/mnt/lustre/projects/team/subdir/",
				"/mnt/lustre/projects/team/",
				"/mnt/lustre/projects/",
				"/mnt/lustre/",
			},
		},
		{
			"Path at mount",
			"/mnt/lustre/",
			"/mnt/lustre/",
			[]string{"/mnt/lustre/"},
		},
		{
			"Path outside mount",
			"/tmp/other/",
			"/mnt/lustre/",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []string
			clickhouse.ForEachAncestor(tt.dir, tt.mountPath, func(a string) bool {
				result = append(result, a)

				return true
			})
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeriveExtLower(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		isDir    bool
		expected string
	}{
		{"Directory", "somedir", true, ""},
		{"No extension", "noext", false, ""},
		{"Simple extension", "file.txt", false, "txt"},
		{"Uppercase extension", "file.TXT", false, "txt"},
		{"Double extension compression", "file.tar.gz", false, "tar.gz"},
		{"Double extension noncompression", "file.tar.bam", false, "bam"},
		{"Hidden file", ".hidden", false, ""},
		{"Hidden with ext", ".hidden.txt", false, "txt"},
		{"Multiple dots", "file.name.with.dots.txt", false, "txt"},
		{"Multiple compression", "archive.tar.bz2", false, "tar.bz2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clickhouse.DeriveExtLower(tt.filename, tt.isDir)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDirPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"Root dir", "/", true},
		{"Subdir with slash", "/path/to/dir/", true},
		{"File path", "/path/to/file.txt", false},
		{"Empty path", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clickhouse.IsDirPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Public-API validation for bucket predicates via SubtreeSummary.
func TestBucketPredicatesViaSubtreeSummary(t *testing.T) {
	// Use in-memory test data and the SubtreeSummary API indirectly through integration helper.
	// We can't call private helpers, so we assert the observed filter behaviour is correct.
	mount := "/mnt/test/"
	base := mount + "dir/"

	// Create synthetic dataset with one old and one recent file.
	// Use current time for the root/dirs so they are NOT considered old by bucket filters.
	now := time.Now().Unix()
	oldAT := now - 400*24*3600 // >1y
	oldMT := now - 70*24*3600  // >2m

	root := statsdata.NewRoot(mount, now)
	statsdata.AddFile(root, "dir/old.log", 1, 1, 10, oldAT, oldMT)
	statsdata.AddFile(root, "dir/recent.txt", 2, 2, 20, now, now)

	// Use a real ClickHouse if available by reusing the integration helper function.
	// Here we create a temporary DB connection using defaults; if not available, skip.
	ch, ctx, cleanup, err := testNewEphemeralClickHouse()
	if err != nil {
		t.Skipf("Skipping: %v", err)

		return
	}

	defer cleanup()

	// Ingest
	r := root.AsReader()
	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// >1y ATime picks old.log only; implementation augments with the directory that contains
	// at least one matching file, adding +1 count and +DirectorySize (4096) to size.
	s, err := ch.SubtreeSummary(ctx, base, clickhouse.Filters{ATimeBucket: ">1y"})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096+10), s.TotalSize)
	assert.Equal(t, uint64(2), s.FileCount)

	// >2m MTime picks old.log only; same augmentation applies
	s, err = ch.SubtreeSummary(ctx, base, clickhouse.Filters{MTimeBucket: ">2m"})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096+10), s.TotalSize)
	assert.Equal(t, uint64(2), s.FileCount)
}

// Public-API validation for glob query generation via SearchGlobPaths.
func TestSearchGlobPathsPublic(t *testing.T) {
	mount := "/mnt/test2/"
	root := statsdata.NewRoot(mount, 1_700_000_100)
	statsdata.AddFile(root, "p/q/fileA.txt", 1, 1, 1, 1_700_000_100, 1_700_000_100)
	statsdata.AddFile(root, "p/q/fileB.log", 1, 1, 1, 1_700_000_100, 1_700_000_100)
	statsdata.AddFile(root, "p/x/other.bin", 1, 1, 1, 1_700_000_100, 1_700_000_100)

	ch, ctx, cleanup, err := testNewEphemeralClickHouse()
	if err != nil {
		t.Skipf("Skipping: %v", err)

		return
	}
	defer cleanup()

	r := root.AsReader()
	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// Case-sensitive search; pattern must match full path, so include a leading wildcard
	paths, err := ch.SearchGlobPaths(ctx, "*/p/q/file*", 0)
	require.NoError(t, err)
	assert.Len(t, paths, 2)

	// Limit works
	paths, err = ch.SearchGlobPaths(ctx, "*/p/q/file*", 1)
	require.NoError(t, err)
	assert.Len(t, paths, 1)
}

// Public-API validation for entry type mapping by round-tripping through ingestion
// and inspecting a few known properties (counting dirs/files via ListImmediateChildren).
func TestEntryTypesViaListImmediateChildren(t *testing.T) {
	mount := "/mnt/test3/"
	root := statsdata.NewRoot(mount, 1_700_000_200)
	// The stats generator will create directories automatically for parent paths
	statsdata.AddFile(root, "dirA/file.txt", 1, 1, 1, 1_700_000_200, 1_700_000_200)

	ch, ctx, cleanup, err := testNewEphemeralClickHouse()
	if err != nil {
		t.Skipf("Skipping: %v", err)

		return
	}
	defer cleanup()

	r := root.AsReader()
	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// dirA should have at least one child (file.txt)
	entries, err := ch.ListImmediateChildren(ctx, mount+"dirA/")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1)
}

// testNewEphemeralClickHouse mirrors the logic used in integration tests to provision
// a temporary database for each test using environment or sensible defaults.
// It returns (client, ctx, cleanup, error).
func testNewEphemeralClickHouse() (*clickhouse.Clickhouse, context.Context, func(), error) {
	ctx := context.Background()

	host := getenv("TEST_CLICKHOUSE_HOST", "127.0.0.1")
	port := getenv("TEST_CLICKHOUSE_PORT", "9000")
	user := getenv("TEST_CLICKHOUSE_USERNAME", "default")
	pass := getenv("TEST_CLICKHOUSE_PASSWORD", "")

	params := clickhouse.ConnectionParams{
		Host:     host,
		Port:     port,
		Database: "default",
		Username: user,
		Password: pass,
	}

	admin, err := clickhouse.New(params)
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("admin connect: %w", err)
	}

	u, uErr := osuser.Current()

	uname := "nouser"
	if uErr == nil && u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := fmt.Sprintf("test_wrstatui_%s_%d", uname, time.Now().UnixNano())

	// Best-effort drop in case it exists (eg. from an interrupted run).
	_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName) //nolint:errcheck // best-effort

	err = admin.ExecuteQuery(ctx, "CREATE DATABASE "+dbName)
	if err != nil {
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("create db: %w", err)
	}

	// Connect to new DB
	params.Database = dbName

	ch, err := clickhouse.New(params)
	if err != nil {
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName) //nolint:errcheck // best-effort
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("connect db: %w", err)
	}

	err = ch.CreateSchema(ctx)
	if err != nil {
		_ = ch.Close()
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName) //nolint:errcheck // best-effort
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("schema: %w", err)
	}

	cleanup := func() {
		_ = ch.Close()
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName) //nolint:errcheck // best-effort
		_ = admin.Close()
	}

	return ch, ctx, cleanup, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
