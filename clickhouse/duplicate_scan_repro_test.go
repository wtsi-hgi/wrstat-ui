package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

// ingestWithScanID performs a full ingest cycle using a fixed scanID.
func ingestWithScanID(t *testing.T, ch *Clickhouse, _ string, scanID uint64, data []byte) {
	t.Helper()

	const mount = "/"

	ctx := context.Background()

	// Use scanID as seconds since epoch for reproducibility in tests.
	sec := int64(scanID) //nolint:gosec // scanID is test value, overflow checked below
	if sec < 0 {
		t.Fatal("scanID too large for int64")
	}

	started := time.Unix(sec, 0)
	finished := started.Add(1 * time.Second)
	scanUUID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s-%d", mount, scanID)))

	// Register, ingest, promote (no retention to keep both ingests present)
	require.NoError(t, ch.registerScan(ctx, mount, scanUUID, started, started))
	require.NoError(t, ch.ingestScan(ctx, mount, scanUUID, started, bytes.NewReader(data)))
	require.NoError(t, ch.promoteScan(ctx, mount, scanUUID, started, started, finished))
}

// TestDuplicateScanIDReproducesDoubleCount reproduces the double-counting seen in the server
// tree API test by ingesting the same dataset twice with an identical scan_id. This causes
// fs_entries_current to include rows from both ingests (same scan_id), inflating file counts.
func TestDuplicateScanIDReproducesDoubleCount(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("no ClickHouse: %v", err)

		return
	}
	defer cleanup()

	// Build the same dataset used by server tests (gidC=0 branch)
	ref := time.Unix(100, 0).Unix()
	root := internaldata.CreateDefaultTestData(1, 2, 0, 101, 0, ref)

	// Materialise to bytes so we can reuse it for two ingests
	tmp := t.TempDir()
	fp := filepath.Join(tmp, "stats.tsv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	_, err = root.WriteTo(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	b, err := os.ReadFile(fp)
	require.NoError(t, err)

	// Use a fixed scanID for both ingests to simulate the server behaviour where
	// two separate ingests within the same second can produce identical scan_ids.
	const scanID uint64 = 1_700_000_000 // arbitrary stable value (represents a DateTime)

	// First ingest with fixed scanID
	ingestWithScanID(t, ch, "/", scanID, b)
	// Second ingest with the same scanID (duplicate)
	ingestWithScanID(t, ch, "/", scanID, b)

	// With UUID/scan_time semantics and deduplication by path, duplicate ingests with the
	// same logical ID do not inflate counts; expect baseline + directory augmentation.

	s, err := ch.SubtreeSummary(ctx, "/", Filters{})
	require.NoError(t, err)

	if s.FileCount != 38 {
		t.Fatalf("root count mismatch (dup scan): got %d want %d", s.FileCount, 38)
	}

	sa, err := ch.SubtreeSummary(ctx, "/a", Filters{})
	require.NoError(t, err)

	if sa.FileCount != 31 {
		t.Fatalf("/a count mismatch (dup scan): got %d want %d", sa.FileCount, 31)
	}

	sk, err := ch.SubtreeSummary(ctx, "/k", Filters{})
	require.NoError(t, err)

	if sk.FileCount != 6 {
		t.Fatalf("/k count mismatch (dup scan): got %d want %d", sk.FileCount, 6)
	}
}

// TestNoDoubleCountWithUniqueScanID verifies that if two ingests use unique scan_ids
// (analogous to using Unix nanoseconds for scan_id), then fs_entries_current does not
// double-count file rows and SubtreeSummary returns the expected baseline + directory
// augmentation (38 at root, 31 at /a, 6 at /k).
func TestNoDoubleCountWithUniqueScanID(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("no ClickHouse: %v", err)

		return
	}
	defer cleanup()

	// Build same dataset
	ref := time.Unix(100, 0).Unix()
	root := internaldata.CreateDefaultTestData(1, 2, 0, 101, 0, ref)

	tmp := t.TempDir()
	fp := filepath.Join(tmp, "stats.tsv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	_, err = root.WriteTo(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	b, err := os.ReadFile(fp)
	require.NoError(t, err)

	// Use two different scanIDs to simulate nanosecond uniqueness
	const (
		scanID1 uint64 = 1_700_000_000
		scanID2 uint64 = 1_700_000_001
	)

	ingestWithScanID(t, ch, "/", scanID1, b)
	ingestWithScanID(t, ch, "/", scanID2, b)

	// With unique scan IDs, fs_entries_current should point to scanID2 only.
	// Expect baseline counts (file-only 24 at root) + directory augmentation (14) = 38.
	s, err := ch.SubtreeSummary(ctx, "/", Filters{})
	require.NoError(t, err)

	if s.FileCount != 38 {
		t.Fatalf("root count mismatch (unique scans): got %d want %d", s.FileCount, 38)
	}

	sa, err := ch.SubtreeSummary(ctx, "/a", Filters{})
	require.NoError(t, err)

	if sa.FileCount != 31 {
		t.Fatalf("/a count mismatch (unique scans): got %d want %d", sa.FileCount, 31)
	}

	sk, err := ch.SubtreeSummary(ctx, "/k", Filters{})
	require.NoError(t, err)

	if sk.FileCount != 6 {
		t.Fatalf("/k count mismatch (unique scans): got %d want %d", sk.FileCount, 6)
	}
}
