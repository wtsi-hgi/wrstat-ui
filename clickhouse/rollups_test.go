package clickhouse

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

// TestAncestorRollupsAggregation verifies that the ingestion pipeline populates
// ancestor_rollups_current with expected aggregated rows and that the new
// public helper query functions operate correctly.
func TestAncestorRollupsAggregation(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("skipping: clickhouse unavailable: %v", err)
		return
	}
	defer cleanup()

	mount := "/aggtest/mnt/"

	refTime := time.Now().Truncate(time.Second)
	ut := refTime.Unix()
	root := statsdata.NewRoot(mount, ut)
	// Directory structure:
	// /aggtest/mnt/dirA/file1 (100)
	// /aggtest/mnt/dirA/file2 (200)
	// /aggtest/mnt/dirB/file3 (300)
	statsdata.AddFile(root, "dirA/file1", 1111, 2222, 100, ut, ut)
	statsdata.AddFile(root, "dirA/file2", 1112, 2223, 200, ut, ut)
	statsdata.AddFile(root, "dirB/file3", 1113, 2224, 300, ut, ut)

	tmp := t.TempDir()
	statsPath := filepath.Join(tmp, "rollup_stats")
	f, err := os.Create(statsPath)
	require.NoError(t, err)
	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, _, err := OpenStatsFile(statsPath)
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// Root summary
	rootSum, err := ch.GetDirectorySummary(ctx, mount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, rootSum.TotalSize, uint64(100+200+300))
	assert.GreaterOrEqual(t, rootSum.FileCount, uint64(3))

	// Child summaries
	children, err := ch.ListChildDirectorySummaries(ctx, mount, 10)
	require.NoError(t, err)
	// Expect at least dirA and dirB
	var seenA, seenB bool
	for _, c := range children {
		if c.Path == mount+"dirA/" {
			seenA = true
		}
		if c.Path == mount+"dirB/" {
			seenB = true
		}
	}
	assert.True(t, seenA && seenB, "expected dirA and dirB in children: %+v", children)

	// Directory A subtree summary (should include its directory augmentation)
	subA, err := ch.SubtreeSummary(ctx, mount+"dirA/", Filters{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, subA.TotalSize, uint64(100+200))
	assert.GreaterOrEqual(t, subA.FileCount, uint64(2))

	// DirCountWithFiles should count at least dirA and dirB under root
	dc, err := ch.DirCountWithFiles(ctx, mount, Filters{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, dc, uint64(2))
}
