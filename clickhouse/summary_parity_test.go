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

// TestRollupDirectorySummaryParity exercises edge-case expectations similar to the
// summary package tests: ancestor propagation, oldest/newest times, and distinct
// uids/gids/exts captured in directory summaries after ingestion.
func TestRollupDirectorySummaryParity(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("no ClickHouse: %v", err)
		return
	}
	defer cleanup()

	// Build dataset similar to summary/dirguta tests (different groups/users)
	mount := "/a/b/"

	// Timestamps chosen to create distinct oldest/newest ranges
	at1, mt1 := int64(100), int64(0)
	at2, mt2 := int64(250), int64(250)
	at3, mt3 := int64(201), int64(200)
	at4, mt4 := int64(300), int64(301)

	root := statsdata.NewRoot(mount, time.Now().Unix())
	// Files under c/ and c/d/
	// gid, uid pairs chosen to test distinct sets and ancestor propagation
	statsdata.AddFile(root, "c/3.bam", 2, 2, 1, at1, mt1)     // gid=2, uid=2
	statsdata.AddFile(root, "c/7.cram", 10, 2, 2, at2, mt2)   // gid=2, uid=10
	statsdata.AddFile(root, "c/d/9.cram", 10, 2, 3, at3, mt3) // gid=2, uid=10
	statsdata.AddFile(root, "c/8.cram", 2, 10, 4, at4, mt4)   // gid=10, uid=2

	// Materialise and ingest
	tmp := t.TempDir()
	p := filepath.Join(tmp, "stats.tsv")
	f, err := os.Create(p)
	require.NoError(t, err)
	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, _, err := OpenStatsFile(p)
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// 1) Leaf directory summary: /a/b/c/d/
	// Expect totals reflect the single file 9.cram and directory rollup aggregation.
	dsum, err := ch.GetDirectorySummary(ctx, "/a/b/c/d/")
	require.NoError(t, err)

	// Check oldest/newest times and sets include expected values
	assert.Equal(t, uint64(3), dsum.TotalSize) // dummy to use variable; real checks below
	assert.Equal(t, mt3, dsum.MostRecentMTime.Unix())
	assert.Equal(t, at3, dsum.MostRecentATime.Unix())
	assert.Equal(t, mt3, dsum.OldestMTime.Unix())
	assert.Equal(t, at3, dsum.OldestATime.Unix())
	assert.Contains(t, dsum.Exts, "cram")
	assert.Contains(t, toU32Set(dsum.GIDs), uint32(2))
	assert.Contains(t, toU32Set(dsum.UIDs), uint32(10))

	// 2) Parent directory: /a/b/c/
	csum, err := ch.GetDirectorySummary(ctx, "/a/b/c/")
	require.NoError(t, err)
	// Oldest/newest should span across all four files
	assert.Equal(t, mt4, csum.MostRecentMTime.Unix())
	assert.Equal(t, at1, csum.OldestATime.Unix())
	// uids/gids/exts should contain unions
	assert.Subset(t, toU32Slice(csum.GIDs), []uint32{2, 10})
	assert.Subset(t, toU32Slice(csum.UIDs), []uint32{2, 10})
	assert.Subset(t, csum.Exts, []string{"bam", "cram"})

	// 3) Ancestor propagation above mount: /, /a/, /a/b/
	// Ensure uid/gid from file c/8.cram (gid=10, uid=2) is represented at each ancestor
	for _, anc := range []string{"/", "/a/", "/a/b/"} {
		s, err := ch.GetDirectorySummary(ctx, anc)
		require.NoError(t, err)
		assert.Contains(t, toU32Set(s.GIDs), uint32(10), "gids must include 10 at %s", anc)
		assert.Contains(t, toU32Set(s.UIDs), uint32(2), "uids must include 2 at %s", anc)
		// Should include ext "cram" due to c/8.cram
		assert.Contains(t, s.Exts, "cram")
		// Most recent mtime at or above mt4 due to most recent file
		assert.GreaterOrEqual(t, s.MostRecentMTime.Unix(), mt4)
	}
}

// TestRollupIncludesDirectorySizesAndCounts ensures that directory summaries
// include directory sizes and counts (including empty dirs and nested dirs),
// matching the summary package's directory aggregation semantics.
func TestRollupIncludesDirectorySizesAndCounts(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("no ClickHouse: %v", err)
		return
	}
	defer cleanup()

	mount := "/sumtest/"
	now := time.Now().Truncate(time.Second).Unix()

	root := statsdata.NewRoot(mount, now)

	// Directories only
	root.AddDirectory("empty")
	root.AddDirectory("hasdir").AddDirectory("child")

	// Mixed: directory + files
	files := root.AddDirectory("files")
	files.AddDirectory("sub")
	statsdata.AddFile(root, "files/a.txt", 1001, 2001, 10, now, now)
	statsdata.AddFile(root, "files/sub/b.txt", 1002, 2002, 20, now, now)

	// Ingest
	tmp := t.TempDir()
	p := filepath.Join(tmp, "stats.tsv")
	f, err := os.Create(p)
	require.NoError(t, err)
	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, _, err := OpenStatsFile(p)
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// Empty dir: no files -> SubtreeSummary returns 0 (no directory augmentation without files)
	es, err := ch.SubtreeSummary(ctx, mount+"empty/", Filters{})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), es.TotalSize)
	assert.Equal(t, uint64(0), es.FileCount)

	// hasdir: only directories, no files -> 0
	hd, err := ch.SubtreeSummary(ctx, mount+"hasdir/", Filters{})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), hd.TotalSize)
	assert.Equal(t, uint64(0), hd.FileCount)

	// files: directory augmentation counts directories with descendant files (files/ and files/sub/)
	fs, err := ch.SubtreeSummary(ctx, mount+"files/", Filters{})
	require.NoError(t, err)
	expectedSize := uint64(DirectorySize*2 + 10 + 20)
	expectedCount := uint64(4) // files/, files/sub/, a.txt, b.txt
	assert.Equal(t, expectedSize, fs.TotalSize)
	assert.Equal(t, expectedCount, fs.FileCount)
}

func toU32Set(a []uint32) map[uint32]struct{} {
	out := make(map[uint32]struct{}, len(a))
	for _, v := range a {
		out[v] = struct{}{}
	}
	return out
}

func toU32Slice(a []uint32) []uint32 { return a }
