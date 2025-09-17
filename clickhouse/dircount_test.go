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

func TestDirCountWithFilesRespectsTimeBuckets(t *testing.T) {
	ch, ctx, cleanup, err := NewUserEphemeralForTests()
	if err != nil {
		t.Skip("skipping: clickhouse unavailable")
	}
	defer cleanup()

	mount := "/buckettest/mnt/"

	// Build dataset with old/new atimes and mtimes
	now := time.Now().Truncate(time.Second)
	oldAT := now.AddDate(-2, 0, 0).Unix() // >1y
	oldMT := now.AddDate(0, -3, 0).Unix() // >2m

	root := statsdata.NewRoot(mount, now.Unix())
	// Two directories with files in each
	statsdata.AddFile(root, "X/old.log", 1001, 1001, 10, oldAT, oldMT)
	statsdata.AddFile(root, "X/new.txt", 1002, 1002, 20, now.Unix(), now.Unix())
	statsdata.AddFile(root, "Y/recent.dat", 1003, 1003, 30, now.Unix(), now.Unix())

	tmp := t.TempDir()
	p := filepath.Join(tmp, "stats")
	f, err := os.Create(p)
	require.NoError(t, err)
	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, _, err := OpenStatsFile(p)
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, ch.UpdateClickhouse(ctx, mount, r))

	// No filter: both X and Y have files; mount root also has descendant files
	dc, err := ch.DirCountWithFiles(ctx, mount, Filters{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), dc)

	// ATime >1y should include directory X (old.log) and the mount root (as descendant)
	dc, err = ch.DirCountWithFiles(ctx, mount, Filters{ATimeBucket: ">1y"})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), dc)

	// MTime >2m should include directory X (old.log) and the mount root (as descendant)
	dc, err = ch.DirCountWithFiles(ctx, mount, Filters{MTimeBucket: ">2m"})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), dc)
}
