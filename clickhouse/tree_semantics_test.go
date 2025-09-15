/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to do so, subject to the following conditions:
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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

// TestSubtreeSummaryAddsDirectoryContributions ensures SubtreeSummary includes
// directory-with-files contributions and exposes directory type in FTypes.
func TestSubtreeSummaryAddsDirectoryContributions(t *testing.T) {
	ch, ctx, cleanup, err := clickhouse.NewUserEphemeralForTests()
	if err != nil {
		t.Skipf("no ClickHouse: %v", err)

		return
	}
	defer cleanup()

	// Build a tiny tree: /mnt/A/{x.txt,y.txt} and /mnt/B/z.txt
	uid, gid := uint32(1001), uint32(2001)
	mnt := "/mnt/"
	ref := time.Now().Truncate(time.Second).Unix()
	root := statsdata.NewRoot(mnt, ref)
	statsdata.AddFile(root, "A/x.txt", uid, gid, 10, ref, ref)
	statsdata.AddFile(root, "A/y.txt", uid, gid, 20, ref, ref)
	statsdata.AddFile(root, "B/z.txt", uid, gid, 30, ref, ref)

	tmpDir := t.TempDir()
	fp := filepath.Join(tmpDir, "data.tsv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	_, err = io.Copy(f, root.AsReader())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, _, err := clickhouse.OpenStatsFile(fp)
	require.NoError(t, err)

	defer r.Close()

	require.NoError(t, ch.UpdateClickhouse(ctx, mnt, r))

	// Unfiltered subtree at /mnt/A/ should have 2 files (10+20) plus 1 directory-with-files
	s, err := ch.SubtreeSummary(ctx, mnt+"A/", clickhouse.Filters{})
	require.NoError(t, err)

	// File-only size = 30, directories-with-files = 1 -> +4096
	assert.Equal(t, uint64(30+clickhouse.DirectorySize), s.TotalSize)
	assert.Equal(t, uint64(2+1), s.FileCount)

	// Should include directory type in FTypes
	hasDir := false

	for _, ft := range s.FTypes {
		if ft == uint8(clickhouse.FileTypeDir) {
			hasDir = true

			break
		}
	}

	assert.True(t, hasDir, "expected directory type in Summary.FTypes")
}
