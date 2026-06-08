/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package clickhouse

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
)

const (
	summariseSpoolHistoryCountQuery = "SELECT count() FROM wrstat_basedirs_history " +
		"WHERE mount_path = ? AND gid = ?"
	summariseSpoolLoaderMountPathColumn = "mount_path"
	summariseSpoolLoaderSchemaMarker    = "test"
	summariseSpoolLoaderUpdatedAtColumn = "updated_at"
)

type summariseSpoolCountRow struct {
	value uint64
	err   error
}

func (r summariseSpoolCountRow) Err() error {
	return r.err
}

func (r summariseSpoolCountRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != 1 {
		return errBootstrapTestUnexpectedScanDestinationN
	}

	value, ok := dest[0].(*uint64)
	if !ok {
		return errBootstrapTestUnexpectedScanDestination
	}

	*value = r.value

	return nil
}

func (r summariseSpoolCountRow) ScanStruct(any) error {
	return r.err
}

type summariseSpoolSlowBatch struct {
	countingDGUTABatch

	delay time.Duration
}

func (b *summariseSpoolSlowBatch) Send() error {
	time.Sleep(b.delay)

	return b.countingDGUTABatch.Send()
}

type summariseSpoolFreshContextConn struct {
	bootstrapTestConn

	sendDelay time.Duration
	prepares  int
	counts    int
	switches  int
}

func (c *summariseSpoolFreshContextConn) Query(
	ctx context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch query {
	case activeSnapshotQuery:
		return &dgutaWriterCloseContextRows{columns: []string{dgutaWriterTestSnapshotIDColumn}}, nil
	case mountsActiveRowsQuery:
		return &dgutaWriterCloseContextRows{
			columns: []string{
				summariseSpoolLoaderMountPathColumn,
				dgutaWriterTestSnapshotIDColumn,
				summariseSpoolLoaderUpdatedAtColumn,
			},
		}, nil
	default:
		return nil, errBootstrapTestUnexpectedCall
	}
}

func (c *summariseSpoolFreshContextConn) QueryRow(
	ctx context.Context,
	_ string,
	_ ...any,
) driver.Row {
	c.counts++

	if err := ctx.Err(); err != nil {
		return summariseSpoolCountRow{err: err}
	}

	return summariseSpoolCountRow{value: 1}
}

func (c *summariseSpoolFreshContextConn) Exec(ctx context.Context, query string, _ ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if query == switchSnapshotQuery {
		c.switches++
	}

	return nil
}

func (c *summariseSpoolFreshContextConn) PrepareBatch(
	ctx context.Context,
	_ string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.prepares++

	return &summariseSpoolSlowBatch{delay: c.sendDelay}, nil
}

func TestClickHouseSummariseSpoolLoader(t *testing.T) {
	Convey("summarise spool reload is idempotent and does not duplicate basedirs history", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderTestSpool(spoolDir, updatedAt)

		conn, err := connectForImportFromConfig(cfg)
		So(err, ShouldBeNil)

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		So(loader.prepareSnapshot(ctx), ShouldBeNil)
		So(loader.loadTables(ctx), ShouldBeNil)
		So(conn.Close(), ShouldBeNil)

		verifyConn := th.openConn(cfg.DSN)

		Reset(func() { So(verifyConn.Close(), ShouldBeNil) })

		sid := manifest.SnapshotID
		So(countRows(ctx, verifyConn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolHistoryCountQuery, testMountPath, uint32(7)), ShouldEqual, 1)

		_, hasActive, err := readActiveSnapshotID(ctx, verifyConn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeFalse)

		So(LoadSummariseSpool(ctx, cfg, spoolDir, manifest, nil), ShouldBeNil)
		So(countRows(ctx, verifyConn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 1)
		So(countRows(ctx, verifyConn, summariseSpoolHistoryCountQuery, testMountPath, uint32(7)), ShouldEqual, 1)

		activeSID, hasActive, err := readActiveSnapshotID(ctx, verifyConn, testMountPath)
		So(err, ShouldBeNil)
		So(hasActive, ShouldBeTrue)
		So(activeSID, ShouldEqual, sid)
	})

	Convey("summarise spool load uses fresh query contexts after a slow table replay", t, func() {
		cfg := Config{QueryTimeout: 25 * time.Millisecond}
		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 5, 13, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderFileOnlySpool(spoolDir, updatedAt)
		conn := &summariseSpoolFreshContextConn{sendDelay: 2 * cfg.QueryTimeout}

		loader, err := newSummariseSpoolLoader(cfg, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.load(context.Background()), ShouldBeNil)
		So(conn.switches, ShouldEqual, 1)
		So(conn.counts, ShouldEqual, 1)
		So(conn.prepares, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history retry cleanup uses the cleanup timeout", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		conn := &summariseSpoolHistoryDeleteDeadlineConn{normalWindow: cfg.QueryTimeout}
		loader := &summariseSpoolLoader{cfg: cfg, conn: conn}
		rows := []chspool.BasedirsHistoryRow{{
			MountPath: testMountPath,
			GID:       7,
			Date:      time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC),
		}}

		ctx, cancel := queryContext(context.Background(), cfg.QueryTimeout)
		defer cancel()

		So(loader.deleteManifestHistoryRows(ctx, rows), ShouldBeNil)
		So(conn.deleteCalls, ShouldEqual, 1)
		So(conn.cleanupDeadlineCalls, ShouldEqual, 1)
	})

	Convey("summarise spool basedirs history cleanup propagates real cleanup errors", t, func() {
		cfg := Config{QueryTimeout: 100 * time.Millisecond}
		conn := &summariseSpoolHistoryDeleteDeadlineConn{
			normalWindow: cfg.QueryTimeout,
			err:          errForcedFailure,
		}
		loader := &summariseSpoolLoader{cfg: cfg, conn: conn}
		rows := []chspool.BasedirsHistoryRow{{
			MountPath: testMountPath,
			GID:       8,
			Date:      time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC),
		}}

		err := loader.deleteManifestHistoryRows(context.Background(), rows)

		So(errors.Is(err, errForcedFailure), ShouldBeTrue)
		So(conn.deleteCalls, ShouldEqual, 1)
		So(conn.cleanupDeadlineCalls, ShouldEqual, 1)
	})

	Convey("summarise spool replay caps schema2 fact and child batches", t, func() {
		const (
			factRows  = defaultProjectionBatchSize + 100
			childRows = defaultChildrenBatchSize + 100
		)

		spoolDir := filepath.Join(t.TempDir(), "spool")
		updatedAt := time.Date(2026, 6, 8, 11, 0, 0, 0, time.UTC)
		manifest := writeSummariseSpoolLoaderSchema2BatchSpool(spoolDir, updatedAt, factRows, childRows)
		conn := &lazyDGUTAImportConn{}

		loader, err := newSummariseSpoolLoader(Config{}, conn, spoolDir, manifest, nil)
		So(err, ShouldBeNil)

		So(loader.loadTables(context.Background()), ShouldBeNil)
		So(conn.totalRowsFor(insertMountDirSummaryQuery), ShouldEqual, factRows)
		So(conn.maxRowsFor(insertMountDirSummaryQuery), ShouldBeLessThanOrEqualTo, defaultProjectionBatchSize)
		So(conn.totalRowsFor(insertChildrenQuery), ShouldEqual, childRows)
		So(conn.maxRowsFor(insertChildrenQuery), ShouldBeLessThanOrEqualTo, defaultChildrenBatchSize)
	})
}

func writeSummariseSpoolLoaderTestSpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	So(set.WriteBasedirsHistory(chspool.BasedirsHistoryRow{
		MountPath:   testMountPath,
		GID:         7,
		Date:        updatedAt,
		UsageSize:   50,
		QuotaSize:   100,
		UsageInodes: 5,
		QuotaInodes: 10,
	}), ShouldBeNil)
	So(set.WriteBasedirsGroupUsage(chspool.BasedirsGroupUsageRow{
		MountPath:   testMountPath,
		SnapshotID:  sid,
		GID:         7,
		BaseDir:     basedirsStoreTestBaseDir,
		Age:         uint8(db.DGUTAgeAll),
		UIDs:        []uint32{17},
		UsageSize:   50,
		QuotaSize:   100,
		UsageInodes: 5,
		QuotaInodes: 10,
		Mtime:       updatedAt,
		DateNoSpace: time.Unix(0, 0).UTC(),
		DateNoFiles: time.Unix(0, 0).UTC(),
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func writeSummariseSpoolLoaderFileOnlySpool(
	spoolDir string,
	updatedAt time.Time,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)
	So(set.WriteFile(chspool.FileRow{
		MountPath:  testMountPath,
		SnapshotID: sid,
		ParentDir:  testMountPath,
		Name:       "file.txt",
		ATime:      updatedAt,
		MTime:      updatedAt,
		CTime:      updatedAt,
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

func writeSummariseSpoolLoaderSchema2BatchSpool(
	spoolDir string,
	updatedAt time.Time,
	factRows int,
	childRows int,
) *chspool.Manifest {
	set, err := chspool.CreateSet(spoolDir)
	So(err, ShouldBeNil)

	sid := SnapshotID(testMountPath, updatedAt)

	var writeErr error
	for range factRows {
		writeErr = errors.Join(writeErr, set.WriteDirFact(chspool.DirFactRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			Dir:        testMountPath,
			UpdatedAt:  updatedAt,
		}))
	}

	for range childRows {
		writeErr = errors.Join(writeErr, set.WriteChild(chspool.ChildRow{
			MountPath:  testMountPath,
			SnapshotID: sid,
			ParentDir:  testMountPath,
			Child:      "spool-child",
		}))
	}

	So(writeErr, ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	manifest := &chspool.Manifest{
		Version:      chspool.Version,
		Format:       chspool.Format,
		State:        chspool.Complete,
		MountPath:    testMountPath,
		SnapshotID:   sid,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339Nano),
		SchemaMarker: summariseSpoolLoaderSchemaMarker,
		Tables:       set.TableManifests(),
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	So(chspool.WriteManifestAtomic(spoolDir, manifest), ShouldBeNil)

	return manifest
}

type summariseSpoolHistoryDeleteDeadlineConn struct {
	bootstrapTestConn

	normalWindow time.Duration
	err          error

	deleteCalls          int
	cleanupDeadlineCalls int
}

func (c *summariseSpoolHistoryDeleteDeadlineConn) Exec(
	ctx context.Context,
	query string,
	_ ...any,
) error {
	if !strings.HasPrefix(query, "ALTER TABLE wrstat_basedirs_history DELETE") {
		return errBootstrapTestUnexpectedCall
	}

	c.deleteCalls++

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= c.normalWindow {
		return context.DeadlineExceeded
	}

	c.cleanupDeadlineCalls++

	return c.err
}
