package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const fileIngestTestPhasePartitionDropReset = "partition_drop_reset"

const (
	filesIngestTestCountQuery = "SELECT count() FROM wrstat_files" +
		" WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	filesIngestTestSelectExts = "SELECT name, ext FROM wrstat_files" +
		" WHERE mount_path = ? AND snapshot_id = toUUID(?) ORDER BY name ASC"
)

var (
	errFileIngestLazyPrepareSend = errors.New(
		"dial tcp: lookup clickhouse-host: i/o timeout",
	)
	errFileIngestLazyPreparePrepare        = errors.New("prepare files batch timeout")
	errFileIngestLazyPrepareUnexpectedCall = errors.New("unexpected file ingest lazy prepare test call")
)

type fileIngestPhaseRecorder interface {
	SetImportPhaseRecorder(recorder func(phase string, duration time.Duration))
}

func TestClickHouseFileIngestOperation(t *testing.T) {
	Convey("File ingest operation records initial partition drop/reset time", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)

		gen, closer, err := NewFileIngestOperation(cfg, testMountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen, ShouldNotBeNil)
		So(closer, ShouldNotBeNil)

		recorder, ok := closer.(fileIngestPhaseRecorder)
		So(ok, ShouldBeTrue)

		phases := make(map[string]time.Duration)

		recorder.SetImportPhaseRecorder(func(phase string, d time.Duration) {
			phases[phase] += d
		})

		op := gen()
		So(op.Add(&summary.FileInfo{
			Path:         root,
			Name:         []byte("a.txt"),
			Size:         123,
			ApparentSize: 456,
			UID:          1,
			GID:          2,
			ATime:        20,
			MTime:        21,
			CTime:        22,
			Inode:        101,
			Nlink:        1,
			EntryType:    stats.FileType,
		}), ShouldBeNil)

		So(closer.Close(), ShouldBeNil)
		So(phases[fileIngestTestPhasePartitionDropReset], ShouldBeGreaterThan, time.Duration(0))
	})

	Convey("File ingest operation drops partitions and writes wrstat_files", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)

		gen, closer, err := NewFileIngestOperation(cfg, testMountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen, ShouldNotBeNil)
		So(closer, ShouldNotBeNil)

		op := gen()
		So(op, ShouldNotBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         root,
			Name:         []byte("subdir/"),
			Size:         0,
			ApparentSize: 0,
			UID:          1,
			GID:          2,
			ATime:        10,
			MTime:        11,
			CTime:        12,
			Inode:        100,
			Nlink:        1,
			EntryType:    stats.DirType,
		}), ShouldBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         root,
			Name:         []byte("a.txt"),
			Size:         123,
			ApparentSize: 456,
			UID:          1,
			GID:          2,
			ATime:        20,
			MTime:        21,
			CTime:        22,
			Inode:        101,
			Nlink:        1,
			EntryType:    stats.FileType,
		}), ShouldBeNil)

		So(closer.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, filesIngestTestCountQuery, testMountPath, sid), ShouldEqual, 2)

		rows, err := conn.Query(ctx, filesIngestTestSelectExts, testMountPath, sid)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var name1, ext1 string
		So(rows.Scan(&name1, &ext1), ShouldBeNil)
		So(name1, ShouldEqual, "a.txt")
		So(ext1, ShouldEqual, "txt")

		So(rows.Next(), ShouldBeTrue)

		var name2, ext2 string
		So(rows.Scan(&name2, &ext2), ShouldBeNil)
		So(name2, ShouldEqual, "subdir/")
		So(ext2, ShouldEqual, "")

		// Rerun: constructor must drop the snapshot partition to make reruns safe.
		gen2, closer2, err := NewFileIngestOperation(cfg, testMountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen2, ShouldNotBeNil)
		op2 := gen2()
		So(op2.Add(&summary.FileInfo{
			Path: root, Name: []byte("b.bin"), Size: 1, ApparentSize: 1,
			UID: 1, GID: 2, ATime: 1, MTime: 1, CTime: 1,
			Inode: 102, Nlink: 1, EntryType: stats.FileType,
		}), ShouldBeNil)
		So(closer2.Close(), ShouldBeNil)

		So(countRows(ctx, conn, filesIngestTestCountQuery, testMountPath, sid), ShouldEqual, 1)
	})

	Convey("File ingest operation canonicalises root mount paths for ListDir", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second
		cfg.MountPoints = []string{"/"}

		const mountPath = "/"

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(mountPath, updatedAt)

		paths := internaltest.NewDirectoryPathCreator()

		gen, closer, err := NewFileIngestOperation(cfg, mountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen, ShouldNotBeNil)
		So(closer, ShouldNotBeNil)

		op := gen()
		So(op, ShouldNotBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         paths.ToDirectoryPath("//"),
			Name:         []byte("/"),
			Size:         0,
			ApparentSize: 0,
			UID:          1,
			GID:          2,
			ATime:        10,
			MTime:        11,
			CTime:        12,
			Inode:        100,
			Nlink:        1,
			EntryType:    stats.DirType,
		}), ShouldBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         paths.ToDirectoryPath("//boot/"),
			Name:         []byte("boot/"),
			Size:         0,
			ApparentSize: 0,
			UID:          1,
			GID:          2,
			ATime:        10,
			MTime:        11,
			CTime:        12,
			Inode:        101,
			Nlink:        1,
			EntryType:    stats.DirType,
		}), ShouldBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         paths.ToDirectoryPath("//"),
			Name:         []byte("bin"),
			Size:         7,
			ApparentSize: 7,
			UID:          1,
			GID:          2,
			ATime:        20,
			MTime:        21,
			CTime:        22,
			Inode:        102,
			Nlink:        1,
			EntryType:    stats.SymlinkType,
		}), ShouldBeNil)

		So(op.Add(&summary.FileInfo{
			Path:         paths.ToDirectoryPath("//boot/"),
			Name:         []byte("vmlinuz"),
			Size:         123,
			ApparentSize: 456,
			UID:          1,
			GID:          2,
			ATime:        30,
			MTime:        31,
			CTime:        32,
			Inode:        103,
			Nlink:        1,
			EntryType:    stats.FileType,
		}), ShouldBeNil)

		So(closer.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, mountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		client, err := NewClient(cfg)
		So(err, ShouldBeNil)
		Reset(func() { So(client.Close(), ShouldBeNil) })

		rows, err := client.ListDir(ctx, "/", ListOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 2)
		So(rows[0].ParentDir, ShouldEqual, "/")
		So(rows[0].Name, ShouldEqual, "bin")
		So(rows[0].Path, ShouldEqual, "/bin")
		So(rows[1].ParentDir, ShouldEqual, "/")
		So(rows[1].Name, ShouldEqual, "boot/")
		So(rows[1].Path, ShouldEqual, "/boot/")

		rows, err = client.ListDir(ctx, "/boot/", ListOptions{})
		So(err, ShouldBeNil)
		So(rows, ShouldHaveLength, 1)
		So(rows[0].ParentDir, ShouldEqual, "/boot/")
		So(rows[0].Name, ShouldEqual, "vmlinuz")
		So(rows[0].Path, ShouldEqual, "/boot/vmlinuz")
	})

	Convey("File ingest operation prepares each files batch only when sending buffered rows", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)
		conn := &fileIngestLazyPrepareConn{}
		w := &fileIngestWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
			batchSize: 1,
		}
		op := &fileIngestOperation{w: w}

		So(conn.prepareCalls, ShouldEqual, 0)
		So(op.Add(fileIngestTestInfo(root, "a.txt", 101)), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].sent, ShouldBeTrue)
		So(w.batch, ShouldBeNil)

		So(op.Add(fileIngestTestInfo(root, "b.txt", 102)), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 2)
		So(conn.batches, ShouldHaveLength, 2)
		So(conn.batches[1].sent, ShouldBeTrue)
		So(w.batch, ShouldBeNil)
		So(w.buf.rows(), ShouldEqual, 0)
	})

	Convey("File ingest operation does not retry an ambiguous files batch send on Close", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)
		conn := &fileIngestLazyPrepareConn{sendErr: errFileIngestLazyPrepareSend}
		w := &fileIngestWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
			batchSize: 1,
		}
		op := &fileIngestOperation{w: w}

		err := op.Add(fileIngestTestInfo(root, "a.txt", 101))
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errFileIngestLazyPrepareSend), ShouldBeTrue)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.batches, ShouldHaveLength, 1)
		So(conn.batches[0].sendCalls, ShouldEqual, 1)

		So(w.Close(), ShouldBeNil)
		So(conn.prepareCalls, ShouldEqual, 1)
		So(conn.batches[0].sendCalls, ShouldEqual, 1)
	})

	Convey("File ingest operation rechecks active snapshot if initial batch prepare fails", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt)
		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)
		conn := &fileIngestLazyPrepareConn{
			prepareErr:           errFileIngestLazyPreparePrepare,
			prepareErrOnce:       true,
			activeSnapshotID:     sid.String(),
			activeSnapshotAtCall: 2,
		}
		w := &fileIngestWriter{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  sid,
			batchSize: 1,
		}
		op := &fileIngestOperation{w: w}

		err := op.Add(fileIngestTestInfo(root, "a.txt", 101))
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errFileIngestLazyPreparePrepare), ShouldBeTrue)
		So(w.batch, ShouldBeNil)
		So(w.prepared, ShouldBeFalse)
		So(conn.queryCalls, ShouldEqual, 1)
		So(conn.execCalls, ShouldEqual, 1)
		So(conn.prepareCalls, ShouldEqual, 1)

		err = w.Close()
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotRewrite), ShouldBeTrue)
		So(conn.queryCalls, ShouldEqual, 2)
		So(conn.execCalls, ShouldEqual, 1)
		So(conn.prepareCalls, ShouldEqual, 1)
	})

	Convey("File ingest reset uses cleanup timeout semantics for partition drops", t, func() {
		updatedAt := time.Unix(1710000000, 0).UTC()
		queryTimeout := 100 * time.Millisecond
		conn := &fileIngestPartitionDropDeadlineConn{
			partitionDropDeadlineConn: partitionDropDeadlineConn{normalWindow: queryTimeout},
		}
		w := &fileIngestWriter{
			cfg:       Config{QueryTimeout: queryTimeout},
			conn:      conn,
			mountPath: testMountPath,
			updatedAt: updatedAt,
			snapshot:  snapshotID(testMountPath, updatedAt),
			batchSize: defaultBatchSize,
		}

		ctx, cancel := queryContext(context.Background(), queryTimeout)
		defer cancel()

		So(w.ensureWriteReady(ctx), ShouldBeNil)
		So(conn.partitionDrops(), ShouldEqual, 1)
		So(conn.cleanupDeadlineDrops(), ShouldEqual, 1)
	})

	Convey("File ingest operation refuses to rewrite an active deterministic snapshot", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		paths := internaltest.NewDirectoryPathCreator()
		root := paths.ToDirectoryPath(testMountPath)

		gen, closer, err := NewFileIngestOperation(cfg, testMountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen, ShouldNotBeNil)
		So(closer, ShouldNotBeNil)

		op := gen()
		So(op.Add(&summary.FileInfo{
			Path:         root,
			Name:         []byte("a.txt"),
			Size:         123,
			ApparentSize: 456,
			UID:          1,
			GID:          2,
			ATime:        20,
			MTime:        21,
			CTime:        22,
			Inode:        101,
			Nlink:        1,
			EntryType:    stats.FileType,
		}), ShouldBeNil)
		So(closer.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)

		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(conn.Exec(ctx, testInsertMountStmt, testMountPath, time.Now(), sid, updatedAt), ShouldBeNil)

		gen2, closer2, err := NewFileIngestOperation(cfg, testMountPath, updatedAt)
		So(err, ShouldBeNil)
		So(gen2, ShouldNotBeNil)
		So(closer2, ShouldNotBeNil)

		op2 := gen2()
		So(op2.Add(&summary.FileInfo{
			Path:         root,
			Name:         []byte("b.bin"),
			Size:         1,
			ApparentSize: 1,
			UID:          1,
			GID:          2,
			ATime:        1,
			MTime:        1,
			CTime:        1,
			Inode:        102,
			Nlink:        1,
			EntryType:    stats.FileType,
		}), ShouldBeNil)

		err = closer2.Close()
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errActiveSnapshotRewrite), ShouldBeTrue)

		So(countRows(ctx, conn, filesIngestTestCountQuery, testMountPath, sid), ShouldEqual, 1)

		rows, err := conn.Query(ctx, filesIngestTestSelectExts, testMountPath, sid)
		So(err, ShouldBeNil)

		defer func() { _ = rows.Close() }()

		So(rows.Next(), ShouldBeTrue)

		var name, ext string
		So(rows.Scan(&name, &ext), ShouldBeNil)
		So(name, ShouldEqual, "a.txt")
		So(ext, ShouldEqual, "txt")
		So(rows.Next(), ShouldBeFalse)
	})
}

func fileIngestTestInfo(path *summary.DirectoryPath, name string, inode int64) *summary.FileInfo {
	return &summary.FileInfo{
		Path:         path,
		Name:         []byte(name),
		Size:         123,
		ApparentSize: 456,
		UID:          1,
		GID:          2,
		ATime:        20,
		MTime:        21,
		CTime:        22,
		Inode:        inode,
		Nlink:        1,
		EntryType:    stats.FileType,
	}
}

type fileIngestLazyPrepareRows struct {
	activeSID string
	seen      bool
}

func (r *fileIngestLazyPrepareRows) Next() bool {
	if r.activeSID == "" || r.seen {
		return false
	}

	r.seen = true

	return true
}

func (r *fileIngestLazyPrepareRows) Scan(dest ...any) error {
	if len(dest) < 2 {
		return errFileIngestLazyPrepareUnexpectedCall
	}

	sid, ok := dest[0].(*string)
	if !ok {
		return errFileIngestLazyPrepareUnexpectedCall
	}

	updatedAt, ok := dest[1].(*time.Time)
	if !ok {
		return errFileIngestLazyPrepareUnexpectedCall
	}

	*sid = r.activeSID
	*updatedAt = time.Unix(1710000000, 0).UTC()

	return nil
}

func (r *fileIngestLazyPrepareRows) ScanStruct(any) error {
	return errFileIngestLazyPrepareUnexpectedCall
}

func (r *fileIngestLazyPrepareRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (r *fileIngestLazyPrepareRows) Totals(...any) error {
	return nil
}

func (r *fileIngestLazyPrepareRows) Columns() []string {
	return nil
}

func (r *fileIngestLazyPrepareRows) Close() error {
	return nil
}

func (r *fileIngestLazyPrepareRows) Err() error {
	return nil
}

func (r *fileIngestLazyPrepareRows) HasData() bool {
	return r.activeSID != ""
}

type fileIngestLazyPrepareRow struct{}

func (r fileIngestLazyPrepareRow) Err() error {
	return errFileIngestLazyPrepareUnexpectedCall
}

func (r fileIngestLazyPrepareRow) Scan(...any) error {
	return errFileIngestLazyPrepareUnexpectedCall
}

func (r fileIngestLazyPrepareRow) ScanStruct(any) error {
	return errFileIngestLazyPrepareUnexpectedCall
}

type fileIngestLazyPrepareColumn struct{}

func (c *fileIngestLazyPrepareColumn) Append(any) error {
	return nil
}

func (c *fileIngestLazyPrepareColumn) AppendRow(any) error {
	return nil
}

type fileIngestLazyPrepareBatch struct {
	columns   []fileIngestLazyPrepareColumn
	sendErr   error
	sendCalls int
	sent      bool
	aborted   bool
	closed    bool
}

func newFileIngestLazyPrepareBatch(sendErr error) *fileIngestLazyPrepareBatch {
	return &fileIngestLazyPrepareBatch{
		columns: make([]fileIngestLazyPrepareColumn, 15),
		sendErr: sendErr,
	}
}

func (b *fileIngestLazyPrepareBatch) Abort() error {
	b.aborted = true

	return nil
}

func (b *fileIngestLazyPrepareBatch) Append(...any) error {
	return nil
}

func (b *fileIngestLazyPrepareBatch) AppendStruct(any) error {
	return nil
}

func (b *fileIngestLazyPrepareBatch) Column(i int) driver.BatchColumn {
	return &b.columns[i]
}

func (b *fileIngestLazyPrepareBatch) Flush() error {
	return nil
}

func (b *fileIngestLazyPrepareBatch) Send() error {
	b.sendCalls++
	if b.sendErr != nil {
		return b.sendErr
	}

	b.sent = true

	return nil
}

func (b *fileIngestLazyPrepareBatch) IsSent() bool {
	return b.sent || b.aborted || b.closed
}

func (b *fileIngestLazyPrepareBatch) Rows() int {
	return 0
}

func (b *fileIngestLazyPrepareBatch) Columns() []column.Interface {
	return nil
}

func (b *fileIngestLazyPrepareBatch) Close() error {
	b.closed = true

	return nil
}

type fileIngestLazyPrepareConn struct {
	prepareCalls         int
	queryCalls           int
	execCalls            int
	batches              []*fileIngestLazyPrepareBatch
	sendErr              error
	prepareErr           error
	prepareErrOnce       bool
	activeSnapshotID     string
	activeSnapshotAtCall int
}

func (c *fileIngestLazyPrepareConn) Contributors() []string {
	return nil
}

func (c *fileIngestLazyPrepareConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}

func (c *fileIngestLazyPrepareConn) Select(context.Context, any, string, ...any) error {
	return errFileIngestLazyPrepareUnexpectedCall
}

func (c *fileIngestLazyPrepareConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.queryCalls++

	activeCall := c.activeSnapshotAtCall
	if activeCall == 0 {
		activeCall = 1
	}

	if c.activeSnapshotID != "" && c.queryCalls >= activeCall {
		return &fileIngestLazyPrepareRows{activeSID: c.activeSnapshotID}, nil
	}

	return &fileIngestLazyPrepareRows{}, nil
}

func (c *fileIngestLazyPrepareConn) QueryRow(context.Context, string, ...any) driver.Row {
	return fileIngestLazyPrepareRow{}
}

func (c *fileIngestLazyPrepareConn) PrepareBatch(
	context.Context,
	string,
	...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepareCalls++
	if c.prepareErr != nil {
		err := c.prepareErr
		if c.prepareErrOnce {
			c.prepareErr = nil
		}

		return nil, err
	}

	batch := newFileIngestLazyPrepareBatch(c.sendErr)
	c.batches = append(c.batches, batch)

	return batch, nil
}

func (c *fileIngestLazyPrepareConn) Exec(context.Context, string, ...any) error {
	c.execCalls++

	return nil
}

func (c *fileIngestLazyPrepareConn) AsyncInsert(context.Context, string, bool, ...any) error {
	return errFileIngestLazyPrepareUnexpectedCall
}

func (c *fileIngestLazyPrepareConn) Ping(context.Context) error {
	return nil
}

func (c *fileIngestLazyPrepareConn) Stats() driver.Stats {
	return driver.Stats{}
}

func (c *fileIngestLazyPrepareConn) Close() error {
	return nil
}

type fileIngestPartitionDropDeadlineConn struct {
	partitionDropDeadlineConn
}

func (c *fileIngestPartitionDropDeadlineConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	if query != activeSnapshotQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	return &dgutaWriterCloseContextRows{
		columns: []string{dgutaWriterTestSnapshotIDColumn},
	}, nil
}
