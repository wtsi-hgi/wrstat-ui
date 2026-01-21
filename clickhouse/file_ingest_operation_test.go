package clickhouse

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	filesIngestTestCountQuery = "SELECT count() FROM wrstat_files WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	filesIngestTestSelectExts = "SELECT name, ext FROM wrstat_files WHERE mount_path = ? AND snapshot_id = toUUID(?) ORDER BY name ASC"
)

func TestClickHouseFileIngestOperation(t *testing.T) {
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
		So(op2.Add(&summary.FileInfo{Path: root, Name: []byte("b.bin"), Size: 1, ApparentSize: 1, UID: 1, GID: 2, ATime: 1, MTime: 1, CTime: 1, Inode: 102, Nlink: 1, EntryType: stats.FileType}), ShouldBeNil)
		So(closer2.Close(), ShouldBeNil)

		So(countRows(ctx, conn, filesIngestTestCountQuery, testMountPath, sid), ShouldEqual, 1)
	})
}
