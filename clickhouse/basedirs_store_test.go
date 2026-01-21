package clickhouse

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	basedirsStoreTestCountGroupUsageQuery = "SELECT count() FROM wrstat_basedirs_group_usage " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?)"
	basedirsStoreTestCountHistoryQuery     = "SELECT count() FROM wrstat_basedirs_history WHERE mount_path = ? AND gid = ?"
	basedirsStoreTestSelectQuotaDatesQuery = "SELECT date_no_space, date_no_files FROM wrstat_basedirs_group_usage " +
		"WHERE mount_path = ? AND snapshot_id = toUUID(?) AND gid = ? AND age = ? LIMIT 1"
)

func TestClickHouseBaseDirsStore(t *testing.T) {
	Convey("BaseDirsStore writes basedirs snapshots and maintains history", t, func() {
		th := newClickHouseTestHarness(t)
		cfg := th.newConfig()
		cfg.QueryTimeout = 2 * time.Second

		updatedAt := time.Unix(1710000000, 0).UTC()
		sid := snapshotID(testMountPath, updatedAt).String()

		store, err := NewBaseDirsStore(cfg)
		So(err, ShouldBeNil)
		So(store, ShouldNotBeNil)

		store.SetMountPath(testMountPath)
		store.SetUpdatedAt(updatedAt)
		So(store.Reset(), ShouldBeNil)

		// History append rule (only strictly increasing dates).
		gid := uint32(7)
		hKey := basedirs.HistoryKey{GID: gid, MountPath: testMountPath}

		h1 := basedirs.History{Date: time.Unix(1709000000, 0).UTC(), UsageSize: 50, QuotaSize: 200, UsageInodes: 5, QuotaInodes: 20}
		h2 := basedirs.History{Date: time.Unix(1709100000, 0).UTC(), UsageSize: 100, QuotaSize: 200, UsageInodes: 10, QuotaInodes: 20}
		hOld := basedirs.History{Date: time.Unix(1708000000, 0).UTC(), UsageSize: 1, QuotaSize: 200, UsageInodes: 1, QuotaInodes: 20}

		So(store.AppendGroupHistory(hKey, h1), ShouldBeNil)
		So(store.AppendGroupHistory(hKey, h2), ShouldBeNil)
		So(store.AppendGroupHistory(hKey, hOld), ShouldBeNil)

		// Usage rows; age=all must be buffered and inserted in Finalise with quota dates.
		uAll := &basedirs.Usage{
			GID:         gid,
			BaseDir:     "/base/",
			UIDs:        []uint32{1, 2},
			UsageSize:   100,
			QuotaSize:   200,
			UsageInodes: 10,
			QuotaInodes: 20,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeAll,
		}
		uA1M := &basedirs.Usage{
			GID:         gid,
			BaseDir:     "/base/",
			UIDs:        []uint32{1},
			UsageSize:   10,
			QuotaSize:   200,
			UsageInodes: 1,
			QuotaInodes: 20,
			Mtime:       updatedAt,
			Age:         db.DGUTAgeA1M,
		}

		So(store.PutGroupUsage(uAll), ShouldBeNil)
		So(store.PutGroupUsage(uA1M), ShouldBeNil)
		So(store.Finalise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		conn := th.openConn(cfg.DSN)
		Reset(func() { So(conn.Close(), ShouldBeNil) })

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		So(countRows(ctx, conn, basedirsStoreTestCountHistoryQuery, testMountPath, gid), ShouldEqual, 2)
		So(countRows(ctx, conn, basedirsStoreTestCountGroupUsageQuery, testMountPath, sid), ShouldEqual, 2)

		rows, err := conn.Query(ctx, basedirsStoreTestSelectQuotaDatesQuery, testMountPath, sid, gid, uint8(db.DGUTAgeAll))
		So(err, ShouldBeNil)
		defer func() { _ = rows.Close() }()
		So(rows.Next(), ShouldBeTrue)

		var gotNoSpace, gotNoFiles time.Time
		So(rows.Scan(&gotNoSpace, &gotNoFiles), ShouldBeNil)

		expNoSpace, expNoFiles := basedirs.DateQuotaFull([]basedirs.History{h1, h2})
		So(gotNoSpace.Unix(), ShouldEqual, expNoSpace.Unix())
		So(gotNoFiles.Unix(), ShouldEqual, expNoFiles.Unix())
	})
}
