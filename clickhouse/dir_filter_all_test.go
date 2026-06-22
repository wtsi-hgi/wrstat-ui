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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
)

type dirFilterAllDerivedSpyConn struct {
	bootstrapTestConn

	batch       *dirFilterAllDerivedSpyBatch
	prepared    []string
	derivedArgs []any
	events      []string
}

func (c *dirFilterAllDerivedSpyConn) PrepareBatch(
	_ context.Context,
	query string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepared = append(c.prepared, query)
	if query != insertDirFilterAllQuery {
		return nil, errBootstrapTestUnexpectedCall
	}

	c.batch = &dirFilterAllDerivedSpyBatch{conn: c}

	return c.batch, nil
}

func (c *dirFilterAllDerivedSpyConn) Exec(_ context.Context, query string, args ...any) error {
	if query != derivedChildFilterAllInsertQuery {
		return errBootstrapTestUnexpectedCall
	}

	c.events = append(c.events, "derive "+chspool.TableChildFilterAll)
	c.derivedArgs = append([]any(nil), args...)

	return nil
}

func TestClickHouseDirFilterAllWriter(t *testing.T) {
	Convey("B2 direct full-filter import writes dir rows and derives child rows server-side", t, func() {
		updatedAt := time.Date(2026, 6, 15, 10, 0, 0, 0, time.UTC)
		mount := activeMount{
			mountPath:  testMountPath,
			snapshotID: SnapshotID(testMountPath, updatedAt),
			updatedAt:  updatedAt,
		}
		conn := &dirFilterAllDerivedSpyConn{}
		writer := newFullFilterAllWriter(conn, 100, updatedAt)

		err := writer.appendRecord(
			context.Background(),
			mount,
			dgutaRecordContext{
				canonicalDir: testMountPath + "project/",
				dirID:        3,
				parentID:     2,
				subtreeEnd:   4,
			},
			db.GUTAs{
				&db.GUTA{
					GID:         7,
					UID:         17,
					FT:          db.DGUTAFileTypeBam,
					Age:         db.DGUTAgeAll,
					Count:       3,
					Size:        30,
					Atime:       100,
					Mtime:       200,
					ATimeRanges: [9]uint64{3, 0, 0, 0, 0, 0, 0, 0, 0},
					MTimeRanges: [9]uint64{0, 3, 0, 0, 0, 0, 0, 0, 0},
				},
			},
			1,
			nil,
		)
		So(err, ShouldBeNil)

		So(writer.flush(context.Background()), ShouldBeNil)

		So(conn.prepared, ShouldResemble, []string{insertDirFilterAllQuery})
		So(conn.batch.appends, ShouldEqual, 1)
		So(conn.events, ShouldResemble, []string{
			"send " + chspool.TableDirFilterAll,
			"derive " + chspool.TableChildFilterAll,
		})
		So(conn.derivedArgs, ShouldResemble, []any{testMountPath, mount.snapshotID})
	})
}

type dirFilterAllDerivedSpyBatch struct {
	b1ImportSQLSpyBatch

	conn *dirFilterAllDerivedSpyConn
}

func (b *dirFilterAllDerivedSpyBatch) Send() error {
	b.conn.events = append(b.conn.events, "send "+chspool.TableDirFilterAll)

	return b.b1ImportSQLSpyBatch.Send()
}
