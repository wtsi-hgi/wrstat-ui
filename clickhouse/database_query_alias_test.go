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
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	. "github.com/smartystreets/goconvey/convey"
)

type childrenQueryAliasConn struct {
	bootstrapTestConn

	query string
}

func (c *childrenQueryAliasConn) Query(
	_ context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	c.query = query

	return newC2Rows(nil), nil
}

func TestClickHouseDatabaseChildrenQueryAliases(t *testing.T) {
	Convey("active mount root fallback query uses declared child aliases", t, func() {
		conn := &childrenQueryAliasConn{}
		database := &clickHouseDatabase{
			cfg:       Config{QueryTimeout: time.Second},
			conn:      conn,
			treeCache: newTreeQueryCache(),
		}
		mounts := []activeMount{{
			mountPath:  testRootMountPath,
			snapshotID: "00000000-0000-0000-0000-000000000001",
		}}

		children, err := database.queryChildrenForActiveMountRoots(mounts)
		So(err, ShouldBeNil)
		So(children, ShouldBeEmpty)

		query := compactSQL(conn.query)
		So(query, ShouldContainSubstring, "(child.mount_path, child.snapshot_id) IN")
		So(query, ShouldNotContainSubstring, "c.mount_path")
		So(query, ShouldNotContainSubstring, "c.snapshot_id")
	})

	Convey("children parent self-join scopes PREWHERE to the child alias", t, func() {
		query, args := scopedBatchQuery(
			childrenForParentsQuery,
			[]string{testRootMountPath},
			testRootMountPath,
			"00000000-0000-0000-0000-000000000001",
		)

		query = compactSQL(query)
		So(query, ShouldContainSubstring, "PREWHERE child.mount_path = ? AND child.snapshot_id = ?")
		So(query, ShouldNotContainSubstring, "PREWHERE mount_path = ? AND snapshot_id = ?")
		So(args, ShouldHaveLength, 3)
	})

	Convey("Info parent count uses path semantics across mount-local dir ids", t, func() {
		query := compactSQL(infoChildrenQuery)
		So(query, ShouldContainSubstring, "uniqExactIf(full_path, child_dir_count > 0) AS num_parents")
		So(query, ShouldNotContainSubstring, "uniqExact(parent_id)")

		snapshotQuery := compactSQL(infoChildrenSnapshotQuery)
		So(snapshotQuery, ShouldContainSubstring, "uniqExactIf(full_path, child_dir_count > 0) AS num_parents")
		So(snapshotQuery, ShouldNotContainSubstring, "uniqExact(parent_id)")
	})
}

func compactSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}
