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

package chperf

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

const (
	clickHouseAPIFieldTestDir      = "/mnt/data/"
	clickHouseAPIFieldTestExt      = "cram"
	clickHouseAPIFieldTestFilePath = clickHouseAPIFieldTestDir + "file." + clickHouseAPIFieldTestExt
)

type recordingClickHouseFileClient struct {
	listOpts      []clickhouse.ListOptions
	statOpts      []clickhouse.StatOptions
	countGlobOpts []clickhouse.FindOptions
	findOpts      []clickhouse.FindOptions
}

func (c *recordingClickHouseFileClient) ListDir(
	_ context.Context,
	dir string,
	opts clickhouse.ListOptions,
) ([]clickhouse.FileRow, error) {
	c.listOpts = append(c.listOpts, opts)

	return []clickhouse.FileRow{{
		Path:      dir + "file." + clickHouseAPIFieldTestExt,
		Ext:       clickHouseAPIFieldTestExt,
		EntryType: 'f',
	}}, nil
}

func (c *recordingClickHouseFileClient) StatPath(
	_ context.Context,
	path string,
	opts clickhouse.StatOptions,
) (*clickhouse.FileRow, error) {
	c.statOpts = append(c.statOpts, opts)

	return &clickhouse.FileRow{Path: path}, nil
}

func (*recordingClickHouseFileClient) PermissionAnyInDir(
	context.Context,
	string,
	uint32,
	[]uint32,
) (bool, error) {
	return true, nil
}

func (c *recordingClickHouseFileClient) CountByGlob(
	_ context.Context,
	_ []string,
	_ []string,
	opts clickhouse.FindOptions,
) (int, error) {
	c.countGlobOpts = append(c.countGlobOpts, opts)

	return 1, nil
}

func (c *recordingClickHouseFileClient) FindByGlob(
	_ context.Context,
	_ []string,
	_ []string,
	opts clickhouse.FindOptions,
) ([]clickhouse.FileRow, error) {
	c.findOpts = append(c.findOpts, opts)

	return []clickhouse.FileRow{{Path: clickHouseAPIFieldTestFilePath}}, nil
}

func (*recordingClickHouseFileClient) Close() error {
	return nil
}

func TestClickHouseQueryClientUsesNarrowFileFields(t *testing.T) {
	Convey("ClickHouse perf query client requests only fields needed by file-level ops", t, func() {
		ctx := context.Background()
		recorder := &recordingClickHouseFileClient{}
		client := clickHouseQueryClient{client: recorder}

		rows, err := client.ListDir(ctx, clickHouseAPIFieldTestDir, 25)
		So(err, ShouldBeNil)
		So(rows, ShouldResemble, []QueryRow{{
			Path:      clickHouseAPIFieldTestFilePath,
			Ext:       clickHouseAPIFieldTestExt,
			EntryType: 'f',
		}})
		So(recorder.listOpts, ShouldHaveLength, 1)
		So(recorder.listOpts[0].Limit, ShouldEqual, int64(25))
		So(recorder.listOpts[0].Fields, ShouldResemble, []string{
			clickHouseFileFieldPath,
			clickHouseFileFieldExt,
			clickHouseFileFieldEntryType,
		})

		So(client.StatPath(ctx, clickHouseAPIFieldTestFilePath), ShouldBeNil)
		So(recorder.statOpts, ShouldHaveLength, 1)
		So(recorder.statOpts[0].Fields, ShouldResemble, []string{clickHouseFileFieldPath})

		count, err := client.FindByGlob(
			ctx,
			[]string{clickHouseAPIFieldTestDir},
			[]string{"**/*." + clickHouseAPIFieldTestExt},
			true,
			123,
			[]uint32{456},
		)
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 1)
		So(recorder.findOpts, ShouldBeEmpty)
		So(recorder.countGlobOpts, ShouldHaveLength, 1)
		So(recorder.countGlobOpts[0].Fields, ShouldBeEmpty)
		So(recorder.countGlobOpts[0].RequireOwner, ShouldBeTrue)
		So(recorder.countGlobOpts[0].UID, ShouldEqual, uint32(123))
		So(recorder.countGlobOpts[0].GIDs, ShouldResemble, []uint32{456})
	})
}
