/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package basedirs

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
)

func TestQuotas(t *testing.T) {
	csvPath := internaldata.CreateQuotasCSV(t, internaldata.ExampleQuotaCSV)

	Convey("Given a valid quotas csv file you can parse it", t, func() {
		quota, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)
		So(quota, ShouldNotBeNil)

		Convey("Then get the quota of a gid and disk", func() {
			s, i := quota.Get(1, "/disk/1/sub")
			So(s, ShouldEqual, 10)
			So(i, ShouldEqual, 20)

			s, i = quota.Get(1, "/disk/2/sub")
			So(s, ShouldEqual, 11)
			So(i, ShouldEqual, 21)

			s, i = quota.Get(2, "/disk/1/sub")
			So(s, ShouldEqual, 12)
			So(i, ShouldEqual, 22)
		})

		Convey("Invalid gids and disks return 0 quota", func() {
			s, i := quota.Get(3, "/disk/1/sub")
			So(s, ShouldEqual, 0)
			So(i, ShouldEqual, 0)

			s, i = quota.Get(2, "/disk/2/sub")
			So(s, ShouldEqual, 0)
			So(i, ShouldEqual, 0)
		})
	})

	Convey("Invalid quotas csv files can't be parsed", t, func() {
		csvPath = internaldata.CreateQuotasCSV(t, `1,/disk/1`)
		_, err := ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, errBadQuotaCSVFile)

		csvPath = internaldata.CreateQuotasCSV(t, `g,/disk/1,10,20`)
		_, err = ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)

		csvPath = internaldata.CreateQuotasCSV(t, `1,/disk/1,s,20`)
		_, err = ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)

		csvPath = internaldata.CreateQuotasCSV(t, `1,/disk/1,10,t`)
		_, err = ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)

		_, err = ParseQuotas("/foo")
		So(err, ShouldNotBeNil)
	})
}

func TestOwners(t *testing.T) {
	Convey("Given an owners tsv you can parse it", t, func() {
		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		owners, err := parseOwners(ownersPath)
		So(err, ShouldBeNil)
		So(owners, ShouldResemble, map[uint32]string{
			1: "Alan",
			2: "Barbara",
			4: "Dellilah",
		})
	})
}
