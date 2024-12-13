/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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
)

func TestTree(t *testing.T) {
	Convey("Given a Tree", t, func() {
		// tree, _, err := internaldb.CreateExampleDGUTADBForBasedirs(t)
		// So(err, ShouldBeNil)

		// Convey("You can get all the gids and uids in it", func() {
		// 	gids, uids, err := getAllGIDsandUIDsInTree(tree)
		// 	So(err, ShouldBeNil)

		// 	expectedGIDs := []uint32{1, 2, 3, 77777}
		// 	expectedUIDs := []uint32{101, 102, 103, 88888}

		// 	gid, uid, _, _, err := internaluser.RealGIDAndUID()
		// 	So(err, ShouldBeNil)
		// 	expectedGIDs = append(expectedGIDs, uint32(gid))
		// 	expectedUIDs = append(expectedUIDs, uint32(uid))

		// 	sort.Slice(expectedGIDs, func(i, j int) bool {
		// 		return expectedGIDs[i] < expectedGIDs[j]
		// 	})

		// 	sort.Slice(expectedUIDs, func(i, j int) bool {
		// 		return expectedUIDs[i] < expectedUIDs[j]
		// 	})

		// 	So(err, ShouldBeNil)
		// 	So(gids, ShouldResemble, expectedGIDs)
		// 	So(uids, ShouldResemble, expectedUIDs)
		// })
	})
}
