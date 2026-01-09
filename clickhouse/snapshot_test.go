/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSnapshotIDDerivation(t *testing.T) {
	Convey("snapshotID is deterministic for (mount_path, updated_at)", t, func() {
		mountPath := "/mnt/foo/"
		updatedAtLocal := time.Date(2026, 1, 9, 12, 34, 56, 789123456, time.FixedZone("offset", 3600))
		updatedAtUTC := updatedAtLocal.UTC()

		sidLocal := snapshotID(mountPath, updatedAtLocal)
		sidUTC := snapshotID(mountPath, updatedAtUTC)

		So(sidLocal, ShouldResemble, sidUTC)
	})

	Convey("snapshotID changes with time and mount_path", t, func() {
		updatedAt := time.Date(2026, 1, 9, 12, 34, 56, 789123456, time.UTC)

		sidA := snapshotID("/mnt/a/", updatedAt)
		sidB := snapshotID("/mnt/b/", updatedAt)
		So(sidA, ShouldNotResemble, sidB)

		sidT1 := snapshotID("/mnt/a/", updatedAt)
		sidT2 := snapshotID("/mnt/a/", updatedAt.Add(time.Nanosecond))
		So(sidT1, ShouldNotResemble, sidT2)
	})
}
