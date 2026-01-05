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

package bolt

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
)

func TestDGUTADatabase(t *testing.T) {
	Convey("DGUTA database uses persisted updatedAt", t, func() {
		dgutaDir := filepath.Join(t.TempDir(), "dguta.dbs")
		So(os.MkdirAll(dgutaDir, 0o755), ShouldBeNil)

		w, err := NewDGUTAWriter(dgutaDir)
		So(err, ShouldBeNil)

		updatedAt := time.Unix(999, 0)

		w.SetMountPath("/a/")
		w.SetUpdatedAt(updatedAt)

		paths := internaltest.NewDirectoryPathCreator()
		rec := db.RecordDGUTA{
			Dir: paths.ToDirectoryPath("/"),
			GUTAs: db.GUTAs{
				&db.GUTA{GID: 1, UID: 2, FT: db.DGUTAFileTypeBam, Age: db.DGUTAgeAll, Count: 1, Size: 1, Atime: 1, Mtime: 2},
			},
		}
		So(w.Add(rec), ShouldBeNil)
		So(w.Close(), ShouldBeNil)

		database, err := openDGUTADatabase([]string{dgutaDir})
		So(err, ShouldBeNil)

		defer database.Close()

		ds, err := database.DirInfo("/", nil)
		So(err, ShouldBeNil)
		So(ds, ShouldNotBeNil)
		So(ds.Modtime, ShouldResemble, updatedAt)
	})
}
