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

package discovery

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testRequired = "testDIR"

func TestFindDatasetDirs(t *testing.T) {
	Convey("You can find new dataset dirs", t, func() {
		tmp := t.TempDir()

		createFakeDataset(t, tmp, "123_abc")
		b := createFakeDataset(t, tmp, "124_abc")
		c := createFakeDataset(t, tmp, "123_def")
		createFakeDataset(t, tmp, ".124_def")

		found, toDelete, err := findDBDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{c, b})
		So(toDelete, ShouldResemble, []string{"123_abc"})
	})

	Convey("Deletion list is correct when lexicographic order is misleading", t, func() {
		tmp := t.TempDir()

		// os.ReadDir sorts by name; "10_" comes before "9_" lexicographically.
		createFakeDataset(t, tmp, "10_abc")
		older := createFakeDataset(t, tmp, "9_abc")

		found, toDelete, err := findDBDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{filepath.Join(tmp, "10_abc")})
		So(toDelete, ShouldResemble, []string{filepath.Base(older)})
	})
}

func createFakeDataset(t *testing.T, base, name string) string {
	t.Helper()

	p := filepath.Join(base, name)

	So(os.Mkdir(p, 0o700), ShouldBeNil)
	So(os.Mkdir(filepath.Join(p, testRequired), 0o700), ShouldBeNil)

	return p
}
