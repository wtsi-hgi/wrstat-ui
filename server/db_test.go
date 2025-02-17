/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

package server

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testDB = "testDIR"

func TestFindDBDirs(t *testing.T) {
	Convey("You can find new DBs for the server to load", t, func() {
		tmp := t.TempDir()

		createFakeDB(t, tmp, "123_abc")
		b := createFakeDB(t, tmp, "124_abc")
		c := createFakeDB(t, tmp, "123_def")
		createFakeDB(t, tmp, ".124_def")

		found, err := FindDBDirs(tmp, testDB)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{c, b})
	})
}

func createFakeDB(t *testing.T, base, name string) string {
	t.Helper()

	p := filepath.Join(base, name)

	So(os.Mkdir(p, 0700), ShouldBeNil)
	So(os.Mkdir(filepath.Join(p, testDB), 0700), ShouldBeNil)

	return p
}
