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

package summariseutil

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseMountpointsFromFile(t *testing.T) {
	Convey("ParseMountpointsFromFile parses quoted mountpoints", t, func() {
		tmp := t.TempDir()
		p := filepath.Join(tmp, "mounts.txt")

		data := []byte("\"/nfs/\"\n\"/lustre/\"\n\n")
		So(os.WriteFile(p, data, 0o600), ShouldBeNil)

		mps, err := ParseMountpointsFromFile(p)
		So(err, ShouldBeNil)
		So(mps, ShouldResemble, []string{"/nfs/", "/lustre/"})
	})

	Convey("ParseMountpointsFromFile returns nil for empty path", t, func() {
		mps, err := ParseMountpointsFromFile("")
		So(err, ShouldBeNil)
		So(mps, ShouldBeNil)
	})
}
