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

package datasets

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestFindLatestDatasetDirs(t *testing.T) {
	Convey("FindLatestDatasetDirs returns the latest dataset dir per key", t, func() {
		baseDir := t.TempDir()

		mkDataset := func(name string, requiredFiles ...string) {
			dir := filepath.Join(baseDir, name)
			So(os.MkdirAll(dir, 0o755), ShouldBeNil)

			for _, req := range requiredFiles {
				p := filepath.Join(dir, req)
				So(os.WriteFile(p, []byte("x"), 0o600), ShouldBeNil)
			}
		}

		mkDataset("20240101_alpha", "stats.gz")
		mkDataset("20240102_alpha", "stats.gz")
		mkDataset("20240101_beta", "stats.gz")
		mkDataset("9_delta", "stats.gz")
		mkDataset("10_delta", "stats.gz")
		mkDataset("20240103_beta")                // missing required file
		mkDataset(".hidden_zzz", "stats.gz")      // invalid
		mkDataset("nounderscore", "stats.gz")     // invalid
		mkDataset("_missingversion", "stats.gz")  // invalid
		mkDataset("missingkey_", "stats.gz")      // invalid
		mkDataset("20240104_gamma", "other.file") // missing required

		dirs, err := FindLatestDatasetDirs(baseDir, "stats.gz")
		So(err, ShouldBeNil)

		So(dirs, ShouldResemble, []string{
			filepath.Join(baseDir, "10_delta"),
			filepath.Join(baseDir, "20240101_beta"),
			filepath.Join(baseDir, "20240102_alpha"),
		})
	})
}

func TestFindDatasetDirs(t *testing.T) {
	const testRequired = "testDIR"

	createFakeDataset := func(base, name string) string {
		p := filepath.Join(base, name)
		So(os.Mkdir(p, 0o700), ShouldBeNil)
		So(os.Mkdir(filepath.Join(p, testRequired), 0o700), ShouldBeNil)

		return p
	}

	Convey("FindDatasetDirs returns latest dirs and a correct deletion list", t, func() {
		tmp := t.TempDir()

		createFakeDataset(tmp, "123_abc")
		b := createFakeDataset(tmp, "124_abc")
		c := createFakeDataset(tmp, "123_def")
		createFakeDataset(tmp, ".124_def")

		found, toDelete, err := FindDatasetDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{c, b})
		So(toDelete, ShouldResemble, []string{"123_abc"})
	})

	Convey("Deletion list is correct when lexicographic order is misleading", t, func() {
		tmp := t.TempDir()

		// os.ReadDir sorts by name; "10_" comes before "9_" lexicographically.
		createFakeDataset(tmp, "10_abc")
		older := createFakeDataset(tmp, "9_abc")

		found, toDelete, err := FindDatasetDirs(tmp, testRequired)
		So(err, ShouldBeNil)
		So(found, ShouldResemble, []string{filepath.Join(tmp, "10_abc")})
		So(toDelete, ShouldResemble, []string{filepath.Base(older)})
	})
}

func TestIsValidDatasetDirName(t *testing.T) {
	Convey("IsValidDatasetDirName validates dataset dir names", t, func() {
		cases := []struct {
			name string
			ok   bool
		}{
			{"20240101_a", true},
			{"v_key", true},
			{".hidden_a", false},
			{"no_underscore", true},
			{"nounderscore", false},
			{"_missingversion", false},
			{"missingkey_", false},
			{"", false},
		}

		for _, tc := range cases {
			So(IsValidDatasetDirName(tc.name), ShouldEqual, tc.ok)
		}
	})
}

func TestSplitDatasetDirName(t *testing.T) {
	Convey("SplitDatasetDirName splits <version>_<key> dataset dir names", t, func() {
		version, key, ok := SplitDatasetDirName("20240101_alpha")
		So(ok, ShouldBeTrue)
		So(version, ShouldEqual, "20240101")
		So(key, ShouldEqual, "alpha")
	})

	Convey("SplitDatasetDirName rejects invalid dataset dir names", t, func() {
		cases := []string{
			"",
			".hidden_a",
			"nounderscore",
			"_missingversion",
			"missingkey_",
		}

		for _, tc := range cases {
			_, _, ok := SplitDatasetDirName(tc)
			So(ok, ShouldBeFalse)
		}
	})
}
