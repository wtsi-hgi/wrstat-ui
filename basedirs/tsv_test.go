/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
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

package basedirs_test

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

func TestTSV(t *testing.T) {
	Convey("Given a specific config string, the output should match", t, func() {
		for _, test := range [...]struct {
			Input  string
			Output basedirs.Config
			Error  error
		}{
			{
				Input: "/some/path/\t1\t2\n/some/other/path\t3\t4\n/some/much/longer/path/\t999\t911",
				Output: basedirs.Config{
					{
						Prefix:  split.SplitPath("/some/much/longer/path/"),
						Splits:  999,
						MinDirs: 911,
					},
					{
						Prefix:  split.SplitPath("/some/other/path"),
						Splits:  3,
						MinDirs: 4,
					},
					{
						Prefix:  split.SplitPath("/some/path/"),
						Splits:  1,
						MinDirs: 2,
					},
				},
			},
			{
				Input: "# A comment\n/some/path/\t1\t2\n/some/other/path\t3\t4\n/some/much/longer/path/\t999\t911\n",
				Output: basedirs.Config{
					{
						Prefix:  split.SplitPath("/some/much/longer/path/"),
						Splits:  999,
						MinDirs: 911,
					},
					{
						Prefix:  split.SplitPath("/some/other/path"),
						Splits:  3,
						MinDirs: 4,
					},
					{
						Prefix:  split.SplitPath("/some/path/"),
						Splits:  1,
						MinDirs: 2,
					},
				},
			},
			{
				Input: "/some/path\t12\n/some/other/path\t3\t4",
				Error: basedirs.ErrBadTSV,
			},
		} {
			c, err := basedirs.ParseConfig(strings.NewReader(test.Input))
			So(err, ShouldEqual, test.Error)
			So(c, ShouldResemble, test.Output)
		}
	})
}

func TestSplitFn(t *testing.T) {
	c := basedirs.Config{
		{
			Prefix:  split.SplitPath("/some/partial/thing"),
			MinDirs: 3,
			Splits:  6,
		},
		{
			Prefix:  split.SplitPath("/ab/cd/"),
			MinDirs: 3,
			Splits:  3,
		},
		{
			Prefix: split.SplitPath("/ab/ef/"),
			Splits: 2,
		},
	}

	paths := internaltest.NewDirectoryPathCreator()

	Convey("Given a particular parsed config, you should be able to determine "+
		"whether particular path should output", t, func() {
		for _, test := range [...]struct {
			Input  *summary.DirectoryPath
			Output bool
		}{
			{
				paths.ToDirectoryPath("/ab/cd/ef/"),
				true,
			},
			{
				paths.ToDirectoryPath("/ab/cd/ef/g/h/"),
				false,
			},
			{
				paths.ToDirectoryPath("/some/partial/thing/"),
				true,
			},
			{
				paths.ToDirectoryPath("/some/partial/thingCat/"),
				true,
			},
			{
				paths.ToDirectoryPath("/some/partial/think/"),
				false,
			},
		} {
			out := c.PathShouldOutput(test.Input)
			So(out, ShouldEqual, test.Output)
		}
	})
}
