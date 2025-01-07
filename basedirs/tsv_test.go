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
	Convey("", t, func() {
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
			Prefix: split.SplitPath("/some/partial/thing/"),
			Splits: 6,
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

	Convey("", t, func() {
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
				true,
			},
			{
				paths.ToDirectoryPath("/ab/cd/ef/g/h/i/"),
				false,
			},
			{
				paths.ToDirectoryPath("/some/partial/thing/"),
				true,
			},
			{
				paths.ToDirectoryPath("/some/partial/thingCat/"),
				false,
			},
		} {
			out := c.PathShouldOutput(test.Input)
			So(out, ShouldEqual, test.Output)
		}
	})
}
