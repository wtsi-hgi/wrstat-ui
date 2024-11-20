package basedirs

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTSV(t *testing.T) {
	Convey("", t, func() {
		for _, test := range [...]struct {
			Input  string
			Output Config
			Error  error
		}{
			{
				Input: "/some/path/\t1\t2\n/some/other/path\t3\t4\n/some/much/longer/path/\t999\t911",
				Output: Config{
					{
						Prefix:  "/some/much/longer/path/",
						Score:   5,
						Splits:  999,
						MinDirs: 911,
					},
					{
						Prefix:  "/some/path/",
						Score:   3,
						Splits:  1,
						MinDirs: 2,
					},
					{
						Prefix:  "/some/other/path",
						Score:   3,
						Splits:  3,
						MinDirs: 4,
					},
				},
			},
			{
				Input: "# A comment\n/some/path/\t1\t2\n/some/other/path\t3\t4\n/some/much/longer/path/\t999\t911\n",
				Output: Config{
					{
						Prefix:  "/some/much/longer/path/",
						Score:   5,
						Splits:  999,
						MinDirs: 911,
					},
					{
						Prefix:  "/some/path/",
						Score:   3,
						Splits:  1,
						MinDirs: 2,
					},
					{
						Prefix:  "/some/other/path",
						Score:   3,
						Splits:  3,
						MinDirs: 4,
					},
				},
			},
			{
				Input: "/some/path\t12\n/some/other/path\t3\t4",
				Error: ErrBadTSV,
			},
		} {
			c, err := ParseConfig(strings.NewReader(test.Input))
			So(err, ShouldEqual, test.Error)
			So(c, ShouldResemble, test.Output)
		}
	})
}

func TestSplitFn(t *testing.T) {
	c := Config{
		{
			Prefix: "/ab/cd/",
			Score:  3,
			Splits: 3,
		},
		{
			Prefix: "/ab/ef/",
			Score:  3,
			Splits: 2,
		},
		{
			Prefix: "/some/partial/thing",
			Score:  3,
			Splits: 6,
		},
	}

	fn := c.splitFn()

	Convey("", t, func() {
		for _, test := range [...]struct {
			Input  string
			Output int
		}{
			{
				"/ab/cd/ef",
				3,
			},
			{
				"/ab/cd/ef/gh",
				3,
			},
			{
				"/some/partial/thing",
				6,
			},
			{
				"/some/partial/thingCat",
				6,
			},
		} {
			out := fn(test.Input)
			So(test.Output, ShouldEqual, out)
		}
	})
}
