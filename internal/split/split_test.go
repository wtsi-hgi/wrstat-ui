package split

import (
	"reflect"
	"testing"
)

func TestSplitPath(t *testing.T) {
	for n, test := range [...]struct {
		Input  string
		Output []string
	}{
		{
			"/",
			[]string{},
		},
		{
			"/a",
			[]string{"a"},
		},
		{
			"/a/",
			[]string{"a/"},
		},
		{
			"a/",
			[]string{"a/"},
		},
		{
			"/a/bc/def/ghij/klmno/",
			[]string{"a/", "bc/", "def/", "ghij/", "klmno/"},
		},
	} {
		if out := SplitPath(test.Input); !reflect.DeepEqual(out, test.Output) {
			t.Errorf("test %d: expecting output %v, got %v", n+1, test.Output, out)
		}
	}
}
