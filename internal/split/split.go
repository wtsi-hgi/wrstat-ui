package split

import "strings"

type SplitFn = func(string) int //nolint:revive

// SplitsToSplitFn returns a simple implementation of the function passed to
// Tree.Where.
func SplitsToSplitFn(splits int) SplitFn {
	return func(_ string) int {
		return splits
	}
}

func SplitPath(path string) []string {

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	path = strings.TrimPrefix(path, "/")
	parts := make([]string, strings.Count(path, "/"))

	for len(path) > 0 {
		pos := strings.IndexByte(path, '/')

		parts = append(parts, path[:pos+1])
		path = path[pos+1:]
	}

	return parts
}
