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

func SplitPath(path string) []string { //nolint:revive
	path = strings.TrimPrefix(path, "/")
	parts := make([]string, 0, strings.Count(path, "/"))

	for len(path) > 0 {
		pos := strings.IndexByte(path, '/')

		if pos < 0 {
			pos = len(path) - 1
		}

		parts = append(parts, path[:pos+1])
		path = path[pos+1:]
	}

	return parts
}
