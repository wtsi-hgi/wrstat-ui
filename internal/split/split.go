package split

type SplitFn = func(string) int //nolint:revive

// SplitsToSplitFn returns a simple implementation of the function passed to
// Tree.Where.
func SplitsToSplitFn(splits int) SplitFn {
	return func(_ string) int {
		return splits
	}
}
