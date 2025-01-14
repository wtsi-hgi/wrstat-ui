/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
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

package split

import "strings"

// SplitFn is a function that returns the number of split for a given path.
type SplitFn = func(string) int //nolint:revive

// SplitsToSplitFn returns a simple implementation of the function passed to
// Tree.Where.
func SplitsToSplitFn(splits int) SplitFn {
	return func(_ string) int {
		return splits
	}
}

// SplitPath splits a path at the '/', but keeping the slash at the end of each
// part.
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
