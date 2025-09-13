/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package clickhouse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildBucketPredicate(t *testing.T) {
	// Valid buckets
	valid := []string{"0d", ">1m", ">2m", ">6m", ">1y", ">2y", ">3y", ">5y", ">7y"}
	for _, b := range valid {
		pred, err := buildBucketPredicate("atime", b)
		assert.NoError(t, err, b)
		assert.True(t, strings.Contains(pred, "atime"))
		assert.NotEmpty(t, pred)
	}

	// Empty bucket -> no predicate, no error
	pred, err := buildBucketPredicate("mtime", "")
	assert.NoError(t, err)
	assert.Empty(t, pred)

	// Invalid bucket -> error
	pred, err = buildBucketPredicate("mtime", "invalid")
	assert.ErrorIs(t, err, ErrInvalidBucket)
	assert.Empty(t, pred)
}

func TestBuildGlobSearchQuery(t *testing.T) {
	// Case-sensitive without limit
	q := buildGlobSearchQuery(false, 0)
	assert.Contains(t, q, "path LIKE ?")
	assert.NotContains(t, q, "lowerUTF8")
	assert.NotContains(t, q, "LIMIT")

	// Case-insensitive with limit
	q = buildGlobSearchQuery(true, 25)
	assert.Contains(t, q, "lowerUTF8(path) LIKE lowerUTF8(?)")
	assert.Contains(t, q, "LIMIT 25")
}
