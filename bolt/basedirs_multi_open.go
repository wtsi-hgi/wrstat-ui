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

package bolt

import (
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

// OpenMultiBaseDirsReader opens multiple basedirs.db files and returns a
// basedirs.Reader that preserves current multi-mount aggregation behaviour.
//
// ownersPath is the owners CSV path used to populate Usage.Owner.
func OpenMultiBaseDirsReader(ownersPath string, dbPaths ...string) (basedirs.Reader, error) {
	if len(dbPaths) == 0 {
		return nil, errors.New("no basedirs db paths provided") //nolint:err113
	}

	readers := make([]basedirs.Reader, 0, len(dbPaths))
	for _, p := range dbPaths {
		r, err := OpenBaseDirsReader(p, ownersPath)
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}

			return nil, err
		}

		readers = append(readers, r)
	}

	return multiBaseDirsReader(readers), nil
}
