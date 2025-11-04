/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

const idLen = 4 + 1

// CleanInvalidDBHistory removes irrelevant paths from the history bucket,
// leaving only those with the specified path prefix.
func CleanInvalidDBHistory(store Store, prefix string) error { //nolint:gocognit
	var toDelete [][2]interface { /* gid,uint32; path,string */
	}

	if err := store.View(func(r Reader) error {
		return r.ForEachGroupHistory(func(gid uint32, path string, _ []History) error {
			if len(path) > 0 && pathHasPrefix(path, prefix) {
				return nil
			}
			toDelete = append(toDelete, [2]interface{}{gid, path})
			return nil
		})
	}); err != nil {
		return err
	}

	return store.Update(func(w Writer) error {
		for _, pair := range toDelete {
			if err := w.DeleteHistory(pair[0].(uint32), pair[1].(string)); err != nil {
				return err
			}
		}
		return nil
	})
}

// FindInvalidHistoryKeys returns a list of the keys from the history bucket
// that do not contain the specified prefix.
func FindInvalidHistoryKeys(store Store, prefix string) ([][]byte, error) {
	// For compatibility, we still return [][]byte, but construct keys from domain fields.
	var toRemove [][]byte
	err := store.View(func(r Reader) error {
		return r.ForEachGroupHistory(func(gid uint32, path string, _ []History) error {
			if pathHasPrefix(path, prefix) {
				return nil
			}
			// Recreate raw key shape for callers expecting it
			toRemove = append(toRemove, recreateHistoryKey(gid, path))
			return nil
		})
	})

	return toRemove, err
}

// helpers maintain original behavior without exposing raw access in interfaces.
func pathHasPrefix(path, prefix string) bool {
	return len(prefix) == 0 || (len(path) >= len(prefix) && path[:len(prefix)] == prefix)
}

func recreateHistoryKey(gid uint32, path string) []byte {
	// key: gid(4 bytes LE) + '-' + path
	b := make([]byte, 4, 5+len(path))
	b[0] = byte(gid)
	b[1] = byte(gid >> 8)
	b[2] = byte(gid >> 16)
	b[3] = byte(gid >> 24)
	b = append(b, '-')
	b = append(b, path...)
	return b
}
