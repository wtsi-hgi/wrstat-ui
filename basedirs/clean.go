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

import (
	"bytes"
)

const idLen = 4 + 1

// CleanInvalidDBHistory removes irrelevant paths from the history bucket,
// leaving only those with the specified path prefix.
func CleanInvalidDBHistory(store Store, prefix string) error { //nolint:gocognit
	prefixB := []byte(prefix)

	var toDelete [][]byte

	if err := store.View(func(r Reader) error {
		return r.ForEachRaw(GroupHistoricalBucket, func(k, _ []byte) error {
			if len(k) > idLen && !bytes.HasPrefix(k[idLen:], prefixB) {
				toDelete = append(toDelete, append([]byte(nil), k...))
			}

			return nil
		})
	}); err != nil {
		return err
	}

	return store.Update(func(w Writer) error {
		for _, k := range toDelete {
			if err := w.DeleteHistoryKey(k); err != nil {
				return err
			}
		}

		return nil
	})
}

// FindInvalidHistoryKeys returns a list of the keys from the history bucket
// that do not contain the specified prefix.
func FindInvalidHistoryKeys(store Store, prefix string) ([][]byte, error) {
	var toRemove [][]byte
	prefixB := []byte(prefix)
	err := store.View(func(tx Reader) error {
		return tx.ForEachRaw(GroupHistoricalBucket, func(k, _ []byte) error {
			if len(k) > idLen && !bytes.HasPrefix(k[idLen:], prefixB) {
				toRemove = append(toRemove, append(make([]byte, 0, len(k)), k...))
			}
			return nil
		})
	})

	return toRemove, err
}
