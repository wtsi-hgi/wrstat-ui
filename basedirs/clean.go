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

	"go.etcd.io/bbolt"
)

const idLen = 4 + 1

// CleanInvalidDBHistory removes irrelevant paths from the history bucket,
// leaving only those with the specified path prefix.
func CleanInvalidDBHistory(dbPath, prefix string) error { //nolint:gocognit
	db, err := bbolt.Open(dbPath, dbOpenMode, &bbolt.Options{})
	if err != nil {
		return err
	}

	prefixB := []byte(prefix)

	if err := db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte(GroupHistoricalBucket)).Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if len(k) > idLen && !bytes.HasPrefix(k[idLen:], prefixB) {
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return db.Close()
}

// FindInvalidHistoryKeys returns a list of the keys from the history bucket
// that do not contain the specified prefix.
func FindInvalidHistoryKeys(dbPath, prefix string) ([][]byte, error) {
	db, err := bbolt.Open(dbPath, dbOpenMode, &bbolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}

	defer db.Close()

	var toRemove [][]byte

	prefixB := []byte(prefix)

	if err := db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(GroupHistoricalBucket)).ForEach(func(k, _ []byte) error {
			if len(k) > idLen && !bytes.HasPrefix(k[idLen:], prefixB) {
				toRemove = append(toRemove, append(make([]byte, 0, len(k)), k...))
			}

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return toRemove, nil
}
