/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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

package basedirs

import (
	"bytes"
)

// DBInfo holds summary information about the database file produced by
// NewCreator().CreateDatabase().
type DBInfo struct {
	GroupDirCombos    int
	GroupMountCombos  int
	GroupHistories    int
	GroupSubDirCombos int
	GroupSubDirs      int
	UserDirCombos     int
	UserSubDirCombos  int
	UserSubDirs       int
}

// Info returns summary information by scanning the provided store.
func Info(store Store) (*DBInfo, error) {
	var info *DBInfo
	err := store.View(func(r Reader) error {
		var e error
		info, e = r.Info()
		return e
	})

	return info, err
}

// CheckAgeOfKeyIsAll returns true if the key represents the db.DGUTAgeAll key.
// Kept exported for tests and admin utilities that need to interpret raw keys.
func CheckAgeOfKeyIsAll(key []byte) bool {
	return bytes.Count(key, bucketKeySeparatorByteSlice) == 1
}
