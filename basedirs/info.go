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

	"github.com/ugorji/go/codec"
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
	info := &DBInfo{}
	ch := new(codec.BincHandle)

	err := store.View(func(r Reader) error {
		info.GroupDirCombos, _ = countFromFullBucketScan(r, GroupUsageBucket, countOnly, ch)
		info.GroupMountCombos, info.GroupHistories = countFromFullBucketScan(r, GroupHistoricalBucket, countHistories, ch)
		info.GroupSubDirCombos, info.GroupSubDirs = countFromFullBucketScan(r, GroupSubDirsBucket, countSubDirs, ch)
		info.UserDirCombos, _ = countFromFullBucketScan(r, UserUsageBucket, countOnly, ch)
		info.UserSubDirCombos, info.UserSubDirs = countFromFullBucketScan(r, UserSubDirsBucket, countSubDirs, ch)
		return nil
	})
	return info, err
}

func countFromFullBucketScan(tx Reader, bucketName string,
	cb func(v []byte, ch codec.Handle) int, ch codec.Handle,
) (int, int) {
	count := 0
	sliceLen := 0

	_ = tx.ForEachRaw(bucketName, func(k, v []byte) error {
		if !CheckAgeOfKeyIsAll(k) {
			return nil
		}
		count++
		sliceLen += cb(v, ch)
		return nil
	})

	return count, sliceLen
}

// CheckAgeOfKeyIsAll returns true if the key represents the db.DGUTAgeAll key.
func CheckAgeOfKeyIsAll(key []byte) bool {
	return bytes.Count(key, bucketKeySeparatorByteSlice) == 1
}

func countOnly(_ []byte, _ codec.Handle) int {
	return 0
}

func countHistories(v []byte, ch codec.Handle) int {
	var histories []History

	codec.NewDecoderBytes(v, ch).MustDecode(&histories)

	return len(histories)
}

func countSubDirs(v []byte, ch codec.Handle) int {
	var subdirs []*SubDir

	codec.NewDecoderBytes(v, ch).MustDecode(&subdirs)

	return len(subdirs)
}
