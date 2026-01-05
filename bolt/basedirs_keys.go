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
	"bytes"
	"encoding/binary"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	bucketKeySeparatorByte = '-'
	sizeOfUint32           = 4
	sizeOfUint16           = 2
	sizeOfKeyWithoutPath   = sizeOfUint32 + sizeOfUint16 + 2
)

var bucketKeySeparatorByteSlice = []byte{bucketKeySeparatorByte} //nolint:gochecknoglobals

func basedirsKeyName(id uint32, path string, age db.DirGUTAge) []byte {
	length := sizeOfKeyWithoutPath + len(path)
	b := make([]byte, sizeOfUint32, length)
	binary.LittleEndian.PutUint32(b, id)
	b = append(b, bucketKeySeparatorByte)
	b = append(b, path...)

	if age != db.DGUTAgeAll {
		b = append(b, bucketKeySeparatorByte)
		b = b[:length]
		binary.LittleEndian.PutUint16(b[length-sizeOfUint16:], uint16(age))
	}

	return b
}

func basedirsKeyAgeIsAll(key []byte) bool {
	return bytes.Count(key, bucketKeySeparatorByteSlice) == 1
}
