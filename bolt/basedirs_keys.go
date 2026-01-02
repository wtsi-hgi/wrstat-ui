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
