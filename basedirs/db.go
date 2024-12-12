/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package basedirs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

const (
	dbOpenMode             = 0600
	bucketKeySeparator     = "-"
	bucketKeySeparatorByte = '-'
	gBytes                 = 1024 * 1024 * 1024

	groupUsageBucket      = "groupUsage"
	groupHistoricalBucket = "groupHistorical"
	groupSubDirsBucket    = "groupSubDirs"
	userUsageBucket       = "userUsage"
	userSubDirsBucket     = "userSubDirs"

	sizeOfUint32         = 4
	sizeOfUint16         = 2
	sizeOfKeyWithoutPath = sizeOfUint32 + sizeOfUint16 + 2
)

var bucketKeySeparatorByteSlice = []byte{bucketKeySeparatorByte} //nolint:gochecknoglobals

// Usage holds information summarising usage by a particular GID/UID-BaseDir.
//
// Only one of GID or UID will be set, and Owner will always be blank when UID
// is set. If GID is set, then UIDs will be set, showing which users own files
// in the BaseDir. If UID is set, then GIDs will be set, showing which groups
// own files in the BaseDir.
type Usage struct {
	GID         uint32
	UID         uint32
	GIDs        []uint32
	UIDs        []uint32
	Name        string // the group or user name
	Owner       string
	BaseDir     string
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
	Mtime       time.Time
	// DateNoSpace is an estimate of when there will be no space quota left.
	DateNoSpace time.Time
	// DateNoFiles is an estimate of when there will be no inode quota left.
	DateNoFiles time.Time
	Age         db.DirGUTAge
}

// CreateDatabase creates a database containing usage information for each of
// our groups and users by calculated base directory.
func (b *BaseDirs) Output(users, groups IDAgeDirs) error {
	db, err := openDB(b.dbPath)
	if err != nil {
		return err
	}

	err = db.Update(b.updateDatabase(users, groups))
	if err != nil {
		return err
	}

	err = db.Update(b.storeDateQuotasFill())
	if err != nil {
		return err
	}

	return db.Close()
}

func openDB(dbPath string) (*bolt.DB, error) {
	return bolt.Open(dbPath, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
}

func (b *BaseDirs) updateDatabase(users, groups IDAgeDirs) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		if err := clearUsageBuckets(tx); err != nil {
			return err
		}

		if err := createBucketsIfNotExist(tx); err != nil {
			return err
		}

		if errc := b.calculateUsage(tx, groups, users); errc != nil {
			return errc
		}

		if errc := b.updateHistories(tx, groups); errc != nil {
			return errc
		}

		return b.calculateSubDirUsage(tx, groups, users)
	}
}

func clearUsageBuckets(tx *bolt.Tx) error {
	if err := tx.DeleteBucket([]byte(groupUsageBucket)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
		return err
	}

	if err := tx.DeleteBucket([]byte(userUsageBucket)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
		return err
	}

	return nil
}

func createBucketsIfNotExist(tx *bolt.Tx) error {
	for _, bucket := range [...]string{
		groupUsageBucket, groupHistoricalBucket,
		groupSubDirsBucket, userUsageBucket, userSubDirsBucket,
	} {
		if _, errc := tx.CreateBucketIfNotExists([]byte(bucket)); errc != nil {
			return errc
		}
	}

	return nil
}

func (b *BaseDirs) calculateUsage(tx *bolt.Tx, gidBase IDAgeDirs, uidBase IDAgeDirs) error {
	if errc := b.storeGIDBaseDirs(tx, gidBase); errc != nil {
		return errc
	}

	return b.storeUIDBaseDirs(tx, uidBase)
}

func (b *BaseDirs) storeGIDBaseDirs(tx *bolt.Tx, gidBase IDAgeDirs) error {
	gub := tx.Bucket([]byte(groupUsageBucket))

	for gid, dcss := range gidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				quotaSize, quotaInode := b.quotas.Get(gid, dcs.Dir)

				uwm := &Usage{
					GID:         gid,
					UIDs:        dcs.UIDs,
					BaseDir:     dcs.Dir,
					UsageSize:   dcs.Size,
					QuotaSize:   quotaSize,
					UsageInodes: dcs.Count,
					QuotaInodes: quotaInode,
					Mtime:       dcs.Mtime,
					Age:         dcs.Age,
				}

				if err := gub.Put(keyName(gid, dcs.Dir, uwm.Age), b.encodeToBytes(uwm)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func keyName(id uint32, path string, age db.DirGUTAge) []byte {
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

func (b *BaseDirs) encodeToBytes(data any) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, b.ch)
	enc.MustEncode(data)

	return encoded
}

func (b *BaseDirs) storeUIDBaseDirs(tx *bolt.Tx, uidBase IDAgeDirs) error {
	uub := tx.Bucket([]byte(userUsageBucket))

	for uid, dcss := range uidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				uwm := &Usage{
					UID:         uid,
					GIDs:        dcs.GIDs,
					BaseDir:     dcs.Dir,
					UsageSize:   dcs.Size,
					UsageInodes: dcs.Count,
					Mtime:       dcs.Mtime,
					Age:         dcs.Age,
				}

				if err := uub.Put(keyName(uid, dcs.Dir, uwm.Age), b.encodeToBytes(uwm)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BaseDirs) updateHistories(tx *bolt.Tx, gidBase IDAgeDirs) error {
	ghb := tx.Bucket([]byte(groupHistoricalBucket))

	gidMounts := b.gidsToMountpoints(gidBase)

	for gid, mounts := range gidMounts {
		if err := b.updateGroupHistories(ghb, gid, mounts); err != nil {
			return err
		}
	}

	return nil
}

type gidMountsMap map[uint32]map[string]db.DirSummary

func (b *BaseDirs) gidsToMountpoints(gidBase IDAgeDirs) gidMountsMap {
	gidMounts := make(gidMountsMap, len(gidBase))

	for gid, dcss := range gidBase {
		gidMounts[gid] = b.dcssToMountPoints(dcss)
	}

	return gidMounts
}

func (b *BaseDirs) dcssToMountPoints(dcss *AgeDirs) map[string]db.DirSummary {
	mounts := make(map[string]db.DirSummary)

	for _, dcs := range dcss[0] {
		mp := b.mountPoints.prefixOf(dcs.Dir)
		if mp == "" {
			continue
		}

		ds := mounts[mp]

		ds.Count += dcs.Count
		ds.Size += dcs.Size

		if dcs.Modtime.After(ds.Modtime) {
			ds.Modtime = dcs.Modtime
		}

		mounts[mp] = ds
	}

	return mounts
}

func (b *BaseDirs) updateGroupHistories(ghb *bolt.Bucket, gid uint32,
	mounts map[string]db.DirSummary,
) error {
	for mount, ds := range mounts {
		quotaSize, quotaInode := b.quotas.Get(gid, mount)

		key := keyName(gid, mount, ds.Age)

		existing := ghb.Get(key)

		histories, err := b.updateHistory(ds, quotaSize, quotaInode, ds.Modtime, existing)
		if err != nil {
			return err
		}

		if err = ghb.Put(key, histories); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) updateHistory(ds db.DirSummary, quotaSize, quotaInode uint64,
	historyDate time.Time, existing []byte,
) ([]byte, error) {
	var histories []History

	if existing != nil {
		if err := b.decodeFromBytes(existing, &histories); err != nil {
			return nil, err
		}

		if len(histories) > 0 && !historyDate.After(histories[len(histories)-1].Date) {
			return existing, nil
		}
	}

	histories = append(histories, History{
		Date:        historyDate,
		UsageSize:   ds.Size,
		UsageInodes: ds.Count,
		QuotaSize:   quotaSize,
		QuotaInodes: quotaInode,
	})

	return b.encodeToBytes(histories), nil
}

func (b *BaseDirs) decodeFromBytes(encoded []byte, data any) error {
	return codec.NewDecoderBytes(encoded, b.ch).Decode(data)
}

// UsageBreakdownByType is a map of file type to total size of files in bytes
// with that type.
type UsageBreakdownByType map[db.DirGUTAFileType]uint64

func (u UsageBreakdownByType) String() string {
	var sb strings.Builder

	types := make([]db.DirGUTAFileType, 0, len(u))

	for ft := range u {
		types = append(types, ft)
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i] < types[j]
	})

	for n, ft := range types {
		if n > 0 {
			sb.WriteByte(' ')
		}

		fmt.Fprintf(&sb, "%s: %.2f", ft, float64(u[ft])/gBytes)
	}

	return sb.String()
}

// SubDir contains information about a sub-directory of a base directory.
type SubDir struct {
	SubDir       string
	NumFiles     uint64
	SizeFiles    uint64
	LastModified time.Time
	FileUsage    UsageBreakdownByType
}

func (b *BaseDirs) calculateSubDirUsage(tx *bolt.Tx, gidBase, uidBase IDAgeDirs) error {
	if errc := b.storeGIDSubDirs(tx, gidBase); errc != nil {
		return errc
	}

	return b.storeUIDSubDirs(tx, uidBase)
}

func (b *BaseDirs) storeGIDSubDirs(tx *bolt.Tx, gidBase IDAgeDirs) error {
	bucket := tx.Bucket([]byte(groupSubDirsBucket))

	for gid, dcss := range gidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				if err := b.storeSubDirs(bucket, gid, dcs); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BaseDirs) storeSubDirs(bucket *bolt.Bucket, id uint32, dcs SummaryWithChildren) error {
	return bucket.Put(keyName(id, dcs.Dir, dcs.Age), b.encodeToBytes(dcs.Children))
}

func makeSubDirs(info *db.DirInfo, parentTypes UsageBreakdownByType, //nolint:funlen
	childToTypes map[string]UsageBreakdownByType,
) []*SubDir {
	subDirs := make([]*SubDir, len(info.Children)+1)

	var (
		totalCount uint64
		totalSize  uint64
	)

	for i, child := range info.Children {
		subDirs[i+1] = &SubDir{
			SubDir:       filepath.Base(child.Dir),
			NumFiles:     child.Count,
			SizeFiles:    child.Size,
			LastModified: child.Mtime,
			FileUsage:    childToTypes[child.Dir],
		}

		totalCount += child.Count
		totalSize += child.Size
	}

	if totalCount == info.Current.Count {
		return subDirs[1:]
	}

	subDirs[0] = &SubDir{
		SubDir:       ".",
		NumFiles:     info.Current.Count - totalCount,
		SizeFiles:    info.Current.Size - totalSize,
		LastModified: info.Current.Mtime,
		FileUsage:    parentTypes,
	}

	return subDirs
}

func (b *BaseDirs) storeUIDSubDirs(tx *bolt.Tx, uidBase IDAgeDirs) error {
	bucket := tx.Bucket([]byte(userSubDirsBucket))

	for uid, dcss := range uidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				if err := b.storeSubDirs(bucket, uid, dcs); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// storeDateQuotasFill goes through all our stored group usage and histories and
// stores the date quota will be full on the group Usage.
//
// This needs to be pre-calculated and stored in the db because it's too slow to
// do for all group-basedirs every time the reader gets all of them.
//
// This is done as a separate transaction to updateDatabase() so we have access
// to the latest stored history, without having to have all histories in memory.
func (b *BaseDirs) storeDateQuotasFill() func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(groupUsageBucket))
		hbucket := tx.Bucket([]byte(groupHistoricalBucket))

		return bucket.ForEach(func(_, data []byte) error {
			gu := new(Usage)

			if err := b.decodeFromBytes(data, gu); err != nil {
				return err
			}

			if gu.Age != db.DGUTAgeAll {
				return nil
			}

			h, err := b.history(hbucket, gu.GID, gu.BaseDir)
			if err != nil {
				return err
			}

			sizeExceedDate, inodeExceedDate := DateQuotaFull(h)
			gu.DateNoSpace = sizeExceedDate
			gu.DateNoFiles = inodeExceedDate

			return bucket.Put(keyName(gu.GID, gu.BaseDir, db.DGUTAgeAll), b.encodeToBytes(gu))
		})
	}
}

func (b *BaseDirs) history(bucket *bolt.Bucket, gid uint32, path string) ([]History, error) {
	mp := b.mountPoints.prefixOf(path)
	if mp == "" {
		return nil, ErrInvalidBasePath
	}

	var history []History

	key := historyKey(gid, mp)

	data := bucket.Get(key)
	if data == nil {
		return nil, ErrNoBaseDirHistory
	}

	err := b.decodeFromBytes(data, &history)

	return history, err
}

// MergeDBs merges the basedirs.db database at the given A and B paths and
// creates a new database file at outputPath.
func MergeDBs(pathA, pathB, outputPath string) (err error) { //nolint:funlen
	var dbA, dbB, dbC *bolt.DB

	closeDB := func(db *bolt.DB) {
		errc := db.Close()
		if err == nil {
			err = errc
		}
	}

	dbA, err = openDBRO(pathA)
	if err != nil {
		return err
	}

	defer closeDB(dbA)

	dbB, err = openDBRO(pathB)
	if err != nil {
		return err
	}

	defer closeDB(dbB)

	dbC, err = openDB(outputPath)
	if err != nil {
		return err
	}

	defer closeDB(dbC)

	err = dbC.Update(func(tx *bolt.Tx) error {
		err = transferAllBucketContents(tx, dbA)
		if err != nil {
			return err
		}

		return transferAllBucketContents(tx, dbB)
	})

	return err
}

func transferAllBucketContents(utx *bolt.Tx, source *bolt.DB) error {
	if err := createBucketsIfNotExist(utx); err != nil {
		return err
	}

	return source.View(func(vtx *bolt.Tx) error {
		for _, bucket := range []string{
			groupUsageBucket, groupHistoricalBucket,
			groupSubDirsBucket, userUsageBucket, userSubDirsBucket,
		} {
			err := transferBucketContents(vtx, utx, bucket)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func transferBucketContents(vtx, utx *bolt.Tx, bucketName string) error {
	sourceBucket := vtx.Bucket([]byte(bucketName))
	destBucket := utx.Bucket([]byte(bucketName))

	return sourceBucket.ForEach(func(k, v []byte) error {
		return destBucket.Put(k, v)
	})
}
