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
	"fmt"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	bolt "go.etcd.io/bbolt"
)

const (
	secondsInDay     = time.Hour * 24
	threeDays        = 3 * secondsInDay
	quotaStatusOK    = "OK"
	quotaStatusNotOK = "Not OK"
)

// BaseDirReader is used to read the information stored in a BaseDir database.
type BaseDirReader struct {
	db          *bolt.DB
	ch          codec.Handle
	mountPoints mountPoints
	groupCache  *GroupCache
	userCache   *UserCache
	owners      map[uint32]string
}

// NewReader returns a BaseDirReader that can return the summary information
// stored in a BaseDir database. It takes an owners file (gid,name csv) to
// associate groups with their owners in certain output.
func NewReader(dbPath, ownersPath string) (*BaseDirReader, error) {
	db, err := openDBRO(dbPath)
	if err != nil {
		return nil, err
	}

	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	owners, err := parseOwners(ownersPath)
	if err != nil {
		return nil, err
	}

	return &BaseDirReader{
		db:          db,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
		groupCache:  NewGroupCache(),
		userCache:   NewUserCache(),
		owners:      owners,
	}, nil
}

func openDBRO(dbPath string) (*bolt.DB, error) {
	return bolt.Open(dbPath, dbOpenMode, &bolt.Options{
		ReadOnly: true,
	})
}

func (b *BaseDirReader) Close() error {
	return b.db.Close()
}

// GroupUsage returns the usage for every GID-BaseDir combination in the
// database.
func (b *BaseDirReader) GroupUsage(age summary.DirGUTAge) ([]*Usage, error) {
	return b.usage(groupUsageBucket, age)
}

func (b *BaseDirReader) usage(bucketName string, age summary.DirGUTAge) ([]*Usage, error) {
	var uwms []*Usage

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))

		return bucket.ForEach(func(_, data []byte) error {
			uwm := new(Usage)
			if err := b.decodeFromBytes(data, uwm); err != nil {
				return err
			}

			if uwm.Age != age {
				return nil
			}

			uwm.Owner = b.owners[uwm.GID]

			uwm.Name = b.getNameBasedOnBucket(bucketName, uwm)

			uwms = append(uwms, uwm)

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return uwms, nil
}

func (b *BaseDirReader) decodeFromBytes(encoded []byte, data any) error {
	return codec.NewDecoderBytes(encoded, b.ch).Decode(data)
}

func (b *BaseDirReader) getNameBasedOnBucket(bucketName string, uwm *Usage) string {
	if bucketName == groupUsageBucket {
		return b.groupCache.GroupName(uwm.GID)
	}

	return b.userCache.UserName(uwm.UID)
}

// UserUsage returns the usage for every UID-BaseDir combination in the
// database.
func (b *BaseDirReader) UserUsage(age summary.DirGUTAge) ([]*Usage, error) {
	return b.usage(userUsageBucket, age)
}

// GroupSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given group. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (b *BaseDirReader) GroupSubDirs(gid uint32, basedir string, age summary.DirGUTAge) ([]*SubDir, error) {
	return b.subDirs(groupSubDirsBucket, gid, basedir, age)
}

func (b *BaseDirReader) subDirs(bucket string, id uint32, basedir string, age summary.DirGUTAge) ([]*SubDir, error) {
	var sds []*SubDir

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		data := bucket.Get(keyName(id, basedir, age))

		if data == nil {
			return nil
		}

		return b.decodeFromBytes(data, &sds)
	}); err != nil {
		return nil, err
	}

	return sds, nil
}

// UserSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given user. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (b *BaseDirReader) UserSubDirs(uid uint32, basedir string, age summary.DirGUTAge) ([]*SubDir, error) {
	return b.subDirs(userSubDirsBucket, uid, basedir, age)
}

// GroupUsageTable returns GroupUsage() information formatted with the following
// tab separated columns:
//
// group_name
// owner_name
// directory_path
// last_modified (number of days ago)
// used size (used bytes)
// quota size (maximum allowed bytes)
// used inodes (number of files)
// quota inodes (maximum allowed number of bytes)
// warning ("OK" or "Not OK" if quota is estimated to have run out in 3 days)
//
// Any error returned is from GroupUsage().
func (b *BaseDirReader) GroupUsageTable(age summary.DirGUTAge) (string, error) {
	gu, err := b.GroupUsage(age)
	if err != nil {
		return "", err
	}

	return b.usageTable(gu)
}

func (b *BaseDirReader) usageTable(usage []*Usage) (string, error) {
	var sb strings.Builder

	for _, u := range usage {
		fmt.Fprintf(&sb, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\n",
			u.Name,
			b.owners[u.GID],
			u.BaseDir,
			daysSince(u.Mtime),
			u.UsageSize,
			u.QuotaSize,
			u.UsageInodes,
			u.QuotaInodes,
			usageStatus(u.DateNoSpace, u.DateNoFiles),
		)
	}

	return sb.String(), nil
}

func usageStatus(sizeExceedDate, inodeExceedDate time.Time) string {
	threeDaysFromNow := time.Now().Add(threeDays)

	if !sizeExceedDate.IsZero() && threeDaysFromNow.After(sizeExceedDate) {
		return quotaStatusNotOK
	}

	if !inodeExceedDate.IsZero() && threeDaysFromNow.After(inodeExceedDate) {
		return quotaStatusNotOK
	}

	return quotaStatusOK
}

// UserUsageTable returns UserUsage() information formatted with the following
// tab separated columns:
//
// user_name
// owner_name (always blank)
// directory_path
// last_modified (number of days ago)
// used size (used bytes)
// quota size (always 0)
// used inodes (number of files)
// quota inodes (always 0)
// warning (always "OK")
//
// Any error returned is from UserUsage().
func (b *BaseDirReader) UserUsageTable(age summary.DirGUTAge) (string, error) {
	uu, err := b.UserUsage(age)
	if err != nil {
		return "", err
	}

	return b.usageTable(uu)
}

func daysSince(mtime time.Time) uint64 {
	return uint64(time.Since(mtime) / secondsInDay)
}

// GroupSubDirUsageTable returns GroupSubDirs() information formatted with the
// following tab separated columns:
//
// base_directory_path
// sub_directory
// num_files
// size
// last_modified
// filetypes
//
// Any error returned is from GroupSubDirs().
func (b *BaseDirReader) GroupSubDirUsageTable(gid uint32, basedir string, age summary.DirGUTAge) (string, error) {
	gsdut, err := b.GroupSubDirs(gid, basedir, age)
	if err != nil {
		return "", err
	}

	return subDirUsageTable(basedir, gsdut), nil
}

func subDirUsageTable(basedir string, subdirs []*SubDir) string {
	var sb strings.Builder

	for _, subdir := range subdirs {
		fmt.Fprintf(&sb, "%s\t%s\t%d\t%d\t%d\t%s\n",
			basedir,
			subdir.SubDir,
			subdir.NumFiles,
			subdir.SizeFiles,
			daysSince(subdir.LastModified),
			subdir.FileUsage,
		)
	}

	return sb.String()
}

// UserSubDirUsageTable returns UserSubDirs() information formatted with the
// following tab separated columns:
//
// base_directory_path
// sub_directory
// num_files
// size
// last_modified
// filetypes
//
// Any error returned is from UserSubDirUsageTable().
func (b *BaseDirReader) UserSubDirUsageTable(uid uint32, basedir string, age summary.DirGUTAge) (string, error) {
	usdut, err := b.UserSubDirs(uid, basedir, age)
	if err != nil {
		return "", err
	}

	return subDirUsageTable(basedir, usdut), nil
}
