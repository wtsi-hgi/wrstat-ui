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
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

var (
	ErrRequiredBucketsMissing   = errors.New("required buckets missing")
	ErrStoreNotReset            = errors.New("store not reset")
	ErrDestHistoryBucketMissing = errors.New("dest history bucket missing")
)

const minHistoryKeyLen = 5

func createInitialBuckets(tx *bolt.Tx) error {
	buckets := []string{
		basedirs.GroupUsageBucket,
		basedirs.UserUsageBucket,
		basedirs.GroupSubDirsBucket,
		basedirs.UserSubDirsBucket,
		basedirs.GroupHistoricalBucket,
		metaBucketName,
	}

	for _, bk := range buckets {
		if _, errc := tx.CreateBucketIfNotExists([]byte(bk)); errc != nil {
			return errc
		}
	}

	return nil
}

type BucketMissingError string

func (e BucketMissingError) Error() string { return "bucket missing: " + string(e) }

type baseDirsStore struct {
	db *bolt.DB
	ch codec.Handle
	tx *bolt.Tx

	mountPath string
	updatedAt time.Time
}

func (s *baseDirsStore) SetMountPath(mountPath string) {
	s.mountPath = mountPath
}

func (s *baseDirsStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
}

func (s *baseDirsStore) Reset() error {
	if err := s.validateResetPreconditions(); err != nil {
		return err
	}
	// Ensure any previous write transaction is closed before taking the writer
	// lock again (persistMeta uses its own Update transaction).
	if err := s.rollbackExistingTx(); err != nil {
		return err
	}

	tx, err := s.prepareTx()
	if err != nil {
		return err
	}

	s.tx = tx

	return nil
}

func (s *baseDirsStore) prepareTx() (*bolt.Tx, error) {
	if err := persistMeta(s.db, s.mountPath, s.updatedAt); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}

	// Replace usage/subdir buckets; keep history.
	buckets := []string{
		basedirs.GroupUsageBucket,
		basedirs.UserUsageBucket,
		basedirs.GroupSubDirsBucket,
		basedirs.UserSubDirsBucket,
	}

	for _, bk := range buckets {
		if err := clearAndRecreateBucket(tx, bk); err != nil {
			return nil, errors.Join(err, tx.Rollback())
		}
	}

	return tx, nil
}

func clearAndRecreateBucket(tx *bolt.Tx, name string) error {
	if err := tx.DeleteBucket([]byte(name)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return err
	}

	_, err := tx.CreateBucketIfNotExists([]byte(name))

	return err
}

func (s *baseDirsStore) validateResetPreconditions() error {
	if s.db == nil {
		return ErrDBClosed
	}

	if s.mountPath == "" || s.updatedAt.IsZero() {
		return ErrMetadataNotSet
	}

	if !strings.HasPrefix(s.mountPath, "/") || !strings.HasSuffix(s.mountPath, "/") {
		return ErrInvalidMountPath
	}

	return nil
}

func (s *baseDirsStore) PutGroupUsage(u *basedirs.Usage) error {
	if s.tx == nil {
		return ErrStoreNotReset
	}

	b := s.tx.Bucket([]byte(basedirs.GroupUsageBucket))
	if b == nil {
		return BucketMissingError(basedirs.GroupUsageBucket)
	}

	return b.Put(basedirsKeyName(u.GID, u.BaseDir, u.Age), s.encode(u))
}

func (s *baseDirsStore) PutUserUsage(u *basedirs.Usage) error {
	if s.tx == nil {
		return ErrStoreNotReset
	}

	b := s.tx.Bucket([]byte(basedirs.UserUsageBucket))
	if b == nil {
		return BucketMissingError(basedirs.UserUsageBucket)
	}

	return b.Put(basedirsKeyName(u.UID, u.BaseDir, u.Age), s.encode(u))
}

func (s *baseDirsStore) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s.tx == nil {
		return ErrStoreNotReset
	}

	b := s.tx.Bucket([]byte(basedirs.GroupSubDirsBucket))
	if b == nil {
		return BucketMissingError(basedirs.GroupSubDirsBucket)
	}

	return b.Put(basedirsKeyName(key.ID, key.BaseDir, key.Age), s.encode(subdirs))
}

func (s *baseDirsStore) PutUserSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s.tx == nil {
		return ErrStoreNotReset
	}

	b := s.tx.Bucket([]byte(basedirs.UserSubDirsBucket))
	if b == nil {
		return BucketMissingError(basedirs.UserSubDirsBucket)
	}

	return b.Put(basedirsKeyName(key.ID, key.BaseDir, key.Age), s.encode(subdirs))
}

func (s *baseDirsStore) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	if s.tx == nil {
		return ErrStoreNotReset
	}

	b := s.tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return BucketMissingError(basedirs.GroupHistoricalBucket)
	}

	k := basedirsKeyName(key.GID, key.MountPath, db.DGUTAgeAll)
	existing := b.Get(k)

	var histories []basedirs.History
	if existing != nil {
		if err := s.decode(existing, &histories); err != nil {
			return err
		}
	}

	if len(histories) > 0 && !point.Date.After(histories[len(histories)-1].Date) {
		return nil
	}

	histories = append(histories, point)

	return b.Put(k, s.encode(histories))
}

func (s *baseDirsStore) Finalise() error {
	if s.tx == nil {
		return nil
	}

	gub, ghb, err := s.getFinaliseBuckets()
	if err != nil {
		return err
	}

	gidMounts, err := s.getGIDMounts(ghb)
	if err != nil {
		return err
	}

	// Precompute DateNoSpace/DateNoFiles for group usage at age=all.
	if err = gub.ForEach(func(k, v []byte) error {
		return s.processFinaliseEntry(gub, ghb, k, v, gidMounts)
	}); err != nil {
		rollbackErr := s.tx.Rollback()
		s.tx = nil

		return errors.Join(err, rollbackErr)
	}

	err = s.tx.Commit()
	s.tx = nil

	return err
}

func (s *baseDirsStore) getFinaliseBuckets() (*bolt.Bucket, *bolt.Bucket, error) {
	gub := s.tx.Bucket([]byte(basedirs.GroupUsageBucket))
	ghb := s.tx.Bucket([]byte(basedirs.GroupHistoricalBucket))

	if gub == nil || ghb == nil {
		rollbackErr := s.tx.Rollback()
		s.tx = nil

		return nil, nil, errors.Join(ErrRequiredBucketsMissing, rollbackErr)
	}

	return gub, ghb, nil
}

func (s *baseDirsStore) getGIDMounts(ghb *bolt.Bucket) (map[uint32][]string, error) {
	gidMounts := make(map[uint32][]string)

	if err := ghb.ForEach(func(k, _ []byte) error {
		gid, mountPath := decodeHistoryKey(k)
		gidMounts[gid] = append(gidMounts[gid], mountPath)

		return nil
	}); err != nil {
		rollbackErr := s.tx.Rollback()
		s.tx = nil

		return nil, errors.Join(err, rollbackErr)
	}

	return gidMounts, nil
}

func decodeHistoryKey(k []byte) (uint32, string) {
	if len(k) < minHistoryKeyLen {
		return 0, ""
	}

	gid := binary.LittleEndian.Uint32(k[:4])
	path := string(k[5:])

	return gid, path
}

func (s *baseDirsStore) processFinaliseEntry(gub, ghb *bolt.Bucket, k, v []byte,
	gidMounts map[uint32][]string) error {
	gu := new(basedirs.Usage)
	if err := s.decode(v, gu); err != nil {
		return err
	}

	if gu.Age != db.DGUTAgeAll {
		return nil
	}

	mountPath := findMountPath(gu, gidMounts)
	if mountPath == "" {
		return nil
	}

	return s.updateUsageWithHistory(gub, ghb, k, gu, mountPath)
}

func findMountPath(gu *basedirs.Usage, gidMounts map[uint32][]string) string {
	mounts, ok := gidMounts[gu.GID]
	if !ok {
		return ""
	}

	var mountPath string
	for _, mp := range mounts {
		if strings.HasPrefix(gu.BaseDir, mp) && len(mp) > len(mountPath) {
			mountPath = mp
		}
	}

	return mountPath
}

func (s *baseDirsStore) updateUsageWithHistory(gub, ghb *bolt.Bucket, k []byte,
	gu *basedirs.Usage, mountPath string) error {
	// Histories are stored per (gid, mountPath).
	hkey := basedirsKeyName(gu.GID, mountPath, db.DGUTAgeAll)

	data := ghb.Get(hkey)
	if data == nil {
		return nil
	}

	var history []basedirs.History
	if err := s.decode(data, &history); err != nil {
		return err
	}

	sizeExceedDate, inodeExceedDate := basedirs.DateQuotaFull(history)
	gu.DateNoSpace = sizeExceedDate
	gu.DateNoFiles = inodeExceedDate

	return gub.Put(k, s.encode(gu))
}

func (s *baseDirsStore) Close() error {
	var err error

	if s.tx != nil {
		err = errors.Join(err, s.rollbackExistingTx())
	}

	if s.db != nil {
		err = errors.Join(err, s.db.Close())
		s.db = nil
	}

	return err
}

func (s *baseDirsStore) encode(v any) []byte {
	var out []byte

	enc := codec.NewEncoderBytes(&out, s.ch)
	enc.MustEncode(v)

	return out
}

func (s *baseDirsStore) decode(encoded []byte, v any) error {
	return codec.NewDecoderBytes(encoded, s.ch).Decode(v)
}

func (s *baseDirsStore) rollbackExistingTx() error {
	if s.tx == nil {
		return nil
	}

	err := s.tx.Rollback()
	s.tx = nil

	return err
}

// NewBaseDirsStore creates a basedirs.Store backed by Bolt.
// dbPath is the path to the new basedirs.db file.
// previousDBPath, if non-empty, is the path to a previous basedirs.db from
// which history will be seeded.
func NewBaseDirsStore(dbPath, previousDBPath string) (basedirs.Store, error) {
	db, err := openBasedirsWritable(dbPath)
	if err != nil {
		return nil, err
	}

	if previousDBPath != "" {
		if err := seedBasedirsHistory(db, previousDBPath); err != nil {
			_ = db.Close()

			return nil, err
		}
	}

	return &baseDirsStore{
		db: db,
		ch: new(codec.BincHandle),
	}, nil
}

func openBasedirsWritable(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, boltFilePerms, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(createInitialBuckets)
	if err != nil {
		_ = db.Close()

		return nil, err
	}

	return db, nil
}

func seedBasedirsHistory(dest *bolt.DB, previousDBPath string) error {
	prev, err := bolt.Open(previousDBPath, boltFilePerms, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer prev.Close()

	return prev.View(func(ptx *bolt.Tx) error {
		pb := ptx.Bucket([]byte(basedirs.GroupHistoricalBucket))
		if pb == nil {
			return nil
		}

		return dest.Update(func(dtx *bolt.Tx) error {
			db := dtx.Bucket([]byte(basedirs.GroupHistoricalBucket))
			if db == nil {
				return ErrDestHistoryBucketMissing
			}

			return pb.ForEach(func(k, v []byte) error {
				return db.Put(k, v)
			})
		})
	})
}
