package bolt

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

const basedirsDBBasename = "basedirs.db"

type baseDirsStore struct {
	db *bolt.DB
	ch codec.Handle
	tx *bolt.Tx

	mountPath string
	updatedAt time.Time
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

func (s *baseDirsStore) SetMountPath(mountPath string) {
	s.mountPath = mountPath
}

func (s *baseDirsStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
}

func (s *baseDirsStore) Reset() error {
	if s.db == nil {
		return fmt.Errorf("nil db") //nolint:err113
	}
	if s.mountPath == "" || s.updatedAt.IsZero() {
		return ErrMetadataNotSet
	}
	if !strings.HasPrefix(s.mountPath, "/") || !strings.HasSuffix(s.mountPath, "/") {
		return ErrInvalidMountPath
	}

	if err := persistMeta(s.db, s.mountPath, s.updatedAt); err != nil {
		return err
	}

	if s.tx != nil {
		_ = s.tx.Rollback()
		s.tx = nil
	}

	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}

	// Replace usage/subdir buckets; keep history.
	if err := clearAndRecreateBucket(tx, basedirs.GroupUsageBucket); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := clearAndRecreateBucket(tx, basedirs.UserUsageBucket); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := clearAndRecreateBucket(tx, basedirs.GroupSubDirsBucket); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := clearAndRecreateBucket(tx, basedirs.UserSubDirsBucket); err != nil {
		_ = tx.Rollback()
		return err
	}

	s.tx = tx

	return nil
}

func (s *baseDirsStore) PutGroupUsage(u *basedirs.Usage) error {
	if s.tx == nil {
		return fmt.Errorf("store not reset") //nolint:err113
	}

	b := s.tx.Bucket([]byte(basedirs.GroupUsageBucket))
	if b == nil {
		return fmt.Errorf("bucket missing: %s", basedirs.GroupUsageBucket) //nolint:err113
	}

	return b.Put(basedirsKeyName(u.GID, u.BaseDir, u.Age), s.encode(u))
}

func (s *baseDirsStore) PutUserUsage(u *basedirs.Usage) error {
	if s.tx == nil {
		return fmt.Errorf("store not reset") //nolint:err113
	}

	b := s.tx.Bucket([]byte(basedirs.UserUsageBucket))
	if b == nil {
		return fmt.Errorf("bucket missing: %s", basedirs.UserUsageBucket) //nolint:err113
	}

	return b.Put(basedirsKeyName(u.UID, u.BaseDir, u.Age), s.encode(u))
}

func (s *baseDirsStore) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s.tx == nil {
		return fmt.Errorf("store not reset") //nolint:err113
	}

	b := s.tx.Bucket([]byte(basedirs.GroupSubDirsBucket))
	if b == nil {
		return fmt.Errorf("bucket missing: %s", basedirs.GroupSubDirsBucket) //nolint:err113
	}

	return b.Put(basedirsKeyName(key.ID, key.BaseDir, key.Age), s.encode(subdirs))
}

func (s *baseDirsStore) PutUserSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	if s.tx == nil {
		return fmt.Errorf("store not reset") //nolint:err113
	}

	b := s.tx.Bucket([]byte(basedirs.UserSubDirsBucket))
	if b == nil {
		return fmt.Errorf("bucket missing: %s", basedirs.UserSubDirsBucket) //nolint:err113
	}

	return b.Put(basedirsKeyName(key.ID, key.BaseDir, key.Age), s.encode(subdirs))
}

func (s *baseDirsStore) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	if s.tx == nil {
		return fmt.Errorf("store not reset") //nolint:err113
	}

	b := s.tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return fmt.Errorf("bucket missing: %s", basedirs.GroupHistoricalBucket) //nolint:err113
	}

	k := basedirsKeyName(key.GID, key.MountPath, db.DGUTAgeAll)
	existing := b.Get(k)

	var histories []basedirs.History
	if existing != nil {
		if err := s.decode(existing, &histories); err != nil {
			return err
		}
		if len(histories) > 0 && !point.Date.After(histories[len(histories)-1].Date) {
			return nil
		}
	}

	histories = append(histories, point)
	return b.Put(k, s.encode(histories))
}

func (s *baseDirsStore) Finalize() error {
	if s.tx == nil {
		return nil
	}

	gub := s.tx.Bucket([]byte(basedirs.GroupUsageBucket))
	ghb := s.tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if gub == nil || ghb == nil {
		_ = s.tx.Rollback()
		s.tx = nil
		return fmt.Errorf("required buckets missing") //nolint:err113
	}

	// Precompute DateNoSpace/DateNoFiles for group usage at age=all.
	if err := gub.ForEach(func(k, v []byte) error {
		gu := new(basedirs.Usage)
		if err := s.decode(v, gu); err != nil {
			return err
		}

		if gu.Age != db.DGUTAgeAll {
			return nil
		}

		// Histories are stored per (gid, mountPath).
		hkey := basedirsKeyName(gu.GID, s.mountPath, db.DGUTAgeAll)
		data := ghb.Get(hkey)
		if data == nil {
			return basedirs.ErrNoBaseDirHistory
		}

		var history []basedirs.History
		if err := s.decode(data, &history); err != nil {
			return err
		}

		sizeExceedDate, inodeExceedDate := basedirs.DateQuotaFull(history)
		gu.DateNoSpace = sizeExceedDate
		gu.DateNoFiles = inodeExceedDate

		return gub.Put(k, s.encode(gu))
	}); err != nil {
		_ = s.tx.Rollback()
		s.tx = nil
		return err
	}

	err := s.tx.Commit()
	s.tx = nil
	return err
}

func (s *baseDirsStore) Close() error {
	var closeErr error

	if s.tx != nil {
		closeErr = errors.Join(closeErr, s.tx.Rollback())
		s.tx = nil
	}

	if s.db != nil {
		closeErr = errors.Join(closeErr, s.db.Close())
		s.db = nil
	}

	return closeErr
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

func openBasedirsWritable(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, boltFilePerms, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		// Data buckets
		if _, errc := tx.CreateBucketIfNotExists([]byte(basedirs.GroupUsageBucket)); errc != nil {
			return errc
		}
		if _, errc := tx.CreateBucketIfNotExists([]byte(basedirs.UserUsageBucket)); errc != nil {
			return errc
		}
		if _, errc := tx.CreateBucketIfNotExists([]byte(basedirs.GroupSubDirsBucket)); errc != nil {
			return errc
		}
		if _, errc := tx.CreateBucketIfNotExists([]byte(basedirs.UserSubDirsBucket)); errc != nil {
			return errc
		}
		if _, errc := tx.CreateBucketIfNotExists([]byte(basedirs.GroupHistoricalBucket)); errc != nil {
			return errc
		}
		// Meta bucket
		_, errc := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		return errc
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func clearAndRecreateBucket(tx *bolt.Tx, name string) error {
	if err := tx.DeleteBucket([]byte(name)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return err
	}
	_, err := tx.CreateBucketIfNotExists([]byte(name))
	return err
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
				return fmt.Errorf("dest history bucket missing") //nolint:err113
			}

			return pb.ForEach(func(k, v []byte) error {
				return db.Put(k, v)
			})
		})
	})
}
