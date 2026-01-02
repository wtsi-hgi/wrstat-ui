package bolt

import (
	"encoding/binary"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

type baseDirsReader struct {
	db *bolt.DB
	ch codec.Handle

	mountPath string
	updatedAt time.Time

	mountPoints basedirs.MountPoints

	owners     map[uint32]string
	groupCache *basedirs.GroupCache
	userCache  *basedirs.UserCache
}

// OpenBaseDirsReader opens a basedirs.db file for reading.
// ownersPath is the owners CSV path used to populate Usage.Owner.
func OpenBaseDirsReader(dbPath, ownersPath string) (basedirs.Reader, error) {
	owners, err := basedirs.ParseOwners(ownersPath)
	if err != nil {
		return nil, err
	}

	db, err := openBoltReadOnly(dbPath)
	if err != nil {
		return nil, err
	}

	r := &baseDirsReader{
		db:         db,
		ch:         new(codec.BincHandle),
		owners:     owners,
		groupCache: basedirs.NewGroupCache(),
		userCache:  basedirs.NewUserCache(),
	}

	// Best-effort load meta; absent meta is allowed for legacy dbs.
	_ = r.loadMeta()

	if r.mountPath != "" {
		r.mountPoints = basedirs.ValidateMountPoints([]string{r.mountPath})
	}

	return r, nil
}

func (r *baseDirsReader) loadMeta() error {
	return r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketName))
		if b == nil {
			return nil
		}

		if mp := b.Get([]byte(metaKeyMountPath)); mp != nil {
			r.mountPath = string(mp)
		}

		if u := b.Get([]byte(metaKeyUpdatedAt)); len(u) == 8 {
			sec := int64(binary.LittleEndian.Uint64(u))
			r.updatedAt = time.Unix(sec, 0)
		}

		return nil
	})
}

func (r *baseDirsReader) GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return r.usage(basedirs.GroupUsageBucket, age)
}

func (r *baseDirsReader) UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return r.usage(basedirs.UserUsageBucket, age)
}

func (r *baseDirsReader) usage(bucketName string, age db.DirGUTAge) ([]*basedirs.Usage, error) {
	var out []*basedirs.Usage

	if err := r.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(_, data []byte) error {
			u := new(basedirs.Usage)
			if err := r.decode(data, u); err != nil {
				return err
			}
			if u.Age != age {
				return nil
			}

			u.Owner = r.owners[u.GID]
			if bucketName == basedirs.GroupUsageBucket {
				u.Name = r.groupCache.GroupName(u.GID)
			} else {
				u.Name = r.userCache.UserName(u.UID)
			}

			out = append(out, u)
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *baseDirsReader) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return r.subDirs(basedirs.GroupSubDirsBucket, gid, basedir, age)
}

func (r *baseDirsReader) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return r.subDirs(basedirs.UserSubDirsBucket, uid, basedir, age)
}

func (r *baseDirsReader) subDirs(bucketName string, id uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	var out []*basedirs.SubDir

	if err := r.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}

		data := bucket.Get(basedirsKeyName(id, basedir, age))
		if data == nil {
			return basedirs.ErrNoSuchUserOrGroup
		}

		return r.decode(data, &out)
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *baseDirsReader) History(gid uint32, path string) ([]basedirs.History, error) {
	mp := r.prefixOf(path)
	if mp == "" {
		return nil, basedirs.ErrInvalidBasePath
	}

	var history []basedirs.History
	k := basedirsKeyName(gid, mp, db.DGUTAgeAll)

	if err := r.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
		if bucket == nil {
			return basedirs.ErrNoBaseDirHistory
		}

		data := bucket.Get(k)
		if data == nil {
			return basedirs.ErrNoBaseDirHistory
		}

		return r.decode(data, &history)
	}); err != nil {
		return nil, err
	}

	return history, nil
}

func (r *baseDirsReader) SetMountPoints(mountpoints []string) {
	r.mountPoints = basedirs.ValidateMountPoints(mountpoints)
}

func (r *baseDirsReader) prefixOf(path string) string {
	if r.mountPoints == nil {
		return ""
	}

	return r.mountPoints.PrefixOf(path)
}

func (r *baseDirsReader) SetCachedGroup(gid uint32, name string) {
	r.groupCache.SetCached(gid, name)
}

func (r *baseDirsReader) SetCachedUser(uid uint32, name string) {
	r.userCache.SetCached(uid, name)
}

func (r *baseDirsReader) MountTimestamps() (map[string]time.Time, error) {
	if r.mountPath == "" || r.updatedAt.IsZero() {
		return map[string]time.Time{}, nil
	}

	mountKey := strings.ReplaceAll(r.mountPath, "/", "／")
	return map[string]time.Time{mountKey: r.updatedAt}, nil
}

func (r *baseDirsReader) Info() (*basedirs.DBInfo, error) {
	info := &basedirs.DBInfo{}

	if err := r.db.View(func(tx *bolt.Tx) error {
		info.GroupDirCombos, _ = countFromFullBucketScan(tx, basedirs.GroupUsageBucket, countOnly, r.ch)
		info.GroupMountCombos, info.GroupHistories = countFromFullBucketScan(tx, basedirs.GroupHistoricalBucket, countHistories, r.ch)
		info.GroupSubDirCombos, info.GroupSubDirs = countFromFullBucketScan(tx, basedirs.GroupSubDirsBucket, countSubDirs, r.ch)
		info.UserDirCombos, _ = countFromFullBucketScan(tx, basedirs.UserUsageBucket, countOnly, r.ch)
		info.UserSubDirCombos, info.UserSubDirs = countFromFullBucketScan(tx, basedirs.UserSubDirsBucket, countSubDirs, r.ch)
		return nil
	}); err != nil {
		return nil, err
	}

	return info, nil
}

func countFromFullBucketScan(tx *bolt.Tx, bucketName string, cb func(v []byte, ch codec.Handle) int, ch codec.Handle) (int, int) {
	b := tx.Bucket([]byte(bucketName))
	if b == nil {
		return 0, 0
	}

	count := 0
	sliceLen := 0

	_ = b.ForEach(func(k, v []byte) error {
		if !basedirsKeyAgeIsAll(k) {
			return nil
		}
		count++
		sliceLen += cb(v, ch)
		return nil
	})

	return count, sliceLen
}

func countOnly(_ []byte, _ codec.Handle) int { return 0 }

func countHistories(v []byte, ch codec.Handle) int {
	var histories []basedirs.History
	codec.NewDecoderBytes(v, ch).MustDecode(&histories)
	return len(histories)
}

func countSubDirs(v []byte, ch codec.Handle) int {
	var subdirs []*basedirs.SubDir
	codec.NewDecoderBytes(v, ch).MustDecode(&subdirs)
	return len(subdirs)
}

func (r *baseDirsReader) Close() error {
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	r.db = nil
	return err
}

func (r *baseDirsReader) decode(encoded []byte, v any) error {
	return codec.NewDecoderBytes(encoded, r.ch).Decode(v)
}
