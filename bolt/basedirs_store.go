package bolt

import (
	"encoding/binary"
	"os"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Basedirs is a Bolt-backed implementation of basedirs.Store.
// Use NewBasedirs for writable and OpenReadOnlyBasedirs for read-only access.
type Basedirs struct{ db *bdb }

// NewBasedirs creates a new writable basedirs store at the given path.
func NewBasedirs(path string) (*Basedirs, error) {
	db, err := open(path, 0640, &boptions{ //nolint:mnd
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bfreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	return &Basedirs{db: db}, nil
}

// OpenReadOnlyBasedirs opens a read-only basedirs store at the given path.
func OpenReadOnlyBasedirs(path string) (*Basedirs, error) {
	db, err := open(path, os.FileMode(0640), &boptions{ReadOnly: true}) //nolint:mnd
	if err != nil {
		return nil, err
	}

	return &Basedirs{db: db}, nil
}

// Close closes the underlying database.
func (s *Basedirs) Close() error { return s.db.Close() }

// Update runs a read-write transaction providing a basedirs.Writer.
func (s *Basedirs) Update(fn func(basedirs.Writer) error) error {
	return s.db.Update(func(tx *btx) error { return fn(&bdWriter{tx}) })
}

// View runs a read-only transaction providing a basedirs.Reader.
func (s *Basedirs) View(fn func(basedirs.Reader) error) error {
	return s.db.View(func(tx *btx) error { return fn(&bdReader{tx}) })
}

// bdReader adapts bolt.Tx to the basedirs.Reader interface.
type bdReader struct{ *btx }

func (r *bdReader) Info() (*basedirs.DBInfo, error) {
	info := &basedirs.DBInfo{}

	// Helper to count entries with age==All (no second '-')
	countAgeAll := func(bucket string) (int, error) {
		b := r.Bucket([]byte(bucket))
		if b == nil {
			return 0, nil
		}
		total := 0
		err := b.ForEach(func(k, _ []byte) error {
			if bytesCountDash(k) == 1 {
				total++
			}
			return nil
		})
		return total, err
	}

	// Count group/user dir combos
	if c, err := countAgeAll(basedirs.GroupUsageBucket); err != nil {
		return nil, err
	} else {
		info.GroupDirCombos = c
	}
	if c, err := countAgeAll(basedirs.UserUsageBucket); err != nil {
		return nil, err
	} else {
		info.UserDirCombos = c
	}

	// Count group histories (combos and total entries)
	if b := r.Bucket([]byte(basedirs.GroupHistoricalBucket)); b != nil {
		if err := b.ForEach(func(_, v []byte) error {
			var histories []basedirs.History
			if err := codecDecode(v, &histories); err != nil {
				return err
			}
			info.GroupMountCombos++
			info.GroupHistories += len(histories)
			return nil
		}); err != nil {
			return nil, err
		}
	}

	// Count subdirs
	countSubDirs := func(bucket string) (int, int, error) {
		b := r.Bucket([]byte(bucket))
		if b == nil {
			return 0, 0, nil
		}
		combos, total := 0, 0
		err := b.ForEach(func(k, v []byte) error {
			if bytesCountDash(k) != 1 {
				return nil
			}
			combos++
			var subs []*basedirs.SubDir
			if err := codecDecode(v, &subs); err != nil {
				return err
			}
			total += len(subs)
			return nil
		})
		return combos, total, err
	}

	if c, t, err := countSubDirs(basedirs.GroupSubDirsBucket); err != nil {
		return nil, err
	} else {
		info.GroupSubDirCombos, info.GroupSubDirs = c, t
	}
	if c, t, err := countSubDirs(basedirs.UserSubDirsBucket); err != nil {
		return nil, err
	} else {
		info.UserSubDirCombos, info.UserSubDirs = c, t
	}

	return info, nil
}

func (r *bdReader) GroupUsage(age uint16) ([]*basedirs.Usage, error) {
	return r.usageFromBucket(basedirs.GroupUsageBucket, age)
}

func (r *bdReader) UserUsage(age uint16) ([]*basedirs.Usage, error) {
	return r.usageFromBucket(basedirs.UserUsageBucket, age)
}

func (r *bdReader) usageFromBucket(bucketName string, age uint16) ([]*basedirs.Usage, error) {
	var out []*basedirs.Usage

	b := r.Bucket([]byte(bucketName))
	if b == nil {
		return nil, nil
	}

	err := b.ForEach(func(_, v []byte) error {
		var u basedirs.Usage
		if err := codecDecode(v, &u); err != nil {
			return err
		}
		if uint16(u.Age) == age {
			out = append(out, &u)
		}
		return nil
	})
	return out, err
}

func (r *bdReader) GroupSubDirs(gid uint32, basedir string, age uint16) ([]*basedirs.SubDir, error) {
	b := r.Bucket([]byte(basedirs.GroupSubDirsBucket))
	if b == nil {
		return nil, nil
	}
	v := b.Get(basedirsKey(gid, basedir, age))
	if v == nil {
		return nil, nil
	}
	var out []*basedirs.SubDir
	if err := codecDecode(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *bdReader) UserSubDirs(uid uint32, basedir string, age uint16) ([]*basedirs.SubDir, error) {
	b := r.Bucket([]byte(basedirs.UserSubDirsBucket))
	if b == nil {
		return nil, nil
	}
	v := b.Get(basedirsKey(uid, basedir, age))
	if v == nil {
		return nil, nil
	}
	var out []*basedirs.SubDir
	if err := codecDecode(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *bdReader) History(gid uint32, mountpoint string) ([]basedirs.History, error) {
	b := r.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return nil, nil
	}
	v := b.Get(basedirsKey(gid, mountpoint, 0))
	if v == nil {
		return nil, nil
	}
	var out []basedirs.History
	if err := codecDecode(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *bdReader) ForEachGroupHistory(fn func(gid uint32, path string, histories []basedirs.History) error) error {
	b := r.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return nil
	}
	return b.ForEach(func(k, v []byte) error {
		gid, path := parseGIDPath(k)
		var histories []basedirs.History
		if err := codecDecode(v, &histories); err != nil {
			return err
		}
		return fn(gid, path, histories)
	})
}

// Domain iterators (age=All)
func (r *bdReader) ForEachGroupUsageAll(fn func(*basedirs.Usage) error) error {
	usages, err := r.usageFromBucket(basedirs.GroupUsageBucket, uint16(db.DGUTAgeAll))
	if err != nil {
		return err
	}
	for _, u := range usages {
		if err := fn(u); err != nil {
			return err
		}
	}
	return nil
}

func (r *bdReader) ForEachUserUsageAll(fn func(*basedirs.Usage) error) error {
	usages, err := r.usageFromBucket(basedirs.UserUsageBucket, uint16(db.DGUTAgeAll))
	if err != nil {
		return err
	}
	for _, u := range usages {
		if err := fn(u); err != nil {
			return err
		}
	}
	return nil
}

func (r *bdReader) ForEachGroupSubDirsAll(fn func(gid uint32, basedir string, subs []*basedirs.SubDir) error) error {
	b := r.Bucket([]byte(basedirs.GroupSubDirsBucket))
	if b == nil {
		return nil
	}
	return b.ForEach(func(k, v []byte) error {
		if bytesCountDash(k) != 1 {
			return nil
		}
		gid, basedir := parseIDPath(k)
		var subs []*basedirs.SubDir
		if err := codecDecode(v, &subs); err != nil {
			return err
		}
		return fn(gid, basedir, subs)
	})
}

func (r *bdReader) ForEachUserSubDirsAll(fn func(uid uint32, basedir string, subs []*basedirs.SubDir) error) error {
	b := r.Bucket([]byte(basedirs.UserSubDirsBucket))
	if b == nil {
		return nil
	}
	return b.ForEach(func(k, v []byte) error {
		if bytesCountDash(k) != 1 {
			return nil
		}
		uid, basedir := parseIDPath(k)
		var subs []*basedirs.SubDir
		if err := codecDecode(v, &subs); err != nil {
			return err
		}
		return fn(uid, basedir, subs)
	})
}

// bdWriter adapts bolt.Tx to the basedirs.Writer interface.
type bdWriter struct{ *btx }

func (w *bdWriter) ensure(bucket string) (*bbucket, error) {
	b := w.Bucket([]byte(bucket))
	if b != nil {
		return b, nil
	}
	nb, err := w.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return nil, err
	}
	return nb, nil
}

func (w *bdWriter) PutGroupUsage(u *basedirs.Usage) error {
	b, err := w.ensure(basedirs.GroupUsageBucket)
	if err != nil {
		return err
	}
	return b.Put(basedirsKey(u.GID, u.BaseDir, uint16(u.Age)), codecEncode(u))
}

func (w *bdWriter) PutUserUsage(u *basedirs.Usage) error {
	b, err := w.ensure(basedirs.UserUsageBucket)
	if err != nil {
		return err
	}
	return b.Put(basedirsKey(u.UID, u.BaseDir, uint16(u.Age)), codecEncode(u))
}

func (w *bdWriter) PutGroupSubDirs(gid uint32, basedir string, age uint16, subs []*basedirs.SubDir) error {
	b, err := w.ensure(basedirs.GroupSubDirsBucket)
	if err != nil {
		return err
	}
	return b.Put(basedirsKey(gid, basedir, age), codecEncode(subs))
}

func (w *bdWriter) PutUserSubDirs(uid uint32, basedir string, age uint16, subs []*basedirs.SubDir) error {
	b, err := w.ensure(basedirs.UserSubDirsBucket)
	if err != nil {
		return err
	}
	return b.Put(basedirsKey(uid, basedir, age), codecEncode(subs))
}

func (w *bdWriter) PutHistory(gid uint32, mountpoint string, histories []basedirs.History) error {
	if err := w.EnsureHistoryBucket(); err != nil {
		return err
	}
	b := w.Bucket([]byte(basedirs.GroupHistoricalBucket))
	return b.Put(basedirsKey(gid, mountpoint, 0), codecEncode(histories))
}

func (w *bdWriter) ForEachGroupUsage(fn func(*basedirs.Usage) error) error {
	b := w.Bucket([]byte(basedirs.GroupUsageBucket))
	if b == nil {
		return nil
	}
	return b.ForEach(func(_, v []byte) error {
		var u basedirs.Usage
		if err := codecDecode(v, &u); err != nil {
			return err
		}
		return fn(&u)
	})
}

func (w *bdWriter) History(gid uint32, mountpoint string) ([]basedirs.History, error) {
	b := w.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return nil, nil
	}
	v := b.Get(basedirsKey(gid, mountpoint, 0))
	if v == nil {
		return nil, nil
	}
	var out []basedirs.History
	if err := codecDecode(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *bdWriter) EnsureHistoryBucket() error {
	_, err := w.CreateBucketIfNotExists([]byte(basedirs.GroupHistoricalBucket))
	return err
}

func (w *bdWriter) DeleteHistory(gid uint32, mountpoint string) error {
	b := w.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return nil
	}
	return b.Delete(basedirsKey(gid, mountpoint, 0))
}

// helpers.
func basedirsKey(id uint32, path string, age uint16) []byte {
	// mirror basedirs.keyName implementation
	const (
		idSize  = 4
		sepSize = 1
		ageSize = 2
	)

	// pre-calculate capacity: id + two possible separators + age bytes + path
	length := idSize + 2*sepSize + ageSize + len(path)
	// id as little-endian uint32
	b := make([]byte, idSize, length)
	binary.LittleEndian.PutUint32(b, id)
	b = append(b, '-')

	b = append(b, path...)
	// compare as uint16 to avoid narrowing conversions
	if age != uint16(db.DGUTAgeAll) {
		b = append(b, '-')
		b = b[:length]
		binary.LittleEndian.PutUint16(b[length-ageSize:], age)
	}

	return b
}

// codec helpers use the same encoding as basedirs (binc).
func codecEncode(v any) []byte {
	var out []byte
	enc := codec.NewEncoderBytes(&out, new(codec.BincHandle))
	enc.MustEncode(v)
	return out
}

func codecDecode(bts []byte, v any) error {
	return codec.NewDecoderBytes(bts, new(codec.BincHandle)).Decode(v)
}

// parseGIDPath parses a key produced by basedirsKey(id,path,0) and returns gid and path.
func parseGIDPath(k []byte) (uint32, string) {
	if len(k) < 5 {
		return 0, ""
	}
	gid := uint32(k[0]) | uint32(k[1])<<8 | uint32(k[2])<<16 | uint32(k[3])<<24
	// k[4] is '-'
	return gid, string(k[5:])
}

// parseIDPath parses a key produced by basedirsKey(id,path,age) for age=All (no trailing age bytes).
func parseIDPath(k []byte) (uint32, string) {
	if len(k) < 5 {
		return 0, ""
	}
	id := uint32(k[0]) | uint32(k[1])<<8 | uint32(k[2])<<16 | uint32(k[3])<<24
	return id, string(k[5:])
}

// bytesCountDash counts '-' occurrences to detect presence of age in keys.
func bytesCountDash(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '-' {
			n++
		}
	}
	return n
}
