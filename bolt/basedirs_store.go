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
type Basedirs struct{ db *DB }

// NewBasedirs creates a new writable basedirs store at the given path.
func NewBasedirs(path string) (*Basedirs, error) {
	db, err := Open(path, 0640, &Options{ //nolint:mnd
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	return &Basedirs{db: db}, nil
}

// OpenReadOnlyBasedirs opens a read-only basedirs store at the given path.
func OpenReadOnlyBasedirs(path string) (*Basedirs, error) {
	db, err := Open(path, os.FileMode(0640), &Options{ReadOnly: true}) //nolint:mnd
	if err != nil {
		return nil, err
	}

	return &Basedirs{db: db}, nil
}

// Close closes the underlying database.
func (s *Basedirs) Close() error { return s.db.Close() }

// Update runs a read-write transaction providing a basedirs.Writer.
func (s *Basedirs) Update(fn func(basedirs.Writer) error) error {
	return s.db.Update(func(tx *Tx) error { return fn(&bdWriter{tx}) })
}

// View runs a read-only transaction providing a basedirs.Reader.
func (s *Basedirs) View(fn func(basedirs.Reader) error) error {
	return s.db.View(func(tx *Tx) error { return fn(&bdReader{tx}) })
}

// bdReader adapts bolt.Tx to the basedirs.Reader interface.
type bdReader struct{ *Tx }

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

func (r *bdReader) ForEachRaw(bucket string, fn func(k, v []byte) error) error {
	b := r.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}

	return b.ForEach(fn)
}

// bdWriter adapts bolt.Tx to the basedirs.Writer interface.
type bdWriter struct{ *Tx }

func (w *bdWriter) ensure(bucket string) (*Bucket, error) {
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

func (w *bdWriter) PutRawHistory(key, value []byte) error {
	b, err := w.ensure(basedirs.GroupHistoricalBucket)
	if err != nil {
		return err
	}

	return b.Put(key, value)
}

func (w *bdWriter) DeleteHistoryKey(key []byte) error {
	b := w.Bucket([]byte(basedirs.GroupHistoricalBucket))
	if b == nil {
		return nil
	}

	return b.Delete(key)
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
