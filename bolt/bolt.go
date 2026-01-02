package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

var ErrInvalidConfig = errors.New("invalid config")

var (
	ErrOutputDirMissing = errors.New("output directory missing")
	ErrMetadataNotSet   = errors.New("mountPath/updatedAt not set")
	ErrInvalidMountPath = errors.New("invalid mountPath")
	ErrNotImplemented   = errors.New("not implemented")
	ErrInvalidUpdatedAt = errors.New("invalid updatedAt")
)

const (
	dgutaBucketName    = "gut"
	childrenBucketName = "children"
	metaBucketName     = "_meta"
	metaKeyMountPath   = "mountPath"
	metaKeyUpdatedAt   = "updatedAt"
	boltFilePerms      = 0o640
	dgutaDBBasename    = "dguta.db"
	childrenDBBasename = dgutaDBBasename + ".children"
)

// Config configures the Bolt backend.
type Config struct {
	// BasePath is the directory scanned for dataset subdirectories.
	BasePath string

	// DGUTADBName and BaseDirDBName are the entry names expected inside each
	// dataset directory (e.g. "dguta.dbs" and "basedirs.db").
	DGUTADBName   string
	BaseDirDBName string

	// OwnersCSVPath is required for basedirs name resolution.
	OwnersCSVPath string

	// MountPoints overrides mount auto-discovery for basedirs history resolution.
	// When empty, the backend auto-discovers mountpoints from the OS.
	MountPoints []string

	// PollInterval controls how often BasePath is rescanned for updates.
	// If zero or negative, automatic reloading is disabled.
	PollInterval time.Duration

	// RemoveOldPaths controls whether older dataset directories are removed
	// after a successful reload.
	RemoveOldPaths bool
}

type dgutaWriter struct {
	outputDir string

	batchSize  int
	writeBatch []db.RecordDGUTA
	writeErr   error

	dgutaDB     *bolt.DB
	childrenDB  *bolt.DB
	codecHandle codec.Handle

	mountPath string
	updatedAt time.Time
	metaDone  bool
}

func (w *dgutaWriter) SetBatchSize(batchSize int) {
	if batchSize <= 0 {
		batchSize = 1
	}

	w.batchSize = batchSize
	if cap(w.writeBatch) < batchSize {
		w.writeBatch = make([]db.RecordDGUTA, 0, batchSize)
	}
}

func (w *dgutaWriter) SetMountPath(mountPath string) {
	w.mountPath = mountPath
}

func (w *dgutaWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
}

func (w *dgutaWriter) Add(dguta db.RecordDGUTA) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if err := w.ensureMetadataPersisted(); err != nil {
		return err
	}

	w.writeBatch = append(w.writeBatch, dguta)
	if len(w.writeBatch) >= w.batchSize {
		w.storeBatch()
		w.writeBatch = w.writeBatch[:0]
	}

	return w.writeErr
}

func (w *dgutaWriter) ensureMetadataPersisted() error {
	if w.metaDone {
		return nil
	}
	if w.mountPath == "" || w.updatedAt.IsZero() {
		return ErrMetadataNotSet
	}
	if len(w.mountPath) == 0 || w.mountPath[0] != '/' || w.mountPath[len(w.mountPath)-1] != '/' {
		return ErrInvalidMountPath
	}

	if err := persistMeta(w.dgutaDB, w.mountPath, w.updatedAt); err != nil {
		return err
	}

	if err := persistMeta(w.childrenDB, w.mountPath, w.updatedAt); err != nil {
		return err
	}

	w.metaDone = true

	return nil
}

func persistMeta(db *bolt.DB, mountPath string, updatedAt time.Time) error {
	if db == nil {
		return fmt.Errorf("nil db: %w", ErrInvalidConfig)
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketName))
		if b == nil {
			return fmt.Errorf("meta bucket missing") //nolint:err113
		}

		if err := b.Put([]byte(metaKeyMountPath), []byte(mountPath)); err != nil {
			return err
		}

		sec := updatedAt.Unix()
		if sec < 0 {
			return ErrInvalidUpdatedAt
		}

		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(sec))
		return b.Put([]byte(metaKeyUpdatedAt), buf)
	})
}

func (w *dgutaWriter) storeBatch() {
	if w.writeErr != nil || len(w.writeBatch) == 0 {
		return
	}

	if err := w.childrenDB.Update(w.storeChildren); err != nil {
		w.writeErr = err
		return
	}

	if err := w.dgutaDB.Update(w.storeDGUTAs); err != nil {
		w.writeErr = err
		return
	}
}

func (w *dgutaWriter) storeChildren(tx *bolt.Tx) error {
	b := tx.Bucket([]byte(childrenBucketName))

	for _, r := range w.writeBatch {
		if len(r.Children) == 0 {
			continue
		}

		parent := string(r.Dir.AppendTo(nil))
		children := make([]string, len(r.Children))
		for i := range r.Children {
			children[i] = parent + strings.TrimSuffix(r.Children[i], "/")
		}

		if err := b.Put(r.Dir.AppendTo(nil), w.encodeChildren(children)); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) storeDGUTAs(tx *bolt.Tx) error {
	b := tx.Bucket([]byte(dgutaBucketName))

	for _, r := range w.writeBatch {
		k, v := r.EncodeToBytes()
		if err := b.Put(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (w *dgutaWriter) encodeChildren(children []string) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, w.codecHandle)
	enc.MustEncode(children)

	return encoded
}

func (w *dgutaWriter) Close() error {
	if len(w.writeBatch) > 0 {
		w.storeBatch()
		w.writeBatch = w.writeBatch[:0]
	}

	var closeErr error

	if w.childrenDB != nil {
		if err := w.childrenDB.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if w.dgutaDB != nil {
		if err := w.dgutaDB.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if w.writeErr != nil {
		closeErr = errors.Join(closeErr, w.writeErr)
	}

	return closeErr
}

// NewDGUTAWriter creates a db.DGUTAWriter backed by Bolt.
// outputDir is the directory where dguta.db and dguta.db.children will be
// created. The directory must already exist.
func NewDGUTAWriter(outputDir string) (db.DGUTAWriter, error) {
	fi, err := os.Stat(outputDir)
	if err != nil {
		return nil, ErrOutputDirMissing
	}
	if !fi.IsDir() {
		return nil, ErrOutputDirMissing
	}

	dgutaPath := filepathJoin(outputDir, dgutaDBBasename)
	childrenPath := filepathJoin(outputDir, childrenDBBasename)

	dgutaDB, err := openBoltWritable(dgutaPath, dgutaBucketName)
	if err != nil {
		return nil, err
	}

	childrenDB, err := openBoltWritable(childrenPath, childrenBucketName)
	if err != nil {
		_ = dgutaDB.Close()
		return nil, err
	}

	return &dgutaWriter{
		outputDir:   outputDir,
		batchSize:   1,
		dgutaDB:     dgutaDB,
		childrenDB:  childrenDB,
		codecHandle: new(codec.BincHandle),
		writeBatch:  make([]db.RecordDGUTA, 0, 1),
		metaDone:    false,
	}, nil
}

func filepathJoin(dir, base string) string {
	if strings.HasSuffix(dir, "/") {
		return dir + base
	}

	return dir + "/" + base
}

func openBoltWritable(path, bucket string) (*bolt.DB, error) {
	db, err := bolt.Open(path, boltFilePerms, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, errc := tx.CreateBucketIfNotExists([]byte(bucket)); errc != nil {
			return errc
		}
		_, errc := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		return errc
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
