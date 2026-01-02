package bolt

import (
	"bytes"
	"encoding/binary"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	bolt "go.etcd.io/bbolt"
)

const idKeyLen = 4 + 1

type baseDirsHistoryMaintainer struct {
	dbPath string
}

func (m *baseDirsHistoryMaintainer) CleanHistoryForMount(prefix string) error {
	db, err := bolt.Open(m.dbPath, boltFilePerms, &bolt.Options{})
	if err != nil {
		return err
	}
	defer db.Close()

	prefixB := []byte(prefix)

	return db.Update(m.cleanHistoryTxFn(prefixB))
}

func (m *baseDirsHistoryMaintainer) cleanHistoryTxFn(prefixB []byte) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		return iterateAndClean(c, prefixB)
	}
}

func iterateAndClean(c *bolt.Cursor, prefixB []byte) error {
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if shouldDeleteHistoryKey(k, prefixB) {
			if err := c.Delete(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *baseDirsHistoryMaintainer) FindInvalidHistory(prefix string) ([]basedirs.HistoryIssue, error) {
	db, err := bolt.Open(m.dbPath, boltFilePerms, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	prefixB := []byte(prefix)

	var out []basedirs.HistoryIssue

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(basedirs.GroupHistoricalBucket))
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, _ []byte) error {
			if len(k) > idKeyLen && !bytes.HasPrefix(k[idKeyLen:], prefixB) {
				gid := binary.LittleEndian.Uint32(k[:4])
				out = append(out, basedirs.HistoryIssue{GID: gid, MountPath: string(k[idKeyLen:])})
			}

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return out, nil
}

// NewHistoryMaintainer returns a basedirs.HistoryMaintainer backed by the
// Bolt database at dbPath.
func NewHistoryMaintainer(dbPath string) (basedirs.HistoryMaintainer, error) {
	if dbPath == "" {
		return nil, ErrInvalidConfig
	}

	return &baseDirsHistoryMaintainer{dbPath: dbPath}, nil
}

func shouldDeleteHistoryKey(k, prefix []byte) bool {
	if len(k) <= idKeyLen {
		return false
	}

	return !bytes.HasPrefix(k[idKeyLen:], prefix)
}
