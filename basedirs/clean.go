package basedirs

import (
	"bytes"

	"go.etcd.io/bbolt"
)

// CleanInvalidDBHistory removes irrelevant paths from the history bucket,
// leaving only those with the specified path prefix.
func CleanInvalidDBHistory(dbPath, prefix string) error {
	db, err := bbolt.Open(dbPath, dbOpenMode, &bbolt.Options{})
	if err != nil {
		return err
	}

	histBucket := []byte(GroupHistoricalBucket)

	toRemove, err := findInvalidHistory(db, histBucket, []byte(prefix))
	if err != nil {
		return err
	}

	if err := cleanHistory(db, histBucket, toRemove); err != nil {
		return err
	}

	return db.Close()
}

// FindInvalidHistoryKeys returns a list of the keys from the history bucket
// that do not contain the specified prefix.
func FindInvalidHistoryKeys(dbPath, prefix string) ([][]byte, error) {
	db, err := bbolt.Open(dbPath, dbOpenMode, &bbolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}

	defer db.Close()

	return findInvalidHistory(db, []byte(GroupHistoricalBucket), []byte(prefix))
}

func findInvalidHistory(db *bbolt.DB, bucket, prefix []byte) ([][]byte, error) {
	const idLen = 4

	var toRemove [][]byte

	if err := db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).ForEachBucket(func(k []byte) error {
			if !bytes.HasPrefix(k[idLen:], prefix) {
				toRemove = append(toRemove, k)
			}

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return toRemove, nil
}

func cleanHistory(db *bbolt.DB, bucket []byte, toRemove [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)

		for _, k := range toRemove {
			if err := b.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}
