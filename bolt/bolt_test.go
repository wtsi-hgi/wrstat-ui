package bolt

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenUpdateViewAndBuckets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := open(path, 0o640, &boptions{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bfreelistMapType,
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create bucket and put a value.
	err = db.Update(func(tx *btx) error {
		b, errc := tx.CreateBucketIfNotExists([]byte("bucket1"))
		if errc != nil {
			return errc
		}

		if errc = b.Put([]byte("k"), []byte("v")); errc != nil {
			return errc
		}

		// Idempotent create
		_, errc = tx.CreateBucketIfNotExists([]byte("bucket1"))

		return errc
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Read the value back and count items with ForEach.
	err = db.View(func(tx *btx) error {
		b := tx.Bucket([]byte("bucket1"))
		if b == nil {
			t.Fatalf("bucket1 missing in view")
		}

		if got := string(b.Get([]byte("k"))); got != "v" {
			t.Fatalf("unexpected value: %q", got)
		}

		count := 0

		if errf := b.ForEach(func(k, v []byte) error {
			count++

			return nil
		}); errf != nil {
			return errf
		}

		if count != 1 {
			t.Fatalf("unexpected count: %d", count)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestCursorDeleteAndDeleteBucket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := open(path, 0o640, &boptions{})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Populate two entries then delete one with a cursor, then delete the bucket.
	if err = db.Update(func(tx *btx) error {
		b, errc := tx.CreateBucketIfNotExists([]byte("b2"))
		if errc != nil {
			return errc
		}
		if errc = b.Put([]byte("a"), []byte("1")); errc != nil {
			return errc
		}
		if errc = b.Put([]byte("b"), []byte("2")); errc != nil {
			return errc
		}
		c := b.Cursor()
		k, _ := c.First()
		if string(k) != "a" {
			t.Fatalf("unexpected first key: %q", k)
		}
		if errc = c.Delete(); errc != nil {
			return errc
		}

		return nil
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Verify only one key remains, then delete the bucket and check error on second delete.
	if err = db.Update(func(tx *btx) error {
		b := tx.Bucket([]byte("b2"))
		count := 0
		_ = b.ForEach(func(k, v []byte) error { //nolint:errcheck
			count++

			return nil
		})
		if count != 1 {
			t.Fatalf("expected 1 key after delete, got %d", count)
		}
		if errc := tx.DeleteBucket([]byte("b2")); errc != nil {
			return errc
		}
		if errc := tx.DeleteBucket([]byte("b2")); errc == nil || !errors.Is(errc, ErrBucketNotFound) {
			t.Fatalf("expected ErrBucketNotFound, got %v", errc)
		}

		return nil
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}
}

func TestReadOnlyOption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.db")

	// Create a DB then reopen read-only and ensure update fails.
	db, err := open(path, 0o640, &boptions{})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	ro, err := open(path, 0o640, &boptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("open RO failed: %v", err)
	}
	defer ro.Close()

	if err = ro.Update(func(tx *btx) error { // should fail on read-only DB
		_, errc := tx.CreateBucketIfNotExists([]byte("x"))

		return errc
	}); err == nil {
		t.Fatalf("expected update to fail on read-only DB")
	}
}

func TestMaxKeySizeExposed(t *testing.T) {
	if MaxKeySize <= 0 {
		t.Fatalf("expected MaxKeySize to be > 0, got %d", MaxKeySize)
	}
}

func TestOpenCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "create.db")

	db, err := open(path, 0o640, &boptions{})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	if _, err = os.Stat(path); err != nil {
		t.Fatalf("expected db file to exist: %v", err)
	}
}
