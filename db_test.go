package leafdb

import "testing"

func TestBucketPutGet(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("config"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("value"))
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("config"))
		if bucket == nil {
			t.Fatalf("expected bucket")
		}
		val := bucket.Get([]byte("key"))
		if string(val) != "value" {
			t.Fatalf("unexpected value: %s", val)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}
