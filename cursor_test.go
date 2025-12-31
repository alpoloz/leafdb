package leafdb

import "testing"

func TestCursorIteration(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("items"))
		if err != nil {
			return err
		}
		keys := []string{"a", "b", "c"}
		for _, k := range keys {
			if err := bucket.Put([]byte(k), []byte(k+k)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("items"))
		cursor := bucket.Cursor()
		k, v := cursor.First()
		if string(k) != "a" || string(v) != "aa" {
			t.Fatalf("unexpected first: %s=%s", k, v)
		}
		k, v = cursor.Next()
		if string(k) != "b" || string(v) != "bb" {
			t.Fatalf("unexpected second: %s=%s", k, v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}
