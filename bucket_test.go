package leafdb

import "testing"

func TestNestedBucket(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucketIfNotExists([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucketIfNotExists([]byte("child"))
		if err != nil {
			return err
		}
		return child.Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		parent := tx.Bucket([]byte("parent"))
		if parent == nil {
			t.Fatalf("missing parent bucket")
		}
		child := parent.Bucket([]byte("child"))
		if child == nil {
			t.Fatalf("missing child bucket")
		}
		val := child.Get([]byte("k"))
		if string(val) != "v" {
			t.Fatalf("unexpected value: %s", val)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}
