package leafdb

import "testing"

func TestReadOnlyTx(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("ro"))
		if err != nil {
			if err != ErrTxReadOnly {
				t.Fatalf("expected read-only error, got %v", err)
			}
			return nil
		}
		if bucket != nil {
			t.Fatalf("expected no bucket in read-only tx")
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}
