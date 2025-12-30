package leafdb

import "testing"

func TestTransactionCommit(t *testing.T) {
	db := New()
	tx := db.Begin(true)
	if err := tx.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := tx.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	val, ok := tx.Get([]byte("a"))
	if !ok || string(val) != "1" {
		t.Fatalf("expected read-your-writes")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	got, ok := db.Get([]byte("b"))
	if !ok || string(got) != "2" {
		t.Fatalf("expected committed value")
	}
}

func TestTransactionRollback(t *testing.T) {
	db := New()
	tx := db.Begin(true)
	if err := tx.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	tx.Rollback()

	if _, ok := db.Get([]byte("a")); ok {
		t.Fatalf("expected rollback to discard changes")
	}
}

func TestViewUpdateHelpers(t *testing.T) {
	db := New()
	if err := db.Update(func(tx *Tx) error {
		return tx.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		val, ok := tx.Get([]byte("k"))
		if !ok || string(val) != "v" {
			t.Fatalf("expected value in view")
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestReadSnapshotIsolation(t *testing.T) {
	db := New()
	if err := db.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	tx := db.Begin(false)
	if err := db.Set([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	val, ok := tx.Get([]byte("k"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected snapshot value v1, got %q", val)
	}
	tx.Rollback()
}
