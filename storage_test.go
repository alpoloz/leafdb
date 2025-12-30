package leafdb

import "testing"

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/leaf.db"

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db.Close()

	val, ok := db.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected v1, got %q", val)
	}
}

func TestBufferedFlushOnClose(t *testing.T) {
	path := t.TempDir() + "/leaf-buffered.db"

	db, err := OpenWithOptions(path, &Options{FlushEvery: 3})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db.Close()

	val, ok := db.Get([]byte("b"))
	if !ok || string(val) != "2" {
		t.Fatalf("expected 2, got %q", val)
	}
}
